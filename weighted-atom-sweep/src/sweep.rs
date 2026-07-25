use crate::map::WeightedMap;
use crate::operation::{OperationObserver, TransformOp};
use crate::traversal::TraversalEngine;
use crate::new_eng_op::build_strategy;
use pathmap::zipper::{ZipperCreation, ZipperHeadOwned, ZipperMoving};
use pathmap::PathMap;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc, Arc, RwLock,
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tracing::{debug, instrument, span, trace, Level};

/// The path of an atom in the trie, represented as a byte vector.
pub type AtomPosition = Vec<u8>;

/// Settings for the weighted atom sweep.
#[derive(Default)]
pub struct WeightedAtomSweepSettings {}

/// A single traversal process: an engine paired with a set of operations.
pub struct SweepProcess {
    pub engine: Box<dyn TraversalEngine>,
    pub operations: Vec<Box<dyn TransformOp>>,
}

impl SweepProcess {
    /// Create a new process with the given traversal engine.
    pub fn new(engine: Box<dyn TraversalEngine>) -> Self {
        Self {
            engine,
            operations: Vec::new(),
        }
    }

    /// Get the number of operations subscribed to this process.
    pub fn operation_count(&self) -> usize {
        self.operations.len()
    }
}

impl OperationObserver for SweepProcess {
    #[instrument(skip_all, name = "process.subscribe")]
    fn subscribe(&mut self, operation: Box<dyn TransformOp>) {
        let name = operation.name().to_string();
        let total_operations = self.operations.len() + 1;
        debug!(
            operation_name = %name,
            total_operations, "subscribing operation to process"
        );
        self.operations.push(operation);
    }

    #[instrument(skip_all, name = "process.unsubscribe_by_name")]
    fn unsubscribe_by_name(&mut self, name: &str) {
        let total_operations = self.operations.len() - 1;
        debug!(
            operation_name = %name,
            total_operations, "unsubscribing operation from process"
        );
        self.operations.retain(|op| op.name() != name);
    }
}

/// Controls the background WeightedAtomSweep.
///
/// ### Channel Preservation
/// The internal mpsc channels and operations buffer are fully preserved and survive
/// pause/resume cycles. Atoms queued during traversal remain in the queue and are
/// processed immediately when the sweep is resumed, guaranteeing zero atom loss.
pub struct SweepController {
    /// The shared container holding the active trie map.
    /// Threads clone the Arc briefly on each iteration, releasing it immediately
    /// to minimize lock contention and allow the trie to be reclaimed.
    pub map: Arc<RwLock<Option<Arc<ZipperHeadOwned<u64>>>>>,
    handles: Vec<JoinHandle<()>>,
    shutdown_signal: Arc<AtomicBool>,
    paused_signal: Arc<AtomicBool>,
    parked_count: Arc<AtomicUsize>,
}

impl SweepController {
    /// Check if the controller is active and running (not paused and threads are alive).
    pub fn is_available(&self) -> bool {
        !self.paused_signal.load(Ordering::Acquire) && !self.shutdown_signal.load(Ordering::Acquire)
    }

    /// Check if the controller is currently paused.
    pub fn is_paused(&self) -> bool {
        self.paused_signal.load(Ordering::Acquire)
    }

    /// Retrieve the current count of parked threads.
    pub fn parked_count(&self) -> usize {
        self.parked_count.load(Ordering::Acquire)
    }

    /// Retrieve the total number of managed threads.
    pub fn thread_count(&self) -> usize {
        self.handles.len()
    }

    /// Wait for sweep to complete naturally.
    pub fn wait(mut self) -> Result<(), Box<dyn std::error::Error>> {
        debug!("waiting for sweep completion");
        for handle in self.handles.drain(..) {
            handle.join().map_err(|_| "thread panicked")?;
        }
        debug!("sweep completed");
        Ok(())
    }

    /// Signal threads to shutdown and wait for them to terminate.
    pub fn shutdown(mut self) -> Result<(), Box<dyn std::error::Error>> {
        debug!("initiating sweep shutdown");
        self.shutdown_signal.store(true, Ordering::SeqCst);
        self.paused_signal.store(false, Ordering::SeqCst); // Unpark any parked threads

        for handle in self.handles.drain(..) {
            handle.join().map_err(|_| "thread panicked during shutdown")?;
        }

        // Clear the map holder to release the trie
        if let Ok(mut guard) = self.map.write() {
            *guard = None;
        }

        debug!("sweep shutdown complete");
        Ok(())
    }

    /// Pause all background threads, wait for them to park, and reclaim exclusive ownership
    /// of the trie, returning it as a PathMap for foreground access.
    pub fn pause(&self) -> PathMap<u64> {
        debug!("starting sweep pause sequence");

        let total_threads = self.handles.len();
        if total_threads == 0 {
            panic!("no threads are currently active in this sweep");
        }

        if self.paused_signal.load(Ordering::Acquire) {
            panic!("sweep is already paused");
        }

        self.paused_signal.store(true, Ordering::SeqCst);

        let pause_deadline = Instant::now() + Duration::from_secs(30);
        while self.parked_count.load(Ordering::SeqCst) < total_threads {
            if self.shutdown_signal.load(Ordering::Acquire) {
                break;
            }
            if Instant::now() > pause_deadline {
                panic!(
                    "pause() timed out after 30s waiting for {}/{} threads to park",
                    self.parked_count.load(Ordering::SeqCst),
                    total_threads
                );
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        debug!("all sweep threads parked and quiescent");

        let map_arc = {
            let mut guard = self.map.write().expect("failed to acquire write lock");
            guard.take().expect("map was already leased or is missing")
        };

        debug_assert_eq!(
            Arc::strong_count(&map_arc),
            1,
            "Invariant violated: Arc strong count must be exactly 1 when threads are parked"
        );

        let zipper_head = Arc::try_unwrap(map_arc)
            .expect("failed to reclaim leased trie: references still held");

        let path_map = zipper_head.into_map();

        debug!("sweep pause sequence completed successfully");
        path_map
    }

    /// Return an updated trie back to the background sweep and resume thread execution.
    pub fn resume(&self, new_map: PathMap<u64>) {
        debug!("starting sweep resume sequence");

        if new_map.val_count() == 0 {
            debug!("warning: incoming PathMap for resume is empty");
        }

        let head = Arc::new(new_map.into_zipper_head([]));

        {
            let mut guard = self.map.write().expect("failed to acquire write lock");
            *guard = Some(head);
        }

        self.paused_signal.store(false, Ordering::SeqCst);

        debug!("sweep resume sequence completed successfully");
    }
}

impl Drop for SweepController {
    fn drop(&mut self) {
        debug!("dropping SweepController, performing resource cleanup");

        self.shutdown_signal.store(true, Ordering::SeqCst);
        self.paused_signal.store(false, Ordering::SeqCst);

        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }

        // Do NOT null `self.map` here: it is the shared lease slot cloned across every
        // controller of this WAS. Nulling it on a non-last drop would pull the trie out
        // from under the still-running sweeps. The WAS-level pause_all/shutdown own the
        // slot's lifecycle and reclaim the trie explicitly.
        debug!("SweepController dropped cleanly");
    }
}

/// Long-lived registry for sweep processes and controllers.
///
/// Manages multiple [`SweepProcess`] instances and their spawned [`SweepController`]s.
/// Supports pause/resume/shutdown lifecycle for all controllers.
pub struct WeightedAtomSweep {
    pub processes: HashMap<String, SweepProcess>,
    pub settings: WeightedAtomSweepSettings,
    pub map: Option<WeightedMap>,
    pub controllers: HashMap<String, SweepController>,
    next_id: usize,
}

impl WeightedAtomSweep {
    /// this will automatically instantiate a weighted map if sweep is called before any foreground metta calculus task
    pub fn init_map(&mut self) {
        if self.map.is_none() {
            self.map = Some(WeightedMap {
                inner: Arc::new(PathMap::<u64>::new().into_zipper_head([])),
            });
        }
    }

    /// Transfer a PathMap into the sweep as its weighted map (STATE B).
    /// Used by Space::sweep() to give its btm to the sweep threads.
    pub fn take_trie(&mut self, btm: PathMap<u64>) {
        self.map = Some(WeightedMap {
            inner: Arc::new(btm.into_zipper_head([])),
        });
    }

    #[instrument(skip_all, name = "sweep.new")]
    pub fn new(settings: WeightedAtomSweepSettings) -> Self {
        debug!("initializing WeightedAtomSweep");
        let result = Self {
            processes: HashMap::new(),
            settings,
            map: None,
            controllers: HashMap::new(),
            next_id: 0,
        };
        debug!("WeightedAtomSweep initialization complete");
        result
    }

    /// Add a traversal engine by strategy key, returning a mutable reference
    /// to the new process for operation subscription.
    #[instrument(skip_all, name = "sweep.add_engine")]
    pub fn add_engine(&mut self, name: &str, strategy_key: &str) -> &mut SweepProcess {
        debug!("adding new traversal engine to sweep");
        let engine = build_strategy(strategy_key)
            .unwrap_or_else(|| panic!("unknown traversal strategy '{strategy_key}'"));

        let process = SweepProcess::new(engine);
        self.processes.insert(name.to_string(), process);

        let process_count = self.processes.len();
        debug!(process_count, "engine added successfully");

        self.processes.get_mut(name).unwrap()
    }

    /// Get the number of registered processes.
    pub fn process_count(&self) -> usize {
        self.processes.len()
    }

    /// Get a mutable reference to a registered process by name.
    pub fn get_process_mut(&mut self, name: &str) -> Option<&mut SweepProcess> {
        self.processes.get_mut(name)
    }

    /// Spawn all registered processes into background threads.
    ///
    /// Takes the pending processes (draining them), creates a shared
    /// `Arc<RwLock<Option<Arc<ZipperHeadOwned>>>>` for all spawned controllers,
    /// and returns a unique handle name.
    ///
    /// The caller must ensure `self.map.is_some()` before calling spawn.
    /// Space is responsible for setting `was.map` during the A→B transition.
    #[instrument(skip_all, name = "sweep.spawn")]
    pub fn spawn(&mut self) -> String {
        let processes = std::mem::take(&mut self.processes);
        let process_count = processes.len();
        debug!(process_count, "spawning WeightedAtomSweep threads");

        if process_count == 0 {
            debug!("warning: no processes added, sweep will do nothing");
        }

        self.init_map();
        // Every controller spawned by this WAS shares ONE lease slot. Reuse the slot
        // an existing controller already holds; only the first spawn mints it. This is
        // what lets pause_all/shutdown drop the leased head to strong_count 1 and
        // reclaim the trie no matter how many sweeps are running — the previous code
        // minted a fresh slot per spawn, so N sweeps left N live clones and reclaim
        // panicked in the multi-sweep case.
        let map_lock = match self.controllers.values().next() {
            Some(ctrl) => ctrl.map.clone(),
            None => Arc::new(RwLock::new(Some(self.map.as_ref().unwrap().inner.clone()))),
        };
        let shutdown = Arc::new(AtomicBool::new(false));
        let paused = Arc::new(AtomicBool::new(false));
        let parked_count = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for (name, process) in processes.into_iter() {
            let engine = process.engine;
            let operations = process.operations;

            let map_lock_t = map_lock.clone();
            let map_lock_o = map_lock.clone();
            let shutdown_traversal = shutdown.clone();
            let shutdown_operations = shutdown.clone();
            let pause_flag = Arc::new(AtomicBool::new(false));
            let pause_for_traversal = pause_flag.clone();
            let pause_for_operations = pause_flag.clone();

            let paused_t = paused.clone();
            let paused_o = paused.clone();
            let parked_t = parked_count.clone();
            let parked_o = parked_count.clone();

            let (atom_sender, atom_receiver) = mpsc::channel::<AtomPosition>();

            let name_t = name.clone();
            let name_o = name.clone();

            let traversal_handle = std::thread::spawn(move || {
                let _span = span!(Level::DEBUG, "traversal_thread", engine = %name_t).entered();
                debug!("traversal thread started - entering sampling loop");
                loop {
                    if paused_t.load(Ordering::Acquire) {
                        parked_t.fetch_add(1, Ordering::SeqCst);
                        while paused_t.load(Ordering::Acquire) {
                            if shutdown_traversal.load(Ordering::Acquire) { break; }
                            std::thread::sleep(Duration::from_millis(10));
                        }
                        parked_t.fetch_sub(1, Ordering::SeqCst);
                    }

                    if shutdown_traversal.load(Ordering::Acquire) {
                        debug!("shutdown signal received, exiting traversal loop");
                        break;
                    }

                    while pause_for_traversal.load(Ordering::Acquire) {
                        if paused_t.load(Ordering::Acquire) { break; }
                        if shutdown_traversal.load(Ordering::Acquire) { break; }
                        std::thread::yield_now();
                    }

                    if shutdown_traversal.load(Ordering::Acquire) {
                        break;
                    }

                    let mut sampled = false;
                    {
                        let local_map_opt = {
                            if let Ok(guard) = map_lock_t.read() {
                                guard.clone()
                            } else {
                                None
                            }
                        };

                        if let Some(map_arc) = local_map_opt {
                            if let Ok(traverse_zp) = (*map_arc).read_zipper_at_borrowed_path(&[]) {
                                match engine.next_atom(traverse_zp) {
                                    Ok(atom_path) => {
                                        if atom_sender.send(atom_path).is_ok() {
                                            sampled = true;
                                        } else {
                                            break;
                                        }
                                    }
                                    Err(_) => {}
                                }
                            }
                        }
                    }

                    if !sampled {
                        // Backoff to avoid spinning 100% CPU when sampling fails
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
                debug!("traversal thread completed");
            });

            handles.push(traversal_handle);

            let operations_handle = std::thread::spawn(move || {
                let _span = span!(Level::DEBUG, "operations_thread", engine = %name_o).entered();
                let mut buffer: Vec<AtomPosition> = Vec::new();

                loop {
                    if paused_o.load(Ordering::Acquire) {
                        parked_o.fetch_add(1, Ordering::SeqCst);
                        while paused_o.load(Ordering::Acquire) {
                            if shutdown_operations.load(Ordering::Acquire) { break; }
                            std::thread::sleep(Duration::from_millis(10));
                        }
                        parked_o.fetch_sub(1, Ordering::SeqCst);
                    }

                    if shutdown_operations.load(Ordering::Acquire) {
                        debug!("operations thread: shutdown detected, exiting");
                        break;
                    }

                    if let Ok(atom_path) = atom_receiver.try_recv() {
                        buffer.push(atom_path);
                    }

                    let mut made_progress = false;
                    if !buffer.is_empty() {
                        let local_map_opt = {
                            if let Ok(guard) = map_lock_o.read() {
                                guard.clone()
                            } else {
                                None
                            }
                        };

                        if let Some(map_arc) = local_map_opt {
                            let mut i = 0;
                            while i < buffer.len() {
                                let atom_path = &buffer[i];
                                let write_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    map_arc.write_zipper_at_exclusive_path(&atom_path[..])
                                }));
                                let wz_result: Result<_, _> = match write_result {
                                    Ok(Ok(wz)) => Ok(wz),
                                    Ok(Err(e)) => Err(e),
                                    Err(panic_payload) => {
                                        let msg = panic_payload.downcast_ref::<&str>().unwrap_or(&"unknown");
                                        debug!(
                                            "operations thread: write_zipper panicked ({}), skipping atom",
                                            msg
                                        );
                                        pause_for_operations.store(true, Ordering::Release);
                                        i += 1;
                                        continue;
                                    }
                                };
                                match wz_result {
                                    Ok(mut wz) => {
                                        for op in operations.iter() {
                                            let _result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                                op.apply(&mut wz, atom_path);
                                            }));
                                            wz.reset();
                                        }
                                        map_arc.cleanup_write_zipper_w(wz);
                                        buffer.remove(i);
                                        made_progress = true;
                                    }
                                    Err(_) => {
                                        pause_for_operations.store(true, Ordering::Release);
                                        i += 1;
                                    }
                                }
                            }
                        }
                    }

                    if buffer.is_empty() {
                        pause_for_operations.store(false, Ordering::Release);
                    }

                    if !made_progress && !buffer.is_empty() {
                        std::thread::yield_now();
                    }

                    if buffer.is_empty() {
                        match atom_receiver.try_recv() {
                            Ok(atom_path) => {
                                buffer.push(atom_path);
                            }
                            Err(mpsc::TryRecvError::Disconnected) => {
                                break;
                            }
                            Err(mpsc::TryRecvError::Empty) => {
                                std::thread::sleep(Duration::from_millis(5));
                            }
                        }
                    }
                }
                debug!("operations thread completed");
                shutdown_operations.store(true, Ordering::Release);
            });
            handles.push(operations_handle);

            debug!(engine = %name, "spawned thread pair for process");
        }

        let name = format!("sweep-{}", self.next_id);
        self.next_id += 1;
        let total_threads = handles.len();

        let controller = SweepController {
            map: map_lock,
            handles,
            shutdown_signal: shutdown,
            paused_signal: paused,
            parked_count,
        };

        debug!(
            name = %name,
            total_threads,
            "spawn operation complete, returning controller"
        );

        self.controllers.insert(name.clone(), controller);
        name
    }

    /// Pause all controllers and reclaim the trie.
    ///
    /// Sets the paused signal on all controllers, waits for all threads to park,
    /// then reclaims the trie from the shared map slot. All controllers share the
    /// same `Arc<RwLock<Option<Arc<ZipperHeadOwned>>>>` so pausing/reclaiming once
    /// on any controller suffices for the shared slot — but we must wait for ALL
    /// threads (across all controllers) to park before reclaiming.
    pub fn pause_all(&mut self) -> PathMap<u64> {
        debug!("pausing all sweep controllers");

        if self.controllers.is_empty() {
            panic!("no controllers to pause");
        }

        // Signal pause to all controllers
        for ctrl in self.controllers.values() {
            ctrl.paused_signal.store(true, Ordering::SeqCst);
        }

        // Wait for ALL threads across all controllers to park
        let pause_deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let total_parked: usize = self.controllers.values().map(|c| c.parked_count.load(Ordering::SeqCst)).sum();
            let total_threads: usize = self.controllers.values().map(|c| c.handles.len()).sum();
            if total_parked >= total_threads {
                break;
            }
            if std::time::Instant::now() > pause_deadline {
                panic!(
                    "pause_all() timed out after 30s waiting for {}/{} threads to park",
                    total_parked, total_threads
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        debug!("all sweep threads parked and quiescent");

        // Reclaim the trie from the first controller's shared slot
        // (all controllers share the same Arc<RwLock<...>>)
        let first_ctrl = self.controllers.values().next().unwrap();
        let map_arc = {
            let mut guard = first_ctrl.map.write().expect("failed to acquire write lock");
            guard.take().expect("map was already leased or is missing")
        };

        // Clear self.map reference so that the Arc strong count drops to 1 for unwrap
        self.map = None;

        debug_assert_eq!(
            Arc::strong_count(&map_arc),
            1,
            "Invariant violated: Arc strong count must be exactly 1 when threads are parked"
        );

        let zipper_head = Arc::try_unwrap(map_arc)
            .expect("failed to reclaim leased trie: references still held");

        let path_map = zipper_head.into_map();

        debug!("sweep pause_all completed successfully");
        path_map
    }

    /// Resume all controllers with an updated trie.
    pub fn resume_all(&mut self, map: PathMap<u64>) {
        debug!("resuming all sweep controllers");
        let head = Arc::new(map.into_zipper_head([]));
        let weighted = WeightedMap { inner: head.clone() };
        self.map = Some(weighted);
        for ctrl in self.controllers.values() {
            // Put the head into the shared slot
            if let Ok(mut guard) = ctrl.map.write() {
                *guard = Some(head.clone());
            }
        }
        // Clear paused signal for all
        for ctrl in self.controllers.values() {
            ctrl.paused_signal.store(false, Ordering::SeqCst);
        }
    }

    /// Shutdown all controllers and reclaim the trie.
    pub fn shutdown_all(&mut self) -> Option<PathMap<u64>> {
        debug!("shutting down all sweep controllers");
        let names: Vec<String> = self.controllers.keys().cloned().collect();
        let mut result = None;
        for name in names {
            if let Some(map) = self.shutdown(&name) {
                result = Some(map);
            }
        }
        result
    }

    /// Shutdown a specific controller by name and reclaim its trie.
    pub fn shutdown(&mut self, name: &str) -> Option<PathMap<u64>> {
        if let Some(mut ctrl) = self.controllers.remove(name) {
            debug!(name, "shutting down sweep controller");
            let is_last = self.controllers.is_empty();

            let mut map_arc = None;
            if is_last {
                // Take Arc and clear self.map before joining threads to allow try_unwrap to succeed
                self.map = None;
                if let Ok(mut guard) = ctrl.map.write() {
                    map_arc = guard.take();
                }
            }

            // Signal shutdown and join all threads to ensure references are released
            ctrl.shutdown_signal.store(true, Ordering::SeqCst);
            ctrl.paused_signal.store(false, Ordering::SeqCst);
            for handle in ctrl.handles.drain(..) {
                let _ = handle.join();
            }

            if let Some(arc) = map_arc {
                if let Ok(head) = Arc::try_unwrap(arc) {
                    let map = head.into_map();
                    self.map = Some(WeightedMap {
                        inner: Arc::new(PathMap::<u64>::new().into_zipper_head([])),
                    });
                    return Some(map);
                }
            }
        }
        None
    }
}
