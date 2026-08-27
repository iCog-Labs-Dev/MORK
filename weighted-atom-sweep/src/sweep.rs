use crate::traversal::TraversalEngine;
use crate::traversal_factory::build_strategy;
use pathmap::PathMap;
use pathmap::zipper::ZipperValues;
use std::collections::{HashMap, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread::JoinHandle;
use std::time::Duration;
use tracing::{Level, debug, instrument, span};

/// The path of an atom in the trie, represented as a byte vector.
pub type AtomPosition = Vec<u8>;

/// Settings for the weighted atom sweep.
#[derive(Default)]
pub struct WeightedAtomSweepSettings {}

/// Strongly typed process identifier.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ProcessId(pub String);

/// Candidate atom sampled from a sweep traversal engine.
#[derive(Clone, Debug)]
pub struct AtomCandidate {
    pub process_id: ProcessId,
    pub path: Vec<u8>,
    pub snapshot_version: u64,
}

/// Candidate validation counters maintained by MORK's serial consumer.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SweepMetrics {
    pub candidates_consumed: u64,
    pub stale_version_candidates: u64,
    pub missing_path_candidates: u64,
}

/// A single traversal process: an engine with its identifier.
pub struct SweepProcess {
    pub id: ProcessId,
    pub engine: Box<dyn TraversalEngine>,
}

impl SweepProcess {
    /// Create a new process with the given traversal engine.
    pub fn new(id: ProcessId, engine: Box<dyn TraversalEngine>) -> Self {
        Self { id, engine }
    }
}

/// Controls the background WeightedAtomSweep.
pub struct SweepController {
    /// Replaceable latest-snapshot slots for the worker threads.
    snapshot_slots: Vec<Arc<Mutex<Option<Arc<(PathMap<u64>, u64)>>>>>,
    handles: Vec<JoinHandle<()>>,
    shutdown_signal: Arc<AtomicBool>,
}

impl SweepController {
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

        for handle in self.handles.drain(..) {
            handle
                .join()
                .map_err(|_| "thread panicked during shutdown")?;
        }

        debug!("sweep shutdown complete");
        Ok(())
    }

    /// Publish a new snapshot specifically to this controller.
    pub fn publish(&self, snapshot: Arc<(PathMap<u64>, u64)>) {
        for slot in &self.snapshot_slots {
            let mut latest = slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            *latest = Some(snapshot.clone());
        }
    }
}

impl Drop for SweepController {
    fn drop(&mut self) {
        debug!("dropping SweepController, performing resource cleanup");

        self.shutdown_signal.store(true, Ordering::SeqCst);

        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }

        debug!("SweepController dropped cleanly");
    }
}

/// Long-lived registry for sweep processes and controllers.
pub struct WeightedAtomSweep {
    pub processes: HashMap<String, SweepProcess>,
    pub settings: WeightedAtomSweepSettings,
    pub controllers: HashMap<String, SweepController>,
    next_id: usize,
    candidate_tx: mpsc::SyncSender<AtomCandidate>,
    pub candidate_rx: Option<mpsc::Receiver<AtomCandidate>>,
    worker_candidate_txs: HashMap<ProcessId, mpsc::SyncSender<AtomCandidate>>,
    worker_candidate_rxs: HashMap<ProcessId, mpsc::Receiver<AtomCandidate>>,
    pub candidate_buffers: HashMap<ProcessId, VecDeque<AtomCandidate>>,
    selected_candidates: HashMap<ProcessId, AtomCandidate>,
    pub metrics: SweepMetrics,
}

pub const CANDIDATE_BUFFER_CAPACITY: usize = 1000;

impl WeightedAtomSweep {
    #[instrument(skip_all, name = "sweep.new")]
    pub fn new(settings: WeightedAtomSweepSettings) -> Self {
        debug!("initializing WeightedAtomSweep");
        let (candidate_tx, candidate_rx) =
            mpsc::sync_channel::<AtomCandidate>(CANDIDATE_BUFFER_CAPACITY);
        let result = Self {
            processes: HashMap::new(),
            settings,
            controllers: HashMap::new(),
            next_id: 0,
            candidate_tx,
            candidate_rx: Some(candidate_rx),
            worker_candidate_txs: HashMap::new(),
            worker_candidate_rxs: HashMap::new(),
            candidate_buffers: HashMap::new(),
            selected_candidates: HashMap::new(),
            metrics: SweepMetrics::default(),
        };
        debug!("WeightedAtomSweep initialization complete");
        result
    }

    /// Add a traversal engine by strategy key, returning a mutable reference
    /// to the new process.
    #[instrument(skip_all, name = "sweep.add_engine")]
    pub fn add_engine(&mut self, name: &str, strategy_key: &str) -> &mut SweepProcess {
        debug!("adding new traversal engine to sweep");
        let engine = build_strategy(strategy_key)
            .unwrap_or_else(|| panic!("unknown traversal strategy '{strategy_key}'"));

        let process_id = ProcessId(name.to_string());
        let (candidate_tx, candidate_rx) =
            mpsc::sync_channel::<AtomCandidate>(CANDIDATE_BUFFER_CAPACITY);
        self.worker_candidate_txs
            .insert(process_id.clone(), candidate_tx);
        self.worker_candidate_rxs
            .insert(process_id.clone(), candidate_rx);
        let process = SweepProcess::new(process_id, engine);
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

    /// Move available worker events into bounded per-process FIFO buffers.
    pub fn buffer_candidates(&mut self) {
        let mut received = Vec::new();
        if self.worker_candidate_rxs.is_empty() {
            if let Some(rx) = self.candidate_rx.as_ref() {
                while let Ok(candidate) = rx.try_recv() {
                    received.push(candidate);
                }
            }
        }
        for rx in self.worker_candidate_rxs.values() {
            while let Ok(candidate) = rx.try_recv() {
                received.push(candidate);
            }
        }
        for candidate in received {
            let buffer = self.candidate_buffers
                .entry(candidate.process_id.clone())
                .or_default();
            if buffer.len() == CANDIDATE_BUFFER_CAPACITY {
                buffer.pop_front();
            }
            buffer.push_back(candidate);
        }
    }

    pub fn pop_candidate(&mut self, process_id: &ProcessId) -> Option<AtomCandidate> {
        self.buffer_candidates();
        self.candidate_buffers.get_mut(process_id)?.pop_front()
    }

    /// Discard buffered and reserved candidates that do not belong to the
    /// currently eligible completed snapshot.
    pub fn discard_obsolete_candidates(&mut self, snapshot_version: u64) {
        self.buffer_candidates();
        for buffer in self.candidate_buffers.values_mut() {
            let before = buffer.len();
            buffer.retain(|candidate| candidate.snapshot_version == snapshot_version);
            self.metrics.stale_version_candidates += (before - buffer.len()) as u64;
        }
        self.selected_candidates.retain(|_, candidate| {
            let current = candidate.snapshot_version == snapshot_version;
            if !current {
                self.metrics.stale_version_candidates += 1;
            }
            current
        });
    }

    /// Reserve the next candidate whose path still exists in the current live map.
    pub fn select_existing_candidate(
        &mut self,
        process_id: &ProcessId,
        live_map: &PathMap<u64>,
        snapshot_version: u64,
    ) -> bool {
        let Some(candidate) = self.pop_existing_candidate(process_id, live_map, snapshot_version)
        else {
            return false;
        };
        self.selected_candidates.insert(process_id.clone(), candidate);
        true
    }

    /// Consume the next candidate for this process that belongs to the eligible
    /// snapshot and whose complete path still exists in the live map.
    pub fn pop_existing_candidate(
        &mut self,
        process_id: &ProcessId,
        live_map: &PathMap<u64>,
        snapshot_version: u64,
    ) -> Option<AtomCandidate> {
        while let Some(candidate) = self.pop_candidate(process_id) {
            if candidate.snapshot_version != snapshot_version {
                self.metrics.stale_version_candidates += 1;
                continue;
            }
            let exists = live_map.read_zipper_at_path(&candidate.path).val().is_some();
            if exists {
                self.metrics.candidates_consumed += 1;
                return Some(candidate);
            }
            self.metrics.missing_path_candidates += 1;
        }
        None
    }

    pub fn take_selected_candidate(
        &mut self,
        process_id: &ProcessId,
        live_map: &PathMap<u64>,
        snapshot_version: u64,
    ) -> Option<AtomCandidate> {
        let candidate = self.selected_candidates.remove(process_id)?;
        if candidate.snapshot_version != snapshot_version {
            self.metrics.stale_version_candidates += 1;
            return None;
        }
        if live_map.read_zipper_at_path(&candidate.path).val().is_none() {
            self.metrics.missing_path_candidates += 1;
            return None;
        }
        Some(candidate)
    }

    /// Spawn all registered processes into background threads.
    #[instrument(skip_all, name = "sweep.spawn")]
    pub fn spawn(&mut self) -> String {
        let processes = std::mem::take(&mut self.processes);
        let process_count = processes.len();
        debug!(process_count, "spawning WeightedAtomSweep threads");

        if process_count == 0 {
            debug!("warning: no processes added, sweep will do nothing");
        }

        let shutdown = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::new();
        let mut snapshot_slots = Vec::new();

        for (name, process) in processes.into_iter() {
            let engine = process.engine;
            let process_id = process.id;

            let snapshot_slot: Arc<Mutex<Option<Arc<(PathMap<u64>, u64)>>>> =
                Arc::new(Mutex::new(None));
            snapshot_slots.push(snapshot_slot.clone());

            let shutdown_traversal = shutdown.clone();
            let tx = self.worker_candidate_txs.remove(&process_id)
                .expect("missing candidate channel for WAS process");
            let legacy_tx = self.candidate_tx.clone();
            let name_t = name.clone();

            let traversal_handle = std::thread::spawn(move || {
                let _span = span!(Level::DEBUG, "traversal_thread", engine = %name_t).entered();
                debug!("traversal thread started - entering sampling loop");
                let mut current_snapshot: Option<Arc<(PathMap<u64>, u64)>> = None;
                let mut current_version = None;

                loop {
                    if shutdown_traversal.load(Ordering::Acquire) {
                        debug!("shutdown signal received, exiting traversal loop");
                        break;
                    }

                    let latest = snapshot_slot
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .clone();
                    if let Some(latest) = latest {
                        if current_version != Some(latest.1) {
                            engine.snapshot_changed();
                            current_version = Some(latest.1);
                        }
                        current_snapshot = Some(latest);
                    }

                    if let Some(ref snapshot_arc) = current_snapshot {
                        let (map, version) = &**snapshot_arc;

                        match engine.next_atom(map) {
                            Ok(atom_path) => {
                                let candidate = AtomCandidate {
                                    process_id: process_id.clone(),
                                    path: atom_path,
                                    snapshot_version: *version,
                                };
                                let mut sent = false;
                                while !sent {
                                    if shutdown_traversal.load(Ordering::Acquire) {
                                        break;
                                    }
                                    match tx.try_send(candidate.clone()) {
                                        Ok(_) => {
                                            let _ = legacy_tx.try_send(candidate.clone());
                                            sent = true;
                                        }
                                        Err(mpsc::TrySendError::Full(_)) => {
                                            std::thread::sleep(Duration::from_millis(5));
                                        }
                                        Err(mpsc::TrySendError::Disconnected(_)) => {
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(_) => {
                                // backoff if sampling fails
                                std::thread::sleep(Duration::from_millis(10));
                            }
                        }
                    } else {
                        // wait for a snapshot to be published
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
                debug!("traversal thread completed");
            });

            handles.push(traversal_handle);
            debug!(engine = %name, "spawned traversal worker thread for process");
        }

        let name = format!("sweep-{}", self.next_id);
        self.next_id += 1;
        let total_threads = handles.len();

        let controller = SweepController {
            snapshot_slots,
            handles,
            shutdown_signal: shutdown,
        };

        debug!(
            name = %name,
            total_threads,
            "spawn operation complete, returning controller"
        );

        self.controllers.insert(name.clone(), controller);
        name
    }

    /// Publish a snapshot of the PathMap to all controllers.
    pub fn publish_snapshot(&self, snapshot: PathMap<u64>, version: u64) {
        let arc_snapshot = Arc::new((snapshot, version));
        for ctrl in self.controllers.values() {
            ctrl.publish(arc_snapshot.clone());
        }
    }

    /// Shutdown all controllers.
    pub fn shutdown_all(&mut self) {
        debug!("shutting down all sweep controllers");
        let names: Vec<String> = self.controllers.keys().cloned().collect();
        for name in names {
            self.shutdown(&name);
        }
    }

    /// Shutdown a specific controller by name.
    pub fn shutdown(&mut self, name: &str) {
        if let Some(ctrl) = self.controllers.remove(name) {
            debug!(name, "shutting down sweep controller");
            let _ = ctrl.shutdown();
        }
    }
}
