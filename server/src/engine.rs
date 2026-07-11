//! The engine thread: sole owner of the `Space` (which is `!Send` — it must be created and
//! dropped on this thread). All mutation is serialized here; concurrency for readers comes
//! from the O(1) COW snapshots published after every step.
//!
//! Scheduling is two-level: round-robin BETWEEN transaction namespaces (one step each per
//! round, so a 10-step program finishes while a 10⁶-step one runs), plain trie order WITHIN
//! a namespace (the program's own loc-ordering / inference control, untouched).

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use mork::space::Space;
use mork_interning::SharedMappingHandle;
use pathmap::PathMap;
use tokio::sync::{broadcast, mpsc, watch};

use crate::transaction::{Event, ReadSnapshot, Transaction, TxId, TxOk};
use crate::wrap;

pub fn spawn_engine(
    events: broadcast::Sender<Event>,
    active: Arc<Mutex<HashSet<TxId>>>,
) -> (mpsc::Sender<Transaction>, watch::Receiver<Arc<ReadSnapshot>>, std::thread::JoinHandle<()>) {
    let (tx_send, rx) = mpsc::channel::<Transaction>(1024);
    let (snap_tx, snap_rx) = watch::channel(Arc::new(ReadSnapshot::empty()));
    let handle = std::thread::Builder::new()
        .name("mork-engine".into())
        // The kernel's expression machinery (parse/serialize/unify) recurses per nesting
        // level; deeply nested expressions like (S (S … Z)) overflow the 2 MB default.
        // Linux commits stack pages lazily, so a large reservation costs nothing up front.
        .stack_size(512 * 1024 * 1024)
        .spawn(move || run(rx, snap_tx, events, active))
        .expect("failed to spawn engine thread");
    (tx_send, snap_rx, handle)
}

fn run(
    mut rx: mpsc::Receiver<Transaction>,
    snap_tx: watch::Sender<Arc<ReadSnapshot>>,
    events: broadcast::Sender<Event>,
    active: Arc<Mutex<HashSet<TxId>>>,
) {
    let mut space = Space::new(); // created HERE: Space is !Send
    let mut version: u64 = 0;
    let mut rotation: VecDeque<TxId> = VecDeque::new();
    publish(&snap_tx, &space, version);

    'main: loop {
        // Drain everything pending before stepping, so submissions are never starved.
        loop {
            match rx.try_recv() {
                Ok(t) => apply_transaction(&mut space, t, &mut version, &snap_tx, &events, &mut rotation, &active),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break 'main,
            }
        }

        if rotation.is_empty() {
            // Execs can exist outside every known namespace (a program can construct an
            // exec whose loc escapes its wrapper at runtime). Step them so the space always
            // drains, attributing by parsed txid when possible.
            if step_stray(&mut space, &mut version, &snap_tx, &events, &mut rotation, &active) {
                continue;
            }
            let _ = events.send(Event::Idle { version });
            match rx.blocking_recv() {
                Some(t) => apply_transaction(&mut space, t, &mut version, &snap_tx, &events, &mut rotation, &active),
                None => break,
            }
            continue;
        }

        // One step per active namespace per round.
        for _ in 0..rotation.len() {
            let txid = rotation.pop_front().unwrap();
            if step_namespace(&mut space, &txid, &mut version, &snap_tx, &events) {
                rotation.push_back(txid);
            } else {
                active.lock().unwrap().remove(&txid);
                let _ = events.send(Event::Quiescent { tx: txid, version });
            }
            // Stay responsive to submissions arriving mid-round.
            match rx.try_recv() {
                Ok(t) => apply_transaction(&mut space, t, &mut version, &snap_tx, &events, &mut rotation, &active),
                Err(mpsc::error::TryRecvError::Empty) => {}
                Err(mpsc::error::TryRecvError::Disconnected) => break 'main,
            }
        }
    }
    log::info!("engine: transaction channel closed, shutting down");
}

/// Snapshots are taken strictly BETWEEN interpret calls — no write-zipper session is ever
/// live here, which is what makes the O(1) `PathMap::clone()` sound.
fn publish(snap_tx: &watch::Sender<Arc<ReadSnapshot>>, space: &Space, version: u64) {
    snap_tx.send_replace(Arc::new(ReadSnapshot {
        btm: space.btm.clone(),
        sm: space.sm.clone(),
        version,
    }));
}

fn apply_transaction(
    space: &mut Space,
    t: Transaction,
    version: &mut u64,
    snap_tx: &watch::Sender<Arc<ReadSnapshot>>,
    events: &broadcast::Sender<Event>,
    rotation: &mut VecDeque<TxId>,
    active: &Arc<Mutex<HashSet<TxId>>>,
) {
    match space.add_all_sexpr(t.source.as_bytes()) {
        Ok(count) => {
            *version += 1;
            publish(snap_tx, space, *version);
            let _ = events.send(Event::Tx { tx: t.id.clone(), count, version: *version });
            active.lock().unwrap().insert(t.id.clone());
            rotation.push_back(t.id.clone());
            let _ = t.reply.send(Ok(TxOk { tx: t.id, count, version: *version }));
        }
        Err(e) => {
            // wrap::rewrite validated the syntax up front, so this is rare — but the kernel
            // loader writes as it parses, so a mid-body failure may leave a partial load.
            let msg = format!("load failed (transaction may be partially applied): {e}");
            let _ = events.send(Event::Error { tx: Some(t.id.clone()), message: msg.clone() });
            let _ = t.reply.send(Err(msg));
        }
    }
}

/// Run exactly one VM step rooted at `(exec (<txid> …))`. Returns false when that
/// namespace has no pending exec (quiescent).
fn step_namespace(
    space: &mut Space,
    txid: &TxId,
    version: &mut u64,
    snap_tx: &watch::Sender<Arc<ReadSnapshot>>,
    events: &broadcast::Sender<Event>,
) -> bool {
    let prefix = wrap::ns_loc_prefix(txid);
    step_at(space, &prefix, Some(txid.clone()), version, snap_tx, events).is_some()
}

/// Step the whole space once (empty prefix) — only used when every known namespace is
/// quiescent but execs remain. If the consumed exec carries a parseable txid, the caller
/// revives that namespace.
fn step_stray(
    space: &mut Space,
    version: &mut u64,
    snap_tx: &watch::Sender<Arc<ReadSnapshot>>,
    events: &broadcast::Sender<Event>,
    rotation: &mut VecDeque<TxId>,
    active: &Arc<Mutex<HashSet<TxId>>>,
) -> bool {
    match step_at(space, &[], None, version, snap_tx, events) {
        None => false,
        Some(None) => true,
        Some(Some(txid)) => {
            // The stray belonged to a namespace we thought quiescent — revive it.
            active.lock().unwrap().insert(txid.clone());
            rotation.push_back(txid);
            true
        }
    }
}

/// Shared single-step: `None` = nothing to step; `Some(txid_of_exec)` otherwise.
fn step_at(
    space: &mut Space,
    loc_prefix: &[u8],
    attributed: Option<TxId>,
    version: &mut u64,
    snap_tx: &watch::Sender<Arc<ReadSnapshot>>,
    events: &broadcast::Sender<Event>,
) -> Option<Option<TxId>> {
    let sm = space.sm.clone();
    let mut v = *version;
    let mut ev: Option<Event> = None;
    let mut exec_txid: Option<TxId> = None;

    let done = space.metta_calculus_scoped(loc_prefix, 1, |info| {
        v += 1;
        let raw = exec_to_text(info.exec, &sm);
        let (exec, parsed_txid) = wrap::unwrap_text(&raw);
        let tx = attributed.clone().or_else(|| parsed_txid.clone()).unwrap_or_else(|| "?".into());
        exec_txid = parsed_txid;
        ev = Some(Event::Step {
            tx,
            exec,
            touched: info.touched,
            new: info.new,
            us: info.micros,
            version: v,
            error: info.error.map(str::to_string),
        });
        true
    });

    *version = v;
    if done == 0 {
        return None;
    }
    publish(snap_tx, space, *version);
    if let Some(e) = ev {
        let _ = events.send(e);
    }
    Some(exec_txid)
}

/// Serialize one stored expression exactly the way `/export` does: insert its path into a
/// throwaway map and reuse the kernel's own dump (handles the `interning` feature
/// consistently).
pub fn exec_to_text(bytes: &[u8], sm: &SharedMappingHandle) -> String {
    let mut m: PathMap<()> = PathMap::new();
    m.insert(bytes, ());
    let mut v = Vec::new();
    let _ = Space::dump_all_sexpr_from(&m, sm, &mut v);
    let mut s = String::from_utf8_lossy(&v).into_owned();
    while s.ends_with('\n') { s.pop(); }
    s
}
