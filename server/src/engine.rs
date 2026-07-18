//! The engine thread: sole owner of the `Space` (which is `!Send` — it must be created and
//! dropped on this thread). All mutation is serialized here; concurrency for readers comes
//! from the O(1) COW snapshots published after every step.
//!
//! Scheduling is purely sequential: one transaction runs to quiescence before the next is
//! dequeued (submissions wait in the channel, unapplied — applying them mid-run would
//! change what the running program observes). Sequential execution is what makes every
//! transaction atomic for free: the O(1) COW clone of the trie taken at tx start is a
//! complete rollback image, and nothing else runs in between that could observe — and
//! outlive — state we might revert. A failing exec (or a partial load) reverts the whole
//! transaction with one pointer swap.

use std::collections::HashSet;
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
    publish(&snap_tx, &space, version);

    loop {
        // Drain queued submissions before parking; `Idle` is only truthful when both the
        // space and the queue are empty.
        match rx.try_recv() {
            Ok(t) => {
                run_tx(&mut space, t, &mut version, &snap_tx, &events, &active);
                continue;
            }
            Err(mpsc::error::TryRecvError::Disconnected) => break,
            Err(mpsc::error::TryRecvError::Empty) => {}
        }
        let _ = events.send(Event::Idle { version });
        match rx.blocking_recv() {
            Some(t) => run_tx(&mut space, t, &mut version, &snap_tx, &events, &active),
            None => break,
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

/// Run one transaction start to finish: load atomically, then step until nothing in the
/// space can step. The COW clone taken up front (same soundness argument as `publish`)
/// makes the transaction atomic — a partial load or a failing exec rolls back to it.
fn run_tx(
    space: &mut Space,
    t: Transaction,
    version: &mut u64,
    snap_tx: &watch::Sender<Arc<ReadSnapshot>>,
    events: &broadcast::Sender<Event>,
    active: &Arc<Mutex<HashSet<TxId>>>,
) {
    let undo = space.btm.clone();

    match space.add_all_sexpr(t.source.as_bytes()) {
        Ok(count) => {
            *version += 1;
            publish(snap_tx, space, *version);
            let _ = events.send(Event::Tx { tx: t.id.clone(), count, version: *version });
            active.lock().unwrap().insert(t.id.clone());
            let _ = t.reply.send(Ok(TxOk { tx: t.id.clone(), count, version: *version }));
        }
        Err(e) => {
            // The kernel loader writes as it parses; the clone undoes any partial load.
            // Nothing was published since the clone, so the version doesn't move.
            space.btm = undo;
            let reason = format!("load failed: {e}");
            let _ = events.send(Event::Abort { tx: t.id.clone(), reason: reason.clone(), version: *version });
            let _ = t.reply.send(Err(reason));
            return;
        }
    }

    loop {
        match step_once(space, &t.id, version, snap_tx, events) {
            StepOutcome::Stepped => {}
            StepOutcome::Done => {
                let _ = events.send(Event::Quiescent { tx: t.id.clone(), version: *version });
                break;
            }
            StepOutcome::Failed(reason) => {
                // Rollback IS a new observable state (steps were published since the
                // clone): bump + publish so /export and the delta stream see the revert.
                space.btm = undo;
                *version += 1;
                publish(snap_tx, space, *version);
                let _ = events.send(Event::Abort { tx: t.id.clone(), reason, version: *version });
                break;
            }
        }
    }
    active.lock().unwrap().remove(&t.id);
}

enum StepOutcome {
    Stepped,
    /// Nothing anywhere in the space can step — the transaction quiesced.
    Done,
    /// The interpreter rejected an exec; the caller rolls the whole transaction back.
    Failed(String),
}

/// One deterministic VM step for `txid`: its own namespace first; only when that is empty,
/// one whole-space step. The latter drains "strays" — execs whose loc a program moved out
/// of its wrapper at runtime — so the space is always fully drained at quiescence. Both
/// picks are trie order: which exec fires is a pure function of trie contents (this is
/// what will make WAL replay deterministic).
fn step_once(
    space: &mut Space,
    txid: &TxId,
    version: &mut u64,
    snap_tx: &watch::Sender<Arc<ReadSnapshot>>,
    events: &broadcast::Sender<Event>,
) -> StepOutcome {
    let prefix = wrap::ns_loc_prefix(txid);
    match step_at(space, &prefix, Some(txid), version, snap_tx, events) {
        Some(outcome) => outcome,
        None => step_at(space, &[], None, version, snap_tx, events).unwrap_or(StepOutcome::Done),
    }
}

/// `None` = nothing to step under this prefix. On success bumps the version, publishes the
/// snapshot and emits the `step` event; on interpreter error does none of that — the exec
/// was consumed and its step wasted, but the caller reverts the entire transaction anyway.
fn step_at(
    space: &mut Space,
    loc_prefix: &[u8],
    attributed: Option<&TxId>,
    version: &mut u64,
    snap_tx: &watch::Sender<Arc<ReadSnapshot>>,
    events: &broadcast::Sender<Event>,
) -> Option<StepOutcome> {
    let sm = space.sm.clone();
    let v = *version + 1;
    let mut ev: Option<Event> = None;
    let mut failure: Option<String> = None;

    let done = space.metta_calculus_scoped(loc_prefix, 1, |info| {
        let raw = exec_to_text(info.exec, &sm);
        let (exec, parsed_txid) = wrap::unwrap_text(&raw);
        match info.error {
            Some(e) => failure = Some(format!("exec {exec}: {e}")),
            None => {
                // Stray steps (no `attributed`) are attributed by the txid parsed out of
                // the exec's own wrapper, when it still carries one.
                let tx = attributed.cloned().or(parsed_txid).unwrap_or_else(|| "?".into());
                ev = Some(Event::Step {
                    tx,
                    exec,
                    touched: info.touched,
                    new: info.new,
                    us: info.micros,
                    version: v,
                });
            }
        }
        true
    });

    if done == 0 {
        return None;
    }
    if let Some(reason) = failure {
        return Some(StepOutcome::Failed(reason));
    }
    *version = v;
    publish(snap_tx, space, *version);
    if let Some(e) = ev {
        let _ = events.send(e);
    }
    Some(StepOutcome::Stepped)
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
