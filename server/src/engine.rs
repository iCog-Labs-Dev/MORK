//! The committer thread: dispatches transactions to a pool of worker threads and
//! validates + installs whatever they hand back. `Committed` (mvcc.rs) is owned
//! outright here — no lock, one owner — while workers run against O(1) copy-on-write
//! snapshots of it, sharing nothing.
//!
//! Concurrency for readers still comes from the O(1) COW snapshots published after
//! every commit. What changed from the old sequential engine is where the work
//! happens: a worker steps a transaction to completion against its own snapshot
//! before the committer ever sees it, so multiple transactions can be in flight at
//! once. Correctness under concurrency comes from `mvcc::validate`: a transaction is
//! installed only if nothing it read or removed-by-pattern was concurrently written.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use mork::space::Space;
use mork_expr::Expr;
use mork_interning::SharedMappingHandle;
use pathmap::PathMap;
use tokio::sync::{broadcast, mpsc, watch};

use crate::mvcc;
use crate::transaction::{Event, ReadSnapshot, Transaction, TxId, TxOk};
use crate::wal::{FsyncPolicy, Wal};
use crate::{worker, wrap};

/// What happens when a transaction exhausts its step budget.
#[derive(Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum BudgetAction {
    /// Keep partial progress; park still-pending execs as inert `(paused …)` data.
    Commit,
    /// Roll the whole transaction back, as if it never happened.
    Abort,
}

pub struct EngineConfig {
    pub step_budget: u64,
    pub budget_action: BudgetAction,
    /// Persistence root; `None` = pure in-memory (no WAL, no recovery). Temporarily
    /// `Some` is refused at startup — see the comment in `run`.
    pub data_dir: Option<std::path::PathBuf>,
    /// Unread while persistence is disabled (see `run`); kept so the CLI flag and
    /// `EngineConfig`'s shape don't need to change again once the WAL rework lands.
    #[allow(dead_code)]
    pub fsync: FsyncPolicy,
    /// Checkpoint + rotate + GC old segments every N finished transactions; 0 = never
    /// (the log then grows without bound and recovery replays it in full).
    pub checkpoint_every: u64,
    /// Shared with the HTTP layer; recovery restores it before `ready` fires.
    pub tx_counter: Arc<AtomicU64>,
    /// Number of concurrent worker threads. 1 = the previous sequential engine.
    pub workers: usize,
}

/// Returned alongside the channels: fires once startup is done and the snapshot is
/// published. `main` must not bind the listener before this — a request arriving
/// too early could mint a txid before the counter is set up.
pub type ReadySignal = std::sync::mpsc::Receiver<()>;

pub fn spawn_engine(
    events: broadcast::Sender<Event>,
    active: Arc<Mutex<HashSet<TxId>>>,
    cfg: EngineConfig,
) -> (
    mpsc::Sender<Transaction>,
    watch::Receiver<Arc<ReadSnapshot>>,
    ReadySignal,
    std::thread::JoinHandle<()>,
) {
    let (tx_send, rx) = mpsc::channel::<Transaction>(1024);
    let (snap_tx, snap_rx) = watch::channel(Arc::new(ReadSnapshot::empty()));
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    let handle = std::thread::Builder::new()
        .name("mork-engine".into())
        // The kernel's expression machinery (parse/serialize/unify) recurses per nesting
        // level; deeply nested expressions like (S (S … Z)) overflow the 2 MB default.
        // Linux commits stack pages lazily, so a large reservation costs nothing up front.
        .stack_size(512 * 1024 * 1024)
        .spawn(move || run(rx, snap_tx, events, active, cfg, ready_tx))
        .expect("failed to spawn engine thread");
    (tx_send, snap_rx, ready_rx, handle)
}

/// The committer: dispatch queued transactions to idle workers, and install the
/// results they hand back. Owns `Committed` outright — no lock, one owner.
fn run(
    mut rx: mpsc::Receiver<Transaction>,
    snap_tx: watch::Sender<Arc<ReadSnapshot>>,
    events: broadcast::Sender<Event>,
    active: Arc<Mutex<HashSet<TxId>>>,
    cfg: EngineConfig,
    ready: std::sync::mpsc::SyncSender<()>,
) {
    if cfg.data_dir.is_some() {
        log::error!(
            "--data-dir is temporarily unsupported: persistence for concurrent \
             execution is being reworked in a later step of this plan. Run without \
             --data-dir for now."
        );
        std::process::exit(1);
    }
    let wal: Option<&Wal> = None; // persistence disabled pending the WAL/recovery rework

    let space = Space::new(); // built only to mint the initial (empty) symbol table
    let sm = space.sm.clone();
    let mut committed = mvcc::Committed::new(space.btm.clone(), 0); // O(1): Space has Drop, can't move the field out
    let mut bases: HashMap<TxId, PathMap<()>> = HashMap::new();

    publish(&snap_tx, &committed, &sm);
    let _ = ready.send(());

    let (job_tx, job_rx) = std::sync::mpsc::channel::<worker::Job>();
    let (res_tx, res_rx) = std::sync::mpsc::channel::<worker::TxResult>();
    let _workers = worker::spawn_workers(
        cfg.workers,
        sm.clone(),
        Arc::new(Mutex::new(job_rx)),
        res_tx,
        cfg.step_budget,
        cfg.budget_action,
    );

    let mut in_flight: usize = 0;
    let mut finished: u64 = 0;
    loop {
        // Prefer draining finished work: it frees a worker and advances the version.
        match res_rx.try_recv() {
            Ok(r) => {
                commit(&mut committed, &mut bases, r, &sm, &snap_tx, &events, &active, wal);
                in_flight -= 1;
                finished += 1;
                maybe_checkpoint(&committed, finished, &cfg, wal);
                continue;
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
            Err(std::sync::mpsc::TryRecvError::Empty) => {}
        }
        if in_flight < cfg.workers {
            match rx.try_recv() {
                Ok(t) => {
                    dispatch(&mut committed, &mut bases, t, &job_tx, &active);
                    in_flight += 1;
                    continue;
                }
                Err(mpsc::error::TryRecvError::Disconnected) if in_flight == 0 => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {}
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
        }
        if in_flight == 0 {
            let _ = events.send(Event::Idle { version: committed.version });
            match rx.blocking_recv() {
                Some(t) => {
                    dispatch(&mut committed, &mut bases, t, &job_tx, &active);
                    in_flight += 1;
                }
                None => break,
            }
        } else {
            // Workers are busy; block until one reports rather than spinning.
            match res_rx.recv() {
                Ok(r) => {
                    commit(&mut committed, &mut bases, r, &sm, &snap_tx, &events, &active, wal);
                    in_flight -= 1;
                    finished += 1;
                    maybe_checkpoint(&committed, finished, &cfg, wal);
                }
                Err(_) => break,
            }
        }
    }
    log::info!("engine: transaction channel closed, shutting down");
}

/// Hand a queued transaction to a worker, stamped with the version it starts from.
/// Retains a copy of the base trie in `bases`, keyed by transaction id — `commit`
/// needs it to compute the writeset, and base_version alone cannot serve as that key
/// because two transactions may legitimately begin() at the same committed version.
fn dispatch(
    committed: &mut mvcc::Committed,
    bases: &mut HashMap<TxId, PathMap<()>>,
    t: Transaction,
    job_tx: &std::sync::mpsc::Sender<worker::Job>,
    active: &Arc<Mutex<HashSet<TxId>>>,
) {
    let base_version = committed.begin();
    bases.insert(t.id.clone(), committed.btm.clone()); // O(1)
    active.lock().unwrap().insert(t.id.clone());
    let _ = job_tx.send(worker::Job {
        id: t.id,
        base: committed.btm.clone(), // O(1)
        base_version,
        source: t.source,
        reply: t.reply,
    });
}

/// Validate a finished transaction and install it, or abort it. Removes the
/// transaction's retained base from `bases` unconditionally — every path through
/// this function ends the transaction's lifetime, so every path must release it.
fn commit(
    committed: &mut mvcc::Committed,
    bases: &mut HashMap<TxId, PathMap<()>>,
    r: worker::TxResult,
    sm: &SharedMappingHandle,
    snap_tx: &watch::Sender<Arc<ReadSnapshot>>,
    events: &broadcast::Sender<Event>,
    active: &Arc<Mutex<HashSet<TxId>>>,
    wal: Option<&Wal>,
) {
    let worker::TxResult { id, base_version, btm, remove_prefixes, count, steps, outcome, reply } = r;
    let base = bases.remove(&id).expect("dispatch always inserts a base for every id it sends to a worker");
    let _ = wal; // persistence deferred; see the startup refusal in `run`

    if let worker::WorkerOutcome::Failed(reason) = outcome {
        // Nothing to validate: the transaction's trie is discarded either way.
        let _ = events.send(Event::Abort { tx: id.clone(), reason: reason.clone(), version: committed.version });
        let _ = reply.send(Err(reason));
        active.lock().unwrap().remove(&id);
        committed.end(base_version);
        return;
    }

    let ws = mvcc::writeset(&base, &btm);
    if let Err(c) = mvcc::validate(&ws, &remove_prefixes, base_version, &committed.history) {
        let reason = c.reason().to_string();
        let _ = events.send(Event::Abort { tx: id.clone(), reason: reason.clone(), version: committed.version });
        let _ = reply.send(Err(reason));
        active.lock().unwrap().remove(&id);
        committed.end(base_version);
        return;
    }

    let version = committed.install(ws, remove_prefixes);
    publish(snap_tx, committed, sm);
    let _ = events.send(Event::Tx { tx: id.clone(), count, version });
    match outcome {
        worker::WorkerOutcome::Budget => {
            let _ = events.send(Event::Budget { tx: id.clone(), steps, version });
        }
        _ => {
            let _ = events.send(Event::Quiescent { tx: id.clone(), version });
        }
    }
    let _ = reply.send(Ok(TxOk { tx: id.clone(), count, version }));
    active.lock().unwrap().remove(&id);
    committed.end(base_version);
}

/// Every `checkpoint_every` finished transactions, hand the WAL an O(1) COW clone of the
/// trie to persist. Currently inert (`wal` is always `None` in this task) but kept in
/// the shape the persistence rework will reactivate.
fn maybe_checkpoint(committed: &mvcc::Committed, finished: u64, cfg: &EngineConfig, wal: Option<&Wal>) {
    let Some(w) = wal else { return };
    if cfg.checkpoint_every == 0 || !finished.is_multiple_of(cfg.checkpoint_every) {
        return;
    }
    let btm = committed.btm.clone();
    let accepted = w.checkpoint(
        committed.version,
        cfg.tx_counter.load(Ordering::Relaxed),
        "paths",
        Box::new(move |out| {
            pathmap::paths_serialization::serialize_paths(btm.read_zipper(), &mut { out }).map(|_| ())
        }),
    );
    if !accepted {
        log::info!("checkpoint at version {} skipped: previous one still writing", committed.version);
    }
}

/// Snapshots are taken strictly between a worker's finish and the next dispatch — no
/// write-zipper session is ever live here, which is what makes the O(1) `PathMap::clone()`
/// sound.
fn publish(snap_tx: &watch::Sender<Arc<ReadSnapshot>>, committed: &mvcc::Committed, sm: &SharedMappingHandle) {
    snap_tx.send_replace(Arc::new(ReadSnapshot {
        btm: committed.btm.clone(),
        sm: sm.clone(),
        version: committed.version,
    }));
}

/// Budget action `commit`: quiesce by force. Every still-pending `(exec …)` — all of them
/// belong to the running transaction, because a worker's space is fully drained between
/// transactions — is re-rooted as inert `(paused (exec …))` data. Partial progress stays,
/// nothing is left steppable (so a later transaction's stray fallback can't resume it
/// under the wrong atomic scope), and clients can inspect or explicitly resume the parked
/// continuations via /export and a follow-up transaction.
///
/// Text roundtrip on purpose: the kernel dump prints variables as `$a`, `$b`, … and the
/// loader re-reads them by name into identical de Bruijn structure, so remove + re-add is
/// exact. Leftover-exec counts at budget stop are small (the program's frontier).
///
/// Returns whether anything was parked — `false` means the space held no pending execs.
pub fn pause_pending_execs(space: &mut Space) -> Result<bool, String> {
    let mut pat = crate::read::parse_expr_bytes("[4] exec $ $ $", &space.sm)?;
    let mut idt = crate::read::parse_expr_bytes("[4] exec _1 _2 _3", &space.sm)?;
    let mut out = Vec::new();
    Space::dump_sexpr_from(
        &space.btm,
        &space.sm,
        Expr {
            ptr: pat.as_mut_ptr(),
        },
        Expr {
            ptr: idt.as_mut_ptr(),
        },
        &mut out,
    );
    if out.is_empty() {
        return Ok(false);
    }
    let execs = String::from_utf8_lossy(&out).into_owned();
    let paused: String = execs
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| format!("(paused {l})\n"))
        .collect();
    space.remove_all_sexpr(execs.as_bytes())?;
    space.add_all_sexpr(paused.as_bytes())?;
    Ok(true)
}

pub enum StepOutcome {
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
/// what makes a worker's run deterministic given its snapshot).
pub fn step_once(space: &mut Space, txid: &TxId, remove_prefixes: &mut Vec<Vec<u8>>) -> StepOutcome {
    let prefix = wrap::ns_loc_prefix(txid);
    match step_at(space, &prefix, remove_prefixes) {
        Some(outcome) => outcome,
        None => step_at(space, &[], remove_prefixes).unwrap_or(StepOutcome::Done),
    }
}

/// `None` = nothing to step under this prefix. A worker's steps are not committed, so
/// there is nothing to publish and no global version to stamp here — it only
/// accumulates the removal prefixes the committer needs for phantom detection.
fn step_at(
    space: &mut Space,
    loc_prefix: &[u8],
    remove_prefixes: &mut Vec<Vec<u8>>,
) -> Option<StepOutcome> {
    let mut failure: Option<String> = None;
    let sm = space.sm.clone();
    let mut exec_text = String::new();

    let done = space.metta_calculus_scoped(loc_prefix, 1, |info| {
        remove_prefixes.extend_from_slice(info.remove_prefixes);
        let raw = exec_to_text(info.exec, &sm);
        let (exec, _) = wrap::unwrap_text(&raw);
        exec_text = exec;
        if let Some(e) = info.error {
            failure = Some(format!("exec {exec_text}: {e}"));
        }
        true
    });

    if done == 0 { return None; }
    if let Some(reason) = failure { return Some(StepOutcome::Failed(reason)); }
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
    while s.ends_with('\n') {
        s.pop();
    }
    s
}
