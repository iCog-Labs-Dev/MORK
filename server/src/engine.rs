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
use std::fs;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use mork::space::Space;
use mork_expr::Expr;
use mork_interning::SharedMappingHandle;
use pathmap::PathMap;
use tokio::sync::{broadcast, mpsc, watch};

use crate::mvcc;
use crate::transaction::{Event, ReadSnapshot, Transaction, TxId, TxOk};
use crate::wal::{self, CkptMeta, FsyncPolicy, OwnedRec, Rec, Wal};
use crate::{worker, wrap};

/// How long the committer waits on finished work before looping back to look for newly
/// submitted transactions, whenever a worker slot is free. See `run`'s loop for why it
/// exists; 1 ms was measured as invisible against both head-of-line latency (which it
/// bounds) and the committer thread's own CPU use.
const DISPATCH_POLL: std::time::Duration = std::time::Duration::from_millis(1);

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
    /// Persistence root; `None` = pure in-memory (no WAL, no recovery). `Some` triggers
    /// `recover()` at startup and opens the WAL for append thereafter.
    pub data_dir: Option<std::path::PathBuf>,
    /// When the log is fsynced (`--fsync`); also the policy `recover()` reopens the WAL
    /// under.
    pub fsync: FsyncPolicy,
    /// Checkpoint + rotate + GC old segments every N finished transactions; 0 = never
    /// (the log then grows without bound and recovery replays it in full).
    pub checkpoint_every: u64,
    /// Shared with the HTTP layer; recovery restores it before `ready` fires.
    pub tx_counter: Arc<AtomicU64>,
    /// Published to `/stats` after every commit: how many `CommitRecord`s the committer
    /// is still holding for in-flight transactions to validate against. Written only
    /// here, read only by the HTTP layer, `Relaxed` both ways — it is a gauge.
    pub history_len: Arc<AtomicUsize>,
    /// Number of concurrent worker threads. 1 = the previous sequential engine.
    /// Non-zero by construction: a pool of 0 workers would leave `run`'s loop blocked
    /// forever on `res_rx.recv()` with nothing that could ever send to it.
    pub workers: std::num::NonZeroUsize,
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
    let space = Space::new(); // built only to mint the initial (empty) symbol table
    let sm = space.sm.clone();

    // Recovery, if any, must finish before `publish`/`ready` below fire — a request
    // arriving mid-recovery could mint a txid that collides with a replayed one.
    let (wal_owned, mut committed): (Option<Wal>, mvcc::Committed) = match &cfg.data_dir {
        Some(dir) => match recover(dir, &cfg, &sm) {
            Ok((w, c)) => (Some(w), c),
            Err(e) => {
                log::error!("recovery failed, refusing to serve: {e}");
                std::process::exit(1);
            }
        },
        None => (None, mvcc::Committed::new(space.btm.clone(), 0)), // O(1): Space has Drop, can't move the field out
    };
    let wal = wal_owned.as_ref();
    let mut bases: HashMap<TxId, PathMap<()>> = HashMap::new();

    publish(&snap_tx, &committed, &sm);
    let _ = ready.send(());

    let (job_tx, job_rx) = std::sync::mpsc::channel::<worker::Job>();
    let (res_tx, res_rx) = std::sync::mpsc::channel::<worker::TxResult>();
    let _workers = worker::spawn_workers(
        cfg.workers.get(),
        sm.clone(),
        Arc::new(Mutex::new(job_rx)),
        res_tx,
        cfg.step_budget,
        cfg.budget_action,
    );

    let mut in_flight: usize = 0;
    let mut finished: u64 = 0;
    // The next `finished` count a checkpoint is due at. Distinct from "checkpoint every
    // Nth commit" (an exact-multiple test) because a due checkpoint can be deferred
    // while transactions are in flight (see `maybe_checkpoint`) — this must stay due
    // across any number of skipped commits until one actually succeeds, or a
    // permanently busy server would silently never checkpoint again after its first skip.
    let mut next_checkpoint = cfg.checkpoint_every;
    loop {
        // Prefer draining finished work: it frees a worker and advances the version.
        match res_rx.try_recv() {
            Ok(r) => {
                commit(&mut committed, &mut bases, r, &sm, &snap_tx, &events, &active, wal);
                cfg.history_len.store(committed.history.len(), Ordering::Relaxed);
                in_flight -= 1;
                finished += 1;
                maybe_checkpoint(&committed, finished, &cfg, wal, &mut next_checkpoint);
                continue;
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
            Err(std::sync::mpsc::TryRecvError::Empty) => {}
        }
        let has_free_slot = in_flight < cfg.workers.get();
        if has_free_slot {
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
            // Workers are busy, so wait for one to report rather than spinning — but how
            // long to wait depends on whether a result is the only thing that could help.
            // With a slot still free, a request arriving right now could start
            // immediately, and nothing but a result would wake us from `recv()`; time out
            // instead and loop, which re-checks `rx`. With every worker busy, a result
            // genuinely is the only thing that can make progress, so block for it.
            //
            // `DISPATCH_POLL` is therefore the worst-case delay before an arriving request
            // is dispatched. It costs two `try_recv`s per tick and only ticks while the
            // pool is partially loaded. Upgrade path if it ever shows up in a profile:
            // make the result channel carry an enum and have the HTTP handler push a
            // `Submitted` wake onto it, so one `recv()` serves both and nothing polls.
            let r = if has_free_slot {
                match res_rx.recv_timeout(DISPATCH_POLL) {
                    Ok(r) => r,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
            } else {
                match res_rx.recv() {
                    Ok(r) => r,
                    Err(_) => break,
                }
            };
            commit(&mut committed, &mut bases, r, &sm, &snap_tx, &events, &active, wal);
            cfg.history_len.store(committed.history.len(), Ordering::Relaxed);
            in_flight -= 1;
            finished += 1;
            maybe_checkpoint(&committed, finished, &cfg, wal, &mut next_checkpoint);
        }
    }
    log::info!("engine: transaction channel closed, shutting down");
}

/// The numeric prefix of a `tx<n>_…` id — what the shared counter must clear after
/// recovery so freshly minted ids can never collide with replayed ones.
fn txid_count(id: &str) -> Option<u64> {
    id.strip_prefix("tx")?.split_once('_')?.0.parse().ok()
}

/// Rebuild `Committed` from `data_dir` — checkpoint restore plus log replay — and open
/// the WAL for append.
///
/// Each logged transaction records the version it ran against, so replay reconstructs
/// that exact snapshot from a running table of committed versions and re-runs the
/// transaction against it, rather than against whatever the serial predecessor
/// happened to leave behind — required because concurrent workers can commit in an
/// order that differs from the order they began, so "the previous committed state"
/// is not necessarily a transaction's actual base. Installing the results back in log
/// order reproduces the live serialization order exactly, regardless of how many
/// workers produced it.
///
/// Validation is deliberately skipped: every logged transaction already passed it
/// live, and replaying from the same bases in the same order yields the same
/// writesets and the same installs, so re-checking would be pure cost. What replay
/// does check is determinism itself (see the step-count and version comparisons
/// below) — if either disagrees with the log, the recovered state provably is not
/// the state that was committed, and this refuses to start rather than serve it.
fn recover(dir: &Path, cfg: &EngineConfig, sm: &SharedMappingHandle) -> io::Result<(Wal, mvcc::Committed)> {
    fs::create_dir_all(dir)?;
    let mut first_segment = 0u64;
    let mut max_tx = 0u64;
    let mut btm: PathMap<()> = PathMap::new();
    let mut version = 0u64;

    if let Some(m) = CkptMeta::load(dir)? {
        if m.format != "paths" {
            return Err(io::Error::other(format!("unsupported checkpoint format '{}'", m.format)));
        }
        let snap_path = dir.join(&m.snapshot);
        // `Space::restore_paths` does `File::open(path).unwrap()` (kernel/src/space.rs),
        // so it PANICS rather than returning `Err` on a permissions error or similar —
        // out of scope to fix here. This check only rules out the missing-file case;
        // it is a TOCTOU mitigation, not a guarantee.
        if !snap_path.exists() {
            return Err(io::Error::other(format!(
                "checkpoint.meta names missing snapshot '{}'", m.snapshot)));
        }
        let mut tmp = Space::with(PathMap::new(), sm.clone());
        tmp.restore_paths(&snap_path)?;
        btm = tmp.btm.clone(); // O(1): Space has Drop, can't move the field out
        version = m.version;
        max_tx = m.tx_counter;
        first_segment = m.first_segment;
        log::info!("recovered checkpoint '{}' at version {}", m.snapshot, m.version);
    }

    let mut committed = mvcc::Committed::new(btm, version);
    // Every base a not-yet-replayed record still refers to, keyed by committed version;
    // each entry is an O(1) COW clone. Under a nonzero `--checkpoint-every` this is small
    // (one entry per version since the last checkpoint), but `--checkpoint-every 0` is a
    // documented, supported setting under which the log is never truncated — in that case
    // this map holds one clone per version in the ENTIRE log. No pruning: out of scope.
    let mut snapshots: HashMap<u64, PathMap<()>> = HashMap::new();
    snapshots.insert(committed.version, committed.btm.clone());

    let recs = wal::read_segments(dir, first_segment)?;
    let n_recs = recs.len();
    for rec in recs {
        let OwnedRec::Commit { id, base_version, source, steps, version: logged } = rec;
        // An unparsable id can only come from a foreign or corrupt log; treating it as 0
        // (rather than erroring) would restore the tx counter too low, i.e. the exact
        // txid collision `txid_count` exists to prevent — so this is a hard error too.
        max_tx = max_tx.max(txid_count(&id).ok_or_else(|| {
            io::Error::other(format!("replay of {id}: not a valid tx<n>_… id"))
        })?);

        let base = snapshots.get(&base_version).cloned().ok_or_else(|| {
            io::Error::other(format!(
                "replay of {id}: base version {base_version} is not in the log window"))
        })?;

        // The live budget, not the logged step count: run_one's loop checks the budget
        // BEFORE stepping, so replaying with budget == logged N would fire the budget
        // action at step N instead of reaching Done — the transaction would never
        // reach the state it actually quiesced (or budgeted) to. Using the live budget
        // lets replay run exactly as the original run did; the logged `steps` becomes
        // a pure determinism canary, checked below instead of fed back in.
        let parts = worker::run_one(id.clone(), base.clone(), source, sm.clone(), cfg.step_budget, cfg.budget_action);

        if let worker::WorkerOutcome::Failed(e) = parts.outcome {
            return Err(io::Error::other(format!(
                "replay of {id} failed (determinism broken?): {e}. If you changed \
                 --budget-action since this log was written, that is a likely cause: a \
                 transaction that originally committed by parking under `commit` can fail \
                 outright when replayed under `abort`.",
            )));
        }
        // WorkerOutcome::Budget is a legitimate replay outcome only if the original run
        // also stopped at the budget, which the step-count check right below already
        // establishes — no separate check needed here.
        if parts.steps != steps {
            return Err(io::Error::other(format!(
                "replay of {id} diverged: ran {} steps, log says {steps}. \
                 The recovered state would not match what was committed, so the server \
                 will not start. If you changed --step-budget since this log was written, \
                 that is the likely cause: a transaction that originally stopped at the \
                 budget will step differently under a new one.",
                parts.steps
            )));
        }

        let ws = mvcc::writeset(&base, &parts.btm);
        let v = committed.install(ws, parts.remove_prefixes);
        if v != logged {
            return Err(io::Error::other(format!(
                "replay of {id} diverged: installed at version {v}, log says {logged}. \
                 The recovered state would not match what was committed, so the server \
                 will not start. If you changed --step-budget since this log was written, \
                 that is the likely cause: a transaction that originally stopped at the \
                 budget will step differently under a new one."
            )));
        }
        snapshots.insert(v, committed.btm.clone());
    }

    cfg.tx_counter.store(max_tx, Ordering::Relaxed);
    // Nothing can validate against replayed history: no transaction is live to need it.
    committed.history.clear();
    let w = Wal::open(dir, cfg.fsync)?;
    log::info!(
        "recovery complete: {n_recs} transactions replayed, version {}, tx counter {max_tx}",
        committed.version
    );
    Ok((w, committed))
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
    let clobbered = bases.insert(t.id.clone(), committed.btm.clone()); // O(1)
    debug_assert!(clobbered.is_none(), "duplicate TxId {}: would hand commit the wrong base", t.id);
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
    let worker::TxResult { id, base_version, source, btm, remove_prefixes, count, steps, outcome, reply } = r;
    let base = bases.remove(&id).expect("dispatch always inserts a base for every id it sends to a worker");

    if let worker::WorkerOutcome::Failed(reason) = outcome {
        // Nothing to validate: the transaction's trie is discarded either way.
        let _ = events.send(Event::Abort { tx: id.clone(), reason: reason.clone(), version: committed.version });
        let _ = reply.send(Err(reason));
        active.lock().unwrap().remove(&id);
        committed.end(base_version);
        return;
    }

    // A poisoned log cannot record what we are about to commit, so installing would
    // acknowledge work that recovery will never replay. Refuse instead: "unavailable:"
    // gets the client a 503 telling it to retry rather than a 200 it cannot trust.
    // Terminal by design — nothing clears the flag, so an operator has to fix the disk
    // and restart.
    //
    // How much this leaves behind depends on the fsync policy. The flag is only set
    // *after* a write fails, and `append` only queues, so by the time we see it the space
    // is already ahead of the last durable version by whatever sat in the 4096-deep queue
    // unwritten. Under `always` those transactions were never acked — their replies were
    // handed to the writer below and it answers them with the failure — so nothing
    // acknowledged is missing on restart. Under `everysec`/`no` the committer answered
    // them itself, so they were acked 200 and are lost: a one-shot window bounded by the
    // queue depth, widest exactly when the device hangs and the committer runs ahead
    // until backpressure stops it.
    if wal.is_some_and(Wal::poisoned) {
        let reason = "unavailable: the write-ahead log is poisoned by an earlier disk error; \
                      the server cannot durably record new transactions until it is restarted"
            .to_string();
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
    let ok = TxOk { tx: id.clone(), count, version };
    // Under `--fsync always` the 200 is supposed to mean "on disk", so the reply travels
    // with the record and the writer thread fires it after the batch fsync. The committer
    // still never touches a disk: `append` only queues, and redo logging needs
    // durable-before-ACK, not durable-before-apply. Under the other policies — which
    // promise no such thing — waiting would buy nothing, so we answer here as before.
    // A poisoned or disconnected log answers the handed-over reply with an `Err` rather
    // than dropping it, so the client is never left hanging either way.
    let deferred_reply = match wal {
        Some(w) if w.acks_mean_durable() => {
            w.append(
                Rec::Commit { id: &id, base_version, source: &source, steps, version },
                Some(wal::Ack { reply, ok }),
            );
            None
        }
        w => {
            if let Some(w) = w {
                w.append(Rec::Commit { id: &id, base_version, source: &source, steps, version }, None);
            }
            Some((reply, ok))
        }
    };
    publish(snap_tx, committed, sm);
    // Stream events, not the client's 200: they report what the committer did and are not
    // gated on durability, so they still fire here under every policy.
    let _ = events.send(Event::Tx { tx: id.clone(), count, version });
    match outcome {
        worker::WorkerOutcome::Budget => {
            let _ = events.send(Event::Budget { tx: id.clone(), steps, version });
        }
        _ => {
            let _ = events.send(Event::Quiescent { tx: id.clone(), version });
        }
    }
    if let Some((reply, ok)) = deferred_reply {
        let _ = reply.send(Ok(ok));
    }
    active.lock().unwrap().remove(&id);
    committed.end(base_version);
}

/// Every `checkpoint_every` finished transactions, hand the WAL an O(1) COW clone of the
/// trie to persist.
///
/// Gated on nothing being in flight (`committed.active_bases.is_empty()`): a still-running
/// transaction was dispatched at some earlier version, so its eventual commit record will
/// carry a `base_version` older than this checkpoint. `Wal::checkpoint`'s rotation GCs
/// every segment before the fresh one, which is exactly where that record's base would
/// have lived — so checkpointing out from under an in-flight transaction deletes the log
/// window `recover` needs for it, and recovery then fails with "base version ... is not in
/// the log window" forever (the data directory is bricked, not transiently unavailable).
/// With `active_bases` empty, every record already on disk has `base_version <=
/// committed.version`, and this checkpoint's trie already reflects all of those — so GC'ing
/// their segments is safe.
///
/// `*next_checkpoint` is a due-flag, not an exact-multiple test: a checkpoint deferred by
/// the in-flight gate (or by one already writing) must stay due and be retried on every
/// subsequent commit, advancing only once one actually succeeds — otherwise a server that's
/// rarely idle would skip its schedule and never retry until a full `checkpoint_every`
/// later, growing the log without bound under sustained load.
fn maybe_checkpoint(
    committed: &mvcc::Committed,
    finished: u64,
    cfg: &EngineConfig,
    wal: Option<&Wal>,
    next_checkpoint: &mut u64,
) {
    let Some(w) = wal else { return };
    if cfg.checkpoint_every == 0 || finished < *next_checkpoint {
        return;
    }
    if !committed.active_bases.is_empty() {
        log::info!(
            "checkpoint at version {} deferred: a transaction dispatched at an earlier \
             version is still in flight; will retry on the next commit",
            committed.version
        );
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
    if accepted {
        *next_checkpoint = finished + cfg.checkpoint_every;
    } else {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(name: &str) -> std::path::PathBuf {
        static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let d = std::env::temp_dir().join(format!(
            "mork-engine-test-{name}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn test_cfg(checkpoint_every: u64) -> EngineConfig {
        EngineConfig {
            step_budget: 1_000_000,
            budget_action: BudgetAction::Commit,
            data_dir: None,
            fsync: FsyncPolicy::Always,
            checkpoint_every,
            tx_counter: Arc::new(AtomicU64::new(0)),
            history_len: Arc::new(AtomicUsize::new(0)),
            workers: std::num::NonZeroUsize::new(1).unwrap(),
        }
    }

    /// Regression for the checkpoint/recovery interaction found in review: a checkpoint
    /// that fires while an older-based transaction is still in flight would GC the exact
    /// log segment `recover` needs to resolve that transaction's eventual commit record,
    /// bricking the data directory permanently. This calls `maybe_checkpoint` directly —
    /// deterministic and instant, unlike trying to force two real HTTP-dispatched
    /// transactions to genuinely overlap under the committer's polling loop, which is a
    /// timing race this test suite already documents elsewhere as unforceable from
    /// outside the process (see `test_concurrent_conflict_one_transaction_aborts` in
    /// `test_e2e.py`).
    #[test]
    fn checkpoint_is_deferred_while_a_transaction_is_in_flight_and_proceeds_once_it_ends() {
        let dir = tmpdir("defer");
        let cfg = test_cfg(1);
        let mut committed = mvcc::Committed::new(PathMap::new(), 0);
        let base = committed.begin(); // simulates a transaction dispatched and still running
        let mut next_checkpoint = 1u64;

        {
            let wal = Wal::open(&dir, FsyncPolicy::Always).unwrap();

            maybe_checkpoint(&committed, 1, &cfg, Some(&wal), &mut next_checkpoint);
            assert!(
                CkptMeta::load(&dir).unwrap().is_none(),
                "a checkpoint must not install while a transaction is in flight"
            );
            assert_eq!(next_checkpoint, 1, "a deferred checkpoint must stay due, not advance");

            committed.end(base); // the in-flight transaction finishes; nothing left running
            maybe_checkpoint(&committed, 1, &cfg, Some(&wal), &mut next_checkpoint);
            assert_eq!(
                next_checkpoint,
                1 + cfg.checkpoint_every,
                "the due-flag advances once a checkpoint is actually accepted"
            );
            wal.shutdown(); // drains + joins: the async install is complete once this returns
        }
        assert!(
            CkptMeta::load(&dir).unwrap().is_some(),
            "the checkpoint must proceed once nothing is in flight"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    /// Regression for silent data loss: once a disk error poisons the WAL, every `append`
    /// is dropped on the floor, so a transaction that installs anyway is acknowledged to
    /// the client and then gone at the next restart — the space drifts ahead of the log
    /// and recovery, seeing a well-formed but short log, never complains. `commit` must
    /// refuse instead: version unchanged, an `Abort` event, and a reply carrying the
    /// `unavailable:` prefix `http.rs` turns into a 503 (retry) rather than a 422.
    ///
    /// The healthy half of the loop is the honest negative — a gate that refused every
    /// commit would satisfy the poisoned half on its own.
    #[test]
    fn commit_refuses_a_poisoned_wal_and_still_installs_on_a_healthy_one() {
        for poisoned in [true, false] {
            let dir = tmpdir("poisoned");
            let wal = Wal::open(&dir, FsyncPolicy::Always).unwrap();
            if poisoned {
                wal.poison_for_test();
            }

            let sm = mork_interning::SharedMapping::new();
            let mut committed = mvcc::Committed::new(PathMap::new(), 7);
            let base_version = committed.begin();
            let base = committed.btm.clone();
            let mut btm = base.clone();
            btm.insert(b"\x01a", ()); // one added path, so the writeset is not empty

            let id: TxId = "tx1_poison".to_string();
            let mut bases = HashMap::new();
            bases.insert(id.clone(), base);
            let active: Arc<Mutex<HashSet<TxId>>> = Arc::new(Mutex::new(HashSet::new()));
            active.lock().unwrap().insert(id.clone());
            let (snap_tx, _snap_rx) = watch::channel(Arc::new(ReadSnapshot::empty()));
            let (events, mut ev_rx) = broadcast::channel(8);
            let (reply, reply_rx) = tokio::sync::oneshot::channel();

            commit(
                &mut committed,
                &mut bases,
                worker::TxResult {
                    id: id.clone(),
                    base_version,
                    source: "(a)".into(),
                    btm,
                    remove_prefixes: Vec::new(),
                    count: 1,
                    steps: 1,
                    outcome: worker::WorkerOutcome::Quiesced,
                    reply,
                },
                &sm,
                &snap_tx,
                &events,
                &active,
                Some(&wal),
            );

            // `blocking_recv`, not `try_recv`: on the healthy half the reply now travels
            // with the record and is fired by the writer thread after its fsync.
            let got = reply_rx.blocking_recv().expect("commit always answers the client");
            if poisoned {
                assert_eq!(committed.version, 7, "a poisoned log must not install anything");
                let Err(err) = got else {
                    panic!("a poisoned log must not be acknowledged as committed");
                };
                assert!(err.starts_with("unavailable:"), "must map to a 503, got: {err}");
                match ev_rx.try_recv() {
                    Ok(Event::Abort { tx, reason, version }) => {
                        assert_eq!(tx, id);
                        assert_eq!(reason, err);
                        assert_eq!(version, 7);
                    }
                    other => panic!("expected an Abort event, got {other:?}"),
                }
            } else {
                assert_eq!(committed.version, 8, "a healthy log must still install");
                assert!(got.is_ok(), "a healthy log must still commit");
            }
            assert!(bases.is_empty(), "the retained base must be released on every path");
            assert!(active.lock().unwrap().is_empty(), "the tx must leave `active` on every path");
            assert!(
                committed.active_bases.is_empty(),
                "every path must end the transaction's lifetime, or the GC watermark stalls"
            );
            drop(wal);
            fs::remove_dir_all(&dir).unwrap();
        }
    }

    /// `--fsync always` exists so an operator can make POST /run's 200 mean "on disk".
    /// That only holds if the client's reply is fired by the WAL writer *after* its
    /// fsync; if the committer answers as soon as the record is queued, the 200 promises
    /// a durability the log has not yet given, and a crash between the two loses an
    /// acknowledged transaction.
    ///
    /// The `everysec` half is the honest negative: that policy does not promise
    /// durable-before-ACK, so the committer must keep replying directly there — a change
    /// that simply waited on every fsync would satisfy the `always` half on its own while
    /// serializing every commit behind the disk.
    ///
    /// Both halves ask "who sent the reply", which is a race unless the writer is
    /// provably still busy when we look. Hence the ballast record queued first: several
    /// megabytes take the writer milliseconds to write, against the microseconds this
    /// thread needs to return from `commit` and look — so a reply that is already waiting
    /// came from the committer, and one that is not came from the writer.
    #[test]
    fn only_always_hands_the_client_reply_to_the_wal_to_fire_after_its_fsync() {
        for policy in [FsyncPolicy::Always, FsyncPolicy::Everysec] {
            let dir = tmpdir("fsync-ack");
            let wal = Wal::open(&dir, policy).unwrap();
            let ballast = "b".repeat(8 << 20);
            wal.append(
                Rec::Commit { id: "tx0_ballast", base_version: 0, source: &ballast, steps: 0, version: 7 },
                None,
            );

            let sm = mork_interning::SharedMapping::new();
            let mut committed = mvcc::Committed::new(PathMap::new(), 7);
            let base_version = committed.begin();
            let base = committed.btm.clone();
            let mut btm = base.clone();
            btm.insert(b"\x01a", ()); // one added path, so the writeset is not empty

            let id: TxId = "tx1_fsyncack".to_string();
            let mut bases = HashMap::new();
            bases.insert(id.clone(), base);
            let active: Arc<Mutex<HashSet<TxId>>> = Arc::new(Mutex::new(HashSet::new()));
            active.lock().unwrap().insert(id.clone());
            let (snap_tx, _snap_rx) = watch::channel(Arc::new(ReadSnapshot::empty()));
            let (events, _ev_rx) = broadcast::channel(8);
            let (reply, mut reply_rx) = tokio::sync::oneshot::channel();

            commit(
                &mut committed,
                &mut bases,
                worker::TxResult {
                    id: id.clone(),
                    base_version,
                    source: "(a)".into(),
                    btm,
                    remove_prefixes: Vec::new(),
                    count: 1,
                    steps: 1,
                    outcome: worker::WorkerOutcome::Quiesced,
                    reply,
                },
                &sm,
                &snap_tx,
                &events,
                &active,
                Some(&wal),
            );

            assert_eq!(committed.version, 8, "the transaction installs under either policy");
            let pending = reply_rx.try_recv();
            let ok = match policy {
                FsyncPolicy::Always => {
                    assert!(
                        matches!(pending, Err(tokio::sync::oneshot::error::TryRecvError::Empty)),
                        "under `always` the committer must hand the reply to the wal, not send it \
                         itself while the record is still queued behind an unwritten batch"
                    );
                    let ok = reply_rx
                        .blocking_recv()
                        .expect("the wal must fire the ack it was handed")
                        .expect("a healthy log must commit");
                    // The ack fires only after the batch fsync, so by now the record —
                    // and everything queued ahead of it — is readable back off the disk.
                    assert_eq!(
                        wal::read_segments(&dir, 0).unwrap().len(),
                        2,
                        "under `always` the 200 must not arrive before the record is durable"
                    );
                    ok
                }
                _ => pending
                    .expect("under `everysec` the committer must reply directly, not wait on the disk")
                    .expect("a healthy log must commit"),
            };
            assert_eq!(ok.tx, id);
            assert_eq!(ok.count, 1);
            assert_eq!(ok.version, 8);

            assert!(bases.is_empty(), "the retained base must be released on every path");
            assert!(active.lock().unwrap().is_empty(), "the tx must leave `active` on every path");
            assert!(
                committed.active_bases.is_empty(),
                "handing the reply to the wal must still end the transaction's lifetime"
            );
            drop(wal);
            fs::remove_dir_all(&dir).unwrap();
        }
    }

    /// Without the due-flag, a deferred checkpoint would only be retried on the next
    /// exact multiple of `checkpoint_every` — under sustained load (where something is
    /// almost always in flight) that means skipping potentially forever. `next_checkpoint`
    /// must instead stay due across any number of "still in flight" and "previous
    /// checkpoint still writing" skips, and only move once one truly succeeds.
    #[test]
    fn deferred_checkpoint_stays_due_across_multiple_skips() {
        let dir = tmpdir("stays-due");
        let cfg = test_cfg(10);
        let mut committed = mvcc::Committed::new(PathMap::new(), 0);
        let base = committed.begin();
        let mut next_checkpoint = 10u64;

        {
            let wal = Wal::open(&dir, FsyncPolicy::Always).unwrap();
            // "Due" fires at finished == 10, and stays due through finished == 15 despite
            // three separate deferrals — never silently rearmed to wait for 20.
            for finished in [10, 12, 15] {
                maybe_checkpoint(&committed, finished, &cfg, Some(&wal), &mut next_checkpoint);
                assert_eq!(next_checkpoint, 10, "still due: the transaction never ended");
            }
            committed.end(base);
            maybe_checkpoint(&committed, 15, &cfg, Some(&wal), &mut next_checkpoint);
            assert_eq!(next_checkpoint, 15 + cfg.checkpoint_every);
            wal.shutdown();
        }
        assert!(CkptMeta::load(&dir).unwrap().is_some());
        fs::remove_dir_all(&dir).unwrap();
    }
}
