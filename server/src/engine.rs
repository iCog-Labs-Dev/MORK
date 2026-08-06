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
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{fs, io};

use mork::space::Space;
use mork_expr::Expr;
use mork_interning::SharedMappingHandle;
use pathmap::PathMap;
use tokio::sync::{broadcast, mpsc, watch};

use crate::transaction::{EngineCmd, EngineError, Event, ReadSnapshot, Transaction, TxId, TxOk};
use crate::wal::{CkptMeta, FsyncPolicy, OwnedRec, Rec, Wal};
use crate::{wal, wrap};

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
    /// Source/sink sweep passes per cooperative scheduler cycle.
    pub sweep_steps_per_cycle: usize,
    /// Whole-space metta-calculus steps after each weighted sweep batch.
    pub sweep_metta_steps: usize,
    /// Backoff when an active source/sink sweep cycle makes no observable progress.
    pub sweep_idle_ms: u64,
    /// Persistence root; `None` = pure in-memory (no WAL, no recovery).
    pub data_dir: Option<std::path::PathBuf>,
    pub fsync: FsyncPolicy,
    /// Checkpoint + rotate + GC old segments every N finished transactions; 0 = never
    /// (the log then grows without bound and recovery replays it in full).
    pub checkpoint_every: u64,
    /// Shared with the HTTP layer; recovery restores it before `ready` fires.
    pub tx_counter: Arc<AtomicU64>,
}

#[derive(Default)]
struct SweepSchedulerState {
    running: bool,
    paused: bool,
    idle_version: Option<u64>,
}

impl SweepSchedulerState {
    fn active(&self) -> bool {
        self.running && !self.paused
    }

    fn start(&mut self) {
        self.running = true;
        self.paused = false;
        self.idle_version = None;
    }

    fn pause(&mut self) {
        self.paused = true;
        self.idle_version = None;
    }

    fn resume(&mut self) {
        self.paused = false;
        self.idle_version = None;
    }

    fn stop(&mut self) {
        self.running = false;
        self.paused = false;
        self.idle_version = None;
    }
}

/// Returned alongside the channels: fires once recovery is done and the snapshot is
/// published. `main` must not bind the listener before this — a request arriving
/// mid-recovery could mint a txid that collides with a replayed one.
pub type ReadySignal = std::sync::mpsc::Receiver<()>;

pub fn spawn_engine(
    events: broadcast::Sender<Event>,
    active: Arc<Mutex<HashSet<TxId>>>,
    cfg: EngineConfig,
) -> (
    mpsc::Sender<EngineCmd>,
    watch::Receiver<Arc<ReadSnapshot>>,
    ReadySignal,
    std::thread::JoinHandle<()>,
) {
    let (tx_send, rx) = mpsc::channel::<EngineCmd>(1024);
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

fn run(
    mut rx: mpsc::Receiver<EngineCmd>,
    snap_tx: watch::Sender<Arc<ReadSnapshot>>,
    events: broadcast::Sender<Event>,
    active: Arc<Mutex<HashSet<TxId>>>,
    cfg: EngineConfig,
    ready: std::sync::mpsc::SyncSender<()>,
) {
    let mut space = Space::new(); // created HERE: Space is !Send
    let mut version: u64 = 0;

    let wal: Option<Wal> = cfg.data_dir.clone().map(|dir| {
        match recover(
            &mut space,
            &mut version,
            &dir,
            &cfg,
            &snap_tx,
            &events,
            &active,
        ) {
            Ok(w) => w,
            Err(e) => {
                // Refusing to serve beats silently serving a wrong or partial space.
                log::error!("recovery failed, refusing to serve: {e}");
                std::process::exit(1);
            }
        }
    });
    let wal = wal.as_ref();

    publish(&snap_tx, &space, version);
    let _ = ready.send(());

    let mut finished: u64 = 0; // transactions run to an outcome, for the checkpoint trigger
    let mut scheduler = SweepSchedulerState::default();
    loop {
        // Drain queued submissions before parking; `Idle` is only truthful when both the
        // space and the queue are empty.
        match rx.try_recv() {
            Ok(cmd) => {
                scheduler.idle_version = None;
                dispatch_cmd(cmd, &mut space, &mut version, &snap_tx, &events, &active, &cfg, wal, &mut finished, &mut scheduler);
                continue;
            }
            Err(mpsc::error::TryRecvError::Disconnected) => break,
            Err(mpsc::error::TryRecvError::Empty) => {}
        }
        if scheduler.active() {
            match run_sweep_scheduler_tick(&mut space, &mut version, &snap_tx, &events, &cfg) {
                Ok(true) => scheduler.idle_version = None,
                Ok(false) => {
                    if scheduler.idle_version != Some(version) {
                        let _ = events.send(Event::Idle { version });
                        scheduler.idle_version = Some(version);
                    }
                    sleep_scheduler_idle(cfg.sweep_idle_ms);
                }
                Err(e) => {
                    log::error!("sweep scheduler stopped: {e}");
                    scheduler.stop();
                }
            }
            continue;
        }
        let _ = events.send(Event::Idle { version });
        match rx.blocking_recv() {
            Some(cmd) => {
                scheduler.idle_version = None;
                dispatch_cmd(cmd, &mut space, &mut version, &snap_tx, &events, &active, &cfg, wal, &mut finished, &mut scheduler);
            }
            None => break,
        }
    }
    log::info!("engine: transaction channel closed, shutting down");
    // Dropping the Wal (owner is still in scope) drains its queue and final-fsyncs.
}

/// Dispatch one command from the engine channel.
fn dispatch_cmd(
    cmd: EngineCmd,
    space: &mut Space,
    version: &mut u64,
    snap_tx: &watch::Sender<Arc<ReadSnapshot>>,
    events: &broadcast::Sender<Event>,
    active: &Arc<Mutex<HashSet<TxId>>>,
    cfg: &EngineConfig,
    wal: Option<&Wal>,
    finished: &mut u64,
    scheduler: &mut SweepSchedulerState,
) {
    match cmd {
        EngineCmd::Tx(t) => {
            run_tx(space, t, version, snap_tx, events, active, cfg, wal);
            *finished += 1;
            maybe_checkpoint(space, *version, *finished, cfg, wal);
        }
        EngineCmd::SweepStart { reply } => {
            let handle_name = space.sweep();
            let source_sink_sweeps = has_source_sink_sweeps(space);
            let legacy_sweeps = !space.was.controllers.is_empty();
            if handle_name.is_empty() && !source_sink_sweeps && !legacy_sweeps {
                let _ = reply.send(Err("No (sweep ...) configuration found in space".into()));
            } else {
                if source_sink_sweeps {
                    scheduler.start();
                }
                if !handle_name.is_empty() && space.was.map.is_none() {
                    *version += 1;
                    publish(snap_tx, space, *version);
                }
                let handle = if source_sink_sweeps
                    && (handle_name.is_empty() || handle_name == "sweep-config")
                {
                    "sweep-scheduler".to_string()
                } else if handle_name.is_empty() && legacy_sweeps {
                    "sweep-running".to_string()
                } else {
                    handle_name
                };
                let _ = reply.send(Ok(handle));
            }
        }
        EngineCmd::SweepPause { reply } => {
            let mut handled = false;
            if !space.was.controllers.is_empty() {
                handled = true;
                if space.was.map.is_some() {
                    space.btm = space.was.pause_all();
                    *version += 1;
                    publish(snap_tx, space, *version);
                }
            }
            if scheduler.running {
                handled = true;
                scheduler.pause();
            }
            if handled {
                let _ = reply.send(Ok(()));
            } else {
                let _ = reply.send(Err("No active sweep controllers to pause".into()));
            }
        }
        EngineCmd::SweepResume { reply } => {
            let mut handled = false;
            if !space.was.controllers.is_empty() {
                handled = true;
                if space.was.map.is_none() {
                    let btm = std::mem::take(&mut space.btm);
                    space.was.resume_all(btm);
                }
            }
            if scheduler.running {
                handled = true;
                scheduler.resume();
            }
            if handled {
                let _ = reply.send(Ok(()));
            } else {
                let _ = reply.send(Err("No sweep controllers to resume".into()));
            }
        }
        EngineCmd::SweepStop { reply } => {
            let mut handled = false;
            let mut old_was_changed = false;
            if !space.was.controllers.is_empty() {
                handled = true;
                if space.was.map.is_some() {
                    space.btm = space.was.pause_all();
                    old_was_changed = true;
                }
                if let Some(btm) = space.was.shutdown_all() {
                    space.btm = btm;
                    old_was_changed = true;
                }
            }
            if scheduler.running {
                handled = true;
                scheduler.stop();
            }
            if handled {
                if old_was_changed {
                    *version += 1;
                    publish(snap_tx, space, *version);
                }
                let _ = reply.send(Ok(()));
            } else {
                let _ = reply.send(Err("No sweep controllers running".into()));
            }
        }
    }
}

fn has_source_sink_sweeps(space: &Space) -> bool {
    space.sweep_specs.values().any(|spec| spec.rule.is_some())
}

fn sleep_scheduler_idle(ms: u64) {
    if ms == 0 {
        std::thread::yield_now();
    } else {
        std::thread::sleep(Duration::from_millis(ms));
    }
}

fn run_sweep_scheduler_tick(
    space: &mut Space,
    version: &mut u64,
    snap_tx: &watch::Sender<Arc<ReadSnapshot>>,
    events: &broadcast::Sender<Event>,
    cfg: &EngineConfig,
) -> Result<bool, String> {
    let was_running = space.was.map.is_some();
    if was_running {
        space.btm = space.was.pause_all();
    }

    let result = run_sweep_scheduler_tick_foreground(space, version, snap_tx, events, cfg);

    if was_running {
        let btm = std::mem::take(&mut space.btm);
        space.was.resume_all(btm);
    }

    result
}

fn run_sweep_scheduler_tick_foreground(
    space: &mut Space,
    version: &mut u64,
    snap_tx: &watch::Sender<Arc<ReadSnapshot>>,
    events: &broadcast::Sender<Event>,
    cfg: &EngineConfig,
) -> Result<bool, String> {
    let mut progressed = false;

    if cfg.sweep_steps_per_cycle > 0 {
        let (_touched, changed) = space
            .run_sweep_cycles(cfg.sweep_steps_per_cycle)
            .map_err(|e| e.to_string())?;
        if changed {
            *version += 1;
            publish(snap_tx, space, *version);
            progressed = true;
        }
    }

    if cfg.sweep_metta_steps == 0 {
        return Ok(progressed);
    }

    let undo = space.btm.clone();
    let tx = TxId::new("sweep".to_string()).expect("sweep txid");
    let mut stepped = false;
    for _ in 0..cfg.sweep_metta_steps {
        match step_at(space, &[], Some(&tx), version, snap_tx, events) {
            Some(StepOutcome::Stepped) => {
                progressed = true;
                stepped = true;
            }
            Some(StepOutcome::Done) | None => {
                if stepped {
                    let _ = events.send(Event::Quiescent {
                        tx: tx.clone(),
                        version: *version,
                    });
                }
                break;
            }
            Some(StepOutcome::Failed(reason)) => {
                space.btm = undo;
                *version += 1;
                publish(snap_tx, space, *version);
                let _ = events.send(Event::Abort {
                    tx: tx.clone(),
                    reason: reason.clone(),
                    version: *version,
                });
                return Err(reason);
            }
        }
    }

    Ok(progressed)
}

/// Every `checkpoint_every` finished transactions, hand the WAL an O(1) COW clone of the
/// trie to persist (the engine's whole cost is the clone; serialization runs on the
/// checkpointer thread). Taken at a tx boundary, the clone IS the consistent image —
/// same soundness argument as `publish`. A skip (previous checkpoint still writing) is
/// fine: the trigger fires again `checkpoint_every` transactions later.
fn maybe_checkpoint(space: &Space, version: u64, finished: u64, cfg: &EngineConfig, wal: Option<&Wal>) {
    let Some(w) = wal else { return };
    if cfg.checkpoint_every == 0 || !finished.is_multiple_of(cfg.checkpoint_every) {
        return;
    }
    let btm = space.btm.clone();
    let accepted = w.checkpoint(
        version,
        cfg.tx_counter.load(Ordering::Relaxed),
        "paths",
        Box::new(move |out| {
            pathmap::paths_serialization::serialize_paths(btm.read_zipper(), &mut { out }).map(|_| ())
        }),
    );
    if !accepted {
        log::info!("checkpoint at version {version} skipped: previous one still writing");
    }
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

/// Run one transaction start to finish: load atomically, log + ack, then step to
/// quiescence via [`finish_tx`]. The COW clone taken up front (same soundness argument
/// as `publish`) makes the transaction atomic — a partial load or a failing exec rolls
/// back to it.
///
/// WAL ordering: the TX record is appended AFTER the in-memory apply (we only know
/// `TxOk` then), which is sound because redo logging requires durable-before-ACK, not
/// durable-before-apply — a crash in between loses only an unacknowledged transaction.
/// A failed load appends nothing at all: no TX record, no outcome needed.
///
/// **Submission/decoupling**: the ack fires immediately after the in-memory load
/// succeeds. The HTTP handler returns 200 as soon as the ack arrives. WAL append and
/// stepping continue asynchronously — progress streams on `/events`.
fn run_tx(
    space: &mut Space,
    t: Transaction,
    version: &mut u64,
    snap_tx: &watch::Sender<Arc<ReadSnapshot>>,
    events: &broadcast::Sender<Event>,
    active: &Arc<Mutex<HashSet<TxId>>>,
    cfg: &EngineConfig,
    wal: Option<&Wal>,
) {
    let Transaction { id, source, reply } = t;
    if let Some(w) = wal {
        if w.poisoned() {
            let _ = reply.send(Err(EngineError::WalPoisoned));
            return;
        }
    }

    let was_running = space.was.map.is_some();
    if was_running {
        space.btm = space.was.pause_all();
    }

    let undo = space.btm.clone();
    match space.add_all_sexpr(source.as_bytes()) {
        Ok(count) => {
            *version += 1;
            publish(snap_tx, space, *version);
            let _ = events.send(Event::Tx {
                tx: id.clone(),
                count,
                version: *version,
            });
            active.lock().unwrap().insert(id.clone());
            let ok = TxOk {
                tx: id.clone(),
                count,
                version: *version,
            };

            // Ack immediately: the HTTP handler returns 200 as soon as this arrives.
            let _ = reply.send(Ok(ok));

            // WAL append continues asynchronously — the client already has their
            // response. Under `always` this means the 200 no longer gates on fsync;
            // durability is still ensured by the WAL for crash recovery.
            if let Some(w) = wal {
                w.append(
                    Rec::Tx {
                        id: &id,
                        source: &source,
                    },
                    None,
                );
            }
        }
        Err(e) => {
            // The kernel loader writes as it parses; the clone undoes any partial load.
            // Nothing was published since the clone, so the version doesn't move.
            space.btm = undo;
            let reason = format!("load failed: {e}");
            let _ = events.send(Event::Abort {
                tx: id.clone(),
                reason: reason.clone(),
                version: *version,
            });
            let _ = reply.send(Err(EngineError::LoadFailed { detail: reason }));
            if was_running {
                let btm = std::mem::take(&mut space.btm);
                space.was.resume_all(btm);
            }
            return;
        }
    }

    // Step to quiescence asynchronously — the client is already gone (received 200).
    finish_tx(space, &id, undo, version, snap_tx, events, active, cfg, wal);

    if was_running {
        let btm = std::mem::take(&mut space.btm);
        space.was.resume_all(btm);
    }
}

/// Step `txid` to its outcome — quiescent commit, budget stop, or abort — emitting the
/// outcome event, appending the outcome WAL record, and releasing the `active` entry.
/// Shared by the live path and the recovery of a crash-interrupted transaction (whose
/// TX record is already in the log).
fn finish_tx(
    space: &mut Space,
    txid: &TxId,
    undo: PathMap<u64>,
    version: &mut u64,
    snap_tx: &watch::Sender<Arc<ReadSnapshot>>,
    events: &broadcast::Sender<Event>,
    active: &Arc<Mutex<HashSet<TxId>>>,
    cfg: &EngineConfig,
    wal: Option<&Wal>,
) {
    let mut steps: u64 = 0;
    loop {
        if steps >= cfg.step_budget {
            match cfg.budget_action {
                BudgetAction::Commit => {
                    match pause_pending_execs(space) {
                        Ok(true) => *version += 1, // parking is an observable change
                        Ok(false) => {}
                        Err(e) => log::error!("parking execs after budget exhaustion: {e}"),
                    }
                    publish(snap_tx, space, *version);
                    let _ = events.send(Event::Budget {
                        tx: txid.clone(),
                        steps,
                        version: *version,
                    });
                    if let Some(w) = wal {
                        w.append(
                            Rec::Commit {
                                id: txid,
                                steps,
                                version: *version,
                            },
                            None,
                        );
                    }
                }
                BudgetAction::Abort => {
                    space.btm = undo;
                    *version += 1;
                    publish(snap_tx, space, *version);
                    let reason = format!("step budget exhausted ({steps} steps)");
                    let _ = events.send(Event::Abort {
                        tx: txid.clone(),
                        reason,
                        version: *version,
                    });
                    if let Some(w) = wal {
                        w.append(
                            Rec::Abort {
                                id: txid,
                                reason: "step budget exhausted",
                            },
                            None,
                        );
                    }
                }
            }
            break;
        }
        match step_once(space, txid, version, snap_tx, events) {
            StepOutcome::Stepped => steps += 1,
            StepOutcome::Done => {
                let _ = events.send(Event::Quiescent {
                    tx: txid.clone(),
                    version: *version,
                });
                if let Some(w) = wal {
                    w.append(
                        Rec::Commit {
                            id: txid,
                            steps,
                            version: *version,
                        },
                        None,
                    );
                }
                break;
            }
            StepOutcome::Failed(reason) => {
                // Rollback IS a new observable state (steps were published since the
                // clone): bump + publish so /export and the delta stream see the revert.
                space.btm = undo;
                *version += 1;
                publish(snap_tx, space, *version);
                if let Some(w) = wal {
                    w.append(
                        Rec::Abort {
                            id: txid,
                            reason: &reason,
                        },
                        None,
                    );
                }
                let _ = events.send(Event::Abort {
                    tx: txid.clone(),
                    reason,
                    version: *version,
                });
                break;
            }
        }
    }
    active.lock().unwrap().remove(txid);
}

/// Budget action `commit`: quiesce by force. Every still-pending `(exec …)` — all of them
/// belong to the running transaction, because the space is fully drained between
/// transactions — is re-rooted as inert `(paused (exec …))` data. Partial progress stays,
/// nothing is left steppable (so a later transaction's stray fallback can't resume it
/// under the wrong atomic scope), and clients can inspect or explicitly resume the parked
/// continuations via /export and a follow-up transaction.
///
/// Text roundtrip on purpose: the kernel dump prints variables as `$a`, `$b`, … and the
/// loader re-reads them by name into identical de Bruijn structure, so remove + re-add is
/// exact. Leftover-exec counts at budget stop are small (the program's frontier).
///
/// Returns whether anything was parked — `false` means the space held no pending execs
/// (the caller skips its version bump, and WAL replay mirrors that decision exactly).
fn pause_pending_execs(space: &mut Space) -> Result<bool, String> {
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

/// **What**: rebuild the Space from `data_dir` — checkpoint restore + log replay — and
/// open the WAL for append. Runs on the engine thread before the `ready` signal.
///
/// **Why** replay re-executes instead of loading trie changes: stepping is a pure
/// function of trie contents (raw-byte symbols, trie-order exec picks, no clock/rand),
/// so the durable history only needs what arrived and how far it ran — the smallest
/// possible log (VoltDB-style command logging).
///
/// **How** (per plan §5.4): load `checkpoint.meta` if present (`restore_paths` +
/// version/tx_counter/first_segment from it), then per record: `Tx` → hold as pending;
/// `Commit{steps}` → apply + run exactly `steps` deterministic steps + mirror the
/// live pause decision; `Abort` → drop pending unapplied. A dangling pending TX (crash
/// mid-execution; its client is gone) is re-run fresh under the live budget and closed
/// with a real outcome record. Any impossibility (mid-log corruption was already a hard
/// error in the scan; replay divergence here) is a hard error — the caller exits rather
/// than serve a wrong space. Version mismatches against `Commit.version` are logged as
/// determinism canaries but don't stop recovery.
///
/// Replay publishes snapshots and events through the normal paths — nobody is
/// subscribed yet (the listener binds after `ready`), and reusing the live code kills a
/// whole parallel non-publishing variant.
fn recover(
    space: &mut Space,
    version: &mut u64,
    dir: &Path,
    cfg: &EngineConfig,
    snap_tx: &watch::Sender<Arc<ReadSnapshot>>,
    events: &broadcast::Sender<Event>,
    active: &Arc<Mutex<HashSet<TxId>>>,
) -> io::Result<Wal> {
    fs::create_dir_all(dir)?;
    let mut first_segment = 0u64;
    let mut max_tx = 0u64;

    if let Some(m) = CkptMeta::load(dir)? {
        if m.format != "paths" {
            return Err(io::Error::other(format!(
                "unsupported checkpoint format '{}'",
                m.format
            )));
        }
        let snap_path = dir.join(&m.snapshot);
        if !snap_path.exists() {
            return Err(io::Error::other(format!(
                "checkpoint.meta names missing snapshot '{}'",
                m.snapshot
            )));
        }
        space.restore_paths(&snap_path)?;
        *version = m.version;
        max_tx = m.tx_counter;
        first_segment = m.first_segment;
        log::info!(
            "recovered checkpoint '{}' at version {}",
            m.snapshot,
            m.version
        );
    }

    let recs = wal::read_segments(dir, first_segment)?;
    let n_recs = recs.len();
    let mut pending: Option<(TxId, String)> = None;
    for rec in recs {
        match rec {
            OwnedRec::Tx { id, source } => {
                max_tx = max_tx.max(txid_count(&id).unwrap_or(0));
                if let Some((prev, _)) = &pending {
                    return Err(io::Error::other(format!(
                        "TX {id} while {prev} is unfinished — malformed log"
                    )));
                }
                let txid = TxId::new(id).map_err(|e| io::Error::other(format!("replay: invalid txid: {e}")))?;
                pending = Some((txid, source));
            }
            OwnedRec::Commit {
                id,
                steps,
                version: logged,
            } => {
                let Some((pid, source)) = pending.take() else {
                    return Err(io::Error::other(format!(
                        "COMMIT for {id} with no pending TX"
                    )));
                };
                let cid = TxId::new(id).map_err(|e| io::Error::other(format!("replay: invalid commit txid: {e}")))?;
                if pid != cid {
                    return Err(io::Error::other(format!(
                        "COMMIT for {cid} but pending TX is {pid}"
                    )));
                }
                space.add_all_sexpr(source.as_bytes()).map_err(|e| {
                    io::Error::other(format!(
                        "replay of {cid}: load failed (determinism broken?): {e}"
                    ))
                })?;
                *version += 1;
                for i in 0..steps {
                    match step_once(space, &cid, version, snap_tx, events) {
                        StepOutcome::Stepped => {}
                        StepOutcome::Done => {
                            return Err(io::Error::other(format!(
                                "replay of {cid}: log says {steps} steps but the space drained after {i}"
                            )));
                        }
                        StepOutcome::Failed(r) => {
                            return Err(io::Error::other(format!(
                                "replay of {cid}: step {i} failed: {r}"
                            )));
                        }
                    }
                }
                if pause_pending_execs(space).map_err(io::Error::other)? {
                    *version += 1; // mirrors the live budget-commit bump
                }
                if *version != logged {
                    log::error!(
                        "replay of {cid}: version {} != logged {logged} (determinism canary)",
                        *version
                    );
                }
            }
            OwnedRec::Abort { id, .. } => {
                max_tx = max_tx.max(txid_count(&id).unwrap_or(0));
                pending = None; // never applied — matches the live rollback exactly
            }
        }
    }

    cfg.tx_counter.store(max_tx, Ordering::Relaxed);
    let w = Wal::open(dir, cfg.fsync)?;

    if let Some((id, source)) = pending {
        log::warn!("transaction {id} was interrupted by the crash: re-running fresh");
        let undo = space.btm.clone();
        match space.add_all_sexpr(source.as_bytes()) {
            Ok(_) => {
                *version += 1;
                finish_tx(
                    space,
                    &id,
                    undo,
                    version,
                    snap_tx,
                    events,
                    active,
                    cfg,
                    Some(&w),
                );
            }
            Err(e) => {
                space.btm = undo;
                w.append(
                    Rec::Abort {
                        id: &id,
                        reason: &format!("load failed on recovery: {e}"),
                    },
                    None,
                );
            }
        }
    }

    log::info!(
        "recovery complete: {n_recs} records replayed, version {}, tx counter {max_tx}",
        *version
    );
    Ok(w)
}

/// The numeric prefix of a `tx<n>_…` id — what the shared counter must clear after
/// recovery so fresh ids never collide with replayed ones.
fn txid_count(id: &str) -> Option<u64> {
    id.strip_prefix("tx")?.split_once('_')?.0.parse().ok()
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
                let tx = attributed
                    .cloned()
                    .or_else(|| parsed_txid.and_then(|s| TxId::new(s).ok()))
                    .unwrap_or_else(|| TxId::new("tx0_00000000".into()).unwrap());
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
    let mut m: PathMap<u64> = PathMap::new();
    m.insert(bytes, 1u64);
    let mut v = Vec::new();
    let _ = Space::dump_all_sexpr_from(&m, sm, &mut v);
    let mut s = String::from_utf8_lossy(&v).into_owned();
    while s.ends_with('\n') {
        s.pop();
    }
    s
}
