//! Worker threads: each runs one transaction to completion against its own O(1)
//! copy-on-write snapshot, sharing nothing with any other worker.
//!
//! `Space` is `!Send`, so a worker never receives one — it receives a `PathMap` and a
//! `SharedMappingHandle` (both `Send + Sync`) and builds its `Space` locally via
//! `Space::with`. Symbol interning across threads is already supported by
//! `mork-interning` (per-thread write permits, 128 thread ceiling).

use std::sync::{Arc, Mutex, mpsc};

use mork::space::Space;
use mork_interning::SharedMappingHandle;
use pathmap::PathMap;
use tokio::sync::oneshot;

use crate::engine::BudgetAction;
use crate::transaction::{TxId, TxOk};

/// How a worker's run ended. Note that `Budget` is a normal, committable outcome —
/// the worker has already parked its pending execs, so its trie is final.
pub enum WorkerOutcome {
    Quiesced,
    Budget,
    Failed(String),
}

/// A dispatched unit of work: everything a worker needs, nothing it does not.
pub struct Job {
    pub id: TxId,
    pub base: PathMap<()>,
    pub base_version: u64,
    pub source: String,
    pub reply: oneshot::Sender<Result<TxOk, String>>,
}

/// A finished run, handed back to the committer for validation.
pub struct TxResult {
    pub id: TxId,
    pub base_version: u64,
    pub btm: PathMap<()>,
    pub remove_prefixes: Vec<Vec<u8>>,
    pub count: usize,
    pub steps: u64,
    pub outcome: WorkerOutcome,
    pub reply: oneshot::Sender<Result<TxOk, String>>,
}

/// The parts of a `TxResult` that a run produces; the committer re-attaches the
/// identity, base version, and the client's reply channel.
pub struct TxResultParts {
    pub btm: PathMap<()>,
    pub remove_prefixes: Vec<Vec<u8>>,
    pub count: usize,
    pub steps: u64,
    pub outcome: WorkerOutcome,
}

/// Run one transaction start to finish. Split out from the thread body so it can be
/// tested without a channel or a pool.
pub fn run_one(
    id: TxId,
    base: PathMap<()>,
    source: String,
    sm: SharedMappingHandle,
    step_budget: u64,
    budget_action: BudgetAction,
) -> TxResultParts {
    let mut space = Space::with(base, sm);
    let mut remove_prefixes: Vec<Vec<u8>> = Vec::new();

    let count = match space.add_all_sexpr(source.as_bytes()) {
        Ok(count) => count,
        Err(e) => {
            return TxResultParts {
                btm: space.btm.clone(), // O(1): Space has Drop, can't move the field out
                remove_prefixes,
                count: 0,
                steps: 0,
                outcome: WorkerOutcome::Failed(format!("load failed: {e}")),
            };
        }
    };

    let mut steps: u64 = 0;
    let outcome = loop {
        if steps >= step_budget {
            match budget_action {
                BudgetAction::Commit => {
                    if let Err(e) = crate::engine::pause_pending_execs(&mut space) {
                        break WorkerOutcome::Failed(format!("parking execs: {e}"));
                    }
                    break WorkerOutcome::Budget;
                }
                BudgetAction::Abort => {
                    break WorkerOutcome::Failed(format!("step budget exhausted ({steps} steps)"));
                }
            }
        }
        match crate::engine::step_once(&mut space, &id, &mut remove_prefixes) {
            crate::engine::StepOutcome::Stepped => steps += 1,
            crate::engine::StepOutcome::Done => break WorkerOutcome::Quiesced,
            crate::engine::StepOutcome::Failed(r) => break WorkerOutcome::Failed(r),
        }
    };

    TxResultParts { btm: space.btm.clone(), remove_prefixes, count, steps, outcome } // O(1)
}

/// Spawn `n` long-lived worker threads pulling from a shared job queue.
///
/// Long-lived, not per-transaction: each thread registers a symbol-table index with
/// `mork-interning`, and those indices must not churn. `n` is validated against
/// `MAX_WRITER_THREADS` by the caller.
pub fn spawn_workers(
    n: usize,
    sm: SharedMappingHandle,
    jobs: Arc<Mutex<mpsc::Receiver<Job>>>,
    results: mpsc::Sender<TxResult>,
    step_budget: u64,
    budget_action: BudgetAction,
) -> Vec<std::thread::JoinHandle<()>> {
    (0..n)
        .map(|i| {
            let sm = sm.clone();
            let jobs = jobs.clone();
            let results = results.clone();
            std::thread::Builder::new()
                .name(format!("mork-worker-{i}"))
                // Same reason as the committer thread: the kernel's parse/serialize/unify
                // machinery recurses per nesting level and overflows the 2 MB default.
                .stack_size(512 * 1024 * 1024)
                .spawn(move || loop {
                    let job = { jobs.lock().unwrap().recv() };
                    let Ok(job) = job else { break };
                    let Job { id, base, base_version, source, reply } = job;
                    // A kernel panic (e.g. an `unreachable!()` on a malformed exec — see
                    // the worker tests) must still yield exactly one `TxResult`: without
                    // this, the committer never hears back, `in_flight` never decrements,
                    // and `committed.end` never runs — pinning `active_bases`' watermark
                    // and growing `history` without bound forever after. On the panic
                    // path the worker's `Space`/trie are simply discarded (never sent
                    // anywhere), so asserting unwind-safety here is sound.
                    let id2 = id.clone();
                    let run = std::panic::AssertUnwindSafe(|| {
                        run_one(id2, base, source, sm.clone(), step_budget, budget_action)
                    });
                    let parts = match std::panic::catch_unwind(run) {
                        Ok(parts) => parts,
                        Err(payload) => {
                            let msg = payload
                                .downcast_ref::<&str>()
                                .map(|s| s.to_string())
                                .or_else(|| payload.downcast_ref::<String>().cloned())
                                .unwrap_or_else(|| "worker panicked".to_string());
                            log::error!("worker panicked running {id}: {msg}");
                            TxResultParts {
                                btm: PathMap::new(),
                                remove_prefixes: Vec::new(),
                                count: 0,
                                steps: 0,
                                outcome: WorkerOutcome::Failed(format!("worker panicked: {msg}")),
                            }
                        }
                    };
                    let _ = results.send(TxResult {
                        id,
                        base_version,
                        btm: parts.btm,
                        remove_prefixes: parts.remove_prefixes,
                        count: parts.count,
                        steps: parts.steps,
                        outcome: parts.outcome,
                        reply,
                    });
                })
                .expect("failed to spawn worker thread")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mork_interning::SharedMapping;
    use pathmap::PathMap;

    #[test]
    fn worker_runs_a_transaction_against_its_snapshot_and_leaves_the_base_alone() {
        let sm = SharedMapping::new();
        let base: PathMap<()> = PathMap::new();

        let parts = run_one(
            "tx1_test".to_string(),
            base.clone(),
            "(edge a b)\n".to_string(),
            sm.clone(),
            1_000_000,
            crate::engine::BudgetAction::Commit,
        );

        assert!(matches!(parts.outcome, WorkerOutcome::Quiesced));
        assert_eq!(parts.count, 1, "one top-level expression was loaded");
        assert!(!parts.btm.is_empty(), "the worker's trie holds its writes");
        assert!(base.is_empty(), "the base snapshot is untouched");
    }

    #[test]
    fn worker_reports_a_failing_exec_without_panicking() {
        let sm = SharedMapping::new();
        let parts = run_one(
            "tx2_test".to_string(),
            PathMap::new(),
            // a malformed exec: the pattern functor must be `,` or `I`
            "(exec 0 (X (edge $x)) (O (+ (y $x))))\n".to_string(),
            sm,
            1_000_000,
            crate::engine::BudgetAction::Commit,
        );
        assert!(matches!(parts.outcome, WorkerOutcome::Failed(_)));
    }

    #[test]
    fn worker_collects_removal_prefixes() {
        let sm = SharedMapping::new();
        let parts = run_one(
            "tx3_test".to_string(),
            PathMap::new(),
            // `I` factors must be wrapped in `(BTM ...)` (see kernel/src/space.rs's own
            // `(I (BTM (edge $x $y)))` test) — a bare pattern here hits `unreachable!()`
            // deep in ASource::new, not a graceful kernel error.
            "(edge a b)\n(exec 0 (I (BTM (edge $x $y))) (O (- (edge $x $y))))\n".to_string(),
            sm,
            1_000_000,
            crate::engine::BudgetAction::Commit,
        );
        assert!(!parts.remove_prefixes.is_empty(), "a removing tx must report prefixes");
    }

    #[test]
    fn a_panicking_run_still_yields_exactly_one_failed_result() {
        // Exercises the real `spawn_workers` thread body (not `run_one` directly), so
        // this proves the committer's channel actually receives a `TxResult` after a
        // kernel panic, not just that `run_one` would return one if called directly.
        let sm = SharedMapping::new();
        let (job_tx, job_rx) = mpsc::channel::<Job>();
        let (res_tx, res_rx) = mpsc::channel::<TxResult>();
        let _workers = spawn_workers(
            1,
            sm,
            Arc::new(Mutex::new(job_rx)),
            res_tx,
            1_000_000,
            crate::engine::BudgetAction::Commit,
        );
        let (reply, _reply_rx) = tokio::sync::oneshot::channel();
        job_tx
            .send(Job {
                id: "txpanic_test".to_string(),
                base: PathMap::new(),
                base_version: 0,
                // A bare `I` pattern (missing the `BTM` wrapper — see the comment on
                // `worker_collects_removal_prefixes`) hits `unreachable!()` deep in
                // `ASource::new`, killing the thread if not caught.
                source: "(exec 0 (I (edge $x $y)) (O (- (edge $x $y))))\n".to_string(),
                reply,
            })
            .unwrap();

        let result = res_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the worker must still report back after a panic, not go silent");
        assert!(
            matches!(result.outcome, WorkerOutcome::Failed(_)),
            "a panicking run must surface as Failed, not be lost"
        );
        assert!(
            res_rx.try_recv().is_err(),
            "exactly one TxResult per dispatched job, even after a panic"
        );
    }
}
