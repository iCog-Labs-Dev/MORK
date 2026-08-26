//! A thread that releases a write permit must be able to acquire a fresh one safely.
//!
//! `try_aquire_permission` claims a slot by CAS and remembers it in a thread-local, and
//! `WritePermit::drop` frees that slot for other threads. If the drop leaves the
//! thread-local set, the next acquire on the same thread short-circuits and hands back
//! the remembered index without a CAS -- by then another thread may own it. Two owners
//! of one slot both write `permissions[index]` and `to_bytes[index]`, whose Relaxed
//! atomics are only sound for a single owner, so symbols come back as other symbols'
//! bytes or the slab pointers go bad.
//!
//! This is the shape mork-server's worker pool has: long-lived threads that take one
//! permit per transaction and drop it, rather than holding a permit forever.

use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mork_interning::SharedMapping;

const WORKERS: usize = 16;
const JOBS: usize = 4000;
const SYMS_PER_JOB: usize = 50;

/// Long enough that a loaded machine never trips it, short enough that a deadlocked
/// run reports a failure instead of hanging the suite.
const JOB_TIMEOUT: Duration = Duration::from_secs(60);

#[test]
fn a_reacquired_permit_does_not_alias_another_threads_slot() {
    let sm = SharedMapping::new();
    let (job_tx, job_rx) = mpsc::channel::<usize>();
    let job_rx = Arc::new(Mutex::new(job_rx));
    let (done_tx, done_rx) = mpsc::channel::<usize>();

    let workers: Vec<_> = (0..WORKERS)
        .map(|_| {
            let sm = sm.clone();
            let job_rx = job_rx.clone();
            let done_tx = done_tx.clone();
            std::thread::spawn(move || {
                loop {
                    let job = { job_rx.lock().unwrap().recv() };
                    let Ok(i) = job else { break };
                    let mut syms = Vec::with_capacity(SYMS_PER_JOB);
                    {
                        // One permit for the whole job, then released -- exactly what
                        // `ParDataParser` does for one `add_all_sexpr` body.
                        let permit = sm.try_aquire_permission().unwrap();
                        for j in 0..SYMS_PER_JOB {
                            // Varying lengths so symbols land in different slab slots;
                            // equal-length symbols would hide a mispointed write.
                            let s = format!("job{i}_sym{j}_{}", "p".repeat((i + j) % 53));
                            syms.push((permit.get_sym_or_insert(s.as_bytes()), s));
                        }
                    }
                    // Read back *after* dropping the permit: any symbol that does not
                    // resolve to its own bytes was corrupted by a concurrent writer.
                    let bad = syms
                        .into_iter()
                        .filter(|(sym, s)| sm.get_bytes(*sym) != Some(s.as_bytes()))
                        .count();
                    if done_tx.send(bad).is_err() {
                        break;
                    }
                }
            })
        })
        .collect();
    drop(done_tx);

    // Warm up one job at a time. Each worker claims a slot while every other worker is
    // idle, so the same free index is handed out repeatedly -- which is what leaves the
    // stale thread-local pointing at a slot someone else now owns. Firing all the jobs
    // at once instead mostly gives each worker a distinct slot and hides the bug.
    for i in 0..WORKERS {
        job_tx.send(i).unwrap();
        recv_job(&done_rx);
    }

    for i in WORKERS..JOBS {
        job_tx.send(i).unwrap();
    }
    let bad: usize = (WORKERS..JOBS).map(|_| recv_job(&done_rx)).sum();

    drop(job_tx);
    for w in workers {
        w.join().expect("a worker panicked; a permit was aliased badly enough to abort");
    }

    let total = (JOBS - WORKERS) * SYMS_PER_JOB;
    assert_eq!(bad, 0, "{bad} of {total} symbols did not read back as their own bytes");
}

/// Turns a deadlock into a test failure. The unfixed code could park every worker on the
/// per-slot lock, which `recv()` would wait on forever.
fn recv_job(done_rx: &mpsc::Receiver<usize>) -> usize {
    match done_rx.recv_timeout(JOB_TIMEOUT) {
        Ok(bad) => bad,
        Err(RecvTimeoutError::Timeout) => {
            panic!("no worker reported within {JOB_TIMEOUT:?}: the permit slots deadlocked")
        }
        Err(RecvTimeoutError::Disconnected) => panic!("every worker died before reporting"),
    }
}
