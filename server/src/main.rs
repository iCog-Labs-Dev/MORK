//! mork-server: expose `Space::metta_calculus` to concurrent network clients.
//!
//! Architecture (see the plan): a pool of worker threads runs transactions concurrently,
//! each against its own snapshot, and one committer thread validates and installs their
//! results in a total order; readers get lock-free O(1) COW snapshots; an SSE stream
//! carries every execution event. One submission verb: `POST /run` — submitting a
//! transaction (data + execs) IS running it.

mod engine;
mod events;
mod http;
mod mvcc;
mod read;
mod transaction;
// `engine::recover` and `commit` drive the append/checkpoint/replay paths; `Ack` and
// `Wal::poisoned` have no caller yet outside wal.rs's own tests (nothing acks writes or
// checks poisoning before accepting new transactions), hence the blanket allow.
#[allow(dead_code)]
mod wal;
mod worker;
mod wrap;

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex};

use clap::Parser;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use transaction::ServerState;

#[derive(Parser)]
#[command(
    name = "mork-server",
    about = "HTTP + SSE server for the MORK metta-calculus VM"
)]
struct Args {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:8081")]
    addr: String,
    /// Broadcast buffer size per SSE subscriber (events beyond this are reported as `lagged`).
    #[arg(long, default_value_t = 4096)]
    events_buffer: usize,
    /// Max VM steps a single transaction may run before `--budget-action` applies
    /// (this bounds how long a transaction can pin its base snapshot, and so how much
    /// version history the committer must retain).
    #[arg(long, default_value_t = 1_000_000)]
    step_budget: u64,
    /// On budget exhaustion: `commit` keeps partial progress and parks pending execs as
    /// `(paused …)` data; `abort` rolls the whole transaction back.
    #[arg(long, value_enum, default_value = "commit")]
    budget_action: engine::BudgetAction,
    /// Enable persistence: WAL + crash recovery rooted at this directory. Absent = pure
    /// in-memory (today's behavior).
    #[arg(long)]
    data_dir: Option<std::path::PathBuf>,
    /// When the log is fsynced — i.e. when POST /run's 200 implies "on disk".
    #[arg(long, value_enum, default_value = "everysec")]
    fsync: wal::FsyncPolicy,
    /// Checkpoint the space and delete pre-checkpoint log segments every N finished
    /// transactions; 0 disables (the log grows unbounded).
    #[arg(long, default_value_t = 1024)]
    checkpoint_every: u64,
    /// Number of transactions that may execute concurrently. 1 = the previous
    /// sequential engine, exactly. Capped by the symbol table's writer-thread limit.
    #[arg(long, default_value_t = 1)]
    workers: usize,
}

fn main() {
    env_logger::init();
    let args = Args::parse();

    if args.workers == 0 || args.workers > mork_interning::MAX_WRITER_THREADS {
        eprintln!("--workers must be between 1 and {}", mork_interning::MAX_WRITER_THREADS);
        std::process::exit(2);
    }

    let (events, _keep) = broadcast::channel(args.events_buffer);
    let active = Arc::new(Mutex::new(HashSet::new()));
    let tx_counter = Arc::new(AtomicU64::new(0));
    let cfg = engine::EngineConfig {
        step_budget: args.step_budget,
        budget_action: args.budget_action,
        data_dir: args.data_dir.clone(),
        fsync: args.fsync,
        checkpoint_every: args.checkpoint_every,
        tx_counter: tx_counter.clone(),
        // Checked non-zero just above; NonZeroUsize downstream makes "0 workers" (which
        // would block run()'s loop forever on a pool with nobody in it) unrepresentable
        // rather than a case every reader has to remember is excluded.
        workers: std::num::NonZeroUsize::new(args.workers).expect("checked non-zero above"),
    };
    let (tx_send, snap_rx, ready, engine_join) = engine::spawn_engine(events.clone(), active.clone(), cfg);

    // Recovery gate: the listener must not accept requests (and mint txids) until the
    // engine has replayed the log and restored the shared counter.
    ready.recv().expect("engine died during startup/recovery");

    let state = Arc::new(ServerState {
        tx_send,
        snapshot: snap_rx.clone(),
        events: events.clone(),
        tx_counter,
        active,
        delta_subs: Arc::new(AtomicUsize::new(0)),
    });

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        // wrap/unwrap and snapshot-diff dumps recurse over expression nesting too
        // (worker + blocking threads); match the engine's generous stack.
        .thread_stack_size(64 * 1024 * 1024)
        .build()
        .unwrap();

    rt.block_on(async {
        tokio::spawn(events::delta_task(
            snap_rx,
            events,
            state.delta_subs.clone(),
        ));

        let listener = TcpListener::bind(&args.addr)
            .await
            .unwrap_or_else(|e| panic!("failed to bind {}: {e}", args.addr));
        println!("mork-server listening on http://{}", args.addr);

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    log::info!("ctrl-c: shutting down");
                    break;
                }
                accepted = listener.accept() => {
                    let Ok((stream, _peer)) = accepted else { continue };
                    let st = state.clone();
                    tokio::spawn(async move {
                        let io = TokioIo::new(stream);
                        let svc = service_fn(move |req| http::handle(req, st.clone()));
                        if let Err(e) = hyper::server::conn::http1::Builder::new()
                            .serve_connection(io, svc)
                            .await
                        {
                            log::debug!("connection error: {e}");
                        }
                    });
                }
            }
        }
    });

    // Dropping the runtime aborts open connections; dropping the last ServerState clone
    // closes the transaction channel, which is the engine's shutdown signal.
    drop(rt);
    drop(state);
    let _ = engine_join.join();
}
