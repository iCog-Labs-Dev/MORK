//! mork-server: expose `Space::metta_calculus` to concurrent network clients.
//!
//! Architecture (see the plan): a single engine thread owns the `Space` and serializes all
//! mutation; readers get lock-free O(1) COW snapshots; an SSE stream carries every
//! execution event. One submission verb: `POST /run` — submitting a transaction (data +
//! execs) IS running it.

mod admission;
mod engine;
mod events;
mod http;
mod read;
mod transaction;
mod wal;
mod wrap;

use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
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
    /// (scheduling is sequential, so this bounds how long one transaction can hold the
    /// engine).
    #[arg(long, default_value_t = 1_000_000)]
    step_budget: u64,
    /// On budget exhaustion: `commit` keeps partial progress and parks pending execs as
    /// `(paused …)` data; `abort` rolls the whole transaction back.
    #[arg(long, value_enum, default_value = "commit")]
    budget_action: engine::BudgetAction,
    /// Source/sink sweep passes per cooperative scheduler cycle.
    #[arg(long, default_value_t = 1)]
    sweep_steps_per_cycle: usize,
    /// Whole-space metta-calculus steps after each weighted sweep batch.
    #[arg(long, default_value_t = 32)]
    sweep_metta_steps: usize,
    /// Milliseconds to back off when an active source/sink sweep cycle changes nothing.
    #[arg(long, default_value_t = 10)]
    sweep_idle_ms: u64,
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
    /// Max request body size in bytes for POST /run. Rejects larger bodies with 413 before
    /// buffering.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    max_body_bytes: usize,
    /// Max concurrent in-flight POST /run requests. Rejects with 503 when at capacity.
    #[arg(long, default_value_t = 64)]
    max_inflight: usize,
    /// Max concurrent SSE subscribers on GET /events. Rejects with 503 when at capacity.
    #[arg(long, default_value_t = 4096)]
    max_sse_subs: usize,
    /// Max concurrent TCP connections. Rejects at accept time with TCP RST when at
    /// capacity — no HTTP response is sent, the client sees a connection reset.
    #[arg(long, default_value_t = 4096)]
    max_connections: usize,
}

fn main() {
    env_logger::init();
    let args = Args::parse();

    let (events, _keep) = broadcast::channel(args.events_buffer);
    let active = Arc::new(Mutex::new(HashSet::new()));
    let tx_counter = Arc::new(AtomicU64::new(0));
    let cfg = engine::EngineConfig {
        step_budget: args.step_budget,
        budget_action: args.budget_action,
        sweep_steps_per_cycle: args.sweep_steps_per_cycle,
        sweep_metta_steps: args.sweep_metta_steps,
        sweep_idle_ms: args.sweep_idle_ms,
        data_dir: args.data_dir.clone(),
        fsync: args.fsync,
        checkpoint_every: args.checkpoint_every,
        tx_counter: tx_counter.clone(),
    };
    let (tx_send, snap_rx, ready, engine_join) = engine::spawn_engine(events.clone(), active.clone(), cfg);

    // Recovery gate: the listener must not accept requests (and mint txids) until the
    // engine has replayed the log and restored the shared counter.
    ready.recv().expect("engine died during startup/recovery");

    let admission = admission::AdmissionController::new(
        args.max_body_bytes,
        args.max_inflight,
        args.max_sse_subs,
    );

    let state = Arc::new(ServerState {
        tx_send,
        snapshot: snap_rx.clone(),
        events: events.clone(),
        tx_counter,
        active,
        admission: admission.clone(),
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
            admission.clone(),
        ));

        let conn_semaphore = Arc::new(tokio::sync::Semaphore::new(args.max_connections));
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
                    let permit = match conn_semaphore.clone().try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => {
                            log::debug!("connection rejected: at capacity ({})", args.max_connections);
                            drop(stream);
                            continue;
                        }
                    };
                    let st = state.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
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
    match engine_join.join() {
        Ok(()) => log::info!("engine: exited cleanly"),
        Err(e) => {
            if let Some(s) = e.downcast_ref::<&str>() {
                log::error!("engine: panicked: {s}");
            } else if let Some(s) = e.downcast_ref::<String>() {
                log::error!("engine: panicked: {s}");
            } else {
                log::error!("engine: panicked with unknown payload");
            }
        }
    }
}
