//! mork-server: expose `Space::metta_calculus` to concurrent network clients.
//!
//! Architecture (see the plan): a single engine thread owns the `Space` and serializes all
//! mutation; readers get lock-free O(1) COW snapshots; an SSE stream carries every
//! execution event. One submission verb: `POST /run` — submitting a transaction (data +
//! execs) IS running it.

mod engine;
mod events;
mod http;
mod read;
mod transaction;
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
#[command(name = "mork-server", about = "HTTP + SSE server for the MORK metta-calculus VM")]
struct Args {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:8081")]
    addr: String,
    /// Broadcast buffer size per SSE subscriber (events beyond this are reported as `lagged`).
    #[arg(long, default_value_t = 4096)]
    events_buffer: usize,
}

fn main() {
    env_logger::init();
    let args = Args::parse();

    let (events, _keep) = broadcast::channel(args.events_buffer);
    let active = Arc::new(Mutex::new(HashSet::new()));
    let (tx_send, snap_rx, engine_join) = engine::spawn_engine(events.clone(), active.clone());

    let state = Arc::new(ServerState {
        tx_send,
        snapshot: snap_rx.clone(),
        events: events.clone(),
        tx_counter: AtomicU64::new(0),
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
        tokio::spawn(events::delta_task(snap_rx, events, state.delta_subs.clone()));

        let listener = TcpListener::bind(&args.addr).await
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
