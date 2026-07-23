//! SSE encoding of the event stream, and the opt-in snapshot-diff ("delta") task.

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use hyper::Response;
use mork::space::Space;
use serde_json::json;
use tokio::sync::{broadcast, watch};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::transaction::{Event, ReadSnapshot, ServerState};
use crate::wrap;

fn frame(event: &str, data: serde_json::Value) -> Bytes {
    Bytes::from(format!("event: {event}\ndata: {data}\n\n"))
}

/// Decrements the delta-subscriber count when an SSE connection ends.
struct DeltaGuard(Option<Arc<AtomicUsize>>);
impl Drop for DeltaGuard {
    fn drop(&mut self) {
        if let Some(c) = &self.0 { c.fetch_sub(1, Relaxed); }
    }
}

/// `GET /events[?tx=<txid>][&deltas=true]` — the single stream carrying everything: a
/// `hello` on connect, then `tx`/`step`/`quiescent`/`idle`/`delta`/`error` as they happen,
/// plus `lagged` if this client falls behind the broadcast buffer.
pub fn sse_response(
    state: &Arc<ServerState>,
    tx_filter: Option<String>,
    want_deltas: bool,
) -> Response<BoxBody<Bytes, Infallible>> {
    // Subscribe BEFORE reading the snapshot so no event between them is missed.
    let rx = state.events.subscribe();
    let snap = state.snapshot.borrow().clone();
    let active: Vec<String> = state.active.lock().unwrap().iter().cloned().collect();

    let guard = DeltaGuard(if want_deltas {
        state.delta_subs.fetch_add(1, Relaxed);
        Some(state.delta_subs.clone())
    } else {
        None
    });

    let hello = frame("hello", json!({
        "version": snap.version,
        "count": snap.btm.val_count(),
        "active_txs": active,
    }));

    let events = BroadcastStream::new(rx).filter_map(move |item| {
        let _keepalive = &guard;
        match item {
            Ok(ev) => {
                if !want_deltas && matches!(ev, Event::Delta { .. }) { return None }
                if let (Some(want), Some(is)) = (tx_filter.as_deref(), ev.tx_id()) {
                    if want != is { return None }
                }
                Some(frame(ev.name(), ev.data()))
            }
            Err(BroadcastStreamRecvError::Lagged(skipped)) =>
                Some(frame("lagged", json!({"skipped": skipped}))),
        }
    });

    let body = tokio_stream::once(hello)
        .chain(events)
        .map(|b| Ok::<_, Infallible>(Frame::data(b)));

    Response::builder()
        .status(200)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(StreamBody::new(body).boxed())
        .unwrap()
}

/// Runs off the engine thread: whenever a new snapshot is published and at least one client
/// opted into deltas, diff it against the previously observed one with PathMap's set
/// algebra (`subtract` — structural-sharing aware where the tries share nodes) and
/// broadcast the added/removed expressions. Under load, several steps may coalesce into
/// one watch update and therefore one delta.
pub async fn delta_task(
    mut snap_rx: watch::Receiver<Arc<ReadSnapshot>>,
    events: broadcast::Sender<Event>,
    delta_subs: Arc<AtomicUsize>,
) {
    let mut prev = snap_rx.borrow().clone();
    while snap_rx.changed().await.is_ok() {
        let cur = snap_rx.borrow_and_update().clone();
        if delta_subs.load(Relaxed) == 0 || cur.version == prev.version {
            prev = cur;
            continue;
        }
        let (p, c) = (prev.clone(), cur.clone());
        let diff = tokio::task::spawn_blocking(move || {
            let added = dump_lines(&c.btm.subtract(&p.btm), &c.sm);
            let removed = dump_lines(&p.btm.subtract(&c.btm), &p.sm);
            (added, removed)
        })
        .await;
        if let Ok((added, removed)) = diff {
            let _ = events.send(Event::Delta { version: cur.version, added, removed });
        }
        prev = cur;
    }
}

fn dump_lines(m: &pathmap::PathMap<u64>, sm: &mork_interning::SharedMappingHandle) -> Vec<String> {
    let mut v = Vec::new();
    let _ = Space::dump_all_sexpr_from(m, sm, &mut v);
    String::from_utf8_lossy(&v)
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| wrap::unwrap_text(l).0)
        .collect()
}
