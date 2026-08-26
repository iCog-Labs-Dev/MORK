//! HTTP surface: `POST /run` (submit = run), `GET /events` (SSE), `GET /export`,
//! `GET /stats`.
//! Everything else the old-style servers exposed (`/count`, `/clear`, `/status`, a separate
//! load step) is expressible as a transaction or already on the stream.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use serde_json::json;

use crate::transaction::{ServerState, Transaction};
use crate::{events, read, wrap};

type Body = BoxBody<Bytes, Infallible>;

pub async fn handle(req: Request<Incoming>, state: Arc<ServerState>) -> Result<Response<Body>, Infallible> {
    let (parts, body) = req.into_parts();
    let query = parse_query(parts.uri.query().unwrap_or(""));
    let resp = match (parts.method.clone(), parts.uri.path()) {
        (Method::POST, "/run") => run_transaction(body, &state).await,
        (Method::GET, "/events") => events::sse_response(
            &state,
            query.get("tx").cloned(),
            query.get("deltas").map(|v| v == "true" || v == "1").unwrap_or(false),
        ),
        (Method::GET, "/export") => export(&state, &query),
        (Method::GET, "/stats") => json_response(StatusCode::OK, stats(&state)),
        _ => json_response(StatusCode::NOT_FOUND, json!({"ok": false, "error": "not found"})),
    };
    Ok(resp)
}

/// The single submission verb: body = MeTTa s-expr text (data + execs, exactly the CLI's
/// input syntax). Applied atomically under a fresh tx namespace; execution starts
/// immediately; progress streams on `/events`.
async fn run_transaction(body: Incoming, state: &Arc<ServerState>) -> Response<Body> {
    let bytes = match body.collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) => return json_response(StatusCode::BAD_REQUEST, json!({"ok": false, "error": format!("body read failed: {e}")})),
    };
    let src = match std::str::from_utf8(&bytes) {
        Ok(s) => s,
        Err(_) => return json_response(StatusCode::BAD_REQUEST, json!({"ok": false, "error": "body must be UTF-8 s-expression text"})),
    };

    let id = wrap::gen_txid(state.tx_counter.fetch_add(1, Relaxed) + 1);
    let source = match wrap::rewrite(src, &id) {
        Ok(w) => w,
        Err(e) => return json_response(StatusCode::BAD_REQUEST, json!({"ok": false, "error": format!("parse error: {e}")})),
    };

    let (reply, reply_rx) = tokio::sync::oneshot::channel();
    if state.tx_send.send(Transaction { id, source, reply }).await.is_err() {
        return json_response(StatusCode::SERVICE_UNAVAILABLE, json!({"ok": false, "error": "engine is shut down"}));
    }
    match reply_rx.await {
        Ok(Ok(ok)) => json_response(
            StatusCode::OK,
            json!({"ok": true, "tx": ok.tx, "count": ok.count, "version": ok.version}),
        ),
        // "unavailable:" is the engine's marker for "not your fault" — 503, not 422. It
        // covers a refused commit (the log is poisoned, nothing was applied) and a failed
        // durability ack (the transaction installed but the disk would not take it). Both
        // need an operator, so no Retry-After is offered.
        Ok(Err(e)) if e.starts_with("unavailable:") => {
            json_response(StatusCode::SERVICE_UNAVAILABLE, json!({"ok": false, "error": e}))
        }
        Ok(Err(e)) => json_response(StatusCode::UNPROCESSABLE_ENTITY, json!({"ok": false, "error": e})),
        Err(_) => json_response(StatusCode::INTERNAL_SERVER_ERROR, json!({"ok": false, "error": "engine dropped the reply"})),
    }
}

fn export(state: &Arc<ServerState>, query: &HashMap<String, String>) -> Response<Body> {
    let snap = state.snapshot.borrow().clone();
    match read::export(&snap, query.get("pattern").map(|s| s.as_str()), query.get("template").map(|s| s.as_str())) {
        Ok(text) => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/plain; charset=utf-8")
            .header("x-mork-version", snap.version.to_string())
            .body(Full::new(Bytes::from(text)).boxed())
            .unwrap(),
        // Same marker `/run` uses: the symbol table being saturated is not a bad request.
        Err(e) if e.starts_with("unavailable:") => {
            json_response(StatusCode::SERVICE_UNAVAILABLE, json!({"ok": false, "error": e}))
        }
        Err(e) => json_response(StatusCode::BAD_REQUEST, json!({"ok": false, "error": e})),
    }
}

/// Engine gauges, for load tests and operators.
///
/// `version` is the published snapshot's, the same number `/export` returns in
/// `x-mork-version`. `in_flight` is how many transactions are executing right now, and
/// `history_len` is how many committed writesets the committer is still retaining so
/// those in-flight transactions can validate against them — the retention number that
/// grows when a long transaction pins an old base snapshot.
///
/// `make_unique_calls` and `cow_clones` appear **only** in a `--features counters`
/// build. The keys are absent, not zero, in a normal build: a zero would read as "this
/// workload has no copy-on-write amplification" rather than "nobody measured".
fn stats(state: &Arc<ServerState>) -> serde_json::Value {
    #[cfg_attr(not(feature = "counters"), allow(unused_mut))]
    let mut v = json!({
        "version": state.snapshot.borrow().version,
        "in_flight": state.active.lock().unwrap().len(),
        "history_len": state.history_len.load(Relaxed),
    });
    #[cfg(feature = "counters")]
    {
        let c = pathmap::counters::cow_counters();
        v["make_unique_calls"] = c.make_unique_calls.into();
        v["cow_clones"] = c.cow_clones.into();
    }
    v
}

fn json_response(status: StatusCode, value: serde_json::Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(value.to_string())).boxed())
        .unwrap()
}

fn parse_query(q: &str) -> HashMap<String, String> {
    q.split('&')
        .filter(|kv| !kv.is_empty())
        .filter_map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            Some((percent_decode(k)?, percent_decode(v)?))
        })
        .collect()
}

fn percent_decode(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                out.push(u8::from_str_radix(std::str::from_utf8(b.get(i + 1..i + 3)?).ok()?, 16).ok()?);
                i += 3;
            }
            b'+' => { out.push(b' '); i += 1; }
            c => { out.push(c); i += 1; }
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::ReadSnapshot;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicU64, AtomicUsize};
    use std::sync::Mutex;

    /// A `ServerState` wired to nothing: `/stats` only reads the shared handles, so the
    /// channels never need an engine on the other end.
    fn detached_state(version: u64, in_flight: &[&str], history_len: usize) -> Arc<ServerState> {
        let (tx_send, _rx) = tokio::sync::mpsc::channel(1);
        // `borrow()` still returns the last published value after the sender drops, so
        // the state needs no live engine behind it.
        let snapshot = tokio::sync::watch::channel(Arc::new(ReadSnapshot {
            version,
            ..ReadSnapshot::empty()
        })).1;
        Arc::new(ServerState {
            tx_send,
            snapshot,
            events: tokio::sync::broadcast::channel(1).0,
            tx_counter: Arc::new(AtomicU64::new(0)),
            active: Arc::new(Mutex::new(in_flight.iter().map(|s| s.to_string()).collect::<HashSet<_>>())),
            history_len: Arc::new(AtomicUsize::new(history_len)),
            delta_subs: Arc::new(AtomicUsize::new(0)),
        })
    }

    #[test]
    fn stats_reports_the_three_always_present_gauges() {
        let v = stats(&detached_state(42, &["tx1_aaaaaaaa", "tx2_bbbbbbbb"], 4));
        assert_eq!(v["version"], 42);
        assert_eq!(v["in_flight"], 2);
        assert_eq!(v["history_len"], 4);
    }

    /// The COW keys are build-gated, and their ABSENCE in a default build is the
    /// contract — a zero would be read as "no amplification measured here".
    #[test]
    fn cow_keys_are_present_only_under_the_counters_feature() {
        let v = stats(&detached_state(0, &[], 0));
        assert_eq!(v.get("cow_clones").is_some(), cfg!(feature = "counters"));
        assert_eq!(v.get("make_unique_calls").is_some(), cfg!(feature = "counters"));
    }
}
