//! HTTP surface: `POST /run` (submit = run), `GET /events` (SSE), `GET /export`.
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

use crate::transaction::{EngineCmd, ServerState, Transaction};
use crate::{events, read, wrap};

type Body = BoxBody<Bytes, Infallible>;

pub async fn handle(req: Request<Incoming>, state: Arc<ServerState>) -> Result<Response<Body>, Infallible> {
    let (parts, body) = req.into_parts();
    let query = parse_query(parts.uri.query().unwrap_or(""));
    let resp = match (parts.method.clone(), parts.uri.path()) {
        (Method::POST, "/run") => run_transaction(body, &state).await,
        (Method::POST, "/sweep/start") => handle_sweep_start(&state).await,
        (Method::POST, "/sweep/pause") => handle_sweep_pause(&state).await,
        (Method::POST, "/sweep/resume") => handle_sweep_resume(&state).await,
        (Method::POST, "/sweep/stop") => handle_sweep_stop(&state).await,
        (Method::GET, "/events") => events::sse_response(
            &state,
            query.get("tx").cloned(),
            query.get("deltas").map(|v| v == "true" || v == "1").unwrap_or(false),
        ),
        (Method::GET, "/export") => export(&state, &query),
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
    if state.tx_send.send(EngineCmd::Tx(Transaction { id, source, reply })).await.is_err() {
        return json_response(StatusCode::SERVICE_UNAVAILABLE, json!({"ok": false, "error": "engine is shut down"}));
    }
    match reply_rx.await {
        Ok(Ok(ok)) => json_response(
            StatusCode::OK,
            json!({"ok": true, "tx": ok.tx, "count": ok.count, "version": ok.version}),
        ),
        // "unavailable:" is the engine's marker for transient refusals (e.g. the WAL is
        // poisoned by a disk error): the transaction was NOT applied and retrying later
        // may succeed — 503, not 422.
        Ok(Err(e)) if e.starts_with("unavailable:") => {
            json_response(StatusCode::SERVICE_UNAVAILABLE, json!({"ok": false, "error": e}))
        }
        Ok(Err(e)) => json_response(StatusCode::UNPROCESSABLE_ENTITY, json!({"ok": false, "error": e})),
        Err(_) => json_response(StatusCode::INTERNAL_SERVER_ERROR, json!({"ok": false, "error": "engine dropped the reply"})),
    }
}

async fn handle_sweep_start(state: &Arc<ServerState>) -> Response<Body> {
    let (reply, reply_rx) = tokio::sync::oneshot::channel();
    if state.tx_send.send(EngineCmd::SweepStart { reply }).await.is_err() {
        return json_response(StatusCode::SERVICE_UNAVAILABLE, json!({"ok": false, "error": "engine is shut down"}));
    }
    match reply_rx.await {
        Ok(Ok(handle)) => json_response(StatusCode::OK, json!({"ok": true, "handle": handle})),
        Ok(Err(e)) => json_response(StatusCode::UNPROCESSABLE_ENTITY, json!({"ok": false, "error": e})),
        Err(_) => json_response(StatusCode::INTERNAL_SERVER_ERROR, json!({"ok": false, "error": "engine dropped the reply"})),
    }
}

async fn handle_sweep_pause(state: &Arc<ServerState>) -> Response<Body> {
    let (reply, reply_rx) = tokio::sync::oneshot::channel();
    if state.tx_send.send(EngineCmd::SweepPause { reply }).await.is_err() {
        return json_response(StatusCode::SERVICE_UNAVAILABLE, json!({"ok": false, "error": "engine is shut down"}));
    }
    match reply_rx.await {
        Ok(Ok(())) => json_response(StatusCode::OK, json!({"ok": true})),
        Ok(Err(e)) => json_response(StatusCode::UNPROCESSABLE_ENTITY, json!({"ok": false, "error": e})),
        Err(_) => json_response(StatusCode::INTERNAL_SERVER_ERROR, json!({"ok": false, "error": "engine dropped the reply"})),
    }
}

async fn handle_sweep_resume(state: &Arc<ServerState>) -> Response<Body> {
    let (reply, reply_rx) = tokio::sync::oneshot::channel();
    if state.tx_send.send(EngineCmd::SweepResume { reply }).await.is_err() {
        return json_response(StatusCode::SERVICE_UNAVAILABLE, json!({"ok": false, "error": "engine is shut down"}));
    }
    match reply_rx.await {
        Ok(Ok(())) => json_response(StatusCode::OK, json!({"ok": true})),
        Ok(Err(e)) => json_response(StatusCode::UNPROCESSABLE_ENTITY, json!({"ok": false, "error": e})),
        Err(_) => json_response(StatusCode::INTERNAL_SERVER_ERROR, json!({"ok": false, "error": "engine dropped the reply"})),
    }
}

async fn handle_sweep_stop(state: &Arc<ServerState>) -> Response<Body> {
    let (reply, reply_rx) = tokio::sync::oneshot::channel();
    if state.tx_send.send(EngineCmd::SweepStop { reply }).await.is_err() {
        return json_response(StatusCode::SERVICE_UNAVAILABLE, json!({"ok": false, "error": "engine is shut down"}));
    }
    match reply_rx.await {
        Ok(Ok(())) => json_response(StatusCode::OK, json!({"ok": true})),
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
        Err(e) => json_response(StatusCode::BAD_REQUEST, json!({"ok": false, "error": e})),
    }
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
