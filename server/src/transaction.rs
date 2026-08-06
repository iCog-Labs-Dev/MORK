//! Shared types: the single command (`Transaction`), the event stream vocabulary, the
//! published read snapshot, and the state handle the HTTP layer works with.

use std::collections::HashSet;
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use mork_interning::{SharedMapping, SharedMappingHandle};
use pathmap::PathMap;
use serde_json::{json, Value};

use crate::admission::AdmissionController;

/// Validated transaction identifier: `tx<count>_<8-char alphanumeric>`.
///
/// Enforces two invariants at construction:
/// - Format matches `tx[0-9]+_[a-z0-9]{8}` (the canonical namespace prefix).
/// - Total length ≤ 63 bytes (the SymbolSize encoding limit from `mork_expr::Tag`).
///
/// If it compiles, the id is safe for WAL encoding, VM namespace wrapping, and trie paths.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TxId(String);

impl serde::Serialize for TxId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl TxId {
    /// Create a validated TxId. Returns `Err` if the format or length is wrong.
    pub fn new(id: String) -> Result<Self, String> {
        if id.len() > 63 {
            return Err(format!("txid too long: {} bytes (max 63)", id.len()));
        }
        let rest = id.strip_prefix("tx").ok_or("txid must start with 'tx'")?;
        let (digits, suffix) = rest.split_once('_').ok_or("txid missing '_' separator")?;
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err("txid count part must be non-empty digits".into());
        }
        if suffix.len() != 8 || !suffix.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()) {
            return Err("txid suffix must be exactly8 lowercase alphanumeric characters".into());
        }
        Ok(Self(id))
    }
}

impl Deref for TxId {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TxId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl PartialEq<str> for TxId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

/// Structured engine error with presentation separation.
///
/// Each variant carries:
/// - An internal message (full detail, for `log::error!` server-side)
/// - An HTTP status code
/// - A user-safe message (no internals leaked to clients)
///
/// The HTTP layer calls `status_code()` and `safe_message()` for the response,
/// and `log_internal()` to record the full detail.
#[derive(Debug)]
pub enum EngineError {
    /// WAL disk write error — server is temporarily unable to persist.
    WalPoisoned,
    /// Transaction body rejected by the kernel loader.
    LoadFailed { detail: String },
}

impl EngineError {
    pub fn status_code(&self) -> hyper::StatusCode {
        use hyper::StatusCode;
        match self {
            EngineError::WalPoisoned => StatusCode::SERVICE_UNAVAILABLE,
            EngineError::LoadFailed { .. } => StatusCode::UNPROCESSABLE_ENTITY,
        }
    }

    pub fn safe_message(&self) -> String {
        match self {
            EngineError::WalPoisoned => {
                "write error on disk; writes refused (reads still serve)".into()
            }
            EngineError::LoadFailed { .. } => "transaction rejected by the kernel loader".into(),
        }
    }

    /// Log the full internal detail server-side. Call before returning the safe message
    /// to the client.
    pub fn log_internal(&self, txid: &str) {
        match self {
            EngineError::WalPoisoned => {
                log::error!("tx {txid}: WAL poisoned, writes refused");
            }
            EngineError::LoadFailed { detail } => {
                log::error!("tx {txid}: load failed: {detail}");
            }
        }
    }
}

/// A transaction: an atomically-applied, auto-running unit of data + execs, already
/// loc-wrapped into its namespace by `wrap::rewrite`.
pub struct Transaction {
    pub id: TxId,
    /// Rewritten MeTTa source, ready for `Space::add_all_sexpr` verbatim.
    pub source: String,
    pub reply: tokio::sync::oneshot::Sender<Result<TxOk, EngineError>>,
}

/// Commands the HTTP layer sends to the engine thread. Everything that mutates `Space`
/// goes through this channel — `Space` is `!Send`, so only the engine thread touches it.
pub enum EngineCmd {
    Tx(Transaction),
    SweepStart { reply: tokio::sync::oneshot::Sender<Result<String, String>> },
    SweepPause { reply: tokio::sync::oneshot::Sender<Result<(), String>> },
    SweepResume { reply: tokio::sync::oneshot::Sender<Result<(), String>> },
    SweepStop { reply: tokio::sync::oneshot::Sender<Result<(), String>> },
}

pub struct TxOk {
    pub tx: TxId,
    pub count: usize,
    pub version: u64,
}

/// Everything the server tells clients, in one SSE vocabulary. Every variant carries
/// `version`, the global step counter, so any client can totally order what it observes.
#[derive(Clone, Debug)]
pub enum Event {
    /// A transaction's expressions were applied atomically; `count` = expressions added.
    Tx { tx: TxId, count: usize, version: u64 },
    /// One VM step ran for `tx`: `exec` is the s-expr that executed (loc reported
    /// unwrapped), `touched` = template instantiations performed, `new` = whether anything
    /// not already present was written, `us` = duration in microseconds.
    Step { tx: TxId, exec: String, touched: usize, new: bool, us: u64, version: u64 },
    /// **Per-transaction**: `tx` committed — it ran to quiescence (nothing steppable
    /// left) and its effects are permanent. The next queued transaction, if any, starts
    /// after this. Contrast with [`Event::Idle`].
    Quiescent { tx: TxId, version: u64 },
    /// **Global**: no pending execs in *any* namespace — every program has quiesced and the
    /// engine thread is parked awaiting new transactions. Always preceded by the last
    /// [`Event::Quiescent`]. Contrast with [`Event::Quiescent`].
    Idle { version: u64 },
    /// Opt-in snapshot diff (`/events?deltas=true`): the expressions added/removed between
    /// two consecutively observed snapshots, computed off the engine thread via PathMap
    /// `subtract`. Under load several steps may coalesce into one delta.
    Delta { version: u64, added: Vec<String>, removed: Vec<String> },
    /// The transaction was rolled back — a failed load, a failing exec, or budget
    /// exhaustion under `--budget-action abort`. The space is exactly as if the
    /// transaction never happened.
    Abort { tx: TxId, reason: String, version: u64 },
    /// The transaction exhausted its step budget under `--budget-action commit` (the
    /// default): partial progress is kept, and every still-pending `(exec …)` was parked
    /// as inert `(paused (exec …))` data — inspect or resume via /export and a new
    /// transaction.
    Budget { tx: TxId, steps: u64, version: u64 },
}

impl Event {
    pub fn name(&self) -> &'static str {
        match self {
            Event::Tx { .. } => "tx",
            Event::Step { .. } => "step",
            Event::Quiescent { .. } => "quiescent",
            Event::Idle { .. } => "idle",
            Event::Delta { .. } => "delta",
            Event::Abort { .. } => "abort",
            Event::Budget { .. } => "budget",
        }
    }

    pub fn data(&self) -> Value {
        match self {
            Event::Tx { tx, count, version } => json!({"tx": tx, "count": count, "version": version}),
            Event::Step { tx, exec, touched, new, us, version } =>
                json!({"tx": tx, "exec": exec, "touched": touched, "new": new, "us": us, "version": version}),
            Event::Quiescent { tx, version } => json!({"tx": tx, "version": version}),
            Event::Idle { version } => json!({"version": version}),
            Event::Delta { version, added, removed } => json!({"version": version, "added": added, "removed": removed}),
            Event::Abort { tx, reason, version } => json!({"tx": tx, "reason": reason, "version": version}),
            Event::Budget { tx, steps, version } => json!({"tx": tx, "steps": steps, "version": version}),
        }
    }

    /// The transaction this event belongs to, for `?tx=` filtering. Events with no id
    /// (idle, delta, global errors) pass every filter.
    pub fn tx_id(&self) -> Option<&str> {
        match self {
            Event::Tx { tx, .. } | Event::Step { tx, .. } | Event::Quiescent { tx, .. }
            | Event::Abort { tx, .. } | Event::Budget { tx, .. } => Some(tx.as_ref()),
            Event::Idle { .. } | Event::Delta { .. } => None,
        }
    }
}

/// A consistent, immutable view of the space: an O(1) copy-on-write clone of the trie plus
/// the (shared, thread-safe) symbol table, stamped with the step version it was taken at.
pub struct ReadSnapshot {
    pub btm: PathMap<u64>,
    pub sm: SharedMappingHandle,
    pub version: u64,
}

impl ReadSnapshot {
    pub fn empty() -> Self {
        Self { btm: PathMap::new(), sm: SharedMapping::new(), version: 0 }
    }
}

/// What the HTTP layer holds: submit transactions, read the latest snapshot, subscribe to
/// events. The `Space` itself is owned exclusively by the engine thread.
pub struct ServerState {
    pub tx_send: tokio::sync::mpsc::Sender<EngineCmd>,
    pub snapshot: tokio::sync::watch::Receiver<Arc<ReadSnapshot>>,
    pub events: tokio::sync::broadcast::Sender<Event>,
    /// Shared with the engine: recovery restores it to the highest replayed txid count
    /// before the listener binds, so fresh txids can never collide with logged ones.
    pub tx_counter: Arc<AtomicU64>,
    /// Transactions with pending execs (kept by the engine; read by `hello`).
    pub active: Arc<Mutex<HashSet<TxId>>>,
    /// Admission control: body size limits, in-flight request budget, SSE subscriber budget.
    pub admission: AdmissionController,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txid_valid_format() {
        let id = TxId::new("tx17_si49f8v6".into()).unwrap();
        assert_eq!(&*id, "tx17_si49f8v6");
        assert_eq!(id.to_string(), "tx17_si49f8v6");
    }

    #[test]
    fn txid_minimal_valid() {
        assert!(TxId::new("tx0_00000000".into()).is_ok());
    }

    #[test]
    fn txid_max_length_63() {
        // 63 bytes: "tx" + 52 digits + "_" + 8 suffix = 63
        let count = "0".repeat(52);
        let id = format!("tx{count}_abcdefgh");
        assert_eq!(id.len(), 63);
        assert!(TxId::new(id).is_ok());
    }

    #[test]
    fn txid_rejects_too_long() {
        let count = "0".repeat(53);
        let id = format!("tx{count}_abcdefgh");
        assert_eq!(id.len(), 64);
        assert!(TxId::new(id).is_err());
    }

    #[test]
    fn txid_rejects_missing_prefix() {
        assert!(TxId::new("17_si49f8v6".into()).is_err());
    }

    #[test]
    fn txid_rejects_missing_separator() {
        assert!(TxId::new("tx17si49f8v6".into()).is_err());
    }

    #[test]
    fn txid_rejects_empty_count() {
        assert!(TxId::new("tx_abcdefgh".into()).is_err());
    }

    #[test]
    fn txid_rejects_non_digit_count() {
        assert!(TxId::new("txabc_abcdefgh".into()).is_err());
    }

    #[test]
    fn txid_rejects_short_suffix() {
        assert!(TxId::new("tx1_abcdefg".into()).is_err());
    }

    #[test]
    fn txid_rejects_long_suffix() {
        assert!(TxId::new("tx1_abcdefghi".into()).is_err());
    }

    #[test]
    fn txid_rejects_uppercase_suffix() {
        assert!(TxId::new("tx1_ABCDEFGH".into()).is_err());
    }

    #[test]
    fn txid_deref_to_str() {
        let id = TxId::new("tx1_abcdefgh".into()).unwrap();
        let s: &str = &id;
        assert_eq!(s, "tx1_abcdefgh");
    }

    #[test]
    fn txid_partial_eq_str() {
        let id = TxId::new("tx1_abcdefgh".into()).unwrap();
        assert_eq!(&*id, "tx1_abcdefgh");
        assert_ne!(&*id, "tx2_abcdefgh");
    }

    #[test]
    fn txid_clone() {
        let id = TxId::new("tx1_abcdefgh".into()).unwrap();
        let id2 = id.clone();
        assert_eq!(id, id2);
    }

    #[test]
    fn txid_hash_consistent() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let id1 = TxId::new("tx1_abcdefgh".into()).unwrap();
        let id2 = TxId::new("tx1_abcdefgh".into()).unwrap();
        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        id1.hash(&mut h1);
        id2.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    #[test]
    fn txid_display() {
        let id = TxId::new("tx1_abcdefgh".into()).unwrap();
        assert_eq!(format!("{id}"), "tx1_abcdefgh");
    }

    #[test]
    fn txid_serialize_json() {
        let id = TxId::new("tx1_abcdefgh".into()).unwrap();
        let v = serde_json::to_value(&id).unwrap();
        assert_eq!(v, serde_json::json!("tx1_abcdefgh"));
    }

    #[test]
    fn engine_error_wal_poisoned_status() {
        let e = EngineError::WalPoisoned;
        assert_eq!(e.status_code(), hyper::StatusCode::SERVICE_UNAVAILABLE);
        // Safe message must not contain the internal error detail
        assert!(!e.safe_message().contains("EIO"));
    }

    #[test]
    fn engine_error_load_failed_status() {
        let e = EngineError::LoadFailed { detail: "parse error at byte 42".into() };
        assert_eq!(e.status_code(), hyper::StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(e.safe_message(), "transaction rejected by the kernel loader");
        // Internal detail must not leak to safe message
        assert!(!e.safe_message().contains("byte 42"));
    }

    #[test]
    fn engine_error_log_internal_does_not_panic() {
        let e1 = EngineError::WalPoisoned;
        e1.log_internal("tx1_abcdefgh");
        let e2 = EngineError::LoadFailed { detail: "test".into() };
        e2.log_internal("tx2_abcdefgh");
    }

    #[test]
    fn event_tx_id_returns_some_for_transaction_events() {
        let id = TxId::new("tx1_abcdefgh".into()).unwrap();
        let ev = Event::Tx { tx: id.clone(), count: 1, version: 1 };
        assert_eq!(ev.tx_id(), Some("tx1_abcdefgh"));
    }

    #[test]
    fn event_tx_id_returns_none_for_idle() {
        let ev = Event::Idle { version: 1 };
        assert_eq!(ev.tx_id(), None);
    }

    #[test]
    fn event_tx_id_returns_none_for_delta() {
        let ev = Event::Delta { version: 1, added: vec![], removed: vec![] };
        assert_eq!(ev.tx_id(), None);
    }

    #[test]
    fn event_name_matches_variant() {
        let id = TxId::new("tx1_abcdefgh".into()).unwrap();
        assert_eq!(Event::Tx { tx: id.clone(), count: 0, version: 0 }.name(), "tx");
        assert_eq!(Event::Step { tx: id.clone(), exec: "()".into(), touched: 0, new: false, us: 0, version: 0 }.name(), "step");
        assert_eq!(Event::Quiescent { tx: id.clone(), version: 0 }.name(), "quiescent");
        assert_eq!(Event::Idle { version: 0 }.name(), "idle");
        assert_eq!(Event::Delta { version: 0, added: vec![], removed: vec![] }.name(), "delta");
        assert_eq!(Event::Abort { tx: id.clone(), reason: "".into(), version: 0 }.name(), "abort");
        assert_eq!(Event::Budget { tx: id, steps: 0, version: 0 }.name(), "budget");
    }
}
