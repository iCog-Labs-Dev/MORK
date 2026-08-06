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

/// A transaction: an atomically-applied, auto-running unit of data + execs, already
/// loc-wrapped into its namespace by `wrap::rewrite`.
pub struct Transaction {
    pub id: TxId,
    /// Rewritten MeTTa source, ready for `Space::add_all_sexpr` verbatim.
    pub source: String,
    pub reply: tokio::sync::oneshot::Sender<Result<TxOk, String>>,
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
