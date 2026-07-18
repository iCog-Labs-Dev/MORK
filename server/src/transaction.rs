//! Shared types: the single command (`Transaction`), the event stream vocabulary, the
//! published read snapshot, and the state handle the HTTP layer works with.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex};

use mork_interning::{SharedMapping, SharedMappingHandle};
use pathmap::PathMap;
use serde_json::{json, Value};

/// `tx<count>_<unique 8-char alphanumeric>`, e.g. `tx17_si49f8v6`.
pub type TxId = String;

/// The one and only engine command: an atomically-applied, auto-running unit of data +
/// execs, already loc-wrapped into its namespace by `wrap::rewrite`.
pub struct Transaction {
    pub id: TxId,
    /// Rewritten MeTTa source, ready for `Space::add_all_sexpr` verbatim.
    pub source: String,
    pub reply: tokio::sync::oneshot::Sender<Result<TxOk, String>>,
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
            | Event::Abort { tx, .. } | Event::Budget { tx, .. } => Some(tx),
            Event::Idle { .. } | Event::Delta { .. } => None,
        }
    }
}

/// A consistent, immutable view of the space: an O(1) copy-on-write clone of the trie plus
/// the (shared, thread-safe) symbol table, stamped with the step version it was taken at.
pub struct ReadSnapshot {
    pub btm: PathMap<()>,
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
    pub tx_send: tokio::sync::mpsc::Sender<Transaction>,
    pub snapshot: tokio::sync::watch::Receiver<Arc<ReadSnapshot>>,
    pub events: tokio::sync::broadcast::Sender<Event>,
    pub tx_counter: AtomicU64,
    /// Transactions with pending execs (kept by the engine; read by `hello`).
    pub active: Arc<Mutex<HashSet<TxId>>>,
    /// Number of connected `?deltas=true` subscribers; the delta task skips work at 0.
    pub delta_subs: Arc<AtomicUsize>,
}
