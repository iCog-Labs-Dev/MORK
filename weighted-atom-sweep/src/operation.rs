use std::io::Write;

use pathmap::zipper::{WriteZipperTracked, ZipperValues};

/// Trait for operations that transform a submap of the PathMap trie.
///
/// Operations receive a mutable reference to a [`WriteZipperTracked<u64>`] focused at the
/// atom's position in the trie, plus the raw `atom_path` bytes returned by the
/// traversal engine. The write zipper is scoped to the subtrie at that position —
/// the operation can read and modify everything at and below the focus, but cannot
/// ascend above it.
///
/// IMPORTANT: Implementations MUST use `wz.set_val_w()` / `wz.remove_val_w()` (not the
/// raw `set_val` / `remove_val`) so that `propagate_agg_w` fires automatically on write.
pub trait TransformOp: Send + Sync {
    /// Returns the name of this operation, used for tracing and identification.
    fn name(&self) -> &str;

    /// Apply the operation to the subtrie accessible via the write zipper.
    ///
    /// # Arguments
    /// * `wz` — Write zipper focused at the atom's position in the PathMap trie.
    /// * `atom_path` — The raw byte path returned by the traversal engine. The
    ///   zipper is already focused at this path; this parameter provides context.
    fn apply(&self, wz: &mut WriteZipperTracked<u64>, atom_path: &[u8]);
}

/// A stateless operation defined by a function pointer.
///
/// This is the simplest form of operation — a named function that receives a
/// [`WriteZipperTracked<u64>`] focused at the atom's position and the atom path bytes.
/// The function can read and modify the subtrie but cannot ascend above the focus.
#[derive(Clone, Copy, Debug)]
pub struct Operation {
    pub name: &'static str,
    pub transform: fn(&mut WriteZipperTracked<u64>, &[u8]),
}

impl Operation {
    /// Create a new operation with the given name and transform function.
    pub fn new(name: &'static str, transform: fn(&mut WriteZipperTracked<u64>, &[u8])) -> Self {
        Self { name, transform }
    }
}

impl PartialEq for Operation {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && (self.transform as usize) == (other.transform as usize)
    }
}

impl TransformOp for Operation {
    fn name(&self) -> &str {
        self.name
    }

    fn apply(&self, wz: &mut WriteZipperTracked<u64>, atom_path: &[u8]) {
        (self.transform)(wz, atom_path);
    }
}

/// Importance-decay transform: read the sampled atom's weight and write back a
/// slightly smaller one. This is the canonical WAS single-atom transform and the
/// simplest exercise of the feedback loop (visited atoms lose weight → lower future
/// visit rate). Mirrors ECAN importance decay.
///
/// Input: `wz` focused at the sampled atom (its weight is the focus value); `atom_path`
/// unused. No return — the weight change propagates via `set_val_w` (+ the sweep's
/// `cleanup_write_zipper_w`).
///
/// Decay rule: subtract 10% (at least 1) so it is strictly monotonic down to 0.
pub fn decay(wz: &mut WriteZipperTracked<u64>, _atom_path: &[u8]) {
    if let Some(&w) = wz.val() {
        if w > 0 {
            let dec = (w / 10).max(1);
            // MUST be set_val_w (not set_val) so agg_w propagates — see trait docs above.
            wz.set_val_w(w - dec);
        }
    }
}

impl Operation {
    /// The built-in importance-decay operation (see [`decay`]).
    pub fn decay() -> Self {
        Operation::new("decay", decay)
    }

    /// Log the atom path and its current weight via `tracing::info!`.
    /// Useful for observing which atoms the sweep is processing and their weights.
    pub fn log_atom() -> Self {
        Operation::new("log-atom", log_atom)
    }
}

fn log_atom(wz: &mut WriteZipperTracked<u64>, atom_path: &[u8]) {
    let weight = wz.val().copied().unwrap_or(0);
    let expr = mork_expr::serialize(atom_path);
    println!("sweep atom: {} weight={}", expr, weight);
}


/// Observer pattern trait for managing operation subscriptions on a sweep process.
pub trait OperationObserver {
    /// Subscribe an operation to be executed during the sweep.
    fn subscribe(&mut self, operation: Box<dyn TransformOp>);
    /// Unsubscribe an operation by name. All operations with the matching name
    /// will be removed.
    fn unsubscribe_by_name(&mut self, name: &str);
}
