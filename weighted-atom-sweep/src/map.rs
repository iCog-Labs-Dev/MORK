use pathmap::zipper::{ZipperCreation, ZipperHeadOwned, ZipperValues, ZipperWriting};
use std::{ops::Deref, sync::Arc};

/// A thread-safe wrapper around PathMap's ZipperHeadOwned for managing weighted atoms.
///
/// Uses `u64` as the value type for weight tracking. Each atom's weight is
/// stored as the trie value at its path. The `agg_w` field on trie nodes
/// (from PathMap's Stage A) keeps aggregate weights up to date.
pub struct WeightedMap {
    pub inner: Arc<ZipperHeadOwned<u64>>,
}

impl Deref for WeightedMap {
    type Target = ZipperHeadOwned<u64>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl WeightedMap {
    /// Read the weight stored at `path`.
    pub fn get_val(&self, path: &[u8]) -> Option<u64> {
        match self.inner.read_zipper_at_path(path) {
            Ok(z) => z.val().cloned(),
            Err(_) => None,
        }
    }

    /// Set the weight at `path`. Uses `set_val_w` so that agg_w is
    /// propagated to ancestors — required for sweeps to read accurate
    /// aggregate weights during sampling.
    pub fn set_weighted_val(&self, path: &[u8], val: u64) -> Result<(), ()> {
        if let Ok(mut z) = self.inner.write_zipper_at_exclusive_path(path) {
            z.set_val_w(val);
            Ok(())
        } else {
            Err(())
        }
    }
}
