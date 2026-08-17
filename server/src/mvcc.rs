//! Snapshot-isolation machinery: writeset extraction, conflict validation, and
//! installation. Everything here is a pure function over `PathMap<()>` — no threads,
//! no I/O — so the whole conflict model is testable without an engine.

// Tasks 4-6 wire this into the engine; nothing calls it yet.
#![allow(dead_code)]

use pathmap::PathMap;
use pathmap::zipper::{ZipperValues, ZipperIteration};

/// What a transaction did, relative to the snapshot it started from.
///
/// `added` and `removed` are disjoint by construction: a path cannot be both absent
/// from the base and present in it. That is what makes `join`-then-`subtract` at
/// install time order-independent.
pub struct WriteSet {
    pub added: PathMap<()>,
    pub removed: PathMap<()>,
}

/// Extract a transaction's writeset by diffing its final trie against its base.
///
/// Cheap for the same reason snapshots are: `subtract` short-circuits on pointer
/// identity at every node, and a transaction's trie shares every node it did not
/// write with its base. Cost is proportional to what the transaction changed, not
/// to the size of the space.
pub fn writeset(base: &PathMap<()>, finished: &PathMap<()>) -> WriteSet {
    WriteSet {
        added: finished.subtract(base),
        removed: base.subtract(finished),
    }
}

/// Does `m` contain any path at or below `prefix`?
///
/// This is the phantom probe: a transaction that removed by pattern `P` conflicts
/// with a concurrent transaction that added anything under `P`'s ground prefix.
/// Testing the value **at** the prefix matters — a ground prefix can name an exact
/// stored path with no children, and descending past it would miss that value.
pub fn has_any_below(m: &PathMap<()>, prefix: &[u8]) -> bool {
    let mut z = m.read_zipper_at_borrowed_path(prefix);
    z.val().is_some() || z.to_next_val()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pathmap::PathMap;

    fn map(paths: &[&[u8]]) -> PathMap<()> {
        let mut m = PathMap::new();
        for p in paths { m.insert(p, ()); }
        m
    }

    #[test]
    fn writeset_is_the_difference_in_both_directions() {
        let base = map(&[b"a", b"b"]);
        let mut fin = base.clone();
        fin.insert(b"c", ());
        fin.remove(b"a");

        let ws = writeset(&base, &fin);
        assert!(ws.added.contains(b"c"), "c was added");
        assert!(!ws.added.contains(b"b"), "b was untouched, not added");
        assert!(ws.removed.contains(b"a"), "a was removed");
        assert!(!ws.removed.contains(b"b"), "b was untouched, not removed");
    }

    #[test]
    fn untouched_snapshot_has_an_empty_writeset() {
        let base = map(&[b"a", b"b", b"c"]);
        let ws = writeset(&base, &base.clone());
        assert!(ws.added.is_empty());
        assert!(ws.removed.is_empty());
    }

    #[test]
    fn has_any_below_finds_descendants() {
        let m = map(&[b"edge/a", b"edge/b", b"node/x"]);
        assert!(has_any_below(&m, b"edge"));
        assert!(has_any_below(&m, b"node"));
        assert!(!has_any_below(&m, b"weight"));
    }

    #[test]
    fn has_any_below_finds_the_value_at_the_prefix_itself() {
        // A pattern's ground prefix can name an exact stored path, with nothing
        // below it. Descending and only looking at children would miss it.
        let m = map(&[b"edge"]);
        assert!(has_any_below(&m, b"edge"), "must see the value AT the prefix");
    }

    #[test]
    fn has_any_below_on_empty_map_is_false() {
        let m: PathMap<()> = PathMap::new();
        assert!(!has_any_below(&m, b"anything"));
    }
}
