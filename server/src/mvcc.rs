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

use std::collections::VecDeque;

/// One committed transaction's effect, retained only long enough for every
/// still-running transaction older than it to validate against it (see `gc`).
#[derive(Debug)]
pub struct CommitRecord {
    pub version: u64,
    pub added: PathMap<()>,
    pub removed: PathMap<()>,
    /// Ground prefixes of the removal patterns this transaction executed.
    pub remove_prefixes: Vec<Vec<u8>>,
}

/// Why a transaction lost validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Conflict {
    /// A path this transaction added was removed by a concurrent one, or vice versa.
    WriteWrite,
    /// A pattern-scoped removal on one side raced a concurrent insertion under the
    /// same ground prefix, on either side.
    Phantom,
}

impl Conflict {
    pub fn reason(&self) -> &'static str {
        match self {
            Conflict::WriteWrite => "conflict: a path this transaction wrote was concurrently written",
            Conflict::Phantom => "phantom: a pattern-scoped removal raced a concurrent insertion under the same prefix",
        }
    }
}

/// Backward validation: check this transaction's writeset against every transaction
/// that committed after it started.
///
/// Four checks, and the two that are *absent* are as important as the two present:
/// both transactions adding the same path is not a conflict, and both removing it is
/// not either, because the space is a set and both operations are idempotent. Only a
/// path added by one side and removed by the other loses information, so only that
/// combination (checked in both directions via `meet`) is a write-write conflict.
///
/// The prefix checks over-approximate: every path a pattern can match lies under its
/// ground prefix, so a spurious abort is possible but a missed conflict is not. That
/// soundness only holds if `remove_prefixes` actually names one ground prefix per
/// removal pattern this transaction executed, each a genuine prefix of everything the
/// pattern can match — a caller that omits a prefix, or supplies one narrower than the
/// pattern, produces a silent **missed** conflict, not a spurious abort. The same is
/// true of each `CommitRecord.remove_prefixes` this function reads from `history`.
///
/// An empty prefix in `remove_prefixes` is a ground prefix over the whole space, so it
/// conflicts with any non-empty concurrent writeset — this is not filtered out here,
/// since a kernel that ever emits one has a bug that should surface, not be hidden.
///
/// Callers must hold every record newer than `base_version`; `gc` maintains that. The
/// assertion below only catches part of that precondition: it fires when the front of
/// `history` is too new, i.e. a live transaction's base was trimmed past. It does
/// *not* fire if `history` was trimmed all the way to empty while this transaction's
/// base is still live, and it cannot detect a hole trimmed out of the middle of the
/// deque — both would make `validate` silently miss real conflicts. The complete
/// invariant needs the engine's current version alongside `history`, which this
/// function does not have; it belongs to the caller that owns the version counter.
pub fn validate(
    ws: &WriteSet,
    remove_prefixes: &[Vec<u8>],
    base_version: u64,
    history: &VecDeque<CommitRecord>,
) -> Result<(), Conflict> {
    debug_assert!(
        history.front().map_or(true, |c| c.version <= base_version + 1),
        "history's front is newer than this transaction's base plus one — a live \
         transaction's base was trimmed past. NOTE: this does not catch history being \
         trimmed to empty, or a hole trimmed from the middle; only the caller that \
         owns the version counter can check the complete invariant."
    );

    for c in history.iter().filter(|c| c.version > base_version) {
        if !ws.added.meet(&c.removed).is_empty() {
            return Err(Conflict::WriteWrite);
        }
        if !ws.removed.meet(&c.added).is_empty() {
            return Err(Conflict::WriteWrite);
        }
        for p in remove_prefixes {
            if has_any_below(&c.added, p) {
                return Err(Conflict::Phantom);
            }
        }
        for p in &c.remove_prefixes {
            if has_any_below(&ws.added, p) {
                return Err(Conflict::Phantom);
            }
        }
    }
    Ok(())
}

use std::collections::BTreeMap;

/// The authoritative state, owned outright by the committer thread. No lock: there is
/// exactly one owner, and workers receive their base snapshot by message.
pub struct Committed {
    pub btm: PathMap<()>,
    pub version: u64,
    /// Effects of recently committed transactions, for validating older ones.
    pub history: VecDeque<CommitRecord>,
    /// base_version -> number of live transactions started at it. The smallest key is
    /// the watermark below which history can be discarded.
    pub active_bases: BTreeMap<u64, usize>,
}

impl Committed {
    pub fn new(btm: PathMap<()>, version: u64) -> Self {
        Self { btm, version, history: VecDeque::new(), active_bases: BTreeMap::new() }
    }

    /// Register a transaction starting now, and return the version it starts from.
    /// Its snapshot is `self.btm.clone()` — taken by the caller, O(1).
    pub fn begin(&mut self) -> u64 {
        *self.active_bases.entry(self.version).or_insert(0) += 1;
        self.version
    }

    /// Deregister a finished transaction (committed OR aborted) and collect history.
    pub fn end(&mut self, base_version: u64) {
        if let Some(n) = self.active_bases.get_mut(&base_version) {
            *n -= 1;
            if *n == 0 { self.active_bases.remove(&base_version); }
        }
        self.gc();
    }

    /// Apply a validated writeset and return the new version.
    ///
    /// `added` and `removed` are disjoint, so the order of these two operations does
    /// not matter. Both cost O(changed region) because the algebra short-circuits on
    /// pointer identity wherever `btm` and the writeset share structure.
    ///
    /// `remove_prefixes` is stored verbatim into the resulting `CommitRecord` for
    /// future callers of `validate` to read back out of `history`. The caller must
    /// supply one ground prefix per removal pattern this transaction executed, each a
    /// genuine prefix of everything that pattern can match — the same precondition
    /// `validate` documents, since this is where that data is produced for it.
    /// Omitting one, or supplying one narrower than the pattern, causes a silent
    /// missed conflict later, not a spurious abort now.
    pub fn install(&mut self, ws: WriteSet, remove_prefixes: Vec<Vec<u8>>) -> u64 {
        self.btm = self.btm.join(&ws.added).subtract(&ws.removed);
        self.version += 1;
        self.history.push_back(CommitRecord {
            version: self.version,
            added: ws.added,
            removed: ws.removed,
            remove_prefixes,
        });
        self.version
    }

    /// Drop history no live transaction can still validate against.
    ///
    /// The watermark is the oldest live base, not the newest: `validate` walks
    /// `history` looking for every record newer than a transaction's own base, so a
    /// long-running transaction that started early still needs every record after it,
    /// even ones a younger transaction (which started later and so already saw them)
    /// no longer does. Using the newest base as the watermark would trim records the
    /// old transaction still needs, producing missed conflicts.
    fn gc(&mut self) {
        let watermark = self.active_bases.keys().next().copied().unwrap_or(self.version);
        while self.history.front().is_some_and(|c| c.version <= watermark) {
            self.history.pop_front();
        }
    }
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

    use std::collections::VecDeque;

    fn record(version: u64, added: &[&[u8]], removed: &[&[u8]], prefixes: &[&[u8]]) -> CommitRecord {
        CommitRecord {
            version,
            added: map(added),
            removed: map(removed),
            remove_prefixes: prefixes.iter().map(|p| p.to_vec()).collect(),
        }
    }

    fn ws(added: &[&[u8]], removed: &[&[u8]]) -> WriteSet {
        WriteSet { added: map(added), removed: map(removed) }
    }

    fn hist(rs: Vec<CommitRecord>) -> VecDeque<CommitRecord> { rs.into() }

    // --- the four NON-conflicts: this is the point of the whole design ---

    #[test]
    fn sibling_paths_under_a_shared_prefix_do_not_conflict() {
        let h = hist(vec![record(2, &[b"edge/a/c"], &[], &[])]);
        let mine = ws(&[b"edge/a/b"], &[]);
        assert!(validate(&mine, &[], 1, &h).is_ok());
    }

    #[test]
    fn sibling_paths_do_not_conflict_when_both_writesets_are_non_empty() {
        // Every other non-conflict test has an empty map on one side of the `meet`,
        // so this pins the case where both sides actually hold a value and the
        // sibling-prefix rationale ("edge/a/b" vs "edge/a/c" share a path prefix but
        // are still unrelated paths) is exercised by a real intersection, not just by
        // one side being trivially empty.
        let h = hist(vec![record(2, &[], &[b"edge/a/c"], &[])]);
        let mine = ws(&[b"edge/a/b"], &[]);
        assert!(validate(&mine, &[], 1, &h).is_ok());
    }

    #[test]
    fn wholly_disjoint_paths_do_not_conflict() {
        let h = hist(vec![record(2, &[b"weight/x"], &[], &[])]);
        let mine = ws(&[b"edge/a/b"], &[]);
        assert!(validate(&mine, &[], 1, &h).is_ok());
    }

    #[test]
    fn both_adding_the_same_path_does_not_conflict() {
        // Set semantics: insertion is idempotent, so there is nothing to lose.
        let h = hist(vec![record(2, &[b"edge/a/b"], &[], &[])]);
        let mine = ws(&[b"edge/a/b"], &[]);
        assert!(validate(&mine, &[], 1, &h).is_ok());
    }

    #[test]
    fn both_removing_the_same_path_does_not_conflict() {
        let h = hist(vec![record(2, &[], &[b"edge/a/b"], &[])]);
        let mine = ws(&[], &[b"edge/a/b"]);
        assert!(validate(&mine, &[], 1, &h).is_ok());
    }

    // --- the conflicts ---

    #[test]
    fn my_add_versus_their_remove_conflicts() {
        let h = hist(vec![record(2, &[], &[b"edge/a/b"], &[])]);
        let mine = ws(&[b"edge/a/b"], &[]);
        assert!(matches!(validate(&mine, &[], 1, &h), Err(Conflict::WriteWrite)));
    }

    #[test]
    fn my_remove_versus_their_add_conflicts() {
        let h = hist(vec![record(2, &[b"edge/a/b"], &[], &[])]);
        let mine = ws(&[], &[b"edge/a/b"]);
        assert!(matches!(validate(&mine, &[], 1, &h), Err(Conflict::WriteWrite)));
    }

    // --- the phantom: the (edge $x) case from the design discussion ---

    #[test]
    fn my_pattern_removal_versus_their_new_matching_path_is_a_phantom() {
        // T1: remove (edge $x)  -> ground prefix b"edge", removed what existed then
        // T2 (already committed): add (edge c)
        // Plain SI would let both commit and leave (edge c) alive.
        let h = hist(vec![record(2, &[b"edge/c"], &[], &[])]);
        let mine = ws(&[], &[b"edge/a", b"edge/b"]);
        assert!(matches!(
            validate(&mine, &[b"edge".to_vec()], 1, &h),
            Err(Conflict::Phantom)
        ));
    }

    #[test]
    fn their_pattern_removal_versus_my_new_matching_path_is_a_phantom() {
        // The mirror image — they committed a pattern removal, I add under it.
        let h = hist(vec![record(2, &[], &[b"edge/a"], &[b"edge"])]);
        let mine = ws(&[b"edge/c"], &[]);
        assert!(matches!(validate(&mine, &[], 1, &h), Err(Conflict::Phantom)));
    }

    #[test]
    fn pattern_removal_does_not_conflict_with_an_unrelated_prefix() {
        let h = hist(vec![record(2, &[b"weight/c"], &[], &[])]);
        let mine = ws(&[], &[b"edge/a"]);
        assert!(validate(&mine, &[b"edge".to_vec()], 1, &h).is_ok());
    }

    // --- the base_version filter ---

    #[test]
    fn records_at_or_below_my_base_are_ignored() {
        // I already saw version 2 — it is part of my snapshot, not a concurrent tx.
        let h = hist(vec![record(2, &[], &[b"edge/a/b"], &[])]);
        let mine = ws(&[b"edge/a/b"], &[]);
        assert!(validate(&mine, &[], 2, &h).is_ok());
    }

    #[test]
    fn empty_history_never_conflicts() {
        let h = hist(vec![]);
        let mine = ws(&[b"edge/a"], &[b"edge/b"]);
        assert!(validate(&mine, &[b"edge".to_vec()], 0, &h).is_ok());
    }

    // --- named risk: an empty prefix matches any non-empty writeset ---

    #[test]
    fn empty_remove_prefix_conflicts_with_any_concurrent_add() {
        // has_any_below(m, b"") is true iff m is non-empty at all, so an empty
        // ground prefix in remove_prefixes is a phantom probe against everything.
        // This is pinned as a decision, not an accident: validate does not filter
        // empty prefixes out, because a kernel that ever emits one has its own bug,
        // and silently dropping it here would hide that bug instead of surfacing it.
        let h = hist(vec![record(2, &[b"weight/x"], &[], &[])]);
        let mine = ws(&[], &[]);
        assert!(matches!(
            validate(&mine, &[Vec::new()], 1, &h),
            Err(Conflict::Phantom)
        ));
    }

    // --- Committed: install and history GC ---

    #[test]
    fn install_applies_adds_and_removes_and_bumps_the_version() {
        let mut c = Committed::new(map(&[b"a", b"b"]), 5);
        let v = c.install(ws(&[b"c"], &[b"a"]), vec![]);
        assert_eq!(v, 6);
        assert_eq!(c.version, 6);
        assert!(c.btm.contains(b"b"), "untouched path survives");
        assert!(c.btm.contains(b"c"), "added path is present");
        assert!(!c.btm.contains(b"a"), "removed path is gone");
    }

    #[test]
    fn install_records_history_for_later_validators() {
        let mut c = Committed::new(map(&[]), 0);
        c.install(ws(&[b"x"], &[]), vec![b"edge".to_vec()]);
        assert_eq!(c.history.len(), 1);
        assert_eq!(c.history[0].version, 1);
        assert_eq!(c.history[0].remove_prefixes, vec![b"edge".to_vec()]);
    }

    #[test]
    fn gc_keeps_records_a_live_transaction_still_needs() {
        let mut c = Committed::new(map(&[]), 0);
        let old = c.begin();                        // base 0, stays live
        c.install(ws(&[b"x"], &[]), vec![]);         // -> version 1
        c.install(ws(&[b"y"], &[]), vec![]);         // -> version 2
        let young = c.begin();                       // base 2, starts and ends immediately
        c.end(young);                                // forces a gc call, watermark = 0 (old is still live)
        assert_eq!(old, 0);
        assert_eq!(c.history.len(), 2, "both records are newer than the still-live old transaction's base");
    }

    #[test]
    fn gc_drops_records_no_transaction_can_still_need() {
        let mut c = Committed::new(map(&[]), 0);
        let base = c.begin();
        c.install(ws(&[b"x"], &[]), vec![]);
        c.install(ws(&[b"y"], &[]), vec![]);
        c.end(base);                                 // no live transactions left
        assert!(c.history.is_empty(), "nothing can validate against these any more");
    }

    #[test]
    fn gc_respects_the_oldest_live_base_not_the_newest() {
        let mut c = Committed::new(map(&[]), 0);
        let old = c.begin();                         // base 0
        c.install(ws(&[b"x"], &[]), vec![]);         // -> 1
        let young = c.begin();                       // base 1
        c.install(ws(&[b"y"], &[]), vec![]);         // -> 2
        c.end(young);                                // the YOUNG one finishes first
        assert_eq!(c.history.len(), 2, "the old transaction still needs both records");
        c.end(old);
        assert!(c.history.is_empty());
    }

    #[test]
    fn gc_watermark_is_the_minimum_not_the_maximum_of_multiple_live_bases() {
        let mut c = Committed::new(map(&[]), 0);
        let old = c.begin();                          // base 0
        c.install(ws(&[b"x"], &[]), vec![]);           // -> 1
        let _mid = c.begin();                          // base 1
        c.install(ws(&[b"y"], &[]), vec![]);           // -> 2
        let young = c.begin();                         // base 2
        c.end(young);                                  // active_bases now {0:1, 1:1} — two keys, genuinely
        assert_eq!(old, 0);
        assert_eq!(c.history.len(), 2, "watermark must be 0 (min), not 1 (max)");
    }
}
