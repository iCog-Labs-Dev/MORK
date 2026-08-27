# ECAN shifting/drifting POC: verified design map

This note records the base-branch interfaces that constrain the POC. The
implementation remains intentionally split between read-only candidate selection
and MM2-owned mutation.

## Verified runtime mapping

- `weighted-atom-sweep/src/traversal.rs` defines a read-only `TraversalEngine`.
  Engines receive an immutable `PathMap<u64>` and return one complete encoded
  value path. `snapshot_changed()` is the reset hook for snapshot-local state.
- `weighted-atom-sweep/src/sweep.rs` publishes immutable `(PathMap, version)`
  snapshots to workers and tags emitted `AtomCandidate`s with `ProcessId` and
  snapshot version.
- `kernel/src/sources.rs` turns a selected candidate into a normal `WAS` source.
  The candidate remains an encoded fact path; operation semantics are not carried
  in the candidate.
- `kernel/src/scheduler.rs` indexes persistent `exec` rules by their structural
  `WAS` source, applies background work serially, restores the consumed rule, and
  publishes completed snapshots.
- `kernel/src/sinks.rs` already provides aggregate-safe add/remove sinks and the
  grounded `pure` sink. No ECAN-specific sink is required or permitted.
- `kernel/resources/grounding.mm2` verifies that numeric inputs bound by source
  patterns can be converted, combined by grounded functions, converted back to a
  symbol, and inserted through `pure`.

## MM2 grammar and ownership

The verified executable form is:

```lisp
(exec location
  (I (WAS process-id pattern) (BTM other-pattern) ...)
  (O (- old-fact) (pure new-template result-variable expression) ...))
```

Foreground stimulation uses concrete `BTM` request/state/parameter matches and no
`WAS` source. Diffusion and rent use a `WAS` source only to select the source STI
fact; all formulas and mutations remain in persistent MM2 rules using existing
sinks and grounded numeric functions. MORK is the only writer.

## Snapshot STI representation

Semantic STI is the ordinary fact `(STI atom value)`, not the hidden PathMap
weight. The shared WAS index scans complete values in each immutable snapshot and
accepts only exact, ground, symbol-valued, finite, nonnegative STI facts. It stores
owned bytes for the complete fact path and atom expression, so no references can
outlive the snapshot passed to the scan. The cache is discarded on
`snapshot_changed()` and rebuilt lazily on the next request.

This scan is O(number of stored values) per observed snapshot. Cached dynamic STI
aggregation in the trie is explicitly outside this POC.

