# mork-server

An HTTP + SSE server exposing the MORK graph database's metta-calculus VM
(`Space::metta_calculus`) to multiple concurrent clients.

One verb: **`POST /run`** — submitting a transaction (data + execs) *is* running it.
Execution feedback streams live over **`GET /events`** (Server-Sent Events), and results are
read from lock-free snapshots via **`GET /export`**. There is no separate load step and no
`/count`, `/clear` or `/status` — counting, clearing, and even cancelling a program are all
expressible as ordinary transactions, and status lives on the event stream.

## Running

The workspace requires the nightly toolchain:

```sh
cargo +nightly run --release -p mork-server -- --addr 127.0.0.1:8081
```

| Flag | Default | Meaning |
|---|---|---|
| `--addr` | `127.0.0.1:8081` | Listen address |
| `--events-buffer` | `4096` | Per-subscriber event buffer; slower clients get `lagged` events instead of back-pressuring the engine |
| `--step-budget` | `1000000` | Max VM steps one transaction may run; bounds how long a transaction can pin its base snapshot, and so how much version history the committer has to retain for it to validate against |
| `--budget-action` | `commit` | On budget exhaustion: `commit` keeps partial progress and parks pending execs as `(paused …)` data; `abort` rolls the whole transaction back |
| `--workers` | `1` | Number of transactions that may execute concurrently; `1` = the previous sequential engine, exactly. Capped at the symbol table's writer-thread limit (`MAX_WRITER_THREADS`) |
| `--data-dir` | *(absent)* | Enable persistence: write-ahead log plus checkpoints rooted at this directory, replayed at startup before the listener binds. Absent = pure in-memory (nothing is written, nothing is recovered) |
| `--fsync` | `everysec` | When the log is fsynced: `always` = every batch of commit records is fsynced as it is written · `everysec` = durable within ~1 s (Redis-style; the loss window covers process crash and power failure) · `no` = page cache decides. Note that the 200 does not currently wait on the fsync under any policy (see Durability below) |
| `--checkpoint-every` | `1024` | Snapshot the space and delete pre-checkpoint log segments every N finished transactions; `0` disables (the log grows unbounded, recovery replays it in full). A due checkpoint is deferred while any transaction is in flight, and stays due until one succeeds |

Logging via `env_logger`: `RUST_LOG=info cargo +nightly run -p mork-server`.
Stop with Ctrl-C (open connections are closed, the engine thread is joined).

## Quick start

```sh
# 1. watch everything the VM does (keep this open in a terminal)
curl -N http://127.0.0.1:8081/events

# 2. submit a program (a process-calculus 2+2 adder) — the exact same file
#    also runs unchanged with the CLI: `mork run server/examples/adder.metta`
curl -X POST --data-binary @server/examples/adder.metta http://127.0.0.1:8081/run
# → {"ok":true,"tx":"tx1_si49f8v6","count":6,"version":1}

# 3. after the stream shows `quiescent`, read the result
curl 'http://127.0.0.1:8081/export?pattern=%5B2%5D%20petri%20%5B3%5D%20%21%20result%20%24&template=_1'
# → (S (S (S (S Z))))
```

## Execution model (what a "transaction" means here)

- **Submit = run.** A transaction is a body of MeTTa s-expressions — plain data plus
  `(exec <loc> <patterns> <templates>)` programs. It is applied **atomically** (never
  half-visible) and its execs start executing immediately.
- **Atomic execution.** A transaction either runs to quiescence and commits, or — if the
  interpreter rejects one of its execs, or the load itself fails — it is **rolled back
  entirely** (an O(1) copy-on-write revert): the space is exactly as if the transaction
  never happened. Watch for `quiescent` (committed) vs `abort` (rolled back) on `/events`.
- **Namespacing.** The server rewrites every `(exec L …)` in your upload to
  `(exec (<tx-id> L) …)` — uniformly across data, patterns, and templates, so your
  pattern-matching still works. This gives each transaction a private work queue. The
  wrapper is stripped from everything you see (events, exports); you never observe it.
- **Concurrent execution.** Up to `--workers` transactions run at once, each on its own
  worker thread against its own consistent snapshot of the space — `--workers` is a
  ceiling, not a promise (see *Head-of-line dispatch* below). **Commit order is not
  submission order**: a short transaction submitted later can commit first, and its
  `version` will be the lower one. *Within* your transaction, execs run in plain trie
  order over your locs — your program's own inference control is untouched. One
  transaction runs for at most `--step-budget` steps (see the flags table).
- **Head-of-line dispatch** *(known limitation)*. The committer only looks for newly
  arrived requests at the moment it dispatches or commits something; while anything is in
  flight it blocks on the *results* channel, so a request that arrives mid-flight waits
  for the next commit however many workers are idle. Measured at `--workers 8` with one
  307 ms transaction running and seven workers free: a trivial `(tiny 1)` submitted
  151 ms in waited 158 ms — the remainder of the long transaction, reproducible to the
  millisecond across trials. A steady stream of submissions keeps the pool busy, a
  trickle does not: four ~310 ms transactions submitted together took 1231 ms at
  `--workers 1` and 683 ms at `--workers 8` — 1.8x faster for 8x the workers, not 4x.
  Nothing is lost or reordered; it is purely latency.
- **Concurrent writes, serialized commit.** Workers mutate only their private snapshots,
  in parallel. A single committer thread then validates and installs each finished
  transaction one at a time, which is what makes commit order a total order and gives
  every commit a distinct `version`. Reads (`/export`) and the event stream are served
  from published copy-on-write snapshots and never block on, or are blocked by, execution.
- **Isolation: snapshot isolation with removal-scan validation.** A transaction sees only
  its base snapshot — the state as of the last commit before it was dispatched — never
  another transaction's in-flight work. At commit the committer diffs its final trie
  against that base and checks the result against everything that committed meanwhile:

  | Does **not** conflict | Conflicts |
  |---|---|
  | Disjoint paths | Both wrote the same path, one adding and one removing it |
  | Siblings under a shared prefix (`(edge a b)` vs `(edge a c)`) | A pattern-scoped removal racing an insertion under the same ground prefix (the phantom the `$`-pattern would otherwise silently miss) |
  | Two adds of the same path, or two removes of it (sets are idempotent) | |
  | Read/read | |

  A loser is rolled back entirely and gets a `422` plus an `abort` event carrying one of
  exactly two reasons, verbatim:
  `conflict: a path this transaction wrote was concurrently written` or
  `phantom: a pattern-scoped removal raced a concurrent insertion under the same prefix`.
  The prefix check over-approximates by design: a spurious abort is possible, a missed
  conflict is not.
- **Cancellation is program semantics.** *Within* a running transaction, a RemoveSink exec
  that matches its pending execs deletes them — a program can stop its own chain. Nothing
  of a transaction survives its commit for another transaction to cancel: a worker only
  commits once its whole snapshot has quiesced (or its leftovers were parked as inert
  `(paused …)` data at the budget), so committed state never holds a steppable exec — and
  an in-flight transaction's execs live in another worker's private snapshot, invisible
  either way.
- **CLI compatibility.** Any file that works as `mork run <file>` works unmodified as a
  `POST /run` body: same parser, same exec shape, same semantics, identical results.

---

# API reference

Control responses are JSON with an `"ok"` flag; bulk bodies are raw s-expression text.

## `POST /run`

Submit a transaction and start executing it.

- **Body**: MeTTa s-expression text (UTF-8) — data and/or `(exec …)` programs, exactly the
  CLI's input syntax.
- **Blocks until the transaction reaches its outcome** — load, then every step to
  quiescence (or to a budget/abort outcome), all before the HTTP response is sent. Request
  duration is bounded by `--step-budget`, not by how fast the network is: a long-running
  program holds the connection open for as long as it actually runs, so size any
  client-side HTTP timeout to your `--step-budget`, not to "should be quick". `/events`
  carries the same events for anyone watching rather than waiting on this one response.

```json
{"ok": true, "tx": "tx17_si49f8v6", "count": 6, "version": 42}
```

| Field | Meaning |
|---|---|
| `tx` | Transaction id, format `tx<count>_<unique 8-char alphanumeric>`. Doubles as the loc-namespace symbol |
| `count` | Expressions added by this transaction |
| `version` | Version after this transaction committed — one bump per transaction, not per step |

**Errors**: `400` non-UTF-8 body or s-expression parse error (nothing applied) ·
`422` the transaction was rolled back — kernel load failure, a rejected exec, a conflict
  with a concurrently-committed transaction, or `--budget-action abort` exhausting the
  step budget (nothing applied in any case; no `200` is ever sent for a transaction that
  ends up rolled back — see the lifecycle note under `/events` below) ·
`503` engine shut down.

Examples:

```sh
# just data — loads atomically, no execution starts (no execs)
printf '(edge a b) (edge b c)' | curl -s -X POST --data-binary @- localhost:8081/run

# a "count" transaction (replaces a /count endpoint)
printf '(exec 0 (, (edge $x $y)) (O (count (edges two) 2 (q $x $y))))' \
  | curl -s -X POST --data-binary @- localhost:8081/run

# a "clear this subtree" transaction (replaces a /clear endpoint)
printf '(exec 0 (, (edge $x $y)) (O (- (edge $x $y))))' \
  | curl -s -X POST --data-binary @- localhost:8081/run
```

## `GET /events` — Server-Sent Events

The single live feed of everything the VM does. Standard SSE: `event: <name>` +
`data: <json>` frames; consume with `curl -N`, `EventSource`, or any SSE client.

**Query parameters**

| Param | Meaning |
|---|---|
| `tx=<tx-id>` | Only events belonging to that transaction (global events — `idle`, `delta` — still pass) |
| `deltas=true` | Additionally receive `delta` snapshot-diff events (off by default; costs nothing when nobody subscribes) |

**Events (exhaustive).** Every payload carries `version`, the global step counter, so any
client can totally order what it observes.

| Event | Payload | Meaning |
|---|---|---|
| `hello` | `{version, count, active_txs}` | First frame on connect: snapshot version, expression count, transactions with pending execs |
| `tx` | `{tx, count, version}` | A transaction committed — applied atomically. **Not emitted for a transaction that aborts**: an aborted transaction gets only the `abort` event below, never a `tx` |
| `quiescent` | `{tx, version}` | **Per-transaction**: `tx` committed — nothing steppable left; its effects are permanent. The next queued transaction, if any, starts after this |
| `idle` | `{version}` | **Global**: nothing steppable and nothing queued; the engine is parked awaiting transactions |
| `delta` | `{version, added, removed}` | Opt-in snapshot diff: expressions added/removed between two consecutively observed snapshots (arrays of s-expression strings). Computed off the engine thread with PathMap set algebra; under load several commits may coalesce into one delta |
| `abort` | `{tx, reason, version}` | The transaction was **rolled back** (failed load, rejected exec, a conflict with a concurrently-committed transaction, or budget exhaustion under `--budget-action abort`): the space is exactly as if it never happened. This is the only event such a transaction ever gets — no `tx` precedes it |
| `budget` | `{tx, steps, version}` | The transaction hit `--step-budget` under `--budget-action commit`: partial progress is kept, still-pending execs are parked as inert `(paused (exec …))` data — inspect via `/export`, resume with a follow-up transaction that rewrites them back to `(exec …)` |
| `lagged` | `{skipped}` | *You* consumed too slowly and missed `skipped` events (buffer overrun). Re-sync with `/export`; the engine was never slowed down |

A per-step `step` event existed before workers ran transactions to completion before
commit; it no longer does, because a step that hasn't committed isn't public state. What
you get instead is the transaction's outcome, in full, once it's known: `tx` (or `abort`)
followed by `quiescent`/`budget`/nothing further.

Typical lifecycle of one transaction on the stream:

```
tx → quiescent      committed — effects permanent  (then idle, if nothing is queued)
abort                rolled back — space as if it never happened (no `tx` precedes this)
tx → budget          quiesced by force under --budget-action commit; partial progress kept
```

## `GET /export`

Read from the **latest published snapshot** — consistent, lock-free, never blocked by
execution. Response is s-expression text, one expression per line, with transaction
namespaces stripped. The `x-mork-version` response header carries the snapshot's version.

**Query parameters** (both or neither; values URL-encoded):

| Param | Meaning |
|---|---|
| *(none)* | Dump the entire space |
| `pattern` | Query pattern in the CLI's bracket notation (same as `mork convert --pattern`), e.g. `[2] petri [3] ! result $` |
| `template` | Output template, e.g. `_1` (= the first `$` binding) |

Bracket notation crash course: `[N]` opens an N-ary expression, `$` introduces a variable,
`_k` refers back to the k-th variable. `[2] petri [3] ! result $` matches
`(petri (! result $x))`; template `_1` outputs the binding of `$x`.

```sh
# everything
curl -s localhost:8081/export

# just the adder's result
curl -s 'localhost:8081/export?pattern=%5B2%5D%20petri%20%5B3%5D%20%21%20result%20%24&template=_1'
```

**Consistency note**: exports see the last *committed* transaction (snapshot isolation) —
a slightly stale but always consistent view. To coordinate, compare the `version` on your
`/run` reply or `tx`/`quiescent` events with the `x-mork-version` header.

---

## Operational notes

- **Toolchain**: nightly only (`cargo +nightly`); the workspace has no `rust-toolchain`
  file and fails on stable.
- **Symbol encoding**: the server assumes the kernel's default features (no `interning`).
- **Deep nesting**: expression machinery recurses per nesting level; the engine thread
  reserves a 512 MB stack (lazily committed) and tokio threads 64 MB, so deeply nested
  terms like a 400-deep `(S (S … Z))` are fine.
- **Per-transaction `Space`**: each transaction runs on a worker against its own `Space`,
  built fresh for that transaction and dropped when it ends — not one `Space` shared for
  the process lifetime. A program that opens a `z3` subprocess or memory-maps an ACT file
  gets that resource for its own transaction only; it does not persist into the next one.
- **Runaway programs**: a program whose continuations never stop is bounded, not
  harmless — after `--step-budget` steps it is either quiesced by force (`commit`:
  results kept, continuations parked as `(paused …)` data) or rolled back (`abort`).
  While it runs it *does* hold up the queue, because dispatch is head-of-line (see
  Concurrency above): requests arriving mid-flight wait for it even with workers idle.
  Size the budget to your workload — it is the upper bound both on how long one
  transaction can occupy a worker and on how long a newly arrived request can sit
  unread.
- **Durability** (`--data-dir`): a logical command log — one record per *committed*
  transaction, holding its source text and the version it ran against — with the engine
  never touching a file (a dedicated writer thread owns all I/O and fsync timing).
  Aborted and mid-execution transactions are never logged, so there is nothing to undo:
  recovery replays each committed record against the deterministic VM, rebuilding the
  exact base snapshot that record names and re-running it there, in log order. Same
  text, same base, same trie order, same steps ⇒ byte-identical space; a replay that
  runs a different number of steps or installs at a different version refuses to start
  rather than serve a divergent space. Checkpoints (`--checkpoint-every`) bound recovery
  time: the engine's cost is an O(1) copy-on-write clone; a background thread serializes
  it (compressed `.paths`), installs it atomically (temp + fsync + rename), and deletes
  the log segments it supersedes — recovery then restores the snapshot and replays only
  the tail. Requires the default non-`interning` build. Two rough edges to know about:
  the 200 is sent as soon as the record is handed to the writer thread, so it does not
  imply "fsynced" even under `--fsync always`; and a disk-write error poisons the log
  (further records are dropped rather than written) without yet being surfaced to
  clients — the server keeps committing in memory.
- **Roadmap** (not yet implemented): parallel execution of write-disjoint execs *within*
  one transaction; durable-before-ACK and a 503 on a poisoned log (the WAL's ack path
  exists but the committer does not use it).

## Testing

Black-box Python suite in [`tests/`](tests/) (managed by `uv`; pinned Python 3.13) plus the
Rust unit tests:

```sh
cargo +nightly test -p mork-server        # Rust unit tests (conflict rules, WAL, wrapping)

cd server/tests
uv sync                                   # one-time env setup
uv run pytest                             # e2e + concurrency + recovery + regression
                                          # (builds & spawns the server itself)
uv run ruff check . && uv run ruff format --check . && uv run mypy .   # style + types
```

`test_concurrency.py` covers the isolation claim above at `--workers 4`/`--workers 8`:
eight writers on disjoint paths, and eight more on sibling paths under one shared prefix,
all of which must commit. Overlap is forced rather than hoped for — each submission is
padded with a self-namespaced diverging program capped by `--step-budget`, and each test
asserts the batch beat its own measured solo latency by enough that the transactions must
have been in flight together, so they really did validate against each other. Both tests
fail at `--workers 1` for exactly that reason. Given the overlap, an abort means conflict
detection started approximating by prefix instead of by path. `test_recovery.py` covers
`--data-dir` (kill, respawn, same space).

Regression cases are golden files: drop `<name>.metta` + `<name>.check` (JSON with
`pattern`, `template`, `expected` lines) into `examples/` and pytest picks them up.

Load testing (locust) targets a manually started server:

```sh
cargo +nightly run --release -p mork-server &
cd server/tests
uv run locust --headless -u 50 -r 10 -t 60s --host http://127.0.0.1:8081
uv run locust --host http://127.0.0.1:8081        # or: web UI at :8089
```

Scenarios (each submitter subscribes to `/events` before acting and waits for its own
`quiescent`, reported as a per-workload `quiesce [...]` stat): compute users mixing cheap
one-step relays with multi-step petri-calculus adders on random operands (results verified
via `/export` after quiescent), bulk telemetry ingesters, equi-join users that check the
joined row count, analysts picking `/export` queries at random against the COW snapshots,
and a watcher that reports `lagged` events as failures (backpressure signal). The space
grows monotonically under load (no isolation/GC yet), so RSS growth is by design — watch
it, don't assert on it.
