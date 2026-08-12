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
| `--step-budget` | `1000000` | Max VM steps one transaction may run; bounds how long a transaction can hold the (sequential) engine |
| `--budget-action` | `commit` | On budget exhaustion: `commit` keeps partial progress and parks pending execs as `(paused …)` data; `abort` rolls the whole transaction back |
| `--sweep-steps-per-cycle` | `1` | Source/sink sweep passes per cooperative scheduler cycle |
| `--sweep-metta-steps` | `32` | Whole-space metta-calculus steps after each weighted sweep batch |
| `--sweep-idle-ms` | `10` | Backoff when an active source/sink sweep cycle changes nothing |
| `--data-dir` | *(absent)* | Enable persistence: WAL + crash recovery rooted here. Absent = pure in-memory |
| `--fsync` | `everysec` | When the log is fsynced: `always` = 200 means on disk (group-committed) · `everysec` = durable within ~1 s (Redis-style; the loss window covers process crash and power failure) · `no` = page cache decides |
| `--checkpoint-every` | `1024` | Snapshot the space and delete pre-checkpoint log segments every N finished transactions; `0` disables (the log grows unbounded, recovery replays it in full) |

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
- **Sequential scheduling.** Transactions execute strictly one at a time, in submission
  order: the running transaction steps to quiescence before the next queued one is even
  applied. (This is what makes atomic rollback sound — nothing else runs in between that
  could observe reverted state.) *Within* your transaction, execs run in plain trie order
  over your locs — your program's own inference control is untouched. A transaction holds
  the engine for at most `--step-budget` steps (see the flags table).
- **Serial writes, parallel reads.** All mutation happens on one engine thread, one step at
  a time (interleaving is turn-taking, never concurrent writes). Reads (`/export`) and the
  event stream are served in parallel from copy-on-write snapshots and never block on, or
  are blocked by, execution.
- **Isolation: serial.** All transactions share one space's data region, but because
  execution is sequential a transaction only ever sees the *committed* results of its
  predecessors — never another program mid-flight. (MVCC snapshots are future work.)
- **Cancellation is program semantics.** *Within* a running transaction, a RemoveSink exec
  that matches its pending execs deletes them — a program can stop its own chain. (With
  sequential scheduling nothing of a transaction survives past its commit for another
  transaction to cancel; namespace wrapping keeps it that way.)
- **CLI compatibility.** Any file that works as `mork run <file>` works unmodified as a
  `POST /run` body: same parser, same exec shape, same semantics, identical results.

---

# API reference

Control responses are JSON with an `"ok"` flag; bulk bodies are raw s-expression text.

## `POST /run`

Submit a transaction and start executing it.

- **Body**: MeTTa s-expression text (UTF-8) — data and/or `(exec …)` programs, exactly the
  CLI's input syntax.
- **Returns immediately** (execution continues in the background; watch `/events`):

```json
{"ok": true, "tx": "tx17_si49f8v6", "count": 6, "version": 42}
```

| Field | Meaning |
|---|---|
| `tx` | Transaction id, format `tx<count>_<unique 8-char alphanumeric>`. Doubles as the loc-namespace symbol |
| `count` | Expressions added by this transaction |
| `version` | Global step counter after the (atomic) load |

**Errors**: `400` non-UTF-8 body or s-expression parse error (nothing applied) ·
`422` kernel load failure (rolled back — nothing applied) ·
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

## Sweep Control

`POST /sweep/start` parses registered `(sweep ...)` atoms. Legacy operation sweeps still
spawn background WAS controllers; source/sink sweeps start a cooperative scheduler on the
engine thread:

```
weighted sweep emits event atoms -> bounded metta_calculus steps -> repeat
```

The lifecycle endpoints are:

```sh
curl -s -X POST localhost:8081/sweep/start
curl -s -X POST localhost:8081/sweep/pause
curl -s -X POST localhost:8081/sweep/resume
curl -s -X POST localhost:8081/sweep/stop
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
| `tx` | `{tx, count, version}` | A transaction was applied atomically |
| `step` | `{tx, exec, touched, new, us, version}` | One VM step ran for `tx`. `exec` = the s-expression that executed (namespace unwrapped), `touched` = template instantiations written, `new` = whether anything not already present was written, `us` = duration in microseconds |
| `quiescent` | `{tx, version}` | **Per-transaction**: `tx` committed — nothing steppable left; its effects are permanent. The next queued transaction, if any, starts after this |
| `idle` | `{version}` | **Global**: nothing steppable and nothing queued; the engine is parked awaiting transactions |
| `delta` | `{version, added, removed}` | Opt-in snapshot diff: expressions added/removed between two consecutively observed snapshots (arrays of s-expression strings). Computed off the engine thread with PathMap set algebra; under load several steps may coalesce into one delta |
| `abort` | `{tx, reason, version}` | The transaction was **rolled back** (failed load, rejected exec, or budget exhaustion under `--budget-action abort`): the space is exactly as if it never happened |
| `budget` | `{tx, steps, version}` | The transaction hit `--step-budget` under `--budget-action commit`: partial progress is kept, still-pending execs are parked as inert `(paused (exec …))` data — inspect via `/export`, resume with a follow-up transaction that rewrites them back to `(exec …)` |
| `lagged` | `{skipped}` | *You* consumed too slowly and missed `skipped` events (buffer overrun). Re-sync with `/export`; the engine was never slowed down |

Typical lifecycle of one transaction on the stream:

```
tx → step → step → … → quiescent        committed  (then idle, if nothing is queued)
tx → step → step → … → abort            rolled back — space as if it never happened
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

**Consistency note**: exports see the last *committed* step (snapshot isolation) — a
slightly stale but always consistent view. To coordinate, compare the `version` on your
`/run` reply or `step` events with the `x-mork-version` header.

---

## Operational notes

- **Toolchain**: nightly only (`cargo +nightly`); the workspace has no `rust-toolchain`
  file and fails on stable.
- **Symbol encoding**: the server assumes the kernel's default features (no `interning`).
- **Deep nesting**: expression machinery recurses per nesting level; the engine thread
  reserves a 512 MB stack (lazily committed) and tokio threads 64 MB, so deeply nested
  terms like a 400-deep `(S (S … Z))` are fine.
- **Runaway programs**: a program whose continuations never stop can't wedge the queue —
  after `--step-budget` steps it is either quiesced by force (`commit`: results kept,
  continuations parked as `(paused …)` data) or rolled back (`abort`). Size the budget to
  your workload: it's the upper bound on how long one transaction can hold the engine.
- **Durability** (`--data-dir`): a logical command log — transaction sources plus
  commit/abort outcome records — with the engine never touching a file (a dedicated
  writer thread owns all I/O and fsync timing; under `--fsync always` it also fires the
  client's 200 after the group-commit fsync). Recovery replays the log against the
  deterministic VM: same text, same trie order, same steps ⇒ byte-identical space. A
  transaction killed mid-execution (logged but no outcome) is re-run fresh on startup,
  before the listener binds. Checkpoints (`--checkpoint-every`) bound both: the engine's
  cost is an O(1) copy-on-write clone; a background thread serializes it (compressed
  `.paths`), installs it atomically (temp + fsync + rename), and deletes the log
  segments it supersedes — recovery then restores the snapshot and replays only the
  tail. A disk-write error poisons the log: writes get 503, reads keep serving.
  Requires the default non-`interning` build.
- **Roadmap** (not yet implemented): MVCC snapshots/isolation, then parallel execution
  of write-disjoint execs.

## Testing

Black-box Python suite in [`tests/`](tests/) (managed by `uv`; pinned Python 3.13) plus the
Rust unit tests:

```sh
cargo +nightly test -p mork-server        # Rust unit tests (wrap.rs)

cd server/tests
uv sync                                   # one-time env setup
uv run pytest                             # e2e + regression (builds & spawns the server itself)
uv run ruff check . && uv run ruff format --check . && uv run mypy .   # style + types
```

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
