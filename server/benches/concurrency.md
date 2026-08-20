# Concurrency measurements

What `--workers` actually buys, measured on the mixed short/long scenario in
`server/tests/locustfile.py`: 95% of users write one fact per transaction, 5% submit a
long generative program (`_scan`: 500 items joined with themselves — 250k matches, 500
written paths, ~250 ms on an idle server, ~60x a short write). Both saturate — no think
time — so throughput is the server's and not the wait time's.

The long program's cost is deliberately independent of what is already in the space
(everything it matches on lives under its own namespace, and it writes k paths for k²
units of work). The first candidate, the petri-calculus adder, was not: each committed
20+20 adder made the next one ~145 ms slower on an idle server (201 ms → 1.8 s over twelve
runs), because its `(petri (! …))` / `(petri (? …))` patterns also match every earlier
adder's leftovers. A table indexed by `--workers` under that program measures how much
junk the faster configurations had time to pile up, so it was replaced.

This is the workload the sequential engine handles worst: a long transaction with a queue
of cheap ones behind it. Every path is disjoint by construction (each short write gets its
own fact, each long program its own loc namespace), so the abort column measures what MVCC
does to a workload that *should* have no conflicts, not how it arbitrates a contended one.

## Host

| | |
|---|---|
| CPU | Intel Core Ultra 7 155H — 16 physical cores / 22 logical: 6 P-cores with SMT (CPUs 0-11, 4.5-4.8 GHz), 8 E-cores (12-19, 3.8 GHz), 2 low-power E-cores (20-21, 2.5 GHz) |
| Build | `RUSTFLAGS="-C target-cpu=native"`, `--release`, nightly |
| Server affinity | `taskset -c 12-21` — **10 physical E-cores, no SMT**. One worker gets one real core up to 10; `--workers 16` oversubscribes 1.6x, which is why the table stops mattering there |
| Load affinity | `taskset -c 0-11` — the 6 P-cores, with `locust --processes 8`. The load generator gets the *fast* cores on purpose; see below |
| Contamination | **The box was not idle.** An unrelated interactive application (a game under Proton) held ~1.8 cores throughout every run, unpinned, so it took time from both sides. Every absolute number here is therefore a floor, and run-to-run noise is higher than an idle box would give. It was running for all five worker counts alike, so the *shape* of the scaling curve is still readable; a single row is not worth quoting on its own |

### Why the load generator gets the P-cores, and a discarded round

The first round of this table was thrown away. It ran the server on `0-15` and a
single-process locust on `16-21`, and it reported 102 tx/s at `--workers 1` rising to
142 tx/s at `--workers 4` and then flat at 142 through 8 and 16 — a tidy scaling curve
that was entirely an artifact. A `top` sample mid-run showed **locust at 97.4% of one
core and the server at 18.6%**: locust is single-process gevent, so it was the ceiling,
and the "plateau" was the load generator's, not the engine's.

With the load generator on the P-cores and split across 8 processes, the same
`--workers 16` configuration does **1559 tx/s** instead of 142 — 11x — with the server at
632% CPU. That is the number to trust, and the lesson is worth keeping: on this box a
locustfile whose users each hold an SSE stream costs more CPU than the server it points
at.

Offered load is sufficient at `-u 41`: doubling it to `-u 82` at `--workers 16` *lowered*
throughput to 1359 tx/s while raising server CPU from 632% to 811% — past the knee, all
queueing. The table is measured at `-u 41`.

## Runbook

One worker count per invocation. From the repo root:

```sh
export RUSTFLAGS="-C target-cpu=native"
taskset -c 0-3 cargo +nightly build --release -p mork-server

W=4        # one of 1 2 4 8 16
taskset -c 12-21 ./target/release/mork-server --addr 127.0.0.1:8099 --workers $W &
# history_len is a gauge, not a counter: sample it, don't read it once at the end
while :; do curl -s localhost:8099/stats; echo; sleep 0.2; done > /tmp/stats-w$W.jsonl &

cd server/tests
taskset -c 0-11 uv run locust --headless --processes 8 -u 41 -r 41 -t 60s \
    --host http://127.0.0.1:8099 --csv /tmp/w$W \
    ShortWriteUser LongWriteUser WatcherUser

pkill -x mork-server; kill %2
```

`-u 41` = 38 `ShortWriteUser` + 2 `LongWriteUser` (weights 19:1 — the 95/5 split) + the
one `WatcherUser` (`fixed_count = 1`), which holds a single `/events` stream and tallies
`abort` frames by reason. Aborts are broadcast events, so a second watcher would count
every abort twice; that is why the count is fixed rather than weighted.

Latency is read off the `/run [short]` / `/run [long]` rows — `POST /run` blocks until the
transaction commits, so that request time *is* submit-to-commit. The `quiesce [...]` rows
add the cost of opening a second `/events` connection per submission and are not the
engine's latency.

Throughput is `/run [short]` + `/run [long]` requests per second, cross-checked against
`version` from `/stats` divided by the run time — every commit bumps `version` exactly
once.

## Results

Default build (no `counters`), mixed 95/5 scenario, `-u 41`, 60 s per row, one server
process started fresh per row.

| `--workers` | tx/s | short p50 | short p99 | long p50 | long p99 | aborts | `history_len` peak / steady |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1  | 41   | 1000 ms | 1000 ms | 1000 ms | 1000 ms | 0 | 0 / 0 |
| 2  | 173  | 160 ms  | 520 ms  | 590 ms  | 620 ms  | 0 | 80 / 49 |
| 4  | 1239 | 10 ms   | 31 ms   | 630 ms  | 900 ms  | 0 | 1188 / 419 |
| 8  | 1244 | 10 ms   | 30 ms   | 630 ms  | 810 ms  | 0 | 1145 / 414 |
| 16 | 1237 | 10 ms   | 30 ms   | 620 ms  | 840 ms  | 0 | 1200 / 408 |

Long-transaction sample sizes are 118-196 per row, so the long p99 is close to that run's
maximum rather than a real tail estimate. Short samples are 2.3k-74k.

Every row observed **zero aborts of either kind**, which is what the workload was built to
produce (see the note at the top): every path is disjoint by construction. The abort tally
itself is not vacuous — driving the phantom race from `test_e2e.py` past the same
`WatcherUser` produced `abort [phantom] 6`, matching the six 422s the client saw.

The `--workers 1` row is not a rounding artifact: 41 users divided by 41 tx/s is one
second, so every request in that configuration waited its full turn behind the queue.

**Run-to-run noise is roughly ±20%.** The `--workers 16` configuration measured 1559 tx/s
in a 30 s trial and 1237 tx/s in the 60 s table run half an hour later. Some of that is the
contaminating process; some is the workload itself, which leaves ~74k more paths in the
trie by the end of a 60 s run than a 30 s one. Do not read a difference under ~25% between
two rows as a real difference.

### Copy-on-write amplification (`--features counters`, separate build)

Never mixed into the table above: the counters bump two process-wide atomics on every
structural write, so a build with them on is not the build whose throughput you want.

The mixed scenario is the wrong workload for this question, and the first attempt shows
why. Its transaction *mix* changes with `--workers` — long transactions are 5.00% of
commits at 1 worker and 0.19% at 16, because the sequential engine spends most of its time
in them — and a long transaction writes ~1000 paths against a short one's 1:

| `--workers` | commits | `make_unique_calls` | `cow_clones` | clones/call | long share of commits |
|---:|---:|---:|---:|---:|---:|
| 1  | 1179  | 541,185   | 8,517   | 1.6%  | 5.00% |
| 16 | 47,390 | 1,713,100 | 409,647 | 23.9% | 0.19% |

That 1.6% → 23.9% looks like amplification climbing steeply with worker count. It is not.
Re-run with the long transactions removed entirely — short writes only, so the mix is
identical on both sides and the two runs even land on the same commit count:

| `--workers` | commits | `make_unique_calls` | `cow_clones` | clones per commit | clones/call |
|---:|---:|---:|---:|---:|---:|
| 1  | 53,587 | 1,034,511 | 415,055 | 7.74 | 40.1% |
| 16 | 53,543 | 953,045   | 414,641 | 7.74 | 43.5% |

Clones per commit are **identical to three significant figures** at 1 and 16 workers. Per
committed transaction, concurrency costs this workload nothing extra in copy-on-write.

### Side probes

Three measurements that are not rows in the table but decide how to read it.

**Per-thread CPU, `--workers 16`, mixed scenario** (`top -bH`, 15 s into a run): the eight
tokio runtime threads sat at 40-50% each (~350% total), the committer thread at **14.8%**,
and the sixteen worker threads at 0-5% each. The front end costs an order of magnitude
more CPU than the committer it feeds. That is mostly SSE fan-out: at ~1240 commits/s with
~41 subscribers, the broadcast formats and writes ~100k frames per second.

**The same scenario with no `/events` subscription at all** (a throwaway locustfile;
`POST /run` already blocks until commit, so the stream costs no fidelity):

| | `--workers 4` | `--workers 16` |
|---|---:|---:|
| tx/s | 6186 | 6332 |
| short p50 / p99 | 5 / 10 ms | 4 / 11 ms |

Five times the table's throughput, from the same engine. So the table's plateau from 4
workers on is **the HTTP and SSE front end, not the transaction engine**.

**And that 6300 tx/s is the load generator's ceiling too, not the server's.** Mid-run,
all eight locust processes were pegged at 93-102% CPU while the server drew 314% of the
1000% available to it; the server's own busiest threads were the two worker threads
running the two long transactions (98% each) with the committer at 29.5%. Doubling the
users to 80 changed throughput by 0.1% (6251 tx/s). This box cannot offer enough load to
find where the engine stops scaling.

## What the data supports

Spec §6 defers three decisions pending these numbers. Taking its four candidate readings
in turn:

**"Abort rate low, throughput scales → the MVCC work is done." — Half supported, and the
supported half is weak evidence.** Aborts were zero in all five rows, but the workload was
built to be conflict-free, so that measures the absence of *false* conflicts, not conflict
handling. It is a real result — path-granular validation raised no spurious abort in
~170,000 committed transactions across five configurations — and it is the same claim
`test_concurrency.py` makes, now at load. The throughput half is **not** established:
scaling is unmeasurable past 4 workers on this box, because the front end saturates at
~1240 tx/s with the scenario's SSE subscriptions and the load generator saturates at
~6300 tx/s without them, both below wherever the engine's own limit is.

**"Long transactions starve → the pessimistic path in spec §6 is justified." — Not
supported, and the opposite effect is the headline number.** No long transaction aborted,
ever; nothing starved in the sense the pessimistic path exists to fix. What the table does
show is the motivation for MVCC itself, in one line: **at 1 worker this workload runs at
41 tx/s, at 4 workers it runs at 1239 tx/s — 30x for 4x the workers.** Nothing about the
engine got 30x faster. The 5% of transactions that are long occupied the only worker there
was, and the 95% queued behind them; short p50 falls from 1000 ms to 10 ms across the same
step. The cliff sits between 2 and 4 workers, which is exactly where worker count passes
the number of concurrently-submitting long users (two): below that, a long transaction is
always occupying a worker.

**"Amplification dominates → investigate node-level sharing before adding workers." —
Ruled out** for this workload, by the cleanest measurement here. Clones per commit are
7.74 at both 1 and 16 workers; the clone *ratio* moves 40.1% → 43.5%. Concurrency is not
what makes copy-on-write expensive. Note what this does **not** cover: every writer here
touches a distinct path, so it says nothing about workloads where concurrent writers share
trie nodes deep in a hot prefix.

**"Commit step dominates → parallel install via `ZipperHead`." — Ruled out at these rates,
and worth re-checking before anyone acts on it.** The committer thread ran at 14.8% of one
core while serializing ~1240 commits/s, and at 29.5% while serializing ~6300/s. It is the
one genuinely serial stage in the design and it is nowhere near being the constraint;
`[[LockContention]]` warns that the tracker registry scales negatively, and nothing here
argues for paying that. Straight-lining 29.5% at 6300 commits/s puts the committer's own
ceiling somewhere above 20k commits/s for this writeset shape — an extrapolation, not a
measurement, and the writeset shape is the caveat: these are 1-path and 1000-path
writesets, not the wide ones a bulk loader would produce.

### What would settle the open half

The unresolved question is where throughput stops scaling with `--workers`, and it is
unresolved for want of a load generator, not for want of an engine. What it needs:

1. **A load generator that is not the bottleneck.** Locust with `requests` costs more CPU
   per transaction than the server does. A separate machine, or a Rust/Go driver, or an
   `h2`/pipelining client — anything that can offer >10k tx/s without eating the box.
2. **A dedicated host.** Both sides of every number here shared 22 logical CPUs with each
   other and with ~1.8 cores of unrelated interactive load.
3. **A workload whose per-transaction server cost is larger than its HTTP cost.** A
   single-fact write is a few microseconds of engine work wrapped in a whole HTTP request;
   at that ratio the front end will always saturate first. The long side of this scenario
   is the right shape; the short side measures hyper, not the VM.
4. **A contended variant.** Everything above measures a conflict-free workload. The abort
   *rate* under real contention — and whether it concentrates in long transactions, which
   is what would actually justify the pessimistic path — is untested at load. The
   ingredients exist: `test_e2e.py`'s phantom race forces conflicts at will, and the
   `WatcherUser` tally is verified to count them.

### Raw summaries

```
--- workers=1 ---
tx/s (locust)          41.0     tx/s (version/60s)     41.5   commits=2487
short  n=  2329  p50=1000ms p99=1000ms max=1024ms
long   n=   118  p50=1000ms p99=1000ms max=1025ms
aborts none   failures=0
history_len peak=0 median(steady)=0 last=0 samples=293
```
```
--- workers=2 ---
tx/s (locust)         172.5     tx/s (version/60s)    172.5   commits=10352
short  n= 10116  p50=160ms p99=520ms max=542ms
long   n=   196  p50=590ms p99=620ms max=622ms
aborts none   failures=0
history_len peak=80 median(steady)=49 last=0 samples=293
```
```
--- workers=4 ---
tx/s (locust)        1239.3     tx/s (version/60s)   1240.5   commits=74431
short  n= 74256  p50=10ms p99=31ms max=77ms
long   n=   173  p50=630ms p99=900ms max=937ms
aborts none   failures=0
history_len peak=1188 median(steady)=419 last=0 samples=280
```
```
--- workers=8 ---
tx/s (locust)        1243.6     tx/s (version/60s)   1244.9   commits=74694
short  n= 74517  p50=10ms p99=30ms max=62ms
long   n=   175  p50=630ms p99=810ms max=831ms
aborts none   failures=0
history_len peak=1145 median(steady)=414 last=0 samples=281
```
```
--- workers=16 ---
tx/s (locust)        1236.5     tx/s (version/60s)   1237.8   commits=74266
short  n= 74089  p50=10ms p99=30ms max=70ms
long   n=   175  p50=620ms p99=840ms max=912ms
aborts none   failures=0
history_len peak=1200 median(steady)=408 last=0 samples=280
```

### Raw side probes

```
no-SSE probe workers=4   tx/s= 6186.2  short p50=5ms p99=10ms  long n=99  p50=600ms  fails=0
no-SSE probe workers=16  tx/s= 6332.3  short p50=4ms p99=11ms  long n=100 p50=600ms  fails=0
no-SSE u=80  workers=16  tx/s= 6250.8  short p50=9ms p99=18ms  long p50=670ms
mixed  u=82  workers=16  tx/s= 1358.7  short p50=20ms p99=67ms long p50=720ms p99=1200ms  (server CPU 811%)
mixed  u=41  workers=16  tx/s= 1559.4  (30 s trial, same config as the 60 s table row that read 1237)

counters, mixed scenario, 30 s:
  workers=1   {"cow_clones":8517,"make_unique_calls":541185,"version":1179}    short=1082 long=57
  workers=16  {"cow_clones":409647,"make_unique_calls":1713100,"version":47390} short=47301 long=88
counters, short writers only, 30 s:
  workers=1   {"cow_clones":415055,"make_unique_calls":1034511,"version":53587}
  workers=16  {"cow_clones":414641,"make_unique_calls":953045,"version":53543}

abort-tally verification (server --workers 4 --step-budget 50, the phantom race from
test_e2e.py driven at 8 users for 20 s, watched by the same WatcherUser):
  SSE,abort [phantom],6,0        # and 6 client-side 422s, so the tally is exact
```

### Per-thread CPU samples

```
--workers 16, mixed scenario (top -bH, 15 s in):
  tokio-runtime x8   40-50% each   (~350% total)
  mork-engine        14.8%         (the committer)
  mork-worker x16    0-5% each

--workers 16, no-SSE probe (top -bH, 15 s in):
  mork-worker        98.4%  \
  mork-worker        98.4%  /  the two long transactions
  mork-engine        29.5%
  tokio-runtime      5-15% each (~80% total)
  locust x8          93-102% each — the load generator is what is saturated
```
