# `server` vs `metta-calculus-server`: mixed short/long workload, native kernels

> Measured 2026-09-01, in one session, on the host described below. Committed after the
> fact: the harness and this write-up were recovered from that session's transcript, and
> the raw locust CSVs and server logs it refers to were lost with the `/tmp` scratchpad
> they were written to. Every number below is what the write-up recorded at the time; none
> of it has been re-measured since. Re-running it means re-running the two sweep scripts
> in this directory.

Both branches run their own actual server binary, built from their own actual kernel,
unmodified. The kernels are **not** held constant between rows (that confound was
named and deliberately not eliminated, per direction) — a difference between the two
tables below is a difference between "MVCC + its kernel" and "prefix-locking + its
kernel" as shipped, not an isolated server-design effect. See Caveats.

Host: Intel Core Ultra 7 155H, 22 logical cores (6 P-cores w/ SMT + 8 E-cores + 2
low-power E-cores) — the same machine `server/benches/concurrency.md` was measured on.
Build: `RUSTFLAGS="-C target-cpu=native"`, `--release`, nightly, compiled under
`taskset -c 0-3` (repo convention). Server pinned to cores 12-21 (10 reserved cores,
matching `concurrency.md`); locust (`--processes 8 -u 41 -r 41`) pinned to 0-11. 30s per
row (half of `concurrency.md`'s 60s, to fit this measurement session) instead of 60s.
Workload: `server/tests/locustfile.py`'s mixed 95/5 `ShortWriteUser`/`LongWriteUser`
scenario (one fact per short transaction; `_scan`-shaped 500-item self-join per long
transaction), ported onto the `server` branch's very different wire format — see
`server_branch_locustfile.py` in this directory for the port and why two early drafts
of it were wrong.

## metta-calculus-server (optimistic MVCC)

`POST /run` blocks until commit, so its own request latency **is** submit-to-commit
latency — no polling needed, no failures at any row.

| `--workers` | short tx/s | short p50 | short p99 | long tx/s | long p50 | long p99 |
|---:|---:|---:|---:|---:|---:|---:|
| 1  | 129  | 300 ms | 350 ms | 6.4 | 300 ms | 350 ms |
| 2  | 334  | 170 ms | 220 ms | 10.0 | 190 ms | 220 ms |
| 4  | 2169 | 6 ms   | 20 ms  | 7.6 | 230 ms | 330 ms |
| 8  | 2159 | 6 ms   | 20 ms  | 7.5 | 240 ms | 340 ms |
| 16 | 2179 | 5 ms   | 20 ms  | 7.6 | 230 ms | 340 ms |

Zero failures, zero aborts, zero `lagged` frames at every row (matches
`concurrency.md`'s own finding that this workload is disjoint by construction).
`version` delta from `/stats` cross-checked against `(short+long)` request counts at
every row and matched within ~1%.

**The `--workers 1` row here measures ~136 tx/s total, not the 41 tx/s `concurrency.md`
originally reported for the same configuration.** Treat this fresh table, not the old
one, as current.

> **Superseded explanation.** This paragraph originally attributed the gap to
> `/events`-stream round-trip overhead in the original measurement. That guess does not
> hold: `concurrency.md` reads its latency off the `/run` rows, not the `quiesce` rows, so
> no second connection is in the timed path. The later five-row re-verification found
> *every* row elevated and long-transaction p50 down across all of them, which points at
> host contamination during the original session instead. See the warning block in
> `../concurrency.md`.

## server (pessimistic prefix-locking)

`GET /metta_thread` dispatches and returns immediately — it does not block for
completion. Long-transaction completion is detected by polling
`GET /status/(exec%20<loc>)` at 10ms intervals, so long-path latency here carries an
extra poll-quantization tail the MVCC branch's single blocking `/run` doesn't. There is
no `--workers` flag; concurrency is varied by CPU affinity (`taskset`), which is the
literal mechanism sizing this branch's admission-control ceiling
(`WorkerPool::thread_count = tokio_worker_threads - 1`) — not an approximation of a
missing flag. A single core can't boot the process at all (`assert!(thread_count >= 1)`
fails), so the "1" row uses 2 cores, the practical floor.

Every command that touches the space (`/upload`, `/metta_thread`) consumes an
admission-control worker-pool slot; past that fixed slot count the server returns `503`
immediately rather than queuing. At 41 offered concurrent users with a non-retrying
client, most of that offered load is rejected outright at low core counts:

| cores (target N) | short attempted | short succ. | short succ. tx/s | short fail % | long attempted | long succ. | long succ. tx/s | long fail % |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 (N=1)  | 249,547 | 13,741  | 458   | 94.5% | 5,022 | 80  | 2.7 | 98.4% |
| 2 (N=2)  | 248,992 | 15,609  | 520   | 93.7% | 5,087 | 81  | 2.7 | 98.4% |
| 4 (N=4)  | 296,011 | 171,113 | 5,704 | 42.2% | 263   | 146 | 4.9 | 44.5% |
| 8 (N=8)  | 327,318 | 304,253 | 10,142| 7.1%  | 166   | 142 | 4.7 | 14.5% |
| 10 (N=16, capped) | 312,565 | 279,668 | 9,322 | 10.5% | 180 | 136 | 4.5 | 24.4% |

Latency percentiles are **not reported as a clean success-only number** here — locust's
default CSV mixes near-instant `503` rejections in with real completions, and at low
core counts up to ~98% of samples are rejections, so the raw p50/p99 columns (3-8ms for
short, dominated by instant-503s at N≤2) would misrepresent successful-transaction
latency if taken at face value. The tx/s-of-successes columns above are the trustworthy
number at this workload; a follow-up that logs success/failure latency separately would
be needed to talk about server-branch tail latency honestly. The one thing crossing that
gap: the eight-core row's few hundred long successes clustered at 420-450ms p50-p99,
which is at least the right order of magnitude for a 500-item self-join.

## Reading the two tables together

- **When it isn't rejecting, `server` completes short writes faster in raw terms**:
  10,142 succ. tx/s at 8 cores vs. MVCC's plateau of ~2,170 tx/s. Per-write cost is
  lower with no MVCC bookkeeping (no COW clone, no post-hoc conflict check) when the
  write's own prefix lock is uncontended — which the corrected harness makes true here,
  same as MVCC's short writes are disjoint by construction.
- **But `server`'s admission control doesn't degrade gracefully under this offered
  load**: at 41 concurrent non-retrying submitters, it rejects 94% of writes on 2 cores
  and still 7-11% on 8-10 cores, where MVCC rejects nothing at any `--workers` setting
  from 1 to 16 (it queues instead of rejecting, at the cost of the `--workers 1` and `2`
  rows' much higher per-request latency).
- **These are different points on a design tradeoff, not one branch strictly beating
  the other**: fail-fast-and-retry vs. queue-and-wait. A real client against `server`
  would need its own retry/backoff logic to get anything like MVCC's zero-failure
  behavior, and that retry logic's cost isn't measured here — this table shows what a
  naive non-retrying client sees, not the best either branch can do.
- Long-transaction throughput is low and roughly flat on both branches (~5-10/s) since
  it's 5% of a 41-way mix either way — not a meaningful basis for comparison at this
  sample size (matches `concurrency.md`'s own note that its long-sample n is small).

## Caveats (read before quoting a number from this file)

1. **Kernel not held constant.** By explicit direction this session, no attempt was
   made to isolate the kernel rewrite from the server-design difference. Every number
   above reflects "MVCC + its own kernel" vs. "prefix-locking + its own kernel" as they
   actually ship, not a controlled ablation.
2. **`/upload` commit vs. `/run` commit are not identical semantics.** `server` has no
   MVCC snapshot concept; a completed `/upload` is immediately live under its prefix
   lock. `/run`'s completion means "visible in the next COW snapshot." Both are
   legitimate "this write is durable/visible" signals but they are not the same
   guarantee.
3. **taskset-based concurrency on `server` is not CPU-isolated the way MVCC's is.** MVCC
   always keeps its full 10-core allocation (12-21) regardless of `--workers`; only an
   internal logical cap changes. `server`'s low-N rows genuinely starve the *entire*
   process (HTTP handling included) down to as few as 2 cores — so its low-N numbers
   reflect "this whole server on 2 cores," not just "this server's compute concurrency
   capped at ~1."
4. **The 503 rejection rate is a property of this specific harness's client (41-way
   offered load, no retry, no backoff), not a fixed constant of the `server` branch.**
   A different offered load or a retrying client would likely show a different curve.
5. **Long-transaction sample sizes are small** (80-146 successes per row on `server`;
   192-298 on MVCC) — treat p99/tail numbers on the long workload as indicative, not
   precise, on both branches.
6. **Two harness bugs were caught and fixed during this run, not before**: an initial
   `server`-branch short-write upload used `pattern=template=$x`, which locks the
   *entire space root* per write (the constant-prefix derivation on a bare variable
   template is empty) — this produced ~100% spurious 401 conflicts across the whole
   population and was not a finding about the branch, just a wrong URL. Fixed by giving
   every upload's pattern/template a per-request unique constant head. A "target N=1"
   row (`taskset -c 12`, one core) also crashed the server outright
   (`WorkerPool::new()`'s `assert!(thread_count >= 1)`) before any request was sent;
   the "N=1" row above is actually 2 cores, the real floor.
7. **This session's `--workers 1` MVCC re-measurement (~136 tx/s) contradicts
   `concurrency.md`'s original ~41 tx/s row for the same nominal configuration** — see
   above. **Resolved the same day:** `concurrency.md`'s exact original runbook was
   re-run at full 60s and reproduced 137.6 tx/s, not 41 -- and re-running all five rows
   found every one of them elevated, so the discrepancy is not specific to `--workers 1`.
   That document's table now carries a warning marking it superseded.

Raw data -- `results/mvcc-w{1,2,4,8,16}_stats.csv`,
`results/server-w{1,2,4,8,16}_stats.csv`, `results/*_failures.csv`, `results/*.server.log`,
`results/*.locust.log` -- was written to the measurement session's `/tmp` scratchpad and
did not survive it. The tables above are the only record. The sweep scripts here write the
same layout into whatever results directory they are given, so a re-run regenerates it.
