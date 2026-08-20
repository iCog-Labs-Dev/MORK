"""Load scenarios for a manually started mork-server.

    cargo +nightly run --release -p mork-server &
    uv run locust --headless -u 50 -r 10 -t 60s --host http://127.0.0.1:8081

A mixed, randomized workload modelled on the kernel's own test programs:

- ``ComputeUser``  — one-step relays and multi-step petri-calculus adders with random
  operands; waits for its own ``quiescent`` on a live event stream, then verifies the
  adder's result via ``/export`` (correctness under load, not just liveness).
- ``IngestUser``   — write-heavy: batches of 20–100 random telemetry facts per POST.
- ``JoinUser``     — loads two small random tables plus an equi-join exec, then checks
  the joined row count.
- ``AnalystUser``  — read path: picks an ``/export`` query at random (full dump,
  telemetry, relay outputs, adder results) against the COW snapshots.
- ``WatcherUser``  — holds a long-lived ``/events`` stream (randomly plain or
  ``deltas=true``), reports every ``lagged`` frame as a failure (backpressure signal),
  and tallies every ``abort`` frame by reason. Exactly one is ever spawned.

Every submitting user subscribes before acting (the broadcast has no replay) and each
per-workload time-to-quiescent shows up as its own ``quiesce [...]`` stat. The space
grows monotonically under load (no isolation/GC yet — MVCC is future work), so RSS
growth is by design.

**Second scenario — mixed short/long** (``ShortWriteUser`` + ``LongWriteUser``): 95% of
users write a single fact, 5% submit a long generative program. This is the shape the
sequential engine handles worst — one long transaction ahead of a queue of cheap ones —
so it is the shape worth measuring per ``--workers``. Both saturate (no think time), so
throughput is the server's, not the wait time's. See ``server/benches/concurrency.md``.

Name the classes you want, because the two scenarios are not meant to be mixed::

    uv run locust --headless -u 50 -r 10 -t 60s --host http://127.0.0.1:8081 \
        ComputeUser IngestUser JoinUser AnalystUser WatcherUser
    uv run locust --headless --processes 8 -u 41 -r 41 -t 60s \
        --host http://127.0.0.1:8081 ShortWriteUser LongWriteUser WatcherUser
"""

import itertools
import random
import time

from locust import HttpUser, between, constant, task
from requests import exceptions as requests_exceptions

from client import iter_sse

_ids = itertools.count()  # locust workers are gevent greenlets, not threads: count() is safe


def _uid() -> int:
    """Unique per submission; random low bits keep distributed workers collision-free."""
    return (next(_ids) << 16) | random.randrange(1 << 16)


def _peano(n: int) -> str:
    return "(S " * n + "Z" + ")" * n


def _relay(n: int) -> str:
    """One-step seed→out rewrite: the cheapest possible exec."""
    return f"(exec go (, (seed {n} $x)) (, (out {n} $x)))\n(seed {n} 1)"


def _adder(n: int, x: int, y: int) -> str:
    """The examples/adder.metta petri-calculus program, channels namespaced by `n` so
    concurrent instances with different operands can't cross-talk in the shared dish."""
    return f"""(exec (IC 0 1 {_peano(3 * (x + y) + 4)})
               (, (exec (IC $x $y (S $c)) $sp $st)
                  ((exec $x) $p $t))
               (, (exec (IC $y $x $c) $sp $st)
                  (exec (R $x) $p $t)))
((exec 0)
      (, (petri (! $channel $payload))
         (petri (? $channel $payload $body)) )
      (, (petri $body)))
((exec 1)
      (, (petri (| $lprocess $rprocess)))
      (, (petri $lprocess)
         (petri $rprocess)))
(petri (? (add{n} $ret) ((S $x) $y) (| (! (add{n} (PN $x $y)) ($x $y))
                                       (? (PN $x $y) $z (! $ret (S $z)))  )  ))
(petri (? (add{n} $ret) (Z $y) (! $ret $y)))
(petri (! (add{n} result{n}) ({_peano(x)} {_peano(y)})))"""


def _telemetry(n: int) -> str:
    """A batch of 20–100 unique sensor readings across a small set of sites."""
    return "\n".join(
        f"(sensor site{random.randrange(20)} dev{n}-{i} {random.randrange(1000)})"
        for i in range(random.randint(20, 100))
    )


def _equi_join(n: int, emps: int, depts: int) -> str:
    """Two small tables plus a join exec (cross_join_tuple from the kernel suite, made an
    equi-join): every employee matches exactly one location, so `emps` joined rows."""
    rows = [f"(emp {n} e{i} d{i % depts})" for i in range(emps)]
    rows += [f"(loc {n} d{i} city{i})" for i in range(depts)]
    rows.append(
        f"(exec go (, (emp {n} $name $dept) (loc {n} $dept $city)) (, (joined {n} $name $city)))"
    )
    return "\n".join(rows)


class _Submitter(HttpUser):
    """Shared submit flow: subscribe, POST, wait for own quiescent on the stream."""

    abstract = True
    wait_time = between(0.1, 1.0)

    def submit_and_wait(self, kind: str, src: str) -> bool:
        """Returns True when the transaction reached quiescent; reports time-to-quiescent
        as a `quiesce [<kind>]` stat either way."""
        start = time.monotonic()
        exc: Exception | None = None
        # plain requests semantics (no with-block: locust reserves those for catch_response)
        stream = self.client.get(
            "/events", stream=True, timeout=(5, 60), name="/events [submit watch]"
        )
        try:
            sse = iter_sse(stream.iter_lines(decode_unicode=True))
            next(sse)  # hello — subscription is live before we act
            resp = self.client.post("/run", data=src.encode(), name=f"/run [{kind}]")
            if resp.status_code != 200:
                return False  # locust already recorded the failure
            tx = str(resp.json()["tx"])
            for ev in sse:
                if ev.name == "quiescent" and ev.data.get("tx") == tx:
                    break
                if ev.name == "lagged":
                    raise RuntimeError("lagged while waiting for quiescent")
        except (requests_exceptions.RequestException, RuntimeError) as e:
            exc = e
        finally:
            stream.close()
        self.environment.events.request.fire(
            request_type="SSE",
            name=f"quiesce [{kind}]",
            response_time=(time.monotonic() - start) * 1000.0,
            response_length=0,
            exception=exc,
        )
        return exc is None


class ComputeUser(_Submitter):
    """Submits compute jobs of random size and reads back its own results."""

    weight = 3

    @task(3)
    def relay(self) -> None:
        self.submit_and_wait("relay", _relay(_uid()))

    @task(1)
    def adder(self) -> None:
        n, x, y = _uid(), random.randint(1, 5), random.randint(1, 5)
        if not self.submit_and_wait("adder", _adder(n, x, y)):
            return
        with self.client.get(
            "/export",
            params={"pattern": f"[2] petri [3] ! result{n} $", "template": "_1"},
            name="/export [verify adder]",
            catch_response=True,
        ) as resp:
            if resp.text.strip() != _peano(x + y):
                resp.failure(f"{x}+{y} on result{n}: got {resp.text.strip()!r}")


class IngestUser(_Submitter):
    """Write-heavy: bulk data-only transactions (no exec)."""

    weight = 2

    @task
    def ingest(self) -> None:
        self.submit_and_wait("ingest", _telemetry(_uid()))


class JoinUser(_Submitter):
    """Relational workload: load two tables, join them, check the cardinality."""

    weight = 1

    @task
    def join(self) -> None:
        n, emps, depts = _uid(), random.randint(2, 6), random.randint(2, 4)
        if not self.submit_and_wait("join", _equi_join(n, emps, depts)):
            return
        with self.client.get(
            "/export",
            params={"pattern": f"[4] joined {n} $ $", "template": "[2] _1 _2"},
            name="/export [verify join]",
            catch_response=True,
        ) as resp:
            got = len(resp.text.splitlines())
            if got != emps:
                resp.failure(f"join {n}: expected {emps} rows, got {resp.text!r}")


# How many items the long program joins with themselves. Cost is quadratic in it and
# measured flat over repeats: 300 → ~99 ms, 600 → ~370 ms, 1000 → ~1.02 s. 500 puts one
# long transaction at ~250 ms, ~60x a short write on the same idle server.
LONG_ITEMS = 500


def _scan(n: int, k: int) -> str:
    """A long, self-terminating program whose cost does not depend on anything else in the
    space: `k` items joined with themselves, so k² matches, all under the namespace `n`.

    The output template names only the outer variable, so the transaction writes `k` paths
    for k² units of work. That ratio is the point. The obvious long program — `_adder` with
    big operands — matches on `(petri (! …))` / `(petri (? …))`, which every earlier adder
    also left in the space: measured on an otherwise idle server, each committed 20+20
    adder made the next one ~145 ms slower (201 ms → 1.8 s over twelve runs). Under that
    program a table indexed by `--workers` would really be measuring how much junk the
    faster configurations had time to accumulate.
    """
    rows = [f"(item {n} i{i})" for i in range(k)]
    rows.append(f"(exec go (, (item {n} $x) (item {n} $y)) (, (pair {n} $x)))")
    return "\n".join(rows)


class ShortWriteUser(_Submitter):
    """95% of the mixed scenario: one fact per transaction, on a path nobody else writes.

    Disjoint by construction, and deliberately so: a short writer that collided with its
    peers would report the conflict rate of an accidentally-contended workload instead of
    what this scenario exists to isolate — what a long transaction does to the latency of
    the cheap ones running alongside it.
    """

    weight = 19
    wait_time = constant(0)  # saturating: throughput is the server's, not the wait's

    @task
    def write(self) -> None:
        self.submit_and_wait("short", f"(mixed w{_uid()} v)")


class LongWriteUser(_Submitter):
    """5%: one long generative program per transaction, in its own loc namespace.

    `_scan` stops on its own rather than diverging under `--step-budget`, because a budget
    big enough for a real workload is also big enough for a runaway to eat the box, and
    this scenario has to be safe to point at a server started with default flags.
    """

    weight = 1
    wait_time = constant(0)

    @task
    def compute(self) -> None:
        self.submit_and_wait("long", _scan(_uid(), LONG_ITEMS))


class AnalystUser(HttpUser):
    """Hammers the lock-free snapshot read path with a random query mix."""

    weight = 4
    wait_time = between(0.2, 1.0)

    QUERIES: list[tuple[str, dict[str, str]]] = [
        ("/export [full]", {}),
        ("/export [sensors]", {"pattern": "[4] sensor $ $ $", "template": "[3] _1 _2 _3"}),
        ("/export [relay outs]", {"pattern": "[3] out $ $", "template": "_2"}),
        ("/export [results]", {"pattern": "[2] petri [3] ! $ $", "template": "[2] _1 _2"}),
    ]

    @task
    def query(self) -> None:
        name, params = random.choice(self.QUERIES)
        self.client.get("/export", params=params, name=name)


class WatcherUser(HttpUser):
    """Holds one long-lived stream (randomly plain or deltas) and reads the run's two
    server-side signals off it: `lagged` (a failure — backpressure) and the `abort` tally,
    split by reason into `abort [conflict]` and `abort [phantom]`.

    `fixed_count = 1`, not a weight: `abort` is a broadcast event, so every extra watcher
    would count the same abort again and report a multiple of the real rate. One stream is
    also all the `lagged` signal needs.

    The tally is a lower bound. Aborts that land while this stream is being re-established
    (after an idle read timeout) are not seen — that gap only opens when the event rate is
    low enough for the stream to go idle, which under a saturating load it is not.
    """

    fixed_count = 1
    wait_time = between(0.5, 2.0)

    def _count(self, name: str) -> None:
        """Record one server-side event as a zero-latency stat, so its count lands in the
        request table (aborts are the run's outcome, not a client failure)."""
        self.environment.events.request.fire(
            request_type="SSE", name=name, response_time=0.0, response_length=0
        )

    @task
    def watch(self) -> None:
        params = random.choice([{}, {"deltas": "true"}])
        resp = self.client.get(
            "/events", params=params, stream=True, timeout=(5, 30), name="/events [watch]"
        )
        try:
            for ev in iter_sse(resp.iter_lines(decode_unicode=True)):
                if ev.name == "lagged":
                    self.environment.events.request.fire(
                        request_type="SSE",
                        name="lagged",
                        response_time=0.0,
                        response_length=0,
                        exception=RuntimeError(f"skipped {ev.data.get('skipped')} events"),
                    )
                elif ev.name == "abort":
                    # The engine's two conflict reasons start "conflict: " / "phantom: ";
                    # anything else is a load or exec failure and must not be filed as one.
                    reason = str(ev.data.get("reason", "")).split(":", 1)[0]
                    kind = reason if reason in ("conflict", "phantom") else "other"
                    self._count(f"abort [{kind}]")
        except requests_exceptions.RequestException:
            pass  # idle stream hit the read timeout; reconnect on the next task round
        finally:
            resp.close()
