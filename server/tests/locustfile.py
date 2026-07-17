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
  ``deltas=true``) and reports every ``lagged`` frame as a failure (backpressure signal).

Every submitting user subscribes before acting (the broadcast has no replay) and each
per-workload time-to-quiescent shows up as its own ``quiesce [...]`` stat. The space
grows monotonically under load (no isolation/GC yet — MVCC is future work), so RSS
growth is by design.
"""

import itertools
import random
import time

from locust import HttpUser, between, task
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
    """Holds one long-lived stream (randomly plain or deltas); `lagged` is a failure."""

    weight = 1
    wait_time = between(0.5, 2.0)

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
        except requests_exceptions.RequestException:
            pass  # idle stream hit the read timeout; reconnect on the next task round
        finally:
            resp.close()
