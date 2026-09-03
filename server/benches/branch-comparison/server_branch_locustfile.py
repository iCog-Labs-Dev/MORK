"""Mixed short/long load scenario for the `server` branch's mork-server (pessimistic
prefix-locking), ported from server/tests/locustfile.py's ShortWriteUser/LongWriteUser
so the two branches can be measured on the same effective workload.

Wire format is very different from the metta-calculus-server branch:

- Short write: `POST /upload/<pattern>/<template>` with one fact as the body.
  IMPORTANT: pattern/template must embed the request's own unique symbol as the
  template's constant head (`(mixed w<uid> $v)`), not a bare `$x`. `new_writer`'s lock
  scope is `derive_prefix_from_expr_slice(template).till_constant_to_full()` -- the
  constant part of the *template*, computed before any row is examined. A bare `$x`
  template has an empty constant prefix, so it locks the entire space root: every
  short-write in the population would serialize against every other short-write (and
  the long-write's own uploads) on one global lock, which is not "one fact on a
  disjoint path" at all -- it's the least disjoint possible upload. (First pass at this
  harness used `$x`/`$x` and produced ~100% 401s at every worker count above 1 as a
  result; this is that fix.) The response is only sent once the write commits under a
  `WritePermission`, so its latency is directly comparable to `/run [short]` on the
  other branch.

- Long compute: `(item <n> i<k>)` facts and the one `(exec (<n> 0) (,...) (,...))`
  clause are uploaded as two separate calls (their top-level shapes differ, so one
  passthrough pattern can't cover both), each with a template whose constant prefix is
  namespaced by `<n>` for the same disjointness reason as above. Then
  `GET /metta_thread/?location=<n>` DISPATCHES the thread and returns immediately
  (`WorkResult::Immediate`) -- it does NOT block until the thread finishes. There is no
  synchronous "runs to commit" request on this branch for compute, so completion is
  detected by polling `GET /status/(exec%20<n>)` (a raw `requests.Session`, not
  `self.client`, so the poll traffic itself doesn't flood locust's stats) at 10ms
  intervals until the internally-tagged `status` field leaves `pathForbidden(Temporary)`
  -- `pathClear` is success, anything else is reported as a failure. This poll loop is
  why the long-path latency here has an extra ~10ms-quantized tail the other branch's
  single blocking `/run` doesn't.

Note: this branch's `WorkerPool::new()` asserts `thread_count >= 1` where
`thread_count = tokio_worker_threads - 1` -- so it cannot boot at all on a single core
(`taskset -c <one core>` panics immediately). The practical floor is 2 cores.

    cargo +nightly build --release -p mork-server   # in a `server`-branch checkout
    MORK_SERVER_ADDR=127.0.0.1 MORK_SERVER_PORT=8099 ./target/release/mork-server &
    uv run --project <mvcc-branch>/server/tests locust -f server_branch_locustfile.py \
        --headless --processes 8 -u 41 -r 41 -t 30s --host http://127.0.0.1:8099
"""

import itertools
import random
import time

import requests
from locust import HttpUser, constant, task

_ids = itertools.count()
LONG_ITEMS = 500
POLL_INTERVAL = 0.01


def _uid() -> int:
    return (next(_ids) << 16) | random.randrange(1 << 16)


class ShortWriteUser(HttpUser):
    """95% of the mixed scenario: one fact per transaction, disjoint path per submission."""

    weight = 19
    wait_time = constant(0)

    @task
    def write(self) -> None:
        uid = _uid()
        shape = f"(mixed w{uid} $v)"
        body = f"(mixed w{uid} v)"
        self.client.post(f"/upload/{shape}/{shape}", data=body.encode(), name="/upload [short]")


class LongWriteUser(HttpUser):
    """5%: one long generative program per transaction, in its own loc namespace."""

    weight = 1
    wait_time = constant(0)

    @task
    def compute(self) -> None:
        n = f"long{_uid()}"
        item_shape = f"(item {n} $v)"
        items_body = "\n".join(f"(item {n} i{i})" for i in range(LONG_ITEMS))
        exec_shape = f"(exec ({n} $p) $pat $tmpl)"
        exec_body = f"(exec ({n} 0) (, (item {n} $x) (item {n} $y)) (, (pair {n} $x)))"

        start = time.monotonic()
        exc: Exception | None = None
        try:
            resp = self.client.post(
                f"/upload/{item_shape}/{item_shape}",
                data=items_body.encode(),
                name="/upload [long-items]",
            )
            if resp.status_code != 200:
                exc = RuntimeError(f"item upload failed: {resp.status_code}")
                return
            resp1b = self.client.post(
                f"/upload/{exec_shape}/{exec_shape}",
                data=exec_body.encode(),
                name="/upload [long-exec]",
            )
            if resp1b.status_code != 200:
                exc = RuntimeError(f"exec upload failed: {resp1b.status_code}")
                return
            resp2 = self.client.get(f"/metta_thread/?location={n}", name="/metta_thread [long]")
            if resp2.status_code != 200:
                exc = RuntimeError(f"metta_thread dispatch failed: {resp2.status_code}")
                return

            session = requests.Session()
            # raw space in the path is rejected by strict URL parsers (confirmed against
            # curl during smoke-testing); %20 it explicitly rather than trust auto-normalization
            status_url = f"{self.host}/status/(exec%20{n})"
            while True:
                r = session.get(status_url, timeout=30)
                try:
                    st = r.json().get("status")
                except ValueError:
                    st = None
                if st in ("pathForbidden", "pathForbiddenTemporary"):
                    time.sleep(POLL_INTERVAL)
                    continue
                if st == "pathClear":
                    break
                exc = RuntimeError(f"unexpected terminal status: {st!r} body={r.text[:200]!r}")
                break
        except requests.exceptions.RequestException as e:
            exc = e
        finally:
            self.environment.events.request.fire(
                request_type="POLL",
                name="quiesce [long]",
                response_time=(time.monotonic() - start) * 1000.0,
                response_length=0,
                exception=exc,
            )
