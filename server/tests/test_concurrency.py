"""Overlapping writers on non-overlapping paths must all commit.

This is the headline claim of the MVCC design, and the one thing the rest of the
suite does not check: conflict detection is *path*-granular, not *prefix*-granular.
Two transactions writing different paths never conflict, even when those paths sit
side by side under a long shared prefix — `mvcc::validate` intersects writesets
(`meet`) rather than comparing subtrees, and only a pattern-scoped removal widens
that to a prefix probe.

Overlap is forced rather than hoped for. `mvcc::validate` only looks at versions
newer than a transaction's base, so transactions that happen to run one after another
validate against an empty history and *any* conflict rule at all would let them
through. Each submission here is padded with a self-namespaced diverging program
capped by `--step-budget`, and every test asserts the batch finished measurably
faster than the same work run one at a time — proof the transactions were genuinely
in flight together while validating against each other. Given that, an abort in the
first test means unrelated paths were treated as conflicting, and an abort in the
second means `(edge a …)` was treated as one conflict unit instead of many paths.

The speedup assertion assumes real parallelism: on a single available CPU the batch
cannot beat serial and these tests fail (loudly, with the measured timings).
"""

import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from client import MorkClient

# A self-namespaced diverging program: the exec stores its own code as data and re-emits
# it via `$p`, bumping `(n<ns> …)` forever, so only the step budget stops it. Giving each
# submission its own `ns` keeps every padded path disjoint from every other submission's
# — the padding buys execution time without inventing a conflict.
PADDING = """(n{ns} z)
(prog{ns} (exec go (, (prog{ns} $p) (n{ns} $t)) (, (prog{ns} $p) (n{ns} (s $t)) $p)))
(exec go (, (prog{ns} $p) (n{ns} $t)) (, (prog{ns} $p) (n{ns} (s $t)) $p))"""

# ~300 ms of work per submission on a warm release build: long enough that eight of them
# cannot serialize inside the batch deadline, short enough to keep each test near a second.
STEP_BUDGET = "100"

# How much faster than serial the batch has to be. Measured on this workload: ~8x solo at
# `--workers 1`, ~2.6x at `--workers 8`, ~3.1x at `--workers 4` — 8/1.5 ≈ 5.3x sits well
# clear of both sides.
SPEEDUP = 1.5


def _padded(fact: str, ns: str) -> str:
    return f"{fact}\n" + PADDING.format(ns=ns)


def _commit_overlapping(server: MorkClient, facts: list[str]) -> None:
    """Submit every fact padded to a budget's worth of work, all at once, and require
    both that nothing aborted and that the batch really did overlap.

    Solo latency is measured here rather than hardcoded, so the deadline follows the
    machine instead of a constant that rots.
    """
    start = time.perf_counter()
    server.run(_padded("(calibration solo)", "solo"))
    solo = time.perf_counter() - start

    sources = [_padded(fact, str(i)) for i, fact in enumerate(facts)]
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=len(sources)) as pool:
        # MorkClient.run raises MorkError on a 422, so "nothing raised" is "no aborts",
        # and POST /run blocks until commit, so once map() returns everything is in.
        list(pool.map(server.run, sources))
    batch = time.perf_counter() - start

    deadline = len(sources) * solo / SPEEDUP
    assert batch < deadline, (
        f"{len(sources)} transactions took {batch:.3f} s against a {solo:.3f} s solo run "
        f"(deadline {deadline:.3f} s): they ran essentially one at a time, so they never "
        f"validated against each other and this test proves nothing about conflicts"
    )
    assert set(facts) <= set(server.export())


@pytest.mark.parametrize(
    "server", [["--workers", "8", "--step-budget", STEP_BUDGET]], indirect=True
)
def test_disjoint_writers_all_commit(server: MorkClient) -> None:
    """Eight overlapping transactions on disjoint paths: none may be rejected."""
    _commit_overlapping(server, [f"(edge n{i} m{i})" for i in range(8)])


@pytest.mark.parametrize(
    "server", [["--workers", "4", "--step-budget", STEP_BUDGET]], indirect=True
)
def test_siblings_under_a_shared_prefix_do_not_commit_conflict(server: MorkClient) -> None:
    """Path granularity, not prefix granularity: eight overlapping writers under `(edge a …)`."""
    _commit_overlapping(server, [f"(edge a n{i})" for i in range(8)])
