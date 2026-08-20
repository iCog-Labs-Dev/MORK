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


@pytest.mark.parametrize(
    "server", [["--workers", "8", "--step-budget", STEP_BUDGET]], indirect=True
)
def test_arrival_is_not_blocked_by_a_transaction_in_flight(server: MorkClient) -> None:
    """A transaction submitted while a long one is running must not wait for it.

    With eight workers and one transaction in flight, seven workers are idle, so a
    trivial transaction arriving mid-flight has nothing legitimate to wait for. If the
    committer only wakes on finished work it never reads the request channel, and the
    trivial one sits there for exactly the remainder of the long one — a scheduling
    bug that would otherwise be measured as MVCC overhead.

    Both latencies are calibrated on this machine rather than hardcoded: the probe is
    fired halfway through the long transaction and must return in less than half of
    what is left of it.
    """
    start = time.perf_counter()
    server.run(_padded("(calibration long)", "hol_cal"))
    long_solo = time.perf_counter() - start

    start = time.perf_counter()
    server.run("(calibration trivial)")
    trivial_solo = time.perf_counter() - start

    with ThreadPoolExecutor(max_workers=1) as pool:
        long_tx = pool.submit(server.run, _padded("(hol long)", "hol"))
        time.sleep(long_solo / 2)
        start = time.perf_counter()
        probe = server.run("(probe arrived)")
        waited = time.perf_counter() - start
        long_result = long_tx.result()

    remaining = long_solo / 2
    assert waited < remaining / 2, (
        f"a trivial transaction submitted {remaining:.3f} s before the in-flight one "
        f"was due to finish took {waited:.3f} s (it takes {trivial_solo:.3f} s alone): "
        f"it was held behind the long transaction instead of running on an idle worker"
    )
    # The server's own commit order, not the client's view of it. `long_tx.done()` would
    # be the obvious check and is worthless here: it flips when the requests thread parses
    # the response, which lags the actual commit by an unbounded amount, so it reports
    # "still running" for a transaction the server finished with long ago. Versions come
    # from the committer, so this cannot lie about which landed first.
    assert probe.version < long_result.version, (
        f"the probe committed at version {probe.version}, after the long transaction at "
        f"{long_result.version} — the long one had already finished, so this run measured "
        f"nothing about overlap (long solo was {long_solo:.3f} s)"
    )
