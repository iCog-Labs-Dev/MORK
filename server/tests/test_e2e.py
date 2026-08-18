"""End-to-end tests: real binary, real HTTP, real SSE.

Every test that watches events opens the stream BEFORE acting (subscribe-then-act):
the broadcast has no replay, so subscribing after a fast transaction loses its events.
"""

import re
from concurrent.futures import ThreadPoolExecutor

import pytest

from client import MorkClient, MorkError, SseEvent
from conftest import EXAMPLES_DIR

TXID_RE = re.compile(r"\btx\d+_[a-z0-9]{8}\b")
PEANO_FOUR = "(S (S (S (S Z))))"
RESULT_PATTERN = "[2] petri [3] ! result $"


def test_adder_end_to_end(server: MorkClient) -> None:
    src = (EXAMPLES_DIR / "adder.metta").read_text()
    with server.events() as stream:
        res = server.run(src)
        assert res.count == 6
        assert res.version == 1
        stream.wait_for("quiescent", tx=res.tx)
    assert server.export(pattern=RESULT_PATTERN, template="_1") == [PEANO_FOUR]
    # the tx-namespace wrapper never leaks into what clients see
    assert not any(TXID_RE.search(line) for line in server.export())


def test_data_only_transaction(server: MorkClient) -> None:
    with server.events() as stream:
        res = server.run("(parent tom bob)\n(parent bob ann)")
        assert res.count == 2
        stream.wait_for("idle")
    out = server.export()
    assert "(parent tom bob)" in out
    assert "(parent bob ann)" in out


def test_parse_error_is_400(server: MorkClient) -> None:
    with pytest.raises(MorkError) as excinfo:
        server.run("(unclosed")
    assert excinfo.value.status == 400


def test_non_utf8_body_is_400(server: MorkClient) -> None:
    assert server.run_raw(b"\xff\xfe(").status_code == 400


def test_hello_reports_current_state(server: MorkClient) -> None:
    with server.events() as stream:
        res = server.run("(a 1)\n(b 2)\n(c 3)")
        stream.wait_for("idle")
    with server.events() as fresh:
        assert fresh.hello.data["version"] == res.version
        assert fresh.hello.data["count"] == 3
        assert fresh.hello.data["active_txs"] == []


def test_tx_filter(server: MorkClient) -> None:
    with server.events() as stream:
        first = server.run("(exec go (, (fsrc $x)) (, (fdst $x)))\n(fsrc 1)")
        stream.wait_for("quiescent", tx=first.tx)

    # A stream filtered on the (finished) first tx must show nothing from a second tx;
    # `idle` passes every filter, giving a deterministic end marker.
    with server.events(tx=first.tx) as filtered:
        server.run("(exec go (, (fdst $x)) (, (fdst2 $x)))")
        leaked: list[SseEvent] = []
        for ev in filtered:
            if ev.name == "idle":
                break
            leaked.append(ev)
        assert leaked == []

    # positive control: an unfiltered stream carries the third tx's quiescent event
    # (per-step events aren't public state under concurrent execution; only
    # per-transaction outcomes are)
    with server.events() as unfiltered:
        third = server.run("(exec go (, (fdst2 $x)) (, (fdst3 $x)))")
        unfiltered.wait_for("quiescent", tx=third.tx)


def test_exec_error_aborts_whole_transaction(server: MorkClient) -> None:
    """Atomicity: an exec the interpreter rejects rolls back the ENTIRE transaction —
    including its data and the steps that already ran — and the server stays healthy.
    A worker runs a transaction to completion (load + step to its outcome) before the
    committer's single reply fires, so a failing exec now surfaces as a rejected
    POST /run (422) rather than a 200 followed by a later `abort` event."""
    # First exec fires fine (good-out), second has a malformed pattern functor and
    # is rejected by the interpreter — everything must revert.
    with pytest.raises(MorkError) as excinfo:
        server.run(
            "(keepme 1)\n"
            "(exec 0 (, (keepme $x)) (, (good-out $x)))\n"
            "(exec 1 (bad pattern) (bad template))"
        )
    assert excinfo.value.status == 422
    assert "exec" in excinfo.value.message
    out = server.export()
    assert "(keepme 1)" not in out          # the tx's own data: gone
    assert not any("good-out" in line for line in out)  # the successful step: reverted
    # rollback doesn't poison the engine
    with server.events() as stream:
        ok = server.run("(alive 1)")
        stream.wait_for("quiescent", tx=ok.tx)
    assert "(alive 1)" in server.export()


# A diverging program: the exec re-creates itself each step (its own code is stored as
# data and re-emitted via $p), bumping (n …) forever. Only a step budget stops it.
DIVERGING = """(n z)
(prog (exec go (, (prog $p) (n $t)) (, (prog $p) (n (s $t)) $p)))
(exec go (, (prog $p) (n $t)) (, (prog $p) (n (s $t)) $p))"""


@pytest.mark.parametrize("server", [["--step-budget", "5"]], indirect=True)
def test_budget_commit_keeps_partial_progress(server: MorkClient) -> None:
    """Default budget action: partial results stay, pending execs are parked as inert
    (paused ...) data, and the server moves on to the next transaction."""
    with server.events() as stream:
        res = server.run(DIVERGING)
        ev = stream.wait_for("budget", tx=res.tx)
        assert ev.data["steps"] == 5
    out = server.export()
    assert "(n (s z))" in out                      # partial progress kept
    assert any(line.startswith("(paused ") for line in out)   # continuation parked
    assert not any(line.startswith("(exec ") for line in out) # nothing left steppable
    # the queue is unblocked: a following tx runs to quiescence normally
    with server.events() as stream:
        ok = server.run("(alive 1)")
        stream.wait_for("quiescent", tx=ok.tx)


@pytest.mark.parametrize(
    "server", [["--step-budget", "5", "--budget-action", "abort"]], indirect=True
)
def test_budget_abort_rolls_back(server: MorkClient) -> None:
    """`--budget-action abort` fails the worker's run outright, so — same reply-timing
    change as `test_exec_error_aborts_whole_transaction` — this now surfaces as a
    rejected POST /run rather than a 200 followed by a later `abort` event."""
    with pytest.raises(MorkError) as excinfo:
        server.run(DIVERGING)
    assert excinfo.value.status == 422
    assert "budget" in excinfo.value.message
    out = server.export()
    assert not any("(n " in line or "prog" in line for line in out)  # no trace at all


def test_deltas_stream_added_expressions(server: MorkClient) -> None:
    with server.events(deltas=True) as stream:
        server.run("(delta-fact 42)")
        ev = stream.wait_for("delta")
        assert "(delta-fact 42)" in ev.data["added"]
        assert ev.data["removed"] == []


def test_exec_namespaces_are_isolated(server: MorkClient) -> None:
    """Two programs using the same exec loc run independently — namespacing keeps the
    second from re-firing the first's (already consumed) exec.
    (The data region is shared by design, so both see `isrc`.)
    Per-step events are gone (not observable state under concurrent execution), so
    isolation is checked via each tx quiescing and producing its own output only."""
    with server.events() as stream:
        a = server.run("(exec 0 (, (isrc $x)) (, (a-out $x)))\n(isrc 1)")
        b = server.run("(exec 0 (, (isrc $x)) (, (b-out $x)))")
        pending = {a.tx, b.tx}
        for ev in stream:
            if ev.name == "quiescent":
                pending.discard(ev.data["tx"])
                if not pending:
                    break
    out = server.export()
    assert "(a-out 1)" in out
    assert "(b-out 1)" in out


def test_concurrent_submissions_drain_sequentially(server: MorkClient) -> None:
    """8 adders submitted concurrently on distinct result channels: every racing
    submission is queued and computes 2+2, and with the default single worker the
    committer never runs two transactions at once — each tx's `tx` event is
    immediately followed by its own `quiescent`, with no other tx's events between
    them. (Per-step events are gone under concurrent execution, so this checks
    non-interleaving at the transaction granularity instead of the step granularity.)"""
    base = (EXAMPLES_DIR / "adder.metta").read_text()
    sources = [base.replace("result", f"result{i}") for i in range(8)]
    with server.events() as stream:
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(server.run, sources))
        pending = {r.tx for r in results}
        order: list[str] = []
        open_tx: str | None = None
        for ev in stream:
            if ev.name == "tx":
                assert open_tx is None, f"tx {ev.data['tx']} started while {open_tx} was still open"
                open_tx = ev.data["tx"]
                order.append(open_tx)
            elif ev.name == "quiescent":
                assert ev.data["tx"] == open_tx, f"quiescent for {ev.data['tx']} while {open_tx} was open"
                open_tx = None
                pending.discard(ev.data["tx"])
                if not pending:
                    break
    assert len(order) == len(set(order)), f"transactions interleaved: {order}"
    for i in range(8):
        out = server.export(pattern=f"[2] petri [3] ! result{i} $", template="_1")
        assert out == [PEANO_FOUR], f"adder {i} result wrong: {out}"


def test_sees_predecessors_computed_results(server: MorkClient) -> None:
    """Sequential consistency: a transaction submitted after another sees its
    predecessor's COMPUTED results, not just its submitted data. (Under the old
    round-robin scheduler this raced: the second exec could fire before the first
    produced `derived`, silently matching nothing.)"""
    with server.events() as stream:
        server.run("(seed 1)\n(exec 0 (, (seed $x)) (, (derived $x)))")
        second = server.run("(exec 0 (, (derived $x)) (, (final $x)))")
        stream.wait_for("quiescent", tx=second.tx)
    out = server.export()
    assert "(derived 1)" in out
    assert "(final 1)" in out
