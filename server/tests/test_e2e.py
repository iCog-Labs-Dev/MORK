"""End-to-end tests: real binary, real HTTP, real SSE.

Every test that watches events opens the stream BEFORE acting (subscribe-then-act):
the broadcast has no replay, so subscribing after a fast transaction loses its events.
"""

import re
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from client import MorkClient, MorkError, SseEvent
from conftest import EXAMPLES_DIR

TXID_RE = re.compile(r"\btx\d+_[a-z0-9]{8}\b")
PEANO_FOUR = "(S (S (S (S Z))))"
RESULT_PATTERN = "[2] petri [3] ! result $"
MLN_SAMPLED_PATTERN = "[3] was-sampled mln [2] mln-site $"
MLN_SAMPLED_TEMPLATE = "[3] was-sampled mln [2] mln-site _1"


def wait_for_export_line(
    server: MorkClient,
    pattern: str,
    template: str,
    expected: str,
    timeout: float = 5.0,
) -> list[str]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        out = server.export(pattern=pattern, template=template)
        if expected in out:
            return out
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {expected!r}, export={server.export()}")


def assert_export_line_absent_for(
    server: MorkClient,
    pattern: str,
    template: str,
    unexpected: str,
    duration: float = 0.25,
) -> None:
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        out = server.export(pattern=pattern, template=template)
        assert unexpected not in out, f"unexpected {unexpected!r}, export={server.export()}"
        time.sleep(0.05)


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


def test_source_sink_sweep_start_emits_event_atom(server: MorkClient) -> None:
    server.run(
        """
        (mln-site A (# 10))
        (mln-site B (# 1))
        (sweep mln
          (e cpq)
          (src (, (mln-site $x)))
          (sink (O (+ (was-sampled mln (mln-site $x))))))
        """
    )

    handle = server.sweep_start()
    assert handle == "sweep-scheduler"

    sampled = wait_for_export_line(
        server,
        MLN_SAMPLED_PATTERN,
        MLN_SAMPLED_TEMPLATE,
        "(was-sampled mln (mln-site A))",
    )

    assert "(was-sampled mln (mln-site B))" not in sampled
    server.sweep_stop()


def test_source_sink_sweep_pause_resume_stop_lifecycle(server: MorkClient) -> None:
    server.run(
        """
        (mln-site A (# 10))
        (sweep mln
          (e cpq)
          (src (, (mln-site $x)))
          (sink (O (+ (was-sampled mln (mln-site $x))))))
        """
    )

    assert server.sweep_start() == "sweep-scheduler"
    assert server.sweep_start() == "sweep-scheduler"
    wait_for_export_line(
        server,
        MLN_SAMPLED_PATTERN,
        MLN_SAMPLED_TEMPLATE,
        "(was-sampled mln (mln-site A))",
    )

    server.sweep_pause()
    with server.events() as stream:
        clear = server.run(
            """
            (exec clear-sampled
              (, (was-sampled mln (mln-site $x)))
              (O (- (was-sampled mln (mln-site $x)))))
            """
        )
        stream.wait_for("quiescent", tx=clear.tx)
    assert_export_line_absent_for(
        server,
        MLN_SAMPLED_PATTERN,
        MLN_SAMPLED_TEMPLATE,
        "(was-sampled mln (mln-site A))",
    )

    server.sweep_resume()
    wait_for_export_line(
        server,
        MLN_SAMPLED_PATTERN,
        MLN_SAMPLED_TEMPLATE,
        "(was-sampled mln (mln-site A))",
    )

    server.sweep_stop()
    with server.events() as stream:
        clear = server.run(
            """
            (exec clear-sampled
              (, (was-sampled mln (mln-site $x)))
              (O (- (was-sampled mln (mln-site $x)))))
            """
        )
        stream.wait_for("quiescent", tx=clear.tx)
    assert_export_line_absent_for(
        server,
        MLN_SAMPLED_PATTERN,
        MLN_SAMPLED_TEMPLATE,
        "(was-sampled mln (mln-site A))",
    )


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

    # positive control: an unfiltered stream carries the third tx's step
    with server.events() as unfiltered:
        third = server.run("(exec go (, (fdst2 $x)) (, (fdst3 $x)))")
        step = unfiltered.wait_for("step", tx=third.tx)
        assert "fdst2" in step.data["exec"]


def test_exec_error_aborts_whole_transaction(server: MorkClient) -> None:
    """Atomicity: an exec the interpreter rejects rolls back the ENTIRE transaction —
    including its data and the steps that already ran — and the server stays healthy."""
    with server.events() as stream:
        # First exec fires fine (good-out), second has a malformed pattern functor and
        # is rejected by the interpreter — everything must revert.
        res = server.run(
            "(keepme 1)\n"
            "(exec 0 (, (keepme $x)) (, (good-out $x)))\n"
            "(exec 1 (bad pattern) (bad template))"
        )
        ev = stream.wait_for("abort", tx=res.tx)
        assert "exec" in ev.data["reason"]
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
    with server.events() as stream:
        res = server.run(DIVERGING)
        ev = stream.wait_for("abort", tx=res.tx)
        assert "budget" in ev.data["reason"]
    out = server.export()
    assert not any("(n " in line or "prog" in line for line in out)  # no trace at all


def test_deltas_stream_added_expressions(server: MorkClient) -> None:
    with server.events(deltas=True) as stream:
        server.run("(delta-fact 42)")
        ev = stream.wait_for("delta")
        assert "(delta-fact 42)" in ev.data["added"]
        assert ev.data["removed"] == []


def test_exec_namespaces_are_isolated(server: MorkClient) -> None:
    """Two programs using the same exec loc run exactly one step each, sequentially —
    namespacing keeps the second from re-firing the first's (already consumed) exec.
    (The data region is shared by design, so both see `isrc`.)"""
    with server.events() as stream:
        a = server.run("(exec 0 (, (isrc $x)) (, (a-out $x)))\n(isrc 1)")
        b = server.run("(exec 0 (, (isrc $x)) (, (b-out $x)))")
        steps: dict[str, int] = {}
        pending = {a.tx, b.tx}
        for ev in stream:
            if ev.name == "step":
                steps[ev.data["tx"]] = steps.get(ev.data["tx"], 0) + 1
            elif ev.name == "quiescent":
                pending.discard(ev.data["tx"])
                if not pending:
                    break
        assert steps == {a.tx: 1, b.tx: 1}
    out = server.export()
    assert "(a-out 1)" in out
    assert "(b-out 1)" in out


def test_concurrent_submissions_drain_sequentially(server: MorkClient) -> None:
    """8 adders submitted concurrently on distinct result channels: every racing
    submission is queued and computes 2+2, and the sequential scheduler never
    interleaves two transactions — each tx's steps form one contiguous block in the
    event stream."""
    base = (EXAMPLES_DIR / "adder.metta").read_text()
    sources = [base.replace("result", f"result{i}") for i in range(8)]
    with server.events() as stream:
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(server.run, sources))
        pending = {r.tx for r in results}
        step_order: list[str] = []
        for ev in stream:
            if ev.name == "step":
                step_order.append(ev.data["tx"])
            elif ev.name == "quiescent":
                pending.discard(ev.data["tx"])
                if not pending:
                    break
    # Collapse consecutive runs; a tx appearing twice after collapsing means another
    # transaction's step ran in the middle of its block.
    collapsed = [tx for i, tx in enumerate(step_order) if i == 0 or tx != step_order[i - 1]]
    assert len(collapsed) == len(set(collapsed)), f"steps interleaved: {collapsed}"
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
