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

    # positive control: an unfiltered stream carries the third tx's step
    with server.events() as unfiltered:
        third = server.run("(exec go (, (fdst2 $x)) (, (fdst3 $x)))")
        step = unfiltered.wait_for("step", tx=third.tx)
        assert "fdst2" in step.data["exec"]


def test_deltas_stream_added_expressions(server: MorkClient) -> None:
    with server.events(deltas=True) as stream:
        server.run("(delta-fact 42)")
        ev = stream.wait_for("delta")
        assert "(delta-fact 42)" in ev.data["added"]
        assert ev.data["removed"] == []


def test_exec_namespaces_are_isolated(server: MorkClient) -> None:
    """Two programs using the same exec loc run exactly one step each — neither namespace
    steals the other's exec. (The data region is shared by design, so both see `isrc`.)"""
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


def test_concurrent_adders(server: MorkClient) -> None:
    """Regression for the round-robin scheduler: 8 adders submitted concurrently on
    distinct result channels all quiesce and each computes 2+2."""
    base = (EXAMPLES_DIR / "adder.metta").read_text()
    sources = [base.replace("result", f"result{i}") for i in range(8)]
    with server.events() as stream:
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(server.run, sources))
        stream.wait_for_all("quiescent", [r.tx for r in results])
    for i in range(8):
        out = server.export(pattern=f"[2] petri [3] ! result{i} $", template="_1")
        assert out == [PEANO_FOUR], f"adder {i} result wrong: {out}"
