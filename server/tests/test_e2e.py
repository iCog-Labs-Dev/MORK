"""End-to-end tests: real binary, real HTTP, real SSE.

Every test that watches events opens the stream BEFORE acting (subscribe-then-act):
the broadcast has no replay, so subscribing after a fast transaction loses its events.
"""

import json
import re
import socket
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from client import MorkClient, MorkError, SseEvent
from conftest import EXAMPLES_DIR
from test_concurrency import PADDING

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
    assert "(keepme 1)" not in out  # the tx's own data: gone
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
    assert "(n (s z))" in out  # partial progress kept
    assert any(line.startswith("(paused ") for line in out)  # continuation parked
    assert not any(line.startswith("(exec ") for line in out)  # nothing left steppable
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


@pytest.mark.parametrize("server", [["--workers", "1"]], indirect=True)
def test_concurrent_submissions_drain_sequentially(server: MorkClient) -> None:
    """8 adders submitted concurrently on distinct result channels: every racing
    submission is queued and computes 2+2, and at `--workers 1` the committer never
    runs two transactions at once — each tx's `tx` event is immediately followed by its
    own `quiescent`, with no other tx's events between them. (Per-step events are gone
    under concurrent execution, so this checks non-interleaving at the transaction
    granularity instead of the step granularity.)

    `--workers 1` is pinned deliberately, not inherited: the default is now the
    machine's core count, under which transactions DO interleave by design. This is the
    sequential configuration's own property — that asking for one worker really gets
    you one — so the assertions stay exactly as strict as they were when it was the
    default."""
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
                assert ev.data["tx"] == open_tx, (
                    f"quiescent for {ev.data['tx']} while {open_tx} was open"
                )
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


def _raw_post(host: str, port: int, body: bytes) -> socket.socket:
    """Fire a `POST /run` and return the still-open socket, without reading the
    response. Used in pairs so two requests hit the listener back-to-back with no
    Python-level scheduling gap between them — a `ThreadPoolExecutor` measurably
    widens that gap (see the comment on `test_concurrent_conflict_one_transaction_aborts`)."""
    s = socket.create_connection((host, port), timeout=10)
    req = (
        f"POST /run HTTP/1.1\r\nHost: {host}\r\nContent-Length: {len(body)}\r\n\r\n".encode() + body
    )
    s.sendall(req)
    return s


def _raw_response(sock: socket.socket) -> tuple[int, str]:
    """Read one HTTP/1.1 response (status code + body) off a socket from `_raw_post`."""
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf += chunk
    head, _, rest = buf.partition(b"\r\n\r\n")
    lines = head.split(b"\r\n")
    status = int(lines[0].split(b" ")[1])
    length = next(
        (
            int(hdr.split(b":")[1].strip())
            for hdr in lines[1:]
            if hdr.lower().startswith(b"content-length:")
        ),
        0,
    )
    while len(rest) < length:
        chunk = sock.recv(4096)
        if not chunk:
            break
        rest += chunk
    sock.close()
    return status, rest.decode()


def _raw_events(host: str, port: int) -> tuple[socket.socket, str]:
    """Open `/events` as HTTP/1.0 (no chunked transfer-encoding, so frames can be read
    as plain bytes with no chunk-size framing to decode) and consume the `hello` frame.
    Returns the socket plus any bytes already read past the `hello` frame's boundary,
    for `_raw_wait_for` to pick up.

    Used instead of `MorkClient.events()` (`requests`-based) for this test only: that
    combination — `requests`'s chunked-SSE consumption running concurrently with the
    raw `/run` sockets below — was observed to intermittently stall during development
    of this test. A minimal reproduction using nothing but raw sockets on both sides
    (no `requests` anywhere) never stalled once across 100+ trials, which points at a
    client-library interaction rather than a mork-server bug — but a test that can
    stall either way isn't shippable, so this sidesteps `requests` for /events here."""
    s = socket.create_connection((host, port), timeout=10)
    s.sendall(f"GET /events HTTP/1.0\r\nHost: {host}\r\n\r\n".encode())
    buf = ""
    s.settimeout(10)
    while "\n\n" not in buf:
        chunk = s.recv(4096)
        if not chunk:
            break
        buf += chunk.decode()
    _hello, _, rest = buf.partition("\n\n")
    return s, rest


def _raw_wait_for(
    sock: socket.socket, buf: str, name: str, timeout: float = 10.0
) -> tuple[dict[str, Any], str]:
    """Read SSE frames off a raw `/events` socket (as opened by `_raw_events`) until one
    named `name` arrives. Returns its data plus any leftover buffered bytes, so callers
    can keep making further calls against the same stream."""
    sock.settimeout(timeout)
    while True:
        while "\n\n" not in buf:
            chunk = sock.recv(4096)
            if not chunk:
                raise AssertionError(f"events stream closed before {name!r} arrived")
            buf += chunk.decode()
        frame, _, buf = buf.partition("\n\n")
        ev_name, data = None, {}
        for line in frame.split("\n"):
            if line.startswith("event: "):
                ev_name = line.removeprefix("event: ")
            elif line.startswith("data: "):
                data = json.loads(line.removeprefix("data: "))
        if ev_name == name:
            return data, buf


# How many racing rounds `test_concurrent_conflict_one_transaction_aborts` may spend
# looking for an overlap, and the step budget that makes each round likely to produce
# one. See that test's docstring for where both numbers come from.
CONFLICT_ROUNDS = 40
CONFLICT_STEP_BUDGET = "50"


@pytest.mark.parametrize(
    "server", [["--workers", "4", "--step-budget", CONFLICT_STEP_BUDGET]], indirect=True
)
def test_concurrent_conflict_one_transaction_aborts(server: MorkClient) -> None:
    """A real MVCC conflict at `--workers 4`: one transaction removes-by-pattern
    `(edge{n} $x $y)`, another concurrently adds a brand new `(edge{n} c d)` — the
    phantom shape `mvcc::validate` exists to catch (a pattern-scoped removal racing a
    concurrent insertion under the same ground prefix).

    Two transactions only conflict if they share a base version, and from outside the
    process that overlap has to be bought rather than hoped for. Submitting the pair
    back-to-back over pre-connected raw sockets is not enough on its own: measured over
    200 rounds with the `/events` subscriber attached, only 1% of rounds overlapped
    (the earlier fix round measured 2% over two 500-round samples, and 5.7% with no
    subscriber). These transactions are small enough that the first one usually commits
    before the second is even dispatched, so closing Python's thread-scheduling gap
    barely moves the number.

    The lever that does work is padding the removing side with the same self-namespaced
    diverging program `test_concurrency.py` uses, capped by `--step-budget`. It holds
    the remover in flight while the adder commits underneath it, so the remover
    validates against a version newer than its base and aborts. Each round gets its own
    `edge{round}` facts and its own `a{round}` padding namespace, so nothing leaks
    between rounds and the padding never invents a conflict of its own. Measured over
    200 rounds at `--step-budget 50`: 80% of rounds conflict, at 0.06 s per round.
    (This rate is machine-sensitive and moves with the committer's dispatch behaviour:
    the same harness measured 34% and 52% on other hosts, and 52% on this one before
    head-of-line dispatch was fixed. The cap below is sized off the pessimistic end.)
    (Padding used to be off the table because it killed the server — that turned out to
    be undefined behaviour in PathMap, `LineListNode::pjoin_dyn` calling
    `as_dense_unchecked()` on a `CellByteNode`, trapping as SIGILL. It is fixed.)

    Hence the 40-round cap. The per-round rate is *not* stationary — conflicts are
    front-loaded into the cold-start rounds and the rate decays as the process warms —
    so a binomial confidence interval over the sample would be the wrong model and is
    deliberately not quoted. Compounding a pessimistic 25% floor instead:
    `0.75^40 ≈ 1e-5` — a floor chosen to stay valid on a slower host, not a reading of
    the 80% measured here. Over 65 trials of this loop — 40 in a standalone harness plus 25
    full runs of this test — the first conflict landed on round 0 in 24 of them, with a
    median of round 1 and a worst case of round 13; none came close to the cap. Cost
    follows the same shape: ~0.12 s in the median, ~2.4 s if the cap is ever spent. If it does exhaust, the test fails loudly rather
    than silently passing — a bounded retry for a genuinely racy phenomenon, not a
    hope-it-works flake.
    """
    host, _, port_s = server.base_url.removeprefix("http://").partition(":")
    port = int(port_s)
    ev_sock, ev_buf = _raw_events(host, port)
    for round_ in range(CONFLICT_ROUNDS):
        a_src = (
            f"(edge{round_} a b)\n"
            f"(exec 0 (, (edge{round_} $x $y)) (O (- (edge{round_} $x $y))))\n"
            + PADDING.format(ns=f"a{round_}")
            + "\n"
        )
        b_src = f"(edge{round_} c d)\n"
        sa = _raw_post(host, port, a_src.encode())
        sb = _raw_post(host, port, b_src.encode())
        # `_raw_response` closes each socket once it has the whole body, so a long run
        # holds no more than the two sockets of the round it is in.
        status_a, body_a = _raw_response(sa)
        status_b, body_b = _raw_response(sb)
        if status_a == 422 or status_b == 422:
            a_lost = status_a == 422
            loser_status, loser_body = (status_a, body_a) if a_lost else (status_b, body_b)
            winner_body = body_b if a_lost else body_a
            break
    else:
        pytest.fail(
            f"no conflict observed in {CONFLICT_ROUNDS} rounds — either the race genuinely "
            "never overlapped (~1e-5 even at a pessimistic 25% per-round rate) or "
            "commit's wiring regressed; investigate before assuming bad luck"
        )

    assert loser_status == 422
    assert "phantom" in loser_body
    abort_data, ev_buf = _raw_wait_for(ev_sock, ev_buf, "abort")
    assert "phantom" in abort_data["reason"]
    ev_sock.close()
    assert '"ok":true' in winner_body
    out = server.export()
    if a_lost:
        # A (the remover) lost: B's add won outright, and A's own transaction
        # (including its local add of "a b") was discarded entirely.
        assert f"(edge{round_} c d)" in out
        assert f"(edge{round_} a b)" not in out
    else:
        # B (the adder) lost: A's removal won, netting its own local "a b" add
        # against its own removal to nothing, and B's "c d" never got installed.
        assert f"(edge{round_} a b)" not in out
        assert f"(edge{round_} c d)" not in out
