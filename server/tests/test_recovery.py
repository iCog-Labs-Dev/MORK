"""Crash recovery e2e: kill -9 a persistent server, respawn on the same --data-dir,
assert the space (and counters) come back exactly.

Note on what kill -9 can and can't test: written-but-unfsynced data lives in the OS page
cache, which survives process death — only power loss defeats it. So these tests verify
the recovery logic (scan, replay, counters) under every fsync policy; the policies'
loss-window differences are only observable under real power failure.
"""

import time

from client import MorkClient
from conftest import EXAMPLES_DIR
from test_e2e import DIVERGING, PEANO_FOUR, RESULT_PATTERN


def _run_and_wait(client: MorkClient, src: str, outcome: str = "quiescent"):
    with client.events() as stream:
        res = client.run(src)
        stream.wait_for(outcome, tx=res.tx)
    return res


def test_recovery_after_quiescence(spawner, tmp_path) -> None:
    args = ["--data-dir", str(tmp_path), "--fsync", "always"]
    c1, p1 = spawner(args)
    _run_and_wait(c1, (EXAMPLES_DIR / "adder.metta").read_text())
    before = c1.export()

    p1.kill()
    p1.wait(timeout=10)

    c2, _ = spawner(args)
    assert c2.export() == before  # dumps are trie-order: byte-identical, not just set-equal
    assert c2.export(pattern=RESULT_PATTERN, template="_1") == [PEANO_FOUR]


def test_recovery_under_everysec(spawner, tmp_path) -> None:
    args = ["--data-dir", str(tmp_path)]  # default --fsync everysec
    c1, p1 = spawner(args)
    _run_and_wait(c1, "(fact 1)\n(exec 0 (, (fact $x)) (, (derived $x)))")
    before = c1.export()

    p1.kill()
    p1.wait(timeout=10)

    c2, _ = spawner(args)
    assert c2.export() == before


def test_crash_mid_execution_reruns_dangling_tx(spawner, tmp_path) -> None:
    """A TX record with no COMMIT/ABORT (killed mid-steps) is re-run fresh on startup
    under the CURRENT budget config, and closed with a real outcome record."""
    c1, p1 = spawner(["--data-dir", str(tmp_path), "--fsync", "always"])
    c1.run(DIVERGING)  # 200 = TX record fsynced; the program then steps ~forever
    time.sleep(0.3)  # let it get properly mid-execution
    p1.kill()
    p1.wait(timeout=10)

    # Respawn with a tiny budget: recovery re-runs the dangling tx, which budget-commits
    # BEFORE the listener binds — so the state is already settled when we connect.
    c2, _ = spawner(["--data-dir", str(tmp_path), "--fsync", "always", "--step-budget", "5"])
    out = c2.export()
    assert "(n (s z))" in out  # partial progress from the re-run
    assert any(line.startswith("(paused ") for line in out)
    # and the outcome record closed it: another restart must NOT re-run it again
    p2 = spawner(["--data-dir", str(tmp_path), "--fsync", "always", "--step-budget", "5"])[0]
    assert p2.export() == out


def test_tx_counter_and_version_survive(spawner, tmp_path) -> None:
    args = ["--data-dir", str(tmp_path), "--fsync", "always"]
    c1, p1 = spawner(args)
    last = None
    for i in range(3):
        last = _run_and_wait(c1, f"(fact {i})")

    p1.kill()
    p1.wait(timeout=10)

    c2, _ = spawner(args)
    with c2.events() as stream:
        assert stream.hello.data["version"] == last.version  # replay reproduced the count
        res = c2.run("(after 1)")
        stream.wait_for("quiescent", tx=res.tx)
    assert int(res.tx[2:].split("_")[0]) == 4  # tx1..tx3 replayed → next is tx4
    assert res.version == last.version + 1


def test_checkpoint_restore_and_log_gc(spawner, tmp_path) -> None:
    """--checkpoint-every N: the space is snapshotted, pre-checkpoint segments are
    deleted, and recovery = restore checkpoint + replay only the log tail."""
    args = ["--data-dir", str(tmp_path), "--fsync", "always", "--checkpoint-every", "2"]
    c1, p1 = spawner(args)
    for i in range(3):  # checkpoint fires after the 2nd tx; the 3rd is log tail
        last = _run_and_wait(c1, f"(fact {i})")
    before = c1.export()

    deadline = time.monotonic() + 5.0  # the install is async on the checkpointer thread
    while time.monotonic() < deadline:
        names = {p.name for p in tmp_path.iterdir()}
        if "checkpoint.meta" in names and "wal-000000.log" not in names:
            break
        time.sleep(0.05)
    names = {p.name for p in tmp_path.iterdir()}
    assert "checkpoint.meta" in names
    assert any(n.startswith("checkpoint-") and n.endswith(".paths") for n in names)
    assert "wal-000000.log" not in names  # pre-checkpoint segment GC'd
    assert "wal-000001.log" in names      # the tail segment

    p1.kill()
    p1.wait(timeout=10)

    c2, _ = spawner(args)  # checkpoint restore + tail replay
    assert c2.export() == before
    with c2.events() as stream:
        assert stream.hello.data["version"] == last.version
        res = c2.run("(after 1)")
        stream.wait_for("quiescent", tx=res.tx)
    assert int(res.tx[2:].split("_")[0]) == 4  # counter: meta (2) + tail replay (3) + 1


def test_clean_restart(spawner, tmp_path) -> None:
    args = ["--data-dir", str(tmp_path)]
    c1, p1 = spawner(args)
    _run_and_wait(c1, "(persistent fact)")
    before = c1.export()

    p1.terminate()  # SIGTERM: no graceful handler — same recovery path as a crash
    p1.wait(timeout=10)

    c2, _ = spawner(args)
    assert c2.export() == before
