"""Crash recovery e2e: kill -9 a persistent server, respawn on the same --data-dir,
assert the space (and counters) come back exactly.

Note on what kill -9 can and can't test: written-but-unfsynced data lives in the OS page
cache, which survives process death — only power loss defeats it. So these tests verify
the recovery logic (scan, replay, counters) under every fsync policy; the policies'
loss-window differences are only observable under real power failure.
"""

import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from client import MorkClient, TxResult
from conftest import EXAMPLES_DIR, Spawner
from test_e2e import DIVERGING, PEANO_FOUR, RESULT_PATTERN


def _run_and_wait(client: MorkClient, src: str, outcome: str = "quiescent") -> TxResult:
    with client.events() as stream:
        res = client.run(src)
        stream.wait_for(outcome, tx=res.tx)
    return res


def test_recovery_after_quiescence(spawner: Spawner, tmp_path: Path) -> None:
    args = ["--data-dir", str(tmp_path), "--fsync", "always"]
    c1, p1 = spawner(args)
    _run_and_wait(c1, (EXAMPLES_DIR / "adder.metta").read_text())
    before = c1.export()

    p1.kill()
    p1.wait(timeout=10)

    c2, _ = spawner(args)
    assert c2.export() == before  # dumps are trie-order: byte-identical, not just set-equal
    assert c2.export(pattern=RESULT_PATTERN, template="_1") == [PEANO_FOUR]


def test_recovery_under_everysec(spawner: Spawner, tmp_path: Path) -> None:
    args = ["--data-dir", str(tmp_path)]  # default --fsync everysec
    c1, p1 = spawner(args)
    _run_and_wait(c1, "(fact 1)\n(exec 0 (, (fact $x)) (, (derived $x)))")
    before = c1.export()

    p1.kill()
    p1.wait(timeout=10)

    c2, _ = spawner(args)
    assert c2.export() == before


def test_crash_mid_execution_leaves_no_trace(spawner: Spawner, tmp_path: Path) -> None:
    """Under the current WAL schema, one record is written per COMMITTED transaction
    only (see wal.rs's module doc) — an uncommitted transaction is invisible, so a
    crash mid-run and an explicit abort are the same fact: "this never happened".
    There is no dangling TX record to re-run on restart, unlike the old sequential
    engine's log. A transaction killed before it ever reaches commit must therefore
    leave no trace at all after recovery, not partial progress."""
    c1, p1 = spawner(["--data-dir", str(tmp_path), "--fsync", "always"])
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(c1.run, DIVERGING)  # blocks until commit; DIVERGING never quiesces
        time.sleep(0.3)  # let it get properly mid-execution
        p1.kill()
        p1.wait(timeout=10)

    c2, _ = spawner(["--data-dir", str(tmp_path), "--fsync", "always"])
    assert c2.export() == []


def test_tx_counter_and_version_survive(spawner: Spawner, tmp_path: Path) -> None:
    args = ["--data-dir", str(tmp_path), "--fsync", "always"]
    c1, p1 = spawner(args)
    last = [_run_and_wait(c1, f"(fact {i})") for i in range(3)][-1]

    p1.kill()
    p1.wait(timeout=10)

    c2, _ = spawner(args)
    with c2.events() as stream:
        assert stream.hello.data["version"] == last.version  # replay reproduced the count
        res = c2.run("(after 1)")
        stream.wait_for("quiescent", tx=res.tx)
    assert int(res.tx[2:].split("_")[0]) == 4  # tx1..tx3 replayed → next is tx4
    assert res.version == last.version + 1


def test_checkpoint_restore_and_log_gc(spawner: Spawner, tmp_path: Path) -> None:
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
    assert "wal-000001.log" in names  # the tail segment

    p1.kill()
    p1.wait(timeout=10)

    c2, _ = spawner(args)  # checkpoint restore + tail replay
    assert c2.export() == before
    with c2.events() as stream:
        assert stream.hello.data["version"] == last.version
        res = c2.run("(after 1)")
        stream.wait_for("quiescent", tx=res.tx)
    assert int(res.tx[2:].split("_")[0]) == 4  # counter: meta (2) + tail replay (3) + 1


def test_clean_restart(spawner: Spawner, tmp_path: Path) -> None:
    args = ["--data-dir", str(tmp_path)]
    c1, p1 = spawner(args)
    _run_and_wait(c1, "(persistent fact)")
    before = c1.export()

    p1.terminate()  # SIGTERM: no graceful handler — same recovery path as a crash
    p1.wait(timeout=10)

    c2, _ = spawner(args)
    assert c2.export() == before


def test_concurrent_writes_survive_restart(spawner: Spawner, tmp_path: Path) -> None:
    """Transactions committed under --workers 4 must all be present after a restart."""
    args = ["--data-dir", str(tmp_path), "--workers", "4"]
    c1, p1 = spawner(args)
    with ThreadPoolExecutor(max_workers=4) as pool:
        # Fired concurrently, not one at a time: up to 4 may genuinely be in flight
        # together, so the resulting log has interleaved base_versions, not a serial chain.
        list(pool.map(c1.run, (f"(edge n{i} m{i})" for i in range(20))))
    before = c1.export()

    p1.kill()
    p1.wait(timeout=10)

    c2, _ = spawner(args)
    assert sorted(c2.export()) == sorted(before)


def test_recovery_is_order_independent_of_worker_count(spawner: Spawner, tmp_path: Path) -> None:
    """Replay follows the log's commit order, not the worker count, so recovering
    a 4-worker log with 1 worker must produce the same space."""
    c1, p1 = spawner(["--data-dir", str(tmp_path), "--workers", "4"])
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(c1.run, (f"(edge n{i} m{i})" for i in range(20))))
    expected = sorted(c1.export())

    p1.kill()
    p1.wait(timeout=10)

    c2, _ = spawner(["--data-dir", str(tmp_path), "--workers", "1"])
    assert sorted(c2.export()) == expected
