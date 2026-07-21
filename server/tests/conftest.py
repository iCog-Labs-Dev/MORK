"""Fixtures: build the release binary once, spawn a fresh server (fresh Space) per test."""

import socket
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
import requests

from client import MorkClient

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLES_DIR = REPO_ROOT / "server" / "examples"


@pytest.fixture(scope="session")
def mork_binary() -> Path:
    subprocess.run(
        ["cargo", "+nightly", "build", "--release", "-p", "mork-server"],
        cwd=REPO_ROOT,
        check=True,
    )
    return REPO_ROOT / "target" / "release" / "mork-server"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _launch(mork_binary: Path, addr: str, extra_args: list[str]) -> subprocess.Popen:
    return subprocess.Popen(
        [str(mork_binary), "--addr", addr, *extra_args],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _wait_ready(proc: subprocess.Popen, addr: str) -> None:
    deadline = time.monotonic() + 10.0
    while True:
        try:
            requests.get(f"http://{addr}/export", timeout=1.0).raise_for_status()
            return
        except requests.exceptions.RequestException:
            if proc.poll() is not None:
                raise RuntimeError(f"server exited early: code {proc.returncode}") from None
            if time.monotonic() > deadline:
                raise RuntimeError("server not ready within 10 s") from None
            time.sleep(0.05)


@pytest.fixture
def server(mork_binary: Path, request: pytest.FixtureRequest) -> Iterator[MorkClient]:
    """Fresh server per test. Parametrize indirectly to pass extra CLI flags:
    @pytest.mark.parametrize("server", [["--step-budget", "5"]], indirect=True)"""
    extra_args: list[str] = getattr(request, "param", [])
    addr = f"127.0.0.1:{_free_port()}"
    proc = _launch(mork_binary, addr, extra_args)
    client = MorkClient(f"http://{addr}")
    try:
        _wait_ready(proc, addr)
        yield client
    finally:
        client.close()
        proc.terminate()
        proc.wait(timeout=10)


@pytest.fixture
def spawner(mork_binary: Path):
    """Full-control spawning for crash-recovery tests: `spawn(extra_args)` returns
    `(client, proc)`; kill/respawn at will (e.g. same --data-dir), everything spawned is
    cleaned up at teardown."""
    procs: list[subprocess.Popen] = []
    clients: list[MorkClient] = []

    def spawn(extra_args: list[str]) -> tuple[MorkClient, subprocess.Popen]:
        addr = f"127.0.0.1:{_free_port()}"
        proc = _launch(mork_binary, addr, extra_args)
        procs.append(proc)
        _wait_ready(proc, addr)
        client = MorkClient(f"http://{addr}")
        clients.append(client)
        return client, proc

    yield spawn
    for client in clients:
        client.close()
    for proc in procs:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
