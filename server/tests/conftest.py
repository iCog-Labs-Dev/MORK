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


@pytest.fixture
def server(mork_binary: Path, request: pytest.FixtureRequest) -> Iterator[MorkClient]:
    """Fresh server per test. Parametrize indirectly to pass extra CLI flags:
    @pytest.mark.parametrize("server", [["--step-budget", "5"]], indirect=True)"""
    extra_args: list[str] = getattr(request, "param", [])
    addr = f"127.0.0.1:{_free_port()}"
    proc = subprocess.Popen(
        [str(mork_binary), "--addr", addr, *extra_args],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = MorkClient(f"http://{addr}")
    try:
        deadline = time.monotonic() + 10.0
        while True:
            try:
                requests.get(f"http://{addr}/export", timeout=1.0).raise_for_status()
                break
            except requests.exceptions.RequestException:
                if proc.poll() is not None:
                    raise RuntimeError(f"server exited early: code {proc.returncode}") from None
                if time.monotonic() > deadline:
                    raise RuntimeError("server not ready within 10 s") from None
                time.sleep(0.05)
        yield client
    finally:
        client.close()
        proc.terminate()
        proc.wait(timeout=10)
