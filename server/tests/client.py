"""Typed HTTP/SSE client for mork-server — the only module that knows the wire format."""

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from types import TracebackType
from typing import Any

import requests


class MorkError(Exception):
    """Error-shaped response from the server (or a broken event stream)."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"{status}: {message}")
        self.status = status
        self.message = message


@dataclass(frozen=True)
class TxResult:
    """Successful `POST /run` response."""

    tx: str
    count: int
    version: int


@dataclass(frozen=True)
class SseEvent:
    """One decoded SSE frame: `event:` name plus parsed `data:` JSON."""

    name: str
    data: dict[str, Any]


def iter_sse(lines: Iterable[str]) -> Iterator[SseEvent]:
    """Decode SSE frames from a line stream.

    The server always writes `event: <name>\\ndata: <json>\\n\\n`, so a frame is complete
    at its data line (no reliance on blank-line handling, which `iter_lines` munges).
    """
    name: str | None = None
    for line in lines:
        if line.startswith("event: "):
            name = line.removeprefix("event: ")
        elif line.startswith("data: ") and name is not None:
            yield SseEvent(name, json.loads(line.removeprefix("data: ")))
            name = None


class EventStream:
    """A live `/events` subscription.

    The `hello` frame is consumed on construction, so once you hold an EventStream the
    subscription is provably active — open it *before* acting (subscribe-then-act), or
    events can be lost: the broadcast has no replay.
    """

    def __init__(self, response: requests.Response) -> None:
        self._response = response
        self._events = iter_sse(response.iter_lines(decode_unicode=True))
        self.hello = next(self._events, None) or self._bad_stream("no hello frame")
        if self.hello.name != "hello":
            self._bad_stream(f"expected hello frame, got {self.hello.name!r}")

    @staticmethod
    def _bad_stream(message: str) -> SseEvent:
        raise MorkError(0, message)

    def __enter__(self) -> "EventStream":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def __iter__(self) -> Iterator[SseEvent]:
        return self._events

    def close(self) -> None:
        self._response.close()

    def wait_for(self, name: str, tx: str | None = None) -> SseEvent:
        """Consume events until one named `name` (and matching `tx`, if given) arrives."""
        for ev in self._events:
            if ev.name == name and (tx is None or ev.data.get("tx") == tx):
                return ev
        return self._bad_stream(f"stream ended before {name!r} event")

    def wait_for_all(self, name: str, txs: Iterable[str]) -> None:
        """Consume events until `name` has been seen for every tx in `txs`, in any order."""
        pending = set(txs)
        for ev in self._events:
            if ev.name == name:
                pending.discard(str(ev.data.get("tx")))
                if not pending:
                    return
        self._bad_stream(f"stream ended with {name!r} still pending for {sorted(pending)}")


class MorkClient:
    """Blocking client for one mork-server instance."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self._session = requests.Session()

    def close(self) -> None:
        self._session.close()

    def run(self, source: str) -> TxResult:
        """Submit a transaction; raises MorkError on any non-ok response."""
        resp = self.run_raw(source.encode())
        body: dict[str, Any] = resp.json()
        if resp.status_code != 200 or not body.get("ok"):
            raise MorkError(resp.status_code, str(body.get("error", body)))
        return TxResult(tx=body["tx"], count=body["count"], version=body["version"])

    def run_raw(self, body: bytes) -> requests.Response:
        """Raw `POST /run` for exercising error paths (bad syntax, non-UTF-8, …)."""
        return self._session.post(f"{self.base_url}/run", data=body, timeout=30.0)

    def sweep_start(self) -> str:
        """`POST /sweep/start`; returns the server's sweep handle name."""
        body = self._sweep_control("start")
        return str(body["handle"])

    def sweep_pause(self) -> None:
        """`POST /sweep/pause`."""
        self._sweep_control("pause")

    def sweep_resume(self) -> None:
        """`POST /sweep/resume`."""
        self._sweep_control("resume")

    def sweep_stop(self) -> None:
        """`POST /sweep/stop`."""
        self._sweep_control("stop")

    def _sweep_control(self, action: str) -> dict[str, Any]:
        resp = self._session.post(f"{self.base_url}/sweep/{action}", timeout=30.0)
        body: dict[str, Any] = resp.json()
        if resp.status_code != 200 or not body.get("ok"):
            raise MorkError(resp.status_code, str(body.get("error", body)))
        return body

    def export(self, pattern: str | None = None, template: str | None = None) -> list[str]:
        """`GET /export` — full dump with no args, query with both."""
        if (pattern is None) != (template is None):
            raise ValueError("pattern and template must be provided together")
        params = {} if pattern is None else {"pattern": pattern, "template": template}
        resp = self._session.get(f"{self.base_url}/export", params=params, timeout=30.0)
        if resp.status_code != 200:
            raise MorkError(resp.status_code, resp.text)
        return [line for line in resp.text.splitlines() if line]

    def events(
        self,
        tx: str | None = None,
        deltas: bool = False,
        read_timeout: float = 30.0,
    ) -> EventStream:
        """Open `GET /events`; the returned stream has already consumed its hello frame."""
        params: dict[str, str] = {}
        if tx is not None:
            params["tx"] = tx
        if deltas:
            params["deltas"] = "true"
        resp = self._session.get(
            f"{self.base_url}/events",
            params=params,
            stream=True,
            timeout=(5.0, read_timeout),
        )
        if resp.status_code != 200:
            raise MorkError(resp.status_code, resp.text)
        return EventStream(resp)
