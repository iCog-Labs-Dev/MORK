#!/usr/bin/env python3
"""Persistent Python worker for MORK Python Source/Sink integration.

Protocol:
- Requests and responses are JSON objects framed as:
  4-byte big-endian payload length + UTF-8 JSON payload.
- The worker is session-scoped: one process serves one named session.

Supported request commands:
- import
- import-file
- getattr
- get-variable
- set-variable
- call
- call-method
- drop-object

Responses are JSON objects of the form:
- {"status": "ok", "result": ...}
- {"status": "error", "type": ..., "message": ..., "traceback": ...}

The worker preserves complex Python objects in an internal handle registry and
returns opaque handles in the form @py:<session>:<id>.
"""

from __future__ import annotations

import argparse
import base64
import importlib
import importlib.util
import json
import os
import struct
import sys
import traceback
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple


Handle = str


def _is_handle(value: Any) -> bool:
    return isinstance(value, str) and value.startswith("@py:")


@dataclass
class SessionState:
    session_name: str
    objects_map: Dict[Handle, Any]
    next_id: int = 1

    def new_handle(self, obj: Any) -> Handle:
        handle = f"@py:{self.session_name}:{self.next_id}"
        self.next_id += 1
        self.objects_map[handle] = obj
        return handle

    def drop_handle(self, handle: Handle) -> None:
        self.objects_map.pop(handle, None)


def _read_exact(stream, size: int) -> bytes:
    chunks: List[bytes] = []
    remaining = size
    while remaining > 0:
        chunk = stream.buffer.read(remaining)
        if not chunk:
            if remaining == size:
                raise EOFError
            raise EOFError("unexpected end of framed input")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_frame(stream) -> Optional[dict]:
    header = stream.buffer.read(4)
    if not header:
        return None
    if len(header) != 4:
        raise EOFError("incomplete frame header")
    (size,) = struct.unpack(">I", header)
    payload = _read_exact(stream, size)
    return json.loads(payload.decode("utf-8"))


def write_frame(stream, message: Mapping[str, Any]) -> None:
    payload = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    stream.buffer.write(struct.pack(">I", len(payload)))
    stream.buffer.write(payload)
    stream.buffer.flush()


def encode_primitive(value: Any) -> Any:
    if value is None:
        return {"type": "none", "value": None}
    if isinstance(value, bool):
        return {"type": "bool", "value": value}
    if isinstance(value, int) and not isinstance(value, bool):
        return {"type": "int", "value": value}
    if isinstance(value, float):
        return {"type": "float", "value": value}
    if isinstance(value, str):
        return {"type": "str", "value": value}
    if isinstance(value, list):
        return {"type": "list", "value": [encode_primitive(item) for item in value]}
    if isinstance(value, tuple):
        return {"type": "list", "value": [encode_primitive(item) for item in value]}
    if isinstance(value, dict):
        items = []
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"dictionary key must be str, got {type(key).__name__}")
            items.append([key, encode_primitive(item)])
        return {"type": "dict", "value": items}
    return None


def encode_result(state: SessionState, value: Any) -> Any:
    primitive = encode_primitive(value)
    if primitive is not None:
        return primitive

    if _is_handle(value):
        # Preserve explicit handles as handles.
        return {"type": "handle", "value": value}

    handle = state.new_handle(value)
    return {"type": "handle", "value": handle}


def decode_value(state: SessionState, value: Any) -> Any:
    if isinstance(value, dict):
        value_type = value.get("type")
        if value_type == "none":
            return None
        if value_type == "bool":
            return bool(value.get("value"))
        if value_type == "int":
            return int(value.get("value"))
        if value_type == "float":
            return float(value.get("value"))
        if value_type == "str":
            return str(value.get("value"))
        if value_type == "list":
            raw_items = value.get("value", [])
            if not isinstance(raw_items, list):
                raise TypeError("list payload must be a list")
            return [decode_value(state, item) for item in raw_items]
        if value_type == "dict":
            raw_items = value.get("value", [])
            if not isinstance(raw_items, list):
                raise TypeError("dict payload must be a list")
            decoded: Dict[str, Any] = {}
            for entry in raw_items:
                if not isinstance(entry, list) or len(entry) != 2:
                    raise TypeError("dict entries must be [key, value]")
                key, item = entry
                if not isinstance(key, str):
                    raise TypeError("dict keys must be strings")
                decoded[key] = decode_value(state, item)
            return decoded
        if value_type == "handle":
            handle = str(value.get("value"))
            if handle not in state.objects_map:
                raise KeyError(f"unknown handle: {handle}")
            return state.objects_map[handle]

    if isinstance(value, str) and _is_handle(value):
        if value not in state.objects_map:
            raise KeyError(f"unknown handle: {value}")
        return state.objects_map[value]

    # Backward-compatible fallback for raw JSON primitives.
    return value


def _normalize_kwargs(state: SessionState, kwargs: Any) -> Dict[str, Any]:
    if kwargs is None:
        return {}
    if isinstance(kwargs, dict):
        return {str(key): decode_value(state, value) for key, value in kwargs.items()}
    if isinstance(kwargs, list):
        normalized: Dict[str, Any] = {}
        for entry in kwargs:
            if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                raise TypeError("kwargs entries must be pairs")
            key, value = entry
            normalized[str(key)] = decode_value(state, value)
        return normalized
    raise TypeError("kwargs must be a dict or list of pairs")


def _resolve_object(state: SessionState, ref: Any) -> Any:
    obj = decode_value(state, ref)
    if _is_handle(ref) and ref in state.objects_map:
        return state.objects_map[ref]
    return obj


def _collect_args(state: SessionState, args: Any) -> List[Any]:
    if args is None:
        return []
    if not isinstance(args, list):
        raise TypeError("args must be a list")
    return [decode_value(state, item) for item in args]


def _frame_import(state: SessionState, request: Mapping[str, Any]) -> Any:
    module_name = request["module"]
    module = importlib.import_module(module_name)
    return encode_result(state, module)


def _frame_import_file(state: SessionState, request: Mapping[str, Any]) -> Any:
    module_name = request["module"]
    path = request["path"]
    if not isinstance(module_name, str) or not module_name:
        raise ValueError("module alias must be a non-empty string")
    if not isinstance(path, str) or not path:
        raise ValueError("file path must be a non-empty string")

    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load Python module {module_name!r} from {path!r}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    globals()[module_name] = module

    # The module remains available in this persistent worker.  No module
    # handle is returned to MORK.
    return {"type": "none", "value": None}


def _frame_getattr(state: SessionState, request: Mapping[str, Any]) -> Any:
    target = _resolve_object(state, request["target"])
    attribute = request["attr"]
    return encode_result(state, getattr(target, attribute))


def _frame_get_variable(state: SessionState, request: Mapping[str, Any]) -> Any:
    name = request["name"]
    if name in globals():
        return encode_result(state, globals()[name])
    if name in state.objects_map:
        return encode_result(state, state.objects_map[name])
    raise KeyError(f"unknown variable: {name}")


def _frame_set_variable(state: SessionState, request: Mapping[str, Any]) -> Any:
    name = request["name"]
    value = decode_value(state, request["value"])
    globals()[name] = value
    return {"type": "none", "value": None}


def _frame_call(state: SessionState, request: Mapping[str, Any]) -> Any:
    module_name = request["module"]
    function_name = request["function"]
    module = importlib.import_module(module_name)
    function = getattr(module, function_name)
    args = _collect_args(state, request.get("args"))
    kwargs = _normalize_kwargs(state, request.get("kwargs"))
    return encode_result(state, function(*args, **kwargs))


def _frame_call_method(state: SessionState, request: Mapping[str, Any]) -> Any:
    target = _resolve_object(state, request["target"])
    method_name = request["method"]
    method = getattr(target, method_name)
    args = _collect_args(state, request.get("args"))
    kwargs = _normalize_kwargs(state, request.get("kwargs"))
    return encode_result(state, method(*args, **kwargs))


def _frame_drop_object(state: SessionState, request: Mapping[str, Any]) -> Any:
    handle = request["handle"]
    if not isinstance(handle, str):
        raise TypeError("handle must be a string")
    state.drop_handle(handle)
    return {"type": "none", "value": None}


def dispatch_request(state: SessionState, request: Mapping[str, Any]) -> Any:
    command = request.get("command") or request.get("op")
    if command is None:
        raise KeyError("request is missing command")

    dispatch = {
        "import": _frame_import,
        "import-file": _frame_import_file,
        "getattr": _frame_getattr,
        "get-variable": _frame_get_variable,
        "set-variable": _frame_set_variable,
        "call": _frame_call,
        "call-method": _frame_call_method,
        "drop-object": _frame_drop_object,
    }
    if command not in dispatch:
        raise KeyError(f"unsupported command: {command}")

    result = dispatch[command](state, request)
    return {"status": "ok", "result": result}


def error_response(exc: Exception) -> Dict[str, Any]:
    return {
        "status": "error",
        "type": exc.__class__.__name__,
        "message": str(exc),
        "traceback": traceback.format_exc(),
    }


def coerce_session_name(argv: List[str]) -> str:
    parser = argparse.ArgumentParser(description="MORK Python worker")
    parser.add_argument("session_name", nargs="?", default=None, help="persistent session identifier")
    parser.add_argument("--session", dest="session_option", default=None, help="persistent session identifier")
    args = parser.parse_args(argv)
    session_name = args.session_option or args.session_name or os.environ.get("MORK_PY_SESSION") or os.environ.get("PY_SESSION")
    if not session_name:
        raise SystemExit("session name is required (argument, --session, or MORK_PY_SESSION)")
    return session_name


def main(argv: List[str]) -> int:
    session_name = coerce_session_name(argv)
    state = SessionState(session_name=session_name, objects_map={})

    while True:
        try:
            request = read_frame(sys.stdin)
            if request is None:
                return 0
            if not isinstance(request, dict):
                raise TypeError("request payload must be a JSON object")
            response = dispatch_request(state, request)
        except EOFError:
            return 0
        except Exception as exc:
            response = error_response(exc)
        write_frame(sys.stdout, response)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
