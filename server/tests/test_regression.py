"""Golden-file regression suite.

A case is two files in `server/examples/`: `<name>.metta` (the program) and `<name>.check`
(JSON: `pattern`, `template`, `expected` lines, optional `timeout_s`). Adding a regression
test means dropping those two files — no code.
"""

import json
from pathlib import Path

import pytest

from client import MorkClient
from conftest import EXAMPLES_DIR

CHECKS = sorted(EXAMPLES_DIR.glob("*.check"))


@pytest.mark.parametrize("check", CHECKS, ids=[c.stem for c in CHECKS])
def test_example(server: MorkClient, check: Path) -> None:
    spec = json.loads(check.read_text())
    source = check.with_suffix(".metta").read_text()
    with server.events(read_timeout=float(spec.get("timeout_s", 30))) as stream:
        res = server.run(source)
        stream.wait_for("quiescent", tx=res.tx)
    out = server.export(pattern=spec["pattern"], template=spec["template"])
    for line in spec["expected"]:
        assert line in out, f"missing {line!r} in export ({len(out)} lines)"
