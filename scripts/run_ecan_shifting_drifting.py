#!/usr/bin/env python3
"""Run the real MORK/MM2 shifting-drifting experiment and write its CSV trace."""

import argparse
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("artifacts/ecan-shifting-drifting.csv"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.output if args.output.is_absolute() else root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["cargo", "run", "-p", "mork", "--example", "ecan_shifting_drifting", "--", str(output)],
        cwd=root,
        check=True,
    )


if __name__ == "__main__":
    main()
