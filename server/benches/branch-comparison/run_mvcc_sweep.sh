#!/usr/bin/env bash
# Sweep --workers in {1,2,4,8,16} for the metta-calculus-server branch's mork-server,
# mirroring server/benches/concurrency.md's runbook (same host, same core split), at
# 30s/row instead of 60s/row to fit this measurement session's time budget.
set -uo pipefail
REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
cd "$REPO"
RESULTS="$1"
BIN=./target/release/mork-server
DUR=30s

pkill -x mork-server 2>/dev/null
sleep 1

for W in 1 2 4 8 16; do
  echo "=== mvcc --workers $W ==="
  taskset -c 12-21 "$BIN" --addr 127.0.0.1:8099 --workers "$W" > "$RESULTS/mvcc-w$W.server.log" 2>&1 &
  SPID=$!
  for i in $(seq 1 50); do
    curl -s -o /dev/null http://127.0.0.1:8099/stats && break
    sleep 0.1
  done
  V0=$(curl -s http://127.0.0.1:8099/stats | python3 -c "import json,sys;print(json.load(sys.stdin)['version'])")
  T0=$(date +%s.%N)

  ( cd server/tests && taskset -c 0-11 uv run locust --headless --processes 8 -u 41 -r 41 -t "$DUR" \
      --host http://127.0.0.1:8099 --csv "$RESULTS/mvcc-w$W" \
      ShortWriteUser LongWriteUser WatcherUser > "$RESULTS/mvcc-w$W.locust.log" 2>&1 )

  T1=$(date +%s.%N)
  V1=$(curl -s http://127.0.0.1:8099/stats | python3 -c "import json,sys;print(json.load(sys.stdin)['version'])")
  echo "version_delta=$((V1-V0)) elapsed=$(python3 -c "print($T1-$T0)")" > "$RESULTS/mvcc-w$W.statscheck.txt"

  kill "$SPID" 2>/dev/null
  wait "$SPID" 2>/dev/null
  pkill -x mork-server 2>/dev/null
  sleep 1
done
echo "MVCC SWEEP DONE"
