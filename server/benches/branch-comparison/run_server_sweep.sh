#!/usr/bin/env bash
# Sweep the `server` branch's mork-server across the concurrency levels used in
# concurrency.md, at 30s/row. There is no --workers flag on this branch: the
# admission-control ceiling (WorkerPool::thread_count = tokio async worker threads - 1)
# is sized from std::thread::available_parallelism(), which respects CPU affinity on
# Linux -- so `taskset -c 12-<12+N-1>` is the literal mechanism, not an approximation,
# for setting N on this branch. The reserved partition is only 10 cores (12-21), so the
# "16" row is capped at 10 -- flagged explicitly in the results, not faked by reaching
# into the load generator's core range.
#
# WorkerPool::new() asserts thread_count >= 1 where thread_count = tokio_worker_threads
# - 1: a single core makes that 0 and the process panics on boot. The "1" row therefore
# uses 2 cores (the practical floor) and is labeled as such in the results, not silently
# relabeled to look like a true single-thread row.
set -uo pipefail
REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
BIN="$1"          # path to server-branch mork-server binary
LOCUSTFILE="$2"   # path to server_branch_locustfile.py
RESULTS="$3"
DUR=30s

pkill -x mork-server 2>/dev/null
sleep 1

declare -A CORES=( [1]="12-13" [2]="12-13" [4]="12-15" [8]="12-19" [16]="12-21" )

for W in 1 2 4 8 16; do
  echo "=== server taskset=${CORES[$W]} (target N=$W) ==="
  taskset -c "${CORES[$W]}" env MORK_SERVER_ADDR=127.0.0.1 MORK_SERVER_PORT=8099 "$BIN" \
      > "$RESULTS/server-w$W.server.log" 2>&1 &
  SPID=$!
  for i in $(seq 1 50); do
    curl -s -o /dev/null http://127.0.0.1:8099/status/- && break
    sleep 0.1
  done

  taskset -c 0-11 uv run --project "$REPO/server/tests" \
      locust -f "$LOCUSTFILE" --headless --processes 8 -u 41 -r 41 -t "$DUR" \
      --host http://127.0.0.1:8099 --csv "$RESULTS/server-w$W" \
      ShortWriteUser LongWriteUser > "$RESULTS/server-w$W.locust.log" 2>&1

  kill "$SPID" 2>/dev/null
  wait "$SPID" 2>/dev/null
  pkill -x mork-server 2>/dev/null
  sleep 1
done
echo "SERVER SWEEP DONE"
