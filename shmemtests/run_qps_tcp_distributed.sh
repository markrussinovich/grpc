#!/usr/bin/env bash
set -euo pipefail

# Distributed-mode TCP QPS runner for gRPC C++.
# Starts one server worker and N client workers on localhost, exports QPS_WORKERS,
# runs qps_json_driver with the provided scenario, then quits the workers.
#
# Usage:
#   ./run_qps_tcp_distributed.sh \
#       --clients 1 \
#       --base_port 20000 \
#       --host localhost \
#       --scenarios_json '{"scenarios":[{...}]}' \
#       [--driver_flags '...']  # optional extra flags to qps_json_driver
#
# Or:
#   ./run_qps_tcp_distributed.sh --clients 2 --scenarios_file path/to/scenarios.json
#
# Notes:
# - Requires bazel-built binaries:
#     bazel build //test/cpp/qps:qps_worker //test/cpp/qps:qps_json_driver
# - QPS_WORKERS is constructed as "server-worker,client-worker[,client-worker...]"
#   per the official performance README.

CLIENTS=1
BASE_PORT=20000
HOST="localhost"
SCENARIOS_JSON=""
SCENARIOS_FILE=""
DRIVER_FLAGS=""

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --clients)        CLIENTS="$2"; shift 2;;
    --base_port)      BASE_PORT="$2"; shift 2;;
    --host)           HOST="$2"; shift 2;;
    --scenarios_json) SCENARIOS_JSON="$2"; shift 2;;
    --scenarios_file) SCENARIOS_FILE="$2"; shift 2;;
    --driver_flags)   DRIVER_FLAGS="$2"; shift 2;;
    -h|--help)
      echo "See script header for usage."; exit 0;;
    *) echo "Unknown arg: $1"; exit 1;;
  esac
done

# Binaries
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
WORKER_BIN="$REPO_ROOT/bazel-bin/test/cpp/qps/qps_worker"
DRIVER_BIN="$REPO_ROOT/bazel-bin/test/cpp/qps/qps_json_driver"

[[ -x "$WORKER_BIN" ]] || { echo "Missing $WORKER_BIN (run: bazel build //test/cpp/qps:qps_worker)"; exit 1; }
[[ -x "$DRIVER_BIN" ]] || { echo "Missing $DRIVER_BIN (run: bazel build //test/cpp/qps:qps_json_driver)"; exit 1; }

# Small helper: wait until a TCP port is listening
wait_for_port() {
  local host="$1" port="$2" deadline=$((SECONDS+15))
  while ! (exec 3<>/dev/tcp/"$host"/"$port") 2>/dev/null; do
    [[ $SECONDS -ge $deadline ]] && { echo "Timeout waiting for $host:$port"; return 1; }
    sleep 0.05
  done
  exec 3>&-  # close
}

# Start server worker
SERVER_PORT="$BASE_PORT"
echo "[qps] starting SERVER worker at $HOST:$SERVER_PORT"
"$WORKER_BIN" --driver_port="$SERVER_PORT" --server_port=0 >"server_worker.$SERVER_PORT.log" 2>&1 &
SERVER_PID=$!
wait_for_port "$HOST" "$SERVER_PORT"

# Start client workers
WORKERS="$HOST:$SERVER_PORT"
PIDS=("$SERVER_PID")
for ((i=1; i<=CLIENTS; ++i)); do
  PORT=$((BASE_PORT+i))
  echo "[qps] starting CLIENT worker $i at $HOST:$PORT"
  "$WORKER_BIN" --driver_port="$PORT" --server_port=0 >"client_worker.$PORT.log" 2>&1 &
  PIDS+=($!)
  wait_for_port "$HOST" "$PORT"
  WORKERS="$WORKERS,$HOST:$PORT"
done

export QPS_WORKERS="$WORKERS"
echo "[qps] QPS_WORKERS=$QPS_WORKERS"

# If no scenario provided, use a simple SYNC unary closed-loop sanity run
if [[ -z "$SCENARIOS_JSON" && -z "$SCENARIOS_FILE" ]]; then
  SCENARIOS_JSON='{"scenarios":[{"name":"tcp-unary-sync","num_clients":1,"num_servers":1,"warmup_seconds":1,"benchmark_seconds":3,"client_config":{"client_type":"SYNC_CLIENT","rpc_type":"UNARY","outstanding_rpcs_per_channel":1,"client_channels":1,"security_params":{"use_test_ca":true},"load_params":{"closed_loop":{}}},"server_config":{"server_type":"SYNC_SERVER","security_params":{"use_test_ca":true}}}]}'
fi

set +e
echo "[qps] running driver..."
if [[ -n "$SCENARIOS_JSON" ]]; then
  "$DRIVER_BIN" --scenarios_json="$SCENARIOS_JSON" $DRIVER_FLAGS
else
  "$DRIVER_BIN" --scenarios_file="$SCENARIOS_FILE" $DRIVER_FLAGS
fi
DRV_RC=$?

echo "[qps] sending --quit to workers..."
"$DRIVER_BIN" --quit >/dev/null 2>&1 || true

# If any worker is still around, kill it
for pid in "${PIDS[@]}"; do
  if kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null || true; fi
done

exit "$DRV_RC"
