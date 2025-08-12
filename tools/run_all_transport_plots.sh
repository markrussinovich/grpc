#!/usr/bin/env bash
# Generate throughput, latency, and ops/sec plots for transports into .perf_tmp directory.
# Creates .perf_tmp if missing. Accepts optional CSV path (default transport_sweeps.csv).
set -euo pipefail
CSV=${1:-transport_sweeps.csv}
OUTDIR=.perf_tmp
mkdir -p "$OUTDIR"

if [[ ! -f "$CSV" ]]; then
  echo "CSV $CSV not found; run extract_benchmark_json_to_csv.py first." >&2
  exit 1
fi

python3 tools/plot_transport_sweeps.py \
  --csv "$CSV" \
  --out "$OUTDIR/transport_throughput.png" \
  --svg "$OUTDIR/transport_throughput.svg" \
  --include-zero

python3 tools/plot_transport_latency_ops.py \
  --csv "$CSV" \
  --latency-out "$OUTDIR/transport_latency.png" \
  --ops-out "$OUTDIR/transport_ops.png" \
  --latency-svg "$OUTDIR/transport_latency.svg" \
  --ops-svg "$OUTDIR/transport_ops.svg" \
  --include-zero

echo "Performance plots written to $OUTDIR"
