#!/usr/bin/env python3
"""Extract transport sweep benchmark JSON (Google Benchmark format) into a CSV.

Usage:
  python tools/extract_benchmark_json_to_csv.py output.csv uds_inprocess_sweep.json shmem_sweep.json [more.json...]

The script infers transport from the benchmark name patterns:
  BM_UnaryPingPong<UDS,...>          -> uds
  BM_UnaryPingPong<InProcess,...>    -> inproc
  BM_ShmemPromiseUnaryPingPong/...   -> shmem

For each benchmark entry whose name ends with '/<req>/<resp>' it emits one CSV row:
  transport,bench_name,req_bytes,resp_bytes,cpu_time_ns,real_time_ns,bytes_per_second

Rows that contain metadata mutation variants (with '<Random...' in the angle brackets)
are still included; you can filter later if desired.

This provides a simple uniform dataset ready for plotting throughput curves by size.
"""
from __future__ import annotations

import json
import re
import sys
import csv
from pathlib import Path
from typing import Iterable, List

SIZE_SUFFIX_RE = re.compile(r"/(\d+)/(\d+)$")

TRANSPORT_PATTERNS = [
    (re.compile(r"^BM_UnaryPingPong<UDS,"), "uds"),
    (re.compile(r"^BM_UnaryPingPong<InProcess,"), "inproc"),
    (re.compile(r"^BM_ShmemPromiseUnaryPingPong/"), "shmem"),
]

def infer_transport(name: str) -> str | None:
    for pat, t in TRANSPORT_PATTERNS:
        if pat.search(name):
            return t
    return None

def extract_rows(json_path: Path) -> List[dict]:
    data = json.loads(json_path.read_text())
    benches = data.get("benchmarks", [])
    out: List[dict] = []
    for b in benches:
        name = b.get("name") or b.get("run_name") or ""
        m = SIZE_SUFFIX_RE.search(name)
        if not m:
            continue  # skip non size-suffixed entries
        req_sz, resp_sz = m.groups()
        transport = infer_transport(name)
        if transport is None:
            continue
        row = {
            "transport": transport,
            "bench_name": name,
            "req_bytes": req_sz,
            "resp_bytes": resp_sz,
            "cpu_time_ns": b.get("cpu_time"),
            "real_time_ns": b.get("real_time"),
            "bytes_per_second": b.get("bytes_per_second"),
        }
        out.append(row)
    return out

def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    out_csv = Path(argv[1])
    fieldnames = ["transport","bench_name","req_bytes","resp_bytes","cpu_time_ns","real_time_ns","bytes_per_second"]
    rows: List[dict] = []
    for p in argv[2:]:
        path = Path(p)
        if not path.exists():
            print(f"Warning: {path} does not exist, skipping", file=sys.stderr)
            continue
        rows.extend(extract_rows(path))
    with out_csv.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    print(f"Wrote {out_csv} with {len(rows)} rows")
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
