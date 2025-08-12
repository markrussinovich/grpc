#!/usr/bin/env python3
"""Generate comparison plots for transport performance sweeps.

Reads the unified CSV produced by extract_benchmark_json_to_csv.py and
outputs a PNG (and optionally SVG) containing throughput curves for
InProcess, UDS, and shmem transports for symmetric request/response sizes.

Usage:
    python tools/plot_transport_sweeps.py \
            --csv transport_sweeps.csv \
            [--out transport_throughput.png] [--svg transport_throughput.svg] \
            [--out-dir .perf_tmp]

Options:
    --csv PATH      Input CSV (default: transport_sweeps.csv)
    --out PATH      Output PNG path (overrides --out-dir default)
    --svg PATH      Optional SVG output.
    --out-dir DIR   Directory to place outputs (default: .perf_tmp). Ignored if --out supplied.
  --include-zero  Include 0/0 point (default: exclude)
    --all           Include all symmetric rows (even with metadata mutators).

The CSV row format is:
  transport,bench_name,req_bytes,resp_bytes,cpu_time_ns,real_time_ns,bytes_per_second

We filter rows where req_bytes == resp_bytes ("symmetric") and (unless --all)
the benchmark name contains 'NoOpMutator' to avoid metadata overhead variants.

Throughput is converted to MiB/s for readability.
"""
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

try:
    import matplotlib.pyplot as plt  # type: ignore
except ModuleNotFoundError as e:  # pragma: no cover - runtime dependency notice
    raise SystemExit("matplotlib is required. Install with: pip install matplotlib") from e


@dataclass
class Point:
    size: int
    mib_per_s: float


def load_points(csv_path: Path, include_zero: bool, include_all: bool) -> Dict[str, List[Point]]:
    transports: Dict[str, Dict[int, float]] = {}
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                transport = row["transport"].strip()
                req = int(row["req_bytes"]) if row["req_bytes"] else 0
                resp = int(row["resp_bytes"]) if row["resp_bytes"] else 0
                bps = float(row["bytes_per_second"]) if row["bytes_per_second"] else 0.0
                name = row["bench_name"]
            except (KeyError, ValueError):  # malformed row
                continue
            if req != resp:
                continue  # only symmetric sizes
            if req == 0 and not include_zero:
                continue
            if not include_all:
                # Default policy: include canonical baseline variants:
                #  - InProcess/UDS NoOpMutator
                #  - ShmemPromise benchmarks
                if ("NoOpMutator" not in name) and ("ShmemPromise" not in name):
                    continue
            if bps <= 0:
                continue
            mib_per_s = bps / (1024 * 1024)
            transports.setdefault(transport, {})[req] = max(
                mib_per_s, transports.setdefault(transport, {}).get(req, 0.0)
            )
    # Convert dicts to sorted point lists
    out: Dict[str, List[Point]] = {}
    for t, mp in transports.items():
        out[t] = [Point(size=s, mib_per_s=mp[s]) for s in sorted(mp.keys())]
    return out


def make_plot(data: Dict[str, List[Point]], out_png: Path, out_svg: Path | None) -> None:
    if not data:
        raise RuntimeError("No data to plot")
    # Consistent color mapping for the three expected transports
    color_map = {
        "inproc": "#1b9e77",
        "shmem": "#d95f02",
        "uds": "#7570b3",
    }
    plt.figure(figsize=(10, 6))
    for transport, points in sorted(data.items()):
        if not points:
            continue
        x = [p.size for p in points]
        y = [p.mib_per_s for p in points]
        label = transport
        plt.plot(x, y, marker="o", linewidth=1.6, label=label, color=color_map.get(transport))
    plt.xscale("log")
    plt.yscale("log")
    plt.xlabel("Payload size (bytes) [symmetric req=resp]")
    plt.ylabel("Throughput (MiB/s)")
    plt.title("gRPC Unary Ping-Pong Throughput by Transport (DEBUG build)")
    plt.grid(True, which="both", linestyle=":", linewidth=0.5)
    plt.legend()
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png, dpi=160)
    if out_svg:
        plt.savefig(out_svg)
    expected = {"inproc", "shmem", "uds"}
    present = set(data.keys())
    missing = expected - present
    note = f" (missing: {','.join(sorted(missing))})" if missing else ""
    print(f"Wrote {out_png}" + (f" and {out_svg}" if out_svg else "") + note)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Plot transport sweep throughput curves")
    ap.add_argument("--csv", default="transport_sweeps.csv", type=Path)
    ap.add_argument("--out", type=Path, help="Explicit output PNG path (else <out-dir>/transport_throughput.png)")
    ap.add_argument("--svg", type=Path, default=None)
    ap.add_argument("--out-dir", type=Path, default=Path(".perf_tmp"), help="Directory for output artifacts")
    ap.add_argument("--include-zero", action="store_true", help="Include 0/0 size point")
    ap.add_argument("--all", action="store_true", help="Include metadata mutation variants")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    if not args.csv.exists():
        raise SystemExit(f"Input CSV not found: {args.csv}")
    data = load_points(args.csv, include_zero=args.include_zero, include_all=args.all)
    if not data:
        raise SystemExit("No data matched filters; consider --all or --include-zero")
    out_png = args.out if args.out else args.out_dir / "transport_throughput.png"
    out_svg = args.svg if args.svg else (args.out_dir / "transport_throughput.svg" if args.out is None else None)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    make_plot(data, out_png, out_svg)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
