#!/usr/bin/env python3
import argparse, csv, json, os, re, subprocess, sys, tempfile
from pathlib import Path

import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parent
BIN = REPO / "bazel-bin/test/cpp/microbenchmarks/bm_fullstack_streaming_ping_pong"

# Heuristics to recognize transports from the benchmark name
TRANSPORT_TOKENS = {
    "InProcess": ["InProcess", "Inproc"],
    "ShmemTransport": ["ShmemTransport", "Shmem"],
    "UDS": ["UDS", "UnixDomain", "Unix"],
    "TCP": ["TCP", "Tcp", "chttp2"],  # chttp2 is the TCP transport in core
}

def ensure_built():
    if not BIN.exists():
        subprocess.check_call([
            "bazel", "build", "//test/cpp/microbenchmarks:bm_fullstack_streaming_ping_pong"
        ])

def run_bench(min_time, outfile_csv, extra_filter=None):
    # Limit to streaming ping-pong benches; let the binary enumerate sizes/variants
    filt = r"BM_StreamingPingPong.*"
    if extra_filter:
        filt = extra_filter
    cmd = [
        str(BIN),
        f"--benchmark_min_time={min_time}",
        "--benchmark_out_format=csv",
        f"--benchmark_out={outfile_csv}",
        f"--benchmark_filter={filt}",
    ]
    print("[run]", " ".join(cmd))
    subprocess.check_call(cmd)

def parse_csv(csv_path):
    # Returns list of dicts: {name, transport, msg_size, real_time_us}
    rows = []
    with open(csv_path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            name = row.get("name") or row.get("benchmark_name") or ""
            # Expect suffix like .../MSG/COUNT[/...]; take first numeric as msg size
            m = re.search(r"/(\d+)(?:/|$)", name)
            msg_size = int(m.group(1)) if m else None

            # Guess transport by substring tokens
            transport = None
            for label, toks in TRANSPORT_TOKENS.items():
                if any(t in name for t in toks):
                    transport = label
                    break
            if transport is None:
                # Skip weird specializations we don't care about
                continue

            # time value & unit
            rt = float(row.get("real_time", row.get("cpu_time", "nan")))
            unit = row.get("time_unit", "ns")
            if unit == "ns":
                rt_us = rt / 1000.0
            elif unit == "us":
                rt_us = rt
            elif unit == "ms":
                rt_us = rt * 1000.0
            else:
                rt_us = rt  # best effort

            rows.append(dict(name=name, transport=transport,
                             msg_size=msg_size, real_time_us=rt_us))
    return rows

def plot(rows, out_png):
    # group by transport, sort by size
    series = {}
    for r in rows:
        if r["msg_size"] is None: 
            continue
        series.setdefault(r["transport"], []).append((r["msg_size"], r["real_time_us"]))
    for t in series:
        series[t] = sorted(series[t], key=lambda x: x[0])

    # Figure layout similar to your example
    fig = plt.figure(figsize=(14, 10))
    fig.suptitle("Streaming RPC Performance (Ping-Pong)", fontsize=16, fontweight="bold")

    # TL: 0-payload bar
    ax = plt.subplot(2, 2, 1)
    labs, vals = [], []
    for t, pts in series.items():
        for s, v in pts:
            if s == 0:
                labs.append(t); vals.append(v); break
    if vals:
        ax.bar(labs, vals)
        for i, v in enumerate(vals):
            ax.text(i, v * 1.03, f"{int(round(v))}µs", ha="center")
    ax.set_title("0-Payload Message Latency"); ax.set_ylabel("Latency (µs)")

    # TR: log-log latency vs size
    ax = plt.subplot(2, 2, 2)
    for t, pts in series.items():
        xs = [max(1, s) for s, _ in pts]
        ys = [max(1e-3, v) for _, v in pts]
        ax.plot(xs, ys, marker="o", label=t)
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_xlabel("Message Size (bytes)"); ax.set_ylabel("Latency (µs)")
    ax.set_title("Latency vs Message Size (Log-Log)"); ax.legend()

    # BL: small (≤4 KiB) linear
    ax = plt.subplot(2, 2, 3)
    for t, pts in series.items():
        xs = [s for s, _ in pts if s <= 4096]
        ys = [v for s, v in pts if s <= 4096]
        if xs:
            ax.plot(xs, ys, marker="o", label=t)
    ax.set_xlabel("Message Size (bytes)"); ax.set_ylabel("Latency (µs)")
    ax.set_title("Small Message Performance"); ax.legend()

    # BR: large (≥2 MiB) bars (ms)
    ax = plt.subplot(2, 2, 4)
    sizes = sorted({s for pts in series.values() for s, _ in pts if s >= 2*1024*1024})
    for idx, s in enumerate(sizes):
        vals = []
        for t in series:
            for s2, v in series[t]:
                if s2 == s:
                    vals.append(v/1000.0)
        if vals:
            ax.bar([idx + i*0.2 for i in range(len(vals))], vals, width=0.18)
    ax.set_xticks(range(len(sizes)))
    ax.set_xticklabels([f"{s//(1024*1024)}MB" for s in sizes])
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Large Message Performance")

    plt.tight_layout(rect=[0,0.03,1,0.96])
    plt.savefig(out_png, dpi=200)
    print(f"[plot] wrote {out_png}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min_time", default="1s", help="google-benchmark min time per run")
    ap.add_argument("--out_csv", default="stream_bench_results.csv")
    ap.add_argument("--out_png", default="streaming_transport_performance.png")
    ap.add_argument("--filter", default=None,
                    help="optional benchmark_filter regex (defaults to BM_StreamingPingPong.*)")
    args = ap.parse_args()

    ensure_built()
    run_bench(args.min_time, args.out_csv, args.filter)
    rows = parse_csv(args.out_csv)
    if not rows:
        print("no rows parsed; try relaxing --filter or check binary output")
        sys.exit(2)
    plot(rows, args.out_png)

if __name__ == "__main__":
    main()
