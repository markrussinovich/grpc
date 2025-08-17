#!/usr/bin/env python3
import argparse
import csv
import re
from pathlib import Path

import matplotlib.pyplot as plt

# parsing utilities
def parse_csv_rows(csv_path):
    lines = csv_path.read_text(errors="ignore").splitlines()
    # skip preamble until header line
    for i, line in enumerate(lines):
        if line.strip().startswith("name,"):
            header = lines[i]
            data_lines = lines[i:]
            break
    else:
        raise ValueError("CSV header not found.")

    reader = csv.DictReader(data_lines)
    rows = []
    for r in reader:
        name = r['name'].strip('"')
        rt = float(r.get('real_time', 'nan'))
        unit = r.get('time_unit', 'ns').lower()
        if unit == 'ns':
            rt_us = rt / 1000.0
        elif unit == 'us':
            rt_us = rt
        elif unit == 'ms':
            rt_us = rt * 1000.0
        else:
            rt_us = rt
        # extract transport and size
        m = re.search(r"<([^,>]+)", name)
        transport = m.group(1) if m else None
        m2 = re.search(r"/(\d+)", name)
        size = int(m2.group(1)) if m2 else None
        if transport and size is not None:
            rows.append((transport, size, rt_us))
    return rows

def plot_streaming(rows, out_png):
    from collections import defaultdict
    series = defaultdict(list)
    for transport, size, rt_us in rows:
        series[transport].append((size, rt_us))
    for t in series:
        series[t] = sorted(series[t], key=lambda x: x[0])

    plt.figure(figsize=(14,10))
    plt.suptitle("StreamingPingPong Latency by Transport", fontsize=16, fontweight="bold")

    # Top-left: 0-size payload bar
    ax = plt.subplot(2,2,1)
    tl = {t:v for t,data in series.items() for s,v in data if s==0}
    if tl:
        ax.bar(tl.keys(), tl.values())
        for i,(k,v) in enumerate(tl.items()):
            ax.text(i, v*1.03, f"{v:.1f} µs", ha="center")
    ax.set_title("0-payload Latency"); ax.set_ylabel("µs")

    # Top-right: log-log
    ax = plt.subplot(2,2,2)
    for t,data in series.items():
        xs = [max(1,s) for s,_ in data]
        ys = [max(1e-3,rt) for _,rt in data]
        ax.plot(xs, ys, marker='o', label=t)
    ax.set_xscale('log'); ax.set_yscale('log')
    ax.set_xlabel("Message Size (bytes)"); ax.set_ylabel("Latency (µs)")
    ax.set_title("Latency vs Payload Size (log-log)")
    ax.legend()

    # Bottom-left: small sizes (<=4KB)
    ax = plt.subplot(2,2,3)
    for t,data in series.items():
        xs=[s for s,rt in data if s<=4096]
        ys=[rt for s,rt in data if s<=4096]
        if xs:
            ax.plot(xs, ys, marker='o', label=t)
    ax.set_title("Small-message Latency"); ax.set_xlabel("Bytes"); ax.set_ylabel("µs")
    ax.legend()

    # Bottom-right: large sizes (>=2MB)
    ax = plt.subplot(2,2,4)
    large = sorted({s for data in series.values() for s,_ in data if s >= 2*1024*1024})
    width = 0.18
    for idx,s in enumerate(large):
        vals=[]
        labs=[]
        for t,data in series.items():
            for sz,rt in data:
                if sz==s:
                    vals.append(rt/1000.0)
                    labs.append(t)
        if vals:
            offsets = [idx - (len(vals)-1)*width/2 + i*width for i in range(len(vals))]
            ax.bar(offsets, vals, width=width)
    ax.set_xticks(range(len(large)))
    ax.set_xticklabels([f"{s//(1024*1024)}MB" for s in large])
    ax.set_title("Large-message Latency (ms)")
    ax.set_ylabel("ms")

    plt.tight_layout(rect=[0,0.03,1,0.95])
    plt.savefig(out_png, dpi=200)
    print(f"Plot saved to {out_png}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--out", default="streaming_plot.png")
    args = parser.parse_args()

    rows = parse_csv_rows(Path(args.csv))
    plot_streaming(rows, Path(args.out))

if __name__ == "__main__":
    main()
