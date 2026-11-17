#!/usr/bin/env python3
import glob
import json
import math
import os
import re
import sys

def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)

def main():
    if len(sys.argv) < 3:
        print("usage: parse_pgbench.py <pgbench_out_file> <log_prefix>")
        sys.exit(1)

    out_file = sys.argv[1]
    log_prefix = sys.argv[2]
    text = open(out_file).read()

    tps_m = re.search(r"tps = ([0-9.]+)", text)
    lat_m = re.search(r"latency average = ([0-9.]+) ms", text)
    proc_m = re.search(r"number of transactions actually processed: (\d+)", text)
    fail_m = re.search(r"number of failed transactions: (\d+)", text)

    lat_us = []
    for path in sorted(glob.glob(f"{log_prefix}*")):
        with open(path) as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 3:
                    try:
                        lat_us.append(int(parts[2]))
                    except ValueError:
                        pass
        os.remove(path)

    lat_us.sort()
    res = {
        "tps": float(tps_m.group(1)) if tps_m else 0.0,
        "avg_lat_ms": float(lat_m.group(1)) if lat_m else 0.0,
        "tx_processed": int(proc_m.group(1)) if proc_m else 0,
        "tx_failed": int(fail_m.group(1)) if fail_m else 0,
        "p50_ms": round(percentile(lat_us, 50) / 1000.0, 3),
        "p95_ms": round(percentile(lat_us, 95) / 1000.0, 3),
        "p99_ms": round(percentile(lat_us, 99) / 1000.0, 3),
        "samples": len(lat_us),
    }
    print(json.dumps(res))

if __name__ == "__main__":
    main()
