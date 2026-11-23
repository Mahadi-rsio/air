#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BENCH_DIR="$ROOT_DIR/benchmarks/phase20.9"
SUITE="${1:-all}"
DURATION="${2:-8}"
SUSTAINED_SEC="${3:-60}"

"$BENCH_DIR/bin/phase209-bench" \
  --suite "$SUITE" \
  --duration "$DURATION" \
  --sustained-sec "$SUSTAINED_SEC" \
  --gateway-bin "$BENCH_DIR/bin/air-gateway" \
  --out-dir "$BENCH_DIR/results" \
  --work-dir "/tmp/air-phase209-work"
