#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BENCH_DIR="$ROOT_DIR/benchmarks/phase20.9"
CGROUP_ROOT="/sys/fs/cgroup/air_phase209"

echo "[setup] Preparing cgroup v2 delegation at $CGROUP_ROOT ..."
sudo mkdir -p "$CGROUP_ROOT"
echo "+memory +cpu +io +pids" | sudo tee /sys/fs/cgroup/cgroup.subtree_control >/dev/null 2>&1 || true
echo "+memory +cpu +io +pids" | sudo tee "$CGROUP_ROOT/cgroup.subtree_control" >/dev/null 2>&1 || true
sudo chown -R "$(id -u):$(id -g)" "$CGROUP_ROOT"

echo "[setup] Building air-gateway, air-server, and phase209-bench ..."
mkdir -p "$BENCH_DIR/bin" "$BENCH_DIR/results"
cd "$ROOT_DIR"
go build -o "$BENCH_DIR/bin/air-gateway" ./cmd/air-gateway
go build -o "$BENCH_DIR/bin/air-server" ./cmd/air-server
go build -o "$BENCH_DIR/bin/phase209-bench" ./benchmarks/phase20.9

echo "[setup] Verifying PostgreSQL 17 binaries ..."
/usr/lib/postgresql/17/bin/postgres --version
/usr/lib/postgresql/17/bin/pgbench --version
echo "[setup] Environment ready."
