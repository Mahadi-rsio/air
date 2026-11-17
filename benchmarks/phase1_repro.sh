#!/usr/bin/env bash
set -euo pipefail

API_URL="http://127.0.0.1:8080"
TOKEN="air_bench_token_local_only"
OUT_DIR="/workspaces/air/benchmarks/results/phase1"
mkdir -p "$OUT_DIR"

echo "=== Phase 1: Reproducibility Baseline (3 Trials) ==="
echo "Git SHA: $(git -C /workspaces/air rev-parse HEAD)"
echo "Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

RESULTS_JSON="$OUT_DIR/repro_trials.jsonl"
: > "$RESULTS_JSON"

for i in 1 2 3; do
  DB="repro_db_${i}"
  echo "--- Trial $i: $DB ---"

  # 1. Provision
  T0=$(date +%s%N)
  PROV_RESP=$(curl -sf -X POST "$API_URL/v1/databases" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: repro-${i}-$(date +%s%N)" \
    -d "{\"tenant_id\":\"$DB\"}")
  T1=$(date +%s%N)
  PROV_MS=$(awk "BEGIN {printf \"%.2f\", ($T1 - $T0)/1000000}")

  PORT=$(echo "$PROV_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['direct_port'])")
  ROUTE_RESP=$(curl -sf "$API_URL/internal/v1/tenants/$DB/route")
  PASS=$(echo "$ROUTE_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['app_password'])")
  export PGPASSWORD="$PASS"

  echo "  Provisioned $DB on port $PORT in ${PROV_MS} ms"

  # 2. Initialize pgbench (scale 5 = 500k rows)
  pgbench -h 127.0.0.1 -p "$PORT" -U app_user -i -s 5 "$DB" > "$OUT_DIR/${DB}_init.log" 2>&1

  # 3. Warm-up (10s)
  pgbench -h 127.0.0.1 -p "$PORT" -U app_user -c 8 -j 2 -T 10 -S "$DB" > /dev/null 2>&1

  # 4. Read-Only (30s)
  RO_OUT=$(pgbench -h 127.0.0.1 -p "$PORT" -U app_user -c 8 -j 2 -T 30 -S "$DB" 2>&1)
  echo "$RO_OUT" > "$OUT_DIR/${DB}_ro.log"
  RO_TPS=$(echo "$RO_OUT" | awk '/tps =/{print $3}' | head -n1)
  RO_LAT=$(echo "$RO_OUT" | awk '/latency average =/{print $4}' | head -n1)

  # 5. Read-Write (30s)
  RW_OUT=$(pgbench -h 127.0.0.1 -p "$PORT" -U app_user -c 8 -j 2 -T 30 "$DB" 2>&1)
  echo "$RW_OUT" > "$OUT_DIR/${DB}_rw.log"
  RW_TPS=$(echo "$RW_OUT" | awk '/tps =/{print $3}' | head -n1)
  RW_LAT=$(echo "$RW_OUT" | awk '/latency average =/{print $4}' | head -n1)

  # 6. Suspend
  T_SUS0=$(date +%s%N)
  curl -sf -X POST "$API_URL/internal/v1/databases/$DB/suspend" > /dev/null
  T_SUS1=$(date +%s%N)
  SUS_MS=$(awk "BEGIN {printf \"%.2f\", ($T_SUS1 - $T_SUS0)/1000000}")

  # 7. Wake
  T_WAKE0=$(date +%s%N)
  WAKE_RESP=$(curl -sf -X POST "$API_URL/internal/v1/databases/$DB/start")
  T_WAKE1=$(date +%s%N)
  WAKE_MS=$(awk "BEGIN {printf \"%.2f\", ($T_WAKE1 - $T_WAKE0)/1000000}")

  echo "  RO: ${RO_TPS} TPS (${RO_LAT} ms) | RW: ${RW_TPS} TPS (${RW_LAT} ms) | Suspend: ${SUS_MS} ms | Wake: ${WAKE_MS} ms"

  python3 -c "import json; print(json.dumps({
    'trial': $i, 'tenant_id': '$DB',
    'provision_ms': float('$PROV_MS'),
    'ro_tps': float('$RO_TPS'), 'ro_lat_ms': float('$RO_LAT'),
    'rw_tps': float('$RW_TPS'), 'rw_lat_ms': float('$RW_LAT'),
    'suspend_ms': float('$SUS_MS'), 'wake_ms': float('$WAKE_MS')
  }))" >> "$RESULTS_JSON"

  # Cleanup tenant after trial so warm pool/memory stay clean
  curl -sf -X DELETE "$API_URL/v1/databases/$DB" -H "Authorization: Bearer $TOKEN" > /dev/null
  sleep 2
done

python3 - <<'PY'
import json, statistics

rows = [json.loads(line) for line in open("/workspaces/air/benchmarks/results/phase1/repro_trials.jsonl")]
metrics = ["provision_ms", "ro_tps", "ro_lat_ms", "rw_tps", "rw_lat_ms", "suspend_ms", "wake_ms"]
print("\n=== Summary (3 trials) ===")
print(f"{'Metric':<16} {'Min':>10} {'Median':>10} {'Max':>10} {'Var':>12} {'StdDev':>10}")
summary = {}
for m in metrics:
    vals = [r[m] for r in rows]
    mn, md, mx = min(vals), statistics.median(vals), max(vals)
    var = statistics.variance(vals) if len(vals) > 1 else 0.0
    sd = statistics.stdev(vals) if len(vals) > 1 else 0.0
    summary[m] = {"min": mn, "median": md, "max": mx, "variance": var, "stddev": sd}
    print(f"{m:<16} {mn:10.2f} {md:10.2f} {mx:10.2f} {var:12.2f} {sd:10.2f}")

with open("/workspaces/air/benchmarks/results/phase1/repro_summary.json", "w") as f:
    json.dump({"trials": rows, "summary": summary}, f, indent=2)
PY
