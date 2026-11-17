#!/usr/bin/env bash
set -euo pipefail

API_URL="http://127.0.0.1:8080"
TOKEN="air_bench_token_local_only"
OUT_DIR="/workspaces/air/benchmarks/results/phase2"
mkdir -p "$OUT_DIR"

DB="perf_p2_db"
echo "=== Phase 2A & 2B: pgbench RO (1,8,16,32,64) & RW (1,8,16,32) ==="

# Clean any leftover tenant
curl -sf -X DELETE "$API_URL/v1/databases/$DB" -H "Authorization: Bearer $TOKEN" >/dev/null 2>&1 || true

PROV=$(curl -sf -X POST "$API_URL/v1/databases" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: p2-$(date +%s%N)" \
  -d "{\"tenant_id\":\"$DB\"}")
PORT=$(echo "$PROV" | python3 -c "import sys,json; print(json.load(sys.stdin)['direct_port'])")
PASS=$(curl -sf "$API_URL/internal/v1/tenants/$DB/route" | python3 -c "import sys,json; print(json.load(sys.stdin)['app_password'])")
export PGPASSWORD="$PASS"

echo "Provisioned $DB on port $PORT"

# Scale 10 = 1,000,000 rows in pgbench_accounts (~150 MB logical DB)
pgbench -h 127.0.0.1 -p "$PORT" -U app_user -i -s 10 "$DB" > "$OUT_DIR/init_s10.log" 2>&1
psql -h 127.0.0.1 -p "$PORT" -U app_user -d "$DB" -c "SELECT pg_size_pretty(pg_database_size('$DB')) AS db_size;"

# Warm-up 15s
pgbench -h 127.0.0.1 -p "$PORT" -U app_user -c 8 -j 2 -T 15 -S "$DB" > /dev/null 2>&1

RESULTS="$OUT_DIR/pgbench_results.jsonl"
: > "$RESULTS"

run_case() {
  local mode="$1"
  local clients="$2"
  local threads="$3"
  local dur="$4"
  local extra_flag="$5"
  local tag="${mode}_c${clients}"
  local out_file="$OUT_DIR/${tag}.out"
  local log_prefix="/tmp/pgb_${tag}_log"
  rm -f "${log_prefix}"*

  # Reset pg_stat_database counters if possible, or take before/after snapshot
  local before_stat after_stat
  before_stat=$(psql -h 127.0.0.1 -p "$PORT" -U app_user -d "$DB" -t -A -c "SELECT blks_hit, blks_read FROM pg_stat_database WHERE datname='$DB';")

  # Start background sampler for docker stats & pg_stat_activity wait_events
  local samp_file="$OUT_DIR/${tag}_samples.txt"
  : > "$samp_file"
  (
    while true; do
      ds=$(docker stats --no-stream --format "{{.CPUPerc}}|{{.MemUsage}}" air-bench 2>/dev/null || echo "0%|0B / 0B")
      waits=$(psql -h 127.0.0.1 -p "$PORT" -U app_user -d "$DB" -t -A -c "SELECT coalesce(wait_event_type||':'||wait_event,'CPU')||':'||count(*) FROM pg_stat_activity WHERE datname='$DB' AND state='active' GROUP BY 1;" 2>/dev/null | tr '\n' ',' || true)
      echo "$(date +%s) $ds waits=[$waits]" >> "$samp_file"
      sleep 2
    done
  ) &
  local SAMP_PID=$!

  pgbench -h 127.0.0.1 -p "$PORT" -U app_user -c "$clients" -j "$threads" -T "$dur" $extra_flag -l --log-prefix="$log_prefix" "$DB" > "$out_file" 2>&1 || true

  kill "$SAMP_PID" 2>/dev/null || true
  wait "$SAMP_PID" 2>/dev/null || true

  after_stat=$(psql -h 127.0.0.1 -p "$PORT" -U app_user -d "$DB" -t -A -c "SELECT blks_hit, blks_read FROM pg_stat_database WHERE datname='$DB';")

  local parsed
  parsed=$(python3 /workspaces/air/benchmarks/parse_pgbench.py "$out_file" "$log_prefix")

  python3 - <<PY >> "$RESULTS"
import json, re
d = json.loads('''$parsed''')
d["mode"] = "$mode"
d["clients"] = $clients
d["duration_s"] = $dur
bh0, br0 = [int(x) for x in "$before_stat".split("|")]
bh1, br1 = [int(x) for x in "$after_stat".split("|")]
dh, dr = bh1 - bh0, br1 - br0
d["blks_hit"] = dh
d["blks_read"] = dr
d["pg_cache_hit_ratio"] = round((dh / (dh + dr)) * 100.0, 4) if (dh + dr) > 0 else 100.0

cpus, mems = [], []
for line in open("$samp_file"):
    m = re.search(r"([0-9.]+)%\|([0-9.]+)([KMG]i?B)", line)
    if m:
        cpus.append(float(m.group(1)))
        val = float(m.group(2))
        unit = m.group(3)
        if unit.startswith("G"): val *= 1024
        elif unit.startswith("K"): val /= 1024
        mems.append(val)
d["avg_cpu_pct"] = round(sum(cpus)/len(cpus), 2) if cpus else 0.0
d["max_cpu_pct"] = round(max(cpus), 2) if cpus else 0.0
d["max_ram_mib"] = round(max(mems), 2) if mems else 0.0
print(json.dumps(d))
print(f"  [{d['mode']} c={d['clients']}] TPS={d['tps']:.2f} avg={d['avg_lat_ms']:.3f}ms p50={d['p50_ms']}ms p95={d['p95_ms']}ms p99={d['p99_ms']}ms fail={d['tx_failed']} hit={d['pg_cache_hit_ratio']}% cpu={d['avg_cpu_pct']}% ram={d['max_ram_mib']}MiB")
PY
}

# Phase 2A: Read-Only
for c in 1 8 16 32 64; do
  t=2
  if [ "$c" -eq 1 ]; then t=1; fi
  run_case "RO" "$c" "$t" 60 "-S"
done

# Phase 2B: Read-Write (TPC-B)
for c in 1 8 16 32; do
  t=2
  if [ "$c" -eq 1 ]; then t=1; fi
  run_case "RW" "$c" "$t" 60 ""
done

echo "Done Phase 2A & 2B. Results in $RESULTS"
