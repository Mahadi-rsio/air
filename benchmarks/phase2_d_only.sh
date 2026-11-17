#!/usr/bin/env bash
set -euo pipefail

API_URL="http://127.0.0.1:8080"
TOKEN="air_bench_token_local_only"
OUT_DIR="/workspaces/air/benchmarks/results/phase2_cd"
mkdir -p "$OUT_DIR"

DB2D="p2d_1m"
curl -sf -X DELETE "$API_URL/v1/databases/$DB2D" -H "Authorization: Bearer $TOKEN" >/dev/null 2>&1 || true

PROV=$(curl -sf -X POST "$API_URL/v1/databases" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: ${DB2D}-$(date +%s%N)" \
  -d "{\"tenant_id\":\"$DB2D\"}")
PORT2D=$(echo "$PROV" | python3 -c "import sys,json; print(json.load(sys.stdin)['direct_port'])")
PASS2D=$(curl -sf "$API_URL/internal/v1/tenants/$DB2D/route" | python3 -c "import sys,json; print(json.load(sys.stdin)['app_password'])")
export PGPASSWORD="$PASS2D"

echo "Loading 1M rows into $DB2D on port $PORT2D..."
psql -h 127.0.0.1 -p "$PORT2D" -U app_user -d "$DB2D" -v ON_ERROR_STOP=1 <<SQL > "$OUT_DIR/p2d_load.log"
CREATE TABLE items (
  id BIGINT PRIMARY KEY,
  category INT NOT NULL,
  status TEXT NOT NULL,
  val BIGINT NOT NULL,
  payload TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO items (id, category, status, val, payload)
SELECT g, (g % 1000), CASE WHEN g % 5 = 0 THEN 'closed' ELSE 'open' END, (g * 17) % 100000,
       md5(g::text) || '-' || repeat('x', 64)
FROM generate_series(1, 1000000) AS g;
CREATE INDEX idx_items_cat_val ON items(category, val);
CREATE INDEX idx_items_status ON items(status);
VACUUM ANALYZE items;
SQL

measure_qset() {
  local label="$1"
  local port="$2"
  echo "--- Measuring $label (port=$port) ---"
  psql -h 127.0.0.1 -p "$port" -U app_user -d "$DB2D" -v ON_ERROR_STOP=1 <<SQL > "$OUT_DIR/p2d_${label}.log"
\timing on
SELECT * FROM items WHERE id = 500000;
SELECT count(*), sum(val) FROM items WHERE id BETWEEN 250000 AND 255000;
SELECT id, val FROM items WHERE category = 42 AND val > 50000 LIMIT 100;
SELECT status, count(*), avg(val)::numeric(10,2) FROM items GROUP BY status;
SELECT count(*) FROM items WHERE payload LIKE '%abc%';
SQL
}

# 1. Suspend cleanly first so data is persisted to R2/layers, then wake for clean startup first-exec
curl -sf -X POST "$API_URL/internal/v1/databases/$DB2D/suspend" > /dev/null
sleep 5
# Wait for any cold offload to settle
for i in $(seq 1 30); do
  ROUTE=$(curl -sf "$API_URL/internal/v1/tenants/$DB2D/route")
  STATE=$(echo "$ROUTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['state'])")
  if [ "$STATE" = "SUSPENDED" ]; then break; fi
  sleep 1
done
WAKE0=$(curl -sf -X POST "$API_URL/internal/v1/databases/$DB2D/start")
PORT2D=$(echo "$WAKE0" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
measure_qset "first_after_clean_startup" "$PORT2D"

# 2. Repeated warm-cache execution
measure_qset "warm" "$PORT2D"

# 3. Execution after database suspend and wake
curl -sf -X POST "$API_URL/internal/v1/databases/$DB2D/suspend" > /dev/null
sleep 3
WAKE1=$(curl -sf -X POST "$API_URL/internal/v1/databases/$DB2D/start")
PORT2D=$(echo "$WAKE1" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
measure_qset "after_suspend_wake" "$PORT2D"

# 4. Execution after local cache eviction
curl -sf -X POST "$API_URL/internal/v1/databases/$DB2D/suspend" > /dev/null
sleep 3
docker exec air-bench rm -rf /var/lib/air/lfc/"$DB2D" /var/lib/air/ps_cache/*
WAKE2=$(curl -sf -X POST "$API_URL/internal/v1/databases/$DB2D/start")
PORT2D=$(echo "$WAKE2" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
measure_qset "after_local_cache_eviction" "$PORT2D"

# 5. Execution after full container restart
curl -sf -X POST "$API_URL/internal/v1/databases/$DB2D/suspend" > /dev/null
sleep 5
docker restart air-bench >/dev/null
for i in $(seq 1 30); do
  if curl -sf http://127.0.0.1:8080/healthz >/dev/null; then break; fi
  sleep 1
done
WAKE3=$(curl -sf -X POST "$API_URL/internal/v1/databases/$DB2D/start")
PORT2D=$(echo "$WAKE3" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
measure_qset "after_container_restart" "$PORT2D"

curl -sf -X DELETE "$API_URL/v1/databases/$DB2D" -H "Authorization: Bearer $TOKEN" >/dev/null || true
echo "Done Phase 2D!"
