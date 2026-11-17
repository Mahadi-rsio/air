#!/usr/bin/env bash
set -euo pipefail

API_URL="http://127.0.0.1:8080"
TOKEN="air_bench_token_local_only"
OUT_DIR="/workspaces/air/benchmarks/results/phase2_cd"
mkdir -p "$OUT_DIR"

echo "=== Phase 2C & 2D: Custom Workloads (100K, 1M, 10M) & Cold vs Warm ==="

setup_db() {
  local db="$1"
  local rows="$2"
  curl -sf -X DELETE "$API_URL/v1/databases/$db" -H "Authorization: Bearer $TOKEN" >/dev/null 2>&1 || true
  local prov port pass
  prov=$(curl -sf -X POST "$API_URL/v1/databases" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: ${db}-$(date +%s%N)" \
    -d "{\"tenant_id\":\"$db\"}")
  port=$(echo "$prov" | python3 -c "import sys,json; print(json.load(sys.stdin)['direct_port'])")
  pass=$(curl -sf "$API_URL/internal/v1/tenants/$db/route" | python3 -c "import sys,json; print(json.load(sys.stdin)['app_password'])")
  echo "$port|$pass"
}

run_custom_suite() {
  local db="$1"
  local rows="$2"
  local port="$3"
  local pass="$4"
  export PGPASSWORD="$pass"
  local tag="${db}_${rows}"

  echo "--- Setting up $db ($rows rows) on port $port ---"
  psql -h 127.0.0.1 -p "$port" -U app_user -d "$db" -v ON_ERROR_STOP=1 <<SQL > "$OUT_DIR/${tag}_load.log"
CREATE TABLE IF NOT EXISTS items (
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
FROM generate_series(1, $rows) AS g;
CREATE INDEX IF NOT EXISTS idx_items_cat_val ON items(category, val);
CREATE INDEX IF NOT EXISTS idx_items_status ON items(status);
VACUUM ANALYZE items;
SELECT pg_size_pretty(pg_total_relation_size('items')) AS total_rel_size,
       pg_total_relation_size('items') AS total_rel_bytes;
SQL

  # 1-5: Read queries + EXPLAIN (ANALYZE, BUFFERS)
  psql -h 127.0.0.1 -p "$port" -U app_user -d "$db" -v ON_ERROR_STOP=1 <<SQL > "$OUT_DIR/${tag}_queries.log"
\timing on
-- Q1: Point lookup by PK
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM items WHERE id = $((rows / 2));
-- Q2: Indexed range scan
EXPLAIN (ANALYZE, BUFFERS) SELECT count(*), sum(val) FROM items WHERE id BETWEEN $((rows / 4)) AND $((rows / 4 + 5000));
-- Q3: Secondary index lookup
EXPLAIN (ANALYZE, BUFFERS) SELECT id, val FROM items WHERE category = 42 AND val > 50000 LIMIT 100;
-- Q4: Aggregation & GROUP BY
EXPLAIN (ANALYZE, BUFFERS) SELECT status, count(*), avg(val)::numeric(10,2), max(val) FROM items GROUP BY status;
-- Q5: Full table scan
EXPLAIN (ANALYZE, BUFFERS) SELECT count(*) FROM items WHERE payload LIKE '%abc%';
-- Q8: Large transaction (10k row insert + delete)
BEGIN;
INSERT INTO items (id, category, status, val, payload)
SELECT $rows + g, g % 100, 'batch', g, repeat('y', 64) FROM generate_series(1, 10000) AS g;
DELETE FROM items WHERE id > $rows;
COMMIT;
SQL

  # Q6 & Q7: Concurrent mixed R/W and INSERT/UPDATE/DELETE via pgbench custom scripts
  cat > /tmp/p2c_mixed.sql <<SQL
\set id random(1, $rows)
\set cat random(0, 999)
SELECT id, val FROM items WHERE id = :id;
SELECT count(*) FROM items WHERE category = :cat AND val > 50000;
UPDATE items SET val = val + 1 WHERE id = :id;
SQL

  cat > /tmp/p2c_iud.sql <<SQL
\set id random(1, $rows)
\set newid random(20000000, 99999999)
INSERT INTO items (id, category, status, val, payload) VALUES (:newid, 1, 'tmp', 123, 'tmp') ON CONFLICT (id) DO NOTHING;
UPDATE items SET val = val + 1 WHERE id = :id;
DELETE FROM items WHERE id = :newid;
SQL

  pgbench -h 127.0.0.1 -p "$port" -U app_user -c 8 -j 2 -T 15 -n -f /tmp/p2c_mixed.sql "$db" > "$OUT_DIR/${tag}_mixed.log" 2>&1
  pgbench -h 127.0.0.1 -p "$port" -U app_user -c 8 -j 2 -T 15 -n -f /tmp/p2c_iud.sql "$db" > "$OUT_DIR/${tag}_iud.log" 2>&1
  echo "  Completed custom suite for $rows rows."
}

# Run 100K, 1M, 10M
for rows in 100000 1000000 10000000; do
  db="custom_${rows}"
  info=$(setup_db "$db" "$rows")
  port="${info%%|*}"
  pass="${info##*|}"
  run_custom_suite "$db" "$rows" "$port" "$pass"
  if [ "$rows" != "1000000" ]; then
    curl -sf -X DELETE "$API_URL/v1/databases/$db" -H "Authorization: Bearer $TOKEN" >/dev/null || true
  fi
done

echo "=== Phase 2D: Cold vs Warm vs Suspend/Wake vs Cache Eviction vs Container Restart (1M rows) ==="
DB2D="custom_1000000"
ROUTE=$(curl -sf "$API_URL/internal/v1/tenants/$DB2D/route")
PORT2D=$(echo "$ROUTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
PASS2D=$(echo "$ROUTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['app_password'])")
export PGPASSWORD="$PASS2D"

measure_qset() {
  local label="$1"
  local port="$2"
  psql -h 127.0.0.1 -p "$port" -U app_user -d "$DB2D" -v ON_ERROR_STOP=1 <<SQL > "$OUT_DIR/p2d_${label}.log"
\timing on
SELECT * FROM items WHERE id = 500000;
SELECT count(*), sum(val) FROM items WHERE id BETWEEN 250000 AND 255000;
SELECT id, val FROM items WHERE category = 42 AND val > 50000 LIMIT 100;
SELECT status, count(*), avg(val)::numeric(10,2) FROM items GROUP BY status;
SELECT count(*) FROM items WHERE payload LIKE '%abc%';
SQL
}

# 1. Warm execution
measure_qset "warm" "$PORT2D"

# 2. Suspend and Wake (local pgdata kept)
curl -sf -X POST "$API_URL/internal/v1/databases/$DB2D/suspend" > /dev/null
sleep 2
WAKE1=$(curl -sf -X POST "$API_URL/internal/v1/databases/$DB2D/start")
PORT2D=$(echo "$WAKE1" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
measure_qset "after_suspend_wake" "$PORT2D"

# 3. Local cache eviction (suspend, remove LFC + ps_cache, wake)
curl -sf -X POST "$API_URL/internal/v1/databases/$DB2D/suspend" > /dev/null
sleep 3
docker exec air-bench rm -rf /var/lib/air/lfc/"$DB2D" /var/lib/air/ps_cache/*
WAKE2=$(curl -sf -X POST "$API_URL/internal/v1/databases/$DB2D/start")
PORT2D=$(echo "$WAKE2" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
measure_qset "after_local_cache_eviction" "$PORT2D"

# 4. Full container restart (stop container, remove disposable ps_cache & lfc, restart container, wake)
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

# Cleanup
curl -sf -X DELETE "$API_URL/v1/databases/$DB2D" -H "Authorization: Bearer $TOKEN" >/dev/null || true
echo "Done Phase 2C & 2D!"
