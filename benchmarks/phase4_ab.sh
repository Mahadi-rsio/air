#!/usr/bin/env bash
set -euo pipefail

API_URL="http://127.0.0.1:8080"
PROXY_ADMIN="http://127.0.0.1:19001"
TOKEN="air_bench_token_local_only"
OUT_DIR="/workspaces/air/benchmarks/results/phase4"
mkdir -p "$OUT_DIR"

set -a; source /workspaces/air/.env; set +a

curl -sf -X POST "$PROXY_ADMIN/reset" >/dev/null
PROV=$(curl -sf -X POST "$API_URL/v1/databases" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: p4-$(date +%s%N)" \
  -d '{"tenant_id":"p4_db"}')
PORT=$(echo "$PROV" | python3 -c "import sys,json; print(json.load(sys.stdin)['direct_port'])")
PASS=$(curl -sf "$API_URL/internal/v1/tenants/p4_db/route" | python3 -c "import sys,json; print(json.load(sys.stdin)['app_password'])")
export PGPASSWORD="$PASS"

curl -sf "$PROXY_ADMIN/stats" > "$OUT_DIR/r2_after_provision.json"

pgbench -h 127.0.0.1 -p "$PORT" -U app_user -i -s 5 p4_db > "$OUT_DIR/pgbench_init.log" 2>&1
pgbench -h 127.0.0.1 -p "$PORT" -U app_user -c 8 -j 2 -T 15 p4_db > "$OUT_DIR/pgbench_rw.log" 2>&1

# Record logical sizes & raw WAL size before suspend
psql -h 127.0.0.1 -p "$PORT" -U app_user -d p4_db -c "SELECT pg_database_size('p4_db') AS db_bytes, pg_total_relation_size('pgbench_accounts') AS acct_bytes;" > "$OUT_DIR/pg_sizes.txt"
docker exec air-bench du -sb /var/lib/air/tenants/active/p4_db/pgdata/pg_wal > "$OUT_DIR/wal_raw_bytes.txt"

curl -sf "$PROXY_ADMIN/stats" > "$OUT_DIR/r2_after_active_rw.json"
curl -sf -X POST "$PROXY_ADMIN/reset" >/dev/null

# Suspend and wait for cold offload
curl -sf -X POST "$API_URL/internal/v1/databases/p4_db/suspend" >/dev/null
for i in $(seq 1 60); do
  ST=$(curl -sf "$API_URL/internal/v1/tenants/p4_db/route" | python3 -c "import sys,json; print(json.load(sys.stdin)['state'])")
  if [ "$ST" = "SUSPENDED" ]; then break; fi
  sleep 1
done
sleep 5

curl -sf "$PROXY_ADMIN/stats" > "$OUT_DIR/r2_during_suspend.json"
go run ./benchmarks/r2tool -bucket air-pg-backups -endpoint "$ENDPOINT" -access-key "$ACCESS_KEY" -secret-key "$SECRET_KEY" -prefix "tenants/p4_db/" -json > "$OUT_DIR/r2_objects.json"

# Evict local cache & wake to measure R2 read requests
curl -sf -X POST "$PROXY_ADMIN/reset" >/dev/null
docker exec air-bench rm -rf /var/lib/air/lfc/p4_db /var/lib/air/ps_cache/*
WAKE=$(curl -sf -X POST "$API_URL/internal/v1/databases/p4_db/start")
PORT=$(echo "$WAKE" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
psql -h 127.0.0.1 -p "$PORT" -U app_user -d p4_db -c "SELECT count(*), sum(abalance) FROM pgbench_accounts;" > "$OUT_DIR/cold_read_check.txt"
curl -sf "$PROXY_ADMIN/stats" > "$OUT_DIR/r2_during_cold_wake_read.json"

echo "Phase 4A & 4B complete! Leaving p4_db active for Phase 4C."
