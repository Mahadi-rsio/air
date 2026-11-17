#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="/workspaces/air/benchmarks/results/phase6"
mkdir -p "$OUT_DIR"
LOG="$OUT_DIR/faults_summary.log"
: > "$LOG"

set -a; source /workspaces/air/.env; set +a
API_URL="http://127.0.0.1:8080"
TOKEN="air_bench_token_local_only"
PROXY_ADMIN="http://127.0.0.1:19001"

log_res() {
  echo "$1" | tee -a "$LOG"
}

# Start r2proxy in background for fault tests 1-5, 8
/tmp/r2proxy -listen 127.0.0.1:19000 -admin 127.0.0.1:19001 -endpoint "$ENDPOINT" -access-key "$ACCESS_KEY" -secret-key "$SECRET_KEY" > "$OUT_DIR/r2proxy_p6.log" 2>&1 &
PROXY_PID=$!
trap 'kill $PROXY_PID 2>/dev/null || true' EXIT
sleep 1

restart_air_with_proxy() {
  docker rm -f air-bench >/dev/null 2>&1 || true
  docker run --rm -v /tmp/air-bench-data:/var/lib/air --entrypoint sh air-server:pageserver -c "rm -rf /var/lib/air/*"
  go run ./benchmarks/r2tool -bucket air-pg-backups -endpoint "$ENDPOINT" -access-key "$ACCESS_KEY" -secret-key "$SECRET_KEY" -clean >/dev/null
  docker run -d --name air-bench --network host \
    -v /tmp/air-server-head:/usr/local/bin/air-server:ro \
    -v /tmp/air-bench-data:/var/lib/air \
    -e AIR_DATA_DIR=/var/lib/air -e AIR_PG_BIN_DIR=/usr/lib/postgresql/17/bin \
    -e AIR_HTTP_ADDR=127.0.0.1:8080 -e AIR_GRPC_ADDR=127.0.0.1:9090 -e AIR_GATEWAY_ADDR=127.0.0.1:5432 \
    -e AIR_PAGESERVER_ENABLED=true -e AIR_PAGESERVER_CACHE_MB=256 \
    -e AIR_R2_PRIMARY=true -e AIR_R2_BUCKET=air-pg-backups \
    -e AIR_R2_ENDPOINT=http://127.0.0.1:19000 -e AIR_R2_ACCESS_KEY_ID="$ACCESS_KEY" -e AIR_R2_SECRET_ACCESS_KEY="$SECRET_KEY" \
    -e AIR_BOOTSTRAP_API_KEY=air_bench_token_local_only -e AIR_IDLE_TIMEOUT=0 -e AIR_POOL_MODE=true \
    air-server:pageserver >/dev/null
  for i in $(seq 1 30); do
    if curl -sf "$API_URL/healthz" >/dev/null; then break; fi
    sleep 1
  done
}

set_fault() {
  curl -sf -X POST "$PROXY_ADMIN/fault" -H "Content-Type: application/json" -d "$1" >/dev/null
}

restart_air_with_proxy

# Seed a baseline DB f6_db
PROV=$(curl -sf -X POST "$API_URL/v1/databases" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -H "Idempotency-Key: f6-init" -d '{"tenant_id":"f6_db"}')
PORT=$(echo "$PROV" | python3 -c "import sys,json; print(json.load(sys.stdin)['direct_port'])")
PASS=$(curl -sf "$API_URL/internal/v1/tenants/f6_db/route" | python3 -c "import sys,json; print(json.load(sys.stdin)['app_password'])")
export PGPASSWORD="$PASS"
psql -h 127.0.0.1 -p "$PORT" -U app_user -d f6_db -c "CREATE TABLE f6(id int primary key, v text); INSERT INTO f6 SELECT g, 'ok' FROM generate_series(1,100) g;" >/dev/null

# 1. R2 temporarily unavailable during provision
set_fault '{"mode":"unavailable"}'
R1=$(curl -s -w " HTTP:%{http_code}" -X POST "$API_URL/v1/databases" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -H "Idempotency-Key: f6-1" -d '{"tenant_id":"f6_unavail"}')
log_res "[F1 R2 Unavailable] $R1"
# Check existing active DB still serves reads/writes while R2 unavailable
Q1=$(psql -h 127.0.0.1 -p "$PORT" -U app_user -d f6_db -t -A -c "SELECT count(*) FROM f6;" 2>&1 || true)
log_res "[F1 Active DB Query During R2 Outage] $Q1"
set_fault '{"mode":""}'

# 2. R2 timeout (3s delay -> 504)
set_fault '{"mode":"timeout","timeout_ms":3000}'
R2=$(curl -s --max-time 25 -w " HTTP:%{http_code}" -X POST "$API_URL/v1/databases" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -H "Idempotency-Key: f6-2" -d '{"tenant_id":"f6_timeout"}')
log_res "[F2 R2 Timeout 3s] $R2"
set_fault '{"mode":""}'

# 3. R2 transient 5xx (50% error rate -> tests AWS SDK retries)
set_fault '{"mode":"5xx"}'
R3=$(curl -s -w " HTTP:%{http_code}" -X POST "$API_URL/v1/databases" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -H "Idempotency-Key: f6-3" -d '{"tenant_id":"f6_5xx"}')
log_res "[F3 R2 Transient 5xx] $R3"
set_fault '{"mode":""}'

# 4. Drop layer upload during suspend
set_fault '{"mode":"drop_layer_put"}'
psql -h 127.0.0.1 -p "$PORT" -U app_user -d f6_db -c "INSERT INTO f6 VALUES (101, 'before_drop_layer');" >/dev/null
S4=$(curl -s -w " HTTP:%{http_code}" -X POST "$API_URL/internal/v1/databases/f6_db/suspend")
sleep 2
set_fault '{"mode":""}'
W4=$(curl -s -w " HTTP:%{http_code}" -X POST "$API_URL/internal/v1/databases/f6_db/start")
PORT=$(curl -sf "$API_URL/internal/v1/tenants/f6_db/route" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
Q4=$(psql -h 127.0.0.1 -p "$PORT" -U app_user -d f6_db -t -A -c "SELECT count(*) FROM f6;" 2>&1 || true)
log_res "[F4 Drop Layer PUT] suspend=$S4 wake=$W4 count=$Q4"

# 5. Drop WAL upload during suspend
set_fault '{"mode":"drop_wal_put"}'
psql -h 127.0.0.1 -p "$PORT" -U app_user -d f6_db -c "INSERT INTO f6 VALUES (102, 'before_drop_wal');" >/dev/null
S5=$(curl -s -w " HTTP:%{http_code}" -X POST "$API_URL/internal/v1/databases/f6_db/suspend")
sleep 3
set_fault '{"mode":""}'
W5=$(curl -s -w " HTTP:%{http_code}" -X POST "$API_URL/internal/v1/databases/f6_db/start")
PORT=$(curl -sf "$API_URL/internal/v1/tenants/f6_db/route" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
Q5=$(psql -h 127.0.0.1 -p "$PORT" -U app_user -d f6_db -t -A -c "SELECT count(*) FROM f6;" 2>&1 || true)
log_res "[F5 Drop WAL PUT] suspend=$S5 wake=$W5 count=$Q5"

# 8. Corrupted layer object on R2 GET (with local ps_layers/ps_cache cleared)
curl -sf -X POST "$API_URL/internal/v1/databases/f6_db/suspend" >/dev/null
sleep 4
docker exec air-bench rm -rf /var/lib/air/ps_layers/f6_db /var/lib/air/ps_cache/* /var/lib/air/lfc/f6_db /var/lib/air/tenants/active/f6_db
set_fault '{"mode":"corrupt_layer_get"}'
W8=$(curl -s -w " HTTP:%{http_code}" -X POST "$API_URL/internal/v1/databases/f6_db/start")
log_res "[F8 Corrupted Layer GET] wake=$W8"
set_fault '{"mode":""}'

# 10. Insufficient local disk (4MB tmpfs container)
D10=$(docker run --rm --mount type=tmpfs,destination=/var/lib/air,tmpfs-size=4194304 \
  -v /tmp/air-server-head:/usr/local/bin/air-server:ro \
  -e AIR_DATA_DIR=/var/lib/air -e AIR_PG_BIN_DIR=/usr/lib/postgresql/17/bin \
  -e AIR_BOOTSTRAP_API_KEY=air_bench_token_local_only \
  air-server:pageserver 2>&1 | tail -n 5 || true)
log_res "[F10 Insufficient Disk 4MB tmpfs] $D10"

echo "Done Phase 6!"
