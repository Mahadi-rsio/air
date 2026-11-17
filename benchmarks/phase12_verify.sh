#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="/workspaces/air/benchmarks/results/phase12"
mkdir -p "$OUT_DIR"
LOG="$OUT_DIR/phase12_verify.log"
: > "$LOG"

set -a; source /workspaces/air/.env; set +a
API_URL="http://127.0.0.1:8080"
TOKEN="air_bench_token_local_only"
INTERNAL_TOK="p12_internal_tok"
PROXY_ADMIN="http://127.0.0.1:19001"

log() {
  echo "$1" | tee -a "$LOG"
}

go build -o /tmp/r2proxy ./benchmarks/r2proxy
/tmp/r2proxy -listen 127.0.0.1:19000 -admin 127.0.0.1:19001 -endpoint "$ENDPOINT" -access-key "$ACCESS_KEY" -secret-key "$SECRET_KEY" > "$OUT_DIR/r2proxy.log" 2>&1 &
PROXY_PID=$!
cleanup() {
  kill "$PROXY_PID" 2>/dev/null || true
  docker rm -f air-bench-p12 >/dev/null 2>&1 || true
}
trap cleanup EXIT
sleep 1

set_fault() {
  curl -sf -X POST "$PROXY_ADMIN/fault" -H "Content-Type: application/json" -d "$1" >/dev/null
}

start_air() {
  docker rm -f air-bench-p12 >/dev/null 2>&1 || true
  docker run --rm -v /tmp/air-p12-data:/var/lib/air --entrypoint sh air-server:pageserver -c "rm -rf /var/lib/air/*"
  go run ./benchmarks/r2tool -bucket air-pg-backups -endpoint "$ENDPOINT" -access-key "$ACCESS_KEY" -secret-key "$SECRET_KEY" -clean >/dev/null
  docker run -d --name air-bench-p12 --network host \
    --entrypoint sh \
    -v /tmp/air-p12-data:/var/lib/air \
    -e AIR_DATA_DIR=/var/lib/air \
    -e AIR_PG_BIN_DIR=/usr/lib/postgresql/17/bin \
    -e AIR_HTTP_ADDR=127.0.0.1:8080 \
    -e AIR_GRPC_ADDR=127.0.0.1:9090 \
    -e AIR_PAGESERVER_ENABLED=true \
    -e AIR_PAGESERVER_CACHE_MB=256 \
    -e AIR_R2_PRIMARY=true \
    -e AIR_R2_BUCKET=air-pg-backups \
    -e AIR_R2_ENDPOINT=http://127.0.0.1:19000 \
    -e AIR_R2_ACCESS_KEY_ID="$ACCESS_KEY" \
    -e AIR_R2_SECRET_ACCESS_KEY="$SECRET_KEY" \
    -e AIR_BOOTSTRAP_API_KEY="$TOKEN" \
    -e AIR_INTERNAL_TOKEN="$INTERNAL_TOK" \
    -e AIR_IDLE_TIMEOUT=0 \
    -e AIR_POOL_MODE=false \
    air-server:pageserver -c "chown -R postgres:postgres /var/lib/air && exec gosu postgres /usr/local/bin/air-server" >/dev/null
  for i in $(seq 1 40); do
    if curl -sf "$API_URL/healthz" >/dev/null; then break; fi
    sleep 1
  done
}

provision_tenant() {
  local tid="$1"
  local idem="$2"
  curl -sf -X POST "$API_URL/v1/databases" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: $idem" \
    -d "{\"tenant_id\":\"$tid\"}"
}

get_route() {
  local tid="$1"
  curl -sf -H "X-Air-Internal-Token: $INTERNAL_TOK" "$API_URL/internal/v1/tenants/$tid/route"
}

time_ms() {
  python3 -c "import time; print(int(time.time()*1000))"
}

log "=== Phase 12 Real Container Verification & Benchmarks ==="
start_air

# ------------------------------------------------------------------
# 1. Part 4: Real PG17 air_smgr Storage Introspection
# ------------------------------------------------------------------
log ""
log "--- [Part 4] PG17 air_smgr Storage Introspection ---"
PROV=$(provision_tenant "p12_introspect" "p12-intro-1")
ROUTE=$(get_route "p12_introspect")
PORT=$(echo "$ROUTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
PASS=$(echo "$ROUTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['app_password'])")
export PGPASSWORD="$PASS"

psql -h 127.0.0.1 -p "$PORT" -U app_user -d p12_introspect -v ON_ERROR_STOP=1 <<'SQL'
CREATE TABLE items (
  id int primary key,
  pad text,
  big_payload text
);
INSERT INTO items SELECT g, repeat('x', 120), repeat(md5(g::text), 400) FROM generate_series(1, 500) g;
CREATE INDEX idx_items_pad ON items(pad);
SQL

SIZES_BEFORE=$(psql -h 127.0.0.1 -p "$PORT" -U app_user -d p12_introspect -t -A -c "
SELECT
  pg_relation_size('items'),
  pg_relation_size('items', 'fsm'),
  pg_relation_size('items', 'vm'),
  pg_table_size('items'),
  pg_total_relation_size('items'),
  pg_relation_size('idx_items_pad'),
  pg_database_size('p12_introspect');
")
log "SIZES_AFTER_INSERT: $SIZES_BEFORE"

# Updates, deletes, vacuum, and second database
psql -h 127.0.0.1 -p "$PORT" -U app_user -d p12_introspect -v ON_ERROR_STOP=1 <<'SQL'
UPDATE items SET pad = repeat('y', 120) WHERE id <= 250;
DELETE FROM items WHERE id > 400;
VACUUM items;
CREATE TABLE aux_tbl (id int, val text);
INSERT INTO aux_tbl SELECT g, repeat('a', 200) FROM generate_series(1, 200) g;
SQL

SIZES_AFTER_MUT=$(psql -h 127.0.0.1 -p "$PORT" -U app_user -d p12_introspect -t -A -c "
SELECT pg_relation_size('items'), pg_table_size('items'), pg_total_relation_size('items'), pg_database_size('p12_introspect');
")
log "SIZES_AFTER_UPDATE_DELETE_VACUUM_AUX: $SIZES_AFTER_MUT"

# Suspend + Wake + Container Restart -> sizes must NOT regress
curl -sf -H "X-Air-Internal-Token: $INTERNAL_TOK" -X POST "$API_URL/internal/v1/databases/p12_introspect/suspend" >/dev/null
curl -sf -H "X-Air-Internal-Token: $INTERNAL_TOK" -X POST "$API_URL/internal/v1/databases/p12_introspect/start" >/dev/null
ROUTE=$(get_route "p12_introspect")
PORT=$(echo "$ROUTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
SIZES_AFTER_WAKE=$(psql -h 127.0.0.1 -p "$PORT" -U app_user -d p12_introspect -t -A -c "
SELECT pg_relation_size('items'), pg_table_size('items'), pg_total_relation_size('items'), pg_database_size('p12_introspect');
")
log "SIZES_AFTER_SUSPEND_WAKE: $SIZES_AFTER_WAKE"

docker restart air-bench-p12 >/dev/null
for i in $(seq 1 40); do
  if curl -sf "$API_URL/healthz" >/dev/null; then break; fi
  sleep 1
done
curl -sf -H "X-Air-Internal-Token: $INTERNAL_TOK" -X POST "$API_URL/internal/v1/databases/p12_introspect/start" >/dev/null
ROUTE=$(get_route "p12_introspect")
PORT=$(echo "$ROUTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
SIZES_AFTER_RESTART=$(psql -h 127.0.0.1 -p "$PORT" -U app_user -d p12_introspect -t -A -c "
SELECT pg_relation_size('items'), pg_table_size('items'), pg_total_relation_size('items'), pg_database_size('p12_introspect');
")
log "SIZES_AFTER_RESTART: $SIZES_AFTER_RESTART"

# Truncate and Drop relation -> sizes must drop cleanly
psql -h 127.0.0.1 -p "$PORT" -U app_user -d p12_introspect -v ON_ERROR_STOP=1 <<'SQL'
TRUNCATE TABLE items;
DROP TABLE aux_tbl;
SQL
SIZES_AFTER_TRUNC_DROP=$(psql -h 127.0.0.1 -p "$PORT" -U app_user -d p12_introspect -t -A -c "
SELECT pg_relation_size('items'), pg_table_size('items'), pg_total_relation_size('items'), pg_database_size('p12_introspect');
")
log "SIZES_AFTER_TRUNCATE_DROP: $SIZES_AFTER_TRUNC_DROP"

# ------------------------------------------------------------------
# 2. Part 1 & 7: Tenant ID Reuse Isolation
# ------------------------------------------------------------------
log ""
log "--- [Part 1 & 7] Tenant ID Reuse Isolation ---"
provision_tenant "p12_reuse" "p12-reuse-1" >/dev/null
ROUTE=$(get_route "p12_reuse")
PORT=$(echo "$ROUTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
PASS=$(echo "$ROUTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['app_password'])")
PGPASSWORD="$PASS" psql -h 127.0.0.1 -p "$PORT" -U app_user -d p12_reuse -v ON_ERROR_STOP=1 -c "
CREATE TABLE old_secret (id int primary key, body text);
INSERT INTO old_secret SELECT g, repeat('secret', 500) FROM generate_series(1, 50) g;
" >/dev/null
curl -sf -H "X-Air-Internal-Token: $INTERNAL_TOK" -X POST "$API_URL/internal/v1/databases/p12_reuse/suspend" >/dev/null
sleep 1
curl -sf -X DELETE -H "Authorization: Bearer $TOKEN" "$API_URL/v1/databases/p12_reuse" >/dev/null

T0=$(time_ms)
provision_tenant "p12_reuse" "p12-reuse-2" >/dev/null
T1=$(time_ms)
ROUTE=$(get_route "p12_reuse")
PORT=$(echo "$ROUTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
PASS=$(echo "$ROUTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['app_password'])")
TABLES_RECREATED=$(PGPASSWORD="$PASS" psql -h 127.0.0.1 -p "$PORT" -U app_user -d p12_reuse -t -A -c "SELECT count(*) FROM information_schema.tables WHERE table_schema='public';")
log "TENANT_RECREATE_MS=$((T1 - T0)) RECREATED_PUBLIC_TABLES=$TABLES_RECREATED"

# ------------------------------------------------------------------
# 3. Part 2, 3, 7: WakeTenant under Normal R2, Slow R2 (3s), Unavailable R2
# ------------------------------------------------------------------
log ""
log "--- [Part 2, 3, 7] Normal vs Slow vs Unavailable R2 ---"
provision_tenant "p12_wake" "p12-wake-1" >/dev/null
ROUTE=$(get_route "p12_wake")
PORT=$(echo "$ROUTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
PASS=$(echo "$ROUTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['app_password'])")
PGPASSWORD="$PASS" psql -h 127.0.0.1 -p "$PORT" -U app_user -d p12_wake -c "CREATE TABLE w(id int); INSERT INTO w VALUES (42);" >/dev/null

# Normal Suspend + Wake
TS0=$(time_ms)
curl -sf -H "X-Air-Internal-Token: $INTERNAL_TOK" -X POST "$API_URL/internal/v1/databases/p12_wake/suspend" >/dev/null
TS1=$(time_ms)
TW0=$(time_ms)
curl -sf -H "X-Air-Internal-Token: $INTERNAL_TOK" -X POST "$API_URL/internal/v1/databases/p12_wake/start" >/dev/null
TW1=$(time_ms)
log "NORMAL_R2_SUSPEND_MS=$((TS1 - TS0)) NORMAL_R2_WAKE_MS=$((TW1 - TW0))"

# Slow R2 (3s timeout/delay on proxy)
provision_tenant "p12_slow" "p12-slow-1" >/dev/null
set_fault '{"mode":"timeout","timeout_ms":3000}'
TS2=$(time_ms)
curl -s -H "X-Air-Internal-Token: $INTERNAL_TOK" -X POST "$API_URL/internal/v1/databases/p12_wake/suspend" >/dev/null &
PID_S=$!
sleep 0.2
TW2=$(time_ms)
curl -sf -H "X-Air-Internal-Token: $INTERNAL_TOK" -X POST "$API_URL/internal/v1/databases/p12_wake/start" >/dev/null
TW3=$(time_ms)
wait "$PID_S" || true
TS3=$(time_ms)
set_fault '{"mode":""}'
log "SLOW_R2_3S_WAKE_MS=$((TW3 - TW2)) SLOW_R2_3S_SUSPEND_MS=$((TS3 - TS2))"

# Unavailable R2 -> Offload status must report failure, no false success
set_fault '{"mode":"unavailable"}'
SUS_UNAVAIL=$(curl -s -w " HTTP:%{http_code}" -H "X-Air-Internal-Token: $INTERNAL_TOK" -X POST "$API_URL/internal/v1/databases/p12_slow/suspend")
OFF_STATUS=$(curl -s -H "Authorization: Bearer $TOKEN" "$API_URL/v1/databases/p12_slow/offload")
set_fault '{"mode":""}'
log "UNAVAIL_R2_SUSPEND: $SUS_UNAVAIL"
log "UNAVAIL_R2_OFFLOAD_STATUS: $OFF_STATUS"

# ------------------------------------------------------------------
# 4. Part 7: 25+ Tenants Isolation + Resource Usage
# ------------------------------------------------------------------
log ""
log "--- [Part 7] 25-Tenant Provisioning & Isolation ---"
M0=$(time_ms)
for i in $(seq 1 25); do
  tid="p12_mt_${i}"
  provision_tenant "$tid" "p12-mt-${i}" >/dev/null
  ROUTE=$(get_route "$tid")
  PORT=$(echo "$ROUTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
  PASS=$(echo "$ROUTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['app_password'])")
  PGPASSWORD="$PASS" psql -h 127.0.0.1 -p "$PORT" -U app_user -d "$tid" -q -c "CREATE TABLE t_iso(v int); INSERT INTO t_iso VALUES ($i);"
done
M1=$(time_ms)

# Suspend first 12, then wake them, verify all 25 return exact marker value
for i in $(seq 1 12); do
  curl -sf -H "X-Air-Internal-Token: $INTERNAL_TOK" -X POST "$API_URL/internal/v1/databases/p12_mt_${i}/suspend" >/dev/null
done
for i in $(seq 1 12); do
  curl -sf -H "X-Air-Internal-Token: $INTERNAL_TOK" -X POST "$API_URL/internal/v1/databases/p12_mt_${i}/start" >/dev/null
done

MISMATCH=0
for i in $(seq 1 25); do
  tid="p12_mt_${i}"
  ROUTE=$(get_route "$tid")
  PORT=$(echo "$ROUTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['port'])")
  PASS=$(echo "$ROUTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['app_password'])")
  VAL=$(PGPASSWORD="$PASS" psql -h 127.0.0.1 -p "$PORT" -U app_user -d "$tid" -t -A -c "SELECT v FROM t_iso;")
  if [ "$VAL" != "$i" ]; then
    MISMATCH=$((MISMATCH + 1))
  fi
done
M2=$(time_ms)
DOCKER_STATS=$(docker stats --no-stream --format "mem={{.MemUsage}} cpu={{.CPUPerc}}" air-bench-p12)
log "MT_25_PROVISION_WRITE_MS=$((M1 - M0)) MT_25_TOTAL_MS=$((M2 - M0)) MISMATCHES=$MISMATCH CONTAINER_STATS=($DOCKER_STATS)"
log "=== Phase 12 Verification Complete ==="
