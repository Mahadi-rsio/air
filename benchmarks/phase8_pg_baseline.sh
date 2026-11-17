#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="/workspaces/air/benchmarks/results/phase8"
mkdir -p "$OUT_DIR"

echo "=== Phase 8: Standard PostgreSQL 17.12 Local Storage Baseline ==="

docker rm -f pg17-baseline 2>/dev/null || true
rm -rf /tmp/pg17-baseline-data
mkdir -p /tmp/pg17-baseline-data
chmod 0777 /tmp/pg17-baseline-data

# Run vanilla PG 17.12 from the exact same image (without air_smgr)
docker run -d --name pg17-baseline \
  --network host \
  -v /tmp/pg17-baseline-data:/var/lib/pgdata \
  --entrypoint /bin/bash \
  air-server:pageserver -c "
    chown -R postgres:postgres /var/lib/pgdata
    gosu postgres /usr/lib/postgresql/17/bin/initdb -D /var/lib/pgdata --auth-local=trust --auth-host=trust --data-checksums >/dev/null
    cat >> /var/lib/pgdata/postgresql.conf <<EOF
listen_addresses = '127.0.0.1'
port = 15433
max_connections = 100
shared_buffers = 32MB
work_mem = 4MB
maintenance_work_mem = 16MB
temp_file_limit = 32MB
wal_level = replica
min_wal_size = 32MB
summarize_wal = on
EOF
    gosu postgres /usr/lib/postgresql/17/bin/pg_ctl -D /var/lib/pgdata -l /var/lib/pgdata/pg.log start
    sleep infinity
  "

for i in $(seq 1 30); do
  if pg_isready -h 127.0.0.1 -p 15433 -U postgres >/dev/null 2>&1; then
    echo "pg17-baseline ready after ${i}s"
    break
  fi
  sleep 1
done

psql -h 127.0.0.1 -p 15433 -U postgres -c "CREATE DATABASE bench;"
pgbench -h 127.0.0.1 -p 15433 -U postgres -i -s 10 bench > "$OUT_DIR/init_s10.log" 2>&1
pgbench -h 127.0.0.1 -p 15433 -U postgres -c 8 -j 2 -T 15 -S bench > /dev/null 2>&1

RESULTS="$OUT_DIR/pg_baseline_results.jsonl"
: > "$RESULTS"

for mode_c in "RO:8:-S" "RO:32:-S" "RW:8:" "RW:32:"; do
  IFS=':' read -r mode c flag <<< "$mode_c"
  out="$OUT_DIR/${mode}_c${c}.out"
  lp="/tmp/pgb_base_${mode}_c${c}"
  rm -f "${lp}"*
  pgbench -h 127.0.0.1 -p 15433 -U postgres -c "$c" -j 2 -T 30 $flag -l --log-prefix="$lp" bench > "$out" 2>&1
  parsed=$(python3 /workspaces/air/benchmarks/parse_pgbench.py "$out" "$lp")
  python3 -c "import json; d=json.loads('''$parsed'''); d['engine']='pg17-local'; d['mode']='$mode'; d['clients']=$c; print(json.dumps(d))" >> "$RESULTS"
done

docker rm -f pg17-baseline >/dev/null
rm -rf /tmp/pg17-baseline-data
cat "$RESULTS"
