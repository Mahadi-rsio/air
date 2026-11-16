#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${AIR_DATA_DIR:-/var/lib/air}"
mkdir -p "$DATA_DIR"

if [ "$(id -u)" = "0" ]; then
  chown -R postgres:postgres "$DATA_DIR"
  exec gosu postgres /usr/local/bin/air-entrypoint.sh "$@"
fi

/usr/local/bin/air-server "$@" &
SERVER_PID=$!

/usr/local/bin/air-gateway &
GATEWAY_PID=$!

cleanup() {
  kill -TERM "$GATEWAY_PID" "$SERVER_PID" 2>/dev/null || true
}
trap cleanup TERM INT

wait -n "$SERVER_PID" "$GATEWAY_PID"
STATUS=$?
cleanup
wait "$SERVER_PID" "$GATEWAY_PID" 2>/dev/null || true
exit "$STATUS"
