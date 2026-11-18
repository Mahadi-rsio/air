#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${AIR_DATA_DIR:-/var/lib/air}"
mkdir -p "$DATA_DIR"

if [ "$(id -u)" = "0" ]; then
  chown -R postgres:postgres "$DATA_DIR"
  exec gosu postgres /usr/local/bin/air-entrypoint.sh "$@"
fi

exec /usr/local/bin/air-server "$@"
