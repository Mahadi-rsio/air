#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${AIR_DATA_DIR:-/var/lib/air-pageserver}"
mkdir -p "$DATA_DIR/ps_layers" "$DATA_DIR/ps_cache"

if [ "$(id -u)" = "0" ]; then
  chown -R airps:airps "$DATA_DIR"
  exec gosu airps /usr/local/bin/air-pageserver "$@"
fi

exec /usr/local/bin/air-pageserver "$@"
