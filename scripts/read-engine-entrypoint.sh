#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${AIR_READ_ENGINE_DATA_DIR:-/var/lib/air-read-engine}"
CACHE_DIR="${AIR_READ_ENGINE_CACHE_DIR:-$DATA_DIR/cache}"
mkdir -p "$DATA_DIR" "$CACHE_DIR"

if [ "$(id -u)" = "0" ]; then
  chown -R airread:airread "$DATA_DIR"
  exec gosu airread /usr/local/bin/air-read-engine "$@"
fi

exec /usr/local/bin/air-read-engine "$@"
