#!/usr/bin/env bash
set -euo pipefail

STATE_DIR="${AUTOSCALER_STATE_DIR:-/var/lib/air-autoscaler}"
mkdir -p "$STATE_DIR"

if [ "$(id -u)" = "0" ]; then
  chown -R airauto:airauto "$STATE_DIR"
  exec gosu airauto /usr/local/bin/air-autoscaler "$@"
fi

exec /usr/local/bin/air-autoscaler "$@"
