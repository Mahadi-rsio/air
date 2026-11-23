#!/usr/bin/env bash
set -euo pipefail

echo "[cleanup] Stopping any stray benchmark processes and removing /tmp/air-phase209-work ..."
pkill -f "/tmp/air-phase209-work" 2>/dev/null || true
rm -rf /tmp/air-phase209-work

CGROUP_ROOT="/sys/fs/cgroup/air_phase209"
if [ -d "$CGROUP_ROOT" ]; then
  for d in "$CGROUP_ROOT"/air_tenant_*; do
    [ -d "$d" ] && rmdir "$d" 2>/dev/null || true
  done
fi
echo "[cleanup] Done."
