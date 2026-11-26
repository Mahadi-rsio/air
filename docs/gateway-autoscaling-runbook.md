# AIR Gateway Metering-Safe Horizontal Autoscaling Runbook

## 1. Architecture Summary

- **Baseline capacity**: One small baseline `air-pg-gateway` Fly Machine (`min_machines_running = 1`, `auto_stop_machines = "off"`, `auto_start_machines = false`) remains protected from autoscaler retirement.
- **Burst capacity**: Under sustained load, the Cloudflare Worker + SQLite Durable Object (`deploy/autoscaler`) provisions a dedicated Fly Volume and one larger burst Machine (`skip_service_registration = true` during startup), verifies `/readyz` + a real TLS PostgreSQL probe, and only then uncordons the burst Machine to admit traffic.
- **Scale-in (gated separately)**: Disabled by default (`AUTO_REMOVE_ENABLED = "false"`). When enabled, the controller selects only a burst Machine after sustained low load, verifies baseline capacity, cordons the burst Machine, waits for active SQL sessions/queries/streams to drain (never forcing disconnects), calls `/internal/v1/drain` with an operation-bound `op_id` to seal and flush the metering spool, stops and verifies the Machine before deletion, and **retains the detached Fly Volume** for recovery/audit.

---

## 2. Metering Durability Boundary

Each Gateway stores pending metering batches on its dedicated local Fly Volume (`AIR_METERING_LOG_DIR=/var/lib/air-gateway/metering`, owned by `airgw:airgw` uid `10001`).

- **What is protected**:
  - In billing-enabled environments (`AIR_BILLING_ENABLED=true`), missing/unwritable mounts or `/tmp` paths fail startup and `/readyz`.
  - Sealed batches (`batch.<id>.log`) have immutable IDs and SHA-256 payload digests committed atomically in a single PostgreSQL transaction alongside `metering_batch_receipts` and `tenant_usage` deltas.
  - Crashes after DB commit but before local file deletion, or ambiguous commit responses, are safely retried without double-counting.
  - Corrupt batches are moved to `quarantine/` and expose `Blocked=true` (blocking `/readyz` and instance retirement) without discarding data.
  - Spool usage is bounded by `AIR_METERING_MAX_SPOOL_BYTES` (default `256 MiB`); hitting the cap returns `ErrMeteringSpoolFull` and blocks retirement.
- **What is NOT protected (explicit exposure)**:
  - A Fly Volume is **local, unreplicated NVMe storage** attached to a single host. **Zero metering loss is NOT claimed on catastrophic host or volume hardware failure.**
  - When `AIR_METERING_SYNC_ON_WRITE=false` (default for throughput), buffered writes in the active `query_metering.log` since the last rotation/flush (`AIR_METERING_FLUSH_INTERVAL`) can be lost on abrupt power/kernel crash; enabling `AIR_METERING_SYNC_ON_WRITE=true` fsyncs each append at higher I/O latency.
  - When `AIR_METERING_LOG_DIR` reaches `AIR_METERING_MAX_SPOOL_BYTES` during a prolonged metadata DB outage, new metering appends fail with `ErrMeteringSpoolFull` and the Gateway surfaces `SpoolFull=true` / `Blocked=true` rather than silently dropping records or replaying customer SQL.

---

## 3. Legacy Metering Log Migration Procedure

Before rolling out durable receipt-based metering on an existing Gateway with legacy `query_metering.*.log` files:

1. **Preserve pending files**: On startup with the new binary, `MeteringLogger.Start()` automatically moves any legacy `query_metering.*.log` files into `${AIR_METERING_LOG_DIR}/legacy_pending/` with `.meta` sidecars (`status: "unknown_prior_commit"`), preventing accidental deletion or double-counting.
2. **Inspect legacy batches (dry-run)**:
   ```bash
   air-gateway --metering-reconcile \
     --metering-log-dir /var/lib/air-gateway/metering \
     --meta-dsn "$AIR_META_DSN"
   ```
   This reports each file in `legacy_pending/`, its deterministic `legacy_<sha256>` ID, entry count, and per-tenant deltas without mutating PostgreSQL.
3. **Reconcile and apply**:
   - Confirm against billing records whether any legacy batch was already applied before the upgrade.
   - For batches confirmed **not yet applied**, run with `--apply`:
     ```bash
     air-gateway --metering-reconcile --apply \
       --metering-log-dir /var/lib/air-gateway/metering \
       --meta-dsn "$AIR_META_DSN"
     ```
     This commits each legacy batch via `SaveMeteringBatchWithReceipt` (idempotent on `legacy_<sha256>`) and moves reconciled files out of `legacy_pending/`.

---

## 4. Detached Burst Volume Recovery Procedure

Automatic volume deletion is intentionally excluded from this release. After a burst Machine is deleted during scale-in:

1. **Verify Machine deletion & volume state**:
   ```bash
   fly machines list -a air-pg-gateway
   fly volumes list -a air-pg-gateway
   ```
2. **If drain/flush was confirmed** (`Drained=true`, `MeteringConfirmed=true`, `PendingBatches=0`, `QuarantinedCount=0`, `LegacyPendingCount=0`), the volume is safe to archive or manually delete after your retention window.
3. **If a burst Machine was stopped/terminated with unconfirmed metering**:
   - Create a temporary maintenance Machine in the same region attaching the retained volume at `/var/lib/air-gateway`:
     ```bash
     fly machine run <gateway-image> -a air-pg-gateway --region sin \
       --volume <vol_id>:/var/lib/air-gateway \
       --entrypoint /bin/sh -- -c "sleep 3600"
     ```
   - Inspect `/var/lib/air-gateway/metering` (`batch.*.log`, `quarantine/`, `legacy_pending/`).
   - Replay sealed/active batches using `--metering-reconcile --apply` (or `SealAndFlush`) against `AIR_META_DSN`. Receipt deduplication ensures already-committed batches are not double-counted.
   - Only after `PendingBatches=0` and `QuarantinedCount=0`, destroy the maintenance Machine and manually delete the volume.

---

## 5. Staged Rollout Gates (Staging -> Production)

Do **not** enable all gates at once. Verify each gate sequentially:

### Gate 1: Durable Metering & Multi-Gateway Safety (Single/Static Replicas)
1. Apply DB migration `migrations/008_metering_batch_receipts.sql` (or start `air-server` v8 migration).
2. Provision dedicated `air_gateway_metering` Fly Volume(s) in `air-pg-gateway` (`sin`) and deploy `fly.gateway.toml` (`AIR_BILLING_ENABLED=true`, `AIR_METERING_LOG_DIR=/var/lib/air-gateway/metering`, `auto_stop_machines="off"`, `min_machines_running=1`).
3. Verify `GET /readyz` on the private endpoint (`:8082`) returns `200 OK` and `GET /internal/v1/metering/status` shows `writable=true`, `sink_healthy=true`, `blocked=false`.
4. Confirm legacy files (if any) are reconciled per Section 3.

### Gate 2: Autoscaler `decision_only` -> `auto-add`
1. Deploy `deploy/autoscaler` with `AUTOSCALER_MODE="decision_only"`, `AUTO_ADD_ENABLED="false"`, `AUTO_REMOVE_ENABLED="false"`, and an app-scoped `FLY_API_TOKEN` + `AIR_INTERNAL_TOKEN`.
2. Observe 1-minute Cron Trigger logs and Durable Object `/status` for several load cycles; verify no unexpected create intents.
3. Enable `AUTOSCALER_MODE="enforce"` and `AUTO_ADD_ENABLED="true"` (keep `AUTO_REMOVE_ENABLED="false"`).
4. **Verify mixed VM routing on Fly Proxy**: Under load with both the small baseline Machine and larger burst Machine uncordoned, inspect per-Machine connection counts and CPU load. Fly Proxy does **not** guarantee CPU-proportional connection distribution; tune `ADD_THRESHOLD_UTIL` / `MAX_AGGREGATE_BACKENDS` based on observed distribution before enabling scale-in.

### Gate 3: Scale-In (`auto-remove`)
1. Enable `AUTO_REMOVE_ENABLED="true"` only after Gate 2 has run cleanly in staging.
2. Trigger a scale-out -> sustained low load cycle and verify in DO `/status` and Gateway logs:
   - Baseline Machine is never selected for removal.
   - Burst Machine is cordoned, active sessions finish without forced disconnect, `/internal/v1/drain` returns `Drained=true` and `MeteringConfirmed=true` for the exact `op_id`, and the Machine is stopped and verified before deletion.
   - The burst Fly Volume remains retained and detached.

---

## 6. Local Verification & Unverified Live Items

### Verified Locally
- **Go unit & integration tests**:
  - `internal/gateway`: durable metering (`/tmp` & unwritable rejection, sink readiness failure, corrupt batch quarantine, `ErrMeteringSpoolFull`, restart recovery, legacy preservation & reconciliation), cross-Gateway `CancelRequest` routing (including while owner is draining), aggregate writer backend limits, tenant drain blocking new checkouts, and operation-bound `DrainInstance` ack.
  - `internal/control`: `NewCoordinatedGRPCPoolDrainFn` deferring tenant suspension on missing membership or active work veto.
  - Real disposable PostgreSQL 17 container (`postgres:17-alpine`): `metering_batch_receipts` idempotency on retry after commit, payload mismatch detection, concurrent dual-Gateway spool flushing, and two Gateway instances serving real `pgx` clients concurrently while an in-flight transaction commits on Gateway A.
- **Cloudflare Worker & SQLite Durable Object tests** (`deploy/autoscaler`):
  - Node `node:sqlite` harness with mocked Fly API & Prometheus responses covering `decision_only` mode, `auto-add` intent persistence & inventory reconciliation on timeout, readiness failure leaving baseline serving, `auto-remove` drain deferral/admission restore on active sessions or unconfirmed metering, and successful scale-in retaining the volume.

### Not Verified Locally (Requires Live Staging)
- Live Fly Proxy TCP connection distribution across mixed VM sizes (`shared-cpu-2x` baseline vs `performance-2x` burst).
- Live Fly Machines API / Volumes API behavior and `.internal` 6PN DNS propagation latency during Machine creation/cordoning.
- Live Cloudflare Workers Cron Trigger execution (`wrangler deploy` was intentionally not run).
