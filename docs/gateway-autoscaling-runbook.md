# AIR Gateway Metering-Safe Horizontal Autoscaling Runbook

## 1. Architecture Summary

- **Baseline capacity**: One small baseline `air-pg-gateway` Fly Machine (`min_machines_running = 1`, `auto_stop_machines = "off"`, `auto_start_machines = false`), explicitly identified by `BASELINE_MACHINE_ID`, remains protected from autoscaler retirement.
- **Native Go controller (`cmd/air-autoscaler`)**: A single controller Machine (`air-pg-autoscaler`, configured in [`deploy/fly/fly.autoscaler.toml`](../deploy/fly/fly.autoscaler.toml)) runs in the same Fly 6PN private network and region (`sin`) as `air-pg-gateway` with no public IP (`--ha=false --no-public-ips`) and a dedicated Fly Volume (`air_autoscaler_data` mounted at `/var/lib/air-autoscaler`) for atomic operation intent and progress persistence.
- **Direct 6PN metrics & TLS verification**: On each tick, the controller reads active connection counts and configured `max_client_conns` limits directly from each gateway Machine's private HTTP API (`http://[<private_ip>]:8082/internal/v1/metrics`), pausing scaling decisions if required metrics are missing. Before admitting a burst Machine, it verifies `/readyz` subsystem checks, `/internal/v1/metering/status`, and a direct PostgreSQL `SSLRequest` + TLS handshake over 6PN (`[<private_ip>]:5432`) with full certificate and hostname validation (`AIR_GATEWAY_TLS_SERVER_NAME`).
- **Burst capacity (`auto-add`)**: Under sustained load, the controller persists `intent`, provisions or reuses a dedicated encrypted Fly Volume, creates one larger burst Machine with `AIR_GATEWAY_CORDONED=true` and `skip_service_registration = true`, verifies readiness + metering + PostgreSQL TLS, and only then uncordons and enables Fly Proxy service registration.
- **Scale-in (`auto-remove`, gated separately)**: Disabled by default (`AUTO_REMOVE_ENABLED = "false"`). When enabled, the controller verifies baseline health, selects only a burst Machine (never `BASELINE_MACHINE_ID`) after sustained low load, cordons it (rejecting new PostgreSQL sessions with `57P03` while existing sessions and `CancelRequest` continue to work), calls `/internal/v1/drain` with an operation-bound `op_id` to drain sessions and flush the metering spool, stops and confirms `stopped` before deleting only that Machine, and **retains the detached Fly Volume** for recovery/audit.

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

## 5. Staged Rollout & Cutover Gates (Staging -> Production)

Do **not** enable all gates at once. Verify each gate sequentially:

### Gate 1: Durable Metering & Multi-Gateway Safety (Single/Static Replicas)
1. Apply DB migration `migrations/008_metering_batch_receipts.sql` (or start `air-server` v8 migration).
2. Provision dedicated `air_gateway_metering` Fly Volume(s) in `air-pg-gateway` (`sin`) and deploy `fly.gateway.toml` (`AIR_BILLING_ENABLED=true`, `AIR_METERING_LOG_DIR=/var/lib/air-gateway/metering`, `auto_stop_machines="off"`, `min_machines_running=1`).
3. Verify `GET /readyz` on the private endpoint (`:8082`) returns `200 OK` and `GET /internal/v1/metering/status` shows `writable=true`, `sink_healthy=true`, `blocked=false`.
4. Confirm legacy files (if any) are reconciled per Section 3.

### Gate 2: Deploy Go Autoscaler in `decision_only` & Reconcile Any In-Flight Worker Operations
1. Provision the single-instance controller app and dedicated Fly Volume in `sin`:
   ```bash
   fly apps create air-pg-autoscaler
   fly volumes create air_autoscaler_data --app air-pg-autoscaler --region sin --size 1 --yes
   ```
2. Record the explicit baseline Machine ID from `fly machines list -a air-pg-gateway`, then stage secrets:
   ```bash
   fly secrets set --app air-pg-autoscaler --stage \
     FLY_API_TOKEN="$FLY_API_TOKEN" \
     AIR_INTERNAL_TOKEN="$AIR_INTERNAL_TOKEN" \
     BASELINE_MACHINE_ID="$BASELINE_MACHINE_ID"
   ```
3. Deploy `air-pg-autoscaler` in `decision_only` mode (`AUTOSCALER_MODE="decision_only"`, `AUTO_ADD_ENABLED="true"`, `AUTO_REMOVE_ENABLED="false"`):
   ```bash
   fly deploy . \
     -c deploy/fly/fly.autoscaler.toml \
     --dockerfile Dockerfile.autoscaler \
     --depot=false --remote-only --ha=false --no-public-ips --yes
   ```
4. Verify `/healthz` and `/status` from within the 6PN network and confirm `air-pg-autoscaler` logs show successful `/internal/v1/metrics` polls over 6PN with zero Fly API mutations.
5. **Reconcile any in-flight Worker operations**: If migrating from an existing Worker autoscaler deployment, inspect its `/status` endpoint and compare against `fly machines list -a air-pg-gateway` and `fly volumes list -a air-pg-gateway`. Ensure no `scale_out` or `scale_in` operation is left in `intent`, `volume_ready`, `machine_created`, `draining`, or `unresolved` state, and resolve or clean up any orphaned `air_gw_burst_*` resources before mutating from Go.
6. **Disable the Worker's mutation path**: Set `AUTOSCALER_MODE="decision_only"`, `AUTO_ADD_ENABLED="false"`, `AUTO_REMOVE_ENABLED="false"` on the legacy Worker (or remove its Cron Trigger / delete the Worker) so only the Go controller can mutate Fly inventory.

### Gate 3: Enable Go Scale-Out (`auto-add`)
1. Only after Gate 2 is clean and the legacy Worker mutation path is disabled, enable Go scale-out by updating `AUTOSCALER_MODE="enforce"` (keeping `AUTO_REMOVE_ENABLED="false"`):
   ```bash
   fly secrets set --app air-pg-autoscaler AUTOSCALER_MODE="enforce"
   ```
2. Trigger sustained load and verify `air-pg-autoscaler` `/status` and logs show:
   - Volume created/reused and burst Machine created with `skip_service_registration=true` and `AIR_GATEWAY_CORDONED=true`.
   - `/readyz`, `/internal/v1/metering/status`, and direct 6PN PostgreSQL TLS verification (`SSLRequest` + cert/hostname check) pass before uncordon and `skip_service_registration=false`.
3. **Verify mixed VM routing on Fly Proxy**: Under load with both the small baseline Machine and larger burst Machine uncordoned, inspect per-Machine connection counts and CPU load. Fly Proxy does **not** guarantee CPU-proportional connection distribution; tune `HIGH_CONN_UTIL_PCT` / `MIN_SPARE_CONNS` / `MAX_AGGREGATE_BACKENDS` based on observed distribution before enabling scale-in.

### Gate 4: Separate Staging Check & Production Scale-In (`auto-remove`)
1. **Separate staging check required**: Before enabling `AUTO_REMOVE_ENABLED="true"` in production, run a dedicated staging check on a staging `air-pg-gateway` + `air-pg-autoscaler` pair with `AUTO_REMOVE_ENABLED="true"`:
   - Trigger a scale-out -> sustained low load cycle.
   - Verify `BASELINE_MACHINE_ID` is never selected for removal and remains healthy (`/readyz` = `200`).
   - Verify cordoning rejects new PostgreSQL sessions (`57P03`) while existing sessions and `CancelRequest` continue to work.
   - Verify `/internal/v1/drain` returns `drained=true` and `metering_confirmed=true` for the exact `op_id`.
   - Verify the burst Machine is stopped, confirmed `stopped`, and deleted, while its burst Fly Volume remains retained and detached.
2. Only after the staging check passes, enable `AUTO_REMOVE_ENABLED="true"` in production.

---

## 6. Local Verification & Unverified Live Items

### Verified Locally
- **Go unit & integration tests (`internal/gateway`)**:
  - Durable metering (`/tmp` & unwritable rejection, sink readiness failure, corrupt batch quarantine, `ErrMeteringSpoolFull`, restart recovery, legacy preservation & reconciliation).
  - Cross-Gateway `CancelRequest` routing (including while owner is draining), aggregate writer backend limits, tenant drain blocking new checkouts, and operation-bound `DrainInstance` ack.
  - Cordoning rejects new PostgreSQL sessions (`57P03`) while existing sessions and `CancelRequest` continue to work.
- **Go unit & integration tests (`internal/autoscaler`)**:
  - `decision_only` defaults (`AUTO_REMOVE_ENABLED=false`) and zero Fly mutations under sustained load.
  - Missing/failing `/internal/v1/metrics` pauses scaling decisions without advancing sustained load counters.
  - Scale-out creates cordoned burst with metering volume and verifies readiness, metering, and direct 6PN PostgreSQL TLS (including certificate + hostname validation against real TLS listeners) before admission.
  - Restart recovery reconciles persisted `intent` on the state volume against Fly inventory without creating duplicate volumes or Machines.
  - Ambiguous create timeouts check Fly inventory before retrying (adopting leaked creates or marking `unresolved` to block duplicates).
  - Explicit `BASELINE_MACHINE_ID` protection, replica limits, and cooldowns.
  - Metering-safe scale-in: baseline health check, cordon + op-bound drain ack, defer/uncordon on unconfirmed metering, stop + confirm `stopped` before deleting only the burst Machine, and retaining the detached volume.

### Not Verified Locally (Requires Live Fly Staging)
- Live Fly Proxy TCP connection distribution across mixed VM sizes (`shared-cpu-2x` baseline vs `shared-cpu-4x` burst).
- Live Fly Machines API / Volumes API behavior and `.internal` 6PN DNS/IPv6 propagation latency during Machine creation and cordoning.
- Live `air-pg-autoscaler` deployment on Fly.io (`fly deploy` was intentionally not run).
