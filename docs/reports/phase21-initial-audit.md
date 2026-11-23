# AIR Phase 21 — Initial Repository Audit

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Commit:** `85aa02e` (`feat(phase20.9): real PostgreSQL 17 validation, telemetry publisher, and benchmark reports`)  
**Working Tree:** Clean (`git status` clean, up to date with `origin/main`)  
**Environment:** Linux `6.18.57` x86_64, 2 vCPU, 7.8 GiB RAM, 8.0 GiB Swap, 85 GiB free disk, Docker `29.10.0` (cgroup v2, `systemd` driver), Go `go` at `/usr/local/go/bin/go`. Host has no system `postgres`/`psql` installed; PostgreSQL 17.12 + `pg17-smgr-hook.patch` + `air_smgr.so` are built reproducibly via `Dockerfile`.

---

## 1. Current Architecture

AIR is a multi-tenant PostgreSQL-compatible database platform composed of four Go services and per-tenant PostgreSQL 17.12 instances:

1. **`air-gateway` (`cmd/air-gateway/main.go`, `internal/gateway/`)**
   - PostgreSQL wire-protocol v3.0 proxy and transaction-mode connection pooler (`PoolMode=true`).
   - Enforces Phase 20 Write Governor (`write_governor.go`), Deficit Round Robin (DRR) scheduling, global/session buffer budgets (`buffer_budget.go`), Lazy LSN RYOW tracking (`pooler.go`), streaming `COPY FROM STDIN`, and optional Adaptive Controller (`adaptive_controller.go`, currently `AIR_WRITE_ADAPTIVE_ENABLED=false` by default).
   - Admin HTTP (`:8081`) exposes `/healthz`, `/metrics`, `/internal/v1/write-governor`, and `/internal/v1/telemetry` (receiving `CellTelemetry` snapshots with 15s TTL).
   - Admin gRPC (`:9093`) handles cooperative pool draining.

2. **`air-server` (`cmd/air-server/main.go`, `internal/control/`, `internal/cell/`)**
   - Hosts the Control Plane (HTTP `:8080`, gRPC `:9090`) and Cell Agent (`internal/cell/agent.go`).
   - Cell Agent manages per-tenant PG17 cluster lifecycle (`ClaimCluster`, `ActivateCluster`, `StartOrAdoptTenant`, `SuspendTenant`), `flock` ownership, `postgresql.conf`/`pg_hba.conf` generation (`WriteTenantConfigs`), cgroup v2 attachment (`<cgroupRoot>/air_tenant_<tenantID>`), and `TelemetryPublisher` (`internal/cell/telemetry_publisher.go`).
   - Control Plane manages tenant metadata (local PG17 meta or external `AIR_META_DSN`), WALReceiver streaming into Pageserver (`ensurePageserverBootstrapAndWAL`), R2 backup/cold-offload, and read-engine orchestration.

3. **`air-pageserver` (`cmd/air-pageserver/main.go`, `internal/pageserver/`)**
   - Multi-tenant `(Key, LSN)` page/WAL storage engine with in-memory layers, disk layers, bounded LRU disk cache (`DiskCache`), optional `air-walredo` subprocess, R2 cold tiering, and envelope KMS encryption.
   - Exposes:
     - **gRPC (`:9091`)**: Control-plane & data-plane RPCs (`EnsureTenant`, `BootstrapTenantFromPGData`, `IngestWALRecords`, `FlushTenant`, `PurgeTenant`, `SaveMetadataSkeleton`, `RestoreMetadataSkeleton`, `GetPageAtLSN`, `GetPageBatch`, `GetRelSize`, `GetRelExists`, `GetDBSize`, `PutPageBatch`, `UnlinkRel`, `GetSLRUPage`, `GetModifiedBlocksSince`, `Health`).
     - **Unix domain socket (`--socket-path` / `AIR_PAGESERVER_SOCKET`)**: Binary `AIRP` wire protocol (`internal/pageserver/protocol.go`) for `air_smgr.so` and `air-read-engine`.
     - **HTTP (`:8083`)**: `/healthz` and `/v1/namespaces` management API.

4. **`air-read-engine` (`cmd/air-read-engine/main.go`, `internal/readadapter/`)**
   - SQLite-backed read-only PostgreSQL wire-protocol adapter (`:5433`, gRPC `:9092`, HTTP `:8082`) reading pages from Pageserver over Unix socket or gRPC.

---

## 2. PostgreSQL 17.12 Patch Requirements

`patches/pg17-smgr-hook.patch` (370 lines) patches PostgreSQL 17.12 in three files:
1. `src/include/storage/smgr.h`:
   - Moves `typedef struct f_smgr` to the public header.
   - Defines `MAX_SMGR = 4`, `smgr_hook_type`, `smgr_dbsize_hook_type`, `smgr_register(const f_smgr *)`, and `smgr_get_standard(void)`.
2. `src/backend/storage/smgr/smgr.c`:
   - Converts `smgrsw[]` from a static 1-element array to `static const f_smgr *smgrsw[MAX_SMGR] = { &smgr_md }`, with `smgr_register()` and `smgr_hook` dispatch in `smgropen()`.
3. `src/backend/utils/adt/dbsize.c`:
   - Invokes `smgr_dbsize_hook` in `calculate_database_size()` and checks `reln->smgr_which != 0` in `calculate_relation_size()`.

---

## 3. `air_smgr` Build and Loading Process

- **Source:** `extensions/air_smgr/` (`air_smgr.c`, `communicator.c`, `communicator.h`, `lfc.c`, `lfc.h`, `protocol.h`, `Makefile`, `air_smgr.control`, `air_smgr--1.0.sql`).
- **Build:** PGXS (`make PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config && make install`) compiles `air_smgr.so` into `/usr/lib/postgresql/17/lib/air_smgr.so`.
- **Loading & GUCs:** Loaded via `shared_preload_libraries = 'supautils,air_smgr'` (`PGC_POSTMASTER`). Registers:
  - `air_smgr.enabled` (bool, default `false`)
  - `air_smgr.read_only` (bool, default `false`)
  - `air_smgr.pageserver_socket` (string, default `/var/lib/air/pageserver.sock`)
  - `air_smgr.tenant_id` (string, default `""`)
  - `air_smgr.lfc_dir` (string, default `/var/lib/air/lfc`)
  - `air_smgr.lfc_max_mb` (int, default `256`)
- **Hook Behavior:** `_PG_init` calls `smgr_register(&air_smgr_impl)` and sets `smgr_hook = air_smgr_hook` and `smgr_dbsize_hook = air_dbsize_hook`. When `air_smgr_enabled == true` and `backend == INVALID_PROC_NUMBER` (persistent relations, not temp tables), `air_smgr_hook` returns `air_smgr_id`, routing `smgrcreate`, `smgrexists`, `smgrunlink`, `smgrextend`, `smgrzeroextend`, `smgrreadv`, `smgrwritev`, `smgrnblocks`, and `smgrtruncate` to `air_smgr.c` -> LFC (`lfc.c`) -> Unix domain socket `AIRP` RPCs (`communicator.c`).

---

## 4. Cell Agent Configuration Flow

- `internal/cell/agent.go` defines `Agent.SetPageserverConfig(enabled bool, socketPath string, lfcMaxMB int)`, `IsPageserverEnabled()`, and `HasAirSmgr()` (checks for `/usr/lib/postgresql/17/lib/air_smgr.so`).
- `Agent.WriteTenantConfigs(tenantID)` checks `a.pageserverEnabled && a.pageserverSocket != ""`. When true, it creates `<volDir>/lfc/<tenantID>`, adds `air_smgr` to `shared_preload_libraries`, and appends the five `air_smgr.*` GUCs to `postgresql.conf`.
- `internal/config/config.go` already parses `AIR_PAGESERVER_ENABLED` (`PageserverEnabled`), `AIR_PAGESERVER_SOCKET` (`PageserverSocket`, default `<DataDir>/pageserver.sock`), `AIR_PAGESERVER_GRPC_ADDR` (`PageserverGRPCAddr`), and `AIR_PAGESERVER_CACHE_MB` (`PageserverCacheMB`, default `256`).

---

## 5. Pageserver Integration Status & Missing Integration Points

1. **`cmd/air-server/main.go` does NOT call `ag.SetPageserverConfig(...)`:**
   - Even when `AIR_PAGESERVER_ENABLED=true` or `AIR_PAGESERVER_GRPC_ADDR` is configured, `cmd/air-server/main.go` never calls `ag.SetPageserverConfig(...)`. As a result, `Agent.WriteTenantConfigs()` always generates `shared_preload_libraries = 'supautils'` without `air_smgr`, and PostgreSQL always uses local `md.c`.
2. **In-process vs. Remote Pageserver Unix Socket Gap:**
   - `air_smgr.so` speaks **only** the `AIRP` binary protocol over a local Unix domain socket (`air_smgr.pageserver_socket`).
   - When `AIR_PAGESERVER_ENABLED=true` and `AIR_PAGESERVER_GRPC_ADDR == ""` (single-cell / co-located mode), `cmd/air-server/main.go` does not instantiate an in-process `pageserver.Pageserver` or start its `UnixServer` on `cfg.PageserverSocket`.
   - When `AIR_PAGESERVER_GRPC_ADDR != ""` (distributed topology like `docker-compose.yml` and `deploy/fly/`), `air-pageserver` runs in a separate container/VM, so `air-server` has no local `pageserver.sock` unless `air-server` runs a local `pageserver.NewUnixServer(cfg.PageserverSocket, grpcBridge)` forwarding `pageserver.Service` calls over gRPC to `air-pageserver`. All required RPCs (`GetPageAtLSN`, `GetPageBatch`, `GetRelSize`, `GetRelExists`, `GetDBSize`, `PutPageBatch`, `UnlinkRel`) already exist in `proto/pageserver.proto` and `internal/pageserver/grpc.go`.
3. **Pageserver Client Resilience & Startup Validation:**
   - `NewGRPCPageserverClient` in `internal/control/pageserver_client.go` uses `grpc.NewClient` (lazy dial) with no startup `Health` check, no retry policy on transient network failures, and no explicit error reporting if `AIR_PAGESERVER_ENABLED=true` is requested when `air_smgr.so` is missing or Pageserver is unreachable.
4. **Adaptive Controller CPU PSI Miscalibration (`internal/gateway/adaptive_controller.go:212`):**
   - `pressured` triggers on `tel.CPUPSISomeAvg10 >= 25.0`, causing false-positive memory throttling on healthy 2-vCPU workloads. Remains disabled by default (`AIR_WRITE_ADAPTIVE_ENABLED=false`).
5. **Docker / Deployment Configs:**
   - `Dockerfile` already builds patched PostgreSQL 17.12 + `supautils` + `air_smgr.so`.
   - `docker-compose.yml` and `deploy/fly/fly.server.toml` do not yet set `AIR_PAGESERVER_ENABLED=true` or `AIR_PAGESERVER_SOCKET`.

---

## 6. Existing Test Infrastructure

- **Unit & Integration Tests:**
  - `internal/cell/`: `cgroup_test.go`, `pageserver_config_test.go`, `telemetry_test.go`, `warm_pool_test.go`, `warm_pool_audit_test.go`.
  - `internal/gateway/`: `write_governor_test.go`, `write_governor_integration_test.go`, `buffer_budget_test.go`, `adaptive_controller_test.go`, `gateway_test.go`, `sql_router_phase15_test.go`.
  - `internal/control/`: `pageserver_integration_test.go`, `pageserver_grpc_test.go`, `phase11_crash_durability_test.go`, `phase12_lifecycle_test.go`, `durability_audit_test.go`, `cold_offload_test.go`.
  - `internal/pageserver/`: `pageserver_test.go`, `walredo_test.go`, `phase8_regression_test.go` through `phase19_p2_walredo_test.go`.
  - `tests/e2e/`: `e2e_test.go`, `four_service_mesh_e2e_test.go`, `gateway_e2e_test.go`, `pageserver_cau_test.go`.
- **Benchmarks:**
  - `benchmarks/phase20.9/` (`main.go`, `env.go`, `workloads.go`, `memory_fairness_correctness.go`).

---

## 7. Known Risks

1. **Bootstrap & WALReceiver Timing with `air_smgr`:** When `air_smgr` is active, `smgrcreate`/`smgrextend`/`smgrwritev` write pages directly to Pageserver via `pageserver.sock`, while `WALReceiver` also streams WAL to Pageserver. Consistency between direct `PutPageBatch` and WAL ingestion must be verified across restarts and crash recovery.
2. **Pageserver Unavailable / Restart During Active PG17:** `air_smgr`'s `communicator.c` retries once on I/O failure (`attempt < 2`). If Pageserver restarts or is temporarily unreachable, `air_smgr` returns `ERROR (ERRCODE_IO_ERROR)`. Recovery behavior after Pageserver restart must be tested end-to-end.
3. **2-vCPU Host Build & Resource Contention:** Building PostgreSQL 17.12 in Docker on 2 vCPUs will take several minutes; running multi-container soak tests on the same 2-vCPU / 7.8 GiB host requires careful cgroup isolation so the test harness does not starve the 512 MB Cell.
4. **Disk Space & WAL Retention:** Sustained 15-minute and 30-minute write soak tests generate significant WAL; `archive_command` / `ps_layers` disk usage must be monitored.

---

## 8. Exact Implementation Plan

1. **Phase 21.1 — PostgreSQL 17.12 Storage Hook Integration:**
   - Build the `Dockerfile` image (or a dedicated reproducible PG17+`air_smgr` image) and verify `pg17-smgr-hook.patch` and `air_smgr.so` compile cleanly.
   - Start real PG17 with `shared_preload_libraries = 'supautils,air_smgr'`, `air_smgr.enabled = on`, and a real `pageserver.sock` server.
   - Prove via Pageserver RPC logs / stats, LFC files, and database operations (initdb/CREATE/INSERT/UPDATE/DELETE/SELECT/restart/WAL recovery) that `air_smgr` hooks actually execute.
   - Write `docs/reports/phase21-smgr-integration.md`.
2. **Phase 21.2 — Cell Agent Pageserver Configuration Wiring:**
   - Update `cmd/air-server/main.go` to wire `ag.SetPageserverConfig(...)` from `cfg` (`PageserverEnabled`, `PageserverSocket`, `PageserverCacheMB`), validate `air_smgr.so` presence when enabled, start in-process Pageserver or a gRPC-backed `UnixServer` bridge when `PageserverGRPCAddr` is set, validate Pageserver health on startup with retry/timeout, and handle graceful shutdown.
   - Implement `pageserver.Service` methods + health/retry/reconnect on `GRPCPageserverClient` in `internal/control/pageserver_client.go`.
   - Add integration tests covering startup failure, Pageserver restart, transient failure, auth failure, invalid config, and recovery.
   - Write `docs/reports/phase21-pageserver-integration.md`.
3. **Phase 21.3 — Full AIR Storage Path Integration:**
   - Run the complete stack (`Client -> air-gateway -> air-server (Cell Agent) -> PG17.12+air_smgr -> air-pageserver -> storage`) in Docker/cgroup v2 and verify all DDL/DML, index creation, commit/rollback, PG restart, WAL recovery, and Pageserver restart.
   - Write `docs/reports/phase21-storage-path-validation.md`.
4. **Phase 21.4 — Durability & Crash Recovery:**
   - Test PG clean shutdown, PG `SIGKILL`, Cell Agent restart, Gateway restart, Pageserver restart, network interruption, and interrupted write workloads with `fsync=on`, `full_page_writes=on`, `synchronous_commit=on`.
   - Write `docs/reports/phase21-crash-recovery.md`.
5. **Phase 21.5 — Production Stability Soak Tests:**
   - Run 5-minute smoke, 15-minute sustained, and 30-minute sustained mixed-workload soak tests on a 512 MB Cell (Static Governor, `WriteMaxGlobal=16`, `WriteMaxPerTenant=8`, Adaptive disabled, Buffer Budget enabled, Lazy LSN enabled) over the full AIR storage path.
   - Write `docs/reports/phase21-soak-test.md`.
6. **Phase 21.6 — Multi-Tenant Stability:**
   - Test 4 real tenants (bulk INSERT+COPY, interactive INSERT/UPDATE, mixed R/W, intermittent) with and without Static Write Governor over the full AIR storage path.
   - Write `docs/reports/phase21-multitenant-stability.md`.
7. **Phase 21.7 — Resource Efficiency:**
   - Measure 256 MB, 512 MB, 1 GB, and 2 GB cgroup v2 profiles separately (separating PG anonymous memory from page cache and total cgroup memory).
   - Write `docs/reports/phase21-resource-efficiency.md`.
8. **Phase 21.8 — Adaptive Controller Follow-up:**
   - Decouple CPU PSI from memory-OOM pressure in `internal/gateway/adaptive_controller.go`, add unit tests, and benchmark against Static Governor (keeping Adaptive disabled by default unless proven superior).
9. **Phase 21.9 — Deployment Topology Validation & Final Summary:**
   - Validate the full deployment topology (`air-gateway`, `air-pageserver`, `air-read-engine`, `air-server`, R2/control-plane interactions where testable, and regression suite `go test -race ./internal/gateway/... ./internal/cell/...`).
   - Write `docs/reports/phase21-deployment-validation.md` and `docs/reports/phase21-final-summary.md`.
