# AIR Phase 20.9 — Initial Repository Audit

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Commit:** `28cc6657` (`feat(gateway,cell): implement Phase 20 Resource-Aware Write Governor`)  
**Host Environment:** Linux `6.8.0-1070-azure` x86_64, 2 vCPU, 7.8 GiB RAM, 0 swap, 4.1 GiB free disk on `/workspaces`, Go `1.27.3`, PostgreSQL `17.12` (`/usr/lib/postgresql/17/bin`), cgroup v2 mounted at `/sys/fs/cgroup` with full `memory`/`cpu`/`io`/`pids` controllers and PSI (`/proc/pressure/{memory,cpu,io}`), `sudo` available, Docker available.

---

## 1. Existing Architecture

AIR consists of four Go services plus per-tenant PostgreSQL 17 instances:

1. **`air-gateway` (`cmd/air-gateway/main.go`, `internal/gateway/`)**:
   - PostgreSQL wire-protocol v3.0 proxy and transaction-mode connection pooler (`PoolMode=true` default).
   - Listens on `AIR_GATEWAY_ADDR` (default `127.0.0.1:5432`) for SQL clients and `AIR_GATEWAY_ADMIN_ADDR` (default `127.0.0.1:8081`) for HTTP health, Prometheus `/metrics`, `/internal/v1/write-governor`, `/internal/v1/telemetry`, and tenant management.
   - Discovers tenants via Control Plane gRPC (`AIR_DB_GRPC_ADDR`) or local metadata DB, and can also register routes via `POST /v1/namespaces`.
   - Routes read-only queries to `air-read-engine` (`AIR_READ_ENGINE_HOST:AIR_READ_ENGINE_PORT`) when configured and causally consistent, and routes writes/transactions/DDL/COPY to the tenant's PostgreSQL 17 primary.
2. **`air-server` (`cmd/air-server/main.go`, `internal/control/`, `internal/cell/`)**:
   - Hosts the Control Plane (HTTP `:8080`, gRPC `:9090`) and the Cell Agent (`internal/cell/agent.go`).
   - Cell Agent provisions per-tenant PostgreSQL 17 clusters (`ClaimCluster`, `ActivateCluster`, `StartOrAdoptTenant`), manages `flock` ownership, configures `postgresql.conf`/`pg_hba.conf`, and attaches each tenant's postmaster process tree to `<cgroupRoot>/air_tenant_<tenantID>`.
3. **`air-pageserver` (`cmd/air-pageserver/main.go`, `internal/pageserver/`)**:
   - Multi-tenant page/WAL storage service exposing gRPC and an optional Unix domain socket (`--socket-path`) for the `air_smgr` binary wire protocol.
4. **`air-read-engine` (`cmd/air-read-engine/main.go`, `internal/readadapter/`)**:
   - SQLite-backed read replica adapter serving PG wire protocol on port `5433` from Pageserver pages.

---

## 2. Current Write Execution Path (Phase 20 Implementation)

Verified from `internal/gateway/pooler.go`, `write_governor.go`, `buffer_budget.go`, `adaptive_controller.go`, `sql_router.go`, and `config.go`:

1. **Client Framing (`TenantPool.ServeClient`)**:
   - Simple Query (`'Q'`): dispatched directly to `handleSimpleQuery`.
   - Extended Query (`'P'`, `'B'`, `'D'`, `'E'`, `'C'`): buffered per session in `extBatch []extMsg` until `Sync ('S')` or `Flush ('H')`. Before appending, `st.bufTracker.Reserve(5 + len(mbody))` enforces `AIR_WRITE_BUFFER_SESSION_MAX` (default 8 MiB, 4096 msgs) and `AIR_WRITE_BUFFER_GLOBAL_MAX` (default 64 MiB). `resetExtBatch()` calls `st.bufTracker.ReleaseAll()` and drops backing arrays with `cap > 256`.
2. **Classification (`internal/gateway/sql_router.go`)**:
   - `ClassifyWriteRequest(sql, inTx)` and `ClassifyExtendedWrite(sqls, inTx)` use the lexical tokenizer (`tokenizeStatements`) to classify into `WriteClassReadOnly`, `WriteClassSimpleAutocommit`, `WriteClassExplicitTx`, `WriteClassBulkCopy`, `WriteClassDDLAdmin`, `WriteClassExtended`, or `WriteClassUnknown`.
   - Unknown statements conservatively return `NeedsWriteAdmission() == true`.
3. **Write Governor Admission (`internal/gateway/write_governor.go`, `pooler.go:443-463`)**:
   - `acquireWritePermit` checks `if st.currentBackend != nil { return nil, true, true }`, then calls `gov.Acquire(context.Background(), p.tenantID, int64(bytesIn))`.
   - Fast path grants immediately if `activeWrites < effectiveGlobalLimit && t.active < tLimit && len(t.waiters) == 0`.
   - Contended path enqueues into a per-tenant FIFO waiter list bounded by `WriteQueueMax` (default 256) and `WriteQueueMaxBytes` (default 64 MiB), with `WriteQueueTimeout` (default 5s). On overload, returns SQLSTATE `53300`.
   - On `permit.Release()`, `dispatchLocked()` wakes waiters using Deficit Round Robin (DRR) across `activeTenants`.
4. **Backend Acquisition (`pooler.go:1430-1512`)**:
   - `acquireBackend` enforces `PoolMaxBackends` (default 16/tenant) via `sync.Cond` with a 10s timeout.
   - **Notable hot-path overhead found in audit:** When reusing an idle backend, `acquireBackend` sends `DISCARD ALL;` if `b.LastClientID != clientID`, or an empty query `""` liveness ping if `b.LastClientID == clientID` (`pooler.go:1455-1465`). Thus every autocommit write incurs **at least 2 round-trips** to PostgreSQL even on a warm connection.
5. **Streaming Relay & COPY (`pooler.go:1733-1780`)**:
   - `relayUntilReady` relays packets from PostgreSQL to client. When `CopyInResponse ('G')` is received, `relayCopyIn` streams `'d'`/`'c'`/`'f'` packets directly from client to backend without buffering the full upload in Gateway memory.
6. **Lazy LSN Watermark (`pooler.go:834-914`, `1158-1172`)**:
   - Active only when `readPort > 0`. With `lazyLSN=true` (default), successful autocommit/commit writes set `st.sessionDirtyWrite = true` instead of executing `SELECT pg_current_wal_lsn()::text`.
   - On a subsequent read, `sessionDirtyWrite=true` forces `sessionWriteLSN = ^uint64(0)`, routing the read to primary (preserving RYOW) and then issuing `SELECT pg_current_wal_lsn()::text` once on that primary backend to clear `sessionDirtyWrite` and record the LSN.

---

## 3. PostgreSQL 17 Startup & `air_smgr` Architecture

- **Host PostgreSQL 17.12:** Stock Ubuntu PGDG PostgreSQL `17.12` is installed at `/usr/lib/postgresql/17/bin` alongside `/usr/lib/postgresql/17/lib/supautils.so`.
- **Cell Agent `postgresql.conf` defaults (`internal/cell/agent.go:490-507`):**
  - `max_connections = 100`, `max_prepared_transactions = 16`, `shared_buffers = 32MB`, `work_mem = 4MB`, `maintenance_work_mem = 16MB`, `temp_file_limit = 32MB`, `wal_level = replica`, `min_wal_size = 32MB`, `summarize_wal = on`, `archive_mode = on`, `archive_command = 'cp %p <walSpoolDir>/%f'`, `shared_preload_libraries = 'supautils'` (plus `air_smgr` if `SetPageserverConfig` is enabled).
  - Durability settings (`fsync`, `synchronous_commit`, `full_page_writes`) remain at PostgreSQL 17 defaults (`on`).
- **`air_smgr` Loading & Blocker Audit:**
  - `extensions/air_smgr/` requires PostgreSQL 17 patched with `patches/pg17-smgr-hook.patch` (adds `smgr_hook`).
  - The host `/usr/lib/postgresql/17/` binary is unpatched stock PGDG PG17 and does **not** have `air_smgr.so`.
  - The pre-built Docker image `air-air-server:latest` **does** contain patched PG17 and `/usr/lib/postgresql/17/lib/air_smgr.so`.
  - **Code gap discovered:** `Agent.SetPageserverConfig(enabled, socketPath, lfcMaxMB)` in `internal/cell/agent.go:107` is **never called** by `cmd/air-server/main.go` (only in `internal/cell/pageserver_config_test.go`). Even in `docker-compose.yml`, `air-server` starts PostgreSQL with standard local storage (`md.c`) while `air-pageserver` runs as a separate container over gRPC without a shared Unix socket.
  - Therefore, Phase 20.9 real PostgreSQL benchmarks on the host will run real PostgreSQL 17.12 via `cell.Agent` + `air-gateway` with standard local storage (`md.c`) and full cgroup v2 enforcement, and we explicitly document the `air_smgr` host/wiring blocker rather than silently mocking it.

---

## 4. Telemetry & Cross-Machine Communication Audit

- **Gateway side (`internal/gateway/admin_http.go:437-455`):** Exposes authenticated `POST /internal/v1/telemetry` which accepts `CellTelemetry` JSON and updates `TelemetryStore` (15s TTL). This works over HTTP/TCP across separate machines.
- **Cell Agent side (`internal/cell/telemetry.go:173-192`):** Implements `CollectTenantTelemetry(tenantID)` and `CollectCellTelemetry()` reading cgroup v2 (`memory.current`, `memory.high`, `memory.max`, `memory.events`, `cpu.stat`, `memory.pressure`, `cpu.pressure`, `io.pressure`) with `/proc/pressure/*` fallback.
- **CRITICAL GAP DISCOVERED:** Nothing in `cmd/air-server/main.go`, `internal/cell/`, or `internal/control/` currently runs a background loop to call `CollectCellTelemetry()` / `CollectTenantTelemetry()` and POST snapshots to `air-gateway`'s `/internal/v1/telemetry`!
  - As a result, if `AIR_WRITE_ADAPTIVE_ENABLED=true` is enabled in production today, `AdaptiveController` will immediately enter `ControllerStateStaleFallback` (clamping `effectiveGlobalLimit` to `maxGlobal / 2`) because no telemetry is ever pushed to the Gateway.
  - **Action required for Phase 20.9:** Implement a lightweight telemetry push loop (or Cell-to-Gateway publisher) so real cgroup v2 + PSI telemetry flows from the Cell Agent to the Gateway over HTTP (supporting both localhost and cross-machine deployments).

---

## 5. Critical Code Findings & Risks Identified Before Benchmarking

1. **Explicit Transactions Release Write Permits Prematurely (`pooler.go:444`, `894-896`):**
   - `acquireWritePermit` returns immediately without acquiring a slot if `st.currentBackend != nil`.
   - For `BEGIN; INSERT ...; UPDATE ...; COMMIT;`, `BEGIN` acquires a write permit and **immediately releases it** when `BEGIN` returns `ReadyForQuery('T')`. Subsequent DML statements inside the transaction see `st.currentBackend != nil` and bypass `WriteGovernor` completely.
   - Impact: Explicit transaction workloads (including default `pgbench` TPC-B-like transactions) are governed only at `BEGIN` admission, not across the duration of their write execution; concurrency during active transactions is bounded only by `PoolMaxBackends` (default 16).
2. **`acquireWritePermit` Uses `context.Background()` (`pooler.go:451`):**
   - Client disconnects while queued in `WriteGovernor.Acquire` do not cancel the context until `WriteQueueTimeout` (5s) expires.
3. **Warm Backend Reuse Extra Round-Trip (`pooler.go:1455-1465`):**
   - Same-client reuse executes `execSimpleBackend(b, "")` and cross-client reuse executes `DISCARD ALL;` on every autocommit statement, doubling round-trips on the write hot path.
4. **Lazy LSN `wlsn > st.sessionWriteLSN` Edge Case (`pooler.go:909`, `1167`):**
   - If a session performs a no-op write (e.g., `ROLLBACK` or `UPDATE` affecting 0 rows) that does not advance WAL LSN, `wlsn == st.sessionWriteLSN` causes `sessionDirtyWrite` to remain `true` until a WAL-generating write occurs. Safe for RYOW (conservative), but may cause unnecessary primary fallback.
5. **Existing Phase 20 Benchmark Limitations:**
   - All prior Phase 20 numbers (`16K ops/s`, `-21.3% p95`, `+1 alloc/op`) came exclusively from `startMockPGServer` (`internal/gateway/write_governor_integration_test.go`) with a synthetic `50 µs` `time.Sleep`, `net.Pipe()`, no SQL execution, no WAL/fsync, no PostgreSQL memory/processes, and no cgroup v2 limits.
   - `benchmarks/phase20/` does not exist yet.
6. **Environment Constraints to Respect:**
   - 2 vCPUs, 7.8 GiB RAM, 4.1 GiB free disk on `/workspaces` (must clean up WAL/temp data between runs and keep WAL archive/pgdata compact).
   - Existing unit tests (`go test -race ./internal/gateway -run "TestWrite|TestBuffer|TestAdaptive|TestPhase20"` and `./internal/cell -run "TestCollectTenantTelemetry|TestCgroup"`) pass cleanly at commit `28cc6657`.

---

## 6. Stage 2+ Plan

1. **Stage 2:** Build a reproducible real PostgreSQL 17.12 + `cell.Agent` + `air-gateway` + cgroup v2 benchmark harness under `benchmarks/phase20.9/`, including a real Cell-to-Gateway cgroup v2/PSI telemetry publisher.
2. **Stage 3–5:** Run the concurrency sweep (`1, 2, 4, 8, 16, 32` clients) and workload suite (INSERT, UPDATE, UPSERT, DELETE, COPY FROM STDIN, explicit transactions) across all 5 configurations (No Governor, Static Governor, Adaptive Governor, Governor+BufferBudget+LazyLSN, Full Phase 20).
3. **Stage 6:** Execute dedicated **512 MB cgroup v2** (`memory.high` / `memory.max`) memory-pressure validation (plus 256 MB and 1 GB comparison profiles) collecting full cgroup v2 + PSI + PG + Gateway metrics.
4. **Stage 7:** Run 4-tenant fairness (`Tenant A` bulk, `Tenant B` interactive, `Tenant C` mixed R/W, `Tenant D` intermittent) and streaming `COPY FROM STDIN` tests.
5. **Stage 8:** Run PostgreSQL correctness, durability, crash recovery, Lazy LSN RYOW validation, and sustained-load stability tests.
6. **Stage 9–10:** Apply evidence-based optimizations only if warranted by real measurements, and deliver all required Phase 20.9 reports.
