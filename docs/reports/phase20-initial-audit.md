# AIR Phase 20 — Initial Repository Audit: Resource-Aware Write Governor

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Scope:** Write execution path, connection pooling, extended-query buffering, routing, read-your-own-write (RYOW) consistency, cgroup v2 resource controls, and observability.

---

## 1. Existing Architecture & Write Execution Path

### 1.1 Client Connection & Startup
1. `cmd/air-gateway/main.go` accepts TCP connections on `AIR_GATEWAY_ADDR` (default `127.0.0.1:5432`).
2. `MetricsRegistry.TryAcquireGlobal()` (`internal/gateway/metrics.go`) enforces `MaxClientConns` (default `1024`).
3. Each accepted connection spawns a goroutine running `HandleClientConnection` (`internal/gateway/client_session.go`):
   - Handles `GSSENCRequest` (declined), `SSLRequest` (TLS upgrade via `TLSContext`), and `CancelRequest` (forwarded via `CancelRegistry` to the backend PostgreSQL process).
   - Parses `StartupMessage` (max `8192` bytes, PG v3.0), extracts `tenantID` from `database` or SNI (`ExtractTenant`), and enforces `reqUser == "app_user"` in `PoolMode`.
   - Enforces `MaxTenantClientConns` (default `128`) and `MaxUserClientConns` (default `0`, unlimited) via `MetricsRegistry.TryAcquireTenantUser`.
   - Resolves tenant routing via `TenantDiscovery.ResolveTenantNoWake` (in `PoolMode`) or `ResolveTenant` (direct mode).
   - In `PoolMode=true` (default), obtains or creates a `*TenantPool` via `PoolManager.GetOrCreate` and invokes `TenantPool.ServeClient`.

### 1.2 How a Client Write Reaches PostgreSQL
Inside `TenantPool.ServeClient` (`internal/gateway/pooler.go`):
- **Authentication:** `authenticateClient` performs cleartext password verification against `expectedPass`, sends `ParamStatus` (`server_version=17.12`), `BackendKeyData` (gateway-issued `gwPID`, `gwSecret`), and `ReadyForQuery('I')`.
- **Simple Query (`'Q'`):** `handleSimpleQuery` reads the SQL string, checks `IsSessionPinningSQL`, handles `SET`/`RESET`/`PREPARE`/`DEALLOCATE`/`EXECUTE` state tracking, and evaluates `RouteQuery` if a ReadEngine is configured and the session is unpinned/idle. If routed to the WriteEngine (primary), it calls `executeSimpleAndRelay`, which calls `ensureBackend`, writes the `'Q'` packet to `st.currentBackend.Conn`, and streams responses back via `relayUntilReady`.
- **Extended Query (`'P'`, `'B'`, `'D'`, `'E'`, `'C'` + `'S'`/`'H'`):** Pipeline messages are buffered into a per-session `extBatch []extMsg` slice until `Sync ('S')` or `Flush ('H')` arrives. `handleExtendedBatch` checks routing eligibility across all messages in the batch; if any statement is a write, DDL, session-pinning, or in an active transaction, it acquires the primary backend via `ensureBackend`, rewrites statement names to globally unique `_air_s_<clientID>_<seq>` identifiers, writes each message to `targetB.Conn`, and relays responses via `relayUntilReady` (for `'S'`) or `relayFlushBatch` (for `'H'`).
- **COPY FROM STDIN:** When PostgreSQL responds with `CopyInResponse ('G')` inside `relayUntilReady` or `relayFlushBatch`, `relayCopyIn` streams `'d'` (`CopyData`), `'c'` (`CopyDone`), and `'f'` (`CopyFail`) packets directly between `client` and `b.Conn` without buffering the full dataset in memory.

### 1.3 Backend Connection Acquisition & Release
- `ensureBackend(st)` (`internal/gateway/pooler.go`) reuses `st.currentBackend` if already attached (active transaction or session-pinned).
- Otherwise `acquireBackend(st.clientID)`:
  1. Calls `acquireSlot()` which blocks on `semMu`/`semCond` (`sync.Cond`) for up to **10 seconds** until `activeBackends < maxBackends` (default `16` per tenant).
  2. Pops LIFO from `idlePool`, evicting connections older than `idleTimeoutSec` (default `300s`).
  3. Runs `DISCARD ALL;` if `b.LastClientID != clientID`, or an empty-query `""` liveness ping if `b.LastClientID == clientID`.
  4. If no idle connection is available, dials primary via `dialAndStartup` (waking a suspended tenant via `wakeFn()` if necessary).
- `ensureSessionAndStmts(b, st)` replays `st.sessionVars` and missing `st.clientStmts` onto the backend.
- `releaseCurrent(st, dirty)` unregisters the cancel target and returns the backend to `idlePool` (or closes it if `dirty`), calling `releaseSlot()`.

### 1.4 Transaction-Pooling Mode
- `st.txStatus` tracks the transaction status byte from PostgreSQL's `ReadyForQuery` (`'I'` idle, `'T'` in-transaction, `'E'` failed-transaction).
- `updateTxCount(st)` maintains `TenantPool.activeTxCount` (`atomic.Int64`).
- At the end of `handleSimpleQuery` or `handleExtendedBatch` (on `'S'`), if `st.txStatus == 'I' && !st.sessionPinned`, `releaseCurrent(st, false)` returns the backend to the pool.
- Active transactions (`txStatus != 'I'`) and pinned sessions (`LISTEN`, `UNLISTEN`, `CREATE TEMP TABLE`, `pg_advisory_lock`) keep `st.currentBackend` pinned to `st` until `txStatus == 'I'` or disconnect.

### 1.5 Write Routing & Read-Your-Own-Write (RYOW) Consistency
- `internal/gateway/sql_router.go` implements a lexical SQL tokenizer (`tokenizeStatements`) handling `--` comments, nested `/* ... */` comments, `'...'` / `E'...'` / `$tag$...$tag$` literals, double-quoted identifiers, and `;` statement separators.
- `ClassifySQLWithHint` / `ClassifySQLForRouting` classifies statements conservatively: unknown heads default to `RouteTargetWriteEngine` (`RouteReasonWriteDDL`).
- **RYOW Watermark:** After every successful autocommit write on primary when `readPort > 0` (`pooler.go:809-814`, `1055-1059`), the gateway executes `SELECT pg_current_wal_lsn()::text` on `currentBackend` and updates `st.sessionWriteLSN`. Subsequent reads check `readEngineLSN >= sessionWriteLSN`, refreshing `readEngineLSN` from ReadEngine if behind, and falling back to WriteEngine on causal lag.

### 1.6 Cell Agent & Cgroup v2 Controls
- `internal/cell/agent.go` manages per-tenant PostgreSQL 17 instances (`ClaimCluster`, `ActivateCluster`, `StartOrAdoptTenant`, `StopTenantClean`, `CheckTenantDrainSafety`).
- `setupTenantCgroupV2` creates `<cgroupRoot>/air_tenant_<tenantID>` with:
  - `memory.high` (`512 MiB` default)
  - `memory.max` (`1 GiB` default)
  - `memory.swap.max` (`0`)
  - `cpu.max` (`100000 100000`)
  - `pids.max` (`256`)
- Currently, **no cgroup v2 telemetry** (`memory.current`, `memory.events`, PSI `memory.pressure`/`cpu.pressure`/`io.pressure`) is collected or exposed to the gateway.

---

## 2. Relevant Source Files & Functions

| Component | File | Key Symbols |
|---|---|---|
| Wire Protocol | `internal/gateway/protocol.go` | `ReadPGPacket`, `BuildPGMessage`, `RewriteParseBody`, `RewriteBindBody`, `RewriteNamedOpBody` |
| Client Session | `internal/gateway/client_session.go` | `HandleClientConnection`, `forwardCancelRequest` |
| Pooler & Write Path | `internal/gateway/pooler.go` | `PoolManager`, `TenantPool`, `ServeClient`, `handleSimpleQuery`, `handleExtendedBatch`, `ensureBackend`, `acquireBackend`, `releaseBackend`, `relayUntilReady`, `relayFlushBatch`, `relayCopyIn` |
| SQL Classification | `internal/gateway/sql_router.go` | `tokenizeStatements`, `ClassifySQLWithHint`, `ClassifySQLForRouting`, `RouteQuery` |
| SQL Inspection | `internal/gateway/sql_inspect.go` | `IsSessionPinningSQL`, `reSetStmt`, `rePrepareStmt`, `reExecuteStmt`, `reDeallocStmt` |
| Configuration | `internal/gateway/config.go` | `Config`, `DefaultConfig`, `LoadConfig` |
| Metrics | `internal/gateway/metrics.go` | `MetricsRegistry`, `QueryStats`, `LatencyHistogram`, `ToJSON`, `ToPrometheus` |
| Admin HTTP / gRPC | `internal/gateway/admin_http.go`, `admin_grpc.go` | `AdminHTTPServer`, `AdminGRPCServer`, `SetTenantLimits`, `DrainTenant` |
| Cell Agent & Cgroups | `internal/cell/agent.go`, `internal/cell/process.go` | `Agent`, `setupTenantCgroupV2`, `killAndRemoveTenantCgroupV2`, `CheckTenantDrainSafety` |

---

## 3. Current Limitations & Correctness/Resource Risks

1. **Unbounded Aggregate Protocol Buffering:** `ServeClient` caps `extBatch` per-session at `4096` messages / `32 MiB`, but there is **no global gateway buffer budget**. With `MaxClientConns = 1024`, concurrent sessions can allocate gigabytes of heap before any rejection occurs. Additionally, `extBatch` slice capacity is retained (`(*extBatch)[:0]`) after large batches.
2. **No Write Admission Control or Concurrency Separation:** `acquireSlot()` conflates backend connections with active write executions. There is no global write concurrency limit, no per-tenant write concurrency limit, no bounded write queue, and no fair scheduling across tenants (a single tenant with 128 client connections can saturate shared host RAM/IO).
3. **No Resource-Pressure Feedback Loop:** The Cell Agent enforces static cgroup v2 limits (`memory.high = 512 MiB`, `memory.max = 1 GiB`) but does not sample `memory.current`, `memory.events`, or Linux PSI (`memory.pressure`, `cpu.pressure`, `io.pressure`), nor does the Gateway adapt write concurrency when memory reclaim or IO pressure rises.
4. **High-Cardinality Prometheus Labels:** `MetricsRegistry.ToPrometheus` emits `tenant` and `user` labels on per-query metrics; new Phase 20 write-governor metrics must avoid unbounded tenant/user/SQL labels.
5. **Extra Post-Write LSN Round-Trip:** When `ReadEnginePort > 0`, every autocommit write performs a synchronous `SELECT pg_current_wal_lsn()::text` query on the backend before releasing it, adding latency to write-heavy sessions that may never issue reads.
6. **Per-Packet Allocations in Hot Path:** `ReadPGPacket` and `BuildPGMessage` allocate fresh `[]byte` slices on every wire packet.

---

## 4. Proposed Minimal Implementation Changes (Incremental Plan)

1. **Phase 20.2 — Global + Per-Session Buffer Budget:** Introduce a lightweight `BufferBudget` (atomic global byte counter + configurable `AIR_WRITE_BUFFER_GLOBAL_MAX` and `AIR_WRITE_BUFFER_SESSION_MAX`) with reserve-before-append and guaranteed release on batch completion, protocol error, client disconnect, and cancellation. Shrink oversized `extBatch` backing arrays after large batches.
2. **Phase 20.3 & 20.4 — Request Classification, Bounded Write Scheduler & Resource Separation:**
   - Extend `internal/gateway/sql_router.go` with `ClassifyWriteRequest` (`WriteClassSimple`, `WriteClassExplicitTx`, `WriteClassReadOnly`, `WriteClassBulkCopy`, `WriteClassDDLAdmin`, `WriteClassExtended`, `WriteClassUnknown`) using the existing tokenizer + protocol type.
   - Add `WriteGovernor` + `WriteScheduler` in `internal/gateway/` with:
     - Global active write limit (`AIR_WRITE_MAX_GLOBAL`), per-tenant active write limit (`AIR_WRITE_MAX_PER_TENANT`), separate open-transaction tracking, and backend pool limits.
     - Bounded per-tenant queues + global queue byte/count caps (`AIR_WRITE_QUEUE_MAX`, `AIR_WRITE_QUEUE_MAX_BYTES`, `AIR_WRITE_QUEUE_TIMEOUT`) with Deficit Round Robin (DRR) / round-robin tenant fairness, context-aware cancellation, and fast overload rejection (`53300` / `57P03`).
     - Preservation of session ordering, transaction pinning (explicit transactions are never split across backends or re-queued mid-tx), and prepared statements.
3. **Phase 20.5 — Cell Resource Telemetry:** Add a lightweight cgroup v2 + `/proc/pressure` snapshot collector in `internal/cell/telemetry.go` (`memory.current`, `memory.high`, `memory.max`, `memory.events`, PSI `memory`/`cpu`/`io`, `cpu.stat`) with timestamp/freshness tracking and graceful degradation when cgroups/PSI are unavailable. Expose snapshot ingestion to the Gateway via internal authenticated HTTP/gRPC or local collector.
4. **Phase 20.6 — Adaptive Write Governor (AIMD):** Implement an AIMD controller with hysteresis (`AIR_WRITE_ADAPTIVE_ENABLED`, `AIR_WRITE_MIN_GLOBAL`, `AIR_WRITE_MAX_GLOBAL`, `AIR_WRITE_CONTROLLER_INTERVAL`) that scales global write concurrency down multiplicatively on sustained memory/CPU/IO pressure or `memory.high`/OOM events, scales up additively when healthy, and safely falls back to conservative static limits when telemetry is missing or stale.
5. **Phase 20.7 — Safe Write-Path Optimizations:** Evaluate lazy/deferred LSN watermark acquisition and reduced allocations in `relayUntilReady` / `BuildPGMessage` with benchmarks before and after.
6. **Phase 20.8 — Benchmarks, Correctness Tests & Final Deliverables:** Dedicated Phase 20 benchmark suite (`benchmarks/phase20/`), comprehensive failure/correctness tests, `docs/architecture/WRITE_GOVERNOR.md`, `docs/reports/PHASE20_BENCHMARK_REPORT.md`, and `docs/reports/PHASE20_CORRECTNESS_REPORT.md`.
