# AIR Phase 20 — Resource-Aware Write Governor Architecture

## 1. Overview

The **Resource-Aware Write Governor** is an adaptive admission-control, buffer-budgeting, and fair-scheduling layer inside `air-gateway` backed by cgroup v2 and Linux PSI resource telemetry from `internal/cell`. It controls how many write operations execute concurrently against PostgreSQL primaries based on actual memory, CPU, and I/O pressure rather than static connection counts.

PostgreSQL remains solely responsible for SQL execution, MVCC, transaction isolation, WAL generation, commit/rollback, crash recovery, lock management, and durability (`fsync` and `full_page_writes` are never weakened).

```
PostgreSQL Client
       |
       v
AIR Gateway (cmd/air-gateway)
       |
       v
Protocol Validation (ReadPGPacket, StartupMessage)
       |
       v
Request Classification (ClassifyWriteRequest / ClassifyExtendedWrite)
       |
       v
Global + Session Buffer Budget (BufferBudget / SessionBufferTracker)
       |
       v
Per-Tenant Fair Write Scheduler (Deficit Round Robin, Bounded Queue)
       |
       v
Adaptive Write Governor (AIMD + Hysteresis + Stale Fallback)
       |
       v
Existing Tenant Backend Pool (TenantPool, Transaction Pooling)
       |
       v
PostgreSQL Write Engine (WAL / Commit)

Cell Agent (internal/cell/telemetry.go)
Linux cgroup v2 + /proc/pressure/*
       |
       v
ResourceSnapshot (memory.current, memory.high, memory.max, memory.events, PSI)
       |
       v (Authenticated POST /internal/v1/telemetry)
Gateway TelemetryStore (Freshness TTL) -> AdaptiveController -> WriteGovernor
```

---

## 2. Component Responsibilities

| Component | File | Responsibility |
|---|---|---|
| Request Classifier | `internal/gateway/sql_router.go` | Lexical classification (`WriteClassReadOnly`, `WriteClassSimpleAutocommit`, `WriteClassExplicitTx`, `WriteClassBulkCopy`, `WriteClassDDLAdmin`, `WriteClassExtended`, `WriteClassUnknown`). Unknown statements conservatively require write admission. |
| Buffer Budget | `internal/gateway/buffer_budget.go` | Enforces global (`AIR_WRITE_BUFFER_GLOBAL_MAX`) and per-session (`AIR_WRITE_BUFFER_SESSION_MAX`) extended-query buffer caps with idempotent `ReleaseAll()` on batch completion, error, disconnect, and cancellation. |
| Bounded Write Scheduler | `internal/gateway/write_governor.go` | Enforces `AIR_WRITE_MAX_GLOBAL` and `AIR_WRITE_MAX_PER_TENANT` with bounded queues (`AIR_WRITE_QUEUE_MAX`, `AIR_WRITE_QUEUE_MAX_BYTES`), Deficit Round Robin (DRR) tenant fairness, context cancellation, and fast `53300` overload rejection. |
| Cell Telemetry | `internal/cell/telemetry.go` | Samples cgroup v2 (`memory.current`, `memory.high`, `memory.max`, `memory.events`, `cpu.stat`) and PSI (`memory`, `cpu`, `io`) with graceful degradation. |
| Adaptive Controller | `internal/gateway/adaptive_controller.go` | AIMD + hysteresis loop adjusting effective global concurrency in `[AIR_WRITE_MIN_GLOBAL, AIR_WRITE_MAX_GLOBAL]`. Falls back safely on missing or stale telemetry. |
| Lazy LSN Watermark | `internal/gateway/pooler.go` | Defers `pg_current_wal_lsn()` round-trips on pure write workloads while preserving Read-Your-Own-Write (RYOW) consistency by routing subsequent reads to primary when `sessionDirtyWrite` is set. |

---

## 3. Configuration Reference

| Environment Variable | Default | Description |
|---|---|---|
| `AIR_WRITE_GOVERNOR_ENABLED` | `true` | Master toggle for the Write Governor (`false` bypasses admission for rollback/baseline). |
| `AIR_WRITE_MAX_GLOBAL` | `32` | Maximum concurrent active write executions across all tenants. |
| `AIR_WRITE_MIN_GLOBAL` | `2` | Minimum global write concurrency floor under adaptive pressure. |
| `AIR_WRITE_MAX_PER_TENANT` | `8` | Maximum concurrent active write executions per tenant. |
| `AIR_WRITE_QUEUE_MAX` | `256` | Maximum queued write requests across all tenants before fast `53300` rejection. |
| `AIR_WRITE_QUEUE_MAX_BYTES` | `67108864` (64 MiB) | Maximum aggregate queued request bytes. |
| `AIR_WRITE_QUEUE_TIMEOUT` | `5s` | Maximum time a write request waits in the scheduler queue. |
| `AIR_WRITE_CONTROLLER_INTERVAL` | `1s` | Adaptive AIMD evaluation interval. |
| `AIR_WRITE_BUFFER_GLOBAL_MAX` | `67108864` (64 MiB) | Global extended-query protocol buffer budget across all sessions. |
| `AIR_WRITE_BUFFER_SESSION_MAX` | `8388608` (8 MiB) | Per-session extended-query protocol buffer limit. |
| `AIR_WRITE_ADAPTIVE_ENABLED` | `false` | Enables AIMD adaptive concurrency scaling using Cell telemetry. |
| `AIR_WRITE_LAZY_LSN_ENABLED` | `true` | Enables lazy LSN watermark tracking to eliminate post-write LSN round-trips. |

---

## 4. Metrics Reference (Low-Cardinality Prometheus)

Exposed via `GET /metrics` (authenticated) with zero high-cardinality tenant/SQL/user labels:
- `air_write_active`, `air_write_queue_depth`, `air_write_queue_bytes`
- `air_write_queue_wait_seconds{quantile="0.5|0.95|0.99"}`, `air_write_execution_seconds{quantile="0.5|0.95|0.99"}`
- `air_write_rejected_total`, `air_write_cancelled_total`, `air_write_timeout_total`, `air_write_errors_total`, `air_write_throughput`
- `air_write_limit_global`, `air_write_limit_tenant`, `air_write_controller_state`
- `air_gateway_buffered_bytes`, `air_gateway_buffer_budget_used`, `air_gateway_buffer_rejections_total`
- `air_cell_memory_current_bytes`, `air_cell_memory_high_bytes`, `air_cell_memory_max_bytes`, `air_cell_memory_pressure`, `air_cell_memory_oom_events`
- `air_transaction_open`, `air_transaction_duration_seconds{quantile="0.5|0.95|0.99"}`, `air_backend_active`, `air_backend_idle`

---

## 5. Rollback Instructions

To disable the Write Governor immediately without code changes:
```bash
export AIR_WRITE_GOVERNOR_ENABLED=false
export AIR_WRITE_ADAPTIVE_ENABLED=false
# or pass --no-write-governor --no-write-adaptive to air-gateway
```
This restores direct pass-through to `TenantPool.ensureBackend` while retaining buffer-budget safety.
