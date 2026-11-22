# AIR Phase 16F — Query Cancellation & Resource Limits (`RESOURCE_LIMITS.md`)

## 1. Objective

Ensure queries can be cancelled safely at any point (client disconnect, execution deadline, PostgreSQL `CancelRequest`) without leaking goroutines, C memory, connections, or locks, and enforce bounded intermediate result limits.

---

## 2. Mechanisms Implemented

### 2.1 Zero-Goroutine SQLite Cancellation ([sqlite_cgo.go](file:///workspaces/air/internal/readadapter/sqlite_cgo.go))
- `SQLiteConn` allocates a single persistent `cancelFlag *C.int` on `OpenSQLiteConn` and frees it under `cancelMu` on `Close()`.
- `queryWithContextInternal` resets `cancelFlag`, installs `air_progress_cb`, and registers `context.AfterFunc(ctx, ...)` to atomically set `cancelFlag` and invoke `sqlite3_interrupt(c.db)` on cancellation/deadline.
- Eliminates per-query watcher goroutines, per-query `C.malloc`/`C.free`, and per-query channel allocations.

### 2.2 End-to-End `CancelRequest` Propagation ([pooler.go](file:///workspaces/air/internal/gateway/pooler.go), [sqlite_server.go](file:///workspaces/air/internal/readadapter/sqlite_server.go))
- `TenantPool.tryExecuteOnReadEngine` registers the active read-engine backend `(readHost, readSockDir, readPort, BackendPID, BackendSecret)` in `CancelRegistry` for the duration of the read query and unregisters on completion/fallback.
- `SQLiteReadServer` tracks active session `pid -> context.CancelFunc` in `sessions` and dispatches PostgreSQL `CancelRequest` (`80877102`) directly to `connCancel()`, interrupting the running SQLite query.

### 2.3 Result & Memory Limits ([memory_budget.go](file:///workspaces/air/internal/readadapter/memory_budget.go), [sqlite_cgo.go](file:///workspaces/air/internal/readadapter/sqlite_cgo.go))
- `QueryResourceLimits.MaxResultRows` (wired via `SQLiteServerConfig.MaxResultRows` and `ContextWithResourceLimits`) bounds materialized result rows, returning `ErrResultLimitExceeded` gracefully.
- `QueryMemoryBudget` (Phase 16B) bounds per-query, per-tenant, and global execution bytes, releasing all reservations via `defer budget.Release()` even on cancellation or error.

---

## 3. Verification

- `TestPhase16F_ContextCancellationAndCleanup`
- `TestPhase16F_MaxResultRowsLimit`
- `TestPhase16F_ConcurrentCancelAndCloseRace` (verified with `-race`)
