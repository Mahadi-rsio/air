# Phase 25 Report: Gateway, SQL Router Upgrade & Reliability

**Status**: `PASS`

## Summary

Phase 25 upgraded `internal/gateway` with an AST-based, capability-aware PostgreSQL SQL router and hardened admission context propagation, `CancelRequest` cancellation, ReadEngine circuit breaking, and pool shutdown cleanup.

## Implemented Changes

### 1. Capability-Aware SQL Router
- **`internal/gateway/capability_registry.go`**:
  - Added `CapabilityRegistry` with four explicit support states: `CapFullSupport` (`FULL_SUPPORT`), `CapPartialSupport` (`PARTIAL_SUPPORT`), `CapUnsupported` (`UNSUPPORTED`), and `CapUnknown` (`UNKNOWN`).
  - `CapabilitySupport.IsReadEngineEligible()` returns `true` only for `CapFullSupport`; `CapPartialSupport`, `CapUnsupported`, and `CapUnknown` always fall back to `RouteTargetWriteEngine` (`RouteReasonCapabilityFallback`).
  - Covers statements, clauses (`DISTINCT`, `DISTINCT_ON`, `FILTER`, `RECURSIVE_CTE`, `FOR_UPDATE`, `SELECT_INTO`, `MODIFYING_CTE`, `GROUPING_SETS`, `LATERAL`), joins (`INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`, `NATURAL`, `LATERAL`), window functions, aggregates, operators (`ILIKE`, regex `~`/`~*`/`!~`/`!~*`, JSON `->`/`->>`/`#>`/`#>>`, `@>`, `SIMILAR_TO`, `ANY`/`ALL`), scalar functions, data types, and casts.
- **`internal/gateway/sql_ast.go`**:
  - Added PostgreSQL SQL lexer (`LexPostgreSQL`) and structural AST parser (`ParseSQLAST`) handling comments (`--`, nested `/* ... */`), dollar-quoted strings (`$tag$...$tag$`), escape/bit/hex string literals (`E'...'`, `B'...'`, `X'...'`), double-quoted identifiers, positional parameters (`$1`), multi-character operators, `::type` and `CAST(... AS ...)` casts, CTEs (including column-list `name(cols) AS (...)`), subqueries, joins, window `OVER (...)`, `FILTER`, and row-locking clauses (`FOR UPDATE`/`FOR SHARE`).
- **`internal/gateway/sql_router.go`**:
  - Updated `ClassifySQLWithHint` (via `ClassifySQLWithRegistry`) to parse SQL via `ParseSQLAST` and evaluate all extracted features against `DefaultCapabilityRegistry()`.
  - Added `RouteReasonCapabilityFallback` and `RoutingMetrics.CapabilityFallbacks`.
  - Kept `ClassifyWriteRequest` classifying capability-fallback read queries as `WriteClassReadOnly` so read fallbacks do not consume write-governor slots.

### 2. Gateway Reliability & Context Propagation
- **`internal/gateway/pooler.go`**:
  - Replaced `context.Background()` in `acquireWritePermit` with per-query `context.Context` derived from `clientSessionState.beginQueryContext()` (itself derived from `sessionCtx` -> `TenantPool.poolCtx`).
  - Added `context.Context` propagation to `acquireSlot(ctx)`, `acquireBackend(ctx, clientID)`, `acquireReadBackend(ctx, clientID)`, `ensureBackend(ctx, st)`, `syncReadEngineForQuery(ctx, ...)`, `tryExecuteOnReadEngine(ctx, ...)`, and `dialAndStartupCtx(ctx, ...)`.
  - `acquireSlot(ctx)` uses `context.AfterFunc` to wake `semCond.Wait()` immediately upon cancellation.
  - Queued `WriteGovernor.Acquire` / `acquireSlot` cancellations emit PostgreSQL SQLSTATE `57014` (`query_canceled`).
  - Added a 3-state ReadEngine circuit breaker on `TenantPool` (`readConsecFails`, `readBreakerOpenUntil`, `readProbeInFlight`) with threshold `3` consecutive transport/session-init failures and a `5s` cooldown before allowing a single half-open probe; query-level `ErrorResponse` (`E`) does not trip the breaker.
  - Added `TenantPool.Close()` and updated `PoolManager.RemoveTenant` and `PoolManager.CloseAll` to cancel `poolCtx`, broadcast `semCond`, close idle/read-idle connections, and clear the pool map.
- **`internal/gateway/auth.go` & `internal/gateway/client_session.go`**:
  - Extended `CancelRegistry` with `RegisterSession`, `UnregisterSession`, and `CancelSession` so `forwardCancelRequest` immediately cancels queued in-process admission/query contexts in addition to forwarding `CancelRequest` to the backend.

## Verification

| Check | Command | Result |
|---|---|---|
| Phase 25 unit & race tests | `go test -v -race -run TestPhase25 ./internal/gateway/...` | `PASS` (6/6 tests, `1.083s`) |
| Gateway package short suite | `go test -short ./internal/gateway/...` | `PASS` (`5.700s`) |
