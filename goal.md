# AIR Phase 25–29: PostgreSQL Compatibility & Intelligent Read Engine Upgrade

## Mission

Upgrade AIR into a highly PostgreSQL-compatible serverless database platform that supports the majority of real-world PostgreSQL queries while maintaining AIR's ultra-lightweight architecture and low infrastructure cost.

- **Repository**: `https://github.com/Mahadi-rsio/air.git`
- **Working Directory**: `/workspaces/air`
- **Target Architecture**: `Client → AIR Go Gateway → Intelligent SQL Router`
  - **AIR SQLite Read Engine**: supported, validated, read-only PostgreSQL queries.
  - **PostgreSQL 17**: writes, transactions requiring full PostgreSQL semantics, unsupported queries, and compatibility fallback.
  - Both paths must respect AIR's snapshot, WAL LSN, MVCC, and consistency guarantees.

---

## Primary Objectives

1. Support the majority of commonly used PostgreSQL `SELECT` queries.
2. Preserve PostgreSQL-compatible query semantics and result types.
3. Automatically route unsupported queries to PostgreSQL.
4. Never return incorrect results just to increase read-engine routing percentage.
5. Keep SQLite as a strictly read-only execution engine.
6. Keep PostgreSQL as the write authority and full-compatibility fallback.
7. Maintain bounded memory and CPU usage under high tenant concurrency.
8. Avoid introducing unnecessary infrastructure or a completely new queue system.
9. Remain lightweight enough to run on small Fly.io Machines.

**Engineering Priority Order**:
1. PostgreSQL compatibility and correctness (first)
2. Query routing percentage (second)
3. Performance (third)

---

## Phase Breakdown & Plan

### Phase 25 — Gateway, SQL Router Upgrade & Reliability
- **Capability-Aware SQL Router (`internal/gateway/`)**:
  - Replace simple keyword/regex classification with a PostgreSQL SQL lexer and AST parser (`sql_ast.go`).
  - Analyze query structure, expressions, operators, data types, casts, functions, joins, subqueries, CTEs, and window functions.
  - Centralize feature support in a `CapabilityRegistry` (`capability_registry.go`) with states: `FULL_SUPPORT`, `PARTIAL_SUPPORT`, `UNSUPPORTED`, `UNKNOWN` (never treat `PARTIAL_SUPPORT` as `FULL_SUPPORT`).
  - Route complex JOINs, aggregations, CTEs, and window functions to SQLite when semantics are `FULL_SUPPORT`; safely fall back to PostgreSQL for `PARTIAL_SUPPORT`, `UNSUPPORTED`, `UNKNOWN`, stateful/session-dependent queries, or snapshot/LSN/resource-limit violations.
- **Gateway Reliability (`internal/gateway/`)**:
  - Propagate real session/query `context.Context` into `WriteGovernor.Acquire()`, `acquireSlot()`, `acquireBackend()`, and `acquireReadBackend()` (eliminating `context.Background()` in admission).
  - Support immediate cancellation of queued admission requests on client disconnect or `CancelRequest`.
  - Audit and harden ReadEngine circuit breakers, health checks, reconnect behavior, prepared statement recovery, graceful shutdown, and tenant cleanup without adding new generic queues or unbounded goroutines.

### Phase 26 — PostgreSQL Semantic Compatibility (`internal/readadapter/`)
- Replace unsafe regex-based `::type` stripping (`reTypeCast`) with AST-aware PostgreSQL-to-SQLite translation.
- Validate and support (or mark `PARTIAL_SUPPORT` / `UNSUPPORTED` for PG fallback):
  - **Data Types**: integer types, `BIGINT`, `NUMERIC`/`DECIMAL`, `REAL`/`DOUBLE PRECISION`, `TEXT`/`VARCHAR`, `BOOLEAN`, `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `INTERVAL`, `UUID`, `JSON`/`JSONB`, `ENUM`, `DOMAIN`, `BYTEA`.
  - **Expressions & Operators**: PostgreSQL casts, `NULL` handling, `IS NULL`/`IS NOT NULL`, `COALESCE`, `NULLIF`, `CASE`, `IN`/`NOT IN`, `BETWEEN`, `ANY`/`ALL`, comparison & arithmetic operators, string functions, pattern matching, `ILIKE`, regex operators, JSON operators, array operations.
  - **Query Features**: `SELECT`, `DISTINCT`, `DISTINCT ON`, `INNER`/`LEFT`/`RIGHT`/`FULL OUTER`/`CROSS` JOINs, multiple JOINs, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`/`OFFSET`, aggregates, subqueries, correlated subqueries, CTEs, recursive CTEs, `UNION`/`UNION ALL`, `INTERSECT`, `EXCEPT`, window functions, `FILTER` clauses, `LATERAL` joins.
  - **PG-Specific Behavior**: `NULL` sorting (`NULLS FIRST`/`LAST`), collation, numeric precision, timestamp/timezone semantics, implicit/explicit casts, parameter type inference, prepared statements, result column types/metadata, PG-compatible errors.

### Phase 27 — Read Engine Query Execution & Optimization (`internal/readadapter/`)
- Upgrade the existing SQLite read engine (`sqlite_vtab.go`, `optimizer.go`, `plan_cache.go`, `stats.go`, `adaptive.go`) without WASM:
  - Index selection and predicate pushdown (`xBestIndex`).
  - Covering index execution.
  - JOIN ordering and cardinality estimation.
  - `GROUP BY`, `ORDER BY`, subquery, CTE, and window-function optimization.
  - Prepared-statement and parameter-sensitive query planning.
  - Statistics, selectivity estimation, and cache-aware execution planning.

### Phase 28 — Bounded Memory & Streaming Results (`internal/readadapter/` & `internal/gateway/`)
- Refactor `SQLiteConn.queryWithContextInternal()`, `SQLiteResult.Rows`, `handleExtendedBatch()`, DataRow encoding, and gateway response buffers:
  - Bounded row batches and incremental PostgreSQL `DataRow` encoding.
  - Reusable buffers and backpressure-aware client writes.
  - Per-query, per-tenant, and global response memory limits.
  - Immediate cancellation on client disconnect and full resource cleanup.
  - **Critical Correctness Rule**: Never retry a query on PostgreSQL after sending partial result rows to the client; validate routing/capability before streaming starts, and emit a proper PG error if execution fails mid-stream.

### Phase 29 — Differential Testing, Workload Compatibility & Resource Validation
- **PostgreSQL 17.12 Differential Testing**: Automated comparison of rows, ordering, NULLs, types, numeric precision, timestamps, timezones, errors, prepared statements, and parameterized queries between live PG 17.12 and AIR SQLite Read Engine.
- **Real-World Workload Compatibility**: Representative SQL suites for SaaS dashboards, Next.js, Drizzle ORM, Prisma, SQLAlchemy, Django ORM, Rails ActiveRecord, Supabase/PostgREST, and analytics/reporting.
- **Resource & Performance Validation**: Cgroup v2 memory limits (`256 MB`, `512 MB`, `1 GB`) and tenant concurrency (`1`, `8`, `32`, `100`, `250` tenants) across mixed read/write, large JOINs, large results, slow clients, disconnects, concurrent DDL, cache misses, restarts, memory pressure, and noisy neighbors.

---

## Mandatory Constraints

- PostgreSQL remains the write authority; SQLite remains strictly read-only.
- No WASM; maintain Go + C++ architecture.
- No new generic queue system; reuse existing schedulers, governors, memory budgets, and caches.
- No unbounded buffering or goroutine creation.
- No cross-tenant data leakage; no weakening of MVCC or LSN consistency.
- Unsupported SQL must safely fall back to PostgreSQL.
- Keep `EnableCAWAL = false` by default unless existing validation requirements are satisfied.
- Report results honestly (`PASS`, `PASS WITH LIMITATIONS`, `FAIL`, `NOT VALIDATED`).

---

## Final Deliverables

- `docs/architecture/postgresql-compatibility.md`
- `docs/architecture/intelligent-query-router.md`
- `docs/architecture/read-engine.md`
- `docs/benchmarks/postgresql-compatibility.md`
- `docs/reports/phase25-gateway-reliability.md`
- `docs/reports/phase26-postgresql-compatibility.md`
- `docs/reports/phase27-read-engine-optimization.md`
- `docs/reports/phase28-streaming-memory.md`
- `docs/reports/phase29-production-validation.md`
