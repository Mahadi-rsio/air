# AIR Phase 15 — Query Planner & Execution Design

## 1. Architectural Boundaries

Phase 15 preserves all core AIR invariants:

1. **PostgreSQL Primary Write Authority**: All writes, DDL, transactions with writes, `FOR UPDATE`/`SHARE`, and unsupported read queries continue to execute on PostgreSQL Primary.
2. **SQLite Strictly Read-Only**: The SQLite read engine enforces `SQLITE_OPEN_READONLY`, `PRAGMA query_only=ON`, and C-level `sqlite3_set_authorizer`.
3. **Zero-RPC Hot Path**: Query classification, plan caching, cost-based index selection, predicate/projection pushdown, and adaptive feedback all execute in-process in Go without external planner RPCs or extra background goroutines.
4. **Tenant Isolation & MVCC Correctness**: Every plan cache key, statistics entry, adaptive feedback entry, and `ReadSnapshot` is scoped by `tenantID` (and `generation` / `LSN`). Index scans validate PostgreSQL MVCC visibility and HOT-chain TIDs on heap pages.

## 2. End-to-End Query Execution Pipeline

```
Client (PG v3 Wire)
  │
  ▼
Go Gateway (internal/gateway/sql_router.go)
  ├─ ClassifySQLWithHint() -> RouteTargetReadEngine vs RouteTargetWriteEngine
  ├─ Transaction/Pinning/Causal-LSN/Health checks -> safe PG fallback if needed
  │
  ▼
SQLiteReadServer (internal/readadapter/sqlite_server.go)
  ├─ QueryPlanCache.GetOrTranslate() [16-shard bounded LRU + singleflight]
  │     Key: (tenantID, generation, catalogLSN, statsVersion, capabilityVer, normalizedQueryHash)
  ├─ Acquire ReadSnapshot (tenant-isolated MVCC snapshot)
  ├─ Refresh catalog & StatsCache (pg_class + pg_statistic) on LSN/catalog change
  │
  ▼
SQLite Engine + air_pg Virtual Table (internal/readadapter/sqlite_vtab.go)
  ├─ xBestIndex -> QueryOptimizer.ChooseBestPlan()
  │     ├─ Evaluates SeqScan vs candidate B-tree IndexScans
  │     ├─ Pushes EQ, NE, GT, GE, LT, LE, IS NULL, IS NOT NULL
  │     ├─ Evaluates composite prefix + ORDER BY compatibility
  │     └─ Passes colUsed bitmask for Projection Pushdown
  ├─ xFilter -> B-tree index scan or sequential heap scan (reusing filterCols slice)
  ├─ DecodeHeapColumnsProjected() -> decodes only needed columns
  ├─ xClose -> records AdaptiveTracker bounded EMA feedback
  │
  ▼
EXPLAIN / EXPLAIN ANALYZE (internal/readadapter/explain.go)
  └─ FormatAIRQueryPlan() -> emits AIR QUERY PLAN + SQLite plan lines
```

## 3. Component Summary

| Component | File | Purpose |
| :--- | :--- | :--- |
| PostgreSQL Stats Layer | `internal/readadapter/stats.go`, `pg_catalog.go` | Caches `pg_class` (`relpages`, `reltuples`, `relallvisible`) and `pg_statistic` (`nullfrac`, `avgwidth`, `distinct`) in a bounded 2048-entry tenant-isolated LRU `StatsCache`. |
| Cost-Based Optimizer | `internal/readadapter/optimizer.go` | `QueryOptimizer.ChooseBestPlan` scores `SeqScan` vs B-tree `IndexScan` / `IndexScanOrdered` using L1/L2/L3 cache probabilities, selectivity, tree depth, and `ORDER BY` elimination. |
| Virtual Table Pushdown | `internal/readadapter/sqlite_vtab.go` | Bridges SQLite `xBestIndex`/`xFilter`/`xClose` with `QueryOptimizer`, `colUsed` projection pushdown, `idxNum`-selected index plans, and `AdaptiveTracker` feedback. |
| Go-Native Plan Cache | `internal/readadapter/plan_cache.go` | 16-shard bounded LRU (`4096` entries) with `singleflight` stampede protection and automatic invalidation on `catalogLSN`, `statsVersion`, `capabilityVer`, or tenant invalidation. |
| Adaptive Feedback | `internal/readadapter/adaptive.go` | Bounded 1024-entry LRU `AdaptiveTracker` updating selectivity factor and L1/L2 hit ratios via EMA (`alpha=0.25`). |
| EXPLAIN Diagnostics | `internal/readadapter/explain.go` | `FormatAIRQueryPlan` formats `AIR QUERY PLAN` with route, strategy, index, cost, rows, cache tier, pushdowns, and plan cache status without leaking parameter values. |
| Gateway Integration | `internal/gateway/sql_router.go` | `ClassifySQLWithHint` and `RoutingMetrics.ExplainRouted` / `ReadFallbackRate()` provide zero-RPC routing hints and observability. |
