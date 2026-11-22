# AIR Phase 16H — Production Observability (`OBSERVABILITY_DESIGN.md`)

## 1. Objective

Provide comprehensive query, resource, and optimizer observability with minimal hot-path overhead and strictly bounded Prometheus label cardinality (no `tenant_id` labels in Prometheus).

---

## 2. Metrics Architecture

### 2.1 Query & Execution Metrics ([sqlite_server.go](file:///workspaces/air/internal/readadapter/sqlite_server.go))
- `QueriesTotal`, `QueriesSucceeded`, `QueriesFailed`, `QueriesCancelled`, `WritesBlocked`
- `LatencyP50Ms`, `LatencyP95Ms`, `LatencyP99Ms` (bounded 1024-sample ring buffer)
- `VTabPagesRead`, `VTabIndexPagesRead`, `VTabIndexScans`, `VTabTuplesDecoded`, `VTabColsDecoded`, `VTabRowsMatched`
- `PlanCache` (`Hits`, `Misses`, `Evictions`, `Invalidations`, `StampedesPrevented`)
- `TotalFallbacks` + categorized `FallbackReasons`

### 2.2 Resource & Scheduler Metrics ([memory_budget.go](file:///workspaces/air/internal/readadapter/memory_budget.go), [scheduler.go](file:///workspaces/air/internal/readadapter/scheduler.go))
- `Memory`: `GlobalUsedBytes`, `GlobalLimitBytes`, `SQLiteUsedBytes`, `SQLiteHighwater`, `Exhaustions`
- `Scheduler`: `ActiveGlobal`, `QueuedGlobal`, `MaxGlobalConcurrency`, `AdmittedTotal`, `BackpressureTotal`, `CancelledWaitTotal`
- `Goroutines`, `GoHeapAllocBytes` (computed only on scrape, never on query hot path)
- Per-tenant breakdown available via `PerTenantQueries` and `Scheduler.TenantStats(tenantID)` / `Memory.TenantUsed(tenantID)` in JSON, never as unbounded Prometheus labels.

### 2.3 Optimizer Metrics ([optimizer.go](file:///workspaces/air/internal/readadapter/optimizer.go))
- Zero-alloc `atomic.Uint64` counters for `PlansEvaluated`, `SeqScansChosen`, `IndexScansChosen`, `OrderedIndexScansChosen`, `ParallelScansChosen`, `PlanningTimeNsTotal`, `EstimatedRowsTotal`, `ActualRowsTotal`, `EstimatedCostMilliTotal`, `ActualExecNsTotal`, `AdaptiveAdjustments`.

---

## 3. Verification

- `TestPhase16H_ObservabilityMetricsAndLowCardinalityPrometheus` (verified with `-race`).
