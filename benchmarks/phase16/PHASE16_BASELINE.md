# AIR Phase 16 — Baseline Measurements (`PHASE16_BASELINE.md`)

## 1. Environment & Hardware

- **CPU**: AMD EPYC 7763 64-Core Processor (2 vCPUs exposed)
- **Memory**: 7.8 GiB RAM (`Mem: 7.8Gi`), 0 B swap
- **OS**: Linux (`6.8.0-1070-azure`, `amd64`)
- **Go**: `go1.27.3 linux/amd64`
- **PostgreSQL**: PostgreSQL `17.12` (verified live via `TestPhase15_LivePG17_EndToEnd`)
- **SQLite**: CGO `-lsqlite3`

---

## 2. Phase 15 Verified Baseline (Pre-Phase 16)

### 2.1 Functional & Race Verification
- `go test ./internal/gateway/... ./internal/readadapter/... -run TestPhase15 -v`: **PASS** (`TestPhase15_ClassifySQLWithHint_AndMetrics`, `TestPhase15_StatsCache_BoundedLRUAndTenantIsolation`, `TestPhase15_QueryOptimizer_CostSelectionAndCacheTiers`, `TestPhase15_QueryPlanCache_InvalidationIsolationAndStampede`, `TestPhase15_AdaptiveTracker_BoundedEMA`, `TestPhase15_ExplainDiagnostics_FormatAndNoSecrets`, `TestPhase15_LivePG17_EndToEnd` in 8.97s).

### 2.2 Benchmark Baseline (`go test ./internal/readadapter -run '^$' -bench 'BenchmarkPhase15|BenchmarkPhase14A_BTreePointLookupVsSeqScan|BenchmarkReadEngineL1HotPage' -benchmem -count=1`)

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkReadEngineL1HotPage-2` | 2,861 | 8,208 | 2 | L1 RAM LRU page hit (8 KiB) |
| `BenchmarkPhase14A_BTreePointLookupVsSeqScan/BTreeIndexPushdown_5000rows_100pages-2` | 56,387 | 37,524 | 77 | B-tree index scan + MVCC/HOT check |
| `BenchmarkPhase14A_BTreePointLookupVsSeqScan/SequentialHeapScan_5000rows_100pages-2` | 853,207 | 830,660 | 178 | Full sequential heap scan (~15.1x slower than B-tree) |
| `BenchmarkPhase15_Optimizer_ChooseBestPlan-2` | 419.3 | 352 | 1 | Cost-based candidate selection |
| `BenchmarkPhase15_PlanCache_Hit-2` | 96.03 | 0 | 0 | 16-shard LRU plan cache hit |
| `BenchmarkPhase15_PlanCache_Miss-2` | 2,219 | 797 | 16 | Plan cache miss + singleflight + translation |
| `BenchmarkPhase15_ConcurrentMultiTenant_PlanCache-2` | 132.2 | 0 | 0 | Concurrent multi-tenant plan cache hit |

---

## 3. Baseline Gaps Targeted by Phase 16

1. **Plan Cache Invalidation & Scoping (16A)**: `PlanCacheKey` lacks `DatabaseID`, `SearchPath`, `ParamSigHash`, `SettingsHash`, and per-tenant `SchemaVersion`; `StatsCache.Version()` is global; `sqlitePreparedStmt` is not invalidated on DDL; `translateCached` uses `SyncedLSN` (thrashing on DML).
2. **Execution Memory & GC Pressure (16B)**: `queryWithContextInternal` materializes all rows into `[][]SQLiteValue` without memory budgets; eager `fmt.Sprintf` on every integer/float cell; `C.CString` and `C.malloc` per query.
3. **Cancellation & Resource Limits (16F)**: Per-query watcher goroutine + `C.malloc` for cancel flag; no max intermediate result byte/row limit.
4. **Prepared Statements (16E)**: All parameters bound as `TEXT` (`air_bind_text`); no prepared statement cache or typed parameter binding.
5. **Scheduling, Parallelism, Joins & Observability (16G, 16C, 16D, 16H)**: No global CPU/memory-aware fair tenant scheduler; serial-only 16-block scans; SQLite-only join execution; missing per-query optimizer/resource observability.
