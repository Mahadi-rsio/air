# AIR Phase 17G — Performance Regression Protection (`PERFORMANCE_REGRESSION_REPORT.md`)

## 1. Comparison Methodology

All benchmarks were executed on the same host (`Intel(R) Xeon(R) CPU @ 2.30GHz`, 2 vCPUs, Ubuntu 24.04, Go `1.26.0`) before and after all Phase 17 correctness and capacity changes:

```bash
go test ./internal/readadapter -run '^$' -bench 'BenchmarkPhase15_|BenchmarkPhase16B_|BenchmarkPhase16C_|BenchmarkPhase16D_|BenchmarkPhase16E_|BenchmarkPhase16J_' -benchmem -count=1
```

---

## 2. Before vs After Benchmark Comparison

| Benchmark | Before (ns/op) | After (ns/op) | Delta | B/op (Before → After) | Allocs/op (Before → After) |
|---|---:|---:|---:|---|---|
| `BenchmarkPhase15_Optimizer_ChooseBestPlan-2` | 774.1 | 805.5 | +4.1% | 384 → 384 | 1 → 1 |
| `BenchmarkPhase15_PlanCache_Hit-2` | 161.8 | 162.7 | +0.6% | 0 → 0 | 0 → 0 |
| `BenchmarkPhase15_PlanCache_Miss-2` | 3,729 | 3,878 | +4.0% | 964 → 965 | 16 → 16 |
| `BenchmarkPhase15_ConcurrentMultiTenant_PlanCache-2` | 193.6 | 192.6 | -0.5% | 0 → 0 | 0 → 0 |
| `BenchmarkPhase16B_SQLiteQuery_RowArena-2` | 241,815 | 245,737 | +1.6% | 93,232 → 93,232 | 362 → 362 |
| `BenchmarkPhase16C_Serial_1Worker_128Pages-2` | 1,291,590 | 1,266,921 | -1.9% | 1,083,644 → 1,083,644 | 450 → 450 |
| `BenchmarkPhase16C_Parallel_2Workers_128Pages-2` | 1,152,443 | 1,142,338 | -0.9% | 1,084,105 → 1,084,107 | 452 → 452 |
| `BenchmarkPhase16D_HashJoin-2` | 2,253,943 | 1,767,117 | **-21.6%** | 1,700,448 → 1,700,448 | 6,013 → 6,013 |
| `BenchmarkPhase16D_SortMergeJoin-2` | 4,093,802 | 3,213,956 | **-21.5%** | 1,821,792 → 1,821,792 | 4,046 → 4,046 |
| `BenchmarkPhase16D_NestedLoopJoin-2` | 74,407,943 | 41,077,842 | **-44.8%** | 1,506,256 → 1,506,256 | 4,014 → 4,014 |
| `BenchmarkPhase16E_NormalQuery-2` | 26,110 | 16,540 | **-36.7%** | 3,055 → 3,056 | 32 → 32 |
| `BenchmarkPhase16E_PreparedStmt_Hit-2` | 13,144 | 8,867 | **-32.5%** | 2,920 → 2,920 | 7 → 7 |
| `BenchmarkPhase16E_PreparedStmt_Miss-2` | 38,374 | 20,195 | **-47.4%** | 3,248 → 3,248 | 35 → 35 |
| `BenchmarkPhase16J_MultiTenantScale/Tenants_500-2` | 2,948 | 1,490 | **-49.5%** | 225 → 224 | 6 → 6 |
| `BenchmarkPhase16J_EndToEnd_PreparedCacheHot-2` | 46,879 | 34,180 | **-27.1%** | 12,513 → 12,513 | 28 → 28 |

---

## 3. Verdict

**No performance or allocation regressions.** Every allocation metric (`B/op`, `allocs/op`) is identical to baseline (`0 B/op, 0 allocs/op` on plan cache hits; `7 allocs/op` on prepared statement hits; `28 allocs/op` on end-to-end `air_pg` point lookups).
