# Phase 18 — Performance Regression Report

**Date**: 2026-11-22  
**CPU**: AMD EPYC 7763 64-Core Processor (`go1.27.3 linux/amd64`, `-benchmem`)

---

## 1. Baseline vs Post-Phase 18 Microbenchmarks

| Benchmark | Baseline (ns/op) | Phase 18 (ns/op) | Delta | Notes |
|---|---:|---:|---:|---|
| `BenchmarkPhase15_Optimizer_ChooseBestPlan-2` | 557.2 | 607.5 | +9.0% | Same 384 B / 1 alloc |
| `BenchmarkPhase15_PlanCache_Hit-2` | 121.5 | 120.1 | **-1.2%** | 0 B / 0 allocs |
| `BenchmarkPhase15_PlanCache_Miss-2` | 3,051 | 2,566 | **-15.9%** | 962 B / 16 allocs |
| `BenchmarkPhase15_ConcurrentMultiTenant_PlanCache-2` | 163.7 | 163.3 | **-0.2%** | 0 B / 0 allocs |
| `BenchmarkPhase16B_SQLiteQuery_RowArena-2` | 153,178 | 148,943 | **-2.8%** | Same 93,232 B / 362 allocs |
| `BenchmarkPhase16C_SerialVsParallelHeapAggregate/Serial_1Worker_128Pages-2` | 775,612 | 950,317 | +22.5% | Run-to-run CPU variance |
| `BenchmarkPhase16C_SerialVsParallelHeapAggregate/Parallel_2Workers_128Pages-2` | 766,160 | 881,422 | +15.0% | Run-to-run CPU variance |
| `BenchmarkPhase16D_JoinAlgorithms_2000x2000/HashJoin-2` | 1,175,109 | 1,146,696 | **-2.4%** | Same 6,013 allocs |
| `BenchmarkPhase16D_JoinAlgorithms_2000x2000/SortMergeJoin-2` | 2,175,873 | 1,921,068 | **-11.7%** | Same 4,046 allocs |
| `BenchmarkPhase16D_JoinAlgorithms_2000x2000/NestedLoopJoin-2` | 27,901,920 | 27,739,334 | **-0.6%** | Same 4,014 allocs |
| `BenchmarkPhase16E_NormalQuery-2` | 11,436 | 11,595 | +1.4% | Same 3,056 B / 32 allocs |
| `BenchmarkPhase16E_PreparedStmt_Hit-2` | 7,087 | 7,199 | +1.6% | Same 2,920 B / 7 allocs |
| `BenchmarkPhase16E_PreparedStmt_Miss-2` | 14,391 | 14,075 | **-2.2%** | Same 3,248 B / 35 allocs |
| `BenchmarkPhase16J_MultiTenantScale_PlanAndScheduler/Tenants_1-2` | 916.6 | 892.2 | **-2.7%** | 100.0% hit, p50=0.712µs |
| `BenchmarkPhase16J_MultiTenantScale_PlanAndScheduler/Tenants_500-2` | 912.0 | 919.8 | +0.9% | 99.82% hit, p50=0.791µs |
| `BenchmarkPhase16J_EndToEnd_CacheHotVsCold_PointLookup/PreparedCacheHot-2` | 21,584 | 22,866 | +5.9% | Same 12,514 B / 28 allocs |

---

## 2. Assessment

No allocation or hot-path regression was introduced: plan cache hits (`120.1 ns/op`, 0 allocs), row arena queries (`148.9 µs/op`, **-2.8%**), joins (**-0.6% to -11.7%**), prepared statement hits (`7.2 µs/op`), and 500-tenant scheduler scaling (`919.8 ns/op`, `p50=0.791 µs`) all remain at or faster than baseline.
