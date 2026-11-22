# AIR Phase 17 — Baseline Report (`PHASE17_BASELINE.md`)

## 1. Environment

- **CPU**: Intel(R) Xeon(R) CPU @ 2.30GHz (2 vCPUs exposed)
- **RAM**: 7.8 GiB (`Mem: 7.8Gi`), 8.0 GiB Swap
- **OS**: Linux `6.18.57` x86_64 (Ubuntu 24.04 noble)
- **Go**: `go1.26.0 linux/amd64`
- **PostgreSQL**: `17.12` (`postgresql-17 17.12-1.pgdg24.04+1`)
- **SQLite**: `libsqlite3-dev 3.45.1-1ubuntu2.9`
- **Starting Commit**: `91b4309 feat(phase16): production-grade execution engine (16A-16J)`

---

## 2. Baseline Test Suite Results

| Suite | Command | Result |
|---|---|---|
| Gateway (`-short`) | `go test -short ./internal/gateway/...` | **PASS** (5.108s) |
| ReadAdapter (`-short`) | `go test -short ./internal/readadapter/...` | **PASS** (6.952s) |
| Pageserver (`-short`) | `go test -short ./internal/pageserver/...` | **PASS** (30.174s) |
| Live PG 17.12 Differential | `go test ./internal/readadapter -run 'TestPhase16I_DifferentialCorrectness_LivePG17' -v` | **PASS** (24.00s) |

---

## 3. Baseline Microbenchmarks (`Intel Xeon @ 2.30GHz`, 2 vCPUs)

| Benchmark | ns/op | B/op | allocs/op | Extra |
|---|---:|---:|---:|---|
| `BenchmarkPhase15_Optimizer_ChooseBestPlan-2` | 774.1 | 384 | 1 | — |
| `BenchmarkPhase15_PlanCache_Hit-2` | 161.8 | 0 | 0 | — |
| `BenchmarkPhase15_PlanCache_Miss-2` | 3,729 | 964 | 16 | — |
| `BenchmarkPhase15_ConcurrentMultiTenant_PlanCache-2` | 193.6 | 0 | 0 | — |
| `BenchmarkPhase16B_SQLiteQuery_RowArena-2` | 241,815 | 93,232 | 362 | — |
| `BenchmarkPhase16C_SerialVsParallelHeapAggregate/Serial_1Worker_128Pages-2` | 1,291,590 | 1,083,644 | 450 | — |
| `BenchmarkPhase16C_SerialVsParallelHeapAggregate/Parallel_2Workers_128Pages-2` | 1,152,443 | 1,084,105 | 452 | — |
| `BenchmarkPhase16D_JoinAlgorithms_2000x2000/HashJoin-2` | 2,253,943 | 1,700,448 | 6,013 | — |
| `BenchmarkPhase16D_JoinAlgorithms_2000x2000/SortMergeJoin-2` | 4,093,802 | 1,821,792 | 4,046 | — |
| `BenchmarkPhase16D_JoinAlgorithms_2000x2000/NestedLoopJoin-2` | 74,407,943 | 1,506,256 | 4,014 | — |
| `BenchmarkPhase16E_NormalQuery-2` | 26,110 | 3,055 | 32 | — |
| `BenchmarkPhase16E_PreparedStmt_Hit-2` | 13,144 | 2,920 | 7 | — |
| `BenchmarkPhase16E_PreparedStmt_Miss-2` | 38,374 | 3,248 | 35 | — |
| `BenchmarkPhase16J_MultiTenantScale_PlanAndScheduler/Tenants_1-2` | 2,096 | 224 | 6 | 100.0% hit, p50=0.68µs, p99=6.52µs |
| `BenchmarkPhase16J_MultiTenantScale_PlanAndScheduler/Tenants_8-2` | 2,263 | 224 | 6 | 99.99% hit, p50=0.77µs, p99=7.08µs |
| `BenchmarkPhase16J_MultiTenantScale_PlanAndScheduler/Tenants_32-2` | 2,139 | 224 | 6 | 99.98% hit, p50=0.72µs, p99=6.56µs |
| `BenchmarkPhase16J_MultiTenantScale_PlanAndScheduler/Tenants_100-2` | 2,209 | 224 | 6 | 99.91% hit, p50=0.74µs, p99=6.56µs |
| `BenchmarkPhase16J_MultiTenantScale_PlanAndScheduler/Tenants_500-2` | 2,948 | 225 | 6 | 99.33% hit, p50=0.85µs, p99=8.36µs |
| `BenchmarkPhase16J_EndToEnd_CacheHotVsCold_PointLookup/PreparedCacheHot-2` | 46,879 | 12,513 | 28 | — |

---

## 4. Known Baseline Gaps Entering Phase 17

1. **PostgreSQL Fast-Default (`atthasmissing` / `attmissingval`)**: `air_pg` currently decodes missing trailing attributes (`i >= natts`) as `NULL`, returning wrong data when `ALTER TABLE ... ADD COLUMN ... DEFAULT <non-null>` is used without table rewrite.
2. **Pageserver `HEAP2_PRUNE_ON_ACCESS` / `HEAP2_PRUNE_VACUUM_*`**: `internal/pageserver/heap_redo.go` does not replay `RmHeap2ID` prune records (`XLOG_HEAP2_PRUNE_ON_ACCESS` etc.), and `DefaultWALRedoer.Redo` silently returns `cur` (the un-updated base page!) when `applyHeapWALRecords` returns `nil` and `len(cur) == PageSize`!
3. **Unbounded SLRU Cache in `CLOGResolver`**: `mvcc.go` stores `map[slruCacheKey][]byte` without eviction, posing a memory leak risk under sustained multi-tenant workloads.
4. **No cgroup-constrained 512 MB / 1 GB / 2 GB / 4 GB capacity measurements yet**: Phase 16 capacity numbers were theoretical estimates from a larger machine.
