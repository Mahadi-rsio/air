# AIR Phase 18 — Baseline Report (`PHASE18_BASELINE.md`)

## 1. Environment

- **CPU**: AMD EPYC 7763 64-Core Processor (2 vCPUs exposed)
- **OS**: Linux (Ubuntu 24.04)
- **Go**: `go1.27.3 linux/amd64`
- **PostgreSQL**: `17.12` (`psql (PostgreSQL) 17.12 (Ubuntu 17.12-1.pgdg24.04+1)`)
- **Cgroup v2**: Writable (`/sys/fs/cgroup/`, controllers: `cpuset cpu io memory hugetlb pids rdma misc`)

---

## 2. Baseline Test Suite Results

| Suite | Command | Result |
|---|---|---|
| Gateway (`-short`) | `go test -short ./internal/gateway/...` | **PASS** (5.073s) |
| ReadAdapter (`-short`) | `go test -short ./internal/readadapter/...` | **PASS** (8.261s) |
| Pageserver (`-short`) | `go test -short ./internal/pageserver/...` | **PASS** (16.260s) |
| Live PG 17.12 Differential (16I) | `go test ./internal/readadapter -run 'TestPhase16I_DifferentialCorrectness_LivePG17' -v` | **PASS** (23.91s) |
| Live PG 17.12 Differential (17B) | `go test ./internal/readadapter -run 'TestPhase17B_DifferentialCorrectness_LivePG17' -v` | **PASS** (56.63s) |

---

## 3. Baseline Microbenchmarks (`AMD EPYC 7763`, 2 vCPUs)

Command:
```bash
go test ./internal/readadapter -run '^$' -bench 'BenchmarkPhase15_|BenchmarkPhase16B_|BenchmarkPhase16C_|BenchmarkPhase16D_|BenchmarkPhase16E_|BenchmarkPhase16J_' -benchmem -count=1
```

| Benchmark | ns/op | B/op | allocs/op | Extra |
|---|---:|---:|---:|---|
| `BenchmarkPhase15_Optimizer_ChooseBestPlan-2` | 557.2 | 384 | 1 | — |
| `BenchmarkPhase15_PlanCache_Hit-2` | 121.5 | 0 | 0 | — |
| `BenchmarkPhase15_PlanCache_Miss-2` | 3,051 | 963 | 16 | — |
| `BenchmarkPhase15_ConcurrentMultiTenant_PlanCache-2` | 163.7 | 0 | 0 | — |
| `BenchmarkPhase16B_SQLiteQuery_RowArena-2` | 153,178 | 93,232 | 362 | — |
| `BenchmarkPhase16C_SerialVsParallelHeapAggregate/Serial_1Worker_128Pages-2` | 775,612 | 1,083,601 | 449 | — |
| `BenchmarkPhase16C_SerialVsParallelHeapAggregate/Parallel_2Workers_128Pages-2` | 766,160 | 1,084,098 | 452 | — |
| `BenchmarkPhase16D_JoinAlgorithms_2000x2000/HashJoin-2` | 1,175,109 | 1,700,448 | 6,013 | — |
| `BenchmarkPhase16D_JoinAlgorithms_2000x2000/SortMergeJoin-2` | 2,175,873 | 1,821,793 | 4,046 | — |
| `BenchmarkPhase16D_JoinAlgorithms_2000x2000/NestedLoopJoin-2` | 27,901,920 | 1,506,256 | 4,014 | — |
| `BenchmarkPhase16E_NormalQuery-2` | 11,436 | 3,056 | 32 | — |
| `BenchmarkPhase16E_PreparedStmt_Hit-2` | 7,087 | 2,920 | 7 | — |
| `BenchmarkPhase16E_PreparedStmt_Miss-2` | 14,391 | 3,248 | 35 | — |
| `BenchmarkPhase16J_MultiTenantScale_PlanAndScheduler/Tenants_1-2` | 916.6 | 224 | 6 | 100.0% hit, p50=0.711µs, p99=1.713µs |
| `BenchmarkPhase16J_MultiTenantScale_PlanAndScheduler/Tenants_8-2` | 904.7 | 224 | 6 | 100.0% hit, p50=0.752µs, p99=1.754µs |
| `BenchmarkPhase16J_MultiTenantScale_PlanAndScheduler/Tenants_32-2` | 1,028 | 224 | 6 | 99.99% hit, p50=0.751µs, p99=4.078µs |
| `BenchmarkPhase16J_MultiTenantScale_PlanAndScheduler/Tenants_100-2` | 881.4 | 224 | 6 | 99.97% hit, p50=0.751µs, p99=2.064µs |
| `BenchmarkPhase16J_MultiTenantScale_PlanAndScheduler/Tenants_500-2` | 912.0 | 224 | 6 | 99.82% hit, p50=0.802µs, p99=7.293µs |
| `BenchmarkPhase16J_EndToEnd_CacheHotVsCold_PointLookup/PreparedCacheHot-2` | 21,584 | 12,514 | 28 | — |

---

## 4. Baseline Gaps & Phase 18 Targets

1. **PostgreSQL Type Coverage**: `numeric` (OID 1700), `inet` (OID 869), `cidr` (OID 650), and scalar arrays (`int2[]`, `int4[]`, `int8[]`, `text[]`, `bool[]`, `float4[]`, `float8[]`) currently map to `ColUnsupported` and trigger `ErrUnsupportedType` fallback to PostgreSQL.
2. **L2 Disk Cache Bounds & Page-Cache Pressure**: `TieredCacheBackend` (`internal/readadapter/cache.go`) only enforces `L2MaxBytes` (without `L2MaxEntries`, `L2MaxEntryBytes`, concurrency bounds, or `L2Dir` bypass/direct-IO/fadvise controls), and `SQLiteServerConfig` does not wire `L2MaxBytes` into `NewTieredCacheBackend`.
3. **Fallback & Cache Observability**: `PrometheusMetrics()` lacks several Phase 18 metrics (`air_native_read_queries_total`, `air_postgres_fallback_queries_total`, `air_postgres_fallback_total{reason}`, `air_cache_l1_hits_total`, `air_cache_l2_hits_total`, `air_cache_misses_total`, `air_cache_evictions_total`, `air_l2_cache_bytes`, `air_l2_cache_entries`, `air_tenant_memory_bytes`, `air_cgroup_memory_bytes`).
