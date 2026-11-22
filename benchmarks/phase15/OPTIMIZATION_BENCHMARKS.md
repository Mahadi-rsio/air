# AIR Phase 15 — Optimization Benchmarks (`OPTIMIZATION_BENCHMARKS.md`)

## 1. Environment

- **CPU**: AMD EPYC 7763 64-Core Processor (2 vCPUs exposed)
- **RAM**: 7.8 GiB
- **OS**: Linux `amd64`
- **Go**: `go1.27.3`
- **PostgreSQL**: `17.12`

## 2. Reproducible Benchmark Results (`-benchmem -count=3`)

Command:
```bash
go test ./internal/readadapter -run '^$' -bench 'BenchmarkPhase15|BenchmarkPhase14A_BTreePointLookupVsSeqScan|BenchmarkReadEngineL1HotPage' -benchmem -count=3
```

| Benchmark | Run 1 | Run 2 | Run 3 | Bytes/op | Allocs/op |
| :--- | ---: | ---: | ---: | ---: | ---: |
| `BenchmarkReadEngineL1HotPage-2` | `2,803 ns/op` | `3,166 ns/op` | `2,818 ns/op` | `8,208 B/op` | `2 allocs/op` |
| `BenchmarkPhase14A_BTreePointLookupVsSeqScan/BTreeIndexPushdown_5000rows_100pages-2` | `57,446 ns/op` | `57,219 ns/op` | `62,087 ns/op` | `37,524 B/op` | `77 allocs/op` |
| `BenchmarkPhase14A_BTreePointLookupVsSeqScan/SequentialHeapScan_5000rows_100pages-2` | `770,083 ns/op` | `844,788 ns/op` | `883,156 ns/op` | `830,666 B/op` | `178 allocs/op` |
| `BenchmarkPhase15_Optimizer_ChooseBestPlan-2` | `418.0 ns/op` | `414.8 ns/op` | `413.2 ns/op` | `352 B/op` | `1 allocs/op` |
| `BenchmarkPhase15_PlanCache_Hit-2` | `100.4 ns/op` | `95.78 ns/op` | `102.9 ns/op` | `0 B/op` | `0 allocs/op` |
| `BenchmarkPhase15_PlanCache_Miss-2` | `2,215 ns/op` | `2,271 ns/op` | `2,222 ns/op` | `797 B/op` | `16 allocs/op` |
| `BenchmarkPhase15_ConcurrentMultiTenant_PlanCache-2` | `132.3 ns/op` | `133.0 ns/op` | `132.9 ns/op` | `0 B/op` | `0 allocs/op` |

## 3. Key Findings

1. **Plan Cache Speedup**: Cache hits run in `~96–103 ns/op` with **`0 B/op` and `0 allocs/op`**, compared to `~2,215–2,271 ns/op` (`797 B/op`, `16 allocs/op`) on a miss — a **~22x latency reduction** with zero GC pressure.
2. **Multi-Tenant Scalability**: Across 8 concurrent tenants (`BenchmarkPhase15_ConcurrentMultiTenant_PlanCache-2`), the 16-shard LRU sustains `~132–133 ns/op` with **`0 B/op` and `0 allocs/op`**.
3. **Sub-Microsecond Cost-Based Planning**: `QueryOptimizer.ChooseBestPlan` evaluates candidate plans in `~413–418 ns/op` with `1 alloc/op` (`352 B/op`).
4. **Indexed Point Lookup vs. Sequential Scan**: Cost-based B-tree index selection achieves `~57.2–62.1 µs/op` vs `~770–883 µs/op` for sequential heap scan (**~13.8x faster**, **~22x lower allocation bytes**).
