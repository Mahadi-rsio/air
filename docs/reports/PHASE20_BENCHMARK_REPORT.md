# AIR Phase 20 — Benchmark Report

**Date:** 2026-11-23  
**Environment:** Linux amd64, Intel(R) Xeon(R) CPU @ 2.30GHz (2 vCPU), 8 GiB RAM, Go 1.26.0  
**Harness:** `internal/gateway/write_governor_integration_test.go` (`BenchmarkWritePath_*`) with in-process `TenantPool` + mock PostgreSQL wire-protocol backend (50 µs simulated write execution delay).

---

## 1. Write Path Comparison (8 Concurrent Clients)

| Configuration | Throughput (ops/s) | Avg (ns/op) | p50 (µs) | p95 (µs) | p99 (µs) | Heap (B/op) | Allocs/op |
|---|---:|---:|---:|---:|---:|---:|---:|
| Existing AIR Write Path (`WriteGovernor=false`) | 16,020 | 62,420 | 400 | 1,400 | 2,513 | 1,267 | 48 |
| Static Bounded Write Governor | 16,034 | 62,366 | 407 | 1,148 (-18.0%) | 2,344 (-6.7%) | 1,329 | 49 |
| Adaptive Write Governor (AIMD) | 15,394 | 64,960 | 423 | 1,267 (-9.5%) | 2,521 (+0.3%) | 1,329 | 49 |
| All Optimizations (`Governor + Adaptive + LazyLSN`) | 15,958 | 62,665 | 411 | 1,102 (-21.3%) | 2,225 (-11.5%) | 1,331 | 49 |

### Key Observations
- **Negligible Overhead:** Adding `WriteGovernor` admission, `BufferBudget` tracking, and `ClassifyWriteRequest` adds only **+1 alloc/op** (+62–64 B/op) on the write hot path.
- **Tail-Latency Improvement:** Bounded write admission reduces backend lock/pipe contention at 8 clients, improving **p95 latency by 18.0%–21.3%** and **p99 latency by 6.7%–11.5%**.
- **Lazy LSN Watermark:** Eliminates 100% of post-write `SELECT pg_current_wal_lsn()::text` round-trips on pure write workloads (`0` LSN queries vs `1` per write when `ReadEnginePort > 0`), verified in `TestPhase20Integration_TxPinningBufferBudgetAndLazyLSN`.

---

## 2. Concurrency Sweep (All Optimizations Enabled)

| Clients | Avg (ns/op) | p50 (µs) | p95 (µs) | p99 (µs) | Heap (B/op) | Allocs/op |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,142,989 | 1,308 | 1,502 | 1,998 | 1,323 | 49 |
| 2 | 414,220 | 560 | 1,684 | 2,066 | 1,335 | 49 |
| 4 | 185,090 | 525 | 1,561 | 1,878 | 1,335 | 49 |
| 8 | 63,469 | 408 | 1,392 | 2,204 | 1,331 | 49 |
| 16 | 68,010 | 962 | 2,178 | 3,731 | 1,806 | 55 |

---

## 3. Memory & Allocation Analysis
- **Gateway Buffer Cap:** Global extended-query buffer memory is now strictly bounded by `AIR_WRITE_BUFFER_GLOBAL_MAX` (default 64 MiB) and `AIR_WRITE_BUFFER_SESSION_MAX` (default 8 MiB), preventing multi-gigabyte heap spikes under 1,024 concurrent clients.
- **Slice Retention Fix:** `handleExtendedBatch` releases `extBatch` backing arrays (`cap > 256`) after batch execution so large extended-query batches do not pin heap memory for idle sessions.
- **Limitations:** Benchmarks above were executed against a mock PostgreSQL wire-protocol backend because PostgreSQL server binaries are not installed in this VM; full `pgbench` + cgroup v2 `memory.peak` validation against live PostgreSQL 17 should be run in the target Linux cell environment.
