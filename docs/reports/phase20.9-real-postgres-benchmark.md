# AIR Phase 20.9 — Real PostgreSQL 17 Benchmark Report

**Date:** 2026-11-23  
**Base Commit:** `28cc6657` (plus Phase 20.9 telemetry publisher + `pooler.go` read-LSN guard)  
**Environment:** Linux `6.8.0-1070-azure` x86_64, 2 vCPU, 7.8 GiB RAM, 0 swap, PostgreSQL `17.12` (`/usr/lib/postgresql/17/bin`), Go `1.27.3`, cgroup v2 (`memory.high=435 MiB`, `memory.max=512 MiB`, `memory.swap.max=0`), real `cell.Agent` + real `air-gateway` over TCP.  
**Data Classification:** All tables below are **Real PostgreSQL 17.12 Measured Results** unless explicitly labeled **Prior Phase 20 Mock Result**.

---

## 1. Real PostgreSQL 17 vs Prior Mock Benchmark Comparison

| Metric (8 Concurrent Clients) | Prior Phase 20 Mock (`net.Pipe`, 50µs sleep) | Real PG 17.12 (`autocommit_insert`, `D_gov_buf_lazylsn`) | Real PG 17.12 (`pgbench_tpcb`, `D_gov_buf_lazylsn`) |
|---|---:|---:|---:|
| Throughput (TPS) | 15,958 (mock) | **4,589.6** | **894.5** |
| p50 Latency | 0.41 ms (mock) | **1.66 ms** | — (avg **8.94 ms**) |
| p95 Latency | 1.10 ms (mock) | **2.75 ms** | — |
| p99 Latency | 2.23 ms (mock) | **3.56 ms** | — |
| Real WAL / fsync | None (mock) | Yes (`fsync=on`, `full_page_writes=on`) | Yes (`fsync=on`, `full_page_writes=on`) |

---

## 2. Workload Suite Results (Real PG 17.12, 512 MB Cell, Full Phase 20, 5s/workload)

Raw output: [workloads.log](file:///workspaces/air/benchmarks/phase20.9/results/workloads.log), [workloads.json](file:///workspaces/air/benchmarks/phase20.9/results/workloads.json).

| Category | Workload | Clients | TPS | Rows/s | p50 (ms) | p95 (ms) | p99 (ms) | Errors | WAL Delta | Cgroup Peak | GW RSS |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| INSERT | `insert_single_noindex` | 8 | 4,360.3 | 4,360.3 | 1.65 | 3.34 | 5.15 | 0 | 18.0 MB | 91.3 MB | 22.1 MB |
| INSERT | `insert_single_indexed` | 8 | 4,199.8 | 4,199.8 | 1.78 | 3.14 | 4.50 | 0 | 6.6 MB | 113.4 MB | 22.1 MB |
| INSERT | `insert_multirow_50` | 8 | 1,771.5 | **88,575.4** | 3.88 | 7.63 | 11.11 | 0 | 36.8 MB | 191.1 MB | 22.1 MB |
| UPDATE | `update_nonindexed_small` | 8 | 2,935.2 | 2,935.2 | 2.62 | 3.52 | 4.99 | 0 | 2.8 MB | 191.1 MB | 22.2 MB |
| UPDATE | `update_indexed_col` | 8 | 2,738.2 | 2,738.2 | 2.75 | 4.13 | 6.23 | 0 | 4.0 MB | 191.2 MB | 22.2 MB |
| UPDATE | `update_large_row_4kb` | 8 | 2,167.1 | 2,167.1 | 3.56 | 4.78 | 6.36 | 0 | 2.6 MB | 191.2 MB | 22.5 MB |
| UPSERT | `upsert_no_conflict` | 8 | 2,911.4 | 2,911.4 | 2.57 | 4.17 | 5.94 | 0 | 2.7 MB | 191.2 MB | 22.4 MB |
| UPSERT | `upsert_on_conflict_low` | 8 | 2,938.2 | 2,938.2 | 2.62 | 3.46 | 5.51 | 0 | 3.5 MB | 191.2 MB | 22.8 MB |
| UPSERT | `upsert_high_conflict_hot` | 8 | 2,835.3 | 2,835.3 | 2.62 | 4.35 | 6.38 | 0 | 2.4 MB | 191.2 MB | 22.4 MB |
| DELETE | `delete_small_single` | 8 | 3,087.8 | 3,087.8 | 2.52 | 3.21 | 4.33 | 0 | 1.4 MB | 208.1 MB | 22.6 MB |
| DELETE | `delete_large_batch_100rows` | 4 | 1,075.3 | **107,530.0** | 3.16 | 7.92 | 11.14 | 0 | 12.6 MB | 208.2 MB | 22.5 MB |
| COPY | `copy_small_1k_rows` | 4 | 446.0 | **446,045.4** | 8.05 | 11.95 | 32.91 | 0 | 90.7 MB | 435.9 MB | **23.1 MB** (peak) |
| COPY | `copy_large_25k_rows` | 4 | 37.5 | **936,746.5** | 92.74 | 186.15 | 213.09 | 0 | 198.5 MB | 435.9 MB | **23.0 MB** (peak) |
| TX | `tx_explicit_begin_commit` | 8 | 1,635.3 | 3,270.6 | 4.62 | 6.79 | 10.52 | 0 | 1.8 MB | 435.9 MB | 22.9 MB |
| TX | `tx_explicit_rollback` | 8 | 2,403.0 | 2,403.0 | 3.04 | 5.25 | 8.70 | 0 | 1.4 MB | 435.9 MB | 89.7 MB* |
| TX | `tx_savepoint_rollback_to` | 8 | 1,275.1 | 2,550.3 | 6.02 | 8.60 | 12.40 | 0 | 1.5 MB | 435.9 MB | 24.7 MB |

*\*Transient Go heap spike during periodic `MeteringLogger` flush; reclaimed immediately by Go GC to 24.7 MB.*

### Key Workload Observations
- **Streaming `COPY FROM STDIN` Verified:** `copy_large_25k_rows` ingested **936,746 rows/s** (~198.5 MB WAL in 5s) across 4 concurrent clients while Gateway RSS peak remained flat at **23.0 MB**, proving `relayCopyIn` streams wire packets without buffering the full upload in Gateway memory.
- **Zero Errors Across All 16 Workloads:** Every INSERT, UPDATE, UPSERT, DELETE, COPY, and explicit transaction workload completed with `err=0`.

---

## 3. Concurrency Sweep (`1, 2, 4, 8, 16, 32` Clients × 5 Configurations)

Raw output: [sweep.log](file:///workspaces/air/benchmarks/phase20.9/results/sweep.log), [sweep.json](file:///workspaces/air/benchmarks/phase20.9/results/sweep.json).  
Configuration parameters: `PoolMaxBackends=32`, `WriteMaxGlobal=16` (B–E), `WriteMinGlobal=2`, `WriteMaxPerTenant=8` (B–E), 512 MB cgroup v2 (`memory.high=435 MiB`, `memory.max=512 MiB`).

### 3.1 Autocommit `INSERT` Throughput (TPS) and Tail Latency (p95 / p99 ms)

| Clients | A: No Governor | B: Static Gov | C: Adaptive Gov | D: Gov+Buf+LazyLSN | E: Full Phase 20 |
|---:|---|---|---|---|---|
| **1** | 2,121.1 TPS (0.58 / 1.05ms) | 2,140.9 TPS (0.56 / 0.93ms) | 2,135.7 TPS (0.57 / 0.98ms) | **2,152.1 TPS (0.56 / 0.93ms)** | 2,150.7 TPS (0.55 / 0.84ms) |
| **2** | 2,976.3 TPS (1.03 / 1.79ms) | 2,934.1 TPS (0.98 / 1.56ms) | 2,994.7 TPS (0.95 / 1.51ms) | **3,060.4 TPS (0.88 / 1.31ms)** | 3,026.7 TPS (0.93 / 1.46ms) |
| **4** | 3,958.3 TPS (1.50 / 1.99ms) | 3,845.7 TPS (1.57 / 2.10ms) | 3,202.2 TPS (1.66 / 2.24ms) | **3,862.1 TPS (1.56 / 2.24ms)** | 3,077.8 TPS (1.77 / 2.46ms) |
| **8** | 4,594.2 TPS (2.76 / 3.79ms) | 4,491.1 TPS (2.83 / 3.82ms) | 3,152.5 TPS (3.24 / 4.46ms) | **4,589.6 TPS (2.75 / 3.56ms)** | 3,127.4 TPS (3.24 / 4.46ms) |
| **16** | 5,091.7 TPS (5.02 / 6.59ms) | 4,450.3 TPS (5.30 / 7.25ms) | 3,032.0 TPS (5.90 / 7.73ms) | **4,456.7 TPS (5.03 / 6.62ms)** | 3,087.3 TPS (6.15 / 8.47ms) |
| **32** | 5,103.4 TPS (10.54 / 14.35ms) | 4,574.3 TPS (**9.07 / 12.52ms**) | 3,115.3 TPS (11.79 / 14.95ms) | **4,653.9 TPS (8.66 / 10.90ms)** | 3,125.5 TPS (11.68 / 16.11ms) |

### 3.2 Real `pgbench` TPC-B (`-M simple -n`, Explicit Transactions, Scale 10)

| Clients | A: No Governor | B: Static Gov | C: Adaptive Gov | D: Gov+Buf+LazyLSN | E: Full Phase 20 |
|---:|---:|---:|---:|---:|---:|
| **1** | 471.6 TPS (2.12ms) | 465.6 TPS (2.15ms) | 468.1 TPS (2.14ms) | 455.5 TPS (2.20ms) | 457.5 TPS (2.19ms) |
| **2** | 701.7 TPS (2.85ms) | 686.2 TPS (2.91ms) | 693.1 TPS (2.89ms) | 685.1 TPS (2.92ms) | 679.7 TPS (2.94ms) |
| **4** | 809.8 TPS (4.94ms) | 786.9 TPS (5.08ms) | 812.9 TPS (4.92ms) | **818.3 TPS (4.89ms)** | 817.3 TPS (4.89ms) |
| **8** | 865.9 TPS (9.24ms) | 868.1 TPS (9.21ms) | 888.1 TPS (9.01ms) | **894.5 TPS (8.94ms)** | 854.0 TPS (9.37ms) |
| **16** | 855.7 TPS (18.70ms) | **877.7 TPS (18.23ms)** | 822.8 TPS (19.45ms) | 841.8 TPS (19.01ms) | 818.6 TPS (19.55ms) |
| **32** | 879.5 TPS (36.38ms) | **885.6 TPS (36.13ms)** | 847.6 TPS (37.76ms) | 860.5 TPS (37.19ms) | 795.8 TPS (40.21ms) |

---

## 4. Bottleneck & Governor Analysis

1. **Best Overall Configuration: `D_gov_buf_lazylsn` (Static Write Governor + Buffer Budget + Lazy LSN, Adaptive OFF):**
   - At `c=8`, matches No-Governor TPS (`4,589.6` vs `4,594.2`) while reducing p99 by `-6.1%` and achieving the highest `pgbench` TPC-B throughput (**894.5 TPS**, `+3.3%`).
   - At `c=32`, caps active writes to `WriteMaxPerTenant=8`, trading `-8.8%` peak single-tenant TPS for **-17.8% p95 latency (`10.54ms -> 8.66ms`)**, **-24.0% p99 latency (`14.35ms -> 10.90ms`)**, and bounded non-reclaimable PostgreSQL anonymous memory.
2. **Adaptive Governor (`C` / `E`) Miscalibration on 2-vCPU Host:**
   - Under real PostgreSQL 17 write loads on a 2-vCPU VM, Linux `cpu.pressure` (`some avg10`) regularly reached `30–65%` even with `0%` memory PSI and `0` `memory.high` events.
   - Because `AdaptiveController` treats `CPUPSISomeAvg10 >= 25.0` as pressured (and requires `< 15.0` to recover), it clamped `effectiveGlobalLimit` permanently to `minGlobal=2`, reducing autocommit TPS by **-32% to -39%** with worse tail latency. **Adaptive mode MUST remain disabled by default (`WriteAdaptiveEnabled=false`)** until CPU PSI thresholds are decoupled or core-normalized.
3. **Primary Write-Path Bottlenecks Identified:**
   - **2-vCPU saturation + WAL sync/commit** caps real PostgreSQL 17 autocommit throughput around `~4,600–5,100 TPS` and `pgbench` TPC-B around `~880–895 TPS`.
   - **Warm backend reuse ping (`pooler.go:1455-1465`):** `execSimpleBackend(b, "")` / `DISCARD ALL;` adds an extra synchronous round-trip to PostgreSQL on every autocommit write.

---

## 5. Limitations & Reproduction

- **Storage integration:** Benchmarks ran against stock host PostgreSQL 17.12 (`md.c`) because `air_smgr.so` requires patched PG17 and `cmd/air-server/main.go` does not yet wire `SetPageserverConfig`.
- **Reproduction commands:**
  ```bash
  ./benchmarks/phase20.9/setup_env.sh
  ./benchmarks/phase20.9/run_benchmarks.sh workloads 5
  ./benchmarks/phase20.9/run_benchmarks.sh sweep 5
  ./benchmarks/phase20.9/cleanup_env.sh
  ```
