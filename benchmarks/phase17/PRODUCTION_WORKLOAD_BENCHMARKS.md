# AIR Phase 17F — Sustained Production Workload Benchmarks (`PRODUCTION_WORKLOAD_BENCHMARKS.md`)

## 1. Test Methodology

All five production workloads were executed for **30 seconds** each inside **real Linux cgroups v2** (`memory.swap.max = 0`) using [cap_harness/main.go](file:///home/antigravity/Mahadi-rsio/air/benchmarks/phase17/cap_harness/main.go) against `SQLiteReadServer` + `air_pg` + `TieredCacheBackend` + `MemoryManager` + `TenantScheduler`.

---

## 2. Sustained Workload Results (`0` errors, `0` OOMs across all 5 workloads)

| Workload | Cgroup Limit | Tenants | Workers | Duration | Total Queries | QPS | p50 (ms) | p95 (ms) | p99 (ms) | Peak RSS (MB) | Cgroup Peak (MB) | L1 Hit % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **W1: Read-Heavy (90/10)** | 1 GB | 50 | 8 | 30.01s | 8,972 | 298.9 | 17.21 | 53.47 | 96.76 | 155.9 | 642.2 | 21.65% |
| **W2: Mixed (70/30)** | 1 GB | 50 | 8 | 30.01s | 10,535 | 351.0 | 16.03 | 43.91 | 73.31 | 153.4 | 648.5 | 21.62% |
| **W3: Analytics (Joins/Aggs)** | 1 GB | 10 | 4 | 30.43s | 257 | 8.45 | 446.70 | 845.89 | 1,155.87 | 39.1 | 31.4 | 99.99% |
| **W4: Multi-Tenant SaaS** | 1 GB | 250 | 16 | 30.01s | 58,000 | **1,932.8** | **4.64** | **11.08** | **16.38** | 271.2 | 311.0 | 99.35% |
| **W5: Stress & Overload (Abusive)** | **512 MB** | 100 | 16 | 30.70s | 26,918 | 876.7 | 5.44 (norm: 5.38) | 20.06 (norm: 19.25) | 79.84 (norm: **63.40**) | 99.3 | 108.5 | 99.85% |

---

## 3. Observations

1. **Multi-Tenant SaaS (W4)**: 250 active tenants sustained **1,932.8 QPS** with **16.38 ms p99** and **99.35% L1 hit rate**, staying at **271.2 MB RSS** (`311.0 MB` cgroup peak) with `0` errors and `0` OOMs.
2. **Stress & Overload (W5)**: Under a **512 MB** hard cgroup limit with 100 tenants and 8 workers hammering `t_0000` with expensive cross-table joins, the other 99 tenants maintained **5.38 ms p50** and **63.40 ms p99**, while `t_0000` absorbed **1,376.6 ms p50** backpressure.
3. **LSN-Advancing Mixed Workloads (W1/W2)**: Advancing LSN on writes invalidates L1 entries (`~21.6%` L1 hit rate) and exercises L2 disk cache, increasing cgroup pagecache to `~642–649 MB`. On 512 MB nodes with high write rates, disabling L2 disk cache (`CacheDir=""`) is recommended to avoid kernel pagecache pressure.
