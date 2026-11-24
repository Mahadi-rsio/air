# AIR Phase 24 — Phase 2: Repeated Benchmark & Statistical Analysis

- **Environment**: `linux/amd64`, `2` vCPU (`nproc=2`), `7.8 GiB` RAM, `go1.27.3`.
- **Workload**: 20 batches × 5 records × 2048 B (`200 KiB` logical WAL per tenant per run, `sharedPattern=true`, `512 MB` `MemoryBudgetConfig` profile, `ApplyGoRuntimeLimit=false`) followed by `FlushTenant` per tenant.
- **Repeats**: 10 runs per configuration at `1, 8, 20, 30, 75, 100` tenants; 20 runs per configuration at `50` tenants.
- **Statistical Methodology**: Per-run `p99` is computed independently for each run, and `P99_MEAN`, `P99_MEDIAN`, `P99_STDDEV`, `P99_MIN`, `P99_MAX`, and `P99_CV%` summarize variability **across runs**. `POOLED_P50`, `POOLED_P95`, `POOLED_P99`, and `POOLED_P999` summarize the pooled per-batch latency samples across all runs (explicitly distinguished from across-run p99).

---

## 1. Root Cause of Phase 23 / Baseline Run-to-Run Variance

Baseline profiling of `TestPhase24_BenchmarkMatrix` (`-mutexprofile`, `-blockprofile`, `-cpuprofile`) identified two primary sources of multi-millisecond p99 outliers (`43–80 ms` spikes, `CV = 47–93%`):

1. **`O(N_tenants)` Lock Storm in `dirtyTenantCount()` (`internal/pageserver/server.go:474-484`, `memory_budget.go:295`)**: Every `enforceBeforeWrite()` call acquired `p.mu.RLock()` and locked every tenant's `tl.mu.RLock()`, creating lock convoys whenever `SetTenantGeneration` / `EnsureTenant` acquired `p.mu.Lock()` (`98.18s` / `96.24%` of total mutex delay in baseline profile).
2. **Unpressured STW `debug.FreeOSMemory()` Loops (`internal/pageserver/memory_budget.go:488-500`)**: The 200ms maintenance loop called `debug.FreeOSMemory()` (synchronous `runtime.GC()` + OS scavenge) every `500ms` whenever `HeapIdle - HeapReleased > 12 MiB`, injecting stop-the-world GC pauses (`GC_MEAN = 0.8–1.0`) directly into 50–100 tenant bursts.

### Applied Fixes
1. Replaced `dirtyTenantCount()` write-path scan with lock-free `MemoryBudget.dirtyTenants` (`atomic.Int64`) updated on `Timeline` dirty transitions (`dirtyTracked atomic.Bool`) and reconciled in `runMaintenanceStep()`.
2. Gated `debug.FreeOSMemory()` behind `highPressure` (`2s` cooldown) or `unreleasedIdle > max(32 MiB, MemoryLimitBytes/4)` (`5s` cooldown).

---

## 2. Baseline Matrix (Before Phase 24 Fixes)

Command: `go test -v ./internal/pageserver -run '^TestPhase24_BenchmarkMatrix$' -count=1 -timeout 300s`

| Config | Tenants | Runs | P99 Mean (µs) | P99 Median (µs) | P99 StdDev (µs) | P99 Min (µs) | P99 Max (µs) | P99 CV (%) | Pooled p50 (µs) | Pooled p95 (µs) | Pooled p99 (µs) | Pooled p99.9 (µs) | Tput Mean (MB/s) | GC Mean |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `A_disabled` | 1 | 10 | 31.0 | 21.5 | 27.6 | 15.0 | 111.0 | 89.06 | 3.0 | 20.0 | 45.0 | 111.0 | 107.79 | 0.0 |
| `B_unadaptive_cawal` | 1 | 10 | 20.5 | 21.0 | 4.3 | 15.0 | 27.0 | 20.95 | 3.0 | 16.0 | 25.0 | 27.0 | 83.12 | 0.0 |
| `C_adaptive_cawal` | 1 | 10 | 21.3 | 17.0 | 7.6 | 15.0 | 40.0 | 35.57 | 3.0 | 16.0 | 25.0 | 40.0 | 84.40 | 0.0 |
| `A_disabled` | 8 | 10 | 4059.7 | 4714.0 | 1934.1 | 855.0 | 6570.0 | 47.64 | 5.0 | 322.0 | 4351.0 | 6933.0 | 173.22 | 0.0 |
| `B_unadaptive_cawal` | 8 | 10 | 1097.2 | 1130.5 | 397.0 | 29.0 | 1682.0 | 36.18 | 5.0 | 177.0 | 1200.0 | 1682.0 | 157.99 | 0.0 |
| `C_adaptive_cawal` | 8 | 10 | 1155.6 | 1140.0 | 111.1 | 933.0 | 1364.0 | 9.61 | 4.0 | 264.0 | 1165.0 | 1393.0 | 163.02 | 0.0 |
| `A_disabled` | 20 | 10 | 5120.4 | 3330.5 | 3845.9 | 1121.0 | 13385.0 | 75.11 | 6.0 | 888.0 | 3817.0 | 14129.0 | 162.16 | 0.0 |
| `B_unadaptive_cawal` | 20 | 10 | 2265.0 | 2253.0 | 660.5 | 821.0 | 3251.0 | 29.16 | 7.0 | 784.0 | 2431.0 | 3386.0 | 162.59 | 0.0 |
| `C_adaptive_cawal` | 20 | 10 | 2814.3 | 2275.5 | 1701.5 | 1079.0 | 6918.0 | 60.46 | 8.0 | 943.0 | 2985.0 | 6918.0 | 135.34 | 0.1 |
| `A_disabled` | 30 | 10 | 3299.0 | 3543.5 | 960.8 | 1772.0 | 4453.0 | 29.12 | 6.0 | 1122.0 | 3567.0 | 5301.0 | 169.93 | 0.0 |
| `B_unadaptive_cawal` | 30 | 10 | 3510.6 | 3683.5 | 1048.0 | 1727.0 | 4682.0 | 29.85 | 8.0 | 1355.0 | 3728.0 | 5345.0 | 145.60 | 0.0 |
| `C_adaptive_cawal` | 30 | 10 | 2788.2 | 2741.0 | 794.0 | 1822.0 | 4350.0 | 28.48 | 8.0 | 1120.0 | 2895.0 | 4841.0 | 166.71 | 0.0 |
| `A_disabled` | 50 | 20 | 16297.4 | 11860.5 | 12348.8 | 4033.0 | 43258.0 | 75.77 | 8.0 | 2719.0 | 18093.0 | 42327.0 | 165.33 | 0.8 |
| `B_unadaptive_cawal` | 50 | 20 | 5715.1 | 4951.5 | 2143.6 | 3916.0 | 13679.0 | 37.51 | 9.0 | 1896.0 | 5358.0 | 9918.0 | 163.33 | 0.0 |
| `C_adaptive_cawal` | 50 | 20 | 5244.3 | 5274.0 | 975.0 | 3582.0 | 7539.0 | 18.59 | 9.0 | 2034.0 | 5319.0 | 8138.0 | 174.85 | 0.0 |
| `A_disabled` | 75 | 10 | 21855.2 | 11858.5 | 20393.4 | 8554.0 | 80273.0 | 93.31 | 9.0 | 3223.0 | 18820.0 | 80273.0 | 162.56 | 1.0 |
| `B_unadaptive_cawal` | 75 | 10 | 17826.4 | 12324.0 | 9446.4 | 9076.0 | 32084.0 | 52.99 | 9.0 | 2880.0 | 18794.0 | 37892.0 | 168.64 | 0.1 |
| `C_adaptive_cawal` | 75 | 10 | 10611.3 | 10302.0 | 1614.4 | 8518.0 | 13397.0 | 15.21 | 11.0 | 2639.0 | 10800.0 | 16323.0 | 164.60 | 0.1 |
| `A_disabled` | 100 | 10 | 28261.8 | 22617.0 | 12915.3 | 14683.0 | 52535.0 | 45.70 | 10.0 | 5533.0 | 27622.0 | 52535.0 | 156.76 | 1.0 |
| `B_unadaptive_cawal` | 100 | 10 | 18293.5 | 16872.5 | 4325.8 | 13940.0 | 28272.0 | 23.65 | 12.0 | 3419.0 | 18238.0 | 30515.0 | 161.31 | 0.9 |
| `C_adaptive_cawal` | 100 | 10 | 30264.5 | 23402.0 | 16380.5 | 15317.0 | 65669.0 | 54.12 | 10.0 | 4199.0 | 32102.0 | 67510.0 | 156.18 | 1.0 |

---

## 3. Post-Fix Matrix (After Lock-Free Dirty Counter & FreeOSMemory Gating)

| Config | Tenants | Runs | P99 Mean (µs) | P99 Median (µs) | P99 StdDev (µs) | P99 Min (µs) | P99 Max (µs) | P99 CV (%) | Pooled p50 (µs) | Pooled p95 (µs) | Pooled p99 (µs) | Pooled p99.9 (µs) | Tput Mean (MB/s) | GC Mean |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `A_disabled` | 1 | 10 | 20.4 | 19.0 | 4.6 | 15.0 | 31.0 | 22.48 | 3.0 | 17.0 | 24.0 | 31.0 | 124.47 | 0.0 |
| `B_unadaptive_cawal` | 1 | 10 | 17.1 | 16.0 | 3.7 | 15.0 | 28.0 | 21.64 | 3.0 | 15.0 | 17.0 | 28.0 | 78.20 | 0.0 |
| `C_adaptive_cawal` | 1 | 10 | 21.0 | 16.0 | 8.9 | 15.0 | 42.0 | 42.54 | 3.0 | 15.0 | 28.0 | 42.0 | 91.92 | 0.0 |
| `A_disabled` | 8 | 10 | 306.7 | 42.0 | 417.5 | 28.0 | 1082.0 | 136.11 | 4.0 | 19.0 | 224.0 | 1235.0 | 178.36 | 0.0 |
| `B_unadaptive_cawal` | 8 | 10 | 183.6 | 38.0 | 382.9 | 29.0 | 1323.0 | 208.56 | 4.0 | 18.0 | 51.0 | 1323.0 | 158.25 | 0.0 |
| `C_adaptive_cawal` | 8 | 10 | 191.6 | 45.0 | 317.5 | 29.0 | 1100.0 | 165.70 | 5.0 | 22.0 | 88.0 | 1100.0 | 159.17 | 0.0 |
| `A_disabled` | 20 | 10 | 720.9 | 134.0 | 1565.7 | 22.0 | 5339.0 | 217.18 | 5.0 | 20.0 | 345.0 | 5339.0 | 176.75 | 0.0 |
| `B_unadaptive_cawal` | 20 | 10 | 82.2 | 34.5 | 94.4 | 21.0 | 296.0 | 114.82 | 4.0 | 13.0 | 39.0 | 571.0 | 173.36 | 0.0 |
| `C_adaptive_cawal` | 20 | 10 | 140.9 | 48.5 | 168.3 | 23.0 | 590.0 | 119.46 | 4.0 | 15.0 | 159.0 | 771.0 | 174.91 | 0.0 |
| `A_disabled` | 30 | 10 | 672.0 | 191.5 | 887.6 | 25.0 | 2813.0 | 132.08 | 4.0 | 16.0 | 561.0 | 2875.0 | 181.40 | 0.0 |
| `B_unadaptive_cawal` | 30 | 10 | 967.9 | 843.0 | 774.8 | 23.0 | 2657.0 | 80.05 | 4.0 | 22.0 | 1057.0 | 2657.0 | 183.75 | 0.0 |
| `C_adaptive_cawal` | 30 | 10 | 246.3 | 35.0 | 394.7 | 19.0 | 1272.0 | 160.26 | 4.0 | 12.0 | 127.0 | 1272.0 | 177.15 | 0.0 |
| `A_disabled` | 50 | 20 | 1346.4 | 839.0 | 1410.4 | 17.0 | 4530.0 | 104.76 | 4.0 | 20.0 | 1352.0 | 4342.0 | 184.24 | 0.0 |
| `B_unadaptive_cawal` | 50 | 20 | 958.7 | 302.0 | 1074.5 | 25.0 | 2989.0 | 112.08 | 4.0 | 20.0 | 1045.0 | 3275.0 | 164.43 | 0.0 |
| `C_adaptive_cawal` | 50 | 20 | 1273.5 | 947.0 | 1034.3 | 23.0 | 3384.0 | 81.21 | 4.0 | 19.0 | 1366.0 | 3510.0 | 170.14 | 0.0 |
| `A_disabled` | 75 | 10 | 1138.1 | 640.5 | 1244.0 | 25.0 | 3197.0 | 109.31 | 4.0 | 18.0 | 976.0 | 4522.0 | 169.64 | 0.0 |
| `B_unadaptive_cawal` | 75 | 10 | 885.6 | 96.0 | 1107.9 | 18.0 | 2830.0 | 125.10 | 4.0 | 16.0 | 327.0 | 3824.0 | 176.46 | 0.0 |
| `C_adaptive_cawal` | 75 | 10 | 1394.4 | 1032.0 | 1409.7 | 25.0 | 3755.0 | 101.10 | 4.0 | 19.0 | 1255.0 | 4104.0 | 178.96 | 0.0 |
| `A_disabled` | 100 | 10 | 4926.4 | 4705.0 | 3381.4 | 21.0 | 10981.0 | 68.64 | 4.0 | 20.0 | 4729.0 | 13411.0 | 163.30 | 1.0 |
| `B_unadaptive_cawal` | 100 | 10 | 4211.4 | 5038.0 | 2529.4 | 49.0 | 6954.0 | 60.06 | 4.0 | 36.0 | 5077.0 | 7951.0 | 165.72 | 0.0 |
| `C_adaptive_cawal` | 100 | 10 | 3168.0 | 3877.0 | 2238.7 | 25.0 | 6165.0 | 70.66 | 4.0 | 19.0 | 3873.0 | 7086.0 | 172.33 | 0.0 |

### Key Takeaways
- **50 tenants (20 runs)**: `A_disabled` mean p99 dropped **12.1x** (`16,297.4 µs -> 1,346.4 µs`, max `43,258 µs -> 4,530 µs`), `B_unadaptive_cawal` dropped **6.0x** (`5,715.1 µs -> 958.7 µs`), and `C_adaptive_cawal` dropped **4.1x** (`5,244.3 µs -> 1,273.5 µs`, max `7,539 µs -> 3,384 µs`).
- **75–100 tenants**: `A_disabled` 75T mean p99 dropped **19.2x** (`21,855.2 µs -> 1,138.1 µs`, max `80,273 µs -> 3,197 µs`), and `C_adaptive_cawal` 100T mean p99 dropped **9.6x** (`30,264.5 µs -> 3,168.0 µs`, max `65,669 µs -> 6,165 µs`).
- Note on remaining CV%: Because fast batches complete in `4–20 µs` while occasional first-batch `SetTenantGeneration` `p.mu.Lock()` calls take `1–4 ms`, relative `CV%` remains `60–217%` even though **absolute** p99 latency and worst-case max latency dropped by an order of magnitude (`1–5 ms` vs `16–80 ms`).
