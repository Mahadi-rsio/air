# AIR Phase 24 — Phase 3: CPU Scheduler Stability

- **Command**: `go test -v ./internal/pageserver -run '^TestPhase24_SchedulerStability$' -count=1 -timeout 180s`
- **Workload**: `50` concurrent tenants, `512 MB` profile, `10` repeated runs per `(GOMAXPROCS, Mode)`.
- **Hardware**: `2` physical vCPUs (`linux/amd64`), `go1.27.3`.

---

## 1. Before vs After Scheduler Metrics (`GOMAXPROCS = 1, 2, 4`)

### Baseline (Before Phase 24 Fixes)

| GOMAXPROCS | Mode | P99 Mean (µs) | P99 Median (µs) | P99 StdDev (µs) | P99 Max (µs) | P99 CV (%) | `/sched/latencies` p50 (ms) | `/sched/latencies` p99 (ms) | Δ GC CPU (s) | Δ User CPU (s) | Tput (MB/s) |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | `A_disabled` | 24.2 | 23.0 | 4.6 | 31.0 | 19.10 | 0.000 | 67.109 | 0.0303 | 0.7238 | 132.16 |
| 1 | `C_adaptive_cawal` | 19.5 | 18.0 | 3.6 | 27.0 | 18.53 | 0.000 | 67.109 | 0.0293 | 0.8073 | 129.21 |
| 2 | `A_disabled` | 11455.7 | 9282.5 | 5083.9 | 19082.0 | 44.38 | 0.000 | 67.109 | 0.0636 | 1.1464 | 175.86 |
| 2 | `C_adaptive_cawal` | 5091.8 | 4730.0 | 1030.2 | 6874.0 | 20.23 | 0.000 | 50.332 | 0.0546 | 0.9171 | 176.83 |
| 4 | `A_disabled` | 5313.6 | 5299.5 | 1198.8 | 7440.0 | 22.56 | 0.000 | 25.166 | 0.0724 | 1.5698 | 177.63 |
| 4 | `C_adaptive_cawal` | 5028.5 | 4820.0 | 842.0 | 6423.0 | 16.74 | 0.001 | 8.389 | 0.0766 | 1.5946 | 159.65 |

### Post-Fix (After Lock-Free Dirty Counter & FreeOSMemory Gating)

| GOMAXPROCS | Mode | P99 Mean (µs) | P99 Median (µs) | P99 StdDev (µs) | P99 Max (µs) | P99 CV (%) | `/sched/latencies` p50 (ms) | `/sched/latencies` p99 (ms) | Δ GC CPU (s) | Δ User CPU (s) | Tput (MB/s) |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | `A_disabled` | 16.7 | 14.0 | 7.1 | 38.0 | 42.60 | 0.000 | 4.194 | 0.0322 | 0.8283 | 134.10 |
| 1 | `C_adaptive_cawal` | 16.0 | 15.0 | 2.0 | 21.0 | 12.81 | 0.000 | 5.243 | 0.0330 | 0.7983 | 131.04 |
| 2 | `A_disabled` | 914.8 | 269.5 | 1147.1 | 3095.0 | 125.39 | 0.000 | 5.243 | 0.0530 | 0.9948 | 186.15 |
| 2 | `C_adaptive_cawal` | 1978.7 | 1874.5 | 1371.0 | 4196.0 | 69.29 | 0.000 | 5.243 | 0.0517 | 0.9665 | 179.76 |
| 4 | `A_disabled` | 1934.3 | 2276.5 | 1479.0 | 4183.0 | 76.46 | 0.000 | 4.194 | 0.0750 | 1.5659 | 183.82 |
| 4 | `C_adaptive_cawal` | 1404.2 | 1382.5 | 606.9 | 2531.0 | 43.22 | 0.000 | 4.194 | 0.0603 | 1.2746 | 188.74 |

---

## 2. Profile Summary & Scheduler Analysis

- **Baseline Profile (`go tool pprof` on `TestPhase24_BenchmarkMatrix`)**:
  - **CPU (`16.09s` samples)**: `Syscall6` (`32.26%`, file I/O + fsync), `sha256.blockSHANI` (`10.69%`), `EncodeLayerV2ToWriter` (`23.62%` cum), `Timeline.InMemBytes` (`3.48%` cum), `InMemoryLayer.SnapshotAll` (`3.17%` cum).
  - **Mutex (`102.01s` total delay)**: `Pageserver.SetTenantGeneration` (`96.24%`, `98.18s`), `Timeline.ApplyWALRecord` (`2.15%`, `2.20s`), `Pageserver.dirtyTenantCount` (`1.01%`, `1.03s`).
  - **Block (`481.28s` total delay)**: `MemoryBudget.acquireFlushSlot` (`61.45%`, `295.74s`), `Pageserver.IngestWALRecords` (`22.36%`, `107.63s`), `SetTenantGeneration` (`20.25%`, `97.44s`), `dirtyTenantCount` (`1.89%`, `9.09s`).
- **`GOMAXPROCS` Behavior**:
  - At `GOMAXPROCS=1`, goroutines execute serially with zero cross-core `p.mu` lock bouncing (`16.0–16.7 µs` p99), at ~28% lower throughput (`131–134 MB/s` vs `180–186 MB/s` at `GOMAXPROCS=2`).
  - At `GOMAXPROCS=2` (matching Fly.io `shared-cpu-2x`), post-fix Go scheduler run-queue latency (`/sched/latencies` p99) dropped **12.8x** (`67.109 ms -> 5.243 ms`), and `A_disabled` p99 dropped **12.5x** (`11,455.7 µs -> 914.8 µs`).
  - Increasing to `GOMAXPROCS=4` on a 2-vCPU host increased user CPU (`0.97s -> 1.27–1.57s`) with no throughput gain over `GOMAXPROCS=2`; therefore **`GOMAXPROCS=2` (or default matching vCPU count) remains optimal**.
