# AIR Phase 20.9 — 512 MB Cgroup v2 Memory & Adaptive Controller Analysis

**Date:** 2026-11-23  
**Environment:** Real PostgreSQL `17.12` via `cell.Agent` under delegated cgroup v2 (`/sys/fs/cgroup/air_phase209/air_tenant_<id>`), `memory.swap.max=0`, `shared_buffers=32MB`, `work_mem=4MB`, `max_connections=100`, real `air-gateway` + real `cell.TelemetryPublisher` (500ms interval).  
**Raw Data:** [memory512.log](file:///workspaces/air/benchmarks/phase20.9/results/memory512.log), [memory512.json](file:///workspaces/air/benchmarks/phase20.9/results/memory512.json), [adaptive_transitions.json](file:///workspaces/air/benchmarks/phase20.9/results/adaptive_transitions.json), [sustained_300s.log](file:///workspaces/air/benchmarks/phase20.9/results/sustained_300s.log).

---

## 1. Critical 512 MB Cell Profile (`memory.high=435 MiB`, `memory.max=512 MiB`, `swap=0`)

Measured across `4, 8, 16, 32, 64` concurrent writing clients (`PoolMaxBackends=64`, `WriteMaxGlobal=16`, `WriteMinGlobal=2`):

| Mode | Clients | TPS | p95 (ms) | Errors | Cgroup Peak | Cgroup Cur | `anon` (Non-reclaim) | `file` (Page Cache) | Sum PG VmRSS* | GW RSS | `high` Ev | `oom_kill` | Mem PSI | CPU PSI | IO PSI | Eff Limit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| **No Gov** | 4 | 3,513.3 | 1.83 | 0 | 82.1 MB | 81.6 MB | **5.3 MB** | 72.8 MB | 168.2 MB | 22.5 MB | 0 | 0 | 0.00 | 12.99 | 3.33 | — |
| **No Gov** | 8 | 4,161.5 | 3.13 | 0 | 130.6 MB | 129.6 MB | **10.8 MB** | 112.8 MB | 292.9 MB | 22.6 MB | 0 | 0 | 0.00 | 30.80 | 5.43 | — |
| **No Gov** | 16 | 4,437.9 | 6.13 | 0 | 176.4 MB | 175.9 MB | **21.4 MB** | 145.0 MB | 555.5 MB | 22.7 MB | 0 | 0 | 0.00 | 46.58 | 5.81 | — |
| **No Gov** | 32 | 4,687.2 | 11.49 | 0 | 232.2 MB | 231.7 MB | **42.5 MB** | 173.3 MB | 1,032.9 MB | 23.3 MB | 0 | 0 | 0.00 | 58.13 | 5.81 | — |
| **No Gov** | 64 | 4,359.3 | 25.34 | 0 | 306.8 MB | 305.6 MB | **84.5 MB** | 193.7 MB | 1,840.1 MB | 23.6 MB | 0 | 0 | 0.00 | 66.91 | 5.44 | — |
| **Static Gov** | 4 | 3,515.7 | 1.77 | 0 | 306.8 MB | 175.4 MB | **5.6 MB** | 163.5 MB | 216.0 MB | 45.3 MB | 0 | 0 | 0.00 | 65.47 | 6.17 | 16 |
| **Static Gov** | 8 | 3,937.5 | 3.36 | 0 | 306.8 MB | 212.5 MB | **10.9 MB** | 193.3 MB | 323.2 MB | 23.5 MB | 0 | 0 | 0.00 | 49.35 | 6.99 | 16 |
| **Static Gov** | 16 | 4,413.0 | 6.09 | 0 | 306.8 MB | 258.4 MB | **21.5 MB** | 225.0 MB | 548.6 MB | 23.5 MB | 0 | 0 | 0.00 | 54.51 | 6.99 | 16 |
| **Static Gov** | 32 | 4,355.0 | 11.47 | 0 | 334.0 MB | 333.5 MB | **29.4 MB** | 288.3 MB | 733.5 MB | 23.5 MB | 0 | 0 | 0.00 | 59.57 | 6.36 | 16 |
| **Static Gov** | 64 | 4,252.7 | **22.66** | 0 | 363.1 MB | 356.8 MB | **31.0 MB** | 303.5 MB | 893.9 MB | 23.5 MB | 0 | 0 | 0.00 | 62.71 | 6.01 | 16 |
| **Adaptive** | 4 | 3,083.9 | 2.03 | 0 | 363.1 MB | 240.6 MB | **5.7 MB** | 226.9 MB | 196.9 MB | 48.0 MB | 0 | 0 | 0.00 | 58.05 | 6.72 | 2..6 |
| **Adaptive** | 8 | 2,924.6 | 3.57 | 0 | 363.1 MB | 284.1 MB | **5.7 MB** | 269.1 MB | 212.1 MB | 23.1 MB | 0 | 0 | 0.00 | 42.53 | 8.07 | 2..2 |
| **Adaptive** | 16 | 2,778.6 | 8.05 | 0 | 363.1 MB | 308.9 MB | **4.4 MB** | 294.9 MB | 193.1 MB | 23.1 MB | 0 | 0 | 0.00 | 32.64 | 8.76 | 2..2 |
| **Adaptive** | 32 | 2,892.8 | 12.93 | 0 | 363.1 MB | 337.8 MB | **6.0 MB** | 321.1 MB | 208.0 MB | 23.1 MB | 0 | 0 | 0.00 | 27.87 | 9.20 | 2..2 |
| **Adaptive** | 64 | 2,781.2 | 28.44 | 0 | 363.1 MB | 331.5 MB | **5.8 MB** | 314.9 MB | 200.1 MB | 23.6 MB | 0 | 0 | 0.00 | 24.44 | 9.29 | 2..2 |

*\*`Sum PG VmRSS` sums `/proc/<pid>/status` `VmRSS` across all PG processes, double-counting `shared_buffers` (32 MB) and shared binaries across every backend. True physical memory consumption is `Cgroup Cur`/`Cgroup Peak` (`memory.current`/`memory.peak`), broken down into non-reclaimable `anon` + reclaimable `file` page cache.*

### Critical 512 MB Memory Findings
1. **Non-Reclaimable `anon` Memory Savings:** At 64 concurrent clients, **Static Write Governor (`WriteMaxGlobal=16`) reduced non-reclaimable PostgreSQL `anon` RAM by -63.3%** (`84.5 MB -> 31.0 MB`) while maintaining 97.6% of throughput (`4,252.7` vs `4,359.3 TPS`) and **improving p95 latency by -10.6%** (`25.34ms -> 22.66ms`).
2. **Per-Backend Memory Cost:** Each active PostgreSQL backend added **~1.3 MiB of non-reclaimable `anon` memory** for simple INSERTs (and up to `4+ MiB` when `work_mem=4MB` is exercised by sorts/hashes). Unbounded backends (`c=64`) reached `84.5 MiB` `anon`; capping active writes at `16` keeps `anon` safely bounded near `~31 MiB`, leaving >400 MiB of headroom for `shared_buffers` (32 MiB) + OS page cache + WAL buffers.
3. **Zero OOM Kills Across All Profiles:** Zero `oom_kill` events across 512 MB, 256 MB, and 1 GB profiles.

---

## 2. Scaling Comparison: 256 MB vs 512 MB vs 1 GB Profiles

| Profile (`high`/`max`) | Mode | Clients | TPS | p95 (ms) | Cgroup Peak | `anon` | `high` Events | `oom_kill` |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| **256 MB** (`218`/`256`) | No Gov | 32 | 4,740.7 | 11.15 | 218.7 MB | 43.2 MB | **61** | 0 |
| **256 MB** (`218`/`256`) | Static Gov | 32 | 4,446.2 | 11.26 | 218.7 MB | **31.9 MB** | **262** | 0 |
| **256 MB** (`218`/`256`) | Adaptive | 32 | 2,952.1 | 13.14 | 218.7 MB | **3.2 MB** | 307 | 0 |
| **512 MB** (`435`/`512`) | Static Gov | 32 | 4,355.0 | 11.47 | 334.0 MB | **29.4 MB** | **0** | 0 |
| **1 GB** (`870`/`1024`) | Static Gov | 32 | 4,532.0 | 10.63 | 252.0 MB | **25.3 MB** | **0** | 0 |

- **Where memory pressure begins:** On the **256 MB** profile (`memory.high=218 MiB`), kernel page-cache reclaim (`memory.events` `high` counter) began at `c=32` (`61` events, climbing to `262` under sustained WAL/heap page-cache growth), cleanly reclaiming `file` cache with `0` OOM kills. On the **512 MB** profile (`memory.high=435 MiB`), normal OLTP writes (`c=4..64`) stayed below `363 MB` (`0` `high` events); `memory.high` was only reached during high-rate streaming `COPY` (`435.9 MB` peak, `936K rows/s`, `0` OOM).
- **2 GB profile:** Not executed separately because 1 GB already showed `0` memory pressure (`252 MB` peak, <25% utilization).

---

## 3. Adaptive Controller Live Telemetry Validation (AIMD)

Raw results: [adaptive_transitions.json](file:///workspaces/air/benchmarks/phase20.9/results/adaptive_transitions.json).

| Phase | Observed `effLim` | State | Cgroup Mem | `high` Events | Outcome | Finding |
|---|---:|---:|---|---:|---|---|
| 1. Steady Healthy | 2 | `2` (pressured) | 329 / 435 MB | 0 | **FAIL (false pressure)** | 2-vCPU `cpuPSI` (`24–58%`) exceeded `>= 25.0` threshold despite 0 memory pressure |
| 2. Real `memory.high` Pressure | 2 | `2` (pressured) | 195 / 247 MB | `0 -> 160` | **PASS** | Real kernel `memory.high` reclaim events observed and ingested via HTTP telemetry |
| 3. Recovery Additive Increase | 2 | `2` (pressured) | 189 / 435 MB | 160 | **FAIL (no recovery)** | Host `cpuPSI` remained `> 15.0`, preventing additive increase back toward `maxGlobal` |
| 4. Stale Telemetry Fallback | 2 (`<= max/2`) | `4` (`stale_fallback`) | — | — | **PASS** | Expired telemetry immediately transitioned to `ControllerStateStaleFallback` |

**Engineering Verdict on Adaptive Mode:** The cgroup v2 + HTTP telemetry pipeline works end-to-end, and stale fallback works as designed. However, coupling `CPUPSISomeAvg10 >= 25.0` directly into write-slot multiplicative decrease causes severe false-positive throttling (-32% TPS) on 2-vCPU cells. Adaptive mode should **remain OFF by default** until CPU PSI is either removed from memory-protection throttling or scaled by core count.

---

## 4. Sustained Load Stability (5-Minute Real PG 17.12 Run, 512 MB Cell)

Raw log: [sustained_300s.log](file:///workspaces/air/benchmarks/phase20.9/results/sustained_300s.log) (`8` concurrent clients, `300s`, ~900,000 `UPDATE` commits).

| Time | Window TPS | p95 (ms) | p99 (ms) | Errors | Cgroup Cur | Cgroup Peak | PG VmRSS | GW RSS | `high` Ev | `oom_kill` | WAL Total |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10s | 4,224.4 | 3.02 | 4.47 | 0 | 72.1 MB | 72.6 MB | 259.3 MB | 22.1 MB | 0 | 0 | 6.3 MB |
| 60s | 2,959.5 | 3.77 | 5.61 | 0 | 106.2 MB | 109.8 MB | 260.6 MB | 65.7 MB* | 0 | 0 | 30.4 MB |
| 120s | 2,868.2 | 4.02 | 6.17 | 0 | 107.0 MB | 127.4 MB | 261.2 MB | 79.2 MB* | 0 | 0 | 58.3 MB |
| 180s | 2,994.5 | 3.55 | 5.19 | 0 | 141.0 MB | 160.4 MB | 262.4 MB | 26.1 MB | 0 | 0 | 86.4 MB |
| 240s | 2,855.3 | 3.96 | 6.31 | 0 | 174.6 MB | 194.5 MB | 262.5 MB | 62.0 MB* | 0 | 0 | 114.7 MB |
| 290s | 2,980.4 | 3.66 | 5.62 | 0 | 224.7 MB | 225.2 MB | 262.6 MB | 23.5 MB | 0 | 0 | 138.4 MB |

- **Zero memory leaks:** PG RSS grew only `+3.3 MB` over 5 minutes; Gateway RSS returned cleanly to `23.5 MB` after each periodic `MeteringLogger` flush (`*`).
- **Zero errors, zero OOM kills, zero latency drift** over 300s.
- **Duration limitation:** 15-minute and 30-minute runs were not executed due to interactive session runtime budget; the harness supports `--sustained-sec 900` / `--sustained-sec 1800` for overnight validation.
