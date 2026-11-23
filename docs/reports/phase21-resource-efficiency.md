# AIR Phase 21.7 — Resource Efficiency & Adaptive Controller Report

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Stack:** `air-gateway:phase21` (256 MB) → `air-server:phase21` (`256m` / `512m` / `1024m` / `2048m`, 2 vCPU, PG 17.12 + `air_smgr.so`) → `air-pageserver:phase21` (256 MB).  
**Workload:** `go run ./benchmarks/phase21 --mode=soak --duration=60s --concurrency=16`

---

## 1. Memory Profile Comparison (Real Measurements)

| Cell Profile | Total Ops (60s) | Avg TPS | Errors | p50 (ms) | p95 (ms) | p99 (ms) | Cell `current` (MB) | Cell `peak` (MB) | Cell `anon` (MB) | Cell `file` Cache (MB) | `oom_kills` |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **256 MB** | 19,264 | 321.1 | 0 | 47.44 | 84.29 | 109.98 | 243.5 | 256.0 | 36.0 | 191.1 | 0 |
| **512 MB** | 18,399 | 306.6 | 0 | 49.27 | 91.16 | 119.06 | 247.6 | 255.9 | 39.3 | 191.8 | 0 |
| **1 GB (1024 MB)** | 18,882 | 314.7 | 0 | 48.87 | 85.29 | 108.55 | 244.9 | 256.5 | 37.0 | 191.8 | 0 |
| **2 GB (2048 MB)** | 17,299 | 288.3 | 0 | 53.13 | 94.09 | 122.04 | 244.2 | 253.9 | 36.8 | 191.1 | 0 |

- **Gateway memory** across all profiles: `9.1–11.0 MB`.
- **Pageserver memory** across all profiles: `86.6–168.2 MB`.
- **Key observation:** With `shared_buffers = 32MB` and `WriteMaxGlobal = 16` (`WriteMaxPerTenant = 8`), PostgreSQL + `air-server` non-reclaimable `anon` memory stays tightly bounded at **`36.0–40.3 MB`**. Total Cell memory stabilizes around `~245–257 MB` during 60s mixed workloads (and `~473–508 MB` over 5–10 min as Linux page cache grows up to `memory.max`). On `256 MB`, `peak` reached `256.0 MB` (100% of limit, leaving minimal headroom for multi-tenant bursts), whereas `512 MB` provides comfortable headroom for page cache and multi-tenant concurrency.

---

## 2. Phase 21.8 — Adaptive Controller Follow-up (Real Measurements)

Modified `internal/gateway/adaptive_controller.go` so `CPUPSISomeAvg10` is evaluated as an independent CPU-starvation signal (`>= 80.0%` pressured, `< 65.0%` healthy) rather than treating normal 2-vCPU OLTP CPU PSI (`25–65%`) as memory OOM risk.

| Controller Mode (512 MB Cell, `c=16`, 60s) | Total Ops | Avg TPS | Errors | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|
| **Static Governor** (`Adaptive=false`) | 18,399 | 306.6 | 0 | 49.27 | 91.16 | 119.06 |
| **Recalibrated Adaptive** (`Adaptive=true`) | 18,569 | 309.5 | 0 | 49.04 | 87.48 | 114.79 |

- Unlike Phase 20.9 (where CPU PSI miscalibration degraded TPS by `-32% to -39%`), the recalibrated `AdaptiveController` no longer false-triggers on healthy 2-vCPU workloads and slightly outperformed Static (`309.5` vs `306.6 TPS`, `p95 87.48ms` vs `91.16ms`).
- Per policy, `AIR_WRITE_ADAPTIVE_ENABLED=false` remains the production default until validated under sustained multi-tenant memory-pressure transitions.

---

## 3. Status

**Phase 21.7 & 21.8 Verdict: PASSED (Real Measurements).**
