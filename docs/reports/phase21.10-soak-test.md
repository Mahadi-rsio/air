# AIR Phase 21.10 — Full Topology Stability Soak Test Report

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Base Commit:** `803c0fd9f0cab7c2c5b0b85c3898f901fd264d4f` (+ Phase 21.10 fixes)  
**Stack:** `air-gateway:phase21.10` (`256 MB`) → `air-server:phase21.10` (`512 MB`, `2` vCPU, PG `17.12` + `air_smgr.so`) → `air-pageserver:phase21.10` (`256 MB`) + `air-read-engine:phase21.10` (`256 MB`, **enabled**).  
**Status:** **PARTIAL** (5-minute full-topology soak **PASS**; 15-minute & 30-minute uninterrupted soaks **NOT TESTED** due to 600s per-command runner limit)

---

## 1. 5-Minute Full-Topology Smoke Soak (`300s`, Read Engine Enabled)

- **Command:** `go run ./benchmarks/phase21 --mode=soak --duration=300s --concurrency=16 --tenant-prefix=p2110_5m`
- **Total Ops:** `95,443` | **Avg TPS:** `318.1`
- **Latency:** `p50 = 46.54 ms`, `p95 = 90.23 ms`, `p99 = 123.47 ms`
- **Errors:** **`0`** (eliminated all 177 `SQLSTATE 42000: no such table` errors from Phase 21!)
- **Cell (`512 MB`) Memory:** `current = 503.90 MB`, `peak = 512.02 MB`, `anon = 87.85 MB` (flat `~87.9–93.8 MB`), `file = 378.40 MB`, `high_events = 0`, `oom_kills = 0`
- **Process RSS (`/proc/<pid>/status` `VmRSS`):**
  - `pg_rss_mb`: `1373.50 MB` (sum across pooled PG17 backends; includes shared `shared_buffers` per process)
  - `gw_rss_mb`: `24.65 MB` (`41.8–42.2 MB` brief GC spikes)
  - `ps_rss_mb`: `231.61 MB`
  - `re_rss_mb`: `27.17 MB`
- **Raw CSV:** `benchmarks/phase21/results/soak_p2110_5m_soak_300s.csv`

---

## 2. 15-Minute (`900s`) and 30-Minute (`1800s`) Soak Tests (`NOT TESTED`)

- **Status:** **NOT TESTED** — The interactive VM runner enforces a `600s` per-command execution timeout, preventing uninterrupted `900s` and `1800s` single-process runs.
- **Reproducible Commands (for non-time-capped CI):**
  ```bash
  go run ./benchmarks/phase21 --mode=soak --duration=900s --concurrency=16 --tenant-prefix=p2110_15m
  go run ./benchmarks/phase21 --mode=soak --duration=1800s --concurrency=16 --tenant-prefix=p2110_30m
  ```
- **Additional Observation:** After accumulating 8 active test tenants' `InMemoryLayer`s across sequential runs on the same `256 MB` `air-pageserver` instance, `air-pageserver` hit its `256 MB` memory limit (`OOMKilled=true, ExitCode=137`). Multi-tenant `256 MB` Pageserver deployments require tighter `defaultMaxInMemBytes` / eviction budgets when hosting >4 concurrent active tenants.
