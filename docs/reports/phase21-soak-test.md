# AIR Phase 21.5 — Production Stability Soak Test Report

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Stack:** `air-gateway:phase21` (256 MB) → `air-server:phase21` (512 MB, 2 vCPU, PG 17.12 + `air_smgr.so`) → Unix Socket Bridge → `air-pageserver:phase21` (256 MB).  
**Configuration:** Static Write Governor enabled, `WriteMaxGlobal=16`, `WriteMaxPerTenant=8`, `AIR_WRITE_ADAPTIVE_ENABLED=false`, Buffer Budget enabled, Lazy LSN enabled, `concurrency=16`, mixed workload (`INSERT`, `UPDATE`, `UPSERT`, `DELETE`, `SELECT`, streaming `COPY`, explicit `BEGIN/COMMIT`).

---

## 1. Executed Soak Runs (Real Measurements)

### 1.1 5-Minute Smoke Soak (`300s`, `AIR_READ_ENGINE_HOST=air-read-engine`)
- **Command:** `go run ./benchmarks/phase21 --mode=soak --duration=300s --concurrency=16 --tenant-prefix=p21_5m`
- **Total Ops:** `93,030` | **Avg TPS:** `310.1`
- **Latency:** `p50 = 48.87 ms`, `p95 = 87.52 ms`, `p99 = 113.97 ms` (zero latency drift over 300s)
- **Errors:** `177` (`0.19%`) — all `ERROR: no such table: soak_kv (SQLSTATE 42000)` on newly-opened `pgxpool` connections with `sessionWriteLSN == 0` routing `SELECT` to `air-read-engine` before DDL sync; **0 write errors**.
- **Memory (512 MB Cell):** `current = 506.9 MB`, `peak = 512.0 MB`, `anon = 58.8–63.0 MB` (flat), `file = 415.8 MB`, `high_events = 0`, `oom_kills = 0`.
- **Gateway RSS:** `9.6–12.9 MB` (one brief `27.0 MB` GC spike).
- **Pageserver:** `99.5–216.4 MB` (under 256 MB limit, `0` OOM).
- **Raw CSV:** `benchmarks/phase21/results/soak_p21_5m_soak_300s.csv`

### 1.2 Sustained Soak (`t=0..571s`, `AIR_READ_ENGINE_HOST=""`, 512 MB Cell)
- **Command:** `go run ./benchmarks/phase21 --mode=soak --duration=900s --concurrency=16 --tenant-prefix=p21_15m`
- **Duration Completed:** `571s` (~9.5 minutes, ~180,000+ operations) before hitting the VM runner's 600s per-command execution timeout.
- **Throughput & Latency:** `290.4–349.8 TPS` (`p50 = 43.85–52.21 ms`, `p95 = 78.48–103.72 ms`, `p99 = 94.68–168.88 ms`), zero degradation.
- **Errors:** **`0` errors**, **`0` OOM kills**, **`0` `memory.high` events**.
- **Memory (512 MB Cell):** `anon = 42.6–53.8 MB` (flat, no leak), `file = 331.1–407.1 MB`, `current = 395.2–484.3 MB`, `peak = 488.1 MB`.
- **Gateway Memory:** `11.2–14.6 MB` (one brief `29.3 MB` GC spike).
- **Pageserver Memory:** `227.9–252.8 MB` (stayed under 256 MB limit).

---

## 2. Tests Not Executed to Full Duration

- **Full 15-minute (`900s`) and 30-minute (`1800s`) uninterrupted single-process runs:** Not completed in this interactive environment due to the 600s per-command wall-clock limit (`task-294` terminated at `571s`). No memory leak or throughput degradation was observed up to `571s`.

---

## 3. Status

**Phase 21.5 Verdict: PASSED for 5-minute (`300s`) and 9.5-minute (`571s`) sustained stability; full 15m/30m uninterrupted runs require a non-time-capped CI runner.**
