# AIR Phase 21.10 — Resource Benchmark Measurement Accuracy Report

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Base Commit:** `803c0fd9f0cab7c2c5b0b85c3898f901fd264d4f` (+ Phase 21.10 fixes)  
**Status:** **PASS**

---

## 1. Root Cause Confirmed

1. `benchmarks/phase21/helpers.go` previously ran `ps -eo comm,rss --no-headers | awk '/postgres/{...}'` inside `air-p21-server`. Minimal Debian runtime images lack `procps` (`ps`), causing `parseF(8)` to silently default to `0.00`.
2. `GatewayRSSMB` and `PageserverMB` previously read `/sys/fs/cgroup/memory.current` (total cgroup memory including page cache) rather than process RSS, and Read Engine RSS was not collected.
3. Missing/unreadable metrics were silently defaulted to `0.00` instead of `NA`.

---

## 2. Changes Implemented

- **`benchmarks/phase21/helpers.go`:**
  - Replaced `ps` with direct `/proc/[0-9]*/comm`, `/proc/[0-9]*/cmdline`, and `/proc/[0-9]*/status` (`VmRSS:`) parsing inside each container (`air-p21-server`, `air-p21-gateway`, `air-p21-pageserver`, `air-p21-read-engine`), handling process restarts and PID changes automatically.
  - Added `/sys/fs/cgroup/cpu.stat` (`usage_usec`) collection alongside `memory.current`, `memory.peak`, `memory.stat` (`anon`/`file`), `memory.events`, and PSI (`memory.pressure`, `cpu.pressure`, `io.pressure`).
  - Missing or unavailable metrics return `math.NaN()` / `"NA"` via `fmtMetric(...)`, never silently `0.00`.
- **`benchmarks/phase21/helpers_test.go`:**
  - Added self-tests `TestReadProcRSSMBFromRoot_MockAndLiveProc` (verifying against both mock `/proc` and live Linux `/proc/self`) and `TestParseCgroupSampleOutput_UnavailableNotZero`.

---

## 3. Verification & Measured Values

- **Collector Self-Test:** `go test -v ./benchmarks/phase21/...` — **PASS** (`0.02s`).
- **Live 5-Minute Full-Topology Soak (`benchmarks/phase21/results/soak_p2110_5m_soak_300s.csv`):**
  - `pg_rss_mb`: `1373.50 MB` (sum of `VmRSS` across active PG17 backends + postmaster; note shared `shared_buffers` pages are counted per backend in Linux `VmRSS`, while cgroup `anon` remained flat at `87.85–93.8 MB` and `file` at `373.5–378.4 MB`).
  - `gw_rss_mb`: `24.1–24.65 MB` (`41.8–42.2 MB` brief GC spikes).
  - `ps_rss_mb`: `231.61–249.2 MB`.
  - `re_rss_mb`: `27.17 MB`.
  - `cell_cur_mb` / `cell_peak_mb`: `503.90 MB` / `512.02 MB` (`high_events = 0`, `oom_kills = 0`).
  - `mem_psi`, `cpu_psi`, `io_psi`: Reported as `NA` (not exposed in container cgroup namespace) rather than fabricated `0.00`.
