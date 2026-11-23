# AIR Phase 22 — Pageserver Memory Optimization & Multi-Tenant Benchmark Report

**Date:** 2026-11-23  
**Repository:** `Mahadi-rsio/air`  
**Forensics Report:** [phase22-memory-forensics.md](file:///home/antigravity/Mahadi-rsio/air/docs/reports/phase22-memory-forensics.md)

---

## 1. Executive Summary & Root-Cause Analysis

Before Phase 22, `air-pageserver` (`256 MB` container limit) suffered deterministic `OOMKilled (ExitCode 137)` failures when bootstrapping or running writes across **5+ simultaneously active tenants**. Detailed forensics identified five compounding root causes:

1. **Unflushed `bootstrap` + `CREATE DATABASE` `InMemoryLayer` retention (`~14.6–22.6 MB`/tenant):** `GRPCPageserverClient.BootstrapTenantFromPGData` streamed all `22.6 MB` (`~2,890` blocks) of PG17 `initdb` files before flushing, and `ActivateCluster` (`CREATE DATABASE`) duplicated `template1` (`7.3 MB`) through both `air_smgr` (`PutPageBatch`) and `WALReceiver` (`IngestWALRecords`), leaving `~14.6 MB` dirty per tenant below the old `16 MB` per-tenant flush threshold.
2. **2x–3x Same-LSN `InMemoryLayer` duplication:** Active workloads buffered `PutPageBatch` (8 KiB), `WALReceiver` FPI (8 KiB), and redundant `RawRecord` payloads simultaneously (`81%` of live heap in `pprof`).
3. **Unbounded Go heap retention (`HeapIdle`):** Without `GOMEMLIMIT` / `debug.SetMemoryLimit`, Go retained `115–321 MB` of freed heap spans in `HeapIdle` (`RssAnon` / cgroup `anon`) instead of returning them to the OS.
4. **Unbounded `decodeV2WALTable` & L0 compaction fan-out:** Background L0 compaction opened all L0 iterators at once, allocating per-record `walTable` slices and appending older compacted L1 layers after newer L0 layers.
5. **Unbounded cache / SLRU retention:** `slruPages` remained pinned in memory indefinitely after L0 flushes, and `DiskCache` duplicated local `LayerDir` files.

### Targeted Fixes Implemented
- **Measurable & Enforceable Memory Budget (`internal/pageserver/memory_budget.go`):** Auto-detected cgroup v2 `memory.max`, Go soft memory limit (`52%` of container limit = `133 MiB` on `256 MB`), global `InMemoryLayer` soft/hard watermarks (`16 MiB` / `32 MiB` on `256 MB`), dynamic fair-share per-tenant watermarks (`2 MiB` / `4 MiB` scaling down to `512 KiB` floor), bounded flush (`2`) and compaction (`1`) concurrency, `200ms` pressure monitor with `750ms` idle flush, cache trimming + `debug.FreeOSMemory()`, and writer admission backpressure (`ErrMemoryBudgetExhausted` -> `codes.ResourceExhausted`).
- **InMemoryLayer Coalescing & SLRU Eviction (`layers.go`, `readpath.go`, `multixact.go`):** Stripped redundant `WALRecord` from materialized FPIs, coalesced same-LSN page images and monotonic `RelSize` updates in place, evicted `slruPages` after durable L0 flush, and added `warmXactSLRU` to reload CLOG/subtrans pages from L0 on demand.
- **Compaction & V2 Memory Fixes (`compactor.go`, `layer_v2.go`):** Pooled 64 KiB V2 raw/compressed chunk buffers, switched `decodeV2WALTable` to a single contiguous backing buffer, bounded background compaction to 4 oldest L0s (`CompactL0Batch`) gated on memory headroom, sorted `t.layers` by `(EndLSN, StartLSN, Seq)` with overlap-safe `GetPageAtLSN`, and fsynced streamed layers before closing.
- **Bounded Bootstrap & WAL Batches (`pageserver_client.go`, `control.go`, `walparser.go`):** Sub-batched `BootstrapTenantFromPGData` (64 blocks, flushing every 256 blocks / `2 MiB`) and `IngestWALBatch` (`<= 64` recs / `<= 512 KiB`), flushed after `CREATE DATABASE`, and compacted `WALStreamParser.buf` in place.

---

## 2. Before / After Comparison (256 MB Pageserver)

| Metric | Before Phase 22 (OOM Repro) | After Phase 22 (12-Tenant Repro) | After Phase 22 (30-Min Soak, 8 Tenants) |
|---|---:|---:|---:|
| Active Tenants Survived | **4** (OOMKill at tenant 5) | **12** (4 prior + 8 active) | **8** (30 min continuous) |
| OOMKill Events (`ExitCode 137`) | **1 (OOMKilled)** | **0** | **0** |
| Cgroup `memory.peak` | **256.00 MB** (Hit cap) | **203.37 MB** | **202.79 MB** |
| Process `VmHWM` (Peak RSS) | **214.73 MB** | **117.25 MB** | **124.69 MB** |
| Final `VmRSS` | Killed | **82.71 MB** | **69.36 MB** |
| Peak Go Live Heap (`HeapAlloc`) | >185 MB | **50.50 MB** | **96.48 MB** |
| Query / Integrity Errors | Fatal / Aborted | **0** | **0** (`305,029` txns) |

---

## 3. Multi-Tenant Scale Matrix (`1, 4, 8, 10, 20` Active Tenants)

All runs used concurrent tenant bootstrap (`--conc-bootstrap=true`) followed by sustained concurrent read/write workloads and full row-count/checksum verification.

### 3.1 `256 MB` Pageserver Profile (`--memory=256m --memory-swap=256m`)

| Tenants | Bootstrap (s) | Peak Cgroup (MB) | Peak VmHWM (MB) | Peak HeapAlloc (MB) | Peak Global InMem (MB) | Peak Tenant InMem (MB) | Peak Dirty Pages | Alloc Rate (MB/s) | Flushes | Avg Flush (MB/s) | GCs / Pause (ms) | Txns Committed | OOMs | Integrity |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| **1** | 14.94 | 44.68 | 51.16 | 29.67 | 1.92 | 1.92 | 390 | 5.03 | 21 | 0.84 | 22 / 9.0 | 1,090 | 0 | PASS |
| **4** | 25.77 | 85.89 | 76.18 | 56.78 | 7.09 | 2.05 | 462 | 15.34 | 84 | 2.48 | 68 / 45.7 | 3,580 | 0 | PASS |
| **8** | 49.39 | 129.68 | 101.28 | 78.32 | 13.92 | 2.05 | 496 | 19.84 | 161 | 3.19 | 111 / 72.4 | 5,219 | 0 | PASS |
| **10** | 70.24 | 146.45 | 105.68 | 81.30 | 12.15 | 2.25 | 484 | 20.30 | 207 | 3.14 | 144 / 104.9 | 6,305 | 0 | PASS |
| **20** | 129.49 | 203.83 | 113.63 | 85.23 | 13.87 | 2.45 | 952 | 24.28 | 455 | 3.74 | 246 / 180.0 | 8,880 | 0 | PASS |

### 3.2 `512 MB` Pageserver Profile (`--memory=512m --memory-swap=512m`)

| Tenants | Bootstrap (s) | Peak Cgroup (MB) | Peak VmHWM (MB) | Peak HeapAlloc (MB) | Peak Global InMem (MB) | Peak Tenant InMem (MB) | Peak Dirty Pages | Alloc Rate (MB/s) | Flushes | Avg Flush (MB/s) | GCs / Pause (ms) | Txns Committed | OOMs | Integrity |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| **1** | 13.87 | 58.92 | 63.85 | 45.46 | 3.94 | 3.94 | 404 | 4.92 | 14 | 0.86 | 20 / 9.6 | 994 | 0 | PASS |
| **4** | 33.48 | 108.88 | 98.88 | 77.73 | 14.31 | 3.84 | 952 | 13.14 | 55 | 2.00 | 55 / 35.9 | 3,362 | 0 | PASS |
| **8** | 51.57 | 167.69 | 139.88 | 110.74 | 28.13 | 3.92 | 494 | 19.52 | 111 | 3.05 | 101 / 90.1 | 4,535 | 0 | PASS |
| **10** | 71.05 | 178.16 | 138.41 | 115.19 | 26.84 | 3.21 | 480 | 19.01 | 139 | 2.94 | 112 / 83.1 | 5,486 | 0 | PASS |
| **20** | 134.85 | 272.30 | 176.57 | 153.29 | 27.89 | 2.45 | 948 | 22.71 | 295 | 3.43 | 192 / 154.2 | 7,209 | 0 | PASS |

---

## 4. Lifecycle, Recovery, Pressure & 30-Minute Soak (`256 MB` Profile)

| Scenario | Tenants | Peak Cgroup (MB) | Peak VmHWM (MB) | Final VmRSS (MB) | Final InMem (MB) | Txns Committed | Query Errors | OOMs | Integrity |
|---|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| **Memory Pressure & Recovery** | 8 | 137.70 | 106.59 | 68.80 | **0.00** | 5,285 | 0 | 0 | **PASS** |
| **Tenant Suspend & Wake** | 4 | 100.05 | 79.00 | 69.93 | **0.00** | 1,821 | 0 | 0 | **PASS** |
| **Graceful Restart + Crash Recovery (`SIGKILL`)** | 4 | 79.20 | 68.54 | 14.33 | **0.00** | 1,854 | 0 | 0 | **PASS** |
| **Phase 21 Storage-Path + Cold Recovery** | 1 | — | — | — | — | 902 | 0 | 0 | **PASS** |
| **30-Minute Continuous Soak (`1857s`)** | 8 | 202.79 | 124.69 | 69.36 | 7.37 | **305,029** | 0 | 0 | **PASS** |

### 30-Minute Soak Stability Highlights
- **Zero memory leak / unbounded growth:** Live `HeapAlloc` remained bounded (`93.2 MB` at 8 min, `83.5 MB` at 16 min, `61.5 MB` at 24 min, `96.48 MB` peak), and `HeapReleased` returned `>99%` of idle heap spans to the OS (`51.3 MB` released out of `51.4 MB` idle at 24 min).
- **Low GC overhead:** `748` GCs over 30 minutes with `903.93 ms` total pause (`1.21 ms` average pause, `0.049%` of runtime).
- **Zero rejections / zero data loss:** `1,606` L0 flushes completed (`1.66 MB/s` avg), `0` backpressure rejections, `0` OOM events, `0` query errors across `305,029` committed transactions.

---

## 5. Acceptance Criteria Verification

1. **No unbounded memory growth:** Verified across the 30-minute soak (`HeapAlloc` flat, `VmRSS` ending at `69.36 MB`, `FinalInMem = 0` on idle/suspend).
2. **No OOM under validated 256 MB workload:** `0` OOM events across all `256 MB` runs (`1, 4, 8, 10, 20` tenants, 12-tenant repro, and 30-minute soak).
3. **At least 8 active tenants under benchmark workload:** Validated up to **20 simultaneously active tenants** on `256 MB`.
4. **No data loss during eviction, flushing, restart, or recovery:** Verified in unit tests (`TestMemoryBudgetBackpressureAndNoDataLoss`, `TestSLRUEvictionAndWarmingAfterFlush`), `suspend-wake`, `restart-recovery` (`docker restart` + `SIGKILL`), `phase21 --mode=storage-path`, and the 30-minute soak.
5. **No regression in read/write correctness:** Full `internal/pageserver`, `internal/control`, `cmd/air-server` test suites and all integration checks pass.
6. **Every memory budget measurable & enforceable:** Exposed via `GET /internal/v1/memory-stats` and `Pageserver.MemoryStats()`, enforced by [`MemoryBudget`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/memory_budget.go).
7. **Before/after benchmark results & root-cause analysis published:** Published in [phase22-memory-forensics.md](file:///home/antigravity/Mahadi-rsio/air/docs/reports/phase22-memory-forensics.md) and this report.
