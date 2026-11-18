# AIR Database Platform — Engineering Benchmark, Durability & Scalability Audit

**Date:** 2026-11-17  
**Repository:** `/workspaces/air`  
**Git Commit:** `c5a98045a8eff4c4a36866fb577b49ecbc0549d6` (`main`)  
**Container Image:** `air-server:pageserver` (PostgreSQL 17.12 + `air_smgr.so` + `supautils.so`)  
**Host Environment:** Linux `6.8.0-1070-azure` (`x86_64`), 2 vCPUs, 7.8 GiB RAM  
**Configuration:** `AIR_PAGESERVER_CACHE_MB=256`, `shared_buffers=32MB`, `wal_level=replica`, `summarize_wal=on`

---

## 1. Executive Summary

### Overall Assessment: **Not Production-Ready** *(Baseline at `c5a98045`; see [Section 12](#12-phase-11--p0-reliability-remediation--validation) for Phase 11 P0 fixes and remaining P1/P2 items)*
While AIR demonstrates strong **steady-state read throughput (`7,977.56 TPS` at 32 clients)**, **high WAL compression (`12.61x` / `92.1%` reduction)**, **clean multi-tenant isolation**, and **20/20 repeated suspend/wake durability cycles**, the audit uncovered **critical P0 durability, crash-recovery, and memory-safety failures** that disqualify it from production workloads without remediation.

### Major Achievements
- **Reproducible Provisioning & Reads:** Provisioning median **`2,627.03 ms`** (`2.57–2.74 s`, `stddev=84.67 ms`); 8-client `pgbench` RO median **`8,054.62 TPS`** (`stddev=109.66`); 8-client RW median **`1,150.23 TPS`** (`stddev=3.18`).
- **WAL & Storage Compression:** Compressed 6 WAL segments from `100,685,864 B` raw `pg_wal` down to `7,982,747 B` (**12.61x compression / 92.1% reduction**). Zero R2 calls on the active read/write hot path.
- **Page Server Socket Throughput:** Scales cleanly from **`11,305.5 pages/s`** (1 reader, `p99=158.2 µs`) to **`33,156.3 pages/s`** (128 concurrent readers, `0` errors), with identical performance under 64-reader hot-page contention (`31,582.0 pages/s`).
- **Multi-Tenant Efficiency:** At 25 tenants, aggregate read throughput reached **`8,796.2 TPS`** (`p95=2.979 ms`), mixed R/W **`5,232.8 TPS`**, active memory **`~72.3 MiB/tenant`** (`1,806.3 MiB` total), **`0` failures**, and verified tenant isolation.
- **Repeated Lifecycle Durability:** **20/20** consecutive `write → commit → suspend → wake → verify` cycles passed with exact row counts and SHA-256 checksums (`0` lost/corrupted rows).

### Major Weaknesses & Blockers (P0)
1. **Unflushed `inMemoryLayer` Data Loss on Shutdown:** `air-server` shutdown/crash does not flush `Timeline.inMemoryLayer`, causing newly created databases/users and committed transactions to vanish on restart (`FATAL: database "..." does not exist`).
2. **Crash Recovery Failure (`SIGKILL`):**
   - Killing `air-bench` (`SIGKILL`) leaves a stale `/var/lib/air/meta/pgdata/postmaster.pid`, permanently bricking container restart (`127.0.0.1:15432 connection refused`) until manual intervention.
   - Killing either `air-bench` or the tenant `postgres` process with `kill -9` permanently corrupts the tenant (`PANIC: could not open critical system index 2662 (SQLSTATE XX001)`).
3. **Un-wakeable Large Dirty Dataset (`1M` rows):** After clean suspend of `p2d_1m` (`135 MB` L0 layer + `68.9 MB` skeleton + `18` WAL segments), waking permanently failed with `FATAL: air_smgr readv failed: pageserver returned error status`.
4. **Unbounded Memory Growth under Concurrent Writes:** `pgbench` RW at 32 clients grew container RAM to **`5.549 GiB`** and aborted after ~30s (`air-server` exited with `ExitCode=0`), losing the test database.
5. **Lifecycle Lock Contention on Wake:** Client wall-clock wake time after suspend was **`12.87–17.05 s`** (median **`14,969.35 ms`**) vs. `272–469 ms` server wake time, because `WakeTenant` blocks on `lockTenantLifecycle` while `processColdOffload` uploads to R2.
6. **Tenant ID Reuse Corruption:** `DELETE /v1/databases/{id}` does not purge Page Server/R2 state; reusing the ID causes stale `pg_toast_16386_index` page bleed (`SQLSTATE XX000`).
7. **Broken Relation Size Introspection:** `pg_total_relation_size()` returns **`0 bytes`** and `pg_database_size()` returns **`161,459 B`** on `air_smgr` tables.

---

## 2. Phase 1 — Baseline Reproducibility (3 Trials, `pgbench -s 5`, 8 Clients)

| Metric | Min | Median | Max | StdDev | Notes |
|---|---:|---:|---:|---:|---|
| **Provisioning (ms)** | 2,574.76 | **2,627.03** | 2,740.39 | 84.67 | Clean DB + `air_smgr` bootstrap |
| **RO TPS (8c)** | 7,908.02 | **8,054.62** | 8,122.60 | 109.66 | Avg latency `0.993 ms` |
| **RW TPS (8c)** | 1,144.80 | **1,150.23** | 1,150.39 | 3.18 | Avg latency `6.955 ms` |
| **Suspend (ms)** | 10,077.32 | **10,079.46** | 10,081.90 | 2.29 | Includes graceful shutdown + L0 |
| **Wake Client Wall (ms)** | 12,869.71 | **14,969.35** | 17,053.13 | 2,091.72 | Blocked by R2 cold-offload lock |

---

## 3. Phase 2 — PostgreSQL Performance (`pgbench` Scale 10 = 1M Rows, 60s)

### 3A. Read-Only (`-S`) & Read-Write (TPC-B) Scaling

| Workload | Clients | TPS | Avg (ms) | P50 (ms) | P95 (ms) | P99 (ms) | PG Hit % | Avg CPU % | Max RAM (MiB) | Fails |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **RO** | 1 | 3,296.93 | 0.303 | 0.266 | 0.411 | 0.694 | 69.21% | 117.22% | 1,490.94 | 0 |
| **RO** | 8 | 7,094.72 | 1.128 | 0.981 | 2.374 | 3.780 | 69.39% | 144.96% | 1,502.21 | 0 |
| **RO** | 16 | 7,569.44 | 2.114 | 1.906 | 4.140 | 6.276 | 69.37% | 146.79% | 1,517.57 | 0 |
| **RO** | 32 | **7,977.56** | 4.011 | 3.701 | 7.544 | 10.919 | 69.40% | 145.78% | 1,544.19 | 0 |
| **RO** | 64 | 7,505.79 | 8.527 | 7.825 | 16.164 | 23.225 | 69.43% | 144.35% | 1,594.37 | 0 |
| **RW** | 1 | 548.53 | 1.823 | 1.642 | 2.718 | 4.414 | 93.85% | 111.15% | 1,947.65 | 0 |
| **RW** | 8 | **1,028.55** | 7.778 | 7.014 | 13.228 | 21.786 | 93.10% | 153.53% | 3,640.32 | 0 |
| **RW** | 16 | 915.69 | 17.473 | 15.021 | 34.501 | 61.672 | 94.03% | 153.94% | 4,952.06 | 0 |
| **RW** | 32 | *645.93\** | *49.541\** | — | — | — | — | — | **5,682.18** | **ABORT** |

*\*RW c=32 aborted after ~30s (`19,392` tx) when `air-server` exited (`ExitCode=0`) at `5.549 GiB` RAM; `perf_p2_db` was lost on restart (`FATAL: database "perf_p2_db" does not exist`).*

### 3B. Custom Application Workloads (100K & 1M Rows)

| Query / Workload | 100K Rows (ms / TPS) | 1M Rows (ms / TPS) | Plan / Notes |
|---|---:|---:|---|
| **Q1: Point Lookup (PK)** | `0.125 ms` | `0.591 ms` | `Index Scan using items_pkey` |
| **Q2: Indexed Range Scan** | `1.055 ms` | `21.282 ms` | `Aggregate -> Index Scan using items_pkey` |
| **Q3: Secondary Index** | `0.688 ms` | `14.088 ms` | `Bitmap Index Scan on idx_items_cat_val` |
| **Q4: GROUP BY Aggregation** | `25.183 ms` | `451.887 ms` | `HashAggregate -> Seq Scan on items` |
| **Q5: Full Table Scan** | `15.122 ms` | `188.228 ms` | `Aggregate -> Seq Scan on items` |
| **Mixed R/W (8c, 15s)** | `1,533.55 TPS` (`5.217 ms`) | `774.07 TPS` (`10.335 ms`) | Custom mixed workload |
| **Concurrent I/U/D (8c)** | `1,592.82 TPS` (`5.023 ms`) | *`1,004.07 TPS` (`7.968 ms`)\** | *1M I/U/D aborted at 6,567 tx (`Connection reset by peer`)* |

### 3C. Cold vs. Warm Execution Latency (100K Rows, `p2d_100k`)

| State | Q1 PK (ms) | Q2 Range (ms) | Q3 SecIdx (ms) | Q4 GroupBy (ms) | Q5 SeqScan (ms) |
|---|---:|---:|---:|---:|---:|
| **Warm Cache** | 1.371 | 1.274 | 0.572 | 21.754 | 18.756 |
| **First after Clean Startup** | 12.983 | 31.959 | 14.290 | 159.521 | 20.307 |
| **After Suspend & Wake** | 12.464 | 28.651 | 12.902 | 153.914 | 20.857 |
| **After Local Cache Eviction** | 14.358 | 33.860 | 15.334 | 178.868 | 20.710 |
| **After Full Container Restart** | 19.189 | 38.181 | 18.029 | 188.497 | 21.110 |

*(Note: `p2d_1m` 1M-row cold/warm test failed after clean suspend because wake failed permanently with `air_smgr readv failed: pageserver returned error status`.)*

---

## 4. Phase 3 — End-to-End Durability & Recovery

| Test | Scenario | Result | Observed Details |
|---|---|---|---|
| **Test A** | Normal suspend & wake (disposable cache cleared) | **PASS** | `cnt=1940`, SHA-256 `43f8100a...`, indexes verified |
| **Test B** | Full container restart (`lfc+ps_cache` cleared) | **PASS** | `cnt=1941`, SHA-256 `25303643...`, indexes verified |
| **Test C1** | Abrupt `SIGKILL` of `air-bench` container | **FAIL (P0)** | Stale `/var/lib/air/meta/pgdata/postmaster.pid` bricks startup (`127.0.0.1:15432 connection refused`); after manual removal, tenant wakes with `PANIC: could not open critical system index 2662` |
| **Test C2** | Abrupt `kill -9` of tenant `postgres` process | **FAIL (P0)** | Tenant permanently corrupted on wake: `PANIC: could not open critical system index 2662 (SQLSTATE XX001)` |
| **Test D** | FULL Backup (`201`) + WAL Archive (`3` segs) + Restore (`200`) | **PARTIAL** | `amcheck_ok=true`, `restored_backups=1`, `wal_segments_replayed=3`, `verify_query_output="500"` (post-backup row `999002` + `pg_switch_wal()` was not present in restored output) |
| **Test E** | 20 cycles: `write → commit → suspend → wake → verify` | **PASS** | `20/20` cycles succeeded (`failures=0`), final `cnt=521` + SHA-256 verified |

---

## 5. Phase 4 — R2 Storage & Page Server Behavior

### 5A. Storage Accounting (`p4_db`, `pgbench -s 5` + 15s RW)
- **Reported by PostgreSQL:** `pg_database_size('p4_db')` = `161,459 B`; `pg_total_relation_size('pgbench_accounts')` = **`0 B`** (broken `air_smgr` size reporting).
- **Physical R2 Objects (`10` objects, `36,455,731 B` = `34.77 MiB` total):**
  - `img_bootstrap`: `3,078,573 B`
  - `L0` layer: `17,372,723 B`
  - `layer_manifest.json`: `2,140 B`
  - `skeleton.tar.zst.enc`: `8,019,548 B`
  - `6` compressed WAL segments: `7,982,747 B` vs. `100,685,864 B` raw `pg_wal` (**12.61x compression / 92.1% reduction**)

### 5B. R2 Request Profile (`r2proxy`)
- **Active RW Workload:** **0** R2 requests.
- **Provision:** `2 HEAD`, `2 PUT` (`3.08 MB` bootstrap + `1.1 KB`).
- **Suspend:** `3 HEAD`, `2 POST`, `4 PUT` (`17.37 MB` L0 + manifest).
- **Async Cold Offload (post-suspend):** `6 HEAD`, `7 PUT` (`8.02 MB` skeleton + `7.98 MB` 6 WAL segments).

### 5C. Page Server Unix Socket Microbenchmark (`/var/lib/air/pageserver.sock`)

| Concurrent Readers | Throughput (pages/s) | P50 Latency | P99 Latency | Errors |
|---:|---:|---:|---:|---:|
| **1** | 11,305.5 | 81.8 µs | 158.2 µs | 0 |
| **8** | 23,876.1 | 247.1 µs | 2.39 ms | 0 |
| **32** | 29,326.6 | 885.0 µs | 4.51 ms | 0 |
| **64** | 31,576.4 | 1.77 ms | 7.52 ms | 0 |
| **128** | **33,156.3** | 3.20 ms | 15.11 ms | 0 |
| **64 (same-page hot)** | 31,582.0 | 1.73 ms | 8.13 ms | 0 |

---

## 6. Phase 5 — Multi-Tenant Scalability

| Tier | Provision Latency | Read TPS / P95 | Mixed TPS / P95 | Active RSS (MiB) | Idle RSS (MiB) | Wake Latency | Fails | Isolation |
|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| **1 tenant** | `2,101 ms` | `5,166.0` / `0.144 ms` | `3,735.9` / `0.284 ms` | `122.4` | `150.7` | `1,080 ms` | 0 | **PASS** |
| **10 tenants** | `p50=2,099 ms`, `p95=2,546 ms` | `8,742.1` / `1.309 ms` | `5,858.4` / `2.420 ms` | `711.0` (`71.1/t`) | `231.2` | `p50=636.5 ms`, `p95=648.6 ms` | 0 | **PASS** |
| **25 tenants** | `p50=2,082 ms`, `p95=2,339 ms` | `8,796.2` / `2.979 ms` | `5,232.8` / `7.460 ms` | `1,806.3` (`72.3/t`) | `601.8` | `p50=630.0 ms`, `p95=644.2 ms` | 0 | **PASS** |

---

## 7. Phase 6 — Failure Injection Matrix

| ID | Fault Injected | Expected Behavior | Actual Observed Behavior |
|---|---|---|---|
| **F1** | R2 Unavailable (`503`) | Clean provision fail; active DB unaffected | Provision failed cleanly `HTTP:400` after 3 SDK attempts; active query returned `100` |
| **F2** | R2 Timeout (`3s → 504`) | Clean timeout/retry fail | Provision failed cleanly `HTTP:400` after 3 attempts |
| **F3** | R2 Transient `5xx` | SDK retry succeeds | AWS SDK retried and provision **SUCCEEDED (`HTTP:201`)** |
| **F4** | Drop Layer `PUT` on suspend | Surface offload error or mark degraded | Suspend returned `HTTP:200` (async offload error hidden from caller); wake succeeded from local disk (`count=101`) |
| **F5** | Drop WAL `PUT` on suspend | Surface offload error or mark degraded | Suspend returned `HTTP:200` (async error hidden); wake succeeded from local disk (`count=102`) |
| **F8** | Corrupted Layer `GET` (local cleared) | Fail wake cleanly | Wake failed cleanly `HTTP:400` (`timeout waiting for postgres on port 18021`) |
| **F9** | Missing `layer_manifest.json` (local cleared) | Fail wake cleanly | Wake failed cleanly `HTTP:400` |
| **F10** | Insufficient Disk (`4 MB tmpfs`) | Fail `initdb` cleanly | Failed cleanly with `FATAL: could not write to file "pg_wal/xlogtemp.39": No space left on device` |

---

## 8. Phase 7 — Resource Efficiency Summary

- **Idle Container (`f6_db` suspended):** `0.17%` CPU, `210.3 MiB / 7.756 GiB` (`2.65%` RAM), `/var/lib/air` = `171 MiB`.
- **Multi-Tenant Memory Footprint:** `~71–72 MiB` incremental RSS per active tenant (10–25 tenants).
- **Heavy Single-Tenant Write Memory Growth (Leak/Unbounded Buffer):** `1,491 MiB` (RO c=1) → `1,948 MiB` (RW c=1) → `3,640 MiB` (RW c=8) → `4,952 MiB` (RW c=16) → **`5,682 MiB` crash/exit** (RW c=32).

---

## 9. Phase 8 — Vanilla PostgreSQL 17.12 Local-Storage Baseline Comparison

Both engines ran on the identical host (`2 vCPUs`, `7.8 GiB RAM`), identical PostgreSQL `17.12` binary (`air-server:pageserver`), identical `shared_buffers=32MB`, `wal_level=replica`, `pgbench -s 10` (1M rows). *(Note: vanilla PG ran 30s per tier vs. 60s for AIR; both were warmed up prior to measurement.)*

| Workload | PG 17.12 Local TPS | PG 17.12 Avg / P95 / P99 (ms) | AIR Warm TPS | AIR Avg / P95 / P99 (ms) | AIR vs. Local PG |
|---|---:|---|---:|---|---|
| **RO 8 clients** | 13,386.64 | 0.598 / 1.067 / 2.229 | 7,094.72 | 1.128 / 2.374 / 3.780 | **0.53x** (-47.0% TPS) |
| **RO 32 clients** | 11,872.62 | 2.695 / 4.881 / 7.645 | 7,977.56 | 4.011 / 7.544 / 10.919 | **0.67x** (-32.8% TPS) |
| **RW 8 clients** | 1,959.82 | 4.082 / 6.704 / 10.417 | 1,028.55 | 7.778 / 13.228 / 21.786 | **0.52x** (-47.5% TPS) |
| **RW 32 clients** | 1,745.41 | 18.334 / 38.572 / 61.146 (`0` fails) | *Aborted (645.93)* | *Exit at 5.55 GiB RAM* | **Failed** |

- **Cold Cache & Suspend/Wake Overhead (Phase 2D, 100K rows):** Warm PK lookup (`1.371 ms`) / GroupBy (`21.754 ms`) degrades to **`12.983 ms` / `159.521 ms`** on first cold query after clean startup, **`12.464 ms` / `153.914 ms`** after suspend/wake, and **`19.189 ms` / `188.497 ms`** after full container restart (9.1x–14.0x cold-read latency penalty vs. warm cache).

---

## 10. Phase 9 — Cost & Storage Efficiency (Cloudflare R2 Pricing)

**Provider Pricing Used (Cloudflare R2 Standard):**
- Storage: **`$0.015 / GB-month`** (`10 GB-mo` free tier)
- Class A Operations (`PUT`, `POST`, `LIST`): **`$4.50 / 1M` requests** (`1M` free/mo)
- Class B Operations (`GET`, `HEAD`): **`$0.36 / 1M` requests** (`10M` free/mo)
- Data Egress: **`$0.00`** (free)

**Measured Consumption (`p4_db`: `pgbench -s 5` + 15s RW = `36.46 MB` / `34.77 MiB` in R2):**
- **R2 Storage Cost per Tenant:** `0.0365 GB × $0.015 = $0.00055 / month` (or **`$0.015 / GB-month`** of R2 physical footprint; `12.61x` WAL compression keeps WAL storage minimal).
- **R2 Requests per Provision:** `2` Class A + `2` Class B = **`$0.0000097`**.
- **R2 Requests per Suspend + Cold Offload:** `13` Class A + `9` Class B = **`$0.0000617`**.
- **Active Transaction R2 Cost:** **`$0.00`** (`0` R2 calls during active SQL reads/writes).
- **Cost Driver:** R2 storage and request fees are negligible (`< $0.001/tenant-month` for small DBs). **Compute RAM (`~72 MiB` per active tenant, `~210 MiB` idle control plane)** dominates infrastructure cost.

---

## 11. Prioritized Engineering Remediation Plan

| Priority | Issue | Recommended Fix |
|---|---|---|
| **P0** | Data loss on `air-server` shutdown (`inMemoryLayer` not flushed) | Flush all dirty `inMemoryLayer`s to L0/disk and checkpoint before closing Page Server on `SIGTERM`/`SIGINT` |
| **P0** | Crash corruption (`PANIC: index 2662`) & stale `postmaster.pid` on `SIGKILL` | Clean stale `postmaster.pid` safely on control-plane startup; ensure atomic LSN/page durability before acknowledging WAL/checkpoints |
| **P0** | `p2d_1m` (`135 MB` L0) un-wakeable after clean suspend | Fix Page Server L0 layer read/index path (`air_smgr readv failed: pageserver returned error status`) |
| **P0** | Unbounded RAM growth (`5.55 GiB` → exit at RW `c=32`) | Enforce strict `inMemoryLayer` flush thresholds and backpressure/eviction bounds |
| **P1** | `WakeTenant` blocked `13–17 s` by `processColdOffload` | Decouple R2 cold-offload upload from `lockTenantLifecycle` so `WakeTenant` can serve from local/L0 immediately |
| **P1** | Tenant ID reuse causes `pg_toast_16386_index` bleed | Purge local layers, cache, and R2 prefix synchronously (or tombstone) on `DELETE /v1/databases/{id}` |
| **P1** | Async R2 offload failures silent to `Suspend` callers | Track `offload_status` in control-plane metadata and expose retry/degraded state |
| **P2** | `pg_total_relation_size()` returns `0` | Implement `smgrnblocks` / relation size reporting accurately in `air_smgr` |

---

## 12. Phase 11 — P0 Reliability Remediation & Validation

All four P0 blockers identified at commit `c5a98045` were investigated, reproduced, fixed, and verified with automated regression tests and container benchmarks.

### 12A. Root Causes & Implemented Fixes

| P0 | Root Cause | Fix Implemented | Regression Tests |
|---|---|---|---|
| **1. `SIGKILL` Crash Recovery & Stale `postmaster.pid`** | (a) `NewControlPlaneWithConfig` checked `os.Stat(meta/pgdata/postmaster.pid)` without checking PID liveness; `verifyPostgresProcessIdentity` treated zombie (`Z`) processes as alive and `CleanupStalePostmasterLocks` did not exist to reap dead child PIDs or remove stale locks. (b) On `Pageserver` restart, `LoadLayersFromR2` populated `t.layers` while `t.relSizes` started empty (`0`). Subsequent writes to lower block numbers set `t.relSizes[rel] = blk+1` and wrote truncated `EntryFlagRelSize` metadata, shrinking `GetRelSize(2662)` below its existing root block (`3`) and causing `PANIC: could not open critical system index 2662`. | Added `CleanupStalePostmasterLocks` (PID liveness check, `WNOHANG` reap of specific dead child PID, stale `postmaster.pid` & `.s.PGSQL.*` socket cleanup) in `internal/cell/process.go` and `internal/control/control.go`; rejected zombie processes in `isProcessAlive`. Added `relSizeKnown` + `EnsureRelSizeSeeded` in `internal/pageserver/readpath.go` and `walreceiver.go` so `t.relSizes` is seeded monotonically from persisted layers before recording new `EntryFlagRelSize`. | `TestPhase11StalePostmasterPIDAndSIGKILLCrashRecovery`, `TestPhase11RelSizeDoesNotRegressAfterRestart` |
| **2. Unflushed `inMemoryLayer` Data Loss** | `cmd/air-server/main.go` closed components without draining active tenants or calling `FlushAll` on `Pageserver`; `WriteLayer` and `PublishLayerManifest` did not `fsync` before atomic rename; `ListLayersFromR2` lacked a local `LoadLayerManifest` fallback when R2 is disabled (`s3c == nil`). | Added ordered shutdown (`grpcSrv.GracefulStop`, `srv.Shutdown`, `cp.ShutdownActiveTenants`, `ps.FlushAll`, `ps.Close`) in `cmd/air-server/main.go` and `FlushAll` in `internal/pageserver/server.go`. Added `Sync()` before `Close()`/`Rename()` in `WriteLayer`/`PublishLayerManifest` and local `LoadLayerManifest` fallback in `internal/pageserver/layers.go`. | `TestPhase11ShutdownDurabilityPreservesDBAndWrites`, `TestPhase11FlushAllAndLocalManifestRecovery`, `TestPhase11FailedR2UploadSurfacesErrorAndSkipsManifest` |
| **3. 135 MB L0 Layer Wake Failure** | `decodeV2WALTable` in `internal/pageserver/layer_v2.go` allocated a contiguous buffer capped at `maxV2DecompressedSize` (`256 MiB`); large L0 WAL tables (>256 MiB decompressed) failed V2 layer decoding on wake, returning `StatusError` (`air_smgr readv failed: pageserver returned error status`). | Stream-decompressed zstd WAL tables in `decodeV2WALTable` with `io.LimitReader` up to `maxV2WALTableSize` (`1 GiB`), removing the 256 MiB contiguous limit, and added `StatusError` diagnostic logging in `internal/pageserver/protocol.go`. (Bounded L0 flushing in P0 #4 also keeps individual L0 layers ~500 KB–16 MiB.) | `TestPhase11LargeWALTableV2Layer` (270 MiB WAL table + SHA-256 + reads) |
| **4. Unbounded Memory Growth & Flush/Read Race** | `InMemoryLayer` had no byte budget or automatic flush/backpressure, growing unboundedly under 32-client RW until `air-server` exited at `5.549 GiB`. Additionally, `FlushInMemory` swapped `t.inMem` before `WriteLayer` + `PublishLayerManifest` completed, creating a visibility window where concurrent `GetPageBatch`/`GetPageAtLSN`/`GetRelSize` reads missed in-flight flushed pages (causing `pgbench_accounts_pkey` duplicate key errors under 32-client RW). | Tracked `ApproxBytes` in `InMemoryLayer.Put`; enforced `defaultMaxInMemBytes = 16 MiB` async flush (`MaybeFlushAsync`), `2x` (`32 MiB`) write backpressure (`EnforceInMemBudget`), and checkpoint-triggered flushes in `WALReceiver.Run`. Kept `frozenMems []*InMemoryLayer` on `Timeline` (`lookupInMemLayersLocked`, `removeFrozenMemLocked`) until `PublishLayerManifest` succeeds and `t.layers` is updated under `t.mu.Lock()`. | `TestPhase11BoundedInMemAndBackpressure` |

### 12B. Before vs. After Benchmark & Reliability Comparison

| Check / Workload | Baseline (`c5a98045`) | Phase 11 Remediated | Notes / Environment |
|---|---|---|---|
| **Go Unit & Integration Suites** | — | **PASS** (`internal/pageserver` 57.38s, `internal/control` 109.68s, `internal/cell`, `internal/config`) | Includes all 7 new `TestPhase11*` tests |
| **`phase3_durability` Test A (Suspend/Wake)** | PASS (`cnt=1940`) | **PASS** (`cnt=1940`, SHA-256 + index verified) | `air-bench`, `AIR_R2_PRIMARY=false` |
| **`phase3_durability` Test B (Container Restart)** | PASS (`cnt=1941`) | **PASS** (`cnt=1941`, SHA-256 + index verified) | `lfc` + `ps_cache` cleared |
| **`phase3_durability` Test C (Abrupt `SIGKILL`)** | **FAIL (P0)** (`postmaster.pid` bricked startup + `PANIC: index 2662`) | **PASS** (`acknowledged_committed=1477`, `found_after_recovery=1477`, `missing=0`, `base_cnt=1941`, `base_hash_ok=true`) | 3-cycle `SIGKILL` + `2662` index scan also verified in `go test` |
| **`phase3_de` Test E (20-Cycle Suspend/Wake)** | **PASS** (`20/20`, `failures=0`) | **PASS** (`20/20`, `failures=0`, `cnt=520`) | `air-bench`, `AIR_R2_PRIMARY=false` |
| **1M-Row `pgbench -s 10` Suspend/Wake (`p11_1m`)** | **FAIL (P0)** (`135 MB` L0 → `air_smgr readv failed`) | **PASS** (`40` bounded L0 layers, `24M` total; `count=1,000,000`, `sum=0`; post-wake RW `731.08 TPS`, `0` fails) | `lfc` + `ps_cache` evicted before wake |
| **32-Client `pgbench` RW (`-s 10 -c 32 -j 8 -T 30`)** | **FAIL (P0)** (`5.549 GiB` RAM → exit at ~30s, DB lost) | **PASS** (`22,103` txns, `0` fails, `740.90 TPS`, avg `43.19 ms`, peak `air-server` `VmRSS` **`633 MiB`**; survives suspend+evict+wake with `accts=1,000,000`, `acct_sum=br_sum=-96117`) | 8.8x lower peak RSS vs. baseline crash |
| **`phase5_multitenant` (Tiers 1 / 10 / 25)** | PASS (1/10/25: `0` fails, isolation PASS) | **PASS** (1/10/25: `0` fails, `read_agg_tps` `4,324.6 / 7,926.5 / 7,943.4`, `mixed_agg_tps` `2,930.4 / 5,382.3 / 5,115.9`, isolation PASS) | Tier 50 hit Docker default `64M` `/dev/shm` (`No space left on device` at `mt_50_43`) |
| **`phase6_faults.sh` (R2 Fault Matrix)** | F1–F5, F8–F10 | **PASS** (`F1` `HTTP:400` + active `100`, `F2` `HTTP:400`, `F3` `HTTP:201`, `F4` `count=101`, `F5` `count=102`, `F8` `HTTP:400`, `F10` clean disk-full fail) | Tested with `r2proxy` + `AIR_R2_PRIMARY=true` |

### 12C. Remaining Open Issues (P1 / P2)
- **All P0 blockers (1–4) are resolved.** No P0 issues remain open.
- **P1 — `WakeTenant` cold-offload lock contention:** `WakeTenant` still waits on `lockTenantLifecycle` when `processColdOffload` is uploading to R2.
- **P1 — Tenant ID reuse bleed:** `DELETE /v1/databases/{id}` does not synchronously purge all Page Server/R2 layer state before allowing ID reuse.
- **P1 — Async R2 offload visibility to `Suspend` callers:** `F4`/`F5` return `HTTP:200` on `Suspend` while R2 upload errors occur asynchronously (surfaced in Page Server logs/unit tests, not yet exposed as `offload_status` on the HTTP control plane).
- **P2 — `air_smgr` relation size reporting:** `pg_total_relation_size()` / `pg_database_size()` introspection remains inaccurate (`0` / underreported).
- **Benchmark Harness Notes:** `benchmarks/phase3_durability` Test E has a pre-existing `SQLSTATE 42P08` parameter type error in `main.go` (covered by `benchmarks/phase3_de` instead); multi-tenant container runs at 50+ active Postgres instances require `--shm-size` > Docker's 64 MiB default.

---

## 13. Phase 12 — Lifecycle Isolation, Offload Reliability, Storage Introspection & Compaction

All remaining P1 and P2 issues from Phase 11 (Tenant ID reuse isolation, `WakeTenant` lock contention during R2 offload, offload error propagation and status tracking, `air_smgr` storage introspection, and L0 fragmentation/compaction) were audited, implemented, and verified with `-race` Go tests and real `air-server:pageserver` PG17 container runs.

### 13A. Root Causes & Architectural Changes

| Area | Root Cause | Implemented Solution | Key Tests |
|---|---|---|---|
| **Part 1 (P1): Tenant ID Reuse & Storage Isolation** | `DELETE` + re-`ProvisionDatabase` reused the same `tenantID` across R2 keys, local paths, `Timeline` state, and shared caches (`DecodedChunkCache`, `boundedIndexCache`, `DiskCache`), allowing stale files, cached pages, or delayed uploads from a previous lifecycle to bleed into the new tenant. | Added immutable per-lifecycle `generation` IDs (`gen_<hex>`) stored in control-plane metadata (`clusters.generation`), propagated to `Pageserver` (`RegisterTenantGen`, `PublishLayerManifestGen`, generation-scoped R2/CAU prefixes, local layer paths, `DiskCache`, `DecodedChunkCache`, `boundedIndexCache` keys, and `offload_ops`). Delayed or stale-generation manifest publishes return `ErrStaleGeneration`. | `TestPhase12TenantIDReuseAndGenerationIsolation` (3 full provision/write/offload/delete/recreate cycles) |
| **Part 2 (P1): `WakeTenant` Lock Contention** | `processColdOffload` held `lockTenantLifecycle(tenantID)` across R2 PUT/network I/O, blocking `WakeTenant` for the entire R2 upload duration (12.87–17.05s in earlier benchmarks). | Replaced coarse lock-across-network-I/O with brief pre- and post-upload lifecycle checks gated by `(tenant_id, generation, lifecycle_seq)` and a cancellable per-offload context. `WakeTenant` cancels any in-flight offload for that tenant immediately without waiting for slow R2 I/O. | `TestPhase12WakeTenantNotBlockedBySlowR2Offload`, `TestPhase12ConcurrentSuspendWakeAndDeleteOffload` |
| **Part 3 (P1): Offload Reliability & Error Propagation** | Async R2 upload failures were logged without durable state tracking or retry visibility, causing false-success perceptions on `Suspend`. | Added durable `offload_ops` tracking (`op_id`, `tenant_id`, `generation`, `state`, `started_at`, `completed_at`, `uploaded_objects`, `failed_objects`, `error_details`, `retry_count`) with bounded exponential-backoff retries, sync-mode failure propagation, restart reconciliation (`cancelled`/`failed` with error details), and HTTP status endpoints (`GET /v1/databases/:id/offload`, `GET /internal/v1/offloads/:op_id`). | `TestPhase12OffloadReliabilityRetryAndHTTPStatus`, `TestPhase12OffloadStatusAfterRestart` |
| **Part 4 (P2): Storage Introspection** | `air_smgr` tracked block counts only transiently or returned `0` for `pg_total_relation_size()` / `pg_database_size()`, and `pg_database_size()` bypassed `air_smgr` when local `pgdata` only held metadata skeletons. | Updated `extensions/air_smgr` (`air_smgr.c`, `communicator.c`), `patches/pg17-smgr-hook.patch` (`air_dbsize_hook`), and `Pageserver` (`EntryFlagRelSize`, `GetDatabaseSize`, `GetRelSize`, unlink/truncate accounting) to persist fork sizes (`main`, `fsm`, `vm`, `init`), multi-segment/TOAST/index relations, truncations, and drops across suspend, wake, compaction, and server restarts. | `TestPhase12StorageIntrospectionSizesAcrossForksDropsTruncateAndRestart`, container PG17 verification |
| **Part 5: L0 Layer Fragmentation & Compaction** | 40 bounded L0 layers kept decode sizes safe in Phase 11, but increased manifest size, R2 GET fan-out, and cold lookup latency on overlapping key ranges. | Investigated 40-L0 fragmentation and verified `CompactIfNeeded` merges L0 layers into a single atomic Image layer while preserving LSN ordering, keeping old layers until manifest publication succeeds, and remaining safe under concurrent reads/writes and R2 failures. | `TestPhase12L0FragmentationAndCompaction`, `TestPhase12CompactionConcurrentReadsAndWrites`, `TestPhase12CompactionR2FailurePreservesOldLayers`, `TestPhase12CompactionRestartAndManifestRecovery` |

### 13B. Measured Before vs. After Results

#### 1. `WakeTenant` Latency & Lifecycle Isolation
- **`WakeTenant` during 3s slow R2 upload (`TestPhase12WakeTenantNotBlockedBySlowR2Offload`):** **96.51 ms** (down from 12.87–17.05 s when `processColdOffload` held the lifecycle lock across R2 I/O).
- **Normal R2 `WakeTenant` in `air-server:pageserver` container:** **127 ms** (`NORMAL_R2_SUSPEND_MS=10087`).
- **Tenant ID reuse (`p12_reuse` delete + recreate in container):** **2,570 ms** recreate latency, **`0` leaked public tables** (`RECREATED_PUBLIC_TABLES=0`), `ErrNotFound` on old-generation pages, and `ErrStaleGeneration` on delayed old-generation manifest publishes.
- **25-Tenant Container Provisioning & Isolation (`phase12_verify.sh`):** Provision + write across 25 tenants in **59,139 ms**, total suspend/wake/verify cycle **184,482 ms**, **`0` cross-tenant mismatches** (`MISMATCHES=0`), container footprint **`1.583 GiB` RAM / `0.38%` CPU**.
- **Unavailable R2 (`r2proxy` `unavailable` fault):** `Suspend` returns `offload_op_id` + `offload_state: "pending"`, and `GET /v1/databases/:id/offload` exposes `"state":"running"` during retries and terminal `"state":"failed"` with `error_details` upon exhaustion (no false-success).

#### 2. L0 Compaction Benchmark (`TestPhase12L0FragmentationAndCompaction`, 40 L0s -> 1 Image Layer)

| Metric | Before Compaction (40 L0s) | After Compaction (1 Image) | Improvement |
|---|---|---|---|
| **L0 Layer Count** | 40 | 0 (1 compacted Image layer) | 40x fewer layers |
| **Manifest Size** | 43,161 bytes | 1,244 bytes | **34.7x smaller** |
| **Cold R2 GET Count** | 100 | 51 | **1.96x fewer GETs** |
| **Cold Page Lookup `p50`** | 75.361 µs | 42.860 µs | **1.76x faster** |
| **Cold Page Lookup `p95`** | 3.767 ms | 1.959 ms | **1.92x faster** |
| **Cold Page Lookup `p99`** | 5.131 ms | 2.735 ms | **1.88x faster** |

#### 3. Real PG17 `air_smgr` Storage Introspection (`air-server:pageserver` Container)
Measured on `p12_introspect` (`items` table with `TEXT` TOAST column + `idx_items_pad` index):
- **After 500-row insert + index:** `pg_relation_size('items')` = `180,224 B`, `fsm` = `24,576 B`, `vm` = `0 B`, `pg_table_size('items')` = `212,992 B`, `pg_total_relation_size('items')` = `262,144 B`, `pg_relation_size('idx_items_pad')` = `16,384 B`, `pg_database_size('p12_introspect')` = `7,763,635 B`.
- **After `UPDATE`/`DELETE`/`VACUUM` + `aux_tbl`:** `pg_relation_size('items')` = `270,336 B`, `pg_table_size('items')` = `311,296 B`, `pg_total_relation_size('items')` = `368,640 B`, `pg_database_size` = `7,771,827 B`.
- **After `Suspend` + `Wake`:** `270,336 B` / `311,296 B` / `368,640 B` / `8,197,811 B` (no size regression).
- **After `docker restart` + `Wake`:** `270,336 B` / `311,296 B` / `368,640 B` / `8,197,811 B` (exact match).
- **After `TRUNCATE TABLE items` + `DROP TABLE aux_tbl`:** `0 B` / `8,192 B` / `24,576 B` / `7,771,827 B`.
- **Architecture Note:** `pg_database_size()` combines catalog/local PGDATA base metadata with `air_smgr`-managed relation fork sizes tracked in Pageserver; temporary relations created only in local backend temp files outside `air_smgr` remote persistence remain local-only per session.

## 14. Phase 13.1 — Code Quality, Architecture Refactoring & Backup/PITR Verification

Phase 13.1 audited the repository baseline at commit `d54bdc2`, modularized `internal/control/` and `internal/backup/`, fixed build/continuity/checksum/PITR defects, and verified backup, WAL archival, manifest verification, and PITR restore end-to-end against real PostgreSQL 17 clusters.

### 14A. Key Remediations & Refactorings

| Area | Finding | Resolution |
| :--- | :--- | :--- |
| **Build & Package Integrity** | `internal/backup/pitr_dag.go` failed to compile (`undefined: backup`, `undefined: r2Key`); `internal/control/control.go` had a corrupted `var (` block and duplicate `RunEncryptedBackupStep` call; `tests/e2e` imported removed `internal/gateway`. | Fixed build errors in `pitr_dag.go`; split 2,445-line `control.go` into [control.go](file:///workspaces/air/internal/control/control.go), [migrations.go](file:///workspaces/air/internal/control/migrations.go), [offload.go](file:///workspaces/air/internal/control/offload.go), [backup_restore.go](file:///workspaces/air/internal/control/backup_restore.go), and [http.go](file:///workspaces/air/internal/control/http.go); cleaned dead `internal/gateway` imports in `tests/e2e`. |
| **WAL Continuity & LSN Math** | `ValidateWALContinuity` used `segs[last].start` instead of `start + WALSegmentSize`, rejected earlier retained segments, and failed on auxiliary `.history`/`.backup` files. | Added `IsWALSegmentName`, fixed `lastCovered = segs[last].start + WALSegmentSize`, allowed `segs[0].start <= firstSegment`, and filtered auxiliary files in `WALRefsFromArtifacts` and `StagePITRRestore`. |
| **Checksum Separation** | Plaintext vs. ciphertext SHA-256 digests were conflated under `x-amz-meta-air-sha256`, causing `Store.VerifyObject` (`HeadObject`) to compare ciphertext against plaintext hashes. | Separated `CipherSHA256` (`x-amz-meta-air-cipher-sha256` / `ChecksumSHA256`) for `HeadObject` verification from `PlainSHA256` (`x-amz-meta-air-plain-sha256`) for post-decryption verification. |
| **Immutable Manifests & Post-Backup WAL PITR** | `PublishCompletedBackupManifest` was missing/unwired; `RestoreDatabaseWithOptions` rejected `target_lsn` in WAL segments archived after the base backup. | Implemented `PublishCompletedBackupManifest` (`If-None-Match: *`) after WAL drain; added `buildRestoreWALArtifacts` to merge base manifest WALs with subsequent archived WAL segments; added `GET /v1/databases/:id/backups` and `POST /v1/databases/:id/backups/:backup_id/verify`. |
| **Warm Pool Goroutine Cleanup** | `cell.Agent.Close()` did not wait for the background `warmPoolLoop` `initdb` goroutine, causing `TestCgroupV2SetupAndTeardown_TempRoot` `TempDir` cleanup races. | Added `warmPoolWG sync.WaitGroup` to `cell.Agent` and `a.warmPoolWG.Wait()` in `Agent.Close()`. |

### 14B. Verification Results (Go `-race` & Live PG17 E2E)

- **`go build ./cmd/air-server ./internal/...` & `go vet ./...`**: PASS (0 errors, 0 warnings).
- **`go test -race -count=1 ./internal/...`**: PASS across `internal/backup` (`1.238s`), `internal/cell` (`9.192s`), `internal/config` (`1.042s`), `internal/control` (`288.242s`), `internal/crypto` (`1.105s`), `internal/fencing` (`1.084s`), and `internal/pageserver` (`141.488s`).
- **`TestPhase13_EndToEnd_BackupManifest_Verification_PITR_And_RestoreToNewTenant`**: PASS (`6.29s`) — verified real PG17 `FULL` backup, immutable `BackupManifest` publication, `GET /v1/databases/:id/backups`, `POST .../verify`, post-backup WAL archival (`type=WAL`), `target_lsn` PITR, and full restore into an isolated `new_tenant_id` (preserving pre-target rows and excluding post-target rows).
- **`TestPhaseF_LivePG17_Backup_WAL_PITR_And_CASFencing` & `TestPhaseF_ControlPlane_BackupAndRestoreHTTPAPI`**: PASS (`5.42s` and `6.07s`).
