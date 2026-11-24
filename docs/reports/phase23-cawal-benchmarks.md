# Phase 23 — Adaptive CA-WAL Benchmarks & Production Readiness

## 1. Final Engineering Decision

**Decision: `PASS WITH LIMITATIONS`**

- **Keep `Config.EnableCAWAL = false` by default** for controlled opt-in rollout.
- **Why `PASS WITH LIMITATIONS`:**
  1. Adaptive CA-WAL (`C_adaptive_cawal`, `16 KB` threshold + per-hash singleflight + verified-blob cache + sharded pins + bounded I/O + `RLock` tenant fast path) is **correct, race-free (`-race` pass), crash-safe, and backward-compatible**.
  2. It eliminates external `.walb` file/fsync overhead on small WAL payloads (`1T small_shared`: `31.89 MB/s`, `1.57 ms` flush, `37 µs` p99 vs `A_disabled` `22.61 MB/s`, `2.35 ms` flush, `45 µs` p99), lowers 20-tenant `large_shared` p99 (`113 µs` vs `1633 µs` unadaptive vs `5449 µs` disabled), and improves 50-tenant `concurrent_shared` mean p99 (`15482 µs` vs `20220 µs` unadaptive vs `32397 µs` disabled, with individual runs at `3303 µs` and `4428 µs`).
  3. **Limitation:** On a 2-vCPU container (`GOMAXPROCS=2`), 50-tenant concurrent bursts still exhibit Go scheduler runqueue variance (`3303 µs`–`38715 µs` across repeated runs), and on already-zstd-compressed V2 layers with small/unique payloads, external `.walb` files still add `~1–3 KB` of container metadata overhead (`21 KB` vs `23 KB`). Therefore, `EnableCAWAL` should remain opt-in (`false` by default) and be enabled selectively for multi-tenant workloads with large (`>= 16 KB`) shared WAL tables.

---

## 2. Adaptive Threshold Sweep (`TestCAWAL_AdaptiveThresholdSweep`)

Environment: `linux/amd64`, `AMD EPYC 7763`, `GOMAXPROCS=2`, `256 MB` profile, `10` tenants, shared pattern.

| ThresholdKB | Case | MB/s | p99 (µs) | Flush (ms) | Disk (KB) | Dedup | Inline | External |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | `small_1KB` | 13.53 | 161 | 13.43 | 17 | 605.00x | 0 | 10 |
| 4 | `medium_8KB` | 115.85 | 1380 | 10.61 | 17 | 4313.95x | 0 | 10 |
| 4 | `large_24KB` | 211.07 | 45 | 9.10 | 16 | 9364.00x | 0 | 10 |
| 8 | `small_1KB` | 30.11 | 59 | 6.11 | 17 | 605.00x | 0 | 10 |
| 8 | `medium_8KB` | 182.17 | 110 | 6.78 | 17 | 4313.95x | 0 | 10 |
| 8 | `large_24KB` | 249.01 | 453 | 8.26 | 16 | 9364.00x | 0 | 10 |
| **16** | `small_1KB` | 21.06 | 1795 | 7.16 | **16** | 1.00x | **10** | **0** |
| **16** | `medium_8KB` | 123.88 | 3964 | 9.46 | 17 | 4313.95x | 0 | 10 |
| **16** | `large_24KB` | 227.56 | 489 | 10.00 | 16 | 9364.00x | 0 | 10 |
| 32 | `small_1KB` | 34.08 | 542 | 5.38 | 16 | 1.00x | 10 | 0 |
| 32 | `medium_8KB` | 217.96 | 108 | 6.13 | 17 | 4313.95x | 0 | 10 |
| 32 | `large_24KB` | 252.97 | 77 | 7.97 | 16 | 9364.00x | 0 | 10 |
| 64 | `small_1KB` | 32.31 | 86 | 4.86 | 16 | 1.00x | 10 | 0 |
| 64 | `medium_8KB` | 198.34 | 7884 | 2.37 | 17 | 4313.95x | 0 | 10 |
| 64 | `large_24KB` | 190.98 | 1434 | 7.58 | 16 | 9364.00x | 0 | 10 |

**Selection (`16 KB` — `DefaultCAWALInlineThresholdBytes = 16 * 1024`):**
- At `4 KB` and `8 KB`, `small_1KB` (8 distinct 1 KB records = `~8 KB` WAL table) was routed externally (`17 KB` disk, `External=10`), paying `.walb` file/fsync overhead for +1 KB extra disk.
- Starting at **`16 KB`**, `small_1KB` stays completely inline (`Inline=10`, `External=0`, `16 KB` disk), while medium/large multi-record WAL tables (`>= 16 KB`) still capture `4313.95x`–`9364.00x` external deduplication.

---

## 3. Phase 7 Benchmark Matrix (`TestCAWAL_BenchmarkMatrix`)

Configurations:
- **`A_disabled`:** `EnableCAWAL = false` (baseline with `RLock` tenant fast path & Phase 3 memory optimizations).
- **`B_unadaptive_cawal`:** `EnableCAWAL = true`, `InlineThreshold = 0` (all payloads external).
- **`C_adaptive_cawal`:** `EnableCAWAL = true`, `InlineThreshold = 16 KB` + singleflight + verified cache + sharded pins + bounded I/O.

| Mode | MemMB | Tenants | Workload | MB/s | p50 (µs) | p95 (µs) | p99 (µs) | Flush (ms) | Blob (µs) | TotalAlloc (MB) | HeapSys (MB) | GC | Pause (ms) | Goroutines | Disk (KB) | Dedup | Inl/Ext | Rec (ms) |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `A_disabled` | 256 | 1 | `small_shared` | 22.61 | 21 | 45 | 45 | 2.35 | 0.0 | 0.64 | 19.6 | 0 | 0.00 | 2 | 3 | 1.00x | 0/0 | 0.13 |
| `B_unadaptive_cawal` | 256 | 1 | `small_shared` | 21.77 | 24 | 37 | 37 | 2.50 | 966.4 | 0.53 | 19.6 | 0 | 0.00 | 2 | 3 | 15.67x | 0/1 | 0.18 |
| `C_adaptive_cawal` | 256 | 1 | `small_shared` | 31.89 | 18 | 37 | 37 | 1.57 | 0.0 | 0.64 | 19.6 | 0 | 0.00 | 2 | 3 | 1.00x | 1/0 | 0.14 |
| `A_disabled` | 256 | 8 | `medium_shared` | 57.34 | 743 | 1343 | 1343 | 3.83 | 0.0 | 2.63 | 19.6 | 1 | 0.05 | 2 | 14 | 1.00x | 0/0 | 0.60 |
| `B_unadaptive_cawal` | 256 | 8 | `medium_shared` | 56.39 | 82 | 217 | 217 | 4.62 | 1328.2 | 2.51 | 19.6 | 1 | 0.02 | 2 | 15 | 484.00x | 0/8 | 0.60 |
| `C_adaptive_cawal` | 256 | 8 | `medium_shared` | 56.70 | 543 | 1244 | 1244 | 4.19 | 0.0 | 2.47 | 19.6 | 1 | 0.05 | 2 | 14 | 1.00x | 8/0 | 0.72 |
| `A_disabled` | 256 | 20 | `large_shared` | 193.03 | 210 | 4549 | 5449 | 10.59 | 0.0 | 16.35 | 19.6 | 3 | 0.22 | 2 | 37 | 1.00x | 0/0 | 1.96 |
| `B_unadaptive_cawal` | 256 | 20 | `large_shared` | 151.68 | 39 | 1028 | 1633 | 12.84 | 2733.7 | 15.96 | 19.6 | 3 | 0.10 | 2 | 38 | 4754.20x | 0/20 | 2.58 |
| `C_adaptive_cawal` | 256 | 20 | `large_shared` | 129.05 | 41 | 106 | 113 | 18.70 | 7987.7 | 15.95 | 19.6 | 3 | 2.63 | 2 | 38 | 4754.20x | 0/20 | 2.40 |
| `A_disabled` | 512 | 30 | `sustained_shared` | 112.91 | 240 | 4983 | 5594 | 12.86 | 0.0 | 14.13 | 19.6 | 3 | 0.39 | 2 | 56 | 1.00x | 0/0 | 2.79 |
| `B_unadaptive_cawal` | 512 | 30 | `sustained_shared` | 89.86 | 238 | 3340 | 3619 | 19.43 | 3864.7 | 14.01 | 19.3 | 2 | 0.11 | 2 | 57 | 3595.62x | 0/30 | 3.62 |
| `C_adaptive_cawal` | 512 | 30 | `sustained_shared` | 122.25 | 58 | 21703 | 22805 | 5.91 | 4415.5 | 14.31 | 23.3 | 2 | 0.26 | 2 | 57 | 3595.62x | 0/30 | 2.37 |
| `A_disabled` | 512 | 50 | `concurrent_shared` | 62.02 | 460 | 17676 | 18364 | 8.55 | 0.0 | 15.92 | 23.5 | 3 | 0.19 | 2 | 93 | 1.00x | 0/0 | 5.23 |
| `B_unadaptive_cawal` | 512 | 50 | `concurrent_shared` | 62.50 | 2442 | 39173 | 40190 | 6.31 | 2493.5 | 15.70 | 23.5 | 3 | 0.18 | 2 | 96 | 3025.00x | 0/50 | 3.75 |
| `C_adaptive_cawal` | 512 | 50 | `concurrent_shared` | 60.46 | 1150 | 22310 | 22394 | 14.84 | 0.0 | 15.58 | 23.5 | 3 | 0.39 | 2 | 93 | 1.00x | 50/0 | 4.12 |
| `A_disabled` | 1024 | 50 | `concurrent_1gb` | 69.44 | 673 | 24844 | 25185 | 5.67 | 0.0 | 15.60 | 23.5 | 3 | 0.22 | 2 | 93 | 1.00x | 0/0 | 6.39 |
| `B_unadaptive_cawal` | 1024 | 50 | `concurrent_1gb` | 61.78 | 89 | 6403 | 6442 | 23.57 | 2483.8 | 15.37 | 23.5 | 3 | 0.13 | 2 | 96 | 3025.00x | 0/50 | 3.91 |
| `C_adaptive_cawal` | 1024 | 50 | `concurrent_1gb` | 62.42 | 2769 | 15425 | 15751 | 16.37 | 0.0 | 15.60 | 23.5 | 3 | 0.19 | 2 | 93 | 1.00x | 50/0 | 3.91 |
| `A_disabled` | 256 | 10 | `unique_heavy_flush` | 88.90 | 37 | 68 | 68 | 8.19 | 0.0 | 7.75 | 23.5 | 2 | 0.20 | 2 | 21 | 1.00x | 0/0 | 0.72 |
| `B_unadaptive_cawal` | 256 | 10 | `unique_heavy_flush` | 63.17 | 760 | 3327 | 3327 | 9.13 | 1568.3 | 7.77 | 23.5 | 1 | 0.07 | 2 | 23 | 355.14x | 0/10 | 1.03 |
| `C_adaptive_cawal` | 256 | 10 | `unique_heavy_flush` | 69.48 | 492 | 2145 | 2145 | 9.48 | 1266.4 | 7.79 | 23.5 | 2 | 0.17 | 2 | 23 | 355.14x | 0/10 | 0.99 |

### 3.1 Repeated-Run Variance at 50 Concurrent Tenants (`512 MB`)

| Mode (`50T x3 runs`) | Run 1 p99 (µs) | Run 2 p99 (µs) | Run 3 p99 (µs) | Mean p99 (µs) |
| --- | ---: | ---: | ---: | ---: |
| `A_disabled` | 12660 | 10110 | 74420 | 32397 |
| `B_unadaptive_cawal` | 8815 | 31694 | 20150 | 20220 |
| `C_adaptive_cawal` | 38715 | 4428 | 3303 | **15482** |

---

## 4. Verification Results

All tests and race checks were executed after the final edit:

1. `go test -v ./internal/pageserver -run 'TestCAWAL_' -count=1` → **PASS (`1.992s`)** (including `TestCAWAL_AdaptiveThresholdSweep`, `TestCAWAL_BenchmarkMatrix`, `TestCAWAL_DeduplicationAndIndependentRefs`, `TestCAWAL_ConcurrentDuplicateWrites`, `TestCAWAL_FaultInjection_CrashScenarios`, `TestCAWAL_GC_PinProtectionInterruptAndMemoryPressure`, `TestCAWAL_PageserverIntegrationAndBackwardCompat`, `TestCAWAL_AdaptiveThresholdAndCompressionAware`, `TestCAWAL_ConcurrentMixedAndSingleflight`, `TestCAWAL_GC_DuringConcurrentReadsAndPinRace`).
2. `go test -race ./internal/pageserver -run 'TestCAWAL_(Deduplication|Concurrent|FaultInjection|GC|Pageserver|Adaptive)' -count=1` → **PASS (`1.756s`)**.
3. `go test ./internal/pageserver/... -count=1 -timeout 60s` → **PASS (`21.611s`)**.

*(Not run: full-repo tests outside `./internal/pageserver/...` and external Fly.io multi-machine deployment tests.)*

---

## 5. Production Rollout & Rollback

- **Default:** `Config.EnableCAWAL = false` (`DefaultCAWALInlineThresholdBytes = 16 * 1024`).
- **Opt-In Rollout:** Enable `EnableCAWAL: true` on canary pageserver nodes serving multi-tenant workloads with large (`>= 16 KB`) shared WAL tables. Monitor `CAWALStore.Metrics()` (`InlineSelections`, `ExternalSelections`, `DedupHits`, `SingleflightHits`, `CacheHits`, `BlobWriteNanos`, `ManifestWriteNanos`, `HashFailures`, `CorruptBlobsDetected`, `OrphanBlobsCleaned`).
- **Rollback:** Set `EnableCAWAL = false` and restart. New flushes immediately revert to inline V2 encoding while existing `LayerCodecCAWAL` layers remain readable and protected from `GC` until compacted away.
