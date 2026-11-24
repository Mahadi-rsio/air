# AIR Phase 24 — Phase 4: Memory & GC Stability

- **Command**: `go test -v ./internal/pageserver -run '^TestPhase24_MemoryAndGCStability$' -count=1 -timeout 180s`
- **Profiles Tested**: `256 MB` (`air-pg-pageserver` Fly.io profile) and `512 MB` (`air-pg-server` Fly.io profile) across `1, 20, 50, 100` tenants, with `EnableCAWAL = false` and `true`.
- **Workload**: Sustained WAL ingestion (`20 x 5 x 2048 B` per tenant), periodic `FlushTenant` (every 5 batches), `CompactL0Batch`, and `GetPageAtLSN` reads, followed by `ps.Close()` + `runtime.GC()` to measure retained memory.

---

## 1. Post-Fix Memory & GC Measurements

| Profile (MB) | Tenants | CA-WAL | Peak RSS (VmHWM MB) | HeapAlloc (MB) | HeapInuse (MB) | HeapIdle (MB) | HeapReleased (MB) | TotalAlloc (MB) | Alloc Rate (MB/s) | NumGC | GC Pause (ms) | Goroutines | Retained After GC (MB) |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 256 | 1 | `false` | 70.92 | 4.66 | 6.11 | 53.36 | 42.54 | 1.18 | 89.73 | 0 | 0.00 | 3 | 3.60 |
| 256 | 1 | `true` | 70.92 | 4.65 | 6.01 | 53.46 | 51.29 | 1.24 | 122.97 | 0 | 0.00 | 3 | 3.66 |
| 256 | 20 | `false` | 70.92 | 10.44 | 11.88 | 47.59 | 41.72 | 20.48 | 272.25 | 4 | 1.12 | 3 | 3.78 |
| 256 | 20 | `true` | 70.92 | 13.68 | 14.91 | 44.55 | 42.16 | 20.02 | 227.75 | 3 | 1.46 | 3 | 3.84 |
| 256 | 50 | `false` | 70.92 | 19.61 | 21.18 | 38.26 | 36.70 | 50.32 | 223.45 | 6 | 0.71 | 3 | 3.78 |
| 256 | 50 | `true` | 70.92 | 24.70 | 26.02 | 33.42 | 31.80 | 48.86 | 163.54 | 5 | 0.57 | 3 | 3.84 |
| 256 | 100 | `false` | 70.92 | 37.73 | 39.55 | 19.88 | 8.23 | 99.88 | 252.26 | 8 | 1.70 | 3 | 3.78 |
| 256 | 100 | `true` | 70.92 | 35.64 | 37.32 | 22.12 | 7.91 | 96.98 | 236.80 | 8 | 2.32 | 3 | 3.79 |
| 512 | 1 | `false` | 70.92 | 4.62 | 6.02 | 53.45 | 26.76 | 1.19 | 128.33 | 0 | 0.00 | 3 | 3.60 |
| 512 | 1 | `true` | 70.92 | 4.66 | 6.02 | 53.45 | 36.32 | 1.24 | 137.79 | 0 | 0.00 | 3 | 3.67 |
| 512 | 20 | `false` | 70.92 | 14.19 | 15.53 | 43.94 | 41.27 | 20.48 | 252.90 | 3 | 0.16 | 3 | 3.79 |
| 512 | 20 | `true` | 70.92 | 13.20 | 14.47 | 45.00 | 41.80 | 20.02 | 252.54 | 3 | 0.15 | 3 | 3.85 |
| 512 | 50 | `false` | 70.92 | 20.97 | 22.20 | 37.23 | 36.36 | 49.97 | 208.94 | 6 | 1.17 | 3 | 3.79 |
| 512 | 50 | `true` | 70.92 | 21.09 | 22.38 | 37.06 | 36.27 | 48.80 | 207.24 | 6 | 2.54 | 3 | 3.79 |
| 512 | 100 | `false` | 70.92 | 36.83 | 38.64 | 20.80 | 7.88 | 99.63 | 250.44 | 8 | 1.33 | 3 | 3.78 |
| 512 | 100 | `true` | 70.92 | 37.05 | 38.74 | 20.70 | 8.55 | 96.82 | 235.45 | 8 | 2.03 | 3 | 3.79 |

---

## 2. Bounds & Retention Verification

- **OOM Margin**: Peak RSS (`VmHWM = 70.92 MB`) stayed well under both the `256 MB` (`27.7%`) and `512 MB` (`13.9%`) machine profiles even at `100` concurrent tenants.
- **Zero Goroutine & Heap Leaks**: Across all 16 combinations, `Goroutines` returned to `3` and `Retained After GC` stayed flat at `3.60–3.85 MB` regardless of tenant count (`1` vs `100`) or `EnableCAWAL` (`false` vs `true`).
- **Bounded Caches & Pools**:
  - `CAWALStore.verified`: capped at `4096` entries (`cawal.go:373-383`).
  - `v2RawChunkPool`, `v2CompChunkPool`, `cawalCompPool`: capped at `512 KiB` slice capacity (`layer_v2.go:43-54`, `cawal.go:41-46`).
  - `MemoryBudget`: `dirtyTenants` is tracked lock-free (`atomic.Int64`), background flushes/compactions are tracked by `MemoryBudget.wg` and cancelled on `Close()`.
- **Remaining Long-Lived Map Note**: `Timeline.relSizes` / `relSizeKnown` (`readpath.go:61-62`) and `WriteGovernor.tenants` / `MetricsRegistry.tenants` (`write_governor.go:145`, `metrics.go:199-200`) still grow with distinct relation/tenant count across process lifetime; neither caused measurable retention in `1–100` tenant tests.
