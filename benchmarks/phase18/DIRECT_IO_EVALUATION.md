# Phase 18E — Experimental Direct I/O (`O_DIRECT`) Evaluation

**Date**: 2026-11-22  
**Filesystem**: Linux ext4 (`/dev/sdc1` on `/tmp`), 4096-byte logical block alignment (`directIOL2FileSize = 12288` bytes = `8192` PG page + `4` CRC32C + `4092` padding).

---

## 1. Implementation & Alignment

- Implemented `L2DirectIO` in `internal/readadapter/cache.go` using `syscall.O_DIRECT`, 4 KiB-aligned buffers from a `sync.Pool` (`alignedSlice`), and 12 KiB L2 files (`directIOL2FileSize = 12288`).
- Implemented automatic fallback to buffered I/O (`l2DirectIOFallbacks++`) whenever `OpenFile`/`Read`/`Write` returns `EINVAL`/`EOPNOTSUPP` (e.g., on `tmpfs` or unaligned filesystems).
- Verified in `TestPhase18E_DirectIOAndFallback` and cgroup v2 ext4 benchmark (`20,408` `O_DIRECT` ops with `0` fallbacks).

---

## 2. Buffered vs `FADV_DONTNEED` vs `O_DIRECT` (512MB Cgroup, 20 Tenants, 512 Pages/Tenant)

| Metric | Buffered L2 | `FADV_DONTNEED` L2 | `O_DIRECT` L2 | `O_DIRECT` vs Buffered |
| :--- | ---: | ---: | ---: | ---: |
| Throughput (QPS) | 492.6 | 386.3 | 342.8 | **-30.4%** |
| p50 Latency (ms) | 7.06 | 9.26 | 10.53 | **+49.2%** |
| p95 Latency (ms) | 15.39 | 18.95 | 21.19 | **+37.7%** |
| p99 Latency (ms) | 22.21 | 29.16 | 33.81 | **+52.2%** |
| Process RSS (MB) | 43.86 | 44.44 | 42.22 | -3.7% |
| Cgroup Peak (MB) | 71.06 | 68.91 | 40.41 | **-43.1%** |
| Kernel `file` Pagecache (MB) | 24.06 | 23.84 | **0.07** | **-99.7%** |
| Disk Bytes for 2,048 Entries | 16.01 MiB | 16.01 MiB | 24.00 MiB | **+50.0% disk** |

---

## 3. Recommendation

- **Keep `L2DirectIO` optional (`default: false`)**, NOT the default path.
- **Why**: While `O_DIRECT` eliminates 99.7% of L2 kernel page cache (`0.07 MB` vs `24.06 MB`), it imposes a **30.4% throughput penalty**, **+49% p50 latency**, and **+50% disk amplification** (8,196 B -> 12,288 B per page), and requires filesystem `O_DIRECT` support (failing over to buffered I/O on `tmpfs`).
- For 512MB nodes, **strictly bounded L2 (`L2MaxBytes`, `L2MaxEntries`, `L2MaxConcurrentIO`, `L2BypassUnderPressure`)** or **`L2Disabled: true`** already achieves safe cgroup peaks (`40–130 MB`) with much higher throughput. `L2DirectIO: true` remains available as an opt-in knob for page-cache-sensitive NVMe deployments.
