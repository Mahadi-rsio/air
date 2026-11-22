# Phase 18D — Strictly Bounded L2 Disk Cache Benchmarks

**Date**: 2026-11-22  
**Environment**: Linux cgroup v2 (`memory.max=512MB`, `memory.swap.max=0`), `/tmp` ext4, 20 tenants, 512 pages/tenant, `L1=1MB`, `L2MaxBytes=32MB`, `L2MaxEntries=2048`, 4 concurrent workers, 5s per run.

---

## 1. L2 Cache Mode Comparison (512MB Cgroup v2)

| L2 Mode | QPS | p50 (ms) | p95 (ms) | p99 (ms) | Process RSS (MB) | Cgroup Peak (MB) | Cgroup File Pagecache (MB) | L2 Hits | L2 Evictions | L2 Entries | L2 Bytes |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `disabled` | 1273.3 | 2.48 | 5.92 | 9.54 | 43.95 | — (parent ns) | — | 0 | 0 | 0 | 0 |
| `buffered` (strictly bounded) | 492.6 | 7.06 | 15.39 | 22.21 | 43.86 | 71.06 | 24.06 | 160 | 26,416 | 2,048 | 16,785,408 |
| `fadvise` (`FADV_DONTNEED`) | 386.3 | 9.26 | 18.95 | 29.16 | 44.44 | 68.91 | 23.84 | 149 | 20,036 | 2,048 | 16,785,408 |
| `direct` (`O_DIRECT`, 12KB aligned) | 342.8 | 10.53 | 21.19 | 33.81 | 42.22 | 40.41 | 0.07 | 127 | 17,616 | 2,048 | 25,165,824 |

---

## 2. Phase 17 vs Phase 18 (`micro-512mb`, 50 tenants `mixed`)

| Metric | Phase 17 (Unbounded L2) | Phase 18 (`fadvise`, `L2MaxBytes=32MB`, `L2MaxEntries=4096`, `L2MaxConcurrentIO=4`) | Delta |
| :--- | ---: | ---: | ---: |
| Process RSS | 93.7 MB | 75.53 MB | **-19.4%** |
| Cgroup `memory.peak` | 381.0 MB | 130.13 MB | **-65.8%** |
| L2 Bypasses under I/O saturation | 0 | 3,202 | Bounded I/O queue |
| L2 Entries / Disk Bytes | Unbounded | 4,094 / 33,554,424 B | Strictly capped |

---

## 3. Key Findings

1. **Strict Bounds Work**: `L2Entries` never exceeded `L2MaxEntries` (2,048 in the L2 thrash test, 4,096 in the 50-tenant test), and `L2Bytes` stayed within `L2MaxBytes`.
2. **Non-Blocking I/O Bypass Prevents Heap Bloat**: Bounding concurrent disk I/O (`L2MaxConcurrentIO=4`) and bypassing writes when saturated (`l2_bypasses=3,202` in 50-tenant `mixed`) cut cgroup peak from **381 MB** (Phase 17) down to **130.13 MB**.
3. **512MB Node Profile**: `LowMemory512MBProfile` (`L1=16MB`, `L2=64MB`, `L2MaxEntries=4096`, `L2MaxConcurrentIO=4`, `L2BypassUnderPressure=true`, `L2DropPageCache=true`) keeps 50-tenant mixed workloads comfortably under 135 MB total cgroup memory, leaving >375 MB headroom in a 512 MB node.
