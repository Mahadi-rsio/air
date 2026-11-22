# Phase 18D — Strictly Bounded L2 Disk Cache Design

**Date**: 2026-11-22  
**Component**: `internal/readadapter/cache.go` (`TieredCacheBackend`, `CacheConfig`), `internal/readadapter/sqlite_server.go` (`SQLiteServerConfig`)

---

## 1. Motivation (Phase 17 Finding)

In Phase 17, `micro-512mb` `mixed` reported ~93.7 MB process RSS while Linux cgroup `memory.peak` reached ~381 MB because unbounded buffered `os.ReadFile` / `os.WriteFile` L2 cache operations populated the Linux kernel page cache (`file` memory in `memory.stat`), and `TieredCacheBackend` lacked entry-count caps, entry-size caps, concurrency bounds, and memory-pressure bypass.

---

## 2. Configuration & Strict Bounds

`CacheConfig` and `SQLiteServerConfig` now expose:

| Parameter | Purpose | `LowMemory512MBProfile` Default |
| :--- | :--- | :--- |
| `L2Disabled` | Completely disable L2 disk cache | `false` (or `true` if `CacheDir == ""`) |
| `L2MaxBytes` | Strict maximum total disk bytes | `64 MiB` |
| `L2MaxEntries` | Strict maximum number of L2 files/metadata entries | `4096` |
| `L2MaxEntryBytes` | Maximum individual entry size (larger entries bypass L2) | `16 KiB` |
| `L2MaxConcurrentIO` | Semaphore bounding concurrent L2 disk reads/writes | `4` (general default `8`) |
| `L2BypassUnderPressure` | Bypass L2 writes when `MemPressureFn()` triggers or I/O slots saturate | `true` |
| `L2DropPageCache` | Issue `posix_fadvise(..., FADV_DONTNEED)` after L2 reads/writes | `true` |
| `L2DirectIO` | Experimental `O_DIRECT` with 4 KiB-aligned buffers & automatic fallback | `false` |

---

## 3. Eviction, Concurrency & Crash Safety

1. **Bounded Metadata Memory**:
   - `l2Map` and `l2List` (LRU) are strictly bounded by both `L2MaxBytes` and `L2MaxEntries`.
   - `registerL2Entry` evicts oldest entries (up to 16 per step until within both byte and entry caps) and deletes the corresponding `.pg` files.
2. **Non-Blocking Admission & Bypass**:
   - `putL2` checks `L2BypassUnderPressure` (`MemPressureFn()`), `L2MaxEntryBytes`, and uses non-blocking `acquireL2IO(true)` on `l2IOSem`; if all concurrent I/O slots are busy, L2 write is bypassed (`l2Bypasses++`) rather than queueing goroutines or buffering pages in heap.
3. **Crash-Safe Atomic Writes & Startup Reconciliation**:
   - Writes go to `<hash>.pg.tmp.<pid>.<seq>`, are `FADV_DONTNEED`-advised and closed, then atomically renamed to `<hash>.pg`. On write/close/rename error, the temp file is immediately removed.
   - `reconcileL2DirOnStartup` removes leftover `.tmp.` files and invalid-sized `.pg` files, and trims oldest files by `mtime` to satisfy both `maxBytes` and `maxEntries`.
4. **Correctness & Invalidation**:
   - L2 is never authoritative. Every page is keyed by `(TenantID, Generation, SpcOID, DbOID, RelNumber, ForkNum, BlockNum, LSN)` and protected by a CRC32C Castagnoli checksum (`PGPageSize + 4` bytes, or `12288` bytes aligned under `O_DIRECT`).
   - Corrupt or truncated files are evicted immediately (`L2Corrupt++`) and transparently recovered from L3 Pageserver.
   - `InvalidateWhere` removes matching L1, L2, and relation metadata entries on WAL advancement or schema/generation changes.
