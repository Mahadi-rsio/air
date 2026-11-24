# Phase 23 — Adaptive CA-WAL Performance Forensics

## 1. Executive Summary

Profiling the Phase 22 CA-WAL implementation (`TestCAWAL_BenchmarkMatrix` on `linux/amd64`, `AMD EPYC 7763`, `GOMAXPROCS=2`) shows that **SHA-256 hashing is not the primary bottleneck** (`8.89%` CPU, `1572.09 MB/s`, `0 B/op`, `0 allocs/op`). Instead, the p99 write latency regression at 50 concurrent tenants (`5498 µs` baseline → `16157 µs` with CA-WAL) is caused by a combination of:

1. **Global mutex serialization on `IngestWALRecords` (`98.44%` of mutex delay):** `Pageserver.SetTenantGeneration` (`61.64%`) and `Pageserver.EnsureTenant` (`36.31%`) acquire an exclusive `p.mu.Lock()` on every `IngestWALRecords` batch even when the tenant and generation are unchanged.
2. **Unconditional external `.walb` file creation + double `fsync` for small payloads:** `v2StreamWriter.Finish` sends *every* non-encrypted V2 WAL table—even `< 4 KB` payloads—through `CAWALStore.PutBlobIfAbsent`, triggering `os.MkdirAll`, `os.OpenFile`, `f.Write`, `f.Sync()`, `f.Close()`, `os.Link`, `os.Remove`, and `syncDirBestEffort()`.
3. **Flush slot contention (`52.68%` of block delay):** Slow external `.walb` I/O holds `MemoryBudget.acquireFlushSlot` longer, causing `3.12s` of cumulative block delay across concurrent tenants and delaying scheduler progress for `IngestWALRecords` goroutines.
4. **Redundant work on concurrent duplicate writes:** Without per-hash singleflight coordination, `N` concurrent tenants writing an identical WAL table all miss `statAndVerifyHeader`, all compress the payload, all write and `f.Sync()` separate `.tmp` files, and `N-1` collide on `os.Link` (`EEXIST`).
5. **Disk I/O on every deduplication hit:** `statAndVerifyHeader` executes `os.Open`, `f.Stat`, `f.ReadAt`, and `f.Close` on disk for every dedup hit instead of consulting a bounded verified-blob cache or in-process index.

---

## 2. Full CA-WAL Write Path Trace

1. **Ingestion (`internal/pageserver/grpc.go:112` — `Pageserver.IngestWALRecords`):**
   - Calls `p.SetTenantGeneration(tenantID, generation)` (`server.go:288`) → takes `p.mu.Lock()`.
   - Calls `p.EnsureTenant(tenantID, epoch)` (`server.go:424`) → takes `p.mu.Lock()`.
   - Enforces `InMemBytes` budget via `tl.EnforceInMemBudget(ctx)` and applies records via `tl.ApplyWALRecord(rec)`.
2. **Flush (`internal/pageserver/readpath.go:677` — `Timeline.FlushInMemory`):**
   - Acquires a flush slot (`MemoryBudget.acquireFlushSlot`), snapshots `InMemoryLayer`, and calls `LayerStore.WriteLayer` (`layers.go:745`).
   - Calls `EncodeLayerV2ToWriter` → `v2StreamWriter.Finish()` (`layer_v2.go:355`).
3. **WAL Table Serialization & CA-WAL Write (`internal/pageserver/layer_v2.go:360-417`):**
   - Packs `sw.walTable` into `walRaw`.
   - When `sw.opts.CAWAL != nil && !sw.encrypted`, unconditionally calls `sw.opts.CAWAL.PutBlobIfAbsent(cctx, walRaw)` (`layer_v2.go:381`), pins the hash (`Pin`), writes the 32-byte raw SHA-256 digest into the `.layer` file with `LayerCodecCAWAL = 2`, and defers `Unpin` in `FlushInMemory` (`readpath.go:730`) / `compactL0s` (`compactor.go:173`) after `PublishLayerManifestGen`.
4. **Blob Persistence (`internal/pageserver/cawal.go:266` — `CAWALStore.PutBlobIfAbsent`):**
   - Computes `sha256.Sum256(raw)`.
   - Checks `statAndVerifyHeader(finalPath, ...)` (`Open` + `Stat` + `ReadAt` + `Close`).
   - On miss: `os.MkdirAll`, `encodeCAWALBlob` (Zstd compression), `os.OpenFile` (`.tmp`), `f.Write`, `f.Sync()`, `f.Close()`, `os.Link(tmpPath, finalPath)`, `os.Remove(tmpPath)`, `syncDirBestEffort(dir)`.
   - On `os.ErrExist` race: re-runs `statAndVerifyHeader`.

---

## 3. Profiling Methodology & Measured Bottlenecks

Profiles were captured simultaneously from `TestCAWAL_BenchmarkMatrix` (`1`, `10`, `20`, `50` tenants; `256 MB` and `512 MB` profiles):

```bash
go test ./internal/pageserver -run '^TestCAWAL_BenchmarkMatrix$' -count=1 \
  -cpuprofile=/tmp/cawal_cpu.prof \
  -mutexprofile=/tmp/cawal_mutex.prof \
  -blockprofile=/tmp/cawal_block.prof \
  -memprofile=/tmp/cawal_mem.prof
```

### 3.1 CPU Profile (`450ms` total samples over `479.61ms`)

| Function / Syscall | Flat | Cum | Share | Source / Cause |
| --- | ---: | ---: | ---: | --- |
| `internal/runtime/syscall/linux.Syscall6` | 150ms | 150ms | 33.33% | File open/write/fsync/link/unlink syscalls during flush & CA-WAL |
| `syscall.Syscall` | 0ms | 110ms | 24.44% | Directory/file metadata syscalls (`Stat`, `Close`, `ReadAt`) |
| `(*Pageserver).FlushTenant` / `FlushInMemory` | 0ms | 210ms | 46.67% | Layer encoding + `WriteLayer` + `PutBlobIfAbsent` + manifest sync |
| `(*LayerStore).WriteLayer` | 0ms | 140ms | 31.11% | V2 `.layer` + `.walb` persistence |
| `crypto/internal/fips140/sha256.blockSHANI` | 40ms | 40ms | 8.89% | Hardware-accelerated SHA-NI hashing (NOT the bottleneck) |
| `runtime.madvise` | 30ms | 30ms | 6.67% | Heap page scavenging |

### 3.2 Mutex Contention Profile (`1421.17ms` total delay)

| Call Path | Cum Delay | Share | Root Cause |
| --- | ---: | ---: | --- |
| `(*Pageserver).IngestWALRecords` | 1414.95ms | 99.56% | Exclusive `p.mu.Lock()` on every batch |
| ↳ `(*Pageserver).SetTenantGeneration` (`server.go:288`) | 875.95ms | 61.64% | Takes `p.mu.Lock()` even when `p.generations[tenantID] == generation` |
| ↳ `(*Pageserver).EnsureTenant` (`server.go:424`) | 515.98ms | 36.31% | Takes `p.mu.Lock()` even when `p.tenants[tenantID]` already exists |
| `(*Timeline).EnforceInMemBudget` | 16.14ms | 1.14% | `dirtyTenantCount` RLock |

### 3.3 Block Profile (`5.92s` total delay)

| Call Path | Cum Delay | Share | Root Cause |
| --- | ---: | ---: | --- |
| `runtime.selectgo` | 3.52s | 59.42% | Waiting on channels (`acquireFlushSlot` / test goroutines) |
| `(*MemoryBudget).acquireFlushSlot` | 3.12s | 52.68% | Flush concurrency slots held longer during `.walb` fsyncs |
| `sync.(*RWMutex).Lock` (`IngestWALRecords`) | 1.52s | 25.66% | Waiting on `p.mu.Lock()` in `SetTenantGeneration` / `EnsureTenant` |

### 3.4 Memory Allocation Profile (`148.79MB` total `alloc_space`)

| Function | Flat | Cum | Share | Cause |
| --- | ---: | ---: | ---: | --- |
| `makeSyntheticWALRecords` (test harness) | 39.58MB | 75.69MB | 50.87% | Benchmark record generation |
| `(*Timeline).FlushInMemory` | 0.50MB | 50.10MB | 33.67% | `WriteLayer` + `EncodeLayerV2ToWriter` |
| `(*v2StreamWriter).Finish` | 10.73MB | 19.02MB | 12.78% | `walRaw` + `idxRaw` assembly & WAL table encoding |
| `newV2StreamWriter` (`bufio.NewWriterSize`) | 13.83MB | 15.34MB | 10.31% | 64 KiB `bufio.Writer` allocation per layer |

---

## 4. Hotspot Classification & Optimization Opportunities

| # | Hotspot | File & Function | Optimization Opportunity (Phase) |
| --- | --- | --- | --- |
| 1 | Unconditional external `.walb` for tiny payloads | `layer_v2.go:376` (`v2StreamWriter.Finish`) | **Phase 2:** Adaptive inline vs external threshold (benchmark 4–64 KB); keep small WAL tables inline in V2 `.layer` |
| 2 | `SetTenantGeneration` & `EnsureTenant` exclusive `p.mu.Lock()` | `server.go:288,424` | Fast-path `p.mu.RLock()` check before upgrading to `p.mu.Lock()` on miss/change |
| 3 | Duplicate work & `os.Link` races on identical hashes | `cawal.go:266` (`PutBlobIfAbsent`) | **Phase 4:** Per-hash in-process singleflight + bounded verified-blob metadata cache to skip disk `statAndVerifyHeader` on known-good blobs |
| 4 | Recompression / redundant compression of small or incompressible payloads | `cawal.go:193` (`encodeCAWALBlob`), `layer_v2.go:376` | **Phase 3:** Compression-aware CA-WAL (`PutPreencodedBlobIfAbsent` / codec-aware framing) so payloads are compressed once |
| 5 | Unbounded concurrent disk blob writes saturating I/O | `cawal.go:266` | **Phase 6:** Bounded semaphore-based CA-WAL I/O scheduler with context cancellation & non-blocking backpressure |
| 6 | Manifest `HasBlob` stat loop & `MarshalIndent` | `cawal.go:516` (`PublishManifest`) | **Phase 5:** Compact `json.Marshal`, verified-set fast check, avoid redundant manifest rewrites |
| 7 | Global `pinMu` mutex | `cawal.go:159-187` | Sharded pin map or lock-free/low-contention ref tracking |
