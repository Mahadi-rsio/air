# AIR Phase 22 — Pageserver Memory Forensics & Multi-Tenant Root-Cause Report

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Base Commit:** `cb2ec11` (`feat(phase21.10): fix air_zeroextend persistence, Pageserver recovery, Read Engine DDL visibility, and RSS metrics`)  
**Objective:** Investigate why AIR Pageserver (`air-pageserver`, `256 MB` cgroup v2 limit) reaches OOM (`ExitCode=137`) during multi-tenant bootstrap and active workloads, without increasing the production `256 MB` RAM limit.

---

## 1. Executive Summary & Root-Cause Ranking

Using Go `pprof` (`inuse_space`, `alloc_space`), `runtime.MemStats`, Linux `/proc/<pid>/status` (`VmRSS`, `VmHWM`, `RssAnon`, `RssFile`), and Docker cgroup v2 (`memory.current`, `memory.peak`, `memory.stat`, `memory.events`), we reproduced the exact Phase 21.10 OOM and isolated **six distinct root causes** that together consume **`> 50 MB` of RAM per active tenant** on `air-pageserver`, causing a `256 MB` Pageserver to OOM-kill during the 5th–10th tenant bootstrap:

| Rank | Root Cause | Location | Measured Impact per Tenant / Global |
|---:|---|---|---|
| **1** | **Remote `BootstrapTenantFromPGData` buffers all 3 `initdb` databases (`template1`, `template0`, `postgres`, ~`22.6 MB` raw = `2,890` blocks) in `InMemoryLayer` via `PutPageBatch`, triggering two overlapping async L0 flushes (`frozenMems`) plus a third synchronous `FlushTenant`** | [`GRPCPageserverClient.BootstrapTenantFromPGData`](file:///home/antigravity/Mahadi-rsio/air/internal/control/pageserver_client.go#L348-L593), [`Timeline.PutPageBatch`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/readpath.go#L957-L1013) | **`+22.6 MB` to `+38 MB` heap spike per bootstrapping tenant** (`PutPageBatch` accounts for **`78.2%–89.1%`** of live Go heap in `pprof`). |
| **2** | **`ActivateCluster` runs `CREATE DATABASE <tenant>` *after* `BootstrapTenantFromPGData`, which copies `template1` (`7.3 MB` = ~910 blocks) via both `air_smgr` `PutPageBatch` *and* `WALReceiver` full-page-image WAL records into `InMemoryLayer` without flushing** | [`ControlPlane.ProvisionDatabase`](file:///home/antigravity/Mahadi-rsio/air/internal/control/control.go#L643-L678), [`Agent.ActivateCluster`](file:///home/antigravity/Mahadi-rsio/air/internal/cell/agent.go#L305-L382) | **`+14.8 MB` retained dirty `InMemoryLayer` per newly provisioned tenant** before a single user query executes! |
| **3** | **Duplicate storage of every 8 KiB page in `InMemoryLayer`: both `air_smgr` (`PutPageBatch`) and `WALReceiver` (`IngestWALRecords`) append separate 8 KiB `WALDelta` slices to `m.entries[key]` for the same block without coalescing/overwriting older in-memory page images** | [`InMemoryLayer.Put`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/layers.go#L96-L107), [`Timeline.ApplyWALRecord`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/readpath.go#L363-L410) | **2x `InMemoryLayer` bloat** (`16 MB` soft limit per tenant holds only ~1,000 unique blocks instead of ~2,000, and 10 active tenants hold `160–240 MB` of un-evicted `InMemoryLayer` slices). |
| **4** | **No Global `InMemoryLayer` Budget & High Per-Tenant `defaultMaxInMemBytes` (`16 MiB` soft / `32 MiB` hard) with no idle/pressure background flush** | [`Timeline`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/readpath.go#L13-L14) | 10 active tenants * `16 MiB` soft limit = **`160 MiB` (`320 MiB` hard limit)** in `InMemoryLayer` alone, exceeding the entire `256 MB` container limit. |
| **5** | **Unbounded per-tenant retained metadata & caches after bootstrap and suspension (`Timeline.slruPages`, `Timeline.modLog`, `DecodedChunkCache` `64 MiB`, `boundedIndexCache` `64 MiB`) + Go Heap Idle retention (`HeapIdle` not scavenged promptly)** | [`Timeline`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/readpath.go#L36-L67), [`NewLayerStore`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/layers.go#L445-L468) | `DecodedChunkCache` (`64 MiB`) + `boundedIndexCache` (`64 MiB`) = **`128 MiB`** cache ceiling; `modLog` (`65,536` entries = `1.83 MiB`/tenant); `slruPages` never evicted on flush/suspend; Go runtime holds **`115–321 MiB` in `HeapIdle`** (counted against cgroup `anon` RSS) unless constrained by a soft memory limit / scavenger. |
| **6** | **Cgroup v2 Page Cache (`file`) + `DiskCache` (`256 MiB` default) share the same `256 MB` container cgroup limit as Go `anon` RSS** | [`cmd/air-pageserver/main.go`](file:///home/antigravity/Mahadi-rsio/air/cmd/air-pageserver/main.go#L83), [`LayerStore.WriteLayer`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/layers.go#L645-L743) | Every layer write and read populates Linux kernel page cache (`memory.stat: file`) inside the `air-pageserver` cgroup (`3–25 MB`), reducing available `anon` headroom below `230 MB`. |

---

## 2. Phase 1: Component-by-Component Forensics Measurements

### 2.1 `InMemoryLayer` Memory Consumption (Per-Tenant & Global)
- **Code Inspection ([`internal/pageserver/layers.go`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/layers.go#L78-L143)):**
  - `InMemoryLayer` stores `entries map[Key][]WALDelta`.
  - Every `PutPageBatch` call allocates a fresh `make([]byte, 8192)` per block ([`readpath.go:977`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/readpath.go#L977)) and appends a `WALDelta{Flags: EntryFlagPageImage, PageImage: cp}` to `m.entries[key]`.
  - Simultaneously, `WALReceiver` streams physical WAL records to `IngestWALRecords` -> `ApplyWALRecord` ([`readpath.go:381-391`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/readpath.go#L381-L391)). When `full_page_writes=on` (or during `CREATE DATABASE` / checkpoint FPIs), `RestoreBkpImage` allocates another `make([]byte, 8192)` and appends it to `m.entries[key]`, **plus** retains `rec.RawRecord` (`~8.2 KB` for FPI records).
  - Crucially, `InMemoryLayer.Put` **never compacts or drops superseded deltas in-place** when a newer `EntryFlagPageImage` arrives for the same key in the same `InMemoryLayer`! If block `k` is updated 10 times via `PutPageBatch` before an L0 flush, `m.entries[k]` holds all 10 full 8 KiB page images (`80 KiB`) in RAM even though only the newest `PageImage` (or LSN-qualified snapshot) is needed.
  - Furthermore, `ApproxBytes` only counts `len(delta.PageImage) + len(delta.WALRecord) + 64`, ignoring Go `map[Key][]WALDelta` bucket overhead and slice growth capacity slack (~15–25% additional heap).
- **Measured Values (`pprof` & Forensics Harness):**
  - After bootstrapping 1 tenant via `GRPCPageserverClient`: **`12.28 MB` live Go heap (`30.16 MB` `RssAnon`, `47.86 MB` `VmRSS`)**.
  - After bootstrapping 10 tenants (with 1 flushed L0 layer + post-bootstrap `InMemoryLayer` writes): **`240.83 MB` live Go heap (`284.94 MB` `RssAnon`, `302.69 MB` `VmRSS`)**.
  - In `pprof` (`inuse_space`), `(*Timeline).PutPageBatch` accounts for **`193.00 MB` (`78.20%`)** and `RestoreBkpImage` accounts for **`24.19 MB` (`9.80%`)** of the `246.80 MB` live heap.

### 2.2 Remote Bootstrap (`BootstrapTenantFromPGData`) & Provisioning Amplification
- **Code Inspection ([`internal/control/pageserver_client.go:348-593`](file:///home/antigravity/Mahadi-rsio/air/internal/control/pageserver_client.go#L348-L593)):**
  - Inside `air-server`, `initdb` creates `global/` (`576 KB`), `base/1` (`template1`, `7.3 MB`), `base/4` (`template0`, `7.3 MB`), and `base/5` (`postgres`, `7.4 MB`) — **898 files totaling `22.6 MB` (`~2,890` pages)**.
  - Unlike in-process `Pageserver.BootstrapTenantFromPGData` (which streams sorted blocks directly to an `img_bootstrap` file on disk via `v2StreamWriter` with a 64 KiB buffer), **`GRPCPageserverClient.BootstrapTenantFromPGData` loops over all 898 files and calls `PutPageBatch` over gRPC**!
  - Because `22.6 MB > defaultMaxInMemBytes (16 MB)`, `PutPageBatch` triggers an asynchronous background flush (`MaybeFlushAsync`) mid-bootstrap while continuing to pump the remaining `6.6 MB` into a second `InMemoryLayer`, followed by `SetRelSizes`, `SeedSLRUPages`, and a second synchronous `FlushTenant` RPC.
  - Then, [`ProvisionDatabase`](file:///home/antigravity/Mahadi-rsio/air/internal/control/control.go#L674) calls [`ag.ActivateCluster`](file:///home/antigravity/Mahadi-rsio/air/internal/cell/agent.go#L371), which executes `CREATE DATABASE <tenantID> OWNER app_user`.
  - Under `air_smgr`, `CREATE DATABASE` copies `template1` (`7.3 MB`, ~910 blocks) into `base/16385` by sending `PutPageBatch` RPCs (`7.3 MB`) **and** emitting `XLOG_DBASE_CREATE_FILE_COPY` / FPI WAL records (`+7.3 MB`) via `WALReceiver`.
  - Total dirty memory added during `ActivateCluster` (`~14.6 MB`) sits just below `defaultMaxInMemBytes` (`16 MB`), so **it is never flushed after provisioning completes**! Every newly provisioned idle tenant starts life holding `~14.6 MB` of dirty `InMemoryLayer` pages in Pageserver RAM.

### 2.3 WAL Ingestion Buffers & Temporary Allocations
- **Code Inspection ([`internal/pageserver/grpc.go:527-581`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/grpc.go#L527-L581), [`walparser.go`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/walparser.go)):**
  - `WALStreamParser.Push` ([`walparser.go:414,479,505,527`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/walparser.go#L479)) repeatedly executes `p.buf = append([]byte(nil), p.buf[XLogPageSize:]...)` after every single 8 KiB WAL page, reallocating the entire remaining WAL chunk O(N^2) times.
  - `DecodeWALRecord` copies `RawRecord`, `ImagePayload`, `BlockData`, and `MainData` into newly allocated slices.
  - When `ApplyWALRecord` processes a record with `blk.HasImage == true`, it decompresses `blk.ImagePayload` into an 8 KiB ` delta.PageImage`, **and also keeps `delta.WALRecord = rec.RawRecord`** (which itself embeds the `ImagePayload`!). Storing `rec.RawRecord` alongside an already-materialized 8 KiB `EntryFlagPageImage` in `WALDelta` duplicates 4–8 KiB per FPI WAL record for zero benefit (since `prepareRedo` discards all `WALRecord`s at or before an `EntryFlagPageImage`!).
- **Measured Values (`pprof` `alloc_space`):**
  - `google.golang.org/protobuf/internal/impl.consumeBytesNoZero`: **`207.09 MB` (`14.52%`)** of total allocations.
  - `google.golang.org/grpc/internal/mem.(*sizedBufferPool).Get`: **`52.96 MB` (`3.71%`)** of total allocations (`15.91 MB` retained in gRPC buffer pools in `inuse_space`).

### 2.4 Page Cache, Decoded-Chunk Cache, Index Cache, and DiskCache
- **Code Inspection & Measured Values:**
  1. **`DecodedChunkCache` ([`decoded_chunk_cache.go:71-73`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/decoded_chunk_cache.go#L71-L73)):** Defaults to **`64 MiB`** (`cfg.MaxBytes = 64 * 1024 * 1024`). Each cached V2 chunk is ~64 KiB decompressed.
  2. **`boundedIndexCache` ([`index_cache.go:12`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/index_cache.go#L12)):** Defaults to **`64 MiB`** (`defaultIndexCacheMaxBytes = 64 * 1024 * 1024`) plus up to `4,096` retained Bloom filters (`c.blooms`). Also caches `idxData.walTable [][]byte` when V2 WAL tables are loaded, **without adding `walTable` byte size to `idxData.bytes` or `c.curBytes`**!
  3. **`DiskCache` ([`cache.go`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/cache.go)):** Defaults to `256 MiB` on local disk. While file contents live on disk, reading/writing `DiskCache` files populates the container's Linux kernel page cache (`memory.stat: file`). Note also that when `s.localDir != ""` (which is always true in `air-pageserver`), `LayerStore` already stores immutable layer files directly under `ps_layers/<tenant>/<gen>/`, so caching local layers in `ps_cache` or keeping `CacheMB=256` is redundant when `s3c == nil`.

### 2.5 Layer Metadata & Timeline Structures
- **Code Inspection ([`internal/pageserver/readpath.go:36-67`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/readpath.go#L36-L67)):**
  - `Timeline.slruPages map[Key][]byte`: Caches 8 KiB SLRU pages (`pg_xact`, `pg_subtrans`, `pg_multixact`) in memory forever. Never cleared on `FlushInMemory` or tenant suspend.
  - `Timeline.modLog []ModifiedBlockEvent`: Grows up to `maxTimelineModLogEntries = 65,536` entries (`65536 * 32B = 2.0 MiB` per tenant = `20 MiB` across 10 tenants; `40 MiB` across 20 tenants) and never shrinks on flush or suspend.
  - `Timeline.relSizes` and `Timeline.relSizeKnown`: Two separate maps tracking `Key -> uint32` and `Key -> bool` (~1,200 entries per tenant).

### 2.6 Flush & Compaction Temporary Memory
- **Code Inspection ([`internal/pageserver/layers.go:127-143`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/layers.go#L127-L143), [`layer_v2.go:179-410`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/layer_v2.go#L179-L410)):**
  - `FlushInMemory` calls `t.inMem.SnapshotAll()`, which allocates a flat `[]LayerRecord` slice of all entries (`~60,000` records during bootstrap = `~4.8 MB` slice header) while keeping `oldMem` in `t.frozenMems` until the entire file is encoded, compressed, encrypted, `fsync`ed, and the manifest is published.
  - `EncodeLayerV2ToWriter` ([`layer_v2.go:414`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/layer_v2.go#L414)) re-sorts `recs` even though `SnapshotAll()` already sorted them.
  - In `v2StreamWriter.flushChunk()` ([`layer_v2.go:223`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/layer_v2.go#L223)) and `compressV2Payload()` ([`layer_v2.go:62`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/layer_v2.go#L62)), every 64 KiB chunk allocates a fresh `rawChunk := make([]byte, totalRaw)` and a fresh `make([]byte, 0, len(raw))` for zstd output. Across 10 tenants, `v2StreamWriter.flushChunk` + `compressV2Payload` allocated **`488.38 MB` (`34.23%` of all allocations)**!
  - `getV2Zstd()` ([`layer_v2.go:43-52`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/layer_v2.go#L43-L52)) and `zstdDecOnce` ([`fpi_decompress.go:11`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/fpi_decompress.go#L11)) allocate two separate global `zstd.Decoder`s and a concurrent `zstd.Encoder` (`~3.87 MB` permanent heap). Worse, `decodeV2WALTable` ([`layer_v2.go:536`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/layer_v2.go#L536)) calls `zstd.NewReader(...)` per call (which allocates several MiB of decoder state and spawns background goroutines) instead of using `v2ZstdDec.DecodeAll`!
  - `PackMetadataSkeleton` / `UnpackMetadataSkeleton` ([`skeleton.go:99,197`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/skeleton.go#L99)) also call `zstd.NewWriter` / `zstd.NewReader` with default concurrency on every suspend/wake, allocating multi-megabyte encoder/decoder windows per call.
  - There is **no global semaphore bounding concurrent flushes and compactions** across tenants: if 8 tenants hit `MaybeFlushAsync()` simultaneously, 8 concurrent `FlushInMemory` goroutines allocate 8 `SnapshotAll` slices and 8 V2 encoding streams at once.

### 2.7 Goroutine & Connection-Related Memory
- **Measured Values:**
  - `air-pageserver` runs with **7–10 goroutines** (and **9 OS threads**) during 4–10 tenant workloads: 1 HTTP server, 1 gRPC server listener, 2–4 gRPC transport handlers (`air-server`, `air-gateway`, `air-read-engine`), and background flush workers.
  - `StackInuse` is **`0.50–0.75 MB`**. Goroutine stack memory is **not** the source of the OOM, though gRPC's `sizedBufferPool` retains ~`15.9 MB` when large multi-megabyte `PutPageBatch` / `IngestWALRecords` messages are sent concurrently.

### 2.8 Go Heap, Live Heap, Heap Idle, Heap Released, and GC Overhead
- **Forensics Table (10 Tenants In-Process Forensics Harness):**

| Stage | Active Tenants | `HeapAlloc` (Live) | `HeapInuse` | `HeapIdle` | `HeapReleased` | Unreleased Idle (`HeapIdle - HeapRel`) | `GCSys` | `/proc` `VmRSS` | `/proc` `RssAnon` | `/proc` `RssFile` |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `00_init` | 0 | 0.65 MB | 1.46 MB | 6.16 MB | 2.69 MB | 3.47 MB | 2.67 MB | 21.54 MB | 6.28 MB | 15.26 MB |
| `01_boot_t01_post_gc` | 1 | 12.28 MB | 13.48 MB | 22.12 MB | 5.38 MB | 16.74 MB | 3.01 MB | 47.86 MB | 30.16 MB | 17.70 MB |
| `01_boot_t04_post_gc` | 4 | 36.44 MB | 38.14 MB | 37.45 MB | 4.43 MB | 33.02 MB | 3.47 MB | 89.23 MB | 71.48 MB | 17.75 MB |
| `01_boot_t08_post_gc` | 8 | 68.63 MB | 70.71 MB | 52.88 MB | 3.25 MB | 49.63 MB | 3.71 MB | 138.54 MB | 120.79 MB | 17.75 MB |
| `01_boot_t10_post_gc` | 10 | 84.73 MB | 86.95 MB | 64.65 MB | 2.26 MB | 62.39 MB | 3.78 MB | 163.26 MB | 145.51 MB | 17.75 MB |
| `01_conc_boot_t10_no_gc` | 10 (conc) | 135.45 MB | 138.12 MB | 65.19 MB | 6.01 MB | 59.18 MB | 4.03 MB | 217.05 MB | 199.85 MB | 17.20 MB |
| `02_sustained_writes_no_gc` | 10 | 273.15 MB | 274.63 MB | 4.99 MB | 4.80 MB | 0.19 MB | 4.49 MB | **301.13 MB** | **283.38 MB** | 17.75 MB |
| `02_sustained_writes_post_gc` | 10 | 240.83 MB | 243.75 MB | 35.84 MB | 1.53 MB | 34.31 MB | 4.65 MB | **302.69 MB** | **284.94 MB** | 17.75 MB |
| `04_after_flush_post_gc` | 10 | 154.71 MB | 157.80 MB | **321.83 MB** | **7.51 MB** | **314.32 MB** | 4.91 MB | **408.87 MB** | **391.12 MB** | 17.75 MB |

- **Critical Finding on Go Heap vs. Process RSS vs. Cgroup Memory:**
  - Notice `04_after_flush_post_gc`: even after `FlushTenant` reduced `HeapAlloc` to `154.71 MB`, **`HeapIdle` jumped to `321.83 MB` while `HeapReleased` was only `7.51 MB`**, pushing `/proc` `RssAnon` (and cgroup `anon`) to **`391.12 MB`**!
  - Because `air-pageserver` runs without `GOMEMLIMIT` (or explicit `debug.SetMemoryLimit` / `debug.FreeOSMemory` under memory pressure), the Go runtime retains hundreds of megabytes of freed heap spans in `HeapIdle` as mapped anonymous pages (`RssAnon`), which Linux cgroup v2 counts against `memory.current` until the container is OOM-killed.

---

## 3. Phase 2: Live 256 MB Docker Cgroup v2 OOM Reproduction

We reproduced the exact Phase 21.10 failure on the live 5-container Docker stack (`air-p21-pageserver` constrained to `--memory=256m --memory-swap=256m`, `air-p21-server` at `512m`, `air-p21-gateway` at `256m`, `air-p21-read-engine` at `256m`):

| Stage | Container State | Cgroup `memory.current` | Cgroup `memory.peak` | Cgroup `anon` | Cgroup `file` | Cgroup `oom_kill` | `/proc` `VmRSS` | `/proc` `RssAnon` | `/proc` `RssFile` | `/proc` `VmHWM` |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `00_initial` (0 tenants) | Running | 3.49 MB | 6.86 MB | 2.38 MB | 0.02 MB | 0 | 15.50 MB | 2.12 MB | 13.37 MB | 15.50 MB |
| `01_prior_t0_loaded` (1 tenant) | Running | 58.69 MB | 64.40 MB | 54.13 MB | 2.99 MB | 0 | 68.05 MB | 53.88 MB | 14.18 MB | 74.18 MB |
| `01_prior_t1_loaded` (2 tenants) | Running | 110.89 MB | 114.31 MB | 103.05 MB | 5.95 MB | 0 | 116.97 MB | 102.80 MB | 14.18 MB | 117.93 MB |
| `01_prior_t2_loaded` (3 tenants) | Running | 177.21 MB | 181.15 MB | 166.47 MB | 8.91 MB | 0 | 180.52 MB | 166.22 MB | 14.30 MB | 180.52 MB |
| `01_prior_t3_loaded` (4 tenants) | Running | 214.62 MB | 218.04 MB | 200.68 MB | 11.62 MB | 0 | 214.73 MB | 200.43 MB | 14.30 MB | 214.73 MB |
| `02_active_t0_provision` (5th tenant bootstrap + active writes) | **OOMKilled (`ExitCode=137`)** | **256.00 MB (limit hit)** | **256.00 MB** | **~243 MB** | **~13 MB** | **1** | — | — | — | — |

### Why Each Active Tenant Consumed ~51 MB on `air-pageserver`:
1. Each tenant provisioning + 3,000-row table load generated:
   - `22.6 MB` of `initdb` pages pushed via `PutPageBatch` (flushed to L0, leaving ~10–15 MB in `HeapIdle` + index cache).
   - `14.6 MB` of `CREATE DATABASE` (`template1` copy) pages + WAL FPIs retained in `InMemoryLayer`.
   - `~12 MB` of `mto_data` table/index writes + WAL FPIs retained in `InMemoryLayer` (or triggered a flush that left another ~15 MB in `InMemoryLayer` + `HeapIdle`).
   - `~3 MB` of cgroup `file` page cache from written L0 layer files.
2. Thus, 4 active tenants consumed `200.68 MB` of `anon` + `11.62 MB` of `file` (`214.62 MB` `memory.current`).
3. When the 5th tenant (`repro_active_t0`) began `BootstrapTenantFromPGData` + `CREATE DATABASE` while the 4 prior tenants continued writing, `memory.current` crossed `256.00 MB` and the Linux kernel OOM killer terminated `air-pageserver` with `SIGKILL` (`ExitCode=137`, `OOMKilled=true`).

---

## 4. Phase 3 Design: Targeted Memory Management Architecture

To make `air-pageserver` genuinely lightweight, predictable, and safe for **8 to 20+ concurrent active tenants within a 256 MB container** without weakening `fsync`, durability, WAL ordering, or crash recovery guarantees:

### 4.1 In-Place `InMemoryLayer` Page Coalescing & FPI Deduplication
1. **Deduplicate `WALRecord` when `EntryFlagPageImage` is already materialized:**
   - In [`Timeline.ApplyWALRecord`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/readpath.go#L370-L391), when a block delta has a full page image (`(delta.Flags & EntryFlagPageImage) != 0 && len(delta.PageImage) == PageSize`), clear `delta.Flags &^= EntryFlagWALRecord` and `delta.WALRecord = nil`. Because `prepareRedo` always resets to the latest `PageImage` and discards any WAL records at or before that LSN, storing the raw 8 KiB FPI WAL record alongside the 8 KiB `PageImage` is 100% redundant.
2. **Coalesce superseded `InMemoryLayer` deltas at the same or older LSN:**
   - In [`InMemoryLayer.Put`](file:///home/antigravity/Mahadi-rsio/air/internal/pageserver/layers.go#L96-L107):
     - Track accurate `ApproxBytes` (including 128B per-entry overhead).
     - When appending an `EntryFlagPageImage` (or `EntryFlagRelSize`) delta for `key` at `delta.LSN >= last.LSN`: if the existing slice for `key` already ends with an entry, any prior deltas in the *unflushed* `InMemoryLayer` that are overwritten at the same LSN—or prior deltas for `key` when no historical snapshot pin exists—can be compacted!
     - Wait: can historical LSN reads (`GetPageAtLSN(key, lsn)`) request an older LSN within the active `InMemoryLayer`?
     - Let's check how `GetPageAtLSN` / `GetPageBatch` are called across the entire codebase:
       - `air_smgr` calls `GetPageAtLSN`/`GetPageBatch` with `lsn = 0` (latest) or `GetXLogInsertRecPtr()` (current latest LSN).
       - `air-read-engine` calls `GetPageAtLSN`/`GetPageBatch` at its snapshot `targetLSN`, which is either `0` (latest) or the `WALLastLSN()` / `LastLSN()` obtained at the start of a read query!
       - Even if a reader holds a snapshot LSN from 10ms ago, if `delta.LSN == prev.LSN` (which happens every time `PutPageBatch` and `IngestWALRecords` both write the same FPI at `rec.EndLSN`, or when `PutPageBatch` overwrites the same block within the same LSN!), replacing `prev` in-place at `prev.LSN == delta.LSN` is 100% snapshot-identical for every LSN!
       - What about when `delta` is a full `EntryFlagPageImage` at `delta.LSN > prev.LSN`? If we flush `InMemoryLayer` promptly (e.g., `2 MiB` per-tenant soft budget), or coalesce `EntryFlagRelSize` entries, wait: what if `InMemoryLayer` coalesces prior deltas for the same `key` when `(delta.Flags & EntryFlagPageImage) != 0`?
       - Wait: could any test in `internal/pageserver` write two different page images to the same key at `LSN=10` and `LSN=20` *without flushing* and then call `GetPageAtLSN(key, 10)`?
       - Let's check existing unit tests in `internal/pageserver` before touching historical LSN retention in `InMemoryLayer`!
       - Even better: if we check whether any test queries a historical LSN inside `InMemoryLayer`, OR if we keep at most the last 2 versions in `InMemoryLayer` (or flush to L0 at `2 MiB`), once flushed to L0 on disk, **all** LSN versions live on disk in the L0 layer and take **0 bytes** of Go heap!

### 4.2 Eliminate Redundant `initdb` Template Databases & Flush After `CREATE DATABASE`
1. **Skip unused `template0` (`base/4`) during `BootstrapTenantFromPGData` or stream-flush immediately:**
   - During `BootstrapTenantFromPGData`, `initdb` has 3 databases in `base/`: `base/1` (`template1`), `base/4` (`template0`), and `base/5` (`postgres`).
   - More importantly, in [`GRPCPageserverClient.BootstrapTenantFromPGData`](file:///home/antigravity/Mahadi-rsio/air/internal/control/pageserver_client.go#L434-L484), why did `PutPageBatch` accumulate 16+ MB in `InMemoryLayer`? Because `PutPageBatch` waited until `16 MiB` before starting an async flush!
   - If `GRPCPageserverClient.BootstrapTenantFromPGData` flushes every `2 MiB` (or per-tenant `maxInMemBytes` is `2 MiB`), `InMemoryLayer` during bootstrap never exceeds `2–4 MiB`!
   - And in [`ControlPlane.ProvisionDatabase`](file:///home/antigravity/Mahadi-rsio/air/internal/control/control.go#L643-L678):
     - Look at the order in `ProvisionDatabase`:
       - First it ran `ps.BootstrapTenantFromPGData` on `claiming/pgdata` (before `ActivateCluster` created the tenant database!).
       - Then `ActivateCluster` started PG17 and ran `CREATE DATABASE <tenantID>`, which copied `template1` (`7.3 MB`) through `air_smgr` into `InMemoryLayer` and left `14.6 MB` dirty in `InMemoryLayer`!
       - If `ProvisionDatabase` (or `ensurePageserverBootstrapAndWAL`) calls `ps.FlushTenant(ctx, tenantID)` right after `ActivateCluster` + initial bootstrap completes, then **100% of the `initdb` + `CREATE DATABASE` catalog pages are durably persisted to an immutable L0 layer on disk and evicted from `InMemoryLayer` immediately upon tenant creation**, dropping idle post-bootstrap `InMemoryLayer` memory from **`14.8 MB` per tenant to `0 B` per tenant**!

### 4.3 Global & Per-Tenant Memory Budgets (`MemoryBudget`)
We will implement a first-class, measurable, enforceable `MemoryBudget` manager in `internal/pageserver/memory_budget.go`:
1. **Auto-Scaled Budgets Based on Container / Configured Memory Limit (`MaxMemoryBytes`):**
   - Detect container cgroup v2 `memory.max` (or `AIR_PAGESERVER_MEMORY_MB` / `Config.MemoryLimitBytes`, defaulting to `256 MiB` when cgroup limit is 256 MiB):
   - For a **256 MiB** Pageserver (`MemoryLimitBytes = 256 MiB`):
     - **Go runtime soft memory limit (`debug.SetMemoryLimit`):** Set to `180 MiB` (70% of 256 MiB) so the Go GC and scavenger aggressively release `HeapIdle` spans back to the OS (`madvise(MADV_DONTNEED)`) instead of letting `HeapIdle` balloon to 300+ MiB.
     - **Global `InMemoryLayer` Budget (`GlobalMaxInMemBytes`):**
       - Soft flush watermark: **`24 MiB`** (`GlobalSoftInMemBytes`)
       - Hard backpressure limit: **`48 MiB`** (`GlobalHardInMemBytes`)
     - **Per-Tenant `InMemoryLayer` Budget (`PerTenantMaxInMemBytes`):**
       - Soft flush threshold: **`2 MiB`** (down from `16 MiB`; dynamically scaled to `min(PerTenantSoftMax, GlobalSoftInMemBytes / max(ActiveWriteTenants, 1))`, with a floor of `512 KiB`).
       - Hard backpressure threshold: **`4 MiB`** (blocks `PutPageBatch` / `IngestWALRecords` until in-flight L0 flush completes).
     - **`DecodedChunkCache` Budget:** **`16 MiB`** (down from `64 MiB` on a 256 MiB Pageserver; `32 MiB` on a 512 MiB Pageserver).
     - **`boundedIndexCache` Budget:** **`16 MiB`** (down from `64 MiB` on a 256 MiB Pageserver; `32 MiB` on a 512 MiB Pageserver), and **include `walTable` bytes** in `idxData.bytes` accounting!
     - **`DiskCache` Budget:** Disabled (`nil`) for local layers that already exist in `LayerDir` (`s.localDir != ""`), or bounded to `32 MiB` when caching remote R2 chunks, avoiding redundant double-writing to disk and double page-cache pollution.
     - **Bounded Background Task Concurrency:**
       - Global flush concurrency semaphore (`maxConcurrentFlushes = 2`): prevents 10 tenants from running 10 concurrent `SnapshotAll` + V2 encode streams simultaneously.
       - Global compaction concurrency semaphore (`maxConcurrentCompactions = 1`): serializes background L0 -> L1 compaction so compaction never spikes memory during multi-tenant write bursts.
       - Automatic background L0 compaction when a tenant accumulates `>= 8` L0 layers, keeping per-tenant layer count and index metadata small.

### 4.4 Memory-Pressure-Aware Background Flusher & Safe Dirty-Page Eviction
1. **Background Pressure Monitor & Idle Flusher (`memoryMonitorLoop`):**
   - Runs every `250ms` in `Pageserver`:
   - Computes total `GlobalInMemBytes` across all timelines (`inMem` + `frozenMems`), plus `runtime.ReadMemStats` (`HeapAlloc`, `HeapInuse`, `HeapIdle - HeapReleased`) and cgroup v2 `memory.current`.
   - **Proactive Idle Flush:** Any timeline with `inMem.ApproxBytes > 0` that has been idle for `>= 1s` (or has `inMem.ApproxBytes >= tenantSoftLimit`) is flushed to a durable L0 layer and its `inMem` map is reset to empty (`0` dirty pages).
   - **Global Pressure Eviction:** When `GlobalInMemBytes >= GlobalSoftInMemBytes` (or cgroup/heap utilization exceeds 65% of limit):
     - Ranks timelines by `InMemBytes()` descending and flushes the largest dirty timelines to durable L0 layers (`WriteLayer` with `fsync` + `PublishLayerManifestGen` with `fsync`).
     - Once a timeline's `InMemoryLayer` is durably persisted to L0 and its manifest is `fsync`ed, `oldMem` is removed from `frozenMems`, `t.slruPages` is trimmed (since SLRU pages are persisted in the L0 layer and reconstructed via `GetPageAtLSN`), and `t.modLog` is capped to `2,048` entries (`64 KiB` max per tenant instead of `2 MiB`).
     - Under high memory pressure (>75% of budget), trims `DecodedChunkCache` and `boundedIndexCache` by 50% and invokes `debug.FreeOSMemory()` to return `HeapIdle` pages to the OS immediately.
   - **Never discards unflushed dirty pages:** Eviction from `InMemoryLayer` occurs **strictly after** `WriteLayer` (`outFile.Sync()`) and `PublishLayerManifestGen` (`f.Sync()`) succeed.

### 4.5 Tenant Admission Backpressure & Bounded WAL Ingestion
1. **Admission Backpressure (`AcquireWriteBudget`):**
   - Before `PutPageBatch`, `IngestWALRecords`, or `BootstrapTenantFromPGData` accepts new page allocations:
   - If the tenant's `InMemBytes() >= PerTenantHardInMemBytes` OR `GlobalInMemBytes >= GlobalHardInMemBytes`:
     - Triggers immediate synchronous/waited L0 flush of the largest dirty timeline(s) and blocks the caller (respecting `ctx.Done()`) until memory drops below the hard watermark.
     - If `ctx` expires while memory remains completely exhausted, returns `codes.ResourceExhausted` (which `GRPCPageserverClient` retries with backoff), applying clean end-to-end backpressure to writers without OOM-killing `air-pageserver`.
2. **Allocation & Page-Copy Reduction:**
   - Fix `WALStreamParser.Push` (`walparser.go`) to advance `p.pos` / compact `p.buf` in-place (`copy(p.buf, p.buf[consumed:])`) instead of reallocating `p.buf` on every 8 KiB WAL page.
   - Pool 64 KiB chunk buffers and zstd compression buffers in `v2StreamWriter` (`sync.Pool`), and skip redundant `sort.Slice` when `SnapshotAll` already sorted `recs`.
   - Reuse the singleton `zstd.Decoder` (`v2ZstdDec.DecodeAll`) in `decodeV2WALTable` and `UnpackMetadataSkeleton`, and use `zstd.WithEncoderConcurrency(1)` + `zstd.WithWindowSize(64*1024)` for `PackMetadataSkeleton` so zstd never allocates multi-megabyte concurrent worker pools.
   - Bound `maxGRPCMessageBytes` and batch `SeedSLRUPages` / `IngestWALRecords` to bounded chunk sizes (`<= 4 MiB`).
