# Phase 23 — Adaptive CA-WAL Architecture & Design

## 1. Design Goals

Based on the Phase 23 forensics findings ([phase23-cawal-performance-forensics.md](file:///workspaces/air/docs/reports/phase23-cawal-performance-forensics.md)), the adaptive CA-WAL design targets:

1. **Adaptive Inline vs. External Storage:** Keep small WAL tables inline in V2 `.layer` files; route larger WAL tables to `CAWALStore` only when external storage is net-beneficial.
2. **Compression-Aware Single-Pass Encoding:** Compress at most once, never recompress already-compressed data, and skip compression entirely on CA-WAL deduplication hits.
3. **Lock-Free / Singleflight Concurrent Blob Writes:** Eliminate 50-way `os.Link` races and duplicate `fsync`s via per-hash singleflight coordination, sharded pin tracking, and a bounded verified-blob cache.
4. **Bounded CA-WAL I/O Scheduling:** Bound concurrent disk blob writes using a native Go channel semaphore with `context.Context` cancellation and backpressure.
5. **Fast-Path Tenant Lookup:** Eliminate the `98.44%` `p.mu.Lock()` contention in `Pageserver.SetTenantGeneration` and `Pageserver.EnsureTenant` via `p.mu.RLock()` fast paths.
6. **Zero Format Breakage:** Preserve `LayerCodecNone` (`0`), `LayerCodecZstd` (`1`), `LayerCodecCAWAL` (`2`), `CAWB` v1 blob format, and `EnableCAWAL = false` default.

---

## 2. Content Identity Definition

**Content identity is strictly defined as `SHA256(uncompressed_logical_wal_payload)`** (`walRaw`), independent of transport/storage compression (`LayerCodecNone` vs `LayerCodecZstd`).

- `CAWB` v1 header bytes `[16:48]` store `SHA256(raw)` and bytes `[8:12]` store `rawLen`.
- `decodeAndVerifyCAWALBlob` decompresses the payload (if `codec == LayerCodecZstd`) and verifies `sha256.Sum256(raw) == expectedSum`.
- Compressed and uncompressed representations are never treated as interchangeable hashes; compression codec is explicit in header byte `[6]`.

---

## 3. Adaptive Inline vs. External Storage

In `v2StreamWriter.Finish()` (`internal/pageserver/layer_v2.go`):

1. Serialize `walRaw` from `sw.walTable`.
2. Let `threshold = sw.opts.CAWALInlineThreshold` (default `16 * 1024` bytes, benchmarked across `4 KB`, `8 KB`, `16 KB`, `32 KB`, `64 KB`).
3. **Inline Fast Path:** If `sw.opts.CAWAL == nil || sw.encrypted || len(walRaw) < threshold`:
   - Encode inline using `compressV2Payload(walRaw, sw.opts.DisableCompression)` (`LayerCodecNone` or `LayerCodecZstd`).
   - Record `InlineSelections` observability counter on `CAWALStore` (if attached).
4. **External CA-WAL Path (`len(walRaw) >= threshold`):**
   - Call `sw.opts.CAWAL.PutBlobIfAbsent(cctx, walRaw)` (or `PutBlobWithHashIfAbsent` if pre-hashed).
   - Pin the returned hash (`Pin`), write the 32-byte raw SHA-256 digest into `.layer` with `LayerCodecCAWAL`, and record `ExternalSelections`.

Because `LayerMeta` and `v2WALTableMeta.Codec` already distinguish `LayerCodecNone`, `LayerCodecZstd`, and `LayerCodecCAWAL` per layer, inline and external layers coexist seamlessly within the same timeline, across compactions, and across restarts/rollbacks.

---

## 4. Concurrent Blob Write Optimization (`CAWALStore`)

### 4.1 Per-Hash Singleflight (`blobFlight`)
- `CAWALStore` maintains `flightMu sync.Mutex` and `flights map[string]*cawalFlight` (keyed by `hashHex`, held only for map lookup/insert, never across I/O).
- Concurrent callers for the **same** `hashHex` join the active `*cawalFlight` and wait on `<-fl.done` or `<-ctx.Done()`.
- Only the leader performs disk existence checks, compression, temp-file write, `f.Sync()`, `os.Link`, and `syncDirBestEffort`.
- Waiters receive the leader's result with `Deduplicated: true` (incrementing `DedupHits`) without touching disk or compressing.
- Concurrent callers for **different** `hashHex` values never block each other.

### 4.2 Bounded Verified-Blob Cache (`verifiedCache`)
- A mutex-protected map (capped at `4096` entries) caches `{RawLen, StoredLen, Codec}` for blobs already verified on disk.
- On `PutBlobIfAbsent`: after computing `sha256.Sum256(raw)`, if `verifiedCache` has `hashHex` and `os.Stat(finalPath)` confirms the file exists with matching `StoredLen`, `PutBlobIfAbsent` returns a dedup hit immediately — skipping `encodeCAWALBlob` (Zstd), `os.Open`, `ReadAt`, and `Close`.
- Invalidated on `GC` deletion or any integrity mismatch.

### 4.3 Sharded Pin Table
- `pins` is sharded across `16` `pinShard` buckets (`pinShardIdx(hashHex)`), eliminating global `pinMu` contention during concurrent `Pin`/`Unpin`/`isPinned` calls.

---

## 5. Bounded CA-WAL I/O Scheduler

- `CAWALStore` uses a native Go channel semaphore `ioSem chan struct{}` (default capacity `max(4, runtime.GOMAXPROCS(0)*2)`, configurable via `CAWALOptions.MaxConcurrentIO`).
- Acquired via `acquireIOSlot(ctx)` **only when a disk write is actually needed** (after `verifiedCache` and singleflight deduplication).
- Context cancellation unblocks waiting callers cleanly; no background goroutines are spawned and no WAL operations are silently dropped.

---

## 6. Manifest & Observability Enhancements

- `PublishManifest` uses compact `json.Marshal` instead of `json.MarshalIndent` and deduplicates `HasBlob` `os.Stat` calls per unique `BlobHash` within the manifest.
- `CAWALMetrics` adds:
  - `InlineSelections`, `ExternalSelections`
  - `SingleflightHits`, `CacheHits`
  - `BlobWriteNanos`, `BlobWriteCount`
  - `ManifestWrites`, `ManifestWriteNanos`
  - `IOSchedulerWaits`
