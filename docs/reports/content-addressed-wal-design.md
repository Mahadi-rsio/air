# AIR Content-Addressed WAL (CA-WAL) — Phase 2 Design Document

**Date:** 2026-11-24  
**Repository:** `github.com/Mahadi-rsio/air`  
**Status:** Design Specification for Implementation  

---

## 1. Design Goals & Constraints

1. **Preserve PostgreSQL WAL semantics**: Never conflate physical content identity (`SHA-256`) with logical WAL position (`TimelineID`, `Epoch`, `StartLSN`, `EndLSN`).
2. **Atomic, crash-safe persistence**: Partial or failed blob writes must never become visible; manifests must never reference uncommitted blobs.
3. **Deduplication without external services**: Use native Go primitives and local/R2 `LayerStore` storage—no Redis, Kafka, NATS, or external DB.
4. **Low-memory Fly.io safety (256 MB / 512 MB)**: Bounded chunk sizes, pooled compression/scratch buffers, singleton zstd encoder/decoder, and memory-pressure-aware GC.
5. **100% Backward Compatibility & Controlled Rollout**: Feature-flagged (`EnableCAWAL`), able to read both legacy inline V1/V2 layers and CA-WAL layers, with zero-downtime rollback.

---

## 2. Content-Addressed Blob Architecture (`CAWALStore`)

### 2.1 Hashing & Blob Identity
- Canonical blob identity is the lowercase 64-hex SHA-256 of the **uncompressed** WAL chunk payload:
  $$\text{BlobHash} = \text{hex}(\text{SHA256}(\text{wal\_chunk\_bytes}))$$
- The hash is computed **once** upon chunk sealing and reused for:
  - Content-addressed storage path lookup (`_wal_cas/sha256/<hh>/<full_sha256>.walb`),
  - Deduplication check (`os.Stat` / in-memory index),
  - Blob header integrity field,
  - Read-path integrity verification (`SHA256(decompressed_bytes) == expectedHash`).

### 2.2 Bounded WAL Chunk & Blob Binary Format
- **Chunk Sizing**: Bounded to `64 KiB` raw target (`v2TargetChunkRawBytes = 64 * 1024`, hard max `16 MiB` per blob `maxCAWALBlobRawBytes`) to align with existing V2 layer chunking (`layer_v2.go`) and keep per-operation buffers small on 256 MB machines.
- **Blob File Layout (`.walb`)**:
  | Offset | Size | Field | Description |
  |---|---|---|---|
  | `0` | 4 B | `Magic` | `"CAWB"` (`0x42574143`) |
  | `4` | 2 B | `Version` | `1` (`CAWALBlobVersion1`) |
  | `6` | 1 B | `Codec` | `0 = None`, `1 = Zstd` |
  | `7` | 1 B | `Flags` | Reserved (`0` for plaintext; bit 0 for future per-tenant envelope) |
  | `8` | 4 B | `RawLen` | Uncompressed payload length (uint32 LE) |
  | `12` | 4 B | `PayloadLen` | Stored payload length (uint32 LE) |
  | `16` | 32 B | `ContentSHA256` | Raw 32-byte SHA-256 of uncompressed payload |
  | `48` | `PayloadLen` B | `Payload` | Raw or Zstd-compressed WAL chunk bytes |

### 2.3 Atomic Create-If-Absent (`PutBlobIfAbsent`)
1. Validate `len(raw) > 0 && len(raw) <= maxCAWALBlobRawBytes`.
2. Compute `sum := sha256.Sum256(raw)` once; format `hashHex`.
3. **Pin** `hashHex` in `CAWALStore.pins` (prevents concurrent GC from deleting the blob before its manifest commits).
4. **Fast-path dedup check**: If `finalPath` exists and header + size match `sum` and `len(raw)` (and optional verify-on-reuse passes), return `PutResult{Hash: hashHex, Deduplicated: true}` without compressing or writing.
   - If an existing file at `finalPath` has a corrupted header or mismatched `ContentSHA256` / `RawLen`, return `ErrCAWALIntegrityMismatch` (never silently reuse corrupt data).
5. **Compress** into pooled buffer (`sync.Pool`) using singleton `v2ZstdEnc` (`WithEncoderConcurrency(1)`, `WithWindowSize(64 KiB)`). Fall back to `CodecNone` if `len(comp) >= len(raw)`.
6. **Write-temp + `fsync` + Atomic `os.Link`**:
   - Write 48-byte header + payload to unique temp file `<finalPath>.tmp.<seq>`.
   - `f.Sync()`, `f.Close()`.
   - Call `os.Link(tmpPath, finalPath)`:
     - On success: `os.Remove(tmpPath)`, sync parent dir, record blob write metric, return `Deduplicated: false`.
     - On `os.ErrExist`: another writer concurrently committed the identical blob! Remove `tmpPath`, verify the existing blob at `finalPath` matches `sum` and `len(raw)`, and return `Deduplicated: true`.
     - On any error: remove `tmpPath` and unpin.

### 2.4 Integrity-Verified Reads (`GetBlobVerified`)
1. Validate `hashHex` (64-hex).
2. Read blob file, validate 48-byte header (`Magic == "CAWB"`, `Version == 1`, `RawLen <= maxCAWALBlobRawBytes`, `len(file) == 48 + PayloadLen`, `header.ContentSHA256 == expectedSum`).
3. Decompress using singleton `v2ZstdDec` (`WithDecoderConcurrency(1)`).
4. Verify `len(raw) == RawLen` and `sha256.Sum256(raw) == expectedSum`.
5. On any mismatch, increment `HashFailures` metric and return `ErrCAWALIntegrityMismatch`.

---

## 3. Logical Reference Mapping (`CAWALRef` & `CAWALManifest`)

Logical WAL ordering and physical blob storage are strictly decoupled:

```go
type CAWALRef struct {
    TenantID    string `json:"tenant_id"`
    Generation  string `json:"generation,omitempty"`
    TimelineID  string `json:"timeline_id"`
    Epoch       int64  `json:"epoch"`
    Seq         uint64 `json:"seq"`
    StartLSN    uint64 `json:"start_lsn"`
    EndLSN      uint64 `json:"end_lsn"`
    BlobHash    string `json:"blob_hash"`
    RawLen      uint32 `json:"raw_len"`
    RecordCount uint32 `json:"record_count"`
}
```

- **Durability Ordering**: A `CAWALManifest` (or `TimelineLayerManifest` / V2 layer referencing a CA-WAL blob via `LayerCodecCAWAL = 2`) is **only** atomically written (`tmp` -> `fsync` -> `rename`) **after** `PutBlobIfAbsent` has durably `fsync`ed and linked the referenced blob(s).
- **Independent LSN & Timeline Semantics**: Two references from different timelines, epochs, or LSN ranges (`[1000..2000)` and `[5000..6000)`) may point to the same `BlobHash`, while preserving their own `TimelineID`, `Epoch`, `StartLSN`, `EndLSN`, and `Seq`.

---

## 4. Layer Format V2 Integration (`LayerCodecCAWAL`)

- `layer_v2.go` adds `LayerCodecCAWAL uint8 = 2` for `v2WALTableMeta.Codec`.
- When `EnableCAWAL` is enabled on `LayerStore` (and `kms == nil`, or tenant-scoped CA-WAL):
  - `v2StreamWriter.Finish()` serializes `walTable` into `walRaw`, calls `CAWALStore.PutBlobIfAbsent(ctx, walRaw)`, and stores only the **32-byte `BlobHash`** in the `.layer` file's `walTable` slot (`RawLen = uint32(len(walRaw))`, `CipherLen = 32`, `Codec = LayerCodecCAWAL`).
  - `Timeline.FlushInMemory` / `CompactL0s` **unpins** the blob hash only *after* `PublishLayerManifestGen` succeeds!
  - `decodeV2WALTable` handles `LayerCodecCAWAL` by reading the 32-byte hash, calling `CAWALStore.GetBlobVerified(ctx, hashHex)`, and decoding the `walTable` entries using zero-copy arena slicing.
- **Backward Compatibility**: Existing `.layer` files with `Codec = 0` (`None`) or `Codec = 1` (`Zstd`) continue to decode identically. Disabling `EnableCAWAL` stops writing `LayerCodecCAWAL` for new layers while keeping `CAWALStore` readable for existing layers.

---

## 5. Reference Tracking & Crash-Safe Garbage Collection

We use **in-memory active write pins + durable manifest reachability mark-and-sweep** (no distributed refcount service, no drift on crash):
1. **Pinning**: Writers call `Pin(hash)` before `PutBlobIfAbsent` and `Unpin(hash)` after durable manifest commit (or failure cleanup).
2. **Mark**: `CAWALStore.GC(ctx, minAge)` scans:
   - All `timeline_*_layer_manifest.json` files (and V2 layer `walMeta` hashes if `LayerCodecCAWAL`),
   - All `cawal_manifest_*.json` files,
   - All currently `pinned` hashes in memory.
3. **Sweep**: Any `.walb` blob not in `reachable` whose `ModTime` is older than `minAge` (grace period) is re-checked against `pins` under lock and unlinked. Stale `.tmp.*` files older than `minAge` are also unlinked.
4. **Memory-Pressure Guard**: If `MemoryBudget` reports high pressure or `ctx` is cancelled, GC aborts early without impacting write latency.

---

## 6. Phase 3 WAL Memory Optimizations

1. **`DecodeWALRecord` zero-copy sub-slicing (`walparser.go`)**: Slice `ImagePayload`, `BlockData`, and `MainData` directly from `rec.RawRecord` instead of allocating and copying each field twice.
2. **`decodeV2WALTable` singleton decoder (`layer_v2.go`)**: Replace per-call `zstd.NewReader` with `decompressV2Payload` (reusing `v2ZstdDec`).
3. **`lookupKeysInLayerV2` zero-clone `walTable` (`layer_v2.go`)**: Return `walTable[idx]` directly (immutable slice) instead of `append([]byte(nil), walTable[idx]...)`.
4. **Idempotent LSN filtering (`grpc.go`)**: In `Pageserver.IngestWALRecords`, skip records with `rec.EndLSN <= tl.walLastLSN` (when `tl.walLastLSN > 0 && rec.EndLSN > 0`) to prevent duplicate ingestion on 8 KiB page-aligned `START_REPLICATION` reconnects.
5. **Accurate multi-block `ApproxBytes` (`layers.go`)**: Deduplicate `WALRecord` byte accounting when the same `RawRecord` slice is attached to multiple blocks in a single `ApplyWALRecord` call.
