AIR Pageserver + CAU Performance Engineering Program

Role

Act as a Principal Storage Systems Engineer, PostgreSQL Storage Engine Engineer, and Distributed Systems Performance Engineer.

Repository: https://github.com/Mahadi-rsio/air

Your task is to improve AIR's Content-Addressed Upload (CAU), Pageserver read/write paths, immutable layer format, caching, and storage efficiency.

Primary Objectives

1. Reduce PostgreSQL page read latency.
2. Reduce R2 request count and network transfer amplification.
3. Reduce immutable layer storage size.
4. Reduce Pageserver RAM consumption.
5. Improve sequential and random read throughput.
6. Improve WAL replay efficiency.
7. Improve CAU upload throughput.
8. Preserve data integrity, crash recovery, and tenant isolation.

Do not prioritize production deployment or business features. Focus on engineering research, architecture, implementation, and reproducible benchmarks.

Phase 1 — Independent Engineering Audit

Inspect the actual implementation of:

- internal/pageserver/cau.go
- internal/pageserver/layers.go
- internal/pageserver/readpath.go
- internal/pageserver/cache.go
- internal/pageserver/compactor.go
- internal/pageserver/walparser.go
- internal/pageserver/walredo.go
- internal/pageserver/server.go
- internal/pageserver/protocol.go
- internal/crypto/envelope.go
- extensions/air_smgr/air_smgr.c
- extensions/air_smgr/lfc.c
- extensions/air_smgr/communicator.c

Investigate:

1. Page read amplification.
2. Per-page versus batched R2 range reads.
3. Index loading and memory consumption.
4. Layer compression opportunities.
5. Duplicate WAL payload storage.
6. Page image and WAL duplication.
7. WAL redo process spawning and serialization.
8. LFC cache validity and synchronization.
9. DiskCache restart recovery.
10. Layer encoding memory amplification.
11. Compaction and bootstrap memory usage.
12. CAU hashing, encryption, verification, and multipart upload overhead.
13. Manifest publication frequency and concurrency.

For each finding provide:

- Exact file and function.
- Root cause.
- Reproducible evidence.
- Performance impact.
- Proposed solution.
- Risks and correctness implications.
- Benchmark required to verify the improvement.

Do not assume an issue exists without inspecting the actual code.

Phase 2 — Benchmark Infrastructure

Create reproducible benchmarks for:

Read performance

- Random 8 KB reads.
- Sequential page reads.
- 16, 128, and 512-page batch reads.
- Cold and warm cache.
- Different L0 layer counts.

Storage performance

- V1 versus compressed V2 layer sizes.
- Compression CPU cost.
- WAL duplication ratio.
- Index memory consumption.
- Compaction input/output size.

Upload performance

- Single-part versus multipart.
- Sequential versus bounded concurrent multipart.
- Hashing throughput.
- Encryption throughput.
- R2 request count and upload throughput.

Record p50, p95, p99 latency, throughput, memory, CPU, and network bytes.

Never claim improvements without measured results.

Phase 3 — Read Path Optimization

Investigate and implement:

1. Batch page resolution instead of repeatedly resolving each page independently.
2. Group page lookups by layer.
3. Coalesce adjacent payload ranges.
4. Avoid repeated footer/index reads.
5. Introduce bounded index caching.
6. Introduce sparse index and optional Bloom-filter-based layer pruning.
7. Implement adaptive range caching.
8. Implement asynchronous prefetch.
9. Remove unnecessary global locking from cache disk I/O.

Preserve LSN correctness and historical reads.

Phase 4 — WAL Redo Optimization

Replace per-page process spawning with a bounded persistent WAL-redo worker pool.

Requirements:

- Persistent workers.
- Bounded request queues.
- Batch redo where PostgreSQL semantics permit.
- Worker failure recovery.
- Cancellation and timeout support.
- Correct synchronization.
- No unbounded concurrency.

Benchmark against the existing implementation.

Phase 5 — Layer Format V2 Research and Implementation

Design a versioned immutable layer format.

Proposed pipeline:

Sorted records
→ Shared WAL record table
→ Independently compressed data chunks
→ AES-GCM encryption
→ Sparse index
→ Footer
→ Content-addressed upload.

Requirements:

1. Use Zstandard as an experimentally evaluated codec.
2. Compress before encryption.
3. Preserve original data when compression is ineffective.
4. Store codec and size metadata.
5. Use unique nonces.
6. Authenticate chunk metadata.
7. Keep integrity verification.
8. Support bounded-memory streaming encoding.
9. Preserve V1 reading.
10. Support V1 and V2 layers in the same timeline.

Do not change the existing V1 format in place.

Phase 6 — CAU Optimization

Investigate:

- Streaming SHA-256 during encoding.
- Avoiding redundant disk reads.
- Bounded multipart concurrency.
- Upload retry and cancellation.
- Remote verification overhead.
- Manifest publication batching.
- Tenant-scoped chunk-level deduplication research.

Preserve immutable object semantics.

Never weaken manifest durability ordering.

Do not implement unsafe cross-tenant deduplication.

Phase 7 — Compaction and Bootstrap

Replace full-memory accumulation with bounded-memory streaming algorithms.

Investigate:

- K-way merge.
- Sorted layer iteration.
- Streaming output encoding.
- Bounded page buffers.
- Incremental hash calculation.
- Safe layer publication.
- Concurrent read compatibility.

Compaction must preserve the latest valid version of every key and historical LSN semantics.

Phase 8 — Correctness and Regression Testing

Mandatory tests:

- PostgreSQL page checksum verification.
- WAL replay parity.
- Historical LSN reads.
- Crash recovery.
- Interrupted multipart uploads.
- Manifest concurrency.
- V1/V2 mixed-layer reads.
- Encryption integrity.
- Cache invalidation.
- Concurrent reads and compaction.
- Tenant isolation.

Run race detection and relevant existing repository tests.

Engineering Rules

- Do not rewrite the entire Pageserver.
- Do not replace working architecture without measured justification.
- Do not remove existing correctness checks for performance.
- Do not introduce unbounded queues or memory allocation.
- Do not change WAL semantics.
- Do not silently change layer formats.
- Do not claim benchmarks that were not executed.
- Keep every change independently testable.
- Preserve backward compatibility.

Execution Strategy

Work in small, independently reviewable stages.

For each stage:

1. Explain the current implementation.
2. Identify the exact bottleneck.
3. Design the solution.
4. Write tests first where practical.
5. Implement the smallest viable change.
6. Run relevant tests and benchmarks.
7. Compare against the baseline.
8. Document results and remaining risks.

Start with Phase 1. Produce an engineering audit and prioritized remediation plan before modifying implementation code.

Do not start all phases simultaneously.


---

## Phase 9 Validation Results, Fixes & Remaining Risks

**Environment**: `go1.27.3 linux/amd64`, 2 vCPU (`AMD EPYC 7763 64-Core Processor`), 7.8 GiB RAM, 0 B swap.

### Issues Discovered & Fixed (with Reproducible Tests)

1. **[P0 - Security / Data Race] `EnvelopeKMS.tenantAEAD` & `UnwrapDEK` vs `ShredDEK` / `LoadWrappedDEK`** (`internal/crypto/envelope.go`):
   - **Finding**: `tenantAEAD` and `UnwrapDEK` read the shared `wrapped` slice after releasing `k.mu.RLock()`, racing with `ShredDEK` zeroing the slice in place under `k.mu.Lock()`, and `tenantAEAD` could cache a stale DEK's AEAD if `LoadWrappedDEK` rotated the DEK concurrently.
   - **Repro**: `TestPhase9_EnvelopeKMS_ConcurrentShredAndEncrypt` (`-race` data race + `cipher: message authentication failed`) and `TestPhase9_EnvelopeKMS_ConcurrentRotationAndCacheInvalidation` (failed at iter 65).
   - **Fix**: Copy `wrapped` bytes under `RLock` and zero the temporary copy; verify `bytes.Equal(cur, wrappedCopy)` under `Lock` before populating `aeadCache` (retrying on concurrent rotation); documented linearizable `ShredDEK` erasure boundary.

2. **[P1 - Correctness] `PublishLayerManifest` Stale-ETag Blind Overwrite** (`internal/pageserver/layers.go`):
   - **Finding**: On `412 Precondition Failed`, `PublishLayerManifest` re-fetched the remote ETag and blindly overwrote the newer manifest with stale `body`, silently losing concurrent layer publications; local manifest was also written before R2 CAS succeeded.
   - **Repro**: `TestPhase9_Part3_ManifestCASConflictDoesNotOverwrite`.
   - **Fix**: Return `ErrManifestCASConflict` on `412 Precondition Failed` without overwriting, and persist local manifest only after R2 CAS succeeds.

3. **[P1 - Security / Availability] Unbounded Allocation on Malformed Unencrypted V2 Metadata** (`internal/pageserver/layer_v2.go`):
   - **Finding**: `decodeV2WALTable` allocated `make([][]byte, count)` before validating `count*4 <= len(data)-4` (reproduced `fatal error: runtime: out of memory`), and `decompressV2Payload` preallocated `make([]byte, 0, rawLen)` from untrusted `rawLen`.
   - **Repro**: `TestPhase9_Part1_V2CorruptionMalformedAndBounds`.
   - **Fix**: Enforce `maxV2DecompressedSize` (256 MiB), cap `DecodeAll` preallocation hint to 1 MiB, and validate `count*4 <= len(data)-4` and `uint64` index size before slice allocation.

4. **[P1 - Concurrency] `Timeline.CompactIfNeeded` Duplicate L1 Race** (`internal/pageserver/compactor.go`, `internal/pageserver/readpath.go`):
   - **Finding**: Concurrent `CompactIfNeeded` calls could snapshot the same L0 set and append duplicate L1/Image layers.
   - **Fix**: Added `Timeline.compactMu` serializing compaction without blocking concurrent readers.

5. **[P2 - Correctness] `BootstrapTenantFromPGData` Multi-Segment RelSize & `writeLayerFromStream` Orphan File** (`internal/pageserver/server.go`, `internal/pageserver/compactor.go`):
   - **Finding**: `BootstrapTenantFromPGData` only emitted `RelSizeKey` for `segNum == 0` and mutated `tl.relSizes` before manifest publication; `writeLayerFromStream` renamed `.tmp` to `.layer` before CAU upload.
   - **Repro**: `TestPhase9_Part4_BootstrapMultiSegmentAndErrors`.
   - **Fix**: Accumulate `segNum*131072 + blkIdx` across all segments, update `tl.relSizes` only after `PublishNewLayer` succeeds, and rename `tmpPath -> localPath` only after CAU upload succeeds.

### Verification & Benchmarks

- Full `-race` suites after final edits:
  - `go test -race -count=1 ./internal/crypto/...` -> `PASS` (`1.144s`)
  - `go test -race -count=1 ./internal/pageserver/...` -> `PASS` (`22.153s`)
- Separated Benchmarks (`-count=5 -benchtime=200ms`, mock R2 — not directly compared to real R2):
  - Unit `EnvelopeKMS_Encrypt_8KiB`: `11.24–13.00 µs/op` (`630.08–728.60 MB/s`, `6 allocs/op`)
  - Unit `V2_Compress_64KiB_Compressible`: `47.40–54.07 µs/op` (`1212.03–1382.59 MB/s`, `1 alloc/op`)
  - Integration `ColdCache_Batch16_16L0`: `401.38–622.77 µs/op` (`210.47–326.55 MB/s`, `151 allocs/op`)
  - Integration `WarmCache_Batch16_16L0`: `422.84–751.39 µs/op` (`174.44–309.98 MB/s`, `124 allocs/op`)
- V2 Large Layer (`1024` records, `8 MiB` raw, encrypted):
  - Compressible: `25,094 B` (`334.29x` ratio), encode `73.7 ms`, decode `48.7 ms` (`164.4 MB/s`)
  - Incompressible: `8,413,039 B` (`1.00x` ratio), encode `376.1 ms`, decode `66.7 ms` (`120.0 MB/s`)
- Multi-Tenant Scale Matrix (`-race`, 0 errors, strict tenant isolation verified):
  - `1tl_1r_1l0_b1`: `3,638 pages/s`, `p50=200µs`, `p99=234µs`, `heapAlloc=4.0 MiB`
  - `10tl_8r_16l0_b16`: `11,481 pages/s`, `p50=9.38ms`, `p99=14.58ms`, `heapAlloc=7.5 MiB`
  - `100tl_32r_1l0_b128`: `14,043 pages/s`, `p50=261.6ms`, `p99=535.4ms`, `heapAlloc=21.2 MiB`
  - `4tl_128r_64l0_b512`: `12,075 pages/s`, `p50=4.91s`, `p99=5.35s`, `heapAlloc=1.61 GiB`

### Remaining Risks (for Phase 10 / Pre-Production)

1. **Corrupted `DiskCache` entry is not automatically evicted on decode/auth failure**: `ReadRange` returns corrupted bytes and fails authentication safely (no silent corruption), but subsequent reads keep hitting the bad cache file until LRU eviction.
2. **Real R2 / Multi-Process / Long-Running Soak Tests**: Current environment has no live R2 credentials/endpoint; real R2 multipart/CAS latency, multi-process manifest contention, and 24h+ soak memory stability remain unexecuted.
3. **High-concurrency large-batch heap spike (`1.61 GiB` at `128 readers * 512 pages`)**: `GetPageBatch` decodes chunks per request without a decoded-chunk cache or per-tenant read concurrency semaphore.
4. **External `walredo` process crash-loop testing**: `WALRedoPool` request cancellation and timeout paths are tested, but repeated external PG17 worker `SIGKILL` recovery requires the compiled PG17 `walredo` binary.
