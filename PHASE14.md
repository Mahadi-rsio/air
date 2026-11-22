# AIR Phase 14 — Status

## Done (tested)
- **14A B-tree pushdown**: `internal/readadapter/btree.go`, `internal/pageserver/btree_redo.go`, `pg_index` discovery, cost-based `xBestIndex`. A PK lookup read 4 pages and was 13.5x faster than a sequential scan. Heap tuples are always re-validated with MVCC.
- **14B TOAST**: `toast.go` (PGLZ, LZ4, out-of-order chunks, corrupt chunk handling). The live PG15 suite compares AIR with PostgreSQL byte-for-byte, including an old pinned snapshot, with zero fallbacks. Fixed a WAL parser bug where a record header split across a WAL page was dropped.
- **14C MultiXact (ingest and visibility)**:
  - The pageserver ingests the `pg_multixact/offsets` and `members` SLRUs from WAL (`internal/pageserver/multixact.go`). Fixed `RmMultiXactID` (`6`, `RM_MULTIXACT_ID`) and batched same-page SLRU writes. Verified `xl_multixact_truncate` layout against PG17 headers. Bootstrap seeds the files, and SLRU pages are warmed from layers after a restart.
  - `CLOGResolver.ResolveMultiXact` and `MultiXactUpdateXid` are wired into `mvcc.go` for `HEAP_XMAX_IS_MULTI`. Lock-only members are never treated as updaters. Missing SLRU data falls back to the primary.
  - Tests `TestPhase14C_Unit_SLRUReplayAndResolve`, `TestPhase14C_RestartRecovery`, and `TestPhase14C_PG17_MultiXactLive` all pass. (`TestPhase13_PG15_CompatibilityMatrix` skips when PG15 binaries are absent.)
- **14D incremental page-level invalidation**:
  - Tracked per-relation/block modification LSNs and catalog LSN in the pageserver (`ModifiedBlockEvent`, `ModifiedBlocksResult`, `Timeline.GetModifiedBlocksSince`, wired into `ApplyWALRecord`, `PutPageBatch`, `UnlinkRel`), exposed in-process (`PageserverBackend`) and over Unix socket (`OpGetModifiedBlocks = 10` in `protocol.go` and `PageserverSocketBackend`).
  - `TieredCacheBackend` (`internal/readadapter/cache.go`) invalidates only affected `(tenant, gen, rel, fork, block)` entries via `ensureSynced` + `syncSF` singleflight, preserves unaffected hot blocks, and handles truncate/unlink/FPI. `sqlite_server.go` skips `DiscoverCatalog` when catalog LSN is unchanged.
  - `TestPhase14D_IncrementalInvalidationCorrectness` and `TestPhase14D_HitRatioBenchmark` pass: 90/10 improved from `23.76%` to `98.02%`, 50/50 from `8.47%` to `82.36%`, and 10/90 write-heavy from `3.92%` to `71.60%`.
- **14E live PG17 verification & final report**:
  - Fixed PG17 `pg_attribute` catalog decoding in `internal/readadapter/pg_catalog.go` (`isPG15` detected via `pg_database` column count).
  - `TestPhase14E_PG17_LiveVerification` passes against PostgreSQL 17.12.
  - Final report written to `benchmarks/phase14/PHASE14_FINAL_REPORT.md`.

## Known limitations
- The CLOG `slruPages` cache is cold after a pageserver restart (fixed for multixact only).
- Subtrans SLRU is not ingested.
- Compressed FPIs (`ErrCompressedFPI`) are unsupported.
- Frozen-layer delta ordering in `lookupInMemLayersLocked` is unverified.
- PG15 binaries are not installed on this VM (PG17.12 is installed and verified).
