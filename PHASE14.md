# AIR Phase 14 — Status

## Done (tested)
- **14A B-tree pushdown**: `internal/readadapter/btree.go`, `internal/pageserver/btree_redo.go`, `pg_index` discovery, cost-based `xBestIndex`. A PK lookup read 4 pages and was 13.5x faster than a sequential scan. Heap tuples are always re-validated with MVCC.
- **14B TOAST**: `toast.go` (PGLZ, LZ4, out-of-order chunks, corrupt chunk handling). The live PG15 suite compares AIR with PostgreSQL byte-for-byte, including an old pinned snapshot, with zero fallbacks. Fixed a WAL parser bug where a record header split across a WAL page was dropped.
- **14C MultiXact (ingest and visibility)**:
  - The pageserver ingests the `pg_multixact/offsets` and `members` SLRUs from WAL (`internal/pageserver/multixact.go`). That covers CREATE_ID, ZERO pages and TRUNCATE_ID tracking. Bootstrap seeds the files, and the SLRU page is warmed from layers after a restart.
  - `CLOGResolver.ResolveMultiXact` and `MultiXactUpdateXid` are wired into `mvcc.go` for `HEAP_XMAX_IS_MULTI`. Lock-only members are never treated as updaters. A missing SLRU data falls back to the primary.
  - Unit test `TestPhase14C_Unit_SLRUReplayAndResolve` passes. It covers lock-only, update and delete members, an offsets page boundary, uint32 wraparound, LSN history, truncation and zeroing.

## Not done
- **14C**: no live PG15 multixact comparison test, and no pageserver restart-recovery test that flushes layers and replays more WAL. The TRUNCATE record layout is assumed. `TestPhase13_PG15_CompatibilityMatrix` may need its expectations updated.
- **14D incremental page-level invalidation**: not started. Plan:
  - Track per-relation/block modification LSNs and a catalog LSN in the pageserver, and expose a "modified blocks since LSN" feed.
  - Invalidate only the affected `(tenant, gen, rel, fork, block)` entries in `cache.go`, keep LSN validation, add singleflight, and skip `DiscoverCatalog` when the catalog LSN is unchanged.
  - Handle truncate, rewrite, VACUUM and FPI.
  - Measure against the Phase 13 hit ratios (90/10 was 23.76%, 50/50 was 8.47%, write-heavy was 3.92%).
- **14E**: PG17 live verification (no PG17 binaries on the dev VM) and `benchmarks/phase14/PHASE14_FINAL_REPORT.md` are not started. No Phase 14 numbers beyond 14A/14B are claimed.

## Known limitations
- The CLOG `slruPages` cache is cold after a pageserver restart (fixed for multixact only).
- Subtrans SLRU is not ingested.
- Compressed FPIs (`ErrCompressedFPI`) are unsupported.
- Frozen-layer delta ordering in `lookupInMemLayersLocked` is unverified.
