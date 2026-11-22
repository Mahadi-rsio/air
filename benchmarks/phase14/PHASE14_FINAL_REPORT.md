# AIR Phase 14 — Final Verification & Benchmark Report

## Overview

Phase 14 completes MultiXact WAL ingest/visibility & restart recovery (**14C**), incremental page-level cache invalidation (**14D**), and live PostgreSQL 17.12 verification (**14E**), building on the completed **14A** (B-tree index pushdown) and **14B** (TOAST decompression & out-of-order/corrupt chunk handling) foundations.

---

## 14A & 14B (Previously Completed)

- **14A B-tree pushdown**: Implemented in `internal/readadapter/btree.go`, `internal/pageserver/btree_redo.go`, `pg_index` discovery, and cost-based `xBestIndex`. PK lookup read 4 pages and was 13.5x faster than a sequential scan; heap tuples are re-validated with MVCC.
- **14B TOAST**: Implemented in `internal/readadapter/toast.go` (PGLZ, LZ4, out-of-order chunks, corrupt chunk handling), plus WAL parser fix for split record headers across WAL pages.

---

## 14C — MultiXact SLRU Ingest, Visibility, and Restart Recovery

### Key Fixes & Verification
1. **`RmMultiXactID` correction (`internal/pageserver/multixact.go`)**:
   - Fixed `RmMultiXactID` from `13` (`RM_HASH_ID`) to `6` (`RM_MULTIXACT_ID`, verified against `/usr/include/postgresql/17/server/access/rmgrlist.h`).
   - Batched same-page `offsets` and `members` writes in `applyMultiXactWALLocked` so `t.inMem` never receives duplicate entries at the same `rec.EndLSN`.
2. **`xl_multixact_truncate` layout verification**:
   - Verified against `/usr/include/postgresql/17/server/access/multixact.h` (`oldestMultiDB` at 0..4, `startTruncOff` at 4..8, `endTruncOff` at 8..12, `startTruncMemb` at 12..16, `endTruncMemb` at 16..20), matching `internal/pageserver/multixact.go`.
3. **PG17 catalog compatibility (`internal/readadapter/pg_catalog.go`)**:
   - Updated `isPG15` detection to use `pg_database` column count (`16` in PG15 vs `17` in PG17) instead of `(thdr.Infomask2 & HeapNattsMask) == 26` (which is `26` in both PG15 and PG17).
4. **New Tests (`internal/readadapter/multixact_test.go`)**:
   - `TestPhase14C_RestartRecovery`: Flushes layers, restarts pageserver/timeline, warms SLRU pages from layers, and replays additional MultiXact WAL.
   - `TestPhase14C_PG17_MultiXactLive`: Runs live PostgreSQL 17.12 MultiXact workloads (`FOR KEY SHARE` + `UPDATE`/`DELETE` lock-only and update members) and compares AIR MVCC visibility against PG17.
   - Note on PG15 matrix: `TestPhase13_PG15_CompatibilityMatrix` (`internal/readadapter/phase13_pg15_compat_test.go`) skips when `/usr/lib/postgresql/15/bin` is not installed on the VM.

---

## 14D — Incremental Page-Level Cache Invalidation

### Architecture Changes
1. **Pageserver Modification Tracking (`internal/pageserver/readpath.go`, `server.go`, `protocol.go`)**:
   - Added `ModifiedBlockEvent`, `ModifiedBlocksResult`, `Timeline.recordModifiedLocked`, and `Timeline.GetModifiedBlocksSince`.
   - Wired modification tracking into `ApplyWALRecord` (`rec.Blocks`, `XLogSmgrCreate`, `XLogSmgrTruncate`), `PutPageBatch` (including updating `t.lastLSN` under `t.mu`), and `UnlinkRel`.
   - Exposed `Pageserver.GetModifiedBlocksSince` in-process and over the Unix socket wire protocol (`OpGetModifiedBlocks = 10`, `ModifiedBlocksProvider`).
2. **Read Adapter Tiered Cache (`internal/readadapter/pageserver_backend.go`, `pageserver_socket_backend.go`, `cache.go`)**:
   - Added `BlockCacheKey`, `l1Latest`, `l2Latest`, `metaLatest`, `ensureSynced` with `syncSF` singleflight, and `CatalogLSN`.
   - Selectively invalidates only affected `(tenant, gen, rel, fork, block)` entries and relation sizes/existence, while preserving unaffected hot blocks across LSN advances.
   - Supports range-trimmed `GetPageBatch` and handles relation truncation, unlink, rewrite, and FPI/WAL block updates.
3. **Catalog Discovery Optimization (`internal/readadapter/sqlite_server.go`)**:
   - Tracks `lastCatalogModLSN` via `getCatalogLSN` to skip `DiscoverCatalog` when the catalog modification LSN is unchanged.

### Hit Ratio Benchmark (`TestPhase14D_HitRatioBenchmark`)

Measured on 200-block working set (`phase14d_invalidation_test.go`):

| Workload Profile | Write % | Phase 13 Hit Ratio | Phase 14D Hit Ratio | Phase 14D Observed Details |
|---|---:|---:|---:|---|
| 90/10 Read-Heavy | 10% | 23.76% | **98.02%** | `hits=4411 reqs=4500 l3_fetches=89` |
| 50/50 Balanced | 50% | 8.47% | **82.36%** | `hits=2059 reqs=2500 l3_fetches=441` |
| 10/90 Write-Heavy | 90% | 3.92% | **71.60%** | `hits=358 reqs=500 l3_fetches=142` |

---

## 14E — Live PostgreSQL 17.12 Verification & Test Summary

All checks were executed after the final code edit:

- `go test ./internal/pageserver/... ./internal/readadapter/... -short`: **PASS** (`internal/pageserver` `17.645s`, `internal/readadapter` `6.662s`).
- `go test ./internal/readadapter -run "TestPhase14C|TestPhase14D|TestPhase14E" -v`: **PASS** (`2.112s`):
  - `TestPhase14C_Unit_SLRUReplayAndResolve`: PASS (`0.00s`)
  - `TestPhase14C_RestartRecovery`: PASS (`0.02s`)
  - `TestPhase14C_PG17_MultiXactLive`: PASS (`0.96s`)
  - `TestPhase14D_IncrementalInvalidationCorrectness`: PASS (`0.00s`)
  - `TestPhase14D_HitRatioBenchmark`: PASS (`0.07s`)
  - `TestPhase14E_PG17_LiveVerification`: PASS (`1.04s`) — `Cache stats: {L1Hits:171 L1Misses:92 L1Evictions:0 L1Bytes:729088 L2Hits:0 L2Misses:92 L2Evictions:0 L2Corrupt:0 L2Bytes:729444 L3Fetches:16 StampedesPrevented:0}`

### Known Limitations (Unchanged)
- CLOG `slruPages` cache is cold after a pageserver restart (warmed from layers for MultiXact only).
- Subtrans SLRU is not ingested.
- Compressed FPIs (`ErrCompressedFPI`) are unsupported.
- Frozen-layer delta ordering in `lookupInMemLayersLocked` is unverified.
- PG15 binaries (`/usr/lib/postgresql/15/bin`) are not installed on this VM, so PG15-only live compatibility tests skip here.
