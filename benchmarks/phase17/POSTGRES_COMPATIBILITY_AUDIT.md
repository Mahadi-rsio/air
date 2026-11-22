# AIR Phase 17A — PostgreSQL Compatibility Audit (`POSTGRES_COMPATIBILITY_AUDIT.md`)

## 1. Executive Summary

We audited AIR's PostgreSQL 17.12 compatibility across catalog decoding, heap tuple deformation, WAL replay, MVCC/HOT visibility, B-tree index scans, TOAST decompression, and gateway SQL routing/fallback. Two critical correctness gaps discovered in Phase 16 were resolved in Phase 17A:
1. **Fast-Default (`atthasmissing` / `attmissingval`)**: `pg_catalog.go`, `pgpage.go`, `sqlite_vtab.go`, and `parallel_scan.go` now decode `pg_attribute.attmissingval` (`anyarray`) and substitute missing trailing attributes for pre-`ALTER TABLE` tuples, falling back safely to PostgreSQL (`ErrUnsupportedType`) if `attmissingval` is TOASTed or an unsupported type.
2. **Pageserver `HEAP2_PRUNE_*` & Strict WAL Replay**: `internal/pageserver/heap_redo.go` now replays PG17 `XLOG_HEAP2_PRUNE_ON_ACCESS`, `XLOG_HEAP2_PRUNE_VACUUM_SCAN`, `XLOG_HEAP2_PRUNE_VACUUM_CLEANUP`, and `XLOG_HEAP2_LOCK_UPDATED` (`xl_heap_prune` freeze plans, `LP_REDIRECT`, `LP_DEAD`, `LP_UNUSED`). `DefaultWALRedoer.Redo`/`RedoBatch` in `walredo.go` no longer silently return an un-updated base page when an unsupported WAL record is encountered.

---

## 2. PostgreSQL Compatibility Matrix

| Feature | Supported | Tested | Fallback | Known Limitations |
|---|---|---|---|---|
| **Core Types** (`int2`, `int4`, `int8`, `float4`, `float8`, `bool`, `char`, `name`, `oid`, `text`, `varchar`, `bpchar`, `bytea`, `date`, `timestamp`, `timestamptz`, `uuid`, `json`, `jsonb`, `int2vector`) | Yes | Yes | N/A | `timestamp`/`date` formatted as ISO text in SQLite; `timestamptz` rendered in UTC |
| **Unsupported Types** (`numeric`, `interval`, `time`, `inet`, arrays, enums, domains, `tsvector`) | No | Yes | **Explicit PG Fallback** (`ErrUnsupportedType`) | Cannot skip unknown-width attributes during tuple deformation; entire table scan falls back to PG |
| **NULL Bitmap & Trailing NULLs** | Yes | Yes | N/A | Full `t_bits` support |
| **Fast-Default (`ALTER TABLE ... ADD COLUMN ... DEFAULT`)** | Yes | Yes | **Explicit PG Fallback** if TOASTed/unsupported | Supports all core types; `attmissingval` >2KB (TOASTed) triggers fallback |
| **DROP COLUMN (`attisdropped`)** | Yes | Yes | N/A | Preserves physical heap offset alignment via `AllCols` while hiding dropped columns from `VisibleIdx` |
| **HOT Updates & Chains (`HEAP_HOT_UPDATED`, `LP_REDIRECT`)** | Yes | Yes | N/A | Bounded chain length `maxHOTChainLen = 64` with cycle detection |
| **Heap Pruning WAL (`HEAP2_PRUNE_ON_ACCESS`, `PRUNE_VACUUM_*`)** | Yes | Yes | Explicit error if malformed | Pure-Go redo updates line pointers & freeze plans; does not compact free space (`PageRepairFragmentation`) which does not affect `offnum` addressing |
| **Unsupported WAL Records** | No | Yes | Explicit error (never silent) | Requires external `walredo` worker binary if non-heap/non-btree RMID deltas are present |
| **TOAST (`PGLZ`, `LZ4`, External `VARTAG_ONDISK`)** | Yes | Yes | **Explicit PG Fallback** (`ErrTOASTUnsupported`) | Max 64 MiB per value; `VARTAG_INDIRECT` / `VARTAG_EXPANDED` fall back to PG |
| **MVCC Snapshot Visibility & CLOG/Subtrans** | Yes | Yes | **Explicit PG Fallback** on missing SLRU | Wraparound-safe XID comparison; `pg_xact` + `pg_subtrans` SLRU resolution |
| **MultiXact (`HEAP_XMAX_IS_MULTI`)** | Partial | Yes | **Explicit PG Fallback** (`ErrMultiXactUnsupported`) | Lock-only MultiXact handled; update-in-multi falls back to PG |
| **B-Tree Index Scans** | Yes | Yes | Safe fallback to SeqScan | Non-partial, non-expression B-tree indexes only; always re-checks heap visibility & predicates; no index-only scans (VM not trusted for visibility) |
| **Concurrent DDL & Plan Invalidation** | Yes | Yes | Re-syncs at `CatalogModLSN` | `SchemaVersionRegistry` + `PlanCacheKey` (`CatalogModLSN`, `SchemaVersion`) prevent stale plans |
| **PG Operators/Functions** (`ILIKE`, regex `~`, `::cast`, window functions, `DISTINCT ON`, CTE writes, `FOR UPDATE`) | Partial | Yes | **Gateway WriteEngine / ReadErrorFallback** | Mutating/locking/transaction queries route to PG; SQLite-incompatible read syntax triggers `RouteReasonReadErrorFallback` to PG |
