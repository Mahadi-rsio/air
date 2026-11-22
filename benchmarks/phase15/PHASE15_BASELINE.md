# AIR Phase 15A — Architecture Audit & Baseline Measurements

## 1. Environment & Hardware

- **CPU**: AMD EPYC 7763 64-Core Processor (2 vCPUs exposed)
- **Memory**: 7.8 GiB RAM, 0 B swap
- **OS**: Linux (amd64)
- **Go**: `go1.27.3 linux/amd64`
- **PostgreSQL**: PostgreSQL 17.12 (`/usr/bin/psql`)
- **SQLite**: `/opt/conda/bin/sqlite3` (CGO `-lsqlite3`)

---

## 2. Architecture Audit Summary

### 2.1 Go Gateway (`internal/gateway/`)
- **Wire Protocol (`protocol.go`, `client_session.go`)**: Full PG v3 startup, TLS/SNI tenant extraction, `CancelRequest` forwarding, simple (`Q`) and extended (`P`/`B`/`D`/`E`/`S`) query handling.
- **Query Classification & Routing (`sql_router.go`, `sql_inspect.go`)**:
  - `tokenizeStatements` + `classifySingleStatement` classify queries into `RouteTargetReadEngine` vs `RouteTargetWriteEngine`.
  - `RouteQuery` enforces causal consistency (`SessionWriteLSN` vs `ReadEngineLSN`), transaction write state (`TxHasWrites`), session pinning (`LISTEN`, `TEMP TABLE`, `pg_advisory_lock`), and `ReadEngineHealthy`.
  - `RecordReadFallback` tracks fallbacks when the read engine returns an error or lags behind `SessionWriteLSN`.
- **Connection Pooling (`pooler.go`)**: Manages `idlePool` (PG primary) and `readIdlePool` (SQLite read engine), per-client prepared statement rewriting (`_air_s_<clientID>_<seq>`).

### 2.2 Read Engine (`internal/readadapter/`)
- **SQLite Server (`sqlite_server.go`, `sqlite_cgo.go`)**:
  - `SQLiteReadServer` speaks PG v3 wire protocol over TCP/Unix socket.
  - Strictly read-only: `SQLITE_OPEN_READONLY`, `PRAGMA query_only=ON`, and C-level `sqlite3_set_authorizer` (`air_sqlite_ro_authorizer`).
  - Per-tenant `SQLiteTenantStore` with `readerPool` (`sqliteReaderSlot`s) and concurrency semaphore (`sem`).
  - `refreshNativeCatalogOnSlot` skips `DiscoverCatalog` when `CatalogLSN` is unchanged (Phase 14D).
  - `translatePGToSQLite` rewrites `$N -> ?N` and strips `::type` casts via regex.
  - `handleExplainStmt` delegates `EXPLAIN SELECT ...` to SQLite `EXPLAIN QUERY PLAN` without AIR-specific cost/index/cache/pushdown diagnostics or `EXPLAIN ANALYZE`.
- **Virtual Table (`sqlite_vtab.go`)**:
  - `air_pg` module implements `xConnect`, `xBestIndex`, `xOpen`, `xClose`, `xFilter`, `xNext`, `xEof`, `xColumn`, `xRowid`.
  - Projection pushdown uses `pIdxInfo->colUsed` bitmask to decode only needed columns (`neededCols`).
  - `air_pg_best_index` supports `EQ`, `GT`, `LE`, `LT`, `GE`, `ISNULL`, `ISNOTNULL`; does not push down `NE`, `ORDER BY` (`orderByConsumed`), or `LIMIT`.
  - `goAirPGBestIndexCost` uses hardcoded constants (`2.0`/`1`, `10.0`/`5`, `40.0`/`50`, `10000.0`/`10000`) without table size, statistics, selectivity, or L1/L2/L3 cache residency awareness.
- **B-Tree Index Scan (`btree.go`)**:
  - Supports multi-column prefix matching, composite keys, `DESC` / `NULLS FIRST`, posting-list TIDs, and MVCC + HOT-chain revalidation (`fetchVisibleTuplesByTIDs`).
  - `goAirPGFilter` always chooses the first matching index without comparing index scan cost against sequential scan cost.
- **Catalog & Storage (`pg_catalog.go`, `cache.go`, `adapter.go`)**:
  - `DiscoverCatalog` scans `pg_database`, `pg_namespace`, `pg_class`, `pg_attribute`, `pg_index`. `pg_class.relpages` and `pg_class.reltuples` are decoded in `pgClassCols` but discarded; `pg_statistic` is not read.
  - `TieredCacheBackend` provides L1 RAM LRU + L2 NVMe CRC32C-checksummed cache + L3 Pageserver (`StorageBackend`) with Phase 14D incremental block invalidation.

### 2.3 Identified Phase 15 Bottlenecks & Opportunities
1. **Hardcoded `xBestIndex` cost model**: Ignores `reltuples`/`relpages`, column selectivity, and L1/L2/L3 cache hit probability.
2. **Missing PostgreSQL statistics integration**: `relpages`/`reltuples` are discarded during catalog discovery; `pg_statistic` (`stadistinct`, `stanullfrac`, `stawidth`) is unused.
3. **Unconditional index selection**: `goAirPGFilter` uses an index even when a sequential scan on a small or low-selectivity table is cheaper.
4. **No `ORDER BY` / `LIMIT` awareness**: B-tree scans collect all matching TIDs even when `ORDER BY` matches the index order or `LIMIT` is small.
5. **No Go-native query plan / prepared statement cache**: Every `QueryWithContext` calls `sqlite3_prepare_v2` / `sqlite3_finalize` and re-translates PG SQL via regex.
6. **No adaptive execution feedback**: Estimated vs actual rows/pages/cache hits are not tracked or fed back into cost estimation.
7. **Limited `EXPLAIN` diagnostics**: `EXPLAIN` only returns SQLite's raw `EXPLAIN QUERY PLAN`, hiding AIR's route, index choice, cache tier, pushdown flags, and `ANALYZE` actuals.

---

## 3. Baseline Test & Benchmark Results (Phase 14 / Pre-Phase 15)

### 3.1 Existing Test Suite
- `go test ./internal/gateway/... ./internal/readadapter/... -short`: **PASS** (`gateway` 5.055s, `readadapter` cached/pass).

### 3.2 Microbenchmarks (`go test -bench=BenchmarkReadEngine -benchmem -benchtime=1s ./internal/readadapter/...`)

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkReadEngineL1HotPage-2` | 3,440 | 8,208 | 2 | L1 RAM LRU hit (8 KiB copy) |
| `BenchmarkReadEngineL2NVMePage-2` | 16,988 | 9,928 | 6 | L2 NVMe read + CRC32C |
| `BenchmarkReadEngineL3PageserverPage-2` | 3,236 | 8,440 | 6 | In-process Pageserver read |
| `BenchmarkReadEngineMVCCWithCLOG-2` | 53.01 | 0 | 0 | Zero-alloc MVCC visibility check |
| `BenchmarkReadEngineCommitPublish-2` | 83.96 | 0 | 0 | Zero-alloc commit publish |
| `BenchmarkReadEngineSQLiteQuery-2` | 9,955 | 312 | 16 | Native SQLite query (no vtab) |
| `BenchmarkReadEnginePageserverSocket-2` | 45,836 | 17,544 | 21 | Unix socket Pageserver RPC |
| `BenchmarkReadEngineAirPGVTab-2` | 24,847 | 9,937 | 38 | `air_pg` vtab point query (50 rows, 1 page) |

### 3.3 B-Tree vs Sequential Scan (`BenchmarkPhase14A_BTreePointLookupVsSeqScan`)

| Benchmark | ns/op | B/op | allocs/op | Speedup |
|---|---:|---:|---:|---|
| `BTreeIndexPushdown_5000rows_100pages-2` | 58,541 | 36,947 | 73 | **12.96x** vs SeqScan |
| `SequentialHeapScan_5000rows_100pages-2` | 758,478 | 830,189 | 173 | 1.00x |

### 3.4 Phase 14D Cache Hit Ratio Baseline (from `PHASE14_FINAL_REPORT.md`)
- 90/10 Read-Heavy: **98.02%** (`hits=4411 reqs=4500 l3_fetches=89`)
- 50/50 Balanced: **82.36%** (`hits=2059 reqs=2500 l3_fetches=441`)
- 10/90 Write-Heavy: **71.60%** (`hits=358 reqs=500 l3_fetches=142`)
