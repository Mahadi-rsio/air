# Read-Only Engine, Storage Adapter & Native PG17 Architecture

This document records the **Phase 1 Architecture Audit** and **Phase 2 Shared Storage Compatibility Proof** for AIR's dual-engine read/write architecture ([pglite.md](file:///workspaces/air/pglite.md)), including the controlled `internal/readadapter` prototype and the architectural decision to use a **native `x86_64` PostgreSQL 17 read-only engine** rather than browser-oriented `wasm32` WASM.

---

## 1. Phase 2 Shared Storage Compatibility Proof

### 1.1 Empirical `wasm32` PGlite vs. AIR PG17.12 Comparison

We tested `@electric-sql/pglite@0.4.6` (`PostgreSQL 17.5` compiled for `wasm32-unknown-linux-gnu`) against AIR's `PostgreSQL 17.12` (`x86_64-pc-linux-gnu`):

| Property | PGlite `0.4.6` (`PG 17.5 wasm32`) | AIR (`PG 17.12 x86_64`) | Compatibility |
| :--- | :--- | :--- | :--- |
| **PostgreSQL Major Version** | `17.5` | `17.12` | Compatible (PG 17) |
| **`pg_control` Version** | `1700` | `1700` | Exact Match |
| **Catalog Version Number** | `202406281` | `202406281` | Exact Match |
| **Maximum Data Alignment** | `8` | `8` | Exact Match |
| **Database Block Size** | `8192` | `8192` | Exact Match |
| **Blocks per Segment** | `131072` | `131072` | Exact Match |
| **WAL Block / Segment Size** | `8192` / `16777216` | `8192` / `16777216` | Exact Match |
| **Data Page Checksum Version** | `0` | `0` | Exact Match |
| **`Float8` Argument Passing** | `by reference` (`USE_FLOAT8_BYVAL = 0`) | `by value` (`USE_FLOAT8_BYVAL = 1`) | **Incompatible** |

Opening AIR `PG17.12` `PGDATA` directly in `wasm32` PGlite fails with:
```text
FATAL:  database files are incompatible with server
DETAIL:  The database cluster was initialized with USE_FLOAT8_BYVAL but the server was compiled without USE_FLOAT8_BYVAL.
```

### 1.2 Architectural Decision: Native `x86_64` PG17 Read-Only Engine

Because AIR runs natively on `x86_64` Linux with Go (`air-server`) and C++20 (`air-gateway`), browser-oriented `wasm32` WASM is unnecessary. Moving forward with a **native `x86_64` PG17 read-only engine** provides:
- **100% Binary Compatibility:** Identical `USE_FLOAT8_BYVAL = 1`, `MAXALIGN = 8`, `pg_control`, catalog layout, heap tuples, TOAST, and B-tree pages.
- **Zero WASM/Emscripten Overhead:** No `emsdk` toolchain dependency, no single-user `--single` memory ceiling, and native IPC/shared-memory performance.

---

## 2. Controlled Read-Only Storage Adapter (`internal/readadapter`)

The Phase 2 prototype in [adapter.go](file:///workspaces/air/internal/readadapter/adapter.go) and [pgpage.go](file:///workspaces/air/internal/readadapter/pgpage.go) provides a controlled, read-only interface over `pageserver.Pageserver`:

- **Snapshot Lifecycle:** `GetSnapshot(ctx, tenantID, generation)` / `ReleaseSnapshot(snap)` with `PinnedMinLSN(tenantID)` tracking for safe GC/compaction retention.
- **Relation Access:** `OpenRelation`, `CloseRelation`, `GetRelationMetadata`, `ReadPage`, `ReadPages`, and `GetPageVersion`.
- **Safety Invariants:**
  1. No secondary authoritative storage layer (`Pageserver` remains the sole source).
  2. Strict tenant ID validation (`tenant.ValidateID`) and generation fencing (`ErrStaleGeneration`).
  3. Cross-tenant/cross-generation snapshot rejection (`ErrTenantMismatch`), released snapshot rejection (`ErrInvalidSnapshot`), and closed handle rejection (`ErrRelationClosed`).
  4. Zero persistent write methods exposed.
- **Live PG17 Verification ([adapter_test.go](file:///workspaces/air/internal/readadapter/adapter_test.go)):**
  - `TestAdapterIsolationAndSnapshotRules` and `TestLivePG17SharedStorageCompatibilityProof` verify PG17 `PageHeaderData` (`0x2004`), `ItemIdData`, `HeapTupleHeaderData`, MVCC visibility (committed rows visible, rolled-back row excluded), `INT4`/`INT8`/`FLOAT8`/`TEXT` (including compressed/TOASTed varlena), and B-tree metapage (`0x053162`) / leaf pages against live `PostgreSQL 17.12`.

---

## 3. Native PG17 Read-Only Execution & Storage Integration (Phase 3)

- **`extensions/air_ro/` (`air_ro.so`):** Native PG17 C extension compiled via PGXS enforcing strict read-only execution when `air_ro.enabled = on` across `ExecutorStart_hook` (blocks DML, modifying CTEs, `SELECT ... FOR UPDATE/SHARE`), `ProcessUtility_hook` (blocks DDL, `COPY FROM`, `SELECT INTO`, `DO`/`CALL`, `BEGIN READ WRITE`, and attempts to disable `transaction_read_only`), and `object_access_hook` (defense-in-depth catalog write rejection). Exposes `air_ro_status()`.
- **`extensions/air_smgr/air_smgr.c` (`air_smgr.read_only`):** Rejects persistent `create`, `unlink`, `extend`, `zeroextend`, `writev`, and `truncate` calls when enabled, while routing backend-local ephemeral temp files to `md.c` (`backend != INVALID_PROC_NUMBER`).
- **`internal/readadapter/pageserver_backend.go`:** Wraps `*pageserver.Pageserver` as a read-only `StorageBackend` with `StaticSnapshotProvider`.

---

## 4. MVCC, `pg_xact` Visibility & Multi-Level Cache (Phases 4–6)

- **MVCC & `pg_xact` / `pg_subtrans` (`internal/readadapter/mvcc.go`):**
  - `CLOGResolver` decodes 2-bit `pg_xact` CLOG statuses (`32,768` XIDs/page) and `pg_subtrans` parent links (`2,048` XIDs/page) at the pinned `SnapshotInfo.LSN`.
  - `IsVisibleWithCLOG` evaluates full PG17 `HeapTupleSatisfiesMVCC` visibility (handling unhinted fresh writes, frozen tuples, lock-only `HeapXmaxLockOnly`, active XIPs, and concurrent commits past `snap.LSN`).
  - `SnapshotCoordinator` provides monotonic `Publish` and causal `AwaitLSN` synchronization.
- **L1 RAM + L2 Local NVMe + L3 Pageserver Cache (`internal/readadapter/cache.go`):**
  - `TieredCacheBackend` layers a bounded LRU L1 RAM cache and a bounded, CRC32C-checksummed L2 NVMe disk cache over L3 `Pageserver`, keyed strictly by `(TenantID, Generation, SpcOID, DbOID, RelNumber, ForkNum, BlockNum, LSN)`. Corrupt/truncated L2 files are automatically detected, evicted, and recovered from L3.
- **Commit Publication & Session Watermarks (`internal/readadapter/publisher.go`):**
  - `CommitPublisher` handles Write Engine `CommitEvent`s, invalidates modified blocks/relations in L1+L2, processes primary fencing (`OnGenerationFence`), and tracks per-session write watermarks (`RecordSessionWrite` / `AwaitSessionRead`) for read-your-own-write consistency.

---

## 5. C++20 Gateway SQL Routing & Automatic Fallback (Phase 7)

- **`gateway/src/pg/sql_router.{hpp,cpp}`:**
  - Lexical SQL classifier (`classify_sql_for_routing`) respecting `--` comments, nested `/* ... */` comments, `'...'` literals, `"..."` identifiers, `$$...$$` dollar-quoting, and multi-statement batches.
  - Routes safe read-only queries (`SELECT`, read CTEs, `EXPLAIN`, `COPY (...) TO`, `BEGIN READ ONLY`, `SHOW`, safe `SET`) to `ReadEngine`; routes DML, DDL, modifying CTEs, `FOR UPDATE/SHARE`, `SELECT INTO`, `COPY ... FROM`, mutating functions (`nextval`, `pg_advisory_lock*`), and mixed batches to `WriteEngine`.
  - `route_query` automatically falls back to `WriteEngine` on `session_pinned`, `in_transaction && tx_has_writes`, `!read_engine_healthy`, or causal lag (`read_engine_lsn < session_write_lsn`), recording `RoutingMetrics`.

---

## 6. Hardening & Benchmarks (Phases 8–10)

All Go (`go test -race -count=1 ./internal/readadapter/...`, 11 tests including live PG 17.12 `air_ro` tests, concurrent storms, generation fencing, and L2 NVMe crash truncation recovery), `go vet ./...`, and C++20 (`make -C gateway test`) suites pass.

Measured on `linux/amd64` (`AMD EPYC 7763`, `go test -bench=BenchmarkReadEngine -benchmem -benchtime=300ms`):

| Benchmark | Latency (`ns/op`) | Allocations |
| :--- | ---: | ---: |
| `BenchmarkReadEngineL1HotPage-2` | `2,159 ns/op` | `8192 B/op`, `1 allocs/op` |
| `BenchmarkReadEngineL2NVMePage-2` | `21,452 ns/op` | `9912 B/op`, `5 allocs/op` |
| `BenchmarkReadEngineL3PageserverPage-2` | `2,522 ns/op` | `8440 B/op`, `6 allocs/op` |
| `BenchmarkReadEngineMVCCWithCLOG-2` | `41.77 ns/op` | `0 B/op`, `0 allocs/op` |
| `BenchmarkReadEngineCommitPublish-2` | `89.05 ns/op` | `0 B/op`, `0 allocs/op` |
| `BenchmarkReadEngineSQLiteQuery-2` | `6,770 ns/op` | `264 B/op`, `15 allocs/op` |

---

## 7. Standalone Process Separation & Default Shared SQLite Read Engine (`air-read-engine` + `air-gateway`)

AIR separates the **Gateway** (`gateway/air-gateway`), **Read Engine** ([`cmd/air-read-engine`](file:///workspaces/air/cmd/air-read-engine/main.go)), and **Cell/Control Plane** ([`cmd/air-server`](file:///workspaces/air/cmd/air-server/main.go)) into independent processes:

- **`air-gateway` as Single Entry Point (`:5432`)**: Clients connect only to `air-gateway`. Both [`TenantPool::handle_simple_query`](file:///workspaces/air/gateway/src/pooler.cpp) and [`TenantPool::handle_extended_batch`](file:///workspaces/air/gateway/src/pooler.cpp) (`Parse`/`Bind`/`Execute`/`Sync`) classify queries via `route_query`:
  - Read-only Simple and Extended Query batches route to `read_idle_pool_` (`--read-engine-host` / `--read-engine-port` / `--read-engine-sock-dir`) with automatic fallback to the Cell (`WriteEngine`) on error or unavailability.
  - After writes commit on the Cell, the gateway captures `pg_current_wal_lsn()` into `session_write_lsn`. Subsequent reads refresh `pg_last_wal_replay_lsn()` from the Read Engine if needed and fall back to the Cell (`CausalLagFallback`) whenever `read_engine_lsn < session_write_lsn`.
- **Shared `air-read-engine` (`:8082` HTTP, `:5433` PG wire) — Default SQLite Engine (`--engine=sqlite`)**:
  - By default (`--engine=sqlite` / `AIR_READ_ENGINE_TYPE=sqlite`), `air-read-engine` runs [`readadapter.SQLiteReadServer`](file:///workspaces/air/internal/readadapter/sqlite_server.go) backed by native `libsqlite3` ([`sqlite_cgo.go`](file:///workspaces/air/internal/readadapter/sqlite_cgo.go)) instead of spawning a PostgreSQL cluster.
  - Serves all tenants over the PostgreSQL v3 wire protocol on TCP (`:5433`) and Unix domain socket (`.s.PGSQL.5433`), supporting both Simple Query (`Q`) and Extended Query (`Parse`/`Bind`/`Describe`/`Execute`/`Close`/`Flush`/`Sync`) protocols, `PREPARE`/`EXECUTE`/`DEALLOCATE`, `COPY (...) TO STDOUT`, `EXPLAIN`, and LSN/status introspection (`pg_last_wal_replay_lsn()`, `air_ro_status()`, `sqlite_version()`).
  - Enforces strict 3-layer read-only execution on every client query:
    1. `SQLITE_OPEN_READONLY` file descriptor flags + `PRAGMA query_only = ON`.
    2. C-level `sqlite3_set_authorizer` hook (`air_sqlite_ro_authorizer`) permitting only `SQLITE_SELECT`, `SQLITE_READ`, `SQLITE_FUNCTION`, read-only transactions, and safe inspection pragmas while denying all writes (`SQLITE_DENY` -> `SQLSTATE 25006`).
    3. `sqlite3_stmt_readonly(stmt)` verification and SQL guard against DML, DDL, modifying CTEs, `SELECT INTO`, `FOR UPDATE/SHARE`, and attempts to disable `transaction_read_only`.
  - Materializes committed PG17 heap tuples from `readadapter.Adapter` (L1 RAM -> L2 NVMe -> L3 Pageserver with MVCC/CLOG visibility) via `MaterializeRelation` or syncs from the primary Cell via `SyncFromPrimary` / `POST /v1/tenants/{id}/sync`, tracking each tenant's synced LSN in `_air_meta` for gateway causal consistency.
  - The legacy native PG17 `air_ro.so` read engine remains available via `--engine=postgres` (`AIR_READ_ENGINE_TYPE=postgres`).


