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
