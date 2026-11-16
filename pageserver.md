# AIR Custom Pageserver Architecture & Implementation Plan

> **Goal:** Replace full-tarball hydration (`HydrateTenantPGDataFromR2`) with a Neon-style **Go Pageserver** backed by Cloudflare R2 (`air-pg-backups`) so a 10 GB+ tenant database can wake in ~1 second and serve queries by fetching **only the 8 KB pages actually read**.

---

## 1. Core Design (Adapted from Neon's Pageserver)

Neon's Pageserver models all PostgreSQL storage in a 2D coordinate space of **(Key, LSN)**:
- **Key (`24 bytes`):** Identifies an 8 KB block or metadata entry:
  - `RelBlock`: `(TablespaceOID, DatabaseOID, RelNumber, ForkNum, BlockNum)`
  - `RelSize`: `(TablespaceOID, DatabaseOID, RelNumber, ForkNum)`
  - `DbSize`: `(TablespaceOID, DatabaseOID)`
  - `SlruSegment`: `(SlruKind, SegNo, BlockNum)` (`pg_xact`, `pg_multixact`, `pg_subtrans`)
  - `AuxFiles`: `pg_twophase`, `pg_logical`, `pg_replslot`
- **LSN (`uint64`):** Exact PostgreSQL Write-Ahead Log position (`XLogRecPtr`).

Every read from compute (`air_smgr`) is **`GetPage(Key, LSN)`** — asking for the state of an 8 KB page as of a specific LSN.

---

## 2. Architecture Components in AIR (`internal/pageserver/`)

```text
  PG17 Compute (air_smgr)
     │  ▲
     │  │ GetPage@LSN / GetRelSize / GetRelExists (Unix socket / TCP)
     ▼  │
  ┌──────────────────────────────────────────────────────────┐
  │                 AIR Pageserver (Go)                      │
  │                                                          │
  │  1. WAL Receiver  ──▶ WAL Parser ──▶ In-Memory Layer     │
  │     (physical repl)                     │ flush          │
  │                                         ▼                │
  │  2. LayerMap Index ◀── L0 Delta ──▶ Compactor ──▶ L1/Img │
  │         │                                         │      │
  │         ▼                                         ▼      │
  │  3. WAL-Redo Worker (postgres --wal-redo)     R2 Upload  │
  └──────────────────────────────────────────────┬───────────┘
                                                 ▼
                                   Cloudflare R2 (air-pg-backups)
                                   tenants/<id>/layers/<key_range>_<lsn_range>
```

### 2.1 WAL Ingestion Pipeline (`walreceiver.go` + `walparser.go`)
- **Stream Source:** Connect to the active tenant PostgreSQL 17 instance via physical replication (`START_REPLICATION` using `pgx/pgconn` or `pg_receivewal` framing) and/or a local WAL-receiver endpoint so **every committed WAL record** is ingested in real time before eviction.
- **WAL Parsing:** Decode PostgreSQL 17 WAL record headers (`XLogRecord`) and block references (`BkpBlockHeader` — `RelFileLocator`, `ForkNumber`, `BlockNumber`, `FPI` flag).
- **In-Memory Layer:**
  - Ephemeral map `Key -> []WALDelta` (plus full-page images when `BKPIMAGE_IS_COMPRESSED` / FPI is present) indexed by LSN.
  - Bounded by `checkpoint_distance` (e.g., `64 MB`) or `checkpoint_timeout` (e.g., `60s`), or flushed immediately on `SuspendTenant`.

### 2.2 Immutable Layer Files (`layers.go`)
1. **L0 Delta Layers:**
   - Flushed directly from the In-Memory Layer.
   - Span the **full key space** for a narrow `[start_lsn, end_lsn)` window.
   - Format: sorted `(Key, LSN) -> RecordPayload` with a footer B-tree/binary index so `GetPage` reads only the index + matching slice (using HTTP `Range` requests when reading from R2!).
2. **L1 Delta Layers (Compacted):**
   - Background compactor merges multiple overlapping L0 layers into **key-partitioned** L1 layers (`[start_key, end_key)` × `[start_lsn, end_lsn)`).
   - Prevents read amplification across hundreds of L0 files.
3. **Image Layers (Materialized Snapshots):**
   - Full 8 KB page snapshots for `[start_key, end_key)` at a specific `LSN`.
   - Acts as the base checkpoint so `GetPage@LSN` never has to replay WAL from `0/0`.
4. **Encryption & R2 Persistence:**
   - Each immutable layer file is encrypted with AIR's existing `EnvelopeKMS` (`internal/crypto`) and uploaded to R2 under `tenants/<tenant_id>/timeline_<epoch>/layers/...`.
   - Local `/var/lib/air/ps_cache/` acts as a **bounded LRU cache** of hot layer files/pages (e.g. `256 MB`–`512 MB` cap on the 1 GB Fly volume).

### 2.3 `GetPage@LSN` Read Path & WAL-Redo (`readpath.go` + `walredo.go`)
When `air_smgr` requests `GetPage(tenantID, key, lsn)`:
1. **In-Memory Layer Check:** Return immediately if an FPI or cached page exists at `<= lsn`.
2. **LayerMap Lookup:** Find the newest **Image Layer** (or FPI in a Delta Layer) at `<= lsn`, plus all **Delta Layer** WAL records between `base_lsn` and `lsn` for `key`.
3. **WAL-Redo Process:**
   - If delta records need to be applied on top of a base 8 KB page, pipe `(base_page, []wal_record)` to a lightweight stateless PostgreSQL WAL-redo worker process (or `postgres` binary running `rm_redo` in a sandboxed loop, just like Neon's `walredo`), returning the exact 8 KB page at `lsn`.
4. **R2 On-Demand Range Fetch:**
   - If a needed layer file is not in `/var/lib/air/ps_cache/`, fetch its footer index + exact byte range from R2 via `GetObject` with `Range: bytes=...` — **never downloading the whole 10 GB database**.

### 2.4 Wire Protocol (`protocol.go`)
Fast length-prefixed binary protocol over Unix domain socket (`/var/lib/air/pageserver.sock`) for co-located tenants on the same Fly VM:
- `GET_PAGE_AT_LSN (tenant_id, rlocator, fork, blkno, lsn) -> 8192 bytes`
- `GET_REL_SIZE (tenant_id, rlocator, fork, lsn) -> uint32 nblocks`
- `GET_REL_EXISTS (tenant_id, rlocator, fork, lsn) -> bool`
- `GET_DB_SIZE (tenant_id, db_oid, lsn) -> int64 bytes`
- `GET_SLRU_SEGMENT (tenant_id, kind, segno, lsn) -> bytes`

### 2.5 Fencing, Branching & Garbage Collection
- **Fencing:** Integrates with `internal/fencing/r2_cas.go` (`FencingEpoch`) so a stale cell cannot write or compact layers after a failover.
- **Garbage Collection:** Extends `GarbageCollectBackupDAG` in `internal/backup/pitr_dag.go` to sweep layer files whose `end_lsn` is below the tenant's PITR retention horizon and superseded by a newer Image Layer.

---

## 3. Phased Implementation Plan

| Phase | Deliverable | Scope & Verification |
|---|---|---|
| **Phase 1: Protocol & WAL Receiver** | `internal/pageserver/protocol.go`, `walreceiver.go`, `walparser.go` | Decode PG17 WAL records + block refs into an In-Memory Layer; unit tests with real PG17 WAL samples. |
| **Phase 2: Layer Files & WAL-Redo** | `internal/pageserver/layers.go`, `walredo.go`, `readpath.go` | Flush L0 layers encrypted to R2; implement `GetPage@LSN` with stateless PG17 WAL-redo worker; verify page-exact parity vs `pg_checksums` / `psql`. |
| **Phase 3: Compaction, Image Layers & LRU Cache** | `internal/pageserver/compactor.go`, `cache.go` | L0 → L1 + Image Layer compaction; HTTP `Range` reads from R2; bounded LRU disk cache on `/var/lib/air`. |
| **Phase 4: Control Plane & Scale-to-Zero Integration** | `internal/control/control.go`, `cmd/air-server/main.go` | Start Pageserver inside `air-server`; flush In-Memory Layer to R2 on `SuspendTenant`; remove full-tarball `HydrateTenantPGDataFromR2` on wake. |
