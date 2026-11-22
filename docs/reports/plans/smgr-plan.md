# AIR Custom PostgreSQL 17 `smgr` Extension (`air_smgr`) Plan

> **Goal:** Intercept PostgreSQL 17's storage manager (`f_smgr`) so relation reads (`8 KB` blocks) fetch pages on demand from the **AIR Pageserver** (`GetPage@LSN`) with a bounded **Local File Cache (LFC)** on `/var/lib/air`, instead of requiring the entire 10 GB+ `pgdata` locally.

---

## 1. Why a Minimal PG17 `smgr_hook` Patch Is Required

In upstream PostgreSQL 17, the storage manager vtable (`f_smgr` in `src/backend/storage/smgr/smgr.c`) is **not** runtime-pluggable via a public hook (upstream only hardcodes `smgr_md`).

Just like Neon (`pgxn/neon/pagestore_smgr.c`) and Percona Server for PostgreSQL 17 (`pg_tde`), we need:
1. A **small, clean PG17 source patch** (~40 lines) exporting `smgr_hook` in `src/backend/storage/smgr/smgr.c` and `src/include/storage/smgr.h`.
2. A **C extension (`air_smgr.so`)** loaded via `shared_preload_libraries = 'supautils,air_smgr'` that registers `smgr_hook = air_smgr_hook` in `_PG_init()`.

---

## 2. PostgreSQL 17 `f_smgr` Interface

In PostgreSQL 17, `f_smgr` includes vectorized I/O (`smgr_readv` / `smgr_writev` / `smgr_zeroextend`):

```c
typedef struct f_smgr
{
    void (*smgr_init)(void);
    void (*smgr_shutdown)(void);
    void (*smgr_open)(SMgrRelation reln);
    void (*smgr_close)(SMgrRelation reln, ForkNumber forknum);
    void (*smgr_create)(SMgrRelation reln, ForkNumber forknum, bool isRedo);
    bool (*smgr_exists)(SMgrRelation reln, ForkNumber forknum);
    void (*smgr_unlink)(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo);
    void (*smgr_extend)(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
                        const void *buffer, bool skipFsync);
    void (*smgr_zeroextend)(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
                            int nblocks, bool skipFsync);
    bool (*smgr_prefetch)(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
                          int nblocks);
    void (*smgr_readv)(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
                       void **buffers, BlockNumber nblocks);
    void (*smgr_writev)(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
                        const void **buffers, BlockNumber nblocks, bool skipFsync);
    void (*smgr_writeback)(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
                           BlockNumber nblocks);
    BlockNumber (*smgr_nblocks)(SMgrRelation reln, ForkNumber forknum);
    void (*smgr_truncate)(SMgrRelation reln, ForkNumber forknum, BlockNumber old_blocks,
                          BlockNumber nblocks);
    void (*smgr_immedsync)(SMgrRelation reln, ForkNumber forknum);
    void (*smgr_registersync)(SMgrRelation reln, ForkNumber forknum);
} f_smgr;
```

### Routing Rule (`air_smgr_hook`)
- **Temporary / unlogged-local relations** (`RelFileLocatorBackendIsTemp(rlocator)`): delegate to standard `smgr_standard` (`md.c`) so temp tables and sort files stay fast and local.
- **Persistent relations** (`MAIN_FORKNUM`, `FSM_FORKNUM`, `VISIBILITYMAP_FORKNUM`, `INIT_FORKNUM`): return `&air_smgr`.

---

## 3. Core Mechanics (Adapted from Neon's `pagestore_smgr.c`)

### 3.1 Read Path (`smgr_readv`, `smgr_nblocks`, `smgr_exists`, `smgr_prefetch`)
1. **Determine Request LSN:**
   - Use current WAL insert/flush LSN (`GetXLogInsertRecPtr()` / `GetFlushRecPtr()`) as the read LSN, tracking `not_modified_since` per relation/block where possible so the Pageserver can serve from cache when nothing changed.
2. **Tiered Read Lookup:**
   - **Tier 1 (RAM):** PostgreSQL `shared_buffers` (handled by PG buffer manager before calling `smgr_readv`).
   - **Tier 2 (Local File Cache - LFC):** Bounded sparse/chunked cache file on `/var/lib/air/lfc/<tenant_id>` (e.g. `256 MB`–`512 MB` LRU). On LFC hit, `pread()` 8 KB directly without IPC.
   - **Tier 3 (AIR Pageserver):** On LFC miss, send vectorized `GET_PAGE_AT_LSN` batch over Unix socket (`/var/lib/air/pageserver.sock`) to the Go Pageserver, populate LFC, and fill `buffers[0..nblocks-1]`.
3. **PG17 Vectorized Reads (`smgr_readv` & Read Streams):**
   - Batch contiguous `nblocks` (e.g. sequential scans, `read_stream`) into a single multi-block request to minimize Unix-socket round-trips.

### 3.2 Write Path (WAL-Only Durability)
- **`smgr_writev` / `smgr_extend` / `smgr_zeroextend`:**
  - PostgreSQL already writes WAL records for all logged relation changes.
  - Ensure **Full-Page Images (FPI)** or `log_newpage_buffer()` are emitted for new/unlogged-to-WAL structures (including FSM/VM pages, avoiding stale FSM reads by tracking `XactLastRecEnd` cleanly).
  - Writes update the **Local File Cache (LFC)** for immediate local re-reads, while **durability is guaranteed by the WAL stream** to the AIR Pageserver — no local `fsync` of 10 GB data files required!
- **`smgr_immedsync` / `smgr_registersync`:**
  - Flush/wait for WAL replication up to the dirty LSN (`WaitForWALFlushToPageserver`) rather than syncing local `base/` files.

### 3.3 `pg_basebackup` / Bootstrap Compatibility
- Only tiny cluster control/catalog bootstrap files (`global/pg_control`, `pg_wal/`, `postgresql.conf`, `pg_hba.conf`, empty `base/` stubs) live on `/var/lib/air`.
- Relation data blocks for a 10 GB table live in R2 via the Pageserver and are **never materialized in full on `/var/lib/air`**.

---

## 4. Build & Packaging in `Dockerfile`

1. Add `patches/pg17-smgr-hook.patch` to the repository.
2. In `Dockerfile` (`pg-builder` stage):
   - Fetch PostgreSQL 17.x source tarball, apply `pg17-smgr-hook.patch`, compile & install PG17 (`--prefix=/usr/lib/postgresql/17`).
   - Build `supautils` and `extensions/air_smgr` against the patched PG17 headers.
3. In `internal/cell/agent.go`:
   - Add `air_smgr` to `shared_preload_libraries = 'supautils,air_smgr'` and configure `air_smgr.pageserver_socket`, `air_smgr.tenant_id`, and `air_smgr.lfc_max_bytes` per tenant in `postgresql.conf`.

---

## 5. Phased Implementation Plan

| Phase | Deliverable | Scope & Verification |
|---|---|---|
| **Phase 1: PG17 `smgr_hook` Patch** | `patches/pg17-smgr-hook.patch`, `Dockerfile` builder update | Compile patched PG17 + `supautils`; verify all existing `air` tests pass with `smgr_hook = NULL` (fallback to `md.c`). |
| **Phase 2: Minimal `air_smgr` Extension** | `extensions/air_smgr/` (`air_smgr.c`, `communicator.c`, `Makefile`) | Implement `f_smgr` vtable talking `GetPage@LSN`, `GetRelSize`, `GetRelExists` to AIR Pageserver over Unix socket. |
| **Phase 3: Local File Cache (LFC) & `smgr_readv`** | `extensions/air_smgr/lfc.c` | Bounded LRU cache on `/var/lib/air/lfc/`; vectorized `smgr_readv` batching + `smgr_prefetch`. |
| **Phase 4: 10 GB Sparse-Read Benchmark** | Fly.io `1 GB` volume test | Load 10 GB dataset via Pageserver → R2, suspend/evict LFC, wake VM, run point-lookup `SELECT` on 1 GB volume and verify <50 MB fetched and sub-2s wake. |
