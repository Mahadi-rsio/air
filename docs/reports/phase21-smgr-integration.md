# AIR Phase 21.1 — PostgreSQL 17.12 Storage Hook (`air_smgr`) Integration Report

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Base Commit:** `85aa02e` + Phase 21.2 `cmd/air-server/main.go` & `internal/control/pageserver_client.go` wiring  
**PostgreSQL Version:** `PostgreSQL 17.12` (compiled from source with `patches/pg17-smgr-hook.patch`)  
**Container Profile:** `air-server:phase21` (`512 MB` RAM limit, `2` vCPU)

---

## 1. Reproducible Build Verification

Built `air-server:phase21` from `Dockerfile` (Stage 2 `pg-builder` + Stage 3 runtime):
- Downloaded and extracted `postgresql-17.12.tar.bz2`.
- Applied `patches/pg17-smgr-hook.patch` cleanly (`src/include/storage/smgr.h`, `src/backend/storage/smgr/smgr.c`, `src/backend/utils/adt/dbsize.c`).
- Configured and built PostgreSQL 17.12 (`./configure --prefix=/usr/lib/postgresql/17 --with-openssl --with-libxml --with-uuid=ossp && make -j2 world-bin && make install-world-bin`).
- Built and installed `supautils.so` and `extensions/air_smgr` (`air_smgr.so`, `air_smgr.control`, `air_smgr--1.0.sql`) into `/usr/lib/postgresql/17/lib/`.

---

## 2. Live `air_smgr` Hook Execution & Storage Path Proof

Executed inside `docker run --rm --memory=512m --cpus=2 air-server:phase21` with `AIR_PAGESERVER_ENABLED=true`, `AIR_PAGESERVER_SOCKET=/var/lib/air/pageserver.sock`, and `AIR_PAGESERVER_CACHE_MB=128`:

### 2.1 Configuration & GUC Verification
`cell.Agent.WriteTenantConfigs` generated `/var/lib/air/tenants/active/t_smgr_p21/pgdata/postgresql.conf` with:
- `shared_preload_libraries = 'supautils,air_smgr'`
- `air_smgr.enabled = on`
- `air_smgr.pageserver_socket = '/var/lib/air/pageserver.sock'`
- `air_smgr.tenant_id = 't_smgr_p21'`
- `air_smgr.lfc_dir = '/var/lib/air/lfc/t_smgr_p21'`
- `air_smgr.lfc_max_mb = 128`

Verified live via `SHOW` as `postgres` superuser:
- `shared_preload_libraries` = `supautils,air_smgr`
- `air_smgr.enabled` = `on`
- `air_smgr.pageserver_socket` = `/var/lib/air/pageserver.sock`
- `air_smgr.tenant_id` = `t_smgr_p21`
- `air_smgr.lfc_dir` = `/var/lib/air/lfc/t_smgr_p21`
- `air_smgr.lfc_max_mb` = `128MB`

### 2.2 DDL/DML & Non-Local Storage Proof
Executed as `app_user` on database `t_smgr_p21`:
- `CREATE TABLE smgr_test (id int PRIMARY KEY, val text);`
- `INSERT INTO smgr_test SELECT g, repeat('x', 200) FROM generate_series(1, 500) g;` -> `INSERT 0 500`
- `UPDATE smgr_test SET val = repeat('y', 200) WHERE id <= 100;` -> `UPDATE 100`
- `DELETE FROM smgr_test WHERE id > 400;` -> `DELETE 100`
- `SELECT count(*), sum(id), pg_relation_size('smgr_test'), pg_database_size(current_database())` -> `count = 400`, `sum = 80200`, `rel_bytes = 147456`, `db_bytes = 7747251`.

**Irrefutable proof of `air_smgr` hook execution:**
- Relation `smgr_test` mapped to `base/16385/16386`.
- Local file `/var/lib/air/tenants/active/t_smgr_p21/pgdata/base/16385/16386` **does not exist on disk** (`air_create`, `air_extend`, `air_writev`, `air_readv`, `air_nblocks` intercepted all persistent relation I/O).
- Local File Cache `/var/lib/air/lfc/t_smgr_p21/t_smgr_p21.lfc` (`11M`) and Pageserver layer directory `/var/lib/air/ps_layers/t_smgr_p21/gen_e03549555bd45b90` were populated.
- `pg_relation_size('smgr_test')` (`147456` bytes) and `pg_database_size()` (`7747251` bytes) were served via `smgrnblocks` and `smgr_dbsize_hook`.

### 2.3 Restart & WAL Crash Recovery Proof
1. **Clean Restart (`POST /internal/v1/databases/t_smgr_p21/suspend` -> `POST /internal/v1/tenants/t_smgr_p21/wake`):**
   - Tenant suspended and woken on port `18201`; `SELECT count(*), sum(id)` returned `400 | 80200`.
2. **`SIGKILL` Crash & WAL Recovery:**
   - Committed `INSERT INTO smgr_test VALUES (9999, 'after_restart_committed')`.
   - Sent `kill -9` to `postmaster`, removed stale `postmaster.pid`, and started PG17.
   - PG17 performed automatic crash recovery (`redo starts at 0/20000A0` -> `redo done at 0/2028C88` -> `checkpoint complete`).
   - `SELECT count(*), sum(id)` returned `401 | 90199` and row `9999` was intact.

---

## 3. Finding: Direct `pg_ctl -m fast` While WALReceiver Is Active

During initial testing, running `pg_ctl -m fast restart` directly on `$PGDATA` while `air-server`'s `WALReceiver` replication connection was active timed out waiting for shutdown. Using Control Plane's `suspend` (which stops `WALReceiver` before stopping PG17) followed by `wake` succeeded cleanly. This is expected lifecycle behavior when `WALReceiver` holds a physical/logical replication slot, and is documented for Phase 21.4 crash/restart testing.

---

## 4. Status

**Phase 21.1 Verdict: PASSED (Real Measurements).**
PostgreSQL 17.12 with `pg17-smgr-hook.patch` and `air_smgr.so` builds cleanly, loads via `shared_preload_libraries`, executes storage-manager and dbsize hooks for all DDL/DML, stores persistent relations in LFC + Pageserver rather than local `base/`, and survives clean restarts and `SIGKILL` WAL recovery.
