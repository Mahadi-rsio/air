# AIR Phase 21.10 — Initial Repository Audit

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Branch:** `main`  
**HEAD Commit SHA:** `803c0fd9f0cab7c2c5b0b85c3898f901fd264d4f` (`feat(phase21): wire Pageserver/air_smgr integration, decouple CPU PSI, and add Phase 21 validation reports`)  
**Status:** Audit Complete — Root Causes Verified Against Source Code

---

## 1. Confirmed Issues & Verified Root Causes

### Issue A: `air_zeroextend()` Page Persistence & Missing-Page Zero Substitution (`SQLSTATE XX002`)
- **Reported Symptom:** `ERROR: index "pg_proc_oid_index" contains unexpected zero page at block 4 (SQLSTATE XX002)` on Pageserver restart, and `ERROR: index "pg_proc_proname_args_nsp_index" contains unexpected zero page at block 27 (SQLSTATE XX002)` on network interruption.
- **Verified Root Causes:**
  1. **`extensions/air_smgr/air_smgr.c` (`air_zeroextend`, lines 184–216):** Inserts zero-filled blocks into LFC (`air_lfc_insert`) and updates `air_relmeta_extend`, **but never calls `air_ps_put_pages`**. Zero-extended blocks exist only in local LFC; when LFC evicts them or Pageserver restarts, Pageserver has no record of those blocks.
  2. **`extensions/air_smgr/air_smgr.c` (`air_extend` lines 160–180, `air_writev` lines 316–341):** Both update `air_lfc_insert` and `air_relmeta_extend` *before* `air_ps_put_pages` succeeds. If `air_ps_put_pages` fails (e.g., network interruption), PostgreSQL aborts the statement, but LFC and shared-memory `relmeta` already contain unpersisted block state.
  3. **`extensions/air_smgr/communicator.c` (`air_ps_get_pages`, lines 330–334):** When Pageserver returns `AIR_PS_STATUS_NOT_FOUND`, `air_ps_get_pages` **silently `memset`s all requested buffers to `0` and returns `true`**. `air_readv` then caches those silent zero pages in LFC. Missing/unpersisted pages are thus silently disguised as valid zero pages, triggering `XX002` in PostgreSQL indexes.
  4. **`extensions/air_smgr/air_smgr.c` (`air_truncate`, lines 392–400):** Truncates LFC and `relmeta` locally without notifying Pageserver, leaving stale blocks/sizes in Pageserver if `relmeta` is lost on PG restart.
  5. **`extensions/air_smgr/lfc.c` (`air_relmeta_lookup` / `AIR_RELMETA_CAP`):** Unlinked entries remain as `status = 2` tombstones forever; `air_lfc_lookup` checks `slot.lsn >= min_lsn` where `min_lsn = GetXLogInsertRecPtr()`, causing frequent LFC misses whenever any unrelated WAL is written.

### Issue B: Read Engine DDL Visibility (`SQLSTATE 42000: no such table`)
- **Reported Symptom:** 177 errors (`ERROR: no such table: soak_kv`, `SQLSTATE 42000`) during the 5-minute soak test with Read Engine enabled.
- **Verified Root Causes:**
  1. **`internal/gateway/pooler.go` (lines 835–870, 972–1038) & `internal/gateway/sql_router.go` (`RouteQuery`, lines 418–436):** `sessionWriteLSN` is tracked strictly per client session (`clientSessionState`). Brand-new connections (and idle connections that have not executed writes) have `sessionWriteLSN == 0`.
  2. **`RouteQuery` (`sql_router.go:430`):** Checks `else if ctx.SessionWriteLSN > 0 && ctx.ReadEngineLSN < ctx.SessionWriteLSN`. When `SessionWriteLSN == 0`, this check is bypassed completely, routing `SELECT` queries to `air-read-engine` even if `TenantPool` recently executed DDL (`CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, `CREATE INDEX`) that has not yet been ingested and reflected in `air-read-engine`'s catalog.
  3. **`internal/gateway/pooler.go` (`tryExecuteOnReadEngine`, lines 604–730):** Does not track pool-wide `latestDDLLSN` / schema epoch or verify Read Engine schema readiness before routing.

### Issue C: Pageserver Persistence & Restart Recovery
- **Reported Symptom:** Pageserver restart during active `air_smgr` operation failed with `XX002`; `air-pageserver` shutdown order flushed after `GracefulStop`.
- **Verified Root Causes:**
  1. **`cmd/air-pageserver/main.go` (lines 357–364):** Calls `grpcSrv.GracefulStop()` before `ps.FlushAll(shutCtx)`. If an active gRPC connection/stream delays `GracefulStop()` beyond Docker's `stop_grace_period` (10s), `SIGKILL` terminates Pageserver before `FlushAll` runs.
  2. **`internal/pageserver/server.go` (`New`, lines 48–83):** Does not scan `LayerDir` on startup to reload existing tenant timelines and their local `layer_manifest.json` files. After a Pageserver restart, `p.tenants` is empty; incoming `air_smgr` requests via `UnixServer` or `GRPCPageserverClient` hit `p.getTimeline(tenantID) -> ErrNotFound` unless `EnsureTenant(..., loadFromR2=true)` is explicitly called again by `air-server`.
  3. **`internal/pageserver/readpath.go` (`PutPageBatch`, lines 940–996):** Writes pages into `InMemoryLayer` only; if Pageserver crashes (`SIGKILL`) before `FlushInMemory` runs, pages written via `PutPageBatch` that were not yet flushed to an L0 layer rely on WAL replay (`IngestWALRecords`) or are lost if not backed by WAL.

### Issue D: Resource Benchmark Measurement Accuracy (`pg_rss_mb = 0.00`)
- **Reported Symptom:** All Phase 21 CSVs in `benchmarks/phase21/results/` recorded `pg_rss_mb = 0.00`.
- **Verified Root Causes:**
  1. **`benchmarks/phase21/helpers.go` (`sampleContainerCgroup`, line 170):** Executes `ps -eo comm,rss --no-headers | awk '/postgres/{sum+=$2} END{printf "%.2f\n", sum/1024}'` inside `air-server`. Minimal Debian runtime images lack `procps` (or `ps` fails), returning empty output so `parseF(8)` silently defaults to `0.00`.
  2. **`benchmarks/phase21/helpers.go` (lines 209–216):** `GatewayRSSMB` and `PageserverMB` read `/sys/fs/cgroup/memory.current` (total cgroup memory including page cache) rather than actual process RSS, and Read Engine RSS is not collected at all.
  3. Missing metrics are silently reported as `0` instead of unavailable (`NA` / `-1`).

### Issue E: Multi-Tenant Contention & Soak Coverage
- **Verified Gaps:**
  - Phase 21 multi-tenant test used 15 workers (`8+2+4+1`) against `WriteMaxGlobal=16`, never exceeding the global limit or exercising real DRR queue contention.
  - Phase 21 15m/30m soak tests did not complete (`571s` max), and the `571s` run disabled `air-read-engine` (`AIR_READ_ENGINE_HOST=""`).

### Issue F: Deployment Configuration Gaps
- **Verified Gaps in `deploy/fly/fly.server.toml`:**
  - Missing `AIR_PAGESERVER_ENABLED = "true"` and `AIR_PAGESERVER_SOCKET = "/var/lib/air/pageserver.sock"`.
  - `fly.server.toml`, `fly.pageserver.toml`, and `fly.read-engine.toml` lack Fly health check blocks.
  - Volume sizes in `fly.server.toml` (`2gb`) and `fly.read-engine.toml` (`1gb`) differ from the target spec (`1GB` Server, `2GB` Read Server).

---

## 2. Relevant Source Files & Functions

| Area | File | Key Functions |
|---|---|---|
| `air_smgr` hooks | `extensions/air_smgr/air_smgr.c` | `air_create`, `air_exists`, `air_unlink`, `air_extend`, `air_zeroextend`, `air_readv`, `air_writev`, `air_nblocks`, `air_truncate` |
| `air_smgr` RPC client | `extensions/air_smgr/communicator.c` | `do_rpc`, `air_ps_get_pages`, `air_ps_put_pages`, `air_ps_get_rel_size`, `air_ps_get_rel_exists`, `air_ps_unlink_rel` |
| LFC & relmeta | `extensions/air_smgr/lfc.c` | `air_lfc_lookup`, `air_lfc_insert`, `air_lfc_invalidate_rel`, `air_relmeta_*` |
| Pageserver core | `internal/pageserver/server.go`, `readpath.go`, `layers.go`, `grpc.go` | `New`, `EnsureTenant`, `PutPageBatch`, `GetPageAtLSN`, `GetPageBatch`, `FlushInMemory`, `FlushAll`, `LoadLayerManifestGen` |
| Pageserver binary | `cmd/air-pageserver/main.go` | `main` (shutdown sequence) |
| Gateway routing | `internal/gateway/pooler.go`, `sql_router.go` | `RouteQuery`, `tryExecuteOnReadEngine`, `handleSimpleQuery`, `handleExtendedBatch` |
| Read Engine | `internal/readadapter/sqlite_server.go`, `cache.go` | `refreshNativeCatalogWithSnap`, `refreshNativeCatalogOnSlot`, `CatalogLSN` |
| Benchmark collector | `benchmarks/phase21/helpers.go`, `main.go` | `sampleContainerCgroup`, `runSoak`, `runMultiTenant` |
| Fly configs | `deploy/fly/fly.*.toml` | `fly.server.toml`, `fly.gateway.toml`, `fly.pageserver.toml`, `fly.read-engine.toml` |

---

## 3. Existing Tests

- `extensions/air_smgr/test_communicator.c` — C unit test for `communicator.c`.
- `cmd/air-server/pageserver_test.go` — `configurePageserver` failure modes & remote bridge wiring.
- `internal/control/pageserver_grpc_test.go`, `pageserver_integration_test.go` — gRPC Pageserver client & lifecycle tests.
- `internal/pageserver/pageserver_test.go`, `phase8_regression_test.go`..`phase19_p2_walredo_test.go` — Pageserver unit/layer/WAL-redo tests.
- `internal/gateway/sql_router_phase15_test.go`, `write_governor_test.go`, `write_governor_integration_test.go`, `sqlite_read_integration_test.go` — Gateway routing & Read Engine integration tests.
- `benchmarks/phase21/main.go` — Phase 21 Docker end-to-end validation harness.

---

## 4. Proposed Fixes

1. **Fix A (`air_smgr`):**
   - In `air_zeroextend`: send zero-filled blocks to Pageserver via `air_ps_put_pages` in batches (up to 128 blocks) **before** updating `air_relmeta_extend` and LFC; abort with `ERROR (ERRCODE_IO_ERROR)` if Pageserver persistence fails.
   - In `air_extend` & `air_writev`: persist to Pageserver (`air_ps_put_pages`) **before** updating `air_relmeta_extend` and LFC so failed RPCs cannot leave phantom blocks in LFC/`relmeta`.
   - In `air_ps_get_pages` (`communicator.c`): **remove** silent `memset(..., 0)` on `AIR_PS_STATUS_NOT_FOUND`; return `false` with an explicit `"page not found in pageserver"` error so `air_readv` raises `ERRCODE_IO_ERROR` instead of returning fabricated zero pages.
   - In `air_truncate`: also notify Pageserver (or record truncation) so Pageserver relation size does not remain stale after PG restart.
   - Add a `TestZeroExtendAndNotFound` test in `extensions/air_smgr/` and real PG17 integration tests.
2. **Fix B (Read Engine DDL Visibility):**
   - Track pool-wide `latestDDLLSN` (and `ddlDirty` / `schemaEpoch`) on `TenantPool` whenever any session executes DDL (`RouteReasonWriteDDL`) or a write that may alter catalog.
   - In `RouteQuery` / `pooler.go`: if `ddlDirty` is set or `ReadEngineLSN < max(sessionWriteLSN, poolLatestDDLLSN)` or Read Engine's synced LSN/catalog readiness cannot be confirmed, route to primary PostgreSQL (`RouteTargetWriteEngine`).
   - Do not let `sessionWriteLSN == 0` bypass pool-level DDL visibility checks.
3. **Fix C (Pageserver Persistence & Recovery):**
   - On `pageserver.New()` startup, scan `LayerDir` for existing `<tenant>/<gen>/timeline_<epoch>/layer_manifest.json` files and reload timelines/layers automatically so Pageserver restarts recover all persisted tenants without requiring `EnsureTenant` first.
   - In `cmd/air-pageserver/main.go`: call `ps.FlushAll(shutCtx)` **before** (or concurrently with bounded timeout before) `grpcSrv.GracefulStop()`, and close Unix socket / stop accepting new writes first so `SIGKILL` cannot preempt flush.
   - Ensure `PutPageBatch` / `FlushInMemory` / `WriteLayer` / `PublishLayerManifestGen` `fsync` layer files and manifest files to disk.
4. **Fix D (Resource Metrics):**
   - Replace `ps -eo comm,rss` with direct `/proc/<pid>/status` (`VmRSS:`) / `/proc/<pid>/comm` / `postmaster.pid` parsing inside containers (zero external tool dependencies).
   - Separate process RSS (`pg_rss_mb`, `gateway_rss_mb`, `pageserver_rss_mb`, `read_engine_rss_mb`) from cgroup `memory.current` / `memory.peak` / `memory.events` / PSI.
   - Report unavailable metrics as `NA` (or `-1.00` sentinel with `avail=false`), never silently `0.00`.
   - Add a self-test verifying `/proc` & cgroup parsing.

---

## 5. Risks & Dependencies

- **PG17 Build Time:** Rebuilding `air-server` Docker image with `extensions/air_smgr` changes is fast if Docker layer caching preserves `pg-builder` (Stage 2), or ~3–5 minutes on 2 vCPUs if Stage 2 rebuilds.
- **Read Engine Sync Latency:** Checking Read Engine LSN on DDL barrier must be lightweight (cached/piggybacked or only triggered when `ddlDirty` / `rlsn < latestDDLLSN`) to avoid per-query RPC overhead.
- **External Integrations:** R2 and Neon DB credentials are not present in this sandbox; their validation must be honestly marked `NOT TESTED` with reproducible instructions.
