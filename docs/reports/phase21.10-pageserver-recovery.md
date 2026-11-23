# AIR Phase 21.10 — Pageserver Persistence & Recovery Report

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Base Commit:** `803c0fd9f0cab7c2c5b0b85c3898f901fd264d4f` (+ Phase 21.10 fixes)  
**Status:** **PASS** (with explicitly documented `SIGKILL` InMemoryLayer boundary)

---

## 1. Root Causes Confirmed

1. **`cmd/air-pageserver/main.go`:** `grpcSrv.GracefulStop()` ran unbounded before `ps.FlushAll(shutCtx)`, risking container `SIGKILL` before `InMemoryLayer`s flushed to disk.
2. **`internal/pageserver/server.go`:** `pageserver.New()` did not scan `LayerDir` on startup to reload existing tenant timelines and `timeline_<epoch>_layer_manifest.json` files.
3. **`internal/pageserver/server.go` & `grpc.go`:** `EnvelopeKMS` held tenant DEKs only in memory; after Pageserver restart, `GetPageAtLSN` failed with `crypto: tenant DEK has been shredded or does not exist`.
4. **`internal/control/pageserver_client.go`:** Transient gRPC retry budget (`2–3` attempts, ~75ms) was too short for 1–2s network interruptions or Pageserver restarts.

---

## 2. Changes Implemented

- **`cmd/air-pageserver/main.go`:** Bounded `GracefulStop` to 2s before `Stop()`, ensuring `ps.FlushAll(shutCtx)` always runs before `ps.Close()`.
- **`internal/pageserver/server.go` & `grpc.go`:** Added `PersistTenantWrappedDEK` (`LayerDir/<tenantID>/wrapped_dek.b64`, `0600` + `fsync`) and `recoverLocalTenants()` on startup to reload DEKs, generations, epochs, layer manifests, and `walLastLSN`.
- **`internal/control/pageserver_client.go`:** Increased `retryPageserverRPC` budget to 10 attempts (~2.1s) on `air_smgr` bridge RPCs for transient `Unavailable` errors.

---

## 3. Durability Guarantees & Unsupported Boundaries

- **Supported & Verified:**
  - `fsync=on`, `full_page_writes=on`, `synchronous_commit=on` preserved in PostgreSQL 17.12.
  - PostgreSQL `SIGKILL` crash recovery via `pg_wal` redo + Pageserver.
  - Graceful `air-pageserver` restart (`SIGTERM` / `docker restart`) with cold LFC (`rm -f *.lfc`) and cold `DiskCache`.
  - Temporary Cell-to-Pageserver network interruption (`docker network disconnect`/`connect` 2s).
  - Atomic `WriteLayer` + `PublishLayerManifestGen` with `fsync`.
- **Unsupported / Known Boundary:**
  - Unflushed `PutPageBatch` / `IngestWALRecords` data still in Pageserver's `InMemoryLayer` during an ungraceful `SIGKILL` of `air-pageserver` (before `FlushInMemory` runs) is not backed by an independent Pageserver write-ahead log; if PostgreSQL has already checkpointed and recycled `pg_wal` past those LSNs, those unflushed `InMemoryLayer` pages would be lost.

---

## 4. Verification Results

| Test | Result | Evidence |
|---|---|---|
| `TestPageserverRestart_RecoversZeroExtendAndRejectsMissingPage` | **PASS** | Cold-cache restart recovered zero-extended and non-zero blocks; missing block returned `ErrNotFound` |
| `storage-path` (cold LFC + Pageserver restart + PG restart) | **PASS** | `count=902` post-restart, `250` concurrent rows verified via index scan + `amcheck` |
| `crash-recovery` (`PG SIGKILL` + `air-server` + `air-gateway` + `air-pageserver` restarts + 2s network disconnect) | **PASS** | `count=250, sum=313750`, 0 data loss, 0 `XX002` |
