# AIR Phase 21.10 — `air_zeroextend()` Page Persistence & Missing-Page Fix Report

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Base Commit:** `803c0fd9f0cab7c2c5b0b85c3898f901fd264d4f` (+ Phase 21.10 fixes)  
**PostgreSQL Version:** `17.12` (patched with `patches/pg17-smgr-hook.patch` + `extensions/air_smgr`)  
**Status:** **PASS**

---

## 1. Root Cause Confirmed

1. `extensions/air_smgr/air_smgr.c` (`air_zeroextend`) inserted zero-filled blocks into LFC (`air_lfc_insert`) and updated `air_relmeta_extend`, **without calling `air_ps_put_pages`**. Zero-extended index/heap blocks existed only in local LFC and were lost on LFC eviction or Pageserver restart.
2. `extensions/air_smgr/air_smgr.c` (`air_extend`, `air_writev`) updated LFC and `air_relmeta_extend` *before* `air_ps_put_pages` succeeded, allowing failed Pageserver RPCs to leave phantom unpersisted blocks in local LFC/metadata.
3. `extensions/air_smgr/communicator.c` (`air_ps_get_pages`) silently `memset` buffers to `0` and returned `true` when Pageserver returned `AIR_PS_STATUS_NOT_FOUND`, disguising missing pages as valid zero pages and triggering `SQLSTATE XX002` in PostgreSQL indexes.

---

## 2. Changes Implemented

- **`extensions/air_smgr/air_smgr.c`:**
  - `air_zeroextend`: Persists all `nblocks` zero-filled blocks to Pageserver via `air_ps_put_pages` **before** updating LFC and `air_relmeta_extend`. Raises `ERROR (ERRCODE_IO_ERROR)` on persistence failure.
  - `air_extend` & `air_writev`: Persist to Pageserver via `air_ps_put_pages` **before** updating LFC and `air_relmeta_extend`.
- **`extensions/air_smgr/communicator.c`:**
  - `air_ps_get_pages`: Removed silent zero-fill on `AIR_PS_STATUS_NOT_FOUND`; returns `false` with explicit error details (`page not found in pageserver (...)`) so `air_readv` raises `ERRCODE_IO_ERROR` instead of substituting zeros.

---

## 3. Verification & Test Results

| Test | Command / Target | Result | Evidence |
|---|---|---|---|
| Pageserver unit recovery & zero-page vs missing-page | `go test -race -v ./internal/pageserver -run TestPageserverRestart_RecoversZeroExtendAndRejectsMissingPage` | **PASS** | Valid zero-extended block 4 recovered after restart + cold cache; missing block 99 returned `ErrNotFound` |
| Full storage path + cold LFC + Pageserver restart + concurrent extension + `amcheck` + `TRUNCATE`/`DROP` | `go run ./benchmarks/phase21 --mode=storage-path --tenant-prefix=p2110b` | **PASS** | `count=901`, `sum=4056277`; Pageserver restart `count=902`; 4-worker concurrent extension (`1000` rows); LFC cleared (`rm -f *.lfc`) + Pageserver restart + PG restart; `amcheck` (`bt_index_check`) passed; `TRUNCATE` + `DROP TABLE` passed; **0 `XX002` errors** |
| Crash recovery + `PG SIGKILL` + Pageserver restart + 2s network interruption | `go run ./benchmarks/phase21 --mode=crash-recovery --tenant-prefix=p2110b` | **PASS** | `fsync=on`, `full_page_writes=on`, `synchronous_commit=on`; `count=250`, `sum=313750`; **0 data loss, 0 `XX002` errors** |
