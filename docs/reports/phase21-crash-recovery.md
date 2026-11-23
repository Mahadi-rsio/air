# AIR Phase 21.4 — Durability & Crash Recovery Report

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Command:** `go run ./benchmarks/phase21 --mode=crash-recovery --tenant-prefix=p21f`

---

## 1. Durability Configuration Verified

Queried live through `air-gateway` on tenant `p21f_cr`:
- `fsync = on`
- `full_page_writes = on`
- `synchronous_commit = on`

No durability settings were weakened.

---

## 2. Failure Scenarios Tested & Measured Results

| # | Scenario | Result | Verification |
|---|---|---|---|
| 1 | PostgreSQL clean shutdown (`suspend`/`wake`) | **PASS** | Verified in `p21c_sp` (`count=901`, `sum=4056277`) |
| 2 | PostgreSQL `SIGKILL` (`kill -9` postmaster) with active uncommitted TX | **PASS** | 100 committed rows intact; uncommitted row `999001` rolled back (`uncommCnt=0`) |
| 3 | Cell Agent (`air-p21-server`) restart | **PASS** | `air-server` restarted cleanly and reconnected to `air-pageserver` |
| 4 | AIR Gateway (`air-p21-gateway`) restart | **PASS** | `air-gateway` restarted and resumed pooling/routing to `p21f_cr` |
| 5 | Pageserver (`air-p21-pageserver`) restart during active workload | **PASS** | Rows `101..150` committed pre-restart + `151..200` post-restart all durable |
| 6 | Temporary network interruption (`docker network disconnect`/`connect` 2s) | **FAIL** | `ERROR: index "pg_proc_proname_args_nsp_index" contains unexpected zero page at block 27 (SQLSTATE XX002)` |

---

## 3. Finding

PostgreSQL `SIGKILL` crash recovery, Cell Agent restart, Gateway restart, and Pageserver restart all preserved committed rows (`1..200`) and rolled back uncommitted transactions (`999001`). However, after the subsequent 2-second network interruption on `air-pageserver`, PostgreSQL 17 encountered `SQLSTATE XX002` (`pg_proc_proname_args_nsp_index` zero page at block 27), confirming the same `air_smgr` / Pageserver missing-block blocker identified in Phase 21.3.

---

## 4. Status

**Phase 21.4 Verdict: PARTIAL PASS (PG SIGKILL, Cell Agent, Gateway, and Pageserver restarts passed; network interruption recovery failed with `XX002`).**
