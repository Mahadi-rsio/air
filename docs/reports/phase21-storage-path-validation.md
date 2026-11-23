# AIR Phase 21.3 — Full AIR Storage Path Integration Report

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Stack:** `air-gateway:phase21` (256 MB) → `air-server:phase21` (512 MB, 2 vCPU, PG 17.12 + `air_smgr.so`) → Unix Socket Bridge (`/var/lib/air/pageserver.sock`) → `air-pageserver:phase21` (256 MB, volume `/var/lib/air-pageserver`) + `air-read-engine:phase21` (256 MB) + `postgres:17-alpine` metadata DB.

---

## 1. End-to-End Data Flow Verified

```
PostgreSQL Client (pgx/v5)
  → air-gateway:15432 (WriteGovernor, BufferBudget, LazyLSN, Pooler)
    → air-server / PG 17.12 (air_smgr.so, LFC)
      → /var/lib/air/pageserver.sock (AIRP binary wire protocol)
        → air-server UnixServer bridge (GRPCPageserverClient)
          → air-pageserver:9091 (gRPC PageserverService)
            → /var/lib/air-pageserver/ps_layers (LayerStore)
```

---

## 2. Verification Results (`go run ./benchmarks/phase21 --mode=storage-path --tenant-prefix=p21c`)

| Operation | Result | Evidence |
|---|---|---|
| Database provisioning | **PASS** | `p21c_sp` (`gen_d721a0bcd1026ca2`) provisioned via `POST /v1/databases` |
| Relation + Index creation | **PASS** | `CREATE TABLE p21_orders` + `CREATE INDEX idx_p21_orders_acct` |
| INSERT / UPDATE / DELETE / SELECT | **PASS** | 1000 inserted, 200 updated, 100 deleted |
| Explicit TX Commit & Rollback | **PASS** | `count=901`, `sum=4056277`, `rbCnt=0` |
| Non-local storage proof | **PASS** | `/var/lib/air/tenants/active/p21c_sp/pgdata/base/16385/16387` **absent** on `air-server`; `ps_layers/p21c_sp/gen_d721a0bcd1026ca2` present on `air-pageserver` |
| PG restart (`suspend`/`wake`) | **PASS** | `count=901`, `sum=4056277` preserved |
| Pageserver restart (`docker restart`) | **FAIL** | `ERROR: index "pg_proc_oid_index" contains unexpected zero page at block 4 (SQLSTATE XX002)` |

---

## 3. Root Cause Found on Pageserver Restart (`SQLSTATE XX002`)

From `air-pageserver` debug logs:
```
[grpc-read] GetPageAtLSN tenant=p21c_sp key=k1:1663/16385/2690.0#4 req_lsn=0/02002848 found=false
```
When `air-pageserver` restarted, `1663/16385/2690.0#4` (`pg_proc_oid_index` block 4) was not found in the persisted L0 layers (`found=false`), causing `air_readv` to return a zero-filled block and PostgreSQL 17 to raise `SQLSTATE XX002`.

**Two contributing factors identified in the codebase:**
1. `extensions/air_smgr/air_smgr.c` (`air_zeroextend`) writes zero blocks to LFC only and does not persist them to Pageserver via `air_ps_put_pages`.
2. `cmd/air-pageserver/main.go` calls `grpcSrv.GracefulStop()` before `ps.FlushAll(shutCtx)`, and `air-server`'s active `WALReceiver` / `CreateDatabase` pages between `0/02000836` and `0/02002848` were not all flushed to disk layers before `air-pageserver` exited.

---

## 4. Status

**Phase 21.3 Verdict: PASSED for normal operations & PG restart; FAILED on `air-pageserver` restart (`XX002` zero-page on `pg_proc_oid_index` block 4).**
