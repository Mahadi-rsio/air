# AIR Phase 21.10 — Final Correctness, Read Consistency & Stability Summary

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Base Commit:** `803c0fd9f0cab7c2c5b0b85c3898f901fd264d4f` (+ Phase 21.10 fixes)  
**PostgreSQL Version:** `17.12` (patched with `patches/pg17-smgr-hook.patch` + `extensions/air_smgr`)  
**Overall Phase 21.10 Status:** **PARTIAL**

---

## 1. Acceptance Criteria Checklist

- [x] **`air_zeroextend()` persistence semantics:** **PASS** — `air_zeroextend`, `air_extend`, and `air_writev` persist to Pageserver before updating LFC/`relmeta`; `air_ps_get_pages` no longer substitutes zeros on `AIR_PS_STATUS_NOT_FOUND`.
- [x] **No unexpected zero-page (`XX002`) errors:** **PASS** — Verified across `storage-path`, `crash-recovery`, `amcheck`, cold LFC, and 5-minute soak.
- [x] **Pageserver restart and recovery tests:** **PASS** — Added `wrapped_dek.b64` persistence, `recoverLocalTenants()` on startup, and bounded `GracefulStop` before `FlushAll`.
- [x] **Network interruption tests:** **PASS** — 2-second Cell-to-Pageserver network disconnect/reconnect passed with 0 data loss and 0 `XX002`.
- [x] **Read Engine schema visibility:** **PASS** — Added pool-wide `poolDDLLSN`/`poolDDLDirty` barrier, separated `walLastLSN` from `PutPageBatch`, and added extended-query `ReadEngine` error fallback.
- [x] **New connections do not read from stale schema state:** **PASS** — Verified 10/10 brand-new connections after `CREATE TABLE`, plus `CREATE INDEX`, `ALTER TABLE`, `DROP TABLE`, and `ReadEngine` restart (`2,742` ops, `0` errors).
- [x] **Read-your-own-write consistency:** **PASS** — Preserved.
- [x] **PostgreSQL RSS collection fixed:** **PASS** — Direct `/proc/<pid>/status` `VmRSS` parsing; missing metrics report `NA` rather than `0.00`.
- [x] **Full topology 5-minute soak (`300s`, Read Engine enabled):** **PASS** — `95,443` ops, `318.1 TPS`, `0` errors, `0` OOM kills.
- [ ] **Full topology 15-minute (`900s`) and 30-minute (`1800s`) soak:** **NOT TESTED** — Blocked by `600s` per-command VM runner timeout.
- [x] **Multi-tenant contention > `WriteMaxGlobal=16` (4 tenants, 32w & 64w):** **PASS** — `0` errors; interactive `t1` held `10.39–11.60 ms` p50 under `25–57` bulk writers.
- [ ] **8-tenant simultaneous active test on `256 MB` Pageserver:** **FAIL** — `air-pageserver` (`256 MB`) OOMKilled (`Exit 137`) during 10th-tenant bootstrap.
- [x] **Fly deployment configuration verified & updated:** **PASS** (`deploy/fly/fly.server.toml`, `docker-compose.yml`).
- [ ] **External R2 & Neon DB integrations:** **NOT TESTED** — No live credentials in sandbox; documented in [phase21.10-deployment-validation.md](file:///home/antigravity/Mahadi-rsio/air/docs/reports/phase21.10-deployment-validation.md).
- [x] **Existing correctness & regression tests:** **PASS** (`internal/pageserver`, `cmd/air-server`, `internal/cell`, `internal/gateway`, `benchmarks/phase21`).

---

## 2. Reports Index

1. [phase21.10-initial-audit.md](file:///home/antigravity/Mahadi-rsio/air/docs/reports/phase21.10-initial-audit.md)
2. [phase21.10-zeroextend-fix.md](file:///home/antigravity/Mahadi-rsio/air/docs/reports/phase21.10-zeroextend-fix.md)
3. [phase21.10-read-engine-consistency.md](file:///home/antigravity/Mahadi-rsio/air/docs/reports/phase21.10-read-engine-consistency.md)
4. [phase21.10-pageserver-recovery.md](file:///home/antigravity/Mahadi-rsio/air/docs/reports/phase21.10-pageserver-recovery.md)
5. [phase21.10-resource-metrics.md](file:///home/antigravity/Mahadi-rsio/air/docs/reports/phase21.10-resource-metrics.md)
6. [phase21.10-soak-test.md](file:///home/antigravity/Mahadi-rsio/air/docs/reports/phase21.10-soak-test.md)
7. [phase21.10-multitenant-fairness.md](file:///home/antigravity/Mahadi-rsio/air/docs/reports/phase21.10-multitenant-fairness.md)
8. [phase21.10-deployment-validation.md](file:///home/antigravity/Mahadi-rsio/air/docs/reports/phase21.10-deployment-validation.md)
9. [phase21.10-final-summary.md](file:///home/antigravity/Mahadi-rsio/air/docs/reports/phase21.10-final-summary.md)
