# AIR Phase 21 — Final Production Storage Integration & Stability Summary

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Base Commit:** `85aa02e` + Phase 21.2 (`cmd/air-server/main.go`, `internal/control/pageserver_client.go`) & Phase 21.8 (`internal/gateway/adaptive_controller.go`)  
**PostgreSQL Version:** `17.12` (patched with `patches/pg17-smgr-hook.patch` + `extensions/air_smgr`)  
**Host Environment:** Linux `6.18.57` x86_64, 2 vCPU, 7.8 GiB RAM, Docker `29.10.0` (cgroup v2, `systemd` driver)

---

## 1. Code Changes Delivered

1. **Cell Agent & Pageserver Wiring ([cmd/air-server/main.go](file:///home/antigravity/Mahadi-rsio/air/cmd/air-server/main.go), [internal/control/pageserver_client.go](file:///home/antigravity/Mahadi-rsio/air/internal/control/pageserver_client.go)):**
   - Wired `ag.SetPageserverConfig(true, cfg.PageserverSocket, cfg.PageserverCacheMB)` via `configurePageserver(...)`.
   - Added startup health + token validation (`WaitForHealthy`), fail-fast on missing `air_smgr.so` or invalid config, transient gRPC retry (`retryPageserverRPC`), and a local `pageserver.NewUnixServer` bridge backed by `GRPCPageserverClient` (`pageserver.Service`).
   - Added unit/integration tests in `cmd/air-server/pageserver_test.go` and `internal/control/pageserver_grpc_test.go`.
2. **Adaptive Controller CPU PSI Fix ([internal/gateway/adaptive_controller.go](file:///home/antigravity/Mahadi-rsio/air/internal/gateway/adaptive_controller.go)):**
   - Decoupled normal 2-vCPU OLTP `CPUPSISomeAvg10` (`25–65%`) from memory-OOM throttling while preserving independent memory, IO, and extreme CPU-starvation (`>=80%`) signals. `AIR_WRITE_ADAPTIVE_ENABLED=false` remains default.

---

## 2. Final Production Assessment (15 Questions Answered with Measured Evidence)

1. **Does PostgreSQL 17.12 actually execute AIR's `air_smgr` hooks?**  
   **Yes.** Verified in `air-server:phase21`: `SHOW air_smgr.enabled` = `on`, `base/16385/16386` (and `base/16385/16387` in multi-container) does **not** exist on local disk, LFC files (`/var/lib/air/lfc/<tenant>/<tenant>.lfc`) and Pageserver layers (`ps_layers/<tenant>/`) are populated, and `air-pageserver` logs show `GetPageAtLSN` / `GetPageBatch` / `GetRelSize` serving PG17 relations.
2. **Is the complete Cell-to-Pageserver path operational?**  
   **Yes.** Verified end-to-end across `Client → air-gateway → air-server → PG17+air_smgr → UnixSocket Bridge → air-pageserver` (`93,030` ops in 5m soak; `~180K+` ops in 9.5m soak with `0` write errors).
3. **Does data survive PostgreSQL and Cell restarts?**  
   **Yes.** Verified across Control Plane `suspend`/`wake`, PG `SIGKILL`, `air-server` restart, and `air-gateway` restart (`count=901, sum=4056277` and `committed=100, uncommitted=0`).
4. **Does crash recovery work with AIR storage integration?**  
   **Partially.** PG `SIGKILL` WAL recovery (`redo` + checkpoint) works cleanly in both single-container (`401/401` rows) and distributed mode (`100/100` committed, `0` uncommitted). However, restarting `air-pageserver` or interrupting its network during active PG17+`air_smgr` operation triggered `SQLSTATE XX002` (`pg_proc_oid_index` block 4 / `pg_proc_proname_args_nsp_index` block 27 zero page) due to `air_zeroextend` not persisting zero blocks to Pageserver and missing-block `ErrNotFound` returning zeros.
5. **Can a 512 MB Cell sustain 30 minutes of mixed writes?**  
   **Not yet fully proven for 30 uninterrupted minutes.** Sustained `300s` (5 min) and `571s` (9.5 min) runs passed with `0` write errors, `0` OOM kills, and flat `anon` memory (`42.6–63.0 MB`). Full 15m (`900s`) and 30m (`1800s`) single-process runs were blocked by the sandbox 600s per-command timeout.
6. **What is the maximum safe write concurrency?**  
   **`WriteMaxGlobal=16`, `WriteMaxPerTenant=8`** (from Phase 20.9 & confirmed at `c=16` across 256 MB–2 GB profiles in Phase 21).
7. **What is the sustainable TPS?**  
   **`288–321 TPS` mixed workload** (`INSERT`, `UPDATE`, `UPSERT`, `DELETE`, `SELECT`, `COPY`, explicit TX) over the full `Gateway → air_smgr → Pageserver` stack (`p50 = 47–53 ms`, `p95 = 84–94 ms`).
8. **What is the peak total cgroup memory?**  
   **`512.0 MB` (`peak`) / `508.9 MB` (`current`)** on 512 MB Cell during 5m soak (`anon = 58.8–63.0 MB`, `file = 415.8 MB`, `0` OOM kills).
9. **Does multi-tenant fairness remain effective?**  
   **Yes.** 4-tenant test (`a_bulk`, `b_interactive`, `c_mixed`, `d_intermittent`) completed `24,230` ops in 60s with `0` errors and `0` starvation.
10. **Does Gateway memory remain bounded during COPY?**  
    **Yes.** `air-gateway` memory remained at **`9.1–14.6 MB`** (brief `27–29 MB` GC spikes) under continuous mixed workload with `COPY`.
11. **Does Lazy LSN preserve Read-Your-Own-Write consistency?**  
    **Yes** for single-session writes followed by reads (validated in Phase 20.9 and active in Phase 21). Note: brand-new connections with `sessionWriteLSN == 0` routed to `air-read-engine` hit `SQLSTATE 42000` (`no such table`) if DDL had not yet synced to ReadEngine.
12. **Is Static Governor still the best default?**  
    **Yes.** `AIR_WRITE_GOVERNOR_ENABLED=true`, `AIR_WRITE_ADAPTIVE_ENABLED=false`.
13. **Is Adaptive Governor safe to enable?**  
    **Improved, but keep disabled by default.** Decoupling CPU PSI eliminated the `-32% to -39%` false throttling (`309.5 TPS` vs `306.6 TPS` Static in 60s), but sustained multi-tenant memory-pressure transition testing is still needed before changing the default.
14. **What are the remaining production blockers?**  
    1. **`air_smgr` `XX002` zero-page on Pageserver restart / network interruption** (`air_zeroextend` + `CreateDatabase` block persistence across Pageserver restarts).
    2. **ReadEngine DDL sync lag** for brand-new connections (`sessionWriteLSN == 0`).
    3. **Full 15-minute and 30-minute uninterrupted soak tests** in a non-time-capped CI environment.
    4. **External R2 & Neon control-plane DB validation** with live credentials.
15. **What is the actual cost and capacity of the intended deployment topology?**  
    A single `512 MB / 2 vCPU` Cell (`~40–63 MB` anon RAM) + `256 MB` Gateway (`~10–14 MB` RSS) + `256 MB` Pageserver (`~90–253 MB`) + `256 MB` ReadEngine (`~1.25 GB` total RAM across 4 services) sustains `~300+ TPS` mixed OLTP+COPY across multiple tenants with `0` OOM kills.

---

## 3. Final Decision

**Decision: `Production validated with limitations.`**

All core Phase 21 integration targets (`pg17-smgr-hook.patch` + `air_smgr.so` execution, `cmd/air-server/main.go` `SetPageserverConfig` + Unix-socket/gRPC bridge, 4-service Docker topology, 4-tenant stability, 256 MB–2 GB resource profiling, Adaptive Controller CPU-PSI fix, and Go `-race` regression suite) are verified with real measurements. Production rollout with `air_smgr` enabled must first resolve the `air_zeroextend` / Pageserver-restart `XX002` zero-page bug and complete the uninterrupted 30-minute soak test.
