# AIR Phase 20.9 — Final Engineering Summary & Production Assessment

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Base Commit:** `28cc6657` + Phase 20.9 additions (`internal/cell/telemetry_publisher.go`, `cmd/air-server/main.go`, `internal/gateway/pooler.go`, `benchmarks/phase20.9/`)

---

## 1. Executive Summary

Phase 20.9 validated AIR's Phase 20 Resource-Aware Write Governor against **real PostgreSQL 17.12**, real `cell.Agent`, real `air-gateway` over TCP, real WAL durability (`fsync=on`, `full_page_writes=on`, `synchronous_commit=on`), and real Linux **cgroup v2** limits (`256 MB`, `512 MB`, `1 GB`) with live PSI telemetry.

### Code Fixes Delivered During Validation
1. **Cell-to-Gateway Telemetry Publisher ([telemetry_publisher.go](file:///workspaces/air/internal/cell/telemetry_publisher.go), [cmd/air-server/main.go](file:///workspaces/air/cmd/air-server/main.go)):** Closed the missing telemetry push loop discovered in the audit so `cell.Agent` pushes cgroup v2 + PSI snapshots to `air-gateway`'s `POST /internal/v1/telemetry` (works both locally and across machines).
2. **Lazy LSN Read-Check Guard ([pooler.go](file:///workspaces/air/internal/gateway/pooler.go#L833-L840)):** Eliminated wasted `pg_last_wal_replay_lsn()` read-backend queries on `BEGIN` and write statements.
3. **Reproducible Real-PG17 Benchmark Suite ([benchmarks/phase20.9/](file:///workspaces/air/benchmarks/phase20.9/)):** Automated setup, workload suite, concurrency sweep, cgroup v2 memory validation, 4-tenant DRR fairness, crash-recovery/correctness, and sustained-load testing.

---

## 2. Final Engineering Assessment (12 Questions Answered with Measured Evidence)

1. **What is the maximum stable write throughput on a 512 MB Cell?**
   - **Autocommit single-row INSERT:** **4,590–4,654 TPS** (Static Governor + Buffer Budget + Lazy LSN, `0` errors, `0` OOM, `0` `memory.high` events).
   - **Explicit `pgbench` TPC-B transactions:** **885–895 TPS** (`c=8..32`, `0` errors).
   - **Multi-row INSERT (50 rows/stmt):** **88,575 rows/s** (1,771.5 TPS).
   - **Streaming `COPY FROM STDIN` (25K rows/batch):** **936,747 rows/s** (198.5 MB WAL in 5s, Gateway RSS peak 23.0 MB).

2. **How many concurrent write operations can a 512 MB Cell safely sustain?**
   - **Per-tenant recommended active write concurrency (`WriteMaxPerTenant`):** **8** (up to **16** global across tenants on a 2-vCPU 512 MB cell).
   - **Client connections sustained safely:** Up to **64 concurrent writing clients** tested with `0` errors, `0` OOM kills, and `0` `memory.high` events when Static Write Governor caps active writes to `16` (bounding non-reclaimable `anon` RAM to **31.0 MB** vs **84.5 MB** without Governor).

3. **What is the p95 and p99 latency?**
   - At `c=8` (`D_gov_buf_lazylsn`, autocommit INSERT): **p95 = 2.75 ms**, **p99 = 3.56 ms** (p50 = 1.66 ms).
   - At `c=32` (`D_gov_buf_lazylsn`, autocommit INSERT): **p95 = 8.66 ms (-17.8% vs No-Gov)**, **p99 = 10.90 ms (-24.0% vs No-Gov)**.
   - During 5-minute sustained `UPDATE` load (`c=8`): **p95 = 3.38–4.45 ms**, **p99 = 4.90–6.31 ms** with zero drift.

4. **What is peak total cgroup memory?**
   - Normal OLTP writes (`c=4..64`, 512 MB cell): **306.8–363.1 MB** (`anon` = **31.0 MB**, `file` page cache = **303.5 MB**).
   - Heavy streaming `COPY` (`936K rows/s`): **435.9 MB** (clamped cleanly at `memory.high = 435 MiB`, `0` OOM).
   - Gateway process RSS: **22.1–26.5 MB** steady state (brief 60–79 MB GC-reclaimed spikes during `MeteringLogger` flush).

5. **At what point does memory pressure begin?**
   - On **512 MB** (`memory.high=435 MiB`), OLTP writes up to `c=64` experienced **`0` `memory.high` events** and **`0.00` memory PSI**. Pressure only began during sustained high-rate `COPY` (~40 MB/s WAL) when page cache reached `435 MiB`.
   - On **256 MB** (`memory.high=218 MiB`), `memory.high` reclaim began at `c=32` (`61–262` events), reclaiming page cache cleanly with **`0` OOM kills**.

6. **Does adaptive control improve stability compared with static control?**
   - **No — on a 2-vCPU host, current Adaptive Mode (`AIR_WRITE_ADAPTIVE_ENABLED=true`) degraded performance without improving memory stability.** Because `AdaptiveController` triggers `pressured` at `CPUPSISomeAvg10 >= 25.0` (and requires `< 15.0` to recover), normal healthy PostgreSQL write workloads (`cpuPSI = 24–65%`, `memPSI = 0.00`, `high_events = 0`) permanently pinned `effectiveGlobalLimit` to `minGlobal=2`, reducing autocommit TPS by **-32% to -39%** and worsening p95 latency (`8.66ms -> 11.68ms`).
   - **Decision:** Keep `AIR_WRITE_ADAPTIVE_ENABLED=false` as the production default until CPU/IO PSI thresholds are decoupled from memory-pressure throttling.

7. **How does `COPY` affect memory consumption?**
   - **Gateway memory:** Flat at **23.0–23.1 MB RSS** even at `936,746 rows/s` across 4 concurrent streams (true streaming via `relayCopyIn`).
   - **PostgreSQL Cell cgroup memory:** Rapidly fills reclaimable Linux `file` page cache up to `memory.high` (`435.9 MB`), where kernel reclaim throttles page-cache growth smoothly with `0` OOM kills.

8. **Can one heavy tenant degrade other tenants?**
   - **Without Governor:** Yes. A 16-client heavy bulk writer (`tenant_a`) reduced interactive `tenant_b` to `507.1 TPS` (`p95 = 7.49ms`) and mixed `tenant_c` to `993.8 TPS` (`p95 = 7.68ms`).
   - **With Static Write Governor (DRR):** Heavy tenant impact is effectively isolated. `tenant_b` throughput rose **+39.1%** (`705.5 TPS`) with **-34.4% p95 latency** (`4.91ms`), and `tenant_c` rose **+41.1%** (`1,402.7 TPS`) with **-33.7% p95 latency** (`5.09ms`), while total cluster TPS stayed within 2%.

9. **Does Lazy LSN preserve read-your-own-write consistency?**
   - **Yes, for single-session RYOW.** Verified against real PostgreSQL 17.12: `20` pure writes incurred **`0` `pg_current_wal_lsn()` queries** (vs `20` with eager LSN), and subsequent `SELECT`s (after both autocommit writes and explicit `BEGIN/COMMIT`) preserved 100% RYOW consistency while issuing exactly `1` `pg_current_wal_lsn()` query. Cross-session RYOW across reconnects is not guaranteed (per-session state).

10. **What is the actual bottleneck in the write path?**
    1. **PostgreSQL WAL commit & 2-vCPU saturation** (~4,600–5,100 autocommit TPS / ~890 TPC-B TPS).
    2. **Gateway warm-backend reuse ping (`pooler.go:1455-1465`)** executing `""` or `DISCARD ALL;` on every autocommit statement (doubles PG round-trips).
    3. **Explicit transaction admission gap (`pooler.go:444`)**: `WriteGovernor` only gates `BEGIN`, releasing the permit immediately while the backend remains pinned for the transaction's duration.

11. **What is the best configuration for 512 MB, 1 GB, and 2 GB profiles?**

| Profile | `memory.high` / `memory.max` | `AIR_WRITE_GOVERNOR_ENABLED` | `AIR_WRITE_ADAPTIVE_ENABLED` | `AIR_WRITE_MAX_GLOBAL` | `AIR_WRITE_MAX_PER_TENANT` | `AIR_POOL_MAX_BACKENDS` | `AIR_WRITE_BUFFER_GLOBAL_MAX` | `AIR_WRITE_LAZY_LSN_ENABLED` |
|---|---|---|---|---:|---:|---:|---|---|
| **512 MB** | `435 MiB` / `512 MiB` | `true` | `false` | `16` | `8` | `16` | `32 MiB` | `true` |
| **1 GB** | `870 MiB` / `1024 MiB` | `true` | `false` | `32` | `12` | `24` | `64 MiB` | `true` |
| **2 GB** *(extrapolated)* | `1740 MiB` / `2048 MiB` | `true` | `false` | `48` | `16` | `32` | `128 MiB` | `true` |

12. **Which optimizations should remain enabled by default?**
    - **Keep Enabled:** Static Write Governor + DRR (`WriteGovernorEnabled=true`), Global/Session Buffer Budget (`WriteBufferGlobalMax`, `WriteBufferSessionMax`), Lazy LSN (`WriteLazyLSNEnabled=true`).
    - **Keep Disabled by Default:** Adaptive Controller (`WriteAdaptiveEnabled=false`) until CPU PSI throttling is recalibrated.

---

## 3. Limitations & Tests Not Executed

- **`air_smgr` storage manager:** Not executed on host because host PostgreSQL 17.12 lacks `pg17-smgr-hook.patch` / `air_smgr.so`, and `cmd/air-server/main.go` does not yet wire `Agent.SetPageserverConfig`.
- **Sustained load duration:** Executed full **5-minute (300s)** stability test (`0` errors, `0` leaks, `0` OOM); 15-minute and 30-minute tests were not run due to session time constraints (reproducible via `--sustained-sec 900` / `1800`).
- **2 GB profile:** Extrapolated from 1 GB measurements (`252 MB` peak, `0` pressure).

---

## 4. Final Production Decision

**Decision: `Production validation passed with limitations.`**

- **Static Write Governor (DRR), Buffer Budget, Streaming COPY, and Lazy LSN** are validated against real PostgreSQL 17.12 with 11/11 correctness & crash-recovery checks passing, zero data races, zero OOM kills on 256 MB/512 MB/1 GB cgroup v2 profiles, -63% non-reclaimable PostgreSQL anonymous memory at 64 clients, -18% to -24% tail latency at high concurrency, and +39–41% throughput / -34% p95 latency protection for interactive tenants under multi-tenant contention.
- **Limitations requiring follow-up before enabling Adaptive Mode or `air_smgr` in production:**
  1. Recalibrate `AdaptiveController` CPU/IO PSI thresholds (decouple CPU PSI from memory-OOM protection) before turning on `AIR_WRITE_ADAPTIVE_ENABLED`.
  2. Wire `Agent.SetPageserverConfig` in `cmd/air-server/main.go` and repeat validation against patched PG17 + `air_smgr.so`.
  3. Run 30-minute soak test in a dedicated CI/staging environment.
