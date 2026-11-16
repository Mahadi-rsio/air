# Air Platform Implementation & Verification Tracker (`todo.md`)

> **Architecture & Phase Reference:** [phase.md](file:///workspaces/air/phase.md) · [plan.md](file:///workspaces/air/plan.md) · [resolution-register.md](file:///workspaces/air/resolution-register.md) · [issue_resolution.md](file:///workspaces/air/issue_resolution.md)

---

## Stage 0 — Gating PoCs & Kernel / Cloud Verification (`COMPLETED`)
- [x] **Docker BuildKit Caching:** Configured `--mount=type=cache` across [stage_test/stage0/Dockerfile](file:///workspaces/air/stage_test/stage0/Dockerfile), [mvp/cell/Dockerfile](file:///workspaces/air/mvp/cell/Dockerfile), [mvp/gateway/Dockerfile](file:///workspaces/air/mvp/gateway/Dockerfile), and [stage_test/stage1/Dockerfile](file:///workspaces/air/stage_test/stage1/Dockerfile).
- [x] **`EXP-A1` (`UQR-01`, `UQR-02`):**
  - [x] Probe live Fly.io `6.12.116-fly` kernel (`ext4`, `PR_SET_CHILD_SUBREAPER`, `unshare --mount --pid --fork --net`, `/dev/loop0..4`, `/dev/shm`).
  - [x] Pre-warmed two-phase `initdb --data-checksums` pool (`100%` unique `system_identifier`s, `0.141–0.194 ms` claim latency vs `<15ms` gate).
  - [x] Background `nice -n 19` refill under sibling OLTP (`0` errors, `p50 = 28.17 ms`).
  - [x] `pg_test_fsync` NVMe baseline (`fdatasync` `2,771.8 ops/sec` / `361 usecs/op`).
- [x] **`EXP-C1` & `UQR-06` (`ADR-005`):**
  - [x] Compile `supautils` v4.0.0 against PostgreSQL 17 (`17.9`) in [stage_test/stage0/supautils/](file:///workspaces/air/stage_test/stage0/supautils/).
  - [x] Verify non-superuser allowlisted `CREATE EXTENSION pg_trgm` (`true`) and `SplitIdentifierString` quoting constraint.
  - [x] Verify `100%` block rate on `ALTER ROLE postgres`, `GRANT pg_execute_server_program`, non-allowlisted `CREATE EXTENSION amcheck`, `COPY PROGRAM`, `pg_read_file('/etc/passwd')`, `unshare --net` SSRF, and cross-tenant UID `10001 -> 10002` `0700` `PGDATA` reads.
- [x] **`EXP-C3` (`ADR-010`):**
  - [x] Pre-allocate `64 MB` `.cell_ballast` emergency reserve.
  - [x] Sub-second tenant disk quota enforcer halts runaway Tenant A (`36 MB` quota -> halted `read-only` at `43.97 MB`, `7.97 MB` overshoot vs `<25 MB` gate) with `0` errors on neighbor Tenant B.
- [x] **`EXP-D1` (`UQR-04`, `ADR-003`):**
  - [x] PG17 Primary + synchronous Physical Standby (`synchronous_standby_names = 'FIRST 1 (standby1)'`).
  - [x] Preserve `50/50` ACKed committed rows (`RPO = 0`) across immediate `kill -9` on Primary, roll back uncommitted row `51`, promote via `pg_ctl promote` in `857 ms` RTO (`<4–8s` gate), and stall commits when standby stops.
- [x] **`EXP-D4` & `EXP-D5` (`UQR-03`, `ADR-002`):**
  - [x] Live Cloudflare R2 (`air-mvp-test`) 20-goroutine `If-None-Match: "*"` race (`1` winner `200 OK`, `19` fenced `412 PreconditionFailed`).
  - [x] `pg_verifybackup` rejects 4-byte bitflip; missing WAL summary fallback verified.
  - [x] PG17 full (`837 ms`) + `--incremental` (`294 ms`) backup to R2 -> `pg_combinebackup` -> `pg_verifybackup` -> LSN PITR (`200` rows recovered before `DROP TABLE`) -> `pg_amcheck` (`0` errors).
- [x] **Stage 0 Artifacts & Git Push:** Recorded in [stage0-results.json](file:///workspaces/air/stage_test/stage0/stage0-results.json) (`all_passed: true`) and pushed to `origin/main`.

---

## Stage 1 — Core Reliability MVP (`COMPLETED`)
- [x] **Envelope Encryption KMS ([stage_test/stage1/pkg/crypto/envelope.go](file:///workspaces/air/stage_test/stage1/pkg/crypto/envelope.go)):**
  - [x] 256-bit Master `KEK` + per-tenant wrapped 256-bit `DEK`.
  - [x] `AES-256-GCM` encryption bound to `AAD = tenant_id|epoch|object_key` (`SI-4`).
  - [x] Instant GDPR `ShredDEK(tenantID)` crypto-shredding (`Flow 12`).
- [x] **Cell Agent Daemon ([stage_test/stage1/pkg/cell/agent.go](file:///workspaces/air/stage_test/stage1/pkg/cell/agent.go) & [stage_test/stage1/cmd/pg-cell-agent/main.go](file:///workspaces/air/stage_test/stage1/cmd/pg-cell-agent/main.go)):**
  - [x] `PR_SET_CHILD_SUBREAPER` + `64 MB` `.cell_ballast` + boot cleanup of `/vol/pool/staging/*`.
  - [x] Exclusive `flock(LOCK_EX | LOCK_NB)` on `agent.lock` + live `postgres` PID re-adoption via `postmaster.pid` and `/proc/<pid>/comm` on agent restart (`ADR-006`).
  - [x] Pre-warmed two-phase `initdb --data-checksums` pool with background `nice -n 19` refill and bounded on-demand `initdb` fallback (`ADR-001`).
  - [x] `SCRAM-SHA-256` secret injection via `0600` ephemeral file (`0` CLI/env secrets) + `supautils` v4.0.0 + `PgBouncer 1.21+` (`pool_mode = transaction`, `max_prepared_statements = 200`).
  - [x] PG17 Full & `--incremental` backup + `pg_verifybackup` + `zstd` + `AES-256-GCM-AAD` R2 upload & `pg_combinebackup` restore.
- [x] **Control Plane Service ([stage_test/stage1/pkg/control/control.go](file:///workspaces/air/stage_test/stage1/pkg/control/control.go)):**
  - [x] PostgreSQL 17 Metadata DB (`clusters`, `idempotency_records`, `usage_ledger`, `audit_events`) enforcing `UNIQUE(tenant_id)` and `UNIQUE(system_identifier)`.
  - [x] Idempotent `ProvisionDatabase` (`Idempotency-Key`), `DeleteDatabase` (7-day `SOFT_DELETED` grace + `immediate=true` crypto-shredding), `IngestUsageBatch` (`ON CONFLICT DO NOTHING`), and `ReconcileOnce` orphan directory sweeper.
- [x] **Wire-Protocol Gateway ([stage_test/stage1/pkg/gateway/gateway.go](file:///workspaces/air/stage_test/stage1/pkg/gateway/gateway.go)):**
  - [x] `SSLRequest` (`80877103`) + wildcard TLS (`*.db.air.local`), `5s` pre-auth deadline, and `8 KB` `StartupMessage` length bomb fence (`FMEA-16`).
  - [x] Cryptographic `SNI == Startup tenant` binding (`SQLSTATE 28000` on spoof, `FMEA-17`) and `SQLSTATE 57P03` gate on soft-deleted DBs.
  - [x] 5-byte frame scanner + 32 KB `sync.Pool` streaming with `30s` `SetWriteDeadline` backpressure + local `bbolt` WAL usage ledger persistence (`ADR-009`, `ADR-012`).
- [x] **Execute & Verify Stage 1 Validation Gates ([stage_test/stage1/cmd/stage1-runner/main.go](file:///workspaces/air/stage_test/stage1/cmd/stage1-runner/main.go) -> [stage1-results.json](file:///workspaces/air/stage_test/stage1/stage1-results.json)):**
  - [x] **`EXP-A2` (`pass: true`):** 100 concurrent/duplicate `Idempotency-Key` provisioning requests across 12 tenants (`88` idempotent replays, `12/12` unique `system_identifier`s, `0.087 ms` p50 / `0.123 ms` p99 pool claim latency vs `<15ms` gate, `12/12` `SCRAM-SHA-256` logins verified).
  - [x] **`EXP-A3` (`pass: true`):** Interrupted `initdb`/claiming orphan purge (`0` orphans post-reconcile) + live `pg-cell-agent` `SIGKILL` under active OLTP load (`117` queries succeeded, `0` dropped SQL queries, `3/3` `postgres` PIDs re-adopted in `154 ms`, `0` dual primaries).
  - [x] **`STAGE1-E2E` (`pass: true`):** Gateway `8 KB` length bomb & SNI spoof (`28000`) rejection, `PgBouncer 1.21+` prepared statements (`true`), `bbolt` WAL `5x` replay idempotency (`0` duplicate billing rows), R2 `AES-256-GCM-AAD` Full (`2.73 MB`) + Incr (`22.3 KB`) combine restore (`20/20` rows + `pg_amcheck` pass), AAD tamper rejection (`true`), Soft Delete (`57P03` with DEK preserved), and Hard Purge Crypto-Shredding (`true`).

---

## Stage 2 — Multi-Tenant Density, Pro Sync HA Failover & Observability (`COMPLETED`)
- [x] **`EXP-C2` & `EXP-C4` (`UQR-07`, `UQR-10` — [stage_test/stage2/pkg/density/density.go](file:///workspaces/air/stage_test/stage2/pkg/density/density.go)):**
  - [x] **`EXP-C2` (`pass: true`):** 4 active + 6 dormant PG17 tenant clusters (`avg_active_cluster_rss_mb = 62.31 MB`), `oom_score_adj = -900` agent shield, Tenant A hash/sort OOM bomb contained (`57014_statement_timeout`), 25-connection fork storm capped (`17` rejected with `53300`), `0` leaked `/dev/shm` bytes, `0` dropped Tenant B OLTP queries (`46/46` succeeded), and `>70%` auto-scale trigger verified across `256 MB / 2 GB / 8 GB` SKU capacity curve.
  - [x] **`EXP-C4` (`pass: true`):** Empirical `shared_buffers` (`8MB / 16MB / 32MB`) RSS & cache hit-rate curve (`0.9964–1.0000`), anti-wraparound `maintenance_lock` vetoing sleep above `100M` XID age + `VACUUM (FREEZE)` reducing `age(datfrozenxid)` to `14`, **`0.0%` Customer Wire RU variance** (`50` vs `50` RUs) on Tenant B during heavy `CREATE INDEX` + `VACUUM FREEZE` + `CHECKPOINT` (`327,680` FinOps background write bytes separated), and `66.7%` R2 Class A cost reduction (`$2.33 -> $0.78/mo`) via 15s dirty-only WAL batching (`>= 52.3%` gross margin across all 4 tiers).
- [x] **`EXP-C5`, `EXP-D2`, `EXP-D3`, `EXP-D6` ([stage_test/stage2/pkg/failover/failover.go](file:///workspaces/air/stage_test/stage2/pkg/failover/failover.go)):**
  - [x] **`EXP-C5` (`pass: true`):** Single-tenant `postgres` `SIGKILL` (`PID 604 -> 627`) + subreaper `wait4(-1, WNOHANG)` zombie reaping recovered in `474.69 ms`, preserving `500/500` committed rows, rolling back uncommitted row `501`, and passing `pg_amcheck`.
  - [x] **`EXP-D2` (`pass: true`):** Pro Sync Primary + Physical Standby (`synchronous_standby_names = 'FIRST 1 (standby_zb)'`) stalled commit `#51` deterministically in `SyncRep` during standby partition (`SIGSTOP`) and resumed cleanly on heal (`51/51` rows synced).
  - [x] **`EXP-D3` (`pass: true`):** Simulated R2 outage spooled `10/10` encrypted `.wal.zst.enc` segments locally in `/vol/spool/wal/` without halting SQL writes and drained `10/10` segments to live Cloudflare R2 (`air-mvp-test`) on recovery (`0` remaining spool files).
  - [x] **`EXP-D6` (`pass: true`):** Asymmetric split-brain partition verified both Standard Async monotonic `CLOCK_MONOTONIC` self-kill (`8,001.28 ms` kill `< 12,206.89 ms` Control promote, `4,205.61 ms` disjoint safety margin, **`0` divergent commits post-promote**, stale epoch R2 upload rejected + `FENCED_QUARANTINE`) and Pro Sync Standby pre-fence (`SyncRepWaitForLSN` stalled partitioned old primary write, **`0` divergent commits**).
- [x] **Observability & DR (`RISK-39..41`, `UQR-11` — [stage_test/stage2/pkg/observability/observability.go](file:///workspaces/air/stage_test/stage2/pkg/observability/observability.go)):**
  - [x] OpenMetrics `/metrics` exporter verified for all **22 mandatory audit signals** (`all_22_audit_metrics_verified: true`).
  - [x] All **15 Alertmanager rules** codified in [alertmanager-rules.yaml](file:///workspaces/air/stage_test/stage2/config/alertmanager-rules.yaml) (`all_15_alert_rules_fired_in_fault_sim: true`).
  - [x] All **12 Disaster Recovery Runbooks** codified in [DR-01-to-12.md](file:///workspaces/air/stage_test/stage2/runbooks/dr/DR-01-to-12.md) and GameDay-verified (`DR-08` corrupted WAL fail-closed, `DR-11` Master `KEK` rotation across `7` tenants with `0` R2 bytes re-uploaded, `DR-12` staggered fleet recovery capped at `5/5` max concurrent starts, and `UQR-11` `95.0%` TSDB cardinality reduction at `50,000` tenants).
- [x] **Stage 2 Artifacts:** Recorded in [stage2-results.json](file:///workspaces/air/stage_test/stage2/stage2-results.json) (`all_passed: true`).

---

## Stage 3 — Cooperative Scale-to-Zero, Deep Freeze & Wire Multiplexer (`COMPLETED`)
- [x] **`EXP-B1`..`EXP-B5` (`UQR-08`, `ADR-007` — [stage_test/stage3/pkg/scalezero/exp_b.go](file:///workspaces/air/stage_test/stage3/pkg/scalezero/exp_b.go)):**
  - [x] **`EXP-B1` (`pass: true`):** Warm local NVMe wake (`10` tables `FirstQueryTime p50 = 149.88 ms`, `500` tables `FirstQueryTime p50 = 145.39 ms`), dirty `SIGKILL` crash-recovery wake (`FirstQueryTime p50 = 238.64 ms`), and Cold `DEEP_FROZEN` Cloudflare R2 hydration (`pg_verifybackup` + restore `FirstQueryTime p50 = 2,913.72 ms`, `deep_freeze_verified: true`).
  - [x] **`EXP-B2` (`pass: true`):** Cooperative drain vetoed by `LISTEN` (with live `NOTIFY` delivery `listen_notify_delivered: true`), `CREATE TEMP TABLE`, `pg_advisory_lock`, `PREPARE TRANSACTION` (2PC), and `maintenance_lock`; `25/25` sleep/wake loops verified `pg_controldata` `shut down` with `0` `ECONNRESET`.
  - [x] **`EXP-B3` (`pass: true`):** `1,000/1,000` concurrent `T_idle - 1ms` shutdown-boundary races succeeded with **`0` `ECONNRESET`**.
  - [x] **`EXP-B4` & `EXP-B5` (`pass: true`):** `100`, `500`, and `1,000` concurrent wake-storm clients coalesced via `singleflight` into exactly **`1` PG spawn per storm** (`pg_spawns = 1`), overflow cleanly rejected with `SQLSTATE 53300` (`0` `other_errors`), and `max_concurrent_starts = 5` semaphore protected sibling OLTP (`p99 = 33.60 ms` during storm).
- [x] **`EXP-E1`..`EXP-E4` (`UQR-09`, `ADR-009` — [stage_test/stage3/pkg/wire/virtualizer.go](file:///workspaces/air/stage_test/stage3/pkg/wire/virtualizer.go) & [stage_test/stage3/pkg/compatibility/exp_e.go](file:///workspaces/air/stage_test/stage3/pkg/compatibility/exp_e.go)):**
  - [x] **`EXP-E1` (`pass: true`):** Stage-3 `pgproto3` session-state virtualizer verified `pgx` Extended Query prepared-statement hashing & backend re-injection across pooled connections, `pgx.Batch` pipeline mode, DDL `0A000`/`26000` invalidation & re-bind, `psql` + `COPY FROM STDIN` / `COPY TO STDOUT`, irreversible session auto-pinning (`LISTEN`/`TEMP`/`SET`), `CancelRequest` (`80877102`) forwarding (`150.43 ms`), and `PgBouncer 1.26.1` compatibility.
  - [x] **`EXP-E2` (`pass: true`):** `2 GB` `StartupMessage` length-bomb rejected, `SNI != tenant` spoof rejected (`28000`), `Slowloris` stalled pre-auth closed cleanly, and local `bbolt` WAL ledger (`18` events) + `5x` Queue replay (`72` deduplicated) achieved **`0%` duplicate billing** (`23 == 23` RUs).
  - [x] **`EXP-E3` & `EXP-E4` (`pass: true`):** `5,000` concurrent idle client connections held at **`10.43 KB/conn`** (`< 20 KB` gate), and `20` slow `COPY OUT` readers streaming `92 MB` completed with **`1.20 MB` Gateway heap delta** (`< 30 MB` gate) via per-chunk backpressure.
- [x] **Stage 3 Artifacts:** Recorded in [stage3-results.json](file:///workspaces/air/stage_test/stage3/stage3-results.json) (`all_passed: true`).

---

## Stage 4 — Principal Database Engineer Production Readiness Verification & Remaining Risk Elimination (`COMPLETED`)

> **Specification & Audit Report:** [stage4-production-readiness.md](file:///workspaces/air/stage4-production-readiness.md)
> **Empirical Results Artifact:** [stage4-results.json](file:///workspaces/air/stage_test/stage4/stage4-results.json) (`all_passed: true`)

### Stage 0–3 Independent Verification & P0/P1 Risk Register (`S4-RISK-01`..`S4-RISK-09`)
- [x] **Independent Code & Evidence Audit of Stages 0–3:** Audited all implementation packages and JSON results across `stage_test/stage0/`, `stage_test/stage1/`, `stage_test/stage2/`, and `stage_test/stage3/`, separating genuinely verified mechanics from simulated/unimplemented paths in [stage4-production-readiness.md](file:///workspaces/air/stage4-production-readiness.md).
- [x] **`S4-RISK-01` (P0 — WS 1: Distributed Leases, Monotonic Watchdog & R2 Epoch CAS Fencing — [stage_test/stage4/pkg/fencing/r2_cas.go](file:///workspaces/air/stage_test/stage4/pkg/fencing/r2_cas.go)):**
  - [x] Implemented `CLOCK_MONOTONIC` lease watchdog inside `cell.Agent` that sets `default_transaction_read_only = on` and sends `SIGKILL` to partitioned primary `postgres` trees (`measured_watchdog_kill_ms = 2,000 ms`) well before the Control Plane promotion barrier (`3,200 ms`, `disjoint_safety_margin_ms = 1,817 ms`, `split_brain_commits_after_promotion = 0`).
  - [x] Implemented Cloudflare R2 conditional `If-Match` `ETag` & `fencing_epoch` CAS on `manifest.json` (`EXP-S4-1A` & `EXP-S4-1B` passed: `zombie_raw_if_match_412_rejected_by_r2: true`, `r2_manifest_preserved_epoch2: true`).
- [x] **`S4-RISK-02` (P0 — WS 2: Continuous WAL Archiving, Multi-Incremental PITR & DAG Garbage Collection — [stage_test/stage4/pkg/backup/pitr_dag.go](file:///workspaces/air/stage_test/stage4/pkg/backup/pitr_dag.go)):**
  - [x] Enabled `archive_mode = on` with `zstd` + `AES-256-GCM-AAD` continuous WAL shipping to live Cloudflare R2 (`10` encrypted WAL segments archived) and `pg_available_wal_summaries()` continuity check (`10` summaries verified) + `2.2x` `statfs` staging guard (`staging_headroom_2_2x_verified: true`).
  - [x] Verified `Full (B0) -> Incr (I1) -> Incr (I2) + WAL` exact-LSN (`0/700ED70`) PITR recovery across a destructive `DROP TABLE` (`EXP-S4-2A` passed: `2,300 / 2,300` rows recovered, `pg_amcheck_passed_post_pitr: true`).
  - [x] Implemented and verified Mark-and-Sweep DAG Garbage Collector preserving active ancestor chains (`B0`, `I1`) while sweeping unreferenced expired chains (`EXP-S4-2B` passed).
- [x] **`S4-RISK-03` (P1 — WS 3: Fly Volume Quota Guard, `.cell_ballast` Watermarks & `XX001` Corruption Recovery — [stage_test/stage4/pkg/isolation/isolation_soak.go](file:///workspaces/air/stage_test/stage4/pkg/isolation/isolation_soak.go)):**
  - [x] Integrated sub-second tenant storage quota enforcement (`quota_detection_latency_ms = 15 ms` `< 500 ms` gate, `SQLSTATE 25006`) + `.cell_ballast` (`75%/85%/92%`, truncated `64 MB -> 32 MB`) emergency truncation directly into `cell.Agent` (`EXP-S4-3A` passed).
  - [x] Injected real heap block corruption (`base/5/16385`), trapped `SQLSTATE XX001` (`data_corrupted`), quarantined corrupt instance, and auto-restored `1,000 / 1,000` rows onto replacement storage from R2 with `pg_amcheck` verification (`EXP-S4-3B` passed).
- [x] **`S4-RISK-04` (P0 — WS 4 & 10: Defense-in-Depth Multi-Tenant Isolation & Red-Team Penetration Suite — [stage_test/stage4/pkg/isolation/isolation_soak.go](file:///workspaces/air/stage_test/stage4/pkg/isolation/isolation_soak.go)):**
  - [x] Enforced `shared_preload_libraries = 'supautils'`, least-privilege non-superuser tenant roles (`NOSUPERUSER NOCREATEDB NOCREATEROLE`), `REVOKE CONNECT ON DATABASE template1 FROM PUBLIC`, `scram-sha-256`, OS UID `10001 -> 10002` `0700` isolation, and `temp_file_limit = '32MB'` (`SQLSTATE 53400` without artificial `statement_timeout`) across all 7 red-team attack vectors (`EXP-S4-4A` passed: `0` victim dropped queries, `victim_latency_ratio = 0.97x`).
- [x] **`S4-RISK-05` (P1 — WS 5: Lifecycle Leak Soak & Live `pg_prepared_xacts` Drain Guard — [stage_test/stage4/pkg/isolation/isolation_soak.go](file:///workspaces/air/stage_test/stage4/pkg/isolation/isolation_soak.go)):**
  - [x] Fixed `agent.lock` FD leak (`releaseFlockLocked`) and TCP port leak (`allocOrReusePort`) in `cell.Agent` across `25` rapid wake/query/stop cycles (`0` net leaked FDs, `7 -> 7` `/proc/self/fd`, `1` reused port) and enforced live SQL `pg_prepared_xacts` + advisory lock checks before drain (`EXP-S4-5A` passed).
- [x] **`S4-RISK-06` (P1 — WS 6: Live Tenant Migration & Major-Version Logical Cutover — [stage_test/stage4/pkg/migration/live_migrator.go](file:///workspaces/air/stage_test/stage4/pkg/migration/live_migrator.go)):**
  - [x] Implemented and verified live logical replication (`PUBLICATION`/`SUBSCRIPTION`) + `pg_sequences` `setval()` synchronization + sub-second read-only pause/cutover (`cutover_pause_duration_ms = 38 ms` `< 1,000 ms` gate) under continuous client writes with **`0` lost rows** (`230 / 230`) and **`0` sequence collisions** (`EXP-S4-6A` passed).
- [x] **`S4-RISK-07` (P0 — WS 7: Zero-Shared-Memory Cross-Region DR & Versioned KEK Rotation — [stage_test/stage4/pkg/migration/live_migrator.go](file:///workspaces/air/stage_test/stage4/pkg/migration/live_migrator.go) & [stage_test/stage4/pkg/crypto/envelope.go](file:///workspaces/air/stage_test/stage4/pkg/crypto/envelope.go)):**
  - [x] Persisted self-contained `WrappedDEKBundle` (`KeyVersion`, `WrappedDEK`, `WrapNonce`) inside R2 `manifest.json`, verified cold `iad -> ord` cross-region restore with `0` bytes of shared RAM/disk (`500 / 500` rows restored at fenced Epoch 2), and verified online KEK rotation (`kek-v1 -> kek-v2`) with `0` R2 backup bytes re-uploaded and `kek-v1` revoked (`EXP-S4-7A` passed).
- [x] **`S4-RISK-08` (P0 — WS 8 & 10: Hardened Wire Virtualizer Auth, `COPY FROM STDIN`, `Flush`, `DISCARD ALL` & `DEALLOCATE` — [stage_test/stage4/pkg/wire/virtualizer.go](file:///workspaces/air/stage_test/stage4/pkg/wire/virtualizer.go)):**
  - [x] Enforced client password authentication (`28P01` on invalid credentials), least-privilege `app_user` backend connections (`usesuper = false`), bidirectional `COPY FROM STDIN` (`500` rows streamed), non-blocking mid-pipeline `Flush ('H')`, `DISCARD ALL`, `DEALLOCATE` + re-`PREPARE` across both Simple & Extended Query protocols, and live `pg_prepared_xacts` 2PC drain veto (`EXP-S4-8A` passed).
- [x] **`S4-RISK-09` (P1 — WS 9: Kernel `/proc/<pid>/smaps_rollup` `PSS` Accounting & Empirical FinOps Sensitivity Engine — [stage_test/stage4/pkg/capacity/pss_finops.go](file:///workspaces/air/stage_test/stage4/pkg/capacity/pss_finops.go)):**
  - [x] Measured true Linux kernel `PSS` (`24.54 MB` idle, `38.46 MB` loaded) and `USS` (`15.97 MB` idle, `23.02 MB` loaded) vs naive summed `VmRSS` (`68.52 MB` idle, `152.13 MB` loaded — proving a **`3.96x` RSS overcount ratio**) across PG17 process trees, and computed SKU safe capacities and unit COGS sensitivity matrices across `5%, 10%, 25%, 50%` active ratios (`EXP-S4-9A` passed, `gross_margin_pct_at_5usd_price = 90.2% – 97.6%`).
- [x] **Stage 4 Artifacts:** Recorded in [stage4-results.json](file:///workspaces/air/stage_test/stage4/stage4-results.json) (`all_passed: true`, all 9 gates `GATE-01`..`GATE-09` passed).

---

## Critical Follow-up Audit & Remediation (`FUP-P0-01`..`FUP-P0-04`, `FUP-P1-01`..`FUP-P1-04` — `COMPLETED`)

> **Audit Deliverables:** [09-critical-followup-findings.md](file:///workspaces/air/audit/09-critical-followup-findings.md) through [16-updated-production-verdict.md](file:///workspaces/air/audit/16-updated-production-verdict.md)
> **Updated Verdict:** `NO-GO FOR GENERAL AVAILABILITY` / `CONDITIONAL GO FOR CONTROLLED STAGING CANARY`

- [x] **`FUP-P0-01` (`NewControlPlane()` Metadata Preservation & Migrations — [stage_test/stage1/pkg/control/control.go](file:///workspaces/air/stage_test/stage1/pkg/control/control.go)):** Removed unconditional `os.RemoveAll(metaDir)`, added `PG_VERSION` detection, `ensureMetaPostgresRunning()` (dead-PID `postmaster.pid` cleanup only), `ErrMetadataCorrupted` fail-closed guard, and `schema_migrations` (`v1`, `v2`).
- [x] **`FUP-P0-02` (`ClaimCluster()` Path Traversal & Command Injection — [stage_test/stage4/pkg/cell/agent.go](file:///workspaces/air/stage_test/stage4/pkg/cell/agent.go)):** Added `ValidateTenantID()` + `safeTenantSubpath()` (canonical `EvalSymlinks`, `filepath.Rel`, `os.Lstat` symlink rejection) across all tenant FS operations and replaced `su - postgres -c` shell interpolation in `StopTenantClean()` with `runPostgresCmd()`.
- [x] **`FUP-P0-03` (`ReadoptRunningTenants()` `flock` & Epoch Ownership — [stage_test/stage4/pkg/cell/agent.go](file:///workspaces/air/stage_test/stage4/pkg/cell/agent.go)):** Enforced `acquireFlockLocked()` error handling (never register on busy lock), `postmaster.pid` PGDATA + `/proc/<pid>/exe` + `/proc/<pid>/stat` `starttime` (`agent.pidmeta`) verification, and `SetEpochChecker()` reconciliation.
- [x] **`FUP-P0-04` (`ProvisionDatabase()` Cross-Tenant Idempotency & Password Encryption — [stage_test/stage1/pkg/control/control.go](file:///workspaces/air/stage_test/stage1/pkg/control/control.go)):** Enforced `tenant_id` ownership + SHA-256 `payload_hash` (`ErrIdempotencyConflict`), encrypted `app_password_enc` at rest via `EnvelopeKMS`, redacted `response_json`, and added per-tenant + refcounted key locks.
- [x] **`FUP-P1-01` (Provisioning Crash Consistency — [stage_test/stage1/pkg/control/control.go](file:///workspaces/air/stage_test/stage1/pkg/control/control.go)):** Implemented durable `provisioning_ops` state machine (`REQUESTED -> CLAIMED -> ACTIVATING -> ACTIVE / ROLLED_BACK`), `RecoverProvisioningOperations()` on startup, and `SetFaultHook()` fault injection across all transitions.
- [x] **`FUP-P1-02` (WAL Durability — [stage_test/stage4/pkg/backup/pitr_dag.go](file:///workspaces/air/stage_test/stage4/pkg/backup/pitr_dag.go)):** `DrainAndEncryptWALSpoolToR2WithFault()` fails closed (`ErrR2ClientRequired`), validates WAL/history filenames, verifies R2 `HeadObject` size, and commits fsynced `wal_upload_manifest.json` before deleting local WAL; `CombineAndReplayPITR()` enforces chain linkage, bounded `io.LimitReader`, and `--no-same-owner --no-same-permissions`.
- [x] **`FUP-P1-03` (Process Identity & Supervision — [stage_test/stage4/pkg/cell/agent.go](file:///workspaces/air/stage_test/stage4/pkg/cell/agent.go)):** Added recursive descendant traversal (`collectProcessTree`), cgroup v2 (`air_tenant_<id>`) attachment/kill, and `waitForPIDsExit` before `releaseFlockLocked()`.
- [x] **`FUP-P1-04` (Drain Fail-Open & Backend Session Isolation — [stage_test/stage4/pkg/wire/virtualizer.go](file:///workspaces/air/stage_test/stage4/pkg/wire/virtualizer.go)):** `RequestDrain()` and `handleClient()` veto (`ErrDrainVetoed`) on any backend state error; added `lastClientID` tracking + `DISCARD ALL;` on cross-client `BackendConn` reuse.
- [x] **`-race` & Live PG17 Verification:** Verified all 4 test suites (`stage_test/stage1/pkg/control`, `stage_test/stage4/pkg/cell`, `stage_test/stage4/pkg/backup`, `stage_test/stage4/pkg/wire`) with `-race` against live PostgreSQL 17.12 ([15-fault-injection-results.md](file:///workspaces/air/audit/15-fault-injection-results.md)).

---

## AIR v1.0 — Single-Cell PostgreSQL 17 Foundation (Phases A–D — `COMPLETED`)

> **Phase Roadmap:** [phase.md](file:///workspaces/air/phase.md)

- [x] **Phase A — Remediation Closure & Stage Archival (`2bb5c48`):**
  - [x] Installed PostgreSQL 17.12 (`postgresql-17`, `postgresql-server-dev-17`) and compiled/installed `/usr/lib/postgresql/17/lib/supautils.so`.
  - [x] Executed all 18 tests across `stage_test/stage1` (9 PASS, 0 FAIL, 0 SKIP) and `stage_test/stage4` (9 PASS, 0 FAIL, 0 SKIP) with `go test -v -race -count=1 ./...`.
  - [x] Archived `stage0`..`stage4` into [stage_test/](file:///workspaces/air/stage_test).
- [x] **Phase B — Root Architecture & Metadata Schema v1 (`b0f9885`):**
  - [x] Created root Go module ([go.mod](file:///workspaces/air/go.mod)) and runtime configuration ([internal/config/config.go](file:///workspaces/air/internal/config/config.go)).
  - [x] Implemented canonical identifier validation and symlink-safe path containment in [internal/tenant/tenant.go](file:///workspaces/air/internal/tenant/tenant.go).
  - [x] Defined Control Plane schema v1 in [migrations/001_initial_schema.sql](file:///workspaces/air/migrations/001_initial_schema.sql).
- [x] **Phase C — Single-Cell Implementation (`b0f9885`):**
  - [x] Implemented `EnvelopeKMS` with persistent `0600` master KEK, wrapped per-tenant DEKs, and `ShredDEK` in [internal/crypto/envelope.go](file:///workspaces/air/internal/crypto/envelope.go).
  - [x] Implemented `cell.Agent` with `initdb --data-checksums`, `supautils` preload, least-privilege `app_user`, `flock` ownership, and `/proc/<pid>/stat` + `system_identifier` + `fencing_epoch` verified `ReadoptRunningTenants` in [internal/cell/agent.go](file:///workspaces/air/internal/cell/agent.go).
  - [x] Implemented `control.ControlPlane` with durable PG17 metadata store, `ErrMetadataCorrupted` fail-closed startup guard, `provisioning_ops` state machine, `RecoverProvisioningOperations`, and HTTP REST API in [internal/control/control.go](file:///workspaces/air/internal/control/control.go) and [internal/control/http.go](file:///workspaces/air/internal/control/http.go).
  - [x] Implemented `gateway.Gateway` PostgreSQL wire-protocol proxy with TLS/SNI verification, 8 KB `StartupMessage` bound, and `postgres` superuser block in [internal/gateway/gateway.go](file:///workspaces/air/internal/gateway/gateway.go).
  - [x] Created runnable single-cell server entrypoint in [cmd/air-server/main.go](file:///workspaces/air/cmd/air-server/main.go).
- [x] **Phase D — Live PostgreSQL 17 End-to-End Verification (`b0f9885`, `418e49c`):**
  - [x] Verified all 9 single-cell milestones in [tests/e2e/e2e_test.go](file:///workspaces/air/tests/e2e/e2e_test.go) (`TestAIRv1_SingleCellEndToEndMilestones` PASS with `-race -count=1`).

---

## Phase E — Control Plane Bearer Auth & Mandatory Gateway TLS (`AIR v1.1` — `COMPLETED`)

- [x] **Schema Migration ([migrations/002_api_keys.sql](file:///workspaces/air/migrations/002_api_keys.sql) & [internal/control/control.go](file:///workspaces/air/internal/control/control.go)):**
  - [x] Add migration version `2` creating `api_keys` (`key_id`, `key_sha256 UNIQUE`, `account_id`, `description`, `created_at`, `revoked_at`).
  - [x] Implement `CreateAPIKey`, `SeedAPIKey`, `AuthenticateAPIKey`, and `RevokeAPIKey` on `ControlPlane`.
  - [x] Add `--bootstrap-api-key` (`AIR_BOOTSTRAP_API_KEY`) and `--bootstrap-account-id` (`AIR_BOOTSTRAP_ACCOUNT_ID`) in [internal/config/config.go](file:///workspaces/air/internal/config/config.go) and seed on `air-server` startup when configured.
- [x] **HTTP Bearer Authentication Middleware ([internal/control/http.go](file:///workspaces/air/internal/control/http.go)):**
  - [x] Enforce `Authorization: Bearer <token>` on all `/v1/*` routes (`POST /v1/databases`, `GET /v1/databases/{id}`, `DELETE /v1/databases/{id}`, `DELETE /v1/api-keys/{id}`).
  - [x] Derive `accountID` from the verified API key; return `401 Unauthorized` on missing/invalid/revoked token and `403 Forbidden` if `X-Account-ID` header is provided and mismatches the token's `account_id`.
- [x] **Gateway Mandatory TLS & Certificate Loading ([internal/gateway/gateway.go](file:///workspaces/air/internal/gateway/gateway.go)):**
  - [x] Add `--require-tls` (`AIR_REQUIRE_TLS`), `--tls-cert` (`AIR_TLS_CERT`), and `--tls-key` (`AIR_TLS_KEY`) to `config.Config` and `gateway.GatewayOptions`.
  - [x] Reject non-TLS `StartupMessage` connections with `SQLSTATE 28000` (`"SSL/TLS connection is required"`) when `RequireTLS` is enabled.
- [x] **`supautils` v4.0.0 & `app_user` Defense-in-Depth Hardening ([internal/cell/agent.go](file:///workspaces/air/internal/cell/agent.go)):**
  - [x] Configure `supautils.superuser = 'postgres'`, `supautils.privileged_role = 'app_user'`, `supautils.privileged_extensions = 'pg_stat_statements,pg_trgm'`, `supautils.reserved_roles = 'postgres'`, `supautils.reserved_memberships = 'pg_read_server_files,pg_write_server_files,pg_execute_server_program,postgres'`, `supautils.restrict_extension_versions = 'error'`, and `temp_file_limit = 32MB`.
  - [x] Enforce `NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT` on `app_user`, revoke `PUBLIC` access on `template1` and `postgres`, and reject TCP `postgres` connections in `pg_hba.conf`.
- [x] **Phase E Live PG17 Verification ([tests/e2e/e2e_test.go](file:///workspaces/air/tests/e2e/e2e_test.go)):**
  - [x] Verified unauthenticated/revoked token (`401`), cross-account token isolation (`403`), `sslmode=disable` rejection (`SQLSTATE 28000`) under `RequireTLS: true`, and full `supautils` user hardening (`TestAIRv1_1_PhaseE_BearerAuthAndMandatoryGatewayTLS` & `TestAIRv1_1_SupautilsUserHardening` PASS with `go test -v -race -count=1 ./...`).

---

## Phase F — Encrypted WAL Durability, CAS-Fenced Backups & Streaming-Safe PITR (`AIR v1.2` — `COMPLETED`)

- [x] **Promote `internal/fencing` & `internal/backup`:**
  - [x] Port `TenantFenceManifest`, `ReadManifestWithETag`, `PutManifestCAS` (`If-None-Match` / `If-Match` HTTP 412), and `StartLeaseWatchdog` (`CLOCK_MONOTONIC`) into `internal/fencing`.
  - [x] Port `DrainAndEncryptWALSpoolToR2WithFault`, `RunEncryptedBackupStep`, `CombineAndReplayPITR`, and `GarbageCollectBackupDAG` into `internal/backup`.
- [x] **Wire `RunEncryptedBackupStep` to `PutManifestCAS`:**
  - [x] Atomically commit backup metadata and `WrappedDEKBundle` via `PutManifestCAS`, aborting on `ErrStaleFencingEpoch` or HTTP 412 `ErrCASPrecondition`.
- [x] **Streaming `archive/tar` Validation & Uncompressed Byte Caps in `CombineAndReplayPITR`:**
  - [x] Extract backup tarballs via Go `archive/tar` + `io.LimitReader` (enforcing per-entry and total uncompressed byte caps).
  - [x] Reject path traversal (`..`, leading `/`), symlinks, hardlinks, device nodes, and FIFOs (`0700` dirs / `0600` files).
- [x] **Control Plane Backup & Restore Endpoints ([internal/control/http.go](file:///workspaces/air/internal/control/http.go)):**
  - [x] Expose `POST /v1/databases/{id}/backups` and `POST /v1/databases/{id}/restore`.
- [x] **Phase F Live PG17 Verification:**
  - [x] Verify Full (`B0`) + Incremental (`I1`) backup + WAL archive + LSN PITR recovery, stale-epoch CAS rejection, and malicious tar rejection with `go test -v -race -count=1 ./...`.

---

## Phase G — Per-Tenant `cgroup v2` Resource Isolation & External HA Metadata DB (`AIR v1.3` — `COMPLETED`)

- [x] **Per-Tenant `cgroup v2` Supervision ([internal/cell/agent.go](file:///workspaces/air/internal/cell/agent.go)):**
  - [x] Configure per-tenant `cgroup v2` slice (`air_tenant_<id>`) with `memory.high`, `memory.max`, `memory.swap.max = 0`, `cpu.max`, and `pids.max` when delegated cgroupfs is writable.
  - [x] Enforce `cgroup.kill = 1` + recursive `/proc` descendant reaping (`collectProcessTree`) + `waitForPIDsExit` before `releaseFlockLocked`.
- [x] **External HA Metadata PostgreSQL Support ([internal/config/config.go](file:///workspaces/air/internal/config/config.go) & [internal/control/control.go](file:///workspaces/air/internal/control/control.go)):**
  - [x] Add `--meta-dsn` (`AIR_META_DSN`) so `NewControlPlane` can use an external HA PostgreSQL cluster without bootstrapping a local single-node `MetaDir`.
- [x] **Phase G Live PG17 Verification:**
  - [x] Verify external `MetaDSN` Control Plane operation and descendant process termination / cgroup lifecycle with `go test -v -race -count=1 ./...`.

---

## Phase H — Transaction-Mode Wire Virtualizer / Pooler & Cooperative Scale-to-Zero (`AIR v2.0` — `COMPLETED`)

- [x] **Transaction-Mode Wire Pooler ([internal/gateway/pooler.go](file:///workspaces/air/internal/gateway/pooler.go)):**
  - [x] Integrate transaction-mode connection multiplexing from `stage_test/stage4/pkg/wire/virtualizer.go` into `internal/gateway`.
  - [x] Enforce `DISCARD ALL;` on cross-client backend reuse (`lastClientID`), Extended Query prepared-statement tracking, `COPY FROM STDIN` / `COPY TO STDOUT`, and session pinning (`LISTEN`, temp tables, advisory locks).
- [x] **Cooperative Drain Guards & Scale-to-Zero ([internal/cell/agent.go](file:///workspaces/air/internal/cell/agent.go) & [internal/control/control.go](file:///workspaces/air/internal/control/control.go)):**
  - [x] Enforce fail-closed drain guards (`ErrDrainVetoed` on active tx, `pg_prepared_xacts`, session pin, or backend error).
  - [x] Implement idle tenant suspend (`ACTIVE -> SUSPENDED`) and `singleflight` wake-on-connect in the Gateway.
- [x] **Phase H Live PG17 Verification ([tests/e2e/phase_h_test.go](file:///workspaces/air/tests/e2e/phase_h_test.go)):**
  - [x] Verify pooled concurrency, session isolation, drain vetoes, and `singleflight` wake storms against PostgreSQL 17 with `go test -v -race -count=1 ./...`.

---

## Phase I — Custom Storage Manager (`air_smgr`) & Encrypted Pageserver (`AIR v2.1` — `COMPLETED`)

- [x] **PostgreSQL 17 Custom `f_smgr` Extension & Patch ([patches/pg17-smgr-hook.patch](file:///workspaces/air/patches/pg17-smgr-hook.patch) & [ext/air_smgr/](file:///workspaces/air/ext/air_smgr)):**
  - [x] Implement `air_smgr.c`, `lfc.c` (CLOCK-Pro / Sharded LRU Local File Cache with `pread`/`pwrite` and CRC32C verification), and `ps_client.c` (persistent Unix domain socket client with `SO_RCVTIMEO`/`SO_SNDTIMEO`, automatic reconnect, `OpGetRelSize`, `OpGetPageAtLSN`, and `OpGetPageBatch`).
  - [x] Verify `air_smgr` against a mock Pageserver (`ext/air_smgr/test_smgr_mock.py` — single page, 32-block batch `readv`, LFC hit after socket close, write-through `extend`/`writev`, and unlogged local-disk bypass).
- [x] **Encrypted Multi-Tenant Pageserver ([internal/pageserver/](file:///workspaces/air/internal/pageserver)):**
  - [x] Implement binary wire protocol & framing ([internal/pageserver/protocol.go](file:///workspaces/air/internal/pageserver/protocol.go)), PG17 WAL record & Full-Page Image (`BKPIMAGE`) decoder + incremental `WALStreamParser` ([internal/pageserver/walparser.go](file:///workspaces/air/internal/pageserver/walparser.go)), and `AES-256-GCM-AAD` + `zstd` 64 KB block-indexed immutable `.layer` writer/reader ([internal/pageserver/layer.go](file:///workspaces/air/internal/pageserver/layer.go)).
  - [x] Implement LSN-aware page reconstruction (`GetPageAtLSN`, `GetPageBatch`, `GetRelSize`) and L0 $\rightarrow$ L1 compaction ([internal/pageserver/readpath.go](file:///workspaces/air/internal/pageserver/readpath.go)), streaming `pglogrepl` `START_REPLICATION` receiver ([internal/pageserver/walreceiver.go](file:///workspaces/air/internal/pageserver/walreceiver.go)), LRU local disk cache ([internal/pageserver/diskcache.go](file:///workspaces/air/internal/pageserver/diskcache.go)), and `BootstrapTenantFromPGData` + Unix socket server ([internal/pageserver/service.go](file:///workspaces/air/internal/pageserver/service.go)).
- [x] **Cell Agent, Control Plane & Server Wiring ([internal/cell/agent.go](file:///workspaces/air/internal/cell/agent.go), [internal/control/control.go](file:///workspaces/air/internal/control/control.go) & [cmd/air-server/main.go](file:///workspaces/air/cmd/air-server/main.go)):**
  - [x] Wire `SetPageserverConfig` and `WriteTenantConfigs` to emit `shared_preload_libraries = 'supautils,air_smgr'` and `air_smgr.*` GUCs ([internal/cell/pageserver_config_test.go](file:///workspaces/air/internal/cell/pageserver_config_test.go)).
  - [x] Wire `ensurePageserverBootstrapAndWAL`, `stopWALReceiver`, and `FlushTenant` into `ProvisionDatabase`, `SuspendTenant`, `WakeTenant`, `DeleteDatabase`, and `Close()`.
- [x] **Phase I Live PG17 End-to-End Verification ([internal/control/pageserver_integration_test.go](file:///workspaces/air/internal/control/pageserver_integration_test.go)):**
  - [x] Verify `TestNeonServerlessLifecycleWithPageserver` (`ProvisionDatabase` $\rightarrow$ `BootstrapTenantFromPGData` $\rightarrow$ live `WALReceiver` streaming $\rightarrow$ Unix socket `OpGetRelSize` & `OpGetPageAtLSN` $\rightarrow$ `SuspendTenant` L0 layer flush $\rightarrow$ `WakeTenant` WAL resume & row verification).

---

## Phase J — True Stateless Compute Eviction & Fast Metadata-Skeleton Wake (`AIR v2.2` — `COMPLETED`)

- [x] **Lightweight Metadata-Skeleton Snapshot on Suspend ([internal/pageserver/skeleton.go](file:///workspaces/air/internal/pageserver/skeleton.go), [internal/cell/agent.go](file:///workspaces/air/internal/cell/agent.go) & [internal/control/control.go](file:///workspaces/air/internal/control/control.go)):**
  - [x] Capture only non-relation compute state (`global/pg_control`, `global/pg_filenode.map`, `pg_xact/`, `pg_multixact/`, `pg_logical/`, `PG_VERSION`, per-DB `pg_filenode.map` / `PG_VERSION`, and active WAL segment tail) into a `zstd` + `AES-256-GCM-AAD` snapshot (`SaveMetadataSkeleton`), skipping all `base/` and `global/` relation fork files stored in the Pageserver.
  - [x] Evict local compute `PGDATA` (including `base/` relation fork files) on `SuspendTenant` after `StopTenantClean` and `Pageserver.FlushTenant` complete when `air_smgr` or `R2Primary` is active.
- [x] **Sub-Second Stateless Compute Hydration on Wake ([internal/pageserver/skeleton.go](file:///workspaces/air/internal/pageserver/skeleton.go) & [internal/control/control.go](file:///workspaces/air/internal/control/control.go)):**
  - [x] Reconstruct the minimal `PGDATA` directory skeleton on `WakeTenant` (`RestoreMetadataSkeleton`) in milliseconds (`~80 ms`, `1.89 MB` compressed) without downloading relation files from R2.
- [x] **Phase J Verification ([internal/pageserver/skeleton_test.go](file:///workspaces/air/internal/pageserver/skeleton_test.go) & [internal/control/pageserver_integration_test.go](file:///workspaces/air/internal/control/pageserver_integration_test.go)):**
  - [x] Verified `TestSaveAndRestoreMetadataSkeleton` and `TestNeonServerlessLifecycleWithPageserver` (`SuspendTenant` $\rightarrow$ `EvictTenantLocalData` $\rightarrow$ `RestoreMetadataSkeleton` in `80.32 ms`, `0` relation files on local disk, Pageserver serving relation blocks over Unix socket).

---

## Phase K — Live Patched PostgreSQL 17.12 (`smgr_hook`) + `air_smgr.so` End-to-End Verification (`AIR v2.3` — `NEXT`)

- [ ] **Build & Install Patched PG 17.12 + `air_smgr.so` ([Dockerfile](file:///workspaces/air/Dockerfile) & [ext/air_smgr/Makefile](file:///workspaces/air/ext/air_smgr/Makefile)):**
  - [ ] Build PostgreSQL 17.12 with [patches/pg17-smgr-hook.patch](file:///workspaces/air/patches/pg17-smgr-hook.patch) and install `air_smgr.so` + `supautils.so`.
- [ ] **Zero-Local-Heap SQL Execution Verification:**
  - [ ] Run end-to-end tests where tenant relation files under `base/<dbOid>/<relNode>` are unlinked/absent on local disk and verify `SELECT`, `INSERT`, `UPDATE`, `VACUUM`, and index scans execute seamlessly through `air_smgr.so` $\rightarrow$ `LFC` $\rightarrow$ `Pageserver` Unix socket.

---

## Phase L — Background Compaction Daemon, R2 Layer Offload & Pageserver Observability (`AIR v2.4`)

- [ ] **Background L0 $\rightarrow$ L1 Compaction & GC Loop ([internal/pageserver/service.go](file:///workspaces/air/internal/pageserver/service.go) & [internal/pageserver/readpath.go](file:///workspaces/air/internal/pageserver/readpath.go)):**
  - [ ] Run periodic background compaction triggered when a tenant's L0 layer count exceeds threshold (e.g., $\ge 8$ L0 layers), merging into key-partitioned L1 layers and removing superseded L0 files.
- [ ] **Cloudflare R2 Layer Upload & On-Demand Range Hydration ([internal/pageserver/service.go](file:///workspaces/air/internal/pageserver/service.go) & [internal/pageserver/diskcache.go](file:///workspaces/air/internal/pageserver/diskcache.go)):**
  - [ ] Upload sealed `.layer` files to R2 with `PutManifestCAS` epoch fencing and fetch evicted layers on demand via `DiskCache` + R2 range reads when local NVMe cache reaches capacity.
- [ ] **Pageserver OpenMetrics Exporter ([internal/control/http.go](file:///workspaces/air/internal/control/http.go) & [internal/pageserver/service.go](file:///workspaces/air/internal/pageserver/service.go)):**
  - [ ] Export `pageserver_getpage_latency_seconds`, `pageserver_batch_pages_total`, `pageserver_l0_layers_count`, `pageserver_compactions_total`, `pageserver_diskcache_bytes`, and `pageserver_wal_ingest_lsn` on `/metrics`.
