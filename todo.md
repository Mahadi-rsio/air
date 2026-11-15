# Air Platform Implementation & Verification Tracker (`todo.md`)

> **Architecture Reference:** [plan.md](file:///workspaces/air/plan.md) · [resolution-register.md](file:///workspaces/air/resolution-register.md) · [issue_resolution.md](file:///workspaces/air/issue_resolution.md)

---

## Stage 0 — Gating PoCs & Kernel / Cloud Verification (`COMPLETED`)
- [x] **Docker BuildKit Caching:** Configured `--mount=type=cache` across [stage0/Dockerfile](file:///workspaces/air/stage0/Dockerfile), [mvp/cell/Dockerfile](file:///workspaces/air/mvp/cell/Dockerfile), [mvp/gateway/Dockerfile](file:///workspaces/air/mvp/gateway/Dockerfile), and [stage1/Dockerfile](file:///workspaces/air/stage1/Dockerfile).
- [x] **`EXP-A1` (`UQR-01`, `UQR-02`):**
  - [x] Probe live Fly.io `6.12.116-fly` kernel (`ext4`, `PR_SET_CHILD_SUBREAPER`, `unshare --mount --pid --fork --net`, `/dev/loop0..4`, `/dev/shm`).
  - [x] Pre-warmed two-phase `initdb --data-checksums` pool (`100%` unique `system_identifier`s, `0.141–0.194 ms` claim latency vs `<15ms` gate).
  - [x] Background `nice -n 19` refill under sibling OLTP (`0` errors, `p50 = 28.17 ms`).
  - [x] `pg_test_fsync` NVMe baseline (`fdatasync` `2,771.8 ops/sec` / `361 usecs/op`).
- [x] **`EXP-C1` & `UQR-06` (`ADR-005`):**
  - [x] Compile `supautils` v4.0.0 against PostgreSQL 17 (`17.9`) in [stage0/supautils/](file:///workspaces/air/stage0/supautils/).
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
- [x] **Stage 0 Artifacts & Git Push:** Recorded in [stage0-results.json](file:///workspaces/air/stage0/stage0-results.json) (`all_passed: true`) and pushed to `origin/main`.

---

## Stage 1 — Core Reliability MVP (`COMPLETED`)
- [x] **Envelope Encryption KMS ([stage1/pkg/crypto/envelope.go](file:///workspaces/air/stage1/pkg/crypto/envelope.go)):**
  - [x] 256-bit Master `KEK` + per-tenant wrapped 256-bit `DEK`.
  - [x] `AES-256-GCM` encryption bound to `AAD = tenant_id|epoch|object_key` (`SI-4`).
  - [x] Instant GDPR `ShredDEK(tenantID)` crypto-shredding (`Flow 12`).
- [x] **Cell Agent Daemon ([stage1/pkg/cell/agent.go](file:///workspaces/air/stage1/pkg/cell/agent.go) & [stage1/cmd/pg-cell-agent/main.go](file:///workspaces/air/stage1/cmd/pg-cell-agent/main.go)):**
  - [x] `PR_SET_CHILD_SUBREAPER` + `64 MB` `.cell_ballast` + boot cleanup of `/vol/pool/staging/*`.
  - [x] Exclusive `flock(LOCK_EX | LOCK_NB)` on `agent.lock` + live `postgres` PID re-adoption via `postmaster.pid` and `/proc/<pid>/comm` on agent restart (`ADR-006`).
  - [x] Pre-warmed two-phase `initdb --data-checksums` pool with background `nice -n 19` refill and bounded on-demand `initdb` fallback (`ADR-001`).
  - [x] `SCRAM-SHA-256` secret injection via `0600` ephemeral file (`0` CLI/env secrets) + `supautils` v4.0.0 + `PgBouncer 1.21+` (`pool_mode = transaction`, `max_prepared_statements = 200`).
  - [x] PG17 Full & `--incremental` backup + `pg_verifybackup` + `zstd` + `AES-256-GCM-AAD` R2 upload & `pg_combinebackup` restore.
- [x] **Control Plane Service ([stage1/pkg/control/control.go](file:///workspaces/air/stage1/pkg/control/control.go)):**
  - [x] PostgreSQL 17 Metadata DB (`clusters`, `idempotency_records`, `usage_ledger`, `audit_events`) enforcing `UNIQUE(tenant_id)` and `UNIQUE(system_identifier)`.
  - [x] Idempotent `ProvisionDatabase` (`Idempotency-Key`), `DeleteDatabase` (7-day `SOFT_DELETED` grace + `immediate=true` crypto-shredding), `IngestUsageBatch` (`ON CONFLICT DO NOTHING`), and `ReconcileOnce` orphan directory sweeper.
- [x] **Wire-Protocol Gateway ([stage1/pkg/gateway/gateway.go](file:///workspaces/air/stage1/pkg/gateway/gateway.go)):**
  - [x] `SSLRequest` (`80877103`) + wildcard TLS (`*.db.air.local`), `5s` pre-auth deadline, and `8 KB` `StartupMessage` length bomb fence (`FMEA-16`).
  - [x] Cryptographic `SNI == Startup tenant` binding (`SQLSTATE 28000` on spoof, `FMEA-17`) and `SQLSTATE 57P03` gate on soft-deleted DBs.
  - [x] 5-byte frame scanner + 32 KB `sync.Pool` streaming with `30s` `SetWriteDeadline` backpressure + local `bbolt` WAL usage ledger persistence (`ADR-009`, `ADR-012`).
- [x] **Execute & Verify Stage 1 Validation Gates ([stage1/cmd/stage1-runner/main.go](file:///workspaces/air/stage1/cmd/stage1-runner/main.go) -> [stage1-results.json](file:///workspaces/air/stage1/stage1-results.json)):**
  - [x] **`EXP-A2` (`pass: true`):** 100 concurrent/duplicate `Idempotency-Key` provisioning requests across 12 tenants (`88` idempotent replays, `12/12` unique `system_identifier`s, `0.087 ms` p50 / `0.123 ms` p99 pool claim latency vs `<15ms` gate, `12/12` `SCRAM-SHA-256` logins verified).
  - [x] **`EXP-A3` (`pass: true`):** Interrupted `initdb`/claiming orphan purge (`0` orphans post-reconcile) + live `pg-cell-agent` `SIGKILL` under active OLTP load (`117` queries succeeded, `0` dropped SQL queries, `3/3` `postgres` PIDs re-adopted in `154 ms`, `0` dual primaries).
  - [x] **`STAGE1-E2E` (`pass: true`):** Gateway `8 KB` length bomb & SNI spoof (`28000`) rejection, `PgBouncer 1.21+` prepared statements (`true`), `bbolt` WAL `5x` replay idempotency (`0` duplicate billing rows), R2 `AES-256-GCM-AAD` Full (`2.73 MB`) + Incr (`22.3 KB`) combine restore (`20/20` rows + `pg_amcheck` pass), AAD tamper rejection (`true`), Soft Delete (`57P03` with DEK preserved), and Hard Purge Crypto-Shredding (`true`).

---

## Stage 2 — Multi-Tenant Density, Pro Sync HA Failover & Observability (`COMPLETED`)
- [x] **`EXP-C2` & `EXP-C4` (`UQR-07`, `UQR-10` — [stage2/pkg/density/density.go](file:///workspaces/air/stage2/pkg/density/density.go)):**
  - [x] **`EXP-C2` (`pass: true`):** 4 active + 6 dormant PG17 tenant clusters (`avg_active_cluster_rss_mb = 62.31 MB`), `oom_score_adj = -900` agent shield, Tenant A hash/sort OOM bomb contained (`57014_statement_timeout`), 25-connection fork storm capped (`17` rejected with `53300`), `0` leaked `/dev/shm` bytes, `0` dropped Tenant B OLTP queries (`46/46` succeeded), and `>70%` auto-scale trigger verified across `256 MB / 2 GB / 8 GB` SKU capacity curve.
  - [x] **`EXP-C4` (`pass: true`):** Empirical `shared_buffers` (`8MB / 16MB / 32MB`) RSS & cache hit-rate curve (`0.9964–1.0000`), anti-wraparound `maintenance_lock` vetoing sleep above `100M` XID age + `VACUUM (FREEZE)` reducing `age(datfrozenxid)` to `14`, **`0.0%` Customer Wire RU variance** (`50` vs `50` RUs) on Tenant B during heavy `CREATE INDEX` + `VACUUM FREEZE` + `CHECKPOINT` (`327,680` FinOps background write bytes separated), and `66.7%` R2 Class A cost reduction (`$2.33 -> $0.78/mo`) via 15s dirty-only WAL batching (`>= 52.3%` gross margin across all 4 tiers).
- [x] **`EXP-C5`, `EXP-D2`, `EXP-D3`, `EXP-D6` ([stage2/pkg/failover/failover.go](file:///workspaces/air/stage2/pkg/failover/failover.go)):**
  - [x] **`EXP-C5` (`pass: true`):** Single-tenant `postgres` `SIGKILL` (`PID 604 -> 627`) + subreaper `wait4(-1, WNOHANG)` zombie reaping recovered in `474.69 ms`, preserving `500/500` committed rows, rolling back uncommitted row `501`, and passing `pg_amcheck`.
  - [x] **`EXP-D2` (`pass: true`):** Pro Sync Primary + Physical Standby (`synchronous_standby_names = 'FIRST 1 (standby_zb)'`) stalled commit `#51` deterministically in `SyncRep` during standby partition (`SIGSTOP`) and resumed cleanly on heal (`51/51` rows synced).
  - [x] **`EXP-D3` (`pass: true`):** Simulated R2 outage spooled `10/10` encrypted `.wal.zst.enc` segments locally in `/vol/spool/wal/` without halting SQL writes and drained `10/10` segments to live Cloudflare R2 (`air-mvp-test`) on recovery (`0` remaining spool files).
  - [x] **`EXP-D6` (`pass: true`):** Asymmetric split-brain partition verified both Standard Async monotonic `CLOCK_MONOTONIC` self-kill (`8,001.28 ms` kill `< 12,206.89 ms` Control promote, `4,205.61 ms` disjoint safety margin, **`0` divergent commits post-promote**, stale epoch R2 upload rejected + `FENCED_QUARANTINE`) and Pro Sync Standby pre-fence (`SyncRepWaitForLSN` stalled partitioned old primary write, **`0` divergent commits**).
- [x] **Observability & DR (`RISK-39..41`, `UQR-11` — [stage2/pkg/observability/observability.go](file:///workspaces/air/stage2/pkg/observability/observability.go)):**
  - [x] OpenMetrics `/metrics` exporter verified for all **22 mandatory audit signals** (`all_22_audit_metrics_verified: true`).
  - [x] All **15 Alertmanager rules** codified in [alertmanager-rules.yaml](file:///workspaces/air/stage2/config/alertmanager-rules.yaml) (`all_15_alert_rules_fired_in_fault_sim: true`).
  - [x] All **12 Disaster Recovery Runbooks** codified in [DR-01-to-12.md](file:///workspaces/air/stage2/runbooks/dr/DR-01-to-12.md) and GameDay-verified (`DR-08` corrupted WAL fail-closed, `DR-11` Master `KEK` rotation across `7` tenants with `0` R2 bytes re-uploaded, `DR-12` staggered fleet recovery capped at `5/5` max concurrent starts, and `UQR-11` `95.0%` TSDB cardinality reduction at `50,000` tenants).
- [x] **Stage 2 Artifacts:** Recorded in [stage2-results.json](file:///workspaces/air/stage2/stage2-results.json) (`all_passed: true`).

---

## Stage 3 — Cooperative Scale-to-Zero, Deep Freeze & Wire Multiplexer (`COMPLETED`)
- [x] **`EXP-B1`..`EXP-B5` (`UQR-08`, `ADR-007` — [stage3/pkg/scalezero/exp_b.go](file:///workspaces/air/stage3/pkg/scalezero/exp_b.go)):**
  - [x] **`EXP-B1` (`pass: true`):** Warm local NVMe wake (`10` tables `FirstQueryTime p50 = 149.88 ms`, `500` tables `FirstQueryTime p50 = 145.39 ms`), dirty `SIGKILL` crash-recovery wake (`FirstQueryTime p50 = 238.64 ms`), and Cold `DEEP_FROZEN` Cloudflare R2 hydration (`pg_verifybackup` + restore `FirstQueryTime p50 = 2,913.72 ms`, `deep_freeze_verified: true`).
  - [x] **`EXP-B2` (`pass: true`):** Cooperative drain vetoed by `LISTEN` (with live `NOTIFY` delivery `listen_notify_delivered: true`), `CREATE TEMP TABLE`, `pg_advisory_lock`, `PREPARE TRANSACTION` (2PC), and `maintenance_lock`; `25/25` sleep/wake loops verified `pg_controldata` `shut down` with `0` `ECONNRESET`.
  - [x] **`EXP-B3` (`pass: true`):** `1,000/1,000` concurrent `T_idle - 1ms` shutdown-boundary races succeeded with **`0` `ECONNRESET`**.
  - [x] **`EXP-B4` & `EXP-B5` (`pass: true`):** `100`, `500`, and `1,000` concurrent wake-storm clients coalesced via `singleflight` into exactly **`1` PG spawn per storm** (`pg_spawns = 1`), overflow cleanly rejected with `SQLSTATE 53300` (`0` `other_errors`), and `max_concurrent_starts = 5` semaphore protected sibling OLTP (`p99 = 33.60 ms` during storm).
- [x] **`EXP-E1`..`EXP-E4` (`UQR-09`, `ADR-009` — [stage3/pkg/wire/virtualizer.go](file:///workspaces/air/stage3/pkg/wire/virtualizer.go) & [stage3/pkg/compatibility/exp_e.go](file:///workspaces/air/stage3/pkg/compatibility/exp_e.go)):**
  - [x] **`EXP-E1` (`pass: true`):** Stage-3 `pgproto3` session-state virtualizer verified `pgx` Extended Query prepared-statement hashing & backend re-injection across pooled connections, `pgx.Batch` pipeline mode, DDL `0A000`/`26000` invalidation & re-bind, `psql` + `COPY FROM STDIN` / `COPY TO STDOUT`, irreversible session auto-pinning (`LISTEN`/`TEMP`/`SET`), `CancelRequest` (`80877102`) forwarding (`150.43 ms`), and `PgBouncer 1.26.1` compatibility.
  - [x] **`EXP-E2` (`pass: true`):** `2 GB` `StartupMessage` length-bomb rejected, `SNI != tenant` spoof rejected (`28000`), `Slowloris` stalled pre-auth closed cleanly, and local `bbolt` WAL ledger (`18` events) + `5x` Queue replay (`72` deduplicated) achieved **`0%` duplicate billing** (`23 == 23` RUs).
  - [x] **`EXP-E3` & `EXP-E4` (`pass: true`):** `5,000` concurrent idle client connections held at **`10.43 KB/conn`** (`< 20 KB` gate), and `20` slow `COPY OUT` readers streaming `92 MB` completed with **`1.20 MB` Gateway heap delta** (`< 30 MB` gate) via per-chunk backpressure.
- [x] **Stage 3 Artifacts:** Recorded in [stage3-results.json](file:///workspaces/air/stage3/stage3-results.json) (`all_passed: true`).
