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

## Stage 2 — Multi-Tenant Density, Pro Sync HA Failover & Observability (`PENDING`)
- [ ] **`EXP-C2` & `EXP-C4` (`UQR-07`, `UQR-10`):** Multi-tenant density, memory/fork bomb containment, `shared_buffers` (`8MB/16MB/32MB`) RSS/hit-rate curves, anti-wraparound `maintenance_lock` (`VACUUM FREEZE`), and 4-way FinOps vs Customer RU separation.
- [ ] **`EXP-C5`, `EXP-D2`, `EXP-D3`, `EXP-D6`:** Process crash recovery, network jitter (`tc netem`), R2 outage spooling, and asymmetric partition split-brain write-boundary fencing (`8.0s` monotonic self-kill `< 12.0s` Control promote; Pro standby pre-fence).
- [ ] **Observability & DR (`RISK-39..41`, `UQR-11`):** OpenMetrics exporter (all 22 audit signals), 15 Alertmanager rules, and 12 scripted DR runbooks.

---

## Stage 3 — Cooperative Scale-to-Zero, Deep Freeze & Wire Multiplexer (`PENDING`)
- [ ] **`EXP-B1`..`EXP-B5` (`UQR-08`, `ADR-007`):** Warm/crash/cold wake latency benchmarks (`ConnectTime` vs `FirstQueryTime`), Gateway-controlled cooperative drain (`LISTEN`/`TEMP`/`2PC` pinning + `0` `ECONNRESET` across 1,000 shutdown boundary races), and `singleflight` 1,000-client wake-storm coalescing (`1` PG spawn per DB).
- [ ] **`EXP-E1`..`EXP-E4` (`UQR-09`, `ADR-009`):** Stage-3 `pgproto3` session-state virtualizer, ORM/driver compatibility suite (`pgx`, `psql`, prepared statement remapping, `CancelRequest`), and slow-reader `COPY OUT` backpressure stress test.
