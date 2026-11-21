# AIR — Multi-Tenant PostgreSQL 17 Platform (`v2.0`)

AIR is a production-oriented, multi-tenant PostgreSQL 17 platform featuring a Go database engine and control plane (`air-server`), a standalone **C++20 PG gateway & connection pooler** (`air-gateway`) communicating over **gRPC**, cooperative scale-to-zero, generation-fenced cold offload, encrypted Cloudflare R2 backups/PITR, and a custom Neon-style Pageserver (`air_smgr`) with Content-Addressed Upload (CAU).

> **Architecture & Developer Guides:** [Overview](file:///workspaces/air/docs/architecture/overview.md) · [Codebase Map](file:///workspaces/air/docs/architecture/codebase-map.md) · [Developer Guide](file:///workspaces/air/docs/development/DEVELOPMENT.md) · [Contributing](file:///workspaces/air/CONTRIBUTING.md) · [Known Issues](file:///workspaces/air/docs/known-issues.md)
>
> **Subsystem Docs:** [Pageserver](file:///workspaces/air/docs/architecture/pageserver.md) · [Storage](file:///workspaces/air/docs/architecture/storage.md) · [Tenant Lifecycle](file:///workspaces/air/docs/architecture/tenant-lifecycle.md) · [WAL & Recovery](file:///workspaces/air/docs/architecture/wal-and-recovery.md) · [Backup & Restore](file:///workspaces/air/docs/architecture/backup-and-restore.md)

## How AIR Differs from Standard PostgreSQL

- **Per-Tenant Process & Filesystem Isolation**: Each tenant database runs in its own hardened PostgreSQL 17 cluster (`PGDATA` under `tenant.SafeSubpath`) with `supautils.so`, `app_user` non-superuser privileges, Linux `cgroup v2` limits, `flock` single-owner locks, and a per-tenant AES-256-GCM DEK (`EnvelopeKMS`).
- **Cooperative Scale-to-Zero & Cold Offload**: Idle `ACTIVE` tenants drain cleanly (`SUSPENDED`) and can offload their storage to R2 (`OFFLOADED`), waking transparently on demand with `singleflight` coalescing and monotonic `Epoch` CAS fencing.
- **Disaggregated Page Storage**: With `--pageserver`, `air_smgr.so` intercepts relation block reads via `smgr_hook` and fetches missing pages at an explicit LSN (`GetPage@LSN`) from the Go Pageserver and R2 CAU layers.
- **Non-Destructive Encrypted PITR**: Backups (`FULL`, `INCR`, `WAL`) are Zstd-compressed, AES-256-GCM encrypted, verified via immutable `BackupManifest` objects (`If-None-Match: *`), and restored either as a staged verification or into a new isolated tenant (`new_tenant_id`).

## Repository Structure

```text
.
├── cmd/
│   ├── air-server/          # Air DB server entrypoint (Control Plane HTTP + gRPC + Cell Agent + Pageserver)
│   └── air-read-engine/     # Standalone Read Engine entrypoint (shared read-only SQLite default, or PG17 air_ro via --engine=postgres)
├── gateway/                 # Standalone C++20 PG Gateway & Pooler (PG wire :5432, Admin API :8081, gRPC client)
├── proto/                   # Protobuf / gRPC contract (airdb.proto: AirDBControl)
├── internal/
│   ├── config/              # Runtime flags and environment configuration
│   ├── crypto/              # EnvelopeKMS (persistent 256-bit KEK + wrapped per-tenant DEKs)
│   ├── tenant/              # Canonical tenant/account ID validation & symlink-safe path containment
│   ├── cell/                # PG17 lifecycle, warm initdb pool, cgroup v2, supautils hardening, flock ownership
│   ├── control/             # Durable PG17 metadata, migrations, offload worker, backup/restore, HTTP & gRPC
│   ├── pb/                  # Generated Go protobuf & gRPC stubs
│   ├── pageserver/          # Go Pageserver (WAL receiver/parser, L0/L1/Image layers, CAU R2 upload, GetPage@LSN)
│   ├── backup/              # Shared R2 Store, LSN continuity, encrypted backups, manifest verify, PITR & DAG GC
│   └── fencing/             # CLOCK_MONOTONIC lease watchdog & R2 ETag CAS fencing manifests
├── extensions/air_smgr/     # PostgreSQL 17 custom smgr extension (Local File Cache + Pageserver IPC)
├── patches/                 # PG17 smgr_hook source patch (pg17-smgr-hook.patch)
├── migrations/              # SQL schema reference (001_initial_schema.sql, 002_api_keys.sql)
├── docs/                    # Architecture, development, known issues, and Phase 13.1 audit reports
├── tests/e2e/               # Real PostgreSQL 17 end-to-end tests (including Phase 13 backup/PITR verification)
├── Dockerfile               # Multi-stage container build (Go air-server + C++ air-gateway + PG17 + air_smgr)
└── fly.toml                 # Fly.io deployment configuration
```

## Prerequisites

- Go `1.24+`
- PostgreSQL `17` binaries in `/usr/lib/postgresql/17/bin` (`initdb`, `pg_ctl`, `pg_controldata`, `psql`)
- `supautils.so` installed in `/usr/lib/postgresql/17/lib/supautils.so`
- For Pageserver mode (`AIR_PAGESERVER_ENABLED=true`): PostgreSQL 17 patched with [pg17-smgr-hook.patch](file:///workspaces/air/patches/pg17-smgr-hook.patch) and `air_smgr.so` built from [extensions/air_smgr](file:///workspaces/air/extensions/air_smgr) (automated in [Dockerfile](file:///workspaces/air/Dockerfile))

## Core Capabilities (`AIR v2.0`)

1. **Control Plane Bearer API Key Authentication ([002_api_keys.sql](file:///workspaces/air/migrations/002_api_keys.sql), [http.go](file:///workspaces/air/internal/control/http.go))**:
   - All `/v1/*` endpoints require `Authorization: Bearer <token>`.
   - Tokens are stored only as SHA-256 digests (`key_sha256 UNIQUE`) bound to `account_id`, with instant revocation via `DELETE /v1/api-keys/{id}`.
   - Supports external HA metadata PostgreSQL via `--meta-dsn` (`AIR_META_DSN`) or an embedded local metadata PG17 instance.
2. **Standalone C++20 Gateway, Transaction Pooler & Metrics ([gateway/src/](file:///workspaces/air/gateway/src))**:
   - Auto-discovers newly created tenant databases from internal metadata PG (`clusters`) and communicates with `air-server` over **gRPC** (`--db-grpc-addr` / `AIR_DB_GRPC_ADDR`, default `127.0.0.1:9090`).
   - Enforces an 8 KB `StartupMessage` limit, TLS 1.2+ (`SSLRequest = 80877103`), cryptographic `SNI == database` tenant binding, and blocks reserved `postgres` superuser connections (`SQLSTATE 28000`).
   - Enforces global, per-tenant, and per-user client connection limits (`--max-client-conns`, `--max-tenant-client-conns`, `--max-user-client-conns`) plus runtime per-tenant overrides via `POST /internal/v1/tenants/{id}/limits`.
   - Tracks per-user and per-tenant query count, error count, latency (`min`/`avg`/`max`/`p50`/`p95`/`p99`), pool wait time, and data size (`bytes_in`, `bytes_out`, `rows_returned`, `data_row_bytes`) exposed at `GET /internal/v1/metrics` (JSON) and `GET /metrics` (Prometheus).
   - Exposes Gateway Admin DB lifecycle endpoints (`POST /v1/databases`, `GET /v1/databases`, `GET/DELETE /v1/databases/{id}`, `POST /v1/databases/{id}/{suspend,start,lock,unlock}`) backed by `AirDBControl` gRPC calls.
3. **Cooperative Scale-to-Zero & Warm `initdb` Pool ([agent.go](file:///workspaces/air/internal/cell/agent.go), [control.go](file:///workspaces/air/internal/control/control.go))**:
   - Suspends idle `ACTIVE` tenants after `--idle-timeout` (`AIR_IDLE_TIMEOUT`) only after cooperative drain guards pass (`ErrDrainVetoed` on active txns, prepared xacts, `LISTEN`, temp tables, or advisory locks).
   - Coalesces concurrent wake-on-connect requests via `singleflight` and accelerates provisioning with a pre-warmed `initdb` pool.
4. **AIR Pageserver, `air_smgr` & Content-Addressed Upload (CAU) ([pageserver.md](file:///workspaces/air/pageserver.md), [smgr.md](file:///workspaces/air/smgr.md))**:
   - Enabled via `--pageserver` / `AIR_PAGESERVER_ENABLED=true` (serves `GetPage@LSN` over `--pageserver-socket` with a bounded `--pageserver-cache-mb` LRU disk cache).
   - Immutable layer files are uploaded to R2 using Content-Addressed Upload (`tenants/{tenant_id}/layers/sha256/{prefix}/{sha256}`) with `If-None-Match: *` deduplication, followed by ETag CAS publication of `tenants/{tenant_id}/timeline_{epoch}/layer_manifest.json`.
   - Both `WakeTenant` and startup `Reconcile` preload tenant layers into the Pageserver before starting/adopting PostgreSQL so `air_smgr` can serve catalog blocks (e.g., `global/1260` / `pg_authid`) immediately.
5. **Encrypted Backups, Fencing & `supautils` Hardening**:
   - Preloads `supautils` with `app_user` (`NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT`), `temp_file_limit = 32MB`, and `pg_hba.conf` rejection of TCP `postgres` logins.
   - Supports `EnvelopeKMS` (`AES-256-GCM`), R2 ETag CAS fencing manifests (`internal/fencing`), and encrypted full/incremental PITR backups (`internal/backup`).

## Quick Start & Testing

### Automated End-to-End Tests
Runs the suites in [tests/e2e/](file:///workspaces/air/tests/e2e) against real PostgreSQL 17 processes, `supautils.so`, persistent storage, the HTTP Control Plane, and the PostgreSQL Gateway:

```bash
go test -v -race -count=1 ./...
```

### Running `air-server`, `air-read-engine`, and C++ `air-gateway` Locally

```bash
# 1. Start air-server (Control Plane HTTP :8080 + Air DB gRPC :9090 + Cell)
go run ./cmd/air-server \
  -data-dir /tmp/air-live \
  -http-addr 127.0.0.1:8080 \
  -grpc-addr 127.0.0.1:9090 \
  -meta-port 15432 \
  -bootstrap-account-id acct_demo \
  -bootstrap-api-key air_live_demo_secret_token_12345 \
  -internal-token air_internal_secret

# 2. Start shared air-read-engine (shared read-only SQLite engine :5433 serving all tenants by default, HTTP :8082)
go run ./cmd/air-read-engine \
  -engine sqlite \
  -data-dir /tmp/air-read-engine \
  -http-addr 127.0.0.1:8082 \
  -pg-port 5433

# 3. Build and start C++ air-gateway (PG Pooler :5432 + Admin API :8081)
make -C gateway all test
./gateway/air-gateway \
  --gateway-addr 127.0.0.1:5432 \
  --gateway-admin-addr 127.0.0.1:8081 \
  --db-grpc-addr 127.0.0.1:9090 \
  --meta-dsn "host=127.0.0.1 port=15432 user=postgres dbname=postgres" \
  --internal-token air_internal_secret \
  --read-engine-host 127.0.0.1 \
  --read-engine-port 5433
```

### Fly.io Deployment ([fly.toml](file:///workspaces/air/fly.toml))

- Uses `ghcr.io/mahadi-rsio/air:v2.0.12` (published on `v*` tags by [.github/workflows/ghcr-publish.yml](file:///workspaces/air/.github/workflows/ghcr-publish.yml)) with a persistent `air_data` volume mounted at `/var/lib/air`.
- Set `AIR_PAGESERVER_ENABLED = "true"` in `[env]` (along with `AIR_R2_*` credentials in Fly secrets) to enable the in-process Pageserver and CAU layer uploads to R2.

### Control Plane HTTP API

All `/v1/*` endpoints require `Authorization: Bearer <token>`:

- `GET /healthz` — Metadata DB health check (unauthenticated)
- `POST /v1/databases` — Provision an isolated tenant database (`Idempotency-Key` required, body `{"tenant_id": "demo_db"}`)
- `GET /v1/databases/{id}` — Inspect database metadata (account-scoped)
- `DELETE /v1/databases/{id}` — Stop PG17, purge storage, shred DEK, and mark `PURGED` (account-scoped)
- `POST /v1/databases/{id}/backups` — Trigger an encrypted full, incremental, or WAL-only backup + WAL drain
- `GET /v1/databases/{id}/backups` — List recorded backup operations and statuses for a tenant
- `POST /v1/databases/{id}/backups/{backup_id}/verify` — Verify an immutable `BackupManifest`, WAL continuity, and R2 object/checksum integrity
- `POST /v1/databases/{id}/restore` — Execute `StagePITRRestore` (validation restore or full restore into `new_tenant_id` with `backup_id`, `target_lsn`, `target_time`, or `latest`)
- `DELETE /v1/api-keys/{id}` — Revoke an API key belonging to the authenticated account

## Current Limitations & Known Issues

- **Container `air-gateway` Runtime Packaging (Deferred)**: In container builds ([Dockerfile](file:///workspaces/air/Dockerfile)), `air-gateway` currently lacks runtime `libgrpc++.so.1.83` shared libraries in the final image stage (scheduled for a dedicated gateway packaging phase). See [known-issues.md](file:///workspaces/air/docs/known-issues.md) for full details.
- **Non-Destructive Restore**: `POST /v1/databases/{id}/restore` never overwrites an active tenant in place; pass `new_tenant_id` to materialize a restored tenant alongside the original.

Example with `curl` and `psql`:

```bash
curl -s -X POST http://127.0.0.1:8080/v1/databases \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer air_live_demo_secret_token_12345" \
  -H "Idempotency-Key: create-demo-1" \
  -d '{"tenant_id":"demo_db"}'

PGPASSWORD='<PASSWORD>' psql "postgres://app_user@127.0.0.1:5432/demo_db?sslmode=require" \
  -c "SELECT current_database(), current_user;"
```

### Archived Stage 1 & Stage 4 Remediation Suites

```bash
(cd stage_test/stage1 && go test -v -race -count=1 ./...)
(cd stage_test/stage4 && go test -v -race -count=1 ./...)
```
