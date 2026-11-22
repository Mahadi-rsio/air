# AIR — Multi-Tenant PostgreSQL 17 Platform (`v2.0`)

AIR is a production-oriented, multi-tenant PostgreSQL 17 platform featuring a Go database engine and control plane (`air-server`), a standalone **Go PG gateway & transaction pooler** (`air-gateway`) communicating over **gRPC**, a **default SQLite read engine** (`air-read-engine`) that reads PostgreSQL 17 pages natively from AIR's Pageserver via the Cgo `air_pg` virtual table (zero `/sync` required), cooperative scale-to-zero, generation-fenced cold offload, encrypted Cloudflare R2 backups/PITR, and a custom Neon-style Pageserver (`air_smgr`) with Content-Addressed Upload (CAU).

> **Quick Docs:** [Setup Guide](setup.md) · [API Reference](api.md) · [Docs Index](docs/README.md) · [Contributing](CONTRIBUTING.md) · [Known Issues](docs/development/known-issues.md)
>
> **Architecture & Subsystem Docs:** [Overview](docs/architecture/overview.md) · [Codebase Map](docs/architecture/codebase-map.md) · [Pageserver](docs/architecture/pageserver.md) · [Read Engine](docs/architecture/read-engine.md) · [Storage](docs/architecture/storage.md) · [Tenant Lifecycle](docs/architecture/tenant-lifecycle.md) · [WAL & Recovery](docs/architecture/wal-and-recovery.md) · [Backup & Restore](docs/architecture/backup-and-restore.md) · [Developer Guide](docs/development/DEVELOPMENT.md)

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
│   ├── air-gateway/         # Standalone Go PG Gateway & Transaction Pooler (PG wire :5432, Admin API :8081, gRPC client)
│   └── air-read-engine/     # Standalone Read Engine entrypoint (shared read-only SQLite + native Pageserver air_pg vtab)
├── gateway/                 # Gateway Dockerfile for container builds
├── proto/                   # Protobuf / gRPC contract (airdb.proto: AirDBControl)
├── internal/
│   ├── config/              # Runtime flags and environment configuration
│   ├── crypto/              # EnvelopeKMS (persistent 256-bit KEK + wrapped per-tenant DEKs)
│   ├── tenant/              # Canonical tenant/account ID validation & symlink-safe path containment
│   ├── cell/                # PG17 lifecycle, warm initdb pool, cgroup v2, supautils hardening, flock ownership
│   ├── control/             # Durable PG17 metadata, migrations, offload worker, backup/restore, HTTP & gRPC
│   ├── gateway/             # Go PG Gateway, SQL router (defaults read-engine to :5433), pooler, metrics & admin API
│   ├── readadapter/         # Native Pageserver socket client, PG17 catalog discovery, MVCC/CLOG, L1/L2 cache & SQLite air_pg vtab
│   ├── pb/                  # Generated Go protobuf & gRPC stubs
│   ├── pageserver/          # Go Pageserver (WAL receiver/parser, pure-Go Heap/XACT redo, L0/L1/Image layers, CAU R2 upload, GetPage@LSN)
│   ├── backup/              # Shared R2 Store, LSN continuity, encrypted backups, manifest verify, PITR & DAG GC
│   └── fencing/             # CLOCK_MONOTONIC lease watchdog & R2 ETag CAS fencing manifests
├── extensions/air_smgr/     # PostgreSQL 17 custom smgr extension (Local File Cache + Pageserver IPC)
├── patches/                 # PG17 smgr_hook source patch (pg17-smgr-hook.patch)
├── migrations/              # SQL schema reference (001_initial_schema.sql, 002_api_keys.sql)
├── docs/                    # Architecture, development, known issues, and audit reports
├── tests/e2e/               # Real PostgreSQL 17 end-to-end tests (including native zero-sync SQLite read E2E)
├── Dockerfile               # Multi-stage container build for air-server (Go + patched PG17 + supautils + air_smgr)
├── Dockerfile.read-engine   # Multi-stage container build for air-read-engine (Go + Cgo libsqlite3)
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
2. **Standalone Go Gateway, Transaction Pooler & Default Read Routing ([internal/gateway/](file:///workspaces/air/internal/gateway))**:
   - Auto-discovers newly created tenant databases via `air-server` over **gRPC** (`--db-grpc-addr` / `AIR_DB_GRPC_ADDR`, default `127.0.0.1:9090`).
   - Routes read-only queries by default to `air-read-engine` (`--read-engine-port=5433` default) with automatic fallback to the PG17 primary on causal lag, write transactions, or unavailability.
   - Enforces an 8 KB `StartupMessage` limit, TLS 1.2+ (`SSLRequest = 80877103`), cryptographic `SNI == database` tenant binding, and blocks reserved `postgres` superuser connections (`SQLSTATE 28000`).
   - Enforces global, per-tenant, and per-user client connection limits (`--max-client-conns`, `--max-tenant-client-conns`, `--max-user-client-conns`) plus runtime per-tenant overrides via `POST /internal/v1/tenants/{id}/limits`.
   - Tracks per-user and per-tenant query count, error count, latency (`min`/`avg`/`max`/`p50`/`p95`/`p99`), pool wait time, and data size (`bytes_in`, `bytes_out`, `rows_returned`, `data_row_bytes`) exposed at `GET /internal/v1/metrics` (JSON) and `GET /metrics` (Prometheus).
   - Buffers per-query metering records (`recorded_at`, `db_id`, `bandwidth_bytes`, `query_time_us`, `query_type`) to a local log file (`AIR_METERING_LOG_DIR`) and flushes batches to `query_metering` in `AIR_META_DSN` via PostgreSQL `COPY` every 2 minutes (`AIR_METERING_FLUSH_INTERVAL_SEC`) or at 10 MB (`AIR_METERING_MAX_FILE_BYTES`), deleting each rotated log file after persistence.
   - Exposes Gateway Admin DB lifecycle endpoints (`POST /v1/databases`, `GET /v1/databases`, `GET/DELETE /v1/databases/{id}`, `POST /v1/databases/{id}/{suspend,start,lock,unlock}`) backed by `AirDBControl` gRPC calls.
3. **Cooperative Scale-to-Zero & Warm `initdb` Pool ([agent.go](file:///workspaces/air/internal/cell/agent.go), [control.go](file:///workspaces/air/internal/control/control.go))**:
   - Suspends idle `ACTIVE` tenants after `--idle-timeout` (`AIR_IDLE_TIMEOUT`) only after cooperative drain guards pass (`ErrDrainVetoed` on active txns, prepared xacts, `LISTEN`, temp tables, or advisory locks).
   - Coalesces concurrent wake-on-connect requests via `singleflight` and accelerates provisioning with a pre-warmed `initdb` pool.
4. **AIR Pageserver, `air_smgr` & Content-Addressed Upload (CAU) ([pageserver.md](docs/architecture/pageserver.md), [pageserver-plan.md](docs/reports/plans/pageserver-plan.md), [smgr-plan.md](docs/reports/plans/smgr-plan.md))**:
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

### Running `air-server`, `air-read-engine`, and `air-gateway` Locally (Zero-Sync Native Mode)

```bash
# 1. Start air-server (Control Plane HTTP :8080 + Air DB gRPC :9090 + Cell + Pageserver)
go run ./cmd/air-server \
  -data-dir /tmp/air-live \
  -http-addr 127.0.0.1:8080 \
  -grpc-addr 127.0.0.1:9090 \
  -meta-port 15432 \
  -bootstrap-account-id acct_demo \
  -bootstrap-api-key air_live_demo_secret_token_12345 \
  -internal-token air_internal_secret \
  -pageserver \
  -pageserver-socket /tmp/air-live/pageserver.sock

# 2. Start shared air-read-engine in native Pageserver mode (PG wire :5433, HTTP :8082, zero /sync!)
go run ./cmd/air-read-engine \
  -data-dir /tmp/air-read-engine \
  -cache-dir /tmp/air-read-engine/cache \
  -http-addr 127.0.0.1:8082 \
  -pg-port 5433 \
  -pageserver-socket /tmp/air-live/pageserver.sock

# 3. Start Go air-gateway (PG Pooler :5432 + Admin API :8081; defaults to read-engine :5433)
go run ./cmd/air-gateway \
  --gateway-addr 127.0.0.1:5432 \
  --gateway-admin-addr 127.0.0.1:8081 \
  --db-grpc-addr 127.0.0.1:9090 \
  --control-plane-url http://127.0.0.1:8080 \
  --internal-token air_internal_secret \
  --pool-mode
```

### Container Images (GHCR)

Published automatically on `v*` tags and manual workflow dispatches by [.github/workflows/ghcr-publish.yml](file:///workspaces/air/.github/workflows/ghcr-publish.yml):

| Component | GHCR Image | Dockerfile | Default Ports |
| :--- | :--- | :--- | :--- |
| **AIR Server** (Control Plane + Cell + Pageserver) | `ghcr.io/mahadi-rsio/air` | [Dockerfile](file:///workspaces/air/Dockerfile) | `8080` (HTTP), `9090` (gRPC) |
| **AIR Gateway** (Go PG Pooler + Read Router) | `ghcr.io/mahadi-rsio/air-gateway` | [gateway/Dockerfile](file:///workspaces/air/gateway/Dockerfile) | `5432` (PG), `8081` (Admin) |
| **AIR Read Engine** (Native SQLite `air_pg` VTab) | `ghcr.io/mahadi-rsio/air-read-engine` | [Dockerfile.read-engine](file:///workspaces/air/Dockerfile.read-engine) | `5433` (PG), `8082` (HTTP) |

### Fly.io Deployment ([fly.toml](file:///workspaces/air/fly.toml))

- Uses `ghcr.io/mahadi-rsio/air` (published on `v*` tags by [.github/workflows/ghcr-publish.yml](file:///workspaces/air/.github/workflows/ghcr-publish.yml)) with a persistent `air_data` volume mounted at `/var/lib/air`.
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

- **Non-Destructive Restore**: `POST /v1/databases/{id}/restore` never overwrites an active tenant in place; pass `new_tenant_id` to materialize a restored tenant alongside the original.
- See [known-issues.md](docs/development/known-issues.md) for full details.

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
