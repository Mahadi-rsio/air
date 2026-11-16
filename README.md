# AIR — Multi-Tenant PostgreSQL 17 Platform (`v2.0`)

AIR is a production-oriented, multi-tenant PostgreSQL 17 platform built in Go with a transaction-mode wire pooler, cooperative scale-to-zero, encrypted R2 backups/PITR, and a custom Neon-style Pageserver (`air_smgr`) with Content-Addressed Upload (CAU).

> **Docs & Architecture:** [phase.md](file:///workspaces/air/phase.md) · [todo.md](file:///workspaces/air/todo.md) · [pageserver.md](file:///workspaces/air/pageserver.md) · [smgr.md](file:///workspaces/air/smgr.md)

## Repository Structure

```text
.
├── cmd/air-server/          # Single-cell server entrypoint (Control Plane + Cell Agent + Pageserver + PG Gateway)
├── internal/
│   ├── config/              # Runtime flags and environment configuration
│   ├── crypto/              # EnvelopeKMS (persistent 256-bit KEK + wrapped per-tenant DEKs)
│   ├── tenant/              # Canonical tenant/account ID validation & symlink-safe path containment
│   ├── cell/                # PG17 lifecycle, warm initdb pool, cgroup v2, supautils hardening, flock ownership
│   ├── control/             # Durable PG17 metadata, schema_migrations, Bearer API key auth, provisioning & wake/reconcile
│   ├── gateway/             # PostgreSQL wire-protocol gateway & transaction-mode pooler (TLS/SNI, SCRAM, wake-on-connect)
│   ├── pageserver/          # Go Pageserver (WAL receiver/parser, L0/L1/Image layers, CAU R2 upload, GetPage@LSN, walredo)
│   ├── backup/              # Encrypted full/incremental PG17 backups, WAL spooling, PITR & DAG GC
│   └── fencing/             # CLOCK_MONOTONIC lease watchdog & R2 ETag CAS fencing manifests
├── extensions/air_smgr/     # PostgreSQL 17 custom smgr extension (Local File Cache + Pageserver IPC)
├── patches/                 # PG17 smgr_hook source patch (pg17-smgr-hook.patch)
├── migrations/              # SQL schema reference (001_initial_schema.sql, 002_api_keys.sql)
├── tests/e2e/               # Real PostgreSQL 17 end-to-end tests (e2e, phase_f/g/h, pageserver_cau)
├── Dockerfile               # Container image build (PG17 + smgr_hook + supautils + air_smgr + air-server)
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
2. **Gateway Wire-Protocol, Mandatory TLS & Transaction Pooler ([gateway.go](file:///workspaces/air/internal/gateway/gateway.go), [pooler.go](file:///workspaces/air/internal/gateway/pooler.go))**:
   - Enforces an 8 KB `StartupMessage` limit, TLS 1.2+ (`SSLRequest = 80877103`), cryptographic `SNI == database` tenant binding (when SNI is present), and blocks reserved `postgres` superuser connections (`SQLSTATE 28000`).
   - Supports `--require-tls` (`AIR_REQUIRE_TLS`) to reject any plaintext connection with `SQLSTATE 28000`.
   - Optional transaction-mode pooling (`--pool-mode` / `AIR_POOL_MODE`, `--pool-max-backends` / `AIR_POOL_MAX_BACKENDS`) with `DISCARD ALL;` session isolation, Extended Query support, `COPY` streaming, and session pinning.
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

### Running `air-server` Locally

```bash
go run ./cmd/air-server \
  -data-dir /tmp/air-live \
  -http-addr 127.0.0.1:8080 \
  -gateway-addr 127.0.0.1:5432 \
  -meta-port 15432 \
  -bootstrap-account-id acct_demo \
  -bootstrap-api-key air_live_demo_secret_token_12345 \
  -require-tls=false
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
- `POST /v1/databases/{id}/backups` — Trigger an encrypted full or incremental backup + WAL drain
- `POST /v1/databases/{id}/restore` — Execute `CombineAndReplayPITR` to a target LSN or latest WAL
- `DELETE /v1/api-keys/{id}` — Revoke an API key belonging to the authenticated account

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
