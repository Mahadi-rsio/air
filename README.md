# AIR — Single-Cell PostgreSQL 17 Foundation (`v1.1`)

AIR is a production-oriented, multi-tenant PostgreSQL 17 platform built in Go.

> **Roadmap & Tracker:** [phase.md](file:///workspaces/air/phase.md) · [todo.md](file:///workspaces/air/todo.md) · [16-updated-production-verdict.md](file:///workspaces/air/audit/16-updated-production-verdict.md)

## Repository Structure

```text
.
├── cmd/air-server/          # Single-cell server entrypoint (Control Plane + Cell Agent + PG Gateway)
├── internal/
│   ├── config/              # Runtime flags and environment configuration
│   ├── crypto/              # EnvelopeKMS (persistent 256-bit KEK + wrapped per-tenant DEKs)
│   ├── tenant/              # Canonical tenant/account ID validation & symlink-safe path containment
│   ├── cell/                # PG17 lifecycle, supautils v4.0.0 user hardening, flock ownership, /proc identity
│   ├── control/             # Durable PG17 metadata, schema_migrations, Bearer API key auth, provisioning state machine
│   └── gateway/             # PostgreSQL wire-protocol gateway (mandatory/optional TLS/SNI binding, role checks, TCP proxy)
├── migrations/              # SQL schema reference (001_initial_schema.sql, 002_api_keys.sql)
├── tests/e2e/               # Real PostgreSQL 17 end-to-end integration tests
├── audit/                   # Security & durability audit reports (01–16)
└── stage_test/              # Archived Stage 0–4 research prototypes & remediation tests
```

## Prerequisites

- Go `1.24+`
- PostgreSQL `17` binaries in `/usr/lib/postgresql/17/bin` (`initdb`, `pg_ctl`, `pg_controldata`, `psql`)
- `supautils.so` installed in `/usr/lib/postgresql/17/lib/supautils.so` (buildable from `stage_test/stage4/supautils` with `postgresql-server-dev-17`)

## Security & Tenant Isolation (`AIR v1.1`)

1. **Control Plane Bearer API Key Authentication ([002_api_keys.sql](file:///workspaces/air/migrations/002_api_keys.sql), [http.go](file:///workspaces/air/internal/control/http.go))**:
   - All `/v1/*` endpoints require `Authorization: Bearer <token>`.
   - Tokens are stored only as SHA-256 digests (`key_sha256 UNIQUE`) bound to `account_id`, with instant revocation via `DELETE /v1/api-keys/{id}`.
2. **Gateway Wire-Protocol & Mandatory TLS Hardening ([gateway.go](file:///workspaces/air/internal/gateway/gateway.go))**:
   - Enforces an 8 KB `StartupMessage` limit, TLS 1.2+ (`SSLRequest = 80877103`), cryptographic `SNI == database` tenant binding, and blocks reserved `postgres` superuser connections (`SQLSTATE 28000`).
   - Supports `--require-tls` (`AIR_REQUIRE_TLS`) to reject any plaintext connection with `SQLSTATE 28000`.
3. **`supautils` v4.0.0 & `app_user` Role Hardening ([agent.go](file:///workspaces/air/internal/cell/agent.go))**:
   - Preloads `shared_preload_libraries = 'supautils'` with:
     - `supautils.superuser = 'postgres'` & `supautils.privileged_role = 'app_user'`
     - `supautils.privileged_extensions = 'pg_stat_statements,pg_trgm'`
     - `supautils.reserved_roles = 'postgres'`
     - `supautils.reserved_memberships = 'pg_read_server_files,pg_write_server_files,pg_execute_server_program,postgres'`
     - `supautils.restrict_extension_versions = 'error'`
     - `temp_file_limit = 32MB` (`SQLSTATE 53400` on runaway disk-spill sorts)
   - Provisions `app_user` with `NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT`, revokes `PUBLIC` access on `template1`, `postgres`, and the tenant database, and blocks direct TCP connections as `postgres` in `pg_hba.conf` (`host all postgres 127.0.0.1/32 reject`).

## Quick Start & Testing

### Automated End-to-End Tests
Runs [tests/e2e/e2e_test.go](file:///workspaces/air/tests/e2e/e2e_test.go) (`TestAIRv1_SingleCellEndToEndMilestones`, `TestAIRv1_1_PhaseE_BearerAuthAndMandatoryGatewayTLS`, and `TestAIRv1_1_SupautilsUserHardening`) against real PostgreSQL 17 processes, `supautils.so`, persistent storage, the HTTP Control Plane, and the PostgreSQL Gateway:

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

### Control Plane HTTP API

All `/v1/*` endpoints require `Authorization: Bearer <token>`:

- `GET /healthz` — Metadata DB health check (unauthenticated)
- `POST /v1/databases` — Provision an isolated tenant database
  - Headers: `Authorization: Bearer <token>`, `Idempotency-Key`
  - Body: `{"tenant_id": "demo_db"}`
- `GET /v1/databases/{id}` — Inspect database metadata (account-scoped)
- `DELETE /v1/databases/{id}` — Stop PG17, purge storage, shred DEK, and mark `PURGED` (account-scoped)
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
