# AIR — Single-Cell PostgreSQL 17 Foundation (`v1.1`)

AIR is a production-oriented, multi-tenant PostgreSQL 17 platform built in Go.

## Repository Structure

```text
.
├── cmd/air-server/          # Single-cell server entrypoint (Control Plane + Cell Agent + PG Gateway)
├── internal/
│   ├── config/              # Runtime flags and environment configuration
│   ├── crypto/              # EnvelopeKMS (persistent 256-bit KEK + wrapped per-tenant DEKs)
│   ├── tenant/              # Canonical tenant/account ID validation & symlink-safe path containment
│   ├── cell/                # PG17 lifecycle, flock ownership, /proc identity verification, restart/purge
│   ├── control/             # Durable PG17 metadata, schema_migrations, API key auth, provisioning state machine, HTTP API
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

## Quick Start & Testing

### Automated End-to-End Tests
Runs [tests/e2e/e2e_test.go](file:///workspaces/air/tests/e2e/e2e_test.go) against real PostgreSQL 17 processes, persistent storage, the HTTP Control Plane, and the PostgreSQL Gateway:

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

All `/v1/*` endpoints require `Authorization: Bearer <token>` (SHA-256 verified against `api_keys`):

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
