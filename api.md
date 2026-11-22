# AIR API Reference

AIR is composed of **4 decoupled services** connected over an authenticated gRPC mesh, backed by an external PostgreSQL metadata database (`AIR_META_DSN`):

| Service | Default HTTP Admin/API Port | Default gRPC Port | Data / Wire Port | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **`air-server`** | `:8080` | `:9090` | dynamic tenant PG ports | Control Plane & PostgreSQL 17 compute manager |
| **`air-pageserver`** | `:8083` | `:9091` | Unix socket (optional) | Multi-tenant storage engine, WAL ingestion & page service |
| **`air-read-engine`** | `:8082` | `:9092` | `:5433` (PG wire) | Shared SQLite read-replica engine |
| **`air-gateway`** | `:8081` | `:9093` | `:5432` (PG wire) | Client PostgreSQL wire gateway, connection pooler & router |

---

## Authentication

### 1. Public Control Plane API (`air-server` `/v1/*`)
- Header: `Authorization: Bearer <API_KEY>` (seeded on startup via `AIR_BOOTSTRAP_API_KEY`, default `air_dev_bootstrap_key` in Docker Compose).
- Database creation also requires an `Idempotency-Key: <unique-key>` header.

### 2. Service-to-Service & Admin APIs (HTTP & gRPC)
Protected by `AIR_INTERNAL_TOKEN` (default `air-dev-internal-token` in Docker Compose) using constant-time comparison:
- **HTTP headers** (either is accepted):
  - `X-Air-Internal-Token: <AIR_INTERNAL_TOKEN>`
  - `Authorization: Bearer <AIR_INTERNAL_TOKEN>`
- **gRPC metadata** (either is accepted):
  - `x-air-internal-token: <AIR_INTERNAL_TOKEN>`
  - `x-internal-token: <AIR_INTERNAL_TOKEN>`

---

## Ordered Provisioning & Teardown Flow

When `POST /v1/databases` (or `POST /v1/namespaces`) is called on `air-server`, provisioning executes in strict order across all 4 services:

1. **`air-server`** — Initializes tenant PostgreSQL 17 instance, generates & wraps tenant DEK (using `AIR_MASTER_KEY_HEX`), records metadata in external PostgreSQL (`clusters`, `tenant_events`).
2. **`air-pageserver`** — Calls `PageserverService.EnsureTenant` (with wrapped DEK & generation) and `BootstrapTenantFromPGData`.
3. **`air-read-engine`** — Calls `ReadEngineService.EnsureTenant` to register the tenant namespace.
4. **`air-gateway`** — Calls `GatewayAdmin.RegisterNamespace` to wire the user namespace to `air-server`, `air-read-engine`, and `air-pageserver`.

`DELETE /v1/databases/:id` cascades teardown in reverse order: `air-gateway` → `air-read-engine` → `air-pageserver` → `air-server` compute & metadata.

---

## 1. `air-server` HTTP API (`:8080`)

### Health Check
```http
GET /healthz
```
Response `200 OK`:
```json
{"status":"ok"}
```

### Create Database (Provision Across All 4 Services)
```http
POST /v1/databases
POST /v1/namespaces
Authorization: Bearer <API_KEY>
Idempotency-Key: <unique-idempotency-key>
Content-Type: application/json
```
Body (`tenant_id` or `namespace`, matching `^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$`):
```json
{
  "tenant_id": "demo_db"
}
```
Example:
```bash
curl -X POST http://127.0.0.1:8080/v1/databases \
  -H "Authorization: Bearer air_dev_bootstrap_key" \
  -H "Idempotency-Key: idem-demo-db-001" \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"demo_db"}'
```
Response `201 Created`:
```json
{
  "account_id": "acct_default",
  "tenant_id": "demo_db",
  "generation": "gen_380e33b412c1b70a",
  "database": "demo_db",
  "username": "app_user",
  "password": "<generated-password>",
  "direct_port": 18021,
  "system_identifier": "7711011875213824053",
  "fencing_epoch": 1,
  "state": "ACTIVE"
}
```

### Get Database / Namespace
```http
GET /v1/databases/:id
GET /v1/namespaces/:id
Authorization: Bearer <API_KEY>
```
Response `200 OK`:
```json
{
  "account_id": "acct_default",
  "tenant_id": "demo_db",
  "generation": "gen_380e33b412c1b70a",
  "direct_port": 18021,
  "system_identifier": "7711011875213824053",
  "fencing_epoch": 1,
  "state": "ACTIVE"
}
```

### Delete Database / Namespace (Cascading Teardown)
```http
DELETE /v1/databases/:id
DELETE /v1/namespaces/:id
Authorization: Bearer <API_KEY>
```
Response `200 OK`:
```json
{"status":"purged","tenant_id":"demo_db"}
```

### Backups, Verification, Restore & Cold Offload
| Method | Path | Body | Description |
| :--- | :--- | :--- | :--- |
| `POST` | `/v1/databases/:id/backups` | `{"type":"full"}` | Trigger encrypted backup to S3/R2 |
| `GET` | `/v1/databases/:id/backups` | — | List backup manifests for tenant |
| `POST` | `/v1/databases/:id/backups/:backup_id/verify` | `{"stream_hash":true,"deep_restore":false}` | Verify backup integrity |
| `POST` | `/v1/databases/:id/restore` | `{"backup_id":"...","target_lsn":"...","target_time":"...","new_tenant_id":"..."}` | Point-in-time / backup restore |
| `GET` | `/v1/databases/:id/offload` | — | Get latest R2 cold-offload status |
| `DELETE` | `/v1/api-keys/:id` | — | Revoke an API key |

---

## 2. `air-pageserver` Admin HTTP API (`:8083`)

All `/v1/namespaces*` routes require `X-Air-Internal-Token` (or `Authorization: Bearer <AIR_INTERNAL_TOKEN>`).

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/healthz` | Health & active tenant count (no auth) |
| `GET` | `/v1/namespaces` | List all registered tenant namespaces, generations & LSNs |
| `POST` | `/v1/namespaces` | Create/ensure a tenant namespace (`{"tenant_id":"...","generation":"...","epoch":1,"wrapped_dek":"...","load_from_r2":false}`) |
| `POST` | `/v1/namespaces/:id` | Same as above with tenant ID in path |
| `DELETE` | `/v1/namespaces/:id` | Purge tenant namespace from pageserver |

Example:
```bash
curl -H "X-Air-Internal-Token: air-dev-internal-token" \
  http://127.0.0.1:8083/v1/namespaces
```
Response:
```json
{
  "service": "air-pageserver",
  "namespaces": [
    {"tenant_id": "demo_db", "generation": "gen_380e33b412c1b70a", "last_lsn": 28811018}
  ]
}
```

---

## 3. `air-read-engine` Admin HTTP API (`:8082`)

All `/v1/namespaces*` routes require `X-Air-Internal-Token`.

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/healthz` | Health, SQLite version, PG wire port & query stats |
| `GET` | `/v1/status` | Engine status & registered tenants |
| `GET` | `/metrics` | Detailed JSON metrics |
| `GET` | `/v1/namespaces` | List all tenant namespaces |
| `POST` | `/v1/namespaces` | Create/ensure tenant namespace (`{"tenant_id":"..."}`) |
| `POST` | `/v1/namespaces/:id` | Same as above with tenant ID in path |
| `DELETE` | `/v1/namespaces/:id` | Remove tenant namespace |
| `POST` | `/v1/tenants/:id/start` | Register/start tenant DB |
| `POST` | `/v1/tenants/:id/sync` | Sync tenant from primary (`{"primary_conninfo":"..."}` or `{"sql":"...","lsn":"..."}`) |

Example:
```bash
curl -H "X-Air-Internal-Token: air-dev-internal-token" \
  http://127.0.0.1:8082/v1/namespaces
```
Response:
```json
{
  "service": "air-read-engine",
  "namespaces": ["postgres", "demo_db"]
}
```

---

## 4. `air-gateway` Admin HTTP API (`:8081`)

All `/v1/*`, `/metrics`, and `/internal/v1/*` routes require `X-Air-Internal-Token`.

### Namespace Management
| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/healthz` | Health check (no auth) |
| `GET` | `/v1/namespaces` | List all cached tenant namespaces |
| `POST` | `/v1/namespaces` | Register & connect namespace to `air-server`, `air-read-engine`, and `air-pageserver` |
| `GET` | `/v1/namespaces/:id` | Inspect namespace connectivity across all 3 backends |
| `DELETE` | `/v1/namespaces/:id` | Drain pool & unregister namespace |

Example (`GET /v1/namespaces/demo_db`):
```bash
curl -H "X-Air-Internal-Token: air-dev-internal-token" \
  http://127.0.0.1:8081/v1/namespaces/demo_db
```
Response:
```json
{
  "air_connected": true,
  "air_host": "air-server",
  "air_port": 18021,
  "namespace": "demo_db",
  "pageserver_connected": true,
  "pageserver_exists": true,
  "pageserver_generation": "gen_380e33b412c1b70a",
  "pageserver_last_lsn": 28811018,
  "read_engine_connected": true,
  "read_engine_host": "0.0.0.0",
  "read_engine_port": 5433,
  "service": "air-gateway",
  "state": "ACTIVE",
  "tenant_id": "demo_db"
}
```

### Database Lifecycle Proxy & Pool Controls
| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/v1/databases` | List databases via `air-server` gRPC (`?account_id=...`) |
| `POST` | `/v1/databases` | Create database via `air-server` gRPC |
| `GET` | `/v1/databases/:id` | Get database info |
| `DELETE` | `/v1/databases/:id` | Drain pool & delete database |
| `POST` | `/v1/databases/:id/suspend` | Drain pool & suspend tenant |
| `POST` | `/v1/databases/:id/start` (or `/wake`) | Start/wake suspended tenant |
| `POST` | `/v1/databases/:id/lock` | Drain pool & lock tenant |
| `POST` | `/v1/databases/:id/unlock` | Unlock tenant |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/internal/v1/metrics` | JSON metrics (`?tenant_id=...&user=...`) |
| `GET` | `/internal/v1/tenants` | List cached tenant routes |
| `POST` | `/internal/v1/tenants/:id/limits` | Update `{"max_client_conns":N,"max_backends":M}` |
| `POST` | `/internal/v1/tenants/:id/drain` | Cooperatively drain tenant connections (`?timeout=2s`) |

---

## 5. gRPC Services

Proto definitions live in `proto/`:

| Service | Proto File | Default Port | Key RPCs |
| :--- | :--- | :--- | :--- |
| `air.db.v1.AirDBControl` | `proto/airdb.proto` | `air-server:9090` | `GetTenantRoute`, `WakeTenant`, `WatchTenantChanges`, `CreateDatabase`, `ListDatabases`, `GetDatabase`, `DeleteDatabase`, `SuspendDatabase`, `StartDatabase`, `LockDatabase`, `UnlockDatabase` |
| `air.db.v1.GatewayAdmin` | `proto/airdb.proto` | `air-gateway:9093` | `RegisterNamespace`, `DeleteNamespace`, `GetNamespace`, `DrainTenant`, `SetTenantLimits` |
| `air.pageserver.v1.PageserverService` | `proto/pageserver.proto` | `air-pageserver:9091` | `Health`, `EnsureTenant`, `GetTenantInfo`, `ListTenants`, `PurgeTenant`, `BootstrapTenantFromPGData`, `IngestWALRecords`, `GetPageAtLSN`, `GetPageBatch`, `PutPageBatch` |
| `air.readengine.v1.ReadEngineService` | `proto/readengine.proto` | `air-read-engine:9092` | `Health`, `EnsureTenant`, `GetTenantRoute`, `DeleteTenant`, `ListTenants`, `SyncTenant` |

---

## 6. Connecting to a Tenant Database (PostgreSQL Wire Protocol)

Connect standard PostgreSQL clients (`psql`, `pgx`, `psycopg`, `node-postgres`, Prisma, etc.) to **`air-gateway`** on port `5432` using the `username`, `password`, and `database` (tenant namespace) returned by `POST /v1/databases`:

```text
postgresql://<username>:<password>@<gateway-host>:5432/<tenant_id>?sslmode=disable
```

Example:
```bash
psql "postgresql://app_user:<password>@127.0.0.1:5432/demo_db?sslmode=disable" -c "SELECT current_user, current_database(), version();"
```
