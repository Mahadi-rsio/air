# AIR API Reference

AIR provides a unified multi-tenant database platform built with 4 interconnected services communicating over an authenticated gRPC mesh, with state recorded in an external PostgreSQL metadata database (`AIR_META_DSN`).

---

## Service Overview & Ports

| Service | HTTP Admin / REST Port | gRPC Port | Client Wire Port | Primary Role |
| :--- | :--- | :--- | :--- | :--- |
| **`air-server`** | `:8080` | `:9090` | Dynamic (tenant PG) | Control Plane, tenant orchestration, KMS & backup coordinator |
| **`air-gateway`** | `:8081` | `:9093` | `:5432` (PG wire) | Client connection entrypoint, transaction pooler, intelligent query router |
| **`air-read-engine`** | `:8082` | `:9092` | `:5433` (PG wire) | High-speed SQLite read replica for `SELECT` queries |
| **`air-pageserver`** | `:8083` | `:9091` | IPC socket (optional)| Pageserver storage engine, `GetPage@LSN` provider & R2/S3 sync |

---

## Authentication Model

### 1. Public Control Plane Endpoints (`air-server` `/v1/*`)
Public management APIs require a Bearer token:
- **Header**: `Authorization: Bearer <API_KEY>`
- Bootstrap key configured via `AIR_BOOTSTRAP_API_KEY` (default in Compose: `air_dev_bootstrap_key`).
- Revoke existing keys via `DELETE /v1/api-keys/{key_id}`.
- Creation requests require an `Idempotency-Key: <unique-uuid-or-slug>` header to prevent duplicate provisioning.

### 2. Internal Mesh & Admin Endpoints
All inter-service gRPC calls and Admin HTTP endpoints require the shared `AIR_INTERNAL_TOKEN` (default: `air-dev-internal-token`).

- **HTTP Requests**:
  - `X-Air-Internal-Token: <AIR_INTERNAL_TOKEN>` (recommended)
  - *or* `Authorization: Bearer <AIR_INTERNAL_TOKEN>`
- **gRPC Metadata**:
  - `x-air-internal-token: <AIR_INTERNAL_TOKEN>`
  - *or* `x-internal-token: <AIR_INTERNAL_TOKEN>`

---

## 1. `air-server` Control Plane REST API (`:8080`)

### Health Check
```http
GET /healthz
```
- **Auth**: None
- **Response `200 OK`**: `{"status":"ok"}`

---

### Provision a Database
Provisions a new isolated PostgreSQL 17 tenant database across all 4 services (PostgreSQL 17 cluster, Pageserver namespace, Read Engine registration, and Gateway route).

```http
POST /v1/databases
POST /v1/namespaces
Authorization: Bearer <API_KEY>
Idempotency-Key: <unique-key>
Content-Type: application/json
```

**Request Body**:
```json
{
  "tenant_id": "analytics_db"
}
```
*(Optionally use `"namespace": "analytics_db"`)*

**Response `201 Created`**:
```json
{
  "account_id": "acct_default",
  "tenant_id": "analytics_db",
  "database": "analytics_db",
  "username": "app_user",
  "password": "<GENERATED_PASSWORD>",
  "direct_port": 18021,
  "system_identifier": "7711011875213824053",
  "generation": "gen_380e33b412c1b70a",
  "fencing_epoch": 1,
  "state": "ACTIVE"
}
```

---

### Get Database Metadata
```http
GET /v1/databases/:id
GET /v1/namespaces/:id
Authorization: Bearer <API_KEY>
```

**Response `200 OK`**:
```json
{
  "account_id": "acct_default",
  "tenant_id": "analytics_db",
  "direct_port": 18021,
  "system_identifier": "7711011875213824053",
  "fencing_epoch": 1,
  "generation": "gen_380e33b412c1b70a",
  "state": "ACTIVE"
}
```

---

### Delete / Purge Database
Cascades teardown in safe reverse order: drains gateway poolers → unregisters from read-engine → purges pageserver layers → stops PostgreSQL process and purges local storage.

```http
DELETE /v1/databases/:id
DELETE /v1/namespaces/:id
Authorization: Bearer <API_KEY>
```

**Response `200 OK`**:
```json
{
  "status": "purged",
  "tenant_id": "analytics_db"
}
```

---

### Backups & Point-in-Time Recovery (PITR)

#### Trigger Backup
```http
POST /v1/databases/:id/backups
Authorization: Bearer <API_KEY>
Content-Type: application/json

{"type": "FULL"}
```
*(Types: `"FULL"` or `"INCR"`)*

#### List Backups
```http
GET /v1/databases/:id/backups
Authorization: Bearer <API_KEY>
```

#### Verify Backup
```http
POST /v1/databases/:id/backups/:backup_id/verify
Authorization: Bearer <API_KEY>
Content-Type: application/json

{
  "stream_hash": true,
  "deep_restore": false
}
```

#### Restore Database / PITR
```http
POST /v1/databases/:id/restore
Authorization: Bearer <API_KEY>
Content-Type: application/json

{
  "backup_id": "bkp_20261125_001",
  "target_lsn": "0/16B2300",
  "target_time": "2026-11-25T01:00:00Z",
  "new_tenant_id": "restored_analytics_db",
  "verify_query": "SELECT COUNT(*) FROM users;"
}
```

#### Check Cold Offload Status
```http
GET /v1/databases/:id/offload
Authorization: Bearer <API_KEY>
```

#### Revoke API Key
```http
DELETE /v1/api-keys/:id
Authorization: Bearer <API_KEY>
```

---

## 2. `air-gateway` Admin HTTP API (`:8081`)

Protected by `AIR_INTERNAL_TOKEN` (via `X-Air-Internal-Token` or `Authorization: Bearer <token>`).

### Tenant & Namespace Inspection
```http
# Check gateway health (No Auth)
GET /healthz

# List all discovered namespaces
GET /v1/namespaces

# Inspect tenant connectivity to all 3 backends
GET /v1/namespaces/:id
```

**Response `GET /v1/namespaces/analytics_db`**:
```json
{
  "service": "air-gateway",
  "tenant_id": "analytics_db",
  "namespace": "analytics_db",
  "state": "ACTIVE",
  "air_connected": true,
  "air_host": "air-server",
  "air_port": 18021,
  "read_engine_connected": true,
  "read_engine_host": "air-read-engine",
  "read_engine_port": 5433,
  "pageserver_connected": true,
  "pageserver_exists": true,
  "pageserver_generation": "gen_380e33b412c1b70a",
  "pageserver_last_lsn": 28811018
}
```

### Tenant Lifecycle Controls (Proxy via gRPC)
```http
# Suspend idle database (drain connections and scale to zero)
POST /v1/databases/:id/suspend

# Wake / start suspended database
POST /v1/databases/:id/start
POST /v1/databases/:id/wake

# Lock / unlock tenant access
POST /v1/databases/:id/lock
POST /v1/databases/:id/unlock
```

### Metrics & Connection Governance
- `GET /metrics`: Standard Prometheus metrics endpoint.
- `GET /internal/v1/metrics`: Detailed JSON metrics breakdown (supports `?tenant_id=...&user=...`).
- `GET /internal/v1/write-governor`: Snapshot of adaptive write governor, active leases, and token bucket status.
- `POST /internal/v1/tenants/:id/limits`: Dynamically set tenant connection limits:
  ```json
  {"max_client_conns": 50, "max_backends": 10}
  ```
- `POST /internal/v1/tenants/:id/drain?timeout=3s`: Force connection drain.

---

## 3. `air-read-engine` Admin HTTP API (`:8082`)

Protected by `AIR_INTERNAL_TOKEN`.

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/healthz` | Health, SQLite version, PG wire port (`5433`), query statistics |
| `GET` | `/v1/status` | Read-engine status and active tenant list |
| `GET` | `/metrics` | Read engine query, cache, and latency metrics |
| `GET` | `/v1/namespaces` | List registered read tenants |
| `POST` | `/v1/namespaces` | Ensure a tenant is registered (`{"tenant_id":"..."}`) |
| `DELETE` | `/v1/namespaces/:id` | Evict tenant and its cached SQLite state |
| `POST` | `/v1/tenants/:id/sync` | Manually sync tenant LSN or apply SQL update |

---

## 4. `air-pageserver` Admin HTTP API (`:8083`)

Protected by `AIR_INTERNAL_TOKEN`.

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/healthz` | Health check & active tenant count |
| `GET` | `/v1/namespaces` | List registered tenants, generations, and last LSN |
| `POST` | `/v1/namespaces` | Register/ensure tenant storage layer |
| `DELETE` | `/v1/namespaces/:id` | Purge tenant storage and delete local cache |

---

## 5. gRPC Protobuf Services

For microservices, Kubernetes operators, and internal mesh communication, AIR exposes gRPC services:

### 1. `AirDBControl` (`proto/airdb.proto` - Port `:9090`)
Implemented by `air-server`:
- `CreateDatabase(CreateDatabaseRequest) returns (CreateDatabaseResponse)`
- `GetDatabase(TenantRequest) returns (DatabaseInfo)`
- `ListDatabases(ListDatabasesRequest) returns (ListDatabasesResponse)`
- `DeleteDatabase(TenantRequest) returns (StatusResponse)`
- `SuspendDatabase(TenantRequest) returns (StatusResponse)`
- `StartDatabase(TenantRequest) returns (TenantRouteResponse)`
- `LockDatabase(TenantRequest) returns (StatusResponse)`
- `UnlockDatabase(TenantRequest) returns (StatusResponse)`
- `WatchTenantChanges(WatchTenantsRequest) returns (stream TenantChangeEvent)`
- `TriggerBackup`, `ListBackups`, `VerifyBackup`, `RestoreDatabase`

### 2. `GatewayAdmin` (`proto/airdb.proto` - Port `:9093`)
Implemented by `air-gateway`:
- `RegisterNamespace(RegisterNamespaceRequest) returns (NamespaceStatusResponse)`
- `DeleteNamespace(TenantRequest) returns (StatusResponse)`
- `GetNamespace(TenantRequest) returns (NamespaceInfoResponse)`
- `DrainTenant(DrainTenantRequest) returns (StatusResponse)`
- `SetTenantLimits(SetTenantLimitsRequest) returns (StatusResponse)`

### 3. `PageserverService` (`proto/pageserver.proto` - Port `:9091`)
Implemented by `air-pageserver`:
- Data plane: `GetPageAtLSN`, `GetPageBatch`, `GetRelSize`, `GetRelExists`, `GetDBSize`, `PutPageBatch`
- Control plane: `EnsureTenant`, `BootstrapTenantFromPGData`, `IngestWALRecords`, `FlushTenant`, `PurgeTenant`

### 4. `ReadEngineService` (`proto/readengine.proto` - Port `:9092`)
Implemented by `air-read-engine`:
- `EnsureTenant`, `GetTenantRoute`, `DeleteTenant`, `ListTenants`, `SyncTenant`

---

## 6. Client Database Connection (PostgreSQL Wire Protocol)

Applications connect to **`air-gateway` on port `5432`**:

```text
postgresql://app_user:<PASSWORD>@<GATEWAY_HOST>:5432/<TENANT_ID>?sslmode=disable
```

### Connection Characteristics
- **Read/Write Query Routing**: `SELECT` queries without volatile functions are transparently routed to `air-read-engine` (`:5433`). Write operations (`INSERT`, `UPDATE`, `DELETE`, `CREATE`, `ALTER`) and transactions with writes are routed to the primary PostgreSQL compute node.
- **Scale-to-Zero Auto-Wake**: If a tenant database was suspended, connecting or issuing a query to `air-gateway` automatically wakes the tenant compute and resumes execution transparently.
- **Connection Pooling**: Supports transaction-mode connection pooling (`AIR_POOL_MODE=true`).
- **TLS Security**: Enforces cryptographic SNI database name binding when TLS is enabled.
