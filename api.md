# AIR API Reference

AIR provides a unified multi-tenant database platform built with 4 interconnected services communicating over an authenticated gRPC mesh, with state recorded in an external PostgreSQL metadata database (`AIR_META_DSN`).

---

## Service Overview & Ports

`air-gateway` is the **single Control Plane and client entrypoint**. The three backend services (`air-server`, `air-read-engine`, `air-pageserver`) do **not** expose HTTP admin APIs — they communicate exclusively over authenticated gRPC (`AIR_INTERNAL_TOKEN`).

| Service | HTTP Control Plane | gRPC Port | Client Wire Port | Primary Role |
| :--- | :--- | :--- | :--- | :--- |
| **`air-gateway`** | `:8081` | `:9093` | `:5432` (PG wire) | **Single Control Plane**, PG client entrypoint, pooler, query router |
| **`air-server`** | *(none)* | `:9090` | Dynamic (tenant PG) | Primary PG17 compute, KMS & backup coordinator |
| **`air-read-engine`** | *(none)* | `:9092` | `:5433` (PG wire) | High-speed SQLite read replica for `SELECT` queries |
| **`air-pageserver`** | *(none)* | `:9091` | IPC socket (optional)| Pageserver storage engine, `GetPage@LSN` provider & R2/S3 sync |

---

## Authentication Model

All HTTP Control Plane endpoints on `air-gateway` (`:8081`) and all inter-service gRPC calls require the shared `AIR_INTERNAL_TOKEN` (default: `air-dev-internal-token`), except `GET /healthz` which is unauthenticated.

- **HTTP Requests** (`air-gateway` `:8081`):
  - `X-Air-Internal-Token: <AIR_INTERNAL_TOKEN>` (recommended)
  - *or* `Authorization: Bearer <AIR_INTERNAL_TOKEN>`
- **gRPC Metadata** (internal mesh):
  - `x-air-internal-token: <AIR_INTERNAL_TOKEN>`
  - *or* `x-internal-token: <AIR_INTERNAL_TOKEN>`

---

## 1. `air-gateway` Unified Control Plane API (`:8081`)

### Health & Cluster Status
```http
# Gateway liveness check (No Auth)
GET /healthz

# Aggregated health across air-gateway, air-server, air-read-engine, and air-pageserver (via gRPC)
GET /v1/cluster/health
GET /v1/status
```
- **Response `GET /healthz`**: `{"status":"ok"}`

---

### Provision a Database
Orchestrates database creation across `air-server`, `air-read-engine`, and `air-pageserver` via gRPC, and registers the route in `air-gateway`.

```http
POST /v1/databases
X-Air-Internal-Token: <AIR_INTERNAL_TOKEN>
Idempotency-Key: <unique-key>
Content-Type: application/json
```

**Request Body**:
```json
{
  "tenant_id": "analytics_db",
  "account_id": "acct_default"
}
```
*(Optionally use `"namespace": "analytics_db"`)*

**Response `201 Created`**:
```json
{
  "tenant_id": "analytics_db",
  "state": "ACTIVE",
  "direct_port": 18021,
  "host": "air-server",
  "user": "analytics_db",
  "password": "<GENERATED_PASSWORD>"
}
```

---

### List & Get Databases
```http
GET /v1/databases
GET /v1/databases?account_id=acct_default
GET /v1/databases/:id
X-Air-Internal-Token: <AIR_INTERNAL_TOKEN>
```

**Response `200 OK` (`GET /v1/databases/:id`)**:
```json
{
  "account_id": "acct_default",
  "tenant_id": "analytics_db",
  "direct_port": 18021,
  "system_identifier": "7711011875213824053",
  "fencing_epoch": 1,
  "state": "ACTIVE"
}
```

---

### Delete / Purge Database
Cascades teardown in safe reverse order via gRPC: drains gateway poolers → deletes from `air-read-engine` → stops PostgreSQL and purges `air-server` local storage → purges `air-pageserver` layers.

```http
DELETE /v1/databases/:id
X-Air-Internal-Token: <AIR_INTERNAL_TOKEN>
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

## 2. Gateway Namespace, Read Engine & Pageserver Admin (`:8081`)

All admin and backend management endpoints are served by `air-gateway` (`:8081`) and dispatched to `air-server`, `air-read-engine`, and `air-pageserver` over gRPC. Protected by `AIR_INTERNAL_TOKEN`.

### Tenant & Namespace Inspection
```http
# List all discovered namespaces
GET /v1/namespaces

# Ensure/register namespace across gateway, read-engine, and pageserver
POST /v1/namespaces

# Inspect tenant connectivity to all 3 backends
GET /v1/namespaces/:id

# Delete namespace across gateway, read-engine, and pageserver
DELETE /v1/namespaces/:id
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

### Tenant Lifecycle Controls (via `AirDBControl` gRPC)
```http
POST /v1/databases/:id/suspend
POST /v1/databases/:id/start
POST /v1/databases/:id/wake
POST /v1/databases/:id/lock
POST /v1/databases/:id/unlock
```

### Read Engine Admin (via `ReadEngineService` gRPC)
| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/v1/read-engine/tenants` | List registered read-engine tenants |
| `GET` | `/v1/read-engine/tenants/:id` | Get read-engine route & state for tenant |
| `POST` | `/v1/read-engine/tenants/:id/sync` | Trigger SQL or primary_conninfo sync |
| `DELETE` | `/v1/read-engine/tenants/:id` | Remove tenant from read-engine |

### Pageserver Admin (via `PageserverService` gRPC)
| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/v1/pageserver/tenants` | List registered tenants, generations, and last LSN |
| `GET` | `/v1/pageserver/tenants/:id` | Get pageserver tenant info |
| `POST` | `/v1/pageserver/tenants/:id/flush` | Flush tenant WAL/layers to disk/R2 |
| `POST` | `/v1/pageserver/flush` | Flush all pageserver tenants |
| `DELETE` | `/v1/pageserver/tenants/:id` | Purge tenant storage and local cache |

### Metrics & Connection Governance
- `GET /metrics`: Standard Prometheus metrics endpoint.
- `GET /internal/v1/metrics`: Detailed JSON metrics breakdown (supports `?tenant_id=...&user=...`).
- `GET /internal/v1/write-governor`: Snapshot of adaptive write governor, active leases, and token bucket status.
- `POST /internal/v1/tenants/:id/limits`: Dynamically set tenant connection limits (`{"max_client_conns": 50, "max_backends": 10}`).
- `POST /internal/v1/tenants/:id/drain?timeout=3s`: Force connection drain.

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
postgresql://<TENANT_ID>:<PASSWORD>@<GATEWAY_HOST>:5432?sslmode=disable
```

### Connection Characteristics
- **Read/Write Query Routing**: `SELECT` queries without volatile functions are transparently routed to `air-read-engine` (`:5433`). Write operations (`INSERT`, `UPDATE`, `DELETE`, `CREATE`, `ALTER`) and transactions with writes are routed to the primary PostgreSQL compute node.
- **Scale-to-Zero Auto-Wake**: If a tenant database was suspended, connecting or issuing a query to `air-gateway` automatically wakes the tenant compute and resumes execution transparently.
- **Connection Pooling**: Supports transaction-mode connection pooling (`AIR_POOL_MODE=true`).
- **TLS Security**: Enforces cryptographic SNI database name binding when TLS is enabled.
