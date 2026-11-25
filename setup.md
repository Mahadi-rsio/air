# AIR Setup Guide

This guide covers running the 4-service AIR stack (`air-server`, `air-pageserver`, `air-read-engine`, `air-gateway`) with an external PostgreSQL metadata database (`air-meta-postgres`), either locally with Docker Compose or from source for development.

> [!TIP]
> If you are new to AIR and just want to run a quick test with step-by-step SQL operations, start with our beginner-focused [Quickstart Guide](quickstart.md).

---

## Architecture Overview

```text
                    +------------------------+
Client (psql/SDK) ->|   air-gateway (:5432)  |<- Admin HTTP (:8081) / gRPC (:9093)
                    +---+--------+-------+---+
                        |        |       |
            gRPC (:9090)|   gRPC |       | gRPC (:9091)
                        v (:9092)|       v
+-------------------+   +----------------+   +------------------------+
| air-meta-postgres |<--|   air-server   |-->|     air-pageserver     |
|  (PG 17 :5435)    |   |  (gRPC :9090)  |   |     (gRPC :9091)       |
+-------------------+   +--------+-------+   +-----------+------------+
                                 |                       ^
                                 | gRPC (:9092)          | gRPC (:9091)
                                 v                       |
                        +--------+-----------------------+--+
                        |   air-read-engine (:5433 / :9092) |
                        +-----------------------------------+
```

- **Single Control Plane & Client Gateway (`air-gateway`)**: Listens on HTTP Control Plane port `8081` and PostgreSQL wire port `5432`. Orchestrates `air-server`, `air-read-engine`, and `air-pageserver` over gRPC, enforces SNI database binding, manages backend connection pools, and performs transparent read/write query routing.
- **Primary Compute Engine (`air-server`)**: Coordinates tenant lifecycles, provisions isolated PostgreSQL 17 clusters, manages envelope KMS keys, coordinates backups, and triggers background cold offload. Exposes `AirDBControl` gRPC on `:9090` (no HTTP admin API).
- **Read Replica Engine (`air-read-engine`)**: Shared SQLite read replica reading PG17 pages directly from the Pageserver. Serves read queries on PG wire port `5433` and `ReadEngineService` gRPC on `:9092` (no HTTP admin API).
- **Pageserver (`air-pageserver`)**: Storage node intercepting relation page reads (`GetPage@LSN`), ingesting WAL records, generating L0/L1 layers, and synchronizing with Cloudflare R2 / S3 cold storage. Exposes `PageserverService` gRPC on `:9091` (no HTTP admin API).
- **External Metadata PostgreSQL (`air-meta-postgres`)**: Stores control-plane metadata and query metering (`clusters`, `tenant_events`, `api_keys`, `provisioning_ops`, `idempotency_records`, `backup_ops`, `offload_ops`, `restore_ops`, `query_metering`, `schema_migrations`) via `AIR_META_DSN`.
- **Shared Master KEK (`AIR_MASTER_KEY_HEX`)**: 32-byte (64 hex chars) key shared between `air-server` and `air-pageserver` so envelope-encrypted tenant DEKs can be unwrapped across separate containers/hosts.
- **Internal Mesh Auth (`AIR_INTERNAL_TOKEN`)**: Shared secret authenticating all inter-service gRPC and Admin HTTP calls.

---

## Prerequisites

### Docker Setup (Recommended)
- Docker Engine 24+ with Docker Compose v2 (`docker compose`)
- `curl` and `psql` (PostgreSQL client) for verification
- ~4 GB free disk space if building `Dockerfile` from source (PostgreSQL 17 is compiled from source with the `smgr_hook` patch, `supautils`, and `air_smgr`)

### Bare-Metal / Development Setup
- Go 1.24+ (tested on Go 1.24 / 1.26)
- PostgreSQL 17 binaries (`/usr/lib/postgresql/17/bin`: `initdb`, `pg_ctl`, `postgres`, `psql`)
- `gcc`, `libsqlite3-dev`, `pkg-config` (for `air-read-engine` CGo virtual table build)
- `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc` (only if modifying `proto/*.proto`)

---

## Option 1: Run with Docker Compose (Local Build)

1. **Clone the repository**:
   ```bash
   git clone https://github.com/Mahadi-rsio/air.git
   cd air
   ```

2. **(Optional) Configure secrets in `.env`**:
   Defaults are pre-configured in `docker-compose.yml` for local development, but for production or custom environments create a `.env` file:
   ```bash
   cat <<EOF > .env
   AIR_INTERNAL_TOKEN=$(openssl rand -hex 24)
   AIR_MASTER_KEY_HEX=$(openssl rand -hex 32)
   AIR_BOOTSTRAP_API_KEY=air_live_$(openssl rand -hex 16)
   AIR_BOOTSTRAP_ACCOUNT_ID=acct_default
   AIR_META_PG_USER=air_meta
   AIR_META_PG_PASSWORD=$(openssl rand -hex 16)
   AIR_META_PG_DB=air_meta
   EOF
   ```

3. **Build service images locally and start the stack**:
   ```bash
   docker compose build
   docker compose up -d --wait
   docker compose ps
   ```

4. **Verify health of all 4 services via `air-gateway`**:
   ```bash
   curl -fsS http://127.0.0.1:8081/healthz
   curl -fsS -H "X-Air-Internal-Token: air-dev-internal-token" http://127.0.0.1:8081/v1/cluster/health
   ```

---

## Option 2: Run with Prebuilt GHCR Images

Prebuilt images are published to GitHub Container Registry:
- `ghcr.io/mahadi-rsio/air:latest`
- `ghcr.io/mahadi-rsio/air-pageserver:latest`
- `ghcr.io/mahadi-rsio/air-read-engine:latest`
- `ghcr.io/mahadi-rsio/air-gateway:latest`

Create a `compose.ghcr.yml` override file:
```yaml
services:
  air-server:
    image: ghcr.io/mahadi-rsio/air:latest
  air-pageserver:
    image: ghcr.io/mahadi-rsio/air-pageserver:latest
  air-read-engine:
    image: ghcr.io/mahadi-rsio/air-read-engine:latest
  air-gateway:
    image: ghcr.io/mahadi-rsio/air-gateway:latest
```

Pull and start:
```bash
docker compose -f docker-compose.yml -f compose.ghcr.yml pull
docker compose -f docker-compose.yml -f compose.ghcr.yml up -d --wait
```

---

## Using an External PostgreSQL Metadata Database

Instead of the bundled `air-meta-postgres` container, point `AIR_META_DSN` to any PostgreSQL 15+ / 17 database (e.g. AWS RDS, Google Cloud SQL, Neon, Supabase, or self-hosted HA PostgreSQL):

```bash
export AIR_META_DSN="postgres://<user>:<password>@<pg-host>:5432/<db>?sslmode=require"
```

Pass `AIR_META_DSN` to both `air-server` and `air-gateway`:
- On startup, `air-server` runs schema migrations automatically and provisions control-plane metadata tables (`clusters`, `tenant_events`, `api_keys`, `query_metering`, etc.).
- `air-gateway` uses `AIR_META_DSN` to flush buffered query metering records.

---

## Provisioning a Database & Running Operations

### Step 1: Provision a Database via `air-gateway`
Run the following `curl` command to provision a new tenant database `demo_db` across `air-server`, `air-read-engine`, and `air-pageserver`:

```bash
curl -sS -X POST http://127.0.0.1:8081/v1/databases \
  -H "X-Air-Internal-Token: air-dev-internal-token" \
  -H "Idempotency-Key: idem-demo-db-001" \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"demo_db","account_id":"acct_default"}'
```

Output:
```json
{
  "account_id": "acct_default",
  "tenant_id": "demo_db",
  "database": "demo_db",
  "username": "demo_db",
  "password": "<GENERATED_PASSWORD>",
  "direct_port": 18021,
  "system_identifier": "7711011875213824053",
  "generation": "gen_380e33b412c1b70a",
  "fencing_epoch": 1,
  "state": "ACTIVE"
}
```

### Step 2: Verify Registration Across the Mesh
Inspect how the tenant is wired across all components:

```bash
# Check Control Plane cluster metadata
PGPASSWORD=air_meta_pass psql -h 127.0.0.1 -p 5435 -U air_meta -d air_meta \
  -c "SELECT tenant_id, account_id, state, generation, direct_port FROM clusters;"

# Check Pageserver registration (via gateway gRPC proxy)
curl -H "X-Air-Internal-Token: air-dev-internal-token" http://127.0.0.1:8081/v1/pageserver/tenants

# Check Read-Engine registration (via gateway gRPC proxy)
curl -H "X-Air-Internal-Token: air-dev-internal-token" http://127.0.0.1:8081/v1/read-engine/tenants

# Check Gateway routing and backend connectivity
curl -H "X-Air-Internal-Token: air-dev-internal-token" http://127.0.0.1:8081/v1/namespaces/demo_db
```

### Step 3: Connect and Execute SQL
Connect through **`air-gateway` on port `5432`**:

```bash
psql "postgresql://demo_db:<GENERATED_PASSWORD>@127.0.0.1:5432?sslmode=disable" <<'SQL'
SELECT current_user, current_database(), version();

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO users (username) VALUES ('alice'), ('bob');

SELECT * FROM users;
SQL
```

### Step 4: Verify Query Metering
AIR Gateway logs every tenant query and flushes to `query_metering` in `air-meta-postgres`:

```bash
PGPASSWORD=air_meta_pass psql -h 127.0.0.1 -p 5435 -U air_meta -d air_meta \
  -c "SELECT id, recorded_at, db_id, bandwidth_bytes, query_time_us, query_type FROM query_metering ORDER BY id DESC LIMIT 10;"
```

---

## Environment Variables Reference

### Shared / Security
| Variable | Services | Description |
| :--- | :--- | :--- |
| `AIR_INTERNAL_TOKEN` | All 4 | Shared secret for inter-service gRPC and Admin HTTP authentication |
| `AIR_MASTER_KEY_HEX` | `air-server`, `air-pageserver` | 64-character hex (32-byte) master key encryption key (KEK) |
| `AIR_META_DSN` | `air-server`, `air-gateway` | PostgreSQL connection string for platform metadata |

### `air-server` (Primary Compute & Control Backend)
| Variable | Default (Compose) | Description |
| :--- | :--- | :--- |
| `AIR_DATA_DIR` | `/var/lib/air` | Root directory for per-tenant clusters |
| `AIR_PG_BIN_DIR` | `/usr/lib/postgresql/17/bin` | Path to PostgreSQL 17 binaries |
| `AIR_GRPC_ADDR` | `0.0.0.0:9090` | `AirDBControl` gRPC listen address |
| `AIR_PAGESERVER_GRPC_ADDR` | `air-pageserver:9091` | Pageserver gRPC endpoint |
| `AIR_READ_ENGINE_GRPC_ADDR` | `air-read-engine:9092` | Read Engine gRPC endpoint |
| `AIR_GATEWAY_ADMIN_GRPC_ADDR` | `air-gateway:9093` | Gateway Admin gRPC endpoint |
| `AIR_BACKEND_HOST` | `air-server` | Hostname advertised to gateway for tenant PG backends |
| `AIR_BOOTSTRAP_API_KEY` | `air_dev_bootstrap_key` | Initial Bearer API token seeded on startup |
| `AIR_BOOTSTRAP_ACCOUNT_ID` | `acct_default` | Account ID associated with the bootstrap API key |
| `AIR_PAGESERVER_CACHE_MB` | `64` | Pageserver disk cache and `air_smgr.lfc_max_mb` size (MB) |
| `AIR_IDLE_TIMEOUT` | *(empty)* | Idle timeout (e.g., `10m`) for automatic scale-to-zero suspension |
| `AIR_EVICT_ON_SUSPEND` | `true` | Delete local tenant cache on suspend |

### `air-pageserver` (Storage Engine)
| Variable | Default (Compose) | Description |
| :--- | :--- | :--- |
| `AIR_DATA_DIR` | `/var/lib/air-pageserver` | Root directory for layers and disk cache |
| `AIR_PAGESERVER_GRPC_ADDR` | `0.0.0.0:9091` | `PageserverService` gRPC listen address |
| `AIR_PAGESERVER_CACHE_MB` | `64` | Maximum size of local LRU layer cache (MB) |

### `air-read-engine` (Read Replica)
| Variable | Default (Compose) | Description |
| :--- | :--- | :--- |
| `AIR_READ_ENGINE_DATA_DIR` | `/var/lib/air-read-engine` | Read engine working directory |
| `AIR_READ_ENGINE_CACHE_DIR` | `/var/lib/air-read-engine/cache` | Page cache directory |
| `AIR_READ_ENGINE_PG_PORT` | `5433` | PostgreSQL wire protocol read port |
| `AIR_READ_ENGINE_GRPC_ADDR` | `0.0.0.0:9092` | `ReadEngineService` gRPC listen address |
| `AIR_PAGESERVER_GRPC_ADDR` | `air-pageserver:9091` | Upstream Pageserver gRPC address |

### `air-gateway` (Client Router & Pooler)
| Variable | Default (Compose) | Description |
| :--- | :--- | :--- |
| `AIR_GATEWAY_ADDR` | `0.0.0.0:5432` | Client PostgreSQL wire listen address |
| `AIR_GATEWAY_ADMIN_ADDR` | `0.0.0.0:8081` | Admin HTTP & metrics listen address |
| `AIR_GATEWAY_ADMIN_GRPC_ADDR` | `0.0.0.0:9093` | `GatewayAdmin` gRPC listen address |
| `AIR_DB_GRPC_ADDR` | `air-server:9090` | `AirDBControl` gRPC endpoint |
| `AIR_PAGESERVER_GRPC_ADDR` | `air-pageserver:9091` | Pageserver gRPC endpoint |
| `AIR_READ_ENGINE_GRPC_ADDR` | `air-read-engine:9092` | Read Engine gRPC endpoint |
| `AIR_READ_ENGINE_HOST` / `PORT` | `air-read-engine` / `5433` | Read Engine PG wire endpoint |
| `AIR_POOL_MODE` | `true` | Enable transaction connection pooling |
| `AIR_METERING_LOG_DIR` | `/tmp/air-gateway-metering` | Directory for query metering buffer files |
| `AIR_METERING_MAX_FILE_BYTES` | `10485760` (10 MB) | File size threshold to trigger flush to database |
| `AIR_METERING_FLUSH_INTERVAL_SEC` | `120` (2 min) | Flush interval in seconds |

### Optional Cloudflare R2 / S3 Cold Storage
Configure on `air-server` and `air-pageserver` for offload and backups:
- `AIR_R2_ENDPOINT` (e.g., `https://<account_id>.r2.cloudflarestorage.com`)
- `AIR_R2_BUCKET` (e.g., `air-backups`)
- `AIR_R2_ACCESS_KEY_ID`
- `AIR_R2_SECRET_ACCESS_KEY`
- `AIR_R2_REGION` (default `auto`)

---

## Local Development & Testing

### Compiling Binaries Locally
```bash
go build -o bin/air-server ./cmd/air-server
go build -o bin/air-pageserver ./cmd/air-pageserver
go build -o bin/air-read-engine ./cmd/air-read-engine
go build -o bin/air-gateway ./cmd/air-gateway
```

### Running Tests
```bash
# Unit tests
go test -short ./internal/...

# Full 4-service gRPC mesh end-to-end integration test
go test -v ./tests/e2e/... -run TestE2E_FourServiceGRPCMesh_NamespaceProvisioningAndIsolation
```

---

## Stopping & Teardown

```bash
# Stop containers (preserves volumes and data)
docker compose down

# Stop containers and delete all volumes
docker compose down -v
```
