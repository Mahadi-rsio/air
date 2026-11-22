# AIR Setup Guide

This guide covers running the 4-service AIR stack (`air-server`, `air-pageserver`, `air-read-engine`, `air-gateway`) with an external PostgreSQL metadata database (`air-meta-postgres`), either locally with Docker Compose or from source.

---

## Architecture Overview

```text
                    +------------------------+
Client (psql/SDK) ->|   air-gateway (:5432)  |<- Admin HTTP (:8081) / gRPC (:9093)
                    +---+--------+-------+---+
                        |        |       |
            gRPC (:9090)|   gRPC |       | gRPC (:9091)
                        v (:9092)|       v
+-------------------+   +--------+-------+   +------------------------+
| air-meta-postgres |<--|   air-server   |-->|     air-pageserver     |
|  (PG 17 :5435)    |   | (:8080 / :9090)|   |    (:8083 / :9091)     |
+-------------------+   +--------+-------+   +-----------+------------+
                                 |                       ^
                                 | gRPC (:9092)          | gRPC (:9091)
                                 v                       |
                        +--------+-----------------------+--+
                        |  air-read-engine (:5433/:8082/:9092) |
                        +-----------------------------------+
```

- **External Metadata PostgreSQL (`air-meta-postgres`)**: Stores control-plane metadata and query metering (`clusters`, `tenant_events`, `api_keys`, `provisioning_ops`, `idempotency_records`, `backup_ops`, `offload_ops`, `restore_ops`, `query_metering`, `schema_migrations`) via `AIR_META_DSN`.
- **Shared Master KEK (`AIR_MASTER_KEY_HEX`)**: 32-byte (64 hex chars) key shared between `air-server` and `air-pageserver` so envelope-encrypted tenant DEKs can be unwrapped across separate containers/hosts.
- **Internal Mesh Auth (`AIR_INTERNAL_TOKEN`)**: Shared secret authenticating all inter-service gRPC and Admin HTTP calls.

---

## Prerequisites

### Docker Setup (Recommended)
- Docker Engine 24+ with Docker Compose v2 (`docker compose`)
- `curl` and `psql` (PostgreSQL client) for verification
- ~4 GB free disk space if building `Dockerfile` (PostgreSQL 17.12 is compiled from source with the `smgr_hook` patch, `supautils`, and `air_smgr`)

### Bare-Metal / Development Setup
- Go 1.26+
- PostgreSQL 17 binaries (`/usr/lib/postgresql/17/bin`)
- `gcc`, `libsqlite3-dev`, `pkg-config` (for `air-read-engine` CGo build)
- `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc` (only if modifying `proto/*.proto`)

---

## Option 1: Run with Docker Compose (Local Build)

1. **Clone the repository**:
   ```bash
   git clone https://github.com/Mahadi-rsio/air.git
   cd air
   ```

2. **(Optional) Configure secrets in `.env`**:
   Defaults are provided for local dev, but for non-dev environments generate strong secrets:
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

3. **Build all 4 service images locally and start the stack**:
   ```bash
   docker compose build
   docker compose up -d --wait
   docker compose ps
   ```

4. **Verify health of all 4 services**:
   ```bash
   curl -fsS http://127.0.0.1:8080/healthz   # air-server
   curl -fsS http://127.0.0.1:8081/healthz   # air-gateway
   curl -fsS http://127.0.0.1:8082/healthz   # air-read-engine
   curl -fsS http://127.0.0.1:8083/healthz   # air-pageserver
   ```

---

## Option 2: Run with Prebuilt GHCR Images

Prebuilt images are published to GitHub Container Registry:
- `ghcr.io/mahadi-rsio/air:latest` (or `:v2.0.27`)
- `ghcr.io/mahadi-rsio/air-pageserver:latest` (or `:v2.0.27`)
- `ghcr.io/mahadi-rsio/air-read-engine:latest` (or `:v2.0.27`)
- `ghcr.io/mahadi-rsio/air-gateway:latest` (or `:v2.0.28`)

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

## Using Your Own External PostgreSQL Metadata Database

Instead of the bundled `air-meta-postgres` container, point `AIR_META_DSN` to any PostgreSQL 15+ / 17 database (e.g., RDS, Cloud SQL, Neon, self-hosted HA Postgres):

```bash
export AIR_META_DSN="postgres://<user>:<password>@<pg-host>:5432/<db>?sslmode=require"
```

Pass `AIR_META_DSN` to both `air-server` and `air-gateway`. On startup, `air-server` automatically runs schema migrations and creates the 10 control-plane tables (including `query_metering`), and `air-gateway` uses `AIR_META_DSN` to flush buffered query metering batches.

---

## Create a Database & Test the Connection

1. **Provision a database (`demo_db`) across all 4 services**:
   ```bash
   curl -sS -X POST http://127.0.0.1:8080/v1/databases \
     -H "Authorization: Bearer air_dev_bootstrap_key" \
     -H "Idempotency-Key: idem-demo-db-001" \
     -H "Content-Type: application/json" \
     -d '{"tenant_id":"demo_db"}'
   ```
   Copy the returned `password` from the JSON response.

2. **Verify the namespace across all services**:
   ```bash
   # External metadata PostgreSQL
   PGPASSWORD=air_meta_pass psql -h 127.0.0.1 -p 5435 -U air_meta -d air_meta \
     -c "SELECT tenant_id, account_id, state, generation, direct_port FROM clusters;"

   # Pageserver, Read Engine, and Gateway Admin APIs
   curl -H "X-Air-Internal-Token: air-dev-internal-token" http://127.0.0.1:8083/v1/namespaces
   curl -H "X-Air-Internal-Token: air-dev-internal-token" http://127.0.0.1:8082/v1/namespaces
   curl -H "X-Air-Internal-Token: air-dev-internal-token" http://127.0.0.1:8081/v1/namespaces/demo_db
   ```

3. **Connect through `air-gateway` (`:5432`) and run SQL**:
   ```bash
   psql "postgresql://app_user:<PASSWORD>@127.0.0.1:5432/demo_db?sslmode=disable" <<'SQL'
   SELECT current_user, current_database(), version();
   CREATE TABLE items (id serial PRIMARY KEY, label text NOT NULL);
   INSERT INTO items (label) VALUES ('hello-air');
   SELECT * FROM items;
   SQL
   ```

4. **Inspect Query Metering (`query_metering` in `air-meta-postgres`)**:
   `air-gateway` buffers each query (`timestamp`, `db_id`, `bandwidth_bytes`, `query_time_us`, `query_type`) into `query_metering.log` and flushes to PostgreSQL via `COPY` whenever the log reaches 10 MB or every 2 minutes (deleting the rotated log file after persistence):
   ```bash
   PGPASSWORD=air_meta_pass psql -h 127.0.0.1 -p 5435 -U air_meta -d air_meta \
     -c "SELECT id, recorded_at, db_id, bandwidth_bytes, query_time_us, query_type FROM query_metering ORDER BY id DESC LIMIT 20;"
   ```

---

## Environment Variables Reference

### Shared / Security
| Variable | Services | Description |
| :--- | :--- | :--- |
| `AIR_INTERNAL_TOKEN` | All 4 | Shared secret for inter-service gRPC and Admin HTTP auth |
| `AIR_MASTER_KEY_HEX` | `air-server`, `air-pageserver` | 64-char hex (32-byte) master KEK for envelope encryption |
| `AIR_META_DSN` | `air-server`, `air-gateway` | External PostgreSQL DSN for control-plane metadata |

### `air-server`
| Variable | Default (Compose) | Description |
| :--- | :--- | :--- |
| `AIR_DATA_DIR` | `/var/lib/air` | Persistent local tenant data directory |
| `AIR_PG_BIN_DIR` | `/usr/lib/postgresql/17/bin` | Patched PostgreSQL 17 binary directory |
| `AIR_HTTP_ADDR` | `0.0.0.0:8080` | Control Plane HTTP listen address |
| `AIR_GRPC_ADDR` | `0.0.0.0:9090` | `AirDBControl` gRPC listen address |
| `AIR_PAGESERVER_GRPC_ADDR` | `air-pageserver:9091` | Pageserver gRPC endpoint |
| `AIR_READ_ENGINE_GRPC_ADDR` | `air-read-engine:9092` | Read Engine gRPC endpoint |
| `AIR_GATEWAY_ADMIN_GRPC_ADDR` | `air-gateway:9093` | Gateway Admin gRPC endpoint |
| `AIR_BACKEND_HOST` | `air-server` | Hostname advertised to gateway for tenant PG backends |
| `AIR_BOOTSTRAP_API_KEY` | `air_dev_bootstrap_key` | Initial Bearer API key seeded on startup |
| `AIR_BOOTSTRAP_ACCOUNT_ID` | `acct_default` | Account ID for bootstrap API key |

### `air-pageserver`
| Variable | Default (Compose) | Description |
| :--- | :--- | :--- |
| `AIR_DATA_DIR` | `/var/lib/air-pageserver` | Layer and cache storage root |
| `AIR_PAGESERVER_GRPC_ADDR` | `0.0.0.0:9091` | `PageserverService` gRPC listen address |
| `AIR_PAGESERVER_HTTP_ADDR` | `0.0.0.0:8083` | Admin & health HTTP listen address |
| `AIR_PAGESERVER_CACHE_MB` | `256` | Bounded LRU disk cache size (MB) |

### `air-read-engine`
| Variable | Default (Compose) | Description |
| :--- | :--- | :--- |
| `AIR_READ_ENGINE_DATA_DIR` | `/var/lib/air-read-engine` | Read engine storage root |
| `AIR_READ_ENGINE_CACHE_DIR` | `/var/lib/air-read-engine/cache` | Page cache directory |
| `AIR_READ_ENGINE_PG_PORT` | `5433` | PostgreSQL wire-protocol read port |
| `AIR_READ_ENGINE_HTTP_ADDR` | `0.0.0.0:8082` | Admin & metrics HTTP listen address |
| `AIR_READ_ENGINE_GRPC_ADDR` | `0.0.0.0:9092` | `ReadEngineService` gRPC listen address |
| `AIR_PAGESERVER_GRPC_ADDR` | `air-pageserver:9091` | Upstream pageserver gRPC endpoint |

### `air-gateway`
| Variable | Default (Compose) | Description |
| :--- | :--- | :--- |
| `AIR_GATEWAY_ADDR` | `0.0.0.0:5432` | Client PostgreSQL wire listen address |
| `AIR_GATEWAY_ADMIN_ADDR` | `0.0.0.0:8081` | Admin HTTP & Prometheus metrics listen address |
| `AIR_GATEWAY_ADMIN_GRPC_ADDR` | `0.0.0.0:9093` | `GatewayAdmin` gRPC listen address |
| `AIR_CONTROL_PLANE_URL` | `http://air-server:8080` | Control Plane HTTP URL |
| `AIR_DB_GRPC_ADDR` | `air-server:9090` | `AirDBControl` gRPC endpoint |
| `AIR_PAGESERVER_GRPC_ADDR` | `air-pageserver:9091` | `PageserverService` gRPC endpoint |
| `AIR_READ_ENGINE_GRPC_ADDR` | `air-read-engine:9092` | `ReadEngineService` gRPC endpoint |
| `AIR_READ_ENGINE_HOST` / `PORT` | `air-read-engine` / `5433` | Read engine PG wire endpoint |
| `AIR_POOL_MODE` | `true` | Enable transaction-mode connection pooling |
| `AIR_METERING_LOG_DIR` | `/tmp/air-gateway-metering` | Directory for buffered `query_metering.log` files |
| `AIR_METERING_MAX_FILE_BYTES` | `10485760` (10 MB) | Rotate & flush log file to `query_metering` when size threshold is reached |
| `AIR_METERING_FLUSH_INTERVAL_SEC` | `120` (2 min) | Periodic flush interval (in seconds) to persist metering logs to `AIR_META_DSN` |

### Optional S3 / Cloudflare R2 Cold Storage
Set on `air-server` and `air-pageserver`: `AIR_R2_ENDPOINT`, `AIR_R2_BUCKET`, `AIR_R2_ACCESS_KEY_ID`, `AIR_R2_SECRET_ACCESS_KEY`, `AIR_R2_REGION` (default `auto`).

---

## Development & Testing

### Build All 4 Binaries Locally
```bash
go build -o bin/air-server ./cmd/air-server
go build -o bin/air-pageserver ./cmd/air-pageserver
go build -o bin/air-read-engine ./cmd/air-read-engine
go build -o bin/air-gateway ./cmd/air-gateway
```

### Run Unit & 4-Service E2E Mesh Tests
```bash
# Unit tests
go test -short ./internal/...

# Full 4-service gRPC mesh E2E test (launches all 4 binaries separately)
go test -v ./tests/e2e/... -run TestE2E_FourServiceGRPCMesh_NamespaceProvisioningAndIsolation
```

### Publishing New GHCR Release Tags
GitHub Actions skips tag push workflows when more than 3 tags are pushed in a single `git push`. Push each per-service tag individually:
```bash
VERSION=v2.0.28
git tag -a "$VERSION" -m "Release $VERSION"
git tag -a "server-$VERSION" -m "air-server $VERSION"
git tag -a "pageserver-$VERSION" -m "air-pageserver $VERSION"
git tag -a "read-engine-$VERSION" -m "air-read-engine $VERSION"
git tag -a "gateway-$VERSION" -m "air-gateway $VERSION"

git push origin "$VERSION"
git push origin "server-$VERSION"
git push origin "pageserver-$VERSION"
git push origin "read-engine-$VERSION"
git push origin "gateway-$VERSION"
```

---

## Stopping & Cleanup

```bash
# Stop containers (keep volumes/data)
docker compose down

# Stop containers and wipe all local volumes
docker compose down -v
```
