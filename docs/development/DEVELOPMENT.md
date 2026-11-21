# AIR Developer Guide

This guide covers everything needed to build, run, test, and debug AIR locally.

---

## 1. Required Dependencies

| Dependency | Version / Path | Purpose |
| :--- | :--- | :--- |
| **Go** | `1.24+` | Builds `air-server`, internal packages, and Go E2E/unit test suites. |
| **PostgreSQL** | `17` (`/usr/lib/postgresql/17/bin`) | Provides `initdb`, `pg_ctl`, `pg_controldata`, `pg_basebackup`, `pg_combinebackup`, `pg_verifybackup`, `pg_amcheck`, and `psql`. |
| **`supautils.so`** | `/usr/lib/postgresql/17/lib/supautils.so` | Hardens tenant PostgreSQL instances against superuser/extension privilege escalation. |
| **C++20 Toolchain** | `g++`, `make`, `libgrpc++-dev`, `libprotobuf-dev`, `libpq-dev`, `libssl-dev` | Required only when building or running the standalone C++20 `air-gateway` (`gateway/`). |
| **Docker** | Engine 24+ (optional) | Needed only for container builds ([Dockerfile](file:///workspaces/air/Dockerfile)). |

> **Pageserver Mode (`AIR_PAGESERVER_ENABLED=true`)**: Requires PostgreSQL 17 compiled with [patches/pg17-smgr-hook.patch](file:///workspaces/air/patches/pg17-smgr-hook.patch) and `air_smgr.so` built from [extensions/air_smgr](file:///workspaces/air/extensions/air_smgr).

---

## 2. Environment Variables & CLI Flags ([internal/config/config.go](file:///workspaces/air/internal/config/config.go))

All runtime configuration is loaded from environment variables first, then overridden by CLI flags in `config.Load`:

| Environment Variable | CLI Flag | Default | Description |
| :--- | :--- | :--- | :--- |
| `AIR_DATA_DIR` | `--data-dir` | `/tmp/air-data` | Root directory for tenant PGDATA, warm pool, KMS, and backups. |
| `AIR_META_DIR` | `--meta-dir` | `<data-dir>/meta` | Directory for the embedded metadata PostgreSQL 17 cluster. |
| `AIR_META_DSN` | `--meta-dsn` | `""` | External HA PostgreSQL DSN for metadata (disables local `--meta-dir`). |
| `AIR_MASTER_KEY_PATH` | `--master-key-path` | `<data-dir>/master.key` | Path to the 32-byte EnvelopeKMS master KEK. |
| `AIR_PG_BIN_DIR` | `--pg-bin-dir` | `/usr/lib/postgresql/17/bin` | PostgreSQL 17 binary directory. |
| `AIR_META_PORT` | `--meta-port` | `15432` | TCP port for embedded metadata PostgreSQL 17. |
| `AIR_HTTP_ADDR` | `--http-addr` | `127.0.0.1:8080` | Control Plane HTTP API listen address. |
| `AIR_GRPC_ADDR` | `--grpc-addr` | `127.0.0.1:9090` | `AirDBControl` gRPC server listen address. |
| `AIR_GATEWAY_ADDR` | `--gateway-addr` | `127.0.0.1:5432` | PostgreSQL wire-protocol gateway listen address. |
| `AIR_GATEWAY_ADMIN_ADDR` | `--gateway-admin-addr` | `127.0.0.1:8081` | Gateway admin HTTP listen address. |
| `AIR_GATEWAY_ADMIN_URL` | `--gateway-admin-url` | `""` | Gateway admin HTTP URL used by `air-server` for pool draining. |
| `AIR_GATEWAY_ADMIN_GRPC_ADDR` | `--gateway-admin-grpc-addr` | `""` | Gateway admin gRPC address used by `air-server` for pool draining. |
| `AIR_CONTROL_PLANE_URL` | `--control-plane-url` | `http://127.0.0.1:8080` | Control Plane HTTP base URL. |
| `AIR_INTERNAL_TOKEN` | `--internal-token` | `""` | Shared secret for internal gateway $\leftrightarrow$ server calls. |
| `AIR_BACKEND_HOST` | `--backend-host` | `""` (`127.0.0.1`) | Backend PostgreSQL host returned to remote gateways. |
| `AIR_BOOTSTRAP_API_KEY` | `--bootstrap-api-key` | `""` | Optional initial Bearer token seeded on startup. |
| `AIR_BOOTSTRAP_ACCOUNT_ID` | `--bootstrap-account-id` | `acct_default` | Account ID bound to `--bootstrap-api-key`. |
| `AIR_REQUIRE_TLS` | `--require-tls` | `false` | Require TLS on Gateway client connections. |
| `AIR_TLS_CERT` / `AIR_TLS_KEY` | `--tls-cert` / `--tls-key` | `""` | PEM TLS certificate and private key paths. |
| `AIR_POOL_MODE` | `--pool-mode` | `false` | Enable transaction-mode connection pooling. |
| `AIR_POOL_MAX_BACKENDS` | `--pool-max-backends` | `16` | Max pooled backend connections per tenant. |
| `AIR_IDLE_TIMEOUT` | `--idle-timeout` | `0` (disabled) | Idle duration before cooperative scale-to-zero (`SUSPENDED`). |
| `AIR_R2_ENDPOINT` | `--r2-endpoint` | `""` | S3/R2 endpoint URL for backups, WAL, CAU layers, and fencing. |
| `AIR_R2_BUCKET` | `--r2-bucket` | `""` | S3/R2 bucket name. |
| `AIR_R2_ACCESS_KEY_ID` | `--r2-access-key-id` | `""` | S3/R2 access key ID. |
| `AIR_R2_SECRET_ACCESS_KEY` | `--r2-secret-access-key` | `""` | S3/R2 secret access key. |
| `AIR_R2_REGION` | `--r2-region` | `auto` | S3/R2 region. |
| `AIR_R2_PRIMARY` | `--r2-primary` | `false` | Use R2 as primary durable storage and evict local pgdata on suspend. |
| `AIR_EVICT_ON_SUSPEND` | `--evict-on-suspend` | `false` | Evict local tenant pgdata after cold upload to R2. |
| `AIR_PAGESERVER_ENABLED` | `--pageserver` | `false` | Enable in-process AIR Pageserver over Unix socket. |
| `AIR_PAGESERVER_SOCKET` | `--pageserver-socket` | `<data-dir>/pageserver.sock` | Unix socket path for `air_smgr` $\leftrightarrow$ `pageserver`. |
| `AIR_PAGESERVER_CACHE_MB` | `--pageserver-cache-mb` | `256` | Bounded LRU disk cache size (MB) for Pageserver. |
| `AIR_READ_ENGINE_HOST` | `--read-engine-host` | `127.0.0.1` | Read Engine PG wire host used by `air-gateway` and `air-read-engine`. |
| `AIR_READ_ENGINE_PORT` | `--read-engine-port` | `0` (`5433` in `air-read-engine`) | Read Engine PG wire port (`0` disables read routing in `air-gateway`). |
| `AIR_READ_ENGINE_SOCK_DIR` | `--read-engine-sock-dir` | `""` | Optional Read Engine Unix socket directory for `air-gateway`. |
| `AIR_READ_ENGINE_HTTP_ADDR` | `--http-addr` | `127.0.0.1:8082` | HTTP control/health listen address for `air-read-engine`. |
| `AIR_READ_ENGINE_PRIMARY_CONNINFO` | `--primary-conninfo` | `""` | Primary Cell conninfo used by `air-read-engine` for syncing from Write Engine. |

---

## 3. Build & Run Locally

### Build Go Server & Read Engine
```bash
go build ./cmd/air-server
go build ./cmd/air-read-engine
```

### Run `air-server` Locally
```bash
go run ./cmd/air-server \
  --data-dir /tmp/air-live \
  --http-addr 127.0.0.1:8080 \
  --grpc-addr 127.0.0.1:9090 \
  --meta-port 15432 \
  --bootstrap-account-id acct_demo \
  --bootstrap-api-key air_live_demo_secret_token_12345 \
  --internal-token air_internal_secret
```

### Run Shared `air-read-engine` Locally (Optional)
```bash
go run ./cmd/air-read-engine \
  -data-dir /tmp/air-read-engine \
  -http-addr 127.0.0.1:8082 \
  -pg-port 5433
```

### Build & Run `air-gateway` (Optional)
```bash
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

---

## 4. Testing Commands

### Unit & Race-Detector Tests (Fast, No Live PG Required for `internal/backup`, `fencing`, `crypto`, `tenant`, `config`)
```bash
go test -race -count=1 ./internal/...
```

### End-to-End Tests (Spawns Real PostgreSQL 17 Clusters + In-Process Mock S3/R2)
```bash
# Full E2E suite
go test -v ./tests/e2e/...

# Targeted Phase 13 Backup, Manifest Verification, PITR & Restore tests
go test -v ./tests/e2e -run "TestPhase13|TestPhaseF"
```

### Static Checks
```bash
go vet ./...
gofmt -l ./cmd ./internal ./tests
```

---

## 5. R2 Development Configuration

For local development and automated tests, AIR does **not** require a live Cloudflare R2 account:
- `tests/e2e` and `internal/backup/backup_test.go` spin up an in-process `httptest.Server` (`mockS3Server`) implementing `PutObject` (including `If-None-Match` and `If-Match`), `GetObject`, `HeadObject`, `DeleteObject`, `ListObjectsV2`, and `x-amz-meta-*` headers.
- To test against a real Cloudflare R2 bucket or MinIO instance locally, set:
  ```bash
  export AIR_R2_ENDPOINT="https://<account-id>.r2.cloudflarestorage.com"
  export AIR_R2_BUCKET="air-dev"
  export AIR_R2_ACCESS_KEY_ID="..."
  export AIR_R2_SECRET_ACCESS_KEY="..."
  export AIR_R2_REGION="auto"
  ```

---

## 6. Debugging & Common Development Problems

1. **`pg_ctl` / `initdb` fails with `cannot be run as root`**:
   - PostgreSQL 17 refuses to run as UID 0. When running tests inside a root container, `cell.Agent` automatically drops privileges to the `postgres` OS user when invoking PG binaries, or run tests as a non-root user (`codespace` / `vscode`).
2. **Orphaned PostgreSQL processes or `.air.lock` after killed test**:
   - Check `ps aux | grep postgres` and clean temporary directories under `/tmp/air-*` or `/tmp/Test*`.
3. **Port conflicts on `:15432`, `:8080`, `:9090`**:
   - E2E tests dynamically allocate free localhost ports (`127.0.0.1:0`), while manual runs default to `:15432` (meta PG), `:8080` (HTTP), and `:9090` (gRPC). Override via `--meta-port`, `--http-addr`, and `--grpc-addr`.
4. **`supautils.so` missing**:
   - Ensure `/usr/lib/postgresql/17/lib/supautils.so` is installed; otherwise tenant PostgreSQL startup with `shared_preload_libraries = 'supautils'` will fail.
