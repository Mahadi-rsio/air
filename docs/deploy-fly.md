# Deploying AIR to Fly.io

This comprehensive guide walks you through deploying the complete production 4-service AIR platform (`air-pg-gateway`, `air-pg-server`, `air-pg-read-engine`, and `air-pg-pageserver`) to [Fly.io](https://fly.io).

The deployment uses Fly's private 6PN IPv6 network for high-speed, secure inter-service communication, persistent NVMe volumes for storage, an external PostgreSQL metadata database (such as Neon, Supabase, or AWS RDS), and Cloudflare R2 / S3 for durable encrypted cold storage.

---

## 1. Architecture & Network Topology

All four services run in the same Fly region (default: `sin` or `iad`) and communicate over Fly's private IPv6 6PN network (`*.internal`).

> [!IMPORTANT]
> **Only `air-pg-gateway` receives a public IP address (a Dedicated IPv4).** All internal services (`air-pg-server`, `air-pg-read-engine`, `air-pg-pageserver`) have no public IPs and are only reachable within the private 6PN network.

```text
Internet Client (psql / SDK on :5432 or Admin HTTPS on :443)
        │
        ▼  Dedicated IPv4 (Public)
┌───────────────────────────────────────────┐
│              air-pg-gateway               │  shared-cpu-1x · 256MB · (No Volume)
└───────┬───────────────────────────┬───────┘
        │ 6PN Private Mesh          │ 6PN Private Mesh (Zero-Sync Reads)
        ▼                           ▼
┌───────────────────────┐   6PN   ┌───────────────────────────┐
│     air-pg-server     │◄───────►│    air-pg-read-engine     │
│  shared-cpu-2x·512MB  │         │   shared-cpu-1x · 256MB   │
│  vol: air_server_data │         │  vol: air_read_engine_data│
│   (2GB -> /var/lib)   │         │    (1GB -> /var/lib)      │
└───────────┬───────────┘         └─────────────┬─────────────┘
            │ 6PN Private Mesh                  │ 6PN Private Mesh
            ▼                                   ▼
      ┌───────────────────────────────────────────────┐
      │               air-pg-pageserver               │
      │             shared-cpu-1x · 256MB             │
      │      vol: air_pageserver_data (1GB)           │
      └───────────────────────┬───────────────────────┘
                              │
            ┌─────────────────┴─────────────────┐
            ▼                                   ▼
     Cloudflare R2                     External Metadata DB
   (Encrypted L0/L1 layers,             (Neon / RDS Postgres —
    WAL archives, manifests)             AIR_META_DSN)
```

---

## 2. Resource Specifications

Configuration files are located in [`deploy/fly/`](../deploy/fly/):

| Service | Fly App Name | Config File | VM Size | Persistent Volume | Public Network Exposure |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Gateway** | `air-pg-gateway` | [`deploy/fly/fly.gateway.toml`](../deploy/fly/fly.gateway.toml) | `shared-cpu-2x` / `256MB` | *None* | **Dedicated IPv4** (`:5432` raw TCP+TLS, `:443` HTTPS Admin API) |
| **Control Plane** | `air-pg-server` | [`deploy/fly/fly.server.toml`](../deploy/fly/fly.server.toml) | `shared-cpu-2x` / `512MB` | `air_server_data` (`2GB`) | *None* (internal 6PN IPv6) |
| **Read Engine** | `air-pg-read-engine` | [`deploy/fly/fly.read-engine.toml`](../deploy/fly/fly.read-engine.toml) | `shared-cpu-2x` / `256MB` | `air_read_engine_data` (`1GB`) | *None* (internal 6PN IPv6) |
| **Pageserver** | `air-pg-pageserver` | [`deploy/fly/fly.pageserver.toml`](../deploy/fly/fly.pageserver.toml) | `shared-cpu-2x` / `256MB` | `air_pageserver_data` (`1GB`) | *None* (internal 6PN IPv6) |

> [!NOTE]
> Fly's shared IPv4 only supports HTTP/TLS proxying. Raw PostgreSQL TCP wire traffic on port `5432` requires a **Dedicated IPv4** address on `air-pg-gateway`.

---

## 3. Prerequisites

1. **`flyctl` CLI**: Installed and logged into your account:
   ```bash
   fly auth login
   ```
2. **External Metadata PostgreSQL Database**:
   - Provider: [Neon](https://neon.tech), Supabase, AWS RDS, or self-hosted PostgreSQL 15+/17.
   - Connection string format:
     ```bash
     export AIR_META_DSN="postgres://<user>:<password>@<neon-host>/<db>?sslmode=require"
     ```
   - Schema tables (`clusters`, `tenant_events`, `query_metering`, `api_keys`, etc.) migrate automatically when `air-pg-server` starts.
3. **Cloudflare R2 Bucket**:
   - Create a bucket in Cloudflare (e.g., `air-pg-backups`).
   - S3-compatible endpoint: `https://<account_id>.r2.cloudflarestorage.com`
   - Access Key ID and Secret Access Key.
4. **Generate Deployment Secrets**:
   ```bash
   export AIR_INTERNAL_TOKEN="$(openssl rand -hex 32)"
   export AIR_MASTER_KEY_HEX="$(openssl rand -hex 32)"
   export AIR_BOOTSTRAP_API_KEY="air_live_$(openssl rand -hex 24)"
   export AIR_R2_ENDPOINT="https://<account_id>.r2.cloudflarestorage.com"
   export AIR_R2_ACCESS_KEY_ID="<your-access-key-id>"
   export AIR_R2_SECRET_ACCESS_KEY="<your-secret-access-key>"
   ```

---

## 4. Step-by-Step Deployment Procedure

### Step 1: Create the Fly Apps
Create the four application entries in your chosen region (e.g. `sin` or `iad`):

```bash
fly apps create air-pg-pageserver
fly apps create air-pg-read-engine
fly apps create air-pg-server
fly apps create air-pg-gateway
```

### Step 2: Allocate Dedicated IPv4 for Gateway
Allocate a Dedicated IPv4 **only** to the gateway app:

```bash
fly ips allocate-v4 --app air-pg-gateway --yes
```

Check the allocated IP:
```bash
fly ips list --app air-pg-gateway
```

### Step 3: Create Persistent Volumes
Create encrypted volumes in your target region (example uses `sin`):

```bash
# 1. Pageserver storage volume (1 GB)
fly volumes create air_pageserver_data --app air-pg-pageserver --region sin --size 1 --yes

# 2. Read Engine cache volume (1 GB)
fly volumes create air_read_engine_data --app air-pg-read-engine --region sin --size 1 --yes

# 3. Control Plane tenant storage volume (2 GB)
fly volumes create air_server_data --app air-pg-server --region sin --size 2 --yes
```

### Step 4: Configure App Secrets
Stage the environment secrets across all apps:

```bash
# 1. Pageserver secrets
fly secrets set --app air-pg-pageserver --stage \
  AIR_INTERNAL_TOKEN="$AIR_INTERNAL_TOKEN" \
  AIR_MASTER_KEY_HEX="$AIR_MASTER_KEY_HEX" \
  AIR_R2_ENDPOINT="$AIR_R2_ENDPOINT" \
  AIR_R2_ACCESS_KEY_ID="$AIR_R2_ACCESS_KEY_ID" \
  AIR_R2_SECRET_ACCESS_KEY="$AIR_R2_SECRET_ACCESS_KEY"

# 2. Read Engine secrets
fly secrets set --app air-pg-read-engine --stage \
  AIR_INTERNAL_TOKEN="$AIR_INTERNAL_TOKEN"

# 3. Air Server (Control Plane) secrets
fly secrets set --app air-pg-server --stage \
  AIR_META_DSN="$AIR_META_DSN" \
  AIR_INTERNAL_TOKEN="$AIR_INTERNAL_TOKEN" \
  AIR_MASTER_KEY_HEX="$AIR_MASTER_KEY_HEX" \
  AIR_BOOTSTRAP_API_KEY="$AIR_BOOTSTRAP_API_KEY" \
  AIR_R2_ENDPOINT="$AIR_R2_ENDPOINT" \
  AIR_R2_ACCESS_KEY_ID="$AIR_R2_ACCESS_KEY_ID" \
  AIR_R2_SECRET_ACCESS_KEY="$AIR_R2_SECRET_ACCESS_KEY"

# 4. Gateway secrets
fly secrets set --app air-pg-gateway --stage \
  AIR_INTERNAL_TOKEN="$AIR_INTERNAL_TOKEN" \
  AIR_META_DSN="$AIR_META_DSN"
```

### Step 5: Deploy Services in Order
Deploy the services from repository root in strict dependency order:

```bash
# 1. Deploy air-pg-pageserver (Internal 6PN)
fly deploy . \
  -c deploy/fly/fly.pageserver.toml \
  --dockerfile Dockerfile.pageserver \
  --depot=false --remote-only --ha=false --no-public-ips --yes

# 2. Deploy air-pg-read-engine (Internal 6PN)
fly deploy . \
  -c deploy/fly/fly.read-engine.toml \
  --dockerfile Dockerfile.read-engine \
  --depot=false --remote-only --ha=false --no-public-ips --yes

# 3. Deploy air-pg-server (Internal 6PN — uses Dockerfile.server-fly fast Go overlay)
fly deploy . \
  -c deploy/fly/fly.server.toml \
  --dockerfile deploy/fly/Dockerfile.server-fly \
  --depot=false --remote-only --ha=false --no-public-ips --yes

# 4. Deploy air-pg-gateway (Public Dedicated IPv4 + HTTPS)
fly deploy . \
  -c deploy/fly/fly.gateway.toml \
  --dockerfile gateway/Dockerfile \
  --depot=false --remote-only --ha=false --yes
```

---

## 5. Verifying the Deployment

### Check Service Status
Confirm all 4 apps are running:

```bash
for app in air-pg-pageserver air-pg-read-engine air-pg-server air-pg-gateway; do
  echo "=== $app ==="
  fly status --app "$app"
done
```

Check the gateway health endpoint:
```bash
curl -fsS https://air-pg-gateway.fly.dev/healthz
# Expected output: {"status":"ok"}
```

---

## 6. End-to-End Database Operations on Fly.io

### Step 1: Provision a New Database
Call the Gateway Admin HTTP API using `AIR_INTERNAL_TOKEN` (which securely proxies to `air-pg-server` over 6PN):

```bash
curl -sS -X POST https://air-pg-gateway.fly.dev/v1/databases \
  -H "Authorization: Bearer $AIR_INTERNAL_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"tenant_id": "production_app"}' | jq .
```

Save the `password` returned in the response!

### Step 2: Connect via `psql` to Gateway Dedicated IPv4
Retrieve the public IP and connect:

```bash
GATEWAY_IP="$(fly ips list --app air-pg-gateway --json | jq -r '.[] | select(.Type=="v4") | .Address')"

psql "postgresql://production_app:<GENERATED_PASSWORD>@${GATEWAY_IP}:5432?sslmode=require"
```

### Step 3: Run SQL Workloads
```sql
-- Create table and insert records
CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    balance NUMERIC(12, 2) NOT NULL DEFAULT 0.00
);

INSERT INTO accounts (name, balance) VALUES ('Company Alpha', 10000.00), ('Company Beta', 5000.00);

-- Read queries are seamlessly routed to the SQLite read engine
SELECT * FROM accounts;
```

### Step 4: Verify Serverless Scale-to-Zero & Auto-Wake
1. **Auto-Suspend**: `air-pg-server` idle timer suspends inactive databases after 30 seconds (`AIR_IDLE_TIMEOUT = "30s"`).
2. **Inspect state**:
   ```bash
   curl -sS https://air-pg-gateway.fly.dev/v1/namespaces/production_app \
     -H "Authorization: Bearer $AIR_INTERNAL_TOKEN" | jq .state
   ```
3. **Transparent Wake**: Running any query or write against `production_app` via port `5432` causes the gateway to hold the query, wake `air-pg-server`, replay the command, and resume normal execution without error.

---

## 7. Troubleshooting & Operational Tips

- **Fast Deployments with `Dockerfile.server-fly`**: Full compilation of patched PostgreSQL 17 and `air_smgr.so` takes ~8 minutes. `deploy/fly/Dockerfile.server-fly` compiles only Go binaries and overlays them on top of `ghcr.io/mahadi-rsio/air:server-v2.0.28`, deploying updates in under 45 seconds.
- **Volume Permissions**: Volumes mounted by Fly are initially owned by `root`. The startup entrypoint scripts ([`scripts/pageserver-entrypoint.sh`](../scripts/pageserver-entrypoint.sh) and [`scripts/read-engine-entrypoint.sh`](../scripts/read-engine-entrypoint.sh)) automatically `chown` the data directories before switching to unprivileged users.
- **Monitoring & Metrics**:
  - Prometheus metrics: `https://air-pg-gateway.fly.dev/metrics` (Requires `Authorization: Bearer $AIR_INTERNAL_TOKEN`).
  - Real-time JSON pool and query stats: `https://air-pg-gateway.fly.dev/internal/v1/metrics`.
