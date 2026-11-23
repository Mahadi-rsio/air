# Deploying AIR to Fly.io

This guide walks through deploying the full 4-service AIR mesh to [Fly.io](https://fly.io) using Fly's private 6PN IPv6 network, persistent volumes, an external PostgreSQL metadata database (e.g., Neon), and Cloudflare R2 for durable object storage.

## Architecture & Topology

All four services run in the same Fly region (default: `sin`) and communicate over Fly's private IPv6 6PN (`*.internal`). **Only `air-pg-gateway` has a public IP address** (a Dedicated IPv4 for raw TCP PostgreSQL traffic on `:5432` and HTTPS admin traffic on `:443`).

```text
Internet (psql :5432 / HTTPS :443)
        │
        ▼  Dedicated IPv4 (public)
┌───────────────────────────┐
│      air-pg-gateway       │  shared-cpu-1x · 256MB · no volume
└──────┬──────────┬─────────┘
       │ 6PN      │ 6PN (suspended reads)
       ▼          ▼
┌─────────────────────┐   6PN   ┌───────────────────────────┐
│    air-pg-server    │◄───────►│    air-pg-read-engine     │
│ shared-cpu-2x·512MB │         │  shared-cpu-1x · 256MB    │
│  vol: 2GB (/var/lib │         │  vol: 1GB (/var/lib/      │
│       /air)         │         │       air-read-engine)    │
└──────────┬──────────┘         └─────────────┬─────────────┘
           │ 6PN                              │ 6PN
           ▼                                  ▼
     ┌──────────────────────────────────────────────┐
     │              air-pg-pageserver               │
     │         shared-cpu-1x · 256MB                │
     │     vol: 1GB (/var/lib/air-pageserver)       │
     └──────────────────────┬───────────────────────┘
                            │
          ┌─────────────────┴─────────────────┐
          ▼                                   ▼
   Cloudflare R2                    External PostgreSQL
 (L0/L1, WAL, skeleton)          (Neon — AIR_META_DSN on
                                  air-pg-server)
```

### Service & Resource Sizing

Configs live in [`deploy/fly/`](../deploy/fly/):

| Service | Fly App Name | Config | VM Size | Volume | Public IP |
| --- | --- | --- | --- | --- | --- |
| Gateway | `air-pg-gateway` | [`deploy/fly/fly.gateway.toml`](../deploy/fly/fly.gateway.toml) | `shared-cpu-1x` / `256MB` | None | **Dedicated IPv4** (`:5432` TCP, `:443` → `:8081` HTTPS) |
| Air DB / Control Plane | `air-pg-server` | [`deploy/fly/fly.server.toml`](../deploy/fly/fly.server.toml) | `shared-cpu-2x` / `512MB` | `air_server_data` (`2GB` → `/var/lib/air`) | None (6PN internal only) |
| Read Engine | `air-pg-read-engine` | [`deploy/fly/fly.read-engine.toml`](../deploy/fly/fly.read-engine.toml) | `shared-cpu-1x` / `256MB` | `air_read_engine_data` (`1GB` → `/var/lib/air-read-engine`) | None (6PN internal only) |
| Pageserver | `air-pg-pageserver` | [`deploy/fly/fly.pageserver.toml`](../deploy/fly/fly.pageserver.toml) | `shared-cpu-1x` / `256MB` | `air_pageserver_data` (`1GB` → `/var/lib/air-pageserver`) | None (6PN internal only) |

> **Why Dedicated IPv4 on the gateway?** Fly's shared IPv4 only supports HTTP/TLS. Raw PostgreSQL TCP connections on port `5432` require a Dedicated IPv4 address.

---

## Prerequisites

1. **`flyctl`** installed and authenticated (`fly auth login`).
2. **External PostgreSQL metadata DB** (e.g., Neon) connection string for `AIR_META_DSN`. All schema migrations run automatically when `air-pg-server` starts.
3. **Cloudflare R2 bucket** (e.g., `air-pg-backups`) with S3-compatible endpoint, access key, and secret key.
4. **Generated secrets** for inter-service authentication and R2 encryption:

```bash
export AIR_INTERNAL_TOKEN="$(openssl rand -hex 32)"
export AIR_BOOTSTRAP_API_KEY="air_live_$(openssl rand -hex 24)"
export AIR_R2_ENCRYPTION_KEY="$(openssl rand -hex 32)"

export AIR_META_DSN="postgresql://<user>:<pass>@<host>/<db>?sslmode=require&channel_binding=require"
export AIR_R2_ENDPOINT="https://<account-id>.r2.cloudflarestorage.com"
export AIR_R2_ACCESS_KEY_ID="<r2-access-key>"
export AIR_R2_SECRET_ACCESS_KEY="<r2-secret-key>"
```

> Save `AIR_INTERNAL_TOKEN` and `AIR_BOOTSTRAP_API_KEY` securely — you will need `AIR_BOOTSTRAP_API_KEY` to call the provisioning API.

---

## Step 1: Create Fly Apps

Create all four apps in your Fly organization (default `personal`):

```bash
fly apps create air-pg-pageserver
fly apps create air-pg-read-engine
fly apps create air-pg-server
fly apps create air-pg-gateway
```

---

## Step 2: Allocate Dedicated IPv4 (Gateway Only)

Allocate a Dedicated IPv4 **only** for `air-pg-gateway`:

```bash
fly ips allocate-v4 --app air-pg-gateway --yes
```

Verify the allocated IP:

```bash
fly ips list --app air-pg-gateway
```

Do **not** allocate public IPs for `air-pg-pageserver`, `air-pg-read-engine`, or `air-pg-server`.

---

## Step 3: Create Persistent Volumes

Create encrypted volumes in the same region as `primary_region` (`sin`):

```bash
fly volumes create air_pageserver_data  --app air-pg-pageserver  --region sin --size 1 --yes
fly volumes create air_read_engine_data --app air-pg-read-engine --region sin --size 1 --yes
fly volumes create air_server_data      --app air-pg-server      --region sin --size 2 --yes
```

---

## Step 4: Set Secrets

Stage secrets for each app (they take effect on deploy):

```bash
# 1. Pageserver (R2 + internal mesh auth)
fly secrets set --app air-pg-pageserver --stage \
  AIR_INTERNAL_TOKEN="$AIR_INTERNAL_TOKEN" \
  AIR_R2_ENDPOINT="$AIR_R2_ENDPOINT" \
  AIR_R2_ACCESS_KEY_ID="$AIR_R2_ACCESS_KEY_ID" \
  AIR_R2_SECRET_ACCESS_KEY="$AIR_R2_SECRET_ACCESS_KEY" \
  AIR_R2_ENCRYPTION_KEY="$AIR_R2_ENCRYPTION_KEY"

# 2. Read Engine (internal mesh auth)
fly secrets set --app air-pg-read-engine --stage \
  AIR_INTERNAL_TOKEN="$AIR_INTERNAL_TOKEN"

# 3. Air Server (Neon metadata DB + R2 + bootstrap API key + internal mesh auth)
fly secrets set --app air-pg-server --stage \
  AIR_META_DSN="$AIR_META_DSN" \
  AIR_INTERNAL_TOKEN="$AIR_INTERNAL_TOKEN" \
  AIR_BOOTSTRAP_API_KEY="$AIR_BOOTSTRAP_API_KEY" \
  AIR_R2_ENDPOINT="$AIR_R2_ENDPOINT" \
  AIR_R2_ACCESS_KEY_ID="$AIR_R2_ACCESS_KEY_ID" \
  AIR_R2_SECRET_ACCESS_KEY="$AIR_R2_SECRET_ACCESS_KEY" \
  AIR_R2_ENCRYPTION_KEY="$AIR_R2_ENCRYPTION_KEY"

# 4. Gateway (internal mesh auth)
fly secrets set --app air-pg-gateway --stage \
  AIR_INTERNAL_TOKEN="$AIR_INTERNAL_TOKEN"
```

---

## Step 5: Deploy Services in Order

Deploy from the repository root in dependency order (`pageserver` → `read-engine` → `server` → `gateway`).

> **Note:** Pass `--ha=false` to keep a single machine per app (matching the single volume), `--no-public-ips` for the three internal services, and `--depot=false --remote-only` if Fly's Depot builder stalls.

```bash
# 1. Pageserver (internal only)
fly deploy . \
  -c deploy/fly/fly.pageserver.toml \
  --dockerfile Dockerfile.pageserver \
  --depot=false --remote-only --ha=false --no-public-ips --yes

# 2. Read Engine (internal only)
fly deploy . \
  -c deploy/fly/fly.read-engine.toml \
  --dockerfile Dockerfile.read-engine \
  --depot=false --remote-only --ha=false --no-public-ips --yes

# 3. Air Server (internal only — uses Dockerfile.server-fly fast Go overlay)
fly deploy . \
  -c deploy/fly/fly.server.toml \
  --dockerfile deploy/fly/Dockerfile.server-fly \
  --depot=false --remote-only --ha=false --no-public-ips --yes

# 4. Gateway (public Dedicated IPv4 + HTTPS)
fly deploy . \
  -c deploy/fly/fly.gateway.toml \
  --dockerfile gateway/Dockerfile \
  --depot=false --remote-only --ha=false --yes
```

### About `deploy/fly/Dockerfile.server-fly`

Compiling PostgreSQL 17 and `air_smgr` from source takes several minutes and significant disk space. [`deploy/fly/Dockerfile.server-fly`](../deploy/fly/Dockerfile.server-fly) builds only the Go `air-server` binary and overlays it onto the prebuilt GHCR base image (`ghcr.io/mahadi-rsio/air:server-v2.0.28`), reducing server deploys to ~30 seconds. If you modify C code (`air_smgr` or Postgres patches), build against the root [`Dockerfile`](../Dockerfile) instead.

---

## Step 6: Verify the Deployment

### 1. Check Machine Health & IPs

```bash
for app in air-pg-pageserver air-pg-read-engine air-pg-server air-pg-gateway; do
  echo "=== $app ==="
  fly status --app "$app"
  fly ips list --app "$app"
done
```

Confirm:
- All 4 apps have 1 `started` machine in `sin`.
- Only `air-pg-gateway` lists a public Dedicated IPv4.
- `curl -sS https://air-pg-gateway.fly.dev/healthz` returns HTTP `200`.

### 2. Provision a Test Database & Run Lifecycle Checks

```bash
GATEWAY_IP="$(fly ips list --app air-pg-gateway --json | jq -r '.[] | select(.Type=="v4") | .Address')"

# Create a tenant database via the gateway HTTP proxy
curl -sS -X POST https://air-pg-gateway.fly.dev/v1/databases \
  -H "Authorization: Bearer $AIR_BOOTSTRAP_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name":"fly_smoke_test"}' | jq .
```

Connect with `psql` using the returned password:

```bash
psql "postgresql://app_user:<password>@${GATEWAY_IP}:5432/fly_smoke_test?sslmode=disable"
```

Verify the full serverless lifecycle:
1. **Active write/read**: `CREATE TABLE items (id serial primary key, name text); INSERT INTO items (name) VALUES ('hello'); SELECT * FROM items;`
2. **Auto-suspend**: Wait `30s` (`AIR_IDLE_TIMEOUT = "30s"` in `fly.server.toml`) and check `GET https://air-pg-gateway.fly.dev/v1/databases/fly_smoke_test` → `state: "SUSPENDED"`.
3. **Suspended read**: Run `SELECT * FROM items;` → served by `air-pg-read-engine.internal`, database stays `SUSPENDED`.
4. **Suspended write**: Run `INSERT INTO items (name) VALUES ('wake');` → gateway holds the connection, wakes `air-pg-server`, replays the write, and database transitions back to `ACTIVE`.

---

## Important Implementation Notes & Troubleshooting

- **IPv6 Dual-Stack Listeners (6PN)**: Fly's private network (`*.internal`) routes over IPv6 (`fdaa:...`). All four services bind `[::]:<port>` (`AIR_READ_ENGINE_LISTEN_HOST = "[::]"`), and `air-pg-server` configures tenant Postgres with `AIR_PG_LISTEN_ADDRESSES = "*"` plus IPv6 `::/0` (`::1/128`) entries in `pg_hba.conf`.
- **Volume Permissions**: Fly mounts new volumes owned by `root:root`. Both [`Dockerfile.pageserver`](../Dockerfile.pageserver) and [`Dockerfile.read-engine`](../Dockerfile.read-engine) use `gosu` entrypoint scripts ([`scripts/pageserver-entrypoint.sh`](../scripts/pageserver-entrypoint.sh), [`scripts/read-engine-entrypoint.sh`](../scripts/read-engine-entrypoint.sh)) that `chown` the mounted data directory before dropping privileges to `airps` / `airread`.
- **Pageserver Memory Budget**: On a `256MB` VM, `AIR_PAGESERVER_CACHE_MB = "64"` keeps the in-memory page cache safely below the OOM ceiling.
- **Neon Migrations**: `air-pg-server` runs embedded SQL migrations against `AIR_META_DSN` automatically on boot (no manual migration step required).
- **Local `.env` vs Fly Secret Names**: If you keep R2 keys in a local `.env` as `ACCESS_KEY`, `SECRET_KEY`, and `ENDPOINT`, remember to map them to `AIR_R2_ACCESS_KEY_ID`, `AIR_R2_SECRET_ACCESS_KEY`, and `AIR_R2_ENDPOINT` when setting Fly secrets.
- **Updating a Single Service**: Re-run only that service's `fly deploy` command from Step 5. Existing secrets and volumes are preserved automatically.
