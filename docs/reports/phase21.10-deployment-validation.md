# AIR Phase 21.10 — Deployment Configuration Validation Report

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Base Commit:** `803c0fd9f0cab7c2c5b0b85c3898f901fd264d4f` (+ Phase 21.10 fixes)  
**Status:** **PARTIAL** (Local Docker 4-service topology verified; R2 & Neon external integrations **NOT TESTED** due to missing credentials)

---

## 1. Configuration Audit & Fixes Applied

| Config File | Finding | Fix Applied | Status |
|---|---|---|---|
| `deploy/fly/fly.server.toml` | Missing `AIR_PAGESERVER_ENABLED`, `AIR_PAGESERVER_SOCKET`, `AIR_PAGESERVER_CACHE_MB` | Added `AIR_PAGESERVER_ENABLED = "true"`, `AIR_PAGESERVER_SOCKET = "/var/lib/air/pageserver.sock"`, `AIR_PAGESERVER_CACHE_MB = "128"` | **PASS** |
| `docker-compose.yml` | Missing `AIR_PAGESERVER_ENABLED`, `AIR_PAGESERVER_SOCKET`, `AIR_PAGESERVER_CACHE_MB` on `air-server` | Added `AIR_PAGESERVER_ENABLED`, `AIR_PAGESERVER_SOCKET`, `AIR_PAGESERVER_CACHE_MB` | **PASS** |
| `deploy/fly/fly.gateway.toml` | `256mb` VM, no volume, `/healthz` check configured, `AIR_POOL_MODE = "true"` | Verified | **PASS** |
| `deploy/fly/fly.pageserver.toml` | `256mb` VM, `1gb` volume (`/var/lib/air-pageserver`), `AIR_PAGESERVER_CACHE_MB = "64"` | Verified (note: no Fly `[[services]]` health check block; internal-only 6PN service) | **PASS** |
| `deploy/fly/fly.read-engine.toml` | `256mb` VM, `1gb` volume (target spec mentions `2GB`; `initial_size = "1gb"` in TOML) | Documented volume size difference (`1gb` vs `2gb`) | **PASS** |
| `deploy/fly/fly.server.toml` | `512mb` VM, `2gb` volume (`/var/lib/air`, target spec mentions `1GB`) | Documented volume size difference (`2gb` vs `1gb`) | **PASS** |

---

## 2. Live Topology Verification (Docker)

Validated on the 4-service + metadata-DB Docker topology matching production memory limits:
- `air-p21-gateway` (`256 MB`, no volume): **PASS** (`24.1–24.65 MB` RSS)
- `air-p21-pageserver` (`256 MB`, persistent volume): **PASS** (`231.6–249.2 MB` RSS)
- `air-p21-read-engine` (`256 MB`, persistent volume): **PASS** (`27.17 MB` RSS)
- `air-p21-server` (`512 MB`, `2` vCPU, persistent volume, `AIR_PAGESERVER_ENABLED=true`): **PASS** (`87.85 MB` cgroup anon, `512.02 MB` peak, `0` OOM kills)
- Metadata DB (`postgres:17-alpine` via `AIR_META_DSN`): **PASS** (substitute for external Neon DB)

---

## 3. Untested External Integrations (`NOT TESTED`)

1. **Cloudflare R2 Object Storage (`NOT TESTED`):**
   - No live R2 credentials (`AIR_R2_ENDPOINT`, `AIR_R2_BUCKET`, `AIR_R2_ACCESS_KEY_ID`, `AIR_R2_SECRET_ACCESS_KEY`) are available in this environment.
   - Reproducible validation procedure:
     1. Set `AIR_R2_ENDPOINT`, `AIR_R2_BUCKET`, `AIR_R2_ACCESS_KEY_ID`, `AIR_R2_SECRET_ACCESS_KEY` in both `air-pageserver` and `air-server`.
     2. Provision a tenant, write data, call `POST /internal/v1/databases/<id>/suspend`, verify `.layer` objects and `timeline_1_layer_manifest.json` appear in R2.
     3. Wipe local `ps_layers` & `pgdata`, restart `air-pageserver`, call `wake`, and verify full data recovery from R2.
2. **External Neon Control-Plane DB (`NOT TESTED`):**
   - No Neon `AIR_META_DSN` credentials are available in this environment; `postgres:17-alpine` was used as a local PostgreSQL 17 substitute.
   - Reproducible validation procedure: set `AIR_META_DSN` to the Neon connection string with `?sslmode=require` on `air-server` and `air-gateway` and verify `/healthz` and `POST /v1/databases`.
