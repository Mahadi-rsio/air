# AIR Phase 21.9 — Deployment Topology Validation Report

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Commit:** `85aa02e` + Phase 21.2 & 21.8 changes

---

## 1. Topology Validated in Docker (Matching Intended Production Limits)

| Service | Container / Memory Limit | Status | Measured Memory |
|---|---|---|---|
| `air-gateway` | `air-gateway:phase21` (`256 MB`, no volume) | **Verified** | `9.1–14.6 MB` steady (`29.3 MB` GC peak) |
| `air-pageserver` | `air-pageserver:phase21` (`256 MB`, volume) | **Verified** | `86.6–252.8 MB` |
| `air-read-engine` | `air-read-engine:phase21` (`256 MB`, volume) | **Verified (with finding)** | Health + gRPC connected; DDL lag on brand-new connections (`SQLSTATE 42000`) |
| `air-server` (Cell) | `air-server:phase21` (`512 MB`, `2` vCPU, volume) | **Verified** | `36.0–63.0 MB` `anon`; `244–508 MB` total with page cache |
| Metadata DB | `postgres:17-alpine` (`AIR_META_DSN`) | **Verified** | Control plane & Gateway discovery operational |
| Cloudflare R2 | External object store | **Not executed** | No R2 credentials/endpoint in isolated sandbox; S3/R2 config path audited |
| Neon Control-Plane DB | External Neon Postgres | **Not executed** | Substituted with `postgres:17-alpine` via `AIR_META_DSN` |

---

## 2. Connectivity & Regression Verification

- **Gateway → Cell (`air-server:9090` gRPC + PG `:18021+`):** Verified across all tests (`0` routing errors).
- **Cell → Pageserver (`air-pageserver:9091` gRPC + `/var/lib/air/pageserver.sock` Unix bridge):** Verified with `AIR_INTERNAL_TOKEN` authentication, `WaitForHealthy`, and `air_smgr.so`.
- **Service Authentication:** Verified in `TestConfigurePageserver_FailureModes` (bad token fails fast) and live stack (`AIR_INTERNAL_TOKEN=air-p21-internal-token`).
- **Go Race Regression Suite:** `go test -race ./internal/gateway/... ./internal/cell/... ./cmd/air-server/...` **PASSED**.

---

## 3. Pending / Environmental Items

- `deploy/fly/fly.server.toml` and `docker-compose.yml` should set `AIR_PAGESERVER_ENABLED=true` and `AIR_PAGESERVER_SOCKET=/var/lib/air/pageserver.sock` when deploying with `air_smgr` enabled.
- Live Cloudflare R2 and external Neon control-plane DB interactions remain pending external credentials.

---

## 4. Status

**Phase 21.9 Verdict: PASSED with external-service exclusions (R2/Neon).**
