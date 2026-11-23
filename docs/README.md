# AIR Documentation

Welcome to the AIR documentation index.

## Getting Started & API
- [Setup Guide](../setup.md) — Docker Compose (local build & GHCR), external PostgreSQL metadata DB (`AIR_META_DSN`), environment variables, and testing
- [Fly.io Deployment Guide](deploy-fly.md) — Full 4-service 6PN mesh on Fly.io with dedicated gateway IPv4, persistent volumes, Neon metadata DB, and Cloudflare R2
- [API Reference](../api.md) — HTTP REST & gRPC reference for all 4 services (`air-server`, `air-pageserver`, `air-read-engine`, `air-gateway`)
- [Contributing](../CONTRIBUTING.md)

## Architecture (`docs/architecture/`)
- [System Overview](architecture/overview.md)
- [Codebase Map](architecture/codebase-map.md)
- [Pageserver & `air_smgr`](architecture/pageserver.md)
- [Network Pageserver Design](architecture/network-pageserver-design.md)
- [Read Engine](architecture/read-engine.md)
- [Storage Engine](architecture/storage.md)
- [Tenant Lifecycle](architecture/tenant-lifecycle.md)
- [WAL & Recovery](architecture/wal-and-recovery.md)
- [Backup & Restore](architecture/backup-and-restore.md)

## Development (`docs/development/`)
- [Development Guide](development/DEVELOPMENT.md)
- [Known Issues](development/known-issues.md)

## Historical Reports & Design Plans (`docs/reports/`)
- [v2.0 Engineering Report (P1–P8)](reports/v2-engineering-report.md)
- [Phase 13 Architecture](reports/phase13-architecture.md) · [Baseline Audit](reports/phase13-1-baseline-audit.md) · [Phase 13.1 Final](reports/phase13-1-final-report.md) · [Phase 13 Final](reports/phase13-final-report.md)
- [Phase 14 Status](reports/PHASE14.md) · [Gateway Audit](reports/phase14-gateway-audit.md) · [Phase 14A Report](reports/phase14a-gateway-report.md)
- [Phase 15 Report](reports/PHASE15.md)
- [Phase 16 Report](reports/PHASE16.md)
- Design Plans (`docs/reports/plans/`): [Pageserver Plan](reports/plans/pageserver-plan.md) · [SMGR Plan](reports/plans/smgr-plan.md) · [PGlite Read Engine Plan](reports/plans/pglite-read-engine-plan.md)
