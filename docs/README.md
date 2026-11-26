# AIR Documentation Index

Welcome to the AIR documentation hub. AIR is a cloud-native, multi-tenant PostgreSQL 17 platform featuring disaggregated storage, transparent query routing to an embedded SQLite read-replica, cooperative scale-to-zero, and encrypted backup/PITR.

---

## 🚀 Getting Started

If you are new to AIR, follow these guides in order:

1. **[Beginner's Quickstart & Operations Guide](quickstart.md)**
   - Start AIR in 5 minutes with Docker Compose.
   - Provision your first tenant database.
   - Connect via `psql` or code (Node.js, Python, Go) and perform CRUD operations.
   - Suspend, wake, backup, and delete databases.

2. **[Setup Guide](../setup.md)**
   - Complete production deployment guide for Docker Compose and prebuilt GHCR images.
   - Configuring an external PostgreSQL metadata database (`AIR_META_DSN`).
   - S3 / Cloudflare R2 cold offload configuration.
   - Complete environment variable reference for all 4 services.

3. **[Fly.io Deployment Guide](deploy-fly.md)**
   - Deploying AIR as a 4-service 6PN private mesh on Fly.io with dedicated gateway IPv4, persistent NVMe volumes, Neon metadata DB, and Cloudflare R2.

4. **[API Reference](../api.md)**
   - Complete reference for public REST APIs (`air-server`), Gateway admin APIs (`air-gateway`), Read Engine APIs (`air-read-engine`), and Pageserver APIs (`air-pageserver`).
   - Authentication protocols (`Bearer` token & `AIR_INTERNAL_TOKEN`).
   - gRPC protobuf specifications (`AirDBControl`, `GatewayAdmin`, `PageserverService`, `ReadEngineService`).

5. **[Contributing](../CONTRIBUTING.md)**
   - Development workflows, testing guidelines, and conventions.

---

## 🏛️ Architecture & Deep Dives (`docs/architecture/`)

Explore the internal design and mechanics of the AIR platform:

- **[System Overview](architecture/overview.md)**: Architectural layout of the 4 core services, data flows, and security model.
- **[Codebase Map](architecture/codebase-map.md)**: Directory structure breakdown and mapping to platform components.
- **[Intelligent Query Router](architecture/intelligent-query-router.md)**: How the Go gateway inspects SQL ASTs, coordinates transaction states, and routes reads vs writes.
- **[Read Engine](architecture/read-engine.md)**: SQLite-backed replica engine reading PG17 pages directly from the Pageserver via the CGo `air_pg` virtual table.
- **[Pageserver & `air_smgr`](architecture/pageserver.md)**: Custom Neon-style storage manager, L0/L1 layer generation, and `GetPage@LSN` caching.
- **[Network Pageserver Design](architecture/network-pageserver-design.md)**: Protocol design and low-latency network communication between compute nodes and pageservers.
- **[Storage Engine](architecture/storage.md)**: Persistent storage layout, Content-Addressed Upload (CAU), and R2 tiering.
- **[Tenant Lifecycle & Scale-to-Zero](architecture/tenant-lifecycle.md)**: Warm `initdb` pools, cooperative drain guards (`ErrDrainVetoed`), and transparent on-demand wake.
- **[Write Governor](architecture/WRITE_GOVERNOR.md)**: Adaptive buffer budgeting, token bucket throttling, and tenant write fairness.
- **[WAL & Crash Recovery](architecture/wal-and-recovery.md)**: WAL streaming, heap/xact redo, and generation fencing.
- **[Backup & Point-in-Time Recovery](architecture/backup-and-restore.md)**: Zstd compression, AES-256-GCM envelope KMS, manifest validation, and non-destructive restores.
- **[PostgreSQL Compatibility](architecture/postgresql-compatibility.md)**: Compatibility matrix across SQL types, functions, and extension behaviors.

---

## 🛠️ Development & Operational Reports

### Development Resources (`docs/development/`)
- **[Developer Guide](development/DEVELOPMENT.md)**: Compiling binaries, setting up the CGo environment, running test suites, and debugging.
- **[Known Issues & Limitations](development/known-issues.md)**: Tracked limitations, edge cases, and known workarounds.

### Engineering & Phase Reports (`docs/reports/`)
- **[v2.0 Engineering Final Report](reports/v2-engineering-report.md)**: Complete roadmap and milestone verification from Phase 1 through Phase 8.
- **Phase Reports**:
  - [Phase 13 Architecture](reports/phase13-architecture.md) & [Phase 13.1 Report](reports/phase13-1-final-report.md)
  - [Phase 14 Gateway Status](reports/PHASE14.md) & [Phase 14A Audit](reports/phase14a-gateway-report.md)
  - [Phase 15 Query Planner Report](reports/PHASE15.md)
  - [Phase 16 Plan Cache & Observability](reports/PHASE16.md)
  - [Phase 20 Correctness & Benchmarks](reports/PHASE20_CORRECTNESS_REPORT.md)
  - [Phase 21 Multitenant Stability & Final Audit](reports/phase21-final-summary.md)
  - [Phase 22–29 Reports](reports/): Memory forensics, CAWAL performance, scheduler stability, and streaming validation.
  - [Phase 25 Read Path Stability & Observability](reports/phase-25-read-path-stability.md): Root cause analysis of the minute-3 stall, `/metrics` observability ([docs/metrics.md](metrics.md)), and Gateway disconnect fix.
  - [Phase 26 Sustained CPU Capacity](reports/phase-26-sustained-cpu-capacity.md): 5-minute CPU tier isolation experiments (`baseline`, `gw_only`, `re_only`, `gw_re`, `gw_re_ps`) with `0–120s` vs `120–300s` burst/sustained analysis.
- **Design Plans (`docs/reports/plans/`)**:
  - [Pageserver Plan](reports/plans/pageserver-plan.md)
  - [SMGR Plan](reports/plans/smgr-plan.md)
  - [PGlite / Read Engine Plan](reports/plans/pglite-read-engine-plan.md)
