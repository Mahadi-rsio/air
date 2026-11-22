AIR — PGlite Read Engine + Shared Storage + NVMe Cache

Master Engineering Implementation Prompt

1. Project Objective

Implement a production-grade, read-only PGlite execution engine inside AIR (PostgreSQL-compatible Database Platform).

The objective is to make patched PGlite the DEFAULT READ QUERY EXECUTION ENGINE while keeping the existing AIR engine exclusively responsible for all write operations.

AIR must use a shared-storage architecture with a multi-level read cache backed by server-local NVMe storage.

This is an architectural extension of the existing AIR project, NOT a database engine rewrite.

2. Core Architecture

Implement the following architecture:

CLIENT
|
v
AIR PostgreSQL Gateway
|
+----------------------------+
|                            |
v                            v
READ REQUEST               WRITE REQUEST
|                            |
v                            v
Patched PGlite             Existing AIR Write Engine
(Read Only)                    |
|                            |
v                            v
L1: RAM Page Cache         AIR Transaction Commit
|                            |
v                            v
L2: Local NVMe Cache       Publish Committed Versions
|                            |
v                            v
L3: AIR Authoritative Storage
|
v
Query Result
|
v
CLIENT

Important architectural rules:

1. AIR remains the single source of truth.
2. Existing AIR Write Engine owns every write operation.
3. PGlite is exclusively a read execution engine.
4. PGlite must never modify authoritative AIR storage.
5. No full database replication.
6. No independent PGlite primary database.
7. PGlite must access AIR data through a controlled shared-storage interface.
8. All reads must respect AIR's transaction visibility and snapshot semantics.
9. Local NVMe cache is disposable and rebuildable.
10. Cache failures must never compromise database durability.

3. Mandatory Engineering Principles

- Audit before implementation.
- Understand existing AIR architecture before modifying code.
- Preserve existing public APIs and PostgreSQL protocol compatibility.
- Do not replace existing working components unnecessarily.
- Avoid duplicated implementations.
- Prefer reusable, modular and documented interfaces.
- Maintain strict tenant and database isolation.
- Avoid unnecessary memory copies and data serialization.
- Use zero-copy techniques where safe and beneficial.
- Benchmark before introducing low-level optimizations.
- Never assume PGlite's storage implementation is compatible with AIR.
- Never assume that WAL LSN alone provides MVCC snapshot compatibility.
- Never expose partially committed data.
- Never compromise transaction correctness for cache performance.

---

PHASE 1 — Repository Audit and Architecture Discovery

DO NOT implement PGlite integration yet.

First inspect the complete existing AIR repository.

Investigate:

1. PostgreSQL Gateway architecture.
2. Existing SQL parsing and query routing.
3. Existing C++ gateway and low-level components.
4. Storage engine implementation.
5. Physical page format.
6. Heap tuple representation.
7. B-tree index format.
8. TOAST implementation.
9. Catalog and relation metadata.
10. Transaction manager.
11. MVCC implementation.
12. WAL implementation.
13. Commit and recovery behavior.
14. Snapshot management.
15. Existing caching mechanisms.
16. Object storage and local storage interactions.
17. Tenant isolation and database generation identifiers.
18. Backup and PITR consistency.
19. Existing benchmark infrastructure.

Investigate the upstream PGlite implementation:

- PostgreSQL version compatibility.
- WASM architecture.
- Storage manager implementation.
- Filesystem abstraction.
- Buffer manager interactions.
- Page access APIs.
- Internal write operations.
- Temporary file handling.
- Transaction behavior.
- Read-only execution capabilities.
- Build and patching workflow.

Deliverables:

- Complete AIR architecture report.
- PGlite internal architecture report.
- AIR vs PGlite compatibility matrix.
- List of reusable existing components.
- List of components requiring modifications.
- Identified architectural blockers.
- Risk assessment.
- Proposed implementation plan.

Do not proceed with assumptions about storage compatibility.

PHASE 2 — Shared Storage Compatibility Proof

Goal: Prove that PGlite can correctly read AIR's authoritative storage.

Investigate whether AIR and PGlite share compatible:

- PostgreSQL major version.
- Physical page formats.
- Heap tuples.
- Index structures.
- Catalog metadata.
- Transaction identifiers.
- MVCC visibility rules.
- TOAST representation.
- Relation and fork metadata.

Design a controlled AIR Storage Adapter.

Proposed interface:

- open_relation()
- close_relation()
- read_page()
- read_pages()
- get_relation_metadata()
- get_snapshot()
- release_snapshot()
- get_page_version()

These are proposed APIs. Adapt them to the existing AIR implementation.

Requirements:

1. Do not introduce another authoritative storage layer.
2. Read only committed and visible data.
3. Support snapshot-aware page access.
4. Ensure tenant and database isolation.
5. Handle database generation changes.
6. Prevent stale or invalid page access.
7. Do not allow persistent writes through the adapter.

Create a minimal proof of concept:

- Open an existing AIR database.
- Read a table's relation metadata.
- Read a heap page.
- Decode a tuple.
- Verify its contents against the existing AIR engine.
- Read an existing index page if compatible.

If physical compatibility is not possible, stop and document the required compatibility layer.

Do not fake compatibility by returning incorrect or partially decoded pages.

Deliverables:

- Working storage adapter prototype.
- Compatibility test suite.
- Page-format validation.
- Technical go/no-go report.

PHASE 3 — PGlite Read-Only Storage Patching

Goal: Integrate AIR Storage Adapter into PGlite.

Create a maintainable PGlite patch set.

Requirements:

1. Preserve upstream PGlite architecture as much as possible.
2. Minimize invasive modifications.
3. Keep AIR-specific code isolated.
4. Implement AIR-backed read access.
5. Support relation and page access.
6. Integrate snapshot handling.
7. Support index and heap reads.
8. Support required PostgreSQL metadata access.
9. Handle temporary files separately.
10. Ensure persistent AIR storage cannot be modified.

Strict read-only enforcement must exist at multiple layers:

- Gateway query classification.
- PostgreSQL execution and transaction permissions.
- Storage adapter persistent-write rejection.

Reject all persistent mutations, including:

- INSERT
- UPDATE
- DELETE
- MERGE
- CREATE
- ALTER
- DROP
- TRUNCATE
- COPY FROM
- SELECT INTO
- Data-modifying CTEs
- Sequence mutations
- SELECT FOR UPDATE / FOR SHARE
- Write-capable functions.

Do not rely only on SQL keyword matching.

Use PostgreSQL parsing and execution semantics.

Important:

PostgreSQL may perform internal writes for hint bits, statistics, temporary files and other housekeeping.

Separate ephemeral/internal writes from authoritative AIR storage writes.

PGlite must never persist internal modifications to AIR's authoritative pages.

Deliverables:

- Patched PGlite build.
- Read-only enforcement tests.
- Storage adapter integration.
- Compatibility tests.
- Upstream patch documentation.

PHASE 4 — MVCC and Snapshot Integration

Goal: Ensure correct concurrent read/write behavior.

Integrate PGlite with AIR's existing transaction visibility model.

Requirements:

1. Consistent read snapshots.
2. Correct transaction ID visibility.
3. xmin/xmax handling where applicable.
4. Commit status visibility.
5. Snapshot pinning.
6. Page-version retention.
7. Safe garbage collection.
8. Concurrent read/write correctness.
9. Recovery-aware snapshot management.

Example:

T1: Read transaction obtains snapshot S100.

T2: PGlite reads committed pages using S100.

T3: AIR Write Engine commits a new version.

T4: Existing read transaction continues seeing S100.

T5: New read transaction receives a newer snapshot.

Never expose uncommitted data.

Never allow a page version to be garbage-collected while an active read snapshot still references it.

If AIR's MVCC semantics cannot be directly reused, document and implement a safe compatibility strategy.

Deliverables:

- Snapshot provider.
- Version-aware page access.
- Concurrency tests.
- MVCC compatibility tests.
- Crash and recovery tests.

PHASE 5 — Multi-Level Read Cache

Implement a dedicated AIR Page Cache.

Architecture:

L1: RAM PAGE CACHE
L2: LOCAL NVMe PAGE CACHE
L3: AIR AUTHORITATIVE STORAGE

L1: RAM Cache

Requirements:

- Fast in-memory page access.
- Thread-safe concurrent reads.
- Sharded cache metadata where beneficial.
- Memory limits.
- Configurable eviction.
- Cache hit/miss metrics.
- Snapshot-aware page versions.

L2: NVMe Cache

Use server-local NVMe SSD for persistent page caching.

Requirements:

- Efficient random reads.
- Batched page reads.
- Sequential prefetch.
- Async I/O where beneficial.
- Page checksums.
- Crash-safe cache metadata.
- Bounded disk usage.
- Cache eviction.
- Cache warm-up support.
- Recovery after restart.
- Safe concurrent access.

The cache must be disposable and rebuildable.

Never treat the NVMe cache as authoritative database storage.

Cache Key

Use a version-aware cache identity:

tenant_id
database_id
generation_id
relation_id
fork_number
block_number
page_version

Prevent cross-tenant and cross-generation cache collisions.

Cache Policy

Evaluate:

- 2Q
- LRU-K
- Clock / CLOCK-Pro
- Workload-aware admission
- Sequential scan bypass

Choose based on real benchmark results.

Do not blindly implement multiple competing policies.

Cache Invalidation

Prefer immutable committed page versions.

When AIR commits a new version:

1. Publish the committed version.
2. Make the version visible to new snapshots.
3. Preserve old versions for active snapshots.
4. Retire old versions only when safe.

Avoid unsafe in-place cache replacement.

Deliverables:

- RAM page cache.
- NVMe page cache.
- Version-aware cache manager.
- Cache eviction.
- Cache metrics.
- Crash recovery tests.
- Performance benchmarks.

PHASE 6 — AIR Write Engine Integration

Do not rewrite the existing Write Engine.

Integrate a minimal committed-version publication interface.

After a successful AIR transaction commit:

1. Publish the new committed storage version.
2. Update relevant version metadata.
3. Make the new version visible to future snapshots.
4. Preserve old versions for existing snapshots.
5. Notify cache metadata if necessary.

Important:

- No PGlite write execution.
- No PGlite mutation of authoritative pages.
- No direct cache invalidation before commit.
- No visibility of uncommitted data.
- Failed transactions must publish nothing.

Cache coordination must never delay or compromise write durability unnecessarily.

Deliverables:

- Commit notification integration.
- Version publication.
- Read-after-write consistency tests.
- Rollback tests.
- Concurrent transaction tests.

PHASE 7 — Gateway Integration

Make patched PGlite the default read execution backend.

Gateway responsibilities:

1. Authenticate clients.
2. Parse PostgreSQL protocol messages.
3. Classify SQL statements.
4. Route read operations to PGlite.
5. Route write operations to existing AIR.
6. Maintain session and transaction state.
7. Manage prepared statements.
8. Preserve error and result compatibility.

Read path:

Client → Gateway → PGlite → RAM Cache → NVMe Cache → AIR Storage on miss.

Write path:

Client → Gateway → Existing AIR Write Engine → AIR Storage.

Important routing cases:

- SELECT
- EXPLAIN
- Prepared SELECT
- Extended Query Protocol
- Multi-statement queries
- Read-only transactions
- Explicit transaction blocks
- Data-modifying CTEs
- SELECT FOR UPDATE
- DDL
- COPY
- Unsupported statements

For mixed read/write transaction blocks, do not split execution between independent engines unless a correct shared transaction coordinator exists.

Initially, route the entire mixed transaction through the existing AIR engine or return an explicit supported-operation error.

Never silently execute an unsafe query on the wrong engine.

Deliverables:

- Gateway router integration.
- PostgreSQL protocol compatibility tests.
- Session tests.
- Prepared statement tests.
- Transaction routing tests.

PHASE 8 — Production Hardening

Implement:

- Resource limits.
- Per-tenant cache quotas.
- Read concurrency limits.
- Snapshot timeout handling.
- Memory pressure management.
- NVMe capacity monitoring.
- Cache corruption detection.
- Cache rebuild.
- Graceful shutdown.
- Engine lifecycle management.
- Error propagation.
- Structured logging.
- Metrics and tracing.

PGlite failures must not affect AIR Write Engine availability.

If the read engine becomes unavailable, use an explicitly designed safe fallback or return a clear error. Do not silently compromise consistency.

PHASE 9 — Benchmark and Performance Optimization

Benchmark against the existing AIR Read Execution Path.

Test workloads:

1. Point SELECT.
2. Primary-key lookup.
3. Index scan.
4. Sequential scan.
5. Large table scan.
6. JOIN.
7. Aggregation.
8. Complex subqueries.
9. Concurrent readers.
10. Concurrent readers and writers.
11. Cold NVMe cache.
12. Warm NVMe cache.
13. RAM cache hits.
14. Storage cache misses.

Measure:

- QPS.
- P50 latency.
- P95 latency.
- P99 latency.
- CPU usage.
- Memory usage.
- NVMe IOPS.
- NVMe throughput.
- Cache hit ratio.
- Page read amplification.
- Context switching.
- WASM overhead.
- Snapshot overhead.

Compare:

A. Existing AIR read execution.
B. Patched PGlite with cold cache.
C. Patched PGlite with warm NVMe cache.
D. Patched PGlite with warm RAM cache.

Do not claim performance improvements without reproducible measurements.

PHASE 10 — Documentation and Final Audit

Produce:

- Architecture documentation.
- Storage adapter documentation.
- PGlite patch maintenance guide.
- Cache architecture documentation.
- MVCC and snapshot documentation.
- Gateway routing documentation.
- Configuration reference.
- Deployment guide.
- Benchmark report.
- Failure recovery guide.
- Security review.
- Known limitations.

Perform a complete final code audit.

Check:

- Race conditions.
- Memory leaks.
- File descriptor leaks.
- Deadlocks.
- Snapshot leaks.
- Cache corruption.
- Tenant isolation.
- Query routing correctness.
- PostgreSQL protocol compatibility.
- Write isolation.
- Recovery correctness.
- Resource exhaustion.

---

4. Implementation Workflow Rules

Work phase by phase.

For EACH phase:

1. Inspect existing code.
2. Identify exact files and components to modify.
3. Document the design.
4. Implement the smallest complete unit.
5. Add unit tests.
6. Add integration tests.
7. Run existing regression tests.
8. Run formatting and static analysis.
9. Run relevant benchmarks.
10. Review the changes.
11. Document results.
12. Report completion and remaining risks.

Do not automatically proceed through all phases without reviewing critical compatibility results.

If a phase exposes a fundamental architectural incompatibility, stop and present the findings and alternatives.

Do not create dummy implementations just to satisfy interfaces.

Do not claim tests passed unless they actually ran successfully.

Preserve existing functionality throughout the project.

5. Final Definition of Done

AIR should eventually provide:

- Existing AIR Write Engine unchanged in responsibility.
- Patched PGlite as default Read Engine.
- Shared authoritative storage access.
- Consistent MVCC snapshots.
- RAM-based L1 page cache.
- Persistent local NVMe L2 page cache.
- Storage-backed L3 cache-miss path.
- Zero authoritative storage writes from PGlite.
- Correct concurrent read/write behavior.
- PostgreSQL-compatible Gateway behavior.
- Production-grade monitoring and recovery.
- Reproducible performance benchmarks.

The final result must be a maintainable, scalable and production-ready dual-engine architecture.

Primary engineering objective:

Make AIR's reads fast through patched PGlite and multi-level NVMe-backed caching, while keeping every write under the exclusive control of the existing AIR engine.

Never sacrifice correctness, durability, isolation or consistency for raw benchmark performance.
