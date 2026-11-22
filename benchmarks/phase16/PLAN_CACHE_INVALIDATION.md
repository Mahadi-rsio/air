# AIR Phase 16A — Plan Cache Invalidation & Versioning (`PLAN_CACHE_INVALIDATION.md`)

## 1. Objective

Ensure AIR never serves stale translated/optimized query plans or stale prepared statements after schema changes (`ALTER TABLE`, `CREATE INDEX`, `DROP INDEX`, `DROP TABLE`), statistics updates (`ANALYZE`), generation bumps, or session context changes (`search_path`, parameter OIDs, execution settings), while avoiding global cache flushes and maintaining 0-allocation cache hits.

---

## 2. Cache Key & Dependency Design

### 2.1 `PlanCacheKey` ([plan_cache.go](file:///workspaces/air/internal/readadapter/plan_cache.go))
`PlanCacheKey` is a comparable Go value struct (0 heap allocations on map lookup) containing:
- `TenantID` (strict tenant isolation)
- `Generation` (tenant storage generation)
- `DatabaseID` (database OID)
- `SearchPath` (session `search_path`)
- `CatalogLSN` (`lastCatalogModLSN`, not `SyncedLSN`, so DML writes do not thrash the plan cache)
- `SchemaVersion` (per-tenant monotonic schema version)
- `StatsVersion` (per-tenant `StatsCache.TenantVersion(tenantID)`)
- `CapabilityVer` (translator/vtab capability version)
- `ParamSigHash` (`ParamSignatureHash(ParamOIDs)` — parameter type signature)
- `SettingsHash` (`ExecSettingsHash(search_path, timezone, lc_collate)`)
- `QueryHash` (64-bit FNV-1a hash of SQL shape, verified against `OrigShape == rawPGSQL` on hit)

### 2.2 Per-Tenant Statistics Versioning ([stats.go](file:///workspaces/air/internal/readadapter/stats.go))
- `StatsCache` now tracks `tenantVersions map[string]uint64` via `TenantVersion(tenantID)`.
- Updating or invalidating stats for Tenant B only bumps Tenant B's stats version, leaving Tenant A's cached plans intact.

### 2.3 Schema, Table, and Index Version Tracking (`SchemaVersionRegistry`)
- `SchemaVersionRegistry` tracks:
  - Per-tenant/database `schemaVersion`
  - Per-table `tableVers[tenantID|relOID]`
  - Per-index `indexVers[tenantID|indexOID]`
  - Structural fingerprints (`hashTableSchema`, `hashIndexSchema`)
- `ReconcileCatalog` compares newly discovered `DiscoveredTable` metadata against the previous snapshot. Only tables/indexes whose structure actually changed (added/dropped/altered columns or indexes) bump versions and trigger targeted invalidation (`InvalidateTable`, `InvalidateIndex`), leaving unrelated tables and other tenants untouched.

### 2.4 Prepared Statement Invalidation ([sqlite_server.go](file:///workspaces/air/internal/readadapter/sqlite_server.go))
- `sqlitePreparedStmt` records `SchemaVersion`, `Generation`, and `SearchPath` at `Parse` time (`handleParse`).
- `revalidatePreparedStmt` runs before `Execute` (`handleExecute`) and `Describe` (`describeColumns`). If any of `SchemaVersion`, `Generation`, or `SearchPath` changed, the statement is transparently re-translated and updated.

---

## 3. Verification

- `TestPhase16A_PlanCacheKeyScopingAndTenantStatsIsolation`
- `TestPhase16A_TargetedInvalidation_TableIndexAndCatalogReconcile`
- `TestPhase16A_ConcurrentInvalidationAndAccess` (verified with `-race`)
- `TestPhase16A_PreparedStatementInvalidation`
