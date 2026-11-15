# Audit 10 — Data Loss and Isolation Review

## 1. Metadata Destruction Prevention (`stage1/pkg/control/control.go`)
- **Before:** `NewControlPlane()` executed `_ = os.RemoveAll(metaDir)` unconditionally on every invocation, destroying all persisted tenant metadata on restart.
- **After:**
  - Removed `os.RemoveAll(metaDir)`.
  - Added `PG_VERSION` detection: initializes a new cluster only when `PG_VERSION` is absent.
  - Added `ensureMetaPostgresRunning()` which checks `pg_ctl status`, starts existing clusters cleanly, and safely clears `postmaster.pid` **only** if `/proc/<pid>` confirms the old postmaster PID is dead.
  - Refuses destructive reset on corrupted metadata (`ErrMetadataCorrupted`).
  - Added `schema_migrations` (`v1` baseline tables, `v2` idempotency/state-machine/encrypted-password columns).

## 2. Cross-Tenant Idempotency & Credential Protection (`stage1/pkg/control/control.go`)
- **Before:** `ProvisionDatabase()` looked up `idempotency_records` by `idempotency_key` alone and returned cached `ProvisionResponse` (with plaintext `AppPassword`) regardless of caller `tenantID`. `clusters.app_password` and `idempotency_records.response_json` stored plaintext passwords.
- **After:**
  - `ProvisionDatabase()` validates `recTenant == tenantID`, `recHash == computeProvisionPayloadHash(tenantID)`, and `cached.TenantID == tenantID`. Cross-tenant or payload-mismatch reuse returns `ErrIdempotencyConflict`.
  - Passwords are encrypted at rest in `clusters.app_password_enc` using `EnvelopeKMS.EncryptObject(tenantID, 0, "control/app_password", ...)` and redacted (`"[REDACTED]"`) in `idempotency_records.response_json`.
  - Per-tenant lock (`"t:"+tenantID`) + refcounted per-key lock prevent concurrent provision/delete races and unbounded `keyLocks` growth.

## 3. Filesystem Path Traversal & Symlink Containment (`stage4/pkg/cell/agent.go`)
- **Before:** `ClaimCluster`, `ActivateCluster`, `StartOrAdoptTenant`, `StopTenantClean`, `StopAndPurgeTenant`, and `MeasureTenantDiskBytes` joined untrusted `tenantID` directly into `filepath.Join` and passed it to `os.RemoveAll`, `os.Rename`, `os.MkdirAll`, and `su - postgres -c` shell interpolation.
- **After:**
  - `ValidateTenantID()` enforces `^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$`.
  - `safeTenantSubpath()` resolves canonical `EvalSymlinks` on the base directory, checks `filepath.Rel(baseDir, target) == tenantID`, and rejects symlinks via `os.Lstat`.
  - `StopTenantClean()` validates `mode` (`smart|fast|immediate`) and invokes `runPostgresCmd` directly without shell interpolation.

## 4. Wire Virtualizer Backend Session-State Isolation (`stage4/pkg/wire/virtualizer.go`)
- **Before:** `BackendConn` instances returned to `idlePool` on `txStatus == 'I'` retained session GUCs (`SET work_mem = '32MB'`), temp tables, and `LISTEN` state across different client connections.
- **After:** Each `BackendConn` tracks `lastClientID`; `acquireBackend(clientID)` executes `DISCARD ALL;` whenever a backend is handed to a different client (closing the backend if `DISCARD ALL;` fails), and `idlePool` is bounded to 64 connections.

## 5. Gateway SNI & Authorization Gap (`stage1/pkg/gateway/gateway.go`)
- **Status:** **CONFIRMED (Open Production Hardening Item)**
- `handleConn` accepts plain TCP (`sniHost == ""`) and skips SNI-to-database matching when `params["database"] == "postgres"`. In production, edge termination must enforce TLS + mandatory SNI-to-JWT/tenant-token authorization before forwarding to any cell.
