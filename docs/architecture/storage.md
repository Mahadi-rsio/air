# Object Storage & Local Storage Architecture

AIR uses a two-tier storage hierarchy:
1. **Local Node Volume (`--data-dir`, default `/var/lib/air`)**: Holds the metadata PostgreSQL 17 cluster, the warm `initdb` pool, active/suspended tenant `PGDATA` directories, local Pageserver layer files, and the `EnvelopeKMS` master key (`master.kek`).
2. **Cloudflare R2 / S3 Object Store (`--r2-bucket`, `--r2-endpoint`)**: Holds durable, generation-fenced tenant manifests, content-addressed Pageserver layers, encrypted full/incremental base backups, encrypted WAL segments, and immutable per-backup manifests.

---

## 1. Local Volume Layout (`--data-dir`)

```text
<data-dir>/
├── kms/
│   ├── master.kek                          # 32-byte AES-256 KEK (0600 permissions)
│   └── deks/
│       └── <tenant_id>_epoch_<N>.dek       # Wrapped per-tenant DEK ciphertext
├── meta_pgdata/                            # Embedded metadata PG17 cluster (when --meta-dsn is empty)
├── pool/
│   └── warm_<N>/                           # Pre-initialized PG17 clusters ready for O(1) rename
├── pgdata/
│   └── <tenant_id>/                        # Tenant PGDATA (0700 permissions, single-owner .air.lock)
│       ├── .air_wal_spool/                 # PostgreSQL archive_command staging spool
│       │   └── wal_upload_manifest.json    # Local ledger of uploaded WAL segments
│       └── ...
├── pageserver/
│   └── tenants/
│       └── <tenant_hash>_<timeline_id>/    # Local L0, L1, and Image layer files (.layer)
└── backups/
    └── <tenant_id>/
        └── pitr_restore_<nanos>/           # Isolated temporary staging directory during PITR
```

### Security & Path Containment Invariants
- All tenant paths are constructed via `tenant.SafeSubpath(baseDir, elem...)` ([internal/tenant/tenant.go](file:///workspaces/air/internal/tenant/tenant.go)), which validates `tenant.ValidateID` (`^[a-z0-9][a-z0-9_-]{1,62}$`) and verifies via `filepath.EvalSymlinks` that the resolved path remains strictly inside `baseDir`.
- Archive extraction (`backup.ExtractTarZstSafe`) rejects symlinks, hardlinks, device nodes, absolute paths, `..` traversal components, and archives exceeding configured file-count or byte caps (`TarExtractLimits`).

---

## 2. Cloudflare R2 Object Key Layout

All R2 keys for a tenant are scoped under `tenants/{tenant_id}/`:

```text
tenants/{tenant_id}/
├── manifest.json                                       # Fencing CAS manifest (fencing.TenantFenceManifest)
├── layers/
│   └── sha256/{sha256[0:2]}/{sha256}                   # Immutable CAU Pageserver layer blob
├── timeline_{epoch}/
│   ├── layer_manifest.json                             # Pageserver timeline layer index (CAS-updated)
│   └── wal/
│       └── {segment_name}.wal.zst.enc                  # Zstd-compressed, AES-256-GCM encrypted WAL segment
└── backups/
    ├── {backup_id}/
    │   └── manifest.json                               # Immutable BackupManifest (If-None-Match: *)
    └── epoch_{epoch}/
        └── {backup_id}_{full|incr}.tar.zst.enc         # Zstd-compressed, AES-256-GCM encrypted PGDATA tar
```

### Key Helper Functions ([internal/backup/store.go](file:///workspaces/air/internal/backup/store.go))
- `BackupManifestKey(tenantID, backupID)` $\rightarrow$ `tenants/{tenant_id}/backups/{backup_id}/manifest.json`
- `BackupBaseObjectKey(tenantID, epoch, backupID, backupType)` $\rightarrow$ `tenants/{tenant_id}/backups/epoch_{epoch}/{backup_id}_{full|incr}.tar.zst.enc`
- `WALObjectKey(tenantID, epoch, segmentName)` $\rightarrow$ `tenants/{tenant_id}/timeline_{epoch}/wal/{segment_name}.wal.zst.enc`

---

## 3. Shared Storage Primitives (`internal/backup/store.go` & `internal/fencing/r2_cas.go`)

### 3.1 `backup.Store`
`backup.Store` wraps the AWS SDK v2 S3 client (`*s3.Client`) with consistent operations used across backup publication, WAL archival, manifest discovery, and verification:
- **`PutImmutable(ctx, key, body, sha256Hex)`**: Writes an object with `If-None-Match: "*"` so published backup manifests can never be overwritten (`ErrManifestImmutable`).
- **`Get(ctx, key, maxBytes)`**: Bounded object reader with `io.LimitReader(maxBytes + 1)`.
- **`Head(ctx, key)`**: Returns `ObjectInfo` with distinct checksum fields:
  - `SHA256`: Stored object (ciphertext) SHA-256 from native S3 `ChecksumSHA256`, `x-amz-meta-air-cipher-sha256`, or `x-amz-meta-air-sha256`.
  - `PlainSHA256`: Unencrypted payload SHA-256 from `x-amz-meta-air-plain-sha256`.
- **`VerifyObject(ctx, key, expectedSize, expectedSHA256)`**: Verifies object existence, exact byte length, and stored SHA-256 digest without downloading the entire object body.
- **`ListKeys(ctx, prefix)`**: Paginates `ListObjectsV2` and returns keys in deterministic lexicographical order.

### 3.2 `fencing.R2CASStore`
`fencing.R2CASStore` manages `tenants/{tenant_id}/manifest.json` using HTTP `ETag` preconditions:
- **Bootstrap (`If-None-Match: "*"`)**: Creates the initial epoch manifest when a tenant is first fenced.
- **Transition (`If-Match: <etag>`)**: Atomically advances `Epoch`, appends `BackupChain` artifacts, or records `WALSegments`. Any concurrent writer with a stale ETag receives HTTP `412 Precondition Failed` (`ErrR2PreconditionFailed`).

---

## 4. Envelope Encryption (`internal/crypto/kms.go`)

Every backup archive (`*.tar.zst.enc`) and WAL segment (`*.wal.zst.enc`) uploaded to R2 is encrypted with `EnvelopeKMS`:
1. **Master KEK**: A 256-bit key persisted at `<data-dir>/kms/master.kek`.
2. **Per-Tenant Epoch DEK**: A 256-bit Data Encryption Key derived/wrapped per `(tenant_id, epoch)`.
3. **Authenticated Additional Data (AAD)**: Both `EncryptObject` and `DecryptObject` bind the AES-256-GCM authentication tag to `tenantID + ":" + epoch + ":" + r2Key`, preventing cross-tenant, cross-epoch, or cross-key object substitution attacks.
4. **Crypto-Shredding on `DELETE /v1/databases/{id}`**: `ShredTenantDEKs(tenantID)` overwrites and unlinks all wrapped DEK files for the tenant, rendering any residual ciphertext in R2 permanently undecipherable.
