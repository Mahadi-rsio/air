# AIR Phase 13 — Backup, PITR & Disaster Recovery Architecture Audit

**Date:** 2026-11-18  
**Scope:** `internal/pageserver`, `internal/control`, `internal/backup`, `internal/fencing`,
`internal/cell`, `internal/tenant`, `extensions/air_smgr`, R2 object layer, gateway/container runtime.

This report records the *existing* data lifecycle, durability boundaries, and recovery
assumptions before Phase 13 changes were made. It intentionally does not rewrite the
storage architecture: the existing immutable-layer/CAU design is retained and backups are
built on top of it.

---

## 1. Data lifecycle trace (pre-Phase 13)

1. **PostgreSQL writes a page.** `air_smgr.so` intercepts smgr read/write and forwards page
   I/O to the AIR Page Server over a Unix socket. PostgreSQL still writes its own WAL,
   checkpoint/control files, and catalog data under the tenant `pgdata` directory.
2. **WAL is generated.** Tenant PostgreSQL runs with `wal_level=replica`,
   `archive_mode=on`, `summarize_wal=on`, and
   `archive_command='cp %p <vol>/spool/wal/<segment>'`.
3. **AIR persists WAL.** `internal/control.drainWALSpoolAndUpdateManifestGen` uploads
   spooled segments to R2 through `internal/backup.drainWALSpoolDirWithFaultGen`. Each
   segment is zstd-compressed, AES-256-GCM encrypted with a tenant DEK, uploaded, and
   verified with `HeadObject` (size + ETag) *before* the local spool segment is deleted.
   A durably-fsynced local `wal_upload_manifest.json` records `PENDING`,
   `VERIFIED_IN_R2`, and `LOCAL_DELETED` states, making the drain restart-safe.
4. **Page layers are created.** `Timeline.FlushInMemory` freezes the in-memory layer and
   writes an immutable L0 layer. Compaction merges L0 -> L1/image with a bounded-memory
   k-way merge.
5. **Layer manifests are published.** `LayerStore.PublishLayerManifestGen` writes
   `tenants/<tenant>/<generation>/timeline_<epoch>/layer_manifest.json` using R2 ETag
   CAS. Layer objects are content-addressed
   (`.../layers/sha256/<prefix>/<sha256>`) and are verified before publication.
6. **Data is uploaded to R2.** Layer uploads use `If-None-Match: *` (immutable objects),
   optional multipart, and streamed SHA-256 verification. Generations are encoded in the
   R2 key prefix.
7. **Tenant suspends.** `SuspendTenant` drains live checks, stops PostgreSQL cleanly,
   flushes page-server layers, saves a metadata skeleton, drains WAL, and may evict local
   `pgdata` only after the pageserver skeleton/R2 manifest is confirmed.
8. **Tenant is deleted.** `DeleteDatabase` advances `lifecycle_seq`, cancels offloads,
   stops/purges local tenant state, calls `Pageserver.PurgeTenant` for the generation, and
   shreds the tenant DEK. R2 immutable objects are deliberately **not** deleted here.
9. **Restore is requested (pre-Phase 13).** `RestoreDatabase` selected the latest
   FULL -> INCR chain from `TenantFenceManifest.Backups`, downloaded/decrypted the chain,
   ran `pg_combinebackup` for INCR chains, replayed archived WAL to an LSN, promoted a
   temporary PostgreSQL, ran `pg_amcheck`, then deleted the staging directory. It was a
   *validation-only* restore: it did not publish a restored tenant.
10. **New tenant recovery.** Wake-with-no-local-pgdata uses
    `HydrateTenantPGDataFromR2Gen`, which restores the latest fence-manifest backup chain
    into the tenant's own `pgdata`, clears recovery signals, and starts PostgreSQL.

## 2. Durability boundaries

| Boundary | Durable when | Notes |
|---|---|---|
| WAL segment | `HeadObject` size/ETag verified **and** local `wal_upload_manifest.json` fsynced | Local file deleted only after this |
| Layer object | Content-addressed PUT verified (size + SHA-256) | CAS/immutable key |
| Layer manifest | ETag-CAS `PutObject` succeeds | References only verified layers |
| Fence manifest | ETag-CAS `PutObject` succeeds | Holds backup artifacts, epoch, wrapped DEK |
| Tenant metadata | Control-plane PostgreSQL transaction commits | Local or external HA metadata |
| Suspend/offload | Local cache eviction only after R2 skeleton/manifest confirmation | Generation + lifecycle seq rechecked under lock |

## 3. Recovery assumptions and root causes found

1. **A collection of files is not a backup.** The old path had no immutable backup
   manifest and no explicit consistency LSN, WAL bounds, object checksums, or state.
   A fence-manifest entry could therefore outlive or misrepresent the backup object.
2. **No backup operation state.** `POST /databases/:id/backups` was synchronous and only
   returned after the whole operation. Crash/interruption left no durable operation record
   to reconcile. No idempotency protection existed for backup/restore.
3. **PITR was LSN-only.** There was no timestamp target, no target validation against the
   backup's recoverable range, and no WAL continuity validation before replay.
4. **Restore was not a restore.** `RestoreDatabase` recovered to a temporary directory,
   verified it, then deleted it. No independent restored tenant/generation was published.
5. **Backup catalog lived only in the fence manifest.** There was no per-backup manifest,
   no deep verification, and no account-scoped backup lookup.
6. **Retention was a pure DAG function.** `GarbageCollectBackupDAG` existed but no
   reference-aware object sweep, no active-restore protection, and no dry-run/reporting.
7. **Container runtime mismatch.** The gateway was linked against an externally provided
   gRPC/conda toolchain while the final image contained neither the matching
   `libgrpc++.so.*` nor a reproducible build path. Phase 12 verification had to bypass the
   gateway and test `air-server` directly.
8. **Backup objects were not backup-aware.** Older keys were only epoch-scoped:
   `tenants/<id>/<generation>/epoch_<n>/<backup>.tar.zst.enc`. A later job could not
   unambiguously identify the owning backup operation from the key alone.

## 4. Phase 13 design decisions

- **Physical base backup + archived WAL** is the chosen recovery strategy. AIR's immutable
  page layers are excellent for the page-server read path, but they are not a complete
  PostgreSQL cluster: catalog files, control files, relation forks, and non-page metadata
  also matter. `pg_basebackup` under a checkpoint plus archived WAL is the only strategy
  that gives PostgreSQL-native, verifiable recovery.
- New backups are stored under **backup-aware, generation-isolated keys**:
  `tenants/<tenant>/<generation>/backups/<backup_id>/...`.
- A backup **manifest is immutable and published last**, only after every referenced
  object and WAL segment has been uploaded and verified. The manifest is sealed with its
  own SHA-256 and includes consistency/min-recovery LSN, WAL bounds, timeline, PostgreSQL
  version, generation, parent chain, layer IDs, object sizes/checksums, and state.
- PITR uses PostgreSQL recovery semantics (`recovery_target_time`, `recovery_target_lsn`,
  `recovery_target_action=promote`) and refuses targets outside the backup's proven
  recoverable range.
- Restores publish a **new tenant ID with a new generation** and do not mutate the source
  tenant. A partially restored tenant is never exposed as `ACTIVE`.
- Operations are tracked durably in control-plane metadata; interrupted operations are
  reconciled on restart into an explicit failed/restartable state.

## 5. Unsupported / honestly documented cases

- PITR before the base backup's minimum recovery LSN/time is refused.
- Timestamp targets cannot be validated at millisecond precision before replay; PostgreSQL
  stops on commit timestamps and the result is verified afterwards.
- Backups currently compress/encrypt the base archive in memory. Very large clusters are
  bounded by `maxRestoreObjMiB` and staging disk headroom; this is documented in the
  Phase 13 report rather than claimed as zero-copy.
- `air_smgr` must be built against the patched PostgreSQL 17 tree used by the container;
  a stock distro PostgreSQL server cannot load the extension.
