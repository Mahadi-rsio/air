# Audit 12 — WAL Durability Review

## 1. Fail-Closed R2 Enforcement (`stage4/pkg/backup/pitr_dag.go`)
- **Before:** `DrainAndEncryptWALSpoolToR2()` silently skipped `s3c.PutObject` when `s3c == nil` and deleted the local WAL segment (`_ = os.Remove(segPath)`).
- **After:** `DrainAndEncryptWALSpoolToR2WithFault()` immediately returns `ErrR2ClientRequired` if `s3c == nil || strings.TrimSpace(bucket) == ""`, preserving all local WAL segments.

## 2. Segment Validation, Ordering, & Checksums
- Every file in `rt.WALSpoolDir` (excluding `wal_upload_manifest.json*` and transient `.zst`) is validated against `^[0-9A-F]{24}$` (WAL segment) or `^[0-9A-F]{8}\.history$` (timeline history); invalid filenames fail closed (`ErrInvalidWALName`).
- Segments are sorted lexicographically (`sort.Strings`) to preserve timeline and LSN upload ordering.
- Plaintext SHA-256 (`PlainSHA256`) is recorded prior to `zstd` compression and AES-256-GCM-AAD encryption.

## 3. Durable Upload Manifest & Two-Phase Verification
Each segment transitions through:
1. `PutObject` to R2 (`stage4/tenants/<id>/epoch_<epoch>/wal/<seg>.zst.enc`).
2. `HeadObject` verification confirming remote existence and exact `ContentLength == len(cipherBytes)`.
3. Atomic fsynced commit of `VERIFIED_IN_R2` (with `R2Key`, `PlainSHA256`, `CipherETag`) to `wal_upload_manifest.json` via temp file + `f.Sync()` + `os.Rename()`.
4. `os.Remove(segPath)` followed by updating the manifest entry to `LOCAL_DELETED`.

## 4. Crash & Retry Safety
- **Crash after `PutObject`/`HeadObject` before manifest commit:** Local WAL segment remains intact; retry re-uploads/verifies idempotently and commits manifest.
- **Crash after manifest commit before local deletion:** On retry, `DrainAndEncryptWALSpoolToR2` reads `VERIFIED_IN_R2` from `wal_upload_manifest.json`, confirms `HeadObject` size match, deletes the local WAL segment, and marks `LOCAL_DELETED`.

## 5. Restore & Tar Hardening (`CombineAndReplayPITR`)
- Validates `chain[0].Type == "FULL"` and `chain[i].ParentID == chain[i-1].BackupID`.
- Bounds R2 object reads with `io.LimitReader(obj.Body, maxRestoreObjMiB*1024*1024 + 1)`.
- Unpacks archives with `tar --zstd --no-same-owner --no-same-permissions`.
