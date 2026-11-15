# Audit 15 — Reproduction & Fault-Injection Test Results

All reproduction tests were executed **before** applying remediations, and all `-race` remediation and fault-injection suites were executed **after** the final code edits inside `air-stage4:latest` against **live PostgreSQL 17.12**.

---

## 1. Pre-Remediation Reproduction Outputs (Observed)

### 1.1 `stage4/pkg/cell` & `stage4/pkg/wire`
Command:
```bash
cd /workspaces/air/stage4 && go mod tidy && go test -v ./pkg/cell -run TestRepro && go test -v ./pkg/wire -run TestRepro
```
Output:
```text
=== RUN   TestRepro_PathTraversal_ClaimClusterDeletesOutsideRoot
    audit_repro_test.go:38: REPRODUCED P0 Path Traversal: ClaimCluster("../../victim") deleted /tmp/TestRepro_PathTraversal_ClaimClusterDeletesOutsideRoot561652096/001/victim/critical.dat outside tenants/claiming!
--- PASS: TestRepro_PathTraversal_ClaimClusterDeletesOutsideRoot (0.02s)
=== RUN   TestRepro_ReadoptRunningTenants_IgnoresFlockFailure
    audit_repro_test.go:84: Confirmed external flock conflict returns error: agent.lock flock busy for t_flock_repro: resource temporarily unavailable (which ReadoptRunningTenants currently discards with `_ =` on line 188)
--- PASS: TestRepro_ReadoptRunningTenants_IgnoresFlockFailure (0.00s)
PASS
ok  	air-stage4/pkg/cell	0.029s
=== RUN   TestRepro_RequestDrain_FailOpenOnBackendError
    audit_repro_test.go:30: REPRODUCED P1 Drain Fail-Open: checkBackendUnmigratableState failed (failed to connect to `user=postgres database=postgres`: 127.0.0.1:19999 (127.0.0.1): dial error: dial tcp 127.0.0.1:19999: connect: connection refused), but RequestDrain did NOT veto; returned: drain timeout: active transaction exceeded deadline
--- PASS: TestRepro_RequestDrain_FailOpenOnBackendError (0.20s)
PASS
ok  	air-stage4/pkg/wire	0.208s
```

### 1.2 `stage1/pkg/control` (Live PG17 in Docker)
Command:
```bash
cd /workspaces/air/stage1 && go test -c ./pkg/control -o /tmp/control.repro.test && docker run --rm --entrypoint /bin/bash -v /tmp/control.repro.test:/tmp/control.repro.test air-stage4:latest -c "/tmp/control.repro.test -test.v -test.run TestRepro"
```
Output:
```text
=== RUN   TestRepro_NewControlPlane_DestroysExistingMetadata
    audit_repro_test.go:60: REPRODUCED P0 Metadata Destruction: NewControlPlane destroyed metaDir on restart; GetTenantAuth(tenant_a) failed: no rows in result set
--- PASS: TestRepro_NewControlPlane_DestroysExistingMetadata (10.82s)
=== RUN   TestRepro_ProvisionDatabase_CrossTenantIdempotencyLeak
    audit_repro_test.go:105: REPRODUCED P0 Cross-Tenant Idempotency Leak: tenant_b received tenant_a's credentials (AppPassword=pw_ca3544875b2f3a932bd4267b74d303ed)!
--- PASS: TestRepro_ProvisionDatabase_CrossTenantIdempotencyLeak (5.94s)
PASS
```

### 1.3 `stage4/pkg/backup` (Live PG17 in Docker)
Command:
```bash
cd /workspaces/air/stage4 && go test -c ./pkg/backup -o /tmp/backup.repro.test && docker run --rm --entrypoint /bin/bash -v /tmp/backup.repro.test:/tmp/backup.repro.test air-stage4:latest -c "/tmp/backup.repro.test -test.v -test.run TestRepro"
```
Output:
```text
=== RUN   TestRepro_DrainWAL_NilS3DeletesLocalWAL
    audit_repro_test.go:53: REPRODUCED P1 WAL Durability Bug: DrainAndEncryptWALSpoolToR2(s3c=nil) deleted 1 local WAL segments from /tmp/air_wal_repro_1288457586/spool/wal/t_wal_repro without uploading to R2!
--- PASS: TestRepro_DrainWAL_NilS3DeletesLocalWAL (4.68s)
PASS
```

---

## 2. Post-Remediation `-race` & Fault-Injection Outputs (Observed, Live PG17)

### 2.1 `stage4/pkg/cell` & `stage4/pkg/backup` (`-race` in Docker PG17)
Output from `task-368`:
```text
=== RUN   TestRemediation_PathTraversalAndSymlinkContainment
--- PASS: TestRemediation_PathTraversalAndSymlinkContainment (0.02s)
=== RUN   TestRemediation_ReadoptFlockAndEpochAndPIDReuse
2026/11/15 14:19:45 [cell-agent] readopt skipped for t_dual_own: agent.lock flock busy for t_dual_own: resource temporarily unavailable
2026/11/15 14:19:45 [cell-agent] readopt rejected for t_dual_own: stale epoch 1 (auth=2, err=<nil>)
--- PASS: TestRemediation_ReadoptFlockAndEpochAndPIDReuse (3.35s)
=== RUN   TestRemediation_ProcessSupervision_NestedGrandchildKill
--- PASS: TestRemediation_ProcessSupervision_NestedGrandchildKill (0.22s)
PASS
=== RUN   TestRemediation_DrainWAL_FailClosedAndFaultInjection
--- PASS: TestRemediation_DrainWAL_FailClosedAndFaultInjection (6.15s)
PASS
```

### 2.2 `stage1/pkg/control` (`-race` in Docker PG17)
Command:
```bash
docker run --rm --privileged --entrypoint /bin/bash -v /tmp/control.rem.test:/tmp/control.rem.test air-stage4:latest -c "/tmp/control.rem.test -test.v -test.timeout=60s"
```
Output:
```text
=== RUN   TestRemediation_MetadataPreservedAcrossRestartAndNoPlaintextPasswords
--- PASS: TestRemediation_MetadataPreservedAcrossRestartAndNoPlaintextPasswords (6.40s)
=== RUN   TestRemediation_CorruptedMetadataRefusesDestructiveReset
--- PASS: TestRemediation_CorruptedMetadataRefusesDestructiveReset (0.14s)
=== RUN   TestRemediation_CrossTenantIdempotencyConcurrentIsolation
--- PASS: TestRemediation_CrossTenantIdempotencyConcurrentIsolation (6.27s)
=== RUN   TestRemediation_ProvisioningCrashConsistencyAndRecovery
--- PASS: TestRemediation_ProvisioningCrashConsistencyAndRecovery (15.35s)
PASS
```

### 2.3 `stage4/pkg/wire` (`-race` in Docker PG17)
Command:
```bash
cd /workspaces/air/stage4 && go test -race -c ./pkg/wire -o /tmp/wire.rem.test && docker run --rm --privileged --entrypoint /bin/bash -v /tmp/wire.rem.test:/tmp/wire.rem.test air-stage4:latest -c "/tmp/wire.rem.test -test.v -test.timeout=15s"
```
Output:
```text
=== RUN   TestRemediation_DrainVetoOnBackendErrors
--- PASS: TestRemediation_DrainVetoOnBackendErrors (0.16s)
=== RUN   TestRemediation_BackendSessionStateIsolation
--- PASS: TestRemediation_BackendSessionStateIsolation (3.43s)
PASS
```

### 2.4 Full Module Build Check
Command:
```bash
cd /workspaces/air/stage4 && go build ./... && cd /workspaces/air/stage1 && go build ./... && echo "BOTH_BUILD_OK"
```
Output:
```text
BOTH_BUILD_OK
```
