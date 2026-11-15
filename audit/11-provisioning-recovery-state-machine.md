# Audit 11 — Provisioning Recovery State Machine

## 1. Durable State Machine (`provisioning_ops`)
Added in schema migration `v2` in [control.go](file:///workspaces/air/stage1/pkg/control/control.go):

```
  [REQUESTED] ──(ClaimCluster OK)──▶ [CLAIMED] ──(INSERT clusters 'CLAIMING')──▶ [ACTIVATING] ──(ActivateCluster + TX commit)──▶ [ACTIVE]
       │                                 │                                            │
       ▼ (fault / crash)                 ▼ (fault / crash)                            ▼ (fault / crash)
  [ROLLED_BACK]                     [ROLLED_BACK]                                [ROLLED_BACK]
```

## 2. Transition Invariants & Idempotent Rollback
1. **Before `ClaimCluster` (`REQUESTED`):** `idempotency_records` is marked `IN_PROGRESS` with `payload_hash` and `provisioning_ops` records `REQUESTED`. Any failure transitions `provisioning_ops -> ROLLED_BACK` and `idempotency_records -> FAILED`.
2. **After `ClaimCluster` (`CLAIMED`):** `system_identifier` and `claim_source` are persisted. Failure calls `agent.RollbackClaim(tenantID)` (cleaning `tenants/claiming/<tenantID>`).
3. **After `INSERT INTO clusters` (`ACTIVATING`):** `clusters` holds `(tenant_id, system_identifier, state='CLAIMING', op_id)`. Failure calls `agent.RollbackClaim(tenantID)`, `agent.StopAndPurgeTenant(tenantID)`, and deletes the incomplete `CLAIMING` row.
4. **Final Commit (`ACTIVE`):** A single PostgreSQL transaction updates `clusters.state = 'ACTIVE'`, `provisioning_ops.state = 'ACTIVE'`, `idempotency_records.status = 'COMPLETED'`, and inserts `audit_events`.

## 3. Crash Recovery on Startup (`RecoverProvisioningOperations`)
Called automatically at the end of `NewControlPlane()`:
- Queries `provisioning_ops WHERE state IN ('REQUESTED', 'CLAIMED', 'ACTIVATING') ORDER BY created_at ASC`.
- Idempotently invokes `agent.RollbackClaim(tenantID)` (`CLAIMED`/`ACTIVATING`) and `agent.StopAndPurgeTenant(tenantID)` (`ACTIVATING`).
- Deletes orphaned `clusters` rows in `'CLAIMING'` state.
- Marks `provisioning_ops.state = 'ROLLED_BACK'`, `idempotency_records.status = 'FAILED'`, and records `PROVISION_RECOVERED` in `audit_events`, allowing safe client retries without orphaned PGDATA.

## 4. Fault-Injection Verification
`SetFaultHook` injected failures at `after_requested`, `after_claim`, `after_db_claim`, and `after_activate`, plus simulated a `SIGKILL` leaving an in-flight `CLAIMED` op and an orphan `tenants/claiming/t_sigkill` directory. Restarting `NewControlPlane()` deterministically rolled back `op_dead`, removed the orphan directory, and allowed a subsequent `ProvisionDatabase` retry to reach `ACTIVE`.
