# Audit 13 — Process Ownership and Supervision Review

## 1. `flock` & Readoption Ownership (`stage4/pkg/cell/agent.go`)
- **Before:** `ReadoptRunningTenants()` discarded `acquireFlockLocked()` errors (`_ = a.acquireFlockLocked(...)`), allowing a second Agent to adopt and manage a cluster already locked by another Agent.
- **After:**
  - `ReadoptRunningTenants()` aborts readoption for any tenant whose `agent.lock` `LOCK_EX|LOCK_NB` fails, increments `dualStartBlock`, and logs the conflict.
  - `SetEpochChecker(fn)` reconciles local `pidMeta.Epoch` against the durable control-plane fencing epoch; if `authEpoch > localEpoch` or `epochChecker` returns an error, readoption is rejected and `flock` is released.

## 2. Process Identity & PID-Reuse Detection
- `verifyPostgresProcessIdentity(pid, expectedPGData, pidPGData)` verifies:
  1. `/proc/<pid>/stat` is not `Z`/`X` and extracts field 22 (`starttime`).
  2. `postmaster.pid` line 2 (`pidPGData`) matches canonical `EvalSymlinks(expectedPGData)`.
  3. `/proc/<pid>/exe` resolves to a binary whose basename contains `postgres`.
  4. `agent.pidmeta` (persisted atomically on `StartOrAdoptTenant`) records `(pid, starttime, system_identifier, epoch, pgdata)`. During `ReadoptRunningTenants()`, if `meta.PID == pid` and `meta.StartTime != currentStartTime`, PID reuse is detected and readoption is rejected.

## 3. Tenant Cgroup & Recursive Process-Tree Supervision
- **Before:** `SignalProcessTree()` only scanned direct children (`ppid == rootPID`), missing grandchildren (e.g., `archive_command` subshells or worker grandchildren), and `fenceAllLocalPrimaries()` released `flock` immediately without waiting for process exit.
- **After:**
  - `movePIDToTenantCgroup(tenantID, pid)` attaches the `postmaster` PID to `/sys/fs/cgroup/air_tenant_<tenantID>/cgroup.procs` when cgroup v2 is writable.
  - `collectProcessTree(rootPID)` performs a full BFS over `/proc/<pid>/stat` parent-child links, and `SignalProcessTree(rootPID, sig)` signals all descendants bottom-up.
  - `killTenantCgroup(tenantID)` writes `1` to `cgroup.kill` and signals any remaining PIDs in `cgroup.procs`.
  - `waitForPIDsExit(pids, 2*time.Second)` verifies complete process termination before `releaseFlockLocked()` is called in `fenceAllLocalPrimaries()`, `StopTenantClean()`, and `StopAndPurgeTenant()`.
