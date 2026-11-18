# Contributing to AIR

Thank you for contributing to AIR! This guide documents repository conventions, testing expectations, and review standards.

---

## 1. Code Organization

- [cmd/air-server](file:///workspaces/air/cmd/air-server/main.go): Binary entrypoint only; keep business logic inside `internal/`.
- [internal/control](file:///workspaces/air/internal/control/control.go): Control plane state machine, migrations ([migrations.go](file:///workspaces/air/internal/control/migrations.go)), cold offload ([offload.go](file:///workspaces/air/internal/control/offload.go)), backup/restore orchestration ([backup_restore.go](file:///workspaces/air/internal/control/backup_restore.go)), HTTP API ([http.go](file:///workspaces/air/internal/control/http.go)), and gRPC server ([grpc.go](file:///workspaces/air/internal/control/grpc.go)).
- [internal/cell](file:///workspaces/air/internal/cell/agent.go): Local PostgreSQL 17 lifecycle, warm `initdb` pool, cooperative drain, `cgroup v2`, and `supautils` hardening.
- [internal/pageserver](file:///workspaces/air/internal/pageserver/server.go): WAL receiver/decoder, in-memory/L0/L1/Image layers, CAU R2 storage, and `GetPage@LSN` Unix socket server.
- [internal/backup](file:///workspaces/air/internal/backup/pitr_dag.go): Shared R2 `Store` ([store.go](file:///workspaces/air/internal/backup/store.go)), LSN math & continuity ([lsn.go](file:///workspaces/air/internal/backup/lsn.go)), manifest models & verification ([manifest.go](file:///workspaces/air/internal/backup/manifest.go)), and PITR/DAG GC.
- [internal/fencing](file:///workspaces/air/internal/fencing/r2_cas.go): R2 ETag CAS manifests and `CLOCK_MONOTONIC` lease watchdog.
- [internal/crypto](file:///workspaces/air/internal/crypto/kms.go) & [internal/tenant](file:///workspaces/air/internal/tenant/tenant.go): `EnvelopeKMS` and canonical tenant ID / path containment helpers.
- [extensions/air_smgr](file:///workspaces/air/extensions/air_smgr/air_smgr.c) & [patches](file:///workspaces/air/patches/pg17-smgr-hook.patch): PostgreSQL 17 C storage manager extension and hook patch.

---

## 2. Coding Conventions

### Go Style & Error Handling
- Always format modified Go files with `gofmt -w`.
- Wrap errors with `%w` and include actionable context (`tenant_id`, `backup_id`, `epoch`, `r2_key`).
- Use sentinel errors (`errors.Is`) for retryable vs. permanent vs. cancellation conditions (`ErrManifestImmutable`, `ErrInvalidRecoveryTarget`, `ErrR2PreconditionFailed`, `ErrStaleGeneration`).
- Never silently discard errors (`_ = ...`) on durability, encryption, manifest publication, or cleanup paths unless the cleanup is best-effort and documented.
- Document exported Go types, functions, and constants with GoDoc comments, and document mutex lock ordering and lifecycle invariants where locks are acquired.

### Concurrency Rules
- Never perform R2/S3 network I/O or PostgreSQL subprocess waits while holding `control.Service.mu`. Snapshot `(epoch, lifecycleSeq)` under lock, release the lock for I/O, and re-verify state before committing.
- Always propagate `context.Context` into background jobs, R2 calls, and command executions.
- Ensure every spawned goroutine has a bounded queue or explicit cancellation/shutdown path.

### PostgreSQL & Security Invariants
- Preserve standard PostgreSQL terminology (`LSN`, `WAL`, `FSM`, `VM`, `SMGR`, `TOAST`).
- Never construct tenant filesystem paths with raw `filepath.Join`; always use `tenant.SafeSubpath` after `tenant.ValidateID`.
- Keep `supautils` restrictions, `app_user` non-superuser role boundaries, and `ExtractTarZstSafe` symlink/traversal protections intact.

---

## 3. Branch & Commit Conventions

- Create focused branches per change (e.g., `fix/backup-wal-continuity`, `refactor/control-offload`).
- Write clear imperative commit messages summarizing the *what* and *why* (e.g., `backup: separate plaintext and ciphertext SHA-256 verification`).
- Do not mix unrelated refactoring with bug fixes or feature changes in the same commit.

---

## 4. Testing & Documentation Requirements

- **Unit & Race Tests**: All changes to `internal/` must pass `go test -race -count=1 ./internal/<pkg>/...`.
- **Static Checks**: `go vet ./...` and `go build ./cmd/air-server ./internal/...` must pass with zero warnings.
- **E2E Tests**: Backup, PITR, fencing, and control-plane API changes must be covered by real PostgreSQL 17 integration tests in [tests/e2e](file:///workspaces/air/tests/e2e).
- **Regression Tests**: Every bug fix must include a deterministic regression test (unit or E2E) that fails without the fix.
- **Documentation**: Update [docs/architecture](file:///workspaces/air/docs/architecture/overview.md) or [docs/development/DEVELOPMENT.md](file:///workspaces/air/docs/development/DEVELOPMENT.md) whenever configuration flags, HTTP/gRPC endpoints, R2 key layouts, or lifecycle invariants change.

---

## 5. Pull Request Checklist

- [ ] `gofmt` and `go vet ./...` pass cleanly.
- [ ] Relevant `go test -race` unit tests and `tests/e2e` integration tests pass.
- [ ] No network I/O or blocking subprocess calls added under `Service.mu` or `Timeline.mu`.
- [ ] All new exported APIs have GoDoc comments.
- [ ] No hardcoded credentials, secrets, or unvalidated user paths introduced.
- [ ] Architecture and operational documentation updated if behavior changed.
