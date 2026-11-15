package control

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"air-stage1/pkg/cell"
	"air-stage1/pkg/crypto"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const pgBin = "/usr/lib/postgresql/17/bin"

var (
	ErrInvalidTenantID     = errors.New("control: invalid tenant_id")
	ErrIdempotencyConflict = errors.New("control: idempotency key conflict (different tenant or payload)")
	ErrMetadataCorrupted   = errors.New("control: metadata database corrupted; refusing destructive reset")
	validTenantIDRe        = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$`)
)

type ProvisionResponse struct {
	TenantID         string  `json:"tenant_id"`
	SystemIdentifier string  `json:"system_identifier"`
	State            string  `json:"state"`
	Epoch            int64   `json:"epoch"`
	Port             int     `json:"port"`
	PgBouncerAddr    string  `json:"pgbouncer_addr"`
	AppUser          string  `json:"app_user"`
	AppPassword      string  `json:"app_password"`
	ClaimSource      string  `json:"claim_source"`
	ClaimLatencyMs   float64 `json:"claim_latency_ms"`
	TotalLatencyMs   float64 `json:"total_latency_ms"`
	IdempotentReplay bool    `json:"idempotent_replay"`
}

type UsageEventBatch struct {
	IdempotencyKey string `json:"idempotency_key"`
	GatewayID      string `json:"gateway_id"`
	TenantID       string `json:"tenant_id"`
	Statements     int    `json:"statements"`
	RowsAffected   int64  `json:"rows_affected"`
	BytesOut       int64  `json:"bytes_out"`
	WallMs         int64  `json:"wall_ms"`
	RUCharged      int64  `json:"ru_charged"`
}

type refLock struct {
	mu   sync.Mutex
	refs int
}

type ControlPlane struct {
	metaDir   string
	metaPort  int
	db        *pgxpool.Pool
	agent     *cell.Agent
	kms       *crypto.EnvelopeKMS
	keyMu     sync.Mutex
	keyLocks  map[string]*refLock
	faultMu   sync.RWMutex
	faultHook func(stage string) error
}

// ValidateTenantID enforces strict alphanumeric/hyphen/underscore naming to prevent traversal.
func ValidateTenantID(tenantID string) error {
	if !validTenantIDRe.MatchString(tenantID) {
		return fmt.Errorf("%w: %q", ErrInvalidTenantID, tenantID)
	}
	return nil
}

// SetFaultHook installs a deterministic fault-injection callback for crash-recovery testing.
func (cp *ControlPlane) SetFaultHook(hook func(stage string) error) {
	cp.faultMu.Lock()
	cp.faultHook = hook
	cp.faultMu.Unlock()
}

func (cp *ControlPlane) checkFault(stage string) error {
	cp.faultMu.RLock()
	h := cp.faultHook
	cp.faultMu.RUnlock()
	if h != nil {
		return h(stage)
	}
	return nil
}

// NewControlPlane safely initializes or reopens a dedicated PostgreSQL 17 Metadata DB on metaDir,
// applies versioned schema migrations, and reconciles any interrupted provisioning operations.
func NewControlPlane(ctx context.Context, metaDir string, metaPort int, agent *cell.Agent, kms *crypto.EnvelopeKMS) (*ControlPlane, error) {
	pgData := filepath.Join(metaDir, "pgdata")
	sockDir := filepath.Join(metaDir, "sock")
	if err := os.MkdirAll(sockDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(pgData, 0o700); err != nil {
		return nil, err
	}
	_ = exec.Command("chown", "-R", "postgres:postgres", metaDir).Run()

	pgVersionPath := filepath.Join(pgData, "PG_VERSION")
	_, statErr := os.Stat(pgVersionPath)
	if os.IsNotExist(statErr) {
		if out, err := runPostgres(filepath.Join(pgBin, "initdb"), "-D", pgData, "-U", "postgres", "--auth=trust", "--no-instructions"); err != nil {
			return nil, fmt.Errorf("control metadata initdb: %w (%s)", err, out)
		}
		conf := fmt.Sprintf(`
listen_addresses = '127.0.0.1'
port = %d
unix_socket_directories = '%s'
max_connections = 150
shared_buffers = '16MB'
fsync = on
logging_collector = off
`, metaPort, sockDir)
		if err := os.WriteFile(filepath.Join(pgData, "postgresql.conf"), []byte(conf), 0o600); err != nil {
			return nil, err
		}
		_ = exec.Command("chown", "-R", "postgres:postgres", metaDir).Run()
	} else if statErr != nil {
		return nil, fmt.Errorf("%w: stat PG_VERSION: %v", ErrMetadataCorrupted, statErr)
	}

	// Start PostgreSQL if not already running; handle stale postmaster.pid safely without wiping metaDir.
	if err := ensureMetaPostgresRunning(pgData, filepath.Join(metaDir, "meta.log")); err != nil {
		return nil, err
	}

	dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable&pool_max_conns=50", metaPort)
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("%w: metadata DB ping failed: %v", ErrMetadataCorrupted, err)
	}

	if err := applySchemaMigrations(ctx, db); err != nil {
		db.Close()
		return nil, fmt.Errorf("control schema migration: %w", err)
	}

	cp := &ControlPlane{
		metaDir:  metaDir,
		metaPort: metaPort,
		db:       db,
		agent:    agent,
		kms:      kms,
		keyLocks: make(map[string]*refLock),
	}

	if _, err := cp.RecoverProvisioningOperations(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("control recovery: %w", err)
	}

	return cp, nil
}

func ensureMetaPostgresRunning(pgData, logPath string) error {
	if out, err := runPostgres(filepath.Join(pgBin, "pg_ctl"), "-D", pgData, "status"); err == nil && strings.Contains(out, "server is running") {
		return nil
	}
	if out, err := runPostgres(filepath.Join(pgBin, "pg_ctl"), "-D", pgData, "-l", logPath, "-w", "-t", "20", "start"); err != nil {
		// Check for stale postmaster.pid after unclean crash where the old PID is no longer alive.
		pidBytes, rErr := os.ReadFile(filepath.Join(pgData, "postmaster.pid"))
		if rErr == nil {
			lines := strings.Split(string(pidBytes), "\n")
			if len(lines) > 0 {
				if pid, pErr := strconv.Atoi(strings.TrimSpace(lines[0])); pErr == nil && pid > 0 {
					if _, statErr := os.Stat(fmt.Sprintf("/proc/%d", pid)); os.IsNotExist(statErr) {
						_ = os.Remove(filepath.Join(pgData, "postmaster.pid"))
						if out2, err2 := runPostgres(filepath.Join(pgBin, "pg_ctl"), "-D", pgData, "-l", logPath, "-w", "-t", "20", "start"); err2 == nil {
							return nil
						} else {
							return fmt.Errorf("%w: pg_ctl start after stale pid cleanup failed: %v (%s)", ErrMetadataCorrupted, err2, out2)
						}
					}
				}
			}
		}
		return fmt.Errorf("%w: control metadata start failed: %v (%s)", ErrMetadataCorrupted, err, out)
	}
	return nil
}

func applySchemaMigrations(ctx context.Context, db *pgxpool.Pool) error {
	if _, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INT PRIMARY KEY,
			description TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`); err != nil {
		return err
	}

	type migration struct {
		version int
		desc    string
		sql     string
	}
	migrations := []migration{
		{
			version: 1,
			desc:    "baseline control plane tables",
			sql: `
CREATE TABLE IF NOT EXISTS clusters (
	tenant_id TEXT PRIMARY KEY,
	system_identifier TEXT NOT NULL UNIQUE,
	state TEXT NOT NULL,
	epoch BIGINT NOT NULL DEFAULT 1,
	port INT NOT NULL DEFAULT 0,
	claim_source TEXT NOT NULL,
	app_password TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS idempotency_records (
	idempotency_key TEXT PRIMARY KEY,
	tenant_id TEXT NOT NULL,
	status TEXT NOT NULL,
	response_json TEXT,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS usage_ledger (
	idempotency_key TEXT PRIMARY KEY,
	gateway_id TEXT NOT NULL,
	tenant_id TEXT NOT NULL,
	statements INT NOT NULL,
	rows_affected BIGINT NOT NULL,
	bytes_out BIGINT NOT NULL,
	wall_ms BIGINT NOT NULL,
	ru_charged BIGINT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_events (
	id BIGSERIAL PRIMARY KEY,
	tenant_id TEXT NOT NULL,
	action TEXT NOT NULL,
	detail TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
`,
		},
		{
			version: 2,
			desc:    "tenant-scoped idempotency with payload hash, encrypted password column, and durable provisioning state machine",
			sql: `
ALTER TABLE clusters ADD COLUMN IF NOT EXISTS app_password_enc TEXT NOT NULL DEFAULT '';
ALTER TABLE clusters ADD COLUMN IF NOT EXISTS op_id TEXT NOT NULL DEFAULT '';
ALTER TABLE clusters ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE idempotency_records ADD COLUMN IF NOT EXISTS payload_hash TEXT NOT NULL DEFAULT '';
ALTER TABLE idempotency_records ADD COLUMN IF NOT EXISTS error_detail TEXT NOT NULL DEFAULT '';
ALTER TABLE idempotency_records ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS provisioning_ops (
	op_id TEXT PRIMARY KEY,
	tenant_id TEXT NOT NULL,
	idempotency_key TEXT NOT NULL,
	state TEXT NOT NULL,
	claim_source TEXT NOT NULL DEFAULT '',
	system_identifier TEXT NOT NULL DEFAULT '',
	error_detail TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_provisioning_ops_inflight ON provisioning_ops (state) WHERE state IN ('REQUESTED', 'CLAIMED', 'ACTIVATING');
`,
		},
	}

	for _, m := range migrations {
		var exists bool
		err := db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, m.version).Scan(&exists)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		tx, err := db.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, m.sql); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migration v%d (%s): %w", m.version, m.desc, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, description) VALUES ($1, $2)`, m.version, m.desc); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// SchemaVersion returns the highest applied migration version.
func (cp *ControlPlane) SchemaVersion(ctx context.Context) (int, error) {
	var ver int
	err := cp.db.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&ver)
	return ver, err
}

func (cp *ControlPlane) acquireKeyLock(k string) func() {
	cp.keyMu.Lock()
	rl, ok := cp.keyLocks[k]
	if !ok {
		rl = &refLock{}
		cp.keyLocks[k] = rl
	}
	rl.refs++
	cp.keyMu.Unlock()

	rl.mu.Lock()
	return func() {
		rl.mu.Unlock()
		cp.keyMu.Lock()
		rl.refs--
		if rl.refs <= 0 {
			delete(cp.keyLocks, k)
		}
		cp.keyMu.Unlock()
	}
}

func computeProvisionPayloadHash(tenantID string) string {
	sum := sha256.Sum256([]byte("provision:v1:" + tenantID))
	return hex.EncodeToString(sum[:])
}

func (cp *ControlPlane) encryptTenantPassword(tenantID, plaintext string) (string, error) {
	if cp.kms == nil {
		return "", errors.New("control: KMS unavailable for password encryption")
	}
	if _, err := cp.kms.EnsureTenantDEK(tenantID); err != nil {
		return "", err
	}
	ct, err := cp.kms.EncryptObject(tenantID, 0, "control/app_password", []byte(plaintext))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(ct), nil
}

func (cp *ControlPlane) decryptTenantPassword(tenantID, encB64 string) (string, error) {
	if encB64 == "" {
		return "", nil
	}
	if cp.kms == nil {
		return "", errors.New("control: KMS unavailable for password decryption")
	}
	raw, err := base64.StdEncoding.DecodeString(encB64)
	if err != nil {
		return "", err
	}
	pt, err := cp.kms.DecryptObject(tenantID, 0, "control/app_password", raw)
	if err != nil {
		return "", err
	}
	return string(pt), nil
}

// RecoverProvisioningOperations deterministically rolls back or reconciles any in-flight
// provisioning operations left in REQUESTED, CLAIMED, or ACTIVATING state after a crash.
func (cp *ControlPlane) RecoverProvisioningOperations(ctx context.Context) (int, error) {
	rows, err := cp.db.Query(ctx, `
		SELECT op_id, tenant_id, idempotency_key, state
		FROM provisioning_ops
		WHERE state IN ('REQUESTED', 'CLAIMED', 'ACTIVATING')
		ORDER BY created_at ASC
	`)
	if err != nil {
		return 0, err
	}
	type inflight struct {
		opID, tenantID, idempKey, state string
	}
	var list []inflight
	for rows.Next() {
		var item inflight
		if err := rows.Scan(&item.opID, &item.tenantID, &item.idempKey, &item.state); err == nil {
			list = append(list, item)
		}
	}
	rows.Close()

	recovered := 0
	for _, item := range list {
		if ValidateTenantID(item.tenantID) == nil && cp.agent != nil {
			switch item.state {
			case "CLAIMED":
				cp.agent.RollbackClaim(item.tenantID)
			case "ACTIVATING":
				cp.agent.RollbackClaim(item.tenantID)
				_ = cp.agent.StopAndPurgeTenant(item.tenantID)
			}
		}
		_, _ = cp.db.Exec(ctx, `DELETE FROM clusters WHERE tenant_id = $1 AND state = 'CLAIMING'`, item.tenantID)
		_, _ = cp.db.Exec(ctx, `
			UPDATE provisioning_ops
			SET state = 'ROLLED_BACK', error_detail = 'recovered after crash', updated_at = now()
			WHERE op_id = $1
		`, item.opID)
		_, _ = cp.db.Exec(ctx, `
			UPDATE idempotency_records
			SET status = 'FAILED', error_detail = 'interrupted provisioning rolled back on recovery', updated_at = now()
			WHERE idempotency_key = $1 AND tenant_id = $2 AND status = 'IN_PROGRESS'
		`, item.idempKey, item.tenantID)
		_, _ = cp.db.Exec(ctx, `
			INSERT INTO audit_events (tenant_id, action, detail)
			VALUES ($1, 'PROVISION_RECOVERED', $2)
		`, item.tenantID, fmt.Sprintf("op=%s prior_state=%s action=ROLLED_BACK", item.opID, item.state))
		recovered++
	}
	return recovered, nil
}

// ProvisionDatabase executes crash-consistent, tenant-scoped idempotent cluster provisioning.
func (cp *ControlPlane) ProvisionDatabase(ctx context.Context, idempotencyKey, tenantID string) (*ProvisionResponse, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return nil, err
	}
	if idempotencyKey == "" {
		idempotencyKey = "auto:" + tenantID
	}
	payloadHash := computeProvisionPayloadHash(tenantID)

	// Serialize per-key AND per-tenant so concurrent provisions/deletes for the same tenant cannot race.
	unlockKey := cp.acquireKeyLock("k:" + idempotencyKey)
	defer unlockKey()
	unlockTenant := cp.acquireKeyLock("t:" + tenantID)
	defer unlockTenant()

	// 1. Check existing idempotency record with strict tenant + payload hash verification.
	var recTenant, recStatus, recHash, respJSON string
	err := cp.db.QueryRow(ctx,
		`SELECT tenant_id, status, COALESCE(payload_hash, ''), COALESCE(response_json, '')
		 FROM idempotency_records WHERE idempotency_key = $1`,
		idempotencyKey,
	).Scan(&recTenant, &recStatus, &recHash, &respJSON)
	if err == nil {
		if recTenant != tenantID {
			return nil, fmt.Errorf("%w: key belongs to another tenant", ErrIdempotencyConflict)
		}
		if recHash != "" && recHash != payloadHash {
			return nil, fmt.Errorf("%w: payload mismatch for existing key", ErrIdempotencyConflict)
		}
		if recStatus == "COMPLETED" && respJSON != "" {
			var cached ProvisionResponse
			if jsonErr := json.Unmarshal([]byte(respJSON), &cached); jsonErr == nil {
				if cached.TenantID != tenantID {
					return nil, fmt.Errorf("%w: cached response tenant mismatch", ErrIdempotencyConflict)
				}
				// Re-hydrate tenant's own encrypted password if redacted in idempotency JSON.
				if cached.AppPassword == "[REDACTED]" || cached.AppPassword == "" {
					var encPW string
					if qErr := cp.db.QueryRow(ctx, `SELECT COALESCE(app_password_enc, '') FROM clusters WHERE tenant_id = $1`, tenantID).Scan(&encPW); qErr == nil && encPW != "" {
						if dec, dErr := cp.decryptTenantPassword(tenantID, encPW); dErr == nil {
							cached.AppPassword = dec
						}
					}
				}
				cached.IdempotentReplay = true
				return &cached, nil
			}
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	t0 := time.Now()
	opBytes := make([]byte, 8)
	_, _ = rand.Read(opBytes)
	opID := "op_" + hex.EncodeToString(opBytes)

	// Record REQUESTED in durable state machine and IN_PROGRESS in idempotency table.
	if _, err := cp.db.Exec(ctx, `
		INSERT INTO idempotency_records (idempotency_key, tenant_id, status, payload_hash, updated_at)
		VALUES ($1, $2, 'IN_PROGRESS', $3, now())
		ON CONFLICT (idempotency_key) DO UPDATE
		SET status = 'IN_PROGRESS', payload_hash = EXCLUDED.payload_hash, updated_at = now()
		WHERE idempotency_records.tenant_id = $2 AND idempotency_records.status IN ('FAILED', 'IN_PROGRESS')
	`, idempotencyKey, tenantID, payloadHash); err != nil {
		return nil, err
	}
	if _, err := cp.db.Exec(ctx, `
		INSERT INTO provisioning_ops (op_id, tenant_id, idempotency_key, state)
		VALUES ($1, $2, $3, 'REQUESTED')
	`, opID, tenantID, idempotencyKey); err != nil {
		return nil, err
	}

	rollbackOp := func(state string, cause error) error {
		if state == "CLAIMED" || state == "ACTIVATING" {
			cp.agent.RollbackClaim(tenantID)
		}
		if state == "ACTIVATING" {
			_ = cp.agent.StopAndPurgeTenant(tenantID)
		}
		_, _ = cp.db.Exec(ctx, `DELETE FROM clusters WHERE tenant_id = $1 AND state = 'CLAIMING'`, tenantID)
		_, _ = cp.db.Exec(ctx, `UPDATE provisioning_ops SET state = 'ROLLED_BACK', error_detail = $2, updated_at = now() WHERE op_id = $1`, opID, cause.Error())
		_, _ = cp.db.Exec(ctx, `UPDATE idempotency_records SET status = 'FAILED', error_detail = $2, updated_at = now() WHERE idempotency_key = $1 AND tenant_id = $3`, idempotencyKey, cause.Error(), tenantID)
		return cause
	}

	if err := cp.checkFault("after_requested"); err != nil {
		return nil, rollbackOp("REQUESTED", err)
	}

	// 2. Claim cluster from Cell Agent
	claim, err := cp.agent.ClaimCluster(tenantID)
	if err != nil {
		return nil, rollbackOp("REQUESTED", err)
	}
	_, _ = cp.db.Exec(ctx, `UPDATE provisioning_ops SET state = 'CLAIMED', claim_source = $2, system_identifier = $3, updated_at = now() WHERE op_id = $1`, opID, claim.Source, claim.SystemIdentifier)

	if err := cp.checkFault("after_claim"); err != nil {
		return nil, rollbackOp("CLAIMED", err)
	}

	passBytes := make([]byte, 16)
	if _, err := rand.Read(passBytes); err != nil {
		return nil, rollbackOp("CLAIMED", err)
	}
	appPassword := "pw_" + hex.EncodeToString(passBytes)

	encPassword, err := cp.encryptTenantPassword(tenantID, appPassword)
	if err != nil {
		return nil, rollbackOp("CLAIMED", err)
	}

	// 3. Commit UNIQUE(tenant_id) and UNIQUE(system_identifier) in Control Metadata DB BEFORE promoting directory!
	_, err = cp.db.Exec(ctx,
		`INSERT INTO clusters (tenant_id, system_identifier, state, epoch, port, claim_source, app_password, app_password_enc, op_id)
		 VALUES ($1, $2, 'CLAIMING', 1, 0, $3, '[ENCRYPTED]', $4, $5)`,
		tenantID, claim.SystemIdentifier, claim.Source, encPassword, opID,
	)
	if err != nil {
		return nil, rollbackOp("CLAIMED", fmt.Errorf("control db unique identity lock rejected claim: %w", err))
	}
	_, _ = cp.db.Exec(ctx, `UPDATE provisioning_ops SET state = 'ACTIVATING', updated_at = now() WHERE op_id = $1`, opID)

	if err := cp.checkFault("after_db_claim"); err != nil {
		return nil, rollbackOp("ACTIVATING", err)
	}

	// 4. Activate cluster on Cell (SCRAM-SHA-256 + supautils + SELECT 1 readiness check)
	rt, err := cp.agent.ActivateCluster(tenantID, appPassword, 1)
	if err != nil {
		return nil, rollbackOp("ACTIVATING", fmt.Errorf("activate cluster failed: %w", err))
	}

	if err := cp.checkFault("after_activate"); err != nil {
		return nil, rollbackOp("ACTIVATING", err)
	}

	totalMs := float64(time.Since(t0).Microseconds()) / 1000.0
	resp := &ProvisionResponse{
		TenantID:         tenantID,
		SystemIdentifier: claim.SystemIdentifier,
		State:            "ACTIVE",
		Epoch:            1,
		Port:             rt.Port,
		PgBouncerAddr:    cp.agent.PgBouncerAddr(),
		AppUser:          "app_user",
		AppPassword:      appPassword,
		ClaimSource:      claim.Source,
		ClaimLatencyMs:   claim.ClaimLatencyMs,
		TotalLatencyMs:   totalMs,
		IdempotentReplay: false,
	}

	// Store redacted response JSON so plaintext app_password is never persisted in idempotency_records.
	redacted := *resp
	redacted.AppPassword = "[REDACTED]"
	b, _ := json.Marshal(&redacted)

	tx, err := cp.db.Begin(ctx)
	if err != nil {
		return nil, rollbackOp("ACTIVATING", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE clusters SET state = 'ACTIVE', port = $2, updated_at = now() WHERE tenant_id = $1`, tenantID, rt.Port); err != nil {
		_ = tx.Rollback(ctx)
		return nil, rollbackOp("ACTIVATING", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE provisioning_ops SET state = 'ACTIVE', updated_at = now() WHERE op_id = $1`, opID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, rollbackOp("ACTIVATING", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE idempotency_records SET status = 'COMPLETED', response_json = $2, updated_at = now() WHERE idempotency_key = $1 AND tenant_id = $3`, idempotencyKey, string(b), tenantID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, rollbackOp("ACTIVATING", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events (tenant_id, action, detail) VALUES ($1, 'PROVISION', $2)`, tenantID, fmt.Sprintf("op=%s sysid=%s source=%s", opID, claim.SystemIdentifier, claim.Source)); err != nil {
		_ = tx.Rollback(ctx)
		return nil, rollbackOp("ACTIVATING", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, rollbackOp("ACTIVATING", err)
	}

	return resp, nil
}

// GetTenantAuth fetches the tenant's state, epoch, Direct Port, and decrypted SCRAM password for Gateway routing.
func (cp *ControlPlane) GetTenantAuth(ctx context.Context, tenantID string) (state string, epoch int64, directPort int, appPassword string, err error) {
	if err = ValidateTenantID(tenantID); err != nil {
		return
	}
	var legacyPW, encPW string
	err = cp.db.QueryRow(ctx, `SELECT state, epoch, port, app_password, COALESCE(app_password_enc, '') FROM clusters WHERE tenant_id = $1`, tenantID).
		Scan(&state, &epoch, &directPort, &legacyPW, &encPW)
	if err != nil {
		return
	}
	if encPW != "" {
		appPassword, err = cp.decryptTenantPassword(tenantID, encPW)
		return
	}
	appPassword = legacyPW
	return
}

// DeleteDatabase implements Flow 12: 7-day Soft Delete vs Immediate Hard Purge (crypto-shredding DEK).
func (cp *ControlPlane) DeleteDatabase(ctx context.Context, tenantID string, immediate bool) error {
	if err := ValidateTenantID(tenantID); err != nil {
		return err
	}
	unlockTenant := cp.acquireKeyLock("t:" + tenantID)
	defer unlockTenant()

	if !immediate {
		_, err := cp.db.Exec(ctx, `UPDATE clusters SET state = 'SOFT_DELETED', epoch = epoch + 1, updated_at = now() WHERE tenant_id = $1`, tenantID)
		if err != nil {
			return err
		}
		_ = cp.agent.StopTenantOnly(tenantID)
		_, _ = cp.db.Exec(ctx, `INSERT INTO audit_events (tenant_id, action, detail) VALUES ($1, 'SOFT_DELETE', '7-day grace window started; auth revoked; DEK retained')`, tenantID)
		return nil
	}

	cp.kms.ShredDEK(tenantID)
	_ = cp.agent.StopAndPurgeTenant(tenantID)
	_, err := cp.db.Exec(ctx, `UPDATE clusters SET state = 'PURGED', epoch = epoch + 1, updated_at = now() WHERE tenant_id = $1`, tenantID)
	_, _ = cp.db.Exec(ctx, `INSERT INTO audit_events (tenant_id, action, detail) VALUES ($1, 'CRYPTO_SHRED_PURGE', 'tenant DEK destroyed; local PGDATA purged')`, tenantID)
	return err
}

// IngestUsageBatch inserts Gateway RU usage windows idempotently using ON CONFLICT (idempotency_key) DO NOTHING.
func (cp *ControlPlane) IngestUsageBatch(ctx context.Context, batch []UsageEventBatch) (inserted int, err error) {
	for _, ev := range batch {
		tag, err := cp.db.Exec(ctx,
			`INSERT INTO usage_ledger (idempotency_key, gateway_id, tenant_id, statements, rows_affected, bytes_out, wall_ms, ru_charged)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			 ON CONFLICT (idempotency_key) DO NOTHING`,
			ev.IdempotencyKey, ev.GatewayID, ev.TenantID, ev.Statements, ev.RowsAffected, ev.BytesOut, ev.WallMs, ev.RUCharged,
		)
		if err != nil {
			return inserted, err
		}
		inserted += int(tag.RowsAffected())
	}
	return inserted, nil
}

// GetTenantUsageSummary returns total RUs and batch count recorded in usage_ledger for a tenant.
func (cp *ControlPlane) GetTenantUsageSummary(ctx context.Context, tenantID string) (batches int, statements int64, totalRUs int64, err error) {
	err = cp.db.QueryRow(ctx,
		`SELECT COUNT(*), COALESCE(SUM(statements), 0), COALESCE(SUM(ru_charged), 0) FROM usage_ledger WHERE tenant_id = $1`,
		tenantID,
	).Scan(&batches, &statements, &totalRUs)
	return
}

// ReconcileOnce compares Control Metadata DB truth against live Cell directories and purges any orphans.
func (cp *ControlPlane) ReconcileOnce(ctx context.Context) (int, error) {
	rows, err := cp.db.Query(ctx, `SELECT tenant_id FROM clusters WHERE state IN ('ACTIVE', 'SOFT_DELETED')`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	valid := make(map[string]bool)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			valid[id] = true
		}
	}
	purged := cp.agent.ReconcileInventory(valid)
	return purged, nil
}

// CountDistinctSystemIdentifiers returns total clusters and distinct system_identifier count in Control DB.
func (cp *ControlPlane) CountDistinctSystemIdentifiers(ctx context.Context) (total int, distinctSysIDs int, err error) {
	err = cp.db.QueryRow(ctx, `SELECT COUNT(*), COUNT(DISTINCT system_identifier) FROM clusters WHERE state = 'ACTIVE'`).
		Scan(&total, &distinctSysIDs)
	return
}

func (cp *ControlPlane) Close() {
	if cp.db != nil {
		cp.db.Close()
	}
	pgData := filepath.Join(cp.metaDir, "pgdata")
	_, _ = runPostgres(filepath.Join(pgBin, "pg_ctl"), "-D", pgData, "-m", "fast", "-w", "-t", "10", "stop")
}

func runPostgres(bin string, args ...string) (string, error) {
	fullArgs := append([]string{"-u", "postgres", "--", bin}, args...)
	out, err := exec.Command("runuser", fullArgs...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func Itoa(n int) string {
	return strconv.Itoa(n)
}
