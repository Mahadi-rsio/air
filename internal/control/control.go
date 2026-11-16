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
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Mahadi-rsio/air/internal/backup"
	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/fencing"
	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/Mahadi-rsio/air/internal/tenant"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/singleflight"
)

const currentSchemaVersion = 2

var (
	ErrIdempotencyConflict = errors.New("idempotency key conflict: different account or payload")
	ErrNotFound            = errors.New("database not found")
	ErrForbidden           = errors.New("forbidden: database belongs to another account")
	ErrUnauthorized        = errors.New("unauthorized: missing, invalid, or revoked API key")
	ErrCorruptedMetadata   = errors.New("corrupted metadata directory: refusing destructive reset")
	ErrBackupNotConfigured = errors.New("backup store is not configured")
)

type APIKeyInfo struct {
	KeyID       string `json:"key_id"`
	AccountID   string `json:"account_id"`
	Token       string `json:"token,omitempty"`
	Description string `json:"description"`
}

type ProvisionResponse struct {
	AccountID        string `json:"account_id"`
	TenantID         string `json:"tenant_id"`
	Database         string `json:"database"`
	Username         string `json:"username"`
	Password         string `json:"password,omitempty"`
	DirectPort       int    `json:"direct_port"`
	SystemIdentifier string `json:"system_identifier"`
	FencingEpoch     int64  `json:"fencing_epoch"`
	State            string `json:"state"`
}

type DatabaseInfo struct {
	AccountID        string `json:"account_id"`
	TenantID         string `json:"tenant_id"`
	DirectPort       int    `json:"direct_port"`
	SystemIdentifier string `json:"system_identifier"`
	FencingEpoch     int64  `json:"fencing_epoch"`
	State            string `json:"state"`
}

// ControlPlaneConfig configures ControlPlane metadata storage (either external HA MetaDSN or local MetaDir).
type ControlPlaneConfig struct {
	MetaDir   string
	MetaDSN   string
	PGBinDir  string
	MetaPort  int
	Agent     *cell.Agent
	KMS       *crypto.EnvelopeKMS
	R2Primary bool
}

// ControlPlane manages durable PG17 metadata, schema migrations, provisioning state machine, and tenant routing.
type ControlPlane struct {
	metaDir      string
	metaDSN      string
	externalMeta bool
	pgBin        string
	metaPort     int
	db           *pgxpool.Pool
	agent        *cell.Agent
	kms          *crypto.EnvelopeKMS
	provMu       sync.Mutex
	faultHook    func(stage string) error
	gatewayPort  int
	s3Client     *s3.Client
	backupBucket string
	r2Primary    bool
	pageserver   *pageserver.Pageserver
	psMu         sync.RWMutex
	wakeGroup    singleflight.Group
	activityMu   sync.Mutex
	lastActivity map[string]time.Time
	walMu        sync.Mutex
	walReceivers map[string]context.CancelFunc
	walDone      map[string]chan struct{}
}

// NewControlPlane initializes a ControlPlane backed by a local single-node PG17 MetaDir.
func NewControlPlane(ctx context.Context, metaDir, pgBinDir string, metaPort int, ag *cell.Agent, kms *crypto.EnvelopeKMS) (*ControlPlane, error) {
	return NewControlPlaneWithConfig(ctx, ControlPlaneConfig{
		MetaDir:  metaDir,
		PGBinDir: pgBinDir,
		MetaPort: metaPort,
		Agent:    ag,
		KMS:      kms,
	})
}

// NewControlPlaneWithConfig initializes a ControlPlane using either an external HA PostgreSQL DSN (cfg.MetaDSN)
// or a self-managed local PG17 instance in cfg.MetaDir when cfg.MetaDSN is empty.
func NewControlPlaneWithConfig(ctx context.Context, cfg ControlPlaneConfig) (*ControlPlane, error) {
	pgBinDir := cfg.PGBinDir
	if pgBinDir == "" {
		pgBinDir = "/usr/lib/postgresql/17/bin"
	}

	metaDSN := strings.TrimSpace(cfg.MetaDSN)
	externalMeta := metaDSN != ""
	dsn := metaDSN

	if !externalMeta {
		if cfg.MetaDir == "" {
			return nil, errors.New("control metadata requires either MetaDSN or MetaDir")
		}
		if err := os.MkdirAll(cfg.MetaDir, 0o755); err != nil {
			return nil, err
		}
		pgData := filepath.Join(cfg.MetaDir, "pgdata")
		sockDir := filepath.Join(cfg.MetaDir, "sock")
		if err := os.MkdirAll(sockDir, 0o755); err != nil {
			return nil, err
		}

		// Safe existing-database detection: never os.RemoveAll(metaDir).
		pgVersionPath := filepath.Join(pgData, "PG_VERSION")
		if _, err := os.Stat(pgVersionPath); os.IsNotExist(err) {
			if entries, rErr := os.ReadDir(pgData); rErr == nil && len(entries) > 0 {
				return nil, fmt.Errorf("%w: %s has files without PG_VERSION", ErrCorruptedMetadata, pgData)
			}
			if err := os.MkdirAll(pgData, 0o700); err != nil {
				return nil, err
			}
			if out, err := runPostgres(pgBinDir, "initdb", "-D", pgData, "-U", "postgres", "--auth=trust", "--data-checksums", "--no-instructions"); err != nil {
				return nil, fmt.Errorf("control metadata initdb: %w (%s)", err, out)
			}
			conf := fmt.Sprintf("listen_addresses = '127.0.0.1'\nport = %d\nunix_socket_directories = '%s'\nmax_connections = 100\nshared_buffers = 32MB\nssl = off\n", cfg.MetaPort, sockDir)
			if err := os.WriteFile(filepath.Join(pgData, "postgresql.conf"), []byte(conf), 0o600); err != nil {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		} else {
			// Ensure port/sockDir in postgresql.conf match current runtime settings cleanly.
			conf := fmt.Sprintf("listen_addresses = '127.0.0.1'\nport = %d\nunix_socket_directories = '%s'\nmax_connections = 100\nshared_buffers = 32MB\nssl = off\n", cfg.MetaPort, sockDir)
			if err := os.WriteFile(filepath.Join(pgData, "postgresql.conf"), []byte(conf), 0o600); err != nil {
				return nil, err
			}
		}

		if _, err := os.Stat(filepath.Join(pgData, "postmaster.pid")); err != nil {
			logFile := filepath.Join(cfg.MetaDir, "control-pg.log")
			if out, err := runPostgres(pgBinDir, "pg_ctl", "-D", pgData, "-l", logFile, "-w", "-t", "15", "start"); err != nil {
				return nil, fmt.Errorf("control metadata start: %w (%s)", err, out)
			}
		}

		dsn = fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", cfg.MetaPort)
	}

	var pool *pgxpool.Pool
	var lastErr error
	for i := 0; i < 40; i++ {
		if ctx.Err() != nil {
			lastErr = ctx.Err()
			break
		}
		pool, lastErr = pgxpool.New(ctx, dsn)
		if lastErr == nil {
			if pingErr := pool.Ping(ctx); pingErr == nil {
				break
			} else {
				lastErr = pingErr
				pool.Close()
				pool = nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if pool == nil {
		return nil, fmt.Errorf("control metadata connect: %w", lastErr)
	}

	cp := &ControlPlane{
		metaDir:      cfg.MetaDir,
		metaDSN:      metaDSN,
		externalMeta: externalMeta,
		pgBin:        pgBinDir,
		metaPort:     cfg.MetaPort,
		db:           pool,
		agent:        cfg.Agent,
		kms:          cfg.KMS,
		r2Primary:    cfg.R2Primary,
		lastActivity: make(map[string]time.Time),
	}
	if err := cp.runMigrations(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := cp.reloadDEKs(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if cfg.Agent != nil {
		cfg.Agent.SetEpochChecker(func(tenantID string) (int64, error) {
			var epoch int64
			err := cp.db.QueryRow(context.Background(), `SELECT fencing_epoch FROM clusters WHERE tenant_id = $1`, tenantID).Scan(&epoch)
			return epoch, err
		})
	}
	return cp, nil
}

func (cp *ControlPlane) SetFaultHook(fn func(stage string) error) {
	cp.provMu.Lock()
	cp.faultHook = fn
	cp.provMu.Unlock()
}

func (cp *ControlPlane) SetGatewayPort(port int) {
	cp.gatewayPort = port
}

func (cp *ControlPlane) SetBackupStore(s3c *s3.Client, bucket string) {
	cp.provMu.Lock()
	cp.s3Client = s3c
	cp.backupBucket = strings.TrimSpace(bucket)
	cp.provMu.Unlock()
}

// SetPageserver attaches an in-process AIR Pageserver for layer flushing and wake coordination.
func (cp *ControlPlane) SetPageserver(ps *pageserver.Pageserver) {
	cp.psMu.Lock()
	cp.pageserver = ps
	cp.psMu.Unlock()
}

func (cp *ControlPlane) getPageserver() *pageserver.Pageserver {
	cp.psMu.RLock()
	defer cp.psMu.RUnlock()
	return cp.pageserver
}

func parsePGLSN(s string) (uint64, error) {
	var hi, lo uint32
	if _, err := fmt.Sscanf(strings.TrimSpace(s), "%X/%X", &hi, &lo); err != nil {
		return 0, fmt.Errorf("parse LSN %q: %w", s, err)
	}
	return (uint64(hi) << 32) | uint64(lo), nil
}

func (cp *ControlPlane) ensurePageserverBootstrapAndWAL(ctx context.Context, tenantID string, rt *cell.TenantRuntime) error {
	ps := cp.getPageserver()
	if ps == nil || rt == nil {
		return nil
	}

	tl := ps.EnsureTenant(tenantID, rt.FencingEpoch)
	if tl.LastLSN() == 0 && rt.PGData != "" && rt.SockDir != "" && rt.Port > 0 {
		connStr := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres", rt.SockDir, rt.Port)
		connCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		conn, err := pgx.Connect(connCtx, connStr)
		if err != nil {
			cancel()
			return fmt.Errorf("connect for pageserver bootstrap: %w", err)
		}
		var lsnStr string
		if _, err := conn.Exec(connCtx, `CHECKPOINT`); err != nil {
			_ = conn.Close(connCtx)
			cancel()
			return fmt.Errorf("checkpoint for pageserver bootstrap: %w", err)
		}
		if err := conn.QueryRow(connCtx, `SELECT pg_current_wal_lsn()::text`).Scan(&lsnStr); err != nil {
			_ = conn.Close(connCtx)
			cancel()
			return fmt.Errorf("read init LSN for pageserver bootstrap: %w", err)
		}
		_ = conn.Close(connCtx)
		cancel()

		initLSN, err := parsePGLSN(lsnStr)
		if err != nil {
			return err
		}
		if _, err := ps.BootstrapTenantFromPGData(ctx, tenantID, rt.FencingEpoch, rt.PGData, initLSN); err != nil {
			return fmt.Errorf("bootstrap pageserver from pgdata: %w", err)
		}
	}

	if rt.SockDir == "" || rt.Port <= 0 {
		return nil
	}

	cp.stopWALReceiver(tenantID)

	rxCtx, rxCancel := context.WithCancel(context.Background())
	doneCh := make(chan struct{})
	dsn := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres replication=database", rt.SockDir, rt.Port)
	rx := pageserver.NewWALReceiver(dsn, tl, tl.LastLSN())

	cp.walMu.Lock()
	if cp.walReceivers == nil {
		cp.walReceivers = make(map[string]context.CancelFunc)
	}
	if cp.walDone == nil {
		cp.walDone = make(map[string]chan struct{})
	}
	cp.walReceivers[tenantID] = rxCancel
	cp.walDone[tenantID] = doneCh
	cp.walMu.Unlock()

	go func() {
		defer close(doneCh)
		if err := rx.Run(rxCtx); err != nil && rxCtx.Err() == nil {
			log.Printf("[pageserver] walreceiver for %s exited: %v", tenantID, err)
		}
	}()
	return nil
}

func (cp *ControlPlane) stopWALReceiver(tenantID string) {
	cp.walMu.Lock()
	cancel := cp.walReceivers[tenantID]
	done := cp.walDone[tenantID]
	delete(cp.walReceivers, tenantID)
	delete(cp.walDone, tenantID)
	cp.walMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
}

func (cp *ControlPlane) stopAllWALReceivers() {
	cp.walMu.Lock()
	cancels := make([]context.CancelFunc, 0, len(cp.walReceivers))
	dones := make([]chan struct{}, 0, len(cp.walDone))
	for k, c := range cp.walReceivers {
		cancels = append(cancels, c)
		delete(cp.walReceivers, k)
	}
	for k, d := range cp.walDone {
		dones = append(dones, d)
		delete(cp.walDone, k)
	}
	cp.walMu.Unlock()

	for _, c := range cancels {
		c()
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for _, d := range dones {
		select {
		case <-d:
		case <-deadline.C:
			return
		}
	}
}

func (cp *ControlPlane) backupStore() (*s3.Client, string) {
	cp.provMu.Lock()
	defer cp.provMu.Unlock()
	return cp.s3Client, cp.backupBucket
}

func (cp *ControlPlane) runMigrations(ctx context.Context) error {
	if _, err := cp.db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return err
	}
	var current int
	_ = cp.db.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&current)
	if current < 1 {
		tx, err := cp.db.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		stmts := []string{
			`CREATE TABLE IF NOT EXISTS clusters (
				tenant_id TEXT PRIMARY KEY,
				account_id TEXT NOT NULL,
				state TEXT NOT NULL,
				direct_port INT NOT NULL DEFAULT 0,
				system_identifier TEXT NOT NULL DEFAULT '',
				fencing_epoch BIGINT NOT NULL DEFAULT 1,
				app_password_enc TEXT NOT NULL DEFAULT '',
				wrapped_dek TEXT NOT NULL DEFAULT '',
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
			`CREATE TABLE IF NOT EXISTS idempotency_records (
				account_id TEXT NOT NULL,
				idempotency_key TEXT NOT NULL,
				tenant_id TEXT NOT NULL,
				payload_hash TEXT NOT NULL,
				response_json JSONB NOT NULL,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				PRIMARY KEY (account_id, idempotency_key)
			)`,
			`CREATE TABLE IF NOT EXISTS provisioning_ops (
				op_id BIGSERIAL PRIMARY KEY,
				account_id TEXT NOT NULL,
				tenant_id TEXT NOT NULL,
				idempotency_key TEXT NOT NULL,
				state TEXT NOT NULL,
				last_error TEXT NOT NULL DEFAULT '',
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
			`INSERT INTO schema_migrations(version) VALUES (1) ON CONFLICT DO NOTHING`,
		}
		for _, s := range stmts {
			if _, err := tx.Exec(ctx, s); err != nil {
				return fmt.Errorf("migration v1 failed: %w", err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	if current < 2 {
		tx, err := cp.db.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		stmts := []string{
			`CREATE TABLE IF NOT EXISTS api_keys (
				key_id TEXT PRIMARY KEY,
				key_sha256 TEXT UNIQUE NOT NULL,
				account_id TEXT NOT NULL,
				description TEXT NOT NULL DEFAULT '',
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				revoked_at TIMESTAMPTZ
			)`,
			`CREATE INDEX IF NOT EXISTS idx_api_keys_account_id ON api_keys(account_id)`,
			`INSERT INTO schema_migrations(version) VALUES (2) ON CONFLICT DO NOTHING`,
		}
		for _, s := range stmts {
			if _, err := tx.Exec(ctx, s); err != nil {
				return fmt.Errorf("migration v2 failed: %w", err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (cp *ControlPlane) reloadDEKs(ctx context.Context) error {
	if cp.kms == nil {
		return nil
	}
	rows, err := cp.db.Query(ctx, `SELECT tenant_id, wrapped_dek FROM clusters WHERE wrapped_dek <> '' AND state <> 'PURGED'`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var tid, w string
		if err := rows.Scan(&tid, &w); err != nil {
			return err
		}
		if err := cp.kms.LoadWrappedDEK(tid, w); err != nil {
			return fmt.Errorf("reload DEK for %s: %w", tid, err)
		}
	}
	return rows.Err()
}

func (cp *ControlPlane) SchemaVersion(ctx context.Context) (int, error) {
	var v int
	err := cp.db.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&v)
	return v, err
}

func payloadHash(accountID, tenantID string) string {
	sum := sha256.Sum256([]byte(accountID + "|" + tenantID))
	return hex.EncodeToString(sum[:])
}

// ProvisionDatabase creates an isolated PostgreSQL 17 tenant database using a durable state machine
// and account-scoped idempotency.
func (cp *ControlPlane) ProvisionDatabase(ctx context.Context, accountID, idempotencyKey, tenantID string) (*ProvisionResponse, error) {
	if err := tenant.ValidateAccountID(accountID); err != nil {
		return nil, err
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, err
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, errors.New("idempotency_key is required")
	}

	cp.provMu.Lock()
	defer cp.provMu.Unlock()

	pHash := payloadHash(accountID, tenantID)

	// 1. Check account-scoped idempotency record.
	var storedTenant, storedHash string
	var storedResp []byte
	err := cp.db.QueryRow(ctx,
		`SELECT tenant_id, payload_hash, response_json FROM idempotency_records WHERE account_id = $1 AND idempotency_key = $2`,
		accountID, idempotencyKey,
	).Scan(&storedTenant, &storedHash, &storedResp)
	if err == nil {
		if storedTenant != tenantID || storedHash != pHash {
			return nil, ErrIdempotencyConflict
		}
		var out ProvisionResponse
		if uErr := json.Unmarshal(storedResp, &out); uErr != nil {
			return nil, uErr
		}
		// Decrypt password for owning account replay.
		if pw, dErr := cp.decryptTenantPassword(ctx, accountID, tenantID); dErr == nil {
			out.Password = pw
		}
		return &out, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	// Ensure tenant_id is not already owned by another account or active.
	var existingAcct, existingState string
	err = cp.db.QueryRow(ctx, `SELECT account_id, state FROM clusters WHERE tenant_id = $1`, tenantID).Scan(&existingAcct, &existingState)
	if err == nil && existingState != "PURGED" {
		if existingAcct != accountID {
			return nil, ErrForbidden
		}
		return nil, fmt.Errorf("tenant %s already exists in state %s", tenantID, existingState)
	}

	// 2. Insert REQUESTED op + initial cluster record.
	var opID int64
	if err := cp.db.QueryRow(ctx,
		`INSERT INTO provisioning_ops (account_id, tenant_id, idempotency_key, state) VALUES ($1, $2, $3, 'REQUESTED') RETURNING op_id`,
		accountID, tenantID, idempotencyKey,
	).Scan(&opID); err != nil {
		return nil, err
	}

	if cp.faultHook != nil {
		if fErr := cp.faultHook("after_requested"); fErr != nil {
			cp.rollbackOp(ctx, opID, tenantID, fErr)
			return nil, fErr
		}
	}

	// 3. CLAIMED: initdb via Cell Agent.
	sysID, err := cp.agent.ClaimCluster(tenantID)
	if err != nil {
		cp.rollbackOp(ctx, opID, tenantID, err)
		return nil, err
	}
	if _, err := cp.db.Exec(ctx, `UPDATE provisioning_ops SET state = 'CLAIMED', updated_at = now() WHERE op_id = $1`, opID); err != nil {
		cp.rollbackOp(ctx, opID, tenantID, err)
		return nil, err
	}
	if cp.faultHook != nil {
		if fErr := cp.faultHook("after_claimed"); fErr != nil {
			cp.rollbackOp(ctx, opID, tenantID, fErr)
			return nil, fErr
		}
	}

	// 4. ACTIVATING: generate password, wrap DEK, encrypt password, activate PG17.
	appPW := randomPassword(24)
	wrappedDEK, err := cp.kms.EnsureTenantDEK(tenantID)
	if err != nil {
		cp.rollbackOp(ctx, opID, tenantID, err)
		return nil, err
	}
	encBytes, err := cp.kms.EncryptObject(tenantID, 1, "app_password", []byte(appPW))
	if err != nil {
		cp.rollbackOp(ctx, opID, tenantID, err)
		return nil, err
	}
	encB64 := base64.StdEncoding.EncodeToString(encBytes)

	if _, err := cp.db.Exec(ctx, `UPDATE provisioning_ops SET state = 'ACTIVATING', updated_at = now() WHERE op_id = $1`, opID); err != nil {
		cp.rollbackOp(ctx, opID, tenantID, err)
		return nil, err
	}

	rt, err := cp.agent.ActivateCluster(tenantID, appPW, 1)
	if err != nil {
		cp.rollbackOp(ctx, opID, tenantID, err)
		return nil, err
	}
	if cp.faultHook != nil {
		if fErr := cp.faultHook("after_activating"); fErr != nil {
			cp.rollbackOp(ctx, opID, tenantID, fErr)
			return nil, fErr
		}
	}

	// 5. Commit ACTIVE cluster + idempotency record (without plaintext password in response_json!).
	resp := ProvisionResponse{
		AccountID:        accountID,
		TenantID:         tenantID,
		Database:         tenantID,
		Username:         "app_user",
		DirectPort:       rt.Port,
		SystemIdentifier: sysID,
		FencingEpoch:     rt.FencingEpoch,
		State:            "ACTIVE",
	}
	respNoSecretJSON, _ := json.Marshal(resp)

	tx, err := cp.db.Begin(ctx)
	if err != nil {
		cp.rollbackOp(ctx, opID, tenantID, err)
		return nil, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO clusters (tenant_id, account_id, state, direct_port, system_identifier, fencing_epoch, app_password_enc, wrapped_dek)
		VALUES ($1, $2, 'ACTIVE', $3, $4, $5, $6, $7)
		ON CONFLICT (tenant_id) DO UPDATE SET
			account_id = EXCLUDED.account_id,
			state = 'ACTIVE',
			direct_port = EXCLUDED.direct_port,
			system_identifier = EXCLUDED.system_identifier,
			fencing_epoch = EXCLUDED.fencing_epoch,
			app_password_enc = EXCLUDED.app_password_enc,
			wrapped_dek = EXCLUDED.wrapped_dek,
			updated_at = now()
	`, tenantID, accountID, rt.Port, sysID, rt.FencingEpoch, encB64, wrappedDEK); err != nil {
		cp.rollbackOp(ctx, opID, tenantID, err)
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO idempotency_records (account_id, idempotency_key, tenant_id, payload_hash, response_json)
		VALUES ($1, $2, $3, $4, $5::jsonb)
	`, accountID, idempotencyKey, tenantID, pHash, string(respNoSecretJSON)); err != nil {
		cp.rollbackOp(ctx, opID, tenantID, err)
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE provisioning_ops SET state = 'ACTIVE', updated_at = now() WHERE op_id = $1`, opID); err != nil {
		cp.rollbackOp(ctx, opID, tenantID, err)
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		cp.rollbackOp(ctx, opID, tenantID, err)
		return nil, err
	}

	if err := cp.ensurePageserverBootstrapAndWAL(ctx, tenantID, rt); err != nil {
		log.Printf("[pageserver] bootstrap/walreceiver for %s failed: %v", tenantID, err)
	}

	resp.Password = appPW
	return &resp, nil
}

func (cp *ControlPlane) rollbackOp(ctx context.Context, opID int64, tenantID string, cause error) {
	if cp.agent != nil {
		if pErr := cp.agent.StopAndPurgeTenant(tenantID); pErr != nil {
			_, _ = cp.db.Exec(ctx, `UPDATE provisioning_ops SET state = 'ROLLBACK_FAILED', last_error = $2, updated_at = now() WHERE op_id = $1`, opID, pErr.Error())
			return
		}
	}
	_, _ = cp.db.Exec(ctx, `UPDATE provisioning_ops SET state = 'ROLLED_BACK', last_error = $2, updated_at = now() WHERE op_id = $1`, opID, cause.Error())
	_, _ = cp.db.Exec(ctx, `DELETE FROM clusters WHERE tenant_id = $1 AND state <> 'ACTIVE'`, tenantID)
}

// RecoverProvisioningOperations reconciles incomplete provisioning/deletion operations after a crash or restart.
func (cp *ControlPlane) RecoverProvisioningOperations(ctx context.Context) (int, error) {
	rows, err := cp.db.Query(ctx, `SELECT op_id, tenant_id, state FROM provisioning_ops WHERE state IN ('REQUESTED', 'CLAIMED', 'ACTIVATING', 'DELETING') ORDER BY op_id ASC`)
	if err != nil {
		return 0, err
	}
	type pendingOp struct {
		opID     int64
		tenantID string
		state    string
	}
	var ops []pendingOp
	for rows.Next() {
		var p pendingOp
		if err := rows.Scan(&p.opID, &p.tenantID, &p.state); err != nil {
			rows.Close()
			return 0, err
		}
		ops = append(ops, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	recovered := 0
	for _, op := range ops {
		if cp.agent != nil {
			if err := cp.agent.StopAndPurgeTenant(op.tenantID); err != nil {
				_, _ = cp.db.Exec(ctx, `UPDATE provisioning_ops SET state = 'ROLLBACK_FAILED', last_error = $2, updated_at = now() WHERE op_id = $1`, op.opID, err.Error())
				continue
			}
		}
		if op.state == "DELETING" {
			_, _ = cp.db.Exec(ctx, `UPDATE clusters SET state = 'PURGED', updated_at = now() WHERE tenant_id = $1`, op.tenantID)
			_, _ = cp.db.Exec(ctx, `UPDATE provisioning_ops SET state = 'PURGED', updated_at = now() WHERE op_id = $1`, op.opID)
		} else {
			_, _ = cp.db.Exec(ctx, `DELETE FROM clusters WHERE tenant_id = $1 AND state <> 'ACTIVE'`, op.tenantID)
			_, _ = cp.db.Exec(ctx, `UPDATE provisioning_ops SET state = 'ROLLED_BACK', last_error = 'recovered interrupted provisioning', updated_at = now() WHERE op_id = $1`, op.opID)
		}
		recovered++
	}

	// Reconcile ACTIVE clusters against Cell Agent runtime (e.g. if PG stopped cleanly during server restart).
	if cp.agent != nil {
		cRows, err := cp.db.Query(ctx, `SELECT tenant_id, fencing_epoch FROM clusters WHERE state = 'ACTIVE'`)
		if err == nil {
			type activeRow struct {
				tid   string
				epoch int64
			}
			var active []activeRow
			for cRows.Next() {
				var r activeRow
				if err := cRows.Scan(&r.tid, &r.epoch); err == nil {
					active = append(active, r)
				}
			}
			cRows.Close()
			for _, r := range active {
				if _, ok := cp.agent.GetTenant(r.tid); !ok {
					if rt, err := cp.agent.StartOrAdoptTenant(r.tid, r.epoch); err == nil {
						_, _ = cp.db.Exec(ctx, `UPDATE clusters SET direct_port = $2, updated_at = now() WHERE tenant_id = $1`, r.tid, rt.Port)
					} else {
						log.Printf("[control] warn: failed to adopt active tenant %s: %v", r.tid, err)
					}
				}
			}
		}
	}
	return recovered, nil
}

func (cp *ControlPlane) GetDatabase(ctx context.Context, accountID, tenantID string) (*DatabaseInfo, error) {
	if err := tenant.ValidateAccountID(accountID); err != nil {
		return nil, err
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, err
	}
	var info DatabaseInfo
	err := cp.db.QueryRow(ctx,
		`SELECT account_id, tenant_id, direct_port, system_identifier, fencing_epoch, state FROM clusters WHERE tenant_id = $1`,
		tenantID,
	).Scan(&info.AccountID, &info.TenantID, &info.DirectPort, &info.SystemIdentifier, &info.FencingEpoch, &info.State)
	if errors.Is(err, pgx.ErrNoRows) || info.State == "PURGED" {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if info.AccountID != accountID {
		return nil, ErrForbidden
	}
	return &info, nil
}

func (cp *ControlPlane) DeleteDatabase(ctx context.Context, accountID, tenantID string) error {
	if err := tenant.ValidateAccountID(accountID); err != nil {
		return err
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return err
	}
	var owner, state string
	err := cp.db.QueryRow(ctx, `SELECT account_id, state FROM clusters WHERE tenant_id = $1`, tenantID).Scan(&owner, &state)
	if errors.Is(err, pgx.ErrNoRows) || state == "PURGED" {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if owner != accountID {
		return ErrForbidden
	}

	if _, err := cp.db.Exec(ctx, `UPDATE clusters SET state = 'DELETING', updated_at = now() WHERE tenant_id = $1`, tenantID); err != nil {
		return err
	}
	cp.stopWALReceiver(tenantID)
	if cp.agent != nil {
		if err := cp.agent.StopAndPurgeTenant(tenantID); err != nil {
			return err
		}
	}
	if cp.kms != nil {
		cp.kms.ShredDEK(tenantID)
	}
	_, err = cp.db.Exec(ctx, `UPDATE clusters SET state = 'PURGED', direct_port = 0, app_password_enc = '', wrapped_dek = '', updated_at = now() WHERE tenant_id = $1`, tenantID)
	return err
}

type BackupResponse struct {
	TenantID            string `json:"tenant_id"`
	BackupID            string `json:"backup_id"`
	ParentID            string `json:"parent_id,omitempty"`
	Type                string `json:"type"`
	R2Key               string `json:"r2_key"`
	Epoch               int64  `json:"epoch"`
	CipherBytes         int    `json:"cipher_bytes"`
	WALSegmentsArchived int    `json:"wal_segments_archived"`
}

type RestoreResponse struct {
	TenantID            string `json:"tenant_id"`
	TargetLSN           string `json:"target_lsn,omitempty"`
	RestoredBackups     int    `json:"restored_backups"`
	WALSegmentsReplayed int    `json:"wal_segments_replayed"`
	StagingHeadroomOK   bool   `json:"staging_headroom_ok"`
	AmcheckOK           bool   `json:"amcheck_ok"`
	VerifyQueryOutput   string `json:"verify_query_output,omitempty"`
}

// TriggerBackup executes an encrypted FULL or INCR backup + WAL spool drain for an active tenant.
func (cp *ControlPlane) TriggerBackup(ctx context.Context, accountID, tenantID, backupType string) (*BackupResponse, error) {
	if err := tenant.ValidateAccountID(accountID); err != nil {
		return nil, err
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, err
	}
	var owner, state string
	err := cp.db.QueryRow(ctx, `SELECT account_id, state FROM clusters WHERE tenant_id = $1`, tenantID).Scan(&owner, &state)
	if errors.Is(err, pgx.ErrNoRows) || state == "PURGED" {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if owner != accountID {
		return nil, ErrForbidden
	}
	if state != "ACTIVE" {
		return nil, fmt.Errorf("tenant %s is not ACTIVE (state=%s)", tenantID, state)
	}
	if cp.agent == nil {
		return nil, errors.New("cell agent is not configured")
	}
	s3c, bucket := cp.backupStore()
	if s3c == nil || bucket == "" {
		return nil, ErrBackupNotConfigured
	}

	bkpType := strings.ToUpper(strings.TrimSpace(backupType))
	if bkpType == "" {
		bkpType = "FULL"
	}
	if bkpType != "FULL" && bkpType != "INCR" && bkpType != "WAL" {
		return nil, fmt.Errorf("unsupported backup type %q (must be FULL, INCR, or WAL)", backupType)
	}
	return cp.backupTenantToR2(ctx, tenantID, bkpType)
}

func (cp *ControlPlane) backupTenantToR2(ctx context.Context, tenantID, bkpType string) (*BackupResponse, error) {
	s3c, bucket := cp.backupStore()
	if s3c == nil || bucket == "" {
		return nil, ErrBackupNotConfigured
	}

	var art *fencing.BackupArtifact
	if bkpType != "WAL" {
		var parentID, parentManifestPath string
		if bkpType == "INCR" {
			manifest, _, mErr := fencing.ReadManifestWithETag(ctx, s3c, bucket, tenantID)
			if mErr == nil && manifest != nil && len(manifest.Backups) > 0 {
				last := manifest.Backups[len(manifest.Backups)-1]
				parentID = last.BackupID
				if bkpBase, sErr := tenant.SafeSubpath(cp.agent.VolDir(), "backups", tenantID); sErr == nil {
					candidate := filepath.Join(bkpBase, last.BackupID, "backup_manifest")
					if _, stErr := os.Stat(candidate); stErr == nil {
						parentManifestPath = candidate
					}
				}
			}
			if parentID == "" || parentManifestPath == "" {
				bkpType = "FULL"
				parentID = ""
				parentManifestPath = ""
			}
		}

		bkpID := fmt.Sprintf("bkp_%d", time.Now().UnixNano())
		var err error
		art, _, err = backup.RunEncryptedBackupStep(ctx, cp.agent, s3c, bucket, tenantID, bkpID, parentID, bkpType, parentManifestPath)
		if err != nil {
			return nil, err
		}
	}

	walSegs, err := backup.DrainAndEncryptWALSpoolToR2(ctx, cp.agent, s3c, bucket, tenantID)
	if err != nil {
		return nil, err
	}
	if len(walSegs) > 0 {
		manifest, etag, mErr := fencing.ReadManifestWithETag(ctx, s3c, bucket, tenantID)
		if mErr == nil && manifest != nil {
			seen := make(map[string]bool, len(manifest.WALSegments))
			for _, k := range manifest.WALSegments {
				seen[k] = true
			}
			for _, w := range walSegs {
				if !seen[w.R2Key] {
					manifest.WALSegments = append(manifest.WALSegments, w.R2Key)
					seen[w.R2Key] = true
				}
			}
			if _, casErr := fencing.PutManifestCAS(ctx, s3c, bucket, manifest, etag); casErr != nil {
				return nil, casErr
			}
		}
	}

	resp := &BackupResponse{
		TenantID:            tenantID,
		Type:                bkpType,
		WALSegmentsArchived: len(walSegs),
	}
	if art != nil {
		resp.BackupID = art.BackupID
		resp.ParentID = art.ParentID
		resp.Type = art.Type
		resp.R2Key = art.R2Key
		resp.Epoch = art.Epoch
		resp.CipherBytes = art.CipherBytes
	}
	return resp, nil
}

// RestoreDatabase executes CombineAndReplayPITR for a tenant up to targetLSN (or latest WAL).
func (cp *ControlPlane) RestoreDatabase(ctx context.Context, accountID, tenantID, targetLSN, verifyQuery string) (*RestoreResponse, error) {
	if err := tenant.ValidateAccountID(accountID); err != nil {
		return nil, err
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, err
	}
	var owner, state string
	err := cp.db.QueryRow(ctx, `SELECT account_id, state FROM clusters WHERE tenant_id = $1`, tenantID).Scan(&owner, &state)
	if errors.Is(err, pgx.ErrNoRows) || state == "PURGED" {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if owner != accountID {
		return nil, ErrForbidden
	}
	if cp.agent == nil {
		return nil, errors.New("cell agent is not configured")
	}
	s3c, bucket := cp.backupStore()
	if s3c == nil || bucket == "" {
		return nil, ErrBackupNotConfigured
	}

	manifest, _, err := fencing.ReadManifestWithETag(ctx, s3c, bucket, tenantID)
	if err != nil {
		if fencing.IsNotFoundErr(err) {
			return nil, fmt.Errorf("%w: no backup manifest for tenant %s", ErrNotFound, tenantID)
		}
		return nil, err
	}
	if len(manifest.Backups) == 0 {
		return nil, fmt.Errorf("%w: manifest has no backups for tenant %s", ErrNotFound, tenantID)
	}

	// Build latest FULL -> INCR chain from manifest.Backups
	lastFullIdx := -1
	for i := len(manifest.Backups) - 1; i >= 0; i-- {
		if manifest.Backups[i].Type == "FULL" {
			lastFullIdx = i
			break
		}
	}
	if lastFullIdx < 0 {
		return nil, fmt.Errorf("no FULL backup found in manifest for tenant %s", tenantID)
	}
	var chain []*fencing.BackupArtifact
	first := manifest.Backups[lastFullIdx]
	chain = append(chain, &first)
	for i := lastFullIdx + 1; i < len(manifest.Backups); i++ {
		cand := manifest.Backups[i]
		if cand.Type == "INCR" && cand.ParentID == chain[len(chain)-1].BackupID {
			c := cand
			chain = append(chain, &c)
		}
	}

	var walSegs []backup.WALSegmentArtifact
	for _, r2Key := range manifest.WALSegments {
		segName := strings.TrimSuffix(filepath.Base(r2Key), ".zst.enc")
		walSegs = append(walSegs, backup.WALSegmentArtifact{
			SegmentName: segName,
			R2Key:       r2Key,
			Epoch:       chain[0].Epoch,
		})
	}

	if strings.TrimSpace(verifyQuery) == "" {
		verifyQuery = "SELECT 1;"
	}
	qOut, headroomOK, amcheckOK, err := backup.CombineAndReplayPITRWithOptions(
		ctx, cp.agent, s3c, bucket, tenantID, tenantID,
		chain, walSegs, targetLSN, verifyQuery, backup.TarExtractLimits{},
	)
	if err != nil {
		return nil, err
	}

	return &RestoreResponse{
		TenantID:            tenantID,
		TargetLSN:           targetLSN,
		RestoredBackups:     len(chain),
		WALSegmentsReplayed: len(walSegs),
		StagingHeadroomOK:   headroomOK,
		AmcheckOK:           amcheckOK,
		VerifyQueryOutput:   qOut,
	}, nil
}

// GetTenantRoute is used by the PostgreSQL Gateway to resolve active tenant backend port.
func (cp *ControlPlane) GetTenantRoute(ctx context.Context, tenantID string) (state string, directPort int, err error) {
	if err := tenant.ValidateID(tenantID); err != nil {
		return "", 0, err
	}
	err = cp.db.QueryRow(ctx, `SELECT state, direct_port FROM clusters WHERE tenant_id = $1`, tenantID).Scan(&state, &directPort)
	return state, directPort, err
}

// TouchTenantActivity updates the in-memory last activity timestamp for idle scale-to-zero tracking.
func (cp *ControlPlane) TouchTenantActivity(tenantID string) {
	if tenantID == "" {
		return
	}
	cp.activityMu.Lock()
	if cp.lastActivity == nil {
		cp.lastActivity = make(map[string]time.Time)
	}
	cp.lastActivity[tenantID] = time.Now()
	cp.activityMu.Unlock()
}

// GetTenantRouteAndCredentials resolves tenant state, backend endpoint, and decrypted app_user password.
func (cp *ControlPlane) GetTenantRouteAndCredentials(ctx context.Context, tenantID string) (state string, directPort int, sockDir string, appPassword string, err error) {
	if err := tenant.ValidateID(tenantID); err != nil {
		return "", 0, "", "", err
	}
	var owner, encB64, wrappedDEK string
	err = cp.db.QueryRow(ctx,
		`SELECT state, direct_port, account_id, app_password_enc, wrapped_dek FROM clusters WHERE tenant_id = $1`,
		tenantID,
	).Scan(&state, &directPort, &owner, &encB64, &wrappedDEK)
	if err != nil {
		return "", 0, "", "", err
	}
	if wrappedDEK != "" {
		_ = cp.kms.LoadWrappedDEK(tenantID, wrappedDEK)
	}
	raw, decErr := base64.StdEncoding.DecodeString(encB64)
	if decErr == nil {
		if pt, ptErr := cp.kms.DecryptObject(tenantID, 1, "app_password", raw); ptErr == nil {
			appPassword = string(pt)
		}
	}
	if cp.agent != nil {
		if rt, ok := cp.agent.GetTenant(tenantID); ok && rt != nil {
			sockDir = rt.SockDir
			if rt.Port > 0 {
				directPort = rt.Port
			}
		} else if sp, spErr := tenant.SafeSubpath(cp.agent.VolDir(), "tenants/active", tenantID); spErr == nil {
			sockDir = filepath.Join(sp, "sock")
		}
	}
	return state, directPort, sockDir, appPassword, nil
}

// WakeTenant coalesces concurrent wake requests for a SUSPENDED tenant via singleflight,
// starting the tenant's PostgreSQL 17 instance once and transitioning state back to ACTIVE.
func (cp *ControlPlane) WakeTenant(ctx context.Context, tenantID string) (directPort int, sockDir string, appPassword string, err error) {
	if err := tenant.ValidateID(tenantID); err != nil {
		return 0, "", "", err
	}
	if cp.agent == nil {
		return 0, "", "", errors.New("cell agent not configured")
	}
	v, err, _ := cp.wakeGroup.Do(tenantID, func() (any, error) {
		var curState string
		var curPort int
		var epoch int64
		if qErr := cp.db.QueryRow(ctx,
			`SELECT state, direct_port, fencing_epoch FROM clusters WHERE tenant_id = $1`,
			tenantID,
		).Scan(&curState, &curPort, &epoch); qErr != nil {
			return nil, qErr
		}
		if curState != "SUSPENDED" && curState != "ACTIVE" {
			return nil, fmt.Errorf("tenant %s cannot wake from state %s", tenantID, curState)
		}
		if !cp.agent.HasLocalPGData(tenantID) {
			s3c, bucket := cp.backupStore()
			if s3c == nil || bucket == "" {
				return nil, fmt.Errorf("tenant %s has no local pgdata and backup store is not configured", tenantID)
			}
			var wrappedDEK string
			if dErr := cp.db.QueryRow(ctx, `SELECT wrapped_dek FROM clusters WHERE tenant_id = $1`, tenantID).Scan(&wrappedDEK); dErr == nil && wrappedDEK != "" {
				_ = cp.kms.LoadWrappedDEK(tenantID, wrappedDEK)
			}
			if hErr := backup.HydrateTenantPGDataFromR2(ctx, cp.agent, s3c, bucket, tenantID); hErr != nil {
				return nil, fmt.Errorf("hydrate tenant %s from R2: %w", tenantID, hErr)
			}
		}
		if cp.getPageserver() != nil {
			_ = cp.agent.WriteTenantConfigs(tenantID)
		}
		rt, sErr := cp.agent.StartOrAdoptTenant(tenantID, epoch)
		if sErr != nil {
			return nil, sErr
		}
		if err := cp.ensurePageserverBootstrapAndWAL(ctx, tenantID, rt); err != nil {
			log.Printf("[pageserver] wake walreceiver for %s failed: %v", tenantID, err)
		}
		if _, uErr := cp.db.Exec(ctx,
			`UPDATE clusters SET state = 'ACTIVE', direct_port = $2, updated_at = NOW() WHERE tenant_id = $1`,
			tenantID, rt.Port,
		); uErr != nil {
			return nil, uErr
		}
		cp.TouchTenantActivity(tenantID)
		_, _, _, pw, credErr := cp.GetTenantRouteAndCredentials(ctx, tenantID)
		if credErr != nil {
			return nil, credErr
		}
		return struct {
			port     int
			sockDir  string
			password string
		}{port: rt.Port, sockDir: rt.SockDir, password: pw}, nil
	})
	if err != nil {
		return 0, "", "", err
	}
	res := v.(struct {
		port     int
		sockDir  string
		password string
	})
	return res.port, res.sockDir, res.password, nil
}

// SuspendTenant verifies fail-closed drain guards (including optional pooler drain and live PG17 checks)
// and transitions an ACTIVE tenant to SUSPENDED via clean pg_ctl stop -m fast.
func (cp *ControlPlane) SuspendTenant(ctx context.Context, tenantID string, drainTimeout time.Duration, poolDrainFn func(tenantID string, timeout time.Duration) error) error {
	if err := tenant.ValidateID(tenantID); err != nil {
		return err
	}
	if cp.agent == nil {
		return errors.New("cell agent not configured")
	}
	if drainTimeout <= 0 {
		drainTimeout = 3 * time.Second
	}

	var state string
	if err := cp.db.QueryRow(ctx, `SELECT state FROM clusters WHERE tenant_id = $1`, tenantID).Scan(&state); err != nil {
		return err
	}
	if state == "SUSPENDED" {
		return nil
	}
	if state != "ACTIVE" {
		return fmt.Errorf("%w: tenant %s is in state %s", cell.ErrDrainVetoed, tenantID, state)
	}

	if poolDrainFn != nil {
		if err := poolDrainFn(tenantID, drainTimeout); err != nil {
			return err
		}
	}

	drainCtx, cancel := context.WithTimeout(ctx, drainTimeout)
	defer cancel()
	if err := cp.agent.CheckTenantDrainSafety(drainCtx, tenantID); err != nil {
		return err
	}

	evictToR2 := false
	if cp.r2Primary {
		if s3c, bucket := cp.backupStore(); s3c != nil && bucket != "" {
			if _, bErr := cp.backupTenantToR2(ctx, tenantID, "INCR"); bErr != nil {
				return fmt.Errorf("sync tenant %s to R2 before suspend: %w", tenantID, bErr)
			}
			evictToR2 = true
		}
	}

		if err := cp.agent.StopTenantClean(tenantID); err != nil {
			return err
		}
		cp.stopWALReceiver(tenantID)
		if ps := cp.getPageserver(); ps != nil {
			if _, fErr := ps.FlushTenant(ctx, tenantID); fErr != nil {
				return fmt.Errorf("flush pageserver layers for %s: %w", tenantID, fErr)
			}
		}
	if evictToR2 {
		if eErr := cp.agent.EvictTenantLocalData(tenantID); eErr != nil {
			return fmt.Errorf("evict local pgdata for %s after R2 sync: %w", tenantID, eErr)
		}
	}

	_, err := cp.db.Exec(ctx,
		`UPDATE clusters SET state = 'SUSPENDED', updated_at = NOW() WHERE tenant_id = $1 AND state = 'ACTIVE'`,
		tenantID,
	)
	return err
}

// SuspendIdleTenants suspends any ACTIVE tenant whose last recorded activity exceeds idleTimeout.
func (cp *ControlPlane) SuspendIdleTenants(ctx context.Context, idleTimeout time.Duration, poolDrainFn func(tenantID string, timeout time.Duration) error) (int, error) {
	if idleTimeout <= 0 {
		return 0, nil
	}
	rows, err := cp.db.Query(ctx, `SELECT tenant_id FROM clusters WHERE state = 'ACTIVE'`)
	if err != nil {
		return 0, err
	}
	var activeIDs []string
	for rows.Next() {
		var tid string
		if scanErr := rows.Scan(&tid); scanErr == nil {
			activeIDs = append(activeIDs, tid)
		}
	}
	rows.Close()

	now := time.Now()
	suspended := 0
	for _, tid := range activeIDs {
		cp.activityMu.Lock()
		if cp.lastActivity == nil {
			cp.lastActivity = make(map[string]time.Time)
		}
		last, seen := cp.lastActivity[tid]
		if !seen {
			cp.lastActivity[tid] = now
			cp.activityMu.Unlock()
			continue
		}
		cp.activityMu.Unlock()

		if now.Sub(last) < idleTimeout {
			continue
		}
		if err := cp.SuspendTenant(ctx, tid, 2*time.Second, poolDrainFn); err == nil {
			suspended++
		}
	}
	return suspended, nil
}

// StartIdleReaper runs periodic cooperative scale-to-zero checks until ctx is cancelled.
func (cp *ControlPlane) StartIdleReaper(ctx context.Context, idleTimeout, interval time.Duration, poolDrainFn func(tenantID string, timeout time.Duration) error) {
	if idleTimeout <= 0 {
		return
	}
	if interval <= 0 {
		interval = idleTimeout / 2
		if interval < 50*time.Millisecond {
			interval = 50 * time.Millisecond
		}
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = cp.SuspendIdleTenants(ctx, idleTimeout, poolDrainFn)
			}
		}
	}()
}

func (cp *ControlPlane) decryptTenantPassword(ctx context.Context, accountID, tenantID string) (string, error) {
	var owner, encB64, wrappedDEK string
	err := cp.db.QueryRow(ctx,
		`SELECT account_id, app_password_enc, wrapped_dek FROM clusters WHERE tenant_id = $1`,
		tenantID,
	).Scan(&owner, &encB64, &wrappedDEK)
	if err != nil {
		return "", err
	}
	if owner != accountID {
		return "", ErrForbidden
	}
	if wrappedDEK != "" {
		_ = cp.kms.LoadWrappedDEK(tenantID, wrappedDEK)
	}
	raw, err := base64.StdEncoding.DecodeString(encB64)
	if err != nil {
		return "", err
	}
	pt, err := cp.kms.DecryptObject(tenantID, 1, "app_password", raw)
	if err != nil {
		return "", err
	}
	return string(pt), nil
}

// CreateAPIKey generates a new Bearer API key bound to accountID, storing only its SHA-256 hash.
func (cp *ControlPlane) CreateAPIKey(ctx context.Context, accountID, description string) (APIKeyInfo, error) {
	if err := tenant.ValidateID(accountID); err != nil {
		return APIKeyInfo{}, err
	}
	rawToken := "air_live_" + randomPassword(32)
	return cp.SeedAPIKey(ctx, accountID, rawToken, description)
}

// SeedAPIKey registers or updates a deterministic Bearer API key bound to accountID, storing only its SHA-256 hash.
func (cp *ControlPlane) SeedAPIKey(ctx context.Context, accountID, rawToken, description string) (APIKeyInfo, error) {
	if err := tenant.ValidateID(accountID); err != nil {
		return APIKeyInfo{}, err
	}
	rawToken = strings.TrimSpace(rawToken)
	if len(rawToken) < 16 {
		return APIKeyInfo{}, errors.New("API key token must be at least 16 characters")
	}
	sum := sha256.Sum256([]byte(rawToken))
	keyHash := hex.EncodeToString(sum[:])
	keyID := "key_" + keyHash[:16]

	_, err := cp.db.Exec(ctx, `
		INSERT INTO api_keys (key_id, key_sha256, account_id, description, revoked_at)
		VALUES ($1, $2, $3, $4, NULL)
		ON CONFLICT (key_sha256) DO UPDATE
		SET account_id = EXCLUDED.account_id,
		    description = EXCLUDED.description,
		    revoked_at = NULL
	`, keyID, keyHash, accountID, description)
	if err != nil {
		return APIKeyInfo{}, err
	}
	return APIKeyInfo{
		KeyID:       keyID,
		AccountID:   accountID,
		Token:       rawToken,
		Description: description,
	}, nil
}

// AuthenticateAPIKey verifies a raw Bearer token against non-revoked SHA-256 records in api_keys.
func (cp *ControlPlane) AuthenticateAPIKey(ctx context.Context, rawToken string) (accountID string, keyID string, err error) {
	rawToken = strings.TrimSpace(rawToken)
	if rawToken == "" {
		return "", "", ErrUnauthorized
	}
	sum := sha256.Sum256([]byte(rawToken))
	keyHash := hex.EncodeToString(sum[:])

	var revokedAt *time.Time
	err = cp.db.QueryRow(ctx,
		`SELECT key_id, account_id, revoked_at FROM api_keys WHERE key_sha256 = $1`,
		keyHash,
	).Scan(&keyID, &accountID, &revokedAt)
	if errors.Is(err, pgx.ErrNoRows) || revokedAt != nil {
		return "", "", ErrUnauthorized
	}
	if err != nil {
		return "", "", err
	}
	return accountID, keyID, nil
}

// RevokeAPIKey marks an API key as revoked if it belongs to accountID.
func (cp *ControlPlane) RevokeAPIKey(ctx context.Context, accountID, keyID string) error {
	if err := tenant.ValidateID(accountID); err != nil {
		return err
	}
	var owner string
	err := cp.db.QueryRow(ctx, `SELECT account_id FROM api_keys WHERE key_id = $1`, keyID).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if owner != accountID {
		return ErrForbidden
	}
	_, err = cp.db.Exec(ctx, `UPDATE api_keys SET revoked_at = now() WHERE key_id = $1`, keyID)
	return err
}

func (cp *ControlPlane) Close() {
	cp.stopAllWALReceivers()
	if cp.db != nil {
		cp.db.Close()
	}
	if !cp.externalMeta && cp.metaDir != "" {
		pgData := filepath.Join(cp.metaDir, "pgdata")
		_, _ = runPostgres(cp.pgBin, "pg_ctl", "-D", pgData, "-m", "fast", "-w", "-t", "10", "stop")
	}
}

func runPostgres(pgBin, binName string, args ...string) (string, error) {
	bin := filepath.Join(pgBin, binName)
	var out []byte
	var err error
	if os.Geteuid() == 0 {
		fullArgs := append([]string{"-u", "postgres", "--", bin}, args...)
		out, err = exec.Command("runuser", fullArgs...).CombinedOutput()
	} else {
		out, err = exec.Command(bin, args...).CombinedOutput()
	}
	return strings.TrimSpace(string(out)), err
}

func randomPassword(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)[:n]
}

func Itoa(n int) string {
	return strconv.Itoa(n)
}
