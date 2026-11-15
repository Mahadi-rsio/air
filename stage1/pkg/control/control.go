package control

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"air-stage1/pkg/cell"
	"air-stage1/pkg/crypto"

	"github.com/jackc/pgx/v5/pgxpool"
)

const pgBin = "/usr/lib/postgresql/17/bin"

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

type ControlPlane struct {
	metaDir  string
	metaPort int
	db       *pgxpool.Pool
	agent    *cell.Agent
	kms      *crypto.EnvelopeKMS
	keyMu    sync.Mutex
	keyLocks map[string]*sync.Mutex
}

// NewControlPlane initializes a dedicated PostgreSQL 17 Metadata DB on metaDir and connects via pgxpool.
func NewControlPlane(ctx context.Context, metaDir string, metaPort int, agent *cell.Agent, kms *crypto.EnvelopeKMS) (*ControlPlane, error) {
	_ = os.RemoveAll(metaDir)
	pgData := filepath.Join(metaDir, "pgdata")
	sockDir := filepath.Join(metaDir, "sock")
	if err := os.MkdirAll(sockDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(pgData, 0o700); err != nil {
		return nil, err
	}
	_ = exec.Command("chown", "-R", "postgres:postgres", metaDir).Run()

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

	if out, err := runPostgres(filepath.Join(pgBin, "pg_ctl"), "-D", pgData, "-l", filepath.Join(metaDir, "meta.log"), "-w", "-t", "20", "start"); err != nil {
		return nil, fmt.Errorf("control metadata start: %w (%s)", err, out)
	}

	dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable&pool_max_conns=50", metaPort)
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}

	schemaSQL := `
CREATE TABLE clusters (
	tenant_id TEXT PRIMARY KEY,
	system_identifier TEXT NOT NULL UNIQUE,
	state TEXT NOT NULL,
	epoch BIGINT NOT NULL DEFAULT 1,
	port INT NOT NULL DEFAULT 0,
	claim_source TEXT NOT NULL,
	app_password TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE idempotency_records (
	idempotency_key TEXT PRIMARY KEY,
	tenant_id TEXT NOT NULL,
	status TEXT NOT NULL,
	response_json TEXT,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE usage_ledger (
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

CREATE TABLE audit_events (
	id BIGSERIAL PRIMARY KEY,
	tenant_id TEXT NOT NULL,
	action TEXT NOT NULL,
	detail TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
`
	if _, err := db.Exec(ctx, schemaSQL); err != nil {
		return nil, fmt.Errorf("control schema init: %w", err)
	}

	return &ControlPlane{
		metaDir:  metaDir,
		metaPort: metaPort,
		db:       db,
		agent:    agent,
		kms:      kms,
		keyLocks: make(map[string]*sync.Mutex),
	}, nil
}

func (cp *ControlPlane) lockForKey(k string) *sync.Mutex {
	cp.keyMu.Lock()
	defer cp.keyMu.Unlock()
	m, ok := cp.keyLocks[k]
	if !ok {
		m = &sync.Mutex{}
		cp.keyLocks[k] = m
	}
	return m
}

// ProvisionDatabase executes idempotent two-phase cluster provisioning (Flow 1, ADR-001, EXP-A2).
func (cp *ControlPlane) ProvisionDatabase(ctx context.Context, idempotencyKey, tenantID string) (*ProvisionResponse, error) {
	if idempotencyKey == "" {
		idempotencyKey = "auto:" + tenantID
	}

	// Serialize concurrent identical Idempotency-Key calls so duplicates never double-provision
	kl := cp.lockForKey(idempotencyKey)
	kl.Lock()
	defer kl.Unlock()

	// 1. Check existing idempotency record
	var status, respJSON string
	err := cp.db.QueryRow(ctx, `SELECT status, COALESCE(response_json, '') FROM idempotency_records WHERE idempotency_key = $1`, idempotencyKey).Scan(&status, &respJSON)
	if err == nil && status == "COMPLETED" && respJSON != "" {
		var cached ProvisionResponse
		if jsonErr := json.Unmarshal([]byte(respJSON), &cached); jsonErr == nil {
			cached.IdempotentReplay = true
			return &cached, nil
		}
	}

	t0 := time.Now()
	_, _ = cp.db.Exec(ctx, `INSERT INTO idempotency_records (idempotency_key, tenant_id, status) VALUES ($1, $2, 'IN_PROGRESS') ON CONFLICT (idempotency_key) DO NOTHING`, idempotencyKey, tenantID)

	// 2. Claim cluster from Cell Agent (pre-warmed pool or bounded on-demand initdb fallback)
	claim, err := cp.agent.ClaimCluster(tenantID)
	if err != nil {
		return nil, err
	}

	passBytes := make([]byte, 16)
	_, _ = rand.Read(passBytes)
	appPassword := "pw_" + hex.EncodeToString(passBytes)

	if _, err := cp.kms.EnsureTenantDEK(tenantID); err != nil {
		cp.agent.RollbackClaim(tenantID)
		return nil, err
	}

	// 3. Commit UNIQUE(tenant_id) and UNIQUE(system_identifier) in Control Metadata DB BEFORE promoting directory!
	_, err = cp.db.Exec(ctx,
		`INSERT INTO clusters (tenant_id, system_identifier, state, epoch, port, claim_source, app_password)
		 VALUES ($1, $2, 'CLAIMING', 1, 0, $3, $4)`,
		tenantID, claim.SystemIdentifier, claim.Source, appPassword,
	)
	if err != nil {
		cp.agent.RollbackClaim(tenantID)
		return nil, fmt.Errorf("control db unique identity lock rejected claim: %w", err)
	}

	// 4. Activate cluster on Cell (SCRAM-SHA-256 + supautils + SELECT 1 readiness check + PgBouncer reload)
	rt, err := cp.agent.ActivateCluster(tenantID, appPassword, 1)
	if err != nil {
		_, _ = cp.db.Exec(ctx, `DELETE FROM clusters WHERE tenant_id = $1`, tenantID)
		cp.agent.RollbackClaim(tenantID)
		return nil, fmt.Errorf("activate cluster failed: %w", err)
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
	b, _ := json.Marshal(resp)

	_, _ = cp.db.Exec(ctx, `UPDATE clusters SET state = 'ACTIVE', port = $2 WHERE tenant_id = $1`, tenantID, rt.Port)
	_, _ = cp.db.Exec(ctx, `UPDATE idempotency_records SET status = 'COMPLETED', response_json = $2 WHERE idempotency_key = $1`, idempotencyKey, string(b))
	_, _ = cp.db.Exec(ctx, `INSERT INTO audit_events (tenant_id, action, detail) VALUES ($1, 'PROVISION', $2)`, tenantID, fmt.Sprintf("sysid=%s source=%s", claim.SystemIdentifier, claim.Source))

	return resp, nil
}

// GetTenantAuth fetches the tenant's state, epoch, Direct Port, PgBouncer addr, and SCRAM password for Gateway routing.
func (cp *ControlPlane) GetTenantAuth(ctx context.Context, tenantID string) (state string, epoch int64, directPort int, appPassword string, err error) {
	err = cp.db.QueryRow(ctx, `SELECT state, epoch, port, app_password FROM clusters WHERE tenant_id = $1`, tenantID).
		Scan(&state, &epoch, &directPort, &appPassword)
	return
}

// DeleteDatabase implements Flow 12: 7-day Soft Delete (revoking auth, stopping PG, keeping DEK) vs Immediate Hard Purge (crypto-shredding DEK).
func (cp *ControlPlane) DeleteDatabase(ctx context.Context, tenantID string, immediate bool) error {
	if !immediate {
		_, err := cp.db.Exec(ctx, `UPDATE clusters SET state = 'SOFT_DELETED', epoch = epoch + 1 WHERE tenant_id = $1`, tenantID)
		if err != nil {
			return err
		}
		_ = cp.agent.StopTenantOnly(tenantID)
		_, _ = cp.db.Exec(ctx, `INSERT INTO audit_events (tenant_id, action, detail) VALUES ($1, 'SOFT_DELETE', '7-day grace window started; auth revoked; DEK retained')`, tenantID)
		return nil
	}

	// Hard purge: instant GDPR crypto-shredding of tenant DEK prior to directory/R2 sweep (SI-4, Flow 12)
	cp.kms.ShredDEK(tenantID)
	_ = cp.agent.StopAndPurgeTenant(tenantID)
	_, err := cp.db.Exec(ctx, `UPDATE clusters SET state = 'PURGED', epoch = epoch + 1 WHERE tenant_id = $1`, tenantID)
	_, _ = cp.db.Exec(ctx, `INSERT INTO audit_events (tenant_id, action, detail) VALUES ($1, 'CRYPTO_SHRED_PURGE', 'tenant DEK destroyed; local PGDATA purged')`, tenantID)
	return err
}

// IngestUsageBatch inserts Gateway RU usage windows idempotently using ON CONFLICT (idempotency_key) DO NOTHING (FMEA-23).
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

// ReconcileOnce compares Control Metadata DB truth against live Cell directories and purges any orphans (EXP-A3).
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
