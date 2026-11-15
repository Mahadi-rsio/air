package cell

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/tenant"
	"github.com/jackc/pgx/v5"
)

type TenantRuntime struct {
	TenantID         string `json:"tenant_id"`
	Port             int    `json:"port"`
	PID              int    `json:"pid"`
	PGData           string `json:"pg_data"`
	SystemIdentifier string `json:"system_identifier"`
	FencingEpoch     int64  `json:"fencing_epoch"`
	State            string `json:"state"`
}

type agentPIDMeta struct {
	PID              int    `json:"pid"`
	StartTime        string `json:"start_time"`
	PGData           string `json:"pg_data"`
	SystemIdentifier string `json:"system_identifier"`
	FencingEpoch     int64  `json:"fencing_epoch"`
}

// Agent manages single-cell PostgreSQL 17 tenant lifecycles, flock ownership, and persistent storage.
type Agent struct {
	volDir       string
	pgBin        string
	kms          *crypto.EnvelopeKMS
	portSeq      atomic.Int32
	tenantMu     sync.RWMutex
	tenants      map[string]*TenantRuntime
	tenantPorts  map[string]int
	lockFDs      map[string]*os.File
	epochChecker func(tenantID string) (int64, error)
}

func NewAgent(volDir, pgBinDir string, kms *crypto.EnvelopeKMS) (*Agent, error) {
	if pgBinDir == "" {
		pgBinDir = "/usr/lib/postgresql/17/bin"
	}
	for _, sub := range []string{
		"tenants/claiming",
		"tenants/active",
		"tmp",
	} {
		if err := os.MkdirAll(filepath.Join(volDir, sub), 0o755); err != nil {
			return nil, err
		}
	}
	a := &Agent{
		volDir:      volDir,
		pgBin:       pgBinDir,
		kms:         kms,
		tenants:     make(map[string]*TenantRuntime),
		tenantPorts: make(map[string]int),
		lockFDs:     make(map[string]*os.File),
	}
	a.portSeq.Store(int32(18000 + (os.Getpid()%400)*20))
	a.ReadoptRunningTenants()
	return a, nil
}

func (a *Agent) SetEpochChecker(fn func(tenantID string) (int64, error)) {
	a.tenantMu.Lock()
	a.epochChecker = fn
	a.tenantMu.Unlock()
}

func (a *Agent) acquireFlockLocked(tenantID, activeBase string) error {
	if _, already := a.lockFDs[tenantID]; already {
		return nil
	}
	lockPath := filepath.Join(activeBase, "agent.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return fmt.Errorf("agent.lock flock busy for %s: %w", tenantID, err)
	}
	a.lockFDs[tenantID] = f
	return nil
}

func (a *Agent) releaseFlockLocked(tenantID string) {
	if f, ok := a.lockFDs[tenantID]; ok && f != nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
		delete(a.lockFDs, tenantID)
	}
}

func (a *Agent) allocOrReusePort(tenantID string) int {
	a.tenantMu.Lock()
	defer a.tenantMu.Unlock()
	if p, ok := a.tenantPorts[tenantID]; ok && p > 0 {
		return p
	}
	for i := 0; i < 200; i++ {
		p := int(a.portSeq.Add(1))
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
		if err == nil {
			_ = ln.Close()
			a.tenantPorts[tenantID] = p
			return p
		}
	}
	p := int(a.portSeq.Add(1))
	a.tenantPorts[tenantID] = p
	return p
}

// ClaimCluster initializes a fresh PG17 data directory under tenants/claiming/<tenantID>.
func (a *Agent) ClaimCluster(tenantID string) (string, error) {
	claimDir, err := tenant.SafeSubpath(a.volDir, "tenants/claiming", tenantID)
	if err != nil {
		return "", err
	}
	activeDir, err := tenant.SafeSubpath(a.volDir, "tenants/active", tenantID)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(filepath.Join(activeDir, "pgdata", "PG_VERSION")); err == nil {
		return "", fmt.Errorf("tenant %s already has active pgdata", tenantID)
	}
	_ = os.RemoveAll(claimDir)
	pgData := filepath.Join(claimDir, "pgdata")
	if err := os.MkdirAll(pgData, 0o700); err != nil {
		return "", err
	}
	if out, err := a.runPostgresCmd(
		filepath.Join(a.pgBin, "initdb"),
		"-D", pgData, "-U", "postgres", "--auth-local=trust", "--auth-host=scram-sha-256", "--data-checksums", "--no-instructions",
	); err != nil {
		return "", fmt.Errorf("initdb failed: %w (%s)", err, out)
	}
	sysID := a.readSystemIdentifier(pgData)
	if sysID == "" {
		return "", errors.New("failed to read system_identifier after initdb")
	}
	return sysID, nil
}

// ActivateCluster promotes claiming -> active, writes hardened config, starts PG17, and provisions app_user + tenant DB.
func (a *Agent) ActivateCluster(tenantID, appPassword string, epoch int64) (*TenantRuntime, error) {
	claimDir, err := tenant.SafeSubpath(a.volDir, "tenants/claiming", tenantID)
	if err != nil {
		return nil, err
	}
	activeBase, err := tenant.SafeSubpath(a.volDir, "tenants/active", tenantID)
	if err != nil {
		return nil, err
	}
	pgData := filepath.Join(activeBase, "pgdata")

	if _, err := os.Stat(filepath.Join(claimDir, "pgdata", "PG_VERSION")); err == nil {
		_ = os.RemoveAll(activeBase)
		if err := os.Rename(claimDir, activeBase); err != nil {
			return nil, fmt.Errorf("promote claiming -> active: %w", err)
		}
	}
	if _, err := os.Stat(filepath.Join(pgData, "PG_VERSION")); err != nil {
		return nil, fmt.Errorf("missing active pgdata for %s: %w", tenantID, err)
	}

	sockDir := filepath.Join(activeBase, "sock")
	if err := os.MkdirAll(sockDir, 0o755); err != nil {
		return nil, err
	}
	port := a.allocOrReusePort(tenantID)

	conf := fmt.Sprintf(`listen_addresses = '127.0.0.1'
port = %d
unix_socket_directories = '%s'
max_connections = 100
shared_buffers = 32MB
password_encryption = 'scram-sha-256'
ssl = off
logging_collector = off
shared_preload_libraries = 'supautils'
supautils.privileged_extensions = 'pg_stat_statements,pg_trgm'
supautils.privileged_extensions_superuser = 'postgres'
supautils.reserved_roles = 'postgres'
`, port, sockDir)
	if err := os.WriteFile(filepath.Join(pgData, "postgresql.conf"), []byte(conf), 0o600); err != nil {
		return nil, err
	}

	hba := `
local   all             postgres                                trust
host    all             all             127.0.0.1/32            scram-sha-256
host    replication     all             127.0.0.1/32            scram-sha-256
`
	if err := os.WriteFile(filepath.Join(pgData, "pg_hba.conf"), []byte(strings.TrimSpace(hba)+"\n"), 0o600); err != nil {
		return nil, err
	}
	_ = os.Chmod(pgData, 0o700)

	rt, err := a.StartOrAdoptTenant(tenantID, epoch)
	if err != nil {
		return nil, err
	}

	// Provision least-privilege app_user and tenant database via Unix socket as superuser postgres.
	connStr := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres", sockDir, rt.Port)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("connect unix socket for tenant bootstrap: %w", err)
	}
	defer conn.Close(ctx)

	escapedPW := strings.ReplaceAll(appPassword, "'", "''")
	quotedDB := pgx.Identifier{tenantID}.Sanitize()
	sqlStmts := []string{
		fmt.Sprintf(`DO $$ BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_user') THEN
				CREATE ROLE app_user WITH LOGIN PASSWORD '%s' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
			ELSE
				ALTER ROLE app_user WITH PASSWORD '%s';
			END IF;
		END $$;`, escapedPW, escapedPW),
	}
	for _, s := range sqlStmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return nil, fmt.Errorf("provision app_user: %w", err)
		}
	}

	var dbExists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`, tenantID).Scan(&dbExists); err != nil {
		return nil, err
	}
	if !dbExists {
		if _, err := conn.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %s OWNER app_user`, quotedDB)); err != nil {
			return nil, fmt.Errorf("create tenant db %s: %w", tenantID, err)
		}
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf(`REVOKE ALL ON DATABASE %s FROM PUBLIC`, quotedDB)); err != nil {
		return nil, err
	}
	return rt, nil
}

// StartOrAdoptTenant acquires flock, starts PG17 if not running (or verifies running identity), and records pidmeta.
func (a *Agent) StartOrAdoptTenant(tenantID string, epoch int64) (*TenantRuntime, error) {
	activeBase, err := tenant.SafeSubpath(a.volDir, "tenants/active", tenantID)
	if err != nil {
		return nil, err
	}
	pgData := filepath.Join(activeBase, "pgdata")
	sockDir := filepath.Join(activeBase, "sock")
	_ = os.MkdirAll(sockDir, 0o755)

	a.tenantMu.Lock()
	if err := a.acquireFlockLocked(tenantID, activeBase); err != nil {
		a.tenantMu.Unlock()
		return nil, err
	}
	a.tenantMu.Unlock()

	pid, port, pidPGData := readPostmasterDetails(pgData)
	if pid > 0 && pidPGData == pgData && verifyPostgresProcessIdentity(pid, pgData) == nil {
		if port == 0 {
			port = a.allocOrReusePort(tenantID)
		}
	} else {
		port = a.allocOrReusePort(tenantID)
		logFile := filepath.Join(activeBase, "postgresql.log")
		if out, err := a.runPostgresCmd(
			filepath.Join(a.pgBin, "pg_ctl"),
			"-D", pgData, "-l", logFile, "-w", "-t", "15", "start",
		); err != nil {
			a.tenantMu.Lock()
			a.releaseFlockLocked(tenantID)
			a.tenantMu.Unlock()
			return nil, fmt.Errorf("pg_ctl start failed: %w (%s)", err, out)
		}
		pid, port, _ = readPostmasterDetails(pgData)
	}

	if err := a.waitReady(sockDir, port, 10*time.Second); err != nil {
		return nil, fmt.Errorf("tenant %s readiness check failed: %w", tenantID, err)
	}

	sysID := a.readSystemIdentifier(pgData)
	startTime, _ := readProcStartTime(pid)
	_ = writeJSONAtomic(filepath.Join(activeBase, "agent.pidmeta"), agentPIDMeta{
		PID:              pid,
		StartTime:        startTime,
		PGData:           pgData,
		SystemIdentifier: sysID,
		FencingEpoch:     epoch,
	})

	rt := &TenantRuntime{
		TenantID:         tenantID,
		Port:             port,
		PID:              pid,
		PGData:           pgData,
		SystemIdentifier: sysID,
		FencingEpoch:     epoch,
		State:            "ACTIVE",
	}
	a.tenantMu.Lock()
	a.tenants[tenantID] = rt
	a.tenantPorts[tenantID] = port
	a.tenantMu.Unlock()
	return rt, nil
}

// RestartTenant performs a clean pg_ctl restart on an active tenant while preserving data and ownership.
func (a *Agent) RestartTenant(tenantID string) (*TenantRuntime, error) {
	activeBase, err := tenant.SafeSubpath(a.volDir, "tenants/active", tenantID)
	if err != nil {
		return nil, err
	}
	pgData := filepath.Join(activeBase, "pgdata")
	logFile := filepath.Join(activeBase, "postgresql.log")

	a.tenantMu.Lock()
	if err := a.acquireFlockLocked(tenantID, activeBase); err != nil {
		a.tenantMu.Unlock()
		return nil, err
	}
	existing := a.tenants[tenantID]
	epoch := int64(1)
	if existing != nil {
		epoch = existing.FencingEpoch
	}
	a.tenantMu.Unlock()

	if out, err := a.runPostgresCmd(
		filepath.Join(a.pgBin, "pg_ctl"),
		"-D", pgData, "-l", logFile, "-m", "fast", "-w", "-t", "15", "restart",
	); err != nil {
		return nil, fmt.Errorf("pg_ctl restart failed: %w (%s)", err, out)
	}
	return a.StartOrAdoptTenant(tenantID, epoch)
}

// ReadoptRunningTenants scans tenants/active/*, verifies flock, PID/startTime, PGDATA, sysID, and fencing epoch.
func (a *Agent) ReadoptRunningTenants() int {
	activeRoot := filepath.Join(a.volDir, "tenants/active")
	entries, err := os.ReadDir(activeRoot)
	if err != nil {
		return 0
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		tid := e.Name()
		if err := tenant.ValidateID(tid); err != nil {
			continue
		}
		activeBase, err := tenant.SafeSubpath(a.volDir, "tenants/active", tid)
		if err != nil {
			continue
		}
		pgData := filepath.Join(activeBase, "pgdata")
		sockDir := filepath.Join(activeBase, "sock")

		a.tenantMu.Lock()
		if err := a.acquireFlockLocked(tid, activeBase); err != nil {
			a.tenantMu.Unlock()
			log.Printf("[cell-agent] readopt skipped for %s: %v", tid, err)
			continue
		}
		a.tenantMu.Unlock()

		var meta agentPIDMeta
		if raw, err := os.ReadFile(filepath.Join(activeBase, "agent.pidmeta")); err == nil {
			_ = json.Unmarshal(raw, &meta)
		}

		pid, port, pidPGData := readPostmasterDetails(pgData)
		if pid <= 0 || pidPGData != pgData || verifyPostgresProcessIdentity(pid, pgData) != nil {
			a.tenantMu.Lock()
			a.releaseFlockLocked(tid)
			a.tenantMu.Unlock()
			continue
		}
		if meta.PID == pid && meta.StartTime != "" {
			if curST, stErr := readProcStartTime(pid); stErr != nil || curST != meta.StartTime {
				a.tenantMu.Lock()
				a.releaseFlockLocked(tid)
				a.tenantMu.Unlock()
				continue
			}
		}

		sysID := a.readSystemIdentifier(pgData)
		if meta.SystemIdentifier != "" && sysID != meta.SystemIdentifier {
			a.tenantMu.Lock()
			a.releaseFlockLocked(tid)
			a.tenantMu.Unlock()
			continue
		}

		a.tenantMu.RLock()
		checker := a.epochChecker
		a.tenantMu.RUnlock()
		if checker != nil && meta.FencingEpoch > 0 {
			authEpoch, err := checker(tid)
			if err != nil || authEpoch > meta.FencingEpoch {
				a.tenantMu.Lock()
				a.releaseFlockLocked(tid)
				a.tenantMu.Unlock()
				continue
			}
		}

		if err := a.waitReady(sockDir, port, 3*time.Second); err != nil {
			a.tenantMu.Lock()
			a.releaseFlockLocked(tid)
			a.tenantMu.Unlock()
			continue
		}

		rt := &TenantRuntime{
			TenantID:         tid,
			Port:             port,
			PID:              pid,
			PGData:           pgData,
			SystemIdentifier: sysID,
			FencingEpoch:     meta.FencingEpoch,
			State:            "ACTIVE",
		}
		a.tenantMu.Lock()
		a.tenants[tid] = rt
		a.tenantPorts[tid] = port
		a.tenantMu.Unlock()
		count++
	}
	return count
}

// StopTenantClean stops PG17 cleanly and verifies process exit before releasing flock, keeping data on disk.
func (a *Agent) StopTenantClean(tenantID string) error {
	activeBase, err := tenant.SafeSubpath(a.volDir, "tenants/active", tenantID)
	if err != nil {
		return err
	}
	pgData := filepath.Join(activeBase, "pgdata")
	pid, _, _ := readPostmasterDetails(pgData)
	_, _ = a.runPostgresCmd(filepath.Join(a.pgBin, "pg_ctl"), "-D", pgData, "-m", "fast", "-w", "-t", "10", "stop")
	if pid > 1 && !waitProcessGone(pid, 3*time.Second) {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		waitProcessGone(pid, 2*time.Second)
	}
	a.tenantMu.Lock()
	delete(a.tenants, tenantID)
	a.releaseFlockLocked(tenantID)
	a.tenantMu.Unlock()
	return nil
}

// StopAndPurgeTenant terminates the tenant PG17 process, verifies termination, releases flock, and deletes data.
func (a *Agent) StopAndPurgeTenant(tenantID string) error {
	claimDir, err := tenant.SafeSubpath(a.volDir, "tenants/claiming", tenantID)
	if err != nil {
		return err
	}
	activeBase, err := tenant.SafeSubpath(a.volDir, "tenants/active", tenantID)
	if err != nil {
		return err
	}
	pgData := filepath.Join(activeBase, "pgdata")
	pid, _, _ := readPostmasterDetails(pgData)
	if _, statErr := os.Stat(pgData); statErr == nil {
		_, _ = a.runPostgresCmd(filepath.Join(a.pgBin, "pg_ctl"), "-D", pgData, "-m", "immediate", "-w", "-t", "10", "stop")
		if pid > 1 && !waitProcessGone(pid, 3*time.Second) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
			if !waitProcessGone(pid, 2*time.Second) {
				return fmt.Errorf("process %d for tenant %s survived SIGKILL", pid, tenantID)
			}
		}
	}

	a.tenantMu.Lock()
	delete(a.tenants, tenantID)
	delete(a.tenantPorts, tenantID)
	a.releaseFlockLocked(tenantID)
	a.tenantMu.Unlock()

	if a.kms != nil {
		a.kms.ShredDEK(tenantID)
	}
	_ = os.RemoveAll(claimDir)
	return os.RemoveAll(activeBase)
}

func (a *Agent) GetTenant(tenantID string) (*TenantRuntime, bool) {
	a.tenantMu.RLock()
	defer a.tenantMu.RUnlock()
	rt, ok := a.tenants[tenantID]
	return rt, ok
}

// Close releases agent flock handles cleanly without destroying running tenant PostgreSQL processes
// so a restarted Cell Agent can readopt running tenants.
func (a *Agent) Close() {
	a.tenantMu.Lock()
	defer a.tenantMu.Unlock()
	for tid := range a.lockFDs {
		a.releaseFlockLocked(tid)
	}
}

func (a *Agent) waitReady(sockDir string, port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	connStr := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres", sockDir, port)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		conn, err := pgx.Connect(ctx, connStr)
		if err == nil {
			var one int
			qErr := conn.QueryRow(ctx, "SELECT 1").Scan(&one)
			_ = conn.Close(ctx)
			cancel()
			if qErr == nil && one == 1 {
				return nil
			}
		} else {
			cancel()
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for postgres on port %d", port)
}

func (a *Agent) readSystemIdentifier(pgData string) string {
	out, err := a.runPostgresCmd(filepath.Join(a.pgBin, "pg_controldata"), pgData)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "Database system identifier:") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				return strings.TrimSpace(parts[1])
			}
		}
	}
	return ""
}

func (a *Agent) runPostgresCmd(bin string, args ...string) (string, error) {
	var cmd *exec.Cmd
	if os.Geteuid() == 0 {
		fullArgs := append([]string{"-u", "postgres", "--", bin}, args...)
		cmd = exec.Command("runuser", fullArgs...)
	} else {
		cmd = exec.Command(bin, args...)
	}
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return strings.TrimSpace(out.String()), err
}

func writeJSONAtomic(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
