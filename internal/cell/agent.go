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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/tenant"
	"github.com/jackc/pgx/v5"
)

// ErrDrainVetoed is returned when cooperative drain/suspend is unsafe because of active transactions,
// 2PC prepared transactions, session advisory locks, session pins, or unknown backend state.
var ErrDrainVetoed = errors.New("drain vetoed")

type TenantRuntime struct {
	TenantID         string `json:"tenant_id"`
	Port             int    `json:"port"`
	PID              int    `json:"pid"`
	PGData           string `json:"pg_data"`
	SockDir          string `json:"sock_dir,omitempty"`
	WALSpoolDir      string `json:"wal_spool_dir,omitempty"`
	SystemIdentifier string `json:"system_identifier"`
	FencingEpoch     int64  `json:"fencing_epoch"`
	State            string `json:"state"`
	CgroupPath       string `json:"cgroup_path,omitempty"`
	Quarantined      bool   `json:"quarantined,omitempty"`
}

// CgroupLimits defines per-tenant cgroup v2 resource constraints.
type CgroupLimits struct {
	MemoryHigh string
	MemoryMax  string
	SwapMax    string
	CPUMax     string
	PIDsMax    string
}

// DefaultCgroupLimits returns default per-tenant cgroup v2 limits for PostgreSQL 17 cells.
func DefaultCgroupLimits() CgroupLimits {
	return CgroupLimits{
		MemoryHigh: "536870912",     // 512 MiB
		MemoryMax:  "1073741824",    // 1 GiB
		SwapMax:    "0",             // disable swap
		CPUMax:     "100000 100000", // 1.0 vCPU quota per 100ms period
		PIDsMax:    "256",
	}
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
	cgroupRoot   string
	cgroupLimits CgroupLimits
	portSeq      atomic.Int32
	tenantMu     sync.RWMutex
	tenants      map[string]*TenantRuntime
	tenantPorts  map[string]int
	lockFDs      map[string]*os.File
	epochChecker func(tenantID string) (int64, error)

	stopCh           chan struct{}
	stopOnce         sync.Once
	leaseMu          sync.Mutex
	lastLeaseAckMono time.Time
	watchdogFenced   atomic.Bool
	watchdogKillMs   atomic.Int64

	pageserverEnabled bool
	pageserverSocket  string
	lfcMaxMB          int
}

// SetPageserverConfig configures air_smgr GUC generation in tenant postgresql.conf files.
func (a *Agent) SetPageserverConfig(enabled bool, socketPath string, lfcMaxMB int) {
	a.tenantMu.Lock()
	defer a.tenantMu.Unlock()
	a.pageserverEnabled = enabled
	a.pageserverSocket = socketPath
	if lfcMaxMB < 0 {
		lfcMaxMB = 0
	}
	a.lfcMaxMB = lfcMaxMB
}

// IsPageserverEnabled reports whether air_smgr/Pageserver mode is enabled on the agent.
func (a *Agent) IsPageserverEnabled() bool {
	a.tenantMu.RLock()
	defer a.tenantMu.RUnlock()
	return a.pageserverEnabled && a.pageserverSocket != ""
}

// HasAirSmgr reports whether Pageserver mode is enabled and the air_smgr.so extension
// is installed alongside PostgreSQL on this host.
func (a *Agent) HasAirSmgr() bool {
	if !a.IsPageserverEnabled() {
		return false
	}
	candidates := []string{
		filepath.Clean(filepath.Join(a.pgBin, "..", "lib", "air_smgr.so")),
		"/usr/lib/postgresql/17/lib/air_smgr.so",
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return true
		}
	}
	return false
}

// TenantPGDataDir returns the validated active PGDATA directory path for tenantID.
func (a *Agent) TenantPGDataDir(tenantID string) (string, error) {
	activeBase, err := tenant.SafeSubpath(a.volDir, "tenants/active", tenantID)
	if err != nil {
		return "", err
	}
	return filepath.Join(activeBase, "pgdata"), nil
}

func NewAgent(volDir, pgBinDir string, kms *crypto.EnvelopeKMS) (*Agent, error) {
	if pgBinDir == "" {
		pgBinDir = "/usr/lib/postgresql/17/bin"
	}
	for _, sub := range []string{
		"tenants/claiming",
		"tenants/active",
		"spool/wal",
		"backups",
		"tmp",
	} {
		if err := os.MkdirAll(filepath.Join(volDir, sub), 0o755); err != nil {
			return nil, err
		}
	}
	a := &Agent{
		volDir:           volDir,
		pgBin:            pgBinDir,
		kms:              kms,
		cgroupRoot:       "/sys/fs/cgroup",
		cgroupLimits:     DefaultCgroupLimits(),
		tenants:          make(map[string]*TenantRuntime),
		tenantPorts:      make(map[string]int),
		lockFDs:          make(map[string]*os.File),
		stopCh:           make(chan struct{}),
		lastLeaseAckMono: time.Now(),
	}
	a.portSeq.Store(int32(18000 + (os.Getpid()%400)*20))
	a.ReadoptRunningTenants()
	return a, nil
}

// SetCgroupRoot overrides the cgroup v2 mount root (primarily for isolated unit testing).
func (a *Agent) SetCgroupRoot(root string) {
	a.tenantMu.Lock()
	a.cgroupRoot = root
	a.tenantMu.Unlock()
}

// SetCgroupLimits overrides the default per-tenant cgroup v2 resource limits.
func (a *Agent) SetCgroupLimits(limits CgroupLimits) {
	a.tenantMu.Lock()
	a.cgroupLimits = limits
	a.tenantMu.Unlock()
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
	portFree := func(p int) bool {
		for tid, assigned := range a.tenantPorts {
			if tid != tenantID && assigned == p {
				return false
			}
		}
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
		if err != nil {
			return false
		}
		_ = ln.Close()
		return true
	}
	if p, ok := a.tenantPorts[tenantID]; ok && p > 0 && portFree(p) {
		return p
	}
	for i := 0; i < 200; i++ {
		p := int(a.portSeq.Add(1))
		if portFree(p) {
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

	if err := a.WriteTenantConfigs(tenantID); err != nil {
		return nil, err
	}
	sockDir := filepath.Join(activeBase, "sock")

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
		`SET password_encryption = 'scram-sha-256'`,
		fmt.Sprintf(`DO $$ BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_user') THEN
				CREATE ROLE app_user WITH LOGIN PASSWORD '%s' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
			ELSE
				ALTER ROLE app_user WITH LOGIN PASSWORD '%s' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
			END IF;
		END $$;`, escapedPW, escapedPW),
		`REVOKE CONNECT ON DATABASE template1 FROM PUBLIC`,
		`REVOKE ALL ON DATABASE postgres FROM PUBLIC`,
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
	if _, err := conn.Exec(ctx, fmt.Sprintf(`GRANT ALL ON DATABASE %s TO app_user`, quotedDB)); err != nil {
		return nil, err
	}
	return rt, nil
}

// HasLocalPGData reports whether the tenant has a local pgdata directory cached on the cell volume.
func (a *Agent) HasLocalPGData(tenantID string) bool {
	activeBase, err := tenant.SafeSubpath(a.volDir, "tenants/active", tenantID)
	if err != nil {
		return false
	}
	_, err = os.Stat(filepath.Join(activeBase, "pgdata", "PG_VERSION"))
	return err == nil
}

// EvictTenantLocalData removes local pgdata, WAL spool, and local backup staging files for a suspended tenant
// after its durable state has been synced to R2.
func (a *Agent) EvictTenantLocalData(tenantID string) error {
	for _, prefix := range []string{"tenants/active", "spool/wal", "backups"} {
		dir, err := tenant.SafeSubpath(a.volDir, prefix, tenantID)
		if err != nil {
			return err
		}
		if err := os.RemoveAll(dir); err != nil {
			return err
		}
	}
	a.tenantMu.Lock()
	delete(a.tenants, tenantID)
	delete(a.tenantPorts, tenantID)
	a.tenantMu.Unlock()
	return nil
}

// WriteTenantConfigs writes standard postgresql.conf, pg_hba.conf, sockDir, and walSpoolDir for an active tenant.
func (a *Agent) WriteTenantConfigs(tenantID string) error {
	activeBase, err := tenant.SafeSubpath(a.volDir, "tenants/active", tenantID)
	if err != nil {
		return err
	}
	pgData := filepath.Join(activeBase, "pgdata")
	sockDir := filepath.Join(activeBase, "sock")
	if err := os.MkdirAll(sockDir, 0o755); err != nil {
		return err
	}
	walSpoolDir, err := tenant.SafeSubpath(a.volDir, "spool/wal", tenantID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(walSpoolDir, 0o700); err != nil {
		return err
	}
	port := a.allocOrReusePort(tenantID)

	a.tenantMu.RLock()
	psEnabled := a.pageserverEnabled
	psSocket := a.pageserverSocket
	lfcMaxMB := a.lfcMaxMB
	a.tenantMu.RUnlock()

	sharedLibs := "supautils"
	smgrConf := ""
	if psEnabled && psSocket != "" {
		lfcDir := filepath.Join(activeBase, "lfc")
		if err := os.MkdirAll(lfcDir, 0o700); err != nil {
			return err
		}
		sharedLibs = "supautils,air_smgr"
		smgrConf = fmt.Sprintf(
			"air_smgr.enabled = on\nair_smgr.pageserver_socket = '%s'\nair_smgr.tenant_id = '%s'\nair_smgr.lfc_dir = '%s'\nair_smgr.lfc_max_mb = %d\n",
			psSocket, tenantID, lfcDir, lfcMaxMB,
		)
	}

	conf := fmt.Sprintf(`listen_addresses = '127.0.0.1'
port = %d
unix_socket_directories = '%s'
max_connections = 100
max_prepared_transactions = 16
shared_buffers = 32MB
work_mem = 4MB
maintenance_work_mem = 16MB
temp_file_limit = 32MB
password_encryption = 'scram-sha-256'
wal_level = replica
summarize_wal = on
archive_mode = on
archive_command = 'cp %%p %s/%%f'
ssl = off
logging_collector = off
shared_preload_libraries = '%s'
supautils.superuser = 'postgres'
supautils.privileged_role = 'app_user'
supautils.privileged_extensions = 'pg_stat_statements,pg_trgm'
supautils.privileged_role_allowed_configs = 'session_replication_role'
supautils.reserved_roles = 'postgres'
supautils.reserved_memberships = 'pg_read_server_files,pg_write_server_files,pg_execute_server_program,postgres'
supautils.restrict_extension_versions = 'error'
%s`, port, sockDir, walSpoolDir, sharedLibs, smgrConf)
	if err := os.WriteFile(filepath.Join(pgData, "postgresql.conf"), []byte(conf), 0o600); err != nil {
		return err
	}

	hba := `
local   all             postgres                                trust
local   replication     postgres                                trust
local   all             app_user                                password
host    all             postgres        127.0.0.1/32            reject
host    all             all             127.0.0.1/32            scram-sha-256
host    replication     postgres        127.0.0.1/32            reject
host    replication     all             127.0.0.1/32            scram-sha-256
`
	if err := os.WriteFile(filepath.Join(pgData, "pg_hba.conf"), []byte(strings.TrimSpace(hba)+"\n"), 0o600); err != nil {
		return err
	}
	return os.Chmod(pgData, 0o700)
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
			"-D", pgData, "-l", logFile, "-w", "-t", "15",
			"-o", fmt.Sprintf("-p %d", port),
			"start",
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

	cgPath, _ := a.setupTenantCgroupV2(tenantID, pid)

	sysID := a.readSystemIdentifier(pgData)
	startTime, _ := readProcStartTime(pid)
	_ = writeJSONAtomic(filepath.Join(activeBase, "agent.pidmeta"), agentPIDMeta{
		PID:              pid,
		StartTime:        startTime,
		PGData:           pgData,
		SystemIdentifier: sysID,
		FencingEpoch:     epoch,
	})

	walSpoolDir, _ := tenant.SafeSubpath(a.volDir, "spool/wal", tenantID)
	rt := &TenantRuntime{
		TenantID:         tenantID,
		Port:             port,
		PID:              pid,
		PGData:           pgData,
		SockDir:          sockDir,
		WALSpoolDir:      walSpoolDir,
		SystemIdentifier: sysID,
		FencingEpoch:     epoch,
		State:            "ACTIVE",
		CgroupPath:       cgPath,
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

		cgPath, _ := a.setupTenantCgroupV2(tid, pid)
		walSpoolDir, _ := tenant.SafeSubpath(a.volDir, "spool/wal", tid)
		rt := &TenantRuntime{
			TenantID:         tid,
			Port:             port,
			PID:              pid,
			PGData:           pgData,
			SockDir:          sockDir,
			WALSpoolDir:      walSpoolDir,
			SystemIdentifier: sysID,
			FencingEpoch:     meta.FencingEpoch,
			State:            "ACTIVE",
			CgroupPath:       cgPath,
		}
		a.tenantMu.Lock()
		a.tenants[tid] = rt
		a.tenantPorts[tid] = port
		a.tenantMu.Unlock()
		count++
	}
	return count
}

// StopTenantClean stops PG17 cleanly and verifies process tree exit + cgroup teardown before releasing flock, keeping data on disk.
func (a *Agent) StopTenantClean(tenantID string) error {
	activeBase, err := tenant.SafeSubpath(a.volDir, "tenants/active", tenantID)
	if err != nil {
		return err
	}
	pgData := filepath.Join(activeBase, "pgdata")
	pid, _, _ := readPostmasterDetails(pgData)
	_, _ = a.runPostgresCmd(filepath.Join(a.pgBin, "pg_ctl"), "-D", pgData, "-m", "fast", "-w", "-t", "10", "stop")
	if kErr := a.killAndRemoveTenantCgroupV2(tenantID, pid); kErr != nil {
		return kErr
	}
	a.tenantMu.Lock()
	delete(a.tenants, tenantID)
	a.releaseFlockLocked(tenantID)
	a.tenantMu.Unlock()
	return nil
}

// StopAndPurgeTenant terminates the tenant PG17 process tree and cgroup, verifies termination, releases flock, and deletes data.
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
	}
	if kErr := a.killAndRemoveTenantCgroupV2(tenantID, pid); kErr != nil {
		return kErr
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
	a.stopOnce.Do(func() { close(a.stopCh) })
	a.tenantMu.Lock()
	defer a.tenantMu.Unlock()
	for tid := range a.lockFDs {
		a.releaseFlockLocked(tid)
	}
}

func (a *Agent) VolDir() string          { return a.volDir }
func (a *Agent) PGBin() string           { return a.pgBin }
func (a *Agent) KMS() *crypto.EnvelopeKMS { return a.kms }

func (a *Agent) RunPostgresCmd(bin string, args ...string) (string, error) {
	return a.runPostgresCmd(bin, args...)
}

// RenewLeaseAck updates the monotonic lease acknowledgement timestamp.
func (a *Agent) RenewLeaseAck() {
	a.leaseMu.Lock()
	a.lastLeaseAckMono = time.Now()
	a.leaseMu.Unlock()
}

// WatchdogStatus returns whether the monotonic lease watchdog has self-fenced and when it fired.
func (a *Agent) WatchdogStatus() (fenced bool, killElapsedMs int64) {
	return a.watchdogFenced.Load(), a.watchdogKillMs.Load()
}

// FenceAllLocalPrimaries immediately terminates all local tenant PostgreSQL primaries and marks them quarantined.
func (a *Agent) FenceAllLocalPrimaries() {
	a.tenantMu.Lock()
	defer a.tenantMu.Unlock()
	for id, rt := range a.tenants {
		if rt.PID > 0 {
			_ = syscall.Kill(rt.PID, syscall.SIGKILL)
			_ = waitProcessGone(rt.PID, 2*time.Second)
			rt.PID = 0
		}
		rt.Quarantined = true
		rt.State = "FENCED"
		a.releaseFlockLocked(id)
	}
}

// StartLeaseWatchdog runs a high-frequency CLOCK_MONOTONIC timer loop inside the Cell Agent.
// If time.Since(lastLeaseAckMono) >= selfFenceDeadline, it immediately self-fences all local primaries.
func (a *Agent) StartLeaseWatchdog(ctx context.Context, selfFenceDeadline time.Duration) {
	a.RenewLeaseAck()
	tStart := time.Now()
	go func() {
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-a.stopCh:
				return
			case <-ticker.C:
				a.leaseMu.Lock()
				elapsed := time.Since(a.lastLeaseAckMono)
				a.leaseMu.Unlock()
				if elapsed >= selfFenceDeadline && !a.watchdogFenced.Load() {
					a.watchdogFenced.Store(true)
					a.watchdogKillMs.Store(time.Since(tStart).Milliseconds())
					a.FenceAllLocalPrimaries()
					return
				}
			}
		}
	}()
}

// VerifyWALSummaryContinuity queries pg_available_wal_summaries() on the live cluster to confirm
// PG17 WAL summaries are present before running pg_basebackup --incremental.
func (a *Agent) VerifyWALSummaryContinuity(ctx context.Context, tenantID string) (int, error) {
	rt, ok := a.GetTenant(tenantID)
	if !ok {
		return 0, fmt.Errorf("tenant %s not active", tenantID)
	}
	connStr := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres", rt.SockDir, rt.Port)
	if rt.SockDir == "" {
		connStr = fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", rt.Port)
	}
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)

	_, _ = conn.Exec(ctx, "CHECKPOINT;")
	var summaryCount int
	for attempt := 0; attempt < 20; attempt++ {
		_ = conn.QueryRow(ctx, "SELECT count(*) FROM pg_available_wal_summaries();").Scan(&summaryCount)
		if summaryCount > 0 {
			return summaryCount, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return summaryCount, nil
}

// VerifyCombineStagingHeadroom enforces the >= 2.2x free staging space requirement before pg_combinebackup.
func (a *Agent) VerifyCombineStagingHeadroom(estimatedClusterBytes uint64) (freeBytes uint64, requiredBytes uint64, ok bool, err error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(a.volDir, &stat); err != nil {
		return 0, 0, false, err
	}
	freeBytes = stat.Bavail * uint64(stat.Bsize)
	requiredBytes = uint64(float64(estimatedClusterBytes) * 2.2)
	return freeBytes, requiredBytes, freeBytes >= requiredBytes, nil
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

// setupTenantCgroupV2 creates/configures air_tenant_<id> under cgroupRoot and attaches pid (+ descendants).
// If cgroupfs is not writable/delegated, it logs clearly and degrades gracefully without failing startup.
func (a *Agent) setupTenantCgroupV2(tenantID string, pid int) (string, bool) {
	if pid <= 1 || tenant.ValidateID(tenantID) != nil {
		return "", false
	}
	a.tenantMu.RLock()
	root := a.cgroupRoot
	limits := a.cgroupLimits
	a.tenantMu.RUnlock()
	if root == "" {
		return "", false
	}

	cgDir := filepath.Join(root, "air_tenant_"+tenantID)
	if err := os.MkdirAll(cgDir, 0o755); err != nil {
		log.Printf("[cell-agent] cgroup v2 unavailable for %s (%s): %v (degrading gracefully)", tenantID, cgDir, err)
		return "", false
	}

	limitFiles := []struct {
		name string
		val  string
	}{
		{"memory.high", limits.MemoryHigh},
		{"memory.max", limits.MemoryMax},
		{"memory.swap.max", limits.SwapMax},
		{"cpu.max", limits.CPUMax},
		{"pids.max", limits.PIDsMax},
	}
	for _, lf := range limitFiles {
		if lf.val == "" {
			continue
		}
		p := filepath.Join(cgDir, lf.name)
		if err := os.WriteFile(p, []byte(lf.val), 0o644); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("[cell-agent] cgroup v2 limit %s=%q failed for %s: %v", lf.name, lf.val, tenantID, err)
		}
	}

	pids := collectProcessTree(pid)
	if len(pids) == 0 {
		pids = []int{pid}
	}
	procsPath := filepath.Join(cgDir, "cgroup.procs")
	attachedRoot := false
	for _, p := range pids {
		if err := os.WriteFile(procsPath, []byte(strconv.Itoa(p)), 0o644); err == nil {
			if p == pid {
				attachedRoot = true
			}
		} else if p == pid {
			log.Printf("[cell-agent] cgroup v2 attach pid %d failed for %s: %v (degrading gracefully)", pid, tenantID, err)
		}
	}
	if !attachedRoot {
		return "", false
	}
	return cgDir, true
}

// killAndRemoveTenantCgroupV2 enforces atomic cgroup.kill + recursive /proc tree SIGKILL + waitForPIDsExit,
// then removes the tenant cgroup slice directory.
func (a *Agent) killAndRemoveTenantCgroupV2(tenantID string, rootPID int) error {
	if tenant.ValidateID(tenantID) != nil {
		return nil
	}
	a.tenantMu.RLock()
	root := a.cgroupRoot
	a.tenantMu.RUnlock()

	seen := make(map[int]bool)
	var allPIDs []int
	addPID := func(p int) {
		if p > 1 && !seen[p] {
			seen[p] = true
			allPIDs = append(allPIDs, p)
		}
	}

	treePIDs := collectProcessTree(rootPID)
	for _, p := range treePIDs {
		addPID(p)
	}

	var cgDir string
	cgExists := false
	if root != "" {
		cgDir = filepath.Join(root, "air_tenant_"+tenantID)
		if _, err := os.Stat(cgDir); err == nil {
			cgExists = true
			_ = os.WriteFile(filepath.Join(cgDir, "cgroup.kill"), []byte("1"), 0o644)
			if data, err := os.ReadFile(filepath.Join(cgDir, "cgroup.procs")); err == nil {
				for _, field := range strings.Fields(string(data)) {
					if p, err := strconv.Atoi(field); err == nil && p > 1 {
						addPID(p)
						_ = syscall.Kill(p, syscall.SIGKILL)
					}
				}
			}
		}
	}

	// Signal descendants first, then rootPID (also covers degraded non-cgroup mode).
	for i := len(treePIDs) - 1; i >= 0; i-- {
		if isProcessAlive(treePIDs[i]) {
			_ = syscall.Kill(treePIDs[i], syscall.SIGKILL)
		}
	}

	if len(allPIDs) > 0 && !waitForPIDsExit(allPIDs, 3*time.Second) {
		return fmt.Errorf("tenant %s process tree %v survived SIGKILL", tenantID, allPIDs)
	}

	if cgExists {
		for i := 0; i < 10; i++ {
			err := os.Remove(cgDir)
			if err == nil || errors.Is(err, os.ErrNotExist) {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	return nil
}

// CheckTenantDrainSafety verifies fail-closed that a tenant has no open 2PC transactions,
// session advisory locks, or non-idle client backends before cooperative scale-to-zero.
func (a *Agent) CheckTenantDrainSafety(ctx context.Context, tenantID string) error {
	if err := tenant.ValidateID(tenantID); err != nil {
		return err
	}
	a.tenantMu.RLock()
	rt, ok := a.tenants[tenantID]
	a.tenantMu.RUnlock()
	if !ok || rt == nil || rt.Port <= 0 {
		return fmt.Errorf("%w: tenant %s runtime unavailable", ErrDrainVetoed, tenantID)
	}

	dsn := fmt.Sprintf("postgres://postgres@localhost:%d/%s?host=%s", rt.Port, tenantID, rt.SockDir)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("%w: backend state unknown: %v", ErrDrainVetoed, err)
	}
	defer func() { _ = conn.Close(ctx) }()

	var prepCount, advCount, busyCount int
	err = conn.QueryRow(ctx, `
		SELECT
			(SELECT count(*)::int FROM pg_prepared_xacts),
			(SELECT count(*)::int FROM pg_locks WHERE locktype = 'advisory' AND granted),
			(SELECT count(*)::int FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND backend_type = 'client backend' AND state <> 'idle')
	`).Scan(&prepCount, &advCount, &busyCount)
	if err != nil {
		return fmt.Errorf("%w: backend state query failed: %v", ErrDrainVetoed, err)
	}
	if prepCount > 0 || advCount > 0 || busyCount > 0 {
		return fmt.Errorf("%w: prepared_xacts=%d advisory_locks=%d active_clients=%d", ErrDrainVetoed, prepCount, advCount, busyCount)
	}
	return nil
}
