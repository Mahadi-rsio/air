package cell

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"air-stage2/pkg/crypto"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const pgBin = "/usr/lib/postgresql/17/bin"

type TenantRuntime struct {
	TenantID         string `json:"tenant_id"`
	SystemIdentifier string `json:"system_identifier"`
	Port             int    `json:"port"`
	PID              int    `json:"pid"`
	Epoch            int64  `json:"epoch"`
	Readopted        bool   `json:"readopted"`
	DataDir          string `json:"data_dir"`
	SockDir          string `json:"sock_dir"`
}

type Agent struct {
	volDir         string
	targetPoolSize int
	poolMu         sync.Mutex
	tenantMu       sync.Mutex
	tenants        map[string]*TenantRuntime
	lockFDs        map[string]*os.File
	fallbackSem    chan struct{}
	refillWake     chan struct{}
	stopCh         chan struct{}
	poolSeq        atomic.Int64
	portSeq        atomic.Int32
	readoptedCount atomic.Int64
	dualStartBlock atomic.Int64
	pgbouncerPort  int
	pgbouncerMu    sync.Mutex
	kms            *crypto.EnvelopeKMS
}

func NewAgent(volDir string, targetPoolSize int, kms *crypto.EnvelopeKMS) (*Agent, error) {
	// 1. Set PR_SET_CHILD_SUBREAPER (36) per ADR-006
	_, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, 36, 1, 0, 0, 0, 0)
	if errno != 0 {
		log.Printf("[cell-agent] warning: PR_SET_CHILD_SUBREAPER errno=%v", errno)
	}

	for _, sub := range []string{
		"pool/staging",
		"pool/ready",
		"tenants/claiming",
		"tenants/active",
		"backups",
		"pgbouncer",
		"tmp",
	} {
		p := filepath.Join(volDir, sub)
		if err := os.MkdirAll(p, 0o755); err != nil {
			return nil, err
		}
	}
	_ = exec.Command("chown", "-R", "postgres:postgres", volDir).Run()

	// 2. Pre-allocate 64MB .cell_ballast emergency space reserve (ADR-010)
	ballastPath := filepath.Join(volDir, ".cell_ballast")
	if _, err := os.Stat(ballastPath); err != nil {
		if f, err := os.Create(ballastPath); err == nil {
			_ = syscall.Fallocate(int(f.Fd()), 0, 0, 64*1024*1024)
			_ = f.Close()
		}
	}

	a := &Agent{
		volDir:         volDir,
		targetPoolSize: targetPoolSize,
		tenants:        make(map[string]*TenantRuntime),
		lockFDs:        make(map[string]*os.File),
		fallbackSem:    make(chan struct{}, 4), // Max 4 concurrent on-demand initdb builds
		refillWake:     make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
		pgbouncerPort:  6432,
		kms:            kms,
	}
	a.portSeq.Store(16000)

	// 3. Boot-time purge of interrupted /vol/pool/staging/* (LI-1, EXP-A3)
	a.PurgeInterruptedStaging()

	// 4. Re-adopt live running tenant postgres clusters via postmaster.pid + /proc (ADR-006, EXP-A3)
	a.ReadoptRunningTenants()

	return a, nil
}

// PurgeInterruptedStaging deletes any half-initialized initdb directories in /vol/pool/staging.
func (a *Agent) PurgeInterruptedStaging() int {
	stagingRoot := filepath.Join(a.volDir, "pool/staging")
	entries, err := os.ReadDir(stagingRoot)
	if err != nil {
		return 0
	}
	purged := 0
	for _, e := range entries {
		_ = os.RemoveAll(filepath.Join(stagingRoot, e.Name()))
		purged++
	}
	return purged
}

// ReadoptRunningTenants scans /vol/tenants/active/* on startup and seamlessly re-adopts live postgres processes.
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
		tenantID := e.Name()
		tDir := filepath.Join(activeRoot, tenantID)
		pgData := filepath.Join(tDir, "pgdata")
		sockDir := filepath.Join(tDir, "sock")
		pid, port := readPostmasterPIDAndPort(pgData)
		if pid > 0 && isPostgresProcessAlive(pid) {
			a.tenantMu.Lock()
			_ = a.acquireFlockLocked(tenantID, filepath.Join(tDir, "agent.lock"))
			sysID := extractSystemIdentifier(pgData)
			if port >= int(a.portSeq.Load()) {
				a.portSeq.Store(int32(port + 1))
			}
			a.tenants[tenantID] = &TenantRuntime{
				TenantID:         tenantID,
				SystemIdentifier: sysID,
				Port:             port,
				PID:              pid,
				Epoch:            1,
				Readopted:        true,
				DataDir:          pgData,
				SockDir:          sockDir,
			}
			a.tenantMu.Unlock()
			a.readoptedCount.Add(1)
			count++
		}
	}
	if count > 0 {
		_ = a.reloadPgBouncer()
	}
	return count
}

func readPostmasterPIDAndPort(pgData string) (int, int) {
	b, err := os.ReadFile(filepath.Join(pgData, "postmaster.pid"))
	if err != nil {
		return 0, 0
	}
	lines := strings.Split(string(b), "\n")
	if len(lines) < 1 {
		return 0, 0
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(lines[0]))
	port := 0
	if len(lines) >= 4 {
		port, _ = strconv.Atoi(strings.TrimSpace(lines[3]))
	}
	return pid, port
}

func isPostgresProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	// Reap any terminated subreaper child processes first so zombies don't linger
	for {
		reaped, _ := syscall.Wait4(-1, nil, syscall.WNOHANG, nil)
		if reaped <= 0 {
			break
		}
	}
	statBytes, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	s := string(statBytes)
	if idx := strings.LastIndex(s, ")"); idx != -1 && idx+2 < len(s) {
		fields := strings.Fields(s[idx+2:])
		if len(fields) > 0 && (fields[0] == "Z" || fields[0] == "X") {
			return false
		}
	}
	commBytes, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
	if err != nil {
		return false
	}
	comm := strings.TrimSpace(string(commBytes))
	return strings.Contains(comm, "postgres")
}

func (a *Agent) acquireFlockLocked(tenantID, lockPath string) error {
	if _, exists := a.lockFDs[tenantID]; exists {
		return nil
	}
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
	if f, ok := a.lockFDs[tenantID]; ok {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
		delete(a.lockFDs, tenantID)
	}
}

// PrewarmPool populates /vol/pool/ready synchronously up to targetPoolSize.
func (a *Agent) PrewarmPool(count int) error {
	for i := 0; i < count; i++ {
		if err := a.createOnePoolSlot(); err != nil {
			return err
		}
	}
	go a.backgroundRefillLoop()
	return nil
}

func (a *Agent) backgroundRefillLoop() {
	for {
		select {
		case <-a.stopCh:
			return
		case <-a.refillWake:
			for a.ReadyPoolCount() < a.targetPoolSize {
				if err := a.createOnePoolSlot(); err != nil {
					break
				}
			}
		}
	}
}

func (a *Agent) ReadyPoolCount() int {
	entries, err := os.ReadDir(filepath.Join(a.volDir, "pool/ready"))
	if err != nil {
		return 0
	}
	return len(entries)
}

func (a *Agent) createOnePoolSlot() error {
	seq := a.poolSeq.Add(1)
	slotName := fmt.Sprintf("slot_%d_%d", time.Now().UnixNano(), seq)
	stageDir := filepath.Join(a.volDir, "pool/staging", slotName)
	readyDir := filepath.Join(a.volDir, "pool/ready", slotName)

	if err := os.MkdirAll(stageDir, 0o700); err != nil {
		return err
	}
	_ = exec.Command("chown", "-R", "postgres:postgres", stageDir).Run()

	// Two-phase initdb with nice -n 19 and --data-checksums (ADR-001)
	out, err := runPostgresCmd("nice", "-n", "19", filepath.Join(pgBin, "initdb"),
		"-D", stageDir, "-U", "postgres", "--data-checksums", "--auth=trust", "--no-instructions")
	if err != nil {
		_ = os.RemoveAll(stageDir)
		return fmt.Errorf("initdb failed: %w (%s)", err, out)
	}

	// Verify pg_controldata == "shut down" before promoting to ready (LI-1)
	ctrlOut, err := runPostgresCmd(filepath.Join(pgBin, "pg_controldata"), stageDir)
	if err != nil || !strings.Contains(ctrlOut, "shut down") {
		_ = os.RemoveAll(stageDir)
		return fmt.Errorf("pg_controldata not shut down: %v", err)
	}

	if d, err := os.Open(stageDir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}

	a.poolMu.Lock()
	err = os.Rename(stageDir, readyDir)
	a.poolMu.Unlock()
	if err != nil {
		_ = os.RemoveAll(stageDir)
		return err
	}
	return nil
}

type ClaimResult struct {
	TenantID         string  `json:"tenant_id"`
	SystemIdentifier string  `json:"system_identifier"`
	Source           string  `json:"source"` // "pool" or "on_demand"
	ClaimLatencyMs   float64 `json:"claim_latency_ms"`
}

// ClaimCluster pops a pre-warmed PGDATA from /vol/pool/ready (or falls back to bounded on-demand initdb).
func (a *Agent) ClaimCluster(tenantID string) (*ClaimResult, error) {
	claimDir := filepath.Join(a.volDir, "tenants/claiming", tenantID)
	_ = os.RemoveAll(claimDir)

	t0 := time.Now()
	a.poolMu.Lock()
	entries, _ := os.ReadDir(filepath.Join(a.volDir, "pool/ready"))
	var popped string
	if len(entries) > 0 {
		popped = filepath.Join(a.volDir, "pool/ready", entries[0].Name())
		if err := os.Rename(popped, claimDir); err != nil {
			popped = ""
		}
	}
	a.poolMu.Unlock()

	// Trigger async pool refill
	select {
	case a.refillWake <- struct{}{}:
	default:
	}

	source := "pool"
	if popped == "" {
		source = "on_demand"
		a.fallbackSem <- struct{}{}
		defer func() { <-a.fallbackSem }()

		if err := os.MkdirAll(claimDir, 0o700); err != nil {
			return nil, err
		}
		_ = exec.Command("chown", "-R", "postgres:postgres", claimDir).Run()
		// Sleep 2ms before initdb to guarantee distinct timestamp+PID entropy under tight concurrency
		time.Sleep(2 * time.Millisecond)
		out, err := runPostgresCmd(filepath.Join(pgBin, "initdb"),
			"-D", claimDir, "-U", "postgres", "--data-checksums", "--auth=trust", "--no-instructions")
		if err != nil {
			_ = os.RemoveAll(claimDir)
			return nil, fmt.Errorf("on-demand initdb failed: %w (%s)", err, out)
		}
	}

	claimMs := float64(time.Since(t0).Microseconds()) / 1000.0
	sysID := extractSystemIdentifier(claimDir)
	if sysID == "" {
		_ = os.RemoveAll(claimDir)
		return nil, errors.New("failed to read Database system identifier from claimed PGDATA")
	}

	return &ClaimResult{
		TenantID:         tenantID,
		SystemIdentifier: sysID,
		Source:           source,
		ClaimLatencyMs:   claimMs,
	}, nil
}

// ActivateCluster promotes /vol/tenants/claiming/{id} -> /vol/tenants/active/{id}/pgdata,
// configures SCRAM-SHA-256 + supautils, starts PG17, and verifies SELECT 1 readiness.
func (a *Agent) ActivateCluster(tenantID, appPassword string, epoch int64) (*TenantRuntime, error) {
	claimDir := filepath.Join(a.volDir, "tenants/claiming", tenantID)
	activeBase := filepath.Join(a.volDir, "tenants/active", tenantID)
	pgData := filepath.Join(activeBase, "pgdata")
	sockDir := filepath.Join(activeBase, "sock")

	if err := os.MkdirAll(sockDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.Rename(claimDir, pgData); err != nil {
		return nil, fmt.Errorf("rename claim->active failed: %w", err)
	}

	port := int(a.portSeq.Add(1))
	conf := fmt.Sprintf(`
listen_addresses = '127.0.0.1'
port = %d
unix_socket_directories = '%s'
max_connections = 20
shared_buffers = '8MB'
work_mem = '2MB'
maintenance_work_mem = '16MB'
max_parallel_workers_per_gather = 0
password_encryption = 'scram-sha-256'
wal_level = replica
summarize_wal = on
max_wal_senders = 4
max_replication_slots = 4
shared_preload_libraries = 'supautils'
supautils.privileged_extensions = 'pg_stat_statements,pg_trgm'
supautils.privileged_extensions_superuser = 'postgres'
supautils.reserved_roles = 'postgres'
logging_collector = off
`, port, sockDir)
	if err := os.WriteFile(filepath.Join(pgData, "postgresql.conf"), []byte(conf), 0o600); err != nil {
		return nil, err
	}

	hba := `
local   all             all                                     trust
local   replication     all                                     trust
host    all             postgres        127.0.0.1/32            trust
host    all             all             127.0.0.1/32            scram-sha-256
host    replication     all             127.0.0.1/32            trust
`
	if err := os.WriteFile(filepath.Join(pgData, "pg_hba.conf"), []byte(strings.TrimSpace(hba)+"\n"), 0o600); err != nil {
		return nil, err
	}
	_ = exec.Command("chown", "-R", "postgres:postgres", activeBase).Run()
	_ = os.Chmod(pgData, 0o700)

	rt, err := a.StartOrAdoptTenant(tenantID, epoch)
	if err != nil {
		return nil, err
	}

	// Deliver tenant password strictly via 0600 root/postgres temp file (never CLI/environ, RISK-22)
	sqlFile := filepath.Join(activeBase, ".init_secret.sql")
	sqlContent := fmt.Sprintf(
		"SET password_encryption = 'scram-sha-256'; CREATE ROLE app_user WITH LOGIN PASSWORD '%s'; GRANT ALL ON DATABASE postgres TO app_user; GRANT ALL ON SCHEMA public TO app_user;\n",
		strings.ReplaceAll(appPassword, "'", "''"),
	)
	if err := os.WriteFile(sqlFile, []byte(sqlContent), 0o600); err != nil {
		return nil, err
	}
	_ = exec.Command("chown", "postgres:postgres", sqlFile).Run()
	out, err := runPostgresCmd(filepath.Join(pgBin, "psql"), "-h", sockDir, "-p", strconv.Itoa(rt.Port), "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-f", sqlFile)
	_ = os.Remove(sqlFile)
	if err != nil {
		return nil, fmt.Errorf("role setup failed: %w (%s)", err, out)
	}

	// Readiness gate: verify SELECT 1 on live cluster (Flow 1)
	probeOut, err := runPostgresCmd(filepath.Join(pgBin, "psql"), "-h", sockDir, "-p", strconv.Itoa(rt.Port), "-U", "postgres", "-d", "postgres", "-t", "-A", "-c", "SELECT 1;")
	if err != nil || strings.TrimSpace(probeOut) != "1" {
		return nil, fmt.Errorf("readiness SELECT 1 failed: %v (%s)", err, probeOut)
	}

	_ = a.reloadPgBouncer()
	return rt, nil
}

// StartOrAdoptTenant ensures single-primary exclusivity via flock + postmaster.pid + /proc check (LI-3, EXP-A3).
func (a *Agent) StartOrAdoptTenant(tenantID string, epoch int64) (*TenantRuntime, error) {
	a.tenantMu.Lock()
	defer a.tenantMu.Unlock()

	activeBase := filepath.Join(a.volDir, "tenants/active", tenantID)
	pgData := filepath.Join(activeBase, "pgdata")
	sockDir := filepath.Join(activeBase, "sock")
	lockPath := filepath.Join(activeBase, "agent.lock")

	if err := a.acquireFlockLocked(tenantID, lockPath); err != nil {
		a.dualStartBlock.Add(1)
		return nil, err
	}

	// Check if postgres is already running for this PGDATA (re-adoption or duplicate start prevention)
	pid, port := readPostmasterPIDAndPort(pgData)
	if pid > 0 && isPostgresProcessAlive(pid) {
		if existing, ok := a.tenants[tenantID]; ok && existing.PID == pid {
			a.dualStartBlock.Add(1)
			return existing, nil
		}
		rt := &TenantRuntime{
			TenantID:         tenantID,
			SystemIdentifier: extractSystemIdentifier(pgData),
			Port:             port,
			PID:              pid,
			Epoch:            epoch,
			Readopted:        true,
			DataDir:          pgData,
			SockDir:          sockDir,
		}
		a.tenants[tenantID] = rt
		a.readoptedCount.Add(1)
		return rt, nil
	}

	// Safe to remove stale postmaster.pid ONLY after flock is held and /proc/<pid> is verified dead (ADR-006)
	if pid > 0 {
		_ = os.Remove(filepath.Join(pgData, "postmaster.pid"))
	}

	logFile := filepath.Join(activeBase, "postgres.log")
	out, err := runPostgresCmd(filepath.Join(pgBin, "pg_ctl"), "-D", pgData, "-l", logFile, "-w", "-t", "20", "start")
	if err != nil {
		return nil, fmt.Errorf("pg_ctl start failed: %w (%s)", err, out)
	}
	newPID, newPort := readPostmasterPIDAndPort(pgData)
	rt := &TenantRuntime{
		TenantID:         tenantID,
		SystemIdentifier: extractSystemIdentifier(pgData),
		Port:             newPort,
		PID:              newPID,
		Epoch:            epoch,
		Readopted:        false,
		DataDir:          pgData,
		SockDir:          sockDir,
	}
	a.tenants[tenantID] = rt
	return rt, nil
}

// RollbackClaim removes an uncommitted /vol/tenants/claiming/{tenantID} directory.
func (a *Agent) RollbackClaim(tenantID string) {
	_ = os.RemoveAll(filepath.Join(a.volDir, "tenants/claiming", tenantID))
}

// ReconcileInventory purges any claiming/ or active/ directories not present in validActiveTenants (EXP-A3).
func (a *Agent) ReconcileInventory(validActiveTenants map[string]bool) (purged int) {
	purged += a.PurgeInterruptedStaging()

	claimingRoot := filepath.Join(a.volDir, "tenants/claiming")
	if entries, err := os.ReadDir(claimingRoot); err == nil {
		for _, e := range entries {
			if !validActiveTenants[e.Name()] {
				_ = os.RemoveAll(filepath.Join(claimingRoot, e.Name()))
				purged++
			}
		}
	}

	activeRoot := filepath.Join(a.volDir, "tenants/active")
	if entries, err := os.ReadDir(activeRoot); err == nil {
		for _, e := range entries {
			id := e.Name()
			if !validActiveTenants[id] {
				_ = a.StopAndPurgeTenant(id)
				purged++
			}
		}
	}
	return purged
}

func (a *Agent) StopAndPurgeTenant(tenantID string) error {
	a.tenantMu.Lock()
	defer a.tenantMu.Unlock()
	activeBase := filepath.Join(a.volDir, "tenants/active", tenantID)
	pgData := filepath.Join(activeBase, "pgdata")
	if pid, _ := readPostmasterPIDAndPort(pgData); pid > 0 && isPostgresProcessAlive(pid) {
		_, _ = runPostgresCmd(filepath.Join(pgBin, "pg_ctl"), "-D", pgData, "-m", "fast", "-w", "-t", "10", "stop")
	}
	a.releaseFlockLocked(tenantID)
	delete(a.tenants, tenantID)
	return os.RemoveAll(activeBase)
}

func (a *Agent) StopTenantOnly(tenantID string) error {
	a.tenantMu.Lock()
	defer a.tenantMu.Unlock()
	activeBase := filepath.Join(a.volDir, "tenants/active", tenantID)
	pgData := filepath.Join(activeBase, "pgdata")
	if pid, _ := readPostmasterPIDAndPort(pgData); pid > 0 && isPostgresProcessAlive(pid) {
		_, err := runPostgresCmd(filepath.Join(pgBin, "pg_ctl"), "-D", pgData, "-m", "fast", "-w", "-t", "10", "stop")
		return err
	}
	return nil
}

// reloadPgBouncer generates pgbouncer.ini (transaction pooling + max_prepared_statements=200, ADR-009) and starts/reloads PgBouncer.
func (a *Agent) reloadPgBouncer() error {
	a.pgbouncerMu.Lock()
	defer a.pgbouncerMu.Unlock()

	pbDir := filepath.Join(a.volDir, "pgbouncer")
	iniPath := filepath.Join(pbDir, "pgbouncer.ini")
	userlistPath := filepath.Join(pbDir, "userlist.txt")
	pidPath := filepath.Join(pbDir, "pgbouncer.pid")
	logPath := filepath.Join(pbDir, "pgbouncer.log")

	var dbLines []string
	a.tenantMu.Lock()
	for id, rt := range a.tenants {
		dbLines = append(dbLines, fmt.Sprintf("%s = host=%s port=%d dbname=postgres user=postgres", id, rt.SockDir, rt.Port))
	}
	a.tenantMu.Unlock()
	sort.Strings(dbLines)

	ini := fmt.Sprintf(`[databases]
%s

[pgbouncer]
listen_addr = 127.0.0.1
listen_port = %d
unix_socket_dir = %s
auth_type = trust
auth_file = %s
pool_mode = transaction
max_client_conn = 500
default_pool_size = 10
max_prepared_statements = 200
logfile = %s
pidfile = %s
admin_users = postgres
ignore_startup_parameters = extra_float_digits,options
`, strings.Join(dbLines, "\n"), a.pgbouncerPort, pbDir, userlistPath, logPath, pidPath)

	if err := os.WriteFile(userlistPath, []byte("\"postgres\" \"\"\n\"app_user\" \"\"\n"), 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(iniPath, []byte(ini), 0o600); err != nil {
		return err
	}
	_ = exec.Command("chown", "-R", "postgres:postgres", pbDir).Run()

	// Check if pgbouncer is already running
	if b, err := os.ReadFile(pidPath); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil && pid > 0 {
			if _, err := os.Stat(fmt.Sprintf("/proc/%d/comm", pid)); err == nil {
				_ = syscall.Kill(pid, syscall.SIGHUP)
				return nil
			}
		}
	}

	_, err := runPostgresCmd("pgbouncer", "-d", iniPath)
	return err
}

func (a *Agent) PgBouncerAddr() string {
	return fmt.Sprintf("127.0.0.1:%d", a.pgbouncerPort)
}

func (a *Agent) GetTenant(tenantID string) (*TenantRuntime, bool) {
	a.tenantMu.Lock()
	defer a.tenantMu.Unlock()
	rt, ok := a.tenants[tenantID]
	return rt, ok
}

type BackupArtifact struct {
	BackupID     string `json:"backup_id"`
	Type         string `json:"type"` // "FULL" or "INCR"
	R2Key        string `json:"r2_key"`
	Epoch        int64  `json:"epoch"`
	CipherBytes  int    `json:"cipher_bytes"`
	Verified     bool   `json:"verified"`
	LocalBackupD string `json:"-"`
}

// RunEncryptedBackup executes pg_basebackup (FULL or --incremental), verifies with pg_verifybackup,
// compresses with zstd, encrypts with per-tenant AES-256-GCM DEK + AAD, and uploads to Cloudflare R2 with If-None-Match: "*".
func (a *Agent) RunEncryptedBackup(ctx context.Context, s3c *s3.Client, bucket, tenantID, bkpType, parentManifestPath string) (*BackupArtifact, string, error) {
	rt, ok := a.GetTenant(tenantID)
	if !ok {
		return nil, "", fmt.Errorf("tenant %s not active", tenantID)
	}
	if _, err := a.kms.EnsureTenantDEK(tenantID); err != nil {
		return nil, "", err
	}

	bkpID := fmt.Sprintf("bkp_%s_%d", strings.ToLower(bkpType), time.Now().UnixNano())
	bkpDir := filepath.Join(a.volDir, "backups", tenantID, bkpID)
	_ = os.RemoveAll(bkpDir)
	if err := os.MkdirAll(filepath.Dir(bkpDir), 0o755); err != nil {
		return nil, "", err
	}
	_ = exec.Command("chown", "-R", "postgres:postgres", filepath.Dir(bkpDir)).Run()

	// Force checkpoint so WAL summarizer is caught up for incremental backups
	_, _ = runPostgresCmd(filepath.Join(pgBin, "psql"), "-h", rt.SockDir, "-p", strconv.Itoa(rt.Port), "-U", "postgres", "-d", "postgres", "-c", "CHECKPOINT;")
	if bkpType == "INCR" {
		time.Sleep(300 * time.Millisecond)
	}

	args := []string{filepath.Join(pgBin, "pg_basebackup"), "-h", rt.SockDir, "-p", strconv.Itoa(rt.Port), "-U", "postgres", "-D", bkpDir, "-c", "fast"}
	if bkpType == "INCR" && parentManifestPath != "" {
		args = append(args, "--incremental="+parentManifestPath)
	}
	if out, err := runPostgresCmd(args[0], args[1:]...); err != nil {
		return nil, "", fmt.Errorf("pg_basebackup (%s) failed: %w (%s)", bkpType, err, out)
	}

	// Mandatory pre-publish pg_verifybackup check (ADR-002)
	if out, err := runPostgresCmd(filepath.Join(pgBin, "pg_verifybackup"), bkpDir); err != nil {
		return nil, "", fmt.Errorf("pg_verifybackup failed: %w (%s)", err, out)
	}

	// Test DEK unwrap before publish
	if _, err := a.kms.UnwrapDEK(tenantID); err != nil {
		return nil, "", err
	}

	// Compress with tar + zstd
	tarZst := bkpDir + ".tar.zst"
	if out, err := exec.Command("tar", "--zstd", "-cf", tarZst, "-C", bkpDir, ".").CombinedOutput(); err != nil {
		return nil, "", fmt.Errorf("tar --zstd failed: %w (%s)", err, string(out))
	}
	plainBytes, err := os.ReadFile(tarZst)
	_ = os.Remove(tarZst)
	if err != nil {
		return nil, "", err
	}

	r2Key := fmt.Sprintf("stage1/tenants/%s/epoch_%06d/%s.tar.zst.enc", tenantID, rt.Epoch, bkpID)
	cipherBytes, err := a.kms.EncryptObject(tenantID, rt.Epoch, r2Key, plainBytes)
	if err != nil {
		return nil, "", err
	}

	if s3c != nil {
		_, err := s3c.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(r2Key),
			Body:        bytes.NewReader(cipherBytes),
			IfNoneMatch: aws.String("*"),
		})
		if err != nil {
			return nil, "", fmt.Errorf("R2 PutObject If-None-Match failed: %w", err)
		}
	}

	return &BackupArtifact{
		BackupID:     bkpID,
		Type:         bkpType,
		R2Key:        r2Key,
		Epoch:        rt.Epoch,
		CipherBytes:  len(cipherBytes),
		Verified:     true,
		LocalBackupD: bkpDir,
	}, filepath.Join(bkpDir, "backup_manifest"), nil
}

// DownloadAndCombineRestore downloads encrypted FULL + INCR backups from R2, verifies AAD + DEK decryption,
// combines them via pg_combinebackup, verifies via pg_verifybackup, boots an isolated PG17, and runs pg_amcheck.
func (a *Agent) DownloadAndCombineRestore(ctx context.Context, s3c *s3.Client, bucket, tenantID string, chain []*BackupArtifact, verifyQuery string) (string, bool, error) {
	restoreRoot := filepath.Join(a.volDir, "backups", tenantID, fmt.Sprintf("restore_%d", time.Now().UnixNano()))
	if err := os.MkdirAll(restoreRoot, 0o755); err != nil {
		return "", false, err
	}
	defer func() { _ = os.RemoveAll(restoreRoot) }()

	var unpackedDirs []string
	for i, art := range chain {
		obj, err := s3c.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(art.R2Key),
		})
		if err != nil {
			return "", false, fmt.Errorf("R2 GetObject %s: %w", art.R2Key, err)
		}
		cipherBytes, err := io.ReadAll(obj.Body)
		_ = obj.Body.Close()
		if err != nil {
			return "", false, err
		}

		// Decrypt and verify AAD = tenantID|epoch|r2Key
		plainBytes, err := a.kms.DecryptObject(tenantID, art.Epoch, art.R2Key, cipherBytes)
		if err != nil {
			return "", false, err
		}

		tarZst := filepath.Join(restoreRoot, fmt.Sprintf("part_%d.tar.zst", i))
		partDir := filepath.Join(restoreRoot, fmt.Sprintf("part_%d", i))
		_ = os.MkdirAll(partDir, 0o700)
		if err := os.WriteFile(tarZst, plainBytes, 0o600); err != nil {
			return "", false, err
		}
		if out, err := exec.Command("tar", "--zstd", "-xf", tarZst, "-C", partDir).CombinedOutput(); err != nil {
			return "", false, fmt.Errorf("untar zstd failed: %w (%s)", err, string(out))
		}
		_ = os.Remove(tarZst)
		unpackedDirs = append(unpackedDirs, partDir)
	}

	combinedDir := filepath.Join(restoreRoot, "combined_pgdata")
	if len(unpackedDirs) == 1 {
		if err := os.Rename(unpackedDirs[0], combinedDir); err != nil {
			return "", false, err
		}
	} else {
		args := append([]string{filepath.Join(pgBin, "pg_combinebackup"), "-o", combinedDir}, unpackedDirs...)
		_ = exec.Command("chown", "-R", "postgres:postgres", restoreRoot).Run()
		if out, err := runPostgresCmd(args[0], args[1:]...); err != nil {
			return "", false, fmt.Errorf("pg_combinebackup failed: %w (%s)", err, out)
		}
	}

	_ = exec.Command("chown", "-R", "postgres:postgres", combinedDir).Run()
	_ = os.Chmod(combinedDir, 0o700)

	if out, err := runPostgresCmd(filepath.Join(pgBin, "pg_verifybackup"), combinedDir); err != nil {
		return "", false, fmt.Errorf("pg_verifybackup on combined dir failed: %w (%s)", err, out)
	}

	// Boot isolated restored PG17 cluster on an ephemeral port and run verifyQuery + pg_amcheck
	sockDir := filepath.Join(restoreRoot, "sock")
	_ = os.MkdirAll(sockDir, 0o755)
	_ = exec.Command("chown", "-R", "postgres:postgres", sockDir).Run()
	port := int(a.portSeq.Add(1))
	f, err := os.OpenFile(filepath.Join(combinedDir, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return "", false, err
	}
	_, _ = fmt.Fprintf(f, "\nport = %d\nunix_socket_directories = '%s'\n", port, sockDir)
	_ = f.Close()

	logFile := filepath.Join(restoreRoot, "restore.log")
	if out, err := runPostgresCmd(filepath.Join(pgBin, "pg_ctl"), "-D", combinedDir, "-l", logFile, "-w", "-t", "20", "start"); err != nil {
		return "", false, fmt.Errorf("restored pg_ctl start failed: %w (%s)", err, out)
	}
	defer func() {
		_, _ = runPostgresCmd(filepath.Join(pgBin, "pg_ctl"), "-D", combinedDir, "-m", "immediate", "-w", "stop")
	}()

	qOut, err := runPostgresCmd(filepath.Join(pgBin, "psql"), "-h", sockDir, "-p", strconv.Itoa(port), "-U", "postgres", "-d", "postgres", "-t", "-A", "-c", verifyQuery)
	if err != nil {
		return "", false, fmt.Errorf("restore verify query failed: %w (%s)", err, qOut)
	}

	_, _ = runPostgresCmd(filepath.Join(pgBin, "psql"), "-h", sockDir, "-p", strconv.Itoa(port), "-U", "postgres", "-d", "postgres", "-c", "CREATE EXTENSION IF NOT EXISTS amcheck;")
	_, amErr := runPostgresCmd(filepath.Join(pgBin, "pg_amcheck"), "-h", sockDir, "-p", strconv.Itoa(port), "-U", "postgres", "postgres")

	return strings.TrimSpace(qOut), amErr == nil, nil
}

// HTTPHandler exposes the Cell Agent RPC API over HTTP (used by both Control Plane and standalone daemon mode in EXP-A3).
func (a *Agent) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		a.tenantMu.Lock()
		n := len(a.tenants)
		a.tenantMu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"status":            "ok",
			"pid":               os.Getpid(),
			"active_tenants":    n,
			"ready_pool_slots":  a.ReadyPoolCount(),
			"readopted_total":   a.readoptedCount.Load(),
			"dual_start_blocks": a.dualStartBlock.Load(),
		})
	})
	mux.HandleFunc("/claim", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("tenant_id")
		res, err := a.ClaimCluster(id)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, res)
	})
	mux.HandleFunc("/activate", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TenantID string `json:"tenant_id"`
			Password string `json:"password"`
			Epoch    int64  `json:"epoch"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		rt, err := a.ActivateCluster(req.TenantID, req.Password, req.Epoch)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, rt)
	})
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("tenant_id")
		rt, err := a.StartOrAdoptTenant(id, 1)
		if err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, rt)
	})
	return mux
}

func (a *Agent) Close() {
	close(a.stopCh)
	a.tenantMu.Lock()
	for id, rt := range a.tenants {
		if isPostgresProcessAlive(rt.PID) {
			_, _ = runPostgresCmd(filepath.Join(pgBin, "pg_ctl"), "-D", rt.DataDir, "-m", "fast", "-w", "-t", "10", "stop")
		}
		a.releaseFlockLocked(id)
	}
	a.tenantMu.Unlock()
}

func extractSystemIdentifier(pgData string) string {
	out, err := runPostgresCmd(filepath.Join(pgBin, "pg_controldata"), pgData)
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

func runPostgresCmd(bin string, args ...string) (string, error) {
	fullArgs := append([]string{"-u", "postgres", "--", bin}, args...)
	cmd := exec.Command("runuser", fullArgs...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return strings.TrimSpace(out.String()), err
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func NewR2ClientFromEnv(ctx context.Context) (*s3.Client, string, error) {
	ak := strings.Trim(os.Getenv("ACCESS_KEY"), "\"' ")
	sk := strings.Trim(os.Getenv("SECRET_KEY"), "\"' ")
	ep := strings.Trim(os.Getenv("ENDPOINT"), "\"' ")
	bucket := strings.Trim(os.Getenv("R2_BUCKET"), "\"' ")
	if bucket == "" {
		bucket = "air-mvp-test"
	}
	if ak == "" || sk == "" || ep == "" {
		return nil, bucket, errors.New("missing R2 env vars (ACCESS_KEY, SECRET_KEY, ENDPOINT)")
	}
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("auto"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
	)
	if err != nil {
		return nil, bucket, err
	}
	cli := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(ep)
		o.UsePathStyle = true
	})
	return cli, bucket, nil
}
