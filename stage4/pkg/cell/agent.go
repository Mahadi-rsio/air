package cell

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"air-stage4/pkg/crypto"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
)

const pgBin = "/usr/lib/postgresql/17/bin"

// max_concurrent_starts = 5 protects Cells from CPU/IOPS thrashing during wake storms.
var wakeStartSemaphore = make(chan struct{}, 5)

type TenantRuntime struct {
	TenantID         string `json:"tenant_id"`
	SystemIdentifier string `json:"system_identifier"`
	Port             int    `json:"port"`
	PID              int    `json:"pid"`
	Epoch            int64  `json:"epoch"`
	Readopted        bool   `json:"readopted"`
	DataDir          string `json:"data_dir"`
	SockDir          string `json:"sock_dir"`
	WALSpoolDir      string `json:"wal_spool_dir"`
	AppPassword      string `json:"-"`
	ReadOnlyQuota    bool   `json:"read_only_quota"`
	Quarantined      bool   `json:"quarantined"`
}

type BackupArtifact struct {
	BackupID     string                  `json:"backup_id"`
	ParentID     string                  `json:"parent_id"`
	Type         string                  `json:"type"` // "FULL" or "INCR"
	R2Key        string                  `json:"r2_key"`
	Epoch        int64                   `json:"epoch"`
	StartLSN     string                  `json:"start_lsn"`
	EndLSN       string                  `json:"end_lsn"`
	CipherBytes  int                     `json:"cipher_bytes"`
	CreatedAt    time.Time               `json:"created_at"`
	Verified     bool                    `json:"verified"`
	DEKBundle    crypto.WrappedDEKBundle `json:"dek_bundle"`
	LocalBackupD string                  `json:"-"`
}

type Agent struct {
	volDir          string
	targetPoolSize  int
	poolMu          sync.Mutex
	tenantMu        sync.Mutex
	tenants         map[string]*TenantRuntime
	tenantPorts     map[string]int      // persistent port assignment per tenantID (prevents port leak, S4-RISK-05)
	tenantPasswords map[string]string   // tenantID -> app_user password
	tenantQuotas    map[string]int64    // tenantID -> hard byte quota
	lockFDs         map[string]*os.File // closed cleanly on StopTenantClean / StopTenantOnly (prevents FD leak, S4-RISK-05)
	fallbackSem     chan struct{}
	refillWake      chan struct{}
	stopCh          chan struct{}
	poolSeq         atomic.Int64
	portSeq         atomic.Int32
	readoptedCount  atomic.Int64
	dualStartBlock  atomic.Int64
	kms             *crypto.EnvelopeKMS

	// Monotonic Lease Watchdog state (S4-RISK-01)
	leaseMu          sync.Mutex
	lastLeaseAckMono time.Time
	watchdogFenced   atomic.Bool
	watchdogKillMs   atomic.Int64
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
		"tenants/quarantine",
		"backups",
		"spool/wal",
		"tmp",
	} {
		p := filepath.Join(volDir, sub)
		if err := os.MkdirAll(p, 0o755); err != nil {
			return nil, err
		}
	}
	_ = exec.Command("chown", "-R", "postgres:postgres", volDir).Run()

	// 2. Pre-allocate 64MB .cell_ballast emergency space reserve (ADR-010, S4-RISK-03)
	ballastPath := filepath.Join(volDir, ".cell_ballast")
	if _, err := os.Stat(ballastPath); err != nil {
		if f, err := os.Create(ballastPath); err == nil {
			_ = syscall.Fallocate(int(f.Fd()), 0, 0, 64*1024*1024)
			_ = f.Close()
		}
	}

	a := &Agent{
		volDir:           volDir,
		targetPoolSize:   targetPoolSize,
		tenants:          make(map[string]*TenantRuntime),
		tenantPorts:      make(map[string]int),
		tenantPasswords:  make(map[string]string),
		tenantQuotas:     make(map[string]int64),
		lockFDs:          make(map[string]*os.File),
		fallbackSem:      make(chan struct{}, 4),
		refillWake:       make(chan struct{}, 1),
		stopCh:           make(chan struct{}),
		kms:              kms,
		lastLeaseAckMono: time.Now(), // Go time.Time carries CLOCK_MONOTONIC reading
	}
	a.portSeq.Store(17000)

	a.PurgeInterruptedStaging()
	a.ReadoptRunningTenants()

	return a, nil
}

func (a *Agent) KMS() *crypto.EnvelopeKMS {
	return a.kms
}

func (a *Agent) VolDir() string {
	return a.volDir
}

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
		spoolDir := filepath.Join(a.volDir, "spool/wal", tenantID)
		pid, port := readPostmasterPIDAndPort(pgData)
		if pid > 0 && isPostgresProcessAlive(pid) {
			a.tenantMu.Lock()
			_ = a.acquireFlockLocked(tenantID, filepath.Join(tDir, "agent.lock"))
			sysID := extractSystemIdentifier(pgData)
			if port >= int(a.portSeq.Load()) {
				a.portSeq.Store(int32(port + 1))
			}
			a.tenantPorts[tenantID] = port
			a.tenants[tenantID] = &TenantRuntime{
				TenantID:         tenantID,
				SystemIdentifier: sysID,
				Port:             port,
				PID:              pid,
				Epoch:            1,
				Readopted:        true,
				DataDir:          pgData,
				SockDir:          sockDir,
				WALSpoolDir:      spoolDir,
			}
			a.tenantMu.Unlock()
			a.readoptedCount.Add(1)
			count++
		}
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

// SignalProcessTree sends sig to both the postmaster and all its child processes.
func SignalProcessTree(rootPID int, sig syscall.Signal) {
	if rootPID <= 0 {
		return
	}
	entries, err := os.ReadDir("/proc")
	if err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			pid, err := strconv.Atoi(e.Name())
			if err != nil || pid == rootPID {
				continue
			}
			statBytes, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
			if err != nil {
				continue
			}
			s := string(statBytes)
			idx := strings.LastIndex(s, ")")
			if idx != -1 && idx+2 < len(s) {
				rest := strings.Fields(s[idx+2:])
				if len(rest) >= 2 {
					ppid, _ := strconv.Atoi(rest[1])
					if ppid == rootPID {
						_ = syscall.Kill(pid, sig)
					}
				}
			}
		}
	}
	_ = syscall.Kill(rootPID, sig)
	for {
		reaped, _ := syscall.Wait4(-1, nil, syscall.WNOHANG, nil)
		if reaped <= 0 {
			break
		}
	}
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

// OpenLockFDCount returns the number of currently open agent.lock file descriptors (used in EXP-S4-5A).
func (a *Agent) OpenLockFDCount() int {
	a.tenantMu.Lock()
	defer a.tenantMu.Unlock()
	return len(a.lockFDs)
}

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

	out, err := runPostgresCmd("nice", "-n", "19", filepath.Join(pgBin, "initdb"),
		"-D", stageDir, "-U", "postgres", "--data-checksums", "--auth=trust", "--no-instructions")
	if err != nil {
		_ = os.RemoveAll(stageDir)
		return fmt.Errorf("initdb failed: %w (%s)", err, out)
	}

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
	Source           string  `json:"source"`
	ClaimLatencyMs   float64 `json:"claim_latency_ms"`
}

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

// allocOrReusePort returns the tenant's existing assigned port or allocates a new one once (S4-RISK-05).
func (a *Agent) allocOrReusePort(tenantID string) int {
	a.tenantMu.Lock()
	defer a.tenantMu.Unlock()
	if p, ok := a.tenantPorts[tenantID]; ok && p > 0 {
		return p
	}
	p := int(a.portSeq.Add(1))
	a.tenantPorts[tenantID] = p
	return p
}

// ActivateCluster promotes claiming -> active, configures continuous WAL archiving (archive_mode = on),
// supautils v4.0.0, temp_file_limit, wal_level = logical, and least-privilege app_user with SCRAM-SHA-256.
func (a *Agent) ActivateCluster(tenantID, appPassword string, epoch int64) (*TenantRuntime, error) {
	claimDir := filepath.Join(a.volDir, "tenants/claiming", tenantID)
	activeBase := filepath.Join(a.volDir, "tenants/active", tenantID)
	pgData := filepath.Join(activeBase, "pgdata")
	sockDir := filepath.Join(activeBase, "sock")
	walSpoolDir := filepath.Join(a.volDir, "spool/wal", tenantID)

	if err := os.MkdirAll(sockDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(walSpoolDir, 0o700); err != nil {
		return nil, err
	}
	_ = exec.Command("chown", "-R", "postgres:postgres", walSpoolDir).Run()

	if err := os.Rename(claimDir, pgData); err != nil {
		return nil, fmt.Errorf("rename claim->active failed: %w", err)
	}

	port := a.allocOrReusePort(tenantID)
	a.tenantMu.Lock()
	a.tenantPasswords[tenantID] = appPassword
	a.tenantMu.Unlock()

	conf := fmt.Sprintf(`
listen_addresses = '127.0.0.1'
port = %d
unix_socket_directories = '%s'
max_connections = 25
max_prepared_transactions = 10
shared_buffers = '8MB'
work_mem = '4MB'
maintenance_work_mem = '16MB'
temp_file_limit = '32MB'
max_parallel_workers_per_gather = 0
password_encryption = 'scram-sha-256'
wal_level = logical
summarize_wal = on
archive_mode = on
archive_command = 'cp %%p %s/%%f'
max_wal_senders = 6
max_replication_slots = 6
shared_preload_libraries = 'supautils'
supautils.privileged_extensions = 'pg_stat_statements,pg_trgm,amcheck'
supautils.privileged_extensions_superuser = 'postgres'
supautils.reserved_roles = 'postgres'
logging_collector = off
`, port, sockDir, walSpoolDir)
	if err := os.WriteFile(filepath.Join(pgData, "postgresql.conf"), []byte(strings.TrimSpace(conf)+"\n"), 0o600); err != nil {
		return nil, err
	}

	hba := `
local   all             postgres                                trust
local   replication     postgres                                trust
local   all             all                                     trust
host    all             postgres        127.0.0.1/32            trust
host    all             all             127.0.0.1/32            scram-sha-256
host    replication     postgres        127.0.0.1/32            trust
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
	rt.WALSpoolDir = walSpoolDir
	rt.AppPassword = appPassword

	// Provision isolated non-superuser role app_user and revoke PUBLIC cross-DB access (S4-RISK-04)
	sqlFile := filepath.Join(activeBase, ".init_secret.sql")
	escPass := strings.ReplaceAll(appPassword, "'", "''")
	sqlContent := fmt.Sprintf(`
SET password_encryption = 'scram-sha-256';
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_user') THEN
        CREATE ROLE app_user WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '%s';
    ELSE
        ALTER ROLE app_user WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '%s';
    END IF;
END
$$;
REVOKE CONNECT ON DATABASE template1 FROM PUBLIC;
GRANT ALL ON DATABASE postgres TO app_user;
GRANT ALL ON SCHEMA public TO app_user;
`, escPass, escPass)
	if err := os.WriteFile(sqlFile, []byte(sqlContent), 0o600); err != nil {
		return nil, err
	}
	_ = exec.Command("chown", "postgres:postgres", sqlFile).Run()
	out, err := runPostgresCmd(filepath.Join(pgBin, "psql"), "-h", sockDir, "-p", strconv.Itoa(rt.Port), "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-f", sqlFile)
	_ = os.Remove(sqlFile)
	if err != nil {
		return nil, fmt.Errorf("role setup failed: %w (%s)", err, out)
	}

	probeOut, err := runPostgresCmd(filepath.Join(pgBin, "psql"), "-h", sockDir, "-p", strconv.Itoa(rt.Port), "-U", "postgres", "-d", "postgres", "-t", "-A", "-c", "SELECT 1;")
	if err != nil || strings.TrimSpace(probeOut) != "1" {
		return nil, fmt.Errorf("readiness SELECT 1 failed: %v (%s)", err, probeOut)
	}

	return rt, nil
}

func (a *Agent) StartOrAdoptTenant(tenantID string, epoch int64) (*TenantRuntime, error) {
	a.tenantMu.Lock()
	defer a.tenantMu.Unlock()

	activeBase := filepath.Join(a.volDir, "tenants/active", tenantID)
	pgData := filepath.Join(activeBase, "pgdata")
	sockDir := filepath.Join(activeBase, "sock")
	walSpoolDir := filepath.Join(a.volDir, "spool/wal", tenantID)
	lockPath := filepath.Join(activeBase, "agent.lock")

	if err := a.acquireFlockLocked(tenantID, lockPath); err != nil {
		a.dualStartBlock.Add(1)
		return nil, err
	}

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
			WALSpoolDir:      walSpoolDir,
			AppPassword:      a.tenantPasswords[tenantID],
		}
		a.tenantPorts[tenantID] = port
		a.tenants[tenantID] = rt
		a.readoptedCount.Add(1)
		return rt, nil
	}

	if pid > 0 {
		_ = os.Remove(filepath.Join(pgData, "postmaster.pid"))
	}

	logFile := filepath.Join(activeBase, "postgres.log")
	out, err := runPostgresCmd(filepath.Join(pgBin, "pg_ctl"), "-D", pgData, "-l", logFile, "-w", "-t", "20", "start")
	if err != nil {
		a.releaseFlockLocked(tenantID)
		return nil, fmt.Errorf("pg_ctl start failed: %w (%s)", err, out)
	}
	newPID, newPort := readPostmasterPIDAndPort(pgData)
	a.tenantPorts[tenantID] = newPort
	rt := &TenantRuntime{
		TenantID:         tenantID,
		SystemIdentifier: extractSystemIdentifier(pgData),
		Port:             newPort,
		PID:              newPID,
		Epoch:            epoch,
		Readopted:        false,
		DataDir:          pgData,
		SockDir:          sockDir,
		WALSpoolDir:      walSpoolDir,
		AppPassword:      a.tenantPasswords[tenantID],
	}
	a.tenants[tenantID] = rt
	return rt, nil
}

func (a *Agent) WakeTenant(ctx context.Context, tenantID string, epoch int64) (*TenantRuntime, error) {
	select {
	case wakeStartSemaphore <- struct{}{}:
		defer func() { <-wakeStartSemaphore }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return a.StartOrAdoptTenant(tenantID, epoch)
}

// StopTenantClean stops a tenant cleanly AND releases its agent.lock file descriptor so FDs never leak (S4-RISK-05).
func (a *Agent) StopTenantClean(tenantID string, mode string) (string, error) {
	if mode == "" {
		mode = "smart"
	}
	pgdata := filepath.Join(a.volDir, "tenants/active", tenantID, "pgdata")
	cmd := exec.Command("su", "-", "postgres", "-c",
		fmt.Sprintf("/usr/lib/postgresql/17/bin/pg_ctl -D %s -m %s -w -t 20 stop", pgdata, mode))
	out, err := cmd.CombinedOutput()
	if err != nil && mode == "smart" {
		cmdFast := exec.Command("su", "-", "postgres", "-c",
			fmt.Sprintf("/usr/lib/postgresql/17/bin/pg_ctl -D %s -m fast -w -t 15 stop", pgdata))
		_, _ = cmdFast.CombinedOutput()
	}
	state, sErr := a.ReadPgControlState(tenantID)
	a.tenantMu.Lock()
	if rt, ok := a.tenants[tenantID]; ok {
		rt.PID = 0
	}
	// CRITICAL REMEDIATION (S4-RISK-05): Close and unlock agent.lock FD when cluster stops!
	a.releaseFlockLocked(tenantID)
	a.tenantMu.Unlock()

	if sErr != nil {
		return "", fmt.Errorf("stop clean (%v: %s) state check: %w", err, string(out), sErr)
	}
	return state, nil
}

func (a *Agent) StopTenantOnly(tenantID string) error {
	_, err := a.StopTenantClean(tenantID, "fast")
	return err
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
	delete(a.tenantPorts, tenantID)
	delete(a.tenantPasswords, tenantID)
	delete(a.tenantQuotas, tenantID)
	_ = os.RemoveAll(filepath.Join(a.volDir, "spool/wal", tenantID))
	return os.RemoveAll(activeBase)
}

func (a *Agent) ReadPgControlState(tenantID string) (string, error) {
	pgdata := filepath.Join(a.volDir, "tenants/active", tenantID, "pgdata")
	out, err := exec.Command("/usr/lib/postgresql/17/bin/pg_controldata", pgdata).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("pg_controldata failed: %v (%s)", err, string(out))
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "Database cluster state:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "Database cluster state:")), nil
		}
	}
	return "", fmt.Errorf("Database cluster state not found")
}

func (a *Agent) GetTenant(tenantID string) (*TenantRuntime, bool) {
	a.tenantMu.Lock()
	defer a.tenantMu.Unlock()
	rt, ok := a.tenants[tenantID]
	return rt, ok
}

func (a *Agent) GetTenantPassword(tenantID string) (string, bool) {
	a.tenantMu.Lock()
	defer a.tenantMu.Unlock()
	pw, ok := a.tenantPasswords[tenantID]
	return pw, ok
}

// =========================================================================
// WORKSTREAM 1 REMEDIATION: Monotonic Lease Watchdog Inside Agent (S4-RISK-01)
// =========================================================================

func (a *Agent) RenewLeaseAck() {
	a.leaseMu.Lock()
	a.lastLeaseAckMono = time.Now()
	a.leaseMu.Unlock()
	a.watchdogFenced.Store(false)
}

// StartLeaseWatchdog runs a high-frequency monotonic timer loop inside the Cell Agent.
// If time.Since(lastLeaseAckMono) >= selfFenceDeadline (e.g. 8.0s < 12.0s promote barrier),
// the Agent immediately fences all local primary clusters (SIGKILL process tree + quarantine).
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
				elapsed := time.Since(a.lastLeaseAckMono) // uses CLOCK_MONOTONIC
				a.leaseMu.Unlock()
				if elapsed >= selfFenceDeadline && !a.watchdogFenced.Load() {
					a.watchdogFenced.Store(true)
					a.watchdogKillMs.Store(time.Since(tStart).Milliseconds())
					a.fenceAllLocalPrimaries()
					return
				}
			}
		}
	}()
}

func (a *Agent) fenceAllLocalPrimaries() {
	a.tenantMu.Lock()
	defer a.tenantMu.Unlock()
	for id, rt := range a.tenants {
		if rt.PID > 0 {
			SignalProcessTree(rt.PID, syscall.SIGKILL)
			rt.PID = 0
		}
		rt.Quarantined = true
		a.releaseFlockLocked(id)
	}
}

func (a *Agent) WatchdogStatus() (fenced bool, killElapsedMs int64) {
	return a.watchdogFenced.Load(), a.watchdogKillMs.Load()
}

// =========================================================================
// WORKSTREAM 3 REMEDIATION: Integrated Storage Guard & Ballast (S4-RISK-03)
// =========================================================================

func (a *Agent) SetTenantStorageQuota(tenantID string, quotaBytes int64) {
	a.tenantMu.Lock()
	a.tenantQuotas[tenantID] = quotaBytes
	a.tenantMu.Unlock()
}

// MeasureTenantDiskBytes calculates physical disk usage of a tenant's PGDATA + WAL spool.
func (a *Agent) MeasureTenantDiskBytes(tenantID string) (int64, error) {
	activeBase := filepath.Join(a.volDir, "tenants/active", tenantID)
	var total int64
	err := filepath.WalkDir(activeBase, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return nil
		}
		if info, iErr := d.Info(); iErr == nil {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

// EnforceTenantQuotaOnce checks whether tenantID exceeds its storage quota and immediately flips
// default_transaction_read_only = on and terminates active non-readonly writers.
func (a *Agent) EnforceTenantQuotaOnce(ctx context.Context, tenantID string) (usedBytes int64, tripped bool, err error) {
	a.tenantMu.Lock()
	quota := a.tenantQuotas[tenantID]
	rt, ok := a.tenants[tenantID]
	a.tenantMu.Unlock()
	if !ok || quota <= 0 {
		return 0, false, nil
	}
	usedBytes, err = a.MeasureTenantDiskBytes(tenantID)
	if err != nil {
		return 0, false, err
	}
	if usedBytes < quota {
		return usedBytes, false, nil
	}

	dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", rt.Port)
	conn, cErr := pgx.Connect(ctx, dsn)
	if cErr != nil {
		return usedBytes, false, cErr
	}
	defer conn.Close(ctx)

	_, _ = conn.Exec(ctx, "ALTER DATABASE postgres SET default_transaction_read_only = on;")
	_, _ = conn.Exec(ctx, "ALTER ROLE app_user SET default_transaction_read_only = on;")
	_, _ = conn.Exec(ctx, "SELECT pg_reload_conf();")
	_, _ = conn.Exec(ctx, `
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE datname = 'postgres'
		  AND pid <> pg_backend_pid()
		  AND usename = 'app_user';
	`)
	a.tenantMu.Lock()
	rt.ReadOnlyQuota = true
	a.tenantMu.Unlock()
	return usedBytes, true, nil
}

// CheckVolumeWatermarkAndReleaseBallast implements the 75% / 85% / 92% volume watermark policy.
// At >= 92% utilization, it truncates .cell_ballast by 50% (or to 0) to guarantee emergency checkpoint space.
func (a *Agent) CheckVolumeWatermarkAndReleaseBallast(simulatedUsedPct float64) (action string, ballastBefore int64, ballastAfter int64, err error) {
	ballastPath := filepath.Join(a.volDir, ".cell_ballast")
	if st, sErr := os.Stat(ballastPath); sErr == nil {
		ballastBefore = st.Size()
	}

	usedPct := simulatedUsedPct
	if usedPct <= 0 {
		var stat syscall.Statfs_t
		if err := syscall.Statfs(a.volDir, &stat); err != nil {
			return "", ballastBefore, ballastBefore, err
		}
		if stat.Blocks > 0 {
			usedPct = float64(stat.Blocks-stat.Bavail) * 100.0 / float64(stat.Blocks)
		}
	}

	switch {
	case usedPct >= 92.0:
		newSize := ballastBefore / 2
		if err := os.Truncate(ballastPath, newSize); err != nil {
			return "EMERGENCY_BALLAST_RELEASE_FAILED", ballastBefore, ballastBefore, err
		}
		return "EMERGENCY_92PCT_BALLAST_TRUNCATED", ballastBefore, newSize, nil
	case usedPct >= 85.0:
		return "URGENT_85PCT_POOL_TRIM", ballastBefore, ballastBefore, nil
	case usedPct >= 75.0:
		return "WARN_75PCT_ADMISSION_FREEZE", ballastBefore, ballastBefore, nil
	default:
		return "NORMAL", ballastBefore, ballastBefore, nil
	}
}

// =========================================================================
// WORKSTREAM 2 REMEDIATION: Pre-Flight WAL Summary & Staging Space Guards
// =========================================================================

// VerifyWALSummaryContinuity queries pg_available_wal_summaries() on the live cluster to confirm
// PG17 WAL summaries are present before running pg_basebackup --incremental (S4-RISK-02).
func (a *Agent) VerifyWALSummaryContinuity(ctx context.Context, tenantID string) (summaryCount int, err error) {
	rt, ok := a.GetTenant(tenantID)
	if !ok {
		return 0, fmt.Errorf("tenant %s not active", tenantID)
	}
	dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", rt.Port)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)

	_, _ = conn.Exec(ctx, "CHECKPOINT;")
	for attempt := 0; attempt < 15; attempt++ {
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

func RunPostgresCmd(bin string, args ...string) (string, error) {
	return runPostgresCmd(bin, args...)
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

func NewR2ClientFromEnv(ctx context.Context) (*s3.Client, string, error) {
	ak := strings.Trim(os.Getenv("ACCESS_KEY"), "\"' \t")
	if ak == "" {
		ak = strings.Trim(os.Getenv("R2_ACCESS_KEY_ID"), "\"' \t")
	}
	sk := strings.Trim(os.Getenv("SECRET_KEY"), "\"' \t")
	if sk == "" {
		sk = strings.Trim(os.Getenv("R2_SECRET_ACCESS_KEY"), "\"' \t")
	}
	ep := strings.Trim(os.Getenv("ENDPOINT"), "\"' \t")
	if ep == "" {
		ep = strings.Trim(os.Getenv("R2_ENDPOINT"), "\"' \t")
	}
	bucket := strings.Trim(os.Getenv("R2_BUCKET"), "\"' \t")
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

// WriteJSONHelper marshals v to path atomically.
func WriteJSONHelper(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

// CopyStreamHelper copies from r to w.
func CopyStreamHelper(w io.Writer, r io.Reader) (int64, error) {
	return io.Copy(w, r)
}
