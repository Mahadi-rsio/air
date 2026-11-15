package e2e_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/control"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/jackc/pgx/v5"
)

func reserveFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve free port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func startExternalPG17(t *testing.T, baseDir string) string {
	t.Helper()
	pgBin := "/usr/lib/postgresql/17/bin"
	pgData := filepath.Join(baseDir, "pgdata")
	sockDir := filepath.Join(baseDir, "sock")
	logFile := filepath.Join(baseDir, "ext-pg.log")
	if err := os.MkdirAll(pgData, 0o700); err != nil {
		t.Fatalf("mkdir pgdata: %v", err)
	}
	if err := os.MkdirAll(sockDir, 0o755); err != nil {
		t.Fatalf("mkdir sock: %v", err)
	}

	port := reserveFreePort(t)
	runPG := func(bin string, args ...string) {
		fullBin := filepath.Join(pgBin, bin)
		var cmd *exec.Cmd
		if os.Geteuid() == 0 {
			cmd = exec.Command("runuser", append([]string{"-u", "postgres", "--", fullBin}, args...)...)
		} else {
			cmd = exec.Command(fullBin, args...)
		}
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s %v failed: %v\n%s", bin, args, err, string(out))
		}
	}

	runPG("initdb", "-D", pgData, "-U", "postgres", "--auth=trust", "--data-checksums", "--no-instructions")
	conf := fmt.Sprintf("listen_addresses = '127.0.0.1'\nport = %d\nunix_socket_directories = '%s'\nmax_connections = 100\nshared_buffers = 32MB\nssl = off\n", port, sockDir)
	if err := os.WriteFile(filepath.Join(pgData, "postgresql.conf"), []byte(conf), 0o600); err != nil {
		t.Fatalf("write postgresql.conf: %v", err)
	}
	runPG("pg_ctl", "-D", pgData, "-l", logFile, "-w", "-t", "15", "start")
	t.Cleanup(func() {
		fullBin := filepath.Join(pgBin, "pg_ctl")
		var cmd *exec.Cmd
		if os.Geteuid() == 0 {
			cmd = exec.Command("runuser", "-u", "postgres", "--", fullBin, "-D", pgData, "-m", "fast", "-w", "-t", "10", "stop")
		} else {
			cmd = exec.Command(fullBin, "-D", pgData, "-m", "fast", "-w", "-t", "10", "stop")
		}
		_ = cmd.Run()
	})

	return fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", port)
}

func TestPhaseG_ExternalMetaDSN_ControlPlaneLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	root, err := os.MkdirTemp("", "air-pg-ext-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	extDSN := startExternalPG17(t, filepath.Join(root, "ext"))

	kms, err := crypto.LoadOrCreateEnvelopeKMS(filepath.Join(root, "master.key"))
	if err != nil {
		t.Fatalf("LoadOrCreateEnvelopeKMS: %v", err)
	}
	ag, err := cell.NewAgent(filepath.Join(root, "cell-vol"), "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer ag.Close()

	// Intentionally leave MetaDir empty to prove no local single-node MetaDir is bootstrapped.
	cp, err := control.NewControlPlaneWithConfig(ctx, control.ControlPlaneConfig{
		MetaDSN:  extDSN,
		PGBinDir: "/usr/lib/postgresql/17/bin",
		Agent:    ag,
		KMS:      kms,
	})
	if err != nil {
		t.Fatalf("NewControlPlaneWithConfig(MetaDSN): %v", err)
	}

	// Verify schema migrations were applied on the external PG17 cluster.
	extConn, err := pgx.Connect(ctx, extDSN)
	if err != nil {
		t.Fatalf("connect external PG17: %v", err)
	}
	var schemaVer int
	if err := extConn.QueryRow(ctx, `SELECT MAX(version) FROM schema_migrations`).Scan(&schemaVer); err != nil {
		t.Fatalf("query schema_migrations: %v", err)
	}
	_ = extConn.Close(ctx)
	if schemaVer < 2 {
		t.Fatalf("expected schema_migrations >= 2 on external MetaDSN, got %d", schemaVer)
	}

	// Provision tenant and verify idempotent replay.
	resp1, err := cp.ProvisionDatabase(ctx, "acct_ext", "idem-ext-1", "t_ext_ha_01")
	if err != nil {
		t.Fatalf("ProvisionDatabase: %v", err)
	}
	if resp1.State != "ACTIVE" {
		t.Fatalf("unexpected provision response: resp=%+v", resp1)
	}
	resp2, err := cp.ProvisionDatabase(ctx, "acct_ext", "idem-ext-1", "t_ext_ha_01")
	if err != nil || resp2.TenantID != resp1.TenantID || resp2.Password != resp1.Password || resp2.DirectPort != resp1.DirectPort {
		t.Fatalf("expected idempotent replay matching original response, got resp1=%+v resp2=%+v err=%v", resp1, resp2, err)
	}

	// Verify tenant DB is live.
	tenantDSN := fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", resp1.Password, resp1.DirectPort, resp1.Database)
	tConn, err := pgx.Connect(ctx, tenantDSN)
	if err != nil {
		t.Fatalf("connect provisioned tenant: %v", err)
	}
	var one int
	if err := tConn.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("tenant SELECT 1 failed: %v", err)
	}
	_ = tConn.Close(ctx)

	// Simulate an abrupt process crash mid-provisioning (after ClaimCluster + CLAIMED state persisted, before rollback).
	if _, err := ag.ClaimCluster("t_ext_crash_02"); err != nil {
		t.Fatalf("ClaimCluster(t_ext_crash_02): %v", err)
	}
	crashConn, err := pgx.Connect(ctx, extDSN)
	if err != nil {
		t.Fatalf("connect external PG17 for crash simulation: %v", err)
	}
	if _, err := crashConn.Exec(ctx, `
		INSERT INTO provisioning_ops (account_id, tenant_id, idempotency_key, state)
		VALUES ('acct_ext', 't_ext_crash_02', 'idem-ext-crash', 'CLAIMED')
	`); err != nil {
		t.Fatalf("insert simulated crashed op: %v", err)
	}
	_ = crashConn.Close(ctx)

	// Closing ControlPlane must NOT stop the external HA metadata PostgreSQL instance.
	cp.Close()

	extPing, err := pgx.Connect(ctx, extDSN)
	if err != nil {
		t.Fatalf("external PG17 should remain running after ControlPlane.Close(): %v", err)
	}
	_ = extPing.Close(ctx)

	// Reconnect a new ControlPlane to the same external MetaDSN and run crash recovery.
	cp2, err := control.NewControlPlaneWithConfig(ctx, control.ControlPlaneConfig{
		MetaDSN:  extDSN,
		PGBinDir: "/usr/lib/postgresql/17/bin",
		Agent:    ag,
		KMS:      kms,
	})
	if err != nil {
		t.Fatalf("reconnect ControlPlaneWithConfig: %v", err)
	}
	defer cp2.Close()

	recovered, err := cp2.RecoverProvisioningOperations(ctx)
	if err != nil {
		t.Fatalf("RecoverProvisioningOperations: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1 recovered provisioning op, got %d", recovered)
	}
	if _, err := os.Stat(filepath.Join(root, "cell-vol/tenants/claiming/t_ext_crash_02")); !os.IsNotExist(err) {
		t.Fatalf("expected orphaned claiming directory to be purged during recovery, stat err=%v", err)
	}

	if err := cp2.DeleteDatabase(ctx, "acct_ext", "t_ext_ha_01"); err != nil {
		t.Fatalf("DeleteDatabase: %v", err)
	}
}

func TestPhaseG_LivePG17_CgroupV2AndProcessTreeTeardown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	root, err := os.MkdirTemp("", "air-pg-cg-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	kms, err := crypto.LoadOrCreateEnvelopeKMS(filepath.Join(root, "master.key"))
	if err != nil {
		t.Fatalf("LoadOrCreateEnvelopeKMS: %v", err)
	}
	volDir := filepath.Join(root, "cell-vol")
	ag, err := cell.NewAgent(volDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer ag.Close()

	// Use real /sys/fs/cgroup when delegated/writable; otherwise fall back to a temp cgroup root.
	cgRoot := "/sys/fs/cgroup"
	probeDir := filepath.Join(cgRoot, fmt.Sprintf("air_probe_%d", os.Getpid()))
	if err := os.Mkdir(probeDir, 0o755); err != nil {
		cgRoot = filepath.Join(root, "fake-cgroupfs")
		if mkErr := os.MkdirAll(cgRoot, 0o755); mkErr != nil {
			t.Fatalf("mkdir fallback cgroup root: %v", mkErr)
		}
		ag.SetCgroupRoot(cgRoot)
	} else {
		_ = os.Remove(probeDir)
	}

	tenantID := fmt.Sprintf("t_pg_cg_%d", os.Getpid())
	if _, err := ag.ClaimCluster(tenantID); err != nil {
		t.Fatalf("ClaimCluster: %v", err)
	}
	t.Cleanup(func() { _ = ag.StopAndPurgeTenant(tenantID) })

	rt, err := ag.ActivateCluster(tenantID, "TestSecret123!", 1)
	if err != nil {
		t.Fatalf("ActivateCluster: %v", err)
	}

	expectedCG := filepath.Join(cgRoot, "air_tenant_"+tenantID)
	if rt.CgroupPath != expectedCG {
		t.Fatalf("expected CgroupPath %q, got %q", expectedCG, rt.CgroupPath)
	}

	// Verify cgroup v2 limits & postmaster PID attachment.
	procsRaw, err := os.ReadFile(filepath.Join(expectedCG, "cgroup.procs"))
	if err != nil {
		t.Fatalf("read cgroup.procs: %v", err)
	}
	if !strings.Contains(string(procsRaw), strconv.Itoa(rt.PID)) {
		t.Fatalf("expected cgroup.procs to contain postmaster PID %d, got %q", rt.PID, string(procsRaw))
	}
	swapRaw, err := os.ReadFile(filepath.Join(expectedCG, "memory.swap.max"))
	if err != nil {
		t.Fatalf("read memory.swap.max: %v", err)
	}
	if strings.TrimSpace(string(swapRaw)) != "0" {
		t.Fatalf("expected memory.swap.max=0, got %q", strings.TrimSpace(string(swapRaw)))
	}

	// Open a live PG17 backend connection so PG forks a backend child process inside the tenant slice.
	dsn := fmt.Sprintf("postgres://app_user:TestSecret123!@127.0.0.1:%d/%s?sslmode=disable", rt.Port, tenantID)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect tenant PG17: %v", err)
	}
	var one int
	if err := conn.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("SELECT 1 failed: %v", err)
	}

	// RestartTenant must re-attach the new postmaster PID to the cgroup slice.
	_ = conn.Close(ctx)
	rtRestart, err := ag.RestartTenant(tenantID)
	if err != nil {
		t.Fatalf("RestartTenant: %v", err)
	}
	if rtRestart.CgroupPath != expectedCG {
		t.Fatalf("expected CgroupPath %q after restart, got %q", expectedCG, rtRestart.CgroupPath)
	}

	// ReadoptRunningTenants on a fresh Agent must also preserve/re-attach cgroup membership.
	ag.Close()
	ag2, err := cell.NewAgent(volDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent(readopt): %v", err)
	}
	ag2.SetCgroupRoot(cgRoot)
	if cgRoot != "/sys/fs/cgroup" {
		// Re-run readopt against custom cgRoot if fallback was used.
		ag2.Close()
		ag2, err = cell.NewAgent(volDir, "/usr/lib/postgresql/17/bin", kms)
		if err != nil {
			t.Fatalf("NewAgent(readopt2): %v", err)
		}
		ag2.SetCgroupRoot(cgRoot)
		if _, err := ag2.StartOrAdoptTenant(tenantID, 1); err != nil {
			t.Fatalf("StartOrAdoptTenant: %v", err)
		}
	}
	defer ag2.Close()

	rtAdopted, ok := ag2.GetTenant(tenantID)
	if !ok || rtAdopted.PID != rtRestart.PID {
		t.Fatalf("expected readopted tenant PID %d, got ok=%v rt=%+v", rtRestart.PID, ok, rtAdopted)
	}

	// StopTenantClean must terminate all processes, remove the cgroup dir, and release agent.lock flock.
	if err := ag2.StopTenantClean(tenantID); err != nil {
		t.Fatalf("StopTenantClean: %v", err)
	}
	if err := syscall.Kill(rtAdopted.PID, 0); err == nil {
		t.Fatalf("expected postmaster PID %d to be terminated after StopTenantClean", rtAdopted.PID)
	}
	if _, err := os.Stat(expectedCG); !os.IsNotExist(err) {
		t.Fatalf("expected cgroup dir %q to be removed after StopTenantClean, err=%v", expectedCG, err)
	}

	// Verify flock on agent.lock is released after process termination.
	lockPath := filepath.Join(volDir, "tenants/active", tenantID, "agent.lock")
	lf, err := os.OpenFile(lockPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open agent.lock: %v", err)
	}
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lf.Close()
		t.Fatalf("expected agent.lock flock to be released after StopTenantClean: %v", err)
	}
	_ = syscall.Flock(int(lf.Fd()), syscall.LOCK_UN)
	_ = lf.Close()
}
