package control

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/jackc/pgx/v5"
)

func TestPhase11StalePostmasterPIDAndSIGKILLCrashRecovery(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/postgres"); err != nil {
		t.Skip("PostgreSQL 17 binaries not installed")
	}
	t.Setenv("AIR_WARM_POOL_SIZE", "0")

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	tmpDir, err := os.MkdirTemp("/tmp", "air-p11-crash-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	kms, err := crypto.LoadOrCreateEnvelopeKMS(filepath.Join(tmpDir, "master.key"))
	if err != nil {
		t.Fatalf("LoadOrCreateEnvelopeKMS: %v", err)
	}
	volDir := filepath.Join(tmpDir, "vol")
	metaDir := filepath.Join(tmpDir, "meta")
	layerDir := filepath.Join(tmpDir, "ps_layers")
	cacheDir := filepath.Join(tmpDir, "ps_cache")
	psSock := filepath.Join(tmpDir, "ps.sock")

	ag, err := cell.NewAgent(volDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	metaPort := freeTCPPort(t)
	cp, err := NewControlPlaneWithConfig(ctx, ControlPlaneConfig{
		MetaDir:  metaDir,
		PGBinDir: "/usr/lib/postgresql/17/bin",
		MetaPort: metaPort,
		Agent:    ag,
		KMS:      kms,
	})
	if err != nil {
		ag.Close()
		t.Fatalf("NewControlPlaneWithConfig: %v", err)
	}
	ps, err := pageserver.New(pageserver.Config{
		SocketPath:    psSock,
		LayerDir:      layerDir,
		CacheDir:      cacheDir,
		CacheMaxBytes: 32 * 1024 * 1024,
		KMS:           kms,
	})
	if err != nil {
		cp.Close()
		ag.Close()
		t.Fatalf("pageserver.New: %v", err)
	}
	if err := ps.Start(ctx); err != nil {
		ps.Close()
		cp.Close()
		ag.Close()
		t.Fatalf("ps.Start: %v", err)
	}
	cp.SetPageserver(ps)

	const tid = "t_p11_crash"
	prov, err := cp.ProvisionDatabase(ctx, "acct_p11_crash", "idem-p11-crash-1", tid)
	if err != nil {
		t.Fatalf("ProvisionDatabase: %v", err)
	}

	dsn := fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", prov.Password, prov.DirectPort, prov.Database)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE crash_ledger (id int PRIMARY KEY, cycle int, payload text);`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_ = conn.Close(ctx)

	expectedRows := 0
	for cycle := 1; cycle <= 3; cycle++ {
		port, _, pw, err := cp.WakeTenant(ctx, tid)
		if err != nil {
			t.Fatalf("cycle %d WakeTenant before insert: %v", cycle, err)
		}
		c, err := pgx.Connect(ctx, fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", pw, port, tid))
		if err != nil {
			t.Fatalf("cycle %d connect: %v", cycle, err)
		}
		for i := 0; i < 50; i++ {
			id := expectedRows + i + 1
			if _, err := c.Exec(ctx, `INSERT INTO crash_ledger VALUES ($1, $2, repeat('x', 128))`, id, cycle); err != nil {
				t.Fatalf("cycle %d insert %d: %v", cycle, id, err)
			}
		}
		expectedRows += 50
		_ = c.Close(ctx)

		// Read postmaster.pid and SIGKILL tenant postgres directly.
		pgDataDir, err := ag.TenantPGDataDir(tid)
		if err != nil {
			t.Fatalf("TenantPGDataDir: %v", err)
		}
		pidBytes, err := os.ReadFile(filepath.Join(pgDataDir, "postmaster.pid"))
		if err != nil {
			t.Fatalf("cycle %d read postmaster.pid: %v", cycle, err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(strings.Split(string(pidBytes), "\n")[0]))
		if err != nil || pid <= 0 {
			t.Fatalf("cycle %d invalid pid %q: %v", cycle, string(pidBytes), err)
		}
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
			t.Fatalf("cycle %d SIGKILL pid %d: %v", cycle, pid, err)
		}
		// Give kernel a moment to transition child to zombie before recovery.
		time.Sleep(150 * time.Millisecond)

		// WakeTenant must clean stale PID/socket lock, reap zombie, and recover without loss.
		port2, _, pw2, err := cp.WakeTenant(ctx, tid)
		if err != nil {
			t.Fatalf("cycle %d WakeTenant after SIGKILL: %v", cycle, err)
		}
		c2, err := pgx.Connect(ctx, fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", pw2, port2, tid))
		if err != nil {
			t.Fatalf("cycle %d connect after SIGKILL: %v", cycle, err)
		}
		var cnt int
		if err := c2.QueryRow(ctx, `SELECT count(*) FROM crash_ledger`).Scan(&cnt); err != nil {
			t.Fatalf("cycle %d count query: %v", cycle, err)
		}
		if cnt != expectedRows {
			t.Fatalf("cycle %d row count = %d, want %d", cycle, cnt, expectedRows)
		}
		// Verify critical system index (OID 2662 = pg_class_oid_index) is readable via index scan.
		if _, err := c2.Exec(ctx, `SET enable_seqscan = off`); err != nil {
			t.Fatalf("cycle %d SET enable_seqscan: %v", cycle, err)
		}
		var relname string
		if err := c2.QueryRow(ctx, `SELECT relname FROM pg_class WHERE oid = 2662`).Scan(&relname); err != nil {
			t.Fatalf("cycle %d system index 2662 query failed: %v", cycle, err)
		}
		if relname == "" {
			t.Fatalf("cycle %d empty relname for oid 2662", cycle)
		}
		_ = c2.Close(ctx)
	}

	// Simulate abrupt control plane crash: stop cleanly, then plant a stale meta/pgdata/postmaster.pid
	// and a stale tenant postmaster.pid, and verify fresh ControlPlane startup cleans both and recovers.
	_ = cp.ShutdownActiveTenants(ctx)
	_ = ps.FlushAll(ctx)
	_ = ps.Close()
	cp.Close()
	ag.Close()

	staleMetaPID := fmt.Sprintf("999999\n%s\n1700000000\n%d\n%s\n*\n  %d\nready   \n",
		filepath.Join(metaDir, "pgdata"), metaPort, filepath.Join(metaDir, "sock"), metaPort*100)
	if err := os.WriteFile(filepath.Join(metaDir, "pgdata", "postmaster.pid"), []byte(staleMetaPID), 0o600); err != nil {
		t.Fatalf("write stale meta postmaster.pid: %v", err)
	}
	pgDataDir, err := ag.TenantPGDataDir(tid)
	if err != nil {
		t.Fatalf("TenantPGDataDir: %v", err)
	}
	staleTenantPID := fmt.Sprintf("999998\n%s\n1700000000\n55432\n/tmp\n", pgDataDir)
	if err := os.WriteFile(filepath.Join(pgDataDir, "postmaster.pid"), []byte(staleTenantPID), 0o600); err != nil {
		t.Fatalf("write stale tenant postmaster.pid: %v", err)
	}

	ag2, err := cell.NewAgent(volDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent 2: %v", err)
	}
	defer ag2.Close()
	cp2, err := NewControlPlaneWithConfig(ctx, ControlPlaneConfig{
		MetaDir:  metaDir,
		PGBinDir: "/usr/lib/postgresql/17/bin",
		MetaPort: metaPort,
		Agent:    ag2,
		KMS:      kms,
	})
	if err != nil {
		t.Fatalf("NewControlPlaneWithConfig after stale meta PID: %v", err)
	}
	defer cp2.Close()
	ps2, err := pageserver.New(pageserver.Config{
		SocketPath:    psSock,
		LayerDir:      layerDir,
		CacheDir:      cacheDir,
		CacheMaxBytes: 32 * 1024 * 1024,
		KMS:           kms,
	})
	if err != nil {
		t.Fatalf("pageserver.New 2: %v", err)
	}
	if err := ps2.Start(ctx); err != nil {
		t.Fatalf("ps2.Start: %v", err)
	}
	defer ps2.Close()
	cp2.SetPageserver(ps2)

	portFinal, _, pwFinal, err := cp2.WakeTenant(ctx, tid)
	if err != nil {
		t.Fatalf("WakeTenant after stale PIDs: %v", err)
	}
	cFinal, err := pgx.Connect(ctx, fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", pwFinal, portFinal, tid))
	if err != nil {
		t.Fatalf("connect after restart: %v", err)
	}
	defer cFinal.Close(ctx)
	var cntFinal int
	if err := cFinal.QueryRow(ctx, `SELECT count(*) FROM crash_ledger`).Scan(&cntFinal); err != nil {
		t.Fatalf("final count query: %v", err)
	}
	if cntFinal != expectedRows {
		t.Fatalf("final row count = %d, want %d", cntFinal, expectedRows)
	}
}

func TestPhase11ShutdownDurabilityPreservesDBAndWrites(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/postgres"); err != nil {
		t.Skip("PostgreSQL 17 binaries not installed")
	}
	t.Setenv("AIR_WARM_POOL_SIZE", "0")

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tmpDir, err := os.MkdirTemp("/tmp", "air-p11-shut-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	kms, err := crypto.LoadOrCreateEnvelopeKMS(filepath.Join(tmpDir, "master.key"))
	if err != nil {
		t.Fatalf("LoadOrCreateEnvelopeKMS: %v", err)
	}
	volDir := filepath.Join(tmpDir, "vol")
	metaDir := filepath.Join(tmpDir, "meta")
	layerDir := filepath.Join(tmpDir, "ps_layers")
	cacheDir := filepath.Join(tmpDir, "ps_cache")
	psSock := filepath.Join(tmpDir, "ps.sock")
	metaPort := freeTCPPort(t)

	ag, err := cell.NewAgent(volDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	cp, err := NewControlPlaneWithConfig(ctx, ControlPlaneConfig{
		MetaDir:  metaDir,
		PGBinDir: "/usr/lib/postgresql/17/bin",
		MetaPort: metaPort,
		Agent:    ag,
		KMS:      kms,
	})
	if err != nil {
		ag.Close()
		t.Fatalf("NewControlPlaneWithConfig: %v", err)
	}
	ps, err := pageserver.New(pageserver.Config{
		SocketPath:    psSock,
		LayerDir:      layerDir,
		CacheDir:      cacheDir,
		CacheMaxBytes: 32 * 1024 * 1024,
		KMS:           kms,
	})
	if err != nil {
		cp.Close()
		ag.Close()
		t.Fatalf("pageserver.New: %v", err)
	}
	if err := ps.Start(ctx); err != nil {
		ps.Close()
		cp.Close()
		ag.Close()
		t.Fatalf("ps.Start: %v", err)
	}
	cp.SetPageserver(ps)

	const tid = "t_p11_shut"
	prov, err := cp.ProvisionDatabase(ctx, "acct_p11_shut", "idem-p11-shut-1", tid)
	if err != nil {
		t.Fatalf("ProvisionDatabase: %v", err)
	}
	pgDataDir, err := ag.TenantPGDataDir(tid)
	if err != nil {
		t.Fatalf("TenantPGDataDir: %v", err)
	}
	sockDir := filepath.Join(filepath.Dir(pgDataDir), "sock")
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@localhost:%d/postgres?host=%s", prov.DirectPort, sockDir))
	if err != nil {
		t.Fatalf("pgx.Connect superuser: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE DATABASE app_db OWNER app_user`); err != nil {
		t.Fatalf("CREATE DATABASE app_db: %v", err)
	}
	_ = conn.Close(ctx)

	connDB, err := pgx.Connect(ctx, fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/app_db?sslmode=disable", prov.Password, prov.DirectPort))
	if err != nil {
		t.Fatalf("pgx.Connect app_db: %v", err)
	}
	if _, err := connDB.Exec(ctx, `CREATE TABLE items (id int PRIMARY KEY, note text); INSERT INTO items SELECT g, 'ok_' || g FROM generate_series(1, 200) g;`); err != nil {
		t.Fatalf("seed app_db: %v", err)
	}
	_ = connDB.Close(ctx)

	// Perform ordered graceful shutdown (matching cmd/air-server/main.go).
	if err := cp.ShutdownActiveTenants(ctx); err != nil {
		t.Fatalf("ShutdownActiveTenants: %v", err)
	}
	if err := ps.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	_ = ps.Close()
	cp.Close()
	ag.Close()

	// Restart fresh stack on same state dirs.
	ag2, err := cell.NewAgent(volDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent 2: %v", err)
	}
	defer ag2.Close()
	cp2, err := NewControlPlaneWithConfig(ctx, ControlPlaneConfig{
		MetaDir:  metaDir,
		PGBinDir: "/usr/lib/postgresql/17/bin",
		MetaPort: metaPort,
		Agent:    ag2,
		KMS:      kms,
	})
	if err != nil {
		t.Fatalf("NewControlPlaneWithConfig 2: %v", err)
	}
	defer cp2.Close()
	ps2, err := pageserver.New(pageserver.Config{
		SocketPath:    psSock,
		LayerDir:      layerDir,
		CacheDir:      cacheDir,
		CacheMaxBytes: 32 * 1024 * 1024,
		KMS:           kms,
	})
	if err != nil {
		t.Fatalf("pageserver.New 2: %v", err)
	}
	if err := ps2.Start(ctx); err != nil {
		t.Fatalf("ps2.Start: %v", err)
	}
	defer ps2.Close()
	cp2.SetPageserver(ps2)

	port2, _, pw2, err := cp2.WakeTenant(ctx, tid)
	if err != nil {
		t.Fatalf("WakeTenant after shutdown: %v", err)
	}
	connDB2, err := pgx.Connect(ctx, fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/app_db?sslmode=disable", pw2, port2))
	if err != nil {
		t.Fatalf("connect app_db after restart: %v", err)
	}
	defer connDB2.Close(ctx)
	var cnt int
	if err := connDB2.QueryRow(ctx, `SELECT count(*) FROM items`).Scan(&cnt); err != nil {
		t.Fatalf("SELECT count(*) FROM items: %v", err)
	}
	if cnt != 200 {
		t.Fatalf("items count after restart = %d, want 200", cnt)
	}
}
