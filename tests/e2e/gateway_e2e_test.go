package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/control"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeTCPAddr: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func TestGateway_Phase14A_EndToEndProtocolAndIsolation(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/initdb"); err != nil {
		t.Fatalf("PostgreSQL 17 is required: %v", err)
	}
	gwBin, err := filepath.Abs("../../gateway/air-gateway")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(gwBin); err != nil {
		t.Fatalf("air-gateway binary not found at %s (run make -C gateway all): %v", gwBin, err)
	}

	ctx := context.Background()
	rootDir, err := os.MkdirTemp("/tmp", "air_gw_e2e_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootDir)

	metaDir := filepath.Join(rootDir, "meta")
	kms, err := crypto.LoadOrCreateEnvelopeKMS(filepath.Join(rootDir, "master.key"))
	if err != nil {
		t.Fatalf("LoadOrCreateEnvelopeKMS: %v", err)
	}

	ag, err := cell.NewAgent(rootDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer func() {
		_ = ag.StopAndPurgeTenant("gw_tenant_a")
		_ = ag.StopAndPurgeTenant("gw_tenant_b")
		ag.Close()
	}()

	cp, err := control.NewControlPlane(ctx, metaDir, "/usr/lib/postgresql/17/bin", 19842, ag, kms)
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	defer cp.Close()

	keyA, err := cp.CreateAPIKey(ctx, "acct_a", "acct_a key")
	if err != nil {
		t.Fatalf("CreateAPIKey acct_a: %v", err)
	}
	keyB, err := cp.CreateAPIKey(ctx, "acct_b", "acct_b key")
	if err != nil {
		t.Fatalf("CreateAPIKey acct_b: %v", err)
	}
	httpSrv := httptest.NewServer(cp.HTTPHandler())
	defer httpSrv.Close()

	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	grpcSrv := control.NewGRPCServer(cp)
	go func() { _ = grpcSrv.Serve(grpcLis) }()
	defer grpcSrv.Stop()

	// Provision two isolated tenants.
	provA := httpCreateDatabase(t, httpSrv.URL, keyA.Token, "idem-gw-a", "gw_tenant_a")
	provB := httpCreateDatabase(t, httpSrv.URL, keyB.Token, "idem-gw-b", "gw_tenant_b")

	gwPGAddr := freeTCPAddr(t)
	gwAdminAddr := freeTCPAddr(t)

	gwCmd := exec.Command(gwBin,
		"--gateway-addr", gwPGAddr,
		"--gateway-admin-addr", gwAdminAddr,
		"--db-grpc-addr", grpcLis.Addr().String(),
		"--internal-token", "gw-test-token",
		"--backend-host", "127.0.0.1",
		"--pool-mode",
		"--pool-max-backends", "4",
		"--max-client-conns", "64",
		"--max-tenant-client-conns", "32",
	)
	gwOut, err := os.Create(filepath.Join(rootDir, "gateway.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer gwOut.Close()
	gwCmd.Stdout = gwOut
	gwCmd.Stderr = gwOut
	if err := gwCmd.Start(); err != nil {
		t.Fatalf("start air-gateway: %v", err)
	}
	defer func() {
		if gwCmd.Process != nil {
			_ = gwCmd.Process.Signal(syscall.SIGTERM)
			_, _ = gwCmd.Process.Wait()
		}
	}()

	// Wait for /healthz
	healthURL := "http://" + gwAdminAddr + "/healthz"
	ready := false
	for i := 0; i < 60; i++ {
		resp, err := http.Get(healthURL)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				ready = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		logBytes, _ := os.ReadFile(filepath.Join(rootDir, "gateway.log"))
		t.Fatalf("air-gateway failed to become healthy; log:\n%s", string(logBytes))
	}

	dsnA := fmt.Sprintf("postgres://app_user:%s@%s/gw_tenant_a?sslmode=disable", provA.Password, gwPGAddr)
	dsnB := fmt.Sprintf("postgres://app_user:%s@%s/gw_tenant_b?sslmode=disable", provB.Password, gwPGAddr)
	dsnATLS := fmt.Sprintf("postgres://app_user:%s@%s/gw_tenant_a?sslmode=require", provA.Password, gwPGAddr)

	// 1. psql CLI: plaintext & TLS + CRUD + COPY FROM STDIN
	if psqlPath, lookErr := exec.LookPath("psql"); lookErr == nil {
		out, err := exec.Command(psqlPath, dsnA, "-v", "ON_ERROR_STOP=1", "-c",
			"CREATE TABLE t_items (id int PRIMARY KEY, v text); INSERT INTO t_items VALUES (1, 'one');").CombinedOutput()
		if err != nil {
			t.Fatalf("psql create/insert failed: %v\n%s", err, string(out))
		}
		// TLS (sslmode=require) to localhost must succeed (regression test for SNI localhost bug)
		out, err = exec.Command(psqlPath, dsnATLS, "-tAc", "SELECT v FROM t_items WHERE id=1").CombinedOutput()
		if err != nil || strings.TrimSpace(string(out)) != "one" {
			t.Fatalf("psql TLS query failed: err=%v out=%q", err, string(out))
		}
		// COPY FROM STDIN through gateway
		copyCmd := exec.Command(psqlPath, dsnA, "-v", "ON_ERROR_STOP=1", "-c", "COPY t_items (id, v) FROM STDIN;")
		copyCmd.Stdin = strings.NewReader("2\ttwo\n3\tthree\n\\.\n")
		if out, err := copyCmd.CombinedOutput(); err != nil {
			t.Fatalf("psql COPY FROM STDIN failed: %v\n%s", err, string(out))
		}
	}

	// 2. pgx driver: CRUD, transactions & rollback, prepared statements
	connA, err := pgx.Connect(ctx, dsnA)
	if err != nil {
		t.Fatalf("pgx.Connect A: %v", err)
	}
	var cnt int
	if err := connA.QueryRow(ctx, "SELECT count(*) FROM t_items").Scan(&cnt); err != nil || cnt != 3 {
		t.Fatalf("expected 3 rows after COPY, got cnt=%d err=%v", cnt, err)
	}

	// UPDATE + DELETE
	if _, err := connA.Exec(ctx, "UPDATE t_items SET v = $1 WHERE id = $2", "two-updated", 2); err != nil {
		t.Fatalf("pgx UPDATE: %v", err)
	}
	if _, err := connA.Exec(ctx, "DELETE FROM t_items WHERE id = $1", 3); err != nil {
		t.Fatalf("pgx DELETE: %v", err)
	}

	// Transaction rollback
	tx, err := connA.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin rollback tx: %v", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO t_items (id, v) VALUES (99, 'rolled-back')"); err != nil {
		t.Fatalf("tx Exec: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := connA.QueryRow(ctx, "SELECT count(*) FROM t_items WHERE id = 99").Scan(&cnt); err != nil || cnt != 0 {
		t.Fatalf("rollback failed: cnt=%d err=%v", cnt, err)
	}

	// SQL PREPARE / EXECUTE / DEALLOCATE
	if _, err := connA.Exec(ctx, "PREPARE p_lookup (int) AS SELECT v FROM t_items WHERE id = $1"); err != nil {
		t.Fatalf("SQL PREPARE: %v", err)
	}
	var gotV string
	if err := connA.QueryRow(ctx, "EXECUTE p_lookup(2)").Scan(&gotV); err != nil || gotV != "two-updated" {
		t.Fatalf("SQL EXECUTE: got=%q err=%v", gotV, err)
	}
	if _, err := connA.Exec(ctx, "DEALLOCATE p_lookup"); err != nil {
		t.Fatalf("SQL DEALLOCATE: %v", err)
	}
	// DEALLOCATE non-existent must return real PG error 26000
	if _, err := connA.Exec(ctx, "DEALLOCATE p_lookup"); err == nil {
		t.Fatalf("expected error on duplicate DEALLOCATE, got nil")
	}
	_ = connA.Close(ctx)

	// 3. Tenant isolation & concurrent clients across A and B
	connB, err := pgx.Connect(ctx, dsnB)
	if err != nil {
		t.Fatalf("pgx.Connect B: %v", err)
	}
	if _, err := connB.Exec(ctx, "CREATE TABLE t_items (id int PRIMARY KEY, v text); INSERT INTO t_items VALUES (1, 'tenant-b-only');"); err != nil {
		t.Fatalf("setup tenant B: %v", err)
	}
	_ = connB.Close(ctx)

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			c, err := pgx.Connect(ctx, dsnA)
			if err != nil {
				errs <- fmt.Errorf("concurrent A connect: %w", err)
				return
			}
			defer c.Close(ctx)
			var v string
			if err := c.QueryRow(ctx, "SELECT v FROM t_items WHERE id = 1").Scan(&v); err != nil || v != "one" {
				errs <- fmt.Errorf("concurrent A isolation mismatch: got=%q err=%v", v, err)
			}
		}()
		go func() {
			defer wg.Done()
			c, err := pgx.Connect(ctx, dsnB)
			if err != nil {
				errs <- fmt.Errorf("concurrent B connect: %w", err)
				return
			}
			defer c.Close(ctx)
			var v string
			if err := c.QueryRow(ctx, "SELECT v FROM t_items WHERE id = 1").Scan(&v); err != nil || v != "tenant-b-only" {
				errs <- fmt.Errorf("concurrent B isolation mismatch: got=%q err=%v", v, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}

	// 4. Invalid credentials, superuser rejection, unknown tenant
	badPassDSN := fmt.Sprintf("postgres://app_user:wrongpassword@%s/gw_tenant_a?sslmode=disable", gwPGAddr)
	if _, err := pgx.Connect(ctx, badPassDSN); err == nil {
		t.Fatalf("expected wrong password to fail")
	} else {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "28P01" {
			t.Fatalf("expected SQLSTATE 28P01, got: %v", err)
		}
	}

	// Cross-tenant credential attempt (B's password against A)
	crossDSN := fmt.Sprintf("postgres://app_user:%s@%s/gw_tenant_a?sslmode=disable", provB.Password, gwPGAddr)
	if _, err := pgx.Connect(ctx, crossDSN); err == nil {
		t.Fatalf("expected cross-tenant password to fail")
	}

	// Superuser postgres rejected
	pgUserDSN := fmt.Sprintf("postgres://postgres:%s@%s/gw_tenant_a?sslmode=disable", provA.Password, gwPGAddr)
	if _, err := pgx.Connect(ctx, pgUserDSN); err == nil {
		t.Fatalf("expected postgres superuser to be rejected")
	}

	// Unknown tenant rejected with 3D000
	unknownDSN := fmt.Sprintf("postgres://app_user:pw@%s/gw_nonexistent?sslmode=disable", gwPGAddr)
	if _, err := pgx.Connect(ctx, unknownDSN); err == nil {
		t.Fatalf("expected unknown tenant to fail")
	} else {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "3D000" {
			t.Fatalf("expected SQLSTATE 3D000, got: %v", err)
		}
	}

	// 5. Client disconnect mid-transaction leaves pool clean
	dirtyConn, err := pgx.Connect(ctx, dsnA)
	if err != nil {
		t.Fatalf("dirtyConn connect: %v", err)
	}
	if _, err := dirtyConn.Exec(ctx, "BEGIN; INSERT INTO t_items VALUES (88, 'dirty-uncommitted');"); err != nil {
		t.Fatalf("dirtyConn begin: %v", err)
	}
	_ = dirtyConn.PgConn().Conn().Close() // abrupt TCP close without Terminate
	time.Sleep(100 * time.Millisecond)

	cleanConn, err := pgx.Connect(ctx, dsnA)
	if err != nil {
		t.Fatalf("cleanConn after dirty close: %v", err)
	}
	if err := cleanConn.QueryRow(ctx, "SELECT count(*) FROM t_items WHERE id = 88").Scan(&cnt); err != nil || cnt != 0 {
		t.Fatalf("dirty uncommitted row leaked: cnt=%d err=%v", cnt, err)
	}
	_ = cleanConn.Close(ctx)

	// 6. Suspended tenant auto-wakes on connect
	if err := cp.SuspendTenant(ctx, "gw_tenant_b", 2*time.Second, nil); err != nil {
		t.Fatalf("SuspendTenant B: %v", err)
	}
	wokenConn, err := pgx.Connect(ctx, dsnB)
	if err != nil {
		t.Fatalf("connect to suspended tenant B (expected auto-wake): %v", err)
	}
	var bv string
	if err := wokenConn.QueryRow(ctx, "SELECT v FROM t_items WHERE id = 1").Scan(&bv); err != nil || bv != "tenant-b-only" {
		t.Fatalf("suspended wake query failed: v=%q err=%v", bv, err)
	}
	_ = wokenConn.Close(ctx)

	// 7. Metrics endpoint check
	req, _ := http.NewRequest("GET", "http://"+gwAdminAddr+"/internal/v1/metrics", nil)
	req.Header.Set("X-Air-Internal-Token", "gw-test-token")
	mresp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /internal/v1/metrics: %v", err)
	}
	mbody, _ := io.ReadAll(mresp.Body)
	_ = mresp.Body.Close()
	if mresp.StatusCode != 200 || !strings.Contains(string(mbody), "\"auth_failures\":") || !strings.Contains(string(mbody), "\"query_count\":") {
		t.Fatalf("unexpected metrics response (%d): %s", mresp.StatusCode, string(mbody))
	}

	// 7b. Performance & resource baseline (warm connect, concurrent query throughput, FD/RSS)
	var connLat []time.Duration
	for i := 0; i < 20; i++ {
		t0 := time.Now()
		c, err := pgx.Connect(ctx, dsnA)
		if err != nil {
			t.Fatalf("baseline connect %d: %v", i, err)
		}
		var one int
		if err := c.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
			t.Fatalf("baseline SELECT 1: %v", err)
		}
		_ = c.Close(ctx)
		connLat = append(connLat, time.Since(t0))
	}
	sort.Slice(connLat, func(i, j int) bool { return connLat[i] < connLat[j] })

	const workers = 16
	const perWorker = 50
	qLat := make([]time.Duration, workers*perWorker)
	var qwg sync.WaitGroup
	qerrs := make(chan error, workers)
	tBenchStart := time.Now()
	for w := 0; w < workers; w++ {
		w := w
		qwg.Add(1)
		go func() {
			defer qwg.Done()
			c, err := pgx.Connect(ctx, dsnA)
			if err != nil {
				qerrs <- err
				return
			}
			defer c.Close(ctx)
			for i := 0; i < perWorker; i++ {
				t0 := time.Now()
				var one int
				if err := c.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
					qerrs <- err
					return
				}
				qLat[w*perWorker+i] = time.Since(t0)
			}
		}()
	}
	qwg.Wait()
	benchElapsed := time.Since(tBenchStart)
	close(qerrs)
	for e := range qerrs {
		t.Fatalf("baseline worker error: %v", e)
	}
	sort.Slice(qLat, func(i, j int) bool { return qLat[i] < qLat[j] })
	fds, _ := os.ReadDir(fmt.Sprintf("/proc/%d/fd", gwCmd.Process.Pid))
	statusBytes, _ := os.ReadFile(fmt.Sprintf("/proc/%d/status", gwCmd.Process.Pid))
	vmRSS := ""
	for _, line := range strings.Split(string(statusBytes), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			vmRSS = strings.TrimSpace(line)
			break
		}
	}
	qps := float64(workers*perWorker) / benchElapsed.Seconds()
	t.Logf("BASELINE warm_connect_select_p50=%v p95=%v | concurrent_16x50_qps=%.1f query_p50=%v query_p95=%v | fds=%d %s",
		connLat[len(connLat)/2], connLat[(len(connLat)*95)/100], qps, qLat[len(qLat)/2], qLat[(len(qLat)*95)/100], len(fds), vmRSS)

	// 8. Control Plane unavailability: stop gRPC server and verify unknown tenant returns 57P03/08006
	grpcSrv.Stop()
	if _, err := pgx.Connect(ctx, unknownDSN); err == nil {
		t.Fatalf("expected error when control plane is unavailable")
	}

	// 9. Graceful SIGTERM shutdown
	if err := gwCmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM gateway: %v", err)
	}
	if err := gwCmd.Wait(); err != nil {
		t.Fatalf("air-gateway exited with error on SIGTERM: %v", err)
	}
}
