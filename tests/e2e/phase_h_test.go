package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/control"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/jackc/pgx/v5"
)

func TestPhaseH_TransactionPooler_ConcurrencyAndSessionIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rootDir, err := os.MkdirTemp("/tmp", "air_phase_h1_")
	if err != nil {
		t.Fatal(err)
	}
	_ = os.Chmod(rootDir, 0o755)
	defer os.RemoveAll(rootDir)
	volDir := filepath.Join(rootDir, "vol")
	metaDir := filepath.Join(rootDir, "meta")
	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatalf("NewEnvelopeKMS: %v", err)
	}
	ag, err := cell.NewAgent(volDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer func() {
		_ = ag.StopAndPurgeTenant("t_pool_iso")
		ag.Close()
	}()
	cp, err := control.NewControlPlane(ctx, metaDir, "/usr/lib/postgresql/17/bin", 16471, ag, kms)
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	defer cp.Close()

	prov, err := cp.ProvisionDatabase(ctx, "acct_pool", "idem-pool-1", "t_pool_iso")
	if err != nil {
		t.Fatalf("ProvisionDatabase: %v", err)
	}

	// 1. Wrong password must be rejected with SQLSTATE 28P01
	badDSN := fmt.Sprintf("postgres://app_user:wrong-secret@127.0.0.1:%d/t_pool_iso?sslmode=disable", prov.DirectPort)
	if badConn, err := pgx.Connect(ctx, badDSN); err == nil {
		_ = badConn.Close(ctx)
		t.Fatalf("expected wrong password to fail")
	} else if !strings.Contains(err.Error(), "28P01") {
		t.Fatalf("expected SQLSTATE 28P01 on wrong password, got: %v", err)
	}

	goodDSN := fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/t_pool_iso?sslmode=disable", prov.Password, prov.DirectPort)

	// Setup test table + verify COPY FROM STDIN
	setupConn, err := pgx.Connect(ctx, goodDSN)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	if _, err := setupConn.Exec(ctx, "CREATE TABLE pool_items (id int PRIMARY KEY, worker int)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	copyRows := [][]any{{1001, 0}, {1002, 0}, {1003, 0}}
	copied, err := setupConn.CopyFrom(ctx, pgx.Identifier{"pool_items"}, []string{"id", "worker"}, pgx.CopyFromRows(copyRows))
	if err != nil || copied != 3 {
		t.Fatalf("CopyFrom failed: copied=%d err=%v", copied, err)
	}
	_ = setupConn.Close(ctx)

	// 2. Session isolation: Client A sets work_mem='16MB', disconnects; Client B must see default '4MB'
	cA, err := pgx.Connect(ctx, goodDSN)
	if err != nil {
		t.Fatalf("connect Client A: %v", err)
	}
	if _, err := cA.Exec(ctx, "SET work_mem = '16MB'"); err != nil {
		t.Fatalf("Client A SET work_mem: %v", err)
	}
	_ = cA.Close(ctx)

	cB, err := pgx.Connect(ctx, goodDSN)
	if err != nil {
		t.Fatalf("connect Client B: %v", err)
	}
	var workMem string
	if err := cB.QueryRow(ctx, "SHOW work_mem").Scan(&workMem); err != nil {
		t.Fatalf("Client B SHOW work_mem: %v", err)
	}
	_ = cB.Close(ctx)
	if workMem != "4MB" {
		t.Fatalf("expected default work_mem 4MB on new session, got %q", workMem)
	}

	// 3. Concurrent workers
	const numWorkers = 18
	var wg sync.WaitGroup
	errs := make(chan error, numWorkers)
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c, err := pgx.Connect(ctx, goodDSN)
			if err != nil {
				errs <- fmt.Errorf("worker %d connect: %w", workerID, err)
				return
			}
			defer func() { _ = c.Close(ctx) }()

			tx, err := c.Begin(ctx)
			if err != nil {
				errs <- fmt.Errorf("worker %d begin: %w", workerID, err)
				return
			}
			if _, err := tx.Exec(ctx, "INSERT INTO pool_items (id, worker) VALUES ($1, $2)", workerID+1, workerID); err != nil {
				_ = tx.Rollback(ctx)
				errs <- fmt.Errorf("worker %d insert: %w", workerID, err)
				return
			}
			if err := tx.Commit(ctx); err != nil {
				errs <- fmt.Errorf("worker %d commit: %w", workerID, err)
				return
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent worker error: %v", err)
	}

	verifyConn, err := pgx.Connect(ctx, goodDSN)
	if err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	defer func() { _ = verifyConn.Close(ctx) }()
	var totalRows int
	if err := verifyConn.QueryRow(ctx, "SELECT count(*)::int FROM pool_items").Scan(&totalRows); err != nil {
		t.Fatalf("count pool_items: %v", err)
	}
	if totalRows != 3+numWorkers {
		t.Fatalf("expected %d rows, got %d", 3+numWorkers, totalRows)
	}
}

func TestPhaseH_CooperativeScaleToZero_DrainVetoesAndWakeStorm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rootDir, err := os.MkdirTemp("/tmp", "air_phase_h2_")
	if err != nil {
		t.Fatal(err)
	}
	_ = os.Chmod(rootDir, 0o755)
	defer os.RemoveAll(rootDir)
	volDir := filepath.Join(rootDir, "vol")
	metaDir := filepath.Join(rootDir, "meta")
	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatalf("NewEnvelopeKMS: %v", err)
	}
	ag, err := cell.NewAgent(volDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer func() {
		_ = ag.StopAndPurgeTenant("t_scale_zero")
		ag.Close()
	}()
	cp, err := control.NewControlPlane(ctx, metaDir, "/usr/lib/postgresql/17/bin", 16472, ag, kms)
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	defer cp.Close()

	prov, err := cp.ProvisionDatabase(ctx, "acct_s2z", "idem-s2z-1", "t_scale_zero")
	if err != nil {
		t.Fatalf("ProvisionDatabase: %v", err)
	}
	dsn := fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/t_scale_zero?sslmode=disable", prov.Password, prov.DirectPort)

	c1, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("initial connect: %v", err)
	}
	if _, err := c1.Exec(ctx, "CREATE TABLE s2z_data (id int PRIMARY KEY, note text)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := c1.Exec(ctx, "INSERT INTO s2z_data VALUES (1, 'before_suspend')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// 1. Drain veto when an active client session has an open transaction
	if _, err := c1.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	if err := cp.SuspendTenant(ctx, "t_scale_zero", 500*time.Millisecond, nil); !errors.Is(err, cell.ErrDrainVetoed) {
		t.Fatalf("expected ErrDrainVetoed with active client connection, got: %v", err)
	}
	_, _ = c1.Exec(ctx, "ROLLBACK")
	_ = c1.Close(ctx)

	// 2. Drain veto on open 2PC prepared transaction
	c2, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect c2: %v", err)
	}
	if _, err := c2.Exec(ctx, "BEGIN; INSERT INTO s2z_data VALUES (2, '2pc_row'); PREPARE TRANSACTION 'tx_s2z_veto';"); err != nil {
		t.Fatalf("PREPARE TRANSACTION: %v", err)
	}
	_ = c2.Close(ctx)
	if err := cp.SuspendTenant(ctx, "t_scale_zero", 500*time.Millisecond, nil); !errors.Is(err, cell.ErrDrainVetoed) {
		t.Fatalf("expected ErrDrainVetoed on open 2PC transaction, got: %v", err)
	}
	c2Commit, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect c2Commit: %v", err)
	}
	if _, err := c2Commit.Exec(ctx, "COMMIT PREPARED 'tx_s2z_veto'"); err != nil {
		t.Fatalf("COMMIT PREPARED: %v", err)
	}
	_ = c2Commit.Close(ctx)

	// 3. Clean cooperative suspend (ACTIVE -> SUSPENDED)
	if err := cp.SuspendTenant(ctx, "t_scale_zero", 2*time.Second, nil); err != nil {
		t.Fatalf("SuspendTenant clean failed: %v", err)
	}
	state, _, err := cp.GetTenantRoute(ctx, "t_scale_zero")
	if err != nil || state != "SUSPENDED" {
		t.Fatalf("expected SUSPENDED state, got state=%q err=%v", state, err)
	}

	// 4. Concurrent wake storm (16 simultaneous callers hitting WakeTenant on SUSPENDED tenant)
	const stormClients = 16
	var wg sync.WaitGroup
	stormErrs := make(chan error, stormClients)
	for i := 0; i < stormClients; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			port, _, appPass, wErr := cp.WakeTenant(ctx, "t_scale_zero")
			if wErr != nil {
				stormErrs <- fmt.Errorf("wake client %d WakeTenant: %w", idx, wErr)
				return
			}
			wDSN := fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/t_scale_zero?sslmode=disable", appPass, port)
			wc, err := pgx.Connect(ctx, wDSN)
			if err != nil {
				stormErrs <- fmt.Errorf("wake client %d connect: %w", idx, err)
				return
			}
			defer func() { _ = wc.Close(ctx) }()
			var cnt int
			if err := wc.QueryRow(ctx, "SELECT count(*)::int FROM s2z_data").Scan(&cnt); err != nil {
				stormErrs <- fmt.Errorf("wake client %d query: %w", idx, err)
				return
			}
			if cnt != 2 {
				stormErrs <- fmt.Errorf("wake client %d expected 2 rows, got %d", idx, cnt)
			}
		}(i)
	}
	wg.Wait()
	close(stormErrs)
	for err := range stormErrs {
		t.Fatalf("wake storm error: %v", err)
	}

	state, _, err = cp.GetTenantRoute(ctx, "t_scale_zero")
	if err != nil || state != "ACTIVE" {
		t.Fatalf("expected ACTIVE state after wake storm, got state=%q err=%v", state, err)
	}

	// 5. Automatic idle reaper suspends tenant after idle timeout
	time.Sleep(150 * time.Millisecond)
	suspendedCount, err := cp.SuspendIdleTenants(ctx, 100*time.Millisecond, nil)
	if err != nil {
		t.Fatalf("SuspendIdleTenants: %v", err)
	}
	if suspendedCount != 1 {
		t.Fatalf("expected 1 tenant suspended by idle reaper, got %d", suspendedCount)
	}
	state, _, err = cp.GetTenantRoute(ctx, "t_scale_zero")
	if err != nil || state != "SUSPENDED" {
		t.Fatalf("expected SUSPENDED state after idle reaper, got state=%q err=%v", state, err)
	}
}
