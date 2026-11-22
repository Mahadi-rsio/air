package readadapter

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/jackc/pgx/v5"
)

func TestPhase17B_DifferentialCorrectness_LivePG17(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live PostgreSQL 17 differential test in -short mode")
	}
	initdbMatches, _ := filepath.Glob("/usr/lib/postgresql/*/bin/initdb")
	if len(initdbMatches) == 0 {
		t.Skip("PostgreSQL server binaries not installed")
	}
	initdbBin := initdbMatches[len(initdbMatches)-1]
	pgBin := filepath.Join(filepath.Dir(initdbBin), "postgres")

	tmpDir := t.TempDir()
	pgData := filepath.Join(tmpDir, "pgdata")
	sockDir := filepath.Join(tmpDir, "pgsock")
	_ = os.MkdirAll(sockDir, 0o755)

	if out, err := exec.Command(initdbBin, "-D", pgData, "-U", "postgres", "-A", "trust", "--no-sync").CombinedOutput(); err != nil {
		t.Fatalf("initdb failed: %v\n%s", err, out)
	}
	f, err := os.OpenFile(filepath.Join(pgData, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("\nwal_level = replica\nmax_wal_senders = 4\n")
	_ = f.Close()

	hf, err := os.OpenFile(filepath.Join(pgData, "pg_hba.conf"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = hf.WriteString("\nlocal replication postgres trust\n")
	_ = hf.Close()

	cmd := exec.Command(pgBin, "-D", pgData, "-k", sockDir, "-h", "", "-p", "55451", "-F")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	defer func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	dsn := fmt.Sprintf("host=%s port=55451 user=postgres dbname=postgres sslmode=disable", sockDir)
	var pgConn *pgx.Conn
	for i := 0; i < 50; i++ {
		pgConn, err = pgx.Connect(ctx, dsn)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if pgConn == nil {
		t.Fatalf("could not connect to PostgreSQL")
	}
	defer pgConn.Close(ctx)

	_, _ = pgConn.Exec(ctx, "CHECKPOINT")
	var lsnStr string
	_ = pgConn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsnStr)
	var hi, lo uint32
	_, _ = fmt.Sscanf(lsnStr, "%X/%X", &hi, &lo)
	initLSN := (uint64(hi) << 32) | uint64(lo)

	layerDir := filepath.Join(tmpDir, "layers")
	psSock := filepath.Join(tmpDir, "ps.sock")
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: psSock,
		LayerDir:   layerDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ps.Start(ctx); err != nil {
		t.Fatal(err)
	}
	psClosed := false
	defer func() {
		if !psClosed {
			_ = ps.Close()
		}
	}()

	const tenantID = "tenant_diff_17b"
	if _, err := ps.BootstrapTenantFromPGData(ctx, tenantID, 1, pgData, initLSN); err != nil {
		t.Fatal(err)
	}
	tl := ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, "gen-1")

	rxCtx, rxCancel := context.WithCancel(ctx)
	defer rxCancel()
	rx := pageserver.NewWALReceiver(dsn+" replication=database", tl, tl.LastLSN())
	go func() { _ = rx.Run(rxCtx) }()

	// Setup base schema and data
	if _, err := pgConn.Exec(ctx, `
		CREATE TABLE cats (cat int4 PRIMARY KEY, title text);
		CREATE TABLE items (
			id int4 PRIMARY KEY,
			cat int4 NOT NULL,
			seq int4 NOT NULL,
			val int4,
			note text
		);
		INSERT INTO cats VALUES (1, 'alpha'), (2, 'beta'), (3, NULL), (4, 'delta');
		INSERT INTO items
			SELECT g, (g % 4) + 1, g * 10,
			       CASE WHEN g % 5 = 0 THEN NULL ELSE g * 3 END,
			       CASE WHEN g % 7 = 0 THEN NULL ELSE 'note_' || g END
			FROM generate_series(1, 200) g;
		CREATE INDEX idx_items_cat_seq ON items (cat, seq);
		ANALYZE cats;
		ANALYZE items;
		CHECKPOINT;
	`); err != nil {
		t.Fatalf("PG setup failed: %v", err)
	}
	waitForWALSync(ctx, pgConn, tl, 10*time.Second)

	srv, err := NewSQLiteReadServer(SQLiteServerConfig{
		DataDir:          filepath.Join(tmpDir, "sqlite_srv"),
		PGPort:           0,
		PageserverSocket: psSock,
		L1CacheBytes:     16 * 1024 * 1024,
		CacheDir:         filepath.Join(tmpDir, "l2cache"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	srvStopped := false
	defer func() {
		if !srvStopped {
			_ = srv.Stop()
		}
	}()

	airConn, err := pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=%s sslmode=disable", srv.Port(), tenantID))
	if err != nil {
		t.Fatal(err)
	}
	defer airConn.Close(ctx)

	assertDifferentialMatch(t, ctx, pgConn, airConn, "init_id159",
		"SELECT id, cat, seq, val FROM items WHERE id = 159")

	// 1. Phase 17A-A: Fast-Default (attmissingval) across multiple ALTER TABLE operations
	if _, err := pgConn.Exec(ctx, `
		ALTER TABLE items ADD COLUMN tier text DEFAULT 'standard';
		ALTER TABLE items ADD COLUMN weight int4 DEFAULT 10;
		ALTER TABLE items ADD COLUMN big_id int8 DEFAULT 9000000000;
		ALTER TABLE items ADD COLUMN active bool DEFAULT true;
		ALTER TABLE items ADD COLUMN ratio float8 DEFAULT 1.5;
		ALTER TABLE items ADD COLUMN opt text DEFAULT NULL;
		-- Insert a new row after ALTER TABLE (has physical attributes)
		INSERT INTO items (id, cat, seq, val, note, tier, weight, big_id, active, ratio, opt)
		VALUES (201, 2, 2010, 603, 'note_201', 'gold', 25, 42, false, 2.5, 'custom');
		CHECKPOINT;
	`); err != nil {
		t.Fatalf("ALTER TABLE fast-default failed: %v", err)
	}
	waitForWALSync(ctx, pgConn, tl, 10*time.Second)

	// Reconnect airConn or query directly (SQLiteReadServer refreshes catalog on CatalogModLSN)
	assertDifferentialMatch(t, ctx, pgConn, airConn, "fast_default_old_row",
		"SELECT id, tier, weight, big_id, active, ratio, opt FROM items WHERE id = 42")
	assertDifferentialMatch(t, ctx, pgConn, airConn, "fast_default_new_row",
		"SELECT id, tier, weight, big_id, active, ratio, opt FROM items WHERE id = 201")
	assertDifferentialMatch(t, ctx, pgConn, airConn, "fast_default_filter_agg",
		"SELECT tier, count(*), sum(weight), min(big_id), max(ratio) FROM items GROUP BY tier ORDER BY tier ASC")
	assertDifferentialMatch(t, ctx, pgConn, airConn, "fast_default_null_col",
		"SELECT count(*) FROM items WHERE opt IS NULL")

	// Drop a middle column and add another fast-default column
	if _, err := pgConn.Exec(ctx, `
		ALTER TABLE items DROP COLUMN ratio;
		ALTER TABLE items ADD COLUMN region text DEFAULT 'us-east';
		INSERT INTO items (id, cat, seq, val, note, tier, weight, big_id, active, opt, region)
		VALUES (202, 1, 2020, 606, 'note_202', 'silver', 15, 7, true, NULL, 'eu-west');
		CHECKPOINT;
	`); err != nil {
		t.Fatalf("ALTER TABLE drop+add failed: %v", err)
	}
	waitForWALSync(ctx, pgConn, tl, 10*time.Second)

	assertDifferentialMatch(t, ctx, pgConn, airConn, "post_drop_add_old_row",
		"SELECT id, tier, weight, region FROM items WHERE id = 10")
	assertDifferentialMatch(t, ctx, pgConn, airConn, "post_drop_add_mid_row",
		"SELECT id, tier, weight, region FROM items WHERE id = 201")
	assertDifferentialMatch(t, ctx, pgConn, airConn, "post_drop_add_new_row",
		"SELECT id, tier, weight, region FROM items WHERE id = 202")
	assertDifferentialMatch(t, ctx, pgConn, airConn, "post_drop_add_group",
		"SELECT region, count(*) FROM items GROUP BY region ORDER BY region ASC")
	assertDifferentialMatch(t, ctx, pgConn, airConn, "post_drop_add_id159",
		"SELECT id, cat, seq, val FROM items WHERE id = 159")

	// 2. Phase 17A-B: HOT Update chain + HEAP2_PRUNE_ON_ACCESS via CREATE INDEX
	if _, err := pgConn.Exec(ctx, `
		UPDATE items SET note = 'hot_1', val = 101 WHERE id = 42;
		UPDATE items SET note = 'hot_2', val = 102 WHERE id = 42;
		UPDATE items SET note = 'hot_3', val = 103 WHERE id = 42;
		UPDATE items SET note = 'hot_final', val = 4242 WHERE id = 42;
		CREATE INDEX idx_items_val ON items (val);
		CHECKPOINT;
	`); err != nil {
		t.Fatalf("HOT + CREATE INDEX failed: %v", err)
	}
	waitForWALSync(ctx, pgConn, tl, 10*time.Second)

	// Both PK index scan and new val index scan must see id=42 with val=4242, note='hot_final'!
	assertDifferentialMatch(t, ctx, pgConn, airConn, "hot_prune_pk_lookup",
		"SELECT id, val, note, tier FROM items WHERE id = 42")
	assertDifferentialMatch(t, ctx, pgConn, airConn, "hot_prune_val_lookup",
		"SELECT id, val, note, tier FROM items WHERE val = 4242")
	assertDifferentialMatch(t, ctx, pgConn, airConn, "post_hot_prune_id159",
		"SELECT id, cat, seq, val FROM items WHERE id = 159")

	// 3. Transaction Commit, Rollback, and Concurrent Transactions
	if _, err := pgConn.Exec(ctx, `
		BEGIN;
		INSERT INTO items (id, cat, seq, val, note) VALUES (9001, 1, 90010, 9001, 'aborted_row');
		UPDATE items SET note = 'aborted_update' WHERE id = 1;
		DELETE FROM items WHERE id = 2;
		ROLLBACK;
		BEGIN;
		INSERT INTO items (id, cat, seq, val, note) VALUES (9002, 1, 90020, 9002, 'committed_row');
		COMMIT;
	`); err != nil {
		t.Fatalf("tx commit/rollback failed: %v", err)
	}

	// Concurrent overlapping transactions
	pgConn2, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pgConn2.Close(ctx)

	if _, err := pgConn.Exec(ctx, "BEGIN; UPDATE items SET note = 'tx1_update' WHERE id = 5;"); err != nil {
		t.Fatal(err)
	}
	if _, err := pgConn2.Exec(ctx, "BEGIN; UPDATE items SET note = 'tx2_update' WHERE id = 6; COMMIT;"); err != nil {
		t.Fatal(err)
	}
	if _, err := pgConn.Exec(ctx, "COMMIT; CHECKPOINT;"); err != nil {
		t.Fatal(err)
	}
	waitForWALSync(ctx, pgConn, tl, 10*time.Second)

	assertDifferentialMatch(t, ctx, pgConn, airConn, "tx_rollback_invisible",
		"SELECT id, note FROM items WHERE id IN (1, 2, 9001, 9002) ORDER BY id ASC")
	assertDifferentialMatch(t, ctx, pgConn, airConn, "tx_concurrent_visible",
		"SELECT id, note FROM items WHERE id IN (5, 6) ORDER BY id ASC")

	assertDifferentialMatch(t, ctx, pgConn, airConn, "id159_pk",
		"SELECT id, cat, seq, val FROM items WHERE id = 159")
	assertDifferentialMatch(t, ctx, pgConn, airConn, "id159_val",
		"SELECT id, cat, seq, val FROM items WHERE val = 477")
	assertDifferentialMatch(t, ctx, pgConn, airConn, "cat4_all_ids",
		"SELECT id, seq FROM items WHERE cat = 4 ORDER BY id ASC")

	// 4. Deterministic-seed Randomized Differential Queries (seed=17, 50 queries)
	rng := rand.New(rand.NewSource(17))
	ops := []string{"=", "!=", "<", "<=", ">", ">="}
	for i := 0; i < 50; i++ {
		cat := rng.Intn(4) + 1
		op := ops[rng.Intn(len(ops))]
		seqBound := (rng.Intn(200) + 1) * 10
		lim := rng.Intn(12) + 1
		off := rng.Intn(5)
		dir := "ASC"
		if rng.Intn(2) == 1 {
			dir = "DESC"
		}
		var q string
		switch i % 3 {
		case 0:
			q = fmt.Sprintf("SELECT id, cat, seq, tier, weight FROM items WHERE cat = %d AND seq %s %d ORDER BY id %s LIMIT %d OFFSET %d",
				cat, op, seqBound, dir, lim, off)
		case 1:
			q = fmt.Sprintf("SELECT i.id, c.title, i.region FROM items i LEFT JOIN cats c ON i.cat = c.cat WHERE i.cat = %d AND i.seq %s %d ORDER BY i.id %s LIMIT %d",
				cat, op, seqBound, dir, lim)
		case 2:
			q = fmt.Sprintf("SELECT cat, count(*), count(val), sum(weight) FROM items WHERE seq %s %d GROUP BY cat ORDER BY cat %s",
				op, seqBound, dir)
		}
		assertDifferentialMatch(t, ctx, pgConn, airConn, fmt.Sprintf("rand17_%d", i), q)
	}

	// 5. Unsupported Column Type Safe Rejection (point -> ErrUnsupportedType for PG fallback)
	if _, err := pgConn.Exec(ctx, `
		CREATE TABLE unsup_tbl (id int4 PRIMARY KEY, pt point);
		INSERT INTO unsup_tbl VALUES (1, '(1,2)');
		CHECKPOINT;
	`); err != nil {
		t.Fatal(err)
	}
	waitForWALSync(ctx, pgConn, tl, 10*time.Second)

	if _, err := queryRowsNormalized(ctx, airConn, "SELECT id, pt FROM unsup_tbl"); err == nil {
		t.Fatalf("expected error for unsupported point column so gateway falls back to PG, got nil")
	}

	// 6. Restart Pageserver + SQLiteReadServer & Verify Recovery
	_ = airConn.Close(ctx)
	_ = srv.Stop()
	srvStopped = true
	rxCancel()
	_, _ = tl.FlushInMemory(ctx)
	_ = ps.Close()
	psClosed = true

	// Restart Pageserver on the same layerDir
	ps2, err := pageserver.New(pageserver.Config{
		SocketPath: psSock,
		LayerDir:   layerDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ps2.Close()
	if err := ps2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ps2.BootstrapTenantFromPGData(ctx, tenantID, 1, pgData, initLSN); err != nil {
		t.Fatal(err)
	}
	tl2 := ps2.EnsureTenant(tenantID, 1)
	ps2.SetTenantGeneration(tenantID, "gen-1")
	rx2Ctx, rx2Cancel := context.WithCancel(ctx)
	defer rx2Cancel()
	rx2 := pageserver.NewWALReceiver(dsn+" replication=database", tl2, tl2.LastLSN())
	go func() { _ = rx2.Run(rx2Ctx) }()
	waitForWALSync(ctx, pgConn, tl2, 10*time.Second)

	srv2, err := NewSQLiteReadServer(SQLiteServerConfig{
		DataDir:          filepath.Join(tmpDir, "sqlite_srv2"),
		PGPort:           0,
		PageserverSocket: psSock,
		L1CacheBytes:     16 * 1024 * 1024,
		CacheDir:         filepath.Join(tmpDir, "l2cache2"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv2.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv2.Stop() }()

	airConn2, err := pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=%s sslmode=disable", srv2.Port(), tenantID))
	if err != nil {
		t.Fatal(err)
	}
	defer airConn2.Close(ctx)

	assertDifferentialMatch(t, ctx, pgConn, airConn2, "post_restart_hot_prune",
		"SELECT id, val, note, tier, region FROM items WHERE id = 42")
	assertDifferentialMatch(t, ctx, pgConn, airConn2, "post_restart_agg",
		"SELECT region, count(*), sum(weight) FROM items GROUP BY region ORDER BY region ASC")
	assertDifferentialMatch(t, ctx, pgConn, airConn2, "post_restart_tx",
		"SELECT id, note FROM items WHERE id IN (5, 6, 9001, 9002) ORDER BY id ASC")
	_ = strings.Contains("", "")
}
