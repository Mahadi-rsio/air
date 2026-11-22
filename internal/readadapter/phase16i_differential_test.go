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

func waitForWALSync(ctx context.Context, pgConn *pgx.Conn, tl interface{ LastLSN() uint64 }, timeout time.Duration) {
	var curLSNStr string
	_ = pgConn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&curLSNStr)
	var hi, lo uint32
	_, _ = fmt.Sscanf(curLSNStr, "%X/%X", &hi, &lo)
	targetLSN := (uint64(hi) << 32) | uint64(lo)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tl.LastLSN() >= targetLSN {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func queryRowsNormalized(ctx context.Context, conn *pgx.Conn, sql string, args ...any) ([][]string, error) {
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	fds := rows.FieldDescriptions()
	var out [][]string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		rec := make([]string, len(fds))
		for i, v := range vals {
			if v == nil {
				rec[i] = "<NULL>"
			} else {
				rec[i] = fmt.Sprintf("%v", v)
			}
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func assertDifferentialMatch(t *testing.T, ctx context.Context, pgConn, airConn *pgx.Conn, label, sql string, args ...any) {
	t.Helper()
	pgRows, err := queryRowsNormalized(ctx, pgConn, sql, args...)
	if err != nil {
		t.Fatalf("[%s] PG query failed (%s): %v", label, sql, err)
	}
	airRows, err := queryRowsNormalized(ctx, airConn, sql, args...)
	if err != nil {
		t.Fatalf("[%s] AIR query failed (%s): %v", label, sql, err)
	}
	if len(pgRows) != len(airRows) {
		t.Fatalf("[%s] row count mismatch for %q: PG=%d AIR=%d\nPG=%v\nAIR=%v",
			label, sql, len(pgRows), len(airRows), pgRows, airRows)
	}
	for i := range pgRows {
		if len(pgRows[i]) != len(airRows[i]) {
			t.Fatalf("[%s] col count mismatch at row %d for %q", label, i, sql)
		}
		for c := range pgRows[i] {
			if pgRows[i][c] != airRows[i][c] {
				t.Fatalf("[%s] value mismatch at row %d col %d for %q: PG=%q AIR=%q",
					label, i, c, sql, pgRows[i][c], airRows[i][c])
			}
		}
	}
}

func TestPhase16I_DifferentialCorrectness_LivePG17(t *testing.T) {
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

	cmd := exec.Command(pgBin, "-D", pgData, "-k", sockDir, "-h", "", "-p", "55450", "-F")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	defer func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dsn := fmt.Sprintf("host=%s port=55450 user=postgres dbname=postgres sslmode=disable", sockDir)
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

	psSock := filepath.Join(tmpDir, "ps.sock")
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: psSock,
		LayerDir:   filepath.Join(tmpDir, "layers"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	if err := ps.Start(ctx); err != nil {
		t.Fatal(err)
	}

	const tenantID = "tenant_diff_16i"
	if _, err := ps.BootstrapTenantFromPGData(ctx, tenantID, 1, pgData, initLSN); err != nil {
		t.Fatal(err)
	}
	tl := ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, "gen-1")

	rxCtx, rxCancel := context.WithCancel(ctx)
	defer rxCancel()
	rx := pageserver.NewWALReceiver(dsn+" replication=database", tl, tl.LastLSN())
	go func() { _ = rx.Run(rxCtx) }()

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
	waitForWALSync(ctx, pgConn, tl, 8*time.Second)

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
	defer func() { _ = srv.Stop() }()

	airConn, err := pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=%s sslmode=disable", srv.Port(), tenantID))
	if err != nil {
		t.Fatal(err)
	}
	defer airConn.Close(ctx)

	// 1. Deterministic Differential Queries
	deterministicQueries := []struct {
		label string
		sql   string
	}{
		{"point_lookup", "SELECT id, cat, seq, val, note FROM items WHERE id = 42"},
		{"composite_range_order", "SELECT id, seq FROM items WHERE cat = 2 AND seq >= 100 AND seq <= 500 ORDER BY seq ASC, id ASC"},
		{"order_limit_offset", "SELECT id, cat, seq FROM items WHERE cat = 3 ORDER BY seq DESC LIMIT 5 OFFSET 2"},
		{"null_is_null", "SELECT id, val FROM items WHERE val IS NULL ORDER BY id ASC"},
		{"null_is_not_null", "SELECT count(*) FROM items WHERE val IS NOT NULL"},
		{"ne_predicate", "SELECT count(*) FROM items WHERE cat != 1"},
		{"agg_sum_min_max", "SELECT count(*), count(val), sum(val), min(val), max(val) FROM items"},
		{"group_by", "SELECT cat, count(*), sum(val) FROM items GROUP BY cat ORDER BY cat ASC"},
		{"inner_join", "SELECT i.id, c.title FROM items i INNER JOIN cats c ON i.cat = c.cat WHERE i.id <= 20 ORDER BY i.id ASC"},
		{"left_join_nulls", "SELECT c.cat, c.title, count(i.id) FROM cats c LEFT JOIN items i ON c.cat = i.cat AND i.val IS NOT NULL GROUP BY c.cat, c.title ORDER BY c.cat ASC"},
	}
	for _, tc := range deterministicQueries {
		assertDifferentialMatch(t, ctx, pgConn, airConn, tc.label, tc.sql)
	}

	// 2. Prepared Statements with Typed Parameters
	assertDifferentialMatch(t, ctx, pgConn, airConn, "prep_int_point", "SELECT id, note FROM items WHERE id = $1", int32(77))
	assertDifferentialMatch(t, ctx, pgConn, airConn, "prep_composite", "SELECT id, seq FROM items WHERE cat = $1 AND seq >= $2 ORDER BY seq ASC LIMIT 5", int32(1), int32(200))

	// 3. Randomized SQL Generation (25 deterministic-seed queries)
	rng := rand.New(rand.NewSource(16))
	ops := []string{"=", "!=", "<", "<=", ">", ">="}
	for i := 0; i < 25; i++ {
		cat := rng.Intn(4) + 1
		op := ops[rng.Intn(len(ops))]
		seqBound := (rng.Intn(200) + 1) * 10
		lim := rng.Intn(10) + 1
		off := rng.Intn(5)
		dir := "ASC"
		if rng.Intn(2) == 1 {
			dir = "DESC"
		}
		q := fmt.Sprintf("SELECT id, cat, seq FROM items WHERE cat = %d AND seq %s %d ORDER BY id %s LIMIT %d OFFSET %d",
			cat, op, seqBound, dir, lim, off)
		assertDifferentialMatch(t, ctx, pgConn, airConn, fmt.Sprintf("rand_%d", i), q)
	}

	// 4. MVCC + HOT Update + Delete Visibility
	if _, err := pgConn.Exec(ctx, `
		UPDATE items SET note = 'hot_updated_42' WHERE id = 42;
		DELETE FROM items WHERE id = 43;
		INSERT INTO items (id, cat, seq, val, note) VALUES (999, 1, 9990, 1234, 'inserted_999');
		CHECKPOINT;
	`); err != nil {
		t.Fatal(err)
	}
	waitForWALSync(ctx, pgConn, tl, 8*time.Second)

	assertDifferentialMatch(t, ctx, pgConn, airConn, "after_hot_update", "SELECT id, note FROM items WHERE id = 42")
	assertDifferentialMatch(t, ctx, pgConn, airConn, "after_delete", "SELECT id FROM items WHERE id = 43")
	assertDifferentialMatch(t, ctx, pgConn, airConn, "after_insert", "SELECT id, val, note FROM items WHERE id = 999")

	// 5. Schema Change (DDL) + Prepared Statement Invalidation
	if _, err := pgConn.Exec(ctx, `
		ALTER TABLE items ADD COLUMN tag text;
		CREATE INDEX idx_items_val ON items (val);
		CHECKPOINT;
	`); err != nil {
		t.Fatal(err)
	}
	waitForWALSync(ctx, pgConn, tl, 8*time.Second)

	assertDifferentialMatch(t, ctx, pgConn, airConn, "post_ddl_select", "SELECT id, tag FROM items WHERE id = $1", int32(77))
	assertDifferentialMatch(t, ctx, pgConn, airConn, "post_ddl_new_idx", "SELECT id, val FROM items WHERE val = $1", int32(1234))

	// 6. Tenant Isolation Check
	_ = ps.EnsureTenant("other_isolated_tenant", 1)
	ps.SetTenantGeneration("other_isolated_tenant", "gen-1")
	airOther, err := pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=other_isolated_tenant sslmode=disable", srv.Port()))
	if err != nil {
		t.Fatal(err)
	}
	defer airOther.Close(ctx)
	if _, err := queryRowsNormalized(ctx, airOther, "SELECT id FROM items"); err == nil || (!strings.Contains(err.Error(), "no such table") && !strings.Contains(err.Error(), "not found in pg_database")) {
		t.Fatalf("expected isolated tenant to not see items table, got err=%v", err)
	}
}
