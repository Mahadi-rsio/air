package readadapter

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/jackc/pgx/v5"
)

func TestPhase19_P1_Differential_LivePG17(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live PostgreSQL 17 differential test in -short mode")
	}
	initdbMatches, _ := filepath.Glob("/usr/lib/postgresql/17/bin/initdb")
	if len(initdbMatches) == 0 {
		t.Skip("PostgreSQL 17 server binaries not installed")
	}
	initdbBin := initdbMatches[len(initdbMatches)-1]
	pgBin := filepath.Join(filepath.Dir(initdbBin), "postgres")

	tmpDir := t.TempDir()
	pgData := filepath.Join(tmpDir, "pgdata")
	sockDir := filepath.Join(tmpDir, "pgsock")
	_ = os.MkdirAll(sockDir, 0o755)

	if out, err := exec.Command(initdbBin, "-D", pgData, "-U", "postgres", "-A", "trust", "--encoding=UTF8", "--locale=C", "--no-sync").CombinedOutput(); err != nil {
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

	cmd := exec.Command(pgBin, "-D", pgData, "-k", sockDir, "-h", "", "-p", "55453", "-F")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	defer func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	dsn := fmt.Sprintf("host=%s port=55453 user=postgres dbname=postgres sslmode=disable", sockDir)
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
	defer func() { _ = ps.Close() }()

	const tenantID = "tenant_p1_diff"
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
		CREATE TYPE mood_enum AS ENUM ('sad', 'ok', 'happy');
		CREATE DOMAIN pos_int AS int4 CHECK (VALUE > 0);

		CREATE TABLE p1_items (
			id int4 PRIMARY KEY,
			cat int4 NOT NULL,
			val int4,
			note text,
			amt numeric(12, 2),
			t time,
			iv interval,
			mood mood_enum,
			score pos_int
		);

		INSERT INTO p1_items VALUES
			(1, 1, 10, 'Alpha_One',   '10.25', '08:30:00',       '1 day 02:03:04',       'happy', 100),
			(2, 1, 20, 'beta_two',    '20.50', '14:15:30.125',   '2 mons 3 days',        'ok',    200),
			(3, 1, NULL, 'Gamma\nNew', '-5.25', '00:00:00',      '-01:30:00',            'sad',   50),
			(4, 2, 40, 'ALPHA_FOUR',  '0.00',  '23:59:59.999999','1 year 2 mons',        'happy', 400),
			(5, 2, 15, '100% legit',  '100.00','12:00:00',       '00:00:00',             NULL,    150),
			(6, 3, 30, NULL,          NULL,    NULL,             NULL,                   'ok',    NULL);
		ANALYZE p1_items;
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
	defer func() { _ = srv.Stop() }()

	airConn, err := pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=%s sslmode=disable", srv.Port(), tenantID))
	if err != nil {
		t.Fatalf("connect to AIR: %v", err)
	}
	defer airConn.Close(ctx)

	assertMatch := func(label, q string) {
		t.Helper()
		scanText := func(conn *pgx.Conn) ([][]string, error) {
			rows, err := conn.Query(ctx, q, pgx.QueryExecModeSimpleProtocol)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			ncols := len(rows.FieldDescriptions())
			var out [][]string
			for rows.Next() {
				ptrs := make([]*string, ncols)
				dest := make([]any, ncols)
				for i := range ptrs {
					dest[i] = &ptrs[i]
				}
				if err := rows.Scan(dest...); err != nil {
					return nil, err
				}
				rec := make([]string, ncols)
				for i, p := range ptrs {
					if p == nil {
						rec[i] = "<NULL>"
					} else {
						rec[i] = *p
					}
				}
				out = append(out, rec)
			}
			return out, rows.Err()
		}
		pgRows, err := scanText(pgConn)
		if err != nil {
			t.Fatalf("[%s] PG query %q failed: %v", label, q, err)
		}
		airRows, err := scanText(airConn)
		if err != nil {
			t.Fatalf("[%s] AIR query %q failed: %v", label, q, err)
		}
		if len(pgRows) != len(airRows) {
			t.Fatalf("[%s] row count mismatch for %q: PG=%d AIR=%d\nPG=%v\nAIR=%v", label, q, len(pgRows), len(airRows), pgRows, airRows)
		}
		for r := range pgRows {
			for c := range pgRows[r] {
				if pgRows[r][c] != airRows[r][c] {
					t.Fatalf("[%s] mismatch at row %d col %d for %q: PG=%q AIR=%q", label, r, c, q, pgRows[r][c], airRows[r][c])
				}
			}
		}
	}

	queries := []struct {
		name string
		sql  string
	}{
		// 1. Window functions
		{"win_row_number", "SELECT id, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY id) FROM p1_items ORDER BY id"},
		{"win_rank_dense", "SELECT id, RANK() OVER (ORDER BY cat), DENSE_RANK() OVER (ORDER BY cat) FROM p1_items ORDER BY id"},
		{"win_sum_lag_lead", "SELECT id, SUM(val) OVER (PARTITION BY cat ORDER BY id), LAG(val) OVER (ORDER BY id), LEAD(val) OVER (ORDER BY id) FROM p1_items ORDER BY id"},
		// 2. ILIKE / simple regex
		{"ilike_prefix", "SELECT id FROM p1_items WHERE note ILIKE 'alpha%' ORDER BY id"},
		{"ilike_not", "SELECT id FROM p1_items WHERE note NOT ILIKE '%alpha%' ORDER BY id"},
		{"ilike_escape", "SELECT id FROM p1_items WHERE note ILIKE '100\\% %' ORDER BY id"},
		{"ilike_newline", "SELECT id FROM p1_items WHERE note ILIKE 'gamma%' ORDER BY id"},
		{"regex_cs", "SELECT id FROM p1_items WHERE note ~ '^Alpha' ORDER BY id"},
		{"regex_ci", "SELECT id FROM p1_items WHERE note ~* '^alpha' ORDER BY id"},
		{"regex_neg", "SELECT id FROM p1_items WHERE note !~* 'alpha' ORDER BY id"},
		{"regex_dot_newline", "SELECT id FROM p1_items WHERE note ~ 'Gamma.New' ORDER BY id"},
		// 3. DISTINCT ON
		{"distinct_on_basic", "SELECT DISTINCT ON (cat) cat, id, val FROM p1_items ORDER BY cat, id"},
		{"distinct_on_desc_limit", "SELECT DISTINCT ON (cat) cat, id, note AS label FROM p1_items ORDER BY cat ASC, id DESC LIMIT 2 OFFSET 1"},
		// 4. Numeric arithmetic & aggregates
		{"num_add", "SELECT id, amt + 1.50 FROM p1_items ORDER BY id"},
		{"num_sub", "SELECT id, amt - 5.00 FROM p1_items ORDER BY id"},
		{"num_mul", "SELECT id, amt * 2 FROM p1_items ORDER BY id"},
		{"num_sum", "SELECT cat, SUM(amt) FROM p1_items GROUP BY cat ORDER BY cat"},
		{"num_avg", "SELECT AVG(amt) FROM p1_items"},
		// 5. Catalog-decoded types: time, interval, enum, domain
		{"type_time", "SELECT id, t FROM p1_items ORDER BY id"},
		{"type_interval", "SELECT id, iv FROM p1_items ORDER BY id"},
		{"type_enum_eq", "SELECT id, mood FROM p1_items WHERE mood = 'happy' ORDER BY id"},
		{"type_domain", "SELECT id, score FROM p1_items WHERE score > 100 ORDER BY id"},
	}

	for _, tc := range queries {
		assertMatch(tc.name, tc.sql)
	}

	// Verify unsafe / ambiguous constructs still safely reject for PG fallback.
	mustFallback := []string{
		"SELECT id FROM p1_items WHERE note ~ '(?=Alpha)'",
		"SELECT id FROM p1_items WHERE note ~ '\\1'",
		"SELECT id FROM p1_items ORDER BY mood",
		"SELECT id FROM p1_items ORDER BY iv",
	}
	for _, q := range mustFallback {
		if _, err := queryRowsNormalized(ctx, airConn, q); err == nil {
			t.Fatalf("expected safe PG fallback error for %q, got nil", q)
		}
	}
}
