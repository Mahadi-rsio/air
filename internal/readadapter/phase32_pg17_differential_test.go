package readadapter

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func pg17DSN() string {
	if v := os.Getenv("AIR_PG17_DSN"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@127.0.0.1:15432/air_diff?sslmode=disable"
}

func queryAllAsStrings(ctx context.Context, conn *pgx.Conn, sql string, args ...any) ([][]string, error) {
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
		rowStr := make([]string, len(fds))
		for i, v := range vals {
			if v == nil {
				rowStr[i] = "<NULL>"
			} else {
				rowStr[i] = fmt.Sprintf("%v", v)
			}
		}
		out = append(out, rowStr)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func TestPhase32_RealPG17_DifferentialValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := pg17DSN()
	pgConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to real PostgreSQL 17.12 (%s): %v", dsn, err)
	}
	defer pgConn.Close(ctx)

	var ver string
	if err := pgConn.QueryRow(ctx, "SELECT version()").Scan(&ver); err != nil {
		t.Fatalf("SELECT version(): %v", err)
	}
	if !strings.Contains(ver, "PostgreSQL 17.") {
		t.Fatalf("expected PostgreSQL 17.x, got %q", ver)
	}

	// Setup identical dataset in PostgreSQL 17.12
	setupSQL := `
		DROP TABLE IF EXISTS p32_orders;
		DROP TABLE IF EXISTS p32_users;
		CREATE TABLE p32_users (
			id BIGINT PRIMARY KEY,
			org_id INT NOT NULL,
			name TEXT NOT NULL,
			score INT,
			active BOOLEAN NOT NULL,
			meta TEXT NOT NULL,
			created_at TEXT NOT NULL
		);
		CREATE TABLE p32_orders (
			id BIGINT PRIMARY KEY,
			user_id BIGINT NOT NULL,
			status TEXT NOT NULL,
			amount NUMERIC(10,2) NOT NULL
		);
		INSERT INTO p32_users (id, org_id, name, score, active, meta, created_at) VALUES
			(1, 10, 'Alice', 100, true, '{"role":"admin","tier":1}', '2026-01-15T10:30:00Z'),
			(2, 10, 'bob', NULL, false, '{"role":"member","tier":2}', '2026-02-20T12:00:00Z'),
			(3, 20, 'Charlie', 50, true, '{"role":"viewer","tier":1}', '2026-02-25T08:15:00Z'),
			(4, 20, 'alice_2', 200, true, '{"role":"admin","tier":3}', '2026-03-01T00:00:00Z');
		INSERT INTO p32_orders (id, user_id, status, amount) VALUES
			(101, 1, 'paid', 120.50),
			(102, 1, 'pending', 30.00),
			(103, 3, 'paid', 75.25),
			(104, 4, 'paid', 250.00);
	`
	if _, err := pgConn.Exec(ctx, setupSQL); err != nil {
		t.Fatalf("PG setup: %v", err)
	}

	dataDir := t.TempDir()
	srv, err := NewSQLiteReadServer(SQLiteServerConfig{
		ListenHost:       "127.0.0.1",
		PGPort:           0,
		DataDir:          dataDir,
		ReadersPerTenant: 2,
		QueryTimeout:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSQLiteReadServer: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("srv.Start: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	if err := srv.SyncFromPrimary(ctx, "air_diff", dsn); err != nil {
		t.Fatalf("SyncFromPrimary: %v", err)
	}

	airDSN := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/air_diff?sslmode=disable", srv.Port())
	airConn, err := pgx.Connect(ctx, airDSN)
	if err != nil {
		t.Fatalf("connect to AIR ReadEngine: %v", err)
	}
	defer airConn.Close(ctx)

	diffCases := []struct {
		name string
		sql  string
	}{
		// Values, ordering, NULLs
		{"order_nulls_asc", "SELECT id, score FROM p32_users ORDER BY score ASC, id ASC"},
		{"order_nulls_desc", "SELECT id, score FROM p32_users ORDER BY score DESC, id ASC"},
		{"order_explicit_nulls_first", "SELECT id, score FROM p32_users ORDER BY score ASC NULLS FIRST, id ASC"},
		// Casts
		{"casts_basic", "SELECT '42'::int4, '12.34'::numeric(10,2)::text, 10::text"},
		// JSON
		{"json_extract", "SELECT id, meta::json->>'role' AS role, meta::json->>'tier' AS tier FROM p32_users ORDER BY id"},
		// Dates & timestamps
		{"dates_extract_trunc", "SELECT id, EXTRACT(YEAR FROM created_at::timestamp)::int, date_trunc('month', created_at::timestamptz)::text FROM p32_users ORDER BY id"},
		// Joins
		{"join_left", "SELECT u.id, COALESCE(o.status, 'none') FROM p32_users u LEFT JOIN p32_orders o ON o.user_id = u.id ORDER BY u.id, o.id"},
		{"join_full", "SELECT COALESCE(u.id, 0), COALESCE(o.id, 0) FROM p32_users u FULL OUTER JOIN p32_orders o ON o.user_id = u.id ORDER BY 1, 2"},
		// CTE & Recursive CTE
		{"cte_recursive", "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 5) SELECT x FROM cnt ORDER BY x"},
		// Window functions
		{"window_rank_lag", "SELECT id, org_id, ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY id), LAG(id) OVER (ORDER BY id) FROM p32_users ORDER BY id"},
		// Drizzle-shaped SQL
		{"drizzle_ilike", "SELECT id, name FROM p32_users WHERE name ILIKE '%alice%' ORDER BY score ASC NULLS LAST, id ASC"},
		// Prisma-shaped SQL
		{"prisma_coalesce", "SELECT id, COALESCE(score, 0)::int AS s FROM p32_users WHERE org_id = 10 ORDER BY id ASC"},
		// Django / SQLAlchemy-shaped SQL
		{"django_sqlalchemy_agg", "SELECT org_id, COUNT(*), SUM(COALESCE(score, 0)) FROM p32_users GROUP BY org_id ORDER BY org_id"},
		// Rails-shaped SQL
		{"rails_distinct_on", "SELECT DISTINCT ON (org_id) org_id, id, name FROM p32_users ORDER BY org_id ASC, id DESC"},
		// PostgREST-shaped SQL
		{"postgrest_bool_string_agg", "SELECT org_id, STRING_AGG(id::text, ',' ORDER BY id), CASE WHEN BOOL_AND(active) THEN 1 ELSE 0 END, CASE WHEN BOOL_OR(active) THEN 1 ELSE 0 END FROM p32_users GROUP BY org_id ORDER BY org_id"},
	}

	for _, tc := range diffCases {
		t.Run(tc.name, func(t *testing.T) {
			pgRows, pgErr := queryAllAsStrings(ctx, pgConn, tc.sql)
			airRows, airErr := queryAllAsStrings(ctx, airConn, tc.sql)
			if (pgErr != nil) != (airErr != nil) {
				t.Fatalf("error mismatch for %q: pgErr=%v airErr=%v", tc.sql, pgErr, airErr)
			}
			if len(pgRows) != len(airRows) {
				t.Fatalf("row count mismatch for %q: pg=%d (%v) air=%d (%v)", tc.sql, len(pgRows), pgRows, len(airRows), airRows)
			}
			for r := range pgRows {
				if len(pgRows[r]) != len(airRows[r]) {
					t.Fatalf("col count mismatch row %d for %q: pg=%v air=%v", r, tc.sql, pgRows[r], airRows[r])
				}
				for c := range pgRows[r] {
					if pgRows[r][c] != airRows[r][c] {
						t.Fatalf("value mismatch at [%d,%d] for %q: pg=%q air=%q", r, c, tc.sql, pgRows[r][c], airRows[r][c])
					}
				}
			}
		})
	}

	// Error parity: invalid boolean cast fails on both PG 17.12 and AIR
	_, pgErr := pgConn.Exec(ctx, "SELECT 'not_a_bool'::boolean")
	_, airErr := airConn.Exec(ctx, "SELECT 'not_a_bool'::boolean")
	if pgErr == nil || airErr == nil {
		t.Fatalf("expected error on both PG and AIR for invalid bool cast: pgErr=%v airErr=%v", pgErr, airErr)
	}

	// Concurrent write on PG 17.12 + server restart + re-sync recovery
	if _, err := pgConn.Exec(ctx, "INSERT INTO p32_users (id, org_id, name, score, active, meta, created_at) VALUES (5, 30, 'Eve', 300, true, '{}', '2026-03-10T00:00:00Z')"); err != nil {
		t.Fatalf("PG concurrent write: %v", err)
	}
	_ = airConn.Close(ctx)
	_ = srv.Stop()

	srv2, err := NewSQLiteReadServer(SQLiteServerConfig{
		ListenHost:       "127.0.0.1",
		PGPort:           0,
		DataDir:          dataDir,
		ReadersPerTenant: 2,
		QueryTimeout:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSQLiteReadServer restart: %v", err)
	}
	if err := srv2.Start(); err != nil {
		t.Fatalf("srv2.Start: %v", err)
	}
	defer func() { _ = srv2.Stop() }()
	if err := srv2.SyncFromPrimary(ctx, "air_diff", dsn); err != nil {
		t.Fatalf("srv2.SyncFromPrimary: %v", err)
	}
	airConn2, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@127.0.0.1:%d/air_diff?sslmode=disable", srv2.Port()))
	if err != nil {
		t.Fatalf("reconnect after restart: %v", err)
	}
	defer airConn2.Close(ctx)
	var cnt int64
	if err := airConn2.QueryRow(ctx, "SELECT COUNT(*) FROM p32_users").Scan(&cnt); err != nil || cnt != 5 {
		t.Fatalf("expected 5 rows after restart+sync, got cnt=%d err=%v", cnt, err)
	}
}

func TestPhase32_TenantIsolation_SeparateDBsAndSlots(t *testing.T) {
	srv, err := NewSQLiteReadServer(SQLiteServerConfig{
		ListenHost:       "127.0.0.1",
		PGPort:           0,
		DataDir:          t.TempDir(),
		ReadersPerTenant: 2,
		QueryTimeout:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSQLiteReadServer: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("srv.Start: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	tenants := []string{"t_iso_a", "t_iso_b", "t_iso_c", "t_iso_d"}
	dbPaths := make(map[string]bool)
	readers := make(map[*SQLiteConn]bool)
	for i, tid := range tenants {
		ts, err := srv.getOrCreateTenant(tid)
		if err != nil {
			t.Fatalf("getOrCreateTenant(%s): %v", tid, err)
		}
		if dbPaths[ts.DBPath] {
			t.Fatalf("duplicate DBPath %q across tenants", ts.DBPath)
		}
		dbPaths[ts.DBPath] = true
		if ts.Reader == nil || readers[ts.Reader] {
			t.Fatalf("expected dedicated Reader connection for %s", tid)
		}
		readers[ts.Reader] = true
		if err := ts.Writer.Exec("CREATE TABLE secrets (id INTEGER PRIMARY KEY, secret TEXT)"); err != nil {
			t.Fatalf("create table %s: %v", tid, err)
		}
		if err := ts.Writer.Exec(fmt.Sprintf("INSERT INTO secrets VALUES (1, 'secret_%s_%d')", tid, i)); err != nil {
			t.Fatalf("insert %s: %v", tid, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errCh := make(chan error, len(tenants)*8)
	for i, tid := range tenants {
		for c := 0; c < 4; c++ {
			wg.Add(1)
			go func(tenant string, idx int) {
				defer wg.Done()
				dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/%s?sslmode=disable", srv.Port(), tenant)
				conn, err := pgx.Connect(ctx, dsn)
				if err != nil {
					errCh <- err
					return
				}
				defer conn.Close(ctx)
				var got string
				if err := conn.QueryRow(ctx, "SELECT secret FROM secrets WHERE id = 1").Scan(&got); err != nil {
					errCh <- err
					return
				}
				want := fmt.Sprintf("secret_%s_%d", tenant, idx)
				if got != want {
					errCh <- fmt.Errorf("tenant %s isolation breach: got %q want %q", tenant, got, want)
				}
			}(tid, i)
		}
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}
