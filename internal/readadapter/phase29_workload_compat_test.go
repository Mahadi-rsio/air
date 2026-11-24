package readadapter

import (
	"path/filepath"
	"testing"
)

func TestPhase29_RealWorldORMAndAnalyticsWorkloads(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "workloads.db")
	conn, err := OpenSQLiteConn(dbPath, false, 1024)
	if err != nil {
		t.Fatalf("OpenSQLiteConn: %v", err)
	}
	defer conn.Close()

	stmts := []string{
		`CREATE TABLE orgs (id INTEGER PRIMARY KEY, name TEXT, plan TEXT, active INTEGER)`,
		`CREATE TABLE users (id INTEGER PRIMARY KEY, org_id INTEGER, email TEXT, role TEXT, score REAL, created_at TEXT)`,
		`CREATE TABLE orders (id INTEGER PRIMARY KEY, org_id INTEGER, user_id INTEGER, amount NUMERIC, status TEXT, created_at TEXT)`,
		`INSERT INTO orgs VALUES (1, 'Acme Corp', 'enterprise', 1), (2, 'Beta LLC', 'pro', 1)`,
		`INSERT INTO users VALUES
			(10, 1, 'Alice@Acme.com', 'admin', 98.5, '2026-01-15 10:00:00'),
			(11, 1, 'bob@acme.com', 'member', NULL, '2026-01-16 12:30:00'),
			(12, 2, 'carol@beta.io', 'admin', 88.0, '2026-02-01 09:15:00')`,
		`INSERT INTO orders VALUES
			(100, 1, 10, 250.50, 'paid', '2026-01-20 14:00:00'),
			(101, 1, 11, 75.25, 'paid', '2026-01-21 16:00:00'),
			(102, 1, 10, 120.00, 'refunded', '2026-01-22 18:00:00'),
			(103, 2, 12, 500.00, 'paid', '2026-02-05 11:00:00')`,
	}
	for _, s := range stmts {
		if err := conn.Exec(s); err != nil {
			t.Fatalf("setup %q: %v", s, err)
		}
	}

	cases := []struct {
		name     string
		pgSQL    string
		args     []string
		wantRows int
		check    func(t *testing.T, res *SQLiteResult)
	}{
		{
			name:     "Drizzle_NextJS_ILIKE_And_NullsLast",
			pgSQL:    `SELECT id, email, score FROM users WHERE org_id = $1 AND email ILIKE $2 ORDER BY score DESC NULLS LAST, id ASC`,
			args:     []string{"1", "%acme%"},
			wantRows: 2,
			check: func(t *testing.T, res *SQLiteResult) {
				if res.Rows[0][0].Int64 != 10 || !res.Rows[1][2].IsNil {
					t.Fatalf("unexpected row order/nulls: %+v", res.Rows)
				}
			},
		},
		{
			name:     "Prisma_Cast_And_Coalesce",
			pgSQL:    `SELECT id, COALESCE(score, 0)::int AS rounded_score FROM users WHERE org_id = $1::int ORDER BY id ASC`,
			args:     []string{"1"},
			wantRows: 2,
			check: func(t *testing.T, res *SQLiteResult) {
				if res.Rows[1][1].Int64 != 0 {
					t.Fatalf("expected coalesced 0, got %+v", res.Rows[1][1])
				}
			},
		},
		{
			name:     "SQLAlchemy_Django_Extract_And_DateTrunc",
			pgSQL:    `SELECT EXTRACT(YEAR FROM created_at::timestamp) AS yr, date_trunc('month', created_at) AS mo, COUNT(*) FROM orders WHERE status = $1 GROUP BY 1, 2 ORDER BY 2 ASC`,
			args:     []string{"paid"},
			wantRows: 2,
			check: func(t *testing.T, res *SQLiteResult) {
				if res.Rows[0][0].Float != 2026 || res.Rows[0][2].Int64 != 2 {
					t.Fatalf("unexpected extract/date_trunc result: %+v", res.Rows[0])
				}
			},
		},
		{
			name:     "Rails_ActiveRecord_DistinctOn",
			pgSQL:    `SELECT DISTINCT ON (org_id) org_id, id, amount FROM orders ORDER BY org_id ASC, amount DESC`,
			wantRows: 2,
			check: func(t *testing.T, res *SQLiteResult) {
				if res.Rows[0][1].Int64 != 100 || res.Rows[1][1].Int64 != 103 {
					t.Fatalf("unexpected DISTINCT ON rows: %+v", res.Rows)
				}
			},
		},
		{
			name:     "Supabase_PostgREST_StringAgg_BoolAnd",
			pgSQL:    `SELECT org_id, string_agg(role, ',') AS roles, bool_and(score IS NOT NULL) AS all_scored FROM users GROUP BY org_id ORDER BY org_id`,
			wantRows: 2,
			check: func(t *testing.T, res *SQLiteResult) {
				if res.Rows[0][2].Int64 != 0 || res.Rows[1][2].Int64 != 1 {
					t.Fatalf("unexpected bool_and values: %+v", res.Rows)
				}
			},
		},
		{
			name:     "Analytics_CTE_And_WindowFunction",
			pgSQL:    `WITH paid AS (SELECT org_id, user_id, amount FROM orders WHERE status = 'paid') SELECT org_id, user_id, SUM(amount) OVER (PARTITION BY org_id) AS org_total FROM paid ORDER BY org_id, user_id`,
			wantRows: 3,
			check: func(t *testing.T, res *SQLiteResult) {
				if res.Rows[0][2].Float != 325.75 {
					t.Fatalf("expected org 1 window total 325.75, got %+v", res.Rows[0][2])
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sqliteSQL, _ := translatePGToSQLite(tc.pgSQL)
			res, err := conn.QueryWithContext(t.Context(), nil, sqliteSQL, tc.args)
			if err != nil {
				t.Fatalf("Query failed (sqlite=%q): %v", sqliteSQL, err)
			}
			if len(res.Rows) != tc.wantRows {
				t.Fatalf("expected %d rows, got %d", tc.wantRows, len(res.Rows))
			}
			if tc.check != nil {
				tc.check(t, res)
			}
		})
	}
}
