package readadapter

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestPhase26_TranslatePGToSQLite_CastsParamsAndNulls(t *testing.T) {
	// 1. String literals with :: and $1 must not be corrupted.
	sql, p := translatePGToSQLite("SELECT 'cost $1 ::int' FROM t WHERE id = $1 AND x = $2")
	if p != 2 {
		t.Fatalf("expected maxParam=2, got %d", p)
	}
	if !strings.Contains(sql, "'cost $1 ::int'") {
		t.Fatalf("string literal corrupted: %s", sql)
	}
	if !strings.Contains(sql, "id = ?1 AND x = ?2") {
		t.Fatalf("params not translated: %s", sql)
	}

	// 2. AST-aware casts
	sql2, _ := translatePGToSQLite("SELECT '42'::int4, 5::float8 / 2, 'true'::boolean, CAST(val AS numeric(10,2)) FROM t")
	if !strings.Contains(sql2, "CAST('42' AS INTEGER)") {
		t.Fatalf("expected CAST('42' AS INTEGER), got %s", sql2)
	}
	if !strings.Contains(sql2, "CAST(5 AS REAL) / 2") {
		t.Fatalf("expected CAST(5 AS REAL) / 2, got %s", sql2)
	}
	if !strings.Contains(sql2, "air_pg_cast_bool('true')") {
		t.Fatalf("expected air_pg_cast_bool('true'), got %s", sql2)
	}
	if !strings.Contains(sql2, "air_pg_cast_numeric(val)") {
		t.Fatalf("expected air_pg_cast_numeric(val), got %s", sql2)
	}

	// 3. EXTRACT, POSITION, SUBSTRING
	sql3, _ := translatePGToSQLite("SELECT EXTRACT(YEAR FROM ts), POSITION('ab' IN name), SUBSTRING(name FROM 2 FOR 3) FROM t")
	if !strings.Contains(sql3, "air_pg_date_part('year', ts)") {
		t.Fatalf("EXTRACT not translated: %s", sql3)
	}
	if !strings.Contains(sql3, "INSTR(name, 'ab')") {
		t.Fatalf("POSITION not translated: %s", sql3)
	}
	if !strings.Contains(sql3, "SUBSTR(name, 2, 3)") {
		t.Fatalf("SUBSTRING not translated: %s", sql3)
	}

	// 4. ORDER BY NULLS semantics
	sql4, _ := translatePGToSQLite("SELECT id FROM t ORDER BY a, b DESC, c ASC NULLS FIRST LIMIT 5")
	if !strings.Contains(sql4, "a NULLS LAST, b DESC NULLS FIRST, c ASC NULLS FIRST LIMIT 5") {
		t.Fatalf("ORDER BY NULLS mismatch: %s", sql4)
	}
}

func TestPhase26_SQLiteConn_PGSemanticExecution(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "phase26.db")
	conn, err := OpenSQLiteConn(dbPath, false, 4096)
	if err != nil {
		t.Fatalf("OpenSQLiteConn: %v", err)
	}
	defer conn.Close()

	setup := `
		CREATE TABLE items (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER, flag TEXT);
		INSERT INTO items VALUES
			(1, 'a', 10, 'true'),
			(2, 'a', NULL, 't'),
			(3, 'b', 30, 'false'),
			(4, 'b', 20, 'f');
		CREATE TABLE extra (item_id INTEGER, note TEXT);
		INSERT INTO extra VALUES (1, 'n1'), (5, 'orphan');
	`
	if err := conn.Exec(setup); err != nil {
		t.Fatalf("setup Exec: %v", err)
	}

	// 1. Casts & arithmetic
	q, _ := translatePGToSQLite("SELECT '42'::int4 + 1, 5::float8 / 2, 'true'::boolean, '12.340'::numeric(10,2)")
	res, err := conn.Query(q, nil)
	if err != nil {
		t.Fatalf("casts query failed: %v (sql=%s)", err, q)
	}
	if len(res.Rows) != 1 || res.Rows[0][0].Int64 != 43 || res.Rows[0][1].Float != 2.5 || res.Rows[0][2].Int64 != 1 || res.Rows[0][3].Text != "12.340" {
		t.Fatalf("unexpected casts result: %+v", res.Rows)
	}

	// Invalid boolean cast must error
	qBadBool, _ := translatePGToSQLite("SELECT 'not_a_bool'::boolean")
	if _, err := conn.Query(qBadBool, nil); err == nil {
		t.Fatal("expected error for invalid boolean cast")
	}

	// 2. String, math, date, UUID functions
	qFuncs, _ := translatePGToSQLite(`
		SELECT
			LEFT('abcdef', 3),
			LEFT('abcdef', -2),
			RIGHT('abcdef', 2),
			RIGHT('abcdef', -2),
			SPLIT_PART('a,b,c', ',', 2),
			STRPOS('hello world', 'world'),
			REPEAT('ab', 3),
			REVERSE('abc'),
			LPAD('42', 5, '0'),
			RPAD('hi', 4, '!'),
			INITCAP('hello WORLD'),
			CONCAT('a', NULL, 'b', 1),
			CONCAT_WS('-', 'a', NULL, 'b'),
			CHAR_LENGTH('héllo'),
			MD5('abc'),
			TRANSLATE('12345', '143', 'ax'),
			REGEXP_REPLACE('foo 123 bar 456', '\d+', 'X', 'g'),
			GREATEST(1, NULL, 9, 3),
			LEAST(1, NULL, 9, 3),
			EXTRACT(MONTH FROM '2026-11-24 15:04:05'),
			TO_CHAR('2026-11-24 15:04:05', 'YYYY/MM/DD'),
			CURRENT_SCHEMA(),
			LENGTH(GEN_RANDOM_UUID())
	`)
	resF, err := conn.Query(qFuncs, nil)
	if err != nil {
		t.Fatalf("functions query failed: %v (sql=%s)", err, qFuncs)
	}
	r := resF.Rows[0]
	checks := []struct {
		idx  int
		want string
	}{
		{0, "abc"},
		{1, "abcd"},
		{2, "ef"},
		{3, "cdef"},
		{4, "b"},
		{7, "cba"},
		{8, "00042"},
		{9, "hi!!"},
		{10, "Hello World"},
		{11, "ab1"},
		{12, "a-b"},
		{14, "900150983cd24fb0d6963f7d28e17f72"},
		{15, "a2x5"},
		{16, "foo X bar X"},
		{20, "2026/11/24"},
		{21, "public"},
	}
	for _, c := range checks {
		if r[c.idx].Text != c.want {
			t.Errorf("col[%d] = %q, want %q", c.idx, r[c.idx].Text, c.want)
		}
	}
	if r[5].Int64 != 7 || r[13].Int64 != 5 || r[17].Int64 != 9 || r[18].Int64 != 1 || r[19].Float != 11 || r[22].Int64 != 36 {
		t.Errorf("unexpected numeric/int function results: strpos=%d charlen=%d greatest=%d least=%v month=%v uuidlen=%d",
			r[5].Int64, r[13].Int64, r[17].Int64, r[18].Int64, r[19].Float, r[22].Int64)
	}

	// 3. Aggregates: STRING_AGG, BOOL_AND, BOOL_OR, FILTER
	qAgg, _ := translatePGToSQLite("SELECT grp, STRING_AGG(id::text, ','), BOOL_AND(flag::boolean), BOOL_OR(flag::boolean), COUNT(*) FILTER (WHERE val >= 20) FROM items GROUP BY grp ORDER BY grp")
	resA, err := conn.Query(qAgg, nil)
	if err != nil {
		t.Fatalf("agg query failed: %v (sql=%s)", err, qAgg)
	}
	if len(resA.Rows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(resA.Rows))
	}
	// grp 'a': ids '1,2', bool_and=1, bool_or=1, count>=20=0
	if resA.Rows[0][0].Text != "a" || resA.Rows[0][1].Text != "1,2" || resA.Rows[0][2].Int64 != 1 || resA.Rows[0][3].Int64 != 1 || resA.Rows[0][4].Int64 != 0 {
		t.Fatalf("unexpected grp 'a' agg: %+v", resA.Rows[0])
	}
	// grp 'b': ids '3,4', bool_and=0, bool_or=0, count>=20=2
	if resA.Rows[1][0].Text != "b" || resA.Rows[1][1].Text != "3,4" || resA.Rows[1][2].Int64 != 0 || resA.Rows[1][3].Int64 != 0 || resA.Rows[1][4].Int64 != 2 {
		t.Fatalf("unexpected grp 'b' agg: %+v", resA.Rows[1])
	}

	// 4. ORDER BY NULLS semantics (PG: ASC -> NULLS LAST, DESC -> NULLS FIRST)
	qAsc, _ := translatePGToSQLite("SELECT id FROM items ORDER BY val ASC")
	resAsc, err := conn.Query(qAsc, nil)
	if err != nil {
		t.Fatalf("ORDER BY ASC failed: %v", err)
	}
	// val=10 (id=1), val=20 (id=4), val=30 (id=3), val=NULL (id=2)
	if resAsc.Rows[3][0].Int64 != 2 {
		t.Fatalf("expected NULL row (id=2) last in ASC, got id=%d", resAsc.Rows[3][0].Int64)
	}

	qDesc, _ := translatePGToSQLite("SELECT id FROM items ORDER BY val DESC")
	resDesc, err := conn.Query(qDesc, nil)
	if err != nil {
		t.Fatalf("ORDER BY DESC failed: %v", err)
	}
	if resDesc.Rows[0][0].Int64 != 2 {
		t.Fatalf("expected NULL row (id=2) first in DESC, got id=%d", resDesc.Rows[0][0].Int64)
	}

	// 5. FULL OUTER JOIN + window function
	qJoin, _ := translatePGToSQLite("SELECT COALESCE(i.id, e.item_id) AS k, ROW_NUMBER() OVER (ORDER BY COALESCE(i.id, e.item_id)) AS rn FROM items i FULL OUTER JOIN extra e ON i.id = e.item_id ORDER BY k")
	resJ, err := conn.Query(qJoin, nil)
	if err != nil {
		t.Fatalf("FULL OUTER JOIN + window failed: %v (sql=%s)", err, qJoin)
	}
	if len(resJ.Rows) != 5 {
		t.Fatalf("expected 5 rows from FULL OUTER JOIN, got %d", len(resJ.Rows))
	}
}
