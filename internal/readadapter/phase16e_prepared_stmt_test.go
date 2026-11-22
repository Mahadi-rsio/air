package readadapter

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func TestPhase16E_TypedBindingAndStmtCacheLifecycle(t *testing.T) {
	conn, err := OpenSQLiteConn(filepath.Join(t.TempDir(), "prep.db"), false, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.Exec("CREATE TABLE users(id INTEGER PRIMARY KEY, score REAL, active INTEGER, name TEXT);"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec("INSERT INTO users VALUES(1, 95.5, 1, 'alice'), (2, 82.0, 0, 'bob');"); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	sql := "SELECT name, score FROM users WHERE id = ?1 AND active = ?2 AND score >= ?3;"
	oids := []uint32{PGOIDInt4, PGOIDBool, PGOIDFloat8}

	// First call: cache miss
	res, _, err := conn.QueryWithContextPlanTyped(ctx, nil, sql, []string{"1", "t", "90.0"}, oids, 1)
	if err != nil || len(res.Rows) != 1 || res.Rows[0][0].Text != "alice" {
		t.Fatalf("unexpected typed query result: err=%v res=%+v", err, res)
	}
	hits, misses := conn.StmtCacheStats()
	if misses != 1 || hits != 0 {
		t.Fatalf("expected hits=0 misses=1, got hits=%d misses=%d", hits, misses)
	}

	// Second call: cache hit with different param values
	res2, _, err := conn.QueryWithContextPlanTyped(ctx, nil, sql, []string{"2", "f", "80.0"}, oids, 1)
	if err != nil || len(res2.Rows) != 1 || res2.Rows[0][0].Text != "bob" {
		t.Fatalf("unexpected cached typed query result: err=%v res=%+v", err, res2)
	}
	hits, misses = conn.StmtCacheStats()
	if hits != 1 || misses != 1 {
		t.Fatalf("expected hits=1 misses=1, got hits=%d misses=%d", hits, misses)
	}

	// Schema version bump -> cache miss
	res3, _, err := conn.QueryWithContextPlanTyped(ctx, nil, sql, []string{"1", "1", "90.0"}, oids, 2)
	if err != nil || len(res3.Rows) != 1 {
		t.Fatalf("schemaVer bump query failed: %v", err)
	}
	hits, misses = conn.StmtCacheStats()
	if misses != 2 {
		t.Fatalf("expected misses=2 on schemaVer bump, got %d", misses)
	}

	// InvalidatePreparedStmts clears cache
	conn.InvalidatePreparedStmts()
	_, _, err = conn.QueryWithContextPlanTyped(ctx, nil, sql, []string{"1", "1", "90.0"}, oids, 2)
	if err != nil {
		t.Fatal(err)
	}
	_, misses = conn.StmtCacheStats()
	if misses != 3 {
		t.Fatalf("expected misses=3 after InvalidatePreparedStmts, got %d", misses)
	}

	// Bounded LRU eviction (> maxCachedSQLiteStmts = 64)
	for i := 0; i < 80; i++ {
		q := fmt.Sprintf("SELECT %d;", i)
		if _, _, err := conn.QueryWithContextPlanTyped(ctx, nil, q, nil, nil, 1); err != nil {
			t.Fatal(err)
		}
	}
	conn.mu.Lock()
	cacheLen := conn.stmtLRU.Len()
	conn.mu.Unlock()
	if cacheLen > maxCachedSQLiteStmts {
		t.Fatalf("stmtCache exceeded bound %d: len=%d", maxCachedSQLiteStmts, cacheLen)
	}
}

func BenchmarkPhase16E_NormalQuery(b *testing.B) {
	conn, err := OpenSQLiteConn(filepath.Join(b.TempDir(), "b_norm.db"), false, 2048)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	_ = conn.Exec("CREATE TABLE items(id INTEGER PRIMARY KEY, val TEXT);")
	_ = conn.Exec("INSERT INTO items VALUES(1, 'hello'), (2, 'world');")
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := conn.QueryWithContext(ctx, nil, "SELECT id, val FROM items WHERE id = ?1;", []string{"1"})
		if err != nil || len(res.Rows) != 1 {
			b.Fatal(err)
		}
	}
}

func BenchmarkPhase16E_PreparedStmt_Hit(b *testing.B) {
	conn, err := OpenSQLiteConn(filepath.Join(b.TempDir(), "b_hit.db"), false, 2048)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	_ = conn.Exec("CREATE TABLE items(id INTEGER PRIMARY KEY, val TEXT);")
	_ = conn.Exec("INSERT INTO items VALUES(1, 'hello'), (2, 'world');")
	ctx := context.Background()
	oids := []uint32{PGOIDInt4}
	// Warm
	_, _, _ = conn.QueryWithContextPlanTyped(ctx, nil, "SELECT id, val FROM items WHERE id = ?1;", []string{"1"}, oids, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, _, err := conn.QueryWithContextPlanTyped(ctx, nil, "SELECT id, val FROM items WHERE id = ?1;", []string{"1"}, oids, 1)
		if err != nil || len(res.Rows) != 1 {
			b.Fatal(err)
		}
	}
}

func BenchmarkPhase16E_PreparedStmt_Miss(b *testing.B) {
	conn, err := OpenSQLiteConn(filepath.Join(b.TempDir(), "b_miss.db"), false, 2048)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	_ = conn.Exec("CREATE TABLE items(id INTEGER PRIMARY KEY, val TEXT);")
	_ = conn.Exec("INSERT INTO items VALUES(1, 'hello'), (2, 'world');")
	ctx := context.Background()
	oids := []uint32{PGOIDInt4}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, _, err := conn.QueryWithContextPlanTyped(ctx, nil, "SELECT id, val FROM items WHERE id = ?1;", []string{"1"}, oids, uint64(i+1))
		if err != nil || len(res.Rows) != 1 {
			b.Fatal(err)
		}
	}
}
