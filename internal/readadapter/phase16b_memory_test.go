package readadapter

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func TestPhase16B_MemoryBudget_PerQueryPerTenantAndRelease(t *testing.T) {
	mgr := NewMemoryManager(MemoryManagerConfig{
		GlobalLimitBytes:        4096,
		DefaultTenantLimitBytes: 2048,
		DefaultQueryLimitBytes:  1024,
	})

	// 1. Query limit enforcement
	q1 := mgr.BeginQuery("t1", 0)
	if err := q1.Reserve(600); err != nil {
		t.Fatalf("unexpected reserve error: %v", err)
	}
	if err := q1.Reserve(600); !errors.Is(err, ErrMemoryBudgetExceeded) {
		t.Fatalf("expected ErrMemoryBudgetExceeded for query limit, got %v", err)
	}
	if mgr.TenantUsed("t1") != 600 || mgr.GlobalUsed() != 600 {
		t.Fatalf("expected 600 bytes used, got tenant=%d global=%d", mgr.TenantUsed("t1"), mgr.GlobalUsed())
	}

	// 2. Tenant limit enforcement across concurrent queries
	q2 := mgr.BeginQuery("t1", 0)
	if err := q2.Reserve(1000); err != nil {
		t.Fatalf("q2 reserve 1000: %v", err)
	}
	q3 := mgr.BeginQuery("t1", 0)
	if err := q3.Reserve(500); !errors.Is(err, ErrMemoryBudgetExceeded) {
		t.Fatalf("expected tenant limit exhaustion (1600+500 > 2048), got %v", err)
	}
	// Tenant t2 is unaffected by t1's tenant limit
	q4 := mgr.BeginQuery("t2", 0)
	if err := q4.Reserve(1000); err != nil {
		t.Fatalf("t2 should be isolated from t1's tenant limit: %v", err)
	}

	// 3. Release restores budgets idempotently
	q1.Release()
	q1.Release()
	q2.Release()
	q3.Release()
	q4.Release()
	if mgr.TenantUsed("t1") != 0 || mgr.TenantUsed("t2") != 0 || mgr.GlobalUsed() != 0 {
		t.Fatalf("expected 0 bytes used after Release, got t1=%d t2=%d global=%d",
			mgr.TenantUsed("t1"), mgr.TenantUsed("t2"), mgr.GlobalUsed())
	}
}

func TestPhase16B_SQLiteQueryMemoryExhaustionAndAccounting(t *testing.T) {
	conn, err := OpenSQLiteConn(filepath.Join(t.TempDir(), "mem.db"), false, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec("CREATE TABLE items(id INTEGER PRIMARY KEY, payload TEXT);"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := conn.Exec("INSERT INTO items(payload) VALUES('0123456789abcdef0123456789abcdef');"); err != nil {
			t.Fatal(err)
		}
	}

	mgr := NewMemoryManager(MemoryManagerConfig{
		GlobalLimitBytes:        1024 * 1024,
		DefaultTenantLimitBytes: 512 * 1024,
		DefaultQueryLimitBytes:  512, // tiny 512-byte budget to force graceful exhaustion
	})
	budget := mgr.BeginQuery("t_mem", 0)
	defer budget.Release()

	ctx := ContextWithMemoryBudget(context.Background(), budget)
	_, err = conn.QueryWithContext(ctx, nil, "SELECT id, payload FROM items;", nil)
	if !errors.Is(err, ErrMemoryBudgetExceeded) {
		t.Fatalf("expected ErrMemoryBudgetExceeded on tight query budget, got %v", err)
	}

	// Verify generous budget succeeds and releases cleanly
	budget2 := mgr.BeginQuery("t_mem", 1024*1024)
	ctx2 := ContextWithMemoryBudget(context.Background(), budget2)
	res, err := conn.QueryWithContext(ctx2, nil, "SELECT id, payload FROM items;", nil)
	if err != nil {
		t.Fatalf("unexpected error with generous budget: %v", err)
	}
	if len(res.Rows) != 50 {
		t.Fatalf("expected 50 rows, got %d", len(res.Rows))
	}
	if budget2.Used() <= 0 {
		t.Fatalf("expected positive memory accounting on budget2, got %d", budget2.Used())
	}
	budget2.Release()
	budget.Release()
	if mgr.GlobalUsed() != 0 {
		t.Fatalf("expected 0 global used after release, got %d", mgr.GlobalUsed())
	}
	st := mgr.Stats()
	if st.SQLiteUsedBytes < 0 || st.Exhaustions == 0 {
		t.Fatalf("unexpected MemoryManagerStats: %+v", st)
	}
}

func TestPhase16B_ConcurrentMemoryBudgets(t *testing.T) {
	mgr := NewMemoryManager(MemoryManagerConfig{
		GlobalLimitBytes:        64 * 1024,
		DefaultTenantLimitBytes: 16 * 1024,
		DefaultQueryLimitBytes:  4 * 1024,
	})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tid := "t_0"
			if id%2 == 1 {
				tid = "t_1"
			}
			for i := 0; i < 100; i++ {
				b := mgr.BeginQuery(tid, 0)
				_ = b.Reserve(1024)
				_ = b.Reserve(1024)
				b.Release()
			}
		}(g)
	}
	wg.Wait()
	if mgr.GlobalUsed() != 0 {
		t.Fatalf("memory leak in MemoryManager: globalUsed=%d", mgr.GlobalUsed())
	}
}

func BenchmarkPhase16B_SQLiteQuery_RowArena(b *testing.B) {
	conn, err := OpenSQLiteConn(filepath.Join(b.TempDir(), "bench.db"), false, 4096)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER, name TEXT);"); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		if err := conn.Exec("INSERT INTO t(val, name) VALUES(42, 'row_payload');"); err != nil {
			b.Fatal(err)
		}
	}
	mgr := NewMemoryManager(MemoryManagerConfig{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		budget := mgr.BeginQuery("t_bench", 0)
		ctx := ContextWithMemoryBudget(context.Background(), budget)
		res, err := conn.QueryWithContext(ctx, nil, "SELECT id, val, name FROM t;", nil)
		if err != nil || len(res.Rows) != 200 {
			b.Fatalf("query failed: err=%v rows=%d", err, len(res.Rows))
		}
		budget.Release()
	}
}
