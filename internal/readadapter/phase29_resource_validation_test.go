package readadapter

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPhase29_MultiTenantConcurrencyAndMemoryIsolation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "multitenant.db")
	conn, err := OpenSQLiteConn(dbPath, false, 1024)
	if err != nil {
		t.Fatalf("OpenSQLiteConn: %v", err)
	}
	defer conn.Close()

	if err := conn.Exec("CREATE TABLE events (id INTEGER, tenant TEXT, val TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 120; i++ {
		if err := conn.Exec(fmt.Sprintf("INSERT INTO events VALUES (%d, 'shared', 'row-payload-data')", i)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	for _, tenantCount := range []int{1, 8, 32, 100, 250} {
		t.Run(fmt.Sprintf("Tenants_%d", tenantCount), func(t *testing.T) {
			mm := NewMemoryManager(MemoryManagerConfig{
				GlobalLimitBytes:        16 * 1024 * 1024,
				DefaultTenantLimitBytes: 256 * 1024,
				DefaultQueryLimitBytes:  64 * 1024,
			})

			var wg sync.WaitGroup
			var completed atomic.Int64
			for i := 0; i < tenantCount; i++ {
				wg.Add(1)
				go func(tid int) {
					defer wg.Done()
					tenantID := fmt.Sprintf("tenant-%d", tid)
					budget := mm.BeginQuery(tenantID, 0)
					defer budget.Release()
					ctx := ContextWithMemoryBudget(context.Background(), budget)

					var rowsSeen int
					_, _, qerr := conn.QueryStreamWithContext(
						ctx, nil,
						"SELECT id, tenant, val FROM events",
						nil, nil, 1, 32,
						nil,
						func(rows [][]SQLiteValue) error {
							rowsSeen += len(rows)
							return nil
						},
					)
					if qerr != nil {
						t.Errorf("tenant %s stream error: %v", tenantID, qerr)
						return
					}
					if rowsSeen != 120 {
						t.Errorf("tenant %s expected 120 rows, got %d", tenantID, rowsSeen)
						return
					}
					completed.Add(1)
				}(i)
			}
			wg.Wait()

			if int(completed.Load()) != tenantCount {
				t.Fatalf("expected %d completed tenants, got %d", tenantCount, completed.Load())
			}
			if mm.GlobalUsed() != 0 {
				t.Fatalf("expected GlobalUsed=0 after all tenant streams finished, got %d", mm.GlobalUsed())
			}
		})
	}
}
