package readadapter

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestPhase16F_ContextCancellationAndCleanup(t *testing.T) {
	conn, err := OpenSQLiteConn(filepath.Join(t.TempDir(), "cancel.db"), false, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Expensive recursive CTE that runs long enough to be interrupted cleanly.
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	_, err = conn.QueryWithContext(ctx, nil,
		"WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 100000000) SELECT sum(x) FROM cnt;",
		nil,
	)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.DeadlineExceeded or Canceled, got %v", err)
	}

	// Connection must remain healthy and usable after cancellation!
	res, err := conn.QueryWithContext(context.Background(), nil, "SELECT 42;", nil)
	if err != nil || len(res.Rows) != 1 || res.Rows[0][0].Int64 != 42 {
		t.Fatalf("connection unusable after cancellation: err=%v res=%+v", err, res)
	}
}

func TestPhase16F_MaxResultRowsLimit(t *testing.T) {
	conn, err := OpenSQLiteConn(filepath.Join(t.TempDir(), "limit.db"), false, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.Exec("CREATE TABLE nums(id INTEGER PRIMARY KEY);"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec("WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 100) INSERT INTO nums(id) SELECT x FROM cnt;"); err != nil {
		t.Fatal(err)
	}

	ctx := ContextWithResourceLimits(context.Background(), QueryResourceLimits{MaxResultRows: 25})
	_, err = conn.QueryWithContext(ctx, nil, "SELECT id FROM nums;", nil)
	if !errors.Is(err, ErrResultLimitExceeded) {
		t.Fatalf("expected ErrResultLimitExceeded, got %v", err)
	}

	// Generous limit succeeds
	ctxOK := ContextWithResourceLimits(context.Background(), QueryResourceLimits{MaxResultRows: 200})
	res, err := conn.QueryWithContext(ctxOK, nil, "SELECT id FROM nums;", nil)
	if err != nil || len(res.Rows) != 100 {
		t.Fatalf("expected 100 rows, got err=%v rows=%d", err, len(res.Rows))
	}
}

func TestPhase16F_ConcurrentCancelAndCloseRace(t *testing.T) {
	conn, err := OpenSQLiteConn(filepath.Join(t.TempDir(), "race_cancel.db"), false, 512)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
			defer cancel()
			_, _ = conn.QueryWithContext(ctx, nil,
				"WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 10000000) SELECT count(*) FROM cnt;",
				nil,
			)
		}()
	}
	time.Sleep(8 * time.Millisecond)
	_ = conn.Close()
	wg.Wait()
}
