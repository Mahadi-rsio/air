package pageserver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestWALRedoPoolPersistenceAndBatch(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	pool := NewWALRedoPool(WALRedoPoolConfig{
		WorkerBin: exe,
		Workers:   2,
		QueueSize: 8,
		Env: []string{
			"AIR_BENCH_WALREDO_HELPER=1",
			"AIR_BENCH_WALREDO_PERSISTENT=1",
		},
	})
	defer pool.Close()

	ctx := context.Background()
	base := make([]byte, PageSize)
	reqs := make([]WALRedoRequest, 12)
	for i := range reqs {
		reqs[i] = WALRedoRequest{
			Key:      RelBlockKey(1663, 5, 16384, 0, uint32(i)),
			BasePage: base,
			Deltas: []WALDelta{
				{LSN: 100, Flags: EntryFlagWALRecord, WALRecord: []byte{byte(i + 1)}},
			},
		}
	}

	pages, errs := pool.RedoBatch(ctx, reqs)
	for i := range reqs {
		if errs[i] != nil {
			t.Fatalf("RedoBatch[%d] error: %v", i, errs[i])
		}
		if len(pages[i]) != PageSize || pages[i][0] != byte(i+1) {
			t.Fatalf("RedoBatch[%d] unexpected page[0]=%d want=%d", i, pages[i][0], byte(i+1))
		}
	}
}

func TestWALRedoPoolWorkerFailureRecovery(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	failMarker := filepath.Join(t.TempDir(), "fail_once")
	if err := os.WriteFile(failMarker, []byte("1"), 0o600); err != nil {
		t.Fatal(err)
	}

	pool := NewWALRedoPool(WALRedoPoolConfig{
		WorkerBin: exe,
		Workers:   1,
		QueueSize: 4,
		Env: []string{
			"AIR_BENCH_WALREDO_HELPER=1",
			"AIR_BENCH_WALREDO_PERSISTENT=1",
			"AIR_BENCH_WALREDO_FAIL_ONCE_FILE=" + failMarker,
		},
	})
	defer pool.Close()

	ctx := context.Background()
	key := RelBlockKey(1663, 5, 16384, 0, 1)
	base := make([]byte, PageSize)
	deltas := []WALDelta{{LSN: 100, Flags: EntryFlagWALRecord, WALRecord: []byte{0x42}}}

	// First call triggers worker crash.
	if _, err := pool.Redo(ctx, key, base, deltas); err == nil {
		t.Fatalf("expected error when worker crashes")
	}

	// Second call must respawn a fresh worker and succeed.
	page, err := pool.Redo(ctx, key, base, deltas)
	if err != nil {
		t.Fatalf("expected recovery on second Redo, got: %v", err)
	}
	if len(page) != PageSize || page[0] != 0x42 {
		t.Fatalf("unexpected page[0]=0x%x want=0x42", page[0])
	}
}

func TestWALRedoPoolTimeoutCancellationAndClose(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	pool := NewWALRedoPool(WALRedoPoolConfig{
		WorkerBin:      exe,
		Workers:        2,
		QueueSize:      4,
		RequestTimeout: 50 * time.Millisecond,
		Env: []string{
			"AIR_BENCH_WALREDO_HELPER=1",
			"AIR_BENCH_WALREDO_PERSISTENT=1",
			"AIR_BENCH_WALREDO_SLEEP_MS=500",
		},
	})

	key := RelBlockKey(1663, 5, 16384, 0, 1)
	base := make([]byte, PageSize)
	deltas := []WALDelta{{LSN: 100, Flags: EntryFlagWALRecord, WALRecord: []byte{0x11}}}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if _, err := pool.Redo(ctx, key, base, deltas); err == nil {
		t.Fatalf("expected timeout error")
	}

	if err := pool.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := pool.Redo(context.Background(), key, base, deltas); !errors.Is(err, ErrWALRedoClosed) {
		t.Fatalf("expected ErrWALRedoClosed after Close, got %v", err)
	}
}

func TestWALRedoPoolConcurrentBounded(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	pool := NewWALRedoPool(WALRedoPoolConfig{
		WorkerBin: exe,
		Workers:   2,
		QueueSize: 4,
		Env: []string{
			"AIR_BENCH_WALREDO_HELPER=1",
			"AIR_BENCH_WALREDO_PERSISTENT=1",
		},
	})
	defer pool.Close()

	var wg sync.WaitGroup
	base := make([]byte, PageSize)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := RelBlockKey(1663, 5, 16384, 0, uint32(id))
			deltas := []WALDelta{{LSN: 100, Flags: EntryFlagWALRecord, WALRecord: []byte{byte(id + 1)}}}
			page, err := pool.Redo(context.Background(), key, base, deltas)
			if err != nil {
				t.Errorf("goroutine %d Redo error: %v", id, err)
				return
			}
			if len(page) != PageSize || page[0] != byte(id+1) {
				t.Errorf("goroutine %d unexpected page[0]=%d", id, page[0])
			}
		}(g)
	}
	wg.Wait()
}
