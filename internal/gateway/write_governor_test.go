package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWriteGovernorBoundedQueueAndLimits(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WriteGovernorEnabled = true
	cfg.WriteMaxGlobal = 2
	cfg.WriteMaxPerTenant = 2
	cfg.WriteQueueMax = 2
	cfg.WriteQueueTimeout = 200 * time.Millisecond

	g := NewWriteGovernor(cfg)

	p1, err := g.Acquire(context.Background(), "t1", 100)
	if err != nil {
		t.Fatalf("p1 acquire: %v", err)
	}
	p2, err := g.Acquire(context.Background(), "t1", 100)
	if err != nil {
		t.Fatalf("p2 acquire: %v", err)
	}

	// Queue capacity is 2; launch 2 queued waiters.
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := g.Acquire(context.Background(), "t1", 100)
			if err != nil {
				errs <- err
				return
			}
			p.Release(false)
			errs <- nil
		}()
	}

	// Wait until both waiters are queued.
	deadline := time.Now().Add(time.Second)
	for g.Snapshot().QueueDepth < 2 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if g.Snapshot().QueueDepth != 2 {
		t.Fatalf("expected queue depth 2, got %d", g.Snapshot().QueueDepth)
	}

	// 5th request must be fast-rejected (queue full).
	if _, err := g.Acquire(context.Background(), "t1", 100); !errors.Is(err, ErrWriteQueueFull) {
		t.Fatalf("expected ErrWriteQueueFull, got %v", err)
	}

	p1.Release(false)
	p2.Release(false)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("queued waiter failed: %v", err)
		}
	}

	snap := g.Snapshot()
	if snap.ActiveWrites != 0 || snap.QueueDepth != 0 || snap.RejectedTotal != 1 || snap.CompletedTotal != 4 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
}

func TestWriteGovernorCancellationAndTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WriteGovernorEnabled = true
	cfg.WriteMaxGlobal = 1
	cfg.WriteMaxPerTenant = 1
	cfg.WriteQueueMax = 10
	cfg.WriteQueueTimeout = 50 * time.Millisecond

	g := NewWriteGovernor(cfg)
	p1, err := g.Acquire(context.Background(), "t1", 64)
	if err != nil {
		t.Fatalf("p1 acquire: %v", err)
	}

	// Context cancellation
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(15 * time.Millisecond)
		cancel()
	}()
	if _, err := g.Acquire(ctx, "t1", 64); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// Queue timeout
	if _, err := g.Acquire(context.Background(), "t1", 64); !errors.Is(err, ErrWriteQueueTimeout) {
		t.Fatalf("expected ErrWriteQueueTimeout, got %v", err)
	}

	p1.Release(false)
	snap := g.Snapshot()
	if snap.CancelledTotal != 1 || snap.TimeoutTotal != 1 || snap.ActiveWrites != 0 || snap.QueueDepth != 0 {
		t.Fatalf("unexpected snapshot after cancel/timeout: %+v", snap)
	}
}

func TestWriteGovernorTenantFairnessDRR(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WriteGovernorEnabled = true
	cfg.WriteMaxGlobal = 1
	cfg.WriteMaxPerTenant = 1
	cfg.WriteQueueMax = 64
	cfg.WriteQueueTimeout = 2 * time.Second

	g := NewWriteGovernor(cfg)

	// Hold the single global slot.
	holder, err := g.Acquire(context.Background(), "bulk", 128)
	if err != nil {
		t.Fatalf("holder acquire: %v", err)
	}

	var order []string
	var orderMu sync.Mutex
	var wg sync.WaitGroup

	// Enqueue 4 requests for "bulk" first, then 2 for "interactive".
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := g.Acquire(context.Background(), "bulk", 128)
			if err != nil {
				return
			}
			orderMu.Lock()
			order = append(order, "bulk")
			orderMu.Unlock()
			p.Release(false)
		}()
	}
	deadline := time.Now().Add(time.Second)
	for g.Snapshot().QueueDepth < 4 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := g.Acquire(context.Background(), "interactive", 64)
			if err != nil {
				return
			}
			orderMu.Lock()
			order = append(order, "interactive")
			orderMu.Unlock()
			p.Release(false)
		}()
	}
	for g.Snapshot().QueueDepth < 6 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}

	holder.Release(false)
	wg.Wait()

	if len(order) != 6 {
		t.Fatalf("expected 6 completions, got %d (%v)", len(order), order)
	}
	// Interactive must not be starved behind all 4 bulk requests.
	firstInteractive := -1
	for i, tid := range order {
		if tid == "interactive" {
			firstInteractive = i
			break
		}
	}
	if firstInteractive < 0 || firstInteractive > 2 {
		t.Fatalf("expected interactive to be scheduled fairly early, got order %v", order)
	}
}

func TestWriteGovernorConcurrentStressNoLeak(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WriteGovernorEnabled = true
	cfg.WriteMaxGlobal = 4
	cfg.WriteMaxPerTenant = 2
	cfg.WriteQueueMax = 32
	cfg.WriteQueueTimeout = 100 * time.Millisecond

	g := NewWriteGovernor(cfg)
	var completed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		tid := "t1"
		if i%2 == 1 {
			tid = "t2"
		}
		go func(tenant string) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				p, err := g.Acquire(context.Background(), tenant, 256)
				if err == nil {
					time.Sleep(time.Millisecond)
					p.Release(false)
					completed.Add(1)
				}
			}
		}(tid)
	}
	wg.Wait()
	snap := g.Snapshot()
	if snap.ActiveWrites != 0 || snap.QueueDepth != 0 || snap.QueueBytes != 0 {
		t.Fatalf("leaked governor state: %+v", snap)
	}
	if completed.Load() == 0 {
		t.Fatal("expected some writes to complete")
	}
}
