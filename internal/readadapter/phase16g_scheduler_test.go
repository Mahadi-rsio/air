package readadapter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPhase16G_FairSchedulingAndNoisyNeighborPrevention(t *testing.T) {
	sched := NewTenantScheduler(SchedulerConfig{
		MaxGlobalConcurrency:     2,
		DefaultTenantConcurrency: 2,
		MaxQueueDepth:            16,
	})

	// Occupy both global slots with noisy_tenant
	rel1, err := sched.Acquire(context.Background(), "noisy_tenant")
	if err != nil {
		t.Fatal(err)
	}
	rel2, err := sched.Acquire(context.Background(), "noisy_tenant")
	if err != nil {
		t.Fatal(err)
	}

	// Queue 3 more queries for noisy_tenant, then 1 query for quiet_tenant
	order := make(chan string, 4)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := sched.Acquire(context.Background(), "noisy_tenant")
			if err != nil {
				return
			}
			order <- "noisy_tenant"
			r()
		}()
	}
	// Let noisy_tenant waiters enqueue first
	time.Sleep(15 * time.Millisecond)

	wg.Add(1)
	go func() {
		defer wg.Done()
		r, err := sched.Acquire(context.Background(), "quiet_tenant")
		if err != nil {
			return
		}
		order <- "quiet_tenant"
		r()
	}()
	time.Sleep(15 * time.Millisecond)

	// Release one slot: quiet_tenant (0 active) MUST be dispatched ahead of noisy_tenant (1 active)!
	rel1()
	first := <-order
	if first != "quiet_tenant" {
		t.Fatalf("expected quiet_tenant to be fairly prioritized over noisy_tenant, got %q first", first)
	}
	rel2()
	wg.Wait()
}

func TestPhase16G_QueueBackpressureAndMemoryAdmission(t *testing.T) {
	mem := NewMemoryManager(MemoryManagerConfig{
		GlobalLimitBytes:        1000,
		DefaultTenantLimitBytes: 1000,
		DefaultQueryLimitBytes:  1000,
	})
	sched := NewTenantScheduler(SchedulerConfig{
		MaxGlobalConcurrency:     1,
		DefaultTenantConcurrency: 1,
		MaxQueueDepth:            1,
		MemoryManager:            mem,
		MemorySoftLimitRatio:     0.80,
	})

	// 1. Bounded queue backpressure
	rel, err := sched.Acquire(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r, err := sched.Acquire(context.Background(), "t2")
		if err == nil {
			r()
		}
	}()
	time.Sleep(15 * time.Millisecond)

	// Queue is now full (1 waiter == MaxQueueDepth=1); next Acquire must fail fast
	if _, err := sched.Acquire(context.Background(), "t3"); !errors.Is(err, ErrSchedulerQueueFull) {
		t.Fatalf("expected ErrSchedulerQueueFull, got %v", err)
	}
	rel()
	wg.Wait()

	// 2. Memory-aware admission control
	b := mem.BeginQuery("t_mem", 0)
	_ = b.Reserve(850) // 85% > 80% soft limit
	defer b.Release()

	// Normal priority rejected under memory pressure
	if _, err := sched.Acquire(context.Background(), "t_mem"); !errors.Is(err, ErrMemoryPressure) {
		t.Fatalf("expected ErrMemoryPressure for PriorityNormal, got %v", err)
	}
	// Interactive priority still admitted
	relInt, err := sched.Acquire(ContextWithQueryPriority(context.Background(), PriorityInteractive), "t_mem")
	if err != nil {
		t.Fatalf("expected PriorityInteractive to bypass soft memory admission: %v", err)
	}
	relInt()

	st := sched.Stats()
	if st.BackpressureTotal < 2 || st.ActiveGlobal != 0 {
		t.Fatalf("unexpected scheduler stats: %+v", st)
	}
}
