package readadapter

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestP7_NoisyNeighborControls_4AbusiveVs4Normal(t *testing.T) {
	sched := NewTenantScheduler(SchedulerConfig{
		MaxGlobalConcurrency:     4,
		DefaultTenantConcurrency: 2,
		MaxQueueDepth:            64,
	})
	nc := NewNoisyNeighborController(NoisyNeighborConfig{
		TenantCPUQuota:    20 * time.Millisecond,
		QuotaWindow:       100 * time.Millisecond,
		HardQueryTimeout:  25 * time.Millisecond,
		MaxPlanCost:       5000,
		ExpensivePlanCost: 1000,
	})

	// 1. Overly expensive plan rejected immediately before acquiring scheduler slot
	if _, err := nc.Admit(context.Background(), "abusive_0", 12000); !errors.Is(err, ErrPlanCostRejected) {
		t.Fatalf("expected ErrPlanCostRejected, got %v", err)
	}

	// 2. Hard timeout clamps context
	hctx, hcancel := nc.ApplyHardTimeout(context.Background())
	defer hcancel()
	if dl, ok := hctx.Deadline(); !ok || time.Until(dl) > 30*time.Millisecond {
		t.Fatalf("expected HardQueryTimeout <= 25ms, got ok=%v rem=%v", ok, time.Until(dl))
	}

	// 3. 4 abusive tenants vs 4 normal tenants concurrency storm
	var normalMu sync.Mutex
	var normalLatencies []time.Duration
	var abusiveBounded atomic.Uint64

	var wg sync.WaitGroup
	// 4 abusive tenants firing heavy plans
	for a := 0; a < 4; a++ {
		wg.Add(1)
		tid := "abusive_" + string(rune('0'+a))
		go func(tenantID string) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				estCost := 2000.0
				if i%5 == 0 {
					estCost = 8000.0 // over MaxPlanCost
				}
				ctx, aerr := nc.Admit(context.Background(), tenantID, estCost)
				if aerr != nil {
					abusiveBounded.Add(1)
					continue
				}
				qctx, qcancel := nc.ApplyHardTimeout(ctx)
				rel, serr := sched.Acquire(qctx, tenantID)
				if serr != nil {
					qcancel()
					abusiveBounded.Add(1)
					continue
				}
				start := time.Now()
				time.Sleep(8 * time.Millisecond)
				nc.RecordExecution(tenantID, time.Since(start))
				rel()
				qcancel()
			}
		}(tid)
	}

	// 4 normal tenants firing light interactive queries
	for n := 0; n < 4; n++ {
		wg.Add(1)
		tid := "normal_" + string(rune('0'+n))
		go func(tenantID string) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				start := time.Now()
				ctx, aerr := nc.Admit(context.Background(), tenantID, 5.0)
				if aerr != nil {
					t.Errorf("normal tenant %s unexpectedly rejected: %v", tenantID, aerr)
					return
				}
				qctx, qcancel := nc.ApplyHardTimeout(ctx)
				rel, serr := sched.Acquire(qctx, tenantID)
				if serr != nil {
					qcancel()
					continue
				}
				time.Sleep(500 * time.Microsecond)
				elapsed := time.Since(start)
				nc.RecordExecution(tenantID, 500*time.Microsecond)
				rel()
				qcancel()

				normalMu.Lock()
				normalLatencies = append(normalLatencies, elapsed)
				normalMu.Unlock()
				time.Sleep(2 * time.Millisecond)
			}
		}(tid)
	}
	wg.Wait()

	if len(normalLatencies) < 50 {
		t.Fatalf("expected >=50 normal tenant samples, got %d", len(normalLatencies))
	}
	sort.Slice(normalLatencies, func(i, j int) bool { return normalLatencies[i] < normalLatencies[j] })
	p99Idx := (len(normalLatencies) * 99) / 100
	if p99Idx >= len(normalLatencies) {
		p99Idx = len(normalLatencies) - 1
	}
	p99 := normalLatencies[p99Idx]
	cpuRej, costRej, demoted := nc.Stats()
	t.Logf("normal_samples=%d normal_p99=%.2fms abusive_bounded=%d cpu_rejects=%d cost_rejects=%d demoted=%d",
		len(normalLatencies), float64(p99.Microseconds())/1000.0, abusiveBounded.Load(), cpuRej, costRej, demoted)

	if p99 > 52510*time.Microsecond {
		t.Fatalf("normal tenant p99 %.2fms exceeded 52.51ms SLA", float64(p99.Microseconds())/1000.0)
	}
	if cpuRej == 0 || costRej == 0 || demoted == 0 {
		t.Fatalf("expected CPU quota rejects, cost rejects, and demotions > 0, got cpu=%d cost=%d demoted=%d", cpuRej, costRej, demoted)
	}
}
