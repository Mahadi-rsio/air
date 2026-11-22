package gateway

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestP4_BlueGreenAtomicSwapRollbackAndEpochCAS(t *testing.T) {
	disc := &TenantDiscovery{
		cache: map[string]TenantRoute{
			"tenant_blue": {
				TenantID:    "tenant_blue",
				State:       "ACTIVE",
				Host:        "127.0.0.1",
				Port:        15432,
				AppPassword: "pw-blue",
			},
			"tenant_green": {
				TenantID:    "tenant_green",
				State:       "ACTIVE",
				Host:        "127.0.0.1",
				Port:        15433,
				AppPassword: "pw-green",
			},
		},
		aliases: map[string]BlueGreenAlias{
			"prod_db": {
				DatabaseName:   "prod_db",
				ActiveTenantID: "tenant_blue",
				Epoch:          1,
			},
		},
	}

	// 1. Concurrent readers during swap must NEVER observe a half-swapped state
	// (route.TenantID == "tenant_blue" must always have Port 15432 & "pw-blue";
	//  route.TenantID == "tenant_green" must always have Port 15433 & "pw-green").
	var stop atomic.Bool
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				r, _, err := disc.ResolveTenant("prod_db")
				if err != nil {
					t.Errorf("ResolveTenant(prod_db) failed: %v", err)
					return
				}
				switch r.TenantID {
				case "tenant_blue":
					if r.Port != 15432 || r.AppPassword != "pw-blue" {
						t.Errorf("half-swapped blue state: %+v", r)
						return
					}
				case "tenant_green":
					if r.Port != 15433 || r.AppPassword != "pw-green" {
						t.Errorf("half-swapped green state: %+v", r)
						return
					}
				default:
					t.Errorf("unexpected tenant %q", r.TenantID)
					return
				}
			}
		}()
	}

	// 2. Two concurrent swaps at expectedEpoch=1: exactly ONE must win!
	var wins atomic.Int32
	var conflicts atomic.Int32
	var swapWg sync.WaitGroup
	for i := 0; i < 2; i++ {
		swapWg.Add(1)
		go func() {
			defer swapWg.Done()
			_, err := disc.SwapDatabaseTenant("prod_db", "tenant_green", 1, 200*time.Millisecond)
			if err == nil {
				wins.Add(1)
			} else if errors.Is(err, ErrSwapEpochConflict) {
				conflicts.Add(1)
			} else {
				t.Errorf("unexpected swap error: %v", err)
			}
		}()
	}
	swapWg.Wait()
	if wins.Load() != 1 || conflicts.Load() != 1 {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("expected 1 CAS winner and 1 conflict, got wins=%d conflicts=%d", wins.Load(), conflicts.Load())
	}

	// Verify connection string "prod_db" now resolves to tenant_green.
	r, _, err := disc.ResolveTenant("prod_db")
	if err != nil || r.TenantID != "tenant_green" {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("expected prod_db -> tenant_green, got %+v err=%v", r, err)
	}

	// 3. Rollback within retention window succeeds and reverts to tenant_blue.
	alias, err := disc.RollbackDatabaseTenant("prod_db", 2)
	if err != nil || alias.ActiveTenantID != "tenant_blue" {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("rollback failed: alias=%+v err=%v", alias, err)
	}
	r, _, err = disc.ResolveTenant("prod_db")
	if err != nil || r.TenantID != "tenant_blue" {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("expected prod_db -> tenant_blue after rollback, got %+v err=%v", r, err)
	}

	stop.Store(true)
	wg.Wait()

	// 4. Rollback after retention window expires must fail cleanly.
	time.Sleep(220 * time.Millisecond)
	if _, err := disc.RollbackDatabaseTenant("prod_db", 3); !errors.Is(err, ErrRollbackUnavailable) {
		t.Fatalf("expected ErrRollbackUnavailable after retention expiry, got %v", err)
	}
}
