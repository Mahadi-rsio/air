package gateway

import (
	"testing"
	"time"
)

func TestAdaptiveControllerAIMDAndHysteresis(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WriteGovernorEnabled = true
	cfg.WriteAdaptiveEnabled = true
	cfg.WriteMinGlobal = 2
	cfg.WriteMaxGlobal = 16
	cfg.WriteMaxPerTenant = 8

	gov := NewWriteGovernor(cfg)
	// Startup must not jump to maxGlobal (16); starts conservatively at 8.
	if got := gov.EffectiveGlobalLimit(); got != 8 {
		t.Fatalf("expected conservative startup limit 8, got %d", got)
	}

	store := NewTelemetryStore(15 * time.Second)
	ctrl := NewAdaptiveController(gov, store, time.Second)
	now := time.Now()

	// Missing telemetry -> StaleFallback, never increases above safe cap.
	if got := ctrl.Step(now); got != 8 {
		t.Fatalf("expected 8 on missing telemetry, got %d", got)
	}
	if gov.Snapshot().ControllerState != ControllerStateStaleFallback {
		t.Fatalf("expected StaleFallback, got %v", gov.Snapshot().ControllerState)
	}

	// Healthy ticks: requires hysteresisUp (3) consecutive ticks for +1.
	healthyTel := CellTelemetry{
		Timestamp:          now,
		MemoryCurrentBytes: 200 << 20,
		MemoryHighBytes:    512 << 20,
		MemoryMaxBytes:     1024 << 20,
	}
	store.Update(healthyTel, now)
	if got := ctrl.Step(now.Add(1 * time.Second)); got != 8 {
		t.Fatalf("tick 1 healthy should hold at 8, got %d", got)
	}
	if got := ctrl.Step(now.Add(2 * time.Second)); got != 8 {
		t.Fatalf("tick 2 healthy should hold at 8, got %d", got)
	}
	if got := ctrl.Step(now.Add(3 * time.Second)); got != 9 {
		t.Fatalf("tick 3 healthy should additive-increase to 9, got %d", got)
	}

	// Sustained pressure: requires hysteresisDown (2) ticks for 0.75x decrease.
	pressuredTel := CellTelemetry{
		Timestamp:          now.Add(4 * time.Second),
		MemoryCurrentBytes: 450 << 20, // ~88% of 512 MiB
		MemoryHighBytes:    512 << 20,
		MemoryMaxBytes:     1024 << 20,
		MemoryPSISomeAvg10: 12.0,
	}
	store.Update(pressuredTel, now.Add(4*time.Second))
	if got := ctrl.Step(now.Add(4 * time.Second)); got != 9 {
		t.Fatalf("single pressured tick should hold at 9 (hysteresis), got %d", got)
	}
	if got := ctrl.Step(now.Add(5 * time.Second)); got != 6 {
		t.Fatalf("second pressured tick should multiplicative-decrease 9*0.75=6, got %d", got)
	}

	// Critical pressure (OOM event or >=95% memory): immediate 0.5x decrease.
	critTel := CellTelemetry{
		Timestamp:          now.Add(6 * time.Second),
		MemoryCurrentBytes: 500 << 20,
		MemoryHighBytes:    512 << 20,
		MemoryOOMEvents:    1,
	}
	store.Update(critTel, now.Add(6*time.Second))
	if got := ctrl.Step(now.Add(6 * time.Second)); got != 3 {
		t.Fatalf("critical pressure should immediately halve 6->3, got %d", got)
	}

	// Stale telemetry after TTL (15s) -> transitions to StaleFallback.
	if got := ctrl.Step(now.Add(30 * time.Second)); got != 3 {
		t.Fatalf("stale telemetry should keep 3 (<= safeCap), got %d", got)
	}
	if gov.Snapshot().ControllerState != ControllerStateStaleFallback {
		t.Fatalf("expected StaleFallback on stale telemetry, got %v", gov.Snapshot().ControllerState)
	}
}
