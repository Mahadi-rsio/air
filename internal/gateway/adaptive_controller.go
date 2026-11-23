package gateway

import (
	"math"
	"sync"
	"time"
)

// CellTelemetry is the gateway-side view of a database Cell's resource pressure snapshot.
type CellTelemetry struct {
	Timestamp          time.Time `json:"timestamp"`
	TenantID           string    `json:"tenant_id,omitempty"`
	MemoryCurrentBytes uint64    `json:"memory_current_bytes"`
	MemoryHighBytes    uint64    `json:"memory_high_bytes"`
	MemoryMaxBytes     uint64    `json:"memory_max_bytes"`
	MemoryHighEvents   uint64    `json:"memory_high_events"`
	MemoryMaxEvents    uint64    `json:"memory_max_events"`
	MemoryOOMEvents    uint64    `json:"memory_oom_events"`
	MemoryPSISomeAvg10 float64   `json:"memory_psi_some_avg10"`
	MemoryPSIFullAvg10 float64   `json:"memory_psi_full_avg10"`
	CPUPSISomeAvg10    float64   `json:"cpu_psi_some_avg10"`
	IOPSIMaxAvg10      float64   `json:"io_psi_max_avg10"`
	CPUUsageUsec       uint64    `json:"cpu_usage_usec"`
	CPUThrottledUsec   uint64    `json:"cpu_throttled_usec"`
	PGActiveConns      int       `json:"pg_active_conns,omitempty"`
	PGActiveQueries    int       `json:"pg_active_queries,omitempty"`
	WALBytesPerSec     float64   `json:"wal_bytes_per_sec,omitempty"`
	Source             string    `json:"source,omitempty"`
}

// MemoryRatio returns MemoryCurrentBytes / MemoryHighBytes (or MemoryMaxBytes if MemoryHighBytes == 0).
func (t CellTelemetry) MemoryRatio() float64 {
	limit := t.MemoryHighBytes
	if limit == 0 {
		limit = t.MemoryMaxBytes
	}
	if limit == 0 {
		return 0
	}
	return float64(t.MemoryCurrentBytes) / float64(limit)
}

// TelemetryStore holds the latest CellTelemetry snapshot with freshness validation.
type TelemetryStore struct {
	mu         sync.RWMutex
	latest     CellTelemetry
	receivedAt time.Time
	ttl        time.Duration
}

func NewTelemetryStore(ttl time.Duration) *TelemetryStore {
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	return &TelemetryStore{ttl: ttl}
}

func (s *TelemetryStore) Update(snap CellTelemetry, now time.Time) {
	if s == nil {
		return
	}
	if snap.Timestamp.IsZero() {
		snap.Timestamp = now
	}
	s.mu.Lock()
	s.latest = snap
	s.receivedAt = now
	s.mu.Unlock()
}

// Latest returns the most recent telemetry snapshot and whether it is fresh at `now`.
func (s *TelemetryStore) Latest(now time.Time) (CellTelemetry, bool) {
	if s == nil {
		return CellTelemetry{}, false
	}
	s.mu.RLock()
	snap := s.latest
	recv := s.receivedAt
	ttl := s.ttl
	s.mu.RUnlock()

	if recv.IsZero() || snap.Timestamp.IsZero() {
		return snap, false
	}
	if now.Sub(recv) > ttl || now.Sub(snap.Timestamp) > ttl {
		return snap, false
	}
	return snap, true
}

// AdaptiveController implements an AIMD + hysteresis concurrency controller
// that adjusts WriteGovernor's effective global write limit based on CellTelemetry.
type AdaptiveController struct {
	gov       *WriteGovernor
	telemetry *TelemetryStore
	interval  time.Duration

	mu              sync.Mutex
	healthyStreak   int
	pressuredStreak int
	lastHighEvents  uint64
	lastOOMEvents   uint64
	hasPrevEvents   bool

	hysteresisUp   int
	hysteresisDown int

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func NewAdaptiveController(gov *WriteGovernor, telemetry *TelemetryStore, interval time.Duration) *AdaptiveController {
	if interval <= 0 {
		interval = time.Second
	}
	return &AdaptiveController{
		gov:            gov,
		telemetry:      telemetry,
		interval:       interval,
		hysteresisUp:   3,
		hysteresisDown: 2,
		stopCh:         make(chan struct{}),
	}
}

func (c *AdaptiveController) Start() {
	if c == nil || c.gov == nil {
		return
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()
		for {
			select {
			case <-c.stopCh:
				return
			case now := <-ticker.C:
				c.Step(now)
			}
		}
	}()
}

func (c *AdaptiveController) Stop() {
	if c == nil {
		return
	}
	c.stopOnce.Do(func() { close(c.stopCh) })
	c.wg.Wait()
}

// Step evaluates telemetry at `now` and applies one AIMD control step.
// Exposed directly for deterministic unit testing.
func (c *AdaptiveController) Step(now time.Time) int {
	if c == nil || c.gov == nil {
		return 0
	}
	snap := c.gov.Snapshot()
	current := snap.EffectiveGlobalLimit
	minG := snap.MinGlobal
	maxG := snap.MaxGlobal
	if minG <= 0 {
		minG = 1
	}
	if maxG < minG {
		maxG = minG
	}

	tel, fresh := c.telemetry.Latest(now)
	c.mu.Lock()
	defer c.mu.Unlock()

	if !fresh {
		c.healthyStreak = 0
		c.pressuredStreak = 0
		c.gov.SetControllerState(ControllerStateStaleFallback)
		safeCap := maxG / 2
		if safeCap < minG {
			safeCap = minG
		}
		if current > safeCap {
			return c.gov.SetEffectiveGlobalLimit(safeCap)
		}
		return current
	}

	highDelta := uint64(0)
	oomDelta := uint64(0)
	if c.hasPrevEvents {
		if tel.MemoryHighEvents > c.lastHighEvents {
			highDelta = tel.MemoryHighEvents - c.lastHighEvents
		}
		if tel.MemoryOOMEvents > c.lastOOMEvents {
			oomDelta = tel.MemoryOOMEvents - c.lastOOMEvents
		}
	}
	c.lastHighEvents = tel.MemoryHighEvents
	c.lastOOMEvents = tel.MemoryOOMEvents
	c.hasPrevEvents = true

	memRatio := tel.MemoryRatio()
	memCritical := memRatio >= 0.95 || oomDelta > 0 || tel.MemoryPSIFullAvg10 >= 10.0
	memPressured := memRatio >= 0.85 ||
		highDelta > 0 ||
		tel.MemoryPSISomeAvg10 >= 10.0 ||
		tel.MemoryPSIFullAvg10 >= 2.0
	ioPressured := tel.IOPSIMaxAvg10 >= 20.0 || snap.QueueWaitP95Us >= 250000
	// CPU contention is evaluated as an independent CPU-starvation signal,
	// not as evidence of memory OOM risk (healthy 2-vCPU OLTP writes routinely
	// produce 25-65% cpu.some.avg10 with zero memory pressure).
	cpuPressured := tel.CPUPSISomeAvg10 >= 80.0

	critical := memCritical
	pressured := critical || memPressured || ioPressured || cpuPressured

	if critical {
		c.healthyStreak = 0
		c.pressuredStreak++
		c.gov.SetControllerState(ControllerStateCritical)
		next := int(math.Floor(float64(current) * 0.5))
		if next >= current && current > minG {
			next = current - 1
		}
		if next < minG {
			next = minG
		}
		return c.gov.SetEffectiveGlobalLimit(next)
	}

	if pressured {
		c.healthyStreak = 0
		c.pressuredStreak++
		c.gov.SetControllerState(ControllerStatePressured)
		if c.pressuredStreak >= c.hysteresisDown {
			c.pressuredStreak = 0
			next := int(math.Floor(float64(current) * 0.75))
			if next >= current && current > minG {
				next = current - 1
			}
			if next < minG {
				next = minG
			}
			return c.gov.SetEffectiveGlobalLimit(next)
		}
		return current
	}

	// Healthy band with hysteresis deadband (low memory/IO pressure, no extreme CPU starvation).
	healthy := memRatio < 0.75 &&
		tel.MemoryPSISomeAvg10 < 3.0 &&
		tel.MemoryPSIFullAvg10 < 0.5 &&
		tel.IOPSIMaxAvg10 < 10.0 &&
		tel.CPUPSISomeAvg10 < 65.0

	c.pressuredStreak = 0
	if healthy {
		c.healthyStreak++
		c.gov.SetControllerState(ControllerStateHealthy)
		if c.healthyStreak >= c.hysteresisUp {
			c.healthyStreak = 0
			if current < maxG {
				return c.gov.SetEffectiveGlobalLimit(current + 1)
			}
		}
	}
	return current
}
