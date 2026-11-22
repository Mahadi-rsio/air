package readadapter

import (
	"container/list"
	"sync"
)

const (
	defaultMaxAdaptiveEntries = 1024
	adaptiveEMAAlpha          = 0.25
)

// AdaptiveKey identifies a tenant relation for runtime execution feedback.
type AdaptiveKey struct {
	TenantID string
	RelOID   uint32
}

// AdaptiveFeedback stores bounded exponential-moving-average runtime feedback.
type AdaptiveFeedback struct {
	SelectivityFactor float64
	L1HitRatio        float64
	L2HitRatio        float64
	SampleCount       uint64
	LastActualRows    uint64
	LastPagesRead     uint64
	LastExecNs        uint64
}

type adaptiveEntry struct {
	key      AdaptiveKey
	feedback AdaptiveFeedback
}

// AdaptiveTracker maintains bounded, tenant-isolated runtime execution feedback
// to refine future cost estimates without mid-query plan changes.
type AdaptiveTracker struct {
	mu         sync.Mutex
	maxEntries int
	ll         *list.List
	entries    map[AdaptiveKey]*list.Element
}

// NewAdaptiveTracker creates a bounded AdaptiveTracker.
func NewAdaptiveTracker(maxEntries int) *AdaptiveTracker {
	if maxEntries <= 0 {
		maxEntries = defaultMaxAdaptiveEntries
	}
	return &AdaptiveTracker{
		maxEntries: maxEntries,
		ll:         list.New(),
		entries:    make(map[AdaptiveKey]*list.Element),
	}
}

// Record updates the EMA feedback for (tenantID, relOID) after query execution.
func (at *AdaptiveTracker) Record(
	tenantID string,
	relOID uint32,
	estRows uint64,
	actualRows uint64,
	pagesRead uint64,
	l1Hits uint64,
	l2Hits uint64,
	execNs uint64,
) {
	if at == nil || tenantID == "" || relOID == 0 {
		return
	}
	denom := float64(estRows)
	if denom < 1.0 {
		denom = 1.0
	}
	ratio := float64(actualRows) / denom
	if ratio < 0.1 {
		ratio = 0.1
	} else if ratio > 10.0 {
		ratio = 10.0
	}

	var l1Ratio, l2Ratio float64
	if pagesRead > 0 {
		l1Ratio = float64(l1Hits) / float64(pagesRead)
		l2Ratio = float64(l2Hits) / float64(pagesRead)
		if l1Ratio > 1.0 {
			l1Ratio = 1.0
		}
		if l2Ratio > 1.0 {
			l2Ratio = 1.0
		}
	}

	key := AdaptiveKey{TenantID: tenantID, RelOID: relOID}
	at.mu.Lock()
	defer at.mu.Unlock()

	if elem, ok := at.entries[key]; ok {
		ent := elem.Value.(*adaptiveEntry)
		fb := &ent.feedback
		fb.SelectivityFactor = (1.0-adaptiveEMAAlpha)*fb.SelectivityFactor + adaptiveEMAAlpha*ratio
		if pagesRead > 0 {
			fb.L1HitRatio = (1.0-adaptiveEMAAlpha)*fb.L1HitRatio + adaptiveEMAAlpha*l1Ratio
			fb.L2HitRatio = (1.0-adaptiveEMAAlpha)*fb.L2HitRatio + adaptiveEMAAlpha*l2Ratio
		}
		fb.SampleCount++
		fb.LastActualRows = actualRows
		fb.LastPagesRead = pagesRead
		fb.LastExecNs = execNs
		at.ll.MoveToFront(elem)
		return
	}

	for at.ll.Len() >= at.maxEntries {
		back := at.ll.Back()
		if back == nil {
			break
		}
		at.ll.Remove(back)
		delete(at.entries, back.Value.(*adaptiveEntry).key)
	}

	fb := AdaptiveFeedback{
		SelectivityFactor: ratio,
		L1HitRatio:        l1Ratio,
		L2HitRatio:        l2Ratio,
		SampleCount:       1,
		LastActualRows:    actualRows,
		LastPagesRead:     pagesRead,
		LastExecNs:        execNs,
	}
	elem := at.ll.PushFront(&adaptiveEntry{key: key, feedback: fb})
	at.entries[key] = elem
}

// Get returns the recorded AdaptiveFeedback for (tenantID, relOID).
func (at *AdaptiveTracker) Get(tenantID string, relOID uint32) (AdaptiveFeedback, bool) {
	if at == nil || tenantID == "" || relOID == 0 {
		return AdaptiveFeedback{}, false
	}
	key := AdaptiveKey{TenantID: tenantID, RelOID: relOID}
	at.mu.Lock()
	defer at.mu.Unlock()
	if elem, ok := at.entries[key]; ok {
		at.ll.MoveToFront(elem)
		return elem.Value.(*adaptiveEntry).feedback, true
	}
	return AdaptiveFeedback{}, false
}

// InvalidateTenant removes all adaptive entries for tenantID.
func (at *AdaptiveTracker) InvalidateTenant(tenantID string) {
	if at == nil {
		return
	}
	at.mu.Lock()
	defer at.mu.Unlock()
	for k, elem := range at.entries {
		if k.TenantID == tenantID {
			at.ll.Remove(elem)
			delete(at.entries, k)
		}
	}
}
