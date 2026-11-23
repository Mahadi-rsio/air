package gateway

import (
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrGlobalBufferBudgetExceeded = errors.New("gateway global protocol buffer budget exceeded")
	ErrSessionBufferBytesExceeded = errors.New("session protocol buffer byte limit exceeded")
	ErrSessionBufferMsgsExceeded  = errors.New("session protocol buffer message limit exceeded")
)

// BufferBudget enforces a gateway-wide aggregate byte budget for buffered
// client protocol messages (e.g. extended-query pipelines) alongside per-session limits.
type BufferBudget struct {
	globalMaxBytes atomic.Int64
	globalUsed     atomic.Int64
	rejections     atomic.Uint64
}

// NewBufferBudget creates a BufferBudget with the given global byte limit.
// A limit <= 0 disables the global byte cap while still tracking usage.
func NewBufferBudget(globalMaxBytes int64) *BufferBudget {
	b := &BufferBudget{}
	b.globalMaxBytes.Store(globalMaxBytes)
	return b
}

// SetGlobalMaxBytes updates the global byte budget at runtime.
func (b *BufferBudget) SetGlobalMaxBytes(maxBytes int64) {
	if b != nil {
		b.globalMaxBytes.Store(maxBytes)
	}
}

// GlobalMaxBytes returns the configured global byte budget.
func (b *BufferBudget) GlobalMaxBytes() int64 {
	if b == nil {
		return 0
	}
	return b.globalMaxBytes.Load()
}

// UsedBytes returns the currently reserved aggregate bytes across all sessions.
func (b *BufferBudget) UsedBytes() int64 {
	if b == nil {
		return 0
	}
	return b.globalUsed.Load()
}

// Rejections returns the total number of buffer reservation rejections.
func (b *BufferBudget) Rejections() uint64 {
	if b == nil {
		return 0
	}
	return b.rejections.Load()
}

// NewSession creates a per-session tracker bound to this global budget.
func (b *BufferBudget) NewSession(sessionMaxBytes int64, sessionMaxMsgs int) *SessionBufferTracker {
	return &SessionBufferTracker{
		budget:          b,
		sessionMaxBytes: sessionMaxBytes,
		sessionMaxMsgs:  sessionMaxMsgs,
	}
}

// SessionBufferTracker accounts for protocol bytes and messages buffered by a single client session.
// All Release operations clamp to the session's currently reserved balance, preventing negative
// accounting and double-release bugs on error, disconnect, or cancellation paths.
type SessionBufferTracker struct {
	budget          *BufferBudget
	sessionMaxBytes int64
	sessionMaxMsgs  int

	mu        sync.Mutex
	usedBytes int64
	usedMsgs  int
}

// Reserve attempts to reserve n bytes and 1 message slot against both session and global budgets.
func (s *SessionBufferTracker) Reserve(n int) error {
	if s == nil || n < 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sessionMaxMsgs > 0 && s.usedMsgs+1 > s.sessionMaxMsgs {
		if s.budget != nil {
			s.budget.rejections.Add(1)
		}
		return ErrSessionBufferMsgsExceeded
	}
	n64 := int64(n)
	if s.sessionMaxBytes > 0 && s.usedBytes+n64 > s.sessionMaxBytes {
		if s.budget != nil {
			s.budget.rejections.Add(1)
		}
		return ErrSessionBufferBytesExceeded
	}

	if s.budget != nil && n64 > 0 {
		maxGlobal := s.budget.globalMaxBytes.Load()
		if maxGlobal > 0 {
			for {
				cur := s.budget.globalUsed.Load()
				if cur+n64 > maxGlobal {
					s.budget.rejections.Add(1)
					return ErrGlobalBufferBudgetExceeded
				}
				if s.budget.globalUsed.CompareAndSwap(cur, cur+n64) {
					break
				}
			}
		} else {
			s.budget.globalUsed.Add(n64)
		}
	}

	s.usedBytes += n64
	s.usedMsgs++
	return nil
}

// Release releases up to n bytes and decrements the message count by msgs (clamped to >= 0).
func (s *SessionBufferTracker) Release(n int, msgs int) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if msgs > 0 {
		if msgs >= s.usedMsgs {
			s.usedMsgs = 0
		} else {
			s.usedMsgs -= msgs
		}
	}
	if n <= 0 || s.usedBytes <= 0 {
		return
	}
	rel := int64(n)
	if rel > s.usedBytes {
		rel = s.usedBytes
	}
	s.usedBytes -= rel
	if s.budget != nil && rel > 0 {
		s.budget.globalUsed.Add(-rel)
	}
}

// ReleaseAll releases all bytes and message slots currently held by this session.
// Safe to call multiple times (idempotent).
func (s *SessionBufferTracker) ReleaseAll() {
	if s == nil {
		return
	}
	s.mu.Lock()
	rel := s.usedBytes
	s.usedBytes = 0
	s.usedMsgs = 0
	s.mu.Unlock()

	if s.budget != nil && rel > 0 {
		s.budget.globalUsed.Add(-rel)
	}
}

// UsedBytes returns the bytes currently reserved by this session.
func (s *SessionBufferTracker) UsedBytes() int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.usedBytes
}
