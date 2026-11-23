package gateway

import (
	"errors"
	"sync"
	"testing"
)

func TestBufferBudgetGlobalAndSessionLimits(t *testing.T) {
	b := NewBufferBudget(1024)
	s1 := b.NewSession(600, 4)
	s2 := b.NewSession(800, 4)

	if err := s1.Reserve(400); err != nil {
		t.Fatalf("s1.Reserve(400): %v", err)
	}
	if err := s2.Reserve(400); err != nil {
		t.Fatalf("s2.Reserve(400): %v", err)
	}
	if b.UsedBytes() != 800 {
		t.Fatalf("expected global used 800, got %d", b.UsedBytes())
	}

	// Exceeds global limit (800 + 300 > 1024), even though s2 session limit (600) has room.
	if err := s2.Reserve(300); !errors.Is(err, ErrGlobalBufferBudgetExceeded) {
		t.Fatalf("expected ErrGlobalBufferBudgetExceeded, got %v", err)
	}
	if b.UsedBytes() != 800 {
		t.Fatalf("expected global used unchanged at 800, got %d", b.UsedBytes())
	}
	if b.Rejections() != 1 {
		t.Fatalf("expected 1 rejection, got %d", b.Rejections())
	}

	// Exceeds session byte limit (400 + 250 > 600).
	if err := s1.Reserve(250); !errors.Is(err, ErrSessionBufferBytesExceeded) {
		t.Fatalf("expected ErrSessionBufferBytesExceeded, got %v", err)
	}

	// Double release & over-release must not cause negative accounting.
	s1.Release(1000, 10)
	s1.ReleaseAll()
	s1.ReleaseAll()
	if s1.UsedBytes() != 0 {
		t.Fatalf("expected s1 used 0, got %d", s1.UsedBytes())
	}
	if b.UsedBytes() != 400 {
		t.Fatalf("expected global used 400 after s1 release, got %d", b.UsedBytes())
	}

	// Message count limit check on s2 (already has 1 msg, max 4).
	for i := 0; i < 3; i++ {
		if err := s2.Reserve(10); err != nil {
			t.Fatalf("s2.Reserve(10) iter %d: %v", i, err)
		}
	}
	if err := s2.Reserve(10); !errors.Is(err, ErrSessionBufferMsgsExceeded) {
		t.Fatalf("expected ErrSessionBufferMsgsExceeded, got %v", err)
	}

	s2.ReleaseAll()
	if b.UsedBytes() != 0 {
		t.Fatalf("expected global used 0 after s2 release, got %d", b.UsedBytes())
	}
}

func TestBufferBudgetConcurrency(t *testing.T) {
	b := NewBufferBudget(64 * 1024)
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := b.NewSession(8*1024, 128)
			defer s.ReleaseAll()
			for i := 0; i < 100; i++ {
				if err := s.Reserve(1024); err == nil {
					if i%3 == 0 {
						s.Release(1024, 1)
					}
				}
				if i%10 == 0 {
					s.ReleaseAll()
				}
			}
		}()
	}
	wg.Wait()
	if b.UsedBytes() != 0 {
		t.Fatalf("expected 0 leaked bytes after concurrent sessions, got %d", b.UsedBytes())
	}
}
