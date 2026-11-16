package pageserver

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
)

// WALRedoer reconstructs an 8192-byte page from an optional base page and ordered WAL deltas.
type WALRedoer interface {
	Redo(ctx context.Context, key Key, basePage []byte, deltas []WALDelta) ([]byte, error)
}

// DefaultWALRedoer applies FPIs directly in Go and optionally delegates non-FPI delta chains
// to an external stateless PG17 walredo worker binary if configured.
type DefaultWALRedoer struct {
	workerBin string
	mu        sync.Mutex
}

// NewDefaultWALRedoer creates a WALRedoer. If workerBin is empty, only FPI/will-init records are applied.
func NewDefaultWALRedoer(workerBin string) *DefaultWALRedoer {
	return &DefaultWALRedoer{workerBin: workerBin}
}

// Redo applies deltas onto basePage in LSN order and returns the materialized 8192-byte page.
func (r *DefaultWALRedoer) Redo(ctx context.Context, key Key, basePage []byte, deltas []WALDelta) ([]byte, error) {
	var cur []byte
	if len(basePage) == PageSize {
		cur = append([]byte(nil), basePage...)
	}

	var pendingWAL [][]byte
	for _, d := range deltas {
		if (d.Flags&EntryFlagPageImage) != 0 && len(d.PageImage) == PageSize {
			cur = append([]byte(nil), d.PageImage...)
			pendingWAL = nil
			continue
		}
		if (d.Flags & EntryFlagWillInit) != 0 {
			cur = make([]byte, PageSize)
		}
		if len(d.WALRecord) > 0 {
			pendingWAL = append(pendingWAL, d.WALRecord)
		}
	}

	if len(pendingWAL) == 0 {
		if len(cur) == PageSize {
			return cur, nil
		}
		return nil, ErrNotFound
	}

	if r.workerBin == "" {
		if len(cur) == PageSize {
			return cur, nil
		}
		return nil, fmt.Errorf("pageserver: %d non-FPI WAL records for %s require walredo worker", len(pendingWAL), key)
	}

	return r.invokeWorker(ctx, key, cur, pendingWAL)
}

func (r *DefaultWALRedoer) invokeWorker(ctx context.Context, key Key, basePage []byte, records [][]byte) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	cmd := exec.CommandContext(ctx, r.workerBin)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// Frame: [24B key][1B hasBase][8192B basePage if hasBase][4B count]([4B len][record])...
	var keyBuf [24]byte
	key.Encode(keyBuf[:])
	_, _ = stdin.Write(keyBuf[:])
	if len(basePage) == PageSize {
		_, _ = stdin.Write([]byte{1})
		_, _ = stdin.Write(basePage)
	} else {
		_, _ = stdin.Write([]byte{0})
	}
	var cnt [4]byte
	binary.LittleEndian.PutUint32(cnt[:], uint32(len(records)))
	_, _ = stdin.Write(cnt[:])
	for _, rec := range records {
		binary.LittleEndian.PutUint32(cnt[:], uint32(len(rec)))
		_, _ = stdin.Write(cnt[:])
		_, _ = stdin.Write(rec)
	}
	_ = stdin.Close()

	out := make([]byte, PageSize)
	if _, err := io.ReadFull(stdout, out); err != nil {
		_ = cmd.Wait()
		return nil, errors.Join(errors.New("pageserver: walredo worker failed"), err)
	}
	if err := cmd.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}
