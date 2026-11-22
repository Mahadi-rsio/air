package backup

import (
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

var pg2000Epoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// ExtractCommitTimeIndexFromWAL scans a raw PostgreSQL 17 WAL segment and extracts
// a sparse, monotonically non-decreasing time->LSN index from XLOG_XACT_COMMIT records.
// At most one entry per minStep is retained (plus the final commit in the segment).
func ExtractCommitTimeIndexFromWAL(walBytes []byte, minStep time.Duration) ([]TimeLSNEntry, error) {
	if len(walBytes) == 0 || len(walBytes)%pageserver.XLogPageSize != 0 {
		return nil, fmt.Errorf("backup: invalid WAL segment length %d", len(walBytes))
	}
	if minStep <= 0 {
		minStep = time.Second
	}
	recs, err := pageserver.ParseWALPages(walBytes)
	if err != nil && len(recs) == 0 {
		return nil, err
	}
	var raw []TimeLSNEntry
	for _, rec := range recs {
		if rec.Rmid != pageserver.RmXactID {
			continue
		}
		op := rec.Info & pageserver.XLogXactOpMask
		if op != pageserver.XLogXactCommit && op != pageserver.XLogXactCommitPrepared {
			continue
		}
		if len(rec.MainData) < 8 {
			continue
		}
		usec := int64(binary.LittleEndian.Uint64(rec.MainData[0:8]))
		ct := pg2000Epoch.Add(time.Duration(usec) * time.Microsecond).UTC()
		raw = append(raw, TimeLSNEntry{
			CommitTime: ct,
			LSN:        FormatLSN(rec.EndLSN),
		})
	}
	return BuildSparseTimeIndex(raw, minStep), nil
}

// BuildSparseTimeIndex normalizes commit entries into a sparse, monotonic time->LSN index
// (handling backward clock skew by clamping timestamps to the running maximum).
func BuildSparseTimeIndex(entries []TimeLSNEntry, minStep time.Duration) []TimeLSNEntry {
	if len(entries) == 0 {
		return nil
	}
	if minStep <= 0 {
		minStep = time.Second
	}
	type parsedEntry struct {
		t   time.Time
		lsn uint64
	}
	parsed := make([]parsedEntry, 0, len(entries))
	for _, e := range entries {
		lsn, err := ParseLSN(e.LSN)
		if err != nil || e.CommitTime.IsZero() {
			continue
		}
		parsed = append(parsed, parsedEntry{t: e.CommitTime.UTC(), lsn: lsn})
	}
	if len(parsed) == 0 {
		return nil
	}
	sort.Slice(parsed, func(i, j int) bool {
		return parsed[i].lsn < parsed[j].lsn
	})

	// Enforce monotonic non-decreasing timestamps (handles backward NTP clock skew)
	// and downsample to at most one entry per minStep, always keeping the last entry.
	out := make([]TimeLSNEntry, 0, len(parsed))
	var maxT time.Time
	var lastKeptT time.Time
	for i, p := range parsed {
		t := p.t
		if t.Before(maxT) {
			t = maxT
		} else {
			maxT = t
		}
		isLast := i == len(parsed)-1
		if i == 0 || t.Sub(lastKeptT) >= minStep || isLast {
			ent := TimeLSNEntry{CommitTime: t, LSN: FormatLSN(p.lsn)}
			if isLast && len(out) > 0 && out[len(out)-1].LSN == ent.LSN {
				out[len(out)-1] = ent
			} else if isLast && len(out) > 0 && t.Sub(lastKeptT) < minStep && len(out) > 1 {
				// Keep the final commit timestamp/LSN accurate for upper-bound checks.
				out = append(out, ent)
			} else {
				out = append(out, ent)
			}
			lastKeptT = t
		}
	}
	return out
}

// LookupTargetTimeLSN converts targetTime into a bracketed LSN via binary search
// over a sparse monotonic TimeIndex.
func LookupTargetTimeLSN(index []TimeLSNEntry, targetTime time.Time) (uint64, error) {
	if len(index) == 0 {
		return 0, fmt.Errorf("%w: empty time index", ErrTargetOutOfRange)
	}
	targetUTC := targetTime.UTC()
	firstT := index[0].CommitTime.UTC()
	lastT := index[len(index)-1].CommitTime.UTC()
	if targetUTC.Before(firstT) {
		return 0, fmt.Errorf("%w: target time %s precedes earliest indexed commit time %s",
			ErrTargetOutOfRange, targetUTC.Format(time.RFC3339Nano), firstT.Format(time.RFC3339Nano))
	}
	if targetUTC.After(lastT) {
		return 0, fmt.Errorf("%w: target time %s exceeds latest indexed commit time %s",
			ErrTargetOutOfRange, targetUTC.Format(time.RFC3339Nano), lastT.Format(time.RFC3339Nano))
	}

	// Binary search for the first entry with CommitTime >= targetUTC.
	idx := sort.Search(len(index), func(i int) bool {
		return !index[i].CommitTime.UTC().Before(targetUTC)
	})
	if idx >= len(index) {
		idx = len(index) - 1
	}
	lsn, err := ParseLSN(index[idx].LSN)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid LSN in time index: %v", ErrInvalidBackupManifest, err)
	}
	return lsn, nil
}
