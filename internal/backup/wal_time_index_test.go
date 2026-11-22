package backup

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

func buildXactCommitWALPage(pageAddr uint64, commits []struct {
	lsnOff uint32
	t      time.Time
}) []byte {
	page := make([]byte, pageserver.XLogPageSize)
	// Short page header (24B): magic=0xD116, info=0, tli=1, pageaddr, remLen=0
	binary.LittleEndian.PutUint16(page[0:2], pageserver.XLogPageMagic)
	binary.LittleEndian.PutUint16(page[2:4], 0)
	binary.LittleEndian.PutUint32(page[4:8], 1)
	binary.LittleEndian.PutUint64(page[8:16], pageAddr)
	binary.LittleEndian.PutUint32(page[16:20], 0)

	pos := pageserver.SizeOfShortPHD
	for _, c := range commits {
		// 24B XLogRecord + 2B XLRBlockIDDataShort + 8B xact_time = 34B
		const recLen = 34
		if pos+recLen > pageserver.XLogPageSize {
			break
		}
		binary.LittleEndian.PutUint32(page[pos:pos+4], recLen)
		binary.LittleEndian.PutUint32(page[pos+4:pos+8], 100) // xid
		page[pos+16] = pageserver.XLogXactCommit
		page[pos+17] = pageserver.RmXactID
		page[pos+24] = pageserver.XLRBlockIDDataShort
		page[pos+25] = 8
		usec := c.t.UTC().Sub(pg2000Epoch) / time.Microsecond
		binary.LittleEndian.PutUint64(page[pos+26:pos+34], uint64(usec))
		pos += (recLen + 7) &^ 7
	}
	return page
}

func TestP3_ExtractCommitTimeIndexAndSparseSampling(t *testing.T) {
	t0 := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	page := buildXactCommitWALPage(0x1000000, []struct {
		lsnOff uint32
		t      time.Time
	}{
		{0, t0},
		{1, t0.Add(200 * time.Millisecond)},  // within 1s -> downsampled
		{2, t0.Add(1500 * time.Millisecond)}, // >= 1s -> kept
		{3, t0.Add(1700 * time.Millisecond)}, // last -> kept
	})

	idx, err := ExtractCommitTimeIndexFromWAL(page, time.Second)
	if err != nil {
		t.Fatalf("ExtractCommitTimeIndexFromWAL: %v", err)
	}
	if len(idx) != 3 {
		t.Fatalf("expected 3 sparse entries (first, +1.5s, last), got %d: %+v", len(idx), idx)
	}
	if !idx[0].CommitTime.Equal(t0) {
		t.Fatalf("entry 0 time=%v want=%v", idx[0].CommitTime, t0)
	}
	if !idx[2].CommitTime.Equal(t0.Add(1700 * time.Millisecond)) {
		t.Fatalf("last entry time=%v want=%v", idx[2].CommitTime, t0.Add(1700*time.Millisecond))
	}
}

func TestP3_ClockSkewMonotonicAndBoundaries(t *testing.T) {
	t0 := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	// Simulate backward NTP clock skew at LSN 0/1000200 (12:00:02 -> 11:59:55 -> 12:00:05)
	entries := []TimeLSNEntry{
		{CommitTime: t0, LSN: "0/1000100"},
		{CommitTime: t0.Add(2 * time.Second), LSN: "0/1000200"},
		{CommitTime: t0.Add(-5 * time.Second), LSN: "0/1000300"}, // backward skew!
		{CommitTime: t0.Add(5 * time.Second), LSN: "0/1000400"},
	}
	idx := BuildSparseTimeIndex(entries, time.Second)
	for i := 1; i < len(idx); i++ {
		if idx[i].CommitTime.Before(idx[i-1].CommitTime) {
			t.Fatalf("clock skew produced non-monotonic time index at %d: %+v", i, idx)
		}
	}

	m := &BackupManifest{
		ManifestVersion: ManifestVersion,
		BackupID:        "bkp-1",
		TenantID:        "t-1",
		Generation:      "gen-1",
		Type:            BackupTypeFull,
		State:           BackupStateSucceeded,
		ConsistencyLSN:  "0/1000100",
		WALStartLSN:     "0/1000000",
		WALEndLSN:       "0/1000500",
		ConsistencyTime: t0,
		TimeIndex:       idx,
	}
	if err := m.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := m.VerifyChecksum(); err != nil {
		t.Fatal(err)
	}

	// Exact lower boundary
	rt, _ := NewTimeRecoveryTarget(t0.Format(time.RFC3339Nano))
	if err := rt.ValidateAgainstManifest(m); err != nil {
		t.Fatalf("lower boundary failed: %v", err)
	}
	// Exact upper boundary
	rt, _ = NewTimeRecoveryTarget(t0.Add(5 * time.Second).Format(time.RFC3339Nano))
	if err := rt.ValidateAgainstManifest(m); err != nil {
		t.Fatalf("upper boundary failed: %v", err)
	}
	// Mid-range binary search
	rt, _ = NewTimeRecoveryTarget(t0.Add(3 * time.Second).Format(time.RFC3339Nano))
	if err := rt.ValidateAgainstManifest(m); err != nil {
		t.Fatalf("mid-range failed: %v", err)
	}
	// Before earliest indexed commit
	rt, _ = NewTimeRecoveryTarget(t0.Add(-1 * time.Second).Format(time.RFC3339Nano))
	if err := rt.ValidateAgainstManifest(m); !errors.Is(err, ErrTargetOutOfRange) {
		t.Fatalf("expected ErrTargetOutOfRange before start, got %v", err)
	}
	// After latest indexed commit
	rt, _ = NewTimeRecoveryTarget(t0.Add(6 * time.Second).Format(time.RFC3339Nano))
	if err := rt.ValidateAgainstManifest(m); !errors.Is(err, ErrTargetOutOfRange) {
		t.Fatalf("expected ErrTargetOutOfRange after end, got %v", err)
	}

	// Empty LSN range (WALEndLSN < ConsistencyLSN)
	emptyRange := *m
	emptyRange.WALEndLSN = "0/1000050"
	rt, _ = NewTimeRecoveryTarget(t0.Add(1 * time.Second).Format(time.RFC3339Nano))
	if err := rt.ValidateAgainstManifest(&emptyRange); !errors.Is(err, ErrTargetOutOfRange) {
		t.Fatalf("expected ErrTargetOutOfRange on empty LSN range, got %v", err)
	}

	// Backward compatibility: legacy manifest without TimeIndex keeps working via coarse path
	legacy := *m
	legacy.TimeIndex = nil
	rt, _ = NewTimeRecoveryTarget(t0.Add(1 * time.Second).Format(time.RFC3339Nano))
	if err := rt.ValidateAgainstManifest(&legacy); err != nil {
		t.Fatalf("legacy manifest coarse path failed: %v", err)
	}
}
