package gateway

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type mockMeteringSaver struct {
	mu      sync.Mutex
	batches [][]MeteringEntry
	failErr error
}

func (m *mockMeteringSaver) SaveMeteringBatch(_ context.Context, entries []MeteringEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failErr != nil {
		return m.failErr
	}
	cp := make([]MeteringEntry, len(entries))
	copy(cp, entries)
	m.batches = append(m.batches, cp)
	return nil
}

func (m *mockMeteringSaver) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failErr = err
}

func (m *mockMeteringSaver) AllEntries() []MeteringEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []MeteringEntry
	for _, b := range m.batches {
		out = append(out, b...)
	}
	return out
}

func TestClassifyQueryType(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{"SELECT * FROM users", "SELECT"},
		{"  /* comment */ WITH c AS (SELECT 1) SELECT * FROM c", "SELECT"},
		{"WITH ins AS (INSERT INTO t VALUES (1) RETURNING id) SELECT * FROM ins", "INSERT"},
		{"INSERT INTO items (name) VALUES ('a')", "INSERT"},
		{"UPDATE items SET name = 'b' WHERE id = 1", "UPDATE"},
		{"DELETE FROM items WHERE id = 1", "DELETE"},
		{"CREATE TABLE foo (id int)", "DDL"},
		{"ALTER TABLE foo ADD COLUMN bar text", "DDL"},
		{"DROP TABLE foo", "DDL"},
		{"BEGIN", "TX"},
		{"COMMIT", "TX"},
		{"SET statement_timeout = 1000", "SET"},
		{"EXPLAIN SELECT 1", "EXPLAIN"},
		{"", "OTHER"},
	}
	for _, tc := range cases {
		got := ClassifyQueryType(tc.sql)
		if got != tc.want {
			t.Errorf("ClassifyQueryType(%q) = %q, want %q", tc.sql, got, tc.want)
		}
	}
}

func TestFormatAndParseMeteringLine(t *testing.T) {
	ts := time.Date(2026, 11, 22, 16, 5, 1, 123456000, time.UTC)
	orig := MeteringEntry{
		Date:           ts,
		DBID:           "db_tenant_1",
		BandwidthBytes: 4096,
		QueryTimeUs:    850,
		QueryType:      "SELECT",
	}
	line := FormatMeteringLine(orig)
	parsed, err := ParseMeteringLine(line)
	if err != nil {
		t.Fatalf("ParseMeteringLine failed: %v", err)
	}
	if !parsed.Date.Equal(orig.Date) || parsed.DBID != orig.DBID ||
		parsed.BandwidthBytes != orig.BandwidthBytes || parsed.QueryTimeUs != orig.QueryTimeUs ||
		parsed.QueryType != orig.QueryType {
		t.Fatalf("parsed mismatch: got %+v, want %+v", parsed, orig)
	}

	// Space-separated input should also parse cleanly.
	spaceLine := "2026-11-22T16:05:01Z  db_tenant_2  512  120  INSERT"
	parsed2, err := ParseMeteringLine(spaceLine)
	if err != nil {
		t.Fatalf("ParseMeteringLine(spaceLine) failed: %v", err)
	}
	if parsed2.DBID != "db_tenant_2" || parsed2.BandwidthBytes != 512 || parsed2.QueryTimeUs != 120 || parsed2.QueryType != "INSERT" {
		t.Fatalf("unexpected space-separated parse: %+v", parsed2)
	}

	if _, err := ParseMeteringLine("bad line"); err == nil {
		t.Fatal("expected error for malformed line")
	}
}

func TestMeteringLogger_FlushOnSizeLimit(t *testing.T) {
	dir := t.TempDir()
	saver := &mockMeteringSaver{}
	// Small maxFileBytes to trigger size-based rotation & flush immediately.
	ml, err := NewMeteringLogger(dir, 200, time.Hour, saver)
	if err != nil {
		t.Fatalf("NewMeteringLogger failed: %v", err)
	}
	ml.Start()
	defer ml.Stop()

	now := time.Now().UTC()
	for i := 0; i < 10; i++ {
		ml.LogQuery(MeteringEntry{
			Date:           now.Add(time.Duration(i) * time.Millisecond),
			DBID:           "db_size_test",
			BandwidthBytes: uint64(100 + i),
			QueryTimeUs:    uint64(50 + i),
			QueryType:      "SELECT",
		})
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(saver.AllEntries()) >= 5 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(saver.AllEntries()) == 0 {
		t.Fatal("expected size-based flush to save metering entries")
	}

	// Ensure rotated files were deleted after successful save.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	for _, ent := range entries {
		if ent.IsDir() || ent.Name() == gatewayIDFileName {
			continue
		}
		if ent.Name() != activeMeteringLogName {
			t.Fatalf("expected rotated log file %q to be deleted after flush", ent.Name())
		}
	}
}

func TestMeteringLogger_FlushOnInterval(t *testing.T) {
	dir := t.TempDir()
	saver := &mockMeteringSaver{}
	ml, err := NewMeteringLogger(dir, 10*1024*1024, 50*time.Millisecond, saver)
	if err != nil {
		t.Fatalf("NewMeteringLogger failed: %v", err)
	}
	ml.Start()
	defer ml.Stop()

	ml.LogQuery(MeteringEntry{
		Date:           time.Now().UTC(),
		DBID:           "db_interval_test",
		BandwidthBytes: 2048,
		QueryTimeUs:    333,
		QueryType:      "INSERT",
	})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(saver.AllEntries()) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	got := saver.AllEntries()
	if len(got) != 1 {
		t.Fatalf("expected 1 flushed entry after interval, got %d", len(got))
	}
	if got[0].DBID != "db_interval_test" || got[0].BandwidthBytes != 2048 || got[0].QueryTimeUs != 333 || got[0].QueryType != "INSERT" {
		t.Fatalf("unexpected flushed entry: %+v", got[0])
	}

	// Both active and rotated files should be gone after interval flush.
	if _, err := os.Stat(filepath.Join(dir, activeMeteringLogName)); !os.IsNotExist(err) {
		t.Fatalf("expected active log file to be rotated and deleted, stat err=%v", err)
	}
}

func TestMeteringLogger_RetainsFileOnSaveError(t *testing.T) {
	dir := t.TempDir()
	saver := &mockMeteringSaver{failErr: errors.New("db unavailable")}
	ml, err := NewMeteringLogger(dir, 10*1024*1024, time.Hour, saver)
	if err != nil {
		t.Fatalf("NewMeteringLogger failed: %v", err)
	}

	ml.LogQuery(MeteringEntry{
		Date:           time.Now().UTC(),
		DBID:           "db_retry_test",
		BandwidthBytes: 777,
		QueryTimeUs:    88,
		QueryType:      "UPDATE",
	})

	if err := ml.FlushNow(context.Background()); err == nil {
		t.Fatal("expected FlushNow to return error when saver fails")
	}

	// Rotated file must still exist because DB save failed.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	countBatches := func(ents []os.DirEntry) int {
		n := 0
		for _, e := range ents {
			if !e.IsDir() && (strings.HasPrefix(e.Name(), sealedBatchPrefix) || strings.HasPrefix(e.Name(), rotatedMeteringLogPrefix)) {
				n++
			}
		}
		return n
	}
	if n := countBatches(entries); n != 1 {
		t.Fatalf("expected 1 retained rotated log file on error, got %d", n)
	}

	// Recover DB and retry flush -> file should now be saved and deleted.
	saver.SetError(nil)
	if err := ml.FlushNow(context.Background()); err != nil {
		t.Fatalf("expected retry FlushNow to succeed, got: %v", err)
	}
	if got := saver.AllEntries(); len(got) != 1 || got[0].DBID != "db_retry_test" {
		t.Fatalf("unexpected entries after retry: %+v", got)
	}
	entries, err = os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if n := countBatches(entries); n != 0 {
		t.Fatalf("expected 0 batch files after retry, got %d", n)
	}
}

func TestMeteringLogger_TenantPoolIntegration(t *testing.T) {
	dir := t.TempDir()
	saver := &mockMeteringSaver{}
	ml, err := NewMeteringLogger(dir, 10*1024*1024, time.Hour, saver)
	if err != nil {
		t.Fatalf("NewMeteringLogger failed: %v", err)
	}

	pm := NewPoolManager(4, 60)
	pm.SetMeteringLogger(ml)
	pool := pm.GetOrCreate(TenantRoute{TenantID: "db_pool_1", Host: "127.0.0.1", Port: 5432}, nil)

	st := &clientSessionState{startupUser: "app_user"}
	pool.recordQueryMetrics(st, time.Now().Add(-2*time.Millisecond), 0, 120, relayStats{bytesOut: 380}, false, 1, "SELECT")

	if err := ml.FlushNow(context.Background()); err != nil {
		t.Fatalf("FlushNow failed: %v", err)
	}
	got := saver.AllEntries()
	if len(got) != 1 {
		t.Fatalf("expected 1 entry from TenantPool, got %d", len(got))
	}
	if got[0].DBID != "db_pool_1" || got[0].BandwidthBytes != 500 || got[0].QueryType != "SELECT" || got[0].QueryTimeUs == 0 {
		t.Fatalf("unexpected TenantPool metering entry: %+v", got[0])
	}
}

func TestAggregateMeteringEntries(t *testing.T) {
	entries := []MeteringEntry{
		{DBID: "tenant_b", BandwidthBytes: 100, QueryType: "SELECT"},
		{DBID: "tenant_a", BandwidthBytes: 250, QueryType: "INSERT"},
		{DBID: "tenant_a", BandwidthBytes: 150, QueryType: "SELECT"},
		{DBID: "tenant_a", BandwidthBytes: 50, QueryType: "DDL"},
		{DBID: "tenant_a", BandwidthBytes: 20, QueryType: "TX"},
		{DBID: "tenant_a", BandwidthBytes: 30, QueryType: "SET"},
		{DBID: "tenant_b", BandwidthBytes: 400, QueryType: "UPDATE"},
		{DBID: "tenant_b", BandwidthBytes: 200, QueryType: "EXPLAIN"},
		{DBID: "unknown", BandwidthBytes: 999, QueryType: "SELECT"},
		{DBID: "", BandwidthBytes: 999, QueryType: "INSERT"},
	}

	deltas := AggregateMeteringEntries(entries)
	if len(deltas) != 2 {
		t.Fatalf("expected 2 tenant deltas, got %d: %+v", len(deltas), deltas)
	}

	// Sorted by TenantID: tenant_a, then tenant_b.
	if deltas[0].TenantID != "tenant_a" || deltas[0].BandwidthBytes != 500 || deltas[0].ReadQueries != 1 || deltas[0].WriteQueries != 2 {
		t.Fatalf("unexpected tenant_a delta: %+v", deltas[0])
	}
	if deltas[1].TenantID != "tenant_b" || deltas[1].BandwidthBytes != 700 || deltas[1].ReadQueries != 2 || deltas[1].WriteQueries != 1 {
		t.Fatalf("unexpected tenant_b delta: %+v", deltas[1])
	}
}
