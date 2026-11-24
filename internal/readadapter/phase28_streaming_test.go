package readadapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

type chunkRecorderWriter struct {
	buf       bytes.Buffer
	writes    int
	maxChunk  int
}

func (w *chunkRecorderWriter) Write(p []byte) (int, error) {
	w.writes++
	if len(p) > w.maxChunk {
		w.maxChunk = len(p)
	}
	return w.buf.Write(p)
}

func TestPhase28_QueryStreamWithContext_BoundedMemoryAndRelease(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "stream.db")
	conn, err := OpenSQLiteConn(dbPath, false, 1024)
	if err != nil {
		t.Fatalf("OpenSQLiteConn: %v", err)
	}
	defer conn.Close()

	if err := conn.Exec("CREATE TABLE items (id INTEGER, payload TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 400; i++ {
		if err := conn.Exec(fmt.Sprintf("INSERT INTO items VALUES (%d, 'payload-data-0123456789')", i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// Tight per-query budget (8 KiB) would fail if all 400 rows were buffered at once,
	// but succeeds with bounded 32-row batches and ReleaseBytes per flush.
	mm := NewMemoryManager(MemoryManagerConfig{
		GlobalLimitBytes:        64 * 1024,
		DefaultTenantLimitBytes: 32 * 1024,
		DefaultQueryLimitBytes:  8 * 1024,
	})
	budget := mm.BeginQuery("tenant_stream", 0)
	defer budget.Release()
	ctx := ContextWithMemoryBudget(context.Background(), budget)

	var gotCols []SQLiteColumn
	var totalRows int
	var batches int
	_, _, err = conn.QueryStreamWithContext(
		ctx, nil,
		"SELECT id, payload FROM items ORDER BY id",
		nil, nil, 1, 32,
		func(cols []SQLiteColumn) error {
			gotCols = append([]SQLiteColumn(nil), cols...)
			return nil
		},
		func(rows [][]SQLiteValue) error {
			batches++
			totalRows += len(rows)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("QueryStreamWithContext failed: %v", err)
	}
	if len(gotCols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(gotCols))
	}
	if totalRows != 400 {
		t.Fatalf("expected 400 streamed rows, got %d", totalRows)
	}
	if batches < 10 {
		t.Fatalf("expected >=10 batches for 400 rows at batchSize=32, got %d", batches)
	}
	if used := budget.Used(); used != 0 {
		t.Fatalf("expected query budget released to 0 after stream, got %d", used)
	}
}

func TestPhase28_QueryStreamWithContext_ImmediateCancel(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cancel.db")
	conn, err := OpenSQLiteConn(dbPath, false, 1024)
	if err != nil {
		t.Fatalf("OpenSQLiteConn: %v", err)
	}
	defer conn.Close()

	if err := conn.Exec("CREATE TABLE nums (v INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 200; i++ {
		if err := conn.Exec(fmt.Sprintf("INSERT INTO nums VALUES (%d)", i)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	batches := 0
	_, _, err = conn.QueryStreamWithContext(
		ctx, nil,
		"SELECT v FROM nums",
		nil, nil, 1, 16,
		nil,
		func(rows [][]SQLiteValue) error {
			batches++
			if batches == 2 {
				cancel()
			}
			return nil
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if batches != 2 {
		t.Fatalf("expected stream to stop at batch 2 after cancel, got %d", batches)
	}
}

func TestPhase28_WriteResultSetStream_BoundedChunks(t *testing.T) {
	cols := []SQLiteColumn{
		{Name: "id", PGType: PGOIDInt8, PGSize: 8},
		{Name: "txt", PGType: PGOIDText, PGSize: -1},
	}
	rows := make([][]SQLiteValue, 200)
	for i := range rows {
		rows[i] = []SQLiteValue{
			{Text: "123"},
			{Text: "streaming-row-payload-value"},
		}
	}

	var rec chunkRecorderWriter
	if err := writeResultSetStream(&rec, nil, cols, rows, 512); err != nil {
		t.Fatalf("writeResultSetStream: %v", err)
	}
	if rec.writes < 5 {
		t.Fatalf("expected multiple incremental flushes, got %d writes", rec.writes)
	}

	// Verify PG wire packet framing: 1 'T', 200 'D', 1 'C'.
	data := rec.buf.Bytes()
	off := 0
	var countT, countD, countC int
	for off+5 <= len(data) {
		msgType := data[off]
		msgLen := int(binary.BigEndian.Uint32(data[off+1 : off+5]))
		off += 1 + msgLen
		switch msgType {
		case 'T':
			countT++
		case 'D':
			countD++
		case 'C':
			countC++
		}
	}
	if off != len(data) || countT != 1 || countD != 200 || countC != 1 {
		t.Fatalf("unexpected PG framing: off=%d/%d T=%d D=%d C=%d", off, len(data), countT, countD, countC)
	}
}
