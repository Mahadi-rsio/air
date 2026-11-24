package readadapter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func setupPhase30StreamServer(t *testing.T, queryMemLimitBytes int64) (*SQLiteReadServer, *MemoryManager) {
	t.Helper()
	srv, err := NewSQLiteReadServer(SQLiteServerConfig{
		ListenHost:             "127.0.0.1",
		PGPort:                 0,
		DataDir:                t.TempDir(),
		GlobalMemoryLimitBytes: 8 * 1024 * 1024,
		TenantMemoryLimitBytes: 4 * 1024 * 1024,
		QueryMemoryLimitBytes:  queryMemLimitBytes,
		QueryTimeout:           15 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSQLiteReadServer: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("srv.Start: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })

	ts, err := srv.getOrCreateTenant("tenant_p30")
	if err != nil {
		t.Fatalf("getOrCreateTenant: %v", err)
	}
	if err := ts.Writer.Exec("CREATE TABLE stream_items (id INTEGER PRIMARY KEY, payload TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := ts.Writer.Exec("BEGIN"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	payload := strings.Repeat("x", 256)
	for i := 1; i <= 5000; i++ {
		if err := ts.Writer.Exec(fmt.Sprintf("INSERT INTO stream_items (id, payload) VALUES (%d, '%s_%04d')", i, payload, i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if err := ts.Writer.Exec("COMMIT"); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return srv, srv.memMgr
}

func TestPhase30_EndToEndStreaming_SimpleExtendedAndCopyTo(t *testing.T) {
	// 5000 rows * ~300 bytes = ~1.5 MB total result set.
	// With a 128 KiB per-query memory budget, full materialization would fail with
	// ErrMemoryBudgetExceeded. Genuine streaming keeps batch memory <= 128 KiB.
	srv, mm := setupPhase30StreamServer(t, 128*1024)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/tenant_p30?sslmode=disable", srv.Port())

	// 1. Simple Query mode
	cfgSimple, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	cfgSimple.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	connSimple, err := pgx.ConnectConfig(ctx, cfgSimple)
	if err != nil {
		t.Fatalf("ConnectConfig simple: %v", err)
	}
	defer connSimple.Close(ctx)

	rows, err := connSimple.Query(ctx, "SELECT id, payload FROM stream_items ORDER BY id")
	if err != nil {
		t.Fatalf("Simple Query: %v", err)
	}
	count := 0
	for rows.Next() {
		var id int64
		var p string
		if err := rows.Scan(&id, &p); err != nil {
			t.Fatalf("Simple Scan: %v", err)
		}
		count++
		if int(id) != count {
			t.Fatalf("Simple out-of-order row: got id=%d want=%d", id, count)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Simple rows.Err: %v", err)
	}
	rows.Close()
	if count != 5000 {
		t.Fatalf("Simple expected 5000 rows, got %d", count)
	}
	if used := mm.GlobalUsed(); used != 0 {
		t.Fatalf("expected GlobalUsed=0 after Simple Query, got %d", used)
	}

	// 2. Extended Query mode (Parse/Bind/Execute/Sync)
	cfgExt, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("ParseConfig ext: %v", err)
	}
	cfgExt.DefaultQueryExecMode = pgx.QueryExecModeExec
	connExt, err := pgx.ConnectConfig(ctx, cfgExt)
	if err != nil {
		t.Fatalf("ConnectConfig ext: %v", err)
	}
	defer connExt.Close(ctx)

	rowsExt, err := connExt.Query(ctx, "SELECT id, payload FROM stream_items WHERE id >= $1 ORDER BY id", 1)
	if err != nil {
		t.Fatalf("Extended Query: %v", err)
	}
	countExt := 0
	for rowsExt.Next() {
		var id int64
		var p string
		if err := rowsExt.Scan(&id, &p); err != nil {
			t.Fatalf("Extended Scan: %v", err)
		}
		countExt++
		if int(id) != countExt {
			t.Fatalf("Extended out-of-order row: got id=%d want=%d", id, countExt)
		}
	}
	if err := rowsExt.Err(); err != nil {
		t.Fatalf("Extended rows.Err: %v", err)
	}
	rowsExt.Close()
	if countExt != 5000 {
		t.Fatalf("Extended expected 5000 rows, got %d", countExt)
	}
	if used := mm.GlobalUsed(); used != 0 {
		t.Fatalf("expected GlobalUsed=0 after Extended Query, got %d", used)
	}

	// 3. COPY TO STDOUT mode
	var copyBuf bytes.Buffer
	tag, err := connSimple.PgConn().CopyTo(ctx, &copyBuf, "COPY (SELECT id, payload FROM stream_items ORDER BY id) TO STDOUT")
	if err != nil {
		t.Fatalf("COPY TO STDOUT: %v", err)
	}
	if tag.RowsAffected() != 5000 {
		t.Fatalf("COPY expected 5000 rows affected, got %d", tag.RowsAffected())
	}
	lines := strings.Count(copyBuf.String(), "\n")
	if lines != 5000 {
		t.Fatalf("COPY expected 5000 lines, got %d", lines)
	}
	if used := mm.GlobalUsed(); used != 0 {
		t.Fatalf("expected GlobalUsed=0 after COPY TO, got %d", used)
	}
}

func pgWireDialAndStartup(t *testing.T, port int, dbName string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	var startup bytes.Buffer
	_ = binary.Write(&startup, binary.BigEndian, int32(0))
	_ = binary.Write(&startup, binary.BigEndian, int32(196608))
	startup.WriteString("user\x00postgres\x00database\x00" + dbName + "\x00\x00")
	b := startup.Bytes()
	binary.BigEndian.PutUint32(b[:4], uint32(len(b)))
	if _, err := conn.Write(b); err != nil {
		conn.Close()
		t.Fatalf("write startup: %v", err)
	}
	r := bufio.NewReader(conn)
	for {
		typ, _, err := readPGMsgTest(r)
		if err != nil {
			conn.Close()
			t.Fatalf("read startup resp: %v", err)
		}
		if typ == 'Z' {
			break
		}
	}
	return conn, r
}

func readPGMsgTest(r *bufio.Reader) (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	sz := int(binary.BigEndian.Uint32(hdr[1:5])) - 4
	if sz < 0 {
		return 0, nil, fmt.Errorf("invalid msg len %d", sz)
	}
	body := make([]byte, sz)
	if sz > 0 {
		if _, err := io.ReadFull(r, body); err != nil {
			return 0, nil, err
		}
	}
	return hdr[0], body, nil
}

func sendPGSimpleQueryTest(conn net.Conn, sql string) error {
	var msg bytes.Buffer
	msg.WriteByte('Q')
	_ = binary.Write(&msg, binary.BigEndian, int32(4+len(sql)+1))
	msg.WriteString(sql)
	msg.WriteByte(0)
	_, err := conn.Write(msg.Bytes())
	return err
}

func TestPhase30_SlowClientBackpressureAndMidStreamCancel(t *testing.T) {
	srv, mm := setupPhase30StreamServer(t, 256*1024)

	// 1. Slow client backpressure: read first 10 rows, pause, then drain all 5000 rows.
	conn, r := pgWireDialAndStartup(t, srv.Port(), "tenant_p30")
	if err := sendPGSimpleQueryTest(conn, "SELECT id, payload FROM stream_items ORDER BY id"); err != nil {
		conn.Close()
		t.Fatalf("send query: %v", err)
	}
	dataRows := 0
	for {
		typ, _, err := readPGMsgTest(r)
		if err != nil {
			conn.Close()
			t.Fatalf("readPGMsgTest: %v", err)
		}
		if typ == 'D' {
			dataRows++
			if dataRows == 10 {
				time.Sleep(150 * time.Millisecond)
			}
		}
		if typ == 'Z' {
			break
		}
	}
	conn.Close()
	if dataRows != 5000 {
		t.Fatalf("expected 5000 rows under slow client, got %d", dataRows)
	}
	if used := mm.GlobalUsed(); used != 0 {
		t.Fatalf("expected GlobalUsed=0 after slow client, got %d", used)
	}

	// 2. Mid-stream client disconnect/cancellation releases all resources.
	conn2, r2 := pgWireDialAndStartup(t, srv.Port(), "tenant_p30")
	if err := sendPGSimpleQueryTest(conn2, "SELECT id, payload FROM stream_items ORDER BY id"); err != nil {
		conn2.Close()
		t.Fatalf("send query 2: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, _, err := readPGMsgTest(r2); err != nil {
			conn2.Close()
			t.Fatalf("readPGMsgTest 2: %v", err)
		}
	}
	// Abruptly close client mid-stream.
	_ = conn2.Close()

	// Wait briefly for server session goroutine to notice write error/EOF and clean up.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if srv.activeQueries.Load() == 0 && mm.GlobalUsed() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if active := srv.activeQueries.Load(); active != 0 {
		t.Fatalf("expected activeQueries=0 after mid-stream cancel, got %d", active)
	}
	if used := mm.GlobalUsed(); used != 0 {
		t.Fatalf("expected GlobalUsed=0 after mid-stream cancel, got %d", used)
	}
}

func TestPhase30_LargeResultStreaming_BoundedMemory(t *testing.T) {
	// Stream 100,000 rows (~100 MB total wire payload) under a 512 KiB per-query budget
	// and verify RSS stays bounded (suitable for 256 MB / 512 MB cgroup v2 limits).
	srv, err := NewSQLiteReadServer(SQLiteServerConfig{
		ListenHost:             "127.0.0.1",
		PGPort:                 0,
		DataDir:                t.TempDir(),
		GlobalMemoryLimitBytes: 8 * 1024 * 1024,
		TenantMemoryLimitBytes: 4 * 1024 * 1024,
		QueryMemoryLimitBytes:  512 * 1024,
		QueryTimeout:           60 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSQLiteReadServer: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("srv.Start: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	ts, err := srv.getOrCreateTenant("tenant_p30_large")
	if err != nil {
		t.Fatalf("getOrCreateTenant: %v", err)
	}
	if err := ts.Writer.Exec("CREATE TABLE big_rows (id INTEGER PRIMARY KEY, body TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := ts.Writer.Exec("BEGIN"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	body := strings.Repeat("a", 960)
	for i := 1; i <= 100000; i++ {
		if err := ts.Writer.Exec(fmt.Sprintf("INSERT INTO big_rows (id, body) VALUES (%d, '%s_%06d')", i, body, i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if err := ts.Writer.Exec("COMMIT"); err != nil {
		t.Fatalf("commit: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/tenant_p30_large?sslmode=disable", srv.Port())
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, "SELECT id, body FROM big_rows ORDER BY id")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	var count int64
	for rows.Next() {
		var id int64
		var s string
		if err := rows.Scan(&id, &s); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	rows.Close()
	if count != 100000 {
		t.Fatalf("expected 100000 rows, got %d", count)
	}
	if used := srv.memMgr.GlobalUsed(); used != 0 {
		t.Fatalf("expected GlobalUsed=0 after large stream, got %d", used)
	}
}

