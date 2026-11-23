package gateway

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWriteRequestClassification(t *testing.T) {
	cases := []struct {
		sql   string
		inTx  bool
		want  WriteRequestClass
		admit bool
	}{
		{"SELECT 1", false, WriteClassReadOnly, false},
		{"WITH c AS (SELECT 1) SELECT * FROM c", false, WriteClassReadOnly, false},
		{"COPY (SELECT 1) TO STDOUT", false, WriteClassReadOnly, false},
		{"INSERT INTO t VALUES (1)", false, WriteClassSimpleAutocommit, true},
		{"UPDATE t SET v = 2 WHERE id = 1", false, WriteClassSimpleAutocommit, true},
		{"DELETE FROM t WHERE id = 1", false, WriteClassSimpleAutocommit, true},
		{"INSERT INTO t VALUES (1)", true, WriteClassExplicitTx, true},
		{"BEGIN", false, WriteClassExplicitTx, true},
		{"SAVEPOINT sp1", true, WriteClassExplicitTx, true},
		{"ROLLBACK TO sp1", true, WriteClassExplicitTx, true},
		{"COMMIT", true, WriteClassExplicitTx, true},
		{"COPY t FROM STDIN", false, WriteClassBulkCopy, true},
		{"CREATE TABLE t (id int)", false, WriteClassDDLAdmin, true},
		{"VACUUM t", false, WriteClassDDLAdmin, true},
		{"FOOBAR_UNKNOWN_STMT x", false, WriteClassUnknown, true},
	}
	for _, tc := range cases {
		got := ClassifyWriteRequest(tc.sql, tc.inTx)
		if got != tc.want {
			t.Errorf("ClassifyWriteRequest(%q, inTx=%v) = %v, want %v", tc.sql, tc.inTx, got, tc.want)
		}
		if got.NeedsWriteAdmission() != tc.admit {
			t.Errorf("NeedsWriteAdmission(%q) = %v, want %v", tc.sql, got.NeedsWriteAdmission(), tc.admit)
		}
	}
}

// startMockPGServer starts a lightweight mock PostgreSQL wire-protocol backend for Phase 20 tests & benchmarks.
func startMockPGServer(t testing.TB, writeDelay time.Duration) (port int, lsnQueries *atomic.Int64, closeFn func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen mock pg: %v", err)
	}
	lsnQueries = &atomic.Int64{}
	var wg sync.WaitGroup

	var connsMu sync.Mutex
	activeConns := make(map[net.Conn]struct{})

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			connsMu.Lock()
			activeConns[conn] = struct{}{}
			connsMu.Unlock()
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				defer func() {
					connsMu.Lock()
					delete(activeConns, c)
					connsMu.Unlock()
					_ = c.Close()
				}()

				var hdr [8]byte
				if _, err := io.ReadFull(c, hdr[:]); err != nil {
					return
				}
				mlen := binary.BigEndian.Uint32(hdr[0:4])
				if mlen > 8 {
					sbody := make([]byte, mlen-8)
					if _, err := io.ReadFull(c, sbody); err != nil {
						return
					}
				}
				_, _ = c.Write(BuildAuthOK())
				_, _ = c.Write(BuildBackendKeyData(1001, 2002))
				_, _ = c.Write(BuildReadyForQuery('I'))

				txStatus := byte('I')
				for {
					mt, body, err := ReadPGPacket(c)
					if err != nil {
						return
					}
					switch mt {
					case 'X':
						return
					case 'Q':
						off := 0
						q := strings.TrimSpace(ReadCStr(body, &off))
						upper := strings.ToUpper(q)
						switch {
						case q == "" || strings.HasPrefix(upper, "DISCARD"):
							_, _ = c.Write(BuildPGMessage('I', nil))
							_, _ = c.Write(BuildReadyForQuery(txStatus))
						case strings.Contains(upper, "PG_CURRENT_WAL_LSN"):
							lsnQueries.Add(1)
							// Minimal DataRow ('D') with "0/1000000"
							val := []byte("0/1000000")
							var dbuf bytes.Buffer
							_ = binary.Write(&dbuf, binary.BigEndian, uint16(1))
							_ = binary.Write(&dbuf, binary.BigEndian, int32(len(val)))
							dbuf.Write(val)
							_, _ = c.Write(BuildPGMessage('D', dbuf.Bytes()))
							_, _ = c.Write(BuildPGMessage('C', []byte("SELECT 1\x00")))
							_, _ = c.Write(BuildReadyForQuery(txStatus))
						case strings.HasPrefix(upper, "BEGIN"):
							txStatus = 'T'
							_, _ = c.Write(BuildPGMessage('C', []byte("BEGIN\x00")))
							_, _ = c.Write(BuildReadyForQuery(txStatus))
						case strings.HasPrefix(upper, "COMMIT"):
							txStatus = 'I'
							_, _ = c.Write(BuildPGMessage('C', []byte("COMMIT\x00")))
							_, _ = c.Write(BuildReadyForQuery(txStatus))
						case strings.HasPrefix(upper, "ROLLBACK") && !strings.Contains(upper, "TO"):
							txStatus = 'I'
							_, _ = c.Write(BuildPGMessage('C', []byte("ROLLBACK\x00")))
							_, _ = c.Write(BuildReadyForQuery(txStatus))
						case strings.HasPrefix(upper, "INSERT") || strings.HasPrefix(upper, "UPDATE") || strings.HasPrefix(upper, "DELETE"):
							if writeDelay > 0 {
								time.Sleep(writeDelay)
							}
							_, _ = c.Write(BuildPGMessage('C', []byte("INSERT 0 1\x00")))
							_, _ = c.Write(BuildReadyForQuery(txStatus))
						default:
							_, _ = c.Write(BuildPGMessage('C', []byte("OK\x00")))
							_, _ = c.Write(BuildReadyForQuery(txStatus))
						}
					case 'P':
						_, _ = c.Write(BuildPGMessage('1', nil))
					case 'B':
						_, _ = c.Write(BuildPGMessage('2', nil))
					case 'D':
						_, _ = c.Write(BuildPGMessage('n', nil))
					case 'E':
						if writeDelay > 0 {
							time.Sleep(writeDelay)
						}
						_, _ = c.Write(BuildPGMessage('C', []byte("INSERT 0 1\x00")))
					case 'C':
						_, _ = c.Write(BuildPGMessage('3', nil))
					case 'S':
						_, _ = c.Write(BuildReadyForQuery(txStatus))
					}
				}
			}(conn)
		}
	}()

	return ln.Addr().(*net.TCPAddr).Port, lsnQueries, func() {
		_ = ln.Close()
		connsMu.Lock()
		for c := range activeConns {
			_ = c.Close()
		}
		connsMu.Unlock()
		wg.Wait()
	}
}

func connectMockClient(t testing.TB, pool *TenantPool, reg *MetricsRegistry, creg *CancelRegistry) net.Conn {
	t.Helper()
	srv, cli := net.Pipe()
	go func() {
		pool.ServeClient(srv, "app_user", "pw123", nil, reg, creg, 0)
		_ = srv.Close()
	}()
	mt, _, err := ReadPGPacket(cli)
	if err != nil || mt != 'R' {
		t.Fatalf("expected auth request, got %c err=%v", mt, err)
	}
	if _, err := cli.Write(BuildPasswordMessage("pw123")); err != nil {
		t.Fatal(err)
	}
	for {
		mt, _, err := ReadPGPacket(cli)
		if err != nil {
			t.Fatal(err)
		}
		if mt == 'Z' {
			break
		}
	}
	return cli
}

func execMockSimple(t testing.TB, cli net.Conn, sql string) (hadErr bool, txStatus byte) {
	t.Helper()
	if _, err := cli.Write(BuildSimpleQuery(sql)); err != nil {
		t.Fatalf("write query %q: %v", sql, err)
	}
	for {
		mt, body, err := ReadPGPacket(cli)
		if err != nil {
			t.Fatalf("read response %q: %v", sql, err)
		}
		if mt == 'E' {
			hadErr = true
		}
		if mt == 'Z' && len(body) > 0 {
			return hadErr, body[0]
		}
	}
}

func TestPhase20Integration_TxPinningBufferBudgetAndLazyLSN(t *testing.T) {
	pgPort, lsnQueries, closePG := startMockPGServer(t, 0)
	defer closePG()

	cfg := DefaultConfig()
	cfg.WriteGovernorEnabled = true
	cfg.WriteMaxGlobal = 4
	cfg.WriteMaxPerTenant = 2
	cfg.WriteBufferGlobalMax = 4096
	cfg.WriteBufferSessionMax = 2048
	cfg.WriteLazyLSNEnabled = true

	gov := NewWriteGovernor(cfg)
	budget := NewBufferBudget(cfg.WriteBufferGlobalMax)
	pm := NewPoolManager(4, 300)
	pm.SetWriteGovernor(gov)
	pm.SetBufferBudget(budget, cfg.WriteBufferSessionMax, 64)
	pm.SetLazyLSN(true)
	// Configure ReadEnginePort so RYOW LSN logic is active.
	pm.SetReadEngineEndpoint("127.0.0.1", "", pgPort)

	pool := pm.GetOrCreate(TenantRoute{
		TenantID:    "t_p20",
		State:       "ACTIVE",
		Host:        "127.0.0.1",
		Port:        pgPort,
		AppPassword: "pw123",
	}, nil)
	reg := NewMetricsRegistry(32, 16, 16)
	reg.SetWriteGovernorComponents(gov, budget, NewTelemetryStore(15*time.Second))
	creg := NewCancelRegistry()

	cli := connectMockClient(t, pool, reg, creg)

	// 1. Explicit transaction with SAVEPOINT + ROLLBACK + COMMIT preserves backend affinity.
	if hadErr, st := execMockSimple(t, cli, "BEGIN"); hadErr || st != 'T' {
		t.Fatalf("BEGIN failed: err=%v st=%c", hadErr, st)
	}
	txWait := time.Now().Add(200 * time.Millisecond)
	for gov.Snapshot().OpenTransactions != 1 && time.Now().Before(txWait) {
		time.Sleep(time.Millisecond)
	}
	if gov.Snapshot().OpenTransactions != 1 {
		t.Fatalf("expected 1 open transaction, got %d", gov.Snapshot().OpenTransactions)
	}
	if hadErr, st := execMockSimple(t, cli, "INSERT INTO items VALUES (1)"); hadErr || st != 'T' {
		t.Fatalf("INSERT in tx failed: err=%v st=%c", hadErr, st)
	}
	if hadErr, st := execMockSimple(t, cli, "SAVEPOINT sp1"); hadErr || st != 'T' {
		t.Fatalf("SAVEPOINT failed: err=%v st=%c", hadErr, st)
	}
	if hadErr, st := execMockSimple(t, cli, "COMMIT"); hadErr || st != 'I' {
		t.Fatalf("COMMIT failed: err=%v st=%c", hadErr, st)
	}
	txWait = time.Now().Add(200 * time.Millisecond)
	for gov.Snapshot().OpenTransactions != 0 && time.Now().Before(txWait) {
		time.Sleep(time.Millisecond)
	}
	if gov.Snapshot().OpenTransactions != 0 {
		t.Fatalf("expected 0 open transactions after COMMIT, got %d", gov.Snapshot().OpenTransactions)
	}

	// 2. Pure autocommit writes with LazyLSN=true must incur ZERO pg_current_wal_lsn() round-trips.
	for i := 0; i < 5; i++ {
		if hadErr, _ := execMockSimple(t, cli, "INSERT INTO items VALUES (2)"); hadErr {
			t.Fatal("autocommit insert failed")
		}
	}
	if lsnQueries.Load() != 0 {
		t.Fatalf("expected 0 LSN queries with LazyLSN enabled, got %d", lsnQueries.Load())
	}

	// 3. Extended query pipeline buffer budget enforcement & release on disconnect.
	// Send a large Parse message that exceeds session max (2048 bytes).
	bigSQL := "INSERT INTO items VALUES ('" + strings.Repeat("a", 2200) + "')"
	_, _ = cli.Write(BuildParseMessage("s_big", bigSQL, nil))
	mt, _, err := ReadPGPacket(cli)
	if err != nil || mt != 'E' {
		t.Fatalf("expected buffer budget ErrorResponse 'E', got %c err=%v", mt, err)
	}
	_ = cli.Close()

	// Wait briefly for cleanup defer to release all buffer budget bytes.
	deadline := time.Now().Add(time.Second)
	for budget.UsedBytes() != 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if budget.UsedBytes() != 0 {
		t.Fatalf("expected 0 buffer bytes after disconnect, got %d", budget.UsedBytes())
	}
	if budget.Rejections() == 0 {
		t.Fatal("expected buffer budget rejection to be recorded")
	}

	// 4. Verify Prometheus metrics & internal HTTP telemetry endpoint.
	prom := reg.ToPrometheus(pm.Snapshots())
	for _, wantMetric := range []string{
		"air_write_active",
		"air_write_queue_depth",
		"air_write_limit_global",
		"air_gateway_buffered_bytes",
		"air_transaction_open",
		"air_backend_active",
	} {
		if !strings.Contains(prom, wantMetric) {
			t.Errorf("missing Prometheus metric %q", wantMetric)
		}
	}

	telStore := NewTelemetryStore(15 * time.Second)
	adm := NewAdminHTTPServer("127.0.0.1:0", "secret-tok", nil, pm, reg)
	adm.SetTelemetryStore(telStore)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/telemetry", strings.NewReader(`{"memory_current_bytes":1048576,"memory_high_bytes":536870912}`))
	req.Header.Set("X-Air-Internal-Token", "secret-tok")
	rec := httptest.NewRecorder()
	adm.serveHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("telemetry POST status=%d body=%s", rec.Code, rec.Body.String())
	}
	if snap, ok := telStore.Latest(time.Now()); !ok || snap.MemoryCurrentBytes != 1048576 {
		t.Fatalf("unexpected stored telemetry: %+v ok=%v", snap, ok)
	}
}

func runWriteBenchmark(b *testing.B, clients int, govEnabled, adaptive, lazyLSN bool) {
	pgPort, _, closePG := startMockPGServer(b, 50*time.Microsecond)
	defer closePG()

	cfg := DefaultConfig()
	cfg.WriteGovernorEnabled = govEnabled
	cfg.WriteAdaptiveEnabled = adaptive
	cfg.WriteLazyLSNEnabled = lazyLSN
	cfg.WriteMaxGlobal = 16
	cfg.WriteMaxPerTenant = 8
	cfg.WriteQueueMax = 512

	gov := NewWriteGovernor(cfg)
	budget := NewBufferBudget(cfg.WriteBufferGlobalMax)
	pm := NewPoolManager(16, 300)
	pm.SetWriteGovernor(gov)
	pm.SetBufferBudget(budget, cfg.WriteBufferSessionMax, 4096)
	pm.SetLazyLSN(lazyLSN)

	pool := pm.GetOrCreate(TenantRoute{
		TenantID:    "t_bench",
		State:       "ACTIVE",
		Host:        "127.0.0.1",
		Port:        pgPort,
		AppPassword: "pw123",
	}, nil)
	reg := NewMetricsRegistry(1024, 256, 256)
	creg := NewCancelRegistry()

	conns := make([]net.Conn, clients)
	for i := 0; i < clients; i++ {
		conns[i] = connectMockClient(b, pool, reg, creg)
	}
	defer func() {
		for _, c := range conns {
			_, _ = c.Write(BuildPGMessage('X', nil))
			_ = c.Close()
		}
	}()

	var latMu sync.Mutex
	latencies := make([]int64, 0, b.N)

	b.ReportAllocs()
	b.ResetTimer()
	var counter atomic.Int64
	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(cli net.Conn) {
			defer wg.Done()
			localLat := make([]int64, 0, b.N/clients+1)
			for {
				idx := counter.Add(1)
				if int(idx) > b.N {
					break
				}
				t0 := time.Now()
				if _, err := cli.Write(BuildSimpleQuery("INSERT INTO bench VALUES (1)")); err != nil {
					b.Error(err)
					return
				}
				for {
					mt, _, err := ReadPGPacket(cli)
					if err != nil {
						b.Error(err)
						return
					}
					if mt == 'Z' {
						break
					}
				}
				localLat = append(localLat, time.Since(t0).Microseconds())
			}
			latMu.Lock()
			latencies = append(latencies, localLat...)
			latMu.Unlock()
		}(conns[c])
	}
	wg.Wait()
	b.StopTimer()

	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		p50 := latencies[len(latencies)*50/100]
		p95 := latencies[len(latencies)*95/100]
		p99 := latencies[len(latencies)*99/100]
		b.ReportMetric(float64(p50), "p50_us")
		b.ReportMetric(float64(p95), "p95_us")
		b.ReportMetric(float64(p99), "p99_us")
	}
	_ = fmt.Sprintf("")
}

func BenchmarkWritePath_Existing_C8(b *testing.B) {
	runWriteBenchmark(b, 8, false, false, false)
}

func BenchmarkWritePath_StaticGovernor_C8(b *testing.B) {
	runWriteBenchmark(b, 8, true, false, false)
}

func BenchmarkWritePath_AdaptiveGovernor_C8(b *testing.B) {
	runWriteBenchmark(b, 8, true, true, false)
}

func BenchmarkWritePath_AllOptimizations_C8(b *testing.B) {
	runWriteBenchmark(b, 8, true, true, true)
}

func BenchmarkWritePath_ConcurrencySweep(b *testing.B) {
	for _, c := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("clients_%d", c), func(b *testing.B) {
			runWriteBenchmark(b, c, true, true, true)
		})
	}
}
