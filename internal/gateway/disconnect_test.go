package gateway

import (
	"encoding/binary"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func buildPGDataRow(val string) []byte {
	var body []byte
	var cnt [2]byte
	binary.BigEndian.PutUint16(cnt[:], 1)
	body = append(body, cnt[:]...)
	var flen [4]byte
	binary.BigEndian.PutUint32(flen[:], uint32(len(val)))
	body = append(body, flen[:]...)
	body = append(body, val...)
	return BuildPGMessage('D', body)
}

func buildPGRowDesc(colName string) []byte {
	var body []byte
	var cnt [2]byte
	binary.BigEndian.PutUint16(cnt[:], 1)
	body = append(body, cnt[:]...)
	body = append(body, colName...)
	body = append(body, 0)
	var meta [18]byte
	binary.BigEndian.PutUint32(meta[6:10], 25) // text OID
	binary.BigEndian.PutUint16(meta[10:12], 0xffff)
	binary.BigEndian.PutUint32(meta[12:16], 0xffffffff)
	body = append(body, meta[:]...)
	return BuildPGMessage('T', body)
}

func readStartupPacket(conn net.Conn) (isCancel bool, pid, secret uint32, err error) {
	var lenBuf [4]byte
	if _, err = io.ReadFull(conn, lenBuf[:]); err != nil {
		return false, 0, 0, err
	}
	mlen := int(binary.BigEndian.Uint32(lenBuf[:]))
	if mlen < 8 || mlen > 65536 {
		return false, 0, 0, io.ErrUnexpectedEOF
	}
	buf := make([]byte, mlen-4)
	if _, err = io.ReadFull(conn, buf); err != nil {
		return false, 0, 0, err
	}
	code := binary.BigEndian.Uint32(buf[0:4])
	if mlen == 16 && code == CancelRequestCode {
		return true, binary.BigEndian.Uint32(buf[4:8]), binary.BigEndian.Uint32(buf[8:12]), nil
	}
	return false, 0, 0, nil
}

type mockReadEngine struct {
	ln           net.Listener
	port         int
	stallStarted atomic.Int32
	cancelsSeen  atomic.Int32
	activeConns  atomic.Int32
	pidSeq       atomic.Uint32
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

func startMockReadEngine(t *testing.T) *mockReadEngine {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen mock read engine: %v", err)
	}
	m := &mockReadEngine{
		ln:     ln,
		port:   ln.Addr().(*net.TCPAddr).Port,
		stopCh: make(chan struct{}),
	}
	m.wg.Add(1)
	go m.acceptLoop()
	t.Cleanup(func() {
		close(m.stopCh)
		_ = ln.Close()
		m.wg.Wait()
	})
	return m
}

func (m *mockReadEngine) acceptLoop() {
	defer m.wg.Done()
	for {
		c, err := m.ln.Accept()
		if err != nil {
			return
		}
		m.wg.Add(1)
		go m.serveConn(c)
	}
}

func (m *mockReadEngine) serveConn(c net.Conn) {
	defer m.wg.Done()
	defer c.Close()

	isCancel, _, _, err := readStartupPacket(c)
	if err != nil {
		return
	}
	if isCancel {
		m.cancelsSeen.Add(1)
		return
	}

	m.activeConns.Add(1)
	defer m.activeConns.Add(-1)

	pid := m.pidSeq.Add(1)
	var authOK [9]byte
	authOK[0] = 'R'
	binary.BigEndian.PutUint32(authOK[1:5], 8)
	_, _ = c.Write(authOK[:])

	var bkd [13]byte
	bkd[0] = 'K'
	binary.BigEndian.PutUint32(bkd[1:5], 12)
	binary.BigEndian.PutUint32(bkd[5:9], pid)
	binary.BigEndian.PutUint32(bkd[9:13], 42)
	_, _ = c.Write(bkd[:])
	_, _ = c.Write(BuildReadyForQuery('I'))

	for {
		mt, mb, rerr := ReadPGPacket(c)
		if rerr != nil {
			return
		}
		switch mt {
		case 'X':
			return
		case 'Q':
			sql := strings.TrimRight(string(mb), "\x00")
			if strings.Contains(sql, "pg_last_wal_replay_lsn") {
				_, _ = c.Write(buildPGRowDesc("lsn"))
				_, _ = c.Write(buildPGDataRow("0/1000"))
				_, _ = c.Write(BuildPGMessage('C', append([]byte("SELECT 1"), 0)))
				_, _ = c.Write(BuildReadyForQuery('I'))
				continue
			}
			if strings.Contains(sql, "STALL") {
				m.stallStarted.Add(1)
				// Block until client/gateway closes the connection or test stops.
				buf := make([]byte, 1)
				for {
					select {
					case <-m.stopCh:
						return
					default:
					}
					_ = c.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
					if _, err := c.Read(buf); err != nil {
						if ne, ok := err.(net.Error); ok && ne.Timeout() {
							continue
						}
						return
					}
				}
			}
			_, _ = c.Write(buildPGRowDesc("ok"))
			_, _ = c.Write(buildPGDataRow("1"))
			_, _ = c.Write(BuildPGMessage('C', append([]byte("SELECT 1"), 0)))
			_, _ = c.Write(BuildReadyForQuery('I'))
		case 'P', 'B', 'D', 'E':
			// Buffer extended query until Sync
		case 'S':
			_, _ = c.Write(BuildPGMessage('1', nil))
			_, _ = c.Write(BuildPGMessage('2', nil))
			_, _ = c.Write(buildPGRowDesc("ok"))
			_, _ = c.Write(buildPGDataRow("1"))
			_, _ = c.Write(BuildPGMessage('C', append([]byte("SELECT 1"), 0)))
			_, _ = c.Write(BuildReadyForQuery('I'))
		}
	}
}

func dialAndStartupClient(t *testing.T, addr, tenant, user string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial gateway: %v", err)
	}
	startup := BuildStartupMessage(map[string]string{
		"user":     user,
		"database": tenant,
	})
	if _, err := c.Write(startup); err != nil {
		_ = c.Close()
		t.Fatalf("write startup: %v", err)
	}
	// Read until ReadyForQuery ('Z') or ErrorResponse ('E')
	for {
		mt, mb, err := ReadPGPacket(c)
		if err != nil {
			_ = c.Close()
			t.Fatalf("read startup resp: %v", err)
		}
		if mt == 'R' && len(mb) >= 4 && binary.BigEndian.Uint32(mb[:4]) == 3 {
			if _, err := c.Write(BuildPGMessage('p', append([]byte("secret"), 0))); err != nil {
				_ = c.Close()
				t.Fatalf("write password: %v", err)
			}
			continue
		}
		if mt == 'E' {
			_ = c.Close()
			t.Fatalf("unexpected startup error: %q", string(mb))
		}
		if mt == 'Z' {
			return c
		}
	}
}

func TestPhase25_ClientDisconnect_ReleasesTenantSlotWithin100ms(t *testing.T) {
	re := startMockReadEngine(t)

	const tenant = "t_disc"
	const user = "u_disc"

	metrics := NewMetricsRegistry(10, 2, 2) // MaxTenantClientConns = 2
	pool := NewTenantPool(tenant, "127.0.0.1", "", 1, "", 4, 60)
	pool.SetReadEngineEndpoint("127.0.0.1", "", re.port)
	defer pool.Close()

	gwLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gateway: %v", err)
	}
	defer gwLn.Close()

	var gwWG sync.WaitGroup
	go func() {
		for {
			c, err := gwLn.Accept()
			if err != nil {
				return
			}
			gwWG.Add(1)
			go func(conn net.Conn) {
				defer gwWG.Done()
				defer conn.Close()
				_, _, _, _ = readStartupPacket(conn)
				if err := metrics.TryAcquireTenantUser(tenant, user); err != nil {
					_, _ = conn.Write(BuildErrorResponse("FATAL", "53300", err.Error()))
					return
				}
				var relOnce sync.Once
				defer relOnce.Do(func() {
					metrics.ReleaseTenantUser(tenant, user)
				})
				pool.ServeClient(conn, user, "secret", nil, metrics, nil, 0)
			}(c)
		}
	}()

	// Stabilize goroutine baseline after listeners start.
	runtime.GC()
	baseGoroutines := runtime.NumGoroutine()

	gwAddr := gwLn.Addr().String()
	c1 := dialAndStartupClient(t, gwAddr, tenant, user)
	c2 := dialAndStartupClient(t, gwAddr, tenant, user)

	// Submit two stalled SELECTs to the mock Read Engine.
	stallQ := BuildPGMessage('Q', append([]byte("SELECT STALL"), 0))
	if _, err := c1.Write(stallQ); err != nil {
		t.Fatalf("write c1 stall query: %v", err)
	}
	if _, err := c2.Write(stallQ); err != nil {
		t.Fatalf("write c2 stall query: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for re.stallStarted.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if re.stallStarted.Load() < 2 {
		t.Fatalf("expected 2 stalled queries on mock read engine, got %d (activeConns=%d, metrics=%s)",
			re.stallStarted.Load(), re.activeConns.Load(), metrics.ToJSON(nil, tenant, ""))
	}

	// Disconnect both clients while queries are executing.
	startRepl := time.Now()
	_ = c1.Close()
	_ = c2.Close()

	// Allow the 10ms disconnect watcher to detect EOF and release tenant slots,
	// then immediately open 2 replacement connections and verify total time < 100ms.
	time.Sleep(25 * time.Millisecond)
	var r1, r2 net.Conn
	for time.Since(startRepl) < 100*time.Millisecond {
		dialRepl := func() net.Conn {
			c, err := net.DialTimeout("tcp", gwAddr, 50*time.Millisecond)
			if err != nil {
				return nil
			}
			_, _ = c.Write(BuildStartupMessage(map[string]string{"user": user, "database": tenant}))
			for {
				mt, mb, rerr := ReadPGPacket(c)
				if rerr != nil || mt == 'E' {
					_ = c.Close()
					return nil
				}
				if mt == 'R' && len(mb) >= 4 && binary.BigEndian.Uint32(mb[:4]) == 3 {
					_, _ = c.Write(BuildPGMessage('p', append([]byte("secret"), 0)))
					continue
				}
				if mt == 'Z' {
					return c
				}
			}
		}
		if r1 == nil {
			r1 = dialRepl()
		}
		if r2 == nil {
			r2 = dialRepl()
		}
		if r1 != nil && r2 != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	elapsed := time.Since(startRepl)
	if r1 == nil || r2 == nil {
		t.Fatalf("replacement connections failed to succeed within 100ms (elapsed=%v, r1=%v, r2=%v)", elapsed, r1 != nil, r2 != nil)
	}

	// Verify both replacement connections can execute a normal query.
	for idx, rc := range []net.Conn{r1, r2} {
		if _, err := rc.Write(BuildPGMessage('Q', append([]byte("SELECT 1"), 0))); err != nil {
			t.Fatalf("replacement %d write failed: %v", idx, err)
		}
		for {
			mt, _, err := ReadPGPacket(rc)
			if err != nil {
				t.Fatalf("replacement %d read failed: %v", idx, err)
			}
			if mt == 'Z' {
				break
			}
		}
		_ = rc.Close()
	}

	// Wait briefly for cleanup.
	gwWG.Wait()

	snap := metrics.ToJSON(nil, tenant, "")
	if !strings.Contains(snap, `"active_connections":0`) {
		t.Fatalf("expected active_connections=0 after disconnect & cleanup, got: %s", snap)
	}
	if !strings.Contains(snap, `"rejected_connections":0`) {
		t.Fatalf("expected 0 SQLSTATE 53300 rejections, got: %s", snap)
	}
	if got := metrics.clientDisconnects.Load(); got < 2 {
		t.Fatalf("expected client_disconnects_total >= 2, got %d", got)
	}
	if got := metrics.queryCancellations.Load(); got < 2 {
		t.Fatalf("expected query_cancellations_total >= 2, got %d", got)
	}

	// Verify stalled backend connections were closed (only idle pooled replacement backends may remain until pool.Close).
	pool.CloseAllIdle()
	waitZero := time.Now().Add(500 * time.Millisecond)
	for re.activeConns.Load() > 0 && time.Now().Before(waitZero) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := re.activeConns.Load(); got != 0 {
		t.Fatalf("expected 0 leaked backend connections on mock read engine, got %d", got)
	}

	// Verify no goroutine leak.
	time.Sleep(20 * time.Millisecond)
	runtime.GC()
	afterGoroutines := runtime.NumGoroutine()
	if afterGoroutines > baseGoroutines+4 {
		t.Fatalf("potential goroutine leak: base=%d after=%d", baseGoroutines, afterGoroutines)
	}
}

func TestPhase25_PipelinedBytesNotConsumedByDisconnectWatcher(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	serverCh := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			serverCh <- c
		}
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	server := <-serverCh
	defer server.Close()

	payload := []byte("PIPELINED_QUERY_BYTES")
	if _, err := client.Write(payload); err != nil {
		t.Fatalf("write payload: %v", err)
	}

	rc := extractRawConn(server)
	if rc == nil {
		t.Fatal("expected non-nil RawConn for TCPConn")
	}
	for i := 0; i < 5; i++ {
		if isSocketDisconnected(rc) {
			t.Fatal("expected connected socket with pipelined bytes not to report disconnected")
		}
	}

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatalf("read pipelined payload: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("pipelined bytes corrupted/consumed: got %q want %q", got, payload)
	}

	_ = client.Close()
	deadline := time.Now().Add(500 * time.Millisecond)
	for !isSocketDisconnected(rc) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !isSocketDisconnected(rc) {
		t.Fatal("expected closed socket to report disconnected")
	}
}
