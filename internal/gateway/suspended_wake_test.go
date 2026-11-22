package gateway

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/readadapter"
	"github.com/jackc/pgx/v5"
)

func TestGateway_SuspendedServesReadsViaReadEngineAndHoldsWriteToWake(t *testing.T) {
	// 1. Start real SQLiteReadServer as the Read Engine.
	sqliteSrv, err := readadapter.NewSQLiteReadServer(readadapter.SQLiteServerConfig{
		ListenHost: "127.0.0.1",
		PGPort:     0,
		DataDir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewSQLiteReadServer: %v", err)
	}
	if err := sqliteSrv.Start(); err != nil {
		t.Fatalf("sqliteSrv.Start: %v", err)
	}
	defer sqliteSrv.Stop()

	const tenantID = "t_gw_suspended"
	if err := sqliteSrv.EnsureTenantDB(tenantID); err != nil {
		t.Fatalf("EnsureTenantDB: %v", err)
	}
	if err := sqliteSrv.ExecInternal(tenantID, `
		CREATE TABLE items (id INT4 PRIMARY KEY, name TEXT);
		INSERT INTO items VALUES (1, 'from-read-while-suspended');
	`, 0x01000000); err != nil {
		t.Fatalf("ExecInternal: %v", err)
	}

	// 2. Prepare a mock primary PostgreSQL listener started lazily when wakeFn runs.
	var (
		writeMu      sync.Mutex
		writeLn      net.Listener
		writePort    int
		writeQueries []string
		wakeCount    atomic.Int32
		touchCount   atomic.Int32
	)
	defer func() {
		writeMu.Lock()
		if writeLn != nil {
			_ = writeLn.Close()
		}
		writeMu.Unlock()
	}()

	startMockWritePG := func() (int, error) {
		writeMu.Lock()
		defer writeMu.Unlock()
		if writeLn != nil {
			return writePort, nil
		}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return 0, err
		}
		writeLn = ln
		writePort = ln.Addr().(*net.TCPAddr).Port
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				go func(conn net.Conn) {
					defer conn.Close()
					var lenBuf [4]byte
					if _, err := conn.Read(lenBuf[:]); err != nil {
						return
					}
					mlen := int(binary.BigEndian.Uint32(lenBuf[:]))
					if mlen > 4 {
						_, _ = conn.Read(make([]byte, mlen-4))
					}
					_, _ = conn.Write(BuildAuthOK())
					_, _ = conn.Write(BuildBackendKeyData(2001, 3003))
					_, _ = conn.Write(BuildReadyForQuery('I'))

					for {
						mtype, body, err := ReadPGPacket(conn)
						if err != nil || mtype == 'X' {
							return
						}
						if mtype == 'Q' {
							q := ReadCStr(body, new(int))
							if q != "" {
								writeMu.Lock()
								writeQueries = append(writeQueries, q)
								writeMu.Unlock()
							}
							if strings.Contains(q, "pg_current_wal_lsn") {
								var td []byte
								td = appendBE16(td, 1)
								td = appendCStr(td, "pg_current_wal_lsn")
								td = appendBE32(td, 0)
								td = appendBE16(td, 0)
								td = appendBE32(td, 25)
								td = appendBE16(td, 0xFFFF)
								td = appendBE32(td, 0xFFFFFFFF)
								td = appendBE16(td, 0)
								_, _ = conn.Write(BuildPGMessage('T', td))

								val := []byte("0/02000000")
								var dr []byte
								dr = appendBE16(dr, 1)
								dr = appendBE32(dr, uint32(len(val)))
								dr = append(dr, val...)
								_, _ = conn.Write(BuildPGMessage('D', dr))
								_, _ = conn.Write(BuildPGMessage('C', appendCStr(nil, "SELECT 1")))
								_, _ = conn.Write(BuildReadyForQuery('I'))
							} else {
								_, _ = conn.Write(BuildPGMessage('C', appendCStr(nil, "INSERT 0 1")))
								_, _ = conn.Write(BuildReadyForQuery('I'))
							}
						}
					}
				}(c)
			}
		}()
		return writePort, nil
	}

	// 3. Create PoolManager with ReadEngine and a SUSPENDED tenant route.
	pm := NewPoolManager(4, 60)
	pm.SetReadEngineEndpoint("127.0.0.1", "", sqliteSrv.Port())
	pool := pm.GetOrCreate(TenantRoute{
		TenantID:    tenantID,
		State:       "SUSPENDED",
		Host:        "127.0.0.1",
		Port:        0,
		AppPassword: "secret",
	}, func() (TenantRoute, error) {
		wakeCount.Add(1)
		time.Sleep(50 * time.Millisecond) // simulate wake latency while query is held
		port, err := startMockWritePG()
		if err != nil {
			return TenantRoute{}, err
		}
		return TenantRoute{
			TenantID:    tenantID,
			State:       "ACTIVE",
			Host:        "127.0.0.1",
			Port:        port,
			AppPassword: "secret",
		}, nil
	})
	defer pool.CloseAllIdle()

	// 4. Start gateway listener serving the suspended pool.
	gwLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gateway: %v", err)
	}
	defer gwLn.Close()

	reg := NewMetricsRegistry(10, 10, 10)
	creg := NewCancelRegistry()
	go func() {
		for {
			c, err := gwLn.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				var lenBuf [4]byte
				if _, err := conn.Read(lenBuf[:]); err != nil {
					return
				}
				mlen := int(binary.BigEndian.Uint32(lenBuf[:]))
				if mlen > 4 {
					_, _ = conn.Read(make([]byte, mlen-4))
				}
				pool.ServeClient(conn, "app_user", "secret", func() {
					touchCount.Add(1)
				}, reg, creg, 0)
			}(c)
		}
	}()

	ctx := context.Background()
	dsn := fmt.Sprintf("postgres://app_user:secret@127.0.0.1:%d/%s?sslmode=disable&default_query_exec_mode=simple_protocol",
		gwLn.Addr().(*net.TCPAddr).Port, tenantID)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect to suspended tenant: %v", err)
	}
	defer conn.Close(ctx)

	// Step A: Read query must succeed via Read Engine WITHOUT waking primary PG.
	var name string
	if err := conn.QueryRow(ctx, "SELECT name FROM items WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("SELECT while suspended failed: %v", err)
	}
	if name != "from-read-while-suspended" {
		t.Fatalf("unexpected read result: got %q, want %q", name, "from-read-while-suspended")
	}
	if got := wakeCount.Load(); got != 0 {
		t.Fatalf("expected 0 wakes for read query while suspended, got %d", got)
	}
	if got := touchCount.Load(); got != 0 {
		t.Fatalf("expected 0 primary activity touches for read-only query, got %d", got)
	}

	// Step B: Write query must be held, wake primary PG, and succeed.
	if _, err := conn.Exec(ctx, "INSERT INTO items VALUES (2, 'woken-write')"); err != nil {
		t.Fatalf("INSERT while suspended failed: %v", err)
	}
	if got := wakeCount.Load(); got != 1 {
		t.Fatalf("expected exactly 1 wake for write query, got %d", got)
	}
	if got := touchCount.Load(); got < 1 {
		t.Fatalf("expected primary activity touch after wake, got %d", got)
	}

	// Step C: Second write query executes directly on woken primary without waking again.
	if _, err := conn.Exec(ctx, "INSERT INTO items VALUES (3, 'second-write')"); err != nil {
		t.Fatalf("second INSERT failed: %v", err)
	}
	if got := wakeCount.Load(); got != 1 {
		t.Fatalf("expected still 1 wake after second write, got %d", got)
	}

	writeMu.Lock()
	defer writeMu.Unlock()
	var sawFirst, sawSecond bool
	for _, q := range writeQueries {
		if strings.Contains(q, "woken-write") {
			sawFirst = true
		}
		if strings.Contains(q, "second-write") {
			sawSecond = true
		}
	}
	if !sawFirst || !sawSecond {
		t.Fatalf("expected both INSERTs on woken primary, got %v", writeQueries)
	}
}
