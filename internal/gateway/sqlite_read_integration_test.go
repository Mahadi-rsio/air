package gateway

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/Mahadi-rsio/air/internal/readadapter"
	"github.com/jackc/pgx/v5"
)

func TestGateway_RoutesReadsToSQLiteReadEngineAndWritesToPrimary(t *testing.T) {
	// 1. Start real SQLiteReadServer
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

	const tenantID = "t_gw_sqlite"
	if err := sqliteSrv.EnsureTenantDB(tenantID); err != nil {
		t.Fatalf("EnsureTenantDB: %v", err)
	}
	// Seed SQLite Read Engine at LSN 0/01000000
	if err := sqliteSrv.ExecInternal(tenantID, `
		CREATE TABLE items (id INT4 PRIMARY KEY, name TEXT);
		INSERT INTO items VALUES (1, 'from-sqlite-1'), (2, 'from-sqlite-2');
	`, 0x01000000); err != nil {
		t.Fatalf("ExecInternal: %v", err)
	}

	// 2. Start minimal mock Write Engine (primary PG) that handles Startup + INSERT + LSN query
	writeLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen write mock: %v", err)
	}
	defer writeLn.Close()
	writePort := writeLn.Addr().(*net.TCPAddr).Port

	var writeQueriesMu sync.Mutex
	var writeQueries []string
	go func() {
		for {
			c, err := writeLn.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				// Read startup packet
				var lenBuf [4]byte
				if _, err := conn.Read(lenBuf[:]); err != nil {
					return
				}
				mlen := int(binary.BigEndian.Uint32(lenBuf[:]))
				if mlen > 4 {
					_, _ = conn.Read(make([]byte, mlen-4))
				}
				// Send AuthOk + BackendKeyData + ReadyForQuery
				_, _ = conn.Write(BuildAuthOK())
				_, _ = conn.Write(BuildBackendKeyData(1001, 2002))
				_, _ = conn.Write(BuildReadyForQuery('I'))

				for {
					mtype, body, err := ReadPGPacket(conn)
					if err != nil || mtype == 'X' {
						return
					}
					if mtype == 'P' {
						_, _ = conn.Write(BuildPGMessage('1', nil))
					} else if mtype == 'B' {
						_, _ = conn.Write(BuildPGMessage('2', nil))
					} else if mtype == 'D' {
						var td []byte
						td = appendBE16(td, 1)
						td = appendCStr(td, "name")
						td = appendBE32(td, 0)
						td = appendBE16(td, 0)
						td = appendBE32(td, 25)
						td = appendBE16(td, 0xFFFF)
						td = appendBE32(td, 0xFFFFFFFF)
						td = appendBE16(td, 0)
						_, _ = conn.Write(BuildPGMessage('T', td))
					} else if mtype == 'E' {
						val := []byte("from-primary-after-write")
						var dr []byte
						dr = appendBE16(dr, 1)
						dr = appendBE32(dr, uint32(len(val)))
						dr = append(dr, val...)
						_, _ = conn.Write(BuildPGMessage('D', dr))
						_, _ = conn.Write(BuildPGMessage('C', appendCStr(nil, "SELECT 1")))
					} else if mtype == 'S' {
						_, _ = conn.Write(BuildReadyForQuery('I'))
					} else if mtype == 'Q' {
						q := ReadCStr(body, new(int))
						if q != "" {
							writeQueriesMu.Lock()
							writeQueries = append(writeQueries, q)
							writeQueriesMu.Unlock()
						}
						if strings.Contains(q, "pg_current_wal_lsn") {
							// Return LSN 0/02000000 (ahead of SQLite's 0/01000000)
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
						} else if strings.Contains(strings.ToUpper(q), "SELECT") {
							var td []byte
							td = appendBE16(td, 1)
							td = appendCStr(td, "name")
							td = appendBE32(td, 0)
							td = appendBE16(td, 0)
							td = appendBE32(td, 25)
							td = appendBE16(td, 0xFFFF)
							td = appendBE32(td, 0xFFFFFFFF)
							td = appendBE16(td, 0)
							_, _ = conn.Write(BuildPGMessage('T', td))

							val := []byte("from-primary-after-write")
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

	// 3. Start Go Gateway TenantPool wired to both Write Engine and SQLite Read Engine
	gwLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gateway: %v", err)
	}
	defer gwLn.Close()

	pool := NewTenantPool(tenantID, "127.0.0.1", "", writePort, "secret", 4, 60)
	pool.SetReadEngineEndpoint("127.0.0.1", "", sqliteSrv.Port())
	defer pool.CloseAllIdle()

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
				// Read client StartupMessage
				var lenBuf [4]byte
				if _, err := conn.Read(lenBuf[:]); err != nil {
					return
				}
				mlen := int(binary.BigEndian.Uint32(lenBuf[:]))
				body := make([]byte, mlen-4)
				if _, err := conn.Read(body); err != nil {
					return
				}
				pool.ServeClient(conn, "app_user", "secret", nil, reg, creg, 0)
			}(c)
		}
	}()

	// 4. Connect pgx client to Go Gateway
	ctx := context.Background()
	dsn := fmt.Sprintf("postgres://app_user:secret@%s/%s?sslmode=disable", gwLn.Addr().String(), tenantID)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect to Go gateway: %v", err)
	}
	defer conn.Close(ctx)

	// 4a. Read-only query should go to SQLiteReadServer (not Write Engine)
	var name string
	if err := conn.QueryRow(ctx, "SELECT name FROM items WHERE id = $1", 2).Scan(&name); err != nil {
		t.Fatalf("SELECT via gateway -> SQLite: %v", err)
	}
	if name != "from-sqlite-2" {
		t.Fatalf("expected 'from-sqlite-2' from SQLiteReadServer, got %q", name)
	}

	allowed, _ := sqliteSrv.Stats()
	if allowed == 0 {
		t.Fatalf("expected SQLiteReadServer queries_allowed > 0")
	}

	// 4b. Write query (Simple Query) goes to Write Engine and updates sessionWriteLSN to 0/02000000
	if _, err := conn.Exec(ctx, "INSERT INTO items VALUES (3, 'new')", pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("INSERT via gateway: %v", err)
	}

	// 4c. Subsequent read sees causal lag (SQLite LSN 0/01000000 < sessionWriteLSN 0/02000000) and falls back to Write Engine
	var afterWrite string
	if err := conn.QueryRow(ctx, "SELECT name FROM items WHERE id = 3", pgx.QueryExecModeExec).Scan(&afterWrite); err != nil {
		t.Fatalf("causal fallback SELECT: %v", err)
	}
	if afterWrite != "from-primary-after-write" {
		t.Fatalf("expected causal fallback to primary, got %q", afterWrite)
	}
}
