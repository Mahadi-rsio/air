package gateway

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/readadapter"
	"github.com/jackc/pgx/v5"
)

func TestPhase32_GatewayFallbackToRealPG17_ArraysAndSession(t *testing.T) {
	pgDSN := os.Getenv("AIR_PG17_DSN")
	if pgDSN == "" {
		pgDSN = "postgres://postgres:postgres@127.0.0.1:15432/air_diff?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Verify PG 17.12 is reachable and ensure p32_users exists
	pgConn, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG 17.12 (%s): %v", pgDSN, err)
	}
	_, err = pgConn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS p32_gw_items (id INT PRIMARY KEY, name TEXT);
		INSERT INTO p32_gw_items (id, name) VALUES (1, 'one'), (2, 'two') ON CONFLICT (id) DO NOTHING;
	`)
	pgConn.Close(ctx)
	if err != nil {
		t.Fatalf("seed PG 17.12: %v", err)
	}

	// Start SQLiteReadServer and sync from PG 17.12
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
	defer func() { _ = sqliteSrv.Stop() }()

	if err := sqliteSrv.SyncFromPrimary(ctx, "air_diff", pgDSN); err != nil {
		t.Fatalf("SyncFromPrimary: %v", err)
	}

	// Start Gateway TenantPool pointing to PG 17.12 (127.0.0.1:15432) and SQLiteReadServer
	gwLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gateway: %v", err)
	}
	defer gwLn.Close()

	pool := NewTenantPool("air_diff", "127.0.0.1", "", 15432, "postgres", 4, 60)
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
				var lenBuf [4]byte
				if _, err := conn.Read(lenBuf[:]); err != nil {
					return
				}
				mlen := int(binary.BigEndian.Uint32(lenBuf[:]))
				if mlen > 4 {
					body := make([]byte, mlen-4)
					if _, err := conn.Read(body); err != nil {
						return
					}
				}
				pool.ServeClient(conn, "postgres", "postgres", nil, reg, creg, 0)
			}(c)
		}
	}()

	gwDSN := fmt.Sprintf("postgres://postgres:postgres@%s/air_diff?sslmode=disable", gwLn.Addr().String())
	gwConn, err := pgx.Connect(ctx, gwDSN)
	if err != nil {
		t.Fatalf("connect gateway: %v", err)
	}
	defer gwConn.Close(ctx)

	// Arrays, IS DISTINCT FROM, and SHOW must safely fall back to PG 17.12 and succeed.
	var arr []int32
	if err := gwConn.QueryRow(ctx, "SELECT '{1,2,3}'::int[]").Scan(&arr); err != nil {
		t.Fatalf("array cast via gateway: %v", err)
	}
	if len(arr) != 3 || arr[0] != 1 || arr[2] != 3 {
		t.Fatalf("unexpected array result: %v", arr)
	}

	var distinct bool
	if err := gwConn.QueryRow(ctx, "SELECT 1 IS DISTINCT FROM NULL").Scan(&distinct); err != nil || !distinct {
		t.Fatalf("IS DISTINCT FROM via gateway: distinct=%v err=%v", distinct, err)
	}

	var tz string
	if err := gwConn.QueryRow(ctx, "SHOW TimeZone").Scan(&tz); err != nil || tz == "" {
		t.Fatalf("SHOW TimeZone via gateway: tz=%q err=%v", tz, err)
	}

	// Read-only SELECT routes to SQLiteReadServer
	var cnt int64
	if err := gwConn.QueryRow(ctx, "SELECT COUNT(*) FROM p32_gw_items").Scan(&cnt); err != nil || cnt < 2 {
		t.Fatalf("SELECT COUNT(*) via gateway: cnt=%d err=%v", cnt, err)
	}
	allowed, _ := sqliteSrv.Stats()
	if allowed == 0 {
		t.Fatalf("expected SQLiteReadServer queries_allowed > 0, got 0")
	}
}
