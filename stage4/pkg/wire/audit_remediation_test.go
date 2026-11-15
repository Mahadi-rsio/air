package wire

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"air-stage4/pkg/cell"
	"air-stage4/pkg/crypto"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestRemediation_DrainVetoOnBackendErrors(t *testing.T) {
	// 1. Backend unavailable (connection refused)
	v1, err := NewVirtualizer("127.0.0.1:0", "", 19998, "app_user", "postgres", "pw")
	if err != nil {
		t.Fatal(err)
	}
	if err := v1.RequestDrain(300 * time.Millisecond); !errors.Is(err, ErrDrainVetoed) {
		v1.Close()
		t.Fatalf("expected ErrDrainVetoed on unavailable backend, got %v", err)
	}
	v1.Close()

	// 2. Query timeout (accepts TCP connection and stalls)
	hangLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer hangLn.Close()
	go func() {
		for {
			c, aErr := hangLn.Accept()
			if aErr != nil {
				return
			}
			go func(conn net.Conn) {
				time.Sleep(2 * time.Second)
				_ = conn.Close()
			}(c)
		}
	}()
	hangPort := hangLn.Addr().(*net.TCPAddr).Port
	v2, _ := NewVirtualizer("127.0.0.1:0", "", hangPort, "app_user", "postgres", "pw")
	if err := v2.RequestDrain(150 * time.Millisecond); !errors.Is(err, ErrDrainVetoed) {
		v2.Close()
		t.Fatalf("expected ErrDrainVetoed on query timeout, got %v", err)
	}
	v2.Close()

	// 3. Connection reset / immediate EOF
	resetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer resetLn.Close()
	go func() {
		for {
			c, aErr := resetLn.Accept()
			if aErr != nil {
				return
			}
			_ = c.Close()
		}
	}()
	resetPort := resetLn.Addr().(*net.TCPAddr).Port
	v3, _ := NewVirtualizer("127.0.0.1:0", "", resetPort, "app_user", "postgres", "pw")
	if err := v3.RequestDrain(250 * time.Millisecond); !errors.Is(err, ErrDrainVetoed) {
		v3.Close()
		t.Fatalf("expected ErrDrainVetoed on connection reset, got %v", err)
	}
	v3.Close()

	// 4. Permission denied (PG ErrorResponse 42501)
	permLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer permLn.Close()
	go func() {
		for {
			c, aErr := permLn.Accept()
			if aErr != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				be := pgproto3.NewBackend(conn, conn)
				_, _ = be.ReceiveStartupMessage()
				be.Send(&pgproto3.ErrorResponse{
					Severity: "FATAL",
					Code:     "42501",
					Message:  "permission denied for relation pg_prepared_xacts",
				})
				_ = be.Flush()
			}(c)
		}
	}()
	permPort := permLn.Addr().(*net.TCPAddr).Port
	v4, _ := NewVirtualizer("127.0.0.1:0", "", permPort, "app_user", "postgres", "pw")
	if err := v4.RequestDrain(300 * time.Millisecond); !errors.Is(err, ErrDrainVetoed) {
		v4.Close()
		t.Fatalf("expected ErrDrainVetoed on permission denied, got %v", err)
	}
	v4.Close()
}

func TestRemediation_BackendSessionStateIsolation(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/initdb"); err != nil {
		t.Skip("PG17 binary not installed on host")
	}
	volDir, err := os.MkdirTemp("/tmp", "air_wire_rem_")
	if err != nil {
		t.Fatal(err)
	}
	_ = os.Chmod(volDir, 0o755)
	defer os.RemoveAll(volDir)

	kms, _ := crypto.NewEnvelopeKMS()
	ag, err := cell.NewAgent(volDir, 0, kms)
	if err != nil {
		t.Fatal(err)
	}
	defer ag.Close()

	if _, err := ag.ClaimCluster("t_wire_iso"); err != nil {
		t.Fatal(err)
	}
	rt, err := ag.ActivateCluster("t_wire_iso", "pw_wire", 1)
	if err != nil {
		t.Fatal(err)
	}

	virt, err := NewVirtualizer("127.0.0.1:0", rt.SockDir, rt.Port, "app_user", "postgres", "pw_wire")
	if err != nil {
		t.Fatal(err)
	}
	defer virt.Close()

	ctx := context.Background()
	dsn := "postgres://app_user:pw_wire@" + virt.Addr() + "/postgres?sslmode=disable"

	// Client 1 mutates session GUC work_mem and disconnects
	c1, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c1.Exec(ctx, "SET work_mem = '32MB'"); err != nil {
		t.Fatal(err)
	}
	_ = c1.Close(ctx)
	time.Sleep(50 * time.Millisecond)

	// Client 2 must see default work_mem ('4MB'), NOT '32MB'!
	c2, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close(ctx)
	var wm string
	if err := c2.QueryRow(ctx, "SHOW work_mem").Scan(&wm); err != nil {
		t.Fatal(err)
	}
	if wm != "4MB" {
		t.Fatalf("backend session state leaked across clients! expected 4MB, got %s", wm)
	}
	_ = filepath.Join(volDir, "unused")
}
