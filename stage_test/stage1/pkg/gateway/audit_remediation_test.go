package gateway

import (
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func buildTestStartupPacket(params map[string]string) []byte {
	return buildStartupMessage(196608, params, params["database"])
}

func readPGErrorResponse(t *testing.T, c net.Conn) string {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	var hdr [5]byte
	if _, err := io.ReadFull(c, hdr[:]); err != nil {
		t.Fatalf("read PG error header: %v", err)
	}
	if hdr[0] != 'E' {
		t.Fatalf("expected 'E' ErrorResponse, got %q", hdr[0])
	}
	ln := int(binary.BigEndian.Uint32(hdr[1:5]))
	body := make([]byte, ln-4)
	if _, err := io.ReadFull(c, body); err != nil {
		t.Fatalf("read PG error body: %v", err)
	}
	return string(body)
}

func TestGateway_RejectsReservedPostgresUserAndInvalidTenant(t *testing.T) {
	boltPath := filepath.Join(t.TempDir(), "gw.bolt")
	gw, err := NewGateway("gw-test", "127.0.0.1:0", boltPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer gw.Close()

	// 1. Reserved superuser "postgres" must be rejected before dialing backend
	c1, err := net.DialTimeout("tcp", gw.Addr(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c1.Write(buildTestStartupPacket(map[string]string{"user": "postgres", "database": "tenant_a"}))
	errBody := readPGErrorResponse(t, c1)
	_ = c1.Close()
	if !strings.Contains(errBody, "28000") || !strings.Contains(errBody, "reserved superuser") {
		t.Fatalf("expected 28000 reserved superuser rejection, got %q", errBody)
	}

	// 2. Invalid tenant ID (path traversal) must be rejected
	c2, err := net.DialTimeout("tcp", gw.Addr(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c2.Write(buildTestStartupPacket(map[string]string{"user": "app_user", "database": "../escape"}))
	errBody2 := readPGErrorResponse(t, c2)
	_ = c2.Close()
	if !strings.Contains(errBody2, "28000") {
		t.Fatalf("expected 28000 invalid tenant rejection, got %q", errBody2)
	}
}

func TestGateway_SNISpoofRejected(t *testing.T) {
	boltPath := filepath.Join(t.TempDir(), "gw_sni.bolt")
	gw, err := NewGateway("gw-test-sni", "127.0.0.1:0", boltPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer gw.Close()

	raw, err := net.DialTimeout("tcp", gw.Addr(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	// Send SSLRequest (len=8, code=80877103)
	var sslReq [8]byte
	binary.BigEndian.PutUint32(sslReq[0:4], 8)
	binary.BigEndian.PutUint32(sslReq[4:8], sslRequestCode)
	_, _ = raw.Write(sslReq[:])

	var ack [1]byte
	if _, err := io.ReadFull(raw, ack[:]); err != nil || ack[0] != 'S' {
		t.Fatalf("expected 'S' SSL ack, got %v (err=%v)", ack, err)
	}

	tlsConn := tls.Client(raw, &tls.Config{
		ServerName:         "tenant-a.db.air.local",
		InsecureSkipVerify: true,
	})
	if err := tlsConn.Handshake(); err != nil {
		t.Fatal(err)
	}
	defer tlsConn.Close()

	// Send StartupMessage claiming a DIFFERENT database tenant_b
	_, _ = tlsConn.Write(buildTestStartupPacket(map[string]string{"user": "app_user", "database": "tenant-b"}))
	errBody := readPGErrorResponse(t, tlsConn)
	if !strings.Contains(errBody, "28000") || !strings.Contains(errBody, "does not match") {
		t.Fatalf("expected SNI spoof rejection, got %q", errBody)
	}
	_, sniSpoofs, _ := gw.Stats()
	if sniSpoofs != 1 {
		t.Fatalf("expected sniSpoofsDrop=1, got %d", sniSpoofs)
	}
}
