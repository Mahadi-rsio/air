package control

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/jackc/pgx/v5"
)

func freeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeTCPPort: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestNeonServerlessLifecycleWithPageserver(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/postgres"); err != nil {
		t.Skip("PostgreSQL 17 binaries not installed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Note: Unix socket paths have a 107-byte sun_path limit on Linux.
	tmpDir, err := os.MkdirTemp("/tmp", "air-ps-e2e-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatalf("NewEnvelopeKMS: %v", err)
	}

	volDir := filepath.Join(tmpDir, "vol")
	ag, err := cell.NewAgent(volDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer ag.Close()

	metaDir := filepath.Join(tmpDir, "meta")
	metaPort := freeTCPPort(t)
	cp, err := NewControlPlane(ctx, metaDir, "/usr/lib/postgresql/17/bin", metaPort, ag, kms)
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	defer cp.Close()

	psSock := filepath.Join(tmpDir, "ps.sock")
	ps, err := pageserver.New(pageserver.Config{
		SocketPath:    psSock,
		LayerDir:      filepath.Join(tmpDir, "ps_layers"),
		CacheDir:      filepath.Join(tmpDir, "ps_cache"),
		CacheMaxBytes: 32 * 1024 * 1024,
		KMS:           kms,
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	if err := ps.Start(ctx); err != nil {
		t.Fatalf("ps.Start: %v", err)
	}
	defer ps.Close()
	cp.SetPageserver(ps)

	const accountID = "acct_neon"
	const tenantID = "t_neon_e2e"
	resp, err := cp.ProvisionDatabase(ctx, accountID, "idem-neon-1", tenantID)
	if err != nil {
		t.Fatalf("ProvisionDatabase: %v", err)
	}
	if resp.State != "ACTIVE" {
		t.Fatalf("expected ACTIVE, got %s", resp.State)
	}

	tl := ps.EnsureTenant(tenantID, resp.FencingEpoch)
	initLSN := tl.LastLSN()
	if initLSN == 0 {
		t.Fatalf("expected bootstrapped initLSN > 0")
	}

	// Connect to the live tenant database as app_user and write data.
	dsn := fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", resp.Password, resp.DirectPort, resp.Database)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect tenant: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		CREATE TABLE neon_items (id int PRIMARY KEY, payload text);
		INSERT INTO neon_items SELECT g, repeat('n', 200) FROM generate_series(1, 250) g;
	`); err != nil {
		conn.Close(ctx)
		t.Fatalf("create/insert neon_items: %v", err)
	}

	var dbOID, relNode uint32
	if err := conn.QueryRow(ctx, `
		SELECT (SELECT oid FROM pg_database WHERE datname = current_database())::int4,
		       pg_relation_filenode('neon_items')::int4
	`).Scan(&dbOID, &relNode); err != nil {
		conn.Close(ctx)
		t.Fatalf("query dbOID/relNode: %v", err)
	}
	conn.Close(ctx)

	// Wait for live WALReceiver to stream the CREATE/INSERT WAL records into Pageserver.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if tl.LastLSN() > initLSN {
			nblks, err := ps.GetRelSize(ctx, tenantID, 1663, dbOID, relNode, 0, tl.LastLSN())
			if err == nil && nblks > 0 {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	afterWriteLSN := tl.LastLSN()
	if afterWriteLSN <= initLSN {
		t.Fatalf("expected WALReceiver to advance LSN past %x, got %x", initLSN, afterWriteLSN)
	}

	// Query Pageserver over the real Unix socket (exact air_smgr wire protocol).
	c, err := net.Dial("unix", psSock)
	if err != nil {
		t.Fatalf("dial pageserver unix socket: %v", err)
	}
	req := make([]byte, pageserver.ReqHeaderSize)
	binary.LittleEndian.PutUint32(req[0:4], pageserver.ProtocolMagic)
	binary.LittleEndian.PutUint16(req[4:6], pageserver.ProtocolVersion)
	binary.LittleEndian.PutUint16(req[6:8], pageserver.OpGetRelSize)
	copy(req[8:72], tenantID)
	binary.LittleEndian.PutUint32(req[72:76], 1663)
	binary.LittleEndian.PutUint32(req[76:80], dbOID)
	binary.LittleEndian.PutUint32(req[80:84], relNode)
	req[84] = 0
	binary.LittleEndian.PutUint64(req[96:104], afterWriteLSN)
	if _, err := c.Write(req); err != nil {
		c.Close()
		t.Fatalf("write OpGetRelSize: %v", err)
	}
	hdr := make([]byte, pageserver.RespHeaderSize)
	if _, err := c.Read(hdr); err != nil {
		c.Close()
		t.Fatalf("read OpGetRelSize resp: %v", err)
	}
	c.Close()
	if status := binary.LittleEndian.Uint16(hdr[6:8]); status != pageserver.StatusOK {
		t.Fatalf("expected StatusOK from pageserver socket, got %d", status)
	}
	nblocks := binary.LittleEndian.Uint32(hdr[12:16])
	if nblocks == 0 {
		t.Fatalf("expected nblocks > 0 for neon_items via pageserver socket")
	}

	// Fetch block 0 of neon_items via OpGetPageAtLSN over the Unix socket.
	c2, err := net.Dial("unix", psSock)
	if err != nil {
		t.Fatalf("dial pageserver unix socket for page: %v", err)
	}
	req2 := make([]byte, pageserver.ReqHeaderSize)
	binary.LittleEndian.PutUint32(req2[0:4], pageserver.ProtocolMagic)
	binary.LittleEndian.PutUint16(req2[4:6], pageserver.ProtocolVersion)
	binary.LittleEndian.PutUint16(req2[6:8], pageserver.OpGetPageAtLSN)
	copy(req2[8:72], tenantID)
	binary.LittleEndian.PutUint32(req2[72:76], 1663)
	binary.LittleEndian.PutUint32(req2[76:80], dbOID)
	binary.LittleEndian.PutUint32(req2[80:84], relNode)
	req2[84] = 0
	binary.LittleEndian.PutUint32(req2[88:92], 0) // block 0
	binary.LittleEndian.PutUint32(req2[92:96], 1)
	binary.LittleEndian.PutUint64(req2[96:104], afterWriteLSN)
	if _, err := c2.Write(req2); err != nil {
		c2.Close()
		t.Fatalf("write OpGetPageAtLSN: %v", err)
	}
	hdr2 := make([]byte, pageserver.RespHeaderSize)
	if _, err := c2.Read(hdr2); err != nil {
		c2.Close()
		t.Fatalf("read OpGetPageAtLSN resp: %v", err)
	}
	if status := binary.LittleEndian.Uint16(hdr2[6:8]); status != pageserver.StatusOK {
		c2.Close()
		t.Fatalf("expected StatusOK for OpGetPageAtLSN, got %d", status)
	}
	payloadLen := binary.LittleEndian.Uint32(hdr2[8:12])
	if payloadLen != uint32(pageserver.PageSize) {
		c2.Close()
		t.Fatalf("expected 8192-byte page payload, got %d", payloadLen)
	}
	pageBuf := make([]byte, payloadLen)
	if _, err := c2.Read(pageBuf); err != nil {
		c2.Close()
		t.Fatalf("read page payload: %v", err)
	}
	c2.Close()

	// Suspend tenant (scale to zero): stops PG17, stops WALReceiver, flushes L0 layer to disk.
	if err := cp.SuspendTenant(ctx, tenantID, 5*time.Second, nil); err != nil {
		t.Fatalf("SuspendTenant: %v", err)
	}
	if tl.LayerCount() == 0 {
		t.Fatalf("expected at least 1 flushed layer after SuspendTenant")
	}

	// Wake tenant (scale from zero): restarts PG17, reconnects WALReceiver, accepts new writes.
	wakePort, _, wakePW, err := cp.WakeTenant(ctx, tenantID)
	if err != nil {
		t.Fatalf("WakeTenant: %v", err)
	}
	wakeDSN := fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", wakePW, wakePort, tenantID)
	conn2, err := pgx.Connect(ctx, wakeDSN)
	if err != nil {
		t.Fatalf("pgx.Connect after wake: %v", err)
	}
	if _, err := conn2.Exec(ctx, `INSERT INTO neon_items SELECT g, repeat('w', 200) FROM generate_series(251, 300) g`); err != nil {
		conn2.Close(ctx)
		t.Fatalf("insert after wake: %v", err)
	}
	var total int
	if err := conn2.QueryRow(ctx, `SELECT count(*) FROM neon_items`).Scan(&total); err != nil {
		conn2.Close(ctx)
		t.Fatalf("count after wake: %v", err)
	}
	conn2.Close(ctx)
	if total != 300 {
		t.Fatalf("expected 300 rows after wake, got %d", total)
	}

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if tl.LastLSN() > afterWriteLSN {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if tl.LastLSN() <= afterWriteLSN {
		t.Fatalf("expected WALReceiver after wake to advance LSN past %x, got %x", afterWriteLSN, tl.LastLSN())
	}
}
