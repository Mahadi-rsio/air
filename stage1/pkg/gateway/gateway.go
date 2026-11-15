package gateway

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"air-stage1/pkg/control"

	bolt "go.etcd.io/bbolt"
)

const (
	maxStartupMessageBytes = 8192 // 8 KB StartupMessage cap (FMEA-16, RISK-23)
	sslRequestCode         = 80877103
)

var walBucketName = []byte("ru_wal")

type Gateway struct {
	gatewayID       string
	listenAddr      string
	listener        net.Listener
	tlsConfig       *tls.Config
	cp              *control.ControlPlane
	boltDB          *bolt.DB
	bufPool         sync.Pool
	seqNo           atomic.Int64
	lengthBombsDrop atomic.Int64
	sniSpoofsDrop   atomic.Int64
	softDeleteDrop  atomic.Int64
}

func NewGateway(gatewayID, listenAddr, boltPath string, cp *control.ControlPlane) (*Gateway, error) {
	tlsCfg, err := generateWildcardTLSConfig()
	if err != nil {
		return nil, err
	}
	db, err := bolt.Open(boltPath, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open bbolt WAL: %w", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(walBucketName)
		return e
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	gw := &Gateway{
		gatewayID:  gatewayID,
		listenAddr: ln.Addr().String(),
		listener:   ln,
		tlsConfig:  tlsCfg,
		cp:         cp,
		boltDB:     db,
		bufPool: sync.Pool{
			New: func() any {
				b := make([]byte, 32*1024)
				return &b
			},
		},
	}
	go gw.acceptLoop()
	return gw, nil
}

func (g *Gateway) Addr() string {
	return g.listenAddr
}

func (g *Gateway) Stats() (lengthBombs, sniSpoofs, softDeletes int64) {
	return g.lengthBombsDrop.Load(), g.sniSpoofsDrop.Load(), g.softDeleteDrop.Load()
}

func (g *Gateway) acceptLoop() {
	for {
		conn, err := g.listener.Accept()
		if err != nil {
			return
		}
		go g.handleConn(conn)
	}
}

func (g *Gateway) handleConn(rawConn net.Conn) {
	defer func() { _ = rawConn.Close() }()

	// 1. Enforce strict 5s pre-auth deadline (RISK-23)
	_ = rawConn.SetDeadline(time.Now().Add(5 * time.Second))

	var hdr [8]byte
	if _, err := io.ReadFull(rawConn, hdr[:]); err != nil {
		return
	}
	msgLen := int(binary.BigEndian.Uint32(hdr[0:4]))
	code := int(binary.BigEndian.Uint32(hdr[4:8]))

	var clientConn net.Conn = rawConn
	var sniHost string

	// Handle PostgreSQL SSLRequest (8 bytes, code 80877103)
	if msgLen == 8 && code == sslRequestCode {
		if _, err := rawConn.Write([]byte{'S'}); err != nil {
			return
		}
		tlsConn := tls.Server(rawConn, g.tlsConfig)
		if err := tlsConn.Handshake(); err != nil {
			return
		}
		sniHost = tlsConn.ConnectionState().ServerName
		clientConn = tlsConn

		// Read actual StartupMessage header after TLS upgrade
		if _, err := io.ReadFull(clientConn, hdr[:]); err != nil {
			return
		}
		msgLen = int(binary.BigEndian.Uint32(hdr[0:4]))
		code = int(binary.BigEndian.Uint32(hdr[4:8]))
	}

	// 2. Enforce 8 KB StartupMessage length cap BEFORE allocating buffer (FMEA-16)
	if msgLen < 8 || msgLen > maxStartupMessageBytes {
		g.lengthBombsDrop.Add(1)
		_ = writePGError(clientConn, "08P01", "StartupMessage length exceeds 8KB security limit")
		return
	}

	bodyLen := msgLen - 8
	body := make([]byte, bodyLen)
	if bodyLen > 0 {
		if _, err := io.ReadFull(clientConn, body); err != nil {
			return
		}
	}

	params := parseStartupParams(body)
	startupTenant, usePooler := extractTenantAndMode(sniHost, params)
	if startupTenant == "" {
		_ = writePGError(clientConn, "28000", "missing tenant identifier in SNI or StartupMessage")
		return
	}

	// 3. Cryptographic SNI == Startup tenant identity binding (SI-3, FMEA-17)
	if sniHost != "" {
		sniTenant := parseSNITenant(sniHost)
		if sniTenant != "" && params["database"] != "" && params["database"] != "postgres" && sniTenant != params["database"] {
			g.sniSpoofsDrop.Add(1)
			_ = writePGError(clientConn, "28000", fmt.Sprintf("SNI tenant (%s) does not match StartupMessage database tenant (%s)", sniTenant, params["database"]))
			return
		}
	}

	// 4. Lookup tenant route & state in Control Plane
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	state, _, directPort, _, err := g.cp.GetTenantAuth(ctx, startupTenant)
	cancel()
	if err != nil {
		_ = writePGError(clientConn, "3D000", fmt.Sprintf("tenant database %q not found", startupTenant))
		return
	}
	if state == "SOFT_DELETED" || state == "PURGED" {
		g.softDeleteDrop.Add(1)
		_ = writePGError(clientConn, "57P03", fmt.Sprintf("tenant database %q is %s and cannot accept connections", startupTenant, state))
		return
	}
	if state != "ACTIVE" {
		_ = writePGError(clientConn, "57P03", fmt.Sprintf("tenant database %q is in state %s", startupTenant, state))
		return
	}

	// 5. Dial backend: either PgBouncer 1.21+ transaction pooler (6432) or Direct PG17 port
	targetAddr := fmt.Sprintf("127.0.0.1:%d", directPort)
	targetDBName := "postgres"
	if usePooler {
		targetAddr = "127.0.0.1:6432"
		targetDBName = startupTenant
	}

	backendConn, err := net.DialTimeout("tcp", targetAddr, 2*time.Second)
	if err != nil {
		_ = writePGError(clientConn, "08006", "failed to dial cell backend")
		return
	}
	defer func() { _ = backendConn.Close() }()

	// Clear pre-auth 5s deadline; streaming pump uses 30s SetWriteDeadline per frame
	_ = rawConn.SetDeadline(time.Time{})

	// Forward rewritten StartupMessage to backend
	rewrittenStartup := buildStartupMessage(uint32(code), params, targetDBName)
	if _, err := backendConn.Write(rewrittenStartup); err != nil {
		return
	}

	tStart := time.Now()
	var stmts atomic.Int32
	var rowsOut atomic.Int64
	var bytesOut atomic.Int64

	errCh := make(chan error, 2)

	// Client -> Backend pump (with 32KB sync.Pool buffer)
	go func() {
		bufPtr := g.bufPool.Get().(*[]byte)
		defer g.bufPool.Put(bufPtr)
		buf := *bufPtr
		for {
			n, rErr := clientConn.Read(buf)
			if n > 0 {
				_ = backendConn.SetWriteDeadline(time.Now().Add(30 * time.Second))
				if _, wErr := backendConn.Write(buf[:n]); wErr != nil {
					errCh <- wErr
					return
				}
			}
			if rErr != nil {
				errCh <- rErr
				return
			}
		}
	}()

	// Backend -> Client pump + lightweight 5-byte frame scanner for RU metering (ADR-009, ADR-012)
	go func() {
		bufPtr := g.bufPool.Get().(*[]byte)
		defer g.bufPool.Put(bufPtr)
		buf := *bufPtr
		for {
			n, rErr := backendConn.Read(buf)
			if n > 0 {
				bytesOut.Add(int64(n))
				scanBackendFrames(buf[:n], &stmts, &rowsOut)
				_ = clientConn.SetWriteDeadline(time.Now().Add(30 * time.Second))
				if _, wErr := clientConn.Write(buf[:n]); wErr != nil {
					errCh <- wErr
					return
				}
			}
			if rErr != nil {
				errCh <- rErr
				return
			}
		}
	}()

	<-errCh

	// 6. Persist RU metering event to local bbolt WAL if statements > 0 (RISK-28)
	sc := int(stmts.Load())
	if sc > 0 {
		wallMs := time.Since(tStart).Milliseconds()
		if wallMs < 1 {
			wallMs = 1
		}
		bOut := bytesOut.Load()
		ru := int64(sc) + (wallMs+9)/10 + (bOut+4095)/4096
		_ = g.recordUsageToBoltWAL(startupTenant, sc, rowsOut.Load(), bOut, wallMs, ru)
	}
}

// scanBackendFrames inspects backend bytes for 'C' (CommandComplete) and 'D' (DataRow) markers.
func scanBackendFrames(chunk []byte, stmts *atomic.Int32, rows *atomic.Int64) {
	i := 0
	for i+5 <= len(chunk) {
		msgType := chunk[i]
		frameLen := int(binary.BigEndian.Uint32(chunk[i+1 : i+5]))
		if frameLen < 4 || frameLen > 64*1024*1024 {
			break
		}
		switch msgType {
		case 'C':
			stmts.Add(1)
		case 'D':
			rows.Add(1)
		}
		next := i + 1 + frameLen
		if next <= i || next > len(chunk) {
			break
		}
		i = next
	}
}

func (g *Gateway) recordUsageToBoltWAL(tenantID string, stmts int, rows, bytesOut, wallMs, ru int64) error {
	seq := g.seqNo.Add(1)
	rawKey := fmt.Sprintf("%s|%s|%d", g.gatewayID, tenantID, seq)
	sum := sha256.Sum256([]byte(rawKey))
	idempKey := hex.EncodeToString(sum[:])

	ev := control.UsageEventBatch{
		IdempotencyKey: idempKey,
		GatewayID:      g.gatewayID,
		TenantID:       tenantID,
		Statements:     stmts,
		RowsAffected:   rows,
		BytesOut:       bytesOut,
		WallMs:         wallMs,
		RUCharged:      ru,
	}
	val, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return g.boltDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(walBucketName)
		return b.Put([]byte(idempKey), val)
	})
}

// ReadPendingBoltWAL reads all uncommitted RU usage batches currently persisted in local bbolt WAL.
func (g *Gateway) ReadPendingBoltWAL() ([]control.UsageEventBatch, error) {
	var out []control.UsageEventBatch
	err := g.boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(walBucketName)
		return b.ForEach(func(k, v []byte) error {
			var ev control.UsageEventBatch
			if err := json.Unmarshal(v, &ev); err == nil {
				out = append(out, ev)
			}
			return nil
		})
	})
	return out, err
}

// FlushBoltWALToControlPlane ships local bbolt WAL batches to Control Plane (optionally replaying N times to test idempotency).
func (g *Gateway) FlushBoltWALToControlPlane(ctx context.Context, replayAttempts int) (totalInserted int, batchCount int, err error) {
	batch, err := g.ReadPendingBoltWAL()
	if err != nil || len(batch) == 0 {
		return 0, 0, err
	}
	batchCount = len(batch)
	if replayAttempts < 1 {
		replayAttempts = 1
	}
	for i := 0; i < replayAttempts; i++ {
		ins, err := g.cp.IngestUsageBatch(ctx, batch)
		if err != nil {
			return totalInserted, batchCount, err
		}
		totalInserted += ins
	}
	// Clear acked entries from bbolt WAL
	err = g.boltDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(walBucketName)
		for _, ev := range batch {
			_ = b.Delete([]byte(ev.IdempotencyKey))
		}
		return nil
	})
	return totalInserted, batchCount, err
}

func (g *Gateway) Close() {
	_ = g.listener.Close()
	_ = g.boltDB.Close()
}

func parseStartupParams(body []byte) map[string]string {
	out := make(map[string]string)
	parts := bytes.Split(body, []byte{0})
	for i := 0; i+1 < len(parts); i += 2 {
		k := string(parts[i])
		v := string(parts[i+1])
		if k == "" {
			break
		}
		out[k] = v
	}
	return out
}

func parseSNITenant(sni string) string {
	parts := strings.Split(sni, ".")
	if len(parts) == 0 {
		return ""
	}
	first := parts[0]
	return strings.TrimSuffix(first, "-pooler")
}

func extractTenantAndMode(sni string, params map[string]string) (tenant string, usePooler bool) {
	if sni != "" {
		first := strings.Split(sni, ".")[0]
		if strings.HasSuffix(first, "-pooler") {
			return strings.TrimSuffix(first, "-pooler"), true
		}
		return first, false
	}
	db := params["database"]
	if strings.HasSuffix(db, "-pooler") {
		return strings.TrimSuffix(db, "-pooler"), true
	}
	if db != "" && db != "postgres" {
		return db, false
	}
	return "", false
}

func buildStartupMessage(proto uint32, params map[string]string, targetDB string) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint32(0)) // placeholder for length
	_ = binary.Write(&buf, binary.BigEndian, proto)
	for k, v := range params {
		if k == "database" {
			v = targetDB
		}
		buf.WriteString(k)
		buf.WriteByte(0)
		buf.WriteString(v)
		buf.WriteByte(0)
	}
	buf.WriteByte(0)
	b := buf.Bytes()
	binary.BigEndian.PutUint32(b[0:4], uint32(len(b)))
	return b
}

// writePGError constructs a spec-compliant PostgreSQL Wire Protocol ErrorResponse ('E') frame (RISK-34).
func writePGError(w io.Writer, sqlState, message string) error {
	var payload bytes.Buffer
	payload.WriteByte('S')
	payload.WriteString("FATAL")
	payload.WriteByte(0)
	payload.WriteByte('C')
	payload.WriteString(sqlState)
	payload.WriteByte(0)
	payload.WriteByte('M')
	payload.WriteString(message)
	payload.WriteByte(0)
	payload.WriteByte(0)

	var frame bytes.Buffer
	frame.WriteByte('E')
	_ = binary.Write(&frame, binary.BigEndian, int32(payload.Len()+4))
	frame.Write(payload.Bytes())
	_, err := w.Write(frame.Bytes())
	return err
}

func generateWildcardTLSConfig() (*tls.Config, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "*.db.air.local"},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"*.db.air.local", "localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, err
	}
	cert := tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  priv,
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

var ErrUnexpected = errors.New("unexpected gateway error")
