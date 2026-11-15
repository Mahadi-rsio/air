package gateway

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Mahadi-rsio/air/internal/control"
	"github.com/Mahadi-rsio/air/internal/tenant"
)

const (
	maxStartupMessageBytes = 8192
	sslRequestCode         = 80877103
)

// GatewayOptions configures TLS enforcement and optional custom certificate loading on the Gateway.
type GatewayOptions struct {
	RequireTLS  bool
	TLSCertPath string
	TLSKeyPath  string
}

// Gateway is the AIR PostgreSQL wire-protocol gateway.
// It terminates TLS/SNI, enforces tenant identity & role checks,
// and proxies authenticated sessions to the tenant's PostgreSQL 17 backend.
type Gateway struct {
	listenAddr string
	listener   net.Listener
	tlsConfig  *tls.Config
	requireTLS bool
	cp         *control.ControlPlane
	bufPool    sync.Pool
}

func NewGateway(listenAddr string, cp *control.ControlPlane) (*Gateway, error) {
	return NewGatewayWithOptions(listenAddr, cp, GatewayOptions{})
}

func NewGatewayWithOptions(listenAddr string, cp *control.ControlPlane, opts GatewayOptions) (*Gateway, error) {
	var (
		tlsCfg *tls.Config
		err    error
	)
	if opts.TLSCertPath != "" && opts.TLSKeyPath != "" {
		cert, loadErr := tls.LoadX509KeyPair(opts.TLSCertPath, opts.TLSKeyPath)
		if loadErr != nil {
			return nil, fmt.Errorf("load gateway TLS cert/key: %w", loadErr)
		}
		tlsCfg = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	} else {
		tlsCfg, err = generateSelfSignedTLSConfig()
		if err != nil {
			return nil, err
		}
	}
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	gw := &Gateway{
		listenAddr: ln.Addr().String(),
		listener:   ln,
		tlsConfig:  tlsCfg,
		requireTLS: opts.RequireTLS,
		cp:         cp,
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

func (g *Gateway) Close() error {
	return g.listener.Close()
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
	_ = rawConn.SetDeadline(time.Now().Add(5 * time.Second))

	var hdr [8]byte
	if _, err := io.ReadFull(rawConn, hdr[:]); err != nil {
		return
	}
	msgLen := int(binary.BigEndian.Uint32(hdr[0:4]))
	code := int(binary.BigEndian.Uint32(hdr[4:8]))

	var clientConn net.Conn = rawConn
	var sniHost string
	var tlsNegotiated bool

	if msgLen == 8 && code == sslRequestCode {
		if _, err := rawConn.Write([]byte{'S'}); err != nil {
			return
		}
		tlsConn := tls.Server(rawConn, g.tlsConfig)
		if err := tlsConn.Handshake(); err != nil {
			return
		}
		tlsNegotiated = true
		sniHost = tlsConn.ConnectionState().ServerName
		clientConn = tlsConn

		if _, err := io.ReadFull(clientConn, hdr[:]); err != nil {
			return
		}
		msgLen = int(binary.BigEndian.Uint32(hdr[0:4]))
		code = int(binary.BigEndian.Uint32(hdr[4:8]))
	}

	if g.requireTLS && !tlsNegotiated {
		_ = writePGError(clientConn, "28000", "SSL/TLS connection is required")
		return
	}

	if msgLen < 8 || msgLen > maxStartupMessageBytes {
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
	startupTenant := extractTenant(sniHost, params)
	if startupTenant == "" || tenant.ValidateID(startupTenant) != nil {
		_ = writePGError(clientConn, "28000", "missing or invalid tenant identifier")
		return
	}

	reqUser := strings.TrimSpace(params["user"])
	if reqUser == "" || strings.EqualFold(reqUser, "postgres") {
		_ = writePGError(clientConn, "28000", "reserved superuser role not permitted through tenant gateway")
		return
	}

	if sniHost != "" {
		sniTenant := parseSNITenant(sniHost)
		dbTenant := strings.TrimSpace(params["database"])
		if sniTenant != "" && dbTenant != "" && dbTenant != "postgres" && sniTenant != dbTenant {
			_ = writePGError(clientConn, "28000", fmt.Sprintf("SNI tenant (%s) does not match StartupMessage database (%s)", sniTenant, dbTenant))
			return
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	state, directPort, err := g.cp.GetTenantRoute(ctx, startupTenant)
	cancel()
	if err != nil {
		_ = writePGError(clientConn, "3D000", fmt.Sprintf("tenant database %q not found", startupTenant))
		return
	}
	if state != "ACTIVE" || directPort <= 0 {
		_ = writePGError(clientConn, "57P03", fmt.Sprintf("tenant database %q is in state %s", startupTenant, state))
		return
	}

	backendConn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", directPort), 2*time.Second)
	if err != nil {
		_ = writePGError(clientConn, "08006", "failed to dial cell backend")
		return
	}
	defer func() { _ = backendConn.Close() }()

	_ = clientConn.SetDeadline(time.Time{})

	rewrittenStartup := buildStartupMessage(uint32(code), params, startupTenant)
	if _, err := backendConn.Write(rewrittenStartup); err != nil {
		return
	}

	errCh := make(chan error, 2)
	go g.pump(backendConn, clientConn, errCh)
	go g.pump(clientConn, backendConn, errCh)
	<-errCh
}

func (g *Gateway) pump(dst, src net.Conn, errCh chan<- error) {
	bufPtr := g.bufPool.Get().(*[]byte)
	defer g.bufPool.Put(bufPtr)
	buf := *bufPtr
	for {
		n, rErr := src.Read(buf)
		if n > 0 {
			_ = dst.SetWriteDeadline(time.Now().Add(30 * time.Second))
			if _, wErr := dst.Write(buf[:n]); wErr != nil {
				errCh <- wErr
				return
			}
		}
		if rErr != nil {
			errCh <- rErr
			return
		}
	}
}

func parseStartupParams(body []byte) map[string]string {
	params := make(map[string]string)
	parts := bytes.Split(body, []byte{0})
	for i := 0; i+1 < len(parts); i += 2 {
		k := string(parts[i])
		if k == "" {
			break
		}
		params[k] = string(parts[i+1])
	}
	return params
}

func parseSNITenant(sniHost string) string {
	sniHost = strings.TrimSpace(sniHost)
	if sniHost == "" {
		return ""
	}
	dot := strings.IndexByte(sniHost, '.')
	if dot <= 0 {
		return sniHost
	}
	return sniHost[:dot]
}

func extractTenant(sniHost string, params map[string]string) string {
	if db := strings.TrimSpace(params["database"]); db != "" && db != "postgres" {
		return db
	}
	return parseSNITenant(sniHost)
}

func buildStartupMessage(proto uint32, params map[string]string, targetDB string) []byte {
	var kv bytes.Buffer
	for k, v := range params {
		if k == "database" {
			continue
		}
		kv.WriteString(k)
		kv.WriteByte(0)
		kv.WriteString(v)
		kv.WriteByte(0)
	}
	kv.WriteString("database")
	kv.WriteByte(0)
	kv.WriteString(targetDB)
	kv.WriteByte(0)
	kv.WriteByte(0)

	totalLen := 8 + kv.Len()
	out := make([]byte, totalLen)
	binary.BigEndian.PutUint32(out[0:4], uint32(totalLen))
	binary.BigEndian.PutUint32(out[4:8], proto)
	copy(out[8:], kv.Bytes())
	return out
}

func writePGError(w io.Writer, sqlstate, msg string) error {
	var body bytes.Buffer
	body.WriteByte('S')
	body.WriteString("FATAL")
	body.WriteByte(0)
	body.WriteByte('C')
	body.WriteString(sqlstate)
	body.WriteByte(0)
	body.WriteByte('M')
	body.WriteString(msg)
	body.WriteByte(0)
	body.WriteByte(0)

	frame := make([]byte, 5+body.Len())
	frame[0] = 'E'
	binary.BigEndian.PutUint32(frame[1:5], uint32(4+body.Len()))
	copy(frame[5:], body.Bytes())
	_, err := w.Write(frame)
	return err
}

func generateSelfSignedTLSConfig() (*tls.Config, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "*.air.local"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"*.air.local", "localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{der},
			PrivateKey:  priv,
		}},
		MinVersion: tls.VersionTLS12,
	}, nil
}
