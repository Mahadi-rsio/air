package autoscaler

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"
)

const pgSSLRequestCode = 80877103

// GatewayMetrics holds the connection counts and configured limits read from /internal/v1/metrics.
type GatewayMetrics struct {
	Limits struct {
		MaxClientConns       int `json:"max_client_conns"`
		MaxTenantClientConns int `json:"max_tenant_client_conns"`
		MaxUserClientConns   int `json:"max_user_client_conns"`
	} `json:"limits"`
	Global struct {
		ActiveConnections int64 `json:"active_connections"`
	} `json:"global"`
}

// ReadinessCheck mirrors gateway.ReadinessCheck.
type ReadinessCheck struct {
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

// ReadinessReport mirrors gateway.ReadinessReport.
type ReadinessReport struct {
	Ready     bool                      `json:"ready"`
	Status    string                    `json:"status"`
	GatewayID string                    `json:"gateway_id"`
	Cordoned  bool                      `json:"cordoned"`
	Draining  bool                      `json:"draining"`
	Checks    map[string]ReadinessCheck `json:"checks"`
}

// MeteringStatus mirrors gateway.MeteringStatus.
type MeteringStatus struct {
	GatewayID          string `json:"gateway_id"`
	BillingEnabled     bool   `json:"billing_enabled"`
	Writable           bool   `json:"writable"`
	SinkConfigured     bool   `json:"sink_configured"`
	SinkHealthy        bool   `json:"sink_healthy"`
	PendingBatches     int    `json:"pending_batches"`
	QuarantinedCount   int    `json:"quarantined_count"`
	LegacyPendingCount int    `json:"legacy_pending_count"`
	SpoolFull          bool   `json:"spool_full"`
	Blocked            bool   `json:"blocked"`
	LastError          string `json:"last_error,omitempty"`
}

// DrainResult mirrors gateway.DrainInstanceResult.
type DrainResult struct {
	OpID               string `json:"op_id"`
	GatewayID          string `json:"gateway_id"`
	Drained            bool   `json:"drained"`
	MeteringConfirmed  bool   `json:"metering_confirmed"`
	ActiveClients      int64  `json:"active_clients"`
	ActiveBackends     int    `json:"active_backends"`
	PendingBatches     int    `json:"pending_batches"`
	QuarantinedCount   int    `json:"quarantined_count"`
	LegacyPendingCount int    `json:"legacy_pending_count"`
	Blocked            bool   `json:"blocked"`
	Error              string `json:"error,omitempty"`
}

// GatewayProber abstracts private 6PN HTTP and PostgreSQL TLS probes against gateway Machines.
type GatewayProber interface {
	FetchMetrics(ctx context.Context, m Machine) (*GatewayMetrics, error)
	CheckReadiness(ctx context.Context, m Machine) (*ReadinessReport, error)
	CheckMeteringStatus(ctx context.Context, m Machine) (*MeteringStatus, error)
	VerifyPostgresTLS(ctx context.Context, m Machine) error
	Cordon(ctx context.Context, m Machine) error
	Uncordon(ctx context.Context, m Machine) error
	Drain(ctx context.Context, m Machine, opID string, timeout time.Duration) (*DrainResult, error)
}

// NetGatewayProber implements GatewayProber over Fly 6PN private IPv6 networking.
type NetGatewayProber struct {
	internalToken   string
	privateHTTPPort int
	pgPort          int
	tlsServerName   string
	rootCAs         *x509.CertPool
	httpClient      *http.Client
}

// LoadCertPoolFromPEM reads a PEM CA bundle file into an *x509.CertPool.
func LoadCertPoolFromPEM(path string) (*x509.CertPool, error) {
	if path == "" {
		return nil, nil
	}
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read CA PEM %s: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("no valid certificates found in %s", path)
	}
	return pool, nil
}

// NewNetGatewayProber constructs a NetGatewayProber for 6PN gateway communication.
func NewNetGatewayProber(internalToken string, privateHTTPPort, pgPort int, tlsServerName string, rootCAs *x509.CertPool) *NetGatewayProber {
	return &NetGatewayProber{
		internalToken:   internalToken,
		privateHTTPPort: privateHTTPPort,
		pgPort:          pgPort,
		tlsServerName:   tlsServerName,
		rootCAs:         rootCAs,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (p *NetGatewayProber) httpBase(m Machine) (string, error) {
	if m.PrivateIP == "" {
		return "", fmt.Errorf("machine %s has no private_ip", m.ID)
	}
	return "http://" + net.JoinHostPort(m.PrivateIP, strconv.Itoa(p.privateHTTPPort)), nil
}

func (p *NetGatewayProber) doHTTP(ctx context.Context, m Machine, method, path string, reqBody any, out any) (int, error) {
	base, err := p.httpBase(m)
	if err != nil {
		return 0, err
	}
	var bodyReader io.Reader
	if reqBody != nil {
		b, err := json.Marshal(reqBody)
		if err != nil {
			return 0, err
		}
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, bodyReader)
	if err != nil {
		return 0, err
	}
	if p.internalToken != "" {
		req.Header.Set("X-Air-Internal-Token", p.internalToken)
		req.Header.Set("Authorization", "Bearer "+p.internalToken)
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return resp.StatusCode, err
	}
	if out != nil && len(respBytes) > 0 {
		_ = json.Unmarshal(respBytes, out)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("gateway %s %s %s returned %d: %s", m.ID, method, path, resp.StatusCode, string(respBytes))
	}
	return resp.StatusCode, nil
}

func (p *NetGatewayProber) FetchMetrics(ctx context.Context, m Machine) (*GatewayMetrics, error) {
	var out GatewayMetrics
	if _, err := p.doHTTP(ctx, m, http.MethodGet, "/internal/v1/metrics", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (p *NetGatewayProber) CheckReadiness(ctx context.Context, m Machine) (*ReadinessReport, error) {
	var out ReadinessReport
	// /readyz may return 503 when cordoned while still providing valid JSON checks.
	_, err := p.doHTTP(ctx, m, http.MethodGet, "/readyz", nil, &out)
	if out.Status != "" || len(out.Checks) > 0 {
		return &out, nil
	}
	return nil, err
}

func (p *NetGatewayProber) CheckMeteringStatus(ctx context.Context, m Machine) (*MeteringStatus, error) {
	var out MeteringStatus
	if _, err := p.doHTTP(ctx, m, http.MethodGet, "/internal/v1/metering/status", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (p *NetGatewayProber) Cordon(ctx context.Context, m Machine) error {
	_, err := p.doHTTP(ctx, m, http.MethodPost, "/internal/v1/cordon", nil, nil)
	return err
}

func (p *NetGatewayProber) Uncordon(ctx context.Context, m Machine) error {
	_, err := p.doHTTP(ctx, m, http.MethodPost, "/internal/v1/uncordon", nil, nil)
	return err
}

func (p *NetGatewayProber) Drain(ctx context.Context, m Machine, opID string, timeout time.Duration) (*DrainResult, error) {
	client := &http.Client{
		Timeout: timeout + 10*time.Second,
	}
	base, err := p.httpBase(m)
	if err != nil {
		return nil, err
	}
	payload := map[string]any{
		"op_id":      opID,
		"timeout_ms": int(timeout.Milliseconds()),
	}
	b, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/internal/v1/drain", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.internalToken != "" {
		req.Header.Set("X-Air-Internal-Token", p.internalToken)
		req.Header.Set("Authorization", "Bearer "+p.internalToken)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out DrainResult
	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if len(respBytes) > 0 {
		_ = json.Unmarshal(respBytes, &out)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &out, fmt.Errorf("drain failed (%d): %s", resp.StatusCode, string(respBytes))
	}
	return &out, nil
}

// VerifyPostgresTLS dials the gateway's PostgreSQL port over 6PN, sends SSLRequest,
// and performs full TLS certificate and hostname validation.
func (p *NetGatewayProber) VerifyPostgresTLS(ctx context.Context, m Machine) error {
	if m.PrivateIP == "" {
		return fmt.Errorf("machine %s has no private_ip", m.ID)
	}
	if p.tlsServerName == "" {
		return fmt.Errorf("gateway TLS server name is required for hostname validation")
	}
	addr := net.JoinHostPort(m.PrivateIP, strconv.Itoa(p.pgPort))
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("dial postgres %s: %w", addr, err)
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	}

	var sslReq [8]byte
	binary.BigEndian.PutUint32(sslReq[0:4], 8)
	binary.BigEndian.PutUint32(sslReq[4:8], pgSSLRequestCode)
	if _, err := conn.Write(sslReq[:]); err != nil {
		return fmt.Errorf("send SSLRequest to %s: %w", addr, err)
	}

	var reply [1]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		return fmt.Errorf("read SSLRequest reply from %s: %w", addr, err)
	}
	if reply[0] != 'S' {
		return fmt.Errorf("postgres server %s declined SSLRequest (%q)", addr, reply[0])
	}

	tlsCfg := &tls.Config{
		ServerName:         p.tlsServerName,
		RootCAs:            p.rootCAs,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: false,
	}
	tlsConn := tls.Client(conn, tlsCfg)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return fmt.Errorf("postgres TLS handshake/verification failed for %s (server_name=%s): %w", addr, p.tlsServerName, err)
	}
	if err := tlsConn.VerifyHostname(p.tlsServerName); err != nil {
		return fmt.Errorf("postgres TLS hostname verification failed for %s: %w", p.tlsServerName, err)
	}
	_ = tlsConn.Close()
	return nil
}
