package wire

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"air-stage3/pkg/cell"
	"github.com/jackc/pgx/v5/pgproto3"
	"golang.org/x/sync/singleflight"
)

type TenantState string

const (
	StateActive     TenantState = "ACTIVE"
	StateDraining   TenantState = "DRAINING"
	StateStopped    TenantState = "STOPPED"
	StateDeepFrozen TenantState = "DEEP_FROZEN"
)

const (
	MaxWaitersPerTenant = 200
	WaitQueueTimeout    = 5 * time.Second
	PreAuthHeaderCap    = 8192
	PreAuthReadTimeout  = 5 * time.Second
	StreamChunkSize     = 32 * 1024
)

var (
	ErrDrainVetoed      = errors.New("drain vetoed: active txn, pinned session, or maintenance lock")
	ErrTooManyWaiters   = errors.New("53300: too many clients waiting for tenant")
	ErrSNISpoof         = errors.New("28000: SNI tenant mismatch")
	ErrWakeTimeout      = errors.New("57P03: tenant wake timeout")
)

var streamBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, StreamChunkSize)
		return &b
	},
}

var pinPattern = regexp.MustCompile(`(?i)\b(LISTEN|CREATE\s+TEMP|CREATE\s+TEMPORARY|DECLARE\s+|PREPARE\s+TRANSACTION|pg_advisory_lock)\b`)

func shouldPinSQL(sql string) bool {
	if pinPattern.MatchString(sql) {
		return true
	}
	trimmed := strings.TrimSpace(strings.ToUpper(sql))
	if strings.HasPrefix(trimmed, "SET ") && !strings.HasPrefix(trimmed, "SET LOCAL ") {
		// Allow harmless driver startup SETs like client_encoding / application_name?
		// For strict compatibility, pin on user session SETs that aren't standard startup params.
		if !strings.Contains(trimmed, "CLIENT_ENCODING") &&
			!strings.Contains(trimmed, "STANDARD_CONFORMING_STRINGS") &&
			!strings.Contains(trimmed, "DATESTYLE") {
			return true
		}
	}
	return false
}

func canonicalStmtName(query string, paramOIDs []uint32) string {
	h := sha256.New()
	h.Write([]byte(query))
	var buf [4]byte
	for _, oid := range paramOIDs {
		binary.BigEndian.PutUint32(buf[:], oid)
		h.Write(buf[:])
	}
	return "air_" + hex.EncodeToString(h.Sum(nil))[:16]
}

type stmtDef struct {
	Query         string
	ParameterOIDs []uint32
}

type backendConn struct {
	netConn     net.Conn
	fe          *pgproto3.Frontend
	pid         uint32
	secret      uint32
	params      map[string]string
	stmts       map[string]bool
	port        int
	createdAt   time.Time
	pumpStarted bool
	replyCh     chan pgproto3.BackendMessage
	pumpAckCh   chan struct{}
	pumpErrCh   chan error
}

func (b *backendConn) close() {
	if b != nil && b.netConn != nil {
		_ = b.netConn.Close()
	}
}

func (r *TenantRoute) ResetBackend(port int, state TenantState) {
	r.mu.Lock()
	oldPool := r.idlePool
	r.idlePool = nil
	r.Port = port
	r.State = state
	r.mu.Unlock()
	for _, bc := range oldPool {
		bc.close()
	}
}

type TenantRoute struct {
	mu              sync.Mutex
	TenantID        string
	State           TenantState
	Port            int
	Epoch           int64
	MaintenanceLock bool
	MaxPoolSize     int

	idlePool       []*backendConn
	activeBorrowed int
	pinnedCount    int
	waiters        int
	drainDoneCh    chan struct{}

	// Metrics
	WakeCount       atomic.Int64
	CoalescedWaiter atomic.Int64
	Rejected53300   atomic.Int64
}

type cancelTarget struct {
	tenantID string
	pgPID    uint32
	pgSecret uint32
	pgPort   int
}

// Gateway implements the Stage-3 pgproto3 session-state virtualizer,
// cooperative scale-to-zero drain, singleflight wakeup, and RU metering.
type Gateway struct {
	agent     *cell.Agent
	ledger    *MeteringLedger
	tlsConfig *tls.Config

	mu      sync.RWMutex
	routes  map[string]*TenantRoute
	wakeSF  singleflight.Group

	cancelMu     sync.RWMutex
	cancelMap    map[uint64]cancelTarget // key = (virtualPID<<32) | virtualSecret
	nextVirtPID  atomic.Uint32

	listener net.Listener
	wg       sync.WaitGroup
	quit     chan struct{}

	// Metrics
	ActiveConns atomic.Int64
	QuerySeq    atomic.Uint64
}

func NewGateway(agent *cell.Agent, ledger *MeteringLedger, tlsCfg *tls.Config) *Gateway {
	gw := &Gateway{
		agent:     agent,
		ledger:    ledger,
		tlsConfig: tlsCfg,
		routes:    make(map[string]*TenantRoute),
		cancelMap: make(map[uint64]cancelTarget),
		quit:      make(chan struct{}),
	}
	gw.nextVirtPID.Store(10000)
	return gw
}

func (g *Gateway) SetLedger(l *MeteringLedger) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ledger = l
}

func (g *Gateway) RegisterTenant(tenantID string, port int, epoch int64, state TenantState) *TenantRoute {
	g.mu.Lock()
	defer g.mu.Unlock()
	r := &TenantRoute{
		TenantID:    tenantID,
		State:       state,
		Port:        port,
		Epoch:       epoch,
		MaxPoolSize: 8,
	}
	g.routes[tenantID] = r
	return r
}

func (g *Gateway) GetRoute(tenantID string) (*TenantRoute, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	r, ok := g.routes[tenantID]
	return r, ok
}

func (g *Gateway) ListenAndServe(addr string) (string, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	g.listener = ln
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-g.quit:
					return
				default:
					continue
				}
			}
			g.wg.Add(1)
			go func(c net.Conn) {
				defer g.wg.Done()
				g.handleConn(c)
			}(conn)
		}
	}()
	return ln.Addr().String(), nil
}

func (g *Gateway) Close() error {
	close(g.quit)
	if g.listener != nil {
		_ = g.listener.Close()
	}
	g.mu.Lock()
	for _, r := range g.routes {
		r.mu.Lock()
		for _, bc := range r.idlePool {
			bc.close()
		}
		r.idlePool = nil
		r.mu.Unlock()
	}
	g.mu.Unlock()
	g.wg.Wait()
	return nil
}

// EnsureActive wakes a STOPPED/DRAINING tenant using singleflight coalescing.
// Exactly 1 PG17 boot occurs per sleeping DB regardless of concurrent caller count.
func (g *Gateway) EnsureActive(ctx context.Context, tenantID string) (*TenantRoute, error) {
	route, ok := g.GetRoute(tenantID)
	if !ok {
		return nil, fmt.Errorf("unknown tenant %s", tenantID)
	}

	for {
		route.mu.Lock()
		switch route.State {
		case StateActive:
			route.mu.Unlock()
			return route, nil
		case StateDraining:
			ch := route.drainDoneCh
			route.mu.Unlock()
			if ch != nil {
				select {
				case <-ch:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			// Loop around: once drain completes to STOPPED, singleflight wakes it!
			continue
		case StateStopped, StateDeepFrozen:
			if route.waiters >= MaxWaitersPerTenant {
				route.mu.Unlock()
				route.Rejected53300.Add(1)
				return nil, ErrTooManyWaiters
			}
			route.waiters++
			route.CoalescedWaiter.Add(1)
			route.mu.Unlock()

			waitCtx, cancel := context.WithTimeout(ctx, WaitQueueTimeout)
			_, err, _ := g.wakeSF.Do(tenantID, func() (any, error) {
				route.mu.Lock()
				if route.State == StateActive {
					route.mu.Unlock()
					return route, nil
				}
				epoch := route.Epoch
				route.mu.Unlock()

				rt, wakeErr := g.agent.WakeTenant(waitCtx, tenantID, epoch)
				if wakeErr != nil {
					return nil, wakeErr
				}
				route.WakeCount.Add(1)
				route.mu.Lock()
				for _, stale := range route.idlePool {
					stale.close()
				}
				route.idlePool = nil
				route.Port = rt.Port
				route.State = StateActive
				route.mu.Unlock()
				return route, nil
			})
			cancel()

			route.mu.Lock()
			route.waiters--
			route.mu.Unlock()

			if err != nil {
				return nil, err
			}
			return route, nil
		}
	}
}

// RequestDrain executes the Stage-3 Gateway-Controlled Cooperative Drain Protocol.
// Refuses drain if active transactions, pinned sessions (LISTEN/TEMP/2PC/advisory_lock),
// or maintenance_lock are active. Holds late inbound clients in memory queue (0 ECONNRESET).
func (g *Gateway) RequestDrain(ctx context.Context, tenantID string) (string, error) {
	route, ok := g.GetRoute(tenantID)
	if !ok {
		return "", fmt.Errorf("unknown tenant %s", tenantID)
	}

	for attempt := 0; attempt < 15; attempt++ {
		route.mu.Lock()
		if route.State != StateActive {
			st := string(route.State)
			route.mu.Unlock()
			return st, nil
		}
		if !route.MaintenanceLock && route.activeBorrowed == 0 && route.pinnedCount == 0 {
			break
		}
		route.mu.Unlock()
		if attempt == 14 {
			return "", ErrDrainVetoed
		}
		time.Sleep(10 * time.Millisecond)
	}
	route.State = StateDraining
	drainCh := make(chan struct{})
	route.drainDoneCh = drainCh
	idle := route.idlePool
	route.idlePool = nil
	route.mu.Unlock()

	defer func() {
		route.mu.Lock()
		close(drainCh)
		route.drainDoneCh = nil
		route.mu.Unlock()
	}()

	// Close idle pooled connections before pg_ctl stop
	for _, bc := range idle {
		bc.close()
	}

	// Re-check pg_prepared_xacts == 0 before stopping
	pgState, err := g.agent.StopTenantClean(tenantID, "smart")
	if err != nil {
		// Rollback drain to ACTIVE if stop failed
		route.mu.Lock()
		route.State = StateActive
		route.mu.Unlock()
		return "", err
	}

	route.mu.Lock()
	route.State = StateStopped
	route.mu.Unlock()
	return pgState, nil
}

func (g *Gateway) dialBackend(port int) (*backendConn, error) {
	c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 3*time.Second)
	if err != nil {
		return nil, err
	}
	fe := pgproto3.NewFrontend(c, c)
	fe.Send(&pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user":     "postgres",
			"database": "postgres",
		},
	})
	if err := fe.Flush(); err != nil {
		_ = c.Close()
		return nil, err
	}

	bc := &backendConn{
		netConn:   c,
		fe:        fe,
		params:    make(map[string]string),
		stmts:     make(map[string]bool),
		port:      port,
		createdAt: time.Now(),
	}

	for {
		msg, err := fe.Receive()
		if err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("backend startup receive: %w", err)
		}
		switch m := msg.(type) {
		case *pgproto3.AuthenticationOk:
		case *pgproto3.ParameterStatus:
			bc.params[m.Name] = m.Value
		case *pgproto3.BackendKeyData:
			bc.pid = m.ProcessID
			bc.secret = m.SecretKey
		case *pgproto3.ReadyForQuery:
			return bc, nil
		case *pgproto3.ErrorResponse:
			_ = c.Close()
			return nil, fmt.Errorf("backend startup error %s: %s", m.Code, m.Message)
		}
	}
}

func (g *Gateway) borrowBackend(ctx context.Context, route *TenantRoute) (*backendConn, error) {
	// Ensure tenant is active (wakes cleanly if stopped or draining)
	if _, err := g.EnsureActive(ctx, route.TenantID); err != nil {
		return nil, err
	}

	route.mu.Lock()
	for len(route.idlePool) > 0 {
		n := len(route.idlePool)
		bc := route.idlePool[n-1]
		route.idlePool = route.idlePool[:n-1]
		if bc.port != route.Port {
			bc.close()
			continue
		}
		route.activeBorrowed++
		route.mu.Unlock()
		return bc, nil
	}
	port := route.Port
	route.activeBorrowed++
	route.mu.Unlock()

	bc, err := g.dialBackend(port)
	if err != nil {
		route.mu.Lock()
		route.activeBorrowed--
		route.mu.Unlock()
		return nil, err
	}
	return bc, nil
}

func (g *Gateway) releaseBackend(route *TenantRoute, bc *backendConn, broken bool) {
	if bc == nil {
		return
	}
	route.mu.Lock()
	route.activeBorrowed--
	if broken || route.State != StateActive || len(route.idlePool) >= route.MaxPoolSize {
		route.mu.Unlock()
		bc.close()
		return
	}
	route.idlePool = append(route.idlePool, bc)
	route.mu.Unlock()
}

func (g *Gateway) registerCancel(vPID, vSecret uint32, tenantID string, pgPID, pgSecret uint32, pgPort int) {
	key := (uint64(vPID) << 32) | uint64(vSecret)
	g.cancelMu.Lock()
	g.cancelMap[key] = cancelTarget{
		tenantID: tenantID,
		pgPID:    pgPID,
		pgSecret: pgSecret,
		pgPort:   pgPort,
	}
	g.cancelMu.Unlock()
}

func (g *Gateway) unregisterCancel(vPID, vSecret uint32) {
	key := (uint64(vPID) << 32) | uint64(vSecret)
	g.cancelMu.Lock()
	delete(g.cancelMap, key)
	g.cancelMu.Unlock()
}

func (g *Gateway) forwardCancelRequest(cr *pgproto3.CancelRequest) {
	key := (uint64(cr.ProcessID) << 32) | uint64(cr.SecretKey)
	g.cancelMu.RLock()
	target, ok := g.cancelMap[key]
	g.cancelMu.RUnlock()
	if !ok || target.pgPort == 0 {
		return
	}
	c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", target.pgPort), 2*time.Second)
	if err != nil {
		return
	}
	defer c.Close()
	fe := pgproto3.NewFrontend(c, c)
	fe.Send(&pgproto3.CancelRequest{
		ProcessID: target.pgPID,
		SecretKey: target.pgSecret,
	})
	_ = fe.Flush()
}

func sendPGError(be *pgproto3.Backend, severity, code, msg string) {
	be.Send(&pgproto3.ErrorResponse{
		Severity: severity,
		Code:     code,
		Message:  msg,
	})
	_ = be.Flush()
}

func (g *Gateway) handleConn(rawConn net.Conn) {
	g.ActiveConns.Add(1)
	defer g.ActiveConns.Add(-1)
	defer rawConn.Close()

	// Pre-auth deadline and length cap (Slowloris & 2GB length-bomb protection)
	_ = rawConn.SetReadDeadline(time.Now().Add(PreAuthReadTimeout))
	br := bufio.NewReaderSize(io.LimitReader(rawConn, 64*1024), 4096)

	var sniHost string
	conn := rawConn

	// Peek first 8 bytes for SSLRequest (0x04d2162f = 80877103) or CancelRequest (80877102)
	var hdr [8]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return
	}
	pktLen := binary.BigEndian.Uint32(hdr[0:4])
	if pktLen > PreAuthHeaderCap || pktLen < 8 {
		return
	}
	protoCode := binary.BigEndian.Uint32(hdr[4:8])

	if protoCode == 80877103 { // SSLRequest
		if g.tlsConfig != nil {
			_, _ = conn.Write([]byte{'S'})
			tlsConn := tls.Server(conn, g.tlsConfig)
			_ = tlsConn.SetDeadline(time.Now().Add(PreAuthReadTimeout))
			if err := tlsConn.Handshake(); err != nil {
				return
			}
			_ = tlsConn.SetDeadline(time.Time{})
			sniHost = tlsConn.ConnectionState().ServerName
			conn = tlsConn
		} else {
			_, _ = conn.Write([]byte{'N'})
		}
		// Read actual StartupMessage or CancelRequest after SSL negotiation
		if _, err := io.ReadFull(conn, hdr[:]); err != nil {
			return
		}
		pktLen = binary.BigEndian.Uint32(hdr[0:4])
		if pktLen > PreAuthHeaderCap || pktLen < 8 {
			return
		}
		protoCode = binary.BigEndian.Uint32(hdr[4:8])
	}

	if protoCode == 80877102 { // CancelRequest
		var cancelBody [8]byte
		if _, err := io.ReadFull(conn, cancelBody[:]); err != nil {
			return
		}
		pid := binary.BigEndian.Uint32(cancelBody[0:4])
		secret := binary.BigEndian.Uint32(cancelBody[4:8])
		g.forwardCancelRequest(&pgproto3.CancelRequest{ProcessID: pid, SecretKey: secret})
		return
	}

	// Reconstruct StartupMessage bytes for pgproto3.Backend
	rest := make([]byte, pktLen-8)
	var startupReader io.Reader
	if g.tlsConfig != nil && sniHost != "" {
		startupReader = conn
	} else {
		startupReader = io.MultiReader(br, conn)
	}
	if _, err := io.ReadFull(startupReader, rest); err != nil {
		return
	}
	_ = conn.SetReadDeadline(time.Time{})

	fullStartup := make([]byte, pktLen)
	copy(fullStartup[0:4], hdr[0:4])
	copy(fullStartup[4:8], hdr[4:8])
	copy(fullStartup[8:], rest)

	be := pgproto3.NewBackend(io.MultiReader(
		// Feed the already-read StartupMessage first, then live conn
		&onceReader{data: fullStartup},
		conn,
	), conn)

	startupMsg, err := be.ReceiveStartupMessage()
	if err != nil {
		return
	}
	sm, ok := startupMsg.(*pgproto3.StartupMessage)
	if !ok {
		return
	}

	// Resolve tenant from database or options or SNI
	tenantID := sm.Parameters["database"]
	if t, exists := sm.Parameters["options"]; exists && strings.HasPrefix(t, "tenant=") {
		tenantID = strings.TrimPrefix(t, "tenant=")
	}
	if tenantID == "" || tenantID == "postgres" {
		// Default to first registered tenant if database=postgres without explicit tenant
		g.mu.RLock()
		for k := range g.routes {
			tenantID = k
			break
		}
		g.mu.RUnlock()
	}

	// Enforce SNI-to-tenant cryptographic binding if SNI was presented
	if sniHost != "" && !strings.HasPrefix(sniHost, tenantID+".") && sniHost != tenantID {
		sendPGError(be, "FATAL", "28000", fmt.Sprintf("SNI host %q does not match tenant %q", sniHost, tenantID))
		return
	}

	route, ok := g.GetRoute(tenantID)
	if !ok {
		sendPGError(be, "FATAL", "3D000", fmt.Sprintf("tenant database %q does not exist", tenantID))
		return
	}

	// Wake tenant (singleflight) & borrow initial backend to get ParameterStatus
	bc, err := g.borrowBackend(context.Background(), route)
	if err != nil {
		if errors.Is(err, ErrTooManyWaiters) {
			sendPGError(be, "FATAL", "53300", err.Error())
		} else {
			sendPGError(be, "FATAL", "57P03", fmt.Sprintf("database wake failed: %v", err))
		}
		return
	}

	vPID := g.nextVirtPID.Add(1)
	vSecret := uint32(time.Now().UnixNano() & 0xffffffff)
	defer g.unregisterCancel(vPID, vSecret)

	// Send startup completion to client
	be.Send(&pgproto3.AuthenticationOk{})
	for k, v := range bc.params {
		be.Send(&pgproto3.ParameterStatus{Name: k, Value: v})
	}
	be.Send(&pgproto3.BackendKeyData{ProcessID: vPID, SecretKey: vSecret})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := be.Flush(); err != nil {
		g.releaseBackend(route, bc, true)
		return
	}
	// Return initial backend to pool immediately so idle connections hold 0 PG backends!
	g.releaseBackend(route, bc, false)

	// Session state for this client
	clientStmts := make(map[string]stmtDef)
	pinned := false
	var pinnedBC *backendConn
	var activeBC *backendConn
	var stripQueue []string // "parse_complete" markers for injected Parses
	var beWriteMu sync.Mutex

	pumpDoneCh := make(chan struct{})
	defer close(pumpDoneCh)

	ensurePinnedPump := func(b *backendConn) {
		if b == nil || b.pumpStarted {
			return
		}
		b.pumpStarted = true
		b.replyCh = make(chan pgproto3.BackendMessage)
		b.pumpAckCh = make(chan struct{})
		b.pumpErrCh = make(chan error, 1)
		go func() {
			for {
				msg, err := b.fe.Receive()
				if err != nil {
					b.pumpErrCh <- err
					return
				}
				if nr, ok := msg.(*pgproto3.NotificationResponse); ok {
					beWriteMu.Lock()
					be.Send(nr)
					_ = be.Flush()
					beWriteMu.Unlock()
					continue
				}
				select {
				case b.replyCh <- msg:
				case <-pumpDoneCh:
					return
				}
				select {
				case <-b.pumpAckCh:
				case <-pumpDoneCh:
					return
				}
			}
		}()
	}

	defer func() {
		if pinnedBC != nil {
			route.mu.Lock()
			if route.pinnedCount > 0 {
				route.pinnedCount--
			}
			route.mu.Unlock()
			g.releaseBackend(route, pinnedBC, true) // close pinned backend on client disconnect
		} else if activeBC != nil {
			g.releaseBackend(route, activeBC, true)
		}
	}()

	ensureBorrowed := func() (*backendConn, error) {
		if pinned && pinnedBC != nil {
			return pinnedBC, nil
		}
		if activeBC != nil {
			return activeBC, nil
		}
		b, err := g.borrowBackend(context.Background(), route)
		if err != nil {
			return nil, err
		}
		g.registerCancel(vPID, vSecret, tenantID, b.pid, b.secret, b.port)
		if pinned {
			pinnedBC = b
		} else {
			activeBC = b
		}
		return b, nil
	}

	markPinned := func() {
		if !pinned {
			pinned = true
			route.mu.Lock()
			route.pinnedCount++
			route.mu.Unlock()
			if activeBC != nil {
				pinnedBC = activeBC
				activeBC = nil
			}
		}
	}

	flushBackendReplies := func(b *backendConn, waitForReady bool) (byte, int64, bool, error) {
		var txStatus byte = 'I'
		var bytesSent int64
		var broken bool
		ackPump := func() {
			if b.pumpStarted {
				select {
				case b.pumpAckCh <- struct{}{}:
				case <-pumpDoneCh:
				}
			}
		}
		for {
			var msg pgproto3.BackendMessage
			var err error
			if b.pumpStarted {
				select {
				case msg = <-b.replyCh:
				case err = <-b.pumpErrCh:
				}
			} else {
				msg, err = b.fe.Receive()
			}
			if err != nil {
				return 0, bytesSent, true, err
			}
			switch m := msg.(type) {
			case *pgproto3.ParseComplete:
				if len(stripQueue) > 0 && stripQueue[0] == "parse_complete" {
					stripQueue = stripQueue[1:]
					ackPump()
					continue
				}
				beWriteMu.Lock()
				be.Send(m)
				beWriteMu.Unlock()
			case *pgproto3.ErrorResponse:
				if m.Code == "0A000" || m.Code == "26000" {
					b.stmts = make(map[string]bool)
				}
				beWriteMu.Lock()
				be.Send(m)
				beWriteMu.Unlock()
				bytesSent += int64(len(m.Message))
			case *pgproto3.DataRow:
				for _, col := range m.Values {
					bytesSent += int64(len(col))
				}
				beWriteMu.Lock()
				be.Send(m)
				beWriteMu.Unlock()
			case *pgproto3.CopyData:
				bytesSent += int64(len(m.Data))
				beWriteMu.Lock()
				be.Send(m)
				err = be.Flush()
				beWriteMu.Unlock()
				ackPump()
				if err != nil {
					return 0, bytesSent, true, err
				}
				continue
			case *pgproto3.ReadyForQuery:
				txStatus = m.TxStatus
				beWriteMu.Lock()
				be.Send(m)
				err = be.Flush()
				beWriteMu.Unlock()
				ackPump()
				if err != nil {
					return txStatus, bytesSent, true, err
				}
				if pinned && b == pinnedBC {
					ensurePinnedPump(pinnedBC)
				}
				return txStatus, bytesSent, broken, nil
			default:
				beWriteMu.Lock()
				be.Send(m)
				beWriteMu.Unlock()
			}
			ackPump()
			if !waitForReady {
				beWriteMu.Lock()
				err = be.Flush()
				beWriteMu.Unlock()
				if err != nil {
					return txStatus, bytesSent, true, err
				}
				return txStatus, bytesSent, broken, nil
			}
		}
	}

	for {
		cMsg, err := be.Receive()
		if err != nil {
			return
		}
		switch m := cMsg.(type) {
		case *pgproto3.Terminate:
			return

		case *pgproto3.Query:
			if shouldPinSQL(m.String) {
				markPinned()
			}
			b, err := ensureBorrowed()
			if err != nil {
				if errors.Is(err, ErrTooManyWaiters) {
					sendPGError(be, "ERROR", "53300", err.Error())
				} else {
					sendPGError(be, "ERROR", "57P03", err.Error())
				}
				be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				_ = be.Flush()
				continue
			}
			b.fe.Send(m)
			if err := b.fe.Flush(); err != nil {
				g.releaseBackend(route, b, true)
				activeBC = nil
				return
			}
			txStatus, bytesSent, broken, err := flushBackendReplies(b, true)
			if g.ledger != nil {
				seq := g.QuerySeq.Add(1)
				_, _ = g.ledger.AppendWindow(tenantID, "w1", seq, bytesSent)
			}
			if !pinned && txStatus == 'I' {
				g.releaseBackend(route, b, broken)
				activeBC = nil
			}
			if err != nil {
				return
			}

		case *pgproto3.Parse:
			if shouldPinSQL(m.Query) {
				markPinned()
			}
			clientStmts[m.Name] = stmtDef{
				Query:         m.Query,
				ParameterOIDs: append([]uint32(nil), m.ParameterOIDs...),
			}
			b, err := ensureBorrowed()
			if err != nil {
				sendPGError(be, "ERROR", "53300", err.Error())
				continue
			}
			targetName := m.Name
			if m.Name != "" {
				targetName = canonicalStmtName(m.Query, m.ParameterOIDs)
			}
			if m.Name != "" && b.stmts[targetName] {
				// Already prepared on this backend; emit ParseComplete directly without round-trip!
				be.Send(&pgproto3.ParseComplete{})
			} else {
				b.fe.Send(&pgproto3.Parse{
					Name:          targetName,
					Query:         m.Query,
					ParameterOIDs: m.ParameterOIDs,
				})
				if m.Name != "" {
					b.stmts[targetName] = true
				}
			}

		case *pgproto3.Bind:
			b, err := ensureBorrowed()
			if err != nil {
				sendPGError(be, "ERROR", "53300", err.Error())
				continue
			}
			targetName := m.PreparedStatement
			if m.PreparedStatement != "" {
				if def, ok := clientStmts[m.PreparedStatement]; ok {
					targetName = canonicalStmtName(def.Query, def.ParameterOIDs)
					if !b.stmts[targetName] {
						b.fe.Send(&pgproto3.Parse{
							Name:          targetName,
							Query:         def.Query,
							ParameterOIDs: def.ParameterOIDs,
						})
						b.stmts[targetName] = true
						stripQueue = append(stripQueue, "parse_complete")
					}
				}
			}
			b.fe.Send(&pgproto3.Bind{
				DestinationPortal:    m.DestinationPortal,
				PreparedStatement:    targetName,
				ParameterFormatCodes: m.ParameterFormatCodes,
				Parameters:           m.Parameters,
				ResultFormatCodes:    m.ResultFormatCodes,
			})

		case *pgproto3.Describe:
			b, err := ensureBorrowed()
			if err != nil {
				sendPGError(be, "ERROR", "53300", err.Error())
				continue
			}
			targetName := m.Name
			if m.ObjectType == 'S' && m.Name != "" {
				if def, ok := clientStmts[m.Name]; ok {
					targetName = canonicalStmtName(def.Query, def.ParameterOIDs)
					if !b.stmts[targetName] {
						b.fe.Send(&pgproto3.Parse{
							Name:          targetName,
							Query:         def.Query,
							ParameterOIDs: def.ParameterOIDs,
						})
						b.stmts[targetName] = true
						stripQueue = append(stripQueue, "parse_complete")
					}
				}
			}
			b.fe.Send(&pgproto3.Describe{
				ObjectType: m.ObjectType,
				Name:       targetName,
			})

		case *pgproto3.Execute:
			b, err := ensureBorrowed()
			if err != nil {
				continue
			}
			b.fe.Send(m)

		case *pgproto3.Close:
			if m.ObjectType == 'S' && m.Name != "" {
				delete(clientStmts, m.Name)
				be.Send(&pgproto3.CloseComplete{})
			} else if b, err := ensureBorrowed(); err == nil {
				b.fe.Send(m)
			}

		case *pgproto3.Flush:
			b, err := ensureBorrowed()
			if err != nil {
				continue
			}
			b.fe.Send(m)
			_ = b.fe.Flush()

		case *pgproto3.Sync:
			b, err := ensureBorrowed()
			if err != nil {
				sendPGError(be, "ERROR", "53300", err.Error())
				be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				_ = be.Flush()
				continue
			}
			b.fe.Send(m)
			if err := b.fe.Flush(); err != nil {
				g.releaseBackend(route, b, true)
				activeBC = nil
				return
			}
			txStatus, bytesSent, broken, err := flushBackendReplies(b, true)
			if g.ledger != nil {
				seq := g.QuerySeq.Add(1)
				_, _ = g.ledger.AppendWindow(tenantID, "w1", seq, bytesSent)
			}
			if !pinned && txStatus == 'I' {
				g.releaseBackend(route, b, broken)
				activeBC = nil
			}
			if err != nil {
				return
			}
		}
	}
}

type onceReader struct {
	data []byte
	pos  int
}

func (r *onceReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
