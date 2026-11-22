package gateway

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PreparedDef holds metadata for a client-prepared statement (SQL PREPARE or Extended Query Parse),
// mapped from the client-local name to a globally unique backend statement name.
type PreparedDef struct {
	ClientName string
	GlobalName string
	TypeList   string
	Query      string
	ParamOIDs  []uint32
	IsSQLPrep  bool
}

// BackendConn represents a pooled PostgreSQL backend connection and its prepared-statement set.
type BackendConn struct {
	Conn          net.Conn
	BackendPID    uint32
	BackendSecret uint32
	LastClientID  uint64
	LastUsed      time.Time
	PreparedStmts map[string]struct{}
}

func (b *BackendConn) Close() {
	if b.Conn != nil {
		_ = b.Conn.Close()
		b.Conn = nil
	}
}

func dialBackendSocket(host, sockDir string, port int) (net.Conn, error) {
	isLocal := host == "" || host == "127.0.0.1" || host == "localhost"
	if sockDir != "" && isLocal {
		sockPath := filepath.Join(sockDir, fmt.Sprintf(".s.PGSQL.%d", port))
		if conn, err := net.DialTimeout("unix", sockPath, 5*time.Second); err == nil {
			return conn, nil
		}
	}
	targetHost := host
	if targetHost == "" {
		targetHost = "127.0.0.1"
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(targetHost, strconv.Itoa(port)), 5*time.Second)
	if err != nil {
		return nil, err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return conn, nil
}

type WakeFn func() (TenantRoute, error)

type relayStats struct {
	bytesOut     uint64
	rowsReturned uint64
	dataRowBytes uint64
}

type clientSessionState struct {
	client          net.Conn
	startupUser     string
	clientID        uint64
	gwPID           uint32
	gwSecret        uint32
	metrics         *MetricsRegistry
	cancelReg       *CancelRegistry
	sessionVars     map[string]string
	clientStmts     map[string]PreparedDef
	currentBackend  *BackendConn
	txStatus        byte
	inTxCounted     bool
	sessionPinned   bool
	backendInFlight bool
	sessionWriteLSN uint64
	touchActivity   func()
}

// TenantPool is a per-tenant transaction-mode PostgreSQL connection pooler.
type TenantPool struct {
	tenantID       string
	maxBackends    int
	idleTimeoutSec atomic.Int64

	mu             sync.Mutex
	host           string
	sockDir        string
	port           int
	appPassword    string
	suspended      bool
	idlePool       []*BackendConn
	readHost       string
	readSockDir    string
	readPort       int
	readIdlePool   []*BackendConn
	routingMetrics RoutingMetrics
	wakeFn         WakeFn
	metering       *MeteringLogger

	semMu          sync.Mutex
	semCond        *sync.Cond
	activeBackends int

	stmtSeq           atomic.Uint64
	clientSeq         atomic.Uint64
	pinnedClients     atomic.Int64
	activeTxCount     atomic.Int64
	readEngineLSN     atomic.Uint64
	readEngineHealthy atomic.Bool
}

func NewTenantPool(tenantID, host, sockDir string, port int, appPassword string, maxBackends, idleTimeoutSec int) *TenantPool {
	if maxBackends <= 0 {
		maxBackends = 16
	}
	if idleTimeoutSec <= 0 {
		idleTimeoutSec = 300
	}
	p := &TenantPool{
		tenantID:    tenantID,
		maxBackends: maxBackends,
		host:        host,
		sockDir:     sockDir,
		port:        port,
		appPassword: appPassword,
	}
	p.idleTimeoutSec.Store(int64(idleTimeoutSec))
	p.readEngineHealthy.Store(true)
	p.semCond = sync.NewCond(&p.semMu)
	return p
}

func (p *TenantPool) SetIdleTimeoutSec(sec int) {
	if sec > 0 {
		p.idleTimeoutSec.Store(int64(sec))
	}
}

func (p *TenantPool) EvictExpiredIdle() int {
	now := time.Now()
	timeout := time.Duration(p.idleTimeoutSec.Load()) * time.Second
	var toClose []*BackendConn

	p.mu.Lock()
	kept := p.idlePool[:0]
	for _, b := range p.idlePool {
		if b == nil || now.Sub(b.LastUsed) >= timeout {
			toClose = append(toClose, b)
		} else {
			kept = append(kept, b)
		}
	}
	p.idlePool = kept

	readKept := p.readIdlePool[:0]
	for _, b := range p.readIdlePool {
		if b == nil || now.Sub(b.LastUsed) >= timeout {
			toClose = append(toClose, b)
		} else {
			readKept = append(readKept, b)
		}
	}
	p.readIdlePool = readKept
	p.mu.Unlock()

	for _, b := range toClose {
		if b != nil {
			b.Close()
		}
	}
	return len(toClose)
}

func (p *TenantPool) SetWakeFn(fn WakeFn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.wakeFn = fn
}

func (p *TenantPool) SetMeteringLogger(m *MeteringLogger) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.metering = m
}

func (p *TenantPool) UpdateEndpoint(host, sockDir string, port int, appPassword string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if appPassword != "" {
		p.appPassword = appPassword
	}
	if port > 0 {
		p.suspended = false
		if p.host != host || p.port != port || p.sockDir != sockDir {
			for _, b := range p.idlePool {
				if b != nil {
					b.Close()
				}
			}
			p.idlePool = nil
			p.host = host
			p.sockDir = sockDir
			p.port = port
		}
	}
}

// MarkSuspended marks the primary PostgreSQL backend as suspended and closes idle
// write backends while keeping read-engine idle connections intact.
func (p *TenantPool) MarkSuspended() {
	p.mu.Lock()
	p.suspended = true
	idle := p.idlePool
	p.idlePool = nil
	p.mu.Unlock()

	for _, b := range idle {
		if b != nil {
			b.Close()
		}
	}
}

func (p *TenantPool) SetReadEngineEndpoint(host, sockDir string, port int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.readHost != host || p.readSockDir != sockDir || p.readPort != port {
		for _, b := range p.readIdlePool {
			if b != nil {
				b.Close()
			}
		}
		p.readIdlePool = nil
		p.readHost = host
		p.readSockDir = sockDir
		p.readPort = port
	}
}

func (p *TenantPool) SetMaxBackends(maxBackends int) {
	if maxBackends <= 0 {
		return
	}
	p.semMu.Lock()
	p.maxBackends = maxBackends
	p.semMu.Unlock()
	p.semCond.Broadcast()
}

func (p *TenantPool) Snapshot() PoolTenantSnapshot {
	p.semMu.Lock()
	active := p.activeBackends
	maxB := p.maxBackends
	p.semMu.Unlock()

	p.mu.Lock()
	idle := len(p.idlePool)
	p.mu.Unlock()

	return PoolTenantSnapshot{
		TenantID:           p.tenantID,
		ActiveBackends:     active,
		MaxBackends:        maxB,
		IdleBackends:       idle,
		PinnedClients:      p.pinnedClients.Load(),
		ActiveTransactions: p.activeTxCount.Load(),
	}
}

func (p *TenantPool) CloseAllIdle() {
	p.mu.Lock()
	idle := p.idlePool
	p.idlePool = nil
	readIdle := p.readIdlePool
	p.readIdlePool = nil
	p.mu.Unlock()

	for _, b := range idle {
		if b != nil {
			b.Close()
		}
	}
	for _, b := range readIdle {
		if b != nil {
			b.Close()
		}
	}
}

func (p *TenantPool) RequestDrain(timeout time.Duration) error {
	if n := p.pinnedClients.Load(); n > 0 {
		return fmt.Errorf("drain vetoed: %d pinned client session(s)", n)
	}
	deadline := time.Now().Add(timeout)
	for p.activeTxCount.Load() > 0 {
		if time.Now().After(deadline) {
			return fmt.Errorf("drain vetoed: %d active transaction(s) exceeded drain timeout", p.activeTxCount.Load())
		}
		time.Sleep(15 * time.Millisecond)
	}
	if n := p.pinnedClients.Load(); n > 0 {
		return fmt.Errorf("drain vetoed: %d pinned client session(s)", n)
	}
	p.CloseAllIdle()
	return nil
}

func (p *TenantPool) authenticateClient(client net.Conn, startupUser, expectedPass string, gwPID, gwSecret uint32, metrics *MetricsRegistry) bool {
	if expectedPass == "" {
		if metrics != nil {
			metrics.RecordAuthFailure()
		}
		_, _ = client.Write(BuildErrorResponse("FATAL", "28000", "tenant credentials unavailable"))
		return false
	}

	if _, err := client.Write(BuildAuthCleartext()); err != nil {
		return false
	}

	mtype, mbody, err := ReadPGPacket(client)
	if err != nil || mtype != 'p' {
		if metrics != nil {
			metrics.RecordAuthFailure()
		}
		_, _ = client.Write(BuildErrorResponse("FATAL", "28P01", "password authentication failed"))
		return false
	}
	off := 0
	gotPass := ReadCStr(mbody, &off)
	if !ConstantTimeEquals(gotPass, expectedPass) {
		if metrics != nil {
			metrics.RecordAuthFailure()
		}
		_, _ = client.Write(BuildErrorResponse("FATAL", "28P01", fmt.Sprintf("password authentication failed for user %q", startupUser)))
		return false
	}

	msgs := [][]byte{
		BuildAuthOK(),
		BuildParamStatus("server_version", "17.12"),
		BuildParamStatus("client_encoding", "UTF8"),
		BuildParamStatus("DateStyle", "ISO, MDY"),
		BuildParamStatus("standard_conforming_strings", "on"),
		BuildBackendKeyData(gwPID, gwSecret),
		BuildReadyForQuery('I'),
	}
	for _, msg := range msgs {
		if _, err := client.Write(msg); err != nil {
			return false
		}
	}
	return true
}

func (p *TenantPool) releaseCurrent(st *clientSessionState, dirty bool) {
	if st.cancelReg != nil {
		st.cancelReg.Unregister(st.gwPID, st.gwSecret)
	}
	st.backendInFlight = false
	if st.currentBackend != nil {
		p.releaseBackend(st.currentBackend, dirty)
		st.currentBackend = nil
	}
}

func (p *TenantPool) updateTxCount(st *clientSessionState) {
	if st.txStatus != 'I' && !st.inTxCounted {
		st.inTxCounted = true
		p.activeTxCount.Add(1)
	} else if st.txStatus == 'I' && st.inTxCounted {
		st.inTxCounted = false
		p.activeTxCount.Add(-1)
	}
}

func (p *TenantPool) recordQueryMetrics(st *clientSessionState, qStart time.Time, poolWaitUs, bytesIn uint64, rstats relayStats, hadErr bool, count uint64, queryType string) {
	durUs := uint64(time.Since(qStart).Microseconds())
	if st.metrics != nil {
		st.metrics.RecordQuery(p.tenantID, st.startupUser, durUs, poolWaitUs, bytesIn, rstats.bytesOut, rstats.rowsReturned, rstats.dataRowBytes, hadErr, count)
	}
	p.mu.Lock()
	m := p.metering
	p.mu.Unlock()
	if m != nil {
		m.LogQuery(MeteringEntry{
			Date:           qStart.UTC(),
			DBID:           p.tenantID,
			BandwidthBytes: bytesIn + rstats.bytesOut,
			QueryTimeUs:    durUs,
			QueryType:      queryType,
		})
	}
}

func (p *TenantPool) ensureSessionAndStmts(b *BackendConn, st *clientSessionState) bool {
	if len(st.sessionVars) > 0 {
		var sql strings.Builder
		for k, v := range st.sessionVars {
			fmt.Fprintf(&sql, "SET %s = %s; ", k, v)
		}
		if !execSimpleBackend(b, sql.String()) {
			return false
		}
	}
	for _, def := range st.clientStmts {
		if _, ok := b.PreparedStmts[def.GlobalName]; ok {
			continue
		}
		if def.IsSQLPrep {
			prepSQL := fmt.Sprintf("PREPARE %s%s AS %s", def.GlobalName, def.TypeList, def.Query)
			if !execSimpleBackend(b, prepSQL) {
				return false
			}
		} else {
			if _, err := b.Conn.Write(BuildParseMessage(def.GlobalName, def.Query, def.ParamOIDs)); err != nil {
				return false
			}
			if _, err := b.Conn.Write(BuildSyncMessage()); err != nil {
				return false
			}
			if !drainUntilReadyBackend(b) {
				return false
			}
		}
		b.PreparedStmts[def.GlobalName] = struct{}{}
	}
	return true
}

func (p *TenantPool) ensureBackend(st *clientSessionState) (uint64, bool) {
	if st.currentBackend != nil {
		if st.touchActivity != nil {
			st.touchActivity()
		}
		return 0, true
	}
	waitStart := time.Now()
	b, err := p.acquireBackend(st.clientID)
	poolWaitUs := uint64(time.Since(waitStart).Microseconds())
	if err != nil {
		if st.metrics != nil {
			st.metrics.RecordPoolExhausted()
		}
		_, _ = st.client.Write(BuildErrorResponse("ERROR", "53300", err.Error()))
		_, _ = st.client.Write(BuildReadyForQuery(st.txStatus))
		return poolWaitUs, false
	}
	if st.touchActivity != nil {
		st.touchActivity()
	}
	st.currentBackend = b
	if !p.ensureSessionAndStmts(b, st) {
		p.releaseCurrent(st, true)
		_, _ = st.client.Write(BuildErrorResponse("ERROR", "08006", "failed to prepare pooled backend session state"))
		_, _ = st.client.Write(BuildReadyForQuery(st.txStatus))
		return poolWaitUs, false
	}
	if st.cancelReg != nil && b.BackendPID != 0 {
		p.mu.Lock()
		host, sockDir, port := p.host, p.sockDir, p.port
		p.mu.Unlock()
		st.cancelReg.Register(st.gwPID, st.gwSecret, CancelTarget{
			Host:          host,
			SockDir:       sockDir,
			Port:          port,
			BackendPID:    b.BackendPID,
			BackendSecret: b.BackendSecret,
		})
	}
	return poolWaitUs, true
}

func (p *TenantPool) executeSimpleAndRelay(st *clientSessionState, wireMsg []byte, bytesIn uint64, qStart time.Time, queryType string) (hadErr bool, ioOK bool, ok bool) {
	ioOK = true
	poolWaitUs, acquired := p.ensureBackend(st)
	if !acquired {
		p.recordQueryMetrics(st, qStart, poolWaitUs, bytesIn, relayStats{}, true, 1, queryType)
		return true, true, false
	}
	st.backendInFlight = true
	if _, err := st.currentBackend.Conn.Write(wireMsg); err != nil {
		p.releaseCurrent(st, true)
		return false, false, false
	}
	rstats, txStatus, hadErr, relayOK := relayUntilReady(st.currentBackend, st.client)
	if !relayOK {
		p.releaseCurrent(st, true)
		return false, false, false
	}
	st.txStatus = txStatus
	st.backendInFlight = false
	p.recordQueryMetrics(st, qStart, poolWaitUs, bytesIn, rstats, hadErr, 1, queryType)
	p.updateTxCount(st)
	return hadErr, true, true
}

func (p *TenantPool) tryExecuteOnReadEngine(st *clientSessionState, wireMsg []byte, bytesIn uint64, qStart time.Time, queryType string) (executed bool, ioOK bool) {
	ioOK = true
	waitStart := time.Now()
	rb, err := p.acquireReadBackend(st.clientID)
	poolWaitUs := uint64(time.Since(waitStart).Microseconds())
	if err != nil || rb == nil {
		log.Printf("[air-gateway] read-engine acquire failed tenant=%s err=%v (falling back to primary)", p.tenantID, err)
		return false, true
	}
	if !p.ensureSessionAndStmts(rb, st) {
		p.releaseReadBackend(rb, true)
		return false, true
	}
	if st.cancelReg != nil && rb.BackendPID != 0 {
		p.mu.Lock()
		rhost, rsock, rport := p.readHost, p.readSockDir, p.readPort
		p.mu.Unlock()
		st.cancelReg.Register(st.gwPID, st.gwSecret, CancelTarget{
			Host:          rhost,
			SockDir:       rsock,
			Port:          rport,
			BackendPID:    rb.BackendPID,
			BackendSecret: rb.BackendSecret,
		})
		defer st.cancelReg.Unregister(st.gwPID, st.gwSecret)
	}
	if _, err := rb.Conn.Write(wireMsg); err != nil {
		p.releaseReadBackend(rb, true)
		return false, true
	}
	firstT, firstBody, err := ReadPGPacket(rb.Conn)
	if err != nil {
		p.releaseReadBackend(rb, true)
		return false, true
	}
	if firstT == 'E' {
		log.Printf("[air-gateway] read-engine returned error for tenant=%s (falling back to primary)", p.tenantID)
		// ReadEngine cannot serve this query (e.g. TOAST, MultiXact, unsupported type/SQL).
		// Drain until ReadyForQuery ('Z') and safely fall back to PostgreSQL primary.
		txStatus := byte('I')
		for {
			t, b, rerr := ReadPGPacket(rb.Conn)
			if rerr != nil {
				p.releaseReadBackend(rb, true)
				return false, true
			}
			if t == 'Z' {
				if len(b) > 0 {
					txStatus = b[0]
				}
				break
			}
		}
		p.releaseReadBackend(rb, txStatus != 'I')
		return false, true
	}
	if _, err := st.client.Write(BuildPGMessage(firstT, firstBody)); err != nil {
		p.releaseReadBackend(rb, true)
		return false, false
	}
	var rstats relayStats
	var txStatus byte = 'I'
	var hadErr bool
	if firstT == 'Z' {
		if len(firstBody) > 0 {
			txStatus = firstBody[0]
		}
	} else {
		var relayOK bool
		rstats, txStatus, hadErr, relayOK = relayUntilReady(rb, st.client)
		if !relayOK {
			p.releaseReadBackend(rb, true)
			return false, false
		}
	}
	rstats.bytesOut += uint64(5 + len(firstBody))
	if firstT == 'D' {
		rstats.rowsReturned++
		rstats.dataRowBytes += uint64(len(firstBody))
	}
	st.txStatus = txStatus
	p.releaseReadBackend(rb, hadErr || st.txStatus != 'I')
	p.recordQueryMetrics(st, qStart, poolWaitUs, bytesIn, rstats, hadErr, 1, queryType)
	return true, true
}

func (p *TenantPool) handleSimpleQuery(st *clientSessionState, mbody []byte) bool {
	qStart := time.Now()
	bytesIn := uint64(5 + len(mbody))

	off := 0
	q := ReadCStr(mbody, &off)
	trimmed := strings.TrimSpace(q)
	queryType := ClassifyQueryType(trimmed)

	if IsSessionPinningSQL(trimmed) && !st.sessionPinned {
		st.sessionPinned = true
		p.pinnedClients.Add(1)
	}

	if sm := reSetStmt.FindStringSubmatch(trimmed); sm != nil {
		key, val := sm[1], sm[2]
		hadErr, ioOK, ok := p.executeSimpleAndRelay(st, BuildPGMessage('Q', mbody), bytesIn, qStart, queryType)
		if !ok {
			return ioOK
		}
		if !hadErr {
			st.sessionVars[key] = val
		}
	} else if sm := reResetStmt.FindStringSubmatch(trimmed); sm != nil {
		key := sm[1]
		hadErr, ioOK, ok := p.executeSimpleAndRelay(st, BuildPGMessage('Q', mbody), bytesIn, qStart, queryType)
		if !ok {
			return ioOK
		}
		if !hadErr {
			if strings.EqualFold(key, "ALL") {
				clear(st.sessionVars)
			} else {
				delete(st.sessionVars, key)
			}
		}
	} else if sm := rePrepareStmt.FindStringSubmatch(trimmed); sm != nil {
		clientName, typeList, innerQ := sm[1], sm[2], sm[3]
		globalName := fmt.Sprintf("_air_s_%d_%d", st.clientID, p.stmtSeq.Add(1))
		rewritten := fmt.Sprintf("PREPARE %s%s AS %s", globalName, typeList, innerQ)
		hadErr, ioOK, ok := p.executeSimpleAndRelay(st, BuildSimpleQuery(rewritten), bytesIn, qStart, queryType)
		if !ok {
			return ioOK
		}
		if !hadErr {
			st.clientStmts[clientName] = PreparedDef{
				ClientName: clientName,
				GlobalName: globalName,
				TypeList:   typeList,
				Query:      innerQ,
				IsSQLPrep:  true,
			}
			if st.currentBackend != nil {
				st.currentBackend.PreparedStmts[globalName] = struct{}{}
			}
		}
	} else if sm := reDeallocStmt.FindStringSubmatch(trimmed); sm != nil {
		cname := sm[1]
		var deallocSQL, erasedGlobal string
		deallocAll := strings.EqualFold(cname, "ALL")
		if deallocAll {
			deallocSQL = "DEALLOCATE ALL"
		} else if def, ok := st.clientStmts[cname]; ok {
			erasedGlobal = def.GlobalName
			deallocSQL = "DEALLOCATE " + erasedGlobal
		} else {
			deallocSQL = "DEALLOCATE " + cname
		}
		hadErr, ioOK, ok := p.executeSimpleAndRelay(st, BuildSimpleQuery(deallocSQL), bytesIn, qStart, queryType)
		if !ok {
			return ioOK
		}
		if !hadErr {
			if deallocAll {
				clear(st.clientStmts)
				if st.currentBackend != nil {
					clear(st.currentBackend.PreparedStmts)
				}
			} else if erasedGlobal != "" {
				delete(st.clientStmts, cname)
				if st.currentBackend != nil {
					delete(st.currentBackend.PreparedStmts, erasedGlobal)
				}
			}
		}
	} else {
		outBody := mbody
		isExecStmt := false
		if sm := reExecuteStmt.FindStringSubmatch(trimmed); sm != nil {
			isExecStmt = true
			if def, ok := st.clientStmts[sm[2]]; ok {
				rewritten := sm[1] + def.GlobalName + sm[3]
				outBody = appendCStr(nil, rewritten)
			}
		}

		p.mu.Lock()
		rport := p.readPort
		p.mu.Unlock()

		routedToRead := false
		if !isExecStmt && !st.sessionPinned && st.txStatus == 'I' && st.currentBackend == nil && rport > 0 {
			rlsn := p.readEngineLSN.Load()
			if st.sessionWriteLSN > 0 && rlsn < st.sessionWriteLSN {
				if rb, err := p.acquireReadBackend(st.clientID); err == nil && rb != nil {
					if fresh, ok := queryLSNBackend(rb, "SELECT COALESCE(pg_last_wal_replay_lsn(), pg_current_wal_lsn())::text"); ok {
						rlsn = fresh
						p.readEngineLSN.Store(fresh)
					}
					p.readEngineHealthy.Store(true)
					p.releaseReadBackend(rb, false)
				} else {
					p.readEngineHealthy.Store(false)
				}
			}
			rctx := SessionRouteContext{
				SessionPinned:     st.sessionPinned,
				InTransaction:     st.txStatus != 'I',
				ReadEngineHealthy: p.readEngineHealthy.Load(),
				SessionWriteLSN:   st.sessionWriteLSN,
				ReadEngineLSN:     rlsn,
			}
			rdec := RouteQuery(trimmed, rctx, &p.routingMetrics)
			if rdec.Target == RouteTargetReadEngine {
				execOK, ioOK := p.tryExecuteOnReadEngine(st, BuildPGMessage('Q', outBody), bytesIn, qStart, queryType)
				if !ioOK {
					return false
				}
				routedToRead = execOK
				if !routedToRead {
					RecordReadFallback(&p.routingMetrics)
				}
			}
		}
		if !routedToRead {
			hadErr, ioOK, ok := p.executeSimpleAndRelay(st, BuildPGMessage('Q', outBody), bytesIn, qStart, queryType)
			if !ok {
				return ioOK
			}
			if rport > 0 && !hadErr && st.txStatus == 'I' && st.currentBackend != nil &&
				ClassifySQLForRouting(trimmed).Target == RouteTargetWriteEngine {
				if wlsn, ok := queryLSNBackend(st.currentBackend, "SELECT pg_current_wal_lsn()::text"); ok && wlsn > st.sessionWriteLSN {
					st.sessionWriteLSN = wlsn
				}
			}
		}
	}

	if st.txStatus == 'I' && !st.sessionPinned {
		p.releaseCurrent(st, false)
	}
	return true
}

type extMsg struct {
	msgType byte
	body    []byte
}

func (p *TenantPool) handleExtendedBatch(st *clientSessionState, terminatorType byte, terminatorBody []byte, extBatch *[]extMsg, extBatchBytes *int) bool {
	qStart := time.Now()
	*extBatch = append(*extBatch, extMsg{msgType: terminatorType, body: terminatorBody})
	var bytesIn, execCount uint64
	var batchSQLs []string
	for _, m := range *extBatch {
		bytesIn += uint64(5 + len(m.body))
		switch m.msgType {
		case 'E':
			execCount++
		case 'P':
			off := 0
			_ = ReadCStr(m.body, &off)
			if pq := ReadCStr(m.body, &off); pq != "" {
				batchSQLs = append(batchSQLs, pq)
			}
		case 'B':
			off := 0
			_ = ReadCStr(m.body, &off)
			sname := ReadCStr(m.body, &off)
			if def, ok := st.clientStmts[sname]; ok && def.Query != "" {
				batchSQLs = append(batchSQLs, def.Query)
			}
		}
	}
	if execCount == 0 {
		execCount = 1
	}
	batchQueryType := "EXTENDED"
	if len(batchSQLs) > 0 {
		batchQueryType = ClassifyQueryType(strings.Join(batchSQLs, "; "))
	}

	p.mu.Lock()
	rport := p.readPort
	p.mu.Unlock()

	batchHasWrite := false
	tryReadEngine := rport > 0 && terminatorType == 'S' && !st.sessionPinned && st.txStatus == 'I' && st.currentBackend == nil
	sawRoutable := false
	if rport > 0 {
		rlsn := p.readEngineLSN.Load()
		if tryReadEngine && st.sessionWriteLSN > 0 && rlsn < st.sessionWriteLSN {
			if rb, err := p.acquireReadBackend(st.clientID); err == nil && rb != nil {
				if fresh, ok := queryLSNBackend(rb, "SELECT COALESCE(pg_last_wal_replay_lsn(), pg_current_wal_lsn())::text"); ok {
					rlsn = fresh
					p.readEngineLSN.Store(fresh)
				}
				p.readEngineHealthy.Store(true)
				p.releaseReadBackend(rb, false)
			} else {
				p.readEngineHealthy.Store(false)
			}
		}
		rctx := SessionRouteContext{
			SessionPinned:     st.sessionPinned,
			InTransaction:     st.txStatus != 'I',
			ReadEngineHealthy: p.readEngineHealthy.Load(),
			SessionWriteLSN:   st.sessionWriteLSN,
			ReadEngineLSN:     rlsn,
		}
		for _, m := range *extBatch {
			switch m.msgType {
			case 'P':
				off := 0
				_ = ReadCStr(m.body, &off)
				pq := ReadCStr(m.body, &off)
				if IsSessionPinningSQL(pq) {
					tryReadEngine = false
				}
				if ClassifySQLForRouting(pq).Target == RouteTargetWriteEngine {
					batchHasWrite = true
					tryReadEngine = false
				} else if tryReadEngine {
					sawRoutable = true
					if RouteQuery(pq, rctx, &p.routingMetrics).Target != RouteTargetReadEngine {
						tryReadEngine = false
					}
				}
			case 'B':
				off := 0
				_ = ReadCStr(m.body, &off)
				sname := ReadCStr(m.body, &off)
				if def, ok := st.clientStmts[sname]; ok {
					if ClassifySQLForRouting(def.Query).Target == RouteTargetWriteEngine {
						batchHasWrite = true
						tryReadEngine = false
					} else if tryReadEngine {
						sawRoutable = true
						if RouteQuery(def.Query, rctx, &p.routingMetrics).Target != RouteTargetReadEngine {
							tryReadEngine = false
						}
					}
				}
			}
		}
		if !sawRoutable {
			tryReadEngine = false
		}
	}

	runBatchOnBackend := func(targetB *BackendConn, isReadBackend bool, waitUs uint64) int {
		type undoParse struct {
			cname   string
			gname   string
			hadPrev bool
			prev    PreparedDef
		}
		var undoParses []undoParse
		preparedMsgs := make([]extMsg, 0, len(*extBatch))

		rollbackParses := func() {
			for i := len(undoParses) - 1; i >= 0; i-- {
				u := undoParses[i]
				delete(targetB.PreparedStmts, u.gname)
				if u.hadPrev {
					st.clientStmts[u.cname] = u.prev
				} else {
					delete(st.clientStmts, u.cname)
				}
			}
		}

		for _, m := range *extBatch {
			bt := m.msgType
			bb := m.body
			switch bt {
			case 'P':
				gname := fmt.Sprintf("_air_s_%d_%d", st.clientID, p.stmtSeq.Add(1))
				rewritten, cname, query, oids := RewriteParseBody(bb, gname)
				trimmedQ := strings.TrimSpace(query)

				if psm := rePrepareStmt.FindStringSubmatch(trimmedQ); psm != nil {
					sqlCname, sqlTlist, sqlInner := psm[1], psm[2], psm[3]
					sqlGname := fmt.Sprintf("_air_s_%d_%d", st.clientID, p.stmtSeq.Add(1))
					query = fmt.Sprintf("PREPARE %s%s AS %s", sqlGname, sqlTlist, sqlInner)
					rewritten = MakeParseBody(gname, query, oids)
					st.clientStmts[sqlCname] = PreparedDef{
						ClientName: sqlCname,
						GlobalName: sqlGname,
						TypeList:   sqlTlist,
						Query:      sqlInner,
						IsSQLPrep:  true,
					}
					targetB.PreparedStmts[sqlGname] = struct{}{}
				} else if psm := reExecuteStmt.FindStringSubmatch(trimmedQ); psm != nil {
					if def, ok := st.clientStmts[psm[2]]; ok {
						query = psm[1] + def.GlobalName + psm[3]
						rewritten = MakeParseBody(gname, query, oids)
					}
				} else if psm := reDeallocStmt.FindStringSubmatch(trimmedQ); psm != nil {
					sqlCname := psm[1]
					if strings.EqualFold(sqlCname, "ALL") {
						clear(st.clientStmts)
						clear(targetB.PreparedStmts)
					} else if def, ok := st.clientStmts[sqlCname]; ok {
						query = "DEALLOCATE " + def.GlobalName
						rewritten = MakeParseBody(gname, query, oids)
						delete(targetB.PreparedStmts, def.GlobalName)
						delete(st.clientStmts, sqlCname)
					}
				}

				if IsSessionPinningSQL(query) && !st.sessionPinned {
					st.sessionPinned = true
					p.pinnedClients.Add(1)
				}
				var u undoParse
				u.cname = cname
				u.gname = gname
				if prev, ok := st.clientStmts[cname]; ok {
					u.hadPrev = true
					u.prev = prev
				}
				undoParses = append(undoParses, u)

				st.clientStmts[cname] = PreparedDef{
					ClientName: cname,
					GlobalName: gname,
					Query:      query,
					ParamOIDs:  oids,
				}
				targetB.PreparedStmts[gname] = struct{}{}
				bb = rewritten
			case 'B':
				bb = RewriteBindBody(bb, st.clientStmts)
			case 'D':
				bb, _ = RewriteNamedOpBody(bb, st.clientStmts, false)
			case 'C':
				var erased string
				bb, erased = RewriteNamedOpBody(bb, st.clientStmts, true)
				if erased != "" {
					if def, ok := st.clientStmts[erased]; ok {
						delete(targetB.PreparedStmts, def.GlobalName)
						delete(st.clientStmts, erased)
					}
				}
			}

			if _, err := targetB.Conn.Write(BuildPGMessage(bt, bb)); err != nil {
				rollbackParses()
				if isReadBackend {
					return 0
				}
				return -1
			}
			preparedMsgs = append(preparedMsgs, extMsg{msgType: bt, body: bb})
		}

		if terminatorType == 'S' {
			rstats, txStatus, hadErr, ok := relayUntilReady(targetB, st.client)
			if !ok {
				return -1
			}
			st.txStatus = txStatus
			if hadErr {
				rollbackParses()
			}
			p.recordQueryMetrics(st, qStart, waitUs, bytesIn, rstats, hadErr, execCount, batchQueryType)
			if !isReadBackend && rport > 0 && !hadErr && st.txStatus == 'I' && batchHasWrite {
				if wlsn, ok := queryLSNBackend(targetB, "SELECT pg_current_wal_lsn()::text"); ok && wlsn > st.sessionWriteLSN {
					st.sessionWriteLSN = wlsn
				}
			}
			return 1
		}

		rstats, hadErr, ok := relayFlushBatch(targetB, st.client, preparedMsgs)
		if !ok {
			return -1
		}
		if hadErr {
			rollbackParses()
		}
		p.recordQueryMetrics(st, qStart, waitUs, bytesIn, rstats, hadErr, execCount, batchQueryType)
		return 1
	}

	if tryReadEngine {
		waitStart := time.Now()
		if rb, err := p.acquireReadBackend(st.clientID); err == nil && rb != nil {
			rwaitUs := uint64(time.Since(waitStart).Microseconds())
			p.readEngineHealthy.Store(true)
			if p.ensureSessionAndStmts(rb, st) {
				rc := runBatchOnBackend(rb, true, rwaitUs)
				if rc == 1 {
					*extBatch = (*extBatch)[:0]
					*extBatchBytes = 0
					p.releaseReadBackend(rb, st.txStatus != 'I')
					return true
				}
				if rc == -1 {
					*extBatch = (*extBatch)[:0]
					*extBatchBytes = 0
					p.releaseReadBackend(rb, true)
					return false
				}
			}
			p.releaseReadBackend(rb, true)
		} else {
			p.readEngineHealthy.Store(false)
		}
		RecordReadFallback(&p.routingMetrics)
	}

	poolWaitUs, acquired := p.ensureBackend(st)
	if !acquired {
		*extBatch = (*extBatch)[:0]
		*extBatchBytes = 0
		p.recordQueryMetrics(st, qStart, poolWaitUs, bytesIn, relayStats{}, true, execCount, batchQueryType)
		return true
	}

	st.backendInFlight = true
	rc := runBatchOnBackend(st.currentBackend, false, poolWaitUs)
	*extBatch = (*extBatch)[:0]
	*extBatchBytes = 0
	st.backendInFlight = false
	if rc == -1 {
		p.releaseCurrent(st, true)
		return false
	}
	if terminatorType == 'S' {
		p.updateTxCount(st)
		if st.txStatus == 'I' && !st.sessionPinned {
			p.releaseCurrent(st, false)
		}
	}
	return true
}

func (p *TenantPool) ServeClient(client net.Conn, startupUser, expectedPass string, touchActivity func(), metrics *MetricsRegistry, cancelReg *CancelRegistry, clientIdleTimeoutMS int) {
	clientID := p.clientSeq.Add(1)
	gwPID := uint32(10000 + (clientID % 50000))
	var gwSecret uint32
	var sb [4]byte
	if _, err := rand.Read(sb[:]); err == nil {
		gwSecret = binary.BigEndian.Uint32(sb[:])
	}
	if gwSecret == 0 {
		gwSecret = uint32(0x9e3779b9 ^ (clientID * 2654435761))
	}

	if !p.authenticateClient(client, startupUser, expectedPass, gwPID, gwSecret, metrics) {
		return
	}

	_ = client.SetDeadline(time.Time{})

	st := &clientSessionState{
		client:        client,
		startupUser:   startupUser,
		clientID:      clientID,
		gwPID:         gwPID,
		gwSecret:      gwSecret,
		metrics:       metrics,
		cancelReg:     cancelReg,
		sessionVars:   make(map[string]string),
		clientStmts:   make(map[string]PreparedDef),
		txStatus:      'I',
		touchActivity: touchActivity,
	}
	cleanup := func() {
		if st.sessionPinned {
			p.pinnedClients.Add(-1)
		}
		if st.inTxCounted {
			p.activeTxCount.Add(-1)
		}
		p.releaseCurrent(st, st.txStatus != 'I' || st.sessionPinned || st.backendInFlight)
	}
	defer cleanup()

	const maxExtBatchMsgs = 4096
	const maxExtBatchBytes = 32 * 1024 * 1024
	var extBatch []extMsg
	extBatchBytes := 0

	for {
		if clientIdleTimeoutMS > 0 {
			_ = client.SetReadDeadline(time.Now().Add(time.Duration(clientIdleTimeoutMS) * time.Millisecond))
		}
		mtype, mbody, err := ReadPGPacket(client)
		if err != nil {
			return
		}
		if clientIdleTimeoutMS > 0 {
			_ = client.SetReadDeadline(time.Time{})
		}

		switch mtype {
		case 'X':
			return
		case 'Q':
			if !p.handleSimpleQuery(st, mbody) {
				return
			}
		case 'P', 'B', 'D', 'E', 'C':
			if len(extBatch) >= maxExtBatchMsgs || extBatchBytes+5+len(mbody) > maxExtBatchBytes {
				extBatch = nil
				extBatchBytes = 0
				if metrics != nil {
					metrics.RecordProtocolError()
				}
				_, _ = client.Write(BuildErrorResponse("FATAL", "54000", "extended query pipeline buffer limit exceeded"))
				return
			}
			extBatchBytes += 5 + len(mbody)
			extBatch = append(extBatch, extMsg{msgType: mtype, body: mbody})
		case 'S', 'H':
			if !p.handleExtendedBatch(st, mtype, mbody, &extBatch, &extBatchBytes) {
				return
			}
		default:
			if metrics != nil {
				metrics.RecordProtocolError()
			}
			if _, err := client.Write(BuildErrorResponse("ERROR", "08P01", "unsupported frontend message type")); err != nil {
				return
			}
			if _, err := client.Write(BuildReadyForQuery(st.txStatus)); err != nil {
				return
			}
		}
	}
}

func (p *TenantPool) acquireSlot() error {
	deadline := time.Now().Add(10 * time.Second)
	p.semMu.Lock()
	defer p.semMu.Unlock()

	for p.activeBackends >= p.maxBackends {
		rem := time.Until(deadline)
		if rem <= 0 {
			return fmt.Errorf("backend connection pool saturated for tenant %s", p.tenantID)
		}
		timer := time.AfterFunc(rem, func() {
			p.semCond.Broadcast()
		})
		p.semCond.Wait()
		timer.Stop()
	}
	p.activeBackends++
	return nil
}

func (p *TenantPool) releaseSlot() {
	p.semMu.Lock()
	if p.activeBackends > 0 {
		p.activeBackends--
	}
	p.semMu.Unlock()
	p.semCond.Signal()
}

func (p *TenantPool) acquireBackend(clientID uint64) (*BackendConn, error) {
	if err := p.acquireSlot(); err != nil {
		return nil, err
	}

	now := time.Now()
	timeout := time.Duration(p.idleTimeoutSec.Load()) * time.Second
	for {
		var b *BackendConn
		p.mu.Lock()
		if !p.suspended {
			if n := len(p.idlePool); n > 0 {
				b = p.idlePool[n-1]
				p.idlePool = p.idlePool[:n-1]
			}
		}
		p.mu.Unlock()

		if b == nil {
			break
		}
		if now.Sub(b.LastUsed) >= timeout {
			b.Close()
			continue
		}
		if b.LastClientID != clientID {
			if !execSimpleBackend(b, "DISCARD ALL;") {
				b.Close()
				continue
			}
			b.LastClientID = clientID
			clear(b.PreparedStmts)
		} else if !execSimpleBackend(b, "") {
			b.Close()
			continue
		}
		return b, nil
	}

	p.mu.Lock()
	host, sockDir, port, pass, wake, suspended := p.host, p.sockDir, p.port, p.appPassword, p.wakeFn, p.suspended
	p.mu.Unlock()

	var b *BackendConn
	var err error
	if suspended || port <= 0 {
		if wake == nil {
			p.releaseSlot()
			return nil, fmt.Errorf("tenant %s is suspended and wake function is unavailable", p.tenantID)
		}
		log.Printf("[air-gateway] tenant=%s is suspended (port=%d); holding query and waking primary", p.tenantID, port)
		wroute, werr := wake()
		if werr != nil {
			p.releaseSlot()
			return nil, fmt.Errorf("wake tenant %s failed: %w", p.tenantID, werr)
		}
		if wroute.Port <= 0 {
			p.releaseSlot()
			return nil, fmt.Errorf("wake tenant %s returned invalid port %d", p.tenantID, wroute.Port)
		}
		p.UpdateEndpoint(wroute.Host, wroute.SockDir, wroute.Port, wroute.AppPassword)
		p.mu.Lock()
		host, sockDir, port, pass = p.host, p.sockDir, p.port, p.appPassword
		p.mu.Unlock()
		b, err = p.dialAndStartup(host, sockDir, port, pass, clientID)
	} else {
		b, err = p.dialAndStartup(host, sockDir, port, pass, clientID)
		if err != nil && wake != nil {
			if wroute, werr := wake(); werr == nil && wroute.Port > 0 {
				p.UpdateEndpoint(wroute.Host, wroute.SockDir, wroute.Port, wroute.AppPassword)
				p.mu.Lock()
				host, sockDir, port, pass = p.host, p.sockDir, p.port, p.appPassword
				p.mu.Unlock()
				b, err = p.dialAndStartup(host, sockDir, port, pass, clientID)
			}
		}
	}
	if err != nil {
		p.releaseSlot()
		return nil, err
	}
	return b, nil
}

func (p *TenantPool) releaseBackend(b *BackendConn, dirty bool) {
	if b == nil {
		return
	}
	if dirty {
		b.Close()
		p.releaseSlot()
		return
	}
	b.LastUsed = time.Now()
	p.mu.Lock()
	if len(p.idlePool) < p.maxBackends {
		p.idlePool = append(p.idlePool, b)
		p.mu.Unlock()
		p.releaseSlot()
		return
	}
	p.mu.Unlock()
	b.Close()
	p.releaseSlot()
}

func (p *TenantPool) acquireReadBackend(clientID uint64) (*BackendConn, error) {
	now := time.Now()
	timeout := time.Duration(p.idleTimeoutSec.Load()) * time.Second
	for {
		var b *BackendConn
		p.mu.Lock()
		if n := len(p.readIdlePool); n > 0 {
			b = p.readIdlePool[n-1]
			p.readIdlePool = p.readIdlePool[:n-1]
		}
		p.mu.Unlock()

		if b == nil {
			break
		}
		if now.Sub(b.LastUsed) >= timeout {
			b.Close()
			continue
		}
		if b.LastClientID != clientID {
			if !execSimpleBackend(b, "DISCARD ALL;") {
				b.Close()
				continue
			}
			b.LastClientID = clientID
			clear(b.PreparedStmts)
		} else if !execSimpleBackend(b, "") {
			b.Close()
			continue
		}
		return b, nil
	}

	p.mu.Lock()
	rhost, rsock, rport, pass := p.readHost, p.readSockDir, p.readPort, p.appPassword
	p.mu.Unlock()
	if rport <= 0 && rsock == "" {
		return nil, fmt.Errorf("read engine disabled")
	}
	return p.dialAndStartup(rhost, rsock, rport, pass, clientID)
}

func (p *TenantPool) releaseReadBackend(b *BackendConn, dirty bool) {
	if b == nil {
		return
	}
	if dirty {
		b.Close()
		return
	}
	b.LastUsed = time.Now()
	p.mu.Lock()
	if len(p.readIdlePool) < p.maxBackends {
		p.readIdlePool = append(p.readIdlePool, b)
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	b.Close()
}

func (p *TenantPool) dialAndStartup(host, sockDir string, port int, appPass string, clientID uint64) (*BackendConn, error) {
	conn, err := dialBackendSocket(host, sockDir, port)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to tenant backend on port %d: %w", port, err)
	}
	b := &BackendConn{
		Conn:          conn,
		LastClientID:  clientID,
		PreparedStmts: make(map[string]struct{}),
	}

	startup := BuildStartupMessage(map[string]string{
		"user":             "app_user",
		"database":         p.tenantID,
		"application_name": "air-pooler",
	})
	if _, err := b.Conn.Write(startup); err != nil {
		b.Close()
		return nil, fmt.Errorf("failed to write startup to backend: %w", err)
	}

	for {
		t, body, err := ReadPGPacket(b.Conn)
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("backend closed during startup: %w", err)
		}
		switch t {
		case 'R':
			if len(body) < 4 {
				b.Close()
				return nil, fmt.Errorf("malformed AuthRequest from backend")
			}
			code := binary.BigEndian.Uint32(body[:4])
			switch code {
			case 0:
				continue
			case 3:
				if _, err := b.Conn.Write(BuildPasswordMessage(appPass)); err != nil {
					b.Close()
					return nil, fmt.Errorf("failed to send cleartext password to backend: %w", err)
				}
			case 10:
				if err := SCRAMSHA256ClientAuth(b.Conn, "app_user", appPass); err != nil {
					b.Close()
					return nil, err
				}
			default:
				b.Close()
				return nil, fmt.Errorf("unsupported backend auth method: %d", code)
			}
		case 'K':
			if len(body) >= 8 {
				b.BackendPID = binary.BigEndian.Uint32(body[0:4])
				b.BackendSecret = binary.BigEndian.Uint32(body[4:8])
			}
		case 'Z':
			if len(body) > 0 {
				return b, nil
			}
		case 'E':
			b.Close()
			return nil, fmt.Errorf("backend startup error")
		}
	}
}

func parsePGLSN(s string) uint64 {
	slash := strings.IndexByte(s, '/')
	if slash <= 0 || slash+1 >= len(s) {
		return 0
	}
	hi, err1 := strconv.ParseUint(s[:slash], 16, 64)
	lo, err2 := strconv.ParseUint(s[slash+1:], 16, 64)
	if err1 != nil || err2 != nil {
		return 0
	}
	return (hi << 32) | (lo & 0xffffffff)
}

func queryLSNBackend(b *BackendConn, sql string) (uint64, bool) {
	if b == nil {
		return 0, false
	}
	if _, err := b.Conn.Write(BuildSimpleQuery(sql)); err != nil {
		return 0, false
	}
	var lsn uint64
	ok := false
	for {
		t, body, err := ReadPGPacket(b.Conn)
		if err != nil {
			return 0, false
		}
		switch t {
		case 'D':
			if len(body) >= 6 {
				ncols := binary.BigEndian.Uint16(body[:2])
				if ncols >= 1 {
					flen := int32(binary.BigEndian.Uint32(body[2:6]))
					if flen > 0 && 6+int(flen) <= len(body) {
						lsn = parsePGLSN(string(body[6 : 6+int(flen)]))
						ok = lsn > 0
					}
				}
			}
		case 'E':
			ok = false
		case 'Z':
			return lsn, ok
		}
	}
}

func execSimpleBackend(b *BackendConn, sql string) bool {
	if _, err := b.Conn.Write(BuildSimpleQuery(sql)); err != nil {
		return false
	}
	return drainUntilReadyBackend(b)
}

func drainUntilReadyBackend(b *BackendConn) bool {
	for {
		t, _, err := ReadPGPacket(b.Conn)
		if err != nil {
			return false
		}
		if t == 'E' {
			return false
		}
		if t == 'Z' {
			return true
		}
	}
}

func relayCopyIn(b *BackendConn, client io.ReadWriter) bool {
	for {
		ct, cbody, err := ReadPGPacket(client)
		if err != nil {
			return false
		}
		if ct != 'd' && ct != 'c' && ct != 'f' {
			return false
		}
		if _, err := b.Conn.Write(BuildPGMessage(ct, cbody)); err != nil {
			return false
		}
		if ct == 'c' || ct == 'f' {
			return true
		}
	}
}

func relayUntilReady(b *BackendConn, client io.ReadWriter) (stats relayStats, txStatus byte, hadErr bool, ok bool) {
	txStatus = 'I'
	for {
		t, body, err := ReadPGPacket(b.Conn)
		if err != nil {
			return stats, txStatus, hadErr, false
		}
		stats.bytesOut += uint64(5 + len(body))
		if t == 'D' {
			stats.rowsReturned++
			stats.dataRowBytes += uint64(len(body))
		}
		if t == 'E' {
			hadErr = true
		}
		if _, err := client.Write(BuildPGMessage(t, body)); err != nil {
			return stats, txStatus, hadErr, false
		}
		if t == 'G' {
			if !relayCopyIn(b, client) {
				return stats, txStatus, hadErr, false
			}
			continue
		}
		if t == 'Z' && len(body) > 0 {
			txStatus = body[0]
			return stats, txStatus, hadErr, true
		}
	}
}

func relayFlushBatch(b *BackendConn, client io.ReadWriter, batch []extMsg) (stats relayStats, hadErr bool, ok bool) {
	forwardPkt := func() (byte, []byte, bool) {
		for {
			t, body, err := ReadPGPacket(b.Conn)
			if err != nil {
				return 0, nil, false
			}
			stats.bytesOut += uint64(5 + len(body))
			if t == 'D' {
				stats.rowsReturned++
				stats.dataRowBytes += uint64(len(body))
			}
			if t == 'E' {
				hadErr = true
			}
			if _, err := client.Write(BuildPGMessage(t, body)); err != nil {
				return 0, nil, false
			}
			if t == 'N' || t == 'S' || t == 'A' {
				continue
			}
			return t, body, true
		}
	}

	for _, m := range batch {
		if m.msgType == 'H' || hadErr {
			break
		}
		switch m.msgType {
		case 'P', 'B', 'C':
			if _, _, ok := forwardPkt(); !ok {
				return stats, hadErr, false
			}
		case 'D':
			t, _, ok := forwardPkt()
			if !ok {
				return stats, hadErr, false
			}
			if t == 't' {
				if _, _, ok := forwardPkt(); !ok {
					return stats, hadErr, false
				}
			}
		case 'E':
			for {
				t, _, ok := forwardPkt()
				if !ok {
					return stats, hadErr, false
				}
				if t == 'G' {
					if !relayCopyIn(b, client) {
						return stats, hadErr, false
					}
					continue
				}
				if t == 'C' || t == 'I' || t == 's' || t == 'E' {
					break
				}
			}
		}
	}
	return stats, hadErr, true
}

// PoolManager is a thread-safe registry of per-tenant TenantPool instances.
type PoolManager struct {
	maxBackends    int
	idleTimeoutSec int

	mu          sync.Mutex
	readHost    string
	readSockDir string
	readPort    int
	metering    *MeteringLogger
	pools       map[string]*TenantPool
}

func NewPoolManager(maxBackends, idleTimeoutSec int) *PoolManager {
	if idleTimeoutSec <= 0 {
		idleTimeoutSec = 300
	}
	return &PoolManager{
		maxBackends:    maxBackends,
		idleTimeoutSec: idleTimeoutSec,
		pools:          make(map[string]*TenantPool),
	}
}

func (pm *PoolManager) SetMeteringLogger(m *MeteringLogger) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.metering = m
	for _, p := range pm.pools {
		if p != nil {
			p.SetMeteringLogger(m)
		}
	}
}

func (pm *PoolManager) SetReadEngineEndpoint(host, sockDir string, port int) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.readHost = host
	pm.readSockDir = sockDir
	pm.readPort = port
	for _, p := range pm.pools {
		if p != nil {
			p.SetReadEngineEndpoint(host, sockDir, port)
		}
	}
}

func (pm *PoolManager) GetOrCreate(route TenantRoute, wakeFn WakeFn) *TenantPool {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if p, ok := pm.pools[route.TenantID]; ok {
		if wakeFn != nil {
			p.SetWakeFn(wakeFn)
		}
		if route.State == "SUSPENDED" {
			if route.AppPassword != "" {
				p.mu.Lock()
				p.appPassword = route.AppPassword
				p.mu.Unlock()
			}
			p.MarkSuspended()
		} else {
			p.UpdateEndpoint(route.Host, route.SockDir, route.Port, route.AppPassword)
		}
		return p
	}
	p := NewTenantPool(route.TenantID, route.Host, route.SockDir, route.Port, route.AppPassword, pm.maxBackends, pm.idleTimeoutSec)
	if route.State == "SUSPENDED" {
		p.MarkSuspended()
	}
	if pm.readPort > 0 || pm.readSockDir != "" {
		p.SetReadEngineEndpoint(pm.readHost, pm.readSockDir, pm.readPort)
	}
	if pm.metering != nil {
		p.SetMeteringLogger(pm.metering)
	}
	p.SetWakeFn(wakeFn)
	pm.pools[route.TenantID] = p
	return p
}

func (pm *PoolManager) EvictExpiredIdle() int {
	pm.mu.Lock()
	list := make([]*TenantPool, 0, len(pm.pools))
	for _, p := range pm.pools {
		if p != nil {
			list = append(list, p)
		}
	}
	pm.mu.Unlock()

	evicted := 0
	for _, p := range list {
		evicted += p.EvictExpiredIdle()
	}
	return evicted
}

func (pm *PoolManager) UpsertDiscovered(route TenantRoute) {
	pm.mu.Lock()
	p := pm.pools[route.TenantID]
	pm.mu.Unlock()
	if p == nil {
		return
	}
	if route.State == "SUSPENDED" {
		p.MarkSuspended()
	} else {
		p.UpdateEndpoint(route.Host, route.SockDir, route.Port, route.AppPassword)
	}
}

func (pm *PoolManager) RemoveTenant(tenantID string) {
	pm.mu.Lock()
	p := pm.pools[tenantID]
	delete(pm.pools, tenantID)
	pm.mu.Unlock()
	if p != nil {
		p.CloseAllIdle()
	}
}

func (pm *PoolManager) RequestDrain(tenantID string, timeout time.Duration) error {
	pm.mu.Lock()
	p := pm.pools[tenantID]
	pm.mu.Unlock()
	if p == nil {
		return nil
	}
	return p.RequestDrain(timeout)
}

func (pm *PoolManager) SetTenantMaxBackends(tenantID string, maxBackends int) {
	pm.mu.Lock()
	p := pm.pools[tenantID]
	pm.mu.Unlock()
	if p != nil {
		p.SetMaxBackends(maxBackends)
	}
}

func (pm *PoolManager) Snapshots() []PoolTenantSnapshot {
	pm.mu.Lock()
	list := make([]*TenantPool, 0, len(pm.pools))
	for _, p := range pm.pools {
		if p != nil {
			list = append(list, p)
		}
	}
	pm.mu.Unlock()

	out := make([]PoolTenantSnapshot, 0, len(list))
	for _, p := range list {
		out = append(out, p.Snapshot())
	}
	return out
}

func (pm *PoolManager) CloseAll() {
	pm.mu.Lock()
	list := make([]*TenantPool, 0, len(pm.pools))
	for _, p := range pm.pools {
		if p != nil {
			list = append(list, p)
		}
	}
	pm.mu.Unlock()

	for _, p := range list {
		p.CloseAllIdle()
	}
}
