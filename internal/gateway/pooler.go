package gateway

import (
	"fmt"
	"net"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/jackc/pgx/v5/pgproto3"
)

var (
	reSetStmt       = regexp.MustCompile(`(?i)^\s*SET\s+(?:SESSION\s+)?([a-zA-Z0-9_.]+)\s*(?:=|TO)\s*(.+?)\s*;?\s*$`)
	rePrepareStmt   = regexp.MustCompile(`(?i)^\s*PREPARE\s+([a-zA-Z0-9_]+)(\s*\([^)]*\))?\s+AS\s+(.+?)\s*;?\s*$`)
	reDeallocStmt   = regexp.MustCompile(`(?i)^\s*DEALLOCATE\s+(?:PREPARE\s+)?([a-zA-Z0-9_]+)\s*;?\s*$`)
	reExecuteStmt   = regexp.MustCompile(`(?i)^(\s*EXECUTE\s+)([a-zA-Z0-9_]+)(\s*(?:\([^)]*\))?\s*;?\s*)$`)
	reListenStmt    = regexp.MustCompile(`(?i)^\s*LISTEN\b`)
	reTempTableStmt = regexp.MustCompile(`(?i)^\s*CREATE\s+(?:TEMP|TEMPORARY)\s+TABLE\b`)
	reAdvLockStmt   = regexp.MustCompile(`(?i)\bpg_advisory_lock(?:_shared)?\s*\(`)
)

type preparedDef struct {
	ClientName string
	GlobalName string
	TypeList   string
	Query      string
	ParamOIDs  []uint32
	IsSQLPrep  bool
}

type backendConn struct {
	conn          net.Conn
	fe            *pgproto3.Frontend
	lastClientID  uint64
	preparedStmts map[string]bool
}

func (b *backendConn) Close() {
	if b != nil && b.conn != nil {
		_ = b.conn.Close()
	}
}

// TenantPool multiplexes client PostgreSQL sessions over a bounded pool of backend connections
// in transaction mode, enforcing DISCARD ALL on cross-client backend reuse.
type TenantPool struct {
	tenantID    string
	maxBackends int

	mu          sync.Mutex
	sockDir     string
	port        int
	backendUser string
	appPassword string
	idlePool    []*backendConn
	sem         chan struct{}

	stmtSeq       atomic.Uint64
	clientSeq     atomic.Uint64
	pinnedClients atomic.Int64
	activeTxCount atomic.Int64
}

func newTenantPool(tenantID, sockDir string, port int, appPassword string, maxBackends int) *TenantPool {
	if maxBackends <= 0 {
		maxBackends = 16
	}
	return &TenantPool{
		tenantID:    tenantID,
		maxBackends: maxBackends,
		sockDir:     sockDir,
		port:        port,
		backendUser: "app_user",
		appPassword: appPassword,
		sem:         make(chan struct{}, maxBackends),
	}
}

func (p *TenantPool) updateEndpoint(sockDir string, port int, appPassword string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if appPassword != "" {
		p.appPassword = appPassword
	}
	if p.port != port || p.sockDir != sockDir {
		for _, b := range p.idlePool {
			b.Close()
		}
		p.idlePool = nil
		p.sockDir = sockDir
		p.port = port
	}
}

func (p *TenantPool) closeAllIdle() {
	p.mu.Lock()
	for _, b := range p.idlePool {
		b.Close()
	}
	p.idlePool = nil
	p.mu.Unlock()
}

// RequestDrain verifies no pinned sessions or active transactions remain, and closes idle pooled backends.
func (p *TenantPool) RequestDrain(timeout time.Duration) error {
	if p.pinnedClients.Load() > 0 {
		return fmt.Errorf("%w: %d pinned client session(s)", cell.ErrDrainVetoed, p.pinnedClients.Load())
	}
	deadline := time.Now().Add(timeout)
	for p.activeTxCount.Load() > 0 {
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: %d active transaction(s) exceeded drain timeout", cell.ErrDrainVetoed, p.activeTxCount.Load())
		}
		time.Sleep(15 * time.Millisecond)
	}
	if p.pinnedClients.Load() > 0 {
		return fmt.Errorf("%w: %d pinned client session(s)", cell.ErrDrainVetoed, p.pinnedClients.Load())
	}
	p.closeAllIdle()
	return nil
}

func (p *TenantPool) acquireBackend(clientID uint64) (*backendConn, error) {
	select {
	case p.sem <- struct{}{}:
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("backend connection pool saturated for tenant %s", p.tenantID)
	}

	b, err := p.acquireBackendLocked(clientID)
	if err != nil {
		<-p.sem
		return nil, err
	}
	return b, nil
}

func (p *TenantPool) acquireBackendLocked(clientID uint64) (*backendConn, error) {
	p.mu.Lock()
	if len(p.idlePool) > 0 {
		b := p.idlePool[len(p.idlePool)-1]
		p.idlePool = p.idlePool[:len(p.idlePool)-1]
		p.mu.Unlock()
		if b.lastClientID != clientID {
			if err := execSimpleBackend(b, "DISCARD ALL;"); err != nil {
				b.Close()
				return p.acquireBackendLocked(clientID)
			}
			b.lastClientID = clientID
			b.preparedStmts = make(map[string]bool)
		}
		return b, nil
	}
	sockDir := p.sockDir
	port := p.port
	user := p.backendUser
	appPass := p.appPassword
	db := p.tenantID
	p.mu.Unlock()

	var c net.Conn
	var err error
	if sockDir != "" {
		sockPath := filepath.Join(sockDir, fmt.Sprintf(".s.PGSQL.%d", port))
		c, err = net.DialTimeout("unix", sockPath, 5*time.Second)
	}
	if c == nil {
		c, err = net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
	}
	if err != nil {
		return nil, err
	}

	fe := pgproto3.NewFrontend(c, c)
	fe.Send(&pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user":             user,
			"database":         db,
			"application_name": "air-pooler",
		},
	})
	if err := fe.Flush(); err != nil {
		_ = c.Close()
		return nil, err
	}

	for {
		msg, err := fe.Receive()
		if err != nil {
			_ = c.Close()
			return nil, err
		}
		switch m := msg.(type) {
		case *pgproto3.AuthenticationOk:
		case *pgproto3.AuthenticationCleartextPassword:
			fe.Send(&pgproto3.PasswordMessage{Password: appPass})
			if err := fe.Flush(); err != nil {
				_ = c.Close()
				return nil, err
			}
		case *pgproto3.ParameterStatus, *pgproto3.BackendKeyData:
		case *pgproto3.ReadyForQuery:
			return &backendConn{conn: c, fe: fe, lastClientID: clientID}, nil
		case *pgproto3.ErrorResponse:
			_ = c.Close()
			return nil, fmt.Errorf("backend startup error (%s): %s", m.Code, m.Message)
		}
	}
}

func (p *TenantPool) releaseBackend(b *backendConn, dirty bool) {
	if b == nil {
		return
	}
	if dirty {
		b.Close()
		<-p.sem
		return
	}
	p.mu.Lock()
	if len(p.idlePool) >= p.maxBackends {
		p.mu.Unlock()
		b.Close()
		<-p.sem
		return
	}
	p.idlePool = append(p.idlePool, b)
	p.mu.Unlock()
	<-p.sem
}

func (p *TenantPool) serveClient(clientConn net.Conn, startupUser, expectedPass string, touchActivity func()) {
	clientID := p.clientSeq.Add(1)
	be := pgproto3.NewBackend(clientConn, clientConn)

	if expectedPass != "" {
		be.Send(&pgproto3.AuthenticationCleartextPassword{})
		if err := be.Flush(); err != nil {
			return
		}
		msg, err := be.Receive()
		if err != nil {
			return
		}
		pwMsg, ok := msg.(*pgproto3.PasswordMessage)
		if !ok || pwMsg.Password != expectedPass {
			be.Send(&pgproto3.ErrorResponse{
				Severity: "FATAL",
				Code:     "28P01",
				Message:  fmt.Sprintf("password authentication failed for user %q", startupUser),
			})
			_ = be.Flush()
			return
		}
	}

	be.Send(&pgproto3.AuthenticationOk{})
	be.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "17.12"})
	be.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	be.Send(&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, MDY"})
	be.Send(&pgproto3.BackendKeyData{ProcessID: uint32(10000 + clientID%50000), SecretKey: 9999})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := be.Flush(); err != nil {
		return
	}

	sessionVars := make(map[string]string)
	clientStmts := make(map[string]*preparedDef)

	var currentBackend *backendConn
	txStatus := byte('I')
	inTxCounted := false
	sessionPinned := false

	defer func() {
		if sessionPinned {
			p.pinnedClients.Add(-1)
		}
		if inTxCounted {
			p.activeTxCount.Add(-1)
		}
		if currentBackend != nil {
			p.releaseBackend(currentBackend, txStatus != 'I' || sessionPinned)
		}
	}()

	ensureSessionAndStmts := func(b *backendConn) error {
		if len(sessionVars) > 0 {
			var sb strings.Builder
			for k, val := range sessionVars {
				sb.WriteString(fmt.Sprintf("SET %s = %s; ", k, val))
			}
			if err := execSimpleBackend(b, sb.String()); err != nil {
				return err
			}
		}
		if b.preparedStmts == nil {
			b.preparedStmts = make(map[string]bool)
		}
		for _, def := range clientStmts {
			if b.preparedStmts[def.GlobalName] {
				continue
			}
			if def.IsSQLPrep {
				if err := execSimpleBackend(b, fmt.Sprintf("PREPARE %s%s AS %s", def.GlobalName, def.TypeList, def.Query)); err != nil {
					return err
				}
			} else {
				b.fe.Send(&pgproto3.Parse{
					Name:          def.GlobalName,
					Query:         def.Query,
					ParameterOIDs: def.ParamOIDs,
				})
				b.fe.Send(&pgproto3.Sync{})
				if err := b.fe.Flush(); err != nil {
					return err
				}
				if err := drainUntilReadyBackend(b); err != nil {
					return err
				}
			}
			b.preparedStmts[def.GlobalName] = true
		}
		return nil
	}

	rewriteSQLQuery := func(sqlStr string) string {
		qTrim := strings.TrimSpace(sqlStr)
		qUpper := strings.ToUpper(strings.TrimSuffix(qTrim, ";"))

		if qUpper == "DISCARD ALL" || qUpper == "RESET ALL" {
			for k := range sessionVars {
				delete(sessionVars, k)
			}
			if qUpper == "DISCARD ALL" {
				for k := range clientStmts {
					delete(clientStmts, k)
				}
				if currentBackend != nil {
					currentBackend.preparedStmts = make(map[string]bool)
				}
				if sessionPinned {
					sessionPinned = false
					p.pinnedClients.Add(-1)
				}
			}
			return sqlStr
		}
		if qUpper == "DEALLOCATE ALL" || qUpper == "DEALLOCATE PREPARE ALL" {
			for k := range clientStmts {
				delete(clientStmts, k)
			}
			if currentBackend != nil {
				currentBackend.preparedStmts = make(map[string]bool)
			}
			return sqlStr
		}
		if dm := reDeallocStmt.FindStringSubmatch(sqlStr); dm != nil {
			cName := strings.ToLower(dm[1])
			if def, ok := clientStmts[cName]; ok {
				delete(clientStmts, cName)
				if currentBackend != nil && currentBackend.preparedStmts != nil {
					delete(currentBackend.preparedStmts, def.GlobalName)
				}
				return fmt.Sprintf("DEALLOCATE %s", def.GlobalName)
			}
			return sqlStr
		}
		if reListenStmt.MatchString(qTrim) || reTempTableStmt.MatchString(qTrim) || reAdvLockStmt.MatchString(qTrim) {
			if !sessionPinned {
				sessionPinned = true
				p.pinnedClients.Add(1)
			}
		}
		if sm := reSetStmt.FindStringSubmatch(sqlStr); sm != nil {
			sessionVars[strings.ToLower(sm[1])] = sm[2]
			return sqlStr
		}
		if pm := rePrepareStmt.FindStringSubmatch(sqlStr); pm != nil {
			cName := strings.ToLower(pm[1])
			typeList := pm[2]
			body := pm[3]
			gName := fmt.Sprintf("__air_p_%d", p.stmtSeq.Add(1))
			def := &preparedDef{
				ClientName: cName,
				GlobalName: gName,
				TypeList:   typeList,
				Query:      body,
				IsSQLPrep:  true,
			}
			clientStmts[cName] = def
			if currentBackend != nil {
				if currentBackend.preparedStmts == nil {
					currentBackend.preparedStmts = make(map[string]bool)
				}
				currentBackend.preparedStmts[gName] = true
			}
			return fmt.Sprintf("PREPARE %s%s AS %s", gName, typeList, body)
		}
		if em := reExecuteStmt.FindStringSubmatch(sqlStr); em != nil {
			cName := strings.ToLower(em[2])
			if def, ok := clientStmts[cName]; ok {
				return em[1] + def.GlobalName + em[3]
			}
		}
		return sqlStr
	}

	for {
		msg, err := be.Receive()
		if err != nil {
			return
		}
		if _, ok := msg.(*pgproto3.Terminate); ok {
			return
		}
		if touchActivity != nil {
			touchActivity()
		}

		if currentBackend == nil {
			b, err := p.acquireBackend(clientID)
			if err != nil {
				be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006", Message: err.Error()})
				be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				_ = be.Flush()
				continue
			}
			if err := ensureSessionAndStmts(b); err != nil {
				p.releaseBackend(b, true)
				be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006", Message: err.Error()})
				be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				_ = be.Flush()
				continue
			}
			currentBackend = b
			if !inTxCounted {
				p.activeTxCount.Add(1)
				inTxCounted = true
			}
		}

		completedCycle := false
		switch m := msg.(type) {
		case *pgproto3.Query:
			m.String = rewriteSQLQuery(m.String)
			currentBackend.fe.Send(m)
			if err := currentBackend.fe.Flush(); err != nil {
				p.releaseBackend(currentBackend, true)
				currentBackend = nil
				if inTxCounted {
					p.activeTxCount.Add(-1)
					inTxCounted = false
				}
				continue
			}
			var errB error
			txStatus, errB = flushPoolBackendReplies(currentBackend, be, true)
			if errB != nil {
				p.releaseBackend(currentBackend, true)
				currentBackend = nil
				txStatus = 'I'
				if inTxCounted {
					p.activeTxCount.Add(-1)
					inTxCounted = false
				}
				continue
			}
			completedCycle = true

		case *pgproto3.Parse:
			rewrittenQuery := rewriteSQLQuery(m.Query)
			cName := m.Name
			gName := fmt.Sprintf("__air_ext_%d", p.stmtSeq.Add(1))
			if existing, ok := clientStmts[cName]; ok && cName != "" {
				gName = existing.GlobalName
			} else {
				clientStmts[cName] = &preparedDef{
					ClientName: cName,
					GlobalName: gName,
					Query:      rewrittenQuery,
					ParamOIDs:  append([]uint32(nil), m.ParameterOIDs...),
					IsSQLPrep:  false,
				}
			}
			if currentBackend.preparedStmts == nil {
				currentBackend.preparedStmts = make(map[string]bool)
			}
			currentBackend.preparedStmts[gName] = true
			currentBackend.fe.Send(&pgproto3.Parse{
				Name:          gName,
				Query:         rewrittenQuery,
				ParameterOIDs: m.ParameterOIDs,
			})

		case *pgproto3.Bind:
			stmtName := m.PreparedStatement
			if def, ok := clientStmts[stmtName]; ok {
				stmtName = def.GlobalName
			}
			currentBackend.fe.Send(&pgproto3.Bind{
				DestinationPortal:    m.DestinationPortal,
				PreparedStatement:    stmtName,
				ParameterFormatCodes: m.ParameterFormatCodes,
				Parameters:           m.Parameters,
				ResultFormatCodes:    m.ResultFormatCodes,
			})

		case *pgproto3.Describe:
			name := m.Name
			if m.ObjectType == 'S' {
				if def, ok := clientStmts[name]; ok {
					name = def.GlobalName
				}
			}
			currentBackend.fe.Send(&pgproto3.Describe{
				ObjectType: m.ObjectType,
				Name:       name,
			})

		case *pgproto3.Execute:
			currentBackend.fe.Send(m)

		case *pgproto3.Close:
			name := m.Name
			if m.ObjectType == 'S' && name != "" {
				if def, ok := clientStmts[name]; ok {
					name = def.GlobalName
					delete(clientStmts, m.Name)
					if currentBackend.preparedStmts != nil {
						delete(currentBackend.preparedStmts, name)
					}
				}
			}
			currentBackend.fe.Send(&pgproto3.Close{
				ObjectType: m.ObjectType,
				Name:       name,
			})

		case *pgproto3.Flush:
			currentBackend.fe.Send(m)
			if err := currentBackend.fe.Flush(); err != nil {
				p.releaseBackend(currentBackend, true)
				currentBackend = nil
				if inTxCounted {
					p.activeTxCount.Add(-1)
					inTxCounted = false
				}
				continue
			}
			if err := flushPoolMidPipelineReplies(currentBackend, be); err != nil {
				p.releaseBackend(currentBackend, true)
				currentBackend = nil
				if inTxCounted {
					p.activeTxCount.Add(-1)
					inTxCounted = false
				}
				continue
			}

		case *pgproto3.Sync:
			currentBackend.fe.Send(m)
			if err := currentBackend.fe.Flush(); err != nil {
				p.releaseBackend(currentBackend, true)
				currentBackend = nil
				if inTxCounted {
					p.activeTxCount.Add(-1)
					inTxCounted = false
				}
				continue
			}
			var errB error
			txStatus, errB = flushPoolBackendReplies(currentBackend, be, true)
			if errB != nil {
				p.releaseBackend(currentBackend, true)
				currentBackend = nil
				txStatus = 'I'
				if inTxCounted {
					p.activeTxCount.Add(-1)
					inTxCounted = false
				}
				continue
			}
			completedCycle = true
		}

		// Transaction-pooling unpin: release backend back to pool only after ReadyForQuery ('I') when session is not pinned.
		if completedCycle && txStatus == 'I' && !sessionPinned && currentBackend != nil {
			p.releaseBackend(currentBackend, false)
			currentBackend = nil
			if inTxCounted {
				p.activeTxCount.Add(-1)
				inTxCounted = false
			}
		}
	}
}

func flushPoolMidPipelineReplies(b *backendConn, be *pgproto3.Backend) error {
	for {
		_ = b.conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		msg, err := b.fe.Receive()
		_ = b.conn.SetReadDeadline(time.Time{})
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				return be.Flush()
			}
			return err
		}
		be.Send(msg)
		if err := be.Flush(); err != nil {
			return err
		}
	}
}

func flushPoolBackendReplies(b *backendConn, be *pgproto3.Backend, waitForReady bool) (byte, error) {
	txStatus := byte('I')
	for {
		msg, err := b.fe.Receive()
		if err != nil {
			return 'I', err
		}
		be.Send(msg)
		if _, ok := msg.(*pgproto3.CopyInResponse); ok {
			if err := be.Flush(); err != nil {
				return 'I', err
			}
			for {
				cMsg, cErr := be.Receive()
				if cErr != nil {
					return 'I', cErr
				}
				switch cm := cMsg.(type) {
				case *pgproto3.CopyData:
					b.fe.Send(cm)
				case *pgproto3.CopyDone:
					b.fe.Send(cm)
					if err := b.fe.Flush(); err != nil {
						return 'I', err
					}
					goto copyFinished
				case *pgproto3.CopyFail:
					b.fe.Send(cm)
					if err := b.fe.Flush(); err != nil {
						return 'I', err
					}
					goto copyFinished
				}
			}
		copyFinished:
			continue
		}
		if rfq, ok := msg.(*pgproto3.ReadyForQuery); ok {
			txStatus = rfq.TxStatus
			if err := be.Flush(); err != nil {
				return txStatus, err
			}
			if waitForReady {
				return txStatus, nil
			}
		}
	}
}

func execSimpleBackend(b *backendConn, sql string) error {
	b.fe.Send(&pgproto3.Query{String: sql})
	if err := b.fe.Flush(); err != nil {
		return err
	}
	return drainUntilReadyBackend(b)
}

func drainUntilReadyBackend(b *backendConn) error {
	for {
		msg, err := b.fe.Receive()
		if err != nil {
			return err
		}
		if errMsg, ok := msg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("backend error (%s): %s", errMsg.Code, errMsg.Message)
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			return nil
		}
	}
}
