package wire

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"air-stage4/pkg/cell"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

var (
	ErrDrainVetoed  = errors.New("drain vetoed: active 2PC prepared transactions or session advisory locks")
	ErrDrainTimeout = errors.New("drain timeout: active transaction exceeded deadline")
	reSetStmt       = regexp.MustCompile(`(?i)^\s*SET\s+(?:SESSION\s+)?([a-zA-Z0-9_.]+)\s*(?:=|TO)\s*(.+?)\s*;?\s*$`)
	rePrepareStmt   = regexp.MustCompile(`(?i)^\s*PREPARE\s+([a-zA-Z0-9_]+)(\s*\([^)]*\))?\s+AS\s+(.+?)\s*;?\s*$`)
	reDeallocStmt   = regexp.MustCompile(`(?i)^\s*DEALLOCATE\s+(?:PREPARE\s+)?([a-zA-Z0-9_]+)\s*;?\s*$`)
	reExecuteStmt   = regexp.MustCompile(`(?i)^(\s*EXECUTE\s+)([a-zA-Z0-9_]+)(\s*(?:\([^)]*\))?\s*;?\s*)$`)
)

type PreparedDef struct {
	ClientName string
	GlobalName string
	TypeList   string
	Query      string
	ParamOIDs  []uint32
	IsSQLPrep  bool
}

type BackendConn struct {
	conn net.Conn
	fe   *pgproto3.Frontend
}

func (b *BackendConn) Close() {
	if b != nil && b.conn != nil {
		_ = b.conn.Close()
	}
}

type Virtualizer struct {
	mu             sync.Mutex
	listenAddr     string
	listener       net.Listener
	backendSockDir string
	backendPort    int
	backendUser    string
	backendDB      string
	expectedPass   string

	idlePool []*BackendConn
	stmtSeq  atomic.Uint64

	drainReqCh  chan chan error
	activeConns sync.WaitGroup
}

func NewVirtualizer(listenAddr, backendSockDir string, backendPort int, backendUser, backendDB, expectedPass string) (*Virtualizer, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	v := &Virtualizer{
		listenAddr:     ln.Addr().String(),
		listener:       ln,
		backendSockDir: backendSockDir,
		backendPort:    backendPort,
		backendUser:    backendUser,
		backendDB:      backendDB,
		expectedPass:   expectedPass,
		drainReqCh:     make(chan chan error, 8),
	}
	go v.acceptLoop()
	return v, nil
}

func (v *Virtualizer) Addr() string {
	return v.listenAddr
}

func (v *Virtualizer) Close() {
	_ = v.listener.Close()
	v.mu.Lock()
	for _, b := range v.idlePool {
		b.Close()
	}
	v.idlePool = nil
	v.mu.Unlock()
}

func (v *Virtualizer) SetBackend(sockDir string, port int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, b := range v.idlePool {
		b.Close()
	}
	v.idlePool = nil
	v.backendSockDir = sockDir
	v.backendPort = port
}

// checkBackendUnmigratableState queries pg_prepared_xacts and session-level advisory locks directly on the backend.
func (v *Virtualizer) checkBackendUnmigratableState(ctx context.Context) (int, error) {
	v.mu.Lock()
	port := v.backendPort
	db := v.backendDB
	v.mu.Unlock()

	dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/%s?sslmode=disable", port, db)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)

	var count int
	err = conn.QueryRow(ctx, `
		SELECT
			(SELECT count(*)::int FROM pg_prepared_xacts) +
			(SELECT count(*)::int FROM pg_locks WHERE locktype = 'advisory' AND granted)
	`).Scan(&count)
	return count, err
}

func (v *Virtualizer) RequestDrain(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	unmigratable, err := v.checkBackendUnmigratableState(ctx)
	if err == nil && unmigratable > 0 {
		return fmt.Errorf("%w (count=%d)", ErrDrainVetoed, unmigratable)
	}

	reply := make(chan error, 1)
	v.drainReqCh <- reply
	select {
	case err := <-reply:
		return err
	case <-time.After(timeout):
		return ErrDrainTimeout
	}
}

func (v *Virtualizer) acquireBackend() (*BackendConn, error) {
	v.mu.Lock()
	if len(v.idlePool) > 0 {
		b := v.idlePool[len(v.idlePool)-1]
		v.idlePool = v.idlePool[:len(v.idlePool)-1]
		v.mu.Unlock()
		return b, nil
	}
	sockDir := v.backendSockDir
	port := v.backendPort
	user := v.backendUser
	db := v.backendDB
	v.mu.Unlock()

	var c net.Conn
	var err error
	if sockDir != "" {
		sockPath := filepath.Join(sockDir, fmt.Sprintf(".s.PGSQL.%d", port))
		c, err = net.DialTimeout("unix", sockPath, 5*time.Second)
	} else {
		c, err = net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
	}
	if err != nil {
		return nil, err
	}
	fe := pgproto3.NewFrontend(c, c)
	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user":             user,
			"database":         db,
			"application_name": "air-stage4-virtualizer",
		},
	}
	fe.Send(startup)
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
		case *pgproto3.ParameterStatus, *pgproto3.BackendKeyData:
		case *pgproto3.ReadyForQuery:
			return &BackendConn{conn: c, fe: fe}, nil
		case *pgproto3.ErrorResponse:
			_ = c.Close()
			return nil, fmt.Errorf("backend startup error: %s", m.Message)
		}
	}
}

func (v *Virtualizer) releaseBackend(b *BackendConn) {
	if b == nil {
		return
	}
	v.mu.Lock()
	v.idlePool = append(v.idlePool, b)
	v.mu.Unlock()
}

func (v *Virtualizer) acceptLoop() {
	for {
		c, err := v.listener.Accept()
		if err != nil {
			return
		}
		v.activeConns.Add(1)
		go func(conn net.Conn) {
			defer v.activeConns.Done()
			v.handleClient(conn)
		}(c)
	}
}

func (v *Virtualizer) handleClient(clientConn net.Conn) {
	defer clientConn.Close()
	be := pgproto3.NewBackend(clientConn, clientConn)

	startupMsg, err := be.ReceiveStartupMessage()
	if err != nil {
		return
	}
	if _, isSSL := startupMsg.(*pgproto3.SSLRequest); isSSL {
		_, _ = clientConn.Write([]byte{'N'})
		startupMsg, err = be.ReceiveStartupMessage()
		if err != nil {
			return
		}
	}
	if _, ok := startupMsg.(*pgproto3.StartupMessage); !ok {
		return
	}

	// Enforce client password authentication (S4-RISK-08 remediation)
	if v.expectedPass != "" {
		be.Send(&pgproto3.AuthenticationCleartextPassword{})
		if err := be.Flush(); err != nil {
			return
		}
		msg, err := be.Receive()
		if err != nil {
			return
		}
		pwMsg, ok := msg.(*pgproto3.PasswordMessage)
		if !ok || pwMsg.Password != v.expectedPass {
			be.Send(&pgproto3.ErrorResponse{
				Severity: "FATAL",
				Code:     "28P01",
				Message:  "password authentication failed for user",
			})
			_ = be.Flush()
			return
		}
	}

	be.Send(&pgproto3.AuthenticationOk{})
	be.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "17.0"})
	be.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	be.Send(&pgproto3.BackendKeyData{ProcessID: 4242, SecretKey: 9999})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := be.Flush(); err != nil {
		return
	}

	sessionVars := make(map[string]string)
	clientStmts := make(map[string]*PreparedDef)
	backendStmts := make(map[*BackendConn]map[string]bool)

	var currentBackend *BackendConn
	txStatus := byte('I')

	ensureSessionAndStmts := func(b *BackendConn) error {
		if len(sessionVars) > 0 {
			var sb strings.Builder
			for k, val := range sessionVars {
				sb.WriteString(fmt.Sprintf("SET %s = %s; ", k, val))
			}
			if err := execSimpleInternal(b, sb.String()); err != nil {
				return err
			}
		}
		preparedOnB := backendStmts[b]
		if preparedOnB == nil {
			preparedOnB = make(map[string]bool)
			backendStmts[b] = preparedOnB
		}
		for _, def := range clientStmts {
			if preparedOnB[def.GlobalName] {
				continue
			}
			if def.IsSQLPrep {
				if err := execSimpleInternal(b, fmt.Sprintf("PREPARE %s%s AS %s", def.GlobalName, def.TypeList, def.Query)); err != nil {
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
				if err := drainUntilReadyInternal(b); err != nil {
					return err
				}
			}
			preparedOnB[def.GlobalName] = true
		}
		return nil
	}

	for {
		if txStatus == 'I' {
			select {
			case reply := <-v.drainReqCh:
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				cnt, err := v.checkBackendUnmigratableState(ctx)
				cancel()
				if err == nil && cnt > 0 {
					reply <- fmt.Errorf("%w (count=%d)", ErrDrainVetoed, cnt)
				} else {
					if currentBackend != nil {
						currentBackend.Close()
						currentBackend = nil
					}
					v.mu.Lock()
					for _, b := range v.idlePool {
						b.Close()
					}
					v.idlePool = nil
					v.mu.Unlock()
					reply <- nil
				}
			default:
			}
		}

		_ = clientConn.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
		msg, err := be.Receive()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			if currentBackend != nil {
				currentBackend.Close()
			}
			return
		}
		_ = clientConn.SetReadDeadline(time.Time{})

		if _, ok := msg.(*pgproto3.Terminate); ok {
			if currentBackend != nil {
				v.releaseBackend(currentBackend)
			}
			return
		}

		if currentBackend == nil {
			b, err := v.acquireBackend()
			if err != nil {
				be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006", Message: err.Error()})
				be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				_ = be.Flush()
				continue
			}
			if err := ensureSessionAndStmts(b); err != nil {
				b.Close()
				be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006", Message: err.Error()})
				be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				_ = be.Flush()
				continue
			}
			currentBackend = b
		}

		rewriteSQLQuery := func(sqlStr string) string {
			qTrim := strings.TrimSpace(sqlStr)
			qUpper := strings.ToUpper(strings.TrimSuffix(qTrim, ";"))

			// Handle DISCARD ALL / RESET ALL / DEALLOCATE ALL / DEALLOCATE <name> (S4-RISK-08 remediation)
			if qUpper == "DISCARD ALL" || qUpper == "RESET ALL" {
				for k := range sessionVars {
					delete(sessionVars, k)
				}
				if qUpper == "DISCARD ALL" {
					for k := range clientStmts {
						delete(clientStmts, k)
					}
					delete(backendStmts, currentBackend)
				}
				return sqlStr
			}
			if qUpper == "DEALLOCATE ALL" || qUpper == "DEALLOCATE PREPARE ALL" {
				for k := range clientStmts {
					delete(clientStmts, k)
				}
				delete(backendStmts, currentBackend)
				return sqlStr
			}
			if dm := reDeallocStmt.FindStringSubmatch(sqlStr); dm != nil {
				cName := strings.ToLower(dm[1])
				if def, ok := clientStmts[cName]; ok {
					delete(clientStmts, cName)
					if backendStmts[currentBackend] != nil {
						delete(backendStmts[currentBackend], def.GlobalName)
					}
					return fmt.Sprintf("DEALLOCATE %s", def.GlobalName)
				}
				return sqlStr
			}
			if sm := reSetStmt.FindStringSubmatch(sqlStr); sm != nil {
				sessionVars[strings.ToLower(sm[1])] = sm[2]
				return sqlStr
			}
			if pm := rePrepareStmt.FindStringSubmatch(sqlStr); pm != nil {
				cName := strings.ToLower(pm[1])
				typeList := pm[2]
				body := pm[3]
				gName := fmt.Sprintf("__air_s4_p_%d", v.stmtSeq.Add(1))
				def := &PreparedDef{
					ClientName: cName,
					GlobalName: gName,
					TypeList:   typeList,
					Query:      body,
					IsSQLPrep:  true,
				}
				clientStmts[cName] = def
				if backendStmts[currentBackend] == nil {
					backendStmts[currentBackend] = make(map[string]bool)
				}
				backendStmts[currentBackend][gName] = true
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

		switch m := msg.(type) {
		case *pgproto3.Query:
			m.String = rewriteSQLQuery(m.String)

			currentBackend.fe.Send(m)
			if err := currentBackend.fe.Flush(); err != nil {
				currentBackend.Close()
				currentBackend = nil
				continue
			}
			var errB error
			txStatus, errB = flushBackendReplies(currentBackend, be, true)
			if errB != nil {
				currentBackend.Close()
				currentBackend = nil
				txStatus = 'I'
				continue
			}

		case *pgproto3.Parse:
			rewrittenQuery := rewriteSQLQuery(m.Query)
			cName := m.Name
			gName := cName
			if cName != "" {
				if existing, ok := clientStmts[cName]; ok {
					gName = existing.GlobalName
				} else {
					gName = fmt.Sprintf("__air_s4_ext_%d", v.stmtSeq.Add(1))
					clientStmts[cName] = &PreparedDef{
						ClientName: cName,
						GlobalName: gName,
						Query:      rewrittenQuery,
						ParamOIDs:  append([]uint32(nil), m.ParameterOIDs...),
						IsSQLPrep:  false,
					}
				}
				if backendStmts[currentBackend] == nil {
					backendStmts[currentBackend] = make(map[string]bool)
				}
				backendStmts[currentBackend][gName] = true
			}
			currentBackend.fe.Send(&pgproto3.Parse{
				Name:          gName,
				Query:         rewrittenQuery,
				ParameterOIDs: m.ParameterOIDs,
			})

		case *pgproto3.Bind:
			stmtName := m.PreparedStatement
			if stmtName != "" {
				if def, ok := clientStmts[stmtName]; ok {
					stmtName = def.GlobalName
				}
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
			if m.ObjectType == 'S' && name != "" {
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
					if backendStmts[currentBackend] != nil {
						delete(backendStmts[currentBackend], name)
					}
				}
			}
			currentBackend.fe.Send(&pgproto3.Close{
				ObjectType: m.ObjectType,
				Name:       name,
			})

		case *pgproto3.Flush:
			// Mid-pipeline Flush ('H'): send Flush to backend and forward available replies without waiting for ReadyForQuery!
			currentBackend.fe.Send(m)
			if err := currentBackend.fe.Flush(); err != nil {
				currentBackend.Close()
				currentBackend = nil
				continue
			}
			if err := flushMidPipelineReplies(currentBackend, be); err != nil {
				currentBackend.Close()
				currentBackend = nil
				continue
			}

		case *pgproto3.Sync:
			currentBackend.fe.Send(m)
			if err := currentBackend.fe.Flush(); err != nil {
				currentBackend.Close()
				currentBackend = nil
				continue
			}
			var errB error
			txStatus, errB = flushBackendReplies(currentBackend, be, true)
			if errB != nil {
				currentBackend.Close()
				currentBackend = nil
				txStatus = 'I'
				continue
			}
		}

		// Transaction-pooling unpin check: release when idle ('I')
		if txStatus == 'I' && currentBackend != nil {
			v.releaseBackend(currentBackend)
			currentBackend = nil
		}
	}
}

// flushMidPipelineReplies reads responses triggered by a mid-batch Flush ('H') without blocking on ReadyForQuery.
func flushMidPipelineReplies(b *BackendConn, be *pgproto3.Backend) error {
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

func flushBackendReplies(b *BackendConn, be *pgproto3.Backend, waitForReady bool) (byte, error) {
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
			// Bidirectional COPY FROM STDIN loop: read CopyData / CopyDone / CopyFail from client and forward to backend!
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

func execSimpleInternal(b *BackendConn, sql string) error {
	b.fe.Send(&pgproto3.Query{String: sql})
	if err := b.fe.Flush(); err != nil {
		return err
	}
	return drainUntilReadyInternal(b)
}

func drainUntilReadyInternal(b *BackendConn) error {
	for {
		msg, err := b.fe.Receive()
		if err != nil {
			return err
		}
		if errMsg, ok := msg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("backend error: %s", errMsg.Message)
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			return nil
		}
	}
}

type Workstream8Report struct {
	Pass                       bool   `json:"pass"`
	WrongPasswordRejected28P01 bool   `json:"wrong_password_rejected_28p01"`
	BackendUserLeastPrivilege  string `json:"backend_user_least_privilege"`
	BackendUserIsSuperuser     bool   `json:"backend_user_is_superuser"`
	CopyFromStdinRowsStreamed  int    `json:"copy_from_stdin_rows_streamed"`
	MidPipelineFlushSucceeded  bool   `json:"mid_pipeline_flush_succeeded"`
	DiscardAllClearedState     bool   `json:"discard_all_cleared_state"`
	DeallocateRePrepareSuccess bool   `json:"deallocate_reprepare_success"`
	DrainVetoedOnOpen2PC       bool   `json:"drain_vetoed_on_open_2pc"`
	DrainSucceededAfter2PC     bool   `json:"drain_succeeded_after_2pc"`
}

func RunWorkstream8WireAndDrain(ctx context.Context, agent *cell.Agent) (Workstream8Report, error) {
	var res Workstream8Report
	tenant := "t_ws8_wire"
	if _, err := agent.ClaimCluster(tenant); err != nil {
		return res, err
	}
	rt, err := agent.ActivateCluster(tenant, "s4-secret-pass", 1)
	if err != nil {
		return res, err
	}
	defer func() { _ = agent.StopAndPurgeTenant(tenant) }()

	virt, err := NewVirtualizer("127.0.0.1:0", rt.SockDir, rt.Port, "app_user", "postgres", "s4-secret-pass")
	if err != nil {
		return res, err
	}
	defer virt.Close()

	// 1. Test wrong password -> must reject with 28P01
	badDSN := fmt.Sprintf("postgres://app_user:wrong-pass@%s/postgres?sslmode=disable", virt.Addr())
	badConn, err := pgx.Connect(ctx, badDSN)
	if err != nil && strings.Contains(err.Error(), "28P01") {
		res.WrongPasswordRejected28P01 = true
	} else if badConn != nil {
		_ = badConn.Close(ctx)
	}

	// 2. Connect with valid password and verify least-privilege backend role
	goodDSN := fmt.Sprintf("postgres://app_user:s4-secret-pass@%s/postgres?sslmode=disable", virt.Addr())
	conn, err := pgx.Connect(ctx, goodDSN)
	if err != nil {
		return res, fmt.Errorf("connect virtualizer with valid password: %w", err)
	}
	defer conn.Close(ctx)

	if err := conn.QueryRow(ctx, "SELECT current_user, usesuper FROM pg_user WHERE usename = current_user").Scan(&res.BackendUserLeastPrivilege, &res.BackendUserIsSuperuser); err != nil {
		return res, fmt.Errorf("check backend user: %w", err)
	}

	// 3. Test COPY FROM STDIN streaming 500 rows through Virtualizer
	if _, err := conn.Exec(ctx, "CREATE TABLE copy_stream_test (id int, label text)"); err != nil {
		return res, fmt.Errorf("create copy_stream_test: %w", err)
	}
	rows := make([][]any, 500)
	for i := 0; i < 500; i++ {
		rows[i] = []any{i + 1, fmt.Sprintf("row-%d", i+1)}
	}
	copied, err := conn.CopyFrom(ctx, pgx.Identifier{"copy_stream_test"}, []string{"id", "label"}, pgx.CopyFromRows(rows))
	if err != nil {
		return res, fmt.Errorf("CopyFrom through virtualizer failed: %w", err)
	}
	res.CopyFromStdinRowsStreamed = int(copied)

	// 4. Test mid-pipeline Flush ('H') before Sync ('S') on raw TCP connection
	rawConn, err := net.DialTimeout("tcp", virt.Addr(), 3*time.Second)
	if err != nil {
		return res, err
	}
	defer rawConn.Close()
	rawFE := pgproto3.NewFrontend(rawConn, rawConn)
	rawFE.Send(&pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "app_user", "database": "postgres"},
	})
	_ = rawFE.Flush()
	for {
		m, err := rawFE.Receive()
		if err != nil {
			return res, err
		}
		if _, ok := m.(*pgproto3.AuthenticationCleartextPassword); ok {
			rawFE.Send(&pgproto3.PasswordMessage{Password: "s4-secret-pass"})
			_ = rawFE.Flush()
		}
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	// Send Parse + Describe + Flush (WITHOUT Sync!) and verify Describe replies arrive without deadlock
	rawFE.Send(&pgproto3.Parse{Name: "pipe_s1", Query: "SELECT $1::int + 10"})
	rawFE.Send(&pgproto3.Describe{ObjectType: 'S', Name: "pipe_s1"})
	rawFE.Send(&pgproto3.Flush{})
	if err := rawFE.Flush(); err != nil {
		return res, err
	}
	_ = rawConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	gotParseComplete := false
	gotRowDesc := false
	for !(gotParseComplete && gotRowDesc) {
		m, err := rawFE.Receive()
		if err != nil {
			return res, fmt.Errorf("mid-pipeline Flush stalled: %w", err)
		}
		switch m.(type) {
		case *pgproto3.ParseComplete:
			gotParseComplete = true
		case *pgproto3.RowDescription:
			gotRowDesc = true
		}
	}
	rawFE.Send(&pgproto3.Sync{})
	_ = rawFE.Flush()
	for {
		m, err := rawFE.Receive()
		if err != nil {
			return res, err
		}
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	_ = rawConn.SetReadDeadline(time.Time{})
	res.MidPipelineFlushSucceeded = gotParseComplete && gotRowDesc

	// 5. Test SET + DISCARD ALL clearing session state
	if _, err := conn.Exec(ctx, "SET work_mem = '8MB'"); err != nil {
		return res, err
	}
	if _, err := conn.Exec(ctx, "DISCARD ALL"); err != nil {
		return res, err
	}
	var wm string
	if err := conn.QueryRow(ctx, "SHOW work_mem").Scan(&wm); err != nil {
		return res, err
	}
	res.DiscardAllClearedState = (wm == "4MB")

	// 6. Test PREPARE -> DEALLOCATE -> re-PREPARE with same statement name
	if _, err := conn.Exec(ctx, "PREPARE s1 (int) AS SELECT $1 + 1"); err != nil {
		return res, fmt.Errorf("first PREPARE s1: %w", err)
	}
	if _, err := conn.Exec(ctx, "DEALLOCATE s1"); err != nil {
		return res, fmt.Errorf("DEALLOCATE s1: %w", err)
	}
	if _, err := conn.Exec(ctx, "PREPARE s1 (text) AS SELECT upper($1)"); err != nil {
		return res, fmt.Errorf("re-PREPARE s1: %w", err)
	}
	var upOut string
	if err := conn.QueryRow(ctx, "EXECUTE s1('reprepared_ok')").Scan(&upOut); err != nil {
		return res, fmt.Errorf("EXECUTE re-prepared s1: %w", err)
	}
	res.DeallocateRePrepareSuccess = (upOut == "REPREPARED_OK")

	// 7. Test 2PC prepared transaction drain veto and recovery
	if _, err := conn.Exec(ctx, "BEGIN; INSERT INTO copy_stream_test VALUES (999, '2pc'); PREPARE TRANSACTION 'tx_2pc_guard';"); err != nil {
		return res, fmt.Errorf("PREPARE TRANSACTION: %w", err)
	}
	if err := virt.RequestDrain(2 * time.Second); errors.Is(err, ErrDrainVetoed) {
		res.DrainVetoedOnOpen2PC = true
	}
	if _, err := conn.Exec(ctx, "COMMIT PREPARED 'tx_2pc_guard'"); err != nil {
		return res, fmt.Errorf("COMMIT PREPARED: %w", err)
	}
	if err := virt.RequestDrain(2 * time.Second); err == nil {
		res.DrainSucceededAfter2PC = true
	}

	res.Pass = res.WrongPasswordRejected28P01 &&
		res.BackendUserLeastPrivilege == "app_user" &&
		!res.BackendUserIsSuperuser &&
		res.CopyFromStdinRowsStreamed == 500 &&
		res.MidPipelineFlushSucceeded &&
		res.DiscardAllClearedState &&
		res.DeallocateRePrepareSuccess &&
		res.DrainVetoedOnOpen2PC &&
		res.DrainSucceededAfter2PC

	return res, nil
}
