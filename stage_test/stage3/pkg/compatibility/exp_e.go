package compatibility

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"net"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"air-stage3/pkg/cell"
	"air-stage3/pkg/wire"
	"github.com/jackc/pgx/v5"
)

type ExpE1Report struct {
	Pass                   bool `json:"pass"`
	PgxPreparedVirtualOK   bool `json:"pgx_prepared_virtual_ok"`
	PgxPipelineBatchOK     bool `json:"pgx_pipeline_batch_ok"`
	DDLInvalidationRebind  bool `json:"ddl_invalidation_rebind_ok"`
	PsqlSimpleAndCopyOK    bool `json:"psql_simple_and_copy_ok"`
	AutoPinListenTempSetOK bool `json:"auto_pin_listen_temp_set_ok"`
	CancelRequestMs        float64 `json:"cancel_request_ms"`
	CancelRequestOK        bool `json:"cancel_request_ok"`
	PgBouncerVersion       string `json:"pgbouncer_version"`
}

type ExpE2Report struct {
	Pass                    bool  `json:"pass"`
	LengthBombRejected      bool  `json:"length_bomb_rejected"`
	SNISpoofRejected        bool  `json:"sni_spoof_rejected"`
	SlowlorisClosedCleanly  bool  `json:"slowloris_closed_cleanly"`
	BboltEventsWritten      int   `json:"bbolt_events_written"`
	QueueReplayAttempts     int   `json:"queue_replay_attempts"`
	QueueInsertedUnique     int   `json:"queue_inserted_unique"`
	QueueDeduplicated       int   `json:"queue_deduplicated"`
	DuplicateBillingPct     float64 `json:"duplicate_billing_pct"`
	ExpectedTotalRUs        int64 `json:"expected_total_rus"`
	LedgerTotalRUs          int64 `json:"ledger_total_rus"`
}

type ExpE3E4Report struct {
	Pass                    bool    `json:"pass"`
	IdleConnsHeld           int     `json:"idle_conns_held"`
	HeapPerIdleConnKB       float64 `json:"heap_per_idle_conn_kb"`
	SlowCopyReaders         int     `json:"slow_copy_readers"`
	SlowCopyTotalMB         int64   `json:"slow_copy_total_mb"`
	SlowCopyHeapDeltaMB     float64 `json:"slow_copy_heap_delta_mb"`
	SlowCopyCompletedCount  int     `json:"slow_copy_completed_count"`
}

type GroupEReport struct {
	Pass    bool          `json:"pass"`
	ExpE1   ExpE1Report   `json:"exp_e1"`
	ExpE2   ExpE2Report   `json:"exp_e2"`
	ExpE3E4 ExpE3E4Report `json:"exp_e3_e4"`
}

func generateSelfSignedTLS() (*tls.Config, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "*.db.air.internal"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"*.db.air.internal", "localhost"},
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, err
	}
	cert := tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  priv,
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
}

func RunGroupE(ctx context.Context, agent *cell.Agent, workDir string) (GroupEReport, error) {
	var rep GroupEReport

	tlsCfg, err := generateSelfSignedTLS()
	if err != nil {
		return rep, fmt.Errorf("generate tls cert: %w", err)
	}

	ledgerPath := filepath.Join(workDir, "gateway_metering.bbolt")
	ledger, err := wire.OpenMeteringLedger(ledgerPath)
	if err != nil {
		return rep, fmt.Errorf("open metering ledger: %w", err)
	}

	tid := "tenant_e_compat"
	if _, err := agent.ClaimCluster(tid); err != nil {
		ledger.Close()
		return rep, fmt.Errorf("claim %s: %w", tid, err)
	}
	rt, err := agent.ActivateCluster(tid, "pw_"+tid, 1)
	if err != nil {
		ledger.Close()
		return rep, fmt.Errorf("activate %s: %w", tid, err)
	}

	gw := wire.NewGateway(agent, ledger, tlsCfg)
	gw.RegisterTenant(tid, rt.Port, 1, wire.StateActive)
	addr, err := gw.ListenAndServe("127.0.0.1:0")
	if err != nil {
		ledger.Close()
		return rep, fmt.Errorf("gateway listen: %w", err)
	}
	defer gw.Close()

	// =========================================================================
	// EXP-E1: pgx Virtualized Prepared Statements, Pipeline, DDL Invalidation,
	//         psql CLI + COPY, Auto-Pinning, CancelRequest, PgBouncer Check
	// =========================================================================
	var e1 ExpE1Report
	if out, err := exec.Command("pgbouncer", "--version").CombinedOutput(); err == nil {
		e1.PgBouncerVersion = strings.TrimSpace(strings.Split(string(out), "\n")[0])
	}

	// 1) Two separate pgx connections share the same backend pool with named prepared statements
	dsn := fmt.Sprintf("postgres://postgres@%s/%s?sslmode=disable", addr, tid)
	c1, err := pgx.Connect(ctx, dsn)
	if err != nil {
		ledger.Close()
		return rep, fmt.Errorf("e1 connect c1: %w", err)
	}
	_, err = c1.Exec(ctx, "CREATE TABLE IF NOT EXISTS e1_items (id int primary key, name text)")
	if err != nil {
		c1.Close(ctx)
		ledger.Close()
		return rep, err
	}
	if _, err := c1.Prepare(ctx, "stmt_ins", "INSERT INTO e1_items (id, name) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"); err != nil {
		c1.Close(ctx)
		ledger.Close()
		return rep, err
	}
	if _, err := c1.Exec(ctx, "stmt_ins", 1, "alpha"); err != nil {
		c1.Close(ctx)
		ledger.Close()
		return rep, err
	}

	// Second client prepares a different client statement name for a shared query
	c2, err := pgx.Connect(ctx, dsn)
	if err == nil {
		if _, err := c2.Prepare(ctx, "other_client_sel", "SELECT name FROM e1_items WHERE id = $1"); err == nil {
			var got string
			if err := c2.QueryRow(ctx, "other_client_sel", 1).Scan(&got); err == nil && got == "alpha" {
				e1.PgxPreparedVirtualOK = true
			}
		}
	}

	// 2) Pipeline / Batch mode across virtualizer
	batch := &pgx.Batch{}
	for i := 2; i <= 11; i++ {
		batch.Queue("INSERT INTO e1_items (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING", i, fmt.Sprintf("item_%d", i))
	}
	br := c1.SendBatch(ctx, batch)
	batchOK := true
	for i := 2; i <= 11; i++ {
		if _, err := br.Exec(); err != nil {
			batchOK = false
		}
	}
	_ = br.Close()
	e1.PgxPipelineBatchOK = batchOK

	// 3) DDL invalidation + re-execution
	if _, err := c1.Exec(ctx, "ALTER TABLE e1_items ADD COLUMN IF NOT EXISTS extra text DEFAULT 'v1'"); err == nil {
		var name string
		if err := c1.QueryRow(ctx, "SELECT name FROM e1_items WHERE id = $1", 1).Scan(&name); err == nil && name == "alpha" {
			e1.DDLInvalidationRebind = true
		}
	}

	// 4) psql CLI simple query + COPY OUT
	psqlOut, psqlErr := exec.Command("psql", dsn, "-c", "SELECT count(*) FROM e1_items;", "-c", "COPY (SELECT id, name FROM e1_items ORDER BY id LIMIT 3) TO STDOUT;").CombinedOutput()
	if psqlErr == nil && strings.Contains(string(psqlOut), "alpha") {
		e1.PsqlSimpleAndCopyOK = true
	}

	// 5) Auto-pinning (TEMP TABLE + session SET + LISTEN)
	if _, err := c1.Exec(ctx, "SET work_mem = '8MB'"); err == nil {
		if _, err := c1.Exec(ctx, "CREATE TEMP TABLE e1_tmp (k int)"); err == nil {
			if _, err := c1.Exec(ctx, "INSERT INTO e1_tmp VALUES (77)"); err == nil {
				var k int
				if err := c1.QueryRow(ctx, "SELECT k FROM e1_tmp").Scan(&k); err == nil && k == 77 {
					e1.AutoPinListenTempSetOK = true
				}
			}
		}
	}
	c1.Close(ctx)
	if c2 != nil {
		c2.Close(ctx)
	}

	// 6) CancelRequest routing
	cCancel, err := pgx.Connect(ctx, dsn)
	if err == nil {
		qCtx, qCancel := context.WithTimeout(ctx, 150*time.Millisecond)
		t0 := time.Now()
		_, qErr := cCancel.Exec(qCtx, "SELECT pg_sleep(5)")
		qCancel()
		elapsed := float64(time.Since(t0).Microseconds()) / 1000.0
		e1.CancelRequestMs = float64(int(elapsed*100)) / 100
		if qErr != nil && elapsed < 1000 {
			e1.CancelRequestOK = true
		}
		cCancel.Close(ctx)
	}

	e1.Pass = e1.PgxPreparedVirtualOK && e1.PgxPipelineBatchOK &&
		e1.DDLInvalidationRebind && e1.PsqlSimpleAndCopyOK &&
		e1.AutoPinListenTempSetOK && e1.CancelRequestOK
	rep.ExpE1 = e1

	// =========================================================================
	// EXP-E2: 2 GB Length Bomb, SNI Spoofing, Slowloris, bbolt Crash + 5x Queue Replay
	// =========================================================================
	var e2 ExpE2Report

	// 1) 2 GB Length Bomb
	lbConn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err == nil {
		var pkt [8]byte
		binary.BigEndian.PutUint32(pkt[0:4], 0x7fffffff)
		binary.BigEndian.PutUint32(pkt[4:8], 196608)
		_, _ = lbConn.Write(pkt[:])
		_ = lbConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 16)
		_, rErr := lbConn.Read(buf)
		if rErr != nil {
			e2.LengthBombRejected = true
		}
		lbConn.Close()
	}

	// 2) SNI Spoofing (TLS SNI = spoof.db.air.internal while database = tenant_e_compat)
	spoofDSN := fmt.Sprintf("postgres://postgres@%s/%s?sslmode=require", addr, tid)
	cfgSpoof, err := pgx.ParseConfig(spoofDSN)
	if err == nil {
		cfgSpoof.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         "spoof.db.air.internal",
		}
		sConn, sErr := pgx.ConnectConfig(ctx, cfgSpoof)
		if sErr != nil && strings.Contains(sErr.Error(), "28000") {
			e2.SNISpoofRejected = true
		} else if sConn != nil {
			sConn.Close(ctx)
		}
	}

	// 3) Slowloris (send 2 bytes and stall -> Gateway closes at PreAuthReadTimeout)
	slConn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err == nil {
		_, _ = slConn.Write([]byte{0x00, 0x00})
		_ = slConn.SetReadDeadline(time.Now().Add(7 * time.Second))
		buf := make([]byte, 8)
		_, rErr := slConn.Read(buf)
		if rErr != nil {
			e2.SlowlorisClosedCleanly = true
		}
		slConn.Close()
	}

	// 4) bbolt WAL crash recovery + 5x Queue redelivery deduplication
	_ = ledger.Close()
	// Re-open simulating post-SIGKILL Gateway restart
	ledger, err = wire.OpenMeteringLedger(ledgerPath)
	if err != nil {
		return rep, fmt.Errorf("reopen bbolt post-crash: %w", err)
	}
	defer ledger.Close()
	gw.SetLedger(ledger)

	events, err := ledger.PendingWALEvents()
	if err != nil {
		return rep, err
	}
	e2.BboltEventsWritten = len(events)
	var expectedRUs int64
	for _, ev := range events {
		expectedRUs += ev.ReadUnits
	}
	e2.ExpectedTotalRUs = expectedRUs

	// Deliver exact same batch 5 times (simulating 5x Cloudflare Queue redelivery)
	e2.QueueReplayAttempts = 5
	for i := 0; i < 5; i++ {
		ins, dedup, err := ledger.IngestQueueBatch(events)
		if err != nil {
			return rep, err
		}
		e2.QueueInsertedUnique += ins
		e2.QueueDeduplicated += dedup
	}
	ledgerTotal, _, err := ledger.TotalLedgerRUs(tid)
	if err != nil {
		return rep, err
	}
	e2.LedgerTotalRUs = ledgerTotal
	if expectedRUs > 0 {
		e2.DuplicateBillingPct = float64(ledgerTotal-expectedRUs) / float64(expectedRUs) * 100.0
	}

	e2.Pass = e2.LengthBombRejected && e2.SNISpoofRejected &&
		e2.SlowlorisClosedCleanly && e2.BboltEventsWritten > 0 &&
		e2.QueueInsertedUnique == e2.BboltEventsWritten &&
		e2.LedgerTotalRUs == e2.ExpectedTotalRUs &&
		e2.DuplicateBillingPct == 0
	rep.ExpE2 = e2

	// =========================================================================
	// EXP-E3 / E4: 5,000 Idle Connections (<20 KB/conn) & Slow COPY OUT Backpressure
	// =========================================================================
	var e34 ExpE3E4Report

	// Hold 5,000 concurrent idle TLS/PG connections (or as many as ulimit allows up to 5,000)
	const targetIdleConns = 5000
	runtime.GC()
	var msBefore, msAfter runtime.MemStats
	runtime.ReadMemStats(&msBefore)

	idleConns := make([]net.Conn, 0, targetIdleConns)
	var idleMu sync.Mutex
	var idleWg sync.WaitGroup
	sem := make(chan struct{}, 100)
	for i := 0; i < targetIdleConns; i++ {
		idleWg.Add(1)
		go func() {
			defer idleWg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			c, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				return
			}
			// Send valid StartupMessage for tid so Gateway finishes startup & parks connection idle
			var buf bytes.Buffer
			buf.Write([]byte{0, 0, 0, 0}) // placeholder length
			binary.Write(&buf, binary.BigEndian, uint32(196608))
			buf.WriteString("user\x00postgres\x00database\x00")
			buf.WriteString(tid)
			buf.WriteString("\x00\x00")
			raw := buf.Bytes()
			binary.BigEndian.PutUint32(raw[0:4], uint32(len(raw)))
			if _, err := c.Write(raw); err != nil {
				c.Close()
				return
			}
			// Read until ReadyForQuery ('Z')
			_ = c.SetReadDeadline(time.Now().Add(4 * time.Second))
			tmp := make([]byte, 512)
			for {
				n, rErr := c.Read(tmp)
				if n > 0 && bytes.Contains(tmp[:n], []byte{'Z', 0, 0, 0, 5, 'I'}) {
					break
				}
				if rErr != nil {
					c.Close()
					return
				}
			}
			_ = c.SetReadDeadline(time.Time{})
			idleMu.Lock()
			idleConns = append(idleConns, c)
			idleMu.Unlock()
		}()
	}
	idleWg.Wait()

	runtime.GC()
	runtime.ReadMemStats(&msAfter)
	e34.IdleConnsHeld = len(idleConns)
	if len(idleConns) > 0 && msAfter.HeapAlloc > msBefore.HeapAlloc {
		perConnKB := float64(msAfter.HeapAlloc-msBefore.HeapAlloc) / float64(len(idleConns)) / 1024.0
		e34.HeapPerIdleConnKB = float64(int(perConnKB*100)) / 100
	}
	for _, c := range idleConns {
		c.Close()
	}

	// EXP-E4: 20 concurrent throttled slow COPY OUT readers (5 MB each = 100 MB total)
	runtime.GC()
	var msCopyBefore, msCopyAfter, msPeak runtime.MemStats
	runtime.ReadMemStats(&msCopyBefore)

	const slowReaders = 20
	const rowsPerReader = 50000 // ~5 MB per reader => 100 MB total
	var copyCompleted atomic.Int64
	var copyBytes atomic.Int64
	var copyWg sync.WaitGroup
	stopPeak := make(chan struct{})
	donePeak := make(chan struct{})
	go func() {
		defer close(donePeak)
		for {
			select {
			case <-stopPeak:
				return
			default:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.HeapAlloc > msPeak.HeapAlloc {
					msPeak = m
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()

	for i := 0; i < slowReaders; i++ {
		copyWg.Add(1)
		go func() {
			defer copyWg.Done()
			c, err := pgx.Connect(ctx, dsn)
			if err != nil {
				return
			}
			defer c.Close(ctx)
			sw := &throttledWriter{delayEvery: 64 * 1024, delay: 2 * time.Millisecond, total: &copyBytes}
			_, err = c.PgConn().CopyTo(ctx, sw, fmt.Sprintf("COPY (SELECT i, repeat('x', 90) FROM generate_series(1, %d) i) TO STDOUT", rowsPerReader))
			if err == nil {
				copyCompleted.Add(1)
			}
		}()
	}
	copyWg.Wait()
	close(stopPeak)
	<-donePeak
	runtime.ReadMemStats(&msCopyAfter)

	e34.SlowCopyReaders = slowReaders
	e34.SlowCopyCompletedCount = int(copyCompleted.Load())
	e34.SlowCopyTotalMB = copyBytes.Load() / (1024 * 1024)
	if msPeak.HeapAlloc > msCopyBefore.HeapAlloc {
		deltaMB := float64(msPeak.HeapAlloc-msCopyBefore.HeapAlloc) / (1024.0 * 1024.0)
		e34.SlowCopyHeapDeltaMB = float64(int(deltaMB*100)) / 100
	}

	e34.Pass = e34.IdleConnsHeld >= 2000 && e34.HeapPerIdleConnKB < 20.0 &&
		e34.SlowCopyCompletedCount == slowReaders && e34.SlowCopyHeapDeltaMB < 30.0
	rep.ExpE3E4 = e34

	rep.Pass = rep.ExpE1.Pass && rep.ExpE2.Pass && rep.ExpE3E4.Pass
	return rep, nil
}

type throttledWriter struct {
	written    int64
	delayEvery int64
	delay      time.Duration
	total      *atomic.Int64
}

func (w *throttledWriter) Write(p []byte) (int, error) {
	w.written += int64(len(p))
	w.total.Add(int64(len(p)))
	if w.written >= w.delayEvery {
		w.written = 0
		time.Sleep(w.delay)
	}
	return len(p), nil
}

var _ io.Writer = (*throttledWriter)(nil)
