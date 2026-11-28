package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type stubMeteringSaver struct {
	mu          sync.Mutex
	pingErr     error
	saveErr     error
	failOnce    bool
	calls       int
	batches     map[string]string
	totalDeltas map[string]int64
}

func newStubMeteringSaver() *stubMeteringSaver {
	return &stubMeteringSaver{
		batches:     make(map[string]string),
		totalDeltas: make(map[string]int64),
	}
}

func (s *stubMeteringSaver) Ping(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pingErr
}

func (s *stubMeteringSaver) SaveMeteringBatch(ctx context.Context, entries []MeteringEntry) error {
	return s.SaveMeteringBatchWithReceipt(ctx, "", "", "", entries)
}

func (s *stubMeteringSaver) SaveMeteringBatchWithReceipt(ctx context.Context, batchID, gatewayID, payloadSHA256 string, entries []MeteringEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failOnce {
		s.failOnce = false
		return errors.New("transient sink failure")
	}
	if s.saveErr != nil {
		return s.saveErr
	}
	if prev, ok := s.batches[batchID]; ok {
		if prev != payloadSHA256 {
			return ErrBatchPayloadMismatch
		}
		return nil
	}
	s.batches[batchID] = payloadSHA256
	for _, e := range entries {
		s.totalDeltas[e.DBID]++
	}
	return nil
}

func newReadyTestDiscovery() *TenantDiscovery {
	d := &TenantDiscovery{
		cache: make(map[string]TenantRoute),
	}
	d.MarkSynced()
	return d
}

func nonTmpDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp(".", "test-metering-*")
	if err != nil {
		t.Fatal(err)
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(abs) })
	return abs
}

func TestDurableMetering_RejectsTmpAndUnwritableWhenBillingEnabled(t *testing.T) {
	_, err := NewMeteringLoggerWithOptions(MeteringLoggerOptions{
		LogDir:          os.TempDir(),
		BillingEnabled:  true,
		RequireWritable: true,
		Saver:           newStubMeteringSaver(),
	})
	if !errors.Is(err, ErrMeteringTempDirForbidden) {
		t.Fatalf("expected ErrMeteringTempDirForbidden for /tmp, got %v", err)
	}

	if os.Geteuid() != 0 {
		roDir := filepath.Join(nonTmpDir(t), "ro-metering")
		if err := os.MkdirAll(roDir, 0555); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(roDir, 0755) })
		_, err = NewMeteringLoggerWithOptions(MeteringLoggerOptions{
			LogDir:          roDir,
			BillingEnabled:  true,
			RequireWritable: true,
			Saver:           newStubMeteringSaver(),
		})
		if !errors.Is(err, ErrMeteringNotWritable) {
			t.Fatalf("expected ErrMeteringNotWritable for readonly dir, got %v", err)
		}
	}

	// Missing/unhealthy sink must fail readiness when billing is enabled.
	dir := filepath.Join(nonTmpDir(t), "metering")
	saver := newStubMeteringSaver()
	saver.pingErr = errors.New("meta pg down")
	ml, err := NewMeteringLoggerWithOptions(MeteringLoggerOptions{
		LogDir:          dir,
		GatewayID:       "gw-test-1",
		BillingEnabled:  true,
		RequireWritable: true,
		Saver:           saver,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer ml.Stop()

	disc := newReadyTestDiscovery()
	pools := NewPoolManager(10, 60)
	metrics := NewMetricsRegistry(100, 10, 10)
	lifecycle := NewGatewayLifecycle("gw-test-1", false, nil, disc, pools, metrics, ml, nil, nil)
	report := lifecycle.Readiness(context.Background())
	if report.Ready {
		t.Fatalf("expected readiness to fail when billing sink is unhealthy: %+v", report)
	}
	if report.Checks["metering"].OK {
		t.Fatalf("expected metering check to fail when sink ping fails")
	}
}

func TestDurableMetering_QuarantineSpoolFullAndRestartRecovery(t *testing.T) {
	dir := filepath.Join(nonTmpDir(t), "metering")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	// Plant a corrupt batch file before startup.
	corruptPath := filepath.Join(dir, "batch.corrupt-1.log")
	if err := os.WriteFile(corruptPath, []byte("not-a-valid-tab-line\n"), 0644); err != nil {
		t.Fatal(err)
	}

	saver := newStubMeteringSaver()
	ml, err := NewMeteringLoggerWithOptions(MeteringLoggerOptions{
		LogDir:          dir,
		GatewayID:       "gw-test-2",
		BillingEnabled:  true,
		RequireWritable: true,
		MaxSpoolBytes:   220,
		Saver:           saver,
	})
	if err != nil {
		t.Fatalf("NewMeteringLoggerWithOptions failed: %v", err)
	}

	// Flushing/scanning should quarantine the corrupt file and mark Blocked=true.
	_ = ml.SealAndFlush(context.Background())
	st := ml.Status(context.Background())
	if st.QuarantinedCount != 1 || !st.Blocked {
		t.Fatalf("expected QuarantinedCount=1 and Blocked=true, got %+v", st)
	}
	if _, err := os.Stat(filepath.Join(dir, "quarantine", "batch.corrupt-1.log")); err != nil {
		t.Fatalf("expected corrupt batch moved to quarantine: %v", err)
	}

	// Bounded spool: writing beyond MaxSpoolBytes must return ErrMeteringSpoolFull.
	var spoolErr error
	for i := 0; i < 20; i++ {
		if err := ml.LogQueryErr(MeteringEntry{
			Date:           time.Now().UTC(),
			DBID:           "tenant-spool",
			BandwidthBytes: 128,
			QueryTimeUs:    500,
			QueryType:      "READ",
		}); err != nil {
			spoolErr = err
			break
		}
	}
	if !errors.Is(spoolErr, ErrMeteringSpoolFull) {
		t.Fatalf("expected ErrMeteringSpoolFull, got %v", spoolErr)
	}
	ml.Stop()

	// Test restart recovery & idempotent retry on clean directory.
	recDir := filepath.Join(nonTmpDir(t), "recover")
	recSaver := newStubMeteringSaver()
	recSaver.saveErr = errors.New("sink offline")
	ml2, err := NewMeteringLoggerWithOptions(MeteringLoggerOptions{
		LogDir:          recDir,
		GatewayID:       "gw-recover",
		BillingEnabled:  true,
		RequireWritable: true,
		Saver:           recSaver,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := ml2.LogQueryErr(MeteringEntry{
			Date:           time.Now().UTC(),
			DBID:           "t1",
			BandwidthBytes: 64,
			QueryTimeUs:    100,
			QueryType:      "WRITE",
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := ml2.SealAndFlush(context.Background()); err == nil {
		t.Fatal("expected SealAndFlush error when sink is offline")
	}
	ml2.Stop()

	// Reopen with same volume and healthy saver; failOnce simulates ambiguous commit/retry.
	recSaver.saveErr = nil
	recSaver.failOnce = true
	ml3, err := NewMeteringLoggerWithOptions(MeteringLoggerOptions{
		LogDir:          recDir,
		GatewayID:       "gw-recover",
		BillingEnabled:  true,
		RequireWritable: true,
		Saver:           recSaver,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ml3.Stop()
	if err := ml3.SealAndFlush(context.Background()); err == nil {
		t.Fatal("expected first SealAndFlush after restart to hit transient failOnce")
	}
	if err := ml3.SealAndFlush(context.Background()); err != nil {
		t.Fatalf("expected second SealAndFlush retry to succeed: %v", err)
	}
	if recSaver.totalDeltas["t1"] != 3 {
		t.Fatalf("expected exactly 3 query_count for t1, got %d", recSaver.totalDeltas["t1"])
	}
}

func TestDurableMetering_LegacyPreservationAndReconcile(t *testing.T) {
	dir := filepath.Join(nonTmpDir(t), "legacy")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	legacyFile := filepath.Join(dir, "query_metering.1700000000.log")
	line := FormatMeteringLine(MeteringEntry{
		Date:           time.Now().UTC(),
		DBID:           "t-legacy",
		BandwidthBytes: 256,
		QueryTimeUs:    400,
		QueryType:      "READ",
	})
	if err := os.WriteFile(legacyFile, []byte(line+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	ml, err := NewMeteringLoggerWithOptions(MeteringLoggerOptions{
		LogDir:          dir,
		GatewayID:       "gw-legacy",
		BillingEnabled:  true,
		RequireWritable: true,
		Saver:           newStubMeteringSaver(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ml.Start()
	defer ml.Stop()

	st := ml.Status(context.Background())
	if st.LegacyPendingCount != 1 {
		t.Fatalf("expected LegacyPendingCount=1, got %+v", st)
	}
	if _, err := os.Stat(filepath.Join(dir, "legacy_pending", "query_metering.1700000000.log")); err != nil {
		t.Fatalf("expected legacy file preserved in legacy_pending/: %v", err)
	}

	reports, err := ReconcileLegacySpool(context.Background(), dir, "", false)
	if err != nil {
		t.Fatalf("ReconcileLegacySpool failed: %v", err)
	}
	if len(reports) != 1 || reports[0].Status != "unknown_prior_commit" || reports[0].EntryCount != 1 {
		t.Fatalf("unexpected legacy reconcile report: %+v", reports)
	}
}

func TestCrossGatewayCancelRouting_AndWhileDraining(t *testing.T) {
	const token = "mesh-secret-token"
	poolsB := NewPoolManager(10, 60)
	metricsB := NewMetricsRegistry(100, 10, 10)
	cancelB := NewCancelRegistry()
	discB := newReadyTestDiscovery()
	lifecycleB := NewGatewayLifecycle("gw-b", false, nil, discB, poolsB, metricsB, nil, nil, nil)

	srvB := NewAdminGRPCServer("127.0.0.1:0", token, poolsB, metricsB)
	srvB.SetMeshHandlers(cancelB, lifecycleB)
	portB, err := srvB.Start()
	if err != nil {
		t.Fatalf("failed to start AdminGRPCServer B: %v", err)
	}
	defer srvB.Stop()

	var cancelled atomic.Bool
	cancelB.RegisterSession(1001, 2002, func() {
		cancelled.Store(true)
	})

	cancelA := NewCancelRegistry()
	peerAddr := "127.0.0.1:" + itoa(portB)
	routerA := NewCancelRouter(cancelA, token, []string{peerAddr}, "", portB)
	defer routerA.Close()

	// Wrong secret must not cancel.
	if routerA.ForwardCancel(context.Background(), 1001, 9999) {
		t.Fatal("expected ForwardCancel with wrong secret to return false")
	}
	if cancelled.Load() {
		t.Fatal("session should not be cancelled on wrong secret")
	}

	// Cordon Gateway B (simulating draining); cancel routing must remain alive!
	lifecycleB.Cordon()
	if !routerA.ForwardCancel(context.Background(), 1001, 2002) {
		t.Fatal("expected cross-gateway ForwardCancel to succeed while owner is draining")
	}
	if !cancelled.Load() {
		t.Fatal("expected owner session cancel callback to be invoked")
	}
}

func TestAggregateWriterLimits_AndTenantDrainBlocksNewCheckouts(t *testing.T) {
	pm := NewPoolManager(10, 60)
	pm.SetAggregateMaxBackends(10)
	pm.SetPeerCount(2)
	if got := pm.EffectiveMaxWriterBackends(); got != 5 {
		t.Fatalf("expected EffectiveMaxWriterBackends=5, got %d", got)
	}
	pm.SetPeerCount(3)
	if got := pm.EffectiveMaxWriterBackends(); got != 3 {
		t.Fatalf("expected EffectiveMaxWriterBackends=3, got %d", got)
	}

	pool := NewTenantPool("tenant-drain", "127.0.0.1", "", 5432, "pass", 5, 60)
	if err := pool.RequestDrain(time.Second); err != nil {
		t.Fatalf("RequestDrain failed: %v", err)
	}
	if !pool.IsDraining() {
		t.Fatal("expected pool.IsDraining() == true")
	}
	if err := pool.acquireSlot(context.Background()); err == nil {
		t.Fatal("expected acquireSlot to fail while tenant pool is draining")
	}
	pool.UpdateEndpoint("127.0.0.1", "", 5432, "pass")
	if pool.IsDraining() {
		t.Fatal("expected UpdateEndpoint with port > 0 to clear draining")
	}
}

func TestGatewayLifecycle_DrainInstanceAck(t *testing.T) {
	dir := filepath.Join(nonTmpDir(t), "metering")
	saver := newStubMeteringSaver()
	ml, err := NewMeteringLoggerWithOptions(MeteringLoggerOptions{
		LogDir:          dir,
		GatewayID:       "gw-drain",
		BillingEnabled:  true,
		RequireWritable: true,
		Saver:           saver,
	})
	if err != nil {
		t.Fatal(err)
	}
	ml.Start()
	defer ml.Stop()

	disc := newReadyTestDiscovery()
	pools := NewPoolManager(10, 60)
	metrics := NewMetricsRegistry(100, 10, 10)
	lifecycle := NewGatewayLifecycle("gw-drain", false, nil, disc, pools, metrics, ml, nil, nil)

	// Active client blocks drain without forcing disconnect.
	if !metrics.TryAcquireGlobal() {
		t.Fatal("expected TryAcquireGlobal to succeed")
	}
	res := lifecycle.DrainInstance(context.Background(), "op-active", 80*time.Millisecond)
	if res.Drained {
		t.Fatalf("expected drain to time out while client is active: %+v", res)
	}
	if metrics.ActiveClients() != 1 {
		t.Fatalf("expected active client not to be force-disconnected")
	}

	// Once client finishes, drain seals & flushes metering and returns op-bound ack.
	metrics.ReleaseGlobal()
	if err := ml.LogQueryErr(MeteringEntry{
		Date:           time.Now().UTC(),
		DBID:           "t-ok",
		BandwidthBytes: 64,
		QueryTimeUs:    100,
		QueryType:      "READ",
	}); err != nil {
		t.Fatal(err)
	}
	res = lifecycle.DrainInstance(context.Background(), "op-clean", time.Second)
	if !res.Drained || !res.MeteringConfirmed || res.OpID != "op-clean" {
		t.Fatalf("expected confirmed drain for op-clean, got %+v", res)
	}
}

func TestGatewayLifecycle_CordonRejectsNewSessionsPreservesExistingAndCancel(t *testing.T) {
	// Start mock backend PG server (non-pooled direct proxy mode for deterministic startup + cancel check).
	backendLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backendLn.Close()
	backendPort := backendLn.Addr().(*net.TCPAddr).Port

	var cancelReceived atomic.Bool
	go func() {
		for {
			c, err := backendLn.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				var hdr [8]byte
				if _, err := io.ReadFull(conn, hdr[:]); err != nil {
					return
				}
				msgLen := binary.BigEndian.Uint32(hdr[0:4])
				code := binary.BigEndian.Uint32(hdr[4:8])
				if msgLen == 16 && code == CancelRequestCode {
					var rest [8]byte
					_, _ = io.ReadFull(conn, rest[:])
					cancelReceived.Store(true)
					return
				}
				if msgLen > 8 {
					buf := make([]byte, msgLen-8)
					_, _ = io.ReadFull(conn, buf)
				}
				_, _ = conn.Write(BuildAuthOK())
				_, _ = conn.Write(BuildBackendKeyData(1111, 2222))
				_, _ = conn.Write(BuildReadyForQuery('I'))
				// Echo any query bytes back so existing session liveness can be verified.
				_, _ = io.Copy(conn, conn)
			}(c)
		}
	}()

	cfg := DefaultConfig()
	cfg.PoolMode = false
	cfg.RequireTLS = false
	cfg.AuthTimeoutMS = 2000

	disc, err := NewTenantDiscovery(cfg)
	if err != nil {
		t.Fatal(err)
	}
	disc.MarkSynced()
	disc.UpsertRoute(TenantRoute{
		TenantID:    "t_cordon",
		State:       "ACTIVE",
		Host:        "127.0.0.1",
		Port:        backendPort,
		AppPassword: "secret",
	})
	pools := NewPoolManager(4, 60)
	defer pools.CloseAll()
	metrics := NewMetricsRegistry(100, 10, 10)
	cancelReg := NewCancelRegistry()
	cancelReg.Register(9001, 8008, CancelTarget{
		Host:          "127.0.0.1",
		Port:          backendPort,
		BackendPID:    1111,
		BackendSecret: 2222,
	})
	lifecycle := NewGatewayLifecycle("gw-cordon", false, nil, disc, pools, metrics, nil, nil, nil)

	gwLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer gwLn.Close()

	go func() {
		for {
			c, err := gwLn.Accept()
			if err != nil {
				return
			}
			if !metrics.TryAcquireGlobal() {
				_ = c.Close()
				continue
			}
			go HandleClientConnectionWithLifecycle(c, cfg, nil, disc, pools, metrics, cancelReg, nil, lifecycle)
		}
	}()

	// 1. Open an existing session BEFORE cordoning.
	existingConn, err := net.DialTimeout("tcp", gwLn.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer existingConn.Close()
	if _, err := existingConn.Write(BuildStartupMessage(map[string]string{"user": "t_cordon", "database": "t_cordon"})); err != nil {
		t.Fatal(err)
	}
	_ = existingConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	startupResp := make([]byte, 256)
	n, err := existingConn.Read(startupResp)
	if err != nil || !bytes.Contains(startupResp[:n], []byte{'Z'}) {
		t.Fatalf("expected ReadyForQuery for existing session, got n=%d err=%v", n, err)
	}

	// 2. Cordon the gateway.
	lifecycle.Cordon()
	if !lifecycle.IsCordoned() {
		t.Fatal("expected gateway to be cordoned")
	}

	// 3. New PostgreSQL session must be rejected with 57P03.
	newConn, err := net.DialTimeout("tcp", gwLn.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer newConn.Close()
	if _, err := newConn.Write(BuildStartupMessage(map[string]string{"user": "t_cordon", "database": "t_cordon"})); err != nil {
		t.Fatal(err)
	}
	_ = newConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	errBuf := make([]byte, 256)
	en, err := newConn.Read(errBuf)
	if err != nil {
		t.Fatalf("expected ErrorResponse from cordoned gateway, got err=%v", err)
	}
	if errBuf[0] != 'E' || !strings.Contains(string(errBuf[:en]), "57P03") {
		t.Fatalf("expected FATAL 57P03 ErrorResponse, got %q", string(errBuf[:en]))
	}

	// 4. Existing session must continue to work while cordoned.
	_ = existingConn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := existingConn.Write([]byte("PING")); err != nil {
		t.Fatalf("existing session write failed while cordoned: %v", err)
	}
	echo := make([]byte, 4)
	if _, err := io.ReadFull(existingConn, echo); err != nil || string(echo) != "PING" {
		t.Fatalf("expected existing session echo while cordoned, got %q err=%v", string(echo), err)
	}

	// 5. CancelRequest must continue to work while cordoned.
	cancelConn, err := net.DialTimeout("tcp", gwLn.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = cancelConn.Write(BuildCancelRequest(9001, 8008))
	_ = cancelConn.Close()

	deadline := time.Now().Add(2 * time.Second)
	for !cancelReceived.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !cancelReceived.Load() {
		t.Fatal("expected CancelRequest to be forwarded while gateway is cordoned")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
