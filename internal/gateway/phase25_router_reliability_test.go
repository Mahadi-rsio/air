package gateway

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestPhase25_CapabilityRegistry_NeverTreatsPartialAsFull(t *testing.T) {
	reg := DefaultCapabilityRegistry()

	cases := []struct {
		cat  CapabilityCategory
		name string
		want CapabilitySupport
		elig bool
	}{
		{CatStatement, "SELECT", CapFullSupport, true},
		{CatStatement, "WITH", CapFullSupport, true},
		{CatJoin, "LEFT", CapFullSupport, true},
		{CatJoin, "FULL", CapFullSupport, true},
		{CatJoin, "LATERAL", CapUnsupported, false},
		{CatOperator, "ILIKE", CapFullSupport, true},
		{CatOperator, "->>", CapFullSupport, true},
		{CatOperator, "@>", CapPartialSupport, false},
		{CatOperator, "SIMILAR_TO", CapPartialSupport, false},
		{CatCast, "INT4", CapFullSupport, true},
		{CatCast, "INET", CapPartialSupport, false},
		{CatFunction, "COALESCE", CapFullSupport, true},
		{CatFunction, "NEXTVAL", CapUnsupported, false},
		{CatFunction, "NON_EXISTENT_UDF_XYZ", CapUnknown, false},
	}

	for _, tc := range cases {
		got := reg.Lookup(tc.cat, tc.name)
		if got != tc.want {
			t.Errorf("Lookup(%v, %q) = %v, want %v", tc.cat, tc.name, got, tc.want)
		}
		if got.IsReadEngineEligible() != tc.elig {
			t.Errorf("Lookup(%v, %q).IsReadEngineEligible() = %v, want %v", tc.cat, tc.name, got.IsReadEngineEligible(), tc.elig)
		}
	}
}

func TestPhase25_ASTCapabilityRouter_RoutingDecisions(t *testing.T) {
	readEligible := []string{
		"SELECT id, name FROM users WHERE id = $1",
		"SELECT DISTINCT ON (dept) dept, salary FROM emp ORDER BY dept, salary DESC NULLS LAST",
		"WITH cte AS (SELECT id, total FROM orders WHERE total > 100) SELECT u.id, SUM(c.total) FILTER (WHERE c.total > 200) FROM users u LEFT JOIN cte c ON u.id = c.id GROUP BY u.id",
		"WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n+1 FROM t WHERE n < 10) SELECT sum(n) FROM t",
		"SELECT id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM emp",
		"SELECT name FROM users WHERE email ILIKE '%@example.com' AND bio ~* '^dev'",
		"SELECT payload->>'role' FROM users WHERE (id::int4) > 0 AND CAST(created_at AS date) >= '2026-01-01'",
		"SELECT u.id FROM users u FULL OUTER JOIN profiles p ON u.id = p.user_id",
		"/* DELETE FROM users */ -- UPDATE users\nSELECT 'INSERT INTO x', $$DROP TABLE y$$ FROM items",
	}
	for _, sql := range readEligible {
		d, _ := ClassifySQLWithHint(sql)
		if d.Target != RouteTargetReadEngine {
			t.Errorf("expected ReadEngine for %q, got target=%v reason=%v", sql, d.Target, d.Reason)
		}
	}

	writeFallback := []struct {
		sql    string
		reason RouteReason
	}{
		{"SELECT * FROM items FOR UPDATE", RouteReasonRowLocking},
		{"SELECT * INTO backup_items FROM items", RouteReasonSelectInto},
		{"WITH d AS (DELETE FROM items RETURNING *) SELECT * FROM d", RouteReasonModifyingCTE},
		{"SELECT nextval('items_id_seq')", RouteReasonMutatingFunction},
		{"SELECT * FROM pg_catalog.pg_class", RouteReasonSystemCatalog},
		{"SELECT * FROM items WHERE tags @> '[\"a\"]'", RouteReasonCapabilityFallback},
		{"SELECT * FROM items WHERE name SIMILAR TO '%(a|b)%'", RouteReasonCapabilityFallback},
		{"SELECT * FROM items WHERE id = ANY($1)", RouteReasonCapabilityFallback},
		{"SELECT '10.0.0.1'::inet", RouteReasonCapabilityFallback},
		{"SELECT * FROM users u, LATERAL (SELECT * FROM orders o WHERE o.user_id = u.id) sub", RouteReasonCapabilityFallback},
		{"SELECT dept, COUNT(*) FROM emp GROUP BY GROUPING SETS ((dept), ())", RouteReasonCapabilityFallback},
		{"SELECT custom_plpgsql_udf(id) FROM items", RouteReasonCapabilityFallback},
		{"SELECT 1; INSERT INTO items VALUES (1)", RouteReasonWriteDML},
	}
	for _, tc := range writeFallback {
		d, _ := ClassifySQLWithHint(tc.sql)
		if d.Target != RouteTargetWriteEngine || d.Reason != tc.reason {
			t.Errorf("ClassifySQLWithHint(%q) = (%v, %v), want (%v, %v)", tc.sql, d.Target, d.Reason, RouteTargetWriteEngine, tc.reason)
		}
	}
}

func TestPhase25_AcquireSlotAndGovernor_ImmediateCancel(t *testing.T) {
	pool := NewTenantPool("t_cancel", "127.0.0.1", "", 5432, "", 1, 60)
	defer pool.Close()

	// Occupy the single backend slot.
	if err := pool.acquireSlot(context.Background()); err != nil {
		t.Fatalf("first acquireSlot failed: %v", err)
	}
	defer pool.releaseSlot()

	// Second acquireSlot should block and cancel immediately when ctx is canceled.
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	start := time.Now()
	go func() {
		errCh <- pool.acquireSlot(ctx)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
			t.Fatalf("acquireSlot cancellation took too long: %v", elapsed)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("acquireSlot did not unblock on context cancellation")
	}
}

func TestPhase25_CancelRequest_UnblocksQueuedWriteGovernor(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WriteGovernorEnabled = true
	cfg.WriteMaxGlobal = 1
	cfg.WriteMaxPerTenant = 1
	cfg.WriteQueueMax = 8
	cfg.WriteQueueTimeout = 5 * time.Second
	gov := NewWriteGovernor(cfg)
	// Hold the only write slot.
	holder, err := gov.Acquire(context.Background(), "t_qcancel", 64)
	if err != nil {
		t.Fatalf("holder acquire failed: %v", err)
	}
	defer holder.Release(false)

	pool := NewTenantPool("t_qcancel", "127.0.0.1", "", 5432, "", 4, 60)
	pool.SetWriteGovernor(gov)
	defer pool.Close()

	cancelReg := NewCancelRegistry()
	clientConn, peerConn := net.Pipe()
	defer clientConn.Close()
	defer peerConn.Close()

	// Drain server responses from peerConn so net.Pipe writes don't block.
	errCodeCh := make(chan string, 1)
	go func() {
		for {
			t, body, rerr := ReadPGPacket(peerConn)
			if rerr != nil {
				return
			}
			if t == 'E' {
				// Extract SQLSTATE ('C' field)
				for i := 0; i < len(body); {
					ftype := body[i]
					i++
					if ftype == 0 {
						break
					}
					end := i
					for end < len(body) && body[end] != 0 {
						end++
					}
					if ftype == 'C' {
						errCodeCh <- string(body[i:end])
					}
					i = end + 1
				}
			}
		}
	}()

	st := &clientSessionState{
		client:      clientConn,
		clientID:    1,
		gwPID:       10001,
		gwSecret:    0xdeadbeef,
		cancelReg:   cancelReg,
		sessionCtx:  context.Background(),
		sessionVars: make(map[string]string),
		clientStmts: make(map[string]PreparedDef),
		txStatus:    'I',
	}
	cancelReg.RegisterSession(st.gwPID, st.gwSecret, st.cancelActiveQuery)
	defer cancelReg.UnregisterSession(st.gwPID, st.gwSecret)

	qctx, endQ := st.beginQueryContext()
	defer endQ()

	doneCh := make(chan bool, 1)
	start := time.Now()
	go func() {
		_, admitted, _ := pool.acquireWritePermit(qctx, st, 128, start, "INSERT", 1)
		doneCh <- admitted
	}()

	time.Sleep(30 * time.Millisecond)
	// Simulate incoming PG CancelRequest packet over a separate connection.
	cClient, cServer := net.Pipe()
	go func() {
		defer cClient.Close()
		creq := BuildCancelRequest(st.gwPID, st.gwSecret)
		// Strip 8-byte header? forwardCancelRequest reads the 8-byte (pid, secret) payload after reading header.
		_, _ = cClient.Write(creq[8:])
	}()
	forwardCancelRequest(cServer, cancelReg)
	_ = cServer.Close()

	select {
	case admitted := <-doneCh:
		if admitted {
			t.Fatal("expected queued write permit to be canceled, not admitted")
		}
		if elapsed := time.Since(start); elapsed > 1*time.Second {
			t.Fatalf("CancelRequest took too long to abort queued permit: %v", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CancelRequest did not unblock queued WriteGovernor.Acquire")
	}

	select {
	case code := <-errCodeCh:
		if code != "57014" {
			t.Fatalf("expected SQLSTATE 57014 (query_canceled), got %q", code)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("expected ErrorResponse with SQLSTATE 57014")
	}
}

func TestPhase25_ReadEngineCircuitBreaker_AndQueryErrorIsolation(t *testing.T) {
	pool := NewTenantPool("t_cb", "127.0.0.1", "", 5432, "", 4, 60)
	defer pool.Close()

	// 1-2 failures should not open the long cooldown yet, 3rd opens it.
	pool.recordReadEngineFailure()
	pool.recordReadEngineFailure()
	if pool.readBreakerOpenUntil.Load() != 0 {
		t.Fatal("circuit breaker opened before threshold=3")
	}
	pool.recordReadEngineFailure()
	if pool.readBreakerOpenUntil.Load() == 0 {
		t.Fatal("expected circuit breaker to open at threshold=3")
	}
	if pool.readEngineHealthy.Load() {
		t.Fatal("expected readEngineHealthy=false when breaker open")
	}

	// While cooldown is active, canAttemptReadEngine must reject.
	if allowed, _ := pool.canAttemptReadEngine(); allowed {
		t.Fatal("expected canAttemptReadEngine=false while breaker cooldown active")
	}

	// Simulate cooldown expiry -> single half-open probe allowed.
	pool.readBreakerOpenUntil.Store(time.Now().Add(-time.Millisecond).UnixNano())
	allowed, isProbe := pool.canAttemptReadEngine()
	if !allowed || !isProbe {
		t.Fatalf("expected single half-open probe, got allowed=%v isProbe=%v", allowed, isProbe)
	}
	// Concurrent second caller during half-open probe should be rejected.
	if allowed2, _ := pool.canAttemptReadEngine(); allowed2 {
		t.Fatal("expected second concurrent half-open probe to be rejected")
	}

	// Probe succeeds -> breaker closes.
	pool.recordReadEngineSuccess()
	if !pool.readEngineHealthy.Load() || pool.readConsecFails.Load() != 0 {
		t.Fatal("expected circuit breaker to reset after success")
	}
}

func TestPhase25_PoolManager_CloseAll_CancelsPools(t *testing.T) {
	pm := NewPoolManager(4, 60)
	var called atomic.Bool
	p := pm.GetOrCreate(TenantRoute{TenantID: "t_close", Host: "127.0.0.1", Port: 5432, State: "ACTIVE"}, nil)
	ctx := p.poolCtx
	pm.CloseAll()
	select {
	case <-ctx.Done():
		called.Store(true)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("PoolManager.CloseAll did not cancel TenantPool context")
	}
	if !called.Load() {
		t.Fatal("expected poolCtx to be canceled")
	}
	if snaps := pm.Snapshots(); len(snaps) != 0 {
		t.Fatalf("expected empty PoolManager after CloseAll, got %d", len(snaps))
	}
	// Ensure binary import is used if needed
	_ = binary.BigEndian
}
