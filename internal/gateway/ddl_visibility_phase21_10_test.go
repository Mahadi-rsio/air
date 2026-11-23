package gateway

import "testing"

func TestTenantPool_DDLVisibilityBarrierBlocksNewConnectionLag(t *testing.T) {
	pool := NewTenantPool("t_ddl", "127.0.0.1", "", 5432, "pw", 8, 60)

	// New connection with sessionWriteLSN == 0, ReadEngine at LSN 0x1000.
	newConn := &clientSessionState{txStatus: 'I'}
	pool.readEngineLSN.Store(0x1000)
	pool.readEngineHealthy.Store(true)

	// Before any DDL, new connection can route reads to ReadEngine.
	rlsn, minLSN, healthy := pool.syncReadEngineForQuery(newConn, false, false)
	dec := RouteQuery("SELECT * FROM t1", SessionRouteContext{
		ReadEngineHealthy: healthy,
		SessionWriteLSN:   minLSN,
		ReadEngineLSN:     rlsn,
	}, nil)
	if dec.Target != RouteTargetReadEngine {
		t.Fatalf("expected ReadEngine before DDL, got %+v", dec)
	}

	// Connection 1 executes DDL at LSN 0x2000.
	pool.advancePoolDDLLSN(0x2000)

	// Brand-new Connection 2 (sessionWriteLSN == 0) must NOT bypass DDL visibility.
	rlsn, minLSN, healthy = pool.syncReadEngineForQuery(newConn, false, false)
	dec = RouteQuery("SELECT * FROM soak_kv", SessionRouteContext{
		ReadEngineHealthy: healthy,
		SessionWriteLSN:   minLSN,
		ReadEngineLSN:     rlsn,
	}, nil)
	if dec.Target != RouteTargetWriteEngine || dec.Reason != RouteReasonCausalLagFallback {
		t.Fatalf("expected WriteEngine causal lag fallback for stale ReadEngine, got %+v", dec)
	}

	// Once ReadEngine catches up to 0x2000, Connection 2 routes to ReadEngine again.
	pool.readEngineLSN.Store(0x2000)
	rlsn, minLSN, healthy = pool.syncReadEngineForQuery(newConn, false, false)
	dec = RouteQuery("SELECT * FROM soak_kv", SessionRouteContext{
		ReadEngineHealthy: healthy,
		SessionWriteLSN:   minLSN,
		ReadEngineLSN:     rlsn,
	}, nil)
	if dec.Target != RouteTargetReadEngine {
		t.Fatalf("expected ReadEngine after sync to 0x2000, got %+v", dec)
	}

	// Uncommitted / dirty DDL must force fallback to primary PostgreSQL.
	pool.poolDDLDirty.Store(true)
	rlsn, minLSN, healthy = pool.syncReadEngineForQuery(newConn, false, false)
	dec = RouteQuery("SELECT * FROM soak_kv", SessionRouteContext{
		ReadEngineHealthy: healthy,
		SessionWriteLSN:   minLSN,
		ReadEngineLSN:     rlsn,
	}, nil)
	if dec.Target != RouteTargetWriteEngine {
		t.Fatalf("expected WriteEngine fallback when poolDDLDirty=true, got %+v", dec)
	}
	pool.poolDDLDirty.Store(false)

	// Unconfirmed ReadEngine (LSN == 0) must safely fall back when primary is active.
	pool.readEngineLSN.Store(0)
	rlsn, minLSN, healthy = pool.syncReadEngineForQuery(newConn, false, false)
	dec = RouteQuery("SELECT * FROM soak_kv", SessionRouteContext{
		ReadEngineHealthy: healthy,
		SessionWriteLSN:   minLSN,
		ReadEngineLSN:     rlsn,
	}, nil)
	if dec.Target != RouteTargetWriteEngine {
		t.Fatalf("expected WriteEngine fallback when ReadEngineLSN==0, got %+v", dec)
	}
}
