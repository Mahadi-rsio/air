package gateway

import "testing"

func TestPhase15_ClassifySQLWithHint_AndMetrics(t *testing.T) {
	d, h := ClassifySQLWithHint("SELECT id, name FROM users WHERE id = 1")
	if d.Target != RouteTargetReadEngine || d.Reason != RouteReasonReadOnlySelect {
		t.Fatalf("unexpected route decision: %+v", d)
	}
	if !h.SimpleRead || h.ComplexJoinOrAgg || h.HasOrderLimit || h.ExplainQuery {
		t.Fatalf("unexpected hint for simple select: %+v", h)
	}

	d2, h2 := ClassifySQLWithHint("SELECT u.id, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.id ORDER BY u.id LIMIT 10")
	if d2.Target != RouteTargetReadEngine {
		t.Fatalf("expected ReadEngine for join+agg, got %+v", d2)
	}
	if !h2.ComplexJoinOrAgg || !h2.HasOrderLimit || h2.SimpleRead {
		t.Fatalf("unexpected hint for join+agg+order+limit: %+v", h2)
	}

	var m RoutingMetrics
	ctx := SessionRouteContext{ReadEngineHealthy: true}
	_ = RouteQuery("EXPLAIN SELECT * FROM users", ctx, &m)
	_ = RouteQuery("SELECT * FROM users", ctx, &m)
	ctxDown := SessionRouteContext{ReadEngineHealthy: false}
	_ = RouteQuery("SELECT * FROM users", ctxDown, &m)

	if m.ExplainRouted.Load() != 1 {
		t.Fatalf("expected ExplainRouted=1, got %d", m.ExplainRouted.Load())
	}
	if m.ReadRouted.Load() != 2 || m.FallbacksToWrite.Load() != 1 {
		t.Fatalf("unexpected metrics: read=%d fb=%d", m.ReadRouted.Load(), m.FallbacksToWrite.Load())
	}
	if rate := m.ReadFallbackRate(); rate <= 0.30 || rate >= 0.35 {
		t.Fatalf("expected fallback rate ~0.333, got %.4f", rate)
	}
}
