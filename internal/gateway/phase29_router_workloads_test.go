package gateway

import "testing"

func TestPhase29_RouterWorkloadClassificationAndFallback(t *testing.T) {
	rctxHealthy := SessionRouteContext{
		ReadEngineHealthy: true,
		SessionWriteLSN:   100,
		ReadEngineLSN:     100,
	}

	readCases := []string{
		`SELECT id, email FROM users WHERE org_id = $1 AND email ILIKE $2 ORDER BY score DESC NULLS LAST`,
		`WITH paid AS (SELECT org_id, amount FROM orders WHERE status = 'paid') SELECT org_id, SUM(amount) FROM paid GROUP BY org_id`,
		`SELECT DISTINCT ON (org_id) org_id, id FROM orders ORDER BY org_id, amount DESC`,
		`SELECT org_id, string_agg(role, ',') FROM users GROUP BY org_id`,
	}
	for _, sql := range readCases {
		dec := RouteQuery(sql, rctxHealthy, nil)
		if dec.Target != RouteTargetReadEngine {
			t.Fatalf("expected RouteTargetReadEngine for %q, got %v (reason=%v)", sql, dec.Target, dec.Reason)
		}
	}

	writeOrFallbackCases := []string{
		`INSERT INTO orders (org_id, amount) VALUES ($1, $2) RETURNING id`,
		`SELECT * FROM orders WHERE id = $1 FOR UPDATE`,
		`SELECT u.id, l.total FROM users u, LATERAL (SELECT SUM(amount) AS total FROM orders o WHERE o.user_id = u.id) l`,
		`NOTIFY events, 'updated'`,
	}
	for _, sql := range writeOrFallbackCases {
		dec := RouteQuery(sql, rctxHealthy, nil)
		if dec.Target != RouteTargetWriteEngine {
			t.Fatalf("expected RouteTargetWriteEngine fallback for %q, got %v (reason=%v)", sql, dec.Target, dec.Reason)
		}
	}
}
