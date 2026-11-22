package readadapter

import (
	"context"
	"strings"
	"testing"
)

func TestP1_FallbackTelemetryAndRanking(t *testing.T) {
	srv, err := NewSQLiteReadServer(SQLiteServerConfig{
		DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop() }()

	ts, err := srv.getOrCreateTenant("tenant_p1_telemetry")
	if err != nil {
		t.Fatal(err)
	}

	// Register extended column types to exercise CheckUnsupportedExtOps + SQLite fallback paths.
	ts.extCols.Store(map[string]ColumnType{
		"amount": ColNumeric,
		"ip":     ColInet,
		"net":    ColCIDR,
		"tags":   ColTextArray,
	})

	// Representative mixed fallback workload (22-query extended-type fallbacks + broader PG features).
	workload := []struct {
		sql string
		rep int
	}{
		// Window functions
		{"SELECT id, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY id) FROM items", 5},
		{"SELECT id, SUM(val) OVER (ORDER BY id) FROM items", 3},
		// DISTINCT ON
		{"SELECT DISTINCT ON (cat) cat, id FROM items ORDER BY cat, id", 6},
		// ILIKE / regex
		{"SELECT id FROM items WHERE note ILIKE '%alpha%'", 4},
		{"SELECT id FROM items WHERE note ~ '^note_[0-9]+$'", 3},
		// numeric arithmetic / aggregates
		{"SELECT SUM(amount) FROM orders", 3},
		{"SELECT amount + 1 FROM orders", 2},
		// inet subnet ops
		{"SELECT id FROM hosts WHERE ip << net", 2},
		{"SELECT host(ip) FROM hosts", 2},
		// array ops
		{"SELECT tags[1] FROM posts", 2},
		{"SELECT id FROM posts WHERE tags @> '{go}'", 2},
		{"SELECT id FROM posts ORDER BY tags", 1},
		// unsupported types (interval, time, enum, domain)
		{"SELECT id FROM events WHERE dur > '1 hour'::interval", 2},
	}

	for _, w := range workload {
		for i := 0; i < w.rep; i++ {
			_, _, _ = srv.executeReaderQueryWithPlanTyped(context.Background(), ts, w.sql, nil, nil)
		}
	}

	// Also run a few succeeding queries so query-shape histogram has non-fallback samples too.
	for i := 0; i < 5; i++ {
		res, _, qerr := srv.executeReaderQueryWithPlanTyped(context.Background(), ts, "SELECT 1 AS ok WHERE 1 = 1;", nil, nil)
		if qerr != nil || len(res.Rows) != 1 {
			t.Fatalf("expected simple query to succeed, got err=%v", qerr)
		}
	}

	ranked := srv.RankedFallbacks()
	if len(ranked) == 0 {
		t.Fatalf("expected non-empty ranked fallbacks")
	}
	for i, e := range ranked {
		t.Logf("rank=%d reason=%s count=%d share=%.1f%% top_shape=%s", i+1, e.Reason, e.Count, e.SharePct, e.TopShape)
		if i > 0 && ranked[i].Count > ranked[i-1].Count {
			t.Fatalf("ranked list not sorted descending at %d: %+v", i, ranked)
		}
	}

	prom := srv.PrometheusMetrics()
	required := []string{
		"fallback_total{reason=\"window_function\"}",
		"fallback_total{reason=\"distinct_on\"}",
		"fallback_total{reason=\"ilike_regex\"}",
		"fallback_total{reason=\"numeric_arith_agg\"}",
		"fallback_total{reason=\"inet_ops\"}",
		"fallback_total{reason=\"array_ops\"}",
		"air_postgres_fallback_total{reason=",
		"air_read_query_shape_duration_seconds_bucket{shape=",
		"air_read_query_shape_fallbacks_total{shape=",
	}
	for _, r := range required {
		if !strings.Contains(prom, r) {
			t.Fatalf("missing metric %q in Prometheus output:\n%s", r, prom)
		}
	}
	if strings.Contains(prom, "tenant_p1_telemetry") {
		t.Fatalf("high-cardinality tenant ID leaked into Prometheus metrics")
	}
}
