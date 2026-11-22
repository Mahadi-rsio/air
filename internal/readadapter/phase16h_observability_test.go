package readadapter

import (
	"context"
	"strings"
	"testing"
)

func TestPhase16H_ObservabilityMetricsAndLowCardinalityPrometheus(t *testing.T) {
	srv, err := NewSQLiteReadServer(SQLiteServerConfig{
		DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop() }()

	ts, err := srv.getOrCreateTenant("tenant_obs_12345")
	if err != nil {
		t.Fatal(err)
	}
	res, _, err := srv.executeReaderQueryWithPlanTyped(context.Background(), ts, "SELECT 1 AS ok;", nil, nil)
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("query failed: err=%v res=%+v", err, res)
	}

	// Trigger optimizer telemetry
	_ = srv.queryOptimizer.ChooseBestPlan(
		DiscoveredTable{RelOID: 10, RelPages: 5, RelTuples: 100},
		nil, []ColumnType{ColInt4}, nil, nil, 1, 5, "tenant_obs_12345", "g1",
	)
	srv.queryOptimizer.RecordExecutionFeedback(100, 95, 1.5, 250000)

	m := srv.DetailedMetrics()
	if m.Optimizer.PlansEvaluated == 0 || m.Optimizer.EstimatedRowsTotal == 0 || m.Optimizer.ActualRowsTotal == 0 {
		t.Fatalf("expected optimizer telemetry populated, got %+v", m.Optimizer)
	}
	if m.Scheduler.AdmittedTotal == 0 || m.Memory.GlobalUsedBytes != 0 || m.Goroutines <= 0 || m.GoHeapAllocBytes == 0 {
		t.Fatalf("unexpected resource metrics: sched=%+v mem=%+v goroutines=%d heap=%d",
			m.Scheduler, m.Memory, m.Goroutines, m.GoHeapAllocBytes)
	}

	prom := srv.PrometheusMetrics()
	if !strings.Contains(prom, "air_read_queries_total{status=\"succeeded\"}") ||
		!strings.Contains(prom, "air_read_optimizer_plans_total{strategy=\"seq\"}") ||
		!strings.Contains(prom, "air_read_exec_memory_used_bytes") {
		t.Fatalf("unexpected Prometheus output:\n%s", prom)
	}
	// Ensure high-cardinality tenant IDs are NEVER emitted in Prometheus labels
	if strings.Contains(prom, "tenant_obs_12345") {
		t.Fatalf("high-cardinality tenant_id leaked into Prometheus metrics")
	}
}
