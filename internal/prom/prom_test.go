package prom

import (
	"strings"
	"testing"
	"time"
)

func TestHistogramAndCounters(t *testing.T) {
	h := NewHistogram([]float64{0.01, 0.1, 1.0})
	h.Observe(5 * time.Millisecond)
	h.Observe(50 * time.Millisecond)
	h.Observe(2 * time.Second)

	var b strings.Builder
	h.WritePrometheus(&b, "test_latency_seconds", "Test latency")
	WriteCounter(&b, "test_total", "Test counter", 42)
	WriteGauge(&b, "test_active", "Test gauge", 3)

	out := b.String()
	for _, want := range []string{
		`test_latency_seconds_bucket{le="0.01"} 1`,
		`test_latency_seconds_bucket{le="0.1"} 2`,
		`test_latency_seconds_bucket{le="1"} 2`,
		`test_latency_seconds_bucket{le="+Inf"} 3`,
		`test_latency_seconds_count 3`,
		`test_total 42`,
		`test_active 3`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in prometheus output:\n%s", want, out)
		}
	}
}
