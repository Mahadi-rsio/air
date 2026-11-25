package prom

import (
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"
)

// DefaultBucketsSec provides standard sub-millisecond to 15s latency buckets in seconds.
var DefaultBucketsSec = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0,
}

// Histogram is a lock-free, allocation-free latency histogram using atomic counters.
type Histogram struct {
	boundsNs []int64
	bounds   []float64
	buckets  []atomic.Uint64
	sumNs    atomic.Uint64
	count    atomic.Uint64
}

// NewHistogram creates a Histogram with the given upper bounds in seconds (or DefaultBucketsSec if nil).
func NewHistogram(boundsSec []float64) *Histogram {
	if len(boundsSec) == 0 {
		boundsSec = DefaultBucketsSec
	}
	b := make([]float64, len(boundsSec))
	bNs := make([]int64, len(boundsSec))
	for i, s := range boundsSec {
		b[i] = s
		bNs[i] = int64(s * 1e9)
	}
	return &Histogram{
		boundsNs: bNs,
		bounds:   b,
		buckets:  make([]atomic.Uint64, len(boundsSec)+1), // +1 for +Inf
	}
}

// Observe records a duration in the histogram without locking or allocating.
func (h *Histogram) Observe(d time.Duration) {
	if h == nil {
		return
	}
	ns := d.Nanoseconds()
	if ns < 0 {
		ns = 0
	}
	h.sumNs.Add(uint64(ns))
	h.count.Add(1)
	for i, bound := range h.boundsNs {
		if ns <= bound {
			h.buckets[i].Add(1)
			return
		}
	}
	h.buckets[len(h.boundsNs)].Add(1)
}

// ObserveCount records a dimensionless count value (e.g. layers scanned) into a histogram
// whose bounds were configured as raw numbers.
func (h *Histogram) ObserveValue(v float64) {
	if h == nil {
		return
	}
	if v < 0 || math.IsNaN(v) {
		v = 0
	}
	h.sumNs.Add(uint64(v))
	h.count.Add(1)
	for i, bound := range h.bounds {
		if v <= bound {
			h.buckets[i].Add(1)
			return
		}
	}
	h.buckets[len(h.bounds)].Add(1)
}

// WritePrometheus writes a Prometheus histogram family (cumulative buckets, _sum, _count).
func (h *Histogram) WritePrometheus(b *strings.Builder, name, help string) {
	if h == nil {
		return
	}
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
	var cum uint64
	for i, bound := range h.bounds {
		cum += h.buckets[i].Load()
		fmt.Fprintf(b, "%s_bucket{le=\"%g\"} %d\n", name, bound, cum)
	}
	cum += h.buckets[len(h.bounds)].Load()
	fmt.Fprintf(b, "%s_bucket{le=\"+Inf\"} %d\n", name, cum)
	fmt.Fprintf(b, "%s_sum %.6f\n", name, float64(h.sumNs.Load())/1e9)
	fmt.Fprintf(b, "%s_count %d\n", name, h.count.Load())
}

// WriteValueHistogram writes a dimensionless value histogram (where sum is not divided by 1e9).
func (h *Histogram) WriteValueHistogram(b *strings.Builder, name, help string) {
	if h == nil {
		return
	}
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
	var cum uint64
	for i, bound := range h.bounds {
		cum += h.buckets[i].Load()
		fmt.Fprintf(b, "%s_bucket{le=\"%g\"} %d\n", name, bound, cum)
	}
	cum += h.buckets[len(h.bounds)].Load()
	fmt.Fprintf(b, "%s_bucket{le=\"+Inf\"} %d\n", name, cum)
	fmt.Fprintf(b, "%s_sum %d\n", name, h.sumNs.Load())
	fmt.Fprintf(b, "%s_count %d\n", name, h.count.Load())
}

// WriteCounter writes a Prometheus counter metric.
func WriteCounter(b *strings.Builder, name, help string, val uint64) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, val)
}

// WriteGauge writes an integer Prometheus gauge metric.
func WriteGauge(b *strings.Builder, name, help string, val int64) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", name, help, name, name, val)
}

// WriteGaugeUint writes an unsigned integer Prometheus gauge metric.
func WriteGaugeUint(b *strings.Builder, name, help string, val uint64) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", name, help, name, name, val)
}
