package readadapter

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// FallbackReason is a low-cardinality enum classifying why a read query fell back to PostgreSQL.
type FallbackReason int

const (
	FallbackReasonWindowFunction FallbackReason = iota
	FallbackReasonDistinctOn
	FallbackReasonILikeRegex
	FallbackReasonNumericArithAgg
	FallbackReasonInetOps
	FallbackReasonArrayOps
	FallbackReasonUnsupportedInterval
	FallbackReasonUnsupportedTime
	FallbackReasonUnsupportedEnum
	FallbackReasonUnsupportedDomain
	FallbackReasonUnsupportedType
	FallbackReasonToast
	FallbackReasonMultiXact
	FallbackReasonUnsupportedSyntax
	FallbackReasonUnsupportedFunction
	FallbackReasonUnsupportedOperator
	FallbackReasonOther
	numFallbackReasons
)

var fallbackReasonNames = [numFallbackReasons]string{
	FallbackReasonWindowFunction:      "window_function",
	FallbackReasonDistinctOn:          "distinct_on",
	FallbackReasonILikeRegex:          "ilike_regex",
	FallbackReasonNumericArithAgg:     "numeric_arith_agg",
	FallbackReasonInetOps:             "inet_ops",
	FallbackReasonArrayOps:            "array_ops",
	FallbackReasonUnsupportedInterval: "unsupported_type_interval",
	FallbackReasonUnsupportedTime:     "unsupported_type_time",
	FallbackReasonUnsupportedEnum:     "unsupported_type_enum",
	FallbackReasonUnsupportedDomain:   "unsupported_type_domain",
	FallbackReasonUnsupportedType:     "unsupported_type",
	FallbackReasonToast:               "toast",
	FallbackReasonMultiXact:           "multixact",
	FallbackReasonUnsupportedSyntax:   "unsupported_syntax",
	FallbackReasonUnsupportedFunction: "unsupported_function",
	FallbackReasonUnsupportedOperator: "unsupported_operator",
	FallbackReasonOther:               "other",
}

func (r FallbackReason) String() string {
	if r >= 0 && r < numFallbackReasons {
		return fallbackReasonNames[r]
	}
	return "other"
}

// QueryShape is a low-cardinality enum classifying the SQL query shape.
type QueryShape int

const (
	QueryShapePointLookup QueryShape = iota
	QueryShapeRangeScan
	QueryShapeAggGroupBy
	QueryShapeJoin
	QueryShapeWindow
	QueryShapeDistinctOn
	QueryShapePatternMatch
	QueryShapeSetOp
	QueryShapeCTE
	QueryShapeOther
	numQueryShapes
)

var queryShapeNames = [numQueryShapes]string{
	QueryShapePointLookup:  "point_lookup",
	QueryShapeRangeScan:    "range_scan",
	QueryShapeAggGroupBy:   "agg_groupby",
	QueryShapeJoin:         "join",
	QueryShapeWindow:       "window",
	QueryShapeDistinctOn:   "distinct_on",
	QueryShapePatternMatch: "pattern_match",
	QueryShapeSetOp:        "set_op",
	QueryShapeCTE:          "cte",
	QueryShapeOther:        "other",
}

func (s QueryShape) String() string {
	if s >= 0 && s < numQueryShapes {
		return queryShapeNames[s]
	}
	return "other"
}

const numShapeHistBuckets = 7

var shapeHistBoundsUs = [numShapeHistBuckets]uint64{
	500,     // 0.0005s
	1000,    // 0.001s
	5000,    // 0.005s
	10000,   // 0.01s
	50000,   // 0.05s
	250000,  // 0.25s
	1000000, // 1.0s
}

var shapeHistBoundLabels = [numShapeHistBuckets]string{
	"0.0005", "0.001", "0.005", "0.01", "0.05", "0.25", "1.0",
}

// FallbackRankEntry represents a single fallback reason's frequency and top query shape.
type FallbackRankEntry struct {
	Reason   string  `json:"reason"`
	Count    uint64  `json:"count"`
	SharePct float64 `json:"share_pct"`
	TopShape string  `json:"top_shape"`
}

// FallbackTelemetry tracks lock-free, low-cardinality fallback counters and a per-shape latency histogram.
type FallbackTelemetry struct {
	reasonCounts        [numFallbackReasons]atomic.Uint64
	shapeCounts         [numQueryShapes]atomic.Uint64
	shapeFallbackCounts [numQueryShapes][numFallbackReasons]atomic.Uint64
	shapeHistBuckets    [numQueryShapes][numShapeHistBuckets + 1]atomic.Uint64
	shapeHistSumUs      [numQueryShapes]atomic.Uint64
	shapeHistCount      [numQueryShapes]atomic.Uint64
}

// RecordQuery records a query's shape and latency in the query-shape histogram.
func (ft *FallbackTelemetry) RecordQuery(shape QueryShape, dur time.Duration) {
	if ft == nil {
		return
	}
	if shape < 0 || shape >= numQueryShapes {
		shape = QueryShapeOther
	}
	ft.shapeCounts[shape].Add(1)
	us := uint64(dur.Microseconds())
	ft.shapeHistSumUs[shape].Add(us)
	ft.shapeHistCount[shape].Add(1)
	for i, bound := range shapeHistBoundsUs {
		if us <= bound {
			ft.shapeHistBuckets[shape][i].Add(1)
			return
		}
	}
	ft.shapeHistBuckets[shape][numShapeHistBuckets].Add(1)
}

// RecordFallback records a fallback reason and its associated query shape.
func (ft *FallbackTelemetry) RecordFallback(shape QueryShape, reason FallbackReason) {
	if ft == nil {
		return
	}
	if shape < 0 || shape >= numQueryShapes {
		shape = QueryShapeOther
	}
	if reason < 0 || reason >= numFallbackReasons {
		reason = FallbackReasonOther
	}
	ft.reasonCounts[reason].Add(1)
	ft.shapeFallbackCounts[shape][reason].Add(1)
}

// ReasonCounts returns a map of non-zero fallback reason counts.
func (ft *FallbackTelemetry) ReasonCounts() map[string]uint64 {
	out := make(map[string]uint64)
	if ft == nil {
		return out
	}
	for i := 0; i < int(numFallbackReasons); i++ {
		if c := ft.reasonCounts[i].Load(); c > 0 {
			out[fallbackReasonNames[i]] = c
		}
	}
	return out
}

// RankedFallbacks returns all non-zero fallback reasons sorted by descending frequency.
func (ft *FallbackTelemetry) RankedFallbacks() []FallbackRankEntry {
	if ft == nil {
		return nil
	}
	var total uint64
	for i := 0; i < int(numFallbackReasons); i++ {
		total += ft.reasonCounts[i].Load()
	}
	if total == 0 {
		return nil
	}
	var entries []FallbackRankEntry
	for r := 0; r < int(numFallbackReasons); r++ {
		cnt := ft.reasonCounts[r].Load()
		if cnt == 0 {
			continue
		}
		var topShape QueryShape = QueryShapeOther
		var topShapeCnt uint64
		for s := 0; s < int(numQueryShapes); s++ {
			sc := ft.shapeFallbackCounts[s][r].Load()
			if sc > topShapeCnt {
				topShapeCnt = sc
				topShape = QueryShape(s)
			}
		}
		entries = append(entries, FallbackRankEntry{
			Reason:   fallbackReasonNames[r],
			Count:    cnt,
			SharePct: 100.0 * float64(cnt) / float64(total),
			TopShape: topShape.String(),
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Count != entries[j].Count {
			return entries[i].Count > entries[j].Count
		}
		return entries[i].Reason < entries[j].Reason
	})
	return entries
}

// WritePrometheus appends fallback_total{reason}, air_postgres_fallback_total{reason},
// and the query-shape histogram metrics to b.
func (ft *FallbackTelemetry) WritePrometheus(b *strings.Builder) {
	if ft == nil {
		return
	}
	b.WriteString("# HELP fallback_total Total read queries that fell back to PostgreSQL by reason\n")
	b.WriteString("# TYPE fallback_total counter\n")
	for i := 0; i < int(numFallbackReasons); i++ {
		c := ft.reasonCounts[i].Load()
		fmt.Fprintf(b, "fallback_total{reason=%q} %d\n", fallbackReasonNames[i], c)
	}

	b.WriteString("# HELP air_postgres_fallback_total Total read queries that fell back to PostgreSQL by reason\n")
	b.WriteString("# TYPE air_postgres_fallback_total counter\n")
	for _, legacyReason := range []string{"toast", "multixact", "unsupported_type", "unsupported_operator", "unsupported_function", "unsupported_syntax", "other"} {
		var c uint64
		switch legacyReason {
		case "toast":
			c = ft.reasonCounts[FallbackReasonToast].Load()
		case "multixact":
			c = ft.reasonCounts[FallbackReasonMultiXact].Load()
		case "unsupported_type":
			c = ft.reasonCounts[FallbackReasonUnsupportedType].Load() +
				ft.reasonCounts[FallbackReasonUnsupportedInterval].Load() +
				ft.reasonCounts[FallbackReasonUnsupportedTime].Load() +
				ft.reasonCounts[FallbackReasonUnsupportedEnum].Load() +
				ft.reasonCounts[FallbackReasonUnsupportedDomain].Load()
		case "unsupported_operator":
			c = ft.reasonCounts[FallbackReasonUnsupportedOperator].Load() +
				ft.reasonCounts[FallbackReasonNumericArithAgg].Load() +
				ft.reasonCounts[FallbackReasonInetOps].Load() +
				ft.reasonCounts[FallbackReasonArrayOps].Load()
		case "unsupported_function":
			c = ft.reasonCounts[FallbackReasonUnsupportedFunction].Load() +
				ft.reasonCounts[FallbackReasonWindowFunction].Load()
		case "unsupported_syntax":
			c = ft.reasonCounts[FallbackReasonUnsupportedSyntax].Load() +
				ft.reasonCounts[FallbackReasonDistinctOn].Load() +
				ft.reasonCounts[FallbackReasonILikeRegex].Load()
		case "other":
			c = ft.reasonCounts[FallbackReasonOther].Load()
		}
		fmt.Fprintf(b, "air_postgres_fallback_total{reason=%q} %d\n", legacyReason, c)
	}

	b.WriteString("# HELP air_read_query_shape_duration_seconds Query latency histogram by query shape\n")
	b.WriteString("# TYPE air_read_query_shape_duration_seconds histogram\n")
	for s := 0; s < int(numQueryShapes); s++ {
		cnt := ft.shapeHistCount[s].Load()
		if cnt == 0 {
			continue
		}
		shapeName := queryShapeNames[s]
		var cum uint64
		for bi := 0; bi < numShapeHistBuckets; bi++ {
			cum += ft.shapeHistBuckets[s][bi].Load()
			fmt.Fprintf(b, "air_read_query_shape_duration_seconds_bucket{shape=%q,le=%q} %d\n", shapeName, shapeHistBoundLabels[bi], cum)
		}
		cum += ft.shapeHistBuckets[s][numShapeHistBuckets].Load()
		fmt.Fprintf(b, "air_read_query_shape_duration_seconds_bucket{shape=%q,le=\"+Inf\"} %d\n", shapeName, cum)
		fmt.Fprintf(b, "air_read_query_shape_duration_seconds_sum{shape=%q} %.6f\n", shapeName, float64(ft.shapeHistSumUs[s].Load())/1e6)
		fmt.Fprintf(b, "air_read_query_shape_duration_seconds_count{shape=%q} %d\n", shapeName, cnt)
	}

	b.WriteString("# HELP air_read_query_shape_fallbacks_total Fallbacks by query shape and reason\n")
	b.WriteString("# TYPE air_read_query_shape_fallbacks_total counter\n")
	for s := 0; s < int(numQueryShapes); s++ {
		for r := 0; r < int(numFallbackReasons); r++ {
			if c := ft.shapeFallbackCounts[s][r].Load(); c > 0 {
				fmt.Fprintf(b, "air_read_query_shape_fallbacks_total{shape=%q,reason=%q} %d\n",
					queryShapeNames[s], fallbackReasonNames[r], c)
			}
		}
	}
}

// containsASCIIFold reports whether s contains substr using ASCII case-insensitive matching
// with zero heap allocations.
func containsASCIIFold(s, substr string) bool {
	n := len(substr)
	if n == 0 {
		return true
	}
	if len(s) < n {
		return false
	}
	for i := 0; i <= len(s)-n; i++ {
		match := true
		for j := 0; j < n; j++ {
			a := s[i+j]
			b := substr[j]
			if a >= 'A' && a <= 'Z' {
				a += 'a' - 'A'
			}
			if b >= 'A' && b <= 'Z' {
				b += 'a' - 'A'
			}
			if a != b {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// ClassifyQueryShape classifies a SQL query into a low-cardinality QueryShape with zero allocations.
func ClassifyQueryShape(sql string) QueryShape {
	if containsASCIIFold(sql, "distinct on") {
		return QueryShapeDistinctOn
	}
	if containsASCIIFold(sql, "over (") || containsASCIIFold(sql, "over(") {
		return QueryShapeWindow
	}
	if containsASCIIFold(sql, " union ") || containsASCIIFold(sql, " intersect ") || containsASCIIFold(sql, " except ") {
		return QueryShapeSetOp
	}
	if containsASCIIFold(sql, "with ") {
		return QueryShapeCTE
	}
	if containsASCIIFold(sql, " join ") {
		return QueryShapeJoin
	}
	if containsASCIIFold(sql, " ilike ") || containsASCIIFold(sql, " like ") ||
		containsASCIIFold(sql, " similar to ") || containsASCIIFold(sql, " ~ ") ||
		containsASCIIFold(sql, " ~* ") || containsASCIIFold(sql, " !~ ") || containsASCIIFold(sql, " !~* ") {
		return QueryShapePatternMatch
	}
	if containsASCIIFold(sql, "group by") || containsASCIIFold(sql, " having ") ||
		containsASCIIFold(sql, "count(") || containsASCIIFold(sql, "sum(") ||
		containsASCIIFold(sql, "avg(") || containsASCIIFold(sql, "min(") || containsASCIIFold(sql, "max(") {
		return QueryShapeAggGroupBy
	}
	if containsASCIIFold(sql, " between ") || containsASCIIFold(sql, "order by") ||
		strings.Contains(sql, "<") || strings.Contains(sql, ">") {
		return QueryShapeRangeScan
	}
	if containsASCIIFold(sql, " where ") && strings.Contains(sql, "=") {
		return QueryShapePointLookup
	}
	return QueryShapeOther
}

// ClassifyFallbackReason classifies a fallback error and its SQL query into a low-cardinality FallbackReason.
func ClassifyFallbackReason(sql string, err error) FallbackReason {
	if err == nil {
		return FallbackReasonOther
	}
	if errors.Is(err, ErrTOASTUnsupported) {
		return FallbackReasonToast
	}
	if errors.Is(err, ErrMultiXactUnsupported) {
		return FallbackReasonMultiXact
	}

	msg := err.Error()
	if containsASCIIFold(sql, "distinct on") {
		return FallbackReasonDistinctOn
	}
	if containsASCIIFold(sql, "over (") || containsASCIIFold(sql, "over(") || containsASCIIFold(msg, "window") {
		return FallbackReasonWindowFunction
	}
	if containsASCIIFold(sql, " ilike ") || containsASCIIFold(sql, " similar to ") ||
		containsASCIIFold(sql, " ~ ") || containsASCIIFold(sql, " ~* ") ||
		containsASCIIFold(sql, " !~ ") || containsASCIIFold(sql, " !~* ") ||
		containsASCIIFold(msg, "ilike") {
		return FallbackReasonILikeRegex
	}
	if containsASCIIFold(msg, "numeric aggregate") || containsASCIIFold(msg, "numeric arithmetic") ||
		containsASCIIFold(msg, "numeric multiplication") || containsASCIIFold(msg, "numeric subtraction") {
		return FallbackReasonNumericArithAgg
	}
	if containsASCIIFold(msg, "inet containment") ||
		(containsASCIIFold(msg, "network") && (containsASCIIFold(sql, "<<") || containsASCIIFold(sql, ">>"))) ||
		containsASCIIFold(msg, "\"host\"") || containsASCIIFold(msg, "\"network\"") ||
		containsASCIIFold(msg, "\"masklen\"") || containsASCIIFold(msg, "\"broadcast\"") {
		return FallbackReasonInetOps
	}
	if containsASCIIFold(msg, "array") || containsASCIIFold(msg, "unnest") ||
		containsASCIIFold(msg, "cardinality") || containsASCIIFold(msg, "= any(") ||
		containsASCIIFold(msg, "= all(") {
		return FallbackReasonArrayOps
	}
	if errors.Is(err, ErrUnsupportedType) {
		switch {
		case containsASCIIFold(msg, "interval") || containsASCIIFold(msg, "type 1186"):
			return FallbackReasonUnsupportedInterval
		case containsASCIIFold(msg, "time") || containsASCIIFold(msg, "type 1083") || containsASCIIFold(msg, "type 1266"):
			return FallbackReasonUnsupportedTime
		case containsASCIIFold(msg, "enum"):
			return FallbackReasonUnsupportedEnum
		case containsASCIIFold(msg, "domain"):
			return FallbackReasonUnsupportedDomain
		default:
			return FallbackReasonUnsupportedType
		}
	}
	if containsASCIIFold(msg, "no such function") {
		return FallbackReasonUnsupportedFunction
	}
	if containsASCIIFold(msg, "syntax error") || containsASCIIFold(msg, "near \"") {
		return FallbackReasonUnsupportedSyntax
	}
	if containsASCIIFold(msg, "unrecognized token") || containsASCIIFold(msg, "operator") {
		return FallbackReasonUnsupportedOperator
	}
	return FallbackReasonOther
}
