package readadapter

import (
	"fmt"
	"strings"
)

// ExplainDiagnostics captures AIR's query plan, cost, pushdown, and runtime
// execution details for EXPLAIN / EXPLAIN ANALYZE output.
type ExplainDiagnostics struct {
	Route              string
	Strategy           ScanStrategy
	SelectedIndex      string
	EstimatedCost      float64
	EstimatedRows      int64
	ActualRows         int64
	HasAnalyze         bool
	ExecTimeMs         float64
	PagesRead          uint64
	CacheTier          string
	PredicatePushdown  bool
	ProjectionPushdown bool
	NeededCols         int
	TotalCols          int
	PlanCacheHit       bool
	Fallback           bool
	FallbackReason     string
	SQLitePlanLines    []string
}

// FormatAIRQueryPlan renders EXPLAIN-compatible diagnostic lines without exposing
// credentials, raw parameter values, or cross-tenant metadata.
func FormatAIRQueryPlan(d ExplainDiagnostics) []string {
	var lines []string
	lines = append(lines, "AIR QUERY PLAN")

	route := d.Route
	if route == "" {
		route = "ReadEngine (SQLite + air_pg)"
	}
	lines = append(lines, fmt.Sprintf("  Route: %s", route))

	strat := string(d.Strategy)
	if strat == "" {
		strat = "SQLite Query Plan"
	}
	if d.HasAnalyze {
		lines = append(lines, fmt.Sprintf("  -> %s (cost=%.2f rows=%d) (actual rows=%d time=%.3fms pages=%d)",
			strat, d.EstimatedCost, d.EstimatedRows, d.ActualRows, d.ExecTimeMs, d.PagesRead))
	} else {
		lines = append(lines, fmt.Sprintf("  -> %s (cost=%.2f rows=%d)",
			strat, d.EstimatedCost, d.EstimatedRows))
	}

	if d.SelectedIndex != "" {
		lines = append(lines, fmt.Sprintf("     Index: %s", d.SelectedIndex))
	}
	if d.CacheTier != "" {
		lines = append(lines, fmt.Sprintf("     Cache: %s", d.CacheTier))
	}
	if d.PredicatePushdown {
		lines = append(lines, "     Predicate Pushdown: Enabled")
	} else {
		lines = append(lines, "     Predicate Pushdown: Disabled")
	}
	if d.ProjectionPushdown && d.TotalCols > 0 {
		lines = append(lines, fmt.Sprintf("     Projection Pushdown: Enabled (%d/%d cols)", d.NeededCols, d.TotalCols))
	} else {
		lines = append(lines, "     Projection Pushdown: Disabled")
	}
	if d.PlanCacheHit {
		lines = append(lines, "     Plan Cache: Hit")
	} else {
		lines = append(lines, "     Plan Cache: Miss")
	}
	if d.Fallback {
		reason := d.FallbackReason
		if reason == "" {
			reason = "unsupported query"
		}
		lines = append(lines, fmt.Sprintf("     Fallback: Yes (%s)", reason))
	} else {
		lines = append(lines, "     Fallback: No")
	}
	for _, sl := range d.SQLitePlanLines {
		trimmed := strings.TrimSpace(sl)
		if trimmed != "" {
			lines = append(lines, fmt.Sprintf("     SQLite: %s", trimmed))
		}
	}
	return lines
}
