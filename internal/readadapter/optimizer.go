package readadapter

import (
	"math"
	"runtime"
	"sync/atomic"
	"time"
)

// CostModelConfig parameterizes AIR's storage-tier and CPU cost model.
type CostModelConfig struct {
	L1PageCost       float64
	L2PageCost       float64
	L3PageCost       float64
	SeqPageFactor    float64
	RandomPageFactor float64
	TupleEvalCost    float64
	IndexTupleCost   float64
	FilterEvalCost   float64
	ColDecodeCost    float64
	SortTupleCost    float64
}

// DefaultCostModelConfig returns hardware-balanced defaults reflecting AIR's
// L1 RAM -> L2 NVMe -> L3 Pageserver latency hierarchy.
func DefaultCostModelConfig() CostModelConfig {
	return CostModelConfig{
		L1PageCost:       0.10,
		L2PageCost:       1.00,
		L3PageCost:       10.00,
		SeqPageFactor:    0.50,
		RandomPageFactor: 1.50,
		TupleEvalCost:    0.01,
		IndexTupleCost:   0.005,
		FilterEvalCost:   0.002,
		ColDecodeCost:    0.001,
		SortTupleCost:    0.02,
	}
}

// ScanStrategy identifies the physical scan operator chosen by the optimizer.
type ScanStrategy string

const (
	StrategySeqScan          ScanStrategy = "Sequential Scan"
	StrategyIndexScan        ScanStrategy = "Index Scan"
	StrategyIndexScanOrdered ScanStrategy = "Index Scan (Ordered)"
)

// OrderBySpec describes a requested ORDER BY term from SQLite xBestIndex.
type OrderBySpec struct {
	AllColIdx int
	Desc      bool
}

// FilterConstraint describes a pushed-down predicate column and operator.
type FilterConstraint struct {
	AllColIdx int
	AttNum    int16
	Op        int
}

// PlanCandidate represents one evaluated physical execution strategy.
type PlanCandidate struct {
	Strategy          ScanStrategy
	IndexPlanIdx      int
	IndexName         string
	EstimatedCost     float64
	EstimatedRows     int64
	EstimatedPages    float64
	OrderByConsumed   bool
	IsCovering        bool
	ExpectedCacheTier string
	ParallelWorkers   int
	Rejected          bool
	RejectReason      string
}

// PlanDecision captures the winning execution strategy and all evaluated candidates.
type PlanDecision struct {
	Selected           PlanCandidate
	Candidates         []PlanCandidate
	PredicatePushdown  bool
	ProjectionPushdown bool
	NeededCols         int
	TotalCols          int
}

// OptimizerStats reports low-overhead atomic optimizer and accuracy metrics.
type OptimizerStats struct {
	PlansEvaluated          uint64 `json:"plans_evaluated"`
	SeqScansChosen          uint64 `json:"seq_scans_chosen"`
	IndexScansChosen        uint64 `json:"index_scans_chosen"`
	OrderedIndexScansChosen uint64 `json:"ordered_index_scans_chosen"`
	ParallelScansChosen     uint64 `json:"parallel_scans_chosen"`
	PlanningTimeNsTotal     uint64 `json:"planning_time_ns_total"`
	EstimatedRowsTotal      uint64 `json:"estimated_rows_total"`
	ActualRowsTotal         uint64 `json:"actual_rows_total"`
	EstimatedCostMilliTotal uint64 `json:"estimated_cost_milli_total"`
	ActualExecNsTotal       uint64 `json:"actual_exec_ns_total"`
	AdaptiveAdjustments     uint64 `json:"adaptive_adjustments"`
}

// QueryOptimizer selects the lowest-cost valid execution plan for air_pg scans.
type QueryOptimizer struct {
	cfg      CostModelConfig
	stats    *StatsCache
	adaptive *AdaptiveTracker
	cache    *TieredCacheBackend

	plansEvaluated          atomic.Uint64
	seqScansChosen          atomic.Uint64
	indexScansChosen        atomic.Uint64
	orderedIndexScansChosen atomic.Uint64
	parallelScansChosen     atomic.Uint64
	planningTimeNsTotal     atomic.Uint64
	estimatedRowsTotal      atomic.Uint64
	actualRowsTotal         atomic.Uint64
	estimatedCostMilliTotal atomic.Uint64
	actualExecNsTotal       atomic.Uint64
	adaptiveAdjustments     atomic.Uint64
}

// Stats returns a snapshot of OptimizerStats.
func (o *QueryOptimizer) Stats() OptimizerStats {
	if o == nil {
		return OptimizerStats{}
	}
	return OptimizerStats{
		PlansEvaluated:          o.plansEvaluated.Load(),
		SeqScansChosen:          o.seqScansChosen.Load(),
		IndexScansChosen:        o.indexScansChosen.Load(),
		OrderedIndexScansChosen: o.orderedIndexScansChosen.Load(),
		ParallelScansChosen:     o.parallelScansChosen.Load(),
		PlanningTimeNsTotal:     o.planningTimeNsTotal.Load(),
		EstimatedRowsTotal:      o.estimatedRowsTotal.Load(),
		ActualRowsTotal:         o.actualRowsTotal.Load(),
		EstimatedCostMilliTotal: o.estimatedCostMilliTotal.Load(),
		ActualExecNsTotal:       o.actualExecNsTotal.Load(),
		AdaptiveAdjustments:     o.adaptiveAdjustments.Load(),
	}
}

// RecordExecutionFeedback records actual vs estimated rows and execution time.
func (o *QueryOptimizer) RecordExecutionFeedback(estRows, actualRows uint64, estCost float64, actualNs uint64) {
	if o == nil {
		return
	}
	o.estimatedRowsTotal.Add(estRows)
	o.actualRowsTotal.Add(actualRows)
	if estCost > 0 {
		o.estimatedCostMilliTotal.Add(uint64(estCost * 1000))
	}
	o.actualExecNsTotal.Add(actualNs)
}

// NewQueryOptimizer creates a QueryOptimizer with the given configuration and optional dependencies.
func NewQueryOptimizer(cfg CostModelConfig, stats *StatsCache, adaptive *AdaptiveTracker, cache *TieredCacheBackend) *QueryOptimizer {
	if cfg.L1PageCost <= 0 {
		cfg = DefaultCostModelConfig()
	}
	return &QueryOptimizer{
		cfg:      cfg,
		stats:    stats,
		adaptive: adaptive,
		cache:    cache,
	}
}

func (o *QueryOptimizer) cacheTierProbabilities(tenantID string, relOID uint32) (l1Prob, l2Prob, l3Prob float64, tierLabel string) {
	// Start with conservative warm-cache priors.
	l1Prob, l2Prob, l3Prob = 0.50, 0.15, 0.35
	if o.cache != nil {
		cs := o.cache.Stats()
		total := cs.L1Hits + cs.L1Misses
		if total > 0 {
			l1Prob = float64(cs.L1Hits) / float64(total)
			rem := 1.0 - l1Prob
			l2Total := cs.L2Hits + cs.L2Misses
			if l2Total > 0 {
				l2Prob = rem * (float64(cs.L2Hits) / float64(l2Total))
			} else {
				l2Prob = 0
			}
			l3Prob = math.Max(0, 1.0-l1Prob-l2Prob)
		}
	}
	if o.adaptive != nil && tenantID != "" && relOID != 0 {
		if fb, ok := o.adaptive.Get(tenantID, relOID); ok && fb.SampleCount > 0 && (fb.L1HitRatio > 0 || fb.L2HitRatio > 0) {
			l1Prob = 0.5*l1Prob + 0.5*fb.L1HitRatio
			l2Prob = 0.5*l2Prob + 0.5*fb.L2HitRatio
			l3Prob = math.Max(0, 1.0-l1Prob-l2Prob)
		}
	}
	switch {
	case l1Prob >= 0.75:
		tierLabel = "L1"
	case l1Prob+l2Prob >= 0.70:
		tierLabel = "L1/L2"
	default:
		tierLabel = "L1/L2/L3"
	}
	return l1Prob, l2Prob, l3Prob, tierLabel
}

func (o *QueryOptimizer) chooseParallelWorkers(nblocks uint32, serialCost float64) int {
	cpus := runtime.GOMAXPROCS(0)
	// Measured on 2-vCPU AMD EPYC 7763: 128-page L1/in-memory parallel scan (1.20ms)
	// is slower than serial (0.74ms) due to synchronization overhead. Retain serial
	// execution on <=2 vCPUs or scans under 512 pages (~4 MiB).
	if cpus < 4 || nblocks < 512 {
		return 1
	}
	workers := cpus / 2
	if workers < 2 {
		workers = 2
	}
	if workers > 4 {
		workers = 4
	}
	// Model parallel startup + merge overhead vs CPU/I/O speedup.
	const parallelStartupCost = 24.0
	parallelCost := parallelStartupCost + serialCost/(0.75*float64(workers))
	if parallelCost < serialCost*0.80 {
		return workers
	}
	return 1
}

// ShouldParallelize returns true and the recommended worker count if parallel
// sequential scanning is cheaper than serial scanning given current CPU load.
func (o *QueryOptimizer) ShouldParallelize(cand PlanCandidate, activeGlobalQueries int) (bool, int) {
	cpus := runtime.GOMAXPROCS(0)
	if cand.Strategy != StrategySeqScan || cand.ParallelWorkers <= 1 || cpus < 2 {
		return false, 1
	}
	// Avoid CPU oversubscription when concurrent tenant queries already saturate cores.
	if activeGlobalQueries >= cpus {
		return false, 1
	}
	return true, cand.ParallelWorkers
}
// and returns the lowest-cost valid PlanDecision.
func (o *QueryOptimizer) ChooseBestPlan(
	tbl DiscoveredTable,
	indexPlans []btreeIndexPlan,
	allCols []ColumnType,
	constraints []FilterConstraint,
	orderBy []OrderBySpec,
	neededCols int,
	nblocksHint uint32,
	tenantID, generation string,
) PlanDecision {
	return o.ChooseBestPlanWithNeededCols(
		tbl, indexPlans, allCols, constraints, orderBy,
		neededCols, nil, nblocksHint, tenantID, generation,
	)
}

// ChooseBestPlanWithNeededCols evaluates scan candidates and selects the
// lowest-cost plan, using neededAllCols (when non-empty) to detect covering
// index scans that avoid heap page fetches.
func (o *QueryOptimizer) ChooseBestPlanWithNeededCols(
	tbl DiscoveredTable,
	indexPlans []btreeIndexPlan,
	allCols []ColumnType,
	constraints []FilterConstraint,
	orderBy []OrderBySpec,
	neededCols int,
	neededAllCols []int,
	nblocksHint uint32,
	tenantID, generation string,
) PlanDecision {
	planStart := time.Now()
	totalCols := len(allCols)
	if neededCols <= 0 || neededCols > totalCols {
		neededCols = totalCols
	}

	var tstats *TableStats
	if o.stats != nil {
		tstats, _ = o.stats.Get(tenantID, generation, tbl.RelOID)
	}
	if tstats == nil {
		tstats = &TableStats{
			TenantID:      tenantID,
			Generation:    generation,
			RelOID:        tbl.RelOID,
			RelPages:      tbl.RelPages,
			RelTuples:     tbl.RelTuples,
			RelAllVisible: tbl.RelAllVisible,
			Columns:       tbl.ColumnStats,
		}
	}

	nblocks := nblocksHint
	if nblocks == 0 && tstats.RelPages > 0 {
		nblocks = uint32(tstats.RelPages)
	}
	// When relation page count and tuple stats are both unknown (0), use a
	// conservative 10-page prior (matching PostgreSQL's un-analyzed table prior)
	// so indexed point lookups are preferred unless the table is known to be tiny.
	if nblocks == 0 {
		if tstats.RelTuples > 0 {
			nblocks = 1
		} else {
			nblocks = 10
		}
	}

	baseRows := tstats.EstimatedRows(nblocks)
	l1Prob, l2Prob, l3Prob, tierLabel := o.cacheTierProbabilities(tenantID, tbl.RelOID)
	pageCost := l1Prob*o.cfg.L1PageCost + l2Prob*o.cfg.L2PageCost + l3Prob*o.cfg.L3PageCost

	selFactor := 1.0
	if o.adaptive != nil && tenantID != "" && tbl.RelOID != 0 {
		if fb, ok := o.adaptive.Get(tenantID, tbl.RelOID); ok && fb.SampleCount > 0 {
			selFactor = fb.SelectivityFactor
			if selFactor != 1.0 {
				o.adaptiveAdjustments.Add(1)
			}
		}
	}

	// Combined selectivity across all pushed-down constraints.
	seqSel := 1.0
	for _, c := range constraints {
		seqSel *= tstats.EstimateSelectivity(c.AttNum, c.Op, baseRows, false)
	}
	seqOutRows := clampRows(baseRows * seqSel * selFactor)

	// 1. Candidate: Sequential Heap Scan
	seqPages := float64(nblocks)
	seqIOCost := seqPages * pageCost * o.cfg.SeqPageFactor
	seqCPUCost := baseRows * (o.cfg.TupleEvalCost +
		float64(len(constraints))*o.cfg.FilterEvalCost +
		float64(neededCols)*o.cfg.ColDecodeCost)
	seqSortCost := 0.0
	if len(orderBy) > 0 && seqOutRows > 1 {
		seqSortCost = seqOutRows * math.Log2(seqOutRows) * o.cfg.SortTupleCost
	}
	seqTotalCost := seqIOCost + seqCPUCost + seqSortCost
	parallelWorkers := o.chooseParallelWorkers(nblocks, seqTotalCost)
	seqCand := PlanCandidate{
		Strategy:          StrategySeqScan,
		IndexPlanIdx:      -1,
		EstimatedCost:     seqTotalCost,
		EstimatedRows:     int64(math.Ceil(seqOutRows)),
		EstimatedPages:    seqPages,
		OrderByConsumed:   false,
		ExpectedCacheTier: tierLabel,
		ParallelWorkers:   parallelWorkers,
	}

	candidates := make([]PlanCandidate, 0, 1+len(indexPlans))
	candidates = append(candidates, seqCand)
	best := seqCand

	// 2. Candidate: Each B-tree Index Scan
	for i, plan := range indexPlans {
		cand := o.evaluateIndexCandidate(
			i, plan, tbl, tstats, constraints, orderBy,
			neededCols, neededAllCols, baseRows, seqPages, pageCost, selFactor, tierLabel,
		)
		candidates = append(candidates, cand)
		if !cand.Rejected && cand.EstimatedCost < best.EstimatedCost {
			best = cand
		}
	}

	o.plansEvaluated.Add(1)
	o.planningTimeNsTotal.Add(uint64(time.Since(planStart).Nanoseconds()))
	switch best.Strategy {
	case StrategySeqScan:
		o.seqScansChosen.Add(1)
	case StrategyIndexScan:
		o.indexScansChosen.Add(1)
	case StrategyIndexScanOrdered:
		o.orderedIndexScansChosen.Add(1)
	}
	if best.ParallelWorkers > 1 {
		o.parallelScansChosen.Add(1)
	}

	return PlanDecision{
		Selected:           best,
		Candidates:         candidates,
		PredicatePushdown:  len(constraints) > 0,
		ProjectionPushdown: neededCols < totalCols,
		NeededCols:         neededCols,
		TotalCols:          totalCols,
	}
}

func (o *QueryOptimizer) evaluateIndexCandidate(
	planIdx int,
	plan btreeIndexPlan,
	tbl DiscoveredTable,
	tstats *TableStats,
	constraints []FilterConstraint,
	orderBy []OrderBySpec,
	neededCols int,
	neededAllCols []int,
	baseRows float64,
	seqPages float64,
	pageCost float64,
	selFactor float64,
	tierLabel string,
) PlanCandidate {
	cand := PlanCandidate{
		Strategy:          StrategyIndexScan,
		IndexPlanIdx:      planIdx,
		IndexName:         plan.Index.Name,
		ExpectedCacheTier: tierLabel,
	}

	// Check if index leading column has a usable B-tree constraint.
	if len(plan.KeyAllCols) == 0 {
		cand.Rejected = true
		cand.RejectReason = "index has no key columns"
		return cand
	}

	matchedPrefixCols := 0
	idxSel := 1.0
	hasLeadingEQ := false
	hasLeadingRange := false

	for kPos, keyAllCol := range plan.KeyAllCols {
		colHasEQ := false
		colHasRange := false
		var bestColSel float64 = 1.0
		for _, c := range constraints {
			if c.AllColIdx != keyAllCol {
				continue
			}
			// B-tree index scan supports EQ, GT, GE, LT, LE (not NE/ISNULL in current btree.go).
			switch c.Op {
			case sqliteConstraintEQ:
				colHasEQ = true
				isUniq := plan.Index.IsUnique && plan.Index.IndNKeyAtts == 1 && kPos == 0
				s := tstats.EstimateSelectivity(c.AttNum, c.Op, baseRows, isUniq)
				if s < bestColSel {
					bestColSel = s
				}
			case sqliteConstraintGT, sqliteConstraintGE, sqliteConstraintLT, sqliteConstraintLE:
				colHasRange = true
				s := tstats.EstimateSelectivity(c.AttNum, c.Op, baseRows, false)
				if s < bestColSel {
					bestColSel = s
				}
			}
		}
		if !colHasEQ && !colHasRange {
			break
		}
		if kPos == 0 {
			hasLeadingEQ = colHasEQ
			hasLeadingRange = colHasRange
		}
		matchedPrefixCols++
		idxSel *= bestColSel
		if !colHasEQ {
			// B-tree prefix matching stops after the first range column.
			break
		}
	}

	// Check if ORDER BY is satisfied by this index's key ordering, skipping any
	// leading key columns fixed to a constant by an equality predicate.
	eqPrefix := 0
	for kPos, keyAllCol := range plan.KeyAllCols {
		hasEQ := false
		for _, c := range constraints {
			if c.AllColIdx == keyAllCol && c.Op == sqliteConstraintEQ {
				hasEQ = true
				break
			}
		}
		if !hasEQ {
			break
		}
		eqPrefix = kPos + 1
	}

	orderConsumed := false
	if len(orderBy) > 0 {
		for _, start := range []int{eqPrefix, 0} {
			if start+len(orderBy) <= len(plan.KeyAllCols) && start+len(orderBy) <= len(plan.Orderings) {
				matched := true
				for j, ob := range orderBy {
					if ob.AllColIdx != plan.KeyAllCols[start+j] || ob.Desc != plan.Orderings[start+j].Desc {
						matched = false
						break
					}
				}
				if matched {
					orderConsumed = true
					break
				}
			}
		}
	}

	if !hasLeadingEQ && !hasLeadingRange && !orderConsumed {
		cand.Rejected = true
		cand.RejectReason = "no usable predicate on leading index column"
		return cand
	}

	// Estimate matched rows and B-tree traversal cost.
	matchedRows := clampRows(baseRows * idxSel * selFactor)
	if plan.Index.IsUnique && matchedPrefixCols == int(plan.Index.IndNKeyAtts) && hasLeadingEQ && !hasLeadingRange {
		matchedRows = 1.0
	}

	// B-tree depth: metapage + internal levels (1 for <=1k rows, 2 for <=100k, 3+ for larger).
	treeDepth := 1.0
	if baseRows > 100000 {
		treeDepth = 3.0
	} else if baseRows > 1000 {
		treeDepth = 2.0
	}
	leafPages := math.Max(1.0, matchedRows/120.0)
	// Heap pages fetched (capped at total table pages using Mackert-Lohman approximation).
	// Covering index scans satisfy all needed columns from the index and skip heap fetches.
	covering := isCoveringIndex(plan, neededAllCols, constraints)
	heapPages := 0.0
	if !covering {
		heapPages = seqPages
		if matchedRows < seqPages {
			heapPages = matchedRows
		}
		if heapPages < 1.0 {
			heapPages = 1.0
		}
	}
	// Metapage and upper internal pages are hot shared pages (modeled at L1 cost);
	// leaf and heap page fetches use the full cache-weighted random page cost.
	hotInternalPages := 1.0 + math.Max(0, treeDepth-1.0)
	coldDataPages := 1.0 + leafPages + heapPages
	totalIndexPages := hotInternalPages + coldDataPages

	ioCost := hotInternalPages*o.cfg.L1PageCost + coldDataPages*pageCost*o.cfg.RandomPageFactor
	cpuCost := matchedRows * (o.cfg.IndexTupleCost + o.cfg.TupleEvalCost + float64(neededCols)*o.cfg.ColDecodeCost)

	sortCost := 0.0
	if len(orderBy) > 0 && !orderConsumed && matchedRows > 1 {
		sortCost = matchedRows * math.Log2(matchedRows) * o.cfg.SortTupleCost
	}
	if orderConsumed {
		cand.Strategy = StrategyIndexScanOrdered
	}

	cand.EstimatedCost = ioCost + cpuCost + sortCost
	cand.EstimatedRows = int64(math.Ceil(matchedRows))
	cand.EstimatedPages = totalIndexPages
	cand.OrderByConsumed = orderConsumed
	cand.IsCovering = covering
	return cand
}

func isCoveringIndex(plan btreeIndexPlan, neededAllCols []int, constraints []FilterConstraint) bool {
	if len(neededAllCols) == 0 || len(plan.KeyAllCols) == 0 {
		return false
	}
	inKey := func(col int) bool {
		for _, k := range plan.KeyAllCols {
			if k == col {
				return true
			}
		}
		return false
	}
	for _, col := range neededAllCols {
		if !inKey(col) {
			return false
		}
	}
	for _, c := range constraints {
		if !inKey(c.AllColIdx) {
			return false
		}
	}
	return true
}

func clampRows(r float64) float64 {
	if r < 1.0 {
		return 1.0
	}
	return r
}
