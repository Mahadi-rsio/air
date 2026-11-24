package readadapter

import (
	"fmt"
	"math"
	"sort"
)

// JoinAlgorithm identifies a physical join execution strategy.
type JoinAlgorithm string

const (
	JoinNestedLoop      JoinAlgorithm = "Nested Loop Join"
	JoinIndexNestedLoop JoinAlgorithm = "Index Nested Loop Join"
	JoinHash            JoinAlgorithm = "Hash Join"
	JoinSortMerge       JoinAlgorithm = "Sort-Merge Join"
)

// JoinType identifies logical join semantics.
type JoinType int

const (
	JoinInner JoinType = iota
	JoinLeft
)

// JoinPlanInput describes the inputs for cost-based join algorithm selection.
type JoinPlanInput struct {
	LeftRows             float64
	RightRows            float64
	BuildRowBytes        int64
	HasRightIndex        bool
	IsEquiJoin           bool
	InputsSorted         bool
	AvailableMemoryBytes int64
}

// JoinPlanDecision holds the selected join algorithm, estimated cost, and estimated output rows.
type JoinPlanDecision struct {
	Algorithm     JoinAlgorithm
	EstimatedCost float64
	EstimatedRows float64
	BuildLeft     bool
	Reason        string
}

// ChooseJoinAlgorithm selects the lowest-cost valid join algorithm given
// cardinality, index availability, sort order, and memory budget.
func ChooseJoinAlgorithm(in JoinPlanInput) JoinPlanDecision {
	l := math.Max(1.0, in.LeftRows)
	r := math.Max(1.0, in.RightRows)

	// Conservative join cardinality estimate: min(l, r) for equijoin, l*r*0.1 otherwise.
	estOut := math.Min(l, r)
	if !in.IsEquiJoin {
		estOut = l * r * 0.1
	}

	// 1. Index Nested Loop Join (when right side has a B-tree index and left is selective)
	if in.IsEquiJoin && in.HasRightIndex {
		inljCost := l * (math.Log2(r) + 1.0) * 0.5
		// Compare against Hash Join if unindexed build is even cheaper for huge left
		hashCost := (l + r) * 1.2
		if inljCost <= hashCost || l <= r*0.25 {
			return JoinPlanDecision{
				Algorithm:     JoinIndexNestedLoop,
				EstimatedCost: inljCost,
				EstimatedRows: estOut,
				Reason:        "right side indexed; O(L log R) lookup",
			}
		}
	}

	// 2. Tiny inputs: Nested Loop avoids hash/sort setup overhead
	if !in.IsEquiJoin || (l*r <= 2500) {
		return JoinPlanDecision{
			Algorithm:     JoinNestedLoop,
			EstimatedCost: l * r * 0.05,
			EstimatedRows: estOut,
			Reason:        "small input or non-equijoin",
		}
	}

	// 3. Memory-aware Hash Join vs Sort-Merge Join for medium/large equijoins
	buildRows := math.Min(l, r)
	buildLeft := l <= r
	rowBytes := in.BuildRowBytes
	if rowBytes <= 0 {
		rowBytes = 64
	}
	estHashBytes := int64(buildRows) * (rowBytes + 32)
	if in.InputsSorted {
		return JoinPlanDecision{
			Algorithm:     JoinSortMerge,
			EstimatedCost: (l + r) * 0.6,
			EstimatedRows: estOut,
			Reason:        "inputs already sorted; zero hash memory",
		}
	}

	if in.AvailableMemoryBytes <= 0 || estHashBytes <= in.AvailableMemoryBytes {
		return JoinPlanDecision{
			Algorithm:     JoinHash,
			EstimatedCost: (l + r) * 1.1,
			EstimatedRows: estOut,
			BuildLeft:     buildLeft,
			Reason:        "equijoin fits within hash memory budget",
		}
	}

	// Exceeds hash memory budget -> fall back to Sort-Merge Join
	smjCost := l*math.Log2(l)*0.5 + r*math.Log2(r)*0.5 + (l + r)
	return JoinPlanDecision{
		Algorithm:     JoinSortMerge,
		EstimatedCost: smjCost,
		EstimatedRows: estOut,
		Reason:        "hash table exceeds memory budget; using sort-merge",
	}
}

// JoinRow represents a materialized row for Go-native join execution.
type JoinRow []SQLiteValue

func joinKeyEqual(a, b SQLiteValue) bool {
	// Strict SQL NULL semantics: NULL never equals NULL.
	if a.IsNil || b.IsNil {
		return false
	}
	if a.Type == b.Type {
		switch a.Type {
		case 1: // SQLITE_INTEGER
			return a.Int64 == b.Int64
		case 2: // SQLITE_FLOAT
			return a.Float == b.Float
		}
	}
	return a.Text == b.Text
}

func joinKeyLess(a, b SQLiteValue) bool {
	if a.Type == 1 && b.Type == 1 {
		return a.Int64 < b.Int64
	}
	if a.Type == 2 && b.Type == 2 {
		return a.Float < b.Float
	}
	return a.Text < b.Text
}

func joinKeyString(v SQLiteValue) string {
	if v.Type == 1 && v.Text == "" {
		return fmt.Sprintf("%d", v.Int64)
	}
	return v.Text
}

// ExecuteHashJoin performs a memory-bounded hash join on left[leftCol] == right[rightCol].
// Enforces SQL NULL semantics (NULL keys never match) and QueryMemoryBudget.
func ExecuteHashJoin(
	left []JoinRow, leftCol int,
	right []JoinRow, rightCol int,
	jtype JoinType,
	budget *QueryMemoryBudget,
) ([]JoinRow, error) {
	// Build hash table on right (or left for INNER, always right for LEFT so left order/unmatched is preserved).
	ht := make(map[string][]int, len(right))
	for i, rrow := range right {
		if rightCol >= len(rrow) {
			continue
		}
		k := rrow[rightCol]
		if k.IsNil {
			continue // SQL NULL never joins
		}
		if budget != nil {
			if err := budget.Reserve(48 + int64(len(k.Text))); err != nil {
				return nil, err
			}
		}
		ks := joinKeyString(k)
		ht[ks] = append(ht[ks], i)
	}

	rightWidth := 0
	if len(right) > 0 {
		rightWidth = len(right[0])
	}
	out := make([]JoinRow, 0, min(len(left), len(right)))
	for _, lrow := range left {
		matched := false
		if leftCol < len(lrow) && !lrow[leftCol].IsNil {
			ks := joinKeyString(lrow[leftCol])
			for _, ridx := range ht[ks] {
				rrow := right[ridx]
				if joinKeyEqual(lrow[leftCol], rrow[rightCol]) {
					combined := make(JoinRow, len(lrow)+len(rrow))
					copy(combined, lrow)
					copy(combined[len(lrow):], rrow)
					out = append(out, combined)
					matched = true
				}
			}
		}
		if !matched && jtype == JoinLeft {
			combined := make(JoinRow, len(lrow)+rightWidth)
			copy(combined, lrow)
			for c := len(lrow); c < len(combined); c++ {
				combined[c] = SQLiteValue{IsNil: true}
			}
			out = append(out, combined)
		}
	}
	return out, nil
}

// ExecuteSortMergeJoin performs a sort-merge equijoin on left[leftCol] == right[rightCol]
// with strict SQL NULL semantics.
func ExecuteSortMergeJoin(
	left []JoinRow, leftCol int,
	right []JoinRow, rightCol int,
	jtype JoinType,
) []JoinRow {
	// Partition non-NULL rows for merge; NULL rows never match in INNER, emit padded in LEFT.
	var lNonNull, lNull []JoinRow
	for _, r := range left {
		if leftCol < len(r) && !r[leftCol].IsNil {
			lNonNull = append(lNonNull, r)
		} else if jtype == JoinLeft {
			lNull = append(lNull, r)
		}
	}
	var rNonNull []JoinRow
	for _, r := range right {
		if rightCol < len(r) && !r[rightCol].IsNil {
			rNonNull = append(rNonNull, r)
		}
	}

	sort.Slice(lNonNull, func(i, j int) bool {
		return joinKeyLess(lNonNull[i][leftCol], lNonNull[j][leftCol])
	})
	sort.Slice(rNonNull, func(i, j int) bool {
		return joinKeyLess(rNonNull[i][rightCol], rNonNull[j][rightCol])
	})

	rightWidth := 0
	if len(right) > 0 {
		rightWidth = len(right[0])
	}
	var out []JoinRow
	i, j := 0, 0
	for i < len(lNonNull) {
		lk := lNonNull[i][leftCol]
		for j < len(rNonNull) && joinKeyLess(rNonNull[j][rightCol], lk) {
			j++
		}
		if j < len(rNonNull) && joinKeyEqual(lk, rNonNull[j][rightCol]) {
			// Advance group of equal keys on both sides
			iEnd := i + 1
			for iEnd < len(lNonNull) && joinKeyEqual(lNonNull[iEnd][leftCol], lk) {
				iEnd++
			}
			jEnd := j + 1
			for jEnd < len(rNonNull) && joinKeyEqual(rNonNull[jEnd][rightCol], lk) {
				jEnd++
			}
			for li := i; li < iEnd; li++ {
				for rj := j; rj < jEnd; rj++ {
					combined := make(JoinRow, len(lNonNull[li])+len(rNonNull[rj]))
					copy(combined, lNonNull[li])
					copy(combined[len(lNonNull[li]):], rNonNull[rj])
					out = append(out, combined)
				}
			}
			i = iEnd
			j = jEnd
		} else {
			if jtype == JoinLeft {
				combined := make(JoinRow, len(lNonNull[i])+rightWidth)
				copy(combined, lNonNull[i])
				for c := len(lNonNull[i]); c < len(combined); c++ {
					combined[c] = SQLiteValue{IsNil: true}
				}
				out = append(out, combined)
			}
			i++
		}
	}
	for _, lr := range lNull {
		combined := make(JoinRow, len(lr)+rightWidth)
		copy(combined, lr)
		for c := len(lr); c < len(combined); c++ {
			combined[c] = SQLiteValue{IsNil: true}
		}
		out = append(out, combined)
	}
	return out
}

// ExecuteNestedLoopJoin performs a nested loop join on left[leftCol] == right[rightCol].
func ExecuteNestedLoopJoin(left []JoinRow, leftCol int, right []JoinRow, rightCol int, jtype JoinType) []JoinRow {
	rightWidth := 0
	if len(right) > 0 {
		rightWidth = len(right[0])
	}
	var out []JoinRow
	for _, lrow := range left {
		matched := false
		if leftCol < len(lrow) && !lrow[leftCol].IsNil {
			for _, rrow := range right {
				if rightCol < len(rrow) && joinKeyEqual(lrow[leftCol], rrow[rightCol]) {
					combined := make(JoinRow, len(lrow)+len(rrow))
					copy(combined, lrow)
					copy(combined[len(lrow):], rrow)
					out = append(out, combined)
					matched = true
				}
			}
		}
		if !matched && jtype == JoinLeft {
			combined := make(JoinRow, len(lrow)+rightWidth)
			copy(combined, lrow)
			for c := len(lrow); c < len(combined); c++ {
				combined[c] = SQLiteValue{IsNil: true}
			}
			out = append(out, combined)
		}
	}
	return out
}

// JoinRelationStats describes cardinality and index availability for one relation
// participating in a multi-table join.
type JoinRelationStats struct {
	TableName        string
	EstimatedRows    float64
	HasJoinIndex     bool
	SelectivityAfter float64
}

// OrderJoinInputs orders relations for a left-deep join tree, prioritizing smaller
// filtered cardinalities first and preferring indexed probes on subsequent inputs.
func OrderJoinInputs(rels []JoinRelationStats) []JoinRelationStats {
	if len(rels) <= 1 {
		out := make([]JoinRelationStats, len(rels))
		copy(out, rels)
		return out
	}
	remaining := make([]JoinRelationStats, len(rels))
	copy(remaining, rels)
	ordered := make([]JoinRelationStats, 0, len(rels))

	effectiveRows := func(r JoinRelationStats) float64 {
		sel := r.SelectivityAfter
		if sel <= 0 || sel > 1 {
			sel = 1.0
		}
		rows := r.EstimatedRows * sel
		if rows < 1 {
			rows = 1
		}
		return rows
	}

	// Pick smallest effective relation as outer driving table.
	bestIdx := 0
	bestRows := effectiveRows(remaining[0])
	for i := 1; i < len(remaining); i++ {
		if rows := effectiveRows(remaining[i]); rows < bestRows {
			bestRows = rows
			bestIdx = i
		}
	}
	ordered = append(ordered, remaining[bestIdx])
	remaining = append(remaining[:bestIdx], remaining[bestIdx+1:]...)

	// Greedily pick next inner relation: indexed joins get a 0.25x cost preference
	// over unindexed hash/nested-loop probes of similar cardinality.
	for len(remaining) > 0 {
		nextIdx := 0
		bestScore := math.MaxFloat64
		for i, r := range remaining {
			score := effectiveRows(r)
			if r.HasJoinIndex {
				score *= 0.25
			}
			if score < bestScore {
				bestScore = score
				nextIdx = i
			}
		}
		ordered = append(ordered, remaining[nextIdx])
		remaining = append(remaining[:nextIdx], remaining[nextIdx+1:]...)
	}
	return ordered
}
