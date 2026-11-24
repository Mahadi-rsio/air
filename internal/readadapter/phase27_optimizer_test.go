package readadapter

import "testing"

func TestPhase27_CoveringIndexScanAvoidsHeapFetch(t *testing.T) {
	sc := NewStatsCache(64)
	at := NewAdaptiveTracker(64)
	opt := NewQueryOptimizer(DefaultCostModelConfig(), sc, at, nil)

	sc.Put(&TableStats{
		TenantID:   "t27",
		Generation: "g1",
		RelOID:     7001,
		RelPages:   1000,
		RelTuples:  50000,
		Columns: map[int16]ColumnStats{
			1: {AttNum: 1, Distinct: 500},
			2: {AttNum: 2, Distinct: 5000},
		},
	})

	tbl := DiscoveredTable{
		Name:   "events",
		RelOID: 7001,
		Columns: []DiscoveredColumn{
			{Name: "tenant_id", AttNum: 1, PGTypeOID: PGOIDInt4},
			{Name: "created_at", AttNum: 2, PGTypeOID: PGOIDInt8},
			{Name: "payload", AttNum: 3, PGTypeOID: PGOIDText},
		},
	}
	plans := []btreeIndexPlan{{
		Index: DiscoveredIndex{
			Name:        "idx_events_cover",
			IndexRelOID: 8001,
			IndNAtts:    2,
			IndNKeyAtts: 2,
			KeyAttNums:  []int16{1, 2},
		},
		KeyAllCols: []int{0, 1},
		KeyVisCols: []int{0, 1},
		Orderings:  []btreeKeyOrdering{{Desc: false}, {Desc: false}},
	}}
	allCols := []ColumnType{ColInt4, ColInt8, ColText}

	// Only columns 0 and 1 are needed -> covering index scan.
	decCover := opt.ChooseBestPlanWithNeededCols(
		tbl, plans, allCols,
		[]FilterConstraint{{AllColIdx: 0, AttNum: 1, Op: sqliteConstraintEQ}},
		[]OrderBySpec{{AllColIdx: 1, Desc: false}},
		2, []int{0, 1}, 1000, "t27", "g1",
	)
	if !decCover.Selected.IsCovering {
		t.Fatalf("expected covering index scan when needed cols %v are in index keys", []int{0, 1})
	}

	// Column 2 (payload) is also needed -> not covering.
	decNonCover := opt.ChooseBestPlanWithNeededCols(
		tbl, plans, allCols,
		[]FilterConstraint{{AllColIdx: 0, AttNum: 1, Op: sqliteConstraintEQ}},
		[]OrderBySpec{{AllColIdx: 1, Desc: false}},
		3, []int{0, 1, 2}, 1000, "t27", "g1",
	)
	if decNonCover.Selected.IsCovering {
		t.Fatalf("did not expect covering index when non-key column 2 is needed")
	}
	if decCover.Selected.EstimatedCost >= decNonCover.Selected.EstimatedCost {
		t.Fatalf("expected covering scan cost (%.3f) < non-covering scan cost (%.3f)",
			decCover.Selected.EstimatedCost, decNonCover.Selected.EstimatedCost)
	}
}

func TestPhase27_OrderByIndexScanWithoutWhere(t *testing.T) {
	sc := NewStatsCache(64)
	opt := NewQueryOptimizer(DefaultCostModelConfig(), sc, nil, nil)

	sc.Put(&TableStats{
		TenantID:   "t27",
		Generation: "g1",
		RelOID:     7002,
		RelPages:   800,
		RelTuples:  40000,
		Columns: map[int16]ColumnStats{
			1: {AttNum: 1, Distinct: 40000},
		},
	})

	tbl := DiscoveredTable{
		Name:   "items",
		RelOID: 7002,
		Columns: []DiscoveredColumn{
			{Name: "id", AttNum: 1, PGTypeOID: PGOIDInt8},
			{Name: "name", AttNum: 2, PGTypeOID: PGOIDText},
		},
	}
	plans := []btreeIndexPlan{{
		Index: DiscoveredIndex{
			Name:        "idx_items_id",
			IndexRelOID: 8002,
			IsUnique:    true,
			IndNAtts:    1,
			IndNKeyAtts: 1,
			KeyAttNums:  []int16{1},
		},
		KeyAllCols: []int{0},
		KeyVisCols: []int{0},
		Orderings:  []btreeKeyOrdering{{Desc: false}},
	}}
	allCols := []ColumnType{ColInt8, ColText}

	dec := opt.ChooseBestPlanWithNeededCols(
		tbl, plans, allCols,
		nil,
		[]OrderBySpec{{AllColIdx: 0, Desc: false}},
		1, []int{0}, 800, "t27", "g1",
	)
	var orderedCand *PlanCandidate
	for i := range dec.Candidates {
		if dec.Candidates[i].Strategy == StrategyIndexScanOrdered {
			orderedCand = &dec.Candidates[i]
			break
		}
	}
	if orderedCand == nil || orderedCand.Rejected {
		t.Fatalf("expected non-rejected StrategyIndexScanOrdered candidate without WHERE, got %+v", dec.Candidates)
	}
	if !orderedCand.OrderByConsumed {
		t.Fatalf("expected OrderByConsumed=true")
	}
}

func TestPhase27_BoundSelectivityAndJoinOrdering(t *testing.T) {
	ts := &TableStats{
		RelTuples: 10000,
		Columns: map[int16]ColumnStats{
			1: {AttNum: 1, NullFrac: 0.2, Distinct: 50},
		},
	}
	selParam := ts.EstimateBoundSelectivity(1, sqliteConstraintEQ, 10000, "abc", false)
	if selParam < 0.015 || selParam > 0.017 {
		t.Fatalf("expected ~0.016 selectivity for (1-0.2)/50, got %f", selParam)
	}
	selUniq := ts.EstimateBoundSelectivity(1, sqliteConstraintEQ, 10000, "abc", true)
	if selUniq > 0.0002 {
		t.Fatalf("expected ~1/10000 selectivity for unique EQ, got %f", selUniq)
	}
	joinSel := EstimateJoinSelectivity(1000, 50000, 100, 5000)
	if joinSel != 1.0/5000.0 {
		t.Fatalf("expected 1/5000 join selectivity, got %f", joinSel)
	}

	ordered := OrderJoinInputs([]JoinRelationStats{
		{TableName: "orders", EstimatedRows: 100000, HasJoinIndex: true, SelectivityAfter: 1.0},
		{TableName: "users", EstimatedRows: 5000, HasJoinIndex: true, SelectivityAfter: 0.05}, // 250 effective rows
		{TableName: "regions", EstimatedRows: 2000, HasJoinIndex: false, SelectivityAfter: 1.0},
	})
	if len(ordered) != 3 {
		t.Fatalf("expected 3 relations, got %d", len(ordered))
	}
	if ordered[0].TableName != "users" {
		t.Fatalf("expected smallest filtered relation 'users' first, got %s", ordered[0].TableName)
	}
	if ordered[1].TableName != "regions" {
		t.Fatalf("expected 'regions' (2000 rows) second, got %s", ordered[1].TableName)
	}
	if ordered[2].TableName != "orders" {
		t.Fatalf("expected 'orders' third, got %s", ordered[2].TableName)
	}
}
