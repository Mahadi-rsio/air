package readadapter

import (
	"context"
	"testing"
)

func TestP5_SecondaryAndCoveringIndexPlans(t *testing.T) {
	tbl := DiscoveredTable{
		Name: "orders",
		Columns: []DiscoveredColumn{
			{Name: "id", AttNum: 1, ColType: ColInt4},
			{Name: "cust_id", AttNum: 2, ColType: ColInt4},
			{Name: "total", AttNum: 3, ColType: ColInt8},
			{Name: "note", AttNum: 4, ColType: ColText},
		},
		Indexes: []DiscoveredIndex{
			{
				Name:        "orders_pkey",
				IndexRelOID: 1001,
				IsPrimary:   true,
				IsUnique:    true,
				IndNAtts:    1,
				IndNKeyAtts: 1,
				KeyAttNums:  []int16{1},
			},
			{
				Name:        "idx_orders_cust_inc_total",
				IndexRelOID: 1002,
				IsPrimary:   false,
				IsUnique:    false,
				IndNAtts:    2,
				IndNKeyAtts: 1,
				KeyAttNums:  []int16{2, 3}, // cust_id + INCLUDE (total)
			},
		},
	}
	allCols := []ColumnType{ColInt4, ColInt4, ColInt8, ColText}
	visibleIdx := []int{0, 1, 2, 3}
	plans := buildTableIndexPlans(tbl, allCols, visibleIdx)
	if len(plans) != 2 {
		t.Fatalf("expected 2 index plans, got %d", len(plans))
	}

	// Secondary index on cust_id must be selected for WHERE cust_id = ?
	bestIdx, eqLen, hasRange := selectBestBTreeIndex(plans, []vtabFilter{
		{allColIdx: 1, colType: ColInt4, op: sqliteConstraintEQ},
	})
	if bestIdx != 1 || eqLen != 1 || hasRange {
		t.Fatalf("expected secondary index plan 1, got idx=%d eq=%d range=%v", bestIdx, eqLen, hasRange)
	}

	// Covering index check: needed={cust_id, total} -> covered; needed={cust_id, note} -> not covered.
	secPlan := plans[1]
	if !secPlan.IsCoveringFor([]bool{false, true, true, false}, nil, 4) {
		t.Fatalf("expected idx_orders_cust_inc_total to cover (cust_id, total)")
	}
	if secPlan.IsCoveringFor([]bool{false, true, true, true}, nil, 4) {
		t.Fatalf("expected idx_orders_cust_inc_total NOT to cover note column")
	}
}

func TestP5_ReadPageIntoZeroAllocL1Hit(t *testing.T) {
	ctx := context.Background()
	l3 := newMockL3(4)
	c, err := NewTieredCacheBackend(l3, CacheConfig{
		L1MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	key := pgKey(0)
	var buf [PGPageSize]byte
	// Warm L1
	if err := c.GetPageAtLSNInto(ctx, "t1", key, 1000, buf[:]); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		if err := c.GetPageAtLSNInto(ctx, "t1", key, 1000, buf[:]); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > 0 {
		t.Fatalf("expected 0 allocs on L1 GetPageAtLSNInto hit, got %.1f", allocs)
	}
}
