package readadapter

import (
	"fmt"
	"testing"
)

func BenchmarkPhase15_Optimizer_ChooseBestPlan(b *testing.B) {
	sc := NewStatsCache(1024)
	at := NewAdaptiveTracker(1024)
	sc.Put(&TableStats{
		TenantID:   "t1",
		Generation: "g1",
		RelOID:     9001,
		RelPages:   1000,
		RelTuples:  50000,
		Columns: map[int16]ColumnStats{
			1: {AttNum: 1, Distinct: -1.0},
			2: {AttNum: 2, Distinct: 250},
		},
	})
	opt := NewQueryOptimizer(DefaultCostModelConfig(), sc, at, nil)
	tbl := DiscoveredTable{
		Name:   "bench_tbl",
		RelOID: 9001,
		Columns: []DiscoveredColumn{
			{Name: "id", AttNum: 1, PGTypeOID: PGOIDInt4},
			{Name: "cat", AttNum: 2, PGTypeOID: PGOIDInt4},
			{Name: "v1", AttNum: 3, PGTypeOID: PGOIDText},
			{Name: "v2", AttNum: 4, PGTypeOID: PGOIDText},
		},
	}
	plans := []btreeIndexPlan{
		{Index: DiscoveredIndex{Name: "pk", IndexRelOID: 9101, IsUnique: true, IndNAtts: 1, IndNKeyAtts: 1, KeyAttNums: []int16{1}}, KeyAllCols: []int{0}, KeyVisCols: []int{0}},
		{Index: DiscoveredIndex{Name: "idx_cat", IndexRelOID: 9102, IsUnique: false, IndNAtts: 1, IndNKeyAtts: 1, KeyAttNums: []int16{2}}, KeyAllCols: []int{1}, KeyVisCols: []int{1}},
	}
	allCols := []ColumnType{ColInt4, ColInt4, ColText, ColText}
	cons := []FilterConstraint{{AllColIdx: 0, AttNum: 1, Op: sqliteConstraintEQ}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = opt.ChooseBestPlan(tbl, plans, allCols, cons, nil, 2, 1000, "t1", "g1")
	}
}

func BenchmarkPhase15_PlanCache_Hit(b *testing.B) {
	pc := NewQueryPlanCache(4096)
	sql := "SELECT id, label FROM items WHERE id = $1"
	_, _ = pc.GetOrTranslate("t1", "g1", 100, 1, 1, sql, translatePGToSQLite)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = pc.GetOrTranslate("t1", "g1", 100, 1, 1, sql, translatePGToSQLite)
	}
}

func BenchmarkPhase15_PlanCache_Miss(b *testing.B) {
	pc := NewQueryPlanCache(4096)
	queries := make([]string, 256)
	for i := range queries {
		queries[i] = fmt.Sprintf("SELECT id, label FROM items WHERE id = $1 AND cat = %d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = pc.GetOrTranslate("t1", "g1", uint64(i/256+1), 1, 1, queries[i%256], translatePGToSQLite)
	}
}

func BenchmarkPhase15_ConcurrentMultiTenant_PlanCache(b *testing.B) {
	pc := NewQueryPlanCache(4096)
	tenants := []string{"t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8"}
	sql := "SELECT id, val FROM bench_items WHERE id = $1"
	for _, tid := range tenants {
		_, _ = pc.GetOrTranslate(tid, "g1", 100, 1, 1, sql, translatePGToSQLite)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			tid := tenants[idx&7]
			idx++
			_, _ = pc.GetOrTranslate(tid, "g1", 100, 1, 1, sql, translatePGToSQLite)
		}
	})
}
