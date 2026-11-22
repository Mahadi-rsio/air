package readadapter

import (
	"errors"
	"strconv"
	"testing"
)

func TestPhase16D_CostBasedJoinSelection(t *testing.T) {
	// 1. Small tables -> Nested Loop
	dSmall := ChooseJoinAlgorithm(JoinPlanInput{
		LeftRows: 20, RightRows: 30, IsEquiJoin: true, AvailableMemoryBytes: 1 << 20,
	})
	if dSmall.Algorithm != JoinNestedLoop {
		t.Fatalf("expected Nested Loop for tiny join, got %s", dSmall.Algorithm)
	}

	// 2. Indexed right side -> Index Nested Loop
	dIdx := ChooseJoinAlgorithm(JoinPlanInput{
		LeftRows: 50, RightRows: 10000, HasRightIndex: true, IsEquiJoin: true, AvailableMemoryBytes: 1 << 20,
	})
	if dIdx.Algorithm != JoinIndexNestedLoop {
		t.Fatalf("expected Index Nested Loop, got %s", dIdx.Algorithm)
	}

	// 3. Large unindexed equijoin within memory budget -> Hash Join
	dHash := ChooseJoinAlgorithm(JoinPlanInput{
		LeftRows: 5000, RightRows: 5000, BuildRowBytes: 64, IsEquiJoin: true, AvailableMemoryBytes: 4 << 20,
	})
	if dHash.Algorithm != JoinHash {
		t.Fatalf("expected Hash Join within budget, got %s", dHash.Algorithm)
	}

	// 4. Exceeds hash memory budget -> Sort-Merge Join
	dSMJ := ChooseJoinAlgorithm(JoinPlanInput{
		LeftRows: 50000, RightRows: 50000, BuildRowBytes: 128, IsEquiJoin: true, AvailableMemoryBytes: 64 * 1024,
	})
	if dSMJ.Algorithm != JoinSortMerge {
		t.Fatalf("expected Sort-Merge Join when hash exceeds budget, got %s", dSMJ.Algorithm)
	}
}

func TestPhase16D_JoinNullSemanticsAndMemoryBudget(t *testing.T) {
	intVal := func(v int64) SQLiteValue {
		return SQLiteValue{Type: 1, Int64: v, Text: strconv.FormatInt(v, 10)}
	}
	nilVal := SQLiteValue{IsNil: true}
	strVal := func(s string) SQLiteValue {
		return SQLiteValue{Type: 3, Text: s}
	}

	left := []JoinRow{
		{intVal(1), strVal("a")},
		{intVal(2), strVal("b")},
		{nilVal, strVal("null_left")},
		{intVal(1), strVal("a_dup")},
	}
	right := []JoinRow{
		{intVal(1), strVal("r1")},
		{nilVal, strVal("null_right")},
		{intVal(3), strVal("r3")},
	}

	// INNER JOIN across Hash, Sort-Merge, and Nested Loop must all return 2 rows (id=1 x2), NEVER matching NULL==NULL
	hInner, err := ExecuteHashJoin(left, 0, right, 0, JoinInner, nil)
	if err != nil {
		t.Fatal(err)
	}
	sInner := ExecuteSortMergeJoin(left, 0, right, 0, JoinInner)
	nInner := ExecuteNestedLoopJoin(left, 0, right, 0, JoinInner)
	if len(hInner) != 2 || len(sInner) != 2 || len(nInner) != 2 {
		t.Fatalf("INNER JOIN NULL semantics mismatch: hash=%d smj=%d nlj=%d (want 2)", len(hInner), len(sInner), len(nInner))
	}

	// LEFT JOIN must return 4 rows (id=1 x2, id=2 padded, NULL padded)
	hLeft, err := ExecuteHashJoin(left, 0, right, 0, JoinLeft, nil)
	if err != nil {
		t.Fatal(err)
	}
	sLeft := ExecuteSortMergeJoin(left, 0, right, 0, JoinLeft)
	nLeft := ExecuteNestedLoopJoin(left, 0, right, 0, JoinLeft)
	if len(hLeft) != 4 || len(sLeft) != 4 || len(nLeft) != 4 {
		t.Fatalf("LEFT JOIN row count mismatch: hash=%d smj=%d nlj=%d (want 4)", len(hLeft), len(sLeft), len(nLeft))
	}

	// Bounded hash memory budget enforcement
	mgr := NewMemoryManager(MemoryManagerConfig{
		GlobalLimitBytes: 1024, DefaultTenantLimitBytes: 1024, DefaultQueryLimitBytes: 64,
	})
	budget := mgr.BeginQuery("t_join", 0)
	defer budget.Release()
	if _, err := ExecuteHashJoin(left, 0, right, 0, JoinInner, budget); !errors.Is(err, ErrMemoryBudgetExceeded) {
		t.Fatalf("expected ErrMemoryBudgetExceeded for tight hash join budget, got %v", err)
	}
}

func BenchmarkPhase16D_JoinAlgorithms_2000x2000(b *testing.B) {
	left := make([]JoinRow, 2000)
	right := make([]JoinRow, 2000)
	for i := 0; i < 2000; i++ {
		k := int64((i * 7) % 1000)
		s := strconv.FormatInt(k, 10)
		left[i] = JoinRow{{Type: 1, Int64: k, Text: s}, {Type: 3, Text: "l"}}
		right[i] = JoinRow{{Type: 1, Int64: k, Text: s}, {Type: 3, Text: "r"}}
	}

	b.Run("HashJoin", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			res, err := ExecuteHashJoin(left, 0, right, 0, JoinInner, nil)
			if err != nil || len(res) != 4000 {
				b.Fatalf("err=%v len=%d", err, len(res))
			}
		}
	})
	b.Run("SortMergeJoin", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			res := ExecuteSortMergeJoin(left, 0, right, 0, JoinInner)
			if len(res) != 4000 {
				b.Fatalf("len=%d", len(res))
			}
		}
	})
	b.Run("NestedLoopJoin", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			res := ExecuteNestedLoopJoin(left, 0, right, 0, JoinInner)
			if len(res) != 4000 {
				b.Fatalf("len=%d", len(res))
			}
		}
	})
}
