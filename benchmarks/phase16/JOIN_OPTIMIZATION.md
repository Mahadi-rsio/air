# AIR Phase 16D — Advanced Join Optimization (`JOIN_OPTIMIZATION.md`)

## 1. Objective

Provide cost-based join algorithm selection (`Nested Loop`, `Index Nested Loop`, `Hash Join`, `Sort-Merge Join`) with bounded hash table memory, strict SQL `NULL` semantics, and empirical benchmarking against AIR's existing SQLite join engine.

---

## 2. Design ([join_exec.go](file:///workspaces/air/internal/readadapter/join_exec.go))

1. **Cost-Based Selection (`ChooseJoinAlgorithm`)**:
   - **Index Nested Loop Join**: Selected when the right side is B-tree indexed and `O(L log R)` beats hash build.
   - **Nested Loop Join**: Selected for tiny inputs (`L * R <= 2500`) or non-equijoins.
   - **Memory-Bounded Hash Join**: Selected for medium/large unindexed equijoins when the build-side hash table fits inside `AvailableMemoryBytes`.
   - **Sort-Merge Join**: Selected when inputs are already sorted or when the hash table would exceed `AvailableMemoryBytes`.
2. **Strict SQL Semantics**:
   - `NULL` keys never match in `INNER JOIN` and emit `NULL`-padded rows in `LEFT JOIN` across all three operators.
   - Duplicate keys and `QueryMemoryBudget` exhaustion (`ErrMemoryBudgetExceeded`) are handled cleanly.
3. **Architectural Role in AIR**:
   - For SQL queries executed through `air_pg` + SQLite, SQLite's native join planner already leverages `air_pg`'s PostgreSQL-calibrated `xBestIndex` cost/row estimates and B-tree index pushdown (Index Nested Loop + automatic transient indexing).
   - `join_exec.go` provides Go-native memory-bounded Hash Join and Sort-Merge Join operators for unindexed equijoins and cost-based join selection.

---

## 3. Verification

- `TestPhase16D_CostBasedJoinSelection`
- `TestPhase16D_JoinNullSemanticsAndMemoryBudget`
- `BenchmarkPhase16D_JoinAlgorithms_2000x2000`
