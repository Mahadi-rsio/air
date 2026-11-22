# AIR Phase 17B — Differential Correctness Testing Report (`DIFFERENTIAL_TEST_REPORT.md`)

## 1. Test Methodology

`TestPhase17B_DifferentialCorrectness_LivePG17` ([phase17_differential_test.go](file:///home/antigravity/Mahadi-rsio/air/internal/readadapter/phase17_differential_test.go)) and `TestPhase16I_DifferentialCorrectness_LivePG17` ([phase16i_differential_test.go](file:///home/antigravity/Mahadi-rsio/air/internal/readadapter/phase16i_differential_test.go)) execute live PostgreSQL `17.12` alongside AIR's Pageserver, `WALReceiver`, and `SQLiteReadServer` (`air_pg`), comparing every query result row-by-row, column-by-column, and `NULL`-for-`NULL` against PostgreSQL `17.12`.

---

## 2. Differential Coverage Matrix

| Category | Scenarios Verified | Result |
|---|---|---|
| **Fast-Default (`attmissingval`)** | `ALTER TABLE ADD COLUMN ... DEFAULT` (`text`, `int4`, `int8`, `bool`, `float8`, `NULL`), old tuples vs new tuples, `GROUP BY` / `IS NULL` | **PASS** |
| **DROP COLUMN + ADD COLUMN** | `ALTER TABLE DROP COLUMN` (`atttypid=0`, `attisdropped=true`) + `ADD COLUMN ... DEFAULT` across old, middle, and new tuples | **PASS** |
| **HOT Chains & `HEAP2_PRUNE_ON_ACCESS`** | 4× HOT `UPDATE` on `id=42` followed by `CREATE INDEX` triggering `HEAP2_PRUNE_ON_ACCESS`; verified via PK scan and new secondary index | **PASS** |
| **Cross-Page `UPDATE` WAL Replay** | `UPDATE` moving tuple from block 0 to block 1 (`id=5` -> `(1,N)`) verified not to corrupt `offnum=5` (`id=159`) on block 1 | **PASS** |
| **Transactions (Commit, Rollback, Concurrent)** | `BEGIN ... INSERT/UPDATE/DELETE ... ROLLBACK` (invisible), `BEGIN ... COMMIT` (visible), overlapping `tx1` + `tx2` | **PASS** |
| **Randomized SQL (`seed=17`, 50 queries)** | `SELECT`, `WHERE` (`=`, `!=`, `<`, `<=`, `>`, `>=`), `LEFT JOIN`, `GROUP BY`, `ORDER BY`, `LIMIT`, `OFFSET`, aggregations | **PASS** |
| **Unsupported Type Fallback** | `numeric(10,2)` table returns explicit `ErrUnsupportedType` for gateway PG fallback | **PASS** |
| **Restart & WAL Replay Recovery** | Stop & restart `Pageserver` + `WALReceiver` + `SQLiteReadServer`, re-verify `HOT+prune`, `attmissingval`, `GROUP BY`, and `tx` queries | **PASS** |

---

## 3. Critical Bugs Discovered & Fixed During Phase 17B

1. **Cross-Page `XLOG_HEAP_UPDATE` Corruption in `heap_redo.go`**:
   - **Discovery**: `TestPhase17B` (`rand17_2` / `cat4_all_ids`) caught that `id=159` (`ctid=(1,5)`, `xmax=0` in PG) vanished in AIR (`Xmax=744`) after `UPDATE items ... WHERE id = 5` (which moved `id=5` from block `0` offnum `5` to block `1`).
   - **Root Cause**: `heap_redo.go` checked `else if oldBlk == nil` to detect same-page updates. When replaying the *new* page (BlockID `0`) of a cross-page update, `findWALBlock(rec, 1, key)` returned `nil` because BlockID `1` belonged to block `0` (`!= key.BlockNum`), causing `heap_redo.go` to mistakenly mark `oldOffnum` (`5`) as updated/deleted on block `1`!
   - **Fix**: Check `!hasOldBlkRef` (whether BlockID `1` exists anywhere in `rec.Blocks`) rather than `oldBlk == nil`. Verified with `TestPhase17B_DifferentialCorrectness_LivePG17` (**PASS** in 57.28s).
2. **Dropped Column Physical Alignment (`attisdropped=true`, `atttypid=0`)**:
   - **Discovery**: `TestPhase17B` (`post_drop_add_old_row`) caught `ErrUnsupportedType` on `ratio` after `ALTER TABLE items DROP COLUMN ratio` because PG sets `atttypid = 0` on dropped columns.
   - **Fix**: Infer physical `ColumnType` from `pg_attribute.attlen` when `isDropped && colType == ColUnsupported` in `pg_catalog.go`, allowing tuple deformation to skip dropped attributes accurately.
