# Phase 18A — PostgreSQL `numeric` Compatibility Report

**Date**: 2026-11-22  
**Oracle**: PostgreSQL 17.12 (`initdb` + `postgres` live differential suite)  
**Target**: AIR Read Engine (`internal/readadapter`)

---

## 1. Implementation Summary

PostgreSQL `numeric` values are decoded directly from PG17 on-disk varlena tuples (`NumericShort`, `NumericLong`, and special headers) into exact decimal strings without any `float64` intermediate conversion:

- **Decoder**: `DecodePGNumeric` in `internal/readadapter/pg_types_ext.go`
  - Handles `NumericShort` (`0x8000` flag, 6-bit signed weight, 6-bit `dscale`, sign bit) and `NumericLong` (`n_sign_dscale` + `int16 n_weight` + base-10000 `NumericDigit` slice).
  - Handles zero (preserving `dscale`, e.g. `0.00`), positive, negative, large multi-digit integers (>30 digits, beyond `float64` 53-bit mantissa), small fractional values (negative weights), and special values (`NaN`, `Infinity`, `-Infinity`).
- **SQLite Representation**: `TEXT_NUMERIC COLLATE PG_NUMERIC`
  - Decltype contains `TEXT`, giving SQLite **TEXT affinity** so high-precision decimal strings are never coerced to IEEE-754 `float64`.
  - `declTypeToPG` maps `TEXT_NUMERIC` back to PostgreSQL OID `1700` (`numeric`) in `RowDescription`.
  - Custom CGO collation `PG_NUMERIC` (`ComparePGNumeric` via `air_pg_numeric_cmp`) implements exact PostgreSQL numeric ordering: `-Infinity < negative < 0 < positive < Infinity < NaN`, normalizing trailing fractional zeros (`123.4500 == 123.45`).
- **Safe Fallback Guard**: `CheckUnsupportedExtOps` rejects arithmetic (`+`, `-`, `*`, `/`) and floating-point aggregates (`SUM`, `AVG`, `VARIANCE`, `STDDEV`) on `ColNumeric` columns with `ErrUnsupportedType`, routing them to PostgreSQL.

---

## 2. Differential Correctness Results (Live PostgreSQL 17.12)

Test suite: `TestPhase18A_NumericDecodingAndCompare` and `TestPhase18_TypesDifferential_LivePG17` (`internal/readadapter/phase18_types_differential_test.go`).

| Case | Query / Scenario | Status |
| :--- | :--- | :--- |
| Exact `numeric(10,2)` & unconstrained `numeric` | `SELECT id, amt10, amt_hp FROM num_items ORDER BY id` | PASS |
| High-precision >53-bit (`12345678901234567890123456.123456789012` vs `...013`), `NaN`, `±Infinity`, `0.00`, `-0.000000000001` | `SELECT id, amt_hp FROM num_items ORDER BY amt_hp ASC, id ASC` | PASS |
| Range filter | `SELECT id, amt10 FROM num_items WHERE amt10 > '0' ORDER BY id` | PASS |
| High-precision equality | `SELECT id, amt_hp FROM num_items WHERE amt_hp = '12345678901234567890123456.123456789012'` | PASS |
| `GROUP BY` + `COUNT`, `MIN`, `MAX` | `SELECT grp, count(*), min(amt10), max(amt10) FROM num_items GROUP BY grp ORDER BY grp` | PASS |
| `JOIN` on numeric keys | `SELECT a.id, b.id FROM num_items a JOIN num_items b ON a.amt10 = b.amt10 WHERE a.id < b.id ORDER BY a.id, b.id` | PASS |
| Arithmetic & `SUM`/`AVG` fallback guard | `SELECT sum(amt10) FROM num_items`, `SELECT amt10 + 1 FROM num_items` | PASS (rejected for PG fallback) |
