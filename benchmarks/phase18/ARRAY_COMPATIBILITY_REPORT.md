# Phase 18C — PostgreSQL Array Compatibility Report

**Date**: 2026-11-22  
**Oracle**: PostgreSQL 17.12  
**Target**: AIR Read Engine (`internal/readadapter`)

---

## 1. Implementation Summary

Native array decoding (`DecodePGArray` in `internal/readadapter/pg_types_ext.go`) supports PostgreSQL 17 on-disk `ArrayType` payloads for:
- `int2[]` (OID 1005)
- `int4[]` (OID 1007)
- `int8[]` (OID 1016)
- `text[]` (OID 1009)
- `bool[]` (OID 1000)
- `float4[]` (OID 1021)
- `float8[]` (OID 1022)

Key engineering details:
- Validates `ndim` (`0..6`), element count overflow (`pgMaxArrayElements = 1_000_000`), and `elemtype` OID match (returning `ErrUnsupportedType` on mismatch).
- Correctly adjusts `dataoffset` relative to the stripped 4-byte `vl_len_` varlena header (`adjOff = dataOffset - 4` when `hasNulls`, `hdrEnd` when `!hasNulls`) and 8-byte alignment for `int8[]`/`float8[]`.
- Preserves multidimensional shapes (`{{1,2},{3,4}}`), custom lower bounds (`[2:3]={7,8}`), `NULL` elements, empty strings (`""`), quoted text elements (`"a,b"`, `"NULL"`, `"x\"y"`), and `float4`/`float8` formatting (`%.6g` / `%.15g`).
- Maps SQLite decltypes `TEXT_INT2ARRAY`, `TEXT_INT4ARRAY`, `TEXT_INT8ARRAY`, `TEXT_TEXTARRAY`, `TEXT_BOOLARRAY`, `TEXT_FLOAT4ARRAY`, `TEXT_FLOAT8ARRAY` back to their PostgreSQL array OIDs in `RowDescription`.
- Rejects array indexing (`[...]`), concatenation (`||`), containment/overlap (`@>`, `<@`, `&&`), `ANY`/`ALL`, array functions (`unnest`, `array_length`, etc.), and array ordering (`ORDER BY arr`, `<`, `>`, `BETWEEN`, `MIN`, `MAX`) with `ErrUnsupportedType` for PostgreSQL fallback.

---

## 2. Differential Correctness Results (Live PostgreSQL 17.12)

Test suite: `TestPhase18C_ArrayDecoding` and `TestPhase18_TypesDifferential_LivePG17`.

| Case | Query / Scenario | Status |
| :--- | :--- | :--- |
| All 7 array types, empty `{}`, 2D, custom lbounds, `NULL` elements, `""`, escaping | `SELECT id, i2, i4, i8, txt, b, f4, f8 FROM arr_items ORDER BY id` | PASS |
| Array equality | `SELECT id FROM arr_items WHERE i4 = '{10,20,30}'`, `WHERE txt = '{}'` | PASS |
| `IS NULL` | `SELECT id FROM arr_items WHERE i4 IS NULL` | PASS |
| Hostile dimensions & truncated payloads | `TestPhase18C_ArrayDecoding`, `TestPhase18I_CacheSafetyAndCorruptionRecovery` | PASS (error without OOM/panic) |
| Indexing, `@>`, `ORDER BY arr` fallback | `SELECT i4[1]`, `WHERE i4 @> '{10}'`, `ORDER BY i4` | PASS (rejected for PG fallback) |
