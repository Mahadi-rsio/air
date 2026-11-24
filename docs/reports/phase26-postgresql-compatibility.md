# Phase 26 Report: PostgreSQL Semantic Compatibility

**Status**: `PASS`

## Summary

Phase 26 replaced regex-based `::type` cast stripping (`reTypeCast`) and `$1` parameter replacement (`reDollarParam`) in `internal/readadapter` with token/AST-aware PostgreSQL-to-SQLite translation, added PostgreSQL default `NULL` sorting semantics, and registered PostgreSQL-compatible scalar, date/time, cast, and aggregate functions in `SQLiteConn`.

## Implemented Changes

### 1. Token/AST-Aware PG-to-SQLite Translation (`internal/readadapter/sql_rewrite_pg.go`, `internal/readadapter/sqlite_server.go`)
- **`replaceDollarParamsTokenAware`**: Translates `$1, $2, ...` to `?1, ?2, ...` while preserving string literals, comments, and quoted identifiers.
- **`rewritePGCasts`**: Replaces `reTypeCast` stripping with semantic-preserving `CAST(expr AS INTEGER|REAL|TEXT|BLOB)`, `air_pg_cast_bool(expr)`, and `air_pg_cast_numeric(expr)` for both `expr::type` (including multi-word types like `DOUBLE PRECISION`, `CHARACTER VARYING`, `TIMESTAMP WITH TIME ZONE`, and precision/scale modifiers) and `CAST(expr AS pg_type)`.
- **`rewritePGSpecialSyntax`**: Translates `EXTRACT(field FROM expr)` -> `air_pg_date_part('field', expr)`, `POSITION(sub IN str)` -> `INSTR(str, sub)`, and `SUBSTRING(s FROM start [FOR len])` -> `SUBSTR(s, start [, len])`.
- **`rewriteOrderByNulls`**: Appends PostgreSQL default `NULL` ordering (`ASC` -> `NULLS LAST`, `DESC` -> `NULLS FIRST`) to `ORDER BY` items (including inside `OVER (ORDER BY ...)`) that do not explicitly specify `NULLS FIRST`/`NULLS LAST`.

### 2. PostgreSQL Functions & Aggregates (`internal/readadapter/sqlite_pg_funcs_ext.go`, `internal/readadapter/sqlite_pg_funcs.go`)
- **Casts**: `air_pg_cast_bool` (strict PG boolean parsing, errors on invalid syntax), `air_pg_cast_numeric` (exact decimal normalization via `PGNumericAdd`).
- **Strings**: `left`, `right` (including negative lengths), `split_part` (1-based, errors on `field <= 0`), `strpos`, `repeat`, `reverse`, `lpad`, `rpad`, `initcap`, `concat`, `concat_ws`, `char_length`, `character_length`, `md5`, `translate`, `regexp_replace` (3- and 4-arg with `'g'`/`'i'` flags).
- **Math/Comparison**: `greatest`, `least` (PG `NULL`-ignoring variadic semantics).
- **Date/Time & Utility**: `now`, `date_part` / `air_pg_date_part`, `date_trunc`, `to_char`, `gen_random_uuid`, `uuid_generate_v4`, `current_schema`.
- **Aggregates**: `string_agg`, `bool_and`, `every`, `bool_or`.

## Verification

| Check | Command | Result |
|---|---|---|
| Phase 26 compatibility tests | `go test -v -run TestPhase26 ./internal/readadapter/...` | `PASS` (`0.032s`) |
| Gateway short suite | `go test -short ./internal/gateway/...` | `PASS` |
