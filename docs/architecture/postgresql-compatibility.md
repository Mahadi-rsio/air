# PostgreSQL Compatibility Architecture

## Overview
AIR preserves PostgreSQL as the single write authority while executing eligible read-only queries on the local SQLite read engine (`internal/readadapter/`) with token-aware SQL translation, PostgreSQL-compatible scalar/aggregate functions, and safe fallback to PostgreSQL for unsupported syntax.

## Translation Pipeline (`internal/readadapter/sql_rewrite_pg.go`, `sqlite_server.go`)
`translatePGToSQLite` applies ordered, string-literal-safe and identifier-safe passes:
1. `replaceDollarParamsTokenAware`: maps `$1, $2, ...` to `?1, ?2, ...` without corrupting quoted strings or dollar-quoted blocks.
2. `rewritePGCasts`: translates `expr::type` and `CAST(expr AS type)` into SQLite-compatible expressions or `air_pg_cast_bool` / `air_pg_cast_numeric`.
3. `rewritePGSpecialSyntax`: rewrites `EXTRACT(field FROM expr)`, `POSITION(sub IN str)`, and `SUBSTRING(str FROM start [FOR len])`.
4. `rewriteDistinctOn`: rewrites `SELECT DISTINCT ON (...)` into partitioned window-ranked subqueries.
5. `rewriteILikeAndRegex`: rewrites `ILIKE`, `NOT ILIKE`, and POSIX regex operators (`~`, `~*`, `!~`, `!~*`).
6. `rewriteOrderByNulls`: enforces PostgreSQL default NULL ordering (`ASC -> NULLS LAST`, `DESC -> NULLS FIRST`) when explicit `NULLS FIRST`/`NULLS LAST` is omitted.

## PostgreSQL Scalar & Aggregate Functions (`sqlite_pg_funcs.go`, `sqlite_pg_funcs_ext.go`)
Registered on every read connection:
- String & formatting: `left`, `right`, `split_part`, `strpos`, `repeat`, `reverse`, `lpad`, `rpad`, `initcap`, `concat`, `concat_ws`, `char_length`, `character_length`, `md5`, `translate`, `regexp_replace`.
- Date/time & UUID: `now`, `air_pg_date_part`/`date_part`, `date_trunc`, `to_char`, `gen_random_uuid`/`uuid_generate_v4`, `current_schema`.
- Multi-arg & aggregates: `greatest`, `least`, `string_agg`, `bool_and`, `every`, `bool_or`.
- Cast helpers: `air_pg_cast_bool`, `air_pg_cast_numeric`.

## Safety & Fallback Boundary
Unsupported constructs (mutations, locking clauses `FOR UPDATE`/`SHARE`, `LATERAL`, `NOTIFY`/`LISTEN`, session-mutating statements, unsupported functions) are classified as `PARTIAL_SUPPORT` or `UNSUPPORTED` by the gateway capability registry and routed directly to PostgreSQL.
