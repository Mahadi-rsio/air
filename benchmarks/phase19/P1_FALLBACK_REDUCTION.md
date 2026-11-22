# P1 — Fallback Reduction in the Read Engine

## 1. PG-vs-Go `regexp` (RE2) Semantic Differences Documented
`air_pg_regex(str, pattern, flags)` supports only the provably-equivalent subset of PostgreSQL POSIX ARE/ERE and falls back to PostgreSQL for all other patterns:
1. **`. ` matching `\n`**: PostgreSQL `.` matches `\n` by default; Go RE2 does not unless `(?s)` is enabled. AIR prepends `(?s)` (or `(?is)` for `~*`).
2. **Backreferences (`\1`..`\9`)**: Supported in PG ARE; unsupported in Go RE2 -> **falls back to PG**.
3. **Lookarounds (`(?=...)`, `(?!...)`, `(?<=...)`, `(?<!...)`)**: Supported in PG ARE; unsupported in Go RE2 -> **falls back to PG**.
4. **PG ARE embedded flags (`(?b)`, `(?e)`, `(?q)`, `(?c)`, `(?n)`, `(?p)`, `(?w)`, `(?t)`, `(?x)`)**: Go RE2 only supports `i`, `m`, `s`, `U` -> **falls back to PG**.
5. **PG word-boundary/anchor escapes (`\y`, `\Y`, `\m`, `\M`, `\A`, `\Z`, `[[:<:]]`, `[[:>:]]`)**: Different or unsupported in Go RE2 -> **falls back to PG**.
6. **NULL propagation**: `NULL ~ pattern` or `text ~ NULL` returns `NULL` in both PG and `air_pg_regex`.

## 2. Measured Fallback Reduction
- **Phase 18 22-Query Extended-Type Workload**: reduced from **31.8% (7/22)** to **22.7% (5/22)** (`sum(amt10)` and `amt10 + 1` now execute natively with exact `math/big.Int` decimal arithmetic matching PG 17.12).
- **Broader P1 Workload (`TestPhase19_P1_Differential_LivePG17`)**: 22/22 new queries across window functions, `ILIKE`/`NOT ILIKE`/`ESCAPE`, regex (`~`, `~*`, `!~*`), `DISTINCT ON`, numeric `+`/`-`/`*`/`SUM`/`AVG`, `time`, `interval`, `enum`, and `domain` executed natively with **0 differential mismatches** against live PostgreSQL 17.12; unsafe constructs (`(?=...)`, `\1`, `ORDER BY enum`, `ORDER BY interval`) safely fell back.
