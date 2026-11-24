# PostgreSQL Compatibility & Read Engine Benchmarks

## Scope & Honest Status
- **In-Process Workload, Streaming & Multi-Tenant Suites**: `PASS` (verified via Go unit and concurrency suites on Linux with SQLite 3.53.4).
- **Live PostgreSQL 17.12 Differential & Cgroup v2 Host Suite**: `NOT VALIDATED` in this container environment (`postgres`, `initdb`, `pg_ctl`, and `psql` binaries are not installed).

## Observed Suite Timings
| Suite | Scope | Result | Observed Duration |
|---|---|---|---|
| `internal/gateway` (`TestPhase25`) | Router reliability, capability registry, governor cancel, circuit breaker | `PASS` | `1.083s` (`-race`) |
| `internal/readadapter` (`TestPhase26`) | Token-aware PG->SQLite casts, params, NULL ordering, PG funcs | `PASS` | `0.032s` |
| `internal/readadapter` (`TestPhase27`) | Covering index, ordered scan without WHERE, join ordering | `PASS` | Included in `0.223s` (`TestPhase15\|16\|19\|26\|27`) |
| `internal/readadapter` (`TestPhase16B\|TestPhase28`) | Bounded streaming batches, `ReleaseBytes`, immediate cancel | `PASS` | `0.056s` |
| `internal/gateway` (`TestPhase25\|TestPhase28`) | Pre-stream fallback vs mid-stream no-retry relay | `PASS` | `0.062s` |
| `internal/readadapter` (`TestPhase29`) | ORM/analytics workloads + `1/8/32/100/250` tenant concurrency | `PASS` | `0.087s` |
