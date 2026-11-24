# Intelligent Query Router Architecture

## Overview
The AIR Gateway router (`internal/gateway/capability_registry.go`, `sql_ast.go`, `sql_router.go`) decides per-query whether a statement can execute safely on the SQLite read engine or must be routed to PostgreSQL.

## Routing Pipeline
1. **Token-Aware AST/Lexical Inspection (`sql_ast.go`)**:
   - Strips comments and inspects top-level statements without naive substring false positives inside string literals.
   - Detects writes, DDL, locking clauses (`FOR UPDATE`, `FOR SHARE`, `FOR NO KEY UPDATE`, `FOR KEY SHARE`), CTE mutations (`WITH ... INSERT/UPDATE/DELETE`), `LATERAL` joins, `RETURNING`, `LISTEN`/`NOTIFY`, and session-pinning commands.
2. **Capability Registry (`capability_registry.go`)**:
   - Classifies PostgreSQL features into `FULL_SUPPORT`, `PARTIAL_SUPPORT`, and `UNSUPPORTED`.
   - Only `FULL_SUPPORT` queries are eligible for read-engine execution; `PARTIAL_SUPPORT` and `UNSUPPORTED` safely fall back to PostgreSQL.
3. **LSN & Session Consistency (`sql_router.go`, `pooler.go`)**:
   - Enforces read-your-writes consistency (`ReadEngineLSN >= SessionWriteLSN`), session pinning (`st.sessionPinned`), transaction state (`txStatus == 'I'`), and read-engine circuit breaker health.
4. **Streaming & No-Retry-After-Partial-Rows (`pooler.go`)**:
   - `relayReadBackendBatch` buffers up to `readFallbackBufferCap` (1 MiB) before flushing to the client.
   - Pre-stream errors allow transparent PostgreSQL fallback; once streaming begins (`streamStarted == true`), errors are forwarded directly to the client and PostgreSQL is never retried.
