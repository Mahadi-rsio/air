# Phase 28: Bounded Memory & Streaming Results Report

## Summary
Phase 28 upgraded both the SQLite read adapter (`internal/readadapter/`) and the PostgreSQL gateway (`internal/gateway/`) to support bounded-batch streaming, reusable buffer pools, per-batch memory budget accounting, immediate cancellation, and strict no-retry-after-partial-rows semantics.

## Implemented Changes
1. **Bounded-Batch SQLite Streaming & Memory Budget Release (`internal/readadapter/sqlite_cgo.go`, `internal/readadapter/memory_budget.go`)**:
   - Added `QueryMemoryBudget.ReleaseBytes(n)` so streamed row batches return reserved bytes back to query, tenant, and global budgets immediately after being flushed.
   - Refactored `queryWithContextInternal` into `queryStreamInternal` and exposed `SQLiteConn.QueryStreamWithContext(...)`, stepping `sqlite3_step` in bounded batches, checking `ctx.Err()` on every batch for immediate cancellation, and enforcing `MaxResultRows` and `QueryMemoryBudget`.

2. **Incremental PostgreSQL Wire Encoding (`internal/readadapter/sqlite_server.go`)**:
   - Added `writeResultSetStream` reusing pooled `bytes.Buffer` instances (`pgRowBufPool`) and flushing `RowDescription` + `DataRow` chunks to `sess.conn` whenever accumulated bytes reach `flushThreshold` (64 KiB in `handleExecute` for >64 rows).

3. **Gateway Read-Backend Streaming & No-Retry Guard (`internal/gateway/pooler.go`)**:
   - Extracted `relayReadBackendBatch(readConn, clientConn, fallbackCap)` with a 1 MiB (`readFallbackBufferCap`) pre-stream fallback buffer:
     - Errors (`'E'`) arriving before `streamStarted` write 0 bytes to the client and safely fall back to PostgreSQL (`return 0`).
     - Responses exceeding `fallbackCap` flush incrementally in 64 KiB chunks (`streamStarted = true`).
     - Mid-stream errors after `streamStarted == true` forward `'E'` and `'Z'` directly to the client and **never** retry on PostgreSQL.

4. **Verification (`internal/readadapter/phase28_streaming_test.go`, `internal/gateway/phase28_streaming_relay_test.go`)**:
   - `go test -v -run "TestPhase16B|TestPhase28" ./internal/readadapter/...` -> `PASS` (`0.056s`).
   - `go test -v -run "TestPhase25|TestPhase28" ./internal/gateway/...` -> `PASS` (`0.062s`).
   - `go test -short ./internal/gateway/...` -> `PASS` (`5.739s`).
