# Phase 18I — Production Safety & Failure Testing Report

**Date**: 2026-11-22  
**Test Suites**: `TestPhase18I_CacheSafetyAndCorruptionRecovery`, `TestPhase18C_ArrayDecoding`, `TestPhase18_UnsupportedOpsFallback`, `TestPhase17B_DifferentialCorrectness_LivePG17`

---

## 1. Failure Scenarios Tested & Verified

| Failure / Stress Scenario | Verification Method | Result |
| :--- | :--- | :--- |
| **L2 bit-flip corruption** | Flip byte in `.pg` file on disk (`TestPhase18I_CacheSafetyAndCorruptionRecovery`) | PASS — CRC32C mismatch detected (`L2Corrupt > 0`), corrupt file deleted, clean page recovered from L3 |
| **Incomplete write / leftover `.tmp.` & truncated `.pg` on restart** | Seed `.pg.tmp.*` and short `.pg` before `NewTieredCacheBackend` | PASS — Startup reconciliation deletes both before serving |
| **Concurrent L2 eviction & reads** | 8 goroutines x 40 reads with `L2MaxEntries=4` | PASS — Zero panics/races, `L2Entries <= 4` invariant preserved |
| **O_DIRECT unsupported FS fallback** | `L2DirectIO: true` on `t.TempDir()` and ext4 | PASS — Cleanly uses `O_DIRECT` on ext4 (`0` fallbacks) or falls back cleanly if `EINVAL` |
| **Hostile array dimensions** | `dim = 100,000,000` in `DecodePGArray` | PASS — Rejected immediately without allocation (`pgMaxArrayElements` guard) |
| **Malformed numeric / inet / array payloads** | Truncated headers & out-of-range prefix lengths (`bits=99`) | PASS — Clean `ErrInvalidPage` error, zero panics |
| **Unsupported operator / syntax fallback** | `SUM(numeric)`, `numeric + 1`, `inet << cidr`, `host(inet)`, `arr[1]`, `arr @> ...`, `ORDER BY arr` | PASS — Returns `ErrUnsupportedType` so gateway routes safely to PostgreSQL |
| **Pageserver + SQLiteReadServer restart recovery** | `TestPhase17B_DifferentialCorrectness_LivePG17` | PASS |
