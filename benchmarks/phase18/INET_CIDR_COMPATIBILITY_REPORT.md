# Phase 18B — PostgreSQL `inet` and `cidr` Compatibility Report

**Date**: 2026-11-22  
**Oracle**: PostgreSQL 17.12  
**Target**: AIR Read Engine (`internal/readadapter`)

---

## 1. Implementation Summary

- **Decoder**: `DecodePGInet(raw []byte, isCIDR bool)` in `internal/readadapter/pg_types_ext.go`
  - Decodes PG17 on-disk `inet` structs: `family` (`PGSQL_AF_INET=2`, `PGSQL_AF_INET6=3`), prefix `bits`, and 4-byte IPv4 / 16-byte IPv6 address bytes.
  - Preserves prefix lengths (`/bits` always shown for `cidr`, and shown for `inet` when non-host prefix `bits != 32` / `bits != 128`).
  - Handles IPv4-mapped IPv6 (`::ffff:192.0.2.128`) using `net/netip.AddrFrom16`.
- **SQLite Representation & Ordering**:
  - `TEXT_INET COLLATE PG_INET` (OID `869`) and `TEXT_CIDR COLLATE PG_INET` (OID `650`).
  - `PG_INET` collation (`ComparePGInet` via `air_pg_inet_cmp`) reproduces PostgreSQL's `network_cmp_internal`:
    1. Masked network prefix bits (`min(bitsA, bitsB)`)
    2. Prefix length (`bits`)
    3. Address family (IPv4 before IPv6)
    4. Full host address bits
- **Safe Fallback Guard**:
  - `CheckUnsupportedExtOps` rejects `<<`, `>>`, `<<=`, `>>=`, `&&` and PG network functions (`host()`, `network()`, `masklen()`, `set_masklen()`, `abbrev()`, `broadcast()`, `netmask()`, `hostmask()`, `family()`, `inet_same_family()`, `inet_merge()`) with `ErrUnsupportedType` for PostgreSQL fallback.

---

## 2. Differential Correctness Results (Live PostgreSQL 17.12)

Test suite: `TestPhase18B_InetCIDRDecodingAndCompare` and `TestPhase18_TypesDifferential_LivePG17`.

| Case | Query / Scenario | Status |
| :--- | :--- | :--- |
| IPv4, IPv4/prefix, IPv6, IPv4-mapped IPv6, `NULL` | `SELECT id, ip, net FROM net_items ORDER BY id` | PASS |
| `inet` `ORDER BY` | `SELECT id, ip FROM net_items WHERE ip IS NOT NULL ORDER BY ip ASC, id ASC` | PASS |
| `cidr` `ORDER BY` | `SELECT id, net FROM net_items WHERE net IS NOT NULL ORDER BY net ASC, id ASC` | PASS |
| Equality & range filtering | `WHERE ip = '10.0.0.1'`, `WHERE ip > '10.0.0.1' ORDER BY id` | PASS |
| Containment & function fallback guard | `WHERE ip << '10.0.0.0/8'`, `SELECT host(ip) FROM net_items` | PASS (rejected for PG fallback) |
