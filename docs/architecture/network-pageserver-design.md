# P8 Design Document — Network Pageserver (TCP/gRPC + mTLS)

**Status:** Design Only (Not Implemented)

---

## 1. Motivation & Goals

Currently, AIR's Pageserver serves `GetPage@LSN` (`OpGetPageAtLSN`, `OpNblocks`, `OpGetSLRUPage`, etc.) over a node-local Unix domain socket (`49`-byte big-endian binary frames) shared by `air-server`, tenant PG 17.12 Cells (`air_smgr.so`), and `air-read-engine`.

To decouple compute from storage across nodes:
1. **Preserve the Unix socket fast path** for co-located compute/pageserver deployments (zero TLS/TCP overhead).
2. **Add a TCP/gRPC + mTLS transport** for remote compute nodes (`air-read-engine` and `air_smgr` cells).
3. **Separate the WAL durability layer** from Pageserver page materialization so compute commits are durable before Pageserver compaction/restart.
4. **Maintain all security invariants**: mTLS mutual authentication with SPIFFE/SAN tenant-scoped or node-scoped identities, AES-256-GCM at-rest encryption, and monotonic epoch CAS fencing.

---

## 2. Dual-Transport Architecture

```text
  Co-located Compute (same host)              Remote Compute (separate node)
  ┌────────────────────────────┐              ┌────────────────────────────┐
  │ PG17 Cell / air-read-engine│              │ PG17 Cell / air-read-engine│
  └─────────────┬──────────────┘              └─────────────┬──────────────┘
                │ Unix Socket (49B binary)                  │ TCP/gRPC + mTLS (TLS 1.3)
                ▼                                           ▼
  ┌──────────────────────────────────────────────────────────────────────┐
  │                       AIR Network Pageserver                         │
  │  Transport Multiplexer -> Auth/Epoch Gate -> Page Read/Write Path    │
  │  (InMemoryLayer -> L0 -> L1 -> Image -> R2 CAU)                      │
  └──────────────────────────────────────────────────────────────────────┘
```

### 2.1 Wire & gRPC Contract
- **Service `PageserverService`** (new `proto/pageserver.proto`):
  - `GetPageAtLSN(GetPageRequest) returns (GetPageResponse)` — single 8 KiB page at explicit LSN + `tenant_id` + `epoch`.
  - `GetPagesAtLSN(stream GetPageRequest) returns (stream GetPageResponse)` — bidirectional streaming for batch/prefetched scans (amortizes RTT).
  - `GetNblocks(NblocksRequest) returns (NblocksResponse)`
  - `GetSLRUPage(SLRURequest) returns (GetPageResponse)`
  - `PreloadCheck(PreloadRequest) returns (PreloadResponse)`
- **Transport selection**:
  - `PageserverSocket != ""` and local: Unix socket `49`-byte binary fast path (`PageserverSocketBackend`).
  - `PageserverAddr != ""`: gRPC over TLS 1.3 with **mTLS** (`ClientAuth: RequireAndVerifyClientCert`), HTTP/2 keepalive, and connection pooling.

### 2.2 `air_smgr.so` Support
- `air_smgr.c` keeps the Unix socket IPC for local Pageserver and adds a lightweight local sidecar proxy (`air-smgr-proxy`) or libpq/gRPC client over `localhost` Unix socket -> remote mTLS gRPC, avoiding C-level OpenSSL/gRPC bloat inside every PG backend process.

---

## 3. WAL Durability Layer (Compute -> WAL Acceptor -> Pageserver)

Separating Pageserver from compute requires WAL durability independent of any single compute disk:
1. **WAL Acceptor / Safekeeper Quorum** (1 local or 3-node Raft/Paxos-style quorum):
   - Primary PG17 Cell streams synchronous WAL (`synchronous_commit = on`) to the WAL Acceptor(s).
   - Acceptor persists WAL segments (`flush_lsn`) + `term`/`epoch` metadata before acknowledging `CommitLSN` to PG17.
2. **Pageserver Pull**:
   - Pageserver subscribes to WAL Acceptor (or R2 WAL archive on cold catch-up) and materializes `InMemoryLayer -> L0 -> L1 -> Image`.
   - If Pageserver restarts or fails over to a standby Pageserver, it replays from its last R2 CAU `layer_manifest.json` `ConsistentLSN` using the durable WAL Acceptor stream.

---

## 4. Failure Modes & Mitigations

| Failure Mode | Detection | Mitigation |
| :--- | :--- | :--- |
| **Network partition between Compute and Pageserver** | gRPC deadline / keepalive timeout (`5s`) | Read Engine fails over or returns `ErrReadEngineUnavailable` so Gateway falls back to Primary PG; Cell retries with bounded backoff |
| **Split-brain / Stale Primary writing WAL** | Monotonic `Epoch` CAS (`fencing.PutManifestCAS`) | Every WAL ingest & `GetPage@LSN` carries `epoch`; Pageserver rejects any request with `epoch < active_epoch` (`ErrStaleGeneration`) |
| **Rogue/compromised compute node** | mTLS client cert SAN verification | Pageserver verifies client cert CN/SAN against authorized compute-node/tenant allowlist; rejects unauthorized `tenant_id` |
| **Pageserver crash mid-layer-flush** | R2 CAU `If-None-Match: *` + ETag CAS manifest | Uncommitted layers are ignored; new Pageserver loads last committed `layer_manifest.json` and replays WAL from `ConsistentLSN` |
| **Tail latency amplification on remote `GetPage@LSN`** | L1/L2 cache miss rate + p99 gRPC latency | Client-side L1 RAM + L2 NVMe cache (`TieredCacheBackend`) + `GetPagesAtLSN` streaming batch prefetch |

---

## 5. Phased Migration Plan

1. **Phase A (Proto & Dual-Listener)**: Define `proto/pageserver.proto`, implement `PageserverService` gRPC + mTLS server in `internal/pageserver/`, keeping Unix socket enabled by default.
2. **Phase B (Read Engine Remote Client)**: Implement `PageserverGRPCBackend` satisfying `StorageBackend` + `SnapshotProvider` in `internal/readadapter/`, selected when `--pageserver-addr` is set.
3. **Phase C (Cell Sidecar Proxy)**: Add `air-smgr-proxy` forwarding local Unix socket 49-byte frames to remote mTLS gRPC `PageserverService`.
4. **Phase D (WAL Acceptor Decoupling)**: Introduce durable WAL Acceptor service between PG17 `walsender` and Pageserver `WALReceiver`, validated with SIGKILL + network partition chaos tests.
