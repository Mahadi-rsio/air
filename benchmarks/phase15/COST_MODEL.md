# AIR Phase 15 — Storage-Aware Cost Model (`COST_MODEL.md`)

## 1. Configurable Cost Parameters

`CostModelConfig` in `internal/readadapter/optimizer.go` parameterizes storage, CPU, and sort costs so hardware profiles can be tuned without changing optimizer code:

| Parameter | Default | Meaning |
| :--- | :--- | :--- |
| `L1PageCost` | `0.10` | RAM page access cost (L1 cache hit). |
| `L2PageCost` | `1.00` | Local NVMe page access cost (L2 cache hit). |
| `L3PageCost` | `10.00` | Pageserver network/layer fetch cost (L3 miss). |
| `SeqPageFactor` | `0.50` | Sequential I/O discount factor for heap scans. |
| `RandomPageFactor` | `1.50` | Random I/O penalty for leaf and heap TID fetches. |
| `TupleEvalCost` | `0.01` | Base CPU cost per evaluated tuple. |
| `IndexTupleCost` | `0.005` | CPU cost per B-tree index entry inspected. |
| `FilterEvalCost` | `0.002` | CPU cost per pushed-down predicate evaluation. |
| `ColDecodeCost` | `0.001` | CPU cost per projected column decoded. |
| `SortTupleCost` | `0.02` | Per-tuple sort factor (`N * log2(N) * SortTupleCost`). |

## 2. Cache-Tier Probability Weighting

`QueryOptimizer.cacheTierProbabilities()` computes expected page cost from live `TieredCacheBackend.Stats()` (falling back to conservative `0.50 L1 / 0.15 L2 / 0.35 L3` priors when cache stats are unavailable):

```text
pageCost = p_L1 * L1PageCost + p_L2 * L2PageCost + p_L3 * L3PageCost
```

## 3. Sequential Scan vs. B-Tree Index Scan Cost

### Sequential Heap Scan
- `seqIOCost = seqPages * pageCost * SeqPageFactor`
- `seqCPUCost = baseRows * (TupleEvalCost + numConstraints * FilterEvalCost + neededCols * ColDecodeCost)`
- `seqSortCost = seqOutRows * log2(seqOutRows) * SortTupleCost` (if `ORDER BY` present and `seqOutRows > 1`)

When `RelPages` and `RelTuples` are both unknown (`0`), the optimizer uses a conservative 10-page prior (matching PostgreSQL's un-analyzed table prior) so selective index lookups are preferred unless the table is known to be tiny (e.g., `RelPages == 1`).

### B-Tree Index Scan
- Matches leading index key prefix (`EQ`, `GT`, `GE`, `LT`, `LE`).
- Checks `ORDER BY` compatibility after skipping leading equality-bound key columns (`eqPrefix`), eliminating `sortCost` when satisfied (`StrategyIndexScanOrdered`).
- Models B-tree metapage and upper internal pages as hot shared L1 pages (`hotInternalPages * L1PageCost`), while leaf and heap TID fetches use `coldDataPages * pageCost * RandomPageFactor`.
- Applies `AdaptiveTracker` bounded EMA `SelectivityFactor` (clamped to `[0.1, 10.0]`) to refine row estimates from observed execution history.
