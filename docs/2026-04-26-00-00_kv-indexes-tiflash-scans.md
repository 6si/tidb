# Can KV Indexes Help TiFlash Scans?

**Date:** 2026-04-26
**Input tokens:** ~8,000 | **Output tokens:** ~2,500 | **Time:** ~90s | **Tokens/sec:** ~28
**Files modified:** 0 | **Files created:** 1 | **Files deleted:** 0
**Tool calls:** 1 (Agent/Explore)
**KV cache reads/writes:** N/A

---

## Short Answer

**No — TiKV (KV) secondary indexes cannot directly accelerate TiFlash scans.** The two storage engines are architecturally separate: TiFlash has its own columnar replica and its own optimization mechanisms. There is no hybrid code path where a TiKV index scan filters rows before handing them to a TiFlash scan.

---

## Why Not?

### 1. Hard Architectural Separation

The planner explicitly blocks index scans when TiFlash is preferred (`find_best_task.go:1825`):

```go
// TiFlash storage do not support index scan.
if ds.PreferStoreType&h.PreferTiFlash != 0 {
    continue
}
```

### 2. Mutually Exclusive Task Types

- **CopTask** → TiKV coprocessor
- **MppTask** → TiFlash MPP engine

`PhysicalIndexLookUpReader` only populates its `tablePlan` with TiKV table scans — there is no branch for TiFlash.

### 3. Separate Data

TiFlash maintains its own columnar replica synced from Raft. It doesn't read TiKV's B-tree index pages.

---

## What DOES Work: Scenarios That Actually Help TiFlash

### 1. TiFlash Vector Indexes (TiFlash-Local) ✅
- TiFlash has its **own** index concept: `IsTiFlashLocalIndex()` returns true when `IndexInfo.VectorInfo != nil`
- Used for approximate nearest-neighbor (ANN) queries
- Pushed down via `PhysicalTableScan.AnnIndexExtra` → `tipb.ANNQueryInfo`
- Relevant model: `pkg/meta/model/index.go:96`

**Scenario:** `SELECT * FROM t ORDER BY vec_l2_distance(embedding, ?) LIMIT 10` — uses TiFlash's local HNSW index, not TiKV.

### 2. Late Materialization ✅
- `pkg/planner/core/tiflash_selection_late_materialization.go`
- Pushes highly selective filters into `PhysicalTableScan.LateMaterializationFilterCondition`
- TiFlash evaluates cheap conditions first and reads fewer columns for passing rows
- Threshold: selectivity ≤ 0.7, column count ≤ 3
- **This is the closest analog to "index filtering" for TiFlash** — selective pushdown without an actual index.

**Scenario:** `WHERE status = 'active'` on a table where 95% of rows are `'inactive'` — late materialization applies and TiFlash skips reading other columns for the filtered rows.

### 3. Partition Pruning for TiFlash ✅
- Partition-key predicates are evaluated in TiDB before creating the MppTask
- Only the pruned partition `physicalTableID`s are included in the scan
- Works the same as TiKV partition pruning — just computed at a higher layer

**Scenario:** `WHERE region = 'US'` on a `PARTITION BY LIST (region)` table — TiFlash scans only the US partition.

### 4. Shard Key "Pruning" ✅ (New on this branch)
- `pkg/table/tables/shard.go`: `locateShard()` maps `shard_col = value` → specific shard ID(s)
- When filtering on shard key columns, only matching shards' physical table IDs are scanned
- Reduces physical tables scanned similarly to partition pruning

**Scenario:** `WHERE user_id = 42` on a table sharded by `user_id` — TiDB computes `hash(42) % N` and only scans that shard on TiFlash.

### 5. Shard Key Co-location for MPP Joins ✅ (Main contribution of this branch)
- `pkg/planner/core/task.go:2557`: `bothCoLocated()` checks `ShardKeysCompatible()`
- When two tables have compatible shard keys and you join on those columns, MPP exchange operators (network shuffle) are **skipped**
- This doesn't reduce scan work, but eliminates redistribution cost in distributed joins

**Scenario:** `SELECT * FROM orders o JOIN order_items i ON o.order_id = i.order_id` where both are sharded on `order_id` — no shuffle needed, each TiFlash node joins its local slices.

---

## Summary Table

| Mechanism | Reduces Scan Rows? | Reduces Data Shuffled? | Uses KV Index? |
|---|---|---|---|
| TiKV Secondary Index | ✅ (TiKV only) | — | ✅ |
| TiFlash Vector Index | ✅ (ANN only) | — | ❌ (TiFlash-local) |
| Late Materialization | Partial (column-level) | — | ❌ |
| Partition Pruning | ✅ (whole partitions) | — | ❌ |
| Shard Key Pruning | ✅ (whole shards) | — | ❌ |
| Shard Key Co-location | ❌ | ✅ | ❌ |

---

## Could We Build KV Index → TiFlash Integration?

Theoretically possible but complex:
1. Use TiKV index scan to get a **set of row handles**
2. Pass that handle set to TiFlash as a filter (like a semi-join or IN-list pushdown)
3. TiFlash applies the handle set to skip rows during columnar scan

This would require a new hybrid task type and protocol changes between TiDB and TiFlash. It would help most when the index is highly selective (few matching rows) but the query needs many columns that only TiFlash has efficiently.

**This is not currently implemented anywhere in the codebase.**

---

## Key Source Files

| File | Purpose |
|---|---|
| `pkg/planner/core/find_best_task.go:1825` | Hard block on index scans for TiFlash |
| `pkg/planner/core/find_best_task.go:2768` | TiFlash scan planning entry point |
| `pkg/planner/core/tiflash_selection_late_materialization.go` | Late materialization logic |
| `pkg/planner/core/task.go:2557` | Shard key co-location / exchange skipping |
| `pkg/table/tables/shard.go` | Shard key implementation and `locateShard()` |
| `pkg/meta/model/index.go:96` | Vector index model (`IsTiFlashLocalIndex()`) |
| `pkg/planner/core/partition_prune.go` | Partition pruning (applies to TiFlash too) |
