# Fix: bothCoLocated per-side columns and double-exchange guard

**Date:** 2026-04-25-22-20
**Commit SHA:** f94d748da6

## Summary

Two correctness fixes applied to `pkg/planner/core/task.go` for the MPP planner shard-key co-location path introduced in commit 477c9dc1.

### C1 (Critical): `bothCoLocated` used left columns to validate right table

**Root cause:** The function accepted a single `partCols` slice (derived from `lTask.hashCols`) and used it to check both the left and right table's shard key. For joins like `t1.company_id = t2.cid` this incorrectly checked `"company_id"` against the right table's shard key `["cid"]`.

**Fix:** Changed signature to `bothCoLocated(lTask, rTask *MppTask, lPartCols, rPartCols []*property.MPPPartitionColumn)` and updated the left/right checks to use their respective column slices. Updated call site to pass `rTask.hashCols` as the fourth argument.

### I1 (Important): Double exchange when `convertPartitionKeysIfNeed` already added one

**Root cause:** After `convertPartitionKeysIfNeed` runs, the task's top-level plan may already be a `PhysicalExchangeReceiver`. Calling `enforceExchangerImpl` unconditionally would wrap it in a second exchange.

**Fix:** Added `alreadyExchanged` type-assertion guards around both `lHasShardKey` and `rHasShardKey` blocks before calling `enforceExchangerImpl`.

## Test Results

- `TestBothCoLocated`: PASS
- All `pkg/planner/core/casetest/mpp/...` tests: PASS (18 tests, 8.846s)

## Metrics

| Metric | Value |
|--------|-------|
| Files modified | 1 (`pkg/planner/core/task.go`) |
| Files created | 1 (this doc) |
| Files deleted | 0 |
| Tool calls | 9 (Read x2, Edit x3, Read x2, Bash x3) |
| Input tokens | N/A |
| Output tokens | N/A |
| Time taken | ~3 min |
| KV cache reads/writes | N/A |
