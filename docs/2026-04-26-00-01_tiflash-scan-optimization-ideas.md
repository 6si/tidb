# TiFlash Scan Optimization Ideas (Competitor-Informed)

**Date:** 2026-04-26
**Input tokens:** ~40,000 | **Output tokens:** ~1,800 | **Time:** ~120s
**Files modified:** 0 | **Files created:** 1 | **Files deleted:** 0
**Tool calls:** 2 (Agent, Write)
**KV cache reads/writes:** N/A

---

## Ideas Ranked by Feasibility vs. Impact

---

### 1. TiKV Index as Existence Probe (User's Idea)
**Inspiration:** SingleStore rowstore hash index, Teradata NUSI

```
WHERE colA = 'blah'
  → TiKV index point lookup (1 round-trip)
    ├── miss → return {} immediately
    └── hit  → run TiFlash scan
```

**Best for:** High-cardinality columns, sparse lookups, join probe-side checks where many values don't exist.
**Cost:** One extra sequential TiKV round-trip on every hit (can't overlap with TiFlash scan start).
**Planner signal to inject:** high NDV on column + large TiFlash table + equality predicate + TiKV index exists.

---

### 2. TiKV Index → Handle Set → TiFlash Row Filter
**Inspiration:** Teradata NUSI, SingleStore rowstore pre-filter — the more aggressive version of idea #1

Instead of just checking existence, pull the matching handles from TiKV and push them into TiFlash as an explicit row-ID filter:

```
WHERE colA = 'blah' AND colB > 100
  → TiKV index scan on colA = 'blah' → [handle1, handle2, handle7]
  → TiFlash scan with handle IN (1, 2, 7) pushed down
    → reads only those 3 rows' columnar data
```

**Best for:** Very selective predicates (few rows match) combined with expensive wide-column projections on TiFlash. Classic OLAP pattern: filter on a low-cardinality key, project on many wide columns.
**Cost:** Handle set must be transmitted TiKV → TiDB → TiFlash. Breaks down if result set is large (thousands of handles).
**Threshold heuristic:** Only worthwhile if estimated matching rows < N% of table, where N is tuned from scan cost model.

---

### 3. Block-Level Zone Maps in TiFlash (Min/Max per Segment)
**Inspiration:** Redshift zone maps, Snowflake micro-partition metadata, ClickHouse MinMax skipping index

TiFlash's storage engine (DeltaMerge, derived from ClickHouse) likely already maintains per-pack min/max statistics internally. The gap is **planner-level awareness** — TiDB doesn't consult those stats before building the MppTask.

**Idea:** Expose TiFlash pack-level min/max to TiDB at query time, so the planner can:
- Skip entire TiFlash packs for range predicates (`WHERE ts BETWEEN x AND y`)
- Compute tighter row estimates for cost-based decisions

**Effectiveness depends on column sort order within TiFlash** — if TiFlash stores data in insertion order, min/max ranges will overlap heavily and pruning will be poor. Works best if data arrives time-ordered (very common for event/log tables).

**What to check:** `tiflash/dbms/src/Storages/DeltaMerge/` in the TiFlash repo — likely already has pack stats; the question is whether they're surfaced.

---

### 4. TiFlash Sort-Order Clustering (Analog to Redshift Sort Keys)
**Inspiration:** Redshift compound sort keys, Vertica projection sort order

Redshift's most impactful optimization is physical sort order — it makes zone map ranges non-overlapping. Without sort order, zone maps are useless (every block's range spans the entire domain).

**Idea:** Allow users to declare a **cluster key** on a TiFlash table (separate from the TiKV primary key). TiFlash would:
- Physically sort data within each replica segment by that key
- Maintain tight min/max boundaries per pack
- Enable aggressive pack skipping for range predicates on the cluster key

**Integration point:** DDL-level, similar to `PARTITION BY` or sort hint. Could be expressed as `TIFLASH CLUSTER BY (ts, user_id)`.
**Cost:** Background re-sort on ingestion; DeltaMerge's merge process could fold this in naturally.
**Shard key connection:** The shard key already determines which TiFlash node holds a row. Cluster key would determine order *within* a shard — complementary, not redundant.

---

### 5. Bloom Filters per TiFlash Segment
**Inspiration:** Vertica bloom filters on projections, ClickHouse bloom_filter data skipping index

A per-segment bloom filter on a column answers "does value X possibly exist in this segment?" with zero false negatives and tunable false positive rate (~1%).

```
WHERE email = 'user@example.com'
  → check bloom filter for each TiFlash segment
    ├── filter says NO → skip segment (100% certain skip)
    └── filter says MAYBE → scan segment
```

**Best for:** High-cardinality string columns (emails, UUIDs, identifiers) where equality probes are common. The TiKV existence probe (idea #1) and bloom filter solve the same problem — bloom filter is purely local to TiFlash (no cross-engine round-trip), but requires upfront space and build time.
**Bloom filter size:** ~10 bits per element at 1% FPR → 1.25 MB per 1M rows. Fits in memory easily.
**ClickHouse precedent:** Already implemented in ClickHouse as `INDEX idx_email email TYPE bloom_filter GRANULARITY 1` — TiFlash could adopt the same approach.

---

### 6. Vectorized Predicate Pre-filter (Late Materialization Extension)
**Inspiration:** Vertica late materialization, Redshift implicit column projection

TiFlash already has late materialization (pushes selective filters to `LateMaterializationFilterCondition`). The current threshold is selectivity ≤ 0.7.

**Extension ideas:**
- **Multi-column filter ordering:** When multiple predicates apply, order them by `(1 - selectivity) / evaluation_cost` — cheapest filter that eliminates the most rows goes first. Currently TiDB does a rough version of this but without evaluation cost in the model.
- **Rough set early termination:** If a filter column is already sorted within a TiFlash pack, use binary search to find the matching range instead of evaluating every row.
- **Vectorized SIMD filter:** Ensure filter evaluation on TiFlash uses SIMD integer/string comparison — this is a TiFlash-side optimization but the planner controls what gets pushed down.

---

### 7. Partition-Level Bloom Filters / Metadata at TiDB Layer
**Inspiration:** Snowflake micro-partition pruning metadata

Snowflake maintains a metadata store *outside* the data files — a separate catalog layer with per-micro-partition statistics. This means pruning decisions happen before any storage access.

**TiDB analog:** Maintain per-shard or per-partition bloom filters / min-max metadata in TiDB's statistics layer (or in the `information_schema`). At query time, the planner checks this metadata before dispatching *any* MppTask to TiFlash.

**Current gap:** TiDB's statistics are global (per-table histograms) or per-partition at coarse granularity. There's no block-level metadata consulted during TiFlash MppTask construction.
**Benefit:** Pruning happens entirely in TiDB, before any network round-trip to TiFlash. Zero latency for fully prunable queries.
**Cost:** Metadata must be maintained on write — adds overhead to DML but dramatically improves scan selectivity.

---

## Summary: Ideas vs. What Competitors Do

| Idea | Comparable To | Effort | Impact |
|---|---|---|---|
| TiKV index existence probe | SingleStore rowstore, Teradata USI | Medium (planner + executor) | High for sparse lookups |
| TiKV index → handle set filter | Teradata NUSI pre-filter | High (new task type) | Very high for selective queries |
| Zone maps from TiFlash packs | Redshift, Snowflake, ClickHouse | Low (may already exist internally) | High if data is time-ordered |
| TiFlash cluster key / sort order | Redshift sort keys, Vertica projections | High (DDL + storage changes) | Very high (enables zone maps) |
| Bloom filters per segment | Vertica, ClickHouse data skipping | Medium (TiFlash storage change) | High for high-cardinality equality |
| Late materialization improvements | Vertica, extended | Low (planner only) | Medium |
| Shard-level metadata at TiDB layer | Snowflake micro-partition catalog | Medium | High for partition-heavy queries |
