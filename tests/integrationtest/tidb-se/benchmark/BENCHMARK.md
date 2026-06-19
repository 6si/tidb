# tidb-se Performance Benchmark Design

This document describes the performance benchmark suite for the tidb-se feature set:
**SHARD BY routing/pruning**, **encoded columnstore operations (TiFlash)**, and **JSON shredding**.

---

## Schema Design

### Fact Tables

#### `fact_sharded` (10M rows default)

```sql
CREATE TABLE fact_sharded (
    id BIGINT NOT NULL,
    tenant_id BIGINT NOT NULL,
    region_id INT NOT NULL,
    industry_id INT NOT NULL,
    status VARCHAR(20) NOT NULL,
    revenue BIGINT NOT NULL,
    created_at DATETIME NOT NULL,
    PRIMARY KEY (id, tenant_id)
) SHARD BY (tenant_id) SHARDS 16;
```

| Column | Cardinality | Distribution | Purpose |
|--------|-------------|--------------|---------|
| `tenant_id` | 10,000 | Uniform mod; CRC32/IEEE hashes to 16 partitions | Shard key — point queries prune to 1 partition |
| `region_id` | 50 | Uniform mod | FK to `dim_region`; star join target |
| `industry_id` | 20 | Uniform mod | FK to `dim_industry`; star join target |
| `status` | 5 | Round-robin (`active`, `inactive`, `pending`, `archived`, `deleted`) | Low-cardinality; ideal for dictionary encoding in TiFlash |
| `revenue` | ~100K distinct | `(id * 7) % 100000` | Numeric aggregation target |
| `created_at` | 1 (constant) | All rows `2024-01-01` | Timestamp filter target |

**Sharding:** `SHARD BY (tenant_id) SHARDS 16` creates 16 internal HASH partitions.
Each `tenant_id` maps to exactly one shard via `CRC32(tenant_id) % 16`.
With 10,000 tenants distributed across 16 shards, each shard holds ~625 tenants (~625K rows).

**TiFlash replica:** 1 replica (columnar copy for analytical queries).

#### `fact_unsharded` (10M rows, identical data)

```sql
CREATE TABLE fact_unsharded (
    id BIGINT NOT NULL,
    tenant_id BIGINT NOT NULL,
    region_id INT NOT NULL,
    industry_id INT NOT NULL,
    status VARCHAR(20) NOT NULL,
    revenue BIGINT NOT NULL,
    created_at DATETIME NOT NULL,
    PRIMARY KEY (id, tenant_id)
);
```

Same schema, same data, **no SHARD BY**. Single partition.
Used as the baseline to measure the performance gain from shard pruning.

---

### Dimension Tables

#### `dim_region` (50 rows)

```sql
CREATE TABLE dim_region (
    id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    country VARCHAR(30) NOT NULL
);
```

| id | name pattern | country |
|----|-------------|---------|
| 1-10 | `us-east-1-0`, `us-west-2-0`, ... | US, EU, APAC, LATAM, AFRICA, ME |
| 11-50 | `us-east-1-1`, ... (rotated) | Same rotation |

Small dimension for broadcast join in TiFlash MPP. 6 distinct countries for grouped results.

#### `dim_industry` (20 rows)

```sql
CREATE TABLE dim_industry (
    id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    sector VARCHAR(30) NOT NULL
);
```

Industries: Technology, Finance, Healthcare, Retail, Manufacturing, Energy, Telecom, Media, Education, Transportation, Agriculture, Construction, Pharma, Insurance, RealEstate, Consulting, Legal, Hospitality, Mining, Aerospace.

5 sectors: Tech, Finance, Health, Consumer, Industrial, Energy, Services, Media, Primary.

---

### JSON Events Table

#### `json_events` (1M rows = 10% of fact rows)

```sql
CREATE TABLE json_events (
    id BIGINT NOT NULL,
    tenant_id BIGINT NOT NULL,
    payload JSON NOT NULL,
    PRIMARY KEY (id, tenant_id)
) SHARD BY (tenant_id) SHARDS 16;
```

**Payload structure** (varies per row to exercise schema inference + sparsity):

```json
// Every row (common paths):
{
  "event": "click|view|purchase|signup|logout",  // 5 values
  "page": "/home|/product|/cart|/checkout|/account|/search|/category|/blog",  // 8 values
  "duration": 0-30000,
  "score": 0.0-100.0,
  "user_agent": "Mozilla/5.0",
  "ip": "10.x.x.x"
}

// 20% of rows add:
{
  "referrer": "https://google.com/search?q=item..."
}

// 10% of rows add nested:
{
  "details": {
    "source": "campaign_0..49",
    "medium": "cpc",
    "cost": 0-999
  }
}
```

This structure exercises:
- **Common paths** (100% density) → shredded into typed sub-columns
- **Sparse paths** (20% density) → may be pruned by sparsity threshold
- **Nested objects** (10% density) → tests nested path inference and access

---

## Benchmark Scenarios

### 1. Shard Key Point Query

**What it proves:** SHARD BY routes queries to fewer partitions when the shard key is in the WHERE clause.

| Variant | Query | Partitions scanned | Expected behavior |
|---------|-------|-------------------|-------------------|
| Baseline | `SELECT COUNT(*), SUM(revenue) FROM fact_sharded WHERE region_id = 5` | All 16 | No shard key → full partition fan-out |
| Optimized | `SELECT COUNT(*), SUM(revenue) FROM fact_sharded WHERE tenant_id = 42` | 1 | Shard key → CRC32(42) maps to 1 partition |
| Sharded vs Unsharded | Same `tenant_id = 42` filter on both tables | 1 vs full table | Shows pruning benefit of SHARD BY DDL |

**Expected speedup:** ~7-16x (depends on partition overhead vs I/O savings at given scale).

---

### 2. IN Clause Shard Pruning

**What it proves:** `WHERE shard_key IN (v1, v2, ...)` prunes to only the partitions containing those specific hash values.

| Variant | Query | Partitions scanned |
|---------|-------|-------------------|
| Baseline | `WHERE region_id IN (1,2,3,4,5)` | All 16 (non-shard column) |
| Optimized | `WHERE tenant_id IN (42,100,555,1234,7777)` | ~4 (CRC32 maps 5 values to ~4 distinct shards) |

**Expected speedup:** ~3-4x (16/4 = 4x theoretical; overhead reduces actual).

---

### 3. Analytical Filter — TiKV vs TiFlash

**What it proves:** TiFlash's columnar storage + dictionary encoding provides faster analytical scans at scale.

| Variant | Query | Engine | Why faster at scale |
|---------|-------|--------|---------------------|
| TiKV | `GROUP BY status` with aggregates | Row-store coprocessor | Data fits in block cache at <10M rows |
| TiFlash | Same query | Columnar + dict encoding | Reads only `status` + `revenue` columns (not full row); dict-encoded status is 3 bits vs 20 bytes |
| TiKV (wide) | 3-column GROUP BY + 4 agg functions | Row-store | Must read all columns from every row |
| TiFlash (wide) | Same wide query | Columnar | Reads only referenced columns; compression ratio 10-50x on repeated values |

**Expected at 2M rows:** TiKV faster (all in-cache, ~300ms MPP overhead dominates).
**Expected at 50M+ rows (live cluster):** TiFlash 5-15x faster (I/O-bound, columnar compression wins).

---

### 4. Encoded GROUP BY — TiKV vs TiFlash

**What it proves:** Dictionary-encoded columns allow array-indexed GROUP BY instead of hash-based grouping.

| Variant | Query | Groups | Mechanism |
|---------|-------|--------|-----------|
| TiKV | `GROUP BY status` | 5 | Hash each decoded string → hash table lookup |
| TiFlash | Same | 5 | Dict ID is array index → direct slot access, no hashing |
| TiKV | `GROUP BY region_id` | 50 | Hash 50 distinct integers |
| TiFlash | Same | 50 | Bit-packed column → vectorized group slot assignment |

**Expected at scale:** 5-15x TiFlash advantage (vectorized processing on compressed data).

---

### 5. Star Join — TiKV vs TiFlash MPP

**What it proves:** TiFlash's MPP engine executes star-schema joins (fact × small dimensions) with broadcast join + columnar scan, avoiding the row-level hash build + probe of TiKV.

| Variant | Query | Join type | TiKV plan | TiFlash plan |
|---------|-------|-----------|-----------|--------------|
| Single dim | `fact × dim_industry` + filter + GROUP BY | INNER | HashJoin (build dim, probe fact rows) | MPP: broadcast dim → local columnar join + pushdown agg |
| Multi dim | `fact × dim_industry × dim_region` + GROUP BY | 2× INNER | Two nested HashJoins | MPP: broadcast both dims → fused scan-join-agg |
| LEFT | `fact × dim_region` LEFT JOIN + GROUP BY | LEFT | HashJoin with NULL handling | MPP: broadcast + local LEFT join |

**Expected at scale:** 10-30x TiFlash advantage (columnar scan reads only join keys + agg columns; broadcast avoids shuffle; MPP parallelism across segments).

---

### 6. JSON Operations — TiKV vs TiFlash

**What it proves:** TiFlash shreds JSON into typed sub-columns at write time, enabling path-level access without parsing the full blob.

| Variant | Query | TiKV cost | TiFlash cost |
|---------|-------|-----------|--------------|
| Path GROUP BY | `GROUP BY payload->>'$.event'` | Parse ~200B blob per row, extract `event` | Read 1-byte dict-encoded `$.event` sub-column |
| Path filter | `WHERE payload->>'$.event' = 'purchase'` | Parse blob + string compare per row | Dict lookup on sub-column (single comparison) |
| Nested path | `WHERE payload->>'$.details.source' IS NOT NULL` | Parse blob + navigate to nested field | Read presence bit from sub-column (1 bit/row) |
| Combined (shard + JSON) | `WHERE tenant_id = 42 AND payload->>'$.event' = 'click' GROUP BY payload->>'$.page'` | Shard prune → still parse blob | Shard prune → read only `$.event` + `$.page` sub-columns |

**Expected at scale:** 10-100x for path-level access (read 1-10 bytes vs 200-1000 bytes per row).

---

## Data Loading

- Fact tables loaded with 8 concurrent workers, 5000-row batches via `INSERT ... VALUES`
- ~190K rows/sec insert rate on single-node (limited by TiKV write path)
- JSON events use random generators with reproducible seeds per worker
- `ANALYZE TABLE` run after load for accurate optimizer statistics
- TiFlash replica sync verified before benchmarks start (wait up to 4 min)

## Interpreting Results

### Why TiKV is faster at small scale (<10M rows)

At 2-10M rows on a single node, **all data fits in TiKV's RocksDB block cache** (typically 2-4GB). This means:
- TiKV reads are effectively memory reads (zero I/O)
- TiKV coprocessor pushdown (COUNT/SUM computed at storage layer) adds ~1ms overhead
- TiFlash has ~300ms fixed overhead per query (MPP task scheduling, segment scan initialization)

### When TiFlash wins

TiFlash's advantage manifests when:
1. **Data exceeds cache** (50M+ rows, multi-GB working set) — columnar compression reduces I/O 10-50x
2. **Wide scans** (many columns) — columnar reads only referenced columns
3. **Low-cardinality aggregation** — dictionary encoding enables O(1) group slot assignment
4. **Star joins** — broadcast join on small dims + columnar fact scan avoids shuffle and row materialization

### Running on the live cluster

```bash
# 50M rows — expected to show TiFlash advantages
go run . --host=tidb-se.playground.6si.com --port=4000 --rows=50000000 --workers=16

# 100M rows — stronger signal
go run . --host=tidb-se.playground.6si.com --port=4000 --rows=100000000 --workers=32
```

At 50M rows across 3 TiKV nodes + 1 TiFlash node, expect:
- Shard pruning: 10-16x
- TiFlash filter/groupby: 5-15x
- TiFlash star join: 10-30x
- TiFlash JSON path access: 10-100x
