# tidb-se Performance Benchmark

Measures the performance impact of SHARD BY, encoded columnstore operations, and JSON shredding.

## Quick Start

### Local (TiUP playground)

```bash
# Start cluster with custom binaries
tiup playground v8.5.0 --tag bench \
  --db.binpath /path/to/tidb-server \
  --pd.binpath /path/to/pd-server \
  --kv.binpath /path/to/tikv-server \
  --tiflash.binpath /path/to/tiflash \
  --db 1 --pd 1 --kv 1 --tiflash 1

# Run with 1M rows (quick validation)
cd tests/integrationtest/tidb-se/benchmark
go run . --rows=1000000

# Run with 10M rows (more accurate)
go run . --rows=10000000
```

### Live Cluster

```bash
# Full benchmark on remote cluster (50M rows, 16 shards)
go run . \
  --host=tidb-se.playground.6si.com \
  --port=4000 \
  --rows=50000000 \
  --shards=16 \
  --workers=16

# Skip data loading (reuse existing data from a previous run)
go run . --host=tidb-se.playground.6si.com --port=4000 --skip-load --rows=50000000

# Run specific benchmarks only
go run . --host=tidb-se.playground.6si.com --port=4000 --skip-load --bench=shard,join
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `127.0.0.1` | TiDB host |
| `--port` | `4000` | TiDB port |
| `--user` | `root` | TiDB user |
| `--password` | `` | TiDB password |
| `--db` | `bench_tidb_se` | Database name |
| `--rows` | `10000000` | Number of fact table rows |
| `--dim-rows` | `1000` | Number of dimension table rows |
| `--shards` | `16` | SHARD BY shard count |
| `--workers` | `8` | Concurrent data loading workers |
| `--skip-load` | `false` | Skip data loading (reuse existing) |
| `--bench` | `all` | Benchmarks to run: `shard,in,filter,groupby,join,json` |
| `--iter` | `5` | Iterations per query (averaged) |

## Benchmarks

### 1. Shard Key Point Query (`shard`)
Compares query latency when filtering on the shard key (prunes to 1 partition) vs filtering on a non-shard column (scans all partitions).

**Expected speedup:** ~Nx where N = number of shards (e.g., 16x for 16 shards)

### 2. IN Clause Pruning (`in`)
Tests `WHERE shard_key IN (v1, v2, ...)` — should prune to only the shards containing those values.

**Expected speedup:** ~N/K where N = total shards, K = shards containing IN values

### 3. Encoded Filter (`filter`)
Compares TiFlash filter performance with `tidb_enable_encoded_filter` ON vs OFF on a low-cardinality column (dictionary-encoded).

**Expected speedup:** 3-10x (dictionary lookup vs row-by-row string comparison)

### 4. Encoded GROUP BY (`groupby`)
Compares GROUP BY on dictionary-encoded columns with `tidb_enable_encoded_groupby` ON vs OFF.

**Expected speedup:** 5-15x (array lookup by dict ID vs hash-based grouping)

### 5. Encoded Star Join (`join`)
Tests the fused star join pipeline (scan → join → aggregate in one pass) with `tidb_enable_encoded_star_join` ON vs OFF.

**Expected speedup:** 10-30x for INNER JOIN, 5-15x for LEFT JOIN

### 6. JSON Shredding (`json`)
Compares reading a specific JSON path from shredded sub-columns vs parsing the full blob, using `tiflash_enable_json_shredding` toggle.

**Expected speedup:** 10-100x (read 10 bytes vs 1KB per row)

## Schema

- `fact_sharded` — Main fact table with SHARD BY (tenant_id), 10K tenants, 50 regions, 20 industries
- `fact_unsharded` — Same schema without SHARD BY (baseline)
- `dim_region` — 50 regions with country grouping
- `dim_industry` — 20 industries with sector grouping
- `json_events` — JSON payloads with common paths + sparse/nested fields

## Interpreting Results

The benchmark reports average query latency over N iterations for each feature's ON vs OFF state. Results are printed as a table:

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                    tidb-se PERFORMANCE BENCHMARK SUMMARY                    ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ Host: 127.0.0.1:4000 | Rows: 10M | Shards: 16                             ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ Benchmark                                │ Baseline │ Optimized │ Speedup ║
╠──────────────────────────────────────────┼──────────┼──────────┼─────────╣
║ Shard Point Query                        │   245ms  │    16ms  │  15.31x ║
║ Encoded Star Join: fact×industry         │  1.23s   │   102ms  │  12.06x ║
║ JSON Path GROUP BY - shredded vs blob    │   890ms  │    45ms  │  19.78x ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Notes:**
- Local single-node results will show lower absolute speedups due to limited parallelism
- On the live cluster (3 TiKV, 3 PD, 2 TiDB, 1 TiFlash), expect higher throughput and more pronounced speedups
- First run after data load may be slower due to TiFlash compaction; use `--skip-load` for subsequent runs
- Encoded operations require TiFlash; they won't show speedup on TiKV-only queries
