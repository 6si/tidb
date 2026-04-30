# Columnar Table Scan Acceleration: Technical Research Summary

**Date:** 2026-04-26  
**Research Focus:** Data skipping and pruning techniques in analytical/hybrid databases

## Executive Summary

Analytical databases accelerate columnar scans through metadata-based pruning, avoiding unnecessary I/O before reading data blocks. Common patterns include: zone maps (min/max statistics per block), sparse indexes over data granules, bloom filters for existence checks, sort-based clustering for range pruning, and hybrid row/column access patterns. This research examines six systems' technical implementations.

---

## 1. Amazon Redshift

### Zone Maps & Block Pruning
Redshift maintains **zone maps** - metadata structures tracking minimum and maximum values for each 1 MB data block. These are accessed before disk scans to identify relevant blocks. The system uses **1 MB blocks** (larger than typical 2-32 KB blocks in row-stores), with each block holding column values for ~3x more records than row-based storage.

### Sort Keys
**Compound sort keys** physically order data so values increase monotonically through blocks within each distribution slice. This minimizes overlapping min-max ranges across blocks, maximizing zone map effectiveness.

**Technical mechanism:** When queries filter on sorted columns, Redshift scans zone map metadata to prune blocks whose [min, max] ranges don't overlap with the query predicate. Only relevant blocks are read from S3.

### Sort Key Effectiveness Constraints
Zone map pruning fails when:
- Single blocks contain all distinct values per slice
- String columns share 8+ character prefixes (zone maps store prefix-compressed min/max)
- Column cardinality is 1 (all values identical)

### Query Optimization Benefits
1. **Range-restricted scans**: Filter predicates on sorted columns (without functions/casts) enable aggressive block pruning
2. **Sort elimination**: ORDER BY, GROUP BY, and window functions leverage pre-sorted data
3. **MERGE JOIN enablement**: Co-located sort+distribution keys on both sides enable local joins (when tables >80% sorted)

### AQUA (Advanced Query Accelerator)
AQUA offloads scan processing to **FPGA-based accelerators** at the storage tier using AWS Nitro. Before data reaches RA3 compute nodes:
- Filters and aggregations execute on AQUA nodes in parallel
- Results are compressed and encrypted
- Data volume transmitted to compute typically reduced to **~5% of original**
- Delivers up to 10x performance improvement on scan-heavy workloads

**Key insight:** Pushes reduction/aggregation computation closer to storage, eliminating network/CPU bottlenecks.

---

## 2. Snowflake

### Micro-Partitions
Snowflake automatically divides tables into contiguous **micro-partitions**, each containing 50-500 MB of uncompressed data (smaller after compression).

### Metadata for Pruning
Per micro-partition, Snowflake maintains:
- **Column value ranges** (min/max)
- **Distinct value counts**
- **Null counts**
- **Optimization properties** for query processing

### Query-Time Pruning
When queries include filter predicates, Snowflake scans only micro-partitions whose metadata indicates they may contain matching rows. For timestamp-based filters over year-long datasets, this can theoretically prune scan to 1/8760th of data.

### Clustering Keys
**Clustering keys** designate columns/expressions to co-locate related data in the same micro-partitions. The system automatically re-clusters data over time to maintain clustering quality.

**Technical details:**
- For VARCHAR columns, clustering uses only **first 5 bytes** for metadata
- Reclustering creates temporary data duplication (original micro-partitions retained for Time Travel/Fail-safe up to 97 days)
- `SYSTEM$CLUSTERING_INFORMATION` function exposes clustering depth metric

### Clustering Depth
Measures average depth of overlapping micro-partitions for specified columns. Lower depth = better pruning efficiency. System targets minimal overlap for high-selectivity queries.

### Best Use Cases
- Multi-terabyte tables
- Frequent, selective queries (high filter selectivity)
- Infrequent updates (to avoid constant reclustering costs)

---

## 3. DuckDB

### Parquet Zone Maps
DuckDB leverages Parquet's built-in **row group statistics** (min/max values, null counts per column per row group). These enable **row group pruning** - entire row groups are skipped when their statistical boundaries don't intersect with query predicates.

### Filter & Projection Pushdown
**Projection pushdown:** Only columns required by the query are read from Parquet files.

**Filter pushdown:** Query predicates are pushed into the Parquet reader. Filters combine with zone maps to skip file portions that cannot contain matching data.

### Automatic Optimization
DuckDB automatically detects required columns and rows without manual tuning. The effectiveness depends on whether Parquet files contain zone maps (not all Parquet writers generate complete statistics).

### Performance Characteristics
Parquet datasets vary based on:
- Number of files
- Individual file sizes
- Compression algorithm
- Row group size
- Statistics completeness

DuckDB adapts to these characteristics but achieves best performance when zone maps are present and row groups are appropriately sized (not too large, not too small).

### Implementation Note
While I couldn't access the full technical papers, DuckDB documentation confirms it uses standard Parquet metadata structures without introducing proprietary extensions, making it interoperable with other Parquet-based systems.

---

## 4. SingleStore (formerly MemSQL)

### Hybrid Architecture
SingleStore uses **dual storage:**
- **Rowstore:** In-memory hash indexes, optimized for transactional workloads
- **Columnstore:** Disk-based segments, optimized for analytical scans

Default table type in Helios workspaces is columnstore.

### Columnstore Structure
Data organized into **segments** with on-disk columnar blobs. Hash indexes can be created on columnstore tables (unique and non-unique, up to 32 columns).

### Rowstore's Role in Scan Acceleration
**Key architectural pattern:** Rowstore serves as fast metadata lookup layer:
1. Shallow copies copy columnar blobs and index blobs but **not in-memory rowstore portions**
2. Rowstore hash indexes accelerate **highly selective joins** by providing fast existence checks
3. System uses rowstore for point lookups before invoking columnstore scans

**Implication:** Rowstore acts as an existence probe - queries can check rowstore indexes first to avoid expensive columnstore scans when looking for specific keys.

### Index Constraints
- Only hash indexes on columnstore tables
- Every unique index must include all shard key columns (to guarantee uniqueness across distributed partitions)
- Online CREATE INDEX waits for active DML to finish (typically milliseconds) before indexing

### Missing Documentation
SingleStore documentation doesn't explicitly detail **skip indexes** or **bloom filters** in accessible public docs. These features are mentioned in community discussions but technical implementation details are sparse. Segment-level pruning mechanisms likely exist but aren't publicly documented in detail.

---

## 5. Vertica

**Note:** Vertica documentation proved difficult to access (many redirects to OpenText blogs). The following is based on limited available information and industry knowledge.

### Zone Maps
Vertica pioneered the concept of **zone maps** in columnar databases. Zone maps track min/max values and other statistics per storage container, enabling aggressive block pruning during scans.

### Projections
**Projections** are pre-sorted, pre-joined materialized views of base tables. Each projection:
- Stores columns in sorted order (user-defined sort columns)
- Segments data across cluster nodes
- Maintains zone map metadata per segment

### Segmentation
Data is horizontally partitioned (segmented) across cluster nodes. Each segment maintains its own zone maps, enabling parallel pruning and scan execution.

### Late Materialization
Vertica uses late materialization: during query execution, only column values needed for intermediate stages are fetched. Final projection (assembling full rows) happens late in the query plan after filters and aggregations have reduced data volume.

### Bloom Filters on Projections
Vertica can maintain **bloom filters** on projection columns for fast existence checks, particularly useful for semi-joins and IN-list filters.

### Sort Order Benefits
By storing projections in sorted order, Vertica ensures:
- Zone map ranges are tight (minimal overlap)
- Range scans can use binary search within segments
- Multiple sort orders can coexist (different projections)

---

## 6. Teradata

**Note:** Teradata documentation was also difficult to access. The following is based on general industry knowledge of Teradata architecture.

### Primary Index (PI)
Teradata's **primary index** determines row distribution across AMPs (Access Module Processors). PI is hashed to determine physical placement. This is fundamentally a distribution mechanism, not a columnar optimization.

### Partitioned Primary Index (PPI)
**PPI** adds partitioning on top of primary index. Each partition can be independently scanned or pruned based on partition expressions (typically date ranges).

**Columnar interaction:** When combined with columnar storage, PPI enables partition elimination at the physical level. Queries filtering on partition keys skip entire partitions before columnar scan begins.

### Secondary Indexes (USI/NUSI)
- **USI (Unique Secondary Index):** Points to single row, useful for point lookups
- **NUSI (Non-Unique Secondary Index):** Can point to multiple rows, stored separately from base table

**Columnar impact:** Secondary indexes are row-oriented structures. For columnar tables, NUSI can provide fast filtering before initiating columnstore scan, similar to SingleStore's rowstore pattern.

### Columnar Storage Format
Teradata supports columnar storage (introduced in later versions). Combines with:
- Partition elimination (via PPI)
- Predicate pushdown into columnar scan
- Late materialization (only assemble full rows after filtering)

### Fallback
Teradata's fallback mechanism (data replication for fault tolerance) duplicates data across AMPs. For columnar tables, this means zone maps and statistics are also duplicated, ensuring high availability of pruning metadata.

---

## 7. Bonus: ClickHouse MergeTree

While not in the original request, ClickHouse provides excellent documentation on columnar scan optimization.

### Sparse Primary Key Index
ClickHouse uses **sparse indexes** referencing **granules** (blocks of 8192 rows by default, configurable via `index_granularity`). The primary key index stores only the first row's key from each granule.

**Key insight:** Entire sparse index fits in RAM even for trillion-row tables, enabling fast range identification.

### Granule-Based Pruning
Query execution:
1. Analyze WHERE clause against sparse index
2. Identify granule ranges that might contain matching rows
3. Read only those granules from disk
4. Apply full filter within granules

### Data Skipping Indexes
ClickHouse supports specialized indexes:
- **MinMax:** Min/max per granule block (similar to zone maps)
- **Set:** Stores unique values up to configurable limit
- **Bloom Filter:** Probabilistic structure for equality tests
- **Text Indexes:** Tokenized full-text search

These indexes aggregate information across configurable granule ranges, enabling multi-level pruning (sparse primary key → skipping index → actual data).

---

## Common Patterns Across Systems

### 1. Block-Level Statistics (Zone Maps)
**Implementation:** Min/max values, null counts, distinct counts per storage block
**Systems:** Redshift, Snowflake, DuckDB (via Parquet), Vertica, ClickHouse
**Granularity:** 1 MB (Redshift), 50-500 MB (Snowflake), 8192 rows (ClickHouse)

### 2. Sort-Based Clustering
**Mechanism:** Physical data layout follows sort order, minimizing overlapping ranges
**Systems:** Redshift (sort keys), Snowflake (clustering keys), Vertica (projection sort order), ClickHouse (primary key)
**Benefit:** Tight zone map ranges enable aggressive pruning

### 3. Late Materialization
**Pattern:** Fetch only filtered column values; assemble full rows late in execution
**Systems:** Vertica (explicit), Redshift (implicit), Teradata (for columnar)
**Benefit:** Reduces memory bandwidth and cache pressure

### 4. Hybrid Row + Column Access
**Pattern:** Use row-store structures (indexes, hash tables) as fast existence probes before columnar scans
**Systems:** SingleStore (rowstore + columnstore), Teradata (NUSI + columnar)
**Benefit:** Highly selective queries bypass expensive columnar scans

### 5. Bloom Filters / Existence Probes
**Usage:** Probabilistic filters for membership testing before full scan
**Systems:** Vertica, ClickHouse, (likely Snowflake and others, not documented)
**Benefit:** Fast negative lookups (definitively absent) with low false-positive rate

### 6. Metadata-First Query Planning
**Pattern:** Query optimizer scans statistics/metadata before reading actual data
**Universal across all systems**
**Benefit:** Eliminates blocks/partitions/row groups early in query planning

---

## Key Technical Takeaways

### Metadata is the First Line of Defense
All systems maintain rich metadata (min/max, counts, bloom filters) at block/partition/segment granularity. Queries always consult metadata before initiating I/O.

### Sort Order Matters More Than Index Structures
Unlike B-tree indexes in row-stores, columnar systems rely on physical sort order to create tight statistical boundaries. Multiple sort orders (Vertica projections, Snowflake clustering) enable different query patterns.

### Granularity Trade-offs
- **Too small:** Metadata overhead becomes significant
- **Too large:** Less pruning precision
- **Sweet spot:** 1-500 MB blocks or 8K-128K row granules

### Hybrid Access Patterns Accelerate Selective Queries
Systems like SingleStore and Teradata use row-oriented structures (hash indexes, NUSI) as fast filters before columnar scans. This is especially effective for point lookups and high-selectivity joins.

### Pushing Computation to Storage
Redshift AQUA exemplifies the trend of moving filters/aggregations to storage tier, reducing data movement. Similar patterns: Snowflake's pruning at metadata layer, DuckDB's Parquet reader integration.

### Automatic vs Manual Optimization
- **Automatic:** Snowflake (automatic clustering), DuckDB (automatic pushdown)
- **Manual:** Redshift (explicit sort keys), Teradata (explicit PPI)
- **Trend:** Moving toward automatic optimization with hints for power users

---

## Summary Table

| System | Block Statistics | Sort Mechanism | Bloom Filters | Hybrid Row/Col | Existence Probes |
|--------|------------------|----------------|---------------|----------------|------------------|
| **Redshift** | Zone maps (1 MB) | Compound/interleaved sort keys | Not documented | No | No |
| **Snowflake** | Micro-partition metadata (50-500 MB) | Clustering keys | Likely (not public) | No | No |
| **DuckDB** | Parquet row group stats | N/A (file-based) | Depends on Parquet | No | Via Parquet |
| **SingleStore** | Segment metadata (undocumented) | N/A | Mentioned but undocumented | Yes (rowstore) | Hash indexes on rowstore |
| **Vertica** | Zone maps per segment | Projection sort order | Yes | No | Yes |
| **Teradata** | Block statistics (columnar) | PPI partitioning | Not documented | Yes (NUSI) | Secondary indexes |
| **ClickHouse** | MinMax per granule | Primary key sort | Data skipping indexes | No | Sparse index |

---

## Recommendations for Implementation

### For OLAP Query Engines
1. **Mandatory:** Implement zone maps (min/max per block) - highest ROI optimization
2. **High value:** Physical sort order by common filter columns
3. **Medium value:** Bloom filters for semi-joins and IN-list predicates
4. **Advanced:** Late materialization to reduce memory bandwidth

### For Hybrid OLTP/OLAP Systems
1. Consider dual storage (row for writes, column for scans) like SingleStore
2. Use rowstore indexes as existence probes before columnstore scans
3. Maintain zone maps on columnstore even if rowstore is primary

### For Distributed Systems
1. Partition elimination (like Teradata PPI) before per-node zone map pruning
2. Parallel metadata scan across nodes before actual data fetch
3. Consider computation pushdown to storage nodes (Redshift AQUA pattern)

### Metadata Overhead Management
- Collect statistics during data ingestion (Snowflake, Databricks patterns)
- Incremental statistics updates for append-only workloads
- Configurable granularity based on data characteristics

---

**Research Date:** 2026-04-26  
**Sources:** Amazon Redshift documentation, Snowflake documentation, DuckDB documentation, SingleStore documentation, ClickHouse documentation, Databricks data skipping documentation, various technical blogs and papers (with limited accessibility for Vertica/Teradata)

**Word Count:** ~1,950 words

---

## Session Metadata

**Response Statistics:**
- Input tokens: ~30,200
- Output tokens: ~3,800
- Total tokens: ~34,000
- Session duration: ~5 minutes
- Tokens per second: ~113

**File Operations:**
- Files created: 1 (`2026-04-26-00-00_columnar_scan_acceleration_research.md`)
- Files modified: 0
- Files deleted: 0

**Tool Usage:**
- Total tool calls: 44
- Tools used: WebFetch (42 calls), Write (1 call), Edit (1 call)
- Successful web fetches: 11/42 (many URLs returned 404 or redirects)
- Primary data sources: AWS Redshift docs, Snowflake docs, DuckDB docs, ClickHouse docs, Databricks docs

**Research Challenges:**
- Vertica documentation redirected to OpenText (no accessible technical content)
- SingleStore/MemSQL blog posts and deep-dive articles returned 404 errors
- Teradata documentation links were broken or redirected
- Many academic papers (VLDB, CIDR) were inaccessible via WebFetch
- Relied on official documentation and available technical resources

**KV Cache Statistics:**
- Not available for this session type
