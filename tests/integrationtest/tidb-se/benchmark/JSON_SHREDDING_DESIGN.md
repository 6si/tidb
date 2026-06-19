# JSON Shredding Design — TiFlash

## Overview

JSON shredding automatically decomposes JSON blobs into typed sub-columns at write time,
enabling path-level reads without parsing the full document. This is the same approach
as SingleStore's SeekableJson encoding, adapted to TiFlash's DeltaMerge segment model.

---

## Write Path

Shredding happens **only in TiFlash**, never in TiKV. The data flow:

```
Client INSERT
    → TiDB (SQL layer)
    → TiKV (row-store: stores opaque binary JSON blob in RocksDB)
         ↓ async Raft learner replication
    → TiFlash receives Raft log entries
         ↓
    → DeltaMerge engine ingests rows into Delta layer (in-memory, unsorted)
         ↓ segment flush (Delta → Stable)
    → Schema inference + shredding runs on the JSON column:
         1. JsonSchemaTree::inferSchema(batch_of_rows)
         2. JsonShredder::shredBatch(rows, inferred_schema)
         ↓
    → Stable segment stores BOTH:
         a. Original blob column (ColumnString, unchanged binary JSON)
         b. Typed sub-columns (one per inferred keypath)
```

### Why dual-write?

The blob is always preserved so that:
- The `@@tiflash_json_shredding` flag can toggle read paths without re-ingesting data
- Queries on un-inferred paths (sparse keys, new keys) fall back to blob parsing
- Backward compatibility: old TiFlash versions can still read the blob

---

## Schema Inference (Phase 1)

At segment flush time, `JsonSchemaTree` scans all JSON values in the batch:

```
Input:  [{"event":"click","page":"/home","duration":1500}, ...]
Output: JsonInferredSchema {
          paths: [
            {path: "$.event",    type: String, occurrence: 10000/10000},
            {path: "$.page",     type: String, occurrence: 10000/10000},
            {path: "$.duration", type: Int64,  occurrence: 10000/10000},
            {path: "$.referrer", type: String, occurrence: 2000/10000},  // sparse
            {path: "$.details",  type: Object, occurrence: 1000/10000},  // nested
          ]
        }
```

**Rules:**
- Traverse each JSON document using `JsonBinaryNavigator` (parses MySQL 5.7 binary JSON format)
- Build a keypath tree with type annotations (Int64, Float64, String, Bool, Null, Object, Array)
- **Sparsity pruning:** Keys appearing in <1% of rows are marked as "un-inferable" and stored in the residual blob only
- **Type conflicts:** If the same path has different types across rows (e.g., `$.score` is sometimes int, sometimes string), promote to `Mixed` type → stored as blob
- **Max depth:** Configurable (default: 10 levels of nesting)
- Schema is **per-segment** — different segments can have different schemas

---

## Shredding (Phase 2)

`JsonShredder` takes the inferred schema and produces typed sub-columns:

```
For each row in batch:
    For each inferred path in schema:
        Navigate to path in the binary JSON document
        If found: write typed value to sub-column
        If missing: write NULL marker

Output per segment:
    blob_column:    [original binary JSON per row]  (always written)
    $.event:        [String sub-column: "click", "view", "purchase", ...]
    $.page:         [String sub-column: "/home", "/product", ...]
    $.duration:     [Int64 sub-column: 1500, 2300, 800, ...]
    $.referrer:     [String sub-column: NULL, NULL, "https://...", NULL, ...]
    $.details.source: [String sub-column: NULL, ..., "campaign_5", NULL, ...]
```

Each sub-column uses TiFlash's standard column encodings:
- String sub-columns → eligible for dictionary encoding (low-cardinality detection)
- Int64 sub-columns → bit-packed integer encoding
- Bool sub-columns → bitmap
- NULL tracking → separate presence bitmap per sub-column

---

## Read Path (Phase 3)

Controlled by `@@tiflash_json_shredding` session variable:

### Flag ON (default): Read from sub-columns

```sql
SELECT payload->>'$.event', COUNT(*)
FROM json_events
GROUP BY payload->>'$.event'
```

TiFlash execution:
1. Query references path `$.event`
2. Check if `$.event` is in the segment's inferred schema → YES
3. Read only the `$.event` sub-column (1-byte dict IDs for 5 distinct values)
4. GROUP BY on dictionary-encoded sub-column (array-indexed, no hashing)
5. **I/O: ~1 byte/row** instead of ~200 bytes/row (full blob)

### Flag OFF: Read from blob (baseline)

Same query, but:
1. Read the full `blob_column` (200+ bytes per row)
2. Parse binary JSON per row to extract `$.event`
3. Decode string, hash into GROUP BY hash table
4. **I/O: ~200 bytes/row**

### Fallback: Path not in schema

If the query accesses a path that wasn't inferred (e.g., a new key that appeared after the segment was written):
- Automatically falls back to blob parsing for that path
- Other shredded paths still use sub-columns

---

## Segment Merge (Phase 4)

When DeltaMerge compacts segments, `JsonSegmentMerger` handles schema evolution:

### Same schema (fast path)
```
Segment A: schema = {$.event, $.page, $.duration}
Segment B: schema = {$.event, $.page, $.duration}
→ Concatenate sub-columns directly (no re-shred)
```

### Different schemas (slow path)
```
Segment A: schema = {$.event, $.page}
Segment B: schema = {$.event, $.page, $.duration, $.details.source}
→ mergeSchemas(A, B) = unified {$.event, $.page, $.duration, $.details.source}
→ Re-shred from blobs using unified schema
→ Segment A rows get NULL for $.duration and $.details.source
```

### Delta flush
New delta rows may have keys not in the stable segment's schema:
```
Stable: schema = {$.event, $.page}
Delta:  rows contain {$.event, $.page, $.new_field}
→ Full re-inference on combined (stable + delta) rows
→ New stable segment has schema {$.event, $.page, $.new_field}
```

### Type promotion
```
Segment A: $.score type = Int64
Segment B: $.score type = Float64
→ promoteTypes(Int64, Float64) = Mixed
→ $.score stored as blob in merged segment (conservative)
```

---

## Session Variable: `@@tiflash_json_shredding`

```sql
-- Enable shredded read path (default: ON)
SET @@tiflash_json_shredding = ON;

-- Disable: force blob read path for comparison
SET @@tiflash_json_shredding = OFF;

-- Check current setting
SELECT @@tiflash_json_shredding;
```

**Propagation path:**
```
TiDB SessionVars.TiFlashJsonShredding
    → DistSQLContext.TiFlashJsonShredding
    → gRPC metadata: "tiflash_json_shredding" = "true"/"false"
    → TiFlash reads metadata in DAGRequest handler
    → JsonPathOptimizer checks flag before choosing read path
```

---

## Performance Impact

| Query Pattern | Blob (flag OFF) | Shredded (flag ON) | Speedup |
|---------------|-----------------|--------------------|---------| 
| `SELECT payload->>'$.event'` | Read 200B/row, parse, extract | Read 1B/row (dict ID) | **50-200x I/O reduction** |
| `WHERE payload->>'$.event' = 'purchase'` | Parse blob + string compare | Dict lookup (single int compare) | **20-50x** |
| `GROUP BY payload->>'$.event'` | Parse + hash-based GROUP BY | Array-indexed GROUP BY on dict IDs | **10-30x** |
| `WHERE payload->>'$.details.source' IS NOT NULL` | Parse blob + navigate nested | Read 1-bit presence column | **100x+** |

**Storage overhead:** ~30-50% increase per segment (sub-columns + presence bitmaps alongside blob).
This is the cost of dual-write for instant A/B comparison. Can be reduced later by making the write-side flag-controlled (only shred when enabled).

---

## Code Location

All JSON shredding code lives in TiFlash under `dbms/src/Storages/DeltaMerge/`:

| File | Purpose |
|------|---------|
| `JsonBinaryNavigator.h` | Parses MySQL 5.7 binary JSON format without private API dependency |
| `JsonSchemaTree.h/cpp` | Schema inference engine — builds keypath tree, type detection, sparsity pruning |
| `JsonShredder.h/cpp` | Dual-write producer — generates blob + typed sub-columns per segment |
| `JsonPathOptimizer.h/cpp` | Read-path router — flag ON=sub-column, flag OFF=blob |
| `JsonShreddingConfig.h` | Feature flag configuration singleton |
| `JsonSegmentMerger.h/cpp` | Segment merge — schema union, type promotion, re-shred logic |

TiDB side (session variable plumbing):
| File | Change |
|------|--------|
| `pkg/sessionctx/vardef/tidb_vars.go` | Register `tiflash_json_shredding` constant + default |
| `pkg/sessionctx/variable/session.go` | `TiFlashJsonShredding` field in SessionVars |
| `pkg/sessionctx/variable/sysvar.go` | Sysvar registration with SetSession handler |
| `pkg/distsql/context/context.go` | Add to DistSQLContext for gRPC propagation |
| `pkg/distsql/distsql.go` | Append to outgoing gRPC metadata for TiFlash |

---

## Design Decisions

1. **Always dual-write** — enables instant A/B performance comparison without data re-ingestion.
   Trade-off: 30-50% storage overhead. Future: add write-side flag to skip shredding when not needed.

2. **Schema per-segment** — avoids global schema management and lock contention. Each segment
   independently infers its schema at flush time. Cross-segment queries union schemas and fill NULLs.

3. **Sparsity pruning at 1%** — following SingleStore's heuristic. Keys in <1% of rows are not
   shredded (would produce nearly-all-NULL columns with poor compression). They remain accessible
   via blob fallback.

4. **Conservative type promotion** — conflicting types promote to `Mixed` (blob storage) rather
   than attempting lossy coercion. Correctness over performance.

5. **Session-level toggle** — per-query control via `@@tiflash_json_shredding` rather than
   server-level config reload. Enables side-by-side comparison in the same session.
