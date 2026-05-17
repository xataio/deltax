# Architecture Guide

This document is a walkthrough of the pg_deltax codebase.

## What is pg_deltax?

pg_deltax is a PostgreSQL extension written in Rust (via pgrx 0.17) that adds time-series capabilities on top of native PostgreSQL declarative partitioning. It automatically manages partitions, compresses old data with columnar codecs, and transparently decompresses during queries using custom scan nodes. It supports PostgreSQL 17-18.

## High-Level Architecture

```
                        ┌─────────────────────────┐
                        │      User SQL API        │
                        │  deltax_create_table()   │
                        │  deltax_compress_*()     │
                        │  time_bucket(), first()  │
                        └────────────┬────────────┘
                                     │
          ┌──────────────────────────┼──────────────────────────┐
          │                          │                          │
          v                          v                          v
   ┌──────────────┐      ┌───────────────────┐      ┌──────────────────┐
   │  Partition    │      │   Compression     │      │  Custom Scan     │
   │  Manager     │      │   Engine          │      │  Nodes           │
   │              │      │                   │      │                  │
   │ partition.rs │      │ compress.rs       │      │ scan/hook.rs     │
   │ catalog.rs   │      │ compression/*.rs  │      │ scan/path.rs     │
   │              │      │                   │      │ scan/exec/       │
   └──────┬───────┘      └────────┬──────────┘      │ scan/cost.rs     │
          │                       │                  │ scan/explain.rs  │
          │                       │                  └──────────────────┘
          └───────────┬───────────┘
                      v
             ┌────────────────┐
             │ Background     │
             │ Worker         │
             │ worker.rs      │
             │ (every 60s)    │
             └────────────────┘
```

The extension has four major subsystems:

1. **Partition Manager** - Creates and manages time-range partitions
2. **Compression Engine** - Columnar compression with multiple codecs
3. **Custom Scan Nodes** - Transparent decompression during query execution
4. **Background Worker** - Periodic maintenance (drain, compress, drop)

---

## Entry Point: `src/lib.rs`

This is where the extension boots. Key things that happen:

- The `pg_deltax.mock_now` GUC is defined. This lets tests override "now" for deterministic time-based behavior.
- Two catalog tables are created via `extension_sql!`:
  - `deltax_deltatable` - One row per managed table. Stores the table name, time column, partition interval, compression settings (segment_by, order_by, segment_size), and retention policy (compress_after, drop_after).
  - `deltax_partition` - One row per partition. Stores range boundaries, compression state, sizes, and row counts.
- `_PG_init()` registers the GUC, background worker, planner hook, and executor hook.

---

## Partition Manager

### `src/partition.rs` - Partition Lifecycle

This is the largest source file and contains all the partition creation and management logic.

**Core helpers** (top of file):
- `interval_to_usec()` - Converts a PG Interval to microseconds. Explicitly rejects month-based intervals since months have variable duration.
- `partition_name()` - Generates names like `mytable_20250115` or `mytable_20250115_1430` depending on whether the interval is sub-daily.
- `align_to_interval()` - Floor-aligns a timestamp to an interval boundary using integer division.
- `now_usec()` - Gets current time as epoch microseconds, respecting the mock_now GUC.

**Table creation** - `deltax_create_table()`:
This is the main user-facing function. Given an existing empty table and a time column name, it:
1. Validates the time column is TIMESTAMPTZ (`validate_time_column`)
2. Checks the table isn't already partitioned (`check_partitioned`)
3. Renames the original table, creates a new partitioned table with the same columns (`convert_to_partitioned`)
4. Creates initial partitions: 1 past + N future + 1 default (`create_initial_partitions`)
5. Registers the table in the `deltax_deltatable` catalog

**Partition maintenance**:
- `ensure_future_partitions()` - Called by the background worker to pre-create future partitions as time advances.
- `auto_drop_partitions()` - Drops partitions older than the retention policy (`drop_after`).

**Info functions**:
- `deltax_partition_info()` - Returns set of (name, range_start, range_end, is_compressed) for a table.
- `deltax_deltatable_info()` - Returns metadata about a managed table.

### `src/catalog.rs` - Metadata CRUD

All reads/writes to the two catalog tables go through this module using pgrx SPI.

**Structs**:
- `DeltatableInfo` - Mirrors a row from `deltax_deltatable`. Fields include id, schema/table name, time_column, partition_interval, segment_by/order_by arrays, compress_after, drop_after, segment_size.
- `PartitionInfo` - Mirrors a row from `deltax_partition`. Fields include id, deltatable_id, schema/table name, range_start, range_end, is_compressed.

**Key functions**:
- `register_deltatable()` / `register_partition()` - INSERT operations
- `get_deltatable()` / `get_deltatable_by_id()` - Lookups
- `get_partitions()` - Gets all partitions for a table, ordered by range_start
- `mark_partition_compressed()` - Updates compression state, sizes, row_count, column_ndistinct
- `update_deltatable_compression()` - Stores segment_by/order_by/segment_size config

---

## Compression Engine

### `src/compress.rs` - Orchestration

This file provides the user-facing compression API and coordinates the actual compression work.

**Constants**:
- `PG_EPOCH_OFFSET_USEC` - 946,684,800,000,000. The microsecond offset between Unix epoch (1970) and PostgreSQL epoch (2000).

**User API**:
- `deltax_enable_compression()` - Configures which columns to segment by, order by, and the segment size (default 30,000 rows).
- `deltax_set_compression_policy()` - Sets `compress_after` interval for automatic compression by the background worker.
- `deltax_compress_partition()` - Manually compress a single partition.
- `deltax_decompress_partition()` - Decompress back to regular rows.
- `deltax_compression_stats()` - Returns per-partition compression ratios and sizes.

**How compression works**: When a partition is compressed, the engine reads all rows, groups them by `segment_by` columns, sorts within each group by `order_by` columns, then compresses each column independently using the best codec for its data type. The compressed data is stored in a companion table (`_deltax_compressed.<partition_name>`), and the original partition is emptied.

### `src/compression/` - Codec Implementations

**`mod.rs`** - Framework and data structures:
- `CompressionType` enum - Nine codecs: Gorilla (floats), DeltaVarint (integers), Dictionary (strings), Lz4, BooleanBitmap, Lz4Blocked, Constant, ForBitpacked, DictionaryLz4.
- `CompressedColumn` struct - Owns a compressed blob with type tag, row count, null bitmap, and data. Has `to_bytes()`/`from_bytes()` for serialization.
- `CompressedColumnRef` struct - Zero-copy borrowing view used during decompression.
- `extract_nulls()` / `reinsert_nulls()` - Null bitmap handling. Nulls are extracted before compression and reinserted after decompression.

**`gorilla.rs`** - Gorilla compression for float columns. This is the XOR-based algorithm from the Facebook Gorilla paper. It exploits the fact that consecutive float values in time-series data often have similar binary representations.

**`integer.rs`** - Integer compression:
- Delta encoding (store differences between consecutive values)
- Zigzag encoding (maps signed to unsigned: 0->0, -1->1, 1->2, -2->3)
- Varint encoding (variable-length: small values use fewer bytes)

**`dictionary.rs`** - Dictionary compression for text columns:
- Builds a dictionary of unique values
- Replaces each value with an index into the dictionary
- `should_use_dictionary()` - Uses dictionary when cardinality < 50% of rows and < 65,536 unique values

**`bitpacked.rs`** - Frame-of-Reference + bit-packing for integer columns with small ranges.

**`boolean.rs`** - Bitmap encoding for boolean columns (1 bit per value).

**`lz4.rs`** - LZ4 compression, used as a fallback or for already-dictionary-encoded data.

---

## Custom Scan Nodes: `src/scan/`

This is the most complex subsystem (~7.5k lines across the `exec/` submodules). It hooks into PostgreSQL's planner and executor to transparently decompress data during queries, so users don't need to know which partitions are compressed.

### How PostgreSQL Calls Into the Extension

PostgreSQL's query processing has distinct phases, and the extension hooks into two of them. Here's the full lifecycle showing what PostgreSQL does vs what the extension does:

```
  User runs: SELECT * FROM metrics WHERE ts > '2025-01-15' AND device_id = 'sensor-1'
  │
  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 1. PARSE + ANALYZE  (pure PostgreSQL, no extension involvement)        │
│    SQL text → parse tree → resolved Query with OIDs                    │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 2. PLANNER — path generation  (PostgreSQL calls our hook)              │
│                                                                        │
│  For each relation in the query, PG calls set_rel_pathlist_hook:       │
│                                                                        │
│  ┌─ PostgreSQL ──────────────────────────────────────────────────────┐  │
│  │ Generates default paths: SeqScan, IndexScan, etc.                │  │
│  │ For partitioned tables, generates Append over child partitions   │  │
│  └──────────────────────────┬───────────────────────────────────────┘  │
│                             │                                          │
│  ┌─ hook.rs: deltax_set_rel_pathlist ─────────────────────────────┐ │
│  │                                                                   │ │
│  │  Is this a partitioned parent with compressed children?           │ │
│  │  YES → path.rs: add DeltaXAppend path (combines all compressed   │ │
│  │        partitions into one custom scan)                           │ │
│  │                                                                   │ │
│  │  Is this a single compressed partition?                           │ │
│  │  YES → path.rs: add DeltaXDecompress path as alternative to      │ │
│  │        SeqScan, with cost estimate from cost.rs                   │ │
│  │                                                                   │ │
│  │  Also extracts Top-N info (LIMIT + ORDER BY on time column)      │ │
│  │  and pathkey info (can we advertise sorted output?)               │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                        │
│  For aggregate queries, PG calls create_upper_paths_hook:              │
│                                                                        │
│  ┌─ hook.rs: deltax_create_upper_paths ───────────────────────────┐ │
│  │  Detects aggregate patterns and offers optimized paths:           │ │
│  │  • COUNT(*) alone, no WHERE → DeltaXCount (metadata-only)        │ │
│  │  • MIN/MAX(col) alone → DeltaXMinMax (segment min/max metadata)  │ │
│  │  • SUM/AVG/COUNT + optional GROUP BY → DeltaXAgg (vectorized)    │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                        │
│  PostgreSQL's optimizer picks the cheapest path (ours or standard)     │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 3. EXECUTOR — plan execution  (PostgreSQL drives, our callbacks run)   │
│                                                                        │
│  PG executor calls our CustomScanState callbacks in this order:        │
│                                                                        │
│  ┌─ CreateCustomScanState (exec/decompress.rs) ─────────────────────┐ │
│  │  Allocates CustomScanState, sets method pointers                  │ │
│  └───────────────────────────┬───────────────────────────────────────┘ │
│                              ▼                                         │
│  ┌─ BeginCustomScan (exec/decompress.rs) ───────────────────────────┐ │
│  │  One-time initialization:                                         │ │
│  │  1. Parse companion OID + needed columns from custom_private      │ │
│  │  2. load_metadata() via SPI — get column names/types from catalog │ │
│  │  3. extract_batch_quals() — convert WHERE quals to vectorized ops │ │
│  │  4. extract_segment_filters() — extract segment_by equality and   │ │
│  │     time range predicates for pruning                             │ │
│  │  5. load_segments_heap() — direct heap scan of companion table,   │ │
│  │     reading compressed blobs, segment_by values, min/max metadata │ │
│  │  6. Sort segments by min_time                                     │ │
│  │  7. Create per-segment MemoryContext                              │ │
│  └───────────────────────────┬───────────────────────────────────────┘ │
│                              ▼                                         │
│  ┌─ ExecCustomScan (exec/decompress.rs) — called repeatedly by PG ──┐ │
│  │  Returns one tuple per call. NULL slot = end of scan.             │ │
│  │  (detailed diagram below)                                         │ │
│  └───────────────────────────┬───────────────────────────────────────┘ │
│                              ▼                                         │
│  ┌─ EndCustomScan (exec/decompress.rs) ────────────────────────────┐  │
│  │  Frees DecompressState, logs timing summary, deletes MemoryCtx   │ │
│  └───────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

### ExecCustomScan: The Row-at-a-Time Loop (exec/decompress.rs)

This is the heart of the extension. PostgreSQL calls `exec_custom_scan` once for every row it wants. The function maintains state across calls via `DecompressState`, which tracks which segment we're in and which row within that segment.

```
exec_custom_scan called by PostgreSQL
│
├─ Top-N fast path? (state.topn_done == true)
│  YES → emit next row from pre-sorted topn_buffer, apply projection, return
│  (topn_buffer was filled on first call via exec_topn_two_pass)
│
├─ Top-N first call? (topn_limit > 0 && !topn_done)
│  YES → exec_topn_two_pass(): scan all segments, keep top-N by time,
│         then re-enter as fast path above
│
└─ Normal path (main loop):
   │
   ├─ Current segment has rows remaining?
   │  │
   │  ├─ Selection vector active? Skip to next passing row (SIMD-friendly scan)
   │  │
   │  ├─ fill_slot(): copy (datum, is_null) from current_segment into scan slot
   │  │
   │  ├─ ExecQual(): PostgreSQL evaluates remaining WHERE clauses
   │  │  FAIL → row filtered, continue loop
   │  │
   │  ├─ ExecProject(): PostgreSQL projects output columns
   │  │
   │  └─ return slot (one row delivered to PostgreSQL)
   │
   └─ Need next segment:
      │
      ├─ No more segments? → return empty slot (EOF)
      │
      ├─ Segment pruning (skip without decompressing):
      │  ├─ segment_by equality filter doesn't match? → skip
      │  ├─ Time range [seg_min, seg_max] doesn't overlap query range? → skip
      │  ├─ Min/max metadata proves no rows can match? → skip
      │  └─ Dictionary LIKE pruning: no dict entry matches pattern? → skip
      │
      ├─ PHASE 1: Decompress filter columns + segment-by
      │  │  (exec/decompress.rs)
      │  │
      │  │  For each column in the table:
      │  │  ├─ Not needed by query? → push empty placeholder
      │  │  ├─ Is segment_by column? → repeat the segment's constant value N times
      │  │  ├─ Has LIKE batch qual? → decompress_text_blob_with_like_filter()
      │  │  │   (decodes dictionary, applies LIKE during decode, produces selection vector)
      │  │  ├─ Has text Eq/Ne batch qual? → decompress_text_blob_with_eq_filter()
      │  │  ├─ Has other batch qual? → decompress_blob_to_datums() (full decode)
      │  │  └─ No batch qual but others exist? → defer to Phase 2
      │  │
      │  └─ evaluate_batch_quals(): vectorized comparison over decoded columns
      │     produces selection_vector (bool per row: true = passes all quals)
      │
      └─ PHASE 2: Decompress remaining needed columns (exec/decompress.rs)
         │
         ├─ No rows selected? → skip Phase 2 entirely (avoids decode + alloc)
         │
         └─ For each deferred column:
            ├─ Text column with selection vector? → decompress_text_blob_with_selection()
            │   (only allocates varlena for rows that pass, null datum for filtered rows)
            └─ Other column? → decompress_blob_to_datums() (full decode)
```

### The Companion Table and Segment Layout

When a partition is compressed, the original partition is emptied and a "companion table" is created at `_deltax_compressed.<partition_name>`. Each row in the companion table represents one **segment** (typically 30,000 rows from the original data):

```
Companion table columns:
┌──────────────┬──────────────────────┬────────────────────────────────────┐
│ segment_by   │ metadata             │ compressed column blobs            │
│ columns      │                      │                                    │
├──────────────┼──────────────────────┼────────────────────────────────────┤
│ device_id    │ _row_count (int)     │ _ts_compressed (bytea)             │
│ (text, one   │ _min_ts (timestamptz)│ _value_compressed (bytea)          │
│  value per   │ _max_ts (timestamptz)│ _status_compressed (bytea)         │
│  segment)    │ _min_value (float8)  │ ...one blob per non-segment column │
│              │ _max_value (float8)  │                                    │
│              │ _sum_value (numeric) │                                    │
│              │ _nonnull_count_* (i8)│                                    │
└──────────────┴──────────────────────┴────────────────────────────────────┘
```

`load_segments_heap()` (exec/segments.rs) opens the companion table directly with `table_open` + `heap_getnext` (bypassing SPI) and reads all segments into memory. During this heap scan, it already applies segment-by and time-range pruning, and optionally defers TOAST detoasting for Top-N optimization.

### Batch Quals: Vectorized Filtering (exec/batch_qual.rs)

Instead of filtering row-by-row through PostgreSQL's `ExecQual`, the extension extracts simple WHERE predicates (comparisons against constants) and evaluates them in bulk over the entire decompressed segment:

`extract_batch_quals()` (exec/batch_qual.rs) walks the plan's qual list and recognizes:
- `column op constant` where op is `=`, `<>`, `<`, `<=`, `>`, `>=`, `LIKE`, `NOT LIKE`
- `column IN (const, const, ...)` — converted to `InList` op
- Flips operand order when constant is on the left (`42 < col` → `col > 42`)

`evaluate_batch_quals()` (exec/batch_qual.rs) iterates over the decompressed datum vectors and applies type-specialized comparisons (`apply_batch_filter_i64`, `_i32`, `_f64`, `_bool`, etc.) to produce a `selection_vector: Vec<bool>`. When all quals are batch-handled, PG's `ExecQual` is skipped entirely (`ps.qual = null`).

### Top-N Optimization (exec/decompress.rs)

For queries like `SELECT ... ORDER BY ts DESC LIMIT 10`, instead of decompressing all segments and letting PostgreSQL sort:

`exec_topn_two_pass()` does:
1. **Pass 1**: Scan all segments, decompress only the sort column (time) + batch qual columns. Collect candidate (segment_index, row_index, time_value) tuples. Sort by time, keep only top-N.
2. **Pass 2**: For the ~N winning rows, go back and decompress the remaining columns from only the segments that contributed winners. This avoids decompressing text blobs for segments with no winning rows.

The result is stored in `topn_buffer` and subsequent `exec_custom_scan` calls just iterate through it.

### Aggregate Pushdown (DeltaXAgg) (exec/agg.rs)

For queries like `SELECT device_id, SUM(value), COUNT(*) FROM metrics GROUP BY device_id`, the extension bypasses PostgreSQL's aggregate machinery entirely:

1. `begin_agg_scan()` (exec/agg.rs) loads segments from all companion tables and processes them in bulk:
   - For each segment, decodes only the columns needed for GROUP BY keys and aggregate inputs
   - Maintains a hash map of `GroupKey → Vec<AggAccumulator>`
   - Accumulators (`AggAccumulator` enum, exec/agg.rs) track SumInt/SumFloat/Count/CountDistinct/Min/Max
   - Supports `GROUP BY regexp_replace(col, ...)`, `GROUP BY date_trunc(unit, ts)`, `GROUP BY extract(field FROM ts)` — all computed in pure Rust without PG function calls
   - Uses `hashbrown::raw::RawTable` (exec/agg.rs) with identity hashing for fast group lookups
   - Applies HAVING filters and optional Top-N on the result groups

2. `exec_agg_scan()` (exec/agg.rs) returns pre-computed result rows one at a time.

### The Five Custom Scan Node Types

| Node | Purpose | Begin callback | Exec behavior |
|------|---------|----------------|---------------|
| **DeltaXDecompress** | Single partition decompression | Load metadata + segments from one companion table | Two-phase decompress, row-at-a-time emit |
| **DeltaXAppend** | Multi-partition decompression | Load segments from ALL companion tables into one state | Same exec as Decompress (shared `exec_custom_scan`) |
| **DeltaXCount** | `COUNT(*)` pushdown | Sum `_row_count` from all segments | Return single row with total |
| **DeltaXMinMax** | `MIN/MAX(col)` pushdown | Read `_min_*`/`_max_*` metadata, compute global min/max | Return single row per aggregate |
| **DeltaXAgg** | Full aggregate pushdown | Decode + aggregate all segments in `begin`, store results | Emit pre-computed result rows |

### Key Helper Functions in exec/

The `exec/` directory is split into focused submodules:

| Submodule | Purpose |
|-----------|---------|
| `exec/mod.rs` | Re-exports, static `CustomExecMethods` tables, shared constants |
| `exec/decompress.rs` | DeltaXDecompress + DeltaXAppend scan execution, Top-N |
| `exec/agg.rs` | DeltaXAgg aggregate pushdown, GROUP BY, accumulators |
| `exec/count_minmax.rs` | DeltaXCount + DeltaXMinMax small pushdown nodes |
| `exec/segments.rs` | Segment loading, metadata, segment pruning |
| `exec/batch_qual.rs` | Batch qual extraction, evaluation, LIKE pattern matching |
| `exec/datum_utils.rs` | Blob decompression to datums, PG type helpers, fill_slot |

| Function | Module | Purpose |
|----------|--------|---------|
| `load_metadata()` | segments | SPI query to get column info from catalog + pg_attribute |
| `load_segments_heap()` | segments | Direct heap scan of companion table, returns Vec\<SegmentData\> |
| `decompress_blob_to_datums()` | datum_utils | Decode a compressed blob to Vec\<(Datum, bool)\> by type |
| `decompress_text_blob_with_like_filter()` | datum_utils | Decode text + apply LIKE during decode (avoids varlena alloc for non-matches) |
| `decompress_text_blob_with_selection()` | datum_utils | Decode text, only allocate varlena for selected rows |
| `decompress_text_blob_with_eq_filter()` | datum_utils | Decode text + apply equality filter during decode |
| `fill_slot()` | decompress | Copy one row's datums into a TupleTableSlot |
| `extract_batch_quals()` | batch_qual | Walk plan qual tree, extract vectorizable predicates |
| `extract_segment_filters()` | segments | Extract segment_by equality + time range from quals |
| `evaluate_batch_quals()` | batch_qual | Run batch comparisons, produce selection_vector |
| `exec_topn_two_pass()` | decompress | Two-pass Top-N: sort column first, then remaining columns |
| `segment_passes_minmax_filter()` | segments | Can this segment be skipped based on min/max metadata? |
| `segment_skippable_by_dict_like()` | segments | Can this segment be skipped based on dictionary LIKE? |

### `scan/mod.rs` - Registration

Defines the custom scan node names and registers the planner/executor hooks via `_PG_init()`. Stores previous hook pointers in `AtomicPtr` statics for chaining. The `SyncStatic` wrapper makes `CustomExecMethods` structs with raw function pointers safe to use as statics. The static `CustomExecMethods` tables (one per scan node type) live in `exec/mod.rs`.

### `scan/hook.rs` - Planner Hook

Thread-local caches (COMPRESSED_CACHE, TIME_COLUMN_CACHE, SEGMENT_BY_CACHE) avoid repeated catalog lookups during planning. The planner hook intercepts query planning:
1. Detects when a query touches compressed partitions
2. Injects custom scan paths as alternatives to sequential scans
3. Chains to any previous planner hook

The `DML_BYPASS` flag lets internal operations (like compress/decompress) write to compressed partitions without being blocked.

Key functions:
- `deltax_set_rel_pathlist()` - The main planner hook. For each relation, checks if it's a compressed partition (via `COMPRESSED_CACHE` → `check_compressed_partition()`), then adds custom paths.
- `deltax_create_upper_paths()` - The upper paths hook. Detects aggregate patterns (COUNT, MIN/MAX, SUM/AVG/GROUP BY) and injects DeltaXCount, DeltaXMinMax, or DeltaXAgg paths.
- `extract_topn_info()` - Checks if LIMIT + ORDER BY matches the time column, enables Top-N.
- `check_time_pathkey()` - Can we advertise sorted output on the time column?

### `scan/path.rs` - Path Construction

Creates custom paths with cost estimates. When the planner sees a compressed partition, this module offers a DeltaXDecompress path. The planner can then choose between this and a regular sequential scan based on cost. Thread-local storage (AGG_HAVING_FILTERS, AGG_TOPN_INFO) passes info from the hook phase to the plan creation phase.

### `scan/cost.rs` - Cost Estimation

`estimate_cost()` computes startup_cost, total_cost, and expected rows for a compressed scan. Uses partition metadata (compressed_size, row_count) from the catalog.

### `scan/explain.rs` - EXPLAIN Output

Adds compression-specific details to EXPLAIN output: compression info, timing statistics, and segment pruning stats (segments scanned vs skipped).

---

## Background Worker: `src/worker.rs`

A background worker process that wakes every 60 seconds and performs maintenance.

**`register_bgworker()`** - Called from `_PG_init()` to register the worker.

**`deltax_worker_main()`** - The main loop:
1. Wait for latch signal (60s timeout)
2. Check if running on a replica (skip if so)
3. For each deltatable in the catalog:
   - `drain_default_partition()` - Move misrouted rows
   - `ensure_future_partitions()` - Pre-create 3 future partitions
   - `auto_compress_partitions()` - Compress partitions older than `compress_after`
   - `auto_drop_partitions()` - Drop partitions older than `drop_after`

**`drain_default_partition()`** - The most complex worker function:
1. Detach the default partition (so new inserts go directly to proper partitions)
2. Query for distinct time ranges in the default partition
3. Create any missing partitions on-demand
4. INSERT rows from the default partition into proper partitions
5. Reattach the empty default partition

---

## Query Helper Functions: `src/functions/`

### `functions/time_bucket.rs` - Time Bucketing

Similar to `date_trunc` but with arbitrary intervals. Two variants:
- `time_bucket(interval, timestamp)` - Truncates to interval boundary
- `time_bucket_offset(interval, timestamp, origin)` - Same but with custom origin point

Uses `floor_div()` for correct rounding toward negative infinity (important for negative timestamps).

### `functions/first_last.rs` - First/Last Aggregates

Custom aggregates that return the value associated with the earliest/latest timestamp:
- `first(value, timestamp)` - Returns value at the minimum timestamp
- `last(value, timestamp)` - Returns value at the maximum timestamp

Both use a serializable state struct (FirstState/LastState) that tracks the current best value and timestamp.

---

## Timestamp Handling: `src/timeparse.rs`

Pure-Rust timestamp parsing to avoid SPI round-trips during hot paths.

- `parse_timestamp_to_usec()` - Parses text like `"2025-01-15 14:30:00+00"` to epoch microseconds. Handles date, time, fractional seconds, and timezone offsets.
- `usec_to_timestamp_string()` - Formats microseconds back to text.
- `usec_to_date_string()` - Formats to `YYYY-MM-DD`.
- Uses Howard Hinnant's algorithm for Gregorian calendar math (`date_to_epoch_days`, `epoch_days_to_date`).

---

## Data Flow: Insert to Query

1. **Create**: `deltax_create_table('metrics', 'ts')` converts `metrics` into a partitioned table with daily partitions.

2. **Insert**: `INSERT INTO metrics VALUES (now(), 'device-1', 42.0)` - PostgreSQL routes to the correct partition natively. Out-of-range data lands in the default partition.

3. **Maintain** (background worker, every 60s):
   - Drain default partition into proper partitions
   - Pre-create future partitions
   - Compress old partitions (if `compress_after` is set)
   - Drop expired partitions (if `drop_after` is set)

4. **Query**: `SELECT * FROM metrics WHERE ts > '2025-01-15'` - The planner hook detects compressed partitions and injects DeltaXDecompress custom scan nodes. The executor transparently decompresses data. Segment elimination skips irrelevant segments.

---

## Build System

### Makefile

Everything runs in Docker containers. Key targets:
- `make dev-image` - Builds the development image (Rust toolchain + pgrx + PG headers)
- `make build` / `make test` / `make clippy` - Standard Rust development
- `make image` - Multi-stage production image (compile + runtime)
- `make run` / `make psql` - Run PG with the extension locally
- `make run-sql SQL="..."` - One-shot: build, run SQL, show output + logs, teardown
- `make bench-clickbench` - Run ClickBench benchmark suite

### Docker

- `docker/Dockerfile.dev` - Development image based on `rust:1-bookworm` with PGDG PostgreSQL headers and `cargo-pgrx 0.17`.
- `docker/Dockerfile` - Multi-stage production image. Stage 1 compiles in the dev image, stage 2 copies the compiled extension into `postgres:$PG_MAJOR-bookworm`.

### Cargo.toml

Key dependencies:
- `pgrx 0.17` - PostgreSQL extension framework
- `lz4_flex 0.11` - LZ4 compression
- `hashbrown 0.15` + `ahash 0.8` - Fast hash maps for GROUP BY
- `serde` / `serde_json` - Serialization for aggregate states

Feature flags: `pg17` and `pg18` for version-specific compilation. Release profile uses full LTO and opt-level 3.

---

## Tests

### Unit Tests (Rust, inline)

Use `#[pg_test]` macro from pgrx. Run with `make test`. Located inline in:
- `src/functions/time_bucket.rs` - Bucketing correctness
- `src/functions/first_last.rs` - Aggregate correctness
- `src/timeparse.rs` - Timestamp parsing edge cases
- `src/compression/mod.rs` - Codec round-trips

### Integration Tests (Python, `tests/`)

Use pytest + psycopg. Run with `make integration-test`.
- `conftest.py` - Fixtures: Docker container lifecycle (session-scoped), per-test database creation with extension loaded
- `test_partitioning.py` - Table creation, custom intervals, insert/query
- `test_functions.py` - time_bucket, first/last aggregates, top-N with compression
- `test_worker.py` - Future partition creation, default partition draining. Uses `pg_deltax.mock_now` GUC for deterministic time.
- `test_compression.py` - Enable compression, compress/decompress, verify data integrity

### Benchmarks (`tests/bench_*.py`)

ClickBench-based performance suite. Loads real-world web analytics data and runs 43 queries. Can compare against TimescaleDB.

---

## Key Constants

| Constant | Value | Location |
|----------|-------|----------|
| Default partition interval | 1 day | `partition.rs` |
| Default premake | 3 future partitions | `partition.rs` |
| Worker interval | 60 seconds | `worker.rs` |
| Default segment size | 30,000 rows | `compress.rs` |
| PG epoch offset | 946,684,800,000,000 us | `compress.rs` |
| Dictionary max cardinality | 65,535 | `compression/dictionary.rs` |
| Dictionary threshold | < 50% of rows | `compression/dictionary.rs` |
