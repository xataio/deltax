# Direct Backfill: Compressed Bulk Loading

## Problem

Loading data into pg_deltax currently follows a two-phase approach:

1. **COPY** data into uncompressed PostgreSQL partitions (WAL, indexes, MVCC overhead)
2. **Compress** each partition via `deltax_compress_partition()` (reads back from heap via cursor, compresses, writes to companion table, truncates heap)

This means every row is written to the heap and then read back — doubling I/O. The heap write also incurs WAL, index maintenance, and partition routing overhead that is ultimately thrown away.

## Proposal

Intercept `COPY ... FROM` with a custom format and compress data in-flight, writing directly to companion tables without ever touching the heap.

```sql
COPY hits FROM '/data/file.csv' WITH (FORMAT deltax_compress);

-- also works with stdin, program, etc.
COPY hits FROM STDIN WITH (FORMAT deltax_compress);
```

Rows are parsed, buffered in memory up to `segment_size` (default 30,000) per partition, sorted by the configured `order_by` columns, compressed, and inserted directly into the companion table in `_deltax_compressed`. The original heap partitions remain empty.

## Mechanism

PostgreSQL has no native pluggable COPY format API (a patch for PG19 exists but is not committed). The standard approach used by extensions like pg_parquet is to intercept COPY statements via the `ProcessUtility_hook`.

### Hook Flow

1. `_PG_init()` registers a `ProcessUtility_hook`
2. Hook inspects incoming utility statements for `CopyStmt` with `FORMAT 'deltax_compress'`
3. Non-matching statements chain to the previous hook / `standard_ProcessUtility`
4. Matching statements are handled entirely by our code

### FORMAT Option Handling

`FORMAT deltax_compress` is not a real PostgreSQL format — it's a signal for our hook to intercept the COPY. The hook strips the custom format option and leaves PG to default to TEXT format (tab-delimited, `\N` for NULL, backslash escapes). Additional standard COPY options (`DELIMITER`, `HEADER`, `NULL`) are preserved and passed through to `BeginCopyFrom`.

For CSV-formatted input (quoted fields, embedded commas — common when any column is jsonb or free-form text), use the companion `deltax_compress_csv` variant. The hook replaces the FORMAT with `FORMAT csv` before calling `BeginCopyFrom`, so PG's CSV parser handles quoting and `""` escapes. The CSV variant always routes through the legacy (non-parallel) path because the fast Rust parser is TEXT-only.

```sql
-- TEXT variant (tab-delimited by default)
COPY hits FROM '/data/file.tsv' WITH (FORMAT deltax_compress, DELIMITER E'\t');
COPY hits FROM STDIN WITH (FORMAT deltax_compress);

-- CSV variant (for files with quoted fields / embedded commas)
COPY events FROM '/data/events.csv' WITH (FORMAT deltax_compress_csv);
COPY events FROM '/data/events.csv' WITH (FORMAT deltax_compress_csv, HEADER true);
```

### Data Flow

```
COPY FROM (csv/text/binary)
  → parse rows (via PG's COPY parser: BeginCopyFrom / NextCopyFrom)
  → extract time column value
  → route to partition buffer (binary search on partition ranges)
  → buffer reaches segment_size
    → sort by order_by columns in Rust
    → compress columns in parallel (std::thread::scope, N workers)
    → compute segment metadata (min/max, sum, nonnull_count, ndistinct)
    → INSERT compressed segment into companion table
  → flush remaining partial segments at end
  → flush blob buffers column-major per partition
  → update catalog (is_compressed, row_count, compressed_size, etc.)
```

### Partition Routing

Each row's time column value determines which partition it belongs to. Partition ranges are loaded from the `deltax_partition` catalog at the start and searched via binary search. Rows that fall outside any existing partition range either go to the default partition (uncompressed, as today) or trigger on-demand partition creation.

### Memory

Memory usage has two components: **row buffers** (TypedColumn vecs being filled with parsed rows) and **blob buffers** (compressed blobs waiting for column-major flush).

**Row buffers**: Each partition buffer holds up to `segment_size` rows in typed column format. For ClickBench (107 columns, 30k rows), this is roughly 50 MB per active partition. With data spanning 6 partitions, peak row buffer memory is ~300 MB.

**Blob buffers**: Compressed blobs must be held per-partition until end-of-COPY for column-major insertion into the blob table (required for sequential TOAST I/O on read). For ClickBench, total compressed data is ~2.8 GB per partition. With 6 partitions, that's ~17 GB — too much.

**Mitigation**: When the input data is time-sorted (common for historical backfills), partitions are processed sequentially — a partition's blob buffer is flushed as soon as the first row for the *next* partition arrives. For unsorted data, partitions that haven't received rows in the current batch are flushed eagerly. A per-partition blob buffer threshold (e.g., 500 MB) triggers early column-major flush; this produces mostly-columnar TOAST layout with minor boundary discontinuities, which is acceptable.

If memory is a concern for very wide time ranges (many partitions active simultaneously), we keep a bounded number of partition buffers in an LRU and evict (flush column-major) the least recently used.

## When to Use

**Use `FORMAT deltax_compress` when:**

- **Backfilling historical data.** The ideal case — loading large volumes of data that will be compressed anyway. Skips the entire write-read-compress-truncate cycle.
- **Initial bulk loading.** Any scenario where you're populating a table for the first time (migrations, data imports, benchmarks).
- **Large batch ingestion.** Bulk loads where the data volume per partition is significantly larger than `segment_size` (default 30,000 rows).

**Use normal COPY/INSERT when:**

- **Small or incremental inserts.** Direct backfill flushes all buffered rows as segments at the end of each COPY, even if the buffer hasn't reached `segment_size`. Repeated small COPYs (e.g. 5,000 rows each) would create many undersized segments with poor compression ratios and more segments to scan at query time. For small batches, use normal COPY/INSERT and let the background worker or `deltax_compress_partition()` handle compression when enough data has accumulated.
- **Unique constraint enforcement is required.** Direct backfill bypasses the heap and its indexes, so unique constraints are not checked.
- **Data needs to be immediately queryable row-by-row during ingestion.** With direct backfill, data becomes visible only when the transaction commits (same as normal COPY, but worth noting).

### Streaming Behavior

COPY FROM is streaming — PostgreSQL parses rows one at a time via `NextCopyFrom()`, regardless of file size. The file is never loaded into memory as a whole. Direct backfill uses the same loop: each row is parsed, routed to the correct partition buffer, and when a buffer reaches `segment_size`, it is sorted, compressed, and flushed to the companion table. Memory is bounded by `segment_size × num_active_partitions × row_width`, not by the input file size.

For example, a 1M-row CSV produces 33 full segments of 30,000 rows + 1 partial segment of 10,000 rows, all flushed incrementally during the COPY.

This works the same with `\copy` in psql, which is a client-side wrapper that streams the file to the server via the COPY protocol — server-side it's identical to `COPY FROM STDIN`.

### Crash Safety

Crash safety is the same as normal COPY. Each completed segment is written to the companion table via a standard SQL INSERT, which is fully WAL-logged. The in-flight buffer (rows parsed but not yet flushed as a segment) lives in memory — but this is no different from normal COPY, which is also transactional: if PostgreSQL crashes mid-COPY, the entire COPY is rolled back regardless of method. Once the COPY transaction commits, all data is durable.

### Segment Sizing

Direct backfill creates one segment per `segment_size` rows per partition. At the end of each COPY, any remaining rows in a partition buffer are flushed as a partial segment. This means the last segment per partition may be undersized, which is acceptable for large bulk loads but problematic if used for many small loads. As a rule of thumb, each COPY should load at least `segment_size` rows per partition to get good segment utilization.

A future enhancement could send the leftover tail to the heap instead of creating a partial segment, letting the background worker merge it into a full segment later.

## Parallelism

Compression is CPU-intensive for wide tables. Direct backfill parallelizes the per-segment compression step using the same `std::thread::scope` pattern used by the DeltaXAgg read path.

### Per-Column Parallel Compression

When a partition buffer reaches `segment_size`, compression proceeds in two phases:

```
1. sort_typed_columns()                         — sequential (shared data)
2. parallel compression (std::thread::scope):
   for each column (distributed across N workers):
     compress_typed_column()                    — pure Rust, thread-safe
     compute_typed_minmax()                     — pure Rust, thread-safe
     compute_typed_sum()                        — pure Rust, thread-safe
3. compute_segment_ndistinct()                  — sequential (reads all columns)
4. compute_segment_blooms()                     — sequential
5. INSERT meta row via SPI                      — sequential (main thread only)
6. buffer compressed blobs                      — sequential
```

Step 2 is embarrassingly parallel — each column's compression is independent. For ClickBench (105 columns) with 8 workers, this reduces compression wall time by ~8×.

Worker count is controlled by the existing `pg_deltax.parallel_workers` GUC (default: auto = `num_cpus::get().min(16)`).

### Threading Constraints

- **Main thread only**: `NextCopyFrom` (COPY parsing), SPI calls (INSERT into meta/blobs), memory context operations
- **Thread-safe (pure Rust)**: `sort_typed_columns`, `compress_typed_column`, `compute_typed_minmax`, `compute_typed_sum`, `compute_segment_ndistinct`, bloom filter construction

This is the same constraint boundary as the existing DeltaXAgg parallel path: workers do pure Rust computation, the main thread handles all PostgreSQL backend calls.

### Future: Pipeline Parallelism

A follow-up optimization could overlap COPY parsing with compression: the main thread fills segment N+1 while workers compress segment N. This requires double-buffering the TypedColumn arrays per partition but would further improve throughput by hiding compression latency behind I/O.

## What We Reuse

The existing compression pipeline in `compress.rs` is already factored into reusable pieces:

- `classify_column` / `TypedColumn` / `init_typed_columns` — column type classification and storage
- `sort_typed_columns` — in-memory sorting by order_by columns
- `flush_segment_metadata` / `flush_with_splitting` — compression + companion table INSERT (returns blobs for caller to buffer)
- `compute_segment_ndistinct` — HyperLogLog cardinality estimation
- `compress_typed_column` — per-column compression dispatch
- `compute_typed_minmax` / `compute_typed_sum` — segment metadata computation

### What Needs Extraction

- **Companion table DDL generation**: Currently inline in `compress_partition_impl`. Extract a `build_companion_ddl(partition_name, columns) -> (meta_ddl, blobs_ddl, blooms_ddl)` helper for reuse by both compression and direct backfill.
- **Datum-to-TypedColumn append**: The existing `append_row_to_columns` reads from `SpiHeapTupleData`. Direct backfill gets `(Datum*, bool*)` arrays from `NextCopyFrom`. A new `append_datums_to_columns(values, nulls, kinds, typed_cols)` function is needed — this is actually simpler than the SPI path since raw Datums are already typed.

The new code is primarily: ProcessUtility hook, FORMAT option handling, COPY row parsing via `BeginCopyFrom`/`NextCopyFrom`, partition routing, and per-partition blob buffer management.

## What Changes for the Scan Hook

Nothing. The scan hook already detects compressed partitions by checking for a companion table in `_deltax_compressed`. Since direct backfill writes to the same companion tables with the same schema, queries work transparently.

## Companion Table Creation

Companion tables (meta, blobs, blooms) are created per-partition on first row arrival for that partition, inside the COPY transaction. Other sessions cannot see them until the transaction commits, so the scan hook is not confused by partially-populated tables.

If the COPY is aborted or PostgreSQL crashes, the transaction rolls back and the companion tables are dropped automatically.

If a partition is already compressed (companion tables already exist), the hook errors rather than silently writing alongside existing compressed data, which would create duplicate/conflicting segments.

## Limitations (Initial Version)

- **Bulk load only.** This is for initial data loading, not for ongoing inserts into compressed partitions. The DML blocking on compressed partitions remains.
- **Already-compressed partitions rejected.** If any row routes to a partition that is already compressed, the COPY errors. Decompress first if you need to reload.
- **Compression must be enabled first.** The table must have `deltax_enable_compression()` called before using `FORMAT deltax_compress`, so we know the order_by, segment_by, and segment_size settings.
- **Partitions must exist.** The target partitions should already be created (via `deltax_create_table`). Rows that don't fit any partition go to the default partition uncompressed.
- **No unique constraint enforcement.** Since we bypass the heap, unique indexes on the original table are not checked.

## Implementation Order

### Step 1: Extract reusable helpers from `compress.rs`

- `build_companion_ddl(partition_name, columns) -> (meta_ddl, blobs_ddl, blooms_ddl)` — factor out of `compress_partition_impl`
- `append_datums_to_columns(values, nulls, tupdesc, kinds, typed_cols)` — new function, takes raw Datum arrays from `NextCopyFrom`
- Make `flush_segment_metadata`, `flush_with_splitting`, `compress_typed_column`, `sort_typed_columns`, etc. `pub(crate)` as needed

### Step 2: ProcessUtility hook skeleton

- Register `ProcessUtility_hook` in `_PG_init()`, chain previous hook using the existing `AtomicPtr` pattern
- Detect `CopyStmt` with `is_from = true` and `FORMAT 'deltax_compress'` in options
- Validate: table must be deltax-managed (`get_deltatable`), compression enabled (`order_by`/`segment_by` configured), target is not `COPY TO`
- Strip custom format option, default to CSV for the underlying parser

### Step 3: Partition routing

- Load partitions via `get_partitions()` at COPY start, build sorted range array
- Binary search on timestamp for each row
- Error if a row routes to an already-compressed partition
- Rows outside all partition ranges go to the default partition (uncompressed, via normal INSERT)

### Step 4: Core COPY loop

- `BeginCopyFrom` with cleaned options and the target relation
- `NextCopyFrom` → extract time column Datum → route to partition buffer → `append_datums_to_columns`
- When buffer reaches `segment_size`: sort → parallel compress → flush metadata → buffer blobs
- `EndCopyFrom` when done

### Step 5: End-of-COPY flush

- Flush remaining partial segments per partition
- Flush blob buffers column-major per partition (SPI INSERT into blobs table)
- Flush bloom buffers per partition
- `ANALYZE` companion tables
- Update catalog: `mark_partition_compressed` for each partition that received data
- Invalidate compressed cache (`invalidate_compressed_cache`)
- Report row counts

### Step 6: Parallel compression

- Add `std::thread::scope` to the segment flush path
- Distribute columns across workers for `compress_typed_column` + metadata computation
- Collect results, continue with sequential SPI INSERT on main thread
- Respect `pg_deltax.parallel_workers` GUC

## Future: Accepting Writes to Compressed Partitions

Direct backfill is a stepping stone toward a hybrid storage model where compressed partitions accept ongoing writes:

1. Allow INSERTs to land in the heap even when a companion table exists (partially compressed partition)
2. Scan hook merges data from both companion (compressed) and heap (uncompressed)
3. Background worker periodically folds heap rows into new compressed segments

This is the same architecture TimescaleDB uses for inserts into compressed chunks. Direct backfill establishes the infrastructure (partition routing, in-memory compression, companion writes) that the hybrid model builds on.
