# Columnar Blob Storage Architecture

## Overview

DeltaX splits each compressed partition into five tables:

1. **Meta table** — thin per-segment metadata: segment-by values, time bounds, row count (no BYTEA, no TOAST)
2. **Colstats table** — normalized per-column statistics (min/max/sum/counts), one row per (column, segment)
3. **Blob table** — compressed column data, inserted in column-major order for sequential I/O
4. **Blooms table** — per-segment packed bloom filters for equality predicate pushdown
5. **Text lengths table** — per-row character-count sidecars for text columns, consulted when a query only needs `length(col)` / `col <> ''`

This layout replaces the original single companion table design where all
compressed column blobs were stored as BYTEA columns in one row per segment.
The motivation and measurements behind this change are in the appendix.

## How Data is Organized into Segments

A DeltaX compressed table partitions data in two dimensions: **time-based
partitions** (PostgreSQL declarative partitioning) and **segments** within
each partition (groups of rows compressed together).

```
Original table: hits (100M rows, 105 columns)
│
├── Partition 1 (2013-07-01 to 2013-07-08) ── ~20M rows
│   ├── Segment 1 ── 30,000 rows, ordered by EventTime
│   ├── Segment 2 ── 30,000 rows
│   ├── ... (~667 segments per partition)
│   └── Segment 667 ── remaining rows
│
├── Partition 2 (2013-07-08 to 2013-07-15)
│   └── ... (~667 segments)
│
├── ... (5 partitions total)
│
└── Partition 5
    └── ...
```

Each segment's compressed blobs are stored in the blob table (one row per
column per segment). The segment size defaults to 30,000 rows (configurable
via `segment_size` parameter).

## Table Layout

### 1. Segment Metadata Table (`<partition>_meta`)

Thin per-segment metadata. Contains only segment-by values, time column bounds,
and row count. No BYTEA columns, no per-column statistics — so scanning it is
extremely fast regardless of how many columns the table has.

```sql
CREATE TABLE "_deltax_compressed"."<partition>_meta" (
    _segment_id        SERIAL PRIMARY KEY,

    -- Segment-by columns (original types)
    "<seg_by_col>"     <type>,

    -- Time column bounds
    _min_<time_col>    TIMESTAMPTZ,
    _max_<time_col>    TIMESTAMPTZ,

    -- Per-segment row count
    _row_count         INT
);
```

For ClickBench (105 columns, ~667 segments per partition): the meta table has
only 4 columns per row, fits in ~31 pages total across 10 partitions, and
scans in <1 ms.

### 2. Column Statistics Table (`<partition>_colstats`)

Normalized per-column statistics with a fixed 8-column schema. One row per
(column, segment) pair — the same layout as the blob table. This replaces
the original wide meta table design where all per-column stats were stored
as columns in the meta table (see Appendix for the motivation).

```sql
CREATE TABLE "_deltax_compressed"."<partition>_colstats" (
    _col_idx           SMALLINT NOT NULL,
    _segment_id        INT NOT NULL,
    _min               INT8,          -- order-preserving encoded min
    _max               INT8,          -- order-preserving encoded max
    _sum               NUMERIC,       -- exact sum (avoids i128 overflow)
    _nonnull_count     INT,
    _nonzero_count     INT,
    _ndistinct         INT,
    PRIMARY KEY (_col_idx, _segment_id)
);
```

**Key design decisions:**

- **Fixed narrow schema**: 8 columns regardless of how many columns the
  original table has. `heap_deform_tuple` cost is negligible.
- **INT8 min/max with order-preserving encoding**: All orderable types
  (integers, floats, timestamps, dates) are encoded to i64 values that
  preserve comparison order. This allows segment pruning to compare encoded
  values directly without type dispatch. Float encoding uses the standard
  sign-bit flip technique; timestamps/dates convert from PG-epoch to
  Unix-epoch microseconds.
- **NUMERIC sum**: Avoids precision loss for large integer sums (i128 range)
  while keeping the row narrow.
- **Column-major insertion order**: Rows are inserted sorted by
  `(_col_idx, _segment_id)` — the same pattern as the blob table. This
  ensures rows for a single column are physically contiguous on the heap,
  enabling fast PK index scans that read sequential pages.
- **PK `(_col_idx, _segment_id)`**: Enables efficient lookups for a single
  column's stats across all segments.

**Adaptive scan strategy**: The read path chooses between PK index scan
(for queries touching few columns) and sequential scan (for queries touching
many columns). The threshold is: use index scan if `needed_cols < total_cols / 2`
or `needed_cols <= 4`.

For ClickBench: each `_col_idx` occupies ~6-8 contiguous heap pages per
partition. A single-column PK index scan reads ~8 pages instead of scanning
all ~630 pages.

### 3. Column Blob Table (`<partition>_blobs`)

Stores compressed column data. One row per (column, segment) pair.

```sql
CREATE TABLE "_deltax_compressed"."<partition>_blobs" (
    _col_idx     SMALLINT NOT NULL,
    _segment_id  INT NOT NULL,
    _data        BYTEA,
    PRIMARY KEY (_col_idx, _segment_id)
);
```

The key design decision is **column-major insertion order**: blobs are
inserted sorted by `(_col_idx, _segment_id)`. Because PostgreSQL writes TOAST
chunks in insertion order, this naturally produces a columnar physical layout
with no post-processing:

```
Meta table (no TOAST, ~31 pages for 10 partitions):
┌──────────────────────────────────────────────────────┐
│ seg 1: seg_by | _min/max_time | _row_count           │
│ seg 2: ...                                           │
│ ...                                                  │
│ seg 667: ...                                         │
└──────────────────────────────────────────────────────┘

Colstats table (column-major insertion → contiguous per col_idx):
┌──────────────────────────────────────────────────────┐
│ (col=0, seg=1) | (col=0, seg=2) | ... | (col=0,667) │  ← col 0 stats
│ (col=1, seg=1) | (col=1, seg=2) | ... | (col=1,667) │    contiguous
│ ...                                                  │
│ (col=104, seg=1) | ... | (col=104, seg=667)          │
└──────────────────────────────────────────────────────┘

Blob table (column-major insertion → columnar TOAST):
┌──────────────────────────────────────────────────────┐
│ (col=0, seg=1, blob) | (col=0, seg=2, blob) | ...   │  ← col 0 blobs
│ (col=0, seg=667, blob) |                             │    contiguous
│ (col=1, seg=1, blob) | (col=1, seg=2, blob) | ...   │  ← col 1 blobs
│ ...                                                  │    contiguous
│ (col=104, seg=1, blob) | ... | (col=104, seg=667)   │
└──────────────────────────────────────────────────────┘

TOAST heap (follows insertion order):
┌──────────────────────────────────────────────────────┐
│ col0_seg1 | col0_seg2 | ... | col0_seg667 |         │  ← sequential
│ col1_seg1 | col1_seg2 | ... | col1_seg667 |         │    for each
│ ...                                                  │    column
│ col104_seg1 | ... | col104_seg667 |                  │
└──────────────────────────────────────────────────────┘
  ↑ Reading AdvEngineID = first 1/105th of TOAST = sequential I/O
```

Reading one column = sequential I/O on a contiguous ~1/105th slice of the
TOAST table. The kernel's readahead (128 KB default on Linux) prefetches
upcoming chunks automatically.

### 4. Bloom Filter Table (`<partition>_blooms`)

Stores per-segment packed bloom filters for equality predicate pushdown.
Kept in a separate table (rather than inline in meta) to avoid adding TOAST
overhead to the meta table, which must stay fast for all queries.

```sql
CREATE TABLE "_deltax_compressed"."<partition>_blooms" (
    _segment_id  INT PRIMARY KEY,
    _data        BYTEA NOT NULL
);
```

The `_data` column contains a packed binary format with variable-size bloom
filters for each numeric/date/timestamp column in the segment:

```
Wire format (repeated per column):
┌─────────────┬────────────┬───────────┬──────────────────────┐
│ col_idx: u16 │ num_hashes: u8 │ size: u16 │ bloom_bits: [u8; size] │
└─────────────┴────────────┴───────────┴──────────────────────┘
```

**Dynamic sizing**: Bloom filter size scales with `ndistinct` for each
column: `bloom_size = ndistinct × 10 bits / 8 bytes`, clamped to 64 B – 8 KB.
The optimal number of hash functions is `k = (m/n) × ln(2)`, clamped to
[1, 10]. This gives a false positive rate of ~0.8% at ~2-3% storage overhead.

Bloom filters are built during compression for numeric, date, and timestamp
columns. Building can be disabled via the `pg_deltax.bloom_filters` GUC
(default: on). The read path gracefully handles missing bloom data — if the
blooms table doesn't exist, the bloom phase is skipped.

### 5. Text Lengths Table (`<partition>_text_lengths`)

Stores a per-row character-length sidecar for every text column, one row per
(text_column, segment) pair. The planner routes text columns to this table
when every query-time reference is `length(col)`, `col = ''`, or `col <> ''`
— i.e. the actual string bytes are never needed. The sidecar is ~50–80×
smaller than the main text blob, so detoasting it is near-free.

```sql
CREATE TABLE "_deltax_compressed"."<partition>_text_lengths" (
    _col_idx     SMALLINT NOT NULL,
    _segment_id  INT NOT NULL,
    _data        BYTEA NOT NULL,
    PRIMARY KEY (_col_idx, _segment_id)
);
```

**Wire format** of `_data` (same `CompressedColumn` framing as the main
blob table, just with a u32-array payload):

```
┌─────────────────────────────────────────────────────────────────┐
│ tag: u8 (= Lz4) | row_count: u32_le | has_nulls: u8             │
│ [null_bitmap: ceil(row_count/8) bytes if has_nulls]             │
│ lz4_compressed_u32_array_of_non_null_character_counts           │
└─────────────────────────────────────────────────────────────────┘
```

Each stored value is `s.chars().count() as u32` — PostgreSQL's `length(text)`
semantics, not byte length. Null rows are encoded via the null bitmap and
appear as `0` in the decoded array; callers distinguishing "null" from
"empty" consult the bitmap.

**Why a separate table, not a column in `blobs`:** sidecar loading is
selective (only for columns the planner has marked sidecar-only) and the
main blob load is suppressed for those columns, so mixing them in one table
would require a second index scan anyway. Keeping them separate also lets
the sidecar table be missing (old compressed data) and the reader silently
falls back to the main blob.

**Which text columns get a sidecar:** all text columns (text / varchar /
bpchar) at compress time. Storage cost is low — typical character-length
arrays compress to a few KB per segment via LZ4 because neighbouring rows
have similar lengths. For ClickBench: ~650 MB across all 18 partitions for
all ~28 text columns combined, vs multi-GB for the main text blobs.

**Relation to colstats.** The colstats `_sum`, `_nonnull_count`, and
`_nonzero_count` columns are also populated for text columns: `_sum` holds
`SUM(length)`, `_nonzero_count` holds the number of non-empty rows. These
powered future metadata-only fast paths when the grouping is constant per
segment.

See PERF_IMPROVEMENTS #42 for the performance impact.

## Read Path

The read path is a four-phase process in `load_segments_heap`, followed by
an optional sidecar load for text-length queries:

### Phase 1: Meta Scan (zero TOAST I/O)

Scan the thin meta table with `heap_getnext()`. Apply pruning:

1. **Segment-by filters**: skip segments with non-matching segment_by values
2. **Time range filters**: skip segments outside query time range using
   `_min_<time>` / `_max_<time>`

Collect surviving segments into an array. This phase involves zero TOAST I/O
and reads only ~31 pages across 10 partitions.

### Phase 1b: Colstats Scan (targeted column stats)

Only runs when the query needs per-column statistics not available in the meta
table (minmax for non-time columns, sum/count data, nonzero counts for WHERE
filters). Callers specify exactly which columns they need via
`needed_minmax_cols` and `needed_stats_cols` parameters.

1. **PK index scan** (few columns needed): one index scan per needed `_col_idx`,
   reading only the contiguous pages for that column
2. **Sequential scan** (many columns needed): single pass over all colstats rows,
   filtering by `_col_idx`

For each surviving segment, populate `col_minmax` (for minmax pruning and
MIN/MAX pushdown) and `col_sums` (for SUM/AVG/COUNT pushdown and WHERE filter
evaluation via nonzero_count).

Additional minmax pruning is applied here: segments where min/max values
prove no rows can match the batch quals are pruned.

### Bloom Phase: Equality Predicate Pushdown

Only runs when the query has equality (`=`) or `IN` predicates on numeric,
date, or timestamp columns. Scans the blooms table for surviving segments:

1. Open the blooms table and scan via `scan_getnextslot`
2. For each row, check if the segment survived Phase 1 (HashSet lookup)
3. Detoast the packed bloom data, look up the target column's bloom filter
4. If the bloom filter says the value is definitely not present, mark the
   segment for pruning

Pruned segments are removed from the surviving set before Phase 2, avoiding
unnecessary blob I/O.

**Performance**: On ClickBench Q19 (`WHERE UserID = <value>`), bloom filters
prune ~97.5% of segments, reducing query time from ~8s to ~0.2s.

### Phase 2: Column Blob Reads (sequential TOAST I/O)

For each needed column, read blobs from the blob table via PK index scan:

```
For each needed col_idx:
    Index scan: _col_idx = X (scans all segment_ids for this column)
    For each row: check if segment_id is in surviving set
    If yes: detoast blob → sequential TOAST I/O (contiguous region)
    Store in SegmentData.compressed_blobs[blob_idx]
```

Because blobs were inserted in column-major order, the TOAST chunks for one
column are contiguous on disk.

### Phase 2b: Text-Length Sidecar (optional)

Runs when the planner marks any text column as sidecar-only — i.e. every
reference is `length(col)` / `col = ''` / `col <> ''` and the column is not
in GROUP BY. The caller suppresses the main-blob entry for that column's
`col_idx` before invoking Phase 2, then calls `load_text_length_sidecars`
to read from the `*_text_lengths` table.

The loader mirrors Phase 2: PK index scan by `_col_idx`, detoast each
matching segment's `_data`, store into `SegmentData.text_length_blobs`.
Because the sidecar is ~1–2% the size of the main text blob, detoast cost
is near-negligible. Works per-column: some cols on a query can be
sidecar-only while others keep the main blob.

When the sidecar table is absent (data compressed before the sidecar
feature), the loader silently no-ops and callers fall back to the main
blob path.

Currently wired into the parallel mixed aggregation path only
(`ParallelMixedConfig.sidecar_only_cols`). Other paths ignore the flag.

### Parallel Workers

The current parallel dispatch pattern is preserved:
1. Main thread: Phase 1 + Bloom + Phase 2 (+ Phase 2b) → `Vec<SegmentData>`
2. Dispatch segments to parallel workers for decompression + aggregation

Phase 2 runs on the main thread because `pg_detoast_datum` requires a valid
PostgreSQL backend context. However, the I/O is now sequential per column,
so the main thread can saturate the storage bandwidth.

## Write Path

### During Compression

Compression processes one segment at a time (all columns for segment 1, then
all columns for segment 2, etc.). To achieve column-major insertion into the
blob table, compressed blobs are buffered in memory and flushed after all
segments are processed.

```
Phase 1: Compress all segments, buffer blobs, colstats, blooms, and text lengths

for each batch of 30,000 rows:
    compress all columns → 105 blobs
    for each text column: build per-row character-length array, LZ4-compress → text length blob
    compute per-column stats (encoded min/max, sum, counts, ndistinct)
        — for text columns, _sum = SUM(length), _nonzero_count = non-empty rows
    compute bloom filters for numeric/date/timestamp columns
    INSERT meta row immediately (segment_by, time bounds, row_count)
    buffer colstats rows in memory (keyed by col_idx, segment_id)
    buffer compressed blobs in memory (keyed by col_idx, segment_id)
    buffer packed bloom data in memory (keyed by segment_id)
    buffer text length blobs in memory (keyed by col_idx, segment_id)

Phase 2: Flush colstats in column-major order

sort colstats_buffer by (col_idx, segment_id)
batch INSERT INTO colstats (100 rows per INSERT)

Phase 3: Flush blobs in column-major order

sort blob_buffer by (col_idx, segment_id)
for each (col_idx, segment_id, blob) in blob_buffer:
    INSERT INTO blobs (_col_idx, _segment_id, _data)

Phase 4: Flush bloom filters

for each (segment_id, bloom_data) in bloom_buffer:
    INSERT INTO blooms (_segment_id, _data)

Phase 5: Flush text-length sidecars in column-major order

sort text_length_buffer by (col_idx, segment_id)
for each (col_idx, segment_id, length_blob) in text_length_buffer:
    INSERT INTO text_lengths (_col_idx, _segment_id, _data)

ANALYZE meta, colstats, blobs, blooms, text_lengths
```

### Memory Impact

Buffering requires holding all compressed blobs and colstats rows for one
partition in memory. For ClickBench (105 columns, ~667 segments per partition),
the compressed blobs total ~2.8 GB per partition. Colstats rows add negligible
overhead (~50 bytes × 105 × 667 ≈ 3.5 MB). Text-length sidecars add ~35 MB
per partition (~28 text columns × 667 segments × a few KB each after LZ4).
A typical time-series table with 10-20 narrower columns and smaller
partitions would buffer tens of MB.

This is acceptable because:
- Compression is a batch operation (not latency-sensitive)
- The server already needs substantial memory for the uncompressed data
  during compression
- Peak memory can be reduced by flushing one column at a time: after all
  segments are compressed, iterate columns and flush each column's blobs,
  freeing them immediately after insertion

### During Decompression

`deltax_decompress_partition` reads from the meta and blob tables (meta for
segment metadata, blobs for column data) and drops all five companion tables
after restoring data to the original partition. The text-length sidecars are
derived data and are simply dropped — they don't need to be read back
because the original row values are restored from the main blobs.

## Alternatives Considered

### Wide meta table (all stats in one row per segment)
The original design stored all per-column statistics (min, max, sum,
nonnull_count, nonzero_count, ndistinct) as columns in the meta table — roughly
5 columns per data column. For ClickBench with 105 columns, this meant ~500+
columns per meta row. `heap_deform_tuple` on these wide rows cost 50-70ms
across 3338 segments, even when only one column's stats were needed. The
normalized colstats table has a fixed 8-column schema, eliminating this cost.
The column-major insertion order ensures that queries needing only a few
columns' stats read contiguous pages via PK index scan.

### CLUSTER after segment-major insertion
Instead of buffering blobs for column-major insertion, insert in segment
order (natural compression order) and then run `CLUSTER blobs USING pkey`
to rewrite the heap + TOAST in column-major order. Rejected because CLUSTER
writes the data twice (2× write amplification) and takes
`AccessExclusiveLock`. Column-major insertion achieves the same physical
layout with no extra I/O.

### One table per column
Perfect I/O locality but creates N tables per partition (105 × 7 = 735
tables for ClickBench). Management overhead is high, catalog bloat affects
planning time, and DDL operations (DROP PARTITION) become expensive.

### Column-chunk concatenation
Store all segments' data for one column in a single large BYTEA with an
offsets array. Perfect locality (one TOAST detoast per column) but:
- Cannot skip individual segments (must read entire column blob)
- Any modification requires rewriting the entire blob
- Maximum BYTEA size is 1 GB (could be hit for large columns)

### posix_fadvise prefetching
Tested and found ineffective on gp2 EBS. The prefetch reads 100× more data
than needed for selective queries. On high-IOPS storage (NVMe), the random
reads aren't a bottleneck in the first place.

### Separate files (outside PostgreSQL)
Would give full control over I/O layout but breaks PostgreSQL replication,
pg_dump, and crash recovery. Incompatible with the requirement that standard
PostgreSQL replication must work.

### Bloom filters inline in meta table
The initial implementation stored packed bloom data as a `_blooms BYTEA`
column in the meta table. This caused 5-15% regression on all queries because
the meta table rows became TOAST-heavy — even queries with no equality
predicates paid the cost of larger heap pages. Moving blooms to a separate
table keeps the meta table TOAST-free.

### Skipping `COMPRESSION lz4` on companion BYTEA columns
The columnar payloads in `_blobs._data`, `_blooms._data`, `_text_lengths._data`,
and `_valbitmap._bits` are already maximally compressed by the Rust codecs
before PostgreSQL ever sees them, so the `COMPRESSION lz4` column attribute
looked like dead weight. We A/B-tested three configurations on the full
ClickBench dataset (100M rows × 105 columns, c6a.4xlarge, 2026-05-21):

|                            |  Load |    Size | Cold sum | Hot sum |
|----------------------------|------:|--------:|---------:|--------:|
| `COMPRESSION lz4` (default)| 328 s | 14.59 GB|   317 s  | 59.8 s  |
| no clause (pglz fallback)  | 656 s | 14.51 GB|   327 s  | 60.0 s  |
| `STORAGE EXTERNAL`         | 415 s | 21.34 GB|   354 s  | 59.6 s  |

`COMPRESSION lz4` wins on every dimension that matters: storage is unchanged
vs. either alternative, hot queries are identical (the blob cache absorbs
detoast cost), cold queries are fastest, and the load is roughly half the
pglz-fallback time. The reasoning ended up being the opposite of the original
hypothesis:

- The lz4 TOAST pass is *not* shrinking the bytes (they're already lz4-flex
  output). What it's doing is exiting fast on uncompressible data via lz4's
  cheap "skip" check.
- With no clause, PostgreSQL falls back to **pglz**, which has no equivalent
  fast-skip path. It grinds through the full bytes, fails to compress, and
  stores raw — paying full pglz CPU on every blob write. That's where the
  2× load-time blowup comes from.
- `STORAGE EXTERNAL` skips compression entirely (good) but forces every
  blob out-of-line into TOAST regardless of size (bad). For us, many
  per-segment blobs are small (constant-encoded numeric columns,
  low-cardinality valbitmaps, tiny bloom payloads); `EXTENDED` with
  uncompressible data keeps those inline, while `EXTERNAL` pays a TOAST
  chunk's worth of overhead per blob. Worst-hit cold queries (Q7 +147%,
  Q11 +52%, Q10 +43%) are exactly the scan-many-small-blobs shapes.

Net: keep `COMPRESSION lz4` as the default; fall back to letting pglz
attempt-then-give-up only when the running PostgreSQL was built without
`--with-lz4`. The fallback's storage and read costs are negligible — the
real price of a missing `--with-lz4` build is the doubled load time.

## Open Questions

1. **TOAST chunk size**: PostgreSQL's default TOAST chunk size is ~2000 bytes.
   For blobs that are 50-100 KB, this means 25-50 chunks per blob. We could
   investigate `toast_tuple_target` to reduce chunk overhead, but this is a
   minor optimization.

2. **Blob table without TOAST**: If we ensure blob sizes stay under ~2 KB
   (by chunking at our level), we could use `ALTER COLUMN _data SET STORAGE
   MAIN` to prevent TOAST entirely. This eliminates the TOAST index lookup
   overhead but requires managing our own chunking. Worth investigating if
   TOAST lookup overhead is significant.

3. **Insertion order durability**: TOAST physical layout depends on insertion
   order, which PostgreSQL does not formally guarantee across restarts or
   `VACUUM FULL`. In practice, since compressed data is write-once (immutable
   after compression) and never updated/deleted, the layout should be stable.
   `VACUUM` won't reorder existing pages. However, if we ever need to
   re-guarantee ordering (e.g., after a pg_dump/restore), we could add a
   `CLUSTER` as a repair step.

## Appendix: Motivation — TOAST I/O Analysis

The columnar layout was motivated by measuring TOAST I/O overhead with the
original single companion table design. In that design, all compressed column
blobs (~105 BYTEA columns) were stored in one row per segment. PostgreSQL
TOASTed these blobs into a shared TOAST heap where chunks from different
columns were interleaved in insertion order.

```
Original single-table layout:
┌──────────────────────────────────────────────────────────────────────┐
│ Row 1: seg_by | _row_count | _min/max_time | _min/max_col0..104 |  │
│        _col0_compressed (BYTEA→TOAST) | ... | _col104_compressed   │
├──────────────────────────────────────────────────────────────────────┤
│ Row 2: ... same 105 BYTEA blobs, all TOASTed ...                   │
├──────────────────────────────────────────────────────────────────────┤
│ ...                                                                 │
└──────────────────────────────────────────────────────────────────────┘

TOAST heap (physical disk order = insertion order):
┌─────────────────────────────────────────────────────────┐
│ seg1_col0 | seg1_col1 | ... | seg1_col104 |            │  ← all cols
│ seg2_col0 | seg2_col1 | ... | seg2_col104 |            │    interleaved
│ ...                                                     │
│ seg667_col0 | seg667_col1 | ... | seg667_col104 |      │
└─────────────────────────────────────────────────────────┘
  ↑ Reading AdvEngineID = every 105th blob = random I/O
```

When a query needed only one column, PostgreSQL detoasted that column's blob
from each segment independently. Because the chunks for one column were
scattered across the entire TOAST table, the I/O pattern was random.

**Measured on gp2 EBS (ClickBench 100M rows, r7i.4xlarge, cold cache):**

We instrumented `load_segments_heap` to separately measure `heap_getnext`
(reading companion table heap pages), `heap_deform_tuple` (extracting
datums), and `pg_detoast_datum` (TOAST I/O). Results across all 43 queries:

- **`heap_getnext` + `heap_deform_tuple`**: ~1-2ms per partition — negligible
- **`pg_detoast_datum`**: 99-100% of `heap_scan` time for every DeltaXAgg query
- **All blobs were TOASTed** (0 inline blobs observed)

Example queries:

| Query | Columns | Cold Total | detoast | detoast % of total |
|-------|---------|------------|---------|-------------------|
| Q7    | 1       | 3.3s       | 3131ms  | 96%               |
| Q21   | 3       | 30.1s      | 28679ms | 95%               |
| Q22   | 5       | 53.4s      | 50308ms | 94%               |
| Q32   | 4       | 36.8s      | 27905ms | 76%               |

The entire cold-run bottleneck was TOAST random I/O. The median detoast % of
total execution time across DeltaXAgg queries was **86%**.

### Full Cold Run Measurements (Original Layout)

Measured on r7i.4xlarge, gp2 500GB EBS, ClickBench 100M rows, PostgreSQL 18.
Each query run after `systemctl restart postgresql && echo 3 > /proc/sys/vm/drop_caches`.

Sorted by detoast % of total execution time (descending).

#### DeltaXAgg Path (32 queries)

| Query | Total (s) | heap_scan (ms) | detoast (ms) | detoast % of heap_scan | detoast % of total | Description |
|-------|-----------|----------------|--------------|------------------------|--------------------|-------------|
| Q10 | 12.5 | 12266 | 12239 | 100% | 98% | MobilePhoneModel, COUNT(DISTINCT UserID) WHERE MobilePhoneModel <> '' |
| Q11 | 14.7 | 14401 | 14371 | 100% | 98% | MobilePhone+Model, COUNT(DISTINCT UserID) |
| Q7 | 3.3 | 3155 | 3131 | 99% | 96% | AdvEngineID, COUNT(*) WHERE <> 0 |
| Q21 | 30.1 | 28709 | 28679 | 100% | 95% | SearchPhrase, MIN(URL), COUNT(*) WHERE URL LIKE google |
| Q9 | 21.7 | 20482 | 20452 | 100% | 94% | 5 aggs GROUP BY RegionID, COUNT(DISTINCT UserID) |
| Q22 | 53.4 | 50340 | 50308 | 100% | 94% | SearchPhrase+URL+Title, 5 aggs, 3 LIKE filters |
| Q14 | 16.9 | 15664 | 15637 | 100% | 93% | SearchEngineID+SearchPhrase, COUNT(*) |
| Q8 | 17.8 | 16396 | 16367 | 100% | 92% | RegionID, COUNT(DISTINCT UserID) |
| Q17 | 21.1 | 19394 | 19365 | 100% | 92% | UserID+SearchPhrase, COUNT(*) LIMIT |
| Q12 | 12.4 | 11350 | 11323 | 100% | 91% | SearchPhrase, COUNT(*) WHERE <> '' |
| Q16 | 21.6 | 19342 | 19314 | 100% | 90% | UserID+SearchPhrase, COUNT(*) ORDER BY |
| Q15 | 11.5 | 10248 | 10220 | 100% | 89% | UserID, COUNT(*) |
| Q18 | 35.3 | 30932 | 30901 | 100% | 88% | UserID+minute+SearchPhrase, COUNT(*) |
| Q41 | 0.7 | 598 | 590 | 99% | 88% | Filtered: CounterID=62, date range, URLHash match |
| Q33 | 25.1 | 21701 | 21673 | 100% | 86% | URL, COUNT(*) (full table) |
| Q34 | 25.1 | 21714 | 21686 | 100% | 86% | 1+URL, COUNT(*) (full table) |
| Q1 | 3.7 | 3142 | 3119 | 99% | 84% | COUNT(*) WHERE AdvEngineID <> 0 |
| Q38 | 0.6 | 510 | 502 | 98% | 84% | Filtered: CounterID=62, date+flags, URL GROUP BY |
| Q20 | 26.3 | 21725 | 21697 | 100% | 83% | COUNT(*) WHERE URL LIKE google |
| Q37 | 0.5 | 407 | 399 | 98% | 80% | Filtered: CounterID=62, date range, Title |
| Q36 | 0.6 | 502 | 494 | 98% | 79% | Filtered: CounterID=62, date range, URL |
| Q5 | 14.5 | 11347 | 11319 | 100% | 78% | COUNT(DISTINCT SearchPhrase) |
| Q30 | 35.7 | 27646 | 27615 | 100% | 77% | SearchEngineID+ClientIP, 3 aggs, WHERE SearchPhrase <> '' |
| Q31 | 46.8 | 35414 | 35382 | 100% | 76% | WatchID+ClientIP, 3 aggs, WHERE SearchPhrase <> '' |
| Q32 | 36.8 | 27937 | 27905 | 100% | 76% | WatchID+ClientIP, 3 aggs (full table) |
| Q13 | 26.2 | 19355 | 19326 | 100% | 74% | SearchPhrase, COUNT(DISTINCT UserID) |
| Q40 | 0.8 | 571 | 564 | 99% | 74% | Filtered: CounterID=62, TraficSourceID IN, RefererHash= |
| Q42 | 0.5 | 388 | 380 | 98% | 74% | Filtered: CounterID=62, narrow date, DATE_TRUNC agg |
| Q27 | 33.4 | 24003 | 23976 | 100% | 72% | CounterID, AVG(length(URL)), HAVING >100K |
| Q4 | 15.6 | 10187 | 10160 | 100% | 65% | COUNT(DISTINCT UserID) |
| Q28 | 33.1 | 20788 | 20761 | 100% | 63% | REGEXP_REPLACE(Referer), AVG(length), HAVING >100K |
| Q35 | 32.6 | 9310 | 9284 | 100% | 29% | ClientIP expressions, COUNT(*) (agg-heavy) |

#### DeltaXAgg — Sum/Count Pushdown (3 queries — no TOAST, metadata only)

| Query | Total (s) | heap_scan (ms) | Description |
|-------|-----------|----------------|-------------|
| Q2 | 0.1 | 101 | SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) |
| Q3 | 0.1 | 99 | AVG(UserID) |
| Q29 | 0.2 | 101 | 90 × SUM(ResolutionWidth + N) |

#### DeltaXCount / DeltaXMinMax (2 queries — metadata only)

| Query | Total (s) | heap_scan (ms) | Description |
|-------|-----------|----------------|-------------|
| Q0 | 0.1 | 82 | COUNT(*) |
| Q6 | 0.1 | 100 | MIN(EventDate), MAX(EventDate) |

#### DeltaXDecompress Path (6 queries)

| Query | Total (s) | heap_scan (ms) | Description |
|-------|-----------|----------------|-------------|
| Q19 | 8.6 | 8054 | WHERE UserID = specific value (point lookup) |
| Q23 | 0.9 | 604 | WHERE URL LIKE google ORDER BY EventTime LIMIT 10 |
| Q24 | 0.4 | 241 | WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10 |
| Q25 | 16.9 | 11335 | WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10 |
| Q26 | 0.3 | 239 | WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10 |
| Q39 | 1.8 | 922 | Filtered: CounterID=62, CASE expression, multi-GROUP BY |
