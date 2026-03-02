# Performance Improvements Roadmap

Tracking the gap between pg_cocoon and TimescaleDB on ClickBench queries.
Current geometric mean: **pg_cocoon 127ms vs TimescaleDB 18ms (~7x gap)**.

## Current Benchmark (2024-03-02, segment pruning implemented)

| Query | Description | Cocoon (ms) | TSDB (ms) | Gap |
|-------|-------------|-------------|-----------|-----|
| Q1 | COUNT(*) | 42.5 | 3.3 | 13x |
| Q2 | COUNT WHERE AdvEngineID | 76.2 | 4.5 | 17x |
| Q3 | SUM/AVG full scan | 102.3 | 6.1 | 17x |
| Q5 | COUNT DISTINCT UserID | 139.1 | 102.8 | 1.4x |
| Q7 | MIN/MAX EventDate | 64.9 | 4.7 | 14x |
| Q8 | GROUP BY AdvEngineID | 114.4 | 4.8 | 24x |
| Q9 | GROUP BY RegionID | 245.0 | 204.6 | 1.2x |
| Q13 | Top SearchPhrase | 139.9 | 14.1 | 10x |
| Q20 | Point lookup UserID | 67.0 | 4.8 | 14x |
| Q21 | URL LIKE google | 177.7 | 34.7 | 5x |
| Q25 | ORDER BY EventTime | 143.8 | 2.2 | 65x |
| Q34 | Top URLs | 287.7 | 229.2 | 1.3x |
| Q37 | CounterID=62 URLs | 236.8 | 107.7 | 2.2x |
| Q38 | CounterID=62 Titles | 168.9 | 36.0 | 4.7x |
| Q43 | CounterID=62 by minute | 151.3 | 26.4 | 5.7x |

## Where the time goes (EXPLAIN ANALYZE breakdown)

The cocoon scan has four phases: **metadata** (SPI catalog lookup), **heap_scan**
(load compressed blobs from companion table), **decompress** (decode blobs to
datums), and **emit** (fill slot + qual + projection, row at a time).

For most queries, **emit dominates** because we return one row per
`ExecCustomScan` call, with per-row qual evaluation, projection, and memory
context switches. For text-heavy queries, **decompress** is significant due to
per-string varlena allocation.

---

## Improvements

### 1. COUNT(*) / COUNT pushdown

**Impact: Q1 42ms -> <1ms, Q2/Q8 partial benefit**
**Complexity: Medium**

For `COUNT(*)` with no WHERE clause, return the sum of `_row_count` from segment
metadata without decompressing anything. The data is already loaded in
`SegmentData.row_count`.

Detect the pattern in the planner hook (`src/scan/hook.rs`): if the only node
above the scan is an Aggregate with a single COUNT(*) target, mark the custom
scan to use the fast path. In `exec_custom_scan`, return a single row with the
pre-computed count instead of iterating all rows.

For `COUNT(*)` with a simple WHERE on segment_by (e.g. `WHERE CounterID = 62`),
combine segment pruning with row_count summation — still zero decompression.

**Files:** `src/scan/hook.rs`, `src/scan/exec.rs` (exec_custom_scan), `src/scan/plan.rs`

### 2. MIN/MAX pushdown for time column

**Impact: Q7 65ms -> <1ms**
**Complexity: Low**

The companion table already stores `_min_{time_column}` and `_max_{time_column}`
per segment, and we already load them into `SegmentData.min_time`/`max_time`.

For `MIN(time_col)` / `MAX(time_col)` queries, scan the segment metadata to find
the global min/max without decompressing any data. Similar planner detection as
COUNT(*) pushdown.

**Files:** `src/scan/hook.rs`, `src/scan/exec.rs`

### 3. Vectorized qual evaluation (batch filtering)

**Impact: Q2 76ms -> ~15ms, Q8 114ms -> ~15ms, Q20 67ms -> ~5ms**
**Complexity: High**

Currently `exec_custom_scan` returns one row at a time, calling `fill_slot` +
`ExecQual` + `ExecProject` per row. For 1M rows, that's 1M function-call
round-trips through the PG executor.

Instead, after decompressing a segment, evaluate simple quals (=, <>, <, >, etc.)
directly on the decompressed datum arrays in a tight loop. Build a selection
vector (bitmap of passing rows). Then only fill slots for passing rows.

This avoids per-row memory context switches and the overhead of PG's expression
evaluation machinery for simple comparisons. The qual would still be applied by
PG for correctness on complex expressions — the batch filter is an optimization
that skips obviously non-matching rows early.

**Files:** `src/scan/exec.rs` (exec_custom_scan, new batch_eval_qual function)

### 4. Lazy decompression with predicate pushdown

**Impact: Q20 67ms -> ~5ms, Q2/Q8 moderate improvement**
**Complexity: Medium-High**

Currently all needed columns are decompressed for all rows in a segment before
any filtering. For Q20 (point lookup, 0 rows match), we decompress all columns
for 1M rows then discard everything.

Instead: decompress only the filter column(s) first, evaluate the predicate,
build a selection vector, then decompress remaining columns only for matching
rows. For queries where <1% of rows match (Q20, Q2), this skips most
decompression work.

**Files:** `src/scan/exec.rs` (segment decompression loop)

### 5. Lazy blob detoasting in load_segments_heap

**Impact: Q37/Q38 heap_scan 16ms -> ~3ms**
**Complexity: Medium**

`load_segments_heap` currently calls `pg_detoast_datum` + `to_vec()` for every
compressed blob of every segment during the initial heap scan. For Q37/Q38 with
segment pruning, 10 of 12 segments are later skipped — but their blobs were
already detoasted and copied.

Options:
- **Two-pass approach**: first pass extracts only segment_by values and min/max
  metadata (cheap, no detoasting). Apply pruning. Second pass detoasts only
  surviving segments.
- **Lazy detoasting**: store raw `Datum` pointers and defer detoasting to
  decompression time. Requires keeping the heap relation open longer.

**Files:** `src/scan/exec.rs` (load_segments_heap)

### 6. Reduce per-string varlena allocation

**Impact: Q21 178ms -> ~80ms, Q34/Q37/Q38 moderate improvement**
**Complexity: Medium**

For text columns (Dictionary/LZ4 codecs), each decoded string gets an individual
`cstring_to_text_with_len` palloc in the segment memory context. For a segment
with 83K rows and multiple text columns, that's hundreds of thousands of small
allocations.

Options:
- **Arena allocation**: allocate one large buffer per segment, pack varlena
  headers + string data sequentially. Dramatically reduces allocator overhead.
- **Deferred materialization**: keep strings as `(offset, len)` references into
  the decompressed buffer. Only create varlena datums for rows that pass the
  qual filter.

**Files:** `src/scan/exec.rs` (str_to_text_datum, decompress_blob_to_datums)

### 7. Store per-column min/max in companion table

**Impact: Enables segment pruning on non-time, non-segment-by columns**
**Complexity: Medium**

Currently segment pruning only works for segment_by columns (equality) and the
time column (range). Storing `_min_<col>` / `_max_<col>` for numeric columns
would enable zone-map style pruning for arbitrary WHERE clauses.

For example, Q2 (`WHERE AdvEngineID <> 0`) could skip segments where
`_max_AdvEngineID = 0` (all zeros). Q20 (`WHERE UserID = X`) could skip segments
where X is outside `[_min_UserID, _max_UserID]`.

**Files:** `src/compress.rs` (companion table schema, compression logic),
`src/scan/exec.rs` (load_segments_heap, segment pruning)

### 8. Sorted/ordered scan for ORDER BY time

**Impact: Q25 144ms -> ~10ms**
**Complexity: High**

Q25 has `ORDER BY EventTime LIMIT 10`. Currently we decompress all segments,
emit all rows, and let PG sort. Since segments have `_min_time`/`_max_time`, we
know their time ordering. If segments are scanned in time order and the time
column is sorted within each segment (which it is — it's the partition key), we
can emit rows in order and stop after LIMIT rows.

This requires the planner to detect ORDER BY + LIMIT patterns and produce a
pathkey-aware custom path so PG knows the output is pre-sorted.

**Files:** `src/scan/hook.rs`, `src/scan/plan.rs`, `src/scan/exec.rs`

### 9. Bloom filters for text column pruning

**Impact: Q21 (URL LIKE) segment pruning**
**Complexity: High**

For text columns with moderate cardinality, store a per-segment bloom filter in
the companion table. This enables pruning segments that definitely don't contain
a given string, without decompressing.

Referenced in the design doc (`pg_cocoon_design_v03.md`) as a future optimization.

**Files:** `src/compress.rs`, `src/scan/exec.rs`

---

## Suggested priority order

The items are roughly ordered by impact/effort ratio:

1. **COUNT(*) pushdown** — huge impact on Q1, low-hanging fruit
2. **MIN/MAX pushdown** — same pattern, covers Q7
3. **Lazy blob detoasting** — improves all queries, especially pruned ones
4. **Vectorized qual / batch filtering** — biggest overall impact, covers Q2/Q8/Q20
5. **Lazy decompression with predicate pushdown** — amplifies #4
6. **Reduce varlena allocation** — helps text-heavy queries Q21/Q34/Q37/Q38
7. **Per-column min/max** — enables pruning for more query patterns
8. **Sorted scan for ORDER BY** — dramatic impact on Q25, complex planner work
9. **Bloom filters** — niche but powerful for text filtering
