# Performance Improvements Roadmap

Tracking the gap between pg_seaturtle and TimescaleDB on ClickBench queries.
Current geometric mean: **pg_seaturtle 18.1ms vs TimescaleDB 17.7ms (0.98x, essentially on par)**.

## Current Benchmark (2026-03-06, sorted scan + arena alloc + text eq/ne pushdown)

| Query | Description | SeaTurtle (ms) | TSDB (ms) | Gap |
|-------|-------------|-------------|-----------|-----|
| Q1 | COUNT(*) | 0.7 | 3.3 | 0.2x |
| Q2 | COUNT WHERE AdvEngineID | 5.1 | 4.5 | 1.1x |
| Q3 | SUM/AVG full scan | 12.5 | 6.1 | 2.0x |
| Q5 | COUNT DISTINCT UserID | 20.8 | 102.8 | 0.2x |
| Q7 | MIN/MAX EventDate | 0.8 | 4.7 | 0.2x |
| Q8 | GROUP BY AdvEngineID | 5.0 | 4.8 | 1.0x |
| Q9 | GROUP BY RegionID | 72.9 | 204.6 | 0.4x |
| Q13 | Top SearchPhrase | 19.9 | 14.1 | 1.4x |
| Q20 | Point lookup UserID | 8.7 | 4.8 | 1.8x |
| Q21 | URL LIKE google | 66.2 | 34.7 | 1.9x |
| Q25 | ORDER BY EventTime | 10.9 | 2.2 | 5.0x |
| Q34 | Top URLs | 286.7 | 229.2 | 1.2x |
| Q37 | CounterID=62 URLs | 146.1 | 107.7 | 1.4x |
| Q38 | CounterID=62 Titles | 83.3 | 36.0 | 2.3x |
| Q43 | CounterID=62 by minute | 62.1 | 26.4 | 2.4x |

## Previous Benchmark (2026-03-05, LIKE filter pushdown + agg pushdown + batch quals)

| Query | Description | SeaTurtle (ms) | TSDB (ms) | Gap |
|-------|-------------|-------------|-----------|-----|
| Q1 | COUNT(*) | 0.5 | 3.3 | 0.2x |
| Q2 | COUNT WHERE AdvEngineID | 4.5 | 4.5 | 1.0x |
| Q3 | SUM/AVG full scan | 11.9 | 6.1 | 2.0x |
| Q5 | COUNT DISTINCT UserID | 20.5 | 102.8 | 0.2x |
| Q7 | MIN/MAX EventDate | 0.7 | 4.7 | 0.1x |
| Q8 | GROUP BY AdvEngineID | 5.2 | 4.8 | 1.1x |
| Q9 | GROUP BY RegionID | 71.8 | 204.6 | 0.4x |
| Q13 | Top SearchPhrase | 59.3 | 14.1 | 4.2x |
| Q20 | Point lookup UserID | 7.1 | 4.8 | 1.5x |
| Q21 | URL LIKE google | 65.2 | 34.7 | 1.9x |
| Q25 | ORDER BY EventTime | 64.0 | 2.2 | 29x |
| Q34 | Top URLs | 284.1 | 229.2 | 1.2x |
| Q37 | CounterID=62 URLs | 148.8 | 107.7 | 1.4x |
| Q38 | CounterID=62 Titles | 84.2 | 36.0 | 2.3x |
| Q43 | CounterID=62 by minute | 61.9 | 26.4 | 2.3x |

## Previous Benchmark (2026-03-02, segment pruning implemented)

| Query | Description | SeaTurtle (ms) | TSDB (ms) | Gap |
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

The seaturtle scan has four phases: **metadata** (SPI catalog lookup), **heap_scan**
(load compressed blobs from companion table), **decompress** (decode blobs to
datums), and **emit** (fill slot + qual + projection, row at a time).

For most queries, **emit dominates** because we return one row per
`ExecCustomScan` call, with per-row qual evaluation, projection, and memory
context switches. For text-heavy queries, **decompress** is significant due to
per-string varlena allocation.

---

## Improvements

### 1. COUNT(*) / COUNT pushdown [DONE]

**Impact: Q1 42ms -> 0.7ms (achieved)**
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

### 2. MIN/MAX pushdown for time column [DONE]

**Impact: Q7 65ms -> 0.7ms (achieved, generalized to all orderable columns)**
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

### 6. LIKE filter pushdown into decompression [DONE]

**Impact: Q21 196ms -> 65ms (achieved)**
**Complexity: Medium**

For text columns with LIKE/NOT LIKE quals, the LIKE match is now evaluated on
raw `&str` slices during decompression — before any PG varlena allocation.
Only matching rows get `str_to_text_datum()` calls; non-matching rows get a
dummy datum and are excluded via a pre-selection vector.

For dictionary-encoded columns, the LIKE pattern is matched against dictionary
entries only (e.g. a few thousand), then a per-row integer index lookup
determines pass/fail — no per-row string work at all.

For LZ4-encoded columns (high cardinality, like ClickBench URLs), the LIKE
match runs on zero-copy `&str` references from the decompressed buffer,
avoiding ~1M PG palloc calls per segment.

The pre-selection vector is passed into `evaluate_batch_quals` as the initial
selection, so the LIKE qual is never re-evaluated on dummy datums.

**Files:** `src/compression/dictionary.rs` (decode_dict_and_indices),
`src/scan/exec.rs` (decompress_text_blob_with_like_filter, decompression loops)

### 7. Store per-column min/max in companion table [DONE]

**Impact: Enables segment pruning on non-time, non-segment-by columns + MIN/MAX pushdown for any column**
**Complexity: Medium**

Currently segment pruning only works for segment_by columns (equality) and the
time column (range). Storing `_min_<col>` / `_max_<col>` for numeric columns
would enable zone-map style pruning for arbitrary WHERE clauses.

For example, Q2 (`WHERE AdvEngineID <> 0`) could skip segments where
`_max_AdvEngineID = 0` (all zeros). Q20 (`WHERE UserID = X`) could skip segments
where X is outside `[_min_UserID, _max_UserID]`.

**Files:** `src/compress.rs` (companion table schema, compression logic),
`src/scan/exec.rs` (load_segments_heap, segment pruning)

### 8. Sorted/ordered scan for ORDER BY time [DONE]

**Impact: Q25 64ms -> 10.9ms (achieved)**
**Complexity: High**

Segments are now sorted by `min_time` during execution, and SeaTurtleDecompress
paths advertise pathkeys matching the time column when the query has
`ORDER BY time_col ASC`. PG's planner sees the sorted children and creates a
MergeAppend → Incremental Sort → Limit plan, short-circuiting after just a
few rows from the first segment instead of decompressing everything.

Key details:
- `find_parent_oid()` resolves the parent hypertable for child partitions
  during planning to look up the time column.
- Pathkeys are set on individual SeaTurtleDecompress paths (not SeaTurtleAppend),
  so PG's `generate_orderedappend_paths` creates MergeAppend naturally.
- Only ASC ordering is advertised for now; DESC can be added later.
- PG18 compatibility: uses `pk_cmptype` instead of `pk_strategy`.

**Files:** `src/scan/hook.rs` (find_parent_oid, pathkey detection),
`src/scan/path.rs` (pathkeys param), `src/scan/exec.rs` (segment sorting)

### 9. Arena allocation for text varlena [DONE]

**Impact: Q25 15ms -> 10.9ms, general improvement on text-heavy queries**
**Complexity: Low**

Instead of N individual `palloc` calls (one per string via `cstring_to_text_with_len`),
all text varlena for a segment are packed into a single contiguous allocation.
This dramatically improves L2/L3 cache locality during the per-row emit loop,
where PG accesses each datum for slot filling and qual evaluation.

`str_slices_to_text_datums_arena()` calculates total size, does one `palloc`,
then packs MAXALIGN'd varlena headers + string data sequentially. Used by both
Dictionary and LZ4 decompression paths, and the LIKE/equality filter paths
(for matched rows only).

Falls back to per-string allocation for `bpchar` (needs type input function
for padding).

**Files:** `src/scan/exec.rs` (str_slices_to_text_datums_arena, decompress_blob_to_datums)

### 10. Text equality/inequality pushdown into decompression [DONE]

**Impact: Q13 59ms -> 19.9ms (3x faster)**
**Complexity: Medium**

For text columns with `=` or `<>` quals (e.g. `WHERE SearchPhrase <> ''`),
the comparison is pushed into decompression — evaluated on raw `&str` slices
before any PG varlena allocation. Only matching rows get datums allocated
(via arena); non-matching rows get dummy datums and are excluded via
pre-selection vector.

For dictionary-compressed columns, each dictionary entry is compared once
(O(dict_size), typically a few thousand), then per-row index lookups determine
pass/fail — no per-row string comparison at all. For Q13, this eliminates
~93% of rows (only 69K of 1M have non-empty SearchPhrase) at dictionary level.

The text constant is extracted during batch qual detection in `extract_batch_quals`
and stored as `BatchQual.text_const`. At decompression time,
`decompress_text_blob_with_eq_filter` handles both Dictionary and LZ4 paths.

**Files:** `src/scan/exec.rs` (decompress_text_blob_with_eq_filter, extract_batch_quals)

### 11. Bloom filters for text column pruning

**Impact: Q21 (URL LIKE) segment pruning**
**Complexity: High**

For text columns with moderate cardinality, store a per-segment bloom filter in
the companion table. This enables pruning segments that definitely don't contain
a given string, without decompressing.

Referenced in the design doc (`pg_seaturtle_design_v03.md`) as a future optimization.

**Files:** `src/compress.rs`, `src/scan/exec.rs`

---

## Suggested priority order

The items are roughly ordered by impact/effort ratio:

1. ~~**COUNT(*) pushdown**~~ [DONE]
2. ~~**MIN/MAX pushdown**~~ [DONE]
3. **Lazy blob detoasting** — improves all queries, especially pruned ones
4. **Vectorized qual / batch filtering** — biggest overall impact, covers Q2/Q8/Q20
5. **Lazy decompression with predicate pushdown** — amplifies #4
6. ~~**LIKE filter pushdown**~~ [DONE]
7. ~~**Per-column min/max**~~ [DONE]
8. ~~**Sorted scan for ORDER BY**~~ [DONE]
9. ~~**Arena allocation for text varlena**~~ [DONE]
10. ~~**Text equality/inequality pushdown**~~ [DONE]
11. **Bloom filters** — niche but powerful for text filtering
