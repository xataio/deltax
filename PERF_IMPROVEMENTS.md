# Performance Improvements Roadmap

Tracking SeaTurtle compressed vs uncompressed performance on ClickBench.

## Current Benchmark (2026-03-13)

### Compressed vs Uncompressed

| Query  | Description               |  Uncompr (ms) |  Compr (ms) |  Ratio |
|--------|---------------------------|---------------|-------------|--------|
| Q1     | COUNT(*)                  |          45.0 |         0.8 | 58.77x |
| Q2     | COUNT WHERE AdvEngineID   |          75.0 |         4.0 | 18.84x |
| Q3     | SUM/AVG full scan         |          84.9 |        11.2 |  7.60x |
| Q4     | AVG UserID                |          52.9 |         7.5 |  7.05x |
| Q5     | COUNT DISTINCT UserID     |         185.6 |         0.6 | 305.40x |
| Q6     | COUNT DISTINCT SearchPhrase |         357.3 |         0.5 | 755.71x |
| Q7     | MIN/MAX EventDate         |          51.5 |         0.9 | 56.21x |
| Q8     | GROUP BY AdvEngineID      |          72.9 |         4.9 | 14.77x |
| Q9     | GROUP BY RegionID         |         289.8 |        50.0 |  5.80x |
| Q10    | RegionID multi-agg        |         446.0 |        55.8 |  7.99x |
| Q11    | MobilePhoneModel users    |         200.0 |         9.2 | 21.73x |
| Q12    | MobilePhone+Model users   |         215.4 |        12.7 | 16.99x |
| Q13    | Top SearchPhrase          |         104.0 |        20.3 |  5.13x |
| Q14    | SearchPhrase users        |         284.8 |        25.3 | 11.24x |
| Q15    | SearchEngine+Phrase       |         216.9 |        23.3 |  9.29x |
| Q16    | Top UserID                |          84.6 |        45.8 |  1.85x |
| Q17    | UserID+SearchPhrase top   |         319.6 |        86.2 |  3.71x |
| Q18    | UserID+SearchPhrase       |         110.3 |        86.8 |  1.27x |
| Q19    | UserID+minute+Phrase      |         505.8 |       351.5 |  1.44x |
| Q20    | Point lookup UserID       |          70.4 |         1.5 | 46.31x |
| Q21    | URL LIKE google           |          90.6 |        59.6 |  1.52x |
| Q22    | SearchPhrase+URL google   |         120.5 |        63.8 |  1.89x |
| Q23    | Title LIKE Google         |         125.1 |       132.7 |  0.94x |
| Q24    | SELECT * google sorted    |          89.6 |       127.0 |  0.71x |
| Q25    | SearchPhrase by time      |          81.4 |        35.4 |  2.30x |
| Q26    | SearchPhrase sorted       |          81.4 |        12.6 |  6.46x |
| Q27    | SearchPhrase time+phrase  |          80.9 |        10.4 |  7.78x |
| Q28    | CounterID avg URL len     |         102.9 |        65.6 |  1.57x |
| Q29    | Referer domain regex      |         954.5 |      1191.2 |  0.80x |
| Q30    | Wide SUM 89 cols          |         204.3 |         4.8 | 42.56x |
| Q31    | SearchEngine+ClientIP     |         222.0 |        27.7 |  8.01x |
| Q32    | WatchID+ClientIP filter   |         291.9 |        59.6 |  4.90x |
| Q33    | WatchID+ClientIP all      |         625.7 |       452.8 |  1.38x |
| Q34    | Top URLs                  |        1194.7 |       326.2 |  3.66x |
| Q35    | Top URLs with const       |        1123.7 |       299.1 |  3.76x |
| Q36    | ClientIP arithmetic       |         110.3 |        66.8 |  1.65x |
| Q37    | CounterID=62 URLs         |        1785.5 |       145.0 | 12.32x |
| Q38    | CounterID=62 Titles       |         494.4 |        68.6 |  7.21x |
| Q39    | CounterID=62 links        |         143.8 |        28.8 |  4.99x |
| Q40    | CounterID=62 traffic src  |        2218.2 |       298.7 |  7.43x |
| Q41    | CounterID=62 URLHash      |         149.3 |        27.2 |  5.48x |
| Q42    | CounterID=62 window dim   |         144.2 |        21.9 |  6.59x |
| Q43    | CounterID=62 by minute    |         129.4 |        28.8 |  4.49x |
|--------|---------------------------|---------------|-------------|--------|
| GMEAN  | Geometric Mean            |         189.1 |        28.6 |  6.62x |

### SeaTurtle Scan Timing Breakdown (EXPLAIN ANALYZE)

| Query  | SeaTurtle Total |   Metadata |  Heap Scan |  Decompress | Batch Eval |       Emit | Stats                                                                                 |
|--------|---------------|------------|------------|-------------|------------|------------|---------------------------------------------------------------------------------------|
| Q1     |      0.443 ms |      0.325 |      0.118 |       0.000 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q2     |      3.777 ms |      0.224 |      0.422 |       1.938 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q3     |     11.226 ms |      0.222 |      1.413 |       3.921 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q4     |      7.271 ms |      0.282 |      1.637 |       2.571 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q5     |      0.326 ms |      0.326 |      0.000 |       0.000 |      0.000 |      0.000 | segments=0 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch_ |
| Q6     |      0.276 ms |      0.276 |      0.000 |       0.000 |      0.000 |      0.000 | segments=0 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch_ |
| Q7     |      0.645 ms |      0.249 |      0.396 |       0.000 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q8     |      4.289 ms |      0.272 |      0.337 |       1.921 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q9     |     48.058 ms |      0.239 |      2.465 |       4.441 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q10    |     55.608 ms |      0.290 |      3.489 |       8.316 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q11    |      8.426 ms |      0.301 |      1.640 |       4.771 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q12    |     11.321 ms |      0.258 |      1.909 |       7.256 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q13    |     15.015 ms |      0.261 |      1.376 |       4.467 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q14    |     20.773 ms |      0.290 |      2.853 |       7.038 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q15    |     19.056 ms |      0.314 |      1.982 |       6.540 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q16    |     39.226 ms |      0.321 |      1.508 |       2.610 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q17    |     65.450 ms |      0.347 |      2.760 |       8.156 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q18    |     63.456 ms |      0.316 |      2.775 |       8.158 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q19    |    213.628 ms |      0.304 |     10.454 |      26.667 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q20    |      1.408 ms |      0.327 |      0.434 |       0.508 |      0.139 |      0.000 | segments=6 segments_skipped=28 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q21    |     56.793 ms |      0.271 |      3.702 |      22.269 |      0.000 |      0.000 | segments=17 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q22    |     61.844 ms |      0.326 |      3.793 |      25.353 |      0.000 |      0.000 | segments=17 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q23    |    125.438 ms |      0.367 |      8.012 |      67.681 |      0.000 |      0.000 | segments=24 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q24    |     91.968 ms |      0.366 |     24.093 |      67.378 |      0.131 |      0.000 | segments=17 segments_skipped=17 phase2_skipped=0 rows_out=10 rows_filtered=0 rows_bat |
| Q25    |     26.974 ms |      0.341 |      8.727 |      17.906 |      0.000 |      0.000 | segments=28 segments_skipped=6 phase2_skipped=0 rows_out=10 rows_filtered=0 rows_batc |
| Q26    |      6.330 ms |      0.323 |      1.443 |       4.505 |      0.000 |      0.059 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=69354 rows_filtered=0 rows_b |
| Q27    |      9.694 ms |      0.321 |      8.642 |       0.730 |      0.000 |      0.001 | segments=1 segments_skipped=0 phase2_skipped=0 rows_out=11 rows_filtered=0 rows_batch |
| Q28    |     64.807 ms |      0.289 |      2.282 |      36.287 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q29    |   1070.818 ms |      0.536 |      3.479 |     755.975 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q30    |      4.227 ms |      0.312 |      1.007 |       1.890 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q31    |     22.504 ms |      0.246 |      4.257 |      12.492 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q32    |     36.777 ms |      0.311 |      5.080 |      20.642 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q33    |     21.422 ms |      0.349 |      3.267 |      17.238 |      0.000 |      0.568 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=1000000 rows_filtered=0 rows |
| Q34    |     32.899 ms |      0.349 |      2.993 |      28.088 |      0.000 |      1.469 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=1000000 rows_filtered=0 rows |
| Q35    |     30.933 ms |      0.340 |      2.469 |      27.430 |      0.000 |      0.694 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=1000000 rows_filtered=0 rows |
| Q36    |     85.609 ms |      1.325 |      2.412 |       6.351 |      0.000 |      0.000 | segments=34 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q37    |     23.929 ms |      0.340 |      1.866 |      19.490 |      1.799 |      0.434 | segments=15 segments_skipped=19 phase2_skipped=0 rows_out=376899 rows_filtered=0 rows |
| Q38    |     16.522 ms |      0.329 |      0.787 |      10.345 |      1.917 |      3.144 | segments=15 segments_skipped=19 phase2_skipped=0 rows_out=370550 rows_filtered=0 rows |
| Q39    |     21.504 ms |      0.350 |      2.406 |      16.875 |      1.825 |      0.048 | segments=15 segments_skipped=19 phase2_skipped=0 rows_out=26918 rows_filtered=0 rows_ |
| Q40    |     44.354 ms |      0.330 |      4.603 |      36.611 |      1.422 |      1.388 | segments=15 segments_skipped=19 phase2_skipped=0 rows_out=406063 rows_filtered=0 rows |
| Q41    |     25.646 ms |      0.317 |      6.004 |      11.291 |      0.000 |      0.000 | segments=15 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q42    |     18.759 ms |      0.341 |      3.843 |       9.031 |      0.000 |      0.000 | segments=15 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |
| Q43    |     31.035 ms |      0.704 |      5.311 |      10.335 |      0.000 |      0.000 | segments=15 segments_skipped=0 phase2_skipped=0 rows_out=0 rows_filtered=0 rows_batch |

## Where the time goes

The SeaTurtle scan has five phases: **metadata** (SPI catalog lookup), **heap_scan**
(load compressed blobs from companion table), **decompress** (decode blobs to
datums), **batch_eval** (vectorized WHERE on decoded arrays), and **emit** (fill
slot + qual + projection, row at a time).

For queries emitting many rows, **decompress + emit dominate** roughly equally.
Decompress is dominated by text varlena allocation (even with arena). Emit is
dominated by PG executor overhead: `fill_slot` + `ExecQual` + `ExecProject` per
row, plus memory context switches.

For queries where the bottleneck is *above* the scan (PG executor evaluating
complex expressions, hash aggregation on high-cardinality keys), the scan itself
is fast but we pay the cost of emitting 1M rows through the custom scan interface
just to feed PG's tuple-at-a-time executor.

---

## Completed Improvements

### 1. COUNT(*) / COUNT pushdown [DONE]

**Impact: Q1 42ms -> 0.5ms**

Sum `_row_count` from segment metadata. Zero decompression. Detected in planner
hook; `SeaTurtleCount` node returns a single row.

### 2. MIN/MAX pushdown [DONE]

**Impact: Q7 65ms -> 0.6ms (generalized to all orderable columns)**

Scan per-column `_min_`/`_max_` metadata in companion table. `SeaTurtleMinMax`
node returns global min/max without decompressing.

### 3. Batch qual evaluation [DONE]

**Impact: Q2 76ms -> 5.2ms, Q8 114ms -> 4.7ms, Q20 67ms -> 7.1ms**

Evaluate simple quals (`=`, `<>`, `<`, `>`, `>=`, `<=`) in tight Rust loops over
decoded datum arrays. Build a `Vec<bool>` selection vector; only `fill_slot` for
passing rows. LLVM auto-vectorizes the `slice.position()` scan.

### 4. LIKE filter pushdown into decompression [DONE]

**Impact: Q21 196ms -> 64ms**

LIKE match evaluated on raw `&str` slices during decompression. For dictionary
columns, pattern matched against dictionary entries only (O(dict_size)). For LZ4
columns, zero-copy match on decompressed buffer.

### 5. Text equality/inequality pushdown [DONE]

**Impact: Q13 59ms -> 18ms (3x)**

`=`/`<>` on text columns evaluated on raw `&str` slices before varlena
allocation. Dictionary columns: one comparison per entry, index lookup per row.

### 6. Per-column min/max in companion table [DONE]

**Impact: Enables segment pruning + MIN/MAX pushdown for any column**

Zone-map style `_min_`/`_max_` for all numeric columns. Enables skipping segments
for arbitrary WHERE clauses.

### 7. Sorted scan for ORDER BY time [DONE]

**Impact: Q25 64ms -> 24ms**

Segments sorted by `min_time`; SeaTurtleDecompress paths advertise pathkeys.
PG creates MergeAppend + Incremental Sort + Limit plans.

### 8. Arena allocation for text varlena [DONE]

**Impact: General improvement on text-heavy queries**

All text varlena for a segment packed into one contiguous `palloc`. Improves
cache locality during emit.

### 9. Lazy blob detoasting [DONE]

**Impact: Q37/Q38 heap_scan 16ms -> 2ms**

Segment-by values and min/max metadata extracted first (cheap). Pruning applied.
BYTEA blobs detoasted only for surviving segments.

### 10. Aggregate pushdown (SUM/AVG/COUNT/COUNT DISTINCT) [DONE]

**Impact: Q3 11ms, Q5 20ms, Q8 4.7ms**

`SeaTurtleAgg` node computes aggregates directly on decompressed columns. Handles
`SUM`, `AVG`, `COUNT`, `COUNT(DISTINCT)`, `GROUP BY` on segment_by columns.

### 11. Lazy column decompression (two-phase decompress) [DONE]

**Impact: Q24 756ms -> improved, Q22/Q23 improved**

Split decompression into two phases. Phase 1 decompresses only filter columns
(referenced in WHERE), applies batch quals, and builds a selection vector.
Phase 2 decompresses remaining columns, skipping text varlena allocation for
rows that don't pass the filter. When no rows survive Phase 1, Phase 2 is
skipped entirely (`phase2_skipped` counter in EXPLAIN ANALYZE).

For Top-N queries, Phase 2 columns are marked as lazy for TOAST detoasting —
only segments that contribute to the top-N result set have their deferred
columns materialized.

### 12. Expression aggregate pushdown — SUM(col + const) [DONE]

**Impact: Q30 425ms -> improved**

Detect `SUM(col + const)` pattern (`AggExpr::AddConst`) in planner hook.
SeaTurtleAgg computes all sums in a single pass over the decoded column,
applying the constant offset algebraically: `result = base_sum + const * count`.
When all agg specs reference the same column, the column is decoded once and
all results derived from a single accumulator.

### 13. String function pushdown — length() [DONE]

**Impact: Q28 207ms -> improved**

`AggExpr::LengthOf` variant computes string length on raw `&str` slices during
decompression without varlena allocation. Combined with aggregate pushdown,
`AVG(length(URL))` is computed entirely inside SeaTurtleAgg — zero text
materialization.

### 14. Regex pushdown via Rust regex crate [DONE]

**Impact: Q29 2837ms -> improved**

`GroupByExpr::RegexpReplace` detected in planner when GROUP BY contains
`regexp_replace(col, const_pattern, const_replacement)`. At scan time, the
Rust `regex` crate compiles the pattern once and applies it on raw `&str`
slices from LZ4/dictionary decompression. A cross-segment regex result cache
(`HashMap<String, String>`) avoids redundant regex calls for repeated input
values — tracked via `regex_cache_size` and `regex_cache_calls` in EXPLAIN.

### 15. IN list batch quals [DONE]

**Impact: Faster filtering for `col IN (v1, v2, ...)` predicates**

`BatchCompareOp::InList` evaluates IN-list predicates in vectorized Rust loops
over decoded datum arrays. The constant values are stored as `Vec<i64>` and
checked per-row. Also integrates with min/max segment pruning — segments whose
min/max range doesn't overlap any IN-list value are skipped entirely.

### 16. GROUP BY expression pushdown [DONE]

**Impact: Queries with date_trunc/extract/regexp_replace in GROUP BY**

SeaTurtleAgg handles GROUP BY on expressions, not just plain columns:

- **`date_trunc(unit, col)`** — truncation computed on epoch microseconds
  using pure arithmetic (`date_trunc_unit_to_usecs`). Supports second, minute,
  hour, day, week, month, year.
- **`extract(field FROM col)`** — field extraction from epoch microseconds
  (`extract_field_from_usecs`). Supports microsecond through epoch.
- **`regexp_replace(col, pattern, replacement)`** — regex applied on raw
  `&str` slices via Rust `regex` crate (see #14).

All three are serialized to `custom_private` and round-trip through plan
caching.

### 17. HAVING filter pushdown [DONE]

**Impact: Eliminates post-aggregation filtering in PG executor**

Simple HAVING clauses of the form `HAVING agg_result <op> const` (where `<op>`
is `>`, `<`, `>=`, `<=`, `=`, `<>`) are pushed into SeaTurtleAgg. Filters are
applied immediately after aggregation, before result rows are emitted. Encoded
as `HavingFilter { agg_idx, op, const_val }` in `custom_private`.

### 18. Min/max segment pruning [DONE]

**Impact: Skips segments whose value ranges don't match WHERE predicates**

Per-segment `_min_`/`_max_` metadata for all orderable types (INT2/INT4/INT8,
FLOAT4/FLOAT8, TIMESTAMP/TIMESTAMPTZ, DATE) is checked before decompression.
Segments that can't contain matching rows are skipped entirely. Supports `=`,
`<`, `<=`, `>`, `>=`, and `IN` list predicates. Tracked via
`segments_minmax_skipped` in EXPLAIN ANALYZE.

### 19. Dictionary-based segment pruning for LIKE [DONE]

**Impact: Skips segments where no dictionary entry matches the LIKE pattern**

For dictionary-compressed text columns, the dictionary (small, at the start of
the blob) is loaded and tested against the LIKE/NOT LIKE pattern before
decompressing indices. If no dictionary entry matches, the entire segment is
skipped. Implemented in `segment_skippable_by_dict_like()`.

### 20. Top-N pushdown for DecompressState [DONE]

**Impact: ORDER BY col LIMIT N on compressed scans**

When `ORDER BY col LIMIT N` is detected, DecompressState maintains a bounded
heap of top-N candidates during Phase 1. Segments are processed in min/max
order; once enough candidates are collected and a segment's min (or max for
DESC) can't beat the current worst candidate, remaining segments are skipped.
Phase 2 decompression is deferred and only performed for winning segments.
Pathkeys are advertised so PG eliminates the Sort node.

### 21. Top-N pushdown for AggScan [DONE]

**Impact: GROUP BY col ORDER BY agg(...) LIMIT N on aggregate queries**

When `ORDER BY <aggregate> [ASC|DESC] LIMIT N` is detected on a SeaTurtleAgg
query, the aggregation result is sorted by the specified aggregate column and
truncated to N rows inside the scan node. Pathkeys are set on the CustomPath
so PG eliminates the redundant Sort node above SeaTurtleAgg. EXPLAIN ANALYZE
shows `TopN: limit=N sort_col=X direction=ASC|DESC pre_topn_groups=M`.

### 22. Dictionary compression for text columns [DONE]

**Impact: Better compression ratio and faster decompression for low-cardinality text**

Text columns with `ndistinct < 10% of row_count AND < 65536 distinct values`
use dictionary encoding: fixed-width indices into a deduplicated string table.
Falls back to LZ4 for high-cardinality columns. Dictionary entries also serve
as a perfect filter for LIKE pruning (see #19).

### 23. Ndistinct statistics tracking [DONE]

**Impact: Enables cardinality-aware compression strategy selection**

Per-column `ndistinct` counts maintained in the catalog during compression.
Used to switch between dictionary encoding (low cardinality) and LZ4 (high
cardinality) for text columns. Also available via `get_column_ndistinct()`
for cost estimation.

### 26. Batch LIKE eval + ExecQual removal [DONE]

**Impact: Q23 0.94x → 1.10x (regression fixed), Q38 68.6ms → 59.4ms (-13%),
Q37 145ms → 131ms (-9%), Q36 143ms → 131ms (-8%)**

Three changes that eliminate redundant per-row overhead:

1. **ExecQual removal:** When all plan quals are successfully extracted as
   batch quals, `ps.qual` is set to NULL at BeginCustomScan time, skipping
   PG's per-row `ExecQual` in the emit loop. `extract_batch_quals` now
   returns a `handled_count` to verify full coverage before nulling.
2. **Skip redundant text eval:** `evaluate_batch_quals` no longer re-evaluates
   text LIKE/NotLike and Eq/Ne quals that were already applied during Phase 1
   decompression (`decompress_text_blob_with_like_filter`).
3. **SIMD Contains search:** For `LIKE '%needle%'` on LZ4 text columns,
   `memchr::memmem::Finder` scans the raw decompressed buffer in a single
   SIMD-accelerated pass instead of per-string `str::contains`. Cross-boundary
   safety: validates the full needle fits within a single string's byte range.

### 27. Expression GROUP BY pushdown (col +/- const) [DONE]

**Impact: Q36 143ms -> 67ms (fixes 0.69x regression -> 1.65x)**

`GroupByExpr::AddConst { offset, op_oid }` detects `col + const` / `col - const`
in GROUP BY expressions during the planner hook. Both `+` and `-` operators are
supported; for `-`, the constant is negated so the offset is always stored as
addition. At execution time, the group key is computed as `col_value + offset`.

For Q36's `GROUP BY ClientIP, ClientIP-1, ClientIP-2, ClientIP-3`, all four keys
are pushed into SeaTurtleAgg as a 4-element key vector. The scan processes 1M
rows and emits only 10 (via TopN pushdown), eliminating the PG hash agg that
previously dominated at 143ms.

---

## Regression Queries (Compressed Slower Than Uncompressed)

Several queries were slower with compression. Many have been addressed:

### Fixed regressions

**Q24 (was 0.13x):** Fixed by lazy column decompression (#11). Phase 2
skips text varlena allocation for non-matching rows.

**Q30 (was 0.48x):** Fixed by expression aggregate pushdown (#12). `SUM(col + N)`
computed algebraically inside SeaTurtleAgg.

**Q28 (was 0.57x):** Fixed by length() pushdown (#13). `AVG(length(URL))`
computed on raw `&str` slices without varlena allocation.

**Q29 (was 0.37x):** Fixed by regex pushdown (#14). `REGEXP_REPLACE` in GROUP BY
runs via Rust `regex` crate on raw slices with cross-segment caching.

**Q23 (was 0.94x):** Fixed by ExecQual removal (#26). Eliminating redundant
per-row PG qual evaluation brought ratio to 1.10x.

**Q36 (was 0.69x):** Fixed by expression GROUP BY pushdown (#27). `col +/- const`
in GROUP BY pushed into AggScan, eliminating 1M-row emit to PG hash agg.

### Remaining regressions

**Q24 (0.71x):** `SELECT * WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10`.
Decompresses all columns for all matching segments. Decompress=67.4ms,
heap_scan=24ms. See planned #26 and #29.

**Q29 (0.80x):** `REGEXP_REPLACE(Referer, ...) GROUP BY`. Decompress=756ms on
Referer (high-cardinality LZ4). The regex runs in Rust but decompression of
the full Referer column dominates. (#24 evaluated and deemed not worth implementing.)

**Q33 (1.38x):** `GROUP BY WatchID, ClientIP` — high-cardinality hash agg.
SeaTurtle scan=21ms, but PG hash agg on 1M rows with ~1M groups dominates.
Would require pushing hash agg into scan — very high effort.

**Q18 (1.27x):** `GROUP BY UserID, SearchPhrase`. Same pattern as Q33:
high-cardinality keys, emit overhead for 1M rows.

---

## Planned Improvements

### ~~24. Late text materialization~~ — Won't implement

**Status: Won't implement — insufficient benefit**

Phase 2 already only materializes varlena for selected rows via
`decompress_text_blob_with_selection`. The text-heavy benchmark queries
(Q34, Q35, Q38) all have `all_quals_batch_handled == true`, meaning every
selected row is emitted — late materialization would save zero work. For
queries with remaining PG quals, the filtered columns are typically
numeric/timestamp, not text. The per-row palloc tradeoff (losing arena
allocation) would partially offset any gain in the narrow case where it helps.

### 25. Bloom filters for text column segment pruning

**Target: Q21 64ms -> ~30ms, Q22/Q23 moderate improvement**
**Complexity: High**

Store a per-segment bloom filter in the companion table for text columns with
moderate cardinality. During segment loading, test the bloom filter against
WHERE constants to skip segments that definitely don't contain the value.

Dictionary-based pruning (#19) already handles dictionary-compressed columns.
Bloom filters would extend pruning to LZ4-compressed (high-cardinality) text
columns where the dictionary approach doesn't apply.

**Files:** `src/compress.rs` (bloom filter in companion table schema),
`src/scan/exec.rs` (bloom filter test in segment loading)

### 28. Text GROUP BY in AggScan

**Target: Q34 326ms -> ~40ms, Q35 299ms -> ~40ms**
**Complexity: Medium-High**

Q34/Q35 (`GROUP BY URL ORDER BY COUNT(*) DESC LIMIT 10`) emit **1M rows** into
PG's hash aggregator. The SeaTurtle scan takes only ~33ms, but PG hash agg on
1M text rows with high cardinality is ~290ms.

Currently AggScan doesn't support text/varchar GROUP BY keys, so these queries
fall back to DecompressState which emits all rows.

**Approach:** Extend AggScan's hash table to support string GROUP BY keys.
For dictionary-compressed columns, hash on dictionary index (u16) and only
materialize the string when emitting result rows. For LZ4 columns, hash on
raw `&str` slices during decompression, store references into the decompressed
buffer. Combined with Top-N pushdown (#21), the hash table can be pruned
during aggregation — only keeping entries that could make it into the top N.

**Note:** Late materialization (#24) was evaluated and deemed not worth
implementing, so this optimization should use standard varlena allocation.

**Files:** `src/scan/hook.rs` (detect text GROUP BY),
`src/scan/exec.rs` (extend `AggState` hash table for string keys)

### 29. Partial decompression for SELECT * with LIMIT

**Target: Q24 127ms -> ~10ms**
**Complexity: Medium**

`SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10`
currently decompresses all columns for all matching segments. With time-ordered
segments, an early-termination strategy is possible:

1. Process segments in time order (already supported via sorted scan #7)
2. Apply LIKE filter in Phase 1; track number of matching rows found so far
3. After accumulating enough candidates (LIMIT + safety margin), skip
   remaining segments
4. Only decompress non-filter columns (Phase 2) for the final winning rows

This is essentially combining sorted scan (#7), batch LIKE (#26), and lazy
column decompression (#11) into a LIMIT-aware pipeline where only the rows
that actually appear in the final result set need full materialization.

**Files:** `src/scan/exec.rs` (early termination in segment loop,
deferred Phase 2 for LIMIT queries)

### 30. High-cardinality integer GROUP BY optimization

**Target: Q16 45.8ms -> ~30ms, Q19 351.5ms -> ~200ms**
**Complexity: Medium**

Q16 (`GROUP BY UserID`) and Q19 (`GROUP BY UserID, minute, SearchPhrase`)
already use AggScan (rows_out=0) but are slow due to hash table pressure
with millions of distinct UserID values.

**Approach:**
- Pre-size hash maps using per-segment `ndistinct` metadata (already tracked
  in catalog via #23) to avoid repeated resizing
- For `ORDER BY agg LIMIT N` queries, use a top-N heap with early pruning:
  once a segment's contribution can't change the top-N result, skip aggregation
  for remaining groups
- Consider a two-pass strategy: first pass counts groups per segment to estimate
  total cardinality, second pass allocates accordingly

**Files:** `src/scan/exec.rs` (hash map sizing, top-N pruning in AggState)

### 31. WHERE + AggScan combined batch evaluation

**Target: Q31 27.7ms -> ~15ms, Q32 59.6ms -> ~30ms**
**Complexity: Medium**

Q31/Q32 have `WHERE SearchPhrase <> ''` combined with GROUP BY aggregation.
Currently the filter and aggregation run in separate passes through the
decoded data. Combining batch qual evaluation with aggregate accumulation in
a single pass would improve cache locality and avoid redundant iteration.

For dictionary columns, the `<> ''` filter can leverage `empty_string_idx`
to skip rows by checking the 1-2 byte index array without decompressing any
string data. Make sure `check_ne_empty()` is wired into the batch eval path
inside AggScan, not just DecompressState.

**Files:** `src/scan/exec.rs` (fused filter+aggregate loop in AggState)
