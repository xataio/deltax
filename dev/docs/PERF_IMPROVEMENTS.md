# Performance Improvements Roadmap

Tracking DeltaX compressed vs uncompressed performance on ClickBench.

## Current Benchmark (2026-03-30)

### Compressed vs Uncompressed (local, 10M rows)

| Query  | Description               |  Uncompr (ms) |  Compr (ms) |  Ratio |
|--------|---------------------------|---------------|-------------|--------|
| Q0     | COUNT(*)                  |          64.4 |         1.0 | 62.19x |
| Q1     | COUNT WHERE AdvEngineID   |          97.4 |         4.2 | 22.94x |
| Q2     | SUM/AVG full scan         |          96.6 |         0.8 | 125.75x |
| Q3     | AVG UserID                |          68.6 |         0.8 | 89.83x |
| Q4     | COUNT DISTINCT UserID     |         243.7 |        16.0 | 15.23x |
| Q5     | COUNT DISTINCT SearchPhrase |         410.8 |         6.6 | 62.45x |
| Q6     | MIN/MAX EventDate         |          69.2 |         0.9 | 72.98x |
| Q7     | GROUP BY AdvEngineID      |          89.0 |         1.9 | 46.92x |
| Q8     | GROUP BY RegionID         |         335.7 |         8.3 | 40.24x |
| Q9     | RegionID multi-agg        |         442.2 |        10.3 | 42.73x |
| Q10    | MobilePhoneModel users    |         241.7 |         3.9 | 61.74x |
| Q11    | MobilePhone+Model users   |         251.9 |         4.7 | 53.63x |
| Q12    | Top SearchPhrase          |         113.4 |         4.7 | 24.33x |
| Q13    | SearchPhrase users        |         331.8 |        10.5 | 31.62x |
| Q14    | SearchEngine+Phrase       |         264.2 |         5.5 | 47.91x |
| Q15    | Top UserID                |         106.7 |         5.8 | 18.51x |
| Q16    | UserID+SearchPhrase top   |         368.4 |         9.2 | 39.89x |
| Q17    | UserID+SearchPhrase       |         128.0 |         7.6 | 16.91x |
| Q18    | UserID+minute+Phrase      |         576.1 |        74.9 |  7.69x |
| Q19    | Point lookup UserID       |          66.9 |         0.9 | 71.44x |
| Q20    | URL LIKE google           |         102.2 |        59.0 |  1.73x |
| Q21    | SearchPhrase+URL google   |         122.0 |        18.4 |  6.63x |
| Q22    | Title LIKE Google         |         138.6 |        31.8 |  4.36x |
| Q23    | SELECT * google sorted    |         101.4 |       144.5 |  0.70x |
| Q24    | SearchPhrase by time      |          92.2 |        36.3 |  2.54x |
| Q25    | SearchPhrase sorted       |          91.1 |        14.4 |  6.32x |
| Q26    | SearchPhrase time+phrase  |          92.9 |        36.0 |  2.58x |
| Q27    | CounterID avg URL len     |         117.1 |        69.5 |  1.69x |
| Q28    | Referer domain regex      |         957.7 |       118.0 |  8.12x |
| Q29    | Wide SUM 89 cols          |         216.5 |         1.7 | 128.46x |
| Q30    | SearchEngine+ClientIP     |         294.5 |        22.3 | 13.18x |
| Q31    | WatchID+ClientIP filter   |         278.1 |        35.3 |  7.88x |
| Q32    | WatchID+ClientIP all      |         759.6 |        44.4 | 17.10x |
| Q33    | Top URLs                  |        1247.5 |        21.7 | 57.58x |
| Q34    | Top URLs with const       |        1118.7 |        22.2 | 50.33x |
| Q35    | ClientIP arithmetic       |         110.6 |        35.4 |  3.13x |
| Q36    | CounterID=62 URLs         |        1894.0 |        20.7 | 91.43x |
| Q37    | CounterID=62 Titles       |         524.0 |        19.9 | 26.38x |
| Q38    | CounterID=62 links        |         155.5 |        13.2 | 11.74x |
| Q39    | CounterID=62 traffic src  |        2814.9 |       314.5 |  8.95x |
| Q40    | CounterID=62 URLHash      |         162.9 |        42.3 |  3.85x |
| Q41    | CounterID=62 window dim   |         158.1 |        11.8 | 13.39x |
| Q42    | CounterID=62 by minute    |         147.0 |        15.0 |  9.80x |
|--------|---------------------------|---------------|-------------|--------|
| GMEAN  | Geometric Mean            |         215.9 |        12.2 | 17.64x |

### ClickBench full dataset (c6a.4xlarge, 100M rows, hot run)

| Query  | Description               | pg_deltax (s) | vs ClickHouse |
|--------|---------------------------|---------------|---------------|
| Q0     | COUNT(*)                  |         0.020 |         2.73x |
| Q1     | COUNT WHERE AdvEngineID   |         0.189 |        12.44x |
| Q2     | SUM/AVG full scan         |         0.072 |         2.65x |
| Q3     | AVG UserID                |         0.071 |         2.19x |
| Q4     | COUNT DISTINCT UserID     |         5.632 |        15.54x |
| Q5     | COUNT DISTINCT SearchPhrase |       3.436 |         5.44x |
| Q6     | MIN/MAX EventDate         |         0.060 |         3.50x |
| Q7     | GROUP BY AdvEngineID      |         0.150 |         8.42x |
| Q8     | GROUP BY RegionID         |         2.164 |         4.71x |
| Q9     | RegionID multi-agg        |         2.185 |         4.13x |
| Q10    | MobilePhoneModel users    |         0.718 |         4.64x |
| Q11    | MobilePhone+Model users   |         0.871 |         5.76x |
| Q12    | Top SearchPhrase          |         1.935 |         3.19x |
| Q13    | SearchPhrase users        |         2.127 |         2.61x |
| Q14    | SearchEngine+Phrase       |         2.195 |         3.63x |
| Q15    | Top UserID                |         1.764 |         4.50x |
| Q16    | UserID+SearchPhrase top   |         3.361 |         1.96x |
| Q17    | UserID+SearchPhrase       |         2.864 |         2.85x |
| Q18    | UserID+minute+Phrase      |         7.443 |         2.44x |
| Q19    | Point lookup UserID       |         0.221 |        17.77x |
| Q20    | URL LIKE google           |         7.680 |        23.88x |
| Q21    | SearchPhrase+URL google   |         4.631 |        42.97x |
| Q22    | Title LIKE Google         |         8.078 |        11.13x |
| Q23    | SELECT * google sorted    |         0.479 |         5.49x |
| Q24    | SearchPhrase by time      |         0.121 |         2.47x |
| Q25    | SearchPhrase sorted       |         4.947 |        24.54x |
| Q26    | SearchPhrase time+phrase  |         0.121 |         2.47x |
| Q27    | CounterID avg URL len     |        12.691 |       136.57x |
| Q28    | Referer domain regex      |        14.239 |         1.49x |
| Q29    | Wide SUM 89 cols          |         0.130 |         3.59x |
| Q30    | SearchEngine+ClientIP     |         9.529 |        27.10x |
| Q31    | WatchID+ClientIP filter   |        13.514 |        23.64x |
| Q32    | WatchID+ClientIP all      |        10.359 |         2.73x |
| Q33    | Top URLs                  |         5.903 |         2.12x |
| Q34    | Top URLs with const       |         5.848 |         2.05x |
| Q35    | ClientIP arithmetic       |        23.103 |        75.29x |
| Q36    | CounterID=62 URLs         |         0.165 |         3.30x |
| Q37    | CounterID=62 Titles       |         0.128 |         4.45x |
| Q38    | CounterID=62 links        |         0.128 |         5.11x |
| Q39    | CounterID=62 traffic src  |         0.863 |        10.03x |
| Q40    | CounterID=62 URLHash      |         0.227 |        10.30x |
| Q41    | CounterID=62 window dim   |         0.107 |         6.16x |
| Q42    | CounterID=62 by minute    |         0.089 |         5.50x |

## Where the time goes

The DeltaX scan has five phases: **metadata** (SPI catalog lookup), **heap_scan**
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
hook; `DeltaXCount` node returns a single row.

### 2. MIN/MAX pushdown [DONE]

**Impact: Q7 65ms -> 0.6ms (generalized to all orderable columns)**

Scan per-column `_min_`/`_max_` metadata in companion table. `DeltaXMinMax`
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

Segments sorted by `min_time`; DeltaXDecompress paths advertise pathkeys.
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

`DeltaXAgg` node computes aggregates directly on decompressed columns. Handles
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
DeltaXAgg computes all sums in a single pass over the decoded column,
applying the constant offset algebraically: `result = base_sum + const * count`.
When all agg specs reference the same column, the column is decoded once and
all results derived from a single accumulator.

### 13. String function pushdown — length() [DONE]

**Impact: Q28 207ms -> improved**

`AggExpr::LengthOf` variant computes string length on raw `&str` slices during
decompression without varlena allocation. Combined with aggregate pushdown,
`AVG(length(URL))` is computed entirely inside DeltaXAgg — zero text
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

DeltaXAgg handles GROUP BY on expressions, not just plain columns:

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
is `>`, `<`, `>=`, `<=`, `=`, `<>`) are pushed into DeltaXAgg. Filters are
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

When `ORDER BY <aggregate> [ASC|DESC] LIMIT N` is detected on a DeltaXAgg
query, the aggregation result is sorted by the specified aggregate column and
truncated to N rows inside the scan node. Pathkeys are set on the CustomPath
so PG eliminates the redundant Sort node above DeltaXAgg. EXPLAIN ANALYZE
shows `TopN: limit=N sort_col=X direction=ASC|DESC pre_topn_groups=M`.

### 22. Per-segment SUM/COUNT metadata for aggregate pushdown [DONE]

**Impact: Q3 11.9ms -> 2.2ms (5.4x), Q4 7.6ms -> 1.4ms (5.4x), Q30 4.7ms -> 1.5ms (3.1x)**

Store per-segment `_sum_<col>` (NUMERIC for integers, DOUBLE PRECISION for floats)
and `_nonnull_count_<col>` (INT) in the companion table for all numeric columns.
During `begin_agg_scan()`, when all aggregates are metadata-resolvable (SUM, AVG,
COUNT, COUNT(*), MIN, MAX on plain columns) and there's no GROUP BY or WHERE clause,
the scan loads only segment metadata — zero decompression, zero row iteration.

Algebraic optimization for `SUM(col + C)`: computes `SUM(col) + C * nonnull_count`
from metadata. This brings Q30 (89 `SUM(col + N)` expressions) from 4.7ms to 1.5ms.

**Files:** `src/compress.rs` (companion DDL, sum computation, INSERT),
`src/scan/exec.rs` (ColSum struct, load_segments_heap load_sums param, metadata fast path)

### 23. Dictionary compression for text columns [DONE]

**Impact: Better compression ratio and faster decompression for low-cardinality text**

Text columns with `ndistinct < 10% of row_count AND < 65536 distinct values`
use dictionary encoding: fixed-width indices into a deduplicated string table.
Falls back to LZ4 for high-cardinality columns. Dictionary entries also serve
as a perfect filter for LIKE pruning (see #19).

### 24. Ndistinct statistics tracking [DONE]

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
are pushed into DeltaXAgg as a 4-element key vector. The scan processes 1M
rows and emits only 10 (via TopN pushdown), eliminating the PG hash agg that
previously dominated at 143ms.

---

## Regression Queries (Compressed Slower Than Uncompressed)

Several queries were slower with compression. Many have been addressed:

### Fixed regressions

**Q24 (was 0.13x):** Fixed by lazy column decompression (#11). Phase 2
skips text varlena allocation for non-matching rows.

**Q30 (was 0.48x):** Fixed by expression aggregate pushdown (#12) and per-segment
SUM metadata (#22). `SUM(col + N)` now resolved from metadata: `SUM(col) + N * nonnull_count`.

**Q28 (was 0.57x):** Fixed by length() pushdown (#13). `AVG(length(URL))`
computed on raw `&str` slices without varlena allocation.

**Q29 (was 0.37x):** Fixed by regex pushdown (#14). `REGEXP_REPLACE` in GROUP BY
runs via Rust `regex` crate on raw slices with cross-segment caching.

**Q23 (was 0.94x):** Fixed by ExecQual removal (#26). Eliminating redundant
per-row PG qual evaluation brought ratio to 1.10x.

**Q36 (was 0.69x):** Fixed by expression GROUP BY pushdown (#27). `col +/- const`
in GROUP BY pushed into AggScan, eliminating 1M-row emit to PG hash agg.

### Remaining regressions

**Q24 (0.82x):** `SELECT * WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10`.
TopN two-pass skips 17/34 segments (dictionary LIKE pruning) and defers Phase 2
to 6 winning segments. Decompress=67ms, heap_scan=24ms. Phase 2 dominates:
decompressing ~100 columns for 6 segments with only 33 candidate rows.
Selection-based decompression was tried (#29) but caused icache regressions.
The fundamental issue is `SELECT *` on a wide table.

**Q29 (0.91x):** `REGEXP_REPLACE(Referer, ...) GROUP BY`. Decompress=756ms on
Referer (high-cardinality LZ4). The regex runs in Rust but decompression of
the full Referer column dominates. (#24 evaluated and deemed not worth implementing.)

**Q33 (1.35x):** `GROUP BY WatchID, ClientIP` — high-cardinality hash agg.
DeltaX scan=21ms, but PG hash agg on 1M rows with ~1M groups dominates.
Would require pushing hash agg into scan — very high effort.

### 32. Metadata-enhanced filtered COUNT/SUM with parallel decompression [DONE]

**Impact: Q1 0.583s -> 0.189s (3.1x improvement, 12.4x vs ClickHouse down from 37x)**

`try_metadata_fast_path` now accepts WHERE clauses on numeric columns. For each
segment, min/max metadata classifies it as:

- **AllPass:** min/max proves all rows satisfy the predicate → use `row_count`/sums
  directly from metadata (zero decompression).
- **Ambiguous:** min/max can't decide → decompress and filter.

Ambiguous segments are decompressed in parallel using `std::thread::scope` with
chunked work distribution (same pattern as the compact aggregation path). Each
thread gets its own `AggAccumulator` vector; results are merged after join. The
fast path only decompresses the qual column + agg column (1-2 columns) vs the
full scan's broader pipeline.

For Q1 (`COUNT(*) WHERE AdvEngineID <> 0`), all 2660 segments are ambiguous
(AdvEngineID has mixed 0/non-0 values in every segment), but parallel
decompression of just one column achieves ~150ms vs ~660ms for the full scan
fallback.

**Files:** `src/scan/exec/agg.rs` (`try_metadata_fast_path`, `merge_accumulator`)

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

### 28. Text GROUP BY in AggScan [DONE]

**Impact: Q16 45.8ms → 22.0ms (2.1x), Q19 351ms → 250ms (1.4x),
Q34 326ms → 258ms (1.3x), Q36 66.8ms → 34.9ms (1.9x),
Q38 68.6ms → 49.3ms (1.4x), GMEAN 6.62x → 7.60x**

AggScan now supports text/varchar GROUP BY keys with several optimizations
for both low- and high-cardinality columns:

1. **hashbrown raw_entry API:** Single hash table lookup without cloning
   the key on cache hit. Uses `from_hash()` with borrowed `GroupKeyRef`
   (raw `*const str` pointers, no lifetime parameter) for zero-copy lookups.
2. **StringArena:** All group key strings packed into one contiguous `Vec<u8>`.
   `GroupKeyVal::Str(u32, u32)` stores (offset, len) into the arena. Eliminates
   275K individual String allocations and their cleanup cost.
3. **GroupKey enum:** `Single(GroupKeyVal)` for the common single-column
   GROUP BY case avoids per-key Vec heap allocation. `Multi(Box<[GroupKeyVal]>)`
   for multi-column.
4. **Flat accumulator storage:** HashMap maps `GroupKey → u32` index into a
   flat `Vec<AggAccumulator>`. Eliminates 275K per-group Vec<AggAccumulator>
   allocations and their O(n) drop cost.
5. **Per-segment SegTextColumn:** Dictionary/LZ4/SegBy text data decoded once
   per segment with O(1) `get_str(row)` access — no cross-segment interning.
6. **Vec reuse:** `key_ref` and `regex_results` buffers allocated once outside
   the row loop, cleared per iteration.

A row-estimate guard in the planner hook skips AggScan for text GROUP BY
when both: (a) PG estimates < 5% of rows survive WHERE filtering, and
(b) the text column has > 100K global ndistinct. For heavily filtered
queries on high-cardinality columns (e.g. Q39: 27K/1M rows with URL),
PG's native HashAgg on a small emitted result set is faster than AggScan's
text decompression overhead. Full-table scans (Q34) and filtered queries
on moderate-cardinality text columns (Q14, Q38) always use AggScan.

### ~~29. Partial decompression for SELECT * with LIMIT~~ — Tried, not effective

**Status: Investigated — marginal Q24 improvement offset by icache regressions**

`SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10`.
TopN two-pass already works (17/34 segments skipped by dictionary LIKE pruning,
6 segments enter Phase 2 with 33 candidates). The bottleneck is Phase 2
decompression of ~100 columns for winning segments.

**Approaches tried:**

1. **Min/max segment skipping on sort column:** Dead end — all 34 segments have
   identical 24h time ranges because `order_by = {counterid, userid, eventtime}`
   with EventTime as 3rd key. Min/max on EventTime gives no discrimination.

2. **Candidate truncation:** After threshold update, truncate candidate list to
   `effective_limit + 1` when oversized. Marginal Phase 1 improvement, and must
   keep at least `effective_limit + 1` candidates to avoid triggering the
   TopN-disabled fallback path.

3. **Selective TOAST detoasting (varatt_is_1b_e):** Only defer detoasting for
   truly external TOAST pointers; eagerly detoast inline blobs. Small improvement
   (~5ms) on Q24 warm runs but doesn't justify the code complexity alone.

4. **Selection-based decompression for ForBitpacked columns:** O(1) random-access
   decode for integer columns (73/105 columns) in Phase 2 — only decode the 1-3
   winning row values per column instead of all ~30K. Phase 2 nontext time dropped
   from 65ms to 13ms. However, adding ~200 lines of new functions (sparse decode,
   Phase2Col enum, null bitmap scanning) increased binary size, causing **10-25%
   icache-induced regressions across 19 unrelated queries** (confirmed by re-running
   baseline on same commit). Net negative.

**Conclusion:** The Q24 bottleneck is fundamentally that `SELECT *` on a 105-column
table requires decompressing all columns for winning rows. The TopN two-pass already
limits this to 6 segments × ~100 columns. Further improvements require either
reducing the number of columns decompressed (projection pushdown) or reducing
per-column decode cost without adding binary bloat.

### ~~30. High-cardinality integer GROUP BY optimization~~ — Largely addressed by #28

**Status: Mostly addressed by hashbrown/flat-accumulator work in #28**

Q16 (`GROUP BY UserID`) improved from 45.8ms → 22.0ms (2.1x) and Q19
(`GROUP BY UserID, minute, SearchPhrase`) from 351ms → 250ms (1.4x) as a
side effect of the hashbrown raw_entry API, flat accumulator storage, and
GroupKey::Single optimizations in #28. Further improvement would require
pre-sizing hash maps or top-N pruning within aggregation.

### 31. WHERE + AggScan combined batch evaluation

**Target: Q31 27.7ms -> ~15ms, Q32 59.6ms -> ~30ms, Q2 broadly**
**Complexity: Medium**

Q31/Q32 have `WHERE SearchPhrase <> ''` combined with GROUP BY aggregation.
Currently the filter and aggregation run in separate passes through the
decoded data. Combining batch qual evaluation with aggregate accumulation in
a single pass would improve cache locality and avoid redundant iteration.

For dictionary columns, the `<> ''` filter can leverage `empty_string_idx`
to skip rows by checking the 1-2 byte index array without decompressing any
string data. Make sure `check_ne_empty()` is wired into the batch eval path
inside AggScan, not just DecompressState.

Simple filtered aggregates without GROUP BY (e.g. Q1
`COUNT(*) WHERE AdvEngineID <> 0`) are now handled by #32's metadata-enhanced
fast path with parallel decompression. This optimization targets the remaining
case: filtered aggregates *with* GROUP BY, where fusing the filter and
accumulation loops improves cache locality.

**Files:** `src/scan/exec/agg.rs` (fused filter+aggregate loop in AggState)

### 33. Trigram bloom filters for LIKE substring pruning [TRIED — NOT EFFECTIVE]

**Target: Q21 7.7s -> ~0.3s, Q22 4.6s -> ~0.5s (ClickBench hot run)**
**Complexity: Medium**
**Status: Investigated and abandoned.**

The idea was to build per-segment trigram bloom filters for LZ4-compressed
text columns and prune segments whose blooms don't contain the pattern's
trigrams.

**Why it doesn't work:** Common search terms like `'%google%'` produce
trigrams (`goo`, `oog`, `ogl`, `gle`) that are individually very frequent
across URL data. With ~30 K distinct URLs per segment, the trigram space is
saturated — virtually every segment contains all common trigrams, so the
bloom filter passes almost everything. Any reasonably-sized bloom (2–8 KB)
has a near-100% false positive rate for common patterns. Only extremely rare
substrings would benefit, and those queries are already fast because
dictionary pruning (#19) handles them.

The cost of storing and checking trigram blooms (extra I/O per segment)
is not justified by the negligible pruning rate on realistic queries.

### 34. Redundant GROUP BY expression elimination ✅

**Target: Q36 23.1s -> ~5s (ClickBench Q35)**
**Result: Q36 23.1s -> 1.4s**
**Complexity: Low-Medium**

Q36: `GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3`. All
four group-by keys are deterministic functions of `ClientIP`. The hash
table stores 4-element keys and computes 4 hash values per row, when a
single `ClientIP` key would suffice.

**Approach:** In the planner hook, detect group-by expressions that are
deterministic functions of other group-by columns:
- `col +/- const` where `col` is already a group-by key → redundant
- Constant expressions (`GROUP BY 1`) → redundant (Q35: `GROUP BY 1, URL`)

Eliminate redundant keys from the GROUP BY during aggregation. At output
time, recompute the eliminated expressions from the base column.

**Impact:** For Q36, reduces from 4-element to 1-element group key:
- 4x less hash table memory (one i64 key vs four)
- 4x fewer comparisons per hash probe
- Better cache utilization (smaller keys = more groups per cache line)

Native columnar engines perform this optimization automatically. Combined
with two-level hashing (#36), Q36 could go from 75x to ~5x.

**Files:** `src/scan/hook.rs` (expression dependency analysis in
`plan_agg_path`), `src/scan/exec/agg.rs` (reconstruct eliminated keys
at output)

### ~~35. Parallel-safe custom scan paths~~ — Investigated, not pursued

**Status: Evaluated — limited upside, high implementation cost**

The original premise was that Q20, Q21, Q25 used DeltaXDecompress and
would benefit ~8x from Parallel Append distributing partition scans
across workers. EXPLAIN ANALYZE shows this was wrong:

- **Q20, Q21, Q22** use `DeltaXAgg` (LIKE-filtered aggregation), not
  `DeltaXDecompress`.
- **Q25** uses `DeltaXAppend` (sorted Top-N text scan), not a plain
  Append over DeltaXDecompress partitions.
- Only the remaining non-aggregate scans (Q19, Q23, Q24, Q25, Q26) go
  through `DeltaXAppend` / `DeltaXDecompress`.

DeltaXAgg already parallelizes internally using `std::thread::scope`
with up to `get_parallel_workers()` (≈16 on a c6a.4xlarge) per
partition — the Append above it runs sequentially over partitions, but
each partition saturates all cores. Enabling PG-level Parallel Append
on top would oversubscribe CPUs (e.g. 8 PG workers × 16 Rust threads
each ≈ 128 threads on 16 cores), almost certainly a regression.

That leaves only DeltaXAppend/DeltaXDecompress queries as candidates.
Those total ~4.7s on the ClickBench hot run, and realistic savings from
Parallel Append would be ~3s. DeltaXAppend is also architecturally
incompatible with Parallel Append: it sets `(*rel).nparts = 0` to
prevent PG from rebuilding Append paths above it, which is how the
leader-side partition scheduling works today. Making it parallel-aware
would require a significant rearchitecture.

Given the modest target and the cost (direct-heap metadata access,
`InitializeWorkerCustomScan` on all five CustomExecMethods, rearchitecting
DeltaXAppend to cooperate with Parallel Append), this optimization is not
being pursued. If the goal changes — e.g. fewer internal Rust threads per
partition so PG workers can compose with them — this should be revisited.

### 36. Two-level hash aggregation [TRIED — LIMITED IMPACT, REVERTED]

**Status (2026-04-18):** Phase 1 (map partitioning only) implemented on
branch `two_phases_hash`. Measured on EC2 c6a.4xlarge 100 M bench (hot
best-of-3):

| Query | Before | After | Δ |
|-------|--------|-------|---|
| Q32 WatchID+ClientIP | 9.53 s | 8.55 s | −0.98 s (−10 %) |
| Q35 ClientIP arith | 1.72 s | 1.50 s | −0.22 s (−13 %) |
| all others | — | — | ±2 % noise |
| **bench total** | **65 s** | **64 s** | **−1.2 s (−1.8 %)** |

Reverted from `main` on 2026-04-18 because the bench-level improvement
is within single-run noise (re-running the full bench multiple times
produces ±2 s variance on EC2). Branch preserved for a possible
revisit if phase 2 (per-sub-table `CompactAccStorage`) is pursued.

**Why phase 1 alone is not enough.** The `CompactAccStorage` remains
one flat 150–200 MB buffer per worker, addressed by a global u32
`group_idx`. Phase-2 merge reads `worker.compact_storage[wgidx]` at
offsets scattered across the whole buffer — every accumulator read is
a DRAM miss. Partitioning the *map* makes group-key lookups cheaper
but does not address the accumulator reads that dominate Q32 merge
time. Early experiments:
- `AGG_SUBMAP_COUNT = 256` gave the measured wins above.
- `AGG_SUBMAP_COUNT = 1024` was slightly worse (more allocator traffic
  per worker, no additional cache benefit for the storage buffer).

**Follow-up approach** (if ever revisited): partition
`CompactAccStorage` and `CountDistinctSideCar` by sub-index too, so
each merge thread reads only its ~580 KB slice of each worker's
storage sequentially. Estimated additional Q32 saving: 1–2 s.
Scope: medium-high — touches `alloc_group`, every
`count_mut`/`sum_int_mut` accessor, and CD merge. Only worth doing if
the projected ~2 s Q32 win is deemed worth the storage-refactor cost.

**Original analysis (kept for context).** For high-cardinality GROUP
BY (>100K groups), the single hashbrown table exceeds L2/L3 cache,
causing random memory access patterns. The idea was to partition the
hash space into 256 sub-tables by one byte of the hash, so (a) each
sub-table fits in L2 during phase-1 accumulation and (b) phase-2
merge threads claim sub-tables by atomic fetch_add with no
synchronization between threads.

Partitioned parallel merge (#41) already captures most of (b) at
`n_workers` granularity. The remaining headroom that two-level
hashing could recover (cache locality on phase-1 inserts + eliminating
the hash-mod filter during merge) is, in practice, small compared to
the unchanged per-accumulator DRAM latency.

**Files (on branch `two_phases_hash`):** `src/scan/exec/agg.rs`
(CompactSubMaps, ParallelCompactMap enum, routing hasher, entry_or_alloc,
iter_partition, iter_all, into_single; phase-1 dispatch in
process_segments_compact; phase-2 dispatch in partitioned merge,
speculative top-N, and full-merge adoption),
`src/scan/path.rs` (est_groups field in plan's custom_private).

### 37. Parallel Top-N text scan with byte-order pruning [DONE]

**Impact: Q25 5.6s -> ~2.0s (2.8x improvement)**

Parallelizes the text Top-N scan (`ORDER BY text_col LIMIT N`) using
`std::thread::scope`. The key insight: `varstr_cmp`/`strcoll` costs ~2μs
per call, dominating execution when ~1.5M rows pass the WHERE filter.
Byte-order comparison (`str::cmp`) costs ~10ns and is used for aggressive
pruning in worker threads, with `strcoll_cmp` applied only on the small
merged candidate set for correct collation-aware final ordering.

**Architecture:**

- **Phase 0 (main thread):** Detoast + segment pruning → surviving segment
  indices. PG API calls (detoast) must stay on the main thread.
- **Phase 1 (parallel):** Workers decompress text columns to `SegTextColumn`
  (pure Rust, thread-safe), evaluate text quals via `apply_text_eq_filter`/
  `apply_text_like_filter`, and collect candidates with byte-order threshold
  pruning. Each worker keeps `max(limit * 100, 10000)` candidates.
- **Merge (main thread):** Byte-order pre-prune → `strcoll_cmp` final sort
  → truncate to limit.
- **Phase 2 (main thread):** Detoast + decompress ALL needed columns for
  winning segments only.

Shared text primitives (`SegTextColumn`, `TextQualInfo`, `decompress_text_to_seg_col`,
`apply_text_eq_filter`, `apply_text_like_filter`, `strcoll_cmp`) extracted to
`src/scan/exec/text_col.rs` for reuse by both the agg and decompress paths.

Falls back to sequential execution when `n_workers <= 1` or fewer than 2
surviving segments.

**Files:** `src/scan/exec/text_col.rs` (new, shared primitives),
`src/scan/exec/decompress.rs` (parallel `exec_topn_text`),
`src/scan/exec/agg.rs` (imports from text_col)

### ~~38. Reduce per-partition SPI overhead~~ — Investigated, kept in branch

**Status: Implemented in a separate branch — modest gain, not merged**

Every partition scan begins with SPI queries to load segment metadata from
companion tables. With 7-8 partitions, that's 7-8 separate SPI calls, each
with SPI_connect/SPI_finish overhead, plan caching, and executor startup.

**What was tried:** Approach 2 — replaced the SPI-based metadata loader
in `src/scan/exec/segments.rs` with direct `table_open` / `heap_getnext`
plus a session-level `thread_local!` cache of the companion OID and the
decoded metadata. A SPI fallback was kept for first-call correctness.

**Results:**
- Metadata phase on Q0 dropped from ~36ms warm to ~1.7ms warm.
- Total query time improvement on Q0 was ~3ms — the rest of the SPI
  time was already hidden behind other work or amortized across
  partitions.
- Cold-run behavior was unchanged after accounting for OS page cache
  variance (the original "39ms cold" baseline was measured with a warm
  page cache; a truly cold Q0 is ~200ms on both SPI and heap paths).
- Other queries saw sub-millisecond changes.

**Why it's not merged:** The main justification for doing this work was
unblocking #35 (parallel-safe paths require no SPI in workers). Since #35
is no longer being pursued, the standalone warm-run win is too small to
justify the added unsafe direct-heap code in the hot path. The branch is
preserved for future reference if parallel-safe paths are revisited.

**Files:** `src/scan/exec/segments.rs` (`load_metadata` direct-heap
implementation), `src/scan/hook.rs` (consolidated `load_deltatable_info`)

### 39. Pipelined detoast + parallel aggregation [DONE — LIMITED IMPACT]

**Target: Q22 9.6s -> ~5s (ClickBench hot run)**
**Actual: modest improvement on CountDistinct (Q4/Q5); negligible on most queries**

Implemented pipelined detoasting for the compact, mixed, and CountDistinct
parallel paths. The main thread detoasts batch N+1 while workers process
batch N, using `std::thread::scope` with `split_at_mut` for safe disjoint
borrows.

**What was done:**
- Compact and mixed GROUP BY paths already had pipelining (n_batches =
  n_workers * 2 for compact, 2 for mixed).
- Extended pipelining to the CountDistinct path (Q4, Q5) by enabling
  lazy loading for all parallel paths (not just GROUP BY).
- Verified `needed_cols` is correct — only referenced columns are detoasted.

**Why impact is limited:** The pipeline only hides detoast latency when
worker processing per batch takes at least as long as detoasting the next
batch. In practice, for queries like Q32:
- Per-batch detoast: ~378 ms (serial, PG backend thread)
- Per-batch worker time: ~60 ms (parallel across 8 threads)
- Workers finish 6× faster → sit idle ~300 ms per batch waiting

The fundamental constraint is `pg_detoast_datum` — serial, I/O-bound,
must run on the PG backend thread. The pipeline can't overcome a 6:1
detoast-to-work ratio.

**Alternatives investigated:**

- **Inline storage (`STORAGE MAIN` + self-chunking):** Chunk blobs into
  ~1.5 KB pieces to stay below the TOAST threshold, eliminating TOAST
  indirection entirely. **Not viable:** PG's LZ4 TOAST compression
  achieves ~31% compression on top of our already-compressed blobs
  (4129 MB raw → 2848 MB on disk for one partition). Inline storage
  would increase I/O by ~45%, likely a net loss.
- **`STORAGE EXTERNAL` (uncompressed TOAST):** Tried earlier. The extra
  LZ4 compression from TOAST still provides meaningful size reduction,
  and the lower I/O from smaller on-disk size is a net win vs the CPU
  cost of double-decompression. Reverted.
- **Session-level blob cache:** Detoast once per session, reuse across
  queries. Would eliminate detoast cost for all but the first query.
  Not yet explored in depth.

**Files:** `src/scan/exec/agg.rs` (pipelined batch loop in compact, mixed,
and CountDistinct paths), `src/scan/exec/segments.rs` (lazy loading)

### 41. Partitioned parallel merge for mixed (text GROUP BY) path [DONE]

**Impact: Q13 4971ms → 2127ms (2.3x improvement, hot run)**

The mixed path (text GROUP BY) had a serial merge bottleneck: all worker
partial results were merged into one hash table on the main thread, then
top-N selection ran as a separate pass. For Q13 (`GROUP BY SearchPhrase
ORDER BY COUNT(DISTINCT UserID) DESC LIMIT 10`) with 3.9M groups, this
serial merge took ~2.9s + 289ms top-N selection.

**What was done:** Added a partitioned parallel merge path for the mixed
path, analogous to the existing one in the compact (int-key) path.
When `topn_limit > 0 && having_filters.is_empty()`:

1. Partition the key space into N slices by hash (N = n_workers)
2. Each thread merges its slice from all workers (including CD sidecar
   unions and MixedKeyStorage copying), writes CD counts, and runs local
   top-N via a bounded heap
3. Copy winners to mini CompactAccStorage + mini MixedKeyStorage
4. Main thread merges N×limit local winners into global top-N

Also removed the `!compact_sort_is_cd` guard from the compact path's
partitioned merge gate — the guard was unnecessary because CD counts are
written to storage via `write_counts_to_storage` before top-N selection.

**Scope:** Primarily benefits Q13. Other mixed-path queries with ORDER BY
+ LIMIT have their speculative top-N succeed (merge=0), so they skip the
full merge entirely. Q28 has HAVING which gates out the partitioned merge.
Q32's 5.8s merge is on the compact path with ~10M groups — the parallel
merge is already active there, the cost is inherent to the cardinality.

**Files:** `src/scan/exec/agg.rs` (partitioned parallel merge in mixed
path, `!compact_sort_is_cd` guard removal in compact path)

### 40. Dict-accelerated LIKE filtering + two-phase column decompression [LARGELY DONE]

**Status (2026-04-21, re-audited):** the two main pieces of this
spec are already in the tree. The third piece ("skip other columns
within a segment that has some matches") is architecturally
incompatible with PG's TOAST model and won't be pursued. Filing
this honestly rather than leaving it on the priority list as
"planned, not impl".

**What's already landed:**

1. **Per-dict-entry LIKE match.**
   `src/scan/exec/text_col.rs::apply_text_like_filter` handles
   dict-encoded columns via a fast path (line ~318): builds
   `dict_matches: Vec<bool>` once per segment by evaluating the
   pattern against each unique dict entry, then per-row does
   `dict_matches[row_to_entry[row]]` — one integer lookup instead
   of a string scan. ~60× fewer string ops, as the original spec
   predicted. Same pattern applies to `apply_text_eq_filter`.

2. **Segment-level pruning on zero dict matches.**
   `src/scan/exec/segments.rs::segment_skippable_by_dict` is
   called in all three worker loops
   (`try_metadata_fast_path`, `process_segments_compact`,
   `process_segments_mixed`) immediately after time/segment-by
   pruning. For a segment whose dict has no matching entry, the
   whole segment is skipped — no decompression of filter or
   non-filter columns.

**What's NOT landed and won't be (re-audited 2026-04-21):**

3. **Sub-segment column deferral.** The original spec proposed a
   two-phase column-load for segments with SOME matches: decompress
   only the filter column first, check which rows survive, then
   detoast other columns only if row count > 0. This doesn't work
   with TOAST: `pg_detoast_datum` is all-or-nothing per column
   blob. We can't "detoast only the rows where Title matched"
   because a column's blob is a single atomic varlena. The per-row
   save only kicks in if an entire blob goes untouched — which is
   exactly the segment-level skip already implemented by #2.

**Measured effect of the already-landed pieces (Q22 today):**
`segments=2915` surviving out of 3338 (13 % dict-pruned already).
Remaining detoast cost (2.37 s) is for the 2915 survivors, each
having at least one Title entry matching `%Google%` but mostly few
rows actually matching. That waste is real but bounded by the
TOAST granularity — no per-row detoast avoidance is possible.

**What could further help Q22/Q20/Q21 (if we ever revisit):**
- Smaller segments (e.g. 5 K rows instead of 30 K) would let
  per-segment dict-skipping prune finer. Big architectural change.
- Bloom filters on LZ4 (non-dict) text (#25, low-card / #33
  trigram, high-card) — both evaluated and not viable today.
- Inverted trigram postings (per-trigram → rowset). Storage-prohibitive.

**Files (reference):** `src/scan/exec/text_col.rs`
(dict fast paths in `apply_text_like_filter`,
`apply_text_eq_filter`), `src/scan/exec/segments.rs`
(`segment_skippable_by_dict`), `src/scan/exec/agg.rs`
(call sites).

### 42. Text-length sidecar for `length()` / `col <> ''` [DONE]

**Impact: Q27 1.80s → 0.55s hot (3.3x), 7.97s → 1.99s cold (4.0x). Bonus:
Q30 1.57s → 1.05s (−33%), Q31 2.24s → 1.75s (−22%) and their cold runs
21–28% faster, because their `WHERE SearchPhrase <> ''` filter is now
served from the sidecar.**

At compression time we emit a per-row character-length array for every text
column and store it LZ4-compressed in a new `*_text_lengths` companion table.
For a text column `col`, if every query-time reference is one of
`length(col)`, `col = ''`, `col <> ''` (and the column is not in GROUP BY),
the scan loads the small length blob instead of detoasting the full text
blob. Lengths are character counts (not bytes), matching PG's `length(text)`
semantics for UTF-8.

Why this works on Q27 specifically: the URL column accounts for the entire
~1 s of hot detoast time. The main URL blob is ~830 KB per segment; the
length sidecar is ~10 KB (~80× smaller). `length(URL)` becomes a direct u32
lookup; `URL <> ''` becomes `length > 0`. No varlena allocation, no string
materialization, no LZ4 decode of the main blob.

**Measured breakdown (Q27 hot, 3338 segments):**

| Phase | Before | After |
|-------|--------|-------|
| detoast | 1037 ms | 245 ms |
| decompress | 253 ms | 25 ms |
| agg | 558 ms | 240 ms |
| **total** | **1.79 s** | **0.54 s** |

Per-segment metadata is also enriched: `_sum` / `_nonnull_count` /
`_nonzero_count` in colstats now get populated for text columns as
`SUM(length)` / non-null count / non-empty count (they were NULL
previously). This also enables future metadata-only fast paths on
`AVG(length(col))` without GROUP BY.

**Gating.** The sidecar is activated only when the parallel mixed path
will run (`n_workers > 1 && can_parallel_mixed(...)`) and the planner-level
detection succeeds. Other paths (compact-only, non-parallel fallback,
decompress path) don't know about sidecars and continue loading the main
blob. This is strict to keep the change non-invasive — a column that's
eligible on query shape but lands in a non-mixed path still works, it
just doesn't get the speedup.

**Disqualifications.** The detection rejects the column if any:
- MIN/MAX agg on the column (Q22, Q28: MIN(URL), MIN(Referer))
- LIKE / NOT LIKE qual (Q20, Q21, Q22)
- GROUP BY on the column (Q33, Q34)
- Any other agg shape that isn't `LengthOf`

Each of these paths needs the full string body.

**Storage cost.** +650 MB across 18 partitions (~65 MB/partition) for all
text columns' sidecars. Compared to ~2.8 GB/partition of main text blobs,
this is within rounding at the benchmark total (both 12.93 GiB).
Load time +4% (310 s → 323 s) for the per-row character-count pass.

**Wire format.** Length blobs reuse the existing `CompressedColumn`
framing: `[tag=Lz4][row_count][has_nulls][null_bitmap?][lz4(u32 array)]`.
Single new variant `SegTextColumn::Lengths` in the text column decoder.

**Files:** `src/compress.rs` (`compress_text_lengths`, text-aware
`compute_typed_sum`, DDL additions), `src/copy.rs` (direct backfill path:
buffer + heap_insert), `src/scan/exec/text_col.rs` (`Lengths` variant,
`get_len()`, empty-string fast path in `apply_text_eq_filter`,
`decompress_length_sidecar`), `src/scan/exec/segments.rs`
(`load_text_length_sidecars` PK index scan), `src/scan/exec/agg.rs`
(sidecar detection in planner, `ParallelMixedConfig.sidecar_only_cols`,
LengthOf accumulators routed through `get_len()`).

### 43. HLL sketches for COUNT(DISTINCT)

**Status (2026-04-21): deprioritized.** Pre-HLL fixes (a)+(b)+(c)
captured the bulk of the original ~4.5 s target. Remaining
HLL-specific saving is ~1.2 s cumulative (see bottleneck analysis
below), against medium implementation cost + an approximate-semantics
GUC. Not currently planned; leaving the design notes intact should
the tradeoff change later (e.g. if the bench runs push Q8/Q9/Q13
back up the priority list).

**Original target (before (a)(b)(c) landed, on EC2 100 M bench, hot best-of-3, 2026-04-18):**
Q4 3.04 s → ~0.2 s, Q5 1.78 s → ~0.3 s (pre-computed sketches).
Q8 1.02 s → ~0.7 s, Q9 1.47 s → ~1.1 s, Q13 2.03 s → ~1.6 s
(query-time sketches). **Cumulative ~4.5 s across the bench.**
**Complexity: Medium**

#### Validated bottleneck analysis (measured on EC2, 2026-04-18)

Instrumented the code to measure the serial merge step directly.
Warm-run numbers with the original `std::collections::HashSet`:

| Query | Wall | detoast | agg | **serial merge** | # partial results | worker-set inserts |
|-------|-----:|--------:|----:|-----------------:|------------------:|-------------------:|
| Q4 `COUNT(DISTINCT UserID)`        | 3024 ms | 420 | 453 | **2513 (83%)** | 479 | 21.98 M |
| Q5 `COUNT(DISTINCT SearchPhrase)`  | 1763 ms | 584 | 608 | **1109 (63%)** | 479 | 8.35 M |

**The serial merge dominated both queries.** The step is at
`src/scan/exec/agg.rs` lines 4824–4844: after parallel workers each
build a thread-local set, the leader iterates every partial result
and inserts every entry into one global set. For Q4 this is 22 M
serial inserts into a set that grows to ~100 M entries, long past
L3 — at ~114 ns per insert (SipHash + cache-miss) this cost ~2.5 s.

After fix (a) hashbrown + fix (b) parallel CD merge (both DONE):

| Query | Wall | merge | Δ wall |
|-------|-----:|------:|-------:|
| Q4 | **703 ms** | 271 ms | −2321 ms (−77 %) |
| Q5 | **753 ms** | 94 ms | −1095 ms (−59 %) |

Additional observation: there are **479 partial results** (pipelined
detoast splits the scan into ~30 batches × 16 workers). Each partial
result holds a small HashSet; the leader walks all 479 linearly.
A partial result count of 16 (fused across batches) would reduce
allocator/iterator overhead but not the insert count.

#### Two cheaper pre-HLL wins uncovered by this measurement

These were evaluated **before** HLL because they deliver most of
the Q4/Q5 improvement with exact semantics and minimal change.

**Fix (a) — swap `std::collections::HashSet` → `hashbrown::HashSet` [DONE].**
The parallel CD path (`ParallelCdResult` and the `AggAccumulator`)
used the std lib's SipHash-based HashSet. Changed to hashbrown with
ahash. ~30 lines in `agg.rs` (type alias, struct field types,
constructors, test fixtures).

Measured result (EC2 c6a.4xlarge, 100 M bench, hot best-of-3):

| Query | Before | After | Δ |
|-------|-------:|------:|--:|
| Q4 `COUNT(DISTINCT UserID)` serial merge | 2513 ms | 1590 ms | −923 ms |
| Q4 wall | 3022 ms | **2017 ms** | **−1005 ms (−33 %)** |
| Q5 serial merge | 1115 ms | 534 ms | −581 ms |
| Q5 wall | 1848 ms | **1214 ms** | **−634 ms (−34 %)** |
| **Bench total (hot)** | 64.96 s | **63.40 s** | **−1.56 s (−2.4 %)** |

No regressions on any other query (within ±10 ms noise). Per-insert
cost dropped from ~114 ns → ~72 ns on Q4 and ~134 ns → ~64 ns on
Q5. hashbrown's ~1.5–2× speedup doesn't fully compound because the
set still grows past L3 — DRAM-miss latency dominates even with
faster hashing.

**Fix (b) — parallelize the CD merge [DONE].** A `thread::scope` pass
partitions the output keyspace by hash (N=16). Each thread owns one
partition, walks every partial result's int_set/str_set, and inserts
only values whose hash routes to its partition. Output buckets are
disjoint by construction, so the final distinct count is
`Σ bucket.len()` with no global reconstruction. The accumulator is
bypassed entirely since this path is gated on all-CountDistinct
(every spec's result is a count). Merge is now visible in EXPLAIN as
`merge=...`.

**Fix (c) — parallelize CD count in speculative top-N Phase 5 [DONE].**
The same pattern applies to the Phase 5 "for each winner, merge CD
accumulators across workers" path that runs when speculative top-N
succeeds on queries with GROUP BY + CountDistinct. Instrumentation
showed 98 % of Q9's 321 ms finalize was `HashSet::extend` (top 10
RegionIDs have ~7 M cumulative distinct UserIDs; destination set
grew past L3 at ~60 ns per insert). Replaced with a parallel
partitioned count across winners — same partitioning trick, but
indexed over winners × cd_slots. Non-CD accumulators (Count, SumInt,
…) still merged serially since they were only ~2 ms of the 321 ms.

Measured results (on top of fix (a)):

| Query | After (a) only | After (a)+(b) | After (a)+(b)+(c) | Δ total |
|-------|---------------:|--------------:|------------------:|--------:|
| Q4 wall | 2017 ms | **703 ms** | 707 ms | −65 % |
| Q4 `merge=` | 1590 ms | **271 ms** | 271 ms | 5.9× |
| Q5 wall | 1214 ms | **753 ms** | 750 ms | −40 % |
| Q5 `merge=` | 534 ms | **94 ms** | 94 ms | 5.7× |
| Q9 wall | 1503 ms | 1487 ms | **1245 ms** | −17 % |
| Q9 `finalize=` | 317 ms | 320 ms | **65 ms** | −80 % |
| **Bench total (hot)** | 63.40 s | 61.44 s | **61.09 s** | **−3.87 s (−6.0 %)** |

**Cumulative pre-HLL wins (a)+(b)+(c):** Q4 3.02 → 0.71 s (−77 %),
Q5 1.85 → 0.75 s (−59 %), Q9 1.50 → 1.25 s (−17 %), bench total
64.96 → 61.09 s (−3.87 s, −6.0 %) — all with exact semantics, ~300
LoC total.

Partitioning uses SplitMix64 for int keys (cheap, well-distributed)
and top bits of u128 for text keys (they're already SipHash-128
digests from `hash128_str`, uniformly random).

After fixes (a)+(b), Q4/Q5 breakdown is:

| Q4 | Q5 | Phase |
|---:|---:|-------|
| 391 ms | 581 ms | detoast |
| 401 ms | 598 ms | agg (per-worker HashSet build) |
| 271 ms | 94 ms | merge (parallel, now trivially fast) |
| ~50 ms | ~30 ms | framework/misc |

HLL still wins on top of (a)+(b) by eliminating:
1. The ~400–600 ms of column detoast (no need to read the data column
   if we have pre-computed per-segment sketches).
2. The ~400–600 ms of per-worker HashSet build (no need to hash
   every value at query time).

Post-HLL projected: Q4 → ~100 ms, Q5 → ~200 ms. Net HLL-specific
saving on top of (a)+(b): **~600 ms on Q4, ~550 ms on Q5, ~1.2 s
cumulative** — about the same as HLL would have contributed before,
but now expressed vs a much lower baseline (a+b already captured
the low-hanging fruit).

#### HLL approach (replaces HashSet entirely)

HLL replaces the union operation with elementwise `max` over a fixed
16 KB register array per sketch — commutative/associative, fully
parallelizable, and trivially fast (~50 µs for 16 sketches). Pair
this with **pre-computed per-segment sketches** (compress-time) and
the no-GROUP-BY CD path becomes metadata-only: no detoast, no
per-worker build, no serial merge.

#### Approach

`COUNT(DISTINCT col)` today goes through `CountDistinctSideCar`: one
`HashSet<u64>` (or `HashSet<u128>` for text) *per group*, inserted
row-by-row during phase-1, union-merged across workers during phase-2,
and finalized as `set.len()`.

**Approach.** Replace the per-group HashSet with a HyperLogLog sketch:
a fixed-size register array (e.g. 16 KB / 2¹⁴ registers, standard
precision = 14) where each register holds the run of leading zeros of
the value's hash that was routed to that register. Properties:

- **Insert:** `reg[hash & mask] = max(reg[hash & mask], clz(hash >> shift))` —
  one AND + one comparison + one store. Constant-time, no hashing
  chain, fixed memory. Batched inserts are SIMD-friendly.
- **Merge across workers:** elementwise `max` over register arrays.
  Sequential access, no hashing.
- **Finalize:** `|distinct| ≈ α · m² / Σ 2^(−reg[i])` — standard HLL
  estimator. ~0.8 % relative error at 16 KB / precision 14.

#### Per-segment sketches for the no-GROUP-BY shape (Q4, Q5) — **biggest win**

Pre-compute an HLL sketch per segment at compress time, stored in a
new companion blob (`_hll_<col>` in the existing `_blobs` table or a
dedicated companion, analogous to `_text_lengths` for #42). Query
time: load one sketch per segment, merge via elementwise max, estimate.

This variant eliminates both:
1. The detoast of the full column blob (Q4: 412 ms on UserID blobs;
   Q5: 581 ms on SearchPhrase blobs).
2. The serial HashSet merge (Q4: ~2.2 s; Q5: ~0.6 s) — replaced by a
   fully-parallel elementwise max over 16 KB arrays.

**Expected Q4:** 3338 segments × 16 KB sketches ≈ 52 MB total detoast
(vs ~200 MB UserID blobs). Merge = 3338 × 2¹⁴ max ops ≈ 55 M ops ≈
50 ms. Plus sketch-blob I/O ≈ 100 ms cold, much less warm. Estimated
warm total: **~200 ms** (down from 3.04 s, ~15× win).

**Expected Q5:** Similar. The dict-only fast path already gets
`count_distinct_only_str` to skip per-row decode, but it still loads
the full 200+ KB dict-encoded blobs. Sketches are ~16 KB each. Merge
like Q4. Estimated warm total: **~300 ms** (down from 1.78 s, ~6×).

#### Per-group sketches for GROUP BY + COUNT(DISTINCT) (Q8, Q9, Q13) — **smaller wins**

Replace `Vec<HashSet>` with `Vec<HllRegisters>` inside
`CountDistinctSideCar`, where each group's sketch is 16 KB of u8
registers. Per-group insert and merge as above.

Honest note: these queries already use the parallel compact / mixed
path with **partitioned parallel merge** (#41). Their CD-sidecar
merge is already parallelized, so HLL gains are modest — mainly
from replacing allocator-heavy HashSet operations with fixed-size
array writes, and saving the union cost across workers.

- Q8 `GROUP BY RegionID COUNT(DISTINCT UserID)`: ~10 K unique regions,
  ~10 K distinct UserIDs per group avg. Sidecar size per worker:
  10 K × 16 KB = 160 MB (fits comfortably). Inserts go from
  ~100 ns hashbrown to ~5 ns register update. Merge
  (elementwise max) ~10 ms vs 310 ms HashSet-union.
  Projected: **1.02 s → ~0.7 s.**
- Q9 `GROUP BY RegionID multi-agg`: same structure as Q8 plus two
  more aggregates. `finalize=317ms` line is the `set.len()` × groups
  cost — HLL estimate is similar O(register_count) per group, net
  saving modest. Projected: **1.47 s → ~1.1 s.**
- Q13 `GROUP BY SearchPhrase COUNT(DISTINCT UserID)`: 4.8 M groups ×
  16 KB = **77 GB per worker — won't fit.** Must use sparse representation:
  start every group as `SparseHLL` (sorted list of `(register_idx,
  register_value)` pairs), switch to dense 16 KB register array only
  when the sparse rep exceeds ~128 entries (~2 KB). Keeps memory bounded
  by actual distinct count. Most Q13 groups have <20 distinct UserIDs,
  so sparse is fine. Projected: **2.03 s → ~1.6 s.**

**Sparse-to-dense conversion** is well-documented in the HLL++ paper
(Google). Implementation is ~200 lines.

These smaller wins are the follow-on tier — the big Q4+Q5 sketch
optimization (above) should land first.

#### Accuracy caveat

HLL is approximate — standard 0.8 % relative error at precision 14.
ClickBench reference queries use ClickHouse's default `uniq()` which
is *also* approximate (HLL-style), so matching that is fine for
bench semantics. But DeltaX currently implements `COUNT(DISTINCT)`
with exact semantics via HashSets. Two options:

1. **Default to HLL, GUC to opt out.** `pg_deltax.exact_count_distinct`
   = false by default. Matches ClickHouse behavior; users who need
   exact semantics set it to true and get the current path.
2. **Default to exact, opt in to HLL.** Safer but means Q4/Q5/Q8/Q13
   don't benefit unless explicitly enabled.

Recommendation: option 1, aligned with ClickHouse. Document the
approximation clearly in the GUC's description.

#### Compile-time vs query-time sketches

**Compile-time:** for queries without GROUP BY (Q4, Q5), pre-compute
one sketch per (segment, column) at `deltax_create_table` and update
on each load. Storage cost: ~16 KB × n_segments × n_high_cardinality
columns. On the 100 M bench, ~3 high-cardinality int/text columns
worth tracking → ~150 MB total. Well within budget.

**Query-time:** for GROUP BY queries, sketches are built per-(group,
segment) during phase-1 aggregation. Must live in worker-local CD
sidecar. No storage overhead; query-time memory is the concern
(addressed by sparse-to-dense above).

#### Orthogonality to other changes

HLL touches **only** `CountDistinctSideCar` and its callers
(`insert_int`, `insert_str`, `union_from`, `write_counts_to_storage`,
and the places that finalize `CompactAccKind::CountDistinct*`). It does
not depend on or conflict with any map-layout change (e.g. #36).

#### Files

- `src/compression/hll.rs` (new) — `HllRegisters`, `SparseHll`,
  `DenseHll`, encode/decode for persistence, merge, estimate.
- `src/compress.rs` — compute and store per-segment sketches for
  opted-in columns.
- `src/scan/exec/agg.rs` — `CountDistinctSideCar` uses `HllRegisters`
  instead of `HashSet`. Finalize reads from the registers.
- `src/scan/exec/segments.rs` — `load_hll_sketches` PK index scan
  (analogous to `load_text_length_sidecars`).
- `src/lib.rs` — `pg_deltax.exact_count_distinct` GUC.

### 44. Two-pass filter for `GROUP BY … LIMIT N` without `ORDER BY` [DONE]

**Measured on EC2 c6a.4xlarge, 100 M bench, hot best-of-3 (2026-04-21):**
Q17 **1.686 s → 0.921 s (−765 ms, −45 %)**. DeltaXAgg `agg` phase
**805 ms → 215 ms (−74 %)**. `f8_preselected=10` visible in
`DeltaX Stats`. No regressions on other queries (all within ±50 ms
noise). Correctness verified: Q17 result rows have exact global
counts (spot-checked four rows against `SELECT COUNT(*) WHERE …`).

**Target: Q17 1.48 s → ~0.8 s (−45 %, agg 805 ms → ~130 ms)**
**Complexity: Medium (~200 LOC)**

Q17 is `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY
UserID, SearchPhrase LIMIT 10` — no ORDER BY. Today we still
materialize every group (`pre_topn_groups ≈ 17 M`), then PG's Limit
node picks 10. EXPLAIN shows `agg=805 ms` on top of `detoast=517 ms`
building a 17 M-entry hashbrown that's 99.99999 % thrown away.

**Correctness constraint (key to the design).** Under PostgreSQL
semantics, `LIMIT N` without ORDER BY may return any N aggregated
rows in any order, but each returned row must carry the
**correct global aggregate value** for its group. A naive "stop
worker loops after accumulating N local groups" approach would
emit partial counts — verified: Q17's `(1148718334461794889, '', 9)`
has true global count 9, a worker that stopped after one segment
would report count 1. That's a correctness bug, not a valid
optimization.

**Approach — two-pass filter.**

1. **Phase 0 (main thread, before spawning workers).** Decompress
   the group-by columns of one (or up to a small M) segment from
   the leader's list. Iterate rows, build a `HashSet<u128>` of the
   first N distinct packed keys using the same `hash_mixed_key`
   routine workers use. Cost: ~30 ms for one segment. Fall back to
   `None` (current path) if we can't fill N keys within M segments.

2. **Phase 1 (workers).** Each worker receives
   `preselected_keys: Option<&HashSet<u128>>` via
   `ParallelMixedConfig`. In the row loop, immediately after
   `hash_mixed_key` produces the u128, probe the preselected set
   (≤ N entries → L1-resident → ~5 ns). On miss, `continue`. On
   hit, fall through to the existing accumulator path. Each
   worker's `compact_map` is now bounded to ≤ N entries.

3. **Merge & emit.** Unchanged — the existing `bare_limit`
   post-worker merge (agg.rs:3820) already handles the "pick N
   keys, merge only those" shape.

**Why this is correct.** Every row whose key is in the preselected
set is counted exactly once across all workers (existing merge sums
workers' contributions). Every row not in the set is skipped —
those groups are never emitted. Output: exactly N rows, each with
correct global count for its key. PG's LIMIT N requires any valid
N rows from the aggregate; these are valid. ✓

**Why this is fast.** The 17 M-entry hashbrown per worker is
DRAM-bound (~100 ns per probe). A 10-entry hashbrown is L1-resident
(~5 ns per probe). Per-row cost in Phase 1 is ~20 ns (hash + probe)
vs ~100 ns today — matches the observed 805 → 130 ms gap. Detoast
(517 ms) and decompress (176 ms) are unchanged; they dominate the
remaining ~800 ms.

**Gating.** Only trigger when:
- `parse->hasLimit && parse->limitCount` is a positive const ≤ 10 K
- `parse->sortClause` is empty (no ORDER BY)
- `parse->groupClause` is non-empty
- `parse->havingQual` is null (HAVING could eliminate preselected
  keys, leaving < N rows in the output)
- Query has no non-trivial WHERE (Phase 0 would pre-select keys
  that might get filtered out of Phase 1 — narrowing scope keeps
  the implementation tractable; can be relaxed later by having
  Phase 0 apply the same batch quals)

**Scope.** Only Q17 in ClickBench hits this exact shape. Wires in
the mixed path only; compact path (int-only GROUP BY) gets no
additional ClickBench benefit but could be added the same way if a
future query needs it.

**Fallbacks.**
- Phase 0 yields < N distinct keys across M segments → `None`,
  current full-build path runs unchanged. Q17's first segment has
  ~30 K distinct pairs, so N = 10 is trivial; the fallback exists
  for degenerate synthetic workloads.
- NULL-containing keys: the existing `has_null → continue` skip
  (agg.rs:8814) is preserved, matching current NULL semantics.

**Files:**
- `src/scan/hook.rs` — extend the existing `bare_limit` gate
  (around line 2304) with `havingQual.is_null` + no-WHERE check;
  signal F8-eligibility via `custom_private`.
- `src/scan/exec/agg.rs` —
  - `ParallelMixedConfig` (line 8412): new `preselected_keys`
    field.
  - New `try_build_preselected` helper (~80 LOC) — reuses
    `decompress_numeric_blob` (agg.rs:7507), `decompress_text_to_seg_col`
    (text_col.rs:137), and `hash_mixed_key` (agg.rs:8816).
  - Row-loop filter (~5 LOC) at agg.rs:8816 — one probe after
    `hash_mixed_key`.
  - EXPLAIN counter for observability (e.g. `f8_preselected=10/10`
    line in DeltaX Stats).
- Unit test (`#[pg_test]` in agg.rs) + integration test
  (`tests/test_functions.py`) asserting exact-count semantics on a
  synthetic table with known (X, Y) duplicate distribution.

**Complexity.** Originally estimated at ~50 LOC / low complexity
based on a (semantically incorrect) "stop early" approach. Revised
to ~200 LOC / medium complexity after working through the
correctness constraint. Savings recomputed: ~0.7 s on Q17 (not
~1.5 s), but the shape is common in interactive querying so the
UX value extends beyond bench.

### 45. Dict sidecar blob for dict-encoded text columns [NOT PURSUED — PREMISE WRONG]

**Status (2026-04-21): investigated, dropped without implementing.**

The original proposal assumed the dict is a small prefix of a
dict-encoded blob (~5 KB vs ~200 KB), so a dict-only sidecar would
let `COUNT(DISTINCT)` / `ORDER BY LIMIT` paths skip ~95 % of
detoast. Direct inspection of the on-disk blobs invalidated that
assumption.

**Measured dict fraction on ClickBench (`_deltax_compressed.hits_p20130702_blobs`):**

| Column | blob size | LZ4 dict bytes | dict % of blob |
|--------|----------:|---------------:|---------------:|
| Title (col 2) | 270 KB | 211 KB | **78 %** |
| URL (col 13) | 217 KB | 157 KB | **72 %** |
| Referer (col 14) | 660 KB | 600 KB | **91 %** |
| SearchPhrase (col 39) | 161 KB | 101 KB | **63 %** |
| MobilePhoneModel (col 34) | 30 KB | <1 KB | 0.5 % (only 15 distinct values) |

Dense-cardinality dict columns (which are the ones
`COUNT(DISTINCT)` targets) have dicts that are **63–91 %** of the
blob, not 2–5 %. A dict sidecar duplicates most of the storage
for marginal detoast savings.

**Revised savings estimate (ClickBench):**
- Q5 COUNT DISTINCT SearchPhrase: detoast 580 ms × (1 − 0.63)
  ≈ 215 ms saved → 708 ms → ~490 ms.
- Q25 ORDER BY SearchPhrase LIMIT: ~150–250 ms saved (not 1.75 s).
- Q22 partial: Title dict is 78 % of blob — same story.
- Cumulative: ~300–500 ms (not 3 s).

**Storage cost:** +2–3 GB across the bench (was estimated 50 MB).
For a ~14 GB on-disk dataset that's a 15–20 % footprint increase
for sub-1 s bench saving. Not a favourable trade.

**Why the original estimate was wrong.** I assumed "dict" meant
~500 entries × ~10 bytes ≈ 5 KB. Reality: ClickBench text columns
have 1,500–8,000 distinct entries per segment with long average
entry lengths (URLs, titles, referer URLs), plus the dict is
LZ4-compressed inline, so the on-disk dict section is large.

**Adjacent finding — #40 is largely already implemented.** While
confirming what the dict-only fast path does today,
`apply_text_like_filter` in `src/scan/exec/text_col.rs:303` already
computes per-dict-entry LIKE matches once per segment
(`dict_matches: Vec<bool>`) and indexes into it per-row.
`segment_skippable_by_dict` in `src/scan/exec/segments.rs` already
prunes segments where no dict entry matches. The remaining item
from the original #40 spec — "skip other columns' decompression
when a segment has some matches" — is incompatible with PG's TOAST
model (detoast is all-or-nothing per blob). So most of what #40
proposed is already in the tree.

**Revival criterion.** If we ever compress a workload where dicts
really are a small prefix (e.g. very low cardinality with short
strings — MobilePhoneModel shape), this optimization becomes
attractive. The design notes below are left intact for that case.

---

**Original design (for reference):**

Several queries only need the dictionary portion of a dict-encoded
blob, not the per-row index array:

- `COUNT(DISTINCT SearchPhrase)` (Q5) — current dict-only fast path
  hashes only the dict entries per segment, but still detoasts the
  full main blob to reach the dict header.
- `ORDER BY text_col LIMIT N` (Q25) — only needs the lex-smallest
  dict entry per segment to produce top-N candidates.
- Dict-accelerated LIKE pre-check — tests the pattern against dict
  entries without touching the index array.

**Approach.** At compress time, emit the dict as its own LZ4 blob
in a new `*_text_dicts` companion table (analogous to
`*_text_lengths` in #42). Wire format can be identical to the
leading bytes of the existing main blob's dict header — just split
out.

Query-time: segments.rs gains a `load_text_dict_sidecars` PK scan,
activated when all query-time references to the column are
dict-resolvable. The main blob detoast is then skipped for those
segments.

**Files (if ever revived):** `src/compress.rs`, `src/copy.rs`,
`src/scan/exec/segments.rs`, `src/scan/exec/text_col.rs`
(`SegTextColumn::DictOnly` variant), `src/scan/exec/agg.rs`
(dict-only dispatchers detect sidecar availability).

### 46. Text-empty segment pruning via `nonzero_count` [TRIED — REVERTED]

**Status: implemented and reverted on 2026-04-21.** The pruning
mechanically works (verified via `DeltaX Stats`: `segments=3332`
on Q12–Q14 vs 3336 baseline; `rows_processed` drops by ~120 K on
SearchPhrase queries) but on the ClickBench dataset only 4–6 of
3338 segments are fully empty for SearchPhrase and 0 for
MobilePhoneModel. Bench total moved ±50 ms across repeat runs —
indistinguishable from noise.

**Why reverted.** The "might help real-world clumpy data"
justification was speculative and carried measurable complexity
cost:
- ~100 LOC across `segments.rs` + `agg.rs`
- Three new helpers (`is_empty_text_const`, `classify_text_empty_qual`,
  `segment_skippable_by_text_empty`)
- A subtle tweak to `classify_segment_quals` that skips the
  post-loop NULL-safety check for text-empty quals (correct but
  non-obvious)
- New `needed_stats_cols` construction in the mixed path that
  only existed to feed F7

The optimization's ceiling on ClickBench (~0.15 % of segments
prunable) is low enough that even with perfect clumpiness on some
hypothetical dataset, the gain would be small vs the permanent
reading cost imposed on everyone maintaining the pruning code.

**If the workload calls for it later:** the helpers and the
planner plumbing were straightforward to write the first time
(this section is effectively the spec). The nonzero_count stats
are still collected at compress time (#42), so the foundation is
in place.

**Target: Q30 1.03 s → ~0.9 s, Q31 1.79 s → ~1.5 s; smaller margins on Q10, Q11, Q12, Q21, Q22.**
**Cumulative: ~0.3–0.6 s** on a dataset with real temporal clumpiness.
**Complexity: Low (~20 LOC core logic, ~100 LOC with plumbing).**

The compressor already tracks `_nonzero_count_<col>` for text
columns (number of non-empty rows per segment — see
`compress.rs::compute_typed_sum` extended for text in #42).
`segments.rs::check_all_pass` already uses `nonzero_count` to prune
segments for `Ne 0` / `Eq 0` on integers — but only when the qual
constant is numeric zero (`is_zero_const`). Text `<> ''` is lowered
to `BatchCompareOp::Ne` with a text-empty constant and misses the
gate.

**Approach.** Extend `is_zero_const` (or add
`is_empty_text_const`) to recognize the empty varlena constant.
Then the existing path works unchanged:
- Segment with `nonzero_count == 0` → filter eliminates every row → `NonePass` → skip segment.
- Segment with `nonzero_count == row_count` and `nonnull_count == row_count` → filter is satisfied for every row → `AllPass` → strip the qual.

**Datasets where this helps.** The optimization is worth the gain
proportional to clumpiness of empty-text values in segments. In
ClickBench, SearchPhrase is only 13 % non-empty globally on Q30/Q31;
MobilePhoneModel is 50 % non-empty on Q10/Q11 — both are time-
clustered in the source data, so some (likely many) segments are
fully empty and can be skipped entirely. Measured exact benefit
would need a run; rough lower bound is 10–30 % of the affected
queries' detoast cost.

**Files:** `src/scan/exec/segments.rs`
(`is_zero_const` → also matches empty varlena;
`check_all_pass` unchanged), possibly
`src/scan/exec/batch_eval.rs` (ensure `Ne` on empty text lands in
the same constant canonicalization).

### 47. Partition-level bloom filter for point lookups

**Target: Q19 43 ms → ~15 ms (ClickBench hot run)**
**Complexity: Low-Medium**

Q19 (`WHERE UserID = <const>`) already benefits from per-segment
min/max pruning (1870 segments skipped) and per-segment bloom
filters (1418 more skipped). But EXPLAIN shows `bloom hit=5926`
buffer pages read on warm — those are the 1468 surviving segments'
blooms being loaded and tested to produce 50 surviving segments.

**Approach.** Store a coarser bloom filter per **partition** (18
total) in the partition-level metadata. At query time, test the
point-lookup constant against each partition's bloom first; skip
all segments in partitions that reject. Remaining partitions fall
through to the existing per-segment bloom path.

Sizing: each partition holds ~185 segments × ~30 K rows ≈ 5.5 M
rows. A 256 KB bloom at 4 hashes gives ~1 % FPR at that scale —
small enough to fit comfortably in a companion row.

**Expected effect.** On Q19, partition-level blooms would likely
reject ~15 of 18 partitions, dropping the per-segment bloom checks
from ~1468 to ~550 and the buffer reads proportionally. The 30 ms
saving is small in absolute terms but the change is cheap; same
infrastructure extends to equality predicates in general.

**Scope.** Only helps equality predicates on columns with bloom
filters. Bench-level impact is ~30 ms (Q19 alone). Worth doing as
part of a broader partition-level pruning pass if/when other
partition-level optimizations land; marginal on its own.

**Files:** `src/compress.rs` (partition-level bloom build — runs
during `deltax_create_table` and on partition compaction),
`src/scan/exec/segments.rs` (partition-level bloom load + test
before per-segment bloom).

### 48. Q40 decompress anomaly — byte-aligned bitpack fast path [DONE]

**Landed 2026-04-21. Measured on EC2, warm best-of-3:**

| Query | Before | After | Δ | Cause |
|-------|------:|-----:|--:|-------|
| Q40 | 138 ms | **107 ms** | **−31 ms (−22.5 %)** | direct target |
| Q22 | 3748 ms | 3653 ms | −95 ms | URL/Title/SearchPhrase decompress |
| Q20 | 6735 ms | 6654 ms | −81 ms | URL column decompress |
| Q31 | 1785 ms | 1712 ms | −73 ms | WatchID+ClientIP i64 decompress |
| Q16 | 2087 ms | 2043 ms | −44 ms | UserID+SearchPhrase i64 decompress |
| **Bench total** | **60.51 s** | **60.26 s** | **−252 ms** | |

No regressions above noise. All 364 unit tests pass.

**Root cause.** The original hypothesis (column over-decompression
— `needed_cols` too broad) was wrong. The real bottleneck was
`unpack_bits_u64` in `src/compression/bitpacked.rs`: a bit-by-bit
inner loop that processed EACH value with ~8 inner iterations,
reading one byte and doing shift/mask/OR per iteration — **even
when `bits == 64`** (the common case for high-cardinality hash
columns like URLHash, RefererHash, UserID, WatchID, ClientIP,
where bit-packing offers no savings and the data is stored at
full 64-bit width).

For a segment of 30 K values at bits=64:
- Old: 30 K × 8 inner iterations × ~10 arith ops ≈ 2.4 M ops per
  segment ≈ 1.4 ms per i64 column per segment.
- New: 30 K `u64::from_le_bytes` calls ≈ 30 µs — essentially
  memcpy speed.

**What was isolated, verified, then fixed:**
1. Filter-ablation experiments on Q40 showed that removing
   `RefererHash = const` cut `decompress` from 73 ms to 37 ms →
   one i64 column costs ~36 ms / 26 segments ≈ 1.4 ms/segment.
2. An isolated `SELECT COUNT(*) WHERE RefererHash = const` query
   confirmed the 37 ms / 1.4 ms-per-segment cost.
3. Blob-layout inspection showed RefererHash is stored as
   `ForBitpacked` with `bits=64` (no actual bitpacking — raw
   u64-per-row).
4. Walking `unpack_bits_u64` revealed the byte-by-byte inner
   loop had no fast path for byte-aligned widths.

**Fix.** Added byte-aligned fast paths for `bits ∈ {8, 16, 32, 64}`
in `unpack_bits_u64` and `unpack_bits_u32`. Each path uses
`chunks_exact` + `from_le_bytes` — trivial to read, vectorizable
by LLVM, and ~45× faster than the bit-loop for the common
`bits=64` case.

Also folded a small refactor of `decompress_numeric_blob` that
was explored first (splitting no-null fast path from
null-containing path). That refactor alone was immaterial (as
measured — the allocator churn wasn't the real problem), but it
cleaned up the structure and saved one intermediate `Vec<Datum>`
allocation per column per segment, so it's kept.

**Files touched:**
- `src/compression/bitpacked.rs` — `unpack_bits_u64` /
  `unpack_bits_u32` fast paths for byte-aligned widths.
- `src/scan/exec/agg.rs` — `decompress_numeric_blob` split into
  no-null fast path + null-containing path helpers.

### 49. Parallel no-GROUP-BY aggregation with text quals + single-sweep LZ4 LIKE [DONE]

**Landed 2026-06-10. Q20 4.63 s → 1.02 s warm (4.5×) on the full
ClickBench EC2 dataset.**

Two stacked changes, both motivated by Q20
(`SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'`):

**(a) No-GROUP-BY aggregates now run on the parallel mixed path.**
`COUNT(*)/SUM/AVG` with a text WHERE qual previously fell through
every parallel dispatch — `can_parallel_mixed` rejected empty
`group_specs`, `try_metadata_fast_path` bails on non-numeric quals,
and the CD path requires all-CountDistinct — so Q20 scanned 1703
segments on one thread (worker-scaling test: flat 4.6 s from 4 to
32 workers). The fix funnels every row into a single constant-key
group (`hash_mixed_key(&[], &[])`):

- `can_parallel_mixed` accepts empty `group_specs`; the existing
  "must involve a text column" requirement still applies, so pure
  numeric-qual shapes keep their metadata fast path.
- The dispatch gate in `begin_agg_scan` admits the no-GROUP-BY shape
  only when batch quals exist (unfiltered aggregates stay on the
  metadata/CD paths). The length-sidecar activation gate mirrors the
  same clause to stay in lockstep.
- The parallel-CD dispatch moved ahead of the mixed dispatch so an
  unfiltered `COUNT(DISTINCT text_col)` keeps the CD-specific
  partitioned merge instead of being claimed by mixed.
- `dispatch_parallel_mixed_path` builds the leader `CompactAccStorage`
  itself when the caller didn't (it only did so for GROUP BY), and
  synthesizes the single empty-key group when no row passed the
  filters — a no-GROUP-BY aggregate must emit exactly one row
  (COUNT = 0, SUM/AVG = NULL).

**(b) `LIKE '%needle%'` on LZ4 text columns is one memmem sweep per
segment.** `apply_text_like_filter` ran `str::contains` per row —
searcher set-up cost on every ~100-byte haystack plus per-row UTF-8
validation in `get_str` (`from_utf8` was ~6 % of the Q20 profile).
`apply_lz4_contains_filter` runs a single SIMD `memmem::Finder` sweep
over the segment's decompressed buffer and maps hits back to rows by
binary search over the offset-ordered row ranges. A hit must lie
fully inside one row's value (the buffer interleaves 4-byte length
prefixes); boundary-spanning hits resume one byte further so an
overlapping in-row match is never lost. NULL rows never pass, even
under NOT LIKE. Q20 agg phase: 752 → 419 ms.

The sweep only fires on the *initial* evaluation (`sel` empty). When
an earlier qual already narrowed the selection — Q22's dict-encoded
`Title LIKE '%Google%'` runs before the `URL NOT LIKE` — the per-row
path visits only surviving rows and beats sweeping the full buffer
(first measured as a +7.6 % Q22 regression before the gate).

**Measured (EC2 c6a.4xlarge, 100 M rows, warm EXPLAIN ANALYZE):**

| Query | Before | After (a) | After (a)+(b) |
|-------|-------:|----------:|--------------:|
| Q20 COUNT(*) URL LIKE | 4,627 ms | 1,177 ms | **1,018 ms** |
| Q22 Title LIKE + URL NOT LIKE | 3,457 ms¹ | — | **3,105 ms** |

¹ Q22 measured after (a) — it has GROUP BY so (a) doesn't affect it;
its gain is from (b).

Result-set equality for all 43 queries verified against
`clickbench/reference_results.json` (`make -C clickbench verify`).

**Files touched:**
- `src/scan/exec/agg/parallel_mixed.rs` — `can_parallel_mixed` empty
  group_specs; leader storage bootstrap + empty-group synthesis in
  `dispatch_parallel_mixed_path`.
- `src/scan/exec/agg/callbacks.rs` — dispatch-gate relaxation,
  CD-before-mixed ordering, sidecar gate lockstep clause.
- `src/scan/exec/text_col.rs` — `apply_lz4_contains_filter` +
  Contains routing in `apply_text_like_filter`.
- `tests/test_nogroup_parallel_agg.py` — integration coverage for the
  no-GROUP-BY shapes (zero-match one-row semantics, NOT LIKE NULLs,
  mixed text+numeric quals, COUNT(DISTINCT) with text qual).

### 50. Blob-cache auto-size: cap 4 → 16 GiB, fraction RAM/4 → RAM/6 [DONE]

**Landed 2026-06-10. Bench hot total 58.8 s → 54.5 s (−4.3 s, −7.4%)
on the full ClickBench EC2 dataset, no regressions.**

Two-constant change in `src/blob_cache/mod.rs`: `AUTO_CAP_MB`
4096 → 16384 and the auto fraction `MemTotal/4` → `MemTotal/6`
(floor unchanged at 256 MiB). On the c6a.4xlarge (32 GiB) auto now
resolves to ~5.2 GiB instead of the capped 4 GiB.

**Why it mattered.** The old cap pinned the cache at 4 GiB while the
compressed dataset is 14.6 GB. The big-text working sets (URL ≈
2.1 GB, URL+Title+SearchPhrase+UserID ≈ 4.3 GB for Q22) didn't fit
alongside the int columns, so warm runs of the text-heavy queries
thrashed the LRU: Q22 ran warm at a **42% miss rate** and re-paid
2.8 s of detoast on every run, Q20 1.5 s. With the working set
resident, warm detoast collapses to ~0 across the board.

**Why RAM/6, not RAM/4.** The first iteration kept 25% (→ 7.8 GiB
here) and OOM-killed Q32 when the full bench ran back-to-back on one
postmaster (`make verify`): once queries 0–31 fill the cache, Q32's
high-cardinality agg transient (~15–18 GB anon at the time, see #51)
+ 8 GB shared_buffers + 7.8 GB cache exceeded the box. Shmem pages,
once touched, are non-reclaimable without swap, so a filled cache is
permanent occupancy — the default has to leave room for worst-case
query transients. RAM/6 + the #51 merge-memory reduction fits with
headroom; the official bench protocol (PG restart before each query)
was never at risk because the cache then only ever holds the current
query's columns.

**Measured (EC2 c6a.4xlarge, 100 M rows, warm EXPLAIN ANALYZE):**

| Query | Before | After | detoast before → after |
|-------|-------:|------:|-----------------------:|
| Q20 COUNT(*) URL LIKE | 2,004 ms | **1,120 ms** | 1,528 → 3 ms |
| Q22 Title LIKE + URL NOT LIKE | 4,158 ms | **2,753 ms** | 2,778 → 8 ms |
| Q28 Referer REGEXP_REPLACE | 8,172 ms¹ | **7,827 ms¹** | — |

¹ best-of-3 bench numbers; Q28's gain is partial-working-set
residency, not a full fit.

Full-bench comparison (best-of-3 hot, history
`20260610_081000` vs `20260610_104132`): only Q20 (−3.5 s, combined
with #49 which landed in the same window), Q22 (−0.41 s) and Q28
(−0.35 s) moved beyond noise; cold totals unchanged (+0.7 s on a
310 s sum).

The cache reserves its full size as shmem up front (DSA in-place),
but pages are only touched as entries land, so the larger cap costs
nothing on workloads that never fill it.

### 51. Dedup-aware partitioned merge (compact path) [DONE]

**Landed 2026-06-10. Q32 warm 9.5 → 8.7 s, peak backend RSS during
Q32 21.4 → 17.1 GB (−4.3 GB). Unblocked the #50 cache-size increase.**

`compact_partitioned_topn` used to have each partition thread build a
full partition-local copy of its slice of the group space: a
`CompactGroupMap` *plus* a `CompactAccStorage` into which every
worker's accumulators were copied slot-by-slot. On essentially-unique
GROUP BY keys (Q32: 99,997,494 groups from 99,997,497 rows) that
second copy is 99.99% pure waste — almost no key appears in more
than one worker partial, so there is nothing to merge.

Now the partition map stores a packed `u64` reference instead of a
group index: bit 63 = dup flag; singles pack
`(worker_idx << 32) | worker_gidx`, dups index into a `dup_storage`
that only materializes groups actually seen in ≥2 worker partials
(via `merge_group_into`, the thread-safe per-group extraction of the
`merge_compact_results` inner loop). Top-N heap reads and HAVING
filters resolve through the reference (`read_slot_i64`); winner
materialization into the per-partition mini-storage stride-copies
from the owning worker's storage and fixes up MinStr/MaxStr arena
offsets and CountDistinct counts (singles read CD counts from the
worker sidecar — they were never written into worker storage Count
slots).

Effects on Q32 (warm, EXPLAIN ANALYZE):

| Metric | Before | After |
|---|---:|---:|
| merge | 5,298 ms | 4,435 ms |
| total DeltaX | 8,854 ms | 7,999 ms |
| peak backend RSS | 21.4 GB | 17.1 GB |

The memory drop matters more than the time: Q32's transient anon was
what made the #50 blob-cache increase OOM in no-restart sessions
(8 GB shared_buffers + filled cache + ~18 GB agg transient > 30 GB
box). With the dedup merge, `make -C clickbench verify` (all 43
queries against one postmaster, cache fully populated) passes:
43 OK, 0 mismatch, no OOM.

Worst-case behaviour (every key present in all workers, e.g.
low-cardinality groups that still route here) degenerates gracefully:
every key promotes to `dup_storage` on its second sighting, which is
the same alloc + merge work as before plus one extra map write; peak
memory is no worse than the old design.

**Files touched:** `src/scan/exec/agg/parallel_compact.rs`
(`merge_group_into` helper + rewritten partition-thread closure in
`compact_partitioned_topn`).

**Bench-level (best-of-3 hot, history `20260610_104132` →
`20260610_112034`):** Q32 9.464 → 8.585 s, Q15 1.944 → 1.842 s, all
other queries within noise. Combined with #50, bench hot total
58.8 → **53.4 s** (−9.1%) for the day.

### 52. Segment-local memo for Lz4 text group keys [TRIED — REGRESSED, REVERTED]

**Tried 2026-06-10, reverted the same day (Q33/Q34 bench hot
3.35 → 3.62 s, +8%).**

The dict-aware fast path in `process_segments_mixed` (single text
group key, no int keys) only covers `SegTextColumn::Dict`. URL-class
columns are LZ4-encoded, so Q33/Q34 (`GROUP BY URL`) hash the full
string twice per row (`hash_mixed_key` builds a u128 from two ahash
streams) and probe the multi-million-entry global group map per row.
URL repeats ~4.2x within a segment (30 K rows ≈ 7.1 K distinct), so a
per-segment `AHashMap<&str, u32>` (key string → group index) looked
like it should collapse most of those probes into small-map hits.

It lost at bench level because the trade is worse than it looks: the
global map is `HashMap<u128, u32>` — collisions are impossible by
construction, so probes never compare keys, only u128s. The memo
probe pays one ahash over the string (vs two) but adds a full-string
`memcmp` on every hit plus an insert on every miss, and at 7 K
entries × ~32 B the memo doesn't stay L2-resident anyway. Warm
EXPLAIN ANALYZE in a mixed session suggested −8% (confounded by blob
cache evictions inflating the baseline); the bench protocol (PG
restart per query, best-of-3) showed +8%. Keep Lz4 group keys on the
generic path.

Two pieces from the same pass survived: skipping the generic per-row
key-component build loop when the Dict fast path is active (it
re-fetched the string per row just to discard it), and an input-side
memo in the regex transform (`apply_regex_to_seg_col`, Lz4 arm) that
skips regex + output-dedup work for repeated inputs (Referer repeats
~2.7x per segment).

**Files touched:** `src/scan/exec/agg/parallel_mixed.rs`
(`process_segments_mixed`), `src/scan/exec/agg/regex.rs`
(`apply_regex_to_seg_col`).

### 53. Byte-op fast path for prefix-strip regexp_replace [DONE]

**Landed 2026-06-10. Q28 bench hot 7.918 → 2.769 s (with #54) —
2.4x faster than ClickHouse's 9.58 s on this query.**

`perf` on Q28 showed 37% of all CPU inside the regex crate — 30.4% of
it in `BoundedBacktracker::search_imp`. The ClickBench pattern
`^https?://(?:www\.)?([^/]+)/.*$` is not one-pass (at byte `w` the
NFA can't tell whether `www.` belongs to the optional literal or to
the capture class), so the regex meta-engine resolves the capture
with its bounded backtracker on every one of the 81 M matching rows.

`SimplePattern` (in `regex.rs`) recognizes the restricted shape
`^ lit [opt-lit | x?]... ([^c]+) c-lit .*$` at plan time and compiles
it to byte comparisons: literals via `starts_with`, the optional
literals via greedy try-then-skip (same preference order as the regex
engine), and the capture via a scan for the stop byte — valid because
parse-time validation requires the component after the capture to be
a literal starting with the stop char, so the capture provably ends
at its first occurrence and no backtracking can ever be needed.
Anything outside the shape (two captures, alternations, class
escapes, unanchored patterns, `\2+` backrefs) returns None and falls
back to the regex crate. Differential tests in `regex.rs` assert
byte-for-byte agreement with the Rust regex engine on the tricky
inputs (backtracking case `https://www./path` → host `www.`, empty
host, embedded newlines, multibyte hosts, case sensitivity).

**Files touched:** `src/scan/exec/agg/regex.rs` (`SimplePattern`),
`src/scan/exec/agg/callbacks.rs` (construction).

### 54. Skip per-row UTF-8 revalidation in text hot paths [DONE]

**Landed 2026-06-10 (same pass as #53).**

The same Q28 profile showed 9.3% of CPU in `core::str::from_utf8` and
3.0% in `do_count_chars`: `SegTextColumn::get_str`/`get_len` (Lz4
arm), `StringArena::get` and the regex-transform row loop revalidated
UTF-8 on every access, although the bytes are decompressed PG text
our own compressor wrote — valid UTF-8 by construction. These four
sites now use `from_utf8_unchecked` with a `debug_assert!` guard.
This is why Q28's agg phase (per-row `MIN(Referer)` get_str +
`AVG(length(Referer))` get_len) dropped 4.3 → 1.7 s on top of the
#53 decompress-side win; every text-heavy aggregate benefits.

**Files touched:** `src/scan/exec/text_col.rs`,
`src/scan/exec/agg/compact.rs`, `src/scan/exec/agg/regex.rs`.

**Bench-level for #52–#54 combined (best-of-3 hot, history
`20260610_114934` → `20260610_131038`):** Q28 7.918 → 2.769 s,
Q20 1.199 → 1.061 s, Q13 2.320 → 2.224 s, Q12 1.179 → 1.112 s,
Q34 3.412 → 3.234 s, everything else within noise. Bench hot total
53.77 → **48.23 s** (−10.3%).

### 55. Singleton-skip two-pass top-N for near-unique COUNT(*) sorts [DONE]

**Landed 2026-06-11. Q32 warm 4.63 → 2.73 s (EXPLAIN ANALYZE;
agg 2.63 → ~2.2 s across two passes, merge 1.57 s → 0.07 s).**

ClickBench Q32 (`GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT
10`, no WHERE) has 99,997,494 groups in 99,997,497 rows — exactly
four pairs occur twice, everything else is a singleton. The compact
path built a 100M-entry map (plus accumulators) across 16 workers and
partition-merged all of it to pick 10 winners, almost all of which
are interchangeable count=1 ties.

The two-pass scheme in `parallel_compact.rs` exploits that a count=1
group can never beat a count>=2 group:

- **Pass 1** decompresses only the GROUP BY key columns and bumps a
  shared `CountingFilter` — a blocked counting Bloom filter (two
  byte-wide saturating counters per key inside one 64-byte block, so
  each row costs a single cache-line fetch; `AtomicU8` with a
  load-before-add guard that makes wraparound impossible). Two
  occurrences of a key always leave both its counters at >= 2, so the
  filter has no false negatives; false positives just take the exact
  path. Sized at 8 slots/row (cap 1 GiB → load ~0.19 at 100M rows),
  measured FP ~3.3% on Q32.
- **Pass 2** is the normal worker aggregation loop, except rows whose
  key the filter proves globally unique skip the group map entirely —
  apart from `limit` filler singletons per worker, aggregated
  normally so the merge always has enough exact count=1 groups to pad
  ties. Worker maps end up at ~3.3M total entries instead of 100M, so
  the partitioned merge collapses (1.57 s → 0.07 s) and map-insert
  traffic disappears.

Gating: `ORDER BY COUNT(*) DESC LIMIT <=10K`, no HAVING / batch quals
/ WHERE / segment filters / time bounds, >= 16M rows, and a
cardinality hint of >= 0.9 × exact row count. The hint is the catalog
HLL ndistinct (`deltax_partition.column_ndistinct`) of the most
distinct single bijective group column, summed across scanned
partitions — a lower bound on the true group count. Planner
`plan_rows` can't serve here: it's clamped to the estimated input
rows (3338 segs × 10K = 33.4M for Q32 vs 100M actual), which would
make Q32 indistinguishable from genuinely duplicate-heavy keys where
pass 2 would degenerate into the full map plus a wasted pass 1
(that's also why UserID- or ClientIP-keyed top-Ns — sum-nd 20M/14M vs
100M rows — correctly stay on the old path). Pass 1 reuses the
pipeline-detoast overlap, so on cold runs it hides behind I/O.

Correctness note: any count=1 group is as valid a LIMIT tie pick as
any other (the old path's pick among ties was equally arbitrary);
Q32 is a `LIMIT_TIE_QUERIES` entry in the verify harness, and the
count>=2 groups + all aggregate values are exact.

**Files touched:** `src/scan/exec/agg/parallel_compact.rs`
(`CountingFilter`, `process_segments_count_filter`,
`process_segments_compact_filtered`, dispatch gating),
`src/scan/exec/agg/callbacks.rs` (`nd_hint` from catalog ndistinct).

### 56. Sampled count-floor two-pass top-N on the mixed (text) path [DONE]

**Landed 2026-06-12. Full bench protocol: hot geomean(+10ms) 0.288 →
0.282 (−2.1%), hot total 28.25 → 26.73 s. Q18 2.72 → 1.90 s,
Q16 1.33 → 0.98 s, Q33 1.97 → 1.82 s, Q34 1.98 → 1.82 s.**

Generalizes #55 from "skip count=1 groups" to "skip every group whose
total count is provably below the limit-th largest" and extends it to
the mixed/text aggregation path, where unfiltered
`GROUP BY … ORDER BY COUNT(*) DESC LIMIT n` queries spend nearly all
their time building tens-of-millions-entry digest maps (Q18: 57.8M
groups in 100M rows; Q16: 26.9M; Q33/Q34: 27.7M).

Three pieces on top of #55:

- **Sample-proven floor.** Pass 1 exact-counts a key-coherent 1/128
  sample alongside the filter bumps (membership depends only on the
  key hash, so a sampled key's count is its exact global count). The
  floor is the count of the `limit`-th largest sampled key
  (`pick_count_floor`): that many global groups provably reach it, so
  the true top-N all do, and pass 2 skipping every key the filter
  reads below the floor is exact — no fallback or verification pass.
  Sampled floors on ClickBench: Q18 59, Q16/Q33/Q34 235 (saturation
  cap). When the sample can't prove more than the singleton floor
  (2), the #55 filler semantics kick in unchanged (Q32's top 10 is
  count-1 ties; its sampled floor stays 2).
- **Cache-resident filter.** The mixed path sizes the
  `CountingFilter` at 2^25 slots (32 MB) instead of #55's 1 GiB: at
  100M rows the full-size filter pays a DRAM miss per bump and per
  probe (~0.5 s per pass, measured — it ate the entire two-pass gain;
  Q18 was 2.72 s with the 1 GiB filter, 1.90 s with 32 MB). Collision
  noise at this density (~6 rows/slot) only matters to floors below
  ~16; pass 2 drops the filter entirely when the sampled floor lands
  below that (pass 1 sunk, correctness unaffected — collisions only
  inflate counts, so skips stay sound at any size).
- **Mixed-path pass 1** (`process_segments_mixed_count_filter`)
  decompresses only the GROUP BY key columns and folds each row into
  the same 128-bit digest the aggregation loop keys its map on
  (int components then text components; per-dict-entry digests for
  dict-encoded text, matching the multi-key dict fast path). Pass-2
  probes sit at the digest sites: the generic/multi-key insert path
  probes per row, the single-dict-key fast path caches a `GIDX_SKIP`
  sentinel per dict entry so skipped entries cost one branch per
  subsequent row.

Gating mirrors #55 (`ORDER BY COUNT(*) DESC LIMIT <=10K`, unfiltered,
>= 16M rows, plain Column/AddConst/DateTrunc/Extract keys, no
sidecar-only key columns) but from `nd_hint >= 0.125 ×` rows instead
of 0.9: text-keyed map entries cost several times an int-keyed one
(arena + key storage + digest map), so the two-pass overhead breaks
even at much lower cardinality. The compact path keeps the 0.9 gate —
measured at Q35/Q15-class mid-cardinality (21M/100M int groups), the
map+merge savings only offset the extra key scan (±50 ms), so the
near-unique regime stays its only target.

Results verified against ground truth on EC2 (subquery form that
disables the top-N pushdown): Q18/Q16 count sequences and Q33 exact
URL sets match; Q32 unchanged (boundary=1, filler path).

**Files touched:** `src/scan/exec/agg/parallel_compact.rs`
(`CountingFilter::{with_max_size, bump_hashed, above_floor,
is_sampled_hashed}`, `pick_count_floor`, sampled pass 1),
`src/scan/exec/agg/parallel_mixed.rs`
(`process_segments_mixed_count_filter`, floor gate + pass-1 dispatch,
probe sites, `GIDX_SKIP`), `src/scan/exec/agg/callbacks.rs`
(`compute_group_nd_hint` shared by both paths).
