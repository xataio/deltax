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

### 40. Dict-accelerated LIKE filtering + two-phase column decompression

**Target: Q22 9.6s -> ~2s, Q20 7.7s -> ~2s, Q21 4.6s -> ~1s (ClickBench hot run)**
**Complexity: Medium**

For dictionary-compressed text columns, LIKE/NOT LIKE filters are currently
evaluated row-by-row: `get_str(row)` looks up the dict entry, then
`string.contains(pattern)` is called for each of ~30K rows per segment.
But every row's value is one of ~500 dict entries — checking the same
strings thousands of times.

**Dict-accelerated filtering:** Check the LIKE pattern against each unique
dict entry once, build a bitset of matching entry IDs, then produce the
row-level selection bitmap via integer lookups into `row_to_entry`:

```
Current:  2787 segments × 30K rows × string.contains() = 83M string ops
Proposed: 2787 segments × 500 entries × string.contains() = 1.4M string ops
          + 83M integer lookups (matching_entries[row_to_entry[row]])
```

~60x fewer string operations. Exact (not approximate) — every non-null
row value IS one of the dict entries.

**Two-phase column decompression:** Combined with dict-accelerated
filtering, enables skipping decompression of non-filter columns for
segments with zero matches:

- Phase 1: Parse only the filter column's dict header → check LIKE against
  dict entries → if 0 match, skip segment entirely (no URL/SearchPhrase/
  UserID decompression)
- Phase 2 (matching segments only): Decompress remaining columns, apply
  remaining filters, aggregate

For Q22 (`Title LIKE '%Google%'`), 37K rows match out of 100M (0.037%).
Dict pruning (#19) already skips segments where no dict entry matches,
but with matches scattered across most segments, few are skipped. The
win here is avoiding full text decompression + row-by-row matching for
the non-filter columns in every segment.

**Applies to:** `apply_text_like_filter` and `apply_text_eq_filter` in
`text_col.rs` for the dict fast path. `process_segments_mixed` in
`agg.rs` for two-phase column loading.

**LZ4 fallback:** High-cardinality LZ4-compressed columns don't have a
dictionary and fall back to the current row-by-row path. For those,
trigram bloom filters (#33) are the appropriate optimization.

**Files:** `src/scan/exec/text_col.rs` (dict-aware `apply_text_like_filter`
and `apply_text_eq_filter`), `src/scan/exec/agg.rs` (`process_segments_mixed`
two-phase column decompression)

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

### 44. Early termination for `GROUP BY … LIMIT N` without `ORDER BY`

**Target: Q17 1.69 s → ~0.02 s (ClickBench hot run)**
**Complexity: Low (~50 LOC)**

Q17 is `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY
UserID, SearchPhrase LIMIT 10` — no ORDER BY. Today we still
materialize every group (`pre_topn_groups ≈ 17 M`), then PG's Limit
node picks 10. EXPLAIN shows `agg=813 ms` on top of `detoast=523 ms`
to build a hash table that's 99.9999 % thrown away.

Under PostgreSQL's semantics, `LIMIT N` without `ORDER BY` is
allowed to return any N rows in any order. Once aggregation has
accumulated ≥ N distinct groups, the remaining segments cannot
change the result.

**Approach.** In the planner hook (`plan_agg_path`), detect the
shape `GROUP BY … LIMIT N` with no sort key, and flag
`GroupByLimit::EarlyTerm { limit: N }` in `custom_private`. In the
phase-1 mixed/compact worker loop in `agg.rs`, after processing
each segment check `local_map.len() >= limit` and, if so, set an
`early_done` atomic flag observed by other workers to stop their
segment loops. Phase-2 merge proceeds normally on the (small)
accumulated state; each worker's map has ≥ `limit` groups but
probably far more (workers don't coordinate on which groups they
already covered), so the global map is still typically larger than
`limit`. That's fine — the LIMIT at the top of the plan truncates.

**Gating.** Only trigger when:
- `parse->hasLimit && parse->limitCount` is a positive const
- `parse->sortClause.is_null()` (no ORDER BY)
- `parse->groupClause.is_not_null()` (actually has GROUP BY —
  otherwise the existing TopN path or plain aggregate applies)
- No HAVING clause

**Scope.** Only one ClickBench query hits this exact shape (Q17),
so the bench-level win is ~1.5 s; but the change is small, safe,
and the same shape shows up in interactive exploration queries
(people frequently prototype with `GROUP BY … LIMIT 10` before
adding an ORDER BY).

**Files:** `src/scan/hook.rs` (shape detection + `custom_private`
encoding), `src/scan/exec/agg.rs` (atomic `early_done` flag, segment
loop check in both compact and mixed paths).

### 45. Dict sidecar blob for dict-encoded text columns

**Target: Q5 0.76 s → ~0.2 s, Q25 1.91 s → ~0.15 s (ClickBench hot run). Cumulative ~3 s.**
**Complexity: Medium**

Several queries only need the dictionary portion of a dict-encoded
blob, not the per-row index array:

- `COUNT(DISTINCT SearchPhrase)` (Q5) — current dict-only fast path
  hashes only ~500 dict entries per segment, but still detoasts the
  full ~200 KB main blob to reach the dict header.
- `ORDER BY text_col LIMIT N` (Q25) — only needs the lex-smallest
  dict entry per segment to produce top-N candidates.
- Dict-accelerated LIKE pre-check (#40 pre-phase) — tests the
  pattern against ~500 dict entries without touching the index
  array.

**Approach.** At compress time, emit the dict as its own LZ4 blob
in a new `*_text_dicts` companion table (analogous to
`*_text_lengths` in #42). Wire format can be identical to the
leading bytes of the existing main blob's dict header — just split
out. Storage cost: per segment, roughly the same size as the dict
section of the main blob (~2–10 KB), minus some framing overhead.

Query-time: segments.rs gains a `load_text_dict_sidecars` PK scan,
activated when all query-time references to the column are
dict-resolvable (COUNT DISTINCT / MIN/MAX by lex / dict-LIKE
pre-check). The main blob detoast is then skipped for those
segments.

**Gating & disqualifications.** Same pattern as #42:
- Any per-row reference (equality with non-dict constant, GROUP BY,
  emit in projection) disqualifies and forces main-blob detoast.
- Only activated when the aggregate dispatcher knows the fast path
  applies (`count_distinct_only_str`, `topn_dict_only_text`,
  dict-LIKE pre-check in Phase 1).

**Interaction with #40.** When #40 lands, the dict-accelerated
LIKE pre-phase reads only the dict. Pairing with this sidecar lets
the pre-phase skip the main blob entirely for non-matching
segments, compounding the gain.

**Storage cost estimate.** 3338 segments × ~5 KB avg per text dict
column × 3 dict columns (SearchPhrase, Title, one more) ≈ 50 MB —
negligible against the existing ~14 GB on-disk footprint.

**Files:** `src/compress.rs` (`compress_text_dict` or emit during
`compress_text_column`, companion DDL), `src/copy.rs` (direct
backfill: buffer + `heap_insert`), `src/scan/exec/segments.rs`
(`load_text_dict_sidecars`), `src/scan/exec/text_col.rs`
(`SegTextColumn::DictOnly` variant), `src/scan/exec/agg.rs`
(dict-only dispatchers detect sidecar availability).

### 46. Text-empty segment pruning via `nonzero_count`

**Target: Q30 1.03 s → ~0.9 s, Q31 1.79 s → ~1.5 s; smaller margins on Q10, Q11, Q12, Q21, Q22.**
**Cumulative: ~0.3–0.6 s.** **Complexity: Low (~20 LOC).**

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

### 48. Q40 column-pruning audit

**Target: Q40 138 ms → ~80 ms**
**Complexity: Investigation**

Q40 (`CounterID=62 URLHash range query`) shows `decompress=71 ms`
for only 89,914 rows out of 100 M — roughly 0.8 µs/row, which is
an order of magnitude higher than other range queries on similar
row counts (Q36 `decompress=12 ms` for 671 K rows). The 6 batch
quals narrow to a tiny working set, but the scan appears to be
decompressing more columns than strictly needed.

**Investigation.** Add per-column decompress timing to the
`decompress` phase of DeltaXAgg (via a feature-flagged or
debug-only dump), run Q40 under it, and identify which columns are
being materialized beyond the ones referenced in SELECT, GROUP BY,
and WHERE. Likely suspects: a fallback in `needed_cols`
construction that adds a column for qual evaluation even when the
qual was batch-handled, or Phase 2 kicking in for columns that
Phase 1 quals already eliminated.

**Expected outcome.** Either a targeted fix (tighten `needed_cols`
at the specific call site), or confirmation that Q40's decompress
cost is fundamental and the query is well-optimized. ~60 ms saving
on Q40 is the plausible ceiling.

**Files:** probably `src/scan/exec/agg.rs` (`needed_cols`
computation) and `src/scan/exec/decompress.rs` (phase boundary).
