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
| Q13    | SearchPhrase users        |         8.027 |         9.87x |
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

### 33. Trigram bloom filters for LIKE substring pruning

**Target: Q21 7.7s -> ~0.3s, Q22 4.6s -> ~0.5s (ClickBench hot run)**
**Complexity: Medium**

Distinct from #25 (value-level bloom filters for equality on text columns).
This targets `LIKE '%pattern%'` queries where the pattern contains a fixed
substring. Dictionary-based pruning (#19) handles dictionary-compressed
columns, but high-cardinality text columns (URL, Referer, Title) use LZ4
and currently require full decompression + scan for LIKE matching.

**Approach:** During compression, for each LZ4-compressed text column,
extract character trigrams from all string values in the segment and build
a per-segment trigram bloom filter. At query time, extract trigrams from
the LIKE pattern's fixed substring (e.g. `'%google%'` → `{goo, oog, ogl,
gle}`) and check against the bloom. A segment is skipped only if ALL
pattern trigrams are absent from the bloom.

This is a well-known technique used by columnar engines as a skip index. On
ClickBench data, 'google' appears in ~0.1% of URLs, so a trigram bloom
would prune ~99% of segments for Q21/Q22.

**Sizing:** Trigram bloom per segment ≈ 2-8 KB (similar to value-level
blooms). 10 bits per distinct trigram, capped at 8 KB. The trigram
alphabet is smaller than the value space, so blooms are compact.

**Pattern extraction:** For LIKE patterns:
- `'%foo%'` → extract trigrams from `foo`
- `'foo%'` / `'%foo'` → same trigram extraction
- Single/two-char patterns → no trigrams, skip optimization
- `_` wildcards → break trigram sequences at wildcard positions

**Storage:** Extend `_blooms` table with a separate trigram bloom alongside
the existing value bloom. Distinguished by a type tag in the packed format.

**Files:** `src/compress.rs` (trigram extraction + bloom build),
`src/bloom.rs` (trigram bloom type), `src/scan/exec/segments.rs` (LIKE
pattern → trigram check during segment loading)

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

### 35. Parallel-safe custom scan paths

**Target: All queries, especially non-aggregate scans**
**Complexity: High**

Currently all DeltaX custom paths set `parallel_safe = false` and
`parallel_aware = false` in `src/scan/path.rs`. This prevents PostgreSQL
from using Parallel Append to distribute partition scans across workers.
On the ClickBench setup (8 partitions, c6a.4xlarge with 16 vCPUs), this
leaves up to 8x parallelism unused.

For DeltaXAgg, internal rayon parallelism compensates within a single
partition, but partitions are still processed sequentially by PG's Append
node. For DeltaXDecompress (non-aggregate queries like Q20, Q21, Q25, Q26),
execution is fully single-threaded — each partition scanned one after
another.

**Approach (incremental):**

1. **Phase 1 — `parallel_safe = true` without `parallel_aware`:** Mark
   DeltaXDecompress and DeltaXAppend paths as parallel-safe. PG's Parallel
   Append then distributes entire partition scans across workers. Each
   worker runs a complete partition scan independently — no shared state,
   no coordination. Requires removing SPI calls from the scan hot path
   (move metadata loading to leader process or use direct heap access).

2. **Phase 2 — `parallel_aware = true`:** Enable within-partition
   parallelism at the PG level. Multiple workers split segments within a
   single partition using shared scan state. More complex but enables
   parallelism even with few partitions.

**Constraints:**
- SPI is not safe in parallel workers. Metadata loading must use direct
  catalog access or be performed in the leader and shared via DSM.
- TOAST detoasting should be safe in parallel workers (uses buffer manager).
- Memory contexts must not be shared across workers.

**Affected queries:** Q20 (24x), Q21 (43x), Q25 (25x) and all queries
that use DeltaXDecompress + PG native aggregation as a fallback. Phase 1
alone would give up to 8x improvement on these.

**Files:** `src/scan/path.rs` (set parallel_safe), `src/scan/exec/segments.rs`
(replace SPI metadata loading with direct heap access),
`src/scan/exec/decompress.rs` (verify no shared mutable state)

### 36. Two-level hash aggregation

**Target: Q16 3.4s -> ~1s, Q28 12.7s -> ~3s, Q31/Q32 10-14s -> ~3s,
Q36 23.1s -> ~5s (ClickBench hot run)**
**Complexity: Medium-High**

Extends beyond #30 (largely addressed by #28). For high-cardinality GROUP
BY (>100K groups), the single hashbrown table exceeds L2/L3 cache, causing
random memory access patterns that dominate execution time.

**Approach:** Partition the hash space into 256 independent sub-tables,
selected by one byte of the hash value (a well-known technique in
columnar engines). Benefits:

1. **Cache locality:** Each sub-table fits in L2 cache during processing.
   A 1M-group table split into 256 buckets ≈ 4K groups per bucket ≈
   128 KB — fits in L2.
2. **Lock-free parallel merge:** Workers claim buckets via atomic
   `fetch_add`. Each bucket merged independently, no synchronization.
3. **Amortized resizing:** 256 small resizes instead of one large resize
   that stalls all processing.

**Conversion threshold:** Switch from single-level to two-level when group
count exceeds a threshold (e.g. ~50K groups or ~256 MB of hash table). Below
the threshold, single-level is faster due to less indirection.

**Integration with existing parallel paths:** The compact and mixed
parallel aggregation paths already produce per-worker partial results.
Two-level hashing improves both the per-worker accumulation (better cache
behavior) and the merge phase (256-way parallel merge instead of serial
hash table union).

**Files:** `src/scan/exec/agg.rs` (two-level wrapper around hashbrown in
compact and mixed aggregation paths)

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

### 38. Reduce per-partition SPI overhead

**Target: All queries, ~20-50ms fixed overhead reduction**
**Complexity: Low**

Every partition scan begins with SPI queries to load segment metadata from
companion tables. With 7-8 partitions, that's 7-8 separate SPI calls, each
with SPI_connect/SPI_finish overhead, plan caching, and executor startup.
For queries where segment pruning eliminates most work (Q2, Q8, Q20, Q41-
Q43), this fixed overhead is a significant fraction of total time.

**Approaches (pick one or combine):**

1. **Batch metadata loading:** Single SPI query with `UNION ALL` across
   all companion table OIDs, partitioned by a discriminator column.
   Reduces N SPI roundtrips to 1.

2. **Direct heap access:** Replace SPI with direct `heap_beginscan` /
   `heap_getnext` on companion tables. Eliminates SPI overhead entirely.
   Already needed for #35 (parallel safety). Uses `table_open` +
   `systable_beginscan` to read companion rows without the SPI layer.

3. **Companion OID caching:** Cache the mapping from parent table OID to
   companion table OIDs across queries in the same session. Eliminates
   the catalog lookup SPI call (currently one per partition per query).

Approach 2 is preferred as it also unblocks #35 (parallel-safe paths
require no SPI in workers).

**Files:** `src/scan/exec/segments.rs` (`load_segments_heap` and metadata
loading functions), `src/catalog.rs` (companion OID lookup)

### 39. Pipelined detoast + parallel aggregation

**Target: Q22 9.6s -> ~5s (ClickBench hot run)**
**Complexity: Medium**

In the DeltaXAgg parallel path, all segments are eagerly detoasted on the
main thread before any parallel work begins. For Q22 this is 2.9s of
single-core TOAST decompression (pglz) blocking 4 parallel workers.

`pg_detoast_datum` is a PG API call that must run on the backend thread —
it cannot be moved into worker threads. The solution is to **pipeline**
detoasting with parallel processing so they overlap in time.

**Approach:**

1. Load all segments lazily (TOAST pointers only, ~100ms).
2. Split segments into B batches (e.g. B = n_workers or 2 * n_workers).
3. For batch 0: main thread detoasts all blobs, then spawns `thread::scope`
   for workers to process batch 0.
4. While workers process batch i, the main thread detoasts batch i+1.
5. When workers finish batch i, they immediately start batch i+1 (already
   detoasted). Main thread detoasts batch i+2, and so on.

This requires a producer-consumer pattern: main thread pushes detoasted
batches into a shared queue, workers pull from it. With `std::thread::scope`
this can be done with a `Mutex<VecDeque<Range<usize>>>` work queue plus
a `Condvar` for notification.

**Expected overlap:** With 2.9s detoast and 3.7s parallel work across 4
threads, the detoast is fully hidden behind parallel processing for all
but the first batch. Net saving: ~2.5s (detoast of first batch ~0.3s
remains serial).

**Constraints:**
- `pg_detoast_datum` must stay on the main thread (PG backend requirement).
- Workers must not touch `SegmentData.toast_pointers` — only read
  `compressed_blobs` after the main thread has detoasted them.
- Each batch's segments must be fully detoasted before workers access them.

**Files:** `src/scan/exec/agg.rs` (pipelined batch loop in compact and
mixed parallel paths), `src/scan/exec/segments.rs` (lazy loading for
agg path)
