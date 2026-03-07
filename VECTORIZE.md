# Vectorized Execution Plan

## Background

PostgreSQL has no native batch/vectorized executor and won't any time soon. Andres
Freund proposed batch execution patches years ago but they were never merged. PG 17
and 18 focused on I/O (async I/O) rather than CPU-bound vectorization.

Any vectorized processing must happen **inside** the custom scan node. The
`ExecCustomScan` callback still returns one tuple per call — the trick is that all
the heavy work (filtering, aggregation) happens in bulk inside the node. This is
exactly how TimescaleDB does it with their ColumnarScan and VectorAgg nodes.

## Phase 1 — Batch Filtering (PERF_IMPROVEMENTS.md item 3)

**No new dependencies. Highest ROI.**

After decompressing a segment to `current_segment: Vec<Vec<(Datum, bool)>>`, evaluate
simple quals (`=`, `<>`, `<`, `>`, `>=`, `<=`) in a tight Rust loop over the datum
arrays. Build a `BitVec` selection vector. Only call `fill_slot` for rows that pass.

This eliminates PG's per-row `ExecQual` overhead for simple predicates. The full PG
qual is still applied for correctness on complex expressions the batch filter doesn't
handle.

**Estimated impact:** Q2 89→~20ms, Q8 124→~25ms, Q20 71→~10ms

**Files:** `src/scan/exec.rs`, `src/scan/hook.rs` (to extract quals at plan time)

## Phase 2 — Lazy Decompression with Predicate Pushdown (item 4)

**No new dependencies. Amplifies Phase 1.**

Currently all needed columns are decompressed for all rows before any filtering.
Instead: decompress only the filter column(s) first, evaluate the predicate, build a
selection vector, then decompress remaining columns only for matching rows.

For queries where <1% of rows match (Q20 point lookup, Q2 AdvEngineID filter), this
skips most decompression work entirely.

**Estimated impact:** Q20 71→~5ms, Q2/Q8 additive 2-5x on top of Phase 1

**Files:** `src/scan/exec.rs` (segment decompression loop)

## Phase 3 — Aggregate Pushdown (extending existing COUNT/MIN/MAX)

**No new dependencies. Extension of existing pattern.**

Extend `seaturtle_create_upper_paths` to detect SUM/AVG/COUNT patterns beyond the
existing COUNT(*) and MIN/MAX pushdowns. Implement as new CustomScan node variants.

For GROUP BY on segment_by columns, each segment maps to exactly one group key, so
aggregation is straightforward — accumulate per segment, merge by group key.

| Pushdown | Complexity | Queries Improved | Expected Improvement |
|----------|-----------|-----------------|---------------------|
| SUM/AVG (no GROUP BY, no WHERE) | Low | Q3 | 118ms → ~10ms |
| COUNT with WHERE on segment_by | Low-Medium | Q2 variant | Major |
| SUM/AVG/COUNT GROUP BY segment_by | Medium | Q8 | 124ms → ~10ms |
| COUNT with WHERE on non-segment_by | Medium-High | Q2 | 89ms → ~15ms |
| GROUP BY non-segment_by column | High | Q9, Q13 | Moderate |

**Files:** `src/scan/hook.rs`, `src/scan/exec.rs`

## Phase 4 (Optional) — Arrow Compute Kernels

**New dependency: `arrow-rs`. Higher complexity but enables SIMD.**

Refactor decompression to output Arrow arrays (`Int64Array`, `StringArray`, etc.)
instead of `Vec<(Datum, bool)>`. Use `arrow::compute::filter`, `sum`, `min`, `max`,
and comparison kernels for SIMD-optimized batch operations.

This is the path TimescaleDB took (they use an Arrow-compatible internal format).
The advantage is that as more vectorized operations are added, arrow-rs provides
tested, SIMD-optimized implementations automatically.

**Estimated impact on top of Phases 1-3:** Additional 2-3x from SIMD on
aggregation-heavy queries.

**Files:** `src/scan/exec.rs` (decompression output format), new `src/scan/arrow.rs`

## Rejected Approaches

- **Embed DuckDB/DataFusion** — data copy overhead and massive dependency weight
  aren't justified when data is already in PG
- **Wait for PG vectorized executor** — discussed for 10+ years, not close to merging
- **VOPS extension** — requires a completely different data model incompatible with
  our partition-based approach
