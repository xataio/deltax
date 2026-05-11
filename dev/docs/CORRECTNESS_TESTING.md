# Correctness Testing

pg_deltax should treat plain PostgreSQL as the source of truth. The dedicated
correctness harness lives in `tests/correctness/` and compares a regular
PostgreSQL table against a pg_deltax-managed table loaded with the same logical
rows.

## Objectives

- Verify that compressed scans return the same answers as PostgreSQL.
- Exercise planner and executor paths that are easy to break: quals, Top-N,
  aggregate fast paths, JSON extraction, segment pruning, parallel scans, and
  mixed compressed/uncompressed layouts.
- Keep benchmark correctness checks useful, but avoid making benchmarks the
  only reference tests.
- Make failures easy to reproduce and promote into permanent regression cases.

## Harness Model

Each case has:

- A deterministic dataset.
- A deltax physical layout: partition interval, `segment_by`, `order_by`,
  segment size, compression path, and planner GUCs.
- One SQL statement with a table placeholder.
- A comparison policy.

The harness runs the query twice:

1. Against a plain PostgreSQL table.
2. Against the pg_deltax table.

The result rows are then compared using the case's policy.

## Comparison Policies

- `ordered_exact`: rows and order must match. This is the preferred policy.
- `unordered_exact`: row multiset must match. Use for queries without a
  deterministic `ORDER BY`.
- `limit_ties`: row count and overlap checks for non-unique `ORDER BY ... LIMIT`
  cases where PostgreSQL may legally choose different tied boundary rows.
- `float_tolerant`: ordered comparison with a small tolerance for floating point
  aggregates.

Tests should prefer adding deterministic tie-breakers over using relaxed
comparators.

## Initial Coverage Areas

The first expansion should focus on areas where pg_deltax has custom behavior:

- Qual evaluation: equality/range predicates, `IN`, `BETWEEN`, `IS NULL`,
  `LIKE`, nested boolean expressions, casts, and expression quals.
- NULL semantics: compressed columns, segment-by columns, order-by columns,
  aggregate inputs, and `ORDER BY ... NULLS FIRST/LAST`.
- Top-N: ascending/descending order, multi-column order, ties, filters, and
  projected columns not needed by the first Top-N pass.
- Aggregates: `count(*)`, `count(col)`, `min`, `max`, `sum`, `avg`, grouped
  aggregates, `HAVING`, and metadata-fast-path fallback boundaries.
- Joins: deltax table as outer/inner side, semi joins, anti joins, and
  RTABench-shaped dimension joins.
- JSONB: raw JSONB reads, `->`, `->>`, nested paths, casts, missing paths, type
  mismatches, and `pg_deltax.json_extract_mode` A/B tests.
- Storage/codecs: dictionary text, high-cardinality text, booleans, integers,
  floats, timestamps, repeated values, monotonic values, and segment-boundary
  row counts.

## Dataset Plan

Use deterministic generated datasets before large benchmark datasets:

- `tiny_edge`: small handpicked data with NULLs, ties, and extremes.
- `codec_matrix`: columns designed to trigger different compression codecs.
- `partition_edges`: timestamps exactly at partition boundaries.
- `segment_edges`: row counts around `segment_size - 1`, `segment_size`, and
  `segment_size + 1`.
- `rtabench_synthetic`: expanded version of the current RTABench-shaped fixture.
- `jsonbench_synthetic`: JSON-heavy fixture modeled after JSONBench.
- `wide_clickbench_like`: many columns with mixed types but small row counts.

## Implementation Plan

Build this in small sessions. Each session should leave the suite runnable and
should add cases that catch a distinct class of correctness bugs.

### Session 1: Harness Baseline

Status: started.

Dataset:

- `tiny_events`: scalar table with timestamps, ids, nullable segment keys, low
  cardinality text, nullable integers, and nullable floats.
- Layout: daily partitions, compressed via `deltax_compress_partition`, small
  segment size, `segment_by = ['device_id']`, `order_by = ['ts', 'id']`.

Queries:

- `count_all`: `count(*)`.
- `filtered_projection`: time range + nested boolean predicate + deterministic
  order.
- `grouped_aggregate`: grouped `count`, `sum`, `min`, `max` with NULL groups.
- `deterministic_topn`: `ORDER BY ... LIMIT` with explicit tie-breaker.

Purpose:

- Prove the postgres/deltax table-pair model.
- Prove comparators and pytest parameterization.
- Keep `make correctness-smoke` fast and stable.

### Session 2: Predicate Matrix

Dataset:

- Extend `tiny_events` or add `predicate_matrix`.
- Include columns for every common predicate family:
  - nullable integers
  - low-cardinality text
  - high-cardinality text
  - booleans
  - timestamps
  - floats, excluding special NaN/Inf until comparator policy is explicit

Layouts:

- Compressed via `deltax_compress_partition`.
- Small segment size to force multiple segments.
- At least one layout sorted by time and one sorted by a non-time key.

Queries:

- Equality and inequality: `=`, `<>`.
- Ranges: `<`, `<=`, `>`, `>=`, `BETWEEN`.
- NULL predicates: `IS NULL`, `IS NOT NULL`.
- Set predicates: `IN`, `NOT IN`, including cases with NULLs.
- Text predicates: `LIKE 'prefix%'`, `LIKE '%contains%'`, `NOT LIKE`.
- Boolean logic: nested `AND`, `OR`, `NOT`, parentheses.
- Casts in predicates: text-to-int where safe, timestamp/date casts.

Purpose:

- Stress batch qual extraction/evaluation.
- Stress min/max and value-bitmap pruning without relying on plan assertions.
- Catch three-valued-logic mistakes around NULLs.

### Session 3: Ordering and Top-N

Dataset:

- `ordering_edges`: rows deliberately designed with repeated sort keys, NULL
  sort keys, and multiple projected columns not needed for sorting.
- Include enough rows per segment that Top-N can choose winners from many
  segments.

Layouts:

- `order_by = ['ts']`.
- `order_by = ['val', 'ts']`.
- Small and medium segment sizes.

Queries:

- `ORDER BY ts ASC/DESC LIMIT n`.
- `ORDER BY val ASC/DESC NULLS FIRST/LAST, id LIMIT n`.
- Multi-column order with deterministic tie-breaker.
- Non-unique `ORDER BY ... LIMIT` using `limit_ties`.
- `ORDER BY ... LIMIT ... OFFSET ...`.
- Top-N with filters on columns that are not the sort key.
- Top-N projecting columns that should be decompressed only after winner
  selection.

Purpose:

- Exercise the two-pass Top-N path.
- Guard DESC ordering and NULL ordering.
- Make tie relaxation explicit rather than accidental.

### Session 4: Aggregate Matrix

Dataset:

- `aggregate_matrix`: numeric-heavy table with:
  - nullable and non-null integer columns
  - low-cardinality group keys
  - groups with all-NULL aggregate inputs
  - negative values and repeated values
  - float values for tolerance-based aggregate checks

Layouts:

- Segment-by group key where metadata aggregation can apply.
- Non-segment-by group key where fallback should apply.
- Time ranges aligned to full partitions and ranges slicing through partitions.

Queries:

- `count(*)`, `count(col)`.
- `min`, `max`, `sum`, `avg`.
- Multi-aggregate queries in one select list.
- `GROUP BY` one key and multiple keys.
- `HAVING` on aggregate results.
- Aggregate with `WHERE` on segment-by equality.
- Aggregate with `WHERE` on non-key column.
- A/B cases with `pg_deltax.disable_meta_agg_fastpath`.

Purpose:

- Verify metadata fast paths and fallback paths against PostgreSQL.
- Catch NULL aggregate semantics and partial-partition overcounting.

### Session 5: Partition and Segment Edges

Status: done.

Datasets:

- `partition_edges`: timestamps exactly at partition starts and ends.
- `segment_edges`: row counts around `segment_size - 1`, `segment_size`,
  `segment_size + 1`, `2 * segment_size`, and empty partitions.

Layouts:

- Fully compressed partitions.
- Mixed compressed/uncompressed partitions.
- Rows in the default partition.
- Direct compression after regular insert.

Queries:

- Time filters exactly matching partition boundaries.
- Half-open time filters: `ts >= lo AND ts < hi`.
- Filters that include only default partition rows.
- Queries spanning compressed and uncompressed children.
- `count(*)`, projection, aggregate, and Top-N across these boundaries.

Purpose:

- Catch off-by-one partition pruning bugs.
- Verify mixed physical storage produces one logical table result.
- Exercise default partition behavior.

### Session 6: Compression Codecs and Direct Backfill

Status: done.

Dataset:

- `codec_matrix`: columns chosen to trigger:
  - dictionary text
  - high-cardinality text
  - booleans
  - small integers
  - large integers
  - monotonic timestamps
  - repeated values
  - nullable values

Layouts:

- Regular insert followed by `deltax_compress_partition`.
- `COPY ... WITH (FORMAT deltax_compress)`.
- `COPY ... WITH (FORMAT deltax_compress_csv)`.

Queries:

- Full projection ordered by primary deterministic key.
- Predicate checks on each codec-targeted column.
- Grouping on low-cardinality compressed columns.
- Aggregates over integer and float columns.
- Text equality and `LIKE` on dictionary and non-dictionary text.

Purpose:

- Verify all compression/decompression paths return PostgreSQL-equivalent
  values.
- Ensure direct backfill and post-insert compression produce equivalent query
  results.

### Session 7: JSONB

Dataset:

- `jsonbench_synthetic`: JSON-heavy rows modeled after JSONBench:
  - top-level `kind`, `did`, `time_us`
  - nested `commit.collection`, `commit.operation`, `commit.rkey`
  - missing paths
  - JSON null
  - scalar type mismatches: string, number, boolean

Layouts:

- Start with the known-good path from existing JSON tests.
- Add raw JSONB compression only after the current `jsonb` compression panic is
  fixed or explicitly avoided.
- Include `json_extract` specs with one path, several paths, and evolved specs
  across partitions.

Queries:

- Raw JSONB projection.
- `data -> 'key'`, `data ->> 'key'`.
- Nested extraction and casts.
- Missing-path comparisons.
- `jsonb_typeof`.
- `COALESCE`, `CASE`, and expression nesting.
- A/B comparison with `pg_deltax.json_extract_mode = 'none' | 'fields'`.

Purpose:

- Verify JSON rewrite semantics match PostgreSQL.
- Guard synthetic-column pruning and mixed-extract-version behavior.
- Keep raw JSONB compression bugs visible but isolated from scalar smoke tests.

### Session 8: Joins and RTABench Shapes

Status: done.

Dataset:

- Expand the current RTABench synthetic fixture into a reusable correctness
  dataset:
  - customers
  - products
  - orders
  - order_items
  - `order_events_plain`
  - `order_events`

Layouts:

- Direct backfill for `order_events`.
- Small segment size for multiple compressed segments.
- Order by `order_id`, `event_created`.

Queries:

- Dimension joins where deltax table is the large fact table.
- Deltax table as inner and outer side.
- `EXISTS` / `NOT EXISTS`.
- `IN` / `NOT IN`.
- `DISTINCT ON`.
- Last event per order.
- Revenue/category aggregates using joins.
- RTABench query subset promoted from benchmark correctness.

Purpose:

- Cover realistic analytical plans and join interactions.
- Move benchmark correctness ideas into the canonical correctness harness.

### Session 9: Parallel and Planner Mode A/B

Datasets:

- Reuse larger `segment_edges`, `aggregate_matrix`, and RTABench synthetic
  datasets.

Planner modes:

- `pg_deltax.max_parallel_workers_per_scan = 0`.
- `pg_deltax.max_parallel_workers_per_scan = -1`.
- `max_parallel_workers_per_gather` high enough to allow Gather plans.
- `pg_deltax.disable_meta_agg_fastpath = on/off`.

Queries:

- Serial-vs-parallel grouped aggregate.
- Serial-vs-parallel projection with filters.
- Top-N queries that must stay serial.
- Metadata fast path vs fallback.

Purpose:

- Verify planner mode changes do not change results.
- Catch incorrect partial-path or fast-path behavior.

### Session 10: Seeded Generated Queries

Datasets:

- Use `predicate_matrix`, `ordering_edges`, `aggregate_matrix`, and
  `codec_matrix`.

Generator dimensions:

- Projection: `*`, subset, expressions.
- Predicate: none, single predicate, conjunction, disjunction, nested boolean.
- Grouping: none, one key, multiple keys.
- Ordering: none, unique order, non-unique order, NULL ordering.
- Limit: none, `0`, `1`, small N, offset.
- Planner GUCs: serial/parallel, metadata fast path on/off.

Rules:

- Every generated query must have a deterministic seed and case id.
- Every failure should print seed, dataset, layout, SQL, comparator, row counts,
  row samples, and deltax `EXPLAIN`.
- Minimized/generated failures should be promoted into curated suites.

Purpose:

- Expand coverage beyond hand-written cases.
- Discover unexpected interactions between expressions, filters, ordering, and
  physical layout.

## Generated Queries

Generated query coverage should be seeded and reproducible. The generator should
combine dimensions such as projection shape, predicate shape, grouping, ordering,
limit/offset, and planner GUCs. When a generated case fails, save enough metadata
to rerun it and then promote the minimized query into a curated suite.

## Running

```bash
make correctness-smoke
make correctness
```

As the suite grows, `correctness-smoke` should remain CI-friendly. Longer
generated or benchmark-derived checks should live behind separate targets such
as `make correctness-fuzz`.
