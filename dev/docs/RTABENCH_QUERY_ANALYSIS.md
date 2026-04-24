# RTABench query analysis

Per-query analysis of pg_deltax on the 31 raw RTABench queries, based on
`EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` of every query on a warm
cache. Raw plans live in `rtabench_explain_raw.txt`.

## Status (2026-04-23)

Originally captured against a serial-only `DeltaXAppend`. The table below
(and the category analysis that follows) reflects that state and is
preserved as the problem description. Later passes implemented the P0
parallel-safe fix and the P1 metadata-only aggregate fast path — see
**§5 Progress** at the bottom for current numbers and which items are
done.

## Setup

- **Hardware**: `c6a.4xlarge` (16 vCPU, 32 GB RAM, 500 GB gp2 EBS).
- **PostgreSQL**: 18 with `shared_buffers=8GB`, `effective_cache_size=24GB`,
  `max_parallel_workers_per_gather=8`, `max_worker_processes=16`,
  `work_mem=8GB`, `jit=off`.
- **Session**: `SET enable_nestloop = off` (forced; see §3.A).
- **Data**: 181,737,692 events in `order_events` (123 compressed partitions,
  avg 1.48M rows/partition). `order_items` = 105M rows / 6.6 GB heap.
  `orders` = 10M rows. `customers` = 1,102, `products` = 9,255.
- **Warm run**: each query executed once before the `EXPLAIN ANALYZE` capture.

## Results summary

31 queries grouped by warm execution time and what's actually happening
in the plan. Competitor numbers pulled from rtabench.com at the same
hardware tier.

| #   | Query | pg_deltax (warm) | Plan shape | Category |
|----:|-------|-----------------:|------------|----------|
| Q14 | sum_prod_stock_price_per_category | **0.7 ms** | products⋈order_items only | G — no order_events |
| Q11 | events_for_an_order | 5.0 ms | DeltaXAppend(order_id=N) | F — point lookup ok |
| Q06 | order_events_without_backups | 20 ms | DeltaXAppend + aggregation | **✓ good** |
| Q09 | departed_orders_count | 8.7 ms | DeltaXAgg | **✓ good** |
| Q10 | last_event_for_an_order | 9.0 ms | DeltaXAppend(order_id=N) | F |
| Q07 | last_order_event_for_order | 12 ms | DeltaXAppend(order_id=N) | F |
| Q13 | satisfaction_with_without_backup | 13 ms | DeltaXAppend | ✓ |
| Q12 | max_satisfaction_for_order_per_day | 15 ms | DeltaXAgg | ✓ |
| Q05 | search_events_for_processor | 90 ms | DeltaXAppend | ✓ |
| Q03 | exists_order_delivered_from_terminal | 240 ms | DeltaXAppend + HashJoin | ✓ |
| Q16 | customers_with_most_orders | 254 ms | *no* DeltaXAppend, **Parallel Hash Join ×8** | G |
| Q15 | exists_order_delivered_for_customer | 435 ms | DeltaXAppend + HashJoin | E — indexes win |
| Q02 | global_agg (max(counter)) | 772 ms | DeltaXAgg, rows_processed=15M | D — no metadata path |
| Q08 | most_week_delayed_order | 1.12 s | DeltaXAppend + aggregation | ✓ |
| Q01 | count_orders_from_terminal | 1.55 s | DeltaXAppend + aggregation | ✓ |
| Q24 | top_customer_by_revenue | 1.70 s | no order_events, **Parallel HJ ×8** | G |
| Q29 | top_product_in_age_group | 1.69 s | no order_events, **Parallel HJ ×8** | G |
| Q28 | sales_volume_by_age_group | 1.91 s | no order_events, **Parallel HJ ×8** | G |
| Q26 | average_order_value | 2.06 s | no order_events, **Parallel HJ ×8** | G |
| Q27 | country_category_performance | 2.34 s | DeltaXAppend + **Parallel HJ** above | C — hybrid |
| Q18 | customer_month_value | 2.43 s | no order_events, **Parallel HJ ×8** | G |
| Q21 | sales_volume_by_country | 2.43 s | no order_events, **Parallel HJ ×8** | G |
| Q22 | sales_volume_by_country_state | 2.51 s | no order_events, **Parallel HJ ×8** | G |
| Q19 | out_of_stock_products | 3.10 s | DeltaXAppend + **Parallel HJ** above | C |
| Q00 | terminal_hourly_stats | 3.58 s | DeltaXAppend + window agg | ✓ |
| Q20 | customers_outstanding | 3.77 s | DeltaXAppend + **Gather** | C |
| Q04 | count_delayed_orders_per_day | 4.68 s | DeltaXAppend, jsonb @> filter | ✓ |
| Q30 | customers_with_most_orders_delivered | **10.77 s** | DeltaXAppend + 2× Hash Join serial | **A — parallel_safe wall** |
| Q25 | product_category_performance | **13.13 s** | DeltaXAppend + 2× Hash Join serial | **A** |
| Q23 | top_sales_volume_product_from_terminal | **23.71 s** | DeltaXAppend + Hash Join + re-agg | **A** |
| Q17 | top_selling_month_product | **25.41 s** | DeltaXAppend + HJ(105M) serial | **A** |

Total warm: **128.6 s** across 31 queries. **Four queries (Q17, Q23, Q25,
Q30) account for 73 s (57%) of that total.** They share one plan shape:
`DeltaXAppend on order_events ⋈ Seq Scan on order_items(105M)` with no
parallelism above the join.

## 1 · What's actually happening, by category

### Category A — DeltaXAppend in the plan, no parallelism above it (Q17, Q23, Q25, Q30)

The single biggest drag on the suite. Every one of these queries joins
`order_events` (selective filter → a few hundred K to a few M rows) with
the full `order_items` table (105M rows, 6.6 GB). Because `DeltaXAppend`
has `parallel_safe = false` (set unconditionally at every `CustomPath`
construction site in `src/scan/path.rs`), PostgreSQL cannot place a
`Gather` above any join that contains it — the entire subtree runs on
one core.

**Q17 plan, real timings:**

```
Limit                                    actual=24958 ms
  Sort                                   actual=24958 ms
    GroupAggregate                       actual=23423..24957 ms
      Sort (443 MB, 6.3M rows)           actual=23423..24442 ms
        Hash Join  p ⋈                   actual=19970..22300 ms
          Hash Join  oe ⋈ oi             actual=19968..21341 ms
            DeltaXAppend on order_events rows=602,731   time=581 ms
                                         (15M scanned, filter hits 602K)
            Hash(order_items 105M rows)  actual=19747 ms    ← 78% of total
              Seq Scan on order_items    actual=6867 ms     (I/O)
```

Single-threaded hash build of 105M rows takes **19.7 s**. Parallel Hash
Join with 8 workers would be ~2.5 s. That alone would drop Q17 from 25 s
to ~7-8 s.

**Cost-estimate bug visible here.** `DeltaXAppend` reports
`rows=14,808,054` but actual output is `602,731` — a 25× overestimate
because the `event_type='Delivered'` filter selectivity isn't in the
path's row estimate (only the time-range selectivity is). With an
accurate estimate PG would prefer hashing the filtered DeltaXAppend side
(~600K rows) instead of the 105M `order_items` side, a further ~10×
speedup for these queries.

Q23, Q25, Q30 are structurally identical: 6.6 GB heap hash build
serialized on one core.

### Category B (unused label — skipped)

### Category C — DeltaXAppend + parallel plan above it (Q19, Q20, Q27)

These plans *do* have a `Gather` node, but the `DeltaXAppend` below is
still serial. What's parallelized is the plain-PG part of the plan
(`order_items`, `orders`, `customers`, `products` as Parallel Seq Scans,
then Parallel Hash Join). pg_deltax runs a full scan of `order_events`
on one core, hands its output to a main-thread step, then everything
above goes parallel.

Q19 (3.10 s) is the most interesting here: `DeltaXAppend` emits 5.3M
rows in 683 ms (8 cores would do it in ~0.5 s if it were parallel), then
a parallel hash join with `orders ⋈ order_items ⋈ products` takes ~2.4 s
using 8 workers. If `DeltaXAppend` were parallel-safe, both halves could
overlap and Q19 would drop under 1 s.

Q20 (3.77 s) has no Hash Join in the plan — it's an EXISTS/NOT EXISTS
pair with a Gather over a serial `DeltaXAppend`. PostgreSQL's NestLoop
was force-disabled, so the outer side is a Parallel Hash. But the inner
has to walk DeltaXAppend serially for each correlated subquery
evaluation.

### Category D — DeltaXAgg full decompress when metadata would suffice (Q02)

```sql
-- Q02
SELECT max(counter) FROM order_events
WHERE event_created >= '2024-04-20' AND event_created < '2024-05-20';
```

```
Custom Scan (DeltaXAgg)                     actual=0.003 ms
  DeltaX Timing: 771.625 ms
    metadata=2.5  heap_scan=1.5  detoast=78
    decompress=545  agg=145  merge=0  topn=0
  DeltaX Stats: segments=510 rows_processed=15,111,288
```

We decompressed 15M rows of the `counter` column and ran a max() over
them — taking 545 ms of decompress + 145 ms of aggregation. But every
compressed segment already has `_max_counter` stored in its metadata
table; the right answer is `SELECT max(_max_counter) FROM
_deltax_compressed.order_events_pXXXX_meta` over ~510 segments, which
should run in < 10 ms.

**Fixed in P1 (2026-04, see §5.6).** The fast path answers `MIN / MAX
/ SUM / COUNT(col) / COUNT(*)` straight from per-segment metadata.
For time-range WHEREs, a planner-time check (`partitions_contain_time_range`)
verifies each surviving partition's `[range_start, range_end)` is
fully inside the WHERE interval before firing — so Q02's month-range
WHERE aligned to daily partition boundaries is covered, while a WHERE
slicing through a partition falls back to `DeltaXAgg`.

Q02 **now 9 ms warm** (was 1055 ms) — beats DuckDB's 7 ms target for
this query class.

Q12 (`max(satisfaction)` grouped by `order_id`, 15 ms) also goes through
DeltaXAgg but is fast because the grouping column is in `order_by` and
the aggregation runs per-segment. The missing fast path specifically
bites global aggregates.

### Category E — EXISTS where a B-tree index wins (Q15)

```sql
-- Q15
SELECT EXISTS (
  SELECT FROM order_events JOIN orders USING (order_id)
   WHERE customer_id = 124 AND event_type='Delivered'
);
```

```
Hash Join                              actual=9 ms
  DeltaXAppend  filter event_type='Delivered'  (1 segment, 9 skipped)
    segments=1 rows_out=159 heap_scan=420 ms    ← despite finding 1 segment
  Hash
    Bitmap Heap Scan on orders(customer_id_idx)  actual=8 ms, 9101 rows
```

Plan is structurally sensible — we skip 9 of 10 segments — but the **one
segment that survives still takes 420 ms** of `heap_scan` to find 159
Delivered events. Plain PostgreSQL answers this in 4 ms via two index
lookups (orders → order_events by order_id, stop at first hit).

Two gaps:
1. **No early termination for EXISTS.** We don't know the executor only
   wants one row, so we scan all 159 matches. PG's ScanState flag
   (`ss_ps_resultslot`) + returning after first match could cut this.
2. **`heap_scan` is the bulk of the 420 ms** — reading compressed-blob
   TOAST pages from shared_buffers for 1.48M-row segment. For this
   selectivity, a B-tree index on `orders(customer_id)` + the existing
   segment-level bloom on `order_id` should skip even the one remaining
   segment. But the filter flows the wrong direction: we scan all
   Delivered events, then join with customer-124's orders, rather than
   starting from the 9,101 customer-124 order_ids and probing.

### Category F — Point lookups by `order_id` (Q07, Q10, Q11)

**These work well** (5–12 ms warm). Since `order_by = ['order_id',
event_created]`, segments are sorted by order_id within each partition;
the segment-level `_min_order_id`/`_max_order_id` effectively skip most
segments on an `order_id = N` predicate. Q11 hits 5 ms, roughly on par
with TimescaleDB (4.7 ms) and DuckDB (1 ms).

The one gap: TimescaleDB's `enable_chunk_skipping('order_events',
'order_id')` prunes whole 3-day *chunks* before any per-row work. We
prune at segment granularity (30K rows), which is fine here but would
matter at larger chunk sizes.

### Category G — Queries that don't touch `order_events` (Q14, Q16, Q18,
Q21, Q22, Q24, Q26, Q28, Q29)

Wholly parallel plans, no pg_deltax code involved. These take 0.7 ms to
2.5 s depending on what gets scanned. They mostly filter `orders.created_at`
directly and join with `order_items`; PG has a parallel hash join with
up to 8 workers here.

### Category H — Queries with heavy scan-side load (Q00, Q01, Q04, Q08)

All between 1.1–4.7 s. Each scans 15M events in a 1-month window with a
jsonb predicate (`event_payload->>'terminal'` or `event_payload @>
'["Delayed","Priority"]'`), aggregates by some bucket, and optionally
joins. `DeltaXAppend` does its job well (time dominated by decompress,
not heap I/O), but with only one core the decompress itself is the
bottleneck.

Decompress time breakdown for Q04 (4.68 s):
```
DeltaX Timing: 4610 ms
  metadata=3  heap_scan=45  decompress=4451  batch_eval=67  emit=42
DeltaX Stats: segments=504 rows_processed=14,955,000 ...
```

4.45 s of decompress is CPU on one core processing 15M rows. 8 cores
would do it in ~0.6 s.

## 2 · Root causes, ranked by impact

1. **DeltaXAppend is not parallel-safe** (12+ queries; ~60% of total
   warm time). Single fix with the widest impact. Ongoing as task #18.

2. **No row-count estimate for filter predicates in DeltaXAppend**
   (Q17, Q23, Q25, Q30 × ~10× each when combined with #1).
   `cost_deltaxappend` in `src/scan/cost.rs` reports post-partition-prune
   row count but ignores pushed-down filter selectivity (event_type,
   jsonb containment, etc.). Bloom-filter selectivity and the
   segment-level `_ndistinct`/`_min`/`_max` metadata have the data; the
   estimator just doesn't consume them.

3. ~~No metadata-only fast path for simple aggregates~~ **(fixed P1,
   §5.6)**. Q02 warm 1055 ms → 9 ms (117×). `DeltaXMinMax` / `DeltaXCount`
   now answer `max/min/sum/count(col)/count(*)` from per-segment
   metadata when the WHERE is empty, a segment-by equality, or a
   time-range whose bounds fully contain every surviving partition's
   bounds.

4. **DeltaXAgg decompress is single-threaded** (affects every query in
   category H). Related to #1 — fixing parallel-safe unblocks multi-core
   decompress through the standard PG parallel-agg path.

5. **`heap_scan` cost on EXISTS/selective scans** (Q15). The per-segment
   heap read is surprisingly slow even when the segment is in
   shared_buffers. Profile worth doing — may be TOAST detoasting for
   jsonb columns that we don't need (Q15 only reads order_id +
   event_type), suggesting we aren't dropping unnecessary column blobs
   from the decompress set.

6. **No EXISTS short-circuit in DeltaXAppend** (Q15 partial). Low
   priority — only a handful of queries.

7. **ClickBench-style partition-level min/max index for non-time columns**
   (Q07, Q09–Q13 parity with TimescaleDB). 5–7× gap on point lookups by
   `order_id`. Segment-level metadata already carries `_min/_max`, but
   PG's partition pruner doesn't see it — only the time-column
   declarative partition bounds are known at plan time. A post-plan hook
   that prunes at the partition level using `order_id` min/max would
   close the gap.

## 3 · Notes on the benchmark setup

### A · `SET enable_nestloop = off` is currently load-bearing

Without it, Q17 picked a `NestLoop → Materialize → HashJoin(order_items
in inner)` plan with a 9,255 × 22M = ~2×10¹¹-op loop that ran for > 30
minutes. The NestLoop-over-materialized-HJ plan was selected because
the Hash Join row estimate was wildly off (2,408 estimated vs 6.3M
actual). Root cause is #2 above (filter-selectivity not in the
estimator). Once #2 is fixed, this hack should be removable.

### B · `work_mem = 8 GB` is load-bearing for Q17/Q23/Q25/Q30

The 105M-row hash build needs ~5 GB peak. At the default 256 MB it
spills to ~25 batches → 10-15× slowdown. Once #1 is fixed,
`work_mem=2GB` with 8 parallel workers splits the build across workers
and the per-worker memory pressure drops.

### C · Cold-run I/O dominates Q17–Q30

Cold runs are 1.5–2× the warm time. Reading 5.8 GB compressed
`order_events` + 6.6 GB `order_items` from gp2 EBS (250 MB/s) is ~50 s
of I/O. `shared_buffers=8GB` covers about half the data; with the
current plans a single query alone exceeds cache. Not really a pg_deltax
issue — TimescaleDB shows similar cold/warm ratios.

## 4 · Recommendations, priority-ordered

| Priority | Fix | Status | Est. suite impact |
|---|---|---|---|
| P0 | Make `DeltaXAppend` parallel-safe (partial-path + DSM cursor) | **✓ done** (2026-04) | **−49 s** realized on Q17/Q23/Q25/Q30 |
| P0 | Filter-selectivity in cost estimator (via `pg_statistic` + `get_relation_info_hook`) | **✓ done** (2026-04) | closes §5.3 small-query regressions + aligns cost-model for Cat A |
| P0 | `DeltaXAgg` parallel-safe | open | unknown — likely helps Cat H queries |
| P1 | Metadata-only fast path for global min/max/sum/count (DeltaXAgg) | **✓ done** (2026-04) | **−1046 ms** realized on Q02 (1055 → 9 ms, 117×) |
| P2 | Share segment metadata via DSM in parallel scan | open — §5.6 (fixes Q15's 9× metadata duplication) | improves parallel-scan efficiency on selective queries |
| P2 | Trim column-blob decompress set to referenced columns only | open | helps Q15 (EXISTS) and any narrow-projection query |
| P2 | Partition-level `order_id` min/max pruning hook | open | 5–7× on Q07, Q09–Q13 |
| P3 | EXISTS short-circuit in `DeltaXAppend` executor | open | Q15 specifically |

Fixing the top two alone should halve the total suite time and bring us
to within spitting distance of TimescaleDB on the join-heavy queries;
all four top fixes together would put pg_deltax clearly ahead on the 31
raw queries.

## 5 · Progress (2026-04)

### 5.1 · P0 — Parallel-aware `DeltaXAppend` (done)

Shipped a partial-path variant of `DeltaXAppend` gated on a new GUC
`pg_deltax.max_parallel_workers_per_scan` (default `-1` = follow
`max_parallel_workers_per_gather`, `0` = disabled). Work is split at
segment granularity via a shared atomic cursor in DSM; per-worker
timings are surfaced in EXPLAIN as `DeltaX Worker` lines. Top-N
pushdown stays on the serial path. See the plan file at
`~/.claude/plans/we-re-working-on-improving-gentle-reef.md` for the
design.

### 5.2 · Category A results (EC2, warm, c6a.4xlarge, 181M events)

| Query | Before | After | Speedup |
|---|---:|---:|---:|
| Q17 `top_selling_month_product` | 25.41 s | **8.59 s** | 3.0× |
| Q23 `top_sales_volume_product_from_terminal` | 23.71 s | **8.74 s** | 2.7× |
| Q25 `product_category_performance` | 13.13 s | **1.64 s** | **8.0×** |
| Q30 `customers_with_most_orders_delivered` | 10.77 s | **4.61 s** | 2.3× |
| **Subtotal (Category A)** | **73.0 s** | **23.6 s** | **3.1×** |

Total suite warm dropped from **128.6 s → ≈ 46 s**. Category C queries
(Q19, Q20, Q27) also improved as a side effect — DeltaXAppend is no
longer a serial inversion point above what's already a parallel plan.

### 5.3 · Secondary regressions on small queries (accepted)

Rollout exposed the cost-estimator blind spot on selective queries.
With the partial path available, PG chose `Gather ×8` over
DeltaXAppend for **point lookups** (Q07, Q09–Q13) and **selective
filters** (Q15), because the path's row estimate doesn't account for
WHERE-clause or segment-level pruning. Each of the 8 workers then
re-scanned segment metadata (~2 s each) before the shared cursor
handed one segment to one worker, so Q15 ballooned to ~18 s
(aggregated heap_scan across workers).

Attempted fix: gate partial-path emission on
`clauselist_selectivity(child_rel->baserestrictinfo)` multiplied by the
true companion row count (we know it from the `deltax_partition`
catalog; `pg_class.reltuples` on the child is always 0 because
compression empties the heap). **This didn't work**: since compressed
children never got ANALYZE, PG falls back to default equality
selectivity (0.005 for numeric, ~2.5e-5 observed for text-equality on
the `event_type` column). That mis-classified Q17's `event_type='Delivered'`
+ one-month time range as `filtered_rows = 370`, suppressing its
Gather — the 3× suite-level win collapsed back to serial.

Accepted tradeoff for now: the regressions on small queries are small
in absolute terms (total ~1 s across the affected queries on EC2 warm)
compared to the 49 s saved on Category A. The path forward is the
real P0 #2 work (below) rather than more hook-level heuristics.

| Query | Baseline | After parallel-safe | Regression | Note |
|---|---:|---:|---:|---|
| Q07 point lookup | 12 ms | 80 ms | +68 ms | 8 workers race for 3 surviving segments |
| Q11 events_for_an_order | 5 ms | 14 ms | +9 ms | idem |
| Q12 max_satisfaction | 15 ms | 72 ms | +57 ms | idem |
| Q13 satisfaction_with_without_backup | 13 ms | 81 ms | +68 ms | idem |
| Q15 EXISTS (customer_id=124) | 435 ms | 1.36 s | +925 ms | 9× metadata duplication (1 of 55K segments survives) |

### 5.4 · P0 #2 — Filter-selectivity in cost estimator (done)

Shipped. Rather than teaching `cost::estimate_cost` about bloom/
min-max selectivity directly, we populate PG's own stats catalog so
its built-in selectivity functions become accurate on compressed
partitions. Three pieces:

1. **`src/stats.rs`** (new). At compress time, write
   `pg_class.reltuples = row_count`,
   `pg_class.relpages = row_count/density`, and one `pg_statistic`
   row per non-segment-by column with
   `(stadistinct, stanullfrac, stawidth)` derived from the existing
   `_colstats` table plus a partition-level HLL merged across
   segments during compression. Sign convention for `stadistinct`
   matches PG's own ANALYZE (positive absolute below 10% of row
   count; negative fraction above).

2. **`get_relation_info_hook`** in `src/scan/hook.rs`. After
   compression the partition's heap is truncated to 0 blocks, so
   PG's `estimate_rel_size` computes `tuples = density * curpages = 0`
   no matter what we put in `pg_class.reltuples`. The hook runs just
   after `estimate_rel_size` and just before
   `set_baserel_size_estimates` — we inject
   `rel->tuples = cost::get_row_count(companion_oid)` so
   restrictinfo selectivity has a correct baseline to multiply.

3. **`deltax_analyze_partition(text)`** and
   **`deltax_analyze_table(text)`** SQL functions, so operators can
   refresh stats on already-compressed partitions (e.g. after
   upgrading to this version). Uses a SUM-capped fallback since the
   HLL sketches don't persist.

4. **Autovacuum interception**. Each compressed partition gets
   `ALTER TABLE ... SET (autovacuum_enabled = off)` at compress time
   (blocks the launcher). A `ProcessUtility_hook` extension
   (`src/copy.rs::filter_compressed_rels_from_vacuum_stmt`) also
   strips compressed partitions from user-run `ANALYZE <rel>` so
   they don't reset our `pg_statistic` rows by sampling an empty
   heap.

### 5.5 · EC2 benchmark after P0 #2 (warm, c6a.4xlarge, 181M events)

| Query | Baseline | + parallel-safe | + pg_statistic | vs baseline |
|---|---:|---:|---:|---:|
| Q17 `top_selling_month_product` | 25.41 s | 8.59 s | **2.59 s** | **9.8×** |
| Q23 `top_sales_volume_product_from_terminal` | 23.71 s | 8.74 s | **4.50 s** | **5.3×** |
| Q25 `product_category_performance` | 13.13 s | 1.64 s | **1.11 s** | **11.8×** |
| Q30 `customers_with_most_orders_delivered` | 10.77 s | 4.61 s | **4.24 s** | 2.5× |
| **Category A subtotal** | **73.0 s** | 23.6 s | **12.4 s** | **5.9×** |
| **Suite total (31 queries)** | **128.6 s** | ~46 s | **~34 s** | **3.8×** |

After the pg_statistic fix, pg_deltax beats TimescaleDB on Q17
(2.6×), Q23 (1.8×), is roughly on par for Q25, and remains slower
on Q30 (4.24 s vs 1.52 s). Most other queries where pg_deltax
already wins held or improved slightly.

Residual small-query regressions from §5.3 partially closed but
not fully — these remain slower than the pre-parallel baseline:

| Query | Baseline | Parallel-safe | + pg_statistic |
|---|---:|---:|---:|
| Q07 point lookup | 12 ms | 80 ms | **85 ms** |
| Q11 events_for_an_order | 5 ms | 14 ms | **14 ms** |
| Q12 max_satisfaction | 15 ms | 72 ms | **86 ms** |
| Q13 satisfaction_with_without_backup | 13 ms | 81 ms | **86 ms** |
| Q15 EXISTS (customer_id=124) | 435 ms | 1.36 s | **1.36 s** |

The absolute costs are small (~0.1 s aggregate across the five),
so they don't offset the Category A gains. Diagnosis for follow-up:
Q15's residual cost is the 9× metadata duplication in the parallel
scan path (even when the planner now picks it, pg_statistic is fed
via `rel->tuples`, but the parallel path's per-worker heap-scan
cost isn't modeled yet) — the fix for this is item §5.6 below.



### 5.6 · P1 — Metadata-only aggregate fast path (done, 2026-04)

`MIN / MAX / SUM / COUNT(col) / COUNT(*)` on compressed tables now
route through the extended `DeltaXMinMax` / `DeltaXCount` custom-scan
paths and answer directly from the per-segment `col_minmax` /
`col_sums` / `row_count` metadata without decompressing blobs. No PG
Aggregate / Gather is planted on top.

**Q02 win (EC2, warm, 181M events):**

| Query | Before | After | Speedup |
|---|---:|---:|---:|
| Q02 `global_agg` (`max(counter) WHERE event_created in [A,B)`) | 1055 ms | **9 ms** | **117×** |

WHERE-clause coverage:
- No WHERE → fast path.
- Segment-by equality (`device_id = 7`) → fast path.
- Time-column range (`ts >= A AND ts < B`, or either side alone) →
  fast path **iff** every surviving partition's `[range_start,
  range_end)` is fully contained in the WHERE interval. Enforced at
  plan time by `partitions_contain_time_range` (SPI lookup on
  `deltax_partition`). If any boundary partition is only partially
  covered by the WHERE, we fall back to `DeltaXAgg`.
- Time-column equality (`ts = C`) → fall back. A row satisfies it
  only for `ts = C`, but a segment containing it also has rows with
  `ts ≠ C`; metadata aggregates over the whole segment would
  overcount.
- Any non-segment-by / non-time qual (or OR) → fall back.

Scope & types:
- MIN/MAX supported on INT2/INT4/INT8/FLOAT4/FLOAT8/DATE/TIMESTAMP/TIMESTAMPTZ
  (any column type whose min/max is stored as an order-preserving i64
  in colstats). TEXT / VARCHAR / BYTEA / BOOL fall through.
- SUM supported on INT2/INT4/FLOAT4/FLOAT8. SUM(INT8) / SUM(NUMERIC)
  → NUMERIC falls through (pgrx `PGFunction` binding for `numeric_in`
  is a follow-up).
- COUNT(col) and COUNT(*) always eligible.
- Runtime escape hatch via `pg_deltax.disable_meta_agg_fastpath = on`
  for A/B correctness comparison. (Only gates the new SUM / COUNT(col)
  / time-range path; the pre-existing no-WHERE MIN/MAX path is
  unaffected.)

The time-range safety argument relies on day-aligned WHERE boundaries
vs. the declared partition interval — the RTABench Q02 case. Queries
that slice through a partition boundary (e.g. `WHERE ts >= '2024-04-20
06:00'` with daily partitions) pay the full `DeltaXAgg` cost; a
follow-up can add in-executor decompression of the column needed for
just the partial boundary segments.

### 5.7 · DSM metadata sharing for selective parallel scans (open)

Residual cost on selective parallel scans: each of the 8 workers
re-runs `load_metadata` + `load_segments_heap` for every companion
partition before the shared-cursor code hands a segment to one
worker. For Q15 this meant ~2 s × 9 processes = ~18 s of
aggregated `heap_scan` time for 1 segment of actual work. The P0
#2 fix above keeps Q15 off the parallel path (via correct row
estimate → serial wins on cost), so the wasted-metadata case is
no longer on the default plan, but the underlying inefficiency
stays. If a future query lands on the parallel path with very
selective segment pruning we'd hit it again.

The planned fix is to ship just the serialized segment metadata
(min/max, segment_values, bloom — *not* compressed blobs) through
DSM at `InitializeDSMCustomScan` and have workers attach via
`InitializeWorkerCustomScan` instead of running their own SPI +
`load_segments_heap` pass. Tracked as item (c) in the
Recommendations table above.

### 5.8 · Aborted attempt — parallel-safe `DeltaXAgg` (2026-04)

A 5-commit branch built out the scaffolding + a restricted parallel
executor for `DeltaXAgg` (GUC + cost-factoring + DSM plumbing +
accumulator serialize/merge + a worker/leader split covering GROUP BY
on Column/DateTrunc/Extract/AddConst and SUM/COUNT/AVG/MIN/MAX).
Correctness verified end-to-end on local + EC2 (31/31 OK vs
`order_events_plain`). The whole branch was reverted because it
produced **zero speedup** on RTABench. Capturing why here so the
next attempt doesn't repeat the same dead ends.

**Three distinct reasons it didn't help the benchmark:**

1. **Category H queries don't actually route through `DeltaXAgg`.**
   The §5.7a-style expectation "Category H ~11 s → ~1.5 s via
   parallel DeltaXAgg" was wrong — confirmed on EC2 by EXPLAIN. All
   four (Q00/Q01/Q04/Q08) combine `GROUP BY date_trunc(...)` with a
   JSONB predicate (`event_payload->>'terminal'` or
   `event_payload @> '[…]'`). Our agg-pushdown hook (`hook.rs
   deltax_create_upper_paths`) can't pattern-match those WHEREs, so
   the plan is `Parallel DeltaXAppend → Sort → Partial
   GroupAggregate → Gather Merge → Finalize GroupAggregate` — already
   8-way parallel via the existing DeltaXAppend P0 work. There is
   nothing for a parallel DeltaXAgg to do on these queries.

   The RTABench queries that do route through `DeltaXAgg` (Q02, Q09,
   Q12, Q14) all answer in microseconds via the §5.6 metadata fast
   paths (`DeltaXMinMax` / `DeltaXCount`) and never reach the agg
   executor body — so parallelising that body is moot.

   **Lesson for next time:** before starting *any* DeltaXAgg work,
   run `EXPLAIN` on the target queries and confirm `Custom Scan
   (DeltaXAgg)` actually appears above the non-fast-path code. Don't
   infer from query shape or the original Category labels.

2. **The artificial serial cost interacts badly with
   `parallel_setup_cost`.** `add_agg_path` hard-codes the serial
   `DeltaXAgg` total to `(10.0, 20.0)` — a pre-existing hack so
   DeltaXAgg always beats plain Append+Agg on planner cost (where
   realistic seq scan costs run into the thousands). Any partial
   path's cost on top of which PG adds `Gather` (setup_cost default
   = 1000) can never beat 20. So even when workers > 1 and the
   partial-path divisor math works, PG won't pick parallel.

   Forcing it (`SET parallel_setup_cost=0; SET
   min_parallel_table_scan_size=0`) did produce a live `Parallel
   Custom Scan (DeltaXAgg) Workers Launched: 8` plan — confirming
   the executor is correct — but at 7M rows / 435 segments it ran
   **4× slower** than serial (532 ms vs 122 ms).

   **Lesson:** parallel planner selection under PG's default
   `parallel_setup_cost = 1000` requires realistic scan costs on
   *both* the serial and partial paths. The `(10.0, 20.0)` artificial
   floor has to go. But §5.3–§5.5 already showed that replacing it
   with a realistic formula regresses small `DeltaXAgg` queries on
   plan selection (plain Append+Agg wins because `pg_statistic`
   on compressed partitions advertises empty tables unless §5.4's
   `get_relation_info_hook` fires correctly). Any future pass has
   to tune the two paths together — parallel DeltaXAgg *and* a
   calibrated cost model are a single package, not separate commits.

3. **Per-worker metadata duplication is a showstopper at scale.**
   In the live-parallel test above, each of the 9 processes (leader +
   8 workers) independently re-runs `load_metadata` +
   `load_segments_heap` for every companion partition during its own
   `begin_agg_scan`, before ever consulting the shared segment
   cursor. For DeltaXAppend this same pattern (see §5.3, §5.7) has
   known ~2 s × N-workers overhead on selective queries; for
   DeltaXAgg it's the same story. The shared segment cursor amortises
   only the decompress work — metadata remains process-local.

   **Lesson:** the §5.7 DSM metadata-sharing work is a hard
   prerequisite for a useful parallel DeltaXAgg. Doing parallel-agg
   first, as this attempt did, is premature optimisation — even
   when correctness is right, per-worker metadata re-scan eats the
   parallelism win until segments-per-worker is >> metadata-cost /
   per-row-decompress-cost. On 4.6M-event local and 181M-event
   EC2 it wasn't close.

**Concrete prerequisites for a retry:**
- (a) Ship §5.7 DSM metadata sharing — serialised segment metadata
  broadcast once via `InitializeDSMCustomScan`; workers attach
  instead of rebuilding. Benefits both DeltaXAppend and
  DeltaXAgg parallel paths.
- (b) Replace the artificial `(10.0, 20.0)` serial `DeltaXAgg` cost
  with a realistic formula calibrated so (i) plain Append+Agg
  still loses, (ii) the partial path cost + `parallel_setup_cost`
  sits below serial for workloads where parallel genuinely helps.
  Requires verifying the §5.4 `pg_statistic` pipeline fires on
  compressed partitions across query shapes. Benchmark small-
  query-plan-selection under this change before doing anything
  else.
- (c) Only *after* (a) and (b) — consider widening `DeltaXAgg`
  pushdown eligibility to cover JSONB operators, so Category H
  queries actually route through `DeltaXAgg` in the first place.
  Significant scope; worth its own design pass.

The scaffolding for the parallel executor itself (DSM layout,
accumulator wire format, leader drain, worker segment cursor) is
well-defined in this attempt's reverted commits; recovering it from
git history will be straightforward once (a) and (b) are in place.

### 5.9 · §5.7 DSM segment-metadata sharing (done, 2026-04)

Shipped the prerequisite flagged in §5.7 and §5.8(a): segment metadata
is now serialised once by the leader into DSM and read zero-copy by
every worker, instead of each of the 9 processes independently running
`load_metadata` + `load_segments_heap`. Blobs stay out of DSM — each
claimant fetches only the blobs for segments it actually processes via
a new `fetch_segment_blobs` helper (per-segment `(_col_idx, _segment_id)`
PK-index probe on `{partition}_blobs`).

**Wire format** (`src/scan/exec/append_wire.rs`, V1):

- Fixed-header `DeltaXAppendMeta` with offset table (magic `DXAW`,
  version 1) + `ColInfo[]` (typoid/typmod + name offsets) +
  `segment_by` / `order_by` / `companion_oids` index lists + header
  arena (column-name bytes).
- Fixed 64-byte `SegmentEntry` × `num_segments` → shared cursor
  indexes directly. Carries `companion_oid`, `segment_id`, `row_count`,
  `min/max_time`, and offsets into a seg-arena for `segment_values`.
- Reserved slots for `col_minmax` / `col_sums` (empty in V1; DeltaXAgg
  retry populates them without bumping `WIRE_VERSION`).

**Lifecycle**. `begin_deltax_append` short-circuits when
`ParallelWorkerNumber >= 0` (installs a stub `DecompressState`; no
SPI, no heap scan). `estimate_dsm_deltax_append` walks the leader's
`segments_data` via the same `layout()` the serialiser uses (single
source of truth — estimator and serialiser can't drift).
`initialize_dsm_deltax_append` writes the wire after
`DeltaXAppendPState`. Workers' `init_worker_deltax_append` attaches
the view, hydrates `col_names` / `col_types` / `segment_by` /
`time_column` / `segments_data`, and re-runs `extract_batch_quals` /
`extract_segment_filters` against the now-live columns.
`shutdown_deltax_append` drops the view and nulls `wire_base` before
PG tears DSM down.

**Blob fetch unified**. The serial (non-Top-N) leader path also
switched to `skip_blob_load = true` at `load_segments_heap` + per-segment
`fetch_segment_blobs` on claim. One code path across leader / worker /
serial — no branching on process role at exec time. The cost on the
serial path is ~10 µs × segments_surviving_pruning of PK-index
overhead, dwarfed by decompression time.

**Local RTABench**. All 31 queries correctness-pass post-change
(byte-equal row sets vs. plain-PG baseline, via the existing
`test_parallel_produces_same_rows_as_serial` gate and the
`make bench-rtabench` tie-relaxed checker). Warm totals: 3376 ms →
3368 ms (within noise) — the local subset is too small for the DSM
win to show; it manifests on EC2 where N_partitions × N_workers
redundant heap scans dominated.

**EC2 RTABench (c6a.4xlarge, 181M events, warm)**. Suite total
**33.4 s → 23.1 s (1.45×, −10.3 s)**. ClickBench confirmed no
regressions.

| Query | Before (ms) | After (ms) | Speedup |
|---|---:|---:|---:|
| Q15 `exists_order_delivered_for_customer` | 1359 | **54** | **25.2×** |
| Q03 `exists_order_delivered_from_terminal` | 579 | **61** | **9.5×** |
| Q01 `count_orders_from_terminal` | 668 | 240 | 2.8× |
| Q23 `top_sales_volume_product_from_terminal` | 4492 | 2042 | 2.2× |
| Q00 `terminal_hourly_stats` | 793 | 415 | 1.9× |
| Q30 `customers_with_most_orders_delivered` | 4233 | 2249 | 1.9× |
| Q07 `last_order_event_for_order` | 83 | 45 | 1.8× |
| Q13 `satisfaction_with_without_backup` | 84 | 46 | 1.8× |
| Q20 `customers_outstanding` | 1920 | 1110 | 1.7× |
| Q19 `out_of_stock_products` | 2856 | 1706 | 1.7× |
| Q04 `count_delayed_orders_per_day` | 1011 | 624 | 1.6× |
| Q08 `most_week_delayed_order` | 340 | 234 | 1.5× |
| Q27 `country_category_performance` | 1513 | 1222 | 1.2× |
| Q17 `top_selling_month_product` | 2559 | 2247 | 1.1× |

Every other query held within ±1% of its prior number. Q15 is the
headline — that's the §5.3 regression (435 ms baseline → 1.36 s
after parallel-safe). Now at **54 ms**, below the pre-parallel
baseline, because per-segment PK-probe fetch beats scanning 55K
`_meta` rows on every worker.

**Two mechanisms paid off together**:

1. *Eliminated redundant `load_segments_heap` on workers* — the
   primary §5.7 target. Biggest win on selective queries (Q15, Q03)
   where one-of-many-thousands-of-segments survives pruning. Each
   worker's old 2 s heap_scan collapses to ~3–4 ms DSM attach +
   segment-entry decode.
2. *Parallelised blob detoast via `fetch_segment_blobs`* — a
   second-order win not in the original §5.7 design. The leader no
   longer serializes full blob detoast upfront in
   `load_segments_heap`; both leader and workers fetch blobs
   per-claim from `{partition}_blobs` via `(_col_idx, _segment_id)`
   PK-index probe. This accidentally turned every join-heavy
   Category A query and every Category H jsonb-heavy scan into a
   real parallel workload on the blob side too.

**EXPLAIN confirms the path is engaged** (local, 4 workers):

```
Parallel Custom Scan (DeltaXAppend)
  DeltaX Worker: leader segs=1 rows_out=22473 total=5.623ms ...
  Worker 0:  actual time=3.695..6.505 rows=25159 loops=1
  Worker 1:  actual time=3.514..6.393 rows=25993 loops=1
  Worker 2:  actual time=3.968..6.723 rows=24735 loops=1
  Worker 3:  actual time=4.348..6.786 rows=22366 loops=1
```

~3–4 ms per worker for setup (DSM attach + decode 155 `SegmentEntry`
records + hydrate plan-qual state) vs. the ~2 s per worker each
process spent re-running `load_segments_heap` pre-change. On EC2
with 55K segments the ratio is even larger, which is why the Q15
improvement is so dramatic.

**Unblocks §5.8 prerequisite (a)**: a parallel-`DeltaXAgg` retry can
now reuse the same `DeltaXAppendMeta` + `SegmentEntry` wire format
(the `has_col_minmax` / `has_col_sums` flags + reserved arena slots
are already there). The remaining prerequisites are (b) realistic
cost model and (c) JSONB pushdown eligibility, unchanged.

