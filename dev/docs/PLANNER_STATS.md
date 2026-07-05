# Planner statistics for compressed partitions — roadmap & testing

pg_deltax compresses each partition's heap to (near) empty and stores the data
in companion blob tables, so PostgreSQL's own `ANALYZE` can't sample it. We
therefore **synthesize `pg_statistic` by hand** from the per-segment / per-
partition metadata we already keep (min/max, HLL sketches, value maps, null
counts). `src/stats.rs` is the implementation; `dev/docs/RTABENCH_QUERY_ANALYSIS.md`
§2–4 has the original motivation and the per-query wins.

## Why this is delicate: wrong stats are worse than no stats

With **no** stats the planner is conservative (defaults, no histograms) and
tends to pick safe-but-slow plans. With **confidently wrong** stats it commits
hard to a bad plan. Every regression we hit during this work was wrong/missing
stats, not absent stats:

| Symptom | Root cause | Impact |
|---|---|---|
| Q19 871 s | `event_type='Shipped'` (absent value) estimated `1/ndistinct` not ~0 | NestLoop over a 224K Materialize, ×9255 |
| Q06 17.6 s | parent `nullfrac` hardcoded 0 → `<> ''` on a 98%-NULL col estimated ~100% | per-partition Append + 3.6M-row Sort instead of parent DeltaXAppend Top-N |
| Q30 254 s | `event_created` range estimated `rows=8` vs 4.2M (no histogram) | NestLoop-over-Materialize join flip |
| Q17 6.3 s | parent `order_id` n_distinct too low (MAX heuristic) → join card 45M vs 6.3M | hash-join a 105M build instead of nested loop |
| histogram silently ignored | `stacoll` written as NULL (`Oid::into_datum(InvalidOid)`) | range estimates never improved |
| histogram neutralised | `stanullfrac=1.0` on the order-by col (its `_nonnull_count` read 0) | range selectivity `(1−nullfrac)` → 0 |
| n_distinct = 264 (9 real) | standalone analyze SUMmed per-segment ndistinct | equality selectivity wrong |
| MCV n_distinct = 12 (634 real) | MCV written from an incomplete valmap union | high-card col looked low-card → bad Top-N |
| child n_distinct(order_id) 1.7K (250K real) | direct-backfill path derived per-column ndistinct from the per-segment range-overlap heuristic (`merge_ndistinct` → MAX) instead of the HLL; `order_id` is ordered physically by time, so all 154 segments span the full id range → MAX, not SUM | per-partition `order_id=N` est 2740 vs 18 (×150) on Q07/Q10/Q11/Q13 point lookups |
| conjunction estimated *higher* than either conjunct | `build_deltax_append_path` gated `path.rows` on `rel->rows > 1`; a legitimately selective predicate drives `rel->rows` to ≤1 (PG's floor), which the guard mistook for the unpopulated-`reltuples` default and replaced with the *full unfiltered* companion sum | Q11 (`event_created` range AND `order_id=512`) est 641 667 vs actual 1 |
| ClickBench Q32 9.5 s → 63 s | accurate `n_distinct` correctly flagged `WatchID` as ~unique, which tripped the **high-cardinality GROUP BY bail** in `hook.rs` — calibrated for high-card *text* keys (string-decompression cost) but firing on the *integer* `GROUP BY WatchID, ClientIP`. The bail disabled DeltaXAgg → PG fell back to an external-merge Sort + GroupAggregate (spilling 0.3 GB/worker; HashAgg was worse at 3.5 GB). Fix: gate the bail on text keys only — integer keys pack into compact u128 and aggregate RAM-resident, beating PG even at unique cardinality | ×6.6 on a no-WHERE near-unique integer GROUP BY |

A second lesson, from the last two rows: **improving a stat can expose a latent
gate that was calibrated against the old (wrong) value.** The HLL n_distinct fix
was correct, yet it surfaced both the `rel->rows > 1` guard and the text-oriented
GROUP BY bail. Always re-run *both* RTABench and ClickBench after a stats change —
a win on one workload's estimates can flip a plan on the other.

The lesson: **any new stat we write needs a way to verify it matches reality
before it ships**, because a benchmark only catches it if a query happens to
exercise the bad estimate, and only catastrophically-bad plans are obvious by
eye. See "Testing strategy" below.

## Current state — what we fill

Per compressed partition (`stainherit=false`) and once on the parent
(`stainherit=true`, merged across partitions):

| Field / kind | Written? | Source | Notes |
|---|---|---|---|
| `stanullfrac` | ✅ | child: `_colstats._nonnull_count`; parent: row-count-weighted avg of children | NOT NULL cols forced to 0 |
| `stawidth` | ✅ | `pg_attribute.attlen` | varlena → flat 32 |
| `stadistinct` | ✅ | child: per-partition merged-HLL (both compress paths now persist the HLL-based scalar); parent: cross-partition merged-HLL | the range-overlap `merge_ndistinct` heuristic is a fallback only, for partitions predating `column_hll` |
| MCV (`stakind=1`) | ✅ | `column_valmap` + `column_valcounts` for low-card cols | **real** per-value freqs (`count / row_count`, child + cross-partition merge — P1); uniform `1/n` fallback only for partitions predating `column_valcounts`; written only when the valmap covers all distinct |
| Histogram (`stakind=2`) | ✅ (partial) | per-partition `column_minmax` (parent: per-partition mins) | int/date/timestamp only; child is 2-point |
| `reltuples` / `relpages` | ✅ | `deltax_partition.row_count` | |

## Backlog — gaps, prioritized

Each item: what, why it matters, effort, whether it needs a data **reload**
(stats computed only at compress time can't be back-filled).

### P1 — Real MCV frequencies  ✅ **DONE** (effort: M, reload: yes)
MCV frequencies *were* uniform (`1/ndistinct`) — correct for absent values (~0)
but flat for present values: `event_type='Delivered'` estimated 11% vs 4.6%
actual; `'Approved'` 11% vs 41%. **Shipped:** `compress.rs` / `copy.rs` now
count each low-cardinality value at compress time (cheap — the per-segment value
scan that builds the valbitmap now tallies occurrences instead of just a
presence set) and persist the summed counts as `deltax_partition.column_valcounts`
(`{col: {value: count}}`). `stats.rs` writes `most_common_freqs = count / row_count`
for both the child partition and the cross-partition parent merge (`mcv_freqs`),
falling back to uniform only for partitions predating `column_valcounts`. Because
the freqs sum to the non-null fraction, an absent value still estimates ~0
(`1 − Σfreq − nullfrac → 0`; verified in `var_eq_const`). Reload-only: the counts
are computed at compress time, so existing partitions need a recompress /
`deltax_analyze_table` won't back-fill them (re-analyze re-reads the persisted
counts, so it *is* reload-free once `column_valcounts` exists).

Validated on local RTABench (250K orders): result-set equality held, the Phase E
estimate oracle reported **0 violations** at the 20× threshold, and per-query
warm times showed **no regression** (>1.25× and >5 ms: none) — several skewed-enum
queries improved (Q20 38→4.5 ms, Q17 272→217 ms; total 2289→2076 ms).

### P2 — MCV *and* histogram on the same column  (effort: M, reload: yes — re-scoped)
**Re-scoped after P1.** The original framing ("effort S, reload no") doesn't hold:
on today's data **no single column qualifies for both** an MCV and a histogram —
the MCV source (`column_valmap`) is built only for **text** columns, while the
histogram source (`column_minmax` + `histogram_type_eligible`) covers only
**ordered numeric/temporal** types. The two slot inputs are disjoint by type, so
the two-slot plumbing alone is inert.

Worse, even where both *could* apply, the histogram adds nothing when the MCV is
**complete**: `scalarineqsel` computes range selectivity as
`mcv_sel + hist_sel·(1 − Σmcv_freq − nullfrac)`, and a complete MCV already drives
`Σmcv_freq ≈ 1 − nullfrac`, collapsing the histogram term to ~0. So "MCV +
histogram on the same column" only buys anything when the MCV is **partial** (hot
values in the MCV, the long tail in the histogram) — which needs real per-value
counts to know which values are hot. That's P1, now landed, *and* it needs the
MCV extended to low-cardinality **ordered** columns (so the same column carries
both inputs), which means collecting an integer/temporal valmap+valcounts at
compress time → reload. Sequence it as: extend `column_valmap`/`column_valcounts`
collection to low-card ordered columns (typed MCV `stavalues`, decoupled from the
scan-time valbitmap to keep the hot path untouched), then write a partial MCV in
slot 1 + the existing histogram in slot 2.

### P3 — Finer child histograms  (effort: M, reload: no)
Child histograms are 2-point `[min,max]` (one bucket → uniform assumption). We
have per-segment min/max to build a multi-bucket child histogram (as the parent
already does). Improves range selectivity *within* a partition for skewed
distributions. Lower value — partition pruning + the parent histogram cover most
range queries.

### P4 — `STATISTIC_KIND_CORRELATION` (kind 3)  (effort: S, reload: no)
Never written. PG uses it for ordered/index-scan costing. The leading order-by
column (`order_id`) is physically ~perfectly correlated; we could assert ~1.0.
Marginal here because scans go through custom nodes, not btree index scans —
but could nudge MergeAppend / ordered-path costs.

### P5 — Float histograms  (effort: S, reload: no)
`FLOAT4/8` are excluded from histogram eligibility (decode→Datum precision
hassle), so `satisfaction` (real) gets only `stadistinct`. Low value — the
satisfaction queries hit the metadata fast path.

### P6 — Extended statistics (`pg_statistic_ext`)  (effort: L, reload: n/a)
Multi-column n_distinct + functional dependencies (e.g. `state` ⇒ `country`).
Mostly relevant to the plain dimension tables (`customers`/`products`), which
PG's own `CREATE STATISTICS`/ANALYZE already handle — not pg_deltax's columns.
Little value for `order_events`.

### P7 — Array / range / JSONB element stats (kinds 4–7)  (effort: L, reload: yes)
`event_payload` is JSONB; PG does little with JSONB element stats anyway.
Negligible.

Suggested order: **~~P1~~ (done) → P2 → P3 → (P4/P5 if a query points at them)**.
P6/P7 are unlikely to be worth it.

## Empirical estimate-gap audit (2026-06-04)

A full est-vs-actual sweep over every RTABench + ClickBench query (per-node
`EXPLAIN ANALYZE`, ranked by est/actual ratio with a ≥100-row floor; deltax vs a
plain-PG oracle — `order_events_plain` for RTABench, a `hits_plain` copy for
ClickBench) to find the next stats target. Tooling: `dev/estimate_gap.py`.

**Verdict: no actionable planner-stats fix.** P1 + prior work closed the
deltax-specific estimate gaps that matter.
- **RTABench:** after the row floor, zero deltax-specific large-cardinality
  mis-estimates. Every remaining big error (Q06, Q17, Q16/18/30, Q20) is matched
  by plain PG (intrinsic predicate hardness — `<>''` on NULL, JSON-extract
  selectivity, generic GROUP-BY-output over-estimate); deltax even beats plain on
  Q00/Q15.
- **ClickBench:** one genuinely deltax-specific gap was found — DeltaXAgg clamped
  its GROUP BY output estimate to the *full* table, not the *post-filter* input
  rows that PG's `estimate_num_groups` uses (Q21: 1.95M est vs 10 actual; plain
  nails it). **Fixed** (`hook.rs`: clamp `ndistinct_estimated_groups` to
  `(*input_rel).rows`): Q21's estimate dropped 1.95M → 10.6K on the full 100M-row
  EC2 dataset, now matching plain PG (the residual ~10× is intrinsic
  `LIKE '%google%'` selectivity, no longer deltax-specific); an unfiltered
  `GROUP BY` (Q8) is correctly unchanged. The over-estimate was cosmetic on
  today's benchmarks (DeltaXAgg/DeltaXAppend absorb GROUP BY + ORDER BY + LIMIT
  via TopN pushdown, the only consumer is a trivial `Limit`, and ClickBench has no
  join above the aggregate) — but accuracy matters for real usage where an
  aggregate result feeds a join/CTE. Guarded by
  `test_groupby_estimate_clamped_to_filtered_input` and validated through the
  RTABench (L5 + oracle) and ClickBench correctness gates with no regression.
- **Real ClickBench slowness is detoast-bound, not estimate-bound** — Q21 23.3 s
  (`detoast=22.6 s` for `URL LIKE '%google%'`), Q22 18.6 s (`Title` detoast),
  Q33 4.4 s (`agg` over 23M URL groups). That's an execution-path track (text
  detoast for substring `LIKE`), orthogonal to planner stats.

## Known-inaccurate stats (audit 2026-06-04)

Stats we *write* (or skip) that are known to diverge from ground truth, ranked by
wrong-value × real-usage impact. Several are invisible to the current RTABench /
ClickBench suites (which use `order_by` only, no `segment_by`; and don't probe
text width), so benchmarks will never catch them — they matter for real usage.

| # | Stat | What's wrong | Real-usage impact | Status |
|---|---|---|---|---|
| 1 | ~~`segment_by` columns: no `pg_statistic` at all~~ | `WHERE segkey = X` fell back to `DEFAULT_EQ_SEL` (0.005) | The segment key is *the* dimension users filter/join on (tenant/device id). Was invisible to our benches (neither uses `segment_by`). | **fixed** — `augment_segment_by_stats` reads exact `(segment value, _row_count)` from the meta table → exact `stadistinct` for any type + a real-frequency MCV for text keys; guarded by `test_segment_by_*` |
| 2 | ~~`stawidth` = flat 32 for every varlena/text col~~ | True avg width was unknown; ClickBench `URL`/`Title` are ~50–70 B | Mis-sized sort/hash work-mem + `relpages`/data-volume costs on wide-text aggregates. | **fixed** — `column_stawidth` derives avg char length from colstats `_sum`/`_nonnull_count` + varlena header (child); parent is the row-count-weighted child avg (`load_parent_stawidth`, like nullfrac). Reload-free (`_sum` already holds the text length sum). Guarded by `test_text_stawidth_reflects_avg_length`. |
| 3 | ~~No MCV for >32-distinct skewed columns~~ | valmap overflowed at 32 → uniform `1/ndistinct` for every value | A 100-value enum with a 40%-hot value estimated it at ~0.5%. | **fixed** — a partition-level heavy-hitter summary (`merge_segment_topvals`, cap `MCV_MAX_DISTINCT=2048`) feeds `select_partial_mcv` (PG's `1.25/ndistinct` admission filter, top-100); `stats.rs` writes a **partial** MCV keeping the real HLL `stadistinct` so the tail is still estimated. Persisted as `column_mcv`; reload required. Guarded by `test_high_cardinality_partial_mcv`. |
| 4 | ~~No float histograms~~ | `WHERE x > c` used `DEFAULT_INEQ_SEL` (~0.33) | Range predicates on real-valued columns. | **fixed** — `FLOAT4/8` added to `histogram_type_eligible` (the order-preserving encode/decode already existed). |
| 5 | ~~No correlation stat (kind 3)~~ | never written | Ordered/index-scan costing; marginal for custom scans (no plan impact today). | **fixed** — multi-slot `pg_statistic` writer; correlation ~1.0 written for physically-sorted columns (those with a multi-bucket histogram). |
| 6 | ~~Child histogram 2-point `[min,max]`~~ | within-partition range assumed uniform | Skewed-within-partition ranges. | **fixed** — multi-bucket child histogram from per-segment colstats `_min` (`build_histogram_bounds`), gated on the mins being well-distributed (only the physically-sorted column). |
| 7 | ~~Parent histogram assumes equal-sized partitions~~ | bounds = sorted partition mins + global max, treated equi-depth | Unequal partitions (variable ingest, backfill) skewed range estimates. | **fixed** — parent histogram pooled from per-segment mins across all partitions (`load_parent_segment_mins`); equal-sized segments make it ~equi-depth regardless of partition sizes. |

## Testing strategy

The disasters above were ultimately caught by a full RTABench/ClickBench run —
slow, manual, EC2-bound, and only loud when a plan is *catastrophically* bad. We
need cheaper, automated checks that catch a wrong stat *value* before it ever
reaches a benchmark. Five layers, cheapest first:

### L1 · Unit tests (pure functions) — have, keep expanding
`histogram_eligible`, `merge_ndistinct`, `parent_histogram_bounds`, the HLL
serialize/merge round-trip, etc. Every new stat-computation helper gets one.
Catches encoding/merge logic bugs. Fast, no PG.

### L2 · Stat-value validation — **implemented** (`tests/test_pg_statistic.py`)
Now asserts, against a synthetic table with a known distribution (controlled
25%-null column, a 5-value low-card enum, a **skewed** 4-value enum
(hot 50% / warm 30% / cool 15% / rare 5%), a high-card column, an ordered
column): `stanullfrac` ≈ truth (child + merged parent), the MCV slot's
`stavalues` == the true distinct set with `staop`/`stacoll` non-null (read from
raw `pg_statistic`, since `pg_stats` hides them), the MCV's `most_common_freqs`
match the **real** skewed distribution within 2% on both child and parent (a
regression to uniform `1/n` → 25% each is caught — P1), the histogram slot
brackets the true min/max and is strictly ascending, and an absent-value
equality estimates ~0. The original form below:

A `#[pg_test]` (or python integration test) that:
1. builds a small synthetic table with a **known** distribution (controlled
   null fraction, skewed low-card column with a value that never appears, an
   ordered column, a high-cardinality column),
2. compresses it through pg_deltax,
3. reads back `pg_statistic` and asserts each field is within tolerance of the
   ground truth computed directly from the data:
   - `stanullfrac` ≈ true null fraction (would have caught Q06's 0 / the 1.0 bug),
   - `stadistinct` ≈ true distinct (would have caught 264-vs-9 and 12-vs-634),
   - MCV `stavalues` == the true distinct set, `stacoll`/`staop` non-null & correct
     (would have caught the NULL-`stacoll` and incomplete-valmap bugs),
   - histogram bounds bracket the true min/max and are strictly ascending.

This directly targets the failure mode "a stat is written but wrong," which is
where almost every bug lived. It's the layer we were missing.

### L3 · Estimate-accuracy assertions — **implemented** (`tests/test_pg_statistic.py`)
Compares the planner's estimated rows (`EXPLAIN`) to the ground-truth fraction on
a known table. `test_skewed_value_estimates_track_real_frequency` (with P1)
asserts the hot value estimates ~50% of rows, the rare value ~5%, an absent value
~0, and that the three are monotone with frequency — catching a regression to
uniform `1/n` (~25% for every present value). `test_absent_value_equality_
estimates_near_zero` covers the Q19 absent-value case. `test_in_range_estimate_
is_substantial` asserts a range over the order-by column covering ~half the time
span estimates ~half the rows, not a handful — the **rows=8 collapse** that a
missing/neutralised parent histogram caused (Q30). A generic bounded-ratio sweep
over an arbitrary predicate set, plus a `make … query`-style spot check, are the
remaining niceties.

### L4 · Plain-PG oracle — **implemented** (`tests/bench_rtabench.py`, Phase E)
`make bench-rtabench` loads the *same* data into both `order_events` (pg_deltax)
and `order_events_plain` (plain PostgreSQL, properly ANALYZEd) and already
asserts **result-set equality**. Phase E now also extracts the fact-table scan
estimate from each variant — the deltax `DeltaXAppend`/`DeltaXDecompress` custom
scan vs plain PG's `order_events_plain` scan — plus the deltax run's *actual*
rows, and **hard-fails** any query whose deltax estimate is off by >`N`× from
**both** the actual count **and** plain PG's estimate. The double condition is
what makes it non-flaky: a predicate that's intrinsically hard to estimate (e.g.
Q06's `<> ''` on a mostly-NULL column — both planners over-estimate it the same
way, ×9190 off actual) agrees between the two estimators and is *not* flagged,
while a deltax-specific blunder (plain nails it, deltax is wild) is. Threshold
`N` defaults to 20 via `RTABENCH_EST_RATIO` (0 disables; report still prints).
Aggregate-pushdown queries (DeltaXAgg/Count/MinMax) emit grouped rows, not scan
rows, so they're skipped.

This gate paid for itself on first run: it surfaced the two bugs in the table
above (child `order_id` n_distinct ×150 low; the conjunction-over-estimate guard)
— both pre-existing, both invisible to the result-set check, both now fixed and
guarded against regression.

### L5 · Disaster guard in the benchmark — **implemented** (`tests/bench_rtabench.py`, Phase F)
The bench harness now **fails loudly** instead of relying on a human noticing a
17-second query. Phase F compares each query's warm deltax time to a **pinned**
baseline (`tests/.bench_results/rtabench_baseline.json`) and hard-fails any query
that regressed >`RTABENCH_TIME_RATIO`× (default 3) above a noise floor
(`RTABENCH_TIME_FLOOR_MS`, default 10 ms — the local 250K subset is noisy at
sub-millisecond times). The baseline is pinned and refreshed *deliberately*
(`make bench-rtabench-bless` / `RTABENCH_BLESS=1`), never the previous run, so a
slow drift can't quietly ratchet it upward. Complements Phase E (L4): E catches a
wild *estimate*, F catches the *time* even when the estimate looks fine — the
exact case where a correct, well-estimated stats change flips a plan to a
slower-but-valid one. `n_orders` mismatch or a missing baseline skips the gate
with a note (non-blocking).

**Priority:** L2 (stat-value validation) and L4 (plain-PG oracle) have **landed**
— they gave the most coverage for the least cost and immediately caught two real
bugs. L1 is ongoing (P1 added `mcv_freqs` / `parse_valcounts_json` unit tests).
L3 added both the skewed-frequency estimate test (with P1) and the
in-partition-range "~half the rows, not rows=8" test — a generic bounded-ratio
sweep over an arbitrary predicate set is the only remaining L3 nicety. L5 (the
wall-clock regression backstop, Phase F) has **landed**. All five layers now have
at least their core in place; what remains is incremental (finer L1/L3 coverage,
and adopting the same pinned-baseline gate in the EC2 harness).

## Operational notes (carried over)
- Stats auto-populate on load (COPY/backfill end + background-worker compaction);
  `deltax_analyze_table` is a manual refresh. See RTABENCH_QUERY_ANALYSIS.md §4.3.
- Stats computed only at compress time (HLL n_distinct, real MCV counts in
  `column_valcounts`) need a reload to back-fill; histogram/MCV-from-valmap,
  MCV freqs-from-valcounts, and nullfrac are reload-free once those catalog
  columns exist (re-run `deltax_analyze_table`).
- Don't run a plain `ANALYZE <deltax table>` — it samples the empty compressed
  heaps and clobbers the synthesized inheritance-tree stats.
