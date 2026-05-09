# Parallel-aware DeltaXAgg — implementation plan

## Status

- [x] DeltaXAgg already exists for serial CustomScan-based aggregation (`src/scan/exec/agg.rs`, ~11K lines). Supports COUNT, COUNT(DISTINCT), MIN, MAX, SUM, AVG with optional GROUP BY/HAVING. Internal `std::thread::scope` parallelism via `process_segments_compact` + `merge_compact_results`.
- [x] **Phase A (refactor for reuse)** — much smaller than originally planned because the per-segment work and merge logic were **already extracted** as standalone functions in the existing codebase. Only addition was `load_agg_metadata_from_plan` helper (`agg.rs`) so worker hydration in Phase C can call the same SPI loader the leader does. The thread-scope path's per-segment body is already callable from anywhere as `process_segments_compact(segments, config) -> ParallelCompactResult`; merge as `merge_compact_results(...)`. No further extraction needed.
- [x] **Phase B (DSM hook scaffolding)** — `DeltaXAggPState` struct + `AggTimingShmem` + `MAX_AGG_WORKER_SLOTS` const + 5 stub DSM hook functions all `#[allow(dead_code)]`. `DELTAX_AGG_EXEC_METHODS` still has `None` for DSM slots; `add_agg_path` keeps `parallel_workers = 0`. No behaviour change. Tests + clippy clean.
- [ ] Phase C — workers do real partial-aggregate work; per-segment fast path; tri-state `BatchEval`. **The big remaining chunk** (~3-5 sessions). Sub-tasks:
  - [x] **C.0**: `AggScanState` carries `pscan: *mut DeltaXAggPState` and `is_parallel_worker: bool` (initialised at every construction site). Phase B's `DeltaXAggPState` struct is now reachable from state. Tests + clippy clean.
  - [x] **C.1**: DSM hook bodies are functional. `estimate_dsm_deltax_agg` sizes `DeltaXAggPState`. `initialize_dsm_deltax_agg` zeroes the struct + sets `n_worker_slots`. `reinit_dsm_deltax_agg` resets the cursor + clears timing. `init_worker_deltax_agg` stores `pscan` + flags `is_parallel_worker = true`. `shutdown_deltax_agg` stamps `populated = 1` in the worker's slot. `begin_agg_scan` short-circuits for workers via `build_minimal_worker_state`; `exec_agg_scan` returns EOF for workers. All 5 hooks wired into `DELTAX_AGG_EXEC_METHODS`. `add_agg_path` keeps `parallel_workers = 0` so the path is dormant at runtime — no behavior change.
  - [ ] **C.2**: serialize `ParallelCompactResult` to DSM bytes (compact format). Workers `process_segments_compact` over claimed segments and write the result to their DSM slab. Leader's `exec_agg_scan` first call deserialises every populated slab and feeds `merge_compact_results`. Sub-tasks:
    - [x] **C.2.a**: extend `DeltaXAggPState` with `partial_slab_size: u32` + `partial_lens: [AtomicU64; N]` + `slab_ptr(slot)` helper. `PARTIAL_SLAB_SIZE_BYTES = 32 MB` const. `estimate_dsm_deltax_agg` sizes for `[state][N+1 slabs]`. `initialize_dsm_deltax_agg` populates `partial_slab_size`. `reinit_dsm_deltax_agg` zeroes `partial_lens`. Tests + clippy clean.
    - [ ] **C.2.b**: serialize / deserialize `ParallelCompactResult` (no `cd_sidecar` — Phase D). Fixed-size header + map entries + storage buf + arena buf + optional topk keys. `Result<bytes_written, ()>` API for serialise; `ParallelCompactResult` reconstruction for deserialise. Round-trip unit test.
    - [ ] **C.2.c**: restructure `begin_agg_scan` for parallel-aware mode — leader stops short of `process_segments_compact`, stores `segments_data` + parsed plan + agg/group specs on `AggScanState` instead. Workers' `init_worker_deltax_agg` extends C.1 with metadata-load (re-SPI for now) + plan-parse + spec build.
    - [ ] **C.2.d**: worker `exec_agg_scan` first-call body — claim segments via `next_segment.fetch_add(1)`, run `process_segments_compact`, accumulate into local `ParallelCompactResult`, serialize to slab, write `partial_lens[k]`, set `populated = 1` with Release ordering, return EOF.
    - [ ] **C.2.e**: leader `exec_agg_scan` first-call body — claim segments alongside workers (leader is slot 0), spin-wait for workers' `populated == 1` with Acquire, deserialise each populated slab, merge via `merge_compact_results`, finalize, cache `result_rows`, emit row 0.
    - [x] **C.2.f (partial)**: cost-aware infrastructure in place — eligibility predicate, `recommend_agg_workers` helper in `cost.rs`, `estimate_agg_cost(..., workers)` parallel-divisor adjustment, `path::peek_agg_topn_info`, `agg::can_use_compact_keys_path` re-export. **Runtime activation deferred** — see "C.2 activation follow-up" below.
    - [ ] **C.2.g**: tests in `tests/test_aggregate_pushdown.py` parameterised over `max_parallel_workers_per_gather ∈ {0, 2, 4}`. Pending C.2 activation.

### C.2 activation follow-up (research-state)

The DSM-merge model from C.2.b–e turned out to be a poor fit for PG's `Gather` semantics: with `add_partial_path` + leader_participation=on, the leader process runs `exec_custom_scan` *both* as a "worker" (contributing to DSM) and as the final-emitter (merging worker partials), causing double-counting (RTABench Q9: 5 → 22). With leader_participation=off, workers can't emit final rows because they don't have the merged accumulator. PG core has no hook to express "leader merges, workers contribute, only one final tuple stream comes out".

**Investigated alternative** — split DeltaXAgg into **partial CustomScan + Gather + final-Aggregate**, the standard PG parallel-aggregate model. Each worker's CustomScan emits per-group rows containing PG's `aggtranstype` partial state; PG's standard Aggregate node above (`AGGSPLIT_FINAL_DESERIAL`) combines partials via the aggregate's `aggcombinefn`. Verified empirically:

- PG's `create_upper_paths_hook` is **never** called for `UPPERREL_PARTIAL_GROUP_AGG` (only `UPPERREL_FINAL`, `UPPERREL_GROUP_AGG`, `UPPERREL_WINDOW`, `UPPERREL_DISTINCT`, `UPPERREL_PARTIAL_DISTINCT`, `UPPERREL_ORDERED`). The FDW interface (`fdwroutine->GetForeignUpperPaths`) is the only callback PG fires for partial-group-agg, and we're not an FDW.
- By the time our `UPPERREL_GROUP_AGG` hook fires (planner.c line 4198), `create_partial_grouping_paths` + `gather_grouping_paths` + `add_paths_to_grouping_rel` have already run. `partially_grouped_rel.reltarget` is populated (verified via probe), so we can read PG's partial target without calling the static `make_partial_grouping_target`.
- The chain construction must be done manually: `fetch_upper_rel(root, UPPERREL_PARTIAL_GROUP_AGG, output_rel.relids)` to grab the existing rel, build a partial CustomPath using `partially_grouped_rel.reltarget`, call `create_gather_path` to wrap it, call `create_agg_path(... AGGSPLIT_FINAL_DESERIAL ...)` over the Gather, and `add_path(grouped_rel, ...)` so the planner picks it.
- All three PG functions are exposed through pgrx 0.17 bindings (`create_gather_path`, `create_agg_path`, `fetch_upper_rel`); `GroupPathExtraData` (containing `agg_partial_costs` / `agg_final_costs` / `havingQual`) is the `extra` parameter to our hook.

**Initial scaffolding (this commit)**: `AggSpec` (path-side) and `AggExecSpec` (exec-side) gain `is_partial: bool` + `transtype_oid: Oid`. Default `false` / `InvalidOid` everywhere — no behaviour change. Wired through every constructor in `hook.rs`, `path.rs`, `agg.rs`, `agg_wire.rs` test fixtures.

**Progress so far**:
1. ✅ `compact_emit_partial(storage, group_idx, slot, spec) -> (Datum, bool)` alongside `compact_finalize`. Coverage: Count → int8; SumIntNarrow (SUM only) → int8; SumFloat (SUM only) → float8; MinStr/MaxStr → text. Unsupported branches `unreachable!()`.
2. ✅ `is_partial: bool` + `transtype_oid: Oid` added to `AggSpec` (path-side) and `AggExecSpec` (exec-side). Trailer plumbed through path-private (built by `build_agg_path_private`), through plan-private (re-emitted by `plan_agg_path`), through `parse_agg_private` + `ParsedAggPlan` + `AggExecContext`.
3. ✅ `finalise_compact_into_result_rows` branches on `ctx.is_partial` → calls `compact_emit_partial` instead of `compact_finalize`.
4. ✅ `exec_agg_scan` partial-mode runtime: `run_partial_aggregate_in_process` (every process claims segments via the shared DSM cursor, accumulates locally, emits per-group partial rows; PG's Gather and the Final Aggregate above combine). No DSM slabs, no leader-merge.
5. ✅ `add_agg_partial_path` in `path.rs` builds the partial CustomPath, calls `add_partial_path(partially_grouped_rel, ...)`, manually wraps in `create_gather_path` + `create_agg_path(AGGSPLIT_FINAL_DESERIAL)`, and adds the result to `grouped_rel.pathlist`. Wired into `deltax_create_upper_paths` after the existing complete `add_agg_path` call.
6. ✅ Eligibility predicate. `quals_reference_only_numeric_vars(jointree.quals)` walks the WHERE clause via `pull_var_clause` and rejects any Var with non-numeric `vartype`. This solves the bug from the first attempt (RTABench Q9 over-counting on text WHERE) — Q9 now correctly falls back to the complete CustomScan path. Plus `agg_specs_partial_emittable` and `can_use_compact_keys_path` cover agg / group eligibility.
7. ✅ Validation:
   - **RTABench**: 31/31 correct vs plain PG. Total 3933ms → 3300ms (1.19x — matches pre-C.2 baseline; partial path activates only on numeric-only queries; per-query fluctuation in the single-digit ms range is run-to-run noise).
   - **ClickBench**: 43/43 correct. Total 1272ms → 1199ms (0.94x — slightly faster). No regressions >20% on queries with pre-C.2 timing >5ms.
   - **JSONBench**: pending EC2 run (target Q3/Q4 3.2s → ~0.8s; runs are real-$$ so save for a focused validation session).

**Coverage today** is intentionally narrow: partial path activates only when (a) no HAVING / Top-N / DISTINCT, (b) all aggs are Count / SUM(int4/float) / MIN/MAX(text), (c) all group keys are integer-packable, and (d) all WHERE-clause Vars are numeric (int / float / timestamp / date / bool). Most ClickBench / RTABench queries have text WHEREs (URL, SearchPhrase, event_type, etc.) so they fall back to the existing complete path — correctness preserved, no parallel speedup yet.

**Follow-ups to broaden coverage**:
- SUM(int8) / AVG via `int8_avg_serialize` for the partial state's `internal` transtype.
- Text WHERE clauses by mirroring the serial parallel-mixed code path inside `process_segments_compact` (or a sibling).
- HAVING in the partial path (Phase E).
- COUNT(DISTINCT) for dict-encoded text (Phase D — fundamentally different from PG's combinefn model; needs the dictionary union approach in PARALLEL_AGG.md Phase D).

  - [ ] **C.3**: extend `evaluate_batch_quals` (`batch_qual.rs:495`) to return tri-state `BatchEval { AllPass, AllReject, Selective(Bitmap) }`; add `enum SegmentEval { FullMetadata, PerSegmentByGroup, PerRow }` so per-segment metadata (`col_minmax / col_sums / row_count`) can short-circuit decompression for full-segment cases.
  - [ ] **C.4**: gate `add_agg_path` for parallel (`parallel_safe = true, parallel_workers = recommend_agg_workers(...)`). Update `cost::estimate_agg_cost` so the planner picks the parallel variant on large inputs. Add `tests/test_aggregate_pushdown.py` (parametrize over `max_parallel_workers_per_gather ∈ {0, 2, 4, 8}`) for correctness, and an EXPLAIN assertion that DeltaXAgg picks `parallel_workers > 0` on Q3/Q4-shaped queries.
- [x] **JSON-extract chain eligibility for DeltaXAgg path** (out-of-band — not in the original plan). Without this, the upper-paths classifier rejected JSONB chain Exprs in agg args, GROUP BY, tlist projections, and WHERE quals, so DeltaXAgg never got a path for any json_extract query. Q1 went from 49s (PG `Gather Merge → external sort → GroupAggregate`) to 2.1s warm (`Custom Scan (DeltaXAgg)` direct, internal rayon). Q0 1.1s → 285ms. Phase D is no longer load-bearing for Q1 — see "JSON-extract chain eligibility" section below.
- [~] Phase D — parallel `COUNT(DISTINCT)` for dictionary-encoded text columns. **Infrastructure landed; high-card extension deferred.** `Bitset`, `DictDistinctRemap`, `build_dict_distinct_remaps`, `CdKind::DictBitset`, and the `process_segments_mixed` insert/merge/finalise switch are all in place + tested. Sequential leader pre-pass with a 250K-string ceiling keeps it dormant on JSONBench Q1 (`x_did` has 3.6M unique values — bypasses the bitset by design); follow-up parallelises the pre-pass + raises the ceiling to actually deliver the Q1 → ~1.6s win. See "Phase D" section below for details.
- [ ] Phase E — HAVING in the parallel path.
- [ ] Phase F — EXPLAIN, GUCs, hardening.

### JSON-extract chain eligibility for DeltaXAgg path ✅ DONE

Discovered while investigating why JSONBench Q1 stayed on the `Gather Merge → external sort → GroupAggregate` plan even after the partial+Gather+FinalAgg activation. Three classifier blockers and one runtime-correctness bug were silently keeping `add_agg_path` from firing for any json_extract query (and would have produced wrong results if it had).

The post-`standard_planner` walker rewrites chains in upper plans for `DeltaXDecompress`/`DeltaXAppend` cscans, but `deltax_create_upper_paths` runs *during* `standard_planner` — it sees raw JSONB chain Exprs (e.g. `data->>'did'`) before the walker has a chance to substitute synthetic Vars. JSON_EXTRACT.md §P4 calls this out as the known interaction blocker.

**Shipped** (all in `src/scan/hook.rs` + `src/scan/path.rs` + a shared helper in `src/scan/json_extract.rs`):

1. **`json_extract::AggChainCtx`** — lazy planner-side context: walks `simple_rte_array` for the inh parent, loads `json_extract` specs via `load_extract_specs_for_rel_pub`, builds `PhysicalCols`, gates on `is_json_extract_safe_for_rel` (mixed-partition gate). Exposes `match_to_synthetic(node) → Option<(col_idx, type_oid)>` where `col_idx = physical_count + spec_index` (matches `MetadataInfo::col_names` layout from `load_metadata`).

2. **Agg-arg classifier** (in `deltax_create_upper_paths`'s aggregate loop) — refactored to a labeled block; chain-match attempt comes first, falls back to the existing `Var / RelabelType / length(Var) / Var+Const` shapes. Chains map to `AggSpec { col_idx, col_type_oid: kind_to_type_oid(spec.target_kind), expr_kind: AggExpr::Column, ... }`.

3. **GROUP BY classifier** — chain-match attempt at the top of the per-clause loop, emitting `GroupByExpr::Column` with the synthetic position. Skips setting `group_by_relid` (parent's `pg_attribute` doesn't expose synthetic attnos), so the ndistinct heuristic falls back to the pathlist row estimate for synthetic-only group queries. Loading synthetic ndistinct from `deltax_partition.column_ndistinct` is a follow-up.

4. **`non_agg_op_exprs` validator** (the projection-vs-GROUP-BY equivalence check) — accepts a chain Expr in tlist when it matches a `GroupByExpr::Column` synthetic; otherwise falls back to the existing `AddConst`-shape match. Without this, every Q0/Q1-style query with a chain `event` projection bailed because `data->'commit'->>'collection'` is a T_OpExpr but not a `Var + Const`.

5. **WHERE qual validator** — `(Var, Const)` shape check now accepts `(chain, Const)` and `(Const, chain)` too, taking the type from the matched spec. The validator just confirms the shape is pushable; the actual rewriting happens in (6).

6. **`plan_agg_path` qual rewriter** — calls `json_extract::rewrite_chains_in_list` on `qual_list` before `nodeToString` serialisation. **Correctness-critical**: chains end up in DeltaXAgg's `custom_private` (not in `scan.plan.qual` — the post-planner walker can't touch them). At exec time `extract_batch_quals` only recognises `Var` nodes; unrewritten chains would be silently dropped → wrong WHERE results. Rewriter reuses the existing `rewrite_walker` infrastructure (produces `Var(INDEX_VAR, k)`); `extract_batch_quals` keys off `varattno` only and doesn't care that the varno is `INDEX_VAR`.

**Validation** (m6i.8xlarge, 100M rows, warm):

| Q  | Before  | After   | Plan after change |
|----|--------:|--------:|-------------------|
| Q0 | 1.1s    | 285ms   | `Custom Scan (DeltaXAgg)` (was `Gather Merge → GroupAggregate`) |
| Q1 | **43s** | **2.1s** | `Custom Scan (DeltaXAgg)` direct |
| Q2 | 17s     | 17s     | unchanged — EXTRACT(HOUR) per-row, separate gap |
| Q3 | 3.2s    | 3.2s    | unchanged — `MIN(timestamp + interval * cast)` Aggref shape rejected by classifier |
| Q4 | 3.6s    | 3.6s    | unchanged — same Aggref shape blocker as Q3 |

Total warm 68s → ~26s (~2.6×). ClickBench (43 q): 1199 → 1215 ms (within run-to-run noise, no chain Exprs so the new branches are dead code). RTABench (31 q): 3300 → 3319 ms (same). All 216 integration tests pass, including all 5 JSONBench correctness tests.

**Why Phase D is no longer load-bearing**: the existing internal-rayon path at `agg.rs::run_grouped_aggregate` (or wherever the grouped CountDistinct emit loop now lives) handles Q1 in 2.1s once it actually runs — the `did` per-segment dictionaries keep the per-row `HashSet<u128>` insertions cheap, and the rayon thread-scope chunking spreads the work. Phase D's bitset-OR optimisation would still cut another ~1s, but at 2.1s vs ClickHouse's ~1s the bigger lever for total bench time is Q2/Q3/Q4.

**Remaining JSONBench-specific work** (not strictly part of this plan, but the next obvious gaps):

- **Q3/Q4 — complex Aggref args**: `MIN(TIMESTAMP WITH TIME ZONE 'epoch' + INTERVAL '1 microsecond' * (data->>'time_us')::BIGINT)`. The agg-arg classifier today only accepts plain `Var`, `RelabelType(Var)`, `length(Var)`, or `Var + Const`. After json_extract, the inner chain becomes a synthetic Var, but the surrounding arithmetic (`epoch + interval * cast`) is still a non-trivial expression tree the classifier rejects. Two options: extend the classifier to recognise specific timestamp-arithmetic patterns (narrow), or emit the inner Var as the agg's storage column and apply the arithmetic in `output_map` finalisation (broader, more aligned with how the executor materialises group keys).
- **Q2 — EXTRACT(HOUR) per-row**: the `EXTRACT(HOUR FROM TO_TIMESTAMP(... / 1000000))` group key currently runs per-row above the scan. Already partially anticipated by `GroupByExpr::Extract` in the executor; the gap is plumbing it through the json_extract chain matcher.

## Context

After json-extract, JSONBench warm timings on m6i.8xlarge were: Q0 1.1s, Q1 43s, Q2 17s, Q3 3.2s, Q4 3.6s. ClickHouse is ~1s on each. After the chain-Expr eligibility work above, Q0 dropped to 285ms and Q1 to 2.1s; Q2/Q3/Q4 still on the old plan, total bench warm 68s → ~26s. The notes below preserve the original premise (single-threaded final aggregation as the dominant cost) since it still applies to Q3/Q4 and motivates Phase D's design — even if Q1 was solved by a different lever.

Original premise (still valid for Q3/Q4): the dominant remaining cost is **single-threaded final aggregation above the parallel scan**: COUNT(DISTINCT) on Q1 (now solved), MIN/MAX on Q3/Q4. Workers do parallel scan + parallel sort fine; PG's `GroupAggregate` runs single-threaded above the `Gather Merge` boundary because partial COUNT(DISTINCT) results can't be combined without re-deduping.

ClickHouse's `uniqExact` (the function JSONBench Q1 uses) is **exact** (hash set), not HLL. The gap is parallel hash-set construction + merge that PG core can't do for `COUNT(DISTINCT)`. pg_deltax can: each worker builds a local hash table per group, leader unions, finalizes. For dictionary-encoded text columns (e.g. the `did` synthetic), workers union segment dictionaries directly without per-row work.

`DeltaXAgg` already pushes the entire aggregation into a CustomScan that replaces `Aggregate → Scan`. **But it is `parallel_safe = false` and has no DSM hooks** — PG runs it single-process. The fix is to make it PG-parallel-aware along the same axes `DeltaXAppend` already is: DSM region, atomic segment-claiming cursor, per-worker partial state, leader-side merge, output emitted at the leader after `Gather` shutdown. The internal rayon path stays as a leader-only fallback.

Original expected outcome: Q1 43s → ~3s, Q3/Q4 3s → ~0.8s, total bench warm 68s → ~22s. Actual after the chain-Expr eligibility work: Q1 → 2.1s (already past the Phase D target, no parallelism needed), Q3/Q4 unchanged (waiting on a different blocker — see "Remaining JSONBench-specific work" above), total ~26s. The remaining ~4s gap to the original 22s target lives in Q3/Q4's complex Aggref args, not in COUNT(DISTINCT).

## Approach

Six phases; each commit compiles and leaves all current tests passing. Realistic effort ~5-7 working days end to end; phases land as independent PRs.

### Phase A — Refactor `begin_agg_scan` ✅ DONE (mostly already done)

The original plan called for extracting four functions out of `begin_agg_scan`'s 5113 lines so worker code could call them. The good news from exploring the existing codebase: `process_segments_compact(segments, &ParallelCompactConfig) -> ParallelCompactResult` and `merge_compact_results(...)` are **already standalone functions** — the internal `std::thread::scope` path calls them. The only gap was metadata loading, since worker hydration shouldn't re-run SPI per worker.

Shipped:

- `load_agg_metadata_from_plan(companion_oids: &[Oid]) -> (MetadataInfo, u64 metadata_us)` in `agg.rs`. Used by `begin_agg_scan` today; Phase C's `init_worker_deltax_agg` takes a different path (deserialise from `append_wire`) but produces the same `MetadataInfo`.

Existing extractions Phase C will reuse without modification:

- `process_segments_compact(segments, config) -> ParallelCompactResult` (`agg.rs:8007`) — per-segment work loop.
- `merge_compact_results(global_*, worker_*, agg_specs)` (`agg.rs:8326`) — merge worker partials into a global accumulator.
- `ParallelCompactConfig` (`agg.rs:7974`) and `ParallelCompactResult` (`agg.rs:7991`) — the config/result types are stable.

Tests + clippy clean. 216 integration tests pass.

### Phase B — DSM hooks scaffolding ✅ DONE

Already shipped:

- `DeltaXAggPState` struct with `next_segment: AtomicU64`, `total_segments: u64`, `n_worker_slots: u32`, `worker_timings: [AggTimingShmem; MAX_AGG_WORKER_SLOTS]`. POD; zero-init is valid.
- `AggTimingShmem` struct mirroring `ScanTimingShmem` from decompress.rs — agg-specific counters (`segments_decompressed`, `rows_in`, `rows_filtered_qual`, `groups_emitted_local`, `hash_probe_us`, `accum_update_us`, `distinct_union_us`, `partial_serialize_us`).
- 5 stub functions: `estimate_dsm_deltax_agg`, `initialize_dsm_deltax_agg`, `reinit_dsm_deltax_agg`, `init_worker_deltax_agg`, `shutdown_deltax_agg` — each errors with "not yet implemented" if invoked unexpectedly.
- `DELTAX_AGG_EXEC_METHODS` still has `None` for DSM slots; `add_agg_path` keeps `parallel_workers = 0`. Stubs are dormant.

### Phase C — Workers do real work for non-DISTINCT, non-HAVING aggregates — ~1-2 days

When the path has no `COUNT(DISTINCT)` and no `HAVING`, set `parallel_workers = recommend_agg_workers(...)` (new helper in `cost.rs`, returns 0 for `total_segments < 16`).

Wire `DELTAX_AGG_EXEC_METHODS` DSM slots to point at non-stub implementations:

- `estimate_dsm_deltax_agg` — `size_of::<DeltaXAggPState>() + append_wire::layout(...).total_size + per_worker_partial_slab(nworkers)`.
- `initialize_dsm_deltax_agg` — leader populates `DeltaXAggPState`, serialises metadata via `append_wire::serialize_into` (same wire format `DeltaXAppend` uses; carries `col_minmax / col_sums / row_count / segment_values`), zeroes per-worker partial slabs.
- `reinit_dsm_deltax_agg` — reset cursor + slab lens.
- `init_worker_deltax_agg` — worker hydrates `AggMetadata` from `DeltaXAppendView`, rebuilds `batch_quals` via `extract_batch_quals`, rebuilds `segment_by_filters / time_min / time_max` via `extract_segment_filters`, rebuilds `needed_cols` from `custom_private`. Mirror of `init_worker_deltax_append`.
- `shutdown_deltax_agg` — serialise `WorkerAggState` into the worker's DSM slab, write timing into `worker_timings[slot]`, snapshot partial-result handles before DSM teardown (`state.cached_partial_handles`). Same lifetime pattern as `shutdown_deltax_append`.

`run_segment_partial_loop`: claim segments via `state.pscan.next_segment.fetch_add(1)`, apply filters, build local `WorkerAggState { groups: HashMap<GroupKey, AggAccum>, ... }`. Body verbatim from the existing rayon-internal closure; only the cursor source changes.

Leader's `exec_agg_scan` first call: deserialise all partial states from DSM, run `merge_and_finalize`, cache results in `state.result_rows`, emit row-by-row.

**Per-segment fast path** (Risk 6.10 below): classify each segment via `enum SegmentEval { FullMetadata, PerSegmentByGroup, PerRow }`. When `FullMetadata`, COUNT(*) / SUM / MIN / MAX use `seg.col_minmax` / `seg.col_sums` / `seg.row_count` directly without decompressing. Requires extending `evaluate_batch_quals` (`batch_qual.rs:495`) to return tri-state `BatchEval { AllPass, AllReject, Selective(Bitmap) }` instead of always producing a row mask. ~30 LOC; useful in DeltaXAppend too.

**Expected JSONBench impact**: Q3 3.2s → ~0.8s, Q4 3.6s → ~0.8s. Q0/Q2 unaffected.

### Phase D — Parallel COUNT(DISTINCT) for dict-encoded text ✅ INFRASTRUCTURE DONE (sequential pre-pass; high-card extension deferred)

**Shipped**:
- `agg.rs::Bitset` — `Vec<u64>` with `set / or_with / count_ones`. `count_ones` lowers to POPCNT/CNT.
- `agg.rs::DictDistinctRemap` — leader-built `(global_count, per_segment[seg_idx][local_id] = global_id)` per dict-eligible CountDistinct(text) spec.
- `agg.rs::build_dict_distinct_remaps` — sequential leader pre-pass. Eligibility gate uses `dict_size_sum > PHASE_D_MAX_GLOBAL_FOR_BITSET (250K)` as a fast upper bound on global cardinality, so high-cardinality columns bail without LZ4-decompressing dict entries.
- `CdEntry { kind: CdKind, ..., bitsets: Vec<Bitset> }` — third variant alongside `Int / Str`. `count(group_idx)` helper dispatches; `union_from` handles bit-OR; `write_counts_to_storage` handles `count_ones`.
- `text_col.rs::SegTextColumn::dict_local_id(row)` — returns the local dict ID for `Dict`-encoded columns; workers use it (instead of `get_str + hash128_str`) to look up `global_id` and set the bit.
- `process_segments_mixed` — accepts `chunk_offset`; classifies CountDistinctStr inserts via `config.dict_distinct_remaps.get(&spec_idx)`; falls back to `insert_str` when the spec wasn't pre-built.
- 4 new tests in `test_meta_agg.py`: low-card text (bitset path), high-card user_id (HashSet fallback), WHERE-filter interaction, NULL-aware semantics. All 220 tests pass.

**JSONBench impact**: zero change. Q1's `x_did` has 3.6M unique values — well past the 250K threshold — so the bitset path is correctly skipped and Q1 stays on the existing HashSet<u128> path at 2.1s warm. ClickBench / RTABench within run-to-run noise. The infrastructure is in place; lifting it onto Q1 needs the high-card extension below.

**Deferred — high-cardinality bitset path**:

The bitset itself works fine for ~3.6M unique strings (~450 KB per group, ~115 MB across 16 groups × 16 workers). The blocker is the **leader pre-pass cost**: walking 27M dict entries through a global `HashMap<String, u32>` is ~1s sequential — a wash with the savings on Q1's 885ms agg+merge. Two follow-up steps to make this useful for Q1:

1. **Parallelise the pre-pass**. Each worker builds a per-thread local `HashMap<String, u32>` over its segment chunk, then a sequential merge step assigns global IDs and rewrites per-segment local→thread-local IDs to global. Mirrors `process_segments_mixed`'s chunking. Estimated: parallel phase ~50ms, sequential merge ~250ms, parallel remap rebuild ~50ms — ~350ms total. With Q1 currently at 885ms agg+merge, switching to bitset (workers ~30ms, merge OR ~15ms) plus the parallel pre-pass nets to ~400ms total → Q1 → ~1.6s.
2. **Raise `PHASE_D_MAX_GLOBAL_FOR_BITSET`** from 250K to ~10M once the parallel pre-pass lands. The cap-on-eligibility logic stays the same; only the threshold value changes.

(The current 250K threshold is a deliberate "ship infrastructure first" choice — it keeps the sequential pre-pass cost bounded. Tests cover the bitset path on low-card columns; the same code paths fire when the threshold is raised.)

Original Phase D plan (archived for reference):

Load-bearing piece for Q1. Each segment's dictionary already enumerates the distinct values in that segment.

**Architecture revision (after exploring the existing code in this repo)**:

Phase D was originally specced around the DSM-parallel infrastructure. After the C.2 activation lessons, Phase D is independent of the partial+Gather+FinalAgg path — PG core has no `aggcombinefn` for COUNT(DISTINCT) so the partial path can't carry it. Phase D uses **internal rayon** (one PG process, multiple threads inside it) — same model as today's `all_count_distinct` rayon path at `agg.rs:5398–5660` (which only fires for ungrouped DISTINCT-only queries with no WHERE). JSONBench Q1 has GROUP BY + COUNT(*) + COUNT(DISTINCT), so it doesn't hit that fast path; it goes through a slower grouped CountDistinct emit loop that's currently single-threaded.

**Two orthogonal optimisations** (both needed for the full Q1 win):

1. **Bitset instead of HashSet** for dict-encoded text (~10×). Today's grouped CountDistinct path stores per-group hashes as `HashSet<u128>`. Replace with a `BitVec` indexed by a **leader-precomputed global string ID** so per-row work collapses to a single bit-set.
2. **Parallelism across segments** via rayon (~4–6×). Today's grouped CountDistinct path is single-threaded; the existing `all_count_distinct` path's `std::thread::scope` chunking shows the shape to mirror.

Both together: 43s → ~3s warm for Q1.

**Algorithm**:

1. **Eligibility detection** (in `begin_agg_scan` after segments load): for each `COUNT(DISTINCT col)` target, check ALL relevant segments have `col` in `CompressionType::Dictionary` or `DictionaryLz4`. If yes → bitset path; otherwise → direct HashSet fallback (today's behaviour).
2. **Leader pre-pass**: walk all eligible segments' dict blobs via `compression::dictionary::parse_header` (zero string allocations — borrows the dict entries from the input buffer). Build a global `HashMap<&str, u32>` interner per dict-eligible column. Build `Vec<Vec<u32>>` per-(column, segment) local-→-global remap tables.
3. **Per-segment work** (rayon thread): decode each row's dict ID → look up `global_remap[col][seg][local_id]` → set that bit in the per-group `BitVec`. **No per-row string work.**
4. **Merge** (rayon collect): union `BitVec` pairwise (`a | b`). Cost: O(global_id_count / 8) per merge, not O(distinct_count).
5. **Finalise**: `bitset.count_ones()` per group.

**`DistinctAcc` enum** (replaces today's `CdEntry.sets_int` / `sets_str` split):

```rust
enum DistinctAcc {
    TextDictBitset(BitVec),         // dict-encoded text — load-bearing for Q1
    TextDirectSet(HashSet<u128>),   // raw text fallback (current behaviour)
    Int64Set(HashSet<i64>),         // integer DISTINCT (current behaviour)
    F64BitsSet(HashSet<u64>),       // float DISTINCT (future)
    Bool(u8),                        // bitset of {seen_false, seen_true}
}
```

NULL handling: `COUNT(DISTINCT col)` excludes NULLs. Dict NULL sentinel filtered before bit-set insertion. Direct sets skip NULLs (today's behaviour).

**Concrete edits**:

| Where (file:approx-line) | Change |
|---|---|
| `agg.rs::AggScanState` (~line 399) | Add `dict_global_interner: Option<HashMap<String, u32>>` and `dict_local_remaps: Vec<Vec<u32>>` (per col × per segment). |
| `agg.rs::begin_agg_scan` after segments loaded (~line 2041) | New eligibility predicate + leader pre-pass building interner + remap tables. |
| `agg.rs::CdEntry` (~line 8157) | Refactor `sets_int` / `sets_str` to `accs: Vec<DistinctAcc>` per group. `insert_*` dispatches by variant. |
| `agg.rs` grouped CountDistinct emit loop (the path Q1 takes — ~line 5480 area) | When dict-eligible, set bits via `global_remap` lookup; otherwise fall back to today's `hash128_str` + `HashSet` path. |
| `agg.rs::merge_compact_results` (~line 9530) | Bitset OR for `TextDictBitset`; existing `union_from` semantics for other variants. |
| `agg.rs::compact_finalize` CountDistinctStr branch (~line 8646) | `bitset.count_ones()` for `TextDictBitset`; `set.len()` for the rest. |
| `agg.rs::process_segments_compact` (~line 9350) | The `unreachable!("CountDistinctStr in compact parallel worker")` becomes reachable when dict-eligible (uses bitset, no string ops). |
| Parallelism | Mirror `all_count_distinct`'s `std::thread::scope` chunking (`agg.rs:5398–5660`) for the grouped path so segments process in parallel too. |
| Tests | Extend `test_meta_agg.py` with `COUNT(DISTINCT dict_text)` shapes — single-segment, multi-segment, NULL excluded, GROUP BY + DISTINCT, merge correctness. Unit `#[pg_test]` for `DistinctAcc::TextDictBitset` OR + `count_ones`. |

**What's unrelated (don't touch)**: C.2 activation, partial+Gather+FinalAgg path, `add_agg_partial_path`, `compact_emit_partial`. Phase D fits entirely within the existing complete-aggregate CustomScan; the partial-mode runtime stays inert for COUNT(DISTINCT).

**Validation gates**:
- Unit: `DistinctAcc::TextDictBitset` OR + `count_ones` (no PG fixture needed).
- Integration: extend `test_meta_agg.py` with COUNT(DISTINCT) on dict-encoded text shapes.
- Local correctness: same query results vs `pg_deltax.disable_meta_agg_fastpath=on` baseline.
- ClickBench (43 queries) + RTABench (31 queries) regression sweep — must stay correct, no >20% regression.
- JSONBench Q1 EC2: target 43s → ~3s warm.

**Expected JSONBench impact**: Q1 43s → ~3s warm. Remaining gap is dict-union itself (fundamental).

### Phase E — Re-enable HAVING in the parallel path — half a day

HAVING is already supported in serial DeltaXAgg. Trivial extension: `merge_and_finalize` runs on leader after parallel reduction; HAVING filter just runs against the merged accumulator before finalizing. No change in workers. Lift the gating in `add_agg_path` to allow parallel even with HAVING.

### Phase F — EXPLAIN, GUCs, hardening — half a day

1. EXPLAIN renders per-worker stats (segments claimed, rows in/filtered, hash probes, distinct unions, partial-merge µs). Mirror `explain_deltax_append`.
2. GUC `pg_deltax.parallel_agg_partial_state_mb` (default 64) — per-worker DSM slab cap. On overflow, spill to per-worker tuplestore (Risk 6.4 below).
3. GUC `pg_deltax.disable_parallel_agg` (default off) — escape hatch.
4. Internal-rayon path becomes the leader-only fallback when `parallel_workers = 0`. Don't delete it.

## Critical files

- `src/scan/exec/agg.rs` — primary work site (refactor + DSM struct + worker hooks + per-segment fast path).
- `src/scan/exec/decompress.rs` — read for templates only (`DeltaXAppendPState`, `flush_timing_to_shmem`, `init_worker_deltax_append`, `current_worker_slot`).
- `src/scan/exec/append_wire.rs` — `WireInput / layout / serialize_into / DeltaXAppendView::attach` — reused unchanged.
- `src/scan/exec/segments.rs` — `SegmentData` and its `col_minmax / col_sums / row_count` fields drive the per-segment fast path.
- `src/scan/exec/batch_qual.rs` — extend `evaluate_batch_quals` to return tri-state `BatchEval`.
- `src/scan/path.rs::add_agg_path` (~line 1010) — flip `parallel_safe = true`, set `parallel_workers` via `recommend_agg_workers`.
- `src/scan/cost.rs::estimate_agg_cost` — adjust to make parallel variant cheaper than serial on large inputs so the planner picks it.
- `src/scan/hook.rs::deltax_create_upper_paths` (~line 1293) — already gates eligibility; add `is_parallel_safe` check on upper-rel pathtarget. Also hosts the `AggChainCtx`-driven chain-Expr branches in the agg / GROUP BY / non_agg_op_exprs / WHERE qual classifiers (see "JSON-extract chain eligibility" section).
- `src/scan/path.rs::plan_agg_path` — for json_extract queries, calls `json_extract::rewrite_chains_in_list` on `qual_list` before `nodeToString`, so chains in DeltaXAgg's `custom_private` deserialise as Vars at exec time.
- `src/scan/json_extract.rs` — `AggChainCtx::from_root` / `match_to_synthetic` (planner-side helper), plus the existing `rewrite_chains_in_node / _in_list / rewrite_walker` pipeline reused for qual rewriting.
- `src/scan/explain.rs` — Phase F render.
- `src/compression/dictionary.rs` — `decode_dict` reused for the leader-side global interner pre-pass.

## Reuse list

| Existing | Where | Used for |
|---|---|---|
| `SegmentData` + `col_minmax` + `col_sums` + `row_count` | `scan/exec/segments.rs:544` | Per-segment fast path; no per-row work for full-segment cases. |
| `extract_batch_quals` + `BatchQual` | `scan/exec/batch_qual.rs:26, 495` | Workers run quals identically to `init_worker_deltax_append`. |
| `extract_segment_filters` | `scan/exec/segments.rs` | Cheap segment pruning before claim. |
| `DeltaXAppendPState` pattern | `scan/exec/decompress.rs:224` | Template for `DeltaXAggPState`. |
| `next_segment.fetch_add(1, Relaxed)` | `decompress.rs:3031` | Verbatim segment-claim primitive. |
| `MAX_WORKER_SLOTS` const | `decompress.rs:216` | Same cap (`MAX_AGG_WORKER_SLOTS` references it). |
| `current_worker_slot` + `flush_timing_to_shmem` | `decompress.rs:240, 252` | Move to a shared `scan::exec::dsm` module so both append and agg use them. |
| `append_wire::WireInput / layout / serialize_into / DeltaXAppendView::attach / decode_segment` | `scan/exec/append_wire.rs` | Reused unchanged. Wire format already carries everything agg workers need. |
| `decode_dict` + segment dict blob layout | `compression/dictionary.rs` | Leader-side global interner pre-pass; worker dict-bitset union. |
| `MetaAggKind / MinMaxAggSpec / AggExecSpec / GroupByColSpec / OutputEntry / HavingFilter / ParsedAggPlan` | `scan/path.rs:616, 640`; `scan/exec/agg.rs:316, 408` | Spec vocabulary unchanged; Phase A just lifts merge/finalize out. |
| `register_custom_scan_methods` | `path.rs:101` | Already registers `DELTAX_AGG_SCAN_METHODS` for worker-side plan-node deserialization. |

## Tests

Add `tests/test_aggregate_pushdown.py`. Parametrize over `max_parallel_workers_per_gather ∈ {0, 2, 4, 8}`. For each:

- **Correctness, no GROUP BY**: COUNT, COUNT(col), COUNT(DISTINCT col), MIN, MAX, SUM, AVG over each supported type (int2/4/8, float4/8, dict-encoded text, raw text, timestamp). Compare against `pg_deltax.disable_parallel_agg=on` baseline.
- **Single GROUP BY**: same agg matrix; sorted-equality check.
- **Multi GROUP BY** (one segment_by + one not — Q3 shape).
- **GROUP BY + WHERE**: each batch_qual shape × GROUP BY.
- **NULL handling in COUNT(DISTINCT)** (excludes NULL).
- **GROUP BY column has NULLs** (PG groups all NULLs as one group).
- **Empty result** (every WHERE filters everything).
- **Single-segment table** (`recommend_agg_workers` returns 0; fallback to leader).
- **Spillover** (>1M groups; per-worker tuplestore path).
- **HAVING + parallel** (Phase E).
- **Aggregate over expression** (`SUM(col + 1)`, `length(col)`).
- **ORDER BY + LIMIT above**: planner places Sort + Limit above; verify order.
- **Inhibition**: queries with `FILTER`, `SUM(DISTINCT)`, `MIN(... ORDER BY)`, `OVER`, `GROUPING SETS`, multi-table joins must NOT pick the parallel DeltaXAgg path. Assert via EXPLAIN.

Bench (`pytest.mark.benchmark`):

| Phase | Q1 | Q3 | Q4 |
|---|---|---|---|
| A (no behaviour change) | 43s | 3.2s | 3.6s |
| B (scaffolding only) | 43s | 3.2s | 3.6s |
| Chain-Expr eligibility ✅ | **2.1s** | 3.2s | 3.6s |
| C (no DISTINCT) | 2.1s | ~0.8s¹ | ~0.8s¹ |
| D (DISTINCT + dict) | ~1s | ~0.8s | ~0.8s |
| F (final) | ~1s | ~0.8s | ~0.8s |

¹ Phase C's Q3/Q4 target assumes the underlying `MIN(timestamp + interval * cast)` Aggref shape is already accepted by the agg classifier. With the current shape blocker, Phase C alone won't move Q3/Q4 — see "Remaining JSONBench-specific work" in the chain-Expr section.

Targets land within 2× safety of ClickHouse's ~1s.

Also extend `tests/test_parallel_scan.py` with an EXPLAIN assertion that DeltaXAgg picks `parallel_workers > 0` on JSONBench Q1/Q3/Q4 plans.

## Risks

- **Float SUM non-associativity** under parallel reduction. PG core has the same behaviour; document, no fix.
- **NULL in COUNT(DISTINCT)** — must drop NULLs before union (both the dict NULL sentinel and direct sets).
- **DSM slab overflow** on adversarial GROUP BY (e.g. UUID column). Per-worker tuplestore spill, leader merges row-by-row from each tuplestore. Same pattern PG uses for Materialize/CTE.
- **Small-table parallel overhead**: `recommend_agg_workers` returns 0 for `total_segments < 16`, capped at `total_segments / 8`. `cost::estimate_agg_cost` updated to reflect parallel setup cost so the planner only picks parallel when worth it.
- **Rescan correctness**: `rescan_agg_scan` must clear `cached_partial_handles + result_rows` and re-run merge on next exec. Today only resets `result_idx`.
- **HAVING + ORDER BY hidden as Sort + Filter** on some PG versions. Hook walks `output_rel->reltarget->exprs` for non-Aggref refs to aggregate output, not just `havingQual`. Guard added in Phase E.
- **Top-N pushdown under parallel**: workers cannot prune top-N locally (a globally-top group might be non-top in any one worker). Workers emit full partial state; leader applies top-N at finalize. Workers carry more state but are CPU-bound on hash updates anyway.
- **High-cardinality DISTINCT remap cost**: ~3K segments × 30K dict entries = 90M lookups for Q1 if done lazily. Mitigate by building the global interner **once at the leader** during `initialize_dsm_deltax_agg`, not lazily during merge — workers use already-final IDs from the start. ~100ms leader-side for ~50K unique strings; big win.
- **`batch_quals` tri-state for the per-segment fast path**: must extend `evaluate_batch_quals` to return `BatchEval { AllPass, AllReject, Selective(Bitmap) }`. Without this, every WHERE-filtered segment falls into per-row evaluation and the fast path doesn't fire. Implement in Phase C, not deferred.

## Out of scope (each gets its own follow-up)

- HAVING with non-`Aggref OP Const` predicates (e.g. `HAVING SUM(a)/SUM(b) > 0.5`).
- Multi-relation aggregates (join-then-aggregate).
- GROUPING SETS / ROLLUP / CUBE.
- FILTER clause (`COUNT(*) FILTER (WHERE x = 1)`).
- WINDOW / OVER.
- Non-fixed-width DISTINCT types (numeric, jsonb) past the direct-HashSet fallback.
- Spilling the merged hash table on the leader (extreme group counts >100M).
- PG18+ partial-aggregate path that skips leader merge entirely (workers produce final groups for disjoint key partitions). Bigger architectural shift.

## Verification

After each phase:

1. `make clippy && make test` clean.
2. `make integration-test PG_VERSIONS=17` — full integration suite + new `test_aggregate_pushdown.py` parametrized at `max_parallel_workers_per_gather ∈ {0, 2, 4, 8}`.
3. `make -C jsonbench deploy EC2=<ip> && make -C jsonbench bench EC2=<ip>` — Q0/Q2 unchanged; Q1/Q3/Q4 land within their phase target above.
4. EXPLAIN ANALYZE on Q1 + Q3 verifies `Parallel Custom Scan (DeltaXAgg)` with `Workers Launched > 0` and per-worker rows.

Cumulative end-state on JSONBench m6i.8xlarge (100M rows): warm Q0 1.1s, Q1 ~3s, Q2 17s (unchanged), Q3 ~0.8s, Q4 ~0.8s. Total ~22s vs ClickHouse's ~5s. Closes most of the remaining gap; what's left is Q2's `EXTRACT(HOUR FROM ...)` per-row work (separate follow-up).
