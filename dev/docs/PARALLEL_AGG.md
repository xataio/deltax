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
    - [x] **C.2.f (partial)**: cost-aware infrastructure in place — eligibility predicate, `recommend_agg_workers` helper in `cost.rs`, `estimate_agg_cost(..., workers)` parallel-divisor adjustment, `path::peek_agg_topn_info`, `agg::can_use_compact_keys_path` re-export. **Runtime activation deferred**: hooking the parallel-aware path through PG's `Gather` model is non-trivial. PG expects "partial-Aggregate → Gather → final-Aggregate" semantics, but DeltaXAgg's design has the leader merge worker partials and emit final rows directly (workers contribute via DSM, not via tuple stream). With `add_partial_path` + Gather, leader_participation=on causes the leader to run `exec_custom_scan` as a worker AND as a final-emitter, double-counting; with leader_participation=off, the workers can't emit final rows because they don't have the merged accumulator. The C.2.b–e infrastructure (DSM hooks, `agg_wire`, deferred `exec_ctx`, worker / leader claim+merge code paths) is sound and exercised by the unit tests; activating it requires a follow-up — either splitting into partial-DeltaXAgg + a final-Aggregate combine stage, or building a Gather-equivalent that knows about our leader-merges semantics. `add_agg_path` keeps `parallel_workers = 0` for now.
    - [ ] **C.2.g**: tests in `tests/test_aggregate_pushdown.py` parameterised over `max_parallel_workers_per_gather ∈ {0, 2, 4}`. Pending the C.2.f follow-up that activates the path; with the gate dormant there's no behaviour to test beyond the unit-level `agg_wire` round-trip already covered.
  - [ ] **C.3**: extend `evaluate_batch_quals` (`batch_qual.rs:495`) to return tri-state `BatchEval { AllPass, AllReject, Selective(Bitmap) }`; add `enum SegmentEval { FullMetadata, PerSegmentByGroup, PerRow }` so per-segment metadata (`col_minmax / col_sums / row_count`) can short-circuit decompression for full-segment cases.
  - [ ] **C.4**: gate `add_agg_path` for parallel (`parallel_safe = true, parallel_workers = recommend_agg_workers(...)`). Update `cost::estimate_agg_cost` so the planner picks the parallel variant on large inputs. Add `tests/test_aggregate_pushdown.py` (parametrize over `max_parallel_workers_per_gather ∈ {0, 2, 4, 8}`) for correctness, and an EXPLAIN assertion that DeltaXAgg picks `parallel_workers > 0` on Q3/Q4-shaped queries.
- [ ] Phase D — parallel `COUNT(DISTINCT)` for dictionary-encoded text columns.
- [ ] Phase E — HAVING in the parallel path.
- [ ] Phase F — EXPLAIN, GUCs, hardening.

## Context

After json-extract, JSONBench warm timings on m6i.8xlarge are: Q0 1.1s, Q1 43s, Q2 17s, Q3 3.2s, Q4 3.6s. ClickHouse is ~1s on each. The dominant remaining cost is **single-threaded final aggregation above the parallel scan**: COUNT(DISTINCT) on Q1, MIN/MAX on Q3/Q4. Workers do parallel scan + parallel sort fine; PG's `GroupAggregate` runs single-threaded above the `Gather Merge` boundary because partial COUNT(DISTINCT) results can't be combined without re-deduping.

ClickHouse's `uniqExact` (the function JSONBench Q1 uses) is **exact** (hash set), not HLL. The gap is parallel hash-set construction + merge that PG core can't do for `COUNT(DISTINCT)`. pg_deltax can: each worker builds a local hash table per group, leader unions, finalizes. For dictionary-encoded text columns (e.g. the `did` synthetic), workers union segment dictionaries directly without per-row work.

`DeltaXAgg` already pushes the entire aggregation into a CustomScan that replaces `Aggregate → Scan`. **But it is `parallel_safe = false` and has no DSM hooks** — PG runs it single-process. The fix is to make it PG-parallel-aware along the same axes `DeltaXAppend` already is: DSM region, atomic segment-claiming cursor, per-worker partial state, leader-side merge, output emitted at the leader after `Gather` shutdown. The internal rayon path stays as a leader-only fallback.

Expected outcome: Q1 43s → ~3s, Q3/Q4 3s → ~0.8s, total bench warm 68s → ~22s. Closes most of the gap to ClickHouse's ~5s aggregate.

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

### Phase D — Parallel COUNT(DISTINCT) for dict-encoded text — ~1-2 days

Load-bearing piece for Q1. Each segment's dictionary already enumerates the distinct values in that segment.

**Algorithm**: during `initialize_dsm_deltax_agg`, the leader scans every relevant segment's dict blob (cheap — dict headers are small, ~few KB per segment) and builds a global string interner mapping each unique value to a final `u32` ID. Workers, on segment claim, decode the local dict, remap segment-local IDs → final global IDs, then for each group accumulator union the matching IDs into a `BitVec` keyed by global ID. **No per-row string work.**

`DistinctAcc` enum:

```rust
enum DistinctAcc {
    TextDictBitset(BitVec),         // dict-encoded text — load-bearing for Q1
    TextDirectSet(HashSet<Box<str>>), // raw text fallback
    Int64Set(HashSet<i64>),
    F64BitsSet(HashSet<u64>),
    Bool(u8),
}
```

Merge: union the bitsets pairwise (`worker_a | worker_b`). Final `count_distinct = bitset.count_ones()`.

For non-dict-encoded columns the path still parallelises via direct `HashSet` per worker + leader-side union — slower per row but still parallel.

NULL handling: `COUNT(DISTINCT col)` excludes NULLs. Dict NULL sentinel filtered before bitset insertion. Direct sets skip NULLs.

In `add_agg_path`: when an aggregate is `COUNT(DISTINCT col)` and `col`'s segment dict shape is detected at metadata load, allow `parallel_workers > 0`. Otherwise fall back to leader-only.

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
- `src/scan/hook.rs::deltax_create_upper_paths` (~line 1293) — already gates eligibility; add `is_parallel_safe` check on upper-rel pathtarget.
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
| C (no DISTINCT) | 43s | ~0.8s | ~0.8s |
| D (DISTINCT + dict) | ~3s | ~0.8s | ~0.8s |
| F (final) | ~2-3s | ~0.8s | ~0.8s |

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
