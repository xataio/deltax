# Cleanup Log

Append a row per cleanup session. Newest first. See
[`CLEANUP_PLAN.md`](./CLEANUP_PLAN.md) for the methodology and the per-file
checklist.

## Format

For each session, add a section like:

```
### YYYY-MM-DD — `path/to/file.rs` — <commit-sha>

**Scope:** which checklist steps ran (simplify / unsafe / tests / verify).
**LOC:** <before> → <after>   **`unsafe`:** <before> → <after>   **Tests:** <before> → <after>

- One line per notable change.
- Note any deferred work explicitly: "deferred: unsafe audit, will revisit
  in a follow-up session."
- **Benchmarks** (required when scan/exec path was touched): "clickbench
  local: no regression vs main", "rtabench: Q17 -8%, Q23 +3% (within
  noise)", "jsonbench: not run, doesn't apply".
- **Correctness:** "ran existing harness, all pass" or "added case for X".
- **Perf opportunities surfaced** (if any): one line per item — what,
  where, expected gain, deferred or done inline.
```

Keep entries terse. The log is for orientation across sessions, not for
narration.

## Sessions

### 2026-05-16 — `src/scan/path.rs` — TBD

**Scope:** read pass, simplify, tests, verify, full end-of-session gauntlet.
`unsafe` audit deferred — all 48 blocks are PG FFI for planner internals
(`palloc0`, `lappend_int`, `lappend_oid`, `nodeToString`, `makeConst`,
`makeTargetEntry`, `pull_varattnos`, etc.).
**LOC:** 2583 → 2714 (non-test) / 2813 total with 99 lines of tests added.
The non-test grew (+131) because rustfmt re-wrapped a few of the
expanded blocks; functional duplication still dropped at every site.
**`unsafe`:** 44 → 48 (+4 from the two new wire-format helpers).
**Tests:** 0 → 5 (all `#[test]`, pure logic — first tests in this file).

- Deleted dead `is_partial` and `transtype_oid` fields from
  `path::AggSpec`. They were written by 7 sites in `hook.rs` (always to
  `false` / `InvalidOid`) and never read on this struct. The active
  flag/transtype on the executor side lives on a separate
  `exec::AggExecSpec`. Removes 14 dead lines from `hook.rs` and clears
  the `#[allow(dead_code)]` on those fields.
- Removed stale `#[allow(dead_code)]` on
  `quals_reference_only_numeric_vars` — it's actively called from
  `add_agg_partial_path`. The annotation was added when the function was
  first introduced, before it was wired up.
- Extracted `append_qual_list_as_bytes(list, qual_list)`. The
  `nodeToString → lappend_int byte loop → pfree` block appeared verbatim
  3 times (in `add_count_star_path`, `add_minmax_path`, and `plan_agg_path`).
  Now one helper.
- Extracted `append_oids_as_ints(list, &[Oid])`. The 5-line
  "for &oid in companion_oids { lappend_int(list, oid as i32) }" loop
  appeared 3 times (in `add_count_star_path`, `add_minmax_path`,
  `build_agg_path_private`, and `plan_minmax_path`'s plan_private builder).
- Added 5 `#[test]` cases (pure logic, first tests in this file):
  - `meta_agg_kind_roundtrip` — every variant round-trips through the i32
    wire encoding (executor depends on this on worker DSM hydration)
  - `meta_agg_kind_rejects_out_of_range` — `from_i32(99)` panics
  - `topn_sort_col_derived_sentinel_is_negative` — const-asserted, can
    never silently collide with a real output-column index
  - `is_partial_eligible_var_type_accepts_numerics_and_temporals` — INT2/4/8,
    FLOAT4/8, TIMESTAMP, TIMESTAMPTZ, DATE, BOOL all accepted
  - `is_partial_eligible_var_type_rejects_text_jsonb_numeric` — TEXT/
    VARCHAR/BPCHAR/JSONB/BYTEA/NUMERIC rejected
  - `parallel_compact_aggs_ok_accepts_compact_set` — Count+Sum(int4) is
    compact-eligible
- Deferred: SAFETY: comment pass on the 48 `unsafe` blocks. All are PG
  FFI on planner internals and would need a wrapper layer to go safe.
- **Benchmarks**:
  - ClickBench EC2 (c6a.4xlarge, 100M-row full dataset): **63.19s vs prior
    63.13s** (+0.1% total, within noise). Zero regressions >10%; worst
    individual Q37 +7.3% on a 41ms query (noise floor).
  - JSONBench EC2 (m6i.8xlarge, 100M Bluesky events): **3.603s vs prior
    3.566s** (+1.0% total). Q4 +5.1% on a 0.65s query, all others within
    ±3%. path.rs only emits plan nodes — runtime is unchanged.
- **Correctness:** `make correctness` 999 passed / 3 skipped / 6 xfailed
  (matches baseline). Unit: 440 pass on PG17 and PG18 (was 434).
  Integration: 234 pass on PG17 and PG18.
- **Perf opportunities surfaced:** none.

### 2026-05-16 — `src/scan/hook.rs` — f902f7d

**Scope:** read pass, simplify, tests, verify, full end-of-session gauntlet.
`unsafe` audit deferred — almost all 85 blocks are PG FFI for planner
internals (`get_namespace_name`, `get_attname`, `get_atttypetypmodcoll`,
`list_nth`, `get_opname`, `SearchSysCache1`, `RangeVarGetRelidExtended`,
`pg_detoast_datum`, etc.). Worth a dedicated SAFETY: comment pass.
**LOC:** 4820 → 4642 (non-test) / 4735 total with 93 lines of tests added.
**`unsafe`:** 84 → 85 (+1 from the new `cached_companion_for_rel` helper).
**Tests:** 0 → 6 (all `#[test]`, pure logic — first tests in this file).

- Deleted `is_pushable_qual` (~70 LOC). It was already marked
  `#[allow(dead_code)]` and `grep -rn is_pushable_qual` confirmed zero
  callers — leftover from an earlier qual-validation refactor.
- Consolidated `has_segment_by` (~50 LOC) to derive from `get_meta_cols`
  rather than running its own SPI query + `SEGMENT_BY_CACHE` thread_local.
  The two caches stored overlapping data and ran two SPI lookups against
  `deltax_deltatable` for the same parent OID. Now a single SPI fires
  (via `META_COLS_CACHE`), and `has_segment_by` is a one-liner.
- Consolidated `get_time_column_attno` similarly — it now delegates to
  `get_meta_cols` for the SPI fetch and only caches the post-`get_attnum`
  resolution. Saves ~40 LOC and one redundant SPI per query for parent
  OIDs that the metadata-only fast path already touched.
- Extracted `cached_companion_for_rel(rel_oid)`. The
  `COMPRESSED_CACHE.with(|c| ... insert if not present ... return oid)`
  block appeared 4 times verbatim across `deltax_set_rel_pathlist`,
  `lookup_companion_from_subpath`, `collect_compressed_children`, and
  `deltax_executor_start`. Now one helper.
- Replaced two inline `unwrap_relabel` closures in
  `deltax_create_upper_paths` and `is_pushable_qual` with the already-
  existing named `unwrap_relabel_node` helper.
- Added 6 `#[test]` cases (pure logic, no PG harness):
  - `time_bounds_default_is_unbounded`
  - `time_bounds_narrow_lo_keeps_max` — narrowing keeps the tighter bound
  - `time_bounds_narrow_hi_keeps_min` — same on the upper side
  - `time_bounds_combined_any` — `any()` flips on first narrow
  - `is_minmax_meta_type_accepts_integer_float_date_timestamp` — INT2/4/8,
    FLOAT4/8, DATE, TIMESTAMP, TIMESTAMPTZ all accepted
  - `is_minmax_meta_type_rejects_text_bool_jsonb_numeric` — TEXT/VARCHAR/
    BPCHAR/JSONB/BOOL/BYTEA/NUMERIC all rejected (these can't be encoded
    as order-preserving i64 in colstats)
- Deferred: SAFETY: comment pass on the 85 `unsafe` blocks. Nearly all
  are PG FFI on planner internals (`PlannerInfo`, `RelOptInfo`,
  `Aggref`, `OpExpr`, `Var` trees, syscache lookups) and can't go safe
  without an enormous wrapper layer.
- **Benchmarks**:
  - ClickBench EC2 (c6a.4xlarge, 100M-row full dataset): **63.13s vs prior
    62.95s** (+0.3% total, within session noise). Worst individual:
    Q28 +7.7%, but cold runs match (19.66 vs 19.75) and Q28 has oscillated
    8.07–8.69 across the last 6 sessions — fully within noise.
  - JSONBench EC2 (m6i.8xlarge, 100M Bluesky events): **3.566s vs prior
    3.637s** (-2.0%). Q1 came back to baseline (1.973 → 1.894); other
    queries within ±2%.
- **Correctness:** `make correctness` 999 passed / 3 skipped / 6 xfailed
  (matches baseline). Unit: 434 pass on PG17 and PG18 (was 428).
  Integration: 234 pass on PG17 and PG18.
- **Perf opportunities surfaced:** none.

### 2026-05-16 — `src/copy.rs` — a41585d

**Scope:** read pass, simplify, tests, verify, full end-of-session gauntlet.
`unsafe` audit deferred — all 69 blocks are PG FFI (table_open, heap_insert,
GetBulkInsertState, palloc, MemoryContextSwitchTo, ProcessUtility hook
chaining, RangeVarGetRelidExtended, list manipulation).
**LOC:** 3456 → 3543 (non-test) / 3635 total with 110 lines of tests added.
The non-test count is roughly flat (+87) because the new helpers carry
docstrings, but functional duplication dropped substantially.
**`unsafe`:** 67 → 69 (+2 from the new `bulk_heap_insert` helper that
encapsulates the unsafe ops previously inlined three times).
**Tests:** 0 → 9 (all `#[test]`, pure logic).

- Extracted `bulk_heap_insert(oid, ctx_name, items, build_datums)`. The three
  near-identical `flush_partition_blobs` blocks (blobs / blooms / text_lengths)
  were ~50 LOC each — they all open the table, build a fresh per-row temp
  memory context to bound TOAST scratch growth, palloc the bytea, form/insert
  the tuple, and reset the context. Now they're three 4-line closures into
  one helper.
- Extracted `parallel_compress_cols` + `compress_one_col`. The
  parallel-or-sequential compression dispatch (~50 LOC) was inlined inside
  both `compress_segment` and `flush_segment`; both call sites are now
  one-liners.
- Extracted `PartitionBuffer::cache_companion_fqns`. The "stash meta/colstats
  FQNs + meta INSERT column list on first segment flush" block (~25 LOC) was
  duplicated between `flush_segment` and `write_compressed_segment`.
- Extracted `create_blobs_table`, `create_blooms_table`,
  `create_text_lengths_table`. The DDL strings were inlined at 4 sites; now
  each is one named helper.
- Simplified `bytea_to_datum`'s signature: returns `Datum` (was
  `(Datum, *mut c_void)` for an explicit pfree pointer). All three callers
  discarded the pfree pointer because the per-row memory context reset
  already frees the bytea.
- Removed unused `_last_part_idx: &mut Option<usize>` parameter from
  `merge_and_flush_results` (the parallel path doesn't update it — the
  cross-partition `flush_partition_blobs` trigger only fires on the
  sequential / trailing-line paths).
- Removed a leftover `let companion_ddl = build_companion_ddl(...);` whose
  uses had all been replaced.
- Added 9 `#[test]` cases for `find_partition` (pure binary search, zero
  prior tests):
  - empty ranges
  - lookup before first / after last
  - exact start inclusive, exact end exclusive
  - gaps between ranges (lookups in the gap return None)
  - single-range edge cases
  - negative-timestamp values (pre-1970 data)
  - `expand_file_glob` literal-path short-circuit
- Deferred: SAFETY: comment pass on `unsafe` blocks. Almost all are PG FFI
  on planner/executor types (heap_insert, table_open, list_nth, palloc,
  MemoryContextSwitchTo, ProcessUtility_hook chain) and can't go safe
  without an enormous wrapper layer. Worth a dedicated session focused on
  SAFETY: comment coverage.
- **Benchmarks**:
  - ClickBench EC2 (c6a.4xlarge, 100M-row full dataset): **62.95s vs prior
    62.43s** (+0.8% total, within session-to-session noise band). Zero
    regressions >10%. Worst individual: Q18 +8.2% but cold runs match
    (11.59 vs 11.36) and run 2 is within 1.8% — only the best-of-3 column
    differs because the prior run got a lucky 3rd-run sample.
  - JSONBench EC2 (m6i.8xlarge, 100M Bluesky events): **3.637s vs prior
    3.554s** (+2.3% total). Q1 +4.4% accounts for most of the drift; cold
    run matches (15.03 vs prior cold), and copy.rs is not on the read
    path, so this is run-to-run variance.
- **Correctness:** `make correctness` 999 passed / 3 skipped / 6 xfailed
  (matches baseline). Unit: 428 pass on PG17 and PG18 (was 419).
  Integration: 234 pass on PG17 and PG18.
- **Perf opportunities surfaced:** none.

### 2026-05-16 — `src/scan/exec/decompress.rs` — 2d4f7e7

**Scope:** read pass, simplify, tests, verify, full end-of-session gauntlet.
`unsafe` audit deferred (most of the 58 blocks are PG FFI: list_nth_int,
palloc, MemoryContextSwitchTo, AllocSetContextCreateInternal, ParallelWorkerNumber).
**LOC:** 4366 → 4218 (non-test) / 4502 total with 284 lines of tests added.
**`unsafe`:** 54 → 58 (+4 from the new `parse_custom_private` helper —
encapsulates the unsafe operations that were previously inlined in three
separate ad-hoc parsers). **Tests:** 0 → 14 (all `#[test]`, no PG state needed).

- Unified the planner-side `custom_private` parser. Three ad-hoc parsers
  (in `begin_custom_scan`, `begin_deltax_append`, and
  `build_needed_cols_from_custom_private`) became one `parse_custom_private`
  that returns the header ints, the needed-column indices (across both `-1`
  and `-3` sections), and an optional Top-N block.
- Extracted `merge_and_selection(target, src)`. The "AND-merge into
  `pre_selection`" pattern that previously appeared 9 times across
  `exec_topn_two_pass`, `exec_topn_text_sequential`, and `load_next_segment`
  is now a 6-line helper.
- Extracted `segment_pre_pruned_by_metadata(seg, segby_filters, time_min, time_max)`.
  The pre-decompress pruning (segment-by equality + time-range overlap)
  appeared 3 times with the same structure; callers now just check the
  return value and increment `segments_skipped`.
- Extracted `compute_phase1_col_indices` and `compute_phase1_blob_indices`.
  Both `exec_topn_two_pass` and `exec_topn_text` had identical inline
  blocks computing these.
- `#[derive(Default)]` on `ScanTiming`. Replaces three verbose struct
  literals (28 `field: 0,` lines each) in `make_worker_stub_state`,
  `begin_deltax_append`, and `load_decompress_state` with `..Default::default()`.
- Removed unused `_phase1_blob_indices: &[usize]` parameter from
  `exec_topn_text_sequential` (caller already passes `phase1_col_indices`,
  and the inner function doesn't need blob indices because the parallel
  path already detoasted Phase 1 blobs before falling through).
- Added 14 `#[test]` cases (pure logic, no PG harness):
  - `cmp_topn_key`: asc/desc/null handling, both `nulls_first` modes
  - `cmp_nullable_str_byte`: byte ordering + NULL semantics
  - `merge_and_selection`: empty target adopts src; non-empty AND-merges
  - `col_to_blob_idx`: skips segment_by columns in blob-layout indexing
  - `compute_phase1_col_indices`: sort col + batch-qual cols, with
    segment_by-only-if-quallified rule
  - `compute_phase1_blob_indices`: dense indexing of non-segment_by columns
  - `segment_pre_pruned_by_metadata`: no-filter pass-through, segment_by
    match/mismatch (single + multi), time-range above/below/overlap/contained
- Deferred: `unsafe` SAFETY: comments. All 58 blocks are PG FFI
  (list_nth_int, palloc, MemoryContextSwitchTo, AllocSetContextCreateInternal,
  ExecStoreVirtualTuple, ParallelWorkerNumber atomics). They cannot go safe
  without wrapping all of PG's planner-state API. Worth a dedicated pass.
- **Benchmarks**:
  - ClickBench EC2 (c6a.4xlarge, 100M-row full dataset): **62.43s vs prior
    62.71s** (−0.4% total, within noise). Zero regressions >10%. Worst
    individual: Q38 +4.8% on a 66ms query (noise floor).
  - JSONBench EC2 (m6i.8xlarge, 100M Bluesky events): **3.55s vs prior
    3.57s** (−0.6%). All 5 queries within ±2.5%.
  - Local Docker benches: not separately re-run (EC2 numbers supersede).
- **Correctness:** `make correctness` 999 passed / 3 skipped / 6 xfailed
  (matches baseline). Unit: 419 pass on PG17 and PG18 (was 406). Integration:
  234 pass on PG17 and PG18.
- **Perf opportunities surfaced:** none.

### 2026-05-16 — `src/scan/exec/datum_utils.rs` — 52f5f74

**Scope:** read pass, simplify, tests, verify, benchmarks. `unsafe` audit
deferred (still 54 blocks; nearly all are PG FFI for `palloc`,
`OidInputFunctionCall`, varlena layout).
**LOC:** 1689 → 1575   **`unsafe`:** 52 → 54 (+2 from the arena helper split,
isolated to the new `varlena_arena_alloc`)   **Tests:** existing `mod.rs` tests
covered the file already (the triage's "0 tests" was misleading); added 7
focused ones below.

- Added `is_null_at(bitmap, i)` private helper, used by 10 inline checks that
  spelled `(bitmap[i / 8] >> (i % 8)) & 1 == 1`. Easier to grep, easier to
  read, identical codegen behind `#[inline]`.
- Added `merge_with_placeholder(matched, sel)` helper. Replaced 12 verbatim
  copies of the "for each pass: push matched or Datum(0)" pattern in
  `decompress_text_blob_with_{like,eq,in}_filter` and
  `decompress_{text,jsonb}_blob_with_selection`.
- Unified the two arena allocators (`str_slices_to_text_datums_arena` and
  `byte_slices_to_jsonb_datums_arena`) on a single private
  `varlena_arena_alloc(&[&[u8]])`. The text version is now a thin wrapper
  that handles the bpchar/jsonb safety-net path; the jsonb version is a
  one-liner. Saves ~50 LOC and consolidates the varlena layout invariants in
  one place.
- Added unit tests in `src/scan/exec/mod.rs`:
  - `test_is_null_at` (pure)
  - `test_merge_with_placeholder` (pure)
  - `#[pg_test] test_decompress_text_eq_filter_dict` and `_ne_dict` (covers
    Dictionary path's `parse_header` + `read_index` + empty-string fast-path)
  - `#[pg_test] test_decompress_text_in_filter_{dict,lz4}` (covers IN set probe
    on both codec paths)
  - `#[pg_test] test_decompress_text_like_contains_lz4` (LIKE `%needle%` SIMD
    `memmem` scan on LZ4)
- Deferred: unsafe audit. Most of the 54 unsafe blocks are FFI to
  `pg_sys::{palloc, OidInputFunctionCall, getTypeInputInfo,
  cstring_to_text_with_len, ExecClearTuple, MemoryContextSwitchTo}` and the
  varlena layout work in the arena helper. Pure-FFI, can't go safe without an
  enormous wrapper layer. Worth a dedicated SAFETY: comment pass in a
  follow-up.
- **Benchmarks** (this file affects per-row exec, so all three):
  - clickbench local: +2.2% total vs prior commit — noise.
  - clickbench EC2 (c6a.4xlarge, full 100M-row dataset): no regressions; 0
    queries >10% slower vs prior commit.
  - jsonbench (EC2 100m): −0.1% total, all 5 queries within ±4%.
  - rtabench local: not run; clickbench + jsonbench cover the relevant
    text/jsonb decompression paths.
- **Correctness:** `make correctness` 999 passed, 3 skipped, 6 xfailed
  (correctness preserved against vanilla PostgreSQL). Unit tests: 406 pass on
  PG17 and PG18 (was 399). Integration: 234 pass on PG17 and PG18.
- **Perf opportunities surfaced:** none.

### 2026-05-16 — `src/scan/json_extract.rs` — 3eab8be

**Scope:** read pass, simplify, tests, verify. Full `unsafe` audit deferred.
**LOC:** 2776 → 2712   **`unsafe`** (non-test): 106 → 104   **Tests:** 1 → 7

- Deleted two genuinely dead `pub(crate)` fns: `rewrite_chains_in_node`,
  `rewrite_chains_in_restrictinfo_list` (no callers).
- Removed stale `#[allow(dead_code)]` from items now live:
  `PhysicalCols::from_rel_oid`, `build_custom_scan_tlist`,
  `build_chain_expr_for_spec`, `make_text_const`.
- Extracted `walk_chain_shape` helper from 5 duplicated chain walkers
  (`match_extract_chain`, `chain_signature_of`, `match_chain_against_child_stl`,
  `match_scan_chain_against_synths`, `classify_custom_scan_tlist_entry`).
- Removed dead `qual_outer` collection in `rebuild_cscan_custom_private`
  (collected via `pull_var_clause` then immediately `let _ =`).
- Removed duplicate comment block in `extend_scan_targetlist_with_forwarders`.
- Moved misplaced doc-comment so each of `check_cscan_has_relevant_synthetics`
  and `rebuild_custom_scan_tlist_from_catalog` documents itself.
- Added 6 `#[pg_test]` cases for `walk_chain_shape`: simple `->>`,
  nested path, outer cast, bare-Var rejection, missing-terminal-`->>`
  rejection, null-node rejection.
- Drive-by clippy fixes in `blob_cache/{mod,storage}.rs`,
  `scan/exec/decompress.rs`, `scan/hook.rs` (collapsible if, type aliases,
  arithmetic with-no-effect, doc lazy-continuation). Pre-existing warnings;
  cleared so `make clippy` is clean per the per-session rule.
- Deferred: full `unsafe` audit. Most blocks are PG FFI on Node trees
  (`pg_sys::list_nth`, `makeVar`, `pull_var_clause`, raw Node-tag dispatch);
  they can't go safe without an enormous wrapper layer, and the function-body
  `unsafe { ... }` blocks already wrap only FFI ops. Worth a dedicated
  follow-up session focused on SAFETY: comment coverage.
- **Benchmarks:** clickbench local: -4.6% total vs main (within noise; this
  file is plan-time only and ClickBench tables don't use json_extract).
  rtabench local: +1.6% total (within noise), **0 result-set mismatches**
  (correctness preserved). JSONBench: not run (EC2-only, optional per plan).
- **Correctness:** ran integration tests on PG17 and PG18 (234 + 234 pass);
  unit tests 399 pass on PG17 and PG18. RTABench correctness harness reports
  0 mismatches against plain PostgreSQL.
- **Perf opportunities surfaced:** none.
- **Note:** `make fmt FILE=<path>` actually reformats the entire workspace
  (cargo fmt's positional args don't restrict scope). Reverted format-only
  diffs in 30 untouched files. If we want true per-file fmt, the Makefile
  needs `cargo fmt -- --check <file>`-style invocation or `rustfmt` directly.
