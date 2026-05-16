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

### 2026-05-16 — `src/scan/exec/decompress.rs` — TBD

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
