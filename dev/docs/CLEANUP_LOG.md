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

### 2026-05-16 — `src/scan/exec/append_wire.rs` — TBD

**Scope:** read pass, simplify, tests, verify, full end-of-session gauntlet.
The triage listed 0 tests but the file actually had 3 `#[pgrx::pg_test]`
tests for the round-trip and magic-mismatch paths. None of them touched
PG state — they're byte-buffer manipulation that happens to use
`pg_sys::Oid::from(...)`. Converting them to plain `#[test]` lets them
run without the PG harness boot, which is a meaningful CI speedup.
**LOC:** 649 → 785 (total) with 4 new tests added.
**`unsafe`:** 17 → 21 (+4 from the new `write_name_indices` helper).
**Tests:** 3 `#[pgrx::pg_test]` → 7 `#[test]` (4 added + 3 converted).

- Extracted `write_name_indices(out, names, col_names)`. The
  `for (k, name) in <names>.iter().enumerate() { let idx = col_names.iter().position(...).unwrap_or(0) as u32; *out.add(k) = idx; }`
  loop was inlined twice in `serialize_into` (segment_by + order_by
  indices). Now one helper.
- Converted 3 `#[pgrx::pg_test]` tests to plain `#[test]`. They don't
  touch PG state — pure byte-buffer serialise/attach/decode that happens
  to use `pg_sys::Oid::from(...)` for type-tagging. Plain `#[test]`
  runs without spinning up the pgrx harness.
- Added 4 `#[test]` cases:
  - `round_up_handles_powers_of_two` — already-aligned, off-by-one, and
    cross-power values; the function is called inside `layout` to align
    `SegmentEntry`s and any drift would break the compile-time `align_of`
    asserts at run time.
  - `encode_segment_values_len_counts` — empty, single NULL, ASCII
    string, and a mixed UTF-8 case ("héllo" → 6 bytes for char count).
    The decoder is byte-perfect on this; a length-fn drift here would
    cause out-of-bounds reads on the worker side.
  - `wire_version_mismatch_rejected` — serialise V1 then hand-corrupt
    the version field; `attach` must return `None`. Catches a future
    drift where someone bumps `WIRE_VERSION` and forgets the attach
    check.
  - `wire_segment_by_index_lookup_uses_col_name_position` — confirms
    that `segment_by` names round-trip through the
    "name → col_names index → name" indirection used by the new
    `write_name_indices` helper.
- **Benchmarks**:
  - ClickBench EC2 (c6a.4xlarge, 100M-row full dataset): **63.22s vs
    prior 62.38s** (+1.3% total, within session noise). Zero
    regressions >10%; top: Q06 +6.3% at 16ms noise floor.
  - JSONBench EC2 (m6i.8xlarge, 100M Bluesky events): **3.632s vs prior
    3.597s** (+1.0%). Q4 +7.5% on a 0.65s query; append_wire.rs is the
    DSM serialiser, not on Q4's read path.
- **Correctness:** `make correctness` 999 passed / 3 skipped / 6 xfailed
  (matches baseline). Unit: 497 pass on PG17 and PG18 (was 493).
  Integration: 234 pass on PG17 and PG18.
- **Perf opportunities surfaced:** none.

### 2026-05-16 — `src/scan/exec/count_minmax.rs` — 9867af0

**Scope:** read pass, simplify, tests, verify, full end-of-session gauntlet.
`unsafe` audit deferred — all blocks are PG FFI (list_nth_int,
get_rel_name, stringToNode, SPI metadata load).
**LOC:** 798 → 901 (non-test) / 998 total with 97 lines of tests added.
Non-test growth (+103) is rustfmt + new helper docstrings; functional
duplication between the two Begin callbacks dropped.
**`unsafe`:** 20 → 28 (+8 from the 4 new helpers being `unsafe fn`).
**Tests:** 0 → 7 (all `#[test]`, pure logic — first tests in this file).

- Extracted four `unsafe fn` helpers shared by `begin_count_scan` and
  `begin_minmax_scan`:
  - `parse_companion_oids(custom_private, label)` — walks the
    `[oid1, ..., -1]` prefix, asserts non-empty.
  - `parse_trailing_qual_bytes(custom_private, idx)` — reads the
    `[qual_bytes_len, bytes...]` trailer.
  - `relation_name_or_error(oid)` — wraps `get_rel_name` with the
    canonical "companion table not found" error.
  - `rehydrate_segment_filters(qual_bytes, ...)` — bytes →
    `stringToNode` → `extract_segment_filters`, returning the no-filter
    triple when bytes are empty.
- Each call site now has a 3-line wire-format header instead of a
  ~30-line ad-hoc parser; the wire format is documented at the call
  site via a single-line comment.
- Added 7 `#[test]` cases (pure logic, no PG harness):
  - `decode_encoded_to_datum_integer_identity` — INT2/4/8 round-trip
  - `decode_encoded_to_datum_timestamp_strips_pg_epoch_offset` —
    Unix-epoch µs → PG-epoch µs by subtracting `PG_EPOCH_OFFSET_USEC`
  - `decode_encoded_to_datum_date_converts_usec_to_pg_days` —
    truncating division + offset subtraction
  - `decode_encoded_to_datum_floats_round_trip` — full f32/f64
    round-trip through `encode_fXX_to_i64` (including ±0, ±∞, denormals)
  - `sum_i128_to_datum_packs_into_int8`
  - `sum_i128_to_datum_overflow_panics_for_int8_result` /
    `_underflow_panics_for_int8_result` — pgrx::error! must fire,
    never silently truncate (wrong sum is worse than a query failure).
    Use bare `#[should_panic]` because the pgrx error payload isn't a
    plain string.
- Deferred: SAFETY: comment pass on the 28 `unsafe` blocks. All are PG
  FFI on List nodes, syscache lookups, and SPI.
- **Benchmarks**:
  - ClickBench EC2 (c6a.4xlarge, 100M-row full dataset): **62.38s vs
    prior 62.62s** (-0.4% total). Zero regressions >10%; Q38 -7.4%,
    Q22 +3.1% all within run-to-run noise.
  - JSONBench EC2 (m6i.8xlarge, 100M Bluesky events): **3.597s vs prior
    3.631s** (-0.9%). All queries within ±1.5% — a stable run.
- **Correctness:** `make correctness` 999 passed / 3 skipped / 6 xfailed
  (matches baseline). Unit: 493 pass on PG17 and PG18 (was 486).
  Integration: 234 pass on PG17 and PG18.
- **Perf opportunities surfaced:** none.

### 2026-05-16 — `src/scan/exec/batch_qual.rs` — 41b5cc9

**Scope:** read pass, simplify, tests, verify, full end-of-session gauntlet.
`unsafe` audit deferred — the file only has 4 `unsafe` ops, all narrow
PG FFI in `extract_batch_quals` for reading planner Var/Const trees and
deconstructing `ScalarArrayOpExpr` arrays.
**LOC:** 884 → 829 (non-test) / 1102 total with 273 lines of tests added.
Non-test code shrunk by 55 lines from the generic-filter dedup.
**`unsafe`:** 4 → 4 (unchanged).
**Tests:** 0 → 18 (all `#[test]`, pure logic — first tests in this file).

- Extracted `apply_batch_filter_typed<T, F>(col, sel, op, constant, decode)`
  to dedupe the 5 monomorphic batch filters (`apply_batch_filter_{i64,
  i32, i16, f64, f32}`). Each was a 28-30 line copy of the same null-
  handling + 6-arm match. Each public wrapper now collapses to a single
  call passing in the type-specific `decode` closure (`d.value() as i64`
  for ints, `f64::from_bits(d.value() as u64)` for floats). Rust
  monomorphises one tight loop per call site, so this doesn't cost the
  auto-vectorisation the explicit versions had.
- Kept `apply_batch_filter_bool` separate — bool only supports `=`/`<>`
  so the generic helper's PartialOrd arithmetic match arms aren't
  meaningful. Note in the body documents why.
- Added `Default for BatchQual`. Replaces the trailing
  `like_strategy: None, text_const: None, in_list_i64: None,
  in_list_text: None` boilerplate at 5 construction sites in
  `extract_batch_quals` with `..Default::default()`.
- Added 18 `#[test]` cases (pure logic, no PG harness):
  - `parse_compare_op_recognised_set`, `parse_compare_op_rejects_non_comparisons`
  - `flip_compare_op_is_involutive_for_symmetric_ops`,
    `flip_compare_op_is_involutive` — `flip(flip(op)) == op` for every variant
  - `compile_like_pattern_classifies_simple_shapes` — Exact / Contains /
    StartsWith / EndsWith / `%`-bare
  - `compile_like_pattern_falls_back_to_general` — backslash / `_` /
    mid-pattern `%` / three+ `%`s all → General
  - `sql_like_match_basic` + `_backslash_escapes_metachars` +
    `_empty_strings` — wildcard semantics, escape, edge cases
  - `apply_batch_filter_i64_ands_into_existing_selection` — confirms
    that an existing `false` bit stays `false` (the AND semantics that
    makes multi-qual evaluation correct)
  - `apply_batch_filter_i64_null_rows_are_dropped` — SQL three-valued
    logic: NULL drops regardless of operator
  - `apply_batch_filter_f64_handles_each_op` — every comparison op
    matrix
  - `apply_batch_filter_in_list_int4` — IN list with mixed match/no-
    match/NULL
  - `batch_qual_default_is_safe_neutral` — `BatchQual::default()`
    populates every field
  - `is_batch_comparable_type_matrix`, `is_text_type_matrix`
- Deferred: SAFETY: comment pass on the 4 `unsafe` blocks (PG node-tree
  reads + `deconstruct_array`).
- **Benchmarks**:
  - ClickBench EC2 (c6a.4xlarge, 100M-row full dataset): **62.62s vs
    prior 62.63s** (essentially identical, -0.0% total). Zero
    regressions >10%; all queries within ±5%.
  - JSONBench EC2 (m6i.8xlarge, 100M Bluesky events): **3.631s vs prior
    3.633s** (-0.1%). All queries within ±1.5% — tightest run yet.
- **Correctness:** `make correctness` 999 passed / 3 skipped / 6 xfailed
  (matches baseline). Unit: 486 pass on PG17 and PG18 (was 470).
  Integration: 234 pass on PG17 and PG18.
- **Perf opportunities surfaced:** none.

### 2026-05-16 — `src/blob_cache/storage.rs` — 15eeee3

**Scope:** read pass, simplify, tests, verify, full end-of-session gauntlet.
The triage flagged this file as a fresh-design candidate: "Mmap-backed
cache. Recent (just merged); add tests while the design is fresh."
`unsafe` audit deferred — most blocks are PG FFI for DSA / LWLock /
ShmemInitStruct that can't easily go safe.
**LOC:** 1019 → 1022 (non-test) / 1128 total with 106 lines of tests
added.
**`unsafe`:** 37 → 35 (-2 from `entry_ptr_mut` removal + dropping the
unused `_shard_idx` parameter on `evict_in_shard`).
**Tests:** 0 → 6 (all `#[test]`, pure logic — first tests in this file).

- Deleted `entry_ptr_mut`. The function body was a verbatim copy of
  `entry_ptr` — both did `dsa_get_address(...) as *mut Entry`. Single
  call site (`insert`) now uses `entry_ptr`.
- Extracted `hash_to_shard_bucket(key, n_shards) -> (usize, u32)`. The
  3-line "hash + shard mask + bucket mask" sequence was inlined in both
  `get_pinned` and `insert`. The helper also documents the invariant —
  shard uses high bits, bucket uses low bits, so the two slot
  dimensions stay independent.
- Removed unused `_shard_idx: usize` parameter from `evict_in_shard`
  (3 call sites passed it, the body never used it).
- Added 6 `#[test]` cases (pure logic, no PG harness — they don't touch
  shared memory):
  - `shards_and_buckets_are_powers_of_two` — load-bearing invariant for
    the `& (n - 1)` slot masks; const-asserted + runtime-asserted
  - `hash_key_is_deterministic`
  - `hash_key_distinguishes_each_field` — bumping any of `companion_oid`,
    `segment_id`, `col_idx` must change the hash so cache hits don't
    alias across unrelated triples
  - `hash_to_shard_bucket_stays_in_range` — every (key, n_shards) pair
    lands inside `[0, n_shards) × [0, BUCKETS_PER_SHARD)`
  - `hash_to_shard_bucket_is_deterministic`
  - `hash_to_shard_bucket_shard_and_bucket_use_different_bits` — across
    64 keys × 16 shards, partial collisions occur in both directions
    (same-shard-diff-bucket and same-bucket-diff-shard). If shard and
    bucket were derived from the same bits, one of these counts would
    be zero — the test guards against that regression.
- Deferred: SAFETY: comment pass on the 35 `unsafe` blocks (DSA, LWLock,
  ShmemInitStruct, atomic loads through DSA pointers). Each is narrow
  but the invariants (lock ownership / lifetime of DSA mappings) deserve
  a dedicated session.
- **Benchmarks**:
  - ClickBench EC2 (c6a.4xlarge, 100M-row full dataset): **62.63s vs
    prior 62.82s** (-0.3% total). Zero regressions >10%; Q10 -5.8%,
    Q28 -3.6% (all within run-to-run noise).
  - JSONBench EC2 (m6i.8xlarge, 100M Bluesky events): **3.633s vs prior
    3.588s** (+1.3% total). All queries within ±3%; storage.rs only
    contains slot-computation simplifications, so deltas are noise.
- **Correctness:** `make correctness` 999 passed / 3 skipped / 6 xfailed
  (matches baseline). Unit: 470 pass on PG17 and PG18 (was 464).
  Integration: 234 pass on PG17 and PG18.
- **Perf opportunities surfaced:** none.

### 2026-05-16 — `src/compress.rs` — 8f54e9d

**Scope:** read pass, simplify, tests, verify, full end-of-session gauntlet.
`unsafe` audit deferred — the file only has 7 `unsafe` ops, all narrow PG
FFI for `jsonb_text_to_binary`.
**LOC:** 3773 → 3880 (non-test) / 4337 total with 9 new tests added.
Non-test grew (+107) because rustfmt expanded many densely-packed
single-line items; functional duplication dropped at every site.
**`unsafe`:** 7 → 7 (no change — none of the cleanups touched FFI).
**Tests:** 13 → 22.

- Extracted `minmax_encoded_via<T>(values, encode)` to dedupe the 5
  numeric branches of `compute_minmax_encoded_i64`. Each branch was 8–10
  lines of identical "for v in flatten / map_or min / map_or max" logic;
  now each is a one-line call passing in the type-specific `encode` fn.
- Extracted `sum_int_column<T>(values)` and `sum_float_column<T>(values)`
  for the 5 numeric branches of `compute_typed_sum`. The integer branches
  (Int16/Int32/Int64) all widened to i128 and computed the same triple;
  the float branches (Float32/Float64) likewise computed via f64 with
  `{:.17e}` formatting. Now each branch is a one-liner; text and bool
  branches remain inline because they have different semantics.
- Extracted `minmax_ord<T: Ord>` and `minmax_float<T: PartialOrd>` for the
  same reduction inside `compute_typed_minmax`. The integer branches now
  share the reduction; only the post-processing (Date / timestamp string
  formatting) differs and stays inline.
- Added 9 `#[test]` cases:
  - `compute_minmax_encoded_i64_handles_each_numeric_kind` — Int16/32/64,
    Float32/64, all-null
  - `compute_minmax_encoded_i64_returns_none_for_unsupported_types` —
    text / bool / declared-mismatch types
  - `compute_typed_sum_integer_branches` — i128 widening, all-null, count
    vs nonzero distinction
  - `compute_typed_sum_float_branches` — Float32 widens via f64
  - `compute_typed_sum_text_returns_char_count_sum` — char count (not byte
    count); the "héllo" case (é = 2 bytes, 1 char) locks in the semantic
  - `compute_typed_sum_bool_and_bytes_have_no_sum`
  - `supports_minmax_matrix`, `supports_sum_matrix`,
    `is_text_data_type_matrix` — comprehensive type-name coverage
  - `is_valid_identifier_accepts_legal_names` — happy + rejects
    starts-with-digit, dashes, spaces, non-ASCII
  - `is_recognized_extract_type_matrix` — every accepted spelling +
    rejection of `jsonb`/`uuid`/`numeric`
  - `classify_column_segment_by_is_text`, `classify_column_maps_pg_aliases`
- Deferred: SAFETY: comment pass on the 7 `unsafe` blocks (all in
  `jsonb_text_to_binary`).
- **Benchmarks**:
  - ClickBench EC2 (c6a.4xlarge, 100M-row full dataset): **62.82s vs
    prior 62.13s** (+1.1% total, within noise). Q40 +10.3% (75ms query,
    noise floor) and Q28 +5.3% are run-to-run variance — compress.rs is
    only on the COPY/ingest path, and the EC2 data was loaded at setup
    time, before any of these changes.
  - JSONBench EC2 (m6i.8xlarge, 100M Bluesky events): **3.588s vs prior
    3.582s** (+0.2% total). All queries within ±7%; Q3 -6.6%.
- **Correctness:** `make correctness` 999 passed / 3 skipped / 6 xfailed
  (matches baseline). Unit: 464 pass on PG17 and PG18 (was 451).
  Integration: 234 pass on PG17 and PG18.
- **Perf opportunities surfaced:** none.

### 2026-05-16 — `src/scan/exec/segments.rs` — fdeea74

**Scope:** read pass, simplify, tests, verify, full end-of-session gauntlet.
`unsafe` audit deferred — all 29 blocks are PG FFI (table_open, index_open,
heap_getnext, pg_detoast_datum, RelationGetIndexList, etc.).
**LOC:** 3150 → 3367 (non-test) / 3626 total with 259 lines of tests added.
Non-test grew (+217) because rustfmt re-wrapped many of the densely-packed
original blocks; functional duplication dropped at every site.
**`unsafe`:** 23 → 29 (+6 from the 3 new helper functions, each marked
`unsafe fn` because they call FFI internally).
**Tests:** 0 → 11 (all `#[test]`, pure logic — first tests in this file).

- Extracted `sibling_table_oid(meta_oid, suffix)`. The "strip `_meta` /
  build `{partition}_<suffix>` / look up by name in same namespace" block
  appeared 3 times verbatim across `load_text_length_sidecars`,
  `fetch_segment_blobs`, and the colstats lookup inside `load_segments_heap`.
- Extracted `primary_key_index_oid(rel)`. The "walk `RelationGetIndexList`,
  open each, test `indisprimary`, close, free list" block appeared 4 times
  in this file (inside `load_text_length_sidecars`, `fetch_segment_blobs`,
  and twice in `load_segments_heap` for colstats and blooms scans, plus a
  vb_rel variant). Now one helper.
- Extracted `detoast_varlena_to_vec(varlena_ptr)`. The "pg_detoast_datum →
  vardata_any / varsize_any_exhdr / from_raw_parts → conditional pfree"
  block appeared 5+ times. Now a single helper consolidates the
  ownership rule (free only when `detoasted != input`).
- Removed stale `#[allow(dead_code)]` on `ColSum` — all fields are
  actively read by `agg.rs` (`sum_datum`, `sum_i128`, `sum_f64`,
  `nonnull_count`, `nonzero_count`, `type_oid`).
- Added 11 `#[test]` cases (pure logic, no PG harness):
  - `segment_passes_minmax_filter` matrix: Eq/Ne/Lt/Le/Gt/Ge edges, InList,
    Like (always-true fallthrough) — 5 tests
  - `segment_all_rows_pass`: equality on point ranges, ambiguous ranges,
    null bounds, comparison-op ranges — 3 tests
  - `is_zero_const` per-type matrix — 1 test
  - `encode_datum_to_i64` identity-on-integers + None-on-text — 2 tests
- Deferred: SAFETY: comment pass on the 29 `unsafe` blocks. All are PG
  FFI on heap/index scans, snapshots, and TOAST.
- **Benchmarks**:
  - ClickBench EC2 (c6a.4xlarge, 100M-row full dataset): **62.13s vs prior
    63.19s** (-1.7% total faster). Zero regressions >10%. Top speedup:
    Q40 -10.5%, Q28 -6.8% (likely PK-find inlining tightening the hot path,
    but plausibly run-to-run variance).
  - JSONBench EC2 (m6i.8xlarge, 100M Bluesky events): **3.582s vs prior
    3.603s** (-0.6% total). Per-query variance ±9% (Q4 -8.5%, Q3 +8.3% —
    consistent with run noise; segments.rs reads metadata only, JSON
    decode happens downstream).
- **Correctness:** `make correctness` 999 passed / 3 skipped / 6 xfailed
  (matches baseline). Unit: 451 pass on PG17 and PG18 (was 440).
  Integration: 234 pass on PG17 and PG18.
- **Perf opportunities surfaced:** none.

### 2026-05-16 — `src/scan/path.rs` — 426c1e8

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
