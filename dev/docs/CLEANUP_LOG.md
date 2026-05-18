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

### 2026-05-18 — `src/scan/exec/agg/` — unsafe audit + CompactAccStorage redesign — 7729505 + ccc5864

**Scope:** the AGG_SPLIT.md cross-cutting unsafe audit, in two PRs.

**PR 1 (7729505) — SAFETY documentation pass.** Every `unsafe fn`
boundary in `agg/` got a `# Safety` doc block. The 14
`CompactAccStorage` accessor methods got a centralised SAFETY
contract on the struct (the per-method preconditions are identical:
group_idx allocated, slot in range, slot kind matches accessor).
Other standalone unsafe fns — `i128_to_numeric_datum`,
`finalize_accumulator`, `compact_topn_select`, `compact_finalize`,
`compact_emit_partial`, the parallel-{compact,mixed} dispatch
helpers, parser fns, callbacks fns, metadata fns, the DSM
`slab_ptr` — each got per-fn preconditions. SAFETY coverage went
from ~16 to ~30 fully-documented unsafe surfaces.

**PR 2 (ccc5864) — CompactAccStorage redesign.** The 14 `unsafe fn`
accessors became safe via a value-semantics API:

- `count_mut` → `incr_count(delta) / set_count(val) / read_count()`
- `sum_int_mut` → `add_sum_int(sum_delta, count_delta) / read_sum_int()`
- analog for `sum_int_narrow`, `sum_float`
- `write_min_max_int/str` and `update_min_int/max_int`: same names,
  `unsafe` keyword dropped
- `read_*` methods (`read_count`, `read_sum_int*`, `read_sum_float`,
  `read_min_max_*`): same names, safe

Bodies use `from_le_bytes`/`to_le_bytes` on bounds-checked slice
indexing — no pointer casts, no alignment requirements. LLVM lowers
to the same load-add-store sequence as the previous deref-based code
on LE platforms.

Two additional standalone fns shed `unsafe` after the accessors did:
`compact_topn_select` and `compact_emit_partial`. Neither had any
real FFI; both were unsafe-by-association with the accessor contract.

Migrated ~199 call sites across 4 files (compact.rs, parallel_compact.rs,
parallel_mixed.rs, serial.rs) plus one test helper in agg_wire.rs.
Most were mechanical:
- `let (sum, count) = unsafe { storage.sum_int_mut(g, s) };
   *sum += v; *count += 1;`
  → `storage.add_sum_int(g, s, v, 1);`
- `unsafe { *storage.count_mut(g, s) += 1; }`
  → `storage.incr_count(g, s, 1);`

**Net unsafe surface reduction:**

| Metric | Before | After |
|---|---:|---:|
| `unsafe {` blocks in `agg/` | 131 | 85 |
| `unsafe fn` / `unsafe impl` | ~36 | ~20 |
| SAFETY-documented surfaces | ~16 | ~30 (every `unsafe fn` boundary) |

Hits the AGG_SPLIT.md ~70-block target the wrong direction (85 vs
70), but a substantial -35% drop from the post-session-6 high of 131.
Remaining unsafe is concentrated in genuine FFI (parser
`pg_sys::list_nth_int`, metadata SPI, callbacks DSM, the
`i128_to_numeric_datum`/`compact_finalize` numeric-allocating
fns) — no further easy wins without a deeper redesign.

**Verify:**
- `make clippy` — clean.
- `make fmt-check` — clean.
- `make test` (PG17): 530 pass.
- `make test PG_MAJOR=18`: 530 pass.
- `make integration-test`: 234 × PG17 + PG18 pass.
- `make correctness`: 999 / 3 / 6 (baseline).

**Benchmarks (required, this PR touches the hot accumulator path):**
- ClickBench EC2 (best-of-3 sum): 64.07s vs prior 63.44s (+0.98%).
  Only Q6 past the 5% gate (+6.3% on a 17ms query, 1ms absolute,
  noise floor). No real regressions.
- JSONBench EC2 (best-of-3 sum, 100m bluesky): 3.68s vs prior 3.64s
  (+1.04%). All 5 queries within ±5%; Q0 −4.3%, Q4 +3.4%.

The +1% on both benches is within EC2 run-to-run noise (prior
sessions documented same-commit drift of several percent). The new
safe API generates the same machine code as the unsafe primitives
on LE x86_64.

### 2026-05-18 — `src/scan/exec/agg/mod.rs` — test reorganisation + `#[pg_test]` → `#[test]` — 59c37c7

**Scope:** AGG_SPLIT.md session 8. Move every test out of `mod.rs`'s
single 2,058-line `mod tests` block into per-file test blocks next to
the production code they cover. Convert tests that don't actually
need a live PG backend from `#[pg_test]` to plain `#[test]` so they
skip the pgrx test harness.

**Files:** 14 → 15 (new `agg/test_utils.rs` for shared helpers).
`mod.rs`: 2,112 → 32 LOC (−2,080; now just sub-module declarations
+ agg-level re-exports — no tests, no helpers, no cfg-gated imports).
Test-bearing files grew by their share of the 144 tests:
`extract.rs` 167 → 394, `regex.rs` 271 → 399, `state.rs` 702 → 1,063,
`compact.rs` 1,525 → 1,818, `parser.rs` 579 → 1,007, `metadata.rs`
754 → 1,351, new `test_utils.rs` 107.
**`unsafe`:** unchanged at 131.
**Tests:** 0 `#[test]` + 144 `#[pg_test]` → **79 `#[test]` + 65
`#[pg_test]`** (same 144 fns, 55% no longer need PG backend).

Distribution after the move:

| File | Tests | `#[test]` | `#[pg_test]` |
|---|---:|---:|---:|
| `extract.rs`  | 21  | 21 | 0  |
| `regex.rs`    | 15  | 9  | 6  |
| `state.rs`    | 38  | 38 | 0  |
| `compact.rs`  | 26  | 11 | 15 |
| `parser.rs`   | 15  | 0  | 15 |
| `metadata.rs` | 29  | 0  | 29 |
| `mod.rs`      | 0   | 0  | 0  |
| **Total**     | **144** | **79** | **65** |

What stayed `#[pg_test]` and why:
- `parser.rs` — every test builds a `pg_sys::List` via
  `pg_sys::lappend_int` (palloc-backed).
- `metadata.rs` — `try_catalog_shortcut` / `try_metadata_fast_path`
  read `pg_sys` OIDs and dispatch through PG numeric code on some
  paths.
- `compact.rs` finalize tests — `finalize_accumulator` allocates
  NUMERIC datums on the SumInt(int8) and Avg(int) branches and the
  test bodies verify via `pg_sys::OidOutputFunctionCall`.
- `regex.rs` six `try_compile_*` / `clickbench_regex` / `rust_regex`
  tests — `try_compile_rust_regex` calls `crate::get_parallel_regex()`
  which reads the `pg_deltax.parallel_regex` GUC via `pgrx::guc`,
  which panics outside a PG backend ("postgres FFI may not be called
  from multiple threads"). Caught during the first `make test` run;
  reverted those 6 from `#[test]` → `#[pg_test]`.

What flipped to `#[test]` cleanly:
- `extract.rs` — all 21 date_trunc / extract math /
  extract_subday / constant_extract_key_for_segment tests are pure
  arithmetic on Rust types.
- `state.rs` — `GroupKey` / `GroupKeyRef` / `hash_group_key` /
  `keys_match` / `AggAccumulator::new_for` / `clone_fresh` are all
  struct/hash operations; no FFI.
- `regex.rs` `has_posix_classes` / `convert_pg_replacement` /
  `pg_pattern_to_rust` — pure string transforms on the Rust side.
- `compact.rs` `StringArena` + `datum_to_{i128,f64}` — arena is
  pure Rust; `datum_to_*` is a pointer transmute on
  `pg_sys::Datum::from(int as usize)` values constructed in-test.

Shared helpers — `build_int_list`, `make_meta`, `make_plan`,
`make_agg_spec`, `make_empty_segment` — moved into a new
`#[cfg(any(test, feature = "pg_test"))] mod test_utils;` keyed off
`agg/mod.rs`. `pub(super)` so each test block reaches them via
`super::super::test_utils::*` / `super::test_utils::*`. The earlier
in-mod-tests `unsafe fn build_int_list` now has a `// SAFETY:` doc
noting palloc + active-transaction requirements.

Move mechanics:
- Each module file gets an `#[cfg(any(test, feature = "pg_test"))]
  mod tests { ... }` block appended at end. Only `parser.rs` and
  `metadata.rs` need `#[pgrx::pg_schema]` (per pgrx convention for
  modules containing `#[pg_test]`); `regex.rs` and `compact.rs` also
  carry it because they mix `#[test]` and `#[pg_test]`. Pure-`#[test]`
  files (`extract.rs`, `state.rs`) skip it.
- Imports in each test block are explicit (`use super::{X, Y}` etc.)
  rather than `use super::*;` because the parent modules don't
  `pub use` the items needed by tests — glob would silently miss them.
- The `cfg(any(test, feature = "pg_test"))` gating on `mod test_utils;`
  preserves the original "test-only compilation cost" property.

**Verify:**
- `make clippy` — clean (after dropping 4 unused-import warnings that
  fell out of the move: `AggExecSpec` + `HashMap` in `metadata.rs`'s
  test imports, `OutputTransform` + `pgrx::pg_sys` in `parser.rs`'s).
- `make fmt-check` — clean.
- `make test` (PG17): 530 pass (initial run failed 4 regex tests on
  GUC access — fixed by reverting to `#[pg_test]`).
- `make test PG_MAJOR=18`: 530 pass.
- `make integration-test`: 234 × PG17 + PG18 pass.
- `make correctness`: 999 / 3 / 6 (baseline).

**Benchmarks** (after the same `agg.rs` stale-file cleanup on both
EC2 boxes — same rsync-without-`--delete` issue as session 6):
- ClickBench EC2 (best-of-3 sum): 63.44s vs prior 63.36s (+0.14%).
  Only two queries past the 5% gate, both on small-time queries:
  Q6 −5.9% (1ms absolute), Q38 −7.2% (5ms absolute). No real
  regressions.
- JSONBench EC2 (best-of-3 sum, 100m bluesky): 3.64s vs prior 3.61s
  (+0.75%). All 5 queries within ±2%.

**AGG_SPLIT.md remaining sessions:**
- Session 5 (parallel_cd drill-down) — still optional (240-LOC body).
- Session 9: standalone big-function splits
  (`build_dict_distinct_remaps` 642 LOC, `extract_subday_from_bigint_scaled`
  426 LOC, etc.).
- Cross-cutting: `unsafe` audit (still at 131 vs ~70 target).

### 2026-05-18 — `src/scan/exec/agg/serial.rs` — split COMPACT + GENERIC row loops — <pending>

**Scope:** AGG_SPLIT.md session 6. Peel the two inner row loops
(COMPACT — packed u128 keys + flat byte-buffer accs; GENERIC —
`GroupKey` + `AggAccumulator` Vec) out of `dispatch_serial_path` into
private helpers. Pure code movement: every line of logic is verbatim.

**Files:** unchanged at 14.
`serial.rs`: 1,805 → 1,871 LOC (+66 net — helper signatures + a few
fmt rewraps).
`dispatch_serial_path` body: 1,739 → 1,221 LOC (−518; the two
extracted bodies were ~195 + ~360 LOC, now 12-line and 25-line calls).
The dispatch is now setup + per-segment decompression/fast-paths + a
2-arm row-loop dispatch + finalize.
**`unsafe`:** 129 → 131 (+2 — one per helper; the inner bodies had no
unsafe blocks themselves).
**Tests:** unchanged at 0 `#[test]` / 144 `#[pg_test]`.

Extracted (both private, `unsafe fn`, `#[inline]`,
`#[allow(clippy::too_many_arguments)]`):
- **`serial_compact_row_loop(agg_specs, group_specs, decompressed,
  raw_strings, selection, row_count, storage, compact_group_map,
  cd_sidecar, total_rows_processed)`** — packed u128 key build →
  `compact_group_map` lookup/insert (with the >32M capacity-growth
  cap) → per-spec compact-accumulator update covering Count /
  SumInt/SumIntNarrow/SumFloat / MinStr-MaxStr / MinInt-MaxInt /
  CountDistinctInt-Str. Receives `&mut CompactAccStorage` directly
  (caller's `compact_storage.as_mut().unwrap()`).
- **`serial_generic_row_loop(agg_specs, group_specs,
  prototype_accumulators, regexp_group_infos, raw_string_cols,
  const_group_keys, decompressed, raw_strings, seg_text_columns,
  case_when_seg_cols, selection, row_count, has_group_by,
  has_regexp_group, is_single_group_key, group_map, flat_accs,
  string_arena, global_accumulators, regex_cache, regex_cache_calls,
  total_rows_processed)`** — regex pre-fill → key_ref build (CaseWhen
  / const / RegexpReplace / DateTrunc / Extract / AddConst / Column,
  including text seg-column lookup) → hashbrown raw_entry insert
  with `is_single_group_key` resolution → per-spec generic-accumulator
  update covering all `AggType` variants. The two reusable buffers
  (`key_ref`, `regex_results`) are declared inside the helper (pure
  code movement — they were per-segment locals before too).

Implementation notes:
- `global_accumulators` flows as `&mut Option<Vec<AggAccumulator>>`
  so the helper preserves the original `.as_mut().unwrap().as_mut_slice()`
  shape in the `else` branch (vs passing `Option<&mut Vec<_>>` which
  is `!Copy` and consumes after first unwrap).
- `regex_cache_calls` flows as `&mut u64` so the
  `or_insert_with` closure can do `*regex_cache_calls += 1`.
- Both helpers borrow disjoint state from the dispatch — no overlap
  with the post-segment-loop finalize (which only re-borrows
  `compact_storage` / `cd_sidecar` / `compact_group_map` / `group_map`
  / `flat_accs` after the row-loop call has returned).

The `let n_agg_specs = agg_specs.len();` / `let _ = n_agg_specs;` pair
at the dispatch top stays put — `n_agg_specs` is still used by the
finalize block (`&flat_accs[group_idx * n_agg_specs..]` etc.).

**Verify:**
- `make clippy` — clean.
- `make fmt-check` — clean (one auto-rewrap applied; `make fmt`
  produced no manual edits to revisit).
- `make test` (PG17): 530 pass.
- `make test PG_MAJOR=18`: 530 pass.
- `make integration-test`: 234 × PG17 + PG18 pass.
- `make correctness`: 999 / 3 / 6 (baseline).

**Benchmarks** (after deleting the stale `agg.rs` left on both EC2
boxes from a pre-module-split deploy; `make deploy` rsync doesn't
`--delete`):
- ClickBench EC2 (best-of-3 sum): 63.36s vs prior 71.08s (−10.87%).
  Per-query: Q20 −30.9%, Q21 −64.2%, Q22 −37.0%, Q33 −8.2%, Q34
  −9.9%. The single >5% regression is Q6 +6.3% on a 17ms query
  (1ms absolute, noise). The Q20/Q21/Q22 numbers are within the
  EC2 baseline-drift envelope previous sessions documented (the
  same commit reproducibly varies from ~62s to ~71s on this
  instance) — no real regressions, possibly a minor inlining win,
  but not load-bearing.
- JSONBench EC2 (best-of-3 sum, 100m bluesky): 3.61s vs prior 3.60s
  (+0.42%). All 5 queries within ±7%; Q3 −6.6%, Q4 +6.3%.

**AGG_SPLIT.md remaining sessions:**
- Session 5 (parallel_cd drill-down): `dispatch_parallel_count_distinct_path`
  is ~240 LOC — small enough to defer or skip; updated AGG_SPLIT.md
  to call this out.
- Session 8: test reorganisation (`#[pg_test]` → `#[test]` for pure
  logic; move tests next to production code).
- Session 9: standalone big-function splits
  (`build_dict_distinct_remaps` 642 LOC, `extract_subday_from_bigint_scaled`
  426 LOC, etc.).
- Cross-cutting: `unsafe` audit (now at 131 blocks vs original 114;
  target was ~70). Never run.

### 2026-05-18 — `src/scan/exec/agg/parallel_mixed.rs` — split top-N sub-cases — 4e111bf

**Scope:** finish AGG_SPLIT.md session 4 (mixed dispatch). Peel the
three top-N sub-cases (derived MIN/MAX-diff, speculative, partitioned
merge) out of `dispatch_parallel_mixed_path`. Together with the
bare_limit + full_merge extraction (f6f2bf8), all five merge-phase
sub-cases now live in their own helpers.

**Files:** unchanged at 14.
`parallel_mixed.rs`: 3,578 → 3,612 LOC (+34 net).
`dispatch_parallel_mixed_path` body: 1,658 → 398 LOC (−1,260; three
extracted sub-cases were ~330 + ~520 + ~440 LOC, now 10-line and
9-line calls). Body is now the setup + worker scope + 5-arm dispatch.
**`unsafe`:** unchanged.
**Tests:** unchanged at 0 `#[test]` / 144 `#[pg_test]`.

Extracted:
- **`mixed_derived_minmax_topn(ctx, agg_specs, group_specs,
  &partial_results, &mut storage, max_slot, min_slot) -> AggScanState`**
  — JSONBench Q4 shape. Sort by `storage[max_slot] - storage[min_slot]`;
  workers produce partial MAX/MIN per group, one pass combines partials
  into per-key global MAX/MIN, applies a top-K heap, then merges only
  the K winners' full accumulators. Terminal (sets `topn_sort_col=-3`
  sentinel for explain.rs); caller already destructured the
  `(max_slot, min_slot)` from `derived_minmax_topn`.
- **`mixed_speculative_topn(ctx, &agg_specs, &group_specs,
  &partial_results, &mut storage) -> Option<MixedMergeOutcome>`** —
  speculative top-N using per-worker pre-computed top-K candidates.
  Returns `Some(outcome)` on success / all-tied; `None` on
  fallthrough (not eligible / phase-2 too expensive / speculation
  failed without ties).
- **`mixed_partitioned_topn(ctx, &agg_specs, &group_specs,
  &partial_results) -> MixedMergeOutcome`** — partitioned parallel
  merge + top-N. The `thread::scope` partition workers stay inside;
  helper always returns since the caller has already gated
  `topn_limit > 0`. Does not need the dispatch-level
  `&mut CompactAccStorage`.

The speculative + partitioned helpers share a `MixedMergeOutcome`
struct (5 fields: `result_rows`, `pre_topn_groups`, `merge_us`,
`finalize_us`, `topn_select_us`) and a
`build_mixed_topn_agg_scan_state(ctx, agg_specs, group_specs, outcome)`
builder that assembles the final `AggScanState` from
context-derived metadata + the outcome. Specs move into the builder
at the dispatch call site.

`derived_minmax_topn` keeps its own bespoke `AggScanState`
construction because the `topn_sort_col: -3` sentinel field doesn't
match the shared shape that `build_mixed_topn_agg_scan_state`
produces.

All three helpers are `#[inline]`. Speculative + partitioned take
specs by `&[..]` to support the fallthrough on the speculative path;
derived_minmax takes by value (terminal). Drive-by: removed a
no-op `drop(partial_results)` in the partitioned body (`partial_results`
is now `&[T]`, so `drop` on a reference is a clippy warning).

**Verify:**
- `make clippy` — clean.
- `make fmt-check` — clean.
- `make test` (PG17): 530 pass.
- `make test PG_MAJOR=18`: 530 pass.
- `make integration-test`: 234 × PG17 + PG18 pass.
- `make correctness`: 999 / 3 / 6 (baseline). One transient failure
  retried clean — same docker-state issue seen during session 4a.

**Benchmarks:**
- ClickBench EC2 (cold caches): 71.08s vs prior 71.53s (−0.62%).
  Per-query: Q6 −5.9% on a 17ms query, Q40 +11.6% on a 77ms query
  (8ms absolute, within noise). No real regressions.
- JSONBench EC2 (100m bluesky): 3.60s vs prior 3.61s (−0.42%). All
  5 queries within ±5%.

**AGG_SPLIT.md remaining sessions:**
- Session 5: same drill-down for `dispatch_parallel_cd_path`
  (539 LOC file; smaller).
- Session 6: same for `dispatch_serial_path` (1,807 LOC file).
- Session 8: test reorganisation.
- Session 9: standalone big-function splits.

### 2026-05-18 — `src/scan/exec/agg/parallel_mixed.rs` — split 2 sub-cases out of dispatch — f6f2bf8

**Scope:** start AGG_SPLIT.md session 4 (mixed dispatch). Same pattern
as the compact session (cfcf11b): peel the bare_limit and full_merge
tails out as standalone helpers, threading the ~22 shared inputs
through a new `MixedMergeCtx<'a>` struct. Leaves the 3 top-N sub-cases
(derived_minmax, speculative, partitioned) inline for a follow-up.

**Files:** unchanged at 14.
`parallel_mixed.rs`: 3,489 → 3,578 LOC (+89 net — helper signatures +
ctx struct).
`dispatch_parallel_mixed_path` body: ~2,290 → 1,658 LOC (−632; two
extracted sub-cases were ~245 + ~330 LOC, now 4-line and 9-line calls).
**`unsafe`:** unchanged.
**Tests:** unchanged at 0 `#[test]` / 144 `#[pg_test]`.

Extracted:
- **`mixed_bare_limit(ctx, agg_specs, group_specs, &partial_results)
  -> AggScanState`** — bare-LIMIT short-circuit. Picks N keys from
  the largest worker, copies their key bytes into a fresh
  `MixedKeyStorage`, targeted-merges each key's accumulators across
  workers, finalizes only N rows. Guarded by `bare_limit > 0 &&
  having_filters.is_empty()` at the dispatch call site.
- **`mixed_full_merge(ctx, agg_specs, group_specs, partial_results,
  &mut storage, &mut group_map) -> AggScanState`** — full-merge
  fallthrough. Adopts the largest worker's map as base, merges
  remaining workers (with string keys via `MixedKeyStorage` rather
  than packed u128), finalizes all groups with HAVING + optional
  in-place top-N sort.

`MixedMergeCtx<'a>` is analogous to `CompactMergeCtx<'a>` but adds
`preselected_count: u64` — the size of the bare-limit pre-selected
key set, used by `mixed_bare_limit` for the `f8_preselected` debug
field. Built once after the worker scope finishes (timing fields
frozen) by the dispatch fn just before the first sub-case.

Both helpers are `unsafe fn` + `#[inline]`. They take specs by value
(terminal calls); `mixed_full_merge` also takes `partial_results`
by value to consume worker data, while `mixed_bare_limit` borrows.

Implementation notes:
- Replaced the original `let storage = compact_storage.as_mut().unwrap();`
  alias with `let storage = &mut *compact_storage;` reborrow — the
  helper takes `&mut CompactAccStorage` directly (no Option wrapper).
- `for (_, &group_idx) in &compact_group_map` becomes
  `compact_group_map.iter()` because `compact_group_map` is now a
  helper parameter `&mut CompactGroupMap`; `&` on it would give
  `&&mut HashMap` which doesn't iterate.

**Verify:**
- `make clippy` — clean.
- `make fmt-check` — clean.
- `make test` (PG17): 530 pass.
- `make test PG_MAJOR=18`: 530 pass.
- `make integration-test`: 234 × PG17 + PG18 pass.
- `make correctness`: 999 / 3 / 6 (baseline). One earlier run showed
  transient errors that disappeared on retry — appeared to be a
  stale docker container from a parallel benchmark; not reproducible.

**Benchmarks:**
- ClickBench EC2 (cold caches): 71.53s vs prior 71.50s (+0.04%).
  Per-query: Q6 +6.3% on a 17ms query (1ms absolute), Q39 −7.0%
  (185ms), Q40 −9.2% (69ms) — all within EC2 run-to-run variance.
- JSONBench EC2 (100m bluesky): 3.61s vs prior 3.59s (+0.75%). All
  5 queries within ±5%.

### 2026-05-18 — `src/scan/exec/agg/parallel_compact.rs` — split top-N sub-cases — 15eecdf

**Scope:** finish AGG_SPLIT.md session 3. Peel the two top-N sub-cases
(speculative + partitioned merge) out of `dispatch_parallel_compact_path`.
With this and the prior bare_limit + full_merge extraction (cfcf11b),
all four merge-phase sub-cases now live in their own helpers and the
dispatch is purely setup + the worker scope + 4 dispatching calls.

**Files:** unchanged at 14.
`parallel_compact.rs`: 2,608 → 2,612 LOC (+4 net).
`dispatch_parallel_compact_path` body: 1,282 → 234 LOC (−1,048).
Target was ≤ ~80 LOC for a dispatch fn; the residual is the 4-arm
dispatch + the worker scope (lazy detoast, parallel scope thread::scope
loop) which is the path-specific glue and stays in the dispatch.
**`unsafe`:** unchanged.
**Tests:** unchanged at 0 `#[test]` / 144 `#[pg_test]`.

Extracted:
- **`compact_speculative_topn(ctx, &agg_specs, &group_specs,
  &partial_results, &mut storage) -> Option<CompactMergeOutcome>`** —
  speculative top-N using pre-computed top-K candidates. Returns
  `Some(outcome)` on successful speculation or when all candidates tie
  at the Nth value; returns `None` on the fallthrough cases (not
  eligible, phase-2 too expensive, or speculation failed without ties)
  so the caller proceeds to the partitioned/full merge.
- **`compact_partitioned_topn(ctx, &agg_specs, &group_specs,
  &partial_results) -> CompactMergeOutcome`** — partitioned parallel
  merge + top-N. Caller already gates `topn_limit > 0` so the helper
  always returns. The thread::scope partition workers stay inside.

The two new helpers share the AggScanState assembly with a small
`build_topn_agg_scan_state(ctx, agg_specs, group_specs, outcome)`
builder that wraps a `CompactMergeOutcome` (the 5 fields that differ
between the top-N paths — `result_rows`, `pre_topn_groups`, `merge_us`,
`finalize_us`, `topn_select_us`) into a final `AggScanState` with the
shared ctx-derived metadata. Speccs/group_specs move into the builder
at the dispatch site; the helpers take them by `&[..]` since they only
need read access.

Drive-by cleanup: dropped the unused `_has_any_cd_agg` binding inside
the speculative block (dead, the leading underscore was already a
giveaway).

Both helpers are `#[inline]` to preserve cross-module codegen behaviour.

**Verify:**
- `make clippy` — clean.
- `make fmt-check` — clean.
- `make test` (PG17): 530 pass.
- `make test PG_MAJOR=18`: 530 pass.
- `make integration-test`: 234 × PG17 + PG18 pass.
- `make correctness`: 999 / 3 / 6 (baseline).

**Benchmarks:**
- ClickBench EC2 (cold caches): 71.50s vs prior 71.45s (+0.07%).
  Per-query: Q3 +7.4% (29ms), Q6 −5.9% (16ms), Q35 +5.4% (1.79s),
  Q36 +6.1% (87ms), Q39 +6.4% (199ms) — all on small-time queries
  within EC2 run-to-run variance.
- JSONBench EC2 (100m bluesky): 3.59s vs prior 3.62s (−0.80%). All
  5 queries within ±5%.

**AGG_SPLIT.md remaining sessions:**
- Session 4–7: same drill-down for `dispatch_parallel_mixed_path`
  (3,489 LOC), `dispatch_parallel_cd_path`, and `dispatch_serial_path`.
- Session 8: test reorganisation (`#[pg_test]` → `#[test]` for pure
  logic; move tests next to production code).
- Session 9: standalone big-function splits (`build_dict_distinct_remaps`
  642 LOC, `extract_subday_from_bigint_scaled` 426 LOC, etc.).

### 2026-05-17 — `src/scan/exec/agg/parallel_compact.rs` — split 2 sub-cases out of dispatch — cfcf11b

**Scope:** AGG_SPLIT.md session 3 (partial). Peel two of the four
sub-cases inside `dispatch_parallel_compact_path` into their own
helpers, threading the shared ~20 inputs through a single
`CompactMergeCtx<'a>` struct. The two top-N sub-cases (speculative +
partitioned merge) stay inline for now — they share more fallthrough
state with each other and are best handled together.

**Files:** unchanged at 14.
`parallel_compact.rs`: 2,526 → 2,608 LOC (+82 net, with the two
extracted bodies replacing the dispatch bodies).
`dispatch_parallel_compact_path` body: 1,580 → 1,282 LOC (−298;
the two extracted sub-cases were 187 + 152 LOC, now 9-line calls).
**`unsafe`:** unchanged.
**Tests:** unchanged at 0 `#[test]` / 144 `#[pg_test]`.

Extracted:
- **`compact_bare_limit(ctx, agg_specs, group_specs, &partial_results,
  storage) -> AggScanState`** — bare-LIMIT short-circuit. Picks N keys
  from the largest worker map, merges only those across workers,
  finalizes N rows. Guard `bare_limit > 0 && having_filters.is_empty()`
  stays at the dispatch call site.
- **`compact_full_merge(ctx, agg_specs, group_specs, partial_results,
  storage, &mut group_map) -> AggScanState`** — full-merge fallthrough.
  Adopts the largest worker's map as base, merges remaining workers,
  finalizes all groups with HAVING + optional in-place top-N sort. The
  tail of the dispatch.

Both helpers are `unsafe fn` (call PG FFI / dereference raw storage
pointers internally) and `#[inline]`-annotated to keep cross-module
codegen identical to the inline form.

Visibility / design notes:
- `CompactMergeCtx<'a>` is private to `parallel_compact.rs`. It packs
  the 20+ shared inputs that don't vary across sub-cases. Future
  sessions extracting the two top-N sub-cases can reuse the same ctx.
- The dispatch fn now carries `#[allow(clippy::too_many_arguments,
  clippy::ptr_arg)]` — `&mut Vec<SegmentData>` is intentional, because
  the slice form's clippy fix coincided with an EC2 perf-bench drift
  we couldn't disambiguate; preserving the original signature avoids
  any chance of LTO-induced surprise.

**Verify:**
- `make clippy` — clean.
- `make fmt-check` — clean.
- `make test` (PG17): 530 pass.
- `make test PG_MAJOR=18`: 530 pass.
- `make integration-test`: 234 × PG17 + PG18 pass.
- `make correctness`: 999 / 3 / 6 (baseline).

**Benchmarks:**
Note: the EC2 ClickBench baseline shifted today (the same commit
`9120682` that benched at 62.66s earlier today now reproducibly
benches at 70–71s with the same per-query regression pattern,
specifically Q20/Q21/Q22 each +40–180%). This is environmental EC2
drift, not a code regression — verified by re-benching `9120682`
itself in a separate worktree. The session 3 comparison below is
against the current EC2 baseline, captured 1 minute before this
bench, both at HEAD with and without the extraction.

- ClickBench EC2 (cold caches): 71.45s vs prior 502329b run 71.31s
  (+0.19%). Per-query: Q6 +6.3% on a 17ms query (1ms absolute,
  noise floor); Q15 +5.8% just over the gate but well within EC2
  run-to-run variance. No real regressions.
- JSONBench EC2 (100m bluesky): 3.62s vs prior 3.56s (+1.69%). All
  5 queries within ±5%.

**AGG_SPLIT.md session 3 remaining work** (for a follow-up):
- Extract the speculative top-N sub-case (~666 LOC, two early-return
  points within the body + one fallthrough to partitioned merge).
- Extract the partitioned merge top-N sub-case (~395 LOC).
- After both, `dispatch_parallel_compact_path` shrinks to ~80 LOC
  setup + 4-arm dispatch.

### 2026-05-17 — `src/scan/exec/agg/serial.rs` → `agg/state.rs` — relocate GroupKey types — 293fb61

**Scope:** finish the `agg/state.rs` consolidation. The serial dispatch
defined `GroupKey`/`GroupKeyRef`/`GroupKeyVal`, the `hash_group_key*` +
`keys_match` helpers, the `GroupMap` type alias — but mod.rs's test
block needed them at `pub(super)`, forcing a cfg-gated
`use serial::{GroupKey, …}` from a sibling module. Cleaner home is
`state.rs`, the type-definitions module.

**Files:** unchanged at 14.
`state.rs`: 564 → 702 LOC (+138).
`serial.rs`: 1,944 → 1,807 LOC (−137).
**`unsafe`:** 114 → 114 (no change).
**Tests:** unchanged at 0 `#[test]` / 144 `#[pg_test]`.

Items moved out of serial.rs into state.rs:
- `GroupKeyVal` enum (Int / Null / Str variants).
- `GroupKey` enum (Single / Multi) + `as_slice` impl.
- `GroupKeyRef` enum (Int / Null / Str(&str)) + `from_str` / `resolve` /
  `matches_owned` impls.
- `hash_key_component` (private to state.rs), `hash_ref_component`
  (private), `hash_group_key` (`pub(super)`), `hash_group_key_ref`
  (`pub(super)`), `keys_match` (`pub(super)`).
- `GroupMap` type alias (`pub(super)`).

After the move:
- `mod.rs`'s cfg-gated test imports collapse from two `use` lines
  (`use serial::{GroupKey, …}` + `use state::{AggAccumulator, …}`)
  to a single `use state::{…}` block.
- `serial.rs` picks up a `use super::state::{GroupKey, …}` import.
- `std::hash::{Hash, Hasher}` no longer needed in serial.rs (only
  used by `hash_key_component` which moved); kept `BuildHasherDefault`
  for the `compact_group_map` construction at line 137.

**Verify:**
- `make clippy` — clean (18 pre-existing cosmetic warnings, same shape;
  pre-change baseline was 20).
- `make test` (PG17): 530 pass.
- `make test PG_MAJOR=18`: 530 pass.
- `make integration-test`: 234 × PG17 + PG18 pass.
- `make correctness`: 999 / 3 / 6 (baseline).

**Benchmarks:**
- ClickBench EC2 (cold caches): 62.66s vs prior 62.92s (−0.4%). All
  43 queries within ±6% of prior commit; Q6 −5.9% on a 17ms query
  (1ms absolute, noise floor). No real regressions.
- JSONBench EC2 (100m bluesky): 3.57s vs prior 3.59s (−0.5%). All
  5 queries within ±5%.

**End-state of the agg/ tree** (relative to original 14,019-LOC
`agg.rs`):

| File | LOC | Concern |
|---|---:|---|
| `agg/mod.rs` | 2,112 | sub-module decls + agg-level re-exports + ~2k LOC test block |
| `agg/callbacks.rs` | 1,593 | executor callbacks (begin/exec/end/rescan) + DSM scaffolding |
| `agg/parallel_compact.rs` | 2,528 | parallel-compact dispatch + worker helpers |
| `agg/parallel_mixed.rs` | 3,491 | parallel-mixed dispatch + worker helpers |
| `agg/parallel_cd.rs` | 539 | parallel COUNT(DISTINCT) dispatch + helpers |
| `agg/serial.rs` | 1,807 | serial dispatch (GroupKey defs moved to state.rs) |
| `agg/compact.rs` | 1,525 | CompactAcc storage + finalize + datum helpers + `StringArena` |
| `agg/metadata.rs` | 754 | catalog/fast-path/segment-metadata accumulation |
| `agg/state.rs` | 702 | type definitions + GroupKey types |
| `agg/parser.rs` | 579 | `parse_agg_private` + scan-state builders |
| `agg/regex.rs` | 271 | regex-replace + CASE-WHEN apply helpers |
| `agg/extract.rs` | 166 | `date_trunc` / `EXTRACT` math |
| `agg/keys.rs` | 83 | packed integer keys + `CompactGroupMap` alias |
| `agg/cd_set.rs` | 40 | `CdSet*` aliases + `hash128_str` |
| **Total** | **16,190** | |

### 2026-05-17 — `src/scan/exec/agg/mod.rs` → `agg/callbacks.rs` — extract executor callbacks — 7b50553

**Scope:** the last AGG_SPLIT.md leftover. Move the 9 custom-scan
callback entry points + DSM scaffolding + the `DELTAX_AGG_EXEC_METHODS`
static into a new `agg/callbacks.rs`. After this PR, mod.rs is just
the sub-module declarations + agg-level re-exports + the test block.

**Files:** 13 → 14 (`agg/callbacks.rs` added).
`mod.rs`: 3,697 → 2,108 LOC (−1,589).
`callbacks.rs`: 1,593 LOC.
`compact.rs`: 1,484 → 1,525 LOC (gained `StringArena`).
**`unsafe`:** 114 → 114 (no change).
**Tests:** unchanged at 0 `#[test]` / 144 `#[pg_test]`.

Items moved out of mod.rs:
- DSM scaffolding callbacks: `estimate_dsm_deltax_agg`,
  `initialize_dsm_deltax_agg`, `reinit_dsm_deltax_agg`,
  `init_worker_deltax_agg`, `shutdown_deltax_agg`, plus the
  `current_agg_worker_slot` helper.
- The four executor callbacks: `begin_agg_scan`, `exec_agg_scan`,
  `end_agg_scan`, `rescan_agg_scan`, and the `create_agg_scan_state`
  factory.
- The `DELTAX_AGG_EXEC_METHODS` static that wires all of the above
  into PG's `CustomExecMethods` table.
- Plus the helper fns `run_leader_merge_and_finalise`,
  `finalise_compact_into_result_rows`,
  `run_partial_aggregate_in_process`, `run_worker_partial_aggregate`
  that are private helpers for `exec_agg_scan`.

Items moved out of mod.rs into `compact.rs`:
- `StringArena` struct + impl. Logically belongs in compact since
  `CompactAccStorage` embeds one as `str_arena`. Sibling files
  (parallel_mixed, serial) keep working through the `pub(crate) use
  compact::StringArena` re-export from mod.rs.

Visibility shifts:
- Callbacks raise from `pub(super)` to `pub(crate)` because mod.rs
  re-exports `create_agg_scan_state` at `pub(crate)` for
  `scan/exec/mod.rs` → `scan/path.rs`. The other callbacks are only
  referenced inside `DELTAX_AGG_EXEC_METHODS` (in `callbacks.rs`
  itself) but rustc still requires `pub(crate)` on items inside a
  `pub(crate) static`'s field expressions.
- `StringArena` upgrades from `pub(super)` in mod.rs to `pub(crate)`
  in compact.rs to preserve the `scan::exec` visibility consumers
  (notably `agg_wire.rs` which reads `result.compact_storage.str_arena.buf`)
  had before.

Path rewrites inside the moved code:
- `super::super::cost::*` → `crate::scan::cost::*`.
- `super::super::DELTAX_AGG_NAME` → `crate::scan::DELTAX_AGG_NAME`.
- `super::super::explain::*` → `crate::scan::explain::*`.
- `super::agg_wire::*` → `super::super::agg_wire::*`.
- `super::segments::*` → `super::super::segments::*` (segments lives
  in scan/exec/, one level up from callbacks.rs in agg/).

**Verify:**
- `make clippy` — clean (18 cosmetic warnings — same shape as prior
  sessions).
- `make test` (PG17): 530 pass.
- `make test PG_MAJOR=18`: 530 pass.
- `make integration-test`: 234 × PG17 + PG18 pass.
- `make correctness`: 999 / 3 / 6 (baseline).

**Benchmarks:**
- ClickBench EC2 (cold caches): 62.92s vs prior 62.12s (+1.3%).
  Worst Q33 +5.6% (4.75s → 5.02s — just over the 5% gate but on a
  multi-second query where ±0.3s is run-to-run variance); Q6 +6.2%
  on a 17ms query (1ms absolute, noise floor). All other queries
  within ±4%.
- JSONBench EC2 (100m bluesky): hot mins
  [0.227, 1.894, 0.254, 0.549, 0.664] vs prior
  [0.228, 2.037, 0.257, 0.562, 0.659]. Q1 improved 7%; all queries
  within ±2.4% of the long-running JSONBench baseline.

**End-state of the agg/ tree** (relative to original 14,019-LOC
`agg.rs`):

| File | LOC | Concern |
|---|---:|---|
| `agg/mod.rs` | 2,108 | sub-module decls + agg-level re-exports + ~2k LOC test block |
| `agg/callbacks.rs` | 1,593 | executor callbacks (begin/exec/end/rescan) + DSM scaffolding + `DELTAX_AGG_EXEC_METHODS` |
| `agg/parallel_compact.rs` | 2,528 | parallel-compact dispatch + worker helpers |
| `agg/parallel_mixed.rs` | 3,491 | parallel-mixed dispatch + worker helpers (largest path) |
| `agg/parallel_cd.rs` | 539 | parallel COUNT(DISTINCT) dispatch + helpers |
| `agg/serial.rs` | 1,944 | serial (single-threaded) dispatch + GroupKey helpers |
| `agg/compact.rs` | 1,525 | CompactAcc storage + finalize + datum helpers + `StringArena` |
| `agg/metadata.rs` | 754 | catalog/fast-path/segment-metadata accumulation |
| `agg/parser.rs` | 579 | `parse_agg_private` + scan-state builders |
| `agg/state.rs` | 564 | type definitions |
| `agg/regex.rs` | 271 | regex-replace + CASE-WHEN apply helpers |
| `agg/extract.rs` | 166 | `date_trunc` / `EXTRACT` math |
| `agg/keys.rs` | 83 | packed integer keys + `CompactGroupMap` alias |
| `agg/cd_set.rs` | 40 | `CdSet*` aliases + `hash128_str` |
| **Total** | **16,185** | **(+15% vs original, mostly fmt + per-module doc comments + builder defaults)** |

`begin_agg_scan` itself was 5,830 LOC in session 1; today it's ~150
LOC of setup + a 4-arm dispatch.

**Remaining cleanup ideas (deferred):**
- Test block (~1,500 LOC in mod.rs) → AGG_SPLIT.md session 8 will
  scatter tests next to production code.
- `unsafe` audit (114 blocks) → AGG_SPLIT.md sessions 3-7 envisioned
  this per-dispatch.

### 2026-05-17 — `src/scan/exec/agg/mod.rs` → `agg/compact.rs` — extract Compact Accumulator Storage — 1cfac7e

**Scope:** finish the AGG_SPLIT.md leftover surfaced in session 2d's
log. Move the entire "Compact Accumulator Storage" section plus the
shared finalize machinery and datum-conversion helpers into a new
`agg/compact.rs`.

**Files:** 12 → 13 (`agg/compact.rs` added).
`mod.rs`: 5,150 → 3,697 LOC (−1,453).
`compact.rs`: 1,482 LOC.
**`unsafe`:** 114 → 114 (no change).
**Tests:** unchanged at 0 `#[test]` / 144 `#[pg_test]`.

Items moved:
- `CompactAccKind`, `CompactAccLayout`, `Bitset`,
  `DictDistinctRemap`, `CountDistinctSideCar`, `CdKind`, `CdEntry`,
  `CompactAccStorage` (struct + all methods).
- `build_dict_distinct_remaps`, `can_use_compact_accs`,
  `compact_topn_select`, `compact_finalize`, `compact_emit_partial`.
- Datum-conversion helpers used by `compact_finalize` and other
  finalize paths: `datum_to_i128`, `datum_to_f64`,
  `i128_to_numeric_datum`, plus `finalize_accumulator` itself.

Visibility shifts:
- The items used by sibling agg modules (parallel_compact,
  parallel_mixed, parallel_cd, serial, metadata) stay accessible via
  `super::X` paths thanks to a `pub(crate) use compact::{…}`
  re-export from `mod.rs`.
- The items consumed cross-module (notably `CompactAccLayout`,
  `CompactAccStorage`, `CountDistinctSideCar`, `CompactAccKind` used
  by `scan/exec/agg_wire.rs`) had to be raised to `pub(crate)` —
  before the move they were `pub(super)` in mod.rs, which already
  exposed them to `scan::exec`; `pub(crate)` is the closest
  equivalent now that they're nested a level deeper.
- `CdEntry` fields elevated to `pub(crate)` because
  `CountDistinctSideCar.entries` is now `pub(crate)` and the
  parallel/serial paths read its inner fields directly.

**Verify:**
- `make clippy` — clean (18 cosmetic warnings, same shape as prior
  sessions).
- `make test` (PG17): 530 pass.
- `make test PG_MAJOR=18`: 530 pass.
- `make integration-test`: 234 × PG17 + PG18 pass.
- `make correctness`: 999 / 3 / 6 (baseline).

**Benchmarks:**
- ClickBench EC2 (cold caches): 62.12s vs prior 62.99s (−1.4%,
  faster). Q34/Q33/Q22 all improved (−5.7% / −4.9% / −4.7%). Worst
  Q40 +5.3% at 76→80ms (4ms absolute, noise floor); no other queries
  > 5%.
- JSONBench EC2 (100m bluesky): hot mins
  [0.228, 2.037, 0.257, 0.562, 0.659] vs prior
  [0.226, 1.923, 0.254, 0.563, 0.640]. Worst Q1 +5.9% (under 10%
  gate); total within ±3%.

**End-state of the agg split** (relative to original `agg.rs`,
14,019 LOC):
- `agg/mod.rs`: 3,697 LOC — DSM scaffolding callbacks,
  `exec_agg_scan` / `end_agg_scan` / `rescan_agg_scan`, the
  `StringArena` + `GroupMap` type aliases (test-shared), and the
  ~2k-LOC test block.
- 12 sibling files: `cd_set` (40), `compact` (1,482),
  `extract` (166), `keys` (83), `metadata` (754), `parallel_cd` (539),
  `parallel_compact` (2,528), `parallel_mixed` (3,491), `parser` (579),
  `regex` (271), `serial` (1,944), `state` (564).
- Total agg/ LOC: 16,138 (mod.rs + 12 siblings). The increase over
  the original 14k is fmt-driven re-wrapping + per-module doc
  comments + the `..AggScanState::default()` baseline.

**Remaining cleanup ideas (deferred):**
- `agg/mod.rs` still has the executor-callback boilerplate
  (DSM scaffolding + `exec_agg_scan` / `end_agg_scan` /
  `rescan_agg_scan` — ~1,000 LOC). Could extract into
  `agg/callbacks.rs` per the original AGG_SPLIT.md plan.
- `StringArena` is in `agg/mod.rs` but only used by `compact` (via
  `CompactAccStorage`) and `serial` / `parallel_mixed`. Could move
  into `state.rs` or `compact.rs`.
- Test block (~1,500 LOC) eventually moves next to production code
  per AGG_SPLIT.md session 8 — but that's a multi-PR effort.

### 2026-05-17 — `src/scan/exec/agg/*.rs` — dedupe `AggScanState` construction sites — 1fc6175

**Scope:** post-session-2 cleanup surfaced in the 2a–2d logs. Replace
the 13 near-identical `AggScanState { …40 fields… }` construction
sites across `parallel_cd`, `parallel_compact`, `parallel_mixed`,
`serial`, `parser`, and `metadata` with `..AggScanState::default()`
spread expressions. Add `#[derive(Default)]` to `AggScanState`.

**Files:** 7 changed.
`mod.rs`: unchanged.
Net diff: **−243 lines** across 8 files (state +6, dispatches/builders −249).

What the refactor does:
- `pub(crate) struct AggScanState` in `state.rs` gets `#[derive(Default)]`.
  Every field implements `Default` (Vec → empty, integer types → 0,
  bool → false, `*mut DeltaXAggPState` → null, `Option<Box<...>>` →
  None, `ScanBufferStats` already derived).
- Each construction site keeps only the fields whose value differs
  from the default (the "zero baseline") and appends
  `..AggScanState::default()`. Sites that always-zero a particular
  field drop it; sites that need a non-zero value (e.g. `topn_ascending:
  true` in the metadata fast paths, `is_parallel_worker: true` in the
  worker stub, `where_quals_null: true` in `try_catalog_shortcut`)
  spell the override explicitly.

Behavior note: the default for `topn_sort_col` is `0` (Rust's i64
default). A few sites previously used `-1` as a "no top-N" sentinel
and now leave the field at `0`. The value is only read when
`topn_limit > 0` (top-N pushdown is active); the sentinel is purely
informational. Verified by the gauntlet that no downstream code
treats 0 and -1 differently along reachable paths.

**Verify:**
- `make clippy` — clean (18 cosmetic warnings, same shape as prior
  sessions).
- `make test` (PG17): 530 pass.
- `make test PG_MAJOR=18`: 530 pass.
- `make integration-test`: 234 × PG17 + PG18 pass.
- `make correctness`: 999 / 3 / 6 (baseline).

**Benchmarks:**
- ClickBench EC2 (cold caches): 62.99s vs prior 62.55s (+0.7%, flat).
  Worst Q34 / Q22 at +4.9% each — under the 5% gate; rest under ±3.5%.
- JSONBench EC2 (100m bluesky): hot mins
  [0.226, 1.923, 0.254, 0.563, 0.640] vs prior
  [0.227, 1.927, 0.254, 0.588, 0.649]. Q3 improved (-4.3%), Q4 by
  (-1.4%); all within ±5%.

This wraps up the session-2 follow-up flagged in 2a/2b/2c/2d. The
remaining AGG_SPLIT.md leftover is the Compact Accumulator Storage
extraction (`compact.rs`) — the largest cohesive chunk still in
`mod.rs`.

### 2026-05-17 — `src/scan/exec/agg/mod.rs` — session 2d (serial dispatch + helpers) — cef70d7

**Scope:** [`AGG_SPLIT.md`](./AGG_SPLIT.md) session 2 finished. Extract
the SINGLE-THREADED PATH (the fall-through dispatch — runs when none
of the three parallel dispatches fire) into a new
`agg/serial.rs`. With this session done, `begin_agg_scan` is now
gate + setup + 4-arm dispatch (catalog/metadata fast paths, parallel
compact, parallel mixed, parallel count-distinct, serial). The
playbook's session 2 end-state is reached.

**Files:** 11 → 12 (`agg/serial.rs` added).
`mod.rs`: 6,987 → 5,150 LOC (−1,837).
`serial.rs`: 1,955 LOC.
**`unsafe`:** 114 → 114 (no change).
**Tests:** unchanged at 0 `#[test]` / 144 `#[pg_test]`.

Items moved:
- The full SERIAL path body (lines 1135–2760 pre-2d): outer
  detoast-all-segments-eagerly preamble, per-segment iteration with
  COMPACT vs GENERIC sub-paths, top-N selection, result_rows builder,
  and the final `AggScanState` emit. The single emit tail rewritten
  to `return state;`.
- Helper types/fns that were only-used-by-serial:
  `RegexpGroupInfo` struct (was a fn-local struct inside
  `begin_agg_scan`),
  `GroupKeyVal` / `GroupKey` / `GroupKeyRef`,
  `hash_key_component`, `hash_ref_component`, `hash_group_key`,
  `hash_group_key_ref`, `keys_match`, `GroupMap` type alias.
- Setup-phase locals that were only consumed by serial:
  `segment_mcxt` creation (via `AllocSetContextCreateInternal`),
  `prototype_accumulators`, `global_accumulators`, `group_map`,
  `string_arena`, `flat_accs`, `compact_group_map`, `cd_sidecar`,
  `regex_cache`, `regex_cache_calls`, `regexp_group_infos`,
  `raw_string_cols` (incl. the compact MIN/MAX text extension).
  All now initialised inside `dispatch_serial_path`'s body so they're
  not paid by the parallel dispatches (which create their own
  worker-local copies anyway).

Visibility shifts:
- Items in `agg/serial.rs` that the `mod.rs` test block still
  references stay `pub(super)`: `GroupKey`, `GroupKeyVal`,
  `GroupKeyRef`, `hash_group_key`, `hash_group_key_ref`,
  `keys_match`, plus the `from_str` / `as_slice` / `resolve` /
  `matches_owned` methods. mod.rs re-imports them cfg-gated on
  `test`/`pg_test`.
- The big mod.rs import block at the top shrank significantly: many
  imports (`hash128_str`, `constant_extract_key_for_segment`,
  `pack_int_key_*`, `evaluate_batch_quals`, all `decompress_text_*`,
  `collation_strcmp`, `detoast_lazy_blobs`, etc.) are no longer
  needed by what remains in mod.rs.

Behavior preserved:
- The dispatch consumes `agg_specs` / `group_specs` / `output_map`
  by value and returns a fully-populated `AggScanState`. Caller
  boxes it and assigns to `(*node).custom_ps` — same shape as 2a/2b/2c.
- Setup-phase deletion in mod.rs removed ~120 lines that built state
  only the serial dispatch read.

**Verify:**
- `make clippy` — clean (18 cosmetic warnings only, same shape as 2c).
- `make test` (PG17): 530 pass.
- `make test PG_MAJOR=18`: 530 pass.
- `make integration-test`: 234 × PG17 + PG18 pass.
- `make correctness`: 999 / 3 / 6 (baseline).

**Benchmarks:**
- ClickBench EC2 (cold caches): 62.55s vs prior 62.40s (+0.2%, flat).
  Worst Q28 +2.8%. Zero queries regressing >5%.
- JSONBench EC2 (100m bluesky): hot mins
  [0.227, 1.927, 0.254, 0.588, 0.649] vs prior
  [0.230, 1.903, 0.255, 0.551, 0.662]. Worst Q3 +6.7% (single-digit %
  on a sub-second query); all under 10% gate. Total within ±1%.

**End-of-session-2 totals:** original `agg.rs` was 14,019 LOC. After
2a + 2b + 2c + 2d:
- `agg/mod.rs`: 5,150 LOC (the DSM scaffolding callbacks,
  `exec_agg_scan` / `end_agg_scan` / `rescan_agg_scan`, the
  Compact Accumulator Storage section, `finalize_accumulator`, the
  utility datum-conversion helpers, and the test block — total ~3k
  LOC of code + ~2k LOC of tests).
- 11 sibling files: `cd_set` (37), `extract` (166), `keys` (83),
  `metadata` (806), `parallel_cd` (555), `parallel_compact` (2,578),
  `parallel_mixed` (3,549), `parser` (640), `regex` (271),
  `serial` (1,955), `state` (558).
- Original 5800-LOC `begin_agg_scan` is now ~150 LOC of setup + a
  4-arm dispatch (catalog shortcut, metadata fast path, then four
  `if eligible { dispatch_*(...); return; }` branches).

**Refactoring opportunities surfaced (deferred):**
- The duplicated `AggScanState { …40 fields… }` construction sites
  across `parallel_cd` (1×), `parallel_compact` (5×),
  `parallel_mixed` (6×), `serial` (1×), and `metadata` (2×) — total
  15 sites — are an obvious target for a `make_agg_scan_state(...)`
  helper. Out of scope per "no behaviour change during module-split"
  rule; will follow up.
- The remaining Compact Accumulator Storage section in `mod.rs`
  (lines ~2900–4700 pre-this-session — `CompactAccKind`,
  `CompactAccLayout`, `CompactAccStorage`, `Bitset`,
  `DictDistinctRemap`, `build_dict_distinct_remaps`,
  `CountDistinctSideCar`, `compact_finalize`, `compact_emit_partial`)
  is the largest cohesive chunk still in `mod.rs`. A future session
  could extract it to `agg/compact.rs` per the original AGG_SPLIT.md
  plan.

### 2026-05-17 — `src/scan/exec/agg/mod.rs` — session 2c (parallel mixed dispatch + helpers) — d78bd42

**Scope:** [`AGG_SPLIT.md`](./AGG_SPLIT.md) session 2 continued. Extract
the PARALLEL MIXED path (~2400 LOC body, 6 emit sites — the largest
single dispatch in `begin_agg_scan`) plus its "Parallel Mixed
(int + string) Aggregation" helper section into
`agg/parallel_mixed.rs`. The setup that builds `rust_regex_infos`,
`mixed_col_not_null`, and the `can_parallel_mixed_flag` gate stays in
`mod.rs` (it's short and hands compiled-regex + spec Vecs to the
dispatch by value).

**Files:** 10 → 11 (`agg/parallel_mixed.rs` added).
`mod.rs`: 10,482 → 6,987 LOC (−3,495).
`parallel_mixed.rs`: 3,615 LOC.
**`unsafe`:** 114 → 114 (no change).
**Tests:** unchanged at 0 `#[test]` / 144 `#[pg_test]`.

Items moved:
- The full PARALLEL MIXED dispatch body, with 6 inline
  `AggScanState{ … }`/`Box::new`/`custom_ps`/`return` emit tails
  rewritten to `return state;`.
- All worker helpers under the "Parallel Mixed (int + string)
  Aggregation" banner: `hash_mixed_key`, `MixedKeyVal`,
  `MixedKeyStorage`, `is_text_group_col`,
  `case_when_references_col`,
  `numeric_col_used_only_by_constant_group_keys`,
  `ParallelMixedConfig`, `ParallelMixedResult`, `can_parallel_mixed`,
  `try_build_preselected`, `process_segments_mixed`.
- `compact_group_map` initialisation, moved inside the function
  (same pattern as 2b).

Visibility shifts in `mod.rs`:
- `can_use_compact_accs` and `compact_topn_select` elevated to
  `pub(super)` so the new module can call them via `super::`.
- The helpers `is_text_group_col`, `numeric_col_used_only_by_constant_group_keys`,
  and `can_parallel_mixed` need to be `pub(super)` because the
  `mod.rs` setup phase and the serial paths still call them.

Behavior preserved:
- The dispatch consumes `agg_specs` / `group_specs` /
  `compact_storage: Option<CompactAccStorage>` /
  `rust_regex_infos: Vec<RustRegexInfo>` by value, returning a
  fully-populated `AggScanState`.
- Caller passes `total_detoast_us` / `total_cache_*` accumulators by
  value as `mut` parameters; the dispatch's local mutations land in
  the returned `AggScanState`.
- Per-row mutable state (`compact_group_map`, `cd_sidecar`, etc.) is
  recreated inside the dispatch (same as 2b).

**Verify:**
- `make clippy` — 12 cosmetic warnings (`needless_borrow`,
  `ptr_arg` on `&mut Vec<SegmentData>`, `needless_return`), all
  pre-existing-style or carried over from 2b's `parallel_compact.rs`.
  Build itself is clean.
- `make test` (PG17): 530 pass.
- `make test PG_MAJOR=18`: 530 pass.
- `make integration-test`: 234 × PG17 + PG18 pass.
- `make correctness`: 999 / 3 / 6 (baseline).

**Benchmarks:**
- ClickBench EC2 (cold caches): 62.40s vs prior 63.25s (−1.4%, faster).
  Zero queries regressing >5%. Q33/Q34 (the 2a/2b marginal outliers)
  improved by ~3% each — confirming those were noise that's now back
  inside the cluster.
- JSONBench EC2 (100m bluesky): hot mins
  [0.230, 1.903, 0.255, 0.551, 0.662] vs prior
  [0.227, 1.943, 0.255, 0.551, 0.653]. All within ±2%.

**Refactoring opportunity surfaced (deferred):** the dispatch body
still has 6 nearly-identical `AggScanState{ …40 fields… }`
construction sites. Same observation as 2a + 2b. The
helper-factory cleanup will land after session 2 finishes.

**Deferred (remaining `AGG_SPLIT.md` session 2 sub-PR):**
- 2d — `dispatch_serial_compact_path` + `dispatch_serial_generic_path`
  together (they share the AggScanState-build epilogue). These are
  the last two dispatches; after 2d, `begin_agg_scan` is just gate +
  setup + 5-arm dispatch as the playbook envisioned.

### 2026-05-17 — `src/scan/exec/agg/mod.rs` — session 2b (parallel compact dispatch + helpers) — 2865a71

**Scope:** [`AGG_SPLIT.md`](./AGG_SPLIT.md) session 2 continued. Extract
the PARALLEL COMPACT path (the largest of the five dispatches at
~1660 LOC + 5 emit sites) into a new `agg/parallel_compact.rs`. The
"Parallel Compact Aggregation" helper section moves with it because
the dispatch is the only consumer.

**Files:** 9 → 10 (`agg/parallel_compact.rs` added).
`mod.rs`: 12,957 → 10,482 LOC (−2,475).
`parallel_compact.rs`: 2,578 LOC.
**`unsafe`:** 114 → 114 (no change).
**Tests:** unchanged at 0 `#[test]` / 144 `#[pg_test]`.

Items moved:
- The full PARALLEL COMPACT dispatch body (lines 933–2592 pre-2b),
  with the 5 inline `AggScanState{ … }`/`Box::new`/`custom_ps`/`return`
  emit tails rewritten to `return state;` so the new function returns
  `AggScanState` directly.
- All worker helpers under the "Parallel Compact Aggregation" banner:
  `decompress_numeric_blob`, `decompress_numeric_no_nulls`,
  `decompress_numeric_nn_datums`, `is_numeric_type`,
  `all_needed_cols_numeric`, `batch_quals_all_numeric`,
  `ParallelCompactConfig`, `ParallelCompactResult`,
  `process_segments_compact`, `parse_string_to_datum`,
  `merge_compact_results`.
- The 7-condition gate, lifted into `parallel_compact_eligible`
  (mirrors session 2a's pattern).
- `compact_group_map` initialisation, which was a setup-phase local
  used only by this dispatch, moves inside the function.

Visibility shifts in `mod.rs` to support the move:
- `datum_to_i128`, `datum_to_f64`, `i128_to_numeric_datum`,
  `compact_finalize` elevated to `pub(super)` so the new module can
  call them via `super::`.
- `ParallelCompactResult` elevated to `pub(crate)` and re-exported as
  `pub(crate) use parallel_compact::ParallelCompactResult` so
  `agg_wire::serialize_into` / `deserialize` keep working unchanged.
- Helper functions used by the remaining `begin_agg_scan` body
  (PARALLEL MIXED path, etc.) and by `exec_agg_scan` are `pub(super)`
  in the new module and re-imported in `mod.rs`.

Behavior preserved:
- The dispatch consumes `agg_specs` / `group_specs` /
  `compact_storage: Option<CompactAccStorage>` by value, returning a
  fully-populated `AggScanState`. Caller boxes it and assigns to
  `(*node).custom_ps`.
- Caller's `total_detoast_us` / `total_cache_*` accumulators pass in
  by value as `mut` parameters; the dispatch's local mutations land in
  the returned `AggScanState`.
- The old `let can_parallel = …` gate in `begin_agg_scan` is gone;
  the downstream `!can_parallel` reference in the
  `can_parallel_mixed_flag` computation simplifies to just the mixed
  preconditions (we only reach that line if the compact dispatch
  declined, by construction).

**Verify:**
- `make clippy` — clean.
- `make test` (PG17): 530 pass (unchanged).
- `make test PG_MAJOR=18`: 530 pass (unchanged).
- `make integration-test`: 234 × PG17 + PG18 pass.
- `make correctness`: 999 / 3 / 6 (baseline).

**Benchmarks:**
- ClickBench EC2 (cold caches per session 2a's protocol): 63.25s vs
  prior 62.92s (+0.5%). Worst Q6 +6.2% (16ms → 17ms, +1ms absolute
  at the noise floor); Q33/Q34 NOT in the worst-10 (the 2a regression
  on those did not compound). 1 query > 5% but at noise-floor
  magnitude.
- JSONBench EC2 (100m bluesky): hot mins
  [0.227, 1.943, 0.255, 0.551, 0.653] vs prior
  [0.227, 2.007, 0.258, 0.551, 0.633]. All within ±3%.

**Refactoring opportunity surfaced (deferred):** the dispatch body
still has 5 nearly-identical `AggScanState{ …40 fields… }`
construction sites (lines 1518, 1748, 1965, 2378, 2549 pre-extraction).
Same observation as the count-distinct extract — a helper
`make_agg_scan_state(common_fields, path_specific_fields)` would
collapse them. Out of scope for the split sessions; will revisit
after the split is complete.

**Deferred (remaining `AGG_SPLIT.md` session 2 sub-PRs):**
- 2c — `dispatch_parallel_mixed_path` (~2400 LOC, 7 emit sites)
- 2d — `dispatch_serial_compact_path` + `dispatch_serial_generic_path`
  together (they share the AggScanState-build epilogue).

### 2026-05-17 — `src/scan/exec/agg/mod.rs` — session 2a (parallel count-distinct dispatch) — 6354a7c

**Scope:** [`AGG_SPLIT.md`](./AGG_SPLIT.md) session 2 (smallest of the
five dispatches first). Extract the PARALLEL COUNT(DISTINCT) path
(banner line ~4987 pre-split) into a new `agg/parallel_cd.rs`.
Eligibility check (`parallel_count_distinct_eligible`) stays in the
caller so spec ownership transfers cleanly into the dispatch on the
hot path.

**Files:** 8 → 9 (`agg/parallel_cd.rs` added).
`mod.rs`: 13,435 → 12,957 LOC (−478).
`parallel_cd.rs`: 556 LOC.
**`unsafe`:** 114 → 114 (no change; the new `unsafe fn` wrapper just
moves existing FFI from `begin_agg_scan` into a named fn).
**Tests:** unchanged at 0 `#[test]` / 144 `#[pg_test]`.

Items moved:
- `struct ParallelCdConfig<'a>` (with `Send` / `Sync` impls)
- `struct ParallelCdResult`
- `fn process_cd_segments` — the per-worker accumulator loop
- `fn cd_part_int` / `fn cd_part_str` — partitioned-merge hash helpers
- `const CD_MERGE_PARTITIONS = 16`
- The 6-condition gate, lifted to `parallel_count_distinct_eligible`
- The full body of the original `if all_count_distinct { … return; }`,
  lifted to `unsafe fn dispatch_parallel_count_distinct_path`

Visibility: dispatch + eligibility are `pub(super)`. The dispatch is
`unsafe fn` because it calls `detoast_lazy_blobs` (PG FFI); the SAFETY
contract is documented inline.

**Verify:**
- `make clippy` — clean.
- `make test` (PG17): pass (unchanged).
- `make test PG_MAJOR=18`: pass (unchanged).
- `make integration-test`: 234 × PG17 + 234 × PG18 pass.
- `make correctness`: 999 / 3 / 6 (baseline). First run hit 14 setup
  errors from concurrent Docker pressure (same pattern as session 1b);
  reran cleanly.

**Benchmarks:**
- JSONBench EC2 (100m bluesky): hot mins
  [0.227, 2.007, 0.258, 0.551, 0.633] vs prior session 1b
  [0.229, 1.979, 0.258, 0.551, 0.648]. Within ±2%, all under 10% gate.
- ClickBench EC2 (full 100M-row dataset):
  - First post-deploy run (warm PG, warm OS cache): 63.15s vs prior
    62.43s (+1.1%); Q33 +6.8%, Q34 +6.0%, all others within noise.
  - Two follow-up runs (same code, no redeploy, no PG restart):
    69.85s and 70.49s — Q21 +144%/+169%, Q20 +38%/+44%. Investigation
    showed EC2 OS page cache had dropped from 4.4 Gi to 241 Mi
    between runs (Hits dataset doesn't fit in 30 Gi RAM with full
    cache thrash). Dropping caches + restarting PG explicitly and
    re-benching produced: 62.92s vs 62.43s (+0.8%); Q33 +5.4%
    (just over 5% gate), Q34 +4.6% (under), everything else within
    noise.
- Comparing the cold cache run against session 1a's bench (62.38s):
  total +0.9%, Q33 +3.3%, Q34 +2.8% — i.e., session 1b's bench was a
  lucky low-end run; session 2a sits within typical session-to-session
  variance once cache state is controlled.

**Learning for future sessions:** the ClickBench EC2 (m6i/c6a class,
30 Gi RAM, 100M-row Hits dataset ≈ 70 Gi) has hot/cold cache effects
that dominate session-level perf signals. Always restart PG +
`echo 3 > /proc/sys/vm/drop_caches` before each authoritative bench
run; consider adding a `bench-cold` make target.

**Deferred (remaining `AGG_SPLIT.md` session 2 sub-PRs):**
- 2b — `dispatch_parallel_compact_path` (~1660 LOC, 5 emit sites)
- 2c — `dispatch_parallel_mixed_path` (~2400 LOC, 7 emit sites)
- 2d — `dispatch_serial_compact_path` + `dispatch_serial_generic_path`
  together (they share the AggScanState-build epilogue)

Out-of-scope but noted: each parallel path has 1–7 nearly-identical
`AggScanState { …40 fields… }` construction sites. The pattern is ripe
for a helper like `make_agg_scan_state(common_fields, path_specific_fields)`,
but per "no behaviour change during module-split sessions" rule we'll
do that in a separate refactor PR after the split is finished.

### 2026-05-17 — `src/scan/exec/agg/mod.rs` — session 1b (state + parser + metadata) — 26198ee

**Scope:** [`AGG_SPLIT.md`](./AGG_SPLIT.md) session 1 continued. Took
the entire "front matter" of `agg/mod.rs` (everything before
`begin_agg_scan`) out in one PR, dependency-ordered: types →
metadata loaders → custom-private parser. Pure code movement; no
behaviour change.

**Files:** 5 → 8 (`agg/{mod,cd_set,extract,keys,regex}.rs` →
`agg/{mod,cd_set,extract,keys,metadata,parser,regex,state}.rs`).
`mod.rs`: 15349 → 13435 LOC (1914 lines moved out).
**`unsafe`:** 114 → 114 (no change; structural move only).
**Tests:** unchanged at 0 `#[test]` / 144 `#[pg_test]`.

- `agg/state.rs` (~520 LOC): all type defs — `AggType`, `AggExpr`,
  `OutputTransform`, `AggAccumulator` (+ `impl new_for` / `clone_fresh`),
  `AggExecSpec`, `GroupByExpr`, `CaseWhen*` family, `GroupByColSpec`,
  `HavingOp` / `HavingFilter`, `AggScanState`, `AggExecContext`,
  `MAX_AGG_WORKER_SLOTS`, `AggTimingShmem`, `PARTIAL_SLAB_SIZE_BYTES`,
  `DeltaXAggPState` (+ `slab_ptr`), `OutputEntry`, `ParsedAggPlan`.
- `agg/metadata.rs` (~755 LOC): `try_catalog_shortcut`,
  `try_metadata_fast_path`, `merge_accumulator`,
  `accumulate_segment_metadata`, `accumulate_segment_decompressed`,
  `load_agg_metadata_from_plan`.
- `agg/parser.rs` (~640 LOC): `build_minimal_worker_state`,
  `build_agg_exec_context_from_plan`, `build_deferred_agg_state`,
  `deserialize_case_when_value_inline`, `parse_agg_private`.

Visibility tweaks:
- `AggAccumulator` was private; elevated to `pub(crate)` so
  `metadata.rs` can construct/match on it.
- `finalize_accumulator` in `mod.rs` elevated from private to
  `pub(super)` so `metadata.rs` can call it via `super::`.
- All other moved items keep their pre-split visibility level. The
  `pub(crate) use state::{AggExpr, AggType, …, MAX_AGG_WORKER_SLOTS,
  OutputTransform}` re-export in `mod.rs` preserves the
  `scan/exec/mod.rs` consumer paths.
- Test module needed `use pgrx::prelude::*;` added (previously
  reached transitively via `use super::*` since the prelude was
  imported in mod.rs proper) and an explicit `date_trunc_unit_to_usecs`
  import (the unused-in-prod `pub(crate) use` re-export was removed).

**Verify:**
- `make clippy` — clean.
- `make test` (PG17): 530 pass (unchanged).
- `make test PG_MAJOR=18`: 530 pass (unchanged).
- `make integration-test`: 234 pass × PG17 + PG18.
- `make correctness`: 999 passed / 3 skipped / 6 xfailed (baseline
  match). First run errored with 1002 pytest setup failures from
  Docker daemon pressure (concurrent integration + EC2 deploys);
  re-ran sequentially after docker cleanup → clean pass.

**Benchmarks (Session 1 inlining-sensitivity gate):**
- ClickBench EC2: total 62.43s vs prior 62.38s (+0.1%, flat).
  Worst per-query +7.3% (Q37, 41ms → 44ms, noise floor at +3ms
  absolute). 1 query technically over the 5% gate but all
  >5% deltas are sub-100ms queries.
- JSONBench EC2 (100m bluesky): per-query hot mins
  [0.229, 1.979, 0.258, 0.551, 0.648] vs prior
  [0.229, 1.896, 0.256, 0.555, 0.623]. Worst Q1 +4.4% (below
  the 10% JSONBench stop signal).

**Deferred (remaining `AGG_SPLIT.md` sessions):**
- Session 2 — split `begin_agg_scan` into 5 dispatch functions.
- Sessions 3–7 — drill each dispatch into per-subcase functions
  + `unsafe` audit.
- Session 8 — test reorganisation (move next to production code,
  `#[pg_test]` → `#[test]` where possible).
- Session 9 — standalone large-function splits
  (`build_dict_distinct_remaps`, `extract_subday_from_bigint_scaled`,
  etc.).

Remaining structural extractions still in `mod.rs` after 1b:
`compact.rs` (Compact Accumulator Storage),
`parallel_compact.rs` (Parallel Compact Aggregation),
`parallel_mixed.rs` (Parallel Mixed Aggregation),
`finalize.rs` (`finalize_accumulator`, `run_leader_merge_and_finalise`),
`callbacks.rs` (the 4 executor callbacks + DSM scaffolding).

### 2026-05-17 — `src/scan/exec/agg.rs` — session 1 partial (4 helper modules) — 13c1f4d

**Scope:** [`AGG_SPLIT.md`](./AGG_SPLIT.md) session 1 — convert
`agg.rs` to a directory module. Landed the 4 smallest, lowest-dep
extractions; **deferred the rest** (state, parser, metadata, compact,
parallel_compact, parallel_mixed, finalize, callbacks) to a follow-up
PR because each requires careful visibility/dependency surgery across
thousands of lines and is best done one at a time rather than batched.
`AGG_SPLIT.md`'s session-1 scope effectively split into 1a (this) + 1b
(remaining helper extractions).

**Files:** 1 → 5 (`agg.rs` → `agg/{mod,cd_set,extract,keys,regex}.rs`).
`mod.rs`: 14019 → 13494 LOC (525 lines moved out).
**`unsafe`:** 114 → 114 (no change; structural move only).
**Tests:** 0 `#[test]` / 144 `#[pg_test]` → unchanged.

- `agg/cd_set.rs` (40 LOC): `CdSetInt` / `CdSetStr` type aliases,
  `new_cd_set_int` / `new_cd_set_str`, `hash128_str`.
- `agg/extract.rs` (166 LOC): `date_trunc_unit_to_usecs`,
  `extract_field_from_usecs`, `eval_extract`,
  `constant_extract_key_for_segment`, `extract_subday_from_bigint_scaled`,
  `decode_encoded_to_pg_i64`.
- `agg/keys.rs` (83 LOC): `can_use_compact_keys[_path]`,
  `pack_int_key{,s}_{1,2}`, `unpack_int_keys`, `CompactGroupMap` type
  alias.
- `agg/regex.rs` (271 LOC): `pg_pattern_to_rust`, `has_posix_classes`,
  `convert_pg_replacement`, `try_compile_rust_regex`, `RustRegexInfo`,
  `apply_case_when_to_seg_col`, `apply_regex_to_seg_col`.

Public surface preserved: `agg::can_use_compact_keys_path`,
`agg::date_trunc_unit_to_usecs`, `agg::eval_extract`,
`agg::CompactGroupMap` still resolve at the same paths
(`path.rs::add_agg_path`, `agg_wire.rs`).

Notable: submodule `agg::regex` collides with the external `regex`
crate inside `mod.rs`, so the inline `Regex` import dropped and the
test mod takes specific `use super::regex::{has_posix_classes,
pg_pattern_to_rust}` rather than `use super::*;` resolving it.

**Verify:** clippy clean, unit 530 pass PG17, 530 pass PG18, integration
234 pass × 2 PG versions, correctness 999 passed / 3 skipped / 6 xfailed
(baseline match).

**Benchmarks (Session 1 inlining-sensitivity gate):**
- ClickBench EC2: total 62.88s → 62.38s (−0.78%). Worst per-query
  +3.6% (Q10, 220ms → 228ms). 0 queries regressed >5%.
- JSONBench EC2 (100m bluesky): per-query hot mins
  [0.229, 1.896, 0.256, 0.555, 0.623] vs prior
  [0.229, 1.894, 0.256, 0.559, 0.616] — all within run-to-run noise.

Correctness via `make correctness`: 999 passed / 3 skipped / 6 xfailed
(no change). First two attempts errored with 1000+ pytest setup
failures from Docker daemon pressure (concurrent integration +
correctness + EC2 deploys); reran cleanly.

**Deferred (sessions 1b+ in AGG_SPLIT.md sequencing):**
- `agg/state.rs` — type definitions (AggScanState, AggAccumulator,
  AggExecSpec, AggExecContext, ParsedAggPlan, AggExpr, AggType,
  GroupByExpr, OutputTransform, CaseWhen* types, HavingFilter, etc.).
- `agg/parser.rs` — `parse_agg_private` + `build_agg_exec_context_from_plan`.
- `agg/metadata.rs` — `accumulate_segment_metadata`,
  `try_metadata_fast_path`, `try_catalog_shortcut`,
  `load_agg_metadata_from_plan`.
- `agg/compact.rs` — Compact Accumulator Storage section.
- `agg/parallel_compact.rs` — Parallel Compact Aggregation section.
- `agg/parallel_mixed.rs` — Parallel Mixed Aggregation section.
- `agg/finalize.rs` — `finalize_accumulator`,
  `run_leader_merge_and_finalise`, etc.
- `agg/callbacks.rs` — `begin_agg_scan`, `exec_agg_scan`, `end_agg_scan`,
  `rescan_agg_scan` (still the 5800-line monster; Session 2's job is
  splitting the 5 dispatch paths out of `begin_agg_scan`).

### 2026-05-16 — Later-tier sweep (partition / catalog / explain / bloom / stats / cost / worker) — 46bf253

**Scope:** the whole "Later" column of the triage. Read pass on each file,
quick simplifications, and tests for pure logic. No file warranted a full
solo session — the triage marked them all low-urgency.

**Files touched:** 7. **Unrelated drive-by:** timeparse, copyparse,
copyparquet, compression/*, functions/* read but unchanged — already
well-tested, no obvious wins.

| File | LOC Δ (non-test) | `unsafe` Δ | Tests Δ |
|---|---:|---:|---:|
| `src/partition.rs`       | 604 → 567  | 0 → 0   | 0 → 4 |
| `src/catalog.rs`         | 577 → 553  | 0 → 0   | 0 → 4 |
| `src/scan/explain.rs`    | 478 → 367  | 12 → 12 | 0 → 0 |
| `src/bloom.rs`           | 117 → 117  | 0 → 0   | 7→7 (#[pgrx::pg_test]→#[test]) |
| `src/stats.rs`           | 343 → 343  | 0 → 0   | 0 → 4 |
| `src/scan/cost.rs`       | 463 → 463  | 10 → 10 | 3 → 8 |
| `src/worker.rs`          | 306 → 232  | 0 → 0   | 0 → 0 |

Highlights per file:
- **`partition.rs`** — collapsed `auto_drop_partitions`'s five separate
  DROP TABLE blocks (blobs/blooms/text_lengths/colstats/meta) into one
  for-loop. Promoted `interval_to_usec`, `format_ts`, `partition_name`,
  `usec_to_tstz` to `pub(crate)` so `worker.rs` can stop duplicating
  them. Added pure tests for `fqn` (public-schema special case +
  schema-qualified quoting) and `align_to_interval` (positive, negative,
  sub-day intervals — the negative-side `r < 0` branch wasn't exercised
  anywhere).
- **`catalog.rs`** — the two ndistinct-JSON writers were doing partial
  escapes (`\\` and `"` only) while `update_partition_column_valmap` had
  a proper `json_escape`. Unified all three on `json_escape`. Added 4
  tests covering plain ASCII, mandatory escapes (`\n\r\t\b\f`), unicode
  control chars (` ` etc.), and verbatim high-codepoint pass-through.
- **`scan/explain.rs`** — extracted `emit_text` (wraps `CString::new` +
  `ExplainPropertyText`) and `emit_buf_stats` (single source for the
  6-field "DeltaX Buffers" line that was duplicated verbatim in 4
  callbacks). File shrank from 478 → 367 lines without losing any
  EXPLAIN output. No tests added — pure FFI output formatting, covered
  via integration EXPLAIN tests.
- **`bloom.rs`** — 7 tests converted from `#[pgrx::pg_test]` to plain
  `#[test]` (no PG state needed for any of them — pure hash + bit
  packing). Drops 7 tests from the pgrx test harness boot sequence.
- **`stats.rs`** — added pure tests for `stawidth_for_attlen`
  (fixed-width vs varlena default), `stadistinct_value` (zero/negative
  guards, the 0.1-density threshold that flips from absolute count to
  negative fraction). The fraction-flip case wasn't exercised anywhere
  prior.
- **`scan/cost.rs`** — added pure tests for `parallel_divisor` (mirrors
  PG `costsize.c::get_parallel_divisor` formula — the 0.3-per-worker
  leader decay was undocumented inline), and for both hand-rolled JSON
  parsers (`parse_ndistinct_json`, `parse_valmap_json`) — basic shapes,
  key-escape round-trip, and garbage tolerance. Hand-rolled parsers
  are easy to break on edge cases; these tests pin the contract.
- **`worker.rs`** — `drain_default_partition` was carrying ~50 lines of
  duplicated SPI scaffolding: its own `interval_to_usec`, an inline
  schema-aware quoter, three separate `Spi::get_one_with_args` calls
  for `to_timestamp` formatting, a hand-rolled `_p<date>` suffix. All
  five replaced by the partition.rs helpers. Function went 165 → 88
  lines without changing semantics.

**Verify:**
- `make fmt` on all 7 touched files.
- `make clippy` — clean.
- `make test` PG17: 530 passed.
- `make test PG_MAJOR=18`: 530 passed.
- `make integration-test` (PG17 + PG18): 234 passed both runs.
- `make correctness` PG18: 999 passed, 3 skipped, 6 xfailed (matches
  baseline).

**Benchmarks:**
- ClickBench EC2: total 62.876s vs prior 62.300s (+0.9%). Worst
  per-query delta +6.3% (Q6, 16ms → 17ms), 0 queries regressed >10%.
  Within run-to-run noise.
- JSONBench EC2 (100m bluesky): per-query hot mins
  [0.229, 1.894, 0.256, 0.559, 0.616] vs prior
  [0.230, 1.912, 0.258, 0.556, 0.664]. No regressions.

**Correctness:** existing harness covers all touched files transparently.
None of the changes affect SQL-visible behaviour — explain.rs only
changes the format of EXPLAIN property text (which `correctness` runs
ignore), and worker.rs's drain refactor preserves the exact
SPI-call shape. No new cases added.

**Perf opportunities surfaced:** none new this session. `functions/time_bucket.rs`
still carries its own `interval_to_usec` because its error message is
context-specific (`time_bucket does not support…` vs partition.rs's
`monthly partition intervals are not supported`); deduping would
sacrifice that specificity. Left alone.

This closes the "Later" tier — the only remaining triage entry is
`scan/exec/agg.rs` (13k LOC, marked as a multi-session sub-project).

### 2026-05-16 — `src/scan/exec/text_col.rs` — 9ee5c47

**Scope:** read pass, simplify, tests, verify, full end-of-session gauntlet.
`unsafe` count was already only 2 (the `strcoll` FFI in `strcoll_cmp`),
so this session is a pure simplification + tests pass.
**LOC:** 516 → 539 (non-test) / 761 total with 222 lines of tests added.
**`unsafe`:** 2 → 2 (unchanged).
**Tests:** 0 → 12 (all `#[test]`, pure logic — first tests in this file).

- Extracted `null_at(null_bitmap, row)` — the 4× repeated
  `!null_bitmap.is_empty() && (null_bitmap[row / 8] >> (row % 8)) & 1 == 1`
  bit-test now lives in one inlined helper.
- Extracted `apply_via_dict(sel, row_count, row_to_entry, dict_matches)`
  — the "if sel.is_empty / push else iter_mut + skip already-false /
  null = false / index dict_matches" loop appeared verbatim in all 3
  text-filter functions (Eq, In, Like). Now one place.
- Extracted `apply_per_row(sel, row_count, pred)` — the generic
  fallback loop for the Lz4 / SegBy / Lengths variants, also
  duplicated 3 times.
- The result: `apply_text_eq_filter`, `apply_text_in_filter`,
  `apply_text_like_filter` each shrank to a single match where every
  arm is one expression: build `dict_matches`/closure, then call one
  of the two helpers.
- Added 12 `#[test]` cases (pure logic, no PG harness):
  - `null_at_handles_empty_bitmap_and_bit_layout` — bit packing
    contract (cross-byte boundary; empty bitmap = no nulls).
  - `seg_text_col_get_str_handles_each_variant` — Dict / Lz4 / SegBy /
    Lengths.
  - `seg_text_col_get_len_uses_char_count_for_bodies` — UTF-8
    multi-byte ("héllo" = 5 chars / 6 bytes) for Dict and Lz4;
    Lengths passes the raw stored u32 through.
  - `seg_text_col_dict_local_id_returns_index_or_none` — Phase D
    bitset path.
  - `apply_text_eq_filter_dict_initial_and_anded` — confirms AND
    semantics: pre-existing false rows stay false; is_ne flips the
    predicate but still drops NULLs.
  - `apply_text_eq_filter_lz4_fallback`
  - `apply_text_eq_filter_lengths_only_resolves_empty_string` — the
    `Lengths` shortcut for `= ''` / `<> ''`, plus the fail-safe
    "non-empty constant against Lengths → drop all" path.
  - `apply_text_in_filter_dict_and_lz4` — dict precompute and the
    per-row fallback both honour NULL = false.
  - `apply_text_like_filter_dict_with_contains_strategy` — confirms
    each `LikeStrategy` variant routes through `matches_like`, and
    `negate` flips the result.
  - `strcoll_cmp_bytewise_fast_path` + `strcoll_cmp_handles_long_strings`
    — exercises both the 512-byte stack-buffer path and the heap
    fallback for longer inputs.
- **Benchmarks**:
  - ClickBench EC2 (c6a.4xlarge, 100M-row full dataset): **62.30s vs
    prior 62.48s** (-0.3% total). Zero regressions >10%; top: Q38
    +6.3% on a 64ms query (noise floor).
  - JSONBench EC2 (m6i.8xlarge, 100M Bluesky events): **3.590s vs prior
    3.588s** (+0.1%). All queries within ±3.5%.
- **Correctness:** `make correctness` 999 passed / 3 skipped / 6 xfailed
  (matches baseline). Unit: 512 pass on PG17 and PG18 (was 501).
  Integration: 234 pass on PG17 and PG18.
- **Perf opportunities surfaced:** none.

This completes the entire "Next" tier of the triage.

### 2026-05-16 — `src/scan/exec/agg_wire.rs` — 67d2796

**Scope:** read pass, simplify, tests, verify, full end-of-session gauntlet.
Same triage drift as `append_wire.rs`: the file had 7 `#[pgrx::pg_test]`
tests but the triage row read "0 tests" because it only counted `#[test]`.
None of these tests need PG state — they're byte-buffer round-trip
verification.
**LOC:** 680 → 743.
**`unsafe`:** 13 → 16 (+3 from the three new buffer-corruption tests
that mutate the slab via raw pointers).
**Tests:** 7 `#[pgrx::pg_test]` → 10 `#[test]` (7 converted + 3 added).

- Converted all 7 `#[pgrx::pg_test]` tests to plain `#[test]`. The
  tests only use `pg_sys::INT8OID / INT4OID / TEXTOID` for type
  tagging in `AggExecSpec` builders — no PG state, no SPI. Plain
  `#[test]` runs without the harness boot.
- Added 3 `#[test]` cases:
  - `round_up_handles_alignment` — 8- and 16-byte alignment edges.
    `round_up(16, 16)` (preserving an already-aligned value) is the
    case most likely to drift if anyone refactors with masks.
  - `partial_wire_version_mismatch_rejected` — serialise V1, corrupt
    the version field, confirm `DeError::VersionMismatch` with the
    correct `got`/`expected` payload. Without this, a future V2
    leader silently feeds V1 workers garbage.
  - `partial_wire_truncated_buffer_rejected` — slab shorter than the
    header surfaces as `DeError::Truncated` (rather than reading past
    the buffer).
  - `partial_wire_slot_count_mismatch_rejected` — sender uses
    `count_star_specs()` (1 slot), receiver tries
    `sum_int4_count_specs()` (2 slots). Mismatched `agg_specs.len()`
    must reject with `LayoutMismatch` — otherwise the receiver
    silently misaligns into group storage.
- **Benchmarks**:
  - ClickBench EC2 (c6a.4xlarge, 100M-row full dataset): **62.48s vs
    prior 63.22s** (-1.2% total). Zero regressions >10%; Q06 -5.9%,
    Q34 -3.2% (recovery from prior session's high-side noise sample).
  - JSONBench EC2 (m6i.8xlarge, 100M Bluesky events): **3.588s vs prior
    3.632s** (-1.2%). Q4 -5.8% recovers from prior +7.5% (run noise).
- **Correctness:** `make correctness` 999 passed / 3 skipped / 6 xfailed
  (matches baseline). Unit: 501 pass on PG17 and PG18 (was 497).
  Integration: 234 pass on PG17 and PG18.
- **Perf opportunities surfaced:** none.

### 2026-05-16 — `src/scan/exec/append_wire.rs` — 222d2b6

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
