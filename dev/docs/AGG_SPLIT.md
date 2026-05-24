# Splitting `scan/exec/agg.rs`

`src/scan/exec/agg.rs` is the largest file in the project (14k LOC) and is
flagged in [`CLEANUP_PLAN.md`](./CLEANUP_PLAN.md) as a multi-session
sub-project. This document is the playbook: what to do, in what order, and
what "done" looks like at the end of each session.

The methodology + per-file checklist + end-of-session gauntlet from
`CLEANUP_PLAN.md` still apply — this file only adds the agg-specific
sequencing. Log each session in [`CLEANUP_LOG.md`](./CLEANUP_LOG.md) like
any other cleanup session.

## Current state (snapshot at plan time)

- 14,019 LOC in a single file
- 67 top-level functions
- 114 `unsafe` blocks
- 144 `#[pg_test]`, zero plain `#[test]`
- One executor callback — `begin_agg_scan` — is **~5,830 lines on its own**

## Diagnosis

### `begin_agg_scan` is five execution paths inlined

Inside `begin_agg_scan` (lines 2244–8075), five paths are separated by
`// ===` banners:

| Banner | Line span | LOC |
|---|---|---:|
| PARALLEL COMPACT PATH         | 2758–4119 | ~1360 |
| PARALLEL MIXED PATH           | 4120–6110 | ~1990 |
| PARALLEL COUNT(DISTINCT) PATH | 6111–6541 | ~430  |
| SINGLE-THREADED PATH (compact)| 6542–7203 | ~660  |
| SINGLE-THREADED PATH (generic)| 7204–8075 | ~870  |

Each path further branches on speculative top-N / bare LIMIT /
partitioned-merge / full-merge / derived-minmax-topn (the `// ----`
sub-banners inside each path).

### Outside `begin_agg_scan`: clean banner sections that read like modules

The rest of the file already has natural section boundaries:

- **DeltaXAgg helpers** (lines 70–570): codecs, cd-set, EXTRACT/date_trunc
- **Parallel DSM scaffolding** (571–1097)
- **Executor callbacks** (1098–8818): begin/exec/end/rescan
- **Compact Accumulator Storage** (8819–10061)
- **Packed Integer Keys** (10062–10127)
- **Parallel Compact Aggregation** (10128–10917)
- **Parallel Mixed Aggregation** (10918–12207)
- **Tests** (12208–14019)

### Other large standalone functions

| Function | LOC |
|---|---:|
| `build_dict_distinct_remaps`         | 642 |
| `extract_subday_from_bigint_scaled`  | 426 |
| `parse_agg_private`                  | 349 |
| `process_segments_compact`           | 324 |
| `try_metadata_fast_path`             | 278 |
| `run_worker_partial_aggregate`       | 249 |
| `finalize_accumulator`               | 198 |
| `new_cd_set_str`                     | 173 |

## Session plan

One PR per session. End-of-session gauntlet (unit on PG17+18, integration,
correctness, ClickBench EC2, JSONBench EC2) after every session.

Most sessions are pure code movement, so benchmarks should be flat — but
**verify**: module boundaries shift compiler inlining decisions, so an
EC2 benchmark sanity-check is required even for "pure moves".

---

### Session 1 — module conversion (no behaviour change)

Convert `src/scan/exec/agg.rs` → `src/scan/exec/agg/` with `mod.rs`
re-exporting the public API. Move sections along their existing banners:

```
agg/mod.rs              — re-exports + wiring
agg/state.rs            — AggScanState, AggAccumulator, AggExecSpec,
                          AggExecContext, ParsedAggPlan
agg/parser.rs           — parse_agg_private + parse_* helpers
agg/extract.rs          — date_trunc, extract_*,
                          constant_extract_key_for_segment
agg/cd_set.rs           — new_cd_set_int/str + hashing
agg/metadata.rs         — accumulate_segment_metadata, try_metadata_fast_path,
                          try_catalog_shortcut, load_agg_metadata_from_plan
agg/compact.rs          — Compact Accumulator Storage +
                          build_dict_distinct_remaps
agg/keys.rs             — Packed Integer Keys helpers
agg/regex.rs            — apply_case_when_to_seg_col, apply_regex_to_seg_col,
                          pg_pattern_to_rust, try_compile_rust_regex
agg/parallel_compact.rs — Parallel Compact Aggregation entry points
agg/parallel_mixed.rs   — Parallel Mixed Aggregation entry points
agg/finalize.rs         — finalize_accumulator, run_leader_merge_and_finalise,
                          compact_finalize, compact_emit_partial
agg/callbacks.rs        — begin_agg_scan, exec_agg_scan, end_agg_scan
                          (still a monster — split in Session 2)
agg/tests/              — split tests by which production module they cover
```

Only changes: `use` statements + `pub(super)` / `pub(crate)` visibility tweaks.
Logic is byte-for-byte verbatim. **Highest-risk session for benchmark surprises**
because cross-module visibility moves around the inlining boundary — run both
EC2 benches and compare every single query against the prior history file.

Stop signal: if any ClickBench query regresses >5% or any JSONBench query
regresses >10%, investigate before merging — adding `#[inline]` to the right
helper usually closes the gap.

---

### Session 2 — split `begin_agg_scan` into 4 dispatch functions

Inside `agg/callbacks.rs`, extract one fn per parallel banner section
plus one combined serial fn (the two single-threaded banners share
all of their per-segment setup, so they collapse to one dispatch with
a 2-arm row-loop branch inside — Session 6 then peels that branch
out as helpers):

```rust
fn dispatch_parallel_compact_path(ctx: &mut ExecCtx, …) -> AccumResults { … }
fn dispatch_parallel_mixed_path(ctx: &mut ExecCtx, …) -> AccumResults { … }
fn dispatch_parallel_count_distinct_path(ctx: &mut ExecCtx, …) -> AccumResults { … }
fn dispatch_serial_path(ctx: &mut ExecCtx, …) -> AccumResults { … }
```

Each body is verbatim — this is purely lifting. The hard part is figuring
out the right `ExecCtx` parameter shape so each dispatch fn gets exactly
what it needs without dragging in 30 unrelated locals. Start by counting
the live-at-banner variable set at each banner line.

After this session:
- `begin_agg_scan` is the gate + setup + a 4-arm match (<500 lines).
- Each `dispatch_*` lives in its own file (400–2000 lines apiece).

---

### Sessions 3–6 — drill into each dispatch (one session per path)

For each `dispatch_*` function, peel its inner sub-cases out:

```rust
fn parallel_compact_topn_speculative(…)
fn parallel_compact_bare_limit(…)
fn parallel_compact_partitioned_merge_topn(…)
fn parallel_compact_full_merge(…)
fn parallel_compact_derived_minmax_topn(…)
```

…and the equivalent set for `parallel_mixed`. For `serial`, the
extracted helpers are the two inner row-loops (`serial_compact_row_loop`
+ `serial_generic_row_loop`) since both COMPACT and GENERIC sub-paths
share the per-segment decompression/fast-path setup. For
`parallel_count_distinct`, the dispatch was already ~240 LOC after
Session 2 so no drill-down is needed — Session 5 in the original plan
is skipped.

Each sub-case ends up as a 100–400-line function.

This is also where the **`unsafe` audit** happens. Many of the 114 unsafe
blocks wrap operations we already have safe handles for (slice indexing
through a raw pointer when a `&[T]` is in scope). For each block:

1. Can the operation be expressed safely? (`slice::from_raw_parts` →
   pass a `&[T]` in; `Datum::value()` instead of pointer casts; pgrx
   safe wrappers instead of raw `pg_sys::*`.)
2. If unsafe must stay, can the block shrink to just the FFI op?
3. Does it have a `// SAFETY:` comment naming the invariant? If not, add one.

Realistic target: 114 → ~70 unsafe blocks. **Status after the audit
(2026-05-18 / `ccc5864`)**: 131 → 85 (-35%). The 14 hot-path
`CompactAccStorage` accessor methods moved to a safe
`from_le_bytes`/`to_le_bytes` API; `compact_topn_select` and
`compact_emit_partial` lost their `unsafe` keyword. Every remaining
`unsafe fn` boundary has a `# Safety` doc block. The remaining 85
blocks are concentrated in genuine FFI (PG list ops, SPI catalog
reads, NUMERIC datum allocation, DSM raw-pointer writes) — no
further easy wins without a deeper redesign of segment loading or
catalog access.

---

### Session 8 — test reorganisation **(done)**

- Tests moved out of `mod.rs`'s 2,058-LOC `mod tests` block into per-
  file `mod tests` blocks next to the production code they cover.
  `mod.rs` shrank from 2,112 → 32 LOC (just sub-module decls +
  re-exports).
- 79 of 144 tests flipped from `#[pg_test]` to `#[test]` (target was
  60–80) — pure-Rust math, struct ops, hash, arena, regex
  translation, datum pointer casts. 65 stayed `#[pg_test]`: parser
  tests build `pg_sys::List` via palloc; metadata tests dispatch
  through PG numeric code; six `compact.rs` finalize tests allocate
  NUMERIC datums on the SumInt(int8) and Avg(int) branches; six
  `regex.rs` `try_compile_*` / `clickbench_regex` / `rust_regex` tests
  read the `pg_deltax.parallel_regex` GUC (caught when the initial
  conversion panicked with "postgres FFI may not be called from
  multiple threads").
- Shared helpers (`build_int_list`, `make_meta`, `make_plan`,
  `make_agg_spec`, `make_empty_segment`) live in a new
  `#[cfg(any(test, feature = "pg_test"))] mod test_utils;` keyed off
  `agg/mod.rs`.
- **Not done in this session**: focused tests for each
  `parallel_compact_*` / `serial_*` sub-case exposed in Sessions 3–6.
  Adding tests was a "while we're here" goal; the core wins
  (organisation + harness cost cut) are independent. Leave for
  whoever next touches one of those helpers.

---

### Session 9 — standalone large-function splits

Files are individually reviewable by now. Tackle the leftover oversize
functions, one or two per PR if they're tangled:

- **`extract_subday_from_bigint_scaled` (426 lines)** — almost certainly a
  giant match on `unit`; extract per-unit helpers.
- **`build_dict_distinct_remaps` (642 lines)** — split by remap kind.
- **`parse_agg_private` (349 lines)** — per-AggKind sub-parsers.
- **`process_segments_compact` / `_mixed`** — peel the segment-loop body out.
- **`try_metadata_fast_path` (278 lines)** — per-shape fast-path matchers.
- **`finalize_accumulator` (198 lines)** — per-AggKind finaliser.
- **`new_cd_set_str` (173 lines)** — likely splittable into hash setup +
  bucket allocation.

## Working rules

Mostly inherited from `CLEANUP_PLAN.md`. Repeated here for emphasis:

- **No behaviour changes during module-split sessions.** If you spot a real
  bug while moving code, file a separate ticket and a separate PR. Bury
  nothing inside a cleanup.
- **One PR per session.** Each PR title names the session:
  `cleanup: agg split session 1 — module conversion`, etc.
- **Bench every session.** Module boundaries shift inlining. Cheap-looking
  splits can regress hot paths in surprising ways. ClickBench + JSONBench
  EC2 after each session; record headline numbers in the log.
- **No new abstractions** that aren't load-bearing. If the only reason
  `ExecCtx` exists is to make the dispatch signatures look clean, don't
  introduce it — pass the args explicitly.
- **Don't chase test counts.** Each new test should fail for a real bug.
- **`#[inline]` is the escape hatch for cross-module perf regressions.**
  Use it sparingly and only when the bench measurably regresses.

## End-state targets

| Metric | Before | Target | After Session 8 |
|---|---:|---:|---:|
| Largest file in `agg/` | 14,019 LOC | ≤ ~1500 LOC | 3,602 (`parallel_mixed.rs`) |
| Largest function       | ~5,830 LOC | ≤ ~400 LOC  | ~1,221 (`dispatch_serial_path`) |
| `unsafe` blocks        | 114        | ~70         | 85 (audit done; FFI bottom) |
| `#[test]` (cheap)      | 0          | ~80         | **79** ✓ |
| `#[pg_test]` (PG state)| 144        | ~70         | **65** ✓ |
| Files in `agg/`        | 1          | ~13         | 15 ✓ |

## When to update this doc

- A session takes a different shape than planned — update the session note
  (don't rewrite history; just say what actually happened in `CLEANUP_LOG.md`).
- A new pattern emerges that should generalise — add it to **Working rules**.
- A session is skipped or reordered — note it inline so the next session
  picks up cleanly.

Otherwise leave the plan alone and write in the log.
