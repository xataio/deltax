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

### 2026-05-16 — `src/scan/json_extract.rs` — pending

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
