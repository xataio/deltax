# Cleanup & Hardening Plan

Now that the feature set has stabilized, we want to spend a stretch of sessions
making the code easier to live with: simpler, smaller `unsafe` surface, better
unit-test coverage, and fewer dark corners. This document is the playbook.

It is **deliberately file-by-file**. Each session picks one file (or a tightly
coupled small group) from the triage table, runs the checklist below, and logs
the result in [`CLEANUP_LOG.md`](./CLEANUP_LOG.md).

Progress tracking lives in `CLEANUP_LOG.md`, not here. This document should stay
stable — update it only when the methodology changes.

## Goals

1. **Simplify.** Delete dead code, collapse premature abstractions, shrink
   functions that grew organically past the point of readability. The
   "[[simplify]]" skill is a good companion for the final pass.
2. **Shrink `unsafe`.** Every `unsafe` block that can become safe should
   become safe. The blocks that must stay unsafe should be small, focused, and
   carry a `// SAFETY:` comment that names the invariant.
3. **Improve unit-test coverage.** Prefer `#[pg_test]` for anything that
   touches PG state, plain `#[test]` for pure logic. Aim for coverage of the
   non-obvious branches, not line-coverage theater.
4. **Lint and warnings.** Consider any lint suppressions
   (`#[allow(...)]`, `#[cfg_attr(..., allow(...))]`): can they be removed?
   `make clippy` must be clean on every session.
5. **Tighten correctness.** Where a file has subtle invariants (segment
   layout, decompression bounds, planner mutation), encode them in tests or
   debug assertions.
6. **Document while the context is fresh.** If a function's contract is
   non-obvious, leave one short comment. Don't write essays.
7. **Format.** Run `rustfmt` via `make fmt FILE=<path>` (uses stock
   `cargo fmt` defaults inside the dev container — no `rustfmt.toml`).
   Format **only the file you touched** in a given session, so the global
   format churn lands gradually rather than as one giant diff.

## Performance Is a Hard Constraint

**Performance must not regress.** This is the single most important rule.
Cleanup that makes the code prettier but slows down a benchmark is not
acceptable.

Concretely:

- A "simplification" that allocates where the previous code didn't, adds a
  bounds check inside a hot loop, or replaces a `memcpy`-shaped loop with a
  general iterator chain — these are *not* simplifications for our purposes.
  Leave them alone.
- Replacing `unsafe` with safe code is only a win if the safe version
  compiles to equivalent (or better) code. When in doubt, check the
  assembly or, more practically, benchmark.
- If you spot a **performance opportunity** during cleanup:
  - **Simple and obvious** (e.g. hoisting a constant out of a loop, avoiding
    a redundant copy, replacing `Vec::push` in a sized loop with
    `Vec::with_capacity` + push, fixing an `O(n²)` that should be `O(n)`):
    just do it, in the same session. Mention it in the log.
  - **More involved** (changing a data layout, introducing SIMD, reworking
    an algorithm, anything that needs design): **don't do it inline**.
    Surface it to the user as a follow-up — short description, where it
    lives, and what the expected gain is. Land the cleanup as-is.

When in doubt, ask before merging. A cleanup PR that needs a benchmark
explanation is fine; one that *regresses* a benchmark is not.

## Test Suites & Benchmarks

We have four lines of defense. Cleanup work should both **lean on them** to
catch regressions and **grow them** where they're thin.

### Unit tests — `#[test]` and `#[pg_test]` in `src/`

- **Run:** `make test` (and `make test PG_MAJOR=18` before merging).
- **Goal during cleanup:** grow this suite. Most files in the triage have
  zero unit tests. Adding tests for non-obvious branches is the single
  most useful thing a cleanup session can produce.
- **Use `#[pg_test]`** when PG state is required (catalog access, SPI,
  custom scan execution). **Use `#[test]`** for pure logic — it's faster
  and runs without the pgrx harness.

### Integration tests — `tests/`

- **Run:** `make integration-test` (or `PG_VERSIONS=17` for a single
  version while iterating).
- Python + pytest + psycopg. Exercises SQL-visible behavior end-to-end.
- **Goal during cleanup:** add coverage for SQL-visible behavior that
  isn't currently exercised, especially around partition management, the
  background worker, and the direct-backfill `COPY` path (`copy.rs` has
  zero unit tests *and* light integration coverage).

### Correctness tests — `tests/correctness/`

- **Run:** see `dev/docs/CORRECTNESS_TESTING.md`.
- Compares pg_deltax results against **vanilla PostgreSQL** as the source
  of truth. This is our strongest defense against subtle decompression /
  planner-rewrite bugs.
- **Goal during cleanup:** every time a session touches a file that
  affects query results (`scan/exec/*`, `compress.rs`, `scan/json_extract.rs`,
  `scan/path.rs`, `scan/hook.rs`), check whether the correctness harness
  already exercises the relevant code path. If not, add a case. New cases
  should target whatever the file actually decides — qual evaluation,
  Top-N, aggregate fast paths, JSON extraction, segment pruning, parallel
  scans.

### Benchmarks — ClickBench, RTABench, JSONBench

- **Run from time to time** (not every session) to confirm no regressions.
  See the Build & Test section of `CLAUDE.md` for the exact commands.
- **Required** for any session that touches the scan/exec path,
  compression, or planner integration. The session log should record the
  result: "clickbench local: no regression", "rtabench Q17 within noise",
  etc.
- **Optional** for sessions that only touch glue code (catalog, partition,
  worker, copyparse), unless something in the diff makes you uncertain.
- **Local first, EC2 if needed.** Local Docker benchmarks (~2 min for
  ClickBench, ~1–2 min for RTABench) catch most regressions. Spin up the
  full EC2 runs only when local results are ambiguous or the change is
  large enough that the small dataset doesn't exercise it.

## Non-Goals

- **No refactors that change observable behavior** without an explicit
  decision. Cleanup PRs should be reviewable as cleanup.
- **No new features** smuggled in under a cleanup banner.
- **No re-architecting** of the planner hook, custom scan node, or
  compression pipeline. Those decisions live in the design docs under this
  directory; if cleanup surfaces a design question, raise it separately.
- **No coverage-chasing**: don't add tests that lock in current behavior just
  to bump a number. Each test should fail for a real bug.
- **No backwards-compatibility hacks**: this extension has no released users
  yet, so deletions can be clean. Drop dead code, don't comment it out.

## The Per-File Checklist

Open a session, pick a file, then walk this list. Skip steps that don't apply
— but explicitly note "N/A" rather than silently dropping them, so the log is
honest.

### 1. Read the whole file in one pass

Before touching anything, read the file end-to-end and note in a scratchpad:

- What is this file's job in one sentence?
- Which public functions are entry points (called from other modules / by
  pgrx)?
- Which functions look suspicious — too long, deeply nested, copy-pasted, or
  obviously stale?
- Which `unsafe` blocks are doing FFI vs. pointer arithmetic vs. type punning?

This step is non-optional. Without it, the rest of the checklist degenerates
into local nitpicks.

### 2. Simplify

- Delete dead code: unused functions, unused fields, unreachable arms.
  Confirm with `cargo check` and a project-wide grep, not just by looking.
- Collapse one-call-site helpers back into their caller if the helper isn't
  carrying a useful name.
- Replace ad-hoc loops with iterator chains *only* when it shortens and
  clarifies — not as a stylistic preference.
- Look for premature abstractions: traits with one impl, enums with one
  variant ever constructed, generic params never instantiated with more
  than one type. Inline them.
- Group related functions; move private helpers next to their caller.
- Remove `// removed` / `// TODO` / `// XXX` comments that are stale. If a
  TODO is still real, leave it; if it's been done, delete it.

### 3. Shrink `unsafe`

For each `unsafe` block:

- Can the operation be expressed safely? (`slice::from_raw_parts` →
  `&[T]` passed in by caller, `Datum::value()` instead of pointer casts,
  pgrx safe wrappers instead of raw `pg_sys::*`.)
- If it must stay unsafe, can the block shrink to the single unsafe op?
  Move safe code out.
- Does it have a `// SAFETY:` comment naming the invariant that makes it
  sound? If not, add one. Format:
  ```rust
  // SAFETY: <what must be true>, guaranteed by <who/why>
  unsafe { ... }
  ```
- Are FFI calls inside `pg_guard_ffi_boundary` where they should be? PG can
  longjmp out of FFI on error; the guard converts that to a Rust panic.

### 4. Improve test coverage

- Identify the non-obvious branches: error paths, empty inputs, off-by-one
  edges, codec edge cases, NULL handling, parallel/leader splits.
- Add `#[test]` for pure logic. Use `#[pg_test]` only when PG state is
  required.
- Where the file deals with SQL-visible behavior, see if an integration
  test in `tests/` is a better home than an inline unit test.
- Use `make coverage` to confirm the new tests actually exercise the lines
  you intended — but do not chase coverage numbers as a goal.

### 5. Verify

Before committing, run, in this order:

```bash
make fmt FILE=src/path/to/file.rs   # format only the file you touched
make clippy
make build
make test                  # unit tests
make integration-test      # if changes touch SQL-visible behavior
```

All must be clean. Per `CLAUDE.md`: pre-existing failures are still your
problem — fix them or stop and ask.

If the file affects query results (anything under `scan/`, `compress.rs`,
the compression codecs), also run the correctness harness — see
`dev/docs/CORRECTNESS_TESTING.md`. Add a new correctness case if this
file's logic isn't already covered.

### 6. End-of-Session Verification

Every session ends with the same gauntlet, regardless of which file was
touched. Run these in order; record the result in the log entry.

1. **Unit tests on both PG versions:**
   ```bash
   make test                  # PG17
   make test PG_MAJOR=18
   ```
2. **Integration tests on both PG versions:**
   ```bash
   make integration-test      # runs PG17 + PG18 by default
   ```
3. **Full correctness suite** (vanilla PostgreSQL as source of truth):
   ```bash
   make correctness
   ```
4. **ClickBench on EC2** (full 100M-row dataset; data is already loaded
   on the instance whose IP lives in `clickbench/.env`):
   ```bash
   make -C clickbench deploy EC2=$(cat clickbench/.env | cut -d= -f2)
   make -C clickbench bench  EC2=$(cat clickbench/.env | cut -d= -f2)
   ```
5. **JSONBench on EC2** (1B-row Bluesky dataset; data loaded on the
   instance whose IP lives in `jsonbench/.env`):
   ```bash
   make -C jsonbench deploy EC2=$(cat jsonbench/.env | cut -d= -f2)
   make -C jsonbench bench  EC2=$(cat jsonbench/.env | cut -d= -f2)
   ```

Each of these must come back clean before the session is done. Record
the headline numbers in the log entry: "unit/integration: N pass on
PG17+PG18", "correctness: X passed / Y skipped / Z xfailed", "clickbench
EC2: <delta> total, 0 regressions >10%", "jsonbench EC2: <delta> total".

Local-Docker benchmarks (`make bench-clickbench-keep`,
`make bench-rtabench-keep`) are useful for fast iteration during the
session but **do not substitute** for the EC2 runs at end-of-session.
Use them while changing code; the EC2 runs are the gate to merge.

RTABench EC2 is not part of the standard end-of-session gauntlet (slow
to provision, partial query overlap with ClickBench/JSONBench). Run it
on demand when a change plausibly affects the join-heavy or pre-aggregated
paths it exercises.

### 7. Log the session

Add a row to `CLEANUP_LOG.md` (newest first). One line per file is fine; if
something interesting came up, add a short note under it. The benchmark
and correctness numbers from step 6 are mandatory fields.

## Triage

Priorities are based on `(LOC, unsafe count, test count, criticality)` as of
2026-05-15. Re-check the numbers before starting a file — they will drift.

### Now (highest leverage)

These are large, untested or under-tested, and on hot paths.

| File | LOC | `unsafe` | Tests | Why now |
|---|---:|---:|---:|---|
| `src/scan/json_extract.rs` | 2776 | 106 | 1 | Heavy `unsafe`, near-zero tests, plenty of branch logic that should be reachable with pure unit tests. |
| `src/copy.rs` | 3456 | 67 | 0 | Direct-backfill path with FFI and ProcessUtility hook. No tests at all. High blast radius. |
| `src/scan/exec/decompress.rs` | 4060 | 54 | 0 | Decompression machinery. Pure logic that should be unit-testable; currently relies entirely on integration coverage. |
| `src/scan/exec/datum_utils.rs` | 1689 | 52 | 0 | Pointer-heavy helpers. Good candidate for shrinking `unsafe` and adding focused tests. |
| `src/scan/hook.rs` | 4690 | 84 | 0 | The planner hook — by far the largest file. Lower simplification ceiling (mostly PG FFI), but read-pass alone will likely surface dead caches/branches. |

### Next

Substantial but either smaller, less unsafe, or partially tested.

| File | LOC | `unsafe` | Tests | Why next |
|---|---:|---:|---:|---|
| `src/scan/path.rs` | 2583 | 44 | 0 | Planner path construction. Untested but largely lifts pgrx wrappers. |
| `src/scan/exec/segments.rs` | 3150 | 23 | 0 | Segment iteration core. Some `unsafe`, no tests. |
| `src/compress.rs` | 3773 | 7 | 13 | Already tested for codec paths; needs a simplify pass and probably some splitting. Low `unsafe`. |
| `src/blob_cache/storage.rs` | 1013 | 37 | 0 | Mmap-backed cache. Recent (just merged); add tests while the design is fresh. |
| `src/scan/exec/agg.rs` | 13691 | 162 | 140 | Largest file. Lots of `unsafe`, but also lots of tests. Strongest candidate for *splitting* into submodules and shrinking each. **Multi-session sub-project — playbook in [`AGG_SPLIT.md`](./AGG_SPLIT.md).** |
| `src/scan/exec/batch_qual.rs` | 884 | 4 | 0 | Low `unsafe`; mostly needs tests. |
| `src/scan/exec/count_minmax.rs` | 798 | 20 | 0 | Same shape: small enough to clean in one session. |
| `src/scan/exec/append_wire.rs` | 649 | 17 | 0 | Wire format. Pure logic, should unit-test. |
| `src/scan/exec/agg_wire.rs` | 680 | 13 | 0 | Same. |
| `src/scan/exec/text_col.rs` | 516 | 2 | 0 | Small; quick win. |

### Later

Lower urgency: smaller, mostly-safe, or already in decent shape.

| File | LOC | `unsafe` | Tests | Notes |
|---|---:|---:|---:|---|
| `src/partition.rs` | 604 | 0 | 0 | No `unsafe`. Needs tests for partition-boundary logic. |
| `src/catalog.rs` | 577 | 0 | 0 | Catalog read/write helpers. Tests should be integration-level. |
| `src/worker.rs` | — | 0 | 0 | Background worker. Hard to unit test; rely on integration. |
| `src/scan/cost.rs` | 493 | 10 | 3 | Already has a few tests; small file. Drive-by cleanup. |
| `src/scan/explain.rs` | 477 | 12 | 0 | Output formatting; integration-tested implicitly. |
| `src/timeparse.rs` | 464 | 0 | 21 | Already well-tested. Drive-by simplify only. |
| `src/compression/*.rs` | — | — | many | All have inline tests already. Drive-by simplify only. |
| `src/bloom.rs`, `src/stats.rs` | — | low | 0 | Small files; opportunistic cleanup. |
| `src/blob_cache/mod.rs` | 312 | low | 7 | Mostly OK. |
| `src/copyparse.rs` | 1349 | 4 | 43 | Heavily tested already; simplify only. |
| `src/copyparquet.rs` | 586 | 1 | 19 | Same. |
| `src/lib.rs` | 307 | 4 | 1 | GUC + module wiring. Light touch. |
| `src/scan/mod.rs` | — | 8 | 0 | Module wiring. |
| `src/functions/*` | small | low | a few | Drive-by. |

## Working Style

- **One file per session is the default.** It is fine for a session to do
  less than the full checklist if the file is huge (`agg.rs`, `hook.rs`),
  in which case the log should make the scope explicit (e.g. "agg.rs:
  read-pass + simplify; `unsafe` audit deferred").
- **One PR per session** unless changes are tightly coupled. Each PR title
  should name the file: `cleanup: simplify scan/json_extract.rs`,
  `tests: cover datum_utils.rs`, etc.
- **Run benchmarks** on any session touching the scan/exec path before
  merging. Note the result in the log.
- **If a session uncovers a real bug**, that's a separate PR. Don't bury
  bug fixes inside cleanup.
- **If a session spots a performance opportunity** that's too involved to
  do inline, surface it to the user at end-of-session with a short
  description, location, and expected gain. Don't bundle it into the
  cleanup PR.

## When to Update This Plan

- A file gets so much attention it leaves the triage — move it to "Done"
  (or simply remove the row; the log has the history).
- The methodology changes — e.g. we decide to also do an architectural
  review pass per file. Update the checklist, not the log.
- A new file is added to `src/` — add it to the appropriate bucket.

Otherwise, leave this doc alone and write in the log.
