# N-gram Substring Skip Index (design)

Status: **proposal** — not implemented. This doc captures the motivation,
the cross-benchmark applicability analysis, and a concrete build/query design
so we can decide whether to invest.

## Problem

Substring predicates of the form `col LIKE '%needle%'` on **high-cardinality
text columns** force a full decompression of the column: the match can occur
anywhere in any value, so every byte must be produced before the filter can
run. For `Lz4Blocked` columns there is no value-level structure to prune on,
and the existing dictionary pruning (`segment_skippable_by_dict` /
`dictionary::any_entry_matches`) does not apply — that path only works for
`Dictionary`/`DictionaryLz4` columns, where the distinct entry set is small
enough to test the predicate per entry.

ClickBench Q21
(`SELECT SearchPhrase, MIN(URL), COUNT(*) WHERE URL LIKE '%google%' AND
SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10`) is the
canonical case. Measured on the EC2 full dataset (100M rows, c6a.4xlarge,
8 physical cores):

- Hot ≈ 0.77s, of which ~38% of CPU is `compression::lz4::decode_to_ranges_blocked`
  + `lz4_flex` decompressing the **entire** URL column.
- The predicate is extremely selective: 15,911 / 100M rows (0.016%) match
  `URL LIKE '%google%'`; 1,038 pass both filters.
- Scaling 4→8 workers = 1.7×, 8→16 = 1.07× — the decode saturates the
  physical cores. Reducing CPU elsewhere does not move the wall (see the
  `should_sweep_lz4_contains` change, which cut the LIKE filter CPU from ~5%
  to <1% with no wall effect). **The only lever is decompressing fewer bytes.**

## Idea

Build a small **trigram bloom filter per block** at compression time. At query
time, decompose the `Contains` needle into its trigrams and skip any block
whose bloom is missing at least one of them — that block provably contains no
occurrence of the needle, so it never has to be decompressed.

This is the same structure ClickHouse exposes as `ngrambf_v1`. It reuses the
existing `bloom::BloomFilter` (10 bits/element, multi-seed hashing) almost
verbatim; only the *granularity* (per block, not per segment) and the *keys*
(trigrams, not whole values) differ.

### Why it pays off: clustering, not just selectivity

A 0.016%-selective predicate over 10,000-row blocks would, under a uniform
distribution, leave ~1.6 matches per block (Poisson → only ~20% of blocks
empty/skippable). That alone would barely help.

What makes it work is that DeltaX stores `hits` ordered by
`(counterid, userid, eventtime)` — i.e. clustered by website. `google` URLs
concentrate in particular sites, so matches cluster heavily in physical order.
Simulating block assignment against the real sort order:

| Granularity | Blocks with ≥1 `URL LIKE '%google%'` | Skippable |
|---|---|---|
| Segment (30,000 rows) | 1,622 / 3,334 | **51%** |
| Block (10,000 rows, current default) | 2,867 / 10,000 | **71%** |
| Block (1,000 rows) | 6,348 / 99,998 | 94% |

At the current `lz4::DEFAULT_BLOCK_SIZE` (10,000) this is a ~3.4× reduction in
URL bytes decompressed for the URL `%google%` queries. Cold wins are larger
still: skipped blocks also skip their I/O.

## Cross-benchmark applicability

The skip index only helps **positive `Contains`** predicates on
**high-cardinality (`Lz4`/`Lz4Blocked`)** text columns. Surveyed all three
benchmarks:

### ClickBench — 4 queries

| Q | Predicate | Column | Block-skip @10K | Hot today |
|---|---|---|---|---|
| Q20 | `URL LIKE '%google%'` | URL (Lz4, ~unique) | 71% | 736ms |
| Q21 | `URL LIKE '%google%'` | URL | 71% | 770ms |
| Q23 | `URL LIKE '%google%'` (SELECT \* top-N) | URL | 71% | 414ms |
| Q22 | `Title LIKE '%Google%'` | Title (Lz4) | 42% | 1222ms |

URL and Title are effectively unique (Lz4Blocked), so neither is covered by
dictionary pruning today. Q22's `URL NOT LIKE '%.google.%'` is negated and
passes ~all rows, so only its positive `Title` filter benefits. Expected hot
wins ≈ 1.5–3× per query; cold wins (Q23 cold ≈ 43s, Q21 ≈ 24s, Q20 ≈ 18s) are
substantially larger because block I/O is skipped too.

### JSONBench — 0 queries

All five queries filter via **exact-match / IN** on JSON-extracted fields
(`data->>'kind' = 'commit'`, `operation = 'create'`,
`collection IN (...)`). These want **value blooms** (the existing
equality-bloom infrastructure on JSON-extracted virtual columns), not
substring n-grams.

### RTABench — ~0 queries

The only `LIKE` is Q05 `processor LIKE '%ron%'`. `processor` is
low-cardinality (payment-processor names) → either `Dictionary`-encoded (and
already covered by `segment_skippable_by_dict`) or the matching values appear
in every segment, which is not selective at block level either way. n-grams
add nothing.

**Conclusion:** this is a ClickBench-specific optimization (substring search is
a ClickBench specialty), not a cross-benchmark lever. Scope and justify it as
such.

## Design

### Compression time

When a column is `Lz4Blocked` and flagged for n-gram indexing (see GUC below),
extend the block encoder (`compression::lz4::encode_blocked`) — or a sidecar
pass alongside it — to also emit, per block:

1. The set of distinct **trigrams** (3-byte windows) over the concatenation of
   that block's values. Use raw bytes, not chars: a valid-UTF-8 needle's byte
   trigrams match the value bytes exactly, so no decoding is needed and
   multibyte characters fall out naturally.
2. A `bloom::BloomFilter` sized for that distinct-trigram count
   (`bloom::bloom_size_for_ndistinct`, already clamped to
   `[MIN_BLOOM_BYTES, MAX_BLOOM_BYTES]`). Each trigram is hashed (a small
   wrapper over the existing multi-seed `BloomFilter::insert`) and inserted.

Short values (< 3 bytes, including the empty string) contribute no trigrams; a
needle shorter than 3 bytes must bypass the index and scan (rare for LIKE; the
planner can leave such predicates on the scan path).

Store the per-block blooms in a new sidecar, parallel to the existing per-
segment **Blooms** companion table (`<partition>_blooms`, today used for
equality pushdown — see `COLUMNAR_STORAGE.md`). Options:

- A new `<partition>_ngram` companion table, keyed by `(segment_id,
  column_id)`, holding the concatenated per-block blooms (block count is
  derivable from row count and block size).
- Or fold into the existing blooms table with a discriminator. The separate
  table keeps the equality-bloom hot path untouched and is easier to make
  opt-in; prefer it.

### Query time

1. **Decompose the predicate.** When a batch qual is `LikeStrategy::Contains(s)`
   with `s.len() >= 3` and `negate == false` (see `batch_qual::compile_like_pattern`),
   compute the needle's trigram set once. The whole needle is present in a
   block only if **all** its trigrams are present, so a block is skippable when
   the bloom is missing **any** needle trigram.

2. **Block selection.** `lz4::decode_to_ranges_blocked` already takes a
   `selection: Option<&[bool]>` and decompresses only blocks that contain a
   selected row. Feed it a block mask derived from the n-gram blooms (AND-ed
   with any existing row selection). Non-selected blocks are never
   decompressed; their rows are treated as non-matching for the predicate.

3. **Caller wiring.** The block mask must be produced before decode in:
   - the agg mixed path (`agg::parallel_mixed::process_segments_mixed`, which
     calls `decompress_text_to_seg_col` → `decode_to_ranges_blocked`) for
     Q20/Q21/Q22;
   - the top-N decompress path (`scan::exec::decompress::exec_topn_text` /
     `exec_topn_two_pass` and the SELECT-\* scan) for Q23.

   The cleanest seam is to thread the trigram set + per-block blooms into the
   `SegTextColumn` construction so the existing `selection`-based block skip in
   `decode_to_ranges_blocked` does the work, and to mark fully-skipped blocks'
   rows as non-matching when building the selection vector.

4. **No correctness risk from false positives.** A bloom false positive only
   means an extra block is decompressed and scanned (the real
   `apply_lz4_contains_filter` / `apply_text_like_filter` sweep still runs and
   is authoritative). False *negatives* are impossible: every needle trigram
   that actually occurs was inserted, so a present substring can never be
   pruned.

### Sizing and storage cost

This is the main cost. A 10,000-row block of ~100-byte URLs is ~1 MB of text
with on the order of 30–60K distinct trigrams. At 10 bits/element that is
~40–75 KB per block, but `bloom::MAX_BLOOM_BYTES` (8 KB) caps it — at the cap
the false-positive rate rises, eroding the skip rate. Tradeoffs to evaluate:

- **Block size.** Smaller blocks skip more (94% @1K vs 71% @10K) and have
  fewer trigrams per bloom (so the 8 KB cap bites less), but cost compression
  ratio and add per-block overhead. A column-local block size for indexed
  columns may be worth it.
- **Bloom cap / bits-per-element.** Raising `MAX_BLOOM_BYTES` for n-gram
  blooms trades storage for skip rate. Needs measurement.
- **Per column.** Only build for columns explicitly opted in. URL/Title-class
  columns are large already; a ~10–20% sidecar overhead on them buys the
  speedups above.

Make it **opt-in via a GUC**, mirroring `pg_deltax.bloom_filters` (the existing
equality-bloom toggle in `lib.rs`), e.g. `pg_deltax.ngram_index` plus a way to
designate columns (table option or per-column list). Default off.

### EXPLAIN / observability

Extend the scan stats already surfaced in EXPLAIN (the `DeltaX Buffers`
bloom hit/read counters in `ScanStats`) with blocks-pruned / blocks-scanned by
the n-gram index, so skip effectiveness is visible per query.

## Limitations / non-goals

- **Positive `Contains` only.** `NOT LIKE`, anchored `LIKE 'x%'` /`'%x'`
  (handled by `StartsWith`/`EndsWith` + min/max where applicable), and needles
  < 3 bytes do not use the index.
- **Dictionary columns** keep using `segment_skippable_by_dict`; do not build
  n-gram blooms for them.
- **Not a JSONBench/RTABench win.** Do not justify the storage cost with those
  benchmarks.
- Case-insensitive `ILIKE` would need case-folded trigrams at build and query
  time (and a separate index or folding both ways); out of scope for v1.

## Suggested phasing

1. **Prototype, URL-only, behind `pg_deltax.ngram_index`.** Per-block trigram
   bloom for `Lz4Blocked` columns; wire into `decode_to_ranges_blocked`'s
   `selection` for the agg mixed path. Validate on Q20/Q21 hot + cold, confirm
   skip-rate counters match the ~71% simulation.
2. **Top-N / SELECT-\* path** (Q23) and the Title column (Q22).
3. **Tune** block size and bloom cap against the storage/skip-rate curve;
   decide defaults and the column opt-in mechanism.
4. **Full ClickBench** cold+hot validation (per `bench-protocol-not-warm-explain`),
   measure aggregate delta and storage overhead before enabling by default.

## Appendix: measurement commands

Block-skip simulation (run against the EC2 full dataset; orders 100M rows, so
needs `work_mem` headroom):

```sql
SET work_mem='2GB'; SET max_parallel_workers_per_gather=8;
WITH ordered AS (
  SELECT (URL LIKE '%google%') AS m,
         row_number() OVER (ORDER BY counterid, userid, eventtime) - 1 AS rn
  FROM hits)
SELECT count(DISTINCT rn/30000)                          AS segs,
       count(DISTINCT rn/30000) FILTER (WHERE m)         AS segs_hit,
       count(DISTINCT rn/10000)                          AS blocks,
       count(DISTINCT rn/10000) FILTER (WHERE m)         AS blocks_hit
FROM ordered;
```
