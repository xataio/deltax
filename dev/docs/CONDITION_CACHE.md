# Condition cache — shared-memory cache of per-segment qual-selection bitmaps

Status: implemented + EC2-validated (2026-08-11/12). Same-box ClickBench
A/B on c6a.4xlarge, 100M rows, full queries-only protocol (true cold
cycle per query, hot = min of tries 2-3), toggled via
`ALTER SYSTEM SET pg_deltax.condition_cache`:

- Q21 −53% (0.745 → 0.347 s), Q22 −26%, Q40 −19%, Q20 −15%, Q19 −13%;
  Q37/Q38/Q41/Q42 −3..−7%.
- Every non-engaging query within ±4% (no-quals queries identical);
  cold sums identical (278.8 vs 278.6 s) — no measurable overhead.
- Hot geomean −3.7%, hot sum 24.9 → 24.0 s.
- Engagement proof: Q21 populates 1,695 segment entries (the other
  ~1,600 segments are dict-pruned upstream, by design); both warm tries
  hit all of them.

Archives: cache-on `results/history/20260812_015836_*`, cache-off
baseline `20260812_021731_*` (see NOTE file there). Local Docker A/B
passed the Phase 5 result-equality gate in both states.

Wired
sites: `load_next_segment` (DeltaXDecompress/Append row path, leader +
PG parallel workers; full hit exploitation — filter columns defer to
selection-aware Phase 2), `dispatch_serial_path` (serial agg; NonePass
skip + eval skip), `process_segments_mixed` (scope-thread agg;
pre-lookup/post-insert via `CondBatch`, filter-only column decode skip),
`process_segments_compact_filtered` (numeric agg; all three execution
contexts). Not wired: Top-N paths (`cutoff_row` complexity),
`agg/metadata.rs`, DeltaXCount/MinMax (metadata-only anyway).
GUC `pg_deltax.condition_cache` (USERSET, default on). Global counters
on `pg_deltax_blob_cache_stats()`; per-query EXPLAIN line on the
decompress path. Tests: `tests/test_condition_cache.py`.

Inspired by ClickHouse's QueryConditionCache: repeated queries with the
same WHERE clause re-evaluate the same predicates over the same immutable
segments. Caching the per-segment survivor bitmap lets a warm re-run skip both
the predicate evaluation and — more importantly — the decompression of columns
that are only needed for filtering (the dominant cost for text LIKE quals, e.g.
ClickBench Q20–Q23 where the URL dictionary must be fully decoded to evaluate
`LIKE '%google%'`).

The benchmark protocols run every query 3× from fresh sessions; a backend-local
cache would contribute nothing. This cache lives in the blob cache's DSA, so it
is shared across backends (and across the leader + PG parallel workers of one
query).

## What is cached

Key: `(companion_oid, segment_id, qual_fingerprint)` → value: one of

- `AllPass` — every row in the segment passes the fingerprinted filter set;
- `NonePass` — no row passes;
- `Bitmap(row_count, packed u64 words)` — per-row pass/fail.

`AllPass` maps to the empty selection vector (the in-tree "all rows pass"
convention, including the `classify_segment_quals` → `SegmentQualResult::AllPass`
empty-vec case). `NonePass` lets the consumer skip the segment without decoding
anything. Stored `row_count` must equal the segment's `row_count` at use time or
the entry is treated as a miss (belt-and-braces against id reuse).

## Fingerprinting: hash what the site applies, not the query

Each integration site computes the fingerprint from **exactly the filter terms
it applies to the selection vector, in application order**: the `BatchQual` list
(col_idx, op, type_oid, by-value const or text/list constants, LIKE strategy)
plus the `TextQualInfo` list (EqNe / Like / InList with their columns and
constants), plus a format-version constant. A site that applies any
selection-writer we cannot canonically encode marks the fingerprint poisoned and
bypasses the cache (fail closed).

Because the fingerprint describes the *applied set*, two different paths share
entries exactly when they apply the same filters, and paths that apply different
subsets (e.g. `agg/metadata.rs` ignores text quals) get disjoint entries — both
outcomes are correct by construction. Sites also bypass the cache when:

- the segment has tombstones (`seg.tombstones.is_some()`) — DELETE sidecars
  change the visible row set without changing `segment_id`. (The DeltaXAgg
  family is structurally tombstone-free; the decompress paths check per
  segment.)
- a partial-segment evaluation is in effect (`cutoff_row` in the time-ordered
  Top-N path) — only full-segment selections are cached.
- segment_by-derived whole-segment rejects fired (those are cheap metadata
  checks; caching would just duplicate them). segment_by terms *are* included
  in the fingerprint when a site folds them into the row-level selection.

## Staleness

Same argument as the blob cache: `(companion_oid, segment_id)` never aliases
different content — recompression drops + recreates the companion (new OID) and
compressed-DML compaction allocates segment ids above a catalog high-water mark
precisely so `(companion_oid, segment_id, …)`-keyed caches stay valid
(COMPRESSED_DML.md). Tombstone changes are handled by the bypass above, not by
invalidation. Entries from aborted compress transactions share the blob cache's
(accepted, unlikely) id-reuse exposure.

## Thread model

LWLocks may only be taken from real backend threads (leader or PG parallel
worker process) — never from `std::thread::scope` closures. Integration
therefore takes two shapes:

- **Direct** (site runs on a backend thread): `load_next_segment`
  (decompress.rs; leader and PG-parallel-worker), `dispatch_serial_path`
  (agg/serial.rs; leader).
- **Pre-lookup / post-insert** (site runs in scope closures):
  `process_segments_mixed` and `process_segments_compact_filtered`. The
  backend thread that owns the batch looks up all segments' entries before
  spawning the scope (attaching `Option<CachedSelection>` per segment; pinned
  DSA bytes are safe for scope threads to read, same as pinned blob bytes
  today), and inserts the selections computed by workers after the scope
  joins. Bitmaps are packed to u64 words (30K rows → 3.75 KB) so the
  per-batch transfer cost is trivial.

## Storage

Rides the blob cache: the `BlobCacheKey` is widened with a `kind` discriminant
(`Blob` / `Condition`) and a `qual_fp: u64` (zero for blob entries), sharing the
DSA, shard LWLocks, LRU, and eviction. Condition entries are tiny (≤ ~4 KB)
next to blob entries (30–300 KB), so they effectively never pressure the LRU.

GUC: `pg_deltax.condition_cache` (bool, USERSET, default on). Requires the blob
cache to be available (full mode, `blob_cache_mb != 0`); in session mode or
with the blob cache disabled it is inert. USERSET context makes A/B runs a
per-session toggle.

Observability: global counters on `pg_deltax_blob_cache_stats()`
(`cond_hits_total`, `cond_misses_total`, `cond_inserts_total`) and a per-query
`DeltaX Condition Cache: hits=… misses=…` EXPLAIN ANALYZE line.

## Drift hazard (the one real risk)

If a future change adds a new selection-vector writer to an integrated site
without extending the fingerprint builder, cached bitmaps for the old
fingerprint would be reused despite the new filter — wrong results. Mitigations:
the fingerprint builder is threaded through the same code that applies filters
(adding a filter without seeing the builder is hard); `CACHE_FP_VERSION` is
bumped on any semantic change; and the local benchmark harnesses assert
result-set equality on every run, which is exactly the regression net that
catches this class.
