# Next Optimizations — local 10M ClickHouse gap analysis

Ranked list of **new** optimization candidates derived from the same-machine
10M-row ClickBench comparison (2026-06-11): pg_deltax vs ClickHouse 24.8, both
in Docker on an M4 Pro (14 cores visible to the VM). Sources:

- pg_deltax per-query best-of-3:
  `tests/.bench_results/history/20260611_173727_10e133c/pg_deltax.json`
  (`compressed_queries`, ms, commit `10e133c`)
- ClickHouse per-query best-of-3: `/tmp/ch_times.txt` (seconds)
- 100M phase breakdowns: `QUERY_ANALYSIS.md` (c6a.4xlarge, warm)

Totals: pg_deltax **5.74 s** vs ClickHouse **2.59 s**. We are faster on only
8/43 queries locally (Q0, Q2, Q3, Q5, Q6, Q24, Q26, Q27) vs 6/43 at 100M.
The local regime exposes different bottlenecks than EC2: detoast is cheap
(small TOAST working set, fast NVMe page cache), so pure decode/agg CPU and
per-query fixed costs dominate the gap.

Everything in `PERF_IMPROVEMENTS.md` #1–#49 is treated as tried; overlaps are
flagged per candidate. **Already in flight from the same perf session**
(extracted into separate PRs; not re-proposed here, listed for context):

- **Storage v2 P1 + P1b**: dual-write segment files + mmap read — attacks the
  F3 detoast wall (~12–15 s at 100M). EC2 validation pending.
- **Decompressed-column cache P1**: shared-memory cache of decoded numeric
  vectors, a warm-run lever for the ~11 s cumulative decode CPU. Prototyped
  and **reverted** — the shmem machinery and invalidation complexity were not
  justified by what it measured; the decode-CPU pool remains open (see N8).
- **#36 merge-side two-level rework**: Q32 local 587→471 ms.
- **#47 partition bloom sentinels**: Q19, EC2 numbers pending.

## 1. Per-query ratio table (local 10M)

Sorted by absolute gap (deltax − CH). Dominant cost from `QUERY_ANALYSIS.md`
breakdowns plus the execution path each query takes locally.

| Q | deltax ms | CH ms | ratio | gap ms | path / dominant cost |
|----|----------:|------:|------:|-------:|----------------------|
| Q28 | 1765.3 | 722 | 2.45 | +1043 | mixed agg: Referer LZ4 decode + per-row regex + MinStr |
| Q18 | 487.5 | 161 | 3.03 | +327 | mixed agg: 3-key (int,int,text) hash+probe, ~3.4M groups |
| Q32 | 471.2 | 192 | 2.45 | +279 | compact agg: ~10M-group merge (post-#36 rework) |
| Q23 | 380.2 | 182 | 2.09 | +198 | DeltaXAppend TopN: Phase 2 decode of ~100 cols |
| Q13 | 239.4 | 55 | 4.35 | +184 | mixed agg + CD sidecar: SearchPhrase × distinct UserID |
| Q22 | 281.9 | 110 | 2.56 | +172 | dict LIKE + LZ4 NOT-LIKE decode, 5-agg row loop |
| Q25 | 151.1 | 12 | 12.59 | +139 | text TopN: full per-row decode of SearchPhrase |
| Q12 | 113.3 | 32 | 3.54 | +81 | mixed agg (dict fast path): per-row global acc store |
| Q35 | 92.0 | 19 | 4.84 | +73 | compact agg: ~2M-group merge (#34 key-elim active) |
| Q31 | 106.3 | 37 | 2.87 | +69 | compact agg: 2-key, sidecar filter, ~3M groups |
| Q14 | 108.8 | 41 | 2.65 | +68 | mixed agg: (int, text) generic key path |
| Q33 | 264.1 | 201 | 1.31 | +63 | mixed agg: URL dict, per-row global acc store |
| Q15 | 79.6 | 20 | 3.98 | +60 | compact agg: ~2.5M-group merge |
| Q39 | 94.6 | 41 | 2.31 | +54 | CASE WHEN transform (per-row String alloc) + agg |
| Q9 | 78.5 | 25 | 3.14 | +53 | CD merge + multi-agg |
| Q16 | 135.6 | 85 | 1.59 | +51 | mixed agg: (UserID, SearchPhrase) probe-bound |
| Q8 | 71.0 | 23 | 3.09 | +48 | CD per-group sidecar |
| Q34 | 225.6 | 183 | 1.23 | +43 | as Q33 |
| Q21 | 92.4 | 55 | 1.68 | +37 | LZ4 LIKE sweep + agg |
| Q30 | 77.1 | 40 | 1.93 | +37 | compact agg 2-key + length-sidecar filter |
| Q20 | 81.6 | 49 | 1.67 | +33 | LZ4 LIKE memmem sweep (#49 active) |
| Q40 | 29.7 | 7 | 4.24 | +23 | filtered decompress + 41K-group agg |
| Q1 | 24.8 | 4 | 6.20 | +21 | metadata fast path — gap is colstats load + planning |
| Q36 | 31.0 | 15 | 2.07 | +16 | filtered dict agg |
| Q4 | 33.6 | 23 | 1.46 | +11 | CD parallel (post-#43 a+b+c) |
| Q37 | 21.3 | 11 | 1.93 | +10 | filtered dict agg |
| Q7 | 12.7 | 5 | 2.55 | +8 | small agg, fixed costs |
| Q38 | 15.4 | 9 | 1.71 | +6 | TopN OFFSET, fixed costs |
| Q11 | 14.8 | 10 | 1.48 | +5 | CD + detoast |
| Q17 | 51.4 | 49 | 1.05 | +2 | F8 preselect active — at parity |
| Q19 | 5.4 | 3 | 1.79 | +2 | bloom point lookup (#47 pending) |
| Q10 | 11.4 | 9 | 1.26 | +2 | CD + detoast |
| Q41 | 8.2 | 6 | 1.36 | +2 | PG Sort fallback |
| Q29 | 8.7 | 7 | 1.24 | +2 | metadata-resolved |
| Q42 | 5.8 | 5 | 1.16 | +1 | framework/emit |
| Q0 | 0.3 | 1 | 0.30 | −1 | **win** |
| Q6 | 1.1 | 4 | 0.28 | −3 | **win** |
| Q2 | 1.0 | 5 | 0.19 | −4 | **win** |
| Q3 | 0.8 | 5 | 0.16 | −4 | **win** |
| Q5 | 32.0 | 41 | 0.78 | −9 | **win** (dict-only CD) |
| Q27 | 32.2 | 44 | 0.73 | −12 | **win** (length sidecar) |
| Q24 | 3.1 | 16 | 0.19 | −13 | **win** |
| Q26 | 2.9 | 25 | 0.12 | −22 | **win** |

### Where the gap concentrates

| Cost class | Queries | Local gap (ms) | Share |
|---|---|---:|---:|
| Regex + text-agg row loop (Q28) | Q28 | 1043 | 33% |
| Mixed-path agg row loop (text GROUP BY) | Q12–Q14, Q16–Q18, Q33, Q34 | 818 | 26% |
| High-cardinality compact merge | Q32, Q15, Q35, Q31 | 481 | 15% |
| TopN decode (SELECT* / ORDER BY text) | Q23, Q25 | 337 | 11% |
| LIKE decode | Q20–Q22 | 242 | 8% |
| CD sidecar | Q4, Q8, Q9, Q13(part) | ~150 | 5% |
| Fixed costs on sub-30ms queries | Q1, Q7, Q19, Q36–Q42… | ~90 | 3% |

Two structural notes that answer the standing questions:

- **Parallelism at 10M is not the problem.** One `DeltaXAgg` node covers all
  compressed partitions (`add_agg_path` takes `companion_oids: &[Oid]`,
  `src/scan/path.rs:1560`), and `get_parallel_workers()` resolves to 14 in the
  local VM. ~340 segments give each worker ~24 work units — fine granularity.
  The serial spots are elsewhere: TopN Phase 2 (N4) and the per-row agg loops.
- **VECTORIZE.md status:** Phases 1–3 landed long ago (PERF #3, #11, #10).
  Phase 4 (SIMD/Arrow kernels) was never tried. There is **no explicit SIMD**
  in the tree beyond `memchr/memmem`; decoded numerics are
  `Vec<(Datum, bool)>` (AoS, 16 B/value — `parallel_compact.rs:80`), which
  blocks meaningful auto-vectorization of filter/accumulate loops. See N8.

### Why Q28 flips between 10M (CH 2.4× faster) and 100M (we win 0.70×)

ClickHouse's `REGEXP_REPLACE` cost is per-row and scales linearly: 0.722 s at
10M → 9.58 s at 100M (~13×, on a ~2× slower core). Our cost is dominated by
*distinct* Referer values, not rows: the LZ4 branch of
`apply_regex_to_seg_col` dedups by **output**, the dict branch runs the regex
**once per dict entry**, and at 100M Referer segments are dict-encoded with
1.5–8 K entries (#45 measurements) — so our per-row cost collapses to an index
lookup as duplication grows. At 10M, per-segment cardinality is high relative
to rows, more segments fall on the LZ4 branch, and we pay the regex engine
per row. The fix is to make the 10M case behave like the 100M case: dedup by
**input** before invoking the regex (N1).

## 2. Candidates

Projections marked (est.) are sized from the QUERY_ANALYSIS phase tables and
local recorded times; none are measured. 100M baselines are the QUERY_ANALYSIS
column (pre-storage-v2).

### N1. Input-keyed regex memoization in the mixed-path transform

**Queries: Q28.** **Projected: local −300–450 ms (1.77 → ~1.4 s); 100M
−0.8–1.2 s (6.7 → ~5.7 s, agg=2073 ms portion).** **Complexity: Low (~40
LOC). Risk: low.**

`apply_regex_to_seg_col` (`src/scan/exec/agg/regex.rs:209`) LZ4 branch runs
`regex.replace()` on **every row**, deduping only by output when inserting
into the result dict. The serial path already memoizes by input
(`serial.rs:140` `regex_cache: HashMap<String, String>`), but the parallel
mixed path — which is what actually runs Q28 — does not.

Change: key a per-segment (or worker-local cross-segment, like serial's)
map by the input `&str` before calling the regex. Hit → reuse output entry
index (~20 ns hash) instead of a regex engine invocation (~250–500 ns).
Referer duplication at 10M is roughly 3–5× and grows with scale, so this
directly removes the regime where CH beats us. Bound the memo size (e.g.
256 K entries) to cap memory on adversarial inputs.

Optional second stage (separate decision): a specialized scanner for
anchored-prefix + capture-until-delimiter patterns
(`^https?://(?:www\.)?([^/]+)/.*$` class) compiled to `starts_with` +
`memchr` — ~10× cheaper than the regex engine on the residual distinct
inputs. Medium complexity, pattern-detection brittleness; only worth it if
N1's measured residual is still regex-bound.

### N2. Segment-local dense pre-aggregation for dict-encoded GROUP BY keys

**Queries: Q12, Q13, Q14, Q28, Q33, Q34 (+Q36/Q37 marginally).**
**Projected: local −250–400 ms cumulative; 100M −1.5–2.5 s cumulative
(agg phases: Q12 563, Q13 758, Q14 619, Q33 1223, Q34 ~1220, Q28 part of
2073 ms).** **Complexity: Medium (~200 LOC). Risk: medium (memory per
segment is bounded by dict size, but the merge step adds a code path).**

The dict fast path (`parallel_mixed.rs:938`) already collapses hash+probe to
a `dict_gidx_cache` lookup, but the **accumulator update is still a per-row
random store** into the worker-global `CompactAccStorage`
(`compact_storage.incr_count(group_idx, …)`, `parallel_mixed.rs:1130`) plus a
per-row loop over `agg_specs`. With millions of groups that storage is
DRAM-resident; 30 K rows per segment mean 30 K scattered read-modify-writes.

Change: for segments where the (single) group key is dict-encoded, accumulate
into a **dense per-segment array indexed by dict entry** (3–8 K entries —
L1/L2-resident; `counts[entry] += 1`, `sums[entry] += v`), then merge once
per distinct entry per segment into the global storage. Converts ~30 K
scattered global updates into ~30 K sequential local updates + ~3–8 K global
merges (≈4–10× fewer DRAM touches). `MIN(Referer)`/`MAX` string accumulators
(Q28, Q22) reduce to per-entry presence flags — the min over rows of a dict
column is the min over *present entries*, no per-row `str` compare at all.
CD sidecars (Q13) can keep per-row inserts initially; counts/sums alone
cover Q12/Q33/Q34/Q28.

Files: `src/scan/exec/agg/parallel_mixed.rs` (dict_fast block),
`src/scan/exec/agg/compact.rs` (bulk-merge helper on `CompactAccStorage`).

### N3. Dict-aware text TopN candidate generation (ORDER BY text LIMIT)

**Queries: Q25 (worst local ratio, 12.6×).** **Projected: local −100–120 ms
(151 → ~35 ms); 100M −1.0–1.3 s (1.91 → ~0.7 s).** **Complexity: Medium.
Risk: low (falls back to existing path for LZ4 segments).**
**Overlap: the idea appears in QUERY_ANALYSIS.md's Q25 note tied to the
rejected dict-sidecar (#45); this variant needs no sidecar and was never
implemented.**

`exec_topn_text` (`src/scan/exec/decompress.rs:1349`) collects candidates by
iterating **every row** of every surviving segment with byte-order pruning
(#37). For dict-encoded segments, the candidate set per segment is fully
determined by the **dict entries**: the K lexicographically-smallest entries
(byte-order) that pass the text quals. Phase 1 then needs zero row iteration
— only when a segment's entries make the global top-K do we decode
`row_to_entry` to expand multiplicities (LIMIT 10 may be 10 copies of the
smallest phrase). Per segment that's a scan of 3–8 K entries instead of 30 K
rows + per-row range lookups, and most segments never touch their index
arrays. LZ4 segments keep the current per-row path.

Files: `src/scan/exec/decompress.rs` (`exec_topn_text` Phase 1),
`src/scan/exec/text_col.rs` (entry-level qual evaluation already exists as
`dict_matches`).

### N4. Parallel Phase 2 column decode in the TopN decompress path

**Queries: Q23 (SELECT*), Q38/Q19 marginally.** **Projected: local −120–180 ms
(380 → ~220 ms); 100M −0.1–0.2 s (Q23 0.48 s).** **Complexity: Low-Medium.
Risk: low — no new decode variants, so it avoids #29's icache trap.**

Pass 2 of the TopN scan (`decompress.rs:1933` onward) detoasts lazy blobs for
winning segments (must stay on the leader) and then decodes **all ~100
columns serially on the main thread**. The decode work after detoast is pure
Rust (`CompressedColumn::from_bytes` + per-codec decode with
`narrowed_selection`) and embarrassingly parallel over (segment, column).
Wrap it in the same `std::thread::scope` pattern the agg paths use; 7 winning
segments × 100 columns gives plenty of granularity for 14 workers. Unlike
#29 (selection-based sparse decode, rejected for binary bloat), this adds a
dispatch loop only — existing decode functions are unchanged.

Files: `src/scan/exec/decompress.rs` (Pass 2 loop), reuse worker-dispatch
helpers from `src/scan/exec/agg/parallel_mixed.rs`.

### N5. Per-segment composite-key memoization for mixed (int+text) GROUP BY

**Queries: Q14, Q21, Q39; gated off for Q16–Q18.** **Projected: local
−30–50 ms; 100M −0.2–0.4 s.** **Complexity: Low-Medium. Risk: low.**

The dict fast path requires `n_int_keys == 0 && n_str_keys == 1`
(`parallel_mixed.rs:945`). Q14 (`SearchEngineID, SearchPhrase`) therefore
takes the generic path: per row, `hash_mixed_key` runs **two** full ahash
passes over the string bytes (`parallel_mixed.rs:59`) plus a probe of the
multi-million-entry global map. Per segment, the distinct (int, entry) pair
count is barely above the dict entry count for low-cardinality int columns —
memoize `(int_key, entry_idx) → gidx` in a small per-segment map (L2-resident)
and fall through to the global probe only on local misses. Gate on segment
colstats: skip when the int column's per-segment ndistinct × dict entries
approaches the row count (Q16–Q18's UserID makes local pairs ≈ unique, where
the memo is pure overhead).

Files: `src/scan/exec/agg/parallel_mixed.rs` (row loop), gate reads existing
per-segment ndistinct from colstats (`segments.rs`).

### N6. Arena/borrowed dedup in the CASE WHEN transform

**Queries: Q39.** **Projected: local −15–25 ms (94.6 ms); 100M −0.1–0.15 s
(0.86 s).** **Complexity: Low (~30 LOC). Risk: none.**

`apply_case_when_to_seg_col` (`src/scan/exec/agg/regex.rs:126`) allocates
per row: `s.to_owned()` for column-ref results and `string_val.clone()` for
the map key — even though the output is dict-shaped and low-cardinality.
Apply the same borrowed-lookup-then-own-on-miss pattern the LZ4 regex branch
already uses (`regex.rs:240`), and resolve `ColumnRef` on dict columns by
entry index instead of by string (the CASE result for a dict input is a
per-entry mapping, computable once per segment).

### N7. Backend-local metadata + colstats cache (revive #38, extend scope)

**Queries: every query; visible on the ~20 sub-30 ms ones (Q1, Q7, Q19,
Q29, Q36–Q42).** **Projected: local −40–60 ms cumulative (e.g. Q1 24.8 →
~8 ms); 100M −0.2–0.3 s.** **Complexity: Medium (branch exists). Risk: cache
invalidation on recompression (companion-OID keying solves it — same trick
as the blob cache).** **Overlap: PERF #38 (implemented in a branch, unmerged
because #35 was dropped; the standalone win was judged too small at 100M —
the local 10M calculus is different).**

Locally, CH answers Q1 in 4 ms; our metadata fast path resolves it entirely
from `nonzero_count` (`agg/metadata.rs:251`) yet still takes 24.8 ms — the
time goes to planning (~3 ms) plus per-query reloading of meta + colstats
rows for ~340 segments across 7 partitions through SPI/heap scans
(`load_segments_heap` Phase 0/1). These rows are immutable until
recompression. #38's `thread_local!` cache cut Q0's metadata phase 36 → 1.7
ms warm; extending it to colstats (sums, nonzero/nonnull, minmax) makes the
whole sub-30 ms query class metadata-free on warm repeats. This is the
single biggest lever for the "interactive dashboard" feel at small scale,
even though it barely moves the 100M bench total.

Files: `src/scan/exec/segments.rs` (`load_metadata`, colstats load),
existing branch as the starting point; key by companion OID (generation
token under storage v2).

### N8. SoA numeric decode + explicit SIMD filter/agg kernels

**Queries: broad (Q8–Q18, Q30–Q35, Q39–Q42 agg/filter loops).**
**Projected: 100M −1–2 s cumulative (est., wide error bars).**
**Complexity: High (touches every decode consumer). Risk: high — #29's
icache regression precedent; AoS→SoA refactor is invasive.**
**Overlap: VECTORIZE.md Phase 4 proposed this with arrow-rs; this is the
no-new-dependency variant.**

Decoded numerics are `Vec<(Datum, bool)>` — 16 B per value with nulls
interleaved (`parallel_compact.rs:83`). That layout defeats
auto-vectorization of batch quals and accumulate loops and doubles memory
bandwidth. The (since-reverted) decompressed-cache prototype already
demonstrated the target representation — flat `Vec<T>` + null-bitmap
sections at the decode boundary. Once consumers read that form natively,
explicit SIMD kernels (NEON/AVX2 via `std::simd` or hand-rolled chunks) for
`=`/`<>`/range filters and SUM/COUNT accumulation become straightforward.
Do this as a consumer-by-consumer AoS→SoA migration, measuring binary size
at each step. Note: the hash-probe-bound queries (Q32, Q18) gain nothing
from SIMD, so the win is capped to scan/filter-bound shapes.

### Overlap-flagged (prior art exists, listed for the ranking)

- **N9 = #36 follow-up: bucket `CompactAccStorage`/`CountDistinctSideCar` by
  sub-partition.** Q32/Q15/Q35 merge: −1.5–2.5 s at 100M, −80–150 ms local.
  Medium-high complexity (every accumulator accessor). The merge-side rework
  just landed; measure it on EC2 first — it changes this item's residual.
- **Storage v2 P2/P3 (text forms)**: the remaining storage-v2 phases own the
  detoast pool (~12–15 s) at 100M; the warm decode pool (~6–8 s) is open
  again after the decompressed-cache revert. Local 10M barely sees detoast,
  which is exactly why this doc's candidates skew toward CPU mechanics.

## 3. Ranking

Score = projected 100M seconds saved ÷ complexity (Low=1, Med=2, High=3).
Local column shows the 10M gap actually addressed.

| Rank | Candidate | Queries | Local (ms) | 100M (s) | Cplx | Score | Overlap |
|---:|---|---|---:|---:|---|---:|---|
| 1 | N2 dict segment-local pre-agg | Q12–14, Q28, Q33, Q34 | 250–400 | 1.5–2.5 | Med | ~1.0 | — |
| 2 | N1 regex input memoization | Q28 | 300–450 | 0.8–1.2 | Low | ~1.0 | — |
| 3 | N9 bucketed phase-1 storage | Q32, Q15, Q35 | 80–150 | 1.5–2.5 | Med-High | ~0.8 | #36 follow-up |
| 4 | N3 dict-aware text TopN | Q25 | 100–120 | 1.0–1.3 | Med | ~0.6 | QUERY_ANALYSIS Q25 note |
| 5 | N8 SoA + SIMD kernels | broad | 100–200 | 1–2 | High | ~0.5 | VECTORIZE P4 |
| 6 | N4 parallel Phase 2 decode | Q23 | 120–180 | 0.1–0.2 | Low-Med | ~0.15 | — |
| 7 | N5 mixed-key memoization | Q14, Q21, Q39 | 30–50 | 0.2–0.4 | Low-Med | ~0.2 | — |
| 8 | N7 metadata/colstats cache | all sub-30ms | 40–60 | 0.2–0.3 | Med | ~0.15 | #38 branch |
| 9 | N6 CASE WHEN arena | Q39 | 15–25 | 0.1–0.15 | Low | ~0.12 | — |

Recommended order: **N1 → N2** (same files, Q28 alone is a third of the local
gap; N1 is an afternoon, N2 builds on its measurement), then **N3** (worst
local ratio in the bench), then re-evaluate **N9** once the landed #36 rework
has EC2 numbers. N4/N6 are cheap fillers that can ride along; N7 is worth it
if local/interactive latency is a product goal, not for the bench total; N8
needs an SoA decode representation to exist first (the reverted
decompressed-cache prototype sketched one).

Local-total projection if ranks 1–4 + N4/N6 land as sized: roughly
5.74 s → **~4.6–4.9 s** vs CH 2.59 s (Q28 ~1.3 s, the mixed-agg class ~−0.3 s,
Q25 ~35 ms, Q23 ~220 ms). The residual is dominated by Q32/Q18-class
hash-probe physics (N9/#36 territory) and LIKE decode (storage-v2/cache
territory) — both owned by in-flight tracks.

## 4. Explicitly not (re-)proposed

- **HLL sketches** — #43, deprioritized after (a)(b)(c) captured ~3.9 s.
- **Dict sidecar blobs** — #45, premise measured wrong (dicts are 63–91 % of
  dense text blobs).
- **Trigram/bloom LIKE pruning for LZ4 text** — #33/#25, saturation kills it.
- **Text-empty segment pruning** — #46, reverted; ClickBench data too mixed.
- **PG parallel-safe custom paths** — #35, oversubscription + DeltaXAppend
  rearchitecture; internal threads already saturate cores.
- **Selection-based sparse Phase 2 decode** — #29, icache regressions; N4
  deliberately parallelizes the *existing* decode instead.
- **Cross-backend compressed-blob/colstats shmem cache** — `shmem_query_cache`
  post-mortem stands; N7 is backend-local (no LWLocks) and caches decoded
  rows, not bytes.
- **Per-(AdvEngineID, segment) pre-aggregated counters** (Q7-class) — storage
  schema change for one query shape; Q7's local gap is 8 ms.

---

## Round 2 candidates (2026-06-11, post-N1/N2/N3 calibration)

Lesson applied from round 1: N2/N3 were built from projections without
fresh per-query profiles and measured ~nothing (N1 −8% vs −70%
projected). Every candidate below requires an EXPLAIN-grounded profile
on the 100M EC2 box BEFORE implementation.

Lanes chosen to change *what work exists* rather than micro-optimize
existing loops:

### R1. FSST string encoding for text columns

DuckDB-style FSST supports equality/comparison/substring evaluation
directly on the compressed form — no dict-blob decompress to answer
`LIKE`/`ORDER BY`/`=`. Attacks the entire Q20–22/Q25/Q33 family at the
root (the N3 post-mortem showed dict decompress, not candidate
generation, is Q25's real cost). New codec: version-neutral, gated per
column at compress time. Effort: medium-high. Profile first: how much
of those queries' time is text-blob decompress vs scan.

### R2. Runtime join filters (dynamic fact pruning)

At executor time, collect dim-side join keys and probe fact-side
partition blooms/segment minmax before scanning (Spark's "dynamic
partition pruning"). RTABench is full of this shape; Postgres has no
native runtime filters. Builds directly on the #47 sentinel + bloom
infrastructure. Effort: medium. Profile: RTABench joins' fact-scan
share.

### R3. Native rollup segments (compress-time pre-aggregation)

Per-(time-bucket, group) pre-aggregated rollup segments built at
compress time would unlock the 10 RTABench `1000_*` queries currently
skipped entirely (they assume TimescaleDB continuous aggregates) — a
whole-benchmark-column gap, not a per-query one. Effort: high
(catalog + query-matching). Strategic, not tactical.

### R4. Z-order / secondary zone maps

Compress-time row ordering by a space-filling curve over 2–3 hot
columns makes minmax pruning effective on all of them (ClickHouse
needs hand-built skip indexes for the same effect). Effort: medium;
benefits any multi-predicate filter workload.

### R5. Per-value count sidecars for low-cardinality ints

Valbitmap-style sidecar with counts → Q7-class GROUP BYs answered
from metadata. (Supersedes the round-1 "per-(AdvEngineID, segment)
counters" note above with the existing valbitmap infra as template.)
Effort: low-medium.

### R6. Min/max-ordered segment visiting for numeric ORDER BY LIMIT

Generalize #37's early-termination from text to numeric sort columns:
visit segments in minmax order, stop when no remaining segment can
beat the current heap. Effort: low.

### R7. Segment-size sweep

30K rows/segment is unexamined dogma; 60–120K halves per-segment fixed
costs and improves compression, at pruning-granularity cost. Zero code:
one reload A/B per size on the EC2 box.

### PG18/19 lane (see PG18_19_OPPORTUNITIES.md)

read_stream TOAST prefetch (PG17+ API, AIO on 18+), chunked
STORAGE-MAIN blob rows (promise-preserving TOAST bypass), skip-scan
index audit — all version-gated per the PG17+ compatibility rule.

### Falsified: small-scale under-parallelization (2026-06-11)

Investigated and rejected — worker counts are CPU-derived
(`get_parallel_workers`, num_cpus min 16), all parallel dispatch gates
fire at 330 segments/10M rows, thread-spawn overhead is ~10% on full
scans only. Sole residual: pipeline detoast batch_size floors at
`2*n_workers` (~2–4 ms local, no-op at 100M); not worth touching the
empirically calibrated batching constants for that without EC2 bench
access to validate.

### R7 answered + a new lead: possible superlinear cost in segment_size (2026-06-11)

Local 10M sweep, identical config except segment_size: 30K = 5.07s,
120K = 52.86s (43/43 correct both). 10× slowdown for 4× segment size
points at something superlinear (quadratic?) in per-segment row count
on a hot path — candidates: arena/Vec growth patterns, TopN candidate
handling, per-segment dict builds, bitpack offset math. Worth a
profile: if a hot path is quadratic, 30K is paying a hidden tax too.
Verdict for R7 itself: 30K is on the right side of the curve; do not
raise it.

### Prefetch verdict (2026-06-12): whole-relation variant REVERTED

Cold A/B on 100M (its own regime): catastrophic regression — Q23 cold
1.9s -> 50.5s, Q20 17.2 -> 54.4s, Q28 16.4 -> 54.7s, Q33 17.3 -> 55.5s.
The prototype's whole-relation streaming reads the entire blobs TOAST
relation (~14GB at gp2 throughput) regardless of how few segments
survive pruning; the >50%-surviving gate fired far too often at
partition granularity. Follow-up candidate (R-prefetch-v2): stream
ONLY surviving blobs' TOAST chunk ranges (resolve va_valueid chunk
locations via the toast index first); expected to capture the latency-
hiding win without the read amplification.
