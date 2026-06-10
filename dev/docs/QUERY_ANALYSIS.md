# ClickBench Query-by-Query Analysis

Investigation of all 43 ClickBench queries on the full 100M row dataset
(c6a.4xlarge EC2, PostgreSQL 18, pg_deltax). Each section records the
EXPLAIN ANALYZE output, what dominates execution, and improvement
ideas.

> Environment: 18 partitions, ~3338 compressed segments total,
> `pg_deltax.parallel_workers=0` (auto, capped at 16), `max_parallel_workers=8`.
> Meta table split into narrow meta + wide stats table. Local meta table
> cache. SPI planning cache. Length-sidecar blob for text columns.
> Partitioned parallel merge for both compact (int) and mixed (text) paths.
> Hashbrown-backed CountDistinct accumulators + parallelized CD merge
> + parallel CD count in speculative top-N Phase 5.

## What changed since the last analysis

Landed in the interval: CountDistinct acceleration via hashbrown +
parallel partitioned CD merge (`PERF_IMPROVEMENTS.md` #43 fixes (a)(b)(c)).

| Change | Queries helped | Effect |
|--------|----------------|--------|
| hashbrown HashSet for CD accumulators | Q4, Q5 | serial-merge latency per-insert −36% |
| Parallel CD merge partitioned by hash | Q4, Q5 | Q4 2.0 → 0.7 s, Q5 1.2 → 0.75 s |
| Parallel CD count in speculative Phase 5 | Q9 | finalize 317 → 65 ms, Q9 1.49 → 1.22 s |

Bench best-of-3 (seconds) — before/after snapshot:

| Query | Before | After | Speedup |
|-------|--------|-------|---------|
| Q4 COUNT(DISTINCT UserID) | 3.022 | **0.693** | 4.4× |
| Q5 COUNT(DISTINCT SearchPhrase) | 1.848 | **0.758** | 2.4× |
| Q9 RegionID multi-agg (CD inside) | 1.499 | **1.221** | 1.2× |

Bench hot-run total: **~65 s → ~59 s** (−6 s, −9 %).

**Now faster than ClickHouse: Q3, Q24, Q26, Q28, Q33, Q34** (6 queries,
same set as last round).

## Top-level findings (actionable)

Ranked by remaining cumulative wallclock across the benchmark.

### F3. Detoast still dominates most DeltaXAgg queries (unchanged)

**Impact: 20+ queries, ~30 s cumulative wallclock.** Pipelined detoast
is active and helps a little, but serial TOAST I/O sets a floor.
Detoast numbers from warm EXPLAIN ANALYZE on queries still above CH:

| Query | detoast (ms) | Total DeltaX (ms) |
|-------|--------------|-------------------|
| Q22 | 2,391 | 3,621 |
| Q20 | 2,182 | 6,725 |
| Q32 | 2,088 | 9,478 |
| Q21 | 1,351 | 1,947 |
| Q28 | 1,186 | 6,612 |
| Q33 | 1,058 | 2,459 |
| Q34 | 1,053 | 2,478 |
| Q18 | 1,023 | 3,294 |
| Q9  |   911 | 1,136 |
| Q31 |   769 | 1,619 |

Previously investigated and ruled out (no change): inline `STORAGE MAIN`
(LZ4-on-LZ4 compression is a net I/O win), `STORAGE EXTERNAL` (same
reason), tightening `needed_cols` (already correct), pipelined detoast
itself (landed, limited impact once worker:detoast ratio exceeds ~6:1).

**Note on caching layer.** A broad cross-backend cache (compressed
blobs, time columns, segment_by, colstats, ndistinct) was explored
on branch `shmem_query_cache` (commit `59b01fd`) and shelved —
"didn't win much" because the OS page cache already covers warm
repeat reads and the LWLock overhead cancels savings when the
working set fits in buffers. Session-local variants would face the
same ceiling with less leverage. Treating this class of idea as
exhausted for the bench regime.

### F4. Merge phase on essentially-unique GROUP BY keys (unchanged, no clear path)

High-cardinality merge on near-unique integer keys remains the biggest
single offender in the bench. Partitioned parallel merge is active but
each thread still iterates every worker's full map with a hash-modulo
filter, so work is O(total entries).

| Query | merge (ms) | pre_topn_groups | Total DeltaX (ms) |
|-------|-----------|-----------------|-------------------|
| Q32 WatchID+ClientIP | 5,772 | 99,997,494 | 9,478 |
| Q15 Top UserID | 1,070 | 21,981,595 | 1,994 |
| Q35 ClientIP arithmetic | 832 | 20,960,937 | 1,691 |

**No clear path forward.** The previously-suggested solution —
`PERF_IMPROVEMENTS.md` #36 two-level hash aggregation — was
implemented on the `two_phases_hash` branch and **reverted**: the
bench-level improvement (~−1.2 s, −1.8 %) was within run-to-run
noise. The follow-up (partition `CompactAccStorage` and
`CountDistinctSideCar` by sub-index too) is spec'd but its projected
~2 s saving on Q32 has to be weighed against the storage-refactor
cost — touches every accumulator accessor in hot code. Currently not
prioritized.

### F5. Q20/Q21/Q22 URL LIKE on LZ4 columns — largely fixed by #49

**Update 2026-06-10:** Q20 was running single-threaded — no GROUP BY
meant every parallel dispatch rejected it (see
`PERF_IMPROVEMENTS.md` #49). With the no-GROUP-BY parallel-mixed
relaxation plus a single-sweep memmem LIKE for LZ4 columns, Q20 went
**4.63 s → 1.02 s** warm (the earlier 6.73 s figure predates the blob
cache). Q22 3.46 → 3.11 s from the sweep alone. Remaining Q20 cost is
~⅓ detoast / ⅓ LZ4 decompress / ⅓ sweep+row-loop. The older notes
below on inverted-index approaches still apply to whatever gap
remains vs ClickHouse, but the "22× CH" outlier status is gone.

### F6. `COUNT(DISTINCT)` on blob-backed columns (narrowed, deprioritized)

With hashbrown + parallel CD merge done, the dramatic CD bottleneck is
gone. Q4/Q5 are now ~700 ms each, dominated by detoast (~400–600 ms)
and per-worker hash-build (~400–600 ms). HLL-sketch sidecars (#43)
could further collapse both to ~100–200 ms, but the remaining
~1.2 s cumulative gain no longer justifies the medium implementation
cost + approximate-semantics GUC tradeoff given how much ground
(a)+(b)+(c) already recovered. #43 marked deprioritized in
`PERF_IMPROVEMENTS.md`.

### F7. Text `<> ''` segments not pruned — tried, reverted

Several queries filter on `WHERE <text_col> <> ''`. The compressor
already tracks `_nonzero_count_<col>` for text columns, and a Low-LOC
extension to the segment-pruning gate can use it to skip segments
that are fully empty / fully non-empty for the filter column.
Implementation landed and was reverted on 2026-04-21 after measurement
showed ClickBench data has almost no fully-empty segments for text
columns (4–6 of 3338 for SearchPhrase, 0 for MobilePhoneModel). See
`PERF_IMPROVEMENTS.md` #46 for the full write-up and the spec to
revive if a more clumpy dataset motivates it.

### F8. `GROUP BY … LIMIT N` without `ORDER BY` — landed

Q17: `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID,
SearchPhrase LIMIT 10`. PG semantics require the returned rows to
carry **correct global counts** for their keys; a naive
"stop early after N local groups" shortcut produces partial counts
and is a correctness bug.

Landed 2026-04-21 via a two-pass filter: Phase 0 extracts `bare_limit`
distinct group-key hashes from one segment on the main thread,
Phase 1 workers then filter per-row via that set (≤ N-entry probe,
L1-resident). Measured: Q17 **1.69 s → 0.92 s (−45 %)**, agg
**805 ms → 215 ms (−74 %)**. No regressions on other queries. See
`PERF_IMPROVEMENTS.md` #44.

---

## Query-by-query details

Format per query:
- **Query** (from queries.sql)
- **CH / deltax / ratio** (CH = ClickHouse c6a.4xlarge reference, deltax = bench best-of-3)
- **EXPLAIN ANALYZE timing breakdown** (warm run unless noted)
- **Analysis + potential improvements**

### Q0 — COUNT(*)

```
SELECT COUNT(*) FROM hits;
```

- CH 0.001 s / deltax **0.003 s** / **3.0×**
- `DeltaXCount`, metadata=0.001 ms, heap_scan=0.000 ms.
- **Near-optimal.** Nothing actionable.

### Q1 — COUNT(*) WHERE AdvEngineID <> 0

```
SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;
```

- CH 0.006 s / deltax **0.022 s** / **3.7×**
- `DeltaXAgg` metadata-fast-path, heap_scan=15 ms.
- Remaining gap is framework overhead (heap_scan + planning).

### Q2 — SUM/AVG full-scan

```
SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;
```

- CH 0.021 s / deltax **0.027 s** / **1.3×**
- Fully metadata-resolved (3338/3338 segments).

### Q3 — AVG(UserID)

```
SELECT AVG(UserID) FROM hits;
```

- CH 0.027 s / deltax **0.026 s** / **0.96× (faster)**
- Metadata-resolved.

### Q4 — COUNT(DISTINCT UserID)

```
SELECT COUNT(DISTINCT UserID) FROM hits;
```

- CH 0.353 s / deltax **0.693 s** / **2.0×** ← **4.4× faster than before**
- detoast=383, agg=393, merge=272.
- Down from 3.02 s via hashbrown + parallel CD merge (a)+(b).
- **Remaining improvement:** HLL sketches (#43). Projected 0.7 → 0.2 s.

### Q5 — COUNT(DISTINCT SearchPhrase)

```
SELECT COUNT(DISTINCT SearchPhrase) FROM hits;
```

- CH 0.623 s / deltax **0.758 s** / **1.2×** ← **2.4× faster than before**
- detoast=592, agg=607, merge=93.
- **Remaining improvements:** HLL sketches (#43), dict sidecar blob
  (new). Either could bring Q5 to ~0.2 s.

### Q6 — MIN/MAX EventDate

```
SELECT MIN(EventDate), MAX(EventDate) FROM hits;
```

- CH 0.010 s / deltax **0.014 s** / **1.4×**
- `DeltaXMinMax`, metadata-resolved.

### Q7 — GROUP BY AdvEngineID

```
SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;
```

- CH 0.009 s / deltax **0.091 s** / **10.1×**
- detoast=35, agg=30. 2257 segments, 630 K rows, 18 groups.
- CH resolves this from metadata (18 fixed values). Could pre-compute
  per-(AdvEngineID, segment) COUNT counters in stats table — ambitious.

### Q8 — GROUP BY RegionID COUNT(DISTINCT UserID)

```
SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;
```

- CH 0.452 s / deltax **1.017 s** / **2.3×**
- detoast=648, agg=34, **merge=315**.
- **Remaining improvement:** HLL per-group sketches (#43). Projected 1.0 → 0.7 s.

### Q9 — RegionID multi-agg (includes CountDistinct)

```
SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;
```

- CH 0.522 s / deltax **1.221 s** / **2.3×** ← improved 1.50 → 1.22 s
- detoast=911, decompress=17, agg=43, finalize=65, topn_select=105.
- finalize dropped 317 → 65 ms from parallelized CD count (fix (c)).
- Remaining is mostly detoast.

### Q10 — MobilePhoneModel users

```
SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;
```

- CH 0.147 s / deltax **0.461 s** / **3.1×**
- detoast=291, decompress=41, agg=43, merge=27.
- Dominant: detoast (F3). Also a candidate for F7 text-empty segment
  pruning (50 % empty globally; some segments likely fully empty).

### Q11 — MobilePhone + Model users

```
SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;
```

- CH 0.143 s / deltax **0.481 s** / **3.4×**
- detoast=305, decompress=93, agg=41, merge=27.
- Dominant: detoast. Same F7 candidate as Q10.

### Q12 — Top 10 SearchPhrase

```
SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
```

- CH 0.599 s / deltax **1.111 s** / **1.9×**
- detoast=389, decompress=144, agg=563, topn_select=10.
- agg=563 ms on 55 M rows with 4.8 M groups. F7 could prune some
  segments; otherwise near floor.

### Q13 — SearchPhrase users (COUNT DISTINCT)

```
SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
```

- CH 0.804 s / deltax **2.135 s** / **2.7×**
- detoast=545, decompress=163, agg=758, **merge=553**.
- Per-group HLL sketches could trim the CD portion of agg+merge.

### Q14 — SearchEngine + SearchPhrase

```
SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;
```

- CH 0.597 s / deltax **1.214 s** / **2.0×**
- detoast=431, decompress=155, agg=619. Same shape as Q12.

### Q15 — Top 10 UserID

```
SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;
```

- CH 0.384 s / deltax **1.976 s** / **5.1×**
- detoast=580, agg=313, **merge=1070**.
- 22 M distinct UserIDs. Partitioned merge active but O(total entries)
  reads scatter over 200 MB per worker.
- **Improvement:** complete #36 two-level hash agg (partition
  `CompactAccStorage` too). Expected: 2.0 → ~1.2 s.

### Q16 — UserID + SearchPhrase top

```
SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
```

- CH 1.709 s / deltax **2.039 s** / **1.2×**
- detoast=513, decompress=187, agg=1145. Competitive.

### Q17 — UserID + SearchPhrase (no order)

```
SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;
```

- CH 0.999 s / deltax **1.686 s** / **1.7×**
- detoast=523, decompress=176, agg=813. No ORDER BY.
- **F8:** `GROUP BY LIMIT` without `ORDER BY` has no early termination.
  Projected: 1.69 → ~0.02 s.

### Q18 — UserID + extract(minute) + SearchPhrase

```
SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
```

- CH 3.041 s / deltax **3.626 s** / **1.2×**
- detoast=1023, decompress=315, agg=1992. Competitive. Three-column
  detoast + 34 M groups makes this inherently hard.

### Q19 — Point lookup UserID = const

```
SELECT UserID FROM hits WHERE UserID = 435090932899640449;
```

- CH 0.003 s / deltax **0.043 s** / **14×**
- `DeltaXAppend`. segments=50, segments_skipped=3288
  (1870 minmax + 1418 bloom). heap_scan=23, decompress=13.
- bloom hit=5926 buffer pages for 1468 bloom-checked segments.
- **Improvement:** Partition-level bloom filter (18 coarse blooms
  across partitions) to prune most of the bloom I/O. Expected: 43 → ~15 ms.

### Q20 — COUNT(*) WHERE URL LIKE '%google%'

```
SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';
```

- CH 0.312 s / deltax **6.728 s** / **21.6×**
- segments=1703 (after dict pruning). rows_processed=15,911.
- detoast=2182, decompress=2680, agg=1837.
- **Open problem.** URL is LZ4; dict-based #40 doesn't apply.

### Q21 — SearchPhrase MIN(URL) WHERE URL LIKE '%google%'

```
SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
```

- CH 0.098 s / deltax **1.948 s** / **19.9×**
- detoast=1351, decompress=313, agg=358, topn_select=0.4.
- Dominant: detoast URL blobs. Same fundamental issue as Q20.

### Q22 — Title LIKE Google + URL NOT LIKE

```
SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
```

- CH 0.717 s / deltax **3.760 s** / **5.2×**
- detoast=2391, decompress=699, agg=702. segments=2915.
- Title is dict-encoded → #40 dict-accelerated LIKE applies. URL is
  LZ4, only detoast-level improvements help.

### Q23 — SELECT * WHERE URL LIKE ... ORDER BY EventTime LIMIT 10

```
SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;
```

- CH 0.393 s / deltax **0.473 s** / **1.2×**
- `DeltaXAppend` TopN, 12 surviving segments, Phase 2 on 7 segments.
  **Competitive.**

### Q24 — SearchPhrase ORDER BY EventTime LIMIT 10

```
SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;
```

- CH 0.147 s / deltax **0.107 s** / **0.73× (faster)**

### Q25 — ORDER BY SearchPhrase LIMIT 10

```
SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;
```

- CH 0.192 s / deltax **1.912 s** / **10.0×**
- `DeltaXAppend`, 3332 segments, decompress=2285 ms (cumulative across
  16 parallel workers).
- **Improvement: dict-only ORDER BY text LIMIT.** For dict-encoded
  columns, read only the dict portion of each blob, merge candidates
  via min-heap. Combined with the dict sidecar blob idea —
  no full-blob detoast. Expected: 1.9 → ~150 ms.

### Q26 — ORDER BY EventTime, SearchPhrase LIMIT 10

```
SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;
```

- CH 0.149 s / deltax **0.108 s** / **0.72× (faster)**

### Q27 — CounterID AVG(length(URL)) HAVING c > 100K

```
SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;
```

- CH 0.083 s / deltax **0.550 s** / **6.6×**
- detoast=244, decompress=26, agg=243, merge=1.
- Length-sidecar blob active; remaining detoast is CounterID +
  length-sidecar.

### Q28 — Referer REGEXP_REPLACE GROUP BY HAVING

```
SELECT REGEXP_REPLACE(Referer, ...) AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;
```

- CH 9.582 s / deltax **6.727 s** / **0.70× (faster)**
- detoast=1186, decompress=3186, agg=2073, merge=268.
- **Outperforms ClickHouse.**

### Q29 — Wide SUM 89 cols

```
SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), ... SUM(ResolutionWidth + 89) FROM hits;
```

- CH 0.029 s / deltax **0.045 s** / **1.6×**
- Fully metadata-resolved.

### Q30 — SearchEngine + ClientIP multi-agg

```
SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;
```

- CH 0.342 s / deltax **1.032 s** / **3.0×**
- detoast=436, decompress=111, agg=433, merge=0, topn_select=13.
- SearchPhrase length-sidecar in use. F7 candidate (SearchPhrase
  only 13 % non-empty globally — lots of segments likely fully empty).

### Q31 — WatchID + ClientIP with SearchPhrase filter

```
SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
```

- CH 0.562 s / deltax **1.785 s** / **3.2×**
- detoast=769, decompress=185, agg=681, merge=0.
- Same F7 candidate as Q30 (13 % non-empty SearchPhrase).

### Q32 — WatchID + ClientIP all

```
SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
```

- CH 3.793 s / deltax **9.417 s** / **2.5×**
- detoast=2088, agg=1610, **merge=5772**.
- 99,997,494 essentially-unique groups.
- **Primary remaining bottleneck in the benchmark.** Complete #36 by
  partitioning `CompactAccStorage` → estimated 9.5 → ~7.5 s.

### Q33 — GROUP BY URL ORDER BY c DESC LIMIT 10

```
SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;
```

- CH 2.782 s / deltax **2.661 s** / **0.96× (tied/faster)**
- detoast=1058, decompress=220, agg=1223. **At parity.**

### Q34 — GROUP BY 1, URL

```
SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;
```

- CH 2.851 s / deltax **2.677 s** / **0.94× (faster)**

### Q35 — GROUP BY ClientIP, IP−1, IP−2, IP−3

```
SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;
```

- CH 0.297 s / deltax **1.713 s** / **5.8×**
- detoast=498, agg=329, **merge=832**.
- 21 M distinct ClientIPs → same #36 territory as Q15/Q32.

### Q36 — Top URLs for CounterID=62

```
SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;
```

- CH 0.043 s / deltax **0.103 s** / **2.4×**
- segments=26, rows_processed=671 K. Reasonable.

### Q37 — Top Titles for CounterID=62

- CH 0.021 s / deltax **0.041 s** / **2.0×**

### Q38 — CounterID=62 links OFFSET 1000

- CH 0.017 s / deltax **0.078 s** / **4.6×**
- segments=26, rows_processed=47,740, TopN pulls 1010, PG limit picks 10.

### Q39 — CounterID=62 traffic src

```
SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE ... THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND ... GROUP BY ... ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;
```

- CH 0.077 s / deltax **0.217 s** / **2.8×**
- detoast=40, decompress=32, agg=114, merge=49.

### Q40 — CounterID=62 URLHash

- CH 0.013 s / deltax **0.138 s** / **10.6×**
- segments=26, rows_processed=89,914, 41 K groups.
- detoast=12, decompress=71, agg=23. Decompress=71 ms for 90 K rows
  = ~0.8 µs/row — column-pruning audit warranted.

### Q41 — CounterID=62 window dim

- CH 0.009 s / deltax **0.052 s** / **5.8×**
- Falls back to PG Sort because result_rows > OFFSET 10000.

### Q42 — CounterID=62 by minute

- CH 0.008 s / deltax **0.041 s** / **5.1×**
- 671 K rows pass through PG's tuple interface — framework overhead.

---

## Summary table

| Q | CH (s) | deltax (s) | Ratio | Dominant cost | Key improvement |
|---|--------|-----------|-------|---------------|-----------------|
| 0 | 0.001 | 0.003 | 3.0× | framework | — |
| 1 | 0.006 | 0.022 | 3.7× | framework | — |
| 2 | 0.021 | 0.027 | 1.3× | metadata I/O | — |
| 3 | 0.027 | 0.026 | **0.96×** | — | — |
| 4 | 0.353 | 0.693 | 2.0× | detoast + agg | — (HLL deprioritized) |
| 5 | 0.623 | 0.758 | 1.2× | detoast + agg | — (sidecar ruled out) |
| 6 | 0.010 | 0.014 | 1.4× | metadata I/O | — |
| 7 | 0.009 | 0.091 | 10.1× | detoast | — |
| 8 | 0.452 | 1.017 | 2.3× | detoast + merge | — |
| 9 | 0.522 | 1.221 | 2.3× | detoast | — |
| 10 | 0.147 | 0.461 | 3.1× | detoast | — |
| 11 | 0.143 | 0.481 | 3.4× | detoast | — |
| 12 | 0.599 | 1.111 | 1.9× | agg + detoast | — |
| 13 | 0.804 | 2.135 | 2.7× | detoast + agg + merge | — |
| 14 | 0.597 | 1.214 | 2.0× | detoast + agg | — |
| 15 | 0.384 | 1.976 | 5.1× | merge | — (#36 reverted) |
| 16 | 1.709 | 2.039 | 1.2× | agg | — |
| 17 | 0.999 | 1.686 | 1.7× | agg | **F8 early-term** |
| 18 | 3.041 | 3.626 | 1.2× | detoast + agg | — |
| 19 | 0.003 | 0.043 | 14.3× | bloom scan | partition bloom |
| 20 | 0.312 | 6.728 | **21.6×** | detoast URL (LZ4) | open problem |
| 21 | 0.098 | 1.948 | 19.9× | detoast URL | — |
| 22 | 0.717 | 3.760 | 5.2× | detoast URL+Title | — (#40 already landed) |
| 23 | 0.393 | 0.473 | 1.2× | decompress | — |
| 24 | 0.147 | 0.107 | **0.73×** | — | — |
| 25 | 0.192 | 1.912 | 10.0× | decompress text | — (sidecar ruled out) |
| 26 | 0.149 | 0.108 | **0.72×** | — | — |
| 27 | 0.083 | 0.550 | 6.6× | detoast (sidecar) | — |
| 28 | 9.582 | 6.727 | **0.70×** | regex | — |
| 29 | 0.029 | 0.045 | 1.6× | metadata I/O | — |
| 30 | 0.342 | 1.032 | 3.0× | detoast + agg | — |
| 31 | 0.562 | 1.785 | 3.2× | detoast + agg | — |
| 32 | 3.793 | 9.417 | 2.5× | **merge** | — (#36 reverted) |
| 33 | 2.782 | 2.661 | **0.96×** | agg | — |
| 34 | 2.851 | 2.677 | **0.94×** | agg | — |
| 35 | 0.297 | 1.713 | 5.8× | merge | — (#36 reverted) |
| 36 | 0.043 | 0.103 | 2.4× | agg | — |
| 37 | 0.021 | 0.041 | 2.0× | agg | — |
| 38 | 0.017 | 0.078 | 4.6× | heap_scan + agg | — |
| 39 | 0.077 | 0.217 | 2.8× | agg + merge | — |
| 40 | 0.013 | 0.107 | 8.2× | decompress | — (#48 landed) |
| 41 | 0.009 | 0.052 | 5.8× | agg + detoast | — |
| 42 | 0.008 | 0.041 | 5.1× | framework | — |

**Queries faster than CH:** Q3, Q24, Q26, Q28, Q33, Q34 (6)
**Within 2× of CH:** Q0, Q2, Q5, Q6, Q14, Q16, Q17, Q18, Q23, Q29, Q36 (11)
**2–5× of CH:** Q1, Q4, Q8, Q9, Q10, Q11, Q12, Q13, Q30, Q31, Q32, Q37 (12)
**5–10× of CH:** Q15, Q22, Q25, Q27, Q35, Q38, Q39, Q41, Q42 (9)
**>10× of CH:** Q7, Q19, Q20, Q21, Q33(gap), Q34(gap), Q40 (5 hard outliers)

Bench total (hot best-of-3): **~59 s** (from ~65 s).

---

## Prioritized improvement list

**Scope rule:** only genuinely untried items are ranked here. Ideas
that were implemented and reverted (#36 two-level hash, #46 F7
text-empty pruning), spec'd but deprioritized after recent
measurements (#43 HLL), or investigated and found not worth
pursuing (#45 dict sidecar, #40 dict-accel LIKE), are listed
separately below so the top of the list reflects real forward work.

| # | Improvement | Queries helped | Est. benefit | Complexity | In PERF? |
|---|-------------|----------------|--------------|------------|---------|
| **1** | **Partition-level bloom for point lookups** | Q19 | ~30 ms | Low-Med | #47 |

The list is thin — most large optimization targets have either
landed or been ruled out after measurement. Further bench gains
likely need structural changes (smaller segments, parallel-safe
custom scans so PG workers can compose with Rust threads, or a
different storage layout for TOAST) rather than algorithmic
tweaks to the current plan.

Already landed:
- **F8 (#44)** — Q17 1.69 s → 0.92 s (−45 %).
- **#48 byte-aligned bitpack fast path** — Q40 138 → 107 ms (−22.5 %),
  bench total −252 ms across Q40/Q22/Q20/Q31/Q16.

### Recommended order of attack

1. **Partition-level bloom filter for Q19 (#47).** Coarse bloom on
   18 partitions before per-segment blooms. Collapses most of the
   5926 bloom buffer pages currently read on warm runs. Modest
   saving but clean implementation.

### Already-explored items (not in the priority list above)

- **F8 GROUP BY LIMIT early-term (#44).** **Landed 2026-04-21.**
  Q17 1.69 s → 0.92 s (−45 %). Correctness verified. No
  regressions on other queries.
- **Byte-aligned bitpack fast path (#48).** **Landed 2026-04-21.**
  Originally scoped as a column-pruning audit on Q40 — the real
  bottleneck turned out to be `unpack_bits_u64` doing a
  byte-by-byte loop even when `bits=64`. Added fast paths for
  byte-aligned widths. Q40 138 → 107 ms (−22.5 %), bench total
  −252 ms across Q40/Q22/Q20/Q31/Q16 (all queries that decompress
  high-cardinality i64 columns).
- **F7 text-empty segment pruning (#46).** **Tried and reverted
  2026-04-21.** The pruning logic works correctly (4–6 segments
  prunable on Q30/Q31) but ClickBench data is too well-mixed —
  bench impact indistinguishable from noise. Reverted to avoid
  carrying ~100 LOC for a speculative "might help real-world
  clumpy data" benefit.
- **Dict sidecar blob (#45).** **Investigated and dropped
  2026-04-21.** Original ~3 s projection rested on a wrong premise
  (dict = ~5 KB of a 200 KB blob). On-disk inspection showed dicts
  are **63–91 %** of dense-text blobs (Title 78 %, URL 72 %,
  Referer 91 %, SearchPhrase 63 %). Revised savings:
  ~300–500 ms total bench, against a +2–3 GB storage cost. Not
  worth the trade.
- **#40 dict-accelerated LIKE.** **Re-audited 2026-04-21,
  largely already implemented.** `apply_text_like_filter`
  computes `dict_matches` once per segment; `segment_skippable_by_dict`
  skips segments with zero matching dict entries. The remaining
  spec item ("skip other columns within a segment that has some
  matches") is architecturally incompatible with PG's TOAST
  (detoast is all-or-nothing per blob). No further work planned.
- **#36 two-level hash aggregation.** Implemented on branch
  `two_phases_hash`, measured −1.2 s at bench level (within run-to-run
  noise), **reverted 2026-04-18**. Follow-up (partition
  `CompactAccStorage` / `CountDistinctSideCar` too) spec'd but gated
  on whether the projected ~2 s Q32 win justifies the
  accumulator-accessor refactor. Not currently prioritized.
- **HLL sketches (#43).** Original target was ~4.5 s bench saving.
  Fixes (a) hashbrown, (b) parallel CD merge, (c) parallel CD count
  in Phase 5 (all DONE) captured ~3.9 s of that; residual HLL-only
  saving is ~1.2 s, which no longer justifies the medium
  implementation cost + approximate-semantics GUC. Deprioritized in
  PERF_IMPROVEMENTS.md.
- **Cross-backend / session-level blob & stats cache.** Explored in
  branch `shmem_query_cache` (commit 59b01fd). Cached compressed
  blobs, time columns, segment_by columns, partition stats,
  ndistinct, and colstats across PG backends, each gated by a GUC
  for A/B measurement. Conclusion: "didn't win much" — the OS page
  cache already covers warm-path reads and the per-segment LWLock
  overhead outweighs the savings when the colstats working set fits
  in buffers. Not merged.
- **Duplicate-hinted merge for Q32.** Considered during this pass
  (detect that almost all groups have count=1 and short-circuit).
  Collapses back onto #36 follow-up (you still need a hash probe per
  entry to decide), so not a distinct optimization.
- **LZ4-compressed-byte prefilter for URL LIKE.** Only soundly
  detects matches (false negatives possible because LIKE could match
  through LZ4 backreferences). Useless for affirmative pruning.
- **Column-wise SELECT * Phase 2 decode for Q23** (#29 revisited).
  Prior binary-size-caused icache regressions on unrelated queries
  were a hard constraint. Not revisited.
