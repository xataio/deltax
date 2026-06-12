# PG18 / PG19 Opportunities for pg_deltax

Research notes (2026-06) on PostgreSQL 18 (released 2025-09) and PostgreSQL 19
beta 1 (released 2026-06-04) features that pg_deltax can exploit. Focus: our
top performance tax is per-query TOAST detoast (toast-index probes, chunk
fetches, reassembly memcpy), second is LZ4 decompression. Hard constraint
throughout: all data stays in regular Postgres tables (heap + TOAST), so
physical/logical replication, pg_dump, crash recovery and backups keep working
natively.

## TL;DR — ranked opportunities

1. **Use the `read_stream` API to prefetch blob-table heap pages and TOAST
   chunk pages before detoast.** Core's detoast path is still fully
   synchronous in PG18 *and* PG19; an extension-driven read stream over the
   toast relation turns Phase 2 cold reads into async, io_uring-batched I/O.
   Works (degraded, fadvise-based) on PG17 too — the API exists since 17.
2. **Chunking blobs into ~7.8 KB `STORAGE MAIN` rows to bypass TOAST is
   VALIDATED** (with caveats): `TOAST_TUPLE_TARGET_MAIN` = `MaximumBytesPerTuple(1)`
   ≈ 8160 bytes means a tuple up to ~8.1 KB stays inline, one tuple per page.
   This removes the toast-index probe + 4-tuples-per-page chunk layout +
   reassembly copy per value. Prior art exists (pg_largeobject does exactly
   this at 2 KB granularity); the cautionary tale is Citus columnar, which
   used custom page layouts and lost logical decoding — chunked heap rows
   do not have that problem.
3. **PG18 btree skip scan** helps every multicolumn-index probe where the
   omitted leading column is low-cardinality — our blob/colstats PKs
   (`_col_idx`, `_segment_id`) qualify when probing by `_segment_id` only.

PG19 has *no* TOAST-mechanics or executor-batching changes that help us
directly; its AIO improvements (read-ahead scheduling, worker autoscaling)
help anything we route through `read_stream` for free.

## 1. PG18 AIO and the read_stream API

### What shipped in PG18

- New `io_method` GUC: `sync` (PG17 behaviour, posix_fadvise), `worker`
  (default; pool of I/O worker processes, works on all platforms), `io_uring`
  (Linux ≥ 5.1, requires build `--with-liburing`; PGDG packages have it).
  Supporting GUCs: `io_workers` (default 3), `io_combine_limit` (default
  128 kB, max raised to 1 MB via `io_max_combine_limit`),
  `effective_io_concurrency` default raised to 16.
  [pganalyze](https://pganalyze.com/blog/postgres-18-async-io),
  [Vondra: Tuning AIO in PG18](https://vondra.me/posts/tuning-aio-in-postgresql-18/),
  [PG18 release notes](https://www.postgresql.org/docs/18/release-18.html).
- AIO is **reads only**, and only for code paths converted to the
  `ReadStream` abstraction: sequential scans, bitmap heap scans, VACUUM
  passes, ANALYZE sampling. **Plain index scans and TOAST detoast are not
  converted** — `detoast_attr() → toast_fetch_datum() →
  heap_fetch_toast_slice() → systable_getnext_ordered()` bottoms out in
  synchronous `ReadBufferExtended()` calls
  ([AIO v2.0 thread](https://www.mail-archive.com/pgsql-hackers@lists.postgresql.org/msg185692.html),
  where prefetching TOAST chunks is explicitly listed as future work).

### Can an extension use read_stream? Yes.

`src/include/storage/read_stream.h` is a public header; contrib modules are
ordinary extensions and use it (`pg_prewarm` since PG17/18, `autoprewarm` in
18), and pgvector experimented with it on -hackers
([Trying out read streams in pgvector](https://www.mail-archive.com/pgsql-hackers@lists.postgresql.org/msg171681.html)).
The AIO README says explicitly: "Most uses of AIO should be done via reusable,
higher-level helpers", i.e. `read_stream`
([src/backend/storage/aio/README.md](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/storage/aio/README.md)).

API shape ([read_stream.h, REL_18_STABLE](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/storage/read_stream.h)):

```c
ReadStream *read_stream_begin_relation(int flags, BufferAccessStrategy strategy,
    Relation rel, ForkNumber forknum,
    ReadStreamBlockNumberCB callback,       /* yields next BlockNumber */
    void *callback_private_data, size_t per_buffer_data_size);
Buffer read_stream_next_buffer(ReadStream *stream, void **per_buffer_data);
void   read_stream_reset(ReadStream *stream);
void   read_stream_end(ReadStream *stream);
```

Flags: `READ_STREAM_DEFAULT` (distance governed by
`effective_io_concurrency`), `READ_STREAM_MAINTENANCE`,
`READ_STREAM_SEQUENTIAL` (rely on OS readahead), `READ_STREAM_FULL`,
`READ_STREAM_USE_BATCHING` (PG18; AIO batch submission, callback must be
batch-safe). The block-number callback is arbitrary extension code — it does
not need to be a contiguous range (`block_range_read_stream_cb` is provided
for that case). Lower-level access (`pgaio_io_acquire()` →
`smgrstartreadv()`) is also exposed, but the README steers everyone to
read_stream; we should not need raw handles.

### How pg_deltax exploits it

Two concrete options, lowest-risk first:

- **Prewarm-style toast prefetch (no detoast logic duplicated).** Phase 2
  reads blobs in `(_col_idx, _segment_id)` order; because blobs are inserted
  column-major, each column's TOAST chunks occupy a contiguous block range
  (see COLUMNAR_STORAGE.md). Before looping over segments for a column, open
  the blob table's toast relation (`pg_class.reltoastrelid`), run a read
  stream over the block range covering that column's chunks, pin+release the
  buffers, then call normal `pg_detoast_datum()` — every chunk read hits
  shared buffers. This replaces our current reliance on Linux OS readahead
  (128 kB) with explicit, io_uring-capable readahead up to
  `effective_io_concurrency` × `io_combine_limit`, and it also works under
  `io_method=worker` and direct I/O setups where OS readahead doesn't exist.
- **TID-driven exact prefetch.** Scan the toast index to collect chunk TIDs
  for the next N blobs, feed their unique block numbers through a read-stream
  callback, then detoast. More precise (no over-read when segments are
  pruned), more code. Worth it only if the block-range version leaves gaps
  (e.g. interleaved columns after backfill).

Same applies to the blob-table heap itself (the inline rows), the blooms
table, and text-lengths sidecars when cold. On PG17 the identical code
compiles and falls back to fadvise-based prefetch (`io_method` doesn't
exist); benefit is smaller but nonzero. pgrx exposes neither API today, so
this is `extern "C"` FFI against the headers — straightforward, same as our
other direct PG calls.

Caveat: `read_stream` keeps buffers pinned across the lookahead window;
respect the existing per-backend pin budget, and call `read_stream_end()`
before long CPU phases.

## 2. PG19 beta 1 — what's in it for us

Source: [PG19 release notes draft](https://www.postgresql.org/docs/19/release-19.html),
[beta 1 announcement](https://www.postgresql.org/about/news/postgresql-19-beta-1-released-3313/),
[thebuild: Async I/O in PG19](https://thebuild.com/blog/2026/04/23/async-io-in-postgresql-19-the-year-after/),
[Habr CF 2025-11](https://habr.com/en/companies/postgrespro/articles/1010634/).

Relevant items, each with the pg_deltax angle:

- **Improved AIO read-ahead scheduling for large requests** (Andres Freund):
  prefetch window now scales with the size of the scan instead of a fixed
  distance. Anything we run through `read_stream` (item 1) gets this for
  free; also speeds the seq-scan-shaped reads our SPI queries over the blob
  table already do.
- **`io_method=worker` autoscaling** (`io_min_workers`/`io_max_workers`/
  `io_worker_idle_timeout`/`io_worker_launch_interval`, Thomas Munro): removes
  the "3 workers is a bottleneck at high concurrency" tuning trap (Vondra's
  PG18 benchmarks showed worker-count sensitivity). Less tuning advice for
  us to ship.
- **`default_toast_compression` now `lz4`** (Euler Taveira): irrelevant to us
  directly — we already declare companion BYTEA columns `COMPRESSION lz4`
  (src/compress.rs) — but it validates the choice and means plain-PG
  comparison baselines get faster detoast, slightly shrinking our headline
  ratios on TOAST-heavy queries.
- **Radix sort for tuplesort** (John Naylor): speeds the `ORDER BY` stages of
  ClickBench/RTABench top-N queries above our scan nodes; no work needed.
- **Faster internal row deformation** (David Rowley): helps SPI fetches of
  blob/colstats rows (wide-ish tuples) marginally.
- **`COUNT(1)`/`COUNT(notnull)` → `COUNT(*)`** planner rewrite: helps our agg
  pushdown matching — fewer shapes to special-case if we currently only match
  `COUNT(*)`.
- **Eager aggregation** (`enable_eager_aggregate`): planner can aggregate
  below joins; relevant to RTABench join queries where our agg-split
  currently loses to plain PG — re-benchmark those on 19.
- **Parallel TID Range Scans**; **table scans can set all-visible VM bits**;
  **SIMD `COPY FROM`**, **parallel autovacuum**, **`EXPLAIN (ANALYZE, IO)`**
  showing AIO metrics in plans (use this when investigating Phase 2 I/O),
  **`REPACK`** (online CLUSTER replacement — interesting for re-ordering the
  blob table after backfill without exclusive-lock CLUSTER).
- **NOT landed, refuting hopeful rumors:** generic **index prefetching**
  (Vondra/Geoghegan — heap prefetch driven by index scans) is still an open
  -hackers thread, not in 19 beta 1
  ([index prefetching thread](https://www.mail-archive.com/pgsql-hackers@lists.postgresql.org/msg202850.html));
  **executor batching** (Amit Langote's "Batching in executor") likewise
  in-progress; **no TOAST API / pluggable TOAST** (see §3); **no new
  bytea/large-value handling** beyond SIMD `hex_encode`/`hex_decode`
  (only matters for COPY TO of bytea, not detoast).

Net: PG19 gives us better plumbing under read_stream and better observability,
but nothing that removes the detoast tax by itself. Item 1 and item 4 remain
on us.

## 3. TOAST mechanics in PG18/19: effectively unchanged

- Chunk size: still `TOAST_MAX_CHUNK_SIZE` ≈ **1996 bytes** (derived from
  `EXTERN_TUPLES_PER_PAGE = 4`), unchanged in 18 and 19
  ([heaptoast.h](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/access/heaptoast.h),
  [TOAST docs](https://www.postgresql.org/docs/current/storage-toast.html)).
  A 1 MB blob is ~525 chunk tuples fetched via a systable index scan per value.
- Detoast API (`detoast_attr`, `detoast_attr_slice`): no signature or
  behaviour changes; still synchronous, still reassembles into a fresh
  palloc'd buffer.
- **Pluggable TOAST ("toasters") never merged** — it was proposed for PG16,
  rejected, and lives only in Postgres Pro Enterprise
  ([Postgres Pro blog](https://postgrespro.com/blog/pgsql/5969559)). Do not
  plan around it.
- Only knob movement: PG19's `default_toast_compression=lz4` (§2). One audit
  worth doing on our side: blob payloads are already extension-LZ4-compressed,
  so column-level `COMPRESSION lz4` makes TOAST attempt a second, futile
  compression pass on insert. `ALTER COLUMN _data SET STORAGE EXTERNAL` on the
  blob table would skip that CPU on write while keeping out-of-line storage.
  (The text-lengths/bloom sidecars may genuinely benefit from TOAST lz4 —
  audit per table.)

## 4. Validate-or-refute: ~7.8 KB STORAGE MAIN chunk rows instead of TOAST

**Verdict: validated — the numbers work, with sharp edges on sizing.**

Mechanics, from
[heaptoast.h](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/access/heaptoast.h):

- `TOAST_TUPLE_TARGET_MAIN = MaximumBytesPerTuple(1)` =
  `MAXALIGN_DOWN((8192 - MAXALIGN(24 + 1*4)) / 1)` = **8160 bytes**. A column
  with `STORAGE MAIN` is compressed first and moved out-of-line "only as a
  last resort" if the tuple still exceeds 8160 bytes. So a row of
  (small fixed PK columns + one bytea) whose total tuple size ≤ 8160 stays
  inline — exactly one tuple per page.
- Payload budget: 8160 − 24 (tuple header) − null bitmap/padding − PK columns
  (e.g. `_col_idx int4` + `_segment_id int4` + `_chunk_no int4` = 12) − 4
  (varlena header) ≈ **~8.1 KB max, ~7.8 KB with margin**. Size the chunk so
  it can *never* exceed the budget — if one row tips over, MAIN silently falls
  back to TOAST for that row and you get the worst of both worlds. Don't use
  `PLAIN` (oversize row → hard error).

What it buys vs. status quo (N bytes of blob):

- TOAST: N/1996 chunk tuples packed 4/page **plus** a toast-index probe per
  value (`toast_fetch_datum` runs an index scan on the toast table) **plus**
  a full reassembly memcpy into a palloc buffer.
- Chunked MAIN rows: N/7800 heap tuples, 1/page, fetched by PK index or TID
  range — no toast index exists, no second relation, and if the compression
  framing is made per-chunk (each row an independently decompressible LZ4
  frame), **no reassembly copy at all**: decompress straight out of the
  pinned buffer, which also composes perfectly with the read_stream prefetch
  from §1 and enables streaming/early-exit decompression.

Prior art:

- **pg_largeobject** is exactly this pattern in core: `LOBLKSIZE = BLCKSZ/4`
  (2 KB) data chunks as ordinary heap rows keyed `(loid, pageno)`
  ([docs](https://www.postgresql.org/docs/current/catalog-pg-largeobject.html)).
  Proves crash recovery, replication, VACUUM all just work; we'd use 4×
  bigger chunks.
- **Citus columnar / Hydra** are the *counter*-example: stripes in a custom
  page layout inside the relfilenode via table AM. pg_dump/pg_upgrade and WAL
  work, but **logical decoding does not**
  ([Citus columnar README](https://github.com/citusdata/citus/blob/main/src/backend/columnar/README.md),
  [Citus docs: integrations](https://docs.citusdata.com/en/stable/develop/integrations.html)).
  Our constraint (logical replication must work) is precisely why chunked
  *heap rows* — which logical decoding understands natively — are the right
  shape, not a custom smgr/TAM page format.
- **TimescaleDB** compressed chunks use the same BYTEA+TOAST scheme we do
  today, so this would be a genuine structural advantage over them.

Gotchas:

- **Page overhead:** 8192 − 24 (page header) − 4 (line pointer) − 8160 usable
  → ~0.4% loss; TOAST's 4×2032 packing wastes a similar amount. Wash.
- **fillfactor must stay 100** on the chunk table (append-only, no updates —
  true for our blob tables already).
- **WAL volume:** inserts log the same payload bytes either way; chunked rows
  log slightly *less* (no toast-index inserts). Full-page writes after
  checkpoint hit 8 KB pages in both layouts. No regression expected.
- **More rows in the main table** (N/7800 instead of 1 per blob): per-tuple
  SPI/visibility overhead per fetch. Mitigate by reading via TID/PK range and
  keeping per-segment-per-column chunk counts small (a 1 MB blob = ~128 rows).
- **VACUUM/ANALYZE** now see the data table itself as large; set per-table
  autovacuum/analyze scale factors (we control these tables).
- The 2 KB→7.8 KB granularity change interacts with the compressed-blob
  cache (BLOB_CACHE.md) and any future decoded-value caching — cache keys
  move from (segment, column) to (segment, column, chunk) or stay
  value-level with chunk-wise fill.

This is storage-v2-grade surgery; prototype on the blooms or text-lengths
table first (small, isolated read path) before touching the main blob table.

## 5. PG18 btree skip scan — when probes benefit

Commit `92fe23d9` et al. (Peter Geoghegan), in PG18
([release notes](https://www.postgresql.org/docs/18/release-18.html),
[-hackers thread](https://www.postgresql.org/message-id/CAH2-Wzmn1YsLzOGgjAQZdn1STSG_y8qP__vggTaPAYXJP+G4bw@mail.gmail.com),
[pgEdge writeup](https://www.pgedge.com/blog/postgres-18-skip-scan-breaking-free-from-the-left-most-index-limitation)).

Conditions for benefit:

- Multicolumn btree where the query omits (or has only an inequality on) one
  or more **leading** columns but has useful quals on later columns.
- The omitted prefix must be **low-cardinality** — the scan synthesizes an
  equality "skip array" per distinct prefix value, so cost ≈ ndistinct(prefix)
  × O(log N) descents. Tens-to-thousands of distinct values: great;
  millions: planner correctly won't pick it.
- Works for equality *and* range quals on later columns (MDAM-style), no
  index or DDL changes needed; PG17's ScalarArrayOp machinery is the basis.

pg_deltax angles:

- Blob/colstats/blooms PKs are `(_col_idx, _segment_id)`. Any probe by
  `_segment_id` alone (e.g. "all blobs for surviving segment S" after meta
  pruning) previously needed a second index or a seq scan; on PG18 it skip
  scans over `ndistinct(_col_idx)` ≤ ~105 prefixes — cheap. This may let us
  drop any segment-id-leading secondary indexes (smaller writes during
  compression) — verify with `EXPLAIN` on PG18 before removing anything.
- User-facing: queries on deltax tables filtering only on later columns of a
  user's multicolumn index benefit transparently; worth a line in perf docs.
- Reminder: PG17 (our default build) does not have it — gate any
  index-dropping decisions on PG version.

## Suggested next steps

1. Spike: FFI bindings for `read_stream_begin_relation` + a prewarm pass over
   the blob toast relation in Phase 2; benchmark cold-cache ClickBench Q20-Q28
   and RTABench on PG18 with `io_method=worker` and `io_uring`.
   **Update 2026-06-12:** a whole-relation streaming variant of this was
   prototyped and **reverted** — it read the entire blobs TOAST relation
   regardless of pruning and regressed cold queries catastrophically
   (e.g. Q23 cold 1.9 s → 50.5 s at 100M). Any retry must stream only the
   surviving blobs' TOAST chunk ranges (resolve `va_valueid` chunk locations
   via the toast index first); see the prefetch-v2 note in
   `NEXT_OPTIMIZATIONS.md`.
2. Audit `SET STORAGE` / column compression on all five companion tables
   (skip TOAST's redundant lz4 pass on already-compressed blobs).
3. Prototype §4 chunked-MAIN storage on the text-lengths table; measure
   detoast-vs-chunk-fetch on cold cache.
4. Re-run RTABench join queries on PG19 beta with `enable_eager_aggregate=on`.
