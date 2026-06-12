# Storage V2 — Extension-Managed Segment Files

Proposal to replace the TOAST-backed `<partition>_blobs` companion tables
with immutable, extension-managed segment files read via mmap, while
remaining a pure PostgreSQL extension (no forked server, no embedded
foreign engine). Meta, colstats, blooms, and text-length companion tables
stay as ordinary Postgres heaps; only the bulk compressed bytes move out.

Status: design. Nothing here is implemented.

## 1. Why — the detoast wall

Per-query TOAST chunk reassembly is the single largest remaining gap vs
ClickHouse on the 100M-row ClickBench (`QUERY_ANALYSIS.md` F3): ~12–15 s
cumulative warm detoast across 20+ DeltaXAgg queries, and the dominant
line item on JSONBench (Q1 2.3 s of 3.6 s) and text-heavy RTABench
queries. ClickHouse reads mmap'd (or page-cached) compressed files with
zero reassembly; we rebuild every blob from ~2 KB TOAST chunks through a
per-blob toast-id index probe, a chunk-reassembly loop, and a
`pg_detoast_datum → Vec<u8>` copy — on every query.

Every incremental fix has been measured and rejected or capped:

- `STORAGE MAIN` / `STORAGE EXTERNAL` on `_data` — net losses
  (`COLUMNAR_STORAGE.md` appendix, `BLOB_CACHE.md` "What we tried first").
- Cross-backend shmem caches for colstats/blobs (`shmem_query_cache`,
  commit 59b01fd) — "didn't win much"; the OS page cache already covers
  warm reads and LWLock overhead eats the rest (F3 note).
- Dict sidecar blobs (#45) — dicts are 63–91 % of dense text blobs, so
  the projected win evaporated.
- Pipelined detoast — landed, limited impact once the worker:detoast
  ratio exceeds ~6:1.
- The shared blob cache (`src/blob_cache/`) — landed, real wins on warm
  repeats (JSONBench Q4 −34 %), but it's a palliative: it caches the
  *output* of detoast, costs up to 4 GiB of shmem, does nothing for cold
  runs, and re-pays the full cost whenever the working set exceeds the cap.

The conclusion in `QUERY_ANALYSIS.md` stands: further gains need "a
different storage layout for TOAST" — i.e. stop using TOAST for bulk
bytes entirely.

### Where the cost lives today (read path)

All in `src/scan/exec/segments.rs`:

- **Phase 2 of `load_segments_heap()`** (~line 3400): per needed column,
  a B-tree index scan on the blobs PK (`_col_idx = N`), then per row
  either an immediate detoast or a deferred TOAST-pointer copy.
- **`fetch_segment_blobs()`** (~line 3830): the on-claim variant used by
  the parallel paths — a two-key PK probe per (column, segment), then
  `detoast_varlena_to_vec()`. Checks the blob cache first.
- **`detoast_lazy_blobs()` / `detoast_lazy_blobs_selective()`**
  (~line 3993): materialize deferred TOAST pointers; same
  cache-then-detoast dance via `detoast_blob_slot()`.

Per blob the price is: PK index descent + heap fetch (or toast-id index
probe per ~2 KB chunk for out-of-line values) + chunk reassembly +
varlena copy into a Rust `Vec<u8>`. ClickBench has ~3338 segments × up
to 105 columns; a 3-column query touches ~10 K blobs, each paying this
in full. Detoast must also run on the leader (or at least in a backend
with a valid snapshot), which serializes it against the Rust worker
threads.

## 2. Proposed design

### One segment file per partition

At compress time, instead of inserting into `<partition>_blobs`, write
all compressed column blobs for the partition into a single immutable
file:

```
$PGDATA/pg_deltax/<database_oid>/<partition_relfilenode>_<generation>.dxs
```

- **`database_oid`** namespaces per database, mirroring `base/`.
- **`partition_relfilenode`** ties the file to the partition relation.
- **`generation`** is a monotonically increasing counter (epoch micros
  is fine) so recompression never reuses a name — the same trick the
  blob cache uses with companion OIDs for free invalidation.
- Files live **inside `$PGDATA`** deliberately: `pg_basebackup` copies
  unknown files in the data directory, so base backups pick them up with
  no extra tooling (see §4 for the streaming-replication caveat).

One file per partition (not per column) keeps fd counts and GC simple:
ClickBench is 18 files instead of 18 × 105. Within the file, blobs are
written **column-major** — all segments of column 0, then column 1, … —
preserving the sequential-read property the current blob table gets from
column-major insertion order (`COLUMNAR_STORAGE.md`). A per
(partition, column-group) split is a possible later refinement if
single files ever exceed practical sizes (ClickBench partitions are
~2.8 GB compressed; well within range), but it is not part of P1.

### On-disk format

Write-once, streamed, index-at-end (footer) so compression can write
blobs as they're produced without knowing offsets up front:

```
┌────────────────────────────────────────────────────────────────┐
│ Header (64 bytes, fixed)                                       │
│   magic        "DXSEG\0"          6 B                          │
│   version      u16                = 1                          │
│   flags        u32                (reserved)                   │
│   partition_id u32                deltax_partition.id          │
│   n_columns    u32                                             │
│   n_segments   u32                                             │
│   index_offset u64                byte offset of blob index    │
│   index_len    u64                                             │
│   header_crc   u32                CRC32C of bytes above        │
│   (pad to 64)                                                  │
├────────────────────────────────────────────────────────────────┤
│ Blob data (column-major)                                       │
│   col 0: seg 1 blob | seg 2 blob | … | seg N blob              │
│   col 1: seg 1 blob | …                                        │
│   …                                                            │
│   Each blob is the exact bytes that go into                    │
│   CompressedColumn::from_bytes today — codec tag, row count,   │
│   null bitmap, payload. No re-framing.                         │
│   Each blob is 64-byte aligned (pad with zeros) so decoders    │
│   that want aligned SIMD loads can have them.                  │
├────────────────────────────────────────────────────────────────┤
│ Blob index (at index_offset)                                   │
│   entry per (col_idx, segment_id), sorted by (col_idx, seg_id):│
│     col_idx     u16                                            │
│     segment_id  u32                                            │
│     offset      u64                                            │
│     length      u32                                            │
│     checksum    u32   (CRC32C of the blob bytes)               │
│   22 B/entry packed → ~7.7 MB for 105 × 3338; typical          │
│   time-series tables are KBs.                                  │
├────────────────────────────────────────────────────────────────┤
│ Footer (16 bytes)                                              │
│   index_crc    u32    CRC32C of the index                      │
│   file_len     u64    total file length (truncation detector)  │
│   magic        "DXSE"                                          │
└────────────────────────────────────────────────────────────────┘
```

Notes:

- **Lookup** is a binary search in the (mmap'd) index — no B-tree, no
  heap tuple, no varlena. The index for a partition is read once per
  scan and is small enough to keep mapped.
- **Checksums** are per-blob CRC32C (hardware-accelerated), verified on
  first touch of each blob, controllable via
  `pg_deltax.verify_file_checksums` (default `on`; cheap relative to
  decompression). The header/footer CRCs make open-time validation O(1).
- **Version** field gates future layout changes; readers reject unknown
  versions with a hint to recompress.
- Missing blobs (columns added after compression, all-null columns) are
  simply absent from the index — same semantics as a missing blobs-table
  row today (`fetch_segment_blobs` already tolerates absent rows).

### What stays in Postgres heaps

Everything that is small, randomly accessed, or load-bearing for
planning stays exactly where it is: `<partition>_meta`,
`<partition>_colstats`, `<partition>_blooms`,
`<partition>_text_lengths`, `<partition>_valbitmap`, and both catalog
tables (`deltax.deltax_deltatable`, `deltax.deltax_partition`). Phase 1
pruning, colstats pushdown, bloom pruning, and the planner-stats
machinery are untouched. Segment ids remain the meta-table SERIAL; the
file index is keyed by the same `(col_idx, segment_id)` pairs the blobs
table uses today, so `SegmentData` and the decode layer don't change
shape.

Text-length sidecars and blooms *could* move into the file later (they
are also write-once bytes), but they are small, their detoast cost is
already near-zero by design, and keeping P1's blast radius minimal
matters more.

### Catalog changes

```sql
ALTER TABLE deltax.deltax_partition
    ADD COLUMN IF NOT EXISTS blob_storage  TEXT,     -- 'toast' | 'file', NULL = 'toast'
    ADD COLUMN IF NOT EXISTS blob_file     TEXT;     -- relative path under pg_deltax/
```

`NULL`/`'toast'` means the existing `<partition>_blobs` table is
authoritative — every already-compressed partition keeps working with
zero migration.

## 3. Crash safety and durability

The key property making this tractable: **segment files are immutable
and their content is reproducible** until the moment the catalog commit
makes them authoritative. During compression the uncompressed rows still
exist in the partition heap (the `TRUNCATE` happens in the same
transaction that records the compression); if anything crashes before
commit, the source of truth is still the heap.

Write protocol at compress time:

1. Write blobs + index + footer to
   `pg_deltax/<db>/<relfilenode>_<gen>.dxs.tmp`.
2. `fsync` the file, `rename` to final name, `fsync` the directory.
3. In the (already open) compression transaction: insert meta/colstats/
   blooms rows as today, set `blob_storage='file'`, `blob_file=<path>`
   on the `deltax_partition` row, `TRUNCATE` the partition.
4. Commit.

Crash analysis:

- **Crash before commit**: catalog never references the file; the
  partition is still uncompressed and intact. The leftover `.tmp` or
  final-named file is an orphan.
- **Crash after commit**: file was fsynced *before* the commit record,
  so the committed catalog row always points at a durable, complete
  file. The footer's `file_len` + CRC detects the (should-be-impossible)
  torn case and fails the scan with a "recompress this partition" error
  rather than returning garbage.
- **Orphan GC**: the background worker (`src/worker.rs` main loop)
  gains a sweep step — list `pg_deltax/<db>/`, drop any file not
  referenced by a `deltax_partition.blob_file` and older than a grace
  period (e.g. 1 h, to avoid racing in-flight compressions). This also
  covers files left behind by `DROP TABLE` of a whole deltatable done
  while the worker was down.

**No WAL for file content.** This is the point of the design: the bytes
never pass through WAL, shared_buffers, or TOAST. WAL still covers
everything transactional (meta rows, catalog flag, TRUNCATE), so
crash-recovery replays to a state where the catalog and the fsynced file
agree.

**`synchronous_commit` interplay**: none, by construction. The file
fsync happens before the commit is even requested, so the file is
durable regardless of `synchronous_commit` setting. With
`synchronous_commit=off` a crash can lose the *commit* (partition
appears uncompressed, file is an orphan → GC'd) but can never expose a
committed reference to a missing file.

**Base backups**: `pg_basebackup` copies unrecognized files under
`$PGDATA`, so a base backup taken after the commit contains the file. A
backup whose snapshot lands mid-compression contains either an orphan
(harmless, GC'd on the restored cluster) or a complete file. The one
genuinely unsupported flow is exclusion-list-based custom backup tools
that skip unknown directories — documented as "recover by recompress"
(§4c).

## 4. Replication and backup story

Physical streaming replication ships WAL; segment files are not in WAL,
so **standbys will not receive them**. A standby created from a base
backup has the files as of backup time but misses any compressed later.
Options considered:

- **(a) Logical replication as the supported story.** Already true in
  spirit: `tests/test_logical_replication.py` exists, documents that
  publications must exclude TRUNCATE, and the subscriber compresses
  independently — each side owns its own companion tables, and under
  this proposal, its own segment files. Nothing changes for logical
  replication; it never replicated companion internals anyway when the
  publication targets the parent with `publish_via_partition_root`.
- **(b) Double-write mode.** Write blobs to *both* the file and the
  TOAST blobs table. Physical standbys read the TOAST copy (they run the
  same extension code; the scan branches on `blob_storage`, and a
  standby-side check falls back to TOAST when the file is absent). Costs
  2× compressed storage and the load-time TOAST overhead, gains full
  physical-replica compatibility. Worth keeping as an escape hatch, not
  as the default.
- **(c) Recovery-by-recompress.** Document that a physical standby /
  restored backup missing files can be repaired: the meta tables say
  which partitions claim `blob_storage='file'`; a standby promoted to
  primary cannot recompress from the (truncated) heap, so (c) only works
  for *backup* flows where the original heap data is restorable, or as
  "decompress is impossible, re-ingest". This limitation is why (a) is
  the headline answer for HA.

**Recommendation: (a) + (c), with a compatibility GUC.**

```
pg_deltax.blob_storage = 'file' | 'toast' | 'dual'   (default 'toast' in P1)
```

- `toast` — current behaviour, full physical-replication compatibility.
- `file` — segment files; documented as requiring logical replication
  (or no replication) and base-backup-style backups.
- `dual` — option (b) for users mid-migration or pinned to physical
  standbys.

The GUC controls what *new* compressions produce; reads always follow
the per-partition catalog flag. A hot-standby backend that encounters
`blob_storage='file'` with no file errors out with a clear message
pointing at the GUC and the docs.

## 5. Read path changes

Replace Phase 2 / `fetch_segment_blobs` blob-table machinery with:

1. **Open + mmap** the partition's file once per scan (first touch),
   keyed off `deltax_partition.blob_file`. Validate header/footer, mmap
   the whole file (`memmap2` crate or direct `mmap(2)`), binary-search
   the index.
2. **`BlobBytes::Cached { data, len }` already exists** — the blob-cache
   to_vec() elimination (BLOB_CACHE.md Phase 5) made
   `SegmentData.compressed_blobs` hold borrowed `*const u8 + len`
   views with a lifetime guarantor dropped after them. File-backed blobs
   reuse exactly this: the mmap handle plays the role of the
   `BlobCachePin`. Rename the guarantor concept to `BlobBacking`
   (enum: cache pin | mmap handle) — `SegmentData` field order already
   enforces drop order.
3. **No detoast, no copy, no leader serialization.** A blob "fetch" is
   pointer arithmetic; `detoast_lazy_blobs` for file-backed partitions
   collapses to "record offset+len". Cold cost becomes page faults
   served by kernel readahead over a column-major contiguous region —
   the same I/O pattern Phase 2 was engineered to approximate through
   TOAST insertion order, now guaranteed by construction.
4. **Parallel paths**: the Rust worker threads need `Send`-able byte
   slices. An mmap'd `&[u8]` is naturally shareable across threads; the
   scan state holds an `Arc<Mmap>` per partition, segments hold raw
   (ptr, len) views into it, and the existing drop-order discipline pins
   the mapping for the scan lifetime. This *removes* today's constraint
   that detoast must run on the leader before `std::thread::scope`
   dispatch — workers can fault pages in parallel, which is the part of
   the I/O TOAST could never parallelize.
5. **Blob cache becomes unnecessary for file-backed partitions.** Its
   entire purpose is to amortize detoast; with mmap there is no detoast
   and the OS page cache *is* the warm-read cache, with no 4 GiB cap, no
   LWLocks, no eviction logic, and shared across backends for free. The
   cache stays for TOAST-backed partitions and is simply skipped when
   `blob_storage='file'` (the `get_pinned`/`insert` calls in
   `detoast_blob_slot` / `fetch_segment_blobs` sit behind the same
   branch).

EXPLAIN keeps the timing counters; `detoast` time becomes `blob_map`
time (open + index search + checksum on first touch) so before/after is
measurable with the existing tooling.

## 6. Tiering and migration

- **Per-partition opt-in.** The unit of storage choice is the partition,
  recorded in `deltax_partition.blob_storage`. Old partitions stay
  TOAST-backed forever if never recompressed; the scan layer branches
  per partition, so a single query over a mixed table works (DeltaXAppend
  already handles per-partition companion state).
- **Migration** = decompress + recompress under the new GUC, or a
  dedicated `deltax_migrate_partition_storage(partition)` that rewrites
  blobs-table → file without a full decode cycle (straight copy of blob
  bytes; the framing is identical). The latter is cheap and the obvious
  v2 convenience function, but P1 can ship with recompress-only.
- **`deltax_decompress_partition`** (`src/compress.rs`,
  `decompress_partition_inner`) currently drops the five companion
  tables; it must additionally `unlink` the segment file and clear
  `blob_storage`/`blob_file`. Same for recompression's
  drop-and-recreate path (`compress.rs` ~line 933).
- **Retention drops** (`auto_drop_partitions`) and user `DROP TABLE`
  must unlink too. DDL-event coverage can't be perfect (a raw `DROP
  TABLE` of the partition bypasses us — the ProcessUtility hook catches
  the common paths), which is the second reason the worker GC sweep
  exists: any file whose catalog row is gone gets collected within one
  worker cycle + grace period.
- **`pg_upgrade`** does not copy unknown files in the old cluster's data
  dir. Document a required step: copy `$OLDPGDATA/pg_deltax/` to the new
  cluster (paths are relfilenode-based; relfilenodes are preserved by
  pg_upgrade's link/copy modes for user tables, but verify per-version —
  if not preserved, key files by `deltax_partition.id` instead of
  relfilenode, which is stable across upgrade since the catalog rows are
  dumped/restored. **Decision: key by partition id + generation, not
  relfilenode**, precisely to make pg_upgrade a plain directory copy).

## 7. Projected wins and risks

### Wins

From F3's warm detoast table, the directly removable cost (detoast →
~0; replaced by page-cache reads measured in tens of ms warm):

| Query | detoast (ms) | total (ms) | projected total |
|-------|--------------|------------|-----------------|
| Q22   | 2,391        | 3,621      | ~1,300          |
| Q20   | 2,182        | 6,725*     | ~4,500 (*pre-#49; post-#49 ~1,020 → ~400) |
| Q32   | 2,088        | 9,478      | ~7,400 (merge-bound after) |
| Q21   | 1,351        | 1,947      | ~650            |
| Q28   | 1,186        | 6,612      | ~5,400          |
| Q33   | 1,058        | 2,459      | ~1,400          |
| Q34   | 1,053        | 2,478      | ~1,400          |
| Q18   | 1,023        | 3,294      | ~2,300          |
| Q9    |   911        | 1,136      | ~250            |
| Q31   |   769        | 1,619      | ~850            |

Roughly **−12–15 s on the warm bench total (~59 s)**, concentrated
exactly on the queries still behind ClickHouse, plus larger cold-run
wins (cold detoast was 86 % median of total in the original layout
measurements) and the removal of the leader-side detoast serialization
for parallel agg. Secondary wins: load time (no TOAST chunk writes, no
WAL for blob bytes — compression currently WALs every blob), and
freeing the blob cache's up-to-4 GiB shmem on file-backed workloads.

These are projections; P1's exit criterion is measuring them, not
assuming them.

### Risks

- **fd / mapping counts**: one fd + one mapping per (scan, file-backed
  partition). Hundreds of partitions per query is fine; thousands needs
  an fd-cache with LRU close. Bounded and known.
- **NFS / exotic filesystems**: mmap coherence and fsync semantics on
  NFS are historically dodgy; document "local filesystem required for
  `blob_storage=file`", same stance as Postgres itself takes for
  reliable operation.
- **SIGBUS on truncated files**: a reader faulting past EOF (file
  corrupted/truncated outside our control) gets SIGBUS, which Postgres
  does not gracefully handle in extension code. Mitigation: open-time
  `file_len` check from the footer + stat, and per-blob bounds checks
  against the validated index before any dereference. Residual risk
  (concurrent external truncation) is the same class as someone deleting
  a relation segment file under Postgres.
- **Windows**: not applicable — pgrx/pg_deltax doesn't target Windows;
  state it explicitly in docs.
- **ENOSPC at compress time**: fails before commit, partition stays
  uncompressed, `.tmp` is GC'd. Clean.
- **Physical-standby users**: the real adoption risk. Mitigated by
  default-`toast`, the `dual` mode, and loud documentation.
- **Backup tooling diversity**: pgBackRest/WAL-G handle unknown PGDATA
  files differently (pgBackRest includes them; verify WAL-G). Needs a
  documented support matrix in P2.

## 8. Implementation phasing

**P1 — read/write path behind a GUC (default `toast`), correctness.**
- File writer in compression path (`compress.rs`): tmp + fsync + rename
  + catalog columns.
- Reader: mmap open/validate, index lookup, `BlobBytes` integration in
  `segments.rs` (Phase 2 branch, `fetch_segment_blobs`,
  `detoast_lazy_blobs*`), `Arc<Mmap>` lifetime on scan state.
- Decompress/drop unlink the file.
- Tests: full integration suite parameterized over
  `blob_storage ∈ {toast, file}`; mixed-storage table test; crash-window
  test (kill between file rename and commit → orphan, partition intact);
  checksum-corruption test (flip a byte → clean error).
- Exit: local + EC2 ClickBench A/B with the F3 queries.
- **Exit measurement (2026-06-12, c6a.4xlarge, 100M ClickBench).**
  Q28-shape sort query confined to `hits_p20130702`, recompressed under
  `blob_storage=dual`, fully cold (PG restart + `drop_caches=3`) per run:
  mmap reads via `.dxs` **894 / 892 ms**; same data with the file
  renamed away (TOAST fallback path) **3966 / 3958 ms** — a **4.4×
  cold-read win** for the file-backed path, and a live validation that
  fail-open to TOAST works (the hidden-file runs completed correctly).
  Verdict: **keep** (`dual` stays opt-in via GUC; default unchanged
  at `toast`).

**P2 — GC, replication, backup hardening.**
- Worker orphan sweep with grace period; `pg_deltax_storage_files()`
  diagnostic SRF (path, size, referenced, partition).
- `dual` mode; hot-standby fallback + error messages.
- Backup tool matrix (pg_basebackup, pgBackRest, WAL-G); pg_upgrade
  copy-step docs + a check function; logical-replication test extended
  to file-backed subscriber.
- `deltax_migrate_partition_storage()` blob-copy migration.

**P3 — default-on.**
- Flip default to `file` once P2 soak (all three benches + correctness
  suites + at least one real workload) is clean.
- Decide blob cache fate: keep for `toast` partitions only, or schedule
  removal with TOAST-storage deprecation. No earlier than one release
  after default flip.

## Decision log

- **One file per partition, column-major interior** — fd/GC simplicity;
  matches today's physical layout intent; per-column-group split
  deferred until a concrete size/parallelism need appears.
- **Index-at-footer, blobs unmodified** — streaming write; reuses
  `CompressedColumn::from_bytes` framing byte-for-byte, so the decode
  layer is untouched.
- **Files inside `$PGDATA`, keyed by partition id + generation** —
  base-backup inclusion for free; pg_upgrade becomes a directory copy;
  generation suffix gives free invalidation on recompress.
- **fsync-before-commit, no WAL for content, worker GC for orphans** —
  immutability + reproducibility-until-commit make this sound without
  any recovery-time hook.
- **Logical replication + recompress as the supported story; `dual`
  double-write as the physical-standby escape hatch** — physical
  replication of extension files is impossible without forking the
  server, which is out of scope by definition.
