# Blob cache — shared-memory cache for detoasted compressed blobs

> **Status: DSA approach working end-to-end (2026-05-13).**
> Postmaster creates the in-place DSA, backends attach lazily, three
> scans round-trip cleanly with hits incrementing. Two bugs that
> previously made this look like a dead end are documented in
> [DSA: what bit us, and the fix](#dsa-what-bit-us-and-the-fix) below
> for future readers; both are gotchas in how PG 17's DSA contract
> interacts with extension shmem hooks, not bugs in pgrx or PG.

## DSA: what bit us, and the fix

The earlier write-up of this section called for abandoning DSA and
hand-rolling a size-class allocator. That was wrong. Two distinct
bugs in our DSA setup looked like one fatal "DSA can't work here"
problem; once both were named they were one-line fixes each. We're
keeping DSA.

**Bug 1 — `dsa_create_in_place_ext(place, size, tranche, NULL, 0, 0)`
violates the DSA contract.** PG 17 exposes only the 6-arg `_ext`
form; the 4-arg `dsa_create_in_place` is a macro (`dsa.h:122-125`)
that fills in `DSA_DEFAULT_INIT_SEGMENT_SIZE` (1 MB) and
`DSA_MAX_SEGMENT_SIZE` for the segment-size knobs. `create_internal`
asserts `init_segment_size >= DSA_MIN_SEGMENT_SIZE` (`dsa.c:1233`),
but in a release build the assertion is stripped — `create_internal`
silently writes `control->init_segment_size = 0;
control->max_segment_size = 0;` into shmem (`dsa.c:1268-1269`), and a
subsequent `dsa_allocate_extended` walks into garbage when
`make_new_segment` multiplies through those zeros (`dsa.c:2125-2127`).
The gdb dump where `area->control` appeared to point into the backend
heap was the post-corruption state, not a binding mismatch — the
binding is fine.

Fix: pass the documented defaults explicitly and cap actual usage
with `dsa_set_size_limit` after create, the way `pgstat_shmem.c`
(`pgstat_shmem.c:163-196`) does it. Specifically: in
`shmem_startup_hook`, after `dsa_create_in_place_ext(..., DSA_DEFAULT_INIT_SEGMENT_SIZE,
DSA_MAX_SEGMENT_SIZE)`, call `dsa_pin` → `dsa_set_size_limit(area,
dsa_size)` → `dsa_detach(area)`. The pin keeps the in-shmem control
block alive across all-backends-detached; the limit caps total
allocations to the in-place chunk so the DSA never tries to allocate
a fresh DSM segment (which is exactly what blew up the "lazy
create-from-first-backend" attempt under Docker Desktop tmpfs); the
detach releases the postmaster-local `dsa_area*` since backends build
their own via `dsa_attach_in_place`.

**Bug 2 — `dsa_attach_in_place` palloc's the backend-local
`dsa_area*` in `CurrentMemoryContext`.** Our backend `attach()` ran
lazily from inside `get_pinned`/`insert`, i.e. inside whatever
memory context the executor had active (typically a per-portal or
per-transaction context). End-of-transaction destroys that context,
freeing the `dsa_area` struct underneath us. The next query's
`get_pinned` reads the stale `dsa_area*` from our process-local
`OnceLock`, dereferences a freed `area->control`, and segfaults. This
is the bug that masqueraded as "second-scan crash" even with the
size-knob fix in place.

Fix: switch to `TopMemoryContext` around the `dsa_attach_in_place` +
`dsa_pin_mapping` pair, again mirroring `pgstat_shmem.c:225-234`.
`dsa_pin_mapping` makes the *segment mappings* survive resource-owner
release, but the `dsa_area` struct itself lives in whatever context
was current when `palloc` ran — so the context switch is the missing
half.

Together those changes fit in `src/blob_cache/storage.rs`:

- Add module-local constants `DSA_DEFAULT_INIT_SEGMENT_SIZE = 1 <<
  20` and `DSA_MAX_SEGMENT_SIZE = 1usize << 40` (pgrx doesn't bind
  `#define` macros).
- In `dsa_create_in_place_compat`, pass those constants instead of
  `0, 0`.
- In `my_shmem_startup_hook`, after a successful create: `dsa_pin` →
  `dsa_set_size_limit(area, dsa_size)` → `dsa_detach(area)`.
- In `attach()`, wrap `dsa_attach_in_place` + `dsa_pin_mapping` with
  `MemoryContextSwitchTo(TopMemoryContext)` / back.

## Implementation status

### Done (2026-05-13)

- GUCs `pg_deltax.blob_cache_mb` (default `1024`) and
  `pg_deltax.blob_cache_shards` (default `64`) registered in `_PG_init`.
  Context is `Postmaster` for both since the shmem reservation is
  decided at startup.
- Module `blob_cache` (`src/blob_cache/mod.rs`, `src/blob_cache/storage.rs`).
  Public API in `mod.rs`: `BlobCacheKey`, `BlobCachePin` (Drop-based pin
  release), `BlobCacheStats`, `get_pinned`, `insert`, `stats`,
  `register_hooks`, `configured_bytes`, `configured_shards`. All
  `unsafe` / raw PG-binding code is confined to `storage.rs`.
- Integration site wired: `detoast_lazy_blobs` and
  `detoast_lazy_blobs_selective` in `src/scan/exec/segments.rs` now try
  the cache first, fall back to `pg_detoast_datum`, and insert on miss.
  Both functions return a new `DetoastLazyStats` (cache_hits /
  cache_misses / cache_bytes_served) for EXPLAIN aggregation.
- `SegmentData` carries a `cached_blob_pins: Vec<BlobCachePin>`. Pins
  live until `end_custom_scan` drops `DecompressState`, guaranteeing the
  DSA bytes outlive every read of `compressed_blobs` (including
  worker-thread reads, since detoast runs on the leader before
  `std::thread::scope` dispatch).
- Build green on PG 17 (default). `make clippy` clean on new code.
  All 382 pgrx unit tests still pass.
- DSA create + attach working end-to-end (2026-05-13). Smoke test
  (`SELECT count(*), sum(length(body))` twice over a 5K-row table of
  32 KB rows compressed by `deltax_compress_partition`) shows
  `misses_total=1`, `hits_total=1` after the second scan, no
  crashes. Root causes were the two DSA gotchas documented in [DSA:
  what bit us, and the fix](#dsa-what-bit-us-and-the-fix).
- `DSA_ALLOC_NO_OOM` flag on every `dsa_allocate_extended` call
  (2026-05-13). Without it, DSA throws `ereport(ERROR)` on
  out-of-space ("Failed on DSA request of size 56") and kills the
  query. With it, insert just returns gracefully — letting eviction
  or the insert-failure counter take over.
- **Per-shard LRU eviction (2026-05-13).** Each `Shard` carries
  `lru_head` / `lru_tail` dsa_pointers; each `Entry` has
  `lru_prev` / `lru_next` pointers. Inserts prepend at the head;
  evictions walk from the tail skipping pinned entries (`pin_count >
  0`). Two eviction triggers: (a) `total_bytes + needed > max_bytes`
  (soft cap), (b) `dsa_allocate_extended` returning 0 (DSA itself
  is out of space — happens before soft cap because of DSA's
  per-page overhead). On either trigger, `insert` evicts and retries
  in a loop until both allocations succeed or there's nothing left
  to evict locally. Verified end-to-end: 50 random-bytes tables
  (~5 MB blobs total) against a 4 MB cache + 1 shard → 91 evictions,
  0 insert failures.
- **EXPLAIN ANALYZE shows per-query cache stats (2026-05-14).**
  Both `DeltaXAgg` and `DeltaXDecompress` custom scan nodes now
  emit a `DeltaX Blob Cache: hits=H misses=M bytes_served=B` line
  whenever `H + M > 0`. The counters live on `ScanTiming` and on
  `ScanTimingShmem` so parallel workers fold their per-process
  counts into the leader's total via the existing
  `flush_timing_to_shmem` path. `DetoastLazyStats` returned by
  `detoast_lazy_blobs` and `detoast_lazy_blobs_selective` is now
  consumed at every call site (10 in `decompress.rs`, 10 in
  `agg.rs`). Example output on a warm 2nd scan of a 30k-row text
  table: `DeltaX Blob Cache: hits=1 misses=0 bytes_served=1263144`.
- **`to_vec()` elimination on cache hits (Phase 5, 2026-05-14).**
  `SegmentData::compressed_blobs` switched from `Vec<Vec<u8>>` to
  `Vec<BlobBytes>`, a small enum: `Owned(Vec<u8>)` for freshly
  detoasted blobs, `Cached { data: *const u8, len: u32 }` for
  cache hits. `Cached` borrows directly from the corresponding
  `BlobCachePin` in `cached_blob_pins`; no memcpy on the hit path.
  Safe because `compressed_blobs` is declared before
  `cached_blob_pins` in the struct, so Rust drops the raw pointers
  before the pins release the cache entries. `BlobBytes:
  Deref<Target = [u8]>` so existing consumers (`&[u8]` consumers)
  needed no changes. Measured warm-scan detoast cost on a ~1.2 MB
  blob dropped from 0.082 ms → 0.006 ms (~13×); MB-scale blobs on
  JSONBench / RTABench will save proportionally more.
- **Per-shard diagnostic SRF (2026-05-14).**
  `pg_deltax_blob_cache_shard_stats()` walks every shard's LRU list
  under shared lock and returns `(shard_id, n_entries, bytes_used,
  lru_walk_count, pinned_count, unpinned_count, lru_head_dp,
  lru_tail_dp)`. Added to debug a stuck-cache scenario where the
  bench showed `evictions_total=0` alongside 895k insert failures;
  the SRF immediately confirmed LRU invariants were intact and
  redirected the investigation to sizing instead.
- **Cache sizing validated (2026-05-14).** Running JSONBench on
  m6i.8xlarge with `pg_deltax.blob_cache_mb` bumped from 1024 to
  8192: hit ratio jumped from 52% → 91%, evictions and
  insert_failures both went to 0, working set fit at 2.3 GB / 8 GB
  cap. Warm-run wins: Q4 2.30s → 1.52s (−34%), Q5 2.45s → 1.77s
  (−28%). Q1/Q2 marginal because they were already detoast-light.
  See [Sizing](#sizing) for the operational story.
- **Auto-sized default (2026-05-14).** `pg_deltax.blob_cache_mb = -1`
  (the new default) reads `MemTotal` from `/proc/meminfo` at
  postmaster start and resolves to `clamp(MemTotal / 4, 256 MiB,
  4096 MiB)`. Falls back to the floor on non-Linux hosts. Resolved
  value is observable via `pg_deltax_blob_cache_stats().bytes_max`.
- **Auto heuristic retuned: cap 4 → 16 GiB, fraction RAM/4 → RAM/6
  (2026-06-10).** On the ClickBench c6a.4xlarge (32 GiB), the 4 GiB
  cap left the URL/Title working sets thrashing: warm Q22 ran at a
  42% miss rate with detoast=2.8 s. With auto now resolving to
  ~5.2 GiB on that box, warm detoast drops to ~0 across the
  text-heavy queries — Q20 2.00 → 1.12 s, Q22 4.16 → 2.75 s, both at
  100% hit rate. The fraction dropped from 1/4 to 1/6 after a first
  iteration at 1/4 (7.8 GiB) OOM-killed ClickBench Q32 in no-restart
  sessions — see `PERF_IMPROVEMENTS.md` #50/#51 for the full story.
- **Integration tests (2026-05-14).** `tests/test_blob_cache.py` —
  6 tests covering: (1) the auto-sized default resolves to a
  non-zero cap, (2) cold scans populate misses + entries, (3) warm
  scans produce hits dominantly, (4) two scans return bytewise-
  identical results (validates the `BlobBytes::Cached` borrow
  path), (5) no entries are pinned between queries (`pin_count == 0`
  across all shards after a scan), (6) `EXPLAIN ANALYZE` surfaces
  the `DeltaX Blob Cache: hits=…` line. Skips cache-on-vs-off parity
  because `blob_cache_mb` is Postmaster-context — manual EC2
  validation covered that already (37/43 ClickBench queries
  hash-identical; 6 differ only in tie-breaking).

### Remaining

1. **Neighbour-shard eviction fallback (v2).** With the default
   64 shards and a hash-distributed key, a target shard can be empty
   when eviction is needed; `insert` then bumps
   `insert_failures_total` (~38 out of 50 attempts in a stress test
   on 4 MB / 64 shards). In practice this only matters when the
   cache is significantly smaller than the working set; the
   auto-sized default + the 16 GiB cap make it a corner case. If a
   workload ever hits it, try shards `±1, ±2, ...` with
   `LWLockConditionalAcquire` (avoids deadlock without strict lock
   ordering).
2. **`dshash` substitute already in place.** pgrx 0.17 doesn't
   expose `dshash_*` so the implementation uses a custom per-shard
   fixed-bucket hashmap (`BUCKETS_PER_SHARD = 256`, separate chaining
   through `Entry::bucket_next`). Acceptable for the working set
   sizes we have; reconsider if buckets get long under churn. (Not
   a remaining task — kept here as a note for future readers.)

### Codebase findings that simplify the original plan

- **`companion_oid` is already on `SegmentData`**, set in
  `begin_deltax_append` before any detoast happens. The original
  proposal threaded it through `detoast_lazy_blobs(seg, companion_oid)`;
  in practice we just read `seg.companion_oid`. No signature change at
  call sites.
- **Workers do not call `detoast_lazy_blobs`** — detoast runs on the
  leader before `std::thread::scope` worker dispatch (in
  `load_next_segment` and the Top-N paths in
  `src/scan/exec/decompress.rs`). Workers consume the already-populated
  `seg.compressed_blobs`. So the cache lookup/insert and pin lifetime
  live entirely on the leader; no cross-thread pin bookkeeping needed.
- **No prior shmem hooks** in pg_deltax — we're the first user, so
  registration is a plain installation rather than chaining.

## Why

On JSONBench warm runs every query spends ~2.2s in detoast (chunk
reassembly + varlena reconstruction + memcpy into Rust `Vec<u8>`), and PG
throws all that work away at end-of-query. The next query does it again.

Measured on m6i.8xlarge, 100M-row bluesky dataset:

| Query | Total warm | of which detoast |
|---|---:|---:|
| Q1 | 3.56s | 2.30s (65%) |
| Q3 | 2.74s | 2.41s (88%) |
| Q4 | 4.37s | 2.35s (54%) |

The detoast cost is also the dominant single line item on RTABench
text-heavy queries (Q17/Q25-class) and ClickBench string queries. A
process-shared cache that lets repeated queries skip detoast on the same
segment-column blob is the single biggest lever remaining.

This is essentially what ClickHouse's "uncompressed cache" + "mark cache"
does. For an OLAP engine answering many queries against a slowly-changing
column store, it's the load-bearing optimisation.

## What we tried first, and why it didn't move the needle

**`STORAGE EXTERNAL` on the `_data` column** (rejected, 2026-05).
Hypothesis: pg_deltax's blobs are already LZ4-blocked compressed, PG's
secondary lz4 pass is wasted CPU. Switching to EXTERNAL skips it.

Result on JSONBench warm: ~340ms total improvement (3%). DB size didn't
change — PG was already deciding not to compress the LZ4-blocked input,
so EXTERNAL only saved the cost of the "is this compressed?" header
check. **On ClickBench it regressed total relative time from ×2.4 to
×3.08 vs ClickHouse**, presumably because the larger inline-skip
threshold (EXTERNAL forces external for anything > target) shifted
some small blobs from inline to toast. Reverted.

The detoast cost we're chasing isn't in PG's compression layer. It's in
the per-chunk reassembly, the per-blob toast-id index probe, and the
`pg_detoast_datum → Vec<u8>` copy in `detoast_lazy_blobs`. Caching the
finished result is the way to bypass all of it.

## Scope

**In scope:**
- Process-shared cache, sized by a `pg_deltax.blob_cache_mb` GUC.
- Cache **detoasted compressed bytes** keyed by
  `(companion_oid, segment_id, col_idx)`. Workers consume the cached
  bytes the same way they consume freshly-detoasted ones.
- LRU eviction with size accounting in bytes (not entries).
- Sharded LWLocks for read/write concurrency.
- EXPLAIN-visible per-query stats (hits, misses, bytes served from cache).
- GUC `pg_deltax.blob_cache_mb` (default `0` = disabled; recommended
  production default `1024-4096`).

**Out of scope (deliberately):**
- Caching *decompressed* column data. We considered it — saves another
  ~150ms of decompress on top of detoast. But: 10× the memory cost,
  correctness questions when encoding schemes change, harder to size in
  practice. Defer until the compressed-blob cache is proven and tuned.
- Cross-restart persistence. Cache is in DSM, lost on PG restart. The
  steady-state warmup is fast — first query of a workload re-populates.
- Negative caching ("this segment has no blob for this col_idx"). Skip
  with the existing in-tree empty-blob check.

## Layer choice — cache compressed, not decompressed

| Layer | Hit saves | Memory cost (JSONBench Q1 working set) | Complexity |
|---|---|---:|---|
| Detoasted compressed blob | detoast (~2.2s) | ~1.6 GB | Low |
| Fully decompressed column | detoast + decompress (~2.35s) | ~15 GB | Medium |

Going compressed-only captures ~95% of the warm-cache benefit at ~10%
of the memory cost. Decompression is already parallel across rayon
workers and is ~100-150ms of the total — saving it doesn't justify
10× the cache size or the wider correctness surface.

## Key and value layout

### Cache key

```rust
#[repr(C)]
#[derive(Copy, Clone, Hash, PartialEq, Eq)]
struct BlobCacheKey {
    companion_oid: u32,   // pg_class.oid of the _blobs table
    segment_id:    u32,   // segment_id within that companion
    col_idx:       u16,   // blob-table _col_idx column
    _pad:          u16,   // align to 12 bytes
}
```

12 bytes, trivially hashable. The companion OID changes on
`deltax_compress_partition` re-run (table is dropped + recreated), so
old entries become unreachable and age out via LRU — no active
invalidation needed for the common case.

### Cache value

A DSA pointer to a contiguous byte buffer holding the detoasted
compressed blob. Length stored separately so the entry is a fixed-size
record:

```rust
#[repr(C)]
struct BlobCacheEntry {
    data_ptr:   dsa_pointer,   // DSA-allocated bytes
    data_len:   u32,
    last_used:  AtomicU64,     // monotonic counter for LRU
    pin_count:  AtomicU16,     // in-use protection (see "Pinning")
    _flags:     u16,
}
```

24 bytes per entry header + variable-length DSA allocation for the bytes.

## Shared-memory infrastructure

Use PG's standard primitives — no hand-rolled allocator.

- **`shmem_request_hook`** (PG 15+): reserve fixed-size slots for the
  control structures (per-shard LWLock array, atomic counters, GUC
  binding) plus an initial DSA chunk size budget.
- **`shmem_startup_hook`**: initialise the control structure, create the
  DSA, create the `dshash` table backed by DSA memory.
- **`dsa_create_in_place` + `dsa_attach_in_place`**: leader process
  creates; subsequent backends attach via the well-known DSA handle
  stored in the control struct.
- **`dshash_create` / `dshash_attach`**: the index from
  `BlobCacheKey → BlobCacheEntry`.

This matches the pattern PG core uses for things like
`SharedFileSet`, the parallel-aware bitmap heap scan state, and
`pg_stat_statements`. It's well-trodden — no surprises.

### Why DSA + dshash, not custom allocator

DSA handles fragmentation reasonably well for the
small-medium-large size distribution we'll have (most blobs 30-300 KB,
a few outliers up to 1MB+). `dshash` gives concurrent hash with
striped locking out of the box. Custom allocator on top of `shm_open`
would need ~2 weeks of allocator engineering before we even start the
cache logic.

Risk: DSA fragmentation under churn. Mitigation: round allocations to
power-of-two size classes (16KB, 32KB, 64KB, 128KB, 256KB, 512KB,
1MB) so freed entries can be reused by similarly-sized inserts.

## Concurrency

### Sharded locks

`NUM_SHARDS = 64`. The shard index is `(key.hash >> 8) & 63`. Each
shard has:

- An `LWLock` (allocated via `RequestNamedLWLockTranche` in
  `shmem_request_hook`).
- A sub-slice of the LRU list (each shard maintains its own LRU,
  evicts independently).

This keeps lock contention bounded for OLAP workloads. 64 shards is
plenty even for highly concurrent point-query workloads; the actual
critical sections are short (hashmap insert + LRU update).

### Read path

1. Compute `key.hash`, identify shard.
2. Take shared lock on shard.
3. `dshash` lookup. If hit: bump `last_used` (relaxed atomic), bump
   `pin_count` (acquire), copy DSA bytes pointer + len out, release
   shared lock, return slice to caller.
4. If miss: release shared lock, fall through to the existing detoast
   path. After detoast succeeds, take exclusive lock, insert.

### Pinning

A reader needs to guarantee the DSA allocation doesn't get freed while
they're reading it. Two options:

- **Pin counter** (chosen): each lookup bumps `pin_count`, holds the
  slice for the duration of the query, decrements on
  `ExecEndCustomScan`. Evictor refuses to free entries with
  `pin_count > 0`; instead unlinks from index/LRU and queues for
  later free. Simple, no shared lock held during reads.
- Hazard pointers / epoch reclamation: cleaner concurrency but more
  code. Defer unless pin-counting proves contentious.

### Write path (cache miss → insert)

1. After detoast returns the `Vec<u8>` (existing code), look up shard.
2. Take exclusive lock.
3. Re-check key (concurrent insert may have happened).
   - If now present, drop our copy, increment its pin, return that.
4. If still missing: allocate DSA bytes, memcpy from Vec, insert into
   `dshash`, push to LRU head, release lock.
5. If allocation fails (cache full): trigger eviction (see below),
   retry once. If still fails, skip caching this miss — query still
   succeeds, we just didn't insert.

### Eviction

Triggered on insert when `total_bytes + new_entry_bytes > max_bytes`.

1. Pick the shard owning the new key.
2. Walk that shard's LRU tail backwards, looking for `pin_count == 0`
   entries.
3. Free DSA bytes, remove from `dshash`, remove from LRU list,
   decrement `total_bytes`.
4. Stop when we've freed enough.
5. If the local shard couldn't free enough (everything pinned), try
   neighbouring shards. If still not enough → log warning, skip insert.

This is per-shard LRU, not global. Approximation is fine for OLAP
workloads where eviction churn isn't on the critical path.

## Invalidation

The lazy story works because pg_deltax segments are immutable after
write:

- **Segment rewrite** (recompress partition): companion table is
  dropped and recreated → new OID → new cache keys. Old entries are
  unreachable, age out via LRU. No active invalidation.
- **Partition drop**: old OID's entries linger until LRU evicts them.
  Wastes some memory but never serves stale data (the OID can't reoccur).
- **In-place segment update**: doesn't happen in pg_deltax today. If
  this ever changes (e.g. a defrag job), it'd need an explicit
  invalidation hook on the modifying SQL function.

We could be more aggressive — register a `RegisterRelcacheCallback` so
that when a companion table relation is invalidated (DROP TABLE etc.),
we walk our shard list and remove entries by OID. Add this only if
profile shows wasted memory from stale entries.

## Integration point

The cache check lives at the start of `detoast_lazy_blobs`
(`src/scan/exec/segments.rs:2800`):

```rust
pub(super) unsafe fn detoast_lazy_blobs(seg: &mut SegmentData,
                                          companion_oid: Oid) {
    let segment_id = seg.segment_id;
    for bi in 0..seg.toast_pointers.len() {
        if seg.toast_pointers[bi].is_empty() {
            continue;
        }
        let key = BlobCacheKey { companion_oid, segment_id, col_idx: bi as u16, _pad: 0 };

        // 1. Try cache hit.
        if let Some(pinned) = blob_cache::get_pinned(&key) {
            // Pinned slice is valid for the rest of this query's lifetime.
            // Copy out — the rest of the pipeline expects Vec<u8>.
            // (After the `to_vec()` elimination follow-up, we can borrow.)
            seg.compressed_blobs[bi] = pinned.as_slice().to_vec();
            seg.toast_pointers[bi].clear();
            seg.cached_blob_pins.push(pinned);   // released at ExecEndCustomScan
            continue;
        }

        // 2. Miss: do the existing detoast.
        let ptr = seg.toast_pointers[bi].as_ptr() as *mut pg_sys::varlena;
        let detoasted = pg_sys::pg_detoast_datum(ptr);
        let len = pgrx::varsize_any_exhdr(detoasted);
        let data = pgrx::vardata_any(detoasted);
        let bytes = std::slice::from_raw_parts(data as *const u8, len).to_vec();
        if detoasted != ptr { pg_sys::pfree(detoasted as *mut _); }
        seg.toast_pointers[bi].clear();

        // 3. Insert into cache (best-effort).
        blob_cache::insert(&key, &bytes);

        seg.compressed_blobs[bi] = bytes;
    }
}
```

`seg.cached_blob_pins: Vec<BlobCachePin>` is a new field on
`SegmentData`. On `Drop` each pin decrements its entry's pin counter.
This guarantees the DSA bytes outlive every read of `seg.compressed_blobs`.

`companion_oid` is already known at the leader site that calls
`detoast_lazy_blobs` (it's in `companion_oids`); the signature change
threads it through. For worker threads inside `std::thread::scope`, no
change needed — they read the already-cached Vec<u8>.

## EXPLAIN integration

Add to `DeltaXTiming` / `DeltaXStats` rendering in `explain.rs`:

```
DeltaX Blob Cache: hits=2812 misses=89 bytes_served=1.4 GB
```

Counters:
- Per-query: hits, misses, bytes from cache (in `AggScanState` /
  `DecompressState`).
- Globally (via `pg_deltax_blob_cache_stats()` SRF): total hits,
  misses, bytes cached, bytes evicted, current entries, current
  bytes.

The global view helps operators size `blob_cache_mb` for their
workload.

## GUC surface

| GUC | Type | Default | Range | Meaning |
|---|---|---|---|---|
| `pg_deltax.blob_cache_mb` | int | `-1` (auto) | `-1..32768` | `-1` = auto (1/6 of physical RAM, clamped to [256, 16384] MiB). `0` = disabled. `N > 0` = explicit MiB. Postmaster context. |
| `pg_deltax.blob_cache_shards` | int | `64` | `1..1024` | Shard count. Powers of two recommended. Postmaster context. |

The auto-size default reads `MemTotal` from `/proc/meminfo` at
postmaster start. The resolved cap is observable via the SRF
(`SELECT bytes_max/1024/1024 AS bytes_max_mb FROM
pg_deltax_blob_cache_stats()`). Heuristic:

- **256 MiB floor.** On a 1 GiB Docker container, you get 256 MiB.
  On non-Linux hosts (where `/proc/meminfo` doesn't exist) the
  resolution silently falls back to the floor.
- **16384 MiB cap.** On a 96+ GiB production box, you get exactly
  16 GiB regardless of how much RAM is available (a 256 GiB
  instance gets 16 GiB, not ~42). The cap was 4 GiB until
  2026-06-10; that left a 32 GiB box thrashing on ClickBench's
  URL/Title working set (warm Q22 ran at a 42% miss rate). Truly
  huge workloads can still set an explicit value up to 32768.
- **1/6 of RAM in between.** A 32 GiB box gets ~5.3 GiB. A 24 GiB
  box gets 4 GiB. A 6 GiB box gets 1 GiB. The fraction was RAM/4
  until 2026-06-10; it dropped to RAM/6 because a filled cache is
  non-reclaimable shmem that must coexist with shared_buffers
  (conventionally RAM/4) and multi-GB transient allocations of
  high-cardinality aggregations — at RAM/4 the combination
  OOM-killed ClickBench Q32 (see `PERF_IMPROVEMENTS.md` #50/#51).

Set `pg_deltax.blob_cache_mb = 0` to disable the cache entirely
(e.g. for a baseline measurement). Set to an explicit positive value
to override the heuristic. See [Sizing](#sizing) for the workload
side of the story.

## Sizing

The cache is sized for the *working set* of detoasted compressed
column blobs across the queries you actually run, not the whole
column-store. Each compressed segment-column is one cache entry; a
typical ClickBench blob is 30–80 KB, JSONBench's single `data` jsonb
column is ~MB-scale. The cache only helps for blobs that get touched
again, so the relevant capacity is "how much hot column data does my
workload re-read."

**Rough heuristics**, in priority order:

1. **Empirical**: run the workload at the current default, query
   `pg_deltax_blob_cache_stats()`, and watch `insert_failures_total`.
   If it stays near zero, the cache fits. If it climbs into the
   thousands or millions, double the cache and re-test.
2. **By column**: `(rows_touched_per_query) × (avg_compressed_bytes_per_row)
   × (num_hot_columns)`. For JSONBench's single jsonb column on
   100 M rows ≈ 2.3 GB compressed; an 8 GB cache covers it with
   headroom for variation.
3. **By RAM budget**: `min(workload_estimate, 25% of physical RAM)`.
   Leave plenty for `shared_buffers`, `work_mem × parallel_workers`,
   and OS file cache. On a 128 GB box, 8–16 GB blob cache is
   comfortable.

**When the cache is too small** — symptom: `insert_failures_total >>
evictions_total`. The cache fills, then a workload-pattern issue
prevents eviction from firing reliably: a query that pins many entries
(via cache hits) during its scan can't evict any of them within the
same query, so subsequent misses in that query fall through without
caching. Between queries, pins release and the cache recovers, but
during a single multi-column heavy query the cache is effectively
read-only. **The right fix is always "size up"** — once the working
set fits, evictions never need to fire at all (they don't on JSONBench
at 8 GB). The neighbour-shard eviction fallback under
[Remaining](#remaining) would only chip at the margin of this
behaviour, not solve it.

**Auto-sizing.** The `-1` default reads `MemTotal` from
`/proc/meminfo` at postmaster start and resolves to `clamp(MemTotal /
6, 256 MiB, 16384 MiB)`. Falls back to the 256 MiB floor on non-Linux
hosts or anywhere `/proc/meminfo` isn't readable. The resolved value
is observable via `pg_deltax_blob_cache_stats().bytes_max` — there's
no separate startup log line; users querying the SRF immediately see
what they got. Postmaster context (locked at start, can't change at
runtime).

The heuristic was retuned on 2026-06-10. The cap was originally
4 GiB on the theory that it covered most working sets; ClickBench on
a 32 GiB c6a.4xlarge disproved that — the URL/Title blobs alone
exceed 4 GiB, so warm runs of Q20/Q22 still paid full detoast (42%
miss rate on Q22). The fraction was originally RAM/4 — that OOM'd:
a filled cache is non-reclaimable shmem, and together with
shared_buffers (conventionally RAM/4) and the transient agg maps of
a high-cardinality GROUP BY (ClickBench Q32: ~14 GB anon after
`PERF_IMPROVEMENTS.md` #51, ~18 GB before) it pushed the box over
the edge. RAM/6 with a 16 GiB cap keeps the default useful on big
boxes while leaving room for worst-case query transients; operators
who want more (or less) should set an explicit value.

## Testing matrix

### Correctness

- Cache parity: every existing query must produce bytewise-identical
  results with `blob_cache_mb=0` and `blob_cache_mb=large`. Run the
  full integration suite under both settings.
- Eviction parity: small cache (`blob_cache_mb=8`) on a workload whose
  working set is 10× larger must still produce identical results
  (LRU evictions don't corrupt anything).
- Concurrent access: parametrise existing parallel tests over
  `pg_deltax.parallel_workers ∈ {1, 2, 4, 16}` × cache on/off.
- Restart recovery: queries after PG restart populate fresh cache,
  identical results.

### Failure modes

- DSA OOM: simulate by setting `blob_cache_mb=1` and inserting a
  larger-than-cache blob; insert should be skipped, query should
  still succeed.
- Pin overflow: contrive a query holding many segments pinned, evict
  attempts must skip pinned entries.
- Concurrent insert race: two backends miss the same key
  simultaneously; only one ends up in the cache, the other's bytes
  are dropped without leaking.

### Performance

JSONBench m6i.8xlarge, post-cache:
- Cold first run: same as today (cache populates).
- Warm 2nd/3rd runs: target Q1 3.6s → ~1.5s, Q3 2.7s → ~0.6s,
  Q4 4.4s → ~2.0s. Total warm 11.8s → ~6-7s.

RTABench full + ClickBench full: 0 regression at default
`blob_cache_mb=0`. Measure with `blob_cache_mb=1024` to confirm
positive on warm subset. Sanity check: no regression on cold runs.

## Implementation phases

Each phase is a self-contained PR that compiles and passes tests.

**Phase 1 (landed 2026-05-13): bootstrap + scaffolding**
- ~~`shmem_request_hook`, `shmem_startup_hook` registered.~~
  Deferred to the storage backend; `register_hooks` exists and is
  called from `_PG_init` but is currently a no-op stub.
- ~~Control struct, DSA + dshash created.~~ Deferred to storage backend.
- GUCs `pg_deltax.blob_cache_mb` (default `1024`) and
  `pg_deltax.blob_cache_shards` (default `64`) registered.
- `blob_cache::{get_pinned, insert}` stubs that always miss — done.
- `BlobCachePin` with Drop-based pin release — done; `SegmentData`
  carries `cached_blob_pins`.
- `detoast_lazy_blobs` / `detoast_lazy_blobs_selective` integrated
  with cache (miss path is identical to old behaviour).
- Build clean, clippy clean on new code, 382 unit tests pass.

**Phase 2 (3-4 days): functional cache**
- DSA allocation, size-class buckets, LRU per shard, pinning.
- Hook into `detoast_lazy_blobs`.
- EXPLAIN counters.
- Tests: cache parity at `blob_cache_mb=0` vs default, full
  integration suite green, EXPLAIN shows hits.

**Phase 3 (2-3 days): eviction + observability**
- LRU eviction, size accounting, neighbour-shard fallback.
- `pg_deltax_blob_cache_stats()` SRF.
- Tests: tiny cache + big workload produces correct results,
  eviction counts non-zero, stats SRF works.

**Phase 4 (2-3 days): bench validation + harden**
- JSONBench EC2: target ~7s warm total.
- RTABench full + ClickBench full: zero regression on default
  (cache off), measurable improvement on tuned setting.
- Decide on production default. Document the GUC in the README.
- Add follow-up integration test: parametrised existing parallel-scan
  tests over cache on/off.

**Phase 5 (optional, follow-up): the `to_vec()` elimination**
- After Phase 2 lands, the cache HIT path can return a borrowed
  slice instead of copying into a Vec<u8> — the DSA bytes are
  guaranteed alive while pinned.
- Saves another ~200ms/query on warm hits.
- Independent of the cache mechanism; tracked separately.

Total: ~2 weeks for a production-quality version. A 3-4 day MVP
(phases 1+2 only, no eviction, no observability) would prove the
cache-hit benefit on JSONBench warm.

## Decision log

- **2026-05: cache compressed bytes, not decompressed.** Decompression
  is parallel and ~5% of warm wall time. Caching decompressed would
  10× memory cost for marginal benefit. Revisit if decompression ever
  becomes the dominant remaining cost.
- **2026-05: use PG `dsa` + `dshash`.** Standard, well-supported
  primitives. Custom allocator is weeks of work before reaching cache
  logic. Accept DSA fragmentation risk; mitigate via size classes.
- **2026-05: per-shard LRU, not global.** Approximation acceptable
  for OLAP. Global LRU would require either a global lock (contention)
  or cross-shard atomics (complex).
- **2026-05: pin-counting over hazard pointers.** Simpler;
  contention is on insert, not read. Defer hazard pointers unless we
  see contention.
- **2026-05: default `blob_cache_mb=0`.** Opt-in until measured.
  Flip when JSONBench + ClickBench + RTABench all show wins at the
  tuned setting.

## What this doc deliberately doesn't cover

- The `to_vec()` elimination (a separate, smaller optimisation that
  can land independently — see Phase 5 above, but it's not a
  prerequisite).
- Combined-column blob storage (PARALLEL_AGG.md follow-up). Reduces
  toast-id lookups; orthogonal to caching detoasted bytes.
- A cache for fully decompressed column data. Re-evaluate after the
  compressed-blob cache is proven, in a separate doc.
