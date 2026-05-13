//! Process-shared cache for detoasted compressed segment blobs.
//!
//! See `dev/docs/BLOB_CACHE.md` for the full design. The cache stores
//! detoasted compressed bytes keyed by
//! `(companion_oid, segment_id, col_idx)`, so repeated queries against the
//! same segment-column skip `pg_detoast_datum`. Entries live in DSA-backed
//! shared memory and are indexed via a sharded LWLock-protected dshash
//! table. Eviction is per-shard LRU with size accounting in bytes.
//!
//! ## API surface
//!
//! - [`get_pinned`] — try to fetch a cached blob. On hit, returns a
//!   [`BlobCachePin`] that keeps the DSA bytes alive until dropped.
//! - [`insert`] — best-effort cache insertion after a miss. Skipped if
//!   the cache is disabled or full.
//! - [`stats`] — global counters (entries, bytes, hits, misses,
//!   evictions, insert failures) exposed via the
//!   `pg_deltax_blob_cache_stats()` SRF.
//! - [`register_hooks`] — called from `_PG_init`. Registers the
//!   `shmem_request_hook` (PG 15+) or `RequestAddinShmemSpace` (PG 14)
//!   plus the `shmem_startup_hook` that creates the control struct,
//!   DSA area, dshash table, and named LWLock tranche.
//!
//! ## Current state
//!
//! Phase 1: scaffolding. The control struct, key/value types, and API
//! surface are defined; `get_pinned` always returns `None` and `insert`
//! is a no-op. The integration site in `detoast_lazy_blobs` is wired up
//! so once the storage layer in [`storage`] is filled in, the cache
//! becomes live without any further plumbing.

use std::sync::atomic::{AtomicU64, Ordering};

use pgrx::iter::TableIterator;
use pgrx::name;
use pgrx::prelude::*;

mod storage;

/// Cache key: 12 bytes, trivially hashable. The companion OID changes on
/// `deltax_compress_partition` re-run (table dropped + recreated), so
/// stale entries become unreachable and age out via LRU without explicit
/// invalidation.
#[repr(C)]
#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug)]
pub(crate) struct BlobCacheKey {
    pub(crate) companion_oid: u32,
    pub(crate) segment_id: u32,
    pub(crate) col_idx: u16,
    pub(crate) _pad: u16,
}

impl BlobCacheKey {
    pub(crate) fn new(companion_oid: pgrx::pg_sys::Oid, segment_id: i32, col_idx: usize) -> Self {
        Self {
            companion_oid: companion_oid.to_u32(),
            segment_id: segment_id as u32,
            col_idx: col_idx as u16,
            _pad: 0,
        }
    }
}

/// Handle returned by [`get_pinned`]. While alive, the underlying DSA
/// allocation is guaranteed not to be freed. Dropped at end of query
/// when the owning `SegmentData` is dropped from `DecompressState`.
///
/// The slice it lends out is valid for the lifetime of the pin.
pub(crate) struct BlobCachePin {
    inner: storage::PinInner,
}

impl BlobCachePin {
    /// Borrow the cached bytes. The slice is valid until this pin drops.
    #[allow(dead_code)] // Will be used by detoast_lazy_blobs once storage lands.
    pub(crate) fn as_slice(&self) -> &[u8] {
        self.inner.as_slice()
    }
}

impl Drop for BlobCachePin {
    fn drop(&mut self) {
        self.inner.release();
    }
}

/// Global counters for the `pg_deltax_blob_cache_stats()` SRF.
#[derive(Copy, Clone, Default, Debug)]
pub(crate) struct BlobCacheStats {
    pub(crate) entries: u64,
    pub(crate) bytes_used: u64,
    pub(crate) bytes_max: u64,
    pub(crate) hits_total: u64,
    pub(crate) misses_total: u64,
    pub(crate) evictions_total: u64,
    pub(crate) insert_failures_total: u64,
}

/// Process-local counter incremented at startup for each session that
/// successfully attached to the cache. Useful for sanity-checking that
/// the shmem hooks ran.
static ATTACH_COUNT: AtomicU64 = AtomicU64::new(0);

#[allow(dead_code)] // Surfaced once the cache is live; useful for debug right now.
pub(crate) fn attach_count() -> u64 {
    ATTACH_COUNT.load(Ordering::Relaxed)
}

/// Look up `key` in the cache. Returns `Some(pin)` on hit, `None` on
/// miss or when the cache is disabled. On hit, increments the per-shard
/// LRU position and the entry's pin count; the returned pin releases the
/// pin count when dropped.
pub(crate) fn get_pinned(key: &BlobCacheKey) -> Option<BlobCachePin> {
    storage::get_pinned(key).map(|inner| BlobCachePin { inner })
}

/// Best-effort insert. If the cache is disabled, full and unable to
/// evict, or the same key already exists (concurrent insert race), the
/// call silently no-ops. Always safe to call after a successful detoast.
pub(crate) fn insert(key: &BlobCacheKey, bytes: &[u8]) {
    storage::insert(key, bytes);
}

/// Snapshot of global stats. Used by the SRF and tests.
pub(crate) fn stats() -> BlobCacheStats {
    storage::stats()
}

/// Called from `_PG_init`. Registers the shmem request + startup hooks
/// so the cache is initialised once the postmaster sets up shared memory.
pub(crate) fn register_hooks() {
    storage::register_hooks();
    ATTACH_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Returns the configured cache size in bytes, clamped to a sane
/// maximum. `0` means the cache is disabled.
pub(crate) fn configured_bytes() -> usize {
    let mb = crate::BLOB_CACHE_MB.get();
    if mb <= 0 {
        return 0;
    }
    (mb as usize).saturating_mul(1024 * 1024)
}

/// Returns the configured shard count, rounded up to the next power of two.
pub(crate) fn configured_shards() -> usize {
    let s = crate::BLOB_CACHE_SHARDS.get();
    let raw = s.max(1) as usize;
    raw.next_power_of_two().min(1024)
}

/// SRF that exposes the current global blob cache counters. One row.
///
/// All counters are returned as `bigint` (PG has no unsigned types).
/// Negative values are not possible — internal counters are `u64` and
/// will saturate before overflowing `i64::MAX` (~9 EiB / hits ~9 quintillion).
#[pg_extern]
fn pg_deltax_blob_cache_stats() -> TableIterator<
    'static,
    (
        name!(entries, i64),
        name!(bytes_used, i64),
        name!(bytes_max, i64),
        name!(hits_total, i64),
        name!(misses_total, i64),
        name!(evictions_total, i64),
        name!(insert_failures_total, i64),
    ),
> {
    let s = stats();
    TableIterator::once((
        s.entries as i64,
        s.bytes_used as i64,
        s.bytes_max as i64,
        s.hits_total as i64,
        s.misses_total as i64,
        s.evictions_total as i64,
        s.insert_failures_total as i64,
    ))
}
