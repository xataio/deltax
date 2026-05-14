//! Shared-memory storage backend for the blob cache.
//!
//! Everything `unsafe` lives here. The public surface in `super` stays
//! safe Rust. See `dev/docs/BLOB_CACHE.md` for the design.
//!
//! ## Layout
//!
//! One named shmem block, allocated by `ShmemInitStruct`:
//!
//! ```text
//! [ BlobCacheCtl + Shard[MAX_SHARDS] ][ DSA in-place chunk ]
//! ```
//!
//! - The control block holds global atomics (totals, hit/miss counters,
//!   dsa_handle) and the fixed-size shard array.
//! - The DSA chunk is `DSA_INITIAL_BYTES` of in-place memory passed to
//!   `dsa_create_in_place`; the DSA grows beyond that via DSM segments
//!   up to the configured `blob_cache_mb`.
//! - LWLocks for shards live in a separate named tranche obtained via
//!   `GetNamedLWLockTranche("pg_deltax_blob_cache", n_shards)`.
//!
//! Entries live in DSA memory. Each Entry stores the key inline, an
//! atomic pin_count, a last_used counter for eventual eviction, and a
//! `bucket_next` dsa_pointer that chains entries within a bucket.
//!
//! ## Concurrency
//!
//! - Reads (`get_pinned`): shared shard LWLock, walk bucket chain,
//!   atomically bump pin_count + last_used, return pin.
//! - Writes (`insert`): exclusive shard LWLock, re-check key, allocate
//!   in DSA, prepend to bucket.
//! - Pin release: lock-free `pin_count -= 1`. Entries with pin_count > 0
//!   are skipped by eviction (eviction not yet implemented; insert
//!   fails when the cache is full).
//!
//! ## Current limitations
//!
//! - No eviction. When the cache fills, subsequent inserts are dropped
//!   (recorded in `insert_failures_total`). For the JSONBench demo this
//!   is acceptable: the hot working set fits in 1 GB and is steady.
//! - No invalidation. Stale entries (post-recompress) age out only by
//!   never being looked up again. Wasted memory, never wrong answers.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, Ordering};

use pgrx::pg_sys;

use super::{BlobCacheKey, BlobCacheStats};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// We reserve the full `blob_cache_mb` of named shmem up front and
// create an in-place DSA inside it. This avoids DSM dynamic-growth
// gymnastics (which need a fully-up DSM subsystem and proper
// dsm_segment registration that's awkward from shmem_startup_hook).
// Trade-off: the user's `blob_cache_mb` becomes a hard shmem
// reservation at postmaster start.

/// Maximum number of shards we provision space for. The actual number
/// in use is taken from `configured_shards()`; unused slots are zeroed.
const MAX_SHARDS: usize = 1024;

/// Per-shard bucket count. Power of two. Chosen so that buckets stay
/// short for typical working sets (a few thousand entries per shard).
const BUCKETS_PER_SHARD: u32 = 256;

/// Shmem block name (also used as the LWLock tranche name).
const SHMEM_NAME: &std::ffi::CStr = c"pg_deltax_blob_cache";

// From PG 17 dsa.h (#defines, not bound by pgrx). Mirror pgstat_shmem.c's
// usage: passing 0,0 violates the contract Assert'd at dsa.c:1233.
const DSA_DEFAULT_INIT_SEGMENT_SIZE: usize = 1 * 1024 * 1024;
const DSA_MAX_SEGMENT_SIZE: usize = 1usize << 40;

// ---------------------------------------------------------------------------
// On-shmem data structures (all #[repr(C)])
// ---------------------------------------------------------------------------

/// Control block + shards. Lives in the named shmem block.
///
/// Sized for the maximum supported shard count to keep the layout
/// static. Unused shards beyond `n_shards` stay zero-initialised.
#[repr(C)]
struct BlobCacheCtl {
    /// `0` until the first backend completes shmem_startup_hook,
    /// `1` afterwards. Other backends spin on this until it flips.
    initialized: AtomicU32,

    /// `1` once `dsa_create_in_place` has succeeded in postmaster
    /// and the in-place chunk is ready for backend `dsa_attach_in_place`.
    /// Backends treat `0` as "DSA unavailable, fall back to no-cache".
    dsa_ready: AtomicU32,

    /// LWLock tranche id assigned by `GetNamedLWLockTranche`.
    lwlock_tranche_id: AtomicI32,

    /// Number of active shards (always <= MAX_SHARDS).
    n_shards: AtomicU32,

    /// Total bytes currently allocated to entry data (excludes Entry
    /// header overhead and DSA metadata). Used to decide when the
    /// cache is full.
    total_bytes: AtomicU64,

    /// Maximum allowed bytes for entry data. Set from
    /// `pg_deltax.blob_cache_mb` at startup.
    max_bytes: AtomicU64,

    /// Monotonically increasing counter for the logical LRU clock.
    /// Each successful lookup/insert calls `fetch_add(1, Relaxed)`.
    last_used_counter: AtomicU64,

    /// Total observed counters. Read by the SRF and EXPLAIN.
    n_entries: AtomicU64,
    hits_total: AtomicU64,
    misses_total: AtomicU64,
    evictions_total: AtomicU64,
    insert_failures_total: AtomicU64,

    /// Our own LWLock storage. Each shard has one LWLock, padded to a
    /// cache line by `LWLockPadded`. We allocate inline rather than via
    /// `RequestNamedLWLockTranche` so we control initialization
    /// explicitly via `LWLockInitialize` in shmem_startup_hook.
    shard_locks: [pg_sys::LWLockPadded; MAX_SHARDS],

    shards: [Shard; MAX_SHARDS],
}

/// Per-shard metadata. The bucket array lives in DSA (lazily allocated
/// on first use). LRU head/tail also live here and are maintained under
/// the shard's exclusive LWLock by inserts and evictions.
#[repr(C)]
struct Shard {
    /// dsa_pointer to a `[u64; BUCKETS_PER_SHARD]` of bucket heads
    /// (each a dsa_pointer to the first Entry in that bucket, or 0).
    /// `0` means the shard has no entries yet.
    bucket_array: AtomicU64,

    /// Per-shard entry count (atomic for lock-free observation).
    n_entries: AtomicU32,

    _pad: u32,

    /// Per-shard byte usage (sum of entry data_len for this shard).
    bytes_used: AtomicU64,

    /// dsa_pointer to the most-recently-inserted entry (MRU end of the
    /// LRU list), or 0 if the shard is empty. Mutated only under the
    /// shard's exclusive LWLock.
    lru_head: AtomicU64,

    /// dsa_pointer to the oldest entry (LRU end), or 0 if empty.
    /// Eviction walks backwards from here.
    lru_tail: AtomicU64,
}

const _: () = {
    // Catch accidental size blowups at compile time.
    assert!(std::mem::size_of::<Shard>() == 40);
};

/// Entry header. Lives in DSA memory, preceded by `data_len` bytes of
/// payload in a separate dsa_pointer allocation.
#[repr(C)]
struct Entry {
    key: BlobCacheKey,
    _pad1: u32,

    /// dsa_pointer to the payload bytes.
    data_ptr: AtomicU64,
    data_len: u32,
    _pad2: u32,

    /// Number of `BlobCachePin`s currently referencing this entry.
    /// Eviction skips entries with `pin_count > 0`.
    pin_count: AtomicU32,
    _pad3: u32,

    /// Snapshot of `BlobCacheCtl::last_used_counter` at last hit/insert.
    /// Recorded for observability; eviction uses LRU list order, not
    /// this value, so it's safe to update under SHARED lock.
    last_used: AtomicU64,

    /// dsa_pointer to next entry in the same bucket chain (0 = end).
    bucket_next: AtomicU64,

    /// dsa_pointer to the next-newer entry in the shard's LRU list,
    /// or 0 if this entry is at the head (MRU). Mutated only under
    /// the shard's exclusive LWLock.
    lru_prev: AtomicU64,

    /// dsa_pointer to the next-older entry, or 0 if at the tail (LRU).
    lru_next: AtomicU64,
}

const _: () = {
    // 16 bytes key+pad, 16 data ptr/len/pad, 8 pin+pad, 8 last_used,
    // 8 bucket_next, 8 lru_prev, 8 lru_next.
    assert!(std::mem::size_of::<Entry>() == 72);
};

// ---------------------------------------------------------------------------
// Process-local state
// ---------------------------------------------------------------------------

/// Pointer to the shared control block in this process. Resolved lazily
/// in `attach()` so backends that never touch the cache don't pay the
/// `ShmemInitStruct`-lookup cost beyond what postmaster already did.
///
/// `OnceLock<usize>` because raw pointers aren't `Sync`. We store the
/// pointer as a usize and reinterpret.
static CTL_PTR: OnceLock<usize> = OnceLock::new();

/// Per-backend DSA area mapping. Resolved lazily in `attach()` after
/// the control block is reachable. Stored as usize for the same reason
/// as `CTL_PTR`.
static DSA_AREA_PTR: OnceLock<usize> = OnceLock::new();

/// LWLock tranche base pointer for this backend. `GetNamedLWLockTranche`
/// returns a `*mut LWLockPadded`; we cache it after first call.
static LWLOCK_BASE_PTR: OnceLock<usize> = OnceLock::new();

/// Previous hooks, captured in `register_hooks`. Wrapping in `OnceLock`
/// keeps us off Rust 2024's `static mut` lint while still allowing the
/// callback to read them.
///
/// `Option` inside `OnceLock`: `None` means "no previous hook was set"
/// (the common case for a single extension). The `OnceLock` itself is
/// populated exactly once in `_PG_init`.
static PREV_SHMEM_REQUEST_HOOK: OnceLock<pg_sys::shmem_request_hook_type> = OnceLock::new();
static PREV_SHMEM_STARTUP_HOOK: OnceLock<pg_sys::shmem_startup_hook_type> = OnceLock::new();

/// Snapshotted at registration time so `shmem_request_hook` doesn't
/// have to call the GUC accessor (the GUC mechanism may not be safe to
/// touch from the request hook on all PG versions).
static RESERVATION_BYTES: AtomicU64 = AtomicU64::new(0);
static RESERVATION_SHARDS: AtomicU32 = AtomicU32::new(0);

/// Set to `true` from the startup hook once we successfully attach.
/// Used by `get_pinned` / `insert` to fast-fail when the cache failed
/// to initialise (e.g. shmem reservation was insufficient).
static CACHE_USABLE: AtomicBool = AtomicBool::new(false);

// ---------------------------------------------------------------------------
// Pin handle plumbing
// ---------------------------------------------------------------------------

pub(super) struct PinInner {
    /// dsa_pointer to the pinned Entry, or 0 for a no-op pin.
    entry_dp: u64,
}

impl PinInner {
    pub(super) fn as_slice(&self) -> &[u8] {
        if self.entry_dp == 0 {
            return &[];
        }
        unsafe {
            let area = dsa_area_ptr();
            let entry = pg_sys::dsa_get_address(area, self.entry_dp) as *const Entry;
            let data_dp = (*entry).data_ptr.load(Ordering::Acquire);
            let data_len = (*entry).data_len as usize;
            if data_dp == 0 || data_len == 0 {
                return &[];
            }
            let data = pg_sys::dsa_get_address(area, data_dp) as *const u8;
            std::slice::from_raw_parts(data, data_len)
        }
    }

    pub(super) fn release(&mut self) {
        if self.entry_dp == 0 {
            return;
        }
        unsafe {
            let area = dsa_area_ptr();
            let entry = pg_sys::dsa_get_address(area, self.entry_dp) as *const Entry;
            (*entry).pin_count.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

// ---------------------------------------------------------------------------
// Public storage API (called from super::mod)
// ---------------------------------------------------------------------------

pub(super) fn get_pinned(key: &BlobCacheKey) -> Option<PinInner> {
    if super::configured_bytes() == 0 || !CACHE_USABLE.load(Ordering::Acquire) {
        return None;
    }
    if !attach() {
        return None;
    }
    unsafe {
        let ctl = ctl_ref();
        let n_shards = ctl.n_shards.load(Ordering::Relaxed) as usize;
        if n_shards == 0 {
            return None;
        }

        let h = hash_key(key);
        let shard_idx = ((h >> 8) as usize) & (n_shards - 1);
        let bucket_idx = (h as u32) & (BUCKETS_PER_SHARD - 1);

        let lock = shard_lwlock(shard_idx);
        pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode::LW_SHARED);

        let shard = &ctl.shards[shard_idx];
        let buckets_dp = shard.bucket_array.load(Ordering::Acquire);
        if buckets_dp == 0 {
            pg_sys::LWLockRelease(lock);
            ctl.misses_total.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        let mut entry_dp = bucket_head(buckets_dp, bucket_idx);
        while entry_dp != 0 {
            let entry = entry_ptr(entry_dp);
            if (*entry).key == *key {
                (*entry).pin_count.fetch_add(1, Ordering::AcqRel);
                let tick = ctl.last_used_counter.fetch_add(1, Ordering::Relaxed);
                (*entry).last_used.store(tick, Ordering::Relaxed);
                pg_sys::LWLockRelease(lock);
                ctl.hits_total.fetch_add(1, Ordering::Relaxed);
                return Some(PinInner { entry_dp });
            }
            entry_dp = (*entry).bucket_next.load(Ordering::Acquire);
        }

        pg_sys::LWLockRelease(lock);
        ctl.misses_total.fetch_add(1, Ordering::Relaxed);
        None
    }
}

pub(super) fn insert(key: &BlobCacheKey, bytes: &[u8]) {
    if super::configured_bytes() == 0 || !CACHE_USABLE.load(Ordering::Acquire) {
        return;
    }
    if bytes.is_empty() {
        return;
    }
    if !attach() {
        return;
    }
    unsafe {
        let ctl = ctl_ref();
        let n_shards = ctl.n_shards.load(Ordering::Relaxed) as usize;
        if n_shards == 0 {
            return;
        }

        let h = hash_key(key);
        let shard_idx = ((h >> 8) as usize) & (n_shards - 1);
        let bucket_idx = (h as u32) & (BUCKETS_PER_SHARD - 1);
        let lock = shard_lwlock(shard_idx);

        pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let area = dsa_area_ptr();
        let shard = &ctl.shards[shard_idx];

        // Lazily allocate the per-shard bucket array.
        let mut buckets_dp = shard.bucket_array.load(Ordering::Acquire);
        if buckets_dp == 0 {
            let nbytes = (BUCKETS_PER_SHARD as usize) * std::mem::size_of::<u64>();
            let new_dp = pg_sys::dsa_allocate_extended(
                area,
                nbytes,
                (pg_sys::DSA_ALLOC_ZERO | pg_sys::DSA_ALLOC_NO_OOM) as i32,
            );
            if new_dp == 0 {
                pg_sys::LWLockRelease(lock);
                ctl.insert_failures_total.fetch_add(1, Ordering::Relaxed);
                return;
            }
            shard.bucket_array.store(new_dp, Ordering::Release);
            buckets_dp = new_dp;
        }

        // Re-check: another backend may have inserted this key while
        // we were waiting on the exclusive lock.
        let mut entry_dp = bucket_head(buckets_dp, bucket_idx);
        while entry_dp != 0 {
            let entry = entry_ptr(entry_dp);
            if (*entry).key == *key {
                pg_sys::LWLockRelease(lock);
                return;
            }
            entry_dp = (*entry).bucket_next.load(Ordering::Acquire);
        }

        // Capacity / allocation loop. Eviction can be triggered by two
        // signals: (a) our soft accounting (`total_bytes + needed >
        // max_bytes`) and (b) DSA itself returning InvalidDsaPointer for
        // the actual allocation. (b) matters because DSA has per-page /
        // per-allocation overhead that's not reflected in `total_bytes`,
        // so the cap can be hit before our accounting catches it. We
        // evict and retry until either both allocations succeed or
        // there's nothing left to evict.
        let max_bytes = ctl.max_bytes.load(Ordering::Relaxed);
        let needed = bytes.len() as u64;
        let entry_size = std::mem::size_of::<Entry>();
        let new_entry_dp: u64;
        let data_dp: u64;
        loop {
            // Soft cap check first — fastest path.
            let cur = ctl.total_bytes.load(Ordering::Relaxed);
            if cur.saturating_add(needed) > max_bytes {
                let to_free = cur.saturating_add(needed).saturating_sub(max_bytes);
                let freed = evict_in_shard(ctl, area, shard, shard_idx, to_free);
                if freed == 0 {
                    // Nothing to evict (empty or all pinned). v1 bails;
                    // neighbour-shard fallback is v2.
                    pg_sys::LWLockRelease(lock);
                    ctl.insert_failures_total.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                continue;
            }

            // Try the actual allocations.
            let entry_alloc = pg_sys::dsa_allocate_extended(
                area,
                entry_size,
                (pg_sys::DSA_ALLOC_ZERO | pg_sys::DSA_ALLOC_NO_OOM) as i32,
            );
            if entry_alloc == 0 {
                // DSA exhausted despite soft cap saying we're OK. Free
                // some entries and retry. Target a chunk larger than
                // the entry so we make real progress.
                let freed = evict_in_shard(ctl, area, shard, shard_idx, (entry_size as u64).max(4096));
                if freed == 0 {
                    pg_sys::LWLockRelease(lock);
                    ctl.insert_failures_total.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                continue;
            }

            let data_alloc = pg_sys::dsa_allocate_extended(
                area,
                bytes.len(),
                pg_sys::DSA_ALLOC_NO_OOM as i32,
            );
            if data_alloc == 0 {
                pg_sys::dsa_free(area, entry_alloc);
                let freed = evict_in_shard(ctl, area, shard, shard_idx, needed);
                if freed == 0 {
                    pg_sys::LWLockRelease(lock);
                    ctl.insert_failures_total.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                continue;
            }

            // Both allocations succeeded.
            new_entry_dp = entry_alloc;
            data_dp = data_alloc;
            break;
        }
        let data_ptr = pg_sys::dsa_get_address(area, data_dp) as *mut u8;
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), data_ptr, bytes.len());

        // Initialise the entry. dsa_allocate_extended with DSA_ALLOC_ZERO
        // already zeroed the memory, so atomics start at 0 (including
        // lru_prev / lru_next; lru_prepend overwrites them).
        let entry = entry_ptr_mut(new_entry_dp);
        (*entry).key = *key;
        (*entry).data_ptr.store(data_dp, Ordering::Release);
        (*entry).data_len = bytes.len() as u32;
        let tick = ctl.last_used_counter.fetch_add(1, Ordering::Relaxed);
        (*entry).last_used.store(tick, Ordering::Relaxed);

        // Prepend to bucket chain.
        let prev_head = bucket_head(buckets_dp, bucket_idx);
        (*entry).bucket_next.store(prev_head, Ordering::Release);
        bucket_head_store(buckets_dp, bucket_idx, new_entry_dp);

        // Prepend to LRU list (MRU end).
        lru_prepend(shard, new_entry_dp, entry);

        shard.n_entries.fetch_add(1, Ordering::Relaxed);
        shard.bytes_used
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        ctl.n_entries.fetch_add(1, Ordering::Relaxed);
        ctl.total_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);

        pg_sys::LWLockRelease(lock);
    }
}

/// Per-shard diagnostic snapshot. Walks each shard's LRU list under
/// shared lock and reports counts. Slow; only for debugging.
pub(super) fn shard_diag() -> Vec<(i32, i64, i64, i64, i64, i64, i64, i64)> {
    let mut out: Vec<(i32, i64, i64, i64, i64, i64, i64, i64)> = Vec::new();
    if !CACHE_USABLE.load(Ordering::Acquire) || !attach() {
        return out;
    }
    unsafe {
        let ctl = ctl_ref();
        let n_shards = ctl.n_shards.load(Ordering::Relaxed) as usize;
        for i in 0..n_shards {
            let shard = &ctl.shards[i];
            let lock = shard_lwlock(i);
            pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode::LW_SHARED);
            let head_dp = shard.lru_head.load(Ordering::Acquire);
            let tail_dp = shard.lru_tail.load(Ordering::Acquire);
            let n_entries = shard.n_entries.load(Ordering::Relaxed) as i64;
            let bytes_used = shard.bytes_used.load(Ordering::Relaxed) as i64;
            let mut walk: i64 = 0;
            let mut pinned: i64 = 0;
            let mut unpinned: i64 = 0;
            let mut cur = tail_dp;
            // Safety stop: don't walk longer than n_entries (catches loops).
            let max_walk = (n_entries.max(0) as u64).saturating_add(8) as i64;
            while cur != 0 && walk < max_walk {
                let entry = entry_ptr(cur);
                if (*entry).pin_count.load(Ordering::Acquire) == 0 {
                    unpinned += 1;
                } else {
                    pinned += 1;
                }
                walk += 1;
                cur = (*entry).lru_prev.load(Ordering::Acquire);
            }
            pg_sys::LWLockRelease(lock);
            // Only include shards with state to keep output small.
            if n_entries > 0 || head_dp != 0 || tail_dp != 0 {
                out.push((
                    i as i32, n_entries, bytes_used, walk, pinned, unpinned,
                    head_dp as i64, tail_dp as i64,
                ));
            }
        }
    }
    out
}

pub(super) fn stats() -> BlobCacheStats {
    if !CACHE_USABLE.load(Ordering::Acquire) || !attach() {
        return BlobCacheStats {
            bytes_max: super::configured_bytes() as u64,
            ..Default::default()
        };
    }
    unsafe {
        let ctl = ctl_ref();
        BlobCacheStats {
            entries: ctl.n_entries.load(Ordering::Relaxed),
            bytes_used: ctl.total_bytes.load(Ordering::Relaxed),
            bytes_max: ctl.max_bytes.load(Ordering::Relaxed),
            hits_total: ctl.hits_total.load(Ordering::Relaxed),
            misses_total: ctl.misses_total.load(Ordering::Relaxed),
            evictions_total: ctl.evictions_total.load(Ordering::Relaxed),
            insert_failures_total: ctl.insert_failures_total.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Shmem registration (called from _PG_init)
// ---------------------------------------------------------------------------

pub(super) fn register_hooks() {
    let bytes = super::configured_bytes();
    let shards = super::configured_shards().clamp(1, MAX_SHARDS);
    RESERVATION_BYTES.store(bytes as u64, Ordering::Relaxed);
    RESERVATION_SHARDS.store(shards as u32, Ordering::Relaxed);

    if bytes == 0 {
        return;
    }

    unsafe {
        #[cfg(feature = "pg14")]
        {
            pg_sys::RequestAddinShmemSpace(reservation_total_bytes(shards));
        }
        #[cfg(not(feature = "pg14"))]
        {
            let _ = PREV_SHMEM_REQUEST_HOOK
                .set(pg_sys::shmem_request_hook);
            pg_sys::shmem_request_hook = Some(my_shmem_request_hook);
        }

        let _ = PREV_SHMEM_STARTUP_HOOK
            .set(pg_sys::shmem_startup_hook);
        pg_sys::shmem_startup_hook = Some(my_shmem_startup_hook);
    }
}

#[cfg(not(feature = "pg14"))]
unsafe extern "C-unwind" fn my_shmem_request_hook() {
    unsafe {
        if let Some(Some(prev)) = PREV_SHMEM_REQUEST_HOOK.get() {
            prev();
        }
        let shards = RESERVATION_SHARDS.load(Ordering::Relaxed) as usize;
        if shards == 0 {
            return;
        }
        pg_sys::RequestAddinShmemSpace(reservation_total_bytes(shards));
    }
}

unsafe extern "C-unwind" fn my_shmem_startup_hook() {
    unsafe {
        if let Some(Some(prev)) = PREV_SHMEM_STARTUP_HOOK.get() {
            prev();
        }
        let shards = RESERVATION_SHARDS.load(Ordering::Relaxed) as usize;
        if shards == 0 {
            return;
        }

        let mut found: bool = false;
        let total = reservation_total_bytes(shards);
        let block = pg_sys::ShmemInitStruct(
            SHMEM_NAME.as_ptr(),
            total,
            &mut found as *mut bool,
        );
        if block.is_null() {
            return;
        }
        let ctl = block as *mut BlobCacheCtl;

        if !found {
            std::ptr::write_bytes(ctl, 0, 1);
            (*ctl).n_shards.store(shards as u32, Ordering::Relaxed);
            (*ctl).max_bytes
                .store(RESERVATION_BYTES.load(Ordering::Relaxed), Ordering::Relaxed);

            // Allocate a private LWLock tranche id and register the
            // tranche name. The locks themselves live inline in
            // BlobCacheCtl::shard_locks (already zero-initialised by
            // write_bytes above); we call LWLockInitialize on each one.
            let tranche_id = pg_sys::LWLockNewTrancheId();
            (*ctl).lwlock_tranche_id.store(tranche_id, Ordering::Relaxed);
            pg_sys::LWLockRegisterTranche(tranche_id, SHMEM_NAME.as_ptr());
            for i in 0..shards {
                pg_sys::LWLockInitialize(
                    &mut (*ctl).shard_locks[i].lock,
                    tranche_id,
                );
            }

            // Create the DSA in place inside the named shmem block we
            // just reserved. The chunk is sized to the full
            // `blob_cache_mb` so the DSA never needs to grow. Backends
            // attach via `dsa_attach_in_place(dsa_chunk, NULL)`.
            let dsa_chunk = (block as *mut u8).add(std::mem::size_of::<BlobCacheCtl>());
            let dsa_size = RESERVATION_BYTES.load(Ordering::Relaxed) as usize;
            let area = dsa_create_in_place_compat(
                dsa_chunk as *mut std::ffi::c_void,
                dsa_size,
                tranche_id,
            );
            if !area.is_null() {
                // Mirror pgstat_shmem.c: pin so the area survives
                // all-backends-detached, cap actual size so the DSA
                // never tries to allocate a fresh DSM segment (which
                // is what blew up Approach B under Docker-for-Mac
                // tmpfs), then detach the postmaster-local handle —
                // each backend builds its own via dsa_attach_in_place.
                pg_sys::dsa_pin(area);
                pg_sys::dsa_set_size_limit(area, dsa_size);
                pg_sys::dsa_detach(area);
                (*ctl).dsa_ready.store(1, Ordering::Release);
            }
            (*ctl).initialized.store(1, Ordering::Release);
        }

        let _ = CTL_PTR.set(ctl as usize);
        CACHE_USABLE.store(true, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// Backend-side attach (lazy, called from get_pinned/insert)
// ---------------------------------------------------------------------------

/// Ensure this backend has resolved the ctl pointer and attached to the
/// DSA area. Returns `true` on success, `false` if the cache is not
/// usable in this backend.
fn attach() -> bool {
    if !CACHE_USABLE.load(Ordering::Acquire) {
        return false;
    }
    if CTL_PTR.get().is_none() {
        // Backend started after shmem_startup_hook fired in some other
        // process; we need to look up the named struct ourselves.
        unsafe {
            let shards = RESERVATION_SHARDS.load(Ordering::Relaxed) as usize;
            if shards == 0 {
                return false;
            }
            let mut found: bool = false;
            let block = pg_sys::ShmemInitStruct(
                SHMEM_NAME.as_ptr(),
                reservation_total_bytes(shards),
                &mut found as *mut bool,
            );
            if block.is_null() {
                return false;
            }
            let _ = CTL_PTR.set(block as usize);
        }
    }
    if DSA_AREA_PTR.get().is_none() {
        unsafe {
            let ctl = ctl_ref();
            while ctl.initialized.load(Ordering::Acquire) == 0 {
                std::hint::spin_loop();
            }
            if ctl.dsa_ready.load(Ordering::Acquire) == 0 {
                return false;
            }
            let block = *CTL_PTR.get().unwrap() as *mut u8;
            let dsa_chunk = block.add(std::mem::size_of::<BlobCacheCtl>());
            // `dsa_attach_in_place` palloc's the backend-local `dsa_area`
            // struct in CurrentMemoryContext. We call this from inside a
            // query, so that context is typically an executor/portal
            // context that gets freed at end-of-transaction — leaving
            // DSA_AREA_PTR dangling for the next query. Pin the struct
            // to TopMemoryContext, mirroring pgstat_shmem.c:225-234.
            let prev_ctx = pg_sys::MemoryContextSwitchTo(pg_sys::TopMemoryContext);
            let area = pg_sys::dsa_attach_in_place(
                dsa_chunk as *mut std::ffi::c_void,
                std::ptr::null_mut(),
            );
            if !area.is_null() {
                pg_sys::dsa_pin_mapping(area);
            }
            pg_sys::MemoryContextSwitchTo(prev_ctx);
            if area.is_null() {
                return false;
            }
            let _ = DSA_AREA_PTR.set(area as usize);
        }
    }
    if LWLOCK_BASE_PTR.get().is_none() {
        unsafe {
            let ctl = ctl_ref();
            // Each backend re-registers the tranche name so it shows
            // up in error messages and pg_stat_activity. The id was
            // assigned in postmaster's shmem_startup_hook.
            let tranche_id = ctl.lwlock_tranche_id.load(Ordering::Relaxed);
            pg_sys::LWLockRegisterTranche(tranche_id, SHMEM_NAME.as_ptr());
            let base = (*CTL_PTR.get().unwrap() as *mut BlobCacheCtl) as *mut u8;
            let off = std::mem::offset_of!(BlobCacheCtl, shard_locks);
            let lock_array = base.add(off) as *mut pg_sys::LWLockPadded;
            let _ = LWLOCK_BASE_PTR.set(lock_array as usize);
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn reservation_total_bytes(_n_shards: usize) -> usize {
    // Full in-place DSA chunk reserved up front so it never needs to
    // grow at runtime. Sized to `blob_cache_mb` (captured at startup
    // in RESERVATION_BYTES).
    std::mem::size_of::<BlobCacheCtl>() + RESERVATION_BYTES.load(Ordering::Relaxed) as usize
}

/// Cross-version wrapper for `dsa_create_in_place`. PG 17+ exposes only
/// the 6-arg `_ext`; the 4-arg form is a macro that fills in the
/// `init_segment_size` / `max_segment_size` defaults. Passing `0,0`
/// (as we did initially) violates the contract Assert'd at dsa.c:1233
/// and silently corrupts the control block in release builds. We
/// follow pgstat_shmem.c's pattern: pass the defaults, then cap actual
/// usage with `dsa_set_size_limit` after create.
unsafe fn dsa_create_in_place_compat(
    place: *mut std::ffi::c_void,
    size: usize,
    tranche_id: i32,
) -> *mut pg_sys::dsa_area {
    unsafe {
        #[cfg(any(feature = "pg17", feature = "pg18"))]
        {
            pg_sys::dsa_create_in_place_ext(
                place,
                size,
                tranche_id,
                std::ptr::null_mut(),
                DSA_DEFAULT_INIT_SEGMENT_SIZE,
                DSA_MAX_SEGMENT_SIZE,
            )
        }
        #[cfg(not(any(feature = "pg17", feature = "pg18")))]
        {
            pg_sys::dsa_create_in_place(place, size, tranche_id, std::ptr::null_mut())
        }
    }
}

#[inline]
fn hash_key(k: &BlobCacheKey) -> u64 {
    let mut h = (k.companion_oid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^= (k.segment_id as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    h ^= (k.col_idx as u64).wrapping_mul(0x94D0_49BB_1331_11EB);
    h ^= h >> 32;
    h
}

#[inline]
unsafe fn ctl_ref() -> &'static BlobCacheCtl {
    unsafe {
        let p = *CTL_PTR.get().expect("ctl ptr not yet set") as *const BlobCacheCtl;
        &*p
    }
}

#[inline]
unsafe fn dsa_area_ptr() -> *mut pg_sys::dsa_area {
    *DSA_AREA_PTR.get().expect("dsa area ptr not yet set") as *mut pg_sys::dsa_area
}

#[inline]
unsafe fn shard_lwlock(shard_idx: usize) -> *mut pg_sys::LWLock {
    unsafe {
        let base = *LWLOCK_BASE_PTR.get().expect("lwlock base not yet set")
            as *mut pg_sys::LWLockPadded;
        &mut (*base.add(shard_idx)).lock as *mut pg_sys::LWLock
    }
}

#[inline]
unsafe fn entry_ptr(dp: u64) -> *mut Entry {
    unsafe { pg_sys::dsa_get_address(dsa_area_ptr(), dp) as *mut Entry }
}

#[inline]
unsafe fn entry_ptr_mut(dp: u64) -> *mut Entry {
    unsafe { pg_sys::dsa_get_address(dsa_area_ptr(), dp) as *mut Entry }
}

#[inline]
unsafe fn bucket_head(buckets_dp: u64, idx: u32) -> u64 {
    unsafe {
        let buckets = pg_sys::dsa_get_address(dsa_area_ptr(), buckets_dp) as *const AtomicU64;
        (*buckets.add(idx as usize)).load(Ordering::Acquire)
    }
}

#[inline]
unsafe fn bucket_head_store(buckets_dp: u64, idx: u32, value: u64) {
    unsafe {
        let buckets = pg_sys::dsa_get_address(dsa_area_ptr(), buckets_dp) as *mut AtomicU64;
        (*buckets.add(idx as usize)).store(value, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// LRU list maintenance + eviction
//
// All four helpers below require the caller to hold the relevant shard's
// exclusive LWLock. Concurrent readers (under SHARED) only read entry
// fields they're known to be touching (bucket_next via Acquire, pin_count
// via AcqRel), and they cannot see partial LRU mutations because the
// shared/exclusive lock pair serialises us against any new bucket walks.
// ---------------------------------------------------------------------------

/// Push `new_entry_dp` onto the MRU end of the shard's LRU list.
/// Caller holds the shard's exclusive lock.
#[inline]
unsafe fn lru_prepend(shard: &Shard, new_entry_dp: u64, new_entry: *mut Entry) {
    unsafe {
        let old_head = shard.lru_head.load(Ordering::Acquire);
        (*new_entry).lru_prev.store(0, Ordering::Release);
        (*new_entry).lru_next.store(old_head, Ordering::Release);
        if old_head != 0 {
            let old_head_ptr = entry_ptr(old_head);
            (*old_head_ptr).lru_prev.store(new_entry_dp, Ordering::Release);
        } else {
            // List was empty — new entry is also the tail.
            shard.lru_tail.store(new_entry_dp, Ordering::Release);
        }
        shard.lru_head.store(new_entry_dp, Ordering::Release);
    }
}

/// Splice `target_dp` out of the shard's LRU list. Caller holds the
/// shard's exclusive lock.
#[inline]
unsafe fn unlink_from_lru(shard: &Shard, target_dp: u64, target: *mut Entry) {
    unsafe {
        let prev_dp = (*target).lru_prev.load(Ordering::Acquire);
        let next_dp = (*target).lru_next.load(Ordering::Acquire);

        if prev_dp != 0 {
            let prev = entry_ptr(prev_dp);
            (*prev).lru_next.store(next_dp, Ordering::Release);
        } else {
            // Target was the head.
            shard.lru_head.store(next_dp, Ordering::Release);
        }

        if next_dp != 0 {
            let next = entry_ptr(next_dp);
            (*next).lru_prev.store(prev_dp, Ordering::Release);
        } else {
            // Target was the tail.
            shard.lru_tail.store(prev_dp, Ordering::Release);
        }

        // Defensive: clear the target's own links so a stale read can't
        // walk back into the list. The entry's about to be dsa_free'd.
        let _ = target_dp;
        (*target).lru_prev.store(0, Ordering::Relaxed);
        (*target).lru_next.store(0, Ordering::Relaxed);
    }
}

/// Remove `target_dp` from its bucket chain. Caller holds the shard's
/// exclusive lock and has already computed `bucket_idx` from the entry's
/// key.
#[inline]
unsafe fn unlink_from_bucket(buckets_dp: u64, bucket_idx: u32, target_dp: u64) {
    unsafe {
        let head = bucket_head(buckets_dp, bucket_idx);
        if head == target_dp {
            let target = entry_ptr(target_dp);
            let next = (*target).bucket_next.load(Ordering::Acquire);
            bucket_head_store(buckets_dp, bucket_idx, next);
            return;
        }
        let mut cursor_dp = head;
        while cursor_dp != 0 {
            let cursor = entry_ptr(cursor_dp);
            let next_dp = (*cursor).bucket_next.load(Ordering::Acquire);
            if next_dp == target_dp {
                let target = entry_ptr(target_dp);
                let target_next = (*target).bucket_next.load(Ordering::Acquire);
                (*cursor).bucket_next.store(target_next, Ordering::Release);
                return;
            }
            cursor_dp = next_dp;
        }
        // Not found — invariant broken. We leak the entry rather than
        // corrupt the chain further; counters will be slightly off.
        debug_assert!(false, "evicted entry not found in bucket chain");
    }
}

/// Evict unpinned entries from the tail of `shard`'s LRU list until at
/// least `bytes_needed` worth of payload bytes have been freed or there
/// are no more candidates. Caller holds the shard's exclusive lock.
/// Returns the number of payload bytes actually freed.
unsafe fn evict_in_shard(
    ctl: &BlobCacheCtl,
    area: *mut pg_sys::dsa_area,
    shard: &Shard,
    _shard_idx: usize,
    bytes_needed: u64,
) -> u64 {
    unsafe {
        let mut freed: u64 = 0;
        let buckets_dp = shard.bucket_array.load(Ordering::Acquire);
        let mut cur = shard.lru_tail.load(Ordering::Acquire);
        while freed < bytes_needed && cur != 0 {
            let entry = entry_ptr(cur);
            // Capture predecessor before potential free.
            let prev = (*entry).lru_prev.load(Ordering::Acquire);

            if (*entry).pin_count.load(Ordering::Acquire) == 0 {
                // Unlink from the bucket chain. The shard's bucket_array
                // must exist if there are any entries; assert in debug.
                debug_assert!(buckets_dp != 0);
                let key = (*entry).key;
                let h = hash_key(&key);
                let bucket_idx = (h as u32) & (BUCKETS_PER_SHARD - 1);
                unlink_from_bucket(buckets_dp, bucket_idx, cur);

                // Unlink from LRU list.
                unlink_from_lru(shard, cur, entry);

                // Capture sizing + ptrs before freeing.
                let dlen = (*entry).data_len as u64;
                let dptr = (*entry).data_ptr.load(Ordering::Acquire);

                // Free DSA allocations.
                if dptr != 0 {
                    pg_sys::dsa_free(area, dptr);
                }
                pg_sys::dsa_free(area, cur);

                // Counters: per-shard first, then global. Relaxed is
                // sufficient — the lock around us provides the only
                // ordering guarantee we need.
                shard.bytes_used.fetch_sub(dlen, Ordering::Relaxed);
                shard.n_entries.fetch_sub(1, Ordering::Relaxed);
                ctl.total_bytes.fetch_sub(dlen, Ordering::Relaxed);
                ctl.n_entries.fetch_sub(1, Ordering::Relaxed);
                ctl.evictions_total.fetch_add(1, Ordering::Relaxed);
                freed = freed.saturating_add(dlen);
            }
            // Pinned entries are skipped — they stay in the LRU list and
            // get re-evaluated on the next eviction round.
            cur = prev;
        }
        freed
    }
}
