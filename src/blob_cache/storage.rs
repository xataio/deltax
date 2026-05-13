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

/// Initial in-place DSA chunk size. Small (1 MiB) — the DSA grows from
/// here by allocating DSM segments up to the configured limit.
const DSA_INITIAL_BYTES: usize = 1024 * 1024;

/// Maximum number of shards we provision space for. The actual number
/// in use is taken from `configured_shards()`; unused slots are zeroed.
const MAX_SHARDS: usize = 1024;

/// Per-shard bucket count. Power of two. Chosen so that buckets stay
/// short for typical working sets (a few thousand entries per shard).
const BUCKETS_PER_SHARD: u32 = 256;

/// Shmem block name (also used as the LWLock tranche name).
const SHMEM_NAME: &std::ffi::CStr = c"pg_deltax_blob_cache";

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

    /// DSA handle (dsm_handle is u32; widened to u64 for atomic ergonomics).
    dsa_handle: AtomicU64,

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

    shards: [Shard; MAX_SHARDS],
}

/// Per-shard metadata. The bucket array lives in DSA (lazily allocated
/// on first use).
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
}

const _: () = {
    // Catch accidental size blowups at compile time.
    assert!(std::mem::size_of::<Shard>() == 24);
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
    /// Used for LRU eviction sampling.
    last_used: AtomicU64,

    /// dsa_pointer to next entry in the same bucket chain (0 = end).
    bucket_next: AtomicU64,
}

const _: () = {
    // 16 bytes key+pad, 16 data ptr/len/pad, 8 pin+pad, 8 last_used, 8 next.
    assert!(std::mem::size_of::<Entry>() == 56);
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

        let max_bytes = ctl.max_bytes.load(Ordering::Relaxed);
        let cur = ctl.total_bytes.load(Ordering::Relaxed);
        if cur.saturating_add(bytes.len() as u64) > max_bytes {
            // No eviction yet — drop the insert.
            ctl.insert_failures_total.fetch_add(1, Ordering::Relaxed);
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
                pg_sys::DSA_ALLOC_ZERO as i32,
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

        // Allocate Entry + data payload.
        let entry_size = std::mem::size_of::<Entry>();
        let new_entry_dp = pg_sys::dsa_allocate_extended(
            area,
            entry_size,
            pg_sys::DSA_ALLOC_ZERO as i32,
        );
        if new_entry_dp == 0 {
            pg_sys::LWLockRelease(lock);
            ctl.insert_failures_total.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let data_dp = pg_sys::dsa_allocate_extended(
            area,
            bytes.len(),
            0,
        );
        if data_dp == 0 {
            pg_sys::dsa_free(area, new_entry_dp);
            pg_sys::LWLockRelease(lock);
            ctl.insert_failures_total.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let data_ptr = pg_sys::dsa_get_address(area, data_dp) as *mut u8;
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), data_ptr, bytes.len());

        // Initialise the entry. dsa_allocate_extended with DSA_ALLOC_ZERO
        // already zeroed the memory, so atomics start at 0.
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

        shard.n_entries.fetch_add(1, Ordering::Relaxed);
        shard.bytes_used
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        ctl.n_entries.fetch_add(1, Ordering::Relaxed);
        ctl.total_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);

        pg_sys::LWLockRelease(lock);
    }
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
        // Cache disabled — skip shmem registration entirely so we don't
        // reserve memory we'll never use.
        return;
    }

    unsafe {
        #[cfg(feature = "pg14")]
        {
            // PG 14 has no shmem_request_hook; the request calls must
            // happen during _PG_init while the postmaster is still
            // setting up shmem.
            pg_sys::RequestAddinShmemSpace(reservation_total_bytes(shards));
            pg_sys::RequestNamedLWLockTranche(SHMEM_NAME.as_ptr(), shards as i32);
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
        pg_sys::RequestNamedLWLockTranche(SHMEM_NAME.as_ptr(), shards as i32);
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
            // Postmaster will have already errored; nothing to do.
            return;
        }
        let ctl = block as *mut BlobCacheCtl;
        let dsa_chunk = (block as *mut u8).add(std::mem::size_of::<BlobCacheCtl>());

        if !found {
            // Zero-initialise then set the fields we care about. ShmemInitStruct
            // hands back uninitialized memory the first time.
            std::ptr::write_bytes(ctl, 0, 1);
            (*ctl).n_shards.store(shards as u32, Ordering::Relaxed);
            (*ctl).max_bytes
                .store(RESERVATION_BYTES.load(Ordering::Relaxed), Ordering::Relaxed);

            // Get the tranche id (the LWLocks themselves are initialised
            // by PG core because we called RequestNamedLWLockTranche).
            // We only stash the tranche id in shmem — the LWLockPadded*
            // pointer must be re-fetched in each backend so PG can
            // register the tranche name in the backend-local lookup.
            let pad = pg_sys::GetNamedLWLockTranche(SHMEM_NAME.as_ptr());
            if !pad.is_null() {
                let tranche_id = (*pad).lock.tranche as i32;
                (*ctl).lwlock_tranche_id.store(tranche_id, Ordering::Relaxed);
            }

            // Create the DSA in place. The tranche id is the same as the
            // shard LWLock tranche — DSA uses LWLocks internally and
            // expects a tranche id we own.
            //
            // The returned `*dsa_area` is process-local: it points into
            // the postmaster's heap. We deliberately do NOT stash it in
            // `DSA_AREA_PTR` here — each backend must call
            // `dsa_attach_in_place` itself to get its own handle.
            let dsa_tranche_id = (*ctl).lwlock_tranche_id.load(Ordering::Relaxed);
            let area = dsa_create_in_place_compat(
                dsa_chunk as *mut std::ffi::c_void,
                DSA_INITIAL_BYTES,
                dsa_tranche_id,
            );
            if !area.is_null() {
                pg_sys::dsa_pin(area);
                pg_sys::dsa_set_size_limit(
                    area,
                    RESERVATION_BYTES.load(Ordering::Relaxed) as usize,
                );
                let handle = pg_sys::dsa_get_handle(area);
                (*ctl).dsa_handle.store(handle as u64, Ordering::Release);
            }
            (*ctl).initialized.store(1, Ordering::Release);
        }

        // CTL_PTR lives in shmem at the same address in all processes,
        // so it's safe to inherit via OnceLock across fork. (Even if
        // backends end up re-resolving it via ShmemInitStruct in
        // `attach()`, the result is identical.)
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
            // Wait briefly if another backend is still initialising.
            while ctl.initialized.load(Ordering::Acquire) == 0 {
                std::hint::spin_loop();
            }
            let handle = ctl.dsa_handle.load(Ordering::Acquire);
            if handle == 0 {
                return false;
            }
            let block = *CTL_PTR.get().unwrap() as *mut u8;
            let dsa_chunk = block.add(std::mem::size_of::<BlobCacheCtl>());
            let area = pg_sys::dsa_attach_in_place(
                dsa_chunk as *mut std::ffi::c_void,
                std::ptr::null_mut(),
            );
            if area.is_null() {
                return false;
            }
            pg_sys::dsa_pin_mapping(area);
            let _ = DSA_AREA_PTR.set(area as usize);
        }
    }
    if LWLOCK_BASE_PTR.get().is_none() {
        unsafe {
            let pad = pg_sys::GetNamedLWLockTranche(SHMEM_NAME.as_ptr());
            if pad.is_null() {
                return false;
            }
            let _ = LWLOCK_BASE_PTR.set(pad as usize);
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn reservation_total_bytes(n_shards: usize) -> usize {
    let _ = n_shards; // currently fixed-size; kept for future variable layout.
    std::mem::size_of::<BlobCacheCtl>() + DSA_INITIAL_BYTES
}

/// Cross-version wrapper for `dsa_create_in_place`. PG 17+ replaced the
/// 4-arg `dsa_create_in_place` with the 6-arg `dsa_create_in_place_ext`
/// that additionally takes an initial and maximum segment size; passing
/// `0` for both means "use PG's defaults".
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
                0,
                0,
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
