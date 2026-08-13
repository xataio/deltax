//! Software-prefetch primitive for the DRAM-bound hash probe loops
//! (count-floor filter, group maps). See PERF_IMPROVEMENTS.md.
//!
//! A random probe into a GiB-scale structure is a guaranteed cache miss;
//! issuing the address a few rows ahead of use overlaps the misses
//! instead of serializing them. The hint is advisory on every
//! architecture — dropped prefetches cost nothing, so callers never need
//! an arch gate.

/// Hint the cache hierarchy to load the line containing `p` for reading.
/// No-op on architectures without a stable prefetch primitive.
#[inline(always)]
pub(super) fn prefetch_read(p: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
        _mm_prefetch::<_MM_HINT_T0>(p as *const i8);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!(
            "prfm pldl1keep, [{0}]",
            in(reg) p,
            options(nostack, preserves_flags),
        );
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let _ = p;
    }
}
