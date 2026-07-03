//! COUNT(DISTINCT) accumulator helpers.
//!
//! - `CdSetInt` / `CdSetStr` — `hashbrown::HashSet` with ahash, the
//!   insert hot path for COUNT(DISTINCT). Swapped from
//!   `std::collections::HashSet` (SipHash) for ~2-3× faster inserts;
//!   the serial CD merge on Q4 goes from ~2.5 s to ~1 s as a result.
//! - `hash128_str` — collapses strings to a 128-bit digest so the
//!   string-CD set stores fixed-size keys (same approach as
//!   ClickHouse's `uniqExact`). Two AHasher instances with different
//!   fixed seeds produce independent halves; collision probability is
//!   negligible for any practical cardinality.

use std::hash::BuildHasherDefault;

pub(super) type CdSetInt = hashbrown::HashSet<i64, BuildHasherDefault<ahash::AHasher>>;
pub(super) type CdSetStr = hashbrown::HashSet<u128, BuildHasherDefault<ahash::AHasher>>;

#[inline]
pub(super) fn new_cd_set_int() -> CdSetInt {
    CdSetInt::with_hasher(BuildHasherDefault::default())
}

#[inline]
pub(super) fn new_cd_set_str() -> CdSetStr {
    CdSetStr::with_hasher(BuildHasherDefault::default())
}

/// Fixed-seed hasher states for `hash128_str`. Built once instead of being
/// reconstructed from seeds on every call (`with_seeds` does real key-mixing
/// work; this runs per distinct string). Seeds are unchanged, so the hash
/// output is byte-identical to the previous per-call construction.
static CD_HASH_S1: std::sync::LazyLock<ahash::RandomState> = std::sync::LazyLock::new(|| {
    ahash::RandomState::with_seeds(0xa1b2c3d4, 0xe5f6a7b8, 0x11223344, 0x55667788)
});
static CD_HASH_S2: std::sync::LazyLock<ahash::RandomState> = std::sync::LazyLock::new(|| {
    ahash::RandomState::with_seeds(0x1234abcd, 0x5678ef01, 0xaabbccdd, 0xeeff0011)
});

/// Compute a 128-bit hash of a byte slice for COUNT(DISTINCT) on strings.
pub(super) fn hash128_str(data: &[u8]) -> u128 {
    use std::hash::{BuildHasher, Hasher};
    let mut h1 = CD_HASH_S1.build_hasher();
    h1.write(data);
    let lo = h1.finish();
    let mut h2 = CD_HASH_S2.build_hasher();
    h2.write(data);
    let hi = h2.finish();
    (hi as u128) << 64 | lo as u128
}
