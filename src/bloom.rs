// Dynamic per-segment bloom filters for equality predicate pushdown.
//
// Bloom filter size is proportional to ndistinct: 10 bits per element,
// giving FPR ~0.8% with optimal k. Capped at 8KB, minimum 64 bytes.
// Total overhead is roughly 5-10% of compressed data.

use std::hash::{BuildHasher, Hasher};

/// Bits allocated per distinct element. 10 bits/element → FPR ~0.8%.
const BITS_PER_ELEMENT: usize = 10;
/// Minimum bloom filter size in bytes.
const MIN_BLOOM_BYTES: usize = 64;
/// Maximum bloom filter size in bytes.
const MAX_BLOOM_BYTES: usize = 8192;

/// Sentinel `_segment_id` for the partition-level bloom row in the blooms
/// companion table. Probed by PK in Phase 0pre of `load_segments_heap`:
/// a rejecting sentinel skips the partition's colstats probes, meta scan,
/// and every per-segment bloom.
pub const PARTITION_BLOOM_SEGMENT_ID: i32 = -1;
/// Hash count for partition-level blooms. Fixed (rather than derived from
/// ndistinct) so filters built at different times stay OR-mergeable.
pub const PARTITION_BLOOM_HASHES: u8 = 4;
/// In-memory build size for partition-level blooms (power of two so the
/// filter can be folded down before storage). 2 MiB ≈ 16.8M bits keeps a
/// ~1.5M-distinct column near 1% FPR and a ~3.5M-distinct one near 10%.
pub const PARTITION_BLOOM_BUILD_BYTES: usize = 2 * 1024 * 1024;
/// Smallest stored partition bloom after folding.
pub const PARTITION_BLOOM_MIN_BYTES: usize = 64;
/// Stored-density ceiling: above this fraction of set bits the filter's FPR
/// is too high to prune anything, so it isn't worth a row. This is the only
/// storage filter — every bloom-supported column accumulates a partition
/// bloom (a per-segment cardinality gate was tried first and dropped: on
/// sort-clustered columns like ClickBench UserID under
/// `order_by = [counterid, userid, ...]`, single segments routinely show low
/// local ndistinct, which disqualified exactly the columns point lookups
/// target).
pub const PARTITION_BLOOM_MAX_DENSITY: f64 = 0.6;

/// Smallest power-of-two byte size that keeps ~`BITS_PER_ELEMENT` bits per
/// element, clamped to the partition-bloom build range.
pub fn partition_bloom_target_bytes(ndistinct: u64) -> usize {
    let bits = ndistinct.saturating_mul(BITS_PER_ELEMENT as u64);
    let bytes = (bits.div_ceil(8).max(1) as usize).next_power_of_two();
    bytes.clamp(PARTITION_BLOOM_MIN_BYTES, PARTITION_BLOOM_BUILD_BYTES)
}

/// Fixed seeds for deterministic hashing across compression and query time.
const SEED1: u64 = 0x517cc1b727220a95;
const SEED2: u64 = 0x6c62272e07bb0142;
const SEED3: u64 = 0x9e3779b97f4a7c15;
const SEED4: u64 = 0xf39cc0605cedc834;

fn make_hasher() -> ahash::RandomState {
    ahash::RandomState::with_seeds(SEED1, SEED2, SEED3, SEED4)
}

/// Hash a datum value to a u64 for bloom filter insertion/lookup.
/// The value should be passed as its raw i64 representation
/// (i16/i32 sign-extended, f32/f64 as bits, timestamps as epoch micros).
pub fn hash_datum_i64(value: i64) -> u64 {
    let state = make_hasher();
    let mut h = state.build_hasher();
    h.write_i64(value);
    h.finish()
}

/// Compute optimal number of hash functions: k = (m/n) * ln(2), clamped to [1, 10].
fn optimal_k(num_bits: usize, ndistinct: usize) -> u8 {
    if ndistinct == 0 {
        return 1;
    }
    let k = ((num_bits as f64 / ndistinct as f64) * core::f64::consts::LN_2).round() as u8;
    k.clamp(1, 10)
}

/// Compute bloom filter size in bytes for a given ndistinct.
pub fn bloom_size_for_ndistinct(ndistinct: usize) -> usize {
    let bits = ndistinct.saturating_mul(BITS_PER_ELEMENT);
    let bytes = bits.div_ceil(8);
    bytes.clamp(MIN_BLOOM_BYTES, MAX_BLOOM_BYTES)
}

pub struct BloomFilter {
    bits: Vec<u8>,
    num_hashes: u8,
}

impl BloomFilter {
    /// Create a new bloom filter sized for the expected number of distinct values.
    pub fn for_ndistinct(ndistinct: usize) -> Self {
        let size = bloom_size_for_ndistinct(ndistinct);
        let num_hashes = optimal_k(size * 8, ndistinct);
        Self {
            bits: vec![0u8; size],
            num_hashes,
        }
    }

    /// Reconstruct from stored bytes and hash count.
    pub fn from_bytes(data: &[u8], num_hashes: u8) -> Self {
        Self {
            bits: data.to_vec(),
            num_hashes,
        }
    }

    /// Create an empty filter of an exact power-of-two byte size. Used for
    /// partition-level blooms, which must be foldable (see `fold_to`).
    pub fn with_bytes(num_bytes: usize, num_hashes: u8) -> Self {
        debug_assert!(num_bytes.is_power_of_two());
        Self {
            bits: vec![0u8; num_bytes],
            num_hashes,
        }
    }

    /// Fold a power-of-two-sized filter down to `target_bytes` by OR-ing the
    /// upper half into the lower half. Because bit positions are
    /// `hash % num_bits` and the size stays a power of two, every value
    /// inserted before the fold is still reported present afterwards (no
    /// false negatives); only the false-positive rate grows.
    pub fn fold_to(&mut self, target_bytes: usize) {
        debug_assert!(self.bits.len().is_power_of_two());
        debug_assert!(target_bytes.is_power_of_two());
        while self.bits.len() > target_bytes.max(1) {
            let half = self.bits.len() / 2;
            for i in 0..half {
                self.bits[i] |= self.bits[i + half];
            }
            self.bits.truncate(half);
        }
    }

    /// Fraction of set bits. ~0.33 at 10 bits/element with 4 hashes; values
    /// near 1.0 mean the filter is saturated and prunes nothing.
    pub fn density(&self) -> f64 {
        if self.bits.is_empty() {
            return 1.0;
        }
        let ones: u64 = self.bits.iter().map(|b| b.count_ones() as u64).sum();
        ones as f64 / (self.bits.len() as f64 * 8.0)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bits
    }

    pub fn num_hashes(&self) -> u8 {
        self.num_hashes
    }

    /// Compute bit positions for a given hash using double hashing.
    #[inline]
    fn bit_positions(&self, hash: u64, out: &mut [usize; 10]) -> usize {
        let num_bits = self.bits.len() * 8;
        let h1 = (hash >> 32) as u32;
        let h2 = hash as u32;
        let k = self.num_hashes as usize;
        for (i, slot) in out[..k].iter_mut().enumerate() {
            *slot = h1.wrapping_add(h2.wrapping_mul(i as u32)) as usize % num_bits;
        }
        k
    }

    /// Insert a value (by its pre-computed hash) into the filter.
    pub fn insert(&mut self, hash: u64) {
        let mut positions = [0usize; 10];
        let k = self.bit_positions(hash, &mut positions);
        for &pos in &positions[..k] {
            self.bits[pos / 8] |= 1 << (pos % 8);
        }
    }

    /// OR-merge `other` into `self`, folding both down to the smaller of the
    /// two sizes first. Returns `false` (and leaves `self` untouched) when the
    /// filters are not fold-compatible: different `num_hashes`, or either size
    /// is not a power of two. Because positions are `hash % 2^n` and folding
    /// OR-halves, the merged filter reports every value inserted into either
    /// input as present — no false negatives (PERF #47 invariant).
    ///
    /// No production caller yet: sentinels are written only at compress
    /// time and dropped wholesale on decompress (DML on compressed
    /// partitions is rejected). The incremental-compaction path that folds
    /// new batches into existing sentinels lands separately and uses this.
    #[allow(dead_code)]
    pub fn merge_fold(&mut self, other: &BloomFilter) -> bool {
        if self.num_hashes != other.num_hashes
            || self.bits.is_empty()
            || other.bits.is_empty()
            || !self.bits.len().is_power_of_two()
            || !other.bits.len().is_power_of_two()
        {
            return false;
        }
        let target = self.bits.len().min(other.bits.len());
        self.fold_to(target);
        let mut folded;
        let other_bits: &[u8] = if other.bits.len() > target {
            folded = BloomFilter {
                bits: other.bits.clone(),
                num_hashes: other.num_hashes,
            };
            folded.fold_to(target);
            &folded.bits
        } else {
            &other.bits
        };
        for (a, b) in self.bits.iter_mut().zip(other_bits.iter()) {
            *a |= b;
        }
        true
    }

    /// Check if a value might be in the filter. False = definitely not present.
    pub fn might_contain(&self, hash: u64) -> bool {
        let mut positions = [0usize; 10];
        let k = self.bit_positions(hash, &mut positions);
        for &pos in &positions[..k] {
            if self.bits[pos / 8] & (1 << (pos % 8)) == 0 {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_basic() {
        let mut bf = BloomFilter::for_ndistinct(100);
        let h = hash_datum_i64(42);
        assert!(!bf.might_contain(h));
        bf.insert(h);
        assert!(bf.might_contain(h));
    }

    #[test]
    fn test_bloom_no_false_negatives() {
        let mut bf = BloomFilter::for_ndistinct(200);
        let values: Vec<i64> = (0..200).collect();
        for &v in &values {
            bf.insert(hash_datum_i64(v));
        }
        for &v in &values {
            assert!(
                bf.might_contain(hash_datum_i64(v)),
                "false negative for {}",
                v
            );
        }
    }

    #[test]
    fn test_bloom_no_false_negatives_large() {
        // Test with ndistinct similar to ClickBench userid
        let mut bf = BloomFilter::for_ndistinct(5000);
        let values: Vec<i64> = (0..5000).collect();
        for &v in &values {
            bf.insert(hash_datum_i64(v));
        }
        for &v in &values {
            assert!(
                bf.might_contain(hash_datum_i64(v)),
                "false negative for {}",
                v
            );
        }
    }

    #[test]
    fn test_bloom_fpr_large() {
        // Verify FPR is reasonable at ndistinct=5000
        let mut bf = BloomFilter::for_ndistinct(5000);
        for i in 0..5000i64 {
            bf.insert(hash_datum_i64(i));
        }
        let mut false_positives = 0;
        let test_count = 10000;
        for i in 100_000..100_000 + test_count {
            if bf.might_contain(hash_datum_i64(i)) {
                false_positives += 1;
            }
        }
        let fpr = false_positives as f64 / test_count as f64;
        // At 10 bits/element with optimal k, expect FPR ~0.8%, allow up to 3%
        assert!(fpr < 0.03, "FPR too high: {:.1}%", fpr * 100.0);
    }

    #[test]
    fn test_bloom_serialization_roundtrip() {
        let mut bf = BloomFilter::for_ndistinct(500);
        for i in 0..100 {
            bf.insert(hash_datum_i64(i));
        }
        let bytes = bf.as_bytes().to_vec();
        let k = bf.num_hashes();
        let bf2 = BloomFilter::from_bytes(&bytes, k);
        assert_eq!(bf.bits, bf2.bits);
        assert_eq!(bf.num_hashes, bf2.num_hashes);
    }

    #[test]
    fn test_bloom_sizing() {
        assert_eq!(bloom_size_for_ndistinct(1), MIN_BLOOM_BYTES); // tiny
        assert_eq!(bloom_size_for_ndistinct(100), 125); // 100*10/8
        assert_eq!(bloom_size_for_ndistinct(5000), 6250); // 5000*10/8
        assert_eq!(bloom_size_for_ndistinct(100000), MAX_BLOOM_BYTES); // capped
    }

    #[test]
    fn test_fold_no_false_negatives() {
        let mut bf = BloomFilter::with_bytes(1 << 16, PARTITION_BLOOM_HASHES);
        let values: Vec<i64> = (0..5000).map(|i| i * 7919 + 13).collect();
        for &v in &values {
            bf.insert(hash_datum_i64(v));
        }
        bf.fold_to(1 << 13);
        assert_eq!(bf.as_bytes().len(), 1 << 13);
        for &v in &values {
            assert!(
                bf.might_contain(hash_datum_i64(v)),
                "false negative after fold for {}",
                v
            );
        }
    }

    #[test]
    fn test_fold_to_larger_is_noop() {
        let mut bf = BloomFilter::with_bytes(1 << 10, PARTITION_BLOOM_HASHES);
        bf.insert(hash_datum_i64(1));
        bf.fold_to(1 << 12);
        assert_eq!(bf.as_bytes().len(), 1 << 10);
    }

    #[test]
    fn test_fold_keeps_useful_fpr() {
        // 20K distinct folded to the 10-bits/element target keeps FPR low.
        let n = 20_000i64;
        let mut bf = BloomFilter::with_bytes(PARTITION_BLOOM_BUILD_BYTES, PARTITION_BLOOM_HASHES);
        for i in 0..n {
            bf.insert(hash_datum_i64(i));
        }
        bf.fold_to(partition_bloom_target_bytes(n as u64));
        assert!(bf.density() < PARTITION_BLOOM_MAX_DENSITY);
        let mut false_positives = 0;
        let probes = 10_000;
        for i in 1_000_000..1_000_000 + probes {
            if bf.might_contain(hash_datum_i64(i)) {
                false_positives += 1;
            }
        }
        let fpr = false_positives as f64 / probes as f64;
        assert!(fpr < 0.10, "FPR too high after fold: {:.1}%", fpr * 100.0);
    }

    #[test]
    fn test_merge_fold_no_false_negatives() {
        // Sentinel (smaller, already folded) merged with a fresh accumulator
        // (larger build size) must keep every value from both sides.
        let mut sentinel = BloomFilter::with_bytes(1 << 10, PARTITION_BLOOM_HASHES);
        let old_values: Vec<i64> = (0..500).map(|i| i * 31 + 7).collect();
        for &v in &old_values {
            sentinel.insert(hash_datum_i64(v));
        }
        let mut acc = BloomFilter::with_bytes(1 << 14, PARTITION_BLOOM_HASHES);
        let new_values: Vec<i64> = (1_000_000..1_000_500).collect();
        for &v in &new_values {
            acc.insert(hash_datum_i64(v));
        }
        assert!(sentinel.merge_fold(&acc));
        assert_eq!(sentinel.as_bytes().len(), 1 << 10);
        for &v in old_values.iter().chain(new_values.iter()) {
            assert!(
                sentinel.might_contain(hash_datum_i64(v)),
                "false negative after merge_fold for {}",
                v
            );
        }
    }

    #[test]
    fn test_merge_fold_rejects_incompatible() {
        let mut a = BloomFilter::with_bytes(1 << 10, PARTITION_BLOOM_HASHES);
        // Different num_hashes — must refuse.
        let b = BloomFilter::with_bytes(1 << 10, PARTITION_BLOOM_HASHES + 1);
        assert!(!a.merge_fold(&b));
        // Non-power-of-two size — must refuse.
        let c = BloomFilter::from_bytes(&[0u8; 100], PARTITION_BLOOM_HASHES);
        assert!(!a.merge_fold(&c));
    }

    #[test]
    fn test_partition_bloom_target_bytes() {
        assert_eq!(partition_bloom_target_bytes(0), PARTITION_BLOOM_MIN_BYTES);
        assert_eq!(partition_bloom_target_bytes(100_000), 1 << 17); // 1Mbit → 128KB
        assert_eq!(
            partition_bloom_target_bytes(100_000_000),
            PARTITION_BLOOM_BUILD_BYTES
        );
    }

    #[test]
    fn test_optimal_k() {
        // At 10 bits/element, optimal k ≈ 10*ln(2) ≈ 6.93 → 7
        assert_eq!(optimal_k(10000, 1000), 7);
        // At 1 bit/element
        assert_eq!(optimal_k(1000, 1000), 1);
    }
}
