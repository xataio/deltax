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
/// Maximum bloom filter size in bytes. Segments at the cap (ndistinct above
/// ~6.5K) get fewer than the designed 10 bits/element, but the measured FPR
/// on high-cardinality real data (ClickBench UserID) is still only ~3.5%,
/// and a larger cap makes the per-survivor bloom detoast during the scan's
/// bloom phase proportionally more expensive.
const MAX_BLOOM_BYTES: usize = 8192;

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
    fn test_optimal_k() {
        // At 10 bits/element, optimal k ≈ 10*ln(2) ≈ 6.93 → 7
        assert_eq!(optimal_k(10000, 1000), 7);
        // At 1 bit/element
        assert_eq!(optimal_k(1000, 1000), 1);
    }
}
