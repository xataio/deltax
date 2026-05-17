//! Frame-of-Reference (FOR) + bit-packing and Constant encoding for integer columns.
//!
//! ## Constant (CompressionType tag = 7)
//! All non-null values are identical → store a single value.
//! Format (i32): `[4B value LE]`
//! Format (i64): `[8B value LE]`
//!
//! ## FOR + Bit-packing (CompressionType tag = 8)
//! Store min value, then pack `(value - min)` using minimum bits.
//! Format (i32): `[4B min_value LE][1B bit_width][packed bits]`
//! Format (i64): `[8B min_value LE][1B bit_width][packed bits]`
//!
//! Packed bits are LSB-first within each byte, byte-aligned at the end.

use super::CompressionType;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Minimum number of bits to represent `range` (the max offset from min).
/// Returns 0 when range == 0 (all values equal to min).
fn bit_width_for_range(range: u64) -> u8 {
    if range == 0 {
        return 0;
    }
    (64 - range.leading_zeros()) as u8
}

/// Pack `values` (each fitting in `bits` bits) into a byte vector, LSB-first.
fn pack_bits_u32(values: &[u32], bits: u8) -> Vec<u8> {
    if bits == 0 || values.is_empty() {
        return Vec::new();
    }
    let total_bits = values.len() as u64 * bits as u64;
    let byte_len = total_bits.div_ceil(8) as usize;
    let mut buf = vec![0u8; byte_len];
    let mut bit_pos: u64 = 0;
    for &v in values {
        let mut remaining = bits;
        let mut val = v;
        let mut bp = bit_pos;
        while remaining > 0 {
            let byte_idx = (bp / 8) as usize;
            let bit_offset = (bp % 8) as u8;
            let space = 8 - bit_offset;
            let to_write = remaining.min(space);
            let mask = if to_write == 32 {
                u32::MAX
            } else {
                (1u32 << to_write) - 1
            };
            buf[byte_idx] |= ((val & mask) as u8) << bit_offset;
            val >>= to_write;
            remaining -= to_write;
            bp += to_write as u64;
        }
        bit_pos += bits as u64;
    }
    buf
}

/// Unpack `count` values of `bits` bits each from a byte slice.
fn unpack_bits_u32(data: &[u8], count: usize, bits: u8) -> Vec<u32> {
    if bits == 0 || count == 0 {
        return vec![0u32; count];
    }

    // Byte-aligned fast paths; see `unpack_bits_u64` for rationale.
    match bits {
        8 => {
            return data[..count].iter().map(|&b| b as u32).collect();
        }
        16 => {
            let mut result = Vec::with_capacity(count);
            for chunk in data[..count * 2].chunks_exact(2) {
                result.push(u16::from_le_bytes(chunk.try_into().unwrap()) as u32);
            }
            return result;
        }
        32 => {
            let mut result = Vec::with_capacity(count);
            for chunk in data[..count * 4].chunks_exact(4) {
                result.push(u32::from_le_bytes(chunk.try_into().unwrap()));
            }
            return result;
        }
        _ => {}
    }

    let mut result = Vec::with_capacity(count);
    let mut bit_pos: u64 = 0;
    for _ in 0..count {
        let mut val: u32 = 0;
        let mut remaining = bits;
        let mut bp = bit_pos;
        let mut shift = 0u8;
        while remaining > 0 {
            let byte_idx = (bp / 8) as usize;
            let bit_offset = (bp % 8) as u8;
            let space = 8 - bit_offset;
            let to_read = remaining.min(space);
            let mask = if to_read == 8 {
                0xFF
            } else {
                (1u8 << to_read) - 1
            };
            let bits_val = (data[byte_idx] >> bit_offset) & mask;
            val |= (bits_val as u32) << shift;
            remaining -= to_read;
            bp += to_read as u64;
            shift += to_read;
        }
        result.push(val);
        bit_pos += bits as u64;
    }
    result
}

/// Pack u64 values.
fn pack_bits_u64(values: &[u64], bits: u8) -> Vec<u8> {
    if bits == 0 || values.is_empty() {
        return Vec::new();
    }
    let total_bits = values.len() as u64 * bits as u64;
    let byte_len = total_bits.div_ceil(8) as usize;
    let mut buf = vec![0u8; byte_len];
    let mut bit_pos: u64 = 0;
    for &v in values {
        let mut remaining = bits;
        let mut val = v;
        let mut bp = bit_pos;
        while remaining > 0 {
            let byte_idx = (bp / 8) as usize;
            let bit_offset = (bp % 8) as u8;
            let space = 8 - bit_offset;
            let to_write = remaining.min(space);
            let mask = if to_write == 64 {
                u64::MAX
            } else {
                (1u64 << to_write) - 1
            };
            buf[byte_idx] |= ((val & mask) as u8) << bit_offset;
            val >>= to_write;
            remaining -= to_write;
            bp += to_write as u64;
        }
        bit_pos += bits as u64;
    }
    buf
}

/// Unpack u64 values.
fn unpack_bits_u64(data: &[u8], count: usize, bits: u8) -> Vec<u64> {
    if bits == 0 || count == 0 {
        return vec![0u64; count];
    }

    // Fast paths for byte-aligned widths. The general bit-loop below reads
    // one byte per inner iteration even when `bits % 8 == 0`, which burns
    // ~8 inner iterations × ~10 arith ops per value — material on queries
    // that decompress high-cardinality i64 columns stored at bits=64
    // (hash columns like URLHash/RefererHash/UserID). See
    // `QUERY_ANALYSIS.md` #48 investigation.
    match bits {
        8 => {
            return data[..count].iter().map(|&b| b as u64).collect();
        }
        16 => {
            let mut result = Vec::with_capacity(count);
            for chunk in data[..count * 2].chunks_exact(2) {
                result.push(u16::from_le_bytes(chunk.try_into().unwrap()) as u64);
            }
            return result;
        }
        32 => {
            let mut result = Vec::with_capacity(count);
            for chunk in data[..count * 4].chunks_exact(4) {
                result.push(u32::from_le_bytes(chunk.try_into().unwrap()) as u64);
            }
            return result;
        }
        64 => {
            let mut result = Vec::with_capacity(count);
            for chunk in data[..count * 8].chunks_exact(8) {
                result.push(u64::from_le_bytes(chunk.try_into().unwrap()));
            }
            return result;
        }
        _ => {}
    }

    let mut result = Vec::with_capacity(count);
    let mut bit_pos: u64 = 0;
    for _ in 0..count {
        let mut val: u64 = 0;
        let mut remaining = bits;
        let mut bp = bit_pos;
        let mut shift = 0u8;
        while remaining > 0 {
            let byte_idx = (bp / 8) as usize;
            let bit_offset = (bp % 8) as u8;
            let space = 8 - bit_offset;
            let to_read = remaining.min(space);
            let mask = if to_read == 8 {
                0xFF
            } else {
                (1u8 << to_read) - 1
            };
            let bits_val = (data[byte_idx] >> bit_offset) & mask;
            val |= (bits_val as u64) << shift;
            remaining -= to_read;
            bp += to_read as u64;
            shift += to_read;
        }
        result.push(val);
        bit_pos += bits as u64;
    }
    result
}

// ---------------------------------------------------------------------------
// Constant encoding
// ---------------------------------------------------------------------------

pub fn encode_constant_i32(value: i32) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

pub fn decode_constant_i32(data: &[u8], count: usize) -> Vec<i32> {
    let value = i32::from_le_bytes(data[..4].try_into().unwrap());
    vec![value; count]
}

pub fn encode_constant_i64(value: i64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

pub fn decode_constant_i64(data: &[u8], count: usize) -> Vec<i64> {
    let value = i64::from_le_bytes(data[..8].try_into().unwrap());
    vec![value; count]
}

// ---------------------------------------------------------------------------
// FOR + Bit-packing encoding
// ---------------------------------------------------------------------------

pub fn encode_for_i32(values: &[i32]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }
    let min_val = *values.iter().min().unwrap();
    // Subtract in i64 space to avoid overflow
    let offsets: Vec<u32> = values
        .iter()
        .map(|&v| (v as i64 - min_val as i64) as u32)
        .collect();
    let max_offset = offsets.iter().copied().max().unwrap_or(0);
    let bits = bit_width_for_range(max_offset as u64);
    let packed = pack_bits_u32(&offsets, bits);

    let mut buf = Vec::with_capacity(4 + 1 + packed.len());
    buf.extend_from_slice(&min_val.to_le_bytes());
    buf.push(bits);
    buf.extend_from_slice(&packed);
    buf
}

pub fn decode_for_i32(data: &[u8], count: usize) -> Vec<i32> {
    if count == 0 || data.is_empty() {
        return Vec::new();
    }
    let min_val = i32::from_le_bytes(data[..4].try_into().unwrap());
    let bits = data[4];
    let packed = &data[5..];
    let offsets = unpack_bits_u32(packed, count, bits);
    // Add back in i64 space to avoid overflow
    offsets
        .iter()
        .map(|&off| (min_val as i64 + off as i64) as i32)
        .collect()
}

pub fn encode_for_i64(values: &[i64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }
    let min_val = *values.iter().min().unwrap();
    // Subtract in i128 space to avoid overflow
    let offsets: Vec<u64> = values
        .iter()
        .map(|&v| (v as i128 - min_val as i128) as u64)
        .collect();
    let max_offset = offsets.iter().copied().max().unwrap_or(0);
    let bits = bit_width_for_range(max_offset);
    let packed = pack_bits_u64(&offsets, bits);

    let mut buf = Vec::with_capacity(8 + 1 + packed.len());
    buf.extend_from_slice(&min_val.to_le_bytes());
    buf.push(bits);
    buf.extend_from_slice(&packed);
    buf
}

pub fn decode_for_i64(data: &[u8], count: usize) -> Vec<i64> {
    if count == 0 || data.is_empty() {
        return Vec::new();
    }
    let min_val = i64::from_le_bytes(data[..8].try_into().unwrap());
    let bits = data[8];
    let packed = &data[9..];
    let offsets = unpack_bits_u64(packed, count, bits);
    // Add back in i128 space to avoid overflow
    offsets
        .iter()
        .map(|&off| (min_val as i128 + off as i128) as i64)
        .collect()
}

// ---------------------------------------------------------------------------
// Best-encoding selection: try all three, return smallest
// ---------------------------------------------------------------------------

pub fn best_encoding_i32(values: &[i32]) -> (CompressionType, Vec<u8>) {
    if values.is_empty() {
        return (CompressionType::ForBitpacked, Vec::new());
    }

    // Check constant: all values equal
    let first = values[0];
    if values.iter().all(|&v| v == first) {
        return (CompressionType::Constant, encode_constant_i32(first));
    }

    // Try FOR + bit-packing
    let for_encoded = encode_for_i32(values);

    // Try DeltaVarint
    let delta_encoded = super::integer::encode_i32(values);

    if for_encoded.len() <= delta_encoded.len() {
        (CompressionType::ForBitpacked, for_encoded)
    } else {
        (CompressionType::DeltaVarint, delta_encoded)
    }
}

pub fn best_encoding_i64(values: &[i64]) -> (CompressionType, Vec<u8>) {
    if values.is_empty() {
        return (CompressionType::ForBitpacked, Vec::new());
    }

    // Check constant: all values equal
    let first = values[0];
    if values.iter().all(|&v| v == first) {
        return (CompressionType::Constant, encode_constant_i64(first));
    }

    // Try FOR + bit-packing
    let for_encoded = encode_for_i64(values);

    // Try DeltaVarint
    let delta_encoded = super::integer::encode_i64(values);

    if for_encoded.len() <= delta_encoded.len() {
        (CompressionType::ForBitpacked, for_encoded)
    } else {
        (CompressionType::DeltaVarint, delta_encoded)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bit_width() {
        assert_eq!(bit_width_for_range(0), 0);
        assert_eq!(bit_width_for_range(1), 1);
        assert_eq!(bit_width_for_range(2), 2);
        assert_eq!(bit_width_for_range(3), 2);
        assert_eq!(bit_width_for_range(255), 8);
        assert_eq!(bit_width_for_range(256), 9);
        assert_eq!(bit_width_for_range(u32::MAX as u64), 32);
        assert_eq!(bit_width_for_range(u64::MAX), 64);
    }

    #[test]
    fn test_pack_unpack_u32() {
        let values = vec![0u32, 1, 0, 1, 1, 0];
        let packed = pack_bits_u32(&values, 1);
        let unpacked = unpack_bits_u32(&packed, 6, 1);
        assert_eq!(unpacked, values);

        let values = vec![0u32, 255, 128, 1, 42];
        let packed = pack_bits_u32(&values, 8);
        let unpacked = unpack_bits_u32(&packed, 5, 8);
        assert_eq!(unpacked, values);

        let values = vec![0u32, 1000, 500, 1023];
        let packed = pack_bits_u32(&values, 10);
        let unpacked = unpack_bits_u32(&packed, 4, 10);
        assert_eq!(unpacked, values);
    }

    #[test]
    fn test_pack_unpack_u64() {
        let values = vec![0u64, 1, 0, 1];
        let packed = pack_bits_u64(&values, 1);
        let unpacked = unpack_bits_u64(&packed, 4, 1);
        assert_eq!(unpacked, values);

        let values = vec![0u64, u64::MAX >> 1, 42, 12345678901234];
        let packed = pack_bits_u64(&values, 63);
        let unpacked = unpack_bits_u64(&packed, 4, 63);
        assert_eq!(unpacked, values);
    }

    #[test]
    fn test_pack_unpack_zero_bits() {
        let packed = pack_bits_u32(&[0, 0, 0], 0);
        assert!(packed.is_empty());
        let unpacked = unpack_bits_u32(&[], 3, 0);
        assert_eq!(unpacked, vec![0, 0, 0]);
    }

    #[test]
    fn test_constant_i32() {
        let encoded = encode_constant_i32(42);
        assert_eq!(encoded.len(), 4);
        let decoded = decode_constant_i32(&encoded, 100);
        assert_eq!(decoded.len(), 100);
        assert!(decoded.iter().all(|&v| v == 42));
    }

    #[test]
    fn test_constant_i32_negative() {
        let encoded = encode_constant_i32(-1);
        let decoded = decode_constant_i32(&encoded, 5);
        assert_eq!(decoded, vec![-1; 5]);
    }

    #[test]
    fn test_constant_i64() {
        let encoded = encode_constant_i64(123456789012345);
        assert_eq!(encoded.len(), 8);
        let decoded = decode_constant_i64(&encoded, 50);
        assert_eq!(decoded.len(), 50);
        assert!(decoded.iter().all(|&v| v == 123456789012345));
    }

    #[test]
    fn test_for_i32_boolean_like() {
        let values: Vec<i32> = (0..1000).map(|i| i % 2).collect();
        let encoded = encode_for_i32(&values);
        // 4B min + 1B bits + 125B packed = 130B for 1000 values
        assert!(encoded.len() < 200);
        let decoded = decode_for_i32(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_for_i32_all_same() {
        let values = vec![7i32; 500];
        let encoded = encode_for_i32(&values);
        // min=7, bits=0, no packed data → 5 bytes
        assert_eq!(encoded.len(), 5);
        let decoded = decode_for_i32(&encoded, 500);
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_for_i32_negative_range() {
        let values = vec![-100i32, -50, 0, 50, 100];
        let encoded = encode_for_i32(&values);
        let decoded = decode_for_i32(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_for_i32_extreme_range() {
        let values = vec![i32::MIN, 0, i32::MAX];
        let encoded = encode_for_i32(&values);
        let decoded = decode_for_i32(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_for_i64_roundtrip() {
        let values = vec![0i64, 1, 100, 1000, 10000];
        let encoded = encode_for_i64(&values);
        let decoded = decode_for_i64(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_for_i64_extreme_range() {
        let values = vec![i64::MIN, 0, i64::MAX];
        let encoded = encode_for_i64(&values);
        let decoded = decode_for_i64(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_best_encoding_i32_constant() {
        let values = vec![42i32; 1000];
        let (tag, data) = best_encoding_i32(&values);
        assert_eq!(tag, CompressionType::Constant);
        assert_eq!(data.len(), 4);
    }

    #[test]
    fn test_best_encoding_i32_boolean() {
        // Boolean-like values should prefer FOR (1 bit/value)
        let values: Vec<i32> = (0..30000).map(|i| i % 2).collect();
        let (tag, data) = best_encoding_i32(&values);
        assert_eq!(tag, CompressionType::ForBitpacked);
        // ~3.75KB for 30K 1-bit values + 5B header
        assert!(data.len() < 4000);
    }

    #[test]
    fn test_best_encoding_i32_random() {
        // Random-ish but bounded values
        let values: Vec<i32> = (0..1000).map(|i| (i * 7 + 13) % 256).collect();
        let (tag, _) = best_encoding_i32(&values);
        // Should pick either FOR or DeltaVarint — both valid
        assert!(tag == CompressionType::ForBitpacked || tag == CompressionType::DeltaVarint);
    }

    #[test]
    fn test_best_encoding_i64_constant() {
        let values = vec![0i64; 500];
        let (tag, data) = best_encoding_i64(&values);
        assert_eq!(tag, CompressionType::Constant);
        assert_eq!(data.len(), 8);
    }

    #[test]
    fn test_best_encoding_i32_empty() {
        let (tag, data) = best_encoding_i32(&[]);
        assert_eq!(tag, CompressionType::ForBitpacked);
        assert!(data.is_empty());
    }

    #[test]
    fn test_for_i32_single_value() {
        let values = vec![42i32];
        let encoded = encode_for_i32(&values);
        let decoded = decode_for_i32(&encoded, 1);
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_for_i64_single_value() {
        let values = vec![-999i64];
        let encoded = encode_for_i64(&values);
        let decoded = decode_for_i64(&encoded, 1);
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_pack_unpack_32_bits() {
        // Full 32-bit range
        let values = vec![0u32, u32::MAX, 1, u32::MAX - 1];
        let packed = pack_bits_u32(&values, 32);
        let unpacked = unpack_bits_u32(&packed, 4, 32);
        assert_eq!(unpacked, values);
    }

    #[test]
    fn test_pack_unpack_64_bits() {
        let values = vec![0u64, u64::MAX, 1, u64::MAX - 1];
        let packed = pack_bits_u64(&values, 64);
        let unpacked = unpack_bits_u64(&packed, 4, 64);
        assert_eq!(unpacked, values);
    }
}
