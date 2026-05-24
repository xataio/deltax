/// Delta + zigzag + variable-length encoding for integer columns (INT4/INT8).
///
/// Encoding format:
///   8 bytes — first value (i64, little-endian)
///   rest    — zigzag-encoded deltas as varints
/// Zigzag encode: maps signed integers to unsigned so small absolute values
/// produce small unsigned values. 0→0, -1→1, 1→2, -2→3, 2→4, ...
fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ (-((v & 1) as i64))
}

/// Write a u64 as a variable-length integer (1–10 bytes).
/// Each byte: 7 data bits (LSB first) + 1 continuation bit (MSB).
fn write_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(byte);
            return;
        }
        buf.push(byte | 0x80);
    }
}

/// Read a varint from a byte slice, returning (value, bytes_consumed).
fn read_varint(buf: &[u8]) -> (u64, usize) {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    for (i, &byte) in buf.iter().enumerate() {
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return (result, i + 1);
        }
        shift += 7;
    }
    (result, buf.len())
}

// ---------------------------------------------------------------------------
// INT8 (i64)
// ---------------------------------------------------------------------------

pub fn encode_i64(values: &[i64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut buf = Vec::with_capacity(values.len()); // rough estimate
    // First value: raw 8 bytes
    buf.extend_from_slice(&values[0].to_le_bytes());

    // Deltas as zigzag varints
    let mut prev = values[0];
    for &val in &values[1..] {
        let delta = val - prev;
        write_varint(&mut buf, zigzag_encode(delta));
        prev = val;
    }

    buf
}

pub fn decode_i64(data: &[u8], count: usize) -> Vec<i64> {
    if count == 0 {
        return Vec::new();
    }

    let mut values = Vec::with_capacity(count);

    // First value
    let first = i64::from_le_bytes(data[0..8].try_into().unwrap());
    values.push(first);

    let mut offset = 8;
    let mut prev = first;
    for _ in 1..count {
        let (zz, consumed) = read_varint(&data[offset..]);
        offset += consumed;
        let delta = zigzag_decode(zz);
        prev += delta;
        values.push(prev);
    }

    values
}

// ---------------------------------------------------------------------------
// INT4 (i32)
// ---------------------------------------------------------------------------

pub fn encode_i32(values: &[i32]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut buf = Vec::with_capacity(values.len());
    buf.extend_from_slice(&values[0].to_le_bytes());

    let mut prev = values[0];
    for &val in &values[1..] {
        let delta = (val as i64) - (prev as i64);
        write_varint(&mut buf, zigzag_encode(delta));
        prev = val;
    }

    buf
}

pub fn decode_i32(data: &[u8], count: usize) -> Vec<i32> {
    if count == 0 {
        return Vec::new();
    }

    let mut values = Vec::with_capacity(count);
    let first = i32::from_le_bytes(data[0..4].try_into().unwrap());
    values.push(first);

    let mut offset = 4;
    let mut prev = first as i64;
    for _ in 1..count {
        let (zz, consumed) = read_varint(&data[offset..]);
        offset += consumed;
        let delta = zigzag_decode(zz);
        prev += delta;
        values.push(prev as i32);
    }

    values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zigzag() {
        assert_eq!(zigzag_encode(0), 0);
        assert_eq!(zigzag_encode(-1), 1);
        assert_eq!(zigzag_encode(1), 2);
        assert_eq!(zigzag_encode(-2), 3);
        assert_eq!(zigzag_encode(2), 4);

        for v in [-100, -1, 0, 1, 100, i64::MIN, i64::MAX] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v);
        }
    }

    #[test]
    fn test_varint_roundtrip() {
        for v in [0u64, 1, 127, 128, 16383, 16384, u64::MAX] {
            let mut buf = Vec::new();
            write_varint(&mut buf, v);
            let (decoded, consumed) = read_varint(&buf);
            assert_eq!(decoded, v);
            assert_eq!(consumed, buf.len());
        }
    }

    #[test]
    fn test_i64_roundtrip_basic() {
        let values: Vec<i64> = vec![100, 200, 300, 400, 500];
        let encoded = encode_i64(&values);
        let decoded = decode_i64(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_i64_roundtrip_timestamps() {
        // Simulated timestamps at 1-second intervals
        let base = 1_700_000_000_000_000i64;
        let values: Vec<i64> = (0..1000).map(|i| base + i * 1_000_000).collect();
        let encoded = encode_i64(&values);
        let decoded = decode_i64(&encoded, values.len());
        assert_eq!(decoded, values);

        let ratio = (values.len() * 8) as f64 / encoded.len() as f64;
        assert!(
            ratio > 2.0,
            "constant-step i64 should compress >2x, got {:.1}x",
            ratio
        );
    }

    #[test]
    fn test_i64_empty() {
        let encoded = encode_i64(&[]);
        let decoded = decode_i64(&encoded, 0);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_i64_single() {
        let values = vec![42i64];
        let encoded = encode_i64(&values);
        let decoded = decode_i64(&encoded, 1);
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_i64_negative_deltas() {
        let values: Vec<i64> = vec![1000, 900, 800, 700, 600];
        let encoded = encode_i64(&values);
        let decoded = decode_i64(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_i32_roundtrip() {
        let values: Vec<i32> = vec![1, 2, 3, 4, 5, -1, -2, -3];
        let encoded = encode_i32(&values);
        let decoded = decode_i32(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_i32_all_same() {
        let values = vec![42i32; 1000];
        let encoded = encode_i32(&values);
        let decoded = decode_i32(&encoded, values.len());
        assert_eq!(decoded, values);

        // All-same: first value (4 bytes) + 999 zero deltas (1 byte each)
        assert!(encoded.len() < 1100);
    }
}
