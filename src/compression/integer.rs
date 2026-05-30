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
///
/// Reference implementation kept for differential testing; the decoders use the
/// faster [`read_varint_at`].
#[cfg(test)]
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

/// Read a varint starting at `data[*offset]`, advancing `offset` past it.
///
/// Hot-loop variant of [`read_varint`] used by the decoders: it indexes `data`
/// directly instead of reslicing `&data[offset..]` on every value, which
/// matters when decoding tens of thousands of deltas per segment. The fast
/// path handles the common 1–2 byte varints with no loop; wider values fall
/// back to a byte-at-a-time loop. Semantics are identical to `read_varint`
/// (LSB-first 7-bit groups, MSB continuation bit).
#[inline(always)]
fn read_varint_at(data: &[u8], offset: &mut usize) -> u64 {
    let mut o = *offset;
    // Fast path: first byte (covers values < 128, the dominant case).
    let b0 = data[o];
    o += 1;
    if b0 & 0x80 == 0 {
        *offset = o;
        return (b0 & 0x7F) as u64;
    }
    let mut result = (b0 & 0x7F) as u64;
    let mut shift = 7u32;
    loop {
        let byte = data[o];
        o += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    *offset = o;
    result
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
    let mut values = Vec::with_capacity(count);
    decode_i64_each(data, count, |v| values.push(v));
    values
}

/// Callback form of [`decode_i64`]: invokes `f` for each decoded value in
/// order, with no intermediate `Vec`. Lets callers decode straight into their
/// final layout (e.g. `(Datum, bool)` pairs) — one allocation, one pass.
#[inline]
pub fn decode_i64_each(data: &[u8], count: usize, mut f: impl FnMut(i64)) {
    if count == 0 {
        return;
    }

    // First value
    let first = i64::from_le_bytes(data[0..8].try_into().unwrap());
    f(first);

    let mut offset = 8;
    let mut prev = first;
    for _ in 1..count {
        let zz = read_varint_at(data, &mut offset);
        prev += zigzag_decode(zz);
        f(prev);
    }
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
    let mut values = Vec::with_capacity(count);
    decode_i32_each(data, count, |v| values.push(v));
    values
}

/// Callback form of [`decode_i32`] — see [`decode_i64_each`].
#[inline]
pub fn decode_i32_each(data: &[u8], count: usize, mut f: impl FnMut(i32)) {
    if count == 0 {
        return;
    }

    let first = i32::from_le_bytes(data[0..4].try_into().unwrap());
    f(first);

    let mut offset = 4;
    let mut prev = first as i64;
    for _ in 1..count {
        let zz = read_varint_at(data, &mut offset);
        prev += zigzag_decode(zz);
        f(prev as i32);
    }
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

    fn xorshift64(state: &mut u64) -> u64 {
        let mut x = *state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *state = x;
        x
    }

    #[test]
    fn test_read_varint_at_matches_reference() {
        // read_varint_at must be bit-identical to the reference read_varint,
        // across all byte widths, and must advance the offset identically.
        let mut s = 0x123456789ABCDEFu64;
        let mut buf = Vec::new();
        let mut expected = Vec::new();
        for _ in 0..5000 {
            // Bias toward small values (the common delta case) but include
            // full-width ones to exercise the multi-byte loop.
            let v = match xorshift64(&mut s) % 4 {
                0 => xorshift64(&mut s) % 128,
                1 => xorshift64(&mut s) % 16384,
                2 => xorshift64(&mut s) % 1_000_000,
                _ => xorshift64(&mut s),
            };
            write_varint(&mut buf, v);
            expected.push(v);
        }
        let mut off_fast = 0usize;
        let mut off_ref = 0usize;
        for &v in &expected {
            let got = read_varint_at(&buf, &mut off_fast);
            let (got_ref, consumed) = read_varint(&buf[off_ref..]);
            off_ref += consumed;
            assert_eq!(got, v);
            assert_eq!(got_ref, v);
            assert_eq!(off_fast, off_ref);
        }
        assert_eq!(off_fast, buf.len());
    }

    #[test]
    fn test_i64_random_roundtrip() {
        let mut s = 0xCAFEF00DD15EA5E5u64;
        for _ in 0..200 {
            let n = (xorshift64(&mut s) % 600) as usize + 1;
            let mut values = Vec::with_capacity(n);
            // Bounded start + bounded deltas so the encoder's `val - prev`
            // stays within i64. Wide deltas (up to ~2^40) still exercise the
            // multi-byte varint path; full-width varints are covered separately
            // by test_read_varint_at_matches_reference.
            let mut cur = (xorshift64(&mut s) as i64) >> 16;
            values.push(cur);
            for _ in 1..n {
                let delta = match xorshift64(&mut s) % 4 {
                    0 => 0i64,
                    1 => (xorshift64(&mut s) % 256) as i64 - 128,
                    2 => (xorshift64(&mut s) % 2_000_000) as i64 - 1_000_000,
                    _ => (xorshift64(&mut s) % (1 << 40)) as i64 - (1 << 39),
                };
                cur = cur.wrapping_add(delta);
                values.push(cur);
            }
            let encoded = encode_i64(&values);
            let decoded = decode_i64(&encoded, values.len());
            assert_eq!(decoded, values);
        }
    }

    #[test]
    fn test_i32_random_roundtrip() {
        let mut s = 0x0DDB1A5E5BAD5EEDu64;
        for _ in 0..200 {
            let n = (xorshift64(&mut s) % 600) as usize + 1;
            let mut values = Vec::with_capacity(n);
            let mut cur = xorshift64(&mut s) as i32;
            values.push(cur);
            for _ in 1..n {
                let delta = match xorshift64(&mut s) % 4 {
                    0 => 0i32,
                    1 => (xorshift64(&mut s) % 256) as i32 - 128,
                    _ => xorshift64(&mut s) as i32,
                };
                cur = cur.wrapping_add(delta);
                values.push(cur);
            }
            let encoded = encode_i32(&values);
            let decoded = decode_i32(&encoded, values.len());
            assert_eq!(decoded, values);
        }
    }
}
