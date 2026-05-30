// Gorilla compression for floating-point and timestamp data.
//
// - Floats: XOR-based encoding (Facebook Gorilla paper)
// - Timestamps: Delta-of-delta encoding

// ---------------------------------------------------------------------------
// Bit-level I/O
// ---------------------------------------------------------------------------

struct BitWriter {
    bytes: Vec<u8>,
    current: u8,
    bits_used: u8, // 0..8
}

impl BitWriter {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            current: 0,
            bits_used: 0,
        }
    }

    fn write_bit(&mut self, bit: bool) {
        if bit {
            self.current |= 1 << (7 - self.bits_used);
        }
        self.bits_used += 1;
        if self.bits_used == 8 {
            self.bytes.push(self.current);
            self.current = 0;
            self.bits_used = 0;
        }
    }

    fn write_bits(&mut self, value: u64, num_bits: u8) {
        if num_bits == 0 {
            return;
        }
        let mut remaining = num_bits;
        while remaining > 0 {
            let avail = 8 - self.bits_used;
            let take = remaining.min(avail);
            // Extract `take` bits from the MSB side of the remaining value
            let shift = remaining - take;
            let mask = ((1u64 << take) - 1) as u8;
            let bits = ((value >> shift) & mask as u64) as u8;
            self.current |= bits << (avail - take);
            self.bits_used += take;
            if self.bits_used == 8 {
                self.bytes.push(self.current);
                self.current = 0;
                self.bits_used = 0;
            }
            remaining -= take;
        }
    }

    fn finish(mut self) -> Vec<u8> {
        if self.bits_used > 0 {
            self.bytes.push(self.current);
        }
        self.bytes
    }
}

/// MSB-first bit reader backed by a 64-bit refill buffer.
///
/// `acc` holds the next bits left-aligned (the next bit to read is bit 63);
/// `cnt` is how many of those high bits are valid. `refill` tops the buffer up
/// from `bytes` 8 bits at a time until it holds >56 bits, so any read of up to
/// 57 bits is served by a single shift/mask without per-bit branching. Reads
/// wider than 57 bits (the 64-bit first value / `1111` delta-of-delta escape)
/// are split into two sub-reads. Out-of-bounds reads return the bits available
/// (zero-padded), matching the previous byte-at-a-time reader; for valid data
/// (exact `count`) this path is never hit.
struct BitReader<'a> {
    bytes: &'a [u8],
    pos: usize, // next byte to load into `acc`
    acc: u64,   // bit buffer; next bit at the MSB
    cnt: u32,   // number of valid bits currently in `acc` (from the MSB side)
}

impl<'a> BitReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        let mut r = Self {
            bytes,
            pos: 0,
            acc: 0,
            cnt: 0,
        };
        r.refill();
        r
    }

    #[inline(always)]
    fn refill(&mut self) {
        while self.cnt <= 56 && self.pos < self.bytes.len() {
            self.acc |= (self.bytes[self.pos] as u64) << (56 - self.cnt);
            self.pos += 1;
            self.cnt += 8;
        }
    }

    #[inline(always)]
    fn read_bit(&mut self) -> bool {
        if self.cnt == 0 {
            self.refill();
            if self.cnt == 0 {
                return false;
            }
        }
        let bit = (self.acc >> 63) & 1 == 1;
        self.acc <<= 1;
        self.cnt -= 1;
        bit
    }

    #[inline(always)]
    fn read_bits(&mut self, num_bits: u8) -> u64 {
        let n = num_bits as u32;
        if n == 0 {
            return 0;
        }
        // Widths beyond 57 (the 64-bit first value / escape delta) don't fit a
        // single refilled buffer — split into a high and low half.
        if n > 57 {
            let lo_bits = num_bits - 32;
            let hi = self.read_bits(32);
            let lo = self.read_bits(lo_bits);
            return (hi << lo_bits) | lo;
        }
        if self.cnt < n {
            self.refill();
            if self.cnt < n {
                // Truncated input: return whatever high bits remain.
                let avail = self.cnt;
                let val = if avail == 0 {
                    0
                } else {
                    self.acc >> (64 - avail)
                };
                self.acc = 0;
                self.cnt = 0;
                return val;
            }
        }
        let val = self.acc >> (64 - n);
        self.acc <<= n;
        self.cnt -= n;
        val
    }
}

// ---------------------------------------------------------------------------
// Float (f64) — Gorilla XOR encoding
// ---------------------------------------------------------------------------

pub fn encode_floats(values: &[f64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut writer = BitWriter::new();

    // First value: raw 64 bits
    writer.write_bits(values[0].to_bits(), 64);

    let mut prev_bits = values[0].to_bits();
    let mut prev_leading: u8 = 64; // sentinel: no previous window
    let mut prev_trailing: u8 = 0;

    for &val in &values[1..] {
        let bits = val.to_bits();
        let xor = prev_bits ^ bits;

        if xor == 0 {
            // Same value: single '0' bit
            writer.write_bit(false);
        } else {
            writer.write_bit(true);

            let leading = xor.leading_zeros() as u8;
            let trailing = xor.trailing_zeros() as u8;

            if prev_leading != 64 && leading >= prev_leading && trailing >= prev_trailing {
                // Fits in previous window: '0' + meaningful bits
                writer.write_bit(false);
                let meaningful = 64 - prev_leading - prev_trailing;
                writer.write_bits(xor >> prev_trailing, meaningful);
            } else {
                // New window: '1' + 6-bit leading + 6-bit length + meaningful bits
                writer.write_bit(true);
                let meaningful = 64 - leading - trailing;
                writer.write_bits(leading as u64, 6);
                // Store meaningful bits count (0 means 64 — but 0 XOR already handled)
                writer.write_bits(meaningful as u64, 6);
                writer.write_bits(xor >> trailing, meaningful);
                prev_leading = leading;
                prev_trailing = trailing;
            }
        }

        prev_bits = bits;
    }

    writer.finish()
}

pub fn decode_floats(data: &[u8], count: usize) -> Vec<f64> {
    let mut values = Vec::with_capacity(count);
    decode_floats_each(data, count, |v| values.push(v));
    values
}

/// Callback form of [`decode_floats`]: invokes `f` for each decoded value in
/// order, with no intermediate `Vec`. Lets callers decode straight into their
/// final layout (e.g. `(Datum, bool)` pairs) — one allocation, one pass.
#[inline]
pub fn decode_floats_each(data: &[u8], count: usize, mut f: impl FnMut(f64)) {
    if count == 0 {
        return;
    }

    let mut reader = BitReader::new(data);
    let first_bits = reader.read_bits(64);
    f(f64::from_bits(first_bits));
    let mut prev_bits = first_bits;
    let mut prev_leading: u8 = 0;
    let mut prev_trailing: u8 = 0;

    for _ in 1..count {
        if !reader.read_bit() {
            // Same value
            f(f64::from_bits(prev_bits));
        } else if !reader.read_bit() {
            // Same window
            let meaningful = 64 - prev_leading - prev_trailing;
            let xor_meaningful = reader.read_bits(meaningful);
            prev_bits ^= xor_meaningful << prev_trailing;
            f(f64::from_bits(prev_bits));
        } else {
            // New window
            let leading = reader.read_bits(6) as u8;
            let meaningful = reader.read_bits(6) as u8;
            let meaningful = if meaningful == 0 { 64 } else { meaningful };
            let trailing = 64 - leading - meaningful;
            let xor_meaningful = reader.read_bits(meaningful);
            prev_bits ^= xor_meaningful << trailing;
            f(f64::from_bits(prev_bits));
            prev_leading = leading;
            prev_trailing = trailing;
        }
    }
}

// ---------------------------------------------------------------------------
// Float (f32) — same algorithm, 32-bit variant
// ---------------------------------------------------------------------------

pub fn encode_floats_f32(values: &[f32]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut writer = BitWriter::new();
    writer.write_bits(values[0].to_bits() as u64, 32);

    let mut prev_bits = values[0].to_bits();
    let mut prev_leading: u8 = 32;
    let mut prev_trailing: u8 = 0;

    for &val in &values[1..] {
        let bits = val.to_bits();
        let xor = prev_bits ^ bits;

        if xor == 0 {
            writer.write_bit(false);
        } else {
            writer.write_bit(true);
            let leading = xor.leading_zeros() as u8;
            let trailing = xor.trailing_zeros() as u8;

            if prev_leading != 32 && leading >= prev_leading && trailing >= prev_trailing {
                writer.write_bit(false);
                let meaningful = 32 - prev_leading - prev_trailing;
                writer.write_bits((xor >> prev_trailing) as u64, meaningful);
            } else {
                writer.write_bit(true);
                let meaningful = 32 - leading - trailing;
                writer.write_bits(leading as u64, 5);
                writer.write_bits(meaningful as u64, 5);
                writer.write_bits((xor >> trailing) as u64, meaningful);
                prev_leading = leading;
                prev_trailing = trailing;
            }
        }
        prev_bits = bits;
    }

    writer.finish()
}

pub fn decode_floats_f32(data: &[u8], count: usize) -> Vec<f32> {
    let mut values = Vec::with_capacity(count);
    decode_floats_f32_each(data, count, |v| values.push(v));
    values
}

/// Callback form of [`decode_floats_f32`] — see [`decode_floats_each`].
#[inline]
pub fn decode_floats_f32_each(data: &[u8], count: usize, mut f: impl FnMut(f32)) {
    if count == 0 {
        return;
    }

    let mut reader = BitReader::new(data);
    let first_bits = reader.read_bits(32) as u32;
    f(f32::from_bits(first_bits));
    let mut prev_bits = first_bits;
    let mut prev_leading: u8 = 0;
    let mut prev_trailing: u8 = 0;

    for _ in 1..count {
        if !reader.read_bit() {
            f(f32::from_bits(prev_bits));
        } else if !reader.read_bit() {
            let meaningful = 32 - prev_leading - prev_trailing;
            let xor_meaningful = reader.read_bits(meaningful) as u32;
            prev_bits ^= xor_meaningful << prev_trailing;
            f(f32::from_bits(prev_bits));
        } else {
            let leading = reader.read_bits(5) as u8;
            let meaningful = reader.read_bits(5) as u8;
            let meaningful = if meaningful == 0 { 32 } else { meaningful };
            let trailing = 32 - leading - meaningful;
            let xor_meaningful = reader.read_bits(meaningful) as u32;
            prev_bits ^= xor_meaningful << trailing;
            f(f32::from_bits(prev_bits));
            prev_leading = leading;
            prev_trailing = trailing;
        }
    }
}

// ---------------------------------------------------------------------------
// Timestamps (i64) — Delta-of-delta encoding
// ---------------------------------------------------------------------------

fn sign_extend(val: u64, bits: u8) -> i64 {
    let shift = 64 - bits as u32;
    ((val as i64) << shift) >> shift
}

pub fn encode_timestamps(values: &[i64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut writer = BitWriter::new();

    // First value: raw 64 bits
    writer.write_bits(values[0] as u64, 64);

    if values.len() == 1 {
        return writer.finish();
    }

    // First delta: raw 64 bits
    let mut prev_delta = values[1] - values[0];
    writer.write_bits(prev_delta as u64, 64);

    for i in 2..values.len() {
        let delta = values[i] - values[i - 1];
        let dod = delta - prev_delta;

        if dod == 0 {
            writer.write_bit(false);
        } else if (-64..=63).contains(&dod) {
            // 7-bit two's complement: [-64, 63]. Must match the decoder's
            // sign_extend(_, 7); the previous (-63..=64) bound mis-encoded
            // dod == 64 as -64 (and likewise at the 9/12-bit boundaries).
            writer.write_bits(0b10, 2);
            writer.write_bits(dod as u64, 7);
        } else if (-256..=255).contains(&dod) {
            writer.write_bits(0b110, 3);
            writer.write_bits(dod as u64, 9);
        } else if (-2048..=2047).contains(&dod) {
            writer.write_bits(0b1110, 4);
            writer.write_bits(dod as u64, 12);
        } else {
            writer.write_bits(0b1111, 4);
            writer.write_bits(dod as u64, 64);
        }

        prev_delta = delta;
    }

    writer.finish()
}

pub fn decode_timestamps(data: &[u8], count: usize) -> Vec<i64> {
    let mut values = Vec::with_capacity(count);
    decode_timestamps_each(data, count, |v| values.push(v));
    values
}

/// Callback form of [`decode_timestamps`] — see [`decode_floats_each`]. Also
/// tracks the running value in a local instead of re-reading `values.last()`
/// every row.
#[inline]
pub fn decode_timestamps_each(data: &[u8], count: usize, mut f: impl FnMut(i64)) {
    if count == 0 {
        return;
    }

    let mut reader = BitReader::new(data);

    // First value
    let first = reader.read_bits(64) as i64;
    f(first);

    if count == 1 {
        return;
    }

    // First delta
    let mut prev_delta = reader.read_bits(64) as i64;
    let mut cur = first + prev_delta;
    f(cur);

    for _ in 2..count {
        let dod = if !reader.read_bit() {
            0i64
        } else if !reader.read_bit() {
            // '10' prefix — 7 bits
            sign_extend(reader.read_bits(7), 7)
        } else if !reader.read_bit() {
            // '110' prefix — 9 bits
            sign_extend(reader.read_bits(9), 9)
        } else if !reader.read_bit() {
            // '1110' prefix — 12 bits
            sign_extend(reader.read_bits(12), 12)
        } else {
            // '1111' prefix — 64 bits
            reader.read_bits(64) as i64
        };

        prev_delta += dod;
        cur += prev_delta;
        f(cur);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_float_roundtrip_basic() {
        let values = vec![1.0, 1.5, 2.0, 2.5, 3.0];
        let encoded = encode_floats(&values);
        let decoded = decode_floats(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_float_roundtrip_same_values() {
        let values = vec![42.0; 100];
        let encoded = encode_floats(&values);
        let decoded = decode_floats(&encoded, values.len());
        assert_eq!(decoded, values);
        // All-same should compress very well
        assert!(
            encoded.len() < 30,
            "all-same should compress to ~20 bytes, got {}",
            encoded.len()
        );
    }

    #[test]
    fn test_float_roundtrip_single() {
        let values = vec![1.5];
        let encoded = encode_floats(&values);
        let decoded = decode_floats(&encoded, 1);
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_float_roundtrip_empty() {
        let encoded = encode_floats(&[]);
        let decoded = decode_floats(&encoded, 0);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_float_nan() {
        let values = vec![1.0, f64::NAN, 3.0];
        let encoded = encode_floats(&values);
        let decoded = decode_floats(&encoded, 3);
        assert_eq!(decoded[0], 1.0);
        assert!(decoded[1].is_nan());
        assert_eq!(decoded[2], 3.0);
    }

    #[test]
    fn test_float_compression_ratio() {
        // Simulated sensor data: small variations
        let mut values = Vec::with_capacity(1000);
        let mut v = 20.0f64;
        for i in 0..1000 {
            v += (i as f64 * 0.01).sin() * 0.1;
            values.push(v);
        }
        let raw_size = values.len() * 8;
        let encoded = encode_floats(&values);
        let decoded = decode_floats(&encoded, values.len());

        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }

        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(
            ratio > 1.0,
            "expected >1x compression on sensor data, got {:.1}x",
            ratio
        );
    }

    #[test]
    fn test_f32_roundtrip() {
        let values: Vec<f32> = vec![1.0, 1.5, 2.0, 2.5, 3.0];
        let encoded = encode_floats_f32(&values);
        let decoded = decode_floats_f32(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_timestamp_roundtrip_basic() {
        // Simulated timestamps: 1-second intervals in microseconds
        let base = 1_700_000_000_000_000i64; // ~2023
        let values: Vec<i64> = (0..100).map(|i| base + i * 1_000_000).collect();
        let encoded = encode_timestamps(&values);
        let decoded = decode_timestamps(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_timestamp_roundtrip_irregular() {
        let base = 1_700_000_000_000_000i64;
        let values = vec![
            base,
            base + 1_000_000,
            base + 3_000_000,
            base + 3_500_000,
            base + 10_000_000,
        ];
        let encoded = encode_timestamps(&values);
        let decoded = decode_timestamps(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_timestamp_single() {
        let values = vec![1_700_000_000_000_000i64];
        let encoded = encode_timestamps(&values);
        let decoded = decode_timestamps(&encoded, 1);
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_timestamp_empty() {
        let encoded = encode_timestamps(&[]);
        let decoded = decode_timestamps(&encoded, 0);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_timestamp_compression_ratio() {
        // Regular 1-second intervals: should compress extremely well (constant delta)
        let base = 1_700_000_000_000_000i64;
        let values: Vec<i64> = (0..10000).map(|i| base + i * 1_000_000).collect();
        let raw_size = values.len() * 8;
        let encoded = encode_timestamps(&values);
        let decoded = decode_timestamps(&encoded, values.len());
        assert_eq!(decoded, values);

        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(
            ratio > 10.0,
            "constant-delta timestamps should compress >10x, got {:.1}x",
            ratio
        );
    }

    // --- Randomized roundtrip coverage for the refill-buffer BitReader -------
    //
    // The encoder is unchanged, so bit-identical roundtrip over varied,
    // randomized inputs exercises every reader branch (same-value, same-window,
    // new-window, each delta-of-delta width, and the 64-bit escape paths).

    fn xorshift64(state: &mut u64) -> u64 {
        let mut x = *state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *state = x;
        x
    }

    #[test]
    fn test_floats_random_roundtrip() {
        let mut s = 0x9E3779B97F4A7C15u64;
        for trial in 0..200 {
            let n = (xorshift64(&mut s) % 500) as usize + 1;
            let mut values = Vec::with_capacity(n);
            let mut base = f64::from_bits(xorshift64(&mut s));
            if !base.is_finite() {
                base = 1.0;
            }
            for _ in 0..n {
                match xorshift64(&mut s) % 4 {
                    0 => {}                                   // repeat previous (xor==0)
                    1 => base += (xorshift64(&mut s) % 8) as f64 * 0.001, // tiny step
                    2 => base = f64::from_bits(xorshift64(&mut s)), // arbitrary bits
                    _ => base *= 1.0001,                       // small relative step
                }
                values.push(base);
            }
            let encoded = encode_floats(&values);
            let decoded = decode_floats(&encoded, values.len());
            assert_eq!(decoded.len(), values.len(), "trial {trial}");
            for (i, (a, b)) in values.iter().zip(decoded.iter()).enumerate() {
                assert_eq!(a.to_bits(), b.to_bits(), "trial {trial} idx {i}");
            }
        }
    }

    #[test]
    fn test_f32_random_roundtrip() {
        let mut s = 0xD1B54A32D192ED03u64;
        for trial in 0..200 {
            let n = (xorshift64(&mut s) % 500) as usize + 1;
            let mut values = Vec::with_capacity(n);
            let mut base = f32::from_bits(xorshift64(&mut s) as u32);
            if !base.is_finite() {
                base = 1.0;
            }
            for _ in 0..n {
                match xorshift64(&mut s) % 4 {
                    0 => {}
                    1 => base += (xorshift64(&mut s) % 8) as f32 * 0.001,
                    2 => base = f32::from_bits(xorshift64(&mut s) as u32),
                    _ => base *= 1.0001,
                }
                values.push(base);
            }
            let encoded = encode_floats_f32(&values);
            let decoded = decode_floats_f32(&encoded, values.len());
            assert_eq!(decoded.len(), values.len(), "trial {trial}");
            for (i, (a, b)) in values.iter().zip(decoded.iter()).enumerate() {
                assert_eq!(a.to_bits(), b.to_bits(), "trial {trial} idx {i}");
            }
        }
    }

    #[test]
    fn test_timestamps_random_roundtrip() {
        let mut s = 0x2545F4914F6CDD1Du64;
        for trial in 0..300 {
            let n = (xorshift64(&mut s) % 500) as usize + 1;
            let mut values = Vec::with_capacity(n);
            // Realistic microsecond-scale base; deltas are bounded so the
            // encoder's delta-of-delta subtraction stays within i64 (real
            // timestamps never produce overflowing dods).
            let mut cur = 1_700_000_000_000_000i64 + ((xorshift64(&mut s) % 1_000_000) as i64);
            values.push(cur);
            for _ in 1..n {
                // Mix of delta-of-delta magnitudes to hit every prefix width,
                // including the 64-bit escape (deltas ≫ 2048 take that branch).
                let delta = match xorshift64(&mut s) % 5 {
                    0 => 0i64,
                    1 => (xorshift64(&mut s) % 64) as i64 - 32,
                    2 => (xorshift64(&mut s) % 512) as i64 - 256,
                    3 => (xorshift64(&mut s) % 4096) as i64 - 2048,
                    _ => (xorshift64(&mut s) % (1 << 40)) as i64 - (1 << 39), // → 64-bit dod escape
                };
                cur = cur.wrapping_add(delta);
                values.push(cur);
            }
            let encoded = encode_timestamps(&values);
            let decoded = decode_timestamps(&encoded, values.len());
            assert_eq!(decoded, values, "trial {trial}");
        }
    }

    #[test]
    fn test_timestamp_dod_boundaries() {
        // Regression for the delta-of-delta prefix-width off-by-one: each
        // boundary dod must round-trip exactly, including the values that sit
        // right on the 7/9/12-bit two's-complement edges (±64, ±256, ±2048).
        let base = 1_700_000_000_000_000i64;
        for &dod in &[
            -2049i64, -2048, -2047, -257, -256, -255, -65, -64, -63, -1, 0, 1, 63, 64, 65, 255,
            256, 257, 2047, 2048, 2049,
        ] {
            // delta0 = 1000; delta1 = 1000 + dod, so values[2]'s dod == `dod`.
            let v1 = base + 1000;
            let values = vec![base, v1, v1 + (1000 + dod)];
            let encoded = encode_timestamps(&values);
            let decoded = decode_timestamps(&encoded, values.len());
            assert_eq!(decoded, values, "dod={dod}");
        }
    }
}
