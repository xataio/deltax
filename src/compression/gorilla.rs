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

struct BitReader<'a> {
    bytes: &'a [u8],
    byte_pos: usize,
    bit_pos: u8,
}

impl<'a> BitReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            byte_pos: 0,
            bit_pos: 0,
        }
    }

    fn read_bit(&mut self) -> bool {
        if self.byte_pos >= self.bytes.len() {
            return false;
        }
        let bit = (self.bytes[self.byte_pos] >> (7 - self.bit_pos)) & 1 == 1;
        self.bit_pos += 1;
        if self.bit_pos == 8 {
            self.byte_pos += 1;
            self.bit_pos = 0;
        }
        bit
    }

    fn read_bits(&mut self, num_bits: u8) -> u64 {
        if num_bits == 0 {
            return 0;
        }
        let mut remaining = num_bits;
        let mut value = 0u64;
        while remaining > 0 {
            if self.byte_pos >= self.bytes.len() {
                break;
            }
            let avail = 8 - self.bit_pos;
            let take = remaining.min(avail);
            let shift = avail - take;
            let mask = ((1u16 << take) - 1) as u8;
            let bits = (self.bytes[self.byte_pos] >> shift) & mask;
            value = (value << take) | bits as u64;
            self.bit_pos += take;
            if self.bit_pos == 8 {
                self.byte_pos += 1;
                self.bit_pos = 0;
            }
            remaining -= take;
        }
        value
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
    if count == 0 {
        return Vec::new();
    }

    let mut reader = BitReader::new(data);
    let mut values = Vec::with_capacity(count);

    let first_bits = reader.read_bits(64);
    values.push(f64::from_bits(first_bits));
    let mut prev_bits = first_bits;
    let mut prev_leading: u8 = 0;
    let mut prev_trailing: u8 = 0;

    for _ in 1..count {
        if !reader.read_bit() {
            // Same value
            values.push(f64::from_bits(prev_bits));
        } else if !reader.read_bit() {
            // Same window
            let meaningful = 64 - prev_leading - prev_trailing;
            let xor_meaningful = reader.read_bits(meaningful);
            let xor = xor_meaningful << prev_trailing;
            prev_bits ^= xor;
            values.push(f64::from_bits(prev_bits));
        } else {
            // New window
            let leading = reader.read_bits(6) as u8;
            let meaningful = reader.read_bits(6) as u8;
            let meaningful = if meaningful == 0 { 64 } else { meaningful };
            let trailing = 64 - leading - meaningful;
            let xor_meaningful = reader.read_bits(meaningful);
            let xor = xor_meaningful << trailing;
            prev_bits ^= xor;
            values.push(f64::from_bits(prev_bits));
            prev_leading = leading;
            prev_trailing = trailing;
        }
    }

    values
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
    if count == 0 {
        return Vec::new();
    }

    let mut reader = BitReader::new(data);
    let mut values = Vec::with_capacity(count);

    let first_bits = reader.read_bits(32) as u32;
    values.push(f32::from_bits(first_bits));
    let mut prev_bits = first_bits;
    let mut prev_leading: u8 = 0;
    let mut prev_trailing: u8 = 0;

    for _ in 1..count {
        if !reader.read_bit() {
            values.push(f32::from_bits(prev_bits));
        } else if !reader.read_bit() {
            let meaningful = 32 - prev_leading - prev_trailing;
            let xor_meaningful = reader.read_bits(meaningful) as u32;
            let xor = xor_meaningful << prev_trailing;
            prev_bits ^= xor;
            values.push(f32::from_bits(prev_bits));
        } else {
            let leading = reader.read_bits(5) as u8;
            let meaningful = reader.read_bits(5) as u8;
            let meaningful = if meaningful == 0 { 32 } else { meaningful };
            let trailing = 32 - leading - meaningful;
            let xor_meaningful = reader.read_bits(meaningful) as u32;
            let xor = xor_meaningful << trailing;
            prev_bits ^= xor;
            values.push(f32::from_bits(prev_bits));
            prev_leading = leading;
            prev_trailing = trailing;
        }
    }

    values
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
        } else if (-63..=64).contains(&dod) {
            writer.write_bits(0b10, 2);
            writer.write_bits(dod as u64, 7);
        } else if (-255..=256).contains(&dod) {
            writer.write_bits(0b110, 3);
            writer.write_bits(dod as u64, 9);
        } else if (-2047..=2048).contains(&dod) {
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
    if count == 0 {
        return Vec::new();
    }

    let mut reader = BitReader::new(data);
    let mut values = Vec::with_capacity(count);

    // First value
    let first = reader.read_bits(64) as i64;
    values.push(first);

    if count == 1 {
        return values;
    }

    // First delta
    let mut prev_delta = reader.read_bits(64) as i64;
    values.push(first + prev_delta);

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
        let val = *values.last().unwrap() + prev_delta;
        values.push(val);
    }

    values
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
}
