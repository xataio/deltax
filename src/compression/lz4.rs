/// LZ4 compression for high-cardinality TEXT columns.
///
/// Format:
///   For each string:
///     4 bytes — string length (u32 LE)
///     N bytes — string UTF-8 data
///   Then the entire buffer is LZ4-compressed.
pub fn encode(values: &[&str]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    // Pack all strings with length prefixes
    let total_raw: usize = values.iter().map(|s| 4 + s.len()).sum();
    let mut raw = Vec::with_capacity(total_raw);
    for &s in values {
        let bytes = s.as_bytes();
        raw.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        raw.extend_from_slice(bytes);
    }

    // LZ4 compress
    lz4_flex::compress_prepend_size(&raw)
}

/// Decode LZ4 data, returning the decompressed buffer and (offset, len) ranges.
/// Callers can reference `&buf[offset..offset+len]` as `&str` without allocating Strings.
pub fn decode_to_ranges(data: &[u8], count: usize) -> (Vec<u8>, Vec<(usize, usize)>) {
    if count == 0 {
        return (Vec::new(), Vec::new());
    }

    let raw = lz4_flex::decompress_size_prepended(data).expect("LZ4 decompression failed");
    let mut ranges = Vec::with_capacity(count);
    let mut offset = 0;
    for _ in 0..count {
        let str_len = u32::from_le_bytes(raw[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        ranges.push((offset, str_len));
        offset += str_len;
    }
    (raw, ranges)
}

pub fn decode(data: &[u8], count: usize) -> Vec<String> {
    if count == 0 {
        return Vec::new();
    }

    let raw = lz4_flex::decompress_size_prepended(data).expect("LZ4 decompression failed");

    let mut values = Vec::with_capacity(count);
    let mut offset = 0;
    for _ in 0..count {
        let str_len = u32::from_le_bytes(raw[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let s = std::str::from_utf8(&raw[offset..offset + str_len])
            .expect("invalid UTF-8 in LZ4 data")
            .to_string();
        offset += str_len;
        values.push(s);
    }

    values
}

/// Default number of rows per independently-compressed block.
pub const DEFAULT_BLOCK_SIZE: usize = 10000;

/// Read a block's byte offset from the offset table.
#[inline]
fn block_offset(data: &[u8], block_idx: usize) -> usize {
    let pos = 4 + block_idx * 4;
    u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize
}

/// Block-based LZ4 encoding for partial decompression support.
///
/// Wire format:
///   2 bytes — block_size (u16 LE, rows per block)
///   2 bytes — num_blocks (u16 LE)
///   4*(num_blocks+1) bytes — block byte offsets (u32 LE, for random access)
///   [block 0: lz4_flex::compress_prepend_size of rows 0..block_size-1]
///   [block 1: lz4_flex::compress_prepend_size of rows block_size..2*block_size-1]
///   ...
///
/// Each block's inner format is identical to the monolithic LZ4: [4-byte len][UTF-8 data] × N.
pub fn encode_blocked(values: &[&str], block_size: usize) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    let num_blocks = values.len().div_ceil(block_size);

    // Compress each block independently
    let mut compressed_blocks: Vec<Vec<u8>> = Vec::with_capacity(num_blocks);
    for chunk in values.chunks(block_size) {
        compressed_blocks.push(encode(chunk));
    }

    // Build the header + offset table + blocks
    let header_size = 2 + 2 + 4 * (num_blocks + 1);
    let total_size: usize = header_size + compressed_blocks.iter().map(|b| b.len()).sum::<usize>();
    let mut buf = Vec::with_capacity(total_size);

    // Header
    buf.extend_from_slice(&(block_size as u16).to_le_bytes());
    buf.extend_from_slice(&(num_blocks as u16).to_le_bytes());

    // Offset table: num_blocks+1 entries (start of each block + end sentinel)
    let mut offset = header_size as u32;
    for block in &compressed_blocks {
        buf.extend_from_slice(&offset.to_le_bytes());
        offset += block.len() as u32;
    }
    buf.extend_from_slice(&offset.to_le_bytes()); // end sentinel

    // Block data
    for block in &compressed_blocks {
        buf.extend_from_slice(block);
    }

    buf
}

/// Decode all blocks of an Lz4Blocked column, returning all strings.
pub fn decode_blocked(data: &[u8], count: usize) -> Vec<String> {
    if count == 0 {
        return Vec::new();
    }

    let num_blocks = u16::from_le_bytes(data[2..4].try_into().unwrap()) as usize;

    // First pass: sum uncompressed sizes to pre-allocate one buffer
    let mut total_uncompressed = 0usize;
    for i in 0..num_blocks {
        let off_start = block_offset(data, i);
        let (uncomp_size, _) = lz4_flex::block::uncompressed_size(&data[off_start..])
            .expect("invalid LZ4 block header");
        total_uncompressed += uncomp_size;
    }

    // Decompress all blocks into a single buffer
    let mut raw = vec![0u8; total_uncompressed];
    let mut buf_pos = 0;
    for i in 0..num_blocks {
        let off_start = block_offset(data, i);
        let off_end = block_offset(data, i + 1);
        let (uncomp_size, compressed) =
            lz4_flex::block::uncompressed_size(&data[off_start..off_end])
                .expect("invalid LZ4 block header");
        lz4_flex::decompress_into(compressed, &mut raw[buf_pos..buf_pos + uncomp_size])
            .expect("LZ4 block decompression failed");
        buf_pos += uncomp_size;
    }

    // Parse strings from the merged buffer
    let mut values = Vec::with_capacity(count);
    let mut offset = 0;
    for _ in 0..count {
        let str_len = u32::from_le_bytes(raw[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let s = std::str::from_utf8(&raw[offset..offset + str_len])
            .expect("invalid UTF-8 in LZ4 data")
            .to_string();
        offset += str_len;
        values.push(s);
    }

    values
}

/// Decode Lz4Blocked data to a merged buffer + ranges, optionally skipping blocks
/// where no row in `selection` is true.
///
/// Returns `(merged_buf, ranges)` where:
/// - For selected rows: `ranges[i]` = `(offset, len)` into `merged_buf`
/// - For skipped rows: `ranges[i]` = `(0, 0)`
///
/// If `selection` is None, all blocks are decoded (full scan path).
///
/// Uses a two-pass approach: first sum uncompressed sizes of needed blocks to
/// pre-allocate the merged buffer, then decompress directly into it — avoiding
/// per-block Vec allocation.
pub fn decode_to_ranges_blocked(
    data: &[u8],
    count: usize,
    selection: Option<&[bool]>,
) -> (Vec<u8>, Vec<(usize, usize)>) {
    if count == 0 {
        return (Vec::new(), Vec::new());
    }

    let block_size = u16::from_le_bytes(data[0..2].try_into().unwrap()) as usize;
    let num_blocks = u16::from_le_bytes(data[2..4].try_into().unwrap()) as usize;

    // First pass: determine which blocks to decode and sum their uncompressed sizes
    let mut block_needed = vec![false; num_blocks];
    let mut total_uncompressed = 0usize;
    let mut global_row = 0;

    for (i, needed) in block_needed.iter_mut().enumerate() {
        let block_count = (count - global_row).min(block_size);
        let need = match selection {
            None => true,
            Some(sel) => sel[global_row..global_row + block_count].iter().any(|&s| s),
        };
        if need {
            *needed = true;
            let off_start = block_offset(data, i);
            let (uncomp_size, _) = lz4_flex::block::uncompressed_size(&data[off_start..])
                .expect("invalid LZ4 block header");
            total_uncompressed += uncomp_size;
        }
        global_row += block_count;
    }

    // Second pass: decompress needed blocks directly into pre-allocated buffer
    let mut merged_buf = vec![0u8; total_uncompressed];
    let mut ranges = vec![(0usize, 0usize); count];
    let mut buf_pos = 0;
    global_row = 0;

    for (i, &needed) in block_needed.iter().enumerate() {
        let block_count = (count - global_row).min(block_size);

        if needed {
            let off_start = block_offset(data, i);
            let off_end = block_offset(data, i + 1);
            let (uncomp_size, compressed) =
                lz4_flex::block::uncompressed_size(&data[off_start..off_end])
                    .expect("invalid LZ4 block header");

            lz4_flex::decompress_into(compressed, &mut merged_buf[buf_pos..buf_pos + uncomp_size])
                .expect("LZ4 block decompression failed");

            // Parse string ranges within the decompressed block
            let mut pos = 0;
            for range in ranges.iter_mut().skip(global_row).take(block_count) {
                let str_len = u32::from_le_bytes(
                    merged_buf[buf_pos + pos..buf_pos + pos + 4]
                        .try_into()
                        .unwrap(),
                ) as usize;
                pos += 4;
                *range = (buf_pos + pos, str_len);
                pos += str_len;
            }
            buf_pos += uncomp_size;
        }

        global_row += block_count;
    }

    (merged_buf, ranges)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_basic() {
        let values = vec!["hello world", "foo bar", "test string"];
        let encoded = encode(&values);
        let decoded = decode(&encoded, values.len());
        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_roundtrip_empty() {
        let encoded = encode(&[]);
        let decoded = decode(&encoded, 0);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_high_cardinality() {
        let strings: Vec<String> = (0..1000)
            .map(|i| format!("unique-string-number-{}-with-some-padding", i))
            .collect();
        let values: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();

        let raw_size: usize = values.iter().map(|s| s.len()).sum();
        let encoded = encode(&values);
        let decoded = decode(&encoded, values.len());

        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);

        // LZ4 should still give some compression due to shared prefixes
        assert!(
            encoded.len() < raw_size,
            "LZ4 should compress, got {} >= {}",
            encoded.len(),
            raw_size
        );
    }

    #[test]
    fn test_empty_strings() {
        let values = vec!["", "", "hello", ""];
        let encoded = encode(&values);
        let decoded = decode(&encoded, values.len());
        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_blocked_roundtrip_basic() {
        let values = vec!["hello", "world", "foo", "bar", "baz"];
        let encoded = encode_blocked(&values, 2);
        let decoded = decode_blocked(&encoded, values.len());
        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_blocked_roundtrip_exact_boundary() {
        let strings: Vec<String> = (0..100).map(|i| format!("val-{}", i)).collect();
        let values: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        let encoded = encode_blocked(&values, 50);
        let decoded = decode_blocked(&encoded, values.len());
        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_blocked_roundtrip_single_block() {
        let values = vec!["a", "b", "c"];
        let encoded = encode_blocked(&values, 1000);
        let decoded = decode_blocked(&encoded, values.len());
        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_blocked_roundtrip_empty() {
        let encoded = encode_blocked(&[], 100);
        let decoded = decode_blocked(&encoded, 0);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_blocked_decode_to_ranges_full() {
        let strings: Vec<String> = (0..250).map(|i| format!("str-{}", i)).collect();
        let values: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        let encoded = encode_blocked(&values, 100);
        let (buf, ranges) = decode_to_ranges_blocked(&encoded, values.len(), None);

        for (i, &(off, len)) in ranges.iter().enumerate() {
            let s = std::str::from_utf8(&buf[off..off + len]).unwrap();
            assert_eq!(s, values[i]);
        }
    }

    #[test]
    fn test_blocked_decode_to_ranges_partial() {
        let strings: Vec<String> = (0..300).map(|i| format!("item-{}", i)).collect();
        let values: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        let encoded = encode_blocked(&values, 100);

        // Select only rows 150..160 (block 1)
        let mut selection = vec![false; 300];
        for sel in &mut selection[150..160] {
            *sel = true;
        }

        let (buf, ranges) = decode_to_ranges_blocked(&encoded, 300, Some(&selection));

        // Skipped blocks should have (0,0)
        for (i, range) in ranges.iter().enumerate().take(100) {
            assert_eq!(*range, (0, 0), "block 0 row {} should be skipped", i);
        }
        for (i, range) in ranges.iter().enumerate().skip(200).take(100) {
            assert_eq!(*range, (0, 0), "block 2 row {} should be skipped", i);
        }

        // Block 1 (rows 100-199) should all be decoded (whole block is decoded)
        for i in 100..200 {
            let (off, len) = ranges[i];
            let s = std::str::from_utf8(&buf[off..off + len]).unwrap();
            assert_eq!(s, values[i]);
        }
    }

    #[test]
    fn test_blocked_large_roundtrip() {
        let strings: Vec<String> = (0..5000)
            .map(|i| format!("unique-string-{}-padding", i))
            .collect();
        let values: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        let encoded = encode_blocked(&values, DEFAULT_BLOCK_SIZE);
        let decoded = decode_blocked(&encoded, values.len());
        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);
    }
}
