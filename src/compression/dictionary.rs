/// Dictionary encoding for low-cardinality TEXT columns.
///
/// Stores a dictionary of unique strings + an array of fixed-width indices.
///
/// Format:
///   4 bytes — dictionary size (number of unique strings, u32 LE)
///   1 byte  — index_width (1 if dict_size ≤ 255, else 2)
///   2 bytes — empty_string_idx (u16 LE, 0xFFFF if no empty string)
///   For each dictionary entry:
///     4 bytes — string length (u32 LE)
///     N bytes — string UTF-8 data
///   Then for each value:
///     index_width bytes — dictionary index (u8 or u16 LE)
///
/// Use dictionary encoding when cardinality < 10% of row count AND < 65536 distinct values.
/// Otherwise fall back to LZ4.
use std::collections::HashMap;

/// Parsed dictionary header — dict entries + metadata for direct index access.
pub struct DictHeader<'a> {
    pub dict: Vec<&'a str>,
    pub index_width: u8,
    pub empty_string_idx: u16,
    pub indices_start: usize,
}

/// Parse header + dictionary entries from encoded data.
pub fn parse_header(data: &[u8]) -> DictHeader<'_> {
    let mut offset = 0;
    let dict_size = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let index_width = data[offset];
    offset += 1;

    let empty_string_idx = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
    offset += 2;

    let mut dict: Vec<&str> = Vec::with_capacity(dict_size);
    for _ in 0..dict_size {
        let str_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let s = std::str::from_utf8(&data[offset..offset + str_len])
            .expect("invalid UTF-8 in dictionary");
        offset += str_len;
        dict.push(s);
    }

    DictHeader {
        dict,
        index_width,
        empty_string_idx,
        indices_start: offset,
    }
}

/// Read a single index from the index array.
#[inline(always)]
pub fn read_index(data: &[u8], indices_start: usize, index_width: u8, row: usize) -> u16 {
    let pos = indices_start + row * index_width as usize;
    if index_width == 1 {
        data[pos] as u16
    } else {
        u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap())
    }
}

/// Returns true if dictionary encoding is suitable for the given values.
pub fn should_use_dictionary(values: &[&str]) -> bool {
    if values.is_empty() {
        return true;
    }
    let mut uniq = std::collections::HashSet::new();
    for v in values {
        uniq.insert(*v);
        // Early exit if cardinality too high
        if uniq.len() > 65535 {
            return false;
        }
    }
    let cardinality = uniq.len();
    cardinality < 65536 && cardinality <= (values.len() / 2).max(1)
}

pub fn encode(values: &[&str]) -> Vec<u8> {
    if values.is_empty() {
        let mut buf = 0u32.to_le_bytes().to_vec();
        buf.push(1); // index_width = 1
        buf.extend_from_slice(&0xFFFFu16.to_le_bytes()); // no empty string
        return buf;
    }

    let mut dict: HashMap<&str, u16> = HashMap::new();
    let mut dict_entries: Vec<&str> = Vec::new();

    for &v in values {
        if !dict.contains_key(v) {
            let idx = dict_entries.len() as u16;
            dict.insert(v, idx);
            dict_entries.push(v);
        }
    }

    let dict_size = dict_entries.len();
    let index_width: u8 = if dict_size <= 255 { 1 } else { 2 };

    // Find empty string index
    let empty_string_idx: u16 = dict_entries
        .iter()
        .position(|&s| s.is_empty())
        .map(|i| i as u16)
        .unwrap_or(0xFFFF);

    let mut buf = Vec::new();

    // Dictionary size
    buf.extend_from_slice(&(dict_size as u32).to_le_bytes());

    // Index width
    buf.push(index_width);

    // Empty string index
    buf.extend_from_slice(&empty_string_idx.to_le_bytes());

    // Dictionary entries
    for &entry in &dict_entries {
        let bytes = entry.as_bytes();
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(bytes);
    }

    // Fixed-width indices
    for &v in values {
        let idx = dict[v];
        if index_width == 1 {
            buf.push(idx as u8);
        } else {
            buf.extend_from_slice(&idx.to_le_bytes());
        }
    }

    buf
}

/// Decode dictionary-encoded data, returning borrowed &str slices.
/// Avoids N String allocations by referencing the dictionary entries in-place.
pub fn decode_to_slices(data: &[u8], count: usize) -> Vec<&str> {
    if count == 0 {
        return Vec::new();
    }

    let hdr = parse_header(data);
    let mut values = Vec::with_capacity(count);
    for i in 0..count {
        let idx = read_index(data, hdr.indices_start, hdr.index_width, i);
        values.push(hdr.dict[idx as usize]);
    }
    values
}

/// Byte-level variant of `parse_header` that does NOT validate UTF-8.
/// Used for jsonb columns where the stored dictionary entries are binary
/// jsonb varlena payloads, not UTF-8 text.
pub fn parse_header_bytes(data: &[u8]) -> (Vec<&[u8]>, usize, u8) {
    let mut offset = 0;
    let dict_size = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let index_width = data[offset];
    offset += 1;

    // skip empty_string_idx (2 bytes) — unused for binary blobs
    offset += 2;

    let mut dict: Vec<&[u8]> = Vec::with_capacity(dict_size);
    for _ in 0..dict_size {
        let str_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        dict.push(&data[offset..offset + str_len]);
        offset += str_len;
    }
    (dict, offset, index_width)
}

/// Byte-level variant of `decode_to_slices` — returns `&[u8]` instead of `&str`.
/// Skips UTF-8 validation so it works for jsonb (binary) payloads.
pub fn decode_to_byte_slices(data: &[u8], count: usize) -> Vec<&[u8]> {
    if count == 0 {
        return Vec::new();
    }
    let (dict, indices_start, index_width) = parse_header_bytes(data);
    let mut values = Vec::with_capacity(count);
    for i in 0..count {
        let idx = read_index(data, indices_start, index_width, i);
        values.push(dict[idx as usize]);
    }
    values
}

/// Decode dictionary-encoded data, returning the dictionary entries and per-row indices separately.
/// This allows matching against only the dictionary entries (e.g. for LIKE filtering)
/// instead of resolving every row.
pub fn decode_dict_and_indices(data: &[u8], count: usize) -> (Vec<&str>, Vec<u16>) {
    if count == 0 {
        return (Vec::new(), Vec::new());
    }

    let hdr = parse_header(data);
    let mut indices = Vec::with_capacity(count);
    for i in 0..count {
        indices.push(read_index(data, hdr.indices_start, hdr.index_width, i));
    }

    (hdr.dict, indices)
}

pub fn decode(data: &[u8], count: usize) -> Vec<String> {
    if count == 0 {
        return Vec::new();
    }

    let hdr = parse_header(data);
    let mut values = Vec::with_capacity(count);
    for i in 0..count {
        let idx = read_index(data, hdr.indices_start, hdr.index_width, i);
        values.push(hdr.dict[idx as usize].to_string());
    }
    values
}

/// Encode with LZ4-compressed dictionary entries.
///
/// Same as `encode()` but sorts dictionary entries alphabetically (improves LZ4 ratio
/// for strings sharing common prefixes like URLs) and LZ4-compresses the dictionary blob.
///
/// Wire format:
///   [4B dict_size][1B index_width][2B empty_string_idx]
///   [4B lz4_blob_len]
///   [LZ4 blob: compress_prepend_size([4B len_0][str_0][4B len_1][str_1]...)]
///   [indices: index_width × value_count bytes]
pub fn encode_lz4(values: &[&str]) -> Vec<u8> {
    if values.is_empty() {
        let mut buf = 0u32.to_le_bytes().to_vec();
        buf.push(1); // index_width = 1
        buf.extend_from_slice(&0xFFFFu16.to_le_bytes()); // no empty string
        buf.extend_from_slice(&0u32.to_le_bytes()); // lz4_blob_len = 0
        return buf;
    }

    // Build dictionary
    let mut dict: HashMap<&str, u16> = HashMap::new();
    let mut dict_entries: Vec<&str> = Vec::new();

    for &v in values {
        if !dict.contains_key(v) {
            dict.insert(v, dict_entries.len() as u16);
            dict_entries.push(v);
        }
    }

    // Sort entries alphabetically for better LZ4 prefix compression
    let mut sorted_entries = dict_entries.clone();
    sorted_entries.sort_unstable();

    // Build remap: old_idx → new_idx
    let mut remap = vec![0u16; dict_entries.len()];
    let mut sorted_map: HashMap<&str, u16> = HashMap::new();
    for (new_idx, &entry) in sorted_entries.iter().enumerate() {
        sorted_map.insert(entry, new_idx as u16);
    }
    for (old_idx, &entry) in dict_entries.iter().enumerate() {
        remap[old_idx] = sorted_map[entry];
    }

    let dict_size = sorted_entries.len();
    let index_width: u8 = if dict_size <= 255 { 1 } else { 2 };

    // Find empty string index in sorted order
    let empty_string_idx: u16 = sorted_entries
        .iter()
        .position(|&s| s.is_empty())
        .map(|i| i as u16)
        .unwrap_or(0xFFFF);

    // Serialize dictionary entries into raw buffer for LZ4 compression
    let total_raw: usize = sorted_entries.iter().map(|s| 4 + s.len()).sum();
    let mut raw = Vec::with_capacity(total_raw);
    for &entry in &sorted_entries {
        let bytes = entry.as_bytes();
        raw.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        raw.extend_from_slice(bytes);
    }

    let compressed = lz4_flex::compress_prepend_size(&raw);

    // Build output
    let mut buf = Vec::new();
    buf.extend_from_slice(&(dict_size as u32).to_le_bytes());
    buf.push(index_width);
    buf.extend_from_slice(&empty_string_idx.to_le_bytes());
    buf.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    buf.extend_from_slice(&compressed);

    // Remapped indices
    for &v in values {
        let old_idx = dict[v];
        let new_idx = remap[old_idx as usize];
        if index_width == 1 {
            buf.push(new_idx as u8);
        } else {
            buf.extend_from_slice(&new_idx.to_le_bytes());
        }
    }

    buf
}

/// Flat-decoded dictionary: all entry bytes in one owned buffer plus
/// per-entry (offset, len) ranges into it. Avoids one `String` allocation
/// per entry and per-entry UTF-8 validation (entries were written by our
/// own encoder from valid PG text).
pub struct FlatDict {
    pub buf: Vec<u8>,
    pub entry_ranges: Vec<(u32, u32)>,
}

/// Walk a `[4B len][bytes]…` entries blob, returning per-entry ranges into it
/// (ranges skip the 4-byte length prefixes).
fn entry_ranges_in(blob: &[u8], dict_size: usize) -> Vec<(u32, u32)> {
    let mut ranges = Vec::with_capacity(dict_size);
    let mut off = 0usize;
    for _ in 0..dict_size {
        let len = u32::from_le_bytes(blob[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        ranges.push((off as u32, len as u32));
        off += len;
    }
    ranges
}

/// Decode a plain-`Dictionary` blob into a `FlatDict` plus per-row indices
/// (`count` = number of non-null rows). One bulk copy of the entries region,
/// no per-entry allocations, no UTF-8 validation.
pub fn decode_flat(data: &[u8], count: usize) -> (FlatDict, Vec<u16>) {
    let dict_size = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let index_width = data[4];
    // Entries region starts right after [4B dict_size][1B width][2B empty_idx].
    let entry_ranges = entry_ranges_in(&data[7..], dict_size);
    let entries_len = entry_ranges.last().map_or(0, |&(o, l)| (o + l) as usize);
    let buf = data[7..7 + entries_len].to_vec();
    let indices_start = 7 + entries_len;
    let mut indices = Vec::with_capacity(count);
    for i in 0..count {
        indices.push(read_index(data, indices_start, index_width, i));
    }
    (FlatDict { buf, entry_ranges }, indices)
}

/// Decode a `DictionaryLz4` blob into a `FlatDict` plus per-row indices.
/// Decompresses only the entries blob and reads indices directly from
/// `data` — avoids `normalize_lz4`'s full-buffer reassembly (which copies
/// the indices array a second time).
pub fn decode_flat_lz4(data: &[u8], count: usize) -> (FlatDict, Vec<u16>) {
    let dict_size = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let index_width = data[4];
    let lz4_blob_len = u32::from_le_bytes(data[7..11].try_into().unwrap()) as usize;
    let buf = if lz4_blob_len > 0 {
        lz4_flex::decompress_size_prepended(&data[11..11 + lz4_blob_len])
            .expect("LZ4 decompression failed in DictionaryLz4")
    } else {
        Vec::new()
    };
    let entry_ranges = entry_ranges_in(&buf, dict_size);
    let indices_start = 11 + lz4_blob_len;
    let mut indices = Vec::with_capacity(count);
    for i in 0..count {
        indices.push(read_index(data, indices_start, index_width, i));
    }
    (FlatDict { buf, entry_ranges }, indices)
}

/// Decompress DictionaryLz4 data back to plain Dictionary wire format.
///
/// Reads the LZ4-compressed dictionary entries, decompresses them, and returns
/// a byte vector in the same layout as plain Dictionary encoding. This allows
/// all existing Dictionary code paths to work unchanged.
pub fn normalize_lz4(data: &[u8]) -> Vec<u8> {
    let dict_size_bytes = &data[0..4];
    let index_width = data[4];
    let empty_string_idx_bytes = &data[5..7];

    let lz4_blob_len = u32::from_le_bytes(data[7..11].try_into().unwrap()) as usize;

    let decompressed = if lz4_blob_len > 0 {
        lz4_flex::decompress_size_prepended(&data[11..11 + lz4_blob_len])
            .expect("LZ4 decompression failed in DictionaryLz4")
    } else {
        Vec::new()
    };

    let indices = &data[11 + lz4_blob_len..];

    // Reconstruct plain Dictionary format: header + decompressed entries + indices
    let mut buf = Vec::with_capacity(7 + decompressed.len() + indices.len());
    buf.extend_from_slice(dict_size_bytes);
    buf.push(index_width);
    buf.extend_from_slice(empty_string_idx_bytes);
    buf.extend_from_slice(&decompressed);
    buf.extend_from_slice(indices);
    buf
}

/// Check `<> ''` for each row using the precomputed empty_string_idx.
/// Returns a Vec<bool> where true = non-empty (passes filter).
/// Returns empty Vec if no empty string in dictionary (all pass).
pub fn check_ne_empty(data: &[u8], count: usize) -> Vec<bool> {
    if count == 0 {
        return Vec::new();
    }

    let hdr = parse_header(data);
    if hdr.empty_string_idx == 0xFFFF {
        // No empty string in dictionary — all rows pass
        return Vec::new();
    }

    let empty_idx = hdr.empty_string_idx;
    let mut sel = Vec::with_capacity(count);
    for i in 0..count {
        let idx = read_index(data, hdr.indices_start, hdr.index_width, i);
        sel.push(idx != empty_idx);
    }
    sel
}

/// Check whether any dictionary entry satisfies the given predicate.
///
/// Parses only the dictionary header (not the per-row indices), so this is
/// O(dict_size) regardless of row count. Useful for segment pruning: if no
/// dictionary entry matches a LIKE pattern, the entire segment can be skipped.
pub fn any_entry_matches(data: &[u8], predicate: impl Fn(&str) -> bool) -> bool {
    let hdr = parse_header(data);
    hdr.dict.iter().any(|&s| predicate(s))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_basic() {
        let values = vec!["hello", "world", "hello", "world", "hello"];
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
    fn test_roundtrip_single() {
        let values = vec!["test"];
        let encoded = encode(&values);
        let decoded = decode(&encoded, 1);
        assert_eq!(decoded, vec!["test".to_string()]);
    }

    #[test]
    fn test_compression_ratio() {
        // Low cardinality: 10 device IDs repeated 1000 times
        let devices: Vec<String> = (0..10).map(|i| format!("device-{:04}", i)).collect();
        let values: Vec<&str> = (0..10000).map(|i| devices[i % 10].as_str()).collect();

        let raw_size: usize = values.iter().map(|s| s.len()).sum();
        let encoded = encode(&values);
        let decoded = decode(&encoded, values.len());

        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);

        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(
            ratio > 5.0,
            "low-cardinality text should compress >5x, got {:.1}x",
            ratio
        );
    }

    #[test]
    fn test_should_use_dictionary() {
        // Low cardinality
        let values: Vec<&str> = vec!["a", "b", "c", "a", "b", "c", "a", "b", "c", "a", "b"];
        assert!(should_use_dictionary(&values));

        // High cardinality — every value unique
        let strings: Vec<String> = (0..100).map(|i| format!("unique-{}", i)).collect();
        let values: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        assert!(!should_use_dictionary(&values));
    }

    #[test]
    fn test_utf8_strings() {
        let values = vec!["héllo", "wörld", "日本語", "🎉"];
        let encoded = encode(&values);
        let decoded = decode(&encoded, values.len());
        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_fixed_width_u8_indices() {
        // Dict size ≤ 255 → index_width = 1
        let values = vec!["a", "b", "c", "a", "b"];
        let encoded = encode(&values);
        let hdr = parse_header(&encoded);
        assert_eq!(hdr.index_width, 1);
        assert_eq!(hdr.dict.len(), 3);

        let decoded = decode(&encoded, values.len());
        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_fixed_width_u16_indices() {
        // Dict size > 255 → index_width = 2
        let strings: Vec<String> = (0..300).map(|i| format!("val-{}", i)).collect();
        // Repeat to satisfy should_use_dictionary cardinality check
        let mut values: Vec<&str> = Vec::new();
        for _ in 0..3 {
            for s in &strings {
                values.push(s.as_str());
            }
        }

        let encoded = encode(&values);
        let hdr = parse_header(&encoded);
        assert_eq!(hdr.index_width, 2);
        assert_eq!(hdr.dict.len(), 300);

        let decoded = decode(&encoded, values.len());
        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_decode_to_slices() {
        let values = vec!["hello", "world", "hello", "world"];
        let encoded = encode(&values);
        let slices = decode_to_slices(&encoded, values.len());
        assert_eq!(slices, values);
    }

    #[test]
    fn test_decode_dict_and_indices() {
        let values = vec!["hello", "world", "hello"];
        let encoded = encode(&values);
        let (dict, indices) = decode_dict_and_indices(&encoded, values.len());
        assert_eq!(dict, vec!["hello", "world"]);
        assert_eq!(indices, vec![0u16, 1, 0]);
    }

    #[test]
    fn test_empty_string_idx() {
        // With empty string
        let values = vec!["hello", "", "world", ""];
        let encoded = encode(&values);
        let hdr = parse_header(&encoded);
        assert_eq!(hdr.empty_string_idx, 1); // "" is second dict entry

        // Without empty string
        let values = vec!["hello", "world"];
        let encoded = encode(&values);
        let hdr = parse_header(&encoded);
        assert_eq!(hdr.empty_string_idx, 0xFFFF);
    }

    #[test]
    fn test_check_ne_empty() {
        let values = vec!["hello", "", "world", "", "hello"];
        let encoded = encode(&values);

        let sel = check_ne_empty(&encoded, values.len());
        assert_eq!(sel, vec![true, false, true, false, true]);
    }

    #[test]
    fn test_check_ne_empty_no_empty_strings() {
        let values = vec!["hello", "world", "hello"];
        let encoded = encode(&values);

        let sel = check_ne_empty(&encoded, values.len());
        assert!(sel.is_empty()); // All pass — no filtering needed
    }

    // ==================== DictionaryLz4 tests ====================

    #[test]
    fn test_lz4_roundtrip_basic() {
        let values = vec!["hello", "world", "hello", "world", "hello"];
        let encoded = encode_lz4(&values);
        let normalized = normalize_lz4(&encoded);
        let decoded = decode(&normalized, values.len());
        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_lz4_roundtrip_empty_values() {
        let encoded = encode_lz4(&[]);
        let normalized = normalize_lz4(&encoded);
        let decoded = decode(&normalized, 0);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_lz4_roundtrip_single() {
        let values = vec!["test"];
        let encoded = encode_lz4(&values);
        let normalized = normalize_lz4(&encoded);
        let decoded = decode(&normalized, 1);
        assert_eq!(decoded, vec!["test".to_string()]);
    }

    #[test]
    fn test_lz4_sorted_dictionary() {
        let values = vec!["cherry", "apple", "banana", "apple", "cherry"];
        let encoded = encode_lz4(&values);
        let normalized = normalize_lz4(&encoded);

        // Dictionary should be sorted alphabetically
        let hdr = parse_header(&normalized);
        assert_eq!(hdr.dict, vec!["apple", "banana", "cherry"]);

        // But decoded values preserve original order
        let decoded = decode(&normalized, values.len());
        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_lz4_with_empty_strings() {
        let values = vec!["hello", "", "world", ""];
        let encoded = encode_lz4(&values);
        let normalized = normalize_lz4(&encoded);

        let hdr = parse_header(&normalized);
        // Empty string sorts first alphabetically
        assert_eq!(hdr.empty_string_idx, 0);

        let decoded = decode(&normalized, values.len());
        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_lz4_check_ne_empty() {
        let values = vec!["hello", "", "world", "", "hello"];
        let encoded = encode_lz4(&values);
        let normalized = normalize_lz4(&encoded);

        let sel = check_ne_empty(&normalized, values.len());
        assert_eq!(sel, vec![true, false, true, false, true]);
    }

    #[test]
    fn test_lz4_decode_dict_and_indices() {
        let values = vec!["banana", "apple", "banana"];
        let encoded = encode_lz4(&values);
        let normalized = normalize_lz4(&encoded);
        let (dict, indices) = decode_dict_and_indices(&normalized, values.len());
        // Sorted: apple=0, banana=1
        assert_eq!(dict, vec!["apple", "banana"]);
        assert_eq!(indices, vec![1u16, 0, 1]);
    }

    #[test]
    fn test_lz4_compression_benefit() {
        // URLs sharing common prefixes should compress well with sorted dictionary
        let urls: Vec<String> = (0..100)
            .map(|i| format!("https://www.example.com/page/{}/content", i))
            .collect();
        let mut values: Vec<&str> = Vec::new();
        for _ in 0..10 {
            for u in &urls {
                values.push(u.as_str());
            }
        }

        let dict_encoded = encode(&values);
        let lz4_encoded = encode_lz4(&values);
        assert!(
            lz4_encoded.len() < dict_encoded.len(),
            "LZ4 dictionary ({}) should be smaller than plain dictionary ({})",
            lz4_encoded.len(),
            dict_encoded.len()
        );
    }

    #[test]
    fn test_lz4_u16_indices() {
        // Dict size > 255 → index_width = 2
        let strings: Vec<String> = (0..300).map(|i| format!("val-{:04}", i)).collect();
        let mut values: Vec<&str> = Vec::new();
        for _ in 0..3 {
            for s in &strings {
                values.push(s.as_str());
            }
        }

        let encoded = encode_lz4(&values);
        let normalized = normalize_lz4(&encoded);
        let hdr = parse_header(&normalized);
        assert_eq!(hdr.index_width, 2);
        assert_eq!(hdr.dict.len(), 300);

        let decoded = decode(&normalized, values.len());
        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_lz4_utf8_strings() {
        let values = vec!["héllo", "wörld", "日本語", "🎉"];
        let encoded = encode_lz4(&values);
        let normalized = normalize_lz4(&encoded);
        let decoded = decode(&normalized, values.len());
        let expected: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        assert_eq!(decoded, expected);
    }
}
