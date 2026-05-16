//! Shared text column primitives for parallel decompress and aggregation paths.
//!
//! All types and functions here are pure Rust (no PG API calls) and thread-safe,
//! suitable for use in `std::thread::scope` workers.

use super::batch_qual::LikeStrategy;
use super::datum_utils::count_non_null;
use crate::compression;

/// Keeps decompressed string data alive during the row loop,
/// providing O(1) &str access per row without interning.
pub(super) enum SegTextColumn {
    /// Dictionary-compressed: dict entries + per-row index (null-expanded).
    Dict {
        entries: Vec<String>,
        /// Per-row index into `entries`. u32::MAX = null.
        row_to_entry: Vec<u32>,
    },
    /// LZ4/LZ4Blocked: decompressed buffer + per-row range (null-expanded).
    Lz4 {
        buf: Vec<u8>,
        /// Per-row (offset, len). offset == u32::MAX means null.
        row_to_range: Vec<(u32, u16)>,
    },
    /// Segment-by column: same value for all rows.
    SegBy(Option<String>),
    /// Length-sidecar only: per-row character count; no string body available.
    /// `null_bitmap[row / 8] & (1 << (row % 8))` == 1 means null; empty bitmap = all non-null.
    /// Callers using `get_str` on this variant will get `None`, which matches the
    /// semantics of a null row — so any code path that actually needs string bytes
    /// must check the variant beforehand. The query planner only routes a column
    /// to this variant when every usage is `length(col)`, `col = ''`, `col <> ''`,
    /// or IS [NOT] NULL.
    Lengths {
        lengths: Vec<u32>,
        null_bitmap: Vec<u8>,
    },
}

impl SegTextColumn {
    /// Get the string for a given row, or None if null. Returns None for the
    /// Lengths variant since string bytes are not available there.
    pub(super) fn get_str(&self, row: usize) -> Option<&str> {
        match self {
            SegTextColumn::Dict {
                entries,
                row_to_entry,
            } => {
                let idx = row_to_entry[row];
                if idx == u32::MAX {
                    None
                } else {
                    Some(&entries[idx as usize])
                }
            }
            SegTextColumn::Lz4 { buf, row_to_range } => {
                let (off, len) = row_to_range[row];
                if off == u32::MAX {
                    None
                } else {
                    Some(
                        std::str::from_utf8(&buf[off as usize..off as usize + len as usize])
                            .unwrap_or(""),
                    )
                }
            }
            SegTextColumn::SegBy(opt) => opt.as_deref(),
            SegTextColumn::Lengths { .. } => None,
        }
    }

    /// Phase D bitset path: return the local dict ID at `row` if this column
    /// is `Dict`-encoded and the row is non-null. Returns `None` for nulls,
    /// non-Dict variants (Lz4/Lengths/SegBy don't have a per-row dict
    /// reference). Callers pair this with a leader-precomputed
    /// local→global remap table to set bits in a per-group bitset.
    pub(super) fn dict_local_id(&self, row: usize) -> Option<u32> {
        match self {
            SegTextColumn::Dict { row_to_entry, .. } => {
                let idx = row_to_entry[row];
                if idx == u32::MAX { None } else { Some(idx) }
            }
            _ => None,
        }
    }

    /// Get the character length of the value at a given row, or None if null.
    /// All variants support this — Dict/Lz4/SegBy compute from the string body,
    /// Lengths reads directly from the stored array.
    pub(super) fn get_len(&self, row: usize) -> Option<usize> {
        match self {
            SegTextColumn::Dict {
                entries,
                row_to_entry,
            } => {
                let idx = row_to_entry[row];
                if idx == u32::MAX {
                    None
                } else {
                    Some(entries[idx as usize].chars().count())
                }
            }
            SegTextColumn::Lz4 { buf, row_to_range } => {
                let (off, len) = row_to_range[row];
                if off == u32::MAX {
                    None
                } else {
                    let slice = &buf[off as usize..off as usize + len as usize];
                    Some(std::str::from_utf8(slice).unwrap_or("").chars().count())
                }
            }
            SegTextColumn::SegBy(opt) => opt.as_deref().map(|s| s.chars().count()),
            SegTextColumn::Lengths {
                lengths,
                null_bitmap,
            } => {
                if null_at(null_bitmap, row) {
                    None
                } else {
                    Some(lengths[row] as usize)
                }
            }
        }
    }
}

/// Pre-extracted text qual info for worker threads.
#[derive(Clone)]
pub(super) enum TextQualInfo {
    EqNe {
        col_idx: usize,
        const_str: String,
        is_ne: bool,
    },
    Like {
        col_idx: usize,
        strategy: LikeStrategy,
        negate: bool,
    },
    /// `col IN (v1, v2, ...)` / `col = ANY(ARRAY[...])`. Per-segment dict
    /// fast path tests each unique dict entry against the values once and
    /// reuses the answer per row. Negated IN (`NOT IN`) is *not* supported
    /// here — PG generates `<> ALL(ARRAY[...])` for that, which surfaces as
    /// an `op_negate` SAOP we currently bail on at the planner gate.
    InList { col_idx: usize, values: Vec<String> },
}

/// Decode a per-row length sidecar blob into a SegTextColumn::Lengths (pure Rust).
pub(super) fn decompress_length_sidecar(blob: &[u8]) -> Option<SegTextColumn> {
    if blob.is_empty() {
        return None;
    }
    let cc = compression::CompressedColumnRef::from_bytes(blob);
    // Only Lz4 raw-u32 encoding is produced today; reject anything else.
    if cc.type_tag != compression::CompressionType::Lz4 {
        return None;
    }
    let raw = lz4_flex::decompress_size_prepended(cc.data).ok()?;
    let row_count = cc.row_count as usize;
    let mut lengths = vec![0u32; row_count];
    if cc.null_bitmap.is_empty() {
        if raw.len() != row_count * 4 {
            return None;
        }
        for (i, slot) in lengths.iter_mut().enumerate() {
            *slot = u32::from_le_bytes(raw[i * 4..i * 4 + 4].try_into().ok()?);
        }
        Some(SegTextColumn::Lengths {
            lengths,
            null_bitmap: Vec::new(),
        })
    } else {
        let mut vi = 0;
        for (i, slot) in lengths.iter_mut().enumerate() {
            let is_null = (cc.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
            if !is_null {
                *slot = u32::from_le_bytes(raw[vi * 4..vi * 4 + 4].try_into().ok()?);
                vi += 1;
            }
        }
        Some(SegTextColumn::Lengths {
            lengths,
            null_bitmap: cc.null_bitmap.to_vec(),
        })
    }
}

/// Decompress a text column blob into a SegTextColumn (pure Rust, thread-safe).
pub(super) fn decompress_text_to_seg_col(blob: &[u8]) -> Option<SegTextColumn> {
    if blob.is_empty() {
        return None;
    }
    let cc = compression::CompressedColumnRef::from_bytes(blob);
    let total = cc.row_count as usize;
    let nn_count = count_non_null(cc.null_bitmap, total);

    match cc.type_tag {
        compression::CompressionType::Dictionary | compression::CompressionType::DictionaryLz4 => {
            let norm_buf;
            let dict_data = if cc.type_tag == compression::CompressionType::DictionaryLz4 {
                norm_buf = compression::dictionary::normalize_lz4(cc.data);
                &norm_buf[..]
            } else {
                cc.data
            };
            let (dict_entries, nn_indices) =
                compression::dictionary::decode_dict_and_indices(dict_data, nn_count);
            let entries: Vec<String> = dict_entries.iter().map(|&s| s.to_string()).collect();

            let row_to_entry = if cc.null_bitmap.is_empty() {
                nn_indices.iter().map(|&idx| idx as u32).collect()
            } else {
                let mut re = Vec::with_capacity(total);
                let mut vi = 0;
                for i in 0..total {
                    let is_null = (cc.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
                    if is_null {
                        re.push(u32::MAX);
                    } else {
                        re.push(nn_indices[vi] as u32);
                        vi += 1;
                    }
                }
                re
            };
            Some(SegTextColumn::Dict {
                entries,
                row_to_entry,
            })
        }
        compression::CompressionType::Lz4 | compression::CompressionType::Lz4Blocked => {
            let (buf, ranges) = if cc.type_tag == compression::CompressionType::Lz4 {
                compression::lz4::decode_to_ranges(cc.data, nn_count)
            } else {
                compression::lz4::decode_to_ranges_blocked(cc.data, nn_count, None)
            };

            let row_to_range = if cc.null_bitmap.is_empty() {
                ranges
                    .iter()
                    .map(|&(off, len)| (off as u32, len as u16))
                    .collect()
            } else {
                let mut rr = Vec::with_capacity(total);
                let mut vi = 0;
                for i in 0..total {
                    let is_null = (cc.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
                    if is_null {
                        rr.push((u32::MAX, 0u16));
                    } else {
                        let (off, len) = ranges[vi];
                        rr.push((off as u32, len as u16));
                        vi += 1;
                    }
                }
                rr
            };
            Some(SegTextColumn::Lz4 { buf, row_to_range })
        }
        _ => None,
    }
}

/// Is row `i` marked null in `null_bitmap`? Treats an empty bitmap as
/// "no nulls". Bit packing matches the on-wire format produced by
/// `compression::extract_nulls`.
#[inline]
fn null_at(null_bitmap: &[u8], row: usize) -> bool {
    !null_bitmap.is_empty() && (null_bitmap[row / 8] >> (row % 8)) & 1 == 1
}

/// AND a freshly-built dict-keyed match table into `sel`. When `sel` is
/// empty it gets initialised (one entry per row); otherwise existing
/// `false` rows short-circuit.
///
/// `dict_matches[idx]` is the precomputed bool for dict entry `idx`;
/// `row_to_entry[row]` is the dict index for that row, or `u32::MAX`
/// for null. Null rows always end up `false`.
#[inline]
fn apply_via_dict(
    sel: &mut Vec<bool>,
    row_count: usize,
    row_to_entry: &[u32],
    dict_matches: &[bool],
) {
    let pass_at = |row: usize| -> bool {
        let idx = row_to_entry[row];
        idx != u32::MAX && dict_matches[idx as usize]
    };
    if sel.is_empty() {
        sel.reserve(row_count);
        for row in 0..row_count {
            sel.push(pass_at(row));
        }
    } else {
        for (row, s) in sel.iter_mut().enumerate() {
            if !*s {
                continue;
            }
            *s = pass_at(row);
        }
    }
}

/// Generic fallback: evaluate `pred(row)` per row, AND-ing into `sel`.
/// Used by the non-Dict / non-Lengths branches of every text filter
/// helper. `pred` is invoked exactly once per row that hasn't already
/// been excluded by a prior qual.
#[inline]
fn apply_per_row<F: Fn(usize) -> bool>(sel: &mut Vec<bool>, row_count: usize, pred: F) {
    if sel.is_empty() {
        sel.reserve(row_count);
        for row in 0..row_count {
            sel.push(pred(row));
        }
    } else {
        for (row, s) in sel.iter_mut().enumerate() {
            if !*s {
                continue;
            }
            *s = pred(row);
        }
    }
}

/// Apply a text EQ/NE filter to a SegTextColumn, AND-ing into an existing selection.
///
/// If `sel` is empty, it is initialized (all rows evaluated).
/// If `sel` is non-empty, rows already false are skipped (short-circuit).
pub(super) fn apply_text_eq_filter(
    seg_col: &SegTextColumn,
    const_str: &str,
    is_ne: bool,
    row_count: usize,
    sel: &mut Vec<bool>,
) {
    // Length-sidecar fast path: only "" comparisons are resolvable (length == 0 / > 0).
    // Non-empty const_str on a Lengths column means we can't evaluate — the planner
    // is supposed to prevent this, but fail safe by zeroing the selection.
    if let SegTextColumn::Lengths {
        lengths,
        null_bitmap,
    } = seg_col
    {
        if const_str.is_empty() {
            apply_per_row(sel, row_count, |row| {
                if null_at(null_bitmap, row) {
                    return false;
                }
                let is_empty = lengths[row] == 0;
                if is_ne { !is_empty } else { is_empty }
            });
        } else {
            // Can't evaluate a non-empty equality on lengths alone — drop all rows.
            if sel.is_empty() {
                sel.resize(row_count, false);
            } else {
                sel.iter_mut().for_each(|s| *s = false);
            }
        }
        return;
    }

    let eq_pred = |s: &str| -> bool {
        let eq = s == const_str;
        if is_ne { !eq } else { eq }
    };

    match seg_col {
        SegTextColumn::Dict {
            entries,
            row_to_entry,
        } => {
            // Dict fast path: precompute pass-bool per dict entry, then O(1) per row.
            let dict_matches: Vec<bool> = entries.iter().map(|s| eq_pred(s.as_str())).collect();
            apply_via_dict(sel, row_count, row_to_entry, &dict_matches);
        }
        _ => {
            apply_per_row(sel, row_count, |row| match seg_col.get_str(row) {
                Some(s) => eq_pred(s),
                None => false,
            });
        }
    }
}

/// Apply a text IN filter to a SegTextColumn, AND-ing into an existing selection.
///
/// If `sel` is empty, it is initialized (all rows evaluated). If `sel` is
/// non-empty, rows already false are skipped (short-circuit).
///
/// Dict fast path: build a bool vector keyed by dict entry — one membership
/// scan over `entries × values`, then `O(1)` per row. For low-cardinality
/// columns like `x_collection` (~16 unique values) this collapses the
/// per-row IN check to a single byte read.
///
/// Length-sidecar columns can only resolve `'' IN (...)` cleanly — the
/// runtime plan never reaches this with a non-empty IN list against a
/// length sidecar (the planner gate routes to a `Lengths`-eligible op),
/// but we fail safe by zeroing the selection.
pub(super) fn apply_text_in_filter(
    seg_col: &SegTextColumn,
    values: &[String],
    row_count: usize,
    sel: &mut Vec<bool>,
) {
    if let SegTextColumn::Lengths {
        lengths,
        null_bitmap,
    } = seg_col
    {
        let allow_empty = values.iter().any(|s| s.is_empty());
        apply_per_row(sel, row_count, |row| {
            if null_at(null_bitmap, row) {
                return false;
            }
            allow_empty && lengths[row] == 0
        });
        return;
    }

    let in_pred = |s: &str| values.iter().any(|cand| cand.as_str() == s);

    match seg_col {
        SegTextColumn::Dict {
            entries,
            row_to_entry,
        } => {
            // Build dict-entry → bool table once per segment. O(|entries| × |values|).
            let dict_matches: Vec<bool> = entries.iter().map(|s| in_pred(s.as_str())).collect();
            apply_via_dict(sel, row_count, row_to_entry, &dict_matches);
        }
        _ => {
            apply_per_row(sel, row_count, |row| match seg_col.get_str(row) {
                Some(s) => in_pred(s),
                None => false,
            });
        }
    }
}

/// Apply a text LIKE filter to a SegTextColumn, AND-ing into an existing selection.
///
/// If `sel` is empty, it is initialized (all rows evaluated).
/// If `sel` is non-empty, rows already false are skipped (short-circuit).
pub(super) fn apply_text_like_filter(
    seg_col: &SegTextColumn,
    strategy: &LikeStrategy,
    negate: bool,
    row_count: usize,
    sel: &mut Vec<bool>,
) {
    use super::batch_qual::sql_like_match;

    let matches_like = |text: &str| -> bool {
        let matched = match strategy {
            LikeStrategy::Contains(s) => text.contains(s.as_str()),
            LikeStrategy::StartsWith(s) => text.starts_with(s.as_str()),
            LikeStrategy::EndsWith(s) => text.ends_with(s.as_str()),
            LikeStrategy::Exact(s) => text == s.as_str(),
            LikeStrategy::General(p) => sql_like_match(text, p),
        };
        if negate { !matched } else { matched }
    };

    match seg_col {
        SegTextColumn::Dict {
            entries,
            row_to_entry,
        } => {
            // Dict fast path: match against unique dict entries only.
            let dict_matches: Vec<bool> = entries.iter().map(|s| matches_like(s)).collect();
            apply_via_dict(sel, row_count, row_to_entry, &dict_matches);
        }
        _ => {
            apply_per_row(sel, row_count, |row| match seg_col.get_str(row) {
                Some(s) => matches_like(s),
                None => false,
            });
        }
    }
}

/// Collation-aware string comparison using libc `strcoll`.
///
/// Safe to call from non-PG worker threads. On glibc, `strcoll` is MT-Safe
/// and uses the process-wide locale set by `setlocale(LC_COLLATE, ...)`.
/// Strings are null-terminated via a small stack buffer or heap fallback.
pub(super) fn strcoll_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    // Fast path: if both are equal bytes, they're equal in any collation
    if a.as_bytes() == b.as_bytes() {
        return std::cmp::Ordering::Equal;
    }

    unsafe extern "C" {
        fn strcoll(s1: *const std::ffi::c_char, s2: *const std::ffi::c_char) -> std::ffi::c_int;
    }

    // Null-terminate strings for strcoll.
    // Use stack buffers for short strings to avoid allocation in the hot path.
    const STACK_BUF: usize = 512;
    let mut buf_a = [0u8; STACK_BUF];
    let mut buf_b = [0u8; STACK_BUF];
    let mut heap_a: Vec<u8>;
    let mut heap_b: Vec<u8>;

    let ptr_a = if a.len() < STACK_BUF {
        buf_a[..a.len()].copy_from_slice(a.as_bytes());
        buf_a[a.len()] = 0;
        buf_a.as_ptr() as *const std::ffi::c_char
    } else {
        heap_a = Vec::with_capacity(a.len() + 1);
        heap_a.extend_from_slice(a.as_bytes());
        heap_a.push(0);
        heap_a.as_ptr() as *const std::ffi::c_char
    };

    let ptr_b = if b.len() < STACK_BUF {
        buf_b[..b.len()].copy_from_slice(b.as_bytes());
        buf_b[b.len()] = 0;
        buf_b.as_ptr() as *const std::ffi::c_char
    } else {
        heap_b = Vec::with_capacity(b.len() + 1);
        heap_b.extend_from_slice(b.as_bytes());
        heap_b.push(0);
        heap_b.as_ptr() as *const std::ffi::c_char
    };

    let result = unsafe { strcoll(ptr_a, ptr_b) };
    if result < 0 {
        std::cmp::Ordering::Less
    } else if result > 0 {
        std::cmp::Ordering::Greater
    } else {
        // Tie-break: byte comparison (matches PG's deterministic collation behavior)
        a.as_bytes().cmp(b.as_bytes())
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;

    fn dict_col(entries: &[&str], row_to_entry: &[u32]) -> SegTextColumn {
        SegTextColumn::Dict {
            entries: entries.iter().map(|s| s.to_string()).collect(),
            row_to_entry: row_to_entry.to_vec(),
        }
    }

    /// Build an Lz4-variant SegTextColumn from a vec of `Option<&str>`.
    /// Per-row range encoding mirrors `decompress_text_to_seg_col`'s Lz4 branch.
    fn lz4_col(values: &[Option<&str>]) -> SegTextColumn {
        let mut buf: Vec<u8> = Vec::new();
        let mut row_to_range: Vec<(u32, u16)> = Vec::with_capacity(values.len());
        for v in values {
            match v {
                Some(s) => {
                    let off = buf.len() as u32;
                    buf.extend_from_slice(s.as_bytes());
                    row_to_range.push((off, s.len() as u16));
                }
                None => row_to_range.push((u32::MAX, 0)),
            }
        }
        SegTextColumn::Lz4 { buf, row_to_range }
    }

    #[test]
    fn null_at_handles_empty_bitmap_and_bit_layout() {
        // Empty bitmap → "no nulls" everywhere.
        assert!(!null_at(&[], 0));
        assert!(!null_at(&[], 100));
        // Bit 3 of byte 0 is row 3.
        let bm: [u8; 1] = [0b0000_1000];
        assert!(!null_at(&bm, 0));
        assert!(!null_at(&bm, 2));
        assert!(null_at(&bm, 3));
        assert!(!null_at(&bm, 4));
        // Cross-byte: bit 0 of byte 1 is row 8.
        let bm: [u8; 2] = [0, 0b0000_0001];
        assert!(!null_at(&bm, 7));
        assert!(null_at(&bm, 8));
    }

    #[test]
    fn seg_text_col_get_str_handles_each_variant() {
        // Dict: index hits entries, u32::MAX means null.
        let c = dict_col(&["foo", "bar"], &[0, u32::MAX, 1]);
        assert_eq!(c.get_str(0), Some("foo"));
        assert_eq!(c.get_str(1), None);
        assert_eq!(c.get_str(2), Some("bar"));

        // Lz4: u32::MAX offset means null.
        let c = lz4_col(&[Some("hello"), None, Some("world")]);
        assert_eq!(c.get_str(0), Some("hello"));
        assert_eq!(c.get_str(1), None);
        assert_eq!(c.get_str(2), Some("world"));

        // SegBy: returns the inner option for every row.
        let c = SegTextColumn::SegBy(Some("seg".to_string()));
        assert_eq!(c.get_str(0), Some("seg"));
        let c = SegTextColumn::SegBy(None);
        assert_eq!(c.get_str(0), None);

        // Lengths: bytes aren't stored, so get_str always returns None.
        let c = SegTextColumn::Lengths {
            lengths: vec![3, 0, 5],
            null_bitmap: Vec::new(),
        };
        assert_eq!(c.get_str(0), None);
    }

    #[test]
    fn seg_text_col_get_len_uses_char_count_for_bodies() {
        // Lz4: "héllo" has 6 bytes (é = 2 bytes) but 5 characters.
        let c = lz4_col(&[Some("héllo"), None, Some("")]);
        assert_eq!(c.get_len(0), Some(5));
        assert_eq!(c.get_len(1), None);
        assert_eq!(c.get_len(2), Some(0));

        // Dict: same char-count semantics.
        let c = dict_col(&["héllo"], &[0]);
        assert_eq!(c.get_len(0), Some(5));

        // Lengths: passes the raw stored u32 through.
        let c = SegTextColumn::Lengths {
            lengths: vec![7, 0, 12],
            null_bitmap: Vec::new(),
        };
        assert_eq!(c.get_len(0), Some(7));
        assert_eq!(c.get_len(2), Some(12));
    }

    #[test]
    fn seg_text_col_dict_local_id_returns_index_or_none() {
        let c = dict_col(&["a", "b"], &[1, u32::MAX, 0]);
        assert_eq!(c.dict_local_id(0), Some(1));
        assert_eq!(c.dict_local_id(1), None);
        assert_eq!(c.dict_local_id(2), Some(0));

        // Non-Dict variants always return None — caller pairs this with
        // dict-bitset paths that only make sense for Dict storage.
        let c = lz4_col(&[Some("x")]);
        assert_eq!(c.dict_local_id(0), None);
        let c = SegTextColumn::SegBy(Some("y".into()));
        assert_eq!(c.dict_local_id(0), None);
    }

    #[test]
    fn apply_text_eq_filter_dict_initial_and_anded() {
        // Dict with rows [a, b, a, null, b]. const = "a".
        let c = dict_col(&["a", "b"], &[0, 1, 0, u32::MAX, 1]);

        // Initial: sel.is_empty() → builds [true, false, true, false, false].
        let mut sel: Vec<bool> = Vec::new();
        apply_text_eq_filter(&c, "a", /*is_ne*/ false, 5, &mut sel);
        assert_eq!(sel, vec![true, false, true, false, false]);

        // ANDed: pre-existing false rows stay false; matching rows confirm true.
        let mut sel = vec![false, true, true, true, true];
        apply_text_eq_filter(&c, "a", false, 5, &mut sel);
        assert_eq!(sel, vec![false, false, true, false, false]);

        // is_ne flips the predicate but still drops NULLs.
        let mut sel: Vec<bool> = Vec::new();
        apply_text_eq_filter(&c, "a", /*is_ne*/ true, 5, &mut sel);
        assert_eq!(sel, vec![false, true, false, false, true]);
    }

    #[test]
    fn apply_text_eq_filter_lz4_fallback() {
        let c = lz4_col(&[Some("foo"), None, Some("bar"), Some("foo")]);
        let mut sel: Vec<bool> = Vec::new();
        apply_text_eq_filter(&c, "foo", false, 4, &mut sel);
        assert_eq!(sel, vec![true, false, false, true]);
    }

    #[test]
    fn apply_text_eq_filter_lengths_only_resolves_empty_string() {
        // Lengths with one empty + one null + one non-empty. Comparing
        // against `""` works (resolvable from length alone).
        let c = SegTextColumn::Lengths {
            lengths: vec![0, 0, 3],
            null_bitmap: vec![0b0000_0010], // row 1 = null
        };
        let mut sel: Vec<bool> = Vec::new();
        apply_text_eq_filter(&c, "", false, 3, &mut sel);
        // Row 0 empty → true; row 1 null → false; row 2 length 3 → false.
        assert_eq!(sel, vec![true, false, false]);

        // Non-empty constant on a Lengths column zeroes the selection
        // (planner should never route this, fail-safe drops everything).
        let mut sel = vec![true, true, true];
        apply_text_eq_filter(&c, "anything", false, 3, &mut sel);
        assert_eq!(sel, vec![false, false, false]);
    }

    #[test]
    fn apply_text_in_filter_dict_and_lz4() {
        // Dict: rows reference entries by index; matches built once.
        let c = dict_col(&["a", "b", "c"], &[0, 1, 2, u32::MAX, 1]);
        let mut sel: Vec<bool> = Vec::new();
        apply_text_in_filter(&c, &["a".into(), "c".into()], 5, &mut sel);
        assert_eq!(sel, vec![true, false, true, false, false]);

        // Lz4 fallback: per-row lookup.
        let c = lz4_col(&[Some("a"), Some("b"), Some("c"), None]);
        let mut sel: Vec<bool> = Vec::new();
        apply_text_in_filter(&c, &["b".into()], 4, &mut sel);
        assert_eq!(sel, vec![false, true, false, false]);
    }

    #[test]
    fn apply_text_like_filter_dict_with_contains_strategy() {
        let c = dict_col(&["alpha", "beta", "gamma"], &[0, 1, 2, 0]);
        let mut sel: Vec<bool> = Vec::new();
        apply_text_like_filter(
            &c,
            &LikeStrategy::Contains("a".into()),
            /*negate*/ false,
            4,
            &mut sel,
        );
        // alpha/beta/gamma all contain 'a' — every non-null row passes.
        assert_eq!(sel, vec![true, true, true, true]);

        // Negated → invert.
        let mut sel: Vec<bool> = Vec::new();
        apply_text_like_filter(&c, &LikeStrategy::Contains("a".into()), true, 4, &mut sel);
        assert_eq!(sel, vec![false, false, false, false]);

        // Exact 'beta' only matches row 1.
        let mut sel: Vec<bool> = Vec::new();
        apply_text_like_filter(&c, &LikeStrategy::Exact("beta".into()), false, 4, &mut sel);
        assert_eq!(sel, vec![false, true, false, false]);
    }

    #[test]
    fn strcoll_cmp_bytewise_fast_path() {
        // Equal bytes always tie immediately, regardless of locale.
        assert_eq!(strcoll_cmp("foo", "foo"), std::cmp::Ordering::Equal);
        assert_eq!(strcoll_cmp("", ""), std::cmp::Ordering::Equal);

        // Different short strings: pick something where strcoll and byte
        // order agree under any locale (lowercase ASCII only).
        assert_eq!(strcoll_cmp("apple", "banana"), std::cmp::Ordering::Less);
        assert_eq!(strcoll_cmp("banana", "apple"), std::cmp::Ordering::Greater);
    }

    #[test]
    fn strcoll_cmp_handles_long_strings() {
        // STACK_BUF = 512; cross the boundary to exercise the heap path.
        let a: String = "a".repeat(600);
        let b: String = "b".repeat(600);
        assert_eq!(strcoll_cmp(&a, &b), std::cmp::Ordering::Less);
        assert_eq!(strcoll_cmp(&b, &a), std::cmp::Ordering::Greater);
        assert_eq!(strcoll_cmp(&a, &a), std::cmp::Ordering::Equal);
    }
}
