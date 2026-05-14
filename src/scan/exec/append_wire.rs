//! DSM wire format for shared segment metadata (§5.7 in
//! `dev/docs/RTABENCH_QUERY_ANALYSIS.md`).
//!
//! Each parallel-worker `DeltaXAppend` scan used to re-run `load_metadata`
//! (SPI) + `load_segments_heap` (heap scan of `_meta` + `_colstats` + blooms)
//! independently, costing ~2 s × N processes for selective queries. After
//! this change the leader runs the heap scan once, serialises the resulting
//! `Vec<SegmentData>` into DSM, and workers read it zero-copy instead of
//! running the scan.
//!
//! Blobs are **not** carried through DSM — each worker fetches only the blobs
//! for segments it actually claims via `fetch_segment_blobs`. That keeps DSM
//! size bounded (tens of MB rather than GB) and parallelises TOAST I/O.
//!
//! Layout (one contiguous DSM buffer, offset-addressed):
//!
//! ```text
//! ┌──────────────────────────────────────────┐ 0
//! │ DeltaXAppendPState  (existing POD)       │  cursor + per-slot timings
//! ├──────────────────────────────────────────┤ sizeof(PState)
//! │ DeltaXAppendMeta    (fixed header)       │  magic, version, offsets,
//! │                                          │  num_columns / num_segments
//! ├──────────────────────────────────────────┤
//! │ ColInfo × num_columns                    │  typoid, typmod, name (off,len)
//! │ segment_by_indices : u32[num_seg_by]     │
//! │ order_by_indices   : u32[num_order_by]   │
//! │ companion_oids     : u32[num_companions] │
//! │ header_arena (col names + time column)   │
//! ├──────────────────────────────────────────┤ segments_offset
//! │ SegmentEntry × num_segments (stride 64)  │  companion_oid, segment_id,
//! │                                          │  row_count, min/max_time,
//! │                                          │  segment_values (off,len)
//! ├──────────────────────────────────────────┤ seg_arena_offset
//! │ segment_values arena                     │  per-segment Vec<Option<String>>
//! └──────────────────────────────────────────┘
//! ```
//!
//! Forward compatibility for a future parallel `DeltaXAgg` retry: the
//! `SegmentEntry` has reserved `(off,len)` slots for `col_minmax` and
//! `col_sums` arena blocks (empty in V1). When DeltaXAgg needs them, flip the
//! `has_col_minmax` / `has_col_sums` bits in `flags` and serialise into the
//! reserved region. `WIRE_VERSION` stays 1 because the layout is strictly
//! additive (new bytes appended past the V1 last-used offset).

use pgrx::pg_sys;

use super::segments::SegmentData;

/// Magic number at the start of the metadata region: ASCII "DXAW".
pub(crate) const WIRE_MAGIC: u32 = 0x44_58_41_57;
/// V1 layout: no `col_minmax` / `col_sums` / `text_length_blobs` payload.
pub(crate) const WIRE_VERSION: u32 = 1;

/// Fixed-size header written at the start of the metadata region (immediately
/// after the existing `DeltaXAppendPState`).
#[repr(C)]
#[derive(Debug, Default)]
pub(crate) struct DeltaXAppendMeta {
    pub(crate) magic: u32,
    pub(crate) version: u32,
    pub(crate) num_columns: u32,
    pub(crate) num_seg_by: u32,
    pub(crate) num_order_by: u32,
    pub(crate) num_companions: u32,
    pub(crate) num_segments: u32,
    /// Index into `ColInfo[]` for the time column (i.e. the column named
    /// `meta.time_column` in the original `MetadataInfo`). `u32::MAX` means
    /// "no time column" (shouldn't happen for DeltaXAppend, kept for safety).
    pub(crate) time_col_idx: u32,
    /// Byte offset from the start of the metadata region to the `ColInfo[]`
    /// array.
    pub(crate) col_info_off: u32,
    pub(crate) seg_by_indices_off: u32,
    pub(crate) order_by_indices_off: u32,
    pub(crate) companion_oids_off: u32,
    pub(crate) header_arena_off: u32,
    pub(crate) header_arena_len: u32,
    pub(crate) segments_off: u32,
    pub(crate) seg_arena_off: u32,
    pub(crate) seg_arena_len: u32,
    /// Bit 0: has_col_minmax  (unset in V1)
    /// Bit 1: has_col_sums    (unset in V1)
    /// Bits 2..: reserved, must be zero.
    pub(crate) flags: u32,
    /// Total size of the metadata region in bytes (from the start of
    /// `DeltaXAppendMeta`, not including the preceding `DeltaXAppendPState`).
    /// Used as a sanity check on the worker side.
    pub(crate) total_size: u32,
}

/// Per-column info. Type OID + typmod + name (offset/length into header arena).
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ColInfo {
    pub(crate) typoid: u32,
    pub(crate) typmod: i32,
    pub(crate) name_off: u32,
    pub(crate) name_len: u32,
}

/// Per-segment record. Fixed 64-byte stride → the shared cursor indexes
/// directly: `segments_ptr.add(idx)`.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SegmentEntry {
    pub(crate) companion_oid: u32,
    pub(crate) segment_id: i32,
    pub(crate) row_count: i32,
    /// 0 / 1 — distinguishes `None` from a real zero `min_time`.
    pub(crate) has_min_time: u8,
    pub(crate) has_max_time: u8,
    _pad0: [u8; 2],
    pub(crate) min_time: i64,
    pub(crate) max_time: i64,
    // Variable-length payloads stored in the segment arena.
    // `segment_values` is `Vec<Option<String>>` encoded as:
    //     [u16 n, for i in 0..n: u8 is_null, u32 len, bytes ...]
    pub(crate) segment_values_off: u32,
    pub(crate) segment_values_len: u32,
    // Reserved for DeltaXAgg: per-column min/max.
    pub(crate) col_minmax_off: u32,
    pub(crate) col_minmax_len: u32,
    // Reserved for DeltaXAgg: per-column sum / nonnull_count / nonzero_count.
    pub(crate) col_sums_off: u32,
    pub(crate) col_sums_len: u32,
    _pad1: [u8; 8],
}

// Compile-time ABI checks. If either assertion fires the wire format drifted;
// fix the layout or bump `WIRE_VERSION`.
const _: () = assert!(std::mem::size_of::<SegmentEntry>() == 64);
const _: () = assert!(std::mem::align_of::<SegmentEntry>() == 8);
const _: () = assert!(std::mem::align_of::<DeltaXAppendMeta>() == 4);
const _: () = assert!(std::mem::align_of::<ColInfo>() == 4);

/// Byte offsets + region sizes for the metadata wire. `layout()` computes
/// these from a leader-side `DecompressState` snapshot (`meta` +
/// `segments_data`); `serialize_into()` consumes the same struct and writes
/// fields at those offsets, so estimator and serialiser can't drift.
#[derive(Debug, Default, Clone)]
pub(crate) struct WireLayout {
    // Offsets are relative to the start of the metadata region (i.e. to
    // `DeltaXAppendMeta`). Add `std::mem::size_of::<DeltaXAppendPState>()` to
    // get DSM-buffer-absolute offsets.
    #[allow(dead_code)] // retained for layout debugging / assertions
    pub(crate) header_size: u32,
    pub(crate) col_info_off: u32,
    pub(crate) seg_by_indices_off: u32,
    pub(crate) order_by_indices_off: u32,
    pub(crate) companion_oids_off: u32,
    pub(crate) header_arena_off: u32,
    pub(crate) header_arena_len: u32,
    pub(crate) segments_off: u32,
    pub(crate) seg_arena_off: u32,
    pub(crate) seg_arena_len: u32,
    pub(crate) total_size: u32,
}

/// Minimal borrowed snapshot used by `layout` and `serialize_into` — avoids
/// tying the wire module to `DecompressState`.
pub(crate) struct WireInput<'a> {
    pub(crate) col_names: &'a [String],
    pub(crate) col_types: &'a [pg_sys::Oid],
    pub(crate) col_typmods: &'a [i32],
    pub(crate) segment_by: &'a [String],
    pub(crate) order_by: &'a [String],
    pub(crate) time_column: &'a str,
    pub(crate) companion_oids: &'a [pg_sys::Oid],
    pub(crate) segments: &'a [SegmentData],
}

fn round_up(n: u32, align: u32) -> u32 {
    (n + align - 1) & !(align - 1)
}

fn encode_segment_values_len(values: &[Option<String>]) -> u32 {
    // [u16 count] + [u8 is_null + u32 len + bytes] × count
    let mut total: u32 = 2;
    for v in values {
        total += 1 + 4;
        if let Some(s) = v {
            total += s.len() as u32;
        }
    }
    total
}

/// Compute the layout of the metadata region in bytes. Caller allocates DSM
/// = `sizeof(DeltaXAppendPState) + layout.total_size`.
pub(crate) fn layout(input: &WireInput<'_>) -> WireLayout {
    let header_size = std::mem::size_of::<DeltaXAppendMeta>() as u32;
    let col_info_off = header_size;
    let col_info_size = (input.col_names.len() * std::mem::size_of::<ColInfo>()) as u32;

    let seg_by_indices_off = col_info_off + col_info_size;
    let seg_by_indices_size = (input.segment_by.len() * 4) as u32;

    let order_by_indices_off = seg_by_indices_off + seg_by_indices_size;
    let order_by_indices_size = (input.order_by.len() * 4) as u32;

    let companion_oids_off = order_by_indices_off + order_by_indices_size;
    let companion_oids_size = (input.companion_oids.len() * 4) as u32;

    // Header arena = all column name bytes + time column name bytes (used by
    // `ColInfo.name_off/len`).
    let header_arena_off = companion_oids_off + companion_oids_size;
    let header_arena_len: u32 = input.col_names.iter().map(|s| s.len() as u32).sum();

    // 8-byte align before SegmentEntry array (SegmentEntry needs 8-byte align).
    let segments_off = round_up(header_arena_off + header_arena_len, 8);
    let segments_size = (input.segments.len() * std::mem::size_of::<SegmentEntry>()) as u32;

    let seg_arena_off = segments_off + segments_size;
    let mut seg_arena_len: u32 = 0;
    for seg in input.segments {
        seg_arena_len += encode_segment_values_len(&seg.segment_values);
    }

    let total_size = seg_arena_off + seg_arena_len;

    WireLayout {
        header_size,
        col_info_off,
        seg_by_indices_off,
        order_by_indices_off,
        companion_oids_off,
        header_arena_off,
        header_arena_len,
        segments_off,
        seg_arena_off,
        seg_arena_len,
        total_size,
    }
}

/// Serialise the leader's metadata + segment array into the DSM region
/// starting at `base`. `base` must point into a buffer at least
/// `layout.total_size` bytes.
///
/// Safety: caller owns the buffer for at least the DSM attach window; bytes
/// are written via raw pointers, no rust-side Drop is run on the buffer.
pub(crate) unsafe fn serialize_into(base: *mut u8, input: &WireInput<'_>, layout: &WireLayout) {
    unsafe {
        // Zero the full region so reserved fields and padding are well-defined.
        std::ptr::write_bytes(base, 0, layout.total_size as usize);

        // ---------------- Header ----------------
        let hdr = base as *mut DeltaXAppendMeta;
        (*hdr).magic = WIRE_MAGIC;
        (*hdr).version = WIRE_VERSION;
        (*hdr).num_columns = input.col_names.len() as u32;
        (*hdr).num_seg_by = input.segment_by.len() as u32;
        (*hdr).num_order_by = input.order_by.len() as u32;
        (*hdr).num_companions = input.companion_oids.len() as u32;
        (*hdr).num_segments = input.segments.len() as u32;
        (*hdr).time_col_idx = input.col_names.iter()
            .position(|c| c == input.time_column)
            .map(|i| i as u32)
            .unwrap_or(u32::MAX);
        (*hdr).col_info_off = layout.col_info_off;
        (*hdr).seg_by_indices_off = layout.seg_by_indices_off;
        (*hdr).order_by_indices_off = layout.order_by_indices_off;
        (*hdr).companion_oids_off = layout.companion_oids_off;
        (*hdr).header_arena_off = layout.header_arena_off;
        (*hdr).header_arena_len = layout.header_arena_len;
        (*hdr).segments_off = layout.segments_off;
        (*hdr).seg_arena_off = layout.seg_arena_off;
        (*hdr).seg_arena_len = layout.seg_arena_len;
        (*hdr).flags = 0;
        (*hdr).total_size = layout.total_size;

        // ---------------- ColInfo[] + header arena (names) ----------------
        let col_info_ptr = base.add(layout.col_info_off as usize) as *mut ColInfo;
        let header_arena_ptr = base.add(layout.header_arena_off as usize);
        let mut arena_off: u32 = 0;
        for (i, name) in input.col_names.iter().enumerate() {
            let bytes = name.as_bytes();
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                header_arena_ptr.add(arena_off as usize),
                bytes.len(),
            );
            *col_info_ptr.add(i) = ColInfo {
                typoid: u32::from(input.col_types[i]),
                typmod: input.col_typmods[i],
                name_off: layout.header_arena_off + arena_off,
                name_len: bytes.len() as u32,
            };
            arena_off += bytes.len() as u32;
        }
        debug_assert_eq!(arena_off, layout.header_arena_len);

        // ---------------- segment_by_indices ----------------
        let seg_by_ptr = base.add(layout.seg_by_indices_off as usize) as *mut u32;
        for (k, name) in input.segment_by.iter().enumerate() {
            let idx = input.col_names.iter().position(|c| c == name).unwrap_or(0) as u32;
            *seg_by_ptr.add(k) = idx;
        }

        // ---------------- order_by_indices ----------------
        let order_by_ptr = base.add(layout.order_by_indices_off as usize) as *mut u32;
        for (k, name) in input.order_by.iter().enumerate() {
            let idx = input.col_names.iter().position(|c| c == name).unwrap_or(0) as u32;
            *order_by_ptr.add(k) = idx;
        }

        // ---------------- companion_oids ----------------
        let comp_oids_ptr = base.add(layout.companion_oids_off as usize) as *mut u32;
        for (k, oid) in input.companion_oids.iter().enumerate() {
            *comp_oids_ptr.add(k) = u32::from(*oid);
        }

        // ---------------- SegmentEntry[] + seg arena ----------------
        let segs_ptr = base.add(layout.segments_off as usize) as *mut SegmentEntry;
        let seg_arena_ptr = base.add(layout.seg_arena_off as usize);
        let mut seg_arena_cursor: u32 = 0;
        for (i, seg) in input.segments.iter().enumerate() {
            let sv_off = layout.seg_arena_off + seg_arena_cursor;
            let sv_len = encode_segment_values_len(&seg.segment_values);

            // Encode segment_values at arena cursor.
            let mut wp = seg_arena_ptr.add(seg_arena_cursor as usize);
            let count = seg.segment_values.len() as u16;
            std::ptr::copy_nonoverlapping(&count as *const u16 as *const u8, wp, 2);
            wp = wp.add(2);
            for opt in &seg.segment_values {
                match opt {
                    Some(s) => {
                        *wp = 0; // is_null = 0
                        wp = wp.add(1);
                        let len = s.len() as u32;
                        std::ptr::copy_nonoverlapping(&len as *const u32 as *const u8, wp, 4);
                        wp = wp.add(4);
                        std::ptr::copy_nonoverlapping(s.as_bytes().as_ptr(), wp, s.len());
                        wp = wp.add(s.len());
                    }
                    None => {
                        *wp = 1; // is_null = 1
                        wp = wp.add(1);
                        let len: u32 = 0;
                        std::ptr::copy_nonoverlapping(&len as *const u32 as *const u8, wp, 4);
                        wp = wp.add(4);
                    }
                }
            }
            debug_assert_eq!(
                wp.offset_from(seg_arena_ptr.add(seg_arena_cursor as usize)) as u32,
                sv_len,
            );

            *segs_ptr.add(i) = SegmentEntry {
                companion_oid: u32::from(seg.companion_oid),
                segment_id: seg.segment_id,
                row_count: seg.row_count,
                has_min_time: seg.min_time.is_some() as u8,
                has_max_time: seg.max_time.is_some() as u8,
                min_time: seg.min_time.unwrap_or(0),
                max_time: seg.max_time.unwrap_or(0),
                segment_values_off: sv_off,
                segment_values_len: sv_len,
                ..SegmentEntry::default()
            };

            seg_arena_cursor += sv_len;
        }
        debug_assert_eq!(seg_arena_cursor, layout.seg_arena_len);
    }
}

/// Borrowed zero-copy view over a serialised metadata region. Lifetime is
/// tied to the DSM-attach window on the worker side; drop the view before
/// `ShutdownCustomScan` returns.
pub(crate) struct DeltaXAppendView {
    base: *const u8,
}

/// Decoded view of one `SegmentEntry` — allocates `segment_values` lazily when
/// the worker actually picks that segment.
pub(crate) struct SegmentBorrow {
    pub(crate) companion_oid: pg_sys::Oid,
    pub(crate) segment_id: i32,
    pub(crate) row_count: i32,
    pub(crate) min_time: Option<i64>,
    pub(crate) max_time: Option<i64>,
    pub(crate) segment_values: Vec<Option<String>>,
}

/// Decoded view of the wire header — cheap to construct on worker attach.
pub(crate) struct HeaderBorrow {
    pub(crate) col_names: Vec<String>,
    pub(crate) col_types: Vec<pg_sys::Oid>,
    pub(crate) col_typmods: Vec<i32>,
    pub(crate) segment_by: Vec<String>,
    /// Leader's `order_by`. DeltaXAppend's worker path doesn't read it because
    /// ordering is derived from `min_time` sort order; kept for DeltaXAgg.
    #[allow(dead_code)]
    pub(crate) order_by: Vec<String>,
    pub(crate) time_column: String,
    /// Leader's companion OIDs. Reconstructed from `SegmentEntry.companion_oid`
    /// on the worker path; kept here for future callers that need the dense list.
    #[allow(dead_code)]
    pub(crate) companion_oids: Vec<pg_sys::Oid>,
    pub(crate) num_segments: usize,
}

impl DeltaXAppendView {
    /// Attach to a serialised metadata region. Validates magic + version.
    ///
    /// Safety: `base` must point to a region written by `serialize_into` that
    /// stays valid for the lifetime of the view.
    pub(crate) unsafe fn attach(base: *const u8) -> Option<Self> {
        unsafe {
            let hdr = base as *const DeltaXAppendMeta;
            if (*hdr).magic != WIRE_MAGIC || (*hdr).version != WIRE_VERSION {
                return None;
            }
            Some(Self { base })
        }
    }

    unsafe fn header(&self) -> &DeltaXAppendMeta {
        unsafe { &*(self.base as *const DeltaXAppendMeta) }
    }

    #[allow(dead_code)] // exercised by tests; callers go through decode_header().num_segments
    pub(crate) fn num_segments(&self) -> usize {
        unsafe { self.header().num_segments as usize }
    }

    /// Decode the wire header into owned Rust types. Called once per worker
    /// during `InitializeWorkerCustomScan`.
    pub(crate) unsafe fn decode_header(&self) -> HeaderBorrow {
        unsafe {
            let hdr = self.header();
            let col_info_ptr = self.base.add(hdr.col_info_off as usize) as *const ColInfo;
            let n = hdr.num_columns as usize;
            let mut col_names = Vec::with_capacity(n);
            let mut col_types = Vec::with_capacity(n);
            let mut col_typmods = Vec::with_capacity(n);
            for i in 0..n {
                let ci = &*col_info_ptr.add(i);
                let name_bytes = std::slice::from_raw_parts(
                    self.base.add(ci.name_off as usize),
                    ci.name_len as usize,
                );
                col_names.push(std::str::from_utf8_unchecked(name_bytes).to_owned());
                col_types.push(pg_sys::Oid::from(ci.typoid));
                col_typmods.push(ci.typmod);
            }

            let seg_by_ptr = self.base.add(hdr.seg_by_indices_off as usize) as *const u32;
            let segment_by: Vec<String> = (0..hdr.num_seg_by as usize)
                .map(|k| col_names[*seg_by_ptr.add(k) as usize].clone())
                .collect();

            let order_by_ptr = self.base.add(hdr.order_by_indices_off as usize) as *const u32;
            let order_by: Vec<String> = (0..hdr.num_order_by as usize)
                .map(|k| col_names[*order_by_ptr.add(k) as usize].clone())
                .collect();

            let time_column = if hdr.time_col_idx == u32::MAX {
                String::new()
            } else {
                col_names[hdr.time_col_idx as usize].clone()
            };

            let comp_ptr = self.base.add(hdr.companion_oids_off as usize) as *const u32;
            let companion_oids: Vec<pg_sys::Oid> = (0..hdr.num_companions as usize)
                .map(|k| pg_sys::Oid::from(*comp_ptr.add(k)))
                .collect();

            HeaderBorrow {
                col_names,
                col_types,
                col_typmods,
                segment_by,
                order_by,
                time_column,
                companion_oids,
                num_segments: hdr.num_segments as usize,
            }
        }
    }

    /// Decode one `SegmentEntry` into an owned `SegmentBorrow`. O(seg_values
    /// size); called on-claim after the shared cursor hands a worker an
    /// index.
    pub(crate) unsafe fn decode_segment(&self, idx: usize) -> SegmentBorrow {
        unsafe {
            let hdr = self.header();
            debug_assert!(idx < hdr.num_segments as usize);
            let segs_ptr = self.base.add(hdr.segments_off as usize) as *const SegmentEntry;
            let se = &*segs_ptr.add(idx);

            let mut segment_values = Vec::new();
            if se.segment_values_len > 0 {
                let mut rp = self.base.add(se.segment_values_off as usize);
                let mut count_bytes = [0u8; 2];
                std::ptr::copy_nonoverlapping(rp, count_bytes.as_mut_ptr(), 2);
                let count = u16::from_ne_bytes(count_bytes);
                rp = rp.add(2);
                segment_values.reserve(count as usize);
                for _ in 0..count {
                    let is_null = *rp;
                    rp = rp.add(1);
                    let mut len_bytes = [0u8; 4];
                    std::ptr::copy_nonoverlapping(rp, len_bytes.as_mut_ptr(), 4);
                    let len = u32::from_ne_bytes(len_bytes);
                    rp = rp.add(4);
                    if is_null != 0 {
                        segment_values.push(None);
                    } else {
                        let bytes = std::slice::from_raw_parts(rp, len as usize);
                        segment_values.push(Some(
                            std::str::from_utf8_unchecked(bytes).to_owned(),
                        ));
                        rp = rp.add(len as usize);
                    }
                }
            }

            SegmentBorrow {
                companion_oid: pg_sys::Oid::from(se.companion_oid),
                segment_id: se.segment_id,
                row_count: se.row_count,
                min_time: if se.has_min_time != 0 { Some(se.min_time) } else { None },
                max_time: if se.has_max_time != 0 { Some(se.max_time) } else { None },
                segment_values,
            }
        }
    }
}

// SAFETY: DeltaXAppendView wraps a raw pointer into DSM memory that is shared
// between the leader and workers for the duration of the parallel scan. Access
// is always through unsafe methods that enforce the attach-window invariant.
unsafe impl Send for DeltaXAppendView {}
unsafe impl Sync for DeltaXAppendView {}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn mk_segment(seg_id: i32, oid: u32, vals: Vec<Option<&str>>, min_t: Option<i64>, max_t: Option<i64>) -> SegmentData {
        SegmentData {
            companion_oid: pg_sys::Oid::from(oid),
            segment_id: seg_id,
            segment_values: vals.into_iter().map(|o| o.map(String::from)).collect(),
            compressed_blobs: Vec::new(),
            text_length_blobs: Vec::new(),
            row_count: 1000,
            min_time: min_t,
            max_time: max_t,
            col_minmax: HashMap::new(),
            col_sums: HashMap::new(),
            toast_pointers: Vec::new(),
            cached_blob_pins: Vec::new(),
        }
    }

    #[pgrx::pg_test]
    fn pg_test_wire_layout_empty_segments() {
        let col_names = vec!["ts".to_string(), "device".to_string(), "value".to_string()];
        let col_types = vec![pg_sys::TIMESTAMPTZOID, pg_sys::INT4OID, pg_sys::FLOAT8OID];
        let col_typmods = vec![-1i32, -1, -1];
        let segment_by = vec!["device".to_string()];
        let order_by = vec!["ts".to_string()];
        let companion_oids = vec![pg_sys::Oid::from(12345u32)];
        let segments: Vec<SegmentData> = Vec::new();
        let input = WireInput {
            col_names: &col_names,
            col_types: &col_types,
            col_typmods: &col_typmods,
            segment_by: &segment_by,
            order_by: &order_by,
            time_column: "ts",
            companion_oids: &companion_oids,
            segments: &segments,
        };
        let layout = layout(&input);
        let mut buf = vec![0u8; layout.total_size as usize];
        unsafe {
            serialize_into(buf.as_mut_ptr(), &input, &layout);
            let view = DeltaXAppendView::attach(buf.as_ptr()).unwrap();
            assert_eq!(view.num_segments(), 0);
            let hdr = view.decode_header();
            assert_eq!(hdr.col_names, col_names);
            assert_eq!(hdr.col_types, col_types);
            assert_eq!(hdr.col_typmods, col_typmods);
            assert_eq!(hdr.segment_by, segment_by);
            assert_eq!(hdr.order_by, order_by);
            assert_eq!(hdr.time_column, "ts");
            assert_eq!(hdr.companion_oids, companion_oids);
            assert_eq!(hdr.num_segments, 0);
        }
    }

    #[pgrx::pg_test]
    fn pg_test_wire_roundtrip() {
        let col_names = vec!["ts".to_string(), "device".to_string(), "value".to_string()];
        let col_types = vec![pg_sys::TIMESTAMPTZOID, pg_sys::INT4OID, pg_sys::FLOAT8OID];
        let col_typmods = vec![-1i32, -1, -1];
        let segment_by = vec!["device".to_string()];
        let order_by = vec!["ts".to_string()];
        let companion_oids = vec![pg_sys::Oid::from(12345u32), pg_sys::Oid::from(67890u32)];
        let segments = vec![
            mk_segment(0, 12345, vec![Some("42")], Some(1_000_000_000), Some(1_000_000_100)),
            mk_segment(1, 12345, vec![None], None, None),
            mk_segment(2, 67890, vec![Some("hello world")], Some(2_000_000_000), Some(2_000_000_500)),
        ];
        let input = WireInput {
            col_names: &col_names,
            col_types: &col_types,
            col_typmods: &col_typmods,
            segment_by: &segment_by,
            order_by: &order_by,
            time_column: "ts",
            companion_oids: &companion_oids,
            segments: &segments,
        };
        let layout = layout(&input);
        let mut buf = vec![0u8; layout.total_size as usize];
        unsafe {
            serialize_into(buf.as_mut_ptr(), &input, &layout);
            let view = DeltaXAppendView::attach(buf.as_ptr()).unwrap();
            assert_eq!(view.num_segments(), 3);
            for (i, seg) in segments.iter().enumerate() {
                let borrow = view.decode_segment(i);
                assert_eq!(borrow.companion_oid, seg.companion_oid);
                assert_eq!(borrow.segment_id, seg.segment_id);
                assert_eq!(borrow.row_count, seg.row_count);
                assert_eq!(borrow.min_time, seg.min_time);
                assert_eq!(borrow.max_time, seg.max_time);
                assert_eq!(borrow.segment_values, seg.segment_values);
            }
        }
    }

    #[pgrx::pg_test]
    fn pg_test_wire_magic_mismatch_rejected() {
        let buf = vec![0u8; 256];
        unsafe {
            // All-zero bytes → magic 0 → attach returns None.
            assert!(DeltaXAppendView::attach(buf.as_ptr()).is_none());
        }
    }
}
