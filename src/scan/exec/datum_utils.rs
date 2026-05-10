use pgrx::pg_sys;

use crate::compression::{self, CompressionType, CompressedColumnRef};
use super::{PG_EPOCH_OFFSET_USEC, PG_EPOCH_OFFSET_DAYS};
use super::batch_qual::{BatchQual, BatchCompareOp, LikeStrategy, sql_like_match};

// ============================================================================
// Inline PG executor helpers (these are static inline in C headers,
// so they are not available via FFI — we re-implement them here).
// ============================================================================

pub(super) const TTS_FLAG_EMPTY: u16 = 1 << 1;

/// Re-implementation of PostgreSQL's static inline `ExecProject`.
pub(super) unsafe fn exec_project(proj_info: *mut pg_sys::ProjectionInfo) -> *mut pg_sys::TupleTableSlot {
    unsafe {
        let econtext = (*proj_info).pi_exprContext;
        let state = &mut (*proj_info).pi_state;
        let slot = state.resultslot;

        pg_sys::ExecClearTuple(slot);

        // ExecEvalExprSwitchContext
        let old_ctx = pg_sys::MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);
        let mut isnull = false;
        if let Some(evalfunc) = state.evalfunc {
            evalfunc(state, econtext, &mut isnull);
        }
        pg_sys::MemoryContextSwitchTo(old_ctx);

        // Mark slot as containing a valid virtual tuple (inlined ExecStoreVirtualTuple)
        (*slot).tts_flags &= !TTS_FLAG_EMPTY;
        (*slot).tts_nvalid = (*(*slot).tts_tupleDescriptor).natts as i16;

        slot
    }
}

/// Re-implementation of PostgreSQL's static inline `ExecQual`.
pub(super) unsafe fn exec_qual(state: *mut pg_sys::ExprState, econtext: *mut pg_sys::ExprContext) -> bool {
    unsafe {
        if state.is_null() {
            return true;
        }

        // ExecEvalExprSwitchContext
        let old_ctx = pg_sys::MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);
        let mut isnull = false;
        let ret = if let Some(evalfunc) = (*state).evalfunc {
            evalfunc(state, econtext, &mut isnull)
        } else {
            pg_sys::Datum::from(0)
        };
        pg_sys::MemoryContextSwitchTo(old_ctx);

        ret != pg_sys::Datum::from(0)
    }
}

// ============================================================================
// TupleDesc attribute access (PG14–17 vs PG18)
// ============================================================================

/// Get a pointer to the i-th `FormData_pg_attribute` from a TupleDesc.
/// PG14–17 store attrs directly; PG18 stores CompactAttribute first, then attrs.
#[cfg(any(
    feature = "pg14",
    feature = "pg15",
    feature = "pg16",
    feature = "pg17"
))]
#[inline]
pub(in crate::scan) unsafe fn tupdesc_get_attr(
    tupdesc: pg_sys::TupleDesc,
    i: usize,
) -> *const pg_sys::FormData_pg_attribute {
    unsafe { (*tupdesc).attrs.as_ptr().add(i) }
}

#[cfg(feature = "pg18")]
#[inline]
pub(in crate::scan) unsafe fn tupdesc_get_attr(
    tupdesc: pg_sys::TupleDesc,
    i: usize,
) -> *const pg_sys::FormData_pg_attribute {
    unsafe {
        let natts = (*tupdesc).natts as usize;
        let att_pointer = (*tupdesc)
            .compact_attrs
            .as_ptr()
            .add(natts)
            .cast::<pg_sys::FormData_pg_attribute>();
        att_pointer.add(i)
    }
}

/// Convert a string to a PostgreSQL Datum using the type's input function.
/// Used only for segment_by values (one per segment, not per row).
pub(super) fn string_to_datum(s: &str, type_oid: pg_sys::Oid) -> pg_sys::Datum {
    unsafe {
        let cstr = std::ffi::CString::new(s).unwrap();
        let mut typinput: pg_sys::Oid = pg_sys::InvalidOid;
        let mut typioparam: pg_sys::Oid = pg_sys::InvalidOid;
        pg_sys::getTypeInputInfo(type_oid, &mut typinput, &mut typioparam);
        pg_sys::OidInputFunctionCall(typinput, cstr.as_ptr() as *mut _, typioparam, -1)
    }
}

/// Map a PG type name (udt_name) to a type OID.
pub(super) fn pg_type_oid(type_name: &str) -> pg_sys::Oid {
    match type_name {
        "timestamptz" => pg_sys::TIMESTAMPTZOID,
        "timestamp" => pg_sys::TIMESTAMPOID,
        "float8" => pg_sys::FLOAT8OID,
        "float4" => pg_sys::FLOAT4OID,
        "int2" => pg_sys::INT2OID,
        "int4" => pg_sys::INT4OID,
        "int8" => pg_sys::INT8OID,
        "date" => pg_sys::DATEOID,
        "bpchar" => pg_sys::BPCHAROID,
        "bool" => pg_sys::BOOLOID,
        "text" => pg_sys::TEXTOID,
        "varchar" => pg_sys::VARCHAROID,
        "jsonb" => pg_sys::JSONBOID,
        _ => pg_sys::TEXTOID,
    }
}

/// Map a type OID back to a data_type string for codec dispatch.
pub(super) fn pg_type_name(type_oid: pg_sys::Oid) -> String {
    if type_oid == pg_sys::TIMESTAMPTZOID || type_oid == pg_sys::TIMESTAMPOID {
        "timestamp with time zone".to_string()
    } else if type_oid == pg_sys::FLOAT8OID {
        "double precision".to_string()
    } else if type_oid == pg_sys::FLOAT4OID {
        "real".to_string()
    } else if type_oid == pg_sys::INT2OID {
        "smallint".to_string()
    } else if type_oid == pg_sys::INT4OID {
        "integer".to_string()
    } else if type_oid == pg_sys::INT8OID {
        "bigint".to_string()
    } else if type_oid == pg_sys::DATEOID {
        "date".to_string()
    } else if type_oid == pg_sys::BOOLOID {
        "boolean".to_string()
    } else if type_oid == pg_sys::JSONBOID {
        "jsonb".to_string()
    } else {
        "text".to_string()
    }
}

// ============================================================================
// Direct datum decompression — bypasses the string round-trip
// ============================================================================

/// Decode compressed data to raw Datums (without null reinsertion).
///
/// This is the shared codec dispatch used by both `decompress_blob_to_datums`
/// and `decompress_blob_to_datums_truncated`.
unsafe fn decode_compressed_datums(
    cc: &CompressedColumnRef,
    dt: &str,
    type_oid: pg_sys::Oid,
    typmod: i32,
    non_null_count: usize,
) -> Vec<pg_sys::Datum> {
    match cc.type_tag {
        CompressionType::Gorilla => {
            if dt.contains("timestamp") || dt == "date" {
                let timestamps =
                    compression::gorilla::decode_timestamps(cc.data, non_null_count);
                if dt == "date" {
                    timestamps
                        .iter()
                        .map(|&usec| {
                            let unix_days = (usec / 86_400_000_000) as i32;
                            let pg_days = unix_days - PG_EPOCH_OFFSET_DAYS;
                            pg_sys::Datum::from(pg_days as usize)
                        })
                        .collect()
                } else {
                    timestamps
                        .iter()
                        .map(|&usec| {
                            let pg_usec = usec - PG_EPOCH_OFFSET_USEC;
                            pg_sys::Datum::from(pg_usec as usize)
                        })
                        .collect()
                }
            } else if dt == "real" || dt.contains("float4") {
                let floats =
                    compression::gorilla::decode_floats_f32(cc.data, non_null_count);
                floats
                    .iter()
                    .map(|&v| pg_sys::Datum::from(v.to_bits() as usize))
                    .collect()
            } else {
                let floats =
                    compression::gorilla::decode_floats(cc.data, non_null_count);
                floats
                    .iter()
                    .map(|&v| pg_sys::Datum::from(v.to_bits() as usize))
                    .collect()
            }
        }
        CompressionType::DeltaVarint => {
            if dt == "integer" || dt.contains("int4") || dt == "smallint" {
                let ints = compression::integer::decode_i32(cc.data, non_null_count);
                if dt == "smallint" {
                    ints.iter()
                        .map(|&v| pg_sys::Datum::from(v as i16 as usize))
                        .collect()
                } else {
                    ints.iter()
                        .map(|&v| pg_sys::Datum::from(v as usize))
                        .collect()
                }
            } else {
                let ints = compression::integer::decode_i64(cc.data, non_null_count);
                ints.iter()
                    .map(|&v| pg_sys::Datum::from(v as usize))
                    .collect()
            }
        }
        CompressionType::Dictionary | CompressionType::DictionaryLz4 => {
            let norm_buf;
            let dict_data = if cc.type_tag == CompressionType::DictionaryLz4 {
                norm_buf = compression::dictionary::normalize_lz4(cc.data);
                &norm_buf
            } else {
                cc.data
            };
            if type_oid == pg_sys::JSONBOID {
                // Skip UTF-8 validation — stored dictionary entries are binary
                // jsonb varlena payloads, not UTF-8 text.
                let byte_slices = compression::dictionary::decode_to_byte_slices(dict_data, non_null_count);
                unsafe { byte_slices_to_jsonb_datums_arena(&byte_slices) }
            } else {
                let slices = compression::dictionary::decode_to_slices(dict_data, non_null_count);
                unsafe { str_slices_to_text_datums_arena(&slices, type_oid, typmod) }
            }
        }
        CompressionType::Lz4 => {
            let (buf, ranges) = compression::lz4::decode_to_ranges(cc.data, non_null_count);
            if type_oid == pg_sys::JSONBOID {
                // Skip UTF-8 validation — stored bytes are jsonb varlena payload.
                let byte_slices: Vec<&[u8]> = ranges
                    .iter()
                    .map(|&(off, len)| &buf[off..off + len])
                    .collect();
                unsafe { byte_slices_to_jsonb_datums_arena(&byte_slices) }
            } else {
                let slices: Vec<&str> = ranges
                    .iter()
                    .map(|&(off, len)| {
                        std::str::from_utf8(&buf[off..off + len])
                            .expect("invalid UTF-8 in LZ4 data")
                    })
                    .collect();
                unsafe { str_slices_to_text_datums_arena(&slices, type_oid, typmod) }
            }
        }
        CompressionType::Lz4Blocked => {
            let (buf, ranges) = compression::lz4::decode_to_ranges_blocked(cc.data, non_null_count, None);
            if type_oid == pg_sys::JSONBOID {
                let byte_slices: Vec<&[u8]> = ranges
                    .iter()
                    .map(|&(off, len)| &buf[off..off + len])
                    .collect();
                unsafe { byte_slices_to_jsonb_datums_arena(&byte_slices) }
            } else {
                let slices: Vec<&str> = ranges
                    .iter()
                    .map(|&(off, len)| {
                        std::str::from_utf8(&buf[off..off + len])
                            .expect("invalid UTF-8 in LZ4 data")
                    })
                    .collect();
                unsafe { str_slices_to_text_datums_arena(&slices, type_oid, typmod) }
            }
        }
        CompressionType::BooleanBitmap => {
            let bools = compression::boolean::decode(cc.data, non_null_count);
            bools
                .iter()
                .map(|&b| pg_sys::Datum::from(b as usize))
                .collect()
        }
        CompressionType::Constant => {
            if dt == "integer" || dt.contains("int4") || dt == "smallint" {
                let ints = compression::bitpacked::decode_constant_i32(cc.data, non_null_count);
                if dt == "smallint" {
                    ints.iter()
                        .map(|&v| pg_sys::Datum::from(v as i16 as usize))
                        .collect()
                } else {
                    ints.iter()
                        .map(|&v| pg_sys::Datum::from(v as usize))
                        .collect()
                }
            } else {
                let ints = compression::bitpacked::decode_constant_i64(cc.data, non_null_count);
                ints.iter()
                    .map(|&v| pg_sys::Datum::from(v as usize))
                    .collect()
            }
        }
        CompressionType::ForBitpacked => {
            if dt == "integer" || dt.contains("int4") || dt == "smallint" {
                let ints = compression::bitpacked::decode_for_i32(cc.data, non_null_count);
                if dt == "smallint" {
                    ints.iter()
                        .map(|&v| pg_sys::Datum::from(v as i16 as usize))
                        .collect()
                } else {
                    ints.iter()
                        .map(|&v| pg_sys::Datum::from(v as usize))
                        .collect()
                }
            } else {
                let ints = compression::bitpacked::decode_for_i64(cc.data, non_null_count);
                ints.iter()
                    .map(|&v| pg_sys::Datum::from(v as usize))
                    .collect()
            }
        }
    }
}

/// Decompress a column blob directly to PostgreSQL Datums.
///
/// For pass-by-value types (int, float, timestamp, date, bool), the decoded
/// value is stored directly in the Datum with zero allocation.
/// For pass-by-reference types (text, varchar, bpchar), a varlena is allocated
/// in the current memory context (caller must set the right context).
pub(super) unsafe fn decompress_blob_to_datums(
    blob: &[u8],
    data_type: &str,
    type_oid: pg_sys::Oid,
    typmod: i32,
) -> Vec<(pg_sys::Datum, bool)> {
    if blob.is_empty() {
        return Vec::new();
    }

    let cc = CompressedColumnRef::from_bytes(blob);
    let total_count = cc.row_count as usize;
    let non_null_count = count_non_null(cc.null_bitmap, total_count);
    let datums = unsafe {
        decode_compressed_datums(&cc, &data_type.to_lowercase(), type_oid, typmod, non_null_count)
    };
    reinsert_nulls_datum(&datums, cc.null_bitmap, total_count)
}

/// Like `decompress_blob_to_datums` but only decodes up to `max_row` rows
/// (0-indexed). For sequential codecs like DeltaVarint and Gorilla, this
/// allows early termination, skipping decode of rows past `max_row`.
/// The returned Vec has `max_row + 1` elements.
pub(super) unsafe fn decompress_blob_to_datums_truncated(
    blob: &[u8],
    data_type: &str,
    type_oid: pg_sys::Oid,
    typmod: i32,
    max_row: usize,
) -> Vec<(pg_sys::Datum, bool)> {
    if blob.is_empty() {
        return Vec::new();
    }

    let cc = CompressedColumnRef::from_bytes(blob);
    let total_count = cc.row_count as usize;
    let truncated_count = total_count.min(max_row + 1);

    if truncated_count >= total_count {
        // No benefit from truncation — use full path
        return unsafe { decompress_blob_to_datums(blob, data_type, type_oid, typmod) };
    }

    let non_null_count = count_non_null(cc.null_bitmap, truncated_count);
    let datums = unsafe {
        decode_compressed_datums(&cc, &data_type.to_lowercase(), type_oid, typmod, non_null_count)
    };
    reinsert_nulls_datum(&datums, cc.null_bitmap, truncated_count)
}

/// Decompress a text column blob with LIKE filtering pushed into decompression.
///
/// Instead of allocating a PG varlena datum for every row and then filtering,
/// this matches the LIKE pattern against raw `&str` slices (zero-copy) and only
/// calls `str_to_text_datum()` for rows that match. Non-matching rows get a
/// dummy datum that will never be read (the returned selection vector marks them
/// as filtered out).
///
/// Returns `(datums, like_selection)` where:
/// - `datums`: Full-length datum array with nulls reinserted. Matching rows have
///   real varlena datums; non-matching rows have `(Datum(0), false)`.
/// - `like_selection`: Per-row bool vector (true = matched LIKE).
pub(super) unsafe fn decompress_text_blob_with_like_filter(
    blob: &[u8],
    type_oid: pg_sys::Oid,
    typmod: i32,
    strategy: &LikeStrategy,
    negate: bool,
    max_rows: Option<usize>,
) -> (Vec<(pg_sys::Datum, bool)>, Vec<bool>) {
    if blob.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let cc = CompressedColumnRef::from_bytes(blob);
    let total_count = match max_rows {
        Some(mr) => mr.min(cc.row_count as usize),
        None => cc.row_count as usize,
    };
    let non_null_count = count_non_null(cc.null_bitmap, total_count);

    // Match a &str against the LikeStrategy, applying negation.
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

    // Build (non-null datums, non-null selection) — only non-null values
    let (nn_datums, nn_sel): (Vec<pg_sys::Datum>, Vec<bool>) = match cc.type_tag {
        CompressionType::Dictionary | CompressionType::DictionaryLz4 => {
            let norm_buf;
            let dict_data = if cc.type_tag == CompressionType::DictionaryLz4 {
                norm_buf = compression::dictionary::normalize_lz4(cc.data);
                &norm_buf
            } else {
                cc.data
            };
            let (dict_entries, indices) =
                compression::dictionary::decode_dict_and_indices(dict_data, non_null_count);

            // Pre-match each dictionary entry (tiny vec, e.g. a few thousand)
            let dict_matches: Vec<bool> = dict_entries.iter().map(|s| matches_like(s)).collect();

            // Collect matched slices for arena allocation
            let sel: Vec<bool> = indices.iter().map(|&idx| dict_matches[idx as usize]).collect();
            let matched_slices: Vec<&str> = indices
                .iter()
                .zip(sel.iter())
                .filter(|&(_, &pass)| pass)
                .map(|(&idx, _)| dict_entries[idx as usize])
                .collect();
            let matched_datums = unsafe {
                str_slices_to_text_datums_arena(&matched_slices, type_oid, typmod)
            };

            // Merge matched datums back with dummy datums for non-matching rows
            let mut datums = Vec::with_capacity(non_null_count);
            let mut match_idx = 0;
            for &pass in &sel {
                if pass {
                    datums.push(matched_datums[match_idx]);
                    match_idx += 1;
                } else {
                    datums.push(pg_sys::Datum::from(0));
                }
            }
            (datums, sel)
        }
        CompressionType::Lz4 | CompressionType::Lz4Blocked => {
            let (buf, ranges) = if cc.type_tag == CompressionType::Lz4 {
                compression::lz4::decode_to_ranges(cc.data, non_null_count)
            } else {
                compression::lz4::decode_to_ranges_blocked(cc.data, non_null_count, None)
            };

            // First pass: determine which rows match.
            // For Contains patterns, use SIMD-accelerated memmem search on the
            // raw decompressed buffer to find all needle positions, then map
            // positions to string ranges. Avoids per-string branching overhead.
            let sel: Vec<bool> = match (strategy, negate) {
                (LikeStrategy::Contains(needle), false) => {
                    let needle_len = needle.len();
                    let finder = memchr::memmem::Finder::new(needle.as_bytes());
                    let mut sel = vec![false; non_null_count];
                    // SIMD scan: find all needle positions in the raw buffer,
                    // then map each hit to the string range it belongs to.
                    let mut search_start = 0;
                    let mut range_idx = 0;
                    while let Some(pos) = finder.find(&buf[search_start..]) {
                        let abs_pos = search_start + pos;
                        // Advance range_idx past ranges that end before this hit
                        while range_idx < ranges.len() {
                            let (off, len) = ranges[range_idx];
                            if abs_pos < off + len {
                                // Hit starts within (or before) this range.
                                // Verify the full needle fits within this string
                                // to avoid false positives from cross-boundary matches.
                                if abs_pos >= off && abs_pos + needle_len <= off + len {
                                    sel[range_idx] = true;
                                }
                                break;
                            }
                            range_idx += 1;
                        }
                        if range_idx >= ranges.len() {
                            break;
                        }
                        search_start = abs_pos + 1;
                    }
                    sel
                }
                (LikeStrategy::Contains(needle), true) => {
                    // NOT LIKE '%needle%': mark rows that DO contain the needle
                    // as false, rest stays true.
                    let needle_len = needle.len();
                    let finder = memchr::memmem::Finder::new(needle.as_bytes());
                    let mut sel = vec![true; non_null_count];
                    let mut search_start = 0;
                    let mut range_idx = 0;
                    while let Some(pos) = finder.find(&buf[search_start..]) {
                        let abs_pos = search_start + pos;
                        while range_idx < ranges.len() {
                            let (off, len) = ranges[range_idx];
                            if abs_pos < off + len {
                                if abs_pos >= off && abs_pos + needle_len <= off + len {
                                    sel[range_idx] = false;
                                }
                                break;
                            }
                            range_idx += 1;
                        }
                        if range_idx >= ranges.len() {
                            break;
                        }
                        search_start = abs_pos + 1;
                    }
                    sel
                }
                _ => {
                    // Other strategies: per-string evaluation
                    ranges
                        .iter()
                        .map(|&(off, len)| {
                            let text = std::str::from_utf8(&buf[off..off + len])
                                .expect("invalid UTF-8 in LZ4 data");
                            matches_like(text)
                        })
                        .collect()
                }
            };

            // Collect matched slices for arena allocation
            let matched_slices: Vec<&str> = ranges
                .iter()
                .zip(sel.iter())
                .filter(|&(_, &pass)| pass)
                .map(|(&(off, len), _)| {
                    std::str::from_utf8(&buf[off..off + len])
                        .expect("invalid UTF-8 in LZ4 data")
                })
                .collect();
            let matched_datums = unsafe {
                str_slices_to_text_datums_arena(&matched_slices, type_oid, typmod)
            };

            // Merge matched datums back
            let mut datums = Vec::with_capacity(non_null_count);
            let mut match_idx = 0;
            for &pass in &sel {
                if pass {
                    datums.push(matched_datums[match_idx]);
                    match_idx += 1;
                } else {
                    datums.push(pg_sys::Datum::from(0));
                }
            }
            (datums, sel)
        }
        _ => {
            // Unexpected compression type for text — fall back to full decompression
            return {
                let full = unsafe { decompress_blob_to_datums(
                    blob,
                    &pg_type_name(type_oid),
                    type_oid,
                    typmod,
                ) };
                let sel = vec![true; full.len()];
                (full, sel)
            };
        }
    };

    // Reinsert nulls into both datums and selection vectors
    let null_bitmap = cc.null_bitmap;
    if null_bitmap.is_empty() {
        // No nulls — pair up directly
        let datums: Vec<(pg_sys::Datum, bool)> = nn_datums.into_iter().map(|d| (d, false)).collect();
        (datums, nn_sel)
    } else {
        let mut datums = Vec::with_capacity(total_count);
        let mut sel = Vec::with_capacity(total_count);
        let mut val_idx = 0;
        for i in 0..total_count {
            let is_null = (null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
            if is_null {
                datums.push((pg_sys::Datum::from(0), true));
                sel.push(false); // NULLs don't match LIKE
            } else {
                datums.push((nn_datums[val_idx], false));
                sel.push(nn_sel[val_idx]);
                val_idx += 1;
            }
        }
        (datums, sel)
    }
}

/// Decompress a text column blob to raw Rust strings (no PG datum allocation).
/// Used for regexp_replace GROUP BY where we need string values for the cache.
///
/// Also applies batch quals for this column (Ne empty string) during decompression.
///
/// Returns `(strings, selection)` where:
/// - `strings`: Vec of Option<String> (None for NULL), length = total_count
/// - `selection`: Per-row bool (true = passes filter). Empty if no filter.
pub(super) fn decompress_text_blob_to_raw_strings(
    blob: &[u8],
    batch_quals: &[BatchQual],
    col_idx: usize,
) -> (Vec<Option<String>>, Vec<bool>) {
    if blob.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let cc = CompressedColumnRef::from_bytes(blob);
    let total_count = cc.row_count as usize;
    let non_null_count = count_non_null(cc.null_bitmap, total_count);

    // Check for Ne '' filter on this column
    let has_ne_empty = batch_quals.iter().any(|bq| {
        bq.col_idx == col_idx
            && bq.text_const.as_deref() == Some("")
            && bq.op == BatchCompareOp::Ne
    });

    let (nn_strings, nn_sel): (Vec<String>, Vec<bool>) = match cc.type_tag {
        CompressionType::Dictionary | CompressionType::DictionaryLz4 => {
            let norm_buf;
            let dict_data = if cc.type_tag == CompressionType::DictionaryLz4 {
                norm_buf = compression::dictionary::normalize_lz4(cc.data);
                &norm_buf
            } else {
                cc.data
            };
            let (dict_entries, indices) =
                compression::dictionary::decode_dict_and_indices(dict_data, non_null_count);

            let strings: Vec<String> = indices.iter().map(|&idx| dict_entries[idx as usize].to_string()).collect();
            let sel: Vec<bool> = if has_ne_empty {
                compression::dictionary::check_ne_empty(dict_data, non_null_count)
            } else {
                Vec::new()
            };
            (strings, sel)
        }
        CompressionType::Lz4 => {
            let (buf, ranges) = compression::lz4::decode_to_ranges(cc.data, non_null_count);
            let strings: Vec<String> = ranges.iter().map(|&(off, len)| {
                std::str::from_utf8(&buf[off..off + len]).unwrap_or("").to_string()
            }).collect();
            let sel: Vec<bool> = if has_ne_empty {
                ranges.iter().map(|&(_off, len)| len > 0).collect()
            } else {
                Vec::new()
            };
            (strings, sel)
        }
        CompressionType::Lz4Blocked => {
            let (buf, ranges) = compression::lz4::decode_to_ranges_blocked(cc.data, non_null_count, None);
            let strings: Vec<String> = ranges.iter().map(|&(off, len)| {
                std::str::from_utf8(&buf[off..off + len]).unwrap_or("").to_string()
            }).collect();
            let sel: Vec<bool> = if has_ne_empty {
                ranges.iter().map(|&(_off, len)| len > 0).collect()
            } else {
                Vec::new()
            };
            (strings, sel)
        }
        _ => {
            let strings = vec![String::new(); non_null_count];
            let sel = if has_ne_empty { vec![false; non_null_count] } else { Vec::new() };
            (strings, sel)
        }
    };

    // Reinsert nulls
    if cc.null_bitmap.is_empty() {
        let strings: Vec<Option<String>> = nn_strings.into_iter().map(Some).collect();
        (strings, nn_sel)
    } else {
        let mut strings = Vec::with_capacity(total_count);
        let mut sel = if has_ne_empty { Vec::with_capacity(total_count) } else { Vec::new() };
        let mut val_idx = 0;
        for i in 0..total_count {
            let is_null = (cc.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
            if is_null {
                strings.push(None);
                if has_ne_empty { sel.push(false); }
            } else {
                strings.push(Some(nn_strings[val_idx].clone()));
                if has_ne_empty { sel.push(nn_sel[val_idx]); }
                val_idx += 1;
            }
        }
        (strings, sel)
    }
}

/// Decompress a text column blob with equality/inequality filtering pushed into decompression.
///
/// Similar to `decompress_text_blob_with_like_filter`, but matches against a constant
/// string using `==` (or `!=` when `is_ne` is true). For dictionary-compressed data,
/// this checks each dictionary entry once and uses the indices to build the selection
/// vector — O(dict_size) comparisons instead of O(row_count).
///
/// Returns `(datums, eq_selection)` where:
/// - `datums`: Full-length datum array with nulls reinserted.
/// - `eq_selection`: Per-row bool vector (true = matched equality/inequality).
pub(super) unsafe fn decompress_text_blob_with_eq_filter(
    blob: &[u8],
    type_oid: pg_sys::Oid,
    typmod: i32,
    const_str: &str,
    is_ne: bool,
    max_rows: Option<usize>,
) -> (Vec<(pg_sys::Datum, bool)>, Vec<bool>) {
    if blob.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let cc = CompressedColumnRef::from_bytes(blob);
    let total_count = match max_rows {
        Some(mr) => mr.min(cc.row_count as usize),
        None => cc.row_count as usize,
    };
    let non_null_count = count_non_null(cc.null_bitmap, total_count);

    let matches_eq = |text: &str| -> bool {
        let eq = text == const_str;
        if is_ne { !eq } else { eq }
    };

    let (nn_datums, nn_sel): (Vec<pg_sys::Datum>, Vec<bool>) = match cc.type_tag {
        CompressionType::Dictionary | CompressionType::DictionaryLz4 => {
            let norm_buf;
            let dict_data = if cc.type_tag == CompressionType::DictionaryLz4 {
                norm_buf = compression::dictionary::normalize_lz4(cc.data);
                &norm_buf
            } else {
                cc.data
            };
            let hdr = compression::dictionary::parse_header(dict_data);

            // Fast path for empty-string comparison: use precomputed empty_string_idx
            let dict_matches: Vec<bool> = if const_str.is_empty() && hdr.empty_string_idx != 0xFFFF {
                let mut m = vec![is_ne; hdr.dict.len()];
                m[hdr.empty_string_idx as usize] = !is_ne;
                m
            } else if const_str.is_empty() && hdr.empty_string_idx == 0xFFFF {
                // No empty string in dict: eq=all false, ne=all true
                vec![is_ne; hdr.dict.len()]
            } else {
                hdr.dict.iter().map(|s| matches_eq(s)).collect()
            };

            let mut indices = Vec::with_capacity(non_null_count);
            for i in 0..non_null_count {
                indices.push(compression::dictionary::read_index(dict_data, hdr.indices_start, hdr.index_width, i));
            }

            let sel: Vec<bool> = indices.iter().map(|&idx| dict_matches[idx as usize]).collect();
            let matched_slices: Vec<&str> = indices
                .iter()
                .zip(sel.iter())
                .filter(|&(_, &pass)| pass)
                .map(|(&idx, _)| hdr.dict[idx as usize])
                .collect();
            let matched_datums = unsafe {
                str_slices_to_text_datums_arena(&matched_slices, type_oid, typmod)
            };

            let mut datums = Vec::with_capacity(non_null_count);
            let mut match_idx = 0;
            for &pass in &sel {
                if pass {
                    datums.push(matched_datums[match_idx]);
                    match_idx += 1;
                } else {
                    datums.push(pg_sys::Datum::from(0));
                }
            }
            (datums, sel)
        }
        CompressionType::Lz4 | CompressionType::Lz4Blocked => {
            let (buf, ranges) = if cc.type_tag == CompressionType::Lz4 {
                compression::lz4::decode_to_ranges(cc.data, non_null_count)
            } else {
                compression::lz4::decode_to_ranges_blocked(cc.data, non_null_count, None)
            };

            let slices: Vec<&str> = ranges
                .iter()
                .map(|&(off, len)| {
                    std::str::from_utf8(&buf[off..off + len])
                        .expect("invalid UTF-8 in LZ4 data")
                })
                .collect();
            let sel: Vec<bool> = slices.iter().map(|s| matches_eq(s)).collect();

            let matched_slices: Vec<&str> = slices
                .iter()
                .zip(sel.iter())
                .filter(|&(_, &pass)| pass)
                .map(|(&s, _)| s)
                .collect();
            let matched_datums = unsafe {
                str_slices_to_text_datums_arena(&matched_slices, type_oid, typmod)
            };

            let mut datums = Vec::with_capacity(non_null_count);
            let mut match_idx = 0;
            for &pass in &sel {
                if pass {
                    datums.push(matched_datums[match_idx]);
                    match_idx += 1;
                } else {
                    datums.push(pg_sys::Datum::from(0));
                }
            }
            (datums, sel)
        }
        _ => {
            // Unexpected compression type — fall back to full decompression
            return {
                let full = unsafe { decompress_blob_to_datums(
                    blob,
                    &pg_type_name(type_oid),
                    type_oid,
                    typmod,
                ) };
                let sel = vec![true; full.len()];
                (full, sel)
            };
        }
    };

    // Reinsert nulls into both datums and selection vectors
    let null_bitmap = cc.null_bitmap;
    if null_bitmap.is_empty() {
        let datums: Vec<(pg_sys::Datum, bool)> = nn_datums.into_iter().map(|d| (d, false)).collect();
        (datums, nn_sel)
    } else {
        let mut datums = Vec::with_capacity(total_count);
        let mut sel = Vec::with_capacity(total_count);
        let mut val_idx = 0;
        for i in 0..total_count {
            let is_null = (null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
            if is_null {
                datums.push((pg_sys::Datum::from(0), true));
                sel.push(false); // NULLs don't match equality
            } else {
                datums.push((nn_datums[val_idx], false));
                sel.push(nn_sel[val_idx]);
                val_idx += 1;
            }
        }
        (datums, sel)
    }
}

/// Decompress a text column blob with `IN (...)` filtering pushed into decompression.
///
/// Same shape as `decompress_text_blob_with_eq_filter` but matches against a
/// set of constant strings (PG `Var = ANY (ARRAY[...])` / `Var IN (...)`).
/// For dictionary-compressed data, the set probe runs once per dict entry,
/// not per row — O(dict_size) hashes vs O(row_count).
///
/// `is_not_in = true` flips to `NOT IN (...)`. Empty `const_strs` returns
/// the all-false (or all-true if `is_not_in`) selection without decompressing.
pub(super) unsafe fn decompress_text_blob_with_in_filter(
    blob: &[u8],
    type_oid: pg_sys::Oid,
    typmod: i32,
    const_strs: &[String],
    is_not_in: bool,
    max_rows: Option<usize>,
) -> (Vec<(pg_sys::Datum, bool)>, Vec<bool>) {
    if blob.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let cc = CompressedColumnRef::from_bytes(blob);
    let total_count = match max_rows {
        Some(mr) => mr.min(cc.row_count as usize),
        None => cc.row_count as usize,
    };
    let non_null_count = count_non_null(cc.null_bitmap, total_count);

    let const_set: std::collections::HashSet<&str> =
        const_strs.iter().map(|s| s.as_str()).collect();
    let matches_in = |text: &str| -> bool {
        let hit = const_set.contains(text);
        if is_not_in { !hit } else { hit }
    };

    let (nn_datums, nn_sel): (Vec<pg_sys::Datum>, Vec<bool>) = match cc.type_tag {
        CompressionType::Dictionary | CompressionType::DictionaryLz4 => {
            let norm_buf;
            let dict_data = if cc.type_tag == CompressionType::DictionaryLz4 {
                norm_buf = compression::dictionary::normalize_lz4(cc.data);
                &norm_buf
            } else {
                cc.data
            };
            let hdr = compression::dictionary::parse_header(dict_data);

            let dict_matches: Vec<bool> = hdr.dict.iter().map(|s| matches_in(s)).collect();

            let mut indices = Vec::with_capacity(non_null_count);
            for i in 0..non_null_count {
                indices.push(compression::dictionary::read_index(
                    dict_data,
                    hdr.indices_start,
                    hdr.index_width,
                    i,
                ));
            }

            let sel: Vec<bool> = indices.iter().map(|&idx| dict_matches[idx as usize]).collect();
            let matched_slices: Vec<&str> = indices
                .iter()
                .zip(sel.iter())
                .filter(|&(_, &pass)| pass)
                .map(|(&idx, _)| hdr.dict[idx as usize])
                .collect();
            let matched_datums = unsafe {
                str_slices_to_text_datums_arena(&matched_slices, type_oid, typmod)
            };

            let mut datums = Vec::with_capacity(non_null_count);
            let mut match_idx = 0;
            for &pass in &sel {
                if pass {
                    datums.push(matched_datums[match_idx]);
                    match_idx += 1;
                } else {
                    datums.push(pg_sys::Datum::from(0));
                }
            }
            (datums, sel)
        }
        CompressionType::Lz4 | CompressionType::Lz4Blocked => {
            let (buf, ranges) = if cc.type_tag == CompressionType::Lz4 {
                compression::lz4::decode_to_ranges(cc.data, non_null_count)
            } else {
                compression::lz4::decode_to_ranges_blocked(cc.data, non_null_count, None)
            };

            let slices: Vec<&str> = ranges
                .iter()
                .map(|&(off, len)| {
                    std::str::from_utf8(&buf[off..off + len])
                        .expect("invalid UTF-8 in LZ4 data")
                })
                .collect();
            let sel: Vec<bool> = slices.iter().map(|s| matches_in(s)).collect();

            let matched_slices: Vec<&str> = slices
                .iter()
                .zip(sel.iter())
                .filter(|&(_, &pass)| pass)
                .map(|(&s, _)| s)
                .collect();
            let matched_datums = unsafe {
                str_slices_to_text_datums_arena(&matched_slices, type_oid, typmod)
            };

            let mut datums = Vec::with_capacity(non_null_count);
            let mut match_idx = 0;
            for &pass in &sel {
                if pass {
                    datums.push(matched_datums[match_idx]);
                    match_idx += 1;
                } else {
                    datums.push(pg_sys::Datum::from(0));
                }
            }
            (datums, sel)
        }
        _ => {
            return {
                let full = unsafe { decompress_blob_to_datums(
                    blob,
                    &pg_type_name(type_oid),
                    type_oid,
                    typmod,
                ) };
                let sel = vec![true; full.len()];
                (full, sel)
            };
        }
    };

    let null_bitmap = cc.null_bitmap;
    if null_bitmap.is_empty() {
        let datums: Vec<(pg_sys::Datum, bool)> =
            nn_datums.into_iter().map(|d| (d, false)).collect();
        (datums, nn_sel)
    } else {
        let mut datums = Vec::with_capacity(total_count);
        let mut sel = Vec::with_capacity(total_count);
        let mut val_idx = 0;
        for i in 0..total_count {
            let is_null = (null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
            if is_null {
                datums.push((pg_sys::Datum::from(0), true));
                sel.push(false);
            } else {
                datums.push((nn_datums[val_idx], false));
                sel.push(nn_sel[val_idx]);
                val_idx += 1;
            }
        }
        (datums, sel)
    }
}

/// Decompress a text column blob to int4 lengths without varlena allocation.
///
/// For Dictionary: compute length of each dict entry once, map indices to lengths.
/// For LZ4/LZ4Blocked: range lengths are the string lengths.
///
/// When `filter_empty` is true, rows where the string is empty ("") are marked
/// as filtered in the returned selection vector. This handles `URL <> ''` without
/// needing full text decompression.
///
/// Returns `(lengths_as_int4_datums, selection)`. Selection is empty if
/// `filter_empty` is false and there are no nulls to filter.
pub(super) fn decompress_text_blob_to_lengths(
    blob: &[u8],
    filter_empty: bool,
) -> (Vec<(pg_sys::Datum, bool)>, Vec<bool>) {
    if blob.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let cc = CompressedColumnRef::from_bytes(blob);
    let total_count = cc.row_count as usize;
    let non_null_count = count_non_null(cc.null_bitmap, total_count);

    // Compute non-null lengths and selection
    let (nn_lengths, nn_sel): (Vec<i32>, Vec<bool>) = match cc.type_tag {
        CompressionType::Dictionary | CompressionType::DictionaryLz4 => {
            let norm_buf;
            let dict_data = if cc.type_tag == CompressionType::DictionaryLz4 {
                norm_buf = compression::dictionary::normalize_lz4(cc.data);
                &norm_buf
            } else {
                cc.data
            };
            let (dict_entries, indices) =
                compression::dictionary::decode_dict_and_indices(dict_data, non_null_count);

            // Pre-compute lengths (character count, not byte count) for each dict entry
            let dict_lengths: Vec<i32> = dict_entries.iter().map(|s| s.chars().count() as i32).collect();

            let lengths: Vec<i32> = indices.iter().map(|&idx| dict_lengths[idx as usize]).collect();
            let sel: Vec<bool> = if filter_empty {
                compression::dictionary::check_ne_empty(dict_data, non_null_count)
            } else {
                Vec::new()
            };

            (lengths, sel)
        }
        CompressionType::Lz4 => {
            let (buf, ranges) = compression::lz4::decode_to_ranges(cc.data, non_null_count);
            let lengths: Vec<i32> = ranges.iter().map(|&(off, len)| {
                let s = std::str::from_utf8(&buf[off..off + len]).unwrap_or("");
                s.chars().count() as i32
            }).collect();
            let sel: Vec<bool> = if filter_empty {
                ranges.iter().map(|&(_off, len)| len > 0).collect()
            } else {
                Vec::new()
            };
            (lengths, sel)
        }
        CompressionType::Lz4Blocked => {
            let (buf, ranges) = compression::lz4::decode_to_ranges_blocked(cc.data, non_null_count, None);
            let lengths: Vec<i32> = ranges.iter().map(|&(off, len)| {
                let s = std::str::from_utf8(&buf[off..off + len]).unwrap_or("");
                s.chars().count() as i32
            }).collect();
            let sel: Vec<bool> = if filter_empty {
                ranges.iter().map(|&(_off, len)| len > 0).collect()
            } else {
                Vec::new()
            };
            (lengths, sel)
        }
        _ => {
            // Unexpected compression type for text — return zeros
            let lengths = vec![0i32; non_null_count];
            let sel = if filter_empty { vec![false; non_null_count] } else { Vec::new() };
            (lengths, sel)
        }
    };

    // Reinsert nulls
    let null_bitmap = cc.null_bitmap;
    if null_bitmap.is_empty() {
        let datums: Vec<(pg_sys::Datum, bool)> = nn_lengths
            .iter()
            .map(|&len| (pg_sys::Datum::from(len as usize), false))
            .collect();
        (datums, nn_sel)
    } else {
        let mut datums = Vec::with_capacity(total_count);
        let mut sel = if filter_empty { Vec::with_capacity(total_count) } else { Vec::new() };
        let mut val_idx = 0;
        for i in 0..total_count {
            let is_null = (null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
            if is_null {
                datums.push((pg_sys::Datum::from(0usize), true));
                if filter_empty {
                    sel.push(false); // NULLs don't pass filter
                }
            } else {
                datums.push((pg_sys::Datum::from(nn_lengths[val_idx] as usize), false));
                if filter_empty {
                    sel.push(nn_sel[val_idx]);
                }
                val_idx += 1;
            }
        }
        (datums, sel)
    }
}

/// Decompress a text column blob but only allocate varlena for rows where
/// `selection[i] == true`. Non-selected rows get a placeholder `Datum(0)`.
///
/// This is used in two-phase decompression: after batch quals produce a
/// selection vector, non-filter text columns only need real datums for the
/// (typically small) set of matching rows.
pub(super) unsafe fn decompress_text_blob_with_selection(
    blob: &[u8],
    type_oid: pg_sys::Oid,
    typmod: i32,
    selection: &[bool],
) -> Vec<(pg_sys::Datum, bool)> {
    if blob.is_empty() {
        return Vec::new();
    }

    let cc = CompressedColumnRef::from_bytes(blob);
    let total_count = cc.row_count as usize;
    let non_null_count = count_non_null(cc.null_bitmap, total_count);

    // Build a non-null selection vector (strip out positions that are null)
    let nn_selection: Vec<bool> = if cc.null_bitmap.is_empty() {
        selection.to_vec()
    } else {
        let mut nn_sel = Vec::with_capacity(non_null_count);
        for (i, &sel) in selection.iter().enumerate().take(total_count) {
            let is_null = (cc.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
            if !is_null {
                nn_sel.push(sel);
            }
        }
        nn_sel
    };

    let nn_datums: Vec<pg_sys::Datum> = match cc.type_tag {
        CompressionType::Dictionary | CompressionType::DictionaryLz4 => {
            let norm_buf;
            let dict_data = if cc.type_tag == CompressionType::DictionaryLz4 {
                norm_buf = compression::dictionary::normalize_lz4(cc.data);
                &norm_buf
            } else {
                cc.data
            };
            let (dict_entries, indices) =
                compression::dictionary::decode_dict_and_indices(dict_data, non_null_count);

            // Collect only selected slices for arena allocation
            let matched_slices: Vec<&str> = indices
                .iter()
                .zip(nn_selection.iter())
                .filter(|&(_, &sel)| sel)
                .map(|(&idx, _)| dict_entries[idx as usize])
                .collect();
            let matched_datums = unsafe {
                str_slices_to_text_datums_arena(&matched_slices, type_oid, typmod)
            };

            // Merge back: selected rows get real datums, others get placeholder
            let mut datums = Vec::with_capacity(non_null_count);
            let mut match_idx = 0;
            for &sel in &nn_selection {
                if sel {
                    datums.push(matched_datums[match_idx]);
                    match_idx += 1;
                } else {
                    datums.push(pg_sys::Datum::from(0));
                }
            }
            datums
        }
        CompressionType::Lz4 => {
            let (buf, ranges) =
                compression::lz4::decode_to_ranges(cc.data, non_null_count);

            let slices: Vec<&str> = ranges
                .iter()
                .map(|&(off, len)| {
                    std::str::from_utf8(&buf[off..off + len])
                        .expect("invalid UTF-8 in LZ4 data")
                })
                .collect();

            // Collect only selected slices for arena allocation
            let matched_slices: Vec<&str> = slices
                .iter()
                .zip(nn_selection.iter())
                .filter(|&(_, &sel)| sel)
                .map(|(&s, _)| s)
                .collect();
            let matched_datums = unsafe {
                str_slices_to_text_datums_arena(&matched_slices, type_oid, typmod)
            };

            let mut datums = Vec::with_capacity(non_null_count);
            let mut match_idx = 0;
            for &sel in &nn_selection {
                if sel {
                    datums.push(matched_datums[match_idx]);
                    match_idx += 1;
                } else {
                    datums.push(pg_sys::Datum::from(0));
                }
            }
            datums
        }
        CompressionType::Lz4Blocked => {
            // Partial decompression: only decode blocks containing selected rows
            let (buf, ranges) =
                compression::lz4::decode_to_ranges_blocked(cc.data, non_null_count, Some(&nn_selection));

            // Collect only selected slices for arena allocation
            let matched_slices: Vec<&str> = ranges
                .iter()
                .zip(nn_selection.iter())
                .filter(|&(_, &sel)| sel)
                .map(|(&(off, len), _)| {
                    std::str::from_utf8(&buf[off..off + len])
                        .expect("invalid UTF-8 in LZ4 data")
                })
                .collect();
            let matched_datums = unsafe {
                str_slices_to_text_datums_arena(&matched_slices, type_oid, typmod)
            };

            let mut datums = Vec::with_capacity(non_null_count);
            let mut match_idx = 0;
            for &sel in &nn_selection {
                if sel {
                    datums.push(matched_datums[match_idx]);
                    match_idx += 1;
                } else {
                    datums.push(pg_sys::Datum::from(0));
                }
            }
            datums
        }
        _ => {
            // Unexpected compression type — fall back to full decompression
            let full = unsafe {
                decompress_blob_to_datums(blob, &pg_type_name(type_oid), type_oid, typmod)
            };
            return full;
        }
    };

    // Reinsert nulls
    if cc.null_bitmap.is_empty() {
        nn_datums.into_iter().map(|d| (d, false)).collect()
    } else {
        let mut result = Vec::with_capacity(total_count);
        let mut val_idx = 0;
        for i in 0..total_count {
            let is_null = (cc.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
            if is_null {
                result.push((pg_sys::Datum::from(0), true));
            } else {
                result.push((nn_datums[val_idx], false));
                val_idx += 1;
            }
        }
        result
    }
}

/// Selection-aware decompression for jsonb columns. Mirrors
/// `decompress_text_blob_with_selection` exactly: decodes the column
/// blob in full (LZ4 / Dictionary; the underlying codecs aren't
/// row-skippable, except `Lz4Blocked` which uses the selection mask
/// to skip whole blocks), but only allocates the per-row jsonb varlena
/// for rows whose `selection[i] == true`. Filtered-out rows get a
/// placeholder `Datum::from(0)` — the row-emission loop in
/// `try_emit_next_row` skips those rows via the selection vector, so
/// the placeholder is never read.
///
/// Big win for queries like Q0023 where a Phase 1 batch_qual filter
/// (e.g. event_type='Delivered' + time range) eliminates ~95% of rows
/// before Phase 2, but jsonb columns referenced by un-pushdownable
/// predicates (`event_payload->>'terminal' = 'Berlin'`) or by
/// projection still need to be decompressed for the survivors.
pub(super) unsafe fn decompress_jsonb_blob_with_selection(
    blob: &[u8],
    selection: &[bool],
) -> Vec<(pg_sys::Datum, bool)> {
    if blob.is_empty() {
        return Vec::new();
    }

    let cc = CompressedColumnRef::from_bytes(blob);
    let total_count = cc.row_count as usize;
    let non_null_count = count_non_null(cc.null_bitmap, total_count);

    // Build a non-null selection vector (strip out null positions).
    let nn_selection: Vec<bool> = if cc.null_bitmap.is_empty() {
        selection.to_vec()
    } else {
        let mut nn_sel = Vec::with_capacity(non_null_count);
        for (i, &sel) in selection.iter().enumerate().take(total_count) {
            let is_null = (cc.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
            if !is_null {
                nn_sel.push(sel);
            }
        }
        nn_sel
    };

    let nn_datums: Vec<pg_sys::Datum> = match cc.type_tag {
        CompressionType::Dictionary | CompressionType::DictionaryLz4 => {
            let norm_buf;
            let dict_data = if cc.type_tag == CompressionType::DictionaryLz4 {
                norm_buf = compression::dictionary::normalize_lz4(cc.data);
                &norm_buf
            } else {
                cc.data
            };
            let byte_slices =
                compression::dictionary::decode_to_byte_slices(dict_data, non_null_count);

            // Allocate jsonb datums only for selected rows.
            let matched_slices: Vec<&[u8]> = byte_slices
                .iter()
                .zip(nn_selection.iter())
                .filter(|&(_, &sel)| sel)
                .map(|(&b, _)| b)
                .collect();
            let matched_datums = unsafe { byte_slices_to_jsonb_datums_arena(&matched_slices) };

            let mut datums = Vec::with_capacity(non_null_count);
            let mut match_idx = 0;
            for &sel in &nn_selection {
                if sel {
                    datums.push(matched_datums[match_idx]);
                    match_idx += 1;
                } else {
                    datums.push(pg_sys::Datum::from(0));
                }
            }
            datums
        }
        CompressionType::Lz4 => {
            let (buf, ranges) =
                compression::lz4::decode_to_ranges(cc.data, non_null_count);

            let matched_slices: Vec<&[u8]> = ranges
                .iter()
                .zip(nn_selection.iter())
                .filter(|&(_, &sel)| sel)
                .map(|(&(off, len), _)| &buf[off..off + len])
                .collect();
            let matched_datums = unsafe { byte_slices_to_jsonb_datums_arena(&matched_slices) };

            let mut datums = Vec::with_capacity(non_null_count);
            let mut match_idx = 0;
            for &sel in &nn_selection {
                if sel {
                    datums.push(matched_datums[match_idx]);
                    match_idx += 1;
                } else {
                    datums.push(pg_sys::Datum::from(0));
                }
            }
            datums
        }
        CompressionType::Lz4Blocked => {
            // Partial decompression: only decode blocks containing selected rows.
            let (buf, ranges) = compression::lz4::decode_to_ranges_blocked(
                cc.data,
                non_null_count,
                Some(&nn_selection),
            );

            let matched_slices: Vec<&[u8]> = ranges
                .iter()
                .zip(nn_selection.iter())
                .filter(|&(_, &sel)| sel)
                .map(|(&(off, len), _)| &buf[off..off + len])
                .collect();
            let matched_datums = unsafe { byte_slices_to_jsonb_datums_arena(&matched_slices) };

            let mut datums = Vec::with_capacity(non_null_count);
            let mut match_idx = 0;
            for &sel in &nn_selection {
                if sel {
                    datums.push(matched_datums[match_idx]);
                    match_idx += 1;
                } else {
                    datums.push(pg_sys::Datum::from(0));
                }
            }
            datums
        }
        _ => {
            // Unexpected compression type — fall back to full decompression.
            let full = unsafe {
                decompress_blob_to_datums(blob, "jsonb", pg_sys::JSONBOID, -1)
            };
            return full;
        }
    };

    // Reinsert nulls.
    if cc.null_bitmap.is_empty() {
        nn_datums.into_iter().map(|d| (d, false)).collect()
    } else {
        let mut result = Vec::with_capacity(total_count);
        let mut val_idx = 0;
        for i in 0..total_count {
            let is_null = (cc.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
            if is_null {
                result.push((pg_sys::Datum::from(0), true));
            } else {
                result.push((nn_datums[val_idx], false));
                val_idx += 1;
            }
        }
        result
    }
}

/// Create a text/varchar/bpchar datum from a Rust string.
/// Allocates in the current memory context.
/// Compare two strings using PG's collation-aware comparison.
/// Returns negative if a < b, 0 if equal, positive if a > b.
#[inline]
pub(super) unsafe fn collation_strcmp(a: &str, b: &str) -> i32 {
    unsafe {
        pg_sys::varstr_cmp(
            a.as_ptr() as *const _,
            a.len() as i32,
            b.as_ptr() as *const _,
            b.len() as i32,
            pg_sys::DEFAULT_COLLATION_OID,
        )
    }
}

pub(super) unsafe fn str_to_text_datum(s: &str, type_oid: pg_sys::Oid, typmod: i32) -> pg_sys::Datum {
    unsafe {
        // bpchar needs the type input function for padding; jsonb stores
        // as canonical text and needs the input function to produce a real
        // jsonb binary Datum (otherwise jsonb operators segfault).
        if type_oid == pg_sys::BPCHAROID || type_oid == pg_sys::JSONBOID {
            let cstr = std::ffi::CString::new(s).unwrap();
            let mut typinput: pg_sys::Oid = pg_sys::InvalidOid;
            let mut typioparam: pg_sys::Oid = pg_sys::InvalidOid;
            pg_sys::getTypeInputInfo(type_oid, &mut typinput, &mut typioparam);
            pg_sys::OidInputFunctionCall(typinput, cstr.as_ptr() as *mut _, typioparam, typmod)
        } else {
            // text/varchar: direct varlena construction (avoids type input function lookup)
            let text = pg_sys::cstring_to_text_with_len(s.as_ptr() as *const _, s.len() as i32);
            pg_sys::Datum::from(text as usize)
        }
    }
}

/// Allocate text/varchar datums from string slices using a single contiguous allocation.
///
/// Instead of N individual palloc calls (one per string), this allocates one
/// large block and packs all varlena headers + string data sequentially.
/// This dramatically improves cache locality during the per-row emit loop.
///
/// For bpchar, falls back to per-string allocation (needs type input function for padding).
pub(super) unsafe fn str_slices_to_text_datums_arena(
    slices: &[&str],
    type_oid: pg_sys::Oid,
    typmod: i32,
) -> Vec<pg_sys::Datum> {
    if slices.is_empty() {
        return Vec::new();
    }

    // bpchar needs the input function for padding. jsonb should normally go
    // through `byte_slices_to_jsonb_datums_arena` (the bytes are binary, not
    // UTF-8); this branch is only a safety net for any caller that still
    // hands us text.
    if type_oid == pg_sys::BPCHAROID || type_oid == pg_sys::JSONBOID {
        return unsafe {
            slices
                .iter()
                .map(|s| str_to_text_datum(s, type_oid, typmod))
                .collect()
        };
    }

    unsafe {
        const VARHDRSZ: usize = pg_sys::VARHDRSZ;
        const MAXALIGN: usize = 8; // 64-bit alignment

        // Calculate total arena size
        let total_size: usize = slices
            .iter()
            .map(|s| {
                let varlena_size = VARHDRSZ + s.len();
                // Align each varlena to MAXALIGN for safe pointer access
                (varlena_size + MAXALIGN - 1) & !(MAXALIGN - 1)
            })
            .sum();

        let arena = pg_sys::palloc(total_size) as *mut u8;
        let mut datums = Vec::with_capacity(slices.len());
        let mut offset = 0;

        for s in slices {
            let varlena_ptr = arena.add(offset) as *mut pg_sys::varlena;
            let total_len = (VARHDRSZ + s.len()) as i32;
            pgrx::set_varsize_4b(varlena_ptr, total_len);
            std::ptr::copy_nonoverlapping(
                s.as_ptr(),
                (varlena_ptr as *mut u8).add(VARHDRSZ),
                s.len(),
            );
            datums.push(pg_sys::Datum::from(varlena_ptr as usize));
            offset += ((total_len as usize) + MAXALIGN - 1) & !(MAXALIGN - 1);
        }

        datums
    }
}

/// Build jsonb Datums from raw byte slices already in PG's binary jsonb
/// representation (the payload after `VARDATA_ANY`, i.e. without varlena
/// header). Each slice is wrapped in a fresh short-varlena header and
/// returned as a `pg_sys::Datum`. This is the hot path that replaces the
/// per-row `jsonb_in` parse — parsing happened once at ingest.
///
/// Arena-allocates a single contiguous palloc block for all varlenas to
/// keep cache locality good on large row batches (mirrors
/// `str_slices_to_text_datums_arena`).
pub(super) unsafe fn byte_slices_to_jsonb_datums_arena(
    slices: &[&[u8]],
) -> Vec<pg_sys::Datum> {
    if slices.is_empty() {
        return Vec::new();
    }

    unsafe {
        const VARHDRSZ: usize = pg_sys::VARHDRSZ;
        const MAXALIGN: usize = 8;

        let total_size: usize = slices
            .iter()
            .map(|b| {
                let varlena_size = VARHDRSZ + b.len();
                (varlena_size + MAXALIGN - 1) & !(MAXALIGN - 1)
            })
            .sum();

        let arena = pg_sys::palloc(total_size) as *mut u8;
        let mut datums = Vec::with_capacity(slices.len());
        let mut offset = 0;

        for b in slices {
            let varlena_ptr = arena.add(offset) as *mut pg_sys::varlena;
            let total_len = (VARHDRSZ + b.len()) as i32;
            pgrx::set_varsize_4b(varlena_ptr, total_len);
            std::ptr::copy_nonoverlapping(
                b.as_ptr(),
                (varlena_ptr as *mut u8).add(VARHDRSZ),
                b.len(),
            );
            datums.push(pg_sys::Datum::from(varlena_ptr as usize));
            offset += ((total_len as usize) + MAXALIGN - 1) & !(MAXALIGN - 1);
        }

        datums
    }
}

/// Reinsert nulls into a datum vector using the null bitmap.
pub(super) fn reinsert_nulls_datum(
    datums: &[pg_sys::Datum],
    null_bitmap: &[u8],
    total_count: usize,
) -> Vec<(pg_sys::Datum, bool)> {
    if null_bitmap.is_empty() {
        // Fast path: no nulls — direct copy with pre-allocated Vec
        let mut result = Vec::with_capacity(total_count);
        for &d in datums {
            result.push((d, false));
        }
        return result;
    }
    let mut result = Vec::with_capacity(total_count);
    let mut val_idx = 0;
    for i in 0..total_count {
        let is_null = (null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
        if is_null {
            result.push((pg_sys::Datum::from(0), true));
        } else {
            result.push((datums[val_idx], false));
            val_idx += 1;
        }
    }
    result
}

/// Compare two Datums of the same type. Returns Ordering for min/max computation.
/// Only supports pass-by-value orderable types (int, float, date, timestamp).
pub(super) fn compare_datums(d1: pg_sys::Datum, d2: pg_sys::Datum, type_oid: pg_sys::Oid) -> std::cmp::Ordering {
    if type_oid == pg_sys::TIMESTAMPTZOID || type_oid == pg_sys::TIMESTAMPOID || type_oid == pg_sys::INT8OID {
        (d1.value() as i64).cmp(&(d2.value() as i64))
    } else if type_oid == pg_sys::DATEOID || type_oid == pg_sys::INT4OID {
        (d1.value() as i32).cmp(&(d2.value() as i32))
    } else if type_oid == pg_sys::INT2OID {
        (d1.value() as i16).cmp(&(d2.value() as i16))
    } else if type_oid == pg_sys::FLOAT8OID {
        let f1 = f64::from_bits(d1.value() as u64);
        let f2 = f64::from_bits(d2.value() as u64);
        f1.partial_cmp(&f2).unwrap_or(std::cmp::Ordering::Equal)
    } else if type_oid == pg_sys::FLOAT4OID {
        let f1 = f32::from_bits(d1.value() as u32);
        let f2 = f32::from_bits(d2.value() as u32);
        f1.partial_cmp(&f2).unwrap_or(std::cmp::Ordering::Equal)
    } else {
        std::cmp::Ordering::Equal
    }
}

pub(super) fn count_non_null(null_bitmap: &[u8], total_count: usize) -> usize {
    if null_bitmap.is_empty() {
        return total_count;
    }
    let full_bytes = total_count / 8;
    let mut null_count: usize = null_bitmap[..full_bytes]
        .iter()
        .map(|b| b.count_ones() as usize)
        .sum();
    let remainder = total_count % 8;
    if remainder > 0 {
        let last = null_bitmap[full_bytes];
        let mask = (1u8 << remainder) - 1;
        null_count += (last & mask).count_ones() as usize;
    }
    total_count - null_count
}
