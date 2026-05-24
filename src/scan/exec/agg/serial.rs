//! SERIAL (single-threaded) dispatch — the fall-through path that runs
//! when none of the parallel dispatches fire. Handles every shape the
//! parallel paths reject: COUNT(DISTINCT) with GROUP BY, HAVING, Top-N,
//! LIMIT, regex GROUP BY, mixed-numeric/text predicates, and the
//! generic group-key + AggAccumulator path.
//!
//! Internally branches into:
//! - **COMPACT** sub-path: packed u128 keys + flat byte-buffer
//!   accumulators (when `use_compact_keys && use_compact_accs`).
//! - **GENERIC** sub-path: original `GroupKey` + `AggAccumulator` Vec.
//!
//! Both sub-paths share the result_rows builder + AggScanState
//! epilogue at the end of the function.

use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::time::Instant;

use pgrx::pg_sys;

use super::super::batch_qual::{BatchCompareOp, BatchQual, evaluate_batch_quals};
use super::super::datum_utils::{
    collation_strcmp, count_non_null, decompress_blob_to_datums, decompress_text_blob_to_lengths,
    decompress_text_blob_to_raw_strings, decompress_text_blob_with_eq_filter,
    decompress_text_blob_with_in_filter, decompress_text_blob_with_like_filter, pg_type_name,
    string_to_datum,
};
use super::super::segments::{
    MetadataInfo, SegmentData, detoast_lazy_blobs, segment_skippable_by_dict, take_scan_buf_stats,
};
use super::super::text_col::SegTextColumn;
use super::cd_set::hash128_str;
use super::extract::{constant_extract_key_for_segment, eval_extract};
use super::keys::{CompactGroupMap, pack_int_key_1, pack_int_keys_2, unpack_int_keys};
use super::parallel_mixed::{is_text_group_col, numeric_col_used_only_by_constant_group_keys};
use super::regex::apply_case_when_to_seg_col;
use super::state::{
    AggAccumulator, AggExecSpec, AggExpr, AggScanState, AggType, GroupByColSpec, GroupByExpr,
    GroupKey, GroupKeyRef, GroupKeyVal, GroupMap, HavingFilter, HavingOp, OutputEntry,
    hash_group_key, hash_group_key_ref, keys_match,
};
use super::{
    CompactAccKind, CompactAccStorage, CountDistinctSideCar, StringArena, compact_finalize,
    compact_topn_select, datum_to_f64, datum_to_i128, finalize_accumulator, i128_to_numeric_datum,
};
use crate::compression;

/// Per-RegexpReplace GROUP BY column: cached PG datums for pattern + replacement,
/// plus the column index. Built once at dispatch entry, used per-segment.
pub(super) struct RegexpGroupInfo {
    pub(super) group_idx: usize,
    pub(super) func_oid: pg_sys::Oid,
    pub(super) collation: pg_sys::Oid,
    pub(super) pattern_datum: pg_sys::Datum,
    pub(super) replacement_datum: pg_sys::Datum,
}

/// Serial dispatch — the fall-through that runs when no parallel
/// dispatch fires. Caller boxes the returned `AggScanState` and
/// assigns to `(*node).custom_ps`.
///
/// SAFETY: calls PG FFI (`detoast_lazy_blobs`,
/// `cstring_to_text_with_len`, `MemoryContextDelete`, etc.). Must run
/// inside an active PG transaction (`BeginCustomScan` invariant).
#[allow(clippy::too_many_arguments)]
pub(super) unsafe fn dispatch_serial_path(
    node: *mut pg_sys::CustomScanState,
    agg_specs: Vec<AggExecSpec>,
    group_specs: Vec<GroupByColSpec>,
    output_map: Vec<OutputEntry>,
    having_filters: Vec<HavingFilter>,
    where_quals: *mut pg_sys::List,
    topn_limit: i64,
    topn_sort_col: usize,
    topn_ascending: bool,
    bare_limit: i64,
    _derived_minmax_topn: Option<(usize, usize)>,
    meta: &MetadataInfo,
    all_segments: &mut [SegmentData],
    needed_cols: &[bool],
    batch_quals: &[BatchQual],
    seg_filters: &[(usize, String)],
    time_min: Option<i64>,
    time_max: Option<i64>,
    use_lazy: bool,
    num_result_cols: usize,
    metadata_us: u64,
    heap_scan_us: u64,
    t_wall: Instant,
    has_group_by: bool,
    is_single_group_key: bool,
    use_compact_accs: bool,
    use_compact_keys: bool,
    has_regexp_group: bool,
    text_group_cols: Vec<bool>,
    length_cols: Vec<bool>,
    _sidecar_only_cols: Vec<bool>,
    count_distinct_only_str: Vec<bool>,
    count_distinct_only_int: Vec<bool>,
    mut compact_storage: Option<CompactAccStorage>,
    mut total_detoast_us: u64,
    mut total_cache_hits: u64,
    mut total_cache_misses: u64,
    mut total_cache_bytes_served: u64,
) -> AggScanState {
    unsafe {
        let query_ctx = (*(*node).ss.ps.state).es_query_cxt;
        let segment_mcxt = pg_sys::AllocSetContextCreateInternal(
            query_ctx,
            c"DeltaXAggSegment".as_ptr(),
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        );

        let n_agg_specs = agg_specs.len();
        let _ = n_agg_specs;
        let prototype_accumulators: Vec<AggAccumulator> = agg_specs
            .iter()
            .map(|spec| AggAccumulator::new_for(spec.agg_type, spec.col_type_oid))
            .collect();
        let mut global_accumulators = if !has_group_by {
            Some(
                prototype_accumulators
                    .iter()
                    .map(|a| a.clone_fresh())
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };
        let mut group_map: GroupMap = GroupMap::with_hasher(BuildHasherDefault::default());
        let mut string_arena = StringArena::new();
        let mut flat_accs: Vec<AggAccumulator> = Vec::new();
        let mut compact_group_map: CompactGroupMap =
            CompactGroupMap::with_hasher(BuildHasherDefault::default());
        let mut cd_sidecar = CountDistinctSideCar::new(&agg_specs);

        // Regex-replace GROUP BY: build PG datums for pattern/replacement once.
        let mut regex_cache: HashMap<String, String> = HashMap::new();
        let mut regex_cache_calls: u64 = 0;
        let mut regexp_group_infos: Vec<RegexpGroupInfo> = Vec::new();
        let mut raw_string_cols: Vec<bool> = vec![false; meta.col_names.len()];
        if has_regexp_group {
            for (gi, gs) in group_specs.iter().enumerate() {
                if let GroupByExpr::RegexpReplace {
                    ref pattern,
                    ref replacement,
                    func_oid,
                    collation,
                } = gs.expr
                {
                    raw_string_cols[gs.col_idx as usize] = true;
                    let pattern_datum = {
                        let text = pg_sys::cstring_to_text_with_len(
                            pattern.as_ptr() as *const _,
                            pattern.len() as i32,
                        );
                        pg_sys::Datum::from(text as usize)
                    };
                    let replacement_datum = {
                        let text = pg_sys::cstring_to_text_with_len(
                            replacement.as_ptr() as *const _,
                            replacement.len() as i32,
                        );
                        pg_sys::Datum::from(text as usize)
                    };
                    regexp_group_infos.push(RegexpGroupInfo {
                        group_idx: gi,
                        func_oid: pg_sys::Oid::from(func_oid),
                        collation: pg_sys::Oid::from(collation),
                        pattern_datum,
                        replacement_datum,
                    });
                }
            }
        }
        if use_compact_accs {
            for spec in &agg_specs {
                if matches!(spec.agg_type, AggType::Min | AggType::Max) {
                    let t = spec.col_type_oid;
                    if t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID {
                        raw_string_cols[spec.col_idx as usize] = true;
                    }
                }
            }
        }
        let mut seg_text_columns: Vec<Option<SegTextColumn>> = Vec::new();

        // ============================================================
        // SINGLE-THREADED PATH (original)
        // ============================================================
        // If lazy loading was used (parallel was possible but conditions
        // weren't met for compact/mixed), detoast all segments now.
        if use_lazy {
            let t_detoast = Instant::now();
            for seg in all_segments.iter_mut() {
                let dl = detoast_lazy_blobs(seg);
                total_cache_hits += dl.cache_hits;
                total_cache_misses += dl.cache_misses;
                total_cache_bytes_served += dl.cache_bytes_served;
            }
            total_detoast_us += t_detoast.elapsed().as_micros() as u64;
        }
        let t2 = Instant::now();
        let mut total_segments: u64 = 0;
        let mut total_rows_processed: u64 = 0;
        let mut decompress_us: u64 = 0;

        for seg in all_segments.iter() {
            if seg.row_count == 0 {
                continue;
            }

            // Segment-by pruning
            if !seg_filters.is_empty() {
                let mut skip = false;
                for &(seg_val_idx, ref filter_val) in seg_filters {
                    match &seg.segment_values[seg_val_idx] {
                        Some(val) if val == filter_val => {}
                        _ => {
                            skip = true;
                            break;
                        }
                    }
                }
                if skip {
                    continue;
                }
            }

            // Time-range pruning
            if let (Some(seg_min), Some(seg_max)) = (seg.min_time, seg.max_time) {
                if time_min.is_some_and(|query_min| seg_max < query_min) {
                    continue;
                }
                if time_max.is_some_and(|query_max| seg_min > query_max) {
                    continue;
                }
            }

            // Dictionary-based LIKE pruning: skip segment if no dict entry matches
            if segment_skippable_by_dict(batch_quals, &meta.blob_idx, &seg.compressed_blobs) {
                continue;
            }

            total_segments += 1;

            let mut const_group_keys: Vec<Option<i64>> = vec![None; group_specs.len()];
            for (gi, gs) in group_specs.iter().enumerate() {
                if is_text_group_col(gs) {
                    continue;
                }
                let GroupByExpr::Extract { unit, divisor, .. } = &gs.expr else {
                    continue;
                };
                let col_idx = gs.col_idx as usize;
                let Some(col_name) = meta.col_names.get(col_idx) else {
                    continue;
                };
                let Some(cm) = seg.col_minmax.get(col_name) else {
                    continue;
                };
                const_group_keys[gi] = constant_extract_key_for_segment(cm, *divisor, unit);
            }

            let skip_numeric_decompress: Vec<bool> = (0..meta.col_names.len())
                .map(|col_idx| {
                    numeric_col_used_only_by_constant_group_keys(
                        col_idx,
                        &group_specs,
                        &const_group_keys,
                        batch_quals,
                        &agg_specs,
                    )
                })
                .collect();

            // Decompress needed columns
            let t_dec = Instant::now();
            pg_sys::MemoryContextReset(segment_mcxt);
            let old_ctx = pg_sys::MemoryContextSwitchTo(segment_mcxt);

            let mut decompressed: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
            // Raw strings for columns that need regexp_replace (parallel to decompressed)
            let mut raw_strings: Vec<Option<Vec<Option<String>>>> = Vec::new();
            let mut seg_val_idx = 0;
            let mut pre_selection: Vec<bool> = Vec::new();

            for (col_idx, col_name) in meta.col_names.iter().enumerate() {
                let type_oid = meta.col_types[col_idx];
                let is_segment_by = meta.segment_by.contains(col_name);
                let blob_slot: Option<usize> = meta
                    .blob_idx
                    .get(col_idx)
                    .copied()
                    .flatten()
                    .map(|s| s as usize);

                if !needed_cols[col_idx] {
                    if is_segment_by {
                        seg_val_idx += 1;
                    }
                    decompressed.push(Vec::new());
                    raw_strings.push(None);
                    continue;
                }

                if is_segment_by {
                    let val = &seg.segment_values[seg_val_idx];
                    let (datum, is_null) = match val {
                        Some(s) => (string_to_datum(s, type_oid), false),
                        None => (pg_sys::Datum::from(0), true),
                    };
                    let repeated: Vec<(pg_sys::Datum, bool)> =
                        (0..seg.row_count).map(|_| (datum, is_null)).collect();
                    decompressed.push(repeated);
                    raw_strings.push(None);
                    seg_val_idx += 1;
                } else if blob_slot.is_none() {
                    // Column added after this partition was compressed —
                    // synthesize from `meta.missing_values`.
                    let (datum, is_null) = meta
                        .missing_values
                        .get(col_idx)
                        .copied()
                        .flatten()
                        .unwrap_or((pg_sys::Datum::from(0), true));
                    let repeated: Vec<(pg_sys::Datum, bool)> =
                        (0..seg.row_count).map(|_| (datum, is_null)).collect();
                    decompressed.push(repeated);
                    raw_strings.push(None);
                } else {
                    let blob = &seg.compressed_blobs[blob_slot.unwrap()];
                    let typmod = meta.col_typmods[col_idx];

                    if skip_numeric_decompress[col_idx] {
                        decompressed.push(Vec::new());
                        raw_strings.push(None);
                        continue;
                    }

                    // Fast path: COUNT(DISTINCT) on text without GROUP BY or
                    // row-level WHERE — hash directly from compressed data,
                    // skipping all datum conversion.
                    if count_distinct_only_str[col_idx] && !has_group_by && batch_quals.is_empty() {
                        let cc_ref = compression::CompressedColumnRef::from_bytes(blob);
                        let accumulators = global_accumulators.as_mut().unwrap();
                        // Find the CountDistinctStr accumulator for this column
                        for (spec_idx, spec) in agg_specs.iter().enumerate() {
                            if spec.col_idx as usize == col_idx {
                                if let AggAccumulator::CountDistinctStr { seen } =
                                    &mut accumulators[spec_idx]
                                {
                                    let non_null_count = count_non_null(
                                        cc_ref.null_bitmap,
                                        cc_ref.row_count as usize,
                                    );
                                    match cc_ref.type_tag {
                                        compression::CompressionType::Dictionary
                                        | compression::CompressionType::DictionaryLz4 => {
                                            // Dict shortcut: hash only the dict entries — O(dict_size)
                                            let norm_buf;
                                            let dict_data = if cc_ref.type_tag == compression::CompressionType::DictionaryLz4 {
                                                norm_buf = compression::dictionary::normalize_lz4(cc_ref.data);
                                                &norm_buf[..]
                                            } else {
                                                cc_ref.data
                                            };
                                            let hdr = compression::dictionary::parse_header(dict_data);
                                            for entry in &hdr.dict {
                                                seen.insert(hash128_str(entry.as_bytes()));
                                            }
                                        }
                                        compression::CompressionType::Lz4 => {
                                            let (buf, ranges) = compression::lz4::decode_to_ranges(cc_ref.data, non_null_count);
                                            let empty_hash = hash128_str(b"");
                                            let mut has_empty = false;
                                            for &(off, len) in &ranges {
                                                if len == 0 {
                                                    has_empty = true;
                                                } else {
                                                    seen.insert(hash128_str(&buf[off..off + len]));
                                                }
                                            }
                                            if has_empty { seen.insert(empty_hash); }
                                        }
                                        compression::CompressionType::Lz4Blocked => {
                                            let (buf, ranges) = compression::lz4::decode_to_ranges_blocked(cc_ref.data, non_null_count, None);
                                            let empty_hash = hash128_str(b"");
                                            let mut has_empty = false;
                                            for &(off, len) in &ranges {
                                                if len == 0 {
                                                    has_empty = true;
                                                } else {
                                                    seen.insert(hash128_str(&buf[off..off + len]));
                                                }
                                            }
                                            if has_empty { seen.insert(empty_hash); }
                                        }
                                        compression::CompressionType::Constant
                                            // Single constant string — hash the raw bytes
                                            if non_null_count > 0 => {
                                                seen.insert(hash128_str(cc_ref.data));
                                            }
                                        _ => {}
                                    }
                                }
                                break;
                            }
                        }
                        // Push empty so the row loop skips this column
                        decompressed.push(Vec::new());
                        raw_strings.push(None);
                        continue;
                    }

                    // Fast path: COUNT(DISTINCT) on integer without GROUP BY or
                    // row-level WHERE — decode directly and insert into HashSet,
                    // skipping all datum conversion.
                    if count_distinct_only_int[col_idx] && !has_group_by && batch_quals.is_empty() {
                        let cc_ref = compression::CompressedColumnRef::from_bytes(blob);
                        let accumulators = global_accumulators.as_mut().unwrap();
                        for (spec_idx, spec) in agg_specs.iter().enumerate() {
                            if spec.col_idx as usize == col_idx {
                                if let AggAccumulator::CountDistinctInt { seen } =
                                    &mut accumulators[spec_idx]
                                {
                                    let non_null_count = count_non_null(
                                        cc_ref.null_bitmap,
                                        cc_ref.row_count as usize,
                                    );
                                    if non_null_count > 0 {
                                        let is_i64 = type_oid == pg_sys::INT8OID;
                                        match cc_ref.type_tag {
                                            compression::CompressionType::Constant => {
                                                if is_i64 {
                                                    let v = i64::from_le_bytes(
                                                        cc_ref.data[..8].try_into().unwrap(),
                                                    );
                                                    seen.insert(v);
                                                } else {
                                                    let v = i32::from_le_bytes(
                                                        cc_ref.data[..4].try_into().unwrap(),
                                                    );
                                                    seen.insert(v as i64);
                                                }
                                            }
                                            compression::CompressionType::ForBitpacked => {
                                                if is_i64 {
                                                    let vals =
                                                        compression::bitpacked::decode_for_i64(
                                                            cc_ref.data,
                                                            non_null_count,
                                                        );
                                                    for v in vals {
                                                        seen.insert(v);
                                                    }
                                                } else {
                                                    let vals =
                                                        compression::bitpacked::decode_for_i32(
                                                            cc_ref.data,
                                                            non_null_count,
                                                        );
                                                    for v in vals {
                                                        seen.insert(v as i64);
                                                    }
                                                }
                                            }
                                            compression::CompressionType::DeltaVarint => {
                                                if is_i64 {
                                                    let vals = compression::integer::decode_i64(
                                                        cc_ref.data,
                                                        non_null_count,
                                                    );
                                                    for v in vals {
                                                        seen.insert(v);
                                                    }
                                                } else {
                                                    let vals = compression::integer::decode_i32(
                                                        cc_ref.data,
                                                        non_null_count,
                                                    );
                                                    for v in vals {
                                                        seen.insert(v as i64);
                                                    }
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                break;
                            }
                        }
                        // Push empty so the row loop skips this column
                        decompressed.push(Vec::new());
                        raw_strings.push(None);
                        continue;
                    }

                    if raw_string_cols[col_idx] {
                        // Dictionary-optimized path: pre-warm regex cache from dict entries only
                        let cc_ref = compression::CompressedColumnRef::from_bytes(blob);
                        if cc_ref.type_tag == compression::CompressionType::Dictionary
                            || cc_ref.type_tag == compression::CompressionType::DictionaryLz4
                        {
                            let total_count = cc_ref.row_count as usize;
                            let non_null_count = count_non_null(cc_ref.null_bitmap, total_count);
                            let norm_buf;
                            let dict_data =
                                if cc_ref.type_tag == compression::CompressionType::DictionaryLz4 {
                                    norm_buf = compression::dictionary::normalize_lz4(cc_ref.data);
                                    &norm_buf[..]
                                } else {
                                    cc_ref.data
                                };
                            let (dict_entries, indices) =
                                compression::dictionary::decode_dict_and_indices(
                                    dict_data,
                                    non_null_count,
                                );

                            // Pre-warm regex cache from dict entries only — O(dict_size) calls
                            for &entry in &dict_entries {
                                let key = entry.to_string();
                                if !regex_cache.contains_key(&key) {
                                    for rgi in &regexp_group_infos {
                                        if group_specs[rgi.group_idx].col_idx as usize == col_idx {
                                            regex_cache_calls += 1;
                                            let input_datum = {
                                                let text = pg_sys::cstring_to_text_with_len(
                                                    entry.as_ptr() as *const _,
                                                    entry.len() as i32,
                                                );
                                                pg_sys::Datum::from(text as usize)
                                            };
                                            let result_datum = pg_sys::OidFunctionCall3Coll(
                                                rgi.func_oid,
                                                rgi.collation,
                                                input_datum,
                                                rgi.pattern_datum,
                                                rgi.replacement_datum,
                                            );
                                            let cstr = pg_sys::text_to_cstring(
                                                result_datum.cast_mut_ptr(),
                                            );
                                            let s = std::ffi::CStr::from_ptr(cstr)
                                                .to_string_lossy()
                                                .into_owned();
                                            pg_sys::pfree(cstr as *mut _);
                                            regex_cache.insert(key.clone(), s);
                                            break;
                                        }
                                    }
                                }
                            }

                            // Build per-row strings from cached regex results via dict index
                            let has_ne_empty = batch_quals.iter().any(|bq| {
                                bq.col_idx == col_idx
                                    && bq.text_const.as_deref() == Some("")
                                    && bq.op == BatchCompareOp::Ne
                            });
                            let ne_sel = if has_ne_empty {
                                compression::dictionary::check_ne_empty(dict_data, non_null_count)
                            } else {
                                Vec::new()
                            };

                            let nn_strings: Vec<String> = indices
                                .iter()
                                .map(|&idx| dict_entries[idx as usize].to_string())
                                .collect();

                            // Reinsert nulls
                            if cc_ref.null_bitmap.is_empty() {
                                let strings: Vec<Option<String>> =
                                    nn_strings.into_iter().map(Some).collect();
                                let datums: Vec<(pg_sys::Datum, bool)> = strings
                                    .iter()
                                    .map(|s| match s {
                                        Some(_) => (pg_sys::Datum::from(0usize), false),
                                        None => (pg_sys::Datum::from(0usize), true),
                                    })
                                    .collect();
                                decompressed.push(datums);
                                raw_strings.push(Some(strings));
                                if !ne_sel.is_empty() {
                                    if pre_selection.is_empty() {
                                        pre_selection = ne_sel;
                                    } else {
                                        for (ps, s) in pre_selection.iter_mut().zip(ne_sel.iter()) {
                                            *ps = *ps && *s;
                                        }
                                    }
                                }
                            } else {
                                let mut strings = Vec::with_capacity(total_count);
                                let mut sel = if has_ne_empty {
                                    Vec::with_capacity(total_count)
                                } else {
                                    Vec::new()
                                };
                                let mut val_idx = 0;
                                for i in 0..total_count {
                                    let is_null = (cc_ref.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
                                    if is_null {
                                        strings.push(None);
                                        if has_ne_empty {
                                            sel.push(false);
                                        }
                                    } else {
                                        strings.push(Some(nn_strings[val_idx].clone()));
                                        if has_ne_empty && !ne_sel.is_empty() {
                                            sel.push(ne_sel[val_idx]);
                                        } else if has_ne_empty {
                                            sel.push(true);
                                        }
                                        val_idx += 1;
                                    }
                                }
                                let datums: Vec<(pg_sys::Datum, bool)> = strings
                                    .iter()
                                    .map(|s| match s {
                                        Some(_) => (pg_sys::Datum::from(0usize), false),
                                        None => (pg_sys::Datum::from(0usize), true),
                                    })
                                    .collect();
                                decompressed.push(datums);
                                raw_strings.push(Some(strings));
                                if !sel.is_empty() {
                                    if pre_selection.is_empty() {
                                        pre_selection = sel;
                                    } else {
                                        for (ps, s) in pre_selection.iter_mut().zip(sel.iter()) {
                                            *ps = *ps && *s;
                                        }
                                    }
                                }
                            }
                        } else {
                            // Non-dictionary: fall back to existing path
                            let (strings, sel) =
                                decompress_text_blob_to_raw_strings(blob, batch_quals, col_idx);
                            let datums: Vec<(pg_sys::Datum, bool)> = strings
                                .iter()
                                .map(|s| match s {
                                    Some(_) => (pg_sys::Datum::from(0usize), false),
                                    None => (pg_sys::Datum::from(0usize), true),
                                })
                                .collect();
                            decompressed.push(datums);
                            raw_strings.push(Some(strings));
                            if !sel.is_empty() {
                                if pre_selection.is_empty() {
                                    pre_selection = sel;
                                } else {
                                    for (ps, s) in pre_selection.iter_mut().zip(sel.iter()) {
                                        *ps = *ps && *s;
                                    }
                                }
                            }
                        }
                    } else if length_cols[col_idx] {
                        // Length-only column: decompress as int4 lengths.
                        let has_ne_empty = batch_quals.iter().any(|bq| {
                            bq.col_idx == col_idx
                                && bq.text_const.as_deref() == Some("")
                                && bq.op == BatchCompareOp::Ne
                        });
                        let (datums, len_sel) = decompress_text_blob_to_lengths(blob, has_ne_empty);
                        decompressed.push(datums);
                        raw_strings.push(None);
                        if !len_sel.is_empty() {
                            if pre_selection.is_empty() {
                                pre_selection = len_sel;
                            } else {
                                for (ps, ls) in pre_selection.iter_mut().zip(len_sel.iter()) {
                                    *ps = *ps && *ls;
                                }
                            }
                        }
                    } else {
                        // Normal text/non-text column
                        let like_qual = batch_quals.iter().find(|bq| {
                            bq.col_idx == col_idx
                                && matches!(bq.op, BatchCompareOp::Like | BatchCompareOp::NotLike)
                        });

                        let text_eq_qual = batch_quals.iter().find(|bq| {
                            bq.col_idx == col_idx
                                && bq.text_const.is_some()
                                && matches!(bq.op, BatchCompareOp::Eq | BatchCompareOp::Ne)
                        });
                        let text_in_qual = batch_quals.iter().find(|bq| {
                            bq.col_idx == col_idx
                                && bq.in_list_text.is_some()
                                && bq.op == BatchCompareOp::InList
                        });

                        if let Some(bq) = like_qual {
                            let strat = bq.like_strategy.as_ref().unwrap();
                            let neg = bq.op == BatchCompareOp::NotLike;
                            let (datums, like_sel) = decompress_text_blob_with_like_filter(
                                blob, type_oid, typmod, strat, neg, None,
                            );
                            decompressed.push(datums);
                            if pre_selection.is_empty() {
                                pre_selection = like_sel;
                            } else {
                                for (ps, ls) in pre_selection.iter_mut().zip(like_sel.iter()) {
                                    *ps = *ps && *ls;
                                }
                            }
                        } else if let Some(bq) = text_eq_qual {
                            let const_str = bq.text_const.as_ref().unwrap();
                            let is_ne = bq.op == BatchCompareOp::Ne;
                            let (datums, eq_sel) = decompress_text_blob_with_eq_filter(
                                blob, type_oid, typmod, const_str, is_ne, None,
                            );
                            decompressed.push(datums);
                            if pre_selection.is_empty() {
                                pre_selection = eq_sel;
                            } else {
                                for (ps, es) in pre_selection.iter_mut().zip(eq_sel.iter()) {
                                    *ps = *ps && *es;
                                }
                            }
                        } else if let Some(bq) = text_in_qual {
                            let strs = bq.in_list_text.as_ref().unwrap();
                            let (datums, in_sel) = decompress_text_blob_with_in_filter(
                                blob, type_oid, typmod, strs, /* is_not_in */ false, None,
                            );
                            decompressed.push(datums);
                            if pre_selection.is_empty() {
                                pre_selection = in_sel;
                            } else {
                                for (ps, is_) in pre_selection.iter_mut().zip(in_sel.iter()) {
                                    *ps = *ps && *is_;
                                }
                            }
                        } else {
                            let type_name = pg_type_name(type_oid);
                            let datums =
                                decompress_blob_to_datums(blob, &type_name, type_oid, typmod);
                            decompressed.push(datums);
                        }
                        raw_strings.push(None);
                    }
                }
            }

            pg_sys::MemoryContextSwitchTo(old_ctx);
            decompress_us += t_dec.elapsed().as_micros() as u64;

            let row_count = seg.row_count as usize;

            // Extract text GROUP BY info: intern strings and build per-row u32 ID vectors.
            // Handles both dictionary-encoded and LZ4-encoded text columns.
            // Build per-segment text column data for GROUP BY.
            // Keeps decoded string data alive during the row loop for O(1) &str access.
            seg_text_columns.clear();
            seg_text_columns.resize_with(meta.col_names.len(), || None);
            {
                let mut seg_val_idx2 = 0;
                for (col_idx, col_name) in meta.col_names.iter().enumerate() {
                    if meta.segment_by.contains(col_name) {
                        if needed_cols[col_idx] && text_group_cols[col_idx] {
                            let val = &seg.segment_values[seg_val_idx2];
                            seg_text_columns[col_idx] = Some(SegTextColumn::SegBy(val.clone()));
                        }
                        seg_val_idx2 += 1;
                        continue;
                    }
                    // Skip columns added to the parent after this partition
                    // was compressed: there's no blob to decode into a text
                    // column. (Missing-value synthesis for non-text columns
                    // happens in the main decompress loop above.)
                    let blob_slot: Option<usize> = meta
                        .blob_idx
                        .get(col_idx)
                        .copied()
                        .flatten()
                        .map(|s| s as usize);
                    let Some(blob_idx2) = blob_slot else {
                        continue;
                    };
                    if needed_cols[col_idx] && text_group_cols[col_idx] {
                        let blob = &seg.compressed_blobs[blob_idx2];
                        if !blob.is_empty() {
                            let cc_ref = compression::CompressedColumnRef::from_bytes(blob);
                            let total = cc_ref.row_count as usize;
                            let nn_count = count_non_null(cc_ref.null_bitmap, total);

                            let seg_col = match cc_ref.type_tag {
                                compression::CompressionType::Dictionary
                                | compression::CompressionType::DictionaryLz4 => {
                                    let norm_buf;
                                    let dict_data = if cc_ref.type_tag
                                        == compression::CompressionType::DictionaryLz4
                                    {
                                        norm_buf =
                                            compression::dictionary::normalize_lz4(cc_ref.data);
                                        &norm_buf[..]
                                    } else {
                                        cc_ref.data
                                    };
                                    let (dict_entries, nn_indices) =
                                        compression::dictionary::decode_dict_and_indices(
                                            dict_data, nn_count,
                                        );
                                    let entries: Vec<String> =
                                        dict_entries.iter().map(|&s| s.to_string()).collect();

                                    // Expand nn_indices to full-row indices (u32::MAX for nulls)
                                    let row_to_entry = if cc_ref.null_bitmap.is_empty() {
                                        nn_indices.iter().map(|&idx| idx as u32).collect()
                                    } else {
                                        let mut re = Vec::with_capacity(total);
                                        let mut vi = 0;
                                        for i in 0..total {
                                            let is_null =
                                                (cc_ref.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
                                            if is_null {
                                                re.push(u32::MAX);
                                            } else {
                                                re.push(nn_indices[vi] as u32);
                                                vi += 1;
                                            }
                                        }
                                        re
                                    };
                                    SegTextColumn::Dict {
                                        entries,
                                        row_to_entry,
                                    }
                                }
                                compression::CompressionType::Lz4
                                | compression::CompressionType::Lz4Blocked => {
                                    let (buf, ranges) = if cc_ref.type_tag
                                        == compression::CompressionType::Lz4
                                    {
                                        compression::lz4::decode_to_ranges(cc_ref.data, nn_count)
                                    } else {
                                        compression::lz4::decode_to_ranges_blocked(
                                            cc_ref.data,
                                            nn_count,
                                            None,
                                        )
                                    };

                                    // Expand ranges to full-row ranges (u32::MAX for nulls)
                                    let row_to_range = if cc_ref.null_bitmap.is_empty() {
                                        ranges
                                            .iter()
                                            .map(|&(off, len)| (off as u32, len as u16))
                                            .collect()
                                    } else {
                                        let mut rr = Vec::with_capacity(total);
                                        let mut vi = 0;
                                        for i in 0..total {
                                            let is_null =
                                                (cc_ref.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
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
                                    SegTextColumn::Lz4 { buf, row_to_range }
                                }
                                _ => continue,
                            };
                            seg_text_columns[col_idx] = Some(seg_col);
                        }
                    }
                }
            }

            // Evaluate batch quals (WHERE) if any.
            // pre_selection seeds the selection vector so that rows already
            // filtered by LIKE during decompression are skipped (their dummy
            // datums are never dereferenced).
            let selection =
                evaluate_batch_quals(&decompressed, row_count, batch_quals, pre_selection);

            // Pre-compute CaseWhen GROUP BY columns into SegTextColumn
            let case_when_seg_cols: Vec<Option<SegTextColumn>> = group_specs
                .iter()
                .map(|gs| {
                    if let GroupByExpr::CaseWhen(ref spec) = gs.expr {
                        Some(apply_case_when_to_seg_col(
                            spec,
                            &decompressed,
                            &seg_text_columns,
                            row_count,
                            &selection,
                        ))
                    } else {
                        None
                    }
                })
                .collect();

            // Fast path: when no GROUP BY and all agg specs are SUM/AVG on the
            // same column with Column or AddConst expr, compute base_sum once
            // and derive each result as base_sum + const_offset * non_null_count.
            // This turns O(N * num_aggs) into O(N + num_aggs).
            if !has_group_by && agg_specs.len() > 1 {
                let first_col = agg_specs[0].col_idx;
                let first_type = agg_specs[0].col_type_oid;
                let all_same_col_sum = agg_specs.iter().all(|s| {
                    s.col_idx == first_col
                        && (s.agg_type == AggType::Sum || s.agg_type == AggType::Avg)
                        && (s.expr_kind == AggExpr::Column || s.expr_kind == AggExpr::AddConst)
                });
                if all_same_col_sum {
                    let col = &decompressed[first_col as usize];
                    if !col.is_empty() {
                        let accumulators = global_accumulators.as_mut().unwrap();
                        let mut base_sum: i128 = 0;
                        let mut non_null_count: i64 = 0;
                        let use_float = matches!(first_type, pg_sys::FLOAT4OID | pg_sys::FLOAT8OID);
                        let mut base_sum_f: f64 = 0.0;
                        for row in 0..row_count {
                            if !selection.is_empty() && !selection[row] {
                                continue;
                            }
                            if !col[row].1 {
                                if use_float {
                                    base_sum_f += datum_to_f64(col[row].0, first_type);
                                } else {
                                    base_sum += datum_to_i128(col[row].0, first_type);
                                }
                                non_null_count += 1;
                            }
                        }
                        total_rows_processed += if selection.is_empty() {
                            row_count as u64
                        } else {
                            selection.iter().filter(|&&v| v).count() as u64
                        };
                        for (spec_idx, spec) in agg_specs.iter().enumerate() {
                            let acc = &mut accumulators[spec_idx];
                            if use_float {
                                if let AggAccumulator::SumFloat { sum, count } = acc {
                                    *sum += base_sum_f
                                        + spec.const_offset as f64 * non_null_count as f64;
                                    *count += non_null_count;
                                }
                            } else {
                                if let AggAccumulator::SumInt { sum, count } = acc {
                                    *sum += base_sum
                                        + spec.const_offset as i128 * non_null_count as i128;
                                    *count += non_null_count;
                                }
                            }
                        }
                        continue; // skip the generic aggregate loop for this segment
                    }
                }
            }

            // ============================================================
            // COMPACT PATH: packed u128 keys + flat byte-buffer accumulators
            // GENERIC PATH: original GroupKey + AggAccumulator
            // ============================================================
            if use_compact_keys && use_compact_accs {
                serial_compact_row_loop(
                    &agg_specs,
                    &group_specs,
                    &decompressed,
                    &raw_strings,
                    &selection,
                    row_count,
                    compact_storage.as_mut().unwrap(),
                    &mut compact_group_map,
                    &mut cd_sidecar,
                    &mut total_rows_processed,
                );
            } else {
                serial_generic_row_loop(
                    &agg_specs,
                    &group_specs,
                    &prototype_accumulators,
                    &regexp_group_infos,
                    &raw_string_cols,
                    &const_group_keys,
                    &decompressed,
                    &raw_strings,
                    &seg_text_columns,
                    &case_when_seg_cols,
                    &selection,
                    row_count,
                    has_group_by,
                    has_regexp_group,
                    is_single_group_key,
                    &mut group_map,
                    &mut flat_accs,
                    &mut string_arena,
                    &mut global_accumulators,
                    &mut regex_cache,
                    &mut regex_cache_calls,
                    &mut total_rows_processed,
                );
            }
        }

        let agg_us = t2.elapsed().as_micros() as u64 - decompress_us;

        // Write CountDistinct counts from sidecar into compact storage
        if use_compact_keys && use_compact_accs {
            cd_sidecar
                .write_counts_to_storage(compact_storage.as_mut().unwrap(), &compact_group_map);
        }

        // Finalize results using output mapping, applying HAVING filters
        let mut topn_select_us: u64 = 0;
        let t_finalize = Instant::now();
        let mut result_rows = if use_compact_keys
            && use_compact_accs
            && topn_limit > 0
            && having_filters.is_empty()
            && compact_group_map.len() > topn_limit as usize
        {
            // Top-N pushdown: heap-select top-N by raw sort value, finalize only those
            let sort_slot = match output_map[topn_sort_col] {
                OutputEntry::Agg(ai) => ai,
                _ => unreachable!(),
            };
            let storage = compact_storage.as_ref().unwrap();
            let t_topn = Instant::now();
            let top_entries = compact_topn_select(
                &compact_group_map,
                storage,
                sort_slot,
                topn_limit as usize,
                topn_ascending,
                agg_specs[sort_slot].agg_type == AggType::Avg,
            );
            topn_select_us = t_topn.elapsed().as_micros() as u64;
            let num_group_keys = group_specs.len();
            let mut rows = Vec::with_capacity(top_entries.len());
            for &(packed_key, group_idx) in &top_entries {
                let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                for (spec_idx, spec) in agg_specs.iter().enumerate() {
                    agg_results.push(compact_finalize(storage, group_idx, spec_idx, spec));
                }
                let keys = unpack_int_keys(packed_key, num_group_keys);
                let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                for entry in &output_map {
                    match entry {
                        OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                        OutputEntry::Group(gi) => {
                            let v = keys[*gi];
                            if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. }) {
                                row.push((i128_to_numeric_datum(v as i128), false));
                            } else {
                                row.push((pg_sys::Datum::from(v as usize), false));
                            }
                        }
                        OutputEntry::DerivedGroup { base_gi, delta } => {
                            let v = keys[*base_gi] + delta;
                            row.push((pg_sys::Datum::from(v as usize), false));
                        }
                        OutputEntry::Const(d, n) => row.push((*d, *n)),
                    }
                }
                rows.push(row);
            }
            rows
        } else if use_compact_keys && use_compact_accs {
            // Full compact finalization (no top-N pushdown, or HAVING present)
            let storage = compact_storage.as_ref().unwrap();
            let num_group_keys = group_specs.len();
            let mut rows = Vec::new();
            'compact_group_loop: for (&packed_key, &group_idx) in &compact_group_map {
                let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                for (spec_idx, spec) in agg_specs.iter().enumerate() {
                    agg_results.push(compact_finalize(storage, group_idx, spec_idx, spec));
                }

                // Apply HAVING filters
                for hf in &having_filters {
                    let (datum, is_null) = agg_results[hf.agg_idx];
                    if is_null {
                        continue 'compact_group_loop;
                    }
                    let val = datum.value() as i64;
                    let pass = match hf.op {
                        HavingOp::Gt => val > hf.const_val,
                        HavingOp::Lt => val < hf.const_val,
                        HavingOp::Ge => val >= hf.const_val,
                        HavingOp::Le => val <= hf.const_val,
                        HavingOp::Eq => val == hf.const_val,
                        HavingOp::Ne => val != hf.const_val,
                    };
                    if !pass {
                        continue 'compact_group_loop;
                    }
                }

                // Unpack keys back to i64 datums
                let keys = unpack_int_keys(packed_key, num_group_keys);
                let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                for entry in &output_map {
                    match entry {
                        OutputEntry::Agg(ai) => {
                            row.push(agg_results[*ai]);
                        }
                        OutputEntry::Group(gi) => {
                            let v = keys[*gi];
                            if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. }) {
                                row.push((i128_to_numeric_datum(v as i128), false));
                            } else {
                                row.push((pg_sys::Datum::from(v as usize), false));
                            }
                        }
                        OutputEntry::DerivedGroup { base_gi, delta } => {
                            let v = keys[*base_gi] + delta;
                            row.push((pg_sys::Datum::from(v as usize), false));
                        }
                        OutputEntry::Const(d, n) => row.push((*d, *n)),
                    }
                }
                rows.push(row);
                if bare_limit > 0 && rows.len() >= bare_limit as usize {
                    break;
                }
            }
            rows
        } else if has_group_by {
            let mut rows = Vec::new();
            // Pre-finalize all agg results keyed by group
            'group_loop: for (key, &group_idx) in &group_map {
                let accs = &flat_accs
                    [group_idx as usize * n_agg_specs..(group_idx as usize + 1) * n_agg_specs];
                let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                for (spec_idx, spec) in agg_specs.iter().enumerate() {
                    agg_results.push(finalize_accumulator(&accs[spec_idx], spec));
                }

                // Apply HAVING filters on finalized aggregate values
                for hf in &having_filters {
                    let (datum, is_null) = agg_results[hf.agg_idx];
                    if is_null {
                        continue 'group_loop; // NULL doesn't satisfy HAVING
                    }
                    let val = datum.value() as i64;
                    let pass = match hf.op {
                        HavingOp::Gt => val > hf.const_val,
                        HavingOp::Lt => val < hf.const_val,
                        HavingOp::Ge => val >= hf.const_val,
                        HavingOp::Le => val <= hf.const_val,
                        HavingOp::Eq => val == hf.const_val,
                        HavingOp::Ne => val != hf.const_val,
                    };
                    if !pass {
                        continue 'group_loop;
                    }
                }

                let key_slice = key.as_slice();
                let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                for entry in &output_map {
                    match entry {
                        OutputEntry::Agg(ai) => {
                            row.push(agg_results[*ai]);
                        }
                        OutputEntry::Group(gi) => {
                            match &key_slice[*gi] {
                                GroupKeyVal::Null => {
                                    row.push((pg_sys::Datum::from(0usize), true));
                                }
                                GroupKeyVal::Int(v) => {
                                    if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. })
                                    {
                                        // extract() returns numeric — convert i64 to numeric datum
                                        row.push((i128_to_numeric_datum(*v as i128), false));
                                    } else {
                                        row.push((pg_sys::Datum::from(*v as usize), false));
                                    }
                                }
                                GroupKeyVal::Str(off, len) => {
                                    let s = string_arena.get(*off, *len);
                                    let datum = string_to_datum(s, group_specs[*gi].type_oid);
                                    row.push((datum, false));
                                }
                            }
                        }
                        OutputEntry::DerivedGroup { base_gi, delta } => {
                            match &key_slice[*base_gi] {
                                GroupKeyVal::Int(v) => {
                                    row.push((pg_sys::Datum::from((*v + delta) as usize), false))
                                }
                                _ => row.push((pg_sys::Datum::from(0usize), true)),
                            }
                        }
                        OutputEntry::Const(d, n) => row.push((*d, *n)),
                    }
                }
                rows.push(row);
                if bare_limit > 0 && rows.len() >= bare_limit as usize {
                    break;
                }
            }
            rows
        } else if let Some(accumulators) = &global_accumulators {
            let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
            for (spec_idx, spec) in agg_specs.iter().enumerate() {
                agg_results.push(finalize_accumulator(&accumulators[spec_idx], spec));
            }
            let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
            for entry in &output_map {
                match entry {
                    OutputEntry::Agg(ai) => {
                        row.push(agg_results[*ai]);
                    }
                    OutputEntry::Group(_) | OutputEntry::DerivedGroup { .. } => {
                        row.push((pg_sys::Datum::from(0usize), true));
                    }
                    OutputEntry::Const(d, n) => row.push((*d, *n)),
                }
            }
            vec![row]
        } else {
            vec![]
        };

        let finalize_us = t_finalize.elapsed().as_micros() as u64;

        // Apply top-N: sort by the specified output column and truncate
        // (compact top-N pushdown path already has correct results; this handles other paths)
        let pre_topn_groups = if use_compact_keys && use_compact_accs {
            compact_group_map.len()
        } else {
            result_rows.len()
        };
        if topn_limit > 0 && has_group_by && result_rows.len() > topn_limit as usize {
            let si = topn_sort_col;
            if topn_ascending {
                result_rows.sort_by_key(|row| {
                    let (datum, is_null) = row[si];
                    if is_null {
                        i64::MAX
                    } else {
                        datum.value() as i64
                    }
                });
            } else {
                result_rows.sort_by(|a, b| {
                    let (da, na) = a[si];
                    let (db, nb) = b[si];
                    let va = if na { i64::MIN } else { da.value() as i64 };
                    let vb = if nb { i64::MIN } else { db.value() as i64 };
                    vb.cmp(&va) // reverse order for DESC
                });
            }
            result_rows.truncate(topn_limit as usize);
        }

        // Clean up segment memory context
        if !segment_mcxt.is_null() {
            pg_sys::MemoryContextDelete(segment_mcxt);
        }

        AggScanState {
            _agg_specs: agg_specs,
            _group_specs: group_specs,
            result_rows,
            _num_result_cols: num_result_cols,
            metadata_us,
            heap_scan_us,
            detoast_us: total_detoast_us,
            blob_cache_hits: total_cache_hits,
            blob_cache_misses: total_cache_misses,
            blob_cache_bytes_served: total_cache_bytes_served,
            decompress_us,
            agg_us,
            total_segments,
            total_rows_processed,
            batch_quals_count: batch_quals.len(),
            where_quals_null: where_quals.is_null(),
            regex_cache_size: regex_cache.len() as u64,
            regex_cache_calls,
            topn_limit: if topn_limit > 0 { topn_limit as u64 } else { 0 },
            topn_sort_col: topn_sort_col as i64,
            topn_ascending,
            pre_topn_groups: pre_topn_groups as u64,
            finalize_us,
            topn_select_us,
            wall_us: t_wall.elapsed().as_micros() as u64,
            buf_stats: take_scan_buf_stats(),
            ..AggScanState::default()
        }
    }
}

/// Inner row loop for the COMPACT serial sub-path (packed u128 keys +
/// flat byte-buffer accumulators).
///
/// SAFETY: dereferences raw datum pointers in `decompressed` via
/// `datum_to_i128` / `datum_to_f64`. Caller must ensure those datums
/// remain valid for the duration of the call.
#[allow(clippy::too_many_arguments)]
#[inline]
unsafe fn serial_compact_row_loop(
    agg_specs: &[AggExecSpec],
    group_specs: &[GroupByColSpec],
    decompressed: &[Vec<(pg_sys::Datum, bool)>],
    raw_strings: &[Option<Vec<Option<String>>>],
    selection: &[bool],
    row_count: usize,
    storage: &mut CompactAccStorage,
    compact_group_map: &mut CompactGroupMap,
    cd_sidecar: &mut CountDistinctSideCar,
    total_rows_processed: &mut u64,
) {
    unsafe {
        let num_group_keys = group_specs.len();

        for row in 0..row_count {
            if !selection.is_empty() && !selection[row] {
                continue;
            }
            *total_rows_processed += 1;

            // Build packed u128 key from integer GROUP BY columns
            let mut int_keys: [i64; 2] = [0; 2];
            let mut has_null = false;
            for (ki, gs) in group_specs.iter().enumerate() {
                let col = &decompressed[gs.col_idx as usize];
                if col.is_empty() || col[row].1 {
                    has_null = true;
                    break;
                }
                int_keys[ki] = match &gs.expr {
                    GroupByExpr::DateTrunc { unit_usecs, .. } => {
                        let pg_usec = col[row].0.value() as i64;
                        pg_usec.div_euclid(*unit_usecs) * *unit_usecs
                    }
                    GroupByExpr::Extract { unit, divisor, .. } => {
                        eval_extract(col[row].0.value() as i64, *divisor, unit)
                    }
                    GroupByExpr::AddConst { offset, .. } => col[row].0.value() as i64 + offset,
                    GroupByExpr::Column => col[row].0.value() as i64,
                    _ => unreachable!(),
                };
            }

            // Skip null groups (they don't appear in GROUP BY results)
            if has_null {
                continue;
            }

            let packed = if num_group_keys == 1 {
                pack_int_key_1(int_keys[0])
            } else {
                pack_int_keys_2(int_keys[0], int_keys[1])
            };

            // Lookup or insert group.
            // Cap hashmap growth: above 32M entries, reserve in 8M
            // increments instead of letting hashbrown double.
            if compact_group_map.len() == compact_group_map.capacity() {
                let cap = compact_group_map.capacity();
                let extra = if cap >= 32_000_000 {
                    8_000_000 // ~170MB at 21B/slot
                } else {
                    0 // let hashbrown double normally for small maps
                };
                if extra > 0 {
                    compact_group_map.reserve(extra);
                }
            }
            let group_idx = match compact_group_map.entry(packed) {
                hashbrown::hash_map::Entry::Occupied(e) => *e.get(),
                hashbrown::hash_map::Entry::Vacant(e) => {
                    let idx = storage.alloc_group();
                    cd_sidecar.alloc_group();
                    e.insert(idx);
                    idx
                }
            };

            // Update compact accumulators
            for (spec_idx, spec) in agg_specs.iter().enumerate() {
                let (_, kind) = storage.layout.slots[spec_idx];
                match kind {
                    CompactAccKind::Count => match spec.agg_type {
                        AggType::CountStar => {
                            storage.incr_count(group_idx, spec_idx, 1);
                        }
                        AggType::Count => {
                            let col = &decompressed[spec.col_idx as usize];
                            if !col.is_empty() && !col[row].1 {
                                storage.incr_count(group_idx, spec_idx, 1);
                            }
                        }
                        _ => {}
                    },
                    CompactAccKind::SumInt => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = datum_to_i128(col[row].0, spec.col_type_oid);
                            let sum_delta = if spec.expr_kind == AggExpr::AddConst {
                                v + spec.const_offset as i128
                            } else {
                                v
                            };
                            storage.add_sum_int(group_idx, spec_idx, sum_delta, 1);
                        }
                    }
                    CompactAccKind::SumIntNarrow => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = col[row].0.value() as i64;
                            let sum_delta = if spec.expr_kind == AggExpr::AddConst {
                                v + spec.const_offset
                            } else {
                                v
                            };
                            storage.add_sum_int_narrow(group_idx, spec_idx, sum_delta, 1);
                        }
                    }
                    CompactAccKind::SumFloat => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = datum_to_f64(col[row].0, spec.col_type_oid);
                            let sum_delta = if spec.expr_kind == AggExpr::AddConst {
                                v + spec.const_offset as f64
                            } else {
                                v
                            };
                            storage.add_sum_float(group_idx, spec_idx, sum_delta, 1);
                        }
                    }
                    CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                        let col_idx = spec.col_idx as usize;
                        if let Some(ref rs) = raw_strings[col_idx]
                            && let Some(ref s) = rs[row]
                        {
                            let (cur_off, cur_len) = storage.read_min_max_str(group_idx, spec_idx);
                            let should_update = if cur_off == u32::MAX {
                                true
                            } else {
                                let cur = storage.str_arena.get(cur_off, cur_len);
                                let cmp = collation_strcmp(s, cur);
                                match kind {
                                    CompactAccKind::MinStr => cmp < 0,
                                    CompactAccKind::MaxStr => cmp > 0,
                                    _ => unreachable!(),
                                }
                            };
                            if should_update {
                                let (new_off, new_len) = storage.str_arena.alloc(s);
                                storage.write_min_max_str(group_idx, spec_idx, new_off, new_len);
                            }
                        }
                    }
                    CompactAccKind::MinInt => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = col[row].0.value() as i64;
                            storage.update_min_int(group_idx, spec_idx, v);
                        }
                    }
                    CompactAccKind::MaxInt => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = col[row].0.value() as i64;
                            storage.update_max_int(group_idx, spec_idx, v);
                        }
                    }
                    CompactAccKind::CountDistinctInt => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            cd_sidecar.insert_int(spec_idx, group_idx, col[row].0.value() as i64);
                        }
                    }
                    CompactAccKind::CountDistinctStr => {
                        let col_idx = spec.col_idx as usize;
                        if let Some(ref rs) = raw_strings[col_idx]
                            && let Some(ref s) = rs[row]
                        {
                            cd_sidecar.insert_str(spec_idx, group_idx, hash128_str(s.as_bytes()));
                        }
                    }
                }
            }
        }
    }
}

/// Inner row loop for the GENERIC serial sub-path (original `GroupKey` +
/// `AggAccumulator` Vec).
///
/// SAFETY: calls PG FFI (`OidFunctionCall3Coll`, `text_to_cstring`,
/// `pfree`, `cstring_to_text_with_len`). Must run inside an active PG
/// transaction.
#[allow(clippy::too_many_arguments)]
#[inline]
unsafe fn serial_generic_row_loop(
    agg_specs: &[AggExecSpec],
    group_specs: &[GroupByColSpec],
    prototype_accumulators: &[AggAccumulator],
    regexp_group_infos: &[RegexpGroupInfo],
    raw_string_cols: &[bool],
    const_group_keys: &[Option<i64>],
    decompressed: &[Vec<(pg_sys::Datum, bool)>],
    raw_strings: &[Option<Vec<Option<String>>>],
    seg_text_columns: &[Option<SegTextColumn>],
    case_when_seg_cols: &[Option<SegTextColumn>],
    selection: &[bool],
    row_count: usize,
    has_group_by: bool,
    has_regexp_group: bool,
    is_single_group_key: bool,
    group_map: &mut GroupMap,
    flat_accs: &mut Vec<AggAccumulator>,
    string_arena: &mut StringArena,
    global_accumulators: &mut Option<Vec<AggAccumulator>>,
    regex_cache: &mut HashMap<String, String>,
    regex_cache_calls: &mut u64,
    total_rows_processed: &mut u64,
) {
    unsafe {
        let n_agg_specs = agg_specs.len();

        // Reusable buffers for the aggregate loop (avoid per-row heap allocation)
        let mut key_ref: Vec<GroupKeyRef> = Vec::with_capacity(group_specs.len());
        let mut regex_results: Vec<Option<String>> = Vec::new();

        for row in 0..row_count {
            if !selection.is_empty() && !selection[row] {
                continue;
            }

            *total_rows_processed += 1;

            let accumulators = if has_group_by {
                // Clear key_ref first to release borrows on regex_results
                key_ref.clear();
                // Pre-compute regex results for this row (needs mutable regex_cache,
                // so must be done before building borrowed key_ref)
                regex_results.clear();
                if has_regexp_group {
                    for (gi, gs) in group_specs.iter().enumerate() {
                        if let GroupByExpr::RegexpReplace { .. } = &gs.expr {
                            let rs = raw_strings[gs.col_idx as usize].as_ref().unwrap();
                            if let Some(ref input_str) = rs[row] {
                                let rgi = regexp_group_infos
                                    .iter()
                                    .find(|r| r.group_idx == gi)
                                    .unwrap();
                                let result =
                                    regex_cache.entry(input_str.clone()).or_insert_with(|| {
                                        *regex_cache_calls += 1;
                                        let input_datum = {
                                            let text = pg_sys::cstring_to_text_with_len(
                                                input_str.as_ptr() as *const _,
                                                input_str.len() as i32,
                                            );
                                            pg_sys::Datum::from(text as usize)
                                        };
                                        let result_datum = pg_sys::OidFunctionCall3Coll(
                                            rgi.func_oid,
                                            rgi.collation,
                                            input_datum,
                                            rgi.pattern_datum,
                                            rgi.replacement_datum,
                                        );
                                        let cstr =
                                            pg_sys::text_to_cstring(result_datum.cast_mut_ptr());
                                        let s = std::ffi::CStr::from_ptr(cstr)
                                            .to_string_lossy()
                                            .into_owned();
                                        pg_sys::pfree(cstr as *mut _);
                                        s
                                    });
                                regex_results.push(Some(result.clone()));
                            } else {
                                regex_results.push(None);
                            }
                        }
                    }
                }

                // Build temporary borrowed key (reuse buffer, no heap alloc)
                let mut regex_idx = 0;
                for (gi, gs) in group_specs.iter().enumerate() {
                    // CaseWhen has col_idx=-1, handle separately via pre-computed SegTextColumn
                    if let GroupByExpr::CaseWhen(_) = &gs.expr {
                        if let Some(Some(seg_col)) = case_when_seg_cols.get(gi) {
                            match seg_col.get_str(row) {
                                Some(s) => key_ref.push(GroupKeyRef::from_str(s)),
                                None => key_ref.push(GroupKeyRef::Null),
                            }
                        } else {
                            key_ref.push(GroupKeyRef::Null);
                        }
                        continue;
                    }
                    if let Some(v) = const_group_keys[gi] {
                        key_ref.push(GroupKeyRef::Int(v));
                        continue;
                    }
                    let col = &decompressed[gs.col_idx as usize];
                    if col.is_empty() || col[row].1 {
                        key_ref.push(GroupKeyRef::Null);
                        if matches!(&gs.expr, GroupByExpr::RegexpReplace { .. }) {
                            regex_idx += 1;
                        }
                    } else {
                        match &gs.expr {
                            GroupByExpr::RegexpReplace { .. } => {
                                match &regex_results[regex_idx] {
                                    Some(s) => key_ref.push(GroupKeyRef::from_str(s.as_str())),
                                    None => key_ref.push(GroupKeyRef::Null),
                                }
                                regex_idx += 1;
                            }
                            GroupByExpr::DateTrunc { unit_usecs, .. } => {
                                let pg_usec = col[row].0.value() as i64;
                                let truncated = pg_usec.div_euclid(*unit_usecs) * *unit_usecs;
                                key_ref.push(GroupKeyRef::Int(truncated));
                            }
                            GroupByExpr::Extract { unit, divisor, .. } => {
                                let extracted =
                                    eval_extract(col[row].0.value() as i64, *divisor, unit);
                                key_ref.push(GroupKeyRef::Int(extracted));
                            }
                            GroupByExpr::AddConst { offset, .. } => {
                                let datum = col[row].0;
                                let v = datum.value() as i64;
                                key_ref.push(GroupKeyRef::Int(v + offset));
                            }
                            GroupByExpr::Column => {
                                // Text GROUP BY: get &str from decoded segment data
                                if let Some(ref seg_text) = seg_text_columns[gs.col_idx as usize] {
                                    match seg_text.get_str(row) {
                                        Some(s) => key_ref.push(GroupKeyRef::from_str(s)),
                                        None => key_ref.push(GroupKeyRef::Null),
                                    }
                                } else {
                                    let datum = col[row].0;
                                    key_ref.push(GroupKeyRef::Int(datum.value() as i64));
                                }
                            }
                            GroupByExpr::CaseWhen(_) => unreachable!(),
                        }
                    }
                }

                // Use hashbrown raw_entry to avoid cloning the key for existing groups
                let h = hash_group_key_ref(&key_ref);
                let group_idx = match group_map
                    .raw_entry_mut()
                    .from_hash(h, |stored| keys_match(stored, &key_ref, string_arena))
                {
                    hashbrown::hash_map::RawEntryMut::Occupied(e) => *e.into_mut(),
                    hashbrown::hash_map::RawEntryMut::Vacant(e) => {
                        let owned_key = if is_single_group_key {
                            GroupKey::Single(key_ref[0].resolve(string_arena))
                        } else {
                            GroupKey::Multi(
                                key_ref.iter().map(|r| r.resolve(string_arena)).collect(),
                            )
                        };
                        let idx = (flat_accs.len() / n_agg_specs) as u32;
                        for proto in prototype_accumulators {
                            flat_accs.push(proto.clone_fresh());
                        }
                        e.insert_with_hasher(h, owned_key, idx, |k| {
                            hash_group_key(k, string_arena)
                        });
                        idx
                    }
                };
                &mut flat_accs
                    [group_idx as usize * n_agg_specs..(group_idx as usize + 1) * n_agg_specs]
            } else {
                global_accumulators.as_mut().unwrap().as_mut_slice()
            };

            for (spec_idx, spec) in agg_specs.iter().enumerate() {
                let acc = &mut accumulators[spec_idx];
                match spec.agg_type {
                    AggType::CountStar => {
                        if let AggAccumulator::Count { count } = acc {
                            *count += 1;
                        }
                    }
                    AggType::Count => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty()
                            && !col[row].1
                            && let AggAccumulator::Count { count } = acc
                        {
                            *count += 1;
                        }
                    }
                    AggType::Sum | AggType::Avg => {
                        // When LengthOf + raw_string_cols, compute length from raw strings
                        // (decompressed has dummy 0 datums for raw_string_cols columns)
                        if spec.expr_kind == AggExpr::LengthOf
                            && raw_string_cols
                                .get(spec.col_idx as usize)
                                .copied()
                                .unwrap_or(false)
                        {
                            if let Some(ref rs) = raw_strings[spec.col_idx as usize]
                                && let Some(ref s) = rs[row]
                            {
                                match acc {
                                    AggAccumulator::SumInt { sum, count } => {
                                        *sum += s.chars().count() as i128;
                                        *count += 1;
                                    }
                                    AggAccumulator::SumFloat { sum, count } => {
                                        *sum += s.chars().count() as f64;
                                        *count += 1;
                                    }
                                    _ => {}
                                }
                            }
                        } else {
                            let col = &decompressed[spec.col_idx as usize];
                            if !col.is_empty() && !col[row].1 {
                                let datum = col[row].0;
                                match acc {
                                    AggAccumulator::SumInt { sum, count } => {
                                        let v = datum_to_i128(datum, spec.col_type_oid);
                                        if spec.expr_kind == AggExpr::AddConst {
                                            *sum += v + spec.const_offset as i128;
                                        } else {
                                            *sum += v;
                                        }
                                        *count += 1;
                                    }
                                    AggAccumulator::SumFloat { sum, count } => {
                                        let v = datum_to_f64(datum, spec.col_type_oid);
                                        if spec.expr_kind == AggExpr::AddConst {
                                            *sum += v + spec.const_offset as f64;
                                        } else {
                                            *sum += v;
                                        }
                                        *count += 1;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    AggType::CountDistinct => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let datum = col[row].0;
                            match acc {
                                AggAccumulator::CountDistinctInt { seen } => {
                                    seen.insert(datum.value() as i64);
                                }
                                AggAccumulator::CountDistinctStr { seen } => {
                                    let cstr = pg_sys::text_to_cstring(datum.cast_mut_ptr());
                                    let bytes = std::ffi::CStr::from_ptr(cstr).to_bytes();
                                    let hash = hash128_str(bytes);
                                    pg_sys::pfree(cstr as *mut _);
                                    seen.insert(hash);
                                }
                                _ => {}
                            }
                        }
                    }
                    AggType::Min => {
                        // For text columns referenced by raw_string_cols, use raw strings
                        if raw_string_cols
                            .get(spec.col_idx as usize)
                            .copied()
                            .unwrap_or(false)
                        {
                            if let Some(ref rs) = raw_strings[spec.col_idx as usize]
                                && let Some(ref s) = rs[row]
                                && let AggAccumulator::MinStr { val } = acc
                                && val.as_ref().is_none_or(|cur| collation_strcmp(s, cur) < 0)
                            {
                                *val = Some(s.clone());
                            }
                        } else {
                            let col = &decompressed[spec.col_idx as usize];
                            if !col.is_empty() && !col[row].1 {
                                let datum = col[row].0;
                                match acc {
                                    AggAccumulator::MinInt { val } => {
                                        let v = datum.value() as i64;
                                        if val.is_none_or(|cur| v < cur) {
                                            *val = Some(v);
                                        }
                                    }
                                    AggAccumulator::MinFloat { val } => {
                                        let v = datum_to_f64(datum, spec.col_type_oid);
                                        if val.is_none_or(|cur| v < cur) {
                                            *val = Some(v);
                                        }
                                    }
                                    AggAccumulator::MinStr { val } => {
                                        let cstr = pg_sys::text_to_cstring(datum.cast_mut_ptr());
                                        let s = std::ffi::CStr::from_ptr(cstr)
                                            .to_string_lossy()
                                            .into_owned();
                                        pg_sys::pfree(cstr as *mut _);
                                        if val
                                            .as_ref()
                                            .is_none_or(|cur| collation_strcmp(&s, cur) < 0)
                                        {
                                            *val = Some(s);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    AggType::Max => {
                        if raw_string_cols
                            .get(spec.col_idx as usize)
                            .copied()
                            .unwrap_or(false)
                        {
                            if let Some(ref rs) = raw_strings[spec.col_idx as usize]
                                && let Some(ref s) = rs[row]
                                && let AggAccumulator::MaxStr { val } = acc
                                && val.as_ref().is_none_or(|cur| collation_strcmp(s, cur) > 0)
                            {
                                *val = Some(s.clone());
                            }
                        } else {
                            let col = &decompressed[spec.col_idx as usize];
                            if !col.is_empty() && !col[row].1 {
                                let datum = col[row].0;
                                match acc {
                                    AggAccumulator::MaxInt { val } => {
                                        let v = datum.value() as i64;
                                        if val.is_none_or(|cur| v > cur) {
                                            *val = Some(v);
                                        }
                                    }
                                    AggAccumulator::MaxFloat { val } => {
                                        let v = datum_to_f64(datum, spec.col_type_oid);
                                        if val.is_none_or(|cur| v > cur) {
                                            *val = Some(v);
                                        }
                                    }
                                    AggAccumulator::MaxStr { val } => {
                                        let cstr = pg_sys::text_to_cstring(datum.cast_mut_ptr());
                                        let s = std::ffi::CStr::from_ptr(cstr)
                                            .to_string_lossy()
                                            .into_owned();
                                        pg_sys::pfree(cstr as *mut _);
                                        if val
                                            .as_ref()
                                            .is_none_or(|cur| collation_strcmp(&s, cur) > 0)
                                        {
                                            *val = Some(s);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
