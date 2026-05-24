//! Plan-time metadata loading + per-segment metadata fast paths.
//!
//! Two entry points used by `begin_agg_scan` before any decompression is
//! attempted:
//!
//! - [`load_agg_metadata_from_plan`] — SPI lookup of column names / types /
//!   typmods / segment-by / time-column for the companion table. The same
//!   shape is what `init_worker_deltax_agg` will hydrate from `append_wire`
//!   when parallel V2 lands.
//! - [`try_catalog_shortcut`] — answer ungrouped, unfiltered queries from
//!   `get_row_count` alone (today: `COUNT(*)` only).
//! - [`try_metadata_fast_path`] — answer ungrouped queries from
//!   per-segment `_sum_*` / `_min_*` / `_max_*` / `_nonnull_count_*` columns
//!   without touching compressed blobs, falling through to a parallel
//!   `accumulate_segment_decompressed` for ambiguous segments.

use std::collections::HashMap;
use std::time::Instant;

use pgrx::pg_sys;
use pgrx::prelude::*;

use super::super::batch_qual::{
    BatchCompareOp, BatchQual, apply_batch_filter_f32, apply_batch_filter_f64,
    apply_batch_filter_i16, apply_batch_filter_i32, apply_batch_filter_i64,
    apply_batch_filter_in_list,
};
use super::super::datum_utils::{decompress_blob_to_datums, pg_type_name};
use super::super::segments::{
    MetadataInfo, SegmentData, SegmentQualResult, classify_segment_quals, is_zero_const,
    load_metadata,
};
use super::extract::decode_encoded_to_pg_i64;
use super::state::{
    AggAccumulator, AggExecSpec, AggExpr, AggScanState, AggType, OutputEntry, ParsedAggPlan,
};
use crate::compress::{decode_i64_to_f32, decode_i64_to_f64};

/// Fast path: answer the query from companion catalog metadata only.
///
/// Used when every aggregate is `COUNT(*)` on a single companion table and
/// there are no `WHERE`/`GROUP BY`/`HAVING` clauses, so the answer is just
/// the sum of `row_count`s from the companion meta table.
///
/// The caller provides pre-fetched catalog data so this function has no
/// external dependencies and is easy to test:
/// - `row_counts`: one `Option<i64>` per companion OID (from `get_row_count`)
///
/// Returns Some(state) if the shortcut succeeded, None to fall through to
/// segment-based execution.
pub(super) fn try_catalog_shortcut(
    plan: &ParsedAggPlan,
    _meta: &MetadataInfo,
    row_counts: &[Option<i64>],
    metadata_us: u64,
) -> Option<AggScanState> {
    if !plan.group_specs.is_empty()
        || !plan.where_quals.is_null()
        || !plan.having_filters.is_empty()
    {
        return None;
    }

    let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
    for spec in &plan.agg_specs {
        match spec.agg_type {
            AggType::CountStar => {
                let mut total: i64 = 0;
                for rc in row_counts {
                    total += (*rc)?;
                }
                agg_results.push((pg_sys::Datum::from(total as usize), false));
            }
            _ => return None, // Non-catalog-answerable agg
        }
    }

    let num_result_cols = plan.output_map.len();
    let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
    for entry in &plan.output_map {
        match entry {
            OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
            OutputEntry::Group(_) | OutputEntry::DerivedGroup { .. } => {
                row.push((pg_sys::Datum::from(0usize), true))
            }
            OutputEntry::Const(d, n) => row.push((*d, *n)),
        }
    }
    Some(AggScanState {
        result_rows: vec![row],
        _num_result_cols: num_result_cols,
        metadata_us,
        where_quals_null: true,
        topn_ascending: true,
        buf_stats: super::super::segments::take_scan_buf_stats(),
        ..AggScanState::default()
    })
}

/// Try to compute scalar aggregates using only per-segment metadata
/// (sum, nonnull_count, min, max) stored in the companion table, without
/// decompressing any column data.
///
/// This works for ungrouped, unfiltered queries where every aggregate is
/// SUM/AVG/COUNT on numeric columns, COUNT(*), or MIN/MAX. The companion
/// table stores pre-computed _sum_*, _min_*, _max_*, and _nonnull_count_*
/// columns for each segment, so we just accumulate across segments.
///
/// For SUM(col + C), we use the identity: SUM(col + C) = SUM(col) + C * COUNT(col).
///
/// The caller loads segments (via `load_segments_heap`) and passes them in,
/// keeping this function free of I/O and easy to test.
///
/// Returns Some(state) if metadata was sufficient, None to fall through to
/// full decompression.
pub(super) fn try_metadata_fast_path(
    plan: &ParsedAggPlan,
    meta: &MetadataInfo,
    segments: &[SegmentData],
    batch_quals: &[BatchQual],
    metadata_us: u64,
    heap_scan_us: u64,
) -> Option<AggScanState> {
    // Still bail on GROUP BY and HAVING
    if !plan.group_specs.is_empty() || !plan.having_filters.is_empty() {
        return None;
    }

    let has_where = !plan.where_quals.is_null();

    if has_where {
        // Bail if no batch quals extracted (unhandled qual types)
        if batch_quals.is_empty() {
            return None;
        }
        // Bail if any qual is on a non-numeric type (text LIKE etc.)
        let numeric_types = [
            pg_sys::INT2OID,
            pg_sys::INT4OID,
            pg_sys::INT8OID,
            pg_sys::FLOAT4OID,
            pg_sys::FLOAT8OID,
            pg_sys::TIMESTAMPOID,
            pg_sys::TIMESTAMPTZOID,
            pg_sys::DATEOID,
        ];
        if batch_quals
            .iter()
            .any(|bq| !numeric_types.contains(&bq.type_oid))
        {
            return None;
        }
    }

    // Check that all agg specs are metadata-resolvable
    let all_resolvable = plan.agg_specs.iter().all(|spec| {
        match spec.agg_type {
            AggType::CountStar => true,
            AggType::Sum => {
                (spec.expr_kind == AggExpr::Column || spec.expr_kind == AggExpr::AddConst)
                    && spec.col_idx >= 0
                    && {
                        let t = spec.col_type_oid;
                        t == pg_sys::INT2OID
                            || t == pg_sys::INT4OID
                            || t == pg_sys::INT8OID
                            || t == pg_sys::FLOAT4OID
                            || t == pg_sys::FLOAT8OID
                    }
            }
            AggType::Avg | AggType::Count => {
                spec.expr_kind == AggExpr::Column && spec.col_idx >= 0 && {
                    let t = spec.col_type_oid;
                    t == pg_sys::INT2OID
                        || t == pg_sys::INT4OID
                        || t == pg_sys::INT8OID
                        || t == pg_sys::FLOAT4OID
                        || t == pg_sys::FLOAT8OID
                }
            }
            // Min/Max can only be resolved from metadata when there's no WHERE
            AggType::Min | AggType::Max => {
                !has_where && spec.expr_kind == AggExpr::Column && spec.col_idx >= 0
            }
            _ => false, // CountDistinct always bails
        }
    });
    if !all_resolvable {
        return None;
    }

    // Check that metadata actually exists for all needed columns in all segments
    let sums_available = plan.agg_specs.iter().all(|spec| match spec.agg_type {
        AggType::Sum | AggType::Avg | AggType::Count => {
            let col_name = &meta.col_names[spec.col_idx as usize];
            segments.is_empty()
                || segments
                    .iter()
                    .all(|seg| seg.col_sums.contains_key(col_name))
        }
        _ => true,
    });
    let minmax_available = plan.agg_specs.iter().all(|spec| match spec.agg_type {
        AggType::Min | AggType::Max => {
            let col_name = &meta.col_names[spec.col_idx as usize];
            segments.is_empty()
                || segments
                    .iter()
                    .all(|seg| seg.col_minmax.contains_key(col_name))
        }
        _ => true,
    });

    if !sums_available || !minmax_available {
        return None;
    }

    // Accumulate from metadata (with optional filtered decompression for ambiguous segments)
    let mut accumulators: Vec<AggAccumulator> = plan
        .agg_specs
        .iter()
        .map(|spec| AggAccumulator::new_for(spec.agg_type, spec.col_type_oid))
        .collect();

    // Pre-classify segments
    let (allpass, ambiguous): (Vec<&SegmentData>, Vec<&SegmentData>) = if has_where {
        let mut ap = Vec::new();
        let mut amb = Vec::new();
        for seg in segments {
            if seg.row_count == 0 {
                continue;
            }
            match classify_segment_quals(seg, batch_quals, &meta.col_names) {
                SegmentQualResult::AllPass => ap.push(seg),
                SegmentQualResult::NonePass => {} // skip — no rows pass
                SegmentQualResult::Ambiguous => amb.push(seg),
            }
        }
        (ap, amb)
    } else {
        (
            segments.iter().filter(|s| s.row_count > 0).collect(),
            vec![],
        )
    };

    // AllPass: instant metadata accumulation
    for seg in &allpass {
        accumulate_segment_metadata(&mut accumulators, seg, &plan.agg_specs, meta);
    }
    let segments_metadata_resolved = allpass.len() as u64;

    // Try to resolve ambiguous segments via nonzero_count metadata
    // Only works for: single qual, Ne/Eq with const 0, all aggs are CountStar
    let ambiguous = if !ambiguous.is_empty()
        && batch_quals.len() == 1
        && plan
            .agg_specs
            .iter()
            .all(|s| s.agg_type == AggType::CountStar)
    {
        let bq = &batch_quals[0];
        if is_zero_const(bq.const_datum, bq.type_oid)
            && matches!(bq.op, BatchCompareOp::Ne | BatchCompareOp::Eq)
        {
            let col_name = &meta.col_names[bq.col_idx];
            // Check all ambiguous segments have nonzero_count metadata
            let all_have_nz = ambiguous.iter().all(|seg| {
                seg.col_sums
                    .get(col_name)
                    .map(|cs| cs.nonzero_count >= 0)
                    .unwrap_or(false)
            });
            if all_have_nz {
                // Resolve from metadata
                for seg in &ambiguous {
                    let cs = seg.col_sums.get(col_name).unwrap();
                    let passing = match bq.op {
                        BatchCompareOp::Ne => cs.nonzero_count,
                        BatchCompareOp::Eq => cs.nonnull_count - cs.nonzero_count,
                        _ => unreachable!(),
                    };
                    for (i, spec) in plan.agg_specs.iter().enumerate() {
                        if spec.agg_type == AggType::CountStar
                            && let AggAccumulator::Count { count } = &mut accumulators[i]
                        {
                            *count += passing;
                        }
                    }
                }
                vec![] // all resolved
            } else {
                ambiguous
            }
        } else {
            ambiguous
        }
    } else {
        ambiguous
    };

    // If ambiguous segments remain but have no blobs loaded (metadata-only fast path),
    // bail out — the full scan path will handle them with proper blob loading.
    if !ambiguous.is_empty()
        && ambiguous
            .iter()
            .any(|s| s.compressed_blobs.iter().all(|b| b.is_empty()))
    {
        return None;
    }

    // Ambiguous: parallel decompression
    let n_workers = crate::get_parallel_workers();
    let (segments_decompressed, agg_us) = if !ambiguous.is_empty() && n_workers > 1 {
        let chunk_size = ambiguous.len().div_ceil(n_workers);
        let results: Vec<_> = std::thread::scope(|s| {
            ambiguous
                .chunks(chunk_size)
                .map(|chunk| {
                    let specs = &plan.agg_specs;
                    let bqs = batch_quals;
                    let m = meta;
                    s.spawn(move || {
                        let mut local_acc: Vec<AggAccumulator> = specs
                            .iter()
                            .map(|sp| AggAccumulator::new_for(sp.agg_type, sp.col_type_oid))
                            .collect();
                        let t = Instant::now();
                        for seg in chunk {
                            unsafe {
                                accumulate_segment_decompressed(&mut local_acc, seg, bqs, specs, m);
                            }
                        }
                        (
                            local_acc,
                            chunk.len() as u64,
                            t.elapsed().as_micros() as u64,
                        )
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect()
        });
        let mut total_decomp = 0u64;
        let mut max_us = 0u64;
        for (local_acc, cnt, us) in results {
            for (dst, src) in accumulators.iter_mut().zip(local_acc.iter()) {
                merge_accumulator(dst, src);
            }
            total_decomp += cnt;
            max_us = max_us.max(us);
        }
        (total_decomp, max_us)
    } else if !ambiguous.is_empty() {
        let t = Instant::now();
        for seg in &ambiguous {
            unsafe {
                accumulate_segment_decompressed(
                    &mut accumulators,
                    seg,
                    batch_quals,
                    &plan.agg_specs,
                    meta,
                );
            }
        }
        (ambiguous.len() as u64, t.elapsed().as_micros() as u64)
    } else {
        (0, 0)
    };

    // Finalize accumulators
    let num_result_cols = plan.output_map.len();
    let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
    for (i, acc) in accumulators.iter().enumerate() {
        agg_results.push(unsafe { super::finalize_accumulator(acc, &plan.agg_specs[i]) });
    }
    let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
    for entry in &plan.output_map {
        match entry {
            OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
            OutputEntry::Group(_) | OutputEntry::DerivedGroup { .. } => {
                row.push((pg_sys::Datum::from(0usize), true))
            }
            OutputEntry::Const(d, n) => row.push((*d, *n)),
        }
    }

    let total_segments = segments.len() as u64;
    Some(AggScanState {
        result_rows: vec![row],
        _num_result_cols: num_result_cols,
        metadata_us,
        heap_scan_us,
        agg_us,
        total_segments,
        batch_quals_count: batch_quals.len(),
        where_quals_null: !has_where,
        segments_metadata_resolved,
        segments_decompressed,
        topn_ascending: true,
        buf_stats: super::super::segments::take_scan_buf_stats(),
        ..AggScanState::default()
    })
}

/// Merge a source accumulator into a destination (used for parallel reduction).
/// Only Count/SumInt/SumFloat are used in filtered fast path (Min/Max/CountDistinct bail earlier).
pub(super) fn merge_accumulator(dst: &mut AggAccumulator, src: &AggAccumulator) {
    match (dst, src) {
        (AggAccumulator::Count { count: dc }, AggAccumulator::Count { count: sc }) => *dc += sc,
        (
            AggAccumulator::SumInt { sum: ds, count: dc },
            AggAccumulator::SumInt { sum: ss, count: sc },
        ) => {
            *ds += ss;
            *dc += sc;
        }
        (
            AggAccumulator::SumFloat { sum: ds, count: dc },
            AggAccumulator::SumFloat { sum: ss, count: sc },
        ) => {
            *ds += ss;
            *dc += sc;
        }
        _ => {}
    }
}

/// Accumulate aggregate results from segment metadata (no decompression).
pub(super) fn accumulate_segment_metadata(
    accumulators: &mut [AggAccumulator],
    seg: &SegmentData,
    agg_specs: &[AggExecSpec],
    meta: &MetadataInfo,
) {
    for (i, spec) in agg_specs.iter().enumerate() {
        match spec.agg_type {
            AggType::CountStar => {
                if let AggAccumulator::Count { count } = &mut accumulators[i] {
                    *count += seg.row_count as i64;
                }
            }
            AggType::Count => {
                let col_name = &meta.col_names[spec.col_idx as usize];
                if let Some(cs) = seg.col_sums.get(col_name)
                    && let AggAccumulator::Count { count } = &mut accumulators[i]
                {
                    *count += cs.nonnull_count;
                }
            }
            AggType::Sum | AggType::Avg => {
                let col_name = &meta.col_names[spec.col_idx as usize];
                if let Some(cs) = seg.col_sums.get(col_name) {
                    if cs.sum_null {
                        continue;
                    }
                    let add_const = if spec.expr_kind == AggExpr::AddConst {
                        spec.const_offset
                    } else {
                        0
                    };
                    match &mut accumulators[i] {
                        AggAccumulator::SumInt { sum, count } => {
                            let v = if let Some(v) = cs.sum_i128 {
                                v
                            } else {
                                let s = unsafe {
                                    let cstr = pg_sys::OidOutputFunctionCall(
                                        pg_sys::Oid::from(1702u32), // numeric_out
                                        cs.sum_datum,
                                    );
                                    let s = std::ffi::CStr::from_ptr(cstr)
                                        .to_string_lossy()
                                        .into_owned();
                                    pg_sys::pfree(cstr as *mut _);
                                    s
                                };
                                match s.parse::<i128>() {
                                    Ok(v) => v,
                                    Err(_) => continue,
                                }
                            };
                            *sum += v + add_const as i128 * cs.nonnull_count as i128;
                            *count += cs.nonnull_count;
                        }
                        AggAccumulator::SumFloat { sum, count } => {
                            let f = if let Some(v) = cs.sum_i128 {
                                v as f64
                            } else if let Some(v) = cs.sum_f64 {
                                v
                            } else if cs.type_oid == pg_sys::NUMERICOID {
                                // Normalized colstats stores all sums as NUMERIC
                                let s = unsafe {
                                    let cstr = pg_sys::OidOutputFunctionCall(
                                        pg_sys::Oid::from(1702u32), // numeric_out
                                        cs.sum_datum,
                                    );
                                    let s = std::ffi::CStr::from_ptr(cstr)
                                        .to_string_lossy()
                                        .into_owned();
                                    pg_sys::pfree(cstr as *mut _);
                                    s
                                };
                                match s.parse::<f64>() {
                                    Ok(v) => v,
                                    Err(_) => continue,
                                }
                            } else {
                                // Legacy wide meta table: sum stored as FLOAT8
                                f64::from_bits(cs.sum_datum.value() as u64)
                            };
                            *sum += f + add_const as f64 * cs.nonnull_count as f64;
                            *count += cs.nonnull_count;
                        }
                        _ => {}
                    }
                }
            }
            AggType::Min => {
                let col_name = &meta.col_names[spec.col_idx as usize];
                if let Some(cm) = seg.col_minmax.get(col_name) {
                    if cm.min_null {
                        continue;
                    }
                    match &mut accumulators[i] {
                        AggAccumulator::MinInt { val } => {
                            // Convert from colstats encoding to PG-native datum domain
                            let v = decode_encoded_to_pg_i64(cm.min_encoded, cm.type_oid);
                            *val = Some(val.map_or(v, |cur| cur.min(v)));
                        }
                        AggAccumulator::MinFloat { val } => {
                            let v = if cm.type_oid == pg_sys::FLOAT4OID {
                                decode_i64_to_f32(cm.min_encoded) as f64
                            } else {
                                decode_i64_to_f64(cm.min_encoded)
                            };
                            *val = Some(val.map_or(v, |cur| if v < cur { v } else { cur }));
                        }
                        _ => {}
                    }
                }
            }
            AggType::Max => {
                let col_name = &meta.col_names[spec.col_idx as usize];
                if let Some(cm) = seg.col_minmax.get(col_name) {
                    if cm.max_null {
                        continue;
                    }
                    match &mut accumulators[i] {
                        AggAccumulator::MaxInt { val } => {
                            // Convert from colstats encoding to PG-native datum domain
                            let v = decode_encoded_to_pg_i64(cm.max_encoded, cm.type_oid);
                            *val = Some(val.map_or(v, |cur| cur.max(v)));
                        }
                        AggAccumulator::MaxFloat { val } => {
                            let v = if cm.type_oid == pg_sys::FLOAT4OID {
                                decode_i64_to_f32(cm.max_encoded) as f64
                            } else {
                                decode_i64_to_f64(cm.max_encoded)
                            };
                            *val = Some(val.map_or(v, |cur| if v > cur { v } else { cur }));
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
}

/// Decompress an ambiguous segment, apply batch quals, and accumulate agg results.
///
/// # Safety
///
/// Calls `decompress_blob_to_datums` which uses PG palloc-backed
/// memory for datums. Must run inside an active PG transaction.
pub(super) unsafe fn accumulate_segment_decompressed(
    accumulators: &mut [AggAccumulator],
    seg: &SegmentData,
    batch_quals: &[BatchQual],
    agg_specs: &[AggExecSpec],
    meta: &MetadataInfo,
) {
    let row_count = seg.row_count as usize;
    if row_count == 0 {
        return;
    }

    // Collect unique column indices needed (qual columns + agg columns)
    let mut col_indices: Vec<usize> = Vec::new();
    for bq in batch_quals {
        if !col_indices.contains(&bq.col_idx) {
            col_indices.push(bq.col_idx);
        }
    }
    for spec in agg_specs {
        if spec.col_idx >= 0 && !col_indices.contains(&(spec.col_idx as usize)) {
            col_indices.push(spec.col_idx as usize);
        }
    }

    // Decompress needed columns. Map col_idx → decompressed datums.
    // For columns added after this partition was compressed
    // (`meta.blob_idx[col_idx] == None && !segment_by`), synthesize from
    // `meta.missing_values[col_idx]` — a single constant Datum repeated
    // `row_count` times so qual evaluation downstream behaves identically
    // to a real decompressed column.
    let mut decompressed: HashMap<usize, Vec<(pg_sys::Datum, bool)>> = HashMap::new();
    for &col_idx in &col_indices {
        let col_name = &meta.col_names[col_idx];
        if meta.segment_by.contains(col_name) {
            continue; // segment_by columns are not in compressed_blobs
        }
        match meta.blob_idx.get(col_idx).copied().flatten() {
            Some(slot) => {
                let blob = &seg.compressed_blobs[slot as usize];
                let data_type = pg_type_name(meta.col_types[col_idx]);
                let typmod = meta.col_typmods[col_idx];
                let datums = unsafe {
                    decompress_blob_to_datums(blob, &data_type, meta.col_types[col_idx], typmod)
                };
                decompressed.insert(col_idx, datums);
            }
            None => {
                let (datum, is_null) = meta
                    .missing_values
                    .get(col_idx)
                    .copied()
                    .flatten()
                    .unwrap_or((pg_sys::Datum::from(0usize), true));
                let datums: Vec<(pg_sys::Datum, bool)> =
                    (0..row_count).map(|_| (datum, is_null)).collect();
                decompressed.insert(col_idx, datums);
            }
        }
    }

    // Build selection vector from batch quals
    let mut sel = vec![true; row_count];
    for bq in batch_quals {
        if let Some(col) = decompressed.get(&bq.col_idx) {
            if col.is_empty() {
                continue;
            }
            if bq.op == BatchCompareOp::InList {
                if let Some(ref values) = bq.in_list_i64 {
                    apply_batch_filter_in_list(col, &mut sel, values, bq.type_oid);
                }
                continue;
            }
            match bq.type_oid {
                pg_sys::INT8OID | pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID => {
                    apply_batch_filter_i64(col, &mut sel, bq.op, bq.const_datum.value() as i64);
                }
                pg_sys::INT4OID | pg_sys::DATEOID => {
                    apply_batch_filter_i32(col, &mut sel, bq.op, bq.const_datum.value() as i32);
                }
                pg_sys::INT2OID => {
                    apply_batch_filter_i16(col, &mut sel, bq.op, bq.const_datum.value() as i16);
                }
                pg_sys::FLOAT8OID => {
                    let c = f64::from_bits(bq.const_datum.value() as u64);
                    apply_batch_filter_f64(col, &mut sel, bq.op, c);
                }
                pg_sys::FLOAT4OID => {
                    let c = f32::from_bits(bq.const_datum.value() as u32);
                    apply_batch_filter_f32(col, &mut sel, bq.op, c);
                }
                _ => {}
            }
        }
    }

    // Accumulate from filtered rows
    for (i, spec) in agg_specs.iter().enumerate() {
        match spec.agg_type {
            AggType::CountStar => {
                if let AggAccumulator::Count { count } = &mut accumulators[i] {
                    *count += sel.iter().filter(|&&s| s).count() as i64;
                }
            }
            AggType::Count => {
                if let AggAccumulator::Count { count } = &mut accumulators[i]
                    && let Some(col) = decompressed.get(&(spec.col_idx as usize))
                {
                    for (j, &selected) in sel.iter().enumerate() {
                        if selected && j < col.len() && !col[j].1 {
                            *count += 1;
                        }
                    }
                }
            }
            AggType::Sum | AggType::Avg => {
                let add_const = if spec.expr_kind == AggExpr::AddConst {
                    spec.const_offset
                } else {
                    0
                };
                if let Some(col) = decompressed.get(&(spec.col_idx as usize)) {
                    match &mut accumulators[i] {
                        AggAccumulator::SumInt { sum, count } => {
                            for (j, &selected) in sel.iter().enumerate() {
                                if selected && j < col.len() && !col[j].1 {
                                    let v = col[j].0.value() as i64;
                                    *sum += v as i128 + add_const as i128;
                                    *count += 1;
                                }
                            }
                        }
                        AggAccumulator::SumFloat { sum, count } => {
                            for (j, &selected) in sel.iter().enumerate() {
                                if selected && j < col.len() && !col[j].1 {
                                    let v = if spec.col_type_oid == pg_sys::FLOAT4OID {
                                        f32::from_bits(col[j].0.value() as u32) as f64
                                    } else {
                                        f64::from_bits(col[j].0.value() as u64)
                                    };
                                    *sum += v + add_const as f64;
                                    *count += 1;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {} // Min/Max/CountDistinct not supported with WHERE in this path
        }
    }
}

/// Load metadata via SPI for a `DeltaXAgg` plan. Returns the
/// `MetadataInfo` plus elapsed micros for the `metadata_us` timer.
///
/// Phase-A extraction from `begin_agg_scan`. The same metadata structure
/// (`col_names`, `col_types`, `col_typmods`, `segment_by`, `time_column`)
/// is what Phase C's `init_worker_deltax_agg` will hydrate from
/// `append_wire` instead of running SPI a second time per worker.
///
/// # Safety
///
/// Calls `load_metadata` which runs SPI queries against PG catalogs.
/// Must run inside an active PG transaction with SPI initialised.
pub(super) unsafe fn load_agg_metadata_from_plan(
    companion_oids: &[pg_sys::Oid],
) -> (MetadataInfo, u64) {
    unsafe {
        if companion_oids.is_empty() {
            pgrx::error!("pg_deltax: load_agg_metadata_from_plan called with empty oids");
        }
        let first_name = {
            let name_ptr = pg_sys::get_rel_name(companion_oids[0]);
            if name_ptr.is_null() {
                pgrx::error!(
                    "pg_deltax: companion table not found for OID {}",
                    u32::from(companion_oids[0])
                );
            }
            std::ffi::CStr::from_ptr(name_ptr)
                .to_string_lossy()
                .into_owned()
        };
        let t0 = Instant::now();
        let meta = Spi::connect(|client| load_metadata(client, &first_name));
        let metadata_us = t0.elapsed().as_micros() as u64;
        (meta, metadata_us)
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::super::super::segments::{ColMinMax, ColSum};
    use super::super::state::{
        AggExpr, AggType, GroupByColSpec, GroupByExpr, HavingFilter, HavingOp,
    };
    use super::super::test_utils::{make_agg_spec, make_empty_segment, make_meta, make_plan};
    use super::{try_catalog_shortcut, try_metadata_fast_path};
    use pgrx::pg_sys;
    use pgrx::prelude::*;

    // -------------------------------------------------------------------
    // try_catalog_shortcut tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_catalog_shortcut_rejects_group_by() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            vec![GroupByColSpec {
                col_idx: 0,
                type_oid: pg_sys::Oid::from(23u32),
                expr: GroupByExpr::Column,
            }],
            Vec::new(),
            true,
        );
        assert!(try_catalog_shortcut(&plan, &meta, &[], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_rejects_where() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            Vec::new(),
            false,
        );
        assert!(try_catalog_shortcut(&plan, &meta, &[], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_rejects_having() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            vec![HavingFilter {
                agg_idx: 0,
                op: HavingOp::Gt,
                const_val: 10,
            }],
            true,
        );
        assert!(try_catalog_shortcut(&plan, &meta, &[], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_rejects_sum() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        assert!(try_catalog_shortcut(&plan, &meta, &[Some(100)], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_rejects_avg() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Avg, 1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        assert!(try_catalog_shortcut(&plan, &meta, &[Some(100)], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_rejects_min_max() {
        let meta = make_meta(&["ts", "value"]);
        for agg_type in [AggType::Min, AggType::Max] {
            let plan = make_plan(
                vec![make_agg_spec(agg_type, 1, 23)],
                Vec::new(),
                Vec::new(),
                true,
            );
            assert!(try_catalog_shortcut(&plan, &meta, &[Some(100)], 0).is_none());
        }
    }

    #[pg_test]
    fn test_catalog_shortcut_count_star_single_partition() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let state = try_catalog_shortcut(&plan, &meta, &[Some(42_000)], 0).unwrap();
        assert_eq!(state.result_rows.len(), 1);
        assert_eq!(state.result_rows[0][0].0.value(), 42_000usize);
        assert!(!state.result_rows[0][0].1); // not null
    }

    #[pg_test]
    fn test_catalog_shortcut_count_star_multi_partition() {
        let meta = make_meta(&["ts", "value"]);
        let mut plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        plan.companion_oids = vec![
            pg_sys::Oid::from(1u32),
            pg_sys::Oid::from(2u32),
            pg_sys::Oid::from(3u32),
        ];
        let state =
            try_catalog_shortcut(&plan, &meta, &[Some(100), Some(200), Some(300)], 0).unwrap();
        assert_eq!(state.result_rows[0][0].0.value(), 600usize);
    }

    #[pg_test]
    fn test_catalog_shortcut_count_star_missing_row_count() {
        // If any partition's row count is None, the shortcut fails
        let meta = make_meta(&["ts", "value"]);
        let mut plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        plan.companion_oids = vec![pg_sys::Oid::from(1u32), pg_sys::Oid::from(2u32)];
        assert!(try_catalog_shortcut(&plan, &meta, &[Some(100), None], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_count_distinct_falls_through() {
        // CountDistinct is no longer a catalog shortcut — always falls through
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountDistinct, 1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        assert!(try_catalog_shortcut(&plan, &meta, &[Some(1000)], 0).is_none());
    }

    // -------------------------------------------------------------------
    // try_metadata_fast_path tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_metadata_fast_path_rejects_group_by() {
        let meta = make_meta(&["ts", "value"]);
        let mut plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        plan.group_specs = vec![GroupByColSpec {
            col_idx: 0,
            type_oid: pg_sys::Oid::from(23u32),
            expr: GroupByExpr::Column,
        }];
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_where() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 23)],
            Vec::new(),
            Vec::new(),
            false,
        );
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_having() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 23)],
            Vec::new(),
            vec![HavingFilter {
                agg_idx: 0,
                op: HavingOp::Gt,
                const_val: 5,
            }],
            true,
        );
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_count_distinct() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountDistinct, 1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_text_sum() {
        let meta = make_meta(&["ts", "name"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 25)],
            Vec::new(),
            Vec::new(),
            true,
        ); // TEXTOID=25
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_length_of_sum() {
        let meta = make_meta(&["ts", "name"]);
        let mut plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        plan.agg_specs[0].expr_kind = AggExpr::LengthOf;
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_count_star_empty() {
        // COUNT(*) with no segments → 0
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let state = try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).unwrap();
        assert_eq!(state.result_rows.len(), 1);
        assert_eq!(state.result_rows[0][0].0.value(), 0usize);
    }

    #[pg_test]
    fn test_metadata_fast_path_count_star() {
        // COUNT(*) sums row_count across segments
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let segs = vec![
            make_empty_segment(1000),
            make_empty_segment(2000),
            make_empty_segment(500),
        ];
        let state = try_metadata_fast_path(&plan, &meta, &segs, &[], 0, 0).unwrap();
        assert_eq!(state.result_rows[0][0].0.value(), 3500usize);
    }

    #[pg_test]
    fn test_metadata_fast_path_count_star_skips_zero_rows() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let segs = vec![
            make_empty_segment(100),
            make_empty_segment(0), // should be skipped
            make_empty_segment(50),
        ];
        let state = try_metadata_fast_path(&plan, &meta, &segs, &[], 0, 0).unwrap();
        assert_eq!(state.result_rows[0][0].0.value(), 150usize);
    }

    #[pg_test]
    fn test_metadata_fast_path_min_int() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Min, 1, 20)],
            Vec::new(),
            Vec::new(),
            true,
        ); // INT8OID=20
        let mut seg1 = make_empty_segment(100);
        seg1.col_minmax.insert(
            "value".to_string(),
            ColMinMax {
                min_encoded: 50i64,
                max_encoded: 200i64,
                min_null: false,
                max_null: false,
                type_oid: pg_sys::Oid::from(20u32),
            },
        );
        let mut seg2 = make_empty_segment(100);
        seg2.col_minmax.insert(
            "value".to_string(),
            ColMinMax {
                min_encoded: 10i64,
                max_encoded: 300i64,
                min_null: false,
                max_null: false,
                type_oid: pg_sys::Oid::from(20u32),
            },
        );
        let state = try_metadata_fast_path(&plan, &meta, &[seg1, seg2], &[], 0, 0).unwrap();
        let result = state.result_rows[0][0].0.value() as i64;
        assert_eq!(result, 10);
    }

    #[pg_test]
    fn test_metadata_fast_path_max_int() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Max, 1, 20)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let mut seg1 = make_empty_segment(100);
        seg1.col_minmax.insert(
            "value".to_string(),
            ColMinMax {
                min_encoded: 10i64,
                max_encoded: 200i64,
                min_null: false,
                max_null: false,
                type_oid: pg_sys::Oid::from(20u32),
            },
        );
        let mut seg2 = make_empty_segment(100);
        seg2.col_minmax.insert(
            "value".to_string(),
            ColMinMax {
                min_encoded: 5i64,
                max_encoded: 999i64,
                min_null: false,
                max_null: false,
                type_oid: pg_sys::Oid::from(20u32),
            },
        );
        let state = try_metadata_fast_path(&plan, &meta, &[seg1, seg2], &[], 0, 0).unwrap();
        let result = state.result_rows[0][0].0.value() as i64;
        assert_eq!(result, 999);
    }

    #[pg_test]
    fn test_metadata_fast_path_min_skips_null() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Min, 1, 20)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let mut seg1 = make_empty_segment(100);
        seg1.col_minmax.insert(
            "value".to_string(),
            ColMinMax {
                min_encoded: 0i64,
                max_encoded: 0i64,
                min_null: true, // all nulls in this segment
                max_null: true,
                type_oid: pg_sys::Oid::from(20u32),
            },
        );
        let mut seg2 = make_empty_segment(100);
        seg2.col_minmax.insert(
            "value".to_string(),
            ColMinMax {
                min_encoded: 77i64,
                max_encoded: 77i64,
                min_null: false,
                max_null: false,
                type_oid: pg_sys::Oid::from(20u32),
            },
        );
        let state = try_metadata_fast_path(&plan, &meta, &[seg1, seg2], &[], 0, 0).unwrap();
        let result = state.result_rows[0][0].0.value() as i64;
        assert_eq!(result, 77);
    }

    #[pg_test]
    fn test_metadata_fast_path_missing_minmax_metadata() {
        // If a segment doesn't have minmax metadata for the needed column, fall through
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Min, 1, 20)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let seg = make_empty_segment(100); // no col_minmax
        assert!(try_metadata_fast_path(&plan, &meta, &[seg], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_missing_sum_metadata() {
        // If a segment doesn't have sum metadata for a SUM column, fall through
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 20)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let seg = make_empty_segment(100); // no col_sums
        assert!(try_metadata_fast_path(&plan, &meta, &[seg], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_count_with_nonnull() {
        // COUNT(col) reads nonnull_count from ColSum
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Count, 1, 20)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let mut seg1 = make_empty_segment(1000);
        seg1.col_sums.insert(
            "value".to_string(),
            ColSum {
                sum_datum: pg_sys::Datum::from(0usize),
                sum_null: true,
                sum_i128: None,
                sum_f64: None,
                nonnull_count: 900,
                nonzero_count: -1,
                type_oid: pg_sys::Oid::from(1700u32),
            },
        );
        let mut seg2 = make_empty_segment(500);
        seg2.col_sums.insert(
            "value".to_string(),
            ColSum {
                sum_datum: pg_sys::Datum::from(0usize),
                sum_null: true,
                sum_i128: None,
                sum_f64: None,
                nonnull_count: 450,
                nonzero_count: -1,
                type_oid: pg_sys::Oid::from(1700u32),
            },
        );
        let state = try_metadata_fast_path(&plan, &meta, &[seg1, seg2], &[], 0, 0).unwrap();
        assert_eq!(state.result_rows[0][0].0.value() as i64, 1350);
    }

    #[pg_test]
    fn test_metadata_fast_path_sum_float() {
        // SUM on float column: reads sum_datum as f64 bits
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 701)], // FLOAT8OID=701
            Vec::new(),
            Vec::new(),
            true,
        );
        let sum_val: f64 = 123.5;
        let mut seg = make_empty_segment(100);
        seg.col_sums.insert(
            "value".to_string(),
            ColSum {
                sum_datum: pg_sys::Datum::from(sum_val.to_bits() as usize),
                sum_null: false,
                sum_i128: None,
                sum_f64: None,
                nonnull_count: 100,
                nonzero_count: -1,
                type_oid: pg_sys::Oid::from(701u32),
            },
        );
        let state = try_metadata_fast_path(&plan, &meta, &[seg], &[], 0, 0).unwrap();
        let result_bits = state.result_rows[0][0].0.value() as u64;
        let result = f64::from_bits(result_bits);
        assert!((result - 123.5).abs() < 1e-10);
    }

    #[pg_test]
    fn test_metadata_fast_path_sum_int_via_numeric() {
        // SUM on integer column: sum_datum is a NUMERIC, needs PG numeric_out to parse.
        // Build a real NUMERIC datum via SPI.
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 20)], // INT8OID=20
            Vec::new(),
            Vec::new(),
            true,
        );
        // Create a real NUMERIC datum for the value 42000 via numeric_in
        let numeric_datum: pg_sys::Datum = unsafe {
            let s = std::ffi::CString::new("42000").unwrap();
            pg_sys::OidFunctionCall3Coll(
                pg_sys::Oid::from(1701u32), // numeric_in
                pg_sys::InvalidOid,
                pg_sys::Datum::from(s.as_ptr()),
                pg_sys::Datum::from(0usize),
                pg_sys::Datum::from(-1i32 as usize),
            )
        };
        let mut seg = make_empty_segment(100);
        seg.col_sums.insert(
            "value".to_string(),
            ColSum {
                sum_datum: numeric_datum,
                sum_null: false,
                sum_i128: None,
                sum_f64: None,
                nonnull_count: 100,
                nonzero_count: -1,
                type_oid: pg_sys::Oid::from(1700u32),
            },
        );
        let state = try_metadata_fast_path(&plan, &meta, &[seg], &[], 0, 0).unwrap();
        // SumInt finalized: returns NUMERIC datum — verify via numeric_out
        let result_datum = state.result_rows[0][0].0;
        let s = unsafe {
            let cstr = pg_sys::OidOutputFunctionCall(pg_sys::Oid::from(1702u32), result_datum);
            let s = std::ffi::CStr::from_ptr(cstr)
                .to_string_lossy()
                .into_owned();
            pg_sys::pfree(cstr as *mut _);
            s
        };
        assert_eq!(s.as_str(), "42000");
    }

    #[pg_test]
    fn test_metadata_fast_path_sum_add_const() {
        // SUM(col + 10): should compute SUM(col) + 10 * nonnull_count
        let meta = make_meta(&["ts", "value"]);
        let mut spec = make_agg_spec(AggType::Sum, 1, 701); // FLOAT8OID
        spec.expr_kind = AggExpr::AddConst;
        spec.const_offset = 10;
        let plan = make_plan(vec![spec], Vec::new(), Vec::new(), true);
        let base_sum: f64 = 100.0;
        let mut seg = make_empty_segment(50);
        seg.col_sums.insert(
            "value".to_string(),
            ColSum {
                sum_datum: pg_sys::Datum::from(base_sum.to_bits() as usize),
                sum_null: false,
                sum_i128: None,
                sum_f64: None,
                nonnull_count: 50,
                nonzero_count: -1,
                type_oid: pg_sys::Oid::from(701u32),
            },
        );
        let state = try_metadata_fast_path(&plan, &meta, &[seg], &[], 0, 0).unwrap();
        let result = f64::from_bits(state.result_rows[0][0].0.value() as u64);
        // Expected: 100.0 + 10 * 50 = 600.0
        assert!((result - 600.0).abs() < 1e-10);
    }

    #[pg_test]
    fn test_metadata_fast_path_reports_segment_count() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let segs = vec![make_empty_segment(10), make_empty_segment(20)];
        let state = try_metadata_fast_path(&plan, &meta, &segs, &[], 123, 456).unwrap();
        assert_eq!(state.total_segments, 2);
        assert_eq!(state.metadata_us, 123);
        assert_eq!(state.heap_scan_us, 456);
    }
}
