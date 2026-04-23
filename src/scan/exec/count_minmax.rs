use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::pg_guard;

use std::time::Instant;

use super::{DELTAX_COUNT_EXEC_METHODS, DELTAX_MINMAX_EXEC_METHODS};
use super::segments::{load_metadata, load_segments_heap, ScanBufferStats};
use super::datum_utils::compare_datums;
use crate::compress::{decode_i64_to_f64, decode_i64_to_f32};

/// State for DeltaXCount (COUNT(*) pushdown).
pub(crate) struct CountScanState {
    pub(crate) total_count: i64,
    returned: bool,
    pub(crate) metadata_us: u64,
    pub(crate) heap_scan_us: u64,
    pub(crate) total_segments: u64,
    pub(crate) buf_stats: ScanBufferStats,
}

/// Result for one MIN/MAX aggregate in a multi-aggregate pushdown.
pub(crate) struct MinMaxResult {
    pub(crate) datum: pg_sys::Datum,
    pub(crate) is_null: bool,
    pub(crate) col_name: String,
    pub(crate) kind: crate::scan::path::MetaAggKind,
    #[allow(dead_code)]
    pub(crate) type_oid: pg_sys::Oid,
}

/// State for DeltaXMinMax (MIN/MAX pushdown on any column, multi-aggregate).
pub(crate) struct MinMaxScanState {
    /// Results: one per aggregate.
    pub(crate) results: Vec<MinMaxResult>,
    returned: bool,
    pub(crate) metadata_us: u64,
    pub(crate) heap_scan_us: u64,
    pub(crate) total_segments: u64,
    pub(crate) buf_stats: ScanBufferStats,
}

/// CreateCustomScanState callback for DeltaXCount.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn create_count_scan_state(
    cscan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    unsafe {
        let css = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScanState>())
            as *mut pg_sys::CustomScanState;

        (*css).ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
        (*css).methods = &DELTAX_COUNT_EXEC_METHODS.0;

        // Copy custom_private for use in BeginCustomScan
        (*css).custom_ps = (*cscan).custom_private;

        css as *mut pg_sys::Node
    }
}

/// BeginCustomScan callback for DeltaXCount: sum total row count across partitions.
///
/// Fast path: read per-partition `row_count` from `deltax_partition` catalog
/// (cached thread-locally in `cost::get_row_count`). Since compressed partitions
/// are read-only, the catalog value is exact. Segment count for EXPLAIN is
/// approximated via `pg_class.reltuples` on the meta relation (one row per segment).
///
/// Fallback: if any companion is missing from the catalog (invariant violation
/// or racing partition creation), fall back to the full meta-scan path.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn begin_count_scan(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: i32,
) {
    unsafe {
        let custom_private = (*node).custom_ps;
        if custom_private.is_null() {
            pgrx::error!("pg_deltax: missing companion table OIDs in DeltaXCount state");
        }

        let list_len = (*custom_private).length;

        // Parse custom_private: [oid1, ..., -1, qual_bytes_len, bytes...]
        let mut companion_oids: Vec<pg_sys::Oid> = Vec::new();
        let mut qual_bytes: Vec<u8> = Vec::new();
        let mut idx: i32 = 0;
        while idx < list_len {
            let val = pg_sys::list_nth_int(custom_private, idx);
            idx += 1;
            if val == -1 {
                break;
            }
            companion_oids.push(pg_sys::Oid::from(val as u32));
        }
        if idx < list_len {
            let qlen = pg_sys::list_nth_int(custom_private, idx);
            idx += 1;
            for _ in 0..qlen {
                qual_bytes.push(pg_sys::list_nth_int(custom_private, idx) as u8);
                idx += 1;
            }
        }

        if companion_oids.is_empty() {
            pgrx::error!("pg_deltax: DeltaXCount has no companion tables");
        }

        // Fast path: no WHERE → sum `deltax_partition.row_count` from
        // the catalog (one lookup per companion, no segment I/O).
        let t0 = Instant::now();
        let mut catalog_total: i64 = 0;
        let mut catalog_hit = qual_bytes.is_empty();
        if catalog_hit {
            for &oid in &companion_oids {
                match super::super::cost::get_row_count(oid) {
                    Some(rc) => catalog_total += rc,
                    None => {
                        catalog_hit = false;
                        break;
                    }
                }
            }
        }

        let state = if catalog_hit {
            // Approximate segment count from pg_class.reltuples (one row per segment).
            let mut total_segments: u64 = 0;
            for &oid in &companion_oids {
                let rt = super::super::cost::get_reltuples(oid);
                if rt > 0.0 {
                    total_segments += rt as u64;
                }
            }
            let metadata_us = t0.elapsed().as_micros() as u64;

            CountScanState {
                total_count: catalog_total,
                returned: false,
                metadata_us,
                heap_scan_us: 0,
                total_segments,
                buf_stats: ScanBufferStats::default(),
            }
        } else {
            // Fallback path: scan meta tables to sum per-segment row counts.
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

            let t_meta = Instant::now();
            let meta = Spi::connect(|client| load_metadata(client, &first_name));
            let metadata_us = t_meta.elapsed().as_micros() as u64;

            let num_cols = meta.col_names.len();
            let needed_cols = vec![false; num_cols];

            // If we have WHERE quals, extract time/segment-by filters
            // so `load_segments_heap` can prune segments by
            // `min_time`/`max_time` and `segment_values` — no decompress.
            let (seg_filters, time_min, time_max) = if qual_bytes.is_empty() {
                (Vec::new(), None, None)
            } else {
                let cstr = std::ffi::CString::new(qual_bytes.clone()).unwrap();
                let qual_list = pg_sys::stringToNode(cstr.as_ptr()) as *mut pg_sys::List;
                super::segments::extract_segment_filters(
                    qual_list, &meta.col_names, &meta.segment_by, &meta.time_column,
                )
            };

            super::segments::reset_scan_buf_stats();
            let t1 = Instant::now();
            let mut total_count: i64 = 0;
            let mut total_segments: u64 = 0;
            for &oid in &companion_oids {
                let (segs, _, _, _, _) = load_segments_heap(
                    oid,
                    &meta.col_names,
                    &meta.segment_by,
                    &needed_cols,
                    &meta.time_column,
                    false,
                    &seg_filters,
                    time_min,
                    time_max,
                    None,
                    &[],
                    &[],
                    &meta.col_types,
                    &[],
                );
                for seg in &segs {
                    total_count += seg.row_count as i64;
                }
                total_segments += segs.len() as u64;
            }
            let heap_scan_us = t1.elapsed().as_micros() as u64;
            let buf_stats = super::segments::take_scan_buf_stats();

            CountScanState {
                total_count,
                returned: false,
                metadata_us,
                heap_scan_us,
                total_segments,
                buf_stats,
            }
        };

        let state_box = Box::new(state);
        let state_ptr = Box::into_raw(state_box);
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}

/// ExecCustomScan callback for DeltaXCount: return one row with the count.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn exec_count_scan(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    unsafe {
        let scan_slot = (*node).ss.ss_ScanTupleSlot;
        let state = &mut *((*node).custom_ps as *mut CountScanState);

        if !state.returned {
            pg_sys::ExecClearTuple(scan_slot);
            (*scan_slot).tts_values.add(0).write(pg_sys::Datum::from(state.total_count as usize));
            (*scan_slot).tts_isnull.add(0).write(false);
            pg_sys::ExecStoreVirtualTuple(scan_slot);
            state.returned = true;
            return scan_slot;
        }

        // EOF — return empty slot
        pg_sys::ExecClearTuple(scan_slot);
        scan_slot
    }
}

/// EndCustomScan callback for DeltaXCount: cleanup state.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn end_count_scan(
    node: *mut pg_sys::CustomScanState,
) {
    unsafe {
        let state_ptr = (*node).custom_ps as *mut CountScanState;
        if !state_ptr.is_null() {
            let state = Box::from_raw(state_ptr);
            let total_us = state.metadata_us + state.heap_scan_us;
            pgrx::log!(
                "pg_deltax DeltaXCount timing: total={:.1}ms  metadata={:.1}ms  heap_scan={:.1}ms  | \
                 total_count={} segments={}",
                total_us as f64 / 1000.0,
                state.metadata_us as f64 / 1000.0,
                state.heap_scan_us as f64 / 1000.0,
                state.total_count,
                state.total_segments,
            );
            (*node).custom_ps = std::ptr::null_mut();
        }
    }
}

/// ReScanCustomScan callback for DeltaXCount: reset returned flag.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn rescan_count_scan(
    node: *mut pg_sys::CustomScanState,
) {
    unsafe {
        let state = &mut *((*node).custom_ps as *mut CountScanState);
        state.returned = false;
    }
}

/// CreateCustomScanState callback for DeltaXMinMax.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn create_minmax_scan_state(
    cscan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    unsafe {
        let css = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScanState>())
            as *mut pg_sys::CustomScanState;

        (*css).ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
        (*css).methods = &DELTAX_MINMAX_EXEC_METHODS.0;

        // Copy custom_private for use in BeginCustomScan
        (*css).custom_ps = (*cscan).custom_private;

        css as *mut pg_sys::Node
    }
}

/// Aggregate specification parsed from plan's custom_private at execution time.
pub(crate) struct ExecAggSpec {
    pub(crate) kind: crate::scan::path::MetaAggKind,
    pub(crate) varattno: i32,
    pub(crate) result_type_oid: pg_sys::Oid,
    pub(crate) col_type_oid: pg_sys::Oid,
    #[allow(dead_code)]
    pub(crate) typlen: i16,
    #[allow(dead_code)]
    pub(crate) typbyval: bool,
}

/// BeginCustomScan callback for DeltaXMinMax: load segment metadata and find global min/max.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn begin_minmax_scan(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: i32,
) {
    unsafe {
        let custom_private = (*node).custom_ps;
        if custom_private.is_null() {
            pgrx::error!("pg_deltax: missing companion table OIDs in DeltaXMinMax state");
        }

        let list_len = (*custom_private).length;

        // Parse custom_private: [oid1, ..., -1, num_aggs,
        //                        kind, varattno, result_type, col_type, typlen, typbyval (×num_aggs),
        //                        qual_bytes_len, qual_byte0, ...]
        let mut companion_oids: Vec<pg_sys::Oid> = Vec::new();
        let mut agg_specs: Vec<ExecAggSpec> = Vec::new();
        let mut qual_bytes: Vec<u8> = Vec::new();
        let mut idx: i32 = 0;

        // Companion OIDs until -1 sentinel.
        while idx < list_len {
            let val = pg_sys::list_nth_int(custom_private, idx);
            idx += 1;
            if val == -1 {
                break;
            }
            companion_oids.push(pg_sys::Oid::from(val as u32));
        }

        let num_aggs = pg_sys::list_nth_int(custom_private, idx);
        idx += 1;
        for _ in 0..num_aggs {
            let fields: Vec<i32> = (0..6)
                .map(|off| pg_sys::list_nth_int(custom_private, idx + off))
                .collect();
            idx += 6;
            agg_specs.push(ExecAggSpec {
                kind: crate::scan::path::MetaAggKind::from_i32(fields[0]),
                varattno: fields[1],
                result_type_oid: pg_sys::Oid::from(fields[2] as u32),
                col_type_oid: pg_sys::Oid::from(fields[3] as u32),
                typlen: fields[4] as i16,
                typbyval: fields[5] != 0,
            });
        }
        if idx < list_len {
            let qlen = pg_sys::list_nth_int(custom_private, idx);
            idx += 1;
            for _ in 0..qlen {
                qual_bytes.push(pg_sys::list_nth_int(custom_private, idx) as u8);
                idx += 1;
            }
        }

        if companion_oids.is_empty() {
            pgrx::error!("pg_deltax: DeltaXMinMax has no companion tables");
        }

        // Get first companion table name for metadata
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

        // Load metadata via SPI from first companion table
        let t0 = Instant::now();
        let meta = Spi::connect(|client| load_metadata(client, &first_name));
        let metadata_us = t0.elapsed().as_micros() as u64;

        // Resolve varattno → column name for MIN/MAX/SUM/COUNT(col) specs.
        // CountStar has varattno=0 and no column.
        use crate::scan::path::MetaAggKind;
        let agg_col_names: Vec<Option<String>> = agg_specs
            .iter()
            .map(|spec| {
                if matches!(spec.kind, MetaAggKind::CountStar) {
                    return None;
                }
                let idx = (spec.varattno - 1) as usize;
                if idx < meta.col_names.len() {
                    Some(meta.col_names[idx].clone())
                } else {
                    pgrx::error!("pg_deltax: DeltaXMinMax varattno {} out of range", spec.varattno);
                }
            })
            .collect();

        // Build needed_cols as all-false — MIN/MAX/SUM/COUNT all use
        // `col_minmax` / `col_sums` metadata, never decompress the blob.
        let num_cols = meta.col_names.len();
        let needed_cols = vec![false; num_cols];

        // Re-hydrate optional qual list for segment pruning (time-range
        // and segment-by equality). Empty bytes = no WHERE.
        let (seg_filters, time_min, time_max) = if qual_bytes.is_empty() {
            (Vec::new(), None, None)
        } else {
            let cstr = std::ffi::CString::new(qual_bytes.clone()).unwrap();
            let qual_list = pg_sys::stringToNode(cstr.as_ptr()) as *mut pg_sys::List;
            super::segments::extract_segment_filters(
                qual_list, &meta.col_names, &meta.segment_by, &meta.time_column,
            )
        };

        // List of cols we need `col_minmax` / `col_sums` populated for.
        let minmax_cols: Vec<String> = agg_specs.iter().zip(agg_col_names.iter())
            .filter_map(|(spec, name)| {
                if matches!(spec.kind, MetaAggKind::Min | MetaAggKind::Max) {
                    name.clone()
                } else { None }
            })
            .collect();
        let stats_cols: Vec<String> = agg_specs.iter().zip(agg_col_names.iter())
            .filter_map(|(spec, name)| {
                if matches!(spec.kind, MetaAggKind::Sum | MetaAggKind::CountCol) {
                    name.clone()
                } else { None }
            })
            .collect();
        super::segments::reset_scan_buf_stats();
        let t1 = Instant::now();

        // Per-spec accumulators. Indices match `agg_specs`.
        enum Acc {
            MinMax { datum: pg_sys::Datum, null: bool, type_oid: pg_sys::Oid, is_min: bool },
            SumI128 { acc: i128, seen: bool, result_oid: pg_sys::Oid },
            SumF64  { acc: f64, seen: bool, result_oid: pg_sys::Oid },
            Count { acc: i64 },
        }
        let mut accs: Vec<Acc> = agg_specs
            .iter()
            .map(|spec| match spec.kind {
                MetaAggKind::Min => Acc::MinMax {
                    datum: pg_sys::Datum::from(0usize), null: true,
                    type_oid: pg_sys::InvalidOid, is_min: true,
                },
                MetaAggKind::Max => Acc::MinMax {
                    datum: pg_sys::Datum::from(0usize), null: true,
                    type_oid: pg_sys::InvalidOid, is_min: false,
                },
                MetaAggKind::Sum => {
                    if matches!(spec.col_type_oid, pg_sys::FLOAT4OID | pg_sys::FLOAT8OID) {
                        Acc::SumF64 { acc: 0.0, seen: false, result_oid: spec.result_type_oid }
                    } else {
                        Acc::SumI128 { acc: 0, seen: false, result_oid: spec.result_type_oid }
                    }
                }
                MetaAggKind::CountCol | MetaAggKind::CountStar => Acc::Count { acc: 0 },
            })
            .collect();

        let mut total_segments: u64 = 0;
        for &oid in &companion_oids {
            let (segs, _, _, _, _) = load_segments_heap(
                oid,
                &meta.col_names,
                &meta.segment_by,
                &needed_cols,
                &meta.time_column,
                true,
                &seg_filters,
                time_min,
                time_max,
                None,
                &[],
                &stats_cols,
                &meta.col_types,
                &minmax_cols,
            );
            for seg in &segs {
                for (spec_idx, spec) in agg_specs.iter().enumerate() {
                    match &mut accs[spec_idx] {
                        Acc::MinMax { datum, null, type_oid, is_min } => {
                            let col_name = match &agg_col_names[spec_idx] {
                                Some(n) => n, None => continue,
                            };
                            let cm = match seg.col_minmax.get(col_name) {
                                Some(c) => c, None => continue,
                            };
                            let seg_encoded = if *is_min { cm.min_encoded } else { cm.max_encoded };
                            let seg_null = if *is_min { cm.min_null } else { cm.max_null };
                            if seg_null { continue; }
                            if *type_oid == pg_sys::InvalidOid { *type_oid = cm.type_oid; }
                            let seg_datum = decode_encoded_to_datum(seg_encoded, cm.type_oid);
                            if *null {
                                *datum = seg_datum;
                                *null = false;
                            } else {
                                let cmp = compare_datums(seg_datum, *datum, cm.type_oid);
                                let dominated = if *is_min {
                                    cmp == std::cmp::Ordering::Less
                                } else {
                                    cmp == std::cmp::Ordering::Greater
                                };
                                if dominated { *datum = seg_datum; }
                            }
                        }
                        Acc::SumI128 { acc, seen, .. } => {
                            let col_name = match &agg_col_names[spec_idx] {
                                Some(n) => n, None => continue,
                            };
                            if let Some(cs) = seg.col_sums.get(col_name)
                                && let Some(v) = cs.sum_i128
                            {
                                *acc = acc.saturating_add(v);
                                *seen = true;
                            }
                        }
                        Acc::SumF64 { acc, seen, .. } => {
                            let col_name = match &agg_col_names[spec_idx] {
                                Some(n) => n, None => continue,
                            };
                            if let Some(cs) = seg.col_sums.get(col_name)
                                && let Some(v) = cs.sum_f64
                            {
                                *acc += v;
                                *seen = true;
                            }
                        }
                        Acc::Count { acc } => {
                            match spec.kind {
                                MetaAggKind::CountStar => *acc += seg.row_count as i64,
                                MetaAggKind::CountCol => {
                                    let col_name = match &agg_col_names[spec_idx] {
                                        Some(n) => n, None => continue,
                                    };
                                    if let Some(cs) = seg.col_sums.get(col_name) {
                                        *acc += cs.nonnull_count;
                                    }
                                }
                                _ => unreachable!(),
                            }
                        }
                    }
                }
            }
            total_segments += segs.len() as u64;
        }
        let heap_scan_us = t1.elapsed().as_micros() as u64;

        // Convert accumulators into the legacy `MinMaxResult` row shape
        // expected by `exec_minmax_scan`. Emit one entry per spec in
        // target-list order; result tuple construction happens there.
        let results: Vec<MinMaxResult> = accs.into_iter()
            .zip(agg_specs.iter())
            .zip(agg_col_names.iter())
            .map(|((acc, spec), col_name)| {
                let col_name_str = col_name.clone().unwrap_or_else(|| "*".to_string());
                let kind = spec.kind;
                match acc {
                    Acc::MinMax { datum, null, type_oid, .. } => MinMaxResult {
                        datum, is_null: null, col_name: col_name_str,
                        kind, type_oid,
                    },
                    Acc::SumI128 { acc, seen, result_oid } => {
                        if !seen {
                            MinMaxResult {
                                datum: pg_sys::Datum::from(0usize), is_null: true,
                                col_name: col_name_str, kind, type_oid: result_oid,
                            }
                        } else {
                            let datum = sum_i128_to_datum(acc, result_oid, spec.col_type_oid);
                            MinMaxResult {
                                datum, is_null: false, col_name: col_name_str,
                                kind, type_oid: result_oid,
                            }
                        }
                    }
                    Acc::SumF64 { acc, seen, result_oid } => {
                        if !seen {
                            MinMaxResult {
                                datum: pg_sys::Datum::from(0usize), is_null: true,
                                col_name: col_name_str, kind, type_oid: result_oid,
                            }
                        } else {
                            // PG's sum(real) → real (FLOAT4); sum(double) → double (FLOAT8).
                            // Pack the accumulator into the bit-width PG expects.
                            let datum = match result_oid {
                                pg_sys::FLOAT4OID => {
                                    let f32_val = acc as f32;
                                    pg_sys::Datum::from(f32_val.to_bits() as usize)
                                }
                                _ => pg_sys::Datum::from(acc.to_bits() as usize),
                            };
                            MinMaxResult {
                                datum, is_null: false, col_name: col_name_str,
                                kind, type_oid: result_oid,
                            }
                        }
                    }
                    Acc::Count { acc } => MinMaxResult {
                        datum: pg_sys::Datum::from(acc as usize), is_null: false,
                        col_name: col_name_str, kind, type_oid: pg_sys::INT8OID,
                    },
                }
            })
            .collect();
        let buf_stats = super::segments::take_scan_buf_stats();

        let state = MinMaxScanState {
            results,
            returned: false,
            metadata_us,
            heap_scan_us,
            total_segments,
            buf_stats,
        };

        let state_box = Box::new(state);
        let state_ptr = Box::into_raw(state_box);
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}

/// ExecCustomScan callback for DeltaXMinMax: return one row with N min/max values.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn exec_minmax_scan(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    unsafe {
        let scan_slot = (*node).ss.ss_ScanTupleSlot;
        let state = &mut *((*node).custom_ps as *mut MinMaxScanState);

        if !state.returned {
            pg_sys::ExecClearTuple(scan_slot);
            for (i, result) in state.results.iter().enumerate() {
                (*scan_slot).tts_values.add(i).write(result.datum);
                (*scan_slot).tts_isnull.add(i).write(result.is_null);
            }
            pg_sys::ExecStoreVirtualTuple(scan_slot);
            state.returned = true;
            return scan_slot;
        }

        // EOF — return empty slot
        pg_sys::ExecClearTuple(scan_slot);
        scan_slot
    }
}

/// EndCustomScan callback for DeltaXMinMax: cleanup state.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn end_minmax_scan(
    node: *mut pg_sys::CustomScanState,
) {
    unsafe {
        let state_ptr = (*node).custom_ps as *mut MinMaxScanState;
        if !state_ptr.is_null() {
            let state = Box::from_raw(state_ptr);
            let total_us = state.metadata_us + state.heap_scan_us;
            use crate::scan::path::MetaAggKind;
            let agg_parts: Vec<String> = state.results.iter().map(|r| {
                let agg_name = match r.kind {
                    MetaAggKind::Min => "MIN",
                    MetaAggKind::Max => "MAX",
                    MetaAggKind::Sum => "SUM",
                    MetaAggKind::CountCol => "COUNT",
                    MetaAggKind::CountStar => "COUNT*",
                };
                format!("{}({})=null={}", agg_name, r.col_name, r.is_null)
            }).collect();
            pgrx::log!(
                "pg_deltax DeltaXMinMax timing: total={:.1}ms  metadata={:.1}ms  heap_scan={:.1}ms  | \
                 {} segments={}",
                total_us as f64 / 1000.0,
                state.metadata_us as f64 / 1000.0,
                state.heap_scan_us as f64 / 1000.0,
                agg_parts.join(", "),
                state.total_segments,
            );
            (*node).custom_ps = std::ptr::null_mut();
        }
    }
}

/// ReScanCustomScan callback for DeltaXMinMax: reset returned flag.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn rescan_minmax_scan(
    node: *mut pg_sys::CustomScanState,
) {
    unsafe {
        let state = &mut *((*node).custom_ps as *mut MinMaxScanState);
        state.returned = false;
    }
}

/// Decode an order-preserving i64 back to a native pg_sys::Datum for the given type OID.
///
/// Timestamps and dates are stored as Unix-epoch microseconds in the colstats table,
/// but PG expects PG-epoch microseconds for timestamps and PG-epoch days for dates.
fn decode_encoded_to_datum(encoded: i64, type_oid: pg_sys::Oid) -> pg_sys::Datum {
    match type_oid {
        pg_sys::FLOAT4OID => {
            let f = decode_i64_to_f32(encoded);
            pg_sys::Datum::from(f.to_bits() as usize)
        }
        pg_sys::FLOAT8OID => {
            let f = decode_i64_to_f64(encoded);
            pg_sys::Datum::from(f.to_bits() as usize)
        }
        pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID => {
            // Convert Unix-epoch usec → PG-epoch usec
            let pg_usec = encoded - crate::compress::PG_EPOCH_OFFSET_USEC;
            pg_sys::Datum::from(pg_usec as usize)
        }
        pg_sys::DATEOID => {
            // Convert Unix-epoch usec → PG-epoch days
            let pg_days = (encoded / 86_400_000_000) - crate::compress::PG_EPOCH_OFFSET_DAYS;
            pg_sys::Datum::from(pg_days as i32 as usize)
        }
        // INT2, INT4, INT8: identity encoding
        _ => pg_sys::Datum::from(encoded as usize),
    }
}

/// Convert a SUM accumulator (i128) into the PG Datum matching the
/// aggregate's declared result type.
///
/// Type table (PG spec):
/// - `SUM(int2)` → `int8` (bigint) — handled here
/// - `SUM(int4)` → `int8` — handled here
/// - `SUM(int8)` / `SUM(numeric)` → `numeric` — NOT handled here (hook
///   rejects them from the fast path; DeltaXAgg takes over). Reason:
///   building a NUMERIC from i128 requires `numeric_in(cstring, …)`
///   which pgrx's `PGFunction` pointer type can't be constructed for
///   our `pg_sys::numeric_in` binding without transmutes. Follow-up.
fn sum_i128_to_datum(acc: i128, result_oid: pg_sys::Oid, _col_oid: pg_sys::Oid) -> pg_sys::Datum {
    match result_oid {
        pg_sys::INT8OID => {
            // PG's sum(int2)/sum(int4) → int8. Overflow would be a
            // real bug; clamp via saturating accumulation upstream
            // and surface as ERROR if we exceed i64 range here.
            if acc > i64::MAX as i128 || acc < i64::MIN as i128 {
                pgrx::error!("pg_deltax: SUM overflow beyond bigint range");
            }
            pg_sys::Datum::from(acc as i64 as usize)
        }
        _ => pg_sys::Datum::from(acc as i64 as usize),
    }
}
