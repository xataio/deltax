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
    pub(crate) is_min: bool,
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

        // Parse companion OIDs (before sentinel -1)
        let mut companion_oids: Vec<pg_sys::Oid> = Vec::new();
        for i in 0..list_len {
            let val = pg_sys::list_nth_int(custom_private, i);
            if val == -1 {
                break;
            }
            companion_oids.push(pg_sys::Oid::from(val as u32));
        }

        if companion_oids.is_empty() {
            pgrx::error!("pg_deltax: DeltaXCount has no companion tables");
        }

        // Fast path: read row counts from deltax_partition catalog.
        let t0 = Instant::now();
        let mut catalog_total: i64 = 0;
        let mut catalog_hit = true;
        for &oid in &companion_oids {
            match super::super::cost::get_row_count(oid) {
                Some(rc) => catalog_total += rc,
                None => {
                    catalog_hit = false;
                    break;
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
                    &[],
                    None,
                    None,
                    None,
                    &[],
                    false,
                    &meta.col_types,
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
struct ExecAggSpec {
    is_min: bool,
    varattno: i32,
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

        // Parse custom_private: [oid1, ..., -1, num_aggs, is_min_0, varattno_0, ...]
        let mut companion_oids: Vec<pg_sys::Oid> = Vec::new();
        let mut agg_specs: Vec<ExecAggSpec> = Vec::new();
        let mut found_sentinel = false;
        let mut num_aggs: i32 = 0;
        let mut after_sentinel_idx = 0;
        let mut current_fields: Vec<i32> = Vec::new();

        for i in 0..list_len {
            let val = pg_sys::list_nth_int(custom_private, i);
            if !found_sentinel {
                if val == -1 {
                    found_sentinel = true;
                    continue;
                }
                companion_oids.push(pg_sys::Oid::from(val as u32));
            } else {
                if after_sentinel_idx == 0 {
                    num_aggs = val;
                    after_sentinel_idx += 1;
                    continue;
                }
                current_fields.push(val);
                if current_fields.len() == 2 {
                    agg_specs.push(ExecAggSpec {
                        is_min: current_fields[0] != 0,
                        varattno: current_fields[1],
                    });
                    current_fields.clear();
                }
                after_sentinel_idx += 1;
            }
        }
        let _ = num_aggs;

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

        // Resolve varattno → column name for each aggregate
        let agg_col_names: Vec<String> = agg_specs
            .iter()
            .map(|spec| {
                let idx = (spec.varattno - 1) as usize;
                if idx < meta.col_names.len() {
                    meta.col_names[idx].clone()
                } else {
                    pgrx::error!("pg_deltax: DeltaXMinMax varattno {} out of range", spec.varattno);
                }
            })
            .collect();

        // Build needed_cols as all-false (no columns needed for MIN/MAX metadata)
        let num_cols = meta.col_names.len();
        let needed_cols = vec![false; num_cols];

        // Load segments from all companion tables and find global min/max per aggregate
        super::segments::reset_scan_buf_stats();
        let t1 = Instant::now();
        let mut results: Vec<MinMaxResult> = agg_specs
            .iter()
            .zip(agg_col_names.iter())
            .map(|(spec, col_name)| MinMaxResult {
                datum: pg_sys::Datum::from(0usize),
                is_null: true,
                col_name: col_name.clone(),
                is_min: spec.is_min,
                type_oid: pg_sys::InvalidOid,
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
                &[],
                None,
                None,
                None,
                &[],
                false,
                &meta.col_types,
            );
            for seg in &segs {
                for (agg_idx, result) in results.iter_mut().enumerate() {
                    if let Some(cm) = seg.col_minmax.get(&agg_col_names[agg_idx]) {
                        let seg_encoded = if result.is_min { cm.min_encoded } else { cm.max_encoded };
                        let seg_null = if result.is_min { cm.min_null } else { cm.max_null };

                        if seg_null {
                            continue;
                        }

                        // Update type_oid from companion metadata
                        if result.type_oid == pg_sys::InvalidOid {
                            result.type_oid = cm.type_oid;
                        }

                        // Decode encoded i64 back to native Datum
                        let seg_datum = decode_encoded_to_datum(seg_encoded, cm.type_oid);

                        if result.is_null {
                            result.datum = seg_datum;
                            result.is_null = false;
                        } else {
                            let cmp = compare_datums(seg_datum, result.datum, cm.type_oid);
                            let dominated = if result.is_min {
                                cmp == std::cmp::Ordering::Less
                            } else {
                                cmp == std::cmp::Ordering::Greater
                            };
                            if dominated {
                                result.datum = seg_datum;
                            }
                        }
                    }
                }
            }
            total_segments += segs.len() as u64;
        }
        let heap_scan_us = t1.elapsed().as_micros() as u64;
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
            let agg_parts: Vec<String> = state.results.iter().map(|r| {
                let agg_name = if r.is_min { "MIN" } else { "MAX" };
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
