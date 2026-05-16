use pgrx::pg_guard;
use pgrx::pg_sys;
use pgrx::prelude::*;

use std::time::Instant;

use super::datum_utils::compare_datums;
use super::segments::{ScanBufferStats, load_metadata, load_segments_heap};
use super::{DELTAX_COUNT_EXEC_METHODS, DELTAX_MINMAX_EXEC_METHODS};
use crate::compress::{decode_i64_to_f32, decode_i64_to_f64};

/// Parse the leading `[oid1, ..., -1]` companion-OID block from
/// `custom_private`. Returns the OID list and the index of the first
/// element after the `-1` sentinel. Errors out (via pgrx::error!) if the
/// list is empty or the sentinel is missing.
unsafe fn parse_companion_oids(
    custom_private: *mut pg_sys::List,
    label: &str,
) -> (Vec<pg_sys::Oid>, i32) {
    unsafe {
        let list_len = (*custom_private).length;
        let mut companion_oids: Vec<pg_sys::Oid> = Vec::new();
        let mut idx: i32 = 0;
        while idx < list_len {
            let val = pg_sys::list_nth_int(custom_private, idx);
            idx += 1;
            if val == -1 {
                break;
            }
            companion_oids.push(pg_sys::Oid::from(val as u32));
        }
        if companion_oids.is_empty() {
            pgrx::error!("pg_deltax: {} has no companion tables", label);
        }
        (companion_oids, idx)
    }
}

/// Read the trailing `[qual_bytes_len, byte0, byte1, ...]` block from
/// `custom_private` starting at `idx`. Returns the raw bytes (empty when
/// the planner emitted no quals).
unsafe fn parse_trailing_qual_bytes(custom_private: *mut pg_sys::List, idx: i32) -> Vec<u8> {
    unsafe {
        let list_len = (*custom_private).length;
        if idx >= list_len {
            return Vec::new();
        }
        let qlen = pg_sys::list_nth_int(custom_private, idx);
        let mut bytes = Vec::with_capacity(qlen.max(0) as usize);
        for off in 0..qlen {
            bytes.push(pg_sys::list_nth_int(custom_private, idx + 1 + off) as u8);
        }
        bytes
    }
}

/// Look up a relation name by OID. Used by Begin* callbacks that derive
/// the table-name for `load_metadata`. Errors out if PG returns NULL
/// (relation dropped between planning and execution).
unsafe fn relation_name_or_error(oid: pg_sys::Oid) -> String {
    unsafe {
        let name_ptr = pg_sys::get_rel_name(oid);
        if name_ptr.is_null() {
            pgrx::error!(
                "pg_deltax: companion table not found for OID {}",
                u32::from(oid)
            );
        }
        std::ffi::CStr::from_ptr(name_ptr)
            .to_string_lossy()
            .into_owned()
    }
}

/// Decode `qual_bytes` (from `nodeToString` at plan time) back into a
/// PG List and run `extract_segment_filters` against it. Returns the
/// no-filter triple when the bytes are empty.
unsafe fn rehydrate_segment_filters(
    qual_bytes: &[u8],
    col_names: &[String],
    segment_by: &[String],
    time_column: &str,
) -> (Vec<(usize, String)>, Option<i64>, Option<i64>) {
    if qual_bytes.is_empty() {
        return (Vec::new(), None, None);
    }
    unsafe {
        let cstr = std::ffi::CString::new(qual_bytes.to_vec()).unwrap();
        let qual_list = pg_sys::stringToNode(cstr.as_ptr()) as *mut pg_sys::List;
        super::segments::extract_segment_filters(qual_list, col_names, segment_by, time_column)
    }
}

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

        // Wire format: [oid1, ..., -1, qual_bytes_len, bytes...]
        let (companion_oids, after_oids) = parse_companion_oids(custom_private, "DeltaXCount");
        let qual_bytes = parse_trailing_qual_bytes(custom_private, after_oids);

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
            let first_name = relation_name_or_error(companion_oids[0]);

            let t_meta = Instant::now();
            let meta = Spi::connect(|client| load_metadata(client, &first_name));
            let metadata_us = t_meta.elapsed().as_micros() as u64;

            let num_cols = meta.col_names.len();
            let needed_cols = vec![false; num_cols];

            // Decode trailing qual bytes back into a List + extract
            // time-range / segment-by filters so `load_segments_heap` can
            // prune by metadata alone (no decompress).
            let (seg_filters, time_min, time_max) = rehydrate_segment_filters(
                &qual_bytes,
                &meta.col_names,
                &meta.segment_by,
                &meta.time_column,
            );

            super::segments::reset_scan_buf_stats();
            let t1 = Instant::now();
            let mut total_count: i64 = 0;
            let mut total_segments: u64 = 0;
            for &oid in &companion_oids {
                let (segs, _, _, _, _, _) = load_segments_heap(
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
                    false,
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
            (*scan_slot)
                .tts_values
                .add(0)
                .write(pg_sys::Datum::from(state.total_count as usize));
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
pub(super) unsafe extern "C-unwind" fn end_count_scan(node: *mut pg_sys::CustomScanState) {
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
pub(super) unsafe extern "C-unwind" fn rescan_count_scan(node: *mut pg_sys::CustomScanState) {
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
    /// For `SUM(col + N)` the meta-path adds `const_offset * nonnull_count`
    /// to the metadata-derived sum at finalize. Zero for every other shape.
    pub(crate) const_offset: i64,
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

        // Wire format: [oid1, ..., -1, num_aggs,
        //               kind, varattno, result_type, col_type, typlen, typbyval,
        //               const_offset_lo, const_offset_hi  (×num_aggs),
        //               qual_bytes_len, qual_byte0, ...]
        let (companion_oids, after_oids) = parse_companion_oids(custom_private, "DeltaXMinMax");

        let mut agg_specs: Vec<ExecAggSpec> = Vec::new();
        let mut idx = after_oids;
        let num_aggs = pg_sys::list_nth_int(custom_private, idx);
        idx += 1;
        for _ in 0..num_aggs {
            let fields: Vec<i32> = (0..8)
                .map(|off| pg_sys::list_nth_int(custom_private, idx + off))
                .collect();
            idx += 8;
            // Reassemble i64 const_offset from two i32 halves (low, high).
            let const_offset = (fields[6] as u32 as i64) | ((fields[7] as i64) << 32);
            agg_specs.push(ExecAggSpec {
                kind: crate::scan::path::MetaAggKind::from_i32(fields[0]),
                varattno: fields[1],
                result_type_oid: pg_sys::Oid::from(fields[2] as u32),
                col_type_oid: pg_sys::Oid::from(fields[3] as u32),
                typlen: fields[4] as i16,
                typbyval: fields[5] != 0,
                const_offset,
            });
        }
        let qual_bytes = parse_trailing_qual_bytes(custom_private, idx);

        // Get first companion table name for metadata
        let first_name = relation_name_or_error(companion_oids[0]);

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
                    pgrx::error!(
                        "pg_deltax: DeltaXMinMax varattno {} out of range",
                        spec.varattno
                    );
                }
            })
            .collect();

        // Build needed_cols as all-false — MIN/MAX/SUM/COUNT all use
        // `col_minmax` / `col_sums` metadata, never decompress the blob.
        let num_cols = meta.col_names.len();
        let needed_cols = vec![false; num_cols];

        // Re-hydrate optional qual list for segment pruning (time-range
        // and segment-by equality). Empty bytes = no WHERE.
        let (seg_filters, time_min, time_max) = rehydrate_segment_filters(
            &qual_bytes,
            &meta.col_names,
            &meta.segment_by,
            &meta.time_column,
        );

        // List of cols we need `col_minmax` / `col_sums` populated for.
        let minmax_cols: Vec<String> = agg_specs
            .iter()
            .zip(agg_col_names.iter())
            .filter_map(|(spec, name)| {
                if matches!(spec.kind, MetaAggKind::Min | MetaAggKind::Max) {
                    name.clone()
                } else {
                    None
                }
            })
            .collect();
        let stats_cols: Vec<String> = agg_specs
            .iter()
            .zip(agg_col_names.iter())
            .filter_map(|(spec, name)| {
                if matches!(spec.kind, MetaAggKind::Sum | MetaAggKind::CountCol) {
                    name.clone()
                } else {
                    None
                }
            })
            .collect();
        super::segments::reset_scan_buf_stats();
        let t1 = Instant::now();

        // Per-spec accumulators. Indices match `agg_specs`.
        enum Acc {
            MinMax {
                datum: pg_sys::Datum,
                null: bool,
                type_oid: pg_sys::Oid,
                is_min: bool,
            },
            // `nonnull` tracks total non-null input rows for the offset-finalize
            // step (`sum(col + N) = sum(col) + N * nonnull`). Stays 0 for plain
            // SUM(col) since const_offset is 0 then — multiplication is a no-op.
            SumI128 {
                acc: i128,
                nonnull: i64,
                seen: bool,
                result_oid: pg_sys::Oid,
                const_offset: i64,
            },
            SumF64 {
                acc: f64,
                nonnull: i64,
                seen: bool,
                result_oid: pg_sys::Oid,
                const_offset: i64,
            },
            Count {
                acc: i64,
            },
        }
        let mut accs: Vec<Acc> = agg_specs
            .iter()
            .map(|spec| match spec.kind {
                MetaAggKind::Min => Acc::MinMax {
                    datum: pg_sys::Datum::from(0usize),
                    null: true,
                    type_oid: pg_sys::InvalidOid,
                    is_min: true,
                },
                MetaAggKind::Max => Acc::MinMax {
                    datum: pg_sys::Datum::from(0usize),
                    null: true,
                    type_oid: pg_sys::InvalidOid,
                    is_min: false,
                },
                MetaAggKind::Sum => {
                    if matches!(spec.col_type_oid, pg_sys::FLOAT4OID | pg_sys::FLOAT8OID) {
                        Acc::SumF64 {
                            acc: 0.0,
                            nonnull: 0,
                            seen: false,
                            result_oid: spec.result_type_oid,
                            const_offset: spec.const_offset,
                        }
                    } else {
                        Acc::SumI128 {
                            acc: 0,
                            nonnull: 0,
                            seen: false,
                            result_oid: spec.result_type_oid,
                            const_offset: spec.const_offset,
                        }
                    }
                }
                MetaAggKind::CountCol | MetaAggKind::CountStar => Acc::Count { acc: 0 },
            })
            .collect();

        let mut total_segments: u64 = 0;
        for &oid in &companion_oids {
            let (segs, _, _, _, _, _) = load_segments_heap(
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
                false,
            );
            for seg in &segs {
                for (spec_idx, spec) in agg_specs.iter().enumerate() {
                    match &mut accs[spec_idx] {
                        Acc::MinMax {
                            datum,
                            null,
                            type_oid,
                            is_min,
                        } => {
                            let col_name = match &agg_col_names[spec_idx] {
                                Some(n) => n,
                                None => continue,
                            };
                            let cm = match seg.col_minmax.get(col_name) {
                                Some(c) => c,
                                None => continue,
                            };
                            let seg_encoded = if *is_min {
                                cm.min_encoded
                            } else {
                                cm.max_encoded
                            };
                            let seg_null = if *is_min { cm.min_null } else { cm.max_null };
                            if seg_null {
                                continue;
                            }
                            if *type_oid == pg_sys::InvalidOid {
                                *type_oid = cm.type_oid;
                            }
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
                                if dominated {
                                    *datum = seg_datum;
                                }
                            }
                        }
                        Acc::SumI128 {
                            acc, nonnull, seen, ..
                        } => {
                            let col_name = match &agg_col_names[spec_idx] {
                                Some(n) => n,
                                None => continue,
                            };
                            if let Some(cs) = seg.col_sums.get(col_name)
                                && let Some(v) = cs.sum_i128
                            {
                                *acc = acc.saturating_add(v);
                                *nonnull = nonnull.saturating_add(cs.nonnull_count);
                                *seen = true;
                            }
                        }
                        Acc::SumF64 {
                            acc, nonnull, seen, ..
                        } => {
                            let col_name = match &agg_col_names[spec_idx] {
                                Some(n) => n,
                                None => continue,
                            };
                            if let Some(cs) = seg.col_sums.get(col_name)
                                && let Some(v) = cs.sum_f64
                            {
                                *acc += v;
                                *nonnull = nonnull.saturating_add(cs.nonnull_count);
                                *seen = true;
                            }
                        }
                        Acc::Count { acc } => match spec.kind {
                            MetaAggKind::CountStar => *acc += seg.row_count as i64,
                            MetaAggKind::CountCol => {
                                let col_name = match &agg_col_names[spec_idx] {
                                    Some(n) => n,
                                    None => continue,
                                };
                                if let Some(cs) = seg.col_sums.get(col_name) {
                                    *acc += cs.nonnull_count;
                                }
                            }
                            _ => unreachable!(),
                        },
                    }
                }
            }
            total_segments += segs.len() as u64;
        }
        let heap_scan_us = t1.elapsed().as_micros() as u64;

        // Convert accumulators into the legacy `MinMaxResult` row shape
        // expected by `exec_minmax_scan`. Emit one entry per spec in
        // target-list order; result tuple construction happens there.
        let results: Vec<MinMaxResult> = accs
            .into_iter()
            .zip(agg_specs.iter())
            .zip(agg_col_names.iter())
            .map(|((acc, spec), col_name)| {
                let col_name_str = col_name.clone().unwrap_or_else(|| "*".to_string());
                let kind = spec.kind;
                match acc {
                    Acc::MinMax {
                        datum,
                        null,
                        type_oid,
                        ..
                    } => MinMaxResult {
                        datum,
                        is_null: null,
                        col_name: col_name_str,
                        kind,
                        type_oid,
                    },
                    Acc::SumI128 {
                        acc,
                        nonnull,
                        seen,
                        result_oid,
                        const_offset,
                    } => {
                        if !seen {
                            MinMaxResult {
                                datum: pg_sys::Datum::from(0usize),
                                is_null: true,
                                col_name: col_name_str,
                                kind,
                                type_oid: result_oid,
                            }
                        } else {
                            // `sum(col + N) = sum(col) + N * nonnull`. const_offset
                            // is 0 for plain `SUM(col)`, so the add is free.
                            let shifted = acc.saturating_add(
                                (const_offset as i128).saturating_mul(nonnull as i128),
                            );
                            let datum = sum_i128_to_datum(shifted, result_oid, spec.col_type_oid);
                            MinMaxResult {
                                datum,
                                is_null: false,
                                col_name: col_name_str,
                                kind,
                                type_oid: result_oid,
                            }
                        }
                    }
                    Acc::SumF64 {
                        acc,
                        nonnull,
                        seen,
                        result_oid,
                        const_offset,
                    } => {
                        if !seen {
                            MinMaxResult {
                                datum: pg_sys::Datum::from(0usize),
                                is_null: true,
                                col_name: col_name_str,
                                kind,
                                type_oid: result_oid,
                            }
                        } else {
                            // Same offset shift as the i128 path, in f64. Float
                            // SUM is already non-associative across workers so
                            // this preserves the existing accuracy regime.
                            let shifted = acc + (const_offset as f64) * (nonnull as f64);
                            // PG's sum(real) → real (FLOAT4); sum(double) → double (FLOAT8).
                            // Pack the accumulator into the bit-width PG expects.
                            let datum = match result_oid {
                                pg_sys::FLOAT4OID => {
                                    let f32_val = shifted as f32;
                                    pg_sys::Datum::from(f32_val.to_bits() as usize)
                                }
                                _ => pg_sys::Datum::from(shifted.to_bits() as usize),
                            };
                            MinMaxResult {
                                datum,
                                is_null: false,
                                col_name: col_name_str,
                                kind,
                                type_oid: result_oid,
                            }
                        }
                    }
                    Acc::Count { acc } => MinMaxResult {
                        datum: pg_sys::Datum::from(acc as usize),
                        is_null: false,
                        col_name: col_name_str,
                        kind,
                        type_oid: pg_sys::INT8OID,
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
pub(super) unsafe extern "C-unwind" fn end_minmax_scan(node: *mut pg_sys::CustomScanState) {
    unsafe {
        let state_ptr = (*node).custom_ps as *mut MinMaxScanState;
        if !state_ptr.is_null() {
            let state = Box::from_raw(state_ptr);
            let total_us = state.metadata_us + state.heap_scan_us;
            use crate::scan::path::MetaAggKind;
            let agg_parts: Vec<String> = state
                .results
                .iter()
                .map(|r| {
                    let agg_name = match r.kind {
                        MetaAggKind::Min => "MIN",
                        MetaAggKind::Max => "MAX",
                        MetaAggKind::Sum => "SUM",
                        MetaAggKind::CountCol => "COUNT",
                        MetaAggKind::CountStar => "COUNT*",
                    };
                    format!("{}({})=null={}", agg_name, r.col_name, r.is_null)
                })
                .collect();
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
pub(super) unsafe extern "C-unwind" fn rescan_minmax_scan(node: *mut pg_sys::CustomScanState) {
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

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use crate::compress::{
        PG_EPOCH_OFFSET_DAYS, PG_EPOCH_OFFSET_USEC, encode_f32_to_i64, encode_f64_to_i64,
    };

    #[test]
    fn decode_encoded_to_datum_integer_identity() {
        // Integer-family OIDs round-trip without offset: storage matches
        // PG's native datum representation, so the encode is identity.
        for oid in [pg_sys::INT2OID, pg_sys::INT4OID, pg_sys::INT8OID] {
            let d = decode_encoded_to_datum(42, oid);
            assert_eq!(d.value() as i64, 42, "oid {:?}", oid);

            let d = decode_encoded_to_datum(-7, oid);
            // Cast back through i64 to interpret the unsigned datum as signed.
            assert_eq!(d.value() as i64, -7, "negative for oid {:?}", oid);
        }
    }

    #[test]
    fn decode_encoded_to_datum_timestamp_strips_pg_epoch_offset() {
        // Stored as Unix-epoch µs; PG datum expects PG-epoch µs.
        // `decode(unix_usec) = unix_usec - PG_EPOCH_OFFSET_USEC`.
        let unix_usec = PG_EPOCH_OFFSET_USEC + 1_000_000_000;
        let d = decode_encoded_to_datum(unix_usec, pg_sys::TIMESTAMPOID);
        assert_eq!(d.value() as i64, 1_000_000_000);

        let d = decode_encoded_to_datum(unix_usec, pg_sys::TIMESTAMPTZOID);
        assert_eq!(d.value() as i64, 1_000_000_000);

        // PG-epoch zero corresponds to Unix-epoch + PG_EPOCH_OFFSET_USEC.
        let d = decode_encoded_to_datum(PG_EPOCH_OFFSET_USEC, pg_sys::TIMESTAMPOID);
        assert_eq!(d.value() as i64, 0);
    }

    #[test]
    fn decode_encoded_to_datum_date_converts_usec_to_pg_days() {
        // `encoded` is Unix-epoch µs; `decode` returns PG-epoch days
        // (truncating, since DATE has no sub-day precision).
        let one_day_usec: i64 = 86_400_000_000;

        // Unix day 10_958 = PG day 1.
        let unix_day = PG_EPOCH_OFFSET_DAYS + 1;
        let d = decode_encoded_to_datum(unix_day * one_day_usec, pg_sys::DATEOID);
        assert_eq!(d.value() as i32, 1);

        // The PG epoch itself.
        let d = decode_encoded_to_datum(PG_EPOCH_OFFSET_DAYS * one_day_usec, pg_sys::DATEOID);
        assert_eq!(d.value() as i32, 0);
    }

    #[test]
    fn decode_encoded_to_datum_floats_round_trip() {
        // Round-trip: encode_fXX_to_i64 → decode_encoded_to_datum → bit-pattern check.
        for f in [1.5f64, -2.5, 0.0, 1e-9, 1e9, f64::INFINITY] {
            let enc = encode_f64_to_i64(f);
            let datum = decode_encoded_to_datum(enc, pg_sys::FLOAT8OID);
            let back = f64::from_bits(datum.value() as u64);
            assert_eq!(back.to_bits(), f.to_bits(), "f64 round trip {}", f);
        }
        for f in [1.5f32, -3.25, 0.0, 1e-9, 1e9, f32::INFINITY] {
            let enc = encode_f32_to_i64(f);
            let datum = decode_encoded_to_datum(enc, pg_sys::FLOAT4OID);
            let back = f32::from_bits(datum.value() as u32);
            assert_eq!(back.to_bits(), f.to_bits(), "f32 round trip {}", f);
        }
    }

    #[test]
    fn sum_i128_to_datum_packs_into_int8() {
        // SUM(int2)/SUM(int4) → int8. Datum is the i64 bit-pattern.
        let d = sum_i128_to_datum(42, pg_sys::INT8OID, pg_sys::INT4OID);
        assert_eq!(d.value() as i64, 42);

        let d = sum_i128_to_datum(-1_234_567_890_123i128, pg_sys::INT8OID, pg_sys::INT8OID);
        assert_eq!(d.value() as i64, -1_234_567_890_123);
    }

    #[test]
    #[should_panic] // pgrx::error! payload isn't a plain string; just confirm it panics.
    fn sum_i128_to_datum_overflow_panics_for_int8_result() {
        // Anything beyond i64::MAX must surface as an error rather than
        // silently truncate — wrong sum is worse than a query failure.
        let too_big: i128 = (i64::MAX as i128) + 1;
        let _ = sum_i128_to_datum(too_big, pg_sys::INT8OID, pg_sys::INT8OID);
    }

    #[test]
    #[should_panic]
    fn sum_i128_to_datum_underflow_panics_for_int8_result() {
        let too_small: i128 = (i64::MIN as i128) - 1;
        let _ = sum_i128_to_datum(too_small, pg_sys::INT8OID, pg_sys::INT8OID);
    }
}
