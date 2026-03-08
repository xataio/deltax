use pgrx::pg_sys;
use pgrx::pg_guard;

use super::exec::{DecompressState, CountScanState, MinMaxScanState, AggScanState};

/// ExplainCustomScan callback: output info for EXPLAIN.
#[pg_guard]
pub unsafe extern "C-unwind" fn explain_custom_scan(
    node: *mut pg_sys::CustomScanState,
    _ancestors: *mut pg_sys::List,
    es: *mut pg_sys::ExplainState,
) {
    unsafe {
        let label = c"Storage";
        let value = c"Compressed (SeaTurtleDecompress)";
        pg_sys::ExplainPropertyText(label.as_ptr(), value.as_ptr(), es);

        explain_timing(node, es);
    }
}

/// ExplainCustomScan callback for SeaTurtleAppend.
#[pg_guard]
pub unsafe extern "C-unwind" fn explain_seaturtle_append(
    node: *mut pg_sys::CustomScanState,
    _ancestors: *mut pg_sys::List,
    es: *mut pg_sys::ExplainState,
) {
    unsafe {
        let label = c"Storage";
        let value = c"Compressed (SeaTurtleAppend)";
        pg_sys::ExplainPropertyText(label.as_ptr(), value.as_ptr(), es);

        explain_timing(node, es);
    }
}

/// ExplainCustomScan callback for SeaTurtleCount (COUNT(*) pushdown).
#[pg_guard]
pub unsafe extern "C-unwind" fn explain_count_scan(
    node: *mut pg_sys::CustomScanState,
    _ancestors: *mut pg_sys::List,
    es: *mut pg_sys::ExplainState,
) {
    unsafe {
        let label = c"Storage";
        let value = c"Compressed (SeaTurtleCount - COUNT(*) pushdown)";
        pg_sys::ExplainPropertyText(label.as_ptr(), value.as_ptr(), es);

        if (*es).analyze {
            let state_ptr = (*node).custom_ps as *const CountScanState;
            if !state_ptr.is_null() {
                let state = &*state_ptr;
                let total_ms =
                    (state.metadata_us + state.heap_scan_us) as f64 / 1000.0;

                let timing_str = std::ffi::CString::new(format!(
                    "{:.3} ms (metadata={:.3} heap_scan={:.3})",
                    total_ms,
                    state.metadata_us as f64 / 1000.0,
                    state.heap_scan_us as f64 / 1000.0,
                ))
                .unwrap();
                pg_sys::ExplainPropertyText(
                    c"SeaTurtle Timing".as_ptr(),
                    timing_str.as_ptr(),
                    es,
                );

                let stats_str = std::ffi::CString::new(format!(
                    "total_count={} segments={}",
                    state.total_count,
                    state.total_segments,
                ))
                .unwrap();
                pg_sys::ExplainPropertyText(
                    c"SeaTurtle Stats".as_ptr(),
                    stats_str.as_ptr(),
                    es,
                );
            }
        }
    }
}

/// ExplainCustomScan callback for SeaTurtleMinMax (MIN/MAX pushdown).
#[pg_guard]
pub unsafe extern "C-unwind" fn explain_minmax_scan(
    node: *mut pg_sys::CustomScanState,
    _ancestors: *mut pg_sys::List,
    es: *mut pg_sys::ExplainState,
) {
    unsafe {
        let label = c"Storage";
        let value = c"Compressed (SeaTurtleMinMax - MIN/MAX pushdown)";
        pg_sys::ExplainPropertyText(label.as_ptr(), value.as_ptr(), es);

        if (*es).analyze {
            let state_ptr = (*node).custom_ps as *const MinMaxScanState;
            if !state_ptr.is_null() {
                let state = &*state_ptr;
                let total_ms =
                    (state.metadata_us + state.heap_scan_us) as f64 / 1000.0;

                let timing_str = std::ffi::CString::new(format!(
                    "{:.3} ms (metadata={:.3} heap_scan={:.3})",
                    total_ms,
                    state.metadata_us as f64 / 1000.0,
                    state.heap_scan_us as f64 / 1000.0,
                ))
                .unwrap();
                pg_sys::ExplainPropertyText(
                    c"SeaTurtle Timing".as_ptr(),
                    timing_str.as_ptr(),
                    es,
                );

                let agg_parts: Vec<String> = state.results.iter().map(|r| {
                    let agg_name = if r.is_min { "MIN" } else { "MAX" };
                    format!("{}({})=null={}", agg_name, r.col_name, r.is_null)
                }).collect();
                let stats_str = std::ffi::CString::new(format!(
                    "{} segments={}",
                    agg_parts.join(", "),
                    state.total_segments,
                ))
                .unwrap();
                pg_sys::ExplainPropertyText(
                    c"SeaTurtle Stats".as_ptr(),
                    stats_str.as_ptr(),
                    es,
                );
            }
        }
    }
}

/// ExplainCustomScan callback for SeaTurtleAgg (aggregate pushdown).
#[pg_guard]
pub unsafe extern "C-unwind" fn explain_agg_scan(
    node: *mut pg_sys::CustomScanState,
    _ancestors: *mut pg_sys::List,
    es: *mut pg_sys::ExplainState,
) {
    unsafe {
        let label = c"Storage";
        let value = c"Compressed (SeaTurtleAgg)";
        pg_sys::ExplainPropertyText(label.as_ptr(), value.as_ptr(), es);

        if (*es).analyze {
            let state_ptr = (*node).custom_ps as *const AggScanState;
            if !state_ptr.is_null() {
                let state = &*state_ptr;
                let total_ms =
                    (state.metadata_us + state.heap_scan_us + state.decompress_us + state.agg_us)
                        as f64 / 1000.0;

                let timing_str = std::ffi::CString::new(format!(
                    "{:.3} ms (metadata={:.3} heap_scan={:.3} decompress={:.3} agg={:.3})",
                    total_ms,
                    state.metadata_us as f64 / 1000.0,
                    state.heap_scan_us as f64 / 1000.0,
                    state.decompress_us as f64 / 1000.0,
                    state.agg_us as f64 / 1000.0,
                ))
                .unwrap();
                pg_sys::ExplainPropertyText(
                    c"SeaTurtle Timing".as_ptr(),
                    timing_str.as_ptr(),
                    es,
                );

                let stats_str = std::ffi::CString::new(format!(
                    "segments={} rows_processed={} result_rows={}",
                    state.total_segments,
                    state.total_rows_processed,
                    state.result_rows.len(),
                ))
                .unwrap();
                pg_sys::ExplainPropertyText(
                    c"SeaTurtle Stats".as_ptr(),
                    stats_str.as_ptr(),
                    es,
                );
            }
        }
    }
}

/// Shared timing/stats output for EXPLAIN ANALYZE.
unsafe fn explain_timing(
    node: *mut pg_sys::CustomScanState,
    es: *mut pg_sys::ExplainState,
) {
    unsafe {
        if (*es).analyze {
            let state_ptr = (*node).custom_ps as *const DecompressState;
            if !state_ptr.is_null() {
                let t = &(*state_ptr).timing;
                let total_ms = (t.metadata_us + t.heap_scan_us + t.decompress_us + t.batch_eval_us + t.emit_us)
                    as f64 / 1000.0;

                let timing_str = std::ffi::CString::new(format!(
                    "{:.3} ms (metadata={:.3} heap_scan={:.3} decompress={:.3} batch_eval={:.3} emit={:.3})",
                    total_ms,
                    t.metadata_us as f64 / 1000.0,
                    t.heap_scan_us as f64 / 1000.0,
                    t.decompress_us as f64 / 1000.0,
                    t.batch_eval_us as f64 / 1000.0,
                    t.emit_us as f64 / 1000.0,
                ))
                .unwrap();
                pg_sys::ExplainPropertyText(
                    c"SeaTurtle Timing".as_ptr(),
                    timing_str.as_ptr(),
                    es,
                );

                let topn_str = if t.topn_limit > 0 {
                    format!(" topn={} topn_candidates={} topn_p2_segs={}", t.topn_limit, t.topn_candidates, t.topn_phase2_segments)
                } else {
                    String::new()
                };
                let stats_str = std::ffi::CString::new(format!(
                    "segments={} segments_skipped={} segments_minmax_skipped={} phase2_skipped={} rows_out={} rows_filtered={} rows_batch_filtered={} compressed_bytes={}{}",
                    t.segments_decompressed,
                    t.segments_skipped,
                    t.segments_minmax_skipped,
                    t.phase2_skipped,
                    t.rows_emitted,
                    t.rows_filtered,
                    t.rows_batch_filtered,
                    t.compressed_bytes,
                    topn_str,
                ))
                .unwrap();
                pg_sys::ExplainPropertyText(
                    c"SeaTurtle Stats".as_ptr(),
                    stats_str.as_ptr(),
                    es,
                );
            }
        }
    }
}
