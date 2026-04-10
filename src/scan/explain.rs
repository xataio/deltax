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
        let value = c"Compressed (DeltaXDecompress)";
        pg_sys::ExplainPropertyText(label.as_ptr(), value.as_ptr(), es);

        explain_timing(node, es);
    }
}

/// ExplainCustomScan callback for DeltaXAppend.
#[pg_guard]
pub unsafe extern "C-unwind" fn explain_deltax_append(
    node: *mut pg_sys::CustomScanState,
    _ancestors: *mut pg_sys::List,
    es: *mut pg_sys::ExplainState,
) {
    unsafe {
        let label = c"Storage";
        let value = c"Compressed (DeltaXAppend)";
        pg_sys::ExplainPropertyText(label.as_ptr(), value.as_ptr(), es);

        explain_timing(node, es);
    }
}

/// ExplainCustomScan callback for DeltaXCount (COUNT(*) pushdown).
#[pg_guard]
pub unsafe extern "C-unwind" fn explain_count_scan(
    node: *mut pg_sys::CustomScanState,
    _ancestors: *mut pg_sys::List,
    es: *mut pg_sys::ExplainState,
) {
    unsafe {
        let label = c"Storage";
        let value = c"Compressed (DeltaXCount - COUNT(*) pushdown)";
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
                    c"DeltaX Timing".as_ptr(),
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
                    c"DeltaX Stats".as_ptr(),
                    stats_str.as_ptr(),
                    es,
                );

                if (*es).buffers {
                    let b = &state.buf_stats;
                    let buffers_str = std::ffi::CString::new(format!(
                        "meta hit={} read={}  bloom hit={} read={}  blob hit={} read={}",
                        b.meta_hit, b.meta_read,
                        b.bloom_hit, b.bloom_read,
                        b.blob_hit, b.blob_read,
                    ))
                    .unwrap();
                    pg_sys::ExplainPropertyText(
                        c"DeltaX Buffers".as_ptr(),
                        buffers_str.as_ptr(),
                        es,
                    );
                }
            }
        }
    }
}

/// ExplainCustomScan callback for DeltaXMinMax (MIN/MAX pushdown).
#[pg_guard]
pub unsafe extern "C-unwind" fn explain_minmax_scan(
    node: *mut pg_sys::CustomScanState,
    _ancestors: *mut pg_sys::List,
    es: *mut pg_sys::ExplainState,
) {
    unsafe {
        let label = c"Storage";
        let value = c"Compressed (DeltaXMinMax - MIN/MAX pushdown)";
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
                    c"DeltaX Timing".as_ptr(),
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
                    c"DeltaX Stats".as_ptr(),
                    stats_str.as_ptr(),
                    es,
                );

                if (*es).buffers {
                    let b = &state.buf_stats;
                    let buffers_str = std::ffi::CString::new(format!(
                        "meta hit={} read={}  bloom hit={} read={}  blob hit={} read={}",
                        b.meta_hit, b.meta_read,
                        b.bloom_hit, b.bloom_read,
                        b.blob_hit, b.blob_read,
                    ))
                    .unwrap();
                    pg_sys::ExplainPropertyText(
                        c"DeltaX Buffers".as_ptr(),
                        buffers_str.as_ptr(),
                        es,
                    );
                }
            }
        }
    }
}

/// ExplainCustomScan callback for DeltaXAgg (aggregate pushdown).
#[pg_guard]
pub unsafe extern "C-unwind" fn explain_agg_scan(
    node: *mut pg_sys::CustomScanState,
    _ancestors: *mut pg_sys::List,
    es: *mut pg_sys::ExplainState,
) {
    unsafe {
        let label = c"Storage";
        let value = c"Compressed (DeltaXAgg)";
        pg_sys::ExplainPropertyText(label.as_ptr(), value.as_ptr(), es);

        if (*es).analyze {
            let state_ptr = (*node).custom_ps as *const AggScanState;
            if !state_ptr.is_null() {
                let state = &*state_ptr;
                let total_ms = if state.wall_us > 0 {
                    state.wall_us as f64 / 1000.0
                } else {
                    (state.metadata_us + state.heap_scan_us + state.decompress_us + state.agg_us
                     + state.merge_us + state.finalize_us + state.topn_select_us)
                        as f64 / 1000.0
                };

                let timing_str = std::ffi::CString::new(format!(
                    "{:.3} ms (metadata={:.3} heap_scan={:.3} [detoast={:.3}] decompress={:.3} agg={:.3} merge={:.3} finalize={:.3} topn_select={:.3})",
                    total_ms,
                    state.metadata_us as f64 / 1000.0,
                    state.heap_scan_us as f64 / 1000.0,
                    state.detoast_us as f64 / 1000.0,
                    state.decompress_us as f64 / 1000.0,
                    state.agg_us as f64 / 1000.0,
                    state.merge_us as f64 / 1000.0,
                    state.finalize_us as f64 / 1000.0,
                    state.topn_select_us as f64 / 1000.0,
                ))
                .unwrap();
                pg_sys::ExplainPropertyText(
                    c"DeltaX Timing".as_ptr(),
                    timing_str.as_ptr(),
                    es,
                );

                let regex_str = if state.regex_cache_size > 0 {
                    format!(" regex_cache_size={} regex_calls={}", state.regex_cache_size, state.regex_cache_calls)
                } else {
                    String::new()
                };
                let filtered_str = if state.segments_metadata_resolved > 0 || state.segments_decompressed > 0 {
                    format!(" segments_metadata_resolved={} segments_decompressed={}",
                        state.segments_metadata_resolved, state.segments_decompressed)
                } else {
                    String::new()
                };
                let stats_str = std::ffi::CString::new(format!(
                    "segments={} rows_processed={} result_rows={} batch_quals={} where_quals_null={}{}{}",
                    state.total_segments,
                    state.total_rows_processed,
                    state.result_rows.len(),
                    state.batch_quals_count,
                    state.where_quals_null,
                    filtered_str,
                    regex_str,
                ))
                .unwrap();
                pg_sys::ExplainPropertyText(
                    c"DeltaX Stats".as_ptr(),
                    stats_str.as_ptr(),
                    es,
                );

                if state.topn_limit > 0 {
                    let direction = if state.topn_ascending { "ASC" } else { "DESC" };
                    let topn_str = std::ffi::CString::new(format!(
                        "limit={} sort_col={} direction={} pre_topn_groups={}",
                        state.topn_limit,
                        state.topn_sort_col,
                        direction,
                        state.pre_topn_groups,
                    ))
                    .unwrap();
                    pg_sys::ExplainPropertyText(
                        c"TopN".as_ptr(),
                        topn_str.as_ptr(),
                        es,
                    );
                }

                if (*es).buffers {
                    let b = &state.buf_stats;
                    let buffers_str = std::ffi::CString::new(format!(
                        "meta hit={} read={}  bloom hit={} read={}  blob hit={} read={}",
                        b.meta_hit, b.meta_read,
                        b.bloom_hit, b.bloom_read,
                        b.blob_hit, b.blob_read,
                    ))
                    .unwrap();
                    pg_sys::ExplainPropertyText(
                        c"DeltaX Buffers".as_ptr(),
                        buffers_str.as_ptr(),
                        es,
                    );
                }
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
                    c"DeltaX Timing".as_ptr(),
                    timing_str.as_ptr(),
                    es,
                );

                let topn_str = if t.topn_limit > 0 {
                    format!(" topn={} topn_candidates={} topn_p2_segs={}", t.topn_limit, t.topn_candidates, t.topn_phase2_segments)
                } else {
                    String::new()
                };
                let stats_str = std::ffi::CString::new(format!(
                    "segments={} segments_skipped={} segments_minmax_skipped={} segments_bloom_skipped={} phase2_skipped={} rows_out={} rows_filtered={} rows_batch_filtered={} compressed_bytes={}{}",
                    t.segments_decompressed,
                    t.segments_skipped,
                    t.segments_minmax_skipped,
                    t.segments_bloom_skipped,
                    t.phase2_skipped,
                    t.rows_emitted,
                    t.rows_filtered,
                    t.rows_batch_filtered,
                    t.compressed_bytes,
                    topn_str,
                ))
                .unwrap();
                pg_sys::ExplainPropertyText(
                    c"DeltaX Stats".as_ptr(),
                    stats_str.as_ptr(),
                    es,
                );

                // Per-phase shared-buffer deltas. Custom scan work happens
                // in BeginCustomScan, outside PG's node-level instrumentation,
                // so the standard `Buffers:` line misses it. This surfaces
                // the breakdown for cold-cache investigations.
                if (*es).buffers {
                    let b = &t.buf_stats;
                    let buffers_str = std::ffi::CString::new(format!(
                        "meta hit={} read={}  bloom hit={} read={}  blob hit={} read={}",
                        b.meta_hit, b.meta_read,
                        b.bloom_hit, b.bloom_read,
                        b.blob_hit, b.blob_read,
                    ))
                    .unwrap();
                    pg_sys::ExplainPropertyText(
                        c"DeltaX Buffers".as_ptr(),
                        buffers_str.as_ptr(),
                        es,
                    );
                }
            }
        }
    }
}
