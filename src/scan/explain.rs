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
        let value = c"Compressed (DeltaXMinMax - metadata-only aggregate)";
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

                use crate::scan::path::MetaAggKind;
                let agg_parts: Vec<String> = state.results.iter().map(|r| {
                    let agg_name = match r.kind {
                        MetaAggKind::Min => "MIN",
                        MetaAggKind::Max => "MAX",
                        MetaAggKind::Sum => "SUM",
                        MetaAggKind::CountCol => "COUNT",
                        MetaAggKind::CountStar => "COUNT*",
                    };
                    let target = if matches!(r.kind, MetaAggKind::CountStar) {
                        "*".to_string()
                    } else {
                        r.col_name.clone()
                    };
                    format!("{}({})=null={}", agg_name, target, r.is_null)
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
                let f8_str = if state.f8_preselected > 0 {
                    format!(" f8_preselected={}", state.f8_preselected)
                } else {
                    String::new()
                };
                let stats_str = std::ffi::CString::new(format!(
                    "segments={} rows_processed={} result_rows={} batch_quals={} where_quals_null={}{}{}{}",
                    state.total_segments,
                    state.total_rows_processed,
                    state.result_rows.len(),
                    state.batch_quals_count,
                    state.where_quals_null,
                    filtered_str,
                    regex_str,
                    f8_str,
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

                if state.blob_cache_hits + state.blob_cache_misses > 0 {
                    let cache_str = std::ffi::CString::new(format!(
                        "hits={} misses={} bytes_served={}",
                        state.blob_cache_hits,
                        state.blob_cache_misses,
                        state.blob_cache_bytes_served,
                    ))
                    .unwrap();
                    pg_sys::ExplainPropertyText(
                        c"DeltaX Blob Cache".as_ptr(),
                        cache_str.as_ptr(),
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
                let state = &*state_ptr;
                let t = &state.timing;

                // Aggregate leader + all worker slots for the top-line totals
                // so EXPLAIN shows total work across the parallel group rather
                // than just the leader's slice.
                let mut metadata_us = t.metadata_us;
                let mut heap_scan_us = t.heap_scan_us;
                let mut decompress_us = t.decompress_us;
                let mut batch_eval_us = t.batch_eval_us;
                let mut emit_us = t.emit_us;
                let mut segments_decompressed = t.segments_decompressed;
                let mut segments_skipped = t.segments_skipped;
                let mut segments_minmax_skipped = t.segments_minmax_skipped;
                let mut segments_bloom_skipped = t.segments_bloom_skipped;
                let mut segments_valbitmap_skipped = t.segments_valbitmap_skipped;
                let mut phase2_skipped = t.phase2_skipped;
                let mut rows_emitted = t.rows_emitted;
                let mut rows_filtered = t.rows_filtered;
                let mut rows_batch_filtered = t.rows_batch_filtered;
                let mut compressed_bytes = t.compressed_bytes;
                let mut blob_cache_hits = t.blob_cache_hits;
                let mut blob_cache_misses = t.blob_cache_misses;
                let mut blob_cache_bytes_served = t.blob_cache_bytes_served;
                // Slot 0 is the leader; its counters are already in `t`.
                // Skip it during aggregation to avoid double-counting.
                for (slot_idx, s) in state.cached_worker_timings.iter().enumerate() {
                    if slot_idx == 0 || s.populated == 0 { continue; }
                    metadata_us += s.metadata_us;
                    heap_scan_us += s.heap_scan_us;
                    decompress_us += s.decompress_us;
                    batch_eval_us += s.batch_eval_us;
                    emit_us += s.emit_us;
                    segments_decompressed += s.segments_decompressed;
                    segments_skipped += s.segments_skipped;
                    segments_minmax_skipped += s.segments_minmax_skipped;
                    segments_bloom_skipped += s.segments_bloom_skipped;
                    segments_valbitmap_skipped += s.segments_valbitmap_skipped;
                    phase2_skipped += s.phase2_skipped;
                    rows_emitted += s.rows_emitted;
                    rows_filtered += s.rows_filtered;
                    rows_batch_filtered += s.rows_batch_filtered;
                    compressed_bytes += s.compressed_bytes;
                    blob_cache_hits += s.blob_cache_hits;
                    blob_cache_misses += s.blob_cache_misses;
                    blob_cache_bytes_served += s.blob_cache_bytes_served;
                }

                let total_ms = (metadata_us + heap_scan_us + decompress_us + batch_eval_us + emit_us)
                    as f64 / 1000.0;

                let timing_str = std::ffi::CString::new(format!(
                    "{:.3} ms (metadata={:.3} heap_scan={:.3} decompress={:.3} batch_eval={:.3} emit={:.3})",
                    total_ms,
                    metadata_us as f64 / 1000.0,
                    heap_scan_us as f64 / 1000.0,
                    decompress_us as f64 / 1000.0,
                    batch_eval_us as f64 / 1000.0,
                    emit_us as f64 / 1000.0,
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
                    "segments={} segments_skipped={} segments_minmax_skipped={} segments_bloom_skipped={} segments_valbitmap_skipped={} phase2_skipped={} rows_out={} rows_filtered={} rows_batch_filtered={} compressed_bytes={}{}",
                    segments_decompressed,
                    segments_skipped,
                    segments_minmax_skipped,
                    segments_bloom_skipped,
                    segments_valbitmap_skipped,
                    phase2_skipped,
                    rows_emitted,
                    rows_filtered,
                    rows_batch_filtered,
                    compressed_bytes,
                    topn_str,
                ))
                .unwrap();
                pg_sys::ExplainPropertyText(
                    c"DeltaX Stats".as_ptr(),
                    stats_str.as_ptr(),
                    es,
                );

                if blob_cache_hits + blob_cache_misses > 0 {
                    let cache_str = std::ffi::CString::new(format!(
                        "hits={} misses={} bytes_served={}",
                        blob_cache_hits, blob_cache_misses, blob_cache_bytes_served,
                    ))
                    .unwrap();
                    pg_sys::ExplainPropertyText(
                        c"DeltaX Blob Cache".as_ptr(),
                        cache_str.as_ptr(),
                        es,
                    );
                }

                // If this was a parallel partial scan, surface per-process
                // segment counts + decompress timing for visibility into
                // worker balance.
                let verbose = (*es).verbose;
                if !state.cached_worker_timings.is_empty() && verbose {
                    for (idx, s) in state.cached_worker_timings.iter().enumerate() {
                        if s.populated == 0 { continue; }
                        let tag = if idx == 0 { "leader".to_string() } else { format!("worker{}", idx - 1) };
                        let w_total_ms =
                            (s.metadata_us + s.heap_scan_us + s.decompress_us + s.batch_eval_us + s.emit_us)
                                as f64 / 1000.0;
                        let line = std::ffi::CString::new(format!(
                            "{} segs={} rows_out={} total={:.3}ms decompress={:.3}ms emit={:.3}ms",
                            tag,
                            s.segments_decompressed,
                            s.rows_emitted,
                            w_total_ms,
                            s.decompress_us as f64 / 1000.0,
                            s.emit_us as f64 / 1000.0,
                        ))
                        .unwrap();
                        pg_sys::ExplainPropertyText(
                            c"DeltaX Worker".as_ptr(),
                            line.as_ptr(),
                            es,
                        );
                    }
                }

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
