use pgrx::pg_sys;
use pgrx::pg_guard;

use super::exec::DecompressState;

/// ExplainCustomScan callback: output info for EXPLAIN.
#[pg_guard]
pub unsafe extern "C-unwind" fn explain_custom_scan(
    node: *mut pg_sys::CustomScanState,
    _ancestors: *mut pg_sys::List,
    es: *mut pg_sys::ExplainState,
) {
    unsafe {
        let label = c"Storage";
        let value = c"Compressed (CocoonDecompress)";
        pg_sys::ExplainPropertyText(label.as_ptr(), value.as_ptr(), es);

        explain_timing(node, es);
    }
}

/// ExplainCustomScan callback for CocoonAppend.
#[pg_guard]
pub unsafe extern "C-unwind" fn explain_cocoon_append(
    node: *mut pg_sys::CustomScanState,
    _ancestors: *mut pg_sys::List,
    es: *mut pg_sys::ExplainState,
) {
    unsafe {
        let label = c"Storage";
        let value = c"Compressed (CocoonAppend)";
        pg_sys::ExplainPropertyText(label.as_ptr(), value.as_ptr(), es);

        explain_timing(node, es);
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
                let total_ms = (t.metadata_us + t.heap_scan_us + t.decompress_us + t.emit_us)
                    as f64 / 1000.0;

                let timing_str = std::ffi::CString::new(format!(
                    "{:.3} ms (metadata={:.3} heap_scan={:.3} decompress={:.3} emit={:.3})",
                    total_ms,
                    t.metadata_us as f64 / 1000.0,
                    t.heap_scan_us as f64 / 1000.0,
                    t.decompress_us as f64 / 1000.0,
                    t.emit_us as f64 / 1000.0,
                ))
                .unwrap();
                pg_sys::ExplainPropertyText(
                    c"Cocoon Timing".as_ptr(),
                    timing_str.as_ptr(),
                    es,
                );

                let stats_str = std::ffi::CString::new(format!(
                    "segments={} rows_out={} rows_filtered={} compressed_bytes={}",
                    t.segments_decompressed,
                    t.rows_emitted,
                    t.rows_filtered,
                    t.compressed_bytes,
                ))
                .unwrap();
                pg_sys::ExplainPropertyText(
                    c"Cocoon Stats".as_ptr(),
                    stats_str.as_ptr(),
                    es,
                );
            }
        }
    }
}
