use pgrx::pg_guard;
use pgrx::pg_sys;
use pgrx::prelude::*;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use super::append_wire::{self, DeltaXAppendView};
use super::batch_qual::{
    BatchCompareOp, BatchQual, evaluate_batch_quals, extract_batch_quals, is_text_type,
};
use super::datum_utils::{
    decompress_blob_to_datums, decompress_blob_to_datums_truncated,
    decompress_jsonb_blob_with_selection, decompress_text_blob_with_eq_filter,
    decompress_text_blob_with_in_filter, decompress_text_blob_with_like_filter,
    decompress_text_blob_with_selection, exec_project, exec_qual, pg_type_name, string_to_datum,
};
use super::segments::{
    SegmentData, detoast_lazy_blobs, detoast_lazy_blobs_selective, extract_segment_filters,
    fetch_segment_blobs, load_metadata, load_segments_heap, segment_skippable_by_dict,
};
use super::text_col::{
    TextQualInfo, apply_text_eq_filter, apply_text_in_filter, apply_text_like_filter,
    decompress_text_to_seg_col, strcoll_cmp,
};
use super::{CUSTOM_EXEC_METHODS, DELTAX_APPEND_EXEC_METHODS};

/// Decompression state stored as a raw pointer in the CustomScanState.
pub(crate) struct DecompressState {
    /// Column names in the original table (in order).
    col_names: Vec<String>,
    /// Column type OIDs (in order).
    col_types: Vec<pg_sys::Oid>,
    /// Column type modifiers (e.g. length for CHAR(n)); -1 means unspecified.
    col_typmods: Vec<i32>,
    /// Segment-by column names.
    segment_by: Vec<String>,
    /// Decompressed datums for the current segment: outer = column, inner = row.
    /// Each element is (datum, is_null).
    current_segment: Vec<Vec<(pg_sys::Datum, bool)>>,
    /// Total row count for the current segment (avoids indexing into empty Vecs).
    current_row_count: usize,
    /// Current row index within current_segment.
    row_cursor: usize,
    /// Current segment index (0-based).
    segment_index: usize,
    /// Pre-loaded segments data from the companion table.
    segments_data: Vec<SegmentData>,
    /// 0-based column indices that the query needs. true = needed.
    /// Empty means decompress all (safety fallback).
    needed_cols: Vec<bool>,
    /// Precomputed indices where needed_cols[i] == true, for sparse iteration.
    needed_col_indices: Vec<usize>,
    /// Per-segment memory context (child of es_query_cxt, reset per segment).
    segment_mcxt: pg_sys::MemoryContext,
    /// Timing: wall-clock durations for profiling (accumulated across calls).
    pub(crate) timing: ScanTiming,
    /// Whether EXPLAIN ANALYZE is active (enables per-call timing).
    /// Set lazily on first exec call (PG sets PlanState.instrument after BeginCustomScan).
    instrument: Option<bool>,

    /// Time column name (from deltatable metadata).
    _time_column: String,
    /// Whether rows within each segment are sorted by the time column
    /// (true when order_by[0] == time_column or order_by is empty).
    rows_sorted_by_time: bool,

    // Segment pruning filters extracted from plan qual
    /// (index into segment_values, value to match) for segment_by equality filters.
    segment_by_filters: Vec<(usize, String)>,
    /// Lower bound for time column (PG epoch microseconds), inclusive.
    time_min: Option<i64>,
    /// Upper bound for time column (PG epoch microseconds), inclusive.
    time_max: Option<i64>,

    /// Batch quals extracted from plan qual for vectorized evaluation.
    batch_quals: Vec<BatchQual>,
    /// Whether all plan quals are handled by batch eval (allows skipping ExecQual).
    all_quals_batch_handled: bool,
    /// Selection vector: true = row passes batch quals. Empty = all pass.
    selection_vector: Vec<bool>,

    /// Top-N optimization: effective LIMIT count (0 = disabled).
    topn_limit: usize,
    /// Sort ascending (true) or descending (false) for Top-N.
    topn_ascending: bool,
    /// Whether NULL sort keys come before normal values.
    topn_nulls_first: bool,
    /// Whether ORDER BY has multiple columns (first is time, rest handled by PG Sort).
    topn_multi_col_sort: bool,
    /// Sort column index (0-based into col_names) for Top-N.
    topn_sort_col: Option<usize>,
    /// Buffered top-N result rows (filled on first exec call).
    /// Each inner Vec contains (Datum, is_null) for ALL columns.
    topn_buffer: Vec<Vec<(pg_sys::Datum, bool)>>,
    /// Cursor into topn_buffer.
    topn_cursor: usize,
    /// Whether the Top-N pass has been executed.
    topn_done: bool,
    /// Whether the Top-N sort column is a text type (uses byte comparison).
    topn_sort_is_text: bool,

    /// DSM-resident shared state when running as a parallel partial scan.
    /// Null in the serial path; non-null after `InitializeDSMCustomScan`
    /// (leader) or `InitializeWorkerCustomScan` (worker) wires it up.
    pub(crate) pscan: *mut DeltaXAppendPState,

    /// Pointer to the serialised metadata region that starts immediately
    /// after `DeltaXAppendPState` in the DSM buffer. Populated in
    /// `InitializeDSMCustomScan` (leader) and `InitializeWorkerCustomScan`
    /// (worker). Workers use this to hydrate `col_names` / `segments_data`
    /// without re-running `load_metadata` + `load_segments_heap`.
    pub(crate) wire_base: *const u8,

    /// Decoded view over `wire_base`. Set lazily on first worker-side access
    /// and cleared in `ShutdownCustomScan` before DSM detach.
    pub(crate) wire_view: Option<DeltaXAppendView>,

    /// True when this `DecompressState` was constructed as a parallel
    /// worker (no SPI/heap-scan load ran in `begin_deltax_append`). Ensures
    /// `init_worker_deltax_append` must run before `exec_custom_scan` reads
    /// segment data.
    pub(crate) is_worker_stub: bool,

    /// Snapshot of DSM per-worker timings, copied by the leader in
    /// `ShutdownCustomScan` before the DSM is torn down. Empty on workers
    /// and in the serial path. Consumed by `ExplainCustomScan`.
    pub(crate) cached_worker_timings: Vec<ScanTimingShmem>,
}

/// Wall-clock timing for the decompress scan phases.
#[derive(Default)]
pub(crate) struct ScanTiming {
    /// Time spent in load_metadata (SPI).
    pub(crate) metadata_us: u64,
    /// Time spent in load_segments_heap (heap scan + detoast).
    pub(crate) heap_scan_us: u64,
    /// Time spent decompressing blobs to datums (per segment).
    pub(crate) decompress_us: u64,
    /// Phase 1 decompress: filter columns + segment-by.
    pub(crate) phase1_us: u64,
    /// Phase 2 decompress: deferred columns with selection awareness.
    pub(crate) phase2_us: u64,
    /// Phase 2 text columns (LZ4/Dict with varlena allocation).
    pub(crate) phase2_text_us: u64,
    /// Phase 2 non-text columns (Gorilla/DeltaVarint/Boolean).
    pub(crate) phase2_nontext_us: u64,
    /// Number of Phase 2 text columns decompressed.
    pub(crate) phase2_text_cols: u64,
    /// Number of Phase 2 non-text columns decompressed.
    pub(crate) phase2_nontext_cols: u64,
    /// Time spent in fill_slot + qual + projection (per row).
    pub(crate) emit_us: u64,
    /// Total rows emitted (passed qual).
    pub(crate) rows_emitted: u64,
    /// Total rows filtered by qual.
    pub(crate) rows_filtered: u64,
    /// Time spent in batch qual evaluation (per segment).
    pub(crate) batch_eval_us: u64,
    /// Total rows filtered by batch quals (before fill_slot).
    pub(crate) rows_batch_filtered: u64,
    /// Total segments decompressed.
    pub(crate) segments_decompressed: u64,
    /// Total compressed bytes loaded.
    pub(crate) compressed_bytes: u64,
    /// Total segments skipped by pruning.
    pub(crate) segments_skipped: u64,
    /// Total segments skipped specifically by min/max predicate filters.
    pub(crate) segments_minmax_skipped: u64,
    /// Total segments skipped specifically by bloom filter checks.
    pub(crate) segments_bloom_skipped: u64,
    /// Total segments skipped specifically by per-segment value-presence
    /// bitmap checks (text Eq on low-cardinality columns; see
    /// `compress::compute_segment_valbitmap_values`).
    pub(crate) segments_valbitmap_skipped: u64,
    /// Per-phase shared-buffer deltas captured during `load_segments_heap`.
    pub(crate) buf_stats: super::segments::ScanBufferStats,
    /// Total segments where Phase 2 was skipped (no selected rows).
    pub(crate) phase2_skipped: u64,
    /// Top-N effective limit (0 = disabled).
    pub(crate) topn_limit: u64,
    /// Top-N candidate rows collected.
    pub(crate) topn_candidates: u64,
    /// Top-N segments processed in Phase 2.
    pub(crate) topn_phase2_segments: u64,
    /// Blob cache hits across this process's detoast calls.
    pub(crate) blob_cache_hits: u64,
    /// Blob cache misses across this process's detoast calls.
    pub(crate) blob_cache_misses: u64,
    /// Bytes served from the blob cache (sum across hits).
    pub(crate) blob_cache_bytes_served: u64,
}

/// POD projection of `ScanTiming` for cross-process DSM aggregation.
/// All fields are u64 (no heap pointers, no buf_stats).
#[repr(C)]
#[derive(Copy, Clone, Default)]
pub(crate) struct ScanTimingShmem {
    /// Marker: non-zero when this slot was written by its worker.
    pub(crate) populated: u64,
    pub(crate) metadata_us: u64,
    pub(crate) heap_scan_us: u64,
    pub(crate) decompress_us: u64,
    pub(crate) phase1_us: u64,
    pub(crate) phase2_us: u64,
    pub(crate) phase2_text_us: u64,
    pub(crate) phase2_nontext_us: u64,
    pub(crate) phase2_text_cols: u64,
    pub(crate) phase2_nontext_cols: u64,
    pub(crate) emit_us: u64,
    pub(crate) rows_emitted: u64,
    pub(crate) rows_filtered: u64,
    pub(crate) batch_eval_us: u64,
    pub(crate) rows_batch_filtered: u64,
    pub(crate) segments_decompressed: u64,
    pub(crate) compressed_bytes: u64,
    pub(crate) segments_skipped: u64,
    pub(crate) segments_minmax_skipped: u64,
    pub(crate) segments_bloom_skipped: u64,
    pub(crate) segments_valbitmap_skipped: u64,
    pub(crate) phase2_skipped: u64,
    pub(crate) blob_cache_hits: u64,
    pub(crate) blob_cache_misses: u64,
    pub(crate) blob_cache_bytes_served: u64,
}

/// Leader + up to 32 workers. PG typically uses much less; we hard-error
/// if the planner requests more than this cap.
pub(crate) const MAX_WORKER_SLOTS: usize = 33;

/// DSM-resident shared state for parallel DeltaXAppend.
///
/// Layout is POD (no rust-side Drop, no pointers). `AtomicU64` has the
/// same memory representation as `u64`, so zero-initialized DSM is a
/// valid state (`next_segment = 0`, nothing populated yet).
#[repr(C)]
pub(crate) struct DeltaXAppendPState {
    /// Shared cursor: workers call `fetch_add(1)` to claim the next
    /// segment index. When it reaches `total_segments`, the scan is done.
    pub(crate) next_segment: AtomicU64,
    /// Total segments in the leader's `segments_data`. Set during
    /// `InitializeDSMCustomScan` after the leader's `BeginCustomScan` ran.
    pub(crate) total_segments: u64,
    /// Number of timing slots (== worker-cap + 1 for the leader).
    pub(crate) n_worker_slots: u32,
    _pad: u32,
    /// Per-process timing aggregation. Slot 0 = leader, slots 1..=N = workers.
    pub(crate) worker_timings: [ScanTimingShmem; MAX_WORKER_SLOTS],
}

/// Returns the DSM slot index for the current process.
/// Leader (ParallelWorkerNumber == -1) → slot 0; worker K → slot K + 1.
impl ScanTiming {
    /// Fold a `DetoastLazyStats` (returned by `detoast_lazy_blobs` /
    /// `detoast_lazy_blobs_selective`) into the per-process counters.
    pub(crate) fn fold_detoast_stats(&mut self, s: super::segments::DetoastLazyStats) {
        self.blob_cache_hits += s.cache_hits;
        self.blob_cache_misses += s.cache_misses;
        self.blob_cache_bytes_served += s.cache_bytes_served;
    }
}

/// Compute the column indices that Phase 1 of the Top-N two-pass scan
/// will decompress: the sort column plus any column referenced by a
/// batch qual (text Eq/Ne/In/LIKE or numeric).
fn compute_phase1_col_indices(
    col_names: &[String],
    segment_by: &[String],
    needed_cols: &[bool],
    batch_quals: &[BatchQual],
    sort_col: usize,
) -> Vec<usize> {
    let mut out = Vec::new();
    for (col_idx, col_name) in col_names.iter().enumerate() {
        if !needed_cols[col_idx] && col_idx != sort_col {
            continue;
        }
        let has_batch_qual = batch_quals.iter().any(|bq| bq.col_idx == col_idx);
        if segment_by.contains(col_name) {
            if has_batch_qual {
                out.push(col_idx);
            }
        } else if has_batch_qual || col_idx == sort_col {
            out.push(col_idx);
        }
    }
    out
}

/// Map Phase 1 column indices to blob indices for `detoast_lazy_blobs_selective`.
/// Non-`segment_by` columns are indexed densely into `compressed_blobs`.
fn compute_phase1_blob_indices(
    col_names: &[String],
    segment_by: &[String],
    phase1_col_indices: &[usize],
) -> Vec<usize> {
    let mut indices = Vec::new();
    let mut bi: usize = 0;
    for (col_idx, col_name) in col_names.iter().enumerate() {
        if segment_by.contains(col_name) {
            continue;
        }
        if phase1_col_indices.contains(&col_idx) {
            indices.push(bi);
        }
        bi += 1;
    }
    indices
}

/// AND-merge a freshly-produced selection vector into a running one.
/// Empty target adopts `src` wholesale; otherwise positions are AND-ed.
/// Both vectors must have the same length when target is non-empty.
#[inline]
fn merge_and_selection(target: &mut Vec<bool>, src: Vec<bool>) {
    if target.is_empty() {
        *target = src;
    } else {
        for (t, s) in target.iter_mut().zip(src.iter()) {
            *t = *t && *s;
        }
    }
}

/// Metadata-only pruning: returns true if the segment cannot match the
/// query given its segment-by values and time range. Does NOT touch
/// compressed blobs — safe to call before detoast.
///
/// Empty segments (`row_count == 0`) are *not* reported here; callers
/// already check that and don't increment `segments_skipped` for them.
fn segment_pre_pruned_by_metadata(
    seg: &SegmentData,
    seg_by_filters: &[(usize, String)],
    time_min: Option<i64>,
    time_max: Option<i64>,
) -> bool {
    for (svi, filter_val) in seg_by_filters {
        match &seg.segment_values[*svi] {
            Some(val) if val == filter_val => {}
            _ => return true,
        }
    }
    if let (Some(seg_min), Some(seg_max)) = (seg.min_time, seg.max_time) {
        if time_min.is_some_and(|qmin| seg_max < qmin) {
            return true;
        }
        if time_max.is_some_and(|qmax| seg_min > qmax) {
            return true;
        }
    }
    false
}

/// Raw shape of the planner-emitted `custom_private` `IntList`.
///
/// Layout (each section optional except `header`):
///   header_ints ... | -1 | needed_indices ... | -2 | topn_block ... | -3 | synth_indices ...
///
/// `header_ints` are interpreted by the caller (single companion OID for
/// `DeltaXDecompress`, vector of OIDs for `DeltaXAppend`).
struct ParsedCustomPrivate {
    header: Vec<i32>,
    needed_indices: Vec<usize>,
    topn: Option<TopNRaw>,
}

struct TopNRaw {
    limit: i64,
    ascending: bool,
    nulls_first: bool,
    multi_col_sort: bool,
    sort_col_attno: i32,
}

/// Walk a `custom_private` `IntList` produced by the planner. Returns the
/// header ints (before `-1`), the needed-column indices (after `-1` and
/// `-3` sections), and the optional Top-N block (after `-2`).
unsafe fn parse_custom_private(list: *mut pg_sys::List) -> ParsedCustomPrivate {
    let mut header = Vec::new();
    let mut needed_indices = Vec::new();
    let mut topn: Option<TopNRaw> = None;
    if list.is_null() {
        return ParsedCustomPrivate {
            header,
            needed_indices,
            topn,
        };
    }
    let len = unsafe { (*list).length };
    let mut sec = 0u8; // 0 = header, 1 = cols, 2 = topn, 3 = synth
    let mut i: i32 = 0;
    while i < len {
        let val = unsafe { pg_sys::list_nth_int(list, i) };
        if val == -1 && sec == 0 {
            sec = 1;
            i += 1;
            continue;
        }
        if val == -2 && sec == 1 {
            let read = |off: i32| -> Option<i32> {
                if i + off < len {
                    Some(unsafe { pg_sys::list_nth_int(list, i + off) })
                } else {
                    None
                }
            };
            topn = Some(TopNRaw {
                limit: read(1).unwrap_or(0) as i64,
                ascending: read(2).unwrap_or(1) != 0,
                multi_col_sort: read(3).unwrap_or(0) != 0,
                sort_col_attno: read(4).unwrap_or(0),
                nulls_first: read(5).unwrap_or(0) != 0,
            });
            sec = 2;
            i += 6;
            continue;
        }
        if val == -3 && sec >= 1 {
            sec = 3;
            i += 1;
            continue;
        }
        match sec {
            0 => header.push(val),
            1 | 3 if val >= 0 => needed_indices.push(val as usize),
            _ => {}
        }
        i += 1;
    }
    ParsedCustomPrivate {
        header,
        needed_indices,
        topn,
    }
}

unsafe fn current_worker_slot() -> usize {
    let n = unsafe { pg_sys::ParallelWorkerNumber };
    if n < 0 {
        0
    } else {
        ((n as usize) + 1).min(MAX_WORKER_SLOTS - 1)
    }
}

/// Copy the running `ScanTiming` counters into this process's DSM slot.
/// Called from `ShutdownCustomScan` (before state teardown) so the leader
/// can aggregate per-worker numbers for EXPLAIN output.
unsafe fn flush_timing_to_shmem(state: &DecompressState) {
    unsafe {
        if state.pscan.is_null() {
            return;
        }
        let slot_idx = current_worker_slot();
        let ps = &mut *state.pscan;
        if slot_idx >= ps.n_worker_slots as usize {
            return;
        }
        let slot = &mut ps.worker_timings[slot_idx];
        let t = &state.timing;
        slot.metadata_us = t.metadata_us;
        slot.heap_scan_us = t.heap_scan_us;
        slot.decompress_us = t.decompress_us;
        slot.phase1_us = t.phase1_us;
        slot.phase2_us = t.phase2_us;
        slot.phase2_text_us = t.phase2_text_us;
        slot.phase2_nontext_us = t.phase2_nontext_us;
        slot.phase2_text_cols = t.phase2_text_cols;
        slot.phase2_nontext_cols = t.phase2_nontext_cols;
        slot.emit_us = t.emit_us;
        slot.rows_emitted = t.rows_emitted;
        slot.rows_filtered = t.rows_filtered;
        slot.batch_eval_us = t.batch_eval_us;
        slot.rows_batch_filtered = t.rows_batch_filtered;
        slot.segments_decompressed = t.segments_decompressed;
        slot.compressed_bytes = t.compressed_bytes;
        slot.segments_skipped = t.segments_skipped;
        slot.segments_minmax_skipped = t.segments_minmax_skipped;
        slot.segments_bloom_skipped = t.segments_bloom_skipped;
        slot.segments_valbitmap_skipped = t.segments_valbitmap_skipped;
        slot.phase2_skipped = t.phase2_skipped;
        slot.blob_cache_hits = t.blob_cache_hits;
        slot.blob_cache_misses = t.blob_cache_misses;
        slot.blob_cache_bytes_served = t.blob_cache_bytes_served;
        // populated must be written last so the leader can treat slots
        // missing this flag (e.g. a worker that crashed) as absent.
        slot.populated = 1;
    }
}

/// CreateCustomScanState callback.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn create_custom_scan_state(
    cscan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    unsafe {
        let css = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScanState>())
            as *mut pg_sys::CustomScanState;

        (*css).ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
        (*css).methods = &CUSTOM_EXEC_METHODS.0;

        // Copy custom_private (companion OID list) for use in BeginCustomScan
        (*css).custom_ps = (*cscan).custom_private;

        css as *mut pg_sys::Node
    }
}

/// BeginCustomScan callback: initialize decompression state.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn begin_custom_scan(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: i32,
) {
    unsafe {
        // Get custom_private (stored as IntList: [oid, -1, col0, col1, ...])
        let custom_private = (*node).custom_ps;
        if custom_private.is_null() {
            pgrx::error!("pg_deltax: missing companion table OID in custom scan state");
        }

        let parsed = parse_custom_private(custom_private);
        if parsed.header.is_empty() {
            pgrx::error!("pg_deltax: custom_private has no companion OID");
        }
        let companion_oid = pg_sys::Oid::from(parsed.header[0] as u32);
        let needed_indices = parsed.needed_indices;
        let (
            topn_limit,
            topn_ascending,
            topn_nulls_first,
            topn_multi_col_sort,
            topn_sort_col_attno,
        ) = match &parsed.topn {
            Some(t) => (
                t.limit,
                t.ascending,
                t.nulls_first,
                t.multi_col_sort,
                t.sort_col_attno,
            ),
            None => (0i64, true, false, false, 0i32),
        };

        // Get companion table name
        let companion_name = {
            let name_ptr = pg_sys::get_rel_name(companion_oid);
            if name_ptr.is_null() {
                pgrx::error!(
                    "pg_deltax: companion table not found for OID {}",
                    u32::from(companion_oid)
                );
            }
            std::ffi::CStr::from_ptr(name_ptr)
                .to_string_lossy()
                .into_owned()
        };

        // Load metadata via SPI, then load segment data via direct heap scan
        // (plan_qual is passed so batch qual columns are included in needed_cols)
        let plan_qual = (*(*node).ss.ps.plan).qual;
        let mut state = load_decompress_state(
            companion_oid,
            &companion_name,
            &needed_indices,
            plan_qual,
            topn_limit,
        );

        // If all plan quals are handled by batch eval, skip PG's per-row ExecQual
        if state.all_quals_batch_handled {
            (*node).ss.ps.qual = std::ptr::null_mut();
        }

        // Set Top-N fields
        state.topn_limit = if topn_limit > 0 {
            topn_limit as usize
        } else {
            0
        };
        state.topn_ascending = topn_ascending;
        state.topn_nulls_first = topn_nulls_first;
        state.topn_multi_col_sort = topn_multi_col_sort;
        state.timing.topn_limit = if topn_limit > 0 { topn_limit as u64 } else { 0 };
        if topn_limit > 0 && topn_sort_col_attno > 0 {
            // Use the sort column attno from the planner (1-based → 0-based)
            let sort_col_idx = (topn_sort_col_attno - 1) as usize;
            if sort_col_idx < state.col_names.len() {
                state.topn_sort_col = Some(sort_col_idx);
                state.topn_sort_is_text = is_text_type(state.col_types[sort_col_idx]);
            }
        } else if topn_limit > 0 {
            // Fallback: assume time column (backward compat)
            state.topn_sort_col = state
                .col_names
                .iter()
                .position(|n| n == &state._time_column);
        }

        // Create per-segment memory context
        let query_ctx = (*(*node).ss.ps.state).es_query_cxt;
        state.segment_mcxt = pg_sys::AllocSetContextCreateInternal(
            query_ctx,
            c"DeltaXSegment".as_ptr(),
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        );

        // Sort segments by min_time for time-ordered output
        state
            .segments_data
            .sort_by_key(|s| s.min_time.unwrap_or(i64::MAX));

        // Box and store as raw pointer in custom_ps
        let state_box = Box::new(state);
        let state_ptr = Box::into_raw(state_box);
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}

/// CreateCustomScanState callback for DeltaXAppend.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn create_deltax_append_state(
    cscan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    unsafe {
        let css = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScanState>())
            as *mut pg_sys::CustomScanState;

        (*css).ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
        (*css).methods = &DELTAX_APPEND_EXEC_METHODS.0;

        // Copy custom_private for use in BeginCustomScan
        (*css).custom_ps = (*cscan).custom_private;

        css as *mut pg_sys::Node
    }
}

/// Construct a worker-side stub `DecompressState` that `init_worker_deltax_append`
/// fills in from the leader's serialised DSM metadata. Every field starts
/// empty/default; `is_worker_stub = true` catches exec-time bugs that would
/// otherwise silently run against empty state.
fn make_worker_stub_state() -> DecompressState {
    DecompressState {
        col_names: Vec::new(),
        col_types: Vec::new(),
        col_typmods: Vec::new(),
        segment_by: Vec::new(),
        current_segment: Vec::new(),
        current_row_count: 0,
        row_cursor: 0,
        segment_index: 0,
        segments_data: Vec::new(),
        needed_cols: Vec::new(),
        needed_col_indices: Vec::new(),
        segment_mcxt: std::ptr::null_mut(),
        timing: ScanTiming::default(),
        instrument: None,
        _time_column: String::new(),
        rows_sorted_by_time: true,
        segment_by_filters: Vec::new(),
        time_min: None,
        time_max: None,
        batch_quals: Vec::new(),
        all_quals_batch_handled: false,
        selection_vector: Vec::new(),
        topn_limit: 0,
        topn_ascending: true,
        topn_nulls_first: false,
        topn_multi_col_sort: false,
        topn_sort_col: None,
        topn_buffer: Vec::new(),
        topn_cursor: 0,
        topn_done: false,
        topn_sort_is_text: false,
        pscan: std::ptr::null_mut(),
        wire_base: std::ptr::null(),
        wire_view: None,
        is_worker_stub: true,
        cached_worker_timings: Vec::new(),
    }
}

/// BeginCustomScan callback for DeltaXAppend: load segments from all companion tables.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn begin_deltax_append(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: i32,
) {
    unsafe {
        // Parallel-worker short-circuit (§5.7). The worker's `begin` is called
        // before DSM is attached, so we cannot read metadata here. Install a
        // stub `DecompressState` and defer full hydration to
        // `init_worker_deltax_append`, which runs once DSM is wired up.
        if pg_sys::ParallelWorkerNumber >= 0 {
            let stub = Box::new(make_worker_stub_state());
            (*node).custom_ps = Box::into_raw(stub) as *mut pg_sys::List;
            return;
        }

        let custom_private = (*node).custom_ps;
        if custom_private.is_null() {
            pgrx::error!("pg_deltax: missing companion table OIDs in DeltaXAppend state");
        }

        let parsed = parse_custom_private(custom_private);
        let companion_oids: Vec<pg_sys::Oid> = parsed
            .header
            .iter()
            .map(|&v| pg_sys::Oid::from(v as u32))
            .collect();
        let needed_indices = parsed.needed_indices;
        let (
            topn_limit,
            topn_ascending,
            topn_nulls_first,
            topn_multi_col_sort,
            topn_sort_col_attno,
        ) = match &parsed.topn {
            Some(t) => (
                t.limit,
                t.ascending,
                t.nulls_first,
                t.multi_col_sort,
                t.sort_col_attno,
            ),
            None => (0i64, true, false, false, 0i32),
        };

        if companion_oids.is_empty() {
            pgrx::error!("pg_deltax: DeltaXAppend has no companion tables");
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

        // Extract batch quals early — we need to know which extra columns to load
        let plan_qual = (*(*node).ss.ps.plan).qual;
        let (batch_quals, handled_count) =
            extract_batch_quals(plan_qual, &meta.col_names, &meta.col_types);
        let nquals = if plan_qual.is_null() {
            0
        } else {
            (*plan_qual).length as usize
        };
        let all_quals_batch_handled = handled_count > 0 && handled_count == nquals;
        if all_quals_batch_handled {
            // All quals are handled by batch eval — skip PG's per-row ExecQual
            (*node).ss.ps.qual = std::ptr::null_mut();
        }

        // Build needed_cols and needed_col_indices (includes batch qual columns)
        let num_cols = meta.col_names.len();
        let (needed_cols, needed_col_indices) = {
            let mut nc = vec![false; num_cols];
            let mut nci = Vec::new();
            for &idx in &needed_indices {
                if idx < num_cols {
                    nc[idx] = true;
                    nci.push(idx);
                }
            }
            for bq in &batch_quals {
                if bq.col_idx < num_cols && !nc[bq.col_idx] {
                    nc[bq.col_idx] = true;
                    nci.push(bq.col_idx);
                }
            }
            (nc, nci)
        };

        // Extract segment pruning filters BEFORE heap scan for lazy detoasting
        let (seg_filters, t_min, t_max) = extract_segment_filters(
            plan_qual,
            &meta.col_names,
            &meta.segment_by,
            &meta.time_column,
        );

        // For Top-N: mark ALL needed columns as lazy (defer TOAST detoasting).
        // Segments are processed in time order with early stop, so segments
        // beyond the threshold are never detoasted — critical for cold-run I/O.
        // Phase 1 blobs are detoasted per-segment inside exec_topn_two_pass.
        //
        // For non-Top-N, we pass `skip_blob_load = true` below so compressed_blobs
        // are *not* loaded at `begin` time; each segment's blobs are fetched
        // on-claim by `fetch_segment_blobs` in `load_next_segment`. This
        // amortises the blob-detoast cost across workers and lets workers share
        // segment metadata via DSM (they fetch only the blobs for segments they
        // actually claim).
        let lazy_cols: Option<Vec<bool>> = if topn_limit > 0 {
            let mut lc = vec![false; num_cols];
            for idx in 0..num_cols {
                if needed_cols[idx] {
                    lc[idx] = true;
                }
            }
            Some(lc)
        } else {
            None
        };
        let skip_blob_load = topn_limit == 0;

        // Load segments from ALL companion tables via heap scan (with lazy pruning)
        super::segments::reset_scan_buf_stats();
        let t1 = Instant::now();
        let mut all_segments: Vec<SegmentData> = Vec::new();
        let mut total_skipped: u64 = 0;
        let mut total_minmax_skipped: u64 = 0;
        let mut total_bloom_skipped: u64 = 0;
        let mut total_valbitmap_skipped: u64 = 0;
        for &oid in &companion_oids {
            // Segments carry their source companion OID so `fetch_segment_blobs`
            // can re-open the right `_blobs` table per claimed segment.
            let (mut segs, skipped, mm_skipped, bloom_skipped, vb_skipped, _dt_us) =
                load_segments_heap(
                    oid,
                    &meta.col_names,
                    &meta.segment_by,
                    &needed_cols,
                    &meta.time_column,
                    false,
                    &seg_filters,
                    t_min,
                    t_max,
                    lazy_cols.as_deref(),
                    &batch_quals,
                    &[],
                    &meta.col_types,
                    &[],
                    skip_blob_load,
                );
            for s in &mut segs {
                s.companion_oid = oid;
            }
            all_segments.extend(segs);
            total_skipped += skipped;
            total_minmax_skipped += mm_skipped;
            total_bloom_skipped += bloom_skipped;
            total_valbitmap_skipped += vb_skipped;
        }
        let heap_scan_us = t1.elapsed().as_micros() as u64;
        let total_buf_stats = super::segments::take_scan_buf_stats();

        let compressed_bytes: u64 = all_segments
            .iter()
            .map(|s| {
                s.compressed_blobs
                    .iter()
                    .map(|b| b.len() as u64)
                    .sum::<u64>()
            })
            .sum();

        // Compute topn_sort_col before moving meta fields
        let topn_sort_col = if topn_limit > 0 && topn_sort_col_attno > 0 {
            let idx = (topn_sort_col_attno - 1) as usize;
            if idx < meta.col_names.len() {
                Some(idx)
            } else {
                None
            }
        } else if topn_limit > 0 {
            meta.col_names.iter().position(|n| n == &meta.time_column)
        } else {
            None
        };
        let topn_sort_is_text = topn_sort_col
            .map(|ci| is_text_type(meta.col_types[ci]))
            .unwrap_or(false);
        // Rows within a segment are sorted by order_by[0]. If that's the time column
        // (or order_by is empty, which defaults to time column), we can exploit
        // intra-segment row ordering for top-N early exit.
        let rows_sorted_by_time = meta.order_by.is_empty()
            || meta
                .order_by
                .first()
                .is_some_and(|c| c == &meta.time_column);

        let mut state = DecompressState {
            col_names: meta.col_names,
            col_types: meta.col_types,
            col_typmods: meta.col_typmods,
            segment_by: meta.segment_by,
            current_segment: Vec::new(),
            current_row_count: 0,
            row_cursor: 0,
            segment_index: 0,
            segments_data: all_segments,
            needed_cols,
            needed_col_indices,
            segment_mcxt: std::ptr::null_mut(),
            timing: ScanTiming {
                metadata_us,
                heap_scan_us,
                compressed_bytes,
                segments_skipped: total_skipped,
                segments_minmax_skipped: total_minmax_skipped,
                segments_bloom_skipped: total_bloom_skipped,
                segments_valbitmap_skipped: total_valbitmap_skipped,
                buf_stats: total_buf_stats,
                topn_limit: if topn_limit > 0 { topn_limit as u64 } else { 0 },
                ..Default::default()
            },
            instrument: None,
            _time_column: meta.time_column,
            rows_sorted_by_time,
            segment_by_filters: seg_filters,
            time_min: t_min,
            time_max: t_max,
            batch_quals,
            all_quals_batch_handled,
            selection_vector: Vec::new(),
            topn_limit: if topn_limit > 0 {
                topn_limit as usize
            } else {
                0
            },
            topn_ascending,
            topn_nulls_first,
            topn_multi_col_sort,
            topn_sort_col,
            topn_buffer: Vec::new(),
            topn_cursor: 0,
            topn_done: false,
            topn_sort_is_text,
            pscan: std::ptr::null_mut(),
            wire_base: std::ptr::null(),
            wire_view: None,
            is_worker_stub: false,
            cached_worker_timings: Vec::new(),
        };

        // Create per-segment memory context
        let query_ctx = (*(*node).ss.ps.state).es_query_cxt;
        state.segment_mcxt = pg_sys::AllocSetContextCreateInternal(
            query_ctx,
            c"DeltaXSegment".as_ptr(),
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        );

        // Sort segments by min_time for time-ordered output
        state
            .segments_data
            .sort_by_key(|s| s.min_time.unwrap_or(i64::MAX));

        let state_box = Box::new(state);
        let state_ptr = Box::into_raw(state_box);
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}

/// Load decompression state: metadata via SPI, segment data via direct heap scan.
///
/// `needed_indices` contains 0-based column indices the query needs.
/// If empty, all columns are loaded (safety fallback).
/// Only compressed blobs for needed columns are loaded from the companion table;
/// unneeded columns get empty placeholder blobs to keep index mapping correct.
fn load_decompress_state(
    companion_oid: pg_sys::Oid,
    companion_name: &str,
    needed_indices: &[usize],
    plan_qual: *mut pg_sys::List,
    topn_limit: i64,
) -> DecompressState {
    // Phase 1: SPI for metadata only (small, fast)
    let t0 = Instant::now();
    let meta = Spi::connect(|client| load_metadata(client, companion_name));
    let metadata_us = t0.elapsed().as_micros() as u64;

    // Extract batch quals early — we need to know which extra columns to load
    let (batch_quals, handled_count) =
        unsafe { extract_batch_quals(plan_qual, &meta.col_names, &meta.col_types) };
    let nquals = if plan_qual.is_null() {
        0
    } else {
        (unsafe { (*plan_qual).length }) as usize
    };
    let all_quals_batch_handled = handled_count > 0 && handled_count == nquals;

    // Build needed_cols and needed_col_indices from needed_indices + batch qual columns
    let num_cols = meta.col_names.len();
    let (needed_cols, needed_col_indices) = {
        let mut nc = vec![false; num_cols];
        let mut nci = Vec::new();
        for &idx in needed_indices {
            if idx < num_cols {
                nc[idx] = true;
                nci.push(idx);
            }
        }
        // Also include columns referenced by batch quals
        for bq in &batch_quals {
            if bq.col_idx < num_cols && !nc[bq.col_idx] {
                nc[bq.col_idx] = true;
                nci.push(bq.col_idx);
            }
        }
        (nc, nci)
    };

    // For Top-N: mark ALL needed columns as lazy (defer TOAST detoasting).
    // Segments are processed in time order with early stop, so segments
    // beyond the threshold are never detoasted — critical for cold-run I/O.
    let lazy_cols: Option<Vec<bool>> = if topn_limit > 0 {
        let mut lc = vec![false; num_cols];
        for idx in 0..num_cols {
            if needed_cols[idx] {
                lc[idx] = true;
            }
        }
        Some(lc)
    } else {
        None
    };

    // Extract segment pruning filters BEFORE heap scan for lazy detoasting
    let (seg_filters, t_min, t_max) = unsafe {
        extract_segment_filters(
            plan_qual,
            &meta.col_names,
            &meta.segment_by,
            &meta.time_column,
        )
    };

    // Phase 2: Direct heap scan for segment data (bypasses SPI overhead)
    super::segments::reset_scan_buf_stats();
    let t1 = Instant::now();
    let (
        segments_data,
        segments_skipped,
        minmax_skipped,
        bloom_skipped,
        valbitmap_skipped,
        _detoast_us,
    ) = unsafe {
        load_segments_heap(
            companion_oid,
            &meta.col_names,
            &meta.segment_by,
            &needed_cols,
            &meta.time_column,
            false,
            &seg_filters,
            t_min,
            t_max,
            lazy_cols.as_deref(),
            &batch_quals,
            &[],
            &meta.col_types,
            &[],
            false,
        )
    };
    let heap_scan_us = t1.elapsed().as_micros() as u64;
    let buf_stats = super::segments::take_scan_buf_stats();

    let compressed_bytes: u64 = segments_data
        .iter()
        .map(|s| {
            s.compressed_blobs
                .iter()
                .map(|b| b.len() as u64)
                .sum::<u64>()
        })
        .sum();

    let rows_sorted_by_time = meta.order_by.is_empty()
        || meta
            .order_by
            .first()
            .is_some_and(|c| c == &meta.time_column);

    DecompressState {
        col_names: meta.col_names,
        col_types: meta.col_types,
        col_typmods: meta.col_typmods,
        segment_by: meta.segment_by,
        current_segment: Vec::new(),
        current_row_count: 0,
        row_cursor: 0,
        segment_index: 0,
        segments_data,
        needed_cols,
        needed_col_indices,
        segment_mcxt: std::ptr::null_mut(),
        timing: ScanTiming {
            metadata_us,
            heap_scan_us,
            compressed_bytes,
            segments_skipped,
            segments_minmax_skipped: minmax_skipped,
            segments_bloom_skipped: bloom_skipped,
            segments_valbitmap_skipped: valbitmap_skipped,
            buf_stats,
            ..Default::default()
        },
        instrument: None,
        _time_column: meta.time_column,
        rows_sorted_by_time,
        segment_by_filters: seg_filters,
        time_min: t_min,
        time_max: t_max,
        batch_quals,
        all_quals_batch_handled,
        selection_vector: Vec::new(),
        topn_limit: 0,
        topn_ascending: true,
        topn_nulls_first: false,
        topn_multi_col_sort: false,
        topn_sort_col: None,
        topn_buffer: Vec::new(),
        topn_cursor: 0,
        topn_done: false,
        topn_sort_is_text: false,
        pscan: std::ptr::null_mut(),
        wire_base: std::ptr::null(),
        wire_view: None,
        is_worker_stub: false,
        cached_worker_timings: Vec::new(),
    }
}

/// Candidate row for Top-N selection (text sort keys).
struct TextTopNCandidate {
    segment_idx: usize,
    row_idx: usize,
    /// Sort key datum (points into phase1_persist_mcxt, survives segment resets).
    sort_datum: pg_sys::Datum,
    sort_is_null: bool,
    /// Datums for Phase 1 columns (filter + sort), keyed by col_idx.
    phase1_datums: Vec<(usize, pg_sys::Datum, bool)>,
}

/// Compare two text datums using PG's collation-aware comparison.
#[inline]
unsafe fn cmp_text_datums(a: pg_sys::Datum, b: pg_sys::Datum) -> std::cmp::Ordering {
    unsafe {
        let a_vl = a.cast_mut_ptr::<pg_sys::varlena>();
        let b_vl = b.cast_mut_ptr::<pg_sys::varlena>();
        let a_ptr = pgrx::vardata_any(a_vl) as *const std::ffi::c_char;
        let a_len = pgrx::varsize_any_exhdr(a_vl) as i32;
        let b_ptr = pgrx::vardata_any(b_vl) as *const std::ffi::c_char;
        let b_len = pgrx::varsize_any_exhdr(b_vl) as i32;
        let result = pg_sys::varstr_cmp(a_ptr, a_len, b_ptr, b_len, pg_sys::DEFAULT_COLLATION_OID);
        result.cmp(&0)
    }
}

unsafe fn cmp_text_key(
    a: pg_sys::Datum,
    a_is_null: bool,
    b: pg_sys::Datum,
    b_is_null: bool,
    ascending: bool,
    nulls_first: bool,
) -> std::cmp::Ordering {
    unsafe {
        match (a_is_null, b_is_null) {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => {
                if nulls_first {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            }
            (false, true) => {
                if nulls_first {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }
            (false, false) => {
                if ascending {
                    cmp_text_datums(a, b)
                } else {
                    cmp_text_datums(b, a)
                }
            }
        }
    }
}

unsafe fn cmp_text_candidate(
    a: &TextTopNCandidate,
    b: &TextTopNCandidate,
    ascending: bool,
    nulls_first: bool,
) -> std::cmp::Ordering {
    unsafe {
        cmp_text_key(
            a.sort_datum,
            a.sort_is_null,
            b.sort_datum,
            b.sort_is_null,
            ascending,
            nulls_first,
        )
    }
}

fn cmp_nullable_str_byte(
    a: Option<&str>,
    b: Option<&str>,
    ascending: bool,
    nulls_first: bool,
) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => {
            if nulls_first {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls_first {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }
        (Some(a), Some(b)) => {
            if ascending {
                a.cmp(b)
            } else {
                b.cmp(a)
            }
        }
    }
}

fn cmp_nullable_str_collation(
    a: Option<&str>,
    b: Option<&str>,
    ascending: bool,
    nulls_first: bool,
) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => {
            if nulls_first {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls_first {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }
        (Some(a), Some(b)) => {
            if ascending {
                strcoll_cmp(a, b)
            } else {
                strcoll_cmp(b, a)
            }
        }
    }
}

/// Candidate row for Top-N selection.
struct TopNCandidate {
    segment_idx: usize,
    row_idx: usize,
    sort_key: i64,
    sort_is_null: bool,
    /// Datums for Phase 1 columns (filter + sort), keyed by col_idx.
    /// Stored so Phase 2 can skip re-decompressing these columns.
    phase1_datums: Vec<(usize, pg_sys::Datum, bool)>,
}

fn cmp_topn_key(
    a_key: i64,
    a_is_null: bool,
    b_key: i64,
    b_is_null: bool,
    ascending: bool,
    nulls_first: bool,
) -> std::cmp::Ordering {
    match (a_is_null, b_is_null) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => {
            if nulls_first {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        }
        (false, true) => {
            if nulls_first {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }
        (false, false) => {
            if ascending {
                a_key.cmp(&b_key)
            } else {
                b_key.cmp(&a_key)
            }
        }
    }
}

fn cmp_topn_candidate(
    a: &TopNCandidate,
    b: &TopNCandidate,
    ascending: bool,
    nulls_first: bool,
) -> std::cmp::Ordering {
    cmp_topn_key(
        a.sort_key,
        a.sort_is_null,
        b.sort_key,
        b.sort_is_null,
        ascending,
        nulls_first,
    )
}

/// Execute the two-pass Top-N optimization.
///
/// Pass 1: For all segments, run Phase 1 (decompress filter + sort columns),
///         evaluate batch quals, collect candidates with sort keys.
/// Pass 2: Sort candidates, truncate to top-N, then run Phase 2 only for
///         segments containing winning rows, with narrowed selection vectors.
///         Store results in `state.topn_buffer`.
unsafe fn exec_topn_two_pass(
    _node: *mut pg_sys::CustomScanState,
    state: &mut DecompressState,
    instrument: bool,
    plan_qual: *mut pg_sys::List,
) {
    unsafe {
        let sort_col = match state.topn_sort_col {
            Some(c) => c,
            None => return,
        };
        let effective_limit = state.topn_limit;

        // Safety check: Top-N is only correct when ALL plan.qual expressions
        // are covered by batch quals. If plan.qual has expressions not covered
        // by batch quals (e.g. filters on segment_by columns), those rows
        // won't be filtered during candidate collection, leading to wrong results.
        if !plan_qual.is_null() {
            let num_plan_quals = (*plan_qual).length as usize;
            let num_batch_quals = state.batch_quals.len();
            if num_plan_quals > num_batch_quals {
                pgrx::log!(
                    "pg_deltax topn: disabled (plan_quals={} > batch_quals={})",
                    num_plan_quals,
                    num_batch_quals,
                );
                // Detoast lazy blobs since normal scan path needs them
                for seg in state.segments_data.iter_mut() {
                    let dl = detoast_lazy_blobs(seg);
                    state.timing.fold_detoast_stats(dl);
                }
                state.topn_limit = 0;
                state.segment_index = 0;
                return;
            }
        }

        pgrx::log!(
            "pg_deltax topn: limit={} ascending={} sort_col={} segments={}",
            effective_limit,
            state.topn_ascending,
            sort_col,
            state.segments_data.len(),
        );

        // Identify which column indices are decompressed in Phase 1 (filter + sort).
        // We'll store their datums per-candidate so Phase 2 can skip them.
        let phase1_col_indices = compute_phase1_col_indices(
            &state.col_names,
            &state.segment_by,
            &state.needed_cols,
            &state.batch_quals,
            sort_col,
        );

        // Phase 1 blob indices for selective detoasting (non-segment-by only).
        let phase1_blob_indices =
            compute_phase1_blob_indices(&state.col_names, &state.segment_by, &phase1_col_indices);

        // === Pass 1: Phase 1 with early stop ===
        // Check if sort column is the time column — time-based optimizations
        // (segment ordering, threshold-based skipping, cutoff_row) only apply
        // when sorting by the time column.
        let sort_is_time_col = state
            .col_names
            .get(sort_col)
            .is_some_and(|name| name == &state._time_column);

        // Sort segments by time for early stop: ASC by min_time, DESC by max_time.
        // After collecting >= effective_limit candidates, skip segments whose
        // entire time range is beyond our worst candidate's sort key.
        let mut candidates: Vec<TopNCandidate> = Vec::new();
        let num_segments = state.segments_data.len();

        let mut seg_order: Vec<usize> = (0..num_segments).collect();
        if sort_is_time_col {
            if state.topn_ascending {
                seg_order.sort_by_key(|&i| state.segments_data[i].min_time.unwrap_or(i64::MAX));
            } else {
                seg_order.sort_by_key(|&i| {
                    std::cmp::Reverse(state.segments_data[i].max_time.unwrap_or(i64::MIN))
                });
            }
        }

        // Persistent memory context for Phase 1 text datums that must survive
        // across segments (stored in candidates for Phase 2 reuse).
        // Created under segment_mcxt's parent so Phase 1 resets don't destroy it.
        let phase1_persist_mcxt = pg_sys::AllocSetContextCreateInternal(
            (*state.segment_mcxt).parent,
            c"TopN Phase1 Persist".as_ptr(),
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        );

        // Track the worst (threshold) sort key among top-N candidates.
        // For ASC: worst = max sort_key; for DESC: worst = min sort_key.
        let mut topn_threshold: Option<i64> = None;

        for &seg_idx in &seg_order {
            // Early stop: if we have enough candidates, check if this segment's
            // time range can possibly contain rows better than our worst candidate.
            // Only valid when sorting by the time column (we have min/max metadata).
            if sort_is_time_col
                && candidates.len() >= effective_limit
                && let Some(threshold) = topn_threshold
            {
                let dominated = if state.topn_ascending {
                    // ASC: skip if segment's min_time > threshold (all rows are later)
                    state.segments_data[seg_idx]
                        .min_time
                        .is_some_and(|m| m > threshold)
                } else {
                    // DESC: skip if segment's max_time < threshold (all rows are earlier)
                    state.segments_data[seg_idx]
                        .max_time
                        .is_some_and(|m| m < threshold)
                };
                if dominated {
                    state.timing.segments_skipped += 1;
                    continue;
                }
            }

            // Pre-detoast pruning: uses only metadata (row_count, segment_values,
            // min/max time) which is available without detoasting blobs.
            {
                let seg = &state.segments_data[seg_idx];
                if seg.row_count == 0 {
                    continue;
                }
                if segment_pre_pruned_by_metadata(
                    seg,
                    &state.segment_by_filters,
                    state.time_min,
                    state.time_max,
                ) {
                    state.timing.segments_skipped += 1;
                    continue;
                }
            }

            // Detoast only Phase 1 (filter + sort) blobs for this segment.
            // Deferred until after early stop / segment-by / time-range pruning
            // so that skipped segments incur zero I/O on cold runs.
            // Phase 2 blobs remain lazy until Phase 2 (winning segments only).
            let t_detoast = if instrument {
                Some(Instant::now())
            } else {
                None
            };
            let dl = detoast_lazy_blobs_selective(
                &mut state.segments_data[seg_idx],
                &phase1_blob_indices,
            );
            state.timing.fold_detoast_stats(dl);
            if let Some(t) = t_detoast {
                state.timing.heap_scan_us += t.elapsed().as_micros() as u64;
            }
            let seg = &state.segments_data[seg_idx];

            // Dictionary-based LIKE pruning
            if segment_skippable_by_dict(
                &state.batch_quals,
                &state.col_names,
                &state.segment_by,
                &seg.compressed_blobs,
            ) {
                state.timing.segments_skipped += 1;
                continue;
            }

            let t_decompress = if instrument {
                Some(Instant::now())
            } else {
                None
            };

            // Reset segment memory context for Phase 1
            pg_sys::MemoryContextReset(state.segment_mcxt);
            let old_ctx = pg_sys::MemoryContextSwitchTo(state.segment_mcxt);

            // Sort-column-first optimization for top-N truncation.
            // When we have a threshold and rows are sorted by time, decompress
            // the sort column first to find a cutoff_row. Filter columns are
            // then truncated to only decompress rows up to cutoff_row.
            let mut cutoff_row: Option<usize> = None;
            let mut sort_col_already_decompressed = false;
            let mut sort_first_datums: Option<Vec<(pg_sys::Datum, bool)>> = None;

            // Only truncate for ASC: rows are sorted ascending, so the cutoff
            // truncates from the end. For DESC, the rows we want are at the end
            // of the ascending-sorted data, and our decompress infrastructure
            // doesn't support "skip from beginning" truncation.
            if let (true, true, true, Some(threshold)) = (
                sort_is_time_col,
                state.rows_sorted_by_time,
                state.topn_ascending,
                topn_threshold,
            ) {
                // Compute sort column's blob index
                let mut sort_blob_idx: Option<usize> = None;
                let mut bi: usize = 0;
                for (ci, cn) in state.col_names.iter().enumerate() {
                    if !state.segment_by.contains(cn) {
                        if ci == sort_col {
                            sort_blob_idx = Some(bi);
                        }
                        bi += 1;
                    }
                }

                if let Some(sbi) = sort_blob_idx {
                    let blob = &seg.compressed_blobs[sbi];
                    if !blob.is_empty() {
                        let type_oid = state.col_types[sort_col];
                        let typmod = state.col_typmods[sort_col];
                        let type_name = pg_type_name(type_oid);
                        let datums = decompress_blob_to_datums(blob, &type_name, type_oid, typmod);
                        let cutoff = datums
                            .iter()
                            .position(|(datum, is_null)| {
                                if *is_null {
                                    return false;
                                }
                                let key = datum.value() as i64;
                                key > threshold
                            })
                            .unwrap_or(datums.len());

                        if cutoff == 0 {
                            // All rows beyond threshold — skip segment entirely
                            continue;
                        }
                        cutoff_row = Some(cutoff);
                        sort_first_datums = Some(datums);
                        sort_col_already_decompressed = true;
                    }
                }
            }

            // Phase 1a: Decompress filter columns (sort column handled above when truncating)
            let mut decompressed: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
            let mut blob_idx: usize = 0;
            let mut seg_val_idx: usize = 0;
            let mut pre_selection: Vec<bool> = Vec::new();
            let mut sort_col_blob_idx: Option<usize> = None;

            for (col_idx, col_name) in state.col_names.iter().enumerate() {
                let type_oid = state.col_types[col_idx];

                if !state.needed_cols[col_idx] && col_idx != sort_col {
                    if state.segment_by.contains(col_name) {
                        seg_val_idx += 1;
                    } else {
                        blob_idx += 1;
                    }
                    decompressed.push(Vec::new());
                    continue;
                }

                if state.segment_by.contains(col_name) {
                    let val = &seg.segment_values[seg_val_idx];
                    let (datum, is_null) = match val {
                        Some(s) => (string_to_datum(s, type_oid), false),
                        None => (pg_sys::Datum::from(0), true),
                    };
                    let repeat_count = cutoff_row.unwrap_or(seg.row_count as usize);
                    let repeated: Vec<(pg_sys::Datum, bool)> =
                        (0..repeat_count).map(|_| (datum, is_null)).collect();
                    decompressed.push(repeated);

                    // Evaluate text Eq/Ne batch quals on segment_by columns.
                    if let Some(bq) = state.batch_quals.iter().find(|bq| {
                        bq.col_idx == col_idx
                            && bq.text_const.is_some()
                            && matches!(bq.op, BatchCompareOp::Eq | BatchCompareOp::Ne)
                    }) {
                        let const_str = bq.text_const.as_ref().unwrap();
                        let is_ne = bq.op == BatchCompareOp::Ne;
                        let matches = match val {
                            Some(s) => {
                                if is_ne {
                                    s != const_str
                                } else {
                                    s == const_str
                                }
                            }
                            None => false,
                        };
                        if !matches {
                            merge_and_selection(&mut pre_selection, vec![false; repeat_count]);
                        }
                    }

                    seg_val_idx += 1;
                } else {
                    let blob = &seg.compressed_blobs[blob_idx];
                    let typmod = state.col_typmods[col_idx];

                    let like_qual = state.batch_quals.iter().find(|bq| {
                        bq.col_idx == col_idx
                            && matches!(bq.op, BatchCompareOp::Like | BatchCompareOp::NotLike)
                    });
                    let text_eq_qual = state.batch_quals.iter().find(|bq| {
                        bq.col_idx == col_idx
                            && bq.text_const.is_some()
                            && matches!(bq.op, BatchCompareOp::Eq | BatchCompareOp::Ne)
                    });
                    let text_in_qual = state.batch_quals.iter().find(|bq| {
                        bq.col_idx == col_idx
                            && bq.in_list_text.is_some()
                            && bq.op == BatchCompareOp::InList
                    });
                    let has_any_batch_qual =
                        state.batch_quals.iter().any(|bq| bq.col_idx == col_idx);

                    if let Some(bq) = like_qual {
                        let strat = bq.like_strategy.as_ref().unwrap();
                        let neg = bq.op == BatchCompareOp::NotLike;
                        let (datums, sel) = decompress_text_blob_with_like_filter(
                            blob, type_oid, typmod, strat, neg, cutoff_row,
                        );
                        decompressed.push(datums);
                        merge_and_selection(&mut pre_selection, sel);
                    } else if let Some(bq) = text_eq_qual {
                        let const_str = bq.text_const.as_ref().unwrap();
                        let is_ne = bq.op == BatchCompareOp::Ne;
                        let (datums, sel) = decompress_text_blob_with_eq_filter(
                            blob, type_oid, typmod, const_str, is_ne, cutoff_row,
                        );
                        decompressed.push(datums);
                        merge_and_selection(&mut pre_selection, sel);
                    } else if let Some(bq) = text_in_qual {
                        let strs = bq.in_list_text.as_ref().unwrap();
                        let (datums, sel) = decompress_text_blob_with_in_filter(
                            blob, type_oid, typmod, strs, /* is_not_in */ false, cutoff_row,
                        );
                        decompressed.push(datums);
                        merge_and_selection(&mut pre_selection, sel);
                    } else if has_any_batch_qual {
                        let type_name = pg_type_name(type_oid);
                        if let Some(mr) = cutoff_row {
                            let datums = decompress_blob_to_datums_truncated(
                                blob,
                                &type_name,
                                type_oid,
                                typmod,
                                mr.saturating_sub(1),
                            );
                            decompressed.push(datums);
                        } else {
                            let datums =
                                decompress_blob_to_datums(blob, &type_name, type_oid, typmod);
                            decompressed.push(datums);
                        }
                    } else if col_idx == sort_col && !sort_col_already_decompressed {
                        sort_col_blob_idx = Some(blob_idx);
                        decompressed.push(Vec::new());
                    } else if col_idx == sort_col {
                        // Sort column already decompressed above — skip blob but keep placeholder
                        decompressed.push(Vec::new());
                    } else {
                        decompressed.push(Vec::new());
                    }
                    blob_idx += 1;
                }
            }

            pg_sys::MemoryContextSwitchTo(old_ctx);

            if let Some(t) = t_decompress {
                let elapsed = t.elapsed().as_micros() as u64;
                state.timing.decompress_us += elapsed;
                state.timing.phase1_us += elapsed;
            }
            state.timing.segments_decompressed += 1;

            let effective_row_count = cutoff_row.unwrap_or(seg.row_count as usize);

            // Evaluate batch quals
            let selection_vector = if !state.batch_quals.is_empty() || !pre_selection.is_empty() {
                let t_batch = if instrument {
                    Some(Instant::now())
                } else {
                    None
                };
                let sv = evaluate_batch_quals(
                    &decompressed,
                    effective_row_count,
                    &state.batch_quals,
                    pre_selection,
                );
                if let Some(t) = t_batch {
                    state.timing.batch_eval_us += t.elapsed().as_micros() as u64;
                }
                sv
            } else {
                Vec::new()
            };

            // Check if any rows passed
            let any_selected = selection_vector.is_empty() || selection_vector.iter().any(|&s| s);

            if !any_selected {
                continue;
            }

            // Phase 1b: Decompress sort column (only for segments with matches)
            if sort_col_already_decompressed {
                // Sort column was decompressed early for cutoff — insert stored datums
                if let Some(datums) = sort_first_datums.take() {
                    decompressed[sort_col] = datums;
                }
            } else if let Some(sort_bi) = sort_col_blob_idx {
                let t_sort = if instrument {
                    Some(Instant::now())
                } else {
                    None
                };
                let old_ctx2 = pg_sys::MemoryContextSwitchTo(state.segment_mcxt);
                let blob = &seg.compressed_blobs[sort_bi];
                let type_oid = state.col_types[sort_col];
                let typmod = state.col_typmods[sort_col];
                let type_name = pg_type_name(type_oid);
                let datums = decompress_blob_to_datums(blob, &type_name, type_oid, typmod);
                decompressed[sort_col] = datums;
                pg_sys::MemoryContextSwitchTo(old_ctx2);
                if let Some(t) = t_sort {
                    let elapsed = t.elapsed().as_micros() as u64;
                    state.timing.decompress_us += elapsed;
                    state.timing.phase1_us += elapsed;
                }
            }

            // Collect candidates with Phase 1 column datums
            let sort_datums = &decompressed[sort_col];
            if sort_datums.is_empty() {
                continue;
            }

            // Row-level early exit: when rows within this segment are sorted
            // by the time column (ascending) and we already have a threshold,
            // skip rows that can't possibly beat it.
            // For ASC: once sort_key > threshold, all subsequent rows are also
            // beyond threshold → break.
            // For DESC: rows we want are at the END of ascending data (highest
            // values). We can't break early, but we can skip individual rows
            // below threshold.
            let can_row_early_exit = sort_is_time_col
                && state.rows_sorted_by_time
                && state.topn_ascending
                && topn_threshold.is_some();
            let can_row_skip_desc = sort_is_time_col
                && state.rows_sorted_by_time
                && !state.topn_ascending
                && topn_threshold.is_some();
            let row_threshold = topn_threshold.unwrap_or(0);

            for row_idx in 0..effective_row_count {
                let passes = selection_vector.is_empty() || selection_vector[row_idx];
                if !passes {
                    continue;
                }
                let (datum, is_null) = sort_datums[row_idx];
                let sort_key = if is_null { 0 } else { datum.value() as i64 };

                // Row-level early exit (ASC only): rows are ascending, so once
                // we see a row beyond the threshold, all subsequent rows are too.
                if !is_null && can_row_early_exit && sort_key > row_threshold {
                    break;
                }
                // Row-level skip (DESC): skip rows below threshold (they can't
                // be in the top-N), but don't break — later rows may be above.
                if !is_null && can_row_skip_desc && sort_key < row_threshold {
                    continue;
                }

                // Store Phase 1 column datums for this candidate.
                // For text datums (varlena), copy to persistent context so they
                // survive segment_mcxt reset.
                let mut p1_datums: Vec<(usize, pg_sys::Datum, bool)> =
                    Vec::with_capacity(phase1_col_indices.len());
                for &ci in &phase1_col_indices {
                    if decompressed[ci].is_empty() {
                        continue;
                    }
                    let (d, isnull) = decompressed[ci][row_idx];
                    if isnull {
                        p1_datums.push((ci, pg_sys::Datum::from(0), true));
                    } else if is_text_type(state.col_types[ci]) {
                        // Copy varlena to persistent context
                        let varlena_ptr = d.cast_mut_ptr::<pg_sys::varlena>();
                        let size = pgrx::varsize_any(varlena_ptr);
                        let old_mc = pg_sys::MemoryContextSwitchTo(phase1_persist_mcxt);
                        let copy_ptr = pg_sys::palloc(size) as *mut u8;
                        std::ptr::copy_nonoverlapping(varlena_ptr as *const u8, copy_ptr, size);
                        pg_sys::MemoryContextSwitchTo(old_mc);
                        p1_datums.push((ci, pg_sys::Datum::from(copy_ptr as usize), false));
                    } else {
                        p1_datums.push((ci, d, false));
                    }
                }

                candidates.push(TopNCandidate {
                    segment_idx: seg_idx,
                    row_idx,
                    sort_key,
                    sort_is_null: is_null,
                    phase1_datums: p1_datums,
                });
            }

            // Update threshold for early stop: find the worst key in top-N so far
            if candidates.len() >= effective_limit {
                // Partial sort to find the N-th element (0-indexed: effective_limit - 1)
                let n = effective_limit - 1;
                candidates.select_nth_unstable_by(n, |a, b| {
                    cmp_topn_candidate(a, b, state.topn_ascending, state.topn_nulls_first)
                });
                topn_threshold = Some(candidates[n].sort_key);
                // Note: we intentionally do NOT truncate candidates here.
                // select_nth_unstable_by_key partitions but doesn't fully sort,
                // and truncating could drop valid tie candidates needed later.
            }
        }

        state.timing.topn_candidates = candidates.len() as u64;

        // If no candidates or all candidates fit in limit, fall back to normal path
        if candidates.is_empty() || candidates.len() <= effective_limit {
            // Detoast all lazy blobs since normal path needs them
            for seg in state.segments_data.iter_mut() {
                let dl = detoast_lazy_blobs(seg);
                state.timing.fold_detoast_stats(dl);
            }
            state.topn_limit = 0;
            state.segment_index = 0;
            pg_sys::MemoryContextDelete(phase1_persist_mcxt);
            return;
        }

        // === Sort and truncate to top-N ===
        candidates
            .sort_by(|a, b| cmp_topn_candidate(a, b, state.topn_ascending, state.topn_nulls_first));
        if state.topn_multi_col_sort {
            // Multi-column ORDER BY: keep all candidates whose time key could appear
            // in the final top-N. Find the threshold at position effective_limit-1,
            // then retain all candidates with sort_key <= that (ASC) or >= that (DESC),
            // since ties on the time column need secondary sort by PG's Sort node.
            let threshold_idx = std::cmp::min(effective_limit - 1, candidates.len() - 1);
            let threshold_key = candidates[threshold_idx].sort_key;
            let threshold_is_null = candidates[threshold_idx].sort_is_null;
            candidates.retain(|c| {
                cmp_topn_key(
                    c.sort_key,
                    c.sort_is_null,
                    threshold_key,
                    threshold_is_null,
                    state.topn_ascending,
                    state.topn_nulls_first,
                ) != std::cmp::Ordering::Greater
            });
        } else {
            candidates.truncate(effective_limit);
        }

        // === Pass 2: Phase 2 only for segments with top-N rows ===
        let mut segment_topn_rows: HashMap<usize, Vec<usize>> = HashMap::new();
        for c in &candidates {
            segment_topn_rows
                .entry(c.segment_idx)
                .or_default()
                .push(c.row_idx);
        }

        state.timing.topn_phase2_segments = segment_topn_rows.len() as u64;

        // Build set of Phase 1 col indices for fast lookup in Phase 2
        let phase1_col_set: std::collections::HashSet<usize> =
            phase1_col_indices.iter().copied().collect();

        struct RowData {
            sort_key: i64,
            sort_is_null: bool,
            datums: Vec<(pg_sys::Datum, bool)>,
        }
        let mut result_rows: Vec<RowData> = Vec::with_capacity(effective_limit);

        // Detoast lazy TOAST pointers for winning segments only.
        // Non-winning segments' pointers are never detoasted (saving I/O).
        let t_lazy = if instrument {
            Some(Instant::now())
        } else {
            None
        };
        for &seg_idx in segment_topn_rows.keys() {
            let dl = detoast_lazy_blobs(&mut state.segments_data[seg_idx]);
            state.timing.fold_detoast_stats(dl);
        }
        if let Some(t) = t_lazy {
            state.timing.heap_scan_us += t.elapsed().as_micros() as u64;
        }

        // Reset segment_mcxt for Phase 2. phase1_persist_mcxt is already under
        // segment_mcxt's parent, so it survives this reset.
        pg_sys::MemoryContextReset(state.segment_mcxt);

        for (&seg_idx, row_indices) in &segment_topn_rows {
            let seg = &state.segments_data[seg_idx];
            let old_ctx = pg_sys::MemoryContextSwitchTo(state.segment_mcxt);

            let max_row = *row_indices.iter().max().unwrap_or(&0);

            let mut narrowed_selection = vec![false; seg.row_count as usize];
            for &ri in row_indices {
                narrowed_selection[ri] = true;
            }

            let t_phase2 = if instrument {
                Some(Instant::now())
            } else {
                None
            };

            let mut decompressed: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
            let mut blob_idx: usize = 0;
            let mut seg_val_idx: usize = 0;

            for (col_idx, col_name) in state.col_names.iter().enumerate() {
                let type_oid = state.col_types[col_idx];

                if !state.needed_cols[col_idx] {
                    if state.segment_by.contains(col_name) {
                        seg_val_idx += 1;
                    } else {
                        blob_idx += 1;
                    }
                    decompressed.push(Vec::new());
                    continue;
                }

                if state.segment_by.contains(col_name) {
                    let val = &seg.segment_values[seg_val_idx];
                    let (datum, is_null) = match val {
                        Some(s) => (string_to_datum(s, type_oid), false),
                        None => (pg_sys::Datum::from(0), true),
                    };
                    let repeated: Vec<(pg_sys::Datum, bool)> =
                        (0..seg.row_count).map(|_| (datum, is_null)).collect();
                    decompressed.push(repeated);
                    seg_val_idx += 1;
                } else {
                    // Skip columns already decompressed in Phase 1 —
                    // their datums are stored in candidate.phase1_datums
                    if phase1_col_set.contains(&col_idx) {
                        decompressed.push(Vec::new());
                        blob_idx += 1;
                        continue;
                    }

                    let blob = &seg.compressed_blobs[blob_idx];
                    let typmod = state.col_typmods[col_idx];

                    let t_col = if instrument {
                        Some(Instant::now())
                    } else {
                        None
                    };
                    let is_text = is_text_type(type_oid);
                    let datums = if is_text {
                        decompress_text_blob_with_selection(
                            blob,
                            type_oid,
                            typmod,
                            &narrowed_selection,
                        )
                    } else {
                        let type_name = pg_type_name(type_oid);
                        decompress_blob_to_datums_truncated(
                            blob, &type_name, type_oid, typmod, max_row,
                        )
                    };
                    if let Some(t) = t_col {
                        let col_us = t.elapsed().as_micros() as u64;
                        if is_text {
                            state.timing.phase2_text_us += col_us;
                            state.timing.phase2_text_cols += 1;
                        } else {
                            state.timing.phase2_nontext_us += col_us;
                            state.timing.phase2_nontext_cols += 1;
                        }
                    }
                    decompressed.push(datums);
                    blob_idx += 1;
                }
            }

            if let Some(t) = t_phase2 {
                let elapsed = t.elapsed().as_micros() as u64;
                state.timing.decompress_us += elapsed;
                state.timing.phase2_us += elapsed;
            }

            pg_sys::MemoryContextSwitchTo(old_ctx);

            // Extract top-N rows from this segment, merging Phase 1 + Phase 2 datums
            for &ri in row_indices {
                let candidate = candidates
                    .iter()
                    .find(|c| c.segment_idx == seg_idx && c.row_idx == ri)
                    .unwrap();

                let mut row_datums = vec![(pg_sys::Datum::from(0), true); state.col_names.len()];

                // Fill from Phase 2 decompressed data
                for &col_idx in &state.needed_col_indices {
                    if !decompressed[col_idx].is_empty() && ri < decompressed[col_idx].len() {
                        row_datums[col_idx] = decompressed[col_idx][ri];
                    }
                }

                // Overlay Phase 1 datums (filter + sort columns)
                for &(ci, d, isnull) in &candidate.phase1_datums {
                    row_datums[ci] = (d, isnull);
                }

                result_rows.push(RowData {
                    sort_key: candidate.sort_key,
                    sort_is_null: candidate.sort_is_null,
                    datums: row_datums,
                });
            }
        }

        // Sort result_rows by sort key (skip for multi-column: PG's Sort handles it)
        if !state.topn_multi_col_sort {
            result_rows.sort_by(|a, b| {
                cmp_topn_key(
                    a.sort_key,
                    a.sort_is_null,
                    b.sort_key,
                    b.sort_is_null,
                    state.topn_ascending,
                    state.topn_nulls_first,
                )
            });
        }

        // Store in topn_buffer
        state.topn_buffer = result_rows.into_iter().map(|r| r.datums).collect();
        state.topn_cursor = 0;

        // Clean up Phase 1 persistent context (datums are now in topn_buffer
        // which holds raw Datum values — text datums point into this context)
        // DON'T delete it yet — text datums in topn_buffer reference it.
        // It will be cleaned up when segment_mcxt is reset/deleted.
        pg_sys::MemoryContextSetParent(phase1_persist_mcxt, state.segment_mcxt);

        pgrx::log!(
            "pg_deltax topn: candidates={} top_n={} phase2_segments={}",
            state.timing.topn_candidates,
            state.topn_buffer.len(),
            state.timing.topn_phase2_segments,
        );
    }
}

/// Configuration for parallel top-N text scan (read-only, shared across threads).
struct ParallelTopNTextConfig<'a> {
    segments_data: &'a [SegmentData],
    col_names: &'a [String],
    col_types: &'a [pg_sys::Oid],
    segment_by: &'a [String],
    batch_quals: &'a [BatchQual],
    text_qual_infos: Vec<TextQualInfo>,
    sort_col: usize,
    sort_blob_idx: usize,
    ascending: bool,
    nulls_first: bool,
    prune_limit: usize,
    /// Blob indices for columns that have batch quals (non-text, for evaluate_batch_quals)
    phase1_blob_col_map: Vec<(usize, usize)>, // (col_idx, blob_idx) for non-segby qual columns
}

/// Result of parallel top-N text worker.
struct ParallelTopNTextResult {
    /// (global_seg_idx, row_idx, sort_string), with None representing NULL.
    candidates: Vec<(usize, usize, Option<String>)>,
    segments_decompressed: u64,
    decompress_us: u64,
    batch_eval_us: u64,
}

/// Worker function for parallel top-N text scan.
/// Processes a chunk of segment indices, returning candidate rows with sort strings.
fn process_topn_text_chunk(
    seg_indices: &[usize],
    config: &ParallelTopNTextConfig,
) -> ParallelTopNTextResult {
    let mut candidates: Vec<(usize, usize, Option<String>)> = Vec::new();
    let mut threshold: Option<Option<String>> = None;
    let mut segments_decompressed: u64 = 0;
    let mut decompress_us: u64 = 0;
    let mut batch_eval_us: u64 = 0;

    for &seg_idx in seg_indices {
        let seg = &config.segments_data[seg_idx];

        let t_decompress = Instant::now();

        // Decompress sort column
        let sort_blob = &seg.compressed_blobs[config.sort_blob_idx];
        let sort_seg_col = match decompress_text_to_seg_col(sort_blob) {
            Some(c) => c,
            None => continue,
        };
        let row_count = seg.row_count as usize;

        // Decompress filter columns and build selection
        let mut selection: Vec<bool> = Vec::new();

        // Apply text quals via SegTextColumn
        for tqi in &config.text_qual_infos {
            match tqi {
                TextQualInfo::EqNe {
                    col_idx,
                    const_str,
                    is_ne,
                } => {
                    // Check if this is a segment_by column
                    let col_name = &config.col_names[*col_idx];
                    if config.segment_by.contains(col_name) {
                        let seg_val_idx = config
                            .segment_by
                            .iter()
                            .position(|sb| sb == col_name)
                            .unwrap();
                        let passes = match &seg.segment_values[seg_val_idx] {
                            Some(s) => {
                                if *is_ne {
                                    s != const_str
                                } else {
                                    s == const_str
                                }
                            }
                            None => false,
                        };
                        if !passes {
                            selection = vec![false; row_count];
                            break;
                        }
                        continue;
                    }
                    // Non-segby text column — decompress and filter
                    let blob_idx = col_to_blob_idx(config.col_names, config.segment_by, *col_idx);
                    let blob = &seg.compressed_blobs[blob_idx];
                    let seg_col = if *col_idx == config.sort_col {
                        // Reuse already-decompressed sort column
                        &sort_seg_col
                    } else {
                        // Need to decompress this column — use a temporary
                        // Since we can't store the temp and reference it, decompress inline
                        if let Some(ref sc) = decompress_text_to_seg_col(blob) {
                            apply_text_eq_filter(sc, const_str, *is_ne, row_count, &mut selection);
                        } else if selection.is_empty() {
                            selection = vec![false; row_count];
                        } else {
                            selection.iter_mut().for_each(|s| *s = false);
                        }
                        continue;
                    };
                    apply_text_eq_filter(seg_col, const_str, *is_ne, row_count, &mut selection);
                }
                TextQualInfo::Like {
                    col_idx,
                    strategy,
                    negate,
                } => {
                    let col_name = &config.col_names[*col_idx];
                    if config.segment_by.contains(col_name) {
                        continue; // segment_by LIKE is handled by dict pruning
                    }
                    let seg_col = if *col_idx == config.sort_col {
                        &sort_seg_col
                    } else {
                        let blob_idx =
                            col_to_blob_idx(config.col_names, config.segment_by, *col_idx);
                        let blob = &seg.compressed_blobs[blob_idx];
                        if let Some(ref sc) = decompress_text_to_seg_col(blob) {
                            apply_text_like_filter(
                                sc,
                                strategy,
                                *negate,
                                row_count,
                                &mut selection,
                            );
                        } else if selection.is_empty() {
                            selection = vec![false; row_count];
                        } else {
                            selection.iter_mut().for_each(|s| *s = false);
                        }
                        continue;
                    };
                    apply_text_like_filter(seg_col, strategy, *negate, row_count, &mut selection);
                }
                TextQualInfo::InList { col_idx, values } => {
                    let col_name = &config.col_names[*col_idx];
                    if config.segment_by.contains(col_name) {
                        let seg_val_idx = config
                            .segment_by
                            .iter()
                            .position(|sb| sb == col_name)
                            .unwrap();
                        let passes = match &seg.segment_values[seg_val_idx] {
                            Some(s) => values.iter().any(|v| v == s),
                            None => false,
                        };
                        if !passes {
                            selection = vec![false; row_count];
                            break;
                        }
                        continue;
                    }
                    let seg_col = if *col_idx == config.sort_col {
                        &sort_seg_col
                    } else {
                        let blob_idx =
                            col_to_blob_idx(config.col_names, config.segment_by, *col_idx);
                        let blob = &seg.compressed_blobs[blob_idx];
                        if let Some(ref sc) = decompress_text_to_seg_col(blob) {
                            apply_text_in_filter(sc, values, row_count, &mut selection);
                        } else if selection.is_empty() {
                            selection = vec![false; row_count];
                        } else {
                            selection.iter_mut().for_each(|s| *s = false);
                        }
                        continue;
                    };
                    apply_text_in_filter(seg_col, values, row_count, &mut selection);
                }
            }
        }

        // Apply numeric batch quals
        let has_numeric_quals = config.batch_quals.iter().any(|bq| {
            !matches!(
                bq.type_oid,
                pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID
            ) && !config.segment_by.contains(&config.col_names[bq.col_idx])
        });
        if has_numeric_quals {
            let t_batch = Instant::now();
            // Build a sparse decompressed array for evaluate_batch_quals
            let mut decompressed: Vec<Vec<(pg_sys::Datum, bool)>> =
                (0..config.col_names.len()).map(|_| Vec::new()).collect();
            for &(col_idx, blob_idx) in &config.phase1_blob_col_map {
                let blob = &seg.compressed_blobs[blob_idx];
                let type_name = super::datum_utils::pg_type_name(config.col_types[col_idx]);
                // Safety: numeric decompression is thread-safe (no PG API calls)
                let datums = unsafe {
                    super::datum_utils::decompress_blob_to_datums(
                        blob,
                        &type_name,
                        config.col_types[col_idx],
                        -1,
                    )
                };
                decompressed[col_idx] = datums;
            }
            let sv = evaluate_batch_quals(&decompressed, row_count, config.batch_quals, selection);
            selection = sv;
            batch_eval_us += t_batch.elapsed().as_micros() as u64;
        }

        let elapsed = t_decompress.elapsed().as_micros() as u64;
        decompress_us += elapsed;
        segments_decompressed += 1;

        // Check if any rows pass
        let any_selected = selection.is_empty() || selection.iter().any(|&s| s);
        if !any_selected {
            continue;
        }

        // Collect qualifying rows
        for row_idx in 0..row_count {
            let passes = selection.is_empty() || selection[row_idx];
            if !passes {
                continue;
            }
            let sort_key = sort_seg_col.get_str(row_idx);

            // Byte-order threshold pruning
            if let Some(ref thr) = threshold
                && cmp_nullable_str_byte(
                    sort_key,
                    thr.as_deref(),
                    config.ascending,
                    config.nulls_first,
                ) == std::cmp::Ordering::Greater
            {
                continue;
            }

            candidates.push((seg_idx, row_idx, sort_key.map(str::to_string)));
        }

        // Update byte-order threshold
        if candidates.len() >= config.prune_limit {
            let k = config.prune_limit - 1;
            candidates.select_nth_unstable_by(k, |a, b| {
                cmp_nullable_str_byte(
                    a.2.as_deref(),
                    b.2.as_deref(),
                    config.ascending,
                    config.nulls_first,
                )
            });
            candidates.truncate(config.prune_limit);
            threshold = Some(candidates[k].2.clone());
        }
    }

    ParallelTopNTextResult {
        candidates,
        segments_decompressed,
        decompress_us,
        batch_eval_us,
    }
}

/// Compute blob index for a non-segment-by column.
fn col_to_blob_idx(col_names: &[String], segment_by: &[String], target_col: usize) -> usize {
    let mut bi = 0;
    for (ci, cn) in col_names.iter().enumerate() {
        if segment_by.contains(cn) {
            continue;
        }
        if ci == target_col {
            return bi;
        }
        bi += 1;
    }
    panic!("col_to_blob_idx: column {} not found in blobs", target_col);
}

/// Execute Top-N for text sort columns (e.g. ORDER BY SearchPhrase LIMIT 10).
///
/// When parallel workers > 1 and enough surviving segments, uses parallel
/// decompress + byte-order pruning in threads, then strcoll sort on merge.
/// Falls back to sequential path otherwise.
unsafe fn exec_topn_text(
    _node: *mut pg_sys::CustomScanState,
    state: &mut DecompressState,
    instrument: bool,
    plan_qual: *mut pg_sys::List,
) {
    unsafe {
        let sort_col = match state.topn_sort_col {
            Some(c) => c,
            None => return,
        };
        let effective_limit = state.topn_limit;

        // Safety check: all plan quals must be batch-handled
        if !plan_qual.is_null() {
            let num_plan_quals = (*plan_qual).length as usize;
            let num_batch_quals = state.batch_quals.len();
            if num_plan_quals > num_batch_quals {
                pgrx::log!(
                    "pg_deltax topn_text: disabled (plan_quals={} > batch_quals={})",
                    num_plan_quals,
                    num_batch_quals,
                );
                for seg in state.segments_data.iter_mut() {
                    let dl = detoast_lazy_blobs(seg);
                    state.timing.fold_detoast_stats(dl);
                }
                state.topn_limit = 0;
                state.segment_index = 0;
                return;
            }
        }

        pgrx::log!(
            "pg_deltax topn_text: limit={} ascending={} sort_col={} segments={}",
            effective_limit,
            state.topn_ascending,
            sort_col,
            state.segments_data.len(),
        );

        // Identify Phase 1 columns (filter + sort) and their blob indices.
        let phase1_col_indices = compute_phase1_col_indices(
            &state.col_names,
            &state.segment_by,
            &state.needed_cols,
            &state.batch_quals,
            sort_col,
        );
        let phase1_blob_indices =
            compute_phase1_blob_indices(&state.col_names, &state.segment_by, &phase1_col_indices);

        // Compute sort column's blob index
        let sort_blob_idx = col_to_blob_idx(&state.col_names, &state.segment_by, sort_col);

        // ===== Phase 0: Main thread — detoast + segment pruning =====
        let mut surviving_seg_indices: Vec<usize> = Vec::new();
        let num_segments = state.segments_data.len();

        for seg_idx in 0..num_segments {
            let seg = &state.segments_data[seg_idx];
            if seg.row_count == 0 {
                continue;
            }
            if segment_pre_pruned_by_metadata(
                seg,
                &state.segment_by_filters,
                state.time_min,
                state.time_max,
            ) {
                state.timing.segments_skipped += 1;
                continue;
            }

            // Detoast Phase 1 blobs (PG API — must be main thread)
            let t_detoast = if instrument {
                Some(Instant::now())
            } else {
                None
            };
            let dl = detoast_lazy_blobs_selective(
                &mut state.segments_data[seg_idx],
                &phase1_blob_indices,
            );
            state.timing.fold_detoast_stats(dl);
            if let Some(t) = t_detoast {
                state.timing.heap_scan_us += t.elapsed().as_micros() as u64;
            }

            // Dictionary-based pruning (reads compressed blobs, no PG calls)
            if segment_skippable_by_dict(
                &state.batch_quals,
                &state.col_names,
                &state.segment_by,
                &state.segments_data[seg_idx].compressed_blobs,
            ) {
                state.timing.segments_skipped += 1;
                continue;
            }

            surviving_seg_indices.push(seg_idx);
        }

        let n_workers = crate::get_parallel_workers();
        let use_parallel = n_workers > 1 && surviving_seg_indices.len() >= 2;

        if use_parallel {
            // ===== Parallel path =====
            let prune_limit = std::cmp::max(effective_limit * 100, 10000);

            // Build text qual infos for worker threads
            let mut text_qual_infos: Vec<TextQualInfo> = Vec::new();
            for bq in &state.batch_quals {
                let t = bq.type_oid;
                if t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID {
                    match bq.op {
                        BatchCompareOp::Eq | BatchCompareOp::Ne => {
                            if let Some(ref cs) = bq.text_const {
                                text_qual_infos.push(TextQualInfo::EqNe {
                                    col_idx: bq.col_idx,
                                    const_str: cs.clone(),
                                    is_ne: bq.op == BatchCompareOp::Ne,
                                });
                            }
                        }
                        BatchCompareOp::Like | BatchCompareOp::NotLike => {
                            if let Some(ref strat) = bq.like_strategy {
                                text_qual_infos.push(TextQualInfo::Like {
                                    col_idx: bq.col_idx,
                                    strategy: strat.clone(),
                                    negate: bq.op == BatchCompareOp::NotLike,
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }

            // Build numeric qual blob map for workers
            let mut phase1_blob_col_map: Vec<(usize, usize)> = Vec::new();
            for bq in &state.batch_quals {
                let t = bq.type_oid;
                if matches!(t, pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID) {
                    continue;
                }
                let col_name = &state.col_names[bq.col_idx];
                if state.segment_by.contains(col_name) {
                    continue;
                }
                let blob_idx = col_to_blob_idx(&state.col_names, &state.segment_by, bq.col_idx);
                if !phase1_blob_col_map.iter().any(|&(ci, _)| ci == bq.col_idx) {
                    phase1_blob_col_map.push((bq.col_idx, blob_idx));
                }
            }

            let config = ParallelTopNTextConfig {
                segments_data: &state.segments_data,
                col_names: &state.col_names,
                col_types: &state.col_types,
                segment_by: &state.segment_by,
                batch_quals: &state.batch_quals,
                text_qual_infos,
                sort_col,
                sort_blob_idx,
                ascending: state.topn_ascending,
                nulls_first: state.topn_nulls_first,
                prune_limit,
                phase1_blob_col_map,
            };

            let chunk_size = surviving_seg_indices.len().div_ceil(n_workers);
            let results: Vec<ParallelTopNTextResult> = std::thread::scope(|s| {
                surviving_seg_indices
                    .chunks(chunk_size)
                    .map(|chunk| {
                        let cfg = &config;
                        s.spawn(move || process_topn_text_chunk(chunk, cfg))
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|h| h.join().unwrap())
                    .collect()
            });

            // Accumulate timing
            for result in &results {
                state.timing.segments_decompressed += result.segments_decompressed;
                state.timing.decompress_us += result.decompress_us;
                state.timing.phase1_us += result.decompress_us;
                state.timing.batch_eval_us += result.batch_eval_us;
            }

            // Merge all candidates
            let mut all_candidates: Vec<(usize, usize, Option<String>)> = Vec::new();
            for result in results {
                all_candidates.extend(result.candidates);
            }

            state.timing.topn_candidates = all_candidates.len() as u64;

            if all_candidates.is_empty() || all_candidates.len() <= effective_limit {
                for seg in state.segments_data.iter_mut() {
                    let dl = detoast_lazy_blobs(seg);
                    state.timing.fold_detoast_stats(dl);
                }
                state.topn_limit = 0;
                state.segment_index = 0;
                return;
            }

            // Byte-order pre-prune if too many candidates
            if all_candidates.len() > prune_limit {
                all_candidates.select_nth_unstable_by(prune_limit - 1, |a, b| {
                    cmp_nullable_str_byte(
                        a.2.as_deref(),
                        b.2.as_deref(),
                        state.topn_ascending,
                        state.topn_nulls_first,
                    )
                });
                all_candidates.truncate(prune_limit);
            }

            // Collation-aware final sort using strcoll_cmp
            all_candidates.sort_by(|a, b| {
                cmp_nullable_str_collation(
                    a.2.as_deref(),
                    b.2.as_deref(),
                    state.topn_ascending,
                    state.topn_nulls_first,
                )
            });

            // Truncate to effective_limit (handle multi_col_sort ties)
            if state.topn_multi_col_sort {
                let threshold_idx = std::cmp::min(effective_limit - 1, all_candidates.len() - 1);
                let threshold_str = all_candidates[threshold_idx].2.clone();
                all_candidates.retain(|c| {
                    cmp_nullable_str_collation(
                        c.2.as_deref(),
                        threshold_str.as_deref(),
                        state.topn_ascending,
                        state.topn_nulls_first,
                    ) != std::cmp::Ordering::Greater
                });
            } else {
                all_candidates.truncate(effective_limit);
            }

            // Phase 2: decompress ALL needed columns for winning segments
            let mut segment_topn_rows: HashMap<usize, Vec<usize>> = HashMap::new();
            for &(seg_idx, row_idx, _) in &all_candidates {
                segment_topn_rows.entry(seg_idx).or_default().push(row_idx);
            }

            state.timing.topn_phase2_segments = segment_topn_rows.len() as u64;

            // Detoast lazy blobs for winning segments
            let t_lazy = if instrument {
                Some(Instant::now())
            } else {
                None
            };
            for &seg_idx in segment_topn_rows.keys() {
                let dl = detoast_lazy_blobs(&mut state.segments_data[seg_idx]);
                state.timing.fold_detoast_stats(dl);
            }
            if let Some(t) = t_lazy {
                state.timing.heap_scan_us += t.elapsed().as_micros() as u64;
            }

            pg_sys::MemoryContextReset(state.segment_mcxt);

            // Build a lookup: (seg_idx, row_idx) -> sort_string for final ordering
            let candidate_sort_strings: HashMap<(usize, usize), Option<String>> = all_candidates
                .iter()
                .map(|(si, ri, s)| ((*si, *ri), s.clone()))
                .collect();

            struct RowData {
                sort_string: Option<String>,
                datums: Vec<(pg_sys::Datum, bool)>,
            }
            let mut result_rows: Vec<RowData> = Vec::with_capacity(effective_limit);

            for (&seg_idx, row_indices) in &segment_topn_rows {
                let seg = &state.segments_data[seg_idx];
                let old_ctx = pg_sys::MemoryContextSwitchTo(state.segment_mcxt);

                let max_row = *row_indices.iter().max().unwrap_or(&0);

                let mut narrowed_selection = vec![false; seg.row_count as usize];
                for &ri in row_indices {
                    narrowed_selection[ri] = true;
                }

                let t_phase2 = if instrument {
                    Some(Instant::now())
                } else {
                    None
                };

                let mut decompressed: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
                let mut blob_idx: usize = 0;
                let mut seg_val_idx: usize = 0;

                for (col_idx, col_name) in state.col_names.iter().enumerate() {
                    let type_oid = state.col_types[col_idx];

                    if !state.needed_cols[col_idx] {
                        if state.segment_by.contains(col_name) {
                            seg_val_idx += 1;
                        } else {
                            blob_idx += 1;
                        }
                        decompressed.push(Vec::new());
                        continue;
                    }

                    if state.segment_by.contains(col_name) {
                        let val = &seg.segment_values[seg_val_idx];
                        let (datum, is_null) = match val {
                            Some(s) => (string_to_datum(s, type_oid), false),
                            None => (pg_sys::Datum::from(0), true),
                        };
                        let repeated: Vec<(pg_sys::Datum, bool)> =
                            (0..seg.row_count).map(|_| (datum, is_null)).collect();
                        decompressed.push(repeated);
                        seg_val_idx += 1;
                    } else {
                        let blob = &seg.compressed_blobs[blob_idx];
                        let typmod = state.col_typmods[col_idx];
                        let is_text = is_text_type(type_oid);
                        let datums = if is_text {
                            decompress_text_blob_with_selection(
                                blob,
                                type_oid,
                                typmod,
                                &narrowed_selection,
                            )
                        } else {
                            let type_name = pg_type_name(type_oid);
                            decompress_blob_to_datums_truncated(
                                blob, &type_name, type_oid, typmod, max_row,
                            )
                        };
                        decompressed.push(datums);
                        blob_idx += 1;
                    }
                }

                if let Some(t) = t_phase2 {
                    let elapsed = t.elapsed().as_micros() as u64;
                    state.timing.decompress_us += elapsed;
                    state.timing.phase2_us += elapsed;
                }

                pg_sys::MemoryContextSwitchTo(old_ctx);

                for &ri in row_indices {
                    let mut row_datums =
                        vec![(pg_sys::Datum::from(0), true); state.col_names.len()];
                    for &col_idx in &state.needed_col_indices {
                        if !decompressed[col_idx].is_empty() && ri < decompressed[col_idx].len() {
                            row_datums[col_idx] = decompressed[col_idx][ri];
                        }
                    }
                    let sort_string = candidate_sort_strings
                        .get(&(seg_idx, ri))
                        .cloned()
                        .unwrap_or(None);
                    result_rows.push(RowData {
                        sort_string,
                        datums: row_datums,
                    });
                }
            }

            // Sort result rows (collation-aware)
            if !state.topn_multi_col_sort {
                result_rows.sort_by(|a, b| {
                    cmp_nullable_str_collation(
                        a.sort_string.as_deref(),
                        b.sort_string.as_deref(),
                        state.topn_ascending,
                        state.topn_nulls_first,
                    )
                });
            }

            state.topn_buffer = result_rows.into_iter().map(|r| r.datums).collect();
            state.topn_cursor = 0;

            pgrx::log!(
                "pg_deltax topn_text: parallel candidates={} top_n={} phase2_segments={} workers={}",
                state.timing.topn_candidates,
                state.topn_buffer.len(),
                state.timing.topn_phase2_segments,
                n_workers,
            );
        } else {
            // ===== Sequential fallback =====
            exec_topn_text_sequential(
                state,
                instrument,
                &phase1_col_indices,
                &surviving_seg_indices,
                sort_col,
                effective_limit,
            );
        }
    }
}

/// Sequential Top-N text implementation (fallback when parallel not beneficial).
unsafe fn exec_topn_text_sequential(
    state: &mut DecompressState,
    instrument: bool,
    phase1_col_indices: &[usize],
    surviving_seg_indices: &[usize],
    sort_col: usize,
    effective_limit: usize,
) {
    unsafe {
        // Persistent memory context for Phase 1 text datums
        let phase1_persist_mcxt = pg_sys::AllocSetContextCreateInternal(
            (*state.segment_mcxt).parent,
            c"TopN Text Persist".as_ptr(),
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        );

        let mut candidates: Vec<TextTopNCandidate> = Vec::new();
        let mut threshold_datum: Option<(pg_sys::Datum, bool)> = None;

        for &seg_idx in surviving_seg_indices {
            let seg = &state.segments_data[seg_idx];

            let t_decompress = if instrument {
                Some(Instant::now())
            } else {
                None
            };

            // Reset segment memory context for Phase 1
            pg_sys::MemoryContextReset(state.segment_mcxt);
            let old_ctx = pg_sys::MemoryContextSwitchTo(state.segment_mcxt);

            // Phase 1: Decompress filter + sort columns
            let mut decompressed: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
            let mut blob_idx: usize = 0;
            let mut seg_val_idx: usize = 0;
            let mut pre_selection: Vec<bool> = Vec::new();

            for (col_idx, col_name) in state.col_names.iter().enumerate() {
                let type_oid = state.col_types[col_idx];

                if !state.needed_cols[col_idx] && col_idx != sort_col {
                    if state.segment_by.contains(col_name) {
                        seg_val_idx += 1;
                    } else {
                        blob_idx += 1;
                    }
                    decompressed.push(Vec::new());
                    continue;
                }

                if state.segment_by.contains(col_name) {
                    let val = &seg.segment_values[seg_val_idx];
                    let (datum, is_null) = match val {
                        Some(s) => (string_to_datum(s, type_oid), false),
                        None => (pg_sys::Datum::from(0), true),
                    };
                    let repeat_count = seg.row_count as usize;
                    let repeated: Vec<(pg_sys::Datum, bool)> =
                        (0..repeat_count).map(|_| (datum, is_null)).collect();
                    decompressed.push(repeated);

                    // Evaluate text Eq/Ne batch quals on segment_by columns
                    if let Some(bq) = state.batch_quals.iter().find(|bq| {
                        bq.col_idx == col_idx
                            && bq.text_const.is_some()
                            && matches!(bq.op, BatchCompareOp::Eq | BatchCompareOp::Ne)
                    }) {
                        let const_str = bq.text_const.as_ref().unwrap();
                        let is_ne = bq.op == BatchCompareOp::Ne;
                        let matches = match val {
                            Some(s) => {
                                if is_ne {
                                    s != const_str
                                } else {
                                    s == const_str
                                }
                            }
                            None => false,
                        };
                        if !matches {
                            merge_and_selection(&mut pre_selection, vec![false; repeat_count]);
                        }
                    }

                    seg_val_idx += 1;
                } else {
                    let blob = &seg.compressed_blobs[blob_idx];
                    let typmod = state.col_typmods[col_idx];

                    let like_qual = state.batch_quals.iter().find(|bq| {
                        bq.col_idx == col_idx
                            && matches!(bq.op, BatchCompareOp::Like | BatchCompareOp::NotLike)
                    });
                    let text_eq_qual = state.batch_quals.iter().find(|bq| {
                        bq.col_idx == col_idx
                            && bq.text_const.is_some()
                            && matches!(bq.op, BatchCompareOp::Eq | BatchCompareOp::Ne)
                    });
                    let text_in_qual = state.batch_quals.iter().find(|bq| {
                        bq.col_idx == col_idx
                            && bq.in_list_text.is_some()
                            && bq.op == BatchCompareOp::InList
                    });
                    let has_any_batch_qual =
                        state.batch_quals.iter().any(|bq| bq.col_idx == col_idx);

                    if let Some(bq) = like_qual {
                        let strat = bq.like_strategy.as_ref().unwrap();
                        let neg = bq.op == BatchCompareOp::NotLike;
                        let (datums, sel) = decompress_text_blob_with_like_filter(
                            blob, type_oid, typmod, strat, neg, None,
                        );
                        decompressed.push(datums);
                        merge_and_selection(&mut pre_selection, sel);
                    } else if let Some(bq) = text_eq_qual {
                        let const_str = bq.text_const.as_ref().unwrap();
                        let is_ne = bq.op == BatchCompareOp::Ne;
                        let (datums, sel) = decompress_text_blob_with_eq_filter(
                            blob, type_oid, typmod, const_str, is_ne, None,
                        );
                        decompressed.push(datums);
                        merge_and_selection(&mut pre_selection, sel);
                    } else if let Some(bq) = text_in_qual {
                        let strs = bq.in_list_text.as_ref().unwrap();
                        let (datums, sel) = decompress_text_blob_with_in_filter(
                            blob, type_oid, typmod, strs, /* is_not_in */ false, None,
                        );
                        decompressed.push(datums);
                        merge_and_selection(&mut pre_selection, sel);
                    } else if has_any_batch_qual || col_idx == sort_col {
                        let type_name = pg_type_name(type_oid);
                        let datums = decompress_blob_to_datums(blob, &type_name, type_oid, typmod);
                        decompressed.push(datums);
                    } else {
                        decompressed.push(Vec::new());
                    }
                    blob_idx += 1;
                }
            }

            pg_sys::MemoryContextSwitchTo(old_ctx);

            if let Some(t) = t_decompress {
                let elapsed = t.elapsed().as_micros() as u64;
                state.timing.decompress_us += elapsed;
                state.timing.phase1_us += elapsed;
            }
            state.timing.segments_decompressed += 1;

            let effective_row_count = seg.row_count as usize;

            // Evaluate batch quals
            let selection_vector = if !state.batch_quals.is_empty() || !pre_selection.is_empty() {
                let t_batch = if instrument {
                    Some(Instant::now())
                } else {
                    None
                };
                let sv = evaluate_batch_quals(
                    &decompressed,
                    effective_row_count,
                    &state.batch_quals,
                    pre_selection,
                );
                if let Some(t) = t_batch {
                    state.timing.batch_eval_us += t.elapsed().as_micros() as u64;
                }
                sv
            } else {
                Vec::new()
            };

            let any_selected = selection_vector.is_empty() || selection_vector.iter().any(|&s| s);
            if !any_selected {
                continue;
            }

            // Collect candidates from passing rows
            let sort_datums = &decompressed[sort_col];
            if sort_datums.is_empty() {
                continue;
            }

            for row_idx in 0..effective_row_count {
                let passes = selection_vector.is_empty() || selection_vector[row_idx];
                if !passes {
                    continue;
                }
                let (datum, is_null) = sort_datums[row_idx];

                // Skip if worse than threshold (collation-aware comparison)
                if let Some((threshold, threshold_is_null)) = threshold_datum
                    && cmp_text_key(
                        datum,
                        is_null,
                        threshold,
                        threshold_is_null,
                        state.topn_ascending,
                        state.topn_nulls_first,
                    ) == std::cmp::Ordering::Greater
                {
                    continue;
                }

                // Store Phase 1 datums for this candidate.
                // Copy text datums to persistent context so they survive segment resets.
                let mut p1_datums: Vec<(usize, pg_sys::Datum, bool)> =
                    Vec::with_capacity(phase1_col_indices.len());
                let mut sort_datum_copy = datum;
                for &ci in phase1_col_indices {
                    if decompressed[ci].is_empty() {
                        continue;
                    }
                    let (d, isnull) = decompressed[ci][row_idx];
                    if isnull {
                        p1_datums.push((ci, pg_sys::Datum::from(0), true));
                    } else if is_text_type(state.col_types[ci]) {
                        let vl_ptr = d.cast_mut_ptr::<pg_sys::varlena>();
                        let size = pgrx::varsize_any(vl_ptr);
                        let old_mc = pg_sys::MemoryContextSwitchTo(phase1_persist_mcxt);
                        let copy_ptr = pg_sys::palloc(size) as *mut u8;
                        std::ptr::copy_nonoverlapping(vl_ptr as *const u8, copy_ptr, size);
                        pg_sys::MemoryContextSwitchTo(old_mc);
                        let copied_datum = pg_sys::Datum::from(copy_ptr as usize);
                        p1_datums.push((ci, copied_datum, false));
                        if ci == sort_col {
                            sort_datum_copy = copied_datum;
                        }
                    } else {
                        p1_datums.push((ci, d, false));
                    }
                }

                candidates.push(TextTopNCandidate {
                    segment_idx: seg_idx,
                    row_idx,
                    sort_datum: sort_datum_copy,
                    sort_is_null: is_null,
                    phase1_datums: p1_datums,
                });
            }

            // Update threshold
            if candidates.len() >= effective_limit {
                let n = effective_limit - 1;
                candidates.select_nth_unstable_by(n, |a, b| {
                    cmp_text_candidate(a, b, state.topn_ascending, state.topn_nulls_first)
                });
                threshold_datum = Some((candidates[n].sort_datum, candidates[n].sort_is_null));
            }
        }

        state.timing.topn_candidates = candidates.len() as u64;

        // If no candidates or all fit in limit, fall back to normal path
        if candidates.is_empty() || candidates.len() <= effective_limit {
            for seg in state.segments_data.iter_mut() {
                let dl = detoast_lazy_blobs(seg);
                state.timing.fold_detoast_stats(dl);
            }
            state.topn_limit = 0;
            state.segment_index = 0;
            pg_sys::MemoryContextDelete(phase1_persist_mcxt);
            return;
        }

        // Sort and truncate to top-N (using collation-aware comparison)
        candidates
            .sort_by(|a, b| cmp_text_candidate(a, b, state.topn_ascending, state.topn_nulls_first));
        if state.topn_multi_col_sort {
            let threshold_idx = std::cmp::min(effective_limit - 1, candidates.len() - 1);
            let threshold_datum_val = candidates[threshold_idx].sort_datum;
            let threshold_is_null = candidates[threshold_idx].sort_is_null;
            candidates.retain(|c| {
                cmp_text_key(
                    c.sort_datum,
                    c.sort_is_null,
                    threshold_datum_val,
                    threshold_is_null,
                    state.topn_ascending,
                    state.topn_nulls_first,
                ) != std::cmp::Ordering::Greater
            });
        } else {
            candidates.truncate(effective_limit);
        }

        // Phase 2: decompress remaining columns for winning segments
        let mut segment_topn_rows: HashMap<usize, Vec<usize>> = HashMap::new();
        for c in &candidates {
            segment_topn_rows
                .entry(c.segment_idx)
                .or_default()
                .push(c.row_idx);
        }

        state.timing.topn_phase2_segments = segment_topn_rows.len() as u64;

        let phase1_col_set: std::collections::HashSet<usize> =
            phase1_col_indices.iter().copied().collect();

        struct RowData {
            sort_datum: pg_sys::Datum,
            sort_is_null: bool,
            datums: Vec<(pg_sys::Datum, bool)>,
        }
        let mut result_rows: Vec<RowData> = Vec::with_capacity(effective_limit);

        // Detoast lazy blobs for winning segments only
        let t_lazy = if instrument {
            Some(Instant::now())
        } else {
            None
        };
        for &seg_idx in segment_topn_rows.keys() {
            let dl = detoast_lazy_blobs(&mut state.segments_data[seg_idx]);
            state.timing.fold_detoast_stats(dl);
        }
        if let Some(t) = t_lazy {
            state.timing.heap_scan_us += t.elapsed().as_micros() as u64;
        }

        pg_sys::MemoryContextReset(state.segment_mcxt);

        for (&seg_idx, row_indices) in &segment_topn_rows {
            let seg = &state.segments_data[seg_idx];
            let old_ctx = pg_sys::MemoryContextSwitchTo(state.segment_mcxt);

            let max_row = *row_indices.iter().max().unwrap_or(&0);

            let mut narrowed_selection = vec![false; seg.row_count as usize];
            for &ri in row_indices {
                narrowed_selection[ri] = true;
            }

            let t_phase2 = if instrument {
                Some(Instant::now())
            } else {
                None
            };

            let mut decompressed: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
            let mut blob_idx: usize = 0;
            let mut seg_val_idx: usize = 0;

            for (col_idx, col_name) in state.col_names.iter().enumerate() {
                let type_oid = state.col_types[col_idx];

                if !state.needed_cols[col_idx] {
                    if state.segment_by.contains(col_name) {
                        seg_val_idx += 1;
                    } else {
                        blob_idx += 1;
                    }
                    decompressed.push(Vec::new());
                    continue;
                }

                if state.segment_by.contains(col_name) {
                    let val = &seg.segment_values[seg_val_idx];
                    let (datum, is_null) = match val {
                        Some(s) => (string_to_datum(s, type_oid), false),
                        None => (pg_sys::Datum::from(0), true),
                    };
                    let repeated: Vec<(pg_sys::Datum, bool)> =
                        (0..seg.row_count).map(|_| (datum, is_null)).collect();
                    decompressed.push(repeated);
                    seg_val_idx += 1;
                } else {
                    if phase1_col_set.contains(&col_idx) {
                        decompressed.push(Vec::new());
                        blob_idx += 1;
                        continue;
                    }

                    let blob = &seg.compressed_blobs[blob_idx];
                    let typmod = state.col_typmods[col_idx];
                    let is_text = is_text_type(type_oid);
                    let datums = if is_text {
                        decompress_text_blob_with_selection(
                            blob,
                            type_oid,
                            typmod,
                            &narrowed_selection,
                        )
                    } else {
                        let type_name = pg_type_name(type_oid);
                        decompress_blob_to_datums_truncated(
                            blob, &type_name, type_oid, typmod, max_row,
                        )
                    };
                    decompressed.push(datums);
                    blob_idx += 1;
                }
            }

            if let Some(t) = t_phase2 {
                let elapsed = t.elapsed().as_micros() as u64;
                state.timing.decompress_us += elapsed;
                state.timing.phase2_us += elapsed;
            }

            pg_sys::MemoryContextSwitchTo(old_ctx);

            // Extract top-N rows, merging Phase 1 + Phase 2 datums
            for &ri in row_indices {
                let candidate = candidates
                    .iter()
                    .find(|c| c.segment_idx == seg_idx && c.row_idx == ri)
                    .unwrap();

                let mut row_datums = vec![(pg_sys::Datum::from(0), true); state.col_names.len()];

                for &col_idx in &state.needed_col_indices {
                    if !decompressed[col_idx].is_empty() && ri < decompressed[col_idx].len() {
                        row_datums[col_idx] = decompressed[col_idx][ri];
                    }
                }

                for &(ci, d, isnull) in &candidate.phase1_datums {
                    row_datums[ci] = (d, isnull);
                }

                result_rows.push(RowData {
                    sort_datum: candidate.sort_datum,
                    sort_is_null: candidate.sort_is_null,
                    datums: row_datums,
                });
            }
        }

        // Sort result rows (collation-aware)
        if !state.topn_multi_col_sort {
            result_rows.sort_by(|a, b| {
                cmp_text_key(
                    a.sort_datum,
                    a.sort_is_null,
                    b.sort_datum,
                    b.sort_is_null,
                    state.topn_ascending,
                    state.topn_nulls_first,
                )
            });
        }

        state.topn_buffer = result_rows.into_iter().map(|r| r.datums).collect();
        state.topn_cursor = 0;

        pg_sys::MemoryContextSetParent(phase1_persist_mcxt, state.segment_mcxt);

        pgrx::log!(
            "pg_deltax topn_text: candidates={} top_n={} phase2_segments={}",
            state.timing.topn_candidates,
            state.topn_buffer.len(),
            state.timing.topn_phase2_segments,
        );
    }
}

/// ExecCustomScan callback: return the next tuple.
///
/// PostgreSQL's ExecCustomScan wrapper does NOT apply qualification or
/// projection — the custom scan provider is responsible for both.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn exec_custom_scan(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    unsafe {
        let scan_slot = (*node).ss.ss_ScanTupleSlot;
        let state = &mut *((*node).custom_ps as *mut DecompressState);
        let econtext = (*node).ss.ps.ps_ExprContext;
        let qual = (*node).ss.ps.qual;
        let proj_info = (*node).ss.ps.ps_ProjInfo;

        let instrument = *state
            .instrument
            .get_or_insert_with(|| !(*node).ss.ps.instrument.is_null());

        // Top-N paths: emit from pre-computed buffer, or trigger the two-pass sort
        if let Some(slot) = exec_topn(node, state, scan_slot, econtext, proj_info, instrument) {
            return slot;
        }

        // Normal row-at-a-time execution
        loop {
            if let Some(slot) =
                try_emit_next_row(state, scan_slot, econtext, qual, proj_info, instrument)
            {
                return slot;
            }
            if !load_next_segment(state, instrument) {
                pg_sys::ExecClearTuple(scan_slot);
                return scan_slot;
            }
        }
    }
}

/// Handle Top-N execution paths for queries with ORDER BY + LIMIT.
///
/// Active for queries like `SELECT * FROM t ORDER BY ts DESC LIMIT 10` where
/// DeltaX can push the sort + limit into the scan. The planner sets
/// `topn_limit > 0` and `topn_sort_col` when it detects this pattern.
///
/// On the first call, triggers `exec_topn_two_pass` which scans all segments
/// to collect candidate rows, sorts them, and stores the top-N results in
/// `state.topn_buffer`. Subsequent calls emit rows one at a time from this
/// pre-computed buffer without touching any more segments.
///
/// The two-pass approach is faster than full decompression because Pass 1 only
/// decompresses filter + sort columns, and Pass 2 only decompresses the
/// remaining columns for the winning rows.
///
/// Returns `Some(slot)` if a Top-N row was emitted or the buffer is exhausted
/// (empty slot = end of scan). Returns `None` to fall through to the normal
/// row-at-a-time path when Top-N is not active or was disabled at runtime
/// (e.g. because a non-batch-comparable qual was detected during execution).
unsafe fn exec_topn(
    node: *mut pg_sys::CustomScanState,
    state: &mut DecompressState,
    scan_slot: *mut pg_sys::TupleTableSlot,
    econtext: *mut pg_sys::ExprContext,
    proj_info: *mut pg_sys::ProjectionInfo,
    instrument: bool,
) -> Option<*mut pg_sys::TupleTableSlot> {
    unsafe {
        // Fast path: emit from pre-computed buffer
        if state.topn_limit > 0 && state.topn_done {
            if state.topn_cursor < state.topn_buffer.len() {
                pg_sys::ExecClearTuple(scan_slot);
                let ncols = state.col_names.len();
                std::ptr::write_bytes((*scan_slot).tts_isnull, true as u8, ncols);
                for &col_idx in &state.needed_col_indices {
                    let (datum, is_null) = state.topn_buffer[state.topn_cursor][col_idx];
                    (*scan_slot).tts_isnull.add(col_idx).write(is_null);
                    (*scan_slot).tts_values.add(col_idx).write(datum);
                }
                pg_sys::ExecStoreVirtualTuple(scan_slot);
                state.topn_cursor += 1;
                state.timing.rows_emitted += 1;

                (*econtext).ecxt_scantuple = scan_slot;
                let result = if !proj_info.is_null() {
                    exec_project(proj_info)
                } else {
                    scan_slot
                };
                return Some(result);
            } else {
                pg_sys::ExecClearTuple(scan_slot);
                return Some(scan_slot);
            }
        }

        // Two-pass execution (first call only): sort all qualifying rows, then
        // re-enter exec_custom_scan to start emitting from the buffer.
        if state.topn_limit > 0 && !state.topn_done && state.topn_sort_col.is_some() {
            let plan_qual_list = (*(*node).ss.ps.plan).qual;
            if state.topn_sort_is_text {
                exec_topn_text(node, state, instrument, plan_qual_list);
            } else {
                exec_topn_two_pass(node, state, instrument, plan_qual_list);
            }
            state.topn_done = true;

            if state.topn_limit == 0 {
                // Top-N was disabled at runtime (e.g. non-batch qual detected)
                return None;
            }
            // Re-enter to emit the first row via the fast path above
            return Some(exec_custom_scan(node));
        }

        None
    }
}

/// Try to emit the next qualifying row from the already-loaded segment.
///
/// This is the inner loop of the normal (non-Top-N) scan path, active for all
/// DeltaXDecompress queries: `SELECT ... FROM t WHERE ...`. It is called
/// repeatedly until the current segment is exhausted, then the caller loads
/// the next segment via `load_next_segment`.
///
/// The function walks the selection vector (a boolean mask produced by batch
/// quals during segment loading) to skip rows that were already filtered out
/// at the batch level. For each surviving row it fills the scan slot with
/// pre-decompressed datums, applies any remaining PostgreSQL WHERE quals that
/// couldn't be pushed down to batch filtering (e.g. cross-column predicates,
/// complex expressions), and runs projection to produce the output tuple.
///
/// Returns `Some(slot)` with the projected result if a qualifying row was
/// found, or `None` when all rows in the current segment are exhausted.
unsafe fn try_emit_next_row(
    state: &mut DecompressState,
    scan_slot: *mut pg_sys::TupleTableSlot,
    econtext: *mut pg_sys::ExprContext,
    qual: *mut pg_sys::ExprState,
    proj_info: *mut pg_sys::ProjectionInfo,
    instrument: bool,
) -> Option<*mut pg_sys::TupleTableSlot> {
    unsafe {
        if state.current_segment.is_empty() {
            return None;
        }

        let seg_rows = state.current_row_count;

        loop {
            // Batch filter: advance row_cursor to the next passing row.
            // Uses slice .position() which LLVM can auto-vectorize (SIMD)
            // to scan 16-32 bytes at a time instead of per-byte branching.
            if !state.selection_vector.is_empty() {
                let start = state.row_cursor;
                let end = seg_rows;
                if let Some(offset) = state.selection_vector[start..end].iter().position(|&v| v) {
                    state.timing.rows_batch_filtered += offset as u64;
                    state.row_cursor = start + offset;
                } else {
                    // All remaining rows fail — skip to end of segment
                    state.timing.rows_batch_filtered += (end - start) as u64;
                    state.row_cursor = end;
                }
            }

            if state.row_cursor >= seg_rows {
                return None;
            }

            let t_row = if instrument {
                Some(Instant::now())
            } else {
                None
            };

            fill_slot(scan_slot, state);
            state.row_cursor += 1;

            // Set the scan tuple in the expression context for qual/projection
            (*econtext).ecxt_scantuple = scan_slot;

            // Apply qualification (WHERE clauses pushed down to scan)
            if !qual.is_null() && !exec_qual(qual, econtext) {
                // Reset per-tuple memory context on filtered rows
                pg_sys::MemoryContextReset((*econtext).ecxt_per_tuple_memory);
                state.timing.rows_filtered += 1;
                if let Some(t) = t_row {
                    state.timing.emit_us += t.elapsed().as_micros() as u64;
                }
                continue; // skip this row, try next
            }

            // Apply projection if needed
            let result = if !proj_info.is_null() {
                exec_project(proj_info)
            } else {
                scan_slot
            };
            state.timing.rows_emitted += 1;
            if let Some(t) = t_row {
                state.timing.emit_us += t.elapsed().as_micros() as u64;
            }
            return Some(result);
        }
    }
}

/// Load and decompress the next qualifying segment into state.
///
/// Active for all DeltaXDecompress and DeltaXAppend queries during the normal
/// (non-Top-N) scan path. Called by the outer loop in `exec_custom_scan` each
/// time `try_emit_next_row` exhausts the current segment.
///
/// Iterates through remaining segments in order, applying three levels of
/// pruning before decompressing:
///   1. Segment-by filters — skip segments whose partition key doesn't match
///   2. Time-range overlap — skip segments outside the query's time window
///   3. Dictionary LIKE — for LIKE/ILIKE predicates on text columns, check
///      the segment's dictionary to see if any value could possibly match
///
/// For segments that pass pruning, decompression happens in two phases to
/// minimize work:
///   - Phase 1: decompress only the columns referenced in WHERE clauses
///     (filter columns) and evaluate batch quals to produce a selection
///     vector (boolean mask of qualifying rows)
///   - Phase 2: decompress the remaining columns needed by SELECT/ORDER BY,
///     but only allocate text/variable-length data for rows that passed
///     Phase 1. This avoids expensive string allocation for filtered-out rows.
///
/// After loading, resets `row_cursor` to 0 so `try_emit_next_row` can start
/// emitting from the beginning of the segment.
///
/// Returns `true` if a segment was loaded, `false` if no segments remain
/// (signaling end of scan).
unsafe fn load_next_segment(state: &mut DecompressState, instrument: bool) -> bool {
    unsafe {
        loop {
            // Parallel path: atomically claim the next segment index from the
            // shared DSM cursor. Serial path: walk segment_index locally.
            if !state.pscan.is_null() {
                let idx = (*state.pscan).next_segment.fetch_add(1, Ordering::Relaxed);
                if idx >= state.segments_data.len() as u64 {
                    return false;
                }
                state.segment_index = idx as usize;
            } else {
                if state.segment_index >= state.segments_data.len() {
                    return false;
                }
            }

            let seg_idx = state.segment_index;
            if state.pscan.is_null() {
                state.segment_index += 1;
            }
            let seg = &state.segments_data[seg_idx];

            if seg.row_count == 0 {
                continue;
            }

            // Metadata-only pruning: segment-by equality + time-range overlap.
            if segment_pre_pruned_by_metadata(
                seg,
                &state.segment_by_filters,
                state.time_min,
                state.time_max,
            ) {
                state.timing.segments_skipped += 1;
                continue;
            }

            // Fetch needed blobs for this segment. `begin_deltax_append` runs with
            // `skip_blob_load = true`, so compressed_blobs are empty until we claim
            // the segment — blob I/O is amortised across workers in the parallel
            // path and kept sequential on the serial path (at a few µs of per-segment
            // PK-lookup overhead).
            let fetch_us = fetch_segment_blobs(
                state.segments_data[seg_idx].companion_oid,
                state.segments_data[seg_idx].segment_id,
                &state.col_names,
                &state.segment_by,
                &state.needed_cols,
                &mut state.segments_data[seg_idx],
            );
            state.timing.heap_scan_us += fetch_us;
            let seg = &state.segments_data[seg_idx];

            // Dictionary-based LIKE pruning
            if segment_skippable_by_dict(
                &state.batch_quals,
                &state.col_names,
                &state.segment_by,
                &seg.compressed_blobs,
            ) {
                state.timing.segments_skipped += 1;
                continue;
            }

            let t_decompress = if instrument {
                Some(Instant::now())
            } else {
                None
            };

            // Reset segment memory context — frees all varlena from previous segment
            pg_sys::MemoryContextReset(state.segment_mcxt);
            let old_ctx = pg_sys::MemoryContextSwitchTo(state.segment_mcxt);

            // === Phase 1: Decompress filter columns and segment-by ===
            // Columns referenced by batch quals are decompressed now.
            // Other needed columns are deferred to Phase 2 so that text
            // varlena allocation can be skipped for rows filtered out.
            let mut decompressed: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
            let mut blob_idx = 0;
            let mut seg_val_idx = 0;
            let mut pre_selection: Vec<bool> = Vec::new();
            let has_batch_quals = !state.batch_quals.is_empty();
            let mut phase2_cols: Vec<(usize, usize)> = Vec::new(); // (col_idx, blob_idx)

            for (col_idx, col_name) in state.col_names.iter().enumerate() {
                let type_oid = state.col_types[col_idx];

                if !state.needed_cols[col_idx] {
                    // Column not needed — push null placeholders and advance index
                    if state.segment_by.contains(col_name) {
                        seg_val_idx += 1;
                    } else {
                        blob_idx += 1;
                    }
                    decompressed.push(Vec::new());
                    continue;
                }

                if state.segment_by.contains(col_name) {
                    let val = &seg.segment_values[seg_val_idx];
                    let (datum, is_null) = match val {
                        Some(s) => (string_to_datum(s, type_oid), false),
                        None => (pg_sys::Datum::from(0), true),
                    };
                    let repeated: Vec<(pg_sys::Datum, bool)> =
                        (0..seg.row_count).map(|_| (datum, is_null)).collect();
                    decompressed.push(repeated);
                    seg_val_idx += 1;
                } else {
                    let blob = &seg.compressed_blobs[blob_idx];
                    let typmod = state.col_typmods[col_idx];

                    // Check if this column has a batch qual (filter column)
                    let like_qual = state.batch_quals.iter().find(|bq| {
                        bq.col_idx == col_idx
                            && matches!(bq.op, BatchCompareOp::Like | BatchCompareOp::NotLike)
                    });
                    let text_eq_qual = state.batch_quals.iter().find(|bq| {
                        bq.col_idx == col_idx
                            && bq.text_const.is_some()
                            && matches!(bq.op, BatchCompareOp::Eq | BatchCompareOp::Ne)
                    });
                    let text_in_qual = state.batch_quals.iter().find(|bq| {
                        bq.col_idx == col_idx
                            && bq.in_list_text.is_some()
                            && bq.op == BatchCompareOp::InList
                    });
                    let has_any_batch_qual =
                        state.batch_quals.iter().any(|bq| bq.col_idx == col_idx);

                    if let Some(bq) = like_qual {
                        let strat = bq.like_strategy.as_ref().unwrap();
                        let neg = bq.op == BatchCompareOp::NotLike;
                        let (datums, sel) = decompress_text_blob_with_like_filter(
                            blob, type_oid, typmod, strat, neg, None,
                        );
                        decompressed.push(datums);
                        merge_and_selection(&mut pre_selection, sel);
                    } else if let Some(bq) = text_eq_qual {
                        let const_str = bq.text_const.as_ref().unwrap();
                        let is_ne = bq.op == BatchCompareOp::Ne;
                        let (datums, sel) = decompress_text_blob_with_eq_filter(
                            blob, type_oid, typmod, const_str, is_ne, None,
                        );
                        decompressed.push(datums);
                        merge_and_selection(&mut pre_selection, sel);
                    } else if let Some(bq) = text_in_qual {
                        let strs = bq.in_list_text.as_ref().unwrap();
                        let (datums, sel) = decompress_text_blob_with_in_filter(
                            blob, type_oid, typmod, strs, /* is_not_in */ false, None,
                        );
                        decompressed.push(datums);
                        merge_and_selection(&mut pre_selection, sel);
                    } else if has_any_batch_qual {
                        // Column has a non-text batch qual (int/float comparison)
                        // — must decompress in Phase 1 for evaluate_batch_quals
                        let type_name = pg_type_name(type_oid);
                        let datums = decompress_blob_to_datums(blob, &type_name, type_oid, typmod);
                        decompressed.push(datums);
                    } else if has_batch_quals {
                        // No batch qual on this column, but other quals exist
                        // — defer to Phase 2 for selection-aware decompression
                        phase2_cols.push((col_idx, blob_idx));
                        decompressed.push(Vec::new());
                    } else {
                        // No batch quals at all — decompress immediately
                        let type_name = pg_type_name(type_oid);
                        let datums = decompress_blob_to_datums(blob, &type_name, type_oid, typmod);
                        decompressed.push(datums);
                    }
                    blob_idx += 1;
                }
            }

            pg_sys::MemoryContextSwitchTo(old_ctx);

            if let Some(t) = t_decompress {
                let elapsed = t.elapsed().as_micros() as u64;
                state.timing.decompress_us += elapsed;
                state.timing.phase1_us += elapsed;
            }
            state.timing.segments_decompressed += 1;

            state.current_segment = decompressed;
            state.current_row_count = seg.row_count as usize;
            state.row_cursor = 0;

            // Evaluate batch quals on the decompressed segment.
            // pre_selection seeds the selection vector so that rows already
            // filtered by LIKE during decompression are skipped.
            if !state.batch_quals.is_empty() || !pre_selection.is_empty() {
                let t_batch = if instrument {
                    Some(Instant::now())
                } else {
                    None
                };
                state.selection_vector = evaluate_batch_quals(
                    &state.current_segment,
                    state.current_row_count,
                    &state.batch_quals,
                    pre_selection,
                );
                if let Some(t) = t_batch {
                    state.timing.batch_eval_us += t.elapsed().as_micros() as u64;
                }
            } else {
                state.selection_vector.clear();
            }

            // === Phase 2: Decompress deferred columns with selection awareness ===
            // Text columns only allocate varlena for rows passing the selection
            // vector, avoiding expensive allocation for filtered-out rows.
            // Skip Phase 2 entirely if no rows are selected in this segment
            // (avoids codec decode for segments with zero matches).
            if !phase2_cols.is_empty() {
                let any_selected =
                    state.selection_vector.is_empty() || state.selection_vector.iter().any(|&s| s);

                if any_selected {
                    let t_phase2 = if instrument {
                        Some(Instant::now())
                    } else {
                        None
                    };
                    let old_ctx2 = pg_sys::MemoryContextSwitchTo(state.segment_mcxt);

                    for &(col_idx, p2_blob_idx) in &phase2_cols {
                        let blob = &seg.compressed_blobs[p2_blob_idx];
                        let type_oid = state.col_types[col_idx];
                        let typmod = state.col_typmods[col_idx];

                        let t_col = if instrument {
                            Some(Instant::now())
                        } else {
                            None
                        };
                        // Variable-length types (TEXT family + JSONB) have a
                        // selection-aware path that skips the per-row varlena
                        // palloc/memcpy for rows already filtered out by the
                        // Phase 1 batch_quals. Fixed-width types have no win
                        // (sequential codecs can't skip rows; the per-row
                        // datum cost is just an `as usize` cast).
                        let is_text = is_text_type(type_oid);
                        let is_varlena_selectable = is_text || type_oid == pg_sys::JSONBOID;
                        let datums = if is_varlena_selectable && !state.selection_vector.is_empty()
                        {
                            if is_text {
                                decompress_text_blob_with_selection(
                                    blob,
                                    type_oid,
                                    typmod,
                                    &state.selection_vector,
                                )
                            } else {
                                decompress_jsonb_blob_with_selection(blob, &state.selection_vector)
                            }
                        } else {
                            let type_name = pg_type_name(type_oid);
                            decompress_blob_to_datums(blob, &type_name, type_oid, typmod)
                        };
                        if let Some(t) = t_col {
                            let col_us = t.elapsed().as_micros() as u64;
                            if is_varlena_selectable {
                                state.timing.phase2_text_us += col_us;
                                state.timing.phase2_text_cols += 1;
                            } else {
                                state.timing.phase2_nontext_us += col_us;
                                state.timing.phase2_nontext_cols += 1;
                            }
                        }
                        state.current_segment[col_idx] = datums;
                    }

                    pg_sys::MemoryContextSwitchTo(old_ctx2);
                    if let Some(t) = t_phase2 {
                        let elapsed = t.elapsed().as_micros() as u64;
                        state.timing.decompress_us += elapsed;
                        state.timing.phase2_us += elapsed;
                    }
                } else {
                    state.timing.phase2_skipped += 1;
                    // no selected rows — skip decompression entirely
                }
            }

            return true;
        }
    }
}

/// EndCustomScan callback: cleanup and emit timing summary.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn end_custom_scan(node: *mut pg_sys::CustomScanState) {
    unsafe {
        let state_ptr = (*node).custom_ps as *mut DecompressState;
        if !state_ptr.is_null() {
            let state = Box::from_raw(state_ptr);

            // Emit timing summary at LOG level (visible with SET client_min_messages = log)
            // All timers are non-overlapping:
            //   metadata  — SPI metadata load (in begin)
            //   heap_scan — direct heap scan of compressed data (in begin)
            //   decompress — Phase 1 + Phase 2 decompression (in exec loop)
            //   batch_eval — batch qual evaluation (in exec loop)
            //   emit      — per-row fill_slot + qual + projection (in exec loop)
            let t = &state.timing;
            let total_us =
                t.metadata_us + t.heap_scan_us + t.decompress_us + t.batch_eval_us + t.emit_us;
            pgrx::log!(
                "pg_deltax timing: total={:.1}ms meta={:.1} heap={:.1} decomp={:.1} batch={:.1} emit={:.1}",
                total_us as f64 / 1000.0,
                t.metadata_us as f64 / 1000.0,
                t.heap_scan_us as f64 / 1000.0,
                t.decompress_us as f64 / 1000.0,
                t.batch_eval_us as f64 / 1000.0,
                t.emit_us as f64 / 1000.0,
            );
            pgrx::log!(
                "pg_deltax decomp: p1={:.1}ms p2={:.1}ms(text={:.1}/{} nontext={:.1}/{}) \
                 segs={}/{} mmskip={} p2skip={} rows={}/{}/{} topn={}/{}",
                t.phase1_us as f64 / 1000.0,
                t.phase2_us as f64 / 1000.0,
                t.phase2_text_us as f64 / 1000.0,
                t.phase2_text_cols,
                t.phase2_nontext_us as f64 / 1000.0,
                t.phase2_nontext_cols,
                t.segments_decompressed,
                t.segments_decompressed + t.segments_skipped,
                t.segments_minmax_skipped,
                t.phase2_skipped,
                t.rows_emitted,
                t.rows_filtered,
                t.rows_batch_filtered,
                t.topn_limit,
                t.topn_candidates,
            );

            if !state.segment_mcxt.is_null() {
                pg_sys::MemoryContextDelete(state.segment_mcxt);
            }
            (*node).custom_ps = std::ptr::null_mut();
        }
    }
}

/// ReScanCustomScan callback: reset the scan.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn rescan_custom_scan(node: *mut pg_sys::CustomScanState) {
    unsafe {
        let state = &mut *((*node).custom_ps as *mut DecompressState);
        state.segment_index = 0;
        state.row_cursor = 0;
        state.current_row_count = 0;
        state.current_segment.clear();
        state.selection_vector.clear();
        state.topn_buffer.clear();
        state.topn_cursor = 0;
        state.topn_done = false;
    }
}

// ============================================================================
// Parallel partial-path DSM hooks (DeltaXAppend only)
// ============================================================================

/// EstimateDSMCustomScan: how much shared memory this node needs.
///
/// Size = fixed `DeltaXAppendPState` (cursor + timing slots) + metadata-wire
/// region sized by `append_wire::layout` against the leader's pre-loaded
/// `segments_data`. The leader has already run `begin_deltax_append` by the
/// time this is called, so segment count and string lengths are known.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn estimate_dsm_deltax_append(
    node: *mut pg_sys::CustomScanState,
    _pcxt: *mut pg_sys::ParallelContext,
) -> pg_sys::Size {
    unsafe {
        let state = &*((*node).custom_ps as *const DecompressState);
        let companion_oids = extract_companion_oids_from_segments(&state.segments_data);
        let input = append_wire::WireInput {
            col_names: &state.col_names,
            col_types: &state.col_types,
            col_typmods: &state.col_typmods,
            segment_by: &state.segment_by,
            order_by: &[], // order_by not tracked in DecompressState; workers don't need it post-hydrate
            time_column: &state._time_column,
            companion_oids: &companion_oids,
            segments: &state.segments_data,
        };
        let layout = append_wire::layout(&input);
        (std::mem::size_of::<DeltaXAppendPState>() + layout.total_size as usize) as pg_sys::Size
    }
}

/// Collect unique companion OIDs from `segments_data` (the leader doesn't
/// keep the original slice around, but every segment carries its companion).
fn extract_companion_oids_from_segments(segments: &[SegmentData]) -> Vec<pg_sys::Oid> {
    let mut seen: Vec<pg_sys::Oid> = Vec::new();
    for seg in segments {
        if !seen.contains(&seg.companion_oid) {
            seen.push(seg.companion_oid);
        }
    }
    seen
}

/// InitializeDSMCustomScan: leader populates the shared state after its own
/// BeginCustomScan has loaded segments_data.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn initialize_dsm_deltax_append(
    node: *mut pg_sys::CustomScanState,
    pcxt: *mut pg_sys::ParallelContext,
    coordinate: *mut std::ffi::c_void,
) {
    unsafe {
        let ps = coordinate as *mut DeltaXAppendPState;
        std::ptr::write_bytes(ps as *mut u8, 0, std::mem::size_of::<DeltaXAppendPState>());

        let state = &mut *((*node).custom_ps as *mut DecompressState);

        // Cap slots: leader + nworkers. Hard-error if the planner asked for more
        // than we support — the cost formula + GUC range should prevent this.
        let nworkers = (*pcxt).nworkers as usize;
        if nworkers + 1 > MAX_WORKER_SLOTS {
            pgrx::error!(
                "pg_deltax: parallel worker count {} exceeds MAX_WORKER_SLOTS {}",
                nworkers,
                MAX_WORKER_SLOTS - 1,
            );
        }

        (*ps).next_segment = AtomicU64::new(0);
        (*ps).total_segments = state.segments_data.len() as u64;
        (*ps).n_worker_slots = (nworkers + 1) as u32;

        state.pscan = ps;

        // Serialise leader's metadata + segments into the region immediately
        // after `DeltaXAppendPState`. Workers read this in
        // `init_worker_deltax_append` instead of re-running SPI + heap scan.
        let wire_base = (coordinate as *mut u8).add(std::mem::size_of::<DeltaXAppendPState>());
        let companion_oids = extract_companion_oids_from_segments(&state.segments_data);
        let input = append_wire::WireInput {
            col_names: &state.col_names,
            col_types: &state.col_types,
            col_typmods: &state.col_typmods,
            segment_by: &state.segment_by,
            order_by: &[],
            time_column: &state._time_column,
            companion_oids: &companion_oids,
            segments: &state.segments_data,
        };
        let layout = append_wire::layout(&input);
        append_wire::serialize_into(wire_base, &input, &layout);
        state.wire_base = wire_base as *const u8;
    }
}

/// ReInitializeDSMCustomScan: reset the shared cursor on rescan.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn reinit_dsm_deltax_append(
    _node: *mut pg_sys::CustomScanState,
    _pcxt: *mut pg_sys::ParallelContext,
    coordinate: *mut std::ffi::c_void,
) {
    unsafe {
        let ps = coordinate as *mut DeltaXAppendPState;
        (*ps).next_segment.store(0, Ordering::Relaxed);
        for slot in (*ps).worker_timings.iter_mut() {
            *slot = ScanTimingShmem::default();
        }
    }
}

/// InitializeWorkerCustomScan: worker picks up the shared DSM pointers and
/// hydrates its `DecompressState` from the serialised metadata region.
///
/// The worker's `begin_deltax_append` ran earlier but skipped SPI + heap
/// scan (see `is_worker_stub`). This function reads the wire written by the
/// leader's `initialize_dsm_deltax_append`, rebuilds col_names / col_types /
/// segment_by / time_column / segments_data, and re-runs the plan-qual
/// extraction passes that depend on those fields.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn init_worker_deltax_append(
    node: *mut pg_sys::CustomScanState,
    _toc: *mut pg_sys::shm_toc,
    coordinate: *mut std::ffi::c_void,
) {
    unsafe {
        let state = &mut *((*node).custom_ps as *mut DecompressState);
        state.pscan = coordinate as *mut DeltaXAppendPState;
        let wire_base =
            (coordinate as *mut u8).add(std::mem::size_of::<DeltaXAppendPState>()) as *const u8;
        state.wire_base = wire_base;

        let view = match DeltaXAppendView::attach(wire_base) {
            Some(v) => v,
            None => pgrx::error!(
                "pg_deltax: DSM wire magic mismatch — worker cannot attach to leader's metadata",
            ),
        };

        let header = view.decode_header();
        state.col_names = header.col_names;
        state.col_types = header.col_types;
        state.col_typmods = header.col_typmods;
        state.segment_by = header.segment_by;
        state._time_column = header.time_column;

        // Pre-decode every SegmentEntry into a process-local `SegmentData`.
        // Each decode is a small memcpy + a UTF-8 validation on segment_values,
        // so the per-worker cost is proportional to (num_segments × avg_seg_values_bytes).
        // Blobs are still fetched lazily inside `load_next_segment` via
        // `fetch_segment_blobs`; this only populates the metadata fields.
        let mut segments_data: Vec<SegmentData> = Vec::with_capacity(header.num_segments);
        for idx in 0..header.num_segments {
            let b = view.decode_segment(idx);
            let num_blob_cols = state
                .col_names
                .iter()
                .filter(|n| !state.segment_by.contains(*n))
                .count();
            let mut compressed_blobs: Vec<super::segments::BlobBytes> =
                Vec::with_capacity(num_blob_cols);
            compressed_blobs.resize_with(num_blob_cols, super::segments::BlobBytes::default);
            segments_data.push(SegmentData {
                companion_oid: b.companion_oid,
                segment_id: b.segment_id,
                segment_values: b.segment_values,
                compressed_blobs,
                text_length_blobs: vec![Vec::new(); num_blob_cols],
                row_count: b.row_count,
                min_time: b.min_time,
                max_time: b.max_time,
                col_minmax: std::collections::HashMap::new(),
                col_sums: std::collections::HashMap::new(),
                toast_pointers: vec![Vec::new(); num_blob_cols],
                cached_blob_pins: Vec::new(),
            });
        }
        state.segments_data = segments_data;
        state.wire_view = Some(view);

        // Now that col_names / col_types are live, rebuild qual-driven state
        // that the leader built in `begin_deltax_append`.
        let plan_qual = (*(*node).ss.ps.plan).qual;
        let (batch_quals, handled_count) =
            extract_batch_quals(plan_qual, &state.col_names, &state.col_types);
        let nquals = if plan_qual.is_null() {
            0
        } else {
            (*plan_qual).length as usize
        };
        let all_quals_batch_handled = handled_count > 0 && handled_count == nquals;
        if all_quals_batch_handled {
            (*node).ss.ps.qual = std::ptr::null_mut();
        }
        state.batch_quals = batch_quals;
        state.all_quals_batch_handled = all_quals_batch_handled;

        let (seg_filters, t_min, t_max) = extract_segment_filters(
            plan_qual,
            &state.col_names,
            &state.segment_by,
            &state._time_column,
        );
        state.segment_by_filters = seg_filters;
        state.time_min = t_min;
        state.time_max = t_max;

        // Build needed_cols / needed_col_indices from the planner-supplied
        // column-index list in custom_private (bytes after the -1 sentinel),
        // plus any extra columns that batch_quals references. The plan node
        // is a `CustomScan` — cast via `*mut _` and read `custom_private`.
        let cscan = (*node).ss.ps.plan as *mut pg_sys::CustomScan;
        let custom_private_list = (*cscan).custom_private;
        let (needed_cols, needed_col_indices) = build_needed_cols_from_custom_private(
            custom_private_list,
            &state.col_names,
            &state.batch_quals,
        );
        state.needed_cols = needed_cols;
        state.needed_col_indices = needed_col_indices;

        // Create per-segment memory context (leader did this in begin).
        if state.segment_mcxt.is_null() {
            let query_ctx = (*(*node).ss.ps.state).es_query_cxt;
            state.segment_mcxt = pg_sys::AllocSetContextCreateInternal(
                query_ctx,
                c"DeltaXSegment".as_ptr(),
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
            );
        }

        state.is_worker_stub = false;
    }
}

/// Parse a `DeltaXAppend` `custom_private` list and compute the needed-column
/// mask + dense-index list against the freshly-hydrated `col_names`. Mirrors
/// the leader's inline logic in `begin_deltax_append`.
fn build_needed_cols_from_custom_private(
    custom_private: *mut pg_sys::List,
    col_names: &[String],
    batch_quals: &[BatchQual],
) -> (Vec<bool>, Vec<usize>) {
    let num_cols = col_names.len();
    let mut needed_cols = vec![false; num_cols];
    let mut needed_col_indices: Vec<usize> = Vec::new();

    let parsed = unsafe { parse_custom_private(custom_private) };
    for idx in parsed.needed_indices {
        if idx < num_cols && !needed_cols[idx] {
            needed_cols[idx] = true;
            needed_col_indices.push(idx);
        }
    }
    for bq in batch_quals {
        if bq.col_idx < num_cols && !needed_cols[bq.col_idx] {
            needed_cols[bq.col_idx] = true;
            needed_col_indices.push(bq.col_idx);
        }
    }

    (needed_cols, needed_col_indices)
}

/// ShutdownCustomScan: flush this process's timing counters into the shared
/// slot so the leader can aggregate per-worker numbers for EXPLAIN output.
/// PG may call this before EndCustomScan when tearing down workers.
///
/// On the leader, also snapshot the DSM worker_timings into a local Vec
/// so EXPLAIN can render them after DSM is detached.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn shutdown_deltax_append(node: *mut pg_sys::CustomScanState) {
    unsafe {
        let state_ptr = (*node).custom_ps as *mut DecompressState;
        if state_ptr.is_null() {
            return;
        }
        let state = &mut *state_ptr;
        flush_timing_to_shmem(state);

        // Leader (ParallelWorkerNumber < 0) caches all DSM slots now while
        // the DSM is still attached. Workers skip this step.
        if !state.pscan.is_null() && pg_sys::ParallelWorkerNumber < 0 {
            let ps = &*state.pscan;
            let n = (ps.n_worker_slots as usize).min(MAX_WORKER_SLOTS);
            state.cached_worker_timings = ps.worker_timings[..n].to_vec();
        }

        // Clear DSM-borrowed pointers before PG tears the DSM region down.
        // `wire_view` borrows from `wire_base`; drop it first. The backing
        // `SegmentData` Vec we decoded in `init_worker` is process-local and
        // stays valid until `EndCustomScan`.
        state.wire_view = None;
        state.wire_base = std::ptr::null();
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Fill a TupleTableSlot from pre-computed datums at the current row cursor.
unsafe fn fill_slot(slot: *mut pg_sys::TupleTableSlot, state: &DecompressState) {
    unsafe {
        pg_sys::ExecClearTuple(slot);

        let ncols = state.col_names.len();
        if state.needed_col_indices.is_empty() {
            // COUNT(*) fast path: no columns needed, just mark all null
            std::ptr::write_bytes((*slot).tts_isnull, true as u8, ncols);
        } else {
            // Set all columns to null first (one memset)
            std::ptr::write_bytes((*slot).tts_isnull, true as u8, ncols);
            // Then fill only needed columns
            for &col_idx in &state.needed_col_indices {
                let (datum, is_null) = state.current_segment[col_idx][state.row_cursor];
                (*slot).tts_isnull.add(col_idx).write(is_null);
                (*slot).tts_values.add(col_idx).write(datum);
            }
        }

        pg_sys::ExecStoreVirtualTuple(slot);
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn test_cmp_topn_key_ascending() {
        // ascending: smaller first
        assert_eq!(
            cmp_topn_key(1, false, 2, false, /*asc*/ true, /*nf*/ false),
            Ordering::Less
        );
        assert_eq!(
            cmp_topn_key(2, false, 1, false, true, false),
            Ordering::Greater
        );
        assert_eq!(
            cmp_topn_key(5, false, 5, false, true, false),
            Ordering::Equal
        );
    }

    #[test]
    fn test_cmp_topn_key_descending() {
        assert_eq!(
            cmp_topn_key(1, false, 2, false, /*asc*/ false, /*nf*/ false),
            Ordering::Greater
        );
        assert_eq!(
            cmp_topn_key(2, false, 1, false, false, false),
            Ordering::Less
        );
    }

    #[test]
    fn test_cmp_topn_key_nulls() {
        // NULL == NULL is Equal regardless of mode
        assert_eq!(cmp_topn_key(0, true, 0, true, true, true), Ordering::Equal);
        // nulls_first=true: NULL < non-NULL
        assert_eq!(
            cmp_topn_key(0, true, 42, false, true, /*nf*/ true),
            Ordering::Less
        );
        assert_eq!(
            cmp_topn_key(42, false, 0, true, true, /*nf*/ true),
            Ordering::Greater
        );
        // nulls_first=false: NULL > non-NULL
        assert_eq!(
            cmp_topn_key(0, true, 42, false, true, /*nf*/ false),
            Ordering::Greater
        );
        assert_eq!(
            cmp_topn_key(42, false, 0, true, true, /*nf*/ false),
            Ordering::Less
        );
    }

    #[test]
    fn test_cmp_nullable_str_byte() {
        // Byte comparison (no collation)
        assert_eq!(
            cmp_nullable_str_byte(Some("aaa"), Some("bbb"), true, false),
            Ordering::Less
        );
        assert_eq!(
            cmp_nullable_str_byte(Some("bbb"), Some("aaa"), true, false),
            Ordering::Greater
        );
        // descending flips
        assert_eq!(
            cmp_nullable_str_byte(Some("aaa"), Some("bbb"), false, false),
            Ordering::Greater
        );
        // NULL handling matches cmp_topn_key
        assert_eq!(
            cmp_nullable_str_byte(None, None, true, false),
            Ordering::Equal
        );
        assert_eq!(
            cmp_nullable_str_byte(None, Some("x"), true, /*nf*/ true),
            Ordering::Less
        );
        assert_eq!(
            cmp_nullable_str_byte(None, Some("x"), true, /*nf*/ false),
            Ordering::Greater
        );
    }

    #[test]
    fn test_merge_and_selection_empty_target_adopts_src() {
        let mut target: Vec<bool> = Vec::new();
        merge_and_selection(&mut target, vec![true, false, true]);
        assert_eq!(target, vec![true, false, true]);
    }

    #[test]
    fn test_merge_and_selection_ands_into_existing() {
        let mut target = vec![true, true, false, true];
        merge_and_selection(&mut target, vec![true, false, true, true]);
        assert_eq!(target, vec![true, false, false, true]);
    }

    #[test]
    fn test_col_to_blob_idx() {
        let cols: Vec<String> = ["t", "host", "metric", "val", "tag"]
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let segby: Vec<String> = ["host", "tag"].iter().map(|s| (*s).to_string()).collect();
        // segment_by columns ("host", "tag") are skipped in blob layout.
        // Blob order is: t (0), metric (1), val (2)
        assert_eq!(col_to_blob_idx(&cols, &segby, 0), 0); // t
        assert_eq!(col_to_blob_idx(&cols, &segby, 2), 1); // metric
        assert_eq!(col_to_blob_idx(&cols, &segby, 3), 2); // val
    }

    #[test]
    fn test_compute_phase1_col_indices() {
        let col_names: Vec<String> = ["t", "host", "metric", "val", "extra"]
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let segby: Vec<String> = ["host"].iter().map(|s| (*s).to_string()).collect();
        let needed = vec![true, true, true, true, false]; // extra not needed
        // batch qual on col 3 (val)
        let bq = BatchQual {
            col_idx: 3,
            op: BatchCompareOp::Eq,
            const_datum: pg_sys::Datum::from(0u64),
            type_oid: pg_sys::INT8OID,
            like_strategy: None,
            text_const: None,
            in_list_i64: None,
            in_list_text: None,
        };
        // sort_col = 0 (t)
        let phase1 = compute_phase1_col_indices(&col_names, &segby, &needed, &[bq], 0);
        // Should include sort col (0) and batch-qual col (3). Not host (no qual on it).
        // Not metric (no qual, not sort). Not extra (not needed).
        assert_eq!(phase1, vec![0, 3]);
    }

    #[test]
    fn test_compute_phase1_col_indices_segby_with_qual() {
        let col_names: Vec<String> = ["t", "host", "metric"]
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let segby: Vec<String> = ["host"].iter().map(|s| (*s).to_string()).collect();
        let needed = vec![true, true, true];
        // batch qual on host (segment_by column)
        let bq = BatchQual {
            col_idx: 1,
            op: BatchCompareOp::Eq,
            const_datum: pg_sys::Datum::from(0u64),
            type_oid: pg_sys::TEXTOID,
            like_strategy: None,
            text_const: Some("h1".into()),
            in_list_i64: None,
            in_list_text: None,
        };
        let phase1 = compute_phase1_col_indices(&col_names, &segby, &needed, &[bq], 0);
        // Sort col 0 + segby col 1 (with qual). metric has no qual.
        assert_eq!(phase1, vec![0, 1]);
    }

    #[test]
    fn test_compute_phase1_blob_indices() {
        let col_names: Vec<String> = ["t", "host", "metric", "val"]
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let segby: Vec<String> = ["host"].iter().map(|s| (*s).to_string()).collect();
        // Phase 1 col indices include t (0) and val (3); host is segment_by and excluded.
        let phase1_cols = vec![0, 1, 3];
        // Blob layout: t=0, metric=1, val=2. So phase1 blob indices = {0, 2}.
        let blob_indices = compute_phase1_blob_indices(&col_names, &segby, &phase1_cols);
        assert_eq!(blob_indices, vec![0, 2]);
    }

    #[test]
    fn test_segment_pre_pruned_by_metadata_no_filters() {
        let seg = SegmentData {
            companion_oid: pg_sys::Oid::from(0u32),
            segment_id: 0,
            segment_values: vec![Some("a".into())],
            compressed_blobs: Vec::new(),
            text_length_blobs: Vec::new(),
            row_count: 100,
            min_time: Some(1_000),
            max_time: Some(2_000),
            col_minmax: std::collections::HashMap::new(),
            col_sums: std::collections::HashMap::new(),
            toast_pointers: Vec::new(),
            cached_blob_pins: Vec::new(),
        };
        // No filters, no time range — never pruned.
        assert!(!segment_pre_pruned_by_metadata(&seg, &[], None, None));
    }

    #[test]
    fn test_segment_pre_pruned_by_segby_match() {
        let seg = SegmentData {
            companion_oid: pg_sys::Oid::from(0u32),
            segment_id: 0,
            segment_values: vec![Some("host-a".into()), Some("metric-x".into())],
            compressed_blobs: Vec::new(),
            text_length_blobs: Vec::new(),
            row_count: 100,
            min_time: None,
            max_time: None,
            col_minmax: std::collections::HashMap::new(),
            col_sums: std::collections::HashMap::new(),
            toast_pointers: Vec::new(),
            cached_blob_pins: Vec::new(),
        };
        // svi=0, filter "host-a" matches
        assert!(!segment_pre_pruned_by_metadata(
            &seg,
            &[(0, "host-a".into())],
            None,
            None
        ));
        // svi=0, filter "host-b" doesn't match → pruned
        assert!(segment_pre_pruned_by_metadata(
            &seg,
            &[(0, "host-b".into())],
            None,
            None
        ));
        // Two filters, both match
        assert!(!segment_pre_pruned_by_metadata(
            &seg,
            &[(0, "host-a".into()), (1, "metric-x".into())],
            None,
            None
        ));
        // One of two doesn't match
        assert!(segment_pre_pruned_by_metadata(
            &seg,
            &[(0, "host-a".into()), (1, "metric-z".into())],
            None,
            None
        ));
    }

    #[test]
    fn test_segment_pre_pruned_by_time_range() {
        let seg = SegmentData {
            companion_oid: pg_sys::Oid::from(0u32),
            segment_id: 0,
            segment_values: Vec::new(),
            compressed_blobs: Vec::new(),
            text_length_blobs: Vec::new(),
            row_count: 100,
            min_time: Some(1_000),
            max_time: Some(2_000),
            col_minmax: std::collections::HashMap::new(),
            col_sums: std::collections::HashMap::new(),
            toast_pointers: Vec::new(),
            cached_blob_pins: Vec::new(),
        };
        // Query range [3_000, ..) → seg.max_time (2_000) < 3_000, prune
        assert!(segment_pre_pruned_by_metadata(&seg, &[], Some(3_000), None));
        // Query range (.., 500] → seg.min_time (1_000) > 500, prune
        assert!(segment_pre_pruned_by_metadata(&seg, &[], None, Some(500)));
        // Overlapping range → no prune
        assert!(!segment_pre_pruned_by_metadata(
            &seg,
            &[],
            Some(1_500),
            Some(1_800)
        ));
        // Range fully contained in segment → no prune
        assert!(!segment_pre_pruned_by_metadata(
            &seg,
            &[],
            Some(0),
            Some(10_000)
        ));
    }
}
