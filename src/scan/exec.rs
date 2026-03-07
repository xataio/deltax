use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::pg_guard;

use std::collections::HashMap;
use std::time::Instant;

use crate::compression::{self, CompressionType, CompressedColumnRef};
use super::SyncStatic;

/// Static CustomExecMethods struct for SeaTurtleDecompress.
pub(crate) static CUSTOM_EXEC_METHODS: SyncStatic<pg_sys::CustomExecMethods> =
    SyncStatic(pg_sys::CustomExecMethods {
        CustomName: super::CUSTOM_NAME.as_ptr(),
        BeginCustomScan: Some(begin_custom_scan),
        ExecCustomScan: Some(exec_custom_scan),
        EndCustomScan: Some(end_custom_scan),
        ReScanCustomScan: Some(rescan_custom_scan),
        MarkPosCustomScan: None,
        RestrPosCustomScan: None,
        EstimateDSMCustomScan: None,
        InitializeDSMCustomScan: None,
        ReInitializeDSMCustomScan: None,
        InitializeWorkerCustomScan: None,
        ShutdownCustomScan: None,
        ExplainCustomScan: Some(super::explain::explain_custom_scan),
    });

/// Static CustomExecMethods struct for SeaTurtleCount (COUNT(*) pushdown).
pub(crate) static SEATURTLE_COUNT_EXEC_METHODS: SyncStatic<pg_sys::CustomExecMethods> =
    SyncStatic(pg_sys::CustomExecMethods {
        CustomName: super::SEATURTLE_COUNT_NAME.as_ptr(),
        BeginCustomScan: Some(begin_count_scan),
        ExecCustomScan: Some(exec_count_scan),
        EndCustomScan: Some(end_count_scan),
        ReScanCustomScan: Some(rescan_count_scan),
        MarkPosCustomScan: None,
        RestrPosCustomScan: None,
        EstimateDSMCustomScan: None,
        InitializeDSMCustomScan: None,
        ReInitializeDSMCustomScan: None,
        InitializeWorkerCustomScan: None,
        ShutdownCustomScan: None,
        ExplainCustomScan: Some(super::explain::explain_count_scan),
    });

/// Static CustomExecMethods struct for SeaTurtleMinMax (MIN/MAX pushdown).
pub(crate) static SEATURTLE_MINMAX_EXEC_METHODS: SyncStatic<pg_sys::CustomExecMethods> =
    SyncStatic(pg_sys::CustomExecMethods {
        CustomName: super::SEATURTLE_MINMAX_NAME.as_ptr(),
        BeginCustomScan: Some(begin_minmax_scan),
        ExecCustomScan: Some(exec_minmax_scan),
        EndCustomScan: Some(end_minmax_scan),
        ReScanCustomScan: Some(rescan_minmax_scan),
        MarkPosCustomScan: None,
        RestrPosCustomScan: None,
        EstimateDSMCustomScan: None,
        InitializeDSMCustomScan: None,
        ReInitializeDSMCustomScan: None,
        InitializeWorkerCustomScan: None,
        ShutdownCustomScan: None,
        ExplainCustomScan: Some(super::explain::explain_minmax_scan),
    });

/// Static CustomExecMethods struct for SeaTurtleAppend.
pub(crate) static SEATURTLE_APPEND_EXEC_METHODS: SyncStatic<pg_sys::CustomExecMethods> =
    SyncStatic(pg_sys::CustomExecMethods {
        CustomName: super::SEATURTLE_APPEND_NAME.as_ptr(),
        BeginCustomScan: Some(begin_seaturtle_append),
        ExecCustomScan: Some(exec_custom_scan),
        EndCustomScan: Some(end_custom_scan),
        ReScanCustomScan: Some(rescan_custom_scan),
        MarkPosCustomScan: None,
        RestrPosCustomScan: None,
        EstimateDSMCustomScan: None,
        InitializeDSMCustomScan: None,
        ReInitializeDSMCustomScan: None,
        InitializeWorkerCustomScan: None,
        ShutdownCustomScan: None,
        ExplainCustomScan: Some(super::explain::explain_seaturtle_append),
    });

// Epoch offset: microseconds between Unix epoch (1970-01-01) and PG epoch (2000-01-01).
const PG_EPOCH_OFFSET_USEC: i64 = 946_684_800_000_000;
// Days between Unix epoch and PG epoch.
const PG_EPOCH_OFFSET_DAYS: i32 = 10_957;

#[derive(Debug, Clone, Copy, PartialEq)]
enum BatchCompareOp { Eq, Ne, Lt, Le, Gt, Ge, Like, NotLike }

#[derive(Debug, Clone)]
enum LikeStrategy {
    Contains(String),    // %foo%  → str::contains
    StartsWith(String),  // foo%   → str::starts_with
    EndsWith(String),    // %foo   → str::ends_with
    Exact(String),       // foo    → ==
    General(String),     // patterns with _, \, or multiple % segments
}

#[derive(Debug, Clone)]
struct BatchQual {
    col_idx: usize,              // 0-based column index
    op: BatchCompareOp,          // comparison operator
    const_datum: pg_sys::Datum,  // constant value
    type_oid: pg_sys::Oid,       // column type OID
    like_strategy: Option<LikeStrategy>, // pre-compiled LIKE pattern
    text_const: Option<String>,  // text constant for Eq/Ne pushdown
}

/// Decompression state stored as a raw pointer in the CustomScanState.
pub(super) struct DecompressState {
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
    pub(super) timing: ScanTiming,
    /// Whether EXPLAIN ANALYZE is active (enables per-call timing).
    /// Set lazily on first exec call (PG sets PlanState.instrument after BeginCustomScan).
    instrument: Option<bool>,

    /// Time column name (from hypertable metadata).
    _time_column: String,

    // Segment pruning filters extracted from plan qual
    /// (index into segment_values, value to match) for segment_by equality filters.
    segment_by_filters: Vec<(usize, String)>,
    /// Lower bound for time column (PG epoch microseconds), inclusive.
    time_min: Option<i64>,
    /// Upper bound for time column (PG epoch microseconds), inclusive.
    time_max: Option<i64>,

    /// Batch quals extracted from plan qual for vectorized evaluation.
    batch_quals: Vec<BatchQual>,
    /// Selection vector: true = row passes batch quals. Empty = all pass.
    selection_vector: Vec<bool>,
}

/// Wall-clock timing for the decompress scan phases.
pub(super) struct ScanTiming {
    /// Time spent in load_metadata (SPI).
    pub(super) metadata_us: u64,
    /// Time spent in load_segments_heap (heap scan + detoast).
    pub(super) heap_scan_us: u64,
    /// Time spent decompressing blobs to datums (per segment).
    pub(super) decompress_us: u64,
    /// Time spent in fill_slot + qual + projection (per row).
    pub(super) emit_us: u64,
    /// Total rows emitted (passed qual).
    pub(super) rows_emitted: u64,
    /// Total rows filtered by qual.
    pub(super) rows_filtered: u64,
    /// Time spent in batch qual evaluation (per segment).
    pub(super) batch_eval_us: u64,
    /// Total rows filtered by batch quals (before fill_slot).
    pub(super) rows_batch_filtered: u64,
    /// Total segments decompressed.
    pub(super) segments_decompressed: u64,
    /// Total compressed bytes loaded.
    pub(super) compressed_bytes: u64,
    /// Total segments skipped by pruning.
    pub(super) segments_skipped: u64,
}

/// Per-column min/max metadata from the companion table.
struct ColMinMax {
    min_datum: pg_sys::Datum,
    max_datum: pg_sys::Datum,
    min_null: bool,
    max_null: bool,
    type_oid: pg_sys::Oid,
}

struct SegmentData {
    segment_values: Vec<Option<String>>,
    compressed_blobs: Vec<Vec<u8>>,
    row_count: i32,
    min_time: Option<i64>,
    max_time: Option<i64>,
    /// Per-column min/max (column name → ColMinMax).
    col_minmax: HashMap<String, ColMinMax>,
}

/// State for SeaTurtleCount (COUNT(*) pushdown).
pub(super) struct CountScanState {
    pub(super) total_count: i64,
    returned: bool,
    pub(super) metadata_us: u64,
    pub(super) heap_scan_us: u64,
    pub(super) total_segments: u64,
}

/// Result for one MIN/MAX aggregate in a multi-aggregate pushdown.
pub(super) struct MinMaxResult {
    pub(super) datum: pg_sys::Datum,
    pub(super) is_null: bool,
    pub(super) col_name: String,
    pub(super) is_min: bool,
    pub(super) type_oid: pg_sys::Oid,
}

/// State for SeaTurtleMinMax (MIN/MAX pushdown on any column, multi-aggregate).
pub(super) struct MinMaxScanState {
    /// Results: one per aggregate.
    pub(super) results: Vec<MinMaxResult>,
    returned: bool,
    pub(super) metadata_us: u64,
    pub(super) heap_scan_us: u64,
    pub(super) total_segments: u64,
}

// ============================================================================
// SeaTurtleAgg: aggregate pushdown (SUM, AVG, COUNT, COUNT(DISTINCT), GROUP BY)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum AggType { Sum, Count, CountStar, Avg, CountDistinct }

enum AggAccumulator {
    SumInt { sum: i128, count: i64 },
    SumFloat { sum: f64, count: i64 },
    Count { count: i64 },
    CountDistinctInt { seen: std::collections::HashSet<i64> },
    CountDistinctStr { seen: std::collections::HashSet<String> },
}

impl AggAccumulator {
    fn new_for(agg_type: AggType, col_type: pg_sys::Oid) -> Self {
        match agg_type {
            AggType::Sum | AggType::Avg => {
                if col_type == pg_sys::FLOAT4OID || col_type == pg_sys::FLOAT8OID {
                    AggAccumulator::SumFloat { sum: 0.0, count: 0 }
                } else {
                    AggAccumulator::SumInt { sum: 0, count: 0 }
                }
            }
            AggType::Count | AggType::CountStar => AggAccumulator::Count { count: 0 },
            AggType::CountDistinct => {
                if col_type == pg_sys::TEXTOID || col_type == pg_sys::VARCHAROID || col_type == pg_sys::BPCHAROID {
                    AggAccumulator::CountDistinctStr { seen: std::collections::HashSet::new() }
                } else {
                    AggAccumulator::CountDistinctInt { seen: std::collections::HashSet::new() }
                }
            }
        }
    }

    fn clone_fresh(&self) -> Self {
        match self {
            AggAccumulator::SumInt { .. } => AggAccumulator::SumInt { sum: 0, count: 0 },
            AggAccumulator::SumFloat { .. } => AggAccumulator::SumFloat { sum: 0.0, count: 0 },
            AggAccumulator::Count { .. } => AggAccumulator::Count { count: 0 },
            AggAccumulator::CountDistinctInt { .. } => AggAccumulator::CountDistinctInt { seen: std::collections::HashSet::new() },
            AggAccumulator::CountDistinctStr { .. } => AggAccumulator::CountDistinctStr { seen: std::collections::HashSet::new() },
        }
    }
}

pub(super) struct AggExecSpec {
    pub(super) agg_type: AggType,
    pub(super) col_idx: i32,               // -1 for COUNT(*)
    pub(super) col_type_oid: pg_sys::Oid,  // source column type
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct GroupByColSpec {
    pub(super) col_idx: i32,  // 0-based column index
    pub(super) type_oid: pg_sys::Oid,
}

/// State for SeaTurtleAgg (aggregate pushdown).
pub(super) struct AggScanState {
    pub(super) _agg_specs: Vec<AggExecSpec>,
    pub(super) _group_specs: Vec<GroupByColSpec>,
    pub(super) result_rows: Vec<Vec<(pg_sys::Datum, bool)>>,
    pub(super) result_idx: usize,
    pub(super) _num_result_cols: usize,
    pub(super) metadata_us: u64,
    pub(super) heap_scan_us: u64,
    pub(super) decompress_us: u64,
    pub(super) agg_us: u64,
    pub(super) total_segments: u64,
    pub(super) total_rows_processed: u64,
}

/// Static CustomExecMethods struct for SeaTurtleAgg.
pub(crate) static SEATURTLE_AGG_EXEC_METHODS: SyncStatic<pg_sys::CustomExecMethods> =
    SyncStatic(pg_sys::CustomExecMethods {
        CustomName: super::SEATURTLE_AGG_NAME.as_ptr(),
        BeginCustomScan: Some(begin_agg_scan),
        ExecCustomScan: Some(exec_agg_scan),
        EndCustomScan: Some(end_agg_scan),
        ReScanCustomScan: Some(rescan_agg_scan),
        MarkPosCustomScan: None,
        RestrPosCustomScan: None,
        EstimateDSMCustomScan: None,
        InitializeDSMCustomScan: None,
        ReInitializeDSMCustomScan: None,
        InitializeWorkerCustomScan: None,
        ShutdownCustomScan: None,
        ExplainCustomScan: Some(super::explain::explain_agg_scan),
    });

/// CreateCustomScanState callback for SeaTurtleCount.
#[pg_guard]
pub unsafe extern "C-unwind" fn create_count_scan_state(
    cscan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    unsafe {
        let css = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScanState>())
            as *mut pg_sys::CustomScanState;

        (*css).ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
        (*css).methods = &SEATURTLE_COUNT_EXEC_METHODS.0;

        // Copy custom_private for use in BeginCustomScan
        (*css).custom_ps = (*cscan).custom_private;

        css as *mut pg_sys::Node
    }
}

/// CreateCustomScanState callback.
#[pg_guard]
pub unsafe extern "C-unwind" fn create_custom_scan_state(
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
pub unsafe extern "C-unwind" fn begin_custom_scan(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: i32,
) {
    unsafe {
        // Get custom_private (stored as IntList: [oid, -1, col0, col1, ...])
        let custom_private = (*node).custom_ps;
        if custom_private.is_null() {
            pgrx::error!("pg_seaturtle: missing companion table OID in custom scan state");
        }

        let companion_oid =
            pg_sys::Oid::from(pg_sys::list_nth_int(custom_private, 0) as u32);

        // Parse needed column indices from custom_private (after sentinel -1)
        let list_len = (*custom_private).length;
        let mut needed_indices: Vec<usize> = Vec::new();
        let mut found_sentinel = false;
        for i in 1..list_len {
            let val = pg_sys::list_nth_int(custom_private, i);
            if val == -1 {
                found_sentinel = true;
                continue;
            }
            if found_sentinel {
                needed_indices.push(val as usize);
            }
        }

        // Get companion table name
        let companion_name = {
            let name_ptr = pg_sys::get_rel_name(companion_oid);
            if name_ptr.is_null() {
                pgrx::error!(
                    "pg_seaturtle: companion table not found for OID {}",
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
        let mut state = load_decompress_state(companion_oid, &companion_name, &needed_indices, plan_qual);

        // Create per-segment memory context
        let query_ctx = (*(*node).ss.ps.state).es_query_cxt;
        state.segment_mcxt = pg_sys::AllocSetContextCreateInternal(
            query_ctx,
            c"SeaTurtleSegment".as_ptr(),
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        );

        // Sort segments by min_time for time-ordered output
        state.segments_data.sort_by_key(|s| s.min_time.unwrap_or(i64::MAX));

        // Box and store as raw pointer in custom_ps
        let state_box = Box::new(state);
        let state_ptr = Box::into_raw(state_box);
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}

/// CreateCustomScanState callback for SeaTurtleAppend.
#[pg_guard]
pub unsafe extern "C-unwind" fn create_seaturtle_append_state(
    cscan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    unsafe {
        let css = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScanState>())
            as *mut pg_sys::CustomScanState;

        (*css).ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
        (*css).methods = &SEATURTLE_APPEND_EXEC_METHODS.0;

        // Copy custom_private for use in BeginCustomScan
        (*css).custom_ps = (*cscan).custom_private;

        css as *mut pg_sys::Node
    }
}

/// BeginCustomScan callback for SeaTurtleAppend: load segments from all companion tables.
#[pg_guard]
pub unsafe extern "C-unwind" fn begin_seaturtle_append(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: i32,
) {
    unsafe {
        let custom_private = (*node).custom_ps;
        if custom_private.is_null() {
            pgrx::error!("pg_seaturtle: missing companion table OIDs in SeaTurtleAppend state");
        }

        let list_len = (*custom_private).length;

        // Parse companion OIDs (before sentinel -1) and needed column indices (after sentinel)
        let mut companion_oids: Vec<pg_sys::Oid> = Vec::new();
        let mut needed_indices: Vec<usize> = Vec::new();
        let mut found_sentinel = false;
        for i in 0..list_len {
            let val = pg_sys::list_nth_int(custom_private, i);
            if val == -1 {
                found_sentinel = true;
                continue;
            }
            if found_sentinel {
                needed_indices.push(val as usize);
            } else {
                companion_oids.push(pg_sys::Oid::from(val as u32));
            }
        }

        if companion_oids.is_empty() {
            pgrx::error!("pg_seaturtle: SeaTurtleAppend has no companion tables");
        }

        // Get first companion table name for metadata
        let first_name = {
            let name_ptr = pg_sys::get_rel_name(companion_oids[0]);
            if name_ptr.is_null() {
                pgrx::error!(
                    "pg_seaturtle: companion table not found for OID {}",
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
        let batch_quals = extract_batch_quals(plan_qual, &meta.col_names, &meta.col_types);

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

        // Load segments from ALL companion tables via heap scan (with lazy pruning)
        let t1 = Instant::now();
        let mut all_segments: Vec<SegmentData> = Vec::new();
        let mut total_skipped: u64 = 0;
        for &oid in &companion_oids {
            let (segs, skipped) = load_segments_heap(
                oid, &meta.col_names, &meta.segment_by, &needed_cols,
                &meta.time_column, false, &seg_filters, t_min, t_max,
            );
            all_segments.extend(segs);
            total_skipped += skipped;
        }
        let heap_scan_us = t1.elapsed().as_micros() as u64;

        let compressed_bytes: u64 = all_segments
            .iter()
            .map(|s| s.compressed_blobs.iter().map(|b| b.len() as u64).sum::<u64>())
            .sum();

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
                decompress_us: 0,
                emit_us: 0,
                rows_emitted: 0,
                rows_filtered: 0,
                batch_eval_us: 0,
                rows_batch_filtered: 0,
                segments_decompressed: 0,
                compressed_bytes,
                segments_skipped: total_skipped,
            },
            instrument: None,
            _time_column: meta.time_column,
            segment_by_filters: seg_filters,
            time_min: t_min,
            time_max: t_max,
            batch_quals,
            selection_vector: Vec::new(),
        };

        // Create per-segment memory context
        let query_ctx = (*(*node).ss.ps.state).es_query_cxt;
        state.segment_mcxt = pg_sys::AllocSetContextCreateInternal(
            query_ctx,
            c"SeaTurtleSegment".as_ptr(),
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        );

        // Sort segments by min_time for time-ordered output
        state.segments_data.sort_by_key(|s| s.min_time.unwrap_or(i64::MAX));

        let state_box = Box::new(state);
        let state_ptr = Box::into_raw(state_box);
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}

/// BeginCustomScan callback for SeaTurtleCount: load segment metadata and sum row counts.
#[pg_guard]
pub unsafe extern "C-unwind" fn begin_count_scan(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: i32,
) {
    unsafe {
        let custom_private = (*node).custom_ps;
        if custom_private.is_null() {
            pgrx::error!("pg_seaturtle: missing companion table OIDs in SeaTurtleCount state");
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
            pgrx::error!("pg_seaturtle: SeaTurtleCount has no companion tables");
        }

        // Get first companion table name for metadata
        let first_name = {
            let name_ptr = pg_sys::get_rel_name(companion_oids[0]);
            if name_ptr.is_null() {
                pgrx::error!(
                    "pg_seaturtle: companion table not found for OID {}",
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

        // Build needed_cols as all-false (no columns needed for COUNT(*))
        let num_cols = meta.col_names.len();
        let needed_cols = vec![false; num_cols];

        // Load segments from all companion tables and sum row counts
        let t1 = Instant::now();
        let mut total_count: i64 = 0;
        let mut total_segments: u64 = 0;
        for &oid in &companion_oids {
            let (segs, _) = load_segments_heap(
                oid,
                &meta.col_names,
                &meta.segment_by,
                &needed_cols,
                &meta.time_column,
                false,
                &[],
                None,
                None,
            );
            for seg in &segs {
                total_count += seg.row_count as i64;
            }
            total_segments += segs.len() as u64;
        }
        let heap_scan_us = t1.elapsed().as_micros() as u64;

        let state = CountScanState {
            total_count,
            returned: false,
            metadata_us,
            heap_scan_us,
            total_segments,
        };

        let state_box = Box::new(state);
        let state_ptr = Box::into_raw(state_box);
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}

/// ExecCustomScan callback for SeaTurtleCount: return one row with the count.
#[pg_guard]
pub unsafe extern "C-unwind" fn exec_count_scan(
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

/// EndCustomScan callback for SeaTurtleCount: cleanup state.
#[pg_guard]
pub unsafe extern "C-unwind" fn end_count_scan(
    node: *mut pg_sys::CustomScanState,
) {
    unsafe {
        let state_ptr = (*node).custom_ps as *mut CountScanState;
        if !state_ptr.is_null() {
            let state = Box::from_raw(state_ptr);
            let total_us = state.metadata_us + state.heap_scan_us;
            pgrx::log!(
                "pg_seaturtle SeaTurtleCount timing: total={:.1}ms  metadata={:.1}ms  heap_scan={:.1}ms  | \
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

/// ReScanCustomScan callback for SeaTurtleCount: reset returned flag.
#[pg_guard]
pub unsafe extern "C-unwind" fn rescan_count_scan(
    node: *mut pg_sys::CustomScanState,
) {
    unsafe {
        let state = &mut *((*node).custom_ps as *mut CountScanState);
        state.returned = false;
    }
}

/// CreateCustomScanState callback for SeaTurtleMinMax.
#[pg_guard]
pub unsafe extern "C-unwind" fn create_minmax_scan_state(
    cscan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    unsafe {
        let css = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScanState>())
            as *mut pg_sys::CustomScanState;

        (*css).ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
        (*css).methods = &SEATURTLE_MINMAX_EXEC_METHODS.0;

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

/// BeginCustomScan callback for SeaTurtleMinMax: load segment metadata and find global min/max.
#[pg_guard]
pub unsafe extern "C-unwind" fn begin_minmax_scan(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: i32,
) {
    unsafe {
        let custom_private = (*node).custom_ps;
        if custom_private.is_null() {
            pgrx::error!("pg_seaturtle: missing companion table OIDs in SeaTurtleMinMax state");
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
            pgrx::error!("pg_seaturtle: SeaTurtleMinMax has no companion tables");
        }

        // Get first companion table name for metadata
        let first_name = {
            let name_ptr = pg_sys::get_rel_name(companion_oids[0]);
            if name_ptr.is_null() {
                pgrx::error!(
                    "pg_seaturtle: companion table not found for OID {}",
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
                    pgrx::error!("pg_seaturtle: SeaTurtleMinMax varattno {} out of range", spec.varattno);
                }
            })
            .collect();

        // Build needed_cols as all-false (no columns needed for MIN/MAX metadata)
        let num_cols = meta.col_names.len();
        let needed_cols = vec![false; num_cols];

        // Load segments from all companion tables and find global min/max per aggregate
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
            let (segs, _) = load_segments_heap(
                oid,
                &meta.col_names,
                &meta.segment_by,
                &needed_cols,
                &meta.time_column,
                true,
                &[],
                None,
                None,
            );
            for seg in &segs {
                for (agg_idx, result) in results.iter_mut().enumerate() {
                    if let Some(cm) = seg.col_minmax.get(&agg_col_names[agg_idx]) {
                        let seg_datum = if result.is_min { cm.min_datum } else { cm.max_datum };
                        let seg_null = if result.is_min { cm.min_null } else { cm.max_null };

                        if seg_null {
                            continue;
                        }

                        // Update type_oid from companion metadata
                        if result.type_oid == pg_sys::InvalidOid {
                            result.type_oid = cm.type_oid;
                        }

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

        let state = MinMaxScanState {
            results,
            returned: false,
            metadata_us,
            heap_scan_us,
            total_segments,
        };

        let state_box = Box::new(state);
        let state_ptr = Box::into_raw(state_box);
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}

/// ExecCustomScan callback for SeaTurtleMinMax: return one row with N min/max values.
#[pg_guard]
pub unsafe extern "C-unwind" fn exec_minmax_scan(
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

/// EndCustomScan callback for SeaTurtleMinMax: cleanup state.
#[pg_guard]
pub unsafe extern "C-unwind" fn end_minmax_scan(
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
                "pg_seaturtle SeaTurtleMinMax timing: total={:.1}ms  metadata={:.1}ms  heap_scan={:.1}ms  | \
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

/// ReScanCustomScan callback for SeaTurtleMinMax: reset returned flag.
#[pg_guard]
pub unsafe extern "C-unwind" fn rescan_minmax_scan(
    node: *mut pg_sys::CustomScanState,
) {
    unsafe {
        let state = &mut *((*node).custom_ps as *mut MinMaxScanState);
        state.returned = false;
    }
}

// ============================================================================
// SeaTurtleAgg execution callbacks
// ============================================================================

/// CreateCustomScanState callback for SeaTurtleAgg.
#[pg_guard]
pub unsafe extern "C-unwind" fn create_agg_scan_state(
    cscan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    unsafe {
        let css = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScanState>())
            as *mut pg_sys::CustomScanState;

        (*css).ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
        (*css).methods = &SEATURTLE_AGG_EXEC_METHODS.0;
        (*css).custom_ps = (*cscan).custom_private;

        css as *mut pg_sys::Node
    }
}

/// Output mapping entry: which internal data to put at this slot position.
#[derive(Debug, Clone, Copy)]
enum OutputEntry {
    Agg(usize),    // index into agg_specs
    Group(usize),  // index into group_specs
}

/// BeginCustomScan callback for SeaTurtleAgg: decompress and aggregate.
#[pg_guard]
pub unsafe extern "C-unwind" fn begin_agg_scan(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: i32,
) {
    unsafe {
        let custom_private = (*node).custom_ps;
        if custom_private.is_null() {
            pgrx::error!("pg_seaturtle: missing custom_private in SeaTurtleAgg state");
        }

        let list_len = (*custom_private).length;

        // Parse custom_private:
        // [oid1, ..., -1, num_aggs, agg_spec_fields...,
        //  num_groups, group_spec_fields...,
        //  num_output, output_type_0, output_ref_0, ...]
        let mut companion_oids: Vec<pg_sys::Oid> = Vec::new();
        let mut agg_specs: Vec<AggExecSpec> = Vec::new();
        let mut group_specs: Vec<GroupByColSpec> = Vec::new();
        let mut output_map: Vec<OutputEntry> = Vec::new();

        let mut idx = 0;
        // Parse OIDs until sentinel
        while idx < list_len {
            let val = pg_sys::list_nth_int(custom_private, idx);
            idx += 1;
            if val == -1 { break; }
            companion_oids.push(pg_sys::Oid::from(val as u32));
        }
        // Parse agg specs
        if idx < list_len {
            let num_aggs = pg_sys::list_nth_int(custom_private, idx) as usize;
            idx += 1;
            for _ in 0..num_aggs {
                let agg_type_val = pg_sys::list_nth_int(custom_private, idx);
                let col_idx = pg_sys::list_nth_int(custom_private, idx + 1);
                let result_oid = pg_sys::list_nth_int(custom_private, idx + 2) as u32;
                let col_type_oid = pg_sys::list_nth_int(custom_private, idx + 3) as u32;
                idx += 4;
                let agg_type = match agg_type_val {
                    0 => AggType::Sum,
                    1 => AggType::Count,
                    2 => AggType::CountStar,
                    3 => AggType::Avg,
                    4 => AggType::CountDistinct,
                    _ => AggType::Count,
                };
                let _ = result_oid; // parsed for offset, not stored
                agg_specs.push(AggExecSpec {
                    agg_type,
                    col_idx,
                    col_type_oid: pg_sys::Oid::from(col_type_oid),
                });
            }
        }
        // Parse group specs
        if idx < list_len {
            let num_groups = pg_sys::list_nth_int(custom_private, idx) as usize;
            idx += 1;
            for _ in 0..num_groups {
                let col_idx = pg_sys::list_nth_int(custom_private, idx);
                let type_oid = pg_sys::list_nth_int(custom_private, idx + 1) as u32;
                idx += 2;
                group_specs.push(GroupByColSpec {
                    col_idx,
                    type_oid: pg_sys::Oid::from(type_oid),
                });
            }
        }
        // Parse output mapping
        if idx < list_len {
            let num_output = pg_sys::list_nth_int(custom_private, idx) as usize;
            idx += 1;
            for _ in 0..num_output {
                let otype = pg_sys::list_nth_int(custom_private, idx);
                let oref = pg_sys::list_nth_int(custom_private, idx + 1) as usize;
                idx += 2;
                output_map.push(if otype == 0 {
                    OutputEntry::Agg(oref)
                } else {
                    OutputEntry::Group(oref)
                });
            }
        }
        // If no output mapping (backward compat), default to aggs then groups
        if output_map.is_empty() {
            for i in 0..agg_specs.len() {
                output_map.push(OutputEntry::Agg(i));
            }
            for i in 0..group_specs.len() {
                output_map.push(OutputEntry::Group(i));
            }
        }

        let _ = idx;

        if companion_oids.is_empty() {
            pgrx::error!("pg_seaturtle: SeaTurtleAgg has no companion tables");
        }

        // Get first companion table name for metadata
        let first_name = {
            let name_ptr = pg_sys::get_rel_name(companion_oids[0]);
            if name_ptr.is_null() {
                pgrx::error!(
                    "pg_seaturtle: companion table not found for OID {}",
                    u32::from(companion_oids[0])
                );
            }
            std::ffi::CStr::from_ptr(name_ptr)
                .to_string_lossy()
                .into_owned()
        };

        // Load metadata via SPI
        let t0 = Instant::now();
        let meta = Spi::connect(|client| load_metadata(client, &first_name));
        let metadata_us = t0.elapsed().as_micros() as u64;

        // Build needed_cols: only columns referenced by aggregates and group-by
        let num_cols = meta.col_names.len();
        let mut needed_cols = vec![false; num_cols];
        for spec in &agg_specs {
            if spec.col_idx >= 0 && (spec.col_idx as usize) < num_cols {
                needed_cols[spec.col_idx as usize] = true;
            }
        }
        for gs in &group_specs {
            if gs.col_idx >= 0 && (gs.col_idx as usize) < num_cols {
                needed_cols[gs.col_idx as usize] = true;
            }
        }

        // Take WHERE quals from thread-local (set during planning)
        let where_quals = super::path::take_agg_plan_quals();

        // Extract batch quals and segment filters from WHERE clause
        let batch_quals = extract_batch_quals(where_quals, &meta.col_names, &meta.col_types);
        for bq in &batch_quals {
            if bq.col_idx < num_cols {
                needed_cols[bq.col_idx] = true;
            }
        }
        let (seg_filters, time_min, time_max) = extract_segment_filters(
            where_quals,
            &meta.col_names,
            &meta.segment_by,
            &meta.time_column,
        );
        // Load segments from all companion tables (with lazy pruning)
        let t1 = Instant::now();
        let mut all_segments: Vec<SegmentData> = Vec::new();
        for &oid in &companion_oids {
            let (segs, _) = load_segments_heap(
                oid, &meta.col_names, &meta.segment_by, &needed_cols,
                &meta.time_column, false, &seg_filters, time_min, time_max,
            );
            all_segments.extend(segs);
        }
        let heap_scan_us = t1.elapsed().as_micros() as u64;

        // Create per-segment memory context
        let query_ctx = (*(*node).ss.ps.state).es_query_cxt;
        let segment_mcxt = pg_sys::AllocSetContextCreateInternal(
            query_ctx,
            c"SeaTurtleAggSegment".as_ptr(),
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        );

        // Initialize accumulators
        let has_group_by = !group_specs.is_empty();
        let num_result_cols = output_map.len();

        let prototype_accumulators: Vec<AggAccumulator> = agg_specs
            .iter()
            .map(|spec| AggAccumulator::new_for(spec.agg_type, spec.col_type_oid))
            .collect();

        let mut global_accumulators = if !has_group_by {
            Some(prototype_accumulators.iter().map(|a| a.clone_fresh()).collect::<Vec<_>>())
        } else {
            None
        };
        let mut group_map: HashMap<Vec<GroupKeyVal>, Vec<AggAccumulator>> = HashMap::new();

        let t2 = Instant::now();
        let mut total_segments: u64 = 0;
        let mut total_rows_processed: u64 = 0;
        let mut decompress_us: u64 = 0;

        for seg in &all_segments {
            if seg.row_count == 0 {
                continue;
            }

            // Segment-by pruning
            if !seg_filters.is_empty() {
                let mut skip = false;
                for &(seg_val_idx, ref filter_val) in &seg_filters {
                    match &seg.segment_values[seg_val_idx] {
                        Some(val) if val == filter_val => {}
                        _ => { skip = true; break; }
                    }
                }
                if skip { continue; }
            }

            // Time-range pruning
            if let (Some(seg_min), Some(seg_max)) = (seg.min_time, seg.max_time) {
                if time_min.is_some_and(|query_min| seg_max < query_min) { continue; }
                if time_max.is_some_and(|query_max| seg_min > query_max) { continue; }
            }

            total_segments += 1;

            // Decompress needed columns
            let t_dec = Instant::now();
            pg_sys::MemoryContextReset(segment_mcxt);
            let old_ctx = pg_sys::MemoryContextSwitchTo(segment_mcxt);

            let mut decompressed: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
            let mut blob_idx = 0;
            let mut seg_val_idx = 0;
            let mut pre_selection: Vec<bool> = Vec::new();

            for (col_idx, col_name) in meta.col_names.iter().enumerate() {
                let type_oid = meta.col_types[col_idx];

                if !needed_cols[col_idx] {
                    if meta.segment_by.contains(col_name) {
                        seg_val_idx += 1;
                    } else {
                        blob_idx += 1;
                    }
                    decompressed.push(Vec::new());
                    continue;
                }

                if meta.segment_by.contains(col_name) {
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
                    let typmod = meta.col_typmods[col_idx];

                    // Check if this text column has a LIKE batch qual
                    let like_qual = batch_quals.iter().find(|bq| {
                        bq.col_idx == col_idx
                            && matches!(bq.op, BatchCompareOp::Like | BatchCompareOp::NotLike)
                    });

                    if let Some(bq) = like_qual {
                        let strat = bq.like_strategy.as_ref().unwrap();
                        let neg = bq.op == BatchCompareOp::NotLike;
                        let (datums, like_sel) =
                            decompress_text_blob_with_like_filter(blob, type_oid, typmod, strat, neg);
                        decompressed.push(datums);
                        // AND the like_sel into pre_selection
                        if pre_selection.is_empty() {
                            pre_selection = like_sel;
                        } else {
                            for (ps, ls) in pre_selection.iter_mut().zip(like_sel.iter()) {
                                *ps = *ps && *ls;
                            }
                        }
                    } else {
                        let type_name = pg_type_name(type_oid);
                        let datums = decompress_blob_to_datums(blob, &type_name, type_oid, typmod);
                        decompressed.push(datums);
                    }
                    blob_idx += 1;
                }
            }

            pg_sys::MemoryContextSwitchTo(old_ctx);
            decompress_us += t_dec.elapsed().as_micros() as u64;

            let row_count = seg.row_count as usize;

            // Evaluate batch quals (WHERE) if any.
            // pre_selection seeds the selection vector so that rows already
            // filtered by LIKE during decompression are skipped (their dummy
            // datums are never dereferenced).
            let selection = evaluate_batch_quals(&decompressed, row_count, &batch_quals, pre_selection);

            // Aggregate loop
            for row in 0..row_count {
                if !selection.is_empty() && !selection[row] {
                    continue;
                }

                total_rows_processed += 1;

                let accumulators = if has_group_by {
                    let mut key = Vec::with_capacity(group_specs.len());
                    for gs in &group_specs {
                        let col = &decompressed[gs.col_idx as usize];
                        if col.is_empty() || col[row].1 {
                            key.push(GroupKeyVal::Null);
                        } else {
                            let datum = col[row].0;
                            match gs.type_oid {
                                pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID => {
                                    let cstr = pg_sys::text_to_cstring(datum.cast_mut_ptr());
                                    let s = std::ffi::CStr::from_ptr(cstr).to_string_lossy().into_owned();
                                    pg_sys::pfree(cstr as *mut _);
                                    key.push(GroupKeyVal::Str(s));
                                }
                                _ => {
                                    key.push(GroupKeyVal::Int(datum.value() as i64));
                                }
                            }
                        }
                    }
                    group_map.entry(key).or_insert_with(|| {
                        prototype_accumulators.iter().map(|a| a.clone_fresh()).collect()
                    })
                } else {
                    global_accumulators.as_mut().unwrap()
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
                            if !col.is_empty() && !col[row].1
                                && let AggAccumulator::Count { count } = acc
                            {
                                *count += 1;
                            }
                        }
                        AggType::Sum | AggType::Avg => {
                            let col = &decompressed[spec.col_idx as usize];
                            if !col.is_empty() && !col[row].1 {
                                let datum = col[row].0;
                                match acc {
                                    AggAccumulator::SumInt { sum, count } => {
                                        let v = datum_to_i128(datum, spec.col_type_oid);
                                        *sum += v;
                                        *count += 1;
                                    }
                                    AggAccumulator::SumFloat { sum, count } => {
                                        let v = datum_to_f64(datum, spec.col_type_oid);
                                        *sum += v;
                                        *count += 1;
                                    }
                                    _ => {}
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
                                        let s = std::ffi::CStr::from_ptr(cstr).to_string_lossy().into_owned();
                                        pg_sys::pfree(cstr as *mut _);
                                        seen.insert(s);
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }

        let agg_us = t2.elapsed().as_micros() as u64 - decompress_us;

        // Finalize results using output mapping
        let result_rows = if has_group_by {
            let mut rows = Vec::new();
            // Pre-finalize all agg results keyed by group
            for (key, accumulators) in &group_map {
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
                        OutputEntry::Group(gi) => {
                            match &key[*gi] {
                                GroupKeyVal::Null => {
                                    row.push((pg_sys::Datum::from(0usize), true));
                                }
                                GroupKeyVal::Int(v) => {
                                    row.push((pg_sys::Datum::from(*v as usize), false));
                                }
                                GroupKeyVal::Str(s) => {
                                    let datum = string_to_datum(s, group_specs[*gi].type_oid);
                                    row.push((datum, false));
                                }
                            }
                        }
                    }
                }
                rows.push(row);
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
                    OutputEntry::Group(_) => {
                        row.push((pg_sys::Datum::from(0usize), true));
                    }
                }
            }
            vec![row]
        } else {
            vec![]
        };

        // Clean up segment memory context
        if !segment_mcxt.is_null() {
            pg_sys::MemoryContextDelete(segment_mcxt);
        }

        let state = AggScanState {
            _agg_specs: agg_specs,
            _group_specs: group_specs,
            result_rows,
            result_idx: 0,
            _num_result_cols: num_result_cols,
            metadata_us,
            heap_scan_us,
            decompress_us,
            agg_us,
            total_segments,
            total_rows_processed,
        };

        let state_box = Box::new(state);
        let state_ptr = Box::into_raw(state_box);
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}

/// Group key value for HashMap key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum GroupKeyVal {
    Null,
    Int(i64),
    Str(String),
}

/// Convert a datum to i128 for SUM accumulation.
fn datum_to_i128(datum: pg_sys::Datum, type_oid: pg_sys::Oid) -> i128 {
    match type_oid {
        pg_sys::INT2OID => (datum.value() as i16) as i128,
        pg_sys::INT4OID => (datum.value() as i32) as i128,
        pg_sys::INT8OID => (datum.value() as i64) as i128,
        _ => datum.value() as i128,
    }
}

/// Convert a datum to f64 for float SUM/AVG.
fn datum_to_f64(datum: pg_sys::Datum, type_oid: pg_sys::Oid) -> f64 {
    match type_oid {
        pg_sys::FLOAT4OID => f32::from_bits(datum.value() as u32) as f64,
        pg_sys::FLOAT8OID => f64::from_bits(datum.value() as u64),
        _ => datum.value() as f64,
    }
}

/// Finalize an accumulator into a (Datum, is_null) result pair.
unsafe fn finalize_accumulator(acc: &AggAccumulator, spec: &AggExecSpec) -> (pg_sys::Datum, bool) {
    unsafe {
        match acc {
            AggAccumulator::SumInt { sum, count } => {
                if *count == 0 {
                    return (pg_sys::Datum::from(0usize), true);
                }
                match spec.agg_type {
                    AggType::Sum => {
                        // SUM(int2/int4) → INT8, SUM(int8) → NUMERIC
                        if spec.col_type_oid == pg_sys::INT8OID {
                            // Result is NUMERIC — convert via OidFunctionCall
                            let i64_val = *sum as i64;
                            let datum = pg_sys::OidFunctionCall1Coll(
                                pg_sys::Oid::from(1781u32),  // int8_numeric OID
                                pg_sys::InvalidOid,
                                pg_sys::Datum::from(i64_val as usize),
                            );
                            (datum, false)
                        } else {
                            // Result is INT8
                            (pg_sys::Datum::from(*sum as i64 as usize), false)
                        }
                    }
                    AggType::Avg => {
                        // AVG(int*) → NUMERIC
                        let avg_f64 = *sum as f64 / *count as f64;
                        let datum = pg_sys::OidFunctionCall1Coll(
                            pg_sys::Oid::from(1743u32),  // float8_numeric OID
                            pg_sys::InvalidOid,
                            pg_sys::Datum::from(avg_f64.to_bits() as usize),
                        );
                        (datum, false)
                    }
                    _ => (pg_sys::Datum::from(*sum as i64 as usize), false),
                }
            }
            AggAccumulator::SumFloat { sum, count } => {
                if *count == 0 {
                    return (pg_sys::Datum::from(0usize), true);
                }
                match spec.agg_type {
                    AggType::Sum => {
                        // SUM(float4) → FLOAT4, SUM(float8) → FLOAT8
                        if spec.col_type_oid == pg_sys::FLOAT4OID {
                            let f4 = *sum as f32;
                            (pg_sys::Datum::from(f4.to_bits() as usize), false)
                        } else {
                            (pg_sys::Datum::from(sum.to_bits() as usize), false)
                        }
                    }
                    AggType::Avg => {
                        // AVG(float*) → FLOAT8
                        let avg = *sum / *count as f64;
                        (pg_sys::Datum::from(avg.to_bits() as usize), false)
                    }
                    _ => (pg_sys::Datum::from(sum.to_bits() as usize), false),
                }
            }
            AggAccumulator::Count { count } => {
                (pg_sys::Datum::from(*count as usize), false)
            }
            AggAccumulator::CountDistinctInt { seen } => {
                (pg_sys::Datum::from(seen.len()), false)
            }
            AggAccumulator::CountDistinctStr { seen } => {
                (pg_sys::Datum::from(seen.len()), false)
            }
        }
    }
}

/// ExecCustomScan callback for SeaTurtleAgg: return result rows.
#[pg_guard]
pub unsafe extern "C-unwind" fn exec_agg_scan(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    unsafe {
        let scan_slot = (*node).ss.ss_ScanTupleSlot;
        let state = &mut *((*node).custom_ps as *mut AggScanState);

        if state.result_idx < state.result_rows.len() {
            pg_sys::ExecClearTuple(scan_slot);
            let row = &state.result_rows[state.result_idx];
            for (i, &(datum, is_null)) in row.iter().enumerate() {
                (*scan_slot).tts_values.add(i).write(datum);
                (*scan_slot).tts_isnull.add(i).write(is_null);
            }
            pg_sys::ExecStoreVirtualTuple(scan_slot);
            state.result_idx += 1;
            return scan_slot;
        }

        // EOF
        pg_sys::ExecClearTuple(scan_slot);
        scan_slot
    }
}

/// EndCustomScan callback for SeaTurtleAgg.
#[pg_guard]
pub unsafe extern "C-unwind" fn end_agg_scan(
    node: *mut pg_sys::CustomScanState,
) {
    unsafe {
        let state_ptr = (*node).custom_ps as *mut AggScanState;
        if !state_ptr.is_null() {
            let state = Box::from_raw(state_ptr);
            let total_us = state.metadata_us + state.heap_scan_us + state.decompress_us + state.agg_us;
            pgrx::log!(
                "pg_seaturtle SeaTurtleAgg timing: total={:.1}ms  metadata={:.1}ms  heap_scan={:.1}ms  \
                 decompress={:.1}ms  agg={:.1}ms  | \
                 segments={} rows_processed={} result_rows={}",
                total_us as f64 / 1000.0,
                state.metadata_us as f64 / 1000.0,
                state.heap_scan_us as f64 / 1000.0,
                state.decompress_us as f64 / 1000.0,
                state.agg_us as f64 / 1000.0,
                state.total_segments,
                state.total_rows_processed,
                state.result_rows.len(),
            );
            (*node).custom_ps = std::ptr::null_mut();
        }
    }
}

/// ReScanCustomScan callback for SeaTurtleAgg.
#[pg_guard]
pub unsafe extern "C-unwind" fn rescan_agg_scan(
    node: *mut pg_sys::CustomScanState,
) {
    unsafe {
        let state = &mut *((*node).custom_ps as *mut AggScanState);
        state.result_idx = 0;
    }
}

/// Metadata returned by the SPI metadata query.
struct MetadataInfo {
    col_names: Vec<String>,
    col_types: Vec<pg_sys::Oid>,
    col_typmods: Vec<i32>,
    segment_by: Vec<String>,
    time_column: String,
}

/// Load metadata (column names, types, segment_by) from catalog via SPI.
fn load_metadata(
    client: &pgrx::spi::SpiClient<'_>,
    companion_name: &str,
) -> MetadataInfo {
    // Get the partition's hypertable info
    let mut ht_result = client
        .select(
            "SELECT h.segment_by, h.order_by, h.time_column, h.schema_name, h.table_name
             FROM seaturtle_partition p
             JOIN seaturtle_hypertable h ON h.id = p.hypertable_id
             WHERE p.table_name = $1 AND p.is_compressed = true",
            None,
            &[companion_name.into()],
        )
        .expect("failed to query partition info");

    let ht_row = ht_result.next().unwrap_or_else(|| {
        pgrx::error!(
            "pg_seaturtle: no compressed partition info found for {}",
            companion_name
        );
    });

    let segment_by: Vec<String> = ht_row
        .get_datum_by_ordinal(1)
        .unwrap()
        .value::<Vec<String>>()
        .unwrap()
        .unwrap_or_default();
    let time_column: String = ht_row
        .get_datum_by_ordinal(3)
        .unwrap()
        .value::<String>()
        .unwrap()
        .unwrap();
    let parent_schema: String = ht_row
        .get_datum_by_ordinal(4)
        .unwrap()
        .value::<String>()
        .unwrap()
        .unwrap();
    let parent_table: String = ht_row
        .get_datum_by_ordinal(5)
        .unwrap()
        .value::<String>()
        .unwrap()
        .unwrap();

    // Get column info from the parent table (pg_attribute gives us atttypmod)
    let col_result = client
        .select(
            "SELECT a.attname::text, t.typname::text, a.atttypmod
             FROM pg_attribute a
             JOIN pg_type t ON a.atttypid = t.oid
             JOIN pg_class c ON a.attrelid = c.oid
             JOIN pg_namespace n ON c.relnamespace = n.oid
             WHERE n.nspname = $1 AND c.relname = $2
               AND a.attnum > 0 AND NOT a.attisdropped
             ORDER BY a.attnum",
            None,
            &[parent_schema.as_str().into(), parent_table.as_str().into()],
        )
        .expect("failed to get column info");

    let mut col_names = Vec::new();
    let mut col_type_names = Vec::new();
    let mut col_typmods = Vec::new();
    for row in col_result {
        let name: String = row.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap().unwrap();
        let type_name: String = row.get_datum_by_ordinal(2).unwrap().value::<String>().unwrap().unwrap();
        let typmod: i32 = row.get_datum_by_ordinal(3).unwrap().value::<i32>().unwrap().unwrap_or(-1);
        col_names.push(name);
        col_type_names.push(type_name);
        col_typmods.push(typmod);
    }

    let col_types: Vec<pg_sys::Oid> = col_type_names.iter().map(|tn| pg_type_oid(tn)).collect();

    MetadataInfo {
        col_names,
        col_types,
        col_typmods,
        segment_by,
        time_column,
    }
}

/// Load segment data from the companion table via direct heap scan.
///
/// Bypasses SPI entirely — opens the companion table, iterates all tuples
/// with `heap_getnext`, and extracts segment_by values, compressed BYTEA blobs,
/// and row counts directly from the heap tuples.
#[allow(clippy::too_many_arguments)]
unsafe fn load_segments_heap(
    companion_oid: pg_sys::Oid,
    col_names: &[String],
    segment_by: &[String],
    needed_cols: &[bool],
    time_column: &str,
    load_minmax: bool,
    segment_by_filters: &[(usize, String)],
    time_min: Option<i64>,
    time_max: Option<i64>,
) -> (Vec<SegmentData>, u64) {
    unsafe {
        // Open companion table with AccessShareLock
        let rel = pg_sys::table_open(companion_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        let tupdesc = (*rel).rd_att;
        let natts = (*tupdesc).natts as usize;

        // Build column-name-to-attno mapping from companion TupleDesc
        let mut attno_map: HashMap<String, usize> = HashMap::new();
        let mut att_type_oids: HashMap<String, pg_sys::Oid> = HashMap::new();
        for i in 0..natts {
            let att = &*tupdesc_get_attr(tupdesc, i);
            if att.attisdropped {
                continue;
            }
            let name = std::ffi::CStr::from_ptr(att.attname.data.as_ptr())
                .to_string_lossy()
                .into_owned();
            att_type_oids.insert(name.clone(), att.atttypid);
            attno_map.insert(name, i);
        }

        // Locate attribute indices for segment_by columns, compressed columns, and _row_count
        let mut segment_by_attnos: Vec<(usize, pg_sys::Oid)> = Vec::new(); // (attno, type_oid)
        for name in col_names {
            if segment_by.contains(name)
                && let Some(&attno) = attno_map.get(name.as_str())
            {
                let type_oid = att_type_oids[name.as_str()];
                segment_by_attnos.push((attno, type_oid));
            }
        }

        let mut compressed_attnos: Vec<Option<usize>> = Vec::new(); // Some(attno) for needed, None for unneeded
        for (idx, name) in col_names.iter().enumerate() {
            if !segment_by.contains(name) {
                if needed_cols[idx] {
                    let comp_name = format!("_{}_compressed", name);
                    compressed_attnos.push(attno_map.get(comp_name.as_str()).copied());
                } else {
                    compressed_attnos.push(None);
                }
            }
        }

        let row_count_attno = attno_map.get("_row_count").copied();

        let min_time_name = format!("_min_{}", time_column);
        let max_time_name = format!("_max_{}", time_column);
        let min_time_attno = attno_map.get(min_time_name.as_str()).copied();
        let max_time_attno = attno_map.get(max_time_name.as_str()).copied();

        // Discover per-column min/max columns: (col_name, min_attno, max_attno, type_oid)
        // Only needed for MinMax pushdown scans — skip for regular decompress scans
        // to avoid overhead from deforming 100+ extra attributes.
        let mut minmax_col_attnos: Vec<(String, usize, usize, pg_sys::Oid)> = Vec::new();
        if load_minmax {
            for col_name in col_names {
                if segment_by.contains(col_name) {
                    continue;
                }
                let min_name = format!("_min_{}", col_name);
                let max_name = format!("_max_{}", col_name);
                if let (Some(&min_att), Some(&max_att)) = (
                    attno_map.get(min_name.as_str()),
                    attno_map.get(max_name.as_str()),
                ) {
                    let type_oid = att_type_oids.get(min_name.as_str()).copied()
                        .unwrap_or(pg_sys::InvalidOid);
                    minmax_col_attnos.push((col_name.clone(), min_att, max_att, type_oid));
                }
            }
        }

        // Begin table scan via TableAmRoutine vtable
        // (table_beginscan is static inline in C, so we call via the vtable)
        let snapshot = pg_sys::GetActiveSnapshot();
        let flags: u32 = pg_sys::ScanOptions::SO_TYPE_SEQSCAN
            | pg_sys::ScanOptions::SO_ALLOW_STRAT
            | pg_sys::ScanOptions::SO_ALLOW_SYNC
            | pg_sys::ScanOptions::SO_ALLOW_PAGEMODE;
        let scan = (*(*rel).rd_tableam).scan_begin.unwrap()(
            rel,
            snapshot,
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            flags,
        );

        // Iterate all tuples
        let mut segments = Vec::new();
        let mut segments_skipped: u64 = 0;
        let mut values = vec![pg_sys::Datum::from(0); natts];
        let mut nulls = vec![true; natts];

        loop {
            let tuple = pg_sys::heap_getnext(
                scan,
                pg_sys::ScanDirection::ForwardScanDirection,
            );
            if tuple.is_null() {
                break;
            }

            // Deform tuple into datums + nulls arrays
            pg_sys::heap_deform_tuple(
                tuple,
                tupdesc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            );

            // Extract segment_by values
            let mut segment_values: Vec<Option<String>> = Vec::new();
            for &(attno, type_oid) in &segment_by_attnos {
                if !nulls[attno] {
                    let mut typoutput: pg_sys::Oid = pg_sys::InvalidOid;
                    let mut typisvarlena: bool = false;
                    pg_sys::getTypeOutputInfo(type_oid, &mut typoutput, &mut typisvarlena);
                    let cstr = pg_sys::OidOutputFunctionCall(typoutput, values[attno]);
                    let s = std::ffi::CStr::from_ptr(cstr)
                        .to_string_lossy()
                        .into_owned();
                    pg_sys::pfree(cstr as *mut _);
                    segment_values.push(Some(s));
                } else {
                    segment_values.push(None);
                }
            }

            // Extract _row_count (INT4) — cheap, needed before pruning
            let row_count = match row_count_attno {
                Some(attno) if !nulls[attno] => values[attno].value() as i32,
                _ => 0,
            };

            // Extract min/max time (TIMESTAMPTZ stored as i64 PG epoch microseconds) — cheap
            let seg_min_time = match min_time_attno {
                Some(attno) if !nulls[attno] => Some(values[attno].value() as i64),
                _ => None,
            };
            let seg_max_time = match max_time_attno {
                Some(attno) if !nulls[attno] => Some(values[attno].value() as i64),
                _ => None,
            };

            // --- Lazy pruning: skip blob detoasting for segments that fail filters ---

            // Check segment_by filters
            if !segment_by_filters.is_empty() {
                let mut skip = false;
                for &(seg_val_idx, ref filter_val) in segment_by_filters {
                    match &segment_values.get(seg_val_idx).and_then(|v| v.as_ref()) {
                        Some(val) if *val == filter_val => {}
                        _ => { skip = true; break; }
                    }
                }
                if skip {
                    segments_skipped += 1;
                    continue;
                }
            }

            // Check time range filters
            if let (Some(s_min), Some(s_max)) = (seg_min_time, seg_max_time)
                && (time_min.is_some_and(|qmin| s_max < qmin)
                    || time_max.is_some_and(|qmax| s_min > qmax))
            {
                segments_skipped += 1;
                continue;
            }

            // --- Segment passed pruning: detoast blobs ---

            // Extract compressed BYTEA blobs
            let mut compressed_blobs: Vec<Vec<u8>> = Vec::new();
            for opt_attno in &compressed_attnos {
                match opt_attno {
                    Some(attno) => {
                        let attno = *attno;
                        if !nulls[attno] {
                            let varlena_ptr: *mut pg_sys::varlena =
                                values[attno].cast_mut_ptr();
                            let detoasted = pg_sys::pg_detoast_datum(varlena_ptr);
                            let len = pgrx::varsize_any_exhdr(detoasted);
                            let data = pgrx::vardata_any(detoasted);
                            let bytes = std::slice::from_raw_parts(
                                data,
                                len,
                            )
                            .to_vec();
                            if detoasted != varlena_ptr {
                                pg_sys::pfree(detoasted as *mut _);
                            }
                            compressed_blobs.push(bytes);
                        } else {
                            compressed_blobs.push(Vec::new());
                        }
                    }
                    None => {
                        // Unneeded column — empty placeholder to keep blob_idx mapping
                        compressed_blobs.push(Vec::new());
                    }
                }
            }

            // Extract per-column min/max
            let mut col_minmax = HashMap::new();
            for (col_name, min_att, max_att, type_oid) in &minmax_col_attnos {
                col_minmax.insert(col_name.clone(), ColMinMax {
                    min_datum: if nulls[*min_att] { pg_sys::Datum::from(0usize) } else { values[*min_att] },
                    max_datum: if nulls[*max_att] { pg_sys::Datum::from(0usize) } else { values[*max_att] },
                    min_null: nulls[*min_att],
                    max_null: nulls[*max_att],
                    type_oid: *type_oid,
                });
            }

            segments.push(SegmentData {
                segment_values,
                compressed_blobs,
                row_count,
                min_time: seg_min_time,
                max_time: seg_max_time,
                col_minmax,
            });
        }

        // End scan + close relation
        (*(*rel).rd_tableam).scan_end.unwrap()(scan);
        pg_sys::table_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

        (segments, segments_skipped)
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
) -> DecompressState {
    // Phase 1: SPI for metadata only (small, fast)
    let t0 = Instant::now();
    let meta = Spi::connect(|client| load_metadata(client, companion_name));
    let metadata_us = t0.elapsed().as_micros() as u64;

    // Extract batch quals early — we need to know which extra columns to load
    let batch_quals = unsafe { extract_batch_quals(plan_qual, &meta.col_names, &meta.col_types) };

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

    // Extract segment pruning filters BEFORE heap scan for lazy detoasting
    let (seg_filters, t_min, t_max) = unsafe {
        extract_segment_filters(plan_qual, &meta.col_names, &meta.segment_by, &meta.time_column)
    };

    // Phase 2: Direct heap scan for segment data (bypasses SPI overhead)
    let t1 = Instant::now();
    let (segments_data, segments_skipped) = unsafe {
        load_segments_heap(
            companion_oid, &meta.col_names, &meta.segment_by, &needed_cols,
            &meta.time_column, false, &seg_filters, t_min, t_max,
        )
    };
    let heap_scan_us = t1.elapsed().as_micros() as u64;

    let compressed_bytes: u64 = segments_data
        .iter()
        .map(|s| s.compressed_blobs.iter().map(|b| b.len() as u64).sum::<u64>())
        .sum();

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
            decompress_us: 0,
            emit_us: 0,
            rows_emitted: 0,
            rows_filtered: 0,
            batch_eval_us: 0,
            rows_batch_filtered: 0,
            segments_decompressed: 0,
            compressed_bytes,
            segments_skipped,
        },
        instrument: None,
        _time_column: meta.time_column,
        segment_by_filters: seg_filters,
        time_min: t_min,
        time_max: t_max,
        batch_quals,
        selection_vector: Vec::new(),
    }
}

/// Extract segment pruning filters from the plan qual (raw expression tree).
///
/// Walks OpExpr nodes looking for:
/// - Equality filters on segment_by columns (e.g. `CounterID = 62`)
/// - Range filters on the time column (e.g. `ts >= '2023-01-01'`)
///
/// Returns (segment_by_filters, time_min, time_max).
unsafe fn extract_segment_filters(
    qual_list: *mut pg_sys::List,
    col_names: &[String],
    segment_by: &[String],
    time_column: &str,
) -> (Vec<(usize, String)>, Option<i64>, Option<i64>) {
    let mut segment_by_filters: Vec<(usize, String)> = Vec::new();
    let mut time_min: Option<i64> = None;
    let mut time_max: Option<i64> = None;

    if qual_list.is_null() {
        return (segment_by_filters, time_min, time_max);
    }

    unsafe {
        // Build segment_by column name -> segment_values index mapping
        let mut seg_val_index_map: HashMap<&str, usize> = HashMap::new();
        let mut seg_val_idx = 0;
        for name in col_names {
            if segment_by.contains(name) {
                seg_val_index_map.insert(name.as_str(), seg_val_idx);
                seg_val_idx += 1;
            }
        }

        let nquals = (*qual_list).length;
        for i in 0..nquals {
            let cell = (*qual_list).elements.add(i as usize);
            let node = (*cell).ptr_value as *const pg_sys::Node;
            if node.is_null() {
                continue;
            }

            let tag = (*node).type_;
            if tag != pg_sys::NodeTag::T_OpExpr {
                continue;
            }

            let opexpr = node as *const pg_sys::OpExpr;
            let args = (*opexpr).args;
            if args.is_null() || (*args).length != 2 {
                continue;
            }

            // Get operator name
            let opname_ptr = pg_sys::get_opname((*opexpr).opno);
            if opname_ptr.is_null() {
                continue;
            }
            let opname = std::ffi::CStr::from_ptr(opname_ptr)
                .to_str()
                .unwrap_or("");

            // Get the two args
            let arg0 = (*(*args).elements.add(0)).ptr_value as *const pg_sys::Node;
            let arg1 = (*(*args).elements.add(1)).ptr_value as *const pg_sys::Node;
            if arg0.is_null() || arg1.is_null() {
                continue;
            }

            // Identify Var and Const (handle both orderings)
            let (var_node, const_node, var_on_left) =
                if (*arg0).type_ == pg_sys::NodeTag::T_Var
                    && (*arg1).type_ == pg_sys::NodeTag::T_Const
                {
                    (arg0 as *const pg_sys::Var, arg1 as *const pg_sys::Const, true)
                } else if (*arg0).type_ == pg_sys::NodeTag::T_Const
                    && (*arg1).type_ == pg_sys::NodeTag::T_Var
                {
                    (arg1 as *const pg_sys::Var, arg0 as *const pg_sys::Const, false)
                } else {
                    continue;
                };

            if (*const_node).constisnull {
                continue;
            }

            // Convert 1-based varattno to 0-based column index
            let varattno = (*var_node).varattno as i32;
            if varattno < 1 || varattno as usize > col_names.len() {
                continue;
            }
            let col_idx = (varattno - 1) as usize;
            let col_name = &col_names[col_idx];

            // Check if this is a segment_by equality filter
            if opname == "="
                && let Some(&sv_idx) = seg_val_index_map.get(col_name.as_str())
            {
                // Extract const value as string (matches how segment_values are stored)
                let mut typoutput: pg_sys::Oid = pg_sys::InvalidOid;
                let mut typisvarlena: bool = false;
                pg_sys::getTypeOutputInfo(
                    (*const_node).consttype,
                    &mut typoutput,
                    &mut typisvarlena,
                );
                let cstr = pg_sys::OidOutputFunctionCall(typoutput, (*const_node).constvalue);
                let s = std::ffi::CStr::from_ptr(cstr)
                    .to_string_lossy()
                    .into_owned();
                pg_sys::pfree(cstr as *mut _);
                segment_by_filters.push((sv_idx, s));
            }

            // Check if this is a time column range filter
            if col_name == time_column {
                let ts_val = (*const_node).constvalue.value() as i64;

                // Normalize operator direction (if Var is on right, flip the operator)
                let effective_op = if var_on_left {
                    opname
                } else {
                    match opname {
                        ">=" => "<=",
                        ">" => "<",
                        "<=" => ">=",
                        "<" => ">",
                        _ => opname,
                    }
                };

                match effective_op {
                    ">=" | ">" => {
                        // Lower bound: take the maximum of all lower bounds
                        time_min = Some(match time_min {
                            Some(existing) => existing.max(ts_val),
                            None => ts_val,
                        });
                    }
                    "<=" | "<" => {
                        // Upper bound: take the minimum of all upper bounds
                        time_max = Some(match time_max {
                            Some(existing) => existing.min(ts_val),
                            None => ts_val,
                        });
                    }
                    _ => {}
                }
            }
        }
    }

    (segment_by_filters, time_min, time_max)
}

// ============================================================================
// Batch / vectorized qual evaluation
// ============================================================================

/// Returns true for pass-by-value types that we can compare directly on datums.
fn is_batch_comparable_type(type_oid: pg_sys::Oid) -> bool {
    matches!(
        type_oid,
        pg_sys::INT2OID
            | pg_sys::INT4OID
            | pg_sys::INT8OID
            | pg_sys::FLOAT4OID
            | pg_sys::FLOAT8OID
            | pg_sys::BOOLOID
            | pg_sys::DATEOID
            | pg_sys::TIMESTAMPOID
            | pg_sys::TIMESTAMPTZOID
    )
}

/// Flip a comparison operator for `Const op Var` → `Var op Const` rewriting.
fn flip_compare_op(op: BatchCompareOp) -> BatchCompareOp {
    match op {
        BatchCompareOp::Eq => BatchCompareOp::Eq,
        BatchCompareOp::Ne => BatchCompareOp::Ne,
        BatchCompareOp::Lt => BatchCompareOp::Gt,
        BatchCompareOp::Le => BatchCompareOp::Ge,
        BatchCompareOp::Gt => BatchCompareOp::Lt,
        BatchCompareOp::Ge => BatchCompareOp::Le,
        BatchCompareOp::Like => BatchCompareOp::Like,
        BatchCompareOp::NotLike => BatchCompareOp::NotLike,
    }
}

/// Parse an operator name to a BatchCompareOp.
fn parse_compare_op(opname: &str) -> Option<BatchCompareOp> {
    match opname {
        "=" => Some(BatchCompareOp::Eq),
        "<>" | "!=" => Some(BatchCompareOp::Ne),
        "<" => Some(BatchCompareOp::Lt),
        "<=" => Some(BatchCompareOp::Le),
        ">" => Some(BatchCompareOp::Gt),
        ">=" => Some(BatchCompareOp::Ge),
        _ => None,
    }
}

// Monomorphized batch filter functions.  Each ANDs the comparison result
// into the selection vector so that multiple quals compose correctly.

fn apply_batch_filter_i64(
    col: &[(pg_sys::Datum, bool)],
    sel: &mut [bool],
    op: BatchCompareOp,
    constant: i64,
) {
    for (i, &(datum, is_null)) in col.iter().enumerate() {
        if !sel[i] { continue; }
        if is_null { sel[i] = false; continue; }
        let v = datum.value() as i64;
        sel[i] = match op {
            BatchCompareOp::Eq => v == constant,
            BatchCompareOp::Ne => v != constant,
            BatchCompareOp::Lt => v < constant,
            BatchCompareOp::Le => v <= constant,
            BatchCompareOp::Gt => v > constant,
            BatchCompareOp::Ge => v >= constant,
            BatchCompareOp::Like | BatchCompareOp::NotLike => unreachable!(),
        };
    }
}

fn apply_batch_filter_i32(
    col: &[(pg_sys::Datum, bool)],
    sel: &mut [bool],
    op: BatchCompareOp,
    constant: i32,
) {
    for (i, &(datum, is_null)) in col.iter().enumerate() {
        if !sel[i] { continue; }
        if is_null { sel[i] = false; continue; }
        let v = datum.value() as i32;
        sel[i] = match op {
            BatchCompareOp::Eq => v == constant,
            BatchCompareOp::Ne => v != constant,
            BatchCompareOp::Lt => v < constant,
            BatchCompareOp::Le => v <= constant,
            BatchCompareOp::Gt => v > constant,
            BatchCompareOp::Ge => v >= constant,
            BatchCompareOp::Like | BatchCompareOp::NotLike => unreachable!(),
        };
    }
}

fn apply_batch_filter_i16(
    col: &[(pg_sys::Datum, bool)],
    sel: &mut [bool],
    op: BatchCompareOp,
    constant: i16,
) {
    for (i, &(datum, is_null)) in col.iter().enumerate() {
        if !sel[i] { continue; }
        if is_null { sel[i] = false; continue; }
        let v = datum.value() as i16;
        sel[i] = match op {
            BatchCompareOp::Eq => v == constant,
            BatchCompareOp::Ne => v != constant,
            BatchCompareOp::Lt => v < constant,
            BatchCompareOp::Le => v <= constant,
            BatchCompareOp::Gt => v > constant,
            BatchCompareOp::Ge => v >= constant,
            BatchCompareOp::Like | BatchCompareOp::NotLike => unreachable!(),
        };
    }
}

fn apply_batch_filter_f64(
    col: &[(pg_sys::Datum, bool)],
    sel: &mut [bool],
    op: BatchCompareOp,
    constant: f64,
) {
    for (i, &(datum, is_null)) in col.iter().enumerate() {
        if !sel[i] { continue; }
        if is_null { sel[i] = false; continue; }
        let v = f64::from_bits(datum.value() as u64);
        sel[i] = match op {
            BatchCompareOp::Eq => v == constant,
            BatchCompareOp::Ne => v != constant,
            BatchCompareOp::Lt => v < constant,
            BatchCompareOp::Le => v <= constant,
            BatchCompareOp::Gt => v > constant,
            BatchCompareOp::Ge => v >= constant,
            BatchCompareOp::Like | BatchCompareOp::NotLike => unreachable!(),
        };
    }
}

fn apply_batch_filter_f32(
    col: &[(pg_sys::Datum, bool)],
    sel: &mut [bool],
    op: BatchCompareOp,
    constant: f32,
) {
    for (i, &(datum, is_null)) in col.iter().enumerate() {
        if !sel[i] { continue; }
        if is_null { sel[i] = false; continue; }
        let v = f32::from_bits(datum.value() as u32);
        sel[i] = match op {
            BatchCompareOp::Eq => v == constant,
            BatchCompareOp::Ne => v != constant,
            BatchCompareOp::Lt => v < constant,
            BatchCompareOp::Le => v <= constant,
            BatchCompareOp::Gt => v > constant,
            BatchCompareOp::Ge => v >= constant,
            BatchCompareOp::Like | BatchCompareOp::NotLike => unreachable!(),
        };
    }
}

fn apply_batch_filter_bool(
    col: &[(pg_sys::Datum, bool)],
    sel: &mut [bool],
    op: BatchCompareOp,
    constant: bool,
) {
    for (i, &(datum, is_null)) in col.iter().enumerate() {
        if !sel[i] { continue; }
        if is_null { sel[i] = false; continue; }
        let v = datum.value() != 0;
        sel[i] = match op {
            BatchCompareOp::Eq => v == constant,
            BatchCompareOp::Ne => v != constant,
            BatchCompareOp::Like | BatchCompareOp::NotLike => unreachable!(),
            _ => v == constant, // bool only supports = / <>
        };
    }
}

fn compile_like_pattern(pattern: &str) -> LikeStrategy {
    // If pattern contains _ or backslash escape, use general matcher
    if pattern.contains('_') || pattern.contains('\\') {
        return LikeStrategy::General(pattern.to_string());
    }
    // Count % occurrences and their positions
    let percent_positions: Vec<usize> = pattern.match_indices('%').map(|(i, _)| i).collect();
    match percent_positions.len() {
        0 => LikeStrategy::Exact(pattern.to_string()),
        1 => {
            let pos = percent_positions[0];
            if pos == 0 && pattern.len() == 1 {
                // Just "%" — matches everything
                LikeStrategy::Contains(String::new())
            } else if pos == 0 {
                LikeStrategy::EndsWith(pattern[1..].to_string())
            } else if pos == pattern.len() - 1 {
                LikeStrategy::StartsWith(pattern[..pos].to_string())
            } else {
                LikeStrategy::General(pattern.to_string())
            }
        }
        2 => {
            let first = percent_positions[0];
            let second = percent_positions[1];
            if first == 0 && second == pattern.len() - 1 {
                LikeStrategy::Contains(pattern[1..second].to_string())
            } else {
                LikeStrategy::General(pattern.to_string())
            }
        }
        _ => LikeStrategy::General(pattern.to_string()),
    }
}

fn sql_like_match(text: &str, pattern: &str) -> bool {
    let t = text.as_bytes();
    let p = pattern.as_bytes();
    sql_like_match_inner(t, p)
}

fn sql_like_match_inner(text: &[u8], pattern: &[u8]) -> bool {
    let mut ti = 0;
    let mut pi = 0;
    let mut star_pi = usize::MAX;
    let mut star_ti = 0;

    while ti < text.len() {
        if pi < pattern.len() && pattern[pi] == b'\\' {
            // Escaped character: match literally
            pi += 1;
            if pi < pattern.len() && text[ti] == pattern[pi] {
                ti += 1;
                pi += 1;
                continue;
            }
            // No match after escape
            if star_pi != usize::MAX {
                pi = star_pi + 1;
                star_ti += 1;
                ti = star_ti;
                continue;
            }
            return false;
        }
        if pi < pattern.len() && pattern[pi] == b'_' {
            // _ matches any single character
            ti += 1;
            pi += 1;
            continue;
        }
        if pi < pattern.len() && pattern[pi] == b'%' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
            continue;
        }
        if pi < pattern.len() && text[ti] == pattern[pi] {
            ti += 1;
            pi += 1;
            continue;
        }
        if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
            continue;
        }
        return false;
    }
    // Consume trailing %
    while pi < pattern.len() && pattern[pi] == b'%' {
        pi += 1;
    }
    pi == pattern.len()
}

unsafe fn apply_batch_filter_like(
    col: &[(pg_sys::Datum, bool)],
    sel: &mut [bool],
    strategy: &LikeStrategy,
    negate: bool,
) {
    for (i, &(datum, is_null)) in col.iter().enumerate() {
        if !sel[i] { continue; }
        if is_null { sel[i] = false; continue; }
        let varlena_ptr = datum.cast_mut_ptr::<pg_sys::varlena>();
        let len = unsafe { pgrx::varsize_any_exhdr(varlena_ptr) };
        let data = unsafe { pgrx::vardata_any(varlena_ptr) };
        let text = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(data, len)) };
        let matched = match strategy {
            LikeStrategy::Contains(s) => text.contains(s.as_str()),
            LikeStrategy::StartsWith(s) => text.starts_with(s.as_str()),
            LikeStrategy::EndsWith(s) => text.ends_with(s.as_str()),
            LikeStrategy::Exact(s) => text == s.as_str(),
            LikeStrategy::General(p) => sql_like_match(text, p),
        };
        sel[i] = if negate { !matched } else { matched };
    }
}

/// Evaluate all batch quals against the current decompressed segment.
/// Returns a selection vector (one bool per row). Empty vec means "no batch quals".
fn evaluate_batch_quals(
    current_segment: &[Vec<(pg_sys::Datum, bool)>],
    row_count: usize,
    batch_quals: &[BatchQual],
    pre_selection: Vec<bool>,
) -> Vec<bool> {
    if batch_quals.is_empty() && pre_selection.is_empty() {
        return Vec::new();
    }

    let mut sel = if pre_selection.is_empty() {
        vec![true; row_count]
    } else {
        pre_selection
    };

    for bq in batch_quals {
        let col = &current_segment[bq.col_idx];
        if col.is_empty() {
            // Column wasn't decompressed (not needed) — can't evaluate, skip
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
            pg_sys::BOOLOID => {
                let c = bq.const_datum.value() != 0;
                apply_batch_filter_bool(col, &mut sel, bq.op, c);
            }
            pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID => {
                if let Some(ref strategy) = bq.like_strategy {
                    let negate = bq.op == BatchCompareOp::NotLike;
                    unsafe { apply_batch_filter_like(col, &mut sel, strategy, negate); }
                }
            }
            _ => {} // unsupported type, skip
        }
    }

    sel
}

/// Extract batch quals from the plan qual list.
///
/// Looks for `OpExpr` nodes with `Var op Const` (or `Const op Var`) where the
/// operator is a simple comparison and the column type is pass-by-value.
unsafe fn extract_batch_quals(
    qual_list: *mut pg_sys::List,
    col_names: &[String],
    col_types: &[pg_sys::Oid],
) -> Vec<BatchQual> {
    let mut batch_quals = Vec::new();

    if qual_list.is_null() {
        return batch_quals;
    }

    unsafe {
        let nquals = (*qual_list).length;
        for i in 0..nquals {
            let cell = (*qual_list).elements.add(i as usize);
            let node = (*cell).ptr_value as *const pg_sys::Node;
            if node.is_null() {
                continue;
            }

            let tag = (*node).type_;
            if tag != pg_sys::NodeTag::T_OpExpr {
                continue;
            }

            let opexpr = node as *const pg_sys::OpExpr;
            let args = (*opexpr).args;
            if args.is_null() || (*args).length != 2 {
                continue;
            }

            // Get operator name
            let opname_ptr = pg_sys::get_opname((*opexpr).opno);
            if opname_ptr.is_null() {
                continue;
            }
            let opname = std::ffi::CStr::from_ptr(opname_ptr)
                .to_str()
                .unwrap_or("");

            // Recognize LIKE/NOT LIKE operators before comparison ops
            let is_like = opname == "~~";
            let is_not_like = opname == "!~~";

            let cmp_op = if is_like {
                BatchCompareOp::Like
            } else if is_not_like {
                BatchCompareOp::NotLike
            } else {
                match parse_compare_op(opname) {
                    Some(op) => op,
                    None => {
                        continue;
                    }
                }
            };

            let raw_arg0 = (*(*args).elements.add(0)).ptr_value as *const pg_sys::Node;
            let raw_arg1 = (*(*args).elements.add(1)).ptr_value as *const pg_sys::Node;
            if raw_arg0.is_null() || raw_arg1.is_null() {
                continue;
            }

            // Unwrap RelabelType (PG adds these for int2→int4 coercions etc.)
            let unwrap_relabel = |n: *const pg_sys::Node| -> *const pg_sys::Node {
                if (*n).type_ == pg_sys::NodeTag::T_RelabelType {
                    let rlt = n as *const pg_sys::RelabelType;
                    (*rlt).arg as *const pg_sys::Node
                } else {
                    n
                }
            };
            let arg0 = unwrap_relabel(raw_arg0);
            let arg1 = unwrap_relabel(raw_arg1);

            let arg0_tag = (*arg0).type_;
            let arg1_tag = (*arg1).type_;

            let (var_node, const_node, var_on_left) =
                if arg0_tag == pg_sys::NodeTag::T_Var
                    && arg1_tag == pg_sys::NodeTag::T_Const
                {
                    (arg0 as *const pg_sys::Var, arg1 as *const pg_sys::Const, true)
                } else if arg0_tag == pg_sys::NodeTag::T_Const
                    && arg1_tag == pg_sys::NodeTag::T_Var
                {
                    (arg1 as *const pg_sys::Var, arg0 as *const pg_sys::Const, false)
                } else {
                    continue;
                };

            if (*const_node).constisnull {
                continue;
            }

            let varattno = (*var_node).varattno as i32;
            if varattno < 1 || varattno as usize > col_names.len() {
                continue;
            }
            let col_idx = (varattno - 1) as usize;
            let type_oid = col_types[col_idx];

            if is_like || is_not_like {
                // LIKE is not symmetric: column must be on the left
                if !var_on_left {
                    continue;
                }
                // Only text-like types
                if !matches!(type_oid, pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID) {
                    continue;
                }
                // Extract pattern string from constant datum
                let varlena_ptr = (*const_node).constvalue.cast_mut_ptr::<pg_sys::varlena>();
                let len = pgrx::varsize_any_exhdr(varlena_ptr);
                let data = pgrx::vardata_any(varlena_ptr);
                let pattern_bytes = std::slice::from_raw_parts(data, len);
                let pattern = match std::str::from_utf8(pattern_bytes) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let strategy = compile_like_pattern(pattern);
                batch_quals.push(BatchQual {
                    col_idx,
                    op: cmp_op,
                    const_datum: (*const_node).constvalue,
                    type_oid,
                    like_strategy: Some(strategy),
                    text_const: None,
                });
            } else if matches!(type_oid, pg_sys::TEXTOID | pg_sys::VARCHAROID)
                && matches!(cmp_op, BatchCompareOp::Eq | BatchCompareOp::Ne)
            {
                // Text equality/inequality: extract the constant string for
                // dictionary-based pushdown during decompression.
                if !var_on_left {
                    continue;
                }
                let varlena_ptr = (*const_node).constvalue.cast_mut_ptr::<pg_sys::varlena>();
                let len = pgrx::varsize_any_exhdr(varlena_ptr);
                let data = pgrx::vardata_any(varlena_ptr);
                let const_bytes = std::slice::from_raw_parts(data, len);
                let const_str = match std::str::from_utf8(const_bytes) {
                    Ok(s) => s.to_string(),
                    Err(_) => continue,
                };

                batch_quals.push(BatchQual {
                    col_idx,
                    op: cmp_op,
                    const_datum: (*const_node).constvalue,
                    type_oid,
                    like_strategy: None,
                    text_const: Some(const_str),
                });
            } else {
                if !is_batch_comparable_type(type_oid) {
                    continue;
                }

                let op = if var_on_left { cmp_op } else { flip_compare_op(cmp_op) };

                batch_quals.push(BatchQual {
                    col_idx,
                    op,
                    const_datum: (*const_node).constvalue,
                    type_oid,
                    like_strategy: None,
                    text_const: None,
                });
            }
        }
    }

    batch_quals
}

/// ExecCustomScan callback: return the next tuple.
///
/// PostgreSQL's ExecCustomScan wrapper does NOT apply qualification or
/// projection — the custom scan provider is responsible for both.
#[pg_guard]
pub unsafe extern "C-unwind" fn exec_custom_scan(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    unsafe {
        let scan_slot = (*node).ss.ss_ScanTupleSlot;
        let state = &mut *((*node).custom_ps as *mut DecompressState);
        let econtext = (*node).ss.ps.ps_ExprContext;
        let qual = (*node).ss.ps.qual;
        let proj_info = (*node).ss.ps.ps_ProjInfo;

        let instrument = *state.instrument.get_or_insert_with(|| {
            !(*node).ss.ps.instrument.is_null()
        });
        let t_call = if instrument { Some(Instant::now()) } else { None };

        loop {
            // If current segment has more rows, try the next one
            if !state.current_segment.is_empty() {
                let seg_rows = state.current_row_count;

                // Batch filter: advance row_cursor to the next passing row.
                // Uses slice .position() which LLVM can auto-vectorize (SIMD)
                // to scan 16-32 bytes at a time instead of per-byte branching.
                if !state.selection_vector.is_empty() {
                    let start = state.row_cursor;
                    let end = seg_rows;
                    if let Some(offset) = state.selection_vector[start..end]
                        .iter()
                        .position(|&v| v)
                    {
                        state.timing.rows_batch_filtered += offset as u64;
                        state.row_cursor = start + offset;
                    } else {
                        // All remaining rows fail — skip to end of segment
                        state.timing.rows_batch_filtered += (end - start) as u64;
                        state.row_cursor = end;
                    }
                }

                if state.row_cursor < seg_rows {
                    fill_slot(scan_slot, state);
                    state.row_cursor += 1;

                    // Set the scan tuple in the expression context for qual/projection
                    (*econtext).ecxt_scantuple = scan_slot;

                    // Apply qualification (WHERE clauses pushed down to scan)
                    if !qual.is_null() && !exec_qual(qual, econtext) {
                        // Reset per-tuple memory context on filtered rows
                        pg_sys::MemoryContextReset((*econtext).ecxt_per_tuple_memory);
                        state.timing.rows_filtered += 1;
                        continue; // skip this row, try next
                    }

                    // Apply projection if needed
                    let result = if !proj_info.is_null() {
                        exec_project(proj_info)
                    } else {
                        scan_slot
                    };
                    state.timing.rows_emitted += 1;
                    if let Some(t) = t_call {
                        state.timing.emit_us += t.elapsed().as_micros() as u64;
                    }
                    return result;
                }
            }

            // Move to next segment
            if state.segment_index >= state.segments_data.len() {
                pg_sys::ExecClearTuple(scan_slot);
                return scan_slot;
            }

            let seg = &state.segments_data[state.segment_index];
            state.segment_index += 1;

            if seg.row_count == 0 {
                continue;
            }

            // Segment-by pruning: skip if any equality filter doesn't match
            if !state.segment_by_filters.is_empty() {
                let mut skip = false;
                for &(seg_val_idx, ref filter_val) in &state.segment_by_filters {
                    match &seg.segment_values[seg_val_idx] {
                        Some(val) if val == filter_val => {}
                        _ => { skip = true; break; }
                    }
                }
                if skip {
                    state.timing.segments_skipped += 1;
                    continue;
                }
            }

            // Time-range pruning: skip if segment's time range doesn't overlap query range
            if let (Some(seg_min), Some(seg_max)) = (seg.min_time, seg.max_time) {
                if state.time_min.is_some_and(|query_min| seg_max < query_min) {
                    state.timing.segments_skipped += 1;
                    continue;
                }
                if state.time_max.is_some_and(|query_max| seg_min > query_max) {
                    state.timing.segments_skipped += 1;
                    continue;
                }
            }

            let t_decompress = if instrument { Some(Instant::now()) } else { None };

            // Reset segment memory context — frees all varlena from previous segment
            pg_sys::MemoryContextReset(state.segment_mcxt);
            let old_ctx = pg_sys::MemoryContextSwitchTo(state.segment_mcxt);

            // Decompress needed columns directly to datums
            let mut decompressed: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
            let mut blob_idx = 0;
            let mut seg_val_idx = 0;
            let mut pre_selection: Vec<bool> = Vec::new();

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

                    // Check if this text column has a LIKE or Eq/Ne batch qual
                    let like_qual = state.batch_quals.iter().find(|bq| {
                        bq.col_idx == col_idx
                            && matches!(bq.op, BatchCompareOp::Like | BatchCompareOp::NotLike)
                    });
                    let text_eq_qual = state.batch_quals.iter().find(|bq| {
                        bq.col_idx == col_idx
                            && bq.text_const.is_some()
                            && matches!(bq.op, BatchCompareOp::Eq | BatchCompareOp::Ne)
                    });

                    if let Some(bq) = like_qual {
                        let strat = bq.like_strategy.as_ref().unwrap();
                        let neg = bq.op == BatchCompareOp::NotLike;
                        let (datums, like_sel) =
                            decompress_text_blob_with_like_filter(blob, type_oid, typmod, strat, neg);
                        decompressed.push(datums);
                        // AND the like_sel into pre_selection
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
                            blob, type_oid, typmod, const_str, is_ne,
                        );
                        decompressed.push(datums);
                        if pre_selection.is_empty() {
                            pre_selection = eq_sel;
                        } else {
                            for (ps, es) in pre_selection.iter_mut().zip(eq_sel.iter()) {
                                *ps = *ps && *es;
                            }
                        }
                    } else {
                        let type_name = pg_type_name(type_oid);
                        let datums = decompress_blob_to_datums(blob, &type_name, type_oid, typmod);
                        decompressed.push(datums);
                    }
                    blob_idx += 1;
                }
            }

            pg_sys::MemoryContextSwitchTo(old_ctx);

            if let Some(t) = t_decompress {
                state.timing.decompress_us += t.elapsed().as_micros() as u64;
            }
            state.timing.segments_decompressed += 1;

            state.current_segment = decompressed;
            state.current_row_count = seg.row_count as usize;
            state.row_cursor = 0;

            // Evaluate batch quals on the decompressed segment.
            // pre_selection seeds the selection vector so that rows already
            // filtered by LIKE during decompression are skipped.
            if !state.batch_quals.is_empty() || !pre_selection.is_empty() {
                let t_batch = if instrument { Some(Instant::now()) } else { None };
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
        }
    }
}

/// EndCustomScan callback: cleanup and emit timing summary.
#[pg_guard]
pub unsafe extern "C-unwind" fn end_custom_scan(
    node: *mut pg_sys::CustomScanState,
) {
    unsafe {
        let state_ptr = (*node).custom_ps as *mut DecompressState;
        if !state_ptr.is_null() {
            let state = Box::from_raw(state_ptr);

            // Emit timing summary at LOG level (visible with SET client_min_messages = log)
            let t = &state.timing;
            let total_us = t.metadata_us + t.heap_scan_us + t.decompress_us + t.batch_eval_us + t.emit_us;
            pgrx::log!(
                "pg_seaturtle scan timing: total={:.1}ms  metadata={:.1}ms  heap_scan={:.1}ms  \
                 decompress={:.1}ms  batch_eval={:.1}ms  emit={:.1}ms  | \
                 segments={} segments_skipped={} rows_out={} rows_filtered={} rows_batch_filtered={} compressed_bytes={}",
                total_us as f64 / 1000.0,
                t.metadata_us as f64 / 1000.0,
                t.heap_scan_us as f64 / 1000.0,
                t.decompress_us as f64 / 1000.0,
                t.batch_eval_us as f64 / 1000.0,
                t.emit_us as f64 / 1000.0,
                t.segments_decompressed,
                t.segments_skipped,
                t.rows_emitted,
                t.rows_filtered,
                t.rows_batch_filtered,
                t.compressed_bytes,
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
pub unsafe extern "C-unwind" fn rescan_custom_scan(
    node: *mut pg_sys::CustomScanState,
) {
    unsafe {
        let state = &mut *((*node).custom_ps as *mut DecompressState);
        state.segment_index = 0;
        state.row_cursor = 0;
        state.current_row_count = 0;
        state.current_segment.clear();
        state.selection_vector.clear();
    }
}

// ============================================================================
// Inline PG executor helpers (these are static inline in C headers,
// so they are not available via FFI — we re-implement them here).
// ============================================================================

const TTS_FLAG_EMPTY: u16 = 1 << 1;

/// Re-implementation of PostgreSQL's static inline `ExecProject`.
unsafe fn exec_project(proj_info: *mut pg_sys::ProjectionInfo) -> *mut pg_sys::TupleTableSlot {
    unsafe {
        let econtext = (*proj_info).pi_exprContext;
        let state = &mut (*proj_info).pi_state;
        let slot = state.resultslot;

        pg_sys::ExecClearTuple(slot);

        // ExecEvalExprSwitchContext
        let old_ctx = pg_sys::MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);
        let mut isnull = false;
        if let Some(evalfunc) = state.evalfunc {
            evalfunc(state, econtext, &mut isnull);
        }
        pg_sys::MemoryContextSwitchTo(old_ctx);

        // Mark slot as containing a valid virtual tuple (inlined ExecStoreVirtualTuple)
        (*slot).tts_flags &= !TTS_FLAG_EMPTY;
        (*slot).tts_nvalid = (*(*slot).tts_tupleDescriptor).natts as i16;

        slot
    }
}

/// Re-implementation of PostgreSQL's static inline `ExecQual`.
unsafe fn exec_qual(state: *mut pg_sys::ExprState, econtext: *mut pg_sys::ExprContext) -> bool {
    unsafe {
        if state.is_null() {
            return true;
        }

        // ExecEvalExprSwitchContext
        let old_ctx = pg_sys::MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);
        let mut isnull = false;
        let ret = if let Some(evalfunc) = (*state).evalfunc {
            evalfunc(state, econtext, &mut isnull)
        } else {
            pg_sys::Datum::from(0)
        };
        pg_sys::MemoryContextSwitchTo(old_ctx);

        ret != pg_sys::Datum::from(0)
    }
}

// ============================================================================
// TupleDesc attribute access (PG14–17 vs PG18)
// ============================================================================

/// Get a pointer to the i-th `FormData_pg_attribute` from a TupleDesc.
/// PG14–17 store attrs directly; PG18 stores CompactAttribute first, then attrs.
#[cfg(any(
    feature = "pg14",
    feature = "pg15",
    feature = "pg16",
    feature = "pg17"
))]
#[inline]
unsafe fn tupdesc_get_attr(
    tupdesc: pg_sys::TupleDesc,
    i: usize,
) -> *const pg_sys::FormData_pg_attribute {
    unsafe { (*tupdesc).attrs.as_ptr().add(i) }
}

#[cfg(feature = "pg18")]
#[inline]
unsafe fn tupdesc_get_attr(
    tupdesc: pg_sys::TupleDesc,
    i: usize,
) -> *const pg_sys::FormData_pg_attribute {
    unsafe {
        let natts = (*tupdesc).natts as usize;
        let att_pointer = (*tupdesc)
            .compact_attrs
            .as_ptr()
            .add(natts)
            .cast::<pg_sys::FormData_pg_attribute>();
        att_pointer.add(i)
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Fill a TupleTableSlot from pre-computed datums at the current row cursor.
unsafe fn fill_slot(
    slot: *mut pg_sys::TupleTableSlot,
    state: &DecompressState,
) {
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

/// Convert a string to a PostgreSQL Datum using the type's input function.
/// Used only for segment_by values (one per segment, not per row).
fn string_to_datum(s: &str, type_oid: pg_sys::Oid) -> pg_sys::Datum {
    unsafe {
        let cstr = std::ffi::CString::new(s).unwrap();
        let mut typinput: pg_sys::Oid = pg_sys::InvalidOid;
        let mut typioparam: pg_sys::Oid = pg_sys::InvalidOid;
        pg_sys::getTypeInputInfo(type_oid, &mut typinput, &mut typioparam);
        pg_sys::OidInputFunctionCall(typinput, cstr.as_ptr() as *mut _, typioparam, -1)
    }
}

/// Map a PG type name (udt_name) to a type OID.
fn pg_type_oid(type_name: &str) -> pg_sys::Oid {
    match type_name {
        "timestamptz" => pg_sys::TIMESTAMPTZOID,
        "timestamp" => pg_sys::TIMESTAMPOID,
        "float8" => pg_sys::FLOAT8OID,
        "float4" => pg_sys::FLOAT4OID,
        "int2" => pg_sys::INT2OID,
        "int4" => pg_sys::INT4OID,
        "int8" => pg_sys::INT8OID,
        "date" => pg_sys::DATEOID,
        "bpchar" => pg_sys::BPCHAROID,
        "bool" => pg_sys::BOOLOID,
        "text" => pg_sys::TEXTOID,
        "varchar" => pg_sys::VARCHAROID,
        _ => pg_sys::TEXTOID,
    }
}

/// Map a type OID back to a data_type string for codec dispatch.
fn pg_type_name(type_oid: pg_sys::Oid) -> String {
    if type_oid == pg_sys::TIMESTAMPTZOID || type_oid == pg_sys::TIMESTAMPOID {
        "timestamp with time zone".to_string()
    } else if type_oid == pg_sys::FLOAT8OID {
        "double precision".to_string()
    } else if type_oid == pg_sys::FLOAT4OID {
        "real".to_string()
    } else if type_oid == pg_sys::INT2OID {
        "smallint".to_string()
    } else if type_oid == pg_sys::INT4OID {
        "integer".to_string()
    } else if type_oid == pg_sys::INT8OID {
        "bigint".to_string()
    } else if type_oid == pg_sys::DATEOID {
        "date".to_string()
    } else if type_oid == pg_sys::BOOLOID {
        "boolean".to_string()
    } else {
        "text".to_string()
    }
}

// ============================================================================
// Direct datum decompression — bypasses the string round-trip
// ============================================================================

/// Decompress a column blob directly to PostgreSQL Datums.
///
/// For pass-by-value types (int, float, timestamp, date, bool), the decoded
/// value is stored directly in the Datum with zero allocation.
/// For pass-by-reference types (text, varchar, bpchar), a varlena is allocated
/// in the current memory context (caller must set the right context).
unsafe fn decompress_blob_to_datums(
    blob: &[u8],
    data_type: &str,
    type_oid: pg_sys::Oid,
    typmod: i32,
) -> Vec<(pg_sys::Datum, bool)> {
    if blob.is_empty() {
        return Vec::new();
    }

    let cc = CompressedColumnRef::from_bytes(blob);
    let total_count = cc.row_count as usize;
    let non_null_count = count_non_null(cc.null_bitmap, total_count);
    let dt = data_type.to_lowercase();

    let datums: Vec<pg_sys::Datum> = match cc.type_tag {
        CompressionType::Gorilla => {
            if dt.contains("timestamp") || dt == "date" {
                let timestamps =
                    compression::gorilla::decode_timestamps(cc.data, non_null_count);
                if dt == "date" {
                    timestamps
                        .iter()
                        .map(|&usec| {
                            let unix_days = (usec / 86_400_000_000) as i32;
                            let pg_days = unix_days - PG_EPOCH_OFFSET_DAYS;
                            pg_sys::Datum::from(pg_days as usize)
                        })
                        .collect()
                } else {
                    timestamps
                        .iter()
                        .map(|&usec| {
                            let pg_usec = usec - PG_EPOCH_OFFSET_USEC;
                            pg_sys::Datum::from(pg_usec as usize)
                        })
                        .collect()
                }
            } else if dt == "real" || dt.contains("float4") {
                let floats =
                    compression::gorilla::decode_floats_f32(cc.data, non_null_count);
                floats
                    .iter()
                    .map(|&v| pg_sys::Datum::from(v.to_bits() as usize))
                    .collect()
            } else {
                let floats =
                    compression::gorilla::decode_floats(cc.data, non_null_count);
                floats
                    .iter()
                    .map(|&v| pg_sys::Datum::from(v.to_bits() as usize))
                    .collect()
            }
        }
        CompressionType::DeltaVarint => {
            if dt == "integer" || dt.contains("int4") || dt == "smallint" {
                let ints = compression::integer::decode_i32(cc.data, non_null_count);
                if dt == "smallint" {
                    ints.iter()
                        .map(|&v| pg_sys::Datum::from(v as i16 as usize))
                        .collect()
                } else {
                    ints.iter()
                        .map(|&v| pg_sys::Datum::from(v as usize))
                        .collect()
                }
            } else {
                let ints = compression::integer::decode_i64(cc.data, non_null_count);
                ints.iter()
                    .map(|&v| pg_sys::Datum::from(v as usize))
                    .collect()
            }
        }
        CompressionType::Dictionary => {
            let slices = compression::dictionary::decode_to_slices(cc.data, non_null_count);
            unsafe { str_slices_to_text_datums_arena(&slices, type_oid, typmod) }
        }
        CompressionType::Lz4 => {
            let (buf, ranges) = compression::lz4::decode_to_ranges(cc.data, non_null_count);
            let slices: Vec<&str> = ranges
                .iter()
                .map(|&(off, len)| {
                    std::str::from_utf8(&buf[off..off + len])
                        .expect("invalid UTF-8 in LZ4 data")
                })
                .collect();
            unsafe { str_slices_to_text_datums_arena(&slices, type_oid, typmod) }
        }
        CompressionType::BooleanBitmap => {
            let bools = compression::boolean::decode(cc.data, non_null_count);
            bools
                .iter()
                .map(|&b| pg_sys::Datum::from(b as usize))
                .collect()
        }
    };

    reinsert_nulls_datum(&datums, cc.null_bitmap, total_count)
}

/// Decompress a text column blob with LIKE filtering pushed into decompression.
///
/// Instead of allocating a PG varlena datum for every row and then filtering,
/// this matches the LIKE pattern against raw `&str` slices (zero-copy) and only
/// calls `str_to_text_datum()` for rows that match. Non-matching rows get a
/// dummy datum that will never be read (the returned selection vector marks them
/// as filtered out).
///
/// Returns `(datums, like_selection)` where:
/// - `datums`: Full-length datum array with nulls reinserted. Matching rows have
///   real varlena datums; non-matching rows have `(Datum(0), false)`.
/// - `like_selection`: Per-row bool vector (true = matched LIKE).
unsafe fn decompress_text_blob_with_like_filter(
    blob: &[u8],
    type_oid: pg_sys::Oid,
    typmod: i32,
    strategy: &LikeStrategy,
    negate: bool,
) -> (Vec<(pg_sys::Datum, bool)>, Vec<bool>) {
    if blob.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let cc = CompressedColumnRef::from_bytes(blob);
    let total_count = cc.row_count as usize;
    let non_null_count = count_non_null(cc.null_bitmap, total_count);

    // Match a &str against the LikeStrategy, applying negation.
    let matches_like = |text: &str| -> bool {
        let matched = match strategy {
            LikeStrategy::Contains(s) => text.contains(s.as_str()),
            LikeStrategy::StartsWith(s) => text.starts_with(s.as_str()),
            LikeStrategy::EndsWith(s) => text.ends_with(s.as_str()),
            LikeStrategy::Exact(s) => text == s.as_str(),
            LikeStrategy::General(p) => sql_like_match(text, p),
        };
        if negate { !matched } else { matched }
    };

    // Build (non-null datums, non-null selection) — only non-null values
    let (nn_datums, nn_sel): (Vec<pg_sys::Datum>, Vec<bool>) = match cc.type_tag {
        CompressionType::Dictionary => {
            let (dict_entries, indices) =
                compression::dictionary::decode_dict_and_indices(cc.data, non_null_count);

            // Pre-match each dictionary entry (tiny vec, e.g. a few thousand)
            let dict_matches: Vec<bool> = dict_entries.iter().map(|s| matches_like(s)).collect();

            // Collect matched slices for arena allocation
            let sel: Vec<bool> = indices.iter().map(|&idx| dict_matches[idx as usize]).collect();
            let matched_slices: Vec<&str> = indices
                .iter()
                .zip(sel.iter())
                .filter(|&(_, &pass)| pass)
                .map(|(&idx, _)| dict_entries[idx as usize])
                .collect();
            let matched_datums = unsafe {
                str_slices_to_text_datums_arena(&matched_slices, type_oid, typmod)
            };

            // Merge matched datums back with dummy datums for non-matching rows
            let mut datums = Vec::with_capacity(non_null_count);
            let mut match_idx = 0;
            for &pass in &sel {
                if pass {
                    datums.push(matched_datums[match_idx]);
                    match_idx += 1;
                } else {
                    datums.push(pg_sys::Datum::from(0));
                }
            }
            (datums, sel)
        }
        CompressionType::Lz4 => {
            let (buf, ranges) =
                compression::lz4::decode_to_ranges(cc.data, non_null_count);

            // First pass: determine which rows match
            let slices: Vec<&str> = ranges
                .iter()
                .map(|&(off, len)| {
                    std::str::from_utf8(&buf[off..off + len])
                        .expect("invalid UTF-8 in LZ4 data")
                })
                .collect();
            let sel: Vec<bool> = slices.iter().map(|s| matches_like(s)).collect();

            // Collect matched slices for arena allocation
            let matched_slices: Vec<&str> = slices
                .iter()
                .zip(sel.iter())
                .filter(|&(_, &pass)| pass)
                .map(|(&s, _)| s)
                .collect();
            let matched_datums = unsafe {
                str_slices_to_text_datums_arena(&matched_slices, type_oid, typmod)
            };

            // Merge matched datums back
            let mut datums = Vec::with_capacity(non_null_count);
            let mut match_idx = 0;
            for &pass in &sel {
                if pass {
                    datums.push(matched_datums[match_idx]);
                    match_idx += 1;
                } else {
                    datums.push(pg_sys::Datum::from(0));
                }
            }
            (datums, sel)
        }
        _ => {
            // Unexpected compression type for text — fall back to full decompression
            return {
                let full = unsafe { decompress_blob_to_datums(
                    blob,
                    &pg_type_name(type_oid),
                    type_oid,
                    typmod,
                ) };
                let sel = vec![true; full.len()];
                (full, sel)
            };
        }
    };

    // Reinsert nulls into both datums and selection vectors
    let null_bitmap = cc.null_bitmap;
    if null_bitmap.is_empty() {
        // No nulls — pair up directly
        let datums: Vec<(pg_sys::Datum, bool)> = nn_datums.into_iter().map(|d| (d, false)).collect();
        (datums, nn_sel)
    } else {
        let mut datums = Vec::with_capacity(total_count);
        let mut sel = Vec::with_capacity(total_count);
        let mut val_idx = 0;
        for i in 0..total_count {
            let is_null = (null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
            if is_null {
                datums.push((pg_sys::Datum::from(0), true));
                sel.push(false); // NULLs don't match LIKE
            } else {
                datums.push((nn_datums[val_idx], false));
                sel.push(nn_sel[val_idx]);
                val_idx += 1;
            }
        }
        (datums, sel)
    }
}

/// Decompress a text column blob with equality/inequality filtering pushed into decompression.
///
/// Similar to `decompress_text_blob_with_like_filter`, but matches against a constant
/// string using `==` (or `!=` when `is_ne` is true). For dictionary-compressed data,
/// this checks each dictionary entry once and uses the indices to build the selection
/// vector — O(dict_size) comparisons instead of O(row_count).
///
/// Returns `(datums, eq_selection)` where:
/// - `datums`: Full-length datum array with nulls reinserted.
/// - `eq_selection`: Per-row bool vector (true = matched equality/inequality).
unsafe fn decompress_text_blob_with_eq_filter(
    blob: &[u8],
    type_oid: pg_sys::Oid,
    typmod: i32,
    const_str: &str,
    is_ne: bool,
) -> (Vec<(pg_sys::Datum, bool)>, Vec<bool>) {
    if blob.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let cc = CompressedColumnRef::from_bytes(blob);
    let total_count = cc.row_count as usize;
    let non_null_count = count_non_null(cc.null_bitmap, total_count);

    let matches_eq = |text: &str| -> bool {
        let eq = text == const_str;
        if is_ne { !eq } else { eq }
    };

    let (nn_datums, nn_sel): (Vec<pg_sys::Datum>, Vec<bool>) = match cc.type_tag {
        CompressionType::Dictionary => {
            let (dict_entries, indices) =
                compression::dictionary::decode_dict_and_indices(cc.data, non_null_count);

            // Check each dictionary entry once — O(dict_size) instead of O(row_count)
            let dict_matches: Vec<bool> = dict_entries.iter().map(|s| matches_eq(s)).collect();

            let sel: Vec<bool> = indices.iter().map(|&idx| dict_matches[idx as usize]).collect();
            let matched_slices: Vec<&str> = indices
                .iter()
                .zip(sel.iter())
                .filter(|&(_, &pass)| pass)
                .map(|(&idx, _)| dict_entries[idx as usize])
                .collect();
            let matched_datums = unsafe {
                str_slices_to_text_datums_arena(&matched_slices, type_oid, typmod)
            };

            let mut datums = Vec::with_capacity(non_null_count);
            let mut match_idx = 0;
            for &pass in &sel {
                if pass {
                    datums.push(matched_datums[match_idx]);
                    match_idx += 1;
                } else {
                    datums.push(pg_sys::Datum::from(0));
                }
            }
            (datums, sel)
        }
        CompressionType::Lz4 => {
            let (buf, ranges) =
                compression::lz4::decode_to_ranges(cc.data, non_null_count);

            let slices: Vec<&str> = ranges
                .iter()
                .map(|&(off, len)| {
                    std::str::from_utf8(&buf[off..off + len])
                        .expect("invalid UTF-8 in LZ4 data")
                })
                .collect();
            let sel: Vec<bool> = slices.iter().map(|s| matches_eq(s)).collect();

            let matched_slices: Vec<&str> = slices
                .iter()
                .zip(sel.iter())
                .filter(|&(_, &pass)| pass)
                .map(|(&s, _)| s)
                .collect();
            let matched_datums = unsafe {
                str_slices_to_text_datums_arena(&matched_slices, type_oid, typmod)
            };

            let mut datums = Vec::with_capacity(non_null_count);
            let mut match_idx = 0;
            for &pass in &sel {
                if pass {
                    datums.push(matched_datums[match_idx]);
                    match_idx += 1;
                } else {
                    datums.push(pg_sys::Datum::from(0));
                }
            }
            (datums, sel)
        }
        _ => {
            // Unexpected compression type — fall back to full decompression
            return {
                let full = unsafe { decompress_blob_to_datums(
                    blob,
                    &pg_type_name(type_oid),
                    type_oid,
                    typmod,
                ) };
                let sel = vec![true; full.len()];
                (full, sel)
            };
        }
    };

    // Reinsert nulls into both datums and selection vectors
    let null_bitmap = cc.null_bitmap;
    if null_bitmap.is_empty() {
        let datums: Vec<(pg_sys::Datum, bool)> = nn_datums.into_iter().map(|d| (d, false)).collect();
        (datums, nn_sel)
    } else {
        let mut datums = Vec::with_capacity(total_count);
        let mut sel = Vec::with_capacity(total_count);
        let mut val_idx = 0;
        for i in 0..total_count {
            let is_null = (null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
            if is_null {
                datums.push((pg_sys::Datum::from(0), true));
                sel.push(false); // NULLs don't match equality
            } else {
                datums.push((nn_datums[val_idx], false));
                sel.push(nn_sel[val_idx]);
                val_idx += 1;
            }
        }
        (datums, sel)
    }
}

/// Create a text/varchar/bpchar datum from a Rust string.
/// Allocates in the current memory context.
unsafe fn str_to_text_datum(s: &str, type_oid: pg_sys::Oid, typmod: i32) -> pg_sys::Datum {
    unsafe {
        if type_oid == pg_sys::BPCHAROID {
            // bpchar needs the type input function with the correct typmod for padding
            let cstr = std::ffi::CString::new(s).unwrap();
            let mut typinput: pg_sys::Oid = pg_sys::InvalidOid;
            let mut typioparam: pg_sys::Oid = pg_sys::InvalidOid;
            pg_sys::getTypeInputInfo(type_oid, &mut typinput, &mut typioparam);
            pg_sys::OidInputFunctionCall(typinput, cstr.as_ptr() as *mut _, typioparam, typmod)
        } else {
            // text/varchar: direct varlena construction (avoids type input function lookup)
            let text = pg_sys::cstring_to_text_with_len(s.as_ptr() as *const _, s.len() as i32);
            pg_sys::Datum::from(text as usize)
        }
    }
}

/// Allocate text/varchar datums from string slices using a single contiguous allocation.
///
/// Instead of N individual palloc calls (one per string), this allocates one
/// large block and packs all varlena headers + string data sequentially.
/// This dramatically improves cache locality during the per-row emit loop.
///
/// For bpchar, falls back to per-string allocation (needs type input function for padding).
unsafe fn str_slices_to_text_datums_arena(
    slices: &[&str],
    type_oid: pg_sys::Oid,
    typmod: i32,
) -> Vec<pg_sys::Datum> {
    if slices.is_empty() {
        return Vec::new();
    }

    // bpchar needs the type input function for padding — can't arena-allocate
    if type_oid == pg_sys::BPCHAROID {
        return unsafe {
            slices
                .iter()
                .map(|s| str_to_text_datum(s, type_oid, typmod))
                .collect()
        };
    }

    unsafe {
        const VARHDRSZ: usize = pg_sys::VARHDRSZ;
        const MAXALIGN: usize = 8; // 64-bit alignment

        // Calculate total arena size
        let total_size: usize = slices
            .iter()
            .map(|s| {
                let varlena_size = VARHDRSZ + s.len();
                // Align each varlena to MAXALIGN for safe pointer access
                (varlena_size + MAXALIGN - 1) & !(MAXALIGN - 1)
            })
            .sum();

        let arena = pg_sys::palloc(total_size) as *mut u8;
        let mut datums = Vec::with_capacity(slices.len());
        let mut offset = 0;

        for s in slices {
            let varlena_ptr = arena.add(offset) as *mut pg_sys::varlena;
            let total_len = (VARHDRSZ + s.len()) as i32;
            pgrx::set_varsize_4b(varlena_ptr, total_len);
            std::ptr::copy_nonoverlapping(
                s.as_ptr(),
                (varlena_ptr as *mut u8).add(VARHDRSZ),
                s.len(),
            );
            datums.push(pg_sys::Datum::from(varlena_ptr as usize));
            offset += ((total_len as usize) + MAXALIGN - 1) & !(MAXALIGN - 1);
        }

        datums
    }
}

/// Reinsert nulls into a datum vector using the null bitmap.
fn reinsert_nulls_datum(
    datums: &[pg_sys::Datum],
    null_bitmap: &[u8],
    total_count: usize,
) -> Vec<(pg_sys::Datum, bool)> {
    if null_bitmap.is_empty() {
        // Fast path: no nulls — direct copy with pre-allocated Vec
        let mut result = Vec::with_capacity(total_count);
        for &d in datums {
            result.push((d, false));
        }
        return result;
    }
    let mut result = Vec::with_capacity(total_count);
    let mut val_idx = 0;
    for i in 0..total_count {
        let is_null = (null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
        if is_null {
            result.push((pg_sys::Datum::from(0), true));
        } else {
            result.push((datums[val_idx], false));
            val_idx += 1;
        }
    }
    result
}

/// Compare two Datums of the same type. Returns Ordering for min/max computation.
/// Only supports pass-by-value orderable types (int, float, date, timestamp).
fn compare_datums(d1: pg_sys::Datum, d2: pg_sys::Datum, type_oid: pg_sys::Oid) -> std::cmp::Ordering {
    if type_oid == pg_sys::TIMESTAMPTZOID || type_oid == pg_sys::TIMESTAMPOID || type_oid == pg_sys::INT8OID {
        (d1.value() as i64).cmp(&(d2.value() as i64))
    } else if type_oid == pg_sys::DATEOID || type_oid == pg_sys::INT4OID {
        (d1.value() as i32).cmp(&(d2.value() as i32))
    } else if type_oid == pg_sys::INT2OID {
        (d1.value() as i16).cmp(&(d2.value() as i16))
    } else if type_oid == pg_sys::FLOAT8OID {
        let f1 = f64::from_bits(d1.value() as u64);
        let f2 = f64::from_bits(d2.value() as u64);
        f1.partial_cmp(&f2).unwrap_or(std::cmp::Ordering::Equal)
    } else if type_oid == pg_sys::FLOAT4OID {
        let f1 = f32::from_bits(d1.value() as u32);
        let f2 = f32::from_bits(d2.value() as u32);
        f1.partial_cmp(&f2).unwrap_or(std::cmp::Ordering::Equal)
    } else {
        std::cmp::Ordering::Equal
    }
}

fn count_non_null(null_bitmap: &[u8], total_count: usize) -> usize {
    if null_bitmap.is_empty() {
        return total_count;
    }
    let full_bytes = total_count / 8;
    let mut null_count: usize = null_bitmap[..full_bytes]
        .iter()
        .map(|b| b.count_ones() as usize)
        .sum();
    let remainder = total_count % 8;
    if remainder > 0 {
        let last = null_bitmap[full_bytes];
        let mask = (1u8 << remainder) - 1;
        null_count += (last & mask).count_ones() as usize;
    }
    total_count - null_count
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    use super::{PG_EPOCH_OFFSET_USEC, PG_EPOCH_OFFSET_DAYS};

    #[pg_test]
    fn test_pg_epoch_offset_usec() {
        // PG_EPOCH_OFFSET_USEC must equal the number of microseconds between
        // the Unix epoch (1970-01-01) and the PostgreSQL epoch (2000-01-01).
        let pg_val: i64 = Spi::get_one(
            "SELECT (EXTRACT(EPOCH FROM '2000-01-01 00:00:00+00'::timestamptz) * 1000000)::bigint"
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            pg_val, PG_EPOCH_OFFSET_USEC,
            "PG_EPOCH_OFFSET_USEC ({}) does not match PG's epoch ({})",
            PG_EPOCH_OFFSET_USEC, pg_val
        );
    }

    #[pg_test]
    fn test_pg_epoch_offset_days() {
        // PG_EPOCH_OFFSET_DAYS must equal the number of days between
        // the Unix epoch (1970-01-01) and the PostgreSQL epoch (2000-01-01).
        let pg_val: i32 = Spi::get_one(
            "SELECT ('2000-01-01'::date - '1970-01-01'::date)::int"
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            pg_val, PG_EPOCH_OFFSET_DAYS,
            "PG_EPOCH_OFFSET_DAYS ({}) does not match PG's value ({})",
            PG_EPOCH_OFFSET_DAYS, pg_val
        );
    }

    #[pg_test]
    fn test_timestamp_datum_matches_pg() {
        // Verify our epoch math produces the same internal representation PG uses.
        // PG stores timestamptz as microseconds since 2000-01-01 00:00:00 UTC.
        let test_cases = [
            "1970-01-01 00:00:00+00",
            "2000-01-01 00:00:00+00",
            "2013-07-14 12:34:56+00",
            "1969-12-31 23:59:59+00",
            "2025-01-15 00:00:00+00",
        ];

        for ts_str in &test_cases {
            // Get PG's internal representation (usec since PG epoch)
            let pg_internal: i64 = Spi::get_one(&format!(
                "SELECT (EXTRACT(EPOCH FROM '{}'::timestamptz) * 1000000)::bigint - {}::bigint",
                ts_str, PG_EPOCH_OFFSET_USEC
            ))
            .unwrap()
            .unwrap();

            // Our conversion: unix_usec - PG_EPOCH_OFFSET_USEC
            let unix_usec: i64 = Spi::get_one(&format!(
                "SELECT (EXTRACT(EPOCH FROM '{}'::timestamptz) * 1000000)::bigint",
                ts_str
            ))
            .unwrap()
            .unwrap();
            let our_datum = unix_usec - PG_EPOCH_OFFSET_USEC;

            assert_eq!(
                our_datum, pg_internal,
                "timestamp datum mismatch for {}: ours={} pg={}",
                ts_str, our_datum, pg_internal
            );
        }
    }

    #[pg_test]
    fn test_date_datum_matches_pg() {
        // PG stores dates as days since 2000-01-01.
        let test_cases = [
            ("1970-01-01", -10957),  // -PG_EPOCH_OFFSET_DAYS
            ("2000-01-01", 0),
            ("2025-01-15", 9146),
            ("1969-12-31", -10958),
        ];

        for (date_str, expected_pg_days) in &test_cases {
            // Get PG's internal representation (days since PG epoch)
            let pg_internal: i32 = Spi::get_one(&format!(
                "SELECT ('{}'::date - '2000-01-01'::date)::int",
                date_str
            ))
            .unwrap()
            .unwrap();

            assert_eq!(
                pg_internal, *expected_pg_days,
                "date sanity check failed for {}: pg={} expected={}",
                date_str, pg_internal, expected_pg_days
            );

            // Our conversion: unix_days - PG_EPOCH_OFFSET_DAYS
            let unix_days: i32 = Spi::get_one(&format!(
                "SELECT ('{}'::date - '1970-01-01'::date)::int",
                date_str
            ))
            .unwrap()
            .unwrap();
            let our_datum = unix_days - PG_EPOCH_OFFSET_DAYS;

            assert_eq!(
                our_datum, pg_internal,
                "date datum mismatch for {}: ours={} pg={}",
                date_str, our_datum, pg_internal
            );
        }
    }

    #[pg_test]
    fn test_float_datum_bit_preservation() {
        // Verify that f64 values survive Gorilla encode/decode with identical bits.
        use crate::compression::gorilla;

        let test_values: Vec<f64> = vec![
            0.0, -0.0, 1.0, -1.0, std::f64::consts::PI,
            1e308, -1e308, 1e-307, f64::MIN_POSITIVE,
        ];

        let encoded = gorilla::encode_floats(&test_values);
        let decoded = gorilla::decode_floats(&encoded, test_values.len());

        for (orig, dec) in test_values.iter().zip(decoded.iter()) {
            assert_eq!(
                orig.to_bits(), dec.to_bits(),
                "float bit mismatch: orig={} (0x{:016x}) decoded={} (0x{:016x})",
                orig, orig.to_bits(), dec, dec.to_bits()
            );
        }
    }

    #[test]
    fn test_reinsert_nulls_datum() {
        use pgrx::pg_sys;
        use super::reinsert_nulls_datum;

        // No nulls: empty bitmap
        let datums = vec![
            pg_sys::Datum::from(1usize),
            pg_sys::Datum::from(2usize),
            pg_sys::Datum::from(3usize),
        ];
        let result = reinsert_nulls_datum(&datums, &[], 3);
        assert_eq!(result.len(), 3);
        assert!(!result[0].1);
        assert!(!result[1].1);
        assert!(!result[2].1);

        // All nulls
        let bitmap = vec![0b11111111u8];
        let result = reinsert_nulls_datum(&[], &bitmap, 4);
        assert_eq!(result.len(), 4);
        for (_, is_null) in &result {
            assert!(is_null, "expected null");
        }

        // Alternating: null at 0, 2 (bits 0 and 2 set)
        let bitmap = vec![0b00000101u8];
        let datums = vec![
            pg_sys::Datum::from(10usize),
            pg_sys::Datum::from(30usize),
        ];
        let result = reinsert_nulls_datum(&datums, &bitmap, 4);
        assert_eq!(result.len(), 4);
        assert!(result[0].1);   // null
        assert!(!result[1].1);  // 10
        assert!(result[2].1);   // null
        assert!(!result[3].1);  // 30
        assert_eq!(result[1].0, pg_sys::Datum::from(10usize));
        assert_eq!(result[3].0, pg_sys::Datum::from(30usize));

        // Sparse: only position 5 is null in 8 values
        let bitmap = vec![0b00100000u8];
        let datums: Vec<pg_sys::Datum> = (0..7).map(|i| pg_sys::Datum::from(i as usize)).collect();
        let result = reinsert_nulls_datum(&datums, &bitmap, 8);
        assert_eq!(result.len(), 8);
        for i in 0..8 {
            if i == 5 {
                assert!(result[i].1, "position 5 should be null");
            } else {
                assert!(!result[i].1, "position {} should not be null", i);
            }
        }
    }
}
