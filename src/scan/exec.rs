use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::pg_guard;

use std::collections::HashMap;
use std::time::Instant;

use crate::compression::{self, CompressionType, CompressedColumnRef};
use super::SyncStatic;

/// Static CustomExecMethods struct for CocoonDecompress.
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

/// Static CustomExecMethods struct for CocoonAppend.
pub(crate) static COCOON_APPEND_EXEC_METHODS: SyncStatic<pg_sys::CustomExecMethods> =
    SyncStatic(pg_sys::CustomExecMethods {
        CustomName: super::COCOON_APPEND_NAME.as_ptr(),
        BeginCustomScan: Some(begin_cocoon_append),
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
        ExplainCustomScan: Some(super::explain::explain_cocoon_append),
    });

// Epoch offset: microseconds between Unix epoch (1970-01-01) and PG epoch (2000-01-01).
const PG_EPOCH_OFFSET_USEC: i64 = 946_684_800_000_000;
// Days between Unix epoch and PG epoch.
const PG_EPOCH_OFFSET_DAYS: i32 = 10_957;

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
    /// Total segments decompressed.
    pub(super) segments_decompressed: u64,
    /// Total compressed bytes loaded.
    pub(super) compressed_bytes: u64,
}

struct SegmentData {
    segment_values: Vec<Option<String>>,
    compressed_blobs: Vec<Vec<u8>>,
    row_count: i32,
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
            pgrx::error!("pg_cocoon: missing companion table OID in custom scan state");
        }

        let companion_oid =
            pg_sys::Oid::from(pg_sys::list_nth_int(custom_private, 0) as u32);

        // Parse needed column indices from custom_private (after sentinel -1)
        let list_len = (*custom_private).length as i32;
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
                    "pg_cocoon: companion table not found for OID {}",
                    u32::from(companion_oid)
                );
            }
            std::ffi::CStr::from_ptr(name_ptr)
                .to_string_lossy()
                .into_owned()
        };

        // Load metadata via SPI, then load segment data via direct heap scan
        let mut state = load_decompress_state(companion_oid, &companion_name, &needed_indices);

        // Create per-segment memory context
        let query_ctx = (*(*node).ss.ps.state).es_query_cxt;
        state.segment_mcxt = pg_sys::AllocSetContextCreateInternal(
            query_ctx,
            c"CocoonSegment".as_ptr(),
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        );

        // Box and store as raw pointer in custom_ps
        let state_box = Box::new(state);
        let state_ptr = Box::into_raw(state_box);
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}

/// CreateCustomScanState callback for CocoonAppend.
#[pg_guard]
pub unsafe extern "C-unwind" fn create_cocoon_append_state(
    cscan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    unsafe {
        let css = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScanState>())
            as *mut pg_sys::CustomScanState;

        (*css).ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
        (*css).methods = &COCOON_APPEND_EXEC_METHODS.0;

        // Copy custom_private for use in BeginCustomScan
        (*css).custom_ps = (*cscan).custom_private;

        css as *mut pg_sys::Node
    }
}

/// BeginCustomScan callback for CocoonAppend: load segments from all companion tables.
#[pg_guard]
pub unsafe extern "C-unwind" fn begin_cocoon_append(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: i32,
) {
    unsafe {
        let custom_private = (*node).custom_ps;
        if custom_private.is_null() {
            pgrx::error!("pg_cocoon: missing companion table OIDs in CocoonAppend state");
        }

        let list_len = (*custom_private).length as i32;

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
            pgrx::error!("pg_cocoon: CocoonAppend has no companion tables");
        }

        // Get first companion table name for metadata
        let first_name = {
            let name_ptr = pg_sys::get_rel_name(companion_oids[0]);
            if name_ptr.is_null() {
                pgrx::error!(
                    "pg_cocoon: companion table not found for OID {}",
                    u32::from(companion_oids[0])
                );
            }
            std::ffi::CStr::from_ptr(name_ptr)
                .to_string_lossy()
                .into_owned()
        };

        // Load metadata via SPI from first companion table
        let t0 = Instant::now();
        let meta = Spi::connect(|client| load_metadata(&client, &first_name));
        let metadata_us = t0.elapsed().as_micros() as u64;

        // Build needed_cols and needed_col_indices
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
            (nc, nci)
        };

        // Load segments from ALL companion tables via heap scan
        let t1 = Instant::now();
        let mut all_segments: Vec<SegmentData> = Vec::new();
        for &oid in &companion_oids {
            let segs = load_segments_heap(oid, &meta.col_names, &meta.segment_by, &needed_cols);
            all_segments.extend(segs);
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
                segments_decompressed: 0,
                compressed_bytes,
            },
            instrument: None,
        };

        // Create per-segment memory context
        let query_ctx = (*(*node).ss.ps.state).es_query_cxt;
        state.segment_mcxt = pg_sys::AllocSetContextCreateInternal(
            query_ctx,
            c"CocoonSegment".as_ptr(),
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        );

        let state_box = Box::new(state);
        let state_ptr = Box::into_raw(state_box);
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}

/// Metadata returned by the SPI metadata query.
struct MetadataInfo {
    col_names: Vec<String>,
    col_types: Vec<pg_sys::Oid>,
    col_typmods: Vec<i32>,
    segment_by: Vec<String>,
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
             FROM cocoon_partition p
             JOIN cocoon_hypertable h ON h.id = p.hypertable_id
             WHERE p.table_name = $1 AND p.is_compressed = true",
            None,
            &[companion_name.into()],
        )
        .expect("failed to query partition info");

    let ht_row = ht_result.next().unwrap_or_else(|| {
        pgrx::error!(
            "pg_cocoon: no compressed partition info found for {}",
            companion_name
        );
    });

    let segment_by: Vec<String> = ht_row
        .get_datum_by_ordinal(1)
        .unwrap()
        .value::<Vec<String>>()
        .unwrap()
        .unwrap_or_default();
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
    }
}

/// Load segment data from the companion table via direct heap scan.
///
/// Bypasses SPI entirely — opens the companion table, iterates all tuples
/// with `heap_getnext`, and extracts segment_by values, compressed BYTEA blobs,
/// and row counts directly from the heap tuples.
unsafe fn load_segments_heap(
    companion_oid: pg_sys::Oid,
    col_names: &[String],
    segment_by: &[String],
    needed_cols: &[bool],
) -> Vec<SegmentData> {
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
            if segment_by.contains(name) {
                if let Some(&attno) = attno_map.get(name.as_str()) {
                    let type_oid = att_type_oids[name.as_str()];
                    segment_by_attnos.push((attno, type_oid));
                }
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
                                data as *const u8,
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

            // Extract _row_count (INT4)
            let row_count = match row_count_attno {
                Some(attno) if !nulls[attno] => values[attno].value() as i32,
                _ => 0,
            };

            segments.push(SegmentData {
                segment_values,
                compressed_blobs,
                row_count,
            });
        }

        // End scan + close relation
        (*(*rel).rd_tableam).scan_end.unwrap()(scan);
        pg_sys::table_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

        segments
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
) -> DecompressState {
    // Phase 1: SPI for metadata only (small, fast)
    let t0 = Instant::now();
    let meta = Spi::connect(|client| load_metadata(&client, companion_name));
    let metadata_us = t0.elapsed().as_micros() as u64;

    // Build needed_cols and needed_col_indices from needed_indices
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
        (nc, nci)
    };

    // Phase 2: Direct heap scan for segment data (bypasses SPI overhead)
    let t1 = Instant::now();
    let segments_data = unsafe {
        load_segments_heap(companion_oid, &meta.col_names, &meta.segment_by, &needed_cols)
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
            segments_decompressed: 0,
            compressed_bytes,
        },
        instrument: None, // set lazily on first exec call
    }
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

            let t_decompress = if instrument { Some(Instant::now()) } else { None };

            // Reset segment memory context — frees all varlena from previous segment
            pg_sys::MemoryContextReset(state.segment_mcxt);
            let old_ctx = pg_sys::MemoryContextSwitchTo(state.segment_mcxt);

            // Decompress needed columns directly to datums
            let mut decompressed: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
            let mut blob_idx = 0;
            let mut seg_val_idx = 0;

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
                    let type_name = pg_type_name(type_oid);
                    let typmod = state.col_typmods[col_idx];
                    let datums = decompress_blob_to_datums(blob, &type_name, type_oid, typmod);
                    decompressed.push(datums);
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
            let total_us = t.metadata_us + t.heap_scan_us + t.decompress_us + t.emit_us;
            pgrx::log!(
                "pg_cocoon scan timing: total={:.1}ms  metadata={:.1}ms  heap_scan={:.1}ms  \
                 decompress={:.1}ms  emit={:.1}ms  | \
                 segments={} rows_out={} rows_filtered={} compressed_bytes={}",
                total_us as f64 / 1000.0,
                t.metadata_us as f64 / 1000.0,
                t.heap_scan_us as f64 / 1000.0,
                t.decompress_us as f64 / 1000.0,
                t.emit_us as f64 / 1000.0,
                t.segments_decompressed,
                t.rows_emitted,
                t.rows_filtered,
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
                    compression::gorilla::decode_timestamps(&cc.data, non_null_count);
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
                    compression::gorilla::decode_floats_f32(&cc.data, non_null_count);
                floats
                    .iter()
                    .map(|&v| pg_sys::Datum::from(v.to_bits() as usize))
                    .collect()
            } else {
                let floats =
                    compression::gorilla::decode_floats(&cc.data, non_null_count);
                floats
                    .iter()
                    .map(|&v| pg_sys::Datum::from(v.to_bits() as usize))
                    .collect()
            }
        }
        CompressionType::DeltaVarint => {
            if dt == "integer" || dt.contains("int4") || dt == "smallint" {
                let ints = compression::integer::decode_i32(&cc.data, non_null_count);
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
                let ints = compression::integer::decode_i64(&cc.data, non_null_count);
                ints.iter()
                    .map(|&v| pg_sys::Datum::from(v as usize))
                    .collect()
            }
        }
        CompressionType::Dictionary => {
            let slices = compression::dictionary::decode_to_slices(cc.data, non_null_count);
            unsafe {
                slices
                    .iter()
                    .map(|s| str_to_text_datum(s, type_oid, typmod))
                    .collect()
            }
        }
        CompressionType::Lz4 => {
            let (buf, ranges) = compression::lz4::decode_to_ranges(cc.data, non_null_count);
            unsafe {
                ranges
                    .iter()
                    .map(|&(off, len)| {
                        let s = std::str::from_utf8(&buf[off..off + len])
                            .expect("invalid UTF-8 in LZ4 data");
                        str_to_text_datum(s, type_oid, typmod)
                    })
                    .collect()
            }
        }
        CompressionType::BooleanBitmap => {
            let bools = compression::boolean::decode(&cc.data, non_null_count);
            bools
                .iter()
                .map(|&b| pg_sys::Datum::from(b as usize))
                .collect()
        }
    };

    reinsert_nulls_datum(&datums, cc.null_bitmap, total_count)
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
