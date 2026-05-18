//! Custom-scan executor callbacks for `DeltaXAgg`:
//! `begin_agg_scan`, `exec_agg_scan`, `end_agg_scan`, `rescan_agg_scan`,
//! plus the parallel-DSM scaffolding (`estimate_dsm_deltax_agg`,
//! `initialize_dsm_deltax_agg`, `reinit_dsm_deltax_agg`,
//! `init_worker_deltax_agg`, `shutdown_deltax_agg`) and the
//! `DELTAX_AGG_EXEC_METHODS` static that wires them all into PG's
//! custom scan API.

use std::sync::atomic::Ordering;
use std::time::Instant;

use pgrx::pg_guard;
use pgrx::pg_sys;

use super::super::SyncStatic;
use super::super::agg_wire;
use super::super::batch_qual::{BatchCompareOp, BatchQual, extract_batch_quals};
use super::super::segments::{
    SegmentData, extract_segment_filters, load_segments_heap, load_text_length_sidecars,
    reset_scan_buf_stats,
};
use super::compact::{
    CompactAccLayout, CompactAccStorage, can_use_compact_accs, compact_emit_partial,
    compact_finalize, i128_to_numeric_datum,
};
use super::keys::{CompactGroupMap, can_use_compact_keys, unpack_int_keys};
use super::metadata::{load_agg_metadata_from_plan, try_catalog_shortcut, try_metadata_fast_path};
use super::parallel_cd::{dispatch_parallel_count_distinct_path, parallel_count_distinct_eligible};
use super::parallel_compact::{
    ParallelCompactConfig, ParallelCompactResult, dispatch_parallel_compact_path,
    merge_compact_results, parallel_compact_eligible, process_segments_compact,
};
use super::parallel_mixed::{can_parallel_mixed, dispatch_parallel_mixed_path};
use super::parser::{
    build_agg_exec_context_from_plan, build_deferred_agg_state, build_minimal_worker_state,
    parse_agg_private,
};
use super::regex::{RustRegexInfo, convert_pg_replacement, try_compile_rust_regex};
use super::serial::dispatch_serial_path;
use super::state::{
    AggExecSpec, AggExpr, AggScanState, AggTimingShmem, AggType, CaseWhenValue, DeltaXAggPState,
    GroupByColSpec, GroupByExpr, MAX_AGG_WORKER_SLOTS, OutputEntry, PARTIAL_SLAB_SIZE_BYTES,
    ParsedAggPlan,
};

/// EstimateDSMCustomScan: bytes for `DeltaXAggPState` + N+1 partial-state
/// slabs (one per leader/worker). The slab count is fixed at the cap so
/// re-sizing isn't needed if the planner picks a smaller worker count
/// later — wasted DSM bytes in that case are bounded by
/// `PARTIAL_SLAB_SIZE_BYTES * (MAX_AGG_WORKER_SLOTS - 1 - nworkers)`.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn estimate_dsm_deltax_agg(
    _node: *mut pg_sys::CustomScanState,
    pcxt: *mut pg_sys::ParallelContext,
) -> pg_sys::Size {
    unsafe {
        let nworkers = (*pcxt).nworkers as usize;
        let nslots = (nworkers + 1).min(MAX_AGG_WORKER_SLOTS);
        (std::mem::size_of::<DeltaXAggPState>() + nslots * PARTIAL_SLAB_SIZE_BYTES) as pg_sys::Size
    }
}

/// InitializeDSMCustomScan: leader populates the shared region after its own
/// `BeginCustomScan` has run. `coordinate` is a `DeltaXAggPState` carved out
/// of PG's parallel-context DSM segment.
///
/// Phase C.1 wires the cursor + slot count. Phase C.2 will set
/// `total_segments` from the leader's `segments_data` and slot the per-
/// worker partial-result region offsets.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn initialize_dsm_deltax_agg(
    node: *mut pg_sys::CustomScanState,
    pcxt: *mut pg_sys::ParallelContext,
    coordinate: *mut std::ffi::c_void,
) {
    unsafe {
        let ps = coordinate as *mut DeltaXAggPState;
        std::ptr::write_bytes(ps as *mut u8, 0, std::mem::size_of::<DeltaXAggPState>());

        let nworkers = (*pcxt).nworkers as usize;
        if nworkers + 1 > MAX_AGG_WORKER_SLOTS {
            pgrx::error!(
                "pg_deltax: parallel worker count {} exceeds MAX_AGG_WORKER_SLOTS {}",
                nworkers,
                MAX_AGG_WORKER_SLOTS - 1,
            );
        }

        // `next_segment` is zero from `write_bytes`; AtomicU64 has the same
        // memory representation as `u64` so that's a valid initial state.
        (*ps).total_segments = 0;
        (*ps).n_worker_slots = (nworkers + 1) as u32;
        (*ps).partial_slab_size = PARTIAL_SLAB_SIZE_BYTES as u32;
        // `partial_lens` is zero-initialised by `write_bytes`; AtomicU64
        // shares layout with u64.

        let state_ptr = (*node).custom_ps as *mut AggScanState;
        if !state_ptr.is_null() {
            (*state_ptr).pscan = ps;
            // Phase C.2.c: leader's `begin_agg_scan` parallel branch already
            // populated `exec_ctx.all_segments`; publish the segment count
            // to the cursor range so workers' `next_segment.fetch_add` knows
            // when to stop. If exec_ctx is None (planner chose
            // parallel_aware but begin's parallel branch didn't fire — a
            // bug), workers will see total_segments == 0 and exit cleanly.
            if let Some(ref ctx) = (*state_ptr).exec_ctx {
                (*ps).total_segments = ctx.all_segments.len() as u64;
            }
        }
    }
}

/// ReInitializeDSMCustomScan: reset the shared cursor and per-slot timing
/// before a rescan. The leader is the only caller.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn reinit_dsm_deltax_agg(
    _node: *mut pg_sys::CustomScanState,
    _pcxt: *mut pg_sys::ParallelContext,
    coordinate: *mut std::ffi::c_void,
) {
    unsafe {
        let ps = coordinate as *mut DeltaXAggPState;
        (*ps).next_segment.store(0, Ordering::Relaxed);
        for slot in (*ps).worker_timings.iter_mut() {
            *slot = AggTimingShmem::default();
        }
        for len in (*ps).partial_lens.iter_mut() {
            len.store(0, Ordering::Relaxed);
        }
    }
}

/// InitializeWorkerCustomScan: worker attaches DSM, re-runs the leader's
/// metadata + segment + qual prelude, and stashes the result on the
/// worker-side `AggScanState.exec_ctx` so `exec_agg_scan`'s claim loop
/// (Phase C.2.d) can drive segment fetches from the shared cursor.
///
/// V1 hydrates via SPI (per PARALLEL_AGG.md C.2.c "re-SPI for now"); V2
/// follow-up will share leader-loaded segments via DSM (mirroring
/// `append_wire`).
///
/// SPI is legal inside `InitializeWorkerCustomScan` because PG opens a
/// transaction in each worker before calling our hooks.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn init_worker_deltax_agg(
    node: *mut pg_sys::CustomScanState,
    _toc: *mut pg_sys::shm_toc,
    coordinate: *mut std::ffi::c_void,
) {
    unsafe {
        // The worker's `begin_agg_scan` short-circuit installed a minimal
        // stub state — replace it now with a deferred state populated by
        // re-SPI'd metadata + segments. Mirror of decompress.rs's
        // `init_worker_deltax_append`.
        let cscan = (*node).ss.ps.plan as *mut pg_sys::CustomScan;
        let custom_private = (*cscan).custom_private;
        if custom_private.is_null() {
            pgrx::error!("pg_deltax: parallel DeltaXAgg worker has no custom_private");
        }
        let plan = parse_agg_private(custom_private);
        if plan.companion_oids.is_empty() {
            pgrx::error!("pg_deltax: parallel DeltaXAgg worker has no companion tables");
        }

        // The leader extracted batch_quals from `where_quals`; reproduce on
        // the worker and clear `ps.qual` if all quals are batch-handled, so
        // PG doesn't re-evaluate them after `exec_agg_scan` returns. Mirrors
        // decompress.rs's qual-clear pattern.
        let plan_qual = (*(*node).ss.ps.plan).qual;

        reset_scan_buf_stats();
        let ctx = build_agg_exec_context_from_plan(plan);

        // If batch_quals fully cover the planner's qual list, clear it so
        // PG's executor won't double-evaluate after exec emits virtual rows.
        if !plan_qual.is_null()
            && !ctx.batch_quals.is_empty()
            && ctx.batch_quals.len() as i32 == (*plan_qual).length
        {
            (*node).ss.ps.qual = std::ptr::null_mut();
        }

        let mut state = build_deferred_agg_state(ctx, /* is_worker */ true);
        state.pscan = coordinate as *mut DeltaXAggPState;

        let state_ptr = Box::into_raw(Box::new(state));
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}

/// ShutdownCustomScan: each process records its worker-slot timing into
/// `worker_timings[slot]` while DSM is still attached, so the leader can
/// aggregate per-process numbers for EXPLAIN.
///
/// Phase C.1 just stamps `populated = 1` so the leader sees that this slot
/// participated. Phase C.2 will serialise the process's local
/// `ParallelCompactResult` into the slot's slab and copy timing.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn shutdown_deltax_agg(node: *mut pg_sys::CustomScanState) {
    unsafe {
        let state_ptr = (*node).custom_ps as *mut AggScanState;
        if state_ptr.is_null() {
            return;
        }
        let state = &mut *state_ptr;
        if state.pscan.is_null() {
            return;
        }
        let slot_idx = current_agg_worker_slot();
        let ps = &mut *state.pscan;
        if slot_idx >= ps.n_worker_slots as usize {
            return;
        }
        let slot = &mut ps.worker_timings[slot_idx];
        slot.populated = 1;
    }
}

/// Returns the DSM slot index for the current process. Slot 0 is the
/// leader; worker N (0-indexed in PG) gets slot N+1.
///
/// # Safety
///
/// Reads `pg_sys::ParallelWorkerNumber` (extern static). Must run
/// inside an active PG executor callback.
#[allow(dead_code)] // Phase C.1; widely used in C.2.
unsafe fn current_agg_worker_slot() -> usize {
    unsafe {
        let n = pg_sys::ParallelWorkerNumber;
        if n < 0 {
            0
        } else {
            ((n as usize) + 1).min(MAX_AGG_WORKER_SLOTS - 1)
        }
    }
}

/// Static CustomExecMethods struct for DeltaXAgg.
pub(crate) static DELTAX_AGG_EXEC_METHODS: SyncStatic<pg_sys::CustomExecMethods> =
    SyncStatic(pg_sys::CustomExecMethods {
        CustomName: crate::scan::DELTAX_AGG_NAME.as_ptr(),
        BeginCustomScan: Some(begin_agg_scan),
        ExecCustomScan: Some(exec_agg_scan),
        EndCustomScan: Some(end_agg_scan),
        ReScanCustomScan: Some(rescan_agg_scan),
        MarkPosCustomScan: None,
        RestrPosCustomScan: None,
        // Phase C.1: hook bodies are functional scaffolding. Workers can
        // attach to DSM and flag themselves as `is_parallel_worker = true`,
        // but they don't yet claim segments — `begin_agg_scan` short-
        // circuits into `build_minimal_worker_state` and `exec_agg_scan`
        // returns EOF. Phase C.2 wires up the actual segment-claim and
        // partial-aggregate work.
        //
        // `add_agg_path` still sets `parallel_workers = 0` until C.4, so
        // these hooks remain unused under the default cost path.
        EstimateDSMCustomScan: Some(estimate_dsm_deltax_agg),
        InitializeDSMCustomScan: Some(initialize_dsm_deltax_agg),
        ReInitializeDSMCustomScan: Some(reinit_dsm_deltax_agg),
        InitializeWorkerCustomScan: Some(init_worker_deltax_agg),
        ShutdownCustomScan: Some(shutdown_deltax_agg),
        ExplainCustomScan: Some(crate::scan::explain::explain_agg_scan),
    });

// ============================================================================
// DeltaXAgg execution callbacks
// ============================================================================

/// CreateCustomScanState callback for DeltaXAgg.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn create_agg_scan_state(
    cscan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    unsafe {
        let css = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScanState>())
            as *mut pg_sys::CustomScanState;

        (*css).ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
        (*css).methods = &DELTAX_AGG_EXEC_METHODS.0;
        (*css).custom_ps = (*cscan).custom_private;

        css as *mut pg_sys::Node
    }
}

/// BeginCustomScan callback for DeltaXAgg: decompress and aggregate.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn begin_agg_scan(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: i32,
) {
    unsafe {
        // Phase C.1: parallel-worker short-circuit. Each worker process runs
        // its own `BeginCustomScan` (PG calls it per-process for parallel-
        // aware paths). Workers must NOT duplicate the leader's SPI + heap
        // scan + accumulator construction here — that runs in
        // `init_worker_deltax_agg` once DSM is wired up, so the worker has
        // the leader's segment-cursor handle to drive the claim loop.
        if pg_sys::ParallelWorkerNumber >= 0 {
            let state = build_minimal_worker_state();
            let state_ptr = Box::into_raw(Box::new(state));
            (*node).custom_ps = state_ptr as *mut pg_sys::List;
            return;
        }

        // Phase C.2.c: parallel-aware leader branch. When the planner chose
        // the parallel path (gated by `add_agg_path` + `recommend_agg_workers`
        // in C.2.f), the leader hydrates an `AggExecContext` once and defers
        // all per-segment work to `exec_agg_scan`, where it claims segments
        // alongside workers via `pscan.next_segment.fetch_add`. The serial
        // / internal-rayon path below stays unchanged for `parallel_aware ==
        // false`.
        if (*(*node).ss.ps.plan).parallel_aware {
            let custom_private = (*node).custom_ps;
            if custom_private.is_null() {
                pgrx::error!("pg_deltax: missing custom_private in DeltaXAgg state");
            }
            let plan = parse_agg_private(custom_private);
            if plan.companion_oids.is_empty() {
                pgrx::error!("pg_deltax: DeltaXAgg has no companion tables");
            }
            reset_scan_buf_stats();
            let ctx = build_agg_exec_context_from_plan(plan);
            let state = build_deferred_agg_state(ctx, /* is_worker */ false);
            let state_ptr = Box::into_raw(Box::new(state));
            (*node).custom_ps = state_ptr as *mut pg_sys::List;
            return;
        }

        let t_wall = Instant::now();
        // Reset per-phase buffer accumulator for this scan. `load_segments_heap`
        // writes to the thread-local; the AggScanState ctor reads it back out
        // via `take_scan_buf_stats()`.
        reset_scan_buf_stats();
        let custom_private = (*node).custom_ps;
        if custom_private.is_null() {
            pgrx::error!("pg_deltax: missing custom_private in DeltaXAgg state");
        }

        let plan = parse_agg_private(custom_private);

        if plan.companion_oids.is_empty() {
            pgrx::error!("pg_deltax: DeltaXAgg has no companion tables");
        }

        let (meta, metadata_us) = load_agg_metadata_from_plan(&plan.companion_oids);

        // Fast path 1: answer from catalog metadata (no segment scan at all)
        {
            let row_counts: Vec<Option<i64>> = plan
                .companion_oids
                .iter()
                .map(|&oid| crate::scan::cost::get_row_count(oid))
                .collect();
            if let Some(state) = try_catalog_shortcut(&plan, &meta, &row_counts, metadata_us) {
                let state_ptr = Box::into_raw(Box::new(state));
                (*node).custom_ps = state_ptr as *mut pg_sys::List;
                return;
            }
        }

        // Fast path 2: answer from per-segment metadata (with selective decompression for filtered queries)
        // Skip early when conditions that try_metadata_fast_path checks immediately
        // would fail: GROUP BY, HAVING, CountDistinct, or non-numeric agg columns.
        let t_fp2 = Instant::now();
        if plan.group_specs.is_empty()
            && plan.having_filters.is_empty()
            && plan
                .agg_specs
                .iter()
                .all(|s| s.agg_type != AggType::CountDistinct)
        {
            let needs_sums = plan
                .agg_specs
                .iter()
                .any(|s| matches!(s.agg_type, AggType::Sum | AggType::Avg));
            let needs_counts = plan
                .agg_specs
                .iter()
                .any(|s| matches!(s.agg_type, AggType::Count));
            let needs_minmax = plan
                .agg_specs
                .iter()
                .any(|s| matches!(s.agg_type, AggType::Min | AggType::Max));
            let num_cols = meta.col_names.len();

            // Extract batch quals early for the filtered metadata fast path
            let fast_batch_quals = if !plan.where_quals.is_null() {
                let (bqs, handled) =
                    extract_batch_quals(plan.where_quals, &meta.col_names, &meta.col_types);
                if handled as i32 == (*plan.where_quals).length {
                    bqs
                } else {
                    vec![]
                }
            } else {
                vec![]
            };

            let mut load_minmax = needs_minmax;
            if !fast_batch_quals.is_empty() {
                load_minmax = true;
            }

            // Build list of columns needing stats (sum/nonnull/nonzero) from colstats
            let mut needed_stats_set: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            if needs_sums || needs_counts {
                for s in &plan.agg_specs {
                    if s.col_idx >= 0
                        && matches!(s.agg_type, AggType::Sum | AggType::Avg | AggType::Count)
                    {
                        needed_stats_set.insert(meta.col_names[s.col_idx as usize].clone());
                    }
                }
            }
            for bq in &fast_batch_quals {
                needed_stats_set.insert(meta.col_names[bq.col_idx].clone());
            }
            let needed_stats_cols: Vec<String> = needed_stats_set.into_iter().collect();

            // Extract segment-by/time filters for pruning
            let (seg_filters, time_min, time_max) = if !plan.where_quals.is_null() {
                extract_segment_filters(
                    plan.where_quals,
                    &meta.col_names,
                    &meta.segment_by,
                    &meta.time_column,
                )
            } else {
                (vec![], None, None)
            };

            // Build list of columns needing minmax from colstats
            let needed_minmax_cols: Vec<String> = plan
                .agg_specs
                .iter()
                .filter(|s| matches!(s.agg_type, AggType::Min | AggType::Max))
                .map(|s| meta.col_names[s.col_idx as usize].clone())
                .collect();

            // Fast path: load metadata only (no blobs) — Phase 2 is skipped
            let no_blobs = vec![false; num_cols];
            let t1 = Instant::now();
            let mut all_segments: Vec<SegmentData> = Vec::new();
            for &oid in &plan.companion_oids {
                let (segs, _, _, _, _, _) = load_segments_heap(
                    oid,
                    &meta.col_names,
                    &meta.segment_by,
                    &no_blobs,
                    &meta.time_column,
                    load_minmax,
                    &seg_filters,
                    time_min,
                    time_max,
                    None,
                    &fast_batch_quals,
                    &needed_stats_cols,
                    &meta.col_types,
                    &needed_minmax_cols,
                    false,
                );
                all_segments.extend(segs);
            }
            let heap_scan_us = t1.elapsed().as_micros() as u64;

            if let Some(state) = try_metadata_fast_path(
                &plan,
                &meta,
                &all_segments,
                &fast_batch_quals,
                metadata_us,
                heap_scan_us,
            ) {
                let state_ptr = Box::into_raw(Box::new(state));
                (*node).custom_ps = state_ptr as *mut pg_sys::List;
                return;
            }
        }

        // Destructure plan for the full scan path
        let ParsedAggPlan {
            companion_oids,
            agg_specs,
            group_specs,
            output_map,
            having_filters,
            where_quals,
            topn_limit,
            topn_sort_col,
            topn_ascending,
            derived_minmax_topn,
            bare_limit,
            is_partial: _,
        } = plan;

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
            // CaseWhen references additional columns in conditions and result values
            if let GroupByExpr::CaseWhen(ref spec) = gs.expr {
                for clause in &spec.clauses {
                    for cond in &clause.conditions {
                        if cond.col_idx < num_cols {
                            needed_cols[cond.col_idx] = true;
                        }
                    }
                    if let CaseWhenValue::ColumnRef(ci) = &clause.result
                        && *ci < num_cols
                    {
                        needed_cols[*ci] = true;
                    }
                }
                if let CaseWhenValue::ColumnRef(ci) = &spec.default
                    && *ci < num_cols
                {
                    needed_cols[*ci] = true;
                }
            }
        }

        // Build length_cols: columns where ALL referencing agg specs use LengthOf.
        // These columns will be decompressed as int4 lengths instead of text datums.
        let length_cols: Vec<bool> = (0..num_cols)
            .map(|col_idx| {
                let refs: Vec<&AggExecSpec> = agg_specs
                    .iter()
                    .filter(|s| s.col_idx >= 0 && s.col_idx as usize == col_idx)
                    .collect();
                !refs.is_empty() && refs.iter().all(|s| s.expr_kind == AggExpr::LengthOf)
            })
            .collect();

        // Build count_distinct_only_str: text columns where ALL referencing agg specs
        // are CountDistinct and the column is not used in GROUP BY.  These can be
        // pre-accumulated directly from compressed data, skipping datum conversion.
        let count_distinct_only_str: Vec<bool> = (0..num_cols)
            .map(|col_idx| {
                let is_text = matches!(
                    meta.col_types[col_idx],
                    x if x == pg_sys::TEXTOID || x == pg_sys::VARCHAROID || x == pg_sys::BPCHAROID
                );
                if !is_text {
                    return false;
                }
                let refs: Vec<&AggExecSpec> = agg_specs
                    .iter()
                    .filter(|s| s.col_idx >= 0 && s.col_idx as usize == col_idx)
                    .collect();
                if refs.is_empty() {
                    return false;
                }
                let all_cd = refs.iter().all(|s| s.agg_type == AggType::CountDistinct);
                let in_group_by = group_specs.iter().any(|gs| gs.col_idx as usize == col_idx);
                all_cd && !in_group_by
            })
            .collect();

        // Build count_distinct_only_int: integer columns where ALL referencing agg specs
        // are CountDistinct and the column is not used in GROUP BY.  These can be
        // pre-accumulated directly from compressed data, skipping datum conversion.
        let count_distinct_only_int: Vec<bool> = (0..num_cols)
            .map(|col_idx| {
                let is_int = meta.col_types[col_idx] == pg_sys::INT2OID
                    || meta.col_types[col_idx] == pg_sys::INT4OID
                    || meta.col_types[col_idx] == pg_sys::INT8OID;
                if !is_int {
                    return false;
                }
                let refs: Vec<&AggExecSpec> = agg_specs
                    .iter()
                    .filter(|s| s.col_idx >= 0 && s.col_idx as usize == col_idx)
                    .collect();
                if refs.is_empty() {
                    return false;
                }
                let all_cd = refs.iter().all(|s| s.agg_type == AggType::CountDistinct);
                let in_group_by = group_specs.iter().any(|gs| gs.col_idx as usize == col_idx);
                all_cd && !in_group_by
            })
            .collect();

        // Extract batch quals and segment filters from WHERE clause (quals from custom_private)
        let (batch_quals, _handled_count) =
            extract_batch_quals(where_quals, &meta.col_names, &meta.col_types);

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

        // Build sidecar_only_cols: text columns where every aggregate on the
        // column is LengthOf AND every batch qual is `= ''` / `<> ''`, and the
        // column is not in GROUP BY. For such columns we can skip detoasting
        // the main text blob and use the compact per-row length sidecar.
        //
        // Only the parallel mixed path knows how to read from the sidecar; if
        // that path won't run for this query, we must load the main blob as
        // usual. So the sidecar flags are cleared when the query isn't a
        // parallel-mixed candidate.
        let sidecar_candidate: Vec<bool> = (0..num_cols)
            .map(|col_idx| {
                let t = meta.col_types[col_idx];
                let is_text =
                    t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID;
                if !is_text {
                    return false;
                }
                if !needed_cols[col_idx] {
                    return false;
                }

                // Must not appear in GROUP BY
                if group_specs
                    .iter()
                    .any(|gs| gs.col_idx >= 0 && gs.col_idx as usize == col_idx)
                {
                    return false;
                }
                // Every agg on this column must be LengthOf
                let agg_refs: Vec<&AggExecSpec> = agg_specs
                    .iter()
                    .filter(|s| s.col_idx >= 0 && s.col_idx as usize == col_idx)
                    .collect();
                if !agg_refs.iter().all(|s| s.expr_kind == AggExpr::LengthOf) {
                    return false;
                }
                // Every batch qual on this column must be EQ/NE with empty string
                let qual_refs: Vec<&BatchQual> = batch_quals
                    .iter()
                    .filter(|bq| bq.col_idx == col_idx)
                    .collect();
                let all_quals_ok = qual_refs.iter().all(|bq| {
                    matches!(bq.op, BatchCompareOp::Eq | BatchCompareOp::Ne)
                        && matches!(bq.text_const.as_deref(), Some(""))
                });
                if !all_quals_ok {
                    return false;
                }
                // Must have at least one usage (otherwise the column wouldn't be needed)
                !agg_refs.is_empty() || !qual_refs.is_empty()
            })
            .collect();

        // Gate sidecar activation on the parallel-mixed path being usable.
        //
        // can_parallel_mixed_flag (computed later) also requires
        // `all_segments.len() > 1` — but segments aren't loaded yet.
        // Without checking that upstream, a query that ends up going
        // through the non-parallel path reads from `compressed_blobs`
        // entries that were never loaded (because sidecar mode skipped
        // them), silently returning NULL from AVG(length()).
        //
        // Use a conservative catalog-based row estimate: one default
        // segment is 30K rows, so require at least 2× that to be
        // confident the parallel path will fire.
        const SIDECAR_MIN_ROWS: i64 = 60_000;
        let estimated_rows: i64 = companion_oids
            .iter()
            .map(|&oid| crate::scan::cost::get_row_count(oid).unwrap_or(0))
            .sum();
        let sidecar_only_cols: Vec<bool> = if sidecar_candidate.iter().any(|&s| s)
            && crate::get_parallel_workers() > 1
            && estimated_rows >= SIDECAR_MIN_ROWS
            && can_parallel_mixed(
                &group_specs,
                &needed_cols,
                &meta.col_types,
                &meta.col_not_null,
                &batch_quals,
                &agg_specs,
            ) {
            sidecar_candidate
        } else {
            vec![false; num_cols]
        };

        // For load_segments_heap, suppress main-blob loading for sidecar-only
        // columns. We load their length sidecars separately below.
        let mut needed_cols_main: Vec<bool> = needed_cols.clone();
        for (i, &s) in sidecar_only_cols.iter().enumerate() {
            if s {
                needed_cols_main[i] = false;
            }
        }

        let mut needed_minmax_set: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for gs in &group_specs {
            if matches!(gs.expr, GroupByExpr::Extract { .. })
                && gs.col_idx >= 0
                && (gs.col_idx as usize) < meta.col_names.len()
            {
                needed_minmax_set.insert(meta.col_names[gs.col_idx as usize].clone());
            }
        }
        let needed_minmax_cols: Vec<String> = needed_minmax_set.into_iter().collect();

        // Load segments from all companion tables (with lazy pruning)
        let n_workers = crate::get_parallel_workers();
        let use_lazy = n_workers > 1;
        let lazy_cols: Vec<bool> = needed_cols_main.clone();
        let mut all_segments: Vec<SegmentData> = Vec::new();
        let mut total_detoast_us: u64 = 0;
        // Blob-cache accumulators paired with `total_detoast_us` so call
        // sites can fold the `DetoastLazyStats` returned by the lazy
        // helpers. Surfaced in EXPLAIN as `DeltaX Blob Cache`.
        let mut total_cache_hits: u64 = 0;
        let mut total_cache_misses: u64 = 0;
        let mut total_cache_bytes_served: u64 = 0;
        for &oid in &companion_oids {
            let (mut segs, _, _, _, _, dt_us) = load_segments_heap(
                oid,
                &meta.col_names,
                &meta.segment_by,
                &needed_cols_main,
                &meta.time_column,
                !needed_minmax_cols.is_empty(),
                &seg_filters,
                time_min,
                time_max,
                if use_lazy { Some(&lazy_cols) } else { None },
                &batch_quals,
                &needed_minmax_cols,
                &meta.col_types,
                &needed_minmax_cols,
                false,
            );
            // Load text-length sidecars for the columns in sidecar-only mode.
            if sidecar_only_cols.iter().any(|&s| s) {
                let sidecar_detoast_us = load_text_length_sidecars(
                    oid,
                    &meta.col_names,
                    &meta.segment_by,
                    &sidecar_only_cols,
                    &mut segs,
                );
                total_detoast_us += sidecar_detoast_us;
            }
            all_segments.extend(segs);
            total_detoast_us += dt_us;
        }
        // heap_scan_us includes fast-path-2 heap scan (eager detoast for metadata
        // check) plus the main lazy scan.  t_fp2 was started before fast-path-2.
        let heap_scan_us = t_fp2.elapsed().as_micros() as u64;

        // Initialize accumulators
        let has_group_by = !group_specs.is_empty();
        let num_result_cols = output_map.len();
        let is_single_group_key = group_specs.len() == 1;

        // Compact path: use flat byte buffer for accumulators when possible
        let use_compact_accs = has_group_by && can_use_compact_accs(&agg_specs);
        let mut compact_storage = if use_compact_accs {
            Some(CompactAccStorage::new(CompactAccLayout::new(&agg_specs)))
        } else {
            None
        };

        // Compact keys: pack integer GROUP BY keys into u128
        let use_compact_keys =
            has_group_by && can_use_compact_keys(&group_specs, &meta.col_not_null);

        // Check if any GROUP BY uses RegexpReplace — set up cross-segment caches
        let has_regexp_group = group_specs
            .iter()
            .any(|gs| matches!(gs.expr, GroupByExpr::RegexpReplace { .. }));

        // Identify text GROUP BY columns (dictionary or LZ4)
        let mut text_group_cols: Vec<bool> = vec![false; meta.col_names.len()];
        for gs in &group_specs {
            if matches!(gs.expr, GroupByExpr::Column)
                && (gs.type_oid == pg_sys::TEXTOID
                    || gs.type_oid == pg_sys::VARCHAROID
                    || gs.type_oid == pg_sys::BPCHAROID
                    || gs.type_oid == pg_sys::NAMEOID)
            {
                text_group_cols[gs.col_idx as usize] = true;
            }
            // CaseWhen ColumnRef results reference text columns that need decompression
            if let GroupByExpr::CaseWhen(ref spec) = gs.expr {
                for clause in &spec.clauses {
                    if let CaseWhenValue::ColumnRef(ci) = &clause.result
                        && *ci < text_group_cols.len()
                    {
                        text_group_cols[*ci] = true;
                    }
                }
                if let CaseWhenValue::ColumnRef(ci) = &spec.default
                    && *ci < text_group_cols.len()
                {
                    text_group_cols[*ci] = true;
                }
            }
        }

        // ============================================================
        // PARALLEL COMPACT PATH: multi-threaded segment processing
        // ============================================================
        if parallel_compact_eligible(
            use_compact_keys,
            use_compact_accs,
            n_workers,
            all_segments.len(),
            has_regexp_group,
            &needed_cols,
            &meta.col_types,
            &batch_quals,
        ) {
            let state = dispatch_parallel_compact_path(
                agg_specs,
                group_specs,
                &output_map,
                &having_filters,
                where_quals,
                topn_limit,
                topn_sort_col,
                topn_ascending,
                bare_limit,
                &meta,
                &mut all_segments,
                &needed_cols,
                &batch_quals,
                &seg_filters,
                time_min,
                time_max,
                n_workers,
                use_lazy,
                num_result_cols,
                metadata_us,
                heap_scan_us,
                t_wall,
                compact_storage.take(),
                total_detoast_us,
                total_cache_hits,
                total_cache_misses,
                total_cache_bytes_served,
            );
            let state_ptr = Box::into_raw(Box::new(state));
            (*node).custom_ps = state_ptr as *mut pg_sys::List;
            return;
        }

        // ============================================================
        // PARALLEL MIXED PATH: multi-threaded with string GROUP BY
        // ============================================================
        // Try to compile regexp patterns with Rust regex for thread-safe parallel execution
        let mut rust_regex_infos: Vec<RustRegexInfo> = Vec::new();
        if has_regexp_group {
            let regexp_count = group_specs
                .iter()
                .filter(|gs| matches!(gs.expr, GroupByExpr::RegexpReplace { .. }))
                .count();
            for gs in group_specs.iter() {
                if let GroupByExpr::RegexpReplace {
                    ref pattern,
                    ref replacement,
                    ..
                } = gs.expr
                    && let Some(compiled) = try_compile_rust_regex(pattern)
                {
                    let rust_replacement = convert_pg_replacement(replacement);
                    rust_regex_infos.push(RustRegexInfo {
                        regex: compiled,
                        replacement: rust_replacement,
                        col_idx: gs.col_idx as usize,
                    });
                }
            }
            if rust_regex_infos.len() != regexp_count {
                rust_regex_infos.clear(); // all-or-nothing: if any failed, fall back entirely
            }
        }
        let all_regexp_compiled = !has_regexp_group || !rust_regex_infos.is_empty();

        let mut mixed_col_not_null = meta.col_not_null.clone();
        mixed_col_not_null.resize(meta.col_names.len(), false);
        for gs in &group_specs {
            if !matches!(gs.expr, GroupByExpr::Extract { .. })
                || gs.col_idx < 0
                || (gs.col_idx as usize) >= meta.col_names.len()
            {
                continue;
            }
            let col_idx = gs.col_idx as usize;
            let col_name = &meta.col_names[col_idx];
            if all_segments.iter().all(|seg| {
                seg.col_sums
                    .get(col_name)
                    .is_some_and(|cs| cs.nonnull_count == seg.row_count as i64)
            }) {
                mixed_col_not_null[col_idx] = true;
            }
        }

        // The parallel-compact dispatch above runs and returns when its
        // gate passes, so reaching this point means compact was ineligible.
        let can_parallel_mixed_flag = has_group_by
            && (n_workers > 1 || derived_minmax_topn.is_some())
            && all_segments.len() > 1
            && all_regexp_compiled
            && can_parallel_mixed(
                &group_specs,
                &needed_cols,
                &meta.col_types,
                &mixed_col_not_null,
                &batch_quals,
                &agg_specs,
            );

        if can_parallel_mixed_flag {
            let state = dispatch_parallel_mixed_path(
                agg_specs,
                group_specs,
                &output_map,
                &having_filters,
                where_quals,
                topn_limit,
                topn_sort_col,
                topn_ascending,
                bare_limit,
                derived_minmax_topn,
                &meta,
                &mut all_segments,
                &needed_cols,
                &batch_quals,
                &seg_filters,
                &sidecar_only_cols,
                time_min,
                time_max,
                n_workers,
                use_lazy,
                num_result_cols,
                metadata_us,
                heap_scan_us,
                t_wall,
                rust_regex_infos,
                compact_storage.take(),
                total_detoast_us,
                total_cache_hits,
                total_cache_misses,
                total_cache_bytes_served,
            );
            let state_ptr = Box::into_raw(Box::new(state));
            (*node).custom_ps = state_ptr as *mut pg_sys::List;
            return;
        }

        // ============================================================
        // PARALLEL COUNT(DISTINCT) PATH: no GROUP BY, all aggs are
        // CountDistinct — parallelize by splitting segments across
        // threads, each builds local HashSets, then merge.
        // ============================================================
        if parallel_count_distinct_eligible(
            &agg_specs,
            &group_specs,
            &batch_quals,
            all_segments.len(),
            n_workers,
        ) {
            let state = dispatch_parallel_count_distinct_path(
                agg_specs,
                group_specs,
                &output_map,
                where_quals,
                topn_ascending,
                &meta,
                &mut all_segments,
                &needed_cols,
                &seg_filters,
                time_min,
                time_max,
                &count_distinct_only_str,
                &count_distinct_only_int,
                n_workers,
                use_lazy,
                num_result_cols,
                metadata_us,
                heap_scan_us,
                t_wall,
                &mut total_detoast_us,
                &mut total_cache_hits,
                &mut total_cache_misses,
                &mut total_cache_bytes_served,
            );
            let state_ptr = Box::into_raw(Box::new(state));
            (*node).custom_ps = state_ptr as *mut pg_sys::List;
            return;
        }

        // ============================================================
        // SINGLE-THREADED PATH (fall-through)
        // ============================================================
        let state = dispatch_serial_path(
            node,
            agg_specs,
            group_specs,
            output_map,
            having_filters,
            where_quals,
            topn_limit,
            topn_sort_col,
            topn_ascending,
            bare_limit,
            derived_minmax_topn,
            &meta,
            &mut all_segments,
            &needed_cols,
            &batch_quals,
            &seg_filters,
            time_min,
            time_max,
            use_lazy,
            num_result_cols,
            metadata_us,
            heap_scan_us,
            t_wall,
            has_group_by,
            is_single_group_key,
            use_compact_accs,
            use_compact_keys,
            has_regexp_group,
            text_group_cols,
            length_cols,
            sidecar_only_cols,
            count_distinct_only_str,
            count_distinct_only_int,
            compact_storage,
            total_detoast_us,
            total_cache_hits,
            total_cache_misses,
            total_cache_bytes_served,
        );
        let state_ptr = Box::into_raw(Box::new(state));
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}
/// ExecCustomScan callback for DeltaXAgg: return result rows.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn exec_agg_scan(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    unsafe {
        let scan_slot = (*node).ss.ss_ScanTupleSlot;
        let state = &mut *((*node).custom_ps as *mut AggScanState);

        // Phase C.2 activation: partial-mode runtime. Every process (leader
        // and workers) claims segments via the shared DSM cursor, builds a
        // process-local accumulator, finalises into per-group partial-state
        // rows, and emits them. PG's Gather concatenates rows from N
        // processes; the Final Aggregate above combines partials per group
        // via aggcombinefn. No DSM-slab serialise / spin-wait / leader
        // merge — each process is independent.
        let is_partial_mode = state
            .exec_ctx
            .as_ref()
            .map(|c| c.is_partial)
            .unwrap_or(false);
        if is_partial_mode {
            let already_done = state.exec_ctx.as_ref().map(|c| c.merged).unwrap_or(true);
            if !already_done {
                run_partial_aggregate_in_process(state);
            }
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
            pg_sys::ExecClearTuple(scan_slot);
            return scan_slot;
        }

        // Phase C.2.d: parallel workers run the claim+aggregate+serialise
        // loop on their first exec call. Subsequent calls return EOF — the
        // leader picks up partial slabs in C.2.e via a spin-wait on
        // `populated`. Workers emit no rows themselves.
        if state.is_parallel_worker {
            if state.exec_ctx.is_some() {
                let already_done = state
                    .exec_ctx
                    .as_ref()
                    .map(|c| c.worker_done)
                    .unwrap_or(true);
                if !already_done {
                    run_worker_partial_aggregate(state);
                }
            }
            pg_sys::ExecClearTuple(scan_slot);
            return scan_slot;
        }

        // Phase C.2.e: leader's first call in the parallel-aware path.
        // Claim segments alongside workers, spin-wait for worker partials,
        // deserialise + merge them into the leader-local accumulator,
        // finalise into `result_rows`. Subsequent calls fall through to the
        // shared row-emit loop below.
        if let Some(ctx) = state.exec_ctx.as_ref()
            && !ctx.merged
        {
            run_leader_merge_and_finalise(state);
        }

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

/// Phase C.2.e: leader's first-call body. Run the same chunked-claim loop as
/// workers (leader is slot 0), spin-wait for each worker slot's `populated`
/// flag with `Acquire`, deserialise each populated slab, merge into the
/// leader's global accumulator, finalise into `result_rows`, mark
/// `ctx.merged = true`. Subsequent exec calls emit cached rows.
///
/// # Safety
///
/// Dereferences `state.pscan` (raw `*const DeltaXAggPState`); reads
/// DSM slab bytes via raw pointer arithmetic. Caller must hold a live
/// reference to the parallel-coordinator state for the duration of
/// the call. Must run inside an active PG executor callback.
unsafe fn run_leader_merge_and_finalise(state: &mut AggScanState) {
    unsafe {
        if state.pscan.is_null() {
            return;
        }
        let total_segments;
        let n_worker_slots;
        {
            let ps = &*state.pscan;
            total_segments = ps.total_segments;
            n_worker_slots = ps.n_worker_slots as usize;
        }

        let ctx = match state.exec_ctx.as_mut() {
            Some(c) => c,
            None => return,
        };

        // Leader-local accumulator. Leader is slot 0 so it claims segments
        // alongside workers; that's fine — `next_segment.fetch_add` is
        // shared.
        let mut global = ParallelCompactResult::empty(&ctx.agg_specs);

        if total_segments > 0 {
            let cfg = ParallelCompactConfig {
                agg_specs: &ctx.agg_specs,
                group_specs: &ctx.group_specs,
                col_names: &ctx.meta.col_names,
                col_types: &ctx.meta.col_types,
                segment_by: &ctx.meta.segment_by,
                needed_cols: &ctx.needed_cols,
                batch_quals: &ctx.batch_quals,
                seg_filters: &ctx.seg_filters,
                time_min: ctx.time_min,
                time_max: ctx.time_max,
                topn_spec: ctx.topn_spec,
            };

            const CHUNK: u64 = 4;
            loop {
                let start = (*state.pscan)
                    .next_segment
                    .fetch_add(CHUNK, Ordering::Relaxed);
                if start >= total_segments {
                    break;
                }
                let end = (start + CHUNK).min(total_segments);
                let slice = &ctx.all_segments[start as usize..end as usize];
                let chunk_result = process_segments_compact(slice, &cfg);
                merge_compact_results(
                    &mut global.compact_map,
                    &mut global.compact_storage,
                    &mut global.cd_sidecar,
                    &chunk_result.compact_map,
                    &chunk_result.compact_storage,
                    &chunk_result.cd_sidecar,
                    &ctx.agg_specs,
                );
                global.segments_processed += chunk_result.segments_processed;
                global.rows_processed += chunk_result.rows_processed;
                global.decompress_us = global.decompress_us.max(chunk_result.decompress_us);
            }
        }

        // Spin-wait for each worker slot to publish its partial. Slot 0 is
        // the leader (us) so iterate 1..n_worker_slots. Spin-loop with a
        // backoff to avoid burning a core when all workers are CPU-bound on
        // their final merge.
        for slot in 1..n_worker_slots {
            let mut spin: u32 = 0;
            loop {
                let populated = (*state.pscan).worker_timings[slot].populated;
                // The populated field is a plain u32; emit an Acquire fence
                // after observing 1 so the slab + partial_lens writes
                // become visible. (worker_timings isn't itself atomic to
                // keep the struct POD-zeroable.)
                if populated == 1 {
                    std::sync::atomic::fence(Ordering::Acquire);
                    break;
                }
                spin = spin.saturating_add(1);
                if spin < 1024 {
                    std::hint::spin_loop();
                } else {
                    std::thread::yield_now();
                    spin = 0;
                }
            }
        }

        // Deserialise each populated slab and merge into the leader's global.
        for slot in 1..n_worker_slots {
            let len = (*state.pscan).partial_lens[slot].load(Ordering::Acquire);
            if len == 0 {
                continue; // empty/unused slot
            }
            let slab_ptr = (*state.pscan).slab_ptr(slot);
            match agg_wire::deserialize_partial(slab_ptr, len, &ctx.agg_specs) {
                Ok(worker) => {
                    merge_compact_results(
                        &mut global.compact_map,
                        &mut global.compact_storage,
                        &mut global.cd_sidecar,
                        &worker.compact_map,
                        &worker.compact_storage,
                        &worker.cd_sidecar,
                        &ctx.agg_specs,
                    );
                    global.segments_processed += worker.segments_processed;
                    global.rows_processed += worker.rows_processed;
                    global.decompress_us = global.decompress_us.max(worker.decompress_us);
                }
                Err(e) => {
                    pgrx::error!(
                        "pg_deltax: failed to deserialise worker slot {} partial: {:?}",
                        slot,
                        e,
                    );
                }
            }
        }

        // Finalise into result_rows. When ctx.is_partial, emit each agg
        // slot's PG aggtranstype value (combined upstream by Final Agg);
        // otherwise emit the user-visible final value.
        let result_rows = finalise_compact_into_result_rows(
            &global.compact_map,
            &global.compact_storage,
            &ctx.agg_specs,
            &ctx.group_specs,
            &ctx.output_map,
            ctx.num_result_cols,
            ctx.is_partial,
        );

        state.total_segments = global.segments_processed;
        state.total_rows_processed = global.rows_processed;
        state.decompress_us = global.decompress_us;
        state.result_rows = result_rows;
        state.result_idx = 0;
        ctx.merged = true;
    }
}

/// Iterate `global_map`'s groups and turn each one into an output row using
/// `compact_finalize` per agg spec and `output_map` for column placement.
/// Mirrors the post-merge emit loop in `begin_agg_scan`'s rayon path.
///
/// The parallel-aware path's eligibility (C.2.f) excludes HAVING, Top-N, and
/// CountDistinct, so this is the simplest variant — no filter pass, no sort,
/// no special-case finalisation.
///
/// When `is_partial = true` (Phase C.2 activation), each agg slot is emitted
/// via `compact_emit_partial` (returning the PG `aggtranstype` value) instead
/// of the user-visible final value — a Final Aggregate node above DeltaXAgg
/// then combines the partials via `aggcombinefn`.
///
/// # Safety
///
/// Inherits the `CompactAccStorage` accessor contract. Must run inside
/// an active PG transaction (finalize allocates datums).
unsafe fn finalise_compact_into_result_rows(
    global_map: &CompactGroupMap,
    global_storage: &CompactAccStorage,
    agg_specs: &[AggExecSpec],
    group_specs: &[GroupByColSpec],
    output_map: &[OutputEntry],
    num_result_cols: usize,
    is_partial: bool,
) -> Vec<Vec<(pg_sys::Datum, bool)>> {
    unsafe {
        let num_group_keys = group_specs.len();
        let mut rows: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::with_capacity(global_map.len());
        for (&packed_key, &group_idx) in global_map {
            let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(agg_specs.len());
            for (spec_idx, spec) in agg_specs.iter().enumerate() {
                let val = if is_partial {
                    compact_emit_partial(global_storage, group_idx, spec_idx, spec)
                } else {
                    compact_finalize(global_storage, group_idx, spec_idx, spec)
                };
                agg_results.push(val);
            }
            let keys = unpack_int_keys(packed_key, num_group_keys);
            let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
            for entry in output_map {
                match entry {
                    OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                    OutputEntry::Group(gi) => {
                        let v = keys[*gi];
                        if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. }) {
                            row.push((i128_to_numeric_datum(v as i128), false));
                        } else {
                            row.push((pg_sys::Datum::from(v as usize), false));
                        }
                    }
                    OutputEntry::DerivedGroup { base_gi, delta } => {
                        let v = keys[*base_gi] + delta;
                        row.push((pg_sys::Datum::from(v as usize), false));
                    }
                    OutputEntry::Const(d, n) => row.push((*d, *n)),
                }
            }
            rows.push(row);
        }
        rows
    }
}

/// Phase C.2 activation — partial-mode in-process aggregation. Every
/// process (leader and workers) calls this on its first `exec_agg_scan`
/// invocation. Claims segments via the shared DSM cursor, accumulates into
/// a process-local `ParallelCompactResult`, then finalises into
/// `state.result_rows` using `compact_emit_partial` per slot. PG's Gather
/// and the Final Aggregate above combine partials per group via the
/// aggregate's `aggcombinefn`.
///
/// Differs from `run_worker_partial_aggregate` (complete-mode workers
/// writing to DSM) and `run_leader_merge_and_finalise` (complete-mode
/// leader merging worker DSM slabs) — neither is invoked in partial mode
/// because there's no inter-process DSM merge step.
///
/// # Safety
///
/// Dereferences `state.pscan` and inherits the `CompactAccStorage`
/// accessor contract. Must run inside an active PG executor callback.
unsafe fn run_partial_aggregate_in_process(state: &mut AggScanState) {
    unsafe {
        if state.pscan.is_null() {
            return;
        }
        let total_segments = (*state.pscan).total_segments;

        let ctx = match state.exec_ctx.as_mut() {
            Some(c) => c,
            None => return,
        };

        let mut local = ParallelCompactResult::empty(&ctx.agg_specs);

        if total_segments > 0 {
            let cfg = ParallelCompactConfig {
                agg_specs: &ctx.agg_specs,
                group_specs: &ctx.group_specs,
                col_names: &ctx.meta.col_names,
                col_types: &ctx.meta.col_types,
                segment_by: &ctx.meta.segment_by,
                needed_cols: &ctx.needed_cols,
                batch_quals: &ctx.batch_quals,
                seg_filters: &ctx.seg_filters,
                time_min: ctx.time_min,
                time_max: ctx.time_max,
                topn_spec: None,
            };
            const CHUNK: u64 = 4;
            loop {
                let start = (*state.pscan)
                    .next_segment
                    .fetch_add(CHUNK, Ordering::Relaxed);
                if start >= total_segments {
                    break;
                }
                let end = (start + CHUNK).min(total_segments);
                let slice = &ctx.all_segments[start as usize..end as usize];
                let chunk_result = process_segments_compact(slice, &cfg);
                merge_compact_results(
                    &mut local.compact_map,
                    &mut local.compact_storage,
                    &mut local.cd_sidecar,
                    &chunk_result.compact_map,
                    &chunk_result.compact_storage,
                    &chunk_result.cd_sidecar,
                    &ctx.agg_specs,
                );
                local.segments_processed += chunk_result.segments_processed;
                local.rows_processed += chunk_result.rows_processed;
                local.decompress_us = local.decompress_us.max(chunk_result.decompress_us);
            }
        }

        // Finalise into per-group partial-state rows. is_partial=true →
        // compact_emit_partial chooses the right `aggtranstype` value per
        // slot; PG's Final Aggregate above combines via aggcombinefn.
        let result_rows = finalise_compact_into_result_rows(
            &local.compact_map,
            &local.compact_storage,
            &ctx.agg_specs,
            &ctx.group_specs,
            &ctx.output_map,
            ctx.num_result_cols,
            /* is_partial */ true,
        );

        state.total_segments = local.segments_processed;
        state.total_rows_processed = local.rows_processed;
        state.decompress_us = local.decompress_us;
        state.result_rows = result_rows;
        state.result_idx = 0;
        ctx.merged = true;
    }
}

/// Phase C.2.d: claim segments from the shared cursor, run
/// `process_segments_compact` on each chunk, accumulate into a worker-local
/// `ParallelCompactResult`, then serialise the result into the worker's DSM
/// slab. The leader's `exec_agg_scan` first call (Phase C.2.e) reads back
/// the slab via `agg_wire::deserialize_partial`.
///
/// Chunked claim (`CHUNK = 4`) amortises atomic cache-line bouncing across N
/// workers; load imbalance is bounded by `CHUNK` segments. Empty-claim case
/// (cursor already exhausted) is handled naturally — `serialize_partial_into`
/// emits a header-only ~96-byte wire that the leader's deserialiser turns
/// into an empty result with no merge effect.
///
/// Memory ordering: `partial_lens[slot]` is written with `Release` here;
/// `shutdown_deltax_agg` later writes `worker_timings[slot].populated = 1`
/// (also Release). The leader's `Acquire` load on `populated` synchronises
/// with both writes in program order.
///
/// # Safety
///
/// Dereferences `state.pscan` and writes into DSM via raw pointers.
/// Inherits the `CompactAccStorage` accessor contract. Must run inside
/// an active PG worker executor callback.
unsafe fn run_worker_partial_aggregate(state: &mut AggScanState) {
    unsafe {
        const CHUNK: u64 = 4;

        if state.pscan.is_null() {
            return;
        }
        let ps = &*state.pscan;
        let total_segments = ps.total_segments;
        let slab_cap = ps.partial_slab_size;

        let slot = current_agg_worker_slot();
        if slot >= ps.n_worker_slots as usize {
            return;
        }
        let slab_ptr = ps.slab_ptr(slot);

        let ctx = match state.exec_ctx.as_mut() {
            Some(c) => c,
            None => return,
        };

        let mut local = ParallelCompactResult::empty(&ctx.agg_specs);

        if total_segments > 0 {
            let cfg = ParallelCompactConfig {
                agg_specs: &ctx.agg_specs,
                group_specs: &ctx.group_specs,
                col_names: &ctx.meta.col_names,
                col_types: &ctx.meta.col_types,
                segment_by: &ctx.meta.segment_by,
                needed_cols: &ctx.needed_cols,
                batch_quals: &ctx.batch_quals,
                seg_filters: &ctx.seg_filters,
                time_min: ctx.time_min,
                time_max: ctx.time_max,
                topn_spec: ctx.topn_spec,
            };

            loop {
                let start = ps.next_segment.fetch_add(CHUNK, Ordering::Relaxed);
                if start >= total_segments {
                    break;
                }
                let end = (start + CHUNK).min(total_segments);
                let slice = &ctx.all_segments[start as usize..end as usize];
                let chunk_result = process_segments_compact(slice, &cfg);

                merge_compact_results(
                    &mut local.compact_map,
                    &mut local.compact_storage,
                    &mut local.cd_sidecar,
                    &chunk_result.compact_map,
                    &chunk_result.compact_storage,
                    &chunk_result.cd_sidecar,
                    &ctx.agg_specs,
                );
                local.segments_processed += chunk_result.segments_processed;
                local.rows_processed += chunk_result.rows_processed;
                local.decompress_us = local.decompress_us.max(chunk_result.decompress_us);
            }
        }

        match agg_wire::serialize_partial_into(slab_ptr, slab_cap, &local, &ctx.agg_specs) {
            Ok(written) => {
                // SAFETY: Release-store `partial_lens[slot]` after the slab
                // bytes are written so the leader's Acquire-load on
                // `populated` (in `shutdown_deltax_agg`) makes the slab
                // contents visible.
                (*state.pscan).partial_lens[slot].store(written as u64, Ordering::Release);
                ctx.worker_done = true;
            }
            Err(agg_wire::SerError::Overflow { needed, have }) => {
                pgrx::error!(
                    "pg_deltax: parallel-agg partial slab overflow ({} bytes needed, {} available); \
                     spill to per-worker tuplestore is Phase F",
                    needed,
                    have,
                );
            }
        }
    }
}

/// EndCustomScan callback for DeltaXAgg.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn end_agg_scan(node: *mut pg_sys::CustomScanState) {
    unsafe {
        let state_ptr = (*node).custom_ps as *mut AggScanState;
        if !state_ptr.is_null() {
            let state = Box::from_raw(state_ptr);
            let total_us = state.wall_us;
            pgrx::log!(
                "pg_deltax DeltaXAgg timing: total={:.1}ms  metadata={:.1}ms  heap_scan={:.1}ms  [detoast={:.1}ms]  \
                 decompress={:.1}ms  agg={:.1}ms  merge={:.1}ms  finalize={:.1}ms  topn_select={:.1}ms  | \
                 workers={} segments={} rows_processed={} groups={} result_rows={} topn_limit={} bare_limit={} f8_preselected={}",
                total_us as f64 / 1000.0,
                state.metadata_us as f64 / 1000.0,
                state.heap_scan_us as f64 / 1000.0,
                state.detoast_us as f64 / 1000.0,
                state.decompress_us as f64 / 1000.0,
                state.agg_us as f64 / 1000.0,
                state.merge_us as f64 / 1000.0,
                state.finalize_us as f64 / 1000.0,
                state.topn_select_us as f64 / 1000.0,
                state.n_workers,
                state.total_segments,
                state.total_rows_processed,
                state.pre_topn_groups,
                state.result_rows.len(),
                state.topn_limit,
                state.bare_limit,
                state.f8_preselected,
            );
            (*node).custom_ps = std::ptr::null_mut();
        }
    }
}

/// ReScanCustomScan callback for DeltaXAgg.
///
/// For parallel-aware scans, also clears `result_rows` and `exec_ctx.merged`
/// so the next exec re-runs the full claim+merge cycle. PG calls
/// `reinit_dsm_deltax_agg` separately to zero the cursor + per-slot state in
/// shared memory.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn rescan_agg_scan(node: *mut pg_sys::CustomScanState) {
    unsafe {
        let state = &mut *((*node).custom_ps as *mut AggScanState);
        state.result_idx = 0;
        if let Some(ctx) = state.exec_ctx.as_mut() {
            ctx.merged = false;
            ctx.worker_done = false;
            state.result_rows.clear();
        }
    }
}
