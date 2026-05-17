mod cd_set;
mod extract;
mod keys;
mod metadata;
mod parallel_cd;
mod parallel_compact;
mod parser;
mod regex;
mod state;

use self::regex::{
    RustRegexInfo, apply_case_when_to_seg_col, apply_regex_to_seg_col, convert_pg_replacement,
    try_compile_rust_regex,
};
use cd_set::hash128_str;
#[cfg(any(test, feature = "pg_test"))]
use cd_set::{new_cd_set_int, new_cd_set_str};
use extract::constant_extract_key_for_segment;
pub(crate) use extract::eval_extract;
pub(crate) use keys::{CompactGroupMap, can_use_compact_keys_path};
use keys::{can_use_compact_keys, pack_int_key_1, pack_int_keys_2, unpack_int_keys};
use metadata::{load_agg_metadata_from_plan, try_catalog_shortcut, try_metadata_fast_path};
use parallel_cd::{dispatch_parallel_count_distinct_path, parallel_count_distinct_eligible};
pub(crate) use parallel_compact::ParallelCompactResult;
use parallel_compact::{
    ParallelCompactConfig, decompress_numeric_blob, dispatch_parallel_compact_path,
    is_numeric_type, merge_compact_results, parallel_compact_eligible, parse_string_to_datum,
    process_segments_compact,
};
use parser::{
    build_agg_exec_context_from_plan, build_deferred_agg_state, build_minimal_worker_state,
    parse_agg_private,
};
use state::{
    AggAccumulator, AggTimingShmem, DeltaXAggPState, OutputEntry, PARTIAL_SLAB_SIZE_BYTES,
    ParsedAggPlan,
};
pub(crate) use state::{
    AggExecSpec, AggExpr, AggScanState, AggType, CaseWhenClause, CaseWhenCondition, CaseWhenOp,
    CaseWhenSpec, CaseWhenValue, GroupByColSpec, GroupByExpr, HavingFilter, HavingOp,
    MAX_AGG_WORKER_SLOTS, OutputTransform,
};

use pgrx::pg_guard;
use pgrx::pg_sys;

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::super::SyncStatic;
use super::batch_qual::{BatchCompareOp, BatchQual, evaluate_batch_quals, extract_batch_quals};
use super::datum_utils::{
    collation_strcmp, count_non_null, decompress_blob_to_datums, decompress_text_blob_to_lengths,
    decompress_text_blob_to_raw_strings, decompress_text_blob_with_eq_filter,
    decompress_text_blob_with_in_filter, decompress_text_blob_with_like_filter, pg_type_name,
    string_to_datum,
};
use super::segments::{
    SegmentData, SegmentQualResult, classify_segment_quals_numeric, detoast_lazy_blobs,
    extract_segment_filters, load_segments_heap, segment_skippable_by_dict,
};
use super::text_col::{
    SegTextColumn, TextQualInfo, apply_text_eq_filter, apply_text_in_filter,
    apply_text_like_filter, decompress_length_sidecar, decompress_text_to_seg_col, strcoll_cmp,
};
use crate::compression;

/// EstimateDSMCustomScan: bytes for `DeltaXAggPState` + N+1 partial-state
/// slabs (one per leader/worker). The slab count is fixed at the cap so
/// re-sizing isn't needed if the planner picks a smaller worker count
/// later — wasted DSM bytes in that case are bounded by
/// `PARTIAL_SLAB_SIZE_BYTES * (MAX_AGG_WORKER_SLOTS - 1 - nworkers)`.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn estimate_dsm_deltax_agg(
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
pub(super) unsafe extern "C-unwind" fn initialize_dsm_deltax_agg(
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
pub(super) unsafe extern "C-unwind" fn reinit_dsm_deltax_agg(
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
pub(super) unsafe extern "C-unwind" fn init_worker_deltax_agg(
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

        super::segments::reset_scan_buf_stats();
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
pub(super) unsafe extern "C-unwind" fn shutdown_deltax_agg(node: *mut pg_sys::CustomScanState) {
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
        CustomName: super::super::DELTAX_AGG_NAME.as_ptr(),
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
        ExplainCustomScan: Some(super::super::explain::explain_agg_scan),
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
pub(super) unsafe extern "C-unwind" fn begin_agg_scan(
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
            super::segments::reset_scan_buf_stats();
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
        super::segments::reset_scan_buf_stats();
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
                .map(|&oid| super::super::cost::get_row_count(oid))
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
            .map(|&oid| super::super::cost::get_row_count(oid).unwrap_or(0))
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
                let sidecar_detoast_us = super::segments::load_text_length_sidecars(
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

        // Create per-segment memory context
        let query_ctx = (*(*node).ss.ps.state).es_query_cxt;
        let segment_mcxt = pg_sys::AllocSetContextCreateInternal(
            query_ctx,
            c"DeltaXAggSegment".as_ptr(),
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
            Some(
                prototype_accumulators
                    .iter()
                    .map(|a| a.clone_fresh())
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };
        let mut group_map: GroupMap = GroupMap::with_hasher(BuildHasherDefault::default());
        let mut string_arena = StringArena::new();
        // Flat accumulator storage: group i's accumulators are at
        // flat_accs[i * n_agg_specs .. (i+1) * n_agg_specs]
        let n_agg_specs = agg_specs.len();
        let mut flat_accs: Vec<AggAccumulator> = Vec::new();
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
        let mut compact_group_map: CompactGroupMap =
            CompactGroupMap::with_hasher(BuildHasherDefault::default());
        let mut cd_sidecar = CountDistinctSideCar::new(&agg_specs);

        // Check if any GROUP BY uses RegexpReplace — set up cross-segment caches
        let has_regexp_group = group_specs
            .iter()
            .any(|gs| matches!(gs.expr, GroupByExpr::RegexpReplace { .. }));

        // Cross-segment regex dedup cache: input_string → regexp_replace result
        let mut regex_cache: HashMap<String, String> = HashMap::new();
        let mut regex_cache_calls: u64 = 0;

        // Build PG datums for regexp pattern/replacement (once, not per-segment)
        // and identify which columns need raw string decompression
        struct RegexpGroupInfo {
            group_idx: usize,
            func_oid: pg_sys::Oid,
            collation: pg_sys::Oid,
            pattern_datum: pg_sys::Datum,
            replacement_datum: pg_sys::Datum,
        }
        let mut regexp_group_infos: Vec<RegexpGroupInfo> = Vec::new();
        // Columns that need raw string decompression instead of PG datum decompression
        let mut raw_string_cols: Vec<bool> = vec![false; meta.col_names.len()];

        if has_regexp_group {
            for (gi, gs) in group_specs.iter().enumerate() {
                if let GroupByExpr::RegexpReplace {
                    ref pattern,
                    ref replacement,
                    func_oid,
                    collation,
                } = gs.expr
                {
                    raw_string_cols[gs.col_idx as usize] = true;
                    let pattern_datum = {
                        let text = pg_sys::cstring_to_text_with_len(
                            pattern.as_ptr() as *const _,
                            pattern.len() as i32,
                        );
                        pg_sys::Datum::from(text as usize)
                    };
                    let replacement_datum = {
                        let text = pg_sys::cstring_to_text_with_len(
                            replacement.as_ptr() as *const _,
                            replacement.len() as i32,
                        );
                        pg_sys::Datum::from(text as usize)
                    };
                    regexp_group_infos.push(RegexpGroupInfo {
                        group_idx: gi,
                        func_oid: pg_sys::Oid::from(func_oid),
                        collation: pg_sys::Oid::from(collation),
                        pattern_datum,
                        replacement_datum,
                    });
                }
            }
        }

        // Mark text columns used for Min/Max aggregations as raw_string_cols
        // only when the compact accumulator path is active (MinStr/MaxStr need raw bytes).
        // The generic path handles MIN/MAX text via normal PG datums.
        if use_compact_accs {
            for spec in &agg_specs {
                if matches!(spec.agg_type, AggType::Min | AggType::Max) {
                    let t = spec.col_type_oid;
                    if t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID {
                        raw_string_cols[spec.col_idx as usize] = true;
                    }
                }
            }
        }

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

        // Per-segment decoded text data for GROUP BY columns.
        // Keeps decompressed string data alive during the row loop,
        // providing O(1) &str access per row without interning.
        let mut seg_text_columns: Vec<Option<SegTextColumn>> = Vec::new();

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
        // Replaces the prior `!can_parallel` reference on the inline gate.
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
            let t2 = Instant::now();
            // For derived MIN/MAX-difference top-N, workers don't maintain a
            // direct-sort heap — sort key is recovered at the leader from
            // partial max/min, see the `derived_minmax_topn` branch below.
            let topn_spec =
                if topn_limit > 0 && having_filters.is_empty() && derived_minmax_topn.is_none() {
                    let sort_slot = match output_map[topn_sort_col] {
                        OutputEntry::Agg(ai) => ai,
                        _ => unreachable!(),
                    };
                    // AVG sort can't use raw sum for speculative top-K pruning
                    if agg_specs[sort_slot].agg_type == AggType::Avg {
                        None
                    } else {
                        let k = (topn_limit as usize).max(1000);
                        Some((sort_slot, k, topn_ascending))
                    }
                } else {
                    None
                };

            // Build text_group_col_flags
            let mut text_group_col_flags: Vec<bool> = (0..meta.col_names.len())
                .map(|i| {
                    group_specs
                        .iter()
                        .any(|gs| gs.col_idx as usize == i && is_text_group_col(gs))
                })
                .collect();
            // CaseWhen ColumnRef results reference text columns that need decompression
            for gs in &group_specs {
                if let GroupByExpr::CaseWhen(ref spec) = gs.expr {
                    for clause in &spec.clauses {
                        if let CaseWhenValue::ColumnRef(ci) = &clause.result
                            && *ci < text_group_col_flags.len()
                        {
                            text_group_col_flags[*ci] = true;
                        }
                    }
                    if let CaseWhenValue::ColumnRef(ci) = &spec.default
                        && *ci < text_group_col_flags.len()
                    {
                        text_group_col_flags[*ci] = true;
                    }
                }
            }

            // Build text qual infos for worker threads.
            // Order: positive LIKE first (most selective — match a pattern),
            // then EQ/NE, then NOT LIKE (negated patterns pass most rows).
            // This maximizes short-circuit benefit when filters AND into selection.
            let mut text_qual_infos: Vec<TextQualInfo> = Vec::new();
            for bq in &batch_quals {
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
                        BatchCompareOp::InList => {
                            if let Some(ref vals) = bq.in_list_text {
                                text_qual_infos.push(TextQualInfo::InList {
                                    col_idx: bq.col_idx,
                                    values: vals.clone(),
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }
            // Reorder: cheap filters first to maximize short-circuit benefit.
            // EQ/NE are O(1) per row; InList against a small list is also O(1)
            // in the dict fast path; LIKE requires substring search.
            text_qual_infos.sort_by_key(|tqi| match tqi {
                TextQualInfo::EqNe { .. } => 0,                // EQ/NE — cheapest
                TextQualInfo::InList { .. } => 1,              // dict-keyed IN
                TextQualInfo::Like { negate: false, .. } => 2, // positive LIKE
                TextQualInfo::Like { negate: true, .. } => 3,  // NOT LIKE
            });

            // Pipeline detoast with parallel processing when enough segments.
            // Use fewer batches than the compact path (2 vs n_workers*2) because
            // the mixed path processes text columns which have high per-segment
            // cost. Fewer batches = fewer thread scope synchronization points.
            //
            // Gate on `!batch_quals.is_empty()`: pipeline overlap only pays off
            // when workers spend non-trivial time per segment (filter eval, more
            // complex aggregation) so the leader can usefully detoast the next
            // batch in parallel. Unfiltered text-grouping queries
            // (e.g. ClickBench Q18) have workers blast through segments
            // faster than detoast can keep up — the per-iteration thread::scope
            // synchronization is then pure overhead. Empirically on ClickBench:
            // pipeline gives ~+0.8s on Q22-class (heavy filter), ~-2.3s on
            // Q18-class (no filter). Net positive on filtered, net negative on
            // unfiltered.
            let use_pipeline =
                use_lazy && all_segments.len() >= n_workers * 16 && !batch_quals.is_empty();
            // Pipeline uses 2 batches: workers run on current batch while leader
            // detoasts the next. Pre-detoast must cover the *entire* first batch
            // before workers spawn, otherwise workers on un-detoasted segments
            // see empty blobs → SegTextColumn::Lz4 returns None → group keys
            // collapse to NULL and distinct group values are lost. Must match
            // PIPELINE_N_BATCHES below.
            const PIPELINE_N_BATCHES: usize = 2;

            if use_lazy {
                let t_detoast = Instant::now();
                if use_pipeline {
                    let n_batches = PIPELINE_N_BATCHES.min(all_segments.len());
                    let batch_size = all_segments.len().div_ceil(n_batches);
                    let first_end = batch_size.min(all_segments.len());
                    for seg in &mut all_segments[..first_end] {
                        let dl = detoast_lazy_blobs(seg);
                        total_cache_hits += dl.cache_hits;
                        total_cache_misses += dl.cache_misses;
                        total_cache_bytes_served += dl.cache_bytes_served;
                    }
                } else {
                    for seg in &mut all_segments {
                        let dl = detoast_lazy_blobs(seg);
                        total_cache_hits += dl.cache_hits;
                        total_cache_misses += dl.cache_misses;
                        total_cache_bytes_served += dl.cache_bytes_served;
                    }
                }
                total_detoast_us += t_detoast.elapsed().as_micros() as u64;
            }

            // F8 Phase 0: when the query shape matches (bare LIMIT, no WHERE,
            // no HAVING, no non-trivial group-by expressions), pre-select
            // `bare_limit` distinct group-key hashes from the first few
            // segments. Workers will then filter phase-1 rows by this set,
            // bounding each worker's hash map to at most `bare_limit` entries.
            // Counts stay exact because every matching row is still counted.
            let has_case_when_group = group_specs
                .iter()
                .any(|gs| matches!(gs.expr, GroupByExpr::CaseWhen(_)));
            let has_regex_group = group_specs
                .iter()
                .any(|gs| matches!(gs.expr, GroupByExpr::RegexpReplace { .. }));
            let preselected_keys: Option<hashbrown::HashSet<u128>> = if bare_limit > 0
                && having_filters.is_empty()
                && batch_quals.is_empty()
                && where_quals.is_null()
                && !has_case_when_group
                && !has_regex_group
            {
                try_build_preselected(
                    bare_limit as usize,
                    &all_segments,
                    &group_specs,
                    &meta.col_names,
                    &meta.col_types,
                    &meta.segment_by,
                    &needed_cols,
                    &text_group_col_flags,
                    /* max_probe_segments */ 4,
                )
            } else {
                None
            };

            // Phase D leader pre-pass: build per-spec dict-distinct remaps
            // for every eligible CountDistinct(text) spec. Workers consult
            // these through `ParallelMixedConfig::dict_distinct_remaps` to
            // set bits in per-(spec, group) bitsets instead of hashing
            // strings into HashSet<u128>. Specs not in the map fall back to
            // the existing HashSet path. Sequential today — see
            // `build_dict_distinct_remaps` for the cost/threshold logic.
            let dict_distinct_remaps = build_dict_distinct_remaps(&all_segments, &agg_specs);

            let config = ParallelMixedConfig {
                agg_specs: &agg_specs,
                group_specs: &group_specs,
                col_names: &meta.col_names,
                col_types: &meta.col_types,
                segment_by: &meta.segment_by,
                needed_cols: &needed_cols,
                batch_quals: &batch_quals,
                seg_filters: &seg_filters,
                time_min,
                time_max,
                topn_spec,
                text_group_col_flags: &text_group_col_flags,
                text_qual_infos: &text_qual_infos,
                rust_regex_infos: &rust_regex_infos,
                sidecar_only_cols: &sidecar_only_cols,
                preselected_keys: preselected_keys.as_ref(),
                dict_distinct_remaps: &dict_distinct_remaps,
            };

            let mut pipeline_detoast_us: u64 = 0;
            let partial_results: Vec<ParallelMixedResult> = if use_pipeline {
                let n_batches = PIPELINE_N_BATCHES.min(all_segments.len());
                let batch_size = all_segments.len().div_ceil(n_batches);
                let mut results: Vec<ParallelMixedResult> = Vec::new();
                let mut batch_start = 0;
                let total_segs = all_segments.len();

                while batch_start < total_segs {
                    let batch_end = (batch_start + batch_size).min(total_segs);
                    let next_end = (batch_end + batch_size).min(total_segs);

                    let (done, pending) = all_segments.split_at_mut(batch_end);
                    let current_batch = &done[batch_start..];

                    std::thread::scope(|s| {
                        let chunk_size = current_batch.len().div_ceil(n_workers);
                        let handles: Vec<_> = current_batch
                            .chunks(chunk_size)
                            .enumerate()
                            .map(|(ci, chunk)| {
                                let cfg = &config;
                                // Phase D: chunk_offset is the seg_idx of chunk[0]
                                // in the leader's all_segments view, used to index
                                // dict_distinct_remaps.per_segment.
                                let chunk_offset = batch_start + ci * chunk_size;
                                s.spawn(move || process_segments_mixed(chunk, chunk_offset, cfg))
                            })
                            .collect();

                        if batch_end < total_segs {
                            let t_pd = Instant::now();
                            for seg in &mut pending[..next_end - batch_end] {
                                let dl = detoast_lazy_blobs(seg);
                                total_cache_hits += dl.cache_hits;
                                total_cache_misses += dl.cache_misses;
                                total_cache_bytes_served += dl.cache_bytes_served;
                            }
                            pipeline_detoast_us += t_pd.elapsed().as_micros() as u64;
                        }

                        for h in handles {
                            results.push(h.join().unwrap());
                        }
                    });

                    batch_start = batch_end;
                }
                results
            } else {
                let chunk_size = all_segments.len().div_ceil(n_workers);
                std::thread::scope(|s| {
                    let handles: Vec<_> = all_segments
                        .chunks(chunk_size)
                        .enumerate()
                        .map(|(ci, chunk)| {
                            let cfg = &config;
                            let chunk_offset = ci * chunk_size;
                            s.spawn(move || process_segments_mixed(chunk, chunk_offset, cfg))
                        })
                        .collect();
                    handles.into_iter().map(|h| h.join().unwrap()).collect()
                })
            };

            // Accumulate stats
            let scan_wall_us = t2.elapsed().as_micros() as u64;
            let mut total_segments: u64 = 0;
            let mut total_rows_processed: u64 = 0;
            let mut decompress_us: u64 = 0;
            for result in &partial_results {
                total_segments += result.segments_processed;
                total_rows_processed += result.rows_processed;
                decompress_us = decompress_us.max(result.decompress_us);
            }
            total_detoast_us += pipeline_detoast_us;
            let agg_us = scan_wall_us.saturating_sub(decompress_us + pipeline_detoast_us);

            // ----------------------------------------------------------
            // Derived MIN/MAX-difference top-N (JSONBench Q4 shape):
            // sort by `storage[max_slot] - storage[min_slot]`. Workers
            // have produced partial MAX/MIN per group; one pass over all
            // (worker, hash) pairs combines partials into per-key
            // global MAX/MIN, applies a top-K heap on `max - min`, and
            // then merges *only the K winners'* full accumulators.
            // Skips the full 1.35M-group merge + finalize that the
            // direct-aggregate paths below would otherwise do.
            // ----------------------------------------------------------
            if let Some((max_slot, min_slot)) = derived_minmax_topn {
                let t_merge = Instant::now();
                let limit = topn_limit as usize;
                let ascending = topn_ascending;

                // Step 1+2: partition worker-local groups by hash, merge
                // each partition's MAX/MIN in parallel, and keep only local
                // top-K candidates. The old implementation built one large
                // leader HashMap and then scanned it; Q4 spends hundreds of
                // milliseconds there with ~1.35M distinct users.
                let n_partitions = n_workers.max(1);
                let mut buckets: Vec<Vec<(usize, u128, u32)>> =
                    (0..n_partitions).map(|_| Vec::new()).collect();
                for (wi, result) in partial_results.iter().enumerate() {
                    for (&hash_key, &wgidx) in &result.compact_map {
                        let p = (((hash_key as u64) ^ ((hash_key >> 64) as u64)) as usize)
                            % n_partitions;
                        buckets[p].push((wi, hash_key, wgidx));
                    }
                }

                let partition_results: Vec<(usize, Vec<(i128, u128)>)> = std::thread::scope(|s| {
                    let workers = &partial_results;
                    let handles: Vec<_> = buckets
                        .into_iter()
                        .map(|bucket| {
                            s.spawn(move || {
                                let mut per_key: hashbrown::HashMap<
                                    u128,
                                    (i64, i64, bool),
                                    BuildHasherDefault<ahash::AHasher>,
                                > = hashbrown::HashMap::with_capacity_and_hasher(
                                    bucket.len(),
                                    Default::default(),
                                );

                                for (wi, hash_key, wgidx) in bucket {
                                    let worker = &workers[wi];
                                    let (max_val, max_has) =
                                        worker.compact_storage.read_min_max_int(wgidx, max_slot);
                                    let (min_val, min_has) =
                                        worker.compact_storage.read_min_max_int(wgidx, min_slot);
                                    let entry = per_key.entry(hash_key).or_insert((
                                        i64::MIN,
                                        i64::MAX,
                                        false,
                                    ));
                                    if max_has && (!entry.2 || max_val > entry.0) {
                                        entry.0 = max_val;
                                        entry.2 = true;
                                    }
                                    if min_has && (entry.1 == i64::MAX || min_val < entry.1) {
                                        entry.1 = min_val;
                                    }
                                }

                                let unique_count = per_key.len();
                                if ascending {
                                    let mut heap: std::collections::BinaryHeap<(i128, u128)> =
                                        std::collections::BinaryHeap::with_capacity(limit + 1);
                                    for (&hash_key, &(max_val, min_val, seen)) in &per_key {
                                        if !seen {
                                            continue;
                                        }
                                        let derived =
                                            (max_val as i128).saturating_sub(min_val as i128);
                                        heap.push((derived, hash_key));
                                        if heap.len() > limit {
                                            heap.pop();
                                        }
                                    }
                                    (unique_count, heap.into_vec())
                                } else {
                                    let mut heap: std::collections::BinaryHeap<
                                        Reverse<(i128, u128)>,
                                    > = std::collections::BinaryHeap::with_capacity(limit + 1);
                                    for (&hash_key, &(max_val, min_val, seen)) in &per_key {
                                        if !seen {
                                            continue;
                                        }
                                        let derived =
                                            (max_val as i128).saturating_sub(min_val as i128);
                                        heap.push(Reverse((derived, hash_key)));
                                        if heap.len() > limit {
                                            heap.pop();
                                        }
                                    }
                                    (
                                        unique_count,
                                        heap.into_iter()
                                            .map(|Reverse(candidate)| candidate)
                                            .collect(),
                                    )
                                }
                            })
                        })
                        .collect();

                    handles.into_iter().map(|h| h.join().unwrap()).collect()
                });

                let pre_topn_groups: usize = partition_results
                    .iter()
                    .map(|(unique_count, _)| *unique_count)
                    .sum();
                let mut heap: std::collections::BinaryHeap<Reverse<(i128, u128)>> =
                    std::collections::BinaryHeap::with_capacity(limit + 1);
                let mut asc_heap: std::collections::BinaryHeap<(i128, u128)> =
                    std::collections::BinaryHeap::with_capacity(limit + 1);
                for (_, candidates) in partition_results {
                    for (derived, hash_key) in candidates {
                        if ascending {
                            asc_heap.push((derived, hash_key));
                            if asc_heap.len() > limit {
                                asc_heap.pop();
                            }
                        } else {
                            heap.push(Reverse((derived, hash_key)));
                            if heap.len() > limit {
                                heap.pop();
                            }
                        }
                    }
                }
                let winners: Vec<u128> = if ascending {
                    asc_heap.into_iter().map(|(_, h)| h).collect()
                } else {
                    heap.into_iter().map(|Reverse((_, h))| h).collect()
                };
                let topn_select_us = t_merge.elapsed().as_micros() as u64;

                // Step 3: full-merge accumulators for the K winners and
                // finalize. This is the existing speculative-path winner
                // merge — duplicated here rather than factored out to
                // keep this commit narrowly scoped.
                let t_fin = Instant::now();
                let storage = compact_storage.as_mut().unwrap();
                let mut result_rows = Vec::with_capacity(winners.len());
                let mut spec_cd_sidecar = CountDistinctSideCar::new(&agg_specs);

                for hash_key in winners {
                    let global_idx = storage.alloc_group();
                    spec_cd_sidecar.alloc_group();

                    let mut key_source_worker: Option<usize> = None;
                    for (wi, result) in partial_results.iter().enumerate() {
                        if let Some(&worker_idx) = result.compact_map.get(&hash_key) {
                            if key_source_worker.is_none() {
                                key_source_worker = Some(wi);
                            }
                            for (slot_idx, _) in agg_specs.iter().enumerate() {
                                let (_, kind) = storage.layout.slots[slot_idx];
                                match kind {
                                    CompactAccKind::Count => {
                                        let wc =
                                            result.compact_storage.read_count(worker_idx, slot_idx);
                                        *storage.count_mut(global_idx, slot_idx) += wc;
                                    }
                                    CompactAccKind::SumInt => {
                                        let (ws, wc) = result
                                            .compact_storage
                                            .read_sum_int(worker_idx, slot_idx);
                                        let (gs, gc) = storage.sum_int_mut(global_idx, slot_idx);
                                        *gs += ws;
                                        *gc += wc;
                                    }
                                    CompactAccKind::SumIntNarrow => {
                                        let (ws, wc) = result
                                            .compact_storage
                                            .read_sum_int_narrow(worker_idx, slot_idx);
                                        let (gs, gc) =
                                            storage.sum_int_narrow_mut(global_idx, slot_idx);
                                        *gs += ws;
                                        *gc += wc;
                                    }
                                    CompactAccKind::SumFloat => {
                                        let (ws, wc) = result
                                            .compact_storage
                                            .read_sum_float(worker_idx, slot_idx);
                                        let (gs, gc) = storage.sum_float_mut(global_idx, slot_idx);
                                        *gs += ws;
                                        *gc += wc;
                                    }
                                    CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                        let (w_off, w_len) = result
                                            .compact_storage
                                            .read_min_max_str(worker_idx, slot_idx);
                                        if w_off != u32::MAX {
                                            let w_str =
                                                result.compact_storage.str_arena.get(w_off, w_len);
                                            let (g_off, g_len) =
                                                storage.read_min_max_str(global_idx, slot_idx);
                                            let should_update = if g_off == u32::MAX {
                                                true
                                            } else {
                                                let g_str = storage.str_arena.get(g_off, g_len);
                                                let cmp = collation_strcmp(w_str, g_str);
                                                match kind {
                                                    CompactAccKind::MinStr => cmp < 0,
                                                    CompactAccKind::MaxStr => cmp > 0,
                                                    _ => unreachable!(),
                                                }
                                            };
                                            if should_update {
                                                let w_str = result
                                                    .compact_storage
                                                    .str_arena
                                                    .get(w_off, w_len);
                                                let (new_off, new_len) =
                                                    storage.str_arena.alloc(w_str);
                                                storage.write_min_max_str(
                                                    global_idx, slot_idx, new_off, new_len,
                                                );
                                            }
                                        }
                                    }
                                    CompactAccKind::MinInt => {
                                        let (w_val, w_has) = result
                                            .compact_storage
                                            .read_min_max_int(worker_idx, slot_idx);
                                        if w_has {
                                            storage.update_min_int(global_idx, slot_idx, w_val);
                                        }
                                    }
                                    CompactAccKind::MaxInt => {
                                        let (w_val, w_has) = result
                                            .compact_storage
                                            .read_min_max_int(worker_idx, slot_idx);
                                        if w_has {
                                            storage.update_max_int(global_idx, slot_idx, w_val);
                                        }
                                    }
                                    CompactAccKind::CountDistinctInt
                                    | CompactAccKind::CountDistinctStr => {
                                        spec_cd_sidecar.union_from(
                                            slot_idx,
                                            global_idx,
                                            &result.cd_sidecar,
                                            worker_idx,
                                        );
                                    }
                                }
                            }
                        }
                    }

                    for e in &spec_cd_sidecar.entries {
                        let count = e.count(global_idx);
                        *storage.count_mut(global_idx, e.spec_idx) = count;
                    }

                    let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                    for (spec_idx, spec) in agg_specs.iter().enumerate() {
                        agg_results.push(compact_finalize(storage, global_idx, spec_idx, spec));
                    }

                    let source_wi = key_source_worker.unwrap();
                    let source_gidx = *partial_results[source_wi]
                        .compact_map
                        .get(&hash_key)
                        .unwrap();
                    let mixed_ks = &partial_results[source_wi].mixed_keys;

                    let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                    for entry in &output_map {
                        match entry {
                            OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                            OutputEntry::Group(gi) => {
                                let kv = mixed_ks.get(source_gidx, *gi);
                                match kv {
                                    MixedKeyVal::Int(v) => {
                                        if matches!(
                                            group_specs[*gi].expr,
                                            GroupByExpr::Extract { .. }
                                        ) {
                                            row.push((i128_to_numeric_datum(v as i128), false));
                                        } else {
                                            row.push((pg_sys::Datum::from(v as usize), false));
                                        }
                                    }
                                    MixedKeyVal::Str(off, len) => {
                                        let s = mixed_ks.arena.get(off, len);
                                        let datum = string_to_datum(s, group_specs[*gi].type_oid);
                                        row.push((datum, false));
                                    }
                                    MixedKeyVal::Null => {
                                        row.push((pg_sys::Datum::from(0usize), true));
                                    }
                                }
                            }
                            OutputEntry::DerivedGroup { base_gi, delta } => {
                                match mixed_ks.get(source_gidx, *base_gi) {
                                    MixedKeyVal::Int(v) => {
                                        row.push((pg_sys::Datum::from((v + delta) as usize), false))
                                    }
                                    _ => row.push((pg_sys::Datum::from(0usize), true)),
                                }
                            }
                            OutputEntry::Const(d, n) => row.push((*d, *n)),
                        }
                    }
                    result_rows.push(row);
                }
                let finalize_us = t_fin.elapsed().as_micros() as u64;

                let state = AggScanState {
                    _agg_specs: agg_specs,
                    _group_specs: group_specs,
                    result_rows,
                    result_idx: 0,
                    _num_result_cols: num_result_cols,
                    metadata_us,
                    heap_scan_us,
                    detoast_us: total_detoast_us,
                    blob_cache_hits: total_cache_hits,
                    blob_cache_misses: total_cache_misses,
                    blob_cache_bytes_served: total_cache_bytes_served,
                    decompress_us,
                    agg_us,
                    total_segments,
                    total_rows_processed,
                    batch_quals_count: batch_quals.len(),
                    where_quals_null: where_quals.is_null(),
                    segments_metadata_resolved: 0,
                    segments_decompressed: 0,
                    regex_cache_size: 0,
                    regex_cache_calls: 0,
                    topn_limit: topn_limit as u64,
                    topn_sort_col: -3, // derived sentinel — see explain.rs
                    topn_ascending,
                    pre_topn_groups: pre_topn_groups as u64,
                    merge_us: 0,
                    finalize_us,
                    topn_select_us,
                    n_workers: n_workers as u64,
                    bare_limit: 0,
                    wall_us: t_wall.elapsed().as_micros() as u64,
                    buf_stats: super::segments::take_scan_buf_stats(),
                    f8_preselected: 0,
                    pscan: std::ptr::null_mut(),
                    is_parallel_worker: false,
                    exec_ctx: None,
                };

                let state_box = Box::new(state);
                let state_ptr = Box::into_raw(state_box);
                (*node).custom_ps = state_ptr as *mut pg_sys::List;
                return;
            }

            // ----------------------------------------------------------
            // Speculative top-N: merge-skip using pre-computed top-K
            // ----------------------------------------------------------
            // CountDistinct sort values can't be summed across workers for speculative
            // top-N (worker counts overestimate merged count due to set overlap).
            let sort_slot_for_spec = match output_map[topn_sort_col] {
                OutputEntry::Agg(ai) => ai,
                _ => 0,
            };
            let sort_is_cd = topn_limit > 0
                && matches!(
                    compact_storage.as_ref().unwrap().layout.slots[sort_slot_for_spec].1,
                    CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr
                );
            let sort_is_avg =
                topn_limit > 0 && agg_specs[sort_slot_for_spec].agg_type == AggType::Avg;
            if topn_limit > 0 && having_filters.is_empty() && !sort_is_cd && !sort_is_avg {
                let sort_slot = sort_slot_for_spec;
                let (_, sort_kind) = compact_storage.as_ref().unwrap().layout.slots[sort_slot];
                let limit = topn_limit as usize;
                let k = (topn_limit as usize).max(1000);

                let read_sort = |storage: &CompactAccStorage, group_idx: u32| -> i64 {
                    match sort_kind {
                        CompactAccKind::Count => storage.read_count(group_idx, sort_slot),
                        CompactAccKind::SumIntNarrow => {
                            storage.read_sum_int_narrow(group_idx, sort_slot).0
                        }
                        _ => storage.read_count(group_idx, sort_slot),
                    }
                };

                let t_spec = Instant::now();

                // Phase 1: Collect pre-computed top-K candidates from workers
                let mut candidate_set: hashbrown::HashSet<
                    u128,
                    BuildHasherDefault<ahash::AHasher>,
                > = hashbrown::HashSet::with_capacity_and_hasher(
                    k * partial_results.len(),
                    BuildHasherDefault::default(),
                );
                let mut floor_sum: i64 = 0;
                for result in &partial_results {
                    if let Some((keys, floor)) = &result.topk {
                        floor_sum = floor_sum.saturating_add(*floor);
                        for &key in keys {
                            candidate_set.insert(key);
                        }
                    }
                }

                // Phase 2: For each candidate, sum sort values across all workers
                let mut merged: Vec<(i64, u128)> = Vec::with_capacity(candidate_set.len());
                for &key in &candidate_set {
                    let mut total: i64 = 0;
                    for result in &partial_results {
                        if let Some(&gidx) = result.compact_map.get(&key) {
                            total = total.saturating_add(read_sort(&result.compact_storage, gidx));
                        }
                    }
                    merged.push((total, key));
                }

                // Phase 3: Sort and take top-N
                if !topn_ascending {
                    merged.sort_unstable_by_key(|b| std::cmp::Reverse(b.0));
                } else {
                    merged.sort_unstable_by_key(|a| a.0);
                }
                merged.truncate(limit);

                // Phase 4: Correctness check
                let speculative_ok = if merged.len() >= limit {
                    let nth_value = merged[limit - 1].0;
                    if !topn_ascending {
                        nth_value > floor_sum
                    } else {
                        nth_value < floor_sum
                    }
                } else {
                    false
                };

                let topn_select_us = t_spec.elapsed().as_micros() as u64;

                if speculative_ok {
                    // Phase 5: For each winner, merge accumulators and finalize
                    let t_fin = Instant::now();
                    let storage = compact_storage.as_mut().unwrap();
                    let mut result_rows = Vec::with_capacity(merged.len());
                    let mut spec_cd_sidecar = CountDistinctSideCar::new(&agg_specs);

                    // Find which worker has each winning key for MixedKeyStorage lookup
                    for &(_, hash_key) in &merged {
                        let global_idx = storage.alloc_group();
                        spec_cd_sidecar.alloc_group();

                        // Targeted merge: only this key's accumulators across workers
                        let mut key_source_worker: Option<usize> = None;
                        for (wi, result) in partial_results.iter().enumerate() {
                            if let Some(&worker_idx) = result.compact_map.get(&hash_key) {
                                if key_source_worker.is_none() {
                                    key_source_worker = Some(wi);
                                }
                                for (slot_idx, _) in agg_specs.iter().enumerate() {
                                    let (_, kind) = storage.layout.slots[slot_idx];
                                    match kind {
                                        CompactAccKind::Count => {
                                            let wc = result
                                                .compact_storage
                                                .read_count(worker_idx, slot_idx);
                                            *storage.count_mut(global_idx, slot_idx) += wc;
                                        }
                                        CompactAccKind::SumInt => {
                                            let (ws, wc) = result
                                                .compact_storage
                                                .read_sum_int(worker_idx, slot_idx);
                                            let (gs, gc) =
                                                storage.sum_int_mut(global_idx, slot_idx);
                                            *gs += ws;
                                            *gc += wc;
                                        }
                                        CompactAccKind::SumIntNarrow => {
                                            let (ws, wc) = result
                                                .compact_storage
                                                .read_sum_int_narrow(worker_idx, slot_idx);
                                            let (gs, gc) =
                                                storage.sum_int_narrow_mut(global_idx, slot_idx);
                                            *gs += ws;
                                            *gc += wc;
                                        }
                                        CompactAccKind::SumFloat => {
                                            let (ws, wc) = result
                                                .compact_storage
                                                .read_sum_float(worker_idx, slot_idx);
                                            let (gs, gc) =
                                                storage.sum_float_mut(global_idx, slot_idx);
                                            *gs += ws;
                                            *gc += wc;
                                        }
                                        CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                            let (w_off, w_len) = result
                                                .compact_storage
                                                .read_min_max_str(worker_idx, slot_idx);
                                            if w_off != u32::MAX {
                                                let w_str = result
                                                    .compact_storage
                                                    .str_arena
                                                    .get(w_off, w_len);
                                                let (g_off, g_len) =
                                                    storage.read_min_max_str(global_idx, slot_idx);
                                                let should_update = if g_off == u32::MAX {
                                                    true
                                                } else {
                                                    let g_str = storage.str_arena.get(g_off, g_len);
                                                    let cmp = collation_strcmp(w_str, g_str);
                                                    match kind {
                                                        CompactAccKind::MinStr => cmp < 0,
                                                        CompactAccKind::MaxStr => cmp > 0,
                                                        _ => unreachable!(),
                                                    }
                                                };
                                                if should_update {
                                                    let w_str = result
                                                        .compact_storage
                                                        .str_arena
                                                        .get(w_off, w_len);
                                                    let (new_off, new_len) =
                                                        storage.str_arena.alloc(w_str);
                                                    storage.write_min_max_str(
                                                        global_idx, slot_idx, new_off, new_len,
                                                    );
                                                }
                                            }
                                        }
                                        CompactAccKind::MinInt => {
                                            let (w_val, w_has) = result
                                                .compact_storage
                                                .read_min_max_int(worker_idx, slot_idx);
                                            if w_has {
                                                storage.update_min_int(global_idx, slot_idx, w_val);
                                            }
                                        }
                                        CompactAccKind::MaxInt => {
                                            let (w_val, w_has) = result
                                                .compact_storage
                                                .read_min_max_int(worker_idx, slot_idx);
                                            if w_has {
                                                storage.update_max_int(global_idx, slot_idx, w_val);
                                            }
                                        }
                                        CompactAccKind::CountDistinctInt
                                        | CompactAccKind::CountDistinctStr => {
                                            spec_cd_sidecar.union_from(
                                                slot_idx,
                                                global_idx,
                                                &result.cd_sidecar,
                                                worker_idx,
                                            );
                                        }
                                    }
                                }
                            }
                        }

                        // Write CountDistinct counts for this group
                        for e in &spec_cd_sidecar.entries {
                            let count = e.count(global_idx);
                            *storage.count_mut(global_idx, e.spec_idx) = count;
                        }

                        // Finalize this group
                        let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                        for (spec_idx, spec) in agg_specs.iter().enumerate() {
                            agg_results.push(compact_finalize(storage, global_idx, spec_idx, spec));
                        }

                        // Get actual key values from the first worker that has this key
                        let source_wi = key_source_worker.unwrap();
                        let source_gidx = *partial_results[source_wi]
                            .compact_map
                            .get(&hash_key)
                            .unwrap();
                        let mixed_ks = &partial_results[source_wi].mixed_keys;

                        let mut row: Vec<(pg_sys::Datum, bool)> =
                            Vec::with_capacity(num_result_cols);
                        for entry in &output_map {
                            match entry {
                                OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                                OutputEntry::Group(gi) => {
                                    let kv = mixed_ks.get(source_gidx, *gi);
                                    match kv {
                                        MixedKeyVal::Int(v) => {
                                            if matches!(
                                                group_specs[*gi].expr,
                                                GroupByExpr::Extract { .. }
                                            ) {
                                                row.push((i128_to_numeric_datum(v as i128), false));
                                            } else {
                                                row.push((pg_sys::Datum::from(v as usize), false));
                                            }
                                        }
                                        MixedKeyVal::Str(off, len) => {
                                            let s = mixed_ks.arena.get(off, len);
                                            let datum =
                                                string_to_datum(s, group_specs[*gi].type_oid);
                                            row.push((datum, false));
                                        }
                                        MixedKeyVal::Null => {
                                            row.push((pg_sys::Datum::from(0usize), true));
                                        }
                                    }
                                }
                                OutputEntry::DerivedGroup { base_gi, delta } => {
                                    match mixed_ks.get(source_gidx, *base_gi) {
                                        MixedKeyVal::Int(v) => row.push((
                                            pg_sys::Datum::from((v + delta) as usize),
                                            false,
                                        )),
                                        _ => row.push((pg_sys::Datum::from(0usize), true)),
                                    }
                                }
                                OutputEntry::Const(d, n) => row.push((*d, *n)),
                            }
                        }
                        result_rows.push(row);
                    }
                    let finalize_us = t_fin.elapsed().as_micros() as u64;

                    let pre_topn_groups: usize =
                        partial_results.iter().map(|r| r.compact_map.len()).sum();

                    let state = AggScanState {
                        _agg_specs: agg_specs,
                        _group_specs: group_specs,
                        result_rows,
                        result_idx: 0,
                        _num_result_cols: num_result_cols,
                        metadata_us,
                        heap_scan_us,
                        detoast_us: total_detoast_us,
                        blob_cache_hits: total_cache_hits,
                        blob_cache_misses: total_cache_misses,
                        blob_cache_bytes_served: total_cache_bytes_served,
                        decompress_us,
                        agg_us,
                        total_segments,
                        total_rows_processed,
                        batch_quals_count: batch_quals.len(),
                        where_quals_null: where_quals.is_null(),
                        segments_metadata_resolved: 0,
                        segments_decompressed: 0,
                        regex_cache_size: 0,
                        regex_cache_calls: 0,
                        topn_limit: topn_limit as u64,
                        topn_sort_col: topn_sort_col as i64,
                        topn_ascending,
                        pre_topn_groups: pre_topn_groups as u64,
                        merge_us: 0,
                        finalize_us,
                        topn_select_us,
                        n_workers: n_workers as u64,
                        bare_limit: 0,
                        wall_us: t_wall.elapsed().as_micros() as u64,
                        buf_stats: super::segments::take_scan_buf_stats(),
                        f8_preselected: 0,
                        pscan: std::ptr::null_mut(),
                        is_parallel_worker: false,
                        exec_ctx: None,
                    };

                    let state_box = Box::new(state);
                    let state_ptr = Box::into_raw(state_box);
                    (*node).custom_ps = state_ptr as *mut pg_sys::List;
                    return;
                }
                // Speculation failed — check if all tied
                let nth_value = merged
                    .get(limit.saturating_sub(1))
                    .map(|x| x.0)
                    .unwrap_or(0);
                let all_tied = merged.len() >= limit && merged.iter().all(|&(v, _)| v == nth_value);

                pgrx::log!(
                    "pg_deltax mixed speculative top-N failed: candidates={} k={} floor_sum={} all_tied={}",
                    merged.len(),
                    k,
                    floor_sum,
                    all_tied,
                );

                if all_tied {
                    merged.truncate(limit);

                    let t_fin = Instant::now();
                    let storage = compact_storage.as_mut().unwrap();
                    let mut result_rows = Vec::with_capacity(merged.len());
                    let mut spec_cd_sidecar = CountDistinctSideCar::new(&agg_specs);

                    for &(_, hash_key) in &merged {
                        let global_idx = storage.alloc_group();
                        spec_cd_sidecar.alloc_group();

                        let mut key_source_worker: Option<usize> = None;
                        for (wi, result) in partial_results.iter().enumerate() {
                            if let Some(&worker_idx) = result.compact_map.get(&hash_key) {
                                if key_source_worker.is_none() {
                                    key_source_worker = Some(wi);
                                }
                                for (slot_idx, _) in agg_specs.iter().enumerate() {
                                    let (_, kind) = storage.layout.slots[slot_idx];
                                    match kind {
                                        CompactAccKind::Count => {
                                            let wc = result
                                                .compact_storage
                                                .read_count(worker_idx, slot_idx);
                                            *storage.count_mut(global_idx, slot_idx) += wc;
                                        }
                                        CompactAccKind::SumInt => {
                                            let (ws, wc) = result
                                                .compact_storage
                                                .read_sum_int(worker_idx, slot_idx);
                                            let (gs, gc) =
                                                storage.sum_int_mut(global_idx, slot_idx);
                                            *gs += ws;
                                            *gc += wc;
                                        }
                                        CompactAccKind::SumIntNarrow => {
                                            let (ws, wc) = result
                                                .compact_storage
                                                .read_sum_int_narrow(worker_idx, slot_idx);
                                            let (gs, gc) =
                                                storage.sum_int_narrow_mut(global_idx, slot_idx);
                                            *gs += ws;
                                            *gc += wc;
                                        }
                                        CompactAccKind::SumFloat => {
                                            let (ws, wc) = result
                                                .compact_storage
                                                .read_sum_float(worker_idx, slot_idx);
                                            let (gs, gc) =
                                                storage.sum_float_mut(global_idx, slot_idx);
                                            *gs += ws;
                                            *gc += wc;
                                        }
                                        CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                            let (w_off, w_len) = result
                                                .compact_storage
                                                .read_min_max_str(worker_idx, slot_idx);
                                            if w_off != u32::MAX {
                                                let w_str = result
                                                    .compact_storage
                                                    .str_arena
                                                    .get(w_off, w_len);
                                                let (g_off, g_len) =
                                                    storage.read_min_max_str(global_idx, slot_idx);
                                                let should_update = if g_off == u32::MAX {
                                                    true
                                                } else {
                                                    let g_str = storage.str_arena.get(g_off, g_len);
                                                    let cmp = collation_strcmp(w_str, g_str);
                                                    match kind {
                                                        CompactAccKind::MinStr => cmp < 0,
                                                        CompactAccKind::MaxStr => cmp > 0,
                                                        _ => unreachable!(),
                                                    }
                                                };
                                                if should_update {
                                                    let w_str = result
                                                        .compact_storage
                                                        .str_arena
                                                        .get(w_off, w_len);
                                                    let (new_off, new_len) =
                                                        storage.str_arena.alloc(w_str);
                                                    storage.write_min_max_str(
                                                        global_idx, slot_idx, new_off, new_len,
                                                    );
                                                }
                                            }
                                        }
                                        CompactAccKind::MinInt => {
                                            let (w_val, w_has) = result
                                                .compact_storage
                                                .read_min_max_int(worker_idx, slot_idx);
                                            if w_has {
                                                storage.update_min_int(global_idx, slot_idx, w_val);
                                            }
                                        }
                                        CompactAccKind::MaxInt => {
                                            let (w_val, w_has) = result
                                                .compact_storage
                                                .read_min_max_int(worker_idx, slot_idx);
                                            if w_has {
                                                storage.update_max_int(global_idx, slot_idx, w_val);
                                            }
                                        }
                                        CompactAccKind::CountDistinctInt
                                        | CompactAccKind::CountDistinctStr => {
                                            spec_cd_sidecar.union_from(
                                                slot_idx,
                                                global_idx,
                                                &result.cd_sidecar,
                                                worker_idx,
                                            );
                                        }
                                    }
                                }
                            }
                        }

                        for e in &spec_cd_sidecar.entries {
                            let count = e.count(global_idx);
                            *storage.count_mut(global_idx, e.spec_idx) = count;
                        }

                        let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                        for (spec_idx, spec) in agg_specs.iter().enumerate() {
                            agg_results.push(compact_finalize(storage, global_idx, spec_idx, spec));
                        }

                        let source_wi = key_source_worker.unwrap();
                        let source_gidx = *partial_results[source_wi]
                            .compact_map
                            .get(&hash_key)
                            .unwrap();
                        let mixed_ks = &partial_results[source_wi].mixed_keys;

                        let mut row: Vec<(pg_sys::Datum, bool)> =
                            Vec::with_capacity(num_result_cols);
                        for entry in &output_map {
                            match entry {
                                OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                                OutputEntry::Group(gi) => {
                                    let kv = mixed_ks.get(source_gidx, *gi);
                                    match kv {
                                        MixedKeyVal::Int(v) => {
                                            if matches!(
                                                group_specs[*gi].expr,
                                                GroupByExpr::Extract { .. }
                                            ) {
                                                row.push((i128_to_numeric_datum(v as i128), false));
                                            } else {
                                                row.push((pg_sys::Datum::from(v as usize), false));
                                            }
                                        }
                                        MixedKeyVal::Str(off, len) => {
                                            let s = mixed_ks.arena.get(off, len);
                                            let datum =
                                                string_to_datum(s, group_specs[*gi].type_oid);
                                            row.push((datum, false));
                                        }
                                        MixedKeyVal::Null => {
                                            row.push((pg_sys::Datum::from(0usize), true));
                                        }
                                    }
                                }
                                OutputEntry::DerivedGroup { base_gi, delta } => {
                                    match mixed_ks.get(source_gidx, *base_gi) {
                                        MixedKeyVal::Int(v) => row.push((
                                            pg_sys::Datum::from((v + delta) as usize),
                                            false,
                                        )),
                                        _ => row.push((pg_sys::Datum::from(0usize), true)),
                                    }
                                }
                                OutputEntry::Const(d, n) => row.push((*d, *n)),
                            }
                        }
                        result_rows.push(row);
                    }
                    let finalize_us = t_fin.elapsed().as_micros() as u64;

                    let pre_topn_groups: usize =
                        partial_results.iter().map(|r| r.compact_map.len()).sum();

                    let state = AggScanState {
                        _agg_specs: agg_specs,
                        _group_specs: group_specs,
                        result_rows,
                        result_idx: 0,
                        _num_result_cols: num_result_cols,
                        metadata_us,
                        heap_scan_us,
                        detoast_us: total_detoast_us,
                        blob_cache_hits: total_cache_hits,
                        blob_cache_misses: total_cache_misses,
                        blob_cache_bytes_served: total_cache_bytes_served,
                        decompress_us,
                        agg_us,
                        total_segments,
                        total_rows_processed,
                        batch_quals_count: batch_quals.len(),
                        where_quals_null: where_quals.is_null(),
                        segments_metadata_resolved: 0,
                        segments_decompressed: 0,
                        regex_cache_size: 0,
                        regex_cache_calls: 0,
                        topn_limit: topn_limit as u64,
                        topn_sort_col: topn_sort_col as i64,
                        topn_ascending,
                        pre_topn_groups: pre_topn_groups as u64,
                        merge_us: 0,
                        finalize_us,
                        topn_select_us,
                        n_workers: n_workers as u64,
                        bare_limit: 0,
                        wall_us: t_wall.elapsed().as_micros() as u64,
                        buf_stats: super::segments::take_scan_buf_stats(),
                        f8_preselected: 0,
                        pscan: std::ptr::null_mut(),
                        is_parallel_worker: false,
                        exec_ctx: None,
                    };

                    let state_box = Box::new(state);
                    let state_ptr = Box::into_raw(state_box);
                    (*node).custom_ps = state_ptr as *mut pg_sys::List;
                    return;
                }
            }

            // ----------------------------------------------------------
            // Bare LIMIT short-circuit for mixed path: pick N groups
            // from largest worker, merge only those, finalize only those
            // ----------------------------------------------------------
            if bare_limit > 0 && having_filters.is_empty() {
                let n = bare_limit as usize;
                let t_merge = Instant::now();

                // Pick the largest worker
                let largest_idx = partial_results
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, r)| r.compact_map.len())
                    .map(|(i, _)| i)
                    .unwrap_or(0);

                // Collect first N group keys from largest worker
                let target_keys: Vec<u128> = partial_results[largest_idx]
                    .compact_map
                    .keys()
                    .take(n)
                    .copied()
                    .collect();

                // Targeted merge: for each target key, merge accumulators from all workers
                let layout = CompactAccLayout {
                    slots: partial_results[0].compact_storage.layout.slots.clone(),
                    group_stride: partial_results[0].compact_storage.layout.group_stride,
                };
                let mut final_storage = CompactAccStorage::new(layout);
                let mut final_mixed_keys = MixedKeyStorage::new(group_specs.len());
                let mut final_cd_sidecar = CountDistinctSideCar::new(&agg_specs);

                for &key in &target_keys {
                    let group_idx = final_storage.alloc_group();
                    final_cd_sidecar.alloc_group();

                    // Copy key values from the largest worker
                    let src = &partial_results[largest_idx];
                    let src_gidx = src.compact_map[&key];
                    let n_cols = group_specs.len();
                    for col in 0..n_cols {
                        let kv = src.mixed_keys.get(src_gidx, col);
                        match kv {
                            MixedKeyVal::Str(off, len) => {
                                let s = src.mixed_keys.arena.get(off, len);
                                let (new_off, new_len) = final_mixed_keys.arena.alloc(s);
                                final_mixed_keys
                                    .keys
                                    .push(MixedKeyVal::Str(new_off, new_len));
                            }
                            other => final_mixed_keys.keys.push(other),
                        }
                    }

                    // Merge accumulators from all workers
                    for result in &partial_results {
                        if let Some(&worker_gidx) = result.compact_map.get(&key) {
                            for (slot_idx, _) in agg_specs.iter().enumerate() {
                                let (_, kind) = final_storage.layout.slots[slot_idx];
                                match kind {
                                    CompactAccKind::Count => {
                                        let wc = result
                                            .compact_storage
                                            .read_count(worker_gidx, slot_idx);
                                        *final_storage.count_mut(group_idx, slot_idx) += wc;
                                    }
                                    CompactAccKind::SumInt => {
                                        let (ws, wc) = result
                                            .compact_storage
                                            .read_sum_int(worker_gidx, slot_idx);
                                        let (gs, gc) =
                                            final_storage.sum_int_mut(group_idx, slot_idx);
                                        *gs += ws;
                                        *gc += wc;
                                    }
                                    CompactAccKind::SumIntNarrow => {
                                        let (ws, wc) = result
                                            .compact_storage
                                            .read_sum_int_narrow(worker_gidx, slot_idx);
                                        let (gs, gc) =
                                            final_storage.sum_int_narrow_mut(group_idx, slot_idx);
                                        *gs += ws;
                                        *gc += wc;
                                    }
                                    CompactAccKind::SumFloat => {
                                        let (ws, wc) = result
                                            .compact_storage
                                            .read_sum_float(worker_gidx, slot_idx);
                                        let (gs, gc) =
                                            final_storage.sum_float_mut(group_idx, slot_idx);
                                        *gs += ws;
                                        *gc += wc;
                                    }
                                    CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                        let (w_off, w_len) = result
                                            .compact_storage
                                            .read_min_max_str(worker_gidx, slot_idx);
                                        if w_off != u32::MAX {
                                            let w_str =
                                                result.compact_storage.str_arena.get(w_off, w_len);
                                            let (g_off, g_len) =
                                                final_storage.read_min_max_str(group_idx, slot_idx);
                                            let should_update = if g_off == u32::MAX {
                                                true
                                            } else {
                                                let g_str =
                                                    final_storage.str_arena.get(g_off, g_len);
                                                let cmp = collation_strcmp(w_str, g_str);
                                                match kind {
                                                    CompactAccKind::MinStr => cmp < 0,
                                                    CompactAccKind::MaxStr => cmp > 0,
                                                    _ => unreachable!(),
                                                }
                                            };
                                            if should_update {
                                                let w_str = result
                                                    .compact_storage
                                                    .str_arena
                                                    .get(w_off, w_len);
                                                let (new_off, new_len) =
                                                    final_storage.str_arena.alloc(w_str);
                                                final_storage.write_min_max_str(
                                                    group_idx, slot_idx, new_off, new_len,
                                                );
                                            }
                                        }
                                    }
                                    CompactAccKind::MinInt => {
                                        let (w_val, w_has) = result
                                            .compact_storage
                                            .read_min_max_int(worker_gidx, slot_idx);
                                        if w_has {
                                            final_storage
                                                .update_min_int(group_idx, slot_idx, w_val);
                                        }
                                    }
                                    CompactAccKind::MaxInt => {
                                        let (w_val, w_has) = result
                                            .compact_storage
                                            .read_min_max_int(worker_gidx, slot_idx);
                                        if w_has {
                                            final_storage
                                                .update_max_int(group_idx, slot_idx, w_val);
                                        }
                                    }
                                    CompactAccKind::CountDistinctInt
                                    | CompactAccKind::CountDistinctStr => {
                                        final_cd_sidecar.union_from(
                                            slot_idx,
                                            group_idx,
                                            &result.cd_sidecar,
                                            worker_gidx,
                                        );
                                    }
                                }
                            }
                        }
                    }
                }

                // Write CountDistinct counts to storage
                if !final_cd_sidecar.is_empty() {
                    for i in 0..target_keys.len() {
                        let group_idx = i as u32;
                        for e in &final_cd_sidecar.entries {
                            let count = e.count(group_idx);
                            *final_storage.count_mut(group_idx, e.spec_idx) = count;
                        }
                    }
                }

                let merge_us = t_merge.elapsed().as_micros() as u64;

                // Finalize just N groups
                let pre_topn_groups: usize =
                    partial_results.iter().map(|r| r.compact_map.len()).sum();
                let t_finalize = Instant::now();
                let mut result_rows = Vec::with_capacity(n);
                for (i, &_key) in target_keys.iter().enumerate() {
                    let group_idx = i as u32;
                    let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                    for (spec_idx, spec) in agg_specs.iter().enumerate() {
                        agg_results.push(compact_finalize(
                            &final_storage,
                            group_idx,
                            spec_idx,
                            spec,
                        ));
                    }
                    let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                    for entry in &output_map {
                        match entry {
                            OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                            OutputEntry::Group(gi) => {
                                let kv = final_mixed_keys.get(group_idx, *gi);
                                match kv {
                                    MixedKeyVal::Int(v) => {
                                        if matches!(
                                            group_specs[*gi].expr,
                                            GroupByExpr::Extract { .. }
                                        ) {
                                            row.push((i128_to_numeric_datum(v as i128), false));
                                        } else {
                                            row.push((pg_sys::Datum::from(v as usize), false));
                                        }
                                    }
                                    MixedKeyVal::Str(off, len) => {
                                        let s = final_mixed_keys.arena.get(off, len);
                                        let datum = string_to_datum(s, group_specs[*gi].type_oid);
                                        row.push((datum, false));
                                    }
                                    MixedKeyVal::Null => {
                                        row.push((pg_sys::Datum::from(0usize), true));
                                    }
                                }
                            }
                            OutputEntry::DerivedGroup { base_gi, delta } => {
                                match final_mixed_keys.get(group_idx, *base_gi) {
                                    MixedKeyVal::Int(v) => {
                                        row.push((pg_sys::Datum::from((v + delta) as usize), false))
                                    }
                                    _ => row.push((pg_sys::Datum::from(0usize), true)),
                                }
                            }
                            OutputEntry::Const(d, n) => row.push((*d, *n)),
                        }
                    }
                    result_rows.push(row);
                }
                let finalize_us = t_finalize.elapsed().as_micros() as u64;

                let state = AggScanState {
                    _agg_specs: agg_specs,
                    _group_specs: group_specs,
                    result_rows,
                    result_idx: 0,
                    _num_result_cols: num_result_cols,
                    metadata_us,
                    heap_scan_us,
                    detoast_us: total_detoast_us,
                    blob_cache_hits: total_cache_hits,
                    blob_cache_misses: total_cache_misses,
                    blob_cache_bytes_served: total_cache_bytes_served,
                    decompress_us,
                    agg_us,
                    total_segments,
                    total_rows_processed,
                    batch_quals_count: batch_quals.len(),
                    where_quals_null: where_quals.is_null(),
                    segments_metadata_resolved: 0,
                    segments_decompressed: 0,
                    regex_cache_size: 0,
                    regex_cache_calls: 0,
                    topn_limit: 0,
                    topn_sort_col: -1,
                    topn_ascending,
                    pre_topn_groups: pre_topn_groups as u64,
                    merge_us,
                    finalize_us,
                    topn_select_us: 0,
                    n_workers: n_workers as u64,
                    bare_limit,
                    wall_us: t_wall.elapsed().as_micros() as u64,
                    buf_stats: super::segments::take_scan_buf_stats(),
                    f8_preselected: preselected_keys
                        .as_ref()
                        .map(|s| s.len() as u64)
                        .unwrap_or(0),
                    pscan: std::ptr::null_mut(),
                    is_parallel_worker: false,
                    exec_ctx: None,
                };

                let state_box = Box::new(state);
                let state_ptr = Box::into_raw(state_box);
                (*node).custom_ps = state_ptr as *mut pg_sys::List;
                return;
            }

            // ----------------------------------------------------------
            // Partitioned parallel merge + top-N for mixed path:
            // partition key space across threads, each merges its slice
            // and finds local top-N, then merge the local results.
            // ----------------------------------------------------------
            if topn_limit > 0 {
                let t_merge = Instant::now();
                let limit = topn_limit as usize;
                let sort_slot = match output_map[topn_sort_col] {
                    OutputEntry::Agg(ai) => ai,
                    _ => unreachable!(),
                };
                let n_partitions = n_workers;
                let n_group_cols = group_specs.len();

                let pre_topn_groups: usize =
                    partial_results.iter().map(|r| r.compact_map.len()).sum();

                // Each partition thread: merge its slice, find local top-N,
                // copy winners to mini storage + mini mixed keys, drop the rest.
                #[allow(clippy::type_complexity)]
                let partition_results: Vec<(
                    CompactAccStorage,
                    MixedKeyStorage,
                    Vec<(i64, u128, u32)>,
                )> = std::thread::scope(|s| {
                    let workers = &partial_results;
                    let specs = &agg_specs;
                    let np = n_partitions;
                    let ascending = topn_ascending;
                    let ngk = n_group_cols;
                    let hfilters = &having_filters;

                    let handles: Vec<_> = (0..np)
                        .map(|p| {
                            s.spawn(move || {
                                let layout = CompactAccLayout::new(specs);
                                let n_slots = layout.slots.len();
                                let mut map: CompactGroupMap =
                                    CompactGroupMap::with_hasher(Default::default());
                                let mut storage = CompactAccStorage::new(layout);
                                let mut cd_sidecar = CountDistinctSideCar::new(specs);
                                let mut mixed_ks = MixedKeyStorage::new(ngk);

                                // Merge entries from all workers belonging to this partition
                                for worker in workers {
                                    for (&key, &wgidx) in &worker.compact_map {
                                        if ((key as u64) ^ ((key >> 64) as u64)) as usize % np != p
                                        {
                                            continue;
                                        }
                                        let gidx = match map.entry(key) {
                                            hashbrown::hash_map::Entry::Occupied(e) => *e.get(),
                                            hashbrown::hash_map::Entry::Vacant(e) => {
                                                let idx = storage.alloc_group();
                                                cd_sidecar.alloc_group();
                                                // Copy key values from this worker
                                                for col in 0..ngk {
                                                    let kv = worker.mixed_keys.get(wgidx, col);
                                                    match kv {
                                                        MixedKeyVal::Str(off, len) => {
                                                            let sv = worker
                                                                .mixed_keys
                                                                .arena
                                                                .get(off, len);
                                                            let (no, nl) = mixed_ks.arena.alloc(sv);
                                                            mixed_ks
                                                                .keys
                                                                .push(MixedKeyVal::Str(no, nl));
                                                        }
                                                        other => mixed_ks.keys.push(other),
                                                    }
                                                }
                                                e.insert(idx);
                                                idx
                                            }
                                        };
                                        for slot_idx in 0..n_slots {
                                            let (_, kind) = storage.layout.slots[slot_idx];
                                            match kind {
                                                CompactAccKind::Count => {
                                                    let wc = worker
                                                        .compact_storage
                                                        .read_count(wgidx, slot_idx);
                                                    *storage.count_mut(gidx, slot_idx) += wc;
                                                }
                                                CompactAccKind::SumInt => {
                                                    let (ws, wc) = worker
                                                        .compact_storage
                                                        .read_sum_int(wgidx, slot_idx);
                                                    let (gs, gc) =
                                                        storage.sum_int_mut(gidx, slot_idx);
                                                    *gs += ws;
                                                    *gc += wc;
                                                }
                                                CompactAccKind::SumIntNarrow => {
                                                    let (ws, wc) = worker
                                                        .compact_storage
                                                        .read_sum_int_narrow(wgidx, slot_idx);
                                                    let (gs, gc) =
                                                        storage.sum_int_narrow_mut(gidx, slot_idx);
                                                    *gs += ws;
                                                    *gc += wc;
                                                }
                                                CompactAccKind::SumFloat => {
                                                    let (ws, wc) = worker
                                                        .compact_storage
                                                        .read_sum_float(wgidx, slot_idx);
                                                    let (gs, gc) =
                                                        storage.sum_float_mut(gidx, slot_idx);
                                                    *gs += ws;
                                                    *gc += wc;
                                                }
                                                CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                                    let (w_off, w_len) = worker
                                                        .compact_storage
                                                        .read_min_max_str(wgidx, slot_idx);
                                                    if w_off != u32::MAX {
                                                        let w_str = worker
                                                            .compact_storage
                                                            .str_arena
                                                            .get(w_off, w_len);
                                                        let (g_off, g_len) = storage
                                                            .read_min_max_str(gidx, slot_idx);
                                                        let should_update = if g_off == u32::MAX {
                                                            true
                                                        } else {
                                                            let g_str =
                                                                storage.str_arena.get(g_off, g_len);
                                                            let cmp = strcoll_cmp(w_str, g_str);
                                                            match kind {
                                                                CompactAccKind::MinStr => {
                                                                    cmp == std::cmp::Ordering::Less
                                                                }
                                                                _ => cmp
                                                                    == std::cmp::Ordering::Greater,
                                                            }
                                                        };
                                                        if should_update {
                                                            let w_str = worker
                                                                .compact_storage
                                                                .str_arena
                                                                .get(w_off, w_len);
                                                            let (new_off, new_len) =
                                                                storage.str_arena.alloc(w_str);
                                                            storage.write_min_max_str(
                                                                gidx, slot_idx, new_off, new_len,
                                                            );
                                                        }
                                                    }
                                                }
                                                CompactAccKind::MinInt => {
                                                    let (w_val, w_has) = worker
                                                        .compact_storage
                                                        .read_min_max_int(wgidx, slot_idx);
                                                    if w_has {
                                                        storage
                                                            .update_min_int(gidx, slot_idx, w_val);
                                                    }
                                                }
                                                CompactAccKind::MaxInt => {
                                                    let (w_val, w_has) = worker
                                                        .compact_storage
                                                        .read_min_max_int(wgidx, slot_idx);
                                                    if w_has {
                                                        storage
                                                            .update_max_int(gidx, slot_idx, w_val);
                                                    }
                                                }
                                                CompactAccKind::CountDistinctInt
                                                | CompactAccKind::CountDistinctStr => {
                                                    cd_sidecar.union_from(
                                                        slot_idx,
                                                        gidx,
                                                        &worker.cd_sidecar,
                                                        wgidx,
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }

                                // Write CD counts into storage before top-N selection
                                cd_sidecar.write_counts_to_storage(&mut storage, &map);

                                // Local top-N selection using a heap
                                let (_, sort_kind) = storage.layout.slots[sort_slot];
                                let sort_is_avg = specs[sort_slot].agg_type == AggType::Avg;
                                let read_val = |gidx: u32| -> i64 {
                                    if sort_is_avg {
                                        let avg = match sort_kind {
                                            CompactAccKind::SumIntNarrow => {
                                                let (s, c) =
                                                    storage.read_sum_int_narrow(gidx, sort_slot);
                                                if c > 0 { s as f64 / c as f64 } else { 0.0 }
                                            }
                                            CompactAccKind::SumFloat => {
                                                let (s, c) =
                                                    storage.read_sum_float(gidx, sort_slot);
                                                if c > 0 { s / c as f64 } else { 0.0 }
                                            }
                                            _ => storage.read_count(gidx, sort_slot) as f64,
                                        };
                                        let bits = avg.to_bits() as i64;
                                        if bits >= 0 { bits } else { bits ^ i64::MAX }
                                    } else {
                                        match sort_kind {
                                            CompactAccKind::Count => {
                                                storage.read_count(gidx, sort_slot)
                                            }
                                            CompactAccKind::SumIntNarrow => {
                                                storage.read_sum_int_narrow(gidx, sort_slot).0
                                            }
                                            _ => storage.read_count(gidx, sort_slot),
                                        }
                                    }
                                };

                                let having_read_val = |gidx: u32, slot: usize| -> i64 {
                                    let (_, kind) = storage.layout.slots[slot];
                                    match kind {
                                        CompactAccKind::Count => storage.read_count(gidx, slot),
                                        CompactAccKind::SumIntNarrow => {
                                            storage.read_sum_int_narrow(gidx, slot).0
                                        }
                                        _ => storage.read_count(gidx, slot),
                                    }
                                };

                                let winners: Vec<(i64, u128, u32)> = if ascending {
                                    let mut heap: BinaryHeap<(i64, u128, u32)> =
                                        BinaryHeap::with_capacity(limit + 1);
                                    for (&key, &gidx) in &map {
                                        let mut passes = true;
                                        for hf in hfilters {
                                            let val = having_read_val(gidx, hf.agg_idx);
                                            let ok = match hf.op {
                                                HavingOp::Gt => val > hf.const_val,
                                                HavingOp::Lt => val < hf.const_val,
                                                HavingOp::Ge => val >= hf.const_val,
                                                HavingOp::Le => val <= hf.const_val,
                                                HavingOp::Eq => val == hf.const_val,
                                                HavingOp::Ne => val != hf.const_val,
                                            };
                                            if !ok {
                                                passes = false;
                                                break;
                                            }
                                        }
                                        if !passes {
                                            continue;
                                        }
                                        let val = read_val(gidx);
                                        heap.push((val, key, gidx));
                                        if heap.len() > limit {
                                            heap.pop();
                                        }
                                    }
                                    heap.into_vec()
                                } else {
                                    let mut heap: BinaryHeap<Reverse<(i64, u128, u32)>> =
                                        BinaryHeap::with_capacity(limit + 1);
                                    for (&key, &gidx) in &map {
                                        let mut passes = true;
                                        for hf in hfilters {
                                            let val = having_read_val(gidx, hf.agg_idx);
                                            let ok = match hf.op {
                                                HavingOp::Gt => val > hf.const_val,
                                                HavingOp::Lt => val < hf.const_val,
                                                HavingOp::Ge => val >= hf.const_val,
                                                HavingOp::Le => val <= hf.const_val,
                                                HavingOp::Eq => val == hf.const_val,
                                                HavingOp::Ne => val != hf.const_val,
                                            };
                                            if !ok {
                                                passes = false;
                                                break;
                                            }
                                        }
                                        if !passes {
                                            continue;
                                        }
                                        let val = read_val(gidx);
                                        heap.push(Reverse((val, key, gidx)));
                                        if heap.len() > limit {
                                            heap.pop();
                                        }
                                    }
                                    heap.into_iter().map(|Reverse(x)| x).collect()
                                };

                                drop(map);

                                // Copy winning groups to mini storage + mini mixed keys
                                let layout2 = CompactAccLayout::new(specs);
                                let stride = storage.layout.group_stride;
                                let mut mini = CompactAccStorage::new(layout2);
                                let mut mini_keys = MixedKeyStorage::new(ngk);
                                let mut top_entries = Vec::with_capacity(winners.len());

                                for (sort_val, key, old_gidx) in winners {
                                    let new_gidx = mini.alloc_group();
                                    let src = old_gidx as usize * stride;
                                    let dst = new_gidx as usize * stride;
                                    mini.buf[dst..dst + stride]
                                        .copy_from_slice(&storage.buf[src..src + stride]);
                                    // Remap MinStr/MaxStr arena references
                                    for slot_idx in 0..n_slots {
                                        let (_, kind) = storage.layout.slots[slot_idx];
                                        if kind == CompactAccKind::MinStr
                                            || kind == CompactAccKind::MaxStr
                                        {
                                            let (off, len) =
                                                storage.read_min_max_str(old_gidx, slot_idx);
                                            if off != u32::MAX {
                                                let val_str = storage.str_arena.get(off, len);
                                                let (no, nl) = mini.str_arena.alloc(val_str);
                                                mini.write_min_max_str(new_gidx, slot_idx, no, nl);
                                            }
                                        }
                                    }
                                    // Copy mixed key values
                                    for col in 0..ngk {
                                        let kv = mixed_ks.get(old_gidx, col);
                                        match kv {
                                            MixedKeyVal::Str(off, len) => {
                                                let sv = mixed_ks.arena.get(off, len);
                                                let (no, nl) = mini_keys.arena.alloc(sv);
                                                mini_keys.keys.push(MixedKeyVal::Str(no, nl));
                                            }
                                            other => mini_keys.keys.push(other),
                                        }
                                    }
                                    top_entries.push((sort_val, key, new_gidx));
                                }

                                drop(storage);
                                drop(mixed_ks);

                                (mini, mini_keys, top_entries)
                            })
                        })
                        .collect();

                    handles.into_iter().map(|h| h.join().unwrap()).collect()
                });

                drop(partial_results);

                let merge_us = t_merge.elapsed().as_micros() as u64;

                // Merge all partition top entries, select global top-N
                let t_finalize = Instant::now();
                let mut all_candidates: Vec<(i64, u128, u32, usize)> = Vec::new();
                for (pi, (_, _, entries)) in partition_results.iter().enumerate() {
                    for &(sort_val, key, gidx) in entries {
                        all_candidates.push((sort_val, key, gidx, pi));
                    }
                }
                if topn_ascending {
                    all_candidates.sort_unstable_by_key(|a| a.0);
                } else {
                    all_candidates.sort_unstable_by_key(|b| std::cmp::Reverse(b.0));
                }
                all_candidates.truncate(limit);

                let mut result_rows = Vec::with_capacity(limit);
                for &(_sort_val, _key, mini_gidx, pi) in &all_candidates {
                    let storage = &partition_results[pi].0;
                    let mixed_ks = &partition_results[pi].1;
                    let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                    for (spec_idx, spec) in agg_specs.iter().enumerate() {
                        agg_results.push(compact_finalize(storage, mini_gidx, spec_idx, spec));
                    }
                    let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                    for entry in &output_map {
                        match entry {
                            OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                            OutputEntry::Group(gi) => {
                                let kv = mixed_ks.get(mini_gidx, *gi);
                                match kv {
                                    MixedKeyVal::Int(v) => {
                                        if matches!(
                                            group_specs[*gi].expr,
                                            GroupByExpr::Extract { .. }
                                        ) {
                                            row.push((i128_to_numeric_datum(v as i128), false));
                                        } else {
                                            row.push((pg_sys::Datum::from(v as usize), false));
                                        }
                                    }
                                    MixedKeyVal::Str(off, len) => {
                                        let sv = mixed_ks.arena.get(off, len);
                                        let datum = string_to_datum(sv, group_specs[*gi].type_oid);
                                        row.push((datum, false));
                                    }
                                    MixedKeyVal::Null => {
                                        row.push((pg_sys::Datum::from(0usize), true));
                                    }
                                }
                            }
                            OutputEntry::DerivedGroup { base_gi, delta } => {
                                match mixed_ks.get(mini_gidx, *base_gi) {
                                    MixedKeyVal::Int(v) => {
                                        row.push((pg_sys::Datum::from((v + delta) as usize), false))
                                    }
                                    _ => row.push((pg_sys::Datum::from(0usize), true)),
                                }
                            }
                            OutputEntry::Const(d, n) => row.push((*d, *n)),
                        }
                    }
                    result_rows.push(row);
                }
                let finalize_us = t_finalize.elapsed().as_micros() as u64;

                let state = AggScanState {
                    _agg_specs: agg_specs,
                    _group_specs: group_specs,
                    result_rows,
                    result_idx: 0,
                    _num_result_cols: num_result_cols,
                    metadata_us,
                    heap_scan_us,
                    detoast_us: total_detoast_us,
                    blob_cache_hits: total_cache_hits,
                    blob_cache_misses: total_cache_misses,
                    blob_cache_bytes_served: total_cache_bytes_served,
                    decompress_us,
                    agg_us,
                    total_segments,
                    total_rows_processed,
                    batch_quals_count: batch_quals.len(),
                    where_quals_null: where_quals.is_null(),
                    segments_metadata_resolved: 0,
                    segments_decompressed: 0,
                    regex_cache_size: 0,
                    regex_cache_calls: 0,
                    topn_limit: topn_limit as u64,
                    topn_sort_col: topn_sort_col as i64,
                    topn_ascending,
                    pre_topn_groups: pre_topn_groups as u64,
                    merge_us,
                    finalize_us,
                    topn_select_us: 0,
                    n_workers: n_workers as u64,
                    bare_limit: 0,
                    wall_us: t_wall.elapsed().as_micros() as u64,
                    buf_stats: super::segments::take_scan_buf_stats(),
                    f8_preselected: 0,
                    pscan: std::ptr::null_mut(),
                    is_parallel_worker: false,
                    exec_ctx: None,
                };

                let state_box = Box::new(state);
                let state_ptr = Box::into_raw(state_box);
                (*node).custom_ps = state_ptr as *mut pg_sys::List;
                return;
            }

            // ----------------------------------------------------------
            // Full merge path for mixed: adopt largest, merge rest
            // ----------------------------------------------------------
            let t_merge = Instant::now();
            let mut partial_results = partial_results;

            let largest_idx = partial_results
                .iter()
                .enumerate()
                .max_by_key(|(_, r)| r.compact_map.len())
                .map(|(i, _)| i)
                .unwrap_or(0);
            let largest = partial_results.swap_remove(largest_idx);
            compact_group_map = largest.compact_map;
            *compact_storage.as_mut().unwrap() = largest.compact_storage;
            let mut merged_mixed_keys = largest.mixed_keys;
            let mut merged_cd_sidecar = largest.cd_sidecar;

            let remaining_entries: usize =
                partial_results.iter().map(|r| r.compact_map.len()).sum();
            compact_group_map.reserve(remaining_entries);

            let storage = compact_storage.as_mut().unwrap();
            for result in &partial_results {
                for (&hash_key, &worker_group_idx) in &result.compact_map {
                    let global_group_idx = match compact_group_map.entry(hash_key) {
                        hashbrown::hash_map::Entry::Occupied(e) => *e.get(),
                        hashbrown::hash_map::Entry::Vacant(e) => {
                            let idx = storage.alloc_group();
                            merged_cd_sidecar.alloc_group();
                            e.insert(idx);
                            // Copy key values from this worker's MixedKeyStorage
                            let src_keys = &result.mixed_keys;
                            let n = group_specs.len();
                            for col in 0..n {
                                let kv = src_keys.get(worker_group_idx, col);
                                match kv {
                                    MixedKeyVal::Str(off, len) => {
                                        let s = src_keys.arena.get(off, len);
                                        let (new_off, new_len) = merged_mixed_keys.arena.alloc(s);
                                        merged_mixed_keys
                                            .keys
                                            .push(MixedKeyVal::Str(new_off, new_len));
                                    }
                                    other => {
                                        merged_mixed_keys.keys.push(other);
                                    }
                                }
                            }
                            idx
                        }
                    };

                    // Merge accumulators
                    for (slot_idx, _) in agg_specs.iter().enumerate() {
                        let (_, kind) = storage.layout.slots[slot_idx];
                        match kind {
                            CompactAccKind::Count => {
                                let wc = result
                                    .compact_storage
                                    .read_count(worker_group_idx, slot_idx);
                                *storage.count_mut(global_group_idx, slot_idx) += wc;
                            }
                            CompactAccKind::SumInt => {
                                let (ws, wc) = result
                                    .compact_storage
                                    .read_sum_int(worker_group_idx, slot_idx);
                                let (gs, gc) = storage.sum_int_mut(global_group_idx, slot_idx);
                                *gs += ws;
                                *gc += wc;
                            }
                            CompactAccKind::SumIntNarrow => {
                                let (ws, wc) = result
                                    .compact_storage
                                    .read_sum_int_narrow(worker_group_idx, slot_idx);
                                let (gs, gc) =
                                    storage.sum_int_narrow_mut(global_group_idx, slot_idx);
                                *gs += ws;
                                *gc += wc;
                            }
                            CompactAccKind::SumFloat => {
                                let (ws, wc) = result
                                    .compact_storage
                                    .read_sum_float(worker_group_idx, slot_idx);
                                let (gs, gc) = storage.sum_float_mut(global_group_idx, slot_idx);
                                *gs += ws;
                                *gc += wc;
                            }
                            CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                let (w_off, w_len) = result
                                    .compact_storage
                                    .read_min_max_str(worker_group_idx, slot_idx);
                                if w_off != u32::MAX {
                                    let w_str = result.compact_storage.str_arena.get(w_off, w_len);
                                    let (g_off, g_len) =
                                        storage.read_min_max_str(global_group_idx, slot_idx);
                                    let should_update = if g_off == u32::MAX {
                                        true
                                    } else {
                                        let g_str = storage.str_arena.get(g_off, g_len);
                                        let cmp = collation_strcmp(w_str, g_str);
                                        match kind {
                                            CompactAccKind::MinStr => cmp < 0,
                                            CompactAccKind::MaxStr => cmp > 0,
                                            _ => unreachable!(),
                                        }
                                    };
                                    if should_update {
                                        let w_str =
                                            result.compact_storage.str_arena.get(w_off, w_len);
                                        let (new_off, new_len) = storage.str_arena.alloc(w_str);
                                        storage.write_min_max_str(
                                            global_group_idx,
                                            slot_idx,
                                            new_off,
                                            new_len,
                                        );
                                    }
                                }
                            }
                            CompactAccKind::MinInt => {
                                let (w_val, w_has) = result
                                    .compact_storage
                                    .read_min_max_int(worker_group_idx, slot_idx);
                                if w_has {
                                    storage.update_min_int(global_group_idx, slot_idx, w_val);
                                }
                            }
                            CompactAccKind::MaxInt => {
                                let (w_val, w_has) = result
                                    .compact_storage
                                    .read_min_max_int(worker_group_idx, slot_idx);
                                if w_has {
                                    storage.update_max_int(global_group_idx, slot_idx, w_val);
                                }
                            }
                            CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                                merged_cd_sidecar.union_from(
                                    slot_idx,
                                    global_group_idx,
                                    &result.cd_sidecar,
                                    worker_group_idx,
                                );
                            }
                        }
                    }
                }
            }
            // Write final CountDistinct counts into compact storage
            if !merged_cd_sidecar.is_empty() {
                merged_cd_sidecar.write_counts_to_storage(storage, &compact_group_map);
            }
            let merge_us = t_merge.elapsed().as_micros() as u64;

            // Finalize
            let pre_topn_groups = compact_group_map.len();
            let mut topn_select_us: u64 = 0;
            let t_finalize = Instant::now();

            // Helper closure to convert a group's keys to datums
            let finalize_mixed_group = |_hash_key: u128,
                                        group_idx: u32,
                                        storage: &CompactAccStorage,
                                        mixed_ks: &MixedKeyStorage|
             -> Vec<(pg_sys::Datum, bool)> {
                let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                for (spec_idx, spec) in agg_specs.iter().enumerate() {
                    agg_results.push(compact_finalize(storage, group_idx, spec_idx, spec));
                }
                let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                for entry in &output_map {
                    match entry {
                        OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                        OutputEntry::Group(gi) => {
                            let kv = mixed_ks.get(group_idx, *gi);
                            match kv {
                                MixedKeyVal::Int(v) => {
                                    if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. })
                                    {
                                        row.push((i128_to_numeric_datum(v as i128), false));
                                    } else {
                                        row.push((pg_sys::Datum::from(v as usize), false));
                                    }
                                }
                                MixedKeyVal::Str(off, len) => {
                                    let s = mixed_ks.arena.get(off, len);
                                    let datum = string_to_datum(s, group_specs[*gi].type_oid);
                                    row.push((datum, false));
                                }
                                MixedKeyVal::Null => {
                                    row.push((pg_sys::Datum::from(0usize), true));
                                }
                            }
                        }
                        OutputEntry::DerivedGroup { base_gi, delta } => {
                            match mixed_ks.get(group_idx, *base_gi) {
                                MixedKeyVal::Int(v) => {
                                    row.push((pg_sys::Datum::from((v + delta) as usize), false))
                                }
                                _ => row.push((pg_sys::Datum::from(0usize), true)),
                            }
                        }
                        OutputEntry::Const(d, n) => row.push((*d, *n)),
                    }
                }
                row
            };

            let result_rows = if topn_limit > 0
                && having_filters.is_empty()
                && compact_group_map.len() > topn_limit as usize
            {
                let sort_slot = match output_map[topn_sort_col] {
                    OutputEntry::Agg(ai) => ai,
                    _ => unreachable!(),
                };
                let storage = compact_storage.as_ref().unwrap();
                let t_topn = Instant::now();
                let top_entries = compact_topn_select(
                    &compact_group_map,
                    storage,
                    sort_slot,
                    topn_limit as usize,
                    topn_ascending,
                    agg_specs[sort_slot].agg_type == AggType::Avg,
                );
                topn_select_us = t_topn.elapsed().as_micros() as u64;
                let mut rows = Vec::with_capacity(top_entries.len());
                for &(hash_key, group_idx) in &top_entries {
                    rows.push(finalize_mixed_group(
                        hash_key,
                        group_idx,
                        storage,
                        &merged_mixed_keys,
                    ));
                }
                rows
            } else {
                let storage = compact_storage.as_ref().unwrap();
                let mut rows = Vec::new();
                'par_mixed_group_loop: for (_, &group_idx) in &compact_group_map {
                    let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                    for (spec_idx, spec) in agg_specs.iter().enumerate() {
                        agg_results.push(compact_finalize(storage, group_idx, spec_idx, spec));
                    }

                    for hf in &having_filters {
                        let (datum, is_null) = agg_results[hf.agg_idx];
                        if is_null {
                            continue 'par_mixed_group_loop;
                        }
                        let val = datum.value() as i64;
                        let pass = match hf.op {
                            HavingOp::Gt => val > hf.const_val,
                            HavingOp::Lt => val < hf.const_val,
                            HavingOp::Ge => val >= hf.const_val,
                            HavingOp::Le => val <= hf.const_val,
                            HavingOp::Eq => val == hf.const_val,
                            HavingOp::Ne => val != hf.const_val,
                        };
                        if !pass {
                            continue 'par_mixed_group_loop;
                        }
                    }

                    let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                    for entry in &output_map {
                        match entry {
                            OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                            OutputEntry::Group(gi) => {
                                let kv = merged_mixed_keys.get(group_idx, *gi);
                                match kv {
                                    MixedKeyVal::Int(v) => {
                                        if matches!(
                                            group_specs[*gi].expr,
                                            GroupByExpr::Extract { .. }
                                        ) {
                                            row.push((i128_to_numeric_datum(v as i128), false));
                                        } else {
                                            row.push((pg_sys::Datum::from(v as usize), false));
                                        }
                                    }
                                    MixedKeyVal::Str(off, len) => {
                                        let s = merged_mixed_keys.arena.get(off, len);
                                        let datum = string_to_datum(s, group_specs[*gi].type_oid);
                                        row.push((datum, false));
                                    }
                                    MixedKeyVal::Null => {
                                        row.push((pg_sys::Datum::from(0usize), true));
                                    }
                                }
                            }
                            OutputEntry::DerivedGroup { base_gi, delta } => {
                                match merged_mixed_keys.get(group_idx, *base_gi) {
                                    MixedKeyVal::Int(v) => {
                                        row.push((pg_sys::Datum::from((v + delta) as usize), false))
                                    }
                                    _ => row.push((pg_sys::Datum::from(0usize), true)),
                                }
                            }
                            OutputEntry::Const(d, n) => row.push((*d, *n)),
                        }
                    }
                    rows.push(row);
                }

                // Apply top-N on full result set (HAVING path or small groups)
                if topn_limit > 0 && has_group_by && rows.len() > topn_limit as usize {
                    let si = topn_sort_col;
                    if topn_ascending {
                        rows.sort_by_key(|row| {
                            let (datum, is_null) = row[si];
                            if is_null {
                                i64::MAX
                            } else {
                                datum.value() as i64
                            }
                        });
                    } else {
                        rows.sort_by(|a, b| {
                            let (da, na) = a[si];
                            let (db, nb) = b[si];
                            let va = if na { i64::MIN } else { da.value() as i64 };
                            let vb = if nb { i64::MIN } else { db.value() as i64 };
                            vb.cmp(&va)
                        });
                    }
                    rows.truncate(topn_limit as usize);
                }
                rows
            };
            let finalize_us = t_finalize.elapsed().as_micros() as u64;

            let state = AggScanState {
                _agg_specs: agg_specs,
                _group_specs: group_specs,
                result_rows,
                result_idx: 0,
                _num_result_cols: num_result_cols,
                metadata_us,
                heap_scan_us,
                detoast_us: total_detoast_us,
                blob_cache_hits: total_cache_hits,
                blob_cache_misses: total_cache_misses,
                blob_cache_bytes_served: total_cache_bytes_served,
                decompress_us,
                agg_us,
                total_segments,
                total_rows_processed,
                batch_quals_count: batch_quals.len(),
                where_quals_null: where_quals.is_null(),
                segments_metadata_resolved: 0,
                segments_decompressed: 0,
                regex_cache_size: 0,
                regex_cache_calls: 0,
                topn_limit: if topn_limit > 0 { topn_limit as u64 } else { 0 },
                topn_sort_col: topn_sort_col as i64,
                topn_ascending,
                pre_topn_groups: pre_topn_groups as u64,
                merge_us,
                finalize_us,
                topn_select_us,
                n_workers: n_workers as u64,
                bare_limit: 0,
                wall_us: t_wall.elapsed().as_micros() as u64,
                buf_stats: super::segments::take_scan_buf_stats(),
                f8_preselected: 0,
                pscan: std::ptr::null_mut(),
                is_parallel_worker: false,
                exec_ctx: None,
            };

            let state_box = Box::new(state);
            let state_ptr = Box::into_raw(state_box);
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
        // SINGLE-THREADED PATH (original)
        // ============================================================
        // If lazy loading was used (parallel was possible but conditions
        // weren't met for compact/mixed), detoast all segments now.
        if use_lazy {
            let t_detoast = Instant::now();
            for seg in &mut all_segments {
                let dl = detoast_lazy_blobs(seg);
                total_cache_hits += dl.cache_hits;
                total_cache_misses += dl.cache_misses;
                total_cache_bytes_served += dl.cache_bytes_served;
            }
            total_detoast_us += t_detoast.elapsed().as_micros() as u64;
        }
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
                        _ => {
                            skip = true;
                            break;
                        }
                    }
                }
                if skip {
                    continue;
                }
            }

            // Time-range pruning
            if let (Some(seg_min), Some(seg_max)) = (seg.min_time, seg.max_time) {
                if time_min.is_some_and(|query_min| seg_max < query_min) {
                    continue;
                }
                if time_max.is_some_and(|query_max| seg_min > query_max) {
                    continue;
                }
            }

            // Dictionary-based LIKE pruning: skip segment if no dict entry matches
            if segment_skippable_by_dict(
                &batch_quals,
                &meta.col_names,
                &meta.segment_by,
                &seg.compressed_blobs,
            ) {
                continue;
            }

            total_segments += 1;

            let mut const_group_keys: Vec<Option<i64>> = vec![None; group_specs.len()];
            for (gi, gs) in group_specs.iter().enumerate() {
                if is_text_group_col(gs) {
                    continue;
                }
                let GroupByExpr::Extract { unit, divisor, .. } = &gs.expr else {
                    continue;
                };
                let col_idx = gs.col_idx as usize;
                let Some(col_name) = meta.col_names.get(col_idx) else {
                    continue;
                };
                let Some(cm) = seg.col_minmax.get(col_name) else {
                    continue;
                };
                const_group_keys[gi] = constant_extract_key_for_segment(cm, *divisor, unit);
            }

            let skip_numeric_decompress: Vec<bool> = (0..meta.col_names.len())
                .map(|col_idx| {
                    numeric_col_used_only_by_constant_group_keys(
                        col_idx,
                        &group_specs,
                        &const_group_keys,
                        &batch_quals,
                        &agg_specs,
                    )
                })
                .collect();

            // Decompress needed columns
            let t_dec = Instant::now();
            pg_sys::MemoryContextReset(segment_mcxt);
            let old_ctx = pg_sys::MemoryContextSwitchTo(segment_mcxt);

            let mut decompressed: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
            // Raw strings for columns that need regexp_replace (parallel to decompressed)
            let mut raw_strings: Vec<Option<Vec<Option<String>>>> = Vec::new();
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
                    raw_strings.push(None);
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
                    raw_strings.push(None);
                    seg_val_idx += 1;
                } else {
                    let blob = &seg.compressed_blobs[blob_idx];
                    let typmod = meta.col_typmods[col_idx];

                    if skip_numeric_decompress[col_idx] {
                        decompressed.push(Vec::new());
                        raw_strings.push(None);
                        blob_idx += 1;
                        continue;
                    }

                    // Fast path: COUNT(DISTINCT) on text without GROUP BY or
                    // row-level WHERE — hash directly from compressed data,
                    // skipping all datum conversion.
                    if count_distinct_only_str[col_idx] && !has_group_by && batch_quals.is_empty() {
                        let cc_ref = compression::CompressedColumnRef::from_bytes(blob);
                        let accumulators = global_accumulators.as_mut().unwrap();
                        // Find the CountDistinctStr accumulator for this column
                        for (spec_idx, spec) in agg_specs.iter().enumerate() {
                            if spec.col_idx as usize == col_idx {
                                if let AggAccumulator::CountDistinctStr { seen } =
                                    &mut accumulators[spec_idx]
                                {
                                    let non_null_count = count_non_null(
                                        cc_ref.null_bitmap,
                                        cc_ref.row_count as usize,
                                    );
                                    match cc_ref.type_tag {
                                        compression::CompressionType::Dictionary
                                        | compression::CompressionType::DictionaryLz4 => {
                                            // Dict shortcut: hash only the dict entries — O(dict_size)
                                            let norm_buf;
                                            let dict_data = if cc_ref.type_tag == compression::CompressionType::DictionaryLz4 {
                                                norm_buf = compression::dictionary::normalize_lz4(cc_ref.data);
                                                &norm_buf[..]
                                            } else {
                                                cc_ref.data
                                            };
                                            let hdr = compression::dictionary::parse_header(dict_data);
                                            for entry in &hdr.dict {
                                                seen.insert(hash128_str(entry.as_bytes()));
                                            }
                                        }
                                        compression::CompressionType::Lz4 => {
                                            let (buf, ranges) = compression::lz4::decode_to_ranges(cc_ref.data, non_null_count);
                                            let empty_hash = hash128_str(b"");
                                            let mut has_empty = false;
                                            for &(off, len) in &ranges {
                                                if len == 0 {
                                                    has_empty = true;
                                                } else {
                                                    seen.insert(hash128_str(&buf[off..off + len]));
                                                }
                                            }
                                            if has_empty { seen.insert(empty_hash); }
                                        }
                                        compression::CompressionType::Lz4Blocked => {
                                            let (buf, ranges) = compression::lz4::decode_to_ranges_blocked(cc_ref.data, non_null_count, None);
                                            let empty_hash = hash128_str(b"");
                                            let mut has_empty = false;
                                            for &(off, len) in &ranges {
                                                if len == 0 {
                                                    has_empty = true;
                                                } else {
                                                    seen.insert(hash128_str(&buf[off..off + len]));
                                                }
                                            }
                                            if has_empty { seen.insert(empty_hash); }
                                        }
                                        compression::CompressionType::Constant
                                            // Single constant string — hash the raw bytes
                                            if non_null_count > 0 => {
                                                seen.insert(hash128_str(cc_ref.data));
                                            }
                                        _ => {}
                                    }
                                }
                                break;
                            }
                        }
                        // Push empty so the row loop skips this column
                        decompressed.push(Vec::new());
                        raw_strings.push(None);
                        blob_idx += 1;
                        continue;
                    }

                    // Fast path: COUNT(DISTINCT) on integer without GROUP BY or
                    // row-level WHERE — decode directly and insert into HashSet,
                    // skipping all datum conversion.
                    if count_distinct_only_int[col_idx] && !has_group_by && batch_quals.is_empty() {
                        let cc_ref = compression::CompressedColumnRef::from_bytes(blob);
                        let accumulators = global_accumulators.as_mut().unwrap();
                        for (spec_idx, spec) in agg_specs.iter().enumerate() {
                            if spec.col_idx as usize == col_idx {
                                if let AggAccumulator::CountDistinctInt { seen } =
                                    &mut accumulators[spec_idx]
                                {
                                    let non_null_count = count_non_null(
                                        cc_ref.null_bitmap,
                                        cc_ref.row_count as usize,
                                    );
                                    if non_null_count > 0 {
                                        let is_i64 = type_oid == pg_sys::INT8OID;
                                        match cc_ref.type_tag {
                                            compression::CompressionType::Constant => {
                                                if is_i64 {
                                                    let v = i64::from_le_bytes(
                                                        cc_ref.data[..8].try_into().unwrap(),
                                                    );
                                                    seen.insert(v);
                                                } else {
                                                    let v = i32::from_le_bytes(
                                                        cc_ref.data[..4].try_into().unwrap(),
                                                    );
                                                    seen.insert(v as i64);
                                                }
                                            }
                                            compression::CompressionType::ForBitpacked => {
                                                if is_i64 {
                                                    let vals =
                                                        compression::bitpacked::decode_for_i64(
                                                            cc_ref.data,
                                                            non_null_count,
                                                        );
                                                    for v in vals {
                                                        seen.insert(v);
                                                    }
                                                } else {
                                                    let vals =
                                                        compression::bitpacked::decode_for_i32(
                                                            cc_ref.data,
                                                            non_null_count,
                                                        );
                                                    for v in vals {
                                                        seen.insert(v as i64);
                                                    }
                                                }
                                            }
                                            compression::CompressionType::DeltaVarint => {
                                                if is_i64 {
                                                    let vals = compression::integer::decode_i64(
                                                        cc_ref.data,
                                                        non_null_count,
                                                    );
                                                    for v in vals {
                                                        seen.insert(v);
                                                    }
                                                } else {
                                                    let vals = compression::integer::decode_i32(
                                                        cc_ref.data,
                                                        non_null_count,
                                                    );
                                                    for v in vals {
                                                        seen.insert(v as i64);
                                                    }
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                break;
                            }
                        }
                        // Push empty so the row loop skips this column
                        decompressed.push(Vec::new());
                        raw_strings.push(None);
                        blob_idx += 1;
                        continue;
                    }

                    if raw_string_cols[col_idx] {
                        // Dictionary-optimized path: pre-warm regex cache from dict entries only
                        let cc_ref = compression::CompressedColumnRef::from_bytes(blob);
                        if cc_ref.type_tag == compression::CompressionType::Dictionary
                            || cc_ref.type_tag == compression::CompressionType::DictionaryLz4
                        {
                            let total_count = cc_ref.row_count as usize;
                            let non_null_count = count_non_null(cc_ref.null_bitmap, total_count);
                            let norm_buf;
                            let dict_data =
                                if cc_ref.type_tag == compression::CompressionType::DictionaryLz4 {
                                    norm_buf = compression::dictionary::normalize_lz4(cc_ref.data);
                                    &norm_buf[..]
                                } else {
                                    cc_ref.data
                                };
                            let (dict_entries, indices) =
                                compression::dictionary::decode_dict_and_indices(
                                    dict_data,
                                    non_null_count,
                                );

                            // Pre-warm regex cache from dict entries only — O(dict_size) calls
                            for &entry in &dict_entries {
                                let key = entry.to_string();
                                if !regex_cache.contains_key(&key) {
                                    for rgi in &regexp_group_infos {
                                        if group_specs[rgi.group_idx].col_idx as usize == col_idx {
                                            regex_cache_calls += 1;
                                            let input_datum = {
                                                let text = pg_sys::cstring_to_text_with_len(
                                                    entry.as_ptr() as *const _,
                                                    entry.len() as i32,
                                                );
                                                pg_sys::Datum::from(text as usize)
                                            };
                                            let result_datum = pg_sys::OidFunctionCall3Coll(
                                                rgi.func_oid,
                                                rgi.collation,
                                                input_datum,
                                                rgi.pattern_datum,
                                                rgi.replacement_datum,
                                            );
                                            let cstr = pg_sys::text_to_cstring(
                                                result_datum.cast_mut_ptr(),
                                            );
                                            let s = std::ffi::CStr::from_ptr(cstr)
                                                .to_string_lossy()
                                                .into_owned();
                                            pg_sys::pfree(cstr as *mut _);
                                            regex_cache.insert(key.clone(), s);
                                            break;
                                        }
                                    }
                                }
                            }

                            // Build per-row strings from cached regex results via dict index
                            let has_ne_empty = batch_quals.iter().any(|bq| {
                                bq.col_idx == col_idx
                                    && bq.text_const.as_deref() == Some("")
                                    && bq.op == BatchCompareOp::Ne
                            });
                            let ne_sel = if has_ne_empty {
                                compression::dictionary::check_ne_empty(dict_data, non_null_count)
                            } else {
                                Vec::new()
                            };

                            let nn_strings: Vec<String> = indices
                                .iter()
                                .map(|&idx| dict_entries[idx as usize].to_string())
                                .collect();

                            // Reinsert nulls
                            if cc_ref.null_bitmap.is_empty() {
                                let strings: Vec<Option<String>> =
                                    nn_strings.into_iter().map(Some).collect();
                                let datums: Vec<(pg_sys::Datum, bool)> = strings
                                    .iter()
                                    .map(|s| match s {
                                        Some(_) => (pg_sys::Datum::from(0usize), false),
                                        None => (pg_sys::Datum::from(0usize), true),
                                    })
                                    .collect();
                                decompressed.push(datums);
                                raw_strings.push(Some(strings));
                                if !ne_sel.is_empty() {
                                    if pre_selection.is_empty() {
                                        pre_selection = ne_sel;
                                    } else {
                                        for (ps, s) in pre_selection.iter_mut().zip(ne_sel.iter()) {
                                            *ps = *ps && *s;
                                        }
                                    }
                                }
                            } else {
                                let mut strings = Vec::with_capacity(total_count);
                                let mut sel = if has_ne_empty {
                                    Vec::with_capacity(total_count)
                                } else {
                                    Vec::new()
                                };
                                let mut val_idx = 0;
                                for i in 0..total_count {
                                    let is_null = (cc_ref.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
                                    if is_null {
                                        strings.push(None);
                                        if has_ne_empty {
                                            sel.push(false);
                                        }
                                    } else {
                                        strings.push(Some(nn_strings[val_idx].clone()));
                                        if has_ne_empty && !ne_sel.is_empty() {
                                            sel.push(ne_sel[val_idx]);
                                        } else if has_ne_empty {
                                            sel.push(true);
                                        }
                                        val_idx += 1;
                                    }
                                }
                                let datums: Vec<(pg_sys::Datum, bool)> = strings
                                    .iter()
                                    .map(|s| match s {
                                        Some(_) => (pg_sys::Datum::from(0usize), false),
                                        None => (pg_sys::Datum::from(0usize), true),
                                    })
                                    .collect();
                                decompressed.push(datums);
                                raw_strings.push(Some(strings));
                                if !sel.is_empty() {
                                    if pre_selection.is_empty() {
                                        pre_selection = sel;
                                    } else {
                                        for (ps, s) in pre_selection.iter_mut().zip(sel.iter()) {
                                            *ps = *ps && *s;
                                        }
                                    }
                                }
                            }
                        } else {
                            // Non-dictionary: fall back to existing path
                            let (strings, sel) =
                                decompress_text_blob_to_raw_strings(blob, &batch_quals, col_idx);
                            let datums: Vec<(pg_sys::Datum, bool)> = strings
                                .iter()
                                .map(|s| match s {
                                    Some(_) => (pg_sys::Datum::from(0usize), false),
                                    None => (pg_sys::Datum::from(0usize), true),
                                })
                                .collect();
                            decompressed.push(datums);
                            raw_strings.push(Some(strings));
                            if !sel.is_empty() {
                                if pre_selection.is_empty() {
                                    pre_selection = sel;
                                } else {
                                    for (ps, s) in pre_selection.iter_mut().zip(sel.iter()) {
                                        *ps = *ps && *s;
                                    }
                                }
                            }
                        }
                    } else if length_cols[col_idx] {
                        // Length-only column: decompress as int4 lengths.
                        let has_ne_empty = batch_quals.iter().any(|bq| {
                            bq.col_idx == col_idx
                                && bq.text_const.as_deref() == Some("")
                                && bq.op == BatchCompareOp::Ne
                        });
                        let (datums, len_sel) = decompress_text_blob_to_lengths(blob, has_ne_empty);
                        decompressed.push(datums);
                        raw_strings.push(None);
                        if !len_sel.is_empty() {
                            if pre_selection.is_empty() {
                                pre_selection = len_sel;
                            } else {
                                for (ps, ls) in pre_selection.iter_mut().zip(len_sel.iter()) {
                                    *ps = *ps && *ls;
                                }
                            }
                        }
                    } else {
                        // Normal text/non-text column
                        let like_qual = batch_quals.iter().find(|bq| {
                            bq.col_idx == col_idx
                                && matches!(bq.op, BatchCompareOp::Like | BatchCompareOp::NotLike)
                        });

                        let text_eq_qual = batch_quals.iter().find(|bq| {
                            bq.col_idx == col_idx
                                && bq.text_const.is_some()
                                && matches!(bq.op, BatchCompareOp::Eq | BatchCompareOp::Ne)
                        });
                        let text_in_qual = batch_quals.iter().find(|bq| {
                            bq.col_idx == col_idx
                                && bq.in_list_text.is_some()
                                && bq.op == BatchCompareOp::InList
                        });

                        if let Some(bq) = like_qual {
                            let strat = bq.like_strategy.as_ref().unwrap();
                            let neg = bq.op == BatchCompareOp::NotLike;
                            let (datums, like_sel) = decompress_text_blob_with_like_filter(
                                blob, type_oid, typmod, strat, neg, None,
                            );
                            decompressed.push(datums);
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
                                blob, type_oid, typmod, const_str, is_ne, None,
                            );
                            decompressed.push(datums);
                            if pre_selection.is_empty() {
                                pre_selection = eq_sel;
                            } else {
                                for (ps, es) in pre_selection.iter_mut().zip(eq_sel.iter()) {
                                    *ps = *ps && *es;
                                }
                            }
                        } else if let Some(bq) = text_in_qual {
                            let strs = bq.in_list_text.as_ref().unwrap();
                            let (datums, in_sel) = decompress_text_blob_with_in_filter(
                                blob, type_oid, typmod, strs, /* is_not_in */ false, None,
                            );
                            decompressed.push(datums);
                            if pre_selection.is_empty() {
                                pre_selection = in_sel;
                            } else {
                                for (ps, is_) in pre_selection.iter_mut().zip(in_sel.iter()) {
                                    *ps = *ps && *is_;
                                }
                            }
                        } else {
                            let type_name = pg_type_name(type_oid);
                            let datums =
                                decompress_blob_to_datums(blob, &type_name, type_oid, typmod);
                            decompressed.push(datums);
                        }
                        raw_strings.push(None);
                    }
                    blob_idx += 1;
                }
            }

            pg_sys::MemoryContextSwitchTo(old_ctx);
            decompress_us += t_dec.elapsed().as_micros() as u64;

            let row_count = seg.row_count as usize;

            // Extract text GROUP BY info: intern strings and build per-row u32 ID vectors.
            // Handles both dictionary-encoded and LZ4-encoded text columns.
            // Build per-segment text column data for GROUP BY.
            // Keeps decoded string data alive during the row loop for O(1) &str access.
            seg_text_columns.clear();
            seg_text_columns.resize_with(meta.col_names.len(), || None);
            {
                let mut blob_idx2 = 0;
                let mut seg_val_idx2 = 0;
                for (col_idx, col_name) in meta.col_names.iter().enumerate() {
                    if meta.segment_by.contains(col_name) {
                        if needed_cols[col_idx] && text_group_cols[col_idx] {
                            let val = &seg.segment_values[seg_val_idx2];
                            seg_text_columns[col_idx] = Some(SegTextColumn::SegBy(val.clone()));
                        }
                        seg_val_idx2 += 1;
                        continue;
                    }
                    if needed_cols[col_idx] && text_group_cols[col_idx] {
                        let blob = &seg.compressed_blobs[blob_idx2];
                        if !blob.is_empty() {
                            let cc_ref = compression::CompressedColumnRef::from_bytes(blob);
                            let total = cc_ref.row_count as usize;
                            let nn_count = count_non_null(cc_ref.null_bitmap, total);

                            let seg_col = match cc_ref.type_tag {
                                compression::CompressionType::Dictionary
                                | compression::CompressionType::DictionaryLz4 => {
                                    let norm_buf;
                                    let dict_data = if cc_ref.type_tag
                                        == compression::CompressionType::DictionaryLz4
                                    {
                                        norm_buf =
                                            compression::dictionary::normalize_lz4(cc_ref.data);
                                        &norm_buf[..]
                                    } else {
                                        cc_ref.data
                                    };
                                    let (dict_entries, nn_indices) =
                                        compression::dictionary::decode_dict_and_indices(
                                            dict_data, nn_count,
                                        );
                                    let entries: Vec<String> =
                                        dict_entries.iter().map(|&s| s.to_string()).collect();

                                    // Expand nn_indices to full-row indices (u32::MAX for nulls)
                                    let row_to_entry = if cc_ref.null_bitmap.is_empty() {
                                        nn_indices.iter().map(|&idx| idx as u32).collect()
                                    } else {
                                        let mut re = Vec::with_capacity(total);
                                        let mut vi = 0;
                                        for i in 0..total {
                                            let is_null =
                                                (cc_ref.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
                                            if is_null {
                                                re.push(u32::MAX);
                                            } else {
                                                re.push(nn_indices[vi] as u32);
                                                vi += 1;
                                            }
                                        }
                                        re
                                    };
                                    SegTextColumn::Dict {
                                        entries,
                                        row_to_entry,
                                    }
                                }
                                compression::CompressionType::Lz4
                                | compression::CompressionType::Lz4Blocked => {
                                    let (buf, ranges) = if cc_ref.type_tag
                                        == compression::CompressionType::Lz4
                                    {
                                        compression::lz4::decode_to_ranges(cc_ref.data, nn_count)
                                    } else {
                                        compression::lz4::decode_to_ranges_blocked(
                                            cc_ref.data,
                                            nn_count,
                                            None,
                                        )
                                    };

                                    // Expand ranges to full-row ranges (u32::MAX for nulls)
                                    let row_to_range = if cc_ref.null_bitmap.is_empty() {
                                        ranges
                                            .iter()
                                            .map(|&(off, len)| (off as u32, len as u16))
                                            .collect()
                                    } else {
                                        let mut rr = Vec::with_capacity(total);
                                        let mut vi = 0;
                                        for i in 0..total {
                                            let is_null =
                                                (cc_ref.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
                                            if is_null {
                                                rr.push((u32::MAX, 0u16));
                                            } else {
                                                let (off, len) = ranges[vi];
                                                rr.push((off as u32, len as u16));
                                                vi += 1;
                                            }
                                        }
                                        rr
                                    };
                                    SegTextColumn::Lz4 { buf, row_to_range }
                                }
                                _ => {
                                    blob_idx2 += 1;
                                    continue;
                                }
                            };
                            seg_text_columns[col_idx] = Some(seg_col);
                        }
                    }
                    blob_idx2 += 1;
                }
            }

            // Evaluate batch quals (WHERE) if any.
            // pre_selection seeds the selection vector so that rows already
            // filtered by LIKE during decompression are skipped (their dummy
            // datums are never dereferenced).
            let selection =
                evaluate_batch_quals(&decompressed, row_count, &batch_quals, pre_selection);

            // Pre-compute CaseWhen GROUP BY columns into SegTextColumn
            let case_when_seg_cols: Vec<Option<SegTextColumn>> = group_specs
                .iter()
                .map(|gs| {
                    if let GroupByExpr::CaseWhen(ref spec) = gs.expr {
                        Some(apply_case_when_to_seg_col(
                            spec,
                            &decompressed,
                            &seg_text_columns,
                            row_count,
                            &selection,
                        ))
                    } else {
                        None
                    }
                })
                .collect();

            // Fast path: when no GROUP BY and all agg specs are SUM/AVG on the
            // same column with Column or AddConst expr, compute base_sum once
            // and derive each result as base_sum + const_offset * non_null_count.
            // This turns O(N * num_aggs) into O(N + num_aggs).
            if !has_group_by && agg_specs.len() > 1 {
                let first_col = agg_specs[0].col_idx;
                let first_type = agg_specs[0].col_type_oid;
                let all_same_col_sum = agg_specs.iter().all(|s| {
                    s.col_idx == first_col
                        && (s.agg_type == AggType::Sum || s.agg_type == AggType::Avg)
                        && (s.expr_kind == AggExpr::Column || s.expr_kind == AggExpr::AddConst)
                });
                if all_same_col_sum {
                    let col = &decompressed[first_col as usize];
                    if !col.is_empty() {
                        let accumulators = global_accumulators.as_mut().unwrap();
                        let mut base_sum: i128 = 0;
                        let mut non_null_count: i64 = 0;
                        let use_float = matches!(first_type, pg_sys::FLOAT4OID | pg_sys::FLOAT8OID);
                        let mut base_sum_f: f64 = 0.0;
                        for row in 0..row_count {
                            if !selection.is_empty() && !selection[row] {
                                continue;
                            }
                            if !col[row].1 {
                                if use_float {
                                    base_sum_f += datum_to_f64(col[row].0, first_type);
                                } else {
                                    base_sum += datum_to_i128(col[row].0, first_type);
                                }
                                non_null_count += 1;
                            }
                        }
                        total_rows_processed += if selection.is_empty() {
                            row_count as u64
                        } else {
                            selection.iter().filter(|&&v| v).count() as u64
                        };
                        for (spec_idx, spec) in agg_specs.iter().enumerate() {
                            let acc = &mut accumulators[spec_idx];
                            if use_float {
                                if let AggAccumulator::SumFloat { sum, count } = acc {
                                    *sum += base_sum_f
                                        + spec.const_offset as f64 * non_null_count as f64;
                                    *count += non_null_count;
                                }
                            } else {
                                if let AggAccumulator::SumInt { sum, count } = acc {
                                    *sum += base_sum
                                        + spec.const_offset as i128 * non_null_count as i128;
                                    *count += non_null_count;
                                }
                            }
                        }
                        continue; // skip the generic aggregate loop for this segment
                    }
                }
            }

            // Reusable buffers for the aggregate loop (avoid per-row heap allocation)
            let mut key_ref: Vec<GroupKeyRef> = Vec::with_capacity(group_specs.len());
            let mut regex_results: Vec<Option<String>> = Vec::new();

            // ============================================================
            // COMPACT PATH: packed u128 keys + flat byte-buffer accumulators
            // ============================================================
            if use_compact_keys && use_compact_accs {
                let storage = compact_storage.as_mut().unwrap();
                let num_group_keys = group_specs.len();

                for row in 0..row_count {
                    if !selection.is_empty() && !selection[row] {
                        continue;
                    }
                    total_rows_processed += 1;

                    // Build packed u128 key from integer GROUP BY columns
                    let mut int_keys: [i64; 2] = [0; 2];
                    let mut has_null = false;
                    for (ki, gs) in group_specs.iter().enumerate() {
                        let col = &decompressed[gs.col_idx as usize];
                        if col.is_empty() || col[row].1 {
                            has_null = true;
                            break;
                        }
                        int_keys[ki] = match &gs.expr {
                            GroupByExpr::DateTrunc { unit_usecs, .. } => {
                                let pg_usec = col[row].0.value() as i64;
                                pg_usec.div_euclid(*unit_usecs) * *unit_usecs
                            }
                            GroupByExpr::Extract { unit, divisor, .. } => {
                                eval_extract(col[row].0.value() as i64, *divisor, unit)
                            }
                            GroupByExpr::AddConst { offset, .. } => {
                                col[row].0.value() as i64 + offset
                            }
                            GroupByExpr::Column => col[row].0.value() as i64,
                            _ => unreachable!(),
                        };
                    }

                    // Skip null groups (they don't appear in GROUP BY results)
                    if has_null {
                        continue;
                    }

                    let packed = if num_group_keys == 1 {
                        pack_int_key_1(int_keys[0])
                    } else {
                        pack_int_keys_2(int_keys[0], int_keys[1])
                    };

                    // Lookup or insert group.
                    // Cap hashmap growth: above 32M entries, reserve in 8M
                    // increments instead of letting hashbrown double.
                    if compact_group_map.len() == compact_group_map.capacity() {
                        let cap = compact_group_map.capacity();
                        let extra = if cap >= 32_000_000 {
                            8_000_000 // ~170MB at 21B/slot
                        } else {
                            0 // let hashbrown double normally for small maps
                        };
                        if extra > 0 {
                            compact_group_map.reserve(extra);
                        }
                    }
                    let group_idx = match compact_group_map.entry(packed) {
                        hashbrown::hash_map::Entry::Occupied(e) => *e.get(),
                        hashbrown::hash_map::Entry::Vacant(e) => {
                            let idx = storage.alloc_group();
                            cd_sidecar.alloc_group();
                            e.insert(idx);
                            idx
                        }
                    };

                    // Update compact accumulators
                    for (spec_idx, spec) in agg_specs.iter().enumerate() {
                        let (_, kind) = storage.layout.slots[spec_idx];
                        match kind {
                            CompactAccKind::Count => match spec.agg_type {
                                AggType::CountStar => {
                                    *storage.count_mut(group_idx, spec_idx) += 1;
                                }
                                AggType::Count => {
                                    let col = &decompressed[spec.col_idx as usize];
                                    if !col.is_empty() && !col[row].1 {
                                        *storage.count_mut(group_idx, spec_idx) += 1;
                                    }
                                }
                                _ => {}
                            },
                            CompactAccKind::SumInt => {
                                let col = &decompressed[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    let v = datum_to_i128(col[row].0, spec.col_type_oid);
                                    let (sum, count) = storage.sum_int_mut(group_idx, spec_idx);
                                    if spec.expr_kind == AggExpr::AddConst {
                                        *sum += v + spec.const_offset as i128;
                                    } else {
                                        *sum += v;
                                    }
                                    *count += 1;
                                }
                            }
                            CompactAccKind::SumIntNarrow => {
                                let col = &decompressed[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    let v = col[row].0.value() as i64;
                                    let (sum, count) =
                                        storage.sum_int_narrow_mut(group_idx, spec_idx);
                                    if spec.expr_kind == AggExpr::AddConst {
                                        *sum += v + spec.const_offset;
                                    } else {
                                        *sum += v;
                                    }
                                    *count += 1;
                                }
                            }
                            CompactAccKind::SumFloat => {
                                let col = &decompressed[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    let v = datum_to_f64(col[row].0, spec.col_type_oid);
                                    let (sum, count) = storage.sum_float_mut(group_idx, spec_idx);
                                    if spec.expr_kind == AggExpr::AddConst {
                                        *sum += v + spec.const_offset as f64;
                                    } else {
                                        *sum += v;
                                    }
                                    *count += 1;
                                }
                            }
                            CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                let col_idx = spec.col_idx as usize;
                                if let Some(ref rs) = raw_strings[col_idx]
                                    && let Some(ref s) = rs[row]
                                {
                                    let (cur_off, cur_len) =
                                        storage.read_min_max_str(group_idx, spec_idx);
                                    let should_update = if cur_off == u32::MAX {
                                        true
                                    } else {
                                        let cur = storage.str_arena.get(cur_off, cur_len);
                                        let cmp = collation_strcmp(s, cur);
                                        match kind {
                                            CompactAccKind::MinStr => cmp < 0,
                                            CompactAccKind::MaxStr => cmp > 0,
                                            _ => unreachable!(),
                                        }
                                    };
                                    if should_update {
                                        let (new_off, new_len) = storage.str_arena.alloc(s);
                                        storage.write_min_max_str(
                                            group_idx, spec_idx, new_off, new_len,
                                        );
                                    }
                                }
                            }
                            CompactAccKind::MinInt => {
                                let col = &decompressed[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    let v = col[row].0.value() as i64;
                                    storage.update_min_int(group_idx, spec_idx, v);
                                }
                            }
                            CompactAccKind::MaxInt => {
                                let col = &decompressed[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    let v = col[row].0.value() as i64;
                                    storage.update_max_int(group_idx, spec_idx, v);
                                }
                            }
                            CompactAccKind::CountDistinctInt => {
                                let col = &decompressed[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    cd_sidecar.insert_int(
                                        spec_idx,
                                        group_idx,
                                        col[row].0.value() as i64,
                                    );
                                }
                            }
                            CompactAccKind::CountDistinctStr => {
                                let col_idx = spec.col_idx as usize;
                                if let Some(ref rs) = raw_strings[col_idx]
                                    && let Some(ref s) = rs[row]
                                {
                                    cd_sidecar.insert_str(
                                        spec_idx,
                                        group_idx,
                                        hash128_str(s.as_bytes()),
                                    );
                                }
                            }
                        }
                    }
                }
            }
            // ============================================================
            // GENERIC PATH: original GroupKey + AggAccumulator
            // ============================================================
            else {
                for row in 0..row_count {
                    if !selection.is_empty() && !selection[row] {
                        continue;
                    }

                    total_rows_processed += 1;

                    let accumulators = if has_group_by {
                        // Clear key_ref first to release borrows on regex_results
                        key_ref.clear();
                        // Pre-compute regex results for this row (needs mutable regex_cache,
                        // so must be done before building borrowed key_ref)
                        regex_results.clear();
                        if has_regexp_group {
                            for (gi, gs) in group_specs.iter().enumerate() {
                                if let GroupByExpr::RegexpReplace { .. } = &gs.expr {
                                    let rs = raw_strings[gs.col_idx as usize].as_ref().unwrap();
                                    if let Some(ref input_str) = rs[row] {
                                        let rgi = regexp_group_infos
                                            .iter()
                                            .find(|r| r.group_idx == gi)
                                            .unwrap();
                                        let result = regex_cache
                                            .entry(input_str.clone())
                                            .or_insert_with(|| {
                                                regex_cache_calls += 1;
                                                let input_datum = {
                                                    let text = pg_sys::cstring_to_text_with_len(
                                                        input_str.as_ptr() as *const _,
                                                        input_str.len() as i32,
                                                    );
                                                    pg_sys::Datum::from(text as usize)
                                                };
                                                let result_datum = pg_sys::OidFunctionCall3Coll(
                                                    rgi.func_oid,
                                                    rgi.collation,
                                                    input_datum,
                                                    rgi.pattern_datum,
                                                    rgi.replacement_datum,
                                                );
                                                let cstr = pg_sys::text_to_cstring(
                                                    result_datum.cast_mut_ptr(),
                                                );
                                                let s = std::ffi::CStr::from_ptr(cstr)
                                                    .to_string_lossy()
                                                    .into_owned();
                                                pg_sys::pfree(cstr as *mut _);
                                                s
                                            });
                                        regex_results.push(Some(result.clone()));
                                    } else {
                                        regex_results.push(None);
                                    }
                                }
                            }
                        }

                        // Build temporary borrowed key (reuse buffer, no heap alloc)
                        let mut regex_idx = 0;
                        for (gi, gs) in group_specs.iter().enumerate() {
                            // CaseWhen has col_idx=-1, handle separately via pre-computed SegTextColumn
                            if let GroupByExpr::CaseWhen(_) = &gs.expr {
                                if let Some(Some(seg_col)) = case_when_seg_cols.get(gi) {
                                    match seg_col.get_str(row) {
                                        Some(s) => key_ref.push(GroupKeyRef::from_str(s)),
                                        None => key_ref.push(GroupKeyRef::Null),
                                    }
                                } else {
                                    key_ref.push(GroupKeyRef::Null);
                                }
                                continue;
                            }
                            if let Some(v) = const_group_keys[gi] {
                                key_ref.push(GroupKeyRef::Int(v));
                                continue;
                            }
                            let col = &decompressed[gs.col_idx as usize];
                            if col.is_empty() || col[row].1 {
                                key_ref.push(GroupKeyRef::Null);
                                if matches!(&gs.expr, GroupByExpr::RegexpReplace { .. }) {
                                    regex_idx += 1;
                                }
                            } else {
                                match &gs.expr {
                                    GroupByExpr::RegexpReplace { .. } => {
                                        match &regex_results[regex_idx] {
                                            Some(s) => {
                                                key_ref.push(GroupKeyRef::from_str(s.as_str()))
                                            }
                                            None => key_ref.push(GroupKeyRef::Null),
                                        }
                                        regex_idx += 1;
                                    }
                                    GroupByExpr::DateTrunc { unit_usecs, .. } => {
                                        let pg_usec = col[row].0.value() as i64;
                                        let truncated =
                                            pg_usec.div_euclid(*unit_usecs) * *unit_usecs;
                                        key_ref.push(GroupKeyRef::Int(truncated));
                                    }
                                    GroupByExpr::Extract { unit, divisor, .. } => {
                                        let extracted =
                                            eval_extract(col[row].0.value() as i64, *divisor, unit);
                                        key_ref.push(GroupKeyRef::Int(extracted));
                                    }
                                    GroupByExpr::AddConst { offset, .. } => {
                                        let datum = col[row].0;
                                        let v = datum.value() as i64;
                                        key_ref.push(GroupKeyRef::Int(v + offset));
                                    }
                                    GroupByExpr::Column => {
                                        // Text GROUP BY: get &str from decoded segment data
                                        if let Some(ref seg_text) =
                                            seg_text_columns[gs.col_idx as usize]
                                        {
                                            match seg_text.get_str(row) {
                                                Some(s) => key_ref.push(GroupKeyRef::from_str(s)),
                                                None => key_ref.push(GroupKeyRef::Null),
                                            }
                                        } else {
                                            let datum = col[row].0;
                                            key_ref.push(GroupKeyRef::Int(datum.value() as i64));
                                        }
                                    }
                                    GroupByExpr::CaseWhen(_) => unreachable!(),
                                }
                            }
                        }

                        // Use hashbrown raw_entry to avoid cloning the key for existing groups
                        let h = hash_group_key_ref(&key_ref);
                        let group_idx = match group_map
                            .raw_entry_mut()
                            .from_hash(h, |stored| keys_match(stored, &key_ref, &string_arena))
                        {
                            hashbrown::hash_map::RawEntryMut::Occupied(e) => *e.into_mut(),
                            hashbrown::hash_map::RawEntryMut::Vacant(e) => {
                                let owned_key = if is_single_group_key {
                                    GroupKey::Single(key_ref[0].resolve(&mut string_arena))
                                } else {
                                    GroupKey::Multi(
                                        key_ref
                                            .iter()
                                            .map(|r| r.resolve(&mut string_arena))
                                            .collect(),
                                    )
                                };
                                let idx = (flat_accs.len() / n_agg_specs) as u32;
                                for proto in &prototype_accumulators {
                                    flat_accs.push(proto.clone_fresh());
                                }
                                e.insert_with_hasher(h, owned_key, idx, |k| {
                                    hash_group_key(k, &string_arena)
                                });
                                idx
                            }
                        };
                        &mut flat_accs[group_idx as usize * n_agg_specs
                            ..(group_idx as usize + 1) * n_agg_specs]
                    } else {
                        global_accumulators.as_mut().unwrap().as_mut_slice()
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
                                if !col.is_empty()
                                    && !col[row].1
                                    && let AggAccumulator::Count { count } = acc
                                {
                                    *count += 1;
                                }
                            }
                            AggType::Sum | AggType::Avg => {
                                // When LengthOf + raw_string_cols, compute length from raw strings
                                // (decompressed has dummy 0 datums for raw_string_cols columns)
                                if spec.expr_kind == AggExpr::LengthOf
                                    && raw_string_cols
                                        .get(spec.col_idx as usize)
                                        .copied()
                                        .unwrap_or(false)
                                {
                                    if let Some(ref rs) = raw_strings[spec.col_idx as usize]
                                        && let Some(ref s) = rs[row]
                                    {
                                        match acc {
                                            AggAccumulator::SumInt { sum, count } => {
                                                *sum += s.chars().count() as i128;
                                                *count += 1;
                                            }
                                            AggAccumulator::SumFloat { sum, count } => {
                                                *sum += s.chars().count() as f64;
                                                *count += 1;
                                            }
                                            _ => {}
                                        }
                                    }
                                } else {
                                    let col = &decompressed[spec.col_idx as usize];
                                    if !col.is_empty() && !col[row].1 {
                                        let datum = col[row].0;
                                        match acc {
                                            AggAccumulator::SumInt { sum, count } => {
                                                let v = datum_to_i128(datum, spec.col_type_oid);
                                                if spec.expr_kind == AggExpr::AddConst {
                                                    *sum += v + spec.const_offset as i128;
                                                } else {
                                                    *sum += v;
                                                }
                                                *count += 1;
                                            }
                                            AggAccumulator::SumFloat { sum, count } => {
                                                let v = datum_to_f64(datum, spec.col_type_oid);
                                                if spec.expr_kind == AggExpr::AddConst {
                                                    *sum += v + spec.const_offset as f64;
                                                } else {
                                                    *sum += v;
                                                }
                                                *count += 1;
                                            }
                                            _ => {}
                                        }
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
                                            let cstr =
                                                pg_sys::text_to_cstring(datum.cast_mut_ptr());
                                            let bytes = std::ffi::CStr::from_ptr(cstr).to_bytes();
                                            let hash = hash128_str(bytes);
                                            pg_sys::pfree(cstr as *mut _);
                                            seen.insert(hash);
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            AggType::Min => {
                                // For text columns referenced by raw_string_cols, use raw strings
                                if raw_string_cols
                                    .get(spec.col_idx as usize)
                                    .copied()
                                    .unwrap_or(false)
                                {
                                    if let Some(ref rs) = raw_strings[spec.col_idx as usize]
                                        && let Some(ref s) = rs[row]
                                        && let AggAccumulator::MinStr { val } = acc
                                        && val
                                            .as_ref()
                                            .is_none_or(|cur| collation_strcmp(s, cur) < 0)
                                    {
                                        *val = Some(s.clone());
                                    }
                                } else {
                                    let col = &decompressed[spec.col_idx as usize];
                                    if !col.is_empty() && !col[row].1 {
                                        let datum = col[row].0;
                                        match acc {
                                            AggAccumulator::MinInt { val } => {
                                                let v = datum.value() as i64;
                                                if val.is_none_or(|cur| v < cur) {
                                                    *val = Some(v);
                                                }
                                            }
                                            AggAccumulator::MinFloat { val } => {
                                                let v = datum_to_f64(datum, spec.col_type_oid);
                                                if val.is_none_or(|cur| v < cur) {
                                                    *val = Some(v);
                                                }
                                            }
                                            AggAccumulator::MinStr { val } => {
                                                let cstr =
                                                    pg_sys::text_to_cstring(datum.cast_mut_ptr());
                                                let s = std::ffi::CStr::from_ptr(cstr)
                                                    .to_string_lossy()
                                                    .into_owned();
                                                pg_sys::pfree(cstr as *mut _);
                                                if val
                                                    .as_ref()
                                                    .is_none_or(|cur| collation_strcmp(&s, cur) < 0)
                                                {
                                                    *val = Some(s);
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                            AggType::Max => {
                                if raw_string_cols
                                    .get(spec.col_idx as usize)
                                    .copied()
                                    .unwrap_or(false)
                                {
                                    if let Some(ref rs) = raw_strings[spec.col_idx as usize]
                                        && let Some(ref s) = rs[row]
                                        && let AggAccumulator::MaxStr { val } = acc
                                        && val
                                            .as_ref()
                                            .is_none_or(|cur| collation_strcmp(s, cur) > 0)
                                    {
                                        *val = Some(s.clone());
                                    }
                                } else {
                                    let col = &decompressed[spec.col_idx as usize];
                                    if !col.is_empty() && !col[row].1 {
                                        let datum = col[row].0;
                                        match acc {
                                            AggAccumulator::MaxInt { val } => {
                                                let v = datum.value() as i64;
                                                if val.is_none_or(|cur| v > cur) {
                                                    *val = Some(v);
                                                }
                                            }
                                            AggAccumulator::MaxFloat { val } => {
                                                let v = datum_to_f64(datum, spec.col_type_oid);
                                                if val.is_none_or(|cur| v > cur) {
                                                    *val = Some(v);
                                                }
                                            }
                                            AggAccumulator::MaxStr { val } => {
                                                let cstr =
                                                    pg_sys::text_to_cstring(datum.cast_mut_ptr());
                                                let s = std::ffi::CStr::from_ptr(cstr)
                                                    .to_string_lossy()
                                                    .into_owned();
                                                pg_sys::pfree(cstr as *mut _);
                                                if val
                                                    .as_ref()
                                                    .is_none_or(|cur| collation_strcmp(&s, cur) > 0)
                                                {
                                                    *val = Some(s);
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } // end generic path
        }

        let agg_us = t2.elapsed().as_micros() as u64 - decompress_us;

        // Write CountDistinct counts from sidecar into compact storage
        if use_compact_keys && use_compact_accs {
            cd_sidecar
                .write_counts_to_storage(compact_storage.as_mut().unwrap(), &compact_group_map);
        }

        // Finalize results using output mapping, applying HAVING filters
        let mut topn_select_us: u64 = 0;
        let t_finalize = Instant::now();
        let mut result_rows = if use_compact_keys
            && use_compact_accs
            && topn_limit > 0
            && having_filters.is_empty()
            && compact_group_map.len() > topn_limit as usize
        {
            // Top-N pushdown: heap-select top-N by raw sort value, finalize only those
            let sort_slot = match output_map[topn_sort_col] {
                OutputEntry::Agg(ai) => ai,
                _ => unreachable!(),
            };
            let storage = compact_storage.as_ref().unwrap();
            let t_topn = Instant::now();
            let top_entries = compact_topn_select(
                &compact_group_map,
                storage,
                sort_slot,
                topn_limit as usize,
                topn_ascending,
                agg_specs[sort_slot].agg_type == AggType::Avg,
            );
            topn_select_us = t_topn.elapsed().as_micros() as u64;
            let num_group_keys = group_specs.len();
            let mut rows = Vec::with_capacity(top_entries.len());
            for &(packed_key, group_idx) in &top_entries {
                let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                for (spec_idx, spec) in agg_specs.iter().enumerate() {
                    agg_results.push(compact_finalize(storage, group_idx, spec_idx, spec));
                }
                let keys = unpack_int_keys(packed_key, num_group_keys);
                let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                for entry in &output_map {
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
        } else if use_compact_keys && use_compact_accs {
            // Full compact finalization (no top-N pushdown, or HAVING present)
            let storage = compact_storage.as_ref().unwrap();
            let num_group_keys = group_specs.len();
            let mut rows = Vec::new();
            'compact_group_loop: for (&packed_key, &group_idx) in &compact_group_map {
                let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                for (spec_idx, spec) in agg_specs.iter().enumerate() {
                    agg_results.push(compact_finalize(storage, group_idx, spec_idx, spec));
                }

                // Apply HAVING filters
                for hf in &having_filters {
                    let (datum, is_null) = agg_results[hf.agg_idx];
                    if is_null {
                        continue 'compact_group_loop;
                    }
                    let val = datum.value() as i64;
                    let pass = match hf.op {
                        HavingOp::Gt => val > hf.const_val,
                        HavingOp::Lt => val < hf.const_val,
                        HavingOp::Ge => val >= hf.const_val,
                        HavingOp::Le => val <= hf.const_val,
                        HavingOp::Eq => val == hf.const_val,
                        HavingOp::Ne => val != hf.const_val,
                    };
                    if !pass {
                        continue 'compact_group_loop;
                    }
                }

                // Unpack keys back to i64 datums
                let keys = unpack_int_keys(packed_key, num_group_keys);
                let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                for entry in &output_map {
                    match entry {
                        OutputEntry::Agg(ai) => {
                            row.push(agg_results[*ai]);
                        }
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
                if bare_limit > 0 && rows.len() >= bare_limit as usize {
                    break;
                }
            }
            rows
        } else if has_group_by {
            let mut rows = Vec::new();
            // Pre-finalize all agg results keyed by group
            'group_loop: for (key, &group_idx) in &group_map {
                let accs = &flat_accs
                    [group_idx as usize * n_agg_specs..(group_idx as usize + 1) * n_agg_specs];
                let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                for (spec_idx, spec) in agg_specs.iter().enumerate() {
                    agg_results.push(finalize_accumulator(&accs[spec_idx], spec));
                }

                // Apply HAVING filters on finalized aggregate values
                for hf in &having_filters {
                    let (datum, is_null) = agg_results[hf.agg_idx];
                    if is_null {
                        continue 'group_loop; // NULL doesn't satisfy HAVING
                    }
                    let val = datum.value() as i64;
                    let pass = match hf.op {
                        HavingOp::Gt => val > hf.const_val,
                        HavingOp::Lt => val < hf.const_val,
                        HavingOp::Ge => val >= hf.const_val,
                        HavingOp::Le => val <= hf.const_val,
                        HavingOp::Eq => val == hf.const_val,
                        HavingOp::Ne => val != hf.const_val,
                    };
                    if !pass {
                        continue 'group_loop;
                    }
                }

                let key_slice = key.as_slice();
                let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                for entry in &output_map {
                    match entry {
                        OutputEntry::Agg(ai) => {
                            row.push(agg_results[*ai]);
                        }
                        OutputEntry::Group(gi) => {
                            match &key_slice[*gi] {
                                GroupKeyVal::Null => {
                                    row.push((pg_sys::Datum::from(0usize), true));
                                }
                                GroupKeyVal::Int(v) => {
                                    if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. })
                                    {
                                        // extract() returns numeric — convert i64 to numeric datum
                                        row.push((i128_to_numeric_datum(*v as i128), false));
                                    } else {
                                        row.push((pg_sys::Datum::from(*v as usize), false));
                                    }
                                }
                                GroupKeyVal::Str(off, len) => {
                                    let s = string_arena.get(*off, *len);
                                    let datum = string_to_datum(s, group_specs[*gi].type_oid);
                                    row.push((datum, false));
                                }
                            }
                        }
                        OutputEntry::DerivedGroup { base_gi, delta } => {
                            match &key_slice[*base_gi] {
                                GroupKeyVal::Int(v) => {
                                    row.push((pg_sys::Datum::from((*v + delta) as usize), false))
                                }
                                _ => row.push((pg_sys::Datum::from(0usize), true)),
                            }
                        }
                        OutputEntry::Const(d, n) => row.push((*d, *n)),
                    }
                }
                rows.push(row);
                if bare_limit > 0 && rows.len() >= bare_limit as usize {
                    break;
                }
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
                    OutputEntry::Group(_) | OutputEntry::DerivedGroup { .. } => {
                        row.push((pg_sys::Datum::from(0usize), true));
                    }
                    OutputEntry::Const(d, n) => row.push((*d, *n)),
                }
            }
            vec![row]
        } else {
            vec![]
        };

        let finalize_us = t_finalize.elapsed().as_micros() as u64;

        // Apply top-N: sort by the specified output column and truncate
        // (compact top-N pushdown path already has correct results; this handles other paths)
        let pre_topn_groups = if use_compact_keys && use_compact_accs {
            compact_group_map.len()
        } else {
            result_rows.len()
        };
        if topn_limit > 0 && has_group_by && result_rows.len() > topn_limit as usize {
            let si = topn_sort_col;
            if topn_ascending {
                result_rows.sort_by_key(|row| {
                    let (datum, is_null) = row[si];
                    if is_null {
                        i64::MAX
                    } else {
                        datum.value() as i64
                    }
                });
            } else {
                result_rows.sort_by(|a, b| {
                    let (da, na) = a[si];
                    let (db, nb) = b[si];
                    let va = if na { i64::MIN } else { da.value() as i64 };
                    let vb = if nb { i64::MIN } else { db.value() as i64 };
                    vb.cmp(&va) // reverse order for DESC
                });
            }
            result_rows.truncate(topn_limit as usize);
        }

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
            detoast_us: total_detoast_us,
            blob_cache_hits: total_cache_hits,
            blob_cache_misses: total_cache_misses,
            blob_cache_bytes_served: total_cache_bytes_served,
            decompress_us,
            agg_us,
            total_segments,
            total_rows_processed,
            batch_quals_count: batch_quals.len(),
            where_quals_null: where_quals.is_null(),
            segments_metadata_resolved: 0,
            segments_decompressed: 0,
            regex_cache_size: regex_cache.len() as u64,
            regex_cache_calls,
            topn_limit: if topn_limit > 0 { topn_limit as u64 } else { 0 },
            topn_sort_col: topn_sort_col as i64,
            topn_ascending,
            pre_topn_groups: pre_topn_groups as u64,
            merge_us: 0,
            finalize_us,
            topn_select_us,
            n_workers: 0,
            bare_limit: 0,
            wall_us: t_wall.elapsed().as_micros() as u64,
            buf_stats: super::segments::take_scan_buf_stats(),
            f8_preselected: 0,
            pscan: std::ptr::null_mut(),
            is_parallel_worker: false,
            exec_ctx: None,
        };

        let state_box = Box::new(state);
        let state_ptr = Box::into_raw(state_box);
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}

/// String arena: all group key strings packed into one Vec<u8>.
/// One deallocation instead of 275K individual String deallocations.
pub(super) struct StringArena {
    pub(super) buf: Vec<u8>,
}

impl StringArena {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub(super) fn alloc(&mut self, s: &str) -> (u32, u32) {
        let off = self.buf.len() as u32;
        let len = s.len() as u32;
        self.buf.extend_from_slice(s.as_bytes());
        (off, len)
    }

    pub(super) fn get(&self, off: u32, len: u32) -> &str {
        std::str::from_utf8(&self.buf[off as usize..off as usize + len as usize]).unwrap_or("")
    }
}

/// Group key value for HashMap key (owned).
/// Str variant stores (offset, len) into a StringArena.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupKeyVal {
    Null,
    Int(i64),
    Str(u32, u32), // (offset, len) into StringArena
}

/// Group key that avoids heap allocation for the common single-column case.
/// For high-cardinality GROUP BY (275K+ groups), eliminating per-key Vec
/// allocation saves ~130ms of cleanup overhead when the HashMap is dropped.
enum GroupKey {
    Single(GroupKeyVal),
    Multi(Box<[GroupKeyVal]>),
}

impl GroupKey {
    fn as_slice(&self) -> &[GroupKeyVal] {
        match self {
            GroupKey::Single(v) => std::slice::from_ref(v),
            GroupKey::Multi(v) => v,
        }
    }
}

/// Borrowed version of GroupKeyVal for hash lookups without allocation.
#[derive(Debug, Clone, Copy)]
/// Borrowed group key component without lifetime parameter.
/// Uses raw pointer for strings to avoid borrow-checker conflicts when reusing
/// the key buffer across loop iterations while mutating regex_results.
/// SAFETY: The pointed-to str data must outlive the current row iteration
/// (guaranteed by seg_text_columns and regex_results living across the loop).
enum GroupKeyRef {
    Null,
    Int(i64),
    Str(*const str),
}

impl GroupKeyRef {
    /// Create a Str variant from a &str. The caller must ensure the str outlives this GroupKeyRef.
    fn from_str(s: &str) -> Self {
        GroupKeyRef::Str(s as *const str)
    }

    fn resolve(&self, arena: &mut StringArena) -> GroupKeyVal {
        match self {
            GroupKeyRef::Null => GroupKeyVal::Null,
            GroupKeyRef::Int(v) => GroupKeyVal::Int(*v),
            GroupKeyRef::Str(p) => {
                // SAFETY: pointer is valid for the current row iteration
                let s = unsafe { &**p };
                let (off, len) = arena.alloc(s);
                GroupKeyVal::Str(off, len)
            }
        }
    }

    fn matches_owned(&self, owned: &GroupKeyVal, arena: &StringArena) -> bool {
        match (self, owned) {
            (GroupKeyRef::Null, GroupKeyVal::Null) => true,
            (GroupKeyRef::Int(a), GroupKeyVal::Int(b)) => a == b,
            (GroupKeyRef::Str(p), GroupKeyVal::Str(off, len)) => {
                // SAFETY: pointer is valid for the current row iteration
                let s = unsafe { &**p };
                s == arena.get(*off, *len)
            }
            _ => false,
        }
    }
}

/// Hash a group key component into a Hasher with a type discriminant.
fn hash_key_component<H: Hasher>(h: &mut H, val: &GroupKeyVal, arena: &StringArena) {
    match val {
        GroupKeyVal::Null => 0u8.hash(h),
        GroupKeyVal::Int(v) => {
            1u8.hash(h);
            v.hash(h);
        }
        GroupKeyVal::Str(off, len) => {
            2u8.hash(h);
            arena.get(*off, *len).hash(h);
        }
    }
}

fn hash_ref_component<H: Hasher>(h: &mut H, val: &GroupKeyRef) {
    match val {
        GroupKeyRef::Null => 0u8.hash(h),
        GroupKeyRef::Int(v) => {
            1u8.hash(h);
            v.hash(h);
        }
        GroupKeyRef::Str(p) => {
            // SAFETY: pointer is valid for the current row iteration
            let s = unsafe { &**p };
            2u8.hash(h);
            s.hash(h);
        }
    }
}

/// Compute hash for an owned GroupKey (needs arena to resolve strings).
fn hash_group_key(key: &GroupKey, arena: &StringArena) -> u64 {
    let mut hasher = ahash::AHasher::default();
    for val in key.as_slice() {
        hash_key_component(&mut hasher, val, arena);
    }
    hasher.finish()
}

/// Compute hash for a borrowed group key slice (no allocation).
fn hash_group_key_ref(key: &[GroupKeyRef]) -> u64 {
    let mut hasher = ahash::AHasher::default();
    for val in key {
        hash_ref_component(&mut hasher, val);
    }
    hasher.finish()
}

/// Check if a stored owned key matches a temporary borrowed key.
fn keys_match(stored: &GroupKey, temp: &[GroupKeyRef], arena: &StringArena) -> bool {
    let s = stored.as_slice();
    s.len() == temp.len()
        && s.iter()
            .zip(temp.iter())
            .all(|(s, t)| t.matches_owned(s, arena))
}

// SegTextColumn is now in text_col.rs

/// Type alias for the group map using hashbrown with raw_entry support.
/// Maps group keys to indices into flat accumulator storage.
/// Using u32 index instead of Vec<AggAccumulator> eliminates per-group heap allocation
/// for accumulators, saving ~130ms cleanup for 275K groups.
type GroupMap = hashbrown::HashMap<GroupKey, u32, BuildHasherDefault<ahash::AHasher>>;

/// Convert a datum to i128 for SUM accumulation.
pub(super) fn datum_to_i128(datum: pg_sys::Datum, type_oid: pg_sys::Oid) -> i128 {
    match type_oid {
        pg_sys::INT2OID => (datum.value() as i16) as i128,
        pg_sys::INT4OID => (datum.value() as i32) as i128,
        pg_sys::INT8OID => (datum.value() as i64) as i128,
        _ => datum.value() as i128,
    }
}

/// Convert a datum to f64 for float SUM/AVG.
pub(super) fn datum_to_f64(datum: pg_sys::Datum, type_oid: pg_sys::Oid) -> f64 {
    match type_oid {
        pg_sys::FLOAT4OID => f32::from_bits(datum.value() as u32) as f64,
        pg_sys::FLOAT8OID => f64::from_bits(datum.value() as u64),
        _ => datum.value() as f64,
    }
}

/// Convert an i128 value to a PostgreSQL NUMERIC datum.
///
/// For values fitting in i64, uses the fast `int8_numeric` path.
/// For larger values, converts via string representation.
pub(super) unsafe fn i128_to_numeric_datum(val: i128) -> pg_sys::Datum {
    unsafe {
        if val >= i64::MIN as i128 && val <= i64::MAX as i128 {
            pg_sys::OidFunctionCall1Coll(
                pg_sys::Oid::from(1781u32), // int8_numeric
                pg_sys::InvalidOid,
                pg_sys::Datum::from(val as i64 as usize),
            )
        } else {
            let s = std::ffi::CString::new(val.to_string()).unwrap();
            pg_sys::OidFunctionCall3Coll(
                pg_sys::Oid::from(1701u32), // numeric_in
                pg_sys::InvalidOid,
                pg_sys::Datum::from(s.as_ptr()),
                pg_sys::Datum::from(0usize),
                pg_sys::Datum::from(-1i32 as usize),
            )
        }
    }
}

/// Finalize an accumulator into a (Datum, is_null) result pair.
pub(super) unsafe fn finalize_accumulator(
    acc: &AggAccumulator,
    spec: &AggExecSpec,
) -> (pg_sys::Datum, bool) {
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
                            // Result is NUMERIC — use i128_to_numeric for full range
                            (i128_to_numeric_datum(*sum), false)
                        } else {
                            // Result is INT8
                            (pg_sys::Datum::from(*sum as i64 as usize), false)
                        }
                    }
                    AggType::Avg => {
                        // AVG(int*) → NUMERIC — use exact NUMERIC arithmetic
                        let sum_numeric = i128_to_numeric_datum(*sum);
                        let count_numeric = pg_sys::OidFunctionCall1Coll(
                            pg_sys::Oid::from(1781u32), // int8_numeric
                            pg_sys::InvalidOid,
                            pg_sys::Datum::from(*count as usize),
                        );
                        let datum = pg_sys::OidFunctionCall2Coll(
                            pg_sys::Oid::from(1727u32), // numeric_div
                            pg_sys::InvalidOid,
                            sum_numeric,
                            count_numeric,
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
            AggAccumulator::Count { count } => (pg_sys::Datum::from(*count as usize), false),
            AggAccumulator::CountDistinctInt { seen } => (pg_sys::Datum::from(seen.len()), false),
            AggAccumulator::CountDistinctStr { seen } => (pg_sys::Datum::from(seen.len()), false),
            AggAccumulator::MinInt { val } | AggAccumulator::MaxInt { val } => match val {
                Some(v) => (pg_sys::Datum::from(*v as usize), false),
                None => (pg_sys::Datum::from(0usize), true),
            },
            AggAccumulator::MinFloat { val } | AggAccumulator::MaxFloat { val } => match val {
                Some(v) => {
                    if spec.col_type_oid == pg_sys::FLOAT4OID {
                        let f4 = *v as f32;
                        (pg_sys::Datum::from(f4.to_bits() as usize), false)
                    } else {
                        (pg_sys::Datum::from(v.to_bits() as usize), false)
                    }
                }
                None => (pg_sys::Datum::from(0usize), true),
            },
            AggAccumulator::MinStr { val } | AggAccumulator::MaxStr { val } => match val {
                Some(s) => {
                    let datum = string_to_datum(s, spec.col_type_oid);
                    (datum, false)
                }
                None => (pg_sys::Datum::from(0usize), true),
            },
        }
    }
}

/// ExecCustomScan callback for DeltaXAgg: return result rows.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn exec_agg_scan(
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
            match super::agg_wire::deserialize_partial(slab_ptr, len, &ctx.agg_specs) {
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

        match super::agg_wire::serialize_partial_into(slab_ptr, slab_cap, &local, &ctx.agg_specs) {
            Ok(written) => {
                // SAFETY: Release-store `partial_lens[slot]` after the slab
                // bytes are written so the leader's Acquire-load on
                // `populated` (in `shutdown_deltax_agg`) makes the slab
                // contents visible.
                (*state.pscan).partial_lens[slot].store(written as u64, Ordering::Release);
                ctx.worker_done = true;
            }
            Err(super::agg_wire::SerError::Overflow { needed, have }) => {
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
pub(super) unsafe extern "C-unwind" fn end_agg_scan(node: *mut pg_sys::CustomScanState) {
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
pub(super) unsafe extern "C-unwind" fn rescan_agg_scan(node: *mut pg_sys::CustomScanState) {
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

// ============================================================================
// Compact Accumulator Storage (Phase 1)
// ============================================================================

/// Kind of accumulator slot in compact storage.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum CompactAccKind {
    Count,            // 8 bytes: i64
    SumInt,           // 24 bytes: i128 sum (16) + i64 count (8) — for INT8 columns
    SumIntNarrow,     // 16 bytes: i64 sum (8) + i64 count (8) — for INT2/INT4 columns
    SumFloat,         // 16 bytes: f64 sum (8) + i64 count (8)
    MinStr,           // 8 bytes: u32 arena_offset + u32 length (sentinel: u32::MAX, 0)
    MaxStr,           // 8 bytes: u32 arena_offset + u32 length (sentinel: u32::MAX, 0)
    CountDistinctInt, // 8 bytes: i64 count cache (real data in CountDistinctSideCar)
    CountDistinctStr, // 8 bytes: i64 count cache (real data in CountDistinctSideCar)
    MinInt,           // 16 bytes: i64 val + i64 has_value flag (0 = unset)
    MaxInt,           // 16 bytes: i64 val + i64 has_value flag (0 = unset)
}

impl CompactAccKind {
    /// 8-bit on-wire tag. Stable across versions because parallel-agg DSM wire
    /// format depends on it.
    #[allow(dead_code)] // wired by C.2.d/e via agg_wire
    pub(super) fn wire_tag(self) -> u8 {
        match self {
            CompactAccKind::Count => 0,
            CompactAccKind::SumInt => 1,
            CompactAccKind::SumIntNarrow => 2,
            CompactAccKind::SumFloat => 3,
            CompactAccKind::MinStr => 4,
            CompactAccKind::MaxStr => 5,
            CompactAccKind::CountDistinctInt => 6,
            CompactAccKind::CountDistinctStr => 7,
            CompactAccKind::MinInt => 8,
            CompactAccKind::MaxInt => 9,
        }
    }

    fn byte_size(self) -> usize {
        match self {
            CompactAccKind::Count
            | CompactAccKind::CountDistinctInt
            | CompactAccKind::CountDistinctStr => 8,
            CompactAccKind::SumInt => 24,
            CompactAccKind::SumIntNarrow => 16,
            CompactAccKind::SumFloat => 16,
            CompactAccKind::MinStr | CompactAccKind::MaxStr => 8,
            CompactAccKind::MinInt | CompactAccKind::MaxInt => 16,
        }
    }

    fn alignment(self) -> usize {
        match self {
            CompactAccKind::Count
            | CompactAccKind::CountDistinctInt
            | CompactAccKind::CountDistinctStr => 8,
            CompactAccKind::SumInt => 16, // i128 needs 16-byte alignment
            CompactAccKind::SumIntNarrow => 8,
            CompactAccKind::SumFloat => 8,
            CompactAccKind::MinStr | CompactAccKind::MaxStr => 4,
            CompactAccKind::MinInt | CompactAccKind::MaxInt => 8,
        }
    }
}

/// Layout of compact accumulator slots for one group.
pub(super) struct CompactAccLayout {
    /// (byte_offset, kind) per aggregate
    pub(super) slots: Vec<(usize, CompactAccKind)>,
    /// Total bytes per group (aligned to 16)
    pub(super) group_stride: usize,
}

impl CompactAccLayout {
    pub(super) fn new(specs: &[AggExecSpec]) -> Self {
        let mut offset: usize = 0;

        // Sort by alignment (descending) to minimize padding.
        // We need to maintain original order for indexing, so we compute
        // offsets in alignment order then map back.
        let mut indexed: Vec<(usize, CompactAccKind)> = specs
            .iter()
            .enumerate()
            .map(|(i, spec)| {
                let kind = compact_acc_kind(spec);
                (i, kind)
            })
            .collect();
        // Sort by alignment descending (i128 first, then i64/f64)
        indexed.sort_by_key(|b| std::cmp::Reverse(b.1.alignment()));

        let mut slots = vec![(0usize, CompactAccKind::Count); specs.len()];
        for (orig_idx, kind) in &indexed {
            let align = kind.alignment();
            offset = (offset + align - 1) & !(align - 1);
            slots[*orig_idx] = (offset, *kind);
            offset += kind.byte_size();
        }

        // Align stride to 16 so i128 fields in next group are aligned
        let group_stride = (offset + 15) & !15;

        CompactAccLayout {
            slots,
            group_stride,
        }
    }
}

/// Determine the CompactAccKind for a given agg spec.
///
/// INT2/INT4 columns use SumIntNarrow (i64 sum, 16B) since their sums
/// cannot overflow i64 even at 2^31 rows × max value (2^31 × 2^31 < 2^63).
/// INT8 columns use SumInt (i128 sum, 24B) to handle potential overflow.
fn compact_acc_kind(spec: &AggExecSpec) -> CompactAccKind {
    match spec.agg_type {
        AggType::CountStar | AggType::Count => CompactAccKind::Count,
        AggType::Sum | AggType::Avg => {
            if spec.col_type_oid == pg_sys::FLOAT4OID || spec.col_type_oid == pg_sys::FLOAT8OID {
                CompactAccKind::SumFloat
            } else if spec.col_type_oid == pg_sys::INT2OID || spec.col_type_oid == pg_sys::INT4OID {
                CompactAccKind::SumIntNarrow
            } else {
                CompactAccKind::SumInt
            }
        }
        AggType::Min => {
            let t = spec.col_type_oid;
            if t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID {
                CompactAccKind::MinStr
            } else if t == pg_sys::INT2OID
                || t == pg_sys::INT4OID
                || t == pg_sys::INT8OID
                || t == pg_sys::DATEOID
                || t == pg_sys::TIMESTAMPOID
                || t == pg_sys::TIMESTAMPTZOID
            {
                CompactAccKind::MinInt
            } else {
                unreachable!(
                    "compact_acc_kind: MIN on type {:?} not supported in compact path",
                    t
                )
            }
        }
        AggType::Max => {
            let t = spec.col_type_oid;
            if t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID {
                CompactAccKind::MaxStr
            } else if t == pg_sys::INT2OID
                || t == pg_sys::INT4OID
                || t == pg_sys::INT8OID
                || t == pg_sys::DATEOID
                || t == pg_sys::TIMESTAMPOID
                || t == pg_sys::TIMESTAMPTZOID
            {
                CompactAccKind::MaxInt
            } else {
                unreachable!(
                    "compact_acc_kind: MAX on type {:?} not supported in compact path",
                    t
                )
            }
        }
        AggType::CountDistinct => {
            let t = spec.col_type_oid;
            if t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID {
                CompactAccKind::CountDistinctStr
            } else {
                CompactAccKind::CountDistinctInt
            }
        }
    }
}

/// Phase D bitset: a bare `Vec<u64>` with set/or/popcount. Used for
/// COUNT(DISTINCT text) when the column is dictionary-encoded across every
/// participating segment — workers set bits indexed by leader-precomputed
/// global string IDs, the merge step OR's two bitsets, and finalisation
/// returns `count_ones`. Avoids a `bitvec` dep — `count_ones` on `u64`
/// lowers to POPCNT on x86_64 and CNT on aarch64.
#[derive(Clone)]
pub(super) struct Bitset {
    words: Vec<u64>,
    nbits: u32,
}

impl Bitset {
    pub(super) fn with_size(nbits: u32) -> Self {
        let nwords = nbits.div_ceil(64) as usize;
        Bitset {
            words: vec![0u64; nwords],
            nbits,
        }
    }
    #[inline]
    pub(super) fn set(&mut self, idx: u32) {
        debug_assert!(idx < self.nbits, "Bitset::set out of range");
        let w = (idx >> 6) as usize;
        let b = idx & 63;
        self.words[w] |= 1u64 << b;
    }
    #[inline]
    pub(super) fn or_with(&mut self, other: &Bitset) {
        debug_assert_eq!(self.words.len(), other.words.len());
        for (a, b) in self.words.iter_mut().zip(other.words.iter()) {
            *a |= *b;
        }
    }
    pub(super) fn count_ones(&self) -> u64 {
        self.words.iter().map(|w| w.count_ones() as u64).sum()
    }
}

/// Phase D dict-eligible CountDistinct(text) global remap. Built once at the
/// leader after segments are loaded — see `build_dict_distinct_remaps`.
///
/// `per_segment[seg_idx][local_dict_id] = global_id`. `seg_idx` is the
/// position in the leader's `all_segments` Vec; `local_dict_id` is the entry
/// position in that segment's per-column dictionary; `global_id` is a unique
/// integer in `[0, global_count)` shared across every segment. Workers set
/// bit `global_id` in their per-(spec,group) `Bitset` instead of hashing the
/// raw string.
pub(super) struct DictDistinctRemap {
    pub(super) global_count: u32,
    pub(super) per_segment: Vec<Vec<u32>>,
}

/// Skip Phase D's bitset path when the per-column **post-dedup global
/// string count** exceeds this. Bitset size is `global_count` bits per
/// (spec, group, worker); at 10M × 16 groups × 16 workers ≈ 320 MB —
/// tolerable on bench-class boxes (m6i.8xlarge has 128 GB) but starts
/// mattering on small ones. Tighten if real workloads push past this.
///
/// Checked AFTER the parallel pre-pass — at that point we know the
/// actual deduplicated count, not the looser per-segment-dict-size sum.
pub(super) const PHASE_D_MAX_GLOBAL_FOR_BITSET: u32 = 10_000_000;

/// Skip Phase D entirely when the **sum of per-segment dict sizes**
/// exceeds this. Sum is a loose upper bound on global cardinality
/// (post-dedup `global_count` ≤ sum). A high sum implies expensive
/// pre-pass: LZ4-decompressing dicts, hashing entries, allocating
/// per-thread String keys. JSONBench Q1's `x_did` (~10K entries per
/// segment × 2700 segs ≈ 27M sum, ~3.6M unique post-dedup) sits in the
/// sweet spot — well under this gate, comfortably under
/// `PHASE_D_MAX_GLOBAL_FOR_BITSET` after dedup. UUID-style columns
/// where every row is unique blow past the sum gate without the
/// pre-pass even running.
///
/// Tuned 4× looser than the global cap to leave room for typical 4-10×
/// dedup ratios while still bounding worst-case pre-pass memory.
pub(super) const PHASE_D_MAX_DICT_SIZE_SUM: u64 = 50_000_000;

/// Phase D leader pre-pass: walk each `CountDistinct(text)` spec's
/// per-segment dictionary blob, build a global string-ID interner, and
/// emit per-segment local→global remap tables. Returns one
/// `DictDistinctRemap` per eligible spec (keyed by its index in
/// `agg_specs`). Specs whose columns aren't dict-encoded across every
/// segment, or whose global cardinality exceeds
/// `PHASE_D_MAX_GLOBAL_FOR_BITSET`, are absent from the result and stay
/// on the `HashSet<u128>` path.
///
/// Parallelised via `std::thread::scope`:
///
///   1. **Phase 1 (parallel)** — each worker takes a chunk of segments,
///      parses every dict (LZ4-decompressing as needed), and builds two
///      things: a per-thread `local_entries: Vec<String>` recording
///      strings in insertion order, and `seg_local_remaps: Vec<Vec<u32>>`
///      mapping `(seg_in_chunk, local_dict_id) → local_thread_id` (the
///      string's index in `local_entries`).
///   2. **Phase 2 (sequential)** — merge each thread's `local_entries`
///      into the global interner. For each thread `t` we end up with
///      `thread_to_global[t][local_thread_id] = global_id`. This is the
///      only sequential bottleneck and is dominated by HashMap probes
///      against the global interner; LZ4 decompression and per-string
///      hashing already happened in parallel.
///   3. **Phase 3 (parallel)** — each worker rewrites its
///      `seg_local_remaps` into the final `per_segment[seg_idx][local_dict_id]
///      = global_id` slabs by indexing into `thread_to_global[my_thread]`.
///      Pure array lookup; runs in well under 100ms even on Q1-scale data.
pub(super) fn build_dict_distinct_remaps(
    all_segments: &[super::segments::SegmentData],
    agg_specs: &[AggExecSpec],
) -> std::collections::HashMap<usize, DictDistinctRemap> {
    use crate::compression::{CompressedColumnRef, CompressionType, dictionary};

    let mut remaps = std::collections::HashMap::new();
    let n_workers = crate::get_parallel_workers().max(1);

    for (spec_idx, spec) in agg_specs.iter().enumerate() {
        if spec.agg_type != AggType::CountDistinct {
            continue;
        }
        if !matches!(
            spec.col_type_oid,
            pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID
        ) {
            continue;
        }
        if spec.col_idx < 0 {
            continue;
        }
        let col_idx = spec.col_idx as usize;

        // Eligibility: every segment whose blob is non-empty must be
        // dict-encoded for this column AND the sum of per-segment dict
        // sizes must fit under PHASE_D_MAX_GLOBAL_FOR_BITSET. The sum is
        // an upper bound on global cardinality (after dedup it can only
        // shrink) and lives in the first 4 bytes of `cc_ref.data` — no
        // LZ4 decompression on bail. Segment-by columns store values in
        // `segment_values` (outside `compressed_blobs`); for those the
        // spec stays on the HashSet path even if every other segment is
        // dict-encoded — too narrow to bother with a separate code path.
        let mut eligible = true;
        let mut dict_size_sum: u64 = 0;
        for seg in all_segments {
            if col_idx >= seg.compressed_blobs.len() {
                eligible = false;
                break;
            }
            let blob = &seg.compressed_blobs[col_idx];
            if blob.is_empty() {
                continue;
            }
            let comp = CompressionType::from_u8(blob[0]);
            if !matches!(
                comp,
                CompressionType::Dictionary | CompressionType::DictionaryLz4
            ) {
                eligible = false;
                break;
            }
            let cc_ref = CompressedColumnRef::from_bytes(blob);
            if cc_ref.data.len() < 4 {
                eligible = false;
                break;
            }
            let dict_size = u32::from_le_bytes(cc_ref.data[0..4].try_into().unwrap()) as u64;
            dict_size_sum = dict_size_sum.saturating_add(dict_size);
            if dict_size_sum > PHASE_D_MAX_DICT_SIZE_SUM {
                eligible = false;
                break;
            }
        }
        if !eligible {
            continue;
        }

        // ---------- Phase 1 (parallel): per-thread local interners ----------
        struct LocalPrePass {
            /// `local_entries[local_thread_id]` = entry string. Insertion
            /// order — preserved so Phase 2's sequential merge is a clean
            /// linear walk and `thread_to_global[t]` is indexable directly
            /// by `local_thread_id`.
            local_entries: Vec<String>,
            /// `seg_local_remaps[seg_in_chunk][local_dict_id] = local_thread_id`.
            seg_local_remaps: Vec<Vec<u32>>,
        }

        let chunk_size = all_segments.len().div_ceil(n_workers).max(1);
        let local_results: Vec<LocalPrePass> = std::thread::scope(|s| {
            all_segments
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(move || {
                        let mut lookup: hashbrown::HashMap<
                            String,
                            u32,
                            BuildHasherDefault<ahash::AHasher>,
                        > = hashbrown::HashMap::with_hasher(BuildHasherDefault::default());
                        let mut local_entries: Vec<String> = Vec::new();
                        let mut seg_local_remaps: Vec<Vec<u32>> = Vec::with_capacity(chunk.len());
                        for seg in chunk {
                            let blob = &seg.compressed_blobs[col_idx];
                            if blob.is_empty() {
                                seg_local_remaps.push(Vec::new());
                                continue;
                            }
                            let cc_ref = CompressedColumnRef::from_bytes(blob);
                            let norm_buf;
                            let dict_data: &[u8] =
                                if cc_ref.type_tag == CompressionType::DictionaryLz4 {
                                    norm_buf = dictionary::normalize_lz4(cc_ref.data);
                                    &norm_buf[..]
                                } else {
                                    cc_ref.data
                                };
                            let header = dictionary::parse_header(dict_data);
                            let mut seg_remap: Vec<u32> = Vec::with_capacity(header.dict.len());
                            for &entry in &header.dict {
                                let local_id = match lookup.get(entry) {
                                    Some(&id) => id,
                                    None => {
                                        let id = local_entries.len() as u32;
                                        local_entries.push(entry.to_string());
                                        lookup.insert(entry.to_string(), id);
                                        id
                                    }
                                };
                                seg_remap.push(local_id);
                            }
                            seg_local_remaps.push(seg_remap);
                        }
                        LocalPrePass {
                            local_entries,
                            seg_local_remaps,
                        }
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect()
        });

        // ---------- Phase 2 (sequential): merge into global interner -------
        // `thread_to_global[t][local_thread_id] = global_id`.
        let mut global_interner: hashbrown::HashMap<
            String,
            u32,
            BuildHasherDefault<ahash::AHasher>,
        > = hashbrown::HashMap::with_hasher(BuildHasherDefault::default());
        let mut thread_to_global: Vec<Vec<u32>> = Vec::with_capacity(local_results.len());
        for local in &local_results {
            let mut t_remap: Vec<u32> = Vec::with_capacity(local.local_entries.len());
            for entry in &local.local_entries {
                let global_id = match global_interner.get(entry) {
                    Some(&id) => id,
                    None => {
                        let id = global_interner.len() as u32;
                        global_interner.insert(entry.clone(), id);
                        id
                    }
                };
                t_remap.push(global_id);
            }
            thread_to_global.push(t_remap);
        }

        let global_count = global_interner.len() as u32;
        if global_count == 0 {
            continue;
        }
        if global_count > PHASE_D_MAX_GLOBAL_FOR_BITSET {
            // Post-dedup the column has more unique strings than the
            // bitset memory budget allows. Drop the pre-pass work and let
            // workers fall back to HashSet<u128>. Pre-pass effort wasted
            // for this query, but the sum gate above keeps the wasted
            // work bounded; in practice this branch only fires on truly
            // pathological cardinality (>10M unique).
            continue;
        }

        // ---------- Phase 3 (parallel): rewrite local IDs to global IDs ----
        // Each worker takes its slice of `local_results` + its slot in
        // `thread_to_global`, rewrites `seg_local_remaps` in place. The
        // resulting `Vec<Vec<u32>>` per chunk is concatenated below into
        // `per_segment` in the original `all_segments` order — chunks were
        // contiguous slices in Phase 1, so the order is preserved.
        let global_chunks: Vec<Vec<Vec<u32>>> = std::thread::scope(|s| {
            local_results
                .into_iter()
                .zip(thread_to_global.iter())
                .map(|(mut local, t_remap)| {
                    s.spawn(move || {
                        for seg_remap in &mut local.seg_local_remaps {
                            for slot in seg_remap.iter_mut() {
                                *slot = t_remap[*slot as usize];
                            }
                        }
                        local.seg_local_remaps
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect()
        });

        let mut per_segment: Vec<Vec<u32>> = Vec::with_capacity(all_segments.len());
        for chunk in global_chunks {
            per_segment.extend(chunk);
        }
        debug_assert_eq!(per_segment.len(), all_segments.len());

        remaps.insert(
            spec_idx,
            DictDistinctRemap {
                global_count,
                per_segment,
            },
        );
    }

    remaps
}

/// Side-car storage for COUNT(DISTINCT) accumulators.
/// Each CountDistinct agg spec gets a Vec of HashSets indexed by group_idx.
/// Int columns store raw i64 values; text columns store 128-bit hash digests.
/// Dict-eligible text columns (Phase D) use per-group `Bitset` indexed by
/// leader-precomputed global string IDs.
pub(super) struct CountDistinctSideCar {
    entries: Vec<CdEntry>,
}

#[derive(PartialEq, Eq, Clone, Copy)]
pub(super) enum CdKind {
    Int,
    Str,
    /// Dict-encoded text with leader pre-pass: per-group `Bitset` of size
    /// `global_count` (the bitset_size below). Merge is bit-OR; finalise is
    /// `count_ones`. Eligibility checked in `build_dict_distinct_remaps`.
    DictBitset,
}

struct CdEntry {
    spec_idx: usize,
    kind: CdKind,
    /// `bitset_size` for `DictBitset`; otherwise unused.
    bitset_size: u32,
    sets_int: Vec<hashbrown::HashSet<i64, BuildHasherDefault<ahash::AHasher>>>,
    sets_str: Vec<hashbrown::HashSet<u128, BuildHasherDefault<ahash::AHasher>>>,
    bitsets: Vec<Bitset>,
}

impl CdEntry {
    /// Count of distinct values seen for `group_idx`. The output of every
    /// CountDistinct accumulator finalisation, regardless of representation.
    #[inline]
    fn count(&self, group_idx: u32) -> i64 {
        let i = group_idx as usize;
        match self.kind {
            CdKind::Str => self.sets_str[i].len() as i64,
            CdKind::Int => self.sets_int[i].len() as i64,
            CdKind::DictBitset => self.bitsets[i].count_ones() as i64,
        }
    }
}

impl CountDistinctSideCar {
    /// Default constructor: every text CountDistinct uses the HashSet<u128>
    /// path. Phase D's bitset path is opted in via `new_with_dict_remaps`,
    /// which classifies eligible text specs as `DictBitset` instead.
    pub(super) fn new(agg_specs: &[AggExecSpec]) -> Self {
        Self::new_inner(agg_specs, &Default::default())
    }

    /// Phase D entry point. `dict_remap_sizes` maps spec_idx → bitset size
    /// (the global string-ID count) for every CountDistinct(text) spec the
    /// leader has confirmed is dict-encoded across all relevant segments.
    /// Specs absent from the map keep the HashSet<u128> behaviour.
    pub(super) fn new_with_dict_remaps(
        agg_specs: &[AggExecSpec],
        dict_remap_sizes: &std::collections::HashMap<usize, u32>,
    ) -> Self {
        Self::new_inner(agg_specs, dict_remap_sizes)
    }

    fn new_inner(
        agg_specs: &[AggExecSpec],
        dict_remap_sizes: &std::collections::HashMap<usize, u32>,
    ) -> Self {
        let mut entries = Vec::new();
        for (i, spec) in agg_specs.iter().enumerate() {
            if spec.agg_type == AggType::CountDistinct {
                let is_str = matches!(
                    spec.col_type_oid,
                    pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID
                );
                let bitset_size = if is_str {
                    dict_remap_sizes.get(&i).copied().unwrap_or(0)
                } else {
                    0
                };
                let kind = if is_str && bitset_size > 0 {
                    CdKind::DictBitset
                } else if is_str {
                    CdKind::Str
                } else {
                    CdKind::Int
                };
                entries.push(CdEntry {
                    spec_idx: i,
                    kind,
                    bitset_size,
                    sets_int: Vec::new(),
                    sets_str: Vec::new(),
                    bitsets: Vec::new(),
                });
            }
        }
        CountDistinctSideCar { entries }
    }

    pub(super) fn alloc_group(&mut self) {
        for e in &mut self.entries {
            match e.kind {
                CdKind::Str => e.sets_str.push(hashbrown::HashSet::with_hasher(
                    BuildHasherDefault::default(),
                )),
                CdKind::Int => e.sets_int.push(hashbrown::HashSet::with_hasher(
                    BuildHasherDefault::default(),
                )),
                CdKind::DictBitset => e.bitsets.push(Bitset::with_size(e.bitset_size)),
            }
        }
    }

    fn insert_int(&mut self, spec_idx: usize, group_idx: u32, val: i64) {
        for e in &mut self.entries {
            if e.spec_idx == spec_idx {
                e.sets_int[group_idx as usize].insert(val);
                return;
            }
        }
    }

    fn insert_str(&mut self, spec_idx: usize, group_idx: u32, hash: u128) {
        for e in &mut self.entries {
            if e.spec_idx == spec_idx {
                e.sets_str[group_idx as usize].insert(hash);
                return;
            }
        }
    }

    /// Phase D: set the bit for `global_id` in the per-group bitset of the
    /// (dict-eligible) CountDistinct(text) spec at `spec_idx`. `global_id`
    /// must be `< bitset_size` (`< DictDistinctRemap::global_count`).
    fn insert_dict_global(&mut self, spec_idx: usize, group_idx: u32, global_id: u32) {
        for e in &mut self.entries {
            if e.spec_idx == spec_idx {
                e.bitsets[group_idx as usize].set(global_id);
                return;
            }
        }
    }

    #[allow(dead_code)]
    fn len(&self, spec_idx: usize, group_idx: u32) -> i64 {
        for e in &self.entries {
            if e.spec_idx == spec_idx {
                return match e.kind {
                    CdKind::Str => e.sets_str[group_idx as usize].len() as i64,
                    CdKind::Int => e.sets_int[group_idx as usize].len() as i64,
                    CdKind::DictBitset => e.bitsets[group_idx as usize].count_ones() as i64,
                };
            }
        }
        0
    }

    fn union_from(&mut self, spec_idx: usize, dst_group: u32, other: &Self, src_group: u32) {
        for (e, oe) in self.entries.iter_mut().zip(other.entries.iter()) {
            if e.spec_idx == spec_idx {
                match e.kind {
                    CdKind::Str => {
                        let src = &oe.sets_str[src_group as usize];
                        e.sets_str[dst_group as usize].extend(src.iter().copied());
                    }
                    CdKind::Int => {
                        let src = &oe.sets_int[src_group as usize];
                        e.sets_int[dst_group as usize].extend(src.iter().copied());
                    }
                    CdKind::DictBitset => {
                        let src = &oe.bitsets[src_group as usize];
                        e.bitsets[dst_group as usize].or_with(src);
                    }
                }
                return;
            }
        }
    }

    /// Write cached counts into compact storage Count slots for top-N sorting.
    fn write_counts_to_storage(&self, storage: &mut CompactAccStorage, map: &CompactGroupMap) {
        for e in &self.entries {
            for (_, &gidx) in map.iter() {
                let count = match e.kind {
                    CdKind::Str => e.sets_str[gidx as usize].len() as i64,
                    CdKind::Int => e.sets_int[gidx as usize].len() as i64,
                    CdKind::DictBitset => e.bitsets[gidx as usize].count_ones() as i64,
                };
                unsafe {
                    *storage.count_mut(gidx, e.spec_idx) = count;
                }
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Flat byte buffer holding compact accumulators for all groups.
pub(super) struct CompactAccStorage {
    pub(super) buf: Vec<u8>,
    pub(super) layout: CompactAccLayout,
    pub(super) str_arena: StringArena,
}

impl CompactAccStorage {
    pub(super) fn new(layout: CompactAccLayout) -> Self {
        CompactAccStorage {
            buf: Vec::new(),
            layout,
            str_arena: StringArena::new(),
        }
    }

    /// Reconstruct from a layout + raw `buf` bytes + raw string arena bytes.
    /// Used by parallel-agg DSM deserialise path; keeps byte interpretation
    /// behind a single constructor so tests cover a stable contract.
    #[allow(dead_code)] // wired by C.2.b's deserialiser
    pub(super) fn from_parts(
        layout: CompactAccLayout,
        buf: Vec<u8>,
        str_arena_buf: Vec<u8>,
    ) -> Self {
        CompactAccStorage {
            buf,
            layout,
            str_arena: StringArena { buf: str_arena_buf },
        }
    }

    /// Allocate accumulators for a new group. Returns the group index.
    ///
    /// Growth strategy: below 1GB, let Vec double normally. Above 1GB,
    /// grow by 2GB increments to cap peak waste at ~2GB instead of 100%.
    #[inline]
    pub(super) fn alloc_group(&mut self) -> u32 {
        let new_len = self.buf.len() + self.layout.group_stride;
        if new_len > self.buf.capacity() {
            const GB: usize = 1 << 30;
            let extra = if self.buf.capacity() >= GB {
                2 * GB // fixed 2GB growth for large buffers
            } else {
                self.buf.capacity().max(self.layout.group_stride) // double (normal)
            };
            self.buf.reserve(extra);
        }
        let group_idx = self.buf.len() / self.layout.group_stride;
        self.buf.resize(new_len, 0);
        // Set MinStr/MaxStr sentinels (u32::MAX offset = no value)
        for slot_idx in 0..self.layout.slots.len() {
            let (_, kind) = self.layout.slots[slot_idx];
            if kind == CompactAccKind::MinStr || kind == CompactAccKind::MaxStr {
                unsafe {
                    self.write_min_max_str(group_idx as u32, slot_idx, u32::MAX, 0);
                }
            }
        }
        group_idx as u32
    }

    /// Get a mutable i64 reference (for Count).
    #[inline]
    pub(super) unsafe fn count_mut(&mut self, group_idx: u32, slot: usize) -> &mut i64 {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let ptr = self
                .buf
                .as_mut_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            &mut *(ptr as *mut i64)
        }
    }

    /// Get mutable references to (sum: i128, count: i64) for SumInt.
    #[inline]
    unsafe fn sum_int_mut(&mut self, group_idx: u32, slot: usize) -> (&mut i128, &mut i64) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self
                .buf
                .as_mut_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            let sum = &mut *(base as *mut i128);
            let count = &mut *(base.add(16) as *mut i64);
            (sum, count)
        }
    }

    /// Get mutable references to (sum: i64, count: i64) for SumIntNarrow.
    #[inline]
    unsafe fn sum_int_narrow_mut(&mut self, group_idx: u32, slot: usize) -> (&mut i64, &mut i64) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self
                .buf
                .as_mut_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            let sum = &mut *(base as *mut i64);
            let count = &mut *(base.add(8) as *mut i64);
            (sum, count)
        }
    }

    /// Get mutable references to (sum: f64, count: i64) for SumFloat.
    #[inline]
    unsafe fn sum_float_mut(&mut self, group_idx: u32, slot: usize) -> (&mut f64, &mut i64) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self
                .buf
                .as_mut_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            let sum = &mut *(base as *mut f64);
            let count = &mut *(base.add(8) as *mut i64);
            (sum, count)
        }
    }

    /// Read count value for finalization.
    #[inline]
    pub(super) unsafe fn read_count(&self, group_idx: u32, slot: usize) -> i64 {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let ptr = self
                .buf
                .as_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            *(ptr as *const i64)
        }
    }

    /// Read (sum_i128, count) for finalization.
    #[inline]
    unsafe fn read_sum_int(&self, group_idx: u32, slot: usize) -> (i128, i64) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self
                .buf
                .as_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            let sum = *(base as *const i128);
            let count = *(base.add(16) as *const i64);
            (sum, count)
        }
    }

    /// Read (sum_i64, count) for finalization (narrow path).
    #[inline]
    unsafe fn read_sum_int_narrow(&self, group_idx: u32, slot: usize) -> (i64, i64) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self
                .buf
                .as_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            let sum = *(base as *const i64);
            let count = *(base.add(8) as *const i64);
            (sum, count)
        }
    }

    /// Read (sum_f64, count) for finalization.
    #[inline]
    unsafe fn read_sum_float(&self, group_idx: u32, slot: usize) -> (f64, i64) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self
                .buf
                .as_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            let sum = *(base as *const f64);
            let count = *(base.add(8) as *const i64);
            (sum, count)
        }
    }

    /// Read MinStr/MaxStr: returns (arena_offset, length). Sentinel is (u32::MAX, 0) = no value.
    #[inline]
    pub(super) unsafe fn read_min_max_str(&self, group_idx: u32, slot: usize) -> (u32, u32) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self
                .buf
                .as_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            let off = *(base as *const u32);
            let len = *(base.add(4) as *const u32);
            (off, len)
        }
    }

    /// Write MinStr/MaxStr arena offset and length.
    #[inline]
    pub(super) unsafe fn write_min_max_str(
        &mut self,
        group_idx: u32,
        slot: usize,
        off: u32,
        len: u32,
    ) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self
                .buf
                .as_mut_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            *(base as *mut u32) = off;
            *(base.add(4) as *mut u32) = len;
        }
    }

    /// Read MinInt/MaxInt: returns (value, has_value). `has_value=false` means
    /// no value has been observed yet (zero-init from `alloc_group`).
    #[inline]
    pub(super) unsafe fn read_min_max_int(&self, group_idx: u32, slot: usize) -> (i64, bool) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self
                .buf
                .as_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            let val = *(base as *const i64);
            let has = *(base.add(8) as *const i64);
            (val, has != 0)
        }
    }

    /// Write MinInt/MaxInt value + has_value flag.
    #[inline]
    pub(super) unsafe fn write_min_max_int(
        &mut self,
        group_idx: u32,
        slot: usize,
        val: i64,
        has: bool,
    ) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self
                .buf
                .as_mut_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            *(base as *mut i64) = val;
            *(base.add(8) as *mut i64) = if has { 1 } else { 0 };
        }
    }

    /// Update MinInt: replace stored value if `candidate < stored` or no value yet.
    #[inline]
    pub(super) unsafe fn update_min_int(&mut self, group_idx: u32, slot: usize, candidate: i64) {
        unsafe {
            let (val, has) = self.read_min_max_int(group_idx, slot);
            if !has || candidate < val {
                self.write_min_max_int(group_idx, slot, candidate, true);
            }
        }
    }

    /// Update MaxInt: replace stored value if `candidate > stored` or no value yet.
    #[inline]
    pub(super) unsafe fn update_max_int(&mut self, group_idx: u32, slot: usize, candidate: i64) {
        unsafe {
            let (val, has) = self.read_min_max_int(group_idx, slot);
            if !has || candidate > val {
                self.write_min_max_int(group_idx, slot, candidate, true);
            }
        }
    }
}

/// Check if all aggregates can use the compact accumulator path.
fn can_use_compact_accs(agg_specs: &[AggExecSpec]) -> bool {
    if agg_specs.is_empty() {
        return false;
    }
    agg_specs.iter().all(|spec| match spec.agg_type {
        AggType::CountStar | AggType::Count => true,
        AggType::Sum | AggType::Avg => {
            let t = spec.col_type_oid;
            t == pg_sys::INT2OID
                || t == pg_sys::INT4OID
                || t == pg_sys::INT8OID
                || t == pg_sys::FLOAT4OID
                || t == pg_sys::FLOAT8OID
        }
        AggType::Min | AggType::Max => {
            let t = spec.col_type_oid;
            t == pg_sys::TEXTOID
                || t == pg_sys::VARCHAROID
                || t == pg_sys::BPCHAROID
                || t == pg_sys::INT2OID
                || t == pg_sys::INT4OID
                || t == pg_sys::INT8OID
                || t == pg_sys::DATEOID
                || t == pg_sys::TIMESTAMPOID
                || t == pg_sys::TIMESTAMPTZOID
        }
        AggType::CountDistinct => {
            let t = spec.col_type_oid;
            t == pg_sys::INT2OID
                || t == pg_sys::INT4OID
                || t == pg_sys::INT8OID
                || t == pg_sys::TEXTOID
                || t == pg_sys::VARCHAROID
                || t == pg_sys::BPCHAROID
        }
    })
}

/// Select the top-N (packed_key, group_idx) pairs by reading raw sort values
/// from compact storage, without full finalization.
///
/// Uses a BinaryHeap of size `limit` to find the top-N in O(n log limit) time.
unsafe fn compact_topn_select(
    map: &CompactGroupMap,
    storage: &CompactAccStorage,
    sort_slot: usize,
    limit: usize,
    ascending: bool,
    sort_is_avg: bool,
) -> Vec<(u128, u32)> {
    unsafe {
        let (_, kind) = storage.layout.slots[sort_slot];
        let read_val = |group_idx: u32| -> i64 {
            if sort_is_avg {
                let avg = match kind {
                    CompactAccKind::SumIntNarrow => {
                        let (s, c) = storage.read_sum_int_narrow(group_idx, sort_slot);
                        if c > 0 { s as f64 / c as f64 } else { 0.0 }
                    }
                    CompactAccKind::SumFloat => {
                        let (s, c) = storage.read_sum_float(group_idx, sort_slot);
                        if c > 0 { s / c as f64 } else { 0.0 }
                    }
                    _ => storage.read_count(group_idx, sort_slot) as f64,
                };
                let bits = avg.to_bits() as i64;
                if bits >= 0 { bits } else { bits ^ i64::MAX }
            } else {
                match kind {
                    CompactAccKind::Count => storage.read_count(group_idx, sort_slot),
                    CompactAccKind::SumIntNarrow => {
                        storage.read_sum_int_narrow(group_idx, sort_slot).0
                    }
                    CompactAccKind::MinInt | CompactAccKind::MaxInt => {
                        storage.read_min_max_int(group_idx, sort_slot).0
                    }
                    _ => storage.read_count(group_idx, sort_slot),
                }
            }
        };
        if ascending {
            // Min-N: max-heap evicts the largest, keeping the smallest N
            let mut heap: BinaryHeap<(i64, u128, u32)> = BinaryHeap::with_capacity(limit + 1);
            for (&packed_key, &group_idx) in map {
                let val = read_val(group_idx);
                heap.push((val, packed_key, group_idx));
                if heap.len() > limit {
                    heap.pop();
                }
            }
            let mut result: Vec<(u128, u32)> = heap.into_iter().map(|(_, k, g)| (k, g)).collect();
            result.sort_by_key(|&(_, g)| read_val(g));
            result
        } else {
            // Max-N: min-heap (via Reverse) evicts the smallest, keeping the largest N
            let mut heap: BinaryHeap<Reverse<(i64, u128, u32)>> =
                BinaryHeap::with_capacity(limit + 1);
            for (&packed_key, &group_idx) in map {
                let val = read_val(group_idx);
                heap.push(Reverse((val, packed_key, group_idx)));
                if heap.len() > limit {
                    heap.pop();
                }
            }
            let mut result: Vec<(u128, u32)> =
                heap.into_iter().map(|Reverse((_, k, g))| (k, g)).collect();
            result.sort_by_key(|&(_, gb)| std::cmp::Reverse(read_val(gb)));
            result
        }
    }
}

/// Finalize a compact accumulator slot into a (Datum, is_null) pair.
pub(super) unsafe fn compact_finalize(
    storage: &CompactAccStorage,
    group_idx: u32,
    slot: usize,
    spec: &AggExecSpec,
) -> (pg_sys::Datum, bool) {
    unsafe {
        let (_, kind) = storage.layout.slots[slot];
        match kind {
            CompactAccKind::Count => {
                let count = storage.read_count(group_idx, slot);
                (pg_sys::Datum::from(count as usize), false)
            }
            CompactAccKind::SumInt => {
                let (sum, count) = storage.read_sum_int(group_idx, slot);
                if count == 0 {
                    return (pg_sys::Datum::from(0usize), true);
                }
                match spec.agg_type {
                    AggType::Sum => {
                        if spec.col_type_oid == pg_sys::INT8OID {
                            (i128_to_numeric_datum(sum), false)
                        } else {
                            (pg_sys::Datum::from(sum as i64 as usize), false)
                        }
                    }
                    AggType::Avg => {
                        let sum_numeric = i128_to_numeric_datum(sum);
                        let count_numeric = pg_sys::OidFunctionCall1Coll(
                            pg_sys::Oid::from(1781u32),
                            pg_sys::InvalidOid,
                            pg_sys::Datum::from(count as usize),
                        );
                        let datum = pg_sys::OidFunctionCall2Coll(
                            pg_sys::Oid::from(1727u32),
                            pg_sys::InvalidOid,
                            sum_numeric,
                            count_numeric,
                        );
                        (datum, false)
                    }
                    _ => (pg_sys::Datum::from(sum as i64 as usize), false),
                }
            }
            CompactAccKind::SumIntNarrow => {
                let (sum, count) = storage.read_sum_int_narrow(group_idx, slot);
                if count == 0 {
                    return (pg_sys::Datum::from(0usize), true);
                }
                match spec.agg_type {
                    AggType::Sum => {
                        // SUM(int2/int4) → INT8
                        (pg_sys::Datum::from(sum as usize), false)
                    }
                    AggType::Avg => {
                        // AVG(int*) → NUMERIC
                        let sum_numeric = i128_to_numeric_datum(sum as i128);
                        let count_numeric = pg_sys::OidFunctionCall1Coll(
                            pg_sys::Oid::from(1781u32),
                            pg_sys::InvalidOid,
                            pg_sys::Datum::from(count as usize),
                        );
                        let datum = pg_sys::OidFunctionCall2Coll(
                            pg_sys::Oid::from(1727u32),
                            pg_sys::InvalidOid,
                            sum_numeric,
                            count_numeric,
                        );
                        (datum, false)
                    }
                    _ => (pg_sys::Datum::from(sum as usize), false),
                }
            }
            CompactAccKind::SumFloat => {
                let (sum, count) = storage.read_sum_float(group_idx, slot);
                if count == 0 {
                    return (pg_sys::Datum::from(0usize), true);
                }
                match spec.agg_type {
                    AggType::Sum => {
                        if spec.col_type_oid == pg_sys::FLOAT4OID {
                            let f4 = sum as f32;
                            (pg_sys::Datum::from(f4.to_bits() as usize), false)
                        } else {
                            (pg_sys::Datum::from(sum.to_bits() as usize), false)
                        }
                    }
                    AggType::Avg => {
                        let avg = sum / count as f64;
                        (pg_sys::Datum::from(avg.to_bits() as usize), false)
                    }
                    _ => (pg_sys::Datum::from(sum.to_bits() as usize), false),
                }
            }
            CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                let (off, len) = storage.read_min_max_str(group_idx, slot);
                if off == u32::MAX {
                    (pg_sys::Datum::from(0usize), true) // NULL
                } else {
                    let s = storage.str_arena.get(off, len);
                    let datum = string_to_datum(s, spec.col_type_oid);
                    (datum, false)
                }
            }
            CompactAccKind::MinInt | CompactAccKind::MaxInt => {
                let (val, has) = storage.read_min_max_int(group_idx, slot);
                if !has {
                    (pg_sys::Datum::from(0usize), true) // NULL
                } else {
                    // H.2: monotonic post-shift for the timestamptz_pl_interval
                    // recognizer. `OutputTransform::None` is the no-op identity.
                    let out = match spec.output_transform {
                        OutputTransform::None => val,
                        OutputTransform::PgUsShift { delta } => val.wrapping_add(delta),
                    };
                    (pg_sys::Datum::from(out as usize), false)
                }
            }
            CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                // Count was pre-written into the compact slot by write_counts_to_storage
                let count = storage.read_count(group_idx, slot);
                (pg_sys::Datum::from(count as usize), false)
            }
        }
    }
}

/// Phase C.2 activation — emit a partial-aggregate transition state into a
/// `(Datum, is_null)` pair for the Final Aggregate node above us to combine
/// via `aggcombinefn`. Mirrors `compact_finalize` but stops one step earlier:
/// returns the value at PG's `aggtranstype` rather than the user-visible
/// final type.
///
/// Coverage:
/// - `Count` → `int8` count (combinefn `int8pl`). Same as finalize for this slot.
/// - `SumIntNarrow` (SUM only) → `int8` sum (combinefn `int8pl`).
/// - `SumFloat` (SUM only) → `float8` sum (combinefn `float8_combine` —
///   actually pl, since float8 sum has no count component).
/// - `MinStr` / `MaxStr` → `text` directly (combinefn `text_smaller` /
///   `text_larger`).
/// - `SumInt` (SUM(int8) / AVG / SumIntNarrow AVG / SumFloat AVG) — NOT yet
///   implemented because the `aggtranstype = internal` path needs
///   `int8_avg_serialize` to produce a `bytea` partial state. Add eligibility
///   gate in `add_agg_path` so we don't reach this with an unsupported shape.
/// - `Count` for COUNT(DISTINCT) — has no `aggcombinefn` in PG core; must be
///   excluded by gating.
///
/// `unreachable!` in the unsupported branches is the bug-catcher: if planner
/// gating drifts and we land here at runtime, we'd produce silently-wrong
/// results otherwise.
#[allow(dead_code)] // wired by the C.2 activation planner code in path.rs
unsafe fn compact_emit_partial(
    storage: &CompactAccStorage,
    group_idx: u32,
    slot: usize,
    spec: &AggExecSpec,
) -> (pg_sys::Datum, bool) {
    unsafe {
        let (_, kind) = storage.layout.slots[slot];
        match kind {
            CompactAccKind::Count => {
                // partial state for `count` and `count(*)` is `int8`.
                let count = storage.read_count(group_idx, slot);
                (pg_sys::Datum::from(count as usize), false)
            }
            CompactAccKind::SumIntNarrow => {
                // SUM(int2/int4): partial state is `int8` (the running sum).
                // count is unused at the partial level for SUM. AVG path
                // not supported here yet — gating must reject it.
                if spec.agg_type != AggType::Sum {
                    unreachable!(
                        "compact_emit_partial: SumIntNarrow only supports Sum (got {:?}); \
                         planner gating drift",
                        spec.agg_type,
                    );
                }
                let (sum, _count) = storage.read_sum_int_narrow(group_idx, slot);
                (pg_sys::Datum::from(sum as usize), false)
            }
            CompactAccKind::SumFloat => {
                // SUM(float4/float8): partial state is `float8` (the running
                // sum). count is unused at the partial level. combinefn is
                // `float8pl`.
                if spec.agg_type != AggType::Sum {
                    unreachable!(
                        "compact_emit_partial: SumFloat only supports Sum (got {:?}); \
                         planner gating drift",
                        spec.agg_type,
                    );
                }
                let (sum, count) = storage.read_sum_float(group_idx, slot);
                if count == 0 {
                    return (pg_sys::Datum::from(0usize), true);
                }
                (pg_sys::Datum::from(sum.to_bits() as usize), false)
            }
            CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                // partial state for MIN/MAX is the value itself; combinefn
                // is `text_smaller` / `text_larger`. Same emit as finalize.
                let (off, len) = storage.read_min_max_str(group_idx, slot);
                if off == u32::MAX {
                    (pg_sys::Datum::from(0usize), true)
                } else {
                    let s = storage.str_arena.get(off, len);
                    let datum = string_to_datum(s, spec.col_type_oid);
                    (datum, false)
                }
            }
            CompactAccKind::MinInt | CompactAccKind::MaxInt => {
                // partial state for MIN/MAX(int|timestamp) is the value itself;
                // combinefn is `int*smaller`/`int*larger` (or `timestamp*_smaller`
                // / `timestamp*_larger`). Same emit as finalize — apply the
                // monotonic OutputTransform here too so the post-shift value
                // is what flows up to PG's combinefn.
                let (val, has) = storage.read_min_max_int(group_idx, slot);
                if !has {
                    (pg_sys::Datum::from(0usize), true)
                } else {
                    let out = match spec.output_transform {
                        OutputTransform::None => val,
                        OutputTransform::PgUsShift { delta } => val.wrapping_add(delta),
                    };
                    (pg_sys::Datum::from(out as usize), false)
                }
            }
            CompactAccKind::SumInt => {
                // SUM(int8) partial state is `internal` via int8_avg_serialize
                // → `bytea`. Not implemented yet; gating must reject SUM(int8)
                // / AVG until we wire int8_avg_serialize.
                unreachable!(
                    "compact_emit_partial: SumInt (transtype=internal) not yet supported \
                     for partial emit; planner gating drift",
                );
            }
            CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                unreachable!(
                    "compact_emit_partial: COUNT(DISTINCT) has no PG aggcombinefn; \
                     planner gating drift",
                );
            }
        }
    }
}

// ============================================================================
// Parallel Mixed (int + string) Aggregation
// ============================================================================

/// Compute a 128-bit hash of mixed integer and string group keys.
/// Uses two independent AHasher instances (different seeds) to produce two 64-bit
/// halves, giving collision probability ~2^-128.
fn hash_mixed_key(ints: &[i64], strs: &[Option<&str>]) -> u128 {
    use std::hash::BuildHasher;
    let s1 = ahash::RandomState::with_seeds(
        0xc4a1_b2e3_d4f5_6789,
        0xa1b2_c3d4_e5f6_7890,
        0x1122_3344_5566_7788,
        0x99aa_bbcc_ddee_ff00,
    );
    let s2 = ahash::RandomState::with_seeds(
        0x1234_abcd_5678_ef01,
        0xaabb_ccdd_eeff_0011,
        0xfed0_cba9_8765_4321,
        0x0011_2233_4455_6677,
    );
    let mut h1 = s1.build_hasher();
    let mut h2 = s2.build_hasher();
    for &v in ints {
        v.hash(&mut h1);
        v.hash(&mut h2);
    }
    for s in strs {
        match s {
            Some(s) => {
                0u8.hash(&mut h1);
                s.hash(&mut h1);
                0u8.hash(&mut h2);
                s.hash(&mut h2);
            }
            None => {
                1u8.hash(&mut h1);
                1u8.hash(&mut h2);
            }
        }
    }
    ((h1.finish() as u128) << 64) | (h2.finish() as u128)
}

/// Value stored per group key component in MixedKeyStorage.
#[derive(Debug, Clone, Copy)]
enum MixedKeyVal {
    Null,
    Int(i64),
    Str(u32, u32), // (offset, len) into arena
}

/// Per-worker side table mapping group_idx → actual key values.
/// Needed because the u128 hash is one-way — we need original values at finalization.
struct MixedKeyStorage {
    arena: StringArena,
    /// Flat storage: group i's key components are at keys[i * n_keys .. (i+1) * n_keys]
    keys: Vec<MixedKeyVal>,
    n_keys: usize,
}

impl MixedKeyStorage {
    fn new(n_keys: usize) -> Self {
        MixedKeyStorage {
            arena: StringArena::new(),
            keys: Vec::new(),
            n_keys,
        }
    }

    /// Store key values for a new group. Must be called in order (group 0, 1, 2, ...).
    fn insert(&mut self, ints: &[i64], strs: &[Option<&str>], group_specs: &[GroupByColSpec]) {
        let mut int_idx = 0;
        let mut str_idx = 0;
        for gs in group_specs {
            if is_text_group_col(gs) {
                let s = strs[str_idx];
                str_idx += 1;
                match s {
                    Some(s) => {
                        let (off, len) = self.arena.alloc(s);
                        self.keys.push(MixedKeyVal::Str(off, len));
                    }
                    None => self.keys.push(MixedKeyVal::Null),
                }
            } else {
                self.keys.push(MixedKeyVal::Int(ints[int_idx]));
                int_idx += 1;
            }
        }
    }

    /// Get a key component for a group.
    #[inline]
    fn get(&self, group_idx: u32, col: usize) -> MixedKeyVal {
        self.keys[group_idx as usize * self.n_keys + col]
    }
}

// strcoll_cmp is now in text_col.rs

/// Check if a GROUP BY column is a text type (including RegexpReplace/CaseWhen which produce text).
fn is_text_group_col(gs: &GroupByColSpec) -> bool {
    let is_text_type = matches!(
        gs.type_oid,
        pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID | pg_sys::NAMEOID
    );
    match &gs.expr {
        GroupByExpr::Column | GroupByExpr::RegexpReplace { .. } | GroupByExpr::CaseWhen(_) => {
            is_text_type
        }
        _ => false,
    }
}

fn case_when_references_col(spec: &CaseWhenSpec, col_idx: usize) -> bool {
    spec.clauses.iter().any(|clause| {
        clause.conditions.iter().any(|cond| cond.col_idx == col_idx)
            || matches!(&clause.result, CaseWhenValue::ColumnRef(ci) if *ci == col_idx)
    }) || matches!(&spec.default, CaseWhenValue::ColumnRef(ci) if *ci == col_idx)
}

fn numeric_col_used_only_by_constant_group_keys(
    col_idx: usize,
    group_specs: &[GroupByColSpec],
    const_group_keys: &[Option<i64>],
    batch_quals: &[BatchQual],
    agg_specs: &[AggExecSpec],
) -> bool {
    if batch_quals.iter().any(|bq| bq.col_idx == col_idx) {
        return false;
    }
    if agg_specs
        .iter()
        .any(|spec| spec.col_idx >= 0 && spec.col_idx as usize == col_idx)
    {
        return false;
    }

    let mut saw_const_group_ref = false;
    for (gi, gs) in group_specs.iter().enumerate() {
        if gs.col_idx >= 0 && gs.col_idx as usize == col_idx {
            if const_group_keys.get(gi).and_then(|v| *v).is_some() {
                saw_const_group_ref = true;
            } else {
                return false;
            }
        }
        if let GroupByExpr::CaseWhen(ref spec) = gs.expr
            && case_when_references_col(spec, col_idx)
        {
            return false;
        }
    }

    saw_const_group_ref
}

struct ParallelMixedConfig<'a> {
    agg_specs: &'a [AggExecSpec],
    group_specs: &'a [GroupByColSpec],
    col_names: &'a [String],
    col_types: &'a [pg_sys::Oid],
    segment_by: &'a [String],
    needed_cols: &'a [bool],
    batch_quals: &'a [BatchQual],
    seg_filters: &'a [(usize, String)],
    time_min: Option<i64>,
    time_max: Option<i64>,
    topn_spec: Option<(usize, usize, bool)>,
    /// Which needed_cols indices are text GROUP BY columns
    text_group_col_flags: &'a [bool],
    /// Which needed_cols indices have text WHERE quals (EQ/NE/LIKE)
    text_qual_infos: &'a [TextQualInfo],
    /// Compiled Rust regex info for RegexpReplace GROUP BY columns
    rust_regex_infos: &'a [RustRegexInfo],
    /// Per-column flag: true when the text column is loaded in sidecar-only
    /// mode (length blob instead of main blob). Parallel to col_names.
    sidecar_only_cols: &'a [bool],
    /// F8 optimization: when `Some`, filter phase-1 rows by group-key
    /// hash. Only rows whose `hash_mixed_key` output is in this set are
    /// inserted into the per-worker map; all others are skipped. This
    /// bounds each worker's map to `|preselected|` entries instead of
    /// the full group cardinality. Set iff the bare-LIMIT shape matches
    /// (no ORDER BY, no HAVING, no WHERE) and the Phase-0 probe
    /// succeeded in finding `bare_limit` distinct keys.
    preselected_keys: Option<&'a hashbrown::HashSet<u128>>,
    /// Phase D: leader-precomputed dict-distinct remaps. Keyed by spec_idx
    /// for every CountDistinct(text) spec where every segment is dict-encoded
    /// for the col AND the global-string count is below the bitset threshold.
    /// Workers consult this to set bits in per-(spec, group) `Bitset`s
    /// (`CdKind::DictBitset`) instead of hashing strings into `HashSet<u128>`.
    /// Specs absent from the map keep the existing HashSet path. The
    /// chunk-offset arg to `process_segments_mixed` indexes into
    /// `per_segment` so each worker resolves `(seg_idx, local_dict_id)` →
    /// `global_id` without further coordination.
    dict_distinct_remaps: &'a std::collections::HashMap<usize, DictDistinctRemap>,
}

// TextQualInfo is now in text_col.rs

/// Result of parallel mixed aggregation from one worker thread.
struct ParallelMixedResult {
    compact_map: CompactGroupMap,
    compact_storage: CompactAccStorage,
    mixed_keys: MixedKeyStorage,
    cd_sidecar: CountDistinctSideCar,
    segments_processed: u64,
    rows_processed: u64,
    decompress_us: u64,
    topk: Option<(Vec<u128>, i64)>,
}

// decompress_text_to_seg_col is now in text_col.rs

// apply_text_eq_filter and apply_text_like_filter are now in text_col.rs

/// Check if a query can use the parallel mixed (int+string) aggregation path.
fn can_parallel_mixed(
    group_specs: &[GroupByColSpec],
    needed_cols: &[bool],
    col_types: &[pg_sys::Oid],
    col_not_null: &[bool],
    batch_quals: &[BatchQual],
    agg_specs: &[AggExecSpec],
) -> bool {
    if group_specs.is_empty() || group_specs.len() > 6 {
        return false;
    }

    // Must have at least one text column involved (GROUP BY, qual, or agg) to justify
    // the mixed path instead of the compact path.
    let has_text_group = group_specs.iter().any(is_text_group_col);
    let has_text_qual = batch_quals.iter().any(|bq| {
        let t = bq.type_oid;
        t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID
    });
    let has_text_agg = agg_specs.iter().any(|s| {
        let t = s.col_type_oid;
        s.expr_kind == AggExpr::LengthOf
            || (matches!(
                s.agg_type,
                AggType::Min | AggType::Max | AggType::CountDistinct
            ) && (t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID))
    });
    if !has_text_group && !has_text_qual && !has_text_agg {
        return false;
    }

    // All GROUP BY columns must be either text columns (including RegexpReplace)
    // or integer-producing expressions
    for gs in group_specs {
        if is_text_group_col(gs) {
            continue; // text column (or RegexpReplace with Rust regex) — OK
        }
        // Must be an integer-producing expression.
        // The mixed per-row loop bails on NULL int keys (see has_null check
        // in process_segments_mixed): hash_mixed_key / MixedKeyStorage have
        // no NULL slot for the int side, so NULL groups would collapse into
        // Int(0). Require attnotnull for any numeric column we'd read, same
        // gate as can_use_compact_keys.
        match &gs.expr {
            GroupByExpr::Column => {
                let t = gs.type_oid;
                if !(t == pg_sys::INT2OID
                    || t == pg_sys::INT4OID
                    || t == pg_sys::INT8OID
                    || t == pg_sys::TIMESTAMPOID
                    || t == pg_sys::TIMESTAMPTZOID)
                {
                    return false;
                }
                if !col_not_null
                    .get(gs.col_idx as usize)
                    .copied()
                    .unwrap_or(false)
                {
                    return false;
                }
            }
            GroupByExpr::DateTrunc { .. }
            | GroupByExpr::Extract { .. }
            | GroupByExpr::AddConst { .. } => {
                if !col_not_null
                    .get(gs.col_idx as usize)
                    .copied()
                    .unwrap_or(false)
                {
                    return false;
                }
            }
            GroupByExpr::RegexpReplace { .. } | GroupByExpr::CaseWhen(_) => return false, // non-text RegexpReplace/CaseWhen not supported
        }
    }

    // Accumulators must be compact-compatible
    if !can_use_compact_accs(agg_specs) {
        return false;
    }

    // All non-text needed columns must be numeric (for thread-safe decompression)
    for (i, (&needed, &type_oid)) in needed_cols.iter().zip(col_types.iter()).enumerate() {
        if !needed {
            continue;
        }
        if is_numeric_type(type_oid) {
            continue; // numeric — fine
        }
        // Text column — must be a GROUP BY column, have a supported text qual,
        // or be used in a MIN/MAX aggregation
        let is_text_gb = group_specs
            .iter()
            .any(|gs| gs.col_idx as usize == i && is_text_group_col(gs));
        // Also check if this column is referenced by a CaseWhen result ColumnRef
        let is_case_when_ref = group_specs.iter().any(|gs| {
            if let GroupByExpr::CaseWhen(ref spec) = gs.expr {
                spec.clauses
                    .iter()
                    .any(|c| matches!(&c.result, CaseWhenValue::ColumnRef(ci) if *ci == i))
                    || matches!(&spec.default, CaseWhenValue::ColumnRef(ci) if *ci == i)
            } else {
                false
            }
        });
        let has_text_qual = batch_quals.iter().any(|bq| {
            bq.col_idx == i
                && ((matches!(bq.op, BatchCompareOp::Eq | BatchCompareOp::Ne)
                    && bq.text_const.is_some())
                    || (matches!(bq.op, BatchCompareOp::Like | BatchCompareOp::NotLike)
                        && bq.like_strategy.is_some()))
        });
        let is_text_minmax_agg = agg_specs.iter().any(|s| {
            s.col_idx as usize == i
                && matches!(s.agg_type, AggType::Min | AggType::Max)
                && (type_oid == pg_sys::TEXTOID
                    || type_oid == pg_sys::VARCHAROID
                    || type_oid == pg_sys::BPCHAROID)
        });
        let is_text_cd_agg = agg_specs.iter().any(|s| {
            s.col_idx as usize == i
                && s.agg_type == AggType::CountDistinct
                && (type_oid == pg_sys::TEXTOID
                    || type_oid == pg_sys::VARCHAROID
                    || type_oid == pg_sys::BPCHAROID)
        });
        let is_text_length_agg = agg_specs.iter().any(|s| {
            s.col_idx as usize == i
                && s.expr_kind == AggExpr::LengthOf
                && (type_oid == pg_sys::TEXTOID
                    || type_oid == pg_sys::VARCHAROID
                    || type_oid == pg_sys::BPCHAROID)
        });
        if !is_text_gb
            && !is_case_when_ref
            && !has_text_qual
            && !is_text_minmax_agg
            && !is_text_cd_agg
            && !is_text_length_agg
        {
            return false; // unsupported column type
        }
    }

    // Text batch quals must be EQ/NE with text_const, LIKE/NotLike with
    // like_strategy, or InList with in_list_text.
    for bq in batch_quals {
        let t = bq.type_oid;
        if t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID {
            match bq.op {
                BatchCompareOp::Eq | BatchCompareOp::Ne => {
                    if bq.text_const.is_none() {
                        return false;
                    }
                }
                BatchCompareOp::Like | BatchCompareOp::NotLike => {
                    if bq.like_strategy.is_none() {
                        return false;
                    }
                }
                BatchCompareOp::InList => {
                    if bq.in_list_text.is_none() {
                        return false;
                    }
                }
                _ => return false, // unsupported text comparison
            }
        }
    }

    true
}

/// Process a chunk of segments on a worker thread using the mixed (int+string) path.
/// F8 Phase 0 — try to extract `bare_limit` distinct hashed group keys by
/// scanning up to `max_probe_segments` segments. Returns Some(set) iff the
/// set reaches `bare_limit` entries; None otherwise (caller falls back to
/// the normal full-agg path). Mirrors the decompression + key-building logic
/// of `process_segments_mixed` but without aggregation, WHERE filtering, or
/// top-N tracking. Caller must have gated this on: no WHERE clause, no
/// HAVING, no CaseWhen / RegexpReplace group-by expression.
#[allow(clippy::too_many_arguments)]
fn try_build_preselected(
    bare_limit: usize,
    segments: &[SegmentData],
    group_specs: &[GroupByColSpec],
    col_names: &[String],
    col_types: &[pg_sys::Oid],
    segment_by: &[String],
    needed_cols: &[bool],
    text_group_col_flags: &[bool],
    max_probe_segments: usize,
) -> Option<hashbrown::HashSet<u128>> {
    if bare_limit == 0 {
        return None;
    }

    // Bail on group-by expressions the probe doesn't support.
    for gs in group_specs {
        match gs.expr {
            GroupByExpr::Column
            | GroupByExpr::AddConst { .. }
            | GroupByExpr::DateTrunc { .. }
            | GroupByExpr::Extract { .. } => {}
            _ => return None,
        }
    }

    let n_int_keys = group_specs
        .iter()
        .filter(|gs| !is_text_group_col(gs))
        .count();
    let n_str_keys = group_specs
        .iter()
        .filter(|gs| is_text_group_col(gs))
        .count();

    let mut keys: hashbrown::HashSet<u128> = hashbrown::HashSet::with_capacity(bare_limit.max(16));

    let probe_budget = max_probe_segments.min(segments.len());

    for seg in segments.iter().take(probe_budget) {
        if seg.row_count == 0 {
            continue;
        }

        // Decompress GROUP BY columns for this segment only.
        let mut numeric_cols: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
        let mut text_seg_cols: Vec<Option<SegTextColumn>> = Vec::new();
        let mut blob_idx = 0;
        let mut seg_val_idx = 0;

        for (col_idx, col_name) in col_names.iter().enumerate() {
            let type_oid = col_types[col_idx];

            if !needed_cols[col_idx] {
                if segment_by.contains(col_name) {
                    seg_val_idx += 1;
                } else {
                    blob_idx += 1;
                }
                numeric_cols.push(Vec::new());
                text_seg_cols.push(None);
                continue;
            }

            if segment_by.contains(col_name) {
                if text_group_col_flags[col_idx] {
                    let val = &seg.segment_values[seg_val_idx];
                    text_seg_cols.push(Some(SegTextColumn::SegBy(val.clone())));
                    numeric_cols.push(Vec::new());
                } else {
                    let val = &seg.segment_values[seg_val_idx];
                    let (datum, is_null) = match val {
                        Some(s) => (parse_string_to_datum(s, type_oid), false),
                        None => (pg_sys::Datum::from(0usize), true),
                    };
                    let repeated: Vec<(pg_sys::Datum, bool)> =
                        (0..seg.row_count).map(|_| (datum, is_null)).collect();
                    numeric_cols.push(repeated);
                    text_seg_cols.push(None);
                }
                seg_val_idx += 1;
            } else {
                // Guard against an under-sized blob vector (shouldn't happen
                // in practice, but Phase 0 runs very early — skip this
                // segment rather than panic).
                if blob_idx >= seg.compressed_blobs.len() {
                    return None;
                }
                let blob = &seg.compressed_blobs[blob_idx];
                if text_group_col_flags[col_idx] {
                    text_seg_cols.push(decompress_text_to_seg_col(blob));
                    numeric_cols.push(Vec::new());
                } else if is_numeric_type(type_oid) {
                    numeric_cols.push(decompress_numeric_blob(blob, type_oid));
                    text_seg_cols.push(None);
                } else {
                    // Unexpected type — bail cleanly.
                    return None;
                }
                blob_idx += 1;
            }
        }

        let row_count = seg.row_count as usize;
        let mut int_keys_buf = vec![0i64; n_int_keys];
        let mut str_keys_buf: Vec<Option<&str>> = vec![None; n_str_keys];

        for row in 0..row_count {
            let mut has_null = false;
            let mut int_idx = 0;
            let mut str_idx = 0;
            for gs in group_specs {
                if is_text_group_col(gs) {
                    let col_idx = gs.col_idx as usize;
                    str_keys_buf[str_idx] = text_seg_cols[col_idx]
                        .as_ref()
                        .and_then(|sc| sc.get_str(row));
                    str_idx += 1;
                } else {
                    let col = &numeric_cols[gs.col_idx as usize];
                    if col.is_empty() || col[row].1 {
                        has_null = true;
                        break;
                    }
                    int_keys_buf[int_idx] = match &gs.expr {
                        GroupByExpr::DateTrunc { unit_usecs, .. } => {
                            let pg_usec = col[row].0.value() as i64;
                            pg_usec.div_euclid(*unit_usecs) * *unit_usecs
                        }
                        GroupByExpr::Extract { unit, divisor, .. } => {
                            eval_extract(col[row].0.value() as i64, *divisor, unit)
                        }
                        GroupByExpr::AddConst { offset, .. } => col[row].0.value() as i64 + offset,
                        GroupByExpr::Column => col[row].0.value() as i64,
                        _ => unreachable!(),
                    };
                    int_idx += 1;
                }
            }
            if has_null {
                continue;
            }

            let hash_key = hash_mixed_key(&int_keys_buf[..n_int_keys], &str_keys_buf[..n_str_keys]);
            keys.insert(hash_key);
            if keys.len() >= bare_limit {
                return Some(keys);
            }
        }
    }

    // Probe exhausted without reaching bare_limit distinct keys.
    None
}

fn process_segments_mixed(
    segments: &[SegmentData],
    chunk_offset: usize,
    config: &ParallelMixedConfig,
) -> ParallelMixedResult {
    let mut compact_map = CompactGroupMap::with_hasher(BuildHasherDefault::default());
    let mut compact_storage = CompactAccStorage::new(CompactAccLayout::new(config.agg_specs));
    let num_group_keys = config.group_specs.len();
    let mut mixed_keys = MixedKeyStorage::new(num_group_keys);
    // Phase D: classify each CountDistinct(text) spec as DictBitset when the
    // leader pre-pass produced a remap for it. Bitset size = global string
    // count for the column. Sized lookup map kept on the stack — at most a
    // handful of CountDistinct specs per query.
    let dict_remap_sizes: std::collections::HashMap<usize, u32> = config
        .dict_distinct_remaps
        .iter()
        .map(|(&spec_idx, remap)| (spec_idx, remap.global_count))
        .collect();
    let mut cd_sidecar =
        CountDistinctSideCar::new_with_dict_remaps(config.agg_specs, &dict_remap_sizes);
    let mut segments_processed: u64 = 0;
    let mut rows_processed: u64 = 0;
    let mut decompress_us: u64 = 0;

    // Count int and str group keys
    let n_int_keys = config
        .group_specs
        .iter()
        .filter(|gs| !is_text_group_col(gs))
        .count();
    let n_str_keys = config
        .group_specs
        .iter()
        .filter(|gs| is_text_group_col(gs))
        .count();

    for (rel_idx, seg) in segments.iter().enumerate() {
        // Phase D: absolute seg_idx into all_segments (and into each
        // dict_distinct_remaps.per_segment). The chunk-mode `for seg in segments`
        // doesn't expose this; we shadow it via `(rel_idx, seg)` so the bitset
        // lookup at the per-row insert site can resolve `(spec_idx, seg_idx,
        // local_id) → global_id` without further coordination.
        let seg_idx_in_all = chunk_offset + rel_idx;
        if seg.row_count == 0 {
            continue;
        }

        // Segment-by pruning
        if !config.seg_filters.is_empty() {
            let mut skip = false;
            for &(seg_val_idx, ref filter_val) in config.seg_filters {
                match &seg.segment_values[seg_val_idx] {
                    Some(val) if val == filter_val => {}
                    _ => {
                        skip = true;
                        break;
                    }
                }
            }
            if skip {
                continue;
            }
        }

        // Time-range pruning
        if let (Some(seg_min), Some(seg_max)) = (seg.min_time, seg.max_time) {
            if config.time_min.is_some_and(|query_min| seg_max < query_min) {
                continue;
            }
            if config.time_max.is_some_and(|query_max| seg_min > query_max) {
                continue;
            }
        }

        // Dictionary-based LIKE pruning
        if segment_skippable_by_dict(
            config.batch_quals,
            config.col_names,
            config.segment_by,
            &seg.compressed_blobs,
        ) {
            continue;
        }

        // C.3 per-segment fast path: classify against the **numeric subset**
        // of batch_quals using col_minmax / nonzero_count. NonePass on any
        // numeric qual rules out the segment (text quals can only narrow
        // further); AllPass lets us skip the per-row numeric eval below
        // while still applying text quals on top of an empty selection.
        // No-op when batch_quals has no numeric entries (helper returns
        // Ambiguous in that case).
        let numeric_quals_all_pass = if config.batch_quals.is_empty() {
            true
        } else {
            match classify_segment_quals_numeric(seg, config.batch_quals, config.col_names) {
                SegmentQualResult::NonePass => continue,
                SegmentQualResult::AllPass => true,
                SegmentQualResult::Ambiguous => false,
            }
        };

        segments_processed += 1;

        let mut const_group_keys: Vec<Option<i64>> = vec![None; config.group_specs.len()];
        for (gi, gs) in config.group_specs.iter().enumerate() {
            if is_text_group_col(gs) {
                continue;
            }
            let GroupByExpr::Extract { unit, divisor, .. } = &gs.expr else {
                continue;
            };
            let col_idx = gs.col_idx as usize;
            let Some(col_name) = config.col_names.get(col_idx) else {
                continue;
            };
            let Some(cm) = seg.col_minmax.get(col_name) else {
                continue;
            };
            const_group_keys[gi] = constant_extract_key_for_segment(cm, *divisor, unit);
        }

        let skip_numeric_decompress: Vec<bool> = (0..config.col_names.len())
            .map(|col_idx| {
                numeric_col_used_only_by_constant_group_keys(
                    col_idx,
                    config.group_specs,
                    &const_group_keys,
                    config.batch_quals,
                    config.agg_specs,
                )
            })
            .collect();

        // Decompress needed columns
        let t_dec = Instant::now();
        let mut numeric_cols: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
        let mut text_seg_cols: Vec<Option<SegTextColumn>> = Vec::new();
        let mut blob_idx = 0;
        let mut seg_val_idx = 0;

        for (col_idx, col_name) in config.col_names.iter().enumerate() {
            let type_oid = config.col_types[col_idx];

            if !config.needed_cols[col_idx] {
                if config.segment_by.contains(col_name) {
                    seg_val_idx += 1;
                } else {
                    blob_idx += 1;
                }
                numeric_cols.push(Vec::new());
                text_seg_cols.push(None);
                continue;
            }

            if config.segment_by.contains(col_name) {
                if config.text_group_col_flags[col_idx] {
                    // Text segment-by column
                    let val = &seg.segment_values[seg_val_idx];
                    text_seg_cols.push(Some(SegTextColumn::SegBy(val.clone())));
                    numeric_cols.push(Vec::new());
                } else {
                    // Numeric segment-by column
                    let val = &seg.segment_values[seg_val_idx];
                    let (datum, is_null) = match val {
                        Some(s) => (parse_string_to_datum(s, type_oid), false),
                        None => (pg_sys::Datum::from(0usize), true),
                    };
                    let repeated: Vec<(pg_sys::Datum, bool)> =
                        (0..seg.row_count).map(|_| (datum, is_null)).collect();
                    numeric_cols.push(repeated);
                    text_seg_cols.push(None);
                }
                seg_val_idx += 1;
            } else if config
                .sidecar_only_cols
                .get(col_idx)
                .copied()
                .unwrap_or(false)
            {
                // Text column in sidecar-only mode: main blob wasn't loaded;
                // decode the length sidecar instead.
                let sidecar_blob = &seg.text_length_blobs[blob_idx];
                text_seg_cols.push(decompress_length_sidecar(sidecar_blob));
                numeric_cols.push(Vec::new());
                blob_idx += 1;
            } else {
                let blob = &seg.compressed_blobs[blob_idx];
                if config.text_group_col_flags[col_idx] {
                    // Text GROUP BY column — decompress to SegTextColumn
                    text_seg_cols.push(decompress_text_to_seg_col(blob));
                    numeric_cols.push(Vec::new());
                } else if skip_numeric_decompress[col_idx] {
                    numeric_cols.push(Vec::new());
                    text_seg_cols.push(None);
                } else if is_numeric_type(type_oid) {
                    // Numeric column
                    numeric_cols.push(decompress_numeric_blob(blob, type_oid));
                    text_seg_cols.push(None);
                } else {
                    // Text column needed only for WHERE qual (not GROUP BY)
                    text_seg_cols.push(decompress_text_to_seg_col(blob));
                    numeric_cols.push(Vec::new());
                }
                blob_idx += 1;
            }
        }
        // Build regex-transformed text columns for GROUP BY keys (separate from originals,
        // since the original column may also be needed for aggregation like MIN/AVG/length)
        let regex_text_cols: Vec<Option<SegTextColumn>> = if config.rust_regex_infos.is_empty() {
            Vec::new()
        } else {
            let mut cols: Vec<Option<SegTextColumn>> =
                (0..config.col_names.len()).map(|_| None).collect();
            for ri in config.rust_regex_infos {
                if let Some(ref seg_col) = text_seg_cols[ri.col_idx] {
                    cols[ri.col_idx] =
                        Some(apply_regex_to_seg_col(seg_col, &ri.regex, &ri.replacement));
                }
            }
            cols
        };

        decompress_us += t_dec.elapsed().as_micros() as u64;

        let row_count = seg.row_count as usize;

        // Build selection vector from quals
        // First: numeric batch quals — skip the per-row eval when C.3
        // metadata classification already proved every row passes the
        // numeric subset.
        let mut selection = if numeric_quals_all_pass {
            Vec::new()
        } else {
            evaluate_batch_quals(&numeric_cols, row_count, config.batch_quals, Vec::new())
        };

        // Then: text quals (applied on SegTextColumn, short-circuiting via selection)
        for tqi in config.text_qual_infos {
            match tqi {
                TextQualInfo::EqNe {
                    col_idx,
                    const_str,
                    is_ne,
                } => {
                    if let Some(ref seg_col) = text_seg_cols[*col_idx] {
                        apply_text_eq_filter(seg_col, const_str, *is_ne, row_count, &mut selection);
                    }
                }
                TextQualInfo::Like {
                    col_idx,
                    strategy,
                    negate,
                } => {
                    if let Some(ref seg_col) = text_seg_cols[*col_idx] {
                        apply_text_like_filter(
                            seg_col,
                            strategy,
                            *negate,
                            row_count,
                            &mut selection,
                        );
                    }
                }
                TextQualInfo::InList { col_idx, values } => {
                    if let Some(ref seg_col) = text_seg_cols[*col_idx] {
                        apply_text_in_filter(seg_col, values, row_count, &mut selection);
                    }
                }
            }
        }
        // Early skip: if all rows are filtered out, skip aggregation for this segment
        if !selection.is_empty() && !selection.iter().any(|&b| b) {
            continue;
        }

        // Build CaseWhen-transformed text columns (indexed by group spec index)
        let case_when_text_cols: Vec<Option<SegTextColumn>> = config
            .group_specs
            .iter()
            .map(|gs| {
                if let GroupByExpr::CaseWhen(ref spec) = gs.expr {
                    Some(apply_case_when_to_seg_col(
                        spec,
                        &numeric_cols,
                        &text_seg_cols,
                        row_count,
                        &selection,
                    ))
                } else {
                    None
                }
            })
            .collect();

        // Aggregation loop
        let mut int_keys = vec![0i64; n_int_keys];
        let mut str_keys: Vec<Option<&str>> = vec![None; n_str_keys];

        for row in 0..row_count {
            if !selection.is_empty() && !selection[row] {
                continue;
            }
            rows_processed += 1;

            // Build key components
            let mut has_null = false;
            let mut int_idx = 0;
            let mut str_idx = 0;
            for (gi, gs) in config.group_specs.iter().enumerate() {
                if is_text_group_col(gs) {
                    // CaseWhen: use pre-computed column indexed by group spec index
                    if matches!(gs.expr, GroupByExpr::CaseWhen(_)) {
                        if let Some(Some(seg_col)) = case_when_text_cols.get(gi) {
                            str_keys[str_idx] = seg_col.get_str(row);
                        } else {
                            str_keys[str_idx] = None;
                        }
                        str_idx += 1;
                        continue;
                    }
                    let col_idx = gs.col_idx as usize;
                    // For RegexpReplace columns, use the pre-transformed column;
                    // for plain text columns, use the original
                    let seg_col_ref = if matches!(gs.expr, GroupByExpr::RegexpReplace { .. })
                        && !regex_text_cols.is_empty()
                    {
                        regex_text_cols[col_idx].as_ref()
                    } else {
                        text_seg_cols[col_idx].as_ref()
                    };
                    match seg_col_ref {
                        Some(seg_col) => {
                            str_keys[str_idx] = seg_col.get_str(row);
                            // NULL text key: leave str_keys[str_idx] as None.
                            // hash_mixed_key and MixedKeyStorage handle None correctly,
                            // producing a NULL group (matching PostgreSQL GROUP BY semantics).
                        }
                        None => {
                            str_keys[str_idx] = None;
                        }
                    }
                    str_idx += 1;
                } else {
                    if let Some(v) = const_group_keys[gi] {
                        int_keys[int_idx] = v;
                    } else {
                        let col = &numeric_cols[gs.col_idx as usize];
                        if col.is_empty() || col[row].1 {
                            has_null = true;
                            break;
                        }
                        int_keys[int_idx] = match &gs.expr {
                            GroupByExpr::DateTrunc { unit_usecs, .. } => {
                                let pg_usec = col[row].0.value() as i64;
                                pg_usec.div_euclid(*unit_usecs) * *unit_usecs
                            }
                            GroupByExpr::Extract { unit, divisor, .. } => {
                                eval_extract(col[row].0.value() as i64, *divisor, unit)
                            }
                            GroupByExpr::AddConst { offset, .. } => {
                                col[row].0.value() as i64 + offset
                            }
                            GroupByExpr::Column => col[row].0.value() as i64,
                            _ => unreachable!(),
                        };
                    }
                    int_idx += 1;
                }
            }

            if has_null {
                continue;
            }

            let hash_key = hash_mixed_key(&int_keys[..n_int_keys], &str_keys[..n_str_keys]);

            // F8: when a preselected key set is supplied, skip rows whose
            // group-key hash is not in the set. The set is bounded to
            // `bare_limit` entries (typically 10), so the probe is an
            // L1-resident hashbrown lookup (~5 ns). Rows that hit proceed
            // to the normal Entry path below; rows that miss contribute
            // nothing to any output group.
            if let Some(preselected) = config.preselected_keys
                && !preselected.contains(&hash_key)
            {
                continue;
            }

            // Lookup or insert group
            if compact_map.len() == compact_map.capacity() {
                let cap = compact_map.capacity();
                if cap >= 32_000_000 {
                    compact_map.reserve(8_000_000);
                }
            }
            let group_idx = match compact_map.entry(hash_key) {
                hashbrown::hash_map::Entry::Occupied(e) => *e.get(),
                hashbrown::hash_map::Entry::Vacant(e) => {
                    let idx = compact_storage.alloc_group();
                    cd_sidecar.alloc_group();
                    e.insert(idx);
                    // Store actual key values for this new group
                    mixed_keys.insert(
                        &int_keys[..n_int_keys],
                        &str_keys[..n_str_keys],
                        config.group_specs,
                    );
                    idx
                }
            };

            // Update compact accumulators (same as compact path)
            for (spec_idx, spec) in config.agg_specs.iter().enumerate() {
                let (_, kind) = compact_storage.layout.slots[spec_idx];
                match kind {
                    CompactAccKind::Count => {
                        match spec.agg_type {
                            AggType::CountStar => unsafe {
                                *compact_storage.count_mut(group_idx, spec_idx) += 1;
                            },
                            AggType::Count => {
                                let col = &numeric_cols[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    unsafe {
                                        *compact_storage.count_mut(group_idx, spec_idx) += 1;
                                    }
                                } else {
                                    // Check text columns for COUNT(text_col)
                                    if let Some(ref seg_col) = text_seg_cols[spec.col_idx as usize]
                                        && seg_col.get_str(row).is_some()
                                    {
                                        unsafe {
                                            *compact_storage.count_mut(group_idx, spec_idx) += 1;
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    CompactAccKind::SumInt => {
                        let col = &numeric_cols[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = datum_to_i128(col[row].0, spec.col_type_oid);
                            let (sum, count) =
                                unsafe { compact_storage.sum_int_mut(group_idx, spec_idx) };
                            if spec.expr_kind == AggExpr::AddConst {
                                *sum += v + spec.const_offset as i128;
                            } else {
                                *sum += v;
                            }
                            *count += 1;
                        } else if spec.expr_kind == AggExpr::LengthOf
                            && let Some(ref seg_col) = text_seg_cols[spec.col_idx as usize]
                            && let Some(len) = seg_col.get_len(row)
                        {
                            let (sum, count) =
                                unsafe { compact_storage.sum_int_mut(group_idx, spec_idx) };
                            *sum += len as i128;
                            *count += 1;
                        }
                    }
                    CompactAccKind::SumIntNarrow => {
                        let col = &numeric_cols[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = col[row].0.value() as i64;
                            let (sum, count) =
                                unsafe { compact_storage.sum_int_narrow_mut(group_idx, spec_idx) };
                            if spec.expr_kind == AggExpr::AddConst {
                                *sum += v + spec.const_offset;
                            } else {
                                *sum += v;
                            }
                            *count += 1;
                        } else if spec.expr_kind == AggExpr::LengthOf
                            && let Some(ref seg_col) = text_seg_cols[spec.col_idx as usize]
                            && let Some(len) = seg_col.get_len(row)
                        {
                            let (sum, count) =
                                unsafe { compact_storage.sum_int_narrow_mut(group_idx, spec_idx) };
                            *sum += len as i64;
                            *count += 1;
                        }
                    }
                    CompactAccKind::SumFloat => {
                        let col = &numeric_cols[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = datum_to_f64(col[row].0, spec.col_type_oid);
                            let (sum, count) =
                                unsafe { compact_storage.sum_float_mut(group_idx, spec_idx) };
                            if spec.expr_kind == AggExpr::AddConst {
                                *sum += v + spec.const_offset as f64;
                            } else {
                                *sum += v;
                            }
                            *count += 1;
                        } else if spec.expr_kind == AggExpr::LengthOf
                            && let Some(ref seg_col) = text_seg_cols[spec.col_idx as usize]
                            && let Some(len) = seg_col.get_len(row)
                        {
                            let (sum, count) =
                                unsafe { compact_storage.sum_float_mut(group_idx, spec_idx) };
                            *sum += len as f64;
                            *count += 1;
                        }
                    }
                    CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                        let col_idx = spec.col_idx as usize;
                        if let Some(ref seg_col) = text_seg_cols[col_idx]
                            && let Some(s) = seg_col.get_str(row)
                        {
                            let (cur_off, cur_len) =
                                unsafe { compact_storage.read_min_max_str(group_idx, spec_idx) };
                            let should_update = if cur_off == u32::MAX {
                                true // no current value
                            } else {
                                let cur = compact_storage.str_arena.get(cur_off, cur_len);
                                let cmp = strcoll_cmp(s, cur);
                                match kind {
                                    CompactAccKind::MinStr => cmp == std::cmp::Ordering::Less,
                                    CompactAccKind::MaxStr => cmp == std::cmp::Ordering::Greater,
                                    _ => unreachable!(),
                                }
                            };
                            if should_update {
                                let (new_off, new_len) = compact_storage.str_arena.alloc(s);
                                unsafe {
                                    compact_storage
                                        .write_min_max_str(group_idx, spec_idx, new_off, new_len);
                                }
                            }
                        }
                    }
                    CompactAccKind::MinInt => {
                        let col = &numeric_cols[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = col[row].0.value() as i64;
                            unsafe {
                                compact_storage.update_min_int(group_idx, spec_idx, v);
                            }
                        }
                    }
                    CompactAccKind::MaxInt => {
                        let col = &numeric_cols[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = col[row].0.value() as i64;
                            unsafe {
                                compact_storage.update_max_int(group_idx, spec_idx, v);
                            }
                        }
                    }
                    CompactAccKind::CountDistinctInt => {
                        let col = &numeric_cols[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            cd_sidecar.insert_int(spec_idx, group_idx, col[row].0.value() as i64);
                        }
                    }
                    CompactAccKind::CountDistinctStr => {
                        let col_idx = spec.col_idx as usize;
                        let Some(ref seg_col) = text_seg_cols[col_idx] else {
                            continue;
                        };
                        if let Some(remap) = config.dict_distinct_remaps.get(&spec_idx) {
                            // Phase D bitset path: per-row work is local
                            // dict_id → global_id → set bit. No hashing or
                            // string materialisation. Eligibility is gated
                            // at the leader pre-pass — this branch only
                            // fires when every segment is dict-encoded for
                            // `col_idx` and the column's global cardinality
                            // fit under PHASE_D_MAX_GLOBAL_FOR_BITSET.
                            if let Some(local_id) = seg_col.dict_local_id(row) {
                                let seg_remap = &remap.per_segment[seg_idx_in_all];
                                let global_id = seg_remap[local_id as usize];
                                cd_sidecar.insert_dict_global(spec_idx, group_idx, global_id);
                            }
                        } else if let Some(s) = seg_col.get_str(row) {
                            cd_sidecar.insert_str(spec_idx, group_idx, hash128_str(s.as_bytes()));
                        }
                    }
                }
            }
        }
    }

    // Write CountDistinct counts into compact storage for top-K sorting
    if !cd_sidecar.is_empty() {
        cd_sidecar.write_counts_to_storage(&mut compact_storage, &compact_map);
    }

    // Compute top-K candidates while data is cache-hot (if requested)
    let topk = config.topn_spec.map(|(sort_slot, k, ascending)| {
        let (_, sort_kind) = compact_storage.layout.slots[sort_slot];
        let read_val = |gidx: u32| -> i64 {
            unsafe {
                match sort_kind {
                    CompactAccKind::Count => compact_storage.read_count(gidx, sort_slot),
                    CompactAccKind::SumIntNarrow => {
                        compact_storage.read_sum_int_narrow(gidx, sort_slot).0
                    }
                    _ => compact_storage.read_count(gidx, sort_slot),
                }
            }
        };

        if compact_map.len() <= k {
            let keys: Vec<u128> = compact_map.keys().copied().collect();
            return (keys, 0i64);
        }

        if !ascending {
            let mut heap: BinaryHeap<Reverse<(i64, u128)>> = BinaryHeap::with_capacity(k + 1);
            for (&key, &gidx) in &compact_map {
                let val = read_val(gidx);
                heap.push(Reverse((val, key)));
                if heap.len() > k {
                    heap.pop();
                }
            }
            let floor = heap.peek().map(|&Reverse((v, _))| v).unwrap_or(0);
            let keys: Vec<u128> = heap.into_iter().map(|Reverse((_, k))| k).collect();
            (keys, floor)
        } else {
            let mut heap: BinaryHeap<(i64, u128)> = BinaryHeap::with_capacity(k + 1);
            for (&key, &gidx) in &compact_map {
                let val = read_val(gidx);
                heap.push((val, key));
                if heap.len() > k {
                    heap.pop();
                }
            }
            let floor = heap.peek().map(|&(v, _)| v).unwrap_or(0);
            let keys: Vec<u128> = heap.into_iter().map(|(_, k)| k).collect();
            (keys, floor)
        }
    });

    ParallelMixedResult {
        compact_map,
        compact_storage,
        mixed_keys,
        cd_sidecar,
        segments_processed,
        rows_processed,
        decompress_us,
        topk,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::extract::{
        date_trunc_unit_to_usecs, extract_field_from_usecs, extract_subday_from_bigint_scaled,
    };
    use super::regex::{has_posix_classes, pg_pattern_to_rust};
    use super::*;
    use pgrx::pg_sys;
    use pgrx::prelude::*;

    /// Helper: build a pg_sys::List of integers from a slice.
    unsafe fn build_int_list(values: &[i32]) -> *mut pg_sys::List {
        unsafe {
            let mut list: *mut pg_sys::List = std::ptr::null_mut();
            for &v in values {
                list = pg_sys::lappend_int(list, v);
            }
            list
        }
    }

    // -------------------------------------------------------------------
    // parse_agg_private tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_parse_single_count_star() {
        // Layout: [oid=1234, sentinel=-1, num_aggs=1, (type=2(CountStar), col=-1, result_oid=0, col_type=0, expr=0)]
        unsafe {
            let list = build_int_list(&[
                1234, -1, // companion OID + sentinel
                1,  // num_aggs
                2, -1, 0, 0,
                0, // CountStar: type=2, col=-1, result_oid=0, col_type=0, expr=Column(0)
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.companion_oids.len(), 1);
            assert_eq!(u32::from(plan.companion_oids[0]), 1234);
            assert_eq!(plan.agg_specs.len(), 1);
            assert_eq!(plan.agg_specs[0].agg_type, AggType::CountStar);
            assert_eq!(plan.agg_specs[0].col_idx, -1);
            assert_eq!(plan.agg_specs[0].expr_kind, AggExpr::Column);
            assert!(plan.group_specs.is_empty());
            assert!(plan.output_map.len() == 1); // default: aggs then groups
            assert!(plan.having_filters.is_empty());
            assert!(plan.where_quals.is_null());
            assert_eq!(plan.topn_limit, 0);
        }
    }

    #[pg_test]
    fn test_parse_multiple_companion_oids() {
        unsafe {
            let list = build_int_list(&[
                100, 200, 300, -1, // three companion OIDs + sentinel
                0,  // num_aggs = 0
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.companion_oids.len(), 3);
            assert_eq!(u32::from(plan.companion_oids[0]), 100);
            assert_eq!(u32::from(plan.companion_oids[1]), 200);
            assert_eq!(u32::from(plan.companion_oids[2]), 300);
        }
    }

    #[pg_test]
    fn test_parse_all_agg_types() {
        // Test that all 7 AggType variants parse correctly.
        // Each agg spec: (type, col_idx, result_oid, col_type_oid, expr_kind)
        unsafe {
            let list = build_int_list(&[
                42, -1, // companion OID + sentinel
                7,  // num_aggs
                0, 0, 0, 23, 0, // Sum, col=0, INT4OID=23
                1, 1, 0, 23, 0, // Count, col=1
                2, -1, 0, 0, 0, // CountStar
                3, 2, 0, 701, 0, // Avg, col=2, FLOAT8OID=701
                4, 3, 0, 25, 0, // CountDistinct, col=3, TEXTOID=25
                5, 0, 0, 23, 0, 0, // Min, col=0, OutputTransform=None
                6, 0, 0, 23, 0, 0, // Max, col=0, OutputTransform=None
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.agg_specs.len(), 7);
            assert_eq!(plan.agg_specs[0].agg_type, AggType::Sum);
            assert_eq!(plan.agg_specs[1].agg_type, AggType::Count);
            assert_eq!(plan.agg_specs[2].agg_type, AggType::CountStar);
            assert_eq!(plan.agg_specs[3].agg_type, AggType::Avg);
            assert_eq!(plan.agg_specs[4].agg_type, AggType::CountDistinct);
            assert_eq!(plan.agg_specs[5].agg_type, AggType::Min);
            assert_eq!(plan.agg_specs[6].agg_type, AggType::Max);
        }
    }

    #[pg_test]
    fn test_parse_expr_kinds() {
        // Test LengthOf (expr=1) and AddConst (expr=2, followed by offset)
        unsafe {
            let list = build_int_list(&[
                42, -1, // companion OID + sentinel
                3,  // num_aggs
                0, 0, 0, 23, 0, // Sum, Column (expr=0)
                0, 1, 0, 23, 1, // Sum, LengthOf (expr=1)
                0, 2, 0, 23, 2, 77, // Sum, AddConst (expr=2), offset=77
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.agg_specs[0].expr_kind, AggExpr::Column);
            assert_eq!(plan.agg_specs[0].const_offset, 0);
            assert_eq!(plan.agg_specs[1].expr_kind, AggExpr::LengthOf);
            assert_eq!(plan.agg_specs[2].expr_kind, AggExpr::AddConst);
            assert_eq!(plan.agg_specs[2].const_offset, 77);
        }
    }

    #[pg_test]
    fn test_parse_group_by_column() {
        // GROUP BY col: expr_tag=0
        unsafe {
            let list = build_int_list(&[
                42, -1, // companion OID + sentinel
                0,  // num_aggs = 0
                1,  // num_groups = 1
                3, 25, 0, // col_idx=3, type_oid=TEXTOID(25), expr_tag=0(Column)
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.group_specs.len(), 1);
            assert_eq!(plan.group_specs[0].col_idx, 3);
            assert_eq!(u32::from(plan.group_specs[0].type_oid), 25);
            assert_eq!(plan.group_specs[0].expr, GroupByExpr::Column);
        }
    }

    #[pg_test]
    fn test_parse_group_by_date_trunc() {
        // GROUP BY date_trunc('hour', ts): expr_tag=2, func_oid, unit_len, unit_bytes...
        unsafe {
            let unit = b"hour";
            let mut vals = vec![
                42i32,
                -1, // companion OID + sentinel
                0,  // num_aggs = 0
                1,  // num_groups = 1
                0,
                1184,
                2,   // col_idx=0, type_oid=TIMESTAMPTZOID(1184), expr_tag=2(DateTrunc)
                100, // func_oid
                unit.len() as i32, // unit_len
            ];
            for &b in unit.iter() {
                vals.push(b as i32);
            }
            let list = build_int_list(&vals);

            let plan = parse_agg_private(list);
            assert_eq!(plan.group_specs.len(), 1);
            match &plan.group_specs[0].expr {
                GroupByExpr::DateTrunc {
                    unit,
                    unit_usecs,
                    func_oid,
                } => {
                    assert_eq!(unit, "hour");
                    assert_eq!(*unit_usecs, 3_600_000_000);
                    assert_eq!(*func_oid, 100);
                }
                other => panic!("expected DateTrunc, got {:?}", other),
            }
        }
    }

    #[pg_test]
    fn test_parse_group_by_extract() {
        // GROUP BY extract(minute FROM ts): expr_tag=3, divisor=0
        unsafe {
            let unit = b"minute";
            let mut vals = vec![
                42i32,
                -1,
                0, // num_aggs
                1, // num_groups
                0,
                1184,
                3,   // col_idx=0, TIMESTAMPTZOID, expr_tag=3(Extract)
                200, // func_oid
                0,
                0, // divisor (i64 hi/lo): 0 = pg_usec input
                unit.len() as i32,
            ];
            for &b in unit.iter() {
                vals.push(b as i32);
            }
            let list = build_int_list(&vals);

            let plan = parse_agg_private(list);
            match &plan.group_specs[0].expr {
                GroupByExpr::Extract {
                    unit,
                    func_oid,
                    divisor,
                } => {
                    assert_eq!(unit, "minute");
                    assert_eq!(*func_oid, 200);
                    assert_eq!(*divisor, 0);
                }
                other => panic!("expected Extract, got {:?}", other),
            }
        }
    }

    #[pg_test]
    fn test_parse_group_by_extract_with_divisor() {
        // GROUP BY extract(hour FROM to_timestamp(bigint_col / 1_000_000)):
        // expr_tag=3 with non-zero divisor.
        unsafe {
            let unit = b"hour";
            let divisor: i64 = 1_000_000;
            let mut vals = vec![
                42i32,
                -1,
                0, // num_aggs
                1, // num_groups
                0,
                20,
                3,   // col_idx=0, INT8OID, expr_tag=3(Extract)
                200, // func_oid
                (divisor >> 32) as i32,
                divisor as i32,
                unit.len() as i32,
            ];
            for &b in unit.iter() {
                vals.push(b as i32);
            }
            let list = build_int_list(&vals);

            let plan = parse_agg_private(list);
            match &plan.group_specs[0].expr {
                GroupByExpr::Extract {
                    unit,
                    func_oid,
                    divisor,
                } => {
                    assert_eq!(unit, "hour");
                    assert_eq!(*func_oid, 200);
                    assert_eq!(*divisor, 1_000_000);
                }
                other => panic!("expected Extract, got {:?}", other),
            }
        }
    }

    #[test]
    fn test_extract_subday_from_bigint_scaled_hour() {
        // 2024-05-09 12:34:56 UTC = unix epoch seconds 1715258096
        // hour-of-day at that point is 12.
        let unix_us: i64 = 1_715_258_096_000_000;
        assert_eq!(
            extract_subday_from_bigint_scaled(unix_us, 1_000_000, "hour"),
            12,
        );
        // minute = 34, second = 56
        assert_eq!(
            extract_subday_from_bigint_scaled(unix_us, 1_000_000, "minute"),
            34,
        );
        assert_eq!(
            extract_subday_from_bigint_scaled(unix_us, 1_000_000, "second"),
            56,
        );
        // Pre-unix-epoch (negative) — div_euclid handles the sign correctly,
        // so hour-of-day is still positive within [0, 24).
        let pre_epoch: i64 = -1; // -1us before 1970 → 23:59:59 prev day
        assert_eq!(
            extract_subday_from_bigint_scaled(pre_epoch, 1_000_000, "hour"),
            23,
        );
    }

    #[test]
    fn test_constant_extract_key_for_segment_same_hour() {
        let cm = super::super::segments::ColMinMax {
            min_encoded: 1_715_258_096_000_000,
            max_encoded: 1_715_259_000_000_000,
            min_null: false,
            max_null: false,
            type_oid: pg_sys::INT8OID,
        };
        assert_eq!(
            constant_extract_key_for_segment(&cm, 1_000_000, "hour"),
            Some(12),
        );
    }

    #[test]
    fn test_constant_extract_key_for_segment_crossing_hour() {
        let cm = super::super::segments::ColMinMax {
            min_encoded: 1_715_259_599_000_000,
            max_encoded: 1_715_259_601_000_000,
            min_null: false,
            max_null: false,
            type_oid: pg_sys::INT8OID,
        };
        assert_eq!(
            constant_extract_key_for_segment(&cm, 1_000_000, "hour"),
            None,
        );
    }

    #[test]
    fn test_constant_extract_key_for_segment_same_minute() {
        let cm = super::super::segments::ColMinMax {
            min_encoded: 10 * 60_000_000 + 20_000_000,
            max_encoded: 10 * 60_000_000 + 40_000_000,
            min_null: false,
            max_null: false,
            type_oid: pg_sys::INT8OID,
        };
        assert_eq!(constant_extract_key_for_segment(&cm, 0, "minute"), Some(10),);
    }

    #[test]
    fn test_constant_extract_key_for_segment_same_minute_value_across_hour() {
        let cm = super::super::segments::ColMinMax {
            min_encoded: 27 * 60_000_000,
            max_encoded: 87 * 60_000_000,
            min_null: false,
            max_null: false,
            type_oid: pg_sys::INT8OID,
        };
        assert_eq!(constant_extract_key_for_segment(&cm, 0, "minute"), None,);
    }

    #[pg_test]
    fn test_parse_group_by_add_const() {
        // GROUP BY col + 5: expr_tag=4, offset, op_oid
        unsafe {
            let list = build_int_list(&[
                42, -1, 0, // num_aggs
                1, // num_groups
                2, 23, 4, // col_idx=2, INT4OID, expr_tag=4(AddConst)
                5, 551, // offset=5, op_oid=551
            ]);

            let plan = parse_agg_private(list);
            match &plan.group_specs[0].expr {
                GroupByExpr::AddConst { offset, op_oid } => {
                    assert_eq!(*offset, 5);
                    assert_eq!(*op_oid, 551);
                }
                other => panic!("expected AddConst, got {:?}", other),
            }
        }
    }

    #[pg_test]
    fn test_parse_group_by_regexp_replace() {
        // GROUP BY regexp_replace(col, 'pat', 'rep'): expr_tag=1
        unsafe {
            let pattern = b"abc";
            let replacement = b"xyz";
            let mut vals = vec![
                42i32,
                -1,
                0, // num_aggs
                1, // num_groups
                1,
                25,
                1, // col_idx=1, TEXTOID, expr_tag=1(RegexpReplace)
                300,
                100, // func_oid=300, collation=100
                pattern.len() as i32,
            ];
            for &b in pattern.iter() {
                vals.push(b as i32);
            }
            vals.push(replacement.len() as i32);
            for &b in replacement.iter() {
                vals.push(b as i32);
            }
            let list = build_int_list(&vals);

            let plan = parse_agg_private(list);
            match &plan.group_specs[0].expr {
                GroupByExpr::RegexpReplace {
                    pattern,
                    replacement,
                    func_oid,
                    collation,
                } => {
                    assert_eq!(pattern, "abc");
                    assert_eq!(replacement, "xyz");
                    assert_eq!(*func_oid, 300);
                    assert_eq!(*collation, 100);
                }
                other => panic!("expected RegexpReplace, got {:?}", other),
            }
        }
    }

    #[pg_test]
    fn test_parse_output_map() {
        // Explicit output map: [num_output=3, (type=1,ref=0), (type=0,ref=0), (type=0,ref=1)]
        // type=0 → Agg, type=1 → Group
        unsafe {
            let list = build_int_list(&[
                42, -1, // companion OID + sentinel
                2,  // num_aggs
                2, -1, 0, 0, 0, // CountStar
                0, 0, 0, 23, 0, // Sum col=0
                1, // num_groups
                0, 23, 0, // col_idx=0, INT4OID, Column
                3, // num_output = 3
                1, 0, // Group(0)
                0, 0, // Agg(0)
                0, 1, // Agg(1)
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.output_map.len(), 3);
            assert!(matches!(plan.output_map[0], OutputEntry::Group(0)));
            assert!(matches!(plan.output_map[1], OutputEntry::Agg(0)));
            assert!(matches!(plan.output_map[2], OutputEntry::Agg(1)));
        }
    }

    #[pg_test]
    fn test_parse_default_output_map() {
        // When no output map is specified, defaults to aggs then groups.
        unsafe {
            let list = build_int_list(&[
                42, -1, 1, // num_aggs
                2, -1, 0, 0, 0, // CountStar
                1, // num_groups
                0, 23, 0, // col_idx=0, INT4OID, Column
                0, // num_output = 0 (triggers default)
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.output_map.len(), 2);
            assert!(matches!(plan.output_map[0], OutputEntry::Agg(0)));
            assert!(matches!(plan.output_map[1], OutputEntry::Group(0)));
        }
    }

    #[pg_test]
    fn test_parse_having_filters() {
        unsafe {
            let list = build_int_list(&[
                42, -1, 1, // num_aggs
                2, -1, 0, 0, 0, // CountStar
                0, // num_groups
                1, // num_output
                0, 0, // Agg(0)
                2, // num_having = 2
                0, 0, 10, // agg_idx=0, op=Gt(0), const=10
                0, 4, 100, // agg_idx=0, op=Eq(4), const=100
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.having_filters.len(), 2);
            assert_eq!(plan.having_filters[0].agg_idx, 0);
            assert!(matches!(plan.having_filters[0].op, HavingOp::Gt));
            assert_eq!(plan.having_filters[0].const_val, 10);
            assert!(matches!(plan.having_filters[1].op, HavingOp::Eq));
            assert_eq!(plan.having_filters[1].const_val, 100);
        }
    }

    #[pg_test]
    fn test_parse_topn() {
        unsafe {
            let list = build_int_list(&[
                42, -1, 1, // num_aggs
                2, -1, 0, 0, 0, // CountStar
                0, // num_groups
                1, // num_output
                0, 0,  // Agg(0)
                0,  // num_having
                0,  // where_str_len = 0 (no WHERE)
                25, // topn_limit = 25
                2,  // topn_sort_col = 2
                0,  // topn_ascending = false
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.topn_limit, 25);
            assert_eq!(plan.topn_sort_col, 2);
            assert!(!plan.topn_ascending);
        }
    }

    #[pg_test]
    fn test_parse_all_having_ops() {
        // Verify all 6 HavingOp variants: Gt=0, Lt=1, Ge=2, Le=3, Eq=4, Ne=5
        unsafe {
            let list = build_int_list(&[
                42, -1, 1, 2, -1, 0, 0, 0, // 1 agg: CountStar
                0, // num_groups
                1, 0, 0, // num_output=1, Agg(0)
                6, // num_having = 6
                0, 0, 1, // Gt
                0, 1, 2, // Lt
                0, 2, 3, // Ge
                0, 3, 4, // Le
                0, 4, 5, // Eq
                0, 5, 6, // Ne
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.having_filters.len(), 6);
            assert!(matches!(plan.having_filters[0].op, HavingOp::Gt));
            assert!(matches!(plan.having_filters[1].op, HavingOp::Lt));
            assert!(matches!(plan.having_filters[2].op, HavingOp::Ge));
            assert!(matches!(plan.having_filters[3].op, HavingOp::Le));
            assert!(matches!(plan.having_filters[4].op, HavingOp::Eq));
            assert!(matches!(plan.having_filters[5].op, HavingOp::Ne));
        }
    }

    // -------------------------------------------------------------------
    // Shared test helpers
    // -------------------------------------------------------------------

    use super::super::segments::{ColMinMax, ColSum, MetadataInfo, SegmentData};

    fn make_meta(col_names: &[&str]) -> MetadataInfo {
        MetadataInfo {
            col_names: col_names.iter().map(|s| s.to_string()).collect(),
            col_types: col_names.iter().map(|_| pg_sys::Oid::from(23u32)).collect(),
            col_typmods: col_names.iter().map(|_| -1).collect(),
            col_not_null: col_names.iter().map(|_| false).collect(),
            segment_by: Vec::new(),
            order_by: Vec::new(),
            time_column: "ts".to_string(),
        }
    }

    fn make_plan(
        agg_specs: Vec<AggExecSpec>,
        group_specs: Vec<GroupByColSpec>,
        having: Vec<HavingFilter>,
        where_null: bool,
    ) -> ParsedAggPlan {
        let output_map: Vec<OutputEntry> = (0..agg_specs.len())
            .map(OutputEntry::Agg)
            .chain((0..group_specs.len()).map(OutputEntry::Group))
            .collect();
        ParsedAggPlan {
            companion_oids: vec![pg_sys::Oid::from(9999u32)],
            agg_specs,
            group_specs,
            output_map,
            having_filters: having,
            where_quals: if where_null {
                std::ptr::null_mut()
            } else {
                // Non-null placeholder (never dereferenced in rejection path)
                std::ptr::dangling_mut::<pg_sys::List>()
            },
            topn_limit: 0,
            topn_sort_col: 0,
            topn_ascending: true,
            derived_minmax_topn: None,
            bare_limit: 0,
            is_partial: false,
        }
    }

    fn make_agg_spec(agg_type: AggType, col_idx: i32, col_type_oid: u32) -> AggExecSpec {
        AggExecSpec {
            agg_type,
            col_idx,
            col_type_oid: pg_sys::Oid::from(col_type_oid),
            expr_kind: AggExpr::Column,
            const_offset: 0,
            is_partial: false,
            transtype_oid: pg_sys::InvalidOid,
            output_transform: OutputTransform::None,
        }
    }

    fn make_empty_segment(row_count: i32) -> SegmentData {
        SegmentData {
            companion_oid: pg_sys::InvalidOid,
            segment_id: 0,
            segment_values: Vec::new(),
            compressed_blobs: Vec::new(),
            text_length_blobs: Vec::new(),
            row_count,
            min_time: None,
            max_time: None,
            col_minmax: HashMap::new(),
            col_sums: HashMap::new(),
            toast_pointers: Vec::new(),
            cached_blob_pins: Vec::new(),
        }
    }

    // -------------------------------------------------------------------
    // try_catalog_shortcut tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_catalog_shortcut_rejects_group_by() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            vec![GroupByColSpec {
                col_idx: 0,
                type_oid: pg_sys::Oid::from(23u32),
                expr: GroupByExpr::Column,
            }],
            Vec::new(),
            true,
        );
        assert!(try_catalog_shortcut(&plan, &meta, &[], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_rejects_where() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            Vec::new(),
            false,
        );
        assert!(try_catalog_shortcut(&plan, &meta, &[], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_rejects_having() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            vec![HavingFilter {
                agg_idx: 0,
                op: HavingOp::Gt,
                const_val: 10,
            }],
            true,
        );
        assert!(try_catalog_shortcut(&plan, &meta, &[], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_rejects_sum() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        assert!(try_catalog_shortcut(&plan, &meta, &[Some(100)], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_rejects_avg() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Avg, 1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        assert!(try_catalog_shortcut(&plan, &meta, &[Some(100)], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_rejects_min_max() {
        let meta = make_meta(&["ts", "value"]);
        for agg_type in [AggType::Min, AggType::Max] {
            let plan = make_plan(
                vec![make_agg_spec(agg_type, 1, 23)],
                Vec::new(),
                Vec::new(),
                true,
            );
            assert!(try_catalog_shortcut(&plan, &meta, &[Some(100)], 0).is_none());
        }
    }

    #[pg_test]
    fn test_catalog_shortcut_count_star_single_partition() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let state = try_catalog_shortcut(&plan, &meta, &[Some(42_000)], 0).unwrap();
        assert_eq!(state.result_rows.len(), 1);
        assert_eq!(state.result_rows[0][0].0.value(), 42_000usize);
        assert!(!state.result_rows[0][0].1); // not null
    }

    #[pg_test]
    fn test_catalog_shortcut_count_star_multi_partition() {
        let meta = make_meta(&["ts", "value"]);
        let mut plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        plan.companion_oids = vec![
            pg_sys::Oid::from(1u32),
            pg_sys::Oid::from(2u32),
            pg_sys::Oid::from(3u32),
        ];
        let state =
            try_catalog_shortcut(&plan, &meta, &[Some(100), Some(200), Some(300)], 0).unwrap();
        assert_eq!(state.result_rows[0][0].0.value(), 600usize);
    }

    #[pg_test]
    fn test_catalog_shortcut_count_star_missing_row_count() {
        // If any partition's row count is None, the shortcut fails
        let meta = make_meta(&["ts", "value"]);
        let mut plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        plan.companion_oids = vec![pg_sys::Oid::from(1u32), pg_sys::Oid::from(2u32)];
        assert!(try_catalog_shortcut(&plan, &meta, &[Some(100), None], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_count_distinct_falls_through() {
        // CountDistinct is no longer a catalog shortcut — always falls through
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountDistinct, 1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        assert!(try_catalog_shortcut(&plan, &meta, &[Some(1000)], 0).is_none());
    }

    // -------------------------------------------------------------------
    // try_metadata_fast_path tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_metadata_fast_path_rejects_group_by() {
        let meta = make_meta(&["ts", "value"]);
        let mut plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        plan.group_specs = vec![GroupByColSpec {
            col_idx: 0,
            type_oid: pg_sys::Oid::from(23u32),
            expr: GroupByExpr::Column,
        }];
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_where() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 23)],
            Vec::new(),
            Vec::new(),
            false,
        );
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_having() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 23)],
            Vec::new(),
            vec![HavingFilter {
                agg_idx: 0,
                op: HavingOp::Gt,
                const_val: 5,
            }],
            true,
        );
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_count_distinct() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountDistinct, 1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_text_sum() {
        let meta = make_meta(&["ts", "name"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 25)],
            Vec::new(),
            Vec::new(),
            true,
        ); // TEXTOID=25
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_length_of_sum() {
        let meta = make_meta(&["ts", "name"]);
        let mut plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        plan.agg_specs[0].expr_kind = AggExpr::LengthOf;
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_count_star_empty() {
        // COUNT(*) with no segments → 0
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let state = try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).unwrap();
        assert_eq!(state.result_rows.len(), 1);
        assert_eq!(state.result_rows[0][0].0.value(), 0usize);
    }

    #[pg_test]
    fn test_metadata_fast_path_count_star() {
        // COUNT(*) sums row_count across segments
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let segs = vec![
            make_empty_segment(1000),
            make_empty_segment(2000),
            make_empty_segment(500),
        ];
        let state = try_metadata_fast_path(&plan, &meta, &segs, &[], 0, 0).unwrap();
        assert_eq!(state.result_rows[0][0].0.value(), 3500usize);
    }

    #[pg_test]
    fn test_metadata_fast_path_count_star_skips_zero_rows() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let segs = vec![
            make_empty_segment(100),
            make_empty_segment(0), // should be skipped
            make_empty_segment(50),
        ];
        let state = try_metadata_fast_path(&plan, &meta, &segs, &[], 0, 0).unwrap();
        assert_eq!(state.result_rows[0][0].0.value(), 150usize);
    }

    #[pg_test]
    fn test_metadata_fast_path_min_int() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Min, 1, 20)],
            Vec::new(),
            Vec::new(),
            true,
        ); // INT8OID=20
        let mut seg1 = make_empty_segment(100);
        seg1.col_minmax.insert(
            "value".to_string(),
            ColMinMax {
                min_encoded: 50i64,
                max_encoded: 200i64,
                min_null: false,
                max_null: false,
                type_oid: pg_sys::Oid::from(20u32),
            },
        );
        let mut seg2 = make_empty_segment(100);
        seg2.col_minmax.insert(
            "value".to_string(),
            ColMinMax {
                min_encoded: 10i64,
                max_encoded: 300i64,
                min_null: false,
                max_null: false,
                type_oid: pg_sys::Oid::from(20u32),
            },
        );
        let state = try_metadata_fast_path(&plan, &meta, &[seg1, seg2], &[], 0, 0).unwrap();
        let result = state.result_rows[0][0].0.value() as i64;
        assert_eq!(result, 10);
    }

    #[pg_test]
    fn test_metadata_fast_path_max_int() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Max, 1, 20)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let mut seg1 = make_empty_segment(100);
        seg1.col_minmax.insert(
            "value".to_string(),
            ColMinMax {
                min_encoded: 10i64,
                max_encoded: 200i64,
                min_null: false,
                max_null: false,
                type_oid: pg_sys::Oid::from(20u32),
            },
        );
        let mut seg2 = make_empty_segment(100);
        seg2.col_minmax.insert(
            "value".to_string(),
            ColMinMax {
                min_encoded: 5i64,
                max_encoded: 999i64,
                min_null: false,
                max_null: false,
                type_oid: pg_sys::Oid::from(20u32),
            },
        );
        let state = try_metadata_fast_path(&plan, &meta, &[seg1, seg2], &[], 0, 0).unwrap();
        let result = state.result_rows[0][0].0.value() as i64;
        assert_eq!(result, 999);
    }

    #[pg_test]
    fn test_metadata_fast_path_min_skips_null() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Min, 1, 20)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let mut seg1 = make_empty_segment(100);
        seg1.col_minmax.insert(
            "value".to_string(),
            ColMinMax {
                min_encoded: 0i64,
                max_encoded: 0i64,
                min_null: true, // all nulls in this segment
                max_null: true,
                type_oid: pg_sys::Oid::from(20u32),
            },
        );
        let mut seg2 = make_empty_segment(100);
        seg2.col_minmax.insert(
            "value".to_string(),
            ColMinMax {
                min_encoded: 77i64,
                max_encoded: 77i64,
                min_null: false,
                max_null: false,
                type_oid: pg_sys::Oid::from(20u32),
            },
        );
        let state = try_metadata_fast_path(&plan, &meta, &[seg1, seg2], &[], 0, 0).unwrap();
        let result = state.result_rows[0][0].0.value() as i64;
        assert_eq!(result, 77);
    }

    #[pg_test]
    fn test_metadata_fast_path_missing_minmax_metadata() {
        // If a segment doesn't have minmax metadata for the needed column, fall through
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Min, 1, 20)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let seg = make_empty_segment(100); // no col_minmax
        assert!(try_metadata_fast_path(&plan, &meta, &[seg], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_missing_sum_metadata() {
        // If a segment doesn't have sum metadata for a SUM column, fall through
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 20)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let seg = make_empty_segment(100); // no col_sums
        assert!(try_metadata_fast_path(&plan, &meta, &[seg], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_count_with_nonnull() {
        // COUNT(col) reads nonnull_count from ColSum
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Count, 1, 20)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let mut seg1 = make_empty_segment(1000);
        seg1.col_sums.insert(
            "value".to_string(),
            ColSum {
                sum_datum: pg_sys::Datum::from(0usize),
                sum_null: true,
                sum_i128: None,
                sum_f64: None,
                nonnull_count: 900,
                nonzero_count: -1,
                type_oid: pg_sys::Oid::from(1700u32),
            },
        );
        let mut seg2 = make_empty_segment(500);
        seg2.col_sums.insert(
            "value".to_string(),
            ColSum {
                sum_datum: pg_sys::Datum::from(0usize),
                sum_null: true,
                sum_i128: None,
                sum_f64: None,
                nonnull_count: 450,
                nonzero_count: -1,
                type_oid: pg_sys::Oid::from(1700u32),
            },
        );
        let state = try_metadata_fast_path(&plan, &meta, &[seg1, seg2], &[], 0, 0).unwrap();
        assert_eq!(state.result_rows[0][0].0.value() as i64, 1350);
    }

    #[pg_test]
    fn test_metadata_fast_path_sum_float() {
        // SUM on float column: reads sum_datum as f64 bits
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 701)], // FLOAT8OID=701
            Vec::new(),
            Vec::new(),
            true,
        );
        let sum_val: f64 = 123.5;
        let mut seg = make_empty_segment(100);
        seg.col_sums.insert(
            "value".to_string(),
            ColSum {
                sum_datum: pg_sys::Datum::from(sum_val.to_bits() as usize),
                sum_null: false,
                sum_i128: None,
                sum_f64: None,
                nonnull_count: 100,
                nonzero_count: -1,
                type_oid: pg_sys::Oid::from(701u32),
            },
        );
        let state = try_metadata_fast_path(&plan, &meta, &[seg], &[], 0, 0).unwrap();
        let result_bits = state.result_rows[0][0].0.value() as u64;
        let result = f64::from_bits(result_bits);
        assert!((result - 123.5).abs() < 1e-10);
    }

    #[pg_test]
    fn test_metadata_fast_path_sum_int_via_numeric() {
        // SUM on integer column: sum_datum is a NUMERIC, needs PG numeric_out to parse.
        // Build a real NUMERIC datum via SPI.
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 20)], // INT8OID=20
            Vec::new(),
            Vec::new(),
            true,
        );
        // Create a real NUMERIC datum for the value 42000 via numeric_in
        let numeric_datum: pg_sys::Datum = unsafe {
            let s = std::ffi::CString::new("42000").unwrap();
            pg_sys::OidFunctionCall3Coll(
                pg_sys::Oid::from(1701u32), // numeric_in
                pg_sys::InvalidOid,
                pg_sys::Datum::from(s.as_ptr()),
                pg_sys::Datum::from(0usize),
                pg_sys::Datum::from(-1i32 as usize),
            )
        };
        let mut seg = make_empty_segment(100);
        seg.col_sums.insert(
            "value".to_string(),
            ColSum {
                sum_datum: numeric_datum,
                sum_null: false,
                sum_i128: None,
                sum_f64: None,
                nonnull_count: 100,
                nonzero_count: -1,
                type_oid: pg_sys::Oid::from(1700u32),
            },
        );
        let state = try_metadata_fast_path(&plan, &meta, &[seg], &[], 0, 0).unwrap();
        // SumInt finalized: returns NUMERIC datum — verify via numeric_out
        let result_datum = state.result_rows[0][0].0;
        let s = unsafe {
            let cstr = pg_sys::OidOutputFunctionCall(pg_sys::Oid::from(1702u32), result_datum);
            let s = std::ffi::CStr::from_ptr(cstr)
                .to_string_lossy()
                .into_owned();
            pg_sys::pfree(cstr as *mut _);
            s
        };
        assert_eq!(s.as_str(), "42000");
    }

    #[pg_test]
    fn test_metadata_fast_path_sum_add_const() {
        // SUM(col + 10): should compute SUM(col) + 10 * nonnull_count
        let meta = make_meta(&["ts", "value"]);
        let mut spec = make_agg_spec(AggType::Sum, 1, 701); // FLOAT8OID
        spec.expr_kind = AggExpr::AddConst;
        spec.const_offset = 10;
        let plan = make_plan(vec![spec], Vec::new(), Vec::new(), true);
        let base_sum: f64 = 100.0;
        let mut seg = make_empty_segment(50);
        seg.col_sums.insert(
            "value".to_string(),
            ColSum {
                sum_datum: pg_sys::Datum::from(base_sum.to_bits() as usize),
                sum_null: false,
                sum_i128: None,
                sum_f64: None,
                nonnull_count: 50,
                nonzero_count: -1,
                type_oid: pg_sys::Oid::from(701u32),
            },
        );
        let state = try_metadata_fast_path(&plan, &meta, &[seg], &[], 0, 0).unwrap();
        let result = f64::from_bits(state.result_rows[0][0].0.value() as u64);
        // Expected: 100.0 + 10 * 50 = 600.0
        assert!((result - 600.0).abs() < 1e-10);
    }

    #[pg_test]
    fn test_metadata_fast_path_reports_segment_count() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            Vec::new(),
            Vec::new(),
            true,
        );
        let segs = vec![make_empty_segment(10), make_empty_segment(20)];
        let state = try_metadata_fast_path(&plan, &meta, &segs, &[], 123, 456).unwrap();
        assert_eq!(state.total_segments, 2);
        assert_eq!(state.metadata_us, 123);
        assert_eq!(state.heap_scan_us, 456);
    }

    // -------------------------------------------------------------------
    // date_trunc_unit_to_usecs tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_date_trunc_microsecond() {
        assert_eq!(date_trunc_unit_to_usecs("microsecond"), 1);
        assert_eq!(date_trunc_unit_to_usecs("microseconds"), 1);
        assert_eq!(date_trunc_unit_to_usecs("us"), 1);
    }

    #[pg_test]
    fn test_date_trunc_millisecond() {
        assert_eq!(date_trunc_unit_to_usecs("millisecond"), 1_000);
        assert_eq!(date_trunc_unit_to_usecs("milliseconds"), 1_000);
        assert_eq!(date_trunc_unit_to_usecs("ms"), 1_000);
    }

    #[pg_test]
    fn test_date_trunc_second() {
        assert_eq!(date_trunc_unit_to_usecs("second"), 1_000_000);
        assert_eq!(date_trunc_unit_to_usecs("seconds"), 1_000_000);
    }

    #[pg_test]
    fn test_date_trunc_minute() {
        assert_eq!(date_trunc_unit_to_usecs("minute"), 60_000_000);
        assert_eq!(date_trunc_unit_to_usecs("minutes"), 60_000_000);
    }

    #[pg_test]
    fn test_date_trunc_hour() {
        assert_eq!(date_trunc_unit_to_usecs("hour"), 3_600_000_000);
        assert_eq!(date_trunc_unit_to_usecs("hours"), 3_600_000_000);
    }

    #[pg_test]
    fn test_date_trunc_day() {
        assert_eq!(date_trunc_unit_to_usecs("day"), 86_400_000_000);
        assert_eq!(date_trunc_unit_to_usecs("days"), 86_400_000_000);
    }

    #[pg_test]
    fn test_date_trunc_unknown_fallback() {
        assert_eq!(date_trunc_unit_to_usecs("week"), 1);
        assert_eq!(date_trunc_unit_to_usecs(""), 1);
    }

    // -------------------------------------------------------------------
    // extract_field_from_usecs tests
    // -------------------------------------------------------------------

    // Helper: PG epoch usecs for 2000-01-01 12:34:56.789012
    // 12h=43200s, 34m=2040s, 56s → total 45296s → 45296_789012 usec
    const SAMPLE_USEC: i64 = 45_296_789_012;

    #[pg_test]
    fn test_extract_microsecond() {
        // PG EXTRACT(microsecond FROM ...) returns seconds_within_minute * 1_000_000 + frac_usec
        // 56 seconds + 789012 usec = 56_789_012
        assert_eq!(
            extract_field_from_usecs(SAMPLE_USEC, "microsecond"),
            56_789_012
        );
        assert_eq!(
            extract_field_from_usecs(SAMPLE_USEC, "microseconds"),
            56_789_012
        );
    }

    #[pg_test]
    fn test_extract_millisecond() {
        // PG EXTRACT(millisecond FROM ...) returns seconds_within_minute * 1_000 + frac_ms
        // 56 seconds + 789 ms = 56_789
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "millisecond"), 56_789);
        assert_eq!(
            extract_field_from_usecs(SAMPLE_USEC, "milliseconds"),
            56_789
        );
    }

    #[pg_test]
    fn test_extract_second() {
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "second"), 56);
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "seconds"), 56);
    }

    #[pg_test]
    fn test_extract_minute() {
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "minute"), 34);
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "minutes"), 34);
    }

    #[pg_test]
    fn test_extract_hour() {
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "hour"), 12);
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "hours"), 12);
    }

    #[pg_test]
    fn test_extract_dow() {
        // PG epoch 2000-01-01 is a Saturday (dow=6), day 0
        assert_eq!(extract_field_from_usecs(0, "dow"), 6); // Saturday
        // Day 1 = Sunday=0
        assert_eq!(extract_field_from_usecs(86_400_000_000, "dow"), 0);
        // Day 2 = Monday=1
        assert_eq!(extract_field_from_usecs(2 * 86_400_000_000, "dow"), 1);
    }

    #[pg_test]
    fn test_extract_epoch() {
        // PG epoch offset to Unix = 946684800 seconds
        assert_eq!(extract_field_from_usecs(0, "epoch"), 946_684_800);
        // 1 second after PG epoch
        assert_eq!(extract_field_from_usecs(1_000_000, "epoch"), 946_684_801);
    }

    #[pg_test]
    fn test_extract_negative_usec() {
        // Before PG epoch (negative usec): 1999-12-31 23:59:59.000000
        // -1 second = -1_000_000 usec
        let usec = -1_000_000i64;
        assert_eq!(extract_field_from_usecs(usec, "second"), 59);
        assert_eq!(extract_field_from_usecs(usec, "minute"), 59);
        assert_eq!(extract_field_from_usecs(usec, "hour"), 23);
        // Day before PG epoch (1999-12-31) is Friday=5
        assert_eq!(extract_field_from_usecs(usec, "dow"), 5);
    }

    #[pg_test]
    fn test_extract_unknown_fallback() {
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "year"), 0);
    }

    // -------------------------------------------------------------------
    // datum_to_i128 tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_datum_to_i128_int2() {
        let d = pg_sys::Datum::from(-5i16 as usize);
        assert_eq!(datum_to_i128(d, pg_sys::INT2OID), -5);
    }

    #[pg_test]
    fn test_datum_to_i128_int4() {
        let d = pg_sys::Datum::from(-100_000i32 as usize);
        assert_eq!(datum_to_i128(d, pg_sys::INT4OID), -100_000);
    }

    #[pg_test]
    fn test_datum_to_i128_int8() {
        let d = pg_sys::Datum::from(-9_000_000_000i64 as usize);
        assert_eq!(datum_to_i128(d, pg_sys::INT8OID), -9_000_000_000);
    }

    #[pg_test]
    fn test_datum_to_i128_unknown_oid() {
        // Falls through to raw usize cast
        let d = pg_sys::Datum::from(42usize);
        assert_eq!(datum_to_i128(d, pg_sys::Oid::from(9999u32)), 42);
    }

    // -------------------------------------------------------------------
    // datum_to_f64 tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_datum_to_f64_float4() {
        let f: f32 = 1.5;
        let d = pg_sys::Datum::from(f.to_bits() as usize);
        let result = datum_to_f64(d, pg_sys::FLOAT4OID);
        assert!((result - 1.5f64).abs() < 0.001);
    }

    #[pg_test]
    fn test_datum_to_f64_float8() {
        let f: f64 = 1.23456789;
        let d = pg_sys::Datum::from(f.to_bits() as usize);
        let result = datum_to_f64(d, pg_sys::FLOAT8OID);
        assert!((result - 1.23456789).abs() < 1e-9);
    }

    #[pg_test]
    fn test_datum_to_f64_unknown_oid() {
        let d = pg_sys::Datum::from(100usize);
        assert_eq!(datum_to_f64(d, pg_sys::Oid::from(9999u32)), 100.0);
    }

    // -------------------------------------------------------------------
    // AggAccumulator::new_for tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_accumulator_new_sum_int() {
        let acc = AggAccumulator::new_for(AggType::Sum, pg_sys::INT4OID);
        assert!(matches!(acc, AggAccumulator::SumInt { sum: 0, count: 0 }));
    }

    #[pg_test]
    fn test_accumulator_new_sum_float() {
        let acc = AggAccumulator::new_for(AggType::Sum, pg_sys::FLOAT8OID);
        assert!(matches!(acc, AggAccumulator::SumFloat { .. }));
    }

    #[pg_test]
    fn test_accumulator_new_avg_float4() {
        let acc = AggAccumulator::new_for(AggType::Avg, pg_sys::FLOAT4OID);
        assert!(matches!(acc, AggAccumulator::SumFloat { .. }));
    }

    #[pg_test]
    fn test_accumulator_new_avg_int() {
        let acc = AggAccumulator::new_for(AggType::Avg, pg_sys::INT8OID);
        assert!(matches!(acc, AggAccumulator::SumInt { .. }));
    }

    #[pg_test]
    fn test_accumulator_new_count() {
        let acc = AggAccumulator::new_for(AggType::Count, pg_sys::INT4OID);
        assert!(matches!(acc, AggAccumulator::Count { count: 0 }));
    }

    #[pg_test]
    fn test_accumulator_new_count_star() {
        let acc = AggAccumulator::new_for(AggType::CountStar, pg_sys::Oid::from(0u32));
        assert!(matches!(acc, AggAccumulator::Count { count: 0 }));
    }

    #[pg_test]
    fn test_accumulator_new_count_distinct_text() {
        let acc = AggAccumulator::new_for(AggType::CountDistinct, pg_sys::TEXTOID);
        assert!(matches!(acc, AggAccumulator::CountDistinctStr { .. }));
    }

    #[pg_test]
    fn test_accumulator_new_count_distinct_int() {
        let acc = AggAccumulator::new_for(AggType::CountDistinct, pg_sys::INT4OID);
        assert!(matches!(acc, AggAccumulator::CountDistinctInt { .. }));
    }

    #[pg_test]
    fn test_accumulator_new_min_text() {
        let acc = AggAccumulator::new_for(AggType::Min, pg_sys::TEXTOID);
        assert!(matches!(acc, AggAccumulator::MinStr { val: None }));
    }

    #[pg_test]
    fn test_accumulator_new_min_float() {
        let acc = AggAccumulator::new_for(AggType::Min, pg_sys::FLOAT8OID);
        assert!(matches!(acc, AggAccumulator::MinFloat { val: None }));
    }

    #[pg_test]
    fn test_accumulator_new_min_int() {
        let acc = AggAccumulator::new_for(AggType::Min, pg_sys::INT4OID);
        assert!(matches!(acc, AggAccumulator::MinInt { val: None }));
    }

    #[pg_test]
    fn test_accumulator_new_max_varchar() {
        let acc = AggAccumulator::new_for(AggType::Max, pg_sys::VARCHAROID);
        assert!(matches!(acc, AggAccumulator::MaxStr { val: None }));
    }

    #[pg_test]
    fn test_accumulator_new_max_float4() {
        let acc = AggAccumulator::new_for(AggType::Max, pg_sys::FLOAT4OID);
        assert!(matches!(acc, AggAccumulator::MaxFloat { val: None }));
    }

    #[pg_test]
    fn test_accumulator_new_max_int() {
        let acc = AggAccumulator::new_for(AggType::Max, pg_sys::INT8OID);
        assert!(matches!(acc, AggAccumulator::MaxInt { val: None }));
    }

    // -------------------------------------------------------------------
    // AggAccumulator::clone_fresh tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_clone_fresh_sum_int_resets() {
        let acc = AggAccumulator::SumInt {
            sum: 999,
            count: 50,
        };
        let fresh = acc.clone_fresh();
        assert!(matches!(fresh, AggAccumulator::SumInt { sum: 0, count: 0 }));
    }

    #[pg_test]
    fn test_clone_fresh_sum_float_resets() {
        let acc = AggAccumulator::SumFloat {
            sum: 1.5,
            count: 10,
        };
        let fresh = acc.clone_fresh();
        assert!(
            matches!(fresh, AggAccumulator::SumFloat { sum, count } if sum == 0.0 && count == 0)
        );
    }

    #[pg_test]
    fn test_clone_fresh_count_resets() {
        let acc = AggAccumulator::Count { count: 42 };
        let fresh = acc.clone_fresh();
        assert!(matches!(fresh, AggAccumulator::Count { count: 0 }));
    }

    #[pg_test]
    fn test_clone_fresh_min_int_resets() {
        let acc = AggAccumulator::MinInt { val: Some(7) };
        let fresh = acc.clone_fresh();
        assert!(matches!(fresh, AggAccumulator::MinInt { val: None }));
    }

    #[pg_test]
    fn test_clone_fresh_count_distinct_int_resets() {
        let mut seen = new_cd_set_int();
        seen.insert(1i64);
        seen.insert(2);
        let acc = AggAccumulator::CountDistinctInt { seen };
        let fresh = acc.clone_fresh();
        if let AggAccumulator::CountDistinctInt { seen } = fresh {
            assert!(seen.is_empty());
        } else {
            panic!("wrong variant");
        }
    }

    // -------------------------------------------------------------------
    // finalize_accumulator tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_finalize_count() {
        let acc = AggAccumulator::Count { count: 42 };
        let spec = make_agg_spec(AggType::Count, 0, 20);
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        assert_eq!(datum.value(), 42);
    }

    #[pg_test]
    fn test_finalize_count_distinct_int() {
        let mut seen = new_cd_set_int();
        seen.insert(10i64);
        seen.insert(20);
        seen.insert(30);
        let acc = AggAccumulator::CountDistinctInt { seen };
        let spec = make_agg_spec(AggType::CountDistinct, 0, 20);
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        assert_eq!(datum.value(), 3);
    }

    #[pg_test]
    fn test_finalize_count_distinct_str() {
        let mut seen = new_cd_set_str();
        seen.insert(0xdeadbeef_u128);
        seen.insert(0xcafebabe_u128);
        let acc = AggAccumulator::CountDistinctStr { seen };
        let spec = make_agg_spec(AggType::CountDistinct, 0, 25);
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        assert_eq!(datum.value(), 2);
    }

    #[pg_test]
    fn test_finalize_sum_int_zero_count_is_null() {
        let acc = AggAccumulator::SumInt { sum: 0, count: 0 };
        let spec = make_agg_spec(AggType::Sum, 0, 20);
        let (_, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(is_null);
    }

    #[pg_test]
    fn test_finalize_sum_int4_returns_int8() {
        // SUM(int4) → INT8 datum
        let acc = AggAccumulator::SumInt {
            sum: 100_000,
            count: 10,
        };
        let spec = make_agg_spec(AggType::Sum, 0, 23); // INT4OID
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        assert_eq!(datum.value() as i64, 100_000);
    }

    #[pg_test]
    fn test_finalize_sum_int8_returns_numeric() {
        // SUM(int8) → NUMERIC
        let acc = AggAccumulator::SumInt {
            sum: 999_999_999,
            count: 5,
        };
        let spec = make_agg_spec(AggType::Sum, 0, 20); // INT8OID
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        // Verify via numeric_out
        let s = unsafe {
            let cstr = pg_sys::OidOutputFunctionCall(pg_sys::Oid::from(1702u32), datum);
            let s = std::ffi::CStr::from_ptr(cstr)
                .to_string_lossy()
                .into_owned();
            pg_sys::pfree(cstr as *mut _);
            s
        };
        assert_eq!(s, "999999999");
    }

    #[pg_test]
    fn test_finalize_sum_float_zero_count_is_null() {
        let acc = AggAccumulator::SumFloat { sum: 0.0, count: 0 };
        let spec = make_agg_spec(AggType::Sum, 0, 701);
        let (_, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(is_null);
    }

    #[pg_test]
    fn test_finalize_sum_float8() {
        let acc = AggAccumulator::SumFloat { sum: 1.5, count: 1 };
        let spec = make_agg_spec(AggType::Sum, 0, 701); // FLOAT8OID
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        let result = f64::from_bits(datum.value() as u64);
        assert!((result - 1.5).abs() < 1e-10);
    }

    #[pg_test]
    fn test_finalize_sum_float4() {
        let acc = AggAccumulator::SumFloat { sum: 2.5, count: 1 };
        let spec = make_agg_spec(AggType::Sum, 0, 700); // FLOAT4OID
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        let result = f32::from_bits(datum.value() as u32);
        assert!((result - 2.5).abs() < 0.001);
    }

    #[pg_test]
    fn test_finalize_avg_int() {
        // AVG(int) → NUMERIC (sum/count via PG numeric_div)
        let acc = AggAccumulator::SumInt { sum: 100, count: 4 };
        let spec = make_agg_spec(AggType::Avg, 0, 23); // INT4OID
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        let s = unsafe {
            let cstr = pg_sys::OidOutputFunctionCall(pg_sys::Oid::from(1702u32), datum);
            let s = std::ffi::CStr::from_ptr(cstr)
                .to_string_lossy()
                .into_owned();
            pg_sys::pfree(cstr as *mut _);
            s
        };
        assert_eq!(s, "25.0000000000000000");
    }

    #[pg_test]
    fn test_finalize_avg_float() {
        // AVG(float8) → FLOAT8 (sum/count)
        let acc = AggAccumulator::SumFloat {
            sum: 10.0,
            count: 4,
        };
        let spec = make_agg_spec(AggType::Avg, 0, 701);
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        let result = f64::from_bits(datum.value() as u64);
        assert!((result - 2.5).abs() < 1e-10);
    }

    #[pg_test]
    fn test_finalize_min_int_some() {
        let acc = AggAccumulator::MinInt { val: Some(-42) };
        let spec = make_agg_spec(AggType::Min, 0, 20);
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        assert_eq!(datum.value() as i64, -42);
    }

    #[pg_test]
    fn test_finalize_min_int_none_is_null() {
        let acc = AggAccumulator::MinInt { val: None };
        let spec = make_agg_spec(AggType::Min, 0, 20);
        let (_, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(is_null);
    }

    #[pg_test]
    fn test_finalize_max_float_some() {
        let acc = AggAccumulator::MaxFloat { val: Some(99.9) };
        let spec = make_agg_spec(AggType::Max, 0, 701);
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        let result = f64::from_bits(datum.value() as u64);
        assert!((result - 99.9).abs() < 1e-10);
    }

    #[pg_test]
    fn test_finalize_max_float_none_is_null() {
        let acc = AggAccumulator::MaxFloat { val: None };
        let spec = make_agg_spec(AggType::Max, 0, 701);
        let (_, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(is_null);
    }

    // -------------------------------------------------------------------
    // StringArena tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_arena_alloc_and_get() {
        let mut arena = StringArena::new();
        let (off, len) = arena.alloc("hello");
        assert_eq!(arena.get(off, len), "hello");
    }

    #[pg_test]
    fn test_arena_multiple_allocs() {
        let mut arena = StringArena::new();
        let (o1, l1) = arena.alloc("foo");
        let (o2, l2) = arena.alloc("bar");
        let (o3, l3) = arena.alloc("baz");
        assert_eq!(arena.get(o1, l1), "foo");
        assert_eq!(arena.get(o2, l2), "bar");
        assert_eq!(arena.get(o3, l3), "baz");
    }

    #[pg_test]
    fn test_arena_empty_string() {
        let mut arena = StringArena::new();
        let (off, len) = arena.alloc("");
        assert_eq!(arena.get(off, len), "");
    }

    #[pg_test]
    fn test_arena_unicode() {
        let mut arena = StringArena::new();
        let (off, len) = arena.alloc("hello\u{00e9}world");
        assert_eq!(arena.get(off, len), "hello\u{00e9}world");
    }

    // -------------------------------------------------------------------
    // GroupKeyRef / GroupKeyVal / GroupKey tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_group_key_ref_resolve_null() {
        let mut arena = StringArena::new();
        let r = GroupKeyRef::Null;
        assert_eq!(r.resolve(&mut arena), GroupKeyVal::Null);
    }

    #[pg_test]
    fn test_group_key_ref_resolve_int() {
        let mut arena = StringArena::new();
        let r = GroupKeyRef::Int(42);
        assert_eq!(r.resolve(&mut arena), GroupKeyVal::Int(42));
    }

    #[pg_test]
    fn test_group_key_ref_resolve_str() {
        let mut arena = StringArena::new();
        let s = "hello";
        let r = GroupKeyRef::from_str(s);
        let val = r.resolve(&mut arena);
        if let GroupKeyVal::Str(off, len) = val {
            assert_eq!(arena.get(off, len), "hello");
        } else {
            panic!("expected Str variant");
        }
    }

    #[pg_test]
    fn test_group_key_ref_matches_owned_null() {
        let arena = StringArena::new();
        assert!(GroupKeyRef::Null.matches_owned(&GroupKeyVal::Null, &arena));
        assert!(!GroupKeyRef::Null.matches_owned(&GroupKeyVal::Int(0), &arena));
    }

    #[pg_test]
    fn test_group_key_ref_matches_owned_int() {
        let arena = StringArena::new();
        assert!(GroupKeyRef::Int(5).matches_owned(&GroupKeyVal::Int(5), &arena));
        assert!(!GroupKeyRef::Int(5).matches_owned(&GroupKeyVal::Int(6), &arena));
        assert!(!GroupKeyRef::Int(5).matches_owned(&GroupKeyVal::Null, &arena));
    }

    #[pg_test]
    fn test_group_key_ref_matches_owned_str() {
        let mut arena = StringArena::new();
        let (off, len) = arena.alloc("test");
        let s = "test";
        let r = GroupKeyRef::from_str(s);
        assert!(r.matches_owned(&GroupKeyVal::Str(off, len), &arena));
        let s2 = "other";
        let r2 = GroupKeyRef::from_str(s2);
        assert!(!r2.matches_owned(&GroupKeyVal::Str(off, len), &arena));
    }

    #[pg_test]
    fn test_group_key_single_as_slice() {
        let key = GroupKey::Single(GroupKeyVal::Int(7));
        assert_eq!(key.as_slice().len(), 1);
        assert_eq!(key.as_slice()[0], GroupKeyVal::Int(7));
    }

    #[pg_test]
    fn test_group_key_multi_as_slice() {
        let key = GroupKey::Multi(vec![GroupKeyVal::Int(1), GroupKeyVal::Null].into_boxed_slice());
        assert_eq!(key.as_slice().len(), 2);
        assert_eq!(key.as_slice()[0], GroupKeyVal::Int(1));
        assert_eq!(key.as_slice()[1], GroupKeyVal::Null);
    }

    // -------------------------------------------------------------------
    // hash_group_key / hash_group_key_ref / keys_match tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_hash_consistency_int() {
        // hash_group_key and hash_group_key_ref must produce the same hash
        // for equivalent keys
        let mut arena = StringArena::new();
        let owned = GroupKey::Single(GroupKeyVal::Int(42));
        let borrowed = [GroupKeyRef::Int(42)];
        let _ = &mut arena; // arena unused for int keys, but needed for API
        assert_eq!(
            hash_group_key(&owned, &arena),
            hash_group_key_ref(&borrowed)
        );
    }

    #[pg_test]
    fn test_hash_consistency_null() {
        let arena = StringArena::new();
        let owned = GroupKey::Single(GroupKeyVal::Null);
        let borrowed = [GroupKeyRef::Null];
        assert_eq!(
            hash_group_key(&owned, &arena),
            hash_group_key_ref(&borrowed)
        );
    }

    #[pg_test]
    fn test_hash_consistency_str() {
        let mut arena = StringArena::new();
        let (off, len) = arena.alloc("hello");
        let owned = GroupKey::Single(GroupKeyVal::Str(off, len));
        let s = "hello";
        let borrowed = [GroupKeyRef::from_str(s)];
        assert_eq!(
            hash_group_key(&owned, &arena),
            hash_group_key_ref(&borrowed)
        );
    }

    #[pg_test]
    fn test_hash_consistency_multi() {
        let mut arena = StringArena::new();
        let (off, len) = arena.alloc("world");
        let owned = GroupKey::Multi(
            vec![
                GroupKeyVal::Int(1),
                GroupKeyVal::Str(off, len),
                GroupKeyVal::Null,
            ]
            .into_boxed_slice(),
        );
        let s = "world";
        let borrowed = [
            GroupKeyRef::Int(1),
            GroupKeyRef::from_str(s),
            GroupKeyRef::Null,
        ];
        assert_eq!(
            hash_group_key(&owned, &arena),
            hash_group_key_ref(&borrowed)
        );
    }

    #[pg_test]
    fn test_hash_different_values_differ() {
        let arena = StringArena::new();
        let k1 = GroupKey::Single(GroupKeyVal::Int(1));
        let k2 = GroupKey::Single(GroupKeyVal::Int(2));
        assert_ne!(hash_group_key(&k1, &arena), hash_group_key(&k2, &arena));
    }

    #[pg_test]
    fn test_hash_different_types_differ() {
        // Int(0) vs Null should hash differently (different discriminant)
        let arena = StringArena::new();
        let k1 = GroupKey::Single(GroupKeyVal::Int(0));
        let k2 = GroupKey::Single(GroupKeyVal::Null);
        assert_ne!(hash_group_key(&k1, &arena), hash_group_key(&k2, &arena));
    }

    #[pg_test]
    fn test_keys_match_single_int() {
        let arena = StringArena::new();
        let owned = GroupKey::Single(GroupKeyVal::Int(42));
        let temp = [GroupKeyRef::Int(42)];
        assert!(keys_match(&owned, &temp, &arena));
    }

    #[pg_test]
    fn test_keys_match_single_mismatch() {
        let arena = StringArena::new();
        let owned = GroupKey::Single(GroupKeyVal::Int(42));
        let temp = [GroupKeyRef::Int(99)];
        assert!(!keys_match(&owned, &temp, &arena));
    }

    #[pg_test]
    fn test_keys_match_length_mismatch() {
        let arena = StringArena::new();
        let owned = GroupKey::Single(GroupKeyVal::Int(42));
        let temp = [GroupKeyRef::Int(42), GroupKeyRef::Null];
        assert!(!keys_match(&owned, &temp, &arena));
    }

    #[pg_test]
    fn test_keys_match_multi_str() {
        let mut arena = StringArena::new();
        let (o1, l1) = arena.alloc("abc");
        let (o2, l2) = arena.alloc("xyz");
        let owned = GroupKey::Multi(
            vec![GroupKeyVal::Str(o1, l1), GroupKeyVal::Str(o2, l2)].into_boxed_slice(),
        );
        let s1 = "abc";
        let s2 = "xyz";
        let temp = [GroupKeyRef::from_str(s1), GroupKeyRef::from_str(s2)];
        assert!(keys_match(&owned, &temp, &arena));
    }

    #[pg_test]
    fn test_keys_match_multi_str_mismatch() {
        let mut arena = StringArena::new();
        let (o1, l1) = arena.alloc("abc");
        let (o2, l2) = arena.alloc("xyz");
        let owned = GroupKey::Multi(
            vec![GroupKeyVal::Str(o1, l1), GroupKeyVal::Str(o2, l2)].into_boxed_slice(),
        );
        let s1 = "abc";
        let s2 = "DIFFERENT";
        let temp = [GroupKeyRef::from_str(s1), GroupKeyRef::from_str(s2)];
        assert!(!keys_match(&owned, &temp, &arena));
    }

    // -------------------------------------------------------------------
    // Rust regex helper tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_has_posix_classes_alpha() {
        assert!(has_posix_classes("[[:alpha:]]"));
    }

    #[pg_test]
    fn test_has_posix_classes_digit() {
        assert!(has_posix_classes("[[:digit:]]"));
    }

    #[pg_test]
    fn test_has_posix_classes_plain_range() {
        assert!(!has_posix_classes("[a-z]"));
    }

    #[pg_test]
    fn test_has_posix_classes_no_brackets() {
        assert!(!has_posix_classes("abc.*def"));
    }

    #[pg_test]
    fn test_convert_pg_replacement_capture_groups() {
        assert_eq!(convert_pg_replacement(r"\1"), "$1");
        assert_eq!(convert_pg_replacement(r"foo\1bar\2"), "foo$1bar$2");
    }

    #[pg_test]
    fn test_convert_pg_replacement_whole_match() {
        assert_eq!(convert_pg_replacement(r"\&"), "$0");
    }

    #[pg_test]
    fn test_convert_pg_replacement_literal_backslash() {
        assert_eq!(convert_pg_replacement(r"\\"), "\\");
    }

    #[pg_test]
    fn test_convert_pg_replacement_no_escapes() {
        assert_eq!(convert_pg_replacement("plain text"), "plain text");
    }

    #[pg_test]
    fn test_try_compile_safe_clickbench_pattern() {
        // The ClickBench Q29 pattern
        let re = try_compile_rust_regex(r"^https?://(?:www\.)?([^/]+)/.*");
        assert!(re.is_some());
    }

    #[pg_test]
    fn test_try_compile_posix_class_fallback() {
        let re = try_compile_rust_regex("[[:alpha:]]+");
        assert!(re.is_none());
    }

    #[pg_test]
    fn test_try_compile_backreference_fallback() {
        // Backreferences are not supported by Rust regex
        let re = try_compile_rust_regex(r"(abc)\1");
        assert!(re.is_none());
    }

    #[pg_test]
    fn test_try_compile_lookahead_fallback() {
        let re = try_compile_rust_regex(r"foo(?=bar)");
        assert!(re.is_none());
    }

    #[pg_test]
    fn test_clickbench_regex_replacement() {
        // Use try_compile_rust_regex which applies pg_pattern_to_rust internally
        let re = try_compile_rust_regex(r"^https?://(?:www\.)?([^/]+)/.*$").unwrap();
        let replacement = convert_pg_replacement(r"\1");
        assert_eq!(replacement, "$1");

        let url = "https://www.example.com/path/to/page";
        let result = re.replace(url, replacement.as_str());
        assert_eq!(result, "example.com");

        let url2 = "http://subdomain.test.org/index.html";
        let result2 = re.replace(url2, replacement.as_str());
        assert_eq!(result2, "subdomain.test.org");

        let url3 = "https://bare-domain.io/";
        let result3 = re.replace(url3, replacement.as_str());
        assert_eq!(result3, "bare-domain.io");

        // Trailing newline: PG's .* matches \n, so the whole string matches
        // and the domain is extracted. Our (?s) + \z conversion ensures same behavior.
        let url4 = "http://example.com/path\n";
        let result4 = re.replace(url4, replacement.as_str());
        assert_eq!(result4, "example.com"); // .* consumes \n, \z matches at end
    }

    #[pg_test]
    fn test_pg_pattern_to_rust_conversions() {
        // (?s) prefix for dot-all mode + $ → \z conversion
        assert_eq!(pg_pattern_to_rust("foo$"), "(?s)foo\\z");
        assert_eq!(pg_pattern_to_rust("foo\\$"), "(?s)foo\\$"); // escaped $ — no \z
        assert_eq!(pg_pattern_to_rust("foo\\\\$"), "(?s)foo\\\\\\z"); // \\$ → $ is unescaped
        assert_eq!(pg_pattern_to_rust("foo"), "(?s)foo"); // no $ — just (?s) prefix
    }

    #[pg_test]
    fn test_rust_regex_dot_matches_newline() {
        // PG's . matches \n by default; our (?s) prefix ensures Rust regex does too
        let re = try_compile_rust_regex("^http://([^/]+)/.*$").unwrap();
        let replacement = convert_pg_replacement(r"\1");
        // URL with embedded \n — PG's .* matches across it
        let url = "http://example.com/path\nmore";
        let result = re.replace(url, replacement.as_str());
        assert_eq!(result, "example.com");
        // URL with embedded \r\n
        let url2 = "http://example.com/path\r\nmore";
        let result2 = re.replace(url2, replacement.as_str());
        assert_eq!(result2, "example.com");
    }
}
