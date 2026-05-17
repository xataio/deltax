mod cd_set;
mod compact;
mod extract;
mod keys;
mod metadata;
mod parallel_cd;
mod parallel_compact;
mod parallel_mixed;
mod parser;
mod regex;
mod serial;
mod state;

// Re-export the compact accumulator items so sibling submodules can
// keep using `super::X` paths instead of `super::compact::X`.
pub(crate) use compact::{
    CompactAccKind, CompactAccLayout, CompactAccStorage, CountDistinctSideCar, DictDistinctRemap,
    build_dict_distinct_remaps, can_use_compact_accs, compact_emit_partial, compact_finalize,
    compact_topn_select, datum_to_f64, datum_to_i128, finalize_accumulator, i128_to_numeric_datum,
};

use self::regex::{RustRegexInfo, convert_pg_replacement, try_compile_rust_regex};
#[cfg(any(test, feature = "pg_test"))]
use cd_set::{new_cd_set_int, new_cd_set_str};
#[cfg(any(test, feature = "pg_test"))]
use extract::constant_extract_key_for_segment;
pub(crate) use keys::{CompactGroupMap, can_use_compact_keys_path};
use keys::{can_use_compact_keys, unpack_int_keys};
use metadata::{load_agg_metadata_from_plan, try_catalog_shortcut, try_metadata_fast_path};
use parallel_cd::{dispatch_parallel_count_distinct_path, parallel_count_distinct_eligible};
pub(crate) use parallel_compact::ParallelCompactResult;
use parallel_compact::{
    ParallelCompactConfig, dispatch_parallel_compact_path, merge_compact_results,
    parallel_compact_eligible, process_segments_compact,
};
use parallel_mixed::{can_parallel_mixed, dispatch_parallel_mixed_path};
use parser::{
    build_agg_exec_context_from_plan, build_deferred_agg_state, build_minimal_worker_state,
    parse_agg_private,
};
use serial::dispatch_serial_path;
#[cfg(any(test, feature = "pg_test"))]
use serial::{GroupKey, GroupKeyRef, GroupKeyVal, hash_group_key, hash_group_key_ref, keys_match};
pub(crate) use state::{
    AggExecSpec, AggExpr, AggScanState, AggType, CaseWhenClause, CaseWhenCondition, CaseWhenOp,
    CaseWhenSpec, CaseWhenValue, GroupByColSpec, GroupByExpr, HavingFilter, HavingOp,
    MAX_AGG_WORKER_SLOTS, OutputTransform,
};
use state::{AggTimingShmem, DeltaXAggPState, OutputEntry, PARTIAL_SLAB_SIZE_BYTES, ParsedAggPlan};
#[cfg(any(test, feature = "pg_test"))]
use state::AggAccumulator;
#[cfg(any(test, feature = "pg_test"))]
use std::collections::HashMap;

use pgrx::pg_guard;
use pgrx::pg_sys;

use std::sync::atomic::Ordering;
use std::time::Instant;

use super::super::SyncStatic;
use super::batch_qual::{BatchCompareOp, BatchQual, extract_batch_quals};
use super::segments::{SegmentData, extract_segment_filters, load_segments_heap};

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
