//! Parse `custom_private` lists produced by the planner into structured
//! Rust types, and build the `AggExecContext` / `AggScanState` pairs that
//! `begin_agg_scan` / `init_worker_deltax_agg` hand to the executor.
//!
//! The planner serialises the aggregate plan as a flat integer list because
//! PostgreSQL's custom-scan API only allows a `List*` through
//! `CustomScan.custom_private`. This module is the inverse of the
//! serialisation in `path.rs`.

use pgrx::pg_sys;

use super::super::batch_qual::extract_batch_quals;
use super::super::segments::{SegmentData, extract_segment_filters, load_segments_heap};
use super::extract::date_trunc_unit_to_usecs;
use super::metadata::load_agg_metadata_from_plan;
use super::state::{
    AggExecContext, AggExecSpec, AggExpr, AggScanState, AggType, CaseWhenClause, CaseWhenCondition,
    CaseWhenOp, CaseWhenSpec, CaseWhenValue, GroupByColSpec, GroupByExpr, HavingFilter, HavingOp,
    OutputEntry, OutputTransform, ParsedAggPlan,
};

/// Allocate a minimal `AggScanState` for parallel-worker processes that
/// short-circuit past the leader's heavy SPI + heap-scan + accumulator
/// construction. The worker won't emit rows (its `exec_agg_scan` returns
/// EOF immediately); Phase C.2 will replace this with an actual segment-
/// claim + partial-aggregate loop driving real `ParallelCompactResult`
/// state on the worker.
#[allow(dead_code)] // Phase C.1; activated by `begin_agg_scan` short-circuit.
pub(super) fn build_minimal_worker_state() -> AggScanState {
    AggScanState {
        where_quals_null: true,
        topn_sort_col: -1,
        topn_ascending: true,
        is_parallel_worker: true,
        ..AggScanState::default()
    }
}

/// Build an `AggExecContext` for the parallel-aware path. Used by both the
/// leader (in `begin_agg_scan`'s parallel branch) and workers (in
/// `init_worker_deltax_agg`). V1 workers re-SPI metadata + re-load segments;
/// V2 follow-up will share the leader's hydration via DSM (mirroring
/// `append_wire`).
///
/// SAFETY: Calls into `load_agg_metadata_from_plan` + `load_segments_heap`
/// which use SPI; caller must be inside a PG transaction (which is true for
/// any `BeginCustomScan` / `InitializeWorkerCustomScan` callback).
#[allow(dead_code)] // wired by C.2.d / C.2.e
pub(super) unsafe fn build_agg_exec_context_from_plan(plan: ParsedAggPlan) -> AggExecContext {
    unsafe {
        let (meta, _metadata_us) = load_agg_metadata_from_plan(&plan.companion_oids);

        let ParsedAggPlan {
            companion_oids,
            agg_specs,
            group_specs,
            output_map,
            having_filters: _,
            where_quals,
            topn_limit,
            topn_sort_col,
            topn_ascending,
            derived_minmax_topn: _,
            bare_limit: _,
            is_partial,
        } = plan;

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

        let (batch_quals, _handled) =
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

        let mut all_segments: Vec<SegmentData> = Vec::new();
        for &oid in &companion_oids {
            let (segs, _, _, _, _, _) = load_segments_heap(
                oid,
                &meta.col_names,
                &meta.segment_by,
                &needed_cols,
                &meta.time_column,
                /* load_minmax */ false,
                &seg_filters,
                time_min,
                time_max,
                /* lazy_cols */ None,
                &batch_quals,
                /* needed_stats_cols */ &[],
                &meta.col_types,
                /* needed_minmax_cols */ &[],
                /* skip_text */ false,
            );
            all_segments.extend(segs);
        }

        // Speculative top-K is excluded from the parallel-aware path's
        // eligibility predicate (Top-N pushdown is gated off in C.2.f), so
        // `topn_spec` remains `None` here. Plumbed through for forward-
        // compat once Top-N joins parallel.
        let topn_spec = if topn_limit > 0 && !output_map.is_empty() {
            match output_map[topn_sort_col] {
                OutputEntry::Agg(ai) if agg_specs[ai].agg_type != AggType::Avg => {
                    let k = (topn_limit as usize).max(1000);
                    Some((ai, k, topn_ascending))
                }
                _ => None,
            }
        } else {
            None
        };

        let num_result_cols = output_map.len();

        AggExecContext {
            meta,
            all_segments,
            agg_specs,
            group_specs,
            output_map,
            needed_cols,
            batch_quals,
            seg_filters,
            time_min,
            time_max,
            topn_spec,
            num_result_cols,
            merged: false,
            worker_done: false,
            is_partial,
        }
    }
}

/// Construct a deferred-exec `AggScanState` for the parallel-aware path:
/// empty `result_rows`, populated `exec_ctx`, all timers zero. Workers and
/// the leader use this as their initial state — the actual claim+merge work
/// runs in `exec_agg_scan`.
#[allow(dead_code)] // wired by C.2.d / C.2.e
pub(super) fn build_deferred_agg_state(ctx: AggExecContext, is_worker: bool) -> AggScanState {
    AggScanState {
        _num_result_cols: ctx.num_result_cols,
        batch_quals_count: ctx.batch_quals.len(),
        where_quals_null: ctx.batch_quals.is_empty() && ctx.seg_filters.is_empty(),
        topn_sort_col: -1,
        topn_ascending: true,
        is_parallel_worker: is_worker,
        exec_ctx: Some(Box::new(ctx)),
        ..AggScanState::default()
    }
}

/// Deserialize a CaseWhenValue from a custom_private integer list.
unsafe fn deserialize_case_when_value_inline(
    list: *mut pg_sys::List,
    idx: &mut i32,
) -> CaseWhenValue {
    unsafe {
        let tag = pg_sys::list_nth_int(list, *idx);
        *idx += 1;
        if tag == 0 {
            let col_idx = pg_sys::list_nth_int(list, *idx) as usize;
            *idx += 1;
            CaseWhenValue::ColumnRef(col_idx)
        } else {
            let str_len = pg_sys::list_nth_int(list, *idx) as usize;
            *idx += 1;
            let mut bytes = Vec::with_capacity(str_len);
            for _ in 0..str_len {
                bytes.push(pg_sys::list_nth_int(list, *idx) as u8);
                *idx += 1;
            }
            CaseWhenValue::StringConst(String::from_utf8_lossy(&bytes).into_owned())
        }
    }
}

/// Deserialize a DeltaXAgg custom_private list into structured Rust types.
///
/// The planner serializes the aggregate plan as a flat integer list with this layout:
///   [companion_oid, ..., -1 (sentinel),
///    num_aggs, (agg_type, col_idx, result_oid, col_type_oid, expr_kind [, extra])...,
///    num_groups, (col_idx, type_oid, expr_tag [, expr-specific fields])...,
///    num_output, (type, ref)...,
///    num_having, (agg_idx, op, const_val)...,
///    where_str_len, char0, char1, ...,
///    topn_limit [, sort_col, ascending]]
///
/// String values (regexp patterns, date_trunc units, WHERE clause) are encoded as
/// (length, byte0, byte1, ...) sequences within the integer list.
pub(super) unsafe fn parse_agg_private(custom_private: *mut pg_sys::List) -> ParsedAggPlan {
    unsafe {
        let list_len = (*custom_private).length;
        let mut companion_oids: Vec<pg_sys::Oid> = Vec::new();
        let mut agg_specs: Vec<AggExecSpec> = Vec::new();
        let mut group_specs: Vec<GroupByColSpec> = Vec::new();
        let mut output_map: Vec<OutputEntry> = Vec::new();

        let mut idx = 0;
        // Parse OIDs until sentinel
        while idx < list_len {
            let val = pg_sys::list_nth_int(custom_private, idx);
            idx += 1;
            if val == -1 {
                break;
            }
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
                let expr_kind_val = pg_sys::list_nth_int(custom_private, idx + 4);
                idx += 5;
                let agg_type = match agg_type_val {
                    0 => AggType::Sum,
                    1 => AggType::Count,
                    2 => AggType::CountStar,
                    3 => AggType::Avg,
                    4 => AggType::CountDistinct,
                    5 => AggType::Min,
                    6 => AggType::Max,
                    _ => AggType::Count,
                };
                let (expr_kind, const_offset) = match expr_kind_val {
                    1 => (AggExpr::LengthOf, 0i64),
                    2 => {
                        let offset = pg_sys::list_nth_int(custom_private, idx) as i64;
                        idx += 1;
                        (AggExpr::AddConst, offset)
                    }
                    _ => (AggExpr::Column, 0i64),
                };
                // H.2: OutputTransform trailer — only present for Min/Max
                // (keeps the wire format identical for Count/Sum/Avg/CountDistinct).
                // tag(0=None,1=PgUsShift); when tag==1, followed by lo+hi i32s
                // for the i64 delta.
                let output_transform = if matches!(agg_type, AggType::Min | AggType::Max)
                    && idx < list_len
                {
                    let tag = pg_sys::list_nth_int(custom_private, idx);
                    idx += 1;
                    match tag {
                        1 => {
                            let lo = pg_sys::list_nth_int(custom_private, idx) as u32 as u64;
                            let hi = pg_sys::list_nth_int(custom_private, idx + 1) as u32 as u64;
                            idx += 2;
                            let delta = ((hi << 32) | lo) as i64;
                            OutputTransform::PgUsShift { delta }
                        }
                        _ => OutputTransform::None,
                    }
                } else {
                    OutputTransform::None
                };
                let _ = result_oid; // parsed for offset, not stored
                agg_specs.push(AggExecSpec {
                    agg_type,
                    col_idx,
                    col_type_oid: pg_sys::Oid::from(col_type_oid),
                    expr_kind,
                    const_offset,
                    is_partial: false,
                    transtype_oid: pg_sys::InvalidOid,
                    output_transform,
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
                let expr_tag = pg_sys::list_nth_int(custom_private, idx + 2);
                idx += 3;
                let expr = if expr_tag == 1 {
                    // RegexpReplace: func_oid, collation, pattern_len, pattern_bytes..., replacement_len, replacement_bytes...
                    let func_oid = pg_sys::list_nth_int(custom_private, idx) as u32;
                    let collation = pg_sys::list_nth_int(custom_private, idx + 1) as u32;
                    idx += 2;
                    let pattern_len = pg_sys::list_nth_int(custom_private, idx) as usize;
                    idx += 1;
                    let mut pattern_bytes = Vec::with_capacity(pattern_len);
                    for _ in 0..pattern_len {
                        pattern_bytes.push(pg_sys::list_nth_int(custom_private, idx) as u8);
                        idx += 1;
                    }
                    let pattern = String::from_utf8_lossy(&pattern_bytes).into_owned();
                    let replacement_len = pg_sys::list_nth_int(custom_private, idx) as usize;
                    idx += 1;
                    let mut replacement_bytes = Vec::with_capacity(replacement_len);
                    for _ in 0..replacement_len {
                        replacement_bytes.push(pg_sys::list_nth_int(custom_private, idx) as u8);
                        idx += 1;
                    }
                    let replacement = String::from_utf8_lossy(&replacement_bytes).into_owned();
                    GroupByExpr::RegexpReplace {
                        pattern,
                        replacement,
                        func_oid,
                        collation,
                    }
                } else if expr_tag == 2 {
                    // DateTrunc: func_oid, unit_len, unit_bytes...
                    let func_oid = pg_sys::list_nth_int(custom_private, idx) as u32;
                    idx += 1;
                    let unit_len = pg_sys::list_nth_int(custom_private, idx) as usize;
                    idx += 1;
                    let mut unit_bytes = Vec::with_capacity(unit_len);
                    for _ in 0..unit_len {
                        unit_bytes.push(pg_sys::list_nth_int(custom_private, idx) as u8);
                        idx += 1;
                    }
                    let unit = String::from_utf8_lossy(&unit_bytes).into_owned();
                    let unit_usecs = date_trunc_unit_to_usecs(&unit);
                    GroupByExpr::DateTrunc {
                        unit,
                        unit_usecs,
                        func_oid,
                    }
                } else if expr_tag == 3 {
                    // Extract: func_oid, divisor_hi, divisor_lo, unit_len, unit_bytes...
                    let func_oid = pg_sys::list_nth_int(custom_private, idx) as u32;
                    idx += 1;
                    let div_hi = pg_sys::list_nth_int(custom_private, idx) as i64;
                    idx += 1;
                    let div_lo = pg_sys::list_nth_int(custom_private, idx) as u32 as i64;
                    idx += 1;
                    let divisor = (div_hi << 32) | div_lo;
                    let unit_len = pg_sys::list_nth_int(custom_private, idx) as usize;
                    idx += 1;
                    let mut unit_bytes = Vec::with_capacity(unit_len);
                    for _ in 0..unit_len {
                        unit_bytes.push(pg_sys::list_nth_int(custom_private, idx) as u8);
                        idx += 1;
                    }
                    let unit = String::from_utf8_lossy(&unit_bytes).into_owned();
                    GroupByExpr::Extract {
                        unit,
                        func_oid,
                        divisor,
                    }
                } else if expr_tag == 4 {
                    // AddConst: offset_i32, op_oid
                    let offset = pg_sys::list_nth_int(custom_private, idx) as i64;
                    let op_oid = pg_sys::list_nth_int(custom_private, idx + 1) as u32;
                    idx += 2;
                    GroupByExpr::AddConst { offset, op_oid }
                } else if expr_tag == 5 {
                    // CaseWhen
                    let num_clauses = pg_sys::list_nth_int(custom_private, idx) as usize;
                    idx += 1;
                    let mut clauses = Vec::with_capacity(num_clauses);
                    for _ in 0..num_clauses {
                        let num_conditions = pg_sys::list_nth_int(custom_private, idx) as usize;
                        idx += 1;
                        let mut conditions = Vec::with_capacity(num_conditions);
                        for _ in 0..num_conditions {
                            let cond_col_idx = pg_sys::list_nth_int(custom_private, idx) as usize;
                            let op_val = pg_sys::list_nth_int(custom_private, idx + 1);
                            let const_hi = pg_sys::list_nth_int(custom_private, idx + 2) as i64;
                            let const_lo =
                                pg_sys::list_nth_int(custom_private, idx + 3) as u32 as i64;
                            idx += 4;
                            let op = if op_val == 0 {
                                CaseWhenOp::Eq
                            } else {
                                CaseWhenOp::NotEq
                            };
                            let const_val = (const_hi << 32) | const_lo;
                            conditions.push(CaseWhenCondition {
                                col_idx: cond_col_idx,
                                op,
                                const_val,
                            });
                        }
                        let result = deserialize_case_when_value_inline(custom_private, &mut idx);
                        clauses.push(CaseWhenClause { conditions, result });
                    }
                    let default = deserialize_case_when_value_inline(custom_private, &mut idx);
                    GroupByExpr::CaseWhen(CaseWhenSpec { clauses, default })
                } else {
                    GroupByExpr::Column
                };
                group_specs.push(GroupByColSpec {
                    col_idx,
                    type_oid: pg_sys::Oid::from(type_oid),
                    expr,
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
                if otype == 0 {
                    output_map.push(OutputEntry::Agg(oref));
                } else if otype == 2 {
                    // Const output: type_oid, const_val_hi, const_val_lo, is_null
                    let type_oid = pg_sys::list_nth_int(custom_private, idx) as u32;
                    let val_hi = pg_sys::list_nth_int(custom_private, idx + 1) as i64;
                    let val_lo = pg_sys::list_nth_int(custom_private, idx + 2) as u32 as i64;
                    let is_null = pg_sys::list_nth_int(custom_private, idx + 3) != 0;
                    idx += 4;
                    let const_val = (val_hi << 32) | val_lo;
                    let datum = if is_null {
                        pg_sys::Datum::from(0usize)
                    } else {
                        // Reconstruct datum based on type
                        match pg_sys::Oid::from(type_oid) {
                            pg_sys::INT2OID => pg_sys::Datum::from(const_val as i16 as usize),
                            pg_sys::INT4OID => pg_sys::Datum::from(const_val as i32 as usize),
                            _ => pg_sys::Datum::from(const_val as usize),
                        }
                    };
                    output_map.push(OutputEntry::Const(datum, is_null));
                } else if otype == 3 {
                    // DerivedGroup: base_gi in oref, delta_hi, delta_lo
                    let delta_hi = pg_sys::list_nth_int(custom_private, idx) as i64;
                    let delta_lo = pg_sys::list_nth_int(custom_private, idx + 1) as u32 as i64;
                    idx += 2;
                    let delta = (delta_hi << 32) | delta_lo;
                    output_map.push(OutputEntry::DerivedGroup {
                        base_gi: oref,
                        delta,
                    });
                } else {
                    output_map.push(OutputEntry::Group(oref));
                }
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

        // Parse HAVING filters
        let mut having_filters: Vec<HavingFilter> = Vec::new();
        if idx < list_len {
            let num_having = pg_sys::list_nth_int(custom_private, idx) as usize;
            idx += 1;
            for _ in 0..num_having {
                let agg_idx = pg_sys::list_nth_int(custom_private, idx) as usize;
                let op_val = pg_sys::list_nth_int(custom_private, idx + 1);
                let const_val = pg_sys::list_nth_int(custom_private, idx + 2) as i64;
                idx += 3;
                let op = match op_val {
                    0 => HavingOp::Gt,
                    1 => HavingOp::Lt,
                    2 => HavingOp::Ge,
                    3 => HavingOp::Le,
                    4 => HavingOp::Eq,
                    5 => HavingOp::Ne,
                    _ => HavingOp::Gt,
                };
                having_filters.push(HavingFilter {
                    agg_idx,
                    op,
                    const_val,
                });
            }
        }
        // Read WHERE quals from custom_private (serialized as string by plan_agg_path).
        // Format: [str_len, char0, char1, ...] where str_len=0 means no quals.
        let where_quals: *mut pg_sys::List = if idx < list_len {
            let str_len = pg_sys::list_nth_int(custom_private, idx) as usize;
            idx += 1;
            if str_len > 0 {
                let mut chars: Vec<u8> = Vec::with_capacity(str_len + 1);
                for _ in 0..str_len {
                    chars.push(pg_sys::list_nth_int(custom_private, idx) as u8);
                    idx += 1;
                }
                chars.push(0); // null terminator
                pg_sys::stringToNode(chars.as_ptr() as *const std::ffi::c_char) as *mut pg_sys::List
            } else {
                std::ptr::null_mut()
            }
        } else {
            std::ptr::null_mut()
        };
        // Parse top-N info
        let mut topn_limit: i64 = 0;
        let mut topn_sort_col: usize = 0;
        let mut topn_ascending: bool = true;
        let mut bare_limit: i64 = 0;
        let mut derived_minmax_topn: Option<(usize, usize)> = None;
        if idx < list_len {
            let limit_val = pg_sys::list_nth_int(custom_private, idx);
            idx += 1;
            if limit_val > 0 {
                let sort_col_raw = pg_sys::list_nth_int(custom_private, idx);
                idx += 1;
                topn_ascending = pg_sys::list_nth_int(custom_private, idx) != 0;
                idx += 1;
                // -1 = bare LIMIT (no sort); -3 = derived MIN/MAX-difference
                // sort (Q4 shape) — two additional ints follow with the
                // (max_agg_idx, min_agg_idx) pair.
                if sort_col_raw == -3 {
                    let max_idx = pg_sys::list_nth_int(custom_private, idx) as usize;
                    idx += 1;
                    let min_idx = pg_sys::list_nth_int(custom_private, idx) as usize;
                    idx += 1;
                    topn_limit = limit_val as i64;
                    topn_sort_col = usize::MAX; // unused — see derived_minmax_topn
                    derived_minmax_topn = Some((max_idx, min_idx));
                } else if sort_col_raw < 0 {
                    bare_limit = limit_val as i64; // bare LIMIT, no sort
                } else {
                    topn_limit = limit_val as i64;
                    topn_sort_col = sort_col_raw as usize;
                }
            }
        }
        // Phase C.2 activation: trailing `is_partial` flag (1 byte). Older
        // plans / test fixtures may end without it — default false in that
        // case so existing paths stay compatible.
        let is_partial = if idx < list_len {
            let v = pg_sys::list_nth_int(custom_private, idx) != 0;
            idx += 1;
            v
        } else {
            false
        };
        let _ = idx;

        ParsedAggPlan {
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
            is_partial,
        }
    } // unsafe
}
