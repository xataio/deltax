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
                &meta.col_not_null,
                /* needed_minmax_cols */ &[],
                &meta.blob_idx,
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
///
/// # Safety
///
/// `list` must be a valid `*mut pg_sys::List`; `*idx` must be within
/// `list.length`. Calls `pg_sys::list_nth_int` (PG FFI).
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
///
/// # Safety
///
/// `custom_private` must be a valid `*mut pg_sys::List` produced by
/// the planner's `serialize_agg_private`; reads `(*custom_private).length`
/// and calls `pg_sys::list_nth_int` repeatedly. Must run inside an active
/// PG transaction (`BeginCustomScan` invariant).
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

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::super::test_utils::build_int_list;
    use super::{AggExpr, AggType, GroupByExpr, HavingOp, OutputEntry, parse_agg_private};
    use pgrx::prelude::*;

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
}
