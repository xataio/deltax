use pgrx::pg_sys;
use pgrx::pg_guard;

use super::cost;
use super::SyncStatic;

thread_local! {
    /// Temporary storage for HAVING filters during DeltaXAgg planning.
    /// Set in add_agg_path (hook), consumed in plan_agg_path.
    static AGG_HAVING_FILTERS: std::cell::RefCell<Vec<super::exec::HavingFilter>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Store HAVING filters for the next DeltaXAgg plan.
pub(super) fn set_agg_having_filters(filters: Vec<super::exec::HavingFilter>) {
    AGG_HAVING_FILTERS.with(|cell| *cell.borrow_mut() = filters);
}

/// Take (consume) the stored HAVING filters.
pub(super) fn take_agg_having_filters() -> Vec<super::exec::HavingFilter> {
    AGG_HAVING_FILTERS.with(|cell| std::mem::take(&mut *cell.borrow_mut()))
}

thread_local! {
    /// Top-N info for DeltaXAgg: (limit, sort_output_col, ascending).
    /// Set in hook (deltax_create_upper_paths), consumed in plan_agg_path.
    static AGG_TOPN_INFO: std::cell::RefCell<Option<(i64, i32, bool)>> =
        const { std::cell::RefCell::new(None) };
}

/// Store top-N info for the next DeltaXAgg plan.
pub(super) fn set_agg_topn_info(limit: i64, sort_col: i32, ascending: bool) {
    AGG_TOPN_INFO.with(|cell| *cell.borrow_mut() = Some((limit, sort_col, ascending)));
}

/// Clear any stale top-N info (e.g. from a previous query whose DeltaXAgg path was not chosen).
pub(super) fn clear_agg_topn_info() {
    AGG_TOPN_INFO.with(|cell| *cell.borrow_mut() = None);
}

/// Take (consume) the stored top-N info.
fn take_agg_topn_info() -> Option<(i64, i32, bool)> {
    AGG_TOPN_INFO.with(|cell| cell.borrow_mut().take())
}

// ============================================================================
// DeltaXAppend path/plan methods
// ============================================================================

/// Static CustomPathMethods for DeltaXAppend.
static DELTAX_APPEND_PATH_METHODS: SyncStatic<pg_sys::CustomPathMethods> =
    SyncStatic(pg_sys::CustomPathMethods {
        CustomName: super::DELTAX_APPEND_NAME.as_ptr(),
        PlanCustomPath: Some(plan_deltax_append_path),
        ReparameterizeCustomPathByChild: None,
    });

/// Static CustomScanMethods for DeltaXAppend.
static DELTAX_APPEND_SCAN_METHODS: SyncStatic<pg_sys::CustomScanMethods> =
    SyncStatic(pg_sys::CustomScanMethods {
        CustomName: super::DELTAX_APPEND_NAME.as_ptr(),
        CreateCustomScanState: Some(super::exec::create_deltax_append_state),
    });

/// Static CustomPathMethods struct.
static CUSTOM_PATH_METHODS: SyncStatic<pg_sys::CustomPathMethods> =
    SyncStatic(pg_sys::CustomPathMethods {
        CustomName: super::CUSTOM_NAME.as_ptr(),
        PlanCustomPath: Some(plan_custom_path),
        ReparameterizeCustomPathByChild: None,
    });

/// Static CustomScanMethods struct.
static CUSTOM_SCAN_METHODS: SyncStatic<pg_sys::CustomScanMethods> =
    SyncStatic(pg_sys::CustomScanMethods {
        CustomName: super::CUSTOM_NAME.as_ptr(),
        CreateCustomScanState: Some(super::exec::create_custom_scan_state),
    });

// Thread-local to pass Top-N info from add_decompress_path to plan_custom_path.
// Stored as (effective_limit, sort_ascending).
thread_local! {
    static TOPN_INFO: std::cell::Cell<(i64, bool)> = const { std::cell::Cell::new((0, true)) };
}

/// Add a DeltaXDecompress custom path to the relation's pathlist.
pub unsafe fn add_decompress_path(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    companion_oid: pg_sys::Oid,
    pathkeys: *mut pg_sys::List,
    effective_limit: i64,
    sort_ascending: bool,
) {
    unsafe {
        let cpath =
            pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;

        (*cpath).path.type_ = pg_sys::NodeTag::T_CustomPath;
        (*cpath).path.pathtype = pg_sys::NodeTag::T_CustomScan;
        (*cpath).path.parent = rel;
        (*cpath).path.pathtarget = (*rel).reltarget;

        let (startup_cost, total_cost, rows) = cost::estimate_cost(companion_oid);
        (*cpath).path.rows = rows;
        (*cpath).path.startup_cost = startup_cost;
        (*cpath).path.total_cost = total_cost;
        (*cpath).path.parallel_workers = 0;
        (*cpath).path.parallel_aware = false;
        (*cpath).path.parallel_safe = false;
        (*cpath).path.pathkeys = pathkeys;

        // Store companion OID in custom_private using lappend_oid
        (*cpath).custom_private =
            pg_sys::lappend_oid(std::ptr::null_mut(), companion_oid);

        (*cpath).custom_paths = std::ptr::null_mut();
        (*cpath).custom_restrictinfo = std::ptr::null_mut();
        (*cpath).methods = &CUSTOM_PATH_METHODS.0;

        // Store Top-N info for plan_custom_path.
        // Caller already validated that ORDER BY matches the time column.
        if effective_limit > 0 {
            TOPN_INFO.with(|cell| cell.set((effective_limit, sort_ascending)));
        } else {
            TOPN_INFO.with(|cell| cell.set((0, true)));
        }

        // Clear existing paths — the partition is truncated so any SeqScan
        // would return 0 rows.  We must replace it with the decompression path.
        (*rel).pathlist = std::ptr::null_mut();
        (*rel).partial_pathlist = std::ptr::null_mut();

        pg_sys::add_path(rel, cpath as *mut pg_sys::Path);
    }
}

/// PlanCustomPath callback: converts a CustomPath into a CustomScan plan node.
#[pg_guard]
pub unsafe extern "C-unwind" fn plan_custom_path(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    best_path: *mut pg_sys::CustomPath,
    tlist: *mut pg_sys::List,
    clauses: *mut pg_sys::List,
    _custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
    unsafe {
        let cscan =
            pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScan>()) as *mut pg_sys::CustomScan;

        (*cscan).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
        (*cscan).scan.plan.targetlist = tlist;
        (*cscan).scan.scanrelid = (*rel).relid;

        let final_clauses = pg_sys::extract_actual_clauses(clauses, false);
        (*cscan).scan.plan.qual = final_clauses;

        // Build custom_private: [companion_oid_as_int, -1 (sentinel), col0, col1, ...]
        let companion_oid = pg_sys::list_nth_oid((*best_path).custom_private, 0);
        let mut private_list =
            pg_sys::lappend_int(std::ptr::null_mut(), u32::from(companion_oid) as i32);

        // Extract needed column attribute numbers from tlist + quals
        let varno = (*rel).relid;
        let mut needed_attrs: *mut pg_sys::Bitmapset = std::ptr::null_mut();
        pg_sys::pull_varattnos(tlist as *mut pg_sys::Node, varno, &mut needed_attrs);
        pg_sys::pull_varattnos(
            final_clauses as *mut pg_sys::Node,
            varno,
            &mut needed_attrs,
        );

        // Append sentinel, then 0-based column indices
        private_list = pg_sys::lappend_int(private_list, -1);
        let offset = pg_sys::FirstLowInvalidHeapAttributeNumber;
        let mut x: i32 = -1;
        loop {
            x = pg_sys::bms_next_member(needed_attrs, x);
            if x < 0 {
                break;
            }
            let attno = x + offset;
            if attno > 0 {
                // Convert 1-based attno to 0-based column index
                private_list = pg_sys::lappend_int(private_list, attno - 1);
            }
        }

        // Append Top-N info: [-2, effective_limit, sort_ascending_flag]
        let (effective_limit, sort_ascending) = TOPN_INFO.with(|cell| cell.replace((0, true)));
        if effective_limit > 0 {
            private_list = pg_sys::lappend_int(private_list, -2);
            private_list = pg_sys::lappend_int(private_list, effective_limit as i32);
            private_list = pg_sys::lappend_int(private_list, if sort_ascending { 1 } else { 0 });
        }

        (*cscan).custom_private = private_list;
        (*cscan).custom_scan_tlist = std::ptr::null_mut();
        (*cscan).custom_plans = std::ptr::null_mut();
        (*cscan).custom_relids = std::ptr::null_mut();
        (*cscan).methods = &CUSTOM_SCAN_METHODS.0;
        (*cscan).flags = 0;

        &mut (*cscan).scan.plan as *mut pg_sys::Plan
    }
}

// ============================================================================
// DeltaXCount: COUNT(*) aggregate pushdown
// ============================================================================

/// Static CustomPathMethods for DeltaXCount.
static DELTAX_COUNT_PATH_METHODS: SyncStatic<pg_sys::CustomPathMethods> =
    SyncStatic(pg_sys::CustomPathMethods {
        CustomName: super::DELTAX_COUNT_NAME.as_ptr(),
        PlanCustomPath: Some(plan_count_star_path),
        ReparameterizeCustomPathByChild: None,
    });

/// Static CustomScanMethods for DeltaXCount.
static DELTAX_COUNT_SCAN_METHODS: SyncStatic<pg_sys::CustomScanMethods> =
    SyncStatic(pg_sys::CustomScanMethods {
        CustomName: super::DELTAX_COUNT_NAME.as_ptr(),
        CreateCustomScanState: Some(super::exec::create_count_scan_state),
    });

/// Add a DeltaXCount custom path to the grouped relation's pathlist.
///
/// This replaces the Aggregate → Scan pipeline with a single CustomScan
/// that returns the pre-computed row count from segment metadata.
pub unsafe fn add_count_star_path(
    _root: *mut pg_sys::PlannerInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    companion_oids: &[pg_sys::Oid],
) {
    unsafe {
        let cpath =
            pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;

        (*cpath).path.type_ = pg_sys::NodeTag::T_CustomPath;
        (*cpath).path.pathtype = pg_sys::NodeTag::T_CustomScan;
        (*cpath).path.parent = output_rel;
        (*cpath).path.pathtarget = (*output_rel).reltarget;

        // Very low cost — metadata-only scan, no decompression
        (*cpath).path.rows = 1.0;
        (*cpath).path.startup_cost = 1.0;
        (*cpath).path.total_cost = 2.0;
        (*cpath).path.parallel_workers = 0;
        (*cpath).path.parallel_aware = false;
        (*cpath).path.parallel_safe = false;

        // Store companion OIDs in custom_private
        let mut private_list: *mut pg_sys::List = std::ptr::null_mut();
        for &oid in companion_oids {
            private_list = pg_sys::lappend_oid(private_list, oid);
        }
        (*cpath).custom_private = private_list;

        (*cpath).custom_paths = std::ptr::null_mut();
        (*cpath).custom_restrictinfo = std::ptr::null_mut();
        (*cpath).methods = &DELTAX_COUNT_PATH_METHODS.0;

        pg_sys::add_path(output_rel, cpath as *mut pg_sys::Path);
    }
}

/// PlanCustomPath callback for DeltaXCount.
///
/// Creates a CustomScan with scanrelid=0 that outputs a single INT8 column
/// containing the pre-computed COUNT(*) result.
#[pg_guard]
pub unsafe extern "C-unwind" fn plan_count_star_path(
    _root: *mut pg_sys::PlannerInfo,
    _rel: *mut pg_sys::RelOptInfo,
    best_path: *mut pg_sys::CustomPath,
    _tlist: *mut pg_sys::List,
    _clauses: *mut pg_sys::List,
    _custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
    unsafe {
        let cscan =
            pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScan>()) as *mut pg_sys::CustomScan;

        (*cscan).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
        // scanrelid = 0: no real table scan, slot built from custom_scan_tlist
        (*cscan).scan.scanrelid = 0;

        // Build custom_scan_tlist: single TargetEntry with Const(0::int8)
        // This defines the scan output schema (one INT8 column)
        let const_node = pg_sys::makeConst(
            pg_sys::INT8OID,
            -1,                     // consttypmod
            pg_sys::InvalidOid,     // constcollid
            8,                      // constlen (sizeof int64)
            pg_sys::Datum::from(0usize),
            false,                  // constisnull
            true,                   // constbyval
        );
        let scan_tle = pg_sys::makeTargetEntry(
            const_node as *mut pg_sys::Expr,
            1,                      // resno
            std::ptr::null_mut(),   // resname
            false,                  // resjunk
        );
        (*cscan).custom_scan_tlist = pg_sys::lappend(std::ptr::null_mut(), scan_tle as *mut _);

        // Build plan.targetlist: same Const(0::int8) expression.
        // PG's setrefs (fix_upper_expr) will find this matching expression
        // in custom_scan_tlist and replace it with Var(INDEX_VAR, 1, INT8OID).
        let const_node2 = pg_sys::makeConst(
            pg_sys::INT8OID,
            -1,
            pg_sys::InvalidOid,
            8,
            pg_sys::Datum::from(0usize),
            false,
            true,
        );
        let plan_tle = pg_sys::makeTargetEntry(
            const_node2 as *mut pg_sys::Expr,
            1,                      // resno
            std::ptr::null_mut(),   // resname
            false,                  // resjunk
        );
        (*cscan).scan.plan.targetlist = pg_sys::lappend(std::ptr::null_mut(), plan_tle as *mut _);

        // Build custom_private: [oid1, oid2, ..., -1 (sentinel)]
        let oid_list = (*best_path).custom_private;
        let num_oids = (*oid_list).length;
        let mut private_list: *mut pg_sys::List = std::ptr::null_mut();
        for i in 0..num_oids {
            let oid = pg_sys::list_nth_oid(oid_list, i);
            private_list = pg_sys::lappend_int(private_list, u32::from(oid) as i32);
        }
        private_list = pg_sys::lappend_int(private_list, -1);

        (*cscan).custom_private = private_list;
        (*cscan).custom_plans = std::ptr::null_mut();
        (*cscan).custom_relids = std::ptr::null_mut();
        (*cscan).methods = &DELTAX_COUNT_SCAN_METHODS.0;
        (*cscan).flags = 0;
        (*cscan).scan.plan.qual = std::ptr::null_mut();

        &mut (*cscan).scan.plan as *mut pg_sys::Plan
    }
}

// ============================================================================
// DeltaXMinMax: MIN/MAX aggregate pushdown on time column
// ============================================================================

/// Static CustomPathMethods for DeltaXMinMax.
static DELTAX_MINMAX_PATH_METHODS: SyncStatic<pg_sys::CustomPathMethods> =
    SyncStatic(pg_sys::CustomPathMethods {
        CustomName: super::DELTAX_MINMAX_NAME.as_ptr(),
        PlanCustomPath: Some(plan_minmax_path),
        ReparameterizeCustomPathByChild: None,
    });

/// Static CustomScanMethods for DeltaXMinMax.
static DELTAX_MINMAX_SCAN_METHODS: SyncStatic<pg_sys::CustomScanMethods> =
    SyncStatic(pg_sys::CustomScanMethods {
        CustomName: super::DELTAX_MINMAX_NAME.as_ptr(),
        CreateCustomScanState: Some(super::exec::create_minmax_scan_state),
    });

/// Specification for one MIN/MAX aggregate in a multi-aggregate pushdown.
pub struct MinMaxAggSpec {
    pub is_min: bool,
    pub varattno: i16,
    pub result_type_oid: pg_sys::Oid,
    pub typlen: i16,
    pub typbyval: bool,
}

/// Add a DeltaXMinMax custom path to the grouped relation's pathlist.
///
/// This replaces the Aggregate → Scan pipeline with a single CustomScan
/// that returns the pre-computed MIN/MAX values from segment metadata.
/// Supports multiple aggregates (e.g., `SELECT MIN(col), MAX(col)`).
pub unsafe fn add_minmax_path(
    _root: *mut pg_sys::PlannerInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    companion_oids: &[pg_sys::Oid],
    agg_specs: &[MinMaxAggSpec],
) {
    unsafe {
        let cpath =
            pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;

        (*cpath).path.type_ = pg_sys::NodeTag::T_CustomPath;
        (*cpath).path.pathtype = pg_sys::NodeTag::T_CustomScan;
        (*cpath).path.parent = output_rel;
        (*cpath).path.pathtarget = (*output_rel).reltarget;

        // Very low cost — metadata-only scan, no decompression
        (*cpath).path.rows = 1.0;
        (*cpath).path.startup_cost = 1.0;
        (*cpath).path.total_cost = 2.0;
        (*cpath).path.parallel_workers = 0;
        (*cpath).path.parallel_aware = false;
        (*cpath).path.parallel_safe = false;

        // Store in custom_private:
        // [oid1, oid2, ..., -1, num_aggs,
        //  is_min_0, varattno_0, type_oid_0, typlen_0, typbyval_0,
        //  is_min_1, varattno_1, type_oid_1, typlen_1, typbyval_1, ...]
        let mut private_list: *mut pg_sys::List = std::ptr::null_mut();
        for &oid in companion_oids {
            private_list = pg_sys::lappend_int(private_list, u32::from(oid) as i32);
        }
        private_list = pg_sys::lappend_int(private_list, -1);
        private_list = pg_sys::lappend_int(private_list, agg_specs.len() as i32);
        for spec in agg_specs {
            private_list = pg_sys::lappend_int(private_list, if spec.is_min { 1 } else { 0 });
            private_list = pg_sys::lappend_int(private_list, spec.varattno as i32);
            private_list = pg_sys::lappend_int(private_list, u32::from(spec.result_type_oid) as i32);
            private_list = pg_sys::lappend_int(private_list, spec.typlen as i32);
            private_list = pg_sys::lappend_int(private_list, if spec.typbyval { 1 } else { 0 });
        }
        (*cpath).custom_private = private_list;

        (*cpath).custom_paths = std::ptr::null_mut();
        (*cpath).custom_restrictinfo = std::ptr::null_mut();
        (*cpath).methods = &DELTAX_MINMAX_PATH_METHODS.0;

        pg_sys::add_path(output_rel, cpath as *mut pg_sys::Path);
    }
}

/// Per-aggregate info parsed from custom_private during plan creation.
struct PlanAggSpec {
    is_min: bool,
    varattno: i32,
    type_oid: pg_sys::Oid,
    typlen: i32,
    typbyval: bool,
}

/// PlanCustomPath callback for DeltaXMinMax.
///
/// Creates a CustomScan with scanrelid=0 that outputs N columns,
/// one per MIN/MAX aggregate, containing the pre-computed results.
#[pg_guard]
pub unsafe extern "C-unwind" fn plan_minmax_path(
    _root: *mut pg_sys::PlannerInfo,
    _rel: *mut pg_sys::RelOptInfo,
    best_path: *mut pg_sys::CustomPath,
    _tlist: *mut pg_sys::List,
    _clauses: *mut pg_sys::List,
    _custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
    unsafe {
        let cscan =
            pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScan>()) as *mut pg_sys::CustomScan;

        (*cscan).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
        // scanrelid = 0: no real table scan, slot built from custom_scan_tlist
        (*cscan).scan.scanrelid = 0;

        // Parse path's custom_private:
        // [oid1, ..., -1, num_aggs, is_min_0, varattno_0, type_oid_0, typlen_0, typbyval_0, ...]
        let path_private = (*best_path).custom_private;
        let path_len = (*path_private).length;

        let mut companion_oids: Vec<pg_sys::Oid> = Vec::new();
        let mut agg_specs: Vec<PlanAggSpec> = Vec::new();
        let mut found_sentinel = false;
        let mut num_aggs: i32 = 0;
        let mut after_sentinel_idx = 0;
        let mut current_spec_fields: Vec<i32> = Vec::new();

        for i in 0..path_len {
            let val = pg_sys::list_nth_int(path_private, i);
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
                current_spec_fields.push(val);
                if current_spec_fields.len() == 5 {
                    agg_specs.push(PlanAggSpec {
                        is_min: current_spec_fields[0] != 0,
                        varattno: current_spec_fields[1],
                        type_oid: pg_sys::Oid::from(current_spec_fields[2] as u32),
                        typlen: current_spec_fields[3],
                        typbyval: current_spec_fields[4] != 0,
                    });
                    current_spec_fields.clear();
                }
                after_sentinel_idx += 1;
            }
        }
        let _ = num_aggs; // validated by agg_specs.len()

        // Build custom_scan_tlist and plan.targetlist: one entry per aggregate
        let mut scan_tlist: *mut pg_sys::List = std::ptr::null_mut();
        let mut plan_tlist: *mut pg_sys::List = std::ptr::null_mut();

        for (idx, spec) in agg_specs.iter().enumerate() {
            let resno = (idx + 1) as i16;

            // custom_scan_tlist entry
            let const_node = pg_sys::makeConst(
                spec.type_oid,
                -1,                     // consttypmod
                pg_sys::InvalidOid,     // constcollid
                spec.typlen,            // constlen
                pg_sys::Datum::from(0usize),
                true,                   // constisnull (placeholder)
                spec.typbyval,          // constbyval
            );
            let scan_tle = pg_sys::makeTargetEntry(
                const_node as *mut pg_sys::Expr,
                resno,
                std::ptr::null_mut(),   // resname
                false,                  // resjunk
            );
            scan_tlist = pg_sys::lappend(scan_tlist, scan_tle as *mut _);

            // plan.targetlist entry (PG setrefs will match to custom_scan_tlist)
            let const_node2 = pg_sys::makeConst(
                spec.type_oid,
                -1,
                pg_sys::InvalidOid,
                spec.typlen,
                pg_sys::Datum::from(0usize),
                true,
                spec.typbyval,
            );
            let plan_tle = pg_sys::makeTargetEntry(
                const_node2 as *mut pg_sys::Expr,
                resno,
                std::ptr::null_mut(),
                false,
            );
            plan_tlist = pg_sys::lappend(plan_tlist, plan_tle as *mut _);
        }

        (*cscan).custom_scan_tlist = scan_tlist;
        (*cscan).scan.plan.targetlist = plan_tlist;

        // Build plan's custom_private: [oid1, ..., -1, num_aggs, is_min_0, varattno_0, ...]
        let mut plan_private: *mut pg_sys::List = std::ptr::null_mut();
        for &oid in &companion_oids {
            plan_private = pg_sys::lappend_int(plan_private, u32::from(oid) as i32);
        }
        plan_private = pg_sys::lappend_int(plan_private, -1);
        plan_private = pg_sys::lappend_int(plan_private, agg_specs.len() as i32);
        for spec in &agg_specs {
            plan_private = pg_sys::lappend_int(plan_private, if spec.is_min { 1 } else { 0 });
            plan_private = pg_sys::lappend_int(plan_private, spec.varattno);
        }

        (*cscan).custom_private = plan_private;
        (*cscan).custom_plans = std::ptr::null_mut();
        (*cscan).custom_relids = std::ptr::null_mut();
        (*cscan).methods = &DELTAX_MINMAX_SCAN_METHODS.0;
        (*cscan).flags = 0;
        (*cscan).scan.plan.qual = std::ptr::null_mut();

        &mut (*cscan).scan.plan as *mut pg_sys::Plan
    }
}

// ============================================================================
// DeltaXAgg: aggregate pushdown (SUM, AVG, COUNT, COUNT(DISTINCT), GROUP BY)
// ============================================================================

/// Static CustomPathMethods for DeltaXAgg.
static DELTAX_AGG_PATH_METHODS: SyncStatic<pg_sys::CustomPathMethods> =
    SyncStatic(pg_sys::CustomPathMethods {
        CustomName: super::DELTAX_AGG_NAME.as_ptr(),
        PlanCustomPath: Some(plan_agg_path),
        ReparameterizeCustomPathByChild: None,
    });

/// Static CustomScanMethods for DeltaXAgg.
static DELTAX_AGG_SCAN_METHODS: SyncStatic<pg_sys::CustomScanMethods> =
    SyncStatic(pg_sys::CustomScanMethods {
        CustomName: super::DELTAX_AGG_NAME.as_ptr(),
        CreateCustomScanState: Some(super::exec::create_agg_scan_state),
    });

/// Specification for one aggregate in a DeltaXAgg pushdown.
pub struct AggSpec {
    pub agg_type: super::exec::AggType,
    pub col_idx: i32,               // 0-based column index, -1 for COUNT(*)
    pub result_type_oid: pg_sys::Oid,
    pub col_type_oid: pg_sys::Oid,  // source column type OID
    pub expr_kind: super::exec::AggExpr,  // Column, LengthOf, or AddConst
    pub const_offset: i64,          // Only used when expr_kind == AddConst
}

/// Add a DeltaXAgg custom path to the grouped relation's pathlist.
#[allow(clippy::too_many_arguments)]
pub unsafe fn add_agg_path(
    _root: *mut pg_sys::PlannerInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    companion_oids: &[pg_sys::Oid],
    agg_specs: &[AggSpec],
    group_specs: &[super::exec::GroupByColSpec],
    having_filters: &[super::exec::HavingFilter],
    pg_estimated_groups: f64,
    pathkeys: *mut pg_sys::List,
) {
    unsafe {
        let cpath =
            pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;

        (*cpath).path.type_ = pg_sys::NodeTag::T_CustomPath;
        (*cpath).path.pathtype = pg_sys::NodeTag::T_CustomScan;
        (*cpath).path.parent = output_rel;
        (*cpath).path.pathtarget = (*output_rel).reltarget;

        // Use PG's group count estimate for row estimate
        let estimated_rows = if group_specs.is_empty() {
            1.0
        } else if pg_estimated_groups > 0.0 {
            pg_estimated_groups
        } else {
            100.0
        };
        (*cpath).path.rows = estimated_rows;
        (*cpath).path.startup_cost = 10.0;
        (*cpath).path.total_cost = 20.0;
        (*cpath).path.parallel_workers = 0;
        (*cpath).path.parallel_aware = false;
        (*cpath).path.parallel_safe = false;
        (*cpath).path.pathkeys = if pathkeys.is_null() { std::ptr::null_mut() } else { pathkeys };

        // Store in custom_private:
        // [oid1, oid2, ..., -1, num_aggs,
        //  agg_type_0, col_idx_0, result_oid_0, col_type_oid_0, expr_kind_0,
        //  ...,
        //  num_groups, group_col_idx_0, group_type_oid_0, ...]
        let mut private_list: *mut pg_sys::List = std::ptr::null_mut();
        for &oid in companion_oids {
            private_list = pg_sys::lappend_int(private_list, u32::from(oid) as i32);
        }
        private_list = pg_sys::lappend_int(private_list, -1);  // sentinel
        private_list = pg_sys::lappend_int(private_list, agg_specs.len() as i32);
        for spec in agg_specs {
            private_list = pg_sys::lappend_int(private_list, spec.agg_type as i32);
            private_list = pg_sys::lappend_int(private_list, spec.col_idx);
            private_list = pg_sys::lappend_int(private_list, u32::from(spec.result_type_oid) as i32);
            private_list = pg_sys::lappend_int(private_list, u32::from(spec.col_type_oid) as i32);
            private_list = pg_sys::lappend_int(private_list, spec.expr_kind as i32);
            if matches!(spec.expr_kind, super::exec::AggExpr::AddConst) {
                private_list = pg_sys::lappend_int(private_list, spec.const_offset as i32);
            }
        }
        private_list = pg_sys::lappend_int(private_list, group_specs.len() as i32);
        for gs in group_specs {
            private_list = pg_sys::lappend_int(private_list, gs.col_idx);
            private_list = pg_sys::lappend_int(private_list, u32::from(gs.type_oid) as i32);
            match &gs.expr {
                super::exec::GroupByExpr::Column => {
                    private_list = pg_sys::lappend_int(private_list, 0); // expr_tag=0
                }
                super::exec::GroupByExpr::RegexpReplace { pattern, replacement, func_oid, collation } => {
                    private_list = pg_sys::lappend_int(private_list, 1); // expr_tag=1
                    private_list = pg_sys::lappend_int(private_list, *func_oid as i32);
                    private_list = pg_sys::lappend_int(private_list, *collation as i32);
                    private_list = pg_sys::lappend_int(private_list, pattern.len() as i32);
                    for &b in pattern.as_bytes() {
                        private_list = pg_sys::lappend_int(private_list, b as i32);
                    }
                    private_list = pg_sys::lappend_int(private_list, replacement.len() as i32);
                    for &b in replacement.as_bytes() {
                        private_list = pg_sys::lappend_int(private_list, b as i32);
                    }
                }
                super::exec::GroupByExpr::DateTrunc { unit, func_oid, .. } => {
                    private_list = pg_sys::lappend_int(private_list, 2); // expr_tag=2
                    private_list = pg_sys::lappend_int(private_list, *func_oid as i32);
                    private_list = pg_sys::lappend_int(private_list, unit.len() as i32);
                    for &b in unit.as_bytes() {
                        private_list = pg_sys::lappend_int(private_list, b as i32);
                    }
                }
                super::exec::GroupByExpr::Extract { unit, func_oid } => {
                    private_list = pg_sys::lappend_int(private_list, 3); // expr_tag=3
                    private_list = pg_sys::lappend_int(private_list, *func_oid as i32);
                    private_list = pg_sys::lappend_int(private_list, unit.len() as i32);
                    for &b in unit.as_bytes() {
                        private_list = pg_sys::lappend_int(private_list, b as i32);
                    }
                }
                super::exec::GroupByExpr::AddConst { offset, op_oid } => {
                    private_list = pg_sys::lappend_int(private_list, 4); // expr_tag=4
                    private_list = pg_sys::lappend_int(private_list, *offset as i32);
                    private_list = pg_sys::lappend_int(private_list, *op_oid as i32);
                }
            }
        }
        // Store HAVING filters for thread-local passing to plan_agg_path
        if !having_filters.is_empty() {
            set_agg_having_filters(having_filters.to_vec());
        }
        (*cpath).custom_private = private_list;

        (*cpath).custom_paths = std::ptr::null_mut();
        (*cpath).custom_restrictinfo = std::ptr::null_mut();
        (*cpath).methods = &DELTAX_AGG_PATH_METHODS.0;

        pg_sys::add_path(output_rel, cpath as *mut pg_sys::Path);
    }
}

/// PlanCustomPath callback for DeltaXAgg.
///
/// Uses PG's provided _tlist for both plan.targetlist and custom_scan_tlist
/// so that PG's setrefs and sort/pathkey matching work correctly.
/// Builds an output mapping in custom_private so the execution knows which
/// slot position corresponds to which accumulator or group column.
#[pg_guard]
pub unsafe extern "C-unwind" fn plan_agg_path(
    root: *mut pg_sys::PlannerInfo,
    _rel: *mut pg_sys::RelOptInfo,
    best_path: *mut pg_sys::CustomPath,
    tlist: *mut pg_sys::List,
    _clauses: *mut pg_sys::List,
    _custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
    unsafe {
        let cscan =
            pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScan>()) as *mut pg_sys::CustomScan;

        (*cscan).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
        (*cscan).scan.scanrelid = 0;

        // Use PG's tlist for both targetlists. PG's setrefs will match
        // expressions by equal() and create proper Var(INDEX_VAR) references.
        // This allows ORDER BY/Sort to find pathkey expressions in our output.
        // Strip resjunk entries from custom_scan_tlist — we only produce
        // non-resjunk output columns. Resjunk entries are added by PG for
        // sort keys and HAVING references and would cause column count mismatches.
        let clean_tlist = {
            let mut list: *mut pg_sys::List = std::ptr::null_mut();
            if !tlist.is_null() {
                let n = (*tlist).length;
                let mut resno: i16 = 1;
                for i in 0..n {
                    let te = pg_sys::list_nth(tlist, i) as *const pg_sys::TargetEntry;
                    if te.is_null() || (*te).resjunk {
                        continue;
                    }
                    let te_copy = pg_sys::copyObjectImpl(te as *const _) as *mut pg_sys::TargetEntry;
                    (*te_copy).resno = resno;
                    resno += 1;
                    list = pg_sys::lappend(list, te_copy as *mut _);
                }
            }
            list
        };
        (*cscan).scan.plan.targetlist = pg_sys::copyObjectImpl(clean_tlist as *const _) as *mut pg_sys::List;
        (*cscan).custom_scan_tlist = clean_tlist;

        // Parse path's custom_private to get agg specs and group specs,
        // then build output mapping by walking tlist
        let path_private = (*best_path).custom_private;
        let path_len = (*path_private).length;

        // Parse OIDs, agg specs, group specs from path's custom_private
        struct ParsedAgg {
            agg_type: i32,
            col_idx: i32,
            result_oid: u32,
            col_type_oid: u32,
            expr_kind: i32,  // 0=Column, 1=LengthOf, 2=AddConst
            const_offset: i32, // Only used when expr_kind == 2
        }
        #[derive(Clone)]
        enum ParsedGroupExpr {
            Column,
            RegexpReplace { func_oid: u32, collation: u32, pattern: String, replacement: String },
            DateTrunc { func_oid: u32, unit: String },
            Extract { func_oid: u32, unit: String },
            AddConst { offset: i32, op_oid: u32 },
        }
        #[derive(Clone)]
        struct ParsedGroup {
            col_idx: i32,
            type_oid: u32,
            expr: ParsedGroupExpr,
        }

        let mut companion_oids: Vec<u32> = Vec::new();
        let mut parsed_aggs: Vec<ParsedAgg> = Vec::new();
        let mut parsed_groups: Vec<ParsedGroup> = Vec::new();

        // Sequential parse with index
        let mut idx = 0;
        // Parse OIDs until sentinel
        while idx < path_len {
            let val = pg_sys::list_nth_int(path_private, idx);
            idx += 1;
            if val == -1 { break; }
            companion_oids.push(val as u32);
        }
        // Parse agg specs
        if idx < path_len {
            let num_aggs = pg_sys::list_nth_int(path_private, idx) as usize;
            idx += 1;
            for _ in 0..num_aggs {
                let agg_type = pg_sys::list_nth_int(path_private, idx);
                let col_idx = pg_sys::list_nth_int(path_private, idx + 1);
                let result_oid = pg_sys::list_nth_int(path_private, idx + 2) as u32;
                let col_type_oid = pg_sys::list_nth_int(path_private, idx + 3) as u32;
                let expr_kind = pg_sys::list_nth_int(path_private, idx + 4);
                idx += 5;
                let const_offset = if expr_kind == 2 {
                    let v = pg_sys::list_nth_int(path_private, idx);
                    idx += 1;
                    v
                } else {
                    0
                };
                parsed_aggs.push(ParsedAgg { agg_type, col_idx, result_oid, col_type_oid, expr_kind, const_offset });
            }
        }
        // Parse group specs (variable-length due to RegexpReplace)
        if idx < path_len {
            let num_groups = pg_sys::list_nth_int(path_private, idx) as usize;
            idx += 1;
            for _ in 0..num_groups {
                let col_idx = pg_sys::list_nth_int(path_private, idx);
                let type_oid = pg_sys::list_nth_int(path_private, idx + 1) as u32;
                let expr_tag = pg_sys::list_nth_int(path_private, idx + 2);
                idx += 3;
                let expr = if expr_tag == 1 {
                    let func_oid = pg_sys::list_nth_int(path_private, idx) as u32;
                    let collation = pg_sys::list_nth_int(path_private, idx + 1) as u32;
                    idx += 2;
                    let pattern_len = pg_sys::list_nth_int(path_private, idx) as usize;
                    idx += 1;
                    let mut pattern_bytes = Vec::with_capacity(pattern_len);
                    for _ in 0..pattern_len {
                        pattern_bytes.push(pg_sys::list_nth_int(path_private, idx) as u8);
                        idx += 1;
                    }
                    let pattern = String::from_utf8_lossy(&pattern_bytes).into_owned();
                    let replacement_len = pg_sys::list_nth_int(path_private, idx) as usize;
                    idx += 1;
                    let mut replacement_bytes = Vec::with_capacity(replacement_len);
                    for _ in 0..replacement_len {
                        replacement_bytes.push(pg_sys::list_nth_int(path_private, idx) as u8);
                        idx += 1;
                    }
                    let replacement = String::from_utf8_lossy(&replacement_bytes).into_owned();
                    ParsedGroupExpr::RegexpReplace { func_oid, collation, pattern, replacement }
                } else if expr_tag == 2 {
                    let func_oid = pg_sys::list_nth_int(path_private, idx) as u32;
                    idx += 1;
                    let unit_len = pg_sys::list_nth_int(path_private, idx) as usize;
                    idx += 1;
                    let mut unit_bytes = Vec::with_capacity(unit_len);
                    for _ in 0..unit_len {
                        unit_bytes.push(pg_sys::list_nth_int(path_private, idx) as u8);
                        idx += 1;
                    }
                    let unit = String::from_utf8_lossy(&unit_bytes).into_owned();
                    ParsedGroupExpr::DateTrunc { func_oid, unit }
                } else if expr_tag == 3 {
                    let func_oid = pg_sys::list_nth_int(path_private, idx) as u32;
                    idx += 1;
                    let unit_len = pg_sys::list_nth_int(path_private, idx) as usize;
                    idx += 1;
                    let mut unit_bytes = Vec::with_capacity(unit_len);
                    for _ in 0..unit_len {
                        unit_bytes.push(pg_sys::list_nth_int(path_private, idx) as u8);
                        idx += 1;
                    }
                    let unit = String::from_utf8_lossy(&unit_bytes).into_owned();
                    ParsedGroupExpr::Extract { func_oid, unit }
                } else if expr_tag == 4 {
                    let offset = pg_sys::list_nth_int(path_private, idx);
                    let op_oid = pg_sys::list_nth_int(path_private, idx + 1) as u32;
                    idx += 2;
                    ParsedGroupExpr::AddConst { offset, op_oid }
                } else {
                    ParsedGroupExpr::Column
                };
                parsed_groups.push(ParsedGroup { col_idx, type_oid, expr });
            }
        }

        // Walk tlist to build output mapping:
        // For each tlist entry, determine if it's an Aggref or a group Var/FuncExpr.
        // Track which agg_spec index or group_spec index it maps to.
        // output_map[i] = (type, index) where type=0 → agg, type=1 → group, type=2 → const
        let mut output_map: Vec<(i32, i32)> = Vec::new();
        let mut const_outputs: Vec<(pg_sys::Oid, i64, bool)> = Vec::new();
        let mut agg_counter = 0;

        if !tlist.is_null() {
            let n = (*tlist).length;
            for i in 0..n {
                let te = pg_sys::list_nth(tlist, i) as *const pg_sys::TargetEntry;
                if te.is_null() {
                    continue;
                }
                // Skip resjunk entries — these are internal PG entries for ORDER BY
                // sort keys or HAVING references, not part of the query's output.
                if (*te).resjunk {
                    continue;
                }
                let expr = (*te).expr as *const pg_sys::Node;
                if expr.is_null() {
                    continue;
                }
                if (*expr).type_ == pg_sys::NodeTag::T_Aggref {
                    // Map to the next agg_spec in order
                    output_map.push((0, agg_counter));
                    agg_counter += 1;
                } else if (*expr).type_ == pg_sys::NodeTag::T_Var {
                    let var_node = expr as *const pg_sys::Var;
                    let var_attno = (*var_node).varattno as i32 - 1;
                    // Find matching group spec
                    let group_idx = parsed_groups.iter().position(|g| g.col_idx == var_attno)
                        .unwrap_or(0) as i32;
                    output_map.push((1, group_idx));
                } else if (*expr).type_ == pg_sys::NodeTag::T_FuncExpr {
                    // FuncExpr in target list — find matching GROUP BY spec.
                    // Var position varies: regexp_replace(Var, ...) vs date_trunc(Const, Var).
                    let funcexpr = expr as *const pg_sys::FuncExpr;
                    let funcid = u32::from((*funcexpr).funcid);
                    let fn_args = (*funcexpr).args;
                    let mut col_idx = -1_i32;
                    if !fn_args.is_null() {
                        let nargs = (*fn_args).length;
                        for ai in 0..nargs {
                            let arg = (*(*fn_args).elements.add(ai as usize)).ptr_value as *const pg_sys::Node;
                            if !arg.is_null() && (*arg).type_ == pg_sys::NodeTag::T_Var {
                                col_idx = (*(arg as *const pg_sys::Var)).varattno as i32 - 1;
                                break;
                            }
                        }
                    }
                    let group_idx = parsed_groups.iter().position(|g| {
                        if g.col_idx != col_idx { return false; }
                        match &g.expr {
                            ParsedGroupExpr::RegexpReplace { func_oid, .. } => *func_oid == funcid,
                            ParsedGroupExpr::DateTrunc { func_oid, .. } => *func_oid == funcid,
                            ParsedGroupExpr::Extract { func_oid, .. } => *func_oid == funcid,
                            _ => false,
                        }
                    }).unwrap_or(0) as i32;
                    output_map.push((1, group_idx));
                } else if (*expr).type_ == pg_sys::NodeTag::T_OpExpr {
                    // OpExpr in target list (e.g. col - 1) — find matching GROUP BY AddConst spec
                    let opexpr = expr as *const pg_sys::OpExpr;
                    let op_oid = u32::from((*opexpr).opno);
                    let op_args = (*opexpr).args;
                    let mut col_idx = -1_i32;
                    let mut tlist_offset: i32 = 0;
                    let mut is_minus = false;
                    if !op_args.is_null() && (*op_args).length == 2 {
                        let left = (*(*op_args).elements.add(0)).ptr_value as *const pg_sys::Node;
                        let right = (*(*op_args).elements.add(1)).ptr_value as *const pg_sys::Node;
                        if !left.is_null() && !right.is_null() {
                            // Determine operator name for sign
                            let opname_ptr = pg_sys::get_opname((*opexpr).opno);
                            if !opname_ptr.is_null() {
                                let opname = std::ffi::CStr::from_ptr(opname_ptr).to_str().unwrap_or("");
                                is_minus = opname == "-";
                            }
                            if (*left).type_ == pg_sys::NodeTag::T_Var && (*right).type_ == pg_sys::NodeTag::T_Const {
                                col_idx = (*(left as *const pg_sys::Var)).varattno as i32 - 1;
                                let c = right as *const pg_sys::Const;
                                if !(*c).constisnull {
                                    let cv: i64 = match (*c).consttype {
                                        pg_sys::INT2OID => (*c).constvalue.value() as i16 as i64,
                                        pg_sys::INT4OID => (*c).constvalue.value() as i32 as i64,
                                        pg_sys::INT8OID => (*c).constvalue.value() as i64,
                                        _ => 0,
                                    };
                                    tlist_offset = if is_minus { -cv } else { cv } as i32;
                                }
                            } else if (*left).type_ == pg_sys::NodeTag::T_Const && (*right).type_ == pg_sys::NodeTag::T_Var {
                                col_idx = (*(right as *const pg_sys::Var)).varattno as i32 - 1;
                                let c = left as *const pg_sys::Const;
                                if !(*c).constisnull {
                                    let cv: i64 = match (*c).consttype {
                                        pg_sys::INT2OID => (*c).constvalue.value() as i16 as i64,
                                        pg_sys::INT4OID => (*c).constvalue.value() as i32 as i64,
                                        pg_sys::INT8OID => (*c).constvalue.value() as i64,
                                        _ => 0,
                                    };
                                    tlist_offset = cv as i32; // const + col, no negation
                                }
                            }
                        }
                    }
                    let group_idx = parsed_groups.iter().position(|g| {
                        if g.col_idx != col_idx { return false; }
                        match &g.expr {
                            ParsedGroupExpr::AddConst { offset, op_oid: spec_op_oid } => {
                                *offset == tlist_offset && *spec_op_oid == op_oid
                            }
                            _ => false,
                        }
                    }).unwrap_or(0) as i32;
                    output_map.push((1, group_idx));
                } else if (*expr).type_ == pg_sys::NodeTag::T_Const {
                    // Constant in SELECT list (e.g. SELECT 1, ...) — serialize type + value
                    let c = expr as *const pg_sys::Const;
                    let type_oid = (*c).consttype;
                    let (const_val, is_null) = if (*c).constisnull {
                        (0i64, true)
                    } else {
                        let v: i64 = match type_oid {
                            pg_sys::INT2OID => (*c).constvalue.value() as i16 as i64,
                            pg_sys::INT4OID => (*c).constvalue.value() as i32 as i64,
                            pg_sys::INT8OID => (*c).constvalue.value() as i64,
                            pg_sys::BOOLOID => (*c).constvalue.value() as i64,
                            _ => (*c).constvalue.value() as i64,
                        };
                        (v, false)
                    };
                    // type=2 signals a constant output: next 3 ints are type_oid, value, is_null
                    output_map.push((2, const_val as i32));
                    // Store type_oid and is_null as extra entries after the output_map
                    // Actually, encode inline: (2, type_oid, value, is_null)
                    // We'll extend the serialization format below
                    const_outputs.push((type_oid, const_val, is_null));
                }
            }
        }

        // Build custom_private: [OIDs..., -1, num_aggs, agg_spec_fields...,
        //                        num_groups, group_spec_fields...,
        //                        num_output, output_type_0, output_ref_0, ...]
        let mut private_list: *mut pg_sys::List = std::ptr::null_mut();
        for &oid in &companion_oids {
            private_list = pg_sys::lappend_int(private_list, oid as i32);
        }
        private_list = pg_sys::lappend_int(private_list, -1);
        private_list = pg_sys::lappend_int(private_list, parsed_aggs.len() as i32);
        for a in &parsed_aggs {
            private_list = pg_sys::lappend_int(private_list, a.agg_type);
            private_list = pg_sys::lappend_int(private_list, a.col_idx);
            private_list = pg_sys::lappend_int(private_list, a.result_oid as i32);
            private_list = pg_sys::lappend_int(private_list, a.col_type_oid as i32);
            private_list = pg_sys::lappend_int(private_list, a.expr_kind);
            if a.expr_kind == 2 {
                private_list = pg_sys::lappend_int(private_list, a.const_offset);
            }
        }
        private_list = pg_sys::lappend_int(private_list, parsed_groups.len() as i32);
        for g in &parsed_groups {
            private_list = pg_sys::lappend_int(private_list, g.col_idx);
            private_list = pg_sys::lappend_int(private_list, g.type_oid as i32);
            match &g.expr {
                ParsedGroupExpr::Column => {
                    private_list = pg_sys::lappend_int(private_list, 0);
                }
                ParsedGroupExpr::RegexpReplace { func_oid, collation, pattern, replacement } => {
                    private_list = pg_sys::lappend_int(private_list, 1);
                    private_list = pg_sys::lappend_int(private_list, *func_oid as i32);
                    private_list = pg_sys::lappend_int(private_list, *collation as i32);
                    private_list = pg_sys::lappend_int(private_list, pattern.len() as i32);
                    for &b in pattern.as_bytes() {
                        private_list = pg_sys::lappend_int(private_list, b as i32);
                    }
                    private_list = pg_sys::lappend_int(private_list, replacement.len() as i32);
                    for &b in replacement.as_bytes() {
                        private_list = pg_sys::lappend_int(private_list, b as i32);
                    }
                }
                ParsedGroupExpr::DateTrunc { func_oid, unit } => {
                    private_list = pg_sys::lappend_int(private_list, 2);
                    private_list = pg_sys::lappend_int(private_list, *func_oid as i32);
                    private_list = pg_sys::lappend_int(private_list, unit.len() as i32);
                    for &b in unit.as_bytes() {
                        private_list = pg_sys::lappend_int(private_list, b as i32);
                    }
                }
                ParsedGroupExpr::Extract { func_oid, unit } => {
                    private_list = pg_sys::lappend_int(private_list, 3);
                    private_list = pg_sys::lappend_int(private_list, *func_oid as i32);
                    private_list = pg_sys::lappend_int(private_list, unit.len() as i32);
                    for &b in unit.as_bytes() {
                        private_list = pg_sys::lappend_int(private_list, b as i32);
                    }
                }
                ParsedGroupExpr::AddConst { offset, op_oid } => {
                    private_list = pg_sys::lappend_int(private_list, 4);
                    private_list = pg_sys::lappend_int(private_list, *offset);
                    private_list = pg_sys::lappend_int(private_list, *op_oid as i32);
                }
            }
        }
        // Output mapping
        private_list = pg_sys::lappend_int(private_list, output_map.len() as i32);
        let mut const_idx = 0usize;
        for (otype, oref) in &output_map {
            private_list = pg_sys::lappend_int(private_list, *otype);
            private_list = pg_sys::lappend_int(private_list, *oref);
            if *otype == 2 {
                // Const output: append type_oid, const_val_hi, const_val_lo, is_null
                let (type_oid, const_val, is_null) = const_outputs[const_idx];
                private_list = pg_sys::lappend_int(private_list, u32::from(type_oid) as i32);
                private_list = pg_sys::lappend_int(private_list, (const_val >> 32) as i32);
                private_list = pg_sys::lappend_int(private_list, const_val as i32);
                private_list = pg_sys::lappend_int(private_list, if is_null { 1 } else { 0 });
                const_idx += 1;
            }
        }

        // HAVING filters: [num_having, agg_idx_0, op_0, const_val_0, ...]
        let having_filters = take_agg_having_filters();
        private_list = pg_sys::lappend_int(private_list, having_filters.len() as i32);
        for hf in &having_filters {
            private_list = pg_sys::lappend_int(private_list, hf.agg_idx as i32);
            private_list = pg_sys::lappend_int(private_list, hf.op as i32);
            private_list = pg_sys::lappend_int(private_list, hf.const_val as i32);
        }

        // Store WHERE clause in custom_private as a serialized string.
        // We can't use plan.qual (setrefs would fail for scanrelid=0) or
        // thread-local (breaks when PG reuses a cached prepared plan).
        // custom_private is deep-copied by PG during plan caching, so the
        // serialized quals survive prepared statement reuse.
        //
        // Format: [str_len, char0, char1, ...] where str_len=0 means no quals.
        // Uses nodeToString/stringToNode for round-trip serialization.
        let parse = (*root).parse;
        let jointree = (*parse).jointree;
        let mut qual_list: *mut pg_sys::List = std::ptr::null_mut();

        let jointree_has_quals = !jointree.is_null() && !(*jointree).quals.is_null();
        if jointree_has_quals {
            let quals_node = (*jointree).quals as *const pg_sys::Node;
            qual_list = if (*quals_node).type_ == pg_sys::NodeTag::T_List {
                pg_sys::copyObjectImpl(quals_node as *const _) as *mut pg_sys::List
            } else {
                let qual_copy = pg_sys::copyObjectImpl(quals_node as *const _) as *mut pg_sys::Node;
                pg_sys::make_ands_implicit(qual_copy as *mut pg_sys::Expr)
            };
        }

        if qual_list.is_null() {
            qual_list = extract_quals_from_baserestrictinfo(root);
        }

        if !qual_list.is_null() {
            let s = pg_sys::nodeToString(qual_list as *const _);
            let s_bytes = std::ffi::CStr::from_ptr(s).to_bytes();
            let len = s_bytes.len() as i32;
            private_list = pg_sys::lappend_int(private_list, len);
            for &b in s_bytes {
                private_list = pg_sys::lappend_int(private_list, b as i32);
            }
            pg_sys::pfree(s as *mut _);
        } else {
            private_list = pg_sys::lappend_int(private_list, 0);
        }

        // Top-N info: [topn_limit, topn_sort_col, topn_ascending] or [0]
        let topn = take_agg_topn_info();
        if let Some((limit, sort_col, ascending)) = topn {
            private_list = pg_sys::lappend_int(private_list, limit as i32);
            private_list = pg_sys::lappend_int(private_list, sort_col);
            private_list = pg_sys::lappend_int(private_list, if ascending { 1 } else { 0 });
        } else {
            private_list = pg_sys::lappend_int(private_list, 0);
        }

        (*cscan).custom_private = private_list;
        (*cscan).scan.plan.qual = std::ptr::null_mut();

        (*cscan).custom_plans = std::ptr::null_mut();
        (*cscan).custom_relids = std::ptr::null_mut();
        (*cscan).methods = &DELTAX_AGG_SCAN_METHODS.0;
        (*cscan).flags = 0;

        let _ = root;

        &mut (*cscan).scan.plan as *mut pg_sys::Plan
    }
}

/// Extract WHERE clause expressions from the base relation's baserestrictinfo.
///
/// After PG's `deconstruct_jointree`, WHERE quals are distributed to base
/// relations as `RestrictInfo` nodes. This is a more reliable source than
/// `parse->jointree->quals`, which may be NULL by the time PlanCustomPath
/// callbacks fire.
///
/// Returns a copied List of clause expressions, or NULL if none found.
unsafe fn extract_quals_from_baserestrictinfo(
    root: *mut pg_sys::PlannerInfo,
) -> *mut pg_sys::List {
    unsafe {
        // Find the first base relation (typically RTI=1 for single-table queries)
        let array_size = (*root).simple_rel_array_size;
        for rti in 1..array_size {
            let rel = *(*root).simple_rel_array.add(rti as usize);
            if rel.is_null() {
                continue;
            }
            let bri = (*rel).baserestrictinfo;
            if bri.is_null() || (*bri).length == 0 {
                continue;
            }
            // Build a List of clause expressions from RestrictInfo nodes
            let mut result: *mut pg_sys::List = std::ptr::null_mut();
            for i in 0..(*bri).length {
                let ri = pg_sys::list_nth(bri, i) as *const pg_sys::RestrictInfo;
                if ri.is_null() {
                    continue;
                }
                let clause = (*ri).clause;
                if clause.is_null() {
                    continue;
                }
                // Copy the clause expression to avoid ownership issues
                let clause_copy = pg_sys::copyObjectImpl(clause as *const _) as *mut pg_sys::Expr;
                result = pg_sys::lappend(result, clause_copy as *mut _);
            }
            if !result.is_null() {
                return result;
            }
        }
        std::ptr::null_mut()
    }
}

// ============================================================================
// DeltaXAppend: replaces Append with single CustomScan for all compressed partitions
// ============================================================================

// Thread-local to pass Top-N info from add_deltax_append_path to plan_deltax_append_path.
thread_local! {
    static APPEND_TOPN_INFO: std::cell::Cell<(i64, bool)> = const { std::cell::Cell::new((0, true)) };
}

/// Add a DeltaXAppend custom path to the parent relation's pathlist.
///
/// This replaces the Append node with a single CustomScan that internally
/// iterates all compressed companion tables.
pub unsafe fn add_deltax_append_path(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    companion_oids: &[pg_sys::Oid],
    pathkeys: *mut pg_sys::List,
    effective_limit: i64,
    sort_ascending: bool,
) {
    unsafe {
        let cpath =
            pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;

        (*cpath).path.type_ = pg_sys::NodeTag::T_CustomPath;
        (*cpath).path.pathtype = pg_sys::NodeTag::T_CustomScan;
        (*cpath).path.parent = rel;
        (*cpath).path.pathtarget = (*rel).reltarget;

        // Cost = sum of individual companion costs
        let mut total_startup = 0.0f64;
        let mut total_cost = 0.0f64;
        let mut total_rows = 0.0f64;
        for &oid in companion_oids {
            let (startup, cost, rows) = cost::estimate_cost(oid);
            total_startup += startup;
            total_cost += cost;
            total_rows += rows;
        }
        (*cpath).path.rows = total_rows;
        (*cpath).path.startup_cost = total_startup;
        (*cpath).path.total_cost = total_cost;
        (*cpath).path.parallel_workers = 0;
        (*cpath).path.parallel_aware = false;
        (*cpath).path.parallel_safe = false;
        (*cpath).path.pathkeys = pathkeys;

        // Store companion OIDs in custom_private
        let mut private_list: *mut pg_sys::List = std::ptr::null_mut();
        for &oid in companion_oids {
            private_list = pg_sys::lappend_oid(private_list, oid);
        }
        (*cpath).custom_private = private_list;

        (*cpath).custom_paths = std::ptr::null_mut();
        (*cpath).custom_restrictinfo = std::ptr::null_mut();
        (*cpath).methods = &DELTAX_APPEND_PATH_METHODS.0;

        // Store Top-N info. Caller validates ORDER BY matches time column.
        if effective_limit > 0 {
            APPEND_TOPN_INFO.with(|cell| cell.set((effective_limit, sort_ascending)));
        } else {
            APPEND_TOPN_INFO.with(|cell| cell.set((0, true)));
        }

        // Clear existing paths (removes Append paths)
        (*rel).pathlist = std::ptr::null_mut();
        (*rel).partial_pathlist = std::ptr::null_mut();

        pg_sys::add_path(rel, cpath as *mut pg_sys::Path);

        // Mark rel as non-partitioned so that apply_scanjoin_target_to_paths()
        // in grouping_planner does NOT discard our path and rebuild Append
        // paths from children.  DeltaXAppend handles all partitions internally,
        // so the planner must treat this rel as a single-scan base rel.
        (*rel).nparts = 0;
    }
}

/// PlanCustomPath callback for DeltaXAppend.
#[pg_guard]
pub unsafe extern "C-unwind" fn plan_deltax_append_path(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    best_path: *mut pg_sys::CustomPath,
    tlist: *mut pg_sys::List,
    clauses: *mut pg_sys::List,
    _custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
    unsafe {
        let cscan =
            pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScan>()) as *mut pg_sys::CustomScan;

        (*cscan).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
        (*cscan).scan.plan.targetlist = tlist;
        // Use parent's RTI — PG creates scan slot from parent TupleDesc
        (*cscan).scan.scanrelid = (*rel).relid;

        let final_clauses = pg_sys::extract_actual_clauses(clauses, false);
        (*cscan).scan.plan.qual = final_clauses;

        // Build custom_private: [oid1, oid2, ..., -1 (sentinel), col0, col1, ...]
        // OIDs are stored as ints (safe since OIDs fit in u32/i32)
        let private_oid_list = (*best_path).custom_private;
        let num_oids = (*private_oid_list).length;

        let mut private_list: *mut pg_sys::List = std::ptr::null_mut();
        for i in 0..num_oids {
            let oid = pg_sys::list_nth_oid(private_oid_list, i);
            private_list = pg_sys::lappend_int(private_list, u32::from(oid) as i32);
        }

        // Append sentinel
        private_list = pg_sys::lappend_int(private_list, -1);

        // Extract needed column attribute numbers from tlist + quals using parent's varno
        let varno = (*rel).relid;
        let mut needed_attrs: *mut pg_sys::Bitmapset = std::ptr::null_mut();
        pg_sys::pull_varattnos(tlist as *mut pg_sys::Node, varno, &mut needed_attrs);
        pg_sys::pull_varattnos(
            final_clauses as *mut pg_sys::Node,
            varno,
            &mut needed_attrs,
        );

        let offset = pg_sys::FirstLowInvalidHeapAttributeNumber;
        let mut x: i32 = -1;
        loop {
            x = pg_sys::bms_next_member(needed_attrs, x);
            if x < 0 {
                break;
            }
            let attno = x + offset;
            if attno > 0 {
                private_list = pg_sys::lappend_int(private_list, attno - 1);
            }
        }

        // Append Top-N info: [-2, effective_limit, sort_ascending_flag]
        let (effective_limit, sort_ascending) = APPEND_TOPN_INFO.with(|cell| cell.replace((0, true)));
        if effective_limit > 0 {
            private_list = pg_sys::lappend_int(private_list, -2);
            private_list = pg_sys::lappend_int(private_list, effective_limit as i32);
            private_list = pg_sys::lappend_int(private_list, if sort_ascending { 1 } else { 0 });
        }

        (*cscan).custom_private = private_list;
        (*cscan).custom_scan_tlist = std::ptr::null_mut();
        (*cscan).custom_plans = std::ptr::null_mut();
        (*cscan).custom_relids = std::ptr::null_mut();
        (*cscan).methods = &DELTAX_APPEND_SCAN_METHODS.0;
        (*cscan).flags = 0;

        &mut (*cscan).scan.plan as *mut pg_sys::Plan
    }
}
