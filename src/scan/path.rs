use pgrx::pg_sys;
use pgrx::pg_guard;

use super::cost;
use super::SyncStatic;

thread_local! {
    /// Temporary storage for WHERE clause quals during SeaTurtleAgg planning.
    /// Set in plan_agg_path, consumed in create_agg_scan_state.
    static AGG_PLAN_QUALS: std::cell::Cell<*mut pg_sys::List> =
        const { std::cell::Cell::new(std::ptr::null_mut()) };
}

/// Store WHERE clause quals for the next SeaTurtleAgg plan.
pub(super) fn set_agg_plan_quals(quals: *mut pg_sys::List) {
    AGG_PLAN_QUALS.with(|cell| cell.set(quals));
}

/// Take (consume) the stored WHERE clause quals.
pub(super) fn take_agg_plan_quals() -> *mut pg_sys::List {
    AGG_PLAN_QUALS.with(|cell| cell.replace(std::ptr::null_mut()))
}

// ============================================================================
// SeaTurtleAppend path/plan methods
// ============================================================================

/// Static CustomPathMethods for SeaTurtleAppend.
static SEATURTLE_APPEND_PATH_METHODS: SyncStatic<pg_sys::CustomPathMethods> =
    SyncStatic(pg_sys::CustomPathMethods {
        CustomName: super::SEATURTLE_APPEND_NAME.as_ptr(),
        PlanCustomPath: Some(plan_seaturtle_append_path),
        ReparameterizeCustomPathByChild: None,
    });

/// Static CustomScanMethods for SeaTurtleAppend.
static SEATURTLE_APPEND_SCAN_METHODS: SyncStatic<pg_sys::CustomScanMethods> =
    SyncStatic(pg_sys::CustomScanMethods {
        CustomName: super::SEATURTLE_APPEND_NAME.as_ptr(),
        CreateCustomScanState: Some(super::exec::create_seaturtle_append_state),
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

/// Add a SeaTurtleDecompress custom path to the relation's pathlist.
pub unsafe fn add_decompress_path(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    companion_oid: pg_sys::Oid,
    pathkeys: *mut pg_sys::List,
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
// SeaTurtleCount: COUNT(*) aggregate pushdown
// ============================================================================

/// Static CustomPathMethods for SeaTurtleCount.
static SEATURTLE_COUNT_PATH_METHODS: SyncStatic<pg_sys::CustomPathMethods> =
    SyncStatic(pg_sys::CustomPathMethods {
        CustomName: super::SEATURTLE_COUNT_NAME.as_ptr(),
        PlanCustomPath: Some(plan_count_star_path),
        ReparameterizeCustomPathByChild: None,
    });

/// Static CustomScanMethods for SeaTurtleCount.
static SEATURTLE_COUNT_SCAN_METHODS: SyncStatic<pg_sys::CustomScanMethods> =
    SyncStatic(pg_sys::CustomScanMethods {
        CustomName: super::SEATURTLE_COUNT_NAME.as_ptr(),
        CreateCustomScanState: Some(super::exec::create_count_scan_state),
    });

/// Add a SeaTurtleCount custom path to the grouped relation's pathlist.
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
        (*cpath).methods = &SEATURTLE_COUNT_PATH_METHODS.0;

        pg_sys::add_path(output_rel, cpath as *mut pg_sys::Path);
    }
}

/// PlanCustomPath callback for SeaTurtleCount.
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
        (*cscan).methods = &SEATURTLE_COUNT_SCAN_METHODS.0;
        (*cscan).flags = 0;
        (*cscan).scan.plan.qual = std::ptr::null_mut();

        &mut (*cscan).scan.plan as *mut pg_sys::Plan
    }
}

// ============================================================================
// SeaTurtleMinMax: MIN/MAX aggregate pushdown on time column
// ============================================================================

/// Static CustomPathMethods for SeaTurtleMinMax.
static SEATURTLE_MINMAX_PATH_METHODS: SyncStatic<pg_sys::CustomPathMethods> =
    SyncStatic(pg_sys::CustomPathMethods {
        CustomName: super::SEATURTLE_MINMAX_NAME.as_ptr(),
        PlanCustomPath: Some(plan_minmax_path),
        ReparameterizeCustomPathByChild: None,
    });

/// Static CustomScanMethods for SeaTurtleMinMax.
static SEATURTLE_MINMAX_SCAN_METHODS: SyncStatic<pg_sys::CustomScanMethods> =
    SyncStatic(pg_sys::CustomScanMethods {
        CustomName: super::SEATURTLE_MINMAX_NAME.as_ptr(),
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

/// Add a SeaTurtleMinMax custom path to the grouped relation's pathlist.
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
        (*cpath).methods = &SEATURTLE_MINMAX_PATH_METHODS.0;

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

/// PlanCustomPath callback for SeaTurtleMinMax.
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
        (*cscan).methods = &SEATURTLE_MINMAX_SCAN_METHODS.0;
        (*cscan).flags = 0;
        (*cscan).scan.plan.qual = std::ptr::null_mut();

        &mut (*cscan).scan.plan as *mut pg_sys::Plan
    }
}

// ============================================================================
// SeaTurtleAgg: aggregate pushdown (SUM, AVG, COUNT, COUNT(DISTINCT), GROUP BY)
// ============================================================================

/// Static CustomPathMethods for SeaTurtleAgg.
static SEATURTLE_AGG_PATH_METHODS: SyncStatic<pg_sys::CustomPathMethods> =
    SyncStatic(pg_sys::CustomPathMethods {
        CustomName: super::SEATURTLE_AGG_NAME.as_ptr(),
        PlanCustomPath: Some(plan_agg_path),
        ReparameterizeCustomPathByChild: None,
    });

/// Static CustomScanMethods for SeaTurtleAgg.
static SEATURTLE_AGG_SCAN_METHODS: SyncStatic<pg_sys::CustomScanMethods> =
    SyncStatic(pg_sys::CustomScanMethods {
        CustomName: super::SEATURTLE_AGG_NAME.as_ptr(),
        CreateCustomScanState: Some(super::exec::create_agg_scan_state),
    });

/// Specification for one aggregate in a SeaTurtleAgg pushdown.
pub struct AggSpec {
    pub agg_type: super::exec::AggType,
    pub col_idx: i32,               // 0-based column index, -1 for COUNT(*)
    pub result_type_oid: pg_sys::Oid,
    pub col_type_oid: pg_sys::Oid,  // source column type OID
}

/// Add a SeaTurtleAgg custom path to the grouped relation's pathlist.
pub unsafe fn add_agg_path(
    _root: *mut pg_sys::PlannerInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    companion_oids: &[pg_sys::Oid],
    agg_specs: &[AggSpec],
    group_specs: &[super::exec::GroupByColSpec],
) {
    unsafe {
        let cpath =
            pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;

        (*cpath).path.type_ = pg_sys::NodeTag::T_CustomPath;
        (*cpath).path.pathtype = pg_sys::NodeTag::T_CustomScan;
        (*cpath).path.parent = output_rel;
        (*cpath).path.pathtarget = (*output_rel).reltarget;

        // Cost: cheaper than full scan + PG aggregation
        let estimated_rows = if group_specs.is_empty() { 1.0 } else { 100.0 };
        (*cpath).path.rows = estimated_rows;
        (*cpath).path.startup_cost = 10.0;
        (*cpath).path.total_cost = 20.0;
        (*cpath).path.parallel_workers = 0;
        (*cpath).path.parallel_aware = false;
        (*cpath).path.parallel_safe = false;

        // Store in custom_private:
        // [oid1, oid2, ..., -1, num_aggs,
        //  agg_type_0, col_idx_0, result_oid_0, col_type_oid_0,
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
        }
        private_list = pg_sys::lappend_int(private_list, group_specs.len() as i32);
        for gs in group_specs {
            private_list = pg_sys::lappend_int(private_list, gs.col_idx);
            private_list = pg_sys::lappend_int(private_list, u32::from(gs.type_oid) as i32);
        }
        (*cpath).custom_private = private_list;

        (*cpath).custom_paths = std::ptr::null_mut();
        (*cpath).custom_restrictinfo = std::ptr::null_mut();
        (*cpath).methods = &SEATURTLE_AGG_PATH_METHODS.0;

        pg_sys::add_path(output_rel, cpath as *mut pg_sys::Path);
    }
}

/// PlanCustomPath callback for SeaTurtleAgg.
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
        (*cscan).scan.plan.targetlist = pg_sys::copyObjectImpl(tlist as *const _) as *mut pg_sys::List;
        (*cscan).custom_scan_tlist = pg_sys::copyObjectImpl(tlist as *const _) as *mut pg_sys::List;

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
        }
        struct ParsedGroup {
            col_idx: i32,
            type_oid: u32,
        }

        let mut companion_oids: Vec<u32> = Vec::new();
        let mut parsed_aggs: Vec<ParsedAgg> = Vec::new();
        let mut parsed_groups: Vec<ParsedGroup> = Vec::new();
        let mut found_sentinel = false;
        let mut after_sentinel_idx = 0;
        let mut num_aggs: i32 = 0;
        let mut agg_fields: Vec<i32> = Vec::new();
        let mut aggs_parsed = false;
        let mut num_groups: i32 = 0;
        let mut group_fields: Vec<i32> = Vec::new();

        for i in 0..path_len {
            let val = pg_sys::list_nth_int(path_private, i);
            if !found_sentinel {
                if val == -1 {
                    found_sentinel = true;
                    continue;
                }
                companion_oids.push(val as u32);
            } else if !aggs_parsed {
                if after_sentinel_idx == 0 {
                    num_aggs = val;
                    after_sentinel_idx += 1;
                    if num_aggs == 0 { aggs_parsed = true; }
                    continue;
                }
                agg_fields.push(val);
                if agg_fields.len() == 4 {
                    parsed_aggs.push(ParsedAgg {
                        agg_type: agg_fields[0],
                        col_idx: agg_fields[1],
                        result_oid: agg_fields[2] as u32,
                        col_type_oid: agg_fields[3] as u32,
                    });
                    agg_fields.clear();
                    if parsed_aggs.len() == num_aggs as usize {
                        aggs_parsed = true;
                    }
                }
                after_sentinel_idx += 1;
            } else {
                if num_groups == 0 && group_fields.is_empty() {
                    num_groups = val;
                    continue;
                }
                group_fields.push(val);
                if group_fields.len() == 2 {
                    parsed_groups.push(ParsedGroup {
                        col_idx: group_fields[0],
                        type_oid: group_fields[1] as u32,
                    });
                    group_fields.clear();
                }
            }
        }

        // Walk tlist to build output mapping:
        // For each tlist entry, determine if it's an Aggref or a group Var.
        // Track which agg_spec index or group_spec index it maps to.
        // output_map[i] = (type, index) where type=0 → agg, type=1 → group
        let mut output_map: Vec<(i32, i32)> = Vec::new();
        let mut agg_counter = 0;

        if !tlist.is_null() {
            let n = (*tlist).length;
            for i in 0..n {
                let te = pg_sys::list_nth(tlist, i) as *const pg_sys::TargetEntry;
                if te.is_null() {
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
        }
        private_list = pg_sys::lappend_int(private_list, parsed_groups.len() as i32);
        for g in &parsed_groups {
            private_list = pg_sys::lappend_int(private_list, g.col_idx);
            private_list = pg_sys::lappend_int(private_list, g.type_oid as i32);
        }
        // Output mapping
        private_list = pg_sys::lappend_int(private_list, output_map.len() as i32);
        for (otype, oref) in &output_map {
            private_list = pg_sys::lappend_int(private_list, *otype);
            private_list = pg_sys::lappend_int(private_list, *oref);
        }

        (*cscan).custom_private = private_list;

        // Store WHERE clause from parse tree for execution.
        // We can't put it on plan.qual (setrefs would fail for scanrelid=0),
        // so we pass it via thread-local to create_agg_scan_state.
        let parse = (*root).parse;
        let jointree = (*parse).jointree;
        if !jointree.is_null() && !(*jointree).quals.is_null() {
            let quals_node = (*jointree).quals as *const pg_sys::Node;
            let qual_list = if (*quals_node).type_ == pg_sys::NodeTag::T_List {
                // quals is already a List — copy it directly
                pg_sys::copyObjectImpl(quals_node as *const _) as *mut pg_sys::List
            } else {
                // Single expression — copy and wrap in list
                let qual_copy = pg_sys::copyObjectImpl(quals_node as *const _) as *mut pg_sys::Node;
                pg_sys::make_ands_implicit(qual_copy as *mut pg_sys::Expr)
            };
            set_agg_plan_quals(qual_list);
        }

        (*cscan).scan.plan.qual = std::ptr::null_mut();

        (*cscan).custom_plans = std::ptr::null_mut();
        (*cscan).custom_relids = std::ptr::null_mut();
        (*cscan).methods = &SEATURTLE_AGG_SCAN_METHODS.0;
        (*cscan).flags = 0;

        let _ = root;

        &mut (*cscan).scan.plan as *mut pg_sys::Plan
    }
}

// ============================================================================
// SeaTurtleAppend: replaces Append with single CustomScan for all compressed partitions
// ============================================================================

/// Add a SeaTurtleAppend custom path to the parent relation's pathlist.
///
/// This replaces the Append node with a single CustomScan that internally
/// iterates all compressed companion tables.
pub unsafe fn add_seaturtle_append_path(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    companion_oids: &[pg_sys::Oid],
    pathkeys: *mut pg_sys::List,
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
        (*cpath).methods = &SEATURTLE_APPEND_PATH_METHODS.0;

        // Clear existing paths (removes Append paths)
        (*rel).pathlist = std::ptr::null_mut();
        (*rel).partial_pathlist = std::ptr::null_mut();

        pg_sys::add_path(rel, cpath as *mut pg_sys::Path);
    }
}

/// PlanCustomPath callback for SeaTurtleAppend.
#[pg_guard]
pub unsafe extern "C-unwind" fn plan_seaturtle_append_path(
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

        (*cscan).custom_private = private_list;
        (*cscan).custom_scan_tlist = std::ptr::null_mut();
        (*cscan).custom_plans = std::ptr::null_mut();
        (*cscan).custom_relids = std::ptr::null_mut();
        (*cscan).methods = &SEATURTLE_APPEND_SCAN_METHODS.0;
        (*cscan).flags = 0;

        &mut (*cscan).scan.plan as *mut pg_sys::Plan
    }
}
