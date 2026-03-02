use pgrx::pg_sys;
use pgrx::pg_guard;

use super::cost;
use super::SyncStatic;

// ============================================================================
// CocoonAppend path/plan methods
// ============================================================================

/// Static CustomPathMethods for CocoonAppend.
static COCOON_APPEND_PATH_METHODS: SyncStatic<pg_sys::CustomPathMethods> =
    SyncStatic(pg_sys::CustomPathMethods {
        CustomName: super::COCOON_APPEND_NAME.as_ptr(),
        PlanCustomPath: Some(plan_cocoon_append_path),
        ReparameterizeCustomPathByChild: None,
    });

/// Static CustomScanMethods for CocoonAppend.
static COCOON_APPEND_SCAN_METHODS: SyncStatic<pg_sys::CustomScanMethods> =
    SyncStatic(pg_sys::CustomScanMethods {
        CustomName: super::COCOON_APPEND_NAME.as_ptr(),
        CreateCustomScanState: Some(super::exec::create_cocoon_append_state),
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

/// Add a CocoonDecompress custom path to the relation's pathlist.
pub unsafe fn add_decompress_path(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    companion_oid: pg_sys::Oid,
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
// CocoonCount: COUNT(*) aggregate pushdown
// ============================================================================

/// Static CustomPathMethods for CocoonCount.
static COCOON_COUNT_PATH_METHODS: SyncStatic<pg_sys::CustomPathMethods> =
    SyncStatic(pg_sys::CustomPathMethods {
        CustomName: super::COCOON_COUNT_NAME.as_ptr(),
        PlanCustomPath: Some(plan_count_star_path),
        ReparameterizeCustomPathByChild: None,
    });

/// Static CustomScanMethods for CocoonCount.
static COCOON_COUNT_SCAN_METHODS: SyncStatic<pg_sys::CustomScanMethods> =
    SyncStatic(pg_sys::CustomScanMethods {
        CustomName: super::COCOON_COUNT_NAME.as_ptr(),
        CreateCustomScanState: Some(super::exec::create_count_scan_state),
    });

/// Add a CocoonCount custom path to the grouped relation's pathlist.
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
        (*cpath).methods = &COCOON_COUNT_PATH_METHODS.0;

        pg_sys::add_path(output_rel, cpath as *mut pg_sys::Path);
    }
}

/// PlanCustomPath callback for CocoonCount.
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
        (*cscan).methods = &COCOON_COUNT_SCAN_METHODS.0;
        (*cscan).flags = 0;
        (*cscan).scan.plan.qual = std::ptr::null_mut();

        &mut (*cscan).scan.plan as *mut pg_sys::Plan
    }
}

// ============================================================================
// CocoonAppend: replaces Append with single CustomScan for all compressed partitions
// ============================================================================

/// Add a CocoonAppend custom path to the parent relation's pathlist.
///
/// This replaces the Append node with a single CustomScan that internally
/// iterates all compressed companion tables.
pub unsafe fn add_cocoon_append_path(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    companion_oids: &[pg_sys::Oid],
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

        // Store companion OIDs in custom_private
        let mut private_list: *mut pg_sys::List = std::ptr::null_mut();
        for &oid in companion_oids {
            private_list = pg_sys::lappend_oid(private_list, oid);
        }
        (*cpath).custom_private = private_list;

        (*cpath).custom_paths = std::ptr::null_mut();
        (*cpath).custom_restrictinfo = std::ptr::null_mut();
        (*cpath).methods = &COCOON_APPEND_PATH_METHODS.0;

        // Clear existing paths (removes Append paths)
        (*rel).pathlist = std::ptr::null_mut();
        (*rel).partial_pathlist = std::ptr::null_mut();

        pg_sys::add_path(rel, cpath as *mut pg_sys::Path);
    }
}

/// PlanCustomPath callback for CocoonAppend.
#[pg_guard]
pub unsafe extern "C-unwind" fn plan_cocoon_append_path(
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
        (*cscan).methods = &COCOON_APPEND_SCAN_METHODS.0;
        (*cscan).flags = 0;

        &mut (*cscan).scan.plan as *mut pg_sys::Plan
    }
}
