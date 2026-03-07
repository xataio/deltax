use pgrx::pg_sys;
use pgrx::pg_guard;
use std::collections::HashMap;
use std::ffi::c_int;
use std::sync::atomic::Ordering;

use super::PREV_HOOK;
use super::PREV_UPPER_HOOK;
use super::PREV_EXECUTOR_START_HOOK;
use super::path;
use super::cost;

thread_local! {
    /// Cache of partition OID → companion table OID (or InvalidOid if not compressed).
    static COMPRESSED_CACHE: std::cell::RefCell<HashMap<pg_sys::Oid, pg_sys::Oid>> =
        std::cell::RefCell::new(HashMap::new());

    /// Cache of parent table OID → time column attribute number (0 = not a hypertable).
    static TIME_COLUMN_CACHE: std::cell::RefCell<HashMap<pg_sys::Oid, i16>> =
        std::cell::RefCell::new(HashMap::new());

    /// When true, the ExecutorStart hook skips the DML-on-compressed check.
    /// Used by internal operations like seaturtle_decompress_partition.
    static DML_BYPASS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub fn invalidate_compressed_cache() {
    COMPRESSED_CACHE.with(|cache| cache.borrow_mut().clear());
    TIME_COLUMN_CACHE.with(|cache| cache.borrow_mut().clear());
}

/// Set or clear the DML bypass flag for internal operations.
pub(crate) fn set_dml_bypass(bypass: bool) {
    DML_BYPASS.with(|flag| flag.set(bypass));
}

/// Get the time column's attribute number for a hypertable parent table.
/// Returns None if the table is not a seaturtle hypertable. Result is cached.
unsafe fn get_time_column_attno(parent_oid: pg_sys::Oid) -> Option<i16> {
    let cached = TIME_COLUMN_CACHE.with(|cache| cache.borrow().get(&parent_oid).copied());
    if let Some(attno) = cached {
        return if attno > 0 { Some(attno) } else { None };
    }

    unsafe {
        let schema_name_ptr = pg_sys::get_namespace_name(pg_sys::get_rel_namespace(parent_oid));
        let table_name_ptr = pg_sys::get_rel_name(parent_oid);
        if schema_name_ptr.is_null() || table_name_ptr.is_null() {
            TIME_COLUMN_CACHE.with(|cache| cache.borrow_mut().insert(parent_oid, 0));
            return None;
        }
        let schema_name = std::ffi::CStr::from_ptr(schema_name_ptr)
            .to_string_lossy()
            .into_owned();
        let table_name = std::ffi::CStr::from_ptr(table_name_ptr)
            .to_string_lossy()
            .into_owned();

        let time_col_name: Option<String> = pgrx::Spi::connect(|client| {
            let result = client.select(
                "SELECT time_column FROM seaturtle_hypertable WHERE schema_name = $1 AND table_name = $2",
                None,
                &[schema_name.as_str().into(), table_name.as_str().into()],
            );
            match result {
                Ok(mut table) => match table.next() {
                    Some(row) => row
                        .get_datum_by_ordinal(1)
                        .ok()
                        .and_then(|d| d.value::<String>().ok())
                        .flatten(),
                    None => None,
                },
                Err(_) => None,
            }
        });

        match time_col_name {
            Some(col_name) => {
                let col_cname = std::ffi::CString::new(col_name).unwrap();
                let attno = pg_sys::get_attnum(parent_oid, col_cname.as_ptr());
                if attno == pg_sys::InvalidAttrNumber as i16 {
                    TIME_COLUMN_CACHE.with(|cache| cache.borrow_mut().insert(parent_oid, 0));
                    None
                } else {
                    TIME_COLUMN_CACHE.with(|cache| cache.borrow_mut().insert(parent_oid, attno));
                    Some(attno)
                }
            }
            None => {
                TIME_COLUMN_CACHE.with(|cache| cache.borrow_mut().insert(parent_oid, 0));
                None
            }
        }
    }
}

/// Find the parent table OID for a child partition via append_rel_list.
unsafe fn find_parent_oid(
    root: *mut pg_sys::PlannerInfo,
    child_rti: pg_sys::Index,
) -> Option<pg_sys::Oid> {
    unsafe {
        let list = (*root).append_rel_list;
        if list.is_null() {
            return None;
        }
        let len = (*list).length;
        for i in 0..len {
            let node = pg_sys::list_nth(list, i) as *const pg_sys::AppendRelInfo;
            if node.is_null() {
                continue;
            }
            if (*node).child_relid == child_rti {
                let parent_rte =
                    *(*root).simple_rte_array.add((*node).parent_relid as usize);
                return Some((*parent_rte).relid);
            }
        }
        None
    }
}

/// Check if the first query pathkey matches the time column (ASC only).
/// Returns a single-element pathkey list if matched, null otherwise.
unsafe fn check_time_pathkey(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    time_col_attno: i16,
) -> *mut pg_sys::List {
    unsafe {
        let query_pathkeys = (*root).query_pathkeys;
        if query_pathkeys.is_null() || (*query_pathkeys).length == 0 {
            return std::ptr::null_mut();
        }

        let first_pk = pg_sys::list_nth(query_pathkeys, 0) as *mut pg_sys::PathKey;
        if first_pk.is_null() {
            return std::ptr::null_mut();
        }

        // Only ASC for now
        #[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
        let is_asc = (*first_pk).pk_strategy == pg_sys::BTLessStrategyNumber as i32;
        #[cfg(feature = "pg18")]
        let is_asc = (*first_pk).pk_cmptype == pg_sys::CompareType::COMPARE_LT;
        if !is_asc {
            return std::ptr::null_mut();
        }

        let eclass = (*first_pk).pk_eclass;
        if eclass.is_null() {
            return std::ptr::null_mut();
        }

        let members = (*eclass).ec_members;
        if members.is_null() {
            return std::ptr::null_mut();
        }

        let rel_varno = (*rel).relid;
        let nmembers = (*members).length;
        for i in 0..nmembers {
            let member = pg_sys::list_nth(members, i) as *const pg_sys::EquivalenceMember;
            if member.is_null() {
                continue;
            }
            let expr = (*member).em_expr as *const pg_sys::Node;
            if expr.is_null() {
                continue;
            }
            if (*expr).type_ != pg_sys::NodeTag::T_Var {
                continue;
            }
            let var = expr as *const pg_sys::Var;
            if (*var).varno as u32 == rel_varno && (*var).varattno == time_col_attno {
                // Match — return single-element list with this PathKey
                return pg_sys::lappend(
                    std::ptr::null_mut(),
                    first_pk as *mut std::ffi::c_void,
                );
            }
        }

        std::ptr::null_mut()
    }
}

/// The planner hook. Called for each relation during path generation.
#[pg_guard]
pub unsafe extern "C-unwind" fn seaturtle_set_rel_pathlist(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    rti: pg_sys::Index,
    rte: *mut pg_sys::RangeTblEntry,
) {
    unsafe {
        // Chain the previous hook first
        let prev = PREV_HOOK.load(Ordering::SeqCst);
        if !prev.is_null() {
            let prev_fn: pg_sys::set_rel_pathlist_hook_type = Some(std::mem::transmute::<*mut (), unsafe extern "C-unwind" fn(*mut pg_sys::PlannerInfo, *mut pg_sys::RelOptInfo, u32, *mut pg_sys::RangeTblEntry)>(prev));
            if let Some(f) = prev_fn {
                f(root, rel, rti, rte);
            }
        }

        // Only handle regular tables
        if (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION {
            return;
        }

        // Check if this is the parent of a partitioned table (for SeaTurtleAppend)
        if (*rel).reloptkind == pg_sys::RelOptKind::RELOPT_BASEREL
            && (*rte).inh
            && let Some(companion_oids) = collect_compressed_children(root, rti)
        {
            path::add_seaturtle_append_path(root, rel, &companion_oids, std::ptr::null_mut());
            return;
        }

        // Only process base relations and child member relations (partitions)
        if (*rel).reloptkind != pg_sys::RelOptKind::RELOPT_BASEREL
            && (*rel).reloptkind != pg_sys::RelOptKind::RELOPT_OTHER_MEMBER_REL
        {
            return;
        }

        let rel_oid = (*rte).relid;

        // Check if this relation is a compressed partition
        let companion_oid = COMPRESSED_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            if let Some(&oid) = cache.get(&rel_oid) {
                return oid;
            }

            let oid = check_compressed_partition(rel_oid);
            cache.insert(rel_oid, oid);
            oid
        });

        if companion_oid == pg_sys::InvalidOid {
            return;
        }

        // For child partitions, check if we can advertise sorted output
        let pathkeys = if (*rel).reloptkind == pg_sys::RelOptKind::RELOPT_OTHER_MEMBER_REL {
            find_parent_oid(root, rti)
                .and_then(|parent_oid| get_time_column_attno(parent_oid))
                .map(|attno| check_time_pathkey(root, rel, attno))
                .unwrap_or(std::ptr::null_mut())
        } else {
            std::ptr::null_mut()
        };

        // Add the custom decompress path
        path::add_decompress_path(root, rel, companion_oid, pathkeys);
    }
}

/// The create_upper_paths hook. Detects aggregate patterns over seaturtle
/// scans and injects optimized custom paths:
/// - COUNT(*) alone → SeaTurtleCount (sum of segment row_counts, metadata-only)
/// - MIN/MAX(col) alone → SeaTurtleMinMax (global min/max from segment metadata)
/// - SUM/AVG/COUNT/COUNT(DISTINCT) with optional GROUP BY and WHERE → SeaTurtleAgg
#[pg_guard]
pub unsafe extern "C-unwind" fn seaturtle_create_upper_paths(
    root: *mut pg_sys::PlannerInfo,
    stage: pg_sys::UpperRelationKind::Type,
    input_rel: *mut pg_sys::RelOptInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    extra: *mut std::ffi::c_void,
) {
    unsafe {
        // Chain the previous hook first
        let prev = PREV_UPPER_HOOK.load(Ordering::SeqCst);
        if !prev.is_null() {
            type UpperHookFn = unsafe extern "C-unwind" fn(
                *mut pg_sys::PlannerInfo,
                pg_sys::UpperRelationKind::Type,
                *mut pg_sys::RelOptInfo,
                *mut pg_sys::RelOptInfo,
                *mut std::ffi::c_void,
            );
            let prev_fn: UpperHookFn = std::mem::transmute(prev);
            prev_fn(root, stage, input_rel, output_rel, extra);
        }

        // Only handle GROUP_AGG stage
        if stage != pg_sys::UpperRelationKind::UPPERREL_GROUP_AGG {
            return;
        }

        let parse = (*root).parse;

        // No HAVING (too complex)
        if !(*parse).havingQual.is_null() {
            return;
        }

        // Check for WHERE clause
        let has_where = {
            let jointree = (*parse).jointree;
            !jointree.is_null() && !(*jointree).quals.is_null()
        };

        // Check for GROUP BY
        let has_group_by = !(*parse).groupClause.is_null();

        // Check target list
        let tlist = (*parse).targetList;
        if tlist.is_null() {
            return;
        }

        let nentries = (*tlist).length;
        let mut aggrefs: Vec<*const pg_sys::Aggref> = Vec::new();
        let mut non_agg_vars: Vec<*const pg_sys::Var> = Vec::new();

        for i in 0..nentries {
            let te = pg_sys::list_nth(tlist, i) as *const pg_sys::TargetEntry;
            if te.is_null() {
                continue;
            }
            if (*te).resjunk {
                continue;
            }

            let expr = (*te).expr as *const pg_sys::Node;
            if expr.is_null() {
                return;
            }
            if (*expr).type_ == pg_sys::NodeTag::T_Aggref {
                aggrefs.push(expr as *const pg_sys::Aggref);
            } else if (*expr).type_ == pg_sys::NodeTag::T_Var && has_group_by {
                // Non-aggregate Var in target list — must be a GROUP BY column
                non_agg_vars.push(expr as *const pg_sys::Var);
            } else {
                return; // Non-aggregate, non-Var expression — bail
            }
        }

        if aggrefs.is_empty() {
            return;
        }

        // Extract companion OIDs from the cheapest input path
        let cheapest = (*input_rel).cheapest_total_path;
        if cheapest.is_null() {
            return;
        }

        let companion_oids = match extract_companion_oids(root, cheapest) {
            Some(oids) if !oids.is_empty() => oids,
            _ => return,
        };

        // =====================================================================
        // Fast path: Single COUNT(*) with no GROUP BY, no WHERE → SeaTurtleCount
        // =====================================================================
        if aggrefs.len() == 1 && (*aggrefs[0]).aggstar && !has_group_by && !has_where {
            path::add_count_star_path(root, output_rel, &companion_oids);
            return;
        }

        // =====================================================================
        // Classify all aggregates
        // =====================================================================
        use super::exec::AggType;

        let mut classified_aggs: Vec<path::AggSpec> = Vec::new();
        let mut all_minmax = true;
        let mut has_non_minmax = false;

        for &aggref in &aggrefs {
            // FILTER clause not supported
            if !(*aggref).aggfilter.is_null() {
                return;
            }

            if (*aggref).aggstar {
                // COUNT(*)
                classified_aggs.push(path::AggSpec {
                    agg_type: AggType::CountStar,
                    col_idx: -1,
                    result_type_oid: (*aggref).aggtype,
                    col_type_oid: pg_sys::InvalidOid,
                });
                all_minmax = false;
                has_non_minmax = true;
                continue;
            }

            // Get function name
            let func_name_ptr = pg_sys::get_func_name((*aggref).aggfnoid);
            if func_name_ptr.is_null() {
                return;
            }
            let func_name = std::ffi::CStr::from_ptr(func_name_ptr)
                .to_str()
                .unwrap_or("");

            // Must have exactly one argument
            let args = (*aggref).args;
            if args.is_null() || (*args).length != 1 {
                return;
            }

            // Extract the Var from the argument
            let arg_te = pg_sys::list_nth(args, 0) as *const pg_sys::TargetEntry;
            if arg_te.is_null() {
                return;
            }
            let arg_expr = (*arg_te).expr as *const pg_sys::Node;
            if arg_expr.is_null() || (*arg_expr).type_ != pg_sys::NodeTag::T_Var {
                return; // Only plain column references
            }
            let var_node = arg_expr as *const pg_sys::Var;
            let varattno = (*var_node).varattno;
            let col_idx = varattno as i32 - 1;

            // Get source column type
            let varno = (*var_node).varno as usize;
            if varno == 0 || varno >= (*root).simple_rel_array_size as usize {
                return;
            }
            let rte = *(*root).simple_rte_array.add(varno);
            if rte.is_null() {
                return;
            }
            let relid = (*rte).relid;
            let mut col_type_oid = pg_sys::InvalidOid;
            let mut col_typmod: i32 = -1;
            let mut col_collation: pg_sys::Oid = pg_sys::InvalidOid;
            pg_sys::get_atttypetypmodcoll(relid, varattno, &mut col_type_oid, &mut col_typmod, &mut col_collation);

            // Check for COUNT(DISTINCT ...)
            let is_distinct = !(*aggref).aggdistinct.is_null()
                && (*(*aggref).aggdistinct).length > 0;

            match func_name {
                "sum" => {
                    classified_aggs.push(path::AggSpec {
                        agg_type: AggType::Sum,
                        col_idx,
                        result_type_oid: (*aggref).aggtype,
                        col_type_oid,
                    });
                    all_minmax = false;
                    has_non_minmax = true;
                }
                "avg" => {
                    classified_aggs.push(path::AggSpec {
                        agg_type: AggType::Avg,
                        col_idx,
                        result_type_oid: (*aggref).aggtype,
                        col_type_oid,
                    });
                    all_minmax = false;
                    has_non_minmax = true;
                }
                "count" => {
                    if is_distinct {
                        classified_aggs.push(path::AggSpec {
                            agg_type: AggType::CountDistinct,
                            col_idx,
                            result_type_oid: (*aggref).aggtype,
                            col_type_oid,
                        });
                    } else {
                        classified_aggs.push(path::AggSpec {
                            agg_type: AggType::Count,
                            col_idx,
                            result_type_oid: (*aggref).aggtype,
                            col_type_oid,
                        });
                    }
                    all_minmax = false;
                    has_non_minmax = true;
                }
                "min" | "max" => {
                    if has_non_minmax {
                        // Mix of min/max with other aggs — use SeaTurtleAgg for all
                        // Mix of min/max with other aggs — skip MinMax fast path
                        return; // Bail — mixing MIN/MAX with SUM/COUNT not supported yet
                    }
                    classified_aggs.push(path::AggSpec {
                        agg_type: AggType::Sum, // placeholder, won't be used
                        col_idx,
                        result_type_oid: (*aggref).aggtype,
                        col_type_oid,
                    });
                    // Keep all_minmax = true
                }
                _ => return, // Unknown aggregate function
            }
        }

        if classified_aggs.is_empty() {
            return;
        }

        // =====================================================================
        // Fast path: All MIN/MAX, no GROUP BY, no WHERE → SeaTurtleMinMax
        // =====================================================================
        if all_minmax && !has_group_by && !has_where {
            let mut minmax_specs: Vec<path::MinMaxAggSpec> = Vec::new();
            for &aggref in &aggrefs {
                let func_name_ptr = pg_sys::get_func_name((*aggref).aggfnoid);
                let func_name = std::ffi::CStr::from_ptr(func_name_ptr)
                    .to_str()
                    .unwrap_or("");
                let is_min = func_name == "min";

                let args = (*aggref).args;
                let arg_te = pg_sys::list_nth(args, 0) as *const pg_sys::TargetEntry;
                let arg_expr = (*arg_te).expr as *const pg_sys::Var;
                let varattno = (*arg_expr).varattno;
                let varno = (*arg_expr).varno as usize;
                let rte = *(*root).simple_rte_array.add(varno);
                let relid = (*rte).relid;

                // Verify the companion table has _min_{colname}
                let col_name_ptr = pg_sys::get_attname(relid, varattno, true);
                if col_name_ptr.is_null() {
                    return;
                }
                let col_name = std::ffi::CStr::from_ptr(col_name_ptr)
                    .to_string_lossy()
                    .into_owned();
                let min_col_cname = std::ffi::CString::new(format!("_min_{}", col_name)).unwrap();
                let attnum = pg_sys::get_attnum(companion_oids[0], min_col_cname.as_ptr());
                if attnum == pg_sys::InvalidAttrNumber as i16 {
                    return;
                }

                let result_type_oid = (*aggref).aggtype;
                let mut typlen: i16 = 0;
                let mut typbyval: bool = false;
                pg_sys::get_typlenbyval(result_type_oid, &mut typlen, &mut typbyval);

                minmax_specs.push(path::MinMaxAggSpec {
                    is_min,
                    varattno,
                    result_type_oid,
                    typlen,
                    typbyval,
                });
            }

            if !minmax_specs.is_empty() {
                path::add_minmax_path(root, output_rel, &companion_oids, &minmax_specs);
            }
            return;
        }

        // =====================================================================
        // SeaTurtleAgg path: SUM/AVG/COUNT/COUNT(DISTINCT) ± GROUP BY (no WHERE)
        // =====================================================================

        // SeaTurtleAgg doesn't support WHERE clauses yet — fall through to the
        // standard SeaTurtleAppend + PG Aggregate path which handles quals correctly
        // via plan.qual. This ensures correctness for all WHERE patterns.
        if has_where {
            return;
        }

        // Parse GROUP BY columns
        let mut group_specs: Vec<super::exec::GroupByColSpec> = Vec::new();
        if has_group_by {
            let group_clause = (*parse).groupClause;
            let ngroups = (*group_clause).length;
            for i in 0..ngroups {
                let sc = pg_sys::list_nth(group_clause, i) as *const pg_sys::SortGroupClause;
                if sc.is_null() {
                    return;
                }
                // Find the TargetEntry for this sort group ref
                let tle = pg_sys::get_sortgroupclause_tle(
                    sc as *mut pg_sys::SortGroupClause,
                    tlist,
                );
                if tle.is_null() {
                    return;
                }
                let expr = (*tle).expr as *const pg_sys::Node;
                if expr.is_null() || (*expr).type_ != pg_sys::NodeTag::T_Var {
                    return; // Only plain column references for GROUP BY
                }
                let var_node = expr as *const pg_sys::Var;
                let col_idx = (*var_node).varattno as i32 - 1;

                // Get type from the Var
                let varno = (*var_node).varno as usize;
                if varno == 0 || varno >= (*root).simple_rel_array_size as usize {
                    return;
                }
                let rte = *(*root).simple_rte_array.add(varno);
                if rte.is_null() {
                    return;
                }
                let relid = (*rte).relid;
                let mut type_oid = pg_sys::InvalidOid;
                let mut typmod: i32 = -1;
                let mut collation: pg_sys::Oid = pg_sys::InvalidOid;
                pg_sys::get_atttypetypmodcoll(relid, (*var_node).varattno, &mut type_oid, &mut typmod, &mut collation);

                // Don't push down GROUP BY on text/varchar columns —
                // PG's HashAggregate is faster for high-cardinality string grouping.
                if type_oid == pg_sys::TEXTOID
                    || type_oid == pg_sys::VARCHAROID
                    || type_oid == pg_sys::BPCHAROID
                    || type_oid == pg_sys::NAMEOID
                {
                    return;
                }

                group_specs.push(super::exec::GroupByColSpec {
                    col_idx,
                    type_oid,
                });
            }
        }

        path::add_agg_path(
            root,
            output_rel,
            &companion_oids,
            &classified_aggs,
            &group_specs,
        );
    }
}

/// Extract companion OIDs from a planner path for COUNT(*) pushdown.
///
/// Handles:
/// - SeaTurtleDecompress/SeaTurtleAppend CustomPath: extract OIDs from custom_private
/// - AppendPath: walk subpaths, extract OIDs from SeaTurtleDecompress CustomPaths
///
/// Returns None if the path doesn't contain seaturtle scan nodes, or if there
/// are non-seaturtle subpaths with actual data (uncompressed partitions).
unsafe fn extract_companion_oids(
    root: *mut pg_sys::PlannerInfo,
    path: *const pg_sys::Path,
) -> Option<Vec<pg_sys::Oid>> {
    unsafe {
        if (*path).type_ == pg_sys::NodeTag::T_CustomPath {
            extract_oids_from_custom_path(path as *const pg_sys::CustomPath)
        } else if (*path).type_ == pg_sys::NodeTag::T_AppendPath {
            let append_path = path as *const pg_sys::AppendPath;
            let subpaths = (*append_path).subpaths;
            if subpaths.is_null() {
                return None;
            }
            let num_subpaths = (*subpaths).length;
            let mut oids = Vec::new();
            for i in 0..num_subpaths {
                let subpath = pg_sys::list_nth(subpaths, i) as *const pg_sys::Path;
                if subpath.is_null() {
                    continue;
                }
                if (*subpath).type_ == pg_sys::NodeTag::T_CustomPath {
                    let cpath = subpath as *const pg_sys::CustomPath;
                    if let Some(sub_oids) = extract_oids_from_custom_path(cpath) {
                        oids.extend(sub_oids);
                    } else if subpath_has_data(root, subpath) {
                        return None;
                    }
                } else if subpath_has_data(root, subpath) {
                    // Non-seaturtle subpath with actual data — can't push down
                    return None;
                }
                // Empty partition (relpages=0) — safe to skip
            }
            if oids.is_empty() { None } else { Some(oids) }
        } else {
            None
        }
    }
}

/// Check if a subpath's underlying table has actual data on disk.
///
/// Opens the relation and checks the actual block count via smgr,
/// which reflects the true on-disk state (not the stale pg_class.relpages
/// that only updates during VACUUM/ANALYZE).
unsafe fn subpath_has_data(
    root: *mut pg_sys::PlannerInfo,
    subpath: *const pg_sys::Path,
) -> bool {
    unsafe {
        let parent = (*subpath).parent;
        if parent.is_null() {
            return false;
        }
        // RelOptInfo.relid is the range table index (RTI)
        let rti = (*parent).relid;
        if rti == 0 {
            return false;
        }
        let rte = *(*root).simple_rte_array.add(rti as usize);
        if rte.is_null() {
            return false;
        }
        let rel_oid = (*rte).relid;
        // Open relation and check actual block count via smgr
        let rel = pg_sys::table_open(rel_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        let nblocks = pg_sys::RelationGetNumberOfBlocksInFork(
            rel,
            pg_sys::ForkNumber::MAIN_FORKNUM,
        );
        pg_sys::table_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        nblocks > 0
    }
}

/// Check if a single qual node is pushable to SeaTurtleAgg's batch filter.
///
/// Requirements (must ALL be true):
/// 1. Node is T_OpExpr with exactly 2 args
/// 2. Operator is =, <>, <, <=, >, >= (not LIKE, ~~, etc.)
/// 3. One arg is T_Var (or T_RelabelType wrapping T_Var), other is T_Const
/// 4. The Var's type is batch-comparable (numeric, bool, date, timestamp)
#[allow(dead_code)]
unsafe fn is_pushable_qual(node: *const pg_sys::Node) -> bool {
    unsafe {
        if node.is_null() || (*node).type_ != pg_sys::NodeTag::T_OpExpr {
            return false;
        }

        let opexpr = node as *const pg_sys::OpExpr;
        let args = (*opexpr).args;
        if args.is_null() || (*args).length != 2 {
            return false;
        }

        // Check operator is a supported comparison
        let opname_ptr = pg_sys::get_opname((*opexpr).opno);
        if opname_ptr.is_null() {
            return false;
        }
        let opname = std::ffi::CStr::from_ptr(opname_ptr)
            .to_str()
            .unwrap_or("");
        if !matches!(opname, "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") {
            return false;
        }

        let arg0 = (*(*args).elements.add(0)).ptr_value as *const pg_sys::Node;
        let arg1 = (*(*args).elements.add(1)).ptr_value as *const pg_sys::Node;
        if arg0.is_null() || arg1.is_null() {
            return false;
        }

        // Unwrap RelabelType to get the underlying node
        let unwrap = |n: *const pg_sys::Node| -> *const pg_sys::Node {
            if (*n).type_ == pg_sys::NodeTag::T_RelabelType {
                let rlt = n as *const pg_sys::RelabelType;
                (*rlt).arg as *const pg_sys::Node
            } else {
                n
            }
        };
        let a0 = unwrap(arg0);
        let a1 = unwrap(arg1);

        // Must be Var op Const or Const op Var
        let var_node = if (*a0).type_ == pg_sys::NodeTag::T_Var
            && (*a1).type_ == pg_sys::NodeTag::T_Const
        {
            a0 as *const pg_sys::Var
        } else if (*a0).type_ == pg_sys::NodeTag::T_Const
            && (*a1).type_ == pg_sys::NodeTag::T_Var
        {
            a1 as *const pg_sys::Var
        } else {
            return false;
        };

        // Check that the Var's type is batch-comparable
        let var_type = (*var_node).vartype;
        matches!(
            var_type,
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
}

/// Extract companion OIDs from a SeaTurtleDecompress or SeaTurtleAppend CustomPath.
unsafe fn extract_oids_from_custom_path(
    cpath: *const pg_sys::CustomPath,
) -> Option<Vec<pg_sys::Oid>> {
    unsafe {
        let methods = (*cpath).methods;
        if methods.is_null() {
            return None;
        }
        let name = std::ffi::CStr::from_ptr((*methods).CustomName);
        if name != super::SEATURTLE_APPEND_NAME && name != super::CUSTOM_NAME {
            return None;
        }
        let private_list = (*cpath).custom_private;
        if private_list.is_null() {
            return None;
        }
        let num_oids = (*private_list).length;
        let mut oids = Vec::new();
        for i in 0..num_oids {
            oids.push(pg_sys::list_nth_oid(private_list, i));
        }
        if oids.is_empty() { None } else { Some(oids) }
    }
}

/// Collect companion OIDs for all compressed children of a partitioned parent.
///
/// Iterates `root->append_rel_list` for children of `parent_rti`.
/// - If a child has a compressed companion, adds its OID to the list.
/// - If a child has no companion AND has uncompressed rows (reltuples > 0),
///   returns None (cannot use SeaTurtleAppend).
/// - Empty partitions (reltuples <= 0) are safely skipped.
///
/// Returns `Some(companion_oids)` if we found at least one compressed child
/// and no uncompressed data; `None` otherwise.
unsafe fn collect_compressed_children(
    root: *mut pg_sys::PlannerInfo,
    parent_rti: pg_sys::Index,
) -> Option<Vec<pg_sys::Oid>> {
    unsafe {
        let list = (*root).append_rel_list;
        if list.is_null() {
            return None;
        }

        let len = (*list).length;
        let mut companion_oids: Vec<pg_sys::Oid> = Vec::new();

        for i in 0..len {
            let node = pg_sys::list_nth(list, i) as *const pg_sys::AppendRelInfo;
            if node.is_null() {
                continue;
            }

            if (*node).parent_relid != parent_rti {
                continue;
            }

            let child_rti = (*node).child_relid;
            let child_rte = *(*root).simple_rte_array.add(child_rti as usize);
            let child_oid = (*child_rte).relid;

            // Check if this child has a compressed companion
            let companion_oid = COMPRESSED_CACHE.with(|cache| {
                let mut cache = cache.borrow_mut();
                if let Some(&oid) = cache.get(&child_oid) {
                    return oid;
                }
                let oid = check_compressed_partition(child_oid);
                cache.insert(child_oid, oid);
                oid
            });

            if companion_oid != pg_sys::InvalidOid {
                companion_oids.push(companion_oid);
            } else {
                // Not compressed — check if partition has data
                let reltuples = cost::get_reltuples(child_oid);
                if reltuples > 0.0 {
                    // Uncompressed partition with data — cannot use SeaTurtleAppend
                    return None;
                }
                // Empty partition, safe to skip
            }
        }

        if companion_oids.is_empty() {
            None
        } else {
            Some(companion_oids)
        }
    }
}

/// Check if a relation OID corresponds to a compressed partition
/// by looking for a companion table in _seaturtle_compressed schema.
pub(crate) unsafe fn check_compressed_partition(rel_oid: pg_sys::Oid) -> pg_sys::Oid {
    unsafe {
        // Get the relation name
        let name_ptr = pg_sys::get_rel_name(rel_oid);
        if name_ptr.is_null() {
            return pg_sys::InvalidOid;
        }
        let rel_name = std::ffi::CStr::from_ptr(name_ptr)
            .to_string_lossy()
            .into_owned();

        // Look up _seaturtle_compressed schema OID
        let schema_cstr = c"_seaturtle_compressed";
        let compressed_ns_oid = pg_sys::get_namespace_oid(schema_cstr.as_ptr(), true);
        if compressed_ns_oid == pg_sys::InvalidOid {
            return pg_sys::InvalidOid;
        }

        // Skip tables already in the _seaturtle_compressed schema to avoid recursion
        let rel_ns_oid = pg_sys::get_rel_namespace(rel_oid);
        if rel_ns_oid == compressed_ns_oid {
            return pg_sys::InvalidOid;
        }

        // Check if _seaturtle_compressed.<rel_name> exists
        let companion_cname = std::ffi::CString::new(rel_name).unwrap();
        pg_sys::get_relname_relid(companion_cname.as_ptr(), compressed_ns_oid)
    }
}

/// ExecutorStart hook: block DML on compressed partitions.
///
/// INSERT/UPDATE/DELETE on a compressed partition would silently produce
/// incorrect results (writes go to the truncated heap, reads come from the
/// companion table). This hook raises an error before execution begins.
#[pg_guard]
pub unsafe extern "C-unwind" fn seaturtle_executor_start(
    query_desc: *mut pg_sys::QueryDesc,
    eflags: c_int,
) {
    unsafe {
        let operation = (*query_desc).operation;

        // Only check DML commands
        if operation != pg_sys::CmdType::CMD_INSERT
            && operation != pg_sys::CmdType::CMD_UPDATE
            && operation != pg_sys::CmdType::CMD_DELETE
        {
            call_prev_executor_start(query_desc, eflags);
            return;
        }

        // Skip check when internal operations (e.g. decompress) set the bypass flag
        if DML_BYPASS.with(|flag| flag.get()) {
            call_prev_executor_start(query_desc, eflags);
            return;
        }

        let planned_stmt = (*query_desc).plannedstmt;
        if planned_stmt.is_null() {
            call_prev_executor_start(query_desc, eflags);
            return;
        }

        let result_relations = (*planned_stmt).resultRelations;
        if !result_relations.is_null() {
            let rtable = (*planned_stmt).rtable;
            let n = (*result_relations).length;

            for i in 0..n {
                // resultRelations is an IntList of 1-based RTE indices
                let rti = (*(*result_relations).elements.add(i as usize)).int_value;
                if rti <= 0 || rtable.is_null() {
                    continue;
                }

                // Get the RTE at this index (0-based in the list)
                let rte = pg_sys::list_nth(rtable, rti - 1) as *const pg_sys::RangeTblEntry;
                if rte.is_null() {
                    continue;
                }
                let relid = (*rte).relid;

                let companion_oid = COMPRESSED_CACHE.with(|cache| {
                    let mut cache = cache.borrow_mut();
                    if let Some(&oid) = cache.get(&relid) {
                        return oid;
                    }
                    let oid = check_compressed_partition(relid);
                    cache.insert(relid, oid);
                    oid
                });

                if companion_oid != pg_sys::InvalidOid {
                    let op_name = match operation {
                        pg_sys::CmdType::CMD_INSERT => "INSERT into",
                        pg_sys::CmdType::CMD_UPDATE => "UPDATE",
                        pg_sys::CmdType::CMD_DELETE => "DELETE from",
                        _ => "modify",
                    };
                    let rel_name_ptr = pg_sys::get_rel_name(relid);
                    let rel_name = if rel_name_ptr.is_null() {
                        format!("OID {}", relid)
                    } else {
                        std::ffi::CStr::from_ptr(rel_name_ptr)
                            .to_string_lossy()
                            .into_owned()
                    };
                    pgrx::error!(
                        "cannot {} compressed partition \"{}\", decompress it first",
                        op_name,
                        rel_name,
                    );
                }
            }
        }

        call_prev_executor_start(query_desc, eflags);
    }
}

/// Chain to the previous ExecutorStart hook or call standard_ExecutorStart.
unsafe fn call_prev_executor_start(query_desc: *mut pg_sys::QueryDesc, eflags: c_int) {
    unsafe {
        let prev = PREV_EXECUTOR_START_HOOK.load(Ordering::SeqCst);
        if !prev.is_null() {
            let prev_fn: unsafe extern "C-unwind" fn(*mut pg_sys::QueryDesc, c_int) =
                std::mem::transmute(prev);
            prev_fn(query_desc, eflags);
        } else {
            pg_sys::standard_ExecutorStart(query_desc, eflags);
        }
    }
}
