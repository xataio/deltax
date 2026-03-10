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

    /// Cache of parent table OID → whether segment_by is configured.
    static SEGMENT_BY_CACHE: std::cell::RefCell<HashMap<pg_sys::Oid, bool>> =
        std::cell::RefCell::new(HashMap::new());

    /// When true, the ExecutorStart hook skips the DML-on-compressed check.
    /// Used by internal operations like seaturtle_decompress_partition.
    static DML_BYPASS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub fn invalidate_compressed_cache() {
    COMPRESSED_CACHE.with(|cache| cache.borrow_mut().clear());
    TIME_COLUMN_CACHE.with(|cache| cache.borrow_mut().clear());
    SEGMENT_BY_CACHE.with(|cache| cache.borrow_mut().clear());
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

/// Check whether a hypertable has segment_by configured. When segment_by is
/// used, segments within a partition have overlapping time ranges, so we cannot
/// advertise sorted output via pathkeys. Result is cached.
unsafe fn has_segment_by(parent_oid: pg_sys::Oid) -> bool {
    let cached = SEGMENT_BY_CACHE.with(|cache| cache.borrow().get(&parent_oid).copied());
    if let Some(val) = cached {
        return val;
    }

    unsafe {
        let schema_name_ptr = pg_sys::get_namespace_name(pg_sys::get_rel_namespace(parent_oid));
        let table_name_ptr = pg_sys::get_rel_name(parent_oid);
        if schema_name_ptr.is_null() || table_name_ptr.is_null() {
            SEGMENT_BY_CACHE.with(|cache| cache.borrow_mut().insert(parent_oid, false));
            return false;
        }
        let schema_name = std::ffi::CStr::from_ptr(schema_name_ptr)
            .to_string_lossy()
            .into_owned();
        let table_name = std::ffi::CStr::from_ptr(table_name_ptr)
            .to_string_lossy()
            .into_owned();

        let result: bool = pgrx::Spi::connect(|client| {
            let result = client.select(
                "SELECT segment_by FROM seaturtle_hypertable WHERE schema_name = $1 AND table_name = $2",
                None,
                &[schema_name.as_str().into(), table_name.as_str().into()],
            );
            match result {
                Ok(mut table) => match table.next() {
                    Some(row) => row
                        .get_datum_by_ordinal(1)
                        .ok()
                        .and_then(|d| d.value::<Vec<String>>().ok())
                        .flatten()
                        .map(|v| !v.is_empty())
                        .unwrap_or(false),
                    None => false,
                },
                Err(_) => false,
            }
        });

        SEGMENT_BY_CACHE.with(|cache| cache.borrow_mut().insert(parent_oid, result));
        result
    }
}

/// Check if the first query pathkey matches the time column (ASC or DESC).
/// Returns `(pathkey_list, is_ascending)` — pathkey_list is null if no match.
unsafe fn check_time_pathkey(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    time_col_attno: i16,
) -> (*mut pg_sys::List, bool) {
    unsafe {
        let query_pathkeys = (*root).query_pathkeys;
        if query_pathkeys.is_null() || (*query_pathkeys).length == 0 {
            return (std::ptr::null_mut(), true);
        }

        let first_pk = pg_sys::list_nth(query_pathkeys, 0) as *mut pg_sys::PathKey;
        if first_pk.is_null() {
            return (std::ptr::null_mut(), true);
        }

        // Accept both ASC and DESC
        #[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
        let is_asc = (*first_pk).pk_strategy == pg_sys::BTLessStrategyNumber as i32;
        #[cfg(feature = "pg18")]
        let is_asc = (*first_pk).pk_cmptype == pg_sys::CompareType::COMPARE_LT;

        #[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
        let is_desc = (*first_pk).pk_strategy == pg_sys::BTGreaterStrategyNumber as i32;
        #[cfg(feature = "pg18")]
        let is_desc = (*first_pk).pk_cmptype == pg_sys::CompareType::COMPARE_GT;

        if !is_asc && !is_desc {
            return (std::ptr::null_mut(), true);
        }

        let eclass = (*first_pk).pk_eclass;
        if eclass.is_null() {
            return (std::ptr::null_mut(), true);
        }

        let members = (*eclass).ec_members;
        if members.is_null() {
            return (std::ptr::null_mut(), true);
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
                let pk_list = pg_sys::lappend(
                    std::ptr::null_mut(),
                    first_pk as *mut std::ffi::c_void,
                );
                return (pk_list, is_asc);
            }
        }

        (std::ptr::null_mut(), true)
    }
}

/// Check if the first query pathkey matches a given column attno.
/// Unlike `check_time_pathkey`, this does NOT require varno to match a specific
/// relation, so it works for both parent and child relations. It only checks
/// that the EC contains a Var with the given attno and that the pathkey is
/// ASC or DESC.
unsafe fn order_by_matches_column(
    root: *mut pg_sys::PlannerInfo,
    col_attno: i16,
) -> bool {
    unsafe {
        let query_pathkeys = (*root).query_pathkeys;
        if query_pathkeys.is_null() || (*query_pathkeys).length == 0 {
            return false;
        }
        let first_pk = pg_sys::list_nth(query_pathkeys, 0) as *mut pg_sys::PathKey;
        if first_pk.is_null() {
            return false;
        }
        let eclass = (*first_pk).pk_eclass;
        if eclass.is_null() {
            return false;
        }
        let members = (*eclass).ec_members;
        if members.is_null() {
            return false;
        }
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
            if (*var).varattno == col_attno {
                return true;
            }
        }
        false
    }
}

/// Extract Top-N info (effective LIMIT + sort direction) from the parse tree.
///
/// Returns `(effective_limit, sort_ascending)`:
/// - effective_limit = 0 means Top-N is disabled
/// - Only enabled when LIMIT is a constant integer ≤ 10000 and ORDER BY matches time column
unsafe fn extract_topn_info(
    root: *mut pg_sys::PlannerInfo,
    parse: *mut pg_sys::Query,
) -> (i64, bool) {
    unsafe {
        if parse.is_null() {
            return (0, true);
        }

        // Extract LIMIT (constant integer only)
        let limit_count: i64 = if !(*parse).limitCount.is_null() {
            let node = (*parse).limitCount as *const pg_sys::Node;
            if (*node).type_ == pg_sys::NodeTag::T_Const {
                let c = node as *const pg_sys::Const;
                if !(*c).constisnull {
                    (*c).constvalue.value() as i64
                } else {
                    0
                }
            } else {
                0
            }
        } else {
            0
        };

        if limit_count <= 0 {
            return (0, true);
        }

        // Extract OFFSET if present, add to limit
        let offset: i64 = if !(*parse).limitOffset.is_null() {
            let node = (*parse).limitOffset as *const pg_sys::Node;
            if (*node).type_ == pg_sys::NodeTag::T_Const {
                let c = node as *const pg_sys::Const;
                if !(*c).constisnull {
                    (*c).constvalue.value() as i64
                } else {
                    0
                }
            } else {
                // Non-constant OFFSET — disable Top-N
                return (0, true);
            }
        } else {
            0
        };

        let effective_limit = limit_count + offset;

        // Cap at 10000 — beyond that, overhead not worth it
        if effective_limit > 10000 {
            return (0, true);
        }

        // Check if ORDER BY matches time column (ASC or DESC).
        // Only single-column ORDER BY: multi-column ORDER BY (e.g. ORDER BY
        // EventTime, SearchPhrase) can't be satisfied by sorting on time alone.
        let query_pathkeys = (*root).query_pathkeys;
        if query_pathkeys.is_null() || (*query_pathkeys).length != 1 {
            return (0, true);
        }

        let first_pk = pg_sys::list_nth(query_pathkeys, 0) as *mut pg_sys::PathKey;
        if first_pk.is_null() {
            return (0, true);
        }

        #[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
        let is_asc = (*first_pk).pk_strategy == pg_sys::BTLessStrategyNumber as i32;
        #[cfg(feature = "pg18")]
        let is_asc = (*first_pk).pk_cmptype == pg_sys::CompareType::COMPARE_LT;

        #[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
        let is_desc = (*first_pk).pk_strategy == pg_sys::BTGreaterStrategyNumber as i32;
        #[cfg(feature = "pg18")]
        let is_desc = (*first_pk).pk_cmptype == pg_sys::CompareType::COMPARE_GT;

        if !is_asc && !is_desc {
            return (0, true);
        }

        (effective_limit, is_asc)
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

        // Extract LIMIT/OFFSET from parse tree for Top-N optimization
        let parse = (*root).parse;
        let (effective_limit, sort_ascending) = extract_topn_info(root, parse);

        // Check if this is the parent of a partitioned table (for SeaTurtleAppend)
        if (*rel).reloptkind == pg_sys::RelOptKind::RELOPT_BASEREL
            && (*rte).inh
            && let Some(companion_oids) = collect_compressed_children(root, rti)
        {
            // For Top-N, validate ORDER BY matches the time column
            let append_topn_limit = if effective_limit > 0 {
                get_time_column_attno((*rte).relid)
                    .map(|attno| {
                        if order_by_matches_column(root, attno) {
                            effective_limit
                        } else {
                            0
                        }
                    })
                    .unwrap_or(0)
            } else {
                0
            };
            path::add_seaturtle_append_path(root, rel, &companion_oids, std::ptr::null_mut(), append_topn_limit, sort_ascending);
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
        // and whether Top-N is valid.
        let parent_oid_opt = if (*rel).reloptkind == pg_sys::RelOptKind::RELOPT_OTHER_MEMBER_REL {
            find_parent_oid(root, rti)
        } else {
            None
        };
        let time_col_attno_opt = parent_oid_opt.and_then(|oid| get_time_column_attno(oid));
        let has_segby = parent_oid_opt.map(|oid| has_segment_by(oid)).unwrap_or(false);

        // Pathkeys for sorted output: only when no segment_by and time pathkey matches
        let (pathkeys, _sort_ascending) = if !has_segby {
            time_col_attno_opt
                .map(|attno| check_time_pathkey(root, rel, attno))
                .unwrap_or((std::ptr::null_mut(), true))
        } else {
            (std::ptr::null_mut(), true)
        };

        // Top-N: enabled when ORDER BY matches time column (works for both
        // parent and child rels, and regardless of segment_by)
        let topn_effective_limit = if effective_limit > 0 {
            time_col_attno_opt
                .map(|attno| {
                    if order_by_matches_column(root, attno) {
                        effective_limit
                    } else {
                        0
                    }
                })
                .unwrap_or(0)
        } else {
            0
        };

        // Add the custom decompress path
        path::add_decompress_path(root, rel, companion_oid, pathkeys, topn_effective_limit, sort_ascending);
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

        // Check for WHERE clause.
        // Primary: parse->jointree->quals. Fallback: baserestrictinfo,
        // which PG always populates even if jointree quals get cleared.
        let has_where = {
            let jointree = (*parse).jointree;
            let jointree_has_quals = !jointree.is_null() && !(*jointree).quals.is_null();
            if jointree_has_quals {
                true
            } else {
                // Fallback: check baserestrictinfo on base relations
                let mut found = false;
                let array_size = (*root).simple_rel_array_size;
                for rti in 1..array_size {
                    let rel = *(*root).simple_rel_array.add(rti as usize);
                    if !rel.is_null() {
                        let bri = (*rel).baserestrictinfo;
                        if !bri.is_null() && (*bri).length > 0 {
                            found = true;
                            break;
                        }
                    }
                }
                found
            }
        };

        // Check for HAVING clause
        let has_having = !(*parse).havingQual.is_null();

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
        use super::exec::{AggType, AggExpr};

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
                    expr_kind: AggExpr::Column,
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

            // Extract the Var from the argument (plain Var or length(Var))
            let arg_te = pg_sys::list_nth(args, 0) as *const pg_sys::TargetEntry;
            if arg_te.is_null() {
                return;
            }
            let arg_expr = (*arg_te).expr as *const pg_sys::Node;
            if arg_expr.is_null() {
                return;
            }

            let (var_node, expr_kind): (*const pg_sys::Var, AggExpr) = if (*arg_expr).type_ == pg_sys::NodeTag::T_Var {
                (arg_expr as *const pg_sys::Var, AggExpr::Column)
            } else if (*arg_expr).type_ == pg_sys::NodeTag::T_FuncExpr {
                // Check for length(Var)
                let funcexpr = arg_expr as *const pg_sys::FuncExpr;
                let fn_name_ptr = pg_sys::get_func_name((*funcexpr).funcid);
                if fn_name_ptr.is_null() {
                    return;
                }
                let fn_name = std::ffi::CStr::from_ptr(fn_name_ptr)
                    .to_str()
                    .unwrap_or("");
                if fn_name != "length" {
                    return; // Only length() is supported
                }
                let fn_args = (*funcexpr).args;
                if fn_args.is_null() || (*fn_args).length != 1 {
                    return;
                }
                let inner = (*(*fn_args).elements.add(0)).ptr_value as *const pg_sys::Node;
                if inner.is_null() || (*inner).type_ != pg_sys::NodeTag::T_Var {
                    return;
                }
                (inner as *const pg_sys::Var, AggExpr::LengthOf)
            } else {
                return; // Only plain column references or length(col)
            };

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

            // For length() expressions, the effective type for aggregation is INT4
            let effective_col_type_oid = if expr_kind == AggExpr::LengthOf {
                pg_sys::INT4OID
            } else {
                col_type_oid
            };

            // Check for COUNT(DISTINCT ...)
            let is_distinct = !(*aggref).aggdistinct.is_null()
                && (*(*aggref).aggdistinct).length > 0;

            match func_name {
                "sum" => {
                    classified_aggs.push(path::AggSpec {
                        agg_type: AggType::Sum,
                        col_idx,
                        result_type_oid: (*aggref).aggtype,
                        col_type_oid: effective_col_type_oid,
                        expr_kind,
                    });
                    all_minmax = false;
                    has_non_minmax = true;
                }
                "avg" => {
                    classified_aggs.push(path::AggSpec {
                        agg_type: AggType::Avg,
                        col_idx,
                        result_type_oid: (*aggref).aggtype,
                        col_type_oid: effective_col_type_oid,
                        expr_kind,
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
                            col_type_oid: effective_col_type_oid,
                            expr_kind,
                        });
                    } else {
                        classified_aggs.push(path::AggSpec {
                            agg_type: AggType::Count,
                            col_idx,
                            result_type_oid: (*aggref).aggtype,
                            col_type_oid: effective_col_type_oid,
                            expr_kind,
                        });
                    }
                    all_minmax = false;
                    has_non_minmax = true;
                }
                "min" | "max" => {
                    if has_non_minmax {
                        return; // Bail — mixing MIN/MAX with SUM/COUNT not supported yet
                    }
                    classified_aggs.push(path::AggSpec {
                        agg_type: AggType::Sum, // placeholder, won't be used
                        col_idx,
                        result_type_oid: (*aggref).aggtype,
                        col_type_oid: effective_col_type_oid,
                        expr_kind,
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
        // SeaTurtleAgg path: SUM/AVG/COUNT/COUNT(DISTINCT) ± GROUP BY ± WHERE ± HAVING
        // =====================================================================

        // Verify all WHERE quals are batch-pushable.  Each qual must be
        // extractable by extract_batch_quals at execution time, otherwise the
        // filter is silently dropped and AggScan produces wrong results.
        if has_where {
            // Get qual nodes from jointree->quals if available, otherwise from baserestrictinfo
            let qual_nodes: Vec<*const pg_sys::Node> = {
                let jointree = (*parse).jointree;
                if !jointree.is_null() && !(*jointree).quals.is_null() {
                    let quals_node = (*jointree).quals as *const pg_sys::Node;
                    if (*quals_node).type_ == pg_sys::NodeTag::T_List {
                        let list = quals_node as *const pg_sys::List;
                        (0..(*list).length)
                            .map(|i| pg_sys::list_nth(list as *mut _, i) as *const pg_sys::Node)
                            .collect()
                    } else {
                        let list = pg_sys::make_ands_implicit(quals_node as *mut pg_sys::Expr);
                        (0..(*list).length)
                            .map(|i| pg_sys::list_nth(list, i) as *const pg_sys::Node)
                            .collect()
                    }
                } else {
                    // Fallback: extract from baserestrictinfo
                    let mut nodes = Vec::new();
                    let array_size = (*root).simple_rel_array_size;
                    for rti in 1..array_size {
                        let rel = *(*root).simple_rel_array.add(rti as usize);
                        if rel.is_null() { continue; }
                        let bri = (*rel).baserestrictinfo;
                        if bri.is_null() { continue; }
                        for i in 0..(*bri).length {
                            let ri = pg_sys::list_nth(bri, i) as *const pg_sys::RestrictInfo;
                            if !ri.is_null() && !(*ri).clause.is_null() {
                                nodes.push((*ri).clause as *const pg_sys::Node);
                            }
                        }
                        if !nodes.is_empty() { break; }
                    }
                    nodes
                }
            };

            let unwrap_relabel = |n: *const pg_sys::Node| -> *const pg_sys::Node {
                if (*n).type_ == pg_sys::NodeTag::T_RelabelType {
                    let rlt = n as *const pg_sys::RelabelType;
                    (*rlt).arg as *const pg_sys::Node
                } else {
                    n
                }
            };

            for &qn in &qual_nodes {
                if qn.is_null() {
                    return;
                }
                let qt = (*qn).type_;
                match qt {
                    pg_sys::NodeTag::T_OpExpr => {
                        // Validate exactly as extract_batch_quals would.
                        let opexpr = qn as *const pg_sys::OpExpr;
                        let args = (*opexpr).args;
                        if args.is_null() || (*args).length != 2 {
                            return;
                        }

                        let opname_ptr = pg_sys::get_opname((*opexpr).opno);
                        if opname_ptr.is_null() {
                            return;
                        }
                        let opname = std::ffi::CStr::from_ptr(opname_ptr)
                            .to_str()
                            .unwrap_or("");

                        let is_like = opname == "~~";
                        let is_not_like = opname == "!~~";
                        let is_recognized_cmp = matches!(opname, "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=");

                        if !is_like && !is_not_like && !is_recognized_cmp {
                            return; // unrecognized operator
                        }

                        let raw_arg0 = (*(*args).elements.add(0)).ptr_value as *const pg_sys::Node;
                        let raw_arg1 = (*(*args).elements.add(1)).ptr_value as *const pg_sys::Node;
                        if raw_arg0.is_null() || raw_arg1.is_null() {
                            return;
                        }

                        let a0 = unwrap_relabel(raw_arg0);
                        let a1 = unwrap_relabel(raw_arg1);

                        let (var_node, const_node, var_on_left) =
                            if (*a0).type_ == pg_sys::NodeTag::T_Var
                                && (*a1).type_ == pg_sys::NodeTag::T_Const
                            {
                                (a0 as *const pg_sys::Var, a1 as *const pg_sys::Const, true)
                            } else if (*a0).type_ == pg_sys::NodeTag::T_Const
                                && (*a1).type_ == pg_sys::NodeTag::T_Var
                            {
                                (a1 as *const pg_sys::Var, a0 as *const pg_sys::Const, false)
                            } else {
                                return; // neither (Var,Const) nor (Const,Var)
                            };

                        if (*const_node).constisnull {
                            return;
                        }

                        let type_oid = (*var_node).vartype;

                        if is_like || is_not_like {
                            if !var_on_left {
                                return;
                            }
                            if !matches!(type_oid, pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID) {
                                return;
                            }
                        } else if matches!(type_oid, pg_sys::TEXTOID | pg_sys::VARCHAROID)
                            && matches!(opname, "=" | "<>" | "!=")
                        {
                            if !var_on_left {
                                return;
                            }
                        } else {
                            // Numeric/date/bool comparison — check type is supported
                            if !matches!(
                                type_oid,
                                pg_sys::INT2OID
                                    | pg_sys::INT4OID
                                    | pg_sys::INT8OID
                                    | pg_sys::FLOAT4OID
                                    | pg_sys::FLOAT8OID
                                    | pg_sys::BOOLOID
                                    | pg_sys::DATEOID
                                    | pg_sys::TIMESTAMPOID
                                    | pg_sys::TIMESTAMPTZOID
                            ) {
                                return;
                            }
                        }
                    }
                    pg_sys::NodeTag::T_Var => {
                        let var_node = qn as *const pg_sys::Var;
                        if (*var_node).vartype != pg_sys::BOOLOID {
                            return;
                        }
                    }
                    pg_sys::NodeTag::T_BoolExpr => {
                        let boolexpr = qn as *const pg_sys::BoolExpr;
                        if (*boolexpr).boolop == pg_sys::BoolExprType::NOT_EXPR {
                            let bargs = (*boolexpr).args;
                            if bargs.is_null() || (*bargs).length != 1 {
                                return;
                            }
                            let inner = (*(*bargs).elements.add(0)).ptr_value as *const pg_sys::Node;
                            if inner.is_null() || (*inner).type_ != pg_sys::NodeTag::T_Var {
                                return;
                            }
                            let inner_var = inner as *const pg_sys::Var;
                            if (*inner_var).vartype != pg_sys::BOOLOID {
                                return;
                            }
                        } else if (*boolexpr).boolop == pg_sys::BoolExprType::AND_EXPR {
                            // AND — quals should already be flattened, but allow it
                        } else {
                            return; // OR in WHERE — not pushable
                        }
                    }
                    _ => {
                        return; // Unknown qual type — bail
                    }
                }
            }
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

        // Parse HAVING clause into simple filters
        let mut having_filters: Vec<super::exec::HavingFilter> = Vec::new();
        if has_having {
            use super::exec::{HavingOp, HavingFilter};
            let having_node = (*parse).havingQual as *const pg_sys::Node;
            // Collect qual nodes (single OpExpr or AND-list)
            let qual_nodes: Vec<*const pg_sys::Node> = if (*having_node).type_ == pg_sys::NodeTag::T_BoolExpr {
                let boolexpr = having_node as *const pg_sys::BoolExpr;
                if (*boolexpr).boolop == pg_sys::BoolExprType::AND_EXPR {
                    let args = (*boolexpr).args;
                    let n = (*args).length;
                    (0..n).map(|i| pg_sys::list_nth(args, i) as *const pg_sys::Node).collect()
                } else {
                    return; // OR/NOT in HAVING not supported
                }
            } else {
                vec![having_node]
            };

            for &qnode in &qual_nodes {
                if (*qnode).type_ != pg_sys::NodeTag::T_OpExpr {
                    return; // Non-OpExpr HAVING not supported
                }
                let opexpr = qnode as *const pg_sys::OpExpr;
                let hargs = (*opexpr).args;
                if hargs.is_null() || (*hargs).length != 2 {
                    return;
                }

                let opname_ptr = pg_sys::get_opname((*opexpr).opno);
                if opname_ptr.is_null() {
                    return;
                }
                let opname = std::ffi::CStr::from_ptr(opname_ptr)
                    .to_str()
                    .unwrap_or("");
                let having_op = match opname {
                    ">" => HavingOp::Gt,
                    "<" => HavingOp::Lt,
                    ">=" => HavingOp::Ge,
                    "<=" => HavingOp::Le,
                    "=" => HavingOp::Eq,
                    "<>" | "!=" => HavingOp::Ne,
                    _ => return,
                };

                let a0 = (*(*hargs).elements.add(0)).ptr_value as *const pg_sys::Node;
                let a1 = (*(*hargs).elements.add(1)).ptr_value as *const pg_sys::Node;

                // Must be Aggref op Const or Const op Aggref
                let (aggref_node, const_node, agg_on_left) =
                    if (*a0).type_ == pg_sys::NodeTag::T_Aggref
                        && (*a1).type_ == pg_sys::NodeTag::T_Const
                    {
                        (a0 as *const pg_sys::Aggref, a1 as *const pg_sys::Const, true)
                    } else if (*a0).type_ == pg_sys::NodeTag::T_Const
                        && (*a1).type_ == pg_sys::NodeTag::T_Aggref
                    {
                        (a1 as *const pg_sys::Aggref, a0 as *const pg_sys::Const, false)
                    } else {
                        return;
                    };

                // For the Const op Aggref case, flip the comparison direction
                let final_op = if !agg_on_left {
                    match having_op {
                        HavingOp::Gt => HavingOp::Lt,
                        HavingOp::Lt => HavingOp::Gt,
                        HavingOp::Ge => HavingOp::Le,
                        HavingOp::Le => HavingOp::Ge,
                        other => other,
                    }
                } else {
                    having_op
                };

                if (*const_node).constisnull {
                    return;
                }
                let const_val = (*const_node).constvalue.value() as i64;

                // Match the Aggref to a classified agg by position.
                // Walk aggrefs in order and find which one matches this HAVING aggref.
                let mut agg_idx = None;
                for (i, &ar) in aggrefs.iter().enumerate() {
                    if std::ptr::eq(ar, aggref_node) {
                        agg_idx = Some(i);
                        break;
                    }
                }
                // If not found by pointer, match by aggfnoid + aggstar
                if agg_idx.is_none() {
                    for (i, &ar) in aggrefs.iter().enumerate() {
                        if (*ar).aggfnoid == (*aggref_node).aggfnoid
                            && (*ar).aggstar == (*aggref_node).aggstar
                        {
                            // For non-star, also match args
                            if (*ar).aggstar {
                                agg_idx = Some(i);
                                break;
                            }
                            // Match by column: compare first arg's Var
                            let ar_args = (*ar).args;
                            let h_args = (*aggref_node).args;
                            if !ar_args.is_null() && !h_args.is_null()
                                && (*ar_args).length == 1 && (*h_args).length == 1
                            {
                                let ar_te = pg_sys::list_nth(ar_args, 0) as *const pg_sys::TargetEntry;
                                let h_te = pg_sys::list_nth(h_args, 0) as *const pg_sys::TargetEntry;
                                let ar_expr = (*ar_te).expr as *const pg_sys::Node;
                                let h_expr = (*h_te).expr as *const pg_sys::Node;
                                if (*ar_expr).type_ == pg_sys::NodeTag::T_Var
                                    && (*h_expr).type_ == pg_sys::NodeTag::T_Var
                                {
                                    let ar_var = ar_expr as *const pg_sys::Var;
                                    let h_var = h_expr as *const pg_sys::Var;
                                    if (*ar_var).varattno == (*h_var).varattno {
                                        agg_idx = Some(i);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                match agg_idx {
                    Some(idx) => {
                        having_filters.push(HavingFilter {
                            agg_idx: idx,
                            op: final_op,
                            const_val,
                        });
                    }
                    None => return, // Can't match HAVING aggref — bail
                }
            }
        }

        path::add_agg_path(
            root,
            output_rel,
            &companion_oids,
            &classified_aggs,
            &group_specs,
            &having_filters,
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
