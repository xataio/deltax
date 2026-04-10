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

    /// Cache of parent table OID → time column attribute number (0 = not a deltatable).
    static TIME_COLUMN_CACHE: std::cell::RefCell<HashMap<pg_sys::Oid, i16>> =
        std::cell::RefCell::new(HashMap::new());

    /// Cache of parent table OID → whether segment_by is configured.
    static SEGMENT_BY_CACHE: std::cell::RefCell<HashMap<pg_sys::Oid, bool>> =
        std::cell::RefCell::new(HashMap::new());

    /// When true, the ExecutorStart hook skips the DML-on-compressed check.
    /// Used by internal operations like deltax_decompress_partition.
    static DML_BYPASS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub fn invalidate_compressed_cache() {
    COMPRESSED_CACHE.with(|cache| cache.borrow_mut().clear());
    TIME_COLUMN_CACHE.with(|cache| cache.borrow_mut().clear());
    SEGMENT_BY_CACHE.with(|cache| cache.borrow_mut().clear());
    cost::invalidate_caches();
}

/// Set or clear the DML bypass flag for internal operations.
pub(crate) fn set_dml_bypass(bypass: bool) {
    DML_BYPASS.with(|flag| flag.set(bypass));
}

/// Get the time column's attribute number for a deltatable parent table.
/// Returns None if the table is not a deltax deltatable. Result is cached.
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
                "SELECT time_column FROM deltax_deltatable WHERE schema_name = $1 AND table_name = $2",
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

/// Check whether a deltatable has segment_by configured. When segment_by is
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
                "SELECT segment_by FROM deltax_deltatable WHERE schema_name = $1 AND table_name = $2",
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

/// Extract the first ORDER BY column's attribute number from pathkeys.
/// Returns the 1-based attno, or None if ORDER BY is not a simple column reference.
unsafe fn extract_order_by_attno(root: *mut pg_sys::PlannerInfo) -> Option<i16> {
    unsafe {
        let query_pathkeys = (*root).query_pathkeys;
        if query_pathkeys.is_null() || (*query_pathkeys).length == 0 {
            return None;
        }
        let first_pk = pg_sys::list_nth(query_pathkeys, 0) as *mut pg_sys::PathKey;
        if first_pk.is_null() {
            return None;
        }
        let eclass = (*first_pk).pk_eclass;
        if eclass.is_null() {
            return None;
        }
        let members = (*eclass).ec_members;
        if members.is_null() {
            return None;
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
            return Some((*var).varattno);
        }
        None
    }
}

/// Extract Top-N info (effective LIMIT + sort direction) from the parse tree.
///
/// Returns `(effective_limit, sort_ascending, multi_col_sort)`:
/// - effective_limit = 0 means Top-N is disabled
/// - multi_col_sort = true when ORDER BY has multiple columns (first must be time)
/// - Only enabled when LIMIT is a constant integer ≤ 10000 and ORDER BY matches time column
unsafe fn extract_topn_info(
    root: *mut pg_sys::PlannerInfo,
    parse: *mut pg_sys::Query,
) -> (i64, bool, bool) {
    unsafe {
        if parse.is_null() {
            return (0, true, false);
        }

        // Scan-level top-N makes no sense for aggregate queries — the aggregate
        // needs all rows from the scan.  (The DeltaXAgg upper path has its own
        // top-N logic.)
        if (*parse).hasAggs || !(*parse).groupClause.is_null() {
            return (0, true, false);
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
            return (0, true, false);
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
                return (0, true, false);
            }
        } else {
            0
        };

        let effective_limit = limit_count + offset;

        // Cap at 10000 — beyond that, overhead not worth it
        if effective_limit > 10000 {
            return (0, true, false);
        }

        // Check if ORDER BY has at least one pathkey and the first is the time column.
        // Multi-column ORDER BY is supported: we use the time column for segment
        // skipping and threshold, PG's Sort node handles the full multi-column sort.
        let query_pathkeys = (*root).query_pathkeys;
        if query_pathkeys.is_null() || (*query_pathkeys).length < 1 {
            return (0, true, false);
        }

        let multi_col_sort = (*query_pathkeys).length > 1;

        let first_pk = pg_sys::list_nth(query_pathkeys, 0) as *mut pg_sys::PathKey;
        if first_pk.is_null() {
            return (0, true, false);
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
            return (0, true, false);
        }

        (effective_limit, is_asc, multi_col_sort)
    }
}

/// The planner hook. Called for each relation during path generation.
#[pg_guard]
pub unsafe extern "C-unwind" fn deltax_set_rel_pathlist(
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
        let (effective_limit, sort_ascending, multi_col_sort) = extract_topn_info(root, parse);

        // Check if this is the parent of a partitioned table (for DeltaXAppend)
        if (*rel).reloptkind == pg_sys::RelOptKind::RELOPT_BASEREL
            && (*rte).inh
            && let Some(companion_oids) = collect_compressed_children(root, rti)
        {
            // For Top-N, validate ORDER BY is a simple column reference.
            // Works for any column (time, text, numeric).
            let (append_topn_limit, append_sort_col_attno) = if effective_limit > 0 {
                if let Some(attno) = extract_order_by_attno(root) {
                    (effective_limit, attno as i32)
                } else {
                    (0, 0)
                }
            } else {
                (0, 0)
            };
            let append_multi_col = if append_topn_limit > 0 { multi_col_sort } else { false };
            path::add_deltax_append_path(root, rel, &companion_oids, std::ptr::null_mut(), append_topn_limit, sort_ascending, append_multi_col, append_sort_col_attno);
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

        // Top-N: enabled when ORDER BY is a simple column reference.
        // Works for any column (time, text, numeric).
        let (topn_effective_limit, topn_sort_col_attno) = if effective_limit > 0 {
            if let Some(attno) = extract_order_by_attno(root) {
                (effective_limit, attno as i32)
            } else {
                (0, 0)
            }
        } else {
            (0, 0)
        };

        // Add the custom decompress path
        let topn_multi_col = if topn_effective_limit > 0 { multi_col_sort } else { false };
        path::add_decompress_path(root, rel, companion_oid, pathkeys, topn_effective_limit, sort_ascending, topn_multi_col, topn_sort_col_attno);
    }
}

/// Unwrap a RelabelType node to get the inner expression.
unsafe fn unwrap_relabel_node(n: *const pg_sys::Node) -> *const pg_sys::Node {
    unsafe {
        if !n.is_null() && (*n).type_ == pg_sys::NodeTag::T_RelabelType {
            let rlt = n as *const pg_sys::RelabelType;
            (*rlt).arg as *const pg_sys::Node
        } else {
            n
        }
    }
}

/// Parse a CASE WHEN expression from the PG node tree into a CaseWhenSpec.
/// Only supports searched CASE (no simple CASE), integer equality/inequality conditions,
/// AND-combined conditions, and text column refs or string constants as results.
/// Returns None if any part is unsupported.
unsafe fn parse_case_expr(
    root: *mut pg_sys::PlannerInfo,
    case_expr: *const pg_sys::CaseExpr,
) -> Option<super::exec::CaseWhenSpec> {
    unsafe {
    use super::exec::{CaseWhenSpec, CaseWhenClause, CaseWhenValue};

    // Must be searched CASE (arg is null), not simple CASE
    if !(*case_expr).arg.is_null() {
        return None;
    }

    let args_list = (*case_expr).args;
    if args_list.is_null() || (*args_list).length == 0 {
        return None;
    }

    let mut clauses: Vec<CaseWhenClause> = Vec::new();
    let nargs = (*args_list).length;
    for i in 0..nargs {
        let when_node = (*(*args_list).elements.add(i as usize)).ptr_value as *const pg_sys::Node;
        if when_node.is_null() || (*when_node).type_ != pg_sys::NodeTag::T_CaseWhen {
            return None;
        }
        let case_when = when_node as *const pg_sys::CaseWhen;

        // Parse conditions from the WHEN expr
        let conditions = parse_case_when_conditions(root, (*case_when).expr as *const pg_sys::Node)?;
        if conditions.is_empty() {
            return None;
        }

        // Parse the THEN result
        let result = parse_case_when_value(root, (*case_when).result as *const pg_sys::Node)?;

        clauses.push(CaseWhenClause { conditions, result });
    }

    // Parse the ELSE (default) result
    let default = if (*case_expr).defresult.is_null() {
        CaseWhenValue::StringConst(String::new()) // implicit ELSE NULL → treat as empty string
    } else {
        parse_case_when_value(root, (*case_expr).defresult as *const pg_sys::Node)?
    };

    Some(CaseWhenSpec { clauses, default })
    }
}

/// Parse conditions from a CASE WHEN clause's expr node.
/// Supports: single OpExpr(col op const) or BoolExpr(AND, [OpExpr, ...])
unsafe fn parse_case_when_conditions(
    root: *mut pg_sys::PlannerInfo,
    expr: *const pg_sys::Node,
) -> Option<Vec<super::exec::CaseWhenCondition>> {
    unsafe {
    if expr.is_null() {
        return None;
    }

    if (*expr).type_ == pg_sys::NodeTag::T_BoolExpr {
        let bool_expr = expr as *const pg_sys::BoolExpr;
        if (*bool_expr).boolop != pg_sys::BoolExprType::AND_EXPR {
            return None; // Only AND is supported
        }
        let args = (*bool_expr).args;
        if args.is_null() || (*args).length == 0 {
            return None;
        }
        let mut conditions = Vec::new();
        for i in 0..(*args).length {
            let arg = (*(*args).elements.add(i as usize)).ptr_value as *const pg_sys::Node;
            let cond = parse_single_condition(root, arg)?;
            conditions.push(cond);
        }
        Some(conditions)
    } else if (*expr).type_ == pg_sys::NodeTag::T_OpExpr {
        let cond = parse_single_condition(root, expr)?;
        Some(vec![cond])
    } else {
        None
    }
    }
}

/// Parse a single OpExpr condition: col = const or col <> const (integer).
unsafe fn parse_single_condition(
    _root: *mut pg_sys::PlannerInfo,
    expr: *const pg_sys::Node,
) -> Option<super::exec::CaseWhenCondition> {
    unsafe {
    use super::exec::{CaseWhenCondition, CaseWhenOp};

    if expr.is_null() || (*expr).type_ != pg_sys::NodeTag::T_OpExpr {
        return None;
    }
    let opexpr = expr as *const pg_sys::OpExpr;
    let opname_ptr = pg_sys::get_opname((*opexpr).opno);
    if opname_ptr.is_null() {
        return None;
    }
    let opname = std::ffi::CStr::from_ptr(opname_ptr).to_str().unwrap_or("");
    let op = match opname {
        "=" => CaseWhenOp::Eq,
        "<>" => CaseWhenOp::NotEq,
        _ => return None,
    };

    let args = (*opexpr).args;
    if args.is_null() || (*args).length != 2 {
        return None;
    }
    let left = (*(*args).elements.add(0)).ptr_value as *const pg_sys::Node;
    let right = (*(*args).elements.add(1)).ptr_value as *const pg_sys::Node;
    if left.is_null() || right.is_null() {
        return None;
    }

    // Extract (Var, Const)
    let left = unwrap_relabel_node(left);
    let right = unwrap_relabel_node(right);
    let (var_ptr, const_ptr) = if (*left).type_ == pg_sys::NodeTag::T_Var
        && (*right).type_ == pg_sys::NodeTag::T_Const
    {
        (left as *const pg_sys::Var, right as *const pg_sys::Const)
    } else if (*left).type_ == pg_sys::NodeTag::T_Const
        && (*right).type_ == pg_sys::NodeTag::T_Var
    {
        (right as *const pg_sys::Var, left as *const pg_sys::Const)
    } else {
        return None;
    };

    if (*const_ptr).constisnull {
        return None;
    }

    // Extract integer constant value
    let const_type = (*const_ptr).consttype;
    let const_val: i64 = match const_type {
        pg_sys::INT2OID => (*const_ptr).constvalue.value() as i16 as i64,
        pg_sys::INT4OID => (*const_ptr).constvalue.value() as i32 as i64,
        pg_sys::INT8OID => (*const_ptr).constvalue.value() as i64,
        _ => return None, // Only integer constants supported
    };

    let col_idx = (*var_ptr).varattno as i32 - 1;
    if col_idx < 0 {
        return None;
    }

    Some(CaseWhenCondition { col_idx: col_idx as usize, op, const_val })
    }
}

/// Parse a CASE WHEN result value: T_Var (column ref) or T_Const (string constant).
unsafe fn parse_case_when_value(
    root: *mut pg_sys::PlannerInfo,
    expr: *const pg_sys::Node,
) -> Option<super::exec::CaseWhenValue> {
    unsafe {
    use super::exec::CaseWhenValue;

    if expr.is_null() {
        return Some(CaseWhenValue::StringConst(String::new()));
    }

    let expr = unwrap_relabel_node(expr);

    if (*expr).type_ == pg_sys::NodeTag::T_Var {
        let var_node = expr as *const pg_sys::Var;
        let col_idx = (*var_node).varattno as i32 - 1;
        if col_idx < 0 {
            return None;
        }
        // Verify the column is a text type
        let varno = (*var_node).varno as usize;
        if varno == 0 || varno >= (*root).simple_rel_array_size as usize {
            return None;
        }
        let rte = *(*root).simple_rte_array.add(varno);
        if rte.is_null() {
            return None;
        }
        let mut type_oid = pg_sys::InvalidOid;
        let mut typmod: i32 = -1;
        let mut collation: pg_sys::Oid = pg_sys::InvalidOid;
        pg_sys::get_atttypetypmodcoll((*rte).relid, (*var_node).varattno, &mut type_oid, &mut typmod, &mut collation);
        if type_oid != pg_sys::TEXTOID && type_oid != pg_sys::VARCHAROID && type_oid != pg_sys::BPCHAROID {
            return None; // Only text column refs supported
        }
        Some(CaseWhenValue::ColumnRef(col_idx as usize))
    } else if (*expr).type_ == pg_sys::NodeTag::T_Const {
        let const_node = expr as *const pg_sys::Const;
        if (*const_node).constisnull {
            return Some(CaseWhenValue::StringConst(String::new()));
        }
        let const_type = (*const_node).consttype;
        if const_type != pg_sys::TEXTOID && const_type != pg_sys::VARCHAROID && const_type != pg_sys::BPCHAROID {
            return None; // Only string constants supported
        }
        let cstr = pg_sys::text_to_cstring((*const_node).constvalue.cast_mut_ptr());
        let s = std::ffi::CStr::from_ptr(cstr).to_string_lossy().into_owned();
        pg_sys::pfree(cstr as *mut _);
        Some(CaseWhenValue::StringConst(s))
    } else {
        None // Unsupported value type
    }
    }
}

/// The create_upper_paths hook. Detects aggregate patterns over deltax
/// scans and injects optimized custom paths:
/// - COUNT(*) alone → DeltaXCount (sum of segment row_counts, metadata-only)
/// - MIN/MAX(col) alone → DeltaXMinMax (global min/max from segment metadata)
/// - SUM/AVG/COUNT/COUNT(DISTINCT) with optional GROUP BY and WHERE → DeltaXAgg
#[pg_guard]
pub unsafe extern "C-unwind" fn deltax_create_upper_paths(
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
        let mut non_agg_func_exprs: Vec<(i32, *const pg_sys::FuncExpr)> = Vec::new(); // (tlist_index, FuncExpr)
        let mut non_agg_op_exprs: Vec<(i32, *const pg_sys::OpExpr)> = Vec::new(); // (tlist_index, OpExpr)
        let mut non_agg_case_exprs: Vec<(i32, *const pg_sys::CaseExpr)> = Vec::new(); // (tlist_index, CaseExpr)
        let mut const_exprs: Vec<(i32, *const pg_sys::Const)> = Vec::new(); // (tlist_index, Const)

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
            } else if (*expr).type_ == pg_sys::NodeTag::T_FuncExpr && has_group_by {
                // Non-aggregate FuncExpr in target list — must match a GROUP BY expression
                non_agg_func_exprs.push((i, expr as *const pg_sys::FuncExpr));
            } else if (*expr).type_ == pg_sys::NodeTag::T_OpExpr && has_group_by {
                // Non-aggregate OpExpr in target list (e.g. col - 1) — must match a GROUP BY expression
                non_agg_op_exprs.push((i, expr as *const pg_sys::OpExpr));
            } else if (*expr).type_ == pg_sys::NodeTag::T_CaseExpr && has_group_by {
                // CASE WHEN in target list — must match a GROUP BY expression
                non_agg_case_exprs.push((i, expr as *const pg_sys::CaseExpr));
            } else if (*expr).type_ == pg_sys::NodeTag::T_Const && has_group_by {
                // Constant in target list (e.g. SELECT 1, ...) — pass through as-is
                const_exprs.push((i, expr as *const pg_sys::Const));
            } else {
                return; // Non-aggregate, non-Var, non-FuncExpr expression — bail
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
            _ => {
                return;
            }
        };

        // =====================================================================
        // Fast path: Single COUNT(*) with no GROUP BY, no WHERE → DeltaXCount
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
                    const_offset: 0,
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

            let mut agg_const_offset: i64 = 0;
            let (var_node, expr_kind): (*const pg_sys::Var, AggExpr) = if (*arg_expr).type_ == pg_sys::NodeTag::T_Var {
                (arg_expr as *const pg_sys::Var, AggExpr::Column)
            } else if (*arg_expr).type_ == pg_sys::NodeTag::T_RelabelType {
                // Unwrap RelabelType → Var
                let rlt = arg_expr as *const pg_sys::RelabelType;
                let inner = (*rlt).arg as *const pg_sys::Node;
                if !inner.is_null() && (*inner).type_ == pg_sys::NodeTag::T_Var {
                    (inner as *const pg_sys::Var, AggExpr::Column)
                } else {
                    return;
                }
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
            } else if (*arg_expr).type_ == pg_sys::NodeTag::T_OpExpr {
                // Check for col + const (or const + col)
                let opexpr = arg_expr as *const pg_sys::OpExpr;
                let opname_ptr = pg_sys::get_opname((*opexpr).opno);
                if opname_ptr.is_null() {
                    return;
                }
                let opname = std::ffi::CStr::from_ptr(opname_ptr)
                    .to_str()
                    .unwrap_or("");
                if opname != "+" {
                    return; // Only + operator supported
                }
                let op_args = (*opexpr).args;
                if op_args.is_null() || (*op_args).length != 2 {
                    return;
                }
                let left = (*(*op_args).elements.add(0)).ptr_value as *const pg_sys::Node;
                let right = (*(*op_args).elements.add(1)).ptr_value as *const pg_sys::Node;
                if left.is_null() || right.is_null() {
                    return;
                }
                // Extract (Var, Const) or (Const, Var)
                let (var_ptr, const_ptr) = if (*left).type_ == pg_sys::NodeTag::T_Var
                    && (*right).type_ == pg_sys::NodeTag::T_Const
                {
                    (left as *const pg_sys::Var, right as *const pg_sys::Const)
                } else if (*left).type_ == pg_sys::NodeTag::T_Const
                    && (*right).type_ == pg_sys::NodeTag::T_Var
                {
                    (right as *const pg_sys::Var, left as *const pg_sys::Const)
                } else {
                    return; // Not a simple Var + Const
                };
                // Extract integer constant value — only INT2/INT4/INT8
                if (*const_ptr).constisnull {
                    return;
                }
                let const_type = (*const_ptr).consttype;
                let const_val: i64 = match const_type {
                    pg_sys::INT2OID => (*const_ptr).constvalue.value() as i16 as i64,
                    pg_sys::INT4OID => (*const_ptr).constvalue.value() as i32 as i64,
                    pg_sys::INT8OID => (*const_ptr).constvalue.value() as i64,
                    _ => return, // Non-integer constant
                };
                // Check fits in i32 for serialization
                if const_val < i32::MIN as i64 || const_val > i32::MAX as i64 {
                    return;
                }
                agg_const_offset = const_val;
                (var_ptr, AggExpr::AddConst)
            } else {
                return; // Only plain column references, length(col), or col + const
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
                        const_offset: agg_const_offset,
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
                        const_offset: agg_const_offset,
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
                            const_offset: agg_const_offset,
                        });
                    } else {
                        classified_aggs.push(path::AggSpec {
                            agg_type: AggType::Count,
                            col_idx,
                            result_type_oid: (*aggref).aggtype,
                            col_type_oid: effective_col_type_oid,
                            expr_kind,
                            const_offset: agg_const_offset,
                        });
                    }
                    all_minmax = false;
                    has_non_minmax = true;
                }
                "min" => {
                    classified_aggs.push(path::AggSpec {
                        agg_type: AggType::Min,
                        col_idx,
                        result_type_oid: (*aggref).aggtype,
                        col_type_oid: effective_col_type_oid,
                        expr_kind,
                        const_offset: agg_const_offset,
                    });
                    if has_non_minmax {
                        // Mixed MIN/MAX with SUM/COUNT/AVG → falls through to general AggScan
                        all_minmax = false;
                    }
                    // else: keep all_minmax = true for potential metadata-only path
                }
                "max" => {
                    classified_aggs.push(path::AggSpec {
                        agg_type: AggType::Max,
                        col_idx,
                        result_type_oid: (*aggref).aggtype,
                        col_type_oid: effective_col_type_oid,
                        expr_kind,
                        const_offset: agg_const_offset,
                    });
                    if has_non_minmax {
                        all_minmax = false;
                    }
                }
                _ => {
                    return;
                }
            }
        }

        if classified_aggs.is_empty() {
            return;
        }


        // =====================================================================
        // Fast path: All MIN/MAX, no GROUP BY, no WHERE → DeltaXMinMax
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
        // DeltaXAgg path: SUM/AVG/COUNT/COUNT(DISTINCT) ± GROUP BY ± WHERE ± HAVING
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
                    pg_sys::NodeTag::T_ScalarArrayOpExpr => {
                        // col IN (...) / col = ANY(ARRAY[...])
                        let saop = qn as *const pg_sys::ScalarArrayOpExpr;
                        if !(*saop).useOr {
                            return; // ALL semantics not supported
                        }
                        let sa_args = (*saop).args;
                        if sa_args.is_null() || (*sa_args).length != 2 {
                            return;
                        }
                        let sa_arg0 = (*(*sa_args).elements.add(0)).ptr_value as *const pg_sys::Node;
                        let sa_arg1 = (*(*sa_args).elements.add(1)).ptr_value as *const pg_sys::Node;
                        if sa_arg0.is_null() || sa_arg1.is_null() {
                            return;
                        }
                        let sa_a0 = unwrap_relabel(sa_arg0);
                        if (*sa_a0).type_ != pg_sys::NodeTag::T_Var {
                            return;
                        }
                        if (*sa_arg1).type_ != pg_sys::NodeTag::T_Const {
                            return;
                        }
                        let sa_const = sa_arg1 as *const pg_sys::Const;
                        if (*sa_const).constisnull {
                            return;
                        }
                        let sa_var = sa_a0 as *const pg_sys::Var;
                        let sa_type_oid = (*sa_var).vartype;
                        if !matches!(
                            sa_type_oid,
                            pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID
                            | pg_sys::DATEOID | pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID
                        ) {
                            return; // only numeric/date/timestamp IN lists
                        }
                    }
                    _ => {
                        return; // Unknown qual type — bail
                    }
                }
            }
        }


        // Parse GROUP BY columns
        use super::exec::GroupByExpr;
        let mut group_specs: Vec<super::exec::GroupByColSpec> = Vec::new();
        let mut group_by_relid: pg_sys::Oid = pg_sys::InvalidOid;
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
                if expr.is_null() {
                    return;
                }

                if (*expr).type_ == pg_sys::NodeTag::T_Const {
                    // Constant GROUP BY key (e.g. GROUP BY 1 where 1 is a literal).
                    // This is a no-op for grouping — skip it.
                    continue;
                } else if (*expr).type_ == pg_sys::NodeTag::T_Var {
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
                    if group_by_relid == pg_sys::InvalidOid {
                        group_by_relid = relid;
                    }
                    let mut type_oid = pg_sys::InvalidOid;
                    let mut typmod: i32 = -1;
                    let mut collation: pg_sys::Oid = pg_sys::InvalidOid;
                    pg_sys::get_atttypetypmodcoll(relid, (*var_node).varattno, &mut type_oid, &mut typmod, &mut collation);

                    // Text/varchar GROUP BY columns are allowed when dictionary-encoded
                    // (ndistinct < 65536). Guarded by ndistinct check below.

                    group_specs.push(super::exec::GroupByColSpec {
                        col_idx,
                        type_oid,
                        expr: GroupByExpr::Column,
                    });
                } else if (*expr).type_ == pg_sys::NodeTag::T_FuncExpr {
                    let funcexpr = expr as *const pg_sys::FuncExpr;
                    let fn_name_ptr = pg_sys::get_func_name((*funcexpr).funcid);
                    if fn_name_ptr.is_null() {
                        return;
                    }
                    let fn_name = std::ffi::CStr::from_ptr(fn_name_ptr)
                        .to_str()
                        .unwrap_or("");

                    if fn_name == "regexp_replace" {
                        // Validate: regexp_replace(Var, Const, Const)
                        let fn_args = (*funcexpr).args;
                        if fn_args.is_null() || (*fn_args).length != 3 {
                            return;
                        }
                        let arg0 = (*(*fn_args).elements.add(0)).ptr_value as *const pg_sys::Node;
                        let arg1 = (*(*fn_args).elements.add(1)).ptr_value as *const pg_sys::Node;
                        let arg2 = (*(*fn_args).elements.add(2)).ptr_value as *const pg_sys::Node;

                        if arg0.is_null() || (*arg0).type_ != pg_sys::NodeTag::T_Var {
                            return;
                        }
                        if arg1.is_null() || (*arg1).type_ != pg_sys::NodeTag::T_Const {
                            return;
                        }
                        if arg2.is_null() || (*arg2).type_ != pg_sys::NodeTag::T_Const {
                            return;
                        }

                        let var_node = arg0 as *const pg_sys::Var;
                        let col_idx = (*var_node).varattno as i32 - 1;

                        let pattern_const = arg1 as *const pg_sys::Const;
                        let replacement_const = arg2 as *const pg_sys::Const;
                        if (*pattern_const).constisnull || (*replacement_const).constisnull {
                            return;
                        }

                        let pattern_cstr = pg_sys::text_to_cstring((*pattern_const).constvalue.cast_mut_ptr());
                        let pattern = std::ffi::CStr::from_ptr(pattern_cstr).to_string_lossy().into_owned();
                        pg_sys::pfree(pattern_cstr as *mut _);

                        let replacement_cstr = pg_sys::text_to_cstring((*replacement_const).constvalue.cast_mut_ptr());
                        let replacement = std::ffi::CStr::from_ptr(replacement_cstr).to_string_lossy().into_owned();
                        pg_sys::pfree(replacement_cstr as *mut _);

                        let func_oid = u32::from((*funcexpr).funcid);
                        let collation = u32::from((*funcexpr).inputcollid);

                        group_specs.push(super::exec::GroupByColSpec {
                            col_idx,
                            type_oid: pg_sys::TEXTOID,
                            expr: GroupByExpr::RegexpReplace { pattern, replacement, func_oid, collation },
                        });
                    } else if fn_name == "date_trunc" {
                        // Validate: date_trunc(Const, Var)
                        let fn_args = (*funcexpr).args;
                        if fn_args.is_null() || (*fn_args).length != 2 {
                            return;
                        }
                        let arg0 = (*(*fn_args).elements.add(0)).ptr_value as *const pg_sys::Node;
                        let arg1 = (*(*fn_args).elements.add(1)).ptr_value as *const pg_sys::Node;

                        if arg0.is_null() || (*arg0).type_ != pg_sys::NodeTag::T_Const {
                            return;
                        }
                        if arg1.is_null() || (*arg1).type_ != pg_sys::NodeTag::T_Var {
                            return;
                        }

                        let var_node = arg1 as *const pg_sys::Var;
                        let col_idx = (*var_node).varattno as i32 - 1;

                        // Get column type — must be timestamp or timestamptz
                        let rte = *(*root).simple_rte_array.add((*var_node).varno as usize);
                        if rte.is_null() {
                            return;
                        }
                        let mut type_oid = pg_sys::InvalidOid;
                        let mut typmod: i32 = -1;
                        let mut collation: pg_sys::Oid = pg_sys::InvalidOid;
                        pg_sys::get_atttypetypmodcoll((*rte).relid, (*var_node).varattno, &mut type_oid, &mut typmod, &mut collation);
                        if type_oid != pg_sys::TIMESTAMPOID && type_oid != pg_sys::TIMESTAMPTZOID {
                            return;
                        }

                        // Extract unit string from Const
                        let unit_const = arg0 as *const pg_sys::Const;
                        if (*unit_const).constisnull {
                            return;
                        }
                        let unit_cstr = pg_sys::text_to_cstring((*unit_const).constvalue.cast_mut_ptr());
                        let unit = std::ffi::CStr::from_ptr(unit_cstr).to_string_lossy().into_owned();
                        pg_sys::pfree(unit_cstr as *mut _);

                        // Only accept sub-day units where integer arithmetic is correct
                        let unit_usecs = match unit.as_str() {
                            "microsecond" | "microseconds" | "us" => 1_i64,
                            "millisecond" | "milliseconds" | "ms" => 1_000,
                            "second" | "seconds" => 1_000_000,
                            "minute" | "minutes" => 60_000_000,
                            "hour" | "hours" => 3_600_000_000,
                            "day" | "days" => 86_400_000_000,
                            _ => return, // week/month/quarter/year need calendar math
                        };

                        let func_oid = u32::from((*funcexpr).funcid);

                        group_specs.push(super::exec::GroupByColSpec {
                            col_idx,
                            type_oid,
                            expr: GroupByExpr::DateTrunc { unit, unit_usecs, func_oid },
                        });
                    } else if fn_name == "extract" {
                        // Validate: extract(Const text, Var timestamp/tz)
                        let fn_args = (*funcexpr).args;
                        if fn_args.is_null() || (*fn_args).length != 2 {
                            return;
                        }
                        let arg0 = (*(*fn_args).elements.add(0)).ptr_value as *const pg_sys::Node;
                        let arg1 = (*(*fn_args).elements.add(1)).ptr_value as *const pg_sys::Node;

                        if arg0.is_null() || (*arg0).type_ != pg_sys::NodeTag::T_Const {
                            return;
                        }
                        if arg1.is_null() || (*arg1).type_ != pg_sys::NodeTag::T_Var {
                            return;
                        }

                        let var_node = arg1 as *const pg_sys::Var;
                        let col_idx = (*var_node).varattno as i32 - 1;

                        // Get column type — must be timestamp or timestamptz
                        let rte = *(*root).simple_rte_array.add((*var_node).varno as usize);
                        if rte.is_null() {
                            return;
                        }
                        let mut type_oid = pg_sys::InvalidOid;
                        let mut typmod: i32 = -1;
                        let mut collation: pg_sys::Oid = pg_sys::InvalidOid;
                        pg_sys::get_atttypetypmodcoll((*rte).relid, (*var_node).varattno, &mut type_oid, &mut typmod, &mut collation);
                        if type_oid != pg_sys::TIMESTAMPOID && type_oid != pg_sys::TIMESTAMPTZOID {
                            return;
                        }

                        // Extract unit string from Const
                        let unit_const = arg0 as *const pg_sys::Const;
                        if (*unit_const).constisnull {
                            return;
                        }
                        let unit_cstr = pg_sys::text_to_cstring((*unit_const).constvalue.cast_mut_ptr());
                        let unit = std::ffi::CStr::from_ptr(unit_cstr).to_string_lossy().into_owned();
                        pg_sys::pfree(unit_cstr as *mut _);

                        // Only accept fields computable with pure arithmetic
                        match unit.as_str() {
                            "microsecond" | "microseconds"
                            | "millisecond" | "milliseconds"
                            | "second" | "seconds"
                            | "minute" | "minutes"
                            | "hour" | "hours"
                            | "dow"
                            | "epoch" => {}
                            _ => return, // calendar-based fields not supported
                        }

                        let func_oid = u32::from((*funcexpr).funcid);

                        group_specs.push(super::exec::GroupByColSpec {
                            col_idx,
                            type_oid: pg_sys::NUMERICOID,
                            expr: GroupByExpr::Extract { unit, func_oid },
                        });
                    } else {
                        return; // Unsupported function in GROUP BY
                    }
                } else if (*expr).type_ == pg_sys::NodeTag::T_OpExpr {
                    // col +/- const expression in GROUP BY
                    let opexpr = expr as *const pg_sys::OpExpr;
                    let opname_ptr = pg_sys::get_opname((*opexpr).opno);
                    if opname_ptr.is_null() {
                        return;
                    }
                    let opname = std::ffi::CStr::from_ptr(opname_ptr)
                        .to_str()
                        .unwrap_or("");
                    let is_plus = opname == "+";
                    let is_minus = opname == "-";
                    if !is_plus && !is_minus {
                        return;
                    }
                    let op_args = (*opexpr).args;
                    if op_args.is_null() || (*op_args).length != 2 {
                        return;
                    }
                    let left = (*(*op_args).elements.add(0)).ptr_value as *const pg_sys::Node;
                    let right = (*(*op_args).elements.add(1)).ptr_value as *const pg_sys::Node;
                    if left.is_null() || right.is_null() {
                        return;
                    }
                    // Extract (Var, Const) — for minus, Var must be on the left
                    let (var_ptr, const_ptr, negate) = if (*left).type_ == pg_sys::NodeTag::T_Var
                        && (*right).type_ == pg_sys::NodeTag::T_Const
                    {
                        (left as *const pg_sys::Var, right as *const pg_sys::Const, is_minus)
                    } else if is_plus
                        && (*left).type_ == pg_sys::NodeTag::T_Const
                        && (*right).type_ == pg_sys::NodeTag::T_Var
                    {
                        (right as *const pg_sys::Var, left as *const pg_sys::Const, false)
                    } else {
                        return;
                    };
                    if (*const_ptr).constisnull {
                        return;
                    }
                    let const_type = (*const_ptr).consttype;
                    let const_val: i64 = match const_type {
                        pg_sys::INT2OID => (*const_ptr).constvalue.value() as i16 as i64,
                        pg_sys::INT4OID => (*const_ptr).constvalue.value() as i32 as i64,
                        pg_sys::INT8OID => (*const_ptr).constvalue.value() as i64,
                        _ => return,
                    };
                    let offset = if negate { -const_val } else { const_val };
                    if offset < i32::MIN as i64 || offset > i32::MAX as i64 {
                        return;
                    }

                    let col_idx = (*var_ptr).varattno as i32 - 1;
                    let varno = (*var_ptr).varno as usize;
                    if varno == 0 || varno >= (*root).simple_rel_array_size as usize {
                        return;
                    }
                    let rte = *(*root).simple_rte_array.add(varno);
                    if rte.is_null() {
                        return;
                    }
                    let relid = (*rte).relid;
                    if group_by_relid == pg_sys::InvalidOid {
                        group_by_relid = relid;
                    }
                    let mut type_oid = pg_sys::InvalidOid;
                    let mut typmod: i32 = -1;
                    let mut collation: pg_sys::Oid = pg_sys::InvalidOid;
                    pg_sys::get_atttypetypmodcoll(relid, (*var_ptr).varattno, &mut type_oid, &mut typmod, &mut collation);

                    let op_oid = u32::from((*opexpr).opno);

                    group_specs.push(super::exec::GroupByColSpec {
                        col_idx,
                        type_oid,
                        expr: GroupByExpr::AddConst { offset, op_oid },
                    });
                } else if (*expr).type_ == pg_sys::NodeTag::T_CaseExpr {
                    // CASE WHEN ... THEN ... ELSE ... END in GROUP BY
                    match parse_case_expr(root, expr as *const pg_sys::CaseExpr) {
                        Some(spec) => {
                            group_specs.push(super::exec::GroupByColSpec {
                                col_idx: -1, // CaseWhen references multiple columns
                                type_oid: pg_sys::TEXTOID,
                                expr: GroupByExpr::CaseWhen(spec),
                            });
                        }
                        None => return, // Unsupported CASE WHEN pattern
                    }
                } else {
                    return; // Unsupported GROUP BY expression type
                }
            }

            // Validate that each non_agg_case_exprs entry matches a GROUP BY CaseWhen spec.
            // CaseExpr in target list must match a GROUP BY CaseExpr exactly (by PG equal()).
            // We find the matching group spec by checking that it's a CaseWhen variant.
            for &(_tlist_idx, _case_expr) in &non_agg_case_exprs {
                // The CaseExpr in the target list must correspond to a GROUP BY CaseWhen spec.
                // PG guarantees this when groupClause references the target entry.
                // If we have non_agg_case_exprs but no CaseWhen group specs, bail.
                let matched = group_specs.iter().any(|gs| matches!(gs.expr, GroupByExpr::CaseWhen(_)));
                if !matched {
                    return;
                }
            }

            // Validate that each non_agg_func_exprs entry matches a GROUP BY spec.
            // Var position varies: regexp_replace(Var, ...) vs date_trunc(Const, Var).
            for &(_tlist_idx, funcexpr) in &non_agg_func_exprs {
                let funcid = (*funcexpr).funcid;
                let fn_args = (*funcexpr).args;
                if fn_args.is_null() || (*fn_args).length < 1 {
                    return;
                }
                // Find the Var in any arg position
                let mut col_idx = -1_i32;
                let nargs = (*fn_args).length;
                for ai in 0..nargs {
                    let arg = (*(*fn_args).elements.add(ai as usize)).ptr_value as *const pg_sys::Node;
                    if !arg.is_null() && (*arg).type_ == pg_sys::NodeTag::T_Var {
                        let var_node = arg as *const pg_sys::Var;
                        col_idx = (*var_node).varattno as i32 - 1;
                        break;
                    }
                }
                if col_idx < 0 {
                    return;
                }

                let matched = group_specs.iter().any(|gs| {
                    let spec_func_oid = match &gs.expr {
                        GroupByExpr::RegexpReplace { func_oid, .. } => Some(*func_oid),
                        GroupByExpr::DateTrunc { func_oid, .. } => Some(*func_oid),
                        GroupByExpr::Extract { func_oid, .. } => Some(*func_oid),
                        _ => None,
                    };
                    if let Some(foid) = spec_func_oid {
                        gs.col_idx == col_idx && foid == u32::from(funcid)
                    } else {
                        false
                    }
                });
                if !matched {
                    return; // FuncExpr in target doesn't match any GROUP BY spec
                }
            }

            // Validate that each non_agg_op_exprs entry matches a GROUP BY AddConst spec.
            for &(_tlist_idx, opexpr) in &non_agg_op_exprs {
                let op_oid = u32::from((*opexpr).opno);
                let op_args = (*opexpr).args;
                if op_args.is_null() || (*op_args).length != 2 {
                    return;
                }
                // Find the Var in the OpExpr args
                let mut col_idx = -1_i32;
                for ai in 0..(*op_args).length {
                    let arg = (*(*op_args).elements.add(ai as usize)).ptr_value as *const pg_sys::Node;
                    if !arg.is_null() && (*arg).type_ == pg_sys::NodeTag::T_Var {
                        let var_node = arg as *const pg_sys::Var;
                        col_idx = (*var_node).varattno as i32 - 1;
                        break;
                    }
                }
                if col_idx < 0 {
                    return;
                }
                let matched = group_specs.iter().any(|gs| {
                    if let GroupByExpr::AddConst { op_oid: spec_op_oid, .. } = &gs.expr {
                        gs.col_idx == col_idx && *spec_op_oid == op_oid
                    } else {
                        false
                    }
                });
                if !matched {
                    return; // OpExpr in target doesn't match any GROUP BY spec
                }
            }
        }


        // Parse HAVING clause into simple filters
        let mut having_filters: Vec<super::exec::HavingFilter> = Vec::new();
        if has_having {
            use super::exec::{HavingOp, HavingFilter};
            let having_node = (*parse).havingQual as *const pg_sys::Node;
            // Collect qual nodes — PG may store as a single OpExpr, a BoolExpr
            // AND-list, or a plain T_List of conditions.
            let qual_nodes: Vec<*const pg_sys::Node> = if (*having_node).type_ == pg_sys::NodeTag::T_List {
                let list = having_node as *const pg_sys::List;
                (0..(*list).length)
                    .map(|i| pg_sys::list_nth(list as *mut _, i) as *const pg_sys::Node)
                    .collect()
            } else if (*having_node).type_ == pg_sys::NodeTag::T_BoolExpr {
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
                    None => {
                        return; // Can't match HAVING aggref — bail
                    }
                }
            }
        }

        // Fetch ndistinct stats for GROUP BY queries to:
        // 1. Bail out on high-cardinality columns (only when no WHERE)
        // 2. Provide accurate row estimates (always)
        let mut ndistinct_estimated_groups: Option<f64> = None;
        if !group_specs.is_empty() && group_by_relid != pg_sys::InvalidOid {
            let total_uncompressed_rows: f64 = companion_oids.iter()
                .map(|&oid| { let (_, _, rows) = cost::estimate_cost(oid); rows })
                .sum();

            if total_uncompressed_rows > 0.0 {
                let mut merged_ndistinct: std::collections::HashMap<String, i64> =
                    std::collections::HashMap::new();
                for &oid in &companion_oids {
                    let nd = cost::get_column_ndistinct(oid);
                    for (col, count) in nd {
                        *merged_ndistinct.entry(col).or_insert(0) += count;
                    }
                }

                // Guard: text GROUP BY columns need low cardinality
                // (ndistinct < 30K). High-cardinality text GROUP BY
                // (e.g. URL with 275K distinct) is still slower in AggScan
                // than PG's HashAgg due to aggregation + cleanup overhead.
                let has_text_group = group_specs.iter().any(|gs| {
                    matches!(gs.expr, GroupByExpr::Column)
                        && (gs.type_oid == pg_sys::TEXTOID
                            || gs.type_oid == pg_sys::VARCHAROID
                            || gs.type_oid == pg_sys::BPCHAROID
                            || gs.type_oid == pg_sys::NAMEOID)
                });
                // Text GROUP BY guard: skip AggScan when both conditions hold:
                // 1. PG estimates < 5% of rows survive filtering (small result set)
                // 2. The text column has very high global ndistinct (> 100K)
                // For small filtered sets on high-cardinality columns, PG's native
                // HashAgg on emitted rows beats AggScan's text decompression overhead.
                // However, when parallel workers are available, the parallel mixed
                // aggregation path handles high-cardinality text efficiently, so we
                // only bail when single-threaded.
                let n_workers = crate::get_parallel_workers();
                if has_text_group && has_where && n_workers <= 1 {
                    let estimated_rows = (*input_rel).rows;
                    let few_rows = estimated_rows < total_uncompressed_rows as f64 * 0.05;
                    let has_high_card_text = group_specs.iter().any(|gs| {
                        if !matches!(gs.expr, GroupByExpr::Column) {
                            return false;
                        }
                        let is_text = gs.type_oid == pg_sys::TEXTOID
                            || gs.type_oid == pg_sys::VARCHAROID
                            || gs.type_oid == pg_sys::BPCHAROID
                            || gs.type_oid == pg_sys::NAMEOID;
                        if !is_text {
                            return false;
                        }
                        let attno = (gs.col_idx + 1) as i16;
                        let name_ptr = pg_sys::get_attname(group_by_relid, attno, false);
                        if name_ptr.is_null() {
                            return false;
                        }
                        let col_name = std::ffi::CStr::from_ptr(name_ptr)
                            .to_str()
                            .unwrap_or("");
                        merged_ndistinct
                            .get(col_name)
                            .map(|&nd| nd > 100_000)
                            .unwrap_or(false)
                    });
                    if few_rows && has_high_card_text {
                        return;
                    }
                }

                if !merged_ndistinct.is_empty() {
                    // Bail out for high-cardinality GROUP BY (only without WHERE)
                    if !has_where {
                        let threshold = total_uncompressed_rows * 0.5;
                        let has_high_cardinality = group_specs.iter().any(|gs| {
                            if !matches!(gs.expr, GroupByExpr::Column) {
                                return false;
                            }
                            let attno = (gs.col_idx + 1) as i16;
                            let name_ptr = pg_sys::get_attname(group_by_relid, attno, false);
                            if name_ptr.is_null() {
                                return false;
                            }
                            let col_name = std::ffi::CStr::from_ptr(name_ptr)
                                .to_str()
                                .unwrap_or("");
                            merged_ndistinct
                                .get(col_name)
                                .map(|&nd| nd as f64 > threshold)
                                .unwrap_or(false)
                        });
                        if has_high_cardinality {
                            return;
                        }
                    }

                    // Compute ndistinct-based group estimate for GROUP BY
                    {
                        let mut product: f64 = 1.0;
                        let mut all_found = true;
                        for gs in &group_specs {
                            match &gs.expr {
                                GroupByExpr::Extract { unit, .. } => {
                                    // Bounded cardinality for extract fields
                                    let card = match unit.as_str() {
                                        "microsecond" | "microseconds" => 60_000_000.0,
                                        "millisecond" | "milliseconds" => 60_000.0,
                                        "second" | "seconds" => 60.0,
                                        "minute" | "minutes" => 60.0,
                                        "hour" | "hours" => 24.0,
                                        "dow" => 7.0,
                                        "epoch" => total_uncompressed_rows, // unique per row
                                        _ => total_uncompressed_rows,
                                    };
                                    product *= card;
                                }
                                GroupByExpr::DateTrunc { .. } | GroupByExpr::RegexpReplace { .. } | GroupByExpr::CaseWhen(_) => {
                                    // Can't easily estimate, skip ndistinct estimate
                                    all_found = false;
                                    break;
                                }
                                GroupByExpr::Column | GroupByExpr::AddConst { .. } => {
                                    let attno = (gs.col_idx + 1) as i16;
                                    let name_ptr = pg_sys::get_attname(group_by_relid, attno, false);
                                    if name_ptr.is_null() {
                                        all_found = false;
                                        break;
                                    }
                                    let col_name = std::ffi::CStr::from_ptr(name_ptr)
                                        .to_str()
                                        .unwrap_or("");
                                    if let Some(&nd) = merged_ndistinct.get(col_name) {
                                        product *= nd as f64;
                                    } else {
                                        all_found = false;
                                        break;
                                    }
                                }
                            }
                        }
                        if all_found {
                            ndistinct_estimated_groups = Some(product.min(total_uncompressed_rows));
                        }
                    }
                }
            }
        }

        // Use ndistinct estimate, fall back to PG's pathlist estimate, then 100.
        let pg_estimated_groups = if !group_specs.is_empty() {
            if let Some(est) = ndistinct_estimated_groups {
                est
            } else {
                let pathlist = (*output_rel).pathlist;
                if !pathlist.is_null() && (*pathlist).length > 0 {
                    let first_path =
                        (*(*pathlist).elements.add(0)).ptr_value as *const pg_sys::Path;
                    (*first_path).rows
                } else {
                    100.0
                }
            }
        } else {
            0.0
        };

        // === Top-N detection: ORDER BY <agg> [ASC|DESC] LIMIT N ===
        // Clear any stale topn info from a previous query whose DeltaXAgg path
        // was not chosen by the planner (leaving the thread-local unconsumed).
        path::clear_agg_topn_info();
        let mut topn_active = false;
        if has_group_by {
            let mut topn_limit: i64 = 0;
            let mut topn_sort_col: i32 = -1;
            let mut topn_ascending: bool = true;

            // 1. Extract LIMIT constant
            if !(*parse).limitCount.is_null() {
                let lnode = (*parse).limitCount as *const pg_sys::Node;
                if (*lnode).type_ == pg_sys::NodeTag::T_Const {
                    let c = lnode as *const pg_sys::Const;
                    if !(*c).constisnull {
                        topn_limit = (*c).constvalue.value() as i64;
                    }
                }
            }

            // Add OFFSET if present (we need top LIMIT+OFFSET rows internally)
            if topn_limit > 0 && !(*parse).limitOffset.is_null() {
                let onode = (*parse).limitOffset as *const pg_sys::Node;
                if (*onode).type_ == pg_sys::NodeTag::T_Const {
                    let c = onode as *const pg_sys::Const;
                    if !(*c).constisnull {
                        topn_limit += (*c).constvalue.value() as i64;
                    } else {
                        topn_limit = 0;
                    }
                } else {
                    topn_limit = 0;
                }
            }

            // Cap at reasonable limit
            if topn_limit > 10000 {
                topn_limit = 0;
            }

            // 2. Check sortClause: single entry referencing an aggregate
            if topn_limit > 0 {
                let sort_clause = (*parse).sortClause;
                if !sort_clause.is_null() && (*sort_clause).length == 1 {
                    let sc = pg_sys::list_nth(sort_clause, 0)
                        as *const pg_sys::SortGroupClause;
                    if !sc.is_null() {
                        // Find target entry for this sort key
                        let tle_ref = (*sc).tleSortGroupRef;
                        let mut sort_tle: *const pg_sys::TargetEntry = std::ptr::null();
                        for i in 0..nentries {
                            let te = pg_sys::list_nth(tlist, i)
                                as *const pg_sys::TargetEntry;
                            if !te.is_null() && (*te).ressortgroupref == tle_ref {
                                sort_tle = te;
                                break;
                            }
                        }
                        if !sort_tle.is_null() {
                            let sort_expr =
                                (*sort_tle).expr as *const pg_sys::Node;
                            if !sort_expr.is_null()
                                && (*sort_expr).type_ == pg_sys::NodeTag::T_Aggref
                            {
                                let sort_aggref =
                                    sort_expr as *const pg_sys::Aggref;
                                // Find which classified_agg this corresponds to
                                let mut sort_agg_idx: Option<usize> = None;
                                for (i, &ar) in aggrefs.iter().enumerate() {
                                    if std::ptr::eq(ar, sort_aggref) {
                                        sort_agg_idx = Some(i);
                                        break;
                                    }
                                }
                                // Fallback: match by aggfnoid + aggstar
                                if sort_agg_idx.is_none() {
                                    for (i, &ar) in aggrefs.iter().enumerate() {
                                        if (*ar).aggfnoid == (*sort_aggref).aggfnoid
                                            && (*ar).aggstar == (*sort_aggref).aggstar
                                        {
                                            sort_agg_idx = Some(i);
                                            break;
                                        }
                                    }
                                }

                                if let Some(agg_idx) = sort_agg_idx {
                                    let spec = &classified_aggs[agg_idx];
                                    // Only for i64-comparable result types
                                    let is_i64 = match spec.agg_type {
                                        AggType::CountStar
                                        | AggType::Count
                                        | AggType::CountDistinct => true,
                                        AggType::Sum => matches!(
                                            spec.col_type_oid,
                                            pg_sys::INT2OID | pg_sys::INT4OID
                                        ),
                                        AggType::Min | AggType::Max => matches!(
                                            spec.col_type_oid,
                                            pg_sys::INT2OID
                                                | pg_sys::INT4OID
                                                | pg_sys::INT8OID
                                        ),
                                        _ => false,
                                    };
                                    if is_i64 {
                                        // Determine sort direction
                                        let opname_ptr =
                                            pg_sys::get_opname((*sc).sortop);
                                        if !opname_ptr.is_null() {
                                            let opname =
                                                std::ffi::CStr::from_ptr(opname_ptr)
                                                    .to_str()
                                                    .unwrap_or("");
                                            topn_ascending = opname == "<";
                                            // Find output column index
                                            // (position among non-resjunk tlist entries)
                                            let resno = (*sort_tle).resno;
                                            let mut non_junk = 0i32;
                                            for j in 0..nentries {
                                                let te2 = pg_sys::list_nth(
                                                    tlist, j,
                                                )
                                                    as *const pg_sys::TargetEntry;
                                                if te2.is_null() || (*te2).resjunk
                                                {
                                                    continue;
                                                }
                                                if (*te2).resno == resno {
                                                    topn_sort_col = non_junk;
                                                    break;
                                                }
                                                non_junk += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if topn_sort_col < 0 {
                    // No ORDER BY on aggregate found
                    let sort_clause = (*parse).sortClause;
                    if sort_clause.is_null() || (*sort_clause).length == 0 {
                        // Bare LIMIT N — pass as bare_limit (sort_col = -1)
                        path::set_agg_topn_info(topn_limit, -1, true);
                        // topn_active stays false — no pathkeys claimed
                    } else {
                        topn_limit = 0; // ORDER BY exists but doesn't match an aggregate — disable
                    }
                }
            }

            if topn_limit > 0 && topn_sort_col >= 0 {
                path::set_agg_topn_info(topn_limit, topn_sort_col, topn_ascending);
                topn_active = true;
            }
        }

        let pathkeys = if topn_active {
            (*root).sort_pathkeys
        } else {
            std::ptr::null_mut()
        };

        path::add_agg_path(
            root,
            output_rel,
            &companion_oids,
            &classified_aggs,
            &group_specs,
            &having_filters,
            pg_estimated_groups,
            pathkeys,
        );
    }
}

/// Extract companion OIDs from a planner path for aggregate pushdown.
///
/// Handles:
/// - DeltaXDecompress/DeltaXAppend CustomPath: extract OIDs from custom_private
/// - AppendPath: walk subpaths, try CustomPath extraction first, then fall back
///   to catalog lookup via the child rel OID (handles ProjectionPath wrapping
///   where PG may use SeqScan instead of our CustomPath as the inner path)
///
/// Returns None if the path doesn't contain compressed partitions, or if there
/// are uncompressed partitions with actual data.
unsafe fn extract_companion_oids(
    root: *mut pg_sys::PlannerInfo,
    path: *const pg_sys::Path,
) -> Option<Vec<pg_sys::Oid>> {
    unsafe {
        // Unwrap ProjectionPath at the top level — PG wraps the input path
        // in ProjectionPath when the GROUP BY target list contains expressions
        // (e.g. regexp_replace) that need evaluation.
        let path = if (*path).type_ == pg_sys::NodeTag::T_ProjectionPath {
            let proj = path as *const pg_sys::ProjectionPath;
            (*proj).subpath as *const pg_sys::Path
        } else {
            path
        };
        if path.is_null() {
            return None;
        }
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
                let raw_subpath = pg_sys::list_nth(subpaths, i) as *const pg_sys::Path;
                if raw_subpath.is_null() {
                    continue;
                }
                // Unwrap ProjectionPath if present (PG wraps child paths when
                // the target list needs expression evaluation, e.g. regexp_replace)
                let subpath = if (*raw_subpath).type_ == pg_sys::NodeTag::T_ProjectionPath {
                    let proj = raw_subpath as *const pg_sys::ProjectionPath;
                    (*proj).subpath as *const pg_sys::Path
                } else {
                    raw_subpath
                };
                if subpath.is_null() {
                    continue;
                }
                // Try CustomPath extraction first (fast path)
                if (*subpath).type_ == pg_sys::NodeTag::T_CustomPath {
                    let cpath = subpath as *const pg_sys::CustomPath;
                    if let Some(sub_oids) = extract_oids_from_custom_path(cpath) {
                        oids.extend(sub_oids);
                        continue;
                    }
                }
                // Fallback: look up companion OID from catalog via the child rel OID.
                // This handles the case where PG picked SeqScan (T_Path) instead of
                // our CustomPath as the inner path of a ProjectionPath.
                if let Some(companion_oid) = lookup_companion_from_subpath(root, subpath) {
                    oids.push(companion_oid);
                    continue;
                }
                // Not a compressed partition — check if it has data
                if subpath_has_data(root, subpath) {
                    return None;
                }
                // Empty partition (0 blocks on disk) — safe to skip
            }
            if oids.is_empty() {
                None
            } else {
                Some(oids)
            }
        } else {
            None
        }
    }
}

/// Look up companion OID from the catalog for a subpath's underlying relation.
/// Returns Some(companion_oid) if the relation is a compressed partition.
unsafe fn lookup_companion_from_subpath(
    root: *mut pg_sys::PlannerInfo,
    subpath: *const pg_sys::Path,
) -> Option<pg_sys::Oid> {
    unsafe {
        let parent = (*subpath).parent;
        if parent.is_null() {
            return None;
        }
        let rti = (*parent).relid;
        if rti == 0 {
            return None;
        }
        let rte = *(*root).simple_rte_array.add(rti as usize);
        if rte.is_null() {
            return None;
        }
        let child_oid = (*rte).relid;
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
            Some(companion_oid)
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

/// Check if a single qual node is pushable to DeltaXAgg's batch filter.
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

/// Extract companion OIDs from a DeltaXDecompress or DeltaXAppend CustomPath.
unsafe fn extract_oids_from_custom_path(
    cpath: *const pg_sys::CustomPath,
) -> Option<Vec<pg_sys::Oid>> {
    unsafe {
        let methods = (*cpath).methods;
        if methods.is_null() {
            return None;
        }
        let name = std::ffi::CStr::from_ptr((*methods).CustomName);
        if name != super::DELTAX_APPEND_NAME && name != super::CUSTOM_NAME {
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
///   returns None (cannot use DeltaXAppend).
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
                // Not compressed — check if partition has data.
                // reltuples == 0.0 means ANALYZE ran and found zero rows.
                // reltuples < 0 means never analyzed — we must assume it
                // could contain data and bail out.
                let reltuples = cost::get_reltuples(child_oid);
                if reltuples != 0.0 {
                    // Has data or unknown — cannot use DeltaXAppend
                    return None;
                }
                // ANALYZE confirmed empty, safe to skip
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
/// by looking for a companion table in _deltax_compressed schema.
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

        // Look up _deltax_compressed schema OID
        let schema_cstr = c"_deltax_compressed";
        let compressed_ns_oid = pg_sys::get_namespace_oid(schema_cstr.as_ptr(), true);
        if compressed_ns_oid == pg_sys::InvalidOid {
            return pg_sys::InvalidOid;
        }

        // Skip tables already in the _deltax_compressed schema to avoid recursion
        let rel_ns_oid = pg_sys::get_rel_namespace(rel_oid);
        if rel_ns_oid == compressed_ns_oid {
            return pg_sys::InvalidOid;
        }

        // Check if _deltax_compressed.<rel_name>_meta exists
        let meta_name = format!("{}_meta", rel_name);
        let companion_cname = std::ffi::CString::new(meta_name).unwrap();
        pg_sys::get_relname_relid(companion_cname.as_ptr(), compressed_ns_oid)
    }
}

/// ExecutorStart hook: block DML on compressed partitions.
///
/// INSERT/UPDATE/DELETE on a compressed partition would silently produce
/// incorrect results (writes go to the truncated heap, reads come from the
/// companion table). This hook raises an error before execution begins.
#[pg_guard]
pub unsafe extern "C-unwind" fn deltax_executor_start(
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
