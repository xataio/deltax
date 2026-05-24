use pgrx::pg_guard;
use pgrx::pg_sys;
use pgrx::prelude::Spi;
use std::collections::HashMap;
use std::ffi::c_int;
use std::sync::atomic::Ordering;

use super::PREV_EXECUTOR_START_HOOK;
use super::PREV_GET_RELATION_INFO_HOOK;
use super::PREV_HOOK;
use super::PREV_PLANNER_HOOK;
use super::PREV_UPPER_HOOK;
use super::cost;
use super::path;

thread_local! {
    /// Cache of partition OID → companion table OID (or InvalidOid if not compressed).
    static COMPRESSED_CACHE: std::cell::RefCell<HashMap<pg_sys::Oid, pg_sys::Oid>> =
        std::cell::RefCell::new(HashMap::new());

    /// Cache of parent table OID → time column attribute number (0 = not a deltatable).
    static TIME_COLUMN_CACHE: std::cell::RefCell<HashMap<pg_sys::Oid, i16>> =
        std::cell::RefCell::new(HashMap::new());

    /// Cache of parent table OID → (time_column_name, segment_by_names).
    /// Used by the metadata-only aggregate fast path (classify_meta_quals)
    /// so we can compare a qual's Var column name against the deltatable's
    /// time and segment-by columns without re-running SPI per query.
    static META_COLS_CACHE: std::cell::RefCell<HashMap<pg_sys::Oid, (String, Vec<String>)>> =
        std::cell::RefCell::new(HashMap::new());

    /// When true, the ExecutorStart hook skips the DML-on-compressed check.
    /// Used by internal operations like deltax_decompress_partition.
    static DML_BYPASS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub fn invalidate_compressed_cache() {
    COMPRESSED_CACHE.with(|cache| cache.borrow_mut().clear());
    TIME_COLUMN_CACHE.with(|cache| cache.borrow_mut().clear());
    META_COLS_CACHE.with(|cache| cache.borrow_mut().clear());
    cost::invalidate_caches();
    super::exec::segments::invalidate_colstats_cache();
}

/// Look up the companion OID for a partition's heap OID, using
/// `COMPRESSED_CACHE` to amortise the catalog probe across the planner's
/// repeated calls on the same query.
unsafe fn cached_companion_for_rel(rel_oid: pg_sys::Oid) -> pg_sys::Oid {
    if let Some(&oid) = COMPRESSED_CACHE
        .with(|c| c.borrow().get(&rel_oid).copied())
        .as_ref()
    {
        return oid;
    }
    let oid = unsafe { check_compressed_partition(rel_oid) };
    COMPRESSED_CACHE.with(|c| c.borrow_mut().insert(rel_oid, oid));
    oid
}

/// Look up the deltatable's `(time_column, segment_by[])` configuration
/// for a parent relation OID, cached thread-locally. Returns `None` if
/// the relation isn't registered in `deltax.deltax_deltatable`.
unsafe fn get_meta_cols(parent_oid: pg_sys::Oid) -> Option<(String, Vec<String>)> {
    if let Some(v) = META_COLS_CACHE.with(|cache| cache.borrow().get(&parent_oid).cloned()) {
        if v.0.is_empty() {
            return None;
        }
        return Some(v);
    }
    unsafe {
        let schema_name_ptr = pg_sys::get_namespace_name(pg_sys::get_rel_namespace(parent_oid));
        let table_name_ptr = pg_sys::get_rel_name(parent_oid);
        if schema_name_ptr.is_null() || table_name_ptr.is_null() {
            META_COLS_CACHE.with(|c| {
                c.borrow_mut()
                    .insert(parent_oid, (String::new(), Vec::new()))
            });
            return None;
        }
        let schema = std::ffi::CStr::from_ptr(schema_name_ptr)
            .to_string_lossy()
            .into_owned();
        let table = std::ffi::CStr::from_ptr(table_name_ptr)
            .to_string_lossy()
            .into_owned();

        let result = Spi::connect(|client| {
            let row = client
                .select(
                    "SELECT time_column, coalesce(segment_by, ARRAY[]::text[]) \
                     FROM deltax.deltax_deltatable WHERE schema_name = $1 AND table_name = $2",
                    Some(1),
                    &[schema.clone().into(), table.clone().into()],
                )
                .ok()?;
            let first = row.first();
            let time_col: Option<String> = first.get(1).ok().flatten();
            let seg_by: Option<Vec<String>> = first.get(2).ok().flatten();
            match (time_col, seg_by) {
                (Some(t), Some(s)) => Some((t, s)),
                _ => None,
            }
        });

        match result {
            Some(v) => {
                META_COLS_CACHE.with(|c| c.borrow_mut().insert(parent_oid, v.clone()));
                Some(v)
            }
            None => {
                META_COLS_CACHE.with(|c| {
                    c.borrow_mut()
                        .insert(parent_oid, (String::new(), Vec::new()))
                });
                None
            }
        }
    }
}

/// Set or clear the DML bypass flag for internal operations.
pub(crate) fn set_dml_bypass(bypass: bool) {
    DML_BYPASS.with(|flag| flag.set(bypass));
}

/// Get the time column's attribute number for a deltatable parent table.
/// Returns None if the table is not a deltax deltatable.
///
/// Caches the resolved attno separately from `META_COLS_CACHE` so the
/// `get_attnum` catalog probe is amortised — but the SPI lookup is shared
/// via `get_meta_cols`. `0` in `TIME_COLUMN_CACHE` is the sentinel for
/// "not a deltatable / column not found".
unsafe fn get_time_column_attno(parent_oid: pg_sys::Oid) -> Option<i16> {
    if let Some(attno) = TIME_COLUMN_CACHE.with(|c| c.borrow().get(&parent_oid).copied()) {
        return if attno > 0 { Some(attno) } else { None };
    }
    let attno = match unsafe { get_meta_cols(parent_oid) } {
        Some((time_col, _)) => {
            let col_cname = match std::ffi::CString::new(time_col) {
                Ok(s) => s,
                Err(_) => return None,
            };
            let a = unsafe { pg_sys::get_attnum(parent_oid, col_cname.as_ptr()) };
            if a == pg_sys::InvalidAttrNumber as i16 {
                0
            } else {
                a
            }
        }
        None => 0,
    };
    TIME_COLUMN_CACHE.with(|c| c.borrow_mut().insert(parent_oid, attno));
    if attno > 0 { Some(attno) } else { None }
}

/// Find the parent table OID for a child partition via append_rel_list.
/// Find the partitioned parent OID for an input_rel in the upper-paths
/// hook by scanning `simple_rte_array` for the first RTE_RELATION with
/// `inh = true` (inheritance). For a single-table `SELECT agg FROM t`
/// query there's only one such RTE; for joins we take the first —
/// acceptable since the metadata-only fast path never fires for joins
/// (aggrefs from other tables fail the aggref classifier).
unsafe fn find_inh_parent_oid(root: *mut pg_sys::PlannerInfo) -> Option<pg_sys::Oid> {
    unsafe {
        let array_size = (*root).simple_rel_array_size;
        for rti in 1..array_size {
            let rte = *(*root).simple_rte_array.add(rti as usize);
            if rte.is_null() {
                continue;
            }
            if (*rte).rtekind == pg_sys::RTEKind::RTE_RELATION && (*rte).inh {
                return Some((*rte).relid);
            }
        }
        None
    }
}

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
                let parent_rte = *(*root).simple_rte_array.add((*node).parent_relid as usize);
                return Some((*parent_rte).relid);
            }
        }
        None
    }
}

/// Check whether a deltatable has segment_by configured. When segment_by is
/// used, segments within a partition have overlapping time ranges, so we cannot
/// advertise sorted output via pathkeys. Backed by the shared `META_COLS_CACHE`.
unsafe fn has_segment_by(parent_oid: pg_sys::Oid) -> bool {
    unsafe { get_meta_cols(parent_oid) }
        .map(|(_, sb)| !sb.is_empty())
        .unwrap_or(false)
}

/// Check if the first query pathkey matches the time column in ASC order.
/// Returns `(pathkey_list, is_ascending)` — pathkey_list is null if no match.
///
/// DESC is intentionally NOT advertised here: the decompress scan emits rows
/// in ASC storage order within each segment, so advertising a DESC pathkey
/// would let the planner skip the sort step and return rows out of order
/// (observed on PG17). For DESC queries we fall through to no advertisement,
/// which lets the planner add a Sort above our scan.
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

        #[cfg(feature = "pg17")]
        let is_asc = (*first_pk).pk_strategy == pg_sys::BTLessStrategyNumber as i32;
        #[cfg(feature = "pg18")]
        let is_asc = (*first_pk).pk_cmptype == pg_sys::CompareType::COMPARE_LT;

        if !is_asc {
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
                let pk_list =
                    pg_sys::lappend(std::ptr::null_mut(), first_pk as *mut std::ffi::c_void);
                return (pk_list, is_asc);
            }
        }

        (std::ptr::null_mut(), true)
    }
}

/// Extract this relation's first ORDER BY column attribute number from pathkeys.
/// Returns the 1-based attno, or None if ORDER BY is not a simple column
/// reference on this relation.
unsafe fn extract_order_by_attno(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
) -> Option<i16> {
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

        // PG17 emitted an EquivalenceMember per partition child. PG18 only
        // keeps the parent's Var — child rels have to walk back through
        // `append_rel_list` to learn their parent relid before they can
        // match. We accept either varno here so both versions work.
        let rel_relid = (*rel).relid;
        let parent_relid = parent_relid_from_appendrel(root, rel_relid);

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
            let varno = (*var).varno as u32;
            if varno == rel_relid || Some(varno) == parent_relid {
                return Some((*var).varattno);
            }
        }
        None
    }
}

/// Walks `root->append_rel_list` looking for an AppendRelInfo whose
/// `child_relid` matches `rel_relid`, returning the corresponding parent
/// relid. Returns `None` when `rel` is a top-level (non-child) rel.
unsafe fn parent_relid_from_appendrel(
    root: *mut pg_sys::PlannerInfo,
    rel_relid: u32,
) -> Option<u32> {
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
            if (*node).child_relid == rel_relid {
                return Some((*node).parent_relid);
            }
        }
        None
    }
}

/// Extract Top-N info (effective LIMIT + sort direction) from the parse tree.
///
/// Returns `(effective_limit, sort_ascending, multi_col_sort, nulls_first)`:
/// - effective_limit = 0 means Top-N is disabled
/// - multi_col_sort = true when ORDER BY has multiple columns (first must be time)
/// - Only enabled when LIMIT is a constant integer ≤ 10000 and ORDER BY matches time column
unsafe fn extract_topn_info(
    root: *mut pg_sys::PlannerInfo,
    parse: *mut pg_sys::Query,
) -> (i64, bool, bool, bool) {
    unsafe {
        if parse.is_null() {
            return (0, true, false, false);
        }

        // Scan-level top-N makes no sense for aggregate queries — the aggregate
        // needs all rows from the scan.  (The DeltaXAgg upper path has its own
        // top-N logic.)
        if (*parse).hasAggs || !(*parse).groupClause.is_null() {
            return (0, true, false, false);
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
            return (0, true, false, false);
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
                return (0, true, false, false);
            }
        } else {
            0
        };

        let effective_limit = limit_count + offset;

        // Cap at 10000 — beyond that, overhead not worth it
        if effective_limit > 10000 {
            return (0, true, false, false);
        }

        // Check if ORDER BY has at least one pathkey and the first is the time column.
        // Multi-column ORDER BY is supported: we use the time column for segment
        // skipping and threshold, PG's Sort node handles the full multi-column sort.
        let query_pathkeys = (*root).query_pathkeys;
        if query_pathkeys.is_null() || (*query_pathkeys).length < 1 {
            return (0, true, false, false);
        }

        let multi_col_sort = (*query_pathkeys).length > 1;

        let first_pk = pg_sys::list_nth(query_pathkeys, 0) as *mut pg_sys::PathKey;
        if first_pk.is_null() {
            return (0, true, false, false);
        }

        #[cfg(feature = "pg17")]
        let is_asc = (*first_pk).pk_strategy == pg_sys::BTLessStrategyNumber as i32;
        #[cfg(feature = "pg18")]
        let is_asc = (*first_pk).pk_cmptype == pg_sys::CompareType::COMPARE_LT;

        #[cfg(feature = "pg17")]
        let is_desc = (*first_pk).pk_strategy == pg_sys::BTGreaterStrategyNumber as i32;
        #[cfg(feature = "pg18")]
        let is_desc = (*first_pk).pk_cmptype == pg_sys::CompareType::COMPARE_GT;

        if !is_asc && !is_desc {
            return (0, true, false, false);
        }

        (
            effective_limit,
            is_asc,
            multi_col_sort,
            (*first_pk).pk_nulls_first,
        )
    }
}

/// `get_relation_info_hook` — invoked inside `get_relation_info` after
/// PG has populated `rel->pages`/`rel->tuples` from `estimate_rel_size`
/// but before `set_baserel_size_estimates` applies restrictinfo
/// selectivity.
///
/// For a compressed pg_deltax child partition, the on-disk heap is
/// truncated to 0 pages. PG's estimator uses `ceil((reltuples/relpages)
/// * curpages)` where `curpages = 0`, so `rel->tuples` collapses to 0
/// and every `clauselist_selectivity * tuples` result rounds up to the
/// `rel->rows = 1` fallback. That's what makes the planner treat
/// `WHERE order_id = N` as returning "maybe 1 row" even though
/// pg_statistic.stadistinct is populated correctly.
///
/// Injecting the true row count here (from `deltax.deltax_partition.row_count`
/// via `cost::get_row_count`) feeds the post-hook selectivity math
/// properly: `rel->rows = row_count * eq_selectivity`.
#[pg_guard]
pub unsafe extern "C-unwind" fn deltax_get_relation_info(
    root: *mut pg_sys::PlannerInfo,
    relation_object_id: pg_sys::Oid,
    inh_parent: bool,
    rel: *mut pg_sys::RelOptInfo,
) {
    unsafe {
        let prev = PREV_GET_RELATION_INFO_HOOK.load(Ordering::SeqCst);
        if !prev.is_null() {
            let prev_fn: pg_sys::get_relation_info_hook_type = Some(std::mem::transmute::<
                *mut (),
                unsafe extern "C-unwind" fn(
                    *mut pg_sys::PlannerInfo,
                    pg_sys::Oid,
                    bool,
                    *mut pg_sys::RelOptInfo,
                ),
            >(prev));
            if let Some(f) = prev_fn {
                f(root, relation_object_id, inh_parent, rel);
            }
        }

        // Only interested in base relations (partitioned children show
        // up as RELOPT_OTHER_MEMBER_REL during partition expansion).
        if (*rel).reloptkind != pg_sys::RelOptKind::RELOPT_BASEREL
            && (*rel).reloptkind != pg_sys::RelOptKind::RELOPT_OTHER_MEMBER_REL
        {
            return;
        }

        // Is this a compressed pg_deltax partition?
        let companion_oid = check_compressed_partition(relation_object_id);
        if companion_oid == pg_sys::InvalidOid {
            return;
        }

        let row_count = match cost::get_row_count(companion_oid) {
            Some(rc) if rc > 0 => rc as f64,
            _ => return,
        };

        // Override with the true row count from our catalog. Keep
        // pages derived from tuple-width × row_count so PG doesn't
        // later read relpages=0 and zero out tuples again.
        (*rel).tuples = row_count;
        if (*rel).pages == 0 {
            // Rough heuristic: 100 rows per page is a reasonable
            // density for typical OLTP-ish row widths. The exact
            // value doesn't matter much — PG primarily uses tuples.
            (*rel).pages = ((row_count / 100.0).ceil() as u32).max(1);
        }
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
            let prev_fn: pg_sys::set_rel_pathlist_hook_type = Some(std::mem::transmute::<
                *mut (),
                unsafe extern "C-unwind" fn(
                    *mut pg_sys::PlannerInfo,
                    *mut pg_sys::RelOptInfo,
                    u32,
                    *mut pg_sys::RangeTblEntry,
                ),
            >(prev));
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
        let (effective_limit, sort_ascending, multi_col_sort, topn_nulls_first) =
            extract_topn_info(root, parse);

        // Check if this is the parent of a partitioned table (for DeltaXAppend)
        if (*rel).reloptkind == pg_sys::RelOptKind::RELOPT_BASEREL
            && (*rte).inh
            && let Some(companion_oids) = collect_compressed_children(root, rti)
        {
            // For Top-N, validate ORDER BY is a simple column reference.
            // Works for any column (time, text, numeric).
            let (append_topn_limit, append_sort_col_attno) = if effective_limit > 0 {
                if let Some(attno) = extract_order_by_attno(root, rel) {
                    (effective_limit, attno as i32)
                } else {
                    (0, 0)
                }
            } else {
                (0, 0)
            };
            let append_multi_col = if append_topn_limit > 0 {
                multi_col_sort
            } else {
                false
            };
            path::add_deltax_append_path(
                root,
                rel,
                &companion_oids,
                std::ptr::null_mut(),
                append_topn_limit,
                sort_ascending,
                append_multi_col,
                append_sort_col_attno,
                topn_nulls_first,
            );

            // Partial-path variant for PG parallel query. Top-N pushdown is
            // suppressed because per-worker top-N would be incorrect without
            // a Gather-Merge combiner.
            //
            // Selective queries (point lookups, EXISTS) still go through
            // the partial path. An attempt to gate on PG's
            // `clauselist_selectivity` was removed because selectivity
            // estimates on compressed children are unreliable: after
            // `deltax_compress_partition` the partition's
            // `pg_class.reltuples` is 0, so ANALYZE never collects stats
            // and PG falls back to equality default 0.005 (or worse for
            // text equality — 2.5e-5 observed). That mis-classified Q17's
            // `event_type='Delivered'` + time-range filter as returning
            // ~370 rows and suppressed its Gather. Until the cost model
            // wires segment-level bloom/min-max selectivity in, we accept
            // the small absolute regression on point lookups in exchange
            // for keeping Q17/Q23/Q25/Q30 parallel.
            if (*rel).consider_parallel && append_topn_limit == 0 {
                let cap = crate::get_scan_parallel_workers();
                if cap > 0 {
                    let pg_cap = pg_sys::max_parallel_workers_per_gather;
                    let per_scan_cap = cap.min(pg_cap);
                    let total_segments: i64 = companion_oids
                        .iter()
                        .map(|&oid| cost::get_segment_count(oid))
                        .sum();
                    // Mirror PG's compute_parallel_worker(): don't spawn a
                    // worker unless it has a meaningful amount of work.
                    const MIN_SEGS_PER_WORKER: i64 = 8;
                    let seg_floor = (total_segments / MIN_SEGS_PER_WORKER) as i32;
                    let workers = per_scan_cap.min(seg_floor).max(0);
                    if workers > 0 {
                        path::add_partial_deltax_append_path(
                            root,
                            rel,
                            &companion_oids,
                            std::ptr::null_mut(),
                            workers,
                        );
                    }
                }
            }
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
        let companion_oid = cached_companion_for_rel(rel_oid);

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
        let has_segby = parent_oid_opt
            .map(|oid| has_segment_by(oid))
            .unwrap_or(false);

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
            if let Some(attno) = extract_order_by_attno(root, rel) {
                (effective_limit, attno as i32)
            } else {
                (0, 0)
            }
        } else {
            (0, 0)
        };

        // Add the custom decompress path
        let topn_multi_col = if topn_effective_limit > 0 {
            multi_col_sort
        } else {
            false
        };
        path::add_decompress_path(
            root,
            rel,
            companion_oid,
            pathkeys,
            topn_effective_limit,
            sort_ascending,
            topn_multi_col,
            topn_sort_col_attno,
            topn_nulls_first,
        );
    }
}

/// Unwrap a RelabelType node to get the inner expression.
/// Extract the flat qual list (after `make_ands_implicit`) from the
/// parse tree, falling back to `baserestrictinfo`. Returns null if
/// there are no WHERE clauses.
unsafe fn extract_query_quals(root: *mut pg_sys::PlannerInfo) -> *mut pg_sys::List {
    unsafe {
        let parse = (*root).parse;
        let jointree = (*parse).jointree;
        if !jointree.is_null() && !(*jointree).quals.is_null() {
            let quals_node = (*jointree).quals as *const pg_sys::Node;
            if (*quals_node).type_ == pg_sys::NodeTag::T_List {
                return pg_sys::copyObjectImpl(quals_node as *const _) as *mut pg_sys::List;
            }
            let qual_copy = pg_sys::copyObjectImpl(quals_node as *const _) as *mut pg_sys::Node;
            return pg_sys::make_ands_implicit(qual_copy as *mut pg_sys::Expr);
        }
        // Fallback: sometimes quals live on baserestrictinfo only.
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
            let mut result: *mut pg_sys::List = std::ptr::null_mut();
            for i in 0..(*bri).length {
                let ri = pg_sys::list_nth(bri, i) as *const pg_sys::RestrictInfo;
                if ri.is_null() || (*ri).clause.is_null() {
                    continue;
                }
                let clause_copy =
                    pg_sys::copyObjectImpl((*ri).clause as *const _) as *mut pg_sys::Expr;
                result = pg_sys::lappend(result, clause_copy as *mut _);
            }
            if !result.is_null() {
                return result;
            }
        }
        std::ptr::null_mut()
    }
}

/// Walk a qual list and verify every clause is either
/// - `OpExpr(Var(time_col), Const)` or `(Const, Var(time_col))` with op
///   in `{=, <, <=, >, >=}`; or
/// - `OpExpr(Var(segment_by_col), Const)` or `(Const, Var(segment_by_col))`
///   with op `=`; or
/// - `BoolExpr(AND)` that recursively satisfies the rules.
///
/// Returns `true` iff every clause qualifies the metadata-only fast
/// path. `BETWEEN` expands to two AND'd OpExprs, so covered by the
/// time-column branch.
///
/// Used by the DeltaXCount/DeltaXMinMax gates to decide whether the
/// `_meta.min_time`/`max_time` + segment_by pruning in
/// `load_segments_heap` is sufficient — if any other predicate is
/// present we fall through to `DeltaXAgg` which decompresses + filters
/// correctly.
/// Types whose per-segment MIN/MAX is stored as order-preserving i64 in
/// the colstats table and can be decoded back to a PG datum.  TEXT/BYTEA/
/// BOOL fall outside this set — a MIN/MAX on them must go through the
/// generic aggregate path.
fn is_minmax_meta_type(col_type_oid: pg_sys::Oid) -> bool {
    matches!(
        col_type_oid,
        pg_sys::INT2OID
            | pg_sys::INT4OID
            | pg_sys::INT8OID
            | pg_sys::FLOAT4OID
            | pg_sys::FLOAT8OID
            | pg_sys::DATEOID
            | pg_sys::TIMESTAMPOID
            | pg_sys::TIMESTAMPTZOID
    )
}

/// Half-open time interval `[lo, hi)` in PG-epoch microseconds
/// (the internal TIMESTAMPTZ representation — same units as the
/// `deltax.deltax_partition.range_start/range_end` datums and the `Const`
/// values extracted from WHERE clauses).
///
/// `None` on either side means unbounded; combining multiple quals
/// narrows the interval (`max` on lo, `min` on hi).
#[derive(Default, Debug, Clone, Copy)]
struct TimeBounds {
    lo: Option<i64>, // inclusive
    hi: Option<i64>, // exclusive
}

impl TimeBounds {
    fn narrow_lo(&mut self, v: i64) {
        self.lo = Some(self.lo.map_or(v, |l| l.max(v)));
    }
    fn narrow_hi(&mut self, v: i64) {
        self.hi = Some(self.hi.map_or(v, |h| h.min(v)));
    }
    fn any(&self) -> bool {
        self.lo.is_some() || self.hi.is_some()
    }
}

unsafe fn classify_meta_quals(
    qual_list: *mut pg_sys::List,
    relid: pg_sys::Oid,
    time_column: &str,
    segment_by: &[String],
) -> Option<TimeBounds> {
    unsafe {
        let mut bounds = TimeBounds::default();
        if qual_list.is_null() {
            return Some(bounds);
        }
        for i in 0..(*qual_list).length {
            let node = pg_sys::list_nth(qual_list, i) as *const pg_sys::Node;
            if node.is_null() {
                return None;
            }
            if !classify_meta_qual_node(node, relid, time_column, segment_by, &mut bounds) {
                return None;
            }
        }
        Some(bounds)
    }
}

unsafe fn classify_meta_qual_node(
    node: *const pg_sys::Node,
    relid: pg_sys::Oid,
    time_column: &str,
    segment_by: &[String],
    bounds: &mut TimeBounds,
) -> bool {
    unsafe {
        if node.is_null() {
            return false;
        }
        // AND: recurse into every arm.
        if (*node).type_ == pg_sys::NodeTag::T_BoolExpr {
            let be = node as *const pg_sys::BoolExpr;
            if (*be).boolop != pg_sys::BoolExprType::AND_EXPR {
                return false;
            }
            let args = (*be).args;
            if args.is_null() {
                return false;
            }
            for i in 0..(*args).length {
                let arg = pg_sys::list_nth(args, i) as *const pg_sys::Node;
                if !classify_meta_qual_node(arg, relid, time_column, segment_by, bounds) {
                    return false;
                }
            }
            return true;
        }
        if (*node).type_ != pg_sys::NodeTag::T_OpExpr {
            return false;
        }
        let opexpr = node as *const pg_sys::OpExpr;
        let args = (*opexpr).args;
        if args.is_null() || (*args).length != 2 {
            return false;
        }
        let a0 = unwrap_relabel_node(pg_sys::list_nth(args, 0) as *const pg_sys::Node);
        let a1 = unwrap_relabel_node(pg_sys::list_nth(args, 1) as *const pg_sys::Node);
        // Identify which side is the Var; the other side must be Const.
        let (var_node, const_node, is_const_left) = if (*a0).type_ == pg_sys::NodeTag::T_Var
            && (*a1).type_ == pg_sys::NodeTag::T_Const
        {
            (a0, a1, false)
        } else if (*a1).type_ == pg_sys::NodeTag::T_Var && (*a0).type_ == pg_sys::NodeTag::T_Const {
            (a1, a0, true)
        } else {
            return false;
        };

        let var = var_node as *const pg_sys::Var;
        let attno = (*var).varattno;
        if attno <= 0 {
            return false;
        }
        let col_name_ptr = pg_sys::get_attname(relid, attno, true);
        if col_name_ptr.is_null() {
            return false;
        }
        let col_name = std::ffi::CStr::from_ptr(col_name_ptr)
            .to_string_lossy()
            .into_owned();

        let is_time = col_name == time_column;
        let is_seg = segment_by.iter().any(|s| s == &col_name);
        if !is_time && !is_seg {
            return false;
        }

        let opname_ptr = pg_sys::get_opname((*opexpr).opno);
        if opname_ptr.is_null() {
            return false;
        }
        let opname = std::ffi::CStr::from_ptr(opname_ptr).to_string_lossy();

        if is_seg {
            // Segment-by: equality only — `extract_segment_filters`
            // only matches `=`, and equality on segment_by is safe
            // because segmentation partitions rows along that value
            // (every row in a surviving segment satisfies the eq).
            return opname == "=";
        }

        // Time column: accept range bounds.  Reject equality — a row
        // with ts=C survives the WHERE but other rows in the same
        // segment (with ts≠C) don't; segment aggregates would overcount.
        // Safety of range bounds is enforced separately by
        // `partitions_contain_time_range` (see hook call site).
        let c = const_node as *const pg_sys::Const;
        if (*c).constisnull {
            return false;
        }
        if !matches!(
            (*c).consttype,
            pg_sys::TIMESTAMPTZOID | pg_sys::TIMESTAMPOID | pg_sys::DATEOID
        ) {
            // Only the internal-i64-encoded time types are comparable
            // to our `deltax.deltax_partition.range_start/range_end` datums.
            return false;
        }
        let v = (*c).constvalue.value() as i64;
        let v_pg_us = if (*c).consttype == pg_sys::DATEOID {
            // DATE datum is int32 days since PG epoch; convert to µs.
            (v as i32 as i64) * 86_400_000_000
        } else {
            v
        };

        // Normalize: when the Const is on the LEFT (`C op ts`),
        // commute the operator so we reason as `ts op' C`.
        let normalized: &str = if is_const_left {
            match opname.as_ref() {
                "<" => ">",
                "<=" => ">=",
                ">" => "<",
                ">=" => "<=",
                "=" => "=",
                _ => return false,
            }
        } else {
            match opname.as_ref() {
                "<" | "<=" | ">" | ">=" | "=" => opname.as_ref(),
                _ => return false,
            }
        };

        match normalized {
            ">=" => {
                bounds.narrow_lo(v_pg_us);
                true
            }
            ">" => {
                bounds.narrow_lo(v_pg_us.saturating_add(1));
                true
            }
            "<" => {
                bounds.narrow_hi(v_pg_us);
                true
            }
            "<=" => {
                bounds.narrow_hi(v_pg_us.saturating_add(1));
                true
            }
            "=" => false, // unsafe: see comment above
            _ => false,
        }
    }
}

/// Verify that every surviving partition's `[range_start, range_end)`
/// is fully contained in `bounds`.  When that's true, every row in
/// every segment of every surviving partition also satisfies the
/// time-WHERE — so `row_count` / `col_sums` / `col_minmax` from the
/// per-segment metadata are exact for the query.
///
/// Called from the planner when a time-range WHERE is present.
/// Returns `false` on any lookup failure so we fall through to
/// DeltaXAgg rather than risk overcounting.
unsafe fn partitions_contain_time_range(
    companion_oids: &[pg_sys::Oid],
    bounds: &TimeBounds,
) -> bool {
    unsafe {
        if !bounds.any() {
            return true;
        }
        // Collect partition names by stripping the `_meta` suffix from
        // each companion table name.
        let mut part_names: Vec<String> = Vec::with_capacity(companion_oids.len());
        for &oid in companion_oids {
            let name_ptr = pg_sys::get_rel_name(oid);
            if name_ptr.is_null() {
                return false;
            }
            let name = std::ffi::CStr::from_ptr(name_ptr).to_string_lossy();
            let part_name = name.strip_suffix("_meta").unwrap_or(&name).to_string();
            part_names.push(part_name);
        }

        // Build an ANY($1) query to fetch all partition ranges in one
        // round trip.  TIMESTAMPTZ columns return i64 PG-epoch µs —
        // same unit we normalized `bounds` to.
        let rows: Option<Vec<(i64, i64)>> = Spi::connect(|client| {
            let names_array: Vec<&str> = part_names.iter().map(|s| s.as_str()).collect();
            let tuples = client
                .select(
                    "SELECT range_start, range_end FROM deltax.deltax_partition \
                     WHERE table_name = ANY($1)",
                    None,
                    &[names_array.into()],
                )
                .ok()?;
            let mut out = Vec::new();
            for row in tuples {
                let rs = row
                    .get_datum_by_ordinal(1)
                    .ok()?
                    .value::<pgrx::datum::TimestampWithTimeZone>()
                    .ok()??;
                let re = row
                    .get_datum_by_ordinal(2)
                    .ok()?
                    .value::<pgrx::datum::TimestampWithTimeZone>()
                    .ok()??;
                // Into PG-epoch microseconds (the internal TIMESTAMPTZ rep).
                let rs_us: i64 = pg_sys::TimestampTz::from(rs);
                let re_us: i64 = pg_sys::TimestampTz::from(re);
                out.push((rs_us, re_us));
            }
            Some(out)
        });

        let rows = match rows {
            Some(r) if r.len() == part_names.len() => r,
            _ => return false, // any lookup gap → bail to the slow path
        };

        for (rs, re) in rows {
            if let Some(lo) = bounds.lo
                && rs < lo
            {
                return false;
            }
            if let Some(hi) = bounds.hi
                && re > hi
            {
                return false;
            }
        }
        true
    }
}

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
        use super::exec::{CaseWhenClause, CaseWhenSpec, CaseWhenValue};

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
            let when_node =
                (*(*args_list).elements.add(i as usize)).ptr_value as *const pg_sys::Node;
            if when_node.is_null() || (*when_node).type_ != pg_sys::NodeTag::T_CaseWhen {
                return None;
            }
            let case_when = when_node as *const pg_sys::CaseWhen;

            // Parse conditions from the WHEN expr
            let conditions =
                parse_case_when_conditions(root, (*case_when).expr as *const pg_sys::Node)?;
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

        Some(CaseWhenCondition {
            col_idx: col_idx as usize,
            op,
            const_val,
        })
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
            pg_sys::get_atttypetypmodcoll(
                (*rte).relid,
                (*var_node).varattno,
                &mut type_oid,
                &mut typmod,
                &mut collation,
            );
            if type_oid != pg_sys::TEXTOID
                && type_oid != pg_sys::VARCHAROID
                && type_oid != pg_sys::BPCHAROID
            {
                return None; // Only text column refs supported
            }
            Some(CaseWhenValue::ColumnRef(col_idx as usize))
        } else if (*expr).type_ == pg_sys::NodeTag::T_Const {
            let const_node = expr as *const pg_sys::Const;
            if (*const_node).constisnull {
                return Some(CaseWhenValue::StringConst(String::new()));
            }
            let const_type = (*const_node).consttype;
            if const_type != pg_sys::TEXTOID
                && const_type != pg_sys::VARCHAROID
                && const_type != pg_sys::BPCHAROID
            {
                return None; // Only string constants supported
            }
            let cstr = pg_sys::text_to_cstring((*const_node).constvalue.cast_mut_ptr());
            let s = std::ffi::CStr::from_ptr(cstr)
                .to_string_lossy()
                .into_owned();
            pg_sys::pfree(cstr as *mut _);
            Some(CaseWhenValue::StringConst(s))
        } else {
            None // Unsupported value type
        }
    }
}

/// H.2: recognizer for the JSONBench Q3/Q4 Aggref shape:
///
/// ```text
/// timestamptz_pl_interval(
///     Const(timestamptz, EPOCH_PGUS),
///     interval_mul(
///         Const(interval, UNIT_USECS),
///         FuncExpr(int8 → float8, [chain (data->>'time_us')::bigint])
///     )
/// )
/// ```
///
/// MIN/MAX over this expression equals MIN/MAX over `time_us` (the bigint
/// chain) shifted by a constant — `pg_us = epoch_pgus + unit_usecs * time_us`.
/// We pick MIN/MAX on the raw bigint and apply the affine shift at finalize
/// via `OutputTransform::PgUsShift { delta }`.
///
/// Returns `Some((col_idx, type_oid, delta))` where `col_idx` is the
/// synthetic chain column, `type_oid = INT8OID` (storage type), and `delta`
/// is the i64 µs offset added at emit time. Falls back when:
///  - constants don't reduce to a positive integer µs coefficient,
///  - the unit coefficient × INT8 max would overflow i64,
///  - the chain doesn't resolve via `AggChainCtx`.
unsafe fn try_match_timestamp_interval_min_max(
    ctx: &super::json_extract::AggChainCtx,
    arg_expr: *const pg_sys::Node,
) -> Option<(i32, pg_sys::Oid, i64)> {
    unsafe {
        if arg_expr.is_null() || (*arg_expr).type_ != pg_sys::NodeTag::T_OpExpr {
            return None;
        }
        let outer = arg_expr as *const pg_sys::OpExpr;
        // Outer op must be `timestamptz + interval` returning timestamptz.
        // Resolve by name + operand types to be PG-version-safe.
        if !is_op_named(
            (*outer).opno,
            "+",
            Some(pg_sys::TIMESTAMPTZOID),
            Some(pg_sys::INTERVALOID),
            pg_sys::TIMESTAMPTZOID,
        ) {
            return None;
        }
        let oargs = (*outer).args;
        if oargs.is_null() || (*oargs).length != 2 {
            return None;
        }
        let l = (*(*oargs).elements.add(0)).ptr_value as *const pg_sys::Node;
        let r = (*(*oargs).elements.add(1)).ptr_value as *const pg_sys::Node;
        if l.is_null() || r.is_null() {
            return None;
        }

        // Identify epoch Const (timestamptz) and the inner interval_mul OpExpr.
        let (epoch_const, inner_op): (*const pg_sys::Const, *const pg_sys::OpExpr) =
            if (*l).type_ == pg_sys::NodeTag::T_Const && (*r).type_ == pg_sys::NodeTag::T_OpExpr {
                (l as *const pg_sys::Const, r as *const pg_sys::OpExpr)
            } else if (*r).type_ == pg_sys::NodeTag::T_Const
                && (*l).type_ == pg_sys::NodeTag::T_OpExpr
            {
                (r as *const pg_sys::Const, l as *const pg_sys::OpExpr)
            } else {
                return None;
            };
        if (*epoch_const).constisnull || (*epoch_const).consttype != pg_sys::TIMESTAMPTZOID {
            return None;
        }
        let epoch_pgus: i64 = (*epoch_const).constvalue.value() as i64;

        // Inner op must be `interval * <numeric>` returning interval. The
        // numeric side is typically float8 (PG's preferred coercion for
        // bigint × interval), but we accept either operand position.
        if !is_op_named((*inner_op).opno, "*", None, None, pg_sys::INTERVALOID) {
            return None;
        }
        let iargs = (*inner_op).args;
        if iargs.is_null() || (*iargs).length != 2 {
            return None;
        }
        let il = (*(*iargs).elements.add(0)).ptr_value as *const pg_sys::Node;
        let ir = (*(*iargs).elements.add(1)).ptr_value as *const pg_sys::Node;
        if il.is_null() || ir.is_null() {
            return None;
        }

        // Pick the Const(interval) operand, the other is the numeric chain.
        let (iv_const, num_node): (*const pg_sys::Const, *const pg_sys::Node) = if (*il).type_
            == pg_sys::NodeTag::T_Const
            && (*(il as *const pg_sys::Const)).consttype == pg_sys::INTERVALOID
        {
            (il as *const pg_sys::Const, ir)
        } else if (*ir).type_ == pg_sys::NodeTag::T_Const
            && (*(ir as *const pg_sys::Const)).consttype == pg_sys::INTERVALOID
        {
            (ir as *const pg_sys::Const, il)
        } else {
            return None;
        };
        if (*iv_const).constisnull {
            return None;
        }

        // Decode interval Const → i64 µs coefficient. PG's Interval is
        // {time: i64 µs, day: i32, month: i32}. Reject month/day intervals
        // (variable length) — only fixed-width `time` units (sub-day) are
        // representable as a constant µs coefficient.
        let iv_ptr = (*iv_const).constvalue.value() as *const pg_sys::Interval;
        if iv_ptr.is_null() {
            return None;
        }
        let iv = *iv_ptr;
        if iv.month != 0 || iv.day != 0 || iv.time <= 0 {
            return None;
        }
        let coeff_us: i64 = iv.time;

        // Strip the int8 → float8 cast from the numeric side. The cast may
        // be a FuncExpr (funcid for `float8(int8)` is 482) or RelabelType.
        let stripped = strip_numeric_cast(num_node);
        // Resolve the chain via AggChainCtx → must yield (col_idx, INT8OID).
        let (col_idx, chain_type) = ctx.match_to_synthetic(stripped)?;
        if chain_type != pg_sys::INT8OID {
            return None;
        }

        // For Q3/Q4 with `1 microsecond`, coeff_us = 1 → no overflow risk.
        // For larger units (e.g., 1ms = 1000), the worst-case product
        // `coeff_us * INT8::MAX` overflows i64 — bail unless coeff_us fits
        // in a small bound. JSONBench's `time_us` is below 2^53; we apply
        // a conservative gate.
        if coeff_us != 1 {
            // Reject anything other than identity for now — H.2 only
            // targets the 1µs unit. Larger coefficients need a runtime
            // multiply per row, which conflicts with the pure-min-of-i64
            // shape of MinInt/MaxInt.
            return None;
        }

        // delta = epoch_pgus (since coeff_us = 1, pg_us = time_us + epoch_pgus).
        Some((col_idx, pg_sys::INT8OID, epoch_pgus))
    }
}

/// Helper: check an OpExpr's identity by name + (optional) left/right
/// operand types + result type. PG-version-safe alternative to hard-coding
/// OIDs (e.g., 1327 for timestamptz_pl_interval).
unsafe fn is_op_named(
    opno: pg_sys::Oid,
    expected_name: &str,
    expected_left: Option<pg_sys::Oid>,
    expected_right: Option<pg_sys::Oid>,
    expected_result: pg_sys::Oid,
) -> bool {
    unsafe {
        let opname_ptr = pg_sys::get_opname(opno);
        if opname_ptr.is_null() {
            return false;
        }
        let opname = std::ffi::CStr::from_ptr(opname_ptr).to_str().unwrap_or("");
        if opname != expected_name {
            return false;
        }
        let tup = pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::OPEROID as i32,
            pg_sys::Datum::from(u32::from(opno) as usize),
        );
        if tup.is_null() {
            return false;
        }
        let op = pg_sys::GETSTRUCT(tup) as *const pg_sys::FormData_pg_operator;
        let ok = (*op).oprresult == expected_result
            && expected_left.is_none_or(|t| (*op).oprleft == t)
            && expected_right.is_none_or(|t| (*op).oprright == t);
        pg_sys::ReleaseSysCache(tup);
        ok
    }
}

/// H.2: predicate for the `non_agg_op_exprs` tlist validator. Returns true
/// when `node` is composed entirely of `Aggref` references, constants, and
/// pure expression wrappers (`OpExpr`, `FuncExpr`, `RelabelType`,
/// `CoerceViaIO`). Such expressions are always valid in a post-aggregation
/// projection — PG computes them after `Aggregate` finishes — so they don't
/// need to match a GROUP BY column.
///
/// Bails on any `Var` reference (which would have to match GROUP BY) and on
/// any other node type (`SubLink`, `WindowFunc`, etc.) we don't want to
/// silently accept. The walk is intentionally narrow.
unsafe fn expr_only_uses_aggrefs_and_consts(node: *const pg_sys::Node) -> bool {
    unsafe {
        if node.is_null() {
            return true;
        }
        match (*node).type_ {
            pg_sys::NodeTag::T_Aggref | pg_sys::NodeTag::T_Const => true,
            pg_sys::NodeTag::T_RelabelType => {
                let r = node as *const pg_sys::RelabelType;
                expr_only_uses_aggrefs_and_consts((*r).arg as *const pg_sys::Node)
            }
            pg_sys::NodeTag::T_CoerceViaIO => {
                let c = node as *const pg_sys::CoerceViaIO;
                expr_only_uses_aggrefs_and_consts((*c).arg as *const pg_sys::Node)
            }
            pg_sys::NodeTag::T_OpExpr => {
                let op = node as *const pg_sys::OpExpr;
                let args = (*op).args;
                if args.is_null() {
                    return true;
                }
                for i in 0..(*args).length {
                    let a = (*(*args).elements.add(i as usize)).ptr_value as *const pg_sys::Node;
                    if !expr_only_uses_aggrefs_and_consts(a) {
                        return false;
                    }
                }
                true
            }
            pg_sys::NodeTag::T_FuncExpr => {
                let f = node as *const pg_sys::FuncExpr;
                let args = (*f).args;
                if args.is_null() {
                    return true;
                }
                for i in 0..(*args).length {
                    let a = (*(*args).elements.add(i as usize)).ptr_value as *const pg_sys::Node;
                    if !expr_only_uses_aggrefs_and_consts(a) {
                        return false;
                    }
                }
                true
            }
            _ => false,
        }
    }
}

/// Walk an Expr tree and push every `Aggref` encountered into `out`. Descends
/// through the same wrappers that `expr_only_uses_aggrefs_and_consts` accepts
/// (`OpExpr`, `FuncExpr`, `RelabelType`, `CoerceViaIO`). Stops at any other
/// node type — in particular Vars, which signal a GROUP BY reference rather
/// than a nested aggregate.
unsafe fn collect_aggrefs_in_expr(node: *const pg_sys::Node, out: &mut Vec<*const pg_sys::Aggref>) {
    unsafe {
        if node.is_null() {
            return;
        }
        match (*node).type_ {
            pg_sys::NodeTag::T_Aggref => out.push(node as *const pg_sys::Aggref),
            pg_sys::NodeTag::T_RelabelType => {
                let r = node as *const pg_sys::RelabelType;
                collect_aggrefs_in_expr((*r).arg as *const pg_sys::Node, out);
            }
            pg_sys::NodeTag::T_CoerceViaIO => {
                let c = node as *const pg_sys::CoerceViaIO;
                collect_aggrefs_in_expr((*c).arg as *const pg_sys::Node, out);
            }
            pg_sys::NodeTag::T_OpExpr => {
                let op = node as *const pg_sys::OpExpr;
                let args = (*op).args;
                if args.is_null() {
                    return;
                }
                for i in 0..(*args).length {
                    let a = (*(*args).elements.add(i as usize)).ptr_value as *const pg_sys::Node;
                    collect_aggrefs_in_expr(a, out);
                }
            }
            pg_sys::NodeTag::T_FuncExpr => {
                let f = node as *const pg_sys::FuncExpr;
                let args = (*f).args;
                if args.is_null() {
                    return;
                }
                for i in 0..(*args).length {
                    let a = (*(*args).elements.add(i as usize)).ptr_value as *const pg_sys::Node;
                    collect_aggrefs_in_expr(a, out);
                }
            }
            _ => {}
        }
    }
}

/// Strip outer monotonic wrappers from a sort expression so the inner
/// `MAX - MIN` shape can be recognized. Walks through:
///
/// - `*` with a positive numeric constant on one side (preserves order)
/// - `FuncExpr(extract / date_part, [Const('epoch'|'milliseconds'|...), arg])`
///   which is monotonic in `arg` for the supported field constants
/// - `RelabelType` / `CoerceViaIO` casts
///
/// Returns the inner expression. For Q4's sort key
/// `EXTRACT(EPOCH FROM (MAX - MIN)) * 1000` this returns the inner
/// `MAX - MIN` OpExpr.
unsafe fn strip_monotonic_topn_wrappers(node: *const pg_sys::Node) -> *const pg_sys::Node {
    unsafe {
        let mut cur = node;
        loop {
            if cur.is_null() {
                return cur;
            }
            match (*cur).type_ {
                pg_sys::NodeTag::T_OpExpr => {
                    // Strip `*` by a positive constant — preserves ordering.
                    let op = cur as *const pg_sys::OpExpr;
                    let opname_ptr = pg_sys::get_opname((*op).opno);
                    if opname_ptr.is_null() {
                        return cur;
                    }
                    let opname = std::ffi::CStr::from_ptr(opname_ptr).to_str().unwrap_or("");
                    if opname != "*" {
                        return cur;
                    }
                    let args = (*op).args;
                    if args.is_null() || (*args).length != 2 {
                        return cur;
                    }
                    let l = (*(*args).elements.add(0)).ptr_value as *const pg_sys::Node;
                    let r = (*(*args).elements.add(1)).ptr_value as *const pg_sys::Node;
                    if l.is_null() || r.is_null() {
                        return cur;
                    }
                    let l_is_const = (*l).type_ == pg_sys::NodeTag::T_Const;
                    let r_is_const = (*r).type_ == pg_sys::NodeTag::T_Const;
                    if l_is_const && !r_is_const {
                        // Bail on non-positive constants — they'd flip ordering.
                        // Conservative: only positive numerics preserve direction.
                        if !const_is_positive_numeric(l as *const pg_sys::Const) {
                            return cur;
                        }
                        cur = r;
                        continue;
                    } else if r_is_const && !l_is_const {
                        if !const_is_positive_numeric(r as *const pg_sys::Const) {
                            return cur;
                        }
                        cur = l;
                        continue;
                    }
                    return cur;
                }
                pg_sys::NodeTag::T_FuncExpr => {
                    let f = cur as *const pg_sys::FuncExpr;
                    let fname_ptr = pg_sys::get_func_name((*f).funcid);
                    if fname_ptr.is_null() {
                        return cur;
                    }
                    let fname = std::ffi::CStr::from_ptr(fname_ptr).to_str().unwrap_or("");
                    // EXTRACT is rewritten by PG to either `extract` (PG16+) or
                    // `date_part` (older). Both have signature `(text, ?)` —
                    // first arg is the field-name Const, last arg is the
                    // value expression. EXTRACT(EPOCH FROM interval) is
                    // monotonic in interval microseconds.
                    if fname != "extract" && fname != "date_part" {
                        return cur;
                    }
                    let args = (*f).args;
                    if args.is_null() || (*args).length < 2 {
                        return cur;
                    }
                    let n = (*args).length as usize;
                    cur = (*(*args).elements.add(n - 1)).ptr_value as *const pg_sys::Node;
                    continue;
                }
                pg_sys::NodeTag::T_RelabelType => {
                    let r = cur as *const pg_sys::RelabelType;
                    cur = (*r).arg as *const pg_sys::Node;
                    continue;
                }
                pg_sys::NodeTag::T_CoerceViaIO => {
                    let c = cur as *const pg_sys::CoerceViaIO;
                    cur = (*c).arg as *const pg_sys::Node;
                    continue;
                }
                _ => return cur,
            }
        }
    }
}

/// Returns true when the Const holds a positive numeric value (int2/int4/int8/
/// float4/float8/numeric). Used by `strip_monotonic_topn_wrappers` to confirm
/// that stripping a `*` doesn't flip sort direction.
unsafe fn const_is_positive_numeric(c: *const pg_sys::Const) -> bool {
    unsafe {
        if (*c).constisnull {
            return false;
        }
        match (*c).consttype {
            pg_sys::INT2OID => ((*c).constvalue.value() as i16) > 0,
            pg_sys::INT4OID => ((*c).constvalue.value() as i32) > 0,
            pg_sys::INT8OID => ((*c).constvalue.value() as i64) > 0,
            pg_sys::FLOAT4OID => f32::from_bits((*c).constvalue.value() as u32) > 0.0,
            pg_sys::FLOAT8OID => f64::from_bits((*c).constvalue.value() as u64) > 0.0,
            pg_sys::NUMERICOID => {
                // PG numeric format encoding (numeric.c):
                //   top 2 bits of n_header decode as:
                //     00  NUMERIC_POS     (long format, positive)
                //     01  NUMERIC_NEG     (long format, negative)
                //     10  NUMERIC_SHORT   (compact format; sign in bit 0x2000)
                //     11  NUMERIC_SPECIAL (NaN / ±inf)
                // We accept anything that's not NEG / NaN / -inf. `'1000'::
                // numeric` is the SHORT form on modern PG.
                let varlena_ptr = (*c).constvalue.cast_mut_ptr::<pg_sys::varlena>();
                if varlena_ptr.is_null() {
                    return false;
                }
                let detoasted = pg_sys::pg_detoast_datum(varlena_ptr);
                // `vardata_any` returns `*const c_char`, whose signedness
                // depends on platform/ABI (i8 on x86_64 PG 18, u8 on aarch64).
                // Cast through `*const u8` so this compiles in both worlds.
                #[allow(clippy::unnecessary_cast)]
                let data = pgrx::vardata_any(detoasted) as *const u8;
                let header = u16::from_le_bytes([*data, *data.add(1)]);
                let was_toasted = detoasted != varlena_ptr;
                if was_toasted {
                    pg_sys::pfree(detoasted as *mut _);
                }
                let top2 = (header >> 14) & 0x3;
                match top2 {
                    0b00 => true,                   // long, positive
                    0b01 => false,                  // long, negative
                    0b10 => (header & 0x2000) == 0, // short — bit 0x2000 = neg
                    _ => false,                     // NaN/±inf
                }
            }
            _ => false,
        }
    }
}

/// Try to recognize a derived MIN/MAX-difference sort key shape:
/// `<monotonic-wrappers>(MAX(x) - MIN(x))`. Returns the (max, min) Aggref
/// indices into the caller's `aggrefs` vec if recognized.
///
/// Designed for JSONBench Q4 — `ORDER BY EXTRACT(EPOCH FROM (MAX(t) - MIN(t))) * 1000 DESC`.
/// The strip step handles the EXTRACT and `* 1000` wrappers; the inner shape
/// is two Aggrefs subtracted.
unsafe fn try_match_derived_minmax_topn(
    sort_expr: *const pg_sys::Node,
    aggrefs: &[*const pg_sys::Aggref],
) -> Option<(usize, usize)> {
    unsafe {
        let inner = strip_monotonic_topn_wrappers(sort_expr);
        if inner.is_null() || (*inner).type_ != pg_sys::NodeTag::T_OpExpr {
            return None;
        }
        let op = inner as *const pg_sys::OpExpr;
        let opname_ptr = pg_sys::get_opname((*op).opno);
        if opname_ptr.is_null() {
            return None;
        }
        let opname = std::ffi::CStr::from_ptr(opname_ptr).to_str().unwrap_or("");
        if opname != "-" {
            return None;
        }
        let args = (*op).args;
        if args.is_null() || (*args).length != 2 {
            return None;
        }
        let l_raw = (*(*args).elements.add(0)).ptr_value as *const pg_sys::Node;
        let r_raw = (*(*args).elements.add(1)).ptr_value as *const pg_sys::Node;
        if l_raw.is_null() || r_raw.is_null() {
            return None;
        }
        let l = unwrap_relabel_node(l_raw);
        let r = unwrap_relabel_node(r_raw);
        if (*l).type_ != pg_sys::NodeTag::T_Aggref || (*r).type_ != pg_sys::NodeTag::T_Aggref {
            return None;
        }
        let l_agg = l as *const pg_sys::Aggref;
        let r_agg = r as *const pg_sys::Aggref;
        let l_name_ptr = pg_sys::get_func_name((*l_agg).aggfnoid);
        let r_name_ptr = pg_sys::get_func_name((*r_agg).aggfnoid);
        if l_name_ptr.is_null() || r_name_ptr.is_null() {
            return None;
        }
        let l_name = std::ffi::CStr::from_ptr(l_name_ptr).to_str().unwrap_or("");
        let r_name = std::ffi::CStr::from_ptr(r_name_ptr).to_str().unwrap_or("");
        if l_name != "max" || r_name != "min" {
            return None;
        }
        // Find indices in caller's aggrefs vec by pointer identity (the
        // sort tree references the same Aggref pointers PG stitched into
        // the tlist, so pointer-equal match is exact).
        let max_idx = aggrefs.iter().position(|&a| std::ptr::eq(a, l_agg))?;
        let min_idx = aggrefs.iter().position(|&a| std::ptr::eq(a, r_agg))?;
        Some((max_idx, min_idx))
    }
}

/// Strip `int8 → float8` cast wrappers (`FuncExpr` with funcid for
/// `float8(int8)` = 482, or `RelabelType`) so the underlying chain can be
/// matched by `AggChainCtx`.
unsafe fn strip_numeric_cast(node: *const pg_sys::Node) -> *const pg_sys::Node {
    unsafe {
        let mut cur = node;
        loop {
            if cur.is_null() {
                return cur;
            }
            match (*cur).type_ {
                pg_sys::NodeTag::T_FuncExpr => {
                    let f = cur as *const pg_sys::FuncExpr;
                    let args = (*f).args;
                    if !args.is_null()
                        && (*args).length == 1
                        // funcid 482 is float8(int8); accept any funcformat
                        // that produces a float — the OutputTransform layer
                        // only cares that the inner chain is INT8.
                        && (*f).funcid == pg_sys::Oid::from(482u32)
                    {
                        cur = (*(*args).elements.add(0)).ptr_value as *const pg_sys::Node;
                        continue;
                    }
                    return cur;
                }
                pg_sys::NodeTag::T_RelabelType => {
                    let r = cur as *const pg_sys::RelabelType;
                    cur = (*r).arg as *const pg_sys::Node;
                    continue;
                }
                _ => return cur,
            }
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
                // Non-aggregate FuncExpr in target list — must match a GROUP BY expression.
                // If it contains nested Aggrefs (e.g. EXTRACT(EPOCH FROM MAX(x))),
                // collect them so they get classified — the agg-only-tree validator
                // below accepts the surrounding shape.
                collect_aggrefs_in_expr(expr, &mut aggrefs);
                non_agg_func_exprs.push((i, expr as *const pg_sys::FuncExpr));
            } else if (*expr).type_ == pg_sys::NodeTag::T_OpExpr && has_group_by {
                // Non-aggregate OpExpr in target list (e.g. col - 1) — must match a GROUP BY
                // expression. If it contains nested Aggrefs (Q4's
                // `EXTRACT(EPOCH FROM (MAX(...) - MIN(...))) * 1000`), collect them so they
                // get classified — the agg-only-tree validator below accepts the surrounding shape.
                collect_aggrefs_in_expr(expr, &mut aggrefs);
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
        // Fast path: Single COUNT(*) with no GROUP BY, no HAVING → DeltaXCount
        //
        // - No WHERE:  catalog lookup of `deltax.deltax_partition.row_count`.
        // - With WHERE, if every qual is a time-column range/equality
        //   or segment-by equality, serialize the quals into the path
        //   and prune at segment level inside the executor. Otherwise
        //   fall through to DeltaXAgg which decompresses and filters.
        // =====================================================================
        if aggrefs.len() == 1 && (*aggrefs[0]).aggstar && !has_group_by && !has_having {
            if !has_where {
                path::add_count_star_path(root, output_rel, &companion_oids, std::ptr::null_mut());
                return;
            }
            if !crate::DISABLE_META_AGG_FASTPATH.get()
                && let Some(parent_oid) = find_inh_parent_oid(root)
                && let Some((time_col, seg_by)) = get_meta_cols(parent_oid)
            {
                let quals = extract_query_quals(root);
                if !quals.is_null()
                    && let Some(bounds) = classify_meta_quals(quals, parent_oid, &time_col, &seg_by)
                    && partitions_contain_time_range(&companion_oids, &bounds)
                {
                    path::add_count_star_path(root, output_rel, &companion_oids, quals);
                    return;
                }
            }
            // Else: fall through to DeltaXAgg.
        }

        // =====================================================================
        // Classify all aggregates
        // =====================================================================
        use super::exec::{AggExpr, AggType};

        let mut classified_aggs: Vec<path::AggSpec> = Vec::new();
        let mut all_minmax = true;
        let mut has_non_minmax = false;
        // Parallel flag for the broader "metadata-only answerable" path:
        // MIN/MAX/SUM/COUNT(col)/COUNT(*) on supported column types with
        // expr_kind == Column. SUM on INT8/NUMERIC falls out (result
        // type is NUMERIC; we don't build NUMERIC datums from i128 yet).
        let mut all_meta_answerable = true;

        // json_extract chain context — built once on first use, reused by
        // both the agg-arg classifier (this loop) and the GROUP BY classifier
        // below. `Some(None)` means we've checked and there's no extract
        // configuration / unsafe partitions; `None` means we haven't checked
        // yet. Only the chain-match branches consult it, so plain queries
        // pay a single SPI lookup at most (when they happen to hit a chain
        // Expr that no other branch recognises).
        let mut json_extract_ctx: Option<Option<super::json_extract::AggChainCtx>> = None;

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
                    output_transform: super::exec::OutputTransform::None,
                });
                all_minmax = false;
                has_non_minmax = true;
                // COUNT(*) is always meta-answerable.
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
            let mut agg_output_transform: super::exec::OutputTransform =
                super::exec::OutputTransform::None;
            let (col_idx, col_type_oid, expr_kind): (i32, pg_sys::Oid, AggExpr) = 'resolve: {
                // First, try interpreting the arg as a JSONB chain over a
                // synthetic column. This must come BEFORE the OpExpr branch
                // below (which expects `Var + Const` shapes) — a chain like
                // `data->>'did'` is itself an OpExpr but with the JSONB ->>
                // operator, so the Var+Const matcher would reject it.
                if json_extract_ctx.is_none() {
                    json_extract_ctx = Some(super::json_extract::AggChainCtx::from_root(root));
                }
                if let Some(ctx) = json_extract_ctx.as_ref().unwrap()
                    && let Some((col_idx, type_oid)) = ctx.match_to_synthetic(arg_expr)
                {
                    break 'resolve (col_idx, type_oid, AggExpr::Column);
                }

                // H.2: monotonic timestamptz-pl-interval recognizer for MIN/MAX.
                // Match the JSONBench Q3/Q4 shape:
                //   timestamptz_pl_interval(<const_tstz>, interval_mul(<const_iv>, float8(int8(chain))))
                // and lift it to MIN/MAX over the bigint synthetic with an
                // OutputTransform::PgUsShift applied at finalize. Only fires
                // when the inner chain resolves via the synthetic-Var path
                // and the constants reduce to an exact i64 µs delta.
                if let Some(ctx) = json_extract_ctx.as_ref().unwrap()
                    && let Some((c_idx, type_oid, delta)) =
                        try_match_timestamp_interval_min_max(ctx, arg_expr)
                {
                    agg_output_transform = super::exec::OutputTransform::PgUsShift { delta };
                    break 'resolve (c_idx, type_oid, AggExpr::Column);
                }

                let (var_node, ek): (*const pg_sys::Var, AggExpr) =
                    if (*arg_expr).type_ == pg_sys::NodeTag::T_Var {
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
                        let fn_name = std::ffi::CStr::from_ptr(fn_name_ptr).to_str().unwrap_or("");
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
                        let opname = std::ffi::CStr::from_ptr(opname_ptr).to_str().unwrap_or("");
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
                pg_sys::get_atttypetypmodcoll(
                    relid,
                    varattno,
                    &mut col_type_oid,
                    &mut col_typmod,
                    &mut col_collation,
                );

                (col_idx, col_type_oid, ek)
            };

            // For length() expressions, the effective type for aggregation is INT4
            let effective_col_type_oid = if expr_kind == AggExpr::LengthOf {
                pg_sys::INT4OID
            } else {
                col_type_oid
            };

            // Check for COUNT(DISTINCT ...)
            let is_distinct =
                !(*aggref).aggdistinct.is_null() && (*(*aggref).aggdistinct).length > 0;

            // Helper: meta-path eligibility for SUM depends on the
            // source column type. NUMERIC output (SUM(int8)/SUM(numeric))
            // isn't handled by the current sum_i128_to_datum (would need
            // numeric_in, see count_minmax.rs). Fall through to DeltaXAgg.
            //
            // `AggExpr::AddConst` (i.e. `SUM(col + N)`) qualifies: the offset
            // is folded in at finalize as `sum + const_offset * nonnull_count`,
            // both quantities available from per-segment metadata. This is
            // load-bearing for ClickBench Q29 (90× `SUM(col + N)` over 100M
            // rows — 7.7s → ~0.05s when the meta path fires).
            let sum_meta_ok = matches!(
                effective_col_type_oid,
                pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::FLOAT4OID | pg_sys::FLOAT8OID
            ) && matches!(expr_kind, AggExpr::Column | AggExpr::AddConst);
            let count_meta_ok = matches!(expr_kind, AggExpr::Column);

            match func_name {
                "sum" => {
                    classified_aggs.push(path::AggSpec {
                        agg_type: AggType::Sum,
                        col_idx,
                        result_type_oid: (*aggref).aggtype,
                        col_type_oid: effective_col_type_oid,
                        expr_kind,
                        const_offset: agg_const_offset,
                        output_transform: super::exec::OutputTransform::None,
                    });
                    all_minmax = false;
                    has_non_minmax = true;
                    if !sum_meta_ok {
                        all_meta_answerable = false;
                    }
                }
                "avg" => {
                    classified_aggs.push(path::AggSpec {
                        agg_type: AggType::Avg,
                        col_idx,
                        result_type_oid: (*aggref).aggtype,
                        col_type_oid: effective_col_type_oid,
                        expr_kind,
                        const_offset: agg_const_offset,
                        output_transform: super::exec::OutputTransform::None,
                    });
                    all_minmax = false;
                    has_non_minmax = true;
                    all_meta_answerable = false; // AVG not yet meta-answerable
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
                            output_transform: super::exec::OutputTransform::None,
                        });
                        all_meta_answerable = false;
                    } else {
                        classified_aggs.push(path::AggSpec {
                            agg_type: AggType::Count,
                            col_idx,
                            result_type_oid: (*aggref).aggtype,
                            col_type_oid: effective_col_type_oid,
                            expr_kind,
                            const_offset: agg_const_offset,
                            output_transform: super::exec::OutputTransform::None,
                        });
                        if !count_meta_ok {
                            all_meta_answerable = false;
                        }
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
                        output_transform: agg_output_transform,
                    });
                    if has_non_minmax {
                        // Mixed MIN/MAX with SUM/COUNT/AVG → falls through to general AggScan
                        all_minmax = false;
                    }
                    if !matches!(expr_kind, AggExpr::Column)
                        || !is_minmax_meta_type(effective_col_type_oid)
                        || !matches!(agg_output_transform, super::exec::OutputTransform::None)
                    {
                        all_meta_answerable = false;
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
                        output_transform: agg_output_transform,
                    });
                    if has_non_minmax {
                        all_minmax = false;
                    }
                    if !matches!(expr_kind, AggExpr::Column)
                        || !is_minmax_meta_type(effective_col_type_oid)
                        || !matches!(agg_output_transform, super::exec::OutputTransform::None)
                    {
                        all_meta_answerable = false;
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
        // Fast path: Every aggregate is MIN/MAX/SUM/COUNT(col)/COUNT(*)
        // answerable from per-segment metadata. Optionally with time-
        // column and/or segment-by equality WHERE clauses.
        // =====================================================================
        if all_meta_answerable
            && !has_group_by
            && !has_having
            && !crate::DISABLE_META_AGG_FASTPATH.get()
        {
            // Build the per-spec list, translating AggSpec into the
            // MinMaxAggSpec/MetaAggKind vocabulary.
            let mut minmax_specs: Vec<path::MinMaxAggSpec> = Vec::new();
            let mut ok = true;
            for (idx, &aggref) in aggrefs.iter().enumerate() {
                let agg = &classified_aggs[idx];
                let kind = match agg.agg_type {
                    AggType::Min => path::MetaAggKind::Min,
                    AggType::Max => path::MetaAggKind::Max,
                    AggType::Sum => path::MetaAggKind::Sum,
                    AggType::Count => path::MetaAggKind::CountCol,
                    AggType::CountStar => path::MetaAggKind::CountStar,
                    _ => {
                        ok = false;
                        break;
                    }
                };
                // Use the classified `AggSpec.col_idx` directly — it was
                // resolved upstream and already handles the `Var + Const`
                // shape (AggExpr::AddConst). varattno is 1-based attno;
                // col_idx is 0-based. CountStar has col_idx = -1.
                let varattno: i16 = if matches!(kind, path::MetaAggKind::CountStar) {
                    0
                } else {
                    (agg.col_idx + 1) as i16
                };
                let result_type_oid = (*aggref).aggtype;
                let mut typlen: i16 = 0;
                let mut typbyval: bool = false;
                pg_sys::get_typlenbyval(result_type_oid, &mut typlen, &mut typbyval);
                minmax_specs.push(path::MinMaxAggSpec {
                    kind,
                    varattno,
                    result_type_oid,
                    col_type_oid: agg.col_type_oid,
                    typlen,
                    typbyval,
                    const_offset: agg.const_offset,
                });
            }

            if ok && !minmax_specs.is_empty() {
                let qual_list_opt: Option<*mut pg_sys::List> = if has_where {
                    let quals = extract_query_quals(root);
                    if quals.is_null() {
                        None
                    } else {
                        let parent_oid = find_inh_parent_oid(root);
                        let cl = parent_oid
                            .and_then(|p| get_meta_cols(p).map(|m| (p, m)))
                            .and_then(|(p, (tc, sb))| {
                                classify_meta_quals(quals, p, &tc, &sb).map(|b| (p, b))
                            });
                        match cl {
                            Some((_, bounds))
                                if partitions_contain_time_range(&companion_oids, &bounds) =>
                            {
                                Some(quals)
                            }
                            _ => None, // fall through to DeltaXAgg
                        }
                    }
                } else {
                    Some(std::ptr::null_mut())
                };

                if let Some(qual_list) = qual_list_opt {
                    path::add_minmax_path(
                        root,
                        output_rel,
                        &companion_oids,
                        &minmax_specs,
                        qual_list,
                    );
                    return;
                }
                // else: fall through to DeltaXAgg
            }
        }

        // Legacy MIN/MAX-only path for backwards compatibility when
        // `all_meta_answerable` bailed out above (e.g. AVG in the mix).
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

                // Verify the companion table has _min_{colname} (time column in meta)
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
                    // Not in meta — check if normalized colstats table exists
                    let meta_name_ptr = pg_sys::get_rel_name(companion_oids[0]);
                    let meta_ns_oid = pg_sys::get_rel_namespace(companion_oids[0]);
                    let meta_name = std::ffi::CStr::from_ptr(meta_name_ptr).to_string_lossy();
                    let partition_name = meta_name.strip_suffix("_meta").unwrap_or(&meta_name);
                    let colstats_name = format!("{}_colstats", partition_name);
                    let colstats_cname = std::ffi::CString::new(colstats_name).unwrap();
                    let colstats_oid =
                        pg_sys::get_relname_relid(colstats_cname.as_ptr(), meta_ns_oid);
                    if colstats_oid == pg_sys::InvalidOid {
                        return;
                    }
                    // Normalized colstats only stores encoded i64 min/max — only orderable types
                    let col_type_oid = pg_sys::get_atttype(relid, varattno);
                    if !matches!(
                        col_type_oid,
                        pg_sys::INT2OID
                            | pg_sys::INT4OID
                            | pg_sys::INT8OID
                            | pg_sys::FLOAT4OID
                            | pg_sys::FLOAT8OID
                            | pg_sys::DATEOID
                            | pg_sys::TIMESTAMPOID
                            | pg_sys::TIMESTAMPTZOID
                    ) {
                        return;
                    }
                }

                let result_type_oid = (*aggref).aggtype;
                let mut typlen: i16 = 0;
                let mut typbyval: bool = false;
                pg_sys::get_typlenbyval(result_type_oid, &mut typlen, &mut typbyval);

                let col_type_oid = pg_sys::get_atttype(relid, varattno);
                minmax_specs.push(path::MinMaxAggSpec {
                    kind: if is_min {
                        path::MetaAggKind::Min
                    } else {
                        path::MetaAggKind::Max
                    },
                    varattno,
                    result_type_oid,
                    col_type_oid,
                    typlen,
                    typbyval,
                    const_offset: 0,
                });
            }

            if !minmax_specs.is_empty() {
                path::add_minmax_path(
                    root,
                    output_rel,
                    &companion_oids,
                    &minmax_specs,
                    std::ptr::null_mut(),
                );
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
                        if rel.is_null() {
                            continue;
                        }
                        let bri = (*rel).baserestrictinfo;
                        if bri.is_null() {
                            continue;
                        }
                        for i in 0..(*bri).length {
                            let ri = pg_sys::list_nth(bri, i) as *const pg_sys::RestrictInfo;
                            if !ri.is_null() && !(*ri).clause.is_null() {
                                nodes.push((*ri).clause as *const pg_sys::Node);
                            }
                        }
                        if !nodes.is_empty() {
                            break;
                        }
                    }
                    nodes
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
                        let opname = std::ffi::CStr::from_ptr(opname_ptr).to_str().unwrap_or("");

                        let is_like = opname == "~~";
                        let is_not_like = opname == "!~~";
                        let is_recognized_cmp =
                            matches!(opname, "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=");

                        if !is_like && !is_not_like && !is_recognized_cmp {
                            return; // unrecognized operator
                        }

                        let raw_arg0 = (*(*args).elements.add(0)).ptr_value as *const pg_sys::Node;
                        let raw_arg1 = (*(*args).elements.add(1)).ptr_value as *const pg_sys::Node;
                        if raw_arg0.is_null() || raw_arg1.is_null() {
                            return;
                        }

                        let a0 = unwrap_relabel_node(raw_arg0);
                        let a1 = unwrap_relabel_node(raw_arg1);

                        // Resolve `(Var/chain, Const)` or `(Const, Var/chain)`
                        // — chain Exprs map to synthetic Vars whose type is
                        // the spec's `target_kind`. `plan_agg_path` rewrites
                        // chains in the qual list before serialisation, so by
                        // execution time `extract_batch_quals` sees a real
                        // Var; this validator just confirms the shape is
                        // pushable.
                        if json_extract_ctx.is_none() {
                            json_extract_ctx =
                                Some(super::json_extract::AggChainCtx::from_root(root));
                        }
                        let ctx_ref = json_extract_ctx.as_ref().unwrap().as_ref();

                        let resolve_side = |node: *const pg_sys::Node| -> Option<pg_sys::Oid> {
                            if (*node).type_ == pg_sys::NodeTag::T_Var {
                                return Some((*(node as *const pg_sys::Var)).vartype);
                            }
                            ctx_ref
                                .and_then(|c| c.match_to_synthetic(node))
                                .map(|(_idx, type_oid)| type_oid)
                        };

                        let lhs_type = resolve_side(a0);
                        let rhs_type = resolve_side(a1);

                        let (type_oid, var_on_left, const_node) = if let Some(ty) = lhs_type
                            && (*a1).type_ == pg_sys::NodeTag::T_Const
                        {
                            (ty, true, a1 as *const pg_sys::Const)
                        } else if let Some(ty) = rhs_type
                            && (*a0).type_ == pg_sys::NodeTag::T_Const
                        {
                            (ty, false, a0 as *const pg_sys::Const)
                        } else {
                            return; // neither (Var/chain, Const) nor (Const, Var/chain)
                        };

                        if (*const_node).constisnull {
                            return;
                        }

                        if is_like || is_not_like {
                            if !var_on_left {
                                return;
                            }
                            if !matches!(
                                type_oid,
                                pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID
                            ) {
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
                            let inner =
                                (*(*bargs).elements.add(0)).ptr_value as *const pg_sys::Node;
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
                        // col IN (...) / col = ANY(ARRAY[...]). Accepts plain
                        // Var or json_extract chain on the LHS; runtime
                        // (`extract_batch_quals` + per-segment text dispatch
                        // in `decompress.rs`) handles both numeric and text
                        // IN lists, so the planner gate matches.
                        let saop = qn as *const pg_sys::ScalarArrayOpExpr;
                        if !(*saop).useOr {
                            return; // ALL semantics not supported
                        }
                        let sa_args = (*saop).args;
                        if sa_args.is_null() || (*sa_args).length != 2 {
                            return;
                        }
                        let sa_arg0 =
                            (*(*sa_args).elements.add(0)).ptr_value as *const pg_sys::Node;
                        let sa_arg1 =
                            (*(*sa_args).elements.add(1)).ptr_value as *const pg_sys::Node;
                        if sa_arg0.is_null() || sa_arg1.is_null() {
                            return;
                        }
                        let sa_a0 = unwrap_relabel_node(sa_arg0);
                        if (*sa_arg1).type_ != pg_sys::NodeTag::T_Const {
                            return;
                        }
                        let sa_const = sa_arg1 as *const pg_sys::Const;
                        if (*sa_const).constisnull {
                            return;
                        }
                        if json_extract_ctx.is_none() {
                            json_extract_ctx =
                                Some(super::json_extract::AggChainCtx::from_root(root));
                        }
                        let sa_type_oid = if (*sa_a0).type_ == pg_sys::NodeTag::T_Var {
                            (*(sa_a0 as *const pg_sys::Var)).vartype
                        } else if let Some(Some(ctx)) = json_extract_ctx.as_ref()
                            && let Some((_idx, ty)) = ctx.match_to_synthetic(sa_a0)
                        {
                            ty
                        } else {
                            return;
                        };
                        if !matches!(
                            sa_type_oid,
                            pg_sys::INT2OID
                                | pg_sys::INT4OID
                                | pg_sys::INT8OID
                                | pg_sys::DATEOID
                                | pg_sys::TIMESTAMPOID
                                | pg_sys::TIMESTAMPTZOID
                                | pg_sys::TEXTOID
                                | pg_sys::VARCHAROID
                                | pg_sys::BPCHAROID
                        ) {
                            return;
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
                let tle =
                    pg_sys::get_sortgroupclause_tle(sc as *mut pg_sys::SortGroupClause, tlist);
                if tle.is_null() {
                    return;
                }
                let expr = (*tle).expr as *const pg_sys::Node;
                if expr.is_null() {
                    return;
                }

                // Try interpreting as a JSONB chain over a synthetic column
                // (json_extract). Must come before the OpExpr/FuncExpr branches
                // since chains share those node tags. We don't set
                // group_by_relid in this branch — the parent table doesn't
                // expose synthetic columns through pg_attribute, so the
                // ndistinct heuristic below would mis-resolve them. Falling
                // through to the pathlist's row estimate is fine for now;
                // synthetic-column ndistinct is a follow-up.
                if json_extract_ctx.is_none() {
                    json_extract_ctx = Some(super::json_extract::AggChainCtx::from_root(root));
                }
                if let Some(ctx) = json_extract_ctx.as_ref().unwrap()
                    && let Some((col_idx, type_oid)) = ctx.match_to_synthetic(expr)
                {
                    group_specs.push(super::exec::GroupByColSpec {
                        col_idx,
                        type_oid,
                        expr: GroupByExpr::Column,
                    });
                    continue;
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
                    pg_sys::get_atttypetypmodcoll(
                        relid,
                        (*var_node).varattno,
                        &mut type_oid,
                        &mut typmod,
                        &mut collation,
                    );

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
                    let fn_name = std::ffi::CStr::from_ptr(fn_name_ptr).to_str().unwrap_or("");

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

                        let pattern_cstr =
                            pg_sys::text_to_cstring((*pattern_const).constvalue.cast_mut_ptr());
                        let pattern = std::ffi::CStr::from_ptr(pattern_cstr)
                            .to_string_lossy()
                            .into_owned();
                        pg_sys::pfree(pattern_cstr as *mut _);

                        let replacement_cstr =
                            pg_sys::text_to_cstring((*replacement_const).constvalue.cast_mut_ptr());
                        let replacement = std::ffi::CStr::from_ptr(replacement_cstr)
                            .to_string_lossy()
                            .into_owned();
                        pg_sys::pfree(replacement_cstr as *mut _);

                        let func_oid = u32::from((*funcexpr).funcid);
                        let collation = u32::from((*funcexpr).inputcollid);

                        group_specs.push(super::exec::GroupByColSpec {
                            col_idx,
                            type_oid: pg_sys::TEXTOID,
                            expr: GroupByExpr::RegexpReplace {
                                pattern,
                                replacement,
                                func_oid,
                                collation,
                            },
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
                        pg_sys::get_atttypetypmodcoll(
                            (*rte).relid,
                            (*var_node).varattno,
                            &mut type_oid,
                            &mut typmod,
                            &mut collation,
                        );
                        if type_oid != pg_sys::TIMESTAMPOID && type_oid != pg_sys::TIMESTAMPTZOID {
                            return;
                        }

                        // Extract unit string from Const
                        let unit_const = arg0 as *const pg_sys::Const;
                        if (*unit_const).constisnull {
                            return;
                        }
                        let unit_cstr =
                            pg_sys::text_to_cstring((*unit_const).constvalue.cast_mut_ptr());
                        let unit = std::ffi::CStr::from_ptr(unit_cstr)
                            .to_string_lossy()
                            .into_owned();
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
                            expr: GroupByExpr::DateTrunc {
                                unit,
                                unit_usecs,
                                func_oid,
                            },
                        });
                    } else if fn_name == "extract" {
                        // Validate: extract(Const text, <inner>)
                        // <inner> is either:
                        //   (a) a plain Var of type timestamp/timestamptz, or
                        //   (b) `to_timestamp(<dividend> / <int_const>)` where
                        //       <dividend> is a Var or json_extract chain that
                        //       resolves to an INT8 synthetic — used by the
                        //       JSONBench Q2 shape:
                        //         extract(hour FROM
                        //           to_timestamp((data->>'time_us')::bigint / 1000000))
                        let fn_args = (*funcexpr).args;
                        if fn_args.is_null() || (*fn_args).length != 2 {
                            return;
                        }
                        let arg0 = (*(*fn_args).elements.add(0)).ptr_value as *const pg_sys::Node;
                        let arg1 = (*(*fn_args).elements.add(1)).ptr_value as *const pg_sys::Node;

                        if arg0.is_null() || (*arg0).type_ != pg_sys::NodeTag::T_Const {
                            return;
                        }
                        if arg1.is_null() {
                            return;
                        }

                        // Try shape (a): plain Var of timestamp/tz.
                        let mut col_idx_opt: Option<i32> = None;
                        let mut divisor: i64 = 0;
                        let mut record_relid: pg_sys::Oid = pg_sys::InvalidOid;

                        if (*arg1).type_ == pg_sys::NodeTag::T_Var {
                            let var_node = arg1 as *const pg_sys::Var;
                            let varno = (*var_node).varno as usize;
                            if varno != 0 && varno < (*root).simple_rel_array_size as usize {
                                let rte = *(*root).simple_rte_array.add(varno);
                                if !rte.is_null() {
                                    let relid = (*rte).relid;
                                    let mut type_oid = pg_sys::InvalidOid;
                                    let mut typmod: i32 = -1;
                                    let mut collation: pg_sys::Oid = pg_sys::InvalidOid;
                                    pg_sys::get_atttypetypmodcoll(
                                        relid,
                                        (*var_node).varattno,
                                        &mut type_oid,
                                        &mut typmod,
                                        &mut collation,
                                    );
                                    if type_oid == pg_sys::TIMESTAMPOID
                                        || type_oid == pg_sys::TIMESTAMPTZOID
                                    {
                                        col_idx_opt = Some((*var_node).varattno as i32 - 1);
                                        record_relid = relid;
                                    }
                                }
                            }
                        }

                        // Try shape (b): `to_timestamp(dividend / int_const)`.
                        if col_idx_opt.is_none() && (*arg1).type_ == pg_sys::NodeTag::T_FuncExpr {
                            let inner_fe = arg1 as *const pg_sys::FuncExpr;
                            let inner_name_ptr = pg_sys::get_func_name((*inner_fe).funcid);
                            if !inner_name_ptr.is_null()
                                && std::ffi::CStr::from_ptr(inner_name_ptr)
                                    .to_str()
                                    .map(|s| s == "to_timestamp")
                                    .unwrap_or(false)
                                && !(*inner_fe).args.is_null()
                                && (*(*inner_fe).args).length == 1
                            {
                                let mut inner_arg = (*(*(*inner_fe).args).elements.add(0)).ptr_value
                                    as *const pg_sys::Node;
                                // PG inserts an `int8 → double precision`
                                // cast around the division result so it
                                // matches `to_timestamp(double precision)`.
                                // The cast appears as a single-arg FuncExpr
                                // (e.g. `float8(int8)`, oid 482) or as a
                                // RelabelType. Peek through either to find
                                // the OpExpr underneath.
                                if !inner_arg.is_null()
                                    && (*inner_arg).type_ == pg_sys::NodeTag::T_FuncExpr
                                {
                                    let cast_fe = inner_arg as *const pg_sys::FuncExpr;
                                    if !(*cast_fe).args.is_null() && (*(*cast_fe).args).length == 1
                                    {
                                        inner_arg = (*(*(*cast_fe).args).elements.add(0)).ptr_value
                                            as *const pg_sys::Node;
                                    }
                                } else if !inner_arg.is_null()
                                    && (*inner_arg).type_ == pg_sys::NodeTag::T_RelabelType
                                {
                                    let rt = inner_arg as *const pg_sys::RelabelType;
                                    inner_arg = (*rt).arg as *const pg_sys::Node;
                                }
                                if !inner_arg.is_null()
                                    && (*inner_arg).type_ == pg_sys::NodeTag::T_OpExpr
                                {
                                    let op = inner_arg as *const pg_sys::OpExpr;
                                    let opname_ptr = pg_sys::get_opname((*op).opno);
                                    if !opname_ptr.is_null()
                                        && std::ffi::CStr::from_ptr(opname_ptr)
                                            .to_str()
                                            .map(|s| s == "/")
                                            .unwrap_or(false)
                                        && !(*op).args.is_null()
                                        && (*(*op).args).length == 2
                                    {
                                        let dividend = (*(*(*op).args).elements.add(0)).ptr_value
                                            as *const pg_sys::Node;
                                        let div_const = (*(*(*op).args).elements.add(1)).ptr_value
                                            as *const pg_sys::Node;

                                        // Divisor must be a positive int constant.
                                        if !div_const.is_null()
                                            && (*div_const).type_ == pg_sys::NodeTag::T_Const
                                        {
                                            let c = div_const as *const pg_sys::Const;
                                            if !(*c).constisnull {
                                                let v = match (*c).consttype {
                                                    pg_sys::INT2OID => {
                                                        (*c).constvalue.value() as i16 as i64
                                                    }
                                                    pg_sys::INT4OID => {
                                                        (*c).constvalue.value() as i32 as i64
                                                    }
                                                    pg_sys::INT8OID => {
                                                        (*c).constvalue.value() as i64
                                                    }
                                                    _ => 0,
                                                };
                                                if v > 0 {
                                                    divisor = v;
                                                }
                                            }
                                        }

                                        if divisor > 0 && !dividend.is_null() {
                                            // Dividend may be a plain Var (BIGINT) or
                                            // a JSONB chain over a synthetic.
                                            if (*dividend).type_ == pg_sys::NodeTag::T_Var {
                                                let dv = dividend as *const pg_sys::Var;
                                                let varno = (*dv).varno as usize;
                                                if varno != 0
                                                    && varno
                                                        < (*root).simple_rel_array_size as usize
                                                {
                                                    let rte = *(*root).simple_rte_array.add(varno);
                                                    if !rte.is_null() {
                                                        let relid = (*rte).relid;
                                                        let mut type_oid = pg_sys::InvalidOid;
                                                        let mut typmod: i32 = -1;
                                                        let mut collation: pg_sys::Oid =
                                                            pg_sys::InvalidOid;
                                                        pg_sys::get_atttypetypmodcoll(
                                                            relid,
                                                            (*dv).varattno,
                                                            &mut type_oid,
                                                            &mut typmod,
                                                            &mut collation,
                                                        );
                                                        if type_oid == pg_sys::INT8OID {
                                                            col_idx_opt =
                                                                Some((*dv).varattno as i32 - 1);
                                                            record_relid = relid;
                                                        }
                                                    }
                                                }
                                            } else {
                                                // Try JSONB chain match against synthetic.
                                                if json_extract_ctx.is_none() {
                                                    json_extract_ctx = Some(
                                                        super::json_extract::AggChainCtx::from_root(
                                                            root,
                                                        ),
                                                    );
                                                }
                                                if let Some(Some(ctx)) = json_extract_ctx.as_ref()
                                                    && let Some((ci, ti)) =
                                                        ctx.match_to_synthetic(dividend)
                                                    && ti == pg_sys::INT8OID
                                                {
                                                    col_idx_opt = Some(ci);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        let Some(col_idx) = col_idx_opt else {
                            return;
                        };
                        if record_relid != pg_sys::InvalidOid
                            && group_by_relid == pg_sys::InvalidOid
                        {
                            group_by_relid = record_relid;
                        }

                        // Extract unit string from Const
                        let unit_const = arg0 as *const pg_sys::Const;
                        if (*unit_const).constisnull {
                            return;
                        }
                        let unit_cstr =
                            pg_sys::text_to_cstring((*unit_const).constvalue.cast_mut_ptr());
                        let unit = std::ffi::CStr::from_ptr(unit_cstr)
                            .to_string_lossy()
                            .into_owned();
                        pg_sys::pfree(unit_cstr as *mut _);

                        // For the divisor>0 (bigint unix-µs) path, restrict to
                        // sub-day units that depend only on `unix_secs %
                        // 86400`. dow/epoch differ by a constant offset
                        // between unix and PG epochs and would need extra
                        // handling — defer.
                        let unit_ok = if divisor == 0 {
                            matches!(
                                unit.as_str(),
                                "microsecond"
                                    | "microseconds"
                                    | "millisecond"
                                    | "milliseconds"
                                    | "second"
                                    | "seconds"
                                    | "minute"
                                    | "minutes"
                                    | "hour"
                                    | "hours"
                                    | "dow"
                                    | "epoch"
                            )
                        } else {
                            matches!(
                                unit.as_str(),
                                "microsecond"
                                    | "microseconds"
                                    | "millisecond"
                                    | "milliseconds"
                                    | "second"
                                    | "seconds"
                                    | "minute"
                                    | "minutes"
                                    | "hour"
                                    | "hours"
                            )
                        };
                        if !unit_ok {
                            return;
                        }

                        let func_oid = u32::from((*funcexpr).funcid);

                        group_specs.push(super::exec::GroupByColSpec {
                            col_idx,
                            type_oid: pg_sys::NUMERICOID,
                            expr: GroupByExpr::Extract {
                                unit,
                                func_oid,
                                divisor,
                            },
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
                    let opname = std::ffi::CStr::from_ptr(opname_ptr).to_str().unwrap_or("");
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
                        (
                            left as *const pg_sys::Var,
                            right as *const pg_sys::Const,
                            is_minus,
                        )
                    } else if is_plus
                        && (*left).type_ == pg_sys::NodeTag::T_Const
                        && (*right).type_ == pg_sys::NodeTag::T_Var
                    {
                        (
                            right as *const pg_sys::Var,
                            left as *const pg_sys::Const,
                            false,
                        )
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
                    pg_sys::get_atttypetypmodcoll(
                        relid,
                        (*var_ptr).varattno,
                        &mut type_oid,
                        &mut typmod,
                        &mut collation,
                    );

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
                let matched = group_specs
                    .iter()
                    .any(|gs| matches!(gs.expr, GroupByExpr::CaseWhen(_)));
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
                    let arg =
                        (*(*fn_args).elements.add(ai as usize)).ptr_value as *const pg_sys::Node;
                    if !arg.is_null() && (*arg).type_ == pg_sys::NodeTag::T_Var {
                        let var_node = arg as *const pg_sys::Var;
                        col_idx = (*var_node).varattno as i32 - 1;
                        break;
                    }
                }
                // For the `extract(hour FROM to_timestamp(<chain>/<const>))`
                // shape there's no plain Var in `args` — recover the
                // synthetic col_idx by matching the chain that lives
                // inside to_timestamp's OpExpr divisor.
                if col_idx < 0 && nargs == 2 {
                    let arg1 = (*(*fn_args).elements.add(1)).ptr_value as *const pg_sys::Node;
                    if !arg1.is_null() && (*arg1).type_ == pg_sys::NodeTag::T_FuncExpr {
                        let inner_fe = arg1 as *const pg_sys::FuncExpr;
                        let inner_name_ptr = pg_sys::get_func_name((*inner_fe).funcid);
                        if !inner_name_ptr.is_null()
                            && std::ffi::CStr::from_ptr(inner_name_ptr)
                                .to_str()
                                .map(|s| s == "to_timestamp")
                                .unwrap_or(false)
                            && !(*inner_fe).args.is_null()
                            && (*(*inner_fe).args).length == 1
                        {
                            let mut inner_arg = (*(*(*inner_fe).args).elements.add(0)).ptr_value
                                as *const pg_sys::Node;
                            // Peek through the int8 → float8 cast.
                            if !inner_arg.is_null()
                                && (*inner_arg).type_ == pg_sys::NodeTag::T_FuncExpr
                            {
                                let cast_fe = inner_arg as *const pg_sys::FuncExpr;
                                if !(*cast_fe).args.is_null() && (*(*cast_fe).args).length == 1 {
                                    inner_arg = (*(*(*cast_fe).args).elements.add(0)).ptr_value
                                        as *const pg_sys::Node;
                                }
                            } else if !inner_arg.is_null()
                                && (*inner_arg).type_ == pg_sys::NodeTag::T_RelabelType
                            {
                                let rt = inner_arg as *const pg_sys::RelabelType;
                                inner_arg = (*rt).arg as *const pg_sys::Node;
                            }
                            if !inner_arg.is_null()
                                && (*inner_arg).type_ == pg_sys::NodeTag::T_OpExpr
                            {
                                let op = inner_arg as *const pg_sys::OpExpr;
                                if !(*op).args.is_null() && (*(*op).args).length == 2 {
                                    let dividend = (*(*(*op).args).elements.add(0)).ptr_value
                                        as *const pg_sys::Node;
                                    if !dividend.is_null() {
                                        if (*dividend).type_ == pg_sys::NodeTag::T_Var {
                                            let dv = dividend as *const pg_sys::Var;
                                            col_idx = (*dv).varattno as i32 - 1;
                                        } else if json_extract_ctx.is_none() {
                                            json_extract_ctx = Some(
                                                super::json_extract::AggChainCtx::from_root(root),
                                            );
                                        }
                                        if col_idx < 0
                                            && let Some(Some(ctx)) = json_extract_ctx.as_ref()
                                            && let Some((ci, _ti)) =
                                                ctx.match_to_synthetic(dividend)
                                        {
                                            col_idx = ci;
                                        }
                                    }
                                }
                            }
                        }
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

            // Validate that each non_agg_op_exprs entry matches a GROUP BY spec.
            // Three shapes are accepted:
            //   1. JSONB chain (`data->>'k'`) — matches a synthetic-column
            //      GroupByExpr::Column whose col_idx equals the chain's
            //      synthetic position.
            //   2. `Var +/- Const` — matches a GroupByExpr::AddConst with the
            //      same col_idx and operator OID.
            //   3. **Agg-only tree** (H.2): expression composed solely of
            //      Aggref / Const / nested OpExpr / FuncExpr / RelabelType
            //      / CoerceViaIO. PG computes these post-aggregation, so they
            //      don't need a GROUP BY match — Q4's
            //      `EXTRACT(EPOCH FROM (MAX-MIN)) * 1000` lives here.
            for &(_tlist_idx, opexpr) in &non_agg_op_exprs {
                // Agg-only tree: accept without further matching. The MIN/MAX
                // Aggrefs were already classified into `classified_aggs` above.
                if expr_only_uses_aggrefs_and_consts(opexpr as *const pg_sys::Node) {
                    continue;
                }
                // Try chain match next.
                if json_extract_ctx.is_none() {
                    json_extract_ctx = Some(super::json_extract::AggChainCtx::from_root(root));
                }
                if let Some(ctx) = json_extract_ctx.as_ref().unwrap()
                    && let Some((chain_col_idx, _type_oid)) =
                        ctx.match_to_synthetic(opexpr as *const pg_sys::Node)
                {
                    let matched = group_specs.iter().any(|gs| {
                        matches!(gs.expr, GroupByExpr::Column) && gs.col_idx == chain_col_idx
                    });
                    if !matched {
                        return; // chain Expr in target doesn't match any GROUP BY synthetic
                    }
                    continue;
                }

                let op_oid = u32::from((*opexpr).opno);
                let op_args = (*opexpr).args;
                if op_args.is_null() || (*op_args).length != 2 {
                    return;
                }
                // Find the Var in the OpExpr args
                let mut col_idx = -1_i32;
                for ai in 0..(*op_args).length {
                    let arg =
                        (*(*op_args).elements.add(ai as usize)).ptr_value as *const pg_sys::Node;
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
                    if let GroupByExpr::AddConst {
                        op_oid: spec_op_oid,
                        ..
                    } = &gs.expr
                    {
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
            use super::exec::{HavingFilter, HavingOp};
            let having_node = (*parse).havingQual as *const pg_sys::Node;
            // Collect qual nodes — PG may store as a single OpExpr, a BoolExpr
            // AND-list, or a plain T_List of conditions.
            let qual_nodes: Vec<*const pg_sys::Node> =
                if (*having_node).type_ == pg_sys::NodeTag::T_List {
                    let list = having_node as *const pg_sys::List;
                    (0..(*list).length)
                        .map(|i| pg_sys::list_nth(list as *mut _, i) as *const pg_sys::Node)
                        .collect()
                } else if (*having_node).type_ == pg_sys::NodeTag::T_BoolExpr {
                    let boolexpr = having_node as *const pg_sys::BoolExpr;
                    if (*boolexpr).boolop == pg_sys::BoolExprType::AND_EXPR {
                        let args = (*boolexpr).args;
                        let n = (*args).length;
                        (0..n)
                            .map(|i| pg_sys::list_nth(args, i) as *const pg_sys::Node)
                            .collect()
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
                let opname = std::ffi::CStr::from_ptr(opname_ptr).to_str().unwrap_or("");
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
                let (aggref_node, const_node, agg_on_left) = if (*a0).type_
                    == pg_sys::NodeTag::T_Aggref
                    && (*a1).type_ == pg_sys::NodeTag::T_Const
                {
                    (
                        a0 as *const pg_sys::Aggref,
                        a1 as *const pg_sys::Const,
                        true,
                    )
                } else if (*a0).type_ == pg_sys::NodeTag::T_Const
                    && (*a1).type_ == pg_sys::NodeTag::T_Aggref
                {
                    (
                        a1 as *const pg_sys::Aggref,
                        a0 as *const pg_sys::Const,
                        false,
                    )
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
                            if !ar_args.is_null()
                                && !h_args.is_null()
                                && (*ar_args).length == 1
                                && (*h_args).length == 1
                            {
                                let ar_te =
                                    pg_sys::list_nth(ar_args, 0) as *const pg_sys::TargetEntry;
                                let h_te =
                                    pg_sys::list_nth(h_args, 0) as *const pg_sys::TargetEntry;
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
            let total_uncompressed_rows: f64 = companion_oids
                .iter()
                .map(|&oid| {
                    let (_, _, rows) = cost::estimate_cost(oid, 0);
                    rows
                })
                .sum();

            if total_uncompressed_rows > 0.0 {
                // Merge per-partition ndistinct across partitions. Summing
                // assumes disjoint key sets, but in time-series data the
                // same entity keys (device_id, user_id, phrase, …) recur
                // across every time partition — summing then inflates
                // ndistinct and misfires the high-cardinality bail
                // downstream. Take the max-across-partitions as a
                // lower-bound-but-closer estimate, then cap by total row
                // count as a hard upper bound.
                let row_cap = total_uncompressed_rows as i64;
                let mut merged_ndistinct: std::collections::HashMap<String, i64> =
                    std::collections::HashMap::new();
                for &oid in &companion_oids {
                    let nd = cost::get_column_ndistinct(oid);
                    for (col, count) in nd {
                        let entry = merged_ndistinct.entry(col).or_insert(0);
                        *entry = (*entry).max(count).min(row_cap);
                    }
                }

                // Resolve a column name for each group spec, distinguishing
                // physical attrs (look up pg_attribute) from json_extract
                // synthetics (look up the spec's target_name). A mixed GROUP
                // BY of physical + synthetic columns (e.g. EXTRACT(HOUR FROM
                // ts) + data->'commit'->>'collection') sets group_by_relid
                // to the parent rel; without this distinction the synthetic
                // col_idx is fed to get_attname against the parent rel and
                // crashes with "cache lookup failed for attribute N of
                // relation X" because the parent has no such physical attno.
                //
                // Try the cheap pg_attribute lookup first (missing_ok=true so
                // a synthetic col_idx returns NULL instead of erroring); only
                // populate the json_extract context — which costs an SPI
                // lookup against `deltax_extract_specs` — when we actually
                // encounter an attno that pg_attribute doesn't have. Queries
                // over tables without json_extract configured (e.g. ClickBench)
                // skip the SPI lookup entirely.
                let group_col_names: Vec<Option<String>> = group_specs
                    .iter()
                    .map(|gs| {
                        let attno = (gs.col_idx + 1) as i16;
                        let name_ptr = pg_sys::get_attname(group_by_relid, attno, true);
                        if !name_ptr.is_null() {
                            return Some(
                                std::ffi::CStr::from_ptr(name_ptr)
                                    .to_string_lossy()
                                    .into_owned(),
                            );
                        }
                        // attno > physical natts → must be a json_extract
                        // synthetic. Lazy-populate the ctx and look it up.
                        if json_extract_ctx.is_none() {
                            json_extract_ctx =
                                Some(super::json_extract::AggChainCtx::from_root(root));
                        }
                        let ctx = json_extract_ctx.as_ref().and_then(|o| o.as_ref())?;
                        let spec_idx = (gs.col_idx - ctx.physical_count as i32) as usize;
                        ctx.specs.get(spec_idx).map(|s| s.target_name.clone())
                    })
                    .collect();

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
                    let few_rows = estimated_rows < total_uncompressed_rows * 0.05;
                    let has_high_card_text = group_specs.iter().enumerate().any(|(i, gs)| {
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
                        let Some(col_name) = group_col_names[i].as_deref() else {
                            return false;
                        };
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
                        let has_high_cardinality = group_specs.iter().enumerate().any(|(i, gs)| {
                            if !matches!(gs.expr, GroupByExpr::Column) {
                                return false;
                            }
                            let Some(col_name) = group_col_names[i].as_deref() else {
                                return false;
                            };
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
                        for (i, gs) in group_specs.iter().enumerate() {
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
                                GroupByExpr::DateTrunc { .. }
                                | GroupByExpr::RegexpReplace { .. }
                                | GroupByExpr::CaseWhen(_) => {
                                    // Can't easily estimate, skip ndistinct estimate
                                    all_found = false;
                                    break;
                                }
                                GroupByExpr::Column | GroupByExpr::AddConst { .. } => {
                                    let Some(col_name) = group_col_names[i].as_deref() else {
                                        all_found = false;
                                        break;
                                    };
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
                    let sc = pg_sys::list_nth(sort_clause, 0) as *const pg_sys::SortGroupClause;
                    if !sc.is_null() {
                        // Find target entry for this sort key
                        let tle_ref = (*sc).tleSortGroupRef;
                        let mut sort_tle: *const pg_sys::TargetEntry = std::ptr::null();
                        for i in 0..nentries {
                            let te = pg_sys::list_nth(tlist, i) as *const pg_sys::TargetEntry;
                            if !te.is_null() && (*te).ressortgroupref == tle_ref {
                                sort_tle = te;
                                break;
                            }
                        }
                        if !sort_tle.is_null() {
                            let sort_expr = (*sort_tle).expr as *const pg_sys::Node;
                            if !sort_expr.is_null()
                                && (*sort_expr).type_ == pg_sys::NodeTag::T_Aggref
                            {
                                let sort_aggref = sort_expr as *const pg_sys::Aggref;
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
                                    // Only for types where compact storage supports sorting
                                    let is_i64 = match spec.agg_type {
                                        AggType::CountStar
                                        | AggType::Count
                                        | AggType::CountDistinct => true,
                                        AggType::Sum => matches!(
                                            spec.col_type_oid,
                                            pg_sys::INT2OID | pg_sys::INT4OID
                                        ),
                                        AggType::Avg => matches!(
                                            spec.col_type_oid,
                                            pg_sys::INT2OID
                                                | pg_sys::INT4OID
                                                | pg_sys::INT8OID
                                                | pg_sys::FLOAT4OID
                                                | pg_sys::FLOAT8OID
                                        ),
                                        AggType::Min | AggType::Max => matches!(
                                            spec.col_type_oid,
                                            pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID
                                        ),
                                    };
                                    if is_i64 {
                                        // Determine sort direction
                                        let opname_ptr = pg_sys::get_opname((*sc).sortop);
                                        if !opname_ptr.is_null() {
                                            let opname = std::ffi::CStr::from_ptr(opname_ptr)
                                                .to_str()
                                                .unwrap_or("");
                                            topn_ascending = opname == "<";
                                            // Find output column index
                                            // (position among non-resjunk tlist entries)
                                            let resno = (*sort_tle).resno;
                                            let mut non_junk = 0i32;
                                            for j in 0..nentries {
                                                let te2 = pg_sys::list_nth(tlist, j)
                                                    as *const pg_sys::TargetEntry;
                                                if te2.is_null() || (*te2).resjunk {
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
                    // Direct-aggregate sort didn't match. Try the derived
                    // MIN/MAX-difference shape — `ORDER BY <wrappers>(MAX(x)
                    // - MIN(x)) [ASC|DESC]` (JSONBench Q4). Both Aggrefs must
                    //   already be in `aggrefs`; the recognizer matches by
                    //   pointer identity.
                    let sort_clause = (*parse).sortClause;
                    let mut derived_matched = false;
                    if !sort_clause.is_null() && (*sort_clause).length == 1 {
                        let sc = pg_sys::list_nth(sort_clause, 0) as *const pg_sys::SortGroupClause;
                        if !sc.is_null() {
                            let tle_ref = (*sc).tleSortGroupRef;
                            let mut sort_tle: *const pg_sys::TargetEntry = std::ptr::null();
                            for i in 0..nentries {
                                let te = pg_sys::list_nth(tlist, i) as *const pg_sys::TargetEntry;
                                if !te.is_null() && (*te).ressortgroupref == tle_ref {
                                    sort_tle = te;
                                    break;
                                }
                            }
                            if !sort_tle.is_null() {
                                let sort_expr = (*sort_tle).expr as *const pg_sys::Node;
                                if let Some((max_idx, min_idx)) =
                                    try_match_derived_minmax_topn(sort_expr, &aggrefs)
                                {
                                    // Check storage compatibility: both
                                    // aggregates must be on i64-storage
                                    // (MinInt/MaxInt) — the only kind whose
                                    // values we can subtract directly.
                                    let max_spec = &classified_aggs[max_idx];
                                    let min_spec = &classified_aggs[min_idx];
                                    let storage_ok = matches!(max_spec.agg_type, AggType::Max)
                                        && matches!(min_spec.agg_type, AggType::Min)
                                        && matches!(
                                            max_spec.col_type_oid,
                                            pg_sys::INT2OID
                                                | pg_sys::INT4OID
                                                | pg_sys::INT8OID
                                                | pg_sys::DATEOID
                                                | pg_sys::TIMESTAMPOID
                                                | pg_sys::TIMESTAMPTZOID
                                        )
                                        && max_spec.col_type_oid == min_spec.col_type_oid
                                        && max_spec.col_idx == min_spec.col_idx;
                                    if storage_ok {
                                        let opname_ptr = pg_sys::get_opname((*sc).sortop);
                                        if !opname_ptr.is_null() {
                                            let opname = std::ffi::CStr::from_ptr(opname_ptr)
                                                .to_str()
                                                .unwrap_or("");
                                            topn_ascending = opname == "<";
                                            path::set_agg_topn_info_derived_minmax(
                                                topn_limit,
                                                topn_ascending,
                                                max_idx as i32,
                                                min_idx as i32,
                                            );
                                            topn_active = true;
                                            derived_matched = true;
                                            // Skip the "no ORDER BY on aggregate"
                                            // disable branch below.
                                            topn_sort_col = path::TOPN_SORT_COL_DERIVED;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if !derived_matched {
                        // No ORDER BY on aggregate found
                        if sort_clause.is_null() || (*sort_clause).length == 0 {
                            // Bare LIMIT N — pass as bare_limit (sort_col = -1)
                            path::set_agg_topn_info(topn_limit, -1, true);
                            // topn_active stays false — no pathkeys claimed
                        } else {
                            topn_limit = 0; // ORDER BY exists but doesn't match an aggregate — disable
                        }
                    }
                }
            }

            if topn_limit > 0 && topn_sort_col >= 0 && topn_sort_col != path::TOPN_SORT_COL_DERIVED
            {
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

        // Phase C.2 activation — add a partial-mode CustomPath through PG's
        // Gather + Final Aggregate model. add_agg_partial_path self-gates
        // (eligibility predicate inside) and silently no-ops when not
        // viable. Both paths compete on cost; the planner picks whichever
        // is cheaper. add_agg_path's complete variant always lands first
        // so correctness is never at risk if the partial variant is
        // rejected for any reason.
        if !topn_active && having_filters.is_empty() {
            path::add_agg_partial_path(
                root,
                output_rel,
                &companion_oids,
                &classified_aggs,
                &group_specs,
                pg_estimated_groups,
                extra as *mut pg_sys::GroupPathExtraData,
            );
        }
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
        // Unwrap wrapper paths the planner might place above our scan:
        // - ProjectionPath: wraps input when the GROUP BY target list contains
        //   expressions (e.g. regexp_replace) that need evaluation.
        // - GatherPath / GatherMergePath: appears once DeltaXAppend is
        //   parallel-safe and PG places a Gather above it. Without this
        //   unwrap, the upper aggregate-pushdown hook would fail to
        //   recognise the scan as compressed and skip DeltaXAgg, which
        //   can cause 5–10× regressions on aggregation queries where
        //   DeltaXAgg's pushdown is vastly cheaper than sort/hash-agg.
        let mut path = path;
        loop {
            if path.is_null() {
                return None;
            }
            match (*path).type_ {
                pg_sys::NodeTag::T_ProjectionPath => {
                    path =
                        (*(path as *const pg_sys::ProjectionPath)).subpath as *const pg_sys::Path;
                }
                pg_sys::NodeTag::T_GatherPath => {
                    path = (*(path as *const pg_sys::GatherPath)).subpath as *const pg_sys::Path;
                }
                pg_sys::NodeTag::T_GatherMergePath => {
                    path =
                        (*(path as *const pg_sys::GatherMergePath)).subpath as *const pg_sys::Path;
                }
                _ => break,
            }
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
            if oids.is_empty() { None } else { Some(oids) }
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
        let companion_oid = cached_companion_for_rel(child_oid);
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
unsafe fn subpath_has_data(root: *mut pg_sys::PlannerInfo, subpath: *const pg_sys::Path) -> bool {
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
        let nblocks =
            pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM);
        pg_sys::table_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        nblocks > 0
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
            let companion_oid = cached_companion_for_rel(child_oid);

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

                let companion_oid = cached_companion_for_rel(relid);

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

/// Planner hook entry point. Wraps `standard_planner` (or the previous hook
/// in the chain) and post-processes the resulting `PlannedStmt` to substitute
/// JSONB-extract chains in upper plans with `Var(OUTER_VAR, attno)` referring
/// to a `DeltaXDecompress`'s pre-computed synthetic columns.
///
/// PG's `set_plan_references` (which runs inside `standard_planner`) cannot
/// match the upper plan's chain `Expr` against our scan's tlist because
/// `set_customscan_references` already rewrote the scan's tlist to
/// `Var(INDEX_VAR, …)` by then — `tlist_member` (equal()) doesn't match
/// `Expr` to `Var`. So we do the matching ourselves on the final plan tree.
#[pg_guard]
pub unsafe extern "C-unwind" fn deltax_planner(
    parse: *mut pg_sys::Query,
    query_string: *const std::ffi::c_char,
    cursor_options: c_int,
    bound_params: pg_sys::ParamListInfo,
) -> *mut pg_sys::PlannedStmt {
    unsafe {
        // Chain to previous hook (if installed) or fall back to standard_planner.
        let prev = PREV_PLANNER_HOOK.load(Ordering::SeqCst);
        let pstmt: *mut pg_sys::PlannedStmt = if !prev.is_null() {
            let prev_fn: unsafe extern "C-unwind" fn(
                *mut pg_sys::Query,
                *const std::ffi::c_char,
                c_int,
                pg_sys::ParamListInfo,
            ) -> *mut pg_sys::PlannedStmt = std::mem::transmute(prev);
            prev_fn(parse, query_string, cursor_options, bound_params)
        } else {
            pg_sys::standard_planner(parse, query_string, cursor_options, bound_params)
        };

        if !pstmt.is_null() && !(*pstmt).planTree.is_null() {
            // Walk the final plan tree and rewrite chain Exprs in upper plans
            // to point at the synthetic columns produced by DeltaXDecompress.
            // The walker is a no-op when no DeltaXDecompress with json_extract
            // is found in the tree.
            super::json_extract::rewrite_plan_tree((*pstmt).planTree, (*pstmt).rtable);
        }

        pstmt
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;

    #[test]
    fn time_bounds_default_is_unbounded() {
        let b = TimeBounds::default();
        assert_eq!(b.lo, None);
        assert_eq!(b.hi, None);
        assert!(!b.any());
    }

    #[test]
    fn time_bounds_narrow_lo_keeps_max() {
        let mut b = TimeBounds::default();
        b.narrow_lo(100);
        assert_eq!(b.lo, Some(100));
        // narrower (higher) lo wins
        b.narrow_lo(200);
        assert_eq!(b.lo, Some(200));
        // wider (lower) lo is ignored
        b.narrow_lo(50);
        assert_eq!(b.lo, Some(200));
        assert!(b.any());
    }

    #[test]
    fn time_bounds_narrow_hi_keeps_min() {
        let mut b = TimeBounds::default();
        b.narrow_hi(1000);
        assert_eq!(b.hi, Some(1000));
        // narrower (lower) hi wins
        b.narrow_hi(500);
        assert_eq!(b.hi, Some(500));
        // wider (higher) hi is ignored
        b.narrow_hi(800);
        assert_eq!(b.hi, Some(500));
        assert!(b.any());
    }

    #[test]
    fn time_bounds_combined_any() {
        let mut b = TimeBounds::default();
        assert!(!b.any());
        b.narrow_lo(0);
        assert!(b.any());
        b.narrow_hi(100);
        assert!(b.any());
        assert_eq!(b.lo, Some(0));
        assert_eq!(b.hi, Some(100));
    }

    #[test]
    fn is_minmax_meta_type_accepts_integer_float_date_timestamp() {
        for oid in [
            pg_sys::INT2OID,
            pg_sys::INT4OID,
            pg_sys::INT8OID,
            pg_sys::FLOAT4OID,
            pg_sys::FLOAT8OID,
            pg_sys::DATEOID,
            pg_sys::TIMESTAMPOID,
            pg_sys::TIMESTAMPTZOID,
        ] {
            assert!(
                is_minmax_meta_type(oid),
                "expected oid {:?} to be meta-min/max-able",
                oid
            );
        }
    }

    #[test]
    fn is_minmax_meta_type_rejects_text_bool_jsonb_numeric() {
        // TEXT/VARCHAR/BPCHAR/JSONB/BOOL aren't encoded as order-preserving i64 in colstats.
        for oid in [
            pg_sys::TEXTOID,
            pg_sys::VARCHAROID,
            pg_sys::BPCHAROID,
            pg_sys::JSONBOID,
            pg_sys::BOOLOID,
            pg_sys::BYTEAOID,
            pg_sys::NUMERICOID,
        ] {
            assert!(
                !is_minmax_meta_type(oid),
                "expected oid {:?} to NOT be meta-min/max-able",
                oid
            );
        }
    }
}
