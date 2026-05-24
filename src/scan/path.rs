use pgrx::pg_guard;
use pgrx::pg_sys;

use super::SyncStatic;
use super::cost;

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

/// Top-N info for DeltaXAgg.
///
/// `sort_col`: index in the post-agg output tlist of a direct-aggregate sort
/// key, or `-3` when the sort key is a derived expression over two
/// aggregates (see `derived_minmax`).
///
/// `derived_minmax`: when `Some((max_agg_idx, min_agg_idx))`, the sort key is
/// `storage[max] - storage[min]` (both i64-storage). This covers the
/// JSONBench Q4 shape `ORDER BY EXTRACT(EPOCH FROM (MAX(t) - MIN(t))) * N`
/// — monotonic in `max - min` regardless of the scaling/extract wrappers.
#[derive(Copy, Clone)]
pub(super) struct AggTopnInfo {
    pub limit: i64,
    pub sort_col: i32,
    pub ascending: bool,
    pub derived_minmax: Option<(i32, i32)>,
}

/// Sentinel for `sort_col` when the sort key is a derived MIN/MAX-difference
/// expression instead of a single aggregate's output column.
pub(super) const TOPN_SORT_COL_DERIVED: i32 = -3;

thread_local! {
    /// Top-N info for DeltaXAgg.
    /// Set in hook (deltax_create_upper_paths), consumed in plan_agg_path.
    static AGG_TOPN_INFO: std::cell::RefCell<Option<AggTopnInfo>> =
        const { std::cell::RefCell::new(None) };
}

/// Store top-N info for the next DeltaXAgg plan. Direct-aggregate form.
pub(super) fn set_agg_topn_info(limit: i64, sort_col: i32, ascending: bool) {
    AGG_TOPN_INFO.with(|cell| {
        *cell.borrow_mut() = Some(AggTopnInfo {
            limit,
            sort_col,
            ascending,
            derived_minmax: None,
        });
    });
}

/// Store top-N info with a derived MIN/MAX-difference sort key (Q4 shape).
pub(super) fn set_agg_topn_info_derived_minmax(
    limit: i64,
    ascending: bool,
    max_agg_idx: i32,
    min_agg_idx: i32,
) {
    AGG_TOPN_INFO.with(|cell| {
        *cell.borrow_mut() = Some(AggTopnInfo {
            limit,
            sort_col: TOPN_SORT_COL_DERIVED,
            ascending,
            derived_minmax: Some((max_agg_idx, min_agg_idx)),
        });
    });
}

/// Clear any stale top-N info (e.g. from a previous query whose DeltaXAgg path was not chosen).
pub(super) fn clear_agg_topn_info() {
    AGG_TOPN_INFO.with(|cell| *cell.borrow_mut() = None);
}

/// Take (consume) the stored top-N info.
fn take_agg_topn_info() -> Option<AggTopnInfo> {
    AGG_TOPN_INFO.with(|cell| cell.borrow_mut().take())
}

/// Peek at the stored top-N info without consuming. Used by `add_agg_path`'s
/// parallel-eligibility check (Phase C.2.f) — Top-N pushdown is excluded
/// from the parallel path because workers can't prune top-N locally.
fn peek_agg_topn_info() -> Option<AggTopnInfo> {
    AGG_TOPN_INFO.with(|cell| *cell.borrow())
}

// ============================================================================
// Serialization helpers
// ============================================================================

/// Append a `qual_list` (PG node tree) to `private_list` as length-prefixed
/// bytes from `nodeToString`. Writes `0` when `qual_list` is null. Used by
/// every `add_*_path` that serialises plan quals into `custom_private`.
unsafe fn append_qual_list_as_bytes(
    private_list: *mut pg_sys::List,
    qual_list: *mut pg_sys::List,
) -> *mut pg_sys::List {
    unsafe {
        if qual_list.is_null() {
            return pg_sys::lappend_int(private_list, 0);
        }
        let s = pg_sys::nodeToString(qual_list as *const _);
        let s_bytes = std::ffi::CStr::from_ptr(s).to_bytes();
        let mut list = pg_sys::lappend_int(private_list, s_bytes.len() as i32);
        for &b in s_bytes {
            list = pg_sys::lappend_int(list, b as i32);
        }
        pg_sys::pfree(s as *mut _);
        list
    }
}

/// Append a slice of OIDs to `private_list` as raw ints. Mirrors the
/// "for &oid in companion_oids { lappend_int(list, oid as i32) }" pattern
/// repeated by every path/plan callback's wire-format builder.
unsafe fn append_oids_as_ints(
    private_list: *mut pg_sys::List,
    oids: &[pg_sys::Oid],
) -> *mut pg_sys::List {
    unsafe {
        let mut list = private_list;
        for &oid in oids {
            list = pg_sys::lappend_int(list, u32::from(oid) as i32);
        }
        list
    }
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
// Stored as (effective_limit, sort_ascending, multi_col_sort, sort_col_attno, nulls_first).
// sort_col_attno is 1-based PG attribute number of the ORDER BY column.
thread_local! {
    static TOPN_INFO: std::cell::Cell<(i64, bool, bool, i32, bool)> = const { std::cell::Cell::new((0, true, false, 0, false)) };
}

/// Register every CustomScanMethods struct with PG's name-keyed registry.
/// Required for parallel workers: when a worker deserializes the plan tree
/// from DSM, it looks up the methods struct by name (not by pointer, since
/// the leader's pointer would be invalid in the worker process).
///
/// # Safety
/// Must be called exactly once from `_PG_init()` before any parallel query
/// can reach a custom scan node.
pub(super) unsafe fn register_custom_scan_methods() {
    unsafe {
        pg_sys::RegisterCustomScanMethods(&CUSTOM_SCAN_METHODS.0);
        pg_sys::RegisterCustomScanMethods(&DELTAX_APPEND_SCAN_METHODS.0);
        pg_sys::RegisterCustomScanMethods(&DELTAX_COUNT_SCAN_METHODS.0);
        pg_sys::RegisterCustomScanMethods(&DELTAX_MINMAX_SCAN_METHODS.0);
        pg_sys::RegisterCustomScanMethods(&DELTAX_AGG_SCAN_METHODS.0);
    }
}

/// Add a DeltaXDecompress custom path to the relation's pathlist.
#[allow(clippy::too_many_arguments)]
pub unsafe fn add_decompress_path(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    companion_oid: pg_sys::Oid,
    pathkeys: *mut pg_sys::List,
    effective_limit: i64,
    sort_ascending: bool,
    multi_col_sort: bool,
    sort_col_attno: i32,
    topn_nulls_first: bool,
) {
    unsafe {
        let cpath =
            pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;

        (*cpath).path.type_ = pg_sys::NodeTag::T_CustomPath;
        (*cpath).path.pathtype = pg_sys::NodeTag::T_CustomScan;
        (*cpath).path.parent = rel;
        (*cpath).path.pathtarget = (*rel).reltarget;

        let (startup_cost, total_cost, rows) = cost::estimate_cost(companion_oid, 0);
        // Prefer PG's filter-aware `rel->rows` estimate when it's
        // meaningful — we now write accurate `pg_class.reltuples` and
        // `pg_statistic` at compress time (see `src/stats.rs`), so the
        // planner's restrictinfo-aware estimate is trustworthy. Fall
        // back to the raw companion row count only if PG hasn't
        // computed a real value yet (rel->rows = 1 is PG's default
        // when reltuples is 0/-1 and no stats are populated).
        let path_rows = if (*rel).rows > 1.0 {
            (*rel).rows.min(rows)
        } else {
            rows
        };
        (*cpath).path.rows = path_rows;
        (*cpath).path.startup_cost = startup_cost;
        (*cpath).path.total_cost = total_cost;
        (*cpath).path.parallel_workers = 0;
        (*cpath).path.parallel_aware = false;
        (*cpath).path.parallel_safe = false;
        (*cpath).path.pathkeys = pathkeys;

        // Store companion OID in custom_private using lappend_oid
        (*cpath).custom_private = pg_sys::lappend_oid(std::ptr::null_mut(), companion_oid);

        (*cpath).custom_paths = std::ptr::null_mut();
        (*cpath).custom_restrictinfo = std::ptr::null_mut();
        (*cpath).methods = &CUSTOM_PATH_METHODS.0;

        // Store Top-N info for plan_custom_path.
        if effective_limit > 0 {
            TOPN_INFO.with(|cell| {
                cell.set((
                    effective_limit,
                    sort_ascending,
                    multi_col_sort,
                    sort_col_attno,
                    topn_nulls_first,
                ))
            });
        } else {
            TOPN_INFO.with(|cell| cell.set((0, true, false, 0, false)));
        }

        // Clear existing paths — the partition is truncated so any SeqScan
        // would return 0 rows.  We must replace it with the decompression path.
        (*rel).pathlist = std::ptr::null_mut();
        (*rel).partial_pathlist = std::ptr::null_mut();

        pg_sys::add_path(rel, cpath as *mut pg_sys::Path);
    }
}

/// Resolve a planner range-table index to the actual relation OID via
/// `simple_rte_array`. Returns `InvalidOid` on out-of-range / null entry.
unsafe fn rti_to_rel_oid(root: *mut pg_sys::PlannerInfo, rti: pg_sys::Index) -> pg_sys::Oid {
    unsafe {
        if root.is_null() {
            return pg_sys::InvalidOid;
        }
        let array_size = (*root).simple_rel_array_size;
        if rti as i32 == 0 || (rti as i32) >= array_size {
            return pg_sys::InvalidOid;
        }
        let rte = *(*root).simple_rte_array.add(rti as usize);
        if rte.is_null() {
            return pg_sys::InvalidOid;
        }
        (*rte).relid
    }
}

/// SPI-look up the json_extract config for the deltatable that owns this
/// relation. `rel_oid` may be either the partitioned parent (matched against
/// `deltax.deltax_deltatable`) or a partition (matched via `deltax.deltax_partition`).
/// Returns an empty Vec on any failure (no deltatable, no json_extract,
/// parse error) — extraction is silently skipped rather than aborting
/// query planning.
/// Public wrapper around `load_extract_specs_for_rel` for use by the
/// planner_hook walker (which lives in `src/scan/json_extract.rs`).
pub(crate) unsafe fn load_extract_specs_for_rel_pub(
    rel_oid: pg_sys::Oid,
) -> Vec<crate::compress::ExtractSpec> {
    unsafe { load_extract_specs_for_rel(rel_oid) }
}

/// Mixed-partition gate: returns true iff every relevant compressed partition
/// has `compressed_at >= json_extract_added_at`. Without this, partitions
/// compressed before json_extract was configured don't have synthetic
/// columns in their companion blobs, and the rewrite would silently emit
/// NULLs at synthetic positions for those partitions.
///
/// For a parent-baserel rel_oid, "relevant" is every compressed partition
/// of that deltatable. For a partition rel_oid, it's just that partition.
/// Returns true (safe) when there are no compressed partitions yet, when
/// `json_extract_added_at` is NULL (no json_extract configured), or when
/// every relevant partition is up to date. Defensive default on any SPI
/// hiccup is `true` — a stale partition would surface as a wrong result;
/// a wrong default of `false` only loses the perf win, never correctness.
/// We pick `true` here because the upstream caller (`load_extract_specs`)
/// has already returned a non-empty spec list, so the deltatable definitely
/// has json_extract configured; the only question is partition freshness,
/// and treating unknowns as fresh is the less-surprising choice.
pub(crate) unsafe fn is_json_extract_safe_for_rel(rel_oid: pg_sys::Oid) -> bool {
    if rel_oid == pg_sys::InvalidOid {
        return true;
    }
    pgrx::Spi::connect(|client| -> bool {
        // Find this rel's deltatable + classify whether rel_oid is the
        // parent or a partition. Then compute the relevant `compressed_at`
        // bound: MIN(compressed_at) across compressed partitions for parent,
        // or this partition's own compressed_at for partition. Compare to
        // json_extract_added_at.
        let row = client
            .select(
                "WITH ident AS (
                     SELECT n.nspname AS s, c.relname AS t
                     FROM pg_class c
                     JOIN pg_namespace n ON c.relnamespace = n.oid
                     WHERE c.oid = $1
                 ),
                 dt AS (
                     SELECT h.id, h.json_extract_added_at, 'parent'::text AS kind
                     FROM deltax.deltax_deltatable h, ident i
                     WHERE h.schema_name = i.s AND h.table_name = i.t
                     UNION ALL
                     SELECT h.id, h.json_extract_added_at, 'partition'::text AS kind
                     FROM deltax.deltax_deltatable h
                     JOIN deltax.deltax_partition p ON p.deltatable_id = h.id
                     JOIN ident i ON p.schema_name = i.s AND p.table_name = i.t
                     LIMIT 1
                 )
                 SELECT
                     dt.json_extract_added_at,
                     CASE
                         WHEN dt.kind = 'parent' THEN
                             (SELECT min(p.compressed_at)
                                FROM deltax.deltax_partition p
                               WHERE p.deltatable_id = dt.id
                                 AND p.is_compressed)
                         ELSE
                             (SELECT p.compressed_at
                                FROM deltax.deltax_partition p, ident i
                               WHERE p.schema_name = i.s
                                 AND p.table_name = i.t)
                     END AS oldest_compressed
                 FROM dt",
                None,
                &[rel_oid.into()],
            )
            .ok();
        let Some(row) = row else { return true };
        let row = row.first();
        let added_at: Option<pgrx::datum::TimestampWithTimeZone> = row.get(1).ok().flatten();
        let oldest: Option<pgrx::datum::TimestampWithTimeZone> = row.get(2).ok().flatten();
        match (added_at, oldest) {
            // No json_extract configured (shouldn't happen here — caller
            // already loaded specs — but defensively safe).
            (None, _) => true,
            // No compressed partitions yet — nothing to gate on.
            (_, None) => true,
            // Both present: safe iff every compressed partition is at or
            // after the json_extract install time.
            (Some(added), Some(oldest)) => oldest >= added,
        }
    })
}

unsafe fn load_extract_specs_for_rel(rel_oid: pg_sys::Oid) -> Vec<crate::compress::ExtractSpec> {
    if rel_oid == pg_sys::InvalidOid {
        return Vec::new();
    }
    pgrx::Spi::connect(|client| -> Vec<crate::compress::ExtractSpec> {
        // Single SQL: resolve rel_oid via name to its containing deltatable,
        // matching either the parent table or a partition. Returns
        // json_extract from the deltatable.
        // Resolve rel_oid → (schema, table). Then look up the deltatable
        // either by the parent's name OR via a partition row that points back
        // at it. UNION ALL keeps the SQL simple and lets either match win.
        let row = client
            .select(
                "WITH ident AS (
                     SELECT n.nspname AS s, c.relname AS t
                     FROM pg_class c
                     JOIN pg_namespace n ON c.relnamespace = n.oid
                     WHERE c.oid = $1
                 )
                 SELECT h.json_extract FROM deltax.deltax_deltatable h, ident i
                  WHERE h.schema_name = i.s AND h.table_name = i.t
                  UNION ALL
                 SELECT h.json_extract FROM deltax.deltax_deltatable h
                  JOIN deltax.deltax_partition p ON p.deltatable_id = h.id
                  JOIN ident i ON p.schema_name = i.s AND p.table_name = i.t
                  LIMIT 1",
                None,
                &[rel_oid.into()],
            )
            .ok();
        let Some(row) = row else { return Vec::new() };
        let jx_value = row
            .first()
            .get_one::<pgrx::datum::JsonB>()
            .ok()
            .flatten()
            .map(|j| j.0);
        let Some(jx_value) = jx_value else {
            return Vec::new();
        };
        crate::compress::parse_extract_specs(&jx_value)
    })
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

        // Read companion_oid from the path's OidList custom_private.
        let companion_oid = pg_sys::list_nth_oid((*best_path).custom_private, 0);

        // Look up json_extract config via the rel's actual OID (works for
        // both partition-direct and parent-baserel queries — the loader
        // matches on partition or parent name). SPI is fine here: we're
        // inside the planner with a valid snapshot. Returns an empty Vec
        // when extraction isn't configured.
        let rti = (*rel).relid;
        let scan_rel_oid = rti_to_rel_oid(_root, rti);
        let extract_specs: Vec<crate::compress::ExtractSpec> =
            load_extract_specs_for_rel(scan_rel_oid);

        // Activate the json_extract synthetic-tlist scaffolding when the
        // deltatable has json_extract specs. We set `custom_scan_tlist` so:
        //   - PG widens the scan slot to `physical_natts + M` positions.
        //   - `set_customscan_references` rewrites our scan's tlist to
        //     `Var(INDEX_VAR, k)` for both physical and synthetic positions.
        //   - The post-setrefs planner_hook walker then reads our
        //     `custom_scan_tlist` to discover what's at each slot position
        //     and substitutes matching chain Exprs in upper plans.
        // Don't set custom_scan_tlist here. PG's set_customscan_references
        // (or some path between plan_custom_path and there) empirically
        // nulls it before our planner_hook walker can see it. The walker
        // rebuilds custom_scan_tlist from catalog config after setrefs runs,
        // sidestepping the issue. See `rebuild_custom_scan_tlist_from_catalog`.
        let _ = (scan_rel_oid, &extract_specs, rti);

        // Build int-form custom_private: [companion_oid_as_int, -1 (sentinel), col0, col1, ...]
        let mut private_list =
            pg_sys::lappend_int(std::ptr::null_mut(), u32::from(companion_oid) as i32);

        // Extract needed column attribute numbers from tlist + quals
        let varno = (*rel).relid;
        let mut needed_attrs: *mut pg_sys::Bitmapset = std::ptr::null_mut();
        pg_sys::pull_varattnos(tlist as *mut pg_sys::Node, varno, &mut needed_attrs);
        pg_sys::pull_varattnos(
            (*cscan).scan.plan.qual as *mut pg_sys::Node,
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

        // Append Top-N info: [-2, effective_limit, sort_ascending_flag, multi_col_sort_flag, sort_col_attno, nulls_first]
        let (effective_limit, sort_ascending, multi_col_sort, sort_col_attno, nulls_first) =
            TOPN_INFO.with(|cell| cell.replace((0, true, false, 0, false)));
        if effective_limit > 0 {
            private_list = pg_sys::lappend_int(private_list, -2);
            private_list = pg_sys::lappend_int(private_list, effective_limit as i32);
            private_list = pg_sys::lappend_int(private_list, if sort_ascending { 1 } else { 0 });
            private_list = pg_sys::lappend_int(private_list, if multi_col_sort { 1 } else { 0 });
            private_list = pg_sys::lappend_int(private_list, sort_col_attno);
            private_list = pg_sys::lappend_int(private_list, if nulls_first { 1 } else { 0 });
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
    qual_list: *mut pg_sys::List,
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

        // Store in custom_private (int list for consistency with MinMax
        // serialization): [oid1, ..., oidN, -1, qual_bytes_len, bytes...]
        let mut private_list = append_oids_as_ints(std::ptr::null_mut(), companion_oids);
        private_list = pg_sys::lappend_int(private_list, -1);
        private_list = append_qual_list_as_bytes(private_list, qual_list);
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
            -1,                 // consttypmod
            pg_sys::InvalidOid, // constcollid
            8,                  // constlen (sizeof int64)
            pg_sys::Datum::from(0usize),
            false, // constisnull
            true,  // constbyval
        );
        let scan_tle = pg_sys::makeTargetEntry(
            const_node as *mut pg_sys::Expr,
            1,                    // resno
            std::ptr::null_mut(), // resname
            false,                // resjunk
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
            1,                    // resno
            std::ptr::null_mut(), // resname
            false,                // resjunk
        );
        (*cscan).scan.plan.targetlist = pg_sys::lappend(std::ptr::null_mut(), plan_tle as *mut _);

        // Forward custom_private verbatim — already int-encoded with
        // [oids..., -1, qual_bytes_len, bytes...] shape.
        let path_private = (*best_path).custom_private;
        let path_len = (*path_private).length;
        let mut private_list: *mut pg_sys::List = std::ptr::null_mut();
        for i in 0..path_len {
            let val = pg_sys::list_nth_int(path_private, i);
            private_list = pg_sys::lappend_int(private_list, val);
        }

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

/// Kind of metadata-only aggregate handled by the (historically-named)
/// `DeltaXMinMax` path. Besides MIN/MAX this path now also answers
/// `SUM(col)`, `COUNT(col)`, and `COUNT(*)` directly from the per-segment
/// stats stored in `_<partition>_colstats` and `_<partition>_meta`.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetaAggKind {
    Min = 0,
    Max = 1,
    Sum = 2,
    CountCol = 3,
    CountStar = 4,
}

impl MetaAggKind {
    pub fn from_i32(v: i32) -> Self {
        match v {
            0 => MetaAggKind::Min,
            1 => MetaAggKind::Max,
            2 => MetaAggKind::Sum,
            3 => MetaAggKind::CountCol,
            4 => MetaAggKind::CountStar,
            _ => unreachable!("pg_deltax: invalid MetaAggKind encoding: {}", v),
        }
    }
}

/// Specification for one metadata-only aggregate in a multi-aggregate
/// pushdown. Name kept as `MinMaxAggSpec` for backwards compatibility
/// with existing call sites (hook + plan + executor).
pub struct MinMaxAggSpec {
    pub kind: MetaAggKind,
    pub varattno: i16,                // 0 for CountStar
    pub result_type_oid: pg_sys::Oid, // PG return type of the aggregate
    pub col_type_oid: pg_sys::Oid,    // source column type (InvalidOid for CountStar)
    pub typlen: i16,
    pub typbyval: bool,
    /// Constant offset for `SUM(col + N)` shape. Adds `const_offset *
    /// nonnull_count` to the metadata-derived sum at finalize. Zero for
    /// all other aggregate shapes (MIN/MAX, COUNT, SUM with plain Var arg).
    pub const_offset: i64,
}

/// Add a DeltaXMinMax custom path to the grouped relation's pathlist.
///
/// Despite the name, this path now serves MIN/MAX/SUM/COUNT(col)/COUNT(*)
/// from per-segment metadata. Optionally accepts a filter-qual list
/// (time-range and/or segment-by equality) — segments that don't match
/// are skipped inside the executor via `extract_segment_filters` +
/// `load_segments_heap`.
pub unsafe fn add_minmax_path(
    _root: *mut pg_sys::PlannerInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    companion_oids: &[pg_sys::Oid],
    agg_specs: &[MinMaxAggSpec],
    qual_list: *mut pg_sys::List,
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
        //  kind_0, varattno_0, result_type_0, col_type_0, typlen_0, typbyval_0,
        //    const_offset_lo_0, const_offset_hi_0,
        //  kind_1, varattno_1, ...,
        //  qual_bytes_len, qual_byte0, qual_byte1, ...]
        // 8 ints per spec (was 6 before const_offset was added for SUM(col+N)).
        let mut private_list = append_oids_as_ints(std::ptr::null_mut(), companion_oids);
        private_list = pg_sys::lappend_int(private_list, -1);
        private_list = pg_sys::lappend_int(private_list, agg_specs.len() as i32);
        for spec in agg_specs {
            private_list = pg_sys::lappend_int(private_list, spec.kind as i32);
            private_list = pg_sys::lappend_int(private_list, spec.varattno as i32);
            private_list =
                pg_sys::lappend_int(private_list, u32::from(spec.result_type_oid) as i32);
            private_list = pg_sys::lappend_int(private_list, u32::from(spec.col_type_oid) as i32);
            private_list = pg_sys::lappend_int(private_list, spec.typlen as i32);
            private_list = pg_sys::lappend_int(private_list, if spec.typbyval { 1 } else { 0 });
            private_list = pg_sys::lappend_int(private_list, spec.const_offset as i32);
            private_list = pg_sys::lappend_int(private_list, (spec.const_offset >> 32) as i32);
        }
        // Serialize quals via nodeToString so they survive plan caching.
        private_list = append_qual_list_as_bytes(private_list, qual_list);
        (*cpath).custom_private = private_list;

        (*cpath).custom_paths = std::ptr::null_mut();
        (*cpath).custom_restrictinfo = std::ptr::null_mut();
        (*cpath).methods = &DELTAX_MINMAX_PATH_METHODS.0;

        pg_sys::add_path(output_rel, cpath as *mut pg_sys::Path);
    }
}

/// Per-aggregate info parsed from custom_private during plan creation.
struct PlanAggSpec {
    kind: MetaAggKind,
    varattno: i32,
    result_type_oid: pg_sys::Oid,
    col_type_oid: pg_sys::Oid,
    typlen: i32,
    typbyval: bool,
    const_offset: i64,
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
        let mut qual_bytes: Vec<u8> = Vec::new();
        let mut found_sentinel = false;
        let mut i: i32 = 0;

        // Parse companion OIDs until the -1 sentinel.
        while i < path_len {
            let val = pg_sys::list_nth_int(path_private, i);
            i += 1;
            if val == -1 {
                found_sentinel = true;
                break;
            }
            companion_oids.push(pg_sys::Oid::from(val as u32));
        }
        if !found_sentinel {
            pgrx::error!("pg_deltax: DeltaXMinMax custom_private missing -1 sentinel");
        }

        // Parse num_aggs.
        let num_aggs = pg_sys::list_nth_int(path_private, i);
        i += 1;

        // Parse 8 ints per agg spec.
        for _ in 0..num_aggs {
            let fields: Vec<i32> = (0..8)
                .map(|off| pg_sys::list_nth_int(path_private, i + off))
                .collect();
            i += 8;
            // i64 reassembled from two i32s (low first, then high).
            let const_offset = (fields[6] as u32 as i64) | ((fields[7] as i64) << 32);
            agg_specs.push(PlanAggSpec {
                kind: MetaAggKind::from_i32(fields[0]),
                varattno: fields[1],
                result_type_oid: pg_sys::Oid::from(fields[2] as u32),
                col_type_oid: pg_sys::Oid::from(fields[3] as u32),
                typlen: fields[4],
                typbyval: fields[5] != 0,
                const_offset,
            });
        }

        // Parse trailing qual bytes.
        if i < path_len {
            let qlen = pg_sys::list_nth_int(path_private, i);
            i += 1;
            for _ in 0..qlen {
                qual_bytes.push(pg_sys::list_nth_int(path_private, i) as u8);
                i += 1;
            }
        }

        // Build custom_scan_tlist and plan.targetlist: one entry per aggregate
        let mut scan_tlist: *mut pg_sys::List = std::ptr::null_mut();
        let mut plan_tlist: *mut pg_sys::List = std::ptr::null_mut();

        for (idx, spec) in agg_specs.iter().enumerate() {
            let resno = (idx + 1) as i16;

            // custom_scan_tlist entry
            let const_node = pg_sys::makeConst(
                spec.result_type_oid,
                -1,                 // consttypmod
                pg_sys::InvalidOid, // constcollid
                spec.typlen,        // constlen
                pg_sys::Datum::from(0usize),
                true,          // constisnull (placeholder)
                spec.typbyval, // constbyval
            );
            let scan_tle = pg_sys::makeTargetEntry(
                const_node as *mut pg_sys::Expr,
                resno,
                std::ptr::null_mut(), // resname
                false,                // resjunk
            );
            scan_tlist = pg_sys::lappend(scan_tlist, scan_tle as *mut _);

            // plan.targetlist entry (PG setrefs will match to custom_scan_tlist)
            let const_node2 = pg_sys::makeConst(
                spec.result_type_oid,
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

        // Build plan's custom_private: same shape as the path's —
        // [oid1, ..., -1, num_aggs, <6 ints per spec>, qual_bytes_len, bytes...]
        // Executor needs col_type_oid for SUM dispatch and the quals for
        // segment pruning, so forward everything.
        let mut plan_private = append_oids_as_ints(std::ptr::null_mut(), &companion_oids);
        plan_private = pg_sys::lappend_int(plan_private, -1);
        plan_private = pg_sys::lappend_int(plan_private, agg_specs.len() as i32);
        for spec in &agg_specs {
            plan_private = pg_sys::lappend_int(plan_private, spec.kind as i32);
            plan_private = pg_sys::lappend_int(plan_private, spec.varattno);
            plan_private =
                pg_sys::lappend_int(plan_private, u32::from(spec.result_type_oid) as i32);
            plan_private = pg_sys::lappend_int(plan_private, u32::from(spec.col_type_oid) as i32);
            plan_private = pg_sys::lappend_int(plan_private, spec.typlen);
            plan_private = pg_sys::lappend_int(plan_private, if spec.typbyval { 1 } else { 0 });
            plan_private = pg_sys::lappend_int(plan_private, spec.const_offset as i32);
            plan_private = pg_sys::lappend_int(plan_private, (spec.const_offset >> 32) as i32);
        }
        plan_private = pg_sys::lappend_int(plan_private, qual_bytes.len() as i32);
        for &b in &qual_bytes {
            plan_private = pg_sys::lappend_int(plan_private, b as i32);
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
    pub col_idx: i32, // 0-based column index, -1 for COUNT(*)
    pub result_type_oid: pg_sys::Oid,
    pub col_type_oid: pg_sys::Oid,       // source column type OID
    pub expr_kind: super::exec::AggExpr, // Column, LengthOf, or AddConst
    pub const_offset: i64,               // Only used when expr_kind == AddConst
    /// H.2: monotonic transform applied to the stored MIN/MAX value at
    /// finalize / partial-emit. Default `None`. Recognizer in `hook.rs` sets
    /// `PgUsShift { delta }` for the timestamptz_pl_interval Aggref shape.
    pub output_transform: super::exec::OutputTransform,
}

/// Serialize a CaseWhenValue into the integer list.
/// Format: tag(0=ColumnRef, 1=StringConst), then value.
unsafe fn serialize_case_when_value(
    value: &super::exec::CaseWhenValue,
    private_list: &mut *mut pg_sys::List,
) {
    unsafe {
        match value {
            super::exec::CaseWhenValue::ColumnRef(col_idx) => {
                *private_list = pg_sys::lappend_int(*private_list, 0); // tag=0
                *private_list = pg_sys::lappend_int(*private_list, *col_idx as i32);
            }
            super::exec::CaseWhenValue::StringConst(s) => {
                *private_list = pg_sys::lappend_int(*private_list, 1); // tag=1
                *private_list = pg_sys::lappend_int(*private_list, s.len() as i32);
                for &b in s.as_bytes() {
                    *private_list = pg_sys::lappend_int(*private_list, b as i32);
                }
            }
        }
    }
}

/// Deserialize a CaseWhenValue from the integer list.
unsafe fn deserialize_case_when_value(
    path_private: *mut pg_sys::List,
    idx: &mut i32,
) -> super::exec::CaseWhenValue {
    unsafe {
        let tag = pg_sys::list_nth_int(path_private, *idx);
        *idx += 1;
        if tag == 0 {
            // ColumnRef
            let col_idx = pg_sys::list_nth_int(path_private, *idx) as usize;
            *idx += 1;
            super::exec::CaseWhenValue::ColumnRef(col_idx)
        } else {
            // StringConst
            let str_len = pg_sys::list_nth_int(path_private, *idx) as usize;
            *idx += 1;
            let mut bytes = Vec::with_capacity(str_len);
            for _ in 0..str_len {
                bytes.push(pg_sys::list_nth_int(path_private, *idx) as u8);
                *idx += 1;
            }
            super::exec::CaseWhenValue::StringConst(String::from_utf8_lossy(&bytes).into_owned())
        }
    }
}

/// Phase C.2 activation — shared `custom_private` (path-level) builder for the
/// DeltaXAgg complete-mode and partial-mode CustomPaths. Both modes use the
/// same plan_agg_path callback so the wire format must match exactly except
/// the trailing `is_partial` flag.
unsafe fn build_agg_path_private(
    companion_oids: &[pg_sys::Oid],
    agg_specs: &[AggSpec],
    group_specs: &[super::exec::GroupByColSpec],
    is_partial: bool,
) -> *mut pg_sys::List {
    unsafe {
        let mut private_list = append_oids_as_ints(std::ptr::null_mut(), companion_oids);
        private_list = pg_sys::lappend_int(private_list, -1); // sentinel
        private_list = pg_sys::lappend_int(private_list, agg_specs.len() as i32);
        for spec in agg_specs {
            private_list = pg_sys::lappend_int(private_list, spec.agg_type as i32);
            private_list = pg_sys::lappend_int(private_list, spec.col_idx);
            private_list =
                pg_sys::lappend_int(private_list, u32::from(spec.result_type_oid) as i32);
            private_list = pg_sys::lappend_int(private_list, u32::from(spec.col_type_oid) as i32);
            private_list = pg_sys::lappend_int(private_list, spec.expr_kind as i32);
            if matches!(spec.expr_kind, super::exec::AggExpr::AddConst) {
                private_list = pg_sys::lappend_int(private_list, spec.const_offset as i32);
            }
            // H.2: per-MIN/MAX OutputTransform trailer. Only emitted for
            // Min/Max — keeps the existing wire format for Count/Sum/Avg/CD
            // intact (no test churn). Tag 0 = None (no follow-up); tag 1 =
            // PgUsShift, followed by lo + hi i32 halves of the i64 delta.
            if matches!(
                spec.agg_type,
                super::exec::AggType::Min | super::exec::AggType::Max
            ) {
                match spec.output_transform {
                    super::exec::OutputTransform::None => {
                        private_list = pg_sys::lappend_int(private_list, 0);
                    }
                    super::exec::OutputTransform::PgUsShift { delta } => {
                        private_list = pg_sys::lappend_int(private_list, 1);
                        private_list = pg_sys::lappend_int(private_list, delta as i32);
                        private_list = pg_sys::lappend_int(private_list, (delta >> 32) as i32);
                    }
                }
            }
        }
        private_list = pg_sys::lappend_int(private_list, group_specs.len() as i32);
        for gs in group_specs {
            private_list = pg_sys::lappend_int(private_list, gs.col_idx);
            private_list = pg_sys::lappend_int(private_list, u32::from(gs.type_oid) as i32);
            match &gs.expr {
                super::exec::GroupByExpr::Column => {
                    private_list = pg_sys::lappend_int(private_list, 0);
                }
                super::exec::GroupByExpr::RegexpReplace {
                    pattern,
                    replacement,
                    func_oid,
                    collation,
                } => {
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
                super::exec::GroupByExpr::DateTrunc { unit, func_oid, .. } => {
                    private_list = pg_sys::lappend_int(private_list, 2);
                    private_list = pg_sys::lappend_int(private_list, *func_oid as i32);
                    private_list = pg_sys::lappend_int(private_list, unit.len() as i32);
                    for &b in unit.as_bytes() {
                        private_list = pg_sys::lappend_int(private_list, b as i32);
                    }
                }
                super::exec::GroupByExpr::Extract {
                    unit,
                    func_oid,
                    divisor,
                } => {
                    // tag=3, func_oid, divisor (i64 split hi/lo), unit_len, unit_bytes...
                    private_list = pg_sys::lappend_int(private_list, 3);
                    private_list = pg_sys::lappend_int(private_list, *func_oid as i32);
                    private_list = pg_sys::lappend_int(private_list, (*divisor >> 32) as i32);
                    private_list = pg_sys::lappend_int(private_list, *divisor as i32);
                    private_list = pg_sys::lappend_int(private_list, unit.len() as i32);
                    for &b in unit.as_bytes() {
                        private_list = pg_sys::lappend_int(private_list, b as i32);
                    }
                }
                super::exec::GroupByExpr::AddConst { offset, op_oid } => {
                    private_list = pg_sys::lappend_int(private_list, 4);
                    private_list = pg_sys::lappend_int(private_list, *offset as i32);
                    private_list = pg_sys::lappend_int(private_list, *op_oid as i32);
                }
                super::exec::GroupByExpr::CaseWhen(spec) => {
                    private_list = pg_sys::lappend_int(private_list, 5);
                    private_list = pg_sys::lappend_int(private_list, spec.clauses.len() as i32);
                    for clause in &spec.clauses {
                        private_list =
                            pg_sys::lappend_int(private_list, clause.conditions.len() as i32);
                        for cond in &clause.conditions {
                            private_list = pg_sys::lappend_int(private_list, cond.col_idx as i32);
                            private_list = pg_sys::lappend_int(private_list, cond.op as i32);
                            private_list =
                                pg_sys::lappend_int(private_list, (cond.const_val >> 32) as i32);
                            private_list = pg_sys::lappend_int(private_list, cond.const_val as i32);
                        }
                        serialize_case_when_value(&clause.result, &mut private_list);
                    }
                    serialize_case_when_value(&spec.default, &mut private_list);
                }
            }
        }
        // Trailer: is_partial. plan_agg_path reads this and forwards into
        // plan_private; runtime uses it to decide between compact_finalize
        // and compact_emit_partial.
        private_list = pg_sys::lappend_int(private_list, if is_partial { 1 } else { 0 });
        private_list
    }
}

/// Phase C.2.f — predicate mirroring `agg::can_use_compact_accs` but operating
/// on `AggSpec` (the path-level type). Both check the same conditions: fixed-
/// size accumulators on integer/float/text columns. Diverging would silently
/// mismatch leader and worker eligibility, so this helper sticks to the same
/// shape.
fn parallel_compact_aggs_ok(agg_specs: &[AggSpec]) -> bool {
    if agg_specs.is_empty() {
        return false;
    }
    agg_specs.iter().all(|spec| {
        match spec.agg_type {
            super::exec::AggType::CountStar | super::exec::AggType::Count => true,
            super::exec::AggType::Sum | super::exec::AggType::Avg => {
                let t = spec.col_type_oid;
                t == pg_sys::INT2OID
                    || t == pg_sys::INT4OID
                    || t == pg_sys::INT8OID
                    || t == pg_sys::FLOAT4OID
                    || t == pg_sys::FLOAT8OID
            }
            super::exec::AggType::Min | super::exec::AggType::Max => {
                let t = spec.col_type_oid;
                t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID
            }
            // CountDistinct is excluded by the eligibility predicate before
            // this helper is reached; Phase D will revisit.
            super::exec::AggType::CountDistinct => false,
        }
    })
}

/// Phase C.2 activation — predicate restricting partial-mode emit to the
/// aggregate shapes `compact_emit_partial` knows how to serialise. Today:
/// COUNT/COUNT(*) → int8; SUM(int2/int4) → int8; SUM(float4/float8) →
/// float8; MIN/MAX(text) → text. Excludes SUM(int8) (transtype=internal,
/// needs int8_avg_serialize), AVG (same), and COUNT(DISTINCT) (no
/// aggcombinefn in PG core).
fn agg_specs_partial_emittable(agg_specs: &[AggSpec]) -> bool {
    if agg_specs.is_empty() {
        return false;
    }
    agg_specs.iter().all(|spec| match spec.agg_type {
        super::exec::AggType::CountStar | super::exec::AggType::Count => true,
        super::exec::AggType::Sum => {
            let t = spec.col_type_oid;
            t == pg_sys::INT2OID
                || t == pg_sys::INT4OID
                || t == pg_sys::FLOAT4OID
                || t == pg_sys::FLOAT8OID
        }
        super::exec::AggType::Min | super::exec::AggType::Max => {
            let t = spec.col_type_oid;
            t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID
        }
        // SUM(int8) / AVG / COUNT(DISTINCT) excluded.
        _ => false,
    })
}

/// Phase C.2 activation — true iff every `Var` reachable from `qual_list`
/// has a numeric `vartype` (int / float / timestamp / date / bool). Used
/// to gate the partial-mode CustomPath: `process_segments_compact` only
/// decompresses numeric columns, so a WHERE qual on a non-numeric col
/// would silently get its filter skipped (the per-row evaluator sees an
/// empty Vec for that column and `continue`s past the qual). Rejecting
/// such queries up front keeps the complete-aggregate path correct.
///
/// `pull_var_clause` walks `OpExpr` / `BoolExpr` / `FuncExpr` /
/// `ScalarArrayOpExpr` / etc. transparently; `flags = 0` skips into
/// aggregate args and placeholder vars (none of which appear in WHERE
/// clauses anyway).
unsafe fn quals_reference_only_numeric_vars(qual_list: *mut pg_sys::List) -> bool {
    if qual_list.is_null() {
        return true;
    }
    unsafe {
        let nquals = (*qual_list).length;
        for i in 0..nquals {
            let cell = (*qual_list).elements.add(i as usize);
            let node = (*cell).ptr_value as *mut pg_sys::Node;
            if node.is_null() {
                continue;
            }
            let vars = pg_sys::pull_var_clause(node, 0);
            if vars.is_null() {
                continue;
            }
            let nvars = (*vars).length;
            for j in 0..nvars {
                let v = (*(*vars).elements.add(j as usize)).ptr_value as *mut pg_sys::Var;
                if v.is_null() {
                    continue;
                }
                if !is_partial_eligible_var_type((*v).vartype) {
                    return false;
                }
            }
        }
    }
    true
}

/// Set of `vartype` OIDs that `process_segments_compact` can correctly
/// decompress + filter. Mirrors `batch_quals_all_numeric` in
/// `agg.rs::batch_quals_all_numeric`. Diverging from that set would let
/// the planner gate accept queries the runtime mishandles.
fn is_partial_eligible_var_type(t: pg_sys::Oid) -> bool {
    matches!(
        t,
        pg_sys::INT2OID
            | pg_sys::INT4OID
            | pg_sys::INT8OID
            | pg_sys::FLOAT4OID
            | pg_sys::FLOAT8OID
            | pg_sys::TIMESTAMPOID
            | pg_sys::TIMESTAMPTZOID
            | pg_sys::DATEOID
            | pg_sys::BOOLOID
    )
}

/// Phase C.2 activation — adds a partial-mode DeltaXAgg CustomPath to
/// `partially_grouped_rel.partial_pathlist`, wraps it in `Gather` (via
/// `create_gather_path`), wraps that in a Final Aggregate
/// (`create_agg_path` with `AGGSPLIT_FINAL_DESERIAL`), and adds the
/// result to `grouped_rel.pathlist`. PG core's `create_grouping_paths`
/// already populated `partially_grouped_rel.reltarget` with the partial
/// target (Aggrefs marked `AGGSPLIT_INITIAL_SERIAL`); we piggyback on
/// that. The planner then picks whichever of (complete-DeltaXAgg) or
/// (partial-DeltaXAgg + Gather + Final Aggregate) is cheaper.
///
/// Eligibility — must all hold or we skip:
/// - `extra` non-null and `havingQual` null (HAVING is Phase E).
/// - `agg_specs_partial_emittable` (Count, SUM int4/float, MIN/MAX text;
///   excludes SUM(int8) / AVG / COUNT(DISTINCT)).
/// - Group keys empty or `can_use_compact_keys_path` (compact-eligible
///   numeric keys).
/// - `quals_reference_only_numeric_vars` — rejects WHERE on text /
///   varchar / json columns. Otherwise `process_segments_compact` would
///   silently skip the qual and over-count.
/// - `recommend_agg_workers > 0` (worth parallelising).
#[allow(clippy::too_many_arguments)]
pub unsafe fn add_agg_partial_path(
    root: *mut pg_sys::PlannerInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    companion_oids: &[pg_sys::Oid],
    agg_specs: &[AggSpec],
    group_specs: &[super::exec::GroupByColSpec],
    pg_estimated_groups: f64,
    extra: *mut pg_sys::GroupPathExtraData,
) {
    unsafe {
        // -------- Eligibility --------
        // Operator escape hatch: when `pg_deltax.disable_parallel_agg = on`,
        // the planner only sees the complete CustomScan DeltaXAgg path.
        // The complete path's internal-rayon parallelism still runs.
        if crate::DISABLE_PARALLEL_AGG.get() {
            return;
        }
        if extra.is_null() {
            return;
        }
        let having_qual = (*extra).havingQual;
        if !having_qual.is_null() {
            return;
        }
        if !agg_specs_partial_emittable(agg_specs) {
            return;
        }
        if !group_specs.is_empty() && !super::exec::can_use_compact_keys_path(group_specs, &[]) {
            return;
        }
        // Reject WHERE clauses that reference non-numeric columns. See
        // `quals_reference_only_numeric_vars` for the rationale.
        let parse = (*root).parse;
        if !parse.is_null() {
            let jointree = (*parse).jointree;
            if !jointree.is_null() && !(*jointree).quals.is_null() {
                let q = (*jointree).quals;
                let wrap = pg_sys::lappend(std::ptr::null_mut(), q as *mut _);
                let ok = quals_reference_only_numeric_vars(wrap);
                if !ok {
                    return;
                }
            }
        }
        let workers = cost::recommend_agg_workers(companion_oids);
        if workers <= 0 {
            return;
        }

        // -------- Fetch partially_grouped_rel --------
        let pgr = pg_sys::fetch_upper_rel(
            root,
            pg_sys::UpperRelationKind::UPPERREL_PARTIAL_GROUP_AGG,
            (*output_rel).relids,
        );
        if pgr.is_null() {
            return;
        }
        if (*pgr).reltarget.is_null() {
            return;
        }

        // -------- Build partial CustomPath --------
        let cpath =
            pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;
        (*cpath).path.type_ = pg_sys::NodeTag::T_CustomPath;
        (*cpath).path.pathtype = pg_sys::NodeTag::T_CustomScan;
        (*cpath).path.parent = pgr;
        (*cpath).path.pathtarget = (*pgr).reltarget;

        let estimated_rows = if group_specs.is_empty() {
            1.0
        } else if pg_estimated_groups > 0.0 {
            pg_estimated_groups
        } else {
            100.0
        };
        (*cpath).path.rows = estimated_rows;

        let (startup, total) = cost::estimate_agg_cost(
            companion_oids,
            agg_specs.len(),
            estimated_rows,
            /* num_having_filters */ 0,
            workers as usize,
        );
        (*cpath).path.startup_cost = startup;
        (*cpath).path.total_cost = total;
        (*cpath).path.parallel_workers = workers;
        (*cpath).path.parallel_aware = true;
        (*cpath).path.parallel_safe = true;
        (*cpath).path.pathkeys = std::ptr::null_mut();

        let private_list = build_agg_path_private(
            companion_oids,
            agg_specs,
            group_specs,
            /* is_partial */ true,
        );
        (*cpath).custom_private = private_list;
        (*cpath).custom_paths = std::ptr::null_mut();
        (*cpath).custom_restrictinfo = std::ptr::null_mut();
        (*cpath).methods = &DELTAX_AGG_PATH_METHODS.0;

        pg_sys::add_partial_path(pgr, cpath as *mut pg_sys::Path);

        // -------- Wrap in Gather --------
        let mut gather_rows: f64 = (estimated_rows * workers as f64).max(1.0);
        let gather_path = pg_sys::create_gather_path(
            root,
            pgr,
            cpath as *mut pg_sys::Path,
            (*pgr).reltarget,
            std::ptr::null_mut(),
            &mut gather_rows,
        );
        if gather_path.is_null() {
            return;
        }

        // -------- Wrap Gather in Final Aggregate (AGGSPLIT_FINAL_DESERIAL) --------
        let agg_strategy = if group_specs.is_empty() {
            pg_sys::AggStrategy::AGG_PLAIN
        } else {
            pg_sys::AggStrategy::AGG_HASHED
        };
        let final_path = pg_sys::create_agg_path(
            root,
            output_rel,
            gather_path as *mut pg_sys::Path,
            (*output_rel).reltarget,
            agg_strategy,
            pg_sys::AggSplit::AGGSPLIT_FINAL_DESERIAL,
            (*root).processed_groupClause,
            having_qual as *mut pg_sys::List,
            &(*extra).agg_final_costs,
            pg_estimated_groups.max(1.0),
        );
        if final_path.is_null() {
            return;
        }

        pg_sys::add_path(output_rel, final_path as *mut pg_sys::Path);
    }
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

        // Phase C.2.f — eligibility for the parallel-aware DeltaXAgg path.
        // The runtime path (workers do partials in their DSM slabs, leader
        // merges + finalises) only handles the compact path: integer-packed
        // group keys, fixed-size accumulators, no DISTINCT / HAVING / Top-N
        // / LIMIT. Anything else stays serial / internal-rayon.
        let topn_info = peek_agg_topn_info();
        let topn_active = topn_info.is_some_and(|info| info.limit > 0);
        let parallel_eligible = having_filters.is_empty()
            && !topn_active
            && !agg_specs
                .iter()
                .any(|s| s.agg_type == super::exec::AggType::CountDistinct)
            && parallel_compact_aggs_ok(agg_specs)
            && (group_specs.is_empty() || super::exec::can_use_compact_keys_path(group_specs, &[]));

        // Phase C.2.f wiring: the predicate + `recommend_agg_workers` are in
        // place but we keep `parallel_workers = 0` here. Hooking the
        // parallel-aware path through PG's Gather model is non-trivial: PG
        // expects "partial aggregate → Gather → final aggregate" semantics,
        // whereas DeltaXAgg's design has the leader merge + emit final rows
        // directly (workers contribute via DSM, not via tuple stream). The
        // C.2.b–e infrastructure (DSM hooks, agg_wire, deferred exec_ctx,
        // worker / leader claim+merge) is sound and exercised by the unit
        // tests; activating it requires a separate planner integration —
        // either splitting into partial-DeltaXAgg + final-Aggregate, or
        // building a custom Gather-equivalent — which is beyond the scope
        // of C.2 and lands in a follow-up.
        let _eligible = parallel_eligible;
        let _recommended = if parallel_eligible {
            cost::recommend_agg_workers(companion_oids)
        } else {
            0
        };
        let workers: i32 = 0;

        // §5.8-b: real formula replaces the historic `(10.0, 20.0)` hack so
        // future parallel-partial paths can be costed meaningfully against
        // `parallel_setup_cost`. See `cost::estimate_agg_cost` for
        // calibration notes and per-constant rationale.
        let (startup, total) = cost::estimate_agg_cost(
            companion_oids,
            agg_specs.len(),
            estimated_rows,
            having_filters.len(),
            workers as usize,
        );
        (*cpath).path.startup_cost = startup;
        (*cpath).path.total_cost = total;
        (*cpath).path.parallel_workers = workers;
        (*cpath).path.parallel_aware = false;
        (*cpath).path.parallel_safe = false;
        (*cpath).path.pathkeys = if pathkeys.is_null() {
            std::ptr::null_mut()
        } else {
            pathkeys
        };

        // Store in custom_private. Wire format: see `build_agg_path_private`.
        // Trailer is_partial = false on the complete path; the partial path
        // constructor (add_agg_partial_path) sets it true.
        let private_list = build_agg_path_private(
            companion_oids,
            agg_specs,
            group_specs,
            /* is_partial */ false,
        );

        // Store HAVING filters for thread-local passing to plan_agg_path
        if !having_filters.is_empty() {
            set_agg_having_filters(having_filters.to_vec());
        }

        (*cpath).custom_private = private_list;

        (*cpath).custom_paths = std::ptr::null_mut();
        (*cpath).custom_restrictinfo = std::ptr::null_mut();
        (*cpath).methods = &DELTAX_AGG_PATH_METHODS.0;

        // Phase C.2.f — parallel-aware paths must go through `add_partial_path`
        // so PG generates a Gather above DeltaXAgg. With Gather, the
        // EstimateDSM/InitializeDSM/InitializeWorker hooks fire and workers
        // attach DSM. Without it, `parallel_aware = true` on a path added via
        // `add_path` runs standalone with no workers — the leader's
        // `run_leader_merge_and_finalise` would never have `pscan` populated.
        //
        // Workers emit no rows themselves (they write to DSM and return EOF
        // in `exec_agg_scan`); the leader emits all final rows. Gather just
        // concatenates → the upper relation sees the leader's full result
        // set with no Aggregate-final wrapper needed.
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
                    let te_copy =
                        pg_sys::copyObjectImpl(te as *const _) as *mut pg_sys::TargetEntry;
                    (*te_copy).resno = resno;
                    resno += 1;
                    list = pg_sys::lappend(list, te_copy as *mut _);
                }
            }
            list
        };
        (*cscan).scan.plan.targetlist =
            pg_sys::copyObjectImpl(clean_tlist as *const _) as *mut pg_sys::List;
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
            expr_kind: i32,    // 0=Column, 1=LengthOf, 2=AddConst
            const_offset: i32, // Only used when expr_kind == 2
            // H.2: OutputTransform trailer (only for MIN/MAX in path_private).
            // tag: 0=None, 1=PgUsShift. lo/hi only meaningful when tag==1.
            output_tag: i32,
            output_lo: i32,
            output_hi: i32,
        }
        #[derive(Clone)]
        enum ParsedGroupExpr {
            Column,
            RegexpReplace {
                func_oid: u32,
                collation: u32,
                pattern: String,
                replacement: String,
            },
            DateTrunc {
                func_oid: u32,
                unit: String,
            },
            Extract {
                func_oid: u32,
                unit: String,
                divisor: i64,
            },
            AddConst {
                offset: i32,
                op_oid: u32,
            },
            CaseWhen(super::exec::CaseWhenSpec),
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
        // Phase C.2 activation: trailing is_partial flag from path_private.
        // Default false for paths that don't write the trailer (current
        // complete-path code path).
        let mut path_is_partial: bool = false;

        // Sequential parse with index
        let mut idx = 0;
        // Parse OIDs until sentinel
        while idx < path_len {
            let val = pg_sys::list_nth_int(path_private, idx);
            idx += 1;
            if val == -1 {
                break;
            }
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
                // H.2: per-MIN/MAX OutputTransform trailer. tag(0=None, 1=PgUsShift)
                // followed by 2 i32 halves of i64 delta when tag==1. Capture on
                // the path side and re-emit on plan_private below — exec parse
                // expects the same shape.
                let (output_tag, output_lo, output_hi) = if (agg_type
                    == super::exec::AggType::Min as i32
                    || agg_type == super::exec::AggType::Max as i32)
                    && idx < path_len
                {
                    let tag = pg_sys::list_nth_int(path_private, idx);
                    idx += 1;
                    if tag == 1 {
                        let lo = pg_sys::list_nth_int(path_private, idx);
                        let hi = pg_sys::list_nth_int(path_private, idx + 1);
                        idx += 2;
                        (1, lo, hi)
                    } else {
                        (0, 0, 0)
                    }
                } else {
                    (0, 0, 0)
                };
                parsed_aggs.push(ParsedAgg {
                    agg_type,
                    col_idx,
                    result_oid,
                    col_type_oid,
                    expr_kind,
                    const_offset,
                    output_tag,
                    output_lo,
                    output_hi,
                });
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
                    ParsedGroupExpr::RegexpReplace {
                        func_oid,
                        collation,
                        pattern,
                        replacement,
                    }
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
                    // Extract: func_oid, divisor (i64 hi/lo), unit_len, unit_bytes...
                    let func_oid = pg_sys::list_nth_int(path_private, idx) as u32;
                    idx += 1;
                    let div_hi = pg_sys::list_nth_int(path_private, idx) as i64;
                    idx += 1;
                    let div_lo = pg_sys::list_nth_int(path_private, idx) as u32 as i64;
                    idx += 1;
                    let divisor = (div_hi << 32) | div_lo;
                    let unit_len = pg_sys::list_nth_int(path_private, idx) as usize;
                    idx += 1;
                    let mut unit_bytes = Vec::with_capacity(unit_len);
                    for _ in 0..unit_len {
                        unit_bytes.push(pg_sys::list_nth_int(path_private, idx) as u8);
                        idx += 1;
                    }
                    let unit = String::from_utf8_lossy(&unit_bytes).into_owned();
                    ParsedGroupExpr::Extract {
                        func_oid,
                        unit,
                        divisor,
                    }
                } else if expr_tag == 4 {
                    let offset = pg_sys::list_nth_int(path_private, idx);
                    let op_oid = pg_sys::list_nth_int(path_private, idx + 1) as u32;
                    idx += 2;
                    ParsedGroupExpr::AddConst { offset, op_oid }
                } else if expr_tag == 5 {
                    // CaseWhen
                    use super::exec::{
                        CaseWhenClause, CaseWhenCondition, CaseWhenOp, CaseWhenSpec,
                    };
                    let num_clauses = pg_sys::list_nth_int(path_private, idx) as usize;
                    idx += 1;
                    let mut clauses = Vec::with_capacity(num_clauses);
                    for _ in 0..num_clauses {
                        let num_conditions = pg_sys::list_nth_int(path_private, idx) as usize;
                        idx += 1;
                        let mut conditions = Vec::with_capacity(num_conditions);
                        for _ in 0..num_conditions {
                            let cond_col_idx = pg_sys::list_nth_int(path_private, idx) as usize;
                            let op_val = pg_sys::list_nth_int(path_private, idx + 1);
                            let const_hi = pg_sys::list_nth_int(path_private, idx + 2) as i64;
                            let const_lo =
                                pg_sys::list_nth_int(path_private, idx + 3) as u32 as i64;
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
                        let result = deserialize_case_when_value(path_private, &mut idx);
                        clauses.push(CaseWhenClause { conditions, result });
                    }
                    let default = deserialize_case_when_value(path_private, &mut idx);
                    ParsedGroupExpr::CaseWhen(CaseWhenSpec { clauses, default })
                } else {
                    ParsedGroupExpr::Column
                };
                parsed_groups.push(ParsedGroup {
                    col_idx,
                    type_oid,
                    expr,
                });
            }
        }

        // Phase C.2 activation: trailing `is_partial` flag (one int, 0/1).
        // Older paths that don't write it default to false. add_agg_path
        // and add_agg_partial_path are responsible for appending it.
        if idx < path_len {
            path_is_partial = pg_sys::list_nth_int(path_private, idx) != 0;
            idx += 1;
        }
        let _ = idx;

        // Eliminate redundant GROUP BY expressions.
        // If multiple specs reference the same col_idx and are all Column/AddConst,
        // keep one (prefer Column) and record eliminated specs for DerivedGroup output.
        // eliminated_specs: maps (col_idx, offset) → (base_new_idx, delta)
        struct EliminatedSpec {
            base_new_idx: usize,
            delta: i64,
        }
        let mut eliminated_specs: Vec<((i32, i32), EliminatedSpec)> = Vec::new();
        {
            // Find base spec for each col_idx (prefer Column over AddConst)
            let mut col_base: Vec<(i32, usize)> = Vec::new(); // (col_idx, parsed_groups index)
            for (i, g) in parsed_groups.iter().enumerate() {
                match &g.expr {
                    ParsedGroupExpr::Column | ParsedGroupExpr::AddConst { .. } => {
                        if let Some(entry) = col_base.iter_mut().find(|(c, _)| *c == g.col_idx) {
                            // Already have a base for this col_idx
                            let base_i = entry.1;
                            let base_is_column =
                                matches!(parsed_groups[base_i].expr, ParsedGroupExpr::Column);
                            if matches!(g.expr, ParsedGroupExpr::Column) && !base_is_column {
                                // This is Column, replace AddConst base
                                entry.1 = i;
                            }
                        } else {
                            col_base.push((g.col_idx, i));
                        }
                    }
                    _ => {} // DateTrunc/Extract/RegexpReplace — not eligible
                }
            }
            // Only eliminate if there are multiple specs for the same col_idx
            let col_with_dupes: Vec<(i32, usize)> = col_base
                .iter()
                .filter(|(col_idx, _)| {
                    parsed_groups
                        .iter()
                        .filter(|g| {
                            g.col_idx == *col_idx
                                && matches!(
                                    g.expr,
                                    ParsedGroupExpr::Column | ParsedGroupExpr::AddConst { .. }
                                )
                        })
                        .count()
                        > 1
                })
                .cloned()
                .collect();
            if !col_with_dupes.is_empty() {
                let mut to_remove: Vec<usize> = Vec::new();
                for &(col_idx, base_i) in &col_with_dupes {
                    let base_offset: i64 = match &parsed_groups[base_i].expr {
                        ParsedGroupExpr::Column => 0,
                        ParsedGroupExpr::AddConst { offset, .. } => *offset as i64,
                        _ => unreachable!(),
                    };
                    for (i, g) in parsed_groups.iter().enumerate() {
                        if g.col_idx != col_idx || i == base_i {
                            continue;
                        }
                        match &g.expr {
                            ParsedGroupExpr::Column => {
                                let delta = 0 - base_offset;
                                // base_new_idx will be computed after removal
                                to_remove.push(i);
                                eliminated_specs.push((
                                    (col_idx, 0),
                                    EliminatedSpec {
                                        base_new_idx: base_i,
                                        delta,
                                    },
                                ));
                            }
                            ParsedGroupExpr::AddConst { offset, .. } => {
                                let delta = *offset as i64 - base_offset;
                                to_remove.push(i);
                                eliminated_specs.push((
                                    (col_idx, *offset),
                                    EliminatedSpec {
                                        base_new_idx: base_i,
                                        delta,
                                    },
                                ));
                            }
                            _ => {}
                        }
                    }
                }
                // Sort removals in reverse order to remove from back to front
                to_remove.sort_unstable();
                to_remove.dedup();
                // Build index remap: old_idx → new_idx
                let mut remap: Vec<usize> = (0..parsed_groups.len()).collect();
                for &ri in to_remove.iter().rev() {
                    parsed_groups.remove(ri);
                    // Shift all indices above ri
                    for r in &mut remap {
                        if *r > ri && *r > 0 {
                            *r -= 1;
                        }
                    }
                    remap[ri] = usize::MAX; // removed
                }
                // Fix up base_new_idx in eliminated_specs
                for es in &mut eliminated_specs {
                    es.1.base_new_idx = remap[es.1.base_new_idx];
                }
            }
        }

        // Walk tlist to build output mapping:
        // For each tlist entry, determine if it's an Aggref or a group Var/FuncExpr.
        // Track which agg_spec index or group_spec index it maps to.
        // output_map[i] = (type, index) where type=0 → agg, type=1 → group, type=2 → const,
        //                  type=3 → derived group (index=base_gi)
        let mut output_map: Vec<(i32, i32)> = Vec::new();
        let mut derived_deltas: Vec<i64> = Vec::new();
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
                    let group_idx = parsed_groups
                        .iter()
                        .position(|g| g.col_idx == var_attno)
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
                            let arg = (*(*fn_args).elements.add(ai as usize)).ptr_value
                                as *const pg_sys::Node;
                            if !arg.is_null() && (*arg).type_ == pg_sys::NodeTag::T_Var {
                                col_idx = (*(arg as *const pg_sys::Var)).varattno as i32 - 1;
                                break;
                            }
                        }
                    }
                    let group_idx = parsed_groups
                        .iter()
                        .position(|g| {
                            if g.col_idx != col_idx {
                                return false;
                            }
                            match &g.expr {
                                ParsedGroupExpr::RegexpReplace { func_oid, .. } => {
                                    *func_oid == funcid
                                }
                                ParsedGroupExpr::DateTrunc { func_oid, .. } => *func_oid == funcid,
                                ParsedGroupExpr::Extract { func_oid, .. } => *func_oid == funcid,
                                _ => false,
                            }
                        })
                        .unwrap_or(0) as i32;
                    output_map.push((1, group_idx));
                } else if (*expr).type_ == pg_sys::NodeTag::T_OpExpr {
                    // OpExpr in target list. Three cases handled below:
                    //   1. JSONB chain (`data->>'k'`) — match to a synthetic
                    //      column GROUP BY entry via AggChainCtx.
                    //   2. `Var ± Const` — match to AddConst GROUP BY.
                    //   3. fall through to a default that points at group_specs[0].
                    // Try chain first because chain Exprs are also OpExpr nodes;
                    // without this, a `data->>'k'` target falls through to the
                    // AddConst path, finds no Const arg, and emits a bogus
                    // `Group(0)` mapping that crashes finalization when there
                    // are multiple GROUP BY columns.
                    let chain_match = super::json_extract::AggChainCtx::from_root(root)
                        .and_then(|ctx| ctx.match_to_synthetic(expr))
                        .map(|(synth_col_idx, _)| synth_col_idx);
                    if let Some(synth_col_idx) = chain_match {
                        let group_idx = parsed_groups
                            .iter()
                            .position(|g| {
                                g.col_idx == synth_col_idx
                                    && matches!(g.expr, ParsedGroupExpr::Column)
                            })
                            .unwrap_or(0) as i32;
                        output_map.push((1, group_idx));
                        continue;
                    }
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
                                let opname =
                                    std::ffi::CStr::from_ptr(opname_ptr).to_str().unwrap_or("");
                                is_minus = opname == "-";
                            }
                            if (*left).type_ == pg_sys::NodeTag::T_Var
                                && (*right).type_ == pg_sys::NodeTag::T_Const
                            {
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
                            } else if (*left).type_ == pg_sys::NodeTag::T_Const
                                && (*right).type_ == pg_sys::NodeTag::T_Var
                            {
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
                    let group_pos = parsed_groups.iter().position(|g| {
                        if g.col_idx != col_idx {
                            return false;
                        }
                        match &g.expr {
                            ParsedGroupExpr::AddConst {
                                offset,
                                op_oid: spec_op_oid,
                            } => *offset == tlist_offset && *spec_op_oid == op_oid,
                            _ => false,
                        }
                    });
                    if let Some(gi) = group_pos {
                        output_map.push((1, gi as i32));
                    } else if let Some(es) = eliminated_specs
                        .iter()
                        .find(|((ci, off), _)| *ci == col_idx && *off == tlist_offset)
                    {
                        // Eliminated redundant GROUP BY — emit DerivedGroup
                        output_map.push((3, es.1.base_new_idx as i32));
                        derived_deltas.push(es.1.delta);
                    } else {
                        output_map.push((1, 0));
                    }
                } else if (*expr).type_ == pg_sys::NodeTag::T_CaseExpr {
                    // CaseExpr in target list — find the first CaseWhen GROUP BY spec.
                    // There can be multiple CaseWhen specs; match by position among CaseExpr tlist entries.
                    let case_group_idx = parsed_groups
                        .iter()
                        .position(|g| matches!(g.expr, ParsedGroupExpr::CaseWhen(_)))
                        .unwrap_or(0) as i32;
                    output_map.push((1, case_group_idx));
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
            // H.2: re-emit OutputTransform trailer for MIN/MAX. Mirrors the
            // path_private layout so exec's `parse_agg_private` reads the same
            // shape it expects.
            if a.agg_type == super::exec::AggType::Min as i32
                || a.agg_type == super::exec::AggType::Max as i32
            {
                private_list = pg_sys::lappend_int(private_list, a.output_tag);
                if a.output_tag == 1 {
                    private_list = pg_sys::lappend_int(private_list, a.output_lo);
                    private_list = pg_sys::lappend_int(private_list, a.output_hi);
                }
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
                ParsedGroupExpr::RegexpReplace {
                    func_oid,
                    collation,
                    pattern,
                    replacement,
                } => {
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
                ParsedGroupExpr::Extract {
                    func_oid,
                    unit,
                    divisor,
                } => {
                    private_list = pg_sys::lappend_int(private_list, 3);
                    private_list = pg_sys::lappend_int(private_list, *func_oid as i32);
                    private_list = pg_sys::lappend_int(private_list, (*divisor >> 32) as i32);
                    private_list = pg_sys::lappend_int(private_list, *divisor as i32);
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
                ParsedGroupExpr::CaseWhen(spec) => {
                    private_list = pg_sys::lappend_int(private_list, 5);
                    private_list = pg_sys::lappend_int(private_list, spec.clauses.len() as i32);
                    for clause in &spec.clauses {
                        private_list =
                            pg_sys::lappend_int(private_list, clause.conditions.len() as i32);
                        for cond in &clause.conditions {
                            private_list = pg_sys::lappend_int(private_list, cond.col_idx as i32);
                            private_list = pg_sys::lappend_int(private_list, cond.op as i32);
                            private_list =
                                pg_sys::lappend_int(private_list, (cond.const_val >> 32) as i32);
                            private_list = pg_sys::lappend_int(private_list, cond.const_val as i32);
                        }
                        serialize_case_when_value(&clause.result, &mut private_list);
                    }
                    serialize_case_when_value(&spec.default, &mut private_list);
                }
            }
        }
        // Output mapping
        private_list = pg_sys::lappend_int(private_list, output_map.len() as i32);
        let mut const_idx = 0usize;
        let mut derived_idx = 0usize;
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
            } else if *otype == 3 {
                // DerivedGroup output: append delta_hi, delta_lo
                let delta = derived_deltas[derived_idx];
                private_list = pg_sys::lappend_int(private_list, (delta >> 32) as i32);
                private_list = pg_sys::lappend_int(private_list, delta as i32);
                derived_idx += 1;
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

        // Rewrite JSONB chain Exprs in the qual list against the parent's
        // json_extract specs so the serialised quals deserialise into Var
        // nodes that `extract_batch_quals` can recognise. Without this,
        // queries like `WHERE data->>'kind' = 'commit'` hit DeltaXAgg with
        // unrewritten chains, `extract_batch_quals` skips them silently
        // (Var-only matcher), and the WHERE filter is dropped — wrong
        // results. The post-`standard_planner` walker only handles
        // DeltaXDecompress / DeltaXAppend cscans; DeltaXAgg keeps its quals
        // in `custom_private` (not in `scan.plan.qual`) so the walker can't
        // touch them after the fact. Doing it here, while the parse tree is
        // still in scope, is the natural fit.
        if !qual_list.is_null()
            && let Some(ctx) = super::json_extract::AggChainCtx::from_root(root)
        {
            qual_list = super::json_extract::rewrite_chains_in_list(
                qual_list,
                ctx.parent_rti,
                &ctx.specs,
                &ctx.phys,
                ctx.physical_count,
            );
        }

        private_list = append_qual_list_as_bytes(private_list, qual_list);

        // Top-N info: [topn_limit, topn_sort_col, topn_ascending,
        //              derived_max_idx, derived_min_idx] or [0].
        // For backward compat: when derived_minmax is None we still emit the
        // 3-int form (no trailing pair). When sort_col == TOPN_SORT_COL_DERIVED
        // we emit the 5-int form with the slot indices.
        let topn = take_agg_topn_info();
        if let Some(info) = topn {
            private_list = pg_sys::lappend_int(private_list, info.limit as i32);
            private_list = pg_sys::lappend_int(private_list, info.sort_col);
            private_list = pg_sys::lappend_int(private_list, if info.ascending { 1 } else { 0 });
            if let Some((max_idx, min_idx)) = info.derived_minmax {
                private_list = pg_sys::lappend_int(private_list, max_idx);
                private_list = pg_sys::lappend_int(private_list, min_idx);
            }
        } else {
            private_list = pg_sys::lappend_int(private_list, 0);
        }

        // Phase C.2 activation trailer: is_partial flag, propagated from
        // path_private (set by add_agg_partial_path) into the runtime
        // plan_private. parse_agg_private treats absence as false for
        // backward-compat.
        private_list = pg_sys::lappend_int(private_list, if path_is_partial { 1 } else { 0 });

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
unsafe fn extract_quals_from_baserestrictinfo(root: *mut pg_sys::PlannerInfo) -> *mut pg_sys::List {
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
    static APPEND_TOPN_INFO: std::cell::Cell<(i64, bool, bool, i32, bool)> = const { std::cell::Cell::new((0, true, false, 0, false)) };
}

/// Add a DeltaXAppend custom path to the parent relation's pathlist.
///
/// This replaces the Append node with a single CustomScan that internally
/// iterates all compressed companion tables.
#[allow(clippy::too_many_arguments)]
pub unsafe fn add_deltax_append_path(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    companion_oids: &[pg_sys::Oid],
    pathkeys: *mut pg_sys::List,
    effective_limit: i64,
    sort_ascending: bool,
    multi_col_sort: bool,
    sort_col_attno: i32,
    topn_nulls_first: bool,
) {
    unsafe {
        // Store Top-N info once — consumed by both the serial and partial
        // plan callbacks. Partial-path emission suppresses Top-N at the
        // hook level, but the thread-local is the mechanism plan_* uses
        // to reach the executor either way.
        if effective_limit > 0 {
            APPEND_TOPN_INFO.with(|cell| {
                cell.set((
                    effective_limit,
                    sort_ascending,
                    multi_col_sort,
                    sort_col_attno,
                    topn_nulls_first,
                ))
            });
        } else {
            APPEND_TOPN_INFO.with(|cell| cell.set((0, true, false, 0, false)));
        }

        // Clear existing paths (removes Append paths). Must happen before we
        // add our serial path; any partial path added afterwards (by a
        // subsequent call to `add_partial_deltax_append_path`) inserts into
        // the now-empty partial_pathlist.
        (*rel).pathlist = std::ptr::null_mut();
        (*rel).partial_pathlist = std::ptr::null_mut();

        let cpath = build_deltax_append_path(rel, companion_oids, pathkeys, 0);
        pg_sys::add_path(rel, cpath as *mut pg_sys::Path);

        // Mark rel as non-partitioned so that apply_scanjoin_target_to_paths()
        // in grouping_planner does NOT discard our path and rebuild Append
        // paths from children.  DeltaXAppend handles all partitions internally,
        // so the planner must treat this rel as a single-scan base rel.
        (*rel).nparts = 0;
    }
}

/// Partial-path variant of `add_deltax_append_path`: builds a parallel-aware
/// CustomPath that splits segment-granularity work across `workers`
/// processes via a shared DSM cursor. Must be called AFTER
/// `add_deltax_append_path` so the serial wrapper's pathlist/partition
/// reshaping has already run.
pub unsafe fn add_partial_deltax_append_path(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    companion_oids: &[pg_sys::Oid],
    pathkeys: *mut pg_sys::List,
    workers: i32,
) {
    unsafe {
        if workers <= 0 {
            return;
        }
        let cpath = build_deltax_append_path(rel, companion_oids, pathkeys, workers);
        (*cpath).path.parallel_workers = workers;
        (*cpath).path.parallel_aware = true;
        (*cpath).path.parallel_safe = true;
        pg_sys::add_partial_path(rel, cpath as *mut pg_sys::Path);
    }
}

/// Shared construction for serial and partial DeltaXAppend paths.
/// `workers > 0` applies the parallel cost divisor; callers then flip
/// `parallel_aware`/`parallel_safe`/`parallel_workers` on the returned path.
unsafe fn build_deltax_append_path(
    rel: *mut pg_sys::RelOptInfo,
    companion_oids: &[pg_sys::Oid],
    pathkeys: *mut pg_sys::List,
    workers: i32,
) -> *mut pg_sys::CustomPath {
    unsafe {
        let cpath =
            pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;

        (*cpath).path.type_ = pg_sys::NodeTag::T_CustomPath;
        (*cpath).path.pathtype = pg_sys::NodeTag::T_CustomScan;
        (*cpath).path.parent = rel;
        (*cpath).path.pathtarget = (*rel).reltarget;

        let w = if workers > 0 { workers as usize } else { 0 };
        let mut total_startup = 0.0f64;
        let mut total_cost = 0.0f64;
        let mut total_rows = 0.0f64;
        for &oid in companion_oids {
            let (startup, cost, rows) = cost::estimate_cost(oid, w);
            total_startup += startup;
            total_cost += cost;
            total_rows += rows;
        }
        // Prefer the filter-aware estimate PG already computed for the
        // partitioned parent rel (it sums children's post-filter rows).
        // `rel->rows = 1.0` is PG's fallback when nothing populated
        // `pg_class.reltuples`; if we see that, trust our companion sum
        // instead. When parallel (workers > 0), divide the PG estimate
        // by the parallel divisor so per-worker row counts stay
        // consistent with the serial path.
        let rel_rows = (*rel).rows;
        let path_rows = if rel_rows > 1.0 {
            if workers > 0 {
                let div = cost::parallel_divisor(workers as usize);
                rel_rows / div
            } else {
                rel_rows
            }
        } else {
            total_rows
        };
        (*cpath).path.rows = path_rows;
        (*cpath).path.startup_cost = total_startup;
        (*cpath).path.total_cost = total_cost;
        (*cpath).path.parallel_workers = 0;
        (*cpath).path.parallel_aware = false;
        (*cpath).path.parallel_safe = false;
        (*cpath).path.pathkeys = pathkeys;

        let mut private_list: *mut pg_sys::List = std::ptr::null_mut();
        for &oid in companion_oids {
            private_list = pg_sys::lappend_oid(private_list, oid);
        }
        (*cpath).custom_private = private_list;

        (*cpath).custom_paths = std::ptr::null_mut();
        (*cpath).custom_restrictinfo = std::ptr::null_mut();
        (*cpath).methods = &DELTAX_APPEND_PATH_METHODS.0;

        cpath
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
        pg_sys::pull_varattnos(final_clauses as *mut pg_sys::Node, varno, &mut needed_attrs);

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

        // Append Top-N info: [-2, effective_limit, sort_ascending_flag, multi_col_sort_flag, sort_col_attno, nulls_first]
        let (effective_limit, sort_ascending, multi_col_sort, sort_col_attno, nulls_first) =
            APPEND_TOPN_INFO.with(|cell| cell.replace((0, true, false, 0, false)));
        if effective_limit > 0 {
            private_list = pg_sys::lappend_int(private_list, -2);
            private_list = pg_sys::lappend_int(private_list, effective_limit as i32);
            private_list = pg_sys::lappend_int(private_list, if sort_ascending { 1 } else { 0 });
            private_list = pg_sys::lappend_int(private_list, if multi_col_sort { 1 } else { 0 });
            private_list = pg_sys::lappend_int(private_list, sort_col_attno);
            private_list = pg_sys::lappend_int(private_list, if nulls_first { 1 } else { 0 });
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

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;

    #[test]
    fn meta_agg_kind_roundtrip() {
        // Wire-format invariant: the i32 produced by `kind as i32` must
        // decode back to the same variant via `from_i32`. The executor
        // depends on this on the worker side when it deserialises the
        // plan_private wire.
        for &k in &[
            MetaAggKind::Min,
            MetaAggKind::Max,
            MetaAggKind::Sum,
            MetaAggKind::CountCol,
            MetaAggKind::CountStar,
        ] {
            assert_eq!(MetaAggKind::from_i32(k as i32), k);
        }
    }

    #[test]
    #[should_panic(expected = "invalid MetaAggKind encoding")]
    fn meta_agg_kind_rejects_out_of_range() {
        let _ = MetaAggKind::from_i32(99);
    }

    #[test]
    fn topn_sort_col_derived_sentinel_is_negative() {
        // The sentinel must be negative so the planner's `topn_sort_col >= 0`
        // check correctly distinguishes the direct-aggregate form (which
        // emits a non-negative output column index) from the derived
        // MIN/MAX-difference form. -3 specifically to avoid colliding with
        // the -1 list-sentinel used elsewhere in the wire format.
        const _: () = assert!(TOPN_SORT_COL_DERIVED < 0);
        const _: () = assert!(TOPN_SORT_COL_DERIVED != -1);
    }

    #[test]
    fn is_partial_eligible_var_type_accepts_numerics_and_temporals() {
        for oid in [
            pg_sys::INT2OID,
            pg_sys::INT4OID,
            pg_sys::INT8OID,
            pg_sys::FLOAT4OID,
            pg_sys::FLOAT8OID,
            pg_sys::TIMESTAMPOID,
            pg_sys::TIMESTAMPTZOID,
            pg_sys::DATEOID,
            pg_sys::BOOLOID,
        ] {
            assert!(is_partial_eligible_var_type(oid));
        }
    }

    #[test]
    fn is_partial_eligible_var_type_rejects_text_jsonb_numeric() {
        // The partial-aggregate path relies on `batch_quals_all_numeric` —
        // accepting text/jsonb here would let the planner route a query
        // the runtime drops the qual for, silently overcounting.
        for oid in [
            pg_sys::TEXTOID,
            pg_sys::VARCHAROID,
            pg_sys::BPCHAROID,
            pg_sys::JSONBOID,
            pg_sys::BYTEAOID,
            pg_sys::NUMERICOID,
        ] {
            assert!(!is_partial_eligible_var_type(oid));
        }
    }

    #[test]
    fn parallel_compact_aggs_ok_accepts_compact_set() {
        use super::super::exec::{AggExpr, AggType, OutputTransform};
        let specs = vec![
            AggSpec {
                agg_type: AggType::CountStar,
                col_idx: -1,
                result_type_oid: pg_sys::INT8OID,
                col_type_oid: pg_sys::InvalidOid,
                expr_kind: AggExpr::Column,
                const_offset: 0,
                output_transform: OutputTransform::None,
            },
            AggSpec {
                agg_type: AggType::Sum,
                col_idx: 0,
                result_type_oid: pg_sys::INT8OID,
                col_type_oid: pg_sys::INT4OID,
                expr_kind: AggExpr::Column,
                const_offset: 0,
                output_transform: OutputTransform::None,
            },
        ];
        assert!(parallel_compact_aggs_ok(&specs));
    }
}
