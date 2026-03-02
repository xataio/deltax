use pgrx::pg_sys;
use pgrx::pg_guard;
use std::collections::HashMap;
use std::sync::atomic::Ordering;

use super::PREV_HOOK;
use super::PREV_UPPER_HOOK;
use super::path;
use super::cost;

thread_local! {
    /// Cache of partition OID → companion table OID (or InvalidOid if not compressed).
    static COMPRESSED_CACHE: std::cell::RefCell<HashMap<pg_sys::Oid, pg_sys::Oid>> =
        std::cell::RefCell::new(HashMap::new());
}

pub fn invalidate_compressed_cache() {
    COMPRESSED_CACHE.with(|cache| cache.borrow_mut().clear());
}

/// The planner hook. Called for each relation during path generation.
#[pg_guard]
pub unsafe extern "C-unwind" fn cocoon_set_rel_pathlist(
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

        // Check if this is the parent of a partitioned table (for CocoonAppend)
        if (*rel).reloptkind == pg_sys::RelOptKind::RELOPT_BASEREL && (*rte).inh {
            if let Some(companion_oids) = collect_compressed_children(root, rti) {
                path::add_cocoon_append_path(root, rel, &companion_oids);
                return;
            }
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

        // Add the custom decompress path
        path::add_decompress_path(root, rel, companion_oid);
    }
}

/// The create_upper_paths hook. Detects simple COUNT(*) over cocoon scans
/// and injects a CocoonCount custom path that returns the pre-computed count
/// directly from segment metadata, bypassing decompression entirely.
#[pg_guard]
pub unsafe extern "C-unwind" fn cocoon_create_upper_paths(
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

        // No GROUP BY
        if !(*parse).groupClause.is_null() {
            return;
        }

        // No HAVING
        if !(*parse).havingQual.is_null() {
            return;
        }

        // No WHERE clause (check parse tree jointree quals)
        let jointree = (*parse).jointree;
        if !jointree.is_null() && !(*jointree).quals.is_null() {
            return;
        }

        // Check target list: must be a single non-junk COUNT(*) aggregate
        let tlist = (*parse).targetList;
        if tlist.is_null() {
            return;
        }

        let nentries = (*tlist).length;
        let mut count_star_found = false;
        let mut non_junk_count = 0;

        for i in 0..nentries {
            let te = pg_sys::list_nth(tlist, i) as *const pg_sys::TargetEntry;
            if te.is_null() {
                continue;
            }
            if (*te).resjunk {
                continue;
            }
            non_junk_count += 1;
            if non_junk_count > 1 {
                return;
            }

            let expr = (*te).expr as *const pg_sys::Node;
            if expr.is_null() {
                return;
            }
            if (*expr).type_ != pg_sys::NodeTag::T_Aggref {
                return;
            }

            let aggref = expr as *const pg_sys::Aggref;
            if !(*aggref).aggstar {
                return;
            }

            count_star_found = true;
        }

        if !count_star_found || non_junk_count != 1 {
            return;
        }

        // Extract companion OIDs from the cheapest input path.
        // Handles CocoonDecompress/CocoonAppend CustomPaths directly,
        // and also AppendPaths whose subpaths are CocoonDecompress.
        let cheapest = (*input_rel).cheapest_total_path;
        if cheapest.is_null() {
            return;
        }

        let companion_oids = match extract_companion_oids(root, cheapest) {
            Some(oids) if !oids.is_empty() => oids,
            _ => return,
        };

        path::add_count_star_path(root, output_rel, &companion_oids);
    }
}

/// Extract companion OIDs from a planner path for COUNT(*) pushdown.
///
/// Handles:
/// - CocoonDecompress/CocoonAppend CustomPath: extract OIDs from custom_private
/// - AppendPath: walk subpaths, extract OIDs from CocoonDecompress CustomPaths
///
/// Returns None if the path doesn't contain cocoon scan nodes, or if there
/// are non-cocoon subpaths with actual data (uncompressed partitions).
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
                    // Non-cocoon subpath with actual data — can't push down
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
/// Looks up the raw `relpages` from `pg_class` via syscache, bypassing PG's
/// inflated estimates in `RelOptInfo.pages` (which PG sets to 10 for
/// never-analyzed tables even when physically empty).
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
        // Check raw relpages from pg_class (0 for truly empty/truncated tables)
        cost::get_relpages(rel_oid) > 0
    }
}

/// Extract companion OIDs from a CocoonDecompress or CocoonAppend CustomPath.
unsafe fn extract_oids_from_custom_path(
    cpath: *const pg_sys::CustomPath,
) -> Option<Vec<pg_sys::Oid>> {
    unsafe {
        let methods = (*cpath).methods;
        if methods.is_null() {
            return None;
        }
        let name = std::ffi::CStr::from_ptr((*methods).CustomName);
        if name != super::COCOON_APPEND_NAME && name != super::CUSTOM_NAME {
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
///   returns None (cannot use CocoonAppend).
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
                    // Uncompressed partition with data — cannot use CocoonAppend
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
/// by looking for a companion table in _cocoon_compressed schema.
unsafe fn check_compressed_partition(rel_oid: pg_sys::Oid) -> pg_sys::Oid {
    unsafe {
        // Get the relation name
        let name_ptr = pg_sys::get_rel_name(rel_oid);
        if name_ptr.is_null() {
            return pg_sys::InvalidOid;
        }
        let rel_name = std::ffi::CStr::from_ptr(name_ptr)
            .to_string_lossy()
            .into_owned();

        // Look up _cocoon_compressed schema OID
        let schema_cstr = c"_cocoon_compressed";
        let compressed_ns_oid = pg_sys::get_namespace_oid(schema_cstr.as_ptr(), true);
        if compressed_ns_oid == pg_sys::InvalidOid {
            return pg_sys::InvalidOid;
        }

        // Skip tables already in the _cocoon_compressed schema to avoid recursion
        let rel_ns_oid = pg_sys::get_rel_namespace(rel_oid);
        if rel_ns_oid == compressed_ns_oid {
            return pg_sys::InvalidOid;
        }

        // Check if _cocoon_compressed.<rel_name> exists
        let companion_cname = std::ffi::CString::new(rel_name).unwrap();
        pg_sys::get_relname_relid(companion_cname.as_ptr(), compressed_ns_oid)
    }
}
