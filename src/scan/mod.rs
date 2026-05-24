/// Custom scan node for transparent querying of compressed partitions.
///
/// Installs a `set_rel_pathlist_hook` that detects compressed partitions
/// and injects a `DeltaXDecompress` custom path/scan node.
mod cost;
pub(crate) mod exec;
mod explain;
mod hook;
pub(crate) mod json_extract;
mod path;

use pgrx::pg_sys;
use std::sync::atomic::{AtomicPtr, Ordering};

/// Previous hook to chain (set_rel_pathlist_hook).
static PREV_HOOK: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

/// Previous hook to chain (create_upper_paths_hook).
static PREV_UPPER_HOOK: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

/// Previous hook to chain (ExecutorStart_hook).
static PREV_EXECUTOR_START_HOOK: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

/// Previous hook to chain (get_relation_info_hook).
static PREV_GET_RELATION_INFO_HOOK: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

/// Previous hook to chain (planner_hook). Used by the json_extract feature
/// to walk the final plan tree post-`set_plan_references` and substitute
/// JSONB-extract chains with `Var(OUTER_VAR, attno)` referring to a
/// DeltaXDecompress's pre-computed synthetic columns.
static PREV_PLANNER_HOOK: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

/// Custom scan method name (NUL-terminated, static lifetime).
const CUSTOM_NAME: &std::ffi::CStr = c"DeltaXDecompress";

/// DeltaXAppend custom scan method name.
const DELTAX_APPEND_NAME: &std::ffi::CStr = c"DeltaXAppend";

/// DeltaXCount custom scan method name (COUNT(*) pushdown).
const DELTAX_COUNT_NAME: &std::ffi::CStr = c"DeltaXCount";

/// DeltaXMinMax custom scan method name (MIN/MAX pushdown).
const DELTAX_MINMAX_NAME: &std::ffi::CStr = c"DeltaXMinMax";

/// DeltaXAgg custom scan method name (aggregate pushdown).
const DELTAX_AGG_NAME: &std::ffi::CStr = c"DeltaXAgg";

/// Wrapper to make pg_sys structs with raw pointers usable in statics.
/// Safety: the static structs only contain function pointers and const string pointers
/// that are valid for the entire backend lifetime.
pub(crate) struct SyncStatic<T>(pub T);
unsafe impl<T> Sync for SyncStatic<T> {}
unsafe impl<T> Send for SyncStatic<T> {}

pub fn invalidate_compressed_cache() {
    hook::invalidate_compressed_cache();
}

pub(crate) fn set_dml_bypass(bypass: bool) {
    hook::set_dml_bypass(bypass);
}

/// Look up the companion OID for a compressed partition's relation OID,
/// or `InvalidOid` if it's not a pg_deltax-managed compressed table.
///
/// # Safety
/// Must be called from a backend with a valid transaction (uses SearchSysCache).
pub(crate) unsafe fn check_compressed_partition(rel_oid: pg_sys::Oid) -> pg_sys::Oid {
    unsafe { hook::check_compressed_partition(rel_oid) }
}

/// Register the planner hook at extension load time.
///
/// # Safety
/// Must be called from `_PG_init()`. Replaces the global planner hook pointer.
pub unsafe fn register_hook() {
    unsafe {
        let prev = pg_sys::set_rel_pathlist_hook;
        if let Some(prev_fn) = prev {
            PREV_HOOK.store(prev_fn as *mut (), Ordering::SeqCst);
        }
        pg_sys::set_rel_pathlist_hook = Some(hook::deltax_set_rel_pathlist);

        // Register create_upper_paths_hook for COUNT(*) pushdown
        let prev_upper = pg_sys::create_upper_paths_hook;
        if let Some(prev_fn) = prev_upper {
            PREV_UPPER_HOOK.store(prev_fn as *mut (), Ordering::SeqCst);
        }
        pg_sys::create_upper_paths_hook = Some(hook::deltax_create_upper_paths);

        // Register get_relation_info_hook so we can patch `rel->tuples`
        // and `rel->pages` for compressed partitions. PG's default
        // `estimate_rel_size` multiplies reltuples by curpages/relpages,
        // and curpages on a compressed partition is 0 (the heap is
        // truncated), so reltuples=50K collapses to tuples=0 and every
        // restrictinfo selectivity multiplies out to 0/1. Setting
        // rel->tuples here (before set_baserel_size_estimates runs)
        // feeds the correct baseline into the planner.
        let prev_gri = pg_sys::get_relation_info_hook;
        if let Some(prev_fn) = prev_gri {
            PREV_GET_RELATION_INFO_HOOK.store(prev_fn as *mut (), Ordering::SeqCst);
        }
        pg_sys::get_relation_info_hook = Some(hook::deltax_get_relation_info);

        // Register planner_hook so we can post-process the final plan tree
        // and rewrite JSONB-extract chains in upper plans (json_extract feature).
        let prev_planner = pg_sys::planner_hook;
        if let Some(prev_fn) = prev_planner {
            PREV_PLANNER_HOOK.store(prev_fn as *mut (), Ordering::SeqCst);
        }
        pg_sys::planner_hook = Some(hook::deltax_planner);

        // Register CustomScanMethods by name so parallel workers can
        // deserialize custom scan nodes from DSM.
        path::register_custom_scan_methods();
    }
}

/// Register the ExecutorStart hook to block DML on compressed partitions.
///
/// # Safety
/// Must be called from `_PG_init()`. Replaces the global ExecutorStart hook pointer.
pub unsafe fn register_executor_start_hook() {
    unsafe {
        let prev = pg_sys::ExecutorStart_hook;
        if let Some(prev_fn) = prev {
            PREV_EXECUTOR_START_HOOK.store(prev_fn as *mut (), Ordering::SeqCst);
        }
        pg_sys::ExecutorStart_hook = Some(hook::deltax_executor_start);
    }
}
