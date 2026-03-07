/// Custom scan node for transparent querying of compressed partitions.
///
/// Installs a `set_rel_pathlist_hook` that detects compressed partitions
/// and injects a `SeaTurtleDecompress` custom path/scan node.
mod cost;
pub(crate) mod exec;
mod explain;
mod hook;
mod path;

use std::sync::atomic::{AtomicPtr, Ordering};
use pgrx::pg_sys;

/// Previous hook to chain (set_rel_pathlist_hook).
static PREV_HOOK: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

/// Previous hook to chain (create_upper_paths_hook).
static PREV_UPPER_HOOK: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

/// Previous hook to chain (ExecutorStart_hook).
static PREV_EXECUTOR_START_HOOK: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

/// Custom scan method name (NUL-terminated, static lifetime).
const CUSTOM_NAME: &std::ffi::CStr = c"SeaTurtleDecompress";

/// SeaTurtleAppend custom scan method name.
const SEATURTLE_APPEND_NAME: &std::ffi::CStr = c"SeaTurtleAppend";

/// SeaTurtleCount custom scan method name (COUNT(*) pushdown).
const SEATURTLE_COUNT_NAME: &std::ffi::CStr = c"SeaTurtleCount";

/// SeaTurtleMinMax custom scan method name (MIN/MAX pushdown).
const SEATURTLE_MINMAX_NAME: &std::ffi::CStr = c"SeaTurtleMinMax";

/// SeaTurtleAgg custom scan method name (aggregate pushdown).
const SEATURTLE_AGG_NAME: &std::ffi::CStr = c"SeaTurtleAgg";

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
        pg_sys::set_rel_pathlist_hook = Some(hook::seaturtle_set_rel_pathlist);

        // Register create_upper_paths_hook for COUNT(*) pushdown
        let prev_upper = pg_sys::create_upper_paths_hook;
        if let Some(prev_fn) = prev_upper {
            PREV_UPPER_HOOK.store(prev_fn as *mut (), Ordering::SeqCst);
        }
        pg_sys::create_upper_paths_hook = Some(hook::seaturtle_create_upper_paths);
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
        pg_sys::ExecutorStart_hook = Some(hook::seaturtle_executor_start);
    }
}
