use pgrx::pg_sys;
use pgrx::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;

thread_local! {
    /// Cache of companion_oid → (row_count, segment_count) from deltax_partition.
    /// Only populated on successful lookups; misses are not cached because
    /// companion lookups can race with partition creation.
    static PARTITION_STATS_CACHE: RefCell<HashMap<pg_sys::Oid, (i64, i64)>> =
        RefCell::new(HashMap::new());

    /// Cache of companion_oid → per-column ndistinct counts from companion table.
    /// An empty map is a valid cached value (stable schema shape with no
    /// `_ndistinct_*` columns).
    static NDISTINCT_CACHE: RefCell<HashMap<pg_sys::Oid, HashMap<String, i64>>> =
        RefCell::new(HashMap::new());
}

/// Clear all cost-related caches. Called from `hook::invalidate_compressed_cache`.
pub(super) fn invalidate_caches() {
    PARTITION_STATS_CACHE.with(|cache| cache.borrow_mut().clear());
    NDISTINCT_CACHE.with(|cache| cache.borrow_mut().clear());
}

/// Estimate the cost and row count for scanning a compressed partition.
/// Returns (startup_cost, total_cost, estimated_rows).
pub unsafe fn estimate_cost(companion_oid: pg_sys::Oid) -> (f64, f64, f64) {
    let (total_rows, segment_count) = get_partition_stats(companion_oid);

    let rows = if total_rows > 0 {
        total_rows as f64
    } else {
        let rel_tuples = unsafe { get_reltuples(companion_oid) };
        let segments = if rel_tuples > 0.0 { rel_tuples } else { 1.0 };
        segments * 10000.0
    };

    let startup = 10.0;
    let segs = if segment_count > 0 {
        segment_count as f64
    } else {
        (rows / 10000.0).max(1.0)
    };
    let per_segment = 100.0;
    let per_row = 0.1;
    let total = startup + segs * per_segment + rows * per_row;

    (startup, total, rows)
}

/// Get partition stats from deltax_partition catalog.
fn get_partition_stats(companion_oid: pg_sys::Oid) -> (i64, i64) {
    if let Some(cached) =
        PARTITION_STATS_CACHE.with(|cache| cache.borrow().get(&companion_oid).copied())
    {
        return cached;
    }

    let name = unsafe {
        let name_ptr = pg_sys::get_rel_name(companion_oid);
        if name_ptr.is_null() {
            return (0, 0);
        }
        std::ffi::CStr::from_ptr(name_ptr)
            .to_string_lossy()
            .into_owned()
    };
    // Strip _meta suffix to get the partition name for catalog lookup
    let partition_name = name.strip_suffix("_meta").unwrap_or(&name);

    let result = Spi::get_one_with_args::<i64>(
        "SELECT row_count FROM deltax_partition WHERE table_name = $1 AND is_compressed = true",
        &[partition_name.into()],
    );

    match result {
        Ok(Some(row_count)) => {
            let segments = (row_count / 100_000).max(1);
            let stats = (row_count, segments);
            PARTITION_STATS_CACHE
                .with(|cache| cache.borrow_mut().insert(companion_oid, stats));
            stats
        }
        // Do not cache misses: companion lookups can race with partition creation.
        _ => (0, 0),
    }
}

/// Get relpages from pg_class for a relation OID.
#[allow(dead_code)]
pub(super) unsafe fn get_relpages(rel_oid: pg_sys::Oid) -> i32 {
    unsafe {
        let tuple = pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::RELOID as i32,
            pg_sys::ObjectIdGetDatum(rel_oid),
        );
        if tuple.is_null() {
            return 0;
        }
        let rel_form = pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_class;
        let pages = (*rel_form).relpages;
        pg_sys::ReleaseSysCache(tuple);
        pages
    }
}

/// Get the uncompressed row count for a companion OID from deltax_partition catalog.
/// Returns Some(row_count) if positive, None otherwise.
pub(super) fn get_row_count(companion_oid: pg_sys::Oid) -> Option<i64> {
    let (row_count, _) = get_partition_stats(companion_oid);
    if row_count > 0 { Some(row_count) } else { None }
}

/// Get per-column ndistinct for a companion OID from the catalog column
/// `deltax_partition.column_ndistinct` (populated at compression time).
/// Returns a map from column name to max-across-segments ndistinct count,
/// or an empty map if the partition has no stored ndistinct info.
///
/// This used to scan the whole meta table via `MAX(_ndistinct_*)`, which
/// was cheap warm but forced ~9 MB of cold reads on the meta table during
/// planning on every fresh backend. Now the info is persisted once at
/// compression time and read via a small catalog lookup.
pub(super) fn get_column_ndistinct(companion_oid: pg_sys::Oid) -> std::collections::HashMap<String, i64> {
    if let Some(cached) =
        NDISTINCT_CACHE.with(|cache| cache.borrow().get(&companion_oid).cloned())
    {
        return cached;
    }

    let companion_name = unsafe {
        let name_ptr = pg_sys::get_rel_name(companion_oid);
        if name_ptr.is_null() {
            return std::collections::HashMap::new();
        }
        std::ffi::CStr::from_ptr(name_ptr).to_string_lossy().into_owned()
    };
    // Strip _meta suffix to get the partition name for catalog lookup
    let partition_name = companion_name.strip_suffix("_meta").unwrap_or(&companion_name);

    let mut result_map: std::collections::HashMap<String, i64> = std::collections::HashMap::new();

    // Retrieve the JSONB column as text and parse manually. This avoids
    // pulling in a JSON dependency just for a trivial `{string: int}` map.
    let json_text = Spi::get_one_with_args::<String>(
        "SELECT column_ndistinct::text FROM deltax_partition
         WHERE table_name = $1 AND is_compressed = true",
        &[partition_name.into()],
    );

    if let Ok(Some(text)) = json_text {
        parse_ndistinct_json(&text, &mut result_map);
    }

    NDISTINCT_CACHE
        .with(|cache| cache.borrow_mut().insert(companion_oid, result_map.clone()));
    result_map
}

/// Parse a `{"col": int, ...}` JSON object (as emitted by
/// `catalog::update_partition_column_ndistinct`) into the result map.
/// Trivial hand-rolled parser — values are always integers, keys are
/// always column names with limited escaping (backslash and quote).
fn parse_ndistinct_json(text: &str, out: &mut std::collections::HashMap<String, i64>) {
    let bytes = text.as_bytes();
    let mut i = 0;
    // Skip leading whitespace and opening brace
    while i < bytes.len() && bytes[i].is_ascii_whitespace() { i += 1; }
    if i >= bytes.len() || bytes[i] != b'{' { return; }
    i += 1;

    loop {
        while i < bytes.len() && (bytes[i].is_ascii_whitespace() || bytes[i] == b',') {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] == b'}' { return; }
        if bytes[i] != b'"' { return; }
        i += 1;

        // Parse key (with \" and \\ escapes).
        let mut key = String::new();
        while i < bytes.len() && bytes[i] != b'"' {
            if bytes[i] == b'\\' && i + 1 < bytes.len() {
                key.push(bytes[i + 1] as char);
                i += 2;
            } else {
                key.push(bytes[i] as char);
                i += 1;
            }
        }
        if i >= bytes.len() { return; }
        i += 1; // closing quote

        while i < bytes.len() && (bytes[i].is_ascii_whitespace() || bytes[i] == b':') {
            i += 1;
        }

        // Parse integer value (may be negative in principle).
        let start = i;
        if i < bytes.len() && (bytes[i] == b'-' || bytes[i] == b'+') { i += 1; }
        while i < bytes.len() && bytes[i].is_ascii_digit() { i += 1; }
        if let Ok(s) = std::str::from_utf8(&bytes[start..i])
            && let Ok(v) = s.parse::<i64>() {
            out.insert(key, v);
        }
    }
}

/// Get reltuples from pg_class for a relation OID.
pub(super) unsafe fn get_reltuples(rel_oid: pg_sys::Oid) -> f64 {
    unsafe {
        let tuple = pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::RELOID as i32,
            pg_sys::ObjectIdGetDatum(rel_oid),
        );
        if tuple.is_null() {
            return 0.0;
        }
        let rel_form = pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_class;
        let tuples = (*rel_form).reltuples;
        pg_sys::ReleaseSysCache(tuple);
        tuples as f64
    }
}
