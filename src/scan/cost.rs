use pgrx::pg_sys;
use pgrx::prelude::*;

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
    let name = unsafe {
        let name_ptr = pg_sys::get_rel_name(companion_oid);
        if name_ptr.is_null() {
            return (0, 0);
        }
        std::ffi::CStr::from_ptr(name_ptr)
            .to_string_lossy()
            .into_owned()
    };

    let result = Spi::get_one_with_args::<i64>(
        "SELECT row_count FROM deltax_partition WHERE table_name = $1 AND is_compressed = true",
        &[name.as_str().into()],
    );

    match result {
        Ok(Some(row_count)) => {
            let segments = (row_count / 100_000).max(1);
            (row_count, segments)
        }
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

/// Get per-column ndistinct from the companion table for a companion OID.
/// Reads MAX(_ndistinct_col) for each _ndistinct_* column in the companion relation.
/// Returns a map from column name to ndistinct count, or empty if unavailable.
pub(super) fn get_column_ndistinct(companion_oid: pg_sys::Oid) -> std::collections::HashMap<String, i64> {
    let (nd_columns, companion_name) = unsafe {
        // Open companion table to inspect tuple descriptor for _ndistinct_* columns
        let rel = pg_sys::table_open(companion_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        let tupdesc = (*rel).rd_att;
        let natts = (*tupdesc).natts as usize;

        let mut cols: Vec<(String, String)> = Vec::new(); // (col_name, attr_name)
        for i in 0..natts {
            let att = &*super::exec::datum_utils::tupdesc_get_attr(tupdesc, i);
            if att.attisdropped {
                continue;
            }
            let attr_name = std::ffi::CStr::from_ptr(att.attname.data.as_ptr())
                .to_string_lossy()
                .into_owned();
            if let Some(col_name) = attr_name.strip_prefix("_ndistinct_") {
                cols.push((col_name.to_string(), attr_name));
            }
        }

        pg_sys::table_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

        if cols.is_empty() {
            return std::collections::HashMap::new();
        }

        let name_ptr = pg_sys::get_rel_name(companion_oid);
        if name_ptr.is_null() {
            return std::collections::HashMap::new();
        }
        let name = std::ffi::CStr::from_ptr(name_ptr)
            .to_string_lossy()
            .into_owned();

        (cols, name)
    };

    // Build query: SELECT MAX("_ndistinct_col1"), ... FROM companion
    let max_exprs: Vec<String> = nd_columns
        .iter()
        .map(|(_, attr)| format!("MAX(\"{}\")::int8", attr))
        .collect();
    let query = format!(
        "SELECT {} FROM \"_deltax_compressed\".\"{}\"",
        max_exprs.join(", "),
        companion_name
    );

    let mut result_map = std::collections::HashMap::new();
    let _ = Spi::connect(|client| {
        let result = client.select(&query, None, &[])?;
        if let Some(row) = result.into_iter().next() {
            for (i, (col_name, _)) in nd_columns.iter().enumerate() {
                if let Some(nd) = row.get_datum_by_ordinal(i + 1)
                    .unwrap()
                    .value::<i64>()
                    .unwrap()
                {
                    result_map.insert(col_name.clone(), nd);
                }
            }
        }
        Ok::<(), spi::Error>(())
    });

    result_map
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
