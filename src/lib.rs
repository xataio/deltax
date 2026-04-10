use std::ffi::CString;

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;

mod bloom;
mod catalog;
mod compress;
mod compression;
mod copy;
mod copyparquet;
mod copyparse;
mod functions;
mod partition;
mod scan;
mod timeparse;
mod worker;

pg_module_magic!();

pub(crate) static MOCK_NOW: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

pub(crate) static PARALLEL_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(0);

pub(crate) static PARALLEL_REGEX: GucSetting<bool> = GucSetting::<bool>::new(true);

pub(crate) static BLOOM_FILTERS: GucSetting<bool> = GucSetting::<bool>::new(true);

/// Resolve the effective number of parallel workers.
/// 0 = auto (num_cpus, capped at 16), 1 = single-threaded, 2..=64 = explicit.
pub(crate) fn get_parallel_workers() -> usize {
    let v = PARALLEL_WORKERS.get();
    if v <= 0 {
        num_cpus::get().min(16)
    } else {
        (v as usize).min(64)
    }
}

pub(crate) fn get_parallel_regex() -> bool {
    PARALLEL_REGEX.get()
}

extension_sql!(
    r#"
CREATE SCHEMA IF NOT EXISTS _deltax_compressed;

CREATE TABLE IF NOT EXISTS deltax_deltatable (
    id              SERIAL PRIMARY KEY,
    schema_name     TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    time_column     TEXT NOT NULL,
    partition_interval INTERVAL NOT NULL,
    compress_after  INTERVAL,
    drop_after      INTERVAL,
    segment_by      TEXT[],
    order_by        TEXT[],
    segment_size    INT DEFAULT 30000,
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS deltax_partition (
    id              SERIAL PRIMARY KEY,
    deltatable_id   INT REFERENCES deltax_deltatable(id) ON DELETE CASCADE,
    schema_name     TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    range_start     TIMESTAMPTZ NOT NULL,
    range_end       TIMESTAMPTZ NOT NULL,
    is_compressed   BOOLEAN DEFAULT false,
    compressed_size BIGINT,
    raw_size        BIGINT,
    row_count       BIGINT,
    compressed_at   TIMESTAMPTZ,
    column_ndistinct JSONB,
    UNIQUE(schema_name, table_name)
);
"#,
    name = "create_catalog_tables",
);

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    GucRegistry::define_string_guc(
        c"pg_deltax.mock_now",
        c"Override current time for testing (timestamptz literal, empty = use real time)",
        c"Override current time for testing (timestamptz literal, empty = use real time)",
        &MOCK_NOW,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_deltax.parallel_workers",
        c"Number of worker threads for parallel aggregation (0=auto, 1=off)",
        c"Number of worker threads for parallel aggregation (0=auto, 1=off)",
        &PARALLEL_WORKERS,
        0,
        64,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_deltax.parallel_regex",
        c"Use Rust regex for parallel REGEXP_REPLACE in GROUP BY",
        c"When ON, compatible regex patterns use the Rust regex crate for thread-safe parallel execution",
        &PARALLEL_REGEX,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_deltax.bloom_filters",
        c"Build per-segment bloom filters during compression for equality predicate pushdown",
        c"When ON, bloom filters are built during compression and used to skip segments during scans. Size is proportional to column cardinality (~2-5% storage overhead).",
        &BLOOM_FILTERS,
        GucContext::Userset,
        GucFlags::default(),
    );
    worker::register_bgworker();
    unsafe { scan::register_hook(); }
    unsafe { scan::register_executor_start_hook(); }
    unsafe { copy::register_process_utility_hook(); }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_extension_loads() {
        // Extension is loaded if this test runs at all
        let result = Spi::get_one::<i32>("SELECT 1").expect("query failed");
        assert_eq!(result, Some(1));
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["shared_preload_libraries = 'pg_deltax'"]
    }
}
