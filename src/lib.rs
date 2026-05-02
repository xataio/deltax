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
mod stats;
mod timeparse;
mod worker;

pg_module_magic!();

pub(crate) static MOCK_NOW: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

pub(crate) static PARALLEL_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(0);

pub(crate) static PARALLEL_REGEX: GucSetting<bool> = GucSetting::<bool>::new(true);

pub(crate) static BLOOM_FILTERS: GucSetting<bool> = GucSetting::<bool>::new(true);

pub(crate) static MAX_PARALLEL_WORKERS_PER_SCAN: GucSetting<i32> =
    GucSetting::<i32>::new(-1);

/// When true, the hook skips `DeltaXCount`/`DeltaXMinMax` fast paths for
/// queries with WHERE clauses. Used by tests and operators to force the
/// generic `DeltaXAgg` path for A/B correctness comparisons.
pub(crate) static DISABLE_META_AGG_FASTPATH: GucSetting<bool> =
    GucSetting::<bool>::new(false);

/// Controls how COPY ... FORMAT deltax_compress extracts JSON paths into
/// extra columnar columns alongside the original JSONB, and whether the
/// planner_hook walker rewrites upper-plan chain Exprs to read from
/// synthetic slot positions. Values:
///   `none`   — disable extraction AND walker rewrite (ignores any
///              json_extract config; queries fall through to slow path).
///   `fields` — extract the user-specified path list from
///              `deltax_enable_compression` AND enable the walker rewrite.
///              Requires Step 5's executor wiring for correct results.
///   `all`    — auto-discover all scalar leaves (not yet implemented).
///
/// Default is `none` until Step 5 (executor synthetic slot population) lands.
pub(crate) static JSON_EXTRACT_MODE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"none"));

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

/// Resolve the effective per-scan PG-worker cap for DeltaXAppend partial paths.
/// -1 = follow `max_parallel_workers_per_gather`, 0 = disabled, N = explicit cap.
pub(crate) fn get_scan_parallel_workers() -> i32 {
    let v = MAX_PARALLEL_WORKERS_PER_SCAN.get();
    if v < 0 {
        unsafe { pg_sys::max_parallel_workers_per_gather }
    } else {
        v
    }
}

pub(crate) fn get_parallel_regex() -> bool {
    PARALLEL_REGEX.get()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Wired up incrementally across the json-extract feature.
pub(crate) enum JsonExtractMode {
    None,
    Fields,
    All,
}

/// Resolve `pg_deltax.json_extract_mode` into a typed enum. Errors out for
/// `all` (not yet implemented) and any unknown value.
#[allow(dead_code)] // Wired up incrementally across the json-extract feature.
pub(crate) fn get_json_extract_mode() -> JsonExtractMode {
    let raw = JSON_EXTRACT_MODE.get();
    let s = raw.as_ref().and_then(|c| c.to_str().ok()).unwrap_or("fields");
    match s {
        "none" => JsonExtractMode::None,
        "fields" => JsonExtractMode::Fields,
        "all" => JsonExtractMode::All,
        other => pgrx::error!(
            "pg_deltax.json_extract_mode: unknown value {:?} (expected: none, fields, all)",
            other
        ),
    }
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
    column_valmap   JSONB,
    UNIQUE(schema_name, table_name)
);

ALTER TABLE deltax_partition ADD COLUMN IF NOT EXISTS column_valmap JSONB;
ALTER TABLE deltax_deltatable ADD COLUMN IF NOT EXISTS json_extract JSONB;
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
    GucRegistry::define_int_guc(
        c"pg_deltax.max_parallel_workers_per_scan",
        c"Max PG parallel workers for DeltaXAppend partial paths (-1=follow max_parallel_workers_per_gather, 0=disabled)",
        c"-1 (default) follows max_parallel_workers_per_gather. 0 disables the partial-path variant (scans run serially). 1..=64 caps the worker count explicitly.",
        &MAX_PARALLEL_WORKERS_PER_SCAN,
        -1,
        64,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_deltax.disable_meta_agg_fastpath",
        c"Disable DeltaXCount/DeltaXMinMax fast paths for queries with WHERE clauses",
        c"When ON, queries that could be answered from per-segment metadata fall through to the generic DeltaXAgg path instead. Used for correctness A/B testing.",
        &DISABLE_META_AGG_FASTPATH,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_deltax.json_extract_mode",
        c"How COPY extracts JSON paths into extra columnar columns: none, fields, or all (all not yet implemented)",
        c"none disables extraction; fields uses the path list configured in deltax_enable_compression; all auto-discovers scalar leaves (not yet implemented).",
        &JSON_EXTRACT_MODE,
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
