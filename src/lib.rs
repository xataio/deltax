use std::ffi::CString;

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;

// Pull the weak-stub static library into the test binary on Linux. The cdylib
// (built via `cargo build --lib`) skips this dev-dependency entirely, so
// Postgres keeps providing the real backend symbols when it loads the .so.
#[cfg(test)]
extern crate pg_deltax_test_stubs as _;

mod blob_cache;
mod bloom;
mod catalog;
mod compress;
mod compression;
mod copy;
mod copyparquet;
mod copyparse;
mod ddl;
mod functions;
mod partition;
mod scan;
mod stats;
mod timeparse;
mod worker;

pg_module_magic!();

pub(crate) static MOCK_NOW: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

/// Comma-separated list of databases the maintenance background worker(s)
/// connect to. SPI binds a background worker to exactly one database for its
/// lifetime, so one static worker is registered per listed database and each
/// services only deltatables registered there. Default "postgres" preserves
/// upstream behaviour (a single worker on the postgres database).
pub(crate) static TARGET_DATABASE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"postgres"));

pub(crate) static PARALLEL_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(0);

pub(crate) static PARALLEL_REGEX: GucSetting<bool> = GucSetting::<bool>::new(true);

pub(crate) static BLOOM_FILTERS: GucSetting<bool> = GucSetting::<bool>::new(true);
/// Testing-only: force natively-coded fallback types (time, uuid, bytea,
/// inet/cidr, numeric) back onto the legacy ::text codecs at compression
/// time, to produce legacy-format blobs for mixed-generation read tests.
pub(crate) static FORCE_TEXT_FALLBACK: GucSetting<bool> = GucSetting::<bool>::new(false);

pub(crate) static MAX_PARALLEL_WORKERS_PER_SCAN: GucSetting<i32> = GucSetting::<i32>::new(-1);

/// When true, the hook skips `DeltaXCount`/`DeltaXMinMax` fast paths for
/// queries with WHERE clauses. Used by tests and operators to force the
/// generic `DeltaXAgg` path for A/B correctness comparisons.
pub(crate) static DISABLE_META_AGG_FASTPATH: GucSetting<bool> = GucSetting::<bool>::new(false);

/// When true (default), SELECT planning collapses a deltax partitioned
/// parent whose data lives entirely in compressed companions to a single
/// un-expanded rel (`rte->inh = false`): PostgreSQL skips building per-child
/// RelOptInfos/paths for the deliberately-empty partition heaps and the
/// set_rel_pathlist hook installs DeltaXAppend directly. standard_planner
/// drops from ~5-8ms to ~1.4ms on a 127-partition table; the eligibility
/// walk costs ~2-4ms on a cold backend (per-child syscache warming — a
/// shared-memory verdict cache is the designed follow-up) and ~0.2ms warm.
/// Tables with uncompressed data (hot partition, non-empty default) are
/// never flattened — they plan through the regular expansion exactly as
/// before.
pub(crate) static FLATTEN_PARTITIONS: GucSetting<bool> = GucSetting::<bool>::new(true);

/// When true, `add_agg_partial_path` returns early and the planner only
/// sees the complete CustomScan DeltaXAgg path. Escape hatch for the
/// partial+Gather+FinalAgg model (PARALLEL_AGG.md "C.2 activation
/// followup"); useful for bisecting suspected regressions on the
/// partial path or comparing the two paths' end-to-end timings on the
/// same query. The complete path's internal-rayon parallelism still
/// runs — this only disables the PG-level partial-path activation.
pub(crate) static DISABLE_PARALLEL_AGG: GucSetting<bool> = GucSetting::<bool>::new(false);

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

/// Size of the process-shared blob cache, in MiB. `0` disables the cache.
/// Default `-1` means auto: 25% of physical RAM, clamped to
/// [256, 4096] MiB. Explicit positive values override; `0` disables
/// the cache entirely. See `dev/docs/BLOB_CACHE.md#sizing`.
pub(crate) static BLOB_CACHE_MB: GucSetting<i32> = GucSetting::<i32>::new(-1);

/// Number of shards (power of two) in the blob cache. More shards reduce
/// LWLock contention; fewer save shmem overhead. Default `64` is a good
/// fit for typical OLAP workloads. Restart required to change.
pub(crate) static BLOB_CACHE_SHARDS: GucSetting<i32> = GucSetting::<i32>::new(64);

/// When ON, internal columnar-blob companion tables (`_blobs`, `_blooms`,
/// `_text_lengths`, `_valbitmap`) are declared with `BYTEA COMPRESSION lz4`.
/// The actual columnar compression happens in Rust regardless; this flag
/// only controls the Postgres TOAST-pass attribute on those BYTEA columns.
///
/// Defaults to ON. If the running PostgreSQL was not built with
/// `--with-lz4`, the DDL is emitted without the `COMPRESSION lz4` clause
/// (so `CREATE TABLE` doesn't fail) and a one-shot WARNING is raised on
/// the first `deltax_enable_compression` call per backend. Users can
/// also set this to OFF explicitly to suppress the lz4 attribute on
/// lz4-capable builds (e.g., for testing the fallback path).
pub(crate) static USE_LZ4: GucSetting<bool> = GucSetting::<bool>::new(true);

/// Emit per-phase DeltaX planner timing as NOTICE at the end of planning.
/// Intended for ad-hoc profiling; default off to avoid benchmark noise.
pub(crate) static PROFILE_PLANNING: GucSetting<bool> = GucSetting::<bool>::new(false);

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
    let s = raw
        .as_ref()
        .and_then(|c| c.to_str().ok())
        .unwrap_or("fields");
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

CREATE TABLE IF NOT EXISTS deltax.deltax_deltatable (
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

CREATE TABLE IF NOT EXISTS deltax.deltax_partition (
    id              SERIAL PRIMARY KEY,
    deltatable_id   INT REFERENCES deltax.deltax_deltatable(id) ON DELETE CASCADE,
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
    column_minmax   JSONB,
    column_valcounts JSONB,
    column_mcv      JSONB,
    UNIQUE(schema_name, table_name)
);

ALTER TABLE deltax.deltax_partition ADD COLUMN IF NOT EXISTS column_valmap JSONB;
ALTER TABLE deltax.deltax_partition ADD COLUMN IF NOT EXISTS column_minmax JSONB;
ALTER TABLE deltax.deltax_partition ADD COLUMN IF NOT EXISTS column_hll JSONB;
ALTER TABLE deltax.deltax_partition ADD COLUMN IF NOT EXISTS column_valcounts JSONB;
ALTER TABLE deltax.deltax_partition ADD COLUMN IF NOT EXISTS column_mcv JSONB;
ALTER TABLE deltax.deltax_deltatable ADD COLUMN IF NOT EXISTS json_extract JSONB;
ALTER TABLE deltax.deltax_deltatable ADD COLUMN IF NOT EXISTS json_extract_added_at TIMESTAMPTZ;
ALTER TABLE deltax.deltax_partition ADD COLUMN IF NOT EXISTS compressed_columns JSONB;

CREATE OR REPLACE FUNCTION deltax.deltax_reject_compressed_partition_dml()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'cannot % compressed partition "%.%", decompress it first',
        TG_OP,
        TG_TABLE_SCHEMA,
        TG_TABLE_NAME
        USING ERRCODE = 'object_not_in_prerequisite_state';
END;
$$;
"#,
    name = "create_catalog_tables",
);

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // NOTE: `PGC_POSTMASTER` GUCs (target_database, blob_cache_mb,
    // blob_cache_shards) are defined ONLY in the shared-preload branch below —
    // PostgreSQL FATALs ("cannot create PGC_POSTMASTER variables after startup")
    // if they are defined in a backend loaded via session_preload / LOAD / fmgr.
    // Everything here is PGC_USERSET / PGC_SUSET, which is safe in any load mode.
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
        c"pg_deltax.force_text_fallback",
        c"TESTING: compress natively-coded fallback types via the legacy text codecs",
        c"When ON, time/uuid/bytea/inet/cidr/numeric columns compress through the legacy ::text rendering codecs instead of their native binary codecs. Produces legacy-format blobs for mixed-generation read testing; never needed in production.",
        &FORCE_TEXT_FALLBACK,
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
    GucRegistry::define_bool_guc(
        c"pg_deltax.flatten_partitions",
        c"Plan all-compressed deltax tables as a single un-expanded relation",
        c"When ON (default), SELECT planning skips PostgreSQL's per-partition expansion for deltax parents whose data is entirely in compressed companions and installs DeltaXAppend directly on the parent rel. Tables with uncompressed data are never flattened and plan through the regular expansion.",
        &FLATTEN_PARTITIONS,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_deltax.disable_parallel_agg",
        c"Disable the partial+Gather+FinalAgg path for DeltaXAgg",
        c"When ON, add_agg_partial_path is a no-op and the planner only sees the complete CustomScan DeltaXAgg. Escape hatch for bisecting suspected regressions on the partial path; the complete path's internal-rayon parallelism still runs.",
        &DISABLE_PARALLEL_AGG,
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
    GucRegistry::define_bool_guc(
        c"pg_deltax.use_lz4",
        c"Declare internal columnar BYTEA companion columns with COMPRESSION lz4",
        c"Default ON. Set OFF (or run on a PG built without --with-lz4) and the companion-table DDL is emitted without the lz4 attribute; the actual columnar compression in Rust is unaffected. On an lz4-less build with this ON, deltax_enable_compression raises a one-shot WARNING and the DDL falls back automatically.",
        &USE_LZ4,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_deltax.profile_planning",
        c"Emit per-phase DeltaX planner timing notices",
        c"When ON, each planned query emits a NOTICE with cumulative time spent in DeltaX planner hooks and custom path planning callbacks. Intended for ad-hoc profiling only.",
        &PROFILE_PLANNING,
        GucContext::Userset,
        GucFlags::default(),
    );
    // Query hooks are per-backend function pointers — install them in EVERY
    // load mode (shared_preload, session_preload, LOAD, on-demand fmgr) so query
    // correctness is identical regardless of how the library was loaded. This
    // mirrors auto_explain, whose _PG_init installs its executor hooks
    // unconditionally.
    unsafe {
        scan::register_hook();
    }
    unsafe {
        scan::register_executor_start_hook();
    }
    unsafe {
        copy::register_process_utility_hook();
    }

    // Postmaster-only setup. The `PGC_POSTMASTER` GUCs, the static maintenance
    // worker (`RegisterBackgroundWorker`), and the shared blob cache
    // (`RequestAddinShmemSpace`) all require the postmaster to be processing
    // `shared_preload_libraries`; `process_shared_preload_libraries_in_progress`
    // is true exactly then. Defining the GUCs (not just registering the worker /
    // shmem) MUST be gated here too — PostgreSQL FATALs if a PGC_POSTMASTER GUC
    // is defined outside postmaster startup, which would crash every
    // session_preload / LOAD / fmgr backend. When pg_deltax is loaded any other
    // way these are all skipped: maintenance is driven externally via
    // `deltax_run_maintenance()`, the blob cache stays off (a performance
    // feature, not correctness), and the three full-mode-only GUCs are simply
    // absent. This mirrors pg_stat_statements, which gates its GUC + shmem
    // machinery on the same flag. Listing pg_deltax in both preload lists is
    // harmless — Postgres won't re-run `_PG_init` for an already-loaded library.
    if unsafe { pg_sys::process_shared_preload_libraries_in_progress } {
        GucRegistry::define_string_guc(
            c"pg_deltax.target_database",
            c"Comma-separated database(s) the pg_deltax maintenance worker services",
            c"One maintenance worker is registered per listed database; each connects to exactly one database and services only deltatables registered there. Each entry consumes a max_worker_processes slot. Changing the list requires a server restart.",
            &TARGET_DATABASE,
            GucContext::Postmaster,
            GucFlags::default(),
        );
        GucRegistry::define_int_guc(
            c"pg_deltax.blob_cache_mb",
            c"Size of the process-shared blob cache, in MiB. -1 = auto (1/6 of physical RAM, clamped to [256, 16384]); 0 = disabled; N > 0 = explicit MiB.",
            c"The blob cache stores detoasted compressed segment blobs keyed by (companion_oid, segment_id, col_idx). Repeated queries against the same segments skip the pg_detoast_datum path. -1 (default) auto-sizes at postmaster start from /proc/meminfo, falling back to the 256 MB floor if it can't be read. Explicit values override the auto heuristic. See dev/docs/BLOB_CACHE.md. Restart required — the shmem reservation is captured at postmaster start.",
            &BLOB_CACHE_MB,
            -1,
            32768,
            GucContext::Postmaster,
            GucFlags::default(),
        );
        GucRegistry::define_int_guc(
            c"pg_deltax.blob_cache_shards",
            c"Number of shards (power of two) in the blob cache. Restart required.",
            c"Each shard owns an LWLock and an LRU list. More shards reduce contention under high concurrency; fewer save shmem overhead. Must be a power of two between 1 and 1024.",
            &BLOB_CACHE_SHARDS,
            1,
            1024,
            GucContext::Postmaster,
            GucFlags::default(),
        );
        blob_cache::register_hooks();
        worker::register_bgworker();
    }
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
