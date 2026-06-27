use std::time::Duration;

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use crate::catalog;
use crate::partition;

const DEFAULT_WORKER_INTERVAL_SECS: u64 = 60;

/// Per-database key for the transaction-level advisory lock that serializes
/// maintenance passes. The background worker and any manual or externally
/// scheduled `deltax_run_maintenance()` call all take this lock with
/// `pg_try_advisory_xact_lock` before touching any table; whoever loses simply
/// skips the pass (maintenance is periodic and idempotent, so a skipped
/// redundant pass is harmless). This is what keeps a manual call from
/// deadlocking against the worker in full mode — both would otherwise run the
/// same detach/attach/compress DDL concurrently.
///
/// Advisory locks are **cluster-wide**, not per-database, so a single fixed key
/// would needlessly serialize maintenance across *every* database (the multiple
/// workers in a full-mode `target_database` list, or one pg_cron job per
/// database). We fold the current database OID into the low 32 bits so passes
/// only mutually exclude *within the same database*; disjoint databases run
/// independently. The high 32 bits are an arbitrary pg_deltax tag ("pdlt").
fn maintenance_lock_key() -> i64 {
    const TAG: i64 = 0x7064_6C74; // "pdlt"
    let dboid = u32::from(unsafe { pg_sys::MyDatabaseId }) as i64;
    (TAG << 32) | dboid
}

/// Read `pg_deltax.target_database` and parse it into a list of database
/// names (see [`parse_target_databases`]). Only call this from a launched
/// process (the launcher) — custom-GUC values from postgresql.conf are not
/// reliably visible during `_PG_init` (verified empirically with both the
/// pgrx GucSetting and GetConfigOption).
pub(crate) fn target_databases() -> Vec<String> {
    let raw = crate::TARGET_DATABASE
        .get()
        .and_then(|c| c.to_str().ok().map(str::to_owned))
        .unwrap_or_default();
    parse_target_databases(&raw)
}

/// Parse a raw `pg_deltax.target_database` value into a trimmed,
/// deduplicated, order-preserving list of database names. A blank value (or
/// one with only empty entries) yields the upstream default `["postgres"]`.
/// Duplicates keep their first occurrence; later repeats are dropped.
pub(crate) fn parse_target_databases(raw: &str) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let dbs: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter(|s| seen.insert(s.to_string()))
        .map(str::to_owned)
        .collect();
    if dbs.is_empty() {
        vec!["postgres".to_string()]
    } else {
        dbs
    }
}

/// Register the static launcher at extension load time.
///
/// A background worker is bound to a single database for its lifetime
/// (`connect_worker_to_spi`), so a comma-separated `target_database` list
/// means one worker per entry. The fan-out cannot happen here: custom-GUC
/// values from postgresql.conf are not reliably visible during `_PG_init`
/// (verified empirically — both the pgrx GucSetting and GetConfigOption
/// still return the built-in default at this point). Instead a single
/// static launcher starts after recovery, reads the list with the GUC
/// system fully initialized, and spawns one dynamic worker per entry —
/// the same pattern pg_cron and pg_partman use. Launcher + each worker
/// consume one max_worker_processes slot apiece; list changes require a
/// restart (the GUC is Postmaster context).
pub fn register_bgworker() {
    BackgroundWorkerBuilder::new("pg_deltax maintenance launcher")
        .set_function("deltax_launcher_main")
        .set_library("pg_deltax")
        .set_argument(0i32.into_datum())
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .load();
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn deltax_launcher_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let dbs = target_databases();
    for db in &dbs {
        // Pass the target database name to the worker via `bgw_extra` (128
        // bytes; database names are at most NAMEDATALEN-1 = 63). The worker
        // connects to exactly this name, so it never has to re-derive the
        // list or agree with the launcher on its ordering.
        let spawned =
            BackgroundWorkerBuilder::new(&format!("pg_deltax maintenance worker ({})", db))
                .set_function("deltax_worker_main")
                .set_library("pg_deltax")
                .set_extra(db)
                .enable_spi_access()
                .set_restart_time(Some(Duration::from_secs(60)))
                .load_dynamic();
        match spawned {
            Ok(_) => log!("pg_deltax: launched maintenance worker for database {}", db),
            Err(e) => log!(
                "pg_deltax: failed to launch maintenance worker for {}: {:?}",
                db,
                e
            ),
        }
    }
    // Fan-out complete; the launcher exits. The static registration has no
    // restart time (BGW_NEVER_RESTART), and the dynamic workers are owned
    // by the postmaster from here on.
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn deltax_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    // The launcher passes this worker's target database name in `bgw_extra`.
    // Connecting by name (rather than re-deriving the list and indexing into
    // it) removes any dependency on the launcher and worker computing an
    // identical, identically-ordered list. The SPI binding is
    // once-per-worker-lifetime, so target_database changes take effect on a
    // server restart. An empty `bgw_extra` (e.g. a worker started by some
    // other means) falls back to the upstream default.
    let extra = BackgroundWorker::get_extra();
    let target_db = if extra.is_empty() { "postgres" } else { extra };
    BackgroundWorker::connect_worker_to_spi(Some(target_db), None);

    // The worker runs as superuser (BackgroundWorkerInitializeConnection with
    // username = NULL sets am_superuser = true), so an attacker who can plant
    // objects in any schema on the session search_path could shadow names this
    // code references unqualified — pg_class, pg_attribute, the `=` operator,
    // `now()`, etc. — and have the worker call into them. Pin search_path to
    // pg_catalog + pg_temp once at session start so unqualified references
    // always resolve to the system catalog. Everything pg_deltax-owned is
    // already schema-qualified (deltax.deltax_partition / _deltax_compressed.*),
    // so we don't need our schema on the path.
    BackgroundWorker::transaction(|| {
        Spi::run("SET search_path = pg_catalog, pg_temp")
            .expect("pg_deltax: failed to lock worker search_path");
    });

    log!(
        "pg_deltax: background worker started, interval = {}s",
        DEFAULT_WORKER_INTERVAL_SECS
    );

    while BackgroundWorker::wait_latch(Some(Duration::from_secs(DEFAULT_WORKER_INTERVAL_SECS))) {
        // One maintenance pass per tick, wrapped in a single transaction. The
        // replica guard, catalog-present check, and per-table error isolation
        // all live inside `run_maintenance_pass`, which is shared with the
        // SQL-callable `deltax_run_maintenance()` so both paths behave
        // identically.
        BackgroundWorker::transaction(|| {
            Spi::connect_mut(|client| {
                run_maintenance_pass(client);
            });
        });
    }

    log!("pg_deltax: background worker shutting down");
}

/// SQL-callable maintenance entry point: run one full pass synchronously over
/// every deltatable in the **current** database.
///
/// This is what session mode uses in place of the static background worker
/// (which can only be registered from a `shared_preload_libraries` postmaster
/// load). Schedule it externally — e.g. once a minute with pg_cron:
/// `SELECT cron.schedule_in_database(..., 'SELECT deltax.deltax_run_maintenance()', 'mydb')`.
/// It performs the same drain → premake → compress → retention steps the
/// background worker runs, with the same per-table error isolation, and no-ops
/// on a replica. Calling it via fmgr loads pg_deltax on demand, so it works
/// regardless of preload mode.
#[pg_extern]
fn deltax_run_maintenance() {
    Spi::connect_mut(|client| {
        run_maintenance_pass(client);
    });
}

/// Run one maintenance pass over every deltatable in the current database:
/// drain the default partition, pre-create future partitions, auto-compress
/// eligible partitions (refreshing parent stats), and drop partitions past
/// their retention. No-ops on a replica or before the extension catalog
/// exists.
///
/// Shared by the background worker (full mode) and `deltax_run_maintenance()`
/// (session mode / manual ops) so there is a single maintenance code path. The
/// caller supplies a connected `SpiClient` inside an open transaction. Each
/// deltatable is processed in its own internal subtransaction, so a failure on
/// one table rolls back only that table's partial work and the pass continues
/// with the rest.
pub(crate) fn run_maintenance_pass(client: &mut SpiClient) {
    // Pin search_path so unqualified names in the maintenance SQL (now(),
    // pg_tables, operators, casts, …) can't be shadowed by objects planted on
    // the caller's search_path. The background worker pins it at session start
    // because it runs as superuser; the SQL-callable path runs with the
    // caller's search_path (e.g. a superuser pg_cron job), so pin it per-pass
    // here too. SET LOCAL reverts at transaction end and never leaks into the
    // caller's session.
    client
        .update("SET LOCAL search_path = pg_catalog, pg_temp", None, &[])
        .expect("pg_deltax: failed to pin maintenance search_path");

    // Replica guard: an external scheduler (pg_cron) could fire this against a
    // standby, where the maintenance DDL would error. Skip the whole pass.
    // Default to "replica" on any error so we never attempt DDL on a standby.
    let is_replica = match client.select("SELECT pg_is_in_recovery()", None, &[]) {
        Ok(t) => t
            .first()
            .get_one::<bool>()
            .unwrap_or(Some(true))
            .unwrap_or(true),
        Err(_) => true,
    };
    if is_replica {
        return;
    }

    // Serialize against any other maintenance pass in THIS database (the
    // background worker vs. a manual/scheduled deltax_run_maintenance(), or two
    // scheduled calls). The lock is always taken before any table-level lock, so
    // two passes can never deadlock on the maintenance DDL; the loser skips this
    // pass. Auto-released when the surrounding transaction ends.
    let got_lock = match client.select(
        "SELECT pg_try_advisory_xact_lock($1)",
        None,
        &[maintenance_lock_key().into()],
    ) {
        Ok(t) => t
            .first()
            .get_one::<bool>()
            .unwrap_or(Some(false))
            .unwrap_or(false),
        Err(_) => false,
    };
    if !got_lock {
        return;
    }

    // Skip if the extension hasn't been installed yet (catalog tables missing).
    let has_catalog = client
        .select(
            "SELECT 1 FROM pg_tables WHERE schemaname = 'deltax' AND tablename = 'deltax_deltatable'",
            None,
            &[],
        )
        .map(|r| !r.is_empty())
        .unwrap_or(false);
    if !has_catalog {
        return;
    }

    let deltatables = match catalog::get_all_deltatables(client) {
        Ok(hts) => hts,
        Err(e) => {
            log!("pg_deltax: failed to get deltatables: {:?}", e);
            return;
        }
    };

    for ht in &deltatables {
        // Per-table isolation: a Postgres error in any step (e.g. a failed
        // compression) rolls back only this table's subtransaction so the rest
        // of the pass still runs. Without this, one broken deltatable would
        // abort the whole tick for every table.
        if let Err(msg) = run_in_subtransaction(|| maintain_one_table(client, ht)) {
            log!(
                "pg_deltax: maintenance failed for {}.{}: {}",
                ht.schema_name,
                ht.table_name,
                msg
            );
        }
    }
}

/// One deltatable's maintenance steps, in order. Informational counts are
/// logged here; a hard error in any step propagates to the caller's
/// subtransaction (see [`run_in_subtransaction`]), which rolls back this
/// table's work and continues with the next table.
fn maintain_one_table(client: &mut SpiClient, ht: &catalog::DeltatableInfo) {
    // Drain default partition first — rows in the default would block creation
    // of new partitions whose range overlaps with those rows.
    match drain_default_partition(client, ht) {
        Ok(drained) => {
            if drained.rows_moved > 0 {
                log!(
                    "pg_deltax: drained {} rows from {}_default into {} partition(s)",
                    drained.rows_moved,
                    ht.table_name,
                    drained.partitions_created
                );
            }
        }
        Err(e) => {
            log!(
                "pg_deltax: failed to drain default partition for {}.{}: {:?}",
                ht.schema_name,
                ht.table_name,
                e
            );
        }
    }

    // Pre-create future partitions (default premake = 3)
    match partition::ensure_future_partitions(client, ht, 3) {
        Ok(created) => {
            if created > 0 {
                log!(
                    "pg_deltax: created {} new partitions for {}.{}",
                    created,
                    ht.schema_name,
                    ht.table_name
                );
            }
        }
        Err(e) => {
            log!(
                "pg_deltax: failed to create partitions for {}.{}: {:?}",
                ht.schema_name,
                ht.table_name,
                e
            );
        }
    }

    // Auto-compress eligible partitions
    let compressed = crate::compress::auto_compress_partitions(client, ht);
    if compressed > 0 {
        log!(
            "pg_deltax: auto-compressed {} partitions for {}.{}",
            compressed,
            ht.schema_name,
            ht.table_name
        );
        // Per-partition stats are written at compress time; the parent-relation
        // merged stats (join/range selectivity) need re-merging across all
        // partitions whenever new ones are compressed.
        if let Err(e) = crate::stats::write_table_stats(client, &ht.schema_name, &ht.table_name) {
            log!(
                "pg_deltax: failed to refresh parent stats for {}.{}: {:?}",
                ht.schema_name,
                ht.table_name,
                e
            );
        }
    }

    // Auto-drop expired partitions (retention policy)
    let dropped = partition::auto_drop_partitions(client, ht);
    if dropped > 0 {
        log!(
            "pg_deltax: dropped {} expired partitions for {}.{}",
            dropped,
            ht.schema_name,
            ht.table_name
        );
    }
}

/// Run `body` inside an internal subtransaction (savepoint). On success the
/// subtransaction is released and its work persists in the surrounding
/// transaction. On any Postgres error or Rust panic the subtransaction is
/// rolled back and the error message is returned as `Err`, leaving the
/// surrounding transaction intact so the caller can log and continue.
///
/// Modeled on PL/pgSQL's `BEGIN ... EXCEPTION` block (`exec_stmt_block` in
/// `pl_exec.c`): save the surrounding memory context + resource owner, begin
/// the subtransaction, run the body, and restore the saved context/owner on
/// both the commit and abort paths. The error message captured in `caught` is
/// an owned copy (pgrx reads it off the error stack before invoking the
/// handler), so it is safe to flush the error state and unwind the
/// subtransaction inside the handler.
fn run_in_subtransaction<R>(body: impl FnOnce() -> R) -> Result<R, String> {
    let old_context = unsafe { pg_sys::CurrentMemoryContext };
    let old_owner = unsafe { pg_sys::CurrentResourceOwner };

    unsafe {
        pg_sys::BeginInternalSubTransaction(std::ptr::null());
        // BeginInternalSubTransaction switches into the subtransaction's
        // context; switch back so the body allocates where the caller expects.
        pg_sys::MemoryContextSwitchTo(old_context);
    }

    PgTryBuilder::new(std::panic::AssertUnwindSafe(|| {
        let r = body();
        unsafe {
            pg_sys::ReleaseCurrentSubTransaction();
            pg_sys::MemoryContextSwitchTo(old_context);
            pg_sys::CurrentResourceOwner = old_owner;
        }
        Ok(r)
    }))
    .catch_others(move |caught| {
        let msg = caught_message(&caught);
        unsafe {
            pg_sys::MemoryContextSwitchTo(old_context);
            pg_sys::FlushErrorState();
            pg_sys::RollbackAndReleaseCurrentSubTransaction();
            pg_sys::MemoryContextSwitchTo(old_context);
            pg_sys::CurrentResourceOwner = old_owner;
        }
        Err(msg)
    })
    .execute()
}

/// Extract an owned error message from a caught error for logging.
fn caught_message(caught: &pg_sys::panic::CaughtError) -> String {
    use pg_sys::panic::CaughtError;
    match caught {
        CaughtError::PostgresError(e)
        | CaughtError::ErrorReport(e)
        | CaughtError::RustPanic { ereport: e, .. } => e.message().to_string(),
    }
}

/// Outcome of a single drain pass: how many rows were moved from the
/// `<table>_default` partition into proper time-aligned partitions, and
/// how many new partitions were created to hold them.
pub(crate) struct DrainResult {
    pub rows_moved: i64,
    pub partitions_created: i32,
}

/// Move rows from the default partition into proper partitions.
/// Creates missing partitions on demand.
pub(crate) fn drain_default_partition(
    client: &mut SpiClient,
    ht: &catalog::DeltatableInfo,
) -> spi::SpiResult<DrainResult> {
    let default_name = format!("{}_default", ht.table_name);
    let fq_default = partition::fqn(&ht.schema_name, &default_name);

    let row_count = client
        .select(&format!("SELECT count(*) FROM {}", fq_default), None, &[])?
        .first()
        .get_one::<i64>()?
        .unwrap_or(0);

    if row_count == 0 {
        return Ok(DrainResult {
            rows_moved: 0,
            partitions_created: 0,
        });
    }

    let interval_usec = partition::interval_to_usec(&ht.partition_interval);

    // Distinct aligned start-of-interval timestamps for the rows currently
    // sitting in the default partition.
    let boundaries: Vec<i64> = {
        let result = client.select(
            &format!(
                "SELECT DISTINCT (EXTRACT(EPOCH FROM \"{}\") * 1000000)::int8 / {} * {} AS boundary
                 FROM {}
                 ORDER BY boundary",
                ht.time_column, interval_usec, interval_usec, fq_default
            ),
            None,
            &[],
        )?;
        let mut v = Vec::new();
        for row in result {
            if let Some(b) = row.get_datum_by_ordinal(1)?.value::<i64>()? {
                v.push(b);
            }
        }
        v
    };

    if boundaries.is_empty() {
        return Ok(DrainResult {
            rows_moved: row_count,
            partitions_created: 0,
        });
    }

    let parent = partition::fqn(&ht.schema_name, &ht.table_name);

    // Detach default first — PG won't allow creating a partition whose
    // range overlaps with rows already sitting in the default. The
    // bypass is needed so our own partition-rotation DDL isn't blocked
    // by the ALTER policy hook (see `src/ddl.rs`).
    crate::ddl::with_bypass(|| {
        client.update(
            &format!("ALTER TABLE {} DETACH PARTITION {}", parent, fq_default),
            None,
            &[],
        )
    })?;

    for &boundary_usec in &boundaries {
        let end_usec = boundary_usec + interval_usec;
        let start_str = partition::format_ts(boundary_usec);
        let end_str = partition::format_ts(end_usec);
        let part_name = partition::partition_name(&ht.table_name, boundary_usec, interval_usec);

        partition::create_partition(
            client,
            &ht.schema_name,
            &ht.table_name,
            &part_name,
            &start_str,
            &end_str,
        )?;

        catalog::register_partition(
            client,
            ht.id,
            &ht.schema_name,
            &part_name,
            partition::usec_to_tstz(boundary_usec),
            partition::usec_to_tstz(end_usec),
        )?;
    }

    // Move rows from the detached default into the proper partitions.
    client.update(
        &format!("INSERT INTO {} SELECT * FROM {}", parent, fq_default),
        None,
        &[],
    )?;
    client.update(&format!("TRUNCATE {}", fq_default), None, &[])?;
    crate::ddl::with_bypass(|| {
        client.update(
            &format!(
                "ALTER TABLE {} ATTACH PARTITION {} DEFAULT",
                parent, fq_default
            ),
            None,
            &[],
        )
    })?;

    Ok(DrainResult {
        rows_moved: row_count,
        partitions_created: boundaries.len() as i32,
    })
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::parse_target_databases;
    #[cfg(any(test, feature = "pg_test"))]
    use pgrx::prelude::*;

    #[test]
    fn blank_input_defaults_to_postgres() {
        assert_eq!(parse_target_databases(""), vec!["postgres"]);
        assert_eq!(parse_target_databases("   "), vec!["postgres"]);
        // Only-empty entries collapse to the default, not to an empty list.
        assert_eq!(parse_target_databases(",, ,"), vec!["postgres"]);
    }

    #[test]
    fn single_entry() {
        assert_eq!(parse_target_databases("postgres"), vec!["postgres"]);
        assert_eq!(parse_target_databases("metrics_db"), vec!["metrics_db"]);
    }

    #[test]
    fn multiple_entries_are_trimmed_and_order_preserved() {
        assert_eq!(
            parse_target_databases("postgres,metrics_db"),
            vec!["postgres", "metrics_db"]
        );
        assert_eq!(
            parse_target_databases("  postgres ,  metrics_db  "),
            vec!["postgres", "metrics_db"]
        );
    }

    #[test]
    fn duplicates_dropped_keeping_first_occurrence() {
        // Mirrors the PR's smoke config `postgres, smoke_db, postgres`.
        assert_eq!(
            parse_target_databases("postgres, smoke_db, postgres"),
            vec!["postgres", "smoke_db"]
        );
        // First-occurrence order wins, regardless of where the repeat sits.
        assert_eq!(parse_target_databases("b, a, b, c, a"), vec!["b", "a", "c"]);
    }

    #[test]
    fn blank_entries_between_real_ones_are_skipped() {
        assert_eq!(parse_target_databases("a,,b, ,c"), vec!["a", "b", "c"]);
    }

    // ---- run_in_subtransaction: the per-table isolation primitive ----------

    /// On success the subtransaction is released and its work persists in the
    /// surrounding transaction.
    #[pg_test]
    fn subtransaction_commits_on_success() {
        Spi::run("CREATE TEMP TABLE sx (n int)").unwrap();
        let out = super::run_in_subtransaction(|| {
            Spi::run("INSERT INTO sx VALUES (1)").unwrap();
            42
        });
        assert_eq!(out, Ok(42));
        let n = Spi::get_one::<i64>("SELECT count(*) FROM sx")
            .unwrap()
            .unwrap();
        assert_eq!(n, 1, "successful subtransaction work must persist");
    }

    /// A Postgres error (here division-by-zero, the `CaughtError::PostgresError`
    /// path) inside the body rolls back only that body's work, is returned as
    /// `Err`, and leaves the surrounding transaction usable.
    #[pg_test]
    fn subtransaction_rolls_back_postgres_error() {
        Spi::run("CREATE TEMP TABLE sx (n int)").unwrap();
        let out = super::run_in_subtransaction(|| {
            Spi::run("INSERT INTO sx VALUES (1)").unwrap();
            // Raises ERROR 22012 mid-body, after the insert.
            Spi::run("SELECT 1 / 0").unwrap();
            99
        });
        assert!(out.is_err(), "expected Err from a failing subtransaction");

        // Outer transaction still alive and the insert was rolled back.
        let n = Spi::get_one::<i64>("SELECT count(*) FROM sx")
            .unwrap()
            .unwrap();
        assert_eq!(n, 0, "failed subtransaction work must be rolled back");
    }

    /// The Err carries the underlying message (what the worker logs). Exercises
    /// the `CaughtError::ErrorReport` path via `pgrx::error!`.
    #[pg_test]
    fn subtransaction_captures_error_message() {
        let out: Result<(), String> = super::run_in_subtransaction(|| {
            pgrx::error!("deliberate boom 4242");
        });
        match out {
            Err(msg) => assert!(msg.contains("deliberate boom 4242"), "got: {msg}"),
            Ok(()) => panic!("expected Err"),
        }
    }

    /// The loop pattern: one unit failing does not stop the next from
    /// committing — i.e. real per-table isolation.
    #[pg_test]
    fn subtransaction_failure_does_not_block_next() {
        Spi::run("CREATE TEMP TABLE sx (n int)").unwrap();

        let first = super::run_in_subtransaction(|| {
            Spi::run("INSERT INTO sx VALUES (1)").unwrap();
            Spi::run("SELECT 1 / 0").unwrap(); // boom — rolls back the insert
        });
        assert!(first.is_err());

        let second = super::run_in_subtransaction(|| {
            Spi::run("INSERT INTO sx VALUES (2)").unwrap();
        });
        assert!(second.is_ok());

        let rows = Spi::get_one::<i64>("SELECT count(*) FROM sx")
            .unwrap()
            .unwrap();
        assert_eq!(rows, 1, "only the successful unit's row should remain");
        let val = Spi::get_one::<i32>("SELECT n FROM sx").unwrap().unwrap();
        assert_eq!(val, 2);
    }

    /// Nested subtransactions compose: an inner failure is contained and the
    /// outer body can still succeed and commit.
    #[pg_test]
    fn subtransaction_nesting_isolates_inner_failure() {
        Spi::run("CREATE TEMP TABLE sx (n int)").unwrap();
        let outer = super::run_in_subtransaction(|| {
            Spi::run("INSERT INTO sx VALUES (10)").unwrap();
            let inner = super::run_in_subtransaction(|| {
                Spi::run("INSERT INTO sx VALUES (20)").unwrap();
                Spi::run("SELECT 1 / 0").unwrap(); // inner boom
            });
            assert!(inner.is_err());
            // Outer continues after the contained inner failure.
            Spi::run("INSERT INTO sx VALUES (30)").unwrap();
        });
        assert!(outer.is_ok());

        // 10 and 30 committed; 20 rolled back with the inner subtransaction.
        let cnt = Spi::get_one::<i64>("SELECT count(*) FROM sx")
            .unwrap()
            .unwrap();
        assert_eq!(cnt, 2);
        let has20 = Spi::get_one::<bool>("SELECT EXISTS(SELECT 1 FROM sx WHERE n = 20)")
            .unwrap()
            .unwrap();
        assert!(!has20, "inner-subtransaction row must be rolled back");
        let mn = Spi::get_one::<i32>("SELECT min(n) FROM sx")
            .unwrap()
            .unwrap();
        let mx = Spi::get_one::<i32>("SELECT max(n) FROM sx")
            .unwrap()
            .unwrap();
        assert_eq!((mn, mx), (10, 30));
    }

    // ---- maintenance advisory-lock key ------------------------------------

    /// The advisory-lock key is per-database: high 32 bits are the fixed
    /// pg_deltax tag, low 32 bits are the current database OID. This is what
    /// keeps maintenance in different databases from serializing on a single
    /// cluster-wide advisory lock.
    #[pg_test]
    fn maintenance_lock_key_is_per_database() {
        let key = super::maintenance_lock_key();
        assert_eq!(
            (key >> 32) & 0xFFFF_FFFF,
            0x7064_6C74,
            "high bits = pg_deltax tag"
        );
        let dboid = Spi::get_one::<i64>(
            "SELECT oid::bigint FROM pg_database WHERE datname = current_database()",
        )
        .unwrap()
        .unwrap();
        assert_eq!(key & 0xFFFF_FFFF, dboid, "low bits = current database OID");
    }

    // ---- run_maintenance_pass smoke ---------------------------------------

    /// A pass over an empty/absent catalog must be a clean no-op (search_path
    /// pin → replica check → advisory lock → catalog-present check → return),
    /// never an error.
    #[pg_test]
    fn run_maintenance_pass_is_noop_smoke() {
        Spi::connect_mut(|client| {
            super::run_maintenance_pass(client);
        });
    }
}
