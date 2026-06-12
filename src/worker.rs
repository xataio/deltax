use std::time::Duration;

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use crate::catalog;
use crate::partition;

const DEFAULT_WORKER_INTERVAL_SECS: u64 = 60;

/// Parse `pg_deltax.target_database` into a trimmed, deduplicated,
/// order-preserving list of database names. An empty/blank GUC yields
/// the upstream default `["postgres"]`. Only call this from a launched
/// process (launcher or worker) — custom-GUC values from postgresql.conf
/// are not reliably visible during `_PG_init` (verified empirically with
/// both the pgrx GucSetting and GetConfigOption).
pub(crate) fn target_databases() -> Vec<String> {
    let raw = crate::TARGET_DATABASE
        .get()
        .and_then(|c| c.to_str().ok().map(str::to_owned))
        .unwrap_or_default();
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
    for (i, db) in dbs.iter().enumerate() {
        let spawned =
            BackgroundWorkerBuilder::new(&format!("pg_deltax maintenance worker ({})", db))
                .set_function("deltax_worker_main")
                .set_library("pg_deltax")
                .set_argument((i as i32).into_datum())
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
pub extern "C-unwind" fn deltax_worker_main(arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    // Each spawned worker carries its index into the target_databases()
    // list as its argument; re-derive the list (fully initialized in a
    // worker process) and connect to our entry. The SPI binding is
    // once-per-worker-lifetime, so list changes take effect on restart.
    let idx = unsafe { i32::from_datum(arg, false) }.unwrap_or(0).max(0) as usize;
    let dbs = target_databases();
    let target_db = dbs
        .get(idx)
        .cloned()
        .unwrap_or_else(|| "postgres".to_string());
    BackgroundWorker::connect_worker_to_spi(Some(&target_db), None);

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
        // Check if we're on a replica — skip all maintenance if so
        let is_replica = BackgroundWorker::transaction(|| {
            Spi::get_one::<bool>("SELECT pg_is_in_recovery()")
                .unwrap_or(Some(true))
                .unwrap_or(true)
        });

        if is_replica {
            continue;
        }

        BackgroundWorker::transaction(|| {
            Spi::connect_mut(|client| {
                // Skip if the extension hasn't been installed yet (catalog tables missing)
                let has_catalog = client.select(
                    "SELECT 1 FROM pg_tables WHERE schemaname = 'deltax' AND tablename = 'deltax_deltatable'",
                    None,
                    &[],
                ).map(|r| !r.is_empty()).unwrap_or(false);
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
                    // Drain default partition first — rows in the default
                    // would block creation of new partitions whose range
                    // overlaps with those rows.
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
                        // Per-partition stats are written at compress time; the
                        // parent-relation merged stats (join/range selectivity)
                        // need re-merging across all partitions whenever new
                        // ones are compressed.
                        if let Err(e) =
                            crate::stats::write_table_stats(client, &ht.schema_name, &ht.table_name)
                        {
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
            })
        });
    }

    log!("pg_deltax: background worker shutting down");
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
