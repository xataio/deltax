use std::time::Duration;

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use crate::catalog;
use crate::partition;

const DEFAULT_WORKER_INTERVAL_SECS: u64 = 60;

/// Register the background worker at extension load time.
pub fn register_bgworker() {
    BackgroundWorkerBuilder::new("pg_seaturtle maintenance worker")
        .set_function("seaturtle_worker_main")
        .set_library("pg_seaturtle")
        .set_argument(0i32.into_datum())
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .load();
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn seaturtle_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    log!(
        "pg_seaturtle: background worker started, interval = {}s",
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
                    "SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'seaturtle_hypertable'",
                    None,
                    &[],
                ).map(|r| !r.is_empty()).unwrap_or(false);
                if !has_catalog {
                    return;
                }

                let hypertables = match catalog::get_all_hypertables(client) {
                    Ok(hts) => hts,
                    Err(e) => {
                        log!("pg_seaturtle: failed to get hypertables: {:?}", e);
                        return;
                    }
                };

                for ht in &hypertables {
                    // Drain default partition first — rows in the default
                    // would block creation of new partitions whose range
                    // overlaps with those rows.
                    match drain_default_partition(client, ht) {
                        Ok(moved) => {
                            if moved > 0 {
                                log!(
                                    "pg_seaturtle: drained {} rows from {}_default",
                                    moved,
                                    ht.table_name
                                );
                            }
                        }
                        Err(e) => {
                            log!(
                                "pg_seaturtle: failed to drain default partition for {}.{}: {:?}",
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
                                    "pg_seaturtle: created {} new partitions for {}.{}",
                                    created,
                                    ht.schema_name,
                                    ht.table_name
                                );
                            }
                        }
                        Err(e) => {
                            log!(
                                "pg_seaturtle: failed to create partitions for {}.{}: {:?}",
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
                            "pg_seaturtle: auto-compressed {} partitions for {}.{}",
                            compressed,
                            ht.schema_name,
                            ht.table_name
                        );
                    }

                    // Auto-drop expired partitions (retention policy)
                    let dropped = partition::auto_drop_partitions(client, ht);
                    if dropped > 0 {
                        log!(
                            "pg_seaturtle: dropped {} expired partitions for {}.{}",
                            dropped,
                            ht.schema_name,
                            ht.table_name
                        );
                    }
                }
            })
        });
    }

    log!("pg_seaturtle: background worker shutting down");
}

/// Move rows from the default partition into proper partitions.
/// Creates missing partitions on demand.
fn drain_default_partition(
    client: &mut SpiClient,
    ht: &catalog::HypertableInfo,
) -> spi::SpiResult<i64> {
    let default_name = format!("{}_default", ht.table_name);
    let fq_default = if ht.schema_name == "public" {
        format!("\"{}\"", default_name)
    } else {
        format!("\"{}\".\"{}\"", ht.schema_name, default_name)
    };

    // Check if default partition has rows
    let row_count = client
        .select(
            &format!("SELECT count(*) FROM {}", fq_default),
            None,
            &[],
        )?
        .first()
        .get_one::<i64>()?
        .unwrap_or(0);

    if row_count == 0 {
        return Ok(0);
    }

    let interval_usec = {
        let days: i64 = ht
            .partition_interval
            .extract_part(DateTimeParts::Day)
            .and_then(|v| v.try_into().ok())
            .unwrap_or(0);
        let hours: i64 = ht
            .partition_interval
            .extract_part(DateTimeParts::Hour)
            .and_then(|v| v.try_into().ok())
            .unwrap_or(0);
        let minutes: i64 = ht
            .partition_interval
            .extract_part(DateTimeParts::Minute)
            .and_then(|v| v.try_into().ok())
            .unwrap_or(0);
        let secs: i64 = ht
            .partition_interval
            .extract_part(DateTimeParts::Second)
            .and_then(|v| v.try_into().ok())
            .unwrap_or(0);
        days * 86_400_000_000 + hours * 3_600_000_000 + minutes * 60_000_000 + secs * 1_000_000
    };

    // Get distinct aligned timestamps from the default partition
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
            let val: Option<i64> = row.get_datum_by_ordinal(1)?.value::<i64>()?;
            if let Some(b) = val {
                v.push(b);
            }
        }
        v
    };

    if !boundaries.is_empty() {
        let parent = if ht.schema_name == "public" {
            format!("\"{}\"", ht.table_name)
        } else {
            format!("\"{}\".\"{}\"", ht.schema_name, ht.table_name)
        };

        // Detach default first — PG won't allow creating a partition whose
        // range overlaps with rows already sitting in the default.
        client.update(
            &format!("ALTER TABLE {} DETACH PARTITION {}", parent, fq_default),
            None,
            &[],
        )?;

        // Now create the missing partitions
        for boundary_usec in &boundaries {
            let end_usec = boundary_usec + interval_usec;

            let start_sec = *boundary_usec as f64 / 1_000_000.0;
            let end_sec = end_usec as f64 / 1_000_000.0;

            let start_str = Spi::get_one_with_args::<String>(
                "SELECT to_char(to_timestamp($1), 'YYYY-MM-DD HH24:MI:SS')",
                &[start_sec.into()],
            )?
            .unwrap();
            let end_str = Spi::get_one_with_args::<String>(
                "SELECT to_char(to_timestamp($1), 'YYYY-MM-DD HH24:MI:SS')",
                &[end_sec.into()],
            )?
            .unwrap();

            let query = if interval_usec >= 86_400_000_000 {
                "SELECT to_char(to_timestamp($1), 'YYYYMMDD')"
            } else {
                "SELECT to_char(to_timestamp($1), 'YYYYMMDD_HH24MI')"
            };
            let suffix =
                Spi::get_one_with_args::<String>(query, &[start_sec.into()])?.unwrap();
            let part_name = format!("{}_p{}", ht.table_name, suffix);

            partition::create_partition(
                client,
                &ht.schema_name,
                &ht.table_name,
                &part_name,
                &start_str,
                &end_str,
            )?;

            let start_tstz = Spi::get_one_with_args::<TimestampWithTimeZone>(
                "SELECT to_timestamp($1)",
                &[start_sec.into()],
            )?
            .unwrap();
            let end_tstz = Spi::get_one_with_args::<TimestampWithTimeZone>(
                "SELECT to_timestamp($1)",
                &[end_sec.into()],
            )?
            .unwrap();
            catalog::register_partition(
                client,
                ht.id,
                &ht.schema_name,
                &part_name,
                start_tstz,
                end_tstz,
            )?;
        }

        // Move rows from the detached default into the proper partitions
        client.update(
            &format!("INSERT INTO {} SELECT * FROM {}", parent, fq_default),
            None,
            &[],
        )?;

        client.update(&format!("TRUNCATE {}", fq_default), None, &[])?;

        client.update(
            &format!(
                "ALTER TABLE {} ATTACH PARTITION {} DEFAULT",
                parent, fq_default
            ),
            None,
            &[],
        )?;
    }

    Ok(row_count)
}
