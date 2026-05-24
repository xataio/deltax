use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use crate::catalog;

/// Convert an Interval to microseconds. Errors if months are present.
pub(crate) fn interval_to_usec(interval: &pgrx::datum::Interval) -> i64 {
    let months: i32 = interval
        .extract_part(DateTimeParts::Month)
        .and_then(|v| v.try_into().ok())
        .unwrap_or(0);

    if months != 0 {
        pgrx::error!("pg_deltax: monthly partition intervals are not supported; use days instead");
    }

    let days: i64 = interval
        .extract_part(DateTimeParts::Day)
        .and_then(|v| v.try_into().ok())
        .unwrap_or(0);
    let hours: i64 = interval
        .extract_part(DateTimeParts::Hour)
        .and_then(|v| v.try_into().ok())
        .unwrap_or(0);
    let minutes: i64 = interval
        .extract_part(DateTimeParts::Minute)
        .and_then(|v| v.try_into().ok())
        .unwrap_or(0);
    let secs: i64 = interval
        .extract_part(DateTimeParts::Second)
        .and_then(|v| v.try_into().ok())
        .unwrap_or(0);

    days * 86_400_000_000 + hours * 3_600_000_000 + minutes * 60_000_000 + secs * 1_000_000
}

/// Format a microsecond epoch timestamp as a PostgreSQL-compatible TIMESTAMPTZ literal.
pub(crate) fn format_ts(usec: i64) -> String {
    let epoch_sec = usec as f64 / 1_000_000.0;
    Spi::get_one_with_args::<String>(
        "SELECT to_char(to_timestamp($1), 'YYYY-MM-DD HH24:MI:SS')",
        &[epoch_sec.into()],
    )
    .expect("failed to format timestamp")
    .unwrap()
}

/// Generate the partition table name from the parent table name and range start.
pub(crate) fn partition_name(
    table_name: &str,
    range_start_usec: i64,
    interval_usec: i64,
) -> String {
    let epoch_sec = range_start_usec as f64 / 1_000_000.0;
    let query = if interval_usec >= 86_400_000_000 {
        "SELECT to_char(to_timestamp($1), 'YYYYMMDD')"
    } else {
        "SELECT to_char(to_timestamp($1), 'YYYYMMDD_HH24MI')"
    };

    let date_part = Spi::get_one_with_args::<String>(query, &[epoch_sec.into()])
        .expect("failed to format partition name")
        .unwrap();

    format!("{}_p{}", table_name, date_part)
}

/// Align a timestamp (in microseconds since Unix epoch) down to the nearest
/// interval boundary.
fn align_to_interval(ts_usec: i64, interval_usec: i64) -> i64 {
    let d = ts_usec / interval_usec;
    let r = ts_usec % interval_usec;
    if r < 0 {
        (d - 1) * interval_usec
    } else {
        d * interval_usec
    }
}

/// Get current time as microseconds since Unix epoch via SPI.
/// Respects the `pg_deltax.mock_now` GUC when set.
pub(crate) fn now_usec() -> i64 {
    if let Some(mock_cstr) = crate::MOCK_NOW.get() {
        let mock_val = mock_cstr.to_str().unwrap_or("");
        if !mock_val.is_empty() {
            return Spi::get_one_with_args::<i64>(
                "SELECT (EXTRACT(EPOCH FROM $1::timestamptz) * 1000000)::int8",
                &[mock_val.into()],
            )
            .expect("failed to parse pg_deltax.mock_now")
            .unwrap();
        }
    }
    Spi::get_one::<i64>("SELECT (EXTRACT(EPOCH FROM now()) * 1000000)::int8")
        .expect("failed to get current time")
        .unwrap()
}

/// Convert unix-epoch microseconds to a TimestampWithTimeZone via SPI.
pub(crate) fn usec_to_tstz(usec: i64) -> TimestampWithTimeZone {
    let epoch_sec = usec as f64 / 1_000_000.0;
    Spi::get_one_with_args::<TimestampWithTimeZone>("SELECT to_timestamp($1)", &[epoch_sec.into()])
        .expect("failed to convert to timestamptz")
        .unwrap()
}

/// Format a fully-qualified table name. Always emits "schema"."table" — even
/// for public — so SPI queries resolve under the bgworker's locked
/// search_path (pg_catalog, pg_temp) where no user schema is on the path.
pub fn fqn(schema: &str, table: &str) -> String {
    format!("\"{}\".\"{}\"", schema, table)
}

/// Create a single partition via SPI.
pub fn create_partition(
    client: &mut SpiClient,
    schema_name: &str,
    table_name: &str,
    part_name: &str,
    range_start: &str,
    range_end: &str,
) -> spi::SpiResult<()> {
    let parent = fqn(schema_name, table_name);
    let child = fqn(schema_name, part_name);
    client.update(
        &format!(
            "CREATE TABLE IF NOT EXISTS {} PARTITION OF {} FOR VALUES FROM ('{}') TO ('{}')",
            child, parent, range_start, range_end
        ),
        None,
        &[],
    )?;
    Ok(())
}

/// Core logic: create initial partitions for a deltatable.
pub fn create_initial_partitions(
    client: &mut SpiClient,
    schema_name: &str,
    table_name: &str,
    deltatable_id: i32,
    interval: &pgrx::datum::Interval,
    premake: i32,
) -> spi::SpiResult<i32> {
    let interval_usec = interval_to_usec(interval);
    let current_usec = now_usec();
    let current_aligned = align_to_interval(current_usec, interval_usec);

    let mut count = 0;

    // Create partitions from 1 in the past to `premake` in the future
    for i in -1..=premake {
        let start_usec = current_aligned + (i as i64 * interval_usec);
        let end_usec = start_usec + interval_usec;
        let start_str = format_ts(start_usec);
        let end_str = format_ts(end_usec);
        let part_name = partition_name(table_name, start_usec, interval_usec);

        create_partition(
            client,
            schema_name,
            table_name,
            &part_name,
            &start_str,
            &end_str,
        )?;

        let start_tstz = usec_to_tstz(start_usec);
        let end_tstz = usec_to_tstz(end_usec);

        catalog::register_partition(
            client,
            deltatable_id,
            schema_name,
            &part_name,
            start_tstz,
            end_tstz,
        )?;
        count += 1;
    }

    // Create default partition
    let default_name = format!("{}_default", table_name);
    let parent = fqn(schema_name, table_name);
    let default_fqn = fqn(schema_name, &default_name);
    client.update(
        &format!(
            "CREATE TABLE IF NOT EXISTS {} PARTITION OF {} DEFAULT",
            default_fqn, parent
        ),
        None,
        &[],
    )?;

    Ok(count)
}

/// Ensure future partitions exist for a deltatable. Called by the background worker.
pub fn ensure_future_partitions(
    client: &mut SpiClient,
    ht: &catalog::DeltatableInfo,
    premake: i32,
) -> spi::SpiResult<i32> {
    let interval_usec = interval_to_usec(&ht.partition_interval);
    let current_usec = now_usec();
    let current_aligned = align_to_interval(current_usec, interval_usec);
    let mut created = 0;

    for i in 0..=premake {
        let start_usec = current_aligned + (i as i64 * interval_usec);
        let end_usec = start_usec + interval_usec;
        let part_name = partition_name(&ht.table_name, start_usec, interval_usec);

        // Check if partition already registered
        let exists = client.select(
            "SELECT 1 FROM deltax.deltax_partition WHERE schema_name = $1 AND table_name = $2",
            None,
            &[ht.schema_name.as_str().into(), part_name.as_str().into()],
        )?;

        if exists.is_empty() {
            let start_str = format_ts(start_usec);
            let end_str = format_ts(end_usec);
            create_partition(
                client,
                &ht.schema_name,
                &ht.table_name,
                &part_name,
                &start_str,
                &end_str,
            )?;

            let start_tstz = usec_to_tstz(start_usec);
            let end_tstz = usec_to_tstz(end_usec);

            catalog::register_partition(
                client,
                ht.id,
                &ht.schema_name,
                &part_name,
                start_tstz,
                end_tstz,
            )?;
            created += 1;
        }
    }

    Ok(created)
}

// ============================================================================
// User-facing SQL functions
// ============================================================================

#[pg_extern]
fn deltax_create_table(
    relation: &str,
    time_column: &str,
    partition_interval: default!(pgrx::datum::Interval, "'1 day'"),
    premake: default!(i32, 3),
) -> String {
    Spi::connect_mut(|client| {
        // 1. Resolve schema and table name
        let (schema, table) = resolve_relation(client, relation);

        // 2. Check if already registered as a deltax table
        if catalog::get_deltatable(client, &schema, &table)
            .unwrap_or(None)
            .is_some()
        {
            return format!("Table {}.{} is already a deltax table", schema, table);
        }

        // 3. Validate the time column exists and is a timestamp type
        validate_time_column(client, &schema, &table, time_column);

        // 4. Check if table is already partitioned
        let is_partitioned = check_partitioned(client, &schema, &table);

        if !is_partitioned {
            // 5. Reject non-empty tables
            let has_rows = client
                .select(
                    &format!(
                        "SELECT EXISTS (SELECT 1 FROM \"{}\".\"{}\" LIMIT 1)",
                        schema, table
                    ),
                    None,
                    &[],
                )
                .expect("failed to check table emptiness")
                .first()
                .get_one::<bool>()
                .unwrap_or(Some(false))
                .unwrap_or(false);

            if has_rows {
                pgrx::error!(
                    "pg_deltax: table {}.{} is not empty. Only empty tables are supported.",
                    schema,
                    table
                );
            }

            // 6. Convert to partitioned table
            convert_to_partitioned(client, &schema, &table, time_column);
        }

        // 7. Register in catalog
        let ht_id =
            catalog::register_deltatable(client, &schema, &table, time_column, &partition_interval)
                .expect("failed to register deltatable");

        // 8. Create initial partitions
        let count =
            create_initial_partitions(client, &schema, &table, ht_id, &partition_interval, premake)
                .expect("failed to create initial partitions");

        format!(
            "Created deltax table {}.{} with {} partitions",
            schema, table, count
        )
    })
}

/// Resolve a relation name to (schema, table).
pub fn resolve_relation(_client: &SpiClient, relation: &str) -> (String, String) {
    let parts: Vec<&str> = relation.split('.').collect();
    match parts.len() {
        1 => {
            let schema = Spi::get_one_with_args::<String>(
                "SELECT schemaname::text FROM pg_tables WHERE tablename = $1::name LIMIT 1",
                &[parts[0].into()],
            )
            .expect("failed to look up table schema")
            .unwrap_or_else(|| {
                pgrx::error!("pg_deltax: table '{}' not found", relation);
            });
            (schema, parts[0].to_string())
        }
        2 => (parts[0].to_string(), parts[1].to_string()),
        _ => {
            pgrx::error!("pg_deltax: invalid relation name '{}'", relation);
        }
    }
}

/// Validate that the time column exists and is a timestamp type.
fn validate_time_column(_client: &SpiClient, schema: &str, table: &str, time_column: &str) {
    let data_type = Spi::get_one_with_args::<String>(
        "SELECT data_type::text FROM information_schema.columns
         WHERE table_schema = $1 AND table_name = $2 AND column_name = $3",
        &[schema.into(), table.into(), time_column.into()],
    )
    .unwrap_or(None);

    match data_type {
        None => {
            pgrx::error!(
                "pg_deltax: column '{}' not found in table {}.{}",
                time_column,
                schema,
                table
            );
        }
        Some(ref dt) if dt.contains("timestamp") => {
            // OK
        }
        Some(ref dt) => {
            pgrx::error!(
                "pg_deltax: column '{}' has type '{}', expected a timestamp type",
                time_column,
                dt
            );
        }
    }
}

/// Check if a table is already partitioned.
fn check_partitioned(_client: &SpiClient, schema: &str, table: &str) -> bool {
    Spi::get_one_with_args::<bool>(
        "SELECT c.relkind = 'p'
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1::name AND c.relname = $2::name",
        &[schema.into(), table.into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false)
}

/// Convert a regular (empty) table to a partitioned table.
fn convert_to_partitioned(client: &mut SpiClient, schema: &str, table: &str, time_column: &str) {
    let table_fqn = fqn(schema, table);
    let tmp_name = format!("_deltax_tmp_{}", table);
    let tmp_fqn = fqn(schema, &tmp_name);

    // Rename original table
    client
        .update(
            &format!("ALTER TABLE {} RENAME TO \"{}\"", table_fqn, tmp_name),
            None,
            &[],
        )
        .expect("failed to rename table");

    // Create new partitioned table with same structure
    client
        .update(
            &format!(
                "CREATE TABLE {} (LIKE {} INCLUDING ALL) PARTITION BY RANGE (\"{}\")",
                table_fqn, tmp_fqn, time_column
            ),
            None,
            &[],
        )
        .expect("failed to create partitioned table");

    // Drop the temp table
    client
        .update(&format!("DROP TABLE {}", tmp_fqn), None, &[])
        .expect("failed to drop temp table");
}

// ============================================================================
// Info functions
// ============================================================================

#[pg_extern]
fn deltax_partition_info(
    relation: &str,
) -> TableIterator<
    'static,
    (
        name!(partition_name, String),
        name!(range_start, TimestampWithTimeZone),
        name!(range_end, TimestampWithTimeZone),
        name!(is_compressed, bool),
    ),
> {
    let rows = Spi::connect(|client| {
        let (schema, table) = resolve_relation(client, relation);
        let ht = catalog::get_deltatable(client, &schema, &table)
            .expect("failed to query deltatable")
            .unwrap_or_else(|| {
                pgrx::error!(
                    "pg_deltax: table {}.{} is not a deltax table",
                    schema,
                    table
                )
            });

        let partitions =
            catalog::get_partitions(client, ht.id).expect("failed to query partitions");
        partitions
            .into_iter()
            .map(|p| (p.table_name, p.range_start, p.range_end, p.is_compressed))
            .collect::<Vec<_>>()
    });

    TableIterator::new(rows)
}

#[pg_extern]
fn deltax_deltatable_info(
    relation: &str,
) -> TableIterator<
    'static,
    (
        name!(schema_name, String),
        name!(table_name, String),
        name!(time_column, String),
        name!(partition_interval, pgrx::datum::Interval),
        name!(num_partitions, i64),
    ),
> {
    let rows = Spi::connect(|client| {
        let (schema, table) = resolve_relation(client, relation);
        let ht = catalog::get_deltatable(client, &schema, &table)
            .expect("failed to query deltatable")
            .unwrap_or_else(|| {
                pgrx::error!(
                    "pg_deltax: table {}.{} is not a deltax table",
                    schema,
                    table
                )
            });

        let partitions =
            catalog::get_partitions(client, ht.id).expect("failed to query partitions");
        let num_partitions = partitions.len() as i64;

        vec![(
            ht.schema_name,
            ht.table_name,
            ht.time_column,
            ht.partition_interval,
            num_partitions,
        )]
    });

    TableIterator::new(rows)
}

/// Drain rows that landed in `<table>_default` into proper, time-aligned
/// partitions on demand. The background worker performs the same step every
/// 60 seconds; calling this explicitly is useful right after a bulk load
/// (or in the README quickstart) so the rows are eligible for
/// `deltax_compress_all_partitions` without waiting for the next worker tick.
#[pg_extern]
fn deltax_drain_default_partition(relation: &str) -> String {
    Spi::connect_mut(|client| {
        let (schema, table) = resolve_relation(client, relation);
        let ht = catalog::get_deltatable(client, &schema, &table)
            .expect("failed to query deltatable")
            .unwrap_or_else(|| {
                pgrx::error!(
                    "pg_deltax: table {}.{} is not a deltax table",
                    schema,
                    table
                )
            });

        let drained = crate::worker::drain_default_partition(client, &ht)
            .expect("failed to drain default partition");

        if drained.rows_moved == 0 {
            format!("{}.{}_default is empty; nothing to drain", schema, table)
        } else {
            format!(
                "Drained {} row(s) from {}.{}_default into {} new partition(s)",
                drained.rows_moved, schema, table, drained.partitions_created
            )
        }
    })
}

/// Set a retention policy on a deltatable.
#[pg_extern]
fn deltax_set_retention(relation: &str, drop_after: pgrx::datum::Interval) -> String {
    Spi::connect_mut(|client| {
        let (schema, table) = resolve_relation(client, relation);
        let ht = catalog::get_deltatable(client, &schema, &table)
            .expect("failed to query deltatable")
            .unwrap_or_else(|| {
                pgrx::error!(
                    "pg_deltax: table {}.{} is not a deltax table",
                    schema,
                    table
                )
            });

        catalog::set_drop_after(client, ht.id, &drop_after)
            .expect("failed to set retention policy");

        format!(
            "Retention policy set on {}.{}: drop_after = {}",
            schema, table, drop_after
        )
    })
}

/// Remove the retention policy from a deltatable.
#[pg_extern]
fn deltax_remove_retention(relation: &str) -> String {
    Spi::connect_mut(|client| {
        let (schema, table) = resolve_relation(client, relation);
        let ht = catalog::get_deltatable(client, &schema, &table)
            .expect("failed to query deltatable")
            .unwrap_or_else(|| {
                pgrx::error!(
                    "pg_deltax: table {}.{} is not a deltax table",
                    schema,
                    table
                )
            });

        catalog::clear_drop_after(client, ht.id).expect("failed to remove retention policy");

        format!("Retention policy removed from {}.{}", schema, table)
    })
}

/// Drop partitions whose range_end is older than now() - drop_after.
/// Called by the background worker. Returns the number of partitions dropped.
pub fn auto_drop_partitions(client: &mut SpiClient, ht: &catalog::DeltatableInfo) -> i32 {
    let drop_after = match &ht.drop_after {
        Some(interval) => interval,
        None => return 0,
    };

    // Compute cutoff using mock-aware now_usec() so the background worker
    // respects pg_deltax.mock_now (important for tests and deterministic behaviour).
    let now = usec_to_tstz(now_usec());

    // Find partitions eligible for dropping: range_end < now() - drop_after
    let eligible = client
        .select(
            "SELECT schema_name, table_name, is_compressed FROM deltax.deltax_partition
             WHERE deltatable_id = $1 AND range_end < $2::timestamptz - $3::interval",
            None,
            &[ht.id.into(), now.into(), (*drop_after).into()],
        )
        .expect("failed to query eligible partitions for retention");

    let mut partitions: Vec<(String, String, bool)> = Vec::new();
    for row in eligible {
        let schema: String = row
            .get_datum_by_ordinal(1)
            .unwrap()
            .value::<String>()
            .unwrap()
            .unwrap();
        let name: String = row
            .get_datum_by_ordinal(2)
            .unwrap()
            .value::<String>()
            .unwrap()
            .unwrap();
        let is_compressed: bool = row
            .get_datum_by_ordinal(3)
            .unwrap()
            .value::<bool>()
            .unwrap()
            .unwrap_or(false);
        partitions.push((schema, name, is_compressed));
    }

    let parent_fqn = fqn(&ht.schema_name, &ht.table_name);

    for (schema, name, is_compressed) in &partitions {
        if *is_compressed {
            for suffix in ["blobs", "blooms", "text_lengths", "colstats", "meta"] {
                let fqn = format!("\"_deltax_compressed\".\"{}_{}\"", name, suffix);
                client
                    .update(&format!("DROP TABLE IF EXISTS {}", fqn), None, &[])
                    .expect("failed to drop companion table");
            }
        }

        let part_fqn = fqn(schema, name);

        // Detach partition from parent
        client
            .update(
                &format!("ALTER TABLE {} DETACH PARTITION {}", parent_fqn, part_fqn),
                None,
                &[],
            )
            .expect("failed to detach partition");

        // Drop the partition table
        client
            .update(&format!("DROP TABLE {}", part_fqn), None, &[])
            .expect("failed to drop partition table");

        // Remove from catalog
        client
            .update(
                "DELETE FROM deltax.deltax_partition WHERE schema_name = $1 AND table_name = $2",
                None,
                &[schema.as_str().into(), name.as_str().into()],
            )
            .expect("failed to remove partition from catalog");
    }

    partitions.len() as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 86_400_000_000;
    const HOUR: i64 = 3_600_000_000;

    #[test]
    fn fqn_always_schema_qualifies() {
        // Always emit "schema"."table" — the bgworker runs under a locked
        // search_path (pg_catalog, pg_temp) so user-schema names must be
        // explicitly qualified to resolve.
        assert_eq!(fqn("public", "foo"), "\"public\".\"foo\"");
        assert_eq!(fqn("myschema", "foo"), "\"myschema\".\"foo\"");
        // Embedded uppercase + reserved-ish names — quoting is required.
        assert_eq!(fqn("S", "Tbl"), "\"S\".\"Tbl\"");
    }

    #[test]
    fn align_to_interval_floors_to_boundary() {
        // Aligned timestamps are unchanged.
        assert_eq!(align_to_interval(0, DAY), 0);
        assert_eq!(align_to_interval(DAY, DAY), DAY);
        assert_eq!(align_to_interval(2 * DAY, DAY), 2 * DAY);
        // Mid-day timestamps floor down to the start-of-day.
        assert_eq!(align_to_interval(DAY + 1, DAY), DAY);
        assert_eq!(align_to_interval(DAY + HOUR, DAY), DAY);
        assert_eq!(align_to_interval(2 * DAY - 1, DAY), DAY);
    }

    #[test]
    fn align_to_interval_handles_negative_timestamps() {
        // Pre-epoch values exercise the `r < 0` arm. Plain integer division
        // truncates toward zero, which would land on the *later* boundary for
        // negatives — the explicit `(d - 1) * interval` keeps the floor
        // semantics consistent with the positive case.
        assert_eq!(align_to_interval(-1, DAY), -DAY);
        assert_eq!(align_to_interval(-DAY, DAY), -DAY);
        assert_eq!(align_to_interval(-DAY - 1, DAY), -2 * DAY);
        assert_eq!(align_to_interval(-2 * DAY + 1, DAY), -2 * DAY);
    }

    #[test]
    fn align_to_interval_handles_subday_intervals() {
        assert_eq!(align_to_interval(HOUR + 1, HOUR), HOUR);
        assert_eq!(align_to_interval(3 * HOUR + HOUR / 2, HOUR), 3 * HOUR);
    }
}
