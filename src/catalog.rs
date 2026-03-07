use pgrx::prelude::*;
use pgrx::spi::SpiClient;

/// Metadata for a seaturtle-managed hypertable.
#[derive(Debug, Clone)]
pub struct HypertableInfo {
    pub id: i32,
    pub schema_name: String,
    pub table_name: String,
    pub time_column: String,
    pub partition_interval: pgrx::datum::Interval,
    pub segment_by: Vec<String>,
    pub order_by: Vec<String>,
    pub compress_after: Option<pgrx::datum::Interval>,
    pub drop_after: Option<pgrx::datum::Interval>,
}

/// Metadata for a single partition.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PartitionInfo {
    pub id: i32,
    pub hypertable_id: i32,
    pub schema_name: String,
    pub table_name: String,
    pub range_start: TimestampWithTimeZone,
    pub range_end: TimestampWithTimeZone,
    pub is_compressed: bool,
}

/// Register a new hypertable in the catalog. Returns the new hypertable id.
pub fn register_hypertable(
    client: &mut SpiClient,
    schema_name: &str,
    table_name: &str,
    time_column: &str,
    partition_interval: &pgrx::datum::Interval,
) -> spi::SpiResult<i32> {
    let result = client.update(
        "INSERT INTO seaturtle_hypertable (schema_name, table_name, time_column, partition_interval)
         VALUES ($1, $2, $3, $4)
         RETURNING id",
        None,
        &[
            schema_name.into(),
            table_name.into(),
            time_column.into(),
            (*partition_interval).into(),
        ],
    )?;
    Ok(result.first().get_one::<i32>()?.unwrap())
}

/// Register a partition in the catalog.
pub fn register_partition(
    client: &mut SpiClient,
    hypertable_id: i32,
    schema_name: &str,
    table_name: &str,
    range_start: TimestampWithTimeZone,
    range_end: TimestampWithTimeZone,
) -> spi::SpiResult<()> {
    client.update(
        "INSERT INTO seaturtle_partition (hypertable_id, schema_name, table_name, range_start, range_end)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (schema_name, table_name) DO NOTHING",
        None,
        &[
            hypertable_id.into(),
            schema_name.into(),
            table_name.into(),
            range_start.into(),
            range_end.into(),
        ],
    )?;
    Ok(())
}

/// Look up a hypertable by schema + table name.
pub fn get_hypertable(
    client: &SpiClient,
    schema_name: &str,
    table_name: &str,
) -> spi::SpiResult<Option<HypertableInfo>> {
    let result = client.select(
        "SELECT id, schema_name, table_name, time_column, partition_interval
         FROM seaturtle_hypertable
         WHERE schema_name = $1 AND table_name = $2",
        None,
        &[schema_name.into(), table_name.into()],
    )?;

    if result.is_empty() {
        return Ok(None);
    }

    let id: Option<i32> = result.first().get_one::<i32>()?;
    let id = match id {
        Some(id) => id,
        None => return Ok(None),
    };

    get_hypertable_by_id(client, id)
}

/// Look up a hypertable by its catalog id.
pub fn get_hypertable_by_id(
    client: &SpiClient,
    id: i32,
) -> spi::SpiResult<Option<HypertableInfo>> {
    let mut result = client.select(
        "SELECT id, schema_name, table_name, time_column, partition_interval,
                segment_by, order_by, compress_after, drop_after
         FROM seaturtle_hypertable
         WHERE id = $1",
        None,
        &[id.into()],
    )?;

    if let Some(row) = result.next() {
        let ht_id: i32 = row.get_datum_by_ordinal(1)?.value::<i32>()?.unwrap();
        let s: String = row.get_datum_by_ordinal(2)?.value::<String>()?.unwrap();
        let t: String = row.get_datum_by_ordinal(3)?.value::<String>()?.unwrap();
        let tc: String = row.get_datum_by_ordinal(4)?.value::<String>()?.unwrap();
        let pi: pgrx::datum::Interval = row.get_datum_by_ordinal(5)?.value::<pgrx::datum::Interval>()?.unwrap();
        let segment_by: Vec<String> = row
            .get_datum_by_ordinal(6)?
            .value::<Vec<String>>()?
            .unwrap_or_default();
        let order_by: Vec<String> = row
            .get_datum_by_ordinal(7)?
            .value::<Vec<String>>()?
            .unwrap_or_default();
        let compress_after: Option<pgrx::datum::Interval> =
            row.get_datum_by_ordinal(8)?.value::<pgrx::datum::Interval>()?;
        let drop_after: Option<pgrx::datum::Interval> =
            row.get_datum_by_ordinal(9)?.value::<pgrx::datum::Interval>()?;
        return Ok(Some(HypertableInfo {
            id: ht_id,
            schema_name: s,
            table_name: t,
            time_column: tc,
            partition_interval: pi,
            segment_by,
            order_by,
            compress_after,
            drop_after,
        }));
    }

    Ok(None)
}

/// Get all hypertables.
pub fn get_all_hypertables(
    client: &SpiClient,
) -> spi::SpiResult<Vec<HypertableInfo>> {
    let result = client.select(
        "SELECT id, schema_name, table_name, time_column, partition_interval,
                segment_by, order_by, compress_after, drop_after
         FROM seaturtle_hypertable",
        None,
        &[],
    )?;

    let mut hypertables = Vec::new();
    for row in result {
        hypertables.push(HypertableInfo {
            id: row.get_datum_by_ordinal(1)?.value::<i32>()?.unwrap(),
            schema_name: row.get_datum_by_ordinal(2)?.value::<String>()?.unwrap(),
            table_name: row.get_datum_by_ordinal(3)?.value::<String>()?.unwrap(),
            time_column: row.get_datum_by_ordinal(4)?.value::<String>()?.unwrap(),
            partition_interval: row.get_datum_by_ordinal(5)?.value::<pgrx::datum::Interval>()?.unwrap(),
            segment_by: row.get_datum_by_ordinal(6)?.value::<Vec<String>>()?.unwrap_or_default(),
            order_by: row.get_datum_by_ordinal(7)?.value::<Vec<String>>()?.unwrap_or_default(),
            compress_after: row.get_datum_by_ordinal(8)?.value::<pgrx::datum::Interval>()?,
            drop_after: row.get_datum_by_ordinal(9)?.value::<pgrx::datum::Interval>()?,
        });
    }
    Ok(hypertables)
}

/// Get partitions for a hypertable, ordered by range_start.
pub fn get_partitions(
    client: &SpiClient,
    hypertable_id: i32,
) -> spi::SpiResult<Vec<PartitionInfo>> {
    let result = client.select(
        "SELECT id, hypertable_id, schema_name, table_name, range_start, range_end, is_compressed
         FROM seaturtle_partition
         WHERE hypertable_id = $1
         ORDER BY range_start",
        None,
        &[hypertable_id.into()],
    )?;

    let mut partitions = Vec::new();
    for row in result {
        partitions.push(PartitionInfo {
            id: row.get_datum_by_ordinal(1)?.value::<i32>()?.unwrap(),
            hypertable_id: row.get_datum_by_ordinal(2)?.value::<i32>()?.unwrap(),
            schema_name: row.get_datum_by_ordinal(3)?.value::<String>()?.unwrap(),
            table_name: row.get_datum_by_ordinal(4)?.value::<String>()?.unwrap(),
            range_start: row.get_datum_by_ordinal(5)?.value::<TimestampWithTimeZone>()?.unwrap(),
            range_end: row.get_datum_by_ordinal(6)?.value::<TimestampWithTimeZone>()?.unwrap(),
            is_compressed: row.get_datum_by_ordinal(7)?.value::<bool>()?.unwrap_or(false),
        });
    }
    Ok(partitions)
}

/// Update compression settings for a hypertable.
pub fn update_hypertable_compression(
    client: &mut SpiClient,
    hypertable_id: i32,
    segment_by: &[String],
    order_by: &[String],
) -> spi::SpiResult<()> {
    let seg_vec = segment_by.to_vec();
    let ord_vec = order_by.to_vec();
    client.update(
        "UPDATE seaturtle_hypertable
         SET segment_by = $1, order_by = $2
         WHERE id = $3",
        None,
        &[seg_vec.into(), ord_vec.into(), hypertable_id.into()],
    )?;
    Ok(())
}

/// Set the compress_after interval for a hypertable.
pub fn set_compress_after(
    client: &mut SpiClient,
    hypertable_id: i32,
    compress_after: &pgrx::datum::Interval,
) -> spi::SpiResult<()> {
    client.update(
        "UPDATE seaturtle_hypertable SET compress_after = $1 WHERE id = $2",
        None,
        &[(*compress_after).into(), hypertable_id.into()],
    )?;
    Ok(())
}

/// Set the drop_after interval for a hypertable (retention policy).
pub fn set_drop_after(
    client: &mut SpiClient,
    hypertable_id: i32,
    drop_after: &pgrx::datum::Interval,
) -> spi::SpiResult<()> {
    client.update(
        "UPDATE seaturtle_hypertable SET drop_after = $1 WHERE id = $2",
        None,
        &[(*drop_after).into(), hypertable_id.into()],
    )?;
    Ok(())
}

/// Clear the drop_after interval for a hypertable (remove retention policy).
pub fn clear_drop_after(
    client: &mut SpiClient,
    hypertable_id: i32,
) -> spi::SpiResult<()> {
    client.update(
        "UPDATE seaturtle_hypertable SET drop_after = NULL WHERE id = $1",
        None,
        &[hypertable_id.into()],
    )?;
    Ok(())
}

/// Mark a partition as compressed with size stats.
pub fn mark_partition_compressed(
    client: &mut SpiClient,
    partition_id: i32,
    compressed_size: i64,
    raw_size: i64,
    row_count: i64,
) -> spi::SpiResult<()> {
    client.update(
        "UPDATE seaturtle_partition
         SET is_compressed = true, compressed_size = $1, raw_size = $2,
             row_count = $3, compressed_at = now()
         WHERE id = $4",
        None,
        &[
            compressed_size.into(),
            raw_size.into(),
            row_count.into(),
            partition_id.into(),
        ],
    )?;
    Ok(())
}

/// Mark a partition as decompressed.
pub fn mark_partition_decompressed(
    client: &mut SpiClient,
    partition_id: i32,
) -> spi::SpiResult<()> {
    client.update(
        "UPDATE seaturtle_partition
         SET is_compressed = false, compressed_size = NULL, raw_size = NULL,
             row_count = NULL, compressed_at = NULL
         WHERE id = $1",
        None,
        &[partition_id.into()],
    )?;
    Ok(())
}

/// Look up a partition by schema + table name.
pub fn get_partition_by_name(
    client: &SpiClient,
    schema_name: &str,
    table_name: &str,
) -> spi::SpiResult<Option<PartitionInfo>> {
    let mut result = client.select(
        "SELECT id, hypertable_id, schema_name, table_name, range_start, range_end, is_compressed
         FROM seaturtle_partition
         WHERE schema_name = $1 AND table_name = $2",
        None,
        &[schema_name.into(), table_name.into()],
    )?;

    if let Some(row) = result.next() {
        return Ok(Some(PartitionInfo {
            id: row.get_datum_by_ordinal(1)?.value::<i32>()?.unwrap(),
            hypertable_id: row.get_datum_by_ordinal(2)?.value::<i32>()?.unwrap(),
            schema_name: row.get_datum_by_ordinal(3)?.value::<String>()?.unwrap(),
            table_name: row.get_datum_by_ordinal(4)?.value::<String>()?.unwrap(),
            range_start: row.get_datum_by_ordinal(5)?.value::<TimestampWithTimeZone>()?.unwrap(),
            range_end: row.get_datum_by_ordinal(6)?.value::<TimestampWithTimeZone>()?.unwrap(),
            is_compressed: row.get_datum_by_ordinal(7)?.value::<bool>()?.unwrap_or(false),
        }));
    }

    Ok(None)
}
