use pgrx::prelude::*;
use pgrx::spi::SpiClient;

/// Metadata for a deltax-managed deltatable.
#[derive(Debug, Clone)]
pub struct DeltatableInfo {
    pub id: i32,
    pub schema_name: String,
    pub table_name: String,
    pub time_column: String,
    pub partition_interval: pgrx::datum::Interval,
    pub segment_by: Vec<String>,
    pub order_by: Vec<String>,
    pub compress_after: Option<pgrx::datum::Interval>,
    pub drop_after: Option<pgrx::datum::Interval>,
    pub segment_size: i32,
}

/// Metadata for a single partition.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PartitionInfo {
    pub id: i32,
    pub deltatable_id: i32,
    pub schema_name: String,
    pub table_name: String,
    pub range_start: TimestampWithTimeZone,
    pub range_end: TimestampWithTimeZone,
    pub is_compressed: bool,
}

/// Register a new deltatable in the catalog. Returns the new deltatable id.
pub fn register_deltatable(
    client: &mut SpiClient,
    schema_name: &str,
    table_name: &str,
    time_column: &str,
    partition_interval: &pgrx::datum::Interval,
) -> spi::SpiResult<i32> {
    let result = client.update(
        "INSERT INTO deltax_deltatable (schema_name, table_name, time_column, partition_interval)
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
    deltatable_id: i32,
    schema_name: &str,
    table_name: &str,
    range_start: TimestampWithTimeZone,
    range_end: TimestampWithTimeZone,
) -> spi::SpiResult<()> {
    client.update(
        "INSERT INTO deltax_partition (deltatable_id, schema_name, table_name, range_start, range_end)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (schema_name, table_name) DO NOTHING",
        None,
        &[
            deltatable_id.into(),
            schema_name.into(),
            table_name.into(),
            range_start.into(),
            range_end.into(),
        ],
    )?;
    Ok(())
}

/// Look up a deltatable by schema + table name.
pub fn get_deltatable(
    client: &SpiClient,
    schema_name: &str,
    table_name: &str,
) -> spi::SpiResult<Option<DeltatableInfo>> {
    let result = client.select(
        "SELECT id, schema_name, table_name, time_column, partition_interval
         FROM deltax_deltatable
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

    get_deltatable_by_id(client, id)
}

/// Look up a deltatable by its catalog id.
pub fn get_deltatable_by_id(
    client: &SpiClient,
    id: i32,
) -> spi::SpiResult<Option<DeltatableInfo>> {
    let mut result = client.select(
        "SELECT id, schema_name, table_name, time_column, partition_interval,
                segment_by, order_by, compress_after, drop_after, segment_size
         FROM deltax_deltatable
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
        let segment_size: i32 = row.get_datum_by_ordinal(10)?.value::<i32>()?.unwrap_or(30000);
        return Ok(Some(DeltatableInfo {
            id: ht_id,
            schema_name: s,
            table_name: t,
            time_column: tc,
            partition_interval: pi,
            segment_by,
            order_by,
            compress_after,
            drop_after,
            segment_size,
        }));
    }

    Ok(None)
}

/// Get all deltatables.
pub fn get_all_deltatables(
    client: &SpiClient,
) -> spi::SpiResult<Vec<DeltatableInfo>> {
    let result = client.select(
        "SELECT id, schema_name, table_name, time_column, partition_interval,
                segment_by, order_by, compress_after, drop_after, segment_size
         FROM deltax_deltatable",
        None,
        &[],
    )?;

    let mut deltatables = Vec::new();
    for row in result {
        deltatables.push(DeltatableInfo {
            id: row.get_datum_by_ordinal(1)?.value::<i32>()?.unwrap(),
            schema_name: row.get_datum_by_ordinal(2)?.value::<String>()?.unwrap(),
            table_name: row.get_datum_by_ordinal(3)?.value::<String>()?.unwrap(),
            time_column: row.get_datum_by_ordinal(4)?.value::<String>()?.unwrap(),
            partition_interval: row.get_datum_by_ordinal(5)?.value::<pgrx::datum::Interval>()?.unwrap(),
            segment_by: row.get_datum_by_ordinal(6)?.value::<Vec<String>>()?.unwrap_or_default(),
            order_by: row.get_datum_by_ordinal(7)?.value::<Vec<String>>()?.unwrap_or_default(),
            compress_after: row.get_datum_by_ordinal(8)?.value::<pgrx::datum::Interval>()?,
            drop_after: row.get_datum_by_ordinal(9)?.value::<pgrx::datum::Interval>()?,
            segment_size: row.get_datum_by_ordinal(10)?.value::<i32>()?.unwrap_or(30000),
        });
    }
    Ok(deltatables)
}

/// Get partitions for a deltatable, ordered by range_start.
pub fn get_partitions(
    client: &SpiClient,
    deltatable_id: i32,
) -> spi::SpiResult<Vec<PartitionInfo>> {
    let result = client.select(
        "SELECT id, deltatable_id, schema_name, table_name, range_start, range_end, is_compressed
         FROM deltax_partition
         WHERE deltatable_id = $1
         ORDER BY range_start",
        None,
        &[deltatable_id.into()],
    )?;

    let mut partitions = Vec::new();
    for row in result {
        partitions.push(PartitionInfo {
            id: row.get_datum_by_ordinal(1)?.value::<i32>()?.unwrap(),
            deltatable_id: row.get_datum_by_ordinal(2)?.value::<i32>()?.unwrap(),
            schema_name: row.get_datum_by_ordinal(3)?.value::<String>()?.unwrap(),
            table_name: row.get_datum_by_ordinal(4)?.value::<String>()?.unwrap(),
            range_start: row.get_datum_by_ordinal(5)?.value::<TimestampWithTimeZone>()?.unwrap(),
            range_end: row.get_datum_by_ordinal(6)?.value::<TimestampWithTimeZone>()?.unwrap(),
            is_compressed: row.get_datum_by_ordinal(7)?.value::<bool>()?.unwrap_or(false),
        });
    }
    Ok(partitions)
}

/// Update compression settings for a deltatable.
pub fn update_deltatable_compression(
    client: &mut SpiClient,
    deltatable_id: i32,
    segment_by: &[String],
    order_by: &[String],
    segment_size: i32,
) -> spi::SpiResult<()> {
    let seg_vec = segment_by.to_vec();
    let ord_vec = order_by.to_vec();
    client.update(
        "UPDATE deltax_deltatable
         SET segment_by = $1, order_by = $2, segment_size = $3
         WHERE id = $4",
        None,
        &[seg_vec.into(), ord_vec.into(), segment_size.into(), deltatable_id.into()],
    )?;
    Ok(())
}

/// Set the compress_after interval for a deltatable.
pub fn set_compress_after(
    client: &mut SpiClient,
    deltatable_id: i32,
    compress_after: &pgrx::datum::Interval,
) -> spi::SpiResult<()> {
    client.update(
        "UPDATE deltax_deltatable SET compress_after = $1 WHERE id = $2",
        None,
        &[(*compress_after).into(), deltatable_id.into()],
    )?;
    Ok(())
}

/// Set the drop_after interval for a deltatable (retention policy).
pub fn set_drop_after(
    client: &mut SpiClient,
    deltatable_id: i32,
    drop_after: &pgrx::datum::Interval,
) -> spi::SpiResult<()> {
    client.update(
        "UPDATE deltax_deltatable SET drop_after = $1 WHERE id = $2",
        None,
        &[(*drop_after).into(), deltatable_id.into()],
    )?;
    Ok(())
}

/// Clear the drop_after interval for a deltatable (remove retention policy).
pub fn clear_drop_after(
    client: &mut SpiClient,
    deltatable_id: i32,
) -> spi::SpiResult<()> {
    client.update(
        "UPDATE deltax_deltatable SET drop_after = NULL WHERE id = $1",
        None,
        &[deltatable_id.into()],
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
        "UPDATE deltax_partition
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

/// Write a pre-computed per-column ndistinct map (typically from
/// HLL sketches merged across segments during compression) to the
/// `deltax_partition.column_ndistinct` JSONB column.
pub fn update_partition_column_ndistinct_from_map(
    client: &mut SpiClient,
    partition_id: i32,
    col_ndistinct: &std::collections::HashMap<String, i64>,
) -> spi::SpiResult<()> {
    let mut parts: Vec<String> = Vec::with_capacity(col_ndistinct.len());
    let mut names: Vec<&String> = col_ndistinct.keys().collect();
    names.sort();
    for name in names {
        let nd_val = col_ndistinct[name];
        let escaped = name.replace('\\', "\\\\").replace('"', "\\\"");
        parts.push(format!("\"{}\":{}", escaped, nd_val));
    }
    let json = format!("{{{}}}", parts.join(","));

    client.update(
        "UPDATE deltax_partition SET column_ndistinct = $1::jsonb WHERE id = $2",
        None,
        &[json.into(), partition_id.into()],
    )?;
    Ok(())
}

/// Persist the per-column value lists used by the segment value-presence
/// bitmap (see `compress::compute_segment_valbitmaps` and the read-side
/// pruner in `scan::exec::segments`). Shape on disk is
/// `{"col_name": ["val0", "val1", ...]}` where the array index is the bit
/// position in each segment's bitmap. Only columns where partition-level
/// ndistinct ≤ 32 (the bitmap budget) get an entry.
pub fn update_partition_column_valmap(
    client: &mut SpiClient,
    partition_id: i32,
    col_valmap: &std::collections::HashMap<String, Vec<String>>,
) -> spi::SpiResult<()> {
    let mut parts: Vec<String> = Vec::with_capacity(col_valmap.len());
    let mut names: Vec<&String> = col_valmap.keys().collect();
    names.sort();
    for name in names {
        let vals = &col_valmap[name];
        let array_body: Vec<String> = vals
            .iter()
            .map(|v| format!("\"{}\"", json_escape(v)))
            .collect();
        parts.push(format!(
            "\"{}\":[{}]",
            json_escape(name),
            array_body.join(",")
        ));
    }
    let json = format!("{{{}}}", parts.join(","));

    client.update(
        "UPDATE deltax_partition SET column_valmap = $1::jsonb WHERE id = $2",
        None,
        &[json.into(), partition_id.into()],
    )?;
    Ok(())
}

/// Escape a string for inclusion in a JSON string literal. Handles all
/// JSON-mandatory escapes (`"`, `\\`, control chars 0x00–0x1F). Used by the
/// hand-rolled JSON writers above; we don't pull in a full JSON crate just
/// for the catalog payloads.
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

/// Compute per-column max ndistinct from the meta table and store the
/// result as a JSONB map on `deltax_partition.column_ndistinct`. Called
/// once at the end of compression so that planner-time cost estimation
/// (see `scan::cost::get_column_ndistinct`) can do a catalog lookup
/// instead of a cold full-scan of the wide meta table on every fresh
/// backend.
pub fn update_partition_column_ndistinct(
    client: &mut SpiClient,
    partition_id: i32,
    meta_fqn: &str,
    col_names: &[String],
) -> spi::SpiResult<()> {
    if col_names.is_empty() {
        return Ok(());
    }

    // Query normalized colstats table: one row per col_idx with MAX(ndistinct)
    let query = format!(
        "SELECT _col_idx, MAX(_ndistinct)::int8 FROM {} GROUP BY _col_idx ORDER BY _col_idx",
        meta_fqn
    );

    let result = client.select(&query, None, &[])?;

    // Build JSON object string: {"col1": 123, "col2": 456, ...}
    let mut parts: Vec<String> = Vec::with_capacity(col_names.len());
    for row in result {
        let col_idx: i32 = row
            .get_datum_by_ordinal(1)
            .ok()
            .and_then(|d| d.value::<i32>().ok().flatten())
            .unwrap_or(-1);
        let nd: Option<i64> = row
            .get_datum_by_ordinal(2)
            .ok()
            .and_then(|d| d.value::<i64>().ok().flatten());

        if col_idx >= 0 && (col_idx as usize) < col_names.len()
            && let Some(nd_val) = nd
        {
            let name = &col_names[col_idx as usize];
            let escaped = name.replace('\\', "\\\\").replace('"', "\\\"");
            parts.push(format!("\"{}\":{}", escaped, nd_val));
        }
    }
    let json = format!("{{{}}}", parts.join(","));

    client.update(
        "UPDATE deltax_partition SET column_ndistinct = $1::jsonb WHERE id = $2",
        None,
        &[json.into(), partition_id.into()],
    )?;
    Ok(())
}

/// Mark a partition as decompressed.
pub fn mark_partition_decompressed(
    client: &mut SpiClient,
    partition_id: i32,
) -> spi::SpiResult<()> {
    client.update(
        "UPDATE deltax_partition
         SET is_compressed = false, compressed_size = NULL, raw_size = NULL,
             row_count = NULL, compressed_at = NULL
         WHERE id = $1",
        None,
        &[partition_id.into()],
    )?;
    Ok(())
}

/// Install the DML rejection trigger on a compressed leaf partition.
///
/// Parent-table INSERTs are routed to the leaf after ExecutorStart, so the
/// result-relation hook cannot reliably catch them. A row trigger on the leaf
/// fires after tuple routing and blocks both direct and routed DML.
pub fn install_compressed_dml_trigger(
    client: &mut SpiClient,
    schema: &str,
    table: &str,
) -> spi::SpiResult<()> {
    let partition_fqn = crate::partition::fqn(schema, table);
    client.update(
        &format!(
            "DROP TRIGGER IF EXISTS deltax_reject_compressed_dml ON {}",
            partition_fqn
        ),
        None,
        &[],
    )?;
    client.update(
        &format!(
            "CREATE TRIGGER deltax_reject_compressed_dml
             BEFORE INSERT OR UPDATE OR DELETE ON {}
             FOR EACH ROW EXECUTE FUNCTION deltax_reject_compressed_partition_dml()",
            partition_fqn
        ),
        None,
        &[],
    )?;
    Ok(())
}

/// Remove the compressed-partition DML trigger before decompression restores
/// rows into the partition heap.
pub fn drop_compressed_dml_trigger(
    client: &mut SpiClient,
    schema: &str,
    table: &str,
) -> spi::SpiResult<()> {
    client.update(
        &format!(
            "DROP TRIGGER IF EXISTS deltax_reject_compressed_dml ON {}",
            crate::partition::fqn(schema, table)
        ),
        None,
        &[],
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
        "SELECT id, deltatable_id, schema_name, table_name, range_start, range_end, is_compressed
         FROM deltax_partition
         WHERE schema_name = $1 AND table_name = $2",
        None,
        &[schema_name.into(), table_name.into()],
    )?;

    if let Some(row) = result.next() {
        return Ok(Some(PartitionInfo {
            id: row.get_datum_by_ordinal(1)?.value::<i32>()?.unwrap(),
            deltatable_id: row.get_datum_by_ordinal(2)?.value::<i32>()?.unwrap(),
            schema_name: row.get_datum_by_ordinal(3)?.value::<String>()?.unwrap(),
            table_name: row.get_datum_by_ordinal(4)?.value::<String>()?.unwrap(),
            range_start: row.get_datum_by_ordinal(5)?.value::<TimestampWithTimeZone>()?.unwrap(),
            range_end: row.get_datum_by_ordinal(6)?.value::<TimestampWithTimeZone>()?.unwrap(),
            is_compressed: row.get_datum_by_ordinal(7)?.value::<bool>()?.unwrap_or(false),
        }));
    }

    Ok(None)
}
