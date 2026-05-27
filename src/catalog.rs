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
    /// Raw `json_extract` JSONB from the catalog as a serde_json Value.
    /// Parsed into `Vec<ExtractSpec>` at use sites via
    /// `compress::parse_extract_specs`. `None` = no JSON paths configured.
    #[allow(dead_code)] // Wired up incrementally across the json-extract feature.
    pub json_extract: Option<serde_json::Value>,
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
    /// Snapshot of physical-column shape (attnum, name, type_oid, typmod,
    /// is_segment_by, compressed_col_idx, dropped) at the moment this
    /// partition was last compressed. `None` for partitions that were
    /// compressed before this catalog column existed — the scan path falls
    /// back to positional `pg_attribute` mapping in that case. See
    /// `dev/docs/SCHEMA_CHANGES.md`.
    pub compressed_columns: Option<serde_json::Value>,
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
        "INSERT INTO deltax.deltax_deltatable (schema_name, table_name, time_column, partition_interval)
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
        "INSERT INTO deltax.deltax_partition (deltatable_id, schema_name, table_name, range_start, range_end)
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
         FROM deltax.deltax_deltatable
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
pub fn get_deltatable_by_id(client: &SpiClient, id: i32) -> spi::SpiResult<Option<DeltatableInfo>> {
    let mut result = client.select(
        "SELECT id, schema_name, table_name, time_column, partition_interval,
                segment_by, order_by, compress_after, drop_after, segment_size,
                json_extract
         FROM deltax.deltax_deltatable
         WHERE id = $1",
        None,
        &[id.into()],
    )?;

    if let Some(row) = result.next() {
        let ht_id: i32 = row.get_datum_by_ordinal(1)?.value::<i32>()?.unwrap();
        let s: String = row.get_datum_by_ordinal(2)?.value::<String>()?.unwrap();
        let t: String = row.get_datum_by_ordinal(3)?.value::<String>()?.unwrap();
        let tc: String = row.get_datum_by_ordinal(4)?.value::<String>()?.unwrap();
        let pi: pgrx::datum::Interval = row
            .get_datum_by_ordinal(5)?
            .value::<pgrx::datum::Interval>()?
            .unwrap();
        let segment_by: Vec<String> = row
            .get_datum_by_ordinal(6)?
            .value::<Vec<String>>()?
            .unwrap_or_default();
        let order_by: Vec<String> = row
            .get_datum_by_ordinal(7)?
            .value::<Vec<String>>()?
            .unwrap_or_default();
        let compress_after: Option<pgrx::datum::Interval> = row
            .get_datum_by_ordinal(8)?
            .value::<pgrx::datum::Interval>()?;
        let drop_after: Option<pgrx::datum::Interval> = row
            .get_datum_by_ordinal(9)?
            .value::<pgrx::datum::Interval>()?;
        let segment_size: i32 = row
            .get_datum_by_ordinal(10)?
            .value::<i32>()?
            .unwrap_or(30000);
        let json_extract: Option<serde_json::Value> = row
            .get_datum_by_ordinal(11)?
            .value::<pgrx::datum::JsonB>()?
            .map(|j| j.0);
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
            json_extract,
        }));
    }

    Ok(None)
}

/// Get all deltatables.
pub fn get_all_deltatables(client: &SpiClient) -> spi::SpiResult<Vec<DeltatableInfo>> {
    let result = client.select(
        "SELECT id, schema_name, table_name, time_column, partition_interval,
                segment_by, order_by, compress_after, drop_after, segment_size,
                json_extract
         FROM deltax.deltax_deltatable",
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
            partition_interval: row
                .get_datum_by_ordinal(5)?
                .value::<pgrx::datum::Interval>()?
                .unwrap(),
            segment_by: row
                .get_datum_by_ordinal(6)?
                .value::<Vec<String>>()?
                .unwrap_or_default(),
            order_by: row
                .get_datum_by_ordinal(7)?
                .value::<Vec<String>>()?
                .unwrap_or_default(),
            compress_after: row
                .get_datum_by_ordinal(8)?
                .value::<pgrx::datum::Interval>()?,
            drop_after: row
                .get_datum_by_ordinal(9)?
                .value::<pgrx::datum::Interval>()?,
            segment_size: row
                .get_datum_by_ordinal(10)?
                .value::<i32>()?
                .unwrap_or(30000),
            json_extract: row
                .get_datum_by_ordinal(11)?
                .value::<pgrx::datum::JsonB>()?
                .map(|j| j.0),
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
        "SELECT id, deltatable_id, schema_name, table_name, range_start, range_end,
                is_compressed, compressed_columns
         FROM deltax.deltax_partition
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
            range_start: row
                .get_datum_by_ordinal(5)?
                .value::<TimestampWithTimeZone>()?
                .unwrap(),
            range_end: row
                .get_datum_by_ordinal(6)?
                .value::<TimestampWithTimeZone>()?
                .unwrap(),
            is_compressed: row
                .get_datum_by_ordinal(7)?
                .value::<bool>()?
                .unwrap_or(false),
            compressed_columns: row
                .get_datum_by_ordinal(8)?
                .value::<pgrx::datum::JsonB>()?
                .map(|j| j.0),
        });
    }
    Ok(partitions)
}

/// Update compression settings for a deltatable. `json_extract` is `None` to
/// leave the existing value untouched, `Some(JsonB(Null))` to clear it, or
/// `Some(JsonB(<array>))` to set a new path list.
pub fn update_deltatable_compression(
    client: &mut SpiClient,
    deltatable_id: i32,
    segment_by: &[String],
    order_by: &[String],
    segment_size: i32,
    json_extract: Option<pgrx::datum::JsonB>,
) -> spi::SpiResult<()> {
    let seg_vec = segment_by.to_vec();
    let ord_vec = order_by.to_vec();
    if let Some(jx) = json_extract {
        // Stamp `json_extract_added_at` whenever json_extract is (re)set so
        // the planner can gate the rewrite on partitions compressed before
        // this point. Any change (add/remove a path, replace the list) bumps
        // the timestamp; partitions whose `compressed_at < json_extract_added_at`
        // are missing the synthetic columns and must fall through to the slow
        // path.
        client.update(
            "UPDATE deltax.deltax_deltatable
             SET segment_by = $1, order_by = $2, segment_size = $3,
                 json_extract = $4, json_extract_added_at = now()
             WHERE id = $5",
            None,
            &[
                seg_vec.into(),
                ord_vec.into(),
                segment_size.into(),
                jx.into(),
                deltatable_id.into(),
            ],
        )?;
    } else {
        client.update(
            "UPDATE deltax.deltax_deltatable
             SET segment_by = $1, order_by = $2, segment_size = $3
             WHERE id = $4",
            None,
            &[
                seg_vec.into(),
                ord_vec.into(),
                segment_size.into(),
                deltatable_id.into(),
            ],
        )?;
    }
    Ok(())
}

/// Set the compress_after interval for a deltatable.
pub fn set_compress_after(
    client: &mut SpiClient,
    deltatable_id: i32,
    compress_after: &pgrx::datum::Interval,
) -> spi::SpiResult<()> {
    client.update(
        "UPDATE deltax.deltax_deltatable SET compress_after = $1 WHERE id = $2",
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
        "UPDATE deltax.deltax_deltatable SET drop_after = $1 WHERE id = $2",
        None,
        &[(*drop_after).into(), deltatable_id.into()],
    )?;
    Ok(())
}

/// Clear the drop_after interval for a deltatable (remove retention policy).
pub fn clear_drop_after(client: &mut SpiClient, deltatable_id: i32) -> spi::SpiResult<()> {
    client.update(
        "UPDATE deltax.deltax_deltatable SET drop_after = NULL WHERE id = $1",
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
        "UPDATE deltax.deltax_partition
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
/// `deltax.deltax_partition.column_ndistinct` JSONB column.
pub fn update_partition_column_ndistinct_from_map(
    client: &mut SpiClient,
    partition_id: i32,
    col_ndistinct: &std::collections::HashMap<String, i64>,
) -> spi::SpiResult<()> {
    let mut parts: Vec<String> = Vec::with_capacity(col_ndistinct.len());
    let mut names: Vec<&String> = col_ndistinct.keys().collect();
    names.sort();
    for name in names {
        parts.push(format!("\"{}\":{}", json_escape(name), col_ndistinct[name]));
    }
    let json = format!("{{{}}}", parts.join(","));

    client.update(
        "UPDATE deltax.deltax_partition SET column_ndistinct = $1::jsonb WHERE id = $2",
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
        "UPDATE deltax.deltax_partition SET column_valmap = $1::jsonb WHERE id = $2",
        None,
        &[json.into(), partition_id.into()],
    )?;
    Ok(())
}

/// Aggregate per-segment colstats min/max into a partition-level
/// `{col_name: [min, max]}` JSONB and persist it on
/// `deltax.deltax_partition.column_minmax`. The aggregation runs as a single
/// SQL pass over the partition's `_colstats` table after all segments have
/// been written — `_col_idx` maps to the i-th non-segment-by column from
/// `columns`, matching the encoding used at scan time.
///
/// Used at read time by partition-level pruning: a `WHERE col = const`
/// equality whose const falls outside `[min, max]` for that partition can
/// skip the partition entirely (no meta open, no colstats probe).
pub fn update_partition_column_minmax(
    client: &mut SpiClient,
    partition_id: i32,
    colstats_fqn: &str,
    columns: &[crate::compress::ColumnMeta],
) -> spi::SpiResult<()> {
    // Build the parallel col_idx → col_name mapping in the same order
    // colstats uses (non-segment-by, 0-based).
    let mut idx_to_name: Vec<&str> = Vec::new();
    for col in columns {
        if !col.is_segment_by {
            idx_to_name.push(col.name.as_str());
        }
    }
    if idx_to_name.is_empty() {
        return Ok(());
    }

    // One pass over _colstats to get per-col_idx min(_min) / max(_max).
    let agg_sql = format!(
        "SELECT _col_idx, MIN(_min), MAX(_max) FROM {} \
         WHERE _min IS NOT NULL AND _max IS NOT NULL \
         GROUP BY _col_idx",
        colstats_fqn
    );
    let rows = client.select(&agg_sql, None, &[])?;

    let mut parts: Vec<String> = Vec::new();
    for row in rows {
        let col_idx: Option<i16> = row.get(1).ok().flatten();
        let min_v: Option<i64> = row.get(2).ok().flatten();
        let max_v: Option<i64> = row.get(3).ok().flatten();
        let (Some(ci), Some(min_v), Some(max_v)) = (col_idx, min_v, max_v) else {
            continue;
        };
        let Some(name) = idx_to_name.get(ci as usize).copied() else {
            continue;
        };
        parts.push(format!("\"{}\":[{},{}]", json_escape(name), min_v, max_v));
    }
    if parts.is_empty() {
        return Ok(());
    }
    let json = format!("{{{}}}", parts.join(","));

    client.update(
        "UPDATE deltax.deltax_partition SET column_minmax = $1::jsonb WHERE id = $2",
        None,
        &[json.into(), partition_id.into()],
    )?;
    Ok(())
}

/// Snapshot the current physical-column shape of the parent (deltatable) at
/// compression time, returning JSON text suitable for
/// `deltax_partition.compressed_columns`. One entry per non-dropped
/// pg_attribute row in ascending attnum order. `compressed_col_idx` is
/// assigned to non-`segment_by` columns starting at 0 — same order
/// `scan::exec::segments` expects when indexing `_blobs`/`_colstats`/etc.
/// Segment-by columns get `compressed_col_idx: null` (they live in `_meta`).
pub fn snapshot_compressed_columns(
    client: &SpiClient,
    parent_schema: &str,
    parent_table: &str,
    segment_by: &[String],
) -> spi::SpiResult<String> {
    let result = client.select(
        "SELECT a.attnum::int4, a.attname::text, a.atttypid::int8, a.atttypmod
         FROM pg_attribute a
         JOIN pg_class c ON c.oid = a.attrelid
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1 AND c.relname = $2
           AND a.attnum > 0 AND NOT a.attisdropped
         ORDER BY a.attnum",
        None,
        &[parent_schema.into(), parent_table.into()],
    )?;

    let segment_by_set: std::collections::HashSet<&str> =
        segment_by.iter().map(|s| s.as_str()).collect();

    let mut entries: Vec<String> = Vec::new();
    let mut next_compressed_col_idx: i32 = 0;
    for row in result {
        let attnum: i32 = row.get_datum_by_ordinal(1)?.value::<i32>()?.unwrap();
        let attname: String = row.get_datum_by_ordinal(2)?.value::<String>()?.unwrap();
        let atttypid: i64 = row.get_datum_by_ordinal(3)?.value::<i64>()?.unwrap();
        let atttypmod: i32 = row.get_datum_by_ordinal(4)?.value::<i32>()?.unwrap();

        let is_segment_by = segment_by_set.contains(attname.as_str());
        let compressed_col_idx_json = if is_segment_by {
            "null".to_string()
        } else {
            let idx = next_compressed_col_idx;
            next_compressed_col_idx += 1;
            idx.to_string()
        };

        entries.push(format!(
            "{{\"attnum\":{},\"name\":\"{}\",\"type_oid\":{},\"typmod\":{},\
             \"is_segment_by\":{},\"compressed_col_idx\":{},\"dropped\":false}}",
            attnum,
            json_escape(&attname),
            atttypid,
            atttypmod,
            is_segment_by,
            compressed_col_idx_json,
        ));
    }

    Ok(format!("[{}]", entries.join(",")))
}

/// Persist the compressed-columns descriptor for a partition. See
/// `snapshot_compressed_columns` for the shape and rationale.
pub fn update_partition_compressed_columns(
    client: &mut SpiClient,
    partition_id: i32,
    json: &str,
) -> spi::SpiResult<()> {
    client.update(
        "UPDATE deltax.deltax_partition SET compressed_columns = $1::jsonb WHERE id = $2",
        None,
        &[json.into(), partition_id.into()],
    )?;
    Ok(())
}

/// Mirror a `RENAME COLUMN old TO new` into the deltax catalog. Updates
/// `deltax_deltatable.time_column` (when it equals `old`), replaces `old`
/// inside the `segment_by` / `order_by` arrays, and rewrites the JSONB
/// keys in every child partition's `column_ndistinct` / `column_valmap`,
/// plus the `name` field inside each entry of `compressed_columns`.
/// The ALTER policy hook blocks renames of any column referenced by
/// segment_by / order_by / time_column today, so the array/scalar
/// updates here are defensive — they only fire after a future hook
/// change that lifts that restriction.
pub fn rename_column_in_deltatable(
    client: &mut SpiClient,
    deltatable_id: i32,
    old: &str,
    new: &str,
) -> spi::SpiResult<()> {
    if old == new {
        return Ok(());
    }
    // Update deltatable scalar + array references in one statement.
    client.update(
        "UPDATE deltax.deltax_deltatable
         SET time_column = CASE WHEN time_column = $2 THEN $3 ELSE time_column END,
             segment_by  = COALESCE(
                 array(SELECT CASE WHEN x = $2 THEN $3 ELSE x END FROM unnest(segment_by) AS x),
                 segment_by
             ),
             order_by    = COALESCE(
                 array(SELECT CASE WHEN x = $2 THEN $3 ELSE x END FROM unnest(order_by) AS x),
                 order_by
             )
         WHERE id = $1",
        None,
        &[deltatable_id.into(), old.into(), new.into()],
    )?;

    // Rewrite the keys of column_ndistinct and column_valmap JSONB
    // objects in every child partition. Use a CTE that re-keys with
    // jsonb_object_agg.
    client.update(
        "UPDATE deltax.deltax_partition
         SET column_ndistinct = CASE
             WHEN column_ndistinct ? $2 THEN
                 (SELECT jsonb_object_agg(
                     CASE WHEN key = $2 THEN $3 ELSE key END,
                     value
                 ) FROM jsonb_each(column_ndistinct))
             ELSE column_ndistinct
         END,
         column_valmap = CASE
             WHEN column_valmap ? $2 THEN
                 (SELECT jsonb_object_agg(
                     CASE WHEN key = $2 THEN $3 ELSE key END,
                     value
                 ) FROM jsonb_each(column_valmap))
             ELSE column_valmap
         END
         WHERE deltatable_id = $1",
        None,
        &[deltatable_id.into(), old.into(), new.into()],
    )?;

    // Rewrite the `name` field inside each entry of compressed_columns.
    // The descriptor is a JSONB array of objects; jsonb_set on each
    // matching entry via jsonb_path expressions is awkward, so unnest +
    // re-aggregate with jsonb_agg.
    client.update(
        "UPDATE deltax.deltax_partition
         SET compressed_columns = (
             SELECT jsonb_agg(
                 CASE WHEN elem ->> 'name' = $2
                      THEN jsonb_set(elem, '{name}', to_jsonb($3::text))
                      ELSE elem
                 END
                 ORDER BY ord
             )
             FROM jsonb_array_elements(compressed_columns) WITH ORDINALITY AS t(elem, ord)
         )
         WHERE deltatable_id = $1
           AND compressed_columns IS NOT NULL
           AND compressed_columns @> jsonb_build_array(jsonb_build_object('name', $2::text))",
        None,
        &[deltatable_id.into(), old.into(), new.into()],
    )?;

    Ok(())
}

/// Mirror a `RENAME TO new` into the deltax catalog. Updates
/// `deltax_deltatable.table_name`. Partition table names in PG don't
/// auto-rename when the parent renames, so child `deltax_partition.table_name`
/// rows keep their existing values — `<parent>_pYYYYMMDD` no longer
/// matches the renamed parent, which is fine because partition rows are
/// keyed by `id` everywhere except metadata-display contexts.
pub fn rename_deltatable(
    client: &mut SpiClient,
    deltatable_id: i32,
    new: &str,
) -> spi::SpiResult<()> {
    client.update(
        "UPDATE deltax.deltax_deltatable SET table_name = $2 WHERE id = $1",
        None,
        &[deltatable_id.into(), new.into()],
    )?;
    Ok(())
}

/// Return the fully-qualified names of every companion table that
/// exists for any partition of the given deltatable. Looks them up by
/// scanning `pg_class` in the `_deltax_compressed` schema and matching
/// on the partition-name prefix recorded in `deltax_partition`. Used
/// by the ALTER policy hook to cascade `OWNER TO` and `GRANT`/`REVOKE`
/// from the parent deltatable onto its companions (otherwise the
/// companions stay owned by / accessible to whoever created the
/// extension).
pub fn compressed_companion_tables(
    client: &SpiClient,
    deltatable_id: i32,
) -> spi::SpiResult<Vec<String>> {
    let result = client.select(
        "SELECT n.nspname::text || '.' || quote_ident(c.relname)
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = '_deltax_compressed'
           AND c.relkind IN ('r', 't')
           AND EXISTS (
               SELECT 1 FROM deltax.deltax_partition p
               WHERE p.deltatable_id = $1
                 AND (
                     c.relname = p.table_name || '_meta'
                     OR c.relname = p.table_name || '_colstats'
                     OR c.relname = p.table_name || '_blobs'
                     OR c.relname = p.table_name || '_blooms'
                     OR c.relname = p.table_name || '_text_lengths'
                     OR c.relname = p.table_name || '_valbitmap'
                 )
           )",
        None,
        &[deltatable_id.into()],
    )?;
    let mut names = Vec::new();
    for row in result {
        let fqn: String = row.get_datum_by_ordinal(1)?.value::<String>()?.unwrap();
        names.push(fqn);
    }
    Ok(names)
}

/// Mirror a Tier 2 `DROP COLUMN col` into every child partition's
/// `compressed_columns` descriptor by flipping `dropped: true` on the
/// matching entry. Orphan rows in `_blobs` / `_colstats` / `_blooms` /
/// `_text_lengths` / `_valbitmap` keyed by that descriptor's
/// `compressed_col_idx` are left as dead weight until the partition is
/// recompressed (see `dev/docs/SCHEMA_CHANGES.md` Future Work — GC).
///
/// Note: the partition table's `pg_attribute` has already been updated
/// by PG by the time this runs (this is a post-success PostAction), so
/// the descriptor and current schema disagree only in the JSON `dropped`
/// flag. `scan::exec::segments::load_metadata` already filters
/// `NOT a.attisdropped` in its `pg_attribute` SPI query, so a dropped
/// column never appears in `col_names`; `entry.dropped` is consulted
/// only on the unusual flow where a column is dropped then re-added
/// under the same name (PG assigns a new `attnum`, and the scan path
/// then synthesizes via `getmissingattr` for the new attnum).
pub fn tombstone_column_in_descriptor(
    client: &mut SpiClient,
    deltatable_id: i32,
    column_name: &str,
) -> spi::SpiResult<()> {
    client.update(
        "UPDATE deltax.deltax_partition
         SET compressed_columns = (
             SELECT jsonb_agg(
                 CASE WHEN elem ->> 'name' = $2
                      THEN jsonb_set(elem, '{dropped}', 'true'::jsonb)
                      ELSE elem
                 END
                 ORDER BY ord
             )
             FROM jsonb_array_elements(compressed_columns) WITH ORDINALITY AS t(elem, ord)
         )
         WHERE deltatable_id = $1
           AND compressed_columns IS NOT NULL
           AND compressed_columns @> jsonb_build_array(jsonb_build_object('name', $2::text))",
        None,
        &[deltatable_id.into(), column_name.into()],
    )?;
    Ok(())
}

/// Mirror a `SET SCHEMA new` into the deltax catalog. Updates only the
/// deltatable's `schema_name` — PG does NOT move partitions when the
/// parent's schema changes (each partition stays in its original
/// schema), so `deltax_partition.schema_name` rows are left alone.
/// Companion tables in `_deltax_compressed` likewise stay put.
pub fn set_deltatable_schema(
    client: &mut SpiClient,
    deltatable_id: i32,
    new: &str,
) -> spi::SpiResult<()> {
    client.update(
        "UPDATE deltax.deltax_deltatable SET schema_name = $2 WHERE id = $1",
        None,
        &[deltatable_id.into(), new.into()],
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
/// result as a JSONB map on `deltax.deltax_partition.column_ndistinct`. Called
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

        if col_idx >= 0
            && (col_idx as usize) < col_names.len()
            && let Some(nd_val) = nd
        {
            parts.push(format!(
                "\"{}\":{}",
                json_escape(&col_names[col_idx as usize]),
                nd_val,
            ));
        }
    }
    let json = format!("{{{}}}", parts.join(","));

    client.update(
        "UPDATE deltax.deltax_partition SET column_ndistinct = $1::jsonb WHERE id = $2",
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
        "UPDATE deltax.deltax_partition
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
    // PG 14+ supports `CREATE OR REPLACE TRIGGER`, so we can avoid the
    // `DROP TRIGGER IF EXISTS` step that otherwise emits a noisy
    // `NOTICE: trigger "..." does not exist, skipping` on the first
    // compression of a partition.
    let partition_fqn = crate::partition::fqn(schema, table);
    client.update(
        &format!(
            "CREATE OR REPLACE TRIGGER deltax_reject_compressed_dml
             BEFORE INSERT OR UPDATE OR DELETE ON {}
             FOR EACH ROW EXECUTE FUNCTION deltax.deltax_reject_compressed_partition_dml()",
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
        "SELECT id, deltatable_id, schema_name, table_name, range_start, range_end,
                is_compressed, compressed_columns
         FROM deltax.deltax_partition
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
            range_start: row
                .get_datum_by_ordinal(5)?
                .value::<TimestampWithTimeZone>()?
                .unwrap(),
            range_end: row
                .get_datum_by_ordinal(6)?
                .value::<TimestampWithTimeZone>()?
                .unwrap(),
            is_compressed: row
                .get_datum_by_ordinal(7)?
                .value::<bool>()?
                .unwrap_or(false),
            compressed_columns: row
                .get_datum_by_ordinal(8)?
                .value::<pgrx::datum::JsonB>()?
                .map(|j| j.0),
        }));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_escape_passes_through_plain_ascii() {
        assert_eq!(json_escape(""), "");
        assert_eq!(json_escape("hello"), "hello");
        assert_eq!(json_escape("col_42"), "col_42");
    }

    #[test]
    fn json_escape_handles_mandatory_escapes() {
        assert_eq!(json_escape("a\"b"), "a\\\"b");
        assert_eq!(json_escape("a\\b"), "a\\\\b");
        assert_eq!(json_escape("a\nb"), "a\\nb");
        assert_eq!(json_escape("a\rb"), "a\\rb");
        assert_eq!(json_escape("a\tb"), "a\\tb");
        assert_eq!(json_escape("a\x08b"), "a\\bb");
        assert_eq!(json_escape("a\x0cb"), "a\\fb");
    }

    #[test]
    fn json_escape_uses_unicode_for_other_control_chars() {
        // Anything below 0x20 without a short form falls through to \uXXXX —
        // omitting this would emit unparseable JSON for raw control bytes.
        assert_eq!(json_escape("\x00"), "\\u0000");
        assert_eq!(json_escape("\x01"), "\\u0001");
        assert_eq!(json_escape("\x1f"), "\\u001f");
    }

    #[test]
    fn json_escape_leaves_high_unicode_alone() {
        // The JSON spec allows any non-control codepoint verbatim, so we
        // don't bloat output for accented or CJK column names.
        assert_eq!(json_escape("héllo"), "héllo");
        assert_eq!(json_escape("日本語"), "日本語");
    }
}
