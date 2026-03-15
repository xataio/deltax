use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use std::collections::{HashMap, HashSet};

use crate::catalog;
use crate::compression::{self, CompressionType, CompressedColumn};

/// Microseconds between Unix epoch (1970-01-01) and PG epoch (2000-01-01).
const PG_EPOCH_OFFSET_USEC: i64 = 946_684_800_000_000;
/// Days between Unix epoch (1970-01-01) and PG epoch (2000-01-01).
const PG_EPOCH_OFFSET_DAYS: i64 = 10_957;

/// Column metadata from information_schema.
#[derive(Debug, Clone)]
struct ColumnMeta {
    name: String,
    data_type: String,
    is_segment_by: bool,
}

// ============================================================================
// SQL-callable functions
// ============================================================================

/// Enable compression on a seaturtle hypertable.
///
/// ```sql
/// SELECT seaturtle_enable_compression('metrics',
///     segment_by => ARRAY['device_id'],
///     order_by => ARRAY['ts']);
/// ```
#[pg_extern]
fn seaturtle_enable_compression(
    relation: &str,
    segment_by: default!(Vec<String>, "ARRAY[]::text[]"),
    order_by: default!(Vec<String>, "ARRAY[]::text[]"),
    segment_size: default!(i32, "30000"),
) -> String {
    Spi::connect_mut(|client| {
        let (schema, table) = crate::partition::resolve_relation(client, relation);
        let ht = catalog::get_hypertable(client, &schema, &table)
            .expect("failed to query hypertable")
            .unwrap_or_else(|| {
                pgrx::error!("pg_seaturtle: table {}.{} is not a seaturtle table", schema, table)
            });

        // Validate segment_by columns exist
        for col in &segment_by {
            let exists = client
                .select(
                    "SELECT 1 FROM information_schema.columns
                     WHERE table_schema = $1 AND table_name = $2 AND column_name::text = $3",
                    None,
                    &[schema.as_str().into(), table.as_str().into(), col.as_str().into()],
                )
                .expect("failed to check column");
            if exists.is_empty() {
                pgrx::error!("pg_seaturtle: segment_by column '{}' not found in {}.{}", col, schema, table);
            }
        }

        // If order_by is empty, default to the time column
        let effective_order_by = if order_by.is_empty() {
            vec![ht.time_column.clone()]
        } else {
            order_by
        };

        let effective_segment_size = if segment_size <= 0 { 30000 } else { segment_size };

        catalog::update_hypertable_compression(client, ht.id, &segment_by, &effective_order_by, effective_segment_size)
            .expect("failed to update compression settings");

        format!(
            "Compression enabled on {}.{} (segment_by: {:?}, order_by: {:?}, segment_size: {})",
            schema, table, segment_by, effective_order_by, effective_segment_size
        )
    })
}

/// Set the automatic compression policy for a hypertable.
#[pg_extern]
fn seaturtle_set_compression_policy(
    relation: &str,
    compress_after: pgrx::datum::Interval,
) -> String {
    Spi::connect_mut(|client| {
        let (schema, table) = crate::partition::resolve_relation(client, relation);
        let ht = catalog::get_hypertable(client, &schema, &table)
            .expect("failed to query hypertable")
            .unwrap_or_else(|| {
                pgrx::error!("pg_seaturtle: table {}.{} is not a seaturtle table", schema, table)
            });

        if ht.segment_by.is_empty() && ht.order_by.is_empty() {
            pgrx::error!("pg_seaturtle: enable compression first with seaturtle_enable_compression()");
        }

        catalog::set_compress_after(client, ht.id, &compress_after)
            .expect("failed to set compression policy");

        format!(
            "Compression policy set on {}.{}: compress_after = {}",
            schema, table, compress_after
        )
    })
}

/// Compress a single partition.
#[pg_extern]
fn seaturtle_compress_partition(partition: &str) -> String {
    Spi::connect_mut(|client| {
        compress_partition_impl(client, partition)
    })
}

/// Decompress a single partition.
#[pg_extern]
fn seaturtle_decompress_partition(partition: &str) -> String {
    Spi::connect_mut(|client| {
        decompress_partition_impl(client, partition)
    })
}

/// Show compression statistics for a hypertable.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn seaturtle_compression_stats(
    relation: &str,
) -> TableIterator<
    'static,
    (
        name!(partition_name, String),
        name!(is_compressed, bool),
        name!(raw_size, Option<i64>),
        name!(compressed_size, Option<i64>),
        name!(compression_ratio, Option<f64>),
        name!(row_count, Option<i64>),
    ),
> {
    let rows = Spi::connect(|client| {
        let (schema, table) = crate::partition::resolve_relation(client, relation);
        let ht = catalog::get_hypertable(client, &schema, &table)
            .expect("failed to query hypertable")
            .unwrap_or_else(|| {
                pgrx::error!("pg_seaturtle: table {}.{} is not a seaturtle table", schema, table)
            });

        let result = client
            .select(
                "SELECT table_name, is_compressed, raw_size, compressed_size, row_count
                 FROM seaturtle_partition
                 WHERE hypertable_id = $1
                 ORDER BY range_start",
                None,
                &[ht.id.into()],
            )
            .expect("failed to query partitions");

        let mut rows = Vec::new();
        for row in result {
            let name: String = row.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap().unwrap();
            let compressed: bool = row.get_datum_by_ordinal(2).unwrap().value::<bool>().unwrap().unwrap_or(false);
            let raw: Option<i64> = row.get_datum_by_ordinal(3).unwrap().value::<i64>().unwrap();
            let comp: Option<i64> = row.get_datum_by_ordinal(4).unwrap().value::<i64>().unwrap();
            let count: Option<i64> = row.get_datum_by_ordinal(5).unwrap().value::<i64>().unwrap();
            let ratio = match (raw, comp) {
                (Some(r), Some(c)) if c > 0 => Some(r as f64 / c as f64),
                _ => None,
            };
            rows.push((name, compressed, raw, comp, ratio, count));
        }
        rows
    });

    TableIterator::new(rows)
}

// ============================================================================
// Internal implementation
// ============================================================================

fn compress_partition_impl(client: &mut SpiClient, partition: &str) -> String {
    // 1. Look up partition in catalog
    let (schema, part_table) = crate::partition::resolve_relation(client, partition);
    let part_info = catalog::get_partition_by_name(client, &schema, &part_table)
        .expect("failed to query partition")
        .unwrap_or_else(|| {
            pgrx::error!("pg_seaturtle: partition {}.{} not found in catalog", schema, part_table)
        });

    if part_info.is_compressed {
        return format!("Partition {}.{} is already compressed", schema, part_table);
    }

    // 2. Get hypertable info (compression settings)
    let ht = catalog::get_hypertable_by_id(client, part_info.hypertable_id)
        .expect("failed to query hypertable")
        .unwrap();

    if ht.order_by.is_empty() && ht.segment_by.is_empty() {
        pgrx::error!("pg_seaturtle: compression not enabled on {}.{}. Call seaturtle_enable_compression() first.",
            ht.schema_name, ht.table_name);
    }

    // 3. Get column metadata
    let columns = get_column_metadata(client, &schema, &part_table, &ht.segment_by);
    if columns.is_empty() {
        pgrx::error!("pg_seaturtle: no columns found for {}.{}", schema, part_table);
    }

    // 4. Count rows
    let part_fqn = crate::partition::fqn(&schema, &part_table);
    let row_count = client
        .select(&format!("SELECT count(*)::int8 FROM {}", part_fqn), None, &[])
        .expect("failed to count rows")
        .first()
        .get_one::<i64>()
        .unwrap()
        .unwrap_or(0);

    if row_count == 0 {
        return format!("Partition {}.{} has no rows to compress", schema, part_table);
    }

    // 5. Build companion table DDL
    let companion_schema = "_seaturtle_compressed";
    let companion_fqn = format!("\"{}\".\"{}\"", companion_schema, part_table);

    let mut create_cols = Vec::new();
    // Segment-by columns stay uncompressed
    for col in &columns {
        if col.is_segment_by {
            create_cols.push(format!("\"{}\" {}", col.name, col.data_type));
        }
    }
    // Compressed columns as BYTEA
    for col in &columns {
        if !col.is_segment_by {
            create_cols.push(format!("\"_{}_compressed\" BYTEA", col.name));
        }
    }
    // Min/max metadata for all orderable (numeric/date/timestamp) columns
    for col in &columns {
        if !col.is_segment_by && supports_minmax(&col.data_type) {
            create_cols.push(format!("\"_min_{}\" {}", col.name, col.data_type));
            create_cols.push(format!("\"_max_{}\" {}", col.name, col.data_type));
        }
    }
    // Sum and non-null count metadata for numeric columns
    for col in &columns {
        if !col.is_segment_by && supports_sum(&col.data_type) {
            let sum_type = if is_float_type(&col.data_type) {
                "DOUBLE PRECISION"
            } else {
                "NUMERIC"
            };
            create_cols.push(format!("\"_sum_{}\" {}", col.name, sum_type));
            create_cols.push(format!("\"_nonnull_count_{}\" INT", col.name));
        }
    }
    create_cols.push("_row_count INT".to_string());

    let create_ddl = format!(
        "CREATE TABLE {} ({})",
        companion_fqn,
        create_cols.join(", ")
    );
    client.update(&create_ddl, None, &[]).expect("failed to create companion table");

    // 6. Read and compress data per segment
    let mut total_compressed_size: i64 = 0;
    let raw_size = estimate_raw_size(client, &part_fqn);

    let segment_size = ht.segment_size as usize;

    // ndistinct: for array_agg path, computed from in-memory data (fast);
    // for streaming path, computed via SQL COUNT(DISTINCT) (slower but segment_by tables are smaller).
    let ndistinct_json;

    if ht.segment_by.is_empty() {
        let (size, nd_map) = compress_partition_array_agg(
            client,
            &part_fqn,
            &companion_fqn,
            &columns,
            &ht.order_by,
            segment_size,
        );
        total_compressed_size += size;
        let entries: Vec<String> = nd_map
            .iter()
            .map(|(k, v)| format!("\"{}\":{}", k, v))
            .collect();
        ndistinct_json = format!("{{{}}}", entries.join(","));
    } else {
        // Compute ndistinct via SQL for segment_by path
        let non_seg_cols: Vec<&ColumnMeta> = columns.iter().filter(|c| !c.is_segment_by).collect();
        ndistinct_json = if !non_seg_cols.is_empty() {
            let count_exprs: String = non_seg_cols
                .iter()
                .map(|c| format!("COUNT(DISTINCT \"{}\")::int8", c.name))
                .collect::<Vec<_>>()
                .join(", ");
            let nd_query = format!("SELECT {} FROM {}", count_exprs, part_fqn);
            let nd_result = client
                .select(&nd_query, None, &[])
                .expect("failed to compute ndistinct");
            let mut nd_map: HashMap<String, i64> = HashMap::new();
            if let Some(row) = nd_result.into_iter().next() {
                for (i, col) in non_seg_cols.iter().enumerate() {
                    if let Some(nd) = row
                        .get_datum_by_ordinal(i + 1)
                        .unwrap()
                        .value::<i64>()
                        .unwrap()
                    {
                        nd_map.insert(col.name.clone(), nd);
                    }
                }
            }
            for col in columns.iter().filter(|c| c.is_segment_by) {
                let sq = format!(
                    "SELECT COUNT(DISTINCT \"{}\")::int8 FROM {}",
                    col.name, part_fqn
                );
                if let Some(nd) = client
                    .select(&sq, None, &[])
                    .expect("failed to compute segment_by ndistinct")
                    .first()
                    .get_one::<i64>()
                    .unwrap()
                {
                    nd_map.insert(col.name.clone(), nd);
                }
            }
            let entries: Vec<String> = nd_map
                .iter()
                .map(|(k, v)| format!("\"{}\":{}", k, v))
                .collect();
            format!("{{{}}}", entries.join(","))
        } else {
            "{}".to_string()
        };

        total_compressed_size += compress_partition_streaming(
            client,
            &part_fqn,
            &companion_fqn,
            &columns,
            &ht.order_by,
            &ht.segment_by,
            segment_size,
        );
    }

    // 8. Truncate original partition (stays attached to parent)
    client
        .update(&format!("TRUNCATE {}", part_fqn), None, &[])
        .expect("failed to truncate partition");

    // 9. Update catalog
    catalog::mark_partition_compressed(
        client,
        part_info.id,
        total_compressed_size,
        raw_size,
        row_count,
        &ndistinct_json,
    )
    .expect("failed to update catalog");

    crate::scan::invalidate_compressed_cache();

    format!(
        "Compressed {}.{}: {} rows, ratio {:.1}x",
        schema,
        part_table,
        row_count,
        if total_compressed_size > 0 {
            raw_size as f64 / total_compressed_size as f64
        } else {
            0.0
        }
    )
}

// ============================================================================
// Typed column storage — avoids text round-trip for numeric/boolean columns
// ============================================================================

/// Classifies how to read a column from SPI.
#[derive(Debug, Clone, Copy)]
enum ColumnKind {
    Text,         // text, varchar, char — read as String
    Int16,        // smallint/int2
    Int32,        // integer/int4
    Int64,        // bigint/int8
    Float32,      // real/float4
    Float64,      // double precision/float8
    Bool,         // boolean/bool
    Timestamp,    // timestamp without time zone — read as pgrx::Timestamp → i64 usec
    TimestampTz,  // timestamp with time zone — read as pgrx::TimestampWithTimeZone → i64 usec
    Date,         // date — read as pgrx::Date → i64 usec
}

/// Column data stored in native types.
enum TypedColumn {
    Text(Vec<Option<String>>),
    Int16(Vec<Option<i16>>),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Float32(Vec<Option<f32>>),
    Float64(Vec<Option<f64>>),
    Bool(Vec<Option<bool>>),
}

fn classify_column(data_type: &str, is_segment_by: bool) -> ColumnKind {
    if is_segment_by {
        return ColumnKind::Text; // segment_by always read as text for SQL literals
    }
    let dt = data_type.to_lowercase();
    if dt == "smallint" || dt == "int2" {
        ColumnKind::Int16
    } else if dt == "integer" || dt == "int4" {
        ColumnKind::Int32
    } else if dt == "bigint" || dt == "int8" {
        ColumnKind::Int64
    } else if dt == "double precision" || dt == "float8" {
        ColumnKind::Float64
    } else if dt == "real" || dt == "float4" {
        ColumnKind::Float32
    } else if dt == "boolean" || dt == "bool" {
        ColumnKind::Bool
    } else if dt == "timestamp with time zone" {
        ColumnKind::TimestampTz
    } else if dt.contains("timestamp") {
        ColumnKind::Timestamp
    } else if dt == "date" {
        ColumnKind::Date
    } else {
        ColumnKind::Text
    }
}

fn new_typed_column(kind: ColumnKind) -> TypedColumn {
    match kind {
        ColumnKind::Text => TypedColumn::Text(Vec::new()),
        ColumnKind::Int16 => TypedColumn::Int16(Vec::new()),
        ColumnKind::Int32 => TypedColumn::Int32(Vec::new()),
        ColumnKind::Int64 => TypedColumn::Int64(Vec::new()),
        ColumnKind::Float32 => TypedColumn::Float32(Vec::new()),
        ColumnKind::Float64 => TypedColumn::Float64(Vec::new()),
        ColumnKind::Bool => TypedColumn::Bool(Vec::new()),
        ColumnKind::Timestamp | ColumnKind::TimestampTz | ColumnKind::Date => {
            TypedColumn::Int64(Vec::new())
        }
    }
}

/// Create empty TypedColumn vectors for all columns based on their ColumnKind.
fn init_typed_columns(columns: &[ColumnMeta], kinds: &[ColumnKind]) -> Vec<TypedColumn> {
    columns
        .iter()
        .zip(kinds.iter())
        .map(|(_, kind)| new_typed_column(*kind))
        .collect()
}

/// Extract one SPI row into typed column accumulators using native datum access.
/// Segment_by columns are skipped (their TypedColumn slots remain empty).
fn append_row_to_columns(
    row: &pgrx::spi::SpiHeapTupleData,
    columns: &[ColumnMeta],
    kinds: &[ColumnKind],
    typed_cols: &mut [TypedColumn],
) {
    for (i, (col, kind)) in columns.iter().zip(kinds.iter()).enumerate() {
        if col.is_segment_by {
            continue;
        }
        let ordinal = i + 1; // SPI ordinals are 1-based
        match kind {
            ColumnKind::Int16 => {
                let v = row
                    .get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<i16>()
                    .unwrap();
                if let TypedColumn::Int16(vec) = &mut typed_cols[i] {
                    vec.push(v);
                }
            }
            ColumnKind::Int32 => {
                let v = row
                    .get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<i32>()
                    .unwrap();
                if let TypedColumn::Int32(vec) = &mut typed_cols[i] {
                    vec.push(v);
                }
            }
            ColumnKind::Int64 => {
                let v = row
                    .get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<i64>()
                    .unwrap();
                if let TypedColumn::Int64(vec) = &mut typed_cols[i] {
                    vec.push(v);
                }
            }
            ColumnKind::Float32 => {
                let v = row
                    .get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<f32>()
                    .unwrap();
                if let TypedColumn::Float32(vec) = &mut typed_cols[i] {
                    vec.push(v);
                }
            }
            ColumnKind::Float64 => {
                let v = row
                    .get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<f64>()
                    .unwrap();
                if let TypedColumn::Float64(vec) = &mut typed_cols[i] {
                    vec.push(v);
                }
            }
            ColumnKind::Bool => {
                let v = row
                    .get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<bool>()
                    .unwrap();
                if let TypedColumn::Bool(vec) = &mut typed_cols[i] {
                    vec.push(v);
                }
            }
            ColumnKind::Timestamp => {
                let v = row
                    .get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<pgrx::datum::Timestamp>()
                    .unwrap();
                if let TypedColumn::Int64(vec) = &mut typed_cols[i] {
                    // Convert PG-epoch usec to Unix-epoch usec
                    vec.push(v.map(|ts| ts.into_inner() + PG_EPOCH_OFFSET_USEC));
                }
            }
            ColumnKind::TimestampTz => {
                let v = row
                    .get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<pgrx::datum::TimestampWithTimeZone>()
                    .unwrap();
                if let TypedColumn::Int64(vec) = &mut typed_cols[i] {
                    // Convert PG-epoch usec to Unix-epoch usec
                    vec.push(v.map(|ts| ts.into_inner() + PG_EPOCH_OFFSET_USEC));
                }
            }
            ColumnKind::Date => {
                let v = row
                    .get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<pgrx::datum::Date>()
                    .unwrap();
                if let TypedColumn::Int64(vec) = &mut typed_cols[i] {
                    // Convert PG-epoch days to Unix-epoch usec
                    vec.push(v.map(|d| {
                        ((d.into_inner() as i64) + PG_EPOCH_OFFSET_DAYS) * 86_400_000_000
                    }));
                }
            }
            ColumnKind::Text => {
                let v = row
                    .get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<String>()
                    .unwrap();
                if let TypedColumn::Text(vec) = &mut typed_cols[i] {
                    vec.push(v);
                }
            }
        }
    }
}

/// Compress accumulated typed column data and INSERT into companion table.
/// Returns total compressed size in bytes.
fn flush_segment_data(
    client: &mut SpiClient,
    companion_fqn: &str,
    columns: &[ColumnMeta],
    typed_cols: &[TypedColumn],
    segment_by_values: &[Option<String>],
    row_count: u32,
) -> i64 {
    use pgrx::datum::DatumWithOid;

    // Compress each non-segment column
    let mut compressed_data: Vec<(String, Vec<u8>)> = Vec::new();
    let mut col_minmax: std::collections::HashMap<String, (Option<String>, Option<String>)> =
        std::collections::HashMap::new();
    let mut total_size: i64 = 0;

    let mut col_sums: std::collections::HashMap<String, (Option<String>, i64)> =
        std::collections::HashMap::new();

    for (i, col) in columns.iter().enumerate() {
        if col.is_segment_by {
            continue;
        }
        let compressed = compress_typed_column(&typed_cols[i], &col.data_type);
        if supports_minmax(&col.data_type) {
            let (min_val, max_val) = compute_typed_minmax(&typed_cols[i], &col.data_type);
            col_minmax.insert(col.name.clone(), (min_val, max_val));
        }
        if supports_sum(&col.data_type) {
            col_sums.insert(col.name.clone(), compute_typed_sum(&typed_cols[i]));
        }
        total_size += compressed.len() as i64;
        compressed_data.push((col.name.clone(), compressed));
    }

    // Build parameterized INSERT — bytea columns use $N placeholders,
    // segment_by and minmax values are SQL literals (small).
    let mut insert_cols = Vec::new();
    let mut insert_vals = Vec::new();
    let mut args: Vec<DatumWithOid> = Vec::new();
    let mut param_idx: usize = 0;

    // Segment-by columns as SQL literals
    let mut seg_idx = 0;
    for col in columns {
        if col.is_segment_by {
            insert_cols.push(format!("\"{}\"", col.name));
            if seg_idx < segment_by_values.len() {
                match &segment_by_values[seg_idx] {
                    Some(v) => insert_vals.push(format!("'{}'", v.replace('\'', "''"))),
                    None => insert_vals.push("NULL".to_string()),
                }
                seg_idx += 1;
            }
        }
    }

    // Compressed columns as $N bytea parameters (consume data, no clone)
    for (col_name, data) in compressed_data {
        insert_cols.push(format!("\"_{}_compressed\"", col_name));
        param_idx += 1;
        insert_vals.push(format!("${}", param_idx));
        args.push(DatumWithOid::from(data));
    }

    // Min/max as SQL literals (small values)
    for col in columns {
        if !col.is_segment_by && supports_minmax(&col.data_type) {
            insert_cols.push(format!("\"_min_{}\"", col.name));
            insert_cols.push(format!("\"_max_{}\"", col.name));
        }
    }
    for col in columns {
        if !col.is_segment_by && supports_minmax(&col.data_type) {
            match col_minmax.get(&col.name) {
                Some((Some(min_val), Some(max_val))) => {
                    insert_vals.push(format_minmax_for_insert(min_val, &col.data_type));
                    insert_vals.push(format_minmax_for_insert(max_val, &col.data_type));
                }
                _ => {
                    insert_vals.push("NULL".to_string());
                    insert_vals.push("NULL".to_string());
                }
            }
        }
    }
    // Sum and non-null count metadata
    for col in columns {
        if !col.is_segment_by && supports_sum(&col.data_type) {
            insert_cols.push(format!("\"_sum_{}\"", col.name));
            insert_cols.push(format!("\"_nonnull_count_{}\"", col.name));
        }
    }
    for col in columns {
        if !col.is_segment_by && supports_sum(&col.data_type) {
            match col_sums.get(&col.name) {
                Some((Some(sum_val), nonnull_count)) => {
                    insert_vals.push(sum_val.clone());
                    insert_vals.push(nonnull_count.to_string());
                }
                _ => {
                    insert_vals.push("NULL".to_string());
                    insert_vals.push("0".to_string());
                }
            }
        }
    }
    insert_cols.push("_row_count".to_string());
    insert_vals.push(row_count.to_string());

    let insert_sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        companion_fqn,
        insert_cols.join(", "),
        insert_vals.join(", ")
    );
    client
        .update(&insert_sql, None, &args)
        .expect("failed to insert compressed segment");

    total_size
}

/// Slice a TypedColumn to a sub-range [start..end).
/// Empty columns (e.g. segment_by placeholders) are returned as-is.
fn slice_typed_column(tc: &TypedColumn, start: usize, end: usize) -> TypedColumn {
    match tc {
        TypedColumn::Text(v) if v.is_empty() => TypedColumn::Text(Vec::new()),
        TypedColumn::Text(v) => TypedColumn::Text(v[start..end].to_vec()),
        TypedColumn::Int16(v) if v.is_empty() => TypedColumn::Int16(Vec::new()),
        TypedColumn::Int16(v) => TypedColumn::Int16(v[start..end].to_vec()),
        TypedColumn::Int32(v) if v.is_empty() => TypedColumn::Int32(Vec::new()),
        TypedColumn::Int32(v) => TypedColumn::Int32(v[start..end].to_vec()),
        TypedColumn::Int64(v) if v.is_empty() => TypedColumn::Int64(Vec::new()),
        TypedColumn::Int64(v) => TypedColumn::Int64(v[start..end].to_vec()),
        TypedColumn::Float32(v) if v.is_empty() => TypedColumn::Float32(Vec::new()),
        TypedColumn::Float32(v) => TypedColumn::Float32(v[start..end].to_vec()),
        TypedColumn::Float64(v) if v.is_empty() => TypedColumn::Float64(Vec::new()),
        TypedColumn::Float64(v) => TypedColumn::Float64(v[start..end].to_vec()),
        TypedColumn::Bool(v) if v.is_empty() => TypedColumn::Bool(Vec::new()),
        TypedColumn::Bool(v) => TypedColumn::Bool(v[start..end].to_vec()),
    }
}

/// Flush typed column data, splitting into segment_size chunks if needed.
fn flush_with_splitting(
    client: &mut SpiClient,
    companion_fqn: &str,
    columns: &[ColumnMeta],
    typed_cols: &[TypedColumn],
    seg_values: &[Option<String>],
    total_rows: usize,
    segment_size: usize,
) -> i64 {
    let mut total_size = 0i64;
    let mut offset = 0;
    while offset < total_rows {
        let chunk_end = (offset + segment_size).min(total_rows);
        let chunk_rows = (chunk_end - offset) as u32;
        if offset == 0 && chunk_end == total_rows {
            total_size +=
                flush_segment_data(client, companion_fqn, columns, typed_cols, seg_values, chunk_rows);
        } else {
            let chunk_cols: Vec<TypedColumn> = typed_cols
                .iter()
                .map(|tc| slice_typed_column(tc, offset, chunk_end))
                .collect();
            total_size +=
                flush_segment_data(client, companion_fqn, columns, &chunk_cols, seg_values, chunk_rows);
        }
        offset = chunk_end;
    }
    total_size
}

/// Compute ndistinct (count of distinct non-NULL values) for each column from in-memory data.
/// Matches SQL `COUNT(DISTINCT col)` semantics (NULLs excluded).
fn compute_ndistinct_from_typed(
    typed_cols: &[TypedColumn],
    columns: &[ColumnMeta],
) -> HashMap<String, i64> {
    let mut result = HashMap::new();
    for (i, col) in columns.iter().enumerate() {
        let nd = match &typed_cols[i] {
            TypedColumn::Int16(v) => {
                let set: HashSet<i16> = v.iter().flatten().copied().collect();
                set.len() as i64
            }
            TypedColumn::Int32(v) => {
                let set: HashSet<i32> = v.iter().flatten().copied().collect();
                set.len() as i64
            }
            TypedColumn::Int64(v) => {
                let set: HashSet<i64> = v.iter().flatten().copied().collect();
                set.len() as i64
            }
            TypedColumn::Float32(v) => {
                let set: HashSet<u32> = v.iter().flatten().map(|f| f.to_bits()).collect();
                set.len() as i64
            }
            TypedColumn::Float64(v) => {
                let set: HashSet<u64> = v.iter().flatten().map(|f| f.to_bits()).collect();
                set.len() as i64
            }
            TypedColumn::Bool(v) => {
                let set: HashSet<bool> = v.iter().flatten().copied().collect();
                set.len() as i64
            }
            TypedColumn::Text(v) => {
                let set: HashSet<&str> = v.iter().flatten().map(|s| s.as_str()).collect();
                set.len() as i64
            }
        };
        result.insert(col.name.clone(), nd);
    }
    result
}

/// Compress a partition using array_agg — single SPI call, bulk extraction.
/// Used for partitions WITHOUT segment_by (e.g., ClickBench).
/// Pushes column aggregation into PG's C code, avoiding per-row Rust overhead.
/// Also computes ndistinct from in-memory data (avoids expensive SQL COUNT(DISTINCT)).
fn compress_partition_array_agg(
    client: &mut SpiClient,
    part_fqn: &str,
    companion_fqn: &str,
    columns: &[ColumnMeta],
    order_by: &[String],
    segment_size: usize,
) -> (i64, HashMap<String, i64>) {
    let kinds: Vec<ColumnKind> = columns
        .iter()
        .map(|c| classify_column(&c.data_type, c.is_segment_by))
        .collect();

    // Build ORDER BY clause for array_agg
    let agg_order = if order_by.is_empty() {
        String::new()
    } else {
        format!(
            " ORDER BY {}",
            order_by
                .iter()
                .map(|o| format!("\"{}\"", o))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };

    // Build array_agg expressions — timestamps/dates converted to unix usec in SQL
    let agg_exprs: Vec<String> = columns
        .iter()
        .zip(kinds.iter())
        .map(|(col, kind)| {
            let expr = match kind {
                ColumnKind::Text => format!("\"{}\"::text", col.name),
                ColumnKind::Timestamp | ColumnKind::TimestampTz => {
                    format!(
                        "(extract(epoch from \"{}\") * 1000000)::bigint",
                        col.name
                    )
                }
                ColumnKind::Date => {
                    format!(
                        "(extract(epoch from \"{}\"::timestamp) * 1000000)::bigint",
                        col.name
                    )
                }
                _ => format!("\"{}\"", col.name),
            };
            format!("array_agg({}{})", expr, agg_order)
        })
        .collect();

    let sql = format!("SELECT {} FROM {}", agg_exprs.join(", "), part_fqn);
    let result = client
        .select(&sql, None, &[])
        .expect("array_agg query failed");

    let mut result_iter = result.into_iter();
    let row = match result_iter.next() {
        Some(row) => row,
        None => return (0, HashMap::new()),
    };

    let mut typed_cols = Vec::with_capacity(columns.len());
    for (i, kind) in kinds.iter().enumerate() {
        let ordinal = i + 1;
        let tc = match kind {
            ColumnKind::Int16 => TypedColumn::Int16(
                row.get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<Vec<Option<i16>>>()
                    .unwrap()
                    .unwrap_or_default(),
            ),
            ColumnKind::Int32 => TypedColumn::Int32(
                row.get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<Vec<Option<i32>>>()
                    .unwrap()
                    .unwrap_or_default(),
            ),
            ColumnKind::Int64
            | ColumnKind::Timestamp
            | ColumnKind::TimestampTz
            | ColumnKind::Date => TypedColumn::Int64(
                row.get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<Vec<Option<i64>>>()
                    .unwrap()
                    .unwrap_or_default(),
            ),
            ColumnKind::Float32 => TypedColumn::Float32(
                row.get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<Vec<Option<f32>>>()
                    .unwrap()
                    .unwrap_or_default(),
            ),
            ColumnKind::Float64 => TypedColumn::Float64(
                row.get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<Vec<Option<f64>>>()
                    .unwrap()
                    .unwrap_or_default(),
            ),
            ColumnKind::Bool => TypedColumn::Bool(
                row.get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<Vec<Option<bool>>>()
                    .unwrap()
                    .unwrap_or_default(),
            ),
            ColumnKind::Text => TypedColumn::Text(
                row.get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<Vec<Option<String>>>()
                    .unwrap()
                    .unwrap_or_default(),
            ),
        };
        typed_cols.push(tc);
    }

    // Compute ndistinct from in-memory data (avoids expensive SQL COUNT(DISTINCT))
    let ndistinct = compute_ndistinct_from_typed(&typed_cols, columns);

    // Determine total rows from first non-empty column
    let total_rows = typed_cols
        .iter()
        .filter_map(|tc| {
            let len = match tc {
                TypedColumn::Int16(v) => v.len(),
                TypedColumn::Int32(v) => v.len(),
                TypedColumn::Int64(v) => v.len(),
                TypedColumn::Float32(v) => v.len(),
                TypedColumn::Float64(v) => v.len(),
                TypedColumn::Bool(v) => v.len(),
                TypedColumn::Text(v) => v.len(),
            };
            if len > 0 {
                Some(len)
            } else {
                None
            }
        })
        .next()
        .unwrap_or(0);

    let compressed_size = flush_with_splitting(
        client,
        companion_fqn,
        columns,
        &typed_cols,
        &[],
        total_rows,
        segment_size,
    );

    (compressed_size, ndistinct)
}

/// Compress a partition using cursor-based streaming.
/// Used for partitions WITH segment_by columns.
/// Reads native PG datums directly — no text round-trip for numeric/timestamp types.
fn compress_partition_streaming(
    client: &mut SpiClient,
    part_fqn: &str,
    companion_fqn: &str,
    columns: &[ColumnMeta],
    order_by: &[String],
    segment_by: &[String],
    segment_size: usize,
) -> i64 {
    let batch_size = segment_size;

    // Classify columns for native datum extraction
    let kinds: Vec<ColumnKind> = columns
        .iter()
        .map(|c| classify_column(&c.data_type, c.is_segment_by))
        .collect();

    // Build SELECT list: segment_by and text-classified cols cast to ::text,
    // others as native types. The ::text cast is needed for CHAR/VARCHAR
    // columns which have different OIDs than text.
    let select_cols = columns
        .iter()
        .zip(kinds.iter())
        .map(|(c, kind)| {
            if c.is_segment_by || matches!(kind, ColumnKind::Text) {
                format!("\"{}\"::text", c.name)
            } else {
                format!("\"{}\"", c.name)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    // Build ORDER BY: segment_by cols first, then order_by cols
    let mut order_parts = Vec::new();
    for s in segment_by {
        order_parts.push(format!("\"{}\"", s));
    }
    for o in order_by {
        order_parts.push(format!("\"{}\"", o));
    }
    let order_clause = if order_parts.is_empty() {
        String::new()
    } else {
        format!(" ORDER BY {}", order_parts.join(", "))
    };

    // Segment_by column indices (for boundary detection)
    let seg_col_indices: Vec<usize> = columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.is_segment_by)
        .map(|(i, _)| i)
        .collect();

    // DECLARE CURSOR
    let cursor_sql = format!(
        "DECLARE comp_cursor CURSOR FOR SELECT {} FROM {}{}",
        select_cols, part_fqn, order_clause
    );
    client
        .update(&cursor_sql, None, &[])
        .expect("failed to declare cursor");

    let fetch_sql = format!("FETCH {} FROM comp_cursor", batch_size);

    let mut typed_cols = init_typed_columns(columns, &kinds);
    let mut current_seg_values: Vec<Option<String>> = Vec::new();
    let mut rows_in_segment: usize = 0;
    let mut total_compressed_size: i64 = 0;

    loop {
        let result = client
            .select(&fetch_sql, None, &[])
            .expect("failed to fetch from cursor");
        let fetched = result.len();
        if fetched == 0 {
            break;
        }

        for row in result {
            // Check segment_by boundary
            if !seg_col_indices.is_empty() {
                let row_seg_values: Vec<Option<String>> = seg_col_indices
                    .iter()
                    .map(|&i| {
                        row.get_datum_by_ordinal(i + 1)
                            .unwrap()
                            .value::<String>()
                            .unwrap()
                    })
                    .collect();

                if current_seg_values.is_empty() {
                    current_seg_values = row_seg_values;
                } else if row_seg_values != current_seg_values {
                    // Segment boundary — flush accumulated data
                    if rows_in_segment > 0 {
                        total_compressed_size += flush_with_splitting(
                            client,
                            companion_fqn,
                            columns,
                            &typed_cols,
                            &current_seg_values,
                            rows_in_segment,
                            segment_size,
                        );
                        typed_cols = init_typed_columns(columns, &kinds);
                        rows_in_segment = 0;
                    }
                    current_seg_values = row_seg_values;
                }
            }

            append_row_to_columns(&row, columns, &kinds, &mut typed_cols);
            rows_in_segment += 1;

            // Check segment_size limit
            if rows_in_segment >= segment_size {
                total_compressed_size += flush_segment_data(
                    client,
                    companion_fqn,
                    columns,
                    &typed_cols,
                    &current_seg_values,
                    rows_in_segment as u32,
                );
                typed_cols = init_typed_columns(columns, &kinds);
                rows_in_segment = 0;
            }
        }

        if fetched < batch_size {
            break;
        }
    }

    // Flush remaining
    if rows_in_segment > 0 {
        total_compressed_size += flush_with_splitting(
            client,
            companion_fqn,
            columns,
            &typed_cols,
            &current_seg_values,
            rows_in_segment,
            segment_size,
        );
    }

    client
        .update("CLOSE comp_cursor", None, &[])
        .expect("failed to close cursor");

    total_compressed_size
}

/// Compress a typed column directly, bypassing string parsing.
fn compress_typed_column(data: &TypedColumn, data_type: &str) -> Vec<u8> {
    match data {
        TypedColumn::Int16(values) => {
            let (non_null, null_bitmap) = compression::extract_nulls(values);
            let ints: Vec<i32> = non_null.iter().map(|&v| v as i32).collect();
            let (type_tag, encoded) = compression::bitpacked::best_encoding_i32(&ints);
            CompressedColumn {
                type_tag,
                row_count: values.len() as u32,
                null_bitmap,
                data: encoded,
            }
            .to_bytes()
        }
        TypedColumn::Int32(values) => {
            let (non_null, null_bitmap) = compression::extract_nulls(values);
            let (type_tag, encoded) = compression::bitpacked::best_encoding_i32(&non_null);
            CompressedColumn {
                type_tag,
                row_count: values.len() as u32,
                null_bitmap,
                data: encoded,
            }
            .to_bytes()
        }
        TypedColumn::Int64(values) => {
            let dt = data_type.to_lowercase();
            let (non_null, null_bitmap) = compression::extract_nulls(values);
            if dt.contains("timestamp") || dt == "date" {
                // Timestamp/date: use Gorilla timestamp encoding
                let data = compression::gorilla::encode_timestamps(&non_null);
                CompressedColumn {
                    type_tag: CompressionType::Gorilla,
                    row_count: values.len() as u32,
                    null_bitmap,
                    data,
                }
                .to_bytes()
            } else {
                // Integer: try Constant, FOR, DeltaVarint — pick smallest
                let (type_tag, encoded) = compression::bitpacked::best_encoding_i64(&non_null);
                CompressedColumn {
                    type_tag,
                    row_count: values.len() as u32,
                    null_bitmap,
                    data: encoded,
                }
                .to_bytes()
            }
        }
        TypedColumn::Float64(values) => {
            let (non_null, null_bitmap) = compression::extract_nulls(values);
            let encoded = compression::gorilla::encode_floats(&non_null);
            CompressedColumn {
                type_tag: CompressionType::Gorilla,
                row_count: values.len() as u32,
                null_bitmap,
                data: encoded,
            }
            .to_bytes()
        }
        TypedColumn::Float32(values) => {
            let (non_null, null_bitmap) = compression::extract_nulls(values);
            let encoded = compression::gorilla::encode_floats_f32(&non_null);
            CompressedColumn {
                type_tag: CompressionType::Gorilla,
                row_count: values.len() as u32,
                null_bitmap,
                data: encoded,
            }
            .to_bytes()
        }
        TypedColumn::Bool(values) => {
            let (non_null, null_bitmap) = compression::extract_nulls(values);
            let encoded = compression::boolean::encode(&non_null);
            CompressedColumn {
                type_tag: CompressionType::BooleanBitmap,
                row_count: values.len() as u32,
                null_bitmap,
                data: encoded,
            }
            .to_bytes()
        }
        TypedColumn::Text(values) => {
            // Delegate to existing string-based compression
            compress_column_values(values, data_type, "")
        }
    }
}

/// Compute min/max for typed columns, returning string representations for SQL INSERT.
fn compute_typed_minmax(data: &TypedColumn, data_type: &str) -> (Option<String>, Option<String>) {
    match data {
        TypedColumn::Int16(values) => {
            let mut min_v: Option<i16> = None;
            let mut max_v: Option<i16> = None;
            for v in values.iter().flatten() {
                min_v = Some(min_v.map_or(*v, |cur: i16| cur.min(*v)));
                max_v = Some(max_v.map_or(*v, |cur: i16| cur.max(*v)));
            }
            (min_v.map(|v| v.to_string()), max_v.map(|v| v.to_string()))
        }
        TypedColumn::Int32(values) => {
            let mut min_v: Option<i32> = None;
            let mut max_v: Option<i32> = None;
            for v in values.iter().flatten() {
                min_v = Some(min_v.map_or(*v, |cur: i32| cur.min(*v)));
                max_v = Some(max_v.map_or(*v, |cur: i32| cur.max(*v)));
            }
            (min_v.map(|v| v.to_string()), max_v.map(|v| v.to_string()))
        }
        TypedColumn::Int64(values) => {
            let mut min_v: Option<i64> = None;
            let mut max_v: Option<i64> = None;
            for v in values.iter().flatten() {
                min_v = Some(min_v.map_or(*v, |cur: i64| cur.min(*v)));
                max_v = Some(max_v.map_or(*v, |cur: i64| cur.max(*v)));
            }
            let dt = data_type.to_lowercase();
            if dt.contains("timestamp") {
                (
                    min_v.map(usec_to_timestamp_string),
                    max_v.map(usec_to_timestamp_string),
                )
            } else if dt == "date" {
                (
                    min_v.map(crate::timeparse::usec_to_date_string),
                    max_v.map(crate::timeparse::usec_to_date_string),
                )
            } else {
                (min_v.map(|v| v.to_string()), max_v.map(|v| v.to_string()))
            }
        }
        TypedColumn::Float64(values) => {
            let mut min_v: Option<f64> = None;
            let mut max_v: Option<f64> = None;
            for v in values.iter().flatten() {
                min_v = Some(min_v.map_or(*v, |cur: f64| if *v < cur { *v } else { cur }));
                max_v = Some(max_v.map_or(*v, |cur: f64| if *v > cur { *v } else { cur }));
            }
            (min_v.map(|v| v.to_string()), max_v.map(|v| v.to_string()))
        }
        TypedColumn::Float32(values) => {
            let mut min_v: Option<f32> = None;
            let mut max_v: Option<f32> = None;
            for v in values.iter().flatten() {
                min_v = Some(min_v.map_or(*v, |cur: f32| if *v < cur { *v } else { cur }));
                max_v = Some(max_v.map_or(*v, |cur: f32| if *v > cur { *v } else { cur }));
            }
            (min_v.map(|v| v.to_string()), max_v.map(|v| v.to_string()))
        }
        TypedColumn::Text(values) => compute_column_minmax(values, data_type),
        TypedColumn::Bool(_) => (None, None), // booleans don't support minmax
    }
}

/// Compress a column's values based on the PostgreSQL data type.
/// Only used for Text columns now — numeric/timestamp types go through compress_typed_column.
fn compress_column_values(values: &[Option<String>], _data_type: &str, _col_name: &str) -> Vec<u8> {
    // Only used for Text columns now — numeric/timestamp types go through compress_typed_column
    let (non_null, null_bitmap) = compression::extract_nulls(values);
    let refs: Vec<&str> = non_null.iter().map(|s| s.as_str()).collect();

    if compression::dictionary::should_use_dictionary(&refs) {
        let dict_encoded = compression::dictionary::encode(&refs);
        let lz4_encoded = compression::dictionary::encode_lz4(&refs);
        let (tag, encoded) = if lz4_encoded.len() < dict_encoded.len() {
            (CompressionType::DictionaryLz4, lz4_encoded)
        } else {
            (CompressionType::Dictionary, dict_encoded)
        };
        CompressedColumn {
            type_tag: tag,
            row_count: values.len() as u32,
            null_bitmap,
            data: encoded,
        }
        .to_bytes()
    } else {
        let encoded = compression::lz4::encode_blocked(&refs, compression::lz4::DEFAULT_BLOCK_SIZE);
        CompressedColumn {
            type_tag: CompressionType::Lz4Blocked,
            row_count: values.len() as u32,
            null_bitmap,
            data: encoded,
        }
        .to_bytes()
    }
}

/// Get column metadata for a table.
fn get_column_metadata(
    client: &SpiClient,
    schema: &str,
    table: &str,
    segment_by: &[String],
) -> Vec<ColumnMeta> {
    let result = client
        .select(
            "SELECT column_name::text, data_type::text
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
            None,
            &[schema.into(), table.into()],
        )
        .expect("failed to get columns");

    let mut columns = Vec::new();
    for row in result {
        let name: String = row.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap().unwrap();
        let data_type: String = row.get_datum_by_ordinal(2).unwrap().value::<String>().unwrap().unwrap();
        let is_segment = segment_by.contains(&name);
        columns.push(ColumnMeta {
            name,
            data_type,
            is_segment_by: is_segment,
        });
    }
    columns
}

/// Estimate raw table size in bytes.
fn estimate_raw_size(client: &SpiClient, table_fqn: &str) -> i64 {
    client
        .select(
            &format!("SELECT pg_total_relation_size('{}'::regclass)::int8", table_fqn),
            None,
            &[],
        )
        .expect("failed to get table size")
        .first()
        .get_one::<i64>()
        .unwrap()
        .unwrap_or(0)
}

// ============================================================================
// Decompression
// ============================================================================

fn decompress_partition_impl(client: &mut SpiClient, partition: &str) -> String {
    // Bypass the DML-on-compressed check for the INSERT we are about to do
    crate::scan::set_dml_bypass(true);
    let result = decompress_partition_inner(client, partition);
    crate::scan::set_dml_bypass(false);
    result
}

fn decompress_partition_inner(client: &mut SpiClient, partition: &str) -> String {
    // 1. Look up partition
    let (schema, part_table) = crate::partition::resolve_relation(client, partition);
    let part_info = catalog::get_partition_by_name(client, &schema, &part_table)
        .expect("failed to query partition")
        .unwrap_or_else(|| {
            pgrx::error!("pg_seaturtle: partition {}.{} not found in catalog", schema, part_table)
        });

    if !part_info.is_compressed {
        return format!("Partition {}.{} is not compressed", schema, part_table);
    }

    let ht = catalog::get_hypertable_by_id(client, part_info.hypertable_id)
        .expect("failed to query hypertable")
        .unwrap();

    // 2. Get column metadata (from the parent table, since partition is truncated)
    let columns = get_column_metadata(client, &ht.schema_name, &ht.table_name, &ht.segment_by);

    let companion_schema = "_seaturtle_compressed";
    let companion_fqn = format!("\"{}\".\"{}\"", companion_schema, part_table);
    let part_fqn = crate::partition::fqn(&schema, &part_table);

    // 3. Read compressed segments
    let mut select_cols = Vec::new();
    for col in &columns {
        if col.is_segment_by {
            select_cols.push(format!("\"{}\"::text", col.name));
        }
    }
    for col in &columns {
        if !col.is_segment_by {
            select_cols.push(format!("\"_{}_compressed\"", col.name));
        }
    }
    select_cols.push("_row_count".to_string());

    let read_query = format!(
        "SELECT {} FROM {}",
        select_cols.join(", "),
        companion_fqn
    );
    let segments = client.select(&read_query, None, &[]).expect("failed to read compressed data");

    let mut total_rows_restored = 0i64;

    for row in segments {
        let mut col_ordinal: usize = 1;
        let mut segment_by_vals: Vec<Option<String>> = Vec::new();
        let mut compressed_blobs: Vec<(String, String, Vec<u8>)> = Vec::new(); // (name, data_type, blob)

        // Read segment_by values
        for col in &columns {
            if col.is_segment_by {
                let val: Option<String> = row
                    .get_datum_by_ordinal(col_ordinal)
                    .unwrap()
                    .value::<String>()
                    .unwrap();
                segment_by_vals.push(val);
                col_ordinal += 1;
            }
        }

        // Read compressed blobs
        for col in &columns {
            if !col.is_segment_by {
                let blob: Option<Vec<u8>> = row
                    .get_datum_by_ordinal(col_ordinal)
                    .unwrap()
                    .value::<Vec<u8>>()
                    .unwrap();
                compressed_blobs.push((
                    col.name.clone(),
                    col.data_type.clone(),
                    blob.unwrap_or_default(),
                ));
                col_ordinal += 1;
            }
        }

        let segment_row_count: i32 = row
            .get_datum_by_ordinal(col_ordinal)
            .unwrap()
            .value::<i32>()
            .unwrap()
            .unwrap_or(0);

        if segment_row_count == 0 {
            continue;
        }

        // Decompress all columns
        let mut decompressed_cols: Vec<(String, Vec<Option<String>>)> = Vec::new();

        // Segment-by columns: repeat the value for every row
        let mut seg_idx = 0;
        for col in &columns {
            if col.is_segment_by {
                let val = &segment_by_vals[seg_idx];
                let repeated: Vec<Option<String>> =
                    (0..segment_row_count).map(|_| val.clone()).collect();
                decompressed_cols.push((col.name.clone(), repeated));
                seg_idx += 1;
            }
        }

        // Compressed columns: decompress
        for (name, data_type, blob) in &compressed_blobs {
            let values = decompress_column_values(blob, data_type);
            decompressed_cols.push((name.clone(), values));
        }

        // Sort columns back to original order
        let mut ordered_cols: Vec<(String, Vec<Option<String>>)> = Vec::new();
        for col in &columns {
            for dc in &decompressed_cols {
                if dc.0 == col.name {
                    ordered_cols.push(dc.clone());
                    break;
                }
            }
        }

        // INSERT rows back into partition
        let col_names: String = ordered_cols
            .iter()
            .map(|(name, _)| format!("\"{}\"", name))
            .collect::<Vec<_>>()
            .join(", ");

        const BATCH_SIZE: usize = 1000;
        let mut batch_start = 0;
        while batch_start < segment_row_count as usize {
            let batch_end = (batch_start + BATCH_SIZE).min(segment_row_count as usize);

            let mut all_row_values = Vec::with_capacity(batch_end - batch_start);
            for row_idx in batch_start..batch_end {
                let vals: Vec<String> = ordered_cols
                    .iter()
                    .enumerate()
                    .map(|(col_idx, (_, values))| {
                        let col_meta = &columns[col_idx];
                        match &values[row_idx] {
                            None => "NULL".to_string(),
                            Some(v) => format_value_for_insert(v, &col_meta.data_type),
                        }
                    })
                    .collect();
                all_row_values.push(format!("({})", vals.join(", ")));
            }

            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                part_fqn,
                col_names,
                all_row_values.join(", ")
            );
            client.update(&insert_sql, None, &[]).expect("failed to insert decompressed rows");

            batch_start = batch_end;
        }

        total_rows_restored += segment_row_count as i64;
    }

    // 4. Drop companion table
    client
        .update(&format!("DROP TABLE IF EXISTS {}", companion_fqn), None, &[])
        .expect("failed to drop companion table");

    // 5. Update catalog
    catalog::mark_partition_decompressed(client, part_info.id)
        .expect("failed to update catalog");

    crate::scan::invalidate_compressed_cache();

    format!(
        "Decompressed {}.{}: {} rows restored",
        schema, part_table, total_rows_restored
    )
}

/// Decompress a column blob back to string representations.
fn decompress_column_values(blob: &[u8], data_type: &str) -> Vec<Option<String>> {
    if blob.is_empty() {
        return Vec::new();
    }

    let cc = CompressedColumn::from_bytes(blob);
    let total_count = cc.row_count as usize;
    let dt = data_type.to_lowercase();

    match cc.type_tag {
        CompressionType::Gorilla => {
            if dt.contains("timestamp") || dt == "date" {
                let timestamps = compression::gorilla::decode_timestamps(&cc.data, count_non_null(&cc.null_bitmap, total_count));
                let strings: Vec<String> = if dt == "date" {
                    timestamps
                        .iter()
                        .map(|&usec| crate::timeparse::usec_to_date_string(usec))
                        .collect()
                } else {
                    timestamps
                        .iter()
                        .map(|&usec| usec_to_timestamp_string(usec))
                        .collect()
                };
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            } else if dt == "real" || dt == "float4" {
                let floats = compression::gorilla::decode_floats_f32(&cc.data, count_non_null(&cc.null_bitmap, total_count));
                let strings: Vec<String> = floats.iter().map(|v| v.to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            } else {
                let floats = compression::gorilla::decode_floats(&cc.data, count_non_null(&cc.null_bitmap, total_count));
                let strings: Vec<String> = floats.iter().map(|v| v.to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            }
        }
        CompressionType::DeltaVarint => {
            if dt == "smallint" || dt == "int2" {
                // Decode as i32 and downcast to i16
                let ints = compression::integer::decode_i32(&cc.data, count_non_null(&cc.null_bitmap, total_count));
                let strings: Vec<String> = ints.iter().map(|v| (*v as i16).to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            } else if dt == "integer" || dt == "int4" {
                let ints = compression::integer::decode_i32(&cc.data, count_non_null(&cc.null_bitmap, total_count));
                let strings: Vec<String> = ints.iter().map(|v| v.to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            } else {
                let ints = compression::integer::decode_i64(&cc.data, count_non_null(&cc.null_bitmap, total_count));
                let strings: Vec<String> = ints.iter().map(|v| v.to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            }
        }
        CompressionType::Dictionary => {
            let strings = compression::dictionary::decode(&cc.data, count_non_null(&cc.null_bitmap, total_count));
            compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
        }
        CompressionType::DictionaryLz4 => {
            let normalized = compression::dictionary::normalize_lz4(&cc.data);
            let strings = compression::dictionary::decode(&normalized, count_non_null(&cc.null_bitmap, total_count));
            compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
        }
        CompressionType::Lz4 => {
            let strings = compression::lz4::decode(&cc.data, count_non_null(&cc.null_bitmap, total_count));
            compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
        }
        CompressionType::Lz4Blocked => {
            let strings = compression::lz4::decode_blocked(&cc.data, count_non_null(&cc.null_bitmap, total_count));
            compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
        }
        CompressionType::BooleanBitmap => {
            let bools = compression::boolean::decode(&cc.data, count_non_null(&cc.null_bitmap, total_count));
            let strings: Vec<String> = bools.iter().map(|&b| if b { "t".to_string() } else { "f".to_string() }).collect();
            compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
        }
        CompressionType::Constant => {
            let non_null_count = count_non_null(&cc.null_bitmap, total_count);
            if dt == "smallint" || dt == "int2" {
                let ints = compression::bitpacked::decode_constant_i32(&cc.data, non_null_count);
                let strings: Vec<String> = ints.iter().map(|v| (*v as i16).to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            } else if dt == "integer" || dt == "int4" {
                let ints = compression::bitpacked::decode_constant_i32(&cc.data, non_null_count);
                let strings: Vec<String> = ints.iter().map(|v| v.to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            } else {
                let ints = compression::bitpacked::decode_constant_i64(&cc.data, non_null_count);
                let strings: Vec<String> = ints.iter().map(|v| v.to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            }
        }
        CompressionType::ForBitpacked => {
            let non_null_count = count_non_null(&cc.null_bitmap, total_count);
            if dt == "smallint" || dt == "int2" {
                let ints = compression::bitpacked::decode_for_i32(&cc.data, non_null_count);
                let strings: Vec<String> = ints.iter().map(|v| (*v as i16).to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            } else if dt == "integer" || dt == "int4" {
                let ints = compression::bitpacked::decode_for_i32(&cc.data, non_null_count);
                let strings: Vec<String> = ints.iter().map(|v| v.to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            } else {
                let ints = compression::bitpacked::decode_for_i64(&cc.data, non_null_count);
                let strings: Vec<String> = ints.iter().map(|v| v.to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            }
        }
    }
}

/// Count non-null values given a null bitmap and total count.
fn count_non_null(null_bitmap: &[u8], total_count: usize) -> usize {
    if null_bitmap.is_empty() {
        return total_count;
    }
    let null_count: usize = (0..total_count)
        .filter(|&i| (null_bitmap[i / 8] >> (i % 8)) & 1 == 1)
        .count();
    total_count - null_count
}

fn usec_to_timestamp_string(usec: i64) -> String {
    crate::timeparse::usec_to_timestamp_string(usec)
}

fn format_value_for_insert(value: &str, data_type: &str) -> String {
    let dt = data_type.to_lowercase();
    if dt.contains("timestamp") {
        format!("'{}'::timestamptz", value.replace('\'', "''"))
    } else if dt == "date" {
        format!("'{}'::date", value.replace('\'', "''"))
    } else if dt == "boolean" || dt == "bool" {
        if value == "t" || value == "true" || value == "1" {
            "true".to_string()
        } else {
            "false".to_string()
        }
    } else if dt == "integer" || dt == "int4" || dt == "bigint" || dt == "int8"
        || dt == "smallint" || dt == "int2"
        || dt == "double precision" || dt == "float8" || dt == "real" || dt == "float4"
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "''"))
    }
}

/// Check if a column type supports min/max metadata.
fn supports_minmax(data_type: &str) -> bool {
    let dt = data_type.to_lowercase();
    dt.contains("timestamp")
        || dt == "date"
        || dt == "integer" || dt == "int4"
        || dt == "bigint" || dt == "int8"
        || dt == "smallint" || dt == "int2"
        || dt == "double precision" || dt == "float8"
        || dt == "real" || dt == "float4"
}

/// Check if a column type supports sum metadata (numeric types only, not timestamps/dates).
fn supports_sum(data_type: &str) -> bool {
    let dt = data_type.to_lowercase();
    dt == "integer" || dt == "int4"
        || dt == "bigint" || dt == "int8"
        || dt == "smallint" || dt == "int2"
        || dt == "double precision" || dt == "float8"
        || dt == "real" || dt == "float4"
}

/// Check if a data type is a floating-point type.
fn is_float_type(data_type: &str) -> bool {
    let dt = data_type.to_lowercase();
    dt == "double precision" || dt == "float8" || dt == "real" || dt == "float4"
}

/// Compute sum and non-null count for a typed column.
/// Returns (sum_as_string, nonnull_count). Uses i128 for integer sums to avoid overflow.
fn compute_typed_sum(data: &TypedColumn) -> (Option<String>, i64) {
    match data {
        TypedColumn::Int16(v) => {
            let mut sum: i128 = 0;
            let mut count: i64 = 0;
            for val in v.iter().flatten() {
                sum += *val as i128;
                count += 1;
            }
            if count > 0 { (Some(sum.to_string()), count) } else { (None, 0) }
        }
        TypedColumn::Int32(v) => {
            let mut sum: i128 = 0;
            let mut count: i64 = 0;
            for val in v.iter().flatten() {
                sum += *val as i128;
                count += 1;
            }
            if count > 0 { (Some(sum.to_string()), count) } else { (None, 0) }
        }
        TypedColumn::Int64(v) => {
            let mut sum: i128 = 0;
            let mut count: i64 = 0;
            for val in v.iter().flatten() {
                sum += *val as i128;
                count += 1;
            }
            if count > 0 { (Some(sum.to_string()), count) } else { (None, 0) }
        }
        TypedColumn::Float32(v) => {
            let mut sum: f64 = 0.0;
            let mut count: i64 = 0;
            for val in v.iter().flatten() {
                sum += *val as f64;
                count += 1;
            }
            if count > 0 { (Some(format!("{:.17e}", sum)), count) } else { (None, 0) }
        }
        TypedColumn::Float64(v) => {
            let mut sum: f64 = 0.0;
            let mut count: i64 = 0;
            for val in v.iter().flatten() {
                sum += *val;
                count += 1;
            }
            if count > 0 { (Some(format!("{:.17e}", sum)), count) } else { (None, 0) }
        }
        TypedColumn::Text(_) | TypedColumn::Bool(_) => (None, 0),
    }
}

/// Compute the min and max of a column's string values using type-aware comparison.
fn compute_column_minmax(
    values: &[Option<String>],
    data_type: &str,
) -> (Option<String>, Option<String>) {
    let mut min_val: Option<&str> = None;
    let mut max_val: Option<&str> = None;

    for val in values.iter().flatten() {
        let v = val.as_str();
        min_val = Some(match min_val {
            None => v,
            Some(cur) => {
                if compare_values(v, cur, data_type) == std::cmp::Ordering::Less {
                    v
                } else {
                    cur
                }
            }
        });
        max_val = Some(match max_val {
            None => v,
            Some(cur) => {
                if compare_values(v, cur, data_type) == std::cmp::Ordering::Greater {
                    v
                } else {
                    cur
                }
            }
        });
    }

    (min_val.map(|s| s.to_string()), max_val.map(|s| s.to_string()))
}

/// Type-aware comparison of string-encoded values.
fn compare_values(a: &str, b: &str, data_type: &str) -> std::cmp::Ordering {
    let dt = data_type.to_lowercase();
    if dt.contains("timestamp") || dt == "date" {
        // ISO format sorts lexicographically
        a.cmp(b)
    } else if dt == "double precision" || dt == "float8" || dt == "real" || dt == "float4" {
        let fa: f64 = a.parse().unwrap_or(0.0);
        let fb: f64 = b.parse().unwrap_or(0.0);
        fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
    } else {
        // Integer types
        let ia: i64 = a.parse().unwrap_or(0);
        let ib: i64 = b.parse().unwrap_or(0);
        ia.cmp(&ib)
    }
}

/// Format a min/max value for SQL INSERT based on the column type.
fn format_minmax_for_insert(val: &str, data_type: &str) -> String {
    let dt = data_type.to_lowercase();
    if dt.contains("timestamp") {
        format!("'{}'::timestamptz", val.replace('\'', "''"))
    } else if dt == "date" {
        format!("'{}'::date", val.replace('\'', "''"))
    } else {
        // Numeric types — use the value directly
        val.to_string()
    }
}

/// Public function used by the background worker for auto-compression.
pub fn auto_compress_partitions(client: &mut SpiClient<'_>, ht: &catalog::HypertableInfo) -> i32 {
    let compress_after = match &ht.compress_after {
        Some(interval) => interval,
        None => return 0,
    };

    if ht.order_by.is_empty() && ht.segment_by.is_empty() {
        return 0;
    }

    // Find partitions eligible for compression:
    // range_end < now() - compress_after AND NOT is_compressed
    let eligible = client
        .select(
            "SELECT table_name FROM seaturtle_partition
             WHERE hypertable_id = $1 AND is_compressed = false
               AND range_end < now() - $2::interval",
            None,
            &[ht.id.into(), (*compress_after).into()],
        )
        .expect("failed to query eligible partitions");

    let mut partition_names: Vec<String> = Vec::new();
    for row in eligible {
        let name: String = row
            .get_datum_by_ordinal(1)
            .unwrap()
            .value::<String>()
            .unwrap()
            .unwrap();
        partition_names.push(name);
    }

    let mut compressed = 0;
    for name in &partition_names {
        compress_partition_impl(client, name);
        compressed += 1;
    }

    compressed
}
