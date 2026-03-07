use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use crate::catalog;
use crate::compression::{self, CompressionType, CompressedColumn};

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

        catalog::update_hypertable_compression(client, ht.id, &segment_by, &effective_order_by)
            .expect("failed to update compression settings");

        format!(
            "Compression enabled on {}.{} (segment_by: {:?}, order_by: {:?})",
            schema, table, segment_by, effective_order_by
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
    create_cols.push("_row_count INT".to_string());

    let create_ddl = format!(
        "CREATE TABLE {} ({})",
        companion_fqn,
        create_cols.join(", ")
    );
    client.update(&create_ddl, None, &[]).expect("failed to create companion table");

    // 6. Build ORDER BY clause
    let order_clause = if !ht.order_by.is_empty() {
        format!(
            "ORDER BY {}",
            ht.order_by
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ")
        )
    } else {
        String::new()
    };

    // 7. Read and compress data per segment
    let mut total_compressed_size: i64 = 0;
    let raw_size = estimate_raw_size(client, &part_fqn);

    if ht.segment_by.is_empty() {
        // No segment_by: entire partition is one segment (split at 100k rows)
        let total = compress_segment(
            client,
            &part_fqn,
            &companion_fqn,
            &columns,
            "TRUE",
            &order_clause,
            &[],
        );
        total_compressed_size += total;
    } else {
        total_compressed_size += compress_segments_single_pass(
            client,
            &part_fqn,
            &companion_fqn,
            &columns,
            &order_clause,
            &ht.segment_by,
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
    Text,       // text, varchar, char, timestamp, date — read as ::text String
    Int16,      // smallint/int2
    Int32,      // integer/int4
    Int64,      // bigint/int8
    Float32,    // real/float4
    Float64,    // double precision/float8
    Bool,       // boolean/bool
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
    } else {
        ColumnKind::Text // timestamp, date, text, varchar, etc.
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
    }
}

// Delimiter and null marker for string_agg-based bulk column reading.
// ASCII Record Separator and Unit Separator — control chars that don't appear in normal text data.
const AGG_DELIM: char = '\x1E';
const AGG_NULL: &str = "\x1F";

/// Build a `string_agg(COALESCE("col"::text, null_marker), delim ORDER BY ...)` expression.
fn build_string_agg_expr(col_name: &str, order_expr: &str) -> String {
    if order_expr.is_empty() {
        format!(
            "string_agg(COALESCE(\"{}\"::text, E'\\x1F'), E'\\x1E')",
            col_name
        )
    } else {
        format!(
            "string_agg(COALESCE(\"{}\"::text, E'\\x1F'), E'\\x1E' ORDER BY {})",
            col_name, order_expr
        )
    }
}

/// Parse a delimited aggregate string into a TypedColumn.
/// For numeric types, parses directly from `&str` slices (no String allocation).
fn parse_agg_to_typed(agg_str: &str, data_type: &str) -> TypedColumn {
    let dt = data_type.to_lowercase();

    if dt == "smallint" || dt == "int2" {
        TypedColumn::Int16(
            agg_str
                .split(AGG_DELIM)
                .map(|s| {
                    if s == AGG_NULL {
                        None
                    } else {
                        Some(s.parse::<i16>().unwrap_or(0))
                    }
                })
                .collect(),
        )
    } else if dt == "integer" || dt == "int4" {
        TypedColumn::Int32(
            agg_str
                .split(AGG_DELIM)
                .map(|s| {
                    if s == AGG_NULL {
                        None
                    } else {
                        Some(s.parse::<i32>().unwrap_or(0))
                    }
                })
                .collect(),
        )
    } else if dt == "bigint" || dt == "int8" {
        TypedColumn::Int64(
            agg_str
                .split(AGG_DELIM)
                .map(|s| {
                    if s == AGG_NULL {
                        None
                    } else {
                        Some(s.parse::<i64>().unwrap_or(0))
                    }
                })
                .collect(),
        )
    } else if dt == "double precision" || dt == "float8" {
        TypedColumn::Float64(
            agg_str
                .split(AGG_DELIM)
                .map(|s| {
                    if s == AGG_NULL {
                        None
                    } else {
                        Some(s.parse::<f64>().unwrap_or(0.0))
                    }
                })
                .collect(),
        )
    } else if dt == "real" || dt == "float4" {
        TypedColumn::Float32(
            agg_str
                .split(AGG_DELIM)
                .map(|s| {
                    if s == AGG_NULL {
                        None
                    } else {
                        Some(s.parse::<f32>().unwrap_or(0.0))
                    }
                })
                .collect(),
        )
    } else if dt == "boolean" || dt == "bool" {
        TypedColumn::Bool(
            agg_str
                .split(AGG_DELIM)
                .map(|s| {
                    if s == AGG_NULL {
                        None
                    } else {
                        Some(s == "t" || s == "true" || s == "1")
                    }
                })
                .collect(),
        )
    } else if dt.contains("timestamp") {
        // Timestamps: parse to i64 microseconds via our fast parser, store as Int64
        // for direct Gorilla encoding without re-parsing
        TypedColumn::Text(
            agg_str
                .split(AGG_DELIM)
                .map(|s| {
                    if s == AGG_NULL {
                        None
                    } else {
                        Some(s.to_string())
                    }
                })
                .collect(),
        )
    } else {
        // text, date, varchar, etc.
        TypedColumn::Text(
            agg_str
                .split(AGG_DELIM)
                .map(|s| {
                    if s == AGG_NULL {
                        None
                    } else {
                        Some(s.to_string())
                    }
                })
                .collect(),
        )
    }
}

/// Get the number of rows in a TypedColumn.
fn typed_column_len(tc: &TypedColumn) -> usize {
    match tc {
        TypedColumn::Text(v) => v.len(),
        TypedColumn::Int16(v) => v.len(),
        TypedColumn::Int32(v) => v.len(),
        TypedColumn::Int64(v) => v.len(),
        TypedColumn::Float32(v) => v.len(),
        TypedColumn::Float64(v) => v.len(),
        TypedColumn::Bool(v) => v.len(),
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

    for (i, col) in columns.iter().enumerate() {
        if col.is_segment_by {
            continue;
        }
        let compressed = compress_typed_column(&typed_cols[i], &col.data_type);
        if supports_minmax(&col.data_type) {
            let (min_val, max_val) = compute_typed_minmax(&typed_cols[i], &col.data_type);
            col_minmax.insert(col.name.clone(), (min_val, max_val));
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

/// Compress all segments using GROUP BY + string_agg for bulk column reading.
///
/// Issues a single `SELECT seg_col, string_agg(col1, ...), ... GROUP BY seg_col`
/// query. Each result row is one complete segment — no per-row SPI overhead.
fn compress_segments_single_pass(
    client: &mut SpiClient,
    part_fqn: &str,
    companion_fqn: &str,
    columns: &[ColumnMeta],
    order_clause: &str,
    segment_by: &[String],
) -> i64 {
    let order_expr = order_clause
        .trim_start_matches("ORDER BY ")
        .trim();

    // Segment-by columns as plain values in SELECT + GROUP BY
    let seg_select: String = segment_by
        .iter()
        .map(|c| format!("\"{}\"::text", c))
        .collect::<Vec<_>>()
        .join(", ");
    let seg_group: String = segment_by
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    // Non-segment columns as string_agg expressions
    let agg_list: String = columns
        .iter()
        .filter(|c| !c.is_segment_by)
        .map(|c| build_string_agg_expr(&c.name, order_expr))
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        "SELECT {}, {} FROM {} GROUP BY {}",
        seg_select, agg_list, part_fqn, seg_group
    );
    let result = client
        .select(&query, None, &[])
        .expect("failed to read partition data");

    let num_seg_cols = segment_by.len();
    let mut total_compressed_size: i64 = 0;

    for row in result {
        // Extract segment_by values (first N columns)
        let seg_values: Vec<Option<String>> = (0..num_seg_cols)
            .map(|i| {
                row.get_datum_by_ordinal(i + 1)
                    .unwrap()
                    .value::<String>()
                    .unwrap()
            })
            .collect();

        // Extract aggregated column strings (remaining columns)
        let mut typed_cols: Vec<TypedColumn> = Vec::with_capacity(columns.len());
        let mut agg_ordinal = num_seg_cols + 1;

        let mut raw_strings: Vec<Option<String>> = Vec::with_capacity(columns.len());
        for col in columns {
            if col.is_segment_by {
                continue;
            }
            let s = row
                .get_datum_by_ordinal(agg_ordinal)
                .unwrap()
                .value::<String>()
                .unwrap();
            raw_strings.push(s);
            agg_ordinal += 1;
        }

        let mut agg_idx = 0;
        for col in columns {
            if col.is_segment_by {
                typed_cols.push(TypedColumn::Text(Vec::new()));
                continue;
            }
            let tc = match &raw_strings[agg_idx] {
                Some(agg_str) => parse_agg_to_typed(agg_str, &col.data_type),
                None => new_typed_column(classify_column(&col.data_type, false)),
            };
            typed_cols.push(tc);
            agg_idx += 1;
        }

        let seg_row_count = typed_cols
            .iter()
            .map(typed_column_len)
            .max()
            .unwrap_or(0);
        if seg_row_count == 0 {
            continue;
        }

        total_compressed_size += flush_segment_data(
            client,
            companion_fqn,
            columns,
            &typed_cols,
            &seg_values,
            seg_row_count as u32,
        );
    }

    total_compressed_size
}

/// Compress a typed column directly, bypassing string parsing.
fn compress_typed_column(data: &TypedColumn, data_type: &str) -> Vec<u8> {
    match data {
        TypedColumn::Int16(values) => {
            let (non_null, null_bitmap) = compression::extract_nulls(values);
            let ints: Vec<i32> = non_null.iter().map(|&v| v as i32).collect();
            let encoded = compression::integer::encode_i32(&ints);
            CompressedColumn {
                type_tag: CompressionType::DeltaVarint,
                row_count: values.len() as u32,
                null_bitmap,
                data: encoded,
            }
            .to_bytes()
        }
        TypedColumn::Int32(values) => {
            let (non_null, null_bitmap) = compression::extract_nulls(values);
            let encoded = compression::integer::encode_i32(&non_null);
            CompressedColumn {
                type_tag: CompressionType::DeltaVarint,
                row_count: values.len() as u32,
                null_bitmap,
                data: encoded,
            }
            .to_bytes()
        }
        TypedColumn::Int64(values) => {
            let (non_null, null_bitmap) = compression::extract_nulls(values);
            let encoded = compression::integer::encode_i64(&non_null);
            CompressedColumn {
                type_tag: CompressionType::DeltaVarint,
                row_count: values.len() as u32,
                null_bitmap,
                data: encoded,
            }
            .to_bytes()
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
            (min_v.map(|v| v.to_string()), max_v.map(|v| v.to_string()))
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

/// Compress a single segment (no segment_by) using string_agg for bulk column reading.
/// One SQL aggregate per column → 105 datum extractions instead of rows×columns.
fn compress_segment(
    client: &mut SpiClient,
    part_fqn: &str,
    companion_fqn: &str,
    columns: &[ColumnMeta],
    where_clause: &str,
    order_clause: &str,
    _segment_by: &[String],
) -> i64 {
    let order_expr = order_clause
        .trim_start_matches("ORDER BY ")
        .trim();

    let agg_list: String = columns
        .iter()
        .map(|c| build_string_agg_expr(&c.name, order_expr))
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        "SELECT {} FROM {} WHERE {}",
        agg_list, part_fqn, where_clause
    );

    let result = client
        .select(&query, None, &[])
        .expect("failed to read segment data");

    let mut typed_cols: Vec<TypedColumn> = Vec::new();
    if let Some(row) = result.into_iter().next() {
        typed_cols = columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                match row
                    .get_datum_by_ordinal(i + 1)
                    .unwrap()
                    .value::<String>()
                    .unwrap()
                {
                    Some(agg_str) => parse_agg_to_typed(&agg_str, &col.data_type),
                    None => new_typed_column(classify_column(&col.data_type, col.is_segment_by)),
                }
            })
            .collect();
    }

    let row_count = typed_cols
        .iter()
        .map(typed_column_len)
        .max()
        .unwrap_or(0);
    if row_count == 0 {
        return 0;
    }

    flush_segment_data(
        client,
        companion_fqn,
        columns,
        &typed_cols,
        &[],
        row_count as u32,
    )
}

/// Compress a column's values based on the PostgreSQL data type.
fn compress_column_values(values: &[Option<String>], data_type: &str, _col_name: &str) -> Vec<u8> {
    let dt = data_type.to_lowercase();

    if dt.contains("timestamp") {
        // Parse as i64 microseconds and use Gorilla timestamp encoding
        let (non_null, null_bitmap) = compression::extract_nulls(values);
        let timestamps: Vec<i64> = non_null
            .iter()
            .map(|v| parse_timestamp_to_usec(v))
            .collect();
        let data = compression::gorilla::encode_timestamps(&timestamps);
        CompressedColumn {
            type_tag: CompressionType::Gorilla,
            row_count: values.len() as u32,
            null_bitmap,
            data,
        }
        .to_bytes()
    } else if dt == "double precision" || dt == "float8" || dt == "real" || dt == "float4" {
        let (non_null, null_bitmap) = compression::extract_nulls(values);
        if dt == "real" || dt == "float4" {
            let floats: Vec<f32> = non_null.iter().map(|v| v.parse::<f32>().unwrap_or(0.0)).collect();
            let data = compression::gorilla::encode_floats_f32(&floats);
            CompressedColumn {
                type_tag: CompressionType::Gorilla,
                row_count: values.len() as u32,
                null_bitmap,
                data,
            }
            .to_bytes()
        } else {
            let floats: Vec<f64> = non_null.iter().map(|v| v.parse::<f64>().unwrap_or(0.0)).collect();
            let data = compression::gorilla::encode_floats(&floats);
            CompressedColumn {
                type_tag: CompressionType::Gorilla,
                row_count: values.len() as u32,
                null_bitmap,
                data,
            }
            .to_bytes()
        }
    } else if dt == "smallint" || dt == "int2" {
        // Upcast smallint to i32 for delta-varint encoding
        let (non_null, null_bitmap) = compression::extract_nulls(values);
        let ints: Vec<i32> = non_null.iter().map(|v| v.parse::<i16>().unwrap_or(0) as i32).collect();
        let data = compression::integer::encode_i32(&ints);
        CompressedColumn {
            type_tag: CompressionType::DeltaVarint,
            row_count: values.len() as u32,
            null_bitmap,
            data,
        }
        .to_bytes()
    } else if dt == "date" {
        // Treat date as timestamp for Gorilla encoding
        let (non_null, null_bitmap) = compression::extract_nulls(values);
        let timestamps: Vec<i64> = non_null
            .iter()
            .map(|v| parse_timestamp_to_usec(v))
            .collect();
        let data = compression::gorilla::encode_timestamps(&timestamps);
        CompressedColumn {
            type_tag: CompressionType::Gorilla,
            row_count: values.len() as u32,
            null_bitmap,
            data,
        }
        .to_bytes()
    } else if dt == "integer" || dt == "int4" {
        let (non_null, null_bitmap) = compression::extract_nulls(values);
        let ints: Vec<i32> = non_null.iter().map(|v| v.parse::<i32>().unwrap_or(0)).collect();
        let data = compression::integer::encode_i32(&ints);
        CompressedColumn {
            type_tag: CompressionType::DeltaVarint,
            row_count: values.len() as u32,
            null_bitmap,
            data,
        }
        .to_bytes()
    } else if dt == "bigint" || dt == "int8" {
        let (non_null, null_bitmap) = compression::extract_nulls(values);
        let ints: Vec<i64> = non_null.iter().map(|v| v.parse::<i64>().unwrap_or(0)).collect();
        let data = compression::integer::encode_i64(&ints);
        CompressedColumn {
            type_tag: CompressionType::DeltaVarint,
            row_count: values.len() as u32,
            null_bitmap,
            data,
        }
        .to_bytes()
    } else if dt == "boolean" || dt == "bool" {
        let (non_null, null_bitmap) = compression::extract_nulls(values);
        let bools: Vec<bool> = non_null.iter().map(|v| v == "t" || v == "true" || v == "1").collect();
        let data = compression::boolean::encode(&bools);
        CompressedColumn {
            type_tag: CompressionType::BooleanBitmap,
            row_count: values.len() as u32,
            null_bitmap,
            data,
        }
        .to_bytes()
    } else {
        // TEXT and other types
        let (non_null, null_bitmap) = compression::extract_nulls(values);
        let refs: Vec<&str> = non_null.iter().map(|s| s.as_str()).collect();

        if compression::dictionary::should_use_dictionary(&refs) {
            let encoded = compression::dictionary::encode(&refs);
            // Wrap with Dictionary tag
            CompressedColumn {
                type_tag: CompressionType::Dictionary,
                row_count: values.len() as u32,
                null_bitmap,
                data: encoded,
            }
            .to_bytes()
        } else {
            let encoded = compression::lz4::encode(&refs);
            CompressedColumn {
                type_tag: CompressionType::Lz4,
                row_count: values.len() as u32,
                null_bitmap,
                data: encoded,
            }
            .to_bytes()
        }
    }
}

fn parse_timestamp_to_usec(s: &str) -> i64 {
    crate::timeparse::parse_timestamp_to_usec(s)
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
        CompressionType::Lz4 => {
            let strings = compression::lz4::decode(&cc.data, count_non_null(&cc.null_bitmap, total_count));
            compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
        }
        CompressionType::BooleanBitmap => {
            let bools = compression::boolean::decode(&cc.data, count_non_null(&cc.null_bitmap, total_count));
            let strings: Vec<String> = bools.iter().map(|&b| if b { "t".to_string() } else { "f".to_string() }).collect();
            compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
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
