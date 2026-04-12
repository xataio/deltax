use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use std::hash::{Hash, Hasher};

use cardinality_estimator::CardinalityEstimator;

use crate::catalog;
use crate::compression::{self, CompressionType, CompressedColumn};

/// Microseconds between Unix epoch (1970-01-01) and PG epoch (2000-01-01).
pub(crate) const PG_EPOCH_OFFSET_USEC: i64 = 946_684_800_000_000;
/// Days between Unix epoch (1970-01-01) and PG epoch (2000-01-01).
pub(crate) const PG_EPOCH_OFFSET_DAYS: i64 = 10_957;

/// Column metadata from information_schema.
#[derive(Debug, Clone)]
pub(crate) struct ColumnMeta {
    pub(crate) name: String,
    pub(crate) data_type: String,
    pub(crate) is_segment_by: bool,
    pub(crate) is_time_column: bool,
}

// ============================================================================
// SQL-callable functions
// ============================================================================

/// Enable compression on a deltax deltatable.
///
/// ```sql
/// SELECT deltax_enable_compression('metrics',
///     segment_by => ARRAY['device_id'],
///     order_by => ARRAY['ts']);
/// ```
#[pg_extern]
fn deltax_enable_compression(
    relation: &str,
    segment_by: default!(Vec<String>, "ARRAY[]::text[]"),
    order_by: default!(Vec<String>, "ARRAY[]::text[]"),
    segment_size: default!(i32, "30000"),
) -> String {
    Spi::connect_mut(|client| {
        let (schema, table) = crate::partition::resolve_relation(client, relation);
        let ht = catalog::get_deltatable(client, &schema, &table)
            .expect("failed to query deltatable")
            .unwrap_or_else(|| {
                pgrx::error!("pg_deltax: table {}.{} is not a deltax table", schema, table)
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
                pgrx::error!("pg_deltax: segment_by column '{}' not found in {}.{}", col, schema, table);
            }
        }

        // If order_by is empty, default to the time column
        let effective_order_by = if order_by.is_empty() {
            vec![ht.time_column.clone()]
        } else {
            order_by
        };

        let effective_segment_size = if segment_size <= 0 { 30000 } else { segment_size };

        catalog::update_deltatable_compression(client, ht.id, &segment_by, &effective_order_by, effective_segment_size)
            .expect("failed to update compression settings");

        format!(
            "Compression enabled on {}.{} (segment_by: {:?}, order_by: {:?}, segment_size: {})",
            schema, table, segment_by, effective_order_by, effective_segment_size
        )
    })
}

/// Set the automatic compression policy for a deltatable.
#[pg_extern]
fn deltax_set_compression_policy(
    relation: &str,
    compress_after: pgrx::datum::Interval,
) -> String {
    Spi::connect_mut(|client| {
        let (schema, table) = crate::partition::resolve_relation(client, relation);
        let ht = catalog::get_deltatable(client, &schema, &table)
            .expect("failed to query deltatable")
            .unwrap_or_else(|| {
                pgrx::error!("pg_deltax: table {}.{} is not a deltax table", schema, table)
            });

        if ht.segment_by.is_empty() && ht.order_by.is_empty() {
            pgrx::error!("pg_deltax: enable compression first with deltax_enable_compression()");
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
fn deltax_compress_partition(partition: &str) -> String {
    Spi::connect_mut(|client| {
        compress_partition_impl(client, partition)
    })
}

/// Decompress a single partition.
#[pg_extern]
fn deltax_decompress_partition(partition: &str) -> String {
    Spi::connect_mut(|client| {
        decompress_partition_impl(client, partition)
    })
}

/// Show compression statistics for a deltatable.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn deltax_compression_stats(
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
        let ht = catalog::get_deltatable(client, &schema, &table)
            .expect("failed to query deltatable")
            .unwrap_or_else(|| {
                pgrx::error!("pg_deltax: table {}.{} is not a deltax table", schema, table)
            });

        let result = client
            .select(
                "SELECT table_name, is_compressed, raw_size, compressed_size, row_count
                 FROM deltax_partition
                 WHERE deltatable_id = $1
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

/// Return the total on-disk size of a deltatable in bytes.
///
/// For compressed partitions, uses the stored `compressed_size` from the catalog.
/// For uncompressed partitions, uses `pg_total_relation_size`.
#[pg_extern]
fn deltax_table_size(relation: &str) -> i64 {
    Spi::connect(|client| {
        let (schema, table) = crate::partition::resolve_relation(client, relation);
        let ht = catalog::get_deltatable(client, &schema, &table)
            .expect("failed to query deltatable")
            .unwrap_or_else(|| {
                pgrx::error!("pg_deltax: table {}.{} is not a deltax table", schema, table)
            });

        let result = client
            .select(
                "SELECT table_name, is_compressed
                 FROM deltax_partition
                 WHERE deltatable_id = $1",
                None,
                &[ht.id.into()],
            )
            .expect("failed to query partitions");

        let companion_schema = "_deltax_compressed";
        let mut total: i64 = 0;
        for row in result {
            let part_name: String = row.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap().unwrap();
            let compressed: bool = row.get_datum_by_ordinal(2).unwrap().value::<bool>().unwrap().unwrap_or(false);

            if compressed {
                // Measure live size of companion tables
                for suffix in &["meta", "blobs", "blooms"] {
                    let fqn = format!("\"{}\".\"{}_{}\"", companion_schema, part_name, suffix);
                    total += estimate_raw_size(client, &fqn);
                }
            } else {
                let fqn = crate::partition::fqn(&schema, &part_name);
                total += estimate_raw_size(client, &fqn);
            }
        }
        total
    })
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
            pgrx::error!("pg_deltax: partition {}.{} not found in catalog", schema, part_table)
        });

    if part_info.is_compressed {
        return format!("Partition {}.{} is already compressed", schema, part_table);
    }

    // 2. Get deltatable info (compression settings)
    let ht = catalog::get_deltatable_by_id(client, part_info.deltatable_id)
        .expect("failed to query deltatable")
        .unwrap();

    if ht.order_by.is_empty() && ht.segment_by.is_empty() {
        pgrx::error!("pg_deltax: compression not enabled on {}.{}. Call deltax_enable_compression() first.",
            ht.schema_name, ht.table_name);
    }

    // 3. Get column metadata
    let columns = get_column_metadata(client, &schema, &part_table, &ht.segment_by, &ht.time_column);
    if columns.is_empty() {
        pgrx::error!("pg_deltax: no columns found for {}.{}", schema, part_table);
    }

    // 4. Estimate row count from pg_class stats (instant, no scan).
    // Used for skipping empty partitions.
    // reltuples: 0 = empty (known), -1 = unknown (no ANALYZE yet), >0 = estimated count.
    let part_fqn = crate::partition::fqn(&schema, &part_table);
    let reltuples = client
        .select(
            &format!(
                "SELECT reltuples::int8 FROM pg_class WHERE oid = '{}'::regclass",
                part_fqn
            ),
            None,
            &[],
        )
        .expect("failed to get reltuples")
        .first()
        .get_one::<i64>()
        .unwrap()
        .unwrap_or(-1);

    // reltuples = 0 means PG knows the partition is empty (e.g. freshly created).
    // Skip compression — creating the companion table would confuse the scan hook.
    if reltuples == 0 {
        return format!("Partition {}.{} has no rows to compress", schema, part_table);
    }

    // 5. Build companion table DDL: meta (thin) + colstats (wide) + blobs + blooms
    let ddl = build_companion_ddl(&part_table, &columns);
    // NOTE: table creation is deferred until we confirm data exists.
    // Creating it early would cause the scan hook to intercept queries on the partition
    // (it checks for meta table existence, not is_compressed in the catalog).

    // 6. Read and compress data per segment
    let raw_size = estimate_raw_size(client, &part_fqn);

    let segment_size = ht.segment_size as usize;

    let (total_compressed_size, row_count) = compress_partition_streaming(
        client,
        &part_fqn,
        &ddl,
        &columns,
        &ht.order_by,
        &ht.segment_by,
        segment_size,
    );

    // Empty partition — clean up tables and return
    if row_count == 0 {
        client
            .update(&format!("DROP TABLE IF EXISTS {}", ddl.meta_fqn), None, &[])
            .expect("failed to drop empty meta table");
        client
            .update(&format!("DROP TABLE IF EXISTS {}", ddl.colstats_fqn), None, &[])
            .expect("failed to drop empty colstats table");
        client
            .update(&format!("DROP TABLE IF EXISTS {}", ddl.blobs_fqn), None, &[])
            .expect("failed to drop empty blobs table");
        client
            .update(&format!("DROP TABLE IF EXISTS {}", ddl.blooms_fqn), None, &[])
            .expect("failed to drop empty blooms table");
        return format!("Partition {}.{} has no rows to compress", schema, part_table);
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

    // Persist per-column max ndistinct so planner cost estimation can do
    // a catalog lookup instead of a cold full scan of the colstats table.
    let nd_col_names: Vec<String> = columns
        .iter()
        .filter(|c| !c.is_segment_by)
        .map(|c| c.name.clone())
        .collect();
    catalog::update_partition_column_ndistinct(client, part_info.id, &ddl.colstats_fqn, &nd_col_names)
        .expect("failed to update partition column_ndistinct");

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
pub(crate) enum ColumnKind {
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
#[derive(Debug, PartialEq)]
pub(crate) enum TypedColumn {
    Text(Vec<Option<String>>),
    Int16(Vec<Option<i16>>),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Float32(Vec<Option<f32>>),
    Float64(Vec<Option<f64>>),
    Bool(Vec<Option<bool>>),
}

impl TypedColumn {
    /// Split off elements from index `at` onward, returning them as a new TypedColumn.
    /// `self` retains elements `0..at`.
    pub(crate) fn split_off(&mut self, at: usize) -> Self {
        match self {
            TypedColumn::Text(v) => TypedColumn::Text(v.split_off(at)),
            TypedColumn::Int16(v) => TypedColumn::Int16(v.split_off(at)),
            TypedColumn::Int32(v) => TypedColumn::Int32(v.split_off(at)),
            TypedColumn::Int64(v) => TypedColumn::Int64(v.split_off(at)),
            TypedColumn::Float32(v) => TypedColumn::Float32(v.split_off(at)),
            TypedColumn::Float64(v) => TypedColumn::Float64(v.split_off(at)),
            TypedColumn::Bool(v) => TypedColumn::Bool(v.split_off(at)),
        }
    }

    pub(crate) fn extend(&mut self, other: Self) {
        match (self, other) {
            (TypedColumn::Text(a), TypedColumn::Text(b)) => a.extend(b),
            (TypedColumn::Int16(a), TypedColumn::Int16(b)) => a.extend(b),
            (TypedColumn::Int32(a), TypedColumn::Int32(b)) => a.extend(b),
            (TypedColumn::Int64(a), TypedColumn::Int64(b)) => a.extend(b),
            (TypedColumn::Float32(a), TypedColumn::Float32(b)) => a.extend(b),
            (TypedColumn::Float64(a), TypedColumn::Float64(b)) => a.extend(b),
            (TypedColumn::Bool(a), TypedColumn::Bool(b)) => a.extend(b),
            _ => panic!("TypedColumn::extend: mismatched variants"),
        }
    }

    /// Push a single row from `src` at index `idx` into `self`.
    pub(crate) fn push_from(&mut self, src: &Self, idx: usize) {
        match (self, src) {
            (TypedColumn::Text(dst), TypedColumn::Text(s)) => dst.push(s[idx].clone()),
            (TypedColumn::Int16(dst), TypedColumn::Int16(s)) => dst.push(s[idx]),
            (TypedColumn::Int32(dst), TypedColumn::Int32(s)) => dst.push(s[idx]),
            (TypedColumn::Int64(dst), TypedColumn::Int64(s)) => dst.push(s[idx]),
            (TypedColumn::Float32(dst), TypedColumn::Float32(s)) => dst.push(s[idx]),
            (TypedColumn::Float64(dst), TypedColumn::Float64(s)) => dst.push(s[idx]),
            (TypedColumn::Bool(dst), TypedColumn::Bool(s)) => dst.push(s[idx]),
            _ => panic!("TypedColumn::push_from: mismatched variants"),
        }
    }
}

pub(crate) fn classify_column(data_type: &str, is_segment_by: bool) -> ColumnKind {
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

pub(crate) fn new_typed_column(kind: ColumnKind) -> TypedColumn {
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
pub(crate) fn init_typed_columns(columns: &[ColumnMeta], kinds: &[ColumnKind]) -> Vec<TypedColumn> {
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

/// Sort typed columns in-place by the given order_by column indices.
/// Computes a permutation from the sort keys, then reorders all columns by that permutation.
pub(crate) fn sort_typed_columns(typed_cols: &mut [TypedColumn], order_col_indices: &[usize], num_rows: usize) {
    if order_col_indices.is_empty() || num_rows <= 1 {
        return;
    }

    // Build sort permutation using indices
    let mut perm: Vec<usize> = (0..num_rows).collect();
    perm.sort_by(|&a, &b| {
        for &col_idx in order_col_indices {
            let cmp = match &typed_cols[col_idx] {
                TypedColumn::Int16(v) => v[a].cmp(&v[b]),
                TypedColumn::Int32(v) => v[a].cmp(&v[b]),
                TypedColumn::Int64(v) => v[a].cmp(&v[b]),
                TypedColumn::Float32(v) => {
                    let fa = v[a].map(|f| f.to_bits());
                    let fb = v[b].map(|f| f.to_bits());
                    fa.cmp(&fb)
                }
                TypedColumn::Float64(v) => {
                    let fa = v[a].map(|f| f.to_bits());
                    let fb = v[b].map(|f| f.to_bits());
                    fa.cmp(&fb)
                }
                TypedColumn::Bool(v) => v[a].cmp(&v[b]),
                TypedColumn::Text(v) => v[a].cmp(&v[b]),
            };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });

    // Apply permutation to all columns
    for tc in typed_cols.iter_mut() {
        match tc {
            TypedColumn::Int16(v) => apply_permutation(v, &perm),
            TypedColumn::Int32(v) => apply_permutation(v, &perm),
            TypedColumn::Int64(v) => apply_permutation(v, &perm),
            TypedColumn::Float32(v) => apply_permutation(v, &perm),
            TypedColumn::Float64(v) => apply_permutation(v, &perm),
            TypedColumn::Bool(v) => apply_permutation(v, &perm),
            TypedColumn::Text(v) => apply_permutation(v, &perm),
        }
    }
}

/// Reorder a Vec according to a permutation, returning a new Vec.
fn apply_permutation<T: Clone>(v: &mut Vec<T>, perm: &[usize]) {
    let reordered: Vec<T> = perm.iter().map(|&i| v[i].clone()).collect();
    *v = reordered;
}

/// A single row for the normalized colstats table.
pub(crate) struct ColstatsRow {
    pub(crate) col_idx: i16,
    pub(crate) segment_id: i32,
    pub(crate) min_val: Option<i64>,
    pub(crate) max_val: Option<i64>,
    pub(crate) sum_val: Option<String>,  // NUMERIC as string
    pub(crate) nonnull_count: i32,
    pub(crate) nonzero_count: i32,
    pub(crate) ndistinct: i64,
}

/// Return type for flush_segment_metadata: (compressed_size, column blobs, per-column bloom entries).
/// Each bloom entry is (col_idx, num_hashes, bloom_bytes).
pub(crate) type FlushResult = (i64, Vec<(u16, Vec<u8>)>, Vec<(u16, u8, Vec<u8>)>);

/// Compress accumulated typed column data and INSERT metadata into the meta + colstats tables.
/// Returns (compressed_size, vec of (col_idx, compressed_blob)) — blobs are NOT inserted,
/// they are returned for column-major buffering by the caller.
#[allow(clippy::too_many_arguments)]
pub(crate) fn flush_segment_metadata(
    client: &mut SpiClient,
    meta_fqn: &str,
    colstats_fqn: &str,
    columns: &[ColumnMeta],
    typed_cols: &[TypedColumn],
    segment_by_values: &[Option<String>],
    ndistinct_values: &[i64],
    row_count: u32,
    segment_id: i32,
) -> FlushResult {
    // Returns (compressed_size, blobs, bloom_entries)
    // Compress each non-segment column, collect blobs for caller
    let mut blobs: Vec<(u16, Vec<u8>)> = Vec::new(); // (col_idx, compressed_data)
    let mut col_minmax: std::collections::HashMap<String, (Option<String>, Option<String>)> =
        std::collections::HashMap::new();
    let mut total_size: i64 = 0;

    let mut col_sums: std::collections::HashMap<String, (Option<String>, i64, i64)> =
        std::collections::HashMap::new();

    let mut col_idx: u16 = 0;
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
        blobs.push((col_idx, compressed));
        col_idx += 1;
    }

    // Build INSERT for thin meta table: segment_id, segment_by, time min/max, row_count
    let mut meta_cols = Vec::new();
    let mut meta_vals = Vec::new();

    meta_cols.push("_segment_id".to_string());
    meta_vals.push(segment_id.to_string());

    // Segment-by columns
    let mut seg_idx = 0;
    for col in columns {
        if col.is_segment_by {
            meta_cols.push(format!("\"{}\"", col.name));
            if seg_idx < segment_by_values.len() {
                match &segment_by_values[seg_idx] {
                    Some(v) => meta_vals.push(format!("'{}'", v.replace('\'', "''"))),
                    None => meta_vals.push("NULL".to_string()),
                }
                seg_idx += 1;
            }
        }
    }

    // Time column min/max only
    for col in columns {
        if col.is_time_column && !col.is_segment_by && supports_minmax(&col.data_type) {
            meta_cols.push(format!("\"_min_{}\"", col.name));
            meta_cols.push(format!("\"_max_{}\"", col.name));
            match col_minmax.get(&col.name) {
                Some((Some(min_val), Some(max_val))) => {
                    meta_vals.push(format_minmax_for_insert(min_val, &col.data_type));
                    meta_vals.push(format_minmax_for_insert(max_val, &col.data_type));
                }
                _ => {
                    meta_vals.push("NULL".to_string());
                    meta_vals.push("NULL".to_string());
                }
            }
        }
    }

    meta_cols.push("_row_count".to_string());
    meta_vals.push(row_count.to_string());

    let meta_sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        meta_fqn,
        meta_cols.join(", "),
        meta_vals.join(", ")
    );
    client
        .update(&meta_sql, None, &[])
        .expect("failed to insert segment metadata");

    // Build normalized colstats rows: one per non-segment-by column
    let mut cs_rows: Vec<String> = Vec::new();
    let mut col_idx_counter: i16 = 0;
    let mut nd_idx = 0;
    for (i, col) in columns.iter().enumerate() {
        if col.is_segment_by {
            continue;
        }
        let (min_enc, max_enc) = compute_minmax_encoded_i64(&typed_cols[i], &col.data_type);
        let min_str = min_enc.map_or("NULL".to_string(), |v| v.to_string());
        let max_str = max_enc.map_or("NULL".to_string(), |v| v.to_string());

        let (sum_str, nonnull, nonzero) = if supports_sum(&col.data_type) {
            let (s, nn, nz) = col_sums.get(&col.name)
                .cloned()
                .unwrap_or((None, 0, 0));
            (s.unwrap_or_else(|| "NULL".to_string()), nn, nz)
        } else {
            ("NULL".to_string(), 0, 0)
        };

        let nd = if nd_idx < ndistinct_values.len() {
            ndistinct_values[nd_idx]
        } else {
            0
        };
        nd_idx += 1;

        cs_rows.push(format!(
            "({}, {}, {}, {}, {}, {}, {}, {})",
            col_idx_counter, segment_id, min_str, max_str, sum_str, nonnull, nonzero, nd
        ));
        col_idx_counter += 1;
    }

    if !cs_rows.is_empty() {
        let cs_sql = format!(
            "INSERT INTO {} (_col_idx, _segment_id, _min, _max, _sum, _nonnull_count, _nonzero_count, _ndistinct) VALUES {}",
            colstats_fqn,
            cs_rows.join(", ")
        );
        client
            .update(&cs_sql, None, &[])
            .expect("failed to insert segment colstats");
    }

    // Compute per-column bloom filters (if enabled via GUC) — stored separately
    let bloom_entries = if crate::BLOOM_FILTERS.get() {
        compute_segment_blooms(typed_cols, columns, ndistinct_values)
    } else {
        Vec::new()
    };

    (total_size, blobs, bloom_entries)
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

/// Hash a value and return the hash for HLL insertion.
fn hash_for_hll<T: Hash>(val: &T) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    val.hash(&mut hasher);
    hasher.finish()
}

/// Compute per-segment ndistinct using HyperLogLog estimators.
/// Returns one estimate per non-segment-by column.
pub(crate) fn compute_segment_ndistinct(
    typed_cols: &[TypedColumn],
    columns: &[ColumnMeta],
) -> Vec<i64> {
    let mut result = Vec::new();
    for (i, col) in columns.iter().enumerate() {
        if col.is_segment_by {
            continue;
        }
        let mut hll = CardinalityEstimator::<u64>::new();
        match &typed_cols[i] {
            TypedColumn::Int16(v) => { for x in v.iter().flatten() { hll.insert_hash(hash_for_hll(x)); } }
            TypedColumn::Int32(v) => { for x in v.iter().flatten() { hll.insert_hash(hash_for_hll(x)); } }
            TypedColumn::Int64(v) => { for x in v.iter().flatten() { hll.insert_hash(hash_for_hll(x)); } }
            TypedColumn::Float32(v) => { for x in v.iter().flatten() { hll.insert_hash(hash_for_hll(&x.to_bits())); } }
            TypedColumn::Float64(v) => { for x in v.iter().flatten() { hll.insert_hash(hash_for_hll(&x.to_bits())); } }
            TypedColumn::Bool(v) => { for x in v.iter().flatten() { hll.insert_hash(hash_for_hll(x)); } }
            TypedColumn::Text(v) => { for x in v.iter().flatten() { hll.insert_hash(hash_for_hll(x)); } }
        }
        result.push(hll.estimate() as i64);
    }
    result
}

/// Compute per-column bloom filters for a segment.
/// Returns one (col_idx, num_hashes, bloom_bytes) entry per column that got a bloom,
/// or empty if no columns qualify. Only builds bloom filters for numeric/date/timestamp
/// columns with ndistinct > 0.
pub(crate) fn compute_segment_blooms(
    typed_cols: &[TypedColumn],
    columns: &[ColumnMeta],
    ndistinct_values: &[i64],
) -> Vec<(u16, u8, Vec<u8>)> {
    use crate::bloom::{BloomFilter, hash_datum_i64};

    let mut entries: Vec<(u16, u8, Vec<u8>)> = Vec::new();
    let mut nd_idx: usize = 0;
    let mut col_idx: u16 = 0;

    for (i, col) in columns.iter().enumerate() {
        if col.is_segment_by {
            continue;
        }
        let nd = if nd_idx < ndistinct_values.len() {
            ndistinct_values[nd_idx]
        } else {
            0
        };
        nd_idx += 1;

        if !supports_minmax(&col.data_type) || nd <= 0 {
            col_idx += 1;
            continue;
        }

        let mut bf = BloomFilter::for_ndistinct(nd as usize);
        match &typed_cols[i] {
            TypedColumn::Int16(v) => {
                for x in v.iter().flatten() {
                    bf.insert(hash_datum_i64(*x as i64));
                }
            }
            TypedColumn::Int32(v) => {
                for x in v.iter().flatten() {
                    bf.insert(hash_datum_i64(*x as i64));
                }
            }
            TypedColumn::Int64(v) => {
                for x in v.iter().flatten() {
                    bf.insert(hash_datum_i64(*x));
                }
            }
            TypedColumn::Float32(v) => {
                for x in v.iter().flatten() {
                    bf.insert(hash_datum_i64(x.to_bits() as i64));
                }
            }
            TypedColumn::Float64(v) => {
                for x in v.iter().flatten() {
                    bf.insert(hash_datum_i64(x.to_bits() as i64));
                }
            }
            _ => {
                col_idx += 1;
                continue;
            }
        }

        entries.push((col_idx, bf.num_hashes(), bf.as_bytes().to_vec()));
        col_idx += 1;
    }

    entries
}

/// Flush typed column data, splitting into segment_size chunks if needed.
/// Returns compressed_size. Blobs and blooms are buffered for batch insertion.
#[allow(clippy::too_many_arguments)]
pub(crate) fn flush_with_splitting(
    client: &mut SpiClient,
    meta_fqn: &str,
    colstats_fqn: &str,
    columns: &[ColumnMeta],
    typed_cols: &[TypedColumn],
    seg_values: &[Option<String>],
    total_rows: usize,
    segment_size: usize,
    next_segment_id: &mut i32,
    blob_buffer: &mut Vec<(u16, i32, Vec<u8>)>,
    bloom_buffer: &mut Vec<(u16, i32, u8, Vec<u8>)>,
) -> i64 {
    let mut total_size = 0i64;
    let mut offset = 0;
    while offset < total_rows {
        let chunk_end = (offset + segment_size).min(total_rows);
        let chunk_rows = (chunk_end - offset) as u32;
        let seg_id = *next_segment_id;
        *next_segment_id += 1;
        if offset == 0 && chunk_end == total_rows {
            let ndistinct = compute_segment_ndistinct(typed_cols, columns);
            let (size, blobs, bloom_entries) =
                flush_segment_metadata(client, meta_fqn, colstats_fqn, columns, typed_cols, seg_values, &ndistinct, chunk_rows, seg_id);
            total_size += size;
            for (col_idx, blob) in blobs {
                blob_buffer.push((col_idx, seg_id, blob));
            }
            for (col_idx, num_hashes, bytes) in bloom_entries {
                bloom_buffer.push((col_idx, seg_id, num_hashes, bytes));
            }
        } else {
            let chunk_cols: Vec<TypedColumn> = typed_cols
                .iter()
                .map(|tc| slice_typed_column(tc, offset, chunk_end))
                .collect();
            let ndistinct = compute_segment_ndistinct(&chunk_cols, columns);
            let (size, blobs, bloom_entries) =
                flush_segment_metadata(client, meta_fqn, colstats_fqn, columns, &chunk_cols, seg_values, &ndistinct, chunk_rows, seg_id);
            total_size += size;
            for (col_idx, blob) in blobs {
                blob_buffer.push((col_idx, seg_id, blob));
            }
            for (col_idx, num_hashes, bytes) in bloom_entries {
                bloom_buffer.push((col_idx, seg_id, num_hashes, bytes));
            }
        }
        offset = chunk_end;
    }
    total_size
}


/// DDL info for all companion tables of a compressed partition.
pub(crate) struct CompanionDdl {
    pub(crate) meta_fqn: String,
    pub(crate) colstats_fqn: String,
    pub(crate) blobs_fqn: String,
    pub(crate) blooms_fqn: String,
    pub(crate) meta_ddl: String,
    pub(crate) colstats_ddl: String,
    pub(crate) blobs_ddl: String,
    pub(crate) blooms_ddl: String,
}

/// Build DDL for companion tables (meta, colstats, blobs, blooms) for a partition.
///
/// The meta table is thin: only segment_id, segment_by cols, time column min/max,
/// and row_count. All other per-column stats (min/max for non-time columns,
/// sum/count, ndistinct) go into the colstats table.
pub(crate) fn build_companion_ddl(
    part_table: &str,
    columns: &[ColumnMeta],
) -> CompanionDdl {
    let companion_schema = "_deltax_compressed";
    let meta_fqn = format!("\"{}\".\"{}_meta\"", companion_schema, part_table);
    let colstats_fqn = format!("\"{}\".\"{}_colstats\"", companion_schema, part_table);
    let blobs_fqn = format!("\"{}\".\"{}_blobs\"", companion_schema, part_table);
    let blooms_fqn = format!("\"{}\".\"{}_blooms\"", companion_schema, part_table);

    // Thin meta table: segment_id, segment_by cols, time column min/max, row_count
    let mut meta_cols = Vec::new();
    meta_cols.push("_segment_id INT PRIMARY KEY".to_string());
    for col in columns {
        if col.is_segment_by {
            meta_cols.push(format!("\"{}\" {}", col.name, col.data_type));
        }
    }
    for col in columns {
        if col.is_time_column && !col.is_segment_by && supports_minmax(&col.data_type) {
            meta_cols.push(format!("\"_min_{}\" {}", col.name, col.data_type));
            meta_cols.push(format!("\"_max_{}\" {}", col.name, col.data_type));
        }
    }
    meta_cols.push("_row_count INT".to_string());

    let meta_ddl = format!(
        "CREATE TABLE {} ({})",
        meta_fqn,
        meta_cols.join(", ")
    );

    // Normalized colstats table: fixed 8-column schema
    let colstats_ddl = format!(
        "CREATE TABLE {} (\
         _col_idx SMALLINT NOT NULL, \
         _segment_id INT NOT NULL, \
         _min INT8, \
         _max INT8, \
         _sum NUMERIC, \
         _nonnull_count INT, \
         _nonzero_count INT, \
         _ndistinct INT, \
         PRIMARY KEY (_col_idx, _segment_id))",
        colstats_fqn
    );

    // STORAGE EXTERNAL: skip TOAST pglz compression on _data — blobs are already zstd-compressed.
    let blobs_ddl = format!(
        "CREATE TABLE {} (_col_idx SMALLINT NOT NULL, _segment_id INT NOT NULL, _data BYTEA COMPRESSION lz4, PRIMARY KEY (_col_idx, _segment_id))",
        blobs_fqn
    );

    let blooms_ddl = format!(
        "CREATE TABLE {} (_col_idx SMALLINT NOT NULL, _segment_id INT NOT NULL, _num_hashes SMALLINT NOT NULL, _data BYTEA COMPRESSION lz4 NOT NULL, PRIMARY KEY (_col_idx, _segment_id))",
        blooms_fqn
    );

    CompanionDdl {
        meta_fqn,
        colstats_fqn,
        blobs_fqn,
        blooms_fqn,
        meta_ddl,
        colstats_ddl,
        blobs_ddl,
        blooms_ddl,
    }
}

/// Compress a partition using cursor-based streaming.
/// Reads native PG datums directly — no text round-trip for numeric/timestamp types.
/// Handles both segment_by and non-segment_by partitions (boundary detection is
/// guarded by `if !seg_col_indices.is_empty()` and naturally skipped when empty).
/// Returns (compressed_size, row_count). ndistinct is tracked per-segment via HLL
/// and stored in the meta table. Blobs are buffered and inserted column-major
/// into the blobs table after all segments are processed.
fn compress_partition_streaming(
    client: &mut SpiClient,
    part_fqn: &str,
    ddl: &CompanionDdl,
    columns: &[ColumnMeta],
    order_by: &[String],
    segment_by: &[String],
    segment_size: usize,
) -> (i64, i64) {
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

    // Build ORDER BY: only needed when segment_by is non-empty (for boundary detection).
    // When segment_by is empty, we skip the SQL ORDER BY to avoid a full-partition sort
    // and instead sort each segment in Rust before flushing.
    let order_clause = if !segment_by.is_empty() {
        let mut order_parts = Vec::new();
        for s in segment_by {
            order_parts.push(format!("\"{}\"", s));
        }
        for o in order_by {
            order_parts.push(format!("\"{}\"", o));
        }
        format!(" ORDER BY {}", order_parts.join(", "))
    } else {
        String::new()
    };

    // Resolve order_by column indices for Rust-side sorting (used when no SQL ORDER BY)
    let order_col_indices: Vec<usize> = order_by
        .iter()
        .filter_map(|name| columns.iter().position(|c| c.name == *name))
        .collect();

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
    let mut total_rows: i64 = 0;
    let mut tables_created = false;
    let mut next_segment_id: i32 = 1;
    let mut blob_buffer: Vec<(u16, i32, Vec<u8>)> = Vec::new(); // (col_idx, segment_id, blob)
    let mut bloom_buffer: Vec<(u16, i32, u8, Vec<u8>)> = Vec::new(); // (col_idx, segment_id, num_hashes, bloom_bytes)

    loop {
        let result = client
            .select(&fetch_sql, None, &[])
            .expect("failed to fetch from cursor");
        let fetched = result.len();
        if fetched == 0 {
            break;
        }
        // Save the tuptable pointer so we can free it after consuming all rows.
        // pgrx doesn't free SPI tuple tables until SPI_finish(), which causes
        // unbounded memory growth when fetching millions of rows via cursor.
        let tuptable_to_free = unsafe { pg_sys::SPI_tuptable };

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
                        if !tables_created {
                            client.update(&ddl.meta_ddl, None, &[]).expect("failed to create meta table");
                            client.update(&ddl.colstats_ddl, None, &[]).expect("failed to create colstats table");
                            tables_created = true;
                        }
                        total_compressed_size += flush_with_splitting(
                            client,
                            &ddl.meta_fqn,
                            &ddl.colstats_fqn,
                            columns,
                            &typed_cols,
                            &current_seg_values,
                            rows_in_segment,
                            segment_size,
                            &mut next_segment_id,
                            &mut blob_buffer,
                            &mut bloom_buffer,
                        );
                        typed_cols = init_typed_columns(columns, &kinds);
                        rows_in_segment = 0;
                    }
                    current_seg_values = row_seg_values;
                }
            }

            append_row_to_columns(&row, columns, &kinds, &mut typed_cols);
            rows_in_segment += 1;
            total_rows += 1;

            // Check segment_size limit
            if rows_in_segment >= segment_size {
                if !tables_created {
                    client.update(&ddl.meta_ddl, None, &[]).expect("failed to create meta table");
                    client.update(&ddl.colstats_ddl, None, &[]).expect("failed to create colstats table");
                    tables_created = true;
                }
                // Sort in Rust when no SQL ORDER BY (non-segment_by path)
                if seg_col_indices.is_empty() {
                    sort_typed_columns(&mut typed_cols, &order_col_indices, rows_in_segment);
                }
                let seg_id = next_segment_id;
                next_segment_id += 1;
                let ndistinct = compute_segment_ndistinct(&typed_cols, columns);
                let (size, blobs, bloom_entries) = flush_segment_metadata(
                    client,
                    &ddl.meta_fqn,
                    &ddl.colstats_fqn,
                    columns,
                    &typed_cols,
                    &current_seg_values,
                    &ndistinct,
                    rows_in_segment as u32,
                    seg_id,
                );
                total_compressed_size += size;
                for (col_idx, blob) in blobs {
                    blob_buffer.push((col_idx, seg_id, blob));
                }
                for (col_idx, num_hashes, bytes) in bloom_entries {
                    bloom_buffer.push((col_idx, seg_id, num_hashes, bytes));
                }
                typed_cols = init_typed_columns(columns, &kinds);
                rows_in_segment = 0;
            }
        }

        // Free the SPI tuple table from this batch to prevent unbounded memory growth.
        // Safe because we've fully consumed all rows and extracted values into owned Rust types.
        if !tuptable_to_free.is_null() {
            unsafe { pg_sys::SPI_freetuptable(tuptable_to_free) };
        }

        if fetched < batch_size {
            break;
        }
    }

    // Flush remaining
    if rows_in_segment > 0 {
        if !tables_created {
            client.update(&ddl.meta_ddl, None, &[]).expect("failed to create meta table");
            client.update(&ddl.colstats_ddl, None, &[]).expect("failed to create colstats table");
        }
        if seg_col_indices.is_empty() {
            sort_typed_columns(&mut typed_cols, &order_col_indices, rows_in_segment);
        }
        total_compressed_size += flush_with_splitting(
            client,
            &ddl.meta_fqn,
            &ddl.colstats_fqn,
            columns,
            &typed_cols,
            &current_seg_values,
            rows_in_segment,
            segment_size,
            &mut next_segment_id,
            &mut blob_buffer,
            &mut bloom_buffer,
        );
    }

    client
        .update("CLOSE comp_cursor", None, &[])
        .expect("failed to close cursor");

    // Flush blobs column-major into the blobs table
    if !blob_buffer.is_empty() {
        client.update(&ddl.blobs_ddl, None, &[]).expect("failed to create blobs table");

        // Sort by (col_idx, segment_id) for column-major insertion order
        blob_buffer.sort_by_key(|&(col_idx, seg_id, _)| (col_idx, seg_id));

        for (col_idx, seg_id, blob) in blob_buffer {
            use pgrx::datum::DatumWithOid;
            let insert_sql = format!(
                "INSERT INTO {} (_col_idx, _segment_id, _data) VALUES ($1, $2, $3)",
                &ddl.blobs_fqn
            );
            let args: Vec<DatumWithOid> = vec![
                (col_idx as i16).into(),
                seg_id.into(),
                DatumWithOid::from(blob),
            ];
            client
                .update(&insert_sql, None, &args)
                .expect("failed to insert blob");
        }

        // Flush bloom filters into separate blooms table
        if !bloom_buffer.is_empty() {
            client.update(&ddl.blooms_ddl, None, &[]).expect("failed to create blooms table");

            // Sort by (col_idx, segment_id) for column-major insertion order
            bloom_buffer.sort_by_key(|&(col_idx, seg_id, _, _)| (col_idx, seg_id));

            for (col_idx, seg_id, num_hashes, bloom_bytes) in bloom_buffer {
                use pgrx::datum::DatumWithOid;
                let insert_sql = format!(
                    "INSERT INTO {} (_col_idx, _segment_id, _num_hashes, _data) VALUES ($1, $2, $3, $4)",
                    &ddl.blooms_fqn
                );
                let args: Vec<DatumWithOid> = vec![
                    (col_idx as i16).into(),
                    seg_id.into(),
                    (num_hashes as i16).into(),
                    DatumWithOid::from(bloom_bytes),
                ];
                client
                    .update(&insert_sql, None, &args)
                    .expect("failed to insert bloom data");
            }
            client
                .update(&format!("ANALYZE {}", ddl.blooms_fqn), None, &[])
                .expect("failed to analyze blooms table");
        }

        // ANALYZE meta, colstats, and blobs tables for planner statistics
        client
            .update(&format!("ANALYZE {}", ddl.meta_fqn), None, &[])
            .expect("failed to analyze meta table");
        client
            .update(&format!("ANALYZE {}", ddl.colstats_fqn), None, &[])
            .expect("failed to analyze colstats table");
        client
            .update(&format!("ANALYZE {}", ddl.blobs_fqn), None, &[])
            .expect("failed to analyze blobs table");
    }

    (total_compressed_size, total_rows)
}

/// Compress a typed column directly, bypassing string parsing.
pub(crate) fn compress_typed_column(data: &TypedColumn, data_type: &str) -> Vec<u8> {
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

/// Encode f64 to i64 in an order-preserving way.
/// Positive floats map to positive i64s, negatives to negative i64s, preserving order.
pub(crate) fn encode_f64_to_i64(v: f64) -> i64 {
    let bits = v.to_bits() as i64;
    if bits >= 0 { bits ^ i64::MIN } else { !bits }
}

/// Decode order-preserving i64 back to f64.
pub(crate) fn decode_i64_to_f64(enc: i64) -> f64 {
    let bits = if enc >= 0 { !enc } else { enc ^ i64::MIN };
    f64::from_bits(bits as u64)
}

/// Encode f32 to i64 in an order-preserving way (via 32-bit transform, then sign-extend).
pub(crate) fn encode_f32_to_i64(v: f32) -> i64 {
    let bits = v.to_bits() as i32;
    let i32_enc = if bits >= 0 { bits ^ i32::MIN } else { !bits };
    i32_enc as i64
}

/// Decode order-preserving i64 back to f32.
pub(crate) fn decode_i64_to_f32(enc: i64) -> f32 {
    let i32_enc = enc as i32;
    let bits = if i32_enc >= 0 { !i32_enc } else { i32_enc ^ i32::MIN };
    f32::from_bits(bits as u32)
}

/// Compute min/max encoded as order-preserving i64, for use in normalized colstats table.
/// Returns None for types without minmax support.
pub(crate) fn compute_minmax_encoded_i64(data: &TypedColumn, data_type: &str) -> (Option<i64>, Option<i64>) {
    if !supports_minmax(data_type) {
        return (None, None);
    }
    match data {
        TypedColumn::Int16(values) => {
            let mut min_v: Option<i64> = None;
            let mut max_v: Option<i64> = None;
            for v in values.iter().flatten() {
                let v64 = *v as i64;
                min_v = Some(min_v.map_or(v64, |cur| cur.min(v64)));
                max_v = Some(max_v.map_or(v64, |cur| cur.max(v64)));
            }
            (min_v, max_v)
        }
        TypedColumn::Int32(values) => {
            let mut min_v: Option<i64> = None;
            let mut max_v: Option<i64> = None;
            for v in values.iter().flatten() {
                let v64 = *v as i64;
                min_v = Some(min_v.map_or(v64, |cur| cur.min(v64)));
                max_v = Some(max_v.map_or(v64, |cur| cur.max(v64)));
            }
            (min_v, max_v)
        }
        TypedColumn::Int64(values) => {
            // For int64, timestamp, timestamptz, date — identity (already i64)
            let mut min_v: Option<i64> = None;
            let mut max_v: Option<i64> = None;
            for v in values.iter().flatten() {
                min_v = Some(min_v.map_or(*v, |cur| cur.min(*v)));
                max_v = Some(max_v.map_or(*v, |cur| cur.max(*v)));
            }
            (min_v, max_v)
        }
        TypedColumn::Float64(values) => {
            let mut min_v: Option<i64> = None;
            let mut max_v: Option<i64> = None;
            for v in values.iter().flatten() {
                let enc = encode_f64_to_i64(*v);
                min_v = Some(min_v.map_or(enc, |cur| cur.min(enc)));
                max_v = Some(max_v.map_or(enc, |cur| cur.max(enc)));
            }
            (min_v, max_v)
        }
        TypedColumn::Float32(values) => {
            let mut min_v: Option<i64> = None;
            let mut max_v: Option<i64> = None;
            for v in values.iter().flatten() {
                let enc = encode_f32_to_i64(*v);
                min_v = Some(min_v.map_or(enc, |cur| cur.min(enc)));
                max_v = Some(max_v.map_or(enc, |cur| cur.max(enc)));
            }
            (min_v, max_v)
        }
        _ => (None, None), // Text, Bool — no minmax
    }
}

/// Compute min/max for typed columns, returning string representations for SQL INSERT.
pub(crate) fn compute_typed_minmax(data: &TypedColumn, data_type: &str) -> (Option<String>, Option<String>) {
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
pub(crate) fn get_column_metadata(
    client: &SpiClient,
    schema: &str,
    table: &str,
    segment_by: &[String],
    time_column: &str,
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
        let is_time = name == time_column;
        columns.push(ColumnMeta {
            name,
            data_type,
            is_segment_by: is_segment,
            is_time_column: is_time,
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
            pgrx::error!("pg_deltax: partition {}.{} not found in catalog", schema, part_table)
        });

    if !part_info.is_compressed {
        return format!("Partition {}.{} is not compressed", schema, part_table);
    }

    let ht = catalog::get_deltatable_by_id(client, part_info.deltatable_id)
        .expect("failed to query deltatable")
        .unwrap();

    // 2. Get column metadata (from the parent table, since partition is truncated)
    let columns = get_column_metadata(client, &ht.schema_name, &ht.table_name, &ht.segment_by, &ht.time_column);

    let companion_schema = "_deltax_compressed";
    let meta_fqn = format!("\"{}\".\"{}_meta\"", companion_schema, part_table);
    let colstats_fqn = format!("\"{}\".\"{}_colstats\"", companion_schema, part_table);
    let blobs_fqn = format!("\"{}\".\"{}_blobs\"", companion_schema, part_table);
    let blooms_fqn = format!("\"{}\".\"{}_blooms\"", companion_schema, part_table);
    let part_fqn = crate::partition::fqn(&schema, &part_table);

    // 3. Read compressed segments from meta + blobs tables

    // Build col_idx mapping: non-segment-by columns in ordinal order
    let mut non_seg_cols: Vec<(u16, String, String)> = Vec::new(); // (col_idx, name, data_type)
    let mut col_idx: u16 = 0;
    for col in &columns {
        if !col.is_segment_by {
            non_seg_cols.push((col_idx, col.name.clone(), col.data_type.clone()));
            col_idx += 1;
        }
    }

    // Read meta table: segment_by cols, _segment_id, _row_count
    let mut meta_select_cols = vec!["_segment_id".to_string()];
    for col in &columns {
        if col.is_segment_by {
            meta_select_cols.push(format!("\"{}\"::text", col.name));
        }
    }
    meta_select_cols.push("_row_count".to_string());

    let meta_query = format!(
        "SELECT {} FROM {} ORDER BY _segment_id",
        meta_select_cols.join(", "),
        meta_fqn
    );
    let meta_rows = client.select(&meta_query, None, &[]).expect("failed to read meta table");

    // Collect all segment metadata
    struct SegMeta {
        segment_id: i32,
        segment_by_vals: Vec<Option<String>>,
        row_count: i32,
    }
    let mut seg_metas: Vec<SegMeta> = Vec::new();
    for row in meta_rows {
        let mut col_ordinal: usize = 1;
        let segment_id: i32 = row.get_datum_by_ordinal(col_ordinal).unwrap().value::<i32>().unwrap().unwrap_or(0);
        col_ordinal += 1;

        let mut segment_by_vals: Vec<Option<String>> = Vec::new();
        for col in &columns {
            if col.is_segment_by {
                let val: Option<String> = row.get_datum_by_ordinal(col_ordinal).unwrap().value::<String>().unwrap();
                segment_by_vals.push(val);
                col_ordinal += 1;
            }
        }

        let row_count: i32 = row.get_datum_by_ordinal(col_ordinal).unwrap().value::<i32>().unwrap().unwrap_or(0);
        seg_metas.push(SegMeta { segment_id, segment_by_vals, row_count });
    }

    let mut total_rows_restored = 0i64;

    for seg_meta in &seg_metas {
        if seg_meta.row_count == 0 {
            continue;
        }

        // Read blobs for this segment from the blobs table
        let blob_query = format!(
            "SELECT _col_idx, _data FROM {} WHERE _segment_id = $1 ORDER BY _col_idx",
            blobs_fqn
        );
        let blob_rows = client.select(&blob_query, None, &[seg_meta.segment_id.into()])
            .expect("failed to read blobs");

        let mut blob_map: std::collections::HashMap<u16, Vec<u8>> = std::collections::HashMap::new();
        for brow in blob_rows {
            let ci: i16 = brow.get_datum_by_ordinal(1).unwrap().value::<i16>().unwrap().unwrap_or(0);
            let data: Option<Vec<u8>> = brow.get_datum_by_ordinal(2).unwrap().value::<Vec<u8>>().unwrap();
            blob_map.insert(ci as u16, data.unwrap_or_default());
        }

        // Decompress all columns
        let mut decompressed_cols: Vec<(String, Vec<Option<String>>)> = Vec::new();

        // Segment-by columns: repeat the value for every row
        let mut seg_idx = 0;
        for col in &columns {
            if col.is_segment_by {
                let val = &seg_meta.segment_by_vals[seg_idx];
                let repeated: Vec<Option<String>> =
                    (0..seg_meta.row_count).map(|_| val.clone()).collect();
                decompressed_cols.push((col.name.clone(), repeated));
                seg_idx += 1;
            }
        }

        // Compressed columns: decompress from blob_map
        for (ci, name, data_type) in &non_seg_cols {
            let blob = blob_map.get(ci).cloned().unwrap_or_default();
            let values = decompress_column_values(&blob, data_type);
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
        let segment_row_count = seg_meta.row_count;

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

    // 4. Drop meta + colstats + blobs + blooms tables
    client
        .update(&format!("DROP TABLE IF EXISTS {}", blobs_fqn), None, &[])
        .expect("failed to drop blobs table");
    client
        .update(&format!("DROP TABLE IF EXISTS {}", blooms_fqn), None, &[])
        .expect("failed to drop blooms table");
    client
        .update(&format!("DROP TABLE IF EXISTS {}", colstats_fqn), None, &[])
        .expect("failed to drop colstats table");
    client
        .update(&format!("DROP TABLE IF EXISTS {}", meta_fqn), None, &[])
        .expect("failed to drop meta table");

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
pub(crate) fn supports_minmax(data_type: &str) -> bool {
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
pub(crate) fn supports_sum(data_type: &str) -> bool {
    let dt = data_type.to_lowercase();
    dt == "integer" || dt == "int4"
        || dt == "bigint" || dt == "int8"
        || dt == "smallint" || dt == "int2"
        || dt == "double precision" || dt == "float8"
        || dt == "real" || dt == "float4"
}

/// Check if a data type is a floating-point type.
pub(crate) fn is_float_type(data_type: &str) -> bool {
    let dt = data_type.to_lowercase();
    dt == "double precision" || dt == "float8" || dt == "real" || dt == "float4"
}

/// Compute sum, non-null count, and nonzero count for a typed column.
/// Returns (sum_as_string, nonnull_count, nonzero_count). Uses i128 for integer sums to avoid overflow.
pub(crate) fn compute_typed_sum(data: &TypedColumn) -> (Option<String>, i64, i64) {
    match data {
        TypedColumn::Int16(v) => {
            let mut sum: i128 = 0;
            let mut count: i64 = 0;
            let mut nonzero: i64 = 0;
            for val in v.iter().flatten() {
                sum += *val as i128;
                count += 1;
                if *val != 0 { nonzero += 1; }
            }
            if count > 0 { (Some(sum.to_string()), count, nonzero) } else { (None, 0, 0) }
        }
        TypedColumn::Int32(v) => {
            let mut sum: i128 = 0;
            let mut count: i64 = 0;
            let mut nonzero: i64 = 0;
            for val in v.iter().flatten() {
                sum += *val as i128;
                count += 1;
                if *val != 0 { nonzero += 1; }
            }
            if count > 0 { (Some(sum.to_string()), count, nonzero) } else { (None, 0, 0) }
        }
        TypedColumn::Int64(v) => {
            let mut sum: i128 = 0;
            let mut count: i64 = 0;
            let mut nonzero: i64 = 0;
            for val in v.iter().flatten() {
                sum += *val as i128;
                count += 1;
                if *val != 0 { nonzero += 1; }
            }
            if count > 0 { (Some(sum.to_string()), count, nonzero) } else { (None, 0, 0) }
        }
        TypedColumn::Float32(v) => {
            let mut sum: f64 = 0.0;
            let mut count: i64 = 0;
            let mut nonzero: i64 = 0;
            for val in v.iter().flatten() {
                sum += *val as f64;
                count += 1;
                if *val != 0.0 { nonzero += 1; }
            }
            if count > 0 { (Some(format!("{:.17e}", sum)), count, nonzero) } else { (None, 0, 0) }
        }
        TypedColumn::Float64(v) => {
            let mut sum: f64 = 0.0;
            let mut count: i64 = 0;
            let mut nonzero: i64 = 0;
            for val in v.iter().flatten() {
                sum += *val;
                count += 1;
                if *val != 0.0 { nonzero += 1; }
            }
            if count > 0 { (Some(format!("{:.17e}", sum)), count, nonzero) } else { (None, 0, 0) }
        }
        TypedColumn::Text(_) | TypedColumn::Bool(_) => (None, 0, 0),
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
pub(crate) fn format_minmax_for_insert(val: &str, data_type: &str) -> String {
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
pub fn auto_compress_partitions(client: &mut SpiClient<'_>, ht: &catalog::DeltatableInfo) -> i32 {
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
            "SELECT table_name FROM deltax_partition
             WHERE deltatable_id = $1 AND is_compressed = false
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_off_int64() {
        let mut col = TypedColumn::Int64(vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
        let tail = col.split_off(3);
        assert_eq!(col, TypedColumn::Int64(vec![Some(1), Some(2), Some(3)]));
        assert_eq!(tail, TypedColumn::Int64(vec![Some(4), Some(5)]));
    }

    #[test]
    fn test_split_off_text_with_nulls() {
        let mut col = TypedColumn::Text(vec![
            Some("a".into()), None, Some("c".into()), Some("d".into()),
        ]);
        let tail = col.split_off(2);
        assert_eq!(col, TypedColumn::Text(vec![Some("a".into()), None]));
        assert_eq!(tail, TypedColumn::Text(vec![Some("c".into()), Some("d".into())]));
    }

    #[test]
    fn test_split_off_at_zero() {
        let mut col = TypedColumn::Bool(vec![Some(true), Some(false)]);
        let tail = col.split_off(0);
        assert_eq!(col, TypedColumn::Bool(vec![]));
        assert_eq!(tail, TypedColumn::Bool(vec![Some(true), Some(false)]));
    }

    #[test]
    fn test_split_off_at_end() {
        let mut col = TypedColumn::Int32(vec![Some(1), Some(2)]);
        let tail = col.split_off(2);
        assert_eq!(col, TypedColumn::Int32(vec![Some(1), Some(2)]));
        assert_eq!(tail, TypedColumn::Int32(vec![]));
    }

    #[test]
    fn test_extend_int64() {
        let mut a = TypedColumn::Int64(vec![Some(1), Some(2)]);
        let b = TypedColumn::Int64(vec![Some(3), None]);
        a.extend(b);
        assert_eq!(a, TypedColumn::Int64(vec![Some(1), Some(2), Some(3), None]));
    }

    #[test]
    fn test_extend_empty() {
        let mut a = TypedColumn::Float32(vec![]);
        let b = TypedColumn::Float32(vec![Some(1.0)]);
        a.extend(b);
        assert_eq!(a, TypedColumn::Float32(vec![Some(1.0)]));
    }

    #[test]
    #[should_panic(expected = "mismatched variants")]
    fn test_extend_mismatched() {
        let mut a = TypedColumn::Int32(vec![]);
        let b = TypedColumn::Int64(vec![]);
        a.extend(b);
    }

    #[test]
    fn test_push_from_int64() {
        let src = TypedColumn::Int64(vec![Some(10), Some(20), None]);
        let mut dst = TypedColumn::Int64(vec![]);
        dst.push_from(&src, 1);
        dst.push_from(&src, 2);
        assert_eq!(dst, TypedColumn::Int64(vec![Some(20), None]));
    }

    #[test]
    fn test_push_from_text() {
        let src = TypedColumn::Text(vec![Some("hello".into()), None, Some("world".into())]);
        let mut dst = TypedColumn::Text(vec![]);
        dst.push_from(&src, 0);
        dst.push_from(&src, 1);
        assert_eq!(dst, TypedColumn::Text(vec![Some("hello".into()), None]));
    }

    #[test]
    #[should_panic(expected = "mismatched variants")]
    fn test_push_from_mismatched() {
        let src = TypedColumn::Int32(vec![Some(1)]);
        let mut dst = TypedColumn::Int64(vec![]);
        dst.push_from(&src, 0);
    }

    #[test]
    fn test_split_off_all_variants() {
        // Ensure split_off works for every TypedColumn variant
        let mut f64_col = TypedColumn::Float64(vec![Some(1.0), Some(2.0)]);
        let tail = f64_col.split_off(1);
        assert_eq!(f64_col, TypedColumn::Float64(vec![Some(1.0)]));
        assert_eq!(tail, TypedColumn::Float64(vec![Some(2.0)]));

        let mut f32_col = TypedColumn::Float32(vec![Some(1.0), Some(2.0)]);
        let tail = f32_col.split_off(1);
        assert_eq!(f32_col, TypedColumn::Float32(vec![Some(1.0)]));
        assert_eq!(tail, TypedColumn::Float32(vec![Some(2.0)]));

        let mut i16_col = TypedColumn::Int16(vec![Some(1), Some(2)]);
        let tail = i16_col.split_off(1);
        assert_eq!(i16_col, TypedColumn::Int16(vec![Some(1)]));
        assert_eq!(tail, TypedColumn::Int16(vec![Some(2)]));
    }
}
