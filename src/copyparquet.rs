//! Parquet file reading: column mapping, null unpacking, type conversion.

use parquet::basic::{LogicalType, TimeUnit as ParquetTimeUnit};
use parquet::column::reader::ColumnReader;
use parquet::file::reader::RowGroupReader;
use parquet::schema::types::SchemaDescriptor;

use crate::compress::{ColumnKind, ColumnMeta, TypedColumn};

/// Map Parquet schema columns to PG table columns by name (case-insensitive).
/// Returns `(parquet_idx, pg_idx)` pairs.
/// Errors if any non-segment_by PG column has no matching Parquet column.
pub(crate) fn map_parquet_to_pg_columns(
    parquet_schema: &SchemaDescriptor,
    pg_columns: &[ColumnMeta],
) -> Result<Vec<(usize, usize)>, String> {
    let mut mapping = Vec::new();

    for (pg_idx, col) in pg_columns.iter().enumerate() {
        let pg_name_lower = col.name.to_lowercase();
        let mut found = false;
        for pq_idx in 0..parquet_schema.num_columns() {
            let pq_col = parquet_schema.column(pq_idx);
            if pq_col.name().to_lowercase() == pg_name_lower {
                mapping.push((pq_idx, pg_idx));
                found = true;
                break;
            }
        }
        if !found && !col.is_segment_by {
            return Err(format!(
                "pg_deltax: PG column '{}' not found in Parquet schema",
                col.name
            ));
        }
    }

    Ok(mapping)
}

/// Read all mapped columns from a row group into TypedColumn vectors.
pub(crate) fn read_row_group_columns(
    rg_reader: &dyn RowGroupReader,
    col_mapping: &[(usize, usize)],
    kinds: &[ColumnKind],
    num_rows: usize,
    num_pg_columns: usize,
) -> Result<Vec<TypedColumn>, String> {
    let mut result: Vec<TypedColumn> = kinds
        .iter()
        .map(|k| crate::compress::new_typed_column(*k))
        .collect();
    debug_assert_eq!(result.len(), num_pg_columns);

    for &(pq_idx, pg_idx) in col_mapping {
        let kind = kinds[pg_idx];
        let col_descr = rg_reader.metadata().column(pq_idx).column_descr();
        let mut col_reader = rg_reader
            .get_column_reader(pq_idx)
            .map_err(|e| format!("pg_deltax: failed to read parquet column {}: {}", pq_idx, e))?;

        result[pg_idx] = read_column(&mut col_reader, col_descr, kind, num_rows)?;
    }

    Ok(result)
}

fn read_column(
    reader: &mut ColumnReader,
    col_descr: &parquet::schema::types::ColumnDescriptor,
    kind: ColumnKind,
    num_rows: usize,
) -> Result<TypedColumn, String> {
    match reader {
        ColumnReader::BoolColumnReader(r) => {
            let mut values = Vec::new();
            let mut def_levels = Vec::new();
            let (_records, num_values, _levels) = r
                .read_records(num_rows, Some(&mut def_levels), None, &mut values)
                .map_err(|e| format!("pg_deltax: parquet read error: {}", e))?;
            let unpacked = unpack_nullable(&values, &def_levels, num_rows, num_values);
            Ok(TypedColumn::Bool(unpacked))
        }
        ColumnReader::Int32ColumnReader(r) => {
            let mut values = Vec::new();
            let mut def_levels = Vec::new();
            let (_records, num_values, _levels) = r
                .read_records(num_rows, Some(&mut def_levels), None, &mut values)
                .map_err(|e| format!("pg_deltax: parquet read error: {}", e))?;
            let unpacked = unpack_nullable(&values, &def_levels, num_rows, num_values);
            Ok(match kind {
                ColumnKind::Int16 => {
                    TypedColumn::Int16(unpacked.into_iter().map(|v| v.map(|x| x as i16)).collect())
                }
                ColumnKind::Int32 => TypedColumn::Int32(unpacked),
                ColumnKind::Date => {
                    // Parquet DATE is days since Unix epoch → convert to Unix epoch usec
                    TypedColumn::Int64(
                        unpacked
                            .into_iter()
                            .map(|v| v.map(|d| (d as i64) * 86_400_000_000))
                            .collect(),
                    )
                }
                _ => {
                    TypedColumn::Int64(unpacked.into_iter().map(|v| v.map(|x| x as i64)).collect())
                }
            })
        }
        ColumnReader::Int64ColumnReader(r) => {
            let mut values = Vec::new();
            let mut def_levels = Vec::new();
            let (_records, num_values, _levels) = r
                .read_records(num_rows, Some(&mut def_levels), None, &mut values)
                .map_err(|e| format!("pg_deltax: parquet read error: {}", e))?;
            let unpacked = unpack_nullable(&values, &def_levels, num_rows, num_values);
            Ok(match kind {
                ColumnKind::Timestamp | ColumnKind::TimestampTz => {
                    let unit = detect_timestamp_unit(col_descr, &unpacked);
                    TypedColumn::Int64(
                        unpacked
                            .into_iter()
                            .map(|v| v.map(|ts| convert_timestamp(ts, &unit)))
                            .collect(),
                    )
                }
                _ => TypedColumn::Int64(unpacked),
            })
        }
        ColumnReader::FloatColumnReader(r) => {
            let mut values = Vec::new();
            let mut def_levels = Vec::new();
            let (_records, num_values, _levels) = r
                .read_records(num_rows, Some(&mut def_levels), None, &mut values)
                .map_err(|e| format!("pg_deltax: parquet read error: {}", e))?;
            let unpacked = unpack_nullable(&values, &def_levels, num_rows, num_values);
            Ok(match kind {
                ColumnKind::Float64 => TypedColumn::Float64(
                    unpacked.into_iter().map(|v| v.map(|x| x as f64)).collect(),
                ),
                _ => TypedColumn::Float32(unpacked),
            })
        }
        ColumnReader::DoubleColumnReader(r) => {
            let mut values = Vec::new();
            let mut def_levels = Vec::new();
            let (_records, num_values, _levels) = r
                .read_records(num_rows, Some(&mut def_levels), None, &mut values)
                .map_err(|e| format!("pg_deltax: parquet read error: {}", e))?;
            let unpacked = unpack_nullable(&values, &def_levels, num_rows, num_values);
            Ok(match kind {
                ColumnKind::Float32 => TypedColumn::Float32(
                    unpacked.into_iter().map(|v| v.map(|x| x as f32)).collect(),
                ),
                _ => TypedColumn::Float64(unpacked),
            })
        }
        ColumnReader::ByteArrayColumnReader(r) => {
            let mut values = Vec::new();
            let mut def_levels = Vec::new();
            let (_records, num_values, _levels) = r
                .read_records(num_rows, Some(&mut def_levels), None, &mut values)
                .map_err(|e| format!("pg_deltax: parquet read error: {}", e))?;
            let unpacked = unpack_nullable_byte_array(&values, &def_levels, num_rows, num_values)?;
            if matches!(kind, ColumnKind::Jsonb) {
                let bytes: Vec<Option<Vec<u8>>> = unpacked
                    .into_iter()
                    .map(|opt| opt.map(|s| unsafe { crate::compress::jsonb_text_to_binary(&s) }))
                    .collect();
                Ok(TypedColumn::Bytes(bytes))
            } else {
                Ok(TypedColumn::Text(unpacked))
            }
        }
        ColumnReader::FixedLenByteArrayColumnReader(r) => {
            let mut values = Vec::new();
            let mut def_levels = Vec::new();
            let (_records, num_values, _levels) = r
                .read_records(num_rows, Some(&mut def_levels), None, &mut values)
                .map_err(|e| format!("pg_deltax: parquet read error: {}", e))?;
            let unpacked =
                unpack_nullable_fixed_byte_array(&values, &def_levels, num_rows, num_values)?;
            Ok(TypedColumn::Text(unpacked))
        }
        ColumnReader::Int96ColumnReader(r) => {
            // INT96 is a legacy Parquet timestamp (Julian day + nanos within day).
            // Convert to Unix epoch microseconds.
            let mut values = Vec::new();
            let mut def_levels = Vec::new();
            let (_records, num_values, _levels) = r
                .read_records(num_rows, Some(&mut def_levels), None, &mut values)
                .map_err(|e| format!("pg_deltax: parquet read error: {}", e))?;
            let unpacked = unpack_nullable_int96(&values, &def_levels, num_rows, num_values);
            Ok(TypedColumn::Int64(unpacked))
        }
    }
}

/// Unpack nullable values from Parquet's packed non-null format using definition levels.
fn unpack_nullable<T: Copy>(
    values: &[T],
    def_levels: &[i16],
    num_rows: usize,
    num_values: usize,
) -> Vec<Option<T>> {
    // If all values are non-null, fast path
    if num_values == num_rows {
        return values.iter().map(|&v| Some(v)).collect();
    }
    let mut result = Vec::with_capacity(num_rows);
    let mut val_idx = 0;
    for &dl in def_levels.iter().take(num_rows) {
        if dl > 0 {
            result.push(Some(values[val_idx]));
            val_idx += 1;
        } else {
            result.push(None);
        }
    }
    result
}

/// Unpack nullable ByteArray values into Option<String>.
fn unpack_nullable_byte_array(
    values: &[parquet::data_type::ByteArray],
    def_levels: &[i16],
    num_rows: usize,
    num_values: usize,
) -> Result<Vec<Option<String>>, String> {
    if num_values == num_rows {
        return values
            .iter()
            .map(|v| {
                String::from_utf8(v.data().to_vec())
                    .map(Some)
                    .map_err(|_| "pg_deltax: invalid UTF-8 in parquet byte array".to_string())
            })
            .collect();
    }
    let mut result = Vec::with_capacity(num_rows);
    let mut val_idx = 0;
    for &dl in def_levels.iter().take(num_rows) {
        if dl > 0 {
            let bytes = values[val_idx].data();
            result.push(Some(String::from_utf8(bytes.to_vec()).map_err(|_| {
                "pg_deltax: invalid UTF-8 in parquet byte array".to_string()
            })?));
            val_idx += 1;
        } else {
            result.push(None);
        }
    }
    Ok(result)
}

/// Unpack nullable FixedLenByteArray values into Option<String>.
fn unpack_nullable_fixed_byte_array(
    values: &[parquet::data_type::FixedLenByteArray],
    def_levels: &[i16],
    num_rows: usize,
    num_values: usize,
) -> Result<Vec<Option<String>>, String> {
    if num_values == num_rows {
        return values
            .iter()
            .map(|v| {
                String::from_utf8(v.data().to_vec())
                    .map(Some)
                    .map_err(|_| "pg_deltax: invalid UTF-8 in parquet fixed-len column".to_string())
            })
            .collect();
    }
    let mut result = Vec::with_capacity(num_rows);
    let mut val_idx = 0;
    for &dl in def_levels.iter().take(num_rows) {
        if dl > 0 {
            let bytes = values[val_idx].data();
            result.push(Some(String::from_utf8(bytes.to_vec()).map_err(|_| {
                "pg_deltax: invalid UTF-8 in parquet fixed-len column".to_string()
            })?));
            val_idx += 1;
        } else {
            result.push(None);
        }
    }
    Ok(result)
}

/// Unpack nullable Int96 values (legacy Parquet timestamps) into Option<i64> usec since PG epoch.
fn unpack_nullable_int96(
    values: &[parquet::data_type::Int96],
    def_levels: &[i16],
    num_rows: usize,
    num_values: usize,
) -> Vec<Option<i64>> {
    // Julian day number for Unix epoch (1970-01-01)
    const JULIAN_UNIX_EPOCH: i64 = 2_440_588;
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;

    let convert = |v: &parquet::data_type::Int96| -> i64 {
        let data = v.data();
        let nanos_in_day = (data[0] as i64) | ((data[1] as i64) << 32);
        let julian_day = data[2] as i64;
        let days_since_unix = julian_day - JULIAN_UNIX_EPOCH;
        let unix_nanos = days_since_unix * NANOS_PER_DAY + nanos_in_day;
        unix_nanos / 1000 // Unix epoch microseconds
    };

    if num_values == num_rows {
        return values.iter().map(|v| Some(convert(v))).collect();
    }
    let mut result = Vec::with_capacity(num_rows);
    let mut val_idx = 0;
    for &dl in def_levels.iter().take(num_rows) {
        if dl > 0 {
            result.push(Some(convert(&values[val_idx])));
            val_idx += 1;
        } else {
            result.push(None);
        }
    }
    result
}

/// Determine the timestamp time unit from Parquet logical type annotation,
/// falling back to magnitude-based auto-detection for bare INT64 columns.
fn detect_timestamp_unit(
    col_descr: &parquet::schema::types::ColumnDescriptor,
    values: &[Option<i64>],
) -> TimestampUnit {
    // If Parquet schema has an explicit timestamp annotation, trust it.
    if let Some(LogicalType::Timestamp { unit, .. }) = col_descr.logical_type() {
        return match unit {
            ParquetTimeUnit::MILLIS(_) => TimestampUnit::Millis,
            ParquetTimeUnit::MICROS(_) => TimestampUnit::Micros,
            ParquetTimeUnit::NANOS(_) => TimestampUnit::Nanos,
        };
    }

    // No annotation — auto-detect from value magnitude using first non-null value.
    // Seconds:      ~1.3e9  (year 2013)
    // Milliseconds: ~1.3e12
    // Microseconds: ~1.3e15
    // Nanoseconds:  ~1.3e18
    if let Some(sample) = values.iter().filter_map(|v| *v).next() {
        let abs = sample.abs();
        if abs < 1_000_000_000_000 {
            // < 1e12 → seconds (values up to year ~33658)
            TimestampUnit::Seconds
        } else if abs < 1_000_000_000_000_000 {
            // < 1e15 → milliseconds
            TimestampUnit::Millis
        } else if abs < 1_000_000_000_000_000_000 {
            // < 1e18 → microseconds
            TimestampUnit::Micros
        } else {
            TimestampUnit::Nanos
        }
    } else {
        TimestampUnit::Micros // all null, doesn't matter
    }
}

enum TimestampUnit {
    Seconds,
    Millis,
    Micros,
    Nanos,
}

/// Convert a Parquet timestamp to Unix epoch microseconds.
/// TypedColumn stores Unix epoch usec (same as the TSV path), not PG epoch.
fn convert_timestamp(value: i64, unit: &TimestampUnit) -> i64 {
    match unit {
        TimestampUnit::Seconds => value * 1_000_000,
        TimestampUnit::Millis => value * 1_000,
        TimestampUnit::Micros => value,
        TimestampUnit::Nanos => value / 1_000,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compress::ColumnMeta;

    // ── unpack_nullable ──────────────────────────────────────────────

    #[test]
    fn test_unpack_nullable_all_present() {
        let values = vec![10i32, 20, 30];
        let def_levels = vec![1i16, 1, 1];
        let result = unpack_nullable(&values, &def_levels, 3, 3);
        assert_eq!(result, vec![Some(10), Some(20), Some(30)]);
    }

    #[test]
    fn test_unpack_nullable_with_nulls() {
        let values = vec![10i32, 30]; // only non-null values
        let def_levels = vec![1i16, 0, 1]; // second row is null
        let result = unpack_nullable(&values, &def_levels, 3, 2);
        assert_eq!(result, vec![Some(10), None, Some(30)]);
    }

    #[test]
    fn test_unpack_nullable_all_null() {
        let values: Vec<i64> = vec![];
        let def_levels = vec![0i16, 0, 0];
        let result = unpack_nullable(&values, &def_levels, 3, 0);
        assert_eq!(result, vec![None, None, None]);
    }

    #[test]
    fn test_unpack_nullable_empty() {
        let values: Vec<f64> = vec![];
        let def_levels: Vec<i16> = vec![];
        let result = unpack_nullable(&values, &def_levels, 0, 0);
        assert_eq!(result, Vec::<Option<f64>>::new());
    }

    // ── convert_timestamp ────────────────────────────────────────────

    #[test]
    fn test_convert_timestamp_seconds() {
        let ts = 1_372_000_000; // ~2013-06-23
        assert_eq!(
            convert_timestamp(ts, &TimestampUnit::Seconds),
            ts * 1_000_000
        );
    }

    #[test]
    fn test_convert_timestamp_millis() {
        let ts = 1_372_000_000_000i64;
        assert_eq!(convert_timestamp(ts, &TimestampUnit::Millis), ts * 1_000);
    }

    #[test]
    fn test_convert_timestamp_micros() {
        let ts = 1_372_000_000_000_000i64;
        assert_eq!(convert_timestamp(ts, &TimestampUnit::Micros), ts);
    }

    #[test]
    fn test_convert_timestamp_nanos() {
        let ts = 1_372_000_000_000_000_000i64;
        assert_eq!(convert_timestamp(ts, &TimestampUnit::Nanos), ts / 1_000);
    }

    // ── detect_timestamp_unit (auto-detect, no schema annotation) ───

    fn make_bare_col_descr() -> parquet::schema::types::ColumnDescriptor {
        use parquet::basic::Type as PhysicalType;
        use parquet::schema::types::{ColumnPath, Type};
        let typ = Type::primitive_type_builder("ts", PhysicalType::INT64)
            .build()
            .unwrap();
        parquet::schema::types::ColumnDescriptor::new(
            std::sync::Arc::new(typ),
            0, // max def level
            0, // max rep level
            ColumnPath::new(vec!["ts".into()]),
        )
    }

    #[test]
    fn test_detect_seconds() {
        let desc = make_bare_col_descr();
        let vals = vec![Some(1_372_000_000i64)]; // ~1.3e9
        let unit = detect_timestamp_unit(&desc, &vals);
        assert_eq!(convert_timestamp(1, &unit), 1_000_000); // seconds
    }

    #[test]
    fn test_detect_millis() {
        let desc = make_bare_col_descr();
        let vals = vec![Some(1_372_000_000_000i64)]; // ~1.3e12
        let unit = detect_timestamp_unit(&desc, &vals);
        assert_eq!(convert_timestamp(1, &unit), 1_000); // millis
    }

    #[test]
    fn test_detect_micros() {
        let desc = make_bare_col_descr();
        let vals = vec![Some(1_372_000_000_000_000i64)]; // ~1.3e15
        let unit = detect_timestamp_unit(&desc, &vals);
        assert_eq!(convert_timestamp(1, &unit), 1); // micros
    }

    #[test]
    fn test_detect_nanos() {
        let desc = make_bare_col_descr();
        let vals = vec![Some(1_372_000_000_000_000_000i64)]; // ~1.3e18
        let unit = detect_timestamp_unit(&desc, &vals);
        assert_eq!(convert_timestamp(1_000, &unit), 1); // nanos
    }

    #[test]
    fn test_detect_all_null_defaults_to_micros() {
        let desc = make_bare_col_descr();
        let vals = vec![None, None];
        let unit = detect_timestamp_unit(&desc, &vals);
        assert_eq!(convert_timestamp(1, &unit), 1); // micros passthrough
    }

    // ── map_parquet_to_pg_columns ────────────────────────────────────

    fn make_parquet_schema(names: &[&str]) -> SchemaDescriptor {
        use parquet::basic::Type as PhysicalType;
        use parquet::schema::types::Type;
        let fields: Vec<_> = names
            .iter()
            .map(|name| {
                std::sync::Arc::new(
                    Type::primitive_type_builder(name, PhysicalType::INT64)
                        .build()
                        .unwrap(),
                )
            })
            .collect();
        let schema = Type::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .unwrap();
        SchemaDescriptor::new(std::sync::Arc::new(schema))
    }

    fn col_meta(name: &str, is_segment_by: bool) -> ColumnMeta {
        ColumnMeta {
            name: name.into(),
            data_type: "bigint".into(),
            is_segment_by,
            is_time_column: false,
            extracted: None,
        }
    }

    #[test]
    fn test_mapping_exact() {
        let schema = make_parquet_schema(&["ts", "value"]);
        let pg_cols = vec![col_meta("ts", false), col_meta("value", false)];
        let mapping = map_parquet_to_pg_columns(&schema, &pg_cols).unwrap();
        assert_eq!(mapping, vec![(0, 0), (1, 1)]);
    }

    #[test]
    fn test_mapping_case_insensitive() {
        let schema = make_parquet_schema(&["TS", "Value"]);
        let pg_cols = vec![col_meta("ts", false), col_meta("value", false)];
        let mapping = map_parquet_to_pg_columns(&schema, &pg_cols).unwrap();
        assert_eq!(mapping, vec![(0, 0), (1, 1)]);
    }

    #[test]
    fn test_mapping_reorder() {
        let schema = make_parquet_schema(&["value", "ts"]);
        let pg_cols = vec![col_meta("ts", false), col_meta("value", false)];
        let mapping = map_parquet_to_pg_columns(&schema, &pg_cols).unwrap();
        // ts maps to parquet idx 1, value to parquet idx 0
        assert_eq!(mapping, vec![(1, 0), (0, 1)]);
    }

    #[test]
    fn test_mapping_missing_column_errors() {
        let schema = make_parquet_schema(&["ts"]);
        let pg_cols = vec![col_meta("ts", false), col_meta("value", false)];
        let result = map_parquet_to_pg_columns(&schema, &pg_cols);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("value"));
    }

    #[test]
    fn test_mapping_missing_segment_by_ok() {
        let schema = make_parquet_schema(&["ts"]);
        let pg_cols = vec![col_meta("ts", false), col_meta("device", true)];
        let mapping = map_parquet_to_pg_columns(&schema, &pg_cols).unwrap();
        // Only ts is mapped; device (segment_by) is skipped
        assert_eq!(mapping, vec![(0, 0)]);
    }

    #[test]
    fn test_mapping_extra_parquet_columns_ignored() {
        let schema = make_parquet_schema(&["ts", "extra1", "value", "extra2"]);
        let pg_cols = vec![col_meta("ts", false), col_meta("value", false)];
        let mapping = map_parquet_to_pg_columns(&schema, &pg_cols).unwrap();
        assert_eq!(mapping, vec![(0, 0), (2, 1)]);
    }
}
