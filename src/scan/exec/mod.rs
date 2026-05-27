mod agg;
mod agg_wire;
mod append_wire;
mod batch_qual;
mod count_minmax;
pub(in crate::scan) mod datum_utils;
mod decompress;
pub(in crate::scan) mod segments;
mod text_col;

use pgrx::pg_sys;

use super::SyncStatic;

// Re-exports for explain.rs
pub(crate) use agg::AggScanState;
pub(crate) use count_minmax::{CountScanState, MinMaxScanState};
pub(crate) use decompress::DecompressState;

// Re-exports for hook.rs
pub(crate) use agg::{
    AggExpr, AggType, CaseWhenClause, CaseWhenCondition, CaseWhenOp, CaseWhenSpec, CaseWhenValue,
    GroupByColSpec, GroupByExpr, HavingFilter, HavingOp, OutputTransform,
};

// Re-export for cost.rs (parallel-agg worker recommendation needs the slot cap).
pub(crate) use agg::MAX_AGG_WORKER_SLOTS;

// Re-export for path.rs's parallel-eligibility check (C.2.f).
pub(crate) use agg::can_use_compact_keys_path;

// Re-exports for path.rs (create_*_state callbacks referenced in CustomScanMethods)
pub(crate) use agg::create_agg_scan_state;
pub(crate) use count_minmax::{create_count_scan_state, create_minmax_scan_state};
pub(crate) use decompress::{create_custom_scan_state, create_deltax_append_state};

// Callback imports for static method tables
use count_minmax::{begin_count_scan, end_count_scan, exec_count_scan, rescan_count_scan};
use count_minmax::{begin_minmax_scan, end_minmax_scan, exec_minmax_scan, rescan_minmax_scan};
use decompress::begin_deltax_append;
use decompress::{begin_custom_scan, end_custom_scan, exec_custom_scan, rescan_custom_scan};
use decompress::{
    estimate_dsm_deltax_append, init_worker_deltax_append, initialize_dsm_deltax_append,
    reinit_dsm_deltax_append, shutdown_deltax_append,
};

/// Static CustomExecMethods struct for DeltaXDecompress.
pub(crate) static CUSTOM_EXEC_METHODS: SyncStatic<pg_sys::CustomExecMethods> =
    SyncStatic(pg_sys::CustomExecMethods {
        CustomName: super::CUSTOM_NAME.as_ptr(),
        BeginCustomScan: Some(begin_custom_scan),
        ExecCustomScan: Some(exec_custom_scan),
        EndCustomScan: Some(end_custom_scan),
        ReScanCustomScan: Some(rescan_custom_scan),
        MarkPosCustomScan: None,
        RestrPosCustomScan: None,
        EstimateDSMCustomScan: None,
        InitializeDSMCustomScan: None,
        ReInitializeDSMCustomScan: None,
        InitializeWorkerCustomScan: None,
        ShutdownCustomScan: None,
        ExplainCustomScan: Some(super::explain::explain_custom_scan),
    });

/// Static CustomExecMethods struct for DeltaXCount (COUNT(*) pushdown).
pub(crate) static DELTAX_COUNT_EXEC_METHODS: SyncStatic<pg_sys::CustomExecMethods> =
    SyncStatic(pg_sys::CustomExecMethods {
        CustomName: super::DELTAX_COUNT_NAME.as_ptr(),
        BeginCustomScan: Some(begin_count_scan),
        ExecCustomScan: Some(exec_count_scan),
        EndCustomScan: Some(end_count_scan),
        ReScanCustomScan: Some(rescan_count_scan),
        MarkPosCustomScan: None,
        RestrPosCustomScan: None,
        EstimateDSMCustomScan: None,
        InitializeDSMCustomScan: None,
        ReInitializeDSMCustomScan: None,
        InitializeWorkerCustomScan: None,
        ShutdownCustomScan: None,
        ExplainCustomScan: Some(super::explain::explain_count_scan),
    });

/// Static CustomExecMethods struct for DeltaXMinMax (MIN/MAX pushdown).
pub(crate) static DELTAX_MINMAX_EXEC_METHODS: SyncStatic<pg_sys::CustomExecMethods> =
    SyncStatic(pg_sys::CustomExecMethods {
        CustomName: super::DELTAX_MINMAX_NAME.as_ptr(),
        BeginCustomScan: Some(begin_minmax_scan),
        ExecCustomScan: Some(exec_minmax_scan),
        EndCustomScan: Some(end_minmax_scan),
        ReScanCustomScan: Some(rescan_minmax_scan),
        MarkPosCustomScan: None,
        RestrPosCustomScan: None,
        EstimateDSMCustomScan: None,
        InitializeDSMCustomScan: None,
        ReInitializeDSMCustomScan: None,
        InitializeWorkerCustomScan: None,
        ShutdownCustomScan: None,
        ExplainCustomScan: Some(super::explain::explain_minmax_scan),
    });

/// Static CustomExecMethods struct for DeltaXAppend.
pub(crate) static DELTAX_APPEND_EXEC_METHODS: SyncStatic<pg_sys::CustomExecMethods> =
    SyncStatic(pg_sys::CustomExecMethods {
        CustomName: super::DELTAX_APPEND_NAME.as_ptr(),
        BeginCustomScan: Some(begin_deltax_append),
        ExecCustomScan: Some(exec_custom_scan),
        EndCustomScan: Some(end_custom_scan),
        ReScanCustomScan: Some(rescan_custom_scan),
        MarkPosCustomScan: None,
        RestrPosCustomScan: None,
        EstimateDSMCustomScan: Some(estimate_dsm_deltax_append),
        InitializeDSMCustomScan: Some(initialize_dsm_deltax_append),
        ReInitializeDSMCustomScan: Some(reinit_dsm_deltax_append),
        InitializeWorkerCustomScan: Some(init_worker_deltax_append),
        ShutdownCustomScan: Some(shutdown_deltax_append),
        ExplainCustomScan: Some(super::explain::explain_deltax_append),
    });

// Epoch offset: microseconds between Unix epoch (1970-01-01) and PG epoch (2000-01-01).
const PG_EPOCH_OFFSET_USEC: i64 = 946_684_800_000_000;
// Days between Unix epoch and PG epoch.
const PG_EPOCH_OFFSET_DAYS: i32 = 10_957;

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    use super::{PG_EPOCH_OFFSET_DAYS, PG_EPOCH_OFFSET_USEC};

    #[pg_test]
    fn test_pg_epoch_offset_usec() {
        // PG_EPOCH_OFFSET_USEC must equal the number of microseconds between
        // the Unix epoch (1970-01-01) and the PostgreSQL epoch (2000-01-01).
        let pg_val: i64 = Spi::get_one(
            "SELECT (EXTRACT(EPOCH FROM '2000-01-01 00:00:00+00'::timestamptz) * 1000000)::bigint",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            pg_val, PG_EPOCH_OFFSET_USEC,
            "PG_EPOCH_OFFSET_USEC ({}) does not match PG's epoch ({})",
            PG_EPOCH_OFFSET_USEC, pg_val
        );
    }

    #[pg_test]
    fn test_pg_epoch_offset_days() {
        // PG_EPOCH_OFFSET_DAYS must equal the number of days between
        // the Unix epoch (1970-01-01) and the PostgreSQL epoch (2000-01-01).
        let pg_val: i32 = Spi::get_one("SELECT ('2000-01-01'::date - '1970-01-01'::date)::int")
            .unwrap()
            .unwrap();
        assert_eq!(
            pg_val, PG_EPOCH_OFFSET_DAYS,
            "PG_EPOCH_OFFSET_DAYS ({}) does not match PG's value ({})",
            PG_EPOCH_OFFSET_DAYS, pg_val
        );
    }

    #[pg_test]
    fn test_timestamp_datum_matches_pg() {
        // Verify our epoch math produces the same internal representation PG uses.
        // PG stores timestamptz as microseconds since 2000-01-01 00:00:00 UTC.
        let test_cases = [
            "1970-01-01 00:00:00+00",
            "2000-01-01 00:00:00+00",
            "2013-07-14 12:34:56+00",
            "1969-12-31 23:59:59+00",
            "2025-01-15 00:00:00+00",
        ];

        for ts_str in &test_cases {
            // Get PG's internal representation (usec since PG epoch)
            let pg_internal: i64 = Spi::get_one(&format!(
                "SELECT (EXTRACT(EPOCH FROM '{}'::timestamptz) * 1000000)::bigint - {}::bigint",
                ts_str, PG_EPOCH_OFFSET_USEC
            ))
            .unwrap()
            .unwrap();

            // Our conversion: unix_usec - PG_EPOCH_OFFSET_USEC
            let unix_usec: i64 = Spi::get_one(&format!(
                "SELECT (EXTRACT(EPOCH FROM '{}'::timestamptz) * 1000000)::bigint",
                ts_str
            ))
            .unwrap()
            .unwrap();
            let our_datum = unix_usec - PG_EPOCH_OFFSET_USEC;

            assert_eq!(
                our_datum, pg_internal,
                "timestamp datum mismatch for {}: ours={} pg={}",
                ts_str, our_datum, pg_internal
            );
        }
    }

    #[pg_test]
    fn test_date_datum_matches_pg() {
        // PG stores dates as days since 2000-01-01.
        let test_cases = [
            ("1970-01-01", -10957), // -PG_EPOCH_OFFSET_DAYS
            ("2000-01-01", 0),
            ("2025-01-15", 9146),
            ("1969-12-31", -10958),
        ];

        for (date_str, expected_pg_days) in &test_cases {
            // Get PG's internal representation (days since PG epoch)
            let pg_internal: i32 = Spi::get_one(&format!(
                "SELECT ('{}'::date - '2000-01-01'::date)::int",
                date_str
            ))
            .unwrap()
            .unwrap();

            assert_eq!(
                pg_internal, *expected_pg_days,
                "date sanity check failed for {}: pg={} expected={}",
                date_str, pg_internal, expected_pg_days
            );

            // Our conversion: unix_days - PG_EPOCH_OFFSET_DAYS
            let unix_days: i32 = Spi::get_one(&format!(
                "SELECT ('{}'::date - '1970-01-01'::date)::int",
                date_str
            ))
            .unwrap()
            .unwrap();
            let our_datum = unix_days - PG_EPOCH_OFFSET_DAYS;

            assert_eq!(
                our_datum, pg_internal,
                "date datum mismatch for {}: ours={} pg={}",
                date_str, our_datum, pg_internal
            );
        }
    }

    #[pg_test]
    fn test_float_datum_bit_preservation() {
        // Verify that f64 values survive Gorilla encode/decode with identical bits.
        use crate::compression::gorilla;

        let test_values: Vec<f64> = vec![
            0.0,
            -0.0,
            1.0,
            -1.0,
            std::f64::consts::PI,
            1e308,
            -1e308,
            1e-307,
            f64::MIN_POSITIVE,
        ];

        let encoded = gorilla::encode_floats(&test_values);
        let decoded = gorilla::decode_floats(&encoded, test_values.len());

        for (orig, dec) in test_values.iter().zip(decoded.iter()) {
            assert_eq!(
                orig.to_bits(),
                dec.to_bits(),
                "float bit mismatch: orig={} (0x{:016x}) decoded={} (0x{:016x})",
                orig,
                orig.to_bits(),
                dec,
                dec.to_bits()
            );
        }
    }

    #[test]
    fn test_reinsert_nulls_datum() {
        use super::datum_utils::reinsert_nulls_datum;
        use pgrx::pg_sys;

        // No nulls: empty bitmap
        let datums = vec![
            pg_sys::Datum::from(1usize),
            pg_sys::Datum::from(2usize),
            pg_sys::Datum::from(3usize),
        ];
        let result = reinsert_nulls_datum(&datums, &[], 3);
        assert_eq!(result.len(), 3);
        assert!(!result[0].1);
        assert!(!result[1].1);
        assert!(!result[2].1);

        // All nulls
        let bitmap = vec![0b11111111u8];
        let result = reinsert_nulls_datum(&[], &bitmap, 4);
        assert_eq!(result.len(), 4);
        for (_, is_null) in &result {
            assert!(is_null, "expected null");
        }

        // Alternating: null at 0, 2 (bits 0 and 2 set)
        let bitmap = vec![0b00000101u8];
        let datums = vec![pg_sys::Datum::from(10usize), pg_sys::Datum::from(30usize)];
        let result = reinsert_nulls_datum(&datums, &bitmap, 4);
        assert_eq!(result.len(), 4);
        assert!(result[0].1); // null
        assert!(!result[1].1); // 10
        assert!(result[2].1); // null
        assert!(!result[3].1); // 30
        assert_eq!(result[1].0, pg_sys::Datum::from(10usize));
        assert_eq!(result[3].0, pg_sys::Datum::from(30usize));

        // Sparse: only position 5 is null in 8 values
        let bitmap = vec![0b00100000u8];
        let datums: Vec<pg_sys::Datum> = (0..7).map(|i| pg_sys::Datum::from(i as usize)).collect();
        let result = reinsert_nulls_datum(&datums, &bitmap, 8);
        assert_eq!(result.len(), 8);
        for (i, (_datum, is_null)) in result.iter().enumerate().take(8) {
            if i == 5 {
                assert!(is_null, "position 5 should be null");
            } else {
                assert!(!is_null, "position {} should not be null", i);
            }
        }
    }

    #[test]
    fn test_count_non_null() {
        use super::datum_utils::count_non_null;

        // No nulls (empty bitmap)
        assert_eq!(count_non_null(&[], 10), 10);

        // All nulls (8 rows, all bits set)
        assert_eq!(count_non_null(&[0xFF], 8), 0);

        // Some nulls: bits 0 and 2 set (positions 0,2 are null)
        assert_eq!(count_non_null(&[0b00000101], 4), 2);

        // Partial last byte: 5 rows, bits 0,3 set
        assert_eq!(count_non_null(&[0b00001001], 5), 3);

        // 16 rows across 2 bytes, 3 nulls
        assert_eq!(count_non_null(&[0b00000001, 0b00000110], 16), 13);
    }

    #[test]
    fn test_compare_datums() {
        use super::datum_utils::compare_datums;
        use pgrx::pg_sys;
        use std::cmp::Ordering;

        // int4
        let d1 = pg_sys::Datum::from(10i32 as usize);
        let d2 = pg_sys::Datum::from(20i32 as usize);
        assert_eq!(compare_datums(d1, d2, pg_sys::INT4OID), Ordering::Less);
        assert_eq!(compare_datums(d2, d1, pg_sys::INT4OID), Ordering::Greater);
        assert_eq!(compare_datums(d1, d1, pg_sys::INT4OID), Ordering::Equal);

        // int2
        let d1 = pg_sys::Datum::from(5i16 as usize);
        let d2 = pg_sys::Datum::from(3i16 as usize);
        assert_eq!(compare_datums(d1, d2, pg_sys::INT2OID), Ordering::Greater);

        // float8
        let d1 = pg_sys::Datum::from(1.5f64.to_bits() as usize);
        let d2 = pg_sys::Datum::from(2.5f64.to_bits() as usize);
        assert_eq!(compare_datums(d1, d2, pg_sys::FLOAT8OID), Ordering::Less);

        // float4
        let d1 = pg_sys::Datum::from(3.0f32.to_bits() as usize);
        let d2 = pg_sys::Datum::from(1.0f32.to_bits() as usize);
        assert_eq!(compare_datums(d1, d2, pg_sys::FLOAT4OID), Ordering::Greater);

        // unsupported type returns Equal
        assert_eq!(compare_datums(d1, d2, pg_sys::TEXTOID), Ordering::Equal);
    }

    #[test]
    fn test_pg_type_name() {
        use super::datum_utils::pg_type_name;
        use pgrx::pg_sys;

        assert_eq!(pg_type_name(pg_sys::INT4OID), "integer");
        assert_eq!(pg_type_name(pg_sys::INT8OID), "bigint");
        assert_eq!(pg_type_name(pg_sys::FLOAT8OID), "double precision");
        assert_eq!(pg_type_name(pg_sys::BOOLOID), "boolean");
        assert_eq!(pg_type_name(pg_sys::TEXTOID), "text"); // fallback
    }

    /// Helper: build a compressed blob from a CompressionType tag and encoded data.
    fn make_blob(
        tag: crate::compression::CompressionType,
        row_count: u32,
        data: Vec<u8>,
    ) -> Vec<u8> {
        crate::compression::CompressedColumn {
            type_tag: tag,
            row_count,
            null_bitmap: Vec::new(),
            data,
        }
        .to_bytes()
    }

    /// Helper: build a compressed blob with a null bitmap.
    fn make_blob_with_nulls(
        tag: crate::compression::CompressionType,
        row_count: u32,
        null_bitmap: Vec<u8>,
        data: Vec<u8>,
    ) -> Vec<u8> {
        crate::compression::CompressedColumn {
            type_tag: tag,
            row_count,
            null_bitmap,
            data,
        }
        .to_bytes()
    }

    #[pg_test]
    fn test_decompress_constant_i32() {
        use super::datum_utils::decompress_blob_to_datums;
        use crate::compression::{CompressionType, bitpacked};

        let data = bitpacked::encode_constant_i32(42);
        let blob = make_blob(CompressionType::Constant, 5, data);
        let result = unsafe { decompress_blob_to_datums(&blob, "integer", pg_sys::INT4OID, -1) };
        assert_eq!(result.len(), 5);
        for (d, is_null) in &result {
            assert!(!is_null);
            assert_eq!(d.value() as i32, 42);
        }
    }

    #[pg_test]
    fn test_decompress_constant_i64() {
        use super::datum_utils::decompress_blob_to_datums;
        use crate::compression::{CompressionType, bitpacked};

        let data = bitpacked::encode_constant_i64(999_999);
        let blob = make_blob(CompressionType::Constant, 3, data);
        let result = unsafe { decompress_blob_to_datums(&blob, "bigint", pg_sys::INT8OID, -1) };
        assert_eq!(result.len(), 3);
        for (d, is_null) in &result {
            assert!(!is_null);
            assert_eq!(d.value() as i64, 999_999);
        }
    }

    #[pg_test]
    fn test_decompress_constant_smallint() {
        use super::datum_utils::decompress_blob_to_datums;
        use crate::compression::{CompressionType, bitpacked};

        let data = bitpacked::encode_constant_i32(7);
        let blob = make_blob(CompressionType::Constant, 4, data);
        let result = unsafe { decompress_blob_to_datums(&blob, "smallint", pg_sys::INT2OID, -1) };
        assert_eq!(result.len(), 4);
        for (d, is_null) in &result {
            assert!(!is_null);
            assert_eq!(d.value() as i16, 7);
        }
    }

    #[pg_test]
    fn test_decompress_for_bitpacked_i32() {
        use super::datum_utils::decompress_blob_to_datums;
        use crate::compression::{CompressionType, bitpacked};

        let values: Vec<i32> = vec![10, 20, 30, 40, 50];
        let data = bitpacked::encode_for_i32(&values);
        let blob = make_blob(CompressionType::ForBitpacked, 5, data);
        let result = unsafe { decompress_blob_to_datums(&blob, "integer", pg_sys::INT4OID, -1) };
        assert_eq!(result.len(), 5);
        for (i, (d, is_null)) in result.iter().enumerate() {
            assert!(!is_null);
            assert_eq!(d.value() as i32, values[i]);
        }
    }

    #[pg_test]
    fn test_decompress_for_bitpacked_i64() {
        use super::datum_utils::decompress_blob_to_datums;
        use crate::compression::{CompressionType, bitpacked};

        let values: Vec<i64> = vec![100, 200, 300];
        let data = bitpacked::encode_for_i64(&values);
        let blob = make_blob(CompressionType::ForBitpacked, 3, data);
        let result = unsafe { decompress_blob_to_datums(&blob, "bigint", pg_sys::INT8OID, -1) };
        assert_eq!(result.len(), 3);
        for (i, (d, is_null)) in result.iter().enumerate() {
            assert!(!is_null);
            assert_eq!(d.value() as i64, values[i]);
        }
    }

    #[pg_test]
    fn test_decompress_for_bitpacked_smallint() {
        use super::datum_utils::decompress_blob_to_datums;
        use crate::compression::{CompressionType, bitpacked};

        let values: Vec<i32> = vec![1, 2, 3, 4];
        let data = bitpacked::encode_for_i32(&values);
        let blob = make_blob(CompressionType::ForBitpacked, 4, data);
        let result = unsafe { decompress_blob_to_datums(&blob, "smallint", pg_sys::INT2OID, -1) };
        assert_eq!(result.len(), 4);
        for (i, (d, is_null)) in result.iter().enumerate() {
            assert!(!is_null);
            assert_eq!(d.value() as i16, values[i] as i16);
        }
    }

    #[pg_test]
    fn test_decompress_boolean() {
        use super::datum_utils::decompress_blob_to_datums;
        use crate::compression::{CompressionType, boolean};

        let values = vec![true, false, true, true, false];
        let data = boolean::encode(&values);
        let blob = make_blob(CompressionType::BooleanBitmap, 5, data);
        let result = unsafe { decompress_blob_to_datums(&blob, "boolean", pg_sys::BOOLOID, -1) };
        assert_eq!(result.len(), 5);
        for (i, (d, is_null)) in result.iter().enumerate() {
            assert!(!is_null);
            assert_eq!(d.value() != 0, values[i]);
        }
    }

    #[pg_test]
    fn test_decompress_gorilla_float4() {
        use super::datum_utils::decompress_blob_to_datums;
        use crate::compression::{CompressionType, gorilla};

        let values: Vec<f32> = vec![1.5, 2.5, 3.5];
        let data = gorilla::encode_floats_f32(&values);
        let blob = make_blob(CompressionType::Gorilla, 3, data);
        let result = unsafe { decompress_blob_to_datums(&blob, "real", pg_sys::FLOAT4OID, -1) };
        assert_eq!(result.len(), 3);
        for (i, (d, is_null)) in result.iter().enumerate() {
            assert!(!is_null);
            let decoded = f32::from_bits(d.value() as u32);
            assert_eq!(decoded, values[i]);
        }
    }

    #[pg_test]
    fn test_decompress_gorilla_date() {
        use super::datum_utils::decompress_blob_to_datums;
        use crate::compression::{CompressionType, gorilla};

        // Dates stored as unix-epoch microseconds (midnight)
        let day_usec = 86_400_000_000i64;
        let values: Vec<i64> = vec![0, day_usec, day_usec * 2]; // 1970-01-01, 02, 03
        let data = gorilla::encode_timestamps(&values);
        let blob = make_blob(CompressionType::Gorilla, 3, data);
        let result = unsafe { decompress_blob_to_datums(&blob, "date", pg_sys::DATEOID, -1) };
        assert_eq!(result.len(), 3);
        // Verify PG days are correct: unix_days - PG_EPOCH_OFFSET_DAYS
        for (i, (d, is_null)) in result.iter().enumerate() {
            assert!(!is_null);
            let pg_days = d.value() as i32;
            let expected = i as i32 - PG_EPOCH_OFFSET_DAYS;
            assert_eq!(pg_days, expected, "date mismatch at index {}", i);
        }
    }

    #[pg_test]
    fn test_decompress_delta_varint_smallint() {
        use super::datum_utils::decompress_blob_to_datums;
        use crate::compression::{CompressionType, integer};

        let values: Vec<i32> = vec![100, 200, 300];
        let data = integer::encode_i32(&values);
        let blob = make_blob(CompressionType::DeltaVarint, 3, data);
        let result = unsafe { decompress_blob_to_datums(&blob, "smallint", pg_sys::INT2OID, -1) };
        assert_eq!(result.len(), 3);
        for (i, (d, is_null)) in result.iter().enumerate() {
            assert!(!is_null);
            assert_eq!(d.value() as i16, values[i] as i16);
        }
    }

    #[pg_test]
    fn test_decompress_with_nulls() {
        use super::datum_utils::decompress_blob_to_datums;
        use crate::compression::{CompressionType, integer};

        // 4 rows, positions 0 and 2 are null (bitmap bit 0 and 2 set)
        let non_null_values: Vec<i32> = vec![10, 30]; // values at positions 1 and 3
        let data = integer::encode_i32(&non_null_values);
        let null_bitmap = vec![0b00000101u8]; // bits 0,2 = null
        let blob = make_blob_with_nulls(CompressionType::DeltaVarint, 4, null_bitmap, data);
        let result = unsafe { decompress_blob_to_datums(&blob, "integer", pg_sys::INT4OID, -1) };
        assert_eq!(result.len(), 4);
        assert!(result[0].1, "position 0 should be null");
        assert!(!result[1].1);
        assert_eq!(result[1].0.value() as i32, 10);
        assert!(result[2].1, "position 2 should be null");
        assert!(!result[3].1);
        assert_eq!(result[3].0.value() as i32, 30);
    }

    #[pg_test]
    fn test_decompress_truncated_basic() {
        use super::datum_utils::decompress_blob_to_datums_truncated;
        use crate::compression::{CompressionType, integer};

        // 10 values, truncate to first 3
        let values: Vec<i32> = (0..10).collect();
        let data = integer::encode_i32(&values);
        let blob = make_blob(CompressionType::DeltaVarint, 10, data);
        let result = unsafe {
            decompress_blob_to_datums_truncated(&blob, "integer", pg_sys::INT4OID, -1, 2)
        };
        // max_row=2 means rows 0,1,2 → 3 elements
        assert_eq!(result.len(), 3);
        for (i, (d, is_null)) in result.iter().enumerate() {
            assert!(!is_null);
            assert_eq!(d.value() as i32, i as i32);
        }
    }

    #[pg_test]
    fn test_decompress_truncated_falls_back_when_no_benefit() {
        use super::datum_utils::decompress_blob_to_datums_truncated;
        use crate::compression::{CompressionType, integer};

        // 5 values, max_row=10 → truncated_count >= total_count, uses full path
        let values: Vec<i32> = vec![1, 2, 3, 4, 5];
        let data = integer::encode_i32(&values);
        let blob = make_blob(CompressionType::DeltaVarint, 5, data);
        let result = unsafe {
            decompress_blob_to_datums_truncated(&blob, "integer", pg_sys::INT4OID, -1, 10)
        };
        assert_eq!(result.len(), 5);
        for (i, (d, is_null)) in result.iter().enumerate() {
            assert!(!is_null);
            assert_eq!(d.value() as i32, values[i]);
        }
    }

    #[pg_test]
    fn test_decompress_truncated_with_nulls() {
        use super::datum_utils::decompress_blob_to_datums_truncated;
        use crate::compression::{CompressionType, integer};

        // 8 rows, null at positions 1 and 5. Truncate to first 4 (max_row=3).
        // Positions 0-3: null at 1, non-null at 0,2,3
        let non_null_values: Vec<i32> = vec![10, 20, 30, 40, 50, 60];
        let data = integer::encode_i32(&non_null_values);
        let null_bitmap = vec![0b00100010u8]; // bits 1,5 = null
        let blob = make_blob_with_nulls(CompressionType::DeltaVarint, 8, null_bitmap, data);
        let result = unsafe {
            decompress_blob_to_datums_truncated(&blob, "integer", pg_sys::INT4OID, -1, 3)
        };
        assert_eq!(result.len(), 4); // rows 0,1,2,3
        assert!(!result[0].1);
        assert_eq!(result[0].0.value() as i32, 10);
        assert!(result[1].1, "position 1 should be null");
        assert!(!result[2].1);
        assert_eq!(result[2].0.value() as i32, 20);
        assert!(!result[3].1);
        assert_eq!(result[3].0.value() as i32, 30);
    }

    #[pg_test]
    fn test_decompress_truncated_constant() {
        use super::datum_utils::decompress_blob_to_datums_truncated;
        use crate::compression::{CompressionType, bitpacked};

        let data = bitpacked::encode_constant_i32(77);
        let blob = make_blob(CompressionType::Constant, 100, data);
        let result = unsafe {
            decompress_blob_to_datums_truncated(&blob, "integer", pg_sys::INT4OID, -1, 4)
        };
        assert_eq!(result.len(), 5); // max_row=4 → 5 elements
        for (d, is_null) in &result {
            assert!(!is_null);
            assert_eq!(d.value() as i32, 77);
        }
    }

    #[pg_test]
    fn test_decompress_truncated_for_bitpacked() {
        use super::datum_utils::decompress_blob_to_datums_truncated;
        use crate::compression::{CompressionType, bitpacked};

        let values: Vec<i64> = (0..20).collect();
        let data = bitpacked::encode_for_i64(&values);
        let blob = make_blob(CompressionType::ForBitpacked, 20, data);
        let result =
            unsafe { decompress_blob_to_datums_truncated(&blob, "bigint", pg_sys::INT8OID, -1, 2) };
        assert_eq!(result.len(), 3);
        for (i, (d, is_null)) in result.iter().enumerate() {
            assert!(!is_null);
            assert_eq!(d.value() as i64, i as i64);
        }
    }

    #[pg_test]
    fn test_decompress_truncated_boolean() {
        use super::datum_utils::decompress_blob_to_datums_truncated;
        use crate::compression::{CompressionType, boolean};

        let values: Vec<bool> = vec![
            true, false, true, false, true, false, true, false, true, false,
        ];
        let data = boolean::encode(&values);
        let blob = make_blob(CompressionType::BooleanBitmap, 10, data);
        let result = unsafe {
            decompress_blob_to_datums_truncated(&blob, "boolean", pg_sys::BOOLOID, -1, 2)
        };
        assert_eq!(result.len(), 3);
        assert!(result[0].0.value() != 0);
        assert!(result[1].0.value() == 0);
        assert!(result[2].0.value() != 0);
    }

    #[pg_test]
    fn test_decompress_lz4_text() {
        use super::datum_utils::decompress_blob_to_datums;
        use crate::compression::{CompressionType, lz4};

        let values: Vec<&str> = vec!["hello", "world", "test"];
        let data = lz4::encode(&values);
        let blob = make_blob(CompressionType::Lz4, 3, data);
        let result = unsafe { decompress_blob_to_datums(&blob, "text", pg_sys::TEXTOID, -1) };
        assert_eq!(result.len(), 3);
        for (d, is_null) in &result {
            assert!(!is_null);
            assert!(d.value() != 0, "datum should not be null pointer");
        }
    }

    #[pg_test]
    fn test_decompress_lz4_blocked_text() {
        use super::datum_utils::decompress_blob_to_datums;
        use crate::compression::{CompressionType, lz4};

        let values: Vec<&str> = vec!["alpha", "beta", "gamma", "delta"];
        let data = lz4::encode_blocked(&values, 2); // block_size=2
        let blob = make_blob(CompressionType::Lz4Blocked, 4, data);
        let result = unsafe { decompress_blob_to_datums(&blob, "text", pg_sys::TEXTOID, -1) };
        assert_eq!(result.len(), 4);
        for (d, is_null) in &result {
            assert!(!is_null);
            assert!(d.value() != 0);
        }
    }

    #[pg_test]
    fn test_decompress_dictionary_lz4_text() {
        use super::datum_utils::decompress_blob_to_datums;
        use crate::compression::{CompressionType, dictionary};

        let values: Vec<&str> = vec!["cat", "dog", "cat", "dog", "cat"];
        let data = dictionary::encode_lz4(&values);
        let blob = make_blob(CompressionType::DictionaryLz4, 5, data);
        let result = unsafe { decompress_blob_to_datums(&blob, "text", pg_sys::TEXTOID, -1) };
        assert_eq!(result.len(), 5);
        for (d, is_null) in &result {
            assert!(!is_null);
            assert!(d.value() != 0);
        }
    }

    #[pg_test]
    fn test_decompress_empty_blob() {
        use super::datum_utils::decompress_blob_to_datums;

        let result = unsafe { decompress_blob_to_datums(&[], "integer", pg_sys::INT4OID, -1) };
        assert!(result.is_empty());
    }

    #[pg_test]
    fn test_decompress_truncated_empty_blob() {
        use super::datum_utils::decompress_blob_to_datums_truncated;

        let result =
            unsafe { decompress_blob_to_datums_truncated(&[], "integer", pg_sys::INT4OID, -1, 5) };
        assert!(result.is_empty());
    }

    #[test]
    fn test_is_null_at() {
        use super::datum_utils::is_null_at;

        // Bit 0 set, bit 1 clear, bit 2 set
        let bitmap = vec![0b00000101u8];
        assert!(is_null_at(&bitmap, 0));
        assert!(!is_null_at(&bitmap, 1));
        assert!(is_null_at(&bitmap, 2));
        assert!(!is_null_at(&bitmap, 3));

        // Across byte boundary (bit 8 = first bit of second byte)
        let bitmap = vec![0u8, 0b00000001u8];
        assert!(!is_null_at(&bitmap, 7));
        assert!(is_null_at(&bitmap, 8));
        assert!(!is_null_at(&bitmap, 9));
    }

    #[test]
    fn test_merge_with_placeholder() {
        use super::datum_utils::merge_with_placeholder;

        // All pass: every position takes the next matched datum
        let matched = vec![pg_sys::Datum::from(1usize), pg_sys::Datum::from(2usize)];
        let sel = vec![true, true];
        let out = merge_with_placeholder(&matched, &sel);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].value(), 1);
        assert_eq!(out[1].value(), 2);

        // Mixed: placeholders at non-passing positions
        let matched = vec![pg_sys::Datum::from(42usize)];
        let sel = vec![false, true, false];
        let out = merge_with_placeholder(&matched, &sel);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].value(), 0);
        assert_eq!(out[1].value(), 42);
        assert_eq!(out[2].value(), 0);

        // Empty
        let out = merge_with_placeholder(&[], &[]);
        assert!(out.is_empty());
    }

    #[pg_test]
    fn test_decompress_text_eq_filter_dict() {
        use super::datum_utils::decompress_text_blob_with_eq_filter;
        use crate::compression::{CompressionType, dictionary};

        // 6 rows, dictionary-compressed; only rows 0, 3 == "cat".
        let values: Vec<&str> = vec!["cat", "dog", "fish", "cat", "dog", "bird"];
        let data = dictionary::encode(&values);
        let blob = make_blob(CompressionType::Dictionary, 6, data);
        let (datums, sel) = unsafe {
            decompress_text_blob_with_eq_filter(
                &blob,
                pg_sys::TEXTOID,
                -1,
                "cat",
                false, // eq, not ne
                None,
            )
        };
        assert_eq!(datums.len(), 6);
        assert_eq!(sel, vec![true, false, false, true, false, false]);
        // Matched rows hold real varlenas; placeholders are Datum(0).
        assert!(datums[0].0.value() != 0);
        assert!(datums[3].0.value() != 0);
        assert_eq!(datums[1].0.value(), 0);
        assert_eq!(datums[2].0.value(), 0);
    }

    #[pg_test]
    fn test_decompress_text_eq_filter_ne_dict() {
        use super::datum_utils::decompress_text_blob_with_eq_filter;
        use crate::compression::{CompressionType, dictionary};

        // != "dog" should flag rows 0, 2, 3, 5.
        let values: Vec<&str> = vec!["cat", "dog", "fish", "cat", "dog", "bird"];
        let data = dictionary::encode(&values);
        let blob = make_blob(CompressionType::Dictionary, 6, data);
        let (_datums, sel) = unsafe {
            decompress_text_blob_with_eq_filter(
                &blob,
                pg_sys::TEXTOID,
                -1,
                "dog",
                true, // ne
                None,
            )
        };
        assert_eq!(sel, vec![true, false, true, true, false, true]);
    }

    #[pg_test]
    fn test_decompress_text_in_filter_dict() {
        use super::datum_utils::decompress_text_blob_with_in_filter;
        use crate::compression::{CompressionType, dictionary};

        let values: Vec<&str> = vec!["a", "b", "c", "a", "d", "b"];
        let data = dictionary::encode(&values);
        let blob = make_blob(CompressionType::Dictionary, 6, data);
        let const_strs = vec!["a".to_string(), "c".to_string()];
        let (_datums, sel) = unsafe {
            decompress_text_blob_with_in_filter(
                &blob,
                pg_sys::TEXTOID,
                -1,
                &const_strs,
                false, // IN, not NOT IN
                None,
            )
        };
        assert_eq!(sel, vec![true, false, true, true, false, false]);
    }

    #[pg_test]
    fn test_decompress_text_in_filter_lz4() {
        use super::datum_utils::decompress_text_blob_with_in_filter;
        use crate::compression::{CompressionType, lz4};

        let values: Vec<&str> = vec!["alpha", "beta", "gamma", "alpha"];
        let data = lz4::encode(&values);
        let blob = make_blob(CompressionType::Lz4, 4, data);
        let const_strs = vec!["alpha".to_string()];
        let (datums, sel) = unsafe {
            decompress_text_blob_with_in_filter(
                &blob,
                pg_sys::TEXTOID,
                -1,
                &const_strs,
                false,
                None,
            )
        };
        assert_eq!(sel, vec![true, false, false, true]);
        assert_eq!(datums.len(), 4);
        assert!(datums[0].0.value() != 0);
        assert!(datums[3].0.value() != 0);
    }

    #[pg_test]
    fn test_decompress_text_like_contains_lz4() {
        use super::batch_qual::LikeStrategy;
        use super::datum_utils::decompress_text_blob_with_like_filter;
        use crate::compression::{CompressionType, lz4};

        // Contains "oo" → "good" and "fool" match, "bar" doesn't.
        let values: Vec<&str> = vec!["good", "bar", "fool", "ham"];
        let data = lz4::encode(&values);
        let blob = make_blob(CompressionType::Lz4, 4, data);
        let strategy = LikeStrategy::Contains("oo".to_string());
        let (_datums, sel) = unsafe {
            decompress_text_blob_with_like_filter(
                &blob,
                pg_sys::TEXTOID,
                -1,
                &strategy,
                false,
                None,
            )
        };
        assert_eq!(sel, vec![true, false, true, false]);
    }
}
