mod batch_qual;
mod datum_utils;
mod segments;
mod count_minmax;
mod agg;
mod decompress;

use pgrx::pg_sys;

use super::SyncStatic;

// Re-exports for explain.rs
pub(crate) use decompress::DecompressState;
pub(crate) use count_minmax::{CountScanState, MinMaxScanState};
pub(crate) use agg::AggScanState;

// Re-exports for hook.rs
pub(crate) use agg::{AggType, AggExpr, GroupByExpr, GroupByColSpec, HavingOp, HavingFilter};

// Re-exports for path.rs (create_*_state callbacks referenced in CustomScanMethods)
pub(crate) use decompress::{create_custom_scan_state, create_deltax_append_state};
pub(crate) use count_minmax::{create_count_scan_state, create_minmax_scan_state};
pub(crate) use agg::create_agg_scan_state;

// Callback imports for static method tables
use decompress::{begin_custom_scan, exec_custom_scan, end_custom_scan, rescan_custom_scan};
use decompress::begin_deltax_append;
use count_minmax::{begin_count_scan, exec_count_scan, end_count_scan, rescan_count_scan};
use count_minmax::{begin_minmax_scan, exec_minmax_scan, end_minmax_scan, rescan_minmax_scan};

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
        EstimateDSMCustomScan: None,
        InitializeDSMCustomScan: None,
        ReInitializeDSMCustomScan: None,
        InitializeWorkerCustomScan: None,
        ShutdownCustomScan: None,
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

    use super::{PG_EPOCH_OFFSET_USEC, PG_EPOCH_OFFSET_DAYS};

    #[pg_test]
    fn test_pg_epoch_offset_usec() {
        // PG_EPOCH_OFFSET_USEC must equal the number of microseconds between
        // the Unix epoch (1970-01-01) and the PostgreSQL epoch (2000-01-01).
        let pg_val: i64 = Spi::get_one(
            "SELECT (EXTRACT(EPOCH FROM '2000-01-01 00:00:00+00'::timestamptz) * 1000000)::bigint"
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
        let pg_val: i32 = Spi::get_one(
            "SELECT ('2000-01-01'::date - '1970-01-01'::date)::int"
        )
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
            ("1970-01-01", -10957),  // -PG_EPOCH_OFFSET_DAYS
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
            0.0, -0.0, 1.0, -1.0, std::f64::consts::PI,
            1e308, -1e308, 1e-307, f64::MIN_POSITIVE,
        ];

        let encoded = gorilla::encode_floats(&test_values);
        let decoded = gorilla::decode_floats(&encoded, test_values.len());

        for (orig, dec) in test_values.iter().zip(decoded.iter()) {
            assert_eq!(
                orig.to_bits(), dec.to_bits(),
                "float bit mismatch: orig={} (0x{:016x}) decoded={} (0x{:016x})",
                orig, orig.to_bits(), dec, dec.to_bits()
            );
        }
    }

    #[test]
    fn test_reinsert_nulls_datum() {
        use pgrx::pg_sys;
        use super::datum_utils::reinsert_nulls_datum;

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
        let datums = vec![
            pg_sys::Datum::from(10usize),
            pg_sys::Datum::from(30usize),
        ];
        let result = reinsert_nulls_datum(&datums, &bitmap, 4);
        assert_eq!(result.len(), 4);
        assert!(result[0].1);   // null
        assert!(!result[1].1);  // 10
        assert!(result[2].1);   // null
        assert!(!result[3].1);  // 30
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
}
