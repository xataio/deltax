//! Pure-arithmetic implementations of `date_trunc(<unit>, ts)` and
//! `EXTRACT(<unit> FROM ts)`, plus a helper that proves an EXTRACT key
//! is constant across a segment using min/max metadata.
//!
//! Both functions only cover the units the planner accepts in
//! `hook.rs` — sub-day fields where the unix-vs-PG epoch shift drops
//! out of the answer.

use pgrx::pg_sys;

use super::super::segments::ColMinMax;

/// Decode a colstats-encoded i64 to the PG-native i64 representation.
///
/// For timestamps, converts Unix-epoch usec → PG-epoch usec.
/// For dates, converts Unix-epoch usec → PG-epoch days.
/// For plain integers, identity.
pub(super) fn decode_encoded_to_pg_i64(encoded: i64, type_oid: pg_sys::Oid) -> i64 {
    match type_oid {
        pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID => {
            encoded - crate::compress::PG_EPOCH_OFFSET_USEC
        }
        pg_sys::DATEOID => (encoded / 86_400_000_000) - crate::compress::PG_EPOCH_OFFSET_DAYS,
        _ => encoded,
    }
}

/// Convert a date_trunc unit string to microseconds.
/// Only sub-day units are supported (integer arithmetic is exact).
pub(crate) fn date_trunc_unit_to_usecs(unit: &str) -> i64 {
    match unit {
        "microsecond" | "microseconds" | "us" => 1,
        "millisecond" | "milliseconds" | "ms" => 1_000,
        "second" | "seconds" => 1_000_000,
        "minute" | "minutes" => 60_000_000,
        "hour" | "hours" => 3_600_000_000,
        "day" | "days" => 86_400_000_000,
        _ => 1, // fallback — should not happen (validated in hook)
    }
}

/// Extract a time field from PG epoch microseconds using pure arithmetic.
/// Only supports sub-day fields + dow + epoch (validated in hook).
pub(super) fn extract_field_from_usecs(pg_usec: i64, unit: &str) -> i64 {
    match unit {
        "microsecond" | "microseconds" => {
            // PG returns second * 1_000_000 (including whole seconds within the minute)
            let usec_in_day = pg_usec.rem_euclid(86_400_000_000);
            let sec_of_min = (usec_in_day / 1_000_000) % 60;
            let frac_usec = usec_in_day.rem_euclid(1_000_000);
            sec_of_min * 1_000_000 + frac_usec
        }
        "millisecond" | "milliseconds" => {
            // PG returns second * 1000 (including whole seconds within the minute)
            let usec_in_day = pg_usec.rem_euclid(86_400_000_000);
            let sec_of_min = (usec_in_day / 1_000_000) % 60;
            let frac_ms = usec_in_day.rem_euclid(1_000_000) / 1_000;
            sec_of_min * 1_000 + frac_ms
        }
        "second" | "seconds" => (pg_usec.rem_euclid(86_400_000_000) / 1_000_000) % 60,
        "minute" | "minutes" => (pg_usec.rem_euclid(86_400_000_000) / 60_000_000) % 60,
        "hour" | "hours" => pg_usec.rem_euclid(86_400_000_000) / 3_600_000_000,
        "dow" => {
            // Day of week (0=Sunday..6=Saturday)
            // PG epoch 2000-01-01 is a Saturday (dow=6)
            let days = pg_usec.div_euclid(86_400_000_000);
            (days + 6).rem_euclid(7)
        }
        "epoch" => {
            // PG epoch is 2000-01-01, Unix epoch offset = 946684800 seconds
            (pg_usec / 1_000_000) + 946_684_800
        }
        _ => 0, // Should not happen (validated in hook)
    }
}

/// Evaluate a `GroupByExpr::Extract` on a single column-row value. When
/// `divisor == 0` the input is interpreted as PG-usec (existing path); when
/// `divisor > 0` the input is interpreted as bigint unix microseconds via
/// `extract_subday_from_bigint_scaled` below. Centralised so the five
/// per-row extract sites in this file stay one-line and consistent.
#[inline]
pub(crate) fn eval_extract(value: i64, divisor: i64, unit: &str) -> i64 {
    if divisor > 0 {
        extract_subday_from_bigint_scaled(value, divisor, unit)
    } else {
        extract_field_from_usecs(value, unit)
    }
}

/// If segment min/max prove an EXTRACT() group key is constant across the
/// segment, return that key. This lets the hot mixed-aggregate path avoid
/// decompressing a time column just to recompute the same bucket for every row.
pub(super) fn constant_extract_key_for_segment(
    cm: &ColMinMax,
    divisor: i64,
    unit: &str,
) -> Option<i64> {
    if cm.min_null || cm.max_null {
        return None;
    }
    let min_value = if divisor > 0 {
        cm.min_encoded
    } else {
        decode_encoded_to_pg_i64(cm.min_encoded, cm.type_oid)
    };
    let max_value = if divisor > 0 {
        cm.max_encoded
    } else {
        decode_encoded_to_pg_i64(cm.max_encoded, cm.type_oid)
    };
    let min_key = eval_extract(min_value, divisor, unit);
    let max_key = eval_extract(max_value, divisor, unit);
    if min_key != max_key {
        return None;
    }

    let bucket_width = match unit {
        "second" | "seconds" => 1_000_000,
        "minute" | "minutes" => 60_000_000,
        "hour" | "hours" => 3_600_000_000,
        _ => return None,
    };
    let (min_bucket, max_bucket) = if divisor > 0 {
        let width = (bucket_width / 1_000_000_i64).saturating_mul(divisor);
        if width <= 0 {
            return None;
        }
        (min_value.div_euclid(width), max_value.div_euclid(width))
    } else {
        (
            min_value.div_euclid(bucket_width),
            max_value.div_euclid(bucket_width),
        )
    };
    (min_bucket == max_bucket).then_some(min_key)
}

/// Extract a sub-day field from a BIGINT column whose value, when divided
/// by `divisor`, yields seconds since the unix epoch (1970-01-01). Used for
/// the `extract(unit FROM to_timestamp(bigint_col / divisor))` shape — the
/// recognizer in `hook.rs` only emits this for units whose value depends
/// only on `unix_secs % 86400` (microsecond/millisecond/second/minute/hour),
/// so the unix-vs-PG epoch shift (a multiple of 86400) drops out and we
/// don't need to convert to PG-usec first.
///
/// `divisor` must be positive; the recognizer enforces this.
pub(super) fn extract_subday_from_bigint_scaled(value: i64, divisor: i64, unit: &str) -> i64 {
    let unix_secs = value.div_euclid(divisor);
    let secs_in_day = unix_secs.rem_euclid(86_400);
    match unit {
        "microsecond" | "microseconds" => {
            // Whole-second arithmetic only: `to_timestamp(bigint / divisor)`
            // already truncated below the second, so any sub-second component
            // of the original bigint is lost. The recognizer accepts the unit
            // anyway because the answer remains exact for divisors that are
            // multiples of 1_000_000 (the only shape we've seen in practice).
            (secs_in_day % 60) * 1_000_000
        }
        "millisecond" | "milliseconds" => (secs_in_day % 60) * 1_000,
        "second" | "seconds" => secs_in_day % 60,
        "minute" | "minutes" => (secs_in_day / 60) % 60,
        "hour" | "hours" => secs_in_day / 3_600,
        _ => 0, // recognizer rejects other units in the divisor>0 path
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use super::*;

    // -------------------------------------------------------------------
    // date_trunc_unit_to_usecs tests
    // -------------------------------------------------------------------

    #[test]
    fn test_date_trunc_microsecond() {
        assert_eq!(date_trunc_unit_to_usecs("microsecond"), 1);
        assert_eq!(date_trunc_unit_to_usecs("microseconds"), 1);
        assert_eq!(date_trunc_unit_to_usecs("us"), 1);
    }

    #[test]
    fn test_date_trunc_millisecond() {
        assert_eq!(date_trunc_unit_to_usecs("millisecond"), 1_000);
        assert_eq!(date_trunc_unit_to_usecs("milliseconds"), 1_000);
        assert_eq!(date_trunc_unit_to_usecs("ms"), 1_000);
    }

    #[test]
    fn test_date_trunc_second() {
        assert_eq!(date_trunc_unit_to_usecs("second"), 1_000_000);
        assert_eq!(date_trunc_unit_to_usecs("seconds"), 1_000_000);
    }

    #[test]
    fn test_date_trunc_minute() {
        assert_eq!(date_trunc_unit_to_usecs("minute"), 60_000_000);
        assert_eq!(date_trunc_unit_to_usecs("minutes"), 60_000_000);
    }

    #[test]
    fn test_date_trunc_hour() {
        assert_eq!(date_trunc_unit_to_usecs("hour"), 3_600_000_000);
        assert_eq!(date_trunc_unit_to_usecs("hours"), 3_600_000_000);
    }

    #[test]
    fn test_date_trunc_day() {
        assert_eq!(date_trunc_unit_to_usecs("day"), 86_400_000_000);
        assert_eq!(date_trunc_unit_to_usecs("days"), 86_400_000_000);
    }

    #[test]
    fn test_date_trunc_unknown_fallback() {
        assert_eq!(date_trunc_unit_to_usecs("week"), 1);
        assert_eq!(date_trunc_unit_to_usecs(""), 1);
    }

    // -------------------------------------------------------------------
    // extract_field_from_usecs tests
    // -------------------------------------------------------------------

    // Helper: PG epoch usecs for 2000-01-01 12:34:56.789012
    // 12h=43200s, 34m=2040s, 56s → total 45296s → 45296_789012 usec
    const SAMPLE_USEC: i64 = 45_296_789_012;

    #[test]
    fn test_extract_microsecond() {
        // PG EXTRACT(microsecond FROM ...) returns seconds_within_minute * 1_000_000 + frac_usec
        // 56 seconds + 789012 usec = 56_789_012
        assert_eq!(
            extract_field_from_usecs(SAMPLE_USEC, "microsecond"),
            56_789_012
        );
        assert_eq!(
            extract_field_from_usecs(SAMPLE_USEC, "microseconds"),
            56_789_012
        );
    }

    #[test]
    fn test_extract_millisecond() {
        // PG EXTRACT(millisecond FROM ...) returns seconds_within_minute * 1_000 + frac_ms
        // 56 seconds + 789 ms = 56_789
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "millisecond"), 56_789);
        assert_eq!(
            extract_field_from_usecs(SAMPLE_USEC, "milliseconds"),
            56_789
        );
    }

    #[test]
    fn test_extract_second() {
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "second"), 56);
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "seconds"), 56);
    }

    #[test]
    fn test_extract_minute() {
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "minute"), 34);
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "minutes"), 34);
    }

    #[test]
    fn test_extract_hour() {
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "hour"), 12);
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "hours"), 12);
    }

    #[test]
    fn test_extract_dow() {
        // PG epoch 2000-01-01 is a Saturday (dow=6), day 0
        assert_eq!(extract_field_from_usecs(0, "dow"), 6); // Saturday
        // Day 1 = Sunday=0
        assert_eq!(extract_field_from_usecs(86_400_000_000, "dow"), 0);
        // Day 2 = Monday=1
        assert_eq!(extract_field_from_usecs(2 * 86_400_000_000, "dow"), 1);
    }

    #[test]
    fn test_extract_epoch() {
        // PG epoch offset to Unix = 946684800 seconds
        assert_eq!(extract_field_from_usecs(0, "epoch"), 946_684_800);
        // 1 second after PG epoch
        assert_eq!(extract_field_from_usecs(1_000_000, "epoch"), 946_684_801);
    }

    #[test]
    fn test_extract_negative_usec() {
        // Before PG epoch (negative usec): 1999-12-31 23:59:59.000000
        // -1 second = -1_000_000 usec
        let usec = -1_000_000i64;
        assert_eq!(extract_field_from_usecs(usec, "second"), 59);
        assert_eq!(extract_field_from_usecs(usec, "minute"), 59);
        assert_eq!(extract_field_from_usecs(usec, "hour"), 23);
        // Day before PG epoch (1999-12-31) is Friday=5
        assert_eq!(extract_field_from_usecs(usec, "dow"), 5);
    }

    #[test]
    fn test_extract_unknown_fallback() {
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "year"), 0);
    }

    // -------------------------------------------------------------------
    // extract_subday_from_bigint_scaled tests
    // -------------------------------------------------------------------

    #[test]
    fn test_extract_subday_from_bigint_scaled_hour() {
        // 2024-05-09 12:34:56 UTC = unix epoch seconds 1715258096
        // hour-of-day at that point is 12.
        let unix_us: i64 = 1_715_258_096_000_000;
        assert_eq!(
            extract_subday_from_bigint_scaled(unix_us, 1_000_000, "hour"),
            12,
        );
        // minute = 34, second = 56
        assert_eq!(
            extract_subday_from_bigint_scaled(unix_us, 1_000_000, "minute"),
            34,
        );
        assert_eq!(
            extract_subday_from_bigint_scaled(unix_us, 1_000_000, "second"),
            56,
        );
        // Pre-unix-epoch (negative) — div_euclid handles the sign correctly,
        // so hour-of-day is still positive within [0, 24).
        let pre_epoch: i64 = -1; // -1us before 1970 → 23:59:59 prev day
        assert_eq!(
            extract_subday_from_bigint_scaled(pre_epoch, 1_000_000, "hour"),
            23,
        );
    }

    // -------------------------------------------------------------------
    // constant_extract_key_for_segment tests
    // -------------------------------------------------------------------

    #[test]
    fn test_constant_extract_key_for_segment_same_hour() {
        let cm = ColMinMax {
            min_encoded: 1_715_258_096_000_000,
            max_encoded: 1_715_259_000_000_000,
            min_null: false,
            max_null: false,
            type_oid: pg_sys::INT8OID,
        };
        assert_eq!(
            constant_extract_key_for_segment(&cm, 1_000_000, "hour"),
            Some(12),
        );
    }

    #[test]
    fn test_constant_extract_key_for_segment_crossing_hour() {
        let cm = ColMinMax {
            min_encoded: 1_715_259_599_000_000,
            max_encoded: 1_715_259_601_000_000,
            min_null: false,
            max_null: false,
            type_oid: pg_sys::INT8OID,
        };
        assert_eq!(
            constant_extract_key_for_segment(&cm, 1_000_000, "hour"),
            None,
        );
    }

    #[test]
    fn test_constant_extract_key_for_segment_same_minute() {
        let cm = ColMinMax {
            min_encoded: 10 * 60_000_000 + 20_000_000,
            max_encoded: 10 * 60_000_000 + 40_000_000,
            min_null: false,
            max_null: false,
            type_oid: pg_sys::INT8OID,
        };
        assert_eq!(constant_extract_key_for_segment(&cm, 0, "minute"), Some(10),);
    }

    #[test]
    fn test_constant_extract_key_for_segment_same_minute_value_across_hour() {
        let cm = ColMinMax {
            min_encoded: 27 * 60_000_000,
            max_encoded: 87 * 60_000_000,
            min_null: false,
            max_null: false,
            type_oid: pg_sys::INT8OID,
        };
        assert_eq!(constant_extract_key_for_segment(&cm, 0, "minute"), None,);
    }
}
