use pgrx::prelude::*;

/// Truncate a timestamp to the nearest interval boundary.
///
/// Similar to `date_trunc` but works with arbitrary intervals.
///
/// # Examples
///
/// ```sql
/// SELECT time_bucket('5 minutes'::interval, '2025-01-15 14:23:42'::timestamptz);
/// -- Returns: 2025-01-15 14:20:00+00
/// ```
#[pg_extern(immutable, parallel_safe)]
fn time_bucket(
    bucket_width: pgrx::datum::Interval,
    ts: TimestampWithTimeZone,
) -> TimestampWithTimeZone {
    let width_usec = interval_to_usec(&bucket_width);
    if width_usec <= 0 {
        pgrx::error!("pg_deltax: time_bucket width must be positive");
    }

    // PostgreSQL stores timestamps as microseconds since 2000-01-01 00:00:00 UTC
    let ts_usec = ts.into_inner();

    let bucketed = floor_div(ts_usec, width_usec) * width_usec;

    TimestampWithTimeZone::try_from(bucketed).expect("time_bucket result out of range")
}

/// Time bucket with an offset.
///
/// ```sql
/// SELECT time_bucket('1 day'::interval, now(), '6 hours'::interval);
/// -- Buckets start at 06:00 instead of 00:00
/// ```
#[pg_extern(immutable, parallel_safe, name = "time_bucket")]
fn time_bucket_offset(
    bucket_width: pgrx::datum::Interval,
    ts: TimestampWithTimeZone,
    origin: pgrx::datum::Interval,
) -> TimestampWithTimeZone {
    let width_usec = interval_to_usec(&bucket_width);
    if width_usec <= 0 {
        pgrx::error!("pg_deltax: time_bucket width must be positive");
    }

    let offset_usec = interval_to_usec(&origin);
    let ts_usec = ts.into_inner();
    let shifted = ts_usec - offset_usec;
    let bucketed = floor_div(shifted, width_usec) * width_usec + offset_usec;

    TimestampWithTimeZone::try_from(bucketed).expect("time_bucket result out of range")
}

/// Convert a pgrx Interval to microseconds.
/// Errors if the interval contains months (not a fixed duration).
fn interval_to_usec(interval: &pgrx::datum::Interval) -> i64 {
    let months: i32 = interval
        .extract_part(DateTimeParts::Month)
        .and_then(|v| v.try_into().ok())
        .unwrap_or(0);
    if months != 0 {
        pgrx::error!("pg_deltax: time_bucket does not support monthly intervals");
    }

    let days: i64 = interval
        .extract_part(DateTimeParts::Day)
        .and_then(|v| v.try_into().ok())
        .unwrap_or(0);
    let hours: i64 = interval
        .extract_part(DateTimeParts::Hour)
        .and_then(|v| v.try_into().ok())
        .unwrap_or(0);
    let minutes: i64 = interval
        .extract_part(DateTimeParts::Minute)
        .and_then(|v| v.try_into().ok())
        .unwrap_or(0);
    let secs: i64 = interval
        .extract_part(DateTimeParts::Second)
        .and_then(|v| v.try_into().ok())
        .unwrap_or(0);

    days * 86_400_000_000 + hours * 3_600_000_000 + minutes * 60_000_000 + secs * 1_000_000
}

/// Floor division that rounds toward negative infinity (not toward zero).
fn floor_div(a: i64, b: i64) -> i64 {
    let d = a / b;
    let r = a % b;
    if (r != 0) && ((r ^ b) < 0) { d - 1 } else { d }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_time_bucket_5min() {
        let result = Spi::get_one::<String>(
            "SELECT time_bucket('5 minutes'::interval, '2025-01-15 14:23:42+00'::timestamptz)::text",
        )
        .expect("query failed");
        assert!(
            result.as_deref().unwrap().contains("14:20:00"),
            "expected 14:20:00 bucket, got {:?}",
            result
        );
    }

    #[pg_test]
    fn test_time_bucket_1hour() {
        let result = Spi::get_one::<String>(
            "SELECT time_bucket('1 hour'::interval, '2025-01-15 14:23:42+00'::timestamptz)::text",
        )
        .expect("query failed");
        assert!(
            result.as_deref().unwrap().contains("14:00:00"),
            "expected 14:00:00 bucket, got {:?}",
            result
        );
    }

    #[pg_test]
    fn test_time_bucket_1day() {
        let result = Spi::get_one::<String>(
            "SELECT time_bucket('1 day'::interval, '2025-01-15 14:23:42+00'::timestamptz)::text",
        )
        .expect("query failed");
        let val = result.as_deref().unwrap();
        assert!(
            val.contains("2025-01-15") && val.contains("00:00:00"),
            "expected day boundary, got {:?}",
            val
        );
    }

    #[pg_test]
    fn test_time_bucket_with_offset() {
        let result = Spi::get_one::<String>(
            "SELECT time_bucket('1 day'::interval, '2025-01-15 14:23:42+00'::timestamptz, '6 hours'::interval)::text",
        )
        .expect("query failed");
        assert!(
            result.as_deref().unwrap().contains("06:00:00"),
            "expected 06:00 offset bucket, got {:?}",
            result
        );
    }
}
