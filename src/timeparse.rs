//! Pure-Rust timestamp parsing and formatting.
//!
//! Replaces SPI round-trips to PostgreSQL for converting between text
//! representations and Unix-epoch microseconds.

/// Parse a PostgreSQL text-format timestamp/date to Unix epoch microseconds.
///
/// Supported formats:
/// - `"2013-07-15 10:23:45"`         — timestamp without tz (treated as UTC)
/// - `"2013-07-15 10:23:45.123456"`  — with fractional seconds
/// - `"2013-07-15 10:23:45+00"`      — timestamptz (offset hours)
/// - `"2013-07-15 10:23:45+03:30"`   — timestamptz (offset hours:minutes)
/// - `"2013-07-15"`                  — date only (midnight UTC)
pub fn parse_timestamp_to_usec(s: &str) -> i64 {
    let s = s.trim();

    // Split on space or 'T' to separate date from time+tz (ISO 8601 uses 'T')
    let split_pos = s.find(' ').or_else(|| {
        // Only treat 'T' as separator if it's at position 10 (after YYYY-MM-DD)
        if s.len() > 10 && s.as_bytes()[10] == b'T' {
            Some(10)
        } else {
            None
        }
    });
    let (date_part, time_tz) = match split_pos {
        Some(pos) => (&s[..pos], Some(&s[pos + 1..])),
        None => (s, None),
    };

    // Parse YYYY-MM-DD
    let (year, month, day) = parse_date_part(date_part);
    let days = date_to_epoch_days(year, month, day);

    let (time_usec, tz_offset_usec) = match time_tz {
        Some(t) => parse_time_and_tz(t),
        None => (0i64, 0i64),
    };

    days * 86_400 * 1_000_000 + time_usec - tz_offset_usec
}

/// Format Unix epoch microseconds as `"YYYY-MM-DD HH:MM:SS[.ffffff]+00"`.
pub fn usec_to_timestamp_string(usec: i64) -> String {
    let (negative, abs_usec) = if usec < 0 {
        (true, (-usec) as u64)
    } else {
        (false, usec as u64)
    };

    let total_secs = (abs_usec / 1_000_000) as i64;
    let frac_usec = (abs_usec % 1_000_000) as u32;

    let (total_secs, frac_usec) = if negative {
        if frac_usec > 0 {
            (-(total_secs + 1), 1_000_000 - frac_usec)
        } else {
            (-total_secs, 0)
        }
    } else {
        (total_secs, frac_usec)
    };

    let day_secs = total_secs.rem_euclid(86_400);
    let days = (total_secs - day_secs) / 86_400;

    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    let seconds = day_secs % 60;

    let (year, month, day) = epoch_days_to_date(days);

    if frac_usec > 0 {
        // Trim trailing zeros from fractional part
        let frac_str = format!("{:06}", frac_usec);
        let trimmed = frac_str.trim_end_matches('0');
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{}+00",
            year, month, day, hours, minutes, seconds, trimmed
        )
    } else {
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}+00",
            year, month, day, hours, minutes, seconds
        )
    }
}

/// Parse a PostgreSQL text-format `time` value (`"HH:MM:SS[.ffffff]"`, also
/// accepting `"HH:MM"`) to microseconds since midnight. PG renders `time`
/// in this fixed 24-hour form regardless of DateStyle.
pub fn parse_time_to_usec(s: &str) -> Result<i64, String> {
    let s = s.trim();
    let mut parts = s.splitn(3, ':');
    let hours: i64 = parts
        .next()
        .and_then(|p| p.parse().ok())
        .ok_or_else(|| format!("invalid time: {}", s))?;
    let minutes: i64 = parts
        .next()
        .and_then(|p| p.parse().ok())
        .ok_or_else(|| format!("invalid time: {}", s))?;
    let (seconds, frac_usec): (i64, i64) = match parts.next() {
        None => (0, 0),
        Some(sec_part) => match sec_part.split_once('.') {
            None => (
                sec_part
                    .parse()
                    .map_err(|_| format!("invalid time: {}", s))?,
                0,
            ),
            Some((sec, frac)) => {
                let sec: i64 = sec.parse().map_err(|_| format!("invalid time: {}", s))?;
                // Right-pad the fractional digits to microseconds.
                let mut usec: i64 = 0;
                let mut digits = 0;
                for c in frac.chars() {
                    let d = c
                        .to_digit(10)
                        .ok_or_else(|| format!("invalid time: {}", s))?;
                    if digits < 6 {
                        usec = usec * 10 + d as i64;
                        digits += 1;
                    }
                }
                while digits < 6 {
                    usec *= 10;
                    digits += 1;
                }
                (sec, usec)
            }
        },
    };
    // 24:00:00 is a valid PG time value.
    if !(0..=24).contains(&hours)
        || !(0..60).contains(&minutes)
        || !(0..61).contains(&seconds)
        || (hours == 24 && (minutes != 0 || seconds != 0 || frac_usec != 0))
    {
        return Err(format!("time out of range: {}", s));
    }
    Ok(((hours * 3600 + minutes * 60 + seconds) * 1_000_000) + frac_usec)
}

/// Format microseconds since midnight as `"HH:MM:SS[.ffffff]"` (PG's `time`
/// text rendering, trailing fractional zeros trimmed).
pub fn usec_to_time_string(usec: i64) -> String {
    let total_secs = usec / 1_000_000;
    let frac_usec = usec % 1_000_000;
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    if frac_usec > 0 {
        let frac_str = format!("{:06}", frac_usec);
        let trimmed = frac_str.trim_end_matches('0');
        format!("{:02}:{:02}:{:02}.{}", hours, minutes, seconds, trimmed)
    } else {
        format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
    }
}

/// Format Unix epoch microseconds as `"YYYY-MM-DD"`.
pub fn usec_to_date_string(usec: i64) -> String {
    // For dates we just need the day portion
    let total_secs = if usec >= 0 {
        usec / 1_000_000
    } else {
        // For negative values, floor division
        (usec - 999_999) / 1_000_000
    };
    let days = if total_secs >= 0 {
        total_secs / 86_400
    } else {
        (total_secs - 86_399) / 86_400
    };
    let (year, month, day) = epoch_days_to_date(days);
    format!("{:04}-{:02}-{:02}", year, month, day)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn parse_date_part(s: &str) -> (i32, u32, u32) {
    // YYYY-MM-DD
    let bytes = s.as_bytes();
    debug_assert!(bytes.len() >= 10);

    let year = parse_int_fast(&bytes[..4]) as i32;
    let month = parse_int_fast(&bytes[5..7]);
    let day = parse_int_fast(&bytes[8..10]);
    (year, month, day)
}

/// Parse time portion and optional timezone offset.
/// Returns (time_usec, tz_offset_usec).
fn parse_time_and_tz(s: &str) -> (i64, i64) {
    // Find timezone offset start: look for +/- after the time portion
    // Time is at least HH:MM:SS (8 chars), tz offset starts at +/- after that
    let (time_str, tz_str) = find_tz_split(s);

    let time_usec = parse_time_part(time_str);
    let tz_offset_usec = match tz_str {
        Some(tz) => parse_tz_offset(tz),
        None => 0,
    };

    (time_usec, tz_offset_usec)
}

/// Split time string into (time, optional_tz).
fn find_tz_split(s: &str) -> (&str, Option<&str>) {
    let bytes = s.as_bytes();
    // Skip past HH:MM:SS (8 chars minimum) then look for +/-
    let mut i = 8;
    // Skip fractional seconds (.xxxxxx)
    if i < bytes.len() && bytes[i] == b'.' {
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
    }
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        (&s[..i], Some(&s[i..]))
    } else {
        (s, None)
    }
}

/// Parse `HH:MM:SS[.ffffff]` to microseconds since midnight.
fn parse_time_part(s: &str) -> i64 {
    let bytes = s.as_bytes();
    let hours = parse_int_fast(&bytes[..2]) as i64;
    let minutes = parse_int_fast(&bytes[3..5]) as i64;
    let seconds = parse_int_fast(&bytes[6..8]) as i64;

    let frac_usec = if bytes.len() > 8 && bytes[8] == b'.' {
        parse_fractional_usec(&bytes[9..])
    } else {
        0i64
    };

    hours * 3_600_000_000 + minutes * 60_000_000 + seconds * 1_000_000 + frac_usec
}

/// Parse fractional seconds digits into microseconds.
/// Input is the digits after the decimal point (up to 6 significant).
fn parse_fractional_usec(digits: &[u8]) -> i64 {
    // Pad/truncate to exactly 6 digits to get microseconds
    let mut buf = [b'0'; 6];
    let len = digits.len().min(6);
    buf[..len].copy_from_slice(&digits[..len]);
    let mut result: i64 = 0;
    for &d in &buf {
        result = result * 10 + (d - b'0') as i64;
    }
    result
}

/// Parse timezone offset `[+-]HH` or `[+-]HH:MM` to microseconds.
fn parse_tz_offset(s: &str) -> i64 {
    let bytes = s.as_bytes();
    let sign: i64 = if bytes[0] == b'-' { -1 } else { 1 };
    let rest = &bytes[1..];

    let hours = parse_int_fast(&rest[..2]) as i64;
    let minutes = if rest.len() >= 5 && rest[2] == b':' {
        parse_int_fast(&rest[3..5]) as i64
    } else if rest.len() >= 4 && rest[2] != b':' {
        // +0530 format (no colon)
        parse_int_fast(&rest[2..4]) as i64
    } else {
        0
    };

    sign * (hours * 3_600_000_000 + minutes * 60_000_000)
}

/// Fast integer parse from ASCII digit bytes (no bounds checking beyond debug).
fn parse_int_fast(bytes: &[u8]) -> u32 {
    let mut result: u32 = 0;
    for &b in bytes {
        result = result * 10 + (b - b'0') as u32;
    }
    result
}

/// Convert a Gregorian date to days since Unix epoch (1970-01-01 = day 0).
fn date_to_epoch_days(year: i32, month: u32, day: u32) -> i64 {
    // Algorithm from Howard Hinnant's date library (public domain).
    // Shifts March to month 0 so the leap day falls at end of "year".
    let y = if month <= 2 { year - 1 } else { year } as i64;
    let m = if month <= 2 { month + 9 } else { month - 3 } as i64;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64; // year of era [0, 399]
    let doy = (153 * m as u64 + 2) / 5 + day as u64 - 1; // day of year [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // day of era [0, 146096]
    era * 146097 + doe as i64 - 719468 // days since 1970-01-01
}

/// Convert days since Unix epoch to (year, month, day).
fn epoch_days_to_date(days: i64) -> (i32, u32, u32) {
    // Inverse of date_to_epoch_days — Howard Hinnant's algorithm.
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y } as i32;
    (y, m, d)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    #[cfg(any(test, feature = "pg_test"))]
    use pgrx::prelude::*;

    #[test]
    fn test_date_only() {
        // 2013-07-15 midnight UTC
        let usec = parse_timestamp_to_usec("2013-07-15");
        // 2013-07-15 00:00:00 UTC in epoch seconds: 1373846400
        assert_eq!(usec, 1_373_846_400_000_000);
    }

    #[test]
    fn test_timestamp_no_tz() {
        let usec = parse_timestamp_to_usec("2013-07-15 10:23:45");
        // 1373846400 + 10*3600 + 23*60 + 45 = 1373846400 + 37425 = 1373883825
        assert_eq!(usec, 1_373_883_825_000_000);
    }

    #[test]
    fn test_timestamp_with_frac() {
        let usec = parse_timestamp_to_usec("2013-07-15 10:23:45.123456");
        assert_eq!(usec, 1_373_883_825_123_456);
    }

    #[test]
    fn test_timestamp_with_tz_plus00() {
        let usec = parse_timestamp_to_usec("2013-07-15 10:23:45+00");
        assert_eq!(usec, 1_373_883_825_000_000);
    }

    #[test]
    fn test_timestamp_with_tz_plus03() {
        // +03 means local time is 3 hours ahead of UTC, so UTC = local - 3h
        let usec = parse_timestamp_to_usec("2013-07-15 10:23:45+03");
        assert_eq!(usec, 1_373_883_825_000_000 - 3 * 3_600_000_000);
    }

    #[test]
    fn test_timestamp_with_tz_minus05() {
        let usec = parse_timestamp_to_usec("2013-07-15 10:23:45-05");
        assert_eq!(usec, 1_373_883_825_000_000 + 5 * 3_600_000_000);
    }

    #[test]
    fn test_timestamp_with_tz_plus0530() {
        let usec = parse_timestamp_to_usec("2013-07-15 10:23:45+05:30");
        assert_eq!(
            usec,
            1_373_883_825_000_000 - 5 * 3_600_000_000 - 30 * 60_000_000
        );
    }

    #[test]
    fn test_epoch() {
        let usec = parse_timestamp_to_usec("1970-01-01 00:00:00");
        assert_eq!(usec, 0);
    }

    #[test]
    fn test_leap_year_feb29() {
        let usec = parse_timestamp_to_usec("2000-02-29");
        // 2000-02-29 00:00:00 UTC = 951782400 seconds
        assert_eq!(usec, 951_782_400_000_000);
    }

    #[test]
    fn test_leap_year_2024() {
        let usec = parse_timestamp_to_usec("2024-02-29 12:00:00");
        // 2024-02-29 12:00:00 UTC = 1709208000 seconds
        assert_eq!(usec, 1_709_208_000_000_000);
    }

    #[test]
    fn test_before_epoch() {
        let usec = parse_timestamp_to_usec("1969-12-31 23:59:59");
        assert_eq!(usec, -1_000_000);
    }

    #[test]
    fn test_usec_to_timestamp_roundtrip() {
        let cases = [
            "2013-07-15 10:23:45+00",
            "1970-01-01 00:00:00+00",
            "2024-02-29 12:00:00+00",
        ];
        for ts in &cases {
            let usec = parse_timestamp_to_usec(ts);
            let back = usec_to_timestamp_string(usec);
            assert_eq!(&back, ts, "roundtrip failed for {}", ts);
        }
    }

    #[test]
    fn test_usec_to_timestamp_with_frac() {
        let usec = 1_373_883_825_123_456i64;
        let s = usec_to_timestamp_string(usec);
        assert_eq!(s, "2013-07-15 10:23:45.123456+00");
    }

    #[test]
    fn test_usec_to_timestamp_frac_trailing_zeros() {
        let usec = 1_373_883_825_100_000i64;
        let s = usec_to_timestamp_string(usec);
        assert_eq!(s, "2013-07-15 10:23:45.1+00");
    }

    #[test]
    fn test_usec_to_date_string() {
        let usec = 1_373_883_825_000_000i64;
        let s = usec_to_date_string(usec);
        assert_eq!(s, "2013-07-15");
    }

    #[test]
    fn test_usec_to_date_string_epoch() {
        assert_eq!(usec_to_date_string(0), "1970-01-01");
    }

    #[test]
    fn test_date_roundtrip() {
        let cases = ["2013-07-15", "1970-01-01", "2024-02-29", "2000-01-01"];
        for d in &cases {
            let usec = parse_timestamp_to_usec(d);
            let back = usec_to_date_string(usec);
            assert_eq!(&back, d, "date roundtrip failed for {}", d);
        }
    }

    #[test]
    fn test_before_epoch_formatting() {
        let usec = parse_timestamp_to_usec("1969-12-31 23:59:59");
        let s = usec_to_timestamp_string(usec);
        assert_eq!(s, "1969-12-31 23:59:59+00");
    }

    #[test]
    fn test_fractional_second_variants() {
        // .1 = 100000 usec
        assert_eq!(
            parse_timestamp_to_usec("2020-01-01 00:00:00.1"),
            parse_timestamp_to_usec("2020-01-01 00:00:00") + 100_000
        );
        // .12 = 120000 usec
        assert_eq!(
            parse_timestamp_to_usec("2020-01-01 00:00:00.12"),
            parse_timestamp_to_usec("2020-01-01 00:00:00") + 120_000
        );
        // .123 = 123000 usec
        assert_eq!(
            parse_timestamp_to_usec("2020-01-01 00:00:00.123"),
            parse_timestamp_to_usec("2020-01-01 00:00:00") + 123_000
        );
    }

    /// Validate our pure-Rust parser against PostgreSQL's EXTRACT(EPOCH FROM ...).
    #[pg_test]
    fn test_parse_matches_pg() {
        use pgrx::prelude::*;
        let test_values = vec![
            "2013-07-15 10:23:45",
            "2013-07-15 10:23:45.123456",
            "2000-02-29 00:00:00",
            "2024-12-31 23:59:59.999999",
            "1970-01-01 00:00:00",
            "2020-06-15 12:30:45.5",
            "1999-01-01 00:00:00",
        ];

        for ts in &test_values {
            let our_usec = parse_timestamp_to_usec(ts);
            let pg_usec = Spi::get_one_with_args::<i64>(
                "SELECT (EXTRACT(EPOCH FROM $1::timestamptz) * 1000000)::int8",
                &[(*ts).into()],
            )
            .expect("pg query failed")
            .unwrap();
            assert_eq!(
                our_usec, pg_usec,
                "Mismatch for '{}': ours={}, pg={}",
                ts, our_usec, pg_usec
            );
        }
    }

    /// Validate our formatting against PG's to_char output.
    #[pg_test]
    fn test_format_matches_pg() {
        use pgrx::prelude::*;
        let test_usec_values: Vec<i64> = vec![
            0,
            1_373_883_825_000_000,
            1_373_883_825_123_456,
            951_782_400_000_000,
        ];

        for usec in &test_usec_values {
            let our_str = usec_to_timestamp_string(*usec);
            // Parse our output back through PG and compare epoch
            let pg_usec = Spi::get_one_with_args::<i64>(
                "SELECT (EXTRACT(EPOCH FROM $1::timestamptz) * 1000000)::int8",
                &[our_str.as_str().into()],
            )
            .expect("pg query failed")
            .unwrap();
            assert_eq!(
                *usec, pg_usec,
                "Format roundtrip mismatch for usec={}: formatted='{}', pg_epoch={}",
                usec, our_str, pg_usec
            );
        }
    }
}
