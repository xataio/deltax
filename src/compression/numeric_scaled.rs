//! Scaled-i64 representation of `numeric` text renderings.
//!
//! `numeric_out` renders plain decimal notation (never exponents), with
//! exactly `dscale` fractional digits. A value like `-123.4500` maps to
//! `(mantissa: -1234500, dscale: 4)` and back losslessly. The compressed
//! form (`CompressionType::NumericScaled`) stores one dscale for the whole
//! segment plus the mantissas through the integer codecs — applicable only
//! when every non-null value shares that dscale and fits an i64, which is
//! the norm for `numeric(p,s)` columns. `NaN`, `Infinity`, mixed-dscale and
//! out-of-range values make the segment fall back to the text codecs.

/// Max dscale we encode. PG allows up to 16383, but a u8 header field and
/// i64 mantissas make anything beyond ~18 pointless (10^19 > i64::MAX).
pub const MAX_DSCALE: u8 = 18;

/// Parse a `numeric_out` rendering into `(mantissa, dscale)`.
/// Returns `None` for special values (NaN/Infinity), exponent forms,
/// dscale > MAX_DSCALE, or mantissas that don't fit i64.
pub fn parse_numeric_scaled(s: &str) -> Option<(i64, u8)> {
    let b = s.as_bytes();
    if b.is_empty() {
        return None;
    }
    let (neg, rest) = match b[0] {
        b'-' => (true, &b[1..]),
        b'+' => (false, &b[1..]),
        _ => (false, b),
    };
    if rest.is_empty() {
        return None;
    }
    let mut mantissa: i128 = 0;
    let mut dscale: u32 = 0;
    let mut seen_dot = false;
    let mut seen_digit = false;
    for &c in rest {
        match c {
            b'0'..=b'9' => {
                seen_digit = true;
                mantissa = mantissa.checked_mul(10)?.checked_add((c - b'0') as i128)?;
                if mantissa > i64::MAX as i128 {
                    return None;
                }
                if seen_dot {
                    dscale += 1;
                    if dscale > MAX_DSCALE as u32 {
                        return None;
                    }
                }
            }
            b'.' if !seen_dot => seen_dot = true,
            _ => return None, // NaN, Infinity, exponent, whitespace, ...
        }
    }
    if !seen_digit {
        return None;
    }
    let m = if neg {
        -(mantissa as i64)
    } else {
        mantissa as i64
    };
    Some((m, dscale as u8))
}

/// Exact inverse of [`parse_numeric_scaled`]: render `(mantissa, dscale)`
/// back to the `numeric_out` text form.
pub fn format_numeric_scaled(mantissa: i64, dscale: u8) -> String {
    if dscale == 0 {
        return mantissa.to_string();
    }
    let neg = mantissa < 0;
    let abs = mantissa.unsigned_abs().to_string();
    let s = dscale as usize;
    let (int_part, frac_part) = if abs.len() > s {
        (
            abs[..abs.len() - s].to_string(),
            abs[abs.len() - s..].to_string(),
        )
    } else {
        ("0".to_string(), format!("{:0>width$}", abs, width = s))
    };
    if neg {
        format!("-{}.{}", int_part, frac_part)
    } else {
        format!("{}.{}", int_part, frac_part)
    }
}

/// Try to reduce a segment of `numeric_out` renderings to a uniform-dscale
/// mantissa vector. Returns `None` (→ text fallback) unless every non-null
/// value parses and shares one dscale.
pub fn to_uniform_scaled(values: &[Option<String>]) -> Option<(Vec<Option<i64>>, u8)> {
    let mut dscale: Option<u8> = None;
    let mut out: Vec<Option<i64>> = Vec::with_capacity(values.len());
    for v in values {
        match v {
            None => out.push(None),
            Some(s) => {
                let (m, d) = parse_numeric_scaled(s)?;
                match dscale {
                    None => dscale = Some(d),
                    Some(prev) if prev != d => return None,
                    _ => {}
                }
                out.push(Some(m));
            }
        }
    }
    // All-null segment: encode with dscale 0 (values never rendered).
    Some((out, dscale.unwrap_or(0)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_format_roundtrip() {
        for s in [
            "0",
            "1",
            "-1",
            "123",
            "-123",
            "0.5",
            "-0.5",
            "1.20",
            "-1.20",
            "123.4500",
            "0.000001",
            "-0.000001",
            "9223372036854775807",
            "92233720368547758.07",
            "0.00",
        ] {
            let (m, d) = parse_numeric_scaled(s).unwrap_or_else(|| panic!("failed to parse {}", s));
            assert_eq!(format_numeric_scaled(m, d), s, "roundtrip of {}", s);
        }
    }

    #[test]
    fn parse_rejects_specials_and_overflow() {
        for s in [
            "NaN",
            "Infinity",
            "-Infinity",
            "1e5",
            "1.5e-3",
            "",
            "-",
            ".",
            "9223372036854775808",   // > i64::MAX
            "1.0000000000000000001", // dscale 19 > MAX_DSCALE
            "12,5",
            " 1",
        ] {
            assert!(parse_numeric_scaled(s).is_none(), "should reject {:?}", s);
        }
    }

    #[test]
    fn negative_mantissa_min() {
        // -i64::MIN overflows i64; the positive-mantissa cap rejects it.
        assert!(parse_numeric_scaled("-9223372036854775808").is_none());
        let (m, d) = parse_numeric_scaled("-9223372036854775807").unwrap();
        assert_eq!((m, d), (-9223372036854775807, 0));
    }

    #[test]
    fn uniform_scaled_gate() {
        let ok = vec![Some("1.20".to_string()), None, Some("-3.05".to_string())];
        let (v, d) = to_uniform_scaled(&ok).unwrap();
        assert_eq!(d, 2);
        assert_eq!(v, vec![Some(120), None, Some(-305)]);

        let mixed = vec![Some("1.20".to_string()), Some("1.2".to_string())];
        assert!(to_uniform_scaled(&mixed).is_none());

        let nan = vec![Some("NaN".to_string())];
        assert!(to_uniform_scaled(&nan).is_none());

        let all_null: Vec<Option<String>> = vec![None, None];
        let (v, d) = to_uniform_scaled(&all_null).unwrap();
        assert_eq!(d, 0);
        assert_eq!(v, vec![None, None]);
    }
}
