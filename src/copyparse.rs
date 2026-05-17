//! Pure-Rust PG TEXT format parser for COPY FROM.
//!
//! Parses PostgreSQL TEXT format identically to `CopyReadLineText` / `CopyReadAttributesText`
//! in `copyfromparse.c`. No `pgrx` imports — testable with plain `cargo test`.

use crate::compress::{ColumnKind, TypedColumn};
use crate::timeparse;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct CopyTextOptions {
    pub delimiter: u8,
    pub null_string: Vec<u8>,
    pub header: HeaderMode,
}

impl Default for CopyTextOptions {
    fn default() -> Self {
        Self {
            delimiter: b'\t',
            null_string: vec![b'\\', b'N'],
            header: HeaderMode::None,
        }
    }
}

#[derive(Clone)]
pub enum HeaderMode {
    None,
    Skip,
    #[allow(dead_code)]
    Match(Vec<String>),
}

// ---------------------------------------------------------------------------
// Line Reader
// ---------------------------------------------------------------------------

pub struct CopyLineReader {
    pub(crate) eol: Option<Eol>,
    pub line_number: u64,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum Eol {
    Lf,
    Cr,
    CrLf,
}

pub enum LineResult {
    /// Byte offset of start and end of the raw line (no EOL).
    Row(usize, usize),
    /// Saw `\.` end-of-copy marker.
    EndOfCopy,
    /// Need more data — line not complete yet.
    Incomplete,
}

impl CopyLineReader {
    pub fn new() -> Self {
        Self {
            eol: None,
            line_number: 0,
        }
    }

    /// Scan `buf[start..]` for the next complete line.
    /// Returns `LineResult` with byte offsets relative to `buf`.
    pub fn next_line(&mut self, buf: &[u8], start: usize) -> LineResult {
        let data = &buf[start..];
        let len = data.len();
        let mut i = 0;

        // Scan for line terminator using SIMD-accelerated search,
        // skipping escaped characters
        while i < len {
            match memchr::memchr3(b'\n', b'\r', b'\\', &data[i..]) {
                None => return LineResult::Incomplete,
                Some(offset) => {
                    i += offset;
                    match data[i] {
                        b'\\' => {
                            // Skip escaped byte
                            i += 2;
                            continue;
                        }
                        b'\n' => {
                            let eol = Eol::Lf;
                            if let Some(expected) = self.eol {
                                if expected != eol {
                                    panic!(
                                        "pg_deltax: inconsistent line endings at line {} (expected {:?}, got {:?})",
                                        self.line_number + 1,
                                        expected,
                                        eol
                                    );
                                }
                            } else {
                                self.eol = Some(eol);
                            }
                            self.line_number += 1;
                            let line_end = start + i;
                            return self.check_end_of_copy(buf, start, line_end, line_end + 1);
                        }
                        b'\r' => {
                            let eol = if i + 1 < len && data[i + 1] == b'\n' {
                                Eol::CrLf
                            } else {
                                Eol::Cr
                            };
                            if let Some(expected) = self.eol {
                                if expected != eol {
                                    if expected == Eol::CrLf && i + 1 >= len {
                                        return LineResult::Incomplete;
                                    }
                                    panic!(
                                        "pg_deltax: inconsistent line endings at line {} (expected {:?}, got {:?})",
                                        self.line_number + 1,
                                        expected,
                                        eol
                                    );
                                }
                            } else {
                                self.eol = Some(eol);
                            }
                            self.line_number += 1;
                            let line_end = start + i;
                            let next_start = if eol == Eol::CrLf {
                                line_end + 2
                            } else {
                                line_end + 1
                            };
                            return self.check_end_of_copy(buf, start, line_end, next_start);
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }
        LineResult::Incomplete
    }

    /// Check if the line is the `\.` end-of-copy marker.
    fn check_end_of_copy(
        &self,
        buf: &[u8],
        line_start: usize,
        line_end: usize,
        _next_start: usize,
    ) -> LineResult {
        let line = &buf[line_start..line_end];
        if line == b"\\." {
            return LineResult::EndOfCopy;
        }
        LineResult::Row(line_start, line_end)
    }
}

// ---------------------------------------------------------------------------
// Field Splitter
// ---------------------------------------------------------------------------

/// Split a raw line into field byte slices by delimiter.
/// Backslash-escaped delimiters are not treated as field boundaries.
pub fn split_fields(line: &[u8], delimiter: u8) -> Vec<&[u8]> {
    let mut fields = Vec::new();
    split_fields_into(line, delimiter, &mut fields);
    fields
}

/// Split a raw line into field byte slices, reusing the provided Vec to avoid allocation.
/// The Vec is cleared first.
pub fn split_fields_into<'a>(line: &'a [u8], delimiter: u8, fields: &mut Vec<&'a [u8]>) {
    fields.clear();
    let mut field_start = 0;
    let mut i = 0;
    while i < line.len() {
        if line[i] == b'\\' {
            i += 2; // skip escaped byte
            continue;
        }
        if line[i] == delimiter {
            fields.push(&line[field_start..i]);
            field_start = i + 1;
        }
        i += 1;
    }
    fields.push(&line[field_start..line.len()]);
}

/// Split a raw line into field (start, end) offsets relative to the line start.
/// Reuses the provided Vec to avoid per-row allocation.
pub fn split_field_offsets(line: &[u8], delimiter: u8, offsets: &mut Vec<(usize, usize)>) {
    offsets.clear();
    let mut field_start = 0;
    let mut i = 0;
    while i < line.len() {
        if line[i] == b'\\' {
            i += 2;
            continue;
        }
        if line[i] == delimiter {
            offsets.push((field_start, i));
            field_start = i + 1;
        }
        i += 1;
    }
    offsets.push((field_start, line.len()));
}

// ---------------------------------------------------------------------------
// NULL Detection + Unescaping
// ---------------------------------------------------------------------------

/// Returns `None` if `raw` matches `null_string` (compared pre-unescape).
/// Returns `Some(unescaped)` otherwise.
#[cfg(test)]
pub fn unescape_field(raw: &[u8], null_string: &[u8]) -> Option<String> {
    if raw == null_string {
        return None;
    }

    // Fast path: no backslash → raw is the value (common for numeric data)
    if memchr::memchr(b'\\', raw).is_none() {
        return Some(
            std::str::from_utf8(raw)
                .unwrap_or_else(|_| panic!("pg_deltax: invalid UTF-8 in COPY field"))
                .to_string(),
        );
    }

    Some(unescape_bytes(raw))
}

/// Process PG TEXT escape sequences in raw field bytes, returning a String.
fn unescape_bytes(raw: &[u8]) -> String {
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'\\' && i + 1 < raw.len() {
            i += 1;
            match raw[i] {
                b'b' => {
                    out.push(0x08);
                    i += 1;
                }
                b'f' => {
                    out.push(0x0C);
                    i += 1;
                }
                b'n' => {
                    out.push(0x0A);
                    i += 1;
                }
                b'r' => {
                    out.push(0x0D);
                    i += 1;
                }
                b't' => {
                    out.push(0x09);
                    i += 1;
                }
                b'v' => {
                    out.push(0x0B);
                    i += 1;
                }
                b'x' => {
                    // Hex escape: \xHH (1-2 hex digits)
                    i += 1;
                    if i < raw.len() && is_hex_digit(raw[i]) {
                        let mut val = hex_val(raw[i]);
                        i += 1;
                        if i < raw.len() && is_hex_digit(raw[i]) {
                            val = val * 16 + hex_val(raw[i]);
                            i += 1;
                        }
                        out.push(val);
                    } else {
                        out.push(b'x');
                    }
                }
                b'0'..=b'7' => {
                    // Octal escape: 1-3 digits
                    let mut val = raw[i] - b'0';
                    i += 1;
                    if i < raw.len() && raw[i] >= b'0' && raw[i] <= b'7' {
                        val = val * 8 + (raw[i] - b'0');
                        i += 1;
                        if i < raw.len() && raw[i] >= b'0' && raw[i] <= b'7' {
                            val = val * 8 + (raw[i] - b'0');
                            i += 1;
                        }
                    }
                    out.push(val);
                }
                other => {
                    out.push(other);
                    i += 1;
                }
            }
        } else {
            out.push(raw[i]);
            i += 1;
        }
    }

    String::from_utf8(out)
        .unwrap_or_else(|_| panic!("pg_deltax: invalid UTF-8 after unescaping COPY field"))
}

fn is_hex_digit(b: u8) -> bool {
    b.is_ascii_hexdigit()
}

fn hex_val(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// Type Conversion
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct ParseError {
    pub message: String,
    pub column: usize,
    pub line: u64,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "line {}, column {}: {}",
            self.line, self.column, self.message
        )
    }
}

/// Parse a raw field (pre-unescape bytes) directly into a typed column.
///
/// For non-text types, if no backslash is present (common case for numeric data),
/// parses directly from the raw byte slice as `&str` — zero allocation.
/// Only Text/escaped fields go through the full unescape → String path.
pub fn parse_raw_field_and_append(
    raw: &[u8],
    null_string: &[u8],
    kind: ColumnKind,
    typed_col: &mut TypedColumn,
    col_idx: usize,
    line_number: u64,
) -> Result<(), ParseError> {
    // NULL check on raw bytes (pre-unescape)
    if raw == null_string {
        return push_null(typed_col);
    }

    match kind {
        ColumnKind::Text => {
            // Text needs full unescape → String
            let unescaped = unescape_field_always(raw);
            if let TypedColumn::Text(vec) = typed_col {
                vec.push(Some(unescaped));
            }
            Ok(())
        }
        ColumnKind::Jsonb => {
            // Direct backfill: unescape the field to canonical JSON text.
            // Main-thread callers pass a `Bytes` column and we convert to
            // jsonb's binary varlena here (so the scan path can skip jsonb_in
            // per row). Worker-thread callers pass a `Text` column to defer
            // the conversion — `jsonb_in` touches PG memory contexts and
            // function-manager globals, which are not thread-safe.
            let unescaped = unescape_field_always(raw);
            match typed_col {
                TypedColumn::Bytes(vec) => {
                    let bytes = unsafe { crate::compress::jsonb_text_to_binary(&unescaped) };
                    vec.push(Some(bytes));
                }
                TypedColumn::Text(vec) => {
                    vec.push(Some(unescaped));
                }
                _ => {}
            }
            Ok(())
        }
        _ => {
            // For non-text: if no backslash, parse directly from raw bytes as &str
            // This is the fast path — no String allocation
            if memchr::memchr(b'\\', raw).is_none() {
                let s = std::str::from_utf8(raw).unwrap_or_else(|_| {
                    panic!(
                        "pg_deltax: invalid UTF-8 in COPY field at line {}",
                        line_number
                    )
                });
                parse_str_and_append(s, kind, typed_col, col_idx, line_number)
            } else {
                // Rare: escaped non-text field — unescape first, then parse
                let unescaped = unescape_field_always(raw);
                parse_str_and_append(&unescaped, kind, typed_col, col_idx, line_number)
            }
        }
    }
}

/// Push a NULL value into any typed column.
#[inline]
fn push_null(typed_col: &mut TypedColumn) -> Result<(), ParseError> {
    match typed_col {
        TypedColumn::Int16(v) => v.push(None),
        TypedColumn::Int32(v) => v.push(None),
        TypedColumn::Int64(v) => v.push(None),
        TypedColumn::Float32(v) => v.push(None),
        TypedColumn::Float64(v) => v.push(None),
        TypedColumn::Bool(v) => v.push(None),
        TypedColumn::Text(v) => v.push(None),
        TypedColumn::Bytes(v) => v.push(None),
    }
    Ok(())
}

/// Unescape a field that is known not to be NULL.
pub(crate) fn unescape_field_always(raw: &[u8]) -> String {
    // Fast path: no backslash
    if memchr::memchr(b'\\', raw).is_none() {
        return unsafe { std::str::from_utf8_unchecked(raw) }.to_string();
    }
    unescape_bytes(raw)
}

/// Parse a `&str` value and append to typed column (no allocation for the input).
fn parse_str_and_append(
    s: &str,
    kind: ColumnKind,
    typed_col: &mut TypedColumn,
    col_idx: usize,
    line_number: u64,
) -> Result<(), ParseError> {
    match kind {
        ColumnKind::Int16 => {
            let v = s.parse::<i16>().map_err(|e| ParseError {
                message: format!("invalid int2: {}", e),
                column: col_idx,
                line: line_number,
            })?;
            if let TypedColumn::Int16(vec) = typed_col {
                vec.push(Some(v));
            }
            Ok(())
        }
        ColumnKind::Int32 => {
            let v = s.parse::<i32>().map_err(|e| ParseError {
                message: format!("invalid int4: {}", e),
                column: col_idx,
                line: line_number,
            })?;
            if let TypedColumn::Int32(vec) = typed_col {
                vec.push(Some(v));
            }
            Ok(())
        }
        ColumnKind::Int64 => {
            let v = s.parse::<i64>().map_err(|e| ParseError {
                message: format!("invalid int8: {}", e),
                column: col_idx,
                line: line_number,
            })?;
            if let TypedColumn::Int64(vec) = typed_col {
                vec.push(Some(v));
            }
            Ok(())
        }
        ColumnKind::Float32 => {
            let v = parse_float32(s).map_err(|e| ParseError {
                message: format!("invalid float4: {}", e),
                column: col_idx,
                line: line_number,
            })?;
            if let TypedColumn::Float32(vec) = typed_col {
                vec.push(Some(v));
            }
            Ok(())
        }
        ColumnKind::Float64 => {
            let v = parse_float64(s).map_err(|e| ParseError {
                message: format!("invalid float8: {}", e),
                column: col_idx,
                line: line_number,
            })?;
            if let TypedColumn::Float64(vec) = typed_col {
                vec.push(Some(v));
            }
            Ok(())
        }
        ColumnKind::Bool => {
            let v = parse_bool(s).map_err(|e| ParseError {
                message: e,
                column: col_idx,
                line: line_number,
            })?;
            if let TypedColumn::Bool(vec) = typed_col {
                vec.push(Some(v));
            }
            Ok(())
        }
        ColumnKind::Timestamp | ColumnKind::TimestampTz => {
            let usec = timeparse::parse_timestamp_to_usec(s);
            if let TypedColumn::Int64(vec) = typed_col {
                vec.push(Some(usec));
            }
            Ok(())
        }
        ColumnKind::Date => {
            let usec = timeparse::parse_timestamp_to_usec(s);
            if let TypedColumn::Int64(vec) = typed_col {
                vec.push(Some(usec));
            }
            Ok(())
        }
        ColumnKind::Text => {
            if let TypedColumn::Text(vec) = typed_col {
                vec.push(Some(s.to_string()));
            }
            Ok(())
        }
        ColumnKind::Jsonb => {
            // See `parse_raw_field_and_append`'s Jsonb arm — workers pass a
            // `Text` column so binary conversion is deferred to the main thread.
            match typed_col {
                TypedColumn::Bytes(vec) => {
                    let bytes = unsafe { crate::compress::jsonb_text_to_binary(s) };
                    vec.push(Some(bytes));
                }
                TypedColumn::Text(vec) => {
                    vec.push(Some(s.to_string()));
                }
                _ => {}
            }
            Ok(())
        }
    }
}

/// Parse a text field and append to the appropriate typed column.
///
/// `field` is `None` for NULL values. `kind` determines the target type.
/// For timestamp/timestamptz/date, `parse_timestamp_to_usec` already returns
/// Unix epoch usec, so no PG_EPOCH_OFFSET correction is needed.
pub fn parse_and_append(
    field: Option<&str>,
    kind: ColumnKind,
    typed_col: &mut TypedColumn,
    col_idx: usize,
    line_number: u64,
) -> Result<(), ParseError> {
    match field {
        None => {
            // NULL
            match typed_col {
                TypedColumn::Int16(v) => v.push(None),
                TypedColumn::Int32(v) => v.push(None),
                TypedColumn::Int64(v) => v.push(None),
                TypedColumn::Float32(v) => v.push(None),
                TypedColumn::Float64(v) => v.push(None),
                TypedColumn::Bool(v) => v.push(None),
                TypedColumn::Text(v) => v.push(None),
                TypedColumn::Bytes(v) => v.push(None),
            }
            Ok(())
        }
        Some(s) => match kind {
            ColumnKind::Int16 => {
                let v = s.parse::<i16>().map_err(|e| ParseError {
                    message: format!("invalid int2: {}", e),
                    column: col_idx,
                    line: line_number,
                })?;
                if let TypedColumn::Int16(vec) = typed_col {
                    vec.push(Some(v));
                }
                Ok(())
            }
            ColumnKind::Int32 => {
                let v = s.parse::<i32>().map_err(|e| ParseError {
                    message: format!("invalid int4: {}", e),
                    column: col_idx,
                    line: line_number,
                })?;
                if let TypedColumn::Int32(vec) = typed_col {
                    vec.push(Some(v));
                }
                Ok(())
            }
            ColumnKind::Int64 => {
                let v = s.parse::<i64>().map_err(|e| ParseError {
                    message: format!("invalid int8: {}", e),
                    column: col_idx,
                    line: line_number,
                })?;
                if let TypedColumn::Int64(vec) = typed_col {
                    vec.push(Some(v));
                }
                Ok(())
            }
            ColumnKind::Float32 => {
                let v = parse_float32(s).map_err(|e| ParseError {
                    message: format!("invalid float4: {}", e),
                    column: col_idx,
                    line: line_number,
                })?;
                if let TypedColumn::Float32(vec) = typed_col {
                    vec.push(Some(v));
                }
                Ok(())
            }
            ColumnKind::Float64 => {
                let v = parse_float64(s).map_err(|e| ParseError {
                    message: format!("invalid float8: {}", e),
                    column: col_idx,
                    line: line_number,
                })?;
                if let TypedColumn::Float64(vec) = typed_col {
                    vec.push(Some(v));
                }
                Ok(())
            }
            ColumnKind::Bool => {
                let v = parse_bool(s).map_err(|e| ParseError {
                    message: e,
                    column: col_idx,
                    line: line_number,
                })?;
                if let TypedColumn::Bool(vec) = typed_col {
                    vec.push(Some(v));
                }
                Ok(())
            }
            ColumnKind::Timestamp | ColumnKind::TimestampTz => {
                let usec = timeparse::parse_timestamp_to_usec(s);
                if let TypedColumn::Int64(vec) = typed_col {
                    vec.push(Some(usec));
                }
                Ok(())
            }
            ColumnKind::Date => {
                let usec = timeparse::parse_timestamp_to_usec(s);
                if let TypedColumn::Int64(vec) = typed_col {
                    vec.push(Some(usec));
                }
                Ok(())
            }
            ColumnKind::Text => {
                if let TypedColumn::Text(vec) = typed_col {
                    vec.push(Some(s.to_string()));
                }
                Ok(())
            }
            ColumnKind::Jsonb => {
                let bytes = unsafe { crate::compress::jsonb_text_to_binary(s) };
                if let TypedColumn::Bytes(vec) = typed_col {
                    vec.push(Some(bytes));
                }
                Ok(())
            }
        },
    }
}

/// Parse boolean matching PG's `boolin`: t/f/true/false/yes/no/on/off/1/0
fn parse_bool(s: &str) -> Result<bool, String> {
    // Fast path for single-char
    if s.len() == 1 {
        match s.as_bytes()[0] {
            b't' | b'T' | b'1' => return Ok(true),
            b'f' | b'F' | b'0' => return Ok(false),
            _ => {}
        }
    }
    match s.to_ascii_lowercase().as_str() {
        "t" | "true" | "yes" | "on" | "1" => Ok(true),
        "f" | "false" | "no" | "off" | "0" => Ok(false),
        _ => Err(format!("invalid boolean: {:?}", s)),
    }
}

/// Parse float32, handling NaN/Inf/-Inf/Infinity
fn parse_float32(s: &str) -> Result<f32, String> {
    match s.to_ascii_lowercase().as_str() {
        "nan" => Ok(f32::NAN),
        "inf" | "infinity" => Ok(f32::INFINITY),
        "-inf" | "-infinity" => Ok(f32::NEG_INFINITY),
        _ => s.parse::<f32>().map_err(|e| e.to_string()),
    }
}

/// Parse float64, handling NaN/Inf/-Inf/Infinity
fn parse_float64(s: &str) -> Result<f64, String> {
    match s.to_ascii_lowercase().as_str() {
        "nan" => Ok(f64::NAN),
        "inf" | "infinity" => Ok(f64::INFINITY),
        "-inf" | "-infinity" => Ok(f64::NEG_INFINITY),
        _ => s.parse::<f64>().map_err(|e| e.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compress::ColumnKind;

    // ===== Line Reader Tests =====

    #[test]
    fn test_line_reader_lf() {
        let buf = b"line1\nline2\nline3\n";
        let mut reader = CopyLineReader::new();
        let mut pos = 0;

        match reader.next_line(buf, pos) {
            LineResult::Row(s, e) => {
                assert_eq!(&buf[s..e], b"line1");
                pos = e + 1; // skip \n
            }
            _ => panic!("expected Row"),
        }
        match reader.next_line(buf, pos) {
            LineResult::Row(s, e) => {
                assert_eq!(&buf[s..e], b"line2");
                pos = e + 1;
            }
            _ => panic!("expected Row"),
        }
        match reader.next_line(buf, pos) {
            LineResult::Row(s, e) => {
                assert_eq!(&buf[s..e], b"line3");
                pos = e + 1;
            }
            _ => panic!("expected Row"),
        }
        match reader.next_line(buf, pos) {
            LineResult::Incomplete => {}
            _ => panic!("expected Incomplete"),
        }
    }

    #[test]
    fn test_line_reader_crlf() {
        let buf = b"line1\r\nline2\r\n";
        let mut reader = CopyLineReader::new();
        let mut pos = 0;

        match reader.next_line(buf, pos) {
            LineResult::Row(s, e) => {
                assert_eq!(&buf[s..e], b"line1");
                pos = e + 2; // skip \r\n
            }
            _ => panic!("expected Row"),
        }
        match reader.next_line(buf, pos) {
            LineResult::Row(s, e) => {
                assert_eq!(&buf[s..e], b"line2");
                pos = e + 2;
            }
            _ => panic!("expected Row"),
        }
        assert!(matches!(reader.next_line(buf, pos), LineResult::Incomplete));
    }

    #[test]
    fn test_line_reader_cr() {
        let buf = b"line1\rline2\r";
        let mut reader = CopyLineReader::new();
        match reader.next_line(buf, 0) {
            LineResult::Row(s, e) => {
                assert_eq!(&buf[s..e], b"line1");
            }
            _ => panic!("expected Row"),
        }
    }

    #[test]
    #[should_panic(expected = "inconsistent line endings")]
    fn test_line_reader_mixed_eol_error() {
        let buf = b"line1\nline2\r\n";
        let mut reader = CopyLineReader::new();
        let mut pos = 0;
        match reader.next_line(buf, pos) {
            LineResult::Row(_, e) => pos = e + 1,
            _ => panic!("expected Row"),
        }
        // This should panic because first line was LF, second is CRLF
        let _ = reader.next_line(buf, pos);
    }

    #[test]
    fn test_line_reader_end_of_copy() {
        let buf = b"line1\n\\.\n";
        let mut reader = CopyLineReader::new();
        match reader.next_line(buf, 0) {
            LineResult::Row(s, e) => {
                assert_eq!(&buf[s..e], b"line1");
                match reader.next_line(buf, e + 1) {
                    LineResult::EndOfCopy => {}
                    _ => panic!("expected EndOfCopy"),
                }
            }
            _ => panic!("expected Row"),
        }
    }

    #[test]
    fn test_line_reader_escaped_backslash_dot_not_eoc() {
        // \\. in raw bytes is [0x5C, 0x5C, 0x2E] — the first \ escapes the second,
        // so the line is NOT end-of-copy
        let buf = b"\\\\.some_data\n";
        let mut reader = CopyLineReader::new();
        match reader.next_line(buf, 0) {
            LineResult::Row(s, e) => {
                assert_eq!(&buf[s..e], b"\\\\.some_data");
            }
            _ => panic!("expected Row"),
        }
    }

    #[test]
    fn test_line_reader_empty_line() {
        let buf = b"\nline2\n";
        let mut reader = CopyLineReader::new();
        match reader.next_line(buf, 0) {
            LineResult::Row(s, e) => {
                assert_eq!(&buf[s..e], b"");
                assert_eq!(s, 0);
                assert_eq!(e, 0);
            }
            _ => panic!("expected Row"),
        }
    }

    #[test]
    fn test_line_reader_incomplete() {
        let buf = b"no newline here";
        let mut reader = CopyLineReader::new();
        assert!(matches!(reader.next_line(buf, 0), LineResult::Incomplete));
    }

    #[test]
    fn test_line_reader_escaped_newline_in_line() {
        // \n in the data (as backslash-n, two bytes 0x5C 0x6E) should NOT split the line
        let buf = b"col1\\ncol2\n";
        let mut reader = CopyLineReader::new();
        match reader.next_line(buf, 0) {
            LineResult::Row(s, e) => {
                assert_eq!(&buf[s..e], b"col1\\ncol2");
            }
            _ => panic!("expected Row"),
        }
    }

    // ===== Field Splitter Tests =====

    #[test]
    fn test_split_fields_tab() {
        let fields = split_fields(b"a\tb\tc", b'\t');
        assert_eq!(fields, vec![b"a".as_slice(), b"b", b"c"]);
    }

    #[test]
    fn test_split_fields_comma() {
        let fields = split_fields(b"a,b,c", b',');
        assert_eq!(fields, vec![b"a".as_slice(), b"b", b"c"]);
    }

    #[test]
    fn test_split_fields_pipe() {
        let fields = split_fields(b"a|b|c", b'|');
        assert_eq!(fields, vec![b"a".as_slice(), b"b", b"c"]);
    }

    #[test]
    fn test_split_fields_escaped_delimiter() {
        // \, with comma delimiter — should NOT split
        let fields = split_fields(b"a\\,b,c", b',');
        assert_eq!(fields, vec![b"a\\,b".as_slice(), b"c"]);
    }

    #[test]
    fn test_split_fields_double_backslash_before_delimiter() {
        // \\, → the first \ escapes the second, so , IS a delimiter
        let fields = split_fields(b"a\\\\,b", b',');
        assert_eq!(fields, vec![b"a\\\\".as_slice(), b"b"]);
    }

    #[test]
    fn test_split_fields_single_field() {
        let fields = split_fields(b"hello", b'\t');
        assert_eq!(fields, vec![b"hello".as_slice()]);
    }

    #[test]
    fn test_split_fields_empty() {
        let fields = split_fields(b"", b'\t');
        assert_eq!(fields, vec![b"".as_slice()]);
    }

    #[test]
    fn test_split_fields_empty_fields() {
        let fields = split_fields(b"\t\t", b'\t');
        assert_eq!(fields, vec![b"".as_slice(), b"".as_slice(), b"".as_slice()]);
    }

    // ===== NULL Detection + Unescape Tests =====

    #[test]
    fn test_unescape_null_default() {
        // \N (two bytes: 0x5C, 0x4E) matches default null string
        assert_eq!(unescape_field(b"\\N", b"\\N"), None);
    }

    #[test]
    fn test_unescape_escaped_backslash_n_not_null() {
        // \\N (three bytes: 0x5C, 0x5C, 0x4E) → unescape to \N string
        let result = unescape_field(b"\\\\N", b"\\N");
        assert_eq!(result, Some("\\N".to_string()));
    }

    #[test]
    fn test_unescape_backslash_n_n_not_null() {
        // \NN (three bytes: 0x5C, 0x4E, 0x4E) — raw is 3 bytes, doesn't match 2-byte null
        let result = unescape_field(b"\\NN", b"\\N");
        assert_eq!(result, Some("NN".to_string()));
    }

    #[test]
    fn test_unescape_empty_field() {
        assert_eq!(unescape_field(b"", b"\\N"), Some(String::new()));
    }

    #[test]
    fn test_unescape_custom_null() {
        assert_eq!(unescape_field(b"NULL", b"NULL"), None);
        assert_eq!(unescape_field(b"hello", b"NULL"), Some("hello".to_string()));
    }

    #[test]
    fn test_unescape_named_escapes() {
        assert_eq!(unescape_field(b"\\b", b"\\N"), Some("\x08".to_string()));
        assert_eq!(unescape_field(b"\\f", b"\\N"), Some("\x0C".to_string()));
        assert_eq!(unescape_field(b"\\n", b"\\N"), Some("\n".to_string()));
        assert_eq!(unescape_field(b"\\r", b"\\N"), Some("\r".to_string()));
        assert_eq!(unescape_field(b"\\t", b"\\N"), Some("\t".to_string()));
        assert_eq!(unescape_field(b"\\v", b"\\N"), Some("\x0B".to_string()));
    }

    #[test]
    fn test_unescape_octal() {
        // \0 → 0x00
        assert_eq!(unescape_field(b"\\0", b"\\N"), Some("\x00".to_string()));
        // \7 → 0x07
        assert_eq!(unescape_field(b"\\7", b"\\N"), Some("\x07".to_string()));
        // \77 → 0x3F = '?'
        assert_eq!(unescape_field(b"\\77", b"\\N"), Some("?".to_string()));
        // \377 → 0xFF — but 0xFF is not valid UTF-8 alone.
        // PG allows this for bytea but for text it would be questionable.
        // Skip this edge case in our text parser.
    }

    #[test]
    fn test_unescape_hex() {
        // \x41 → 'A'
        assert_eq!(unescape_field(b"\\x41", b"\\N"), Some("A".to_string()));
        // \xG → literal "xG" (G is not hex)
        assert_eq!(unescape_field(b"\\xG", b"\\N"), Some("xG".to_string()));
    }

    #[test]
    fn test_unescape_unknown_escape() {
        // \q → 'q'
        assert_eq!(unescape_field(b"\\q", b"\\N"), Some("q".to_string()));
    }

    #[test]
    fn test_unescape_no_backslash_fast_path() {
        assert_eq!(
            unescape_field(b"hello world", b"\\N"),
            Some("hello world".to_string())
        );
    }

    #[test]
    fn test_unescape_backslash_backslash() {
        assert_eq!(unescape_field(b"\\\\", b"\\N"), Some("\\".to_string()));
    }

    // ===== Type Conversion Tests =====

    fn make_int16_col() -> TypedColumn {
        TypedColumn::Int16(Vec::new())
    }
    fn make_int32_col() -> TypedColumn {
        TypedColumn::Int32(Vec::new())
    }
    fn make_int64_col() -> TypedColumn {
        TypedColumn::Int64(Vec::new())
    }
    fn make_f32_col() -> TypedColumn {
        TypedColumn::Float32(Vec::new())
    }
    fn make_f64_col() -> TypedColumn {
        TypedColumn::Float64(Vec::new())
    }
    fn make_bool_col() -> TypedColumn {
        TypedColumn::Bool(Vec::new())
    }
    fn make_text_col() -> TypedColumn {
        TypedColumn::Text(Vec::new())
    }

    #[test]
    fn test_parse_int16() {
        let mut col = make_int16_col();
        parse_and_append(Some("42"), ColumnKind::Int16, &mut col, 0, 1).unwrap();
        parse_and_append(Some("-1"), ColumnKind::Int16, &mut col, 0, 1).unwrap();
        parse_and_append(None, ColumnKind::Int16, &mut col, 0, 1).unwrap();
        if let TypedColumn::Int16(v) = &col {
            assert_eq!(v, &[Some(42i16), Some(-1), None]);
        }
    }

    #[test]
    fn test_parse_int16_overflow() {
        let mut col = make_int16_col();
        assert!(parse_and_append(Some("99999"), ColumnKind::Int16, &mut col, 0, 1).is_err());
    }

    #[test]
    fn test_parse_int32() {
        let mut col = make_int32_col();
        parse_and_append(Some("123456"), ColumnKind::Int32, &mut col, 0, 1).unwrap();
        parse_and_append(Some("-42"), ColumnKind::Int32, &mut col, 0, 1).unwrap();
        if let TypedColumn::Int32(v) = &col {
            assert_eq!(v, &[Some(123456i32), Some(-42)]);
        }
    }

    #[test]
    fn test_parse_int64() {
        let mut col = make_int64_col();
        parse_and_append(
            Some("9223372036854775807"),
            ColumnKind::Int64,
            &mut col,
            0,
            1,
        )
        .unwrap();
        if let TypedColumn::Int64(v) = &col {
            assert_eq!(v, &[Some(i64::MAX)]);
        }
    }

    #[test]
    fn test_parse_float32() {
        let mut col = make_f32_col();
        parse_and_append(Some("3.25"), ColumnKind::Float32, &mut col, 0, 1).unwrap();
        parse_and_append(Some("NaN"), ColumnKind::Float32, &mut col, 0, 1).unwrap();
        parse_and_append(Some("Infinity"), ColumnKind::Float32, &mut col, 0, 1).unwrap();
        parse_and_append(Some("-Infinity"), ColumnKind::Float32, &mut col, 0, 1).unwrap();
        if let TypedColumn::Float32(v) = &col {
            assert!((v[0].unwrap() - 3.25).abs() < 0.001);
            assert!(v[1].unwrap().is_nan());
            assert!(v[2].unwrap().is_infinite() && v[2].unwrap() > 0.0);
            assert!(v[3].unwrap().is_infinite() && v[3].unwrap() < 0.0);
        }
    }

    #[test]
    fn test_parse_float64() {
        let mut col = make_f64_col();
        parse_and_append(Some("1.23e10"), ColumnKind::Float64, &mut col, 0, 1).unwrap();
        parse_and_append(Some("NaN"), ColumnKind::Float64, &mut col, 0, 1).unwrap();
        parse_and_append(Some("Inf"), ColumnKind::Float64, &mut col, 0, 1).unwrap();
        parse_and_append(Some("-Inf"), ColumnKind::Float64, &mut col, 0, 1).unwrap();
        if let TypedColumn::Float64(v) = &col {
            assert!((v[0].unwrap() - 1.23e10).abs() < 1.0);
            assert!(v[1].unwrap().is_nan());
            assert!(v[2].unwrap().is_infinite());
            assert!(v[3].unwrap().is_infinite() && v[3].unwrap() < 0.0);
        }
    }

    #[test]
    fn test_parse_bool_variants() {
        let cases = [
            ("t", true),
            ("f", false),
            ("true", true),
            ("false", false),
            ("TRUE", true),
            ("FALSE", false),
            ("True", true),
            ("False", false),
            ("yes", true),
            ("no", false),
            ("on", true),
            ("off", false),
            ("1", true),
            ("0", false),
        ];
        for (input, expected) in &cases {
            let mut col = make_bool_col();
            parse_and_append(Some(input), ColumnKind::Bool, &mut col, 0, 1).unwrap();
            if let TypedColumn::Bool(v) = &col {
                assert_eq!(v[0], Some(*expected), "failed for input: {}", input);
            }
        }
    }

    #[test]
    fn test_parse_timestamp() {
        let mut col = TypedColumn::Int64(Vec::new());
        parse_and_append(
            Some("2013-07-15 10:23:45"),
            ColumnKind::Timestamp,
            &mut col,
            0,
            1,
        )
        .unwrap();
        if let TypedColumn::Int64(v) = &col {
            assert_eq!(v[0], Some(1_373_883_825_000_000));
        }
    }

    #[test]
    fn test_parse_timestamptz() {
        let mut col = TypedColumn::Int64(Vec::new());
        parse_and_append(
            Some("2013-07-15 10:23:45+03"),
            ColumnKind::TimestampTz,
            &mut col,
            0,
            1,
        )
        .unwrap();
        if let TypedColumn::Int64(v) = &col {
            assert_eq!(v[0], Some(1_373_883_825_000_000 - 3 * 3_600_000_000));
        }
    }

    #[test]
    fn test_parse_date() {
        let mut col = TypedColumn::Int64(Vec::new());
        parse_and_append(Some("2013-07-15"), ColumnKind::Date, &mut col, 0, 1).unwrap();
        if let TypedColumn::Int64(v) = &col {
            assert_eq!(v[0], Some(1_373_846_400_000_000));
        }
    }

    #[test]
    fn test_parse_text() {
        let mut col = make_text_col();
        parse_and_append(Some("hello"), ColumnKind::Text, &mut col, 0, 1).unwrap();
        parse_and_append(None, ColumnKind::Text, &mut col, 0, 1).unwrap();
        if let TypedColumn::Text(v) = &col {
            assert_eq!(v, &[Some("hello".to_string()), None]);
        }
    }

    // ===== End-to-end Pure-Rust Tests =====

    #[test]
    fn test_end_to_end_tsv() {
        // Simulate a multi-line TSV with various types and NULLs
        let data = b"42\t3.14\t\\N\thello world\n\
                     -1\t2.72\tsome text\t\\N\n";

        let opts = CopyTextOptions::default();
        let mut reader = CopyLineReader::new();
        let mut pos = 0;

        // Line 1
        match reader.next_line(data, pos) {
            LineResult::Row(s, e) => {
                let fields = split_fields(&data[s..e], opts.delimiter);
                assert_eq!(fields.len(), 4);

                let f0 = unescape_field(fields[0], &opts.null_string);
                assert_eq!(f0, Some("42".to_string()));

                let f1 = unescape_field(fields[1], &opts.null_string);
                assert_eq!(f1, Some("3.14".to_string()));

                let f2 = unescape_field(fields[2], &opts.null_string);
                assert_eq!(f2, None); // \N → NULL

                let f3 = unescape_field(fields[3], &opts.null_string);
                assert_eq!(f3, Some("hello world".to_string()));

                pos = e + 1;
            }
            _ => panic!("expected Row"),
        }

        // Line 2
        match reader.next_line(data, pos) {
            LineResult::Row(s, e) => {
                let fields = split_fields(&data[s..e], opts.delimiter);
                assert_eq!(fields.len(), 4);

                let f0 = unescape_field(fields[0], &opts.null_string);
                assert_eq!(f0, Some("-1".to_string()));

                let f2 = unescape_field(fields[2], &opts.null_string);
                assert_eq!(f2, Some("some text".to_string()));

                let f3 = unescape_field(fields[3], &opts.null_string);
                assert_eq!(f3, None); // \N → NULL
            }
            _ => panic!("expected Row"),
        }
    }

    #[test]
    fn test_end_to_end_with_type_conversion() {
        let line = b"42\t2013-07-15 10:23:45\thello\t\\N\ttrue";
        let fields = split_fields(line, b'\t');
        let kinds = [
            ColumnKind::Int32,
            ColumnKind::Timestamp,
            ColumnKind::Text,
            ColumnKind::Float64,
            ColumnKind::Bool,
        ];
        let null_string = b"\\N".as_slice();

        let mut cols: Vec<TypedColumn> = vec![
            make_int32_col(),
            TypedColumn::Int64(Vec::new()),
            make_text_col(),
            make_f64_col(),
            make_bool_col(),
        ];

        for (i, (field, kind)) in fields.iter().zip(kinds.iter()).enumerate() {
            let unescaped = unescape_field(field, null_string);
            parse_and_append(unescaped.as_deref(), *kind, &mut cols[i], i, 1).unwrap();
        }

        if let TypedColumn::Int32(v) = &cols[0] {
            assert_eq!(v[0], Some(42));
        }
        if let TypedColumn::Int64(v) = &cols[1] {
            assert_eq!(v[0], Some(1_373_883_825_000_000));
        }
        if let TypedColumn::Text(v) = &cols[2] {
            assert_eq!(v[0], Some("hello".to_string()));
        }
        if let TypedColumn::Float64(v) = &cols[3] {
            assert_eq!(v[0], None); // was \N
        }
        if let TypedColumn::Bool(v) = &cols[4] {
            assert_eq!(v[0], Some(true));
        }
    }

    #[test]
    fn test_pg_copy2_null_variants() {
        // Ported from PG's copy2.sql: a row with \N \\N \NN
        // \N → NULL
        // \\N → string "\N"
        // \NN → string "NN"
        let line = b"\\N\t\\\\N\t\\NN";
        let fields = split_fields(line, b'\t');
        let null_string = b"\\N";

        let r0 = unescape_field(fields[0], null_string);
        assert_eq!(r0, None); // NULL

        let r1 = unescape_field(fields[1], null_string);
        assert_eq!(r1, Some("\\N".to_string())); // escaped backslash + N

        let r2 = unescape_field(fields[2], null_string);
        assert_eq!(r2, Some("NN".to_string())); // \N + extra N → not null match, unescape \N to N, keep extra N
    }

    #[test]
    fn test_header_skip_mode() {
        // Verify that first line can be consumed as header
        let data = b"col1\tcol2\tcol3\n1\t2\t3\n";
        let mut reader = CopyLineReader::new();

        // Read and skip header
        match reader.next_line(data, 0) {
            LineResult::Row(s, e) => {
                assert_eq!(&data[s..e], b"col1\tcol2\tcol3");
                // Data line
                match reader.next_line(data, e + 1) {
                    LineResult::Row(s2, e2) => {
                        assert_eq!(&data[s2..e2], b"1\t2\t3");
                    }
                    _ => panic!("expected Row"),
                }
            }
            _ => panic!("expected Row for header"),
        }
    }
}
