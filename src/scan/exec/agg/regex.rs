//! Regex / CASE WHEN helpers for GROUP BY transformations.
//!
//! - Regex group keys (`GROUP BY regexp_replace(col, ...)`): translate PG
//!   regex syntax to Rust regex, then apply per-segment for the parallel
//!   mixed path.
//! - CASE WHEN group keys: evaluate the conditional and dict-encode the
//!   result string per segment.

use std::collections::HashMap;

use pgrx::pg_sys;
use pgrx::warning;
use regex::Regex;

use super::super::text_col::SegTextColumn;
use super::{CaseWhenOp, CaseWhenSpec, CaseWhenValue};

/// Info for a regexp GROUP BY column that compiled successfully with Rust regex.
pub(super) struct RustRegexInfo {
    pub(super) regex: Regex,
    pub(super) replacement: String,
    pub(super) col_idx: usize,
}

/// Detect POSIX character classes (e.g. `[:alpha:]`) inside `[]` —
/// Rust's regex crate doesn't support them, so we fall back to PG's
/// regex for these patterns instead of mis-compiling.
pub(super) fn has_posix_classes(pattern: &str) -> bool {
    let bytes = pattern.as_bytes();
    let mut in_bracket = false;
    for i in 0..bytes.len() {
        if bytes[i] == b'[' && !in_bracket {
            in_bracket = true;
        } else if bytes[i] == b']' && in_bracket {
            in_bracket = false;
        } else if in_bracket && bytes[i] == b'[' && i + 1 < bytes.len() && bytes[i + 1] == b':' {
            return true;
        }
    }
    false
}

/// Convert PG replacement syntax (\1, \2, \&) to Rust regex syntax ($1, $2, $0).
pub(super) fn convert_pg_replacement(replacement: &str) -> String {
    let mut result = String::with_capacity(replacement.len());
    let bytes = replacement.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() {
            let next = bytes[i + 1];
            if next.is_ascii_digit() {
                result.push('$');
                result.push(next as char);
                i += 2;
                continue;
            } else if next == b'&' {
                result.push_str("$0");
                i += 2;
                continue;
            } else if next == b'\\' {
                result.push('\\');
                i += 2;
                continue;
            }
        }
        result.push(bytes[i] as char);
        i += 1;
    }
    result
}

/// Convert a PG regex pattern to Rust regex, adjusting for semantic differences.
/// 1. PG's ARE mode: `.` matches `\n` by default (REG_NLSTOP is NOT set).
///    Rust regex: `.` does NOT match `\n`. Fix: prepend `(?s)` (dot-all mode).
/// 2. PG's `$` is strict end-of-string.
///    Rust's `$` also matches before trailing `\n`. Fix: convert trailing `$` to `\z`.
pub(super) fn pg_pattern_to_rust(pattern: &str) -> String {
    let mut result = String::with_capacity(pattern.len() + 8);
    // Enable dot-all mode so . matches \n (matching PG's ARE default)
    result.push_str("(?s)");

    // Replace unescaped $ at end of pattern with \z
    if let Some(prefix) = pattern.strip_suffix('$') {
        let preceding_backslashes = prefix.chars().rev().take_while(|&c| c == '\\').count();
        if preceding_backslashes % 2 == 0 {
            result.push_str(prefix);
            result.push_str("\\z");
            return result;
        }
    }
    result.push_str(pattern);
    result
}

/// Try to compile a PG regex pattern for use with Rust regex crate.
/// Returns Some(Regex) if compatible, None if incompatible (with warning logged).
pub(super) fn try_compile_rust_regex(pattern: &str) -> Option<Regex> {
    if !crate::get_parallel_regex() {
        return None;
    }
    if has_posix_classes(pattern) {
        warning!(
            "pg_deltax: regex pattern contains POSIX character classes, falling back to PG regex (pattern: {})",
            pattern
        );
        return None;
    }
    let rust_pattern = pg_pattern_to_rust(pattern);
    match Regex::new(&rust_pattern) {
        Ok(re) => Some(re),
        Err(e) => {
            warning!(
                "pg_deltax: regex pattern not supported by Rust regex crate, falling back to PG regex (pattern: {}, error: {})",
                pattern,
                e
            );
            None
        }
    }
}

/// Evaluate a CASE WHEN expression on a segment, producing a SegTextColumn.
///
/// For each row, evaluates clauses in order; first match wins, else default.
/// Condition columns come from `numeric_cols`, result ColumnRef values from `text_seg_cols`.
pub(super) fn apply_case_when_to_seg_col(
    spec: &CaseWhenSpec,
    numeric_cols: &[Vec<(pg_sys::Datum, bool)>],
    text_seg_cols: &[Option<SegTextColumn>],
    row_count: usize,
    selection: &[bool],
) -> SegTextColumn {
    // Build dict-style: unique strings → entries, per-row index.
    let mut unique_map: HashMap<String, u32> = HashMap::new();
    let mut entries: Vec<String> = Vec::new();
    let mut row_to_entry: Vec<u32> = Vec::with_capacity(row_count);

    for row in 0..row_count {
        if !selection.is_empty() && !selection[row] {
            row_to_entry.push(u32::MAX); // filtered out, treat as null
            continue;
        }

        // Evaluate clauses in order
        let mut matched_value: Option<&CaseWhenValue> = None;
        'clauses: for clause in &spec.clauses {
            let mut all_conditions_true = true;
            for cond in &clause.conditions {
                let col = &numeric_cols[cond.col_idx];
                if col.is_empty() || col[row].1 {
                    // NULL column value — condition is false
                    all_conditions_true = false;
                    break;
                }
                let val = col[row].0.value() as i64;
                let cond_met = match cond.op {
                    CaseWhenOp::Eq => val == cond.const_val,
                    CaseWhenOp::NotEq => val != cond.const_val,
                };
                if !cond_met {
                    all_conditions_true = false;
                    break;
                }
            }
            if all_conditions_true {
                matched_value = Some(&clause.result);
                break 'clauses;
            }
        }
        let value = matched_value.unwrap_or(&spec.default);

        // Resolve the value to a string
        let s: Option<String> = match value {
            CaseWhenValue::StringConst(s) => Some(s.clone()),
            CaseWhenValue::ColumnRef(col_idx) => {
                if let Some(ref seg_col) = text_seg_cols[*col_idx] {
                    seg_col.get_str(row).map(|s| s.to_owned())
                } else {
                    None // null
                }
            }
        };

        match s {
            Some(string_val) => {
                let idx = *unique_map.entry(string_val.clone()).or_insert_with(|| {
                    let idx = entries.len() as u32;
                    entries.push(string_val);
                    idx
                });
                row_to_entry.push(idx);
            }
            None => {
                row_to_entry.push(u32::MAX);
            }
        }
    }

    SegTextColumn::Dict {
        entries,
        row_to_entry,
    }
}

/// Apply a Rust regex replacement to a SegTextColumn, producing a new transformed column.
/// The original column is not modified (needed for aggregations on the same column).
/// For Dict columns, only applies regex to unique dict entries (O(dict_size)).
/// For LZ4 columns, converts to Dict after applying regex.
pub(super) fn apply_regex_to_seg_col(
    seg_col: &SegTextColumn,
    regex: &Regex,
    replacement: &str,
) -> SegTextColumn {
    match seg_col {
        SegTextColumn::Dict {
            entries,
            row_to_entry,
        } => {
            let new_entries: Vec<String> = entries
                .iter()
                .map(|e| regex.replace(e, replacement).into_owned())
                .collect();
            SegTextColumn::Dict {
                entries: new_entries,
                row_to_entry: row_to_entry.clone(),
            }
        }
        SegTextColumn::Lz4 { buf, row_to_range } => {
            let mut unique_map: HashMap<String, u32> = HashMap::new();
            let mut entries: Vec<String> = Vec::new();
            let mut new_row_to_entry: Vec<u32> = Vec::with_capacity(row_to_range.len());
            for &(off, len) in row_to_range {
                if off == u32::MAX {
                    new_row_to_entry.push(u32::MAX);
                } else {
                    let s = std::str::from_utf8(&buf[off as usize..off as usize + len as usize])
                        .unwrap_or("");
                    let replaced = regex.replace(s, replacement).into_owned();
                    let idx = *unique_map.entry(replaced.clone()).or_insert_with(|| {
                        let idx = entries.len() as u32;
                        entries.push(replaced);
                        idx
                    });
                    new_row_to_entry.push(idx);
                }
            }
            SegTextColumn::Dict {
                entries,
                row_to_entry: new_row_to_entry,
            }
        }
        SegTextColumn::SegBy(opt) => {
            let new_opt = opt
                .as_deref()
                .map(|s| regex.replace(s, replacement).into_owned());
            SegTextColumn::SegBy(new_opt)
        }
        SegTextColumn::Lengths {
            lengths,
            null_bitmap,
        } => {
            // Regex on a length-only column is meaningless (the planner should
            // never route a RegexpReplace column into sidecar mode). Preserve
            // the shape so callers don't panic if this ever fires.
            SegTextColumn::Lengths {
                lengths: lengths.clone(),
                null_bitmap: null_bitmap.clone(),
            }
        }
    }
}
