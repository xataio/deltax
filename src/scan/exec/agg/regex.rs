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

use super::super::text_col::{SegTextColumn, dict_entry_str};
use super::{CaseWhenOp, CaseWhenSpec, CaseWhenValue};

/// Info for a regexp GROUP BY column that compiled successfully with Rust regex.
pub(super) struct RustRegexInfo {
    pub(super) regex: Regex,
    pub(super) replacement: String,
    /// Byte-op evaluator for restricted prefix-strip patterns; used instead
    /// of `regex` when present. The optional-literal alternation in patterns
    /// like `^https?://(?:www\.)?([^/]+)/.*$` makes them not one-pass, so the
    /// regex crate resolves the capture with its bounded backtracker — ~10x
    /// slower than these byte comparisons.
    pub(super) simple: Option<SimplePattern>,
    pub(super) col_idx: usize,
}

/// One component of a [`SimplePattern`].
enum SimpleComp {
    /// Literal byte run.
    Lit(Vec<u8>),
    /// Optional (greedy) literal byte run: `(?:lit)?` or `x?`.
    OptLit(Vec<u8>),
    /// The single capture group `([^stop]+)`. Parse-time validation
    /// guarantees the next component is a `Lit` starting with `stop`, so the
    /// capture always ends at the first occurrence of `stop` — no
    /// backtracking is ever needed.
    Cap { stop: u8 },
}

/// One part of the replacement template.
enum TemplPart {
    Lit(String),
    /// `\1` — the capture.
    Group1,
    /// `\&` — the whole match (= the whole input: the pattern is fully
    /// anchored with a `.*$` tail, so a match always covers the input).
    Group0,
}

/// A `regexp_replace` pattern of the restricted shape
/// `^ lit [opt-lit|x?]... ([^c]+) c-lit .*$` compiled down to byte
/// comparisons plus a `position()` scan for the capture.
pub(super) struct SimplePattern {
    comps: Vec<SimpleComp>,
    templ: Vec<TemplPart>,
}

impl SimplePattern {
    /// Parse a PG regex `pattern` + `replacement` (PG syntax, i.e. `\1`)
    /// into a SimplePattern. Returns None for anything outside the
    /// restricted shape — callers then use the general regex engine.
    pub(super) fn try_parse(pattern: &str, replacement: &str) -> Option<SimplePattern> {
        let b = pattern.as_bytes();
        if b.first() != Some(&b'^') {
            return None;
        }
        let is_plain = |c: u8| {
            !matches!(
                c,
                b'.' | b'?'
                    | b'*'
                    | b'+'
                    | b'('
                    | b')'
                    | b'['
                    | b']'
                    | b'{'
                    | b'}'
                    | b'|'
                    | b'^'
                    | b'$'
                    | b'\\'
            )
        };
        let mut comps: Vec<SimpleComp> = Vec::new();
        let mut cur: Vec<u8> = Vec::new();
        let mut cap_seen = false;
        let mut tail_seen = false;
        let mut i = 1usize;
        while i < b.len() {
            match b[i] {
                // `.` is only allowed as the closing `.*$` tail
                b'.' => {
                    if &b[i..] != b".*$" {
                        return None;
                    }
                    if !cur.is_empty() {
                        comps.push(SimpleComp::Lit(std::mem::take(&mut cur)));
                    }
                    tail_seen = true;
                    i = b.len();
                }
                b'(' => {
                    if b[i..].starts_with(b"(?:") {
                        // optional literal group `(?:lit)?`
                        let mut j = i + 3;
                        let mut lit: Vec<u8> = Vec::new();
                        loop {
                            match *b.get(j)? {
                                b')' => break,
                                b'\\' => {
                                    let e = *b.get(j + 1)?;
                                    if e.is_ascii_alphanumeric() {
                                        return None; // class escape (\d, \w, ...)
                                    }
                                    lit.push(e);
                                    j += 2;
                                }
                                c if is_plain(c) => {
                                    lit.push(c);
                                    j += 1;
                                }
                                _ => return None,
                            }
                        }
                        if b.get(j + 1) != Some(&b'?') || lit.is_empty() {
                            return None;
                        }
                        if !cur.is_empty() {
                            comps.push(SimpleComp::Lit(std::mem::take(&mut cur)));
                        }
                        comps.push(SimpleComp::OptLit(lit));
                        i = j + 2;
                    } else {
                        // the capture group: exactly `([^X]+)` with ASCII X
                        if cap_seen {
                            return None;
                        }
                        let rest = &b[i..];
                        if !rest.starts_with(b"([^") {
                            return None;
                        }
                        let (stop, after) = if rest.get(3) == Some(&b'\\') {
                            (*rest.get(4)?, 5)
                        } else {
                            (*rest.get(3)?, 4)
                        };
                        if !stop.is_ascii() || stop == b']' {
                            return None;
                        }
                        if rest.get(after..after + 3) != Some(b"]+)".as_slice()) {
                            return None;
                        }
                        if !cur.is_empty() {
                            comps.push(SimpleComp::Lit(std::mem::take(&mut cur)));
                        }
                        comps.push(SimpleComp::Cap { stop });
                        cap_seen = true;
                        i += after + 3;
                    }
                }
                // optional single char: `x?` — pop the last char of the
                // pending literal into an OptLit
                b'?' => {
                    let s = std::str::from_utf8(&cur).ok()?;
                    let last = s.chars().last()?;
                    let cut = cur.len() - last.len_utf8();
                    let tail: Vec<u8> = cur[cut..].to_vec();
                    cur.truncate(cut);
                    if !cur.is_empty() {
                        comps.push(SimpleComp::Lit(std::mem::take(&mut cur)));
                    }
                    comps.push(SimpleComp::OptLit(tail));
                    i += 1;
                }
                b'\\' => {
                    let e = *b.get(i + 1)?;
                    if e.is_ascii_alphanumeric() {
                        return None; // class escape (\d, \w, ...)
                    }
                    cur.push(e);
                    i += 2;
                }
                c if is_plain(c) => {
                    cur.push(c);
                    i += 1;
                }
                _ => return None,
            }
        }
        if !tail_seen || !cap_seen {
            return None;
        }
        // The capture-end heuristic (first occurrence of `stop`) matches the
        // regex semantics only when the component right after the capture is
        // a literal starting with `stop` (as in `([^/]+)/`); otherwise the
        // true capture end would need backtracking — bail.
        let cap_pos = comps
            .iter()
            .position(|c| matches!(c, SimpleComp::Cap { .. }))?;
        let SimpleComp::Cap { stop } = comps[cap_pos] else {
            return None;
        };
        match comps.get(cap_pos + 1) {
            Some(SimpleComp::Lit(l)) if l.first() == Some(&stop) => {}
            _ => return None,
        }
        let templ = Self::parse_replacement(replacement)?;
        Some(SimplePattern { comps, templ })
    }

    /// Parse a PG-syntax replacement (`\1`, `\&`, `\\`, literals).
    fn parse_replacement(replacement: &str) -> Option<Vec<TemplPart>> {
        let mut parts: Vec<TemplPart> = Vec::new();
        let mut lit = String::new();
        let mut chars = replacement.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\\' {
                match chars.next()? {
                    '1' => {
                        if !lit.is_empty() {
                            parts.push(TemplPart::Lit(std::mem::take(&mut lit)));
                        }
                        parts.push(TemplPart::Group1);
                    }
                    '&' => {
                        if !lit.is_empty() {
                            parts.push(TemplPart::Lit(std::mem::take(&mut lit)));
                        }
                        parts.push(TemplPart::Group0);
                    }
                    '\\' => lit.push('\\'),
                    // \2..\9 reference groups the pattern doesn't have
                    d if d.is_ascii_digit() => return None,
                    other => lit.push(other),
                }
            } else {
                lit.push(c);
            }
        }
        if !lit.is_empty() {
            parts.push(TemplPart::Lit(lit));
        }
        Some(parts)
    }

    /// regexp_replace semantics: on match, the (fully anchored) match covers
    /// the whole input and is replaced by the template; on no match the
    /// input is returned unchanged.
    pub(super) fn replace<'a>(&self, s: &'a str) -> std::borrow::Cow<'a, str> {
        use std::borrow::Cow;
        match self.exec(s.as_bytes()) {
            None => Cow::Borrowed(s),
            Some((cs, ce)) => {
                // capture bounds always fall on char boundaries: cs follows
                // literal bytes (whole UTF-8 sequences) and ce is at an ASCII
                // stop byte
                if let [TemplPart::Group1] = self.templ.as_slice() {
                    return Cow::Borrowed(&s[cs..ce]);
                }
                let mut out = String::with_capacity(s.len());
                for p in &self.templ {
                    match p {
                        TemplPart::Lit(l) => out.push_str(l),
                        TemplPart::Group1 => out.push_str(&s[cs..ce]),
                        TemplPart::Group0 => out.push_str(s),
                    }
                }
                Cow::Owned(out)
            }
        }
    }

    /// Match the whole input, returning the capture range on success.
    fn exec(&self, s: &[u8]) -> Option<(usize, usize)> {
        fn rec(
            comps: &[SimpleComp],
            s: &[u8],
            pos: usize,
            cap: Option<(usize, usize)>,
        ) -> Option<(usize, usize)> {
            let Some((first, rest)) = comps.split_first() else {
                // remaining input is consumed by the `.*$` tail
                return cap;
            };
            match first {
                SimpleComp::Lit(l) => {
                    if s[pos..].starts_with(l) {
                        rec(rest, s, pos + l.len(), cap)
                    } else {
                        None
                    }
                }
                SimpleComp::OptLit(l) => {
                    // greedy: prefer consuming the literal, backtrack to
                    // skipping it (mirrors the regex engine's preference
                    // order for `(?:lit)?`)
                    if s[pos..].starts_with(l)
                        && let Some(r) = rec(rest, s, pos + l.len(), cap)
                    {
                        return Some(r);
                    }
                    rec(rest, s, pos, cap)
                }
                SimpleComp::Cap { stop } => {
                    let k = s[pos..].iter().position(|&c| c == *stop)?;
                    if k == 0 {
                        return None; // `+` needs at least one char
                    }
                    rec(rest, s, pos + k, Some((pos, pos + k)))
                }
            }
        }
        rec(&self.comps, s, 0, None)
    }
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

    SegTextColumn::dict_from_owned_entries(entries, row_to_entry)
}

/// Apply a Rust regex replacement to a SegTextColumn, producing a new transformed column.
/// The original column is not modified (needed for aggregations on the same column).
/// For Dict columns, only applies regex to unique dict entries (O(dict_size)).
/// For LZ4 columns, converts to Dict after applying regex.
pub(super) fn apply_regex_to_seg_col(seg_col: &SegTextColumn, ri: &RustRegexInfo) -> SegTextColumn {
    let RustRegexInfo {
        regex,
        replacement,
        simple,
        ..
    } = ri;
    macro_rules! do_replace {
        ($s:expr) => {
            match simple {
                Some(sp) => sp.replace($s),
                None => regex.replace($s, replacement.as_str()),
            }
        };
    }
    match seg_col {
        SegTextColumn::Dict {
            buf,
            entry_ranges,
            row_to_entry,
            ..
        } => {
            // Replace each unique dict entry once, writing results into a
            // fresh flat buffer (no per-entry String allocation).
            let mut new_buf = Vec::with_capacity(buf.len());
            let mut new_ranges = Vec::with_capacity(entry_ranges.len());
            for &r in entry_ranges {
                let replaced = do_replace!(dict_entry_str(buf, r));
                new_ranges.push((new_buf.len() as u32, replaced.len() as u32));
                new_buf.extend_from_slice(replaced.as_bytes());
            }
            SegTextColumn::Dict {
                buf: new_buf,
                entry_ranges: new_ranges,
                row_to_entry: row_to_entry.clone(),
                entry_char_lens: Vec::new(),
            }
        }
        SegTextColumn::Lz4 { buf, row_to_range } => {
            let mut unique_map: HashMap<String, u32> = HashMap::new();
            let mut entries: Vec<String> = Vec::new();
            // Input-side memo: within a segment the same input string repeats
            // ~2-3x (e.g. ClickBench Referer), and a hash probe on the input
            // bytes is an order of magnitude cheaper than re-running the regex.
            let mut input_memo: ahash::AHashMap<&str, u32> = ahash::AHashMap::new();
            let mut new_row_to_entry: Vec<u32> = Vec::with_capacity(row_to_range.len());
            for &(off, len) in row_to_range {
                if off == u32::MAX {
                    new_row_to_entry.push(u32::MAX);
                } else {
                    let slice = &buf[off as usize..off as usize + len as usize];
                    // SAFETY: decompressed PG text — valid UTF-8 by construction
                    // (see SegTextColumn::get_str).
                    debug_assert!(std::str::from_utf8(slice).is_ok());
                    let s = unsafe { std::str::from_utf8_unchecked(slice) };
                    let idx = match input_memo.get(s) {
                        Some(&idx) => idx,
                        None => {
                            // The replace returns a `Cow` (borrowed when nothing
                            // matched). The replaced output is typically low-cardinality
                            // (e.g. a host extracted from many distinct URLs), so look up
                            // by borrowed `&str` and only allocate an owned `String` for
                            // genuinely new outputs — instead of one allocation plus a
                            // clone for the map key on every row.
                            let replaced = do_replace!(s);
                            let idx = match unique_map.get(replaced.as_ref()) {
                                Some(&idx) => idx,
                                None => {
                                    let owned = replaced.into_owned();
                                    let idx = entries.len() as u32;
                                    unique_map.insert(owned.clone(), idx);
                                    entries.push(owned);
                                    idx
                                }
                            };
                            input_memo.insert(s, idx);
                            idx
                        }
                    };
                    new_row_to_entry.push(idx);
                }
            }
            SegTextColumn::dict_from_owned_entries(entries, new_row_to_entry)
        }
        SegTextColumn::SegBy(opt) => {
            let new_opt = opt.as_deref().map(|s| do_replace!(s).into_owned());
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

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use pgrx::prelude::*;

    #[test]
    fn test_has_posix_classes_alpha() {
        assert!(has_posix_classes("[[:alpha:]]"));
    }

    #[test]
    fn test_has_posix_classes_digit() {
        assert!(has_posix_classes("[[:digit:]]"));
    }

    #[test]
    fn test_has_posix_classes_plain_range() {
        assert!(!has_posix_classes("[a-z]"));
    }

    #[test]
    fn test_has_posix_classes_no_brackets() {
        assert!(!has_posix_classes("abc.*def"));
    }

    #[test]
    fn test_convert_pg_replacement_capture_groups() {
        assert_eq!(convert_pg_replacement(r"\1"), "$1");
        assert_eq!(convert_pg_replacement(r"foo\1bar\2"), "foo$1bar$2");
    }

    #[test]
    fn test_convert_pg_replacement_whole_match() {
        assert_eq!(convert_pg_replacement(r"\&"), "$0");
    }

    #[test]
    fn test_convert_pg_replacement_literal_backslash() {
        assert_eq!(convert_pg_replacement(r"\\"), "\\");
    }

    #[test]
    fn test_convert_pg_replacement_no_escapes() {
        assert_eq!(convert_pg_replacement("plain text"), "plain text");
    }

    // `try_compile_rust_regex` reads the `pg_deltax.parallel_regex` GUC
    // (via `crate::get_parallel_regex`), so the tests below need a live
    // PG backend and stay `#[pg_test]`.

    #[pg_test]
    fn test_try_compile_safe_clickbench_pattern() {
        // The ClickBench Q29 pattern
        let re = try_compile_rust_regex(r"^https?://(?:www\.)?([^/]+)/.*");
        assert!(re.is_some());
    }

    #[pg_test]
    fn test_try_compile_posix_class_fallback() {
        let re = try_compile_rust_regex("[[:alpha:]]+");
        assert!(re.is_none());
    }

    #[pg_test]
    fn test_try_compile_backreference_fallback() {
        // Backreferences are not supported by Rust regex
        let re = try_compile_rust_regex(r"(abc)\1");
        assert!(re.is_none());
    }

    #[pg_test]
    fn test_try_compile_lookahead_fallback() {
        let re = try_compile_rust_regex(r"foo(?=bar)");
        assert!(re.is_none());
    }

    #[pg_test]
    fn test_clickbench_regex_replacement() {
        // Use try_compile_rust_regex which applies pg_pattern_to_rust internally
        let re = try_compile_rust_regex(r"^https?://(?:www\.)?([^/]+)/.*$").unwrap();
        let replacement = convert_pg_replacement(r"\1");
        assert_eq!(replacement, "$1");

        let url = "https://www.example.com/path/to/page";
        let result = re.replace(url, replacement.as_str());
        assert_eq!(result, "example.com");

        let url2 = "http://subdomain.test.org/index.html";
        let result2 = re.replace(url2, replacement.as_str());
        assert_eq!(result2, "subdomain.test.org");

        let url3 = "https://bare-domain.io/";
        let result3 = re.replace(url3, replacement.as_str());
        assert_eq!(result3, "bare-domain.io");

        // Trailing newline: PG's .* matches \n, so the whole string matches
        // and the domain is extracted. Our (?s) + \z conversion ensures same behavior.
        let url4 = "http://example.com/path\n";
        let result4 = re.replace(url4, replacement.as_str());
        assert_eq!(result4, "example.com"); // .* consumes \n, \z matches at end
    }

    /// Differential check: SimplePattern must agree with the Rust regex
    /// engine (which itself is verified against PG semantics above) on
    /// every input.
    fn assert_simple_matches_regex(pattern: &str, replacement: &str, inputs: &[&str]) {
        let sp = SimplePattern::try_parse(pattern, replacement)
            .unwrap_or_else(|| panic!("pattern should parse: {pattern}"));
        let re = Regex::new(&pg_pattern_to_rust(pattern)).unwrap();
        let rust_repl = convert_pg_replacement(replacement);
        for input in inputs {
            let expected = re.replace(input, rust_repl.as_str());
            let got = sp.replace(input);
            assert_eq!(
                got, expected,
                "mismatch for pattern {pattern:?} replacement {replacement:?} input {input:?}"
            );
        }
    }

    #[test]
    fn test_simple_pattern_clickbench_q28() {
        assert_simple_matches_regex(
            r"^https?://(?:www\.)?([^/]+)/.*$",
            r"\1",
            &[
                "https://www.example.com/path/to/page",
                "http://example.com/",
                "http://example.com", // no trailing slash — no match
                "https://www./path",  // backtracking: host = "www."
                "https://www.www.x/", // www. stripped once
                "http://www.x/",
                "",
                "ftp://x/y",
                "http:///path",            // empty host — no match
                "https:///",               // empty host — no match
                "http://exämple.com/päth", // multibyte
                "http://x.com/a\nb",       // dot-all tail
                "http://x.com/\n",
                "http://example.com/path\n", // trailing newline
                "HTTPS://X.COM/",            // case-sensitive — no match
                "https//x.com/",             // missing colon — no match
                "http://www.a/b",
                "not a url at all",
                "http://",
            ],
        );
    }

    #[test]
    fn test_simple_pattern_other_shapes() {
        // capture not followed by its stop char in a literal: must not parse
        assert!(SimplePattern::try_parse(r"^([^/]+)x.*$", r"\1").is_none());
        // two captures: must not parse
        assert!(SimplePattern::try_parse(r"^([^/]+)/([^/]+)/.*$", r"\1").is_none());
        // alternation: must not parse
        assert!(SimplePattern::try_parse(r"^(a|b)/.*$", r"\1").is_none());
        // no ^ anchor / no .*$ tail: must not parse
        assert!(SimplePattern::try_parse(r"https?://([^/]+)/.*$", r"\1").is_none());
        assert!(SimplePattern::try_parse(r"^https?://([^/]+)/", r"\1").is_none());
        assert!(SimplePattern::try_parse(r"^https?://([^/]+)/.*", r"\1").is_none());
        // class escapes: must not parse
        assert!(SimplePattern::try_parse(r"^\d+([^/]+)/.*$", r"\1").is_none());
        // backreference to a group we don't have: must not parse
        assert!(SimplePattern::try_parse(r"^a([^/]+)/.*$", r"\2").is_none());
        // unsupported quantifiers: must not parse
        assert!(SimplePattern::try_parse(r"^a+([^/]+)/.*$", r"\1").is_none());
        assert!(SimplePattern::try_parse(r"^a*([^/]+)/.*$", r"\1").is_none());
        assert!(SimplePattern::try_parse(r"^a{2}([^/]+)/.*$", r"\1").is_none());
        // no capture at all: must not parse
        assert!(SimplePattern::try_parse(r"^https?://.*$", r"\1").is_none());

        // escaped stop char in the class + escaped literals
        assert_simple_matches_regex(
            r"^([^\.]+)\..*$",
            r"\1",
            &["foo.bar", "foo", ".bar", "a.b.c", "", "x."],
        );
        // literal replacement parts around the capture + \& whole match
        assert_simple_matches_regex(
            r"^https?://(?:www\.)?([^/]+)/.*$",
            r"host=\1",
            &["https://www.example.com/p", "nope"],
        );
        assert_simple_matches_regex(
            r"^https?://(?:www\.)?([^/]+)/.*$",
            r"\&|\1",
            &["https://www.example.com/p", "nope"],
        );
    }

    /// Property test: SimplePattern must agree with the regex engine on
    /// thousands of randomized inputs, for every accepted pattern shape.
    /// Uses a fixed-seed xorshift PRNG so failures are reproducible.
    #[test]
    fn test_simple_pattern_property_random_inputs() {
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        // Char pool biased toward the pattern alphabets (prefix chars, stop
        // chars, optional-literal chars) so random strings frequently land
        // on near-matches — the interesting boundary cases.
        const POOL: &[char] = &[
            'h', 't', 'p', 's', ':', '/', '/', 'w', 'w', '.', '.', 'a', 'b', 'c', 'd', 'e', 'x',
            '-', '_', '0', '%', '\n', '\\', 'é', '日', 'H',
        ];

        let cases: &[(&str, &str)] = &[
            (r"^https?://(?:www\.)?([^/]+)/.*$", r"\1"),
            (r"^https?://(?:www\.)?([^/]+)/.*$", r"host=\1;"),
            (r"^https?://(?:www\.)?([^/]+)/.*$", r"\&|\1"),
            (r"^([^\.]+)\..*$", r"\1"),
            // two optionals in a row + multi-char literal after the capture
            (r"^a(?:bc)?(?:de)?([^/]+)/x.*$", r"<\1>"),
            // optional single char adjacent to the capture
            (r"^ab?([^.]+)\.c.*$", r"\1"),
        ];

        for (pattern, replacement) in cases {
            let sp = SimplePattern::try_parse(pattern, replacement)
                .unwrap_or_else(|| panic!("pattern should parse: {pattern}"));
            let re = Regex::new(&pg_pattern_to_rust(pattern)).unwrap();
            let rust_repl = convert_pg_replacement(replacement);

            for iter in 0..6000u32 {
                let mut s = String::new();
                if iter % 3 == 0 {
                    // Structured URL-ish input: prefix + optional www. + host
                    // + optional path, each piece independently mutated.
                    let prefixes = [
                        "http://",
                        "https://",
                        "httpss://",
                        "http:/",
                        "HTTP://",
                        "ftp://",
                        "",
                        "http://www.",
                        "https://www",
                        "a",
                        "abc",
                        "abcde",
                    ];
                    s.push_str(prefixes[(next() % prefixes.len() as u64) as usize]);
                    for _ in 0..(next() % 8) {
                        s.push(POOL[(next() % POOL.len() as u64) as usize]);
                    }
                    if next() % 2 == 0 {
                        s.push('/');
                        for _ in 0..(next() % 6) {
                            s.push(POOL[(next() % POOL.len() as u64) as usize]);
                        }
                    }
                } else {
                    // Fully random string from the pool, length 0..32
                    for _ in 0..(next() % 32) {
                        s.push(POOL[(next() % POOL.len() as u64) as usize]);
                    }
                }

                let expected = re.replace(&s, rust_repl.as_str());
                let got = sp.replace(&s);
                assert_eq!(
                    got, expected,
                    "mismatch for pattern {pattern:?} replacement {replacement:?} input {s:?} (iter {iter})"
                );
            }
        }
    }

    #[test]
    fn test_pg_pattern_to_rust_conversions() {
        // (?s) prefix for dot-all mode + $ → \z conversion
        assert_eq!(pg_pattern_to_rust("foo$"), "(?s)foo\\z");
        assert_eq!(pg_pattern_to_rust("foo\\$"), "(?s)foo\\$"); // escaped $ — no \z
        assert_eq!(pg_pattern_to_rust("foo\\\\$"), "(?s)foo\\\\\\z"); // \\$ → $ is unescaped
        assert_eq!(pg_pattern_to_rust("foo"), "(?s)foo"); // no $ — just (?s) prefix
    }

    #[pg_test]
    fn test_rust_regex_dot_matches_newline() {
        // PG's . matches \n by default; our (?s) prefix ensures Rust regex does too
        let re = try_compile_rust_regex("^http://([^/]+)/.*$").unwrap();
        let replacement = convert_pg_replacement(r"\1");
        // URL with embedded \n — PG's .* matches across it
        let url = "http://example.com/path\nmore";
        let result = re.replace(url, replacement.as_str());
        assert_eq!(result, "example.com");
        // URL with embedded \r\n
        let url2 = "http://example.com/path\r\nmore";
        let result2 = re.replace(url2, replacement.as_str());
        assert_eq!(result2, "example.com");
    }
}
