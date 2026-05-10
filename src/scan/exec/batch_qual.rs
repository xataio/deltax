use pgrx::pg_sys;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum BatchCompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    NotLike,
    InList,
}

#[derive(Debug, Clone)]
pub(super) enum LikeStrategy {
    Contains(String),   // %foo%  → str::contains
    StartsWith(String), // foo%   → str::starts_with
    EndsWith(String),   // %foo   → str::ends_with
    Exact(String),      // foo    → ==
    General(String),    // patterns with _, \, or multiple % segments
}

#[derive(Debug, Clone)]
pub(super) struct BatchQual {
    pub(super) col_idx: usize,                      // 0-based column index
    pub(super) op: BatchCompareOp,                  // comparison operator
    pub(super) const_datum: pg_sys::Datum,          // constant value
    pub(super) type_oid: pg_sys::Oid,               // column type OID
    pub(super) like_strategy: Option<LikeStrategy>, // pre-compiled LIKE pattern
    pub(super) text_const: Option<String>,          // text constant for Eq/Ne pushdown
    pub(super) in_list_i64: Option<Vec<i64>>,       // constant values for IN list (stored as i64)
    pub(super) in_list_text: Option<Vec<String>>,   // constant values for text IN list
}

// SAFETY: BatchQual is shared across threads only via immutable references
// during parallel aggregation, and only when all quals reference numeric types
// (verified by batch_quals_all_numeric). For numeric types, const_datum
// contains an integer/float value (not a PG pointer).
unsafe impl Send for BatchQual {}
unsafe impl Sync for BatchQual {}

// ============================================================================
// Batch / vectorized qual evaluation
// ============================================================================

/// Returns true for pass-by-value types that we can compare directly on datums.
pub(super) fn is_batch_comparable_type(type_oid: pg_sys::Oid) -> bool {
    matches!(
        type_oid,
        pg_sys::INT2OID
            | pg_sys::INT4OID
            | pg_sys::INT8OID
            | pg_sys::FLOAT4OID
            | pg_sys::FLOAT8OID
            | pg_sys::BOOLOID
            | pg_sys::DATEOID
            | pg_sys::TIMESTAMPOID
            | pg_sys::TIMESTAMPTZOID
    )
}

pub(super) fn is_text_type(type_oid: pg_sys::Oid) -> bool {
    matches!(
        type_oid,
        pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID
    )
}

/// Flip a comparison operator for `Const op Var` → `Var op Const` rewriting.
pub(super) fn flip_compare_op(op: BatchCompareOp) -> BatchCompareOp {
    match op {
        BatchCompareOp::Eq => BatchCompareOp::Eq,
        BatchCompareOp::Ne => BatchCompareOp::Ne,
        BatchCompareOp::Lt => BatchCompareOp::Gt,
        BatchCompareOp::Le => BatchCompareOp::Ge,
        BatchCompareOp::Gt => BatchCompareOp::Lt,
        BatchCompareOp::Ge => BatchCompareOp::Le,
        BatchCompareOp::Like => BatchCompareOp::Like,
        BatchCompareOp::NotLike => BatchCompareOp::NotLike,
        BatchCompareOp::InList => BatchCompareOp::InList,
    }
}

/// Parse an operator name to a BatchCompareOp.
pub(super) fn parse_compare_op(opname: &str) -> Option<BatchCompareOp> {
    match opname {
        "=" => Some(BatchCompareOp::Eq),
        "<>" | "!=" => Some(BatchCompareOp::Ne),
        "<" => Some(BatchCompareOp::Lt),
        "<=" => Some(BatchCompareOp::Le),
        ">" => Some(BatchCompareOp::Gt),
        ">=" => Some(BatchCompareOp::Ge),
        _ => None,
    }
}

// Monomorphized batch filter functions.  Each ANDs the comparison result
// into the selection vector so that multiple quals compose correctly.

pub(super) fn apply_batch_filter_i64(
    col: &[(pg_sys::Datum, bool)],
    sel: &mut [bool],
    op: BatchCompareOp,
    constant: i64,
) {
    for (i, &(datum, is_null)) in col.iter().enumerate() {
        if !sel[i] {
            continue;
        }
        if is_null {
            sel[i] = false;
            continue;
        }
        let v = datum.value() as i64;
        sel[i] = match op {
            BatchCompareOp::Eq => v == constant,
            BatchCompareOp::Ne => v != constant,
            BatchCompareOp::Lt => v < constant,
            BatchCompareOp::Le => v <= constant,
            BatchCompareOp::Gt => v > constant,
            BatchCompareOp::Ge => v >= constant,
            BatchCompareOp::Like | BatchCompareOp::NotLike | BatchCompareOp::InList => {
                unreachable!()
            }
        };
    }
}

pub(super) fn apply_batch_filter_i32(
    col: &[(pg_sys::Datum, bool)],
    sel: &mut [bool],
    op: BatchCompareOp,
    constant: i32,
) {
    for (i, &(datum, is_null)) in col.iter().enumerate() {
        if !sel[i] {
            continue;
        }
        if is_null {
            sel[i] = false;
            continue;
        }
        let v = datum.value() as i32;
        sel[i] = match op {
            BatchCompareOp::Eq => v == constant,
            BatchCompareOp::Ne => v != constant,
            BatchCompareOp::Lt => v < constant,
            BatchCompareOp::Le => v <= constant,
            BatchCompareOp::Gt => v > constant,
            BatchCompareOp::Ge => v >= constant,
            BatchCompareOp::Like | BatchCompareOp::NotLike | BatchCompareOp::InList => {
                unreachable!()
            }
        };
    }
}

pub(super) fn apply_batch_filter_i16(
    col: &[(pg_sys::Datum, bool)],
    sel: &mut [bool],
    op: BatchCompareOp,
    constant: i16,
) {
    for (i, &(datum, is_null)) in col.iter().enumerate() {
        if !sel[i] {
            continue;
        }
        if is_null {
            sel[i] = false;
            continue;
        }
        let v = datum.value() as i16;
        sel[i] = match op {
            BatchCompareOp::Eq => v == constant,
            BatchCompareOp::Ne => v != constant,
            BatchCompareOp::Lt => v < constant,
            BatchCompareOp::Le => v <= constant,
            BatchCompareOp::Gt => v > constant,
            BatchCompareOp::Ge => v >= constant,
            BatchCompareOp::Like | BatchCompareOp::NotLike | BatchCompareOp::InList => {
                unreachable!()
            }
        };
    }
}

pub(super) fn apply_batch_filter_f64(
    col: &[(pg_sys::Datum, bool)],
    sel: &mut [bool],
    op: BatchCompareOp,
    constant: f64,
) {
    for (i, &(datum, is_null)) in col.iter().enumerate() {
        if !sel[i] {
            continue;
        }
        if is_null {
            sel[i] = false;
            continue;
        }
        let v = f64::from_bits(datum.value() as u64);
        sel[i] = match op {
            BatchCompareOp::Eq => v == constant,
            BatchCompareOp::Ne => v != constant,
            BatchCompareOp::Lt => v < constant,
            BatchCompareOp::Le => v <= constant,
            BatchCompareOp::Gt => v > constant,
            BatchCompareOp::Ge => v >= constant,
            BatchCompareOp::Like | BatchCompareOp::NotLike | BatchCompareOp::InList => {
                unreachable!()
            }
        };
    }
}

pub(super) fn apply_batch_filter_f32(
    col: &[(pg_sys::Datum, bool)],
    sel: &mut [bool],
    op: BatchCompareOp,
    constant: f32,
) {
    for (i, &(datum, is_null)) in col.iter().enumerate() {
        if !sel[i] {
            continue;
        }
        if is_null {
            sel[i] = false;
            continue;
        }
        let v = f32::from_bits(datum.value() as u32);
        sel[i] = match op {
            BatchCompareOp::Eq => v == constant,
            BatchCompareOp::Ne => v != constant,
            BatchCompareOp::Lt => v < constant,
            BatchCompareOp::Le => v <= constant,
            BatchCompareOp::Gt => v > constant,
            BatchCompareOp::Ge => v >= constant,
            BatchCompareOp::Like | BatchCompareOp::NotLike | BatchCompareOp::InList => {
                unreachable!()
            }
        };
    }
}

pub(super) fn apply_batch_filter_bool(
    col: &[(pg_sys::Datum, bool)],
    sel: &mut [bool],
    op: BatchCompareOp,
    constant: bool,
) {
    for (i, &(datum, is_null)) in col.iter().enumerate() {
        if !sel[i] {
            continue;
        }
        if is_null {
            sel[i] = false;
            continue;
        }
        let v = datum.value() != 0;
        sel[i] = match op {
            BatchCompareOp::Eq => v == constant,
            BatchCompareOp::Ne => v != constant,
            BatchCompareOp::Like | BatchCompareOp::NotLike | BatchCompareOp::InList => {
                unreachable!()
            }
            _ => v == constant, // bool only supports = / <>
        };
    }
}

/// Batch filter for IN list: checks if each row's value is in the given list.
/// Values are stored as i64; the actual comparison width is determined by type_oid.
pub(super) fn apply_batch_filter_in_list(
    col: &[(pg_sys::Datum, bool)],
    sel: &mut [bool],
    values: &[i64],
    type_oid: pg_sys::Oid,
) {
    match type_oid {
        pg_sys::INT2OID => {
            let vals: Vec<i16> = values.iter().map(|&v| v as i16).collect();
            for (i, &(datum, is_null)) in col.iter().enumerate() {
                if !sel[i] {
                    continue;
                }
                if is_null {
                    sel[i] = false;
                    continue;
                }
                sel[i] = vals.contains(&(datum.value() as i16));
            }
        }
        pg_sys::INT4OID | pg_sys::DATEOID => {
            let vals: Vec<i32> = values.iter().map(|&v| v as i32).collect();
            for (i, &(datum, is_null)) in col.iter().enumerate() {
                if !sel[i] {
                    continue;
                }
                if is_null {
                    sel[i] = false;
                    continue;
                }
                sel[i] = vals.contains(&(datum.value() as i32));
            }
        }
        pg_sys::INT8OID | pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID => {
            for (i, &(datum, is_null)) in col.iter().enumerate() {
                if !sel[i] {
                    continue;
                }
                if is_null {
                    sel[i] = false;
                    continue;
                }
                sel[i] = values.contains(&(datum.value() as i64));
            }
        }
        _ => {} // unsupported type, skip
    }
}

pub(super) fn compile_like_pattern(pattern: &str) -> LikeStrategy {
    // If pattern contains _ or backslash escape, use general matcher
    if pattern.contains('_') || pattern.contains('\\') {
        return LikeStrategy::General(pattern.to_string());
    }
    // Count % occurrences and their positions
    let percent_positions: Vec<usize> = pattern.match_indices('%').map(|(i, _)| i).collect();
    match percent_positions.len() {
        0 => LikeStrategy::Exact(pattern.to_string()),
        1 => {
            let pos = percent_positions[0];
            if pos == 0 && pattern.len() == 1 {
                // Just "%" — matches everything
                LikeStrategy::Contains(String::new())
            } else if pos == 0 {
                LikeStrategy::EndsWith(pattern[1..].to_string())
            } else if pos == pattern.len() - 1 {
                LikeStrategy::StartsWith(pattern[..pos].to_string())
            } else {
                LikeStrategy::General(pattern.to_string())
            }
        }
        2 => {
            let first = percent_positions[0];
            let second = percent_positions[1];
            if first == 0 && second == pattern.len() - 1 {
                LikeStrategy::Contains(pattern[1..second].to_string())
            } else {
                LikeStrategy::General(pattern.to_string())
            }
        }
        _ => LikeStrategy::General(pattern.to_string()),
    }
}

pub(super) fn sql_like_match(text: &str, pattern: &str) -> bool {
    let t = text.as_bytes();
    let p = pattern.as_bytes();
    sql_like_match_inner(t, p)
}

pub(super) fn sql_like_match_inner(text: &[u8], pattern: &[u8]) -> bool {
    let mut ti = 0;
    let mut pi = 0;
    let mut star_pi = usize::MAX;
    let mut star_ti = 0;

    while ti < text.len() {
        if pi < pattern.len() && pattern[pi] == b'\\' {
            // Escaped character: match literally
            pi += 1;
            if pi < pattern.len() && text[ti] == pattern[pi] {
                ti += 1;
                pi += 1;
                continue;
            }
            // No match after escape
            if star_pi != usize::MAX {
                pi = star_pi + 1;
                star_ti += 1;
                ti = star_ti;
                continue;
            }
            return false;
        }
        if pi < pattern.len() && pattern[pi] == b'_' {
            // _ matches any single character
            ti += 1;
            pi += 1;
            continue;
        }
        if pi < pattern.len() && pattern[pi] == b'%' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
            continue;
        }
        if pi < pattern.len() && text[ti] == pattern[pi] {
            ti += 1;
            pi += 1;
            continue;
        }
        if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
            continue;
        }
        return false;
    }
    // Consume trailing %
    while pi < pattern.len() && pattern[pi] == b'%' {
        pi += 1;
    }
    pi == pattern.len()
}

/// Evaluate all batch quals against the current decompressed segment.
/// Returns a selection vector (one bool per row). Empty vec means "no batch quals".
pub(super) fn evaluate_batch_quals(
    current_segment: &[Vec<(pg_sys::Datum, bool)>],
    row_count: usize,
    batch_quals: &[BatchQual],
    pre_selection: Vec<bool>,
) -> Vec<bool> {
    if batch_quals.is_empty() && pre_selection.is_empty() {
        return Vec::new();
    }

    let mut sel = if pre_selection.is_empty() {
        vec![true; row_count]
    } else {
        pre_selection
    };

    for bq in batch_quals {
        let col = &current_segment[bq.col_idx];
        if col.is_empty() {
            // Column wasn't decompressed (not needed) — can't evaluate, skip
            continue;
        }
        // Handle IN list filter separately
        if bq.op == BatchCompareOp::InList {
            if let Some(ref values) = bq.in_list_i64 {
                apply_batch_filter_in_list(col, &mut sel, values, bq.type_oid);
            }
            continue;
        }
        match bq.type_oid {
            pg_sys::INT8OID | pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID => {
                apply_batch_filter_i64(col, &mut sel, bq.op, bq.const_datum.value() as i64);
            }
            pg_sys::INT4OID | pg_sys::DATEOID => {
                apply_batch_filter_i32(col, &mut sel, bq.op, bq.const_datum.value() as i32);
            }
            pg_sys::INT2OID => {
                apply_batch_filter_i16(col, &mut sel, bq.op, bq.const_datum.value() as i16);
            }
            pg_sys::FLOAT8OID => {
                let c = f64::from_bits(bq.const_datum.value() as u64);
                apply_batch_filter_f64(col, &mut sel, bq.op, c);
            }
            pg_sys::FLOAT4OID => {
                let c = f32::from_bits(bq.const_datum.value() as u32);
                apply_batch_filter_f32(col, &mut sel, bq.op, c);
            }
            pg_sys::BOOLOID => {
                let c = bq.const_datum.value() != 0;
                apply_batch_filter_bool(col, &mut sel, bq.op, c);
            }
            pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID => {
                // Text LIKE/NotLike and Eq/Ne are already handled during Phase 1
                // decompression (decompress_text_blob_with_like_filter /
                // decompress_text_blob_with_eq_filter) and their results are
                // folded into pre_selection. Skip to avoid redundant evaluation.
            }
            _ => {} // unsupported type, skip
        }
    }

    sel
}

/// Extract batch quals from the plan qual list.
///
/// Looks for `OpExpr` nodes with `Var op Const` (or `Const op Var`) where the
/// operator is a simple comparison and the column type is pass-by-value.
///
/// Returns `(batch_quals, handled_count)` where `handled_count` is the number
/// of qual list nodes that were successfully converted to batch quals. When
/// `handled_count == list_length(qual_list)`, all quals are handled by batch
/// evaluation and PG's per-row ExecQual can be skipped.
pub(super) unsafe fn extract_batch_quals(
    qual_list: *mut pg_sys::List,
    col_names: &[String],
    col_types: &[pg_sys::Oid],
) -> (Vec<BatchQual>, usize) {
    let mut batch_quals = Vec::new();

    if qual_list.is_null() {
        return (batch_quals, 0);
    }

    let mut handled_count: usize = 0;
    unsafe {
        let nquals = (*qual_list).length;
        for i in 0..nquals {
            let cell = (*qual_list).elements.add(i as usize);
            let node = (*cell).ptr_value as *const pg_sys::Node;
            if node.is_null() {
                continue;
            }
            let prev_len = batch_quals.len();

            let tag = (*node).type_;

            // Handle bare Var (boolean): PG simplifies `val_bool = true` to just `val_bool`
            if tag == pg_sys::NodeTag::T_Var {
                let var_node = node as *const pg_sys::Var;
                let varattno = (*var_node).varattno as i32;
                if varattno >= 1 && (varattno as usize) <= col_names.len() {
                    let col_idx = (varattno - 1) as usize;
                    if col_types[col_idx] == pg_sys::BOOLOID {
                        batch_quals.push(BatchQual {
                            col_idx,
                            op: BatchCompareOp::Eq,
                            const_datum: pg_sys::Datum::from(1usize), // true
                            type_oid: pg_sys::BOOLOID,
                            like_strategy: None,
                            text_const: None,
                            in_list_i64: None,
                            in_list_text: None,
                        });
                    }
                }
                continue;
            }

            // Handle NOT Var (boolean): PG may emit BoolExpr(NOT, [Var])
            if tag == pg_sys::NodeTag::T_BoolExpr {
                let boolexpr = node as *const pg_sys::BoolExpr;
                if (*boolexpr).boolop == pg_sys::BoolExprType::NOT_EXPR {
                    let args = (*boolexpr).args;
                    if !args.is_null() && (*args).length == 1 {
                        let inner = (*(*args).elements.add(0)).ptr_value as *const pg_sys::Node;
                        if !inner.is_null() && (*inner).type_ == pg_sys::NodeTag::T_Var {
                            let var_node = inner as *const pg_sys::Var;
                            let varattno = (*var_node).varattno as i32;
                            if varattno >= 1 && (varattno as usize) <= col_names.len() {
                                let col_idx = (varattno - 1) as usize;
                                if col_types[col_idx] == pg_sys::BOOLOID {
                                    batch_quals.push(BatchQual {
                                        col_idx,
                                        op: BatchCompareOp::Eq,
                                        const_datum: pg_sys::Datum::from(0usize), // false
                                        type_oid: pg_sys::BOOLOID,
                                        like_strategy: None,
                                        text_const: None,
                                        in_list_i64: None,
                                        in_list_text: None,
                                    });
                                }
                            }
                        }
                    }
                }
                continue;
            }

            // Handle ScalarArrayOpExpr: col = ANY(ARRAY[...]) / col IN (...)
            if tag == pg_sys::NodeTag::T_ScalarArrayOpExpr {
                let saop = node as *const pg_sys::ScalarArrayOpExpr;
                // Only support OR semantics (IN), not AND (ALL)
                if !(*saop).useOr {
                    continue;
                }
                let sa_args = (*saop).args;
                if sa_args.is_null() || (*sa_args).length != 2 {
                    continue;
                }
                let raw_arg0 = (*(*sa_args).elements.add(0)).ptr_value as *const pg_sys::Node;
                let raw_arg1 = (*(*sa_args).elements.add(1)).ptr_value as *const pg_sys::Node;
                if raw_arg0.is_null() || raw_arg1.is_null() {
                    continue;
                }
                // Unwrap RelabelType on the scalar side
                let sa_a0 = if (*raw_arg0).type_ == pg_sys::NodeTag::T_RelabelType {
                    let rlt = raw_arg0 as *const pg_sys::RelabelType;
                    (*rlt).arg as *const pg_sys::Node
                } else {
                    raw_arg0
                };
                // arg0 must be a Var, arg1 must be a Const (array)
                if (*sa_a0).type_ != pg_sys::NodeTag::T_Var {
                    continue;
                }
                if (*raw_arg1).type_ != pg_sys::NodeTag::T_Const {
                    continue;
                }
                let sa_var = sa_a0 as *const pg_sys::Var;
                let sa_const = raw_arg1 as *const pg_sys::Const;
                if (*sa_const).constisnull {
                    continue;
                }
                let sa_varattno = (*sa_var).varattno as i32;
                if sa_varattno < 1 || sa_varattno as usize > col_names.len() {
                    continue;
                }
                let sa_col_idx = (sa_varattno - 1) as usize;
                let sa_type_oid = col_types[sa_col_idx];
                let is_numeric_in = matches!(
                    sa_type_oid,
                    pg_sys::INT2OID
                        | pg_sys::INT4OID
                        | pg_sys::INT8OID
                        | pg_sys::DATEOID
                        | pg_sys::TIMESTAMPOID
                        | pg_sys::TIMESTAMPTZOID
                );
                let is_text_in = matches!(
                    sa_type_oid,
                    pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID
                );
                if !is_numeric_in && !is_text_in {
                    continue;
                }
                // Deconstruct the array constant to extract element values
                let array_datum = (*sa_const).constvalue;
                let array_ptr = array_datum.cast_mut_ptr::<pg_sys::ArrayType>();
                let ndim = (*array_ptr).ndim;
                if ndim != 1 {
                    continue; // only 1-D arrays
                }
                let mut sa_elems: *mut pg_sys::Datum = std::ptr::null_mut();
                let mut sa_nulls: *mut bool = std::ptr::null_mut();
                let mut sa_nelems: i32 = 0;
                // Get element type info for deconstruct_array
                let sa_elem_type = (*array_ptr).elemtype;
                let mut sa_elem_len: i16 = 0;
                let mut sa_elem_byval: bool = false;
                let mut sa_elem_align: i8 = 0;
                pg_sys::get_typlenbyvalalign(
                    sa_elem_type,
                    &mut sa_elem_len as *mut i16 as *mut _,
                    &mut sa_elem_byval,
                    &mut sa_elem_align as *mut i8 as *mut _,
                );
                pg_sys::deconstruct_array(
                    array_ptr,
                    sa_elem_type,
                    sa_elem_len as _,
                    sa_elem_byval,
                    sa_elem_align as _,
                    &mut sa_elems,
                    &mut sa_nulls,
                    &mut sa_nelems,
                );
                if sa_nelems <= 0 || sa_elems.is_null() {
                    continue;
                }
                // Collect values: i64 for numeric types, owned String for text.
                let mut sa_has_null = false;
                let mut in_values_i64: Vec<i64> = Vec::new();
                let mut in_values_text: Vec<String> = Vec::new();
                for ei in 0..sa_nelems as usize {
                    if !sa_nulls.is_null() && *sa_nulls.add(ei) {
                        sa_has_null = true;
                        break;
                    }
                    let datum = *sa_elems.add(ei);
                    if is_text_in {
                        let varlena_ptr = datum.cast_mut_ptr::<pg_sys::varlena>();
                        let len = pgrx::varsize_any_exhdr(varlena_ptr);
                        let data = pgrx::vardata_any(varlena_ptr);
                        #[allow(clippy::unnecessary_cast)]
                        let bytes = std::slice::from_raw_parts(data as *const u8, len);
                        match std::str::from_utf8(bytes) {
                            Ok(s) => in_values_text.push(s.to_string()),
                            Err(_) => {
                                sa_has_null = true; // bail rather than guess
                                break;
                            }
                        }
                    } else {
                        in_values_i64.push(datum.value() as i64);
                    }
                }
                if sa_has_null {
                    continue;
                }
                batch_quals.push(BatchQual {
                    col_idx: sa_col_idx,
                    op: BatchCompareOp::InList,
                    const_datum: pg_sys::Datum::from(0usize), // unused
                    type_oid: sa_type_oid,
                    like_strategy: None,
                    text_const: None,
                    in_list_i64: if is_numeric_in { Some(in_values_i64) } else { None },
                    in_list_text: if is_text_in { Some(in_values_text) } else { None },
                });
                continue;
            }

            if tag != pg_sys::NodeTag::T_OpExpr {
                continue;
            }

            let opexpr = node as *const pg_sys::OpExpr;
            let args = (*opexpr).args;
            if args.is_null() || (*args).length != 2 {
                continue;
            }

            // Get operator name
            let opname_ptr = pg_sys::get_opname((*opexpr).opno);
            if opname_ptr.is_null() {
                continue;
            }
            let opname = std::ffi::CStr::from_ptr(opname_ptr).to_str().unwrap_or("");

            // Recognize LIKE/NOT LIKE operators before comparison ops
            let is_like = opname == "~~";
            let is_not_like = opname == "!~~";

            let cmp_op = if is_like {
                BatchCompareOp::Like
            } else if is_not_like {
                BatchCompareOp::NotLike
            } else {
                match parse_compare_op(opname) {
                    Some(op) => op,
                    None => {
                        continue;
                    }
                }
            };

            let raw_arg0 = (*(*args).elements.add(0)).ptr_value as *const pg_sys::Node;
            let raw_arg1 = (*(*args).elements.add(1)).ptr_value as *const pg_sys::Node;
            if raw_arg0.is_null() || raw_arg1.is_null() {
                continue;
            }

            // Unwrap RelabelType (PG adds these for int2→int4 coercions etc.)
            let unwrap_relabel = |n: *const pg_sys::Node| -> *const pg_sys::Node {
                if (*n).type_ == pg_sys::NodeTag::T_RelabelType {
                    let rlt = n as *const pg_sys::RelabelType;
                    (*rlt).arg as *const pg_sys::Node
                } else {
                    n
                }
            };
            let arg0 = unwrap_relabel(raw_arg0);
            let arg1 = unwrap_relabel(raw_arg1);

            let arg0_tag = (*arg0).type_;
            let arg1_tag = (*arg1).type_;

            let (var_node, const_node, var_on_left) = if arg0_tag == pg_sys::NodeTag::T_Var
                && arg1_tag == pg_sys::NodeTag::T_Const
            {
                (
                    arg0 as *const pg_sys::Var,
                    arg1 as *const pg_sys::Const,
                    true,
                )
            } else if arg0_tag == pg_sys::NodeTag::T_Const && arg1_tag == pg_sys::NodeTag::T_Var {
                (
                    arg1 as *const pg_sys::Var,
                    arg0 as *const pg_sys::Const,
                    false,
                )
            } else {
                continue;
            };

            if (*const_node).constisnull {
                continue;
            }

            let varattno = (*var_node).varattno as i32;
            if varattno < 1 || varattno as usize > col_names.len() {
                continue;
            }
            let col_idx = (varattno - 1) as usize;
            let type_oid = col_types[col_idx];

            if is_like || is_not_like {
                // LIKE is not symmetric: column must be on the left
                if !var_on_left {
                    continue;
                }
                // Only text-like types
                if !matches!(
                    type_oid,
                    pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID
                ) {
                    continue;
                }
                // Extract pattern string from constant datum
                let varlena_ptr = (*const_node).constvalue.cast_mut_ptr::<pg_sys::varlena>();
                let len = pgrx::varsize_any_exhdr(varlena_ptr);
                let data = pgrx::vardata_any(varlena_ptr);
                // Cast needed: vardata_any returns *const i8 on Linux, *const u8 on macOS
                #[allow(clippy::unnecessary_cast)]
                let pattern_bytes = std::slice::from_raw_parts(data as *const u8, len);
                let pattern = match std::str::from_utf8(pattern_bytes) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let strategy = compile_like_pattern(pattern);
                batch_quals.push(BatchQual {
                    col_idx,
                    op: cmp_op,
                    const_datum: (*const_node).constvalue,
                    type_oid,
                    like_strategy: Some(strategy),
                    text_const: None,
                    in_list_i64: None,
                    in_list_text: None,
                });
            } else if matches!(type_oid, pg_sys::TEXTOID | pg_sys::VARCHAROID)
                && matches!(cmp_op, BatchCompareOp::Eq | BatchCompareOp::Ne)
            {
                // Text equality/inequality: extract the constant string for
                // dictionary-based pushdown during decompression.
                if !var_on_left {
                    continue;
                }
                let varlena_ptr = (*const_node).constvalue.cast_mut_ptr::<pg_sys::varlena>();
                let len = pgrx::varsize_any_exhdr(varlena_ptr);
                let data = pgrx::vardata_any(varlena_ptr);
                #[allow(clippy::unnecessary_cast)]
                let const_bytes = std::slice::from_raw_parts(data as *const u8, len);
                let const_str = match std::str::from_utf8(const_bytes) {
                    Ok(s) => s.to_string(),
                    Err(_) => continue,
                };

                batch_quals.push(BatchQual {
                    col_idx,
                    op: cmp_op,
                    const_datum: (*const_node).constvalue,
                    type_oid,
                    like_strategy: None,
                    text_const: Some(const_str),
                    in_list_i64: None,
                    in_list_text: None,
                });
            } else {
                if !is_batch_comparable_type(type_oid) {
                    continue;
                }

                let op = if var_on_left {
                    cmp_op
                } else {
                    flip_compare_op(cmp_op)
                };

                batch_quals.push(BatchQual {
                    col_idx,
                    op,
                    const_datum: (*const_node).constvalue,
                    type_oid,
                    like_strategy: None,
                    text_const: None,
                    in_list_i64: None,
                    in_list_text: None,
                });
            }

            // Track whether this qual node was handled
            if batch_quals.len() > prev_len {
                handled_count += 1;
            }
        }
    }

    (batch_quals, handled_count)
}
