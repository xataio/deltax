//! Plan-time recognition and rewriting of JSONB-extract chains
//! (e.g. `data->'commit'->>'collection'`) so the scan node can serve them
//! from extracted columnar columns instead of decompressing JSONB and
//! re-evaluating `->`/`->>` per row.
//!
//! The mechanism: when a partition has `json_extract` configuration, we
//! publish synthetic `TargetEntry`s in `CustomScan->custom_scan_tlist` whose
//! `expr` is the original chain (`((data->'commit')->>'collection')` etc.).
//! PG's `setrefs.c` handles upper-plan reference fixup automatically:
//! `search_indexed_tlist_for_non_var` matches the chain via `equal()` against
//! our tlist and replaces upper-plan occurrences with `Var(INDEX_VAR, attno)`.
//! For the scan node's own quals (`baserestrictinfo`) we walk and substitute
//! manually since those bypass setrefs's tlist matching. The executor
//! (`scan/exec/decompress.rs`) materializes the extracted columns into the
//! slot at positions [physical_natts .. physical_natts + M).

use std::collections::HashMap;

use pgrx::pg_sys;

use crate::compress::{ColumnKind, ExtractSpec};

/// Operator OIDs from `pg_operator.dat`. The array-element variants
/// (`->` / `->>` with int4 right operand) are not matched in v1 — paths
/// only support string keys.
pub(crate) const JSONB_OBJECT_FIELD_OPNO: pg_sys::Oid = pg_sys::Oid::from_u32(3211);
pub(crate) const JSONB_OBJECT_FIELD_TEXT_OPNO: pg_sys::Oid = pg_sys::Oid::from_u32(3477);

/// Map our `ColumnKind` (the configured target type) to a PG type OID, used
/// when we synthesize a `Var(INDEX_VAR, ...)` to replace a matched chain.
pub(crate) fn kind_to_type_oid(kind: ColumnKind) -> pg_sys::Oid {
    match kind {
        ColumnKind::Text => pg_sys::TEXTOID,
        ColumnKind::Int16 => pg_sys::INT2OID,
        ColumnKind::Int32 => pg_sys::INT4OID,
        ColumnKind::Int64 => pg_sys::INT8OID,
        ColumnKind::Float32 => pg_sys::FLOAT4OID,
        ColumnKind::Float64 => pg_sys::FLOAT8OID,
        ColumnKind::Bool => pg_sys::BOOLOID,
        ColumnKind::Timestamp => pg_sys::TIMESTAMPOID,
        ColumnKind::TimestampTz => pg_sys::TIMESTAMPTZOID,
        ColumnKind::Date => pg_sys::DATEOID,
        ColumnKind::Jsonb => pg_sys::JSONBOID,
    }
}

/// Lookup table: physical column name -> (attno, pg type OID). Built once
/// per rel and consulted by the chain matcher to identify the source jsonb
/// column from a Var's `varattno`.
pub(crate) struct PhysicalCols {
    pub(crate) by_attno: HashMap<i16, String>,
}

impl PhysicalCols {
    #[allow(dead_code)] // Wired up in step 4 follow-up.
    pub(crate) unsafe fn from_rel_oid(rel_oid: pg_sys::Oid) -> Self {
        let mut by_attno: HashMap<i16, String> = HashMap::new();
        unsafe {
            let rel = pg_sys::relation_open(rel_oid, pg_sys::AccessShareLock as i32);
            let tupdesc = (*rel).rd_att;
            for i in 0..(*tupdesc).natts {
                let att = crate::scan::exec::datum_utils::tupdesc_get_attr(tupdesc, i as usize);
                if (*att).attisdropped {
                    continue;
                }
                let name_cstr = std::ffi::CStr::from_ptr((*att).attname.data.as_ptr());
                if let Ok(name) = name_cstr.to_str() {
                    by_attno.insert((*att).attnum, name.to_string());
                }
            }
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
        }
        PhysicalCols { by_attno }
    }
}

/// Try to match `node` as a JSONB extract chain rooted at `Var(varno=rti)`,
/// against any of `specs`. Returns the spec index if matched.
///
/// Recognized shapes:
///   - `var ->> 'k'`                         (text result)
///   - `(var -> 'k1') ->> 'kN'`              (text result, depth ≥ 2)
///   - `((var ->> 'k')::TYPE)`               (cast wraps via CoerceViaIO)
///
/// Everything else (array indexing, jsonb_path_query, complex expressions)
/// falls through and the chain stays as-is in the plan.
pub(crate) unsafe fn match_extract_chain(
    node: *mut pg_sys::Node,
    rti: pg_sys::Index,
    specs: &[ExtractSpec],
    phys: &PhysicalCols,
) -> Option<usize> {
    unsafe {
        if node.is_null() {
            return None;
        }

        // Strip an outer cast to a non-text type (CoerceViaIO is what PG emits
        // for `(text)::int8` / `::int4` / `::float8` / etc.). RelabelType
        // covers binary-compatible casts (less common here but cheap to peel).
        let (inner, leaf_kind) = strip_outer_cast(node);

        // Inner must be the chain itself, ending in `->>` (text).
        let mut path_keys: Vec<String> = Vec::new();
        let mut cursor = inner;

        // Outermost step: ->> with a string Const right operand.
        let (k, deeper) = match_op_step(cursor, JSONB_OBJECT_FIELD_TEXT_OPNO)?;
        path_keys.push(k);
        cursor = deeper;

        // Subsequent inner steps must be -> with string keys, until we hit a Var.
        loop {
            // Allow RelabelType wrappers between op steps (rare).
            cursor = unwrap_relabel(cursor);
            match match_op_step(cursor, JSONB_OBJECT_FIELD_OPNO) {
                Some((k, deeper)) => {
                    path_keys.push(k);
                    cursor = deeper;
                }
                None => break,
            }
        }

        // Innermost should be a Var(varno=rti).
        cursor = unwrap_relabel(cursor);
        if (*cursor).type_ != pg_sys::NodeTag::T_Var {
            return None;
        }
        let var = cursor as *mut pg_sys::Var;
        if (*var).varno != rti as i32 {
            return None;
        }
        let var_attno = (*var).varattno;
        let src_col_name = phys.by_attno.get(&var_attno)?;

        // Path was accumulated outermost-first while spec.path is innermost-first;
        // reverse to match.
        path_keys.reverse();

        for (i, spec) in specs.iter().enumerate() {
            if &spec.src_column == src_col_name
                && spec.path == path_keys
                && spec.target_kind == leaf_kind
            {
                return Some(i);
            }
        }
        None
    }
}

/// Peel `CoerceViaIO` / `RelabelType` wrappers and return the inner Node and
/// the effective `ColumnKind` of the outer expression. The naked chain (no
/// cast) returns `ColumnKind::Text` (the type of `->>`).
unsafe fn strip_outer_cast(node: *mut pg_sys::Node) -> (*mut pg_sys::Node, ColumnKind) {
    unsafe {
        let mut cur = node;
        let mut leaf = ColumnKind::Text;
        loop {
            if cur.is_null() {
                return (cur, leaf);
            }
            match (*cur).type_ {
                pg_sys::NodeTag::T_CoerceViaIO => {
                    let c = cur as *mut pg_sys::CoerceViaIO;
                    leaf = type_oid_to_kind((*c).resulttype).unwrap_or(leaf);
                    cur = (*c).arg as *mut pg_sys::Node;
                }
                pg_sys::NodeTag::T_RelabelType => {
                    let r = cur as *mut pg_sys::RelabelType;
                    leaf = type_oid_to_kind((*r).resulttype).unwrap_or(leaf);
                    cur = (*r).arg as *mut pg_sys::Node;
                }
                _ => return (cur, leaf),
            }
        }
    }
}

/// Match `node` as `OpExpr(opno=expected_opno, args=[inner, Const(text key)])`.
/// Returns `(key, inner)` on match.
unsafe fn match_op_step(
    node: *mut pg_sys::Node,
    expected_opno: pg_sys::Oid,
) -> Option<(String, *mut pg_sys::Node)> {
    unsafe {
        if node.is_null() || (*node).type_ != pg_sys::NodeTag::T_OpExpr {
            return None;
        }
        let op = node as *mut pg_sys::OpExpr;
        if (*op).opno != expected_opno {
            return None;
        }
        let args = (*op).args;
        if args.is_null() || (*args).length != 2 {
            return None;
        }
        let a0 = pg_sys::list_nth(args, 0) as *mut pg_sys::Node;
        let a1 = pg_sys::list_nth(args, 1) as *mut pg_sys::Node;
        if a0.is_null() || a1.is_null() {
            return None;
        }
        let inner = unwrap_relabel(a0);
        if (*a1).type_ != pg_sys::NodeTag::T_Const {
            return None;
        }
        let c = a1 as *mut pg_sys::Const;
        if (*c).consttype != pg_sys::TEXTOID || (*c).constisnull {
            return None;
        }
        let key = pgrx_text_const_to_string(c)?;
        Some((key, inner))
    }
}

/// Convert a Const(TEXT) datum to a Rust String.
unsafe fn pgrx_text_const_to_string(c: *mut pg_sys::Const) -> Option<String> {
    unsafe {
        let datum = (*c).constvalue;
        let varlena = datum.cast_mut_ptr::<pg_sys::varlena>();
        if varlena.is_null() {
            return None;
        }
        let detoasted = pg_sys::pg_detoast_datum(varlena);
        let len = pgrx::varsize_any_exhdr(detoasted);
        let data = pgrx::vardata_any(detoasted);
        let bytes = std::slice::from_raw_parts(data, len);
        std::str::from_utf8(bytes).ok().map(|s| s.to_string())
    }
}

unsafe fn unwrap_relabel(n: *mut pg_sys::Node) -> *mut pg_sys::Node {
    unsafe {
        if !n.is_null() && (*n).type_ == pg_sys::NodeTag::T_RelabelType {
            (*(n as *mut pg_sys::RelabelType)).arg as *mut pg_sys::Node
        } else {
            n
        }
    }
}

/// Reverse mapping of `kind_to_type_oid`.
fn type_oid_to_kind(oid: pg_sys::Oid) -> Option<ColumnKind> {
    match oid {
        pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID => Some(ColumnKind::Text),
        pg_sys::INT2OID => Some(ColumnKind::Int16),
        pg_sys::INT4OID => Some(ColumnKind::Int32),
        pg_sys::INT8OID => Some(ColumnKind::Int64),
        pg_sys::FLOAT4OID => Some(ColumnKind::Float32),
        pg_sys::FLOAT8OID => Some(ColumnKind::Float64),
        pg_sys::BOOLOID => Some(ColumnKind::Bool),
        pg_sys::TIMESTAMPOID => Some(ColumnKind::Timestamp),
        pg_sys::TIMESTAMPTZOID => Some(ColumnKind::TimestampTz),
        pg_sys::DATEOID => Some(ColumnKind::Date),
        pg_sys::JSONBOID => Some(ColumnKind::Jsonb),
        _ => None,
    }
}

/// Recursively walk `node`, replacing every matched chain with
/// `Var(varno=INDEX_VAR, varattno=physical_natts + spec_idx + 1, vartype=...)`.
/// Returns the rewritten node; `node` itself is mutated in place where feasible
/// (PG nodes are heap-allocated, so callers should reassign the returned ptr).
#[allow(dead_code)]
pub(crate) unsafe fn rewrite_chains_in_node(
    node: *mut pg_sys::Node,
    rti: pg_sys::Index,
    specs: &[ExtractSpec],
    phys: &PhysicalCols,
    physical_natts: i16,
) -> *mut pg_sys::Node {
    unsafe {
        rewrite_walker(node, rti, specs, phys, physical_natts)
    }
}

/// Same as `rewrite_chains_in_node` but for a list of expressions; returns a
/// new list with each element mapped through the rewriter.
pub(crate) unsafe fn rewrite_chains_in_list(
    list: *mut pg_sys::List,
    rti: pg_sys::Index,
    specs: &[ExtractSpec],
    phys: &PhysicalCols,
    physical_natts: i16,
) -> *mut pg_sys::List {
    unsafe {
        if list.is_null() {
            return list;
        }
        let mut out: *mut pg_sys::List = std::ptr::null_mut();
        for i in 0..(*list).length {
            let e = pg_sys::list_nth(list, i) as *mut pg_sys::Node;
            let rewritten = rewrite_walker(e, rti, specs, phys, physical_natts);
            out = pg_sys::lappend(out, rewritten as *mut _);
        }
        out
    }
}

/// Rewrite each `RestrictInfo->clause` in-place. Returns the same list
/// (RestrictInfos are reused; only their clause pointer is swapped).
#[allow(dead_code)]
pub(crate) unsafe fn rewrite_chains_in_restrictinfo_list(
    list: *mut pg_sys::List,
    rti: pg_sys::Index,
    specs: &[ExtractSpec],
    phys: &PhysicalCols,
    physical_natts: i16,
) {
    unsafe {
        if list.is_null() {
            return;
        }
        for i in 0..(*list).length {
            let ri = pg_sys::list_nth(list, i) as *mut pg_sys::RestrictInfo;
            if ri.is_null() || (*ri).clause.is_null() {
                continue;
            }
            let clause = (*ri).clause as *mut pg_sys::Node;
            let rewritten = rewrite_walker(clause, rti, specs, phys, physical_natts);
            (*ri).clause = rewritten as *mut pg_sys::Expr;
        }
    }
}

/// Manual recursive walker. We don't use `expression_tree_mutator` because
/// it requires registering a C function pointer; the cases we care about
/// (OpExpr / BoolExpr / cast wrappers / our chains) are few enough that
/// hand-rolling is simpler.
unsafe fn rewrite_walker(
    node: *mut pg_sys::Node,
    rti: pg_sys::Index,
    specs: &[ExtractSpec],
    phys: &PhysicalCols,
    physical_natts: i16,
) -> *mut pg_sys::Node {
    unsafe {
        if node.is_null() {
            return node;
        }

        // First, try to match the WHOLE node (including any outer cast) as
        // an extract chain. If yes, replace with a Var(INDEX_VAR).
        if let Some(spec_idx) = match_extract_chain(node, rti, specs, phys) {
            let spec = &specs[spec_idx];
            let var = pg_sys::makeVar(
                pg_sys::INDEX_VAR,
                physical_natts + spec_idx as i16 + 1,
                kind_to_type_oid(spec.target_kind),
                -1,                  // typmod
                pg_sys::InvalidOid,  // collation
                0,                   // varlevelsup
            );
            return var as *mut pg_sys::Node;
        }

        // Not a chain — recurse into structural children.
        match (*node).type_ {
            pg_sys::NodeTag::T_OpExpr => {
                let op = node as *mut pg_sys::OpExpr;
                (*op).args = rewrite_chains_in_list((*op).args, rti, specs, phys, physical_natts);
            }
            pg_sys::NodeTag::T_BoolExpr => {
                let b = node as *mut pg_sys::BoolExpr;
                (*b).args = rewrite_chains_in_list((*b).args, rti, specs, phys, physical_natts);
            }
            pg_sys::NodeTag::T_FuncExpr => {
                let f = node as *mut pg_sys::FuncExpr;
                (*f).args = rewrite_chains_in_list((*f).args, rti, specs, phys, physical_natts);
            }
            pg_sys::NodeTag::T_CoerceViaIO => {
                let c = node as *mut pg_sys::CoerceViaIO;
                (*c).arg =
                    rewrite_walker((*c).arg as *mut pg_sys::Node, rti, specs, phys, physical_natts)
                        as *mut pg_sys::Expr;
            }
            pg_sys::NodeTag::T_RelabelType => {
                let r = node as *mut pg_sys::RelabelType;
                (*r).arg =
                    rewrite_walker((*r).arg as *mut pg_sys::Node, rti, specs, phys, physical_natts)
                        as *mut pg_sys::Expr;
            }
            pg_sys::NodeTag::T_NullTest => {
                let n = node as *mut pg_sys::NullTest;
                (*n).arg = rewrite_walker(
                    (*n).arg as *mut pg_sys::Node,
                    rti,
                    specs,
                    phys,
                    physical_natts,
                ) as *mut pg_sys::Expr;
            }
            pg_sys::NodeTag::T_CaseExpr => {
                let c = node as *mut pg_sys::CaseExpr;
                (*c).args = rewrite_chains_in_list((*c).args, rti, specs, phys, physical_natts);
                if !(*c).defresult.is_null() {
                    (*c).defresult = rewrite_walker(
                        (*c).defresult as *mut pg_sys::Node,
                        rti,
                        specs,
                        phys,
                        physical_natts,
                    ) as *mut pg_sys::Expr;
                }
            }
            pg_sys::NodeTag::T_CaseWhen => {
                let c = node as *mut pg_sys::CaseWhen;
                (*c).expr = rewrite_walker(
                    (*c).expr as *mut pg_sys::Node,
                    rti,
                    specs,
                    phys,
                    physical_natts,
                ) as *mut pg_sys::Expr;
                (*c).result = rewrite_walker(
                    (*c).result as *mut pg_sys::Node,
                    rti,
                    specs,
                    phys,
                    physical_natts,
                ) as *mut pg_sys::Expr;
            }
            pg_sys::NodeTag::T_Aggref => {
                let a = node as *mut pg_sys::Aggref;
                (*a).args = rewrite_chains_in_list((*a).args, rti, specs, phys, physical_natts);
            }
            // Vars, Consts, Params, etc. are leaves.
            _ => {}
        }

        node
    }
}

/// Build the `custom_scan_tlist` for a json-extract-enabled DeltaXDecompress.
#[allow(dead_code)] // Activated by the upper-plan rewrite follow-up.
/// Layout:
///   resno = 1..physical_natts:    Var(rti, attno=k, vartype=physical type)
///   resno = physical_natts+1..M:  the original chain Expr (kept verbatim so
///                                  `setrefs` matches it via tlist_member).
///
/// The chain Expr is built as `(... ((Var->'k1')->'k2') ->> 'kN')` plus an
/// optional `CoerceViaIO` if the target type isn't text.
pub(crate) unsafe fn build_custom_scan_tlist(
    rti: pg_sys::Index,
    rel_oid: pg_sys::Oid,
    specs: &[ExtractSpec],
) -> *mut pg_sys::List {
    unsafe {
        let mut tlist: *mut pg_sys::List = std::ptr::null_mut();
        let rel = pg_sys::relation_open(rel_oid, pg_sys::AccessShareLock as i32);
        let tupdesc = (*rel).rd_att;
        let physical_natts = (*tupdesc).natts;

        // Physical columns first.
        for i in 0..physical_natts {
            let att = crate::scan::exec::datum_utils::tupdesc_get_attr(tupdesc, i as usize);
            if (*att).attisdropped {
                // Emit a Const NULL placeholder so attno alignment is preserved.
                let null_const = pg_sys::makeNullConst(pg_sys::INT4OID, -1, pg_sys::InvalidOid);
                let tle = pg_sys::makeTargetEntry(
                    null_const as *mut pg_sys::Expr,
                    (i + 1) as i16,
                    std::ptr::null_mut(),
                    false,
                );
                tlist = pg_sys::lappend(tlist, tle as *mut _);
                continue;
            }
            let var = pg_sys::makeVar(
                rti as i32,
                (*att).attnum,
                (*att).atttypid,
                (*att).atttypmod,
                (*att).attcollation,
                0,
            );
            // resname is informational; copy from pg_attribute for nicer EXPLAIN.
            let resname = pg_sys::pstrdup((*att).attname.data.as_ptr());
            let tle = pg_sys::makeTargetEntry(
                var as *mut pg_sys::Expr,
                (i + 1) as i16,
                resname,
                false,
            );
            tlist = pg_sys::lappend(tlist, tle as *mut _);
        }

        // Synthetic extracted columns. Build the chain Expr from the spec.
        for (i, spec) in specs.iter().enumerate() {
            let chain = build_chain_expr_for_spec(rti, rel_oid, spec, tupdesc);
            if chain.is_null() {
                // Unknown src column or build failure — skip; the recognizer
                // will simply not match this spec, and queries fall through.
                continue;
            }
            let resno: i16 = (physical_natts + i as i32 + 1) as i16;
            let resname = std::ffi::CString::new(spec.target_name.as_str())
                .map(|s| pg_sys::pstrdup(s.as_ptr()))
                .unwrap_or(std::ptr::null_mut());
            let tle = pg_sys::makeTargetEntry(chain as *mut pg_sys::Expr, resno, resname, false);
            tlist = pg_sys::lappend(tlist, tle as *mut _);
        }

        pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
        tlist
    }
}

/// Construct the Expr `( ... ((Var(rti, src_attno) -> 'k1') -> 'k2') ->> 'kN' )`,
/// optionally wrapped in `CoerceViaIO(target_kind)` when the target isn't text.
#[allow(dead_code)] // Activated by the upper-plan rewrite follow-up.
unsafe fn build_chain_expr_for_spec(
    rti: pg_sys::Index,
    _rel_oid: pg_sys::Oid,
    spec: &ExtractSpec,
    tupdesc: pg_sys::TupleDesc,
) -> *mut pg_sys::Node {
    unsafe {
        // Find src_attno by name.
        let mut src_attno: i16 = 0;
        let mut src_typid: pg_sys::Oid = pg_sys::InvalidOid;
        let mut src_collation: pg_sys::Oid = pg_sys::InvalidOid;
        for i in 0..(*tupdesc).natts {
            let att = crate::scan::exec::datum_utils::tupdesc_get_attr(tupdesc, i as usize);
            if (*att).attisdropped {
                continue;
            }
            let name = std::ffi::CStr::from_ptr((*att).attname.data.as_ptr())
                .to_str()
                .unwrap_or("");
            if name == spec.src_column {
                src_attno = (*att).attnum;
                src_typid = (*att).atttypid;
                src_collation = (*att).attcollation;
                break;
            }
        }
        if src_attno == 0 || src_typid != pg_sys::JSONBOID {
            return std::ptr::null_mut();
        }

        // Build the root Var.
        let mut node: *mut pg_sys::Node =
            pg_sys::makeVar(rti as i32, src_attno, src_typid, -1, src_collation, 0)
                as *mut pg_sys::Node;

        // Apply each path step. Last step is `->>` (text result), earlier
        // steps are `->` (jsonb). Match PG's parser exactly: opno from
        // pg_operator.dat, opfuncid from pg_proc.dat, opcollid is 0 for the
        // jsonb-returning `->` and DEFAULT_COLLATION_OID for the text-returning
        // `->>`. inputcollid is DEFAULT_COLLATION_OID throughout (the right
        // operand is text).
        let last_idx = spec.path.len() - 1;
        for (i, key) in spec.path.iter().enumerate() {
            let key_const = make_text_const(key);
            let (opno, opfuncid, result_oid, opcollid) = if i == last_idx {
                (
                    JSONB_OBJECT_FIELD_TEXT_OPNO,
                    pg_sys::Oid::from_u32(3214), // jsonb_object_field_text
                    pg_sys::TEXTOID,
                    pg_sys::DEFAULT_COLLATION_OID,
                )
            } else {
                (
                    JSONB_OBJECT_FIELD_OPNO,
                    pg_sys::Oid::from_u32(3478), // jsonb_object_field
                    pg_sys::JSONBOID,
                    pg_sys::InvalidOid,
                )
            };
            let mut args: *mut pg_sys::List = std::ptr::null_mut();
            args = pg_sys::lappend(args, node as *mut _);
            args = pg_sys::lappend(args, key_const as *mut _);
            let op = pg_sys::palloc0(std::mem::size_of::<pg_sys::OpExpr>()) as *mut pg_sys::OpExpr;
            (*op).xpr.type_ = pg_sys::NodeTag::T_OpExpr;
            (*op).opno = opno;
            (*op).opfuncid = opfuncid;
            (*op).opresulttype = result_oid;
            (*op).opretset = false;
            (*op).opcollid = opcollid;
            (*op).inputcollid = pg_sys::DEFAULT_COLLATION_OID;
            (*op).args = args;
            (*op).location = -1;
            node = op as *mut pg_sys::Node;
        }

        // Wrap in CoerceViaIO if target type isn't text.
        if !matches!(spec.target_kind, ColumnKind::Text) {
            let target_oid = kind_to_type_oid(spec.target_kind);
            let coerce = pg_sys::palloc0(std::mem::size_of::<pg_sys::CoerceViaIO>())
                as *mut pg_sys::CoerceViaIO;
            (*coerce).xpr.type_ = pg_sys::NodeTag::T_CoerceViaIO;
            (*coerce).arg = node as *mut pg_sys::Expr;
            (*coerce).resulttype = target_oid;
            (*coerce).resultcollid = pg_sys::InvalidOid;
            (*coerce).coerceformat = pg_sys::CoercionForm::COERCE_EXPLICIT_CAST;
            (*coerce).location = -1;
            node = coerce as *mut pg_sys::Node;
        }

        node
    }
}

#[allow(dead_code)] // Activated by the upper-plan rewrite follow-up.
unsafe fn make_text_const(s: &str) -> *mut pg_sys::Const {
    unsafe {
        // cstring_to_text_with_len allocates a varlena in CurrentMemoryContext;
        // makeConst takes ownership.
        let ptr = pg_sys::cstring_to_text_with_len(
            s.as_ptr() as *const std::os::raw::c_char,
            s.len() as i32,
        );
        let datum = pgrx::pg_sys::Datum::from(ptr as *mut std::ffi::c_void);
        pg_sys::makeConst(
            pg_sys::TEXTOID,
            -1,
            pg_sys::DEFAULT_COLLATION_OID,
            -1, // varlena length
            datum,
            false,
            false,
        )
    }
}

// ---------------------------------------------------------------------------
// Plan-tree walker (post-`set_plan_references`)
// ---------------------------------------------------------------------------
//
// Walks the final `Plan` tree produced by `standard_planner` and rewrites
// JSONB-extract chains in upper-plan expressions to read pre-computed
// synthetic columns from a `DeltaXDecompress`'s slot. See `planner_hook`
// installation in `src/scan/hook.rs::deltax_planner` and the design notes
// in the plan file (`Plan rewrite — refined: post-set_plan_references walker
// via planner_hook`).
//
// Top-down recursion. Each plan node returns its `SubplanTlist` describing
// what's available at each tlist position from the parent's perspective —
// physical column from the underlying rel, synthetic JSON-extract column,
// or anything else we can't reason about. Parents use that information to
// substitute matching chain Exprs in their own expressions with
// `Var(OUTER_VAR, k)` referring to the synthetic position.
//
// Status: stub walker that traverses the tree and logs discovery via
// `pgrx::log!`. Full substitution lands incrementally.

/// What's at one tlist position of a plan node, from the perspective of the
/// parent plan (which references it via `Var(OUTER_VAR, attno)`).
#[derive(Debug, Clone)]
#[allow(dead_code)] // fields read by the substitution walker (next iteration)
pub(crate) enum SubplanColumn {
    /// A physical column of the underlying relation. `rel_var_attno` is the
    /// `pg_attribute.attnum` in that rel. Useful for chain matching: a chain
    /// `Var(OUTER_VAR, k)->>'kind'` whose Var resolves through us to a
    /// physical column with attno N is the same as `data->>'kind'` where
    /// `data` is column N.
    Physical { rel_var_attno: i16 },
    /// A synthetic JSON-extract column produced by a DeltaXDecompress
    /// somewhere below us. The walker substitutes matching chain Exprs at
    /// the parent level with `Var(OUTER_VAR, attno_pointing_here)`.
    Synthetic {
        path: Vec<String>,
        target_kind: ColumnKind,
        /// Attno of the source jsonb column in the underlying rel — used to
        /// disambiguate when multiple jsonb columns are extracted.
        src_var_attno: i16,
    },
    /// Any other expression we don't try to reason about.
    Other,
}

/// Per-plan-node summary returned by `rewrite_plan_subtree`.
pub(crate) type SubplanTlist = Vec<SubplanColumn>;

/// Top-level entry called by `planner_hook`. Walks the plan tree and applies
/// JSON-extract chain substitutions. Returns nothing — mutates plan nodes
/// in place.
pub(crate) unsafe fn rewrite_plan_tree(plan: *mut pg_sys::Plan) {
    unsafe {
        let _ = rewrite_plan_subtree(plan);
    }
}

/// Walk a plan subtree. Returns the `SubplanTlist` describing what each
/// position of THIS plan's targetlist provides to the parent.
unsafe fn rewrite_plan_subtree(plan: *mut pg_sys::Plan) -> SubplanTlist {
    unsafe {
        if plan.is_null() {
            return Vec::new();
        }

        // Leaf cases first.
        if (*plan).type_ == pg_sys::NodeTag::T_CustomScan {
            let cscan = plan as *mut pg_sys::CustomScan;
            return subplan_tlist_from_deltax_decompress(cscan).unwrap_or_default();
        }

        // Recursive case: collect children's SubplanTlists.
        let _child_stl = collect_child_subplan_tlist(plan);

        // TODO(json-extract): substitute matched chain Exprs in this plan's
        // expressions using `_child_stl`. For now this is a no-op walker so we
        // can validate plumbing without changing query results.

        // Compute MY SubplanTlist by walking my targetlist (post-substitution).
        compute_my_subplan_tlist(plan, &_child_stl)
    }
}

/// Inspect a `CustomScan` and, if it's a `DeltaXDecompress` carrying
/// json_extract specs in its `custom_private`, return a `SubplanTlist` that
/// describes its `custom_scan_tlist` shape (physical Vars + synthetic
/// extracts). Returns `None` for any other CustomScan or when no specs are
/// configured.
unsafe fn subplan_tlist_from_deltax_decompress(
    cscan: *mut pg_sys::CustomScan,
) -> Option<SubplanTlist> {
    unsafe {
        if cscan.is_null() {
            return None;
        }
        // Identify by methods name. Avoids depending on a specific layout.
        let methods = (*cscan).methods;
        if methods.is_null() {
            return None;
        }
        let name_ptr = (*methods).CustomName;
        if name_ptr.is_null() {
            return None;
        }
        let name = std::ffi::CStr::from_ptr(name_ptr);
        if name != crate::scan::CUSTOM_NAME {
            return None;
        }

        // custom_scan_tlist null → no synthetic columns; treat as plain
        // physical-only scan and let the caller fall through.
        let cstlist = (*cscan).custom_scan_tlist;
        if cstlist.is_null() {
            return None;
        }

        // Build SubplanTlist by inspecting each TargetEntry. Physical entries
        // are Var nodes referencing the rel; synthetic entries are chain
        // OpExprs (or CoerceViaIO-wrapped chains).
        let len = (*cstlist).length;
        let mut stl: SubplanTlist = Vec::with_capacity(len as usize);
        for i in 0..len {
            let tle = pg_sys::list_nth(cstlist, i) as *mut pg_sys::TargetEntry;
            if tle.is_null() || (*tle).expr.is_null() {
                stl.push(SubplanColumn::Other);
                continue;
            }
            let expr = (*tle).expr as *mut pg_sys::Node;
            stl.push(classify_custom_scan_tlist_entry(expr));
        }
        Some(stl)
    }
}

/// Classify one `custom_scan_tlist` entry: physical Var, JSON-extract chain,
/// or other. The chain detection mirrors `match_extract_chain` but without
/// the spec-list lookup — we only need to surface its shape upward, the
/// matching against user expressions happens at parent-level rewrite.
unsafe fn classify_custom_scan_tlist_entry(expr: *mut pg_sys::Node) -> SubplanColumn {
    unsafe {
        // Physical: a top-level Var referencing the underlying rel.
        if (*expr).type_ == pg_sys::NodeTag::T_Var {
            let v = expr as *mut pg_sys::Var;
            return SubplanColumn::Physical {
                rel_var_attno: (*v).varattno,
            };
        }

        // Synthetic: peel cast wrappers and walk the `->`/`->>` chain.
        let (chain_root, leaf_kind) = strip_outer_cast(expr);
        let mut path_keys: Vec<String> = Vec::new();
        let mut cursor = chain_root;
        let Some((k, deeper)) = match_op_step(cursor, JSONB_OBJECT_FIELD_TEXT_OPNO) else {
            return SubplanColumn::Other;
        };
        path_keys.push(k);
        cursor = deeper;
        loop {
            cursor = unwrap_relabel(cursor);
            match match_op_step(cursor, JSONB_OBJECT_FIELD_OPNO) {
                Some((k, deeper)) => {
                    path_keys.push(k);
                    cursor = deeper;
                }
                None => break,
            }
        }
        cursor = unwrap_relabel(cursor);
        if (*cursor).type_ != pg_sys::NodeTag::T_Var {
            return SubplanColumn::Other;
        }
        let v = cursor as *mut pg_sys::Var;
        path_keys.reverse();
        SubplanColumn::Synthetic {
            path: path_keys,
            target_kind: leaf_kind,
            src_var_attno: (*v).varattno,
        }
    }
}

/// Recurse into children. For Append/MergeAppend take the intersection of
/// child SubplanTlists at each position so a synthetic only propagates if
/// every child can serve it. For everything else, take the lefttree's STL.
unsafe fn collect_child_subplan_tlist(plan: *mut pg_sys::Plan) -> SubplanTlist {
    unsafe {
        match (*plan).type_ {
            pg_sys::NodeTag::T_Append => {
                let app = plan as *mut pg_sys::Append;
                intersect_children_subplan_tlists((*app).appendplans)
            }
            pg_sys::NodeTag::T_MergeAppend => {
                let mapp = plan as *mut pg_sys::MergeAppend;
                intersect_children_subplan_tlists((*mapp).mergeplans)
            }
            _ => {
                if !(*plan).lefttree.is_null() {
                    rewrite_plan_subtree((*plan).lefttree)
                } else {
                    Vec::new()
                }
            }
        }
    }
}

unsafe fn intersect_children_subplan_tlists(plan_list: *mut pg_sys::List) -> SubplanTlist {
    unsafe {
        if plan_list.is_null() || (*plan_list).length == 0 {
            return Vec::new();
        }
        let n = (*plan_list).length;
        let first = pg_sys::list_nth(plan_list, 0) as *mut pg_sys::Plan;
        let mut acc = rewrite_plan_subtree(first);
        for i in 1..n {
            let child = pg_sys::list_nth(plan_list, i) as *mut pg_sys::Plan;
            let stl = rewrite_plan_subtree(child);
            // Intersect element-wise: positions disagreeing become Other.
            // If lengths differ, truncate to shorter — over-approximation that
            // never produces wrong substitutions.
            let common = acc.len().min(stl.len());
            acc.truncate(common);
            for k in 0..common {
                if !subplan_columns_equivalent(&acc[k], &stl[k]) {
                    acc[k] = SubplanColumn::Other;
                }
            }
        }
        acc
    }
}

fn subplan_columns_equivalent(a: &SubplanColumn, b: &SubplanColumn) -> bool {
    match (a, b) {
        (
            SubplanColumn::Physical { rel_var_attno: a_n },
            SubplanColumn::Physical { rel_var_attno: b_n },
        ) => a_n == b_n,
        (
            SubplanColumn::Synthetic {
                path: a_p,
                target_kind: a_k,
                src_var_attno: a_s,
            },
            SubplanColumn::Synthetic {
                path: b_p,
                target_kind: b_k,
                src_var_attno: b_s,
            },
        ) => a_p == b_p && a_k == b_k && a_s == b_s,
        _ => false,
    }
}

/// Build THIS plan's SubplanTlist by walking its targetlist (post any
/// substitution we made). Each TargetEntry's expr tells us what's at its
/// resno: a forwarding `Var(OUTER_VAR, k)` carries the child's STL[k-1]
/// upward; anything else is Other.
unsafe fn compute_my_subplan_tlist(
    plan: *mut pg_sys::Plan,
    child_stl: &SubplanTlist,
) -> SubplanTlist {
    unsafe {
        let tlist = (*plan).targetlist;
        if tlist.is_null() {
            return Vec::new();
        }
        let mut my_stl: SubplanTlist = Vec::with_capacity((*tlist).length as usize);
        for i in 0..(*tlist).length {
            let tle = pg_sys::list_nth(tlist, i) as *mut pg_sys::TargetEntry;
            if tle.is_null() || (*tle).expr.is_null() {
                my_stl.push(SubplanColumn::Other);
                continue;
            }
            let expr = (*tle).expr as *mut pg_sys::Node;
            // Forwarding Var(OUTER_VAR, k)?
            if (*expr).type_ == pg_sys::NodeTag::T_Var {
                let v = expr as *mut pg_sys::Var;
                if (*v).varno == pg_sys::OUTER_VAR {
                    let k = (*v).varattno as usize;
                    if k >= 1 && k <= child_stl.len() {
                        my_stl.push(child_stl[k - 1].clone());
                        continue;
                    }
                }
            }
            my_stl.push(SubplanColumn::Other);
        }
        my_stl
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use pgrx::prelude::*;

    #[test]
    fn kind_to_type_oid_round_trip() {
        for kind in [
            ColumnKind::Text,
            ColumnKind::Int16,
            ColumnKind::Int32,
            ColumnKind::Int64,
            ColumnKind::Float32,
            ColumnKind::Float64,
            ColumnKind::Bool,
            ColumnKind::Timestamp,
            ColumnKind::TimestampTz,
            ColumnKind::Date,
            ColumnKind::Jsonb,
        ] {
            let oid = kind_to_type_oid(kind);
            let back = type_oid_to_kind(oid).expect("recognized");
            assert_eq!(back, kind, "round trip failed for {:?}", kind);
        }
    }
}
