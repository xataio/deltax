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
        // `vardata_any` returns `*const c_char`, whose signedness depends on
        // platform/ABI (i8 on x86_64 PG 18, u8 on aarch64). Cast through
        // `*const u8` so this compiles in both worlds.
        #[allow(clippy::unnecessary_cast)]
        let data = pgrx::vardata_any(detoasted) as *const u8;
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
    unsafe { rewrite_walker(node, rti, specs, phys, physical_natts) }
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
                -1,                 // typmod
                pg_sys::InvalidOid, // collation
                0,                  // varlevelsup
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
                (*c).arg = rewrite_walker(
                    (*c).arg as *mut pg_sys::Node,
                    rti,
                    specs,
                    phys,
                    physical_natts,
                ) as *mut pg_sys::Expr;
            }
            pg_sys::NodeTag::T_RelabelType => {
                let r = node as *mut pg_sys::RelabelType;
                (*r).arg = rewrite_walker(
                    (*r).arg as *mut pg_sys::Node,
                    rti,
                    specs,
                    phys,
                    physical_natts,
                ) as *mut pg_sys::Expr;
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
            let tle =
                pg_sys::makeTargetEntry(var as *mut pg_sys::Expr, (i + 1) as i16, resname, false);
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
pub(crate) enum SubplanColumn {
    /// A physical column of the underlying relation. `rel_var_attno` is the
    /// `pg_attribute.attnum` in that rel. Useful for chain matching: a chain
    /// `Var(OUTER_VAR, k)->>'kind'` whose Var resolves through us to a
    /// physical column with attno N is the same as `data->>'kind'` where
    /// `data` is column N.
    Physical { rel_var_attno: i16 },
    /// A synthetic JSON-extract column produced by a DeltaXDecompress
    /// somewhere below us. The walker substitutes matching chain Exprs at
    /// the parent level with `Var(OUTER_VAR, forwarder_resno)`.
    Synthetic {
        path: Vec<String>,
        target_kind: ColumnKind,
        /// Attno of the source jsonb column in the underlying rel — used to
        /// disambiguate when multiple jsonb columns are extracted.
        src_var_attno: i16,
        /// Position (resno) of the forwarder TargetEntry in this plan's
        /// `scan.plan.targetlist`, i.e. the attno that the parent plan should
        /// use in its `Var(OUTER_VAR, _)` to read this synthetic value.
        forwarder_resno: i16,
    },
    /// Any other expression we don't try to reason about.
    Other,
}

/// Per-plan-node summary returned by `rewrite_plan_subtree`.
pub(crate) type SubplanTlist = Vec<SubplanColumn>;

/// Top-level entry called by `planner_hook`. Walks the plan tree and applies
/// JSON-extract chain substitutions. Returns nothing — mutates plan nodes
/// in place. `rtable` is the PlannedStmt's range table, used to resolve
/// `cscan->scan.scanrelid` to a relation OID for the SPI spec lookup.
///
/// Gated on `pg_deltax.json_extract_mode`. With `none`, the walker is a
/// complete no-op (queries fall through to the slow path and return
/// correct results). With `fields`, the walker rewrites.
pub(crate) unsafe fn rewrite_plan_tree(plan: *mut pg_sys::Plan, rtable: *mut pg_sys::List) {
    unsafe {
        if !matches!(
            crate::get_json_extract_mode(),
            crate::JsonExtractMode::Fields
        ) {
            return;
        }
        let _ = rewrite_plan_subtree(plan, rtable);
    }
}

/// Walk a plan subtree. Returns the `SubplanTlist` describing what each
/// position of THIS plan's targetlist provides to the parent.
unsafe fn rewrite_plan_subtree(plan: *mut pg_sys::Plan, rtable: *mut pg_sys::List) -> SubplanTlist {
    unsafe {
        if plan.is_null() {
            return Vec::new();
        }

        // Leaf cases first.
        if (*plan).type_ == pg_sys::NodeTag::T_CustomScan {
            let cscan = plan as *mut pg_sys::CustomScan;
            return subplan_tlist_from_deltax_decompress(cscan, rtable).unwrap_or_default();
        }

        // Recursive case: collect children's SubplanTlists.
        let child_stl = collect_child_subplan_tlist(plan, rtable);

        // Substitute any matched chain Exprs in THIS plan's expressions.
        if !child_stl.is_empty() && has_any_synthetic(&child_stl) {
            substitute_chains_in_plan(plan, &child_stl);
        }

        // Compute MY SubplanTlist by walking my targetlist (post-substitution).
        compute_my_subplan_tlist(plan, &child_stl)
    }
}

fn has_any_synthetic(stl: &SubplanTlist) -> bool {
    stl.iter()
        .any(|c| matches!(c, SubplanColumn::Synthetic { .. }))
}

/// Walk the plan node's expression-bearing fields (`targetlist`, `qual`,
/// plus type-specific extras like `Agg.havingQual`) and substitute matched
/// JSONB-extract chains with `Var(OUTER_VAR, k)` referring to the child's
/// synthetic positions.
unsafe fn substitute_chains_in_plan(plan: *mut pg_sys::Plan, child_stl: &SubplanTlist) {
    unsafe {
        // Generic fields on every Plan.
        (*plan).targetlist = substitute_chains_in_tlist((*plan).targetlist, child_stl);
        (*plan).qual = substitute_chains_in_list((*plan).qual, child_stl);

        // Per-node-type extras.
        match (*plan).type_ {
            pg_sys::NodeTag::T_Agg => {
                let agg = plan as *mut pg_sys::Agg;
                (*agg).chain = substitute_chains_in_list((*agg).chain, child_stl);
            }
            pg_sys::NodeTag::T_WindowAgg => {
                // WindowAgg has runCondition (a List of Exprs).
                let wa = plan as *mut pg_sys::WindowAgg;
                (*wa).runCondition = substitute_chains_in_list((*wa).runCondition, child_stl);
            }
            _ => {}
        }
    }
}

/// Walk a `List<TargetEntry*>` and rewrite each TE's `expr` in place.
unsafe fn substitute_chains_in_tlist(
    tlist: *mut pg_sys::List,
    child_stl: &SubplanTlist,
) -> *mut pg_sys::List {
    unsafe {
        if tlist.is_null() {
            return tlist;
        }
        for i in 0..(*tlist).length {
            let tle = pg_sys::list_nth(tlist, i) as *mut pg_sys::TargetEntry;
            if tle.is_null() || (*tle).expr.is_null() {
                continue;
            }
            let new_expr = substitute_in_expr_node((*tle).expr as *mut pg_sys::Node, child_stl);
            (*tle).expr = new_expr as *mut pg_sys::Expr;
        }
        tlist
    }
}

/// Walk a `List<Expr*>` and substitute in place.
unsafe fn substitute_chains_in_list(
    list: *mut pg_sys::List,
    child_stl: &SubplanTlist,
) -> *mut pg_sys::List {
    unsafe {
        if list.is_null() {
            return list;
        }
        let mut out: *mut pg_sys::List = std::ptr::null_mut();
        for i in 0..(*list).length {
            let elt = pg_sys::list_nth(list, i) as *mut pg_sys::Node;
            let rewritten = substitute_in_expr_node(elt, child_stl);
            out = pg_sys::lappend(out, rewritten as *mut _);
        }
        out
    }
}

/// Recursively walk an Expr tree. At each node, first check if the WHOLE
/// node is a JSONB-extract chain that matches a synthetic from `child_stl`;
/// if yes, replace with `Var(OUTER_VAR, attno)`. Otherwise, recurse into
/// children (OpExpr.args, BoolExpr.args, FuncExpr.args, casts, NullTest,
/// CaseExpr, Aggref.args, etc.).
unsafe fn substitute_in_expr_node(
    node: *mut pg_sys::Node,
    child_stl: &SubplanTlist,
) -> *mut pg_sys::Node {
    unsafe {
        if node.is_null() {
            return node;
        }
        if let Some(replacement) = match_chain_against_child_stl(node, child_stl) {
            return replacement;
        }
        // Not a chain — recurse structurally.
        match (*node).type_ {
            pg_sys::NodeTag::T_OpExpr => {
                let op = node as *mut pg_sys::OpExpr;
                (*op).args = substitute_chains_in_list((*op).args, child_stl);
            }
            pg_sys::NodeTag::T_BoolExpr => {
                let b = node as *mut pg_sys::BoolExpr;
                (*b).args = substitute_chains_in_list((*b).args, child_stl);
            }
            pg_sys::NodeTag::T_FuncExpr => {
                let f = node as *mut pg_sys::FuncExpr;
                (*f).args = substitute_chains_in_list((*f).args, child_stl);
            }
            pg_sys::NodeTag::T_CoerceViaIO => {
                let c = node as *mut pg_sys::CoerceViaIO;
                (*c).arg = substitute_in_expr_node((*c).arg as *mut pg_sys::Node, child_stl)
                    as *mut pg_sys::Expr;
            }
            pg_sys::NodeTag::T_RelabelType => {
                let r = node as *mut pg_sys::RelabelType;
                (*r).arg = substitute_in_expr_node((*r).arg as *mut pg_sys::Node, child_stl)
                    as *mut pg_sys::Expr;
            }
            pg_sys::NodeTag::T_NullTest => {
                let n = node as *mut pg_sys::NullTest;
                (*n).arg = substitute_in_expr_node((*n).arg as *mut pg_sys::Node, child_stl)
                    as *mut pg_sys::Expr;
            }
            pg_sys::NodeTag::T_CaseExpr => {
                let c = node as *mut pg_sys::CaseExpr;
                (*c).args = substitute_chains_in_list((*c).args, child_stl);
                if !(*c).defresult.is_null() {
                    (*c).defresult =
                        substitute_in_expr_node((*c).defresult as *mut pg_sys::Node, child_stl)
                            as *mut pg_sys::Expr;
                }
            }
            pg_sys::NodeTag::T_CaseWhen => {
                let c = node as *mut pg_sys::CaseWhen;
                (*c).expr = substitute_in_expr_node((*c).expr as *mut pg_sys::Node, child_stl)
                    as *mut pg_sys::Expr;
                (*c).result = substitute_in_expr_node((*c).result as *mut pg_sys::Node, child_stl)
                    as *mut pg_sys::Expr;
            }
            pg_sys::NodeTag::T_Aggref => {
                let a = node as *mut pg_sys::Aggref;
                (*a).args = substitute_chains_in_list((*a).args, child_stl);
            }
            pg_sys::NodeTag::T_WindowFunc => {
                let w = node as *mut pg_sys::WindowFunc;
                (*w).args = substitute_chains_in_list((*w).args, child_stl);
            }
            pg_sys::NodeTag::T_ScalarArrayOpExpr => {
                let s = node as *mut pg_sys::ScalarArrayOpExpr;
                (*s).args = substitute_chains_in_list((*s).args, child_stl);
            }
            // Vars, Consts, Params, etc. are leaves with no children to rewrite.
            _ => {}
        }
        node
    }
}

/// If `node` is a JSONB-extract chain that matches one of the child's
/// synthetic entries, return a `Var(OUTER_VAR, attno=synthetic_position,
/// vartype=target_kind_oid)` ready to take its place. Otherwise return None.
///
/// Matching procedure:
/// 1. Peel cast wrappers; remember the leaf `ColumnKind`.
/// 2. Match outermost `OpExpr(opno=->>, _, Const(text key))`, accumulate keys.
/// 3. Walk `OpExpr(opno=->, _, Const)` steps inward.
/// 4. The innermost cursor must be a Var with `varno = OUTER_VAR` (the
///    standard post-setrefs form for refs into the immediate child plan).
/// 5. Look up `child_stl[var.varattno - 1]`; it must be `Physical` with the
///    same `rel_var_attno` as one of the child's `Synthetic` entries' src.
/// 6. Find a `Synthetic` entry in `child_stl` matching (path_keys, leaf_kind,
///    src_var_attno=physical's attno). Return Var(OUTER_VAR, that_attno).
unsafe fn match_chain_against_child_stl(
    node: *mut pg_sys::Node,
    child_stl: &SubplanTlist,
) -> Option<*mut pg_sys::Node> {
    unsafe {
        if node.is_null() {
            return None;
        }
        let (chain_root, leaf_kind) = strip_outer_cast(node);
        let mut path_keys: Vec<String> = Vec::new();
        let mut cursor = chain_root;

        let (k, deeper) = match_op_step(cursor, JSONB_OBJECT_FIELD_TEXT_OPNO)?;
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
            return None;
        }
        let inner_var = cursor as *mut pg_sys::Var;
        path_keys.reverse();

        // Inner var attno indexes into child's tlist. The child's STL at that
        // position must be Physical (the underlying jsonb column).
        let idx = (*inner_var).varattno;
        if idx < 1 {
            return None;
        }
        let i = (idx - 1) as usize;
        if i >= child_stl.len() {
            return None;
        }
        let physical_attno = match &child_stl[i] {
            SubplanColumn::Physical { rel_var_attno } => *rel_var_attno,
            // Already a synthetic — caller is referencing it via the chain
            // somehow (e.g. the lower plan already provided it). Don't try
            // to double-rewrite.
            _ => return None,
        };

        // Find a matching synthetic in child_stl with that source attno
        // and our (path_keys, leaf_kind).
        for col in child_stl.iter() {
            if let SubplanColumn::Synthetic {
                path,
                target_kind,
                src_var_attno,
                forwarder_resno,
            } = col
                && *src_var_attno == physical_attno
                && *target_kind == leaf_kind
                && path == &path_keys
            {
                let new_var = pg_sys::makeVar(
                    pg_sys::OUTER_VAR,
                    *forwarder_resno,
                    kind_to_type_oid(*target_kind),
                    -1,
                    if matches!(target_kind, ColumnKind::Text) {
                        pg_sys::DEFAULT_COLLATION_OID
                    } else {
                        pg_sys::InvalidOid
                    },
                    0,
                );
                return Some(new_var as *mut pg_sys::Node);
            }
        }
        None
    }
}

/// Inspect a `CustomScan` and, if it's a `DeltaXDecompress` or `DeltaXAppend`
/// carrying json_extract specs in its `custom_private`, return a
/// `SubplanTlist` describing its `custom_scan_tlist` shape (physical Vars
/// followed by synthetic extracts). Returns `None` for any other CustomScan
/// or when no specs are configured.
///
/// `DeltaXAppend` is the parallel-aware parent-baserel custom scan (one node
/// for the whole partitioned table); `DeltaXDecompress` is the per-partition
/// scan PG's planner picks for direct-partition queries. Both flavors expose
/// the same companion-blob shape, so the rebuild + forwarder logic is shared.
unsafe fn subplan_tlist_from_deltax_decompress(
    cscan: *mut pg_sys::CustomScan,
    rtable: *mut pg_sys::List,
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
        if name != crate::scan::CUSTOM_NAME && name != crate::scan::DELTAX_APPEND_NAME {
            return None;
        }

        // Rebuild `custom_scan_tlist` here. We can't trust whatever
        // `plan_custom_path` set on it: empirically, that value is
        // observed as NIL by the time `planner_hook` runs (post-
        // `set_plan_references`) — even though `plan_custom_path` left it
        // non-NIL a moment earlier. Static analysis of PG core says the only
        // writer is setrefs.c:1706, itself guarded by the same NIL check, so
        // the empirical observation contradicts the source. Rather than chase
        // it with GDB, we just rebuild the tlist here. This rebuild happens
        // AFTER `set_plan_references`, so nothing is going to mutate it again
        // before `ExecInitCustomScan` reads it to size the scan tuple slot.
        let cstlist = rebuild_custom_scan_tlist_from_catalog(cscan, rtable)?;
        if cstlist.is_null() {
            return None;
        }
        (*cscan).custom_scan_tlist = cstlist;

        // Rewrite scan.plan.targetlist so it (a) has its existing Var(rti)
        // entries replaced with `Var(INDEX_VAR, k)` (matching the slot
        // semantics tlistvarno=INDEX_VAR that PG assumes when
        // custom_scan_tlist is set), and (b) is extended with synthetic
        // forwarder TargetEntries — one per chain Expr in cstlist — so
        // upper plans' `Var(OUTER_VAR, forwarder_resno)` refs resolve to
        // the right slot positions.
        let _ = extend_scan_targetlist_with_forwarders(cscan, cstlist);

        // Append synthetic col_idx values to custom_private under the `-3`
        // sentinel so the executor's `build_needed_cols_from_custom_private`
        // loads the corresponding companion-blob columns. cstlist is
        // physical-first, synthetic-after; physical entries are top-level
        // Vars, synthetic entries are chain Exprs. Position k (0-indexed)
        // in cstlist == col_idx k in `col_names`.
        append_synthetic_col_indices_to_custom_private(cscan, cstlist);

        // Drop the original Section::Cols (the col_idx values between -1 and
        // -2/-3 that `plan_custom_path` populated). They were computed before
        // the walker ran and reflect the pre-rewrite needs (e.g. raw `data`
        // for chain Exprs that have since been rewritten to synthetic Vars).
        // Keeping them forces the executor to decompress columns nothing
        // references — defeats the whole point of json_extract.
        //
        // For json_extract paths that cover all upper-plan references to a
        // physical column, dropping Section::Cols entirely is safe. JSONBench
        // queries fit this profile; queries that reference raw `data`
        // alongside chain Exprs would need a ref-count to be correct (TODO).
        prune_section_cols_in_custom_private(cscan);

        // Build SubplanTlist by walking scan.plan.targetlist, NOT
        // custom_scan_tlist. Upper plans' OUTER_VAR refs index into
        // scan.plan.targetlist, so the SubplanTlist Vec must align with
        // it position-by-position. Each entry is classified by following
        // its `Var(INDEX_VAR, k_in_cstlist)` back into cstlist.
        Some(build_subplan_tlist_from_scan_targetlist(
            (*cscan).scan.plan.targetlist,
            cstlist,
        ))
    }
}

/// Build a `SubplanTlist` aligned 1:1 with `scan_targetlist`. For each
/// `TargetEntry`, follow `Var(INDEX_VAR, k)` back into `cstlist[k-1]` and
/// classify (physical Var on the rel → `Physical { rel_var_attno }`; chain
/// expression → `Synthetic { path, kind, src, forwarder_resno = entry.resno }`;
/// anything else → `Other`).
unsafe fn build_subplan_tlist_from_scan_targetlist(
    scan_targetlist: *mut pg_sys::List,
    cstlist: *mut pg_sys::List,
) -> SubplanTlist {
    unsafe {
        if scan_targetlist.is_null() {
            return Vec::new();
        }
        let n = (*scan_targetlist).length;
        let mut stl: SubplanTlist = Vec::with_capacity(n as usize);
        for i in 0..n {
            let tle = pg_sys::list_nth(scan_targetlist, i) as *mut pg_sys::TargetEntry;
            if tle.is_null() || (*tle).expr.is_null() {
                stl.push(SubplanColumn::Other);
                continue;
            }
            let expr = (*tle).expr as *mut pg_sys::Node;
            // Resolve `Var(INDEX_VAR, k)` → cstlist[k-1].
            if (*expr).type_ != pg_sys::NodeTag::T_Var {
                stl.push(SubplanColumn::Other);
                continue;
            }
            let v = expr as *mut pg_sys::Var;
            if (*v).varno != pg_sys::INDEX_VAR {
                stl.push(SubplanColumn::Other);
                continue;
            }
            let k = (*v).varattno;
            if k < 1 || cstlist.is_null() || k as i32 > (*cstlist).length {
                stl.push(SubplanColumn::Other);
                continue;
            }
            let cs_tle = pg_sys::list_nth(cstlist, (k - 1) as i32) as *mut pg_sys::TargetEntry;
            if cs_tle.is_null() || (*cs_tle).expr.is_null() {
                stl.push(SubplanColumn::Other);
                continue;
            }
            let cs_expr = (*cs_tle).expr as *mut pg_sys::Node;
            stl.push(classify_custom_scan_tlist_entry(cs_expr, (*tle).resno));
        }
        stl
    }
}

/// Strip Section::Cols (the col_idx values between sentinels `-1` and
/// `-2`/`-3`) from `cscan->custom_private`. Preserves companion OIDs (before
/// `-1`), Top-N info (after `-2`), and synthetic col_idx values (after `-3`).
/// Leaves the `-1` sentinel in place.
unsafe fn prune_section_cols_in_custom_private(cscan: *mut pg_sys::CustomScan) {
    unsafe {
        let cp = (*cscan).custom_private;
        if cp.is_null() {
            return;
        }
        let n = (*cp).length;
        let mut new_list: *mut pg_sys::List = std::ptr::null_mut();
        #[derive(PartialEq)]
        enum Phase {
            BeforeMinus1,
            InCols,
            After,
        }
        let mut phase = Phase::BeforeMinus1;
        for i in 0..n {
            let v = pg_sys::list_nth_int(cp, i);
            match phase {
                Phase::BeforeMinus1 => {
                    new_list = pg_sys::lappend_int(new_list, v);
                    if v == -1 {
                        phase = Phase::InCols;
                    }
                }
                Phase::InCols => {
                    if v == -2 || v == -3 {
                        new_list = pg_sys::lappend_int(new_list, v);
                        phase = Phase::After;
                    }
                    // else: drop (was a Section::Cols col_idx)
                }
                Phase::After => {
                    new_list = pg_sys::lappend_int(new_list, v);
                }
            }
        }
        (*cscan).custom_private = new_list;
    }
}

/// Append `[-3, col_idx_0, col_idx_1, ...]` to `cscan->custom_private` for
/// each synthetic column in `cstlist`. The executor's
/// `build_needed_cols_from_custom_private` recognizes the `-3` sentinel and
/// adds these col_idx values to the needed-cols mask so the decompress
/// loop loads them from companion blobs.
unsafe fn append_synthetic_col_indices_to_custom_private(
    cscan: *mut pg_sys::CustomScan,
    cstlist: *mut pg_sys::List,
) {
    unsafe {
        if cstlist.is_null() {
            return;
        }
        let mut col_indices: Vec<i32> = Vec::new();
        for i in 0..(*cstlist).length {
            let tle = pg_sys::list_nth(cstlist, i) as *mut pg_sys::TargetEntry;
            if tle.is_null() || (*tle).expr.is_null() {
                continue;
            }
            let expr = (*tle).expr as *mut pg_sys::Node;
            // Synthetic = non-Var entry. Position i in cstlist == col_idx i
            // in `col_names` (load_metadata layout).
            if (*expr).type_ != pg_sys::NodeTag::T_Var {
                col_indices.push(i);
            }
        }
        if col_indices.is_empty() {
            return;
        }
        // Append sentinel + col_idx values to custom_private.
        (*cscan).custom_private = pg_sys::lappend_int((*cscan).custom_private, -3);
        for idx in col_indices {
            (*cscan).custom_private = pg_sys::lappend_int((*cscan).custom_private, idx);
        }
    }
}

/// Rewrite the cscan's `scan.plan.targetlist`:
///   - Existing Var(varno=rti) entries → `Var(INDEX_VAR, k_in_cstlist)` where
///     k matches by varattno (preserves resnos so any pre-built upper-plan
///     `Var(OUTER_VAR, k)` refs into this tlist still resolve correctly).
///   - Append one forwarder TargetEntry per chain Expr in cstlist, with
///     `expr = Var(INDEX_VAR, k_in_cstlist, vartype=chain.result_type)` and
///     a fresh resno > current max. Returns the resnos of the appended
///     forwarders, in cstlist order.
unsafe fn extend_scan_targetlist_with_forwarders(
    cscan: *mut pg_sys::CustomScan,
    cstlist: *mut pg_sys::List,
) -> Vec<i16> {
    unsafe {
        // Build (varno, varattno) -> k_in_cstlist map for physical entries.
        let mut physical_map: Vec<(i32, i16, i16)> = Vec::new();
        let cs_len = (*cstlist).length;
        for i in 0..cs_len {
            let tle = pg_sys::list_nth(cstlist, i) as *mut pg_sys::TargetEntry;
            if tle.is_null() || (*tle).expr.is_null() {
                continue;
            }
            let expr = (*tle).expr as *mut pg_sys::Node;
            if (*expr).type_ == pg_sys::NodeTag::T_Var {
                let v = expr as *mut pg_sys::Var;
                physical_map.push(((*v).varno, (*v).varattno, (*tle).resno));
            }
        }

        // Rewrite existing entries' Vars in scan.plan.targetlist.
        let tlist = (*cscan).scan.plan.targetlist;
        if !tlist.is_null() {
            for i in 0..(*tlist).length {
                let tle = pg_sys::list_nth(tlist, i) as *mut pg_sys::TargetEntry;
                if tle.is_null() || (*tle).expr.is_null() {
                    continue;
                }
                let expr = (*tle).expr as *mut pg_sys::Node;
                if (*expr).type_ != pg_sys::NodeTag::T_Var {
                    continue;
                }
                let v = expr as *mut pg_sys::Var;
                for &(p_varno, p_attno, p_resno) in &physical_map {
                    if (*v).varno == p_varno && (*v).varattno == p_attno {
                        let new_var = pg_sys::makeVar(
                            pg_sys::INDEX_VAR,
                            p_resno,
                            (*v).vartype,
                            (*v).vartypmod,
                            (*v).varcollid,
                            (*v).varlevelsup,
                        );
                        (*tle).expr = new_var as *mut pg_sys::Expr;
                        break;
                    }
                }
            }
        }

        // Append synthetic forwarder TargetEntries.
        let mut next_resno: i16 = if (*cscan).scan.plan.targetlist.is_null() {
            1
        } else {
            ((*(*cscan).scan.plan.targetlist).length + 1) as i16
        };
        let mut forwarder_resnos: Vec<i16> = Vec::new();
        for i in 0..cs_len {
            let cs_tle = pg_sys::list_nth(cstlist, i) as *mut pg_sys::TargetEntry;
            if cs_tle.is_null() || (*cs_tle).expr.is_null() {
                continue;
            }
            let cs_expr = (*cs_tle).expr;
            if (*(cs_expr as *mut pg_sys::Node)).type_ == pg_sys::NodeTag::T_Var {
                continue; // skip physical entries
            }
            let vartype = pg_sys::exprType(cs_expr as *const _);
            let varcollid = pg_sys::exprCollation(cs_expr as *const _);
            let new_var = pg_sys::makeVar(
                pg_sys::INDEX_VAR,
                (*cs_tle).resno,
                vartype,
                -1,
                varcollid,
                0,
            );
            let resname = if !(*cs_tle).resname.is_null() {
                pg_sys::pstrdup((*cs_tle).resname)
            } else {
                std::ptr::null_mut()
            };
            let new_tle = pg_sys::makeTargetEntry(
                new_var as *mut pg_sys::Expr,
                next_resno,
                resname,
                true, // resjunk: not part of the user-visible projection
            );
            (*cscan).scan.plan.targetlist =
                pg_sys::lappend((*cscan).scan.plan.targetlist, new_tle as *mut _);
            forwarder_resnos.push(next_resno);
            next_resno += 1;
        }

        forwarder_resnos
    }
}

/// Classify one `custom_scan_tlist` entry: physical Var, JSON-extract chain,
/// or other. The chain detection mirrors `match_extract_chain` but without
/// the spec-list lookup — we only need to surface its shape upward, the
/// matching against user expressions happens at parent-level rewrite.
/// `forwarder_resno` is the resno the caller chose for the corresponding
/// forwarder TargetEntry in `scan.plan.targetlist`; it's stored so the
/// matcher can produce `Var(OUTER_VAR, forwarder_resno)`.
unsafe fn classify_custom_scan_tlist_entry(
    expr: *mut pg_sys::Node,
    forwarder_resno: i16,
) -> SubplanColumn {
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
            forwarder_resno,
        }
    }
}

/// Recurse into children. For Append/MergeAppend take the intersection of
/// child SubplanTlists at each position so a synthetic only propagates if
/// every child can serve it. For everything else, take the lefttree's STL.
unsafe fn collect_child_subplan_tlist(
    plan: *mut pg_sys::Plan,
    rtable: *mut pg_sys::List,
) -> SubplanTlist {
    unsafe {
        match (*plan).type_ {
            pg_sys::NodeTag::T_Append => {
                let app = plan as *mut pg_sys::Append;
                intersect_children_subplan_tlists((*app).appendplans, rtable)
            }
            pg_sys::NodeTag::T_MergeAppend => {
                let mapp = plan as *mut pg_sys::MergeAppend;
                intersect_children_subplan_tlists((*mapp).mergeplans, rtable)
            }
            _ => {
                if !(*plan).lefttree.is_null() {
                    rewrite_plan_subtree((*plan).lefttree, rtable)
                } else {
                    Vec::new()
                }
            }
        }
    }
}

unsafe fn intersect_children_subplan_tlists(
    plan_list: *mut pg_sys::List,
    rtable: *mut pg_sys::List,
) -> SubplanTlist {
    unsafe {
        if plan_list.is_null() || (*plan_list).length == 0 {
            return Vec::new();
        }
        let n = (*plan_list).length;
        let first = pg_sys::list_nth(plan_list, 0) as *mut pg_sys::Plan;
        let mut acc = rewrite_plan_subtree(first, rtable);
        for i in 1..n {
            let child = pg_sys::list_nth(plan_list, i) as *mut pg_sys::Plan;
            let stl = rewrite_plan_subtree(child, rtable);
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

/// Resolve `cscan->scan.scanrelid` to the underlying relation OID by
/// indexing into the PlannedStmt range table, then SPI-look up the
/// deltatable's `json_extract` config and rebuild the custom_scan_tlist
/// from scratch. Returns `None` when the rel isn't a deltax-managed table
/// or has no extraction configured. Returns `Some(NIL)` to signal "tried
/// but nothing to add" (caller treats as None).
unsafe fn rebuild_custom_scan_tlist_from_catalog(
    cscan: *mut pg_sys::CustomScan,
    rtable: *mut pg_sys::List,
) -> Option<*mut pg_sys::List> {
    unsafe {
        let scanrelid = (*cscan).scan.scanrelid;
        if scanrelid == 0 || rtable.is_null() {
            return None;
        }
        let rti_zero_indexed = (scanrelid as i32) - 1;
        if rti_zero_indexed < 0 || rti_zero_indexed >= (*rtable).length {
            return None;
        }
        let rte = pg_sys::list_nth(rtable, rti_zero_indexed) as *mut pg_sys::RangeTblEntry;
        if rte.is_null() {
            return None;
        }
        let rel_oid = (*rte).relid;
        if rel_oid == pg_sys::InvalidOid {
            return None;
        }

        // SPI lookup of json_extract for this rel.
        let specs = crate::scan::path::load_extract_specs_for_rel_pub(rel_oid);
        if specs.is_empty() {
            return None;
        }

        Some(build_custom_scan_tlist(scanrelid, rel_oid, &specs))
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
                forwarder_resno: a_r,
            },
            SubplanColumn::Synthetic {
                path: b_p,
                target_kind: b_k,
                src_var_attno: b_s,
                forwarder_resno: b_r,
            },
        ) => a_p == b_p && a_k == b_k && a_s == b_s && a_r == b_r,
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
