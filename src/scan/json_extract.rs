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

/// Plan-time helper for translating JSONB chain Exprs into synthetic-column
/// references during DeltaXAgg path construction. Built once per call to
/// `deltax_create_upper_paths` (or `plan_agg_path`) and consulted by:
///
///   - `hook.rs`'s aggregate-arg / GROUP BY classifier — to accept chains
///     that map to extracted columns and emit `AggSpec` / `GroupByColSpec`
///     entries with `col_idx = physical_count + spec_position`.
///   - `path.rs::plan_agg_path` — to rewrite chains in the WHERE qual list
///     before `nodeToString` serialisation, so `extract_batch_quals` sees
///     plain Var nodes at execution time.
///
/// Returns `None` from `from_root` when the query has no inh parent, the
/// parent has no `json_extract` configuration, or the rewrite would touch
/// partitions compressed before json_extract was added (mixed-partition
/// gate).
pub(crate) struct AggChainCtx {
    pub(crate) specs: Vec<ExtractSpec>,
    pub(crate) phys: PhysicalCols,
    pub(crate) parent_rti: pg_sys::Index,
    pub(crate) physical_count: i16,
}

impl AggChainCtx {
    /// Discover the inh parent of this query, load its `json_extract` specs
    /// + physical columns, and confirm the rewrite is partition-safe.
    pub(crate) unsafe fn from_root(root: *mut pg_sys::PlannerInfo) -> Option<Self> {
        unsafe {
            let array_size = (*root).simple_rel_array_size;
            for rti in 1..array_size {
                let rte = *(*root).simple_rte_array.add(rti as usize);
                if rte.is_null() {
                    continue;
                }
                if (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION {
                    continue;
                }
                // Regular expanded parent, or one the flatten walker planned
                // un-expanded (pg_deltax.flatten_partitions) — the chain
                // rewrite treats both identically.
                let flattened = super::hook::is_partitioned_relkind((*rte).relkind)
                    && super::hook::is_flattened_parent((*rte).relid);
                if !(*rte).inh && !flattened {
                    continue;
                }
                let parent_oid = (*rte).relid;
                let specs = crate::scan::path::load_extract_specs_for_rel_pub(parent_oid);
                if specs.is_empty() {
                    return None;
                }
                if !crate::scan::path::is_json_extract_safe_for_rel(parent_oid) {
                    return None;
                }
                let phys = PhysicalCols::from_rel_oid(parent_oid);
                let physical_count = phys.by_attno.len() as i16;
                return Some(Self {
                    specs,
                    phys,
                    parent_rti: rti as pg_sys::Index,
                    physical_count,
                });
            }
            None
        }
    }

    /// Try to interpret `node` as a JSONB extract chain rooted at the inh
    /// parent's RTI. On match returns `(col_idx, type_oid)` where `col_idx`
    /// is the position of the synthetic column in `MetadataInfo::col_names`
    /// (= `physical_count + spec_index`).
    pub(crate) unsafe fn match_to_synthetic(
        &self,
        node: *const pg_sys::Node,
    ) -> Option<(i32, pg_sys::Oid)> {
        unsafe {
            let spec_idx = match_extract_chain(
                node as *mut pg_sys::Node,
                self.parent_rti,
                &self.specs,
                &self.phys,
            )?;
            let spec = &self.specs[spec_idx];
            let col_idx = self.physical_count as i32 + spec_idx as i32;
            let type_oid = kind_to_type_oid(spec.target_kind);
            Some((col_idx, type_oid))
        }
    }
}

impl PhysicalCols {
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
        let shape = walk_chain_shape(node)?;
        if (*shape.inner_var).varno != rti as i32 {
            return None;
        }
        let src_col_name = phys.by_attno.get(&(*shape.inner_var).varattno)?;
        specs.iter().position(|s| {
            &s.src_column == src_col_name
                && s.path == shape.keys
                && s.target_kind == shape.leaf_kind
        })
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

/// Result of walking a JSONB-extract chain shape: the keys (innermost-first),
/// the effective leaf type after any outer cast, and the innermost Var pointer
/// the chain reads from. Returned by [`walk_chain_shape`].
struct ChainShape {
    keys: Vec<String>,
    leaf_kind: ColumnKind,
    inner_var: *mut pg_sys::Var,
}

/// Recognize the JSONB-extract chain shape `cast?(->>(... ->(Var, k), k))`
/// and return its constituent parts. Used by every chain-matching site in
/// this file (planner-level recognition, plan-tree rewrite, scan-level qual
/// rewrite, cstlist classification). Callers add their own constraints on
/// `inner_var.varno` / `varattno` and on the spec lookup.
unsafe fn walk_chain_shape(node: *mut pg_sys::Node) -> Option<ChainShape> {
    unsafe {
        if node.is_null() {
            return None;
        }
        // Peel outer cast wrappers to learn the effective result type.
        let (chain_root, leaf_kind) = strip_outer_cast(node);

        // Outermost step is `->>` (text result); inner steps are `->` until
        // a Var. `match_op_step` returns `deeper` already unwrap_relabel'd,
        // so we don't need to peel again between iterations.
        let mut keys: Vec<String> = Vec::new();
        let (k, mut cursor) = match_op_step(chain_root, JSONB_OBJECT_FIELD_TEXT_OPNO)?;
        keys.push(k);
        while let Some((k, deeper)) = match_op_step(cursor, JSONB_OBJECT_FIELD_OPNO) {
            keys.push(k);
            cursor = deeper;
        }
        // Innermost must be a Var. unwrap any RelabelType the planner inserted
        // (rare, but cheap).
        cursor = unwrap_relabel(cursor);
        if (*cursor).type_ != pg_sys::NodeTag::T_Var {
            return None;
        }
        keys.reverse();
        Some(ChainShape {
            keys,
            leaf_kind,
            inner_var: cursor as *mut pg_sys::Var,
        })
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

/// Walk a list of expressions and rewrite each in place, returning a new list
/// with each element mapped through the JSONB-extract chain rewriter. Matched
/// chains become `Var(varno=INDEX_VAR, varattno=physical_natts + spec_idx + 1,
/// vartype=...)`.
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
            pg_sys::NodeTag::T_ScalarArrayOpExpr => {
                // `chain IN (...)` / `chain = ANY(ARRAY[...])`. The planner
                // gate in `deltax_create_upper_paths` accepts SAOP with a
                // chain LHS; without rewriting here, the chain stays as a
                // T_OpExpr in the serialised qual list and `extract_batch_quals`
                // (which keys off T_Var) silently drops the entire qual at
                // exec time → wrong results for `IN`-on-chain queries.
                let s = node as *mut pg_sys::ScalarArrayOpExpr;
                (*s).args = rewrite_chains_in_list((*s).args, rti, specs, phys, physical_natts);
            }
            // Vars, Consts, Params, etc. are leaves.
            _ => {}
        }

        node
    }
}

/// Build the `custom_scan_tlist` for a json-extract-enabled DeltaXDecompress.
///
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
        // Phase 0: pre-walk the plan tree to collect every chain Expr's
        // (path, leaf_kind) signature. Phase 1 below uses this set to narrow
        // its synthetic-forwarder propagation to only the synthetics some
        // upper-plan chain Expr could actually reference. Without this the
        // walker propagates ALL of the cscan's synthetics into every ancestor
        // tlist — fine for correctness, but per-row slot copy overhead shows
        // up as a 30-50% regression on simple GROUP BY queries that don't
        // need any propagation at all (JSONBench Q0/Q2).
        let mut needed: std::collections::HashSet<ChainSig> = std::collections::HashSet::new();
        collect_chain_signatures_in_plan(plan, &mut needed);

        // Phase 1: rewrite chain Exprs in upper plans to Var(OUTER_VAR, k) refs
        // into our scan's synthetic forwarder positions.
        let _ = rewrite_plan_subtree(plan, rtable, &needed);
        // Phase 2: ref-count upper-plan Var(OUTER_VAR, k) refs and rewrite each
        // touched cscan's custom_private to load only the col_idx values that
        // are actually needed (referenced in upper plans, or by scan-level
        // qual). Without this we'd over-fetch — and worse, the overlap with
        // the plan_custom_path-supplied Section::Cols would re-include `data`
        // even when no upper-plan ref to it survives.
        prune_cscans_by_ref_count(plan);
    }
}

/// `(path, leaf_kind)` — uniquely identifies a chain Expr's shape. Used by
/// the phase-0 pre-walk to mark which cscan synthetics are worth propagating
/// up through intermediate plan tlists.
type ChainSig = (Vec<String>, ColumnKind);

/// Walk the plan tree and accumulate signatures of every chain Expr found in
/// any plan's tlist, qual, or Aggref/WindowFunc args. Stops at cscan boundary
/// (chain Exprs at the cscan are handled by `rewrite_scan_qual_chains` and
/// don't need propagation).
unsafe fn collect_chain_signatures_in_plan(
    plan: *mut pg_sys::Plan,
    out: &mut std::collections::HashSet<ChainSig>,
) {
    unsafe {
        if plan.is_null() {
            return;
        }
        if (*plan).type_ == pg_sys::NodeTag::T_CustomScan {
            return;
        }
        collect_chain_signatures_in_list((*plan).targetlist, out);
        collect_chain_signatures_in_list((*plan).qual, out);
        // `Agg.chain` is a list of sub-Agg *plan* nodes (GROUPING SETS
        // rollup) — not expressions — so don't pass it through the
        // expression walker. Each chain Agg's targetlist/qual is
        // processed via the normal plan-tree recursion below.
        if let pg_sys::NodeTag::T_WindowAgg = (*plan).type_ {
            let w = plan as *mut pg_sys::WindowAgg;
            collect_chain_signatures_in_list((*w).runCondition, out);
        }
        if !(*plan).lefttree.is_null() {
            collect_chain_signatures_in_plan((*plan).lefttree, out);
        }
        if !(*plan).righttree.is_null() {
            collect_chain_signatures_in_plan((*plan).righttree, out);
        }
        match (*plan).type_ {
            pg_sys::NodeTag::T_Append => {
                let app = plan as *mut pg_sys::Append;
                let lst = (*app).appendplans;
                if !lst.is_null() {
                    for i in 0..(*lst).length {
                        let child = pg_sys::list_nth(lst, i) as *mut pg_sys::Plan;
                        collect_chain_signatures_in_plan(child, out);
                    }
                }
            }
            pg_sys::NodeTag::T_MergeAppend => {
                let mapp = plan as *mut pg_sys::MergeAppend;
                let lst = (*mapp).mergeplans;
                if !lst.is_null() {
                    for i in 0..(*lst).length {
                        let child = pg_sys::list_nth(lst, i) as *mut pg_sys::Plan;
                        collect_chain_signatures_in_plan(child, out);
                    }
                }
            }
            _ => {}
        }
    }
}

unsafe fn collect_chain_signatures_in_list(
    list: *mut pg_sys::List,
    out: &mut std::collections::HashSet<ChainSig>,
) {
    unsafe {
        if list.is_null() {
            return;
        }
        for i in 0..(*list).length {
            let elt = pg_sys::list_nth(list, i) as *mut pg_sys::Node;
            collect_chain_signatures_in_node(elt, out);
        }
    }
}

unsafe fn collect_chain_signatures_in_node(
    node: *mut pg_sys::Node,
    out: &mut std::collections::HashSet<ChainSig>,
) {
    unsafe {
        if node.is_null() {
            return;
        }
        if let Some(sig) = chain_signature_of(node) {
            out.insert(sig);
        }
        // Recurse — chain Exprs may be nested inside larger Exprs (CASE,
        // FuncExpr args, etc.), and we want signatures from every chain.
        match (*node).type_ {
            pg_sys::NodeTag::T_OpExpr => {
                let op = node as *mut pg_sys::OpExpr;
                collect_chain_signatures_in_list((*op).args, out);
            }
            pg_sys::NodeTag::T_BoolExpr => {
                let b = node as *mut pg_sys::BoolExpr;
                collect_chain_signatures_in_list((*b).args, out);
            }
            pg_sys::NodeTag::T_FuncExpr => {
                let f = node as *mut pg_sys::FuncExpr;
                collect_chain_signatures_in_list((*f).args, out);
            }
            pg_sys::NodeTag::T_CoerceViaIO => {
                let c = node as *mut pg_sys::CoerceViaIO;
                collect_chain_signatures_in_node((*c).arg as *mut pg_sys::Node, out);
            }
            pg_sys::NodeTag::T_RelabelType => {
                let r = node as *mut pg_sys::RelabelType;
                collect_chain_signatures_in_node((*r).arg as *mut pg_sys::Node, out);
            }
            pg_sys::NodeTag::T_NullTest => {
                let n = node as *mut pg_sys::NullTest;
                collect_chain_signatures_in_node((*n).arg as *mut pg_sys::Node, out);
            }
            pg_sys::NodeTag::T_BooleanTest => {
                let b = node as *mut pg_sys::BooleanTest;
                collect_chain_signatures_in_node((*b).arg as *mut pg_sys::Node, out);
            }
            pg_sys::NodeTag::T_CaseExpr => {
                let c = node as *mut pg_sys::CaseExpr;
                collect_chain_signatures_in_list((*c).args, out);
                if !(*c).defresult.is_null() {
                    collect_chain_signatures_in_node((*c).defresult as *mut pg_sys::Node, out);
                }
                if !(*c).arg.is_null() {
                    collect_chain_signatures_in_node((*c).arg as *mut pg_sys::Node, out);
                }
            }
            pg_sys::NodeTag::T_CaseWhen => {
                let c = node as *mut pg_sys::CaseWhen;
                collect_chain_signatures_in_node((*c).expr as *mut pg_sys::Node, out);
                collect_chain_signatures_in_node((*c).result as *mut pg_sys::Node, out);
            }
            pg_sys::NodeTag::T_Aggref => {
                let a = node as *mut pg_sys::Aggref;
                collect_chain_signatures_in_list((*a).args, out);
                if !(*a).aggfilter.is_null() {
                    collect_chain_signatures_in_node((*a).aggfilter as *mut pg_sys::Node, out);
                }
            }
            pg_sys::NodeTag::T_WindowFunc => {
                let w = node as *mut pg_sys::WindowFunc;
                collect_chain_signatures_in_list((*w).args, out);
                if !(*w).aggfilter.is_null() {
                    collect_chain_signatures_in_node((*w).aggfilter as *mut pg_sys::Node, out);
                }
            }
            pg_sys::NodeTag::T_ScalarArrayOpExpr => {
                let s = node as *mut pg_sys::ScalarArrayOpExpr;
                collect_chain_signatures_in_list((*s).args, out);
            }
            pg_sys::NodeTag::T_TargetEntry => {
                let t = node as *mut pg_sys::TargetEntry;
                collect_chain_signatures_in_node((*t).expr as *mut pg_sys::Node, out);
            }
            _ => {}
        }
    }
}

/// Walk a candidate chain Expr; return its `(path, leaf_kind)` signature if
/// it has the JSON-extract chain shape (cast?(`->>`(`->`*(Var, key), key))).
unsafe fn chain_signature_of(node: *mut pg_sys::Node) -> Option<ChainSig> {
    unsafe { walk_chain_shape(node).map(|s| (s.keys, s.leaf_kind)) }
}

/// Walk a plan subtree. Returns the `SubplanTlist` describing what each
/// position of THIS plan's targetlist provides to the parent.
///
/// `parent_stack` records the chain of ancestors visited during recursion
/// (root first, immediate parent last). When we reach a cscan leaf, we use
/// this stack to propagate synthetic forwarders up through every ancestor
/// — without that, intermediate plans like `GatherMerge` that PG built with
/// only the columns it thought necessary (typically passing raw `data`)
/// would never expose the synthetic to upper-level Aggrefs, and chains
/// inside `count(distinct data->>'did')`-style aggregates couldn't be
/// rewritten. Forwarders added here are `resjunk = true` so they don't
/// affect the user-visible output shape.
unsafe fn rewrite_plan_subtree(
    plan: *mut pg_sys::Plan,
    rtable: *mut pg_sys::List,
    needed: &std::collections::HashSet<ChainSig>,
) -> SubplanTlist {
    unsafe {
        let mut stack: Vec<*mut pg_sys::Plan> = Vec::new();
        rewrite_plan_subtree_with_stack(plan, &mut stack, rtable, needed)
    }
}

unsafe fn rewrite_plan_subtree_with_stack(
    plan: *mut pg_sys::Plan,
    parent_stack: &mut Vec<*mut pg_sys::Plan>,
    rtable: *mut pg_sys::List,
    needed: &std::collections::HashSet<ChainSig>,
) -> SubplanTlist {
    unsafe {
        if plan.is_null() {
            return Vec::new();
        }

        // Leaf cases first.
        if (*plan).type_ == pg_sys::NodeTag::T_CustomScan {
            let cscan = plan as *mut pg_sys::CustomScan;
            let stl =
                subplan_tlist_from_deltax_decompress(cscan, rtable, needed).unwrap_or_default();
            if !stl.is_empty() {
                // Add resjunk forwarder TLEs to every ancestor so chains in
                // upper-level Aggrefs/exprs can reference the synthetic via
                // `Var(OUTER_VAR, k)`. Without this, intermediate plans that
                // weren't asked to project the synthetic (e.g. GatherMerge
                // when the upper plan only "needs" raw `data` per PG's plan
                // analysis) hide it from the chain matcher above. We only
                // propagate synthetics whose `(path, kind)` matches a chain
                // Expr seen anywhere in the plan tree (phase-0 set), so simple
                // queries like `SELECT data->>'kind', count(*) GROUP BY 1`
                // don't pay propagation overhead for paths nothing references.
                propagate_synthetics_through_ancestors(parent_stack, &stl, needed);
            }
            return stl;
        }

        parent_stack.push(plan);
        let child_stl = collect_child_subplan_tlist(plan, parent_stack, rtable, needed);
        parent_stack.pop();

        // Substitute any matched chain Exprs in THIS plan's expressions.
        if !child_stl.is_empty() && has_any_synthetic(&child_stl) {
            substitute_chains_in_plan(plan, &child_stl);
        }

        // Compute MY SubplanTlist by walking my targetlist (post-substitution).
        compute_my_subplan_tlist(plan, &child_stl)
    }
}

/// For each synthetic in `cscan_stl`, walk up `ancestors` (immediate parent
/// first) and ensure each ancestor's targetlist exposes a `Var(OUTER_VAR, k)`
/// forwarder pointing at the synthetic. Stops at the first ancestor that
/// already exposes the position (de-dup), so multiple sibling cscans of an
/// Append don't double-add. New TLEs are `resjunk = true`.
unsafe fn propagate_synthetics_through_ancestors(
    ancestors: &[*mut pg_sys::Plan],
    cscan_stl: &SubplanTlist,
    needed: &std::collections::HashSet<ChainSig>,
) {
    unsafe {
        for col in cscan_stl.iter() {
            let SubplanColumn::Synthetic {
                forwarder_resno,
                target_kind,
                path,
                ..
            } = col
            else {
                continue;
            };
            // Skip synthetics no upper-plan chain references — propagation
            // is purely overhead in that case.
            let sig: ChainSig = (path.clone(), *target_kind);
            if !needed.contains(&sig) {
                continue;
            }
            // `forwarder_resno` is the synthetic's position in cscan's
            // `scan.plan.targetlist`. From there we walk up: each ancestor
            // adds (or finds) a forwarder TLE that references the position
            // exposed by the ancestor's immediate child.
            let var_type = kind_to_type_oid(*target_kind);
            let var_collid = if matches!(target_kind, ColumnKind::Text) {
                pg_sys::DEFAULT_COLLATION_OID
            } else {
                pg_sys::InvalidOid
            };
            let mut current_pos: i16 = *forwarder_resno;
            for &ancestor in ancestors.iter().rev() {
                if ancestor.is_null() || (*ancestor).targetlist.is_null() {
                    continue;
                }
                // Reuse an existing TLE that already forwards the same
                // position — typical when a sibling cscan or a chain
                // already added one. That keeps the tlist tight and makes
                // the forwarder chain consistent across siblings.
                if let Some(existing_resno) =
                    find_outer_var_forwarder((*ancestor).targetlist, current_pos)
                {
                    current_pos = existing_resno;
                    continue;
                }
                let new_resno = ((*(*ancestor).targetlist).length + 1) as i16;
                let new_var =
                    pg_sys::makeVar(pg_sys::OUTER_VAR, current_pos, var_type, -1, var_collid, 0);
                let new_tle = pg_sys::makeTargetEntry(
                    new_var as *mut pg_sys::Expr,
                    new_resno,
                    std::ptr::null_mut(),
                    true,
                );
                (*ancestor).targetlist = pg_sys::lappend((*ancestor).targetlist, new_tle as *mut _);
                current_pos = new_resno;
            }
        }
    }
}

/// If `tlist` contains a `TargetEntry` whose expr is `Var(OUTER_VAR, k)` with
/// `k == varattno`, return that TLE's resno. Otherwise None.
unsafe fn find_outer_var_forwarder(tlist: *mut pg_sys::List, varattno: i16) -> Option<i16> {
    unsafe {
        if tlist.is_null() {
            return None;
        }
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
            if (*v).varno == pg_sys::OUTER_VAR && (*v).varattno == varattno {
                return Some((*tle).resno);
            }
        }
        None
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
            pg_sys::NodeTag::T_TargetEntry => {
                // Aggref.args is a list of TargetEntry, not raw Exprs.
                // Without descending into TargetEntry.expr, chain Exprs
                // inside aggregates like `count(distinct data->>'did')`
                // never get rewritten — the walker stops at the TargetEntry
                // boundary and the COUNT DISTINCT case in JSONBench Q1 stays
                // on the slow chain-eval path.
                let t = node as *mut pg_sys::TargetEntry;
                (*t).expr = substitute_in_expr_node((*t).expr as *mut pg_sys::Node, child_stl)
                    as *mut pg_sys::Expr;
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
        let ChainShape {
            keys: path_keys,
            leaf_kind,
            inner_var,
        } = walk_chain_shape(node)?;

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
    needed: &std::collections::HashSet<ChainSig>,
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

        // Skip the cstlist rebuild entirely when no chain Expr in the plan
        // could reference any synthetic. Without this gate, queries that
        // don't touch any chain Expr but run over a parent table whose
        // deltatable has json_extract configured ended up with a cstlist
        // containing all synthetics, which widens the cscan's scan tuple
        // descriptor (it's sized from custom_scan_tlist, not
        // scan.plan.targetlist). The widened tuple shape then breaks
        // direct-feed JOINs over the cscan output: setrefs computes
        // `Var(OUTER_VAR, k)` against scan.plan.targetlist, but the
        // slot-position math in HashJoin / NestLoop reads from the wider
        // scan tuple slot — the join's probe value comes from the wrong
        // physical slot position and matches nothing.
        let needs_chain_rewrite = check_cscan_has_relevant_synthetics(cscan, rtable, needed);
        if !needs_chain_rewrite {
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
        let _ = extend_scan_targetlist_with_forwarders(cscan, cstlist, needed);

        // Rewrite chain Exprs in the cscan's own scan-level qual to use
        // `Var(INDEX_VAR, synth_position)` refs into the synthetic columns.
        // Without this, filters like `WHERE data ->> 'kind' = 'commit'` get
        // evaluated by per-row JSONB chain expression — `data` is loaded just
        // for the qual, and 86× slower than letting the filter run against
        // the dictionary-encoded synthetic column.
        rewrite_scan_qual_chains(cscan, cstlist);

        // (custom_private rewriting is deferred to phase 2 in
        // `rewrite_plan_tree`. Phase 1 only sets up cstlist + scan tlist
        // forwarders and returns the SubplanTlist; phase 2 ref-counts upper
        // plans and writes back the correct Section::Cols + Section::Synth.)

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

/// Rewrite chain Exprs in `cscan.scan.plan.qual` (the scan-level filter list)
/// to `Var(INDEX_VAR, synth_position, target_kind_oid)` refs into the
/// synthetic columns of `cstlist`.
///
/// Differs from the upper-plan rewrite in `match_chain_against_child_stl`:
/// at the scan level the inner Var is `Var(varno=rti, varattno=src_attno)`
/// (post-setrefs scan-level Var), not `Var(varno=OUTER_VAR, ...)`. Match by
/// `(src_var_attno, path, target_kind)` against cstlist synthetics; the
/// physical attno comparison goes against the `Var.varattno` directly.
unsafe fn rewrite_scan_qual_chains(cscan: *mut pg_sys::CustomScan, cstlist: *mut pg_sys::List) {
    unsafe {
        let qual = (*cscan).scan.plan.qual;
        if qual.is_null() || cstlist.is_null() {
            return;
        }

        // Build a lookup of cstlist synthetics: (src_var_attno, path, kind) → cstlist position.
        let mut synths: Vec<SynthInfo> = Vec::new();
        for i in 0..(*cstlist).length {
            let tle = pg_sys::list_nth(cstlist, i) as *mut pg_sys::TargetEntry;
            if tle.is_null() || (*tle).expr.is_null() {
                continue;
            }
            let expr = (*tle).expr as *mut pg_sys::Node;
            // Skip physical entries (Var); we only care about chain-Expr synthetics.
            if (*expr).type_ == pg_sys::NodeTag::T_Var {
                continue;
            }
            if let SubplanColumn::Synthetic {
                path,
                target_kind,
                src_var_attno,
                ..
            } = classify_custom_scan_tlist_entry(expr, (*tle).resno)
            {
                synths.push(SynthInfo {
                    src_attno: src_var_attno,
                    path,
                    kind: target_kind,
                    position: (i + 1) as i16,
                });
            }
        }
        if synths.is_empty() {
            return;
        }

        // Walk each qual element and rewrite in place.
        for i in 0..(*qual).length {
            let cell_ptr = pg_sys::list_nth(qual, i) as *mut pg_sys::Node;
            let new_node = substitute_scan_chains_in_node(cell_ptr, &synths);
            // Update the list cell.
            let cell = (*qual).elements.add(i as usize);
            (*cell).ptr_value = new_node as *mut std::ffi::c_void;
        }
    }
}

unsafe fn substitute_scan_chains_in_node(
    node: *mut pg_sys::Node,
    synths: &[SynthInfo],
) -> *mut pg_sys::Node {
    unsafe {
        if node.is_null() {
            return node;
        }
        if let Some(replacement) = match_scan_chain_against_synths(node, synths) {
            return replacement;
        }
        match (*node).type_ {
            pg_sys::NodeTag::T_OpExpr => {
                let op = node as *mut pg_sys::OpExpr;
                (*op).args = substitute_scan_chains_in_list((*op).args, synths);
            }
            pg_sys::NodeTag::T_BoolExpr => {
                let b = node as *mut pg_sys::BoolExpr;
                (*b).args = substitute_scan_chains_in_list((*b).args, synths);
            }
            pg_sys::NodeTag::T_FuncExpr => {
                let f = node as *mut pg_sys::FuncExpr;
                (*f).args = substitute_scan_chains_in_list((*f).args, synths);
            }
            pg_sys::NodeTag::T_CoerceViaIO => {
                let c = node as *mut pg_sys::CoerceViaIO;
                (*c).arg = substitute_scan_chains_in_node((*c).arg as *mut pg_sys::Node, synths)
                    as *mut pg_sys::Expr;
            }
            pg_sys::NodeTag::T_RelabelType => {
                let r = node as *mut pg_sys::RelabelType;
                (*r).arg = substitute_scan_chains_in_node((*r).arg as *mut pg_sys::Node, synths)
                    as *mut pg_sys::Expr;
            }
            pg_sys::NodeTag::T_NullTest => {
                let n = node as *mut pg_sys::NullTest;
                (*n).arg = substitute_scan_chains_in_node((*n).arg as *mut pg_sys::Node, synths)
                    as *mut pg_sys::Expr;
            }
            pg_sys::NodeTag::T_ScalarArrayOpExpr => {
                let s = node as *mut pg_sys::ScalarArrayOpExpr;
                (*s).args = substitute_scan_chains_in_list((*s).args, synths);
            }
            _ => {}
        }
        node
    }
}

unsafe fn substitute_scan_chains_in_list(
    list: *mut pg_sys::List,
    synths: &[SynthInfo],
) -> *mut pg_sys::List {
    unsafe {
        if list.is_null() {
            return list;
        }
        for i in 0..(*list).length {
            let elt = pg_sys::list_nth(list, i) as *mut pg_sys::Node;
            let new_elt = substitute_scan_chains_in_node(elt, synths);
            let cell = (*list).elements.add(i as usize);
            (*cell).ptr_value = new_elt as *mut std::ffi::c_void;
        }
        list
    }
}

unsafe fn match_scan_chain_against_synths(
    node: *mut pg_sys::Node,
    synths: &[SynthInfo],
) -> Option<*mut pg_sys::Node> {
    unsafe {
        let ChainShape {
            keys: path_keys,
            leaf_kind,
            inner_var,
        } = walk_chain_shape(node)?;
        // At scan level, the inner var should be `Var(varno = rti)`; we
        // accept any varno except OUTER/INNER (those are upper-plan refs).
        if (*inner_var).varno == pg_sys::OUTER_VAR || (*inner_var).varno == pg_sys::INNER_VAR {
            return None;
        }
        let src_attno = (*inner_var).varattno;

        for s in synths {
            if s.src_attno == src_attno && s.kind == leaf_kind && s.path == path_keys {
                let new_var = pg_sys::makeVar(
                    pg_sys::INDEX_VAR,
                    s.position,
                    kind_to_type_oid(s.kind),
                    -1,
                    if matches!(s.kind, ColumnKind::Text) {
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

/// Helper alias so the qual rewriter's signatures don't need the inline
/// struct definition (Rust doesn't let us name a struct defined inside a fn).
struct SynthInfo {
    src_attno: i16,
    path: Vec<String>,
    kind: ColumnKind,
    position: i16,
}

// ============================================================================
// Phase-2 ref counter
//
// After phase 1 has rewritten chain Exprs into `Var(OUTER_VAR, k)` refs, walk
// the plan tree top-down to determine, per touched cscan, which positions in
// its `scan.plan.targetlist` are still referenced by some upper plan. Use
// that to rebuild `custom_private`'s Section::Cols (physical col_idx values
// the executor must load) and Section::Synth (synthetic col_idx values).
//
// Without this we either over-fetch (still loading `data` even though the
// chain was rewritten away — pre-fix behavior, blocking the JSONBench warm
// speedup) or silently NULL out positions that ARE still referenced (the
// unconditional-prune hack, which broke `SELECT data, data->>'kind'`-style
// queries).
// ============================================================================

/// Set of position numbers (1-indexed AttrNumber) referenced in some plan's
/// targetlist. Stored in a small Vec since these sets are tiny.
type PosSet = Vec<i16>;

unsafe fn descend_for_refs(plan: *mut pg_sys::Plan, my_refs: &PosSet) {
    unsafe {
        if plan.is_null() {
            return;
        }

        // Leaf: if this is a touched cscan, rebuild its custom_private from
        // `my_refs`. Don't recurse — the cscan IS the underlying data source.
        if (*plan).type_ == pg_sys::NodeTag::T_CustomScan {
            let cscan = plan as *mut pg_sys::CustomScan;
            if cscan_is_touched(cscan) {
                rebuild_cscan_custom_private(cscan, my_refs);
            }
            return;
        }

        // Compute child_refs: positions in lefttree's tlist that this plan
        // reads, by walking `Var(OUTER_VAR, k)` refs in:
        //   (a) my tlist entries at `my_refs` positions
        //   (b) always-evaluated exprs (qual, having, run conditions, ...)
        //   (c) tlist entries at indirectly-referenced positions (sort keys,
        //       group keys, partition/order keys for window) — these must be
        //       evaluated even if the parent doesn't read them.
        let mut effective: PosSet = my_refs.clone();
        add_node_specific_referenced_positions(plan, &mut effective);

        let mut child_refs: PosSet = Vec::new();
        let tlist = (*plan).targetlist;
        if !tlist.is_null() {
            let len = (*tlist).length as i16;
            for &p in &effective {
                if p < 1 || p > len {
                    continue;
                }
                let tle = pg_sys::list_nth(tlist, (p - 1) as i32) as *mut pg_sys::TargetEntry;
                if tle.is_null() || (*tle).expr.is_null() {
                    continue;
                }
                collect_outer_var_attnos((*tle).expr as *mut pg_sys::Node, &mut child_refs);
            }
        }
        // Always-evaluated exprs.
        collect_outer_var_attnos_in_list((*plan).qual, &mut child_refs);
        add_node_specific_outer_var_refs(plan, &mut child_refs);

        // Recurse.
        if !(*plan).lefttree.is_null() {
            descend_for_refs((*plan).lefttree, &child_refs);
        }
        // Inner side of joins uses INNER_VAR; we don't track those for now.
        // Touched cscans on the inner side would not get pruned correctly — a
        // future improvement.
        if !(*plan).righttree.is_null() {
            descend_for_refs_conservative((*plan).righttree);
        }
        // Append/MergeAppend: each child gets the same child_refs (Append's
        // child tlists are aligned 1:1 with Append's tlist by construction).
        match (*plan).type_ {
            pg_sys::NodeTag::T_Append => {
                let app = plan as *mut pg_sys::Append;
                let lst = (*app).appendplans;
                if !lst.is_null() {
                    for i in 0..(*lst).length {
                        let child = pg_sys::list_nth(lst, i) as *mut pg_sys::Plan;
                        descend_for_refs(child, &child_refs);
                    }
                }
            }
            pg_sys::NodeTag::T_MergeAppend => {
                let mapp = plan as *mut pg_sys::MergeAppend;
                let lst = (*mapp).mergeplans;
                if !lst.is_null() {
                    for i in 0..(*lst).length {
                        let child = pg_sys::list_nth(lst, i) as *mut pg_sys::Plan;
                        descend_for_refs(child, &child_refs);
                    }
                }
            }
            _ => {}
        }
    }
}

/// Conservative pass for plan subtrees we can't reason about precisely (e.g.
/// inner side of a join — INNER_VAR refs we don't track). Treats every
/// position in every tlist as referenced. Safe but defeats the prune for
/// any cscan reached this way.
unsafe fn descend_for_refs_conservative(plan: *mut pg_sys::Plan) {
    unsafe {
        if plan.is_null() {
            return;
        }
        if (*plan).type_ == pg_sys::NodeTag::T_CustomScan {
            let cscan = plan as *mut pg_sys::CustomScan;
            if cscan_is_touched(cscan) {
                let scan_tlist = (*cscan).scan.plan.targetlist;
                let n = if scan_tlist.is_null() {
                    0
                } else {
                    (*scan_tlist).length as i16
                };
                let all: PosSet = (1..=n).collect();
                rebuild_cscan_custom_private(cscan, &all);
            }
            return;
        }
        if !(*plan).lefttree.is_null() {
            descend_for_refs_conservative((*plan).lefttree);
        }
        if !(*plan).righttree.is_null() {
            descend_for_refs_conservative((*plan).righttree);
        }
        match (*plan).type_ {
            pg_sys::NodeTag::T_Append => {
                let app = plan as *mut pg_sys::Append;
                let lst = (*app).appendplans;
                if !lst.is_null() {
                    for i in 0..(*lst).length {
                        let child = pg_sys::list_nth(lst, i) as *mut pg_sys::Plan;
                        descend_for_refs_conservative(child);
                    }
                }
            }
            pg_sys::NodeTag::T_MergeAppend => {
                let mapp = plan as *mut pg_sys::MergeAppend;
                let lst = (*mapp).mergeplans;
                if !lst.is_null() {
                    for i in 0..(*lst).length {
                        let child = pg_sys::list_nth(lst, i) as *mut pg_sys::Plan;
                        descend_for_refs_conservative(child);
                    }
                }
            }
            _ => {}
        }
    }
}

unsafe fn prune_cscans_by_ref_count(root: *mut pg_sys::Plan) {
    unsafe {
        if root.is_null() {
            return;
        }
        // Root is the query output: every targetlist position is referenced.
        let tlist = (*root).targetlist;
        let n = if tlist.is_null() {
            0
        } else {
            (*tlist).length as i16
        };
        let initial: PosSet = (1..=n).collect();
        descend_for_refs(root, &initial);
    }
}

/// `cscan` was touched by phase 1 if `custom_scan_tlist` is non-NULL — that's
/// the bit `subplan_tlist_from_deltax_decompress` set when it rebuilt the
/// tlist with synthetic forwarders. Also gates on the methods name so we
/// don't rewrite somebody else's CustomScan.
unsafe fn cscan_is_touched(cscan: *mut pg_sys::CustomScan) -> bool {
    unsafe {
        if cscan.is_null() || (*cscan).custom_scan_tlist.is_null() {
            return false;
        }
        let methods = (*cscan).methods;
        if methods.is_null() {
            return false;
        }
        let name_ptr = (*methods).CustomName;
        if name_ptr.is_null() {
            return false;
        }
        let name = std::ffi::CStr::from_ptr(name_ptr);
        name == crate::scan::CUSTOM_NAME || name == crate::scan::DELTAX_APPEND_NAME
    }
}

/// Add positions in `plan.targetlist` that are referenced by node-specific
/// indirect mechanisms (sort keys, group keys, etc.) — i.e., positions that
/// must be evaluated even if the parent doesn't read them.
unsafe fn add_node_specific_referenced_positions(plan: *mut pg_sys::Plan, out: &mut PosSet) {
    unsafe {
        match (*plan).type_ {
            pg_sys::NodeTag::T_Sort => {
                let s = plan as *mut pg_sys::Sort;
                push_attr_array(out, (*s).sortColIdx, (*s).numCols);
            }
            pg_sys::NodeTag::T_IncrementalSort => {
                let s = plan as *mut pg_sys::IncrementalSort;
                push_attr_array(out, (*s).sort.sortColIdx, (*s).sort.numCols);
            }
            pg_sys::NodeTag::T_Agg => {
                let a = plan as *mut pg_sys::Agg;
                push_attr_array(out, (*a).grpColIdx, (*a).numCols);
            }
            pg_sys::NodeTag::T_Group => {
                let g = plan as *mut pg_sys::Group;
                push_attr_array(out, (*g).grpColIdx, (*g).numCols);
            }
            pg_sys::NodeTag::T_WindowAgg => {
                let w = plan as *mut pg_sys::WindowAgg;
                push_attr_array(out, (*w).partColIdx, (*w).partNumCols);
                push_attr_array(out, (*w).ordColIdx, (*w).ordNumCols);
            }
            _ => {}
        }
    }
}

/// Collect `Var(OUTER_VAR, k)` refs from node-specific expression-bearing
/// fields beyond `tlist` and `qual` (which the caller already walks).
///
/// Note: `Agg.chain` is intentionally **not** walked here — its entries
/// are sub-Agg *plan* nodes (the GROUPING SETS rollup chain), not
/// expressions, so passing them to `pull_var_clause` errors with
/// "unrecognized node type" on T_Agg. Each chain Agg has its own
/// targetlist/qual that PG processes separately at execution time.
unsafe fn add_node_specific_outer_var_refs(plan: *mut pg_sys::Plan, out: &mut PosSet) {
    unsafe {
        if (*plan).type_ == pg_sys::NodeTag::T_WindowAgg {
            let w = plan as *mut pg_sys::WindowAgg;
            collect_outer_var_attnos_in_list((*w).runCondition, out);
        }
    }
}

unsafe fn push_attr_array(out: &mut PosSet, arr: *mut pg_sys::AttrNumber, n: i32) {
    unsafe {
        if arr.is_null() || n <= 0 {
            return;
        }
        for i in 0..n as usize {
            let v = *arr.add(i);
            if !out.contains(&v) {
                out.push(v);
            }
        }
    }
}

// PG's pull_var_clause flag bits — extracted from optimizer.h. Recursing
// into aggregates/windowfuncs/placeholders means a single call covers any
// node tree the planner might hand us, including JsonValueExpr,
// CoalesceExpr, RowExpr, etc. — node types our ref-counter would otherwise
// have to enumerate by hand.
const PVC_RECURSE_AGGREGATES: i32 = 0x0002;
const PVC_RECURSE_WINDOWFUNCS: i32 = 0x0008;
const PVC_RECURSE_PLACEHOLDERS: i32 = 0x0020;
const PVC_FLAGS_FULL: i32 =
    PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_RECURSE_PLACEHOLDERS;

/// Collect attnos of every `Var(OUTER_VAR, k)` reachable from `node`.
/// Delegates the tree walk to PG's `pull_var_clause` so we cover every node
/// type PG knows about, not just the ones we hand-roll.
unsafe fn collect_outer_var_attnos(node: *mut pg_sys::Node, out: &mut PosSet) {
    unsafe {
        if node.is_null() {
            return;
        }
        let var_list = pg_sys::pull_var_clause(node, PVC_FLAGS_FULL);
        if var_list.is_null() {
            return;
        }
        for i in 0..(*var_list).length {
            let v = pg_sys::list_nth(var_list, i) as *mut pg_sys::Var;
            if v.is_null() || (*(v as *mut pg_sys::Node)).type_ != pg_sys::NodeTag::T_Var {
                continue;
            }
            if (*v).varno == pg_sys::OUTER_VAR {
                let k = (*v).varattno;
                if !out.contains(&k) {
                    out.push(k);
                }
            }
        }
    }
}

unsafe fn collect_outer_var_attnos_in_list(list: *mut pg_sys::List, out: &mut PosSet) {
    unsafe {
        if list.is_null() {
            return;
        }
        // pull_var_clause accepts a List* directly when treated as a Node*,
        // walking each element. Saves a per-element loop.
        collect_outer_var_attnos(list as *mut pg_sys::Node, out);
    }
}

/// Rewrite `cscan->custom_private` so the executor loads only the col_idx
/// values that are actually needed. Inputs:
///   * `referenced` — positions in `cscan.scan.plan.targetlist` that some
///     upper plan still references (post-rewrite). For each `p`, the entry
///     `scan_tlist[p-1]` is `Var(INDEX_VAR, k)` indexing into
///     `custom_scan_tlist`; `cstlist[k-1]` is either a physical Var
///     (Section::Cols) or a synthetic chain Expr (Section::Synth).
///   * `cscan.scan.plan.qual` — scan-level qual exprs (e.g. time-range
///     filters used for segment pruning) reference physical columns directly
///     via `Var(varno=rti)`. Those columns must remain in Section::Cols
///     regardless of upper-plan refs, otherwise pruning breaks.
///
/// New encoding (preserving everything the executor expects):
///   `[oid_0, oid_1, ..., -1, cols..., (-2, topn0..topn3,)? -3, synth...]`
unsafe fn rebuild_cscan_custom_private(cscan: *mut pg_sys::CustomScan, referenced: &PosSet) {
    unsafe {
        let cstlist = (*cscan).custom_scan_tlist;
        let scan_tlist = (*cscan).scan.plan.targetlist;
        if cstlist.is_null() || scan_tlist.is_null() {
            return;
        }

        // Map referenced scan-tlist positions → cstlist indices, partition
        // into Cols vs Synth based on cstlist entry type.
        let mut new_cols: Vec<i32> = Vec::new();
        let mut new_synth: Vec<i32> = Vec::new();
        let cs_len = (*cstlist).length as i16;
        for &p in referenced {
            if p < 1 || p > (*scan_tlist).length as i16 {
                continue;
            }
            let tle = pg_sys::list_nth(scan_tlist, (p - 1) as i32) as *mut pg_sys::TargetEntry;
            if tle.is_null() || (*tle).expr.is_null() {
                continue;
            }
            let expr = (*tle).expr as *mut pg_sys::Node;
            if (*expr).type_ != pg_sys::NodeTag::T_Var {
                continue;
            }
            let v = expr as *mut pg_sys::Var;
            if (*v).varno != pg_sys::INDEX_VAR {
                continue;
            }
            let k = (*v).varattno;
            if k < 1 || k > cs_len {
                continue;
            }
            let cs_tle = pg_sys::list_nth(cstlist, (k - 1) as i32) as *mut pg_sys::TargetEntry;
            if cs_tle.is_null() || (*cs_tle).expr.is_null() {
                continue;
            }
            let col_idx = (k - 1) as i32; // 0-based col_idx into col_names
            let cs_expr = (*cs_tle).expr as *mut pg_sys::Node;
            if (*cs_expr).type_ == pg_sys::NodeTag::T_Var {
                if !new_cols.contains(&col_idx) {
                    new_cols.push(col_idx);
                }
            } else if !new_synth.contains(&col_idx) {
                new_synth.push(col_idx);
            }
        }

        // Add columns referenced by scan-level qual via direct Var refs.
        // After set_plan_references, scan-level Vars carry varno=INDEX_VAR
        // (custom_scan_tlist active) — same shape we already mapped.
        // Vars that are NOT INDEX_VAR (still varno=rti) reference the underlying
        // relation by attno — find a physical entry in cstlist whose Var has
        // matching varattno.
        let scan_qual = (*cscan).scan.plan.qual;
        if !scan_qual.is_null() {
            // OUTER_VAR refs in scan-level qual shouldn't normally appear
            // here; if they did, they'd already be in `referenced` (from the
            // parent's descent). Walk for INDEX_VAR (cstlist refs) and
            // relation-level Vars (unrewritten quals).
            let mut qual_index: PosSet = Vec::new();
            let mut qual_relvar_attnos: PosSet = Vec::new();
            collect_index_and_rel_var_attnos_in_list(
                scan_qual,
                &mut qual_index,
                &mut qual_relvar_attnos,
            );
            for &k in &qual_index {
                if k < 1 || k > cs_len {
                    continue;
                }
                let col_idx = (k - 1) as i32;
                let cs_tle = pg_sys::list_nth(cstlist, (k - 1) as i32) as *mut pg_sys::TargetEntry;
                if cs_tle.is_null() || (*cs_tle).expr.is_null() {
                    continue;
                }
                let cs_expr = (*cs_tle).expr as *mut pg_sys::Node;
                if (*cs_expr).type_ == pg_sys::NodeTag::T_Var {
                    if !new_cols.contains(&col_idx) {
                        new_cols.push(col_idx);
                    }
                } else if !new_synth.contains(&col_idx) {
                    new_synth.push(col_idx);
                }
            }
            for &attno in &qual_relvar_attnos {
                // Find cstlist entry with physical Var(varattno == attno).
                for k in 1..=cs_len {
                    let cs_tle =
                        pg_sys::list_nth(cstlist, (k - 1) as i32) as *mut pg_sys::TargetEntry;
                    if cs_tle.is_null() || (*cs_tle).expr.is_null() {
                        continue;
                    }
                    let cs_expr = (*cs_tle).expr as *mut pg_sys::Node;
                    if (*cs_expr).type_ != pg_sys::NodeTag::T_Var {
                        continue;
                    }
                    let cs_v = cs_expr as *mut pg_sys::Var;
                    if (*cs_v).varattno == attno {
                        let col_idx = (k - 1) as i32;
                        if !new_cols.contains(&col_idx) {
                            new_cols.push(col_idx);
                        }
                        break;
                    }
                }
            }
        }

        // Walk the existing custom_private to:
        //   * preserve oids (before -1)
        //   * preserve top-n payload (after -2 if present)
        //   * replace Section::Cols and Section::Synth
        let cp = (*cscan).custom_private;
        let mut new_cp: *mut pg_sys::List = std::ptr::null_mut();
        if !cp.is_null() {
            #[derive(PartialEq)]
            enum Phase {
                BeforeMinus1,
                InCols,
                InTopn,
                InSynth,
            }
            let mut phase = Phase::BeforeMinus1;
            for i in 0..(*cp).length {
                let v = pg_sys::list_nth_int(cp, i);
                match phase {
                    Phase::BeforeMinus1 => {
                        new_cp = pg_sys::lappend_int(new_cp, v);
                        if v == -1 {
                            for &c in &new_cols {
                                new_cp = pg_sys::lappend_int(new_cp, c);
                            }
                            phase = Phase::InCols;
                        }
                    }
                    Phase::InCols => {
                        if v == -2 {
                            new_cp = pg_sys::lappend_int(new_cp, v);
                            phase = Phase::InTopn;
                        } else if v == -3 {
                            // Skip ahead; we'll emit -3 ourselves below.
                            phase = Phase::InSynth;
                        }
                        // else: drop (was a stale Section::Cols col_idx)
                    }
                    Phase::InTopn => {
                        if v == -3 {
                            phase = Phase::InSynth;
                        } else {
                            new_cp = pg_sys::lappend_int(new_cp, v);
                        }
                    }
                    Phase::InSynth => {
                        // Skip; we emit fresh synth list at the end.
                    }
                }
            }
        }
        if !new_synth.is_empty() {
            new_cp = pg_sys::lappend_int(new_cp, -3);
            for &c in &new_synth {
                new_cp = pg_sys::lappend_int(new_cp, c);
            }
        }
        (*cscan).custom_private = new_cp;
    }
}

/// Walk a node tree (typically a qual list) and partition every Var by
/// varno: `INDEX_VAR` refs go into `out_index`, refs that don't have one of
/// the special varnos (`OUTER_VAR`/`INNER_VAR`/`INDEX_VAR`) are treated as
/// relation-level Vars and their attnos go into `out_relvar`. The latter
/// arise when scan-level quals haven't been mapped through `custom_scan_tlist`
/// — typical for quals our walker hasn't rewritten.
///
/// Uses `pull_var_clause` so node-type coverage matches PG core.
unsafe fn collect_index_and_rel_var_attnos_in_list(
    list: *mut pg_sys::List,
    out_index: &mut PosSet,
    out_relvar: &mut PosSet,
) {
    unsafe {
        if list.is_null() {
            return;
        }
        let var_list = pg_sys::pull_var_clause(list as *mut pg_sys::Node, PVC_FLAGS_FULL);
        if var_list.is_null() {
            return;
        }
        for i in 0..(*var_list).length {
            let v = pg_sys::list_nth(var_list, i) as *mut pg_sys::Var;
            if v.is_null() || (*(v as *mut pg_sys::Node)).type_ != pg_sys::NodeTag::T_Var {
                continue;
            }
            let varno = (*v).varno;
            if varno == pg_sys::INDEX_VAR {
                let k = (*v).varattno;
                if !out_index.contains(&k) {
                    out_index.push(k);
                }
            } else if varno != pg_sys::OUTER_VAR && varno != pg_sys::INNER_VAR {
                let k = (*v).varattno;
                if k > 0 && !out_relvar.contains(&k) {
                    out_relvar.push(k);
                }
            }
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
    needed: &std::collections::HashSet<ChainSig>,
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
            // Only emit forwarders for synthetics whose `(path, leaf_kind)`
            // signature is in the plan tree. Without this gate, queries that
            // don't reference a chain Expr but run over a parent table whose
            // deltatable has json_extract configured end up with the
            // synthetic in cscan output → Append width mismatch when mixed
            // with non-cscan partition children.
            match classify_custom_scan_tlist_entry(cs_expr as *mut pg_sys::Node, (*cs_tle).resno) {
                SubplanColumn::Synthetic {
                    ref path,
                    target_kind,
                    ..
                } => {
                    let sig: ChainSig = (path.clone(), target_kind);
                    if !needed.contains(&sig) {
                        continue;
                    }
                }
                _ => continue,
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
        if !expr.is_null() && (*expr).type_ == pg_sys::NodeTag::T_Var {
            let v = expr as *mut pg_sys::Var;
            return SubplanColumn::Physical {
                rel_var_attno: (*v).varattno,
            };
        }
        match walk_chain_shape(expr) {
            Some(shape) => SubplanColumn::Synthetic {
                path: shape.keys,
                target_kind: shape.leaf_kind,
                src_var_attno: (*shape.inner_var).varattno,
                forwarder_resno,
            },
            None => SubplanColumn::Other,
        }
    }
}

/// Recurse into children. For Append/MergeAppend take the intersection of
/// child SubplanTlists at each position so a synthetic only propagates if
/// every child can serve it. For everything else, take the lefttree's STL.
unsafe fn collect_child_subplan_tlist(
    plan: *mut pg_sys::Plan,
    parent_stack: &mut Vec<*mut pg_sys::Plan>,
    rtable: *mut pg_sys::List,
    needed: &std::collections::HashSet<ChainSig>,
) -> SubplanTlist {
    unsafe {
        match (*plan).type_ {
            pg_sys::NodeTag::T_Append => {
                let app = plan as *mut pg_sys::Append;
                intersect_children_subplan_tlists((*app).appendplans, parent_stack, rtable, needed)
            }
            pg_sys::NodeTag::T_MergeAppend => {
                let mapp = plan as *mut pg_sys::MergeAppend;
                intersect_children_subplan_tlists((*mapp).mergeplans, parent_stack, rtable, needed)
            }
            _ => {
                if !(*plan).lefttree.is_null() {
                    rewrite_plan_subtree_with_stack((*plan).lefttree, parent_stack, rtable, needed)
                } else {
                    Vec::new()
                }
            }
        }
    }
}

unsafe fn intersect_children_subplan_tlists(
    plan_list: *mut pg_sys::List,
    parent_stack: &mut Vec<*mut pg_sys::Plan>,
    rtable: *mut pg_sys::List,
    needed: &std::collections::HashSet<ChainSig>,
) -> SubplanTlist {
    unsafe {
        if plan_list.is_null() || (*plan_list).length == 0 {
            return Vec::new();
        }
        let n = (*plan_list).length;
        let first = pg_sys::list_nth(plan_list, 0) as *mut pg_sys::Plan;
        let mut acc = rewrite_plan_subtree_with_stack(first, parent_stack, rtable, needed);
        for i in 1..n {
            let child = pg_sys::list_nth(plan_list, i) as *mut pg_sys::Plan;
            let stl = rewrite_plan_subtree_with_stack(child, parent_stack, rtable, needed);
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

/// Companion to `rebuild_custom_scan_tlist_from_catalog`: returns true iff
/// any `json_extract` spec for this cscan's parent rel matches a chain
/// signature in `needed` (the set collected from upper-plan chain Exprs).
/// When false, the cscan can stay at the shape `plan_custom_path` set,
/// avoiding a slot-descriptor widening that breaks setrefs's slot-position
/// math in direct-feed JOINs over the cscan output.
unsafe fn check_cscan_has_relevant_synthetics(
    cscan: *mut pg_sys::CustomScan,
    rtable: *mut pg_sys::List,
    needed: &std::collections::HashSet<ChainSig>,
) -> bool {
    unsafe {
        if needed.is_empty() {
            return false;
        }
        let scanrelid = (*cscan).scan.scanrelid;
        if scanrelid == 0 || rtable.is_null() {
            return false;
        }
        let rti_zero_indexed = (scanrelid as i32) - 1;
        if rti_zero_indexed < 0 || rti_zero_indexed >= (*rtable).length {
            return false;
        }
        let rte = pg_sys::list_nth(rtable, rti_zero_indexed) as *mut pg_sys::RangeTblEntry;
        if rte.is_null() {
            return false;
        }
        let rel_oid = (*rte).relid;
        if rel_oid == pg_sys::InvalidOid {
            return false;
        }
        let specs = crate::scan::path::load_extract_specs_for_rel_pub(rel_oid);
        if specs.is_empty() {
            return false;
        }
        // Any spec whose (path, target_kind) is in `needed` triggers the
        // rewrite. Otherwise the synthetics in this deltatable are
        // unreferenced by this query and we can skip the rewrite entirely.
        specs.iter().any(|s| {
            let sig: ChainSig = (s.path.clone(), s.target_kind);
            needed.contains(&sig)
        })
    }
}

/// Resolve `cscan->scan.scanrelid` to the underlying relation OID by indexing
/// into the PlannedStmt range table, then SPI-look up the deltatable's
/// `json_extract` config and rebuild the custom_scan_tlist from scratch.
/// Returns `None` when the rel isn't a deltax-managed table, has no extraction
/// configured, or when the mixed-partition gate fires (some relevant
/// compressed partition predates `json_extract_added_at` and lacks the
/// synthetic columns).
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
        // Mixed-partition gate: if any relevant compressed partition
        // predates `json_extract_added_at`, its companion blobs lack the
        // synthetic columns and the rewrite would emit NULLs at synthetic
        // positions. Skip the rewrite — falls through to the slow chain-Expr
        // path which still works correctly on those partitions.
        if !crate::scan::path::is_json_extract_safe_for_rel(rel_oid) {
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
                        // Clone the child STL entry but rebase the
                        // synthetic's `forwarder_resno` to MY tlist
                        // position (i + 1, 1-based). When a chain at
                        // a level above me matches against `my_stl`,
                        // the matcher returns `Var(OUTER_VAR, fr)` —
                        // that varattno needs to index into MY tlist
                        // (= my parent's immediate child), not into
                        // some deeper descendant's tlist.
                        let mut entry = child_stl[k - 1].clone();
                        if let SubplanColumn::Synthetic {
                            ref mut forwarder_resno,
                            ..
                        } = entry
                        {
                            *forwarder_resno = (i + 1) as i16;
                        }
                        my_stl.push(entry);
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

    /// Build a `Var(varno=1, varattno=1, vartype=jsonb)` for chain tests.
    unsafe fn make_jsonb_var() -> *mut pg_sys::Var {
        unsafe { pg_sys::makeVar(1, 1, pg_sys::JSONBOID, -1, pg_sys::InvalidOid, 0) }
    }

    /// Wrap `left` with `left <opno> <Const(key)>`, mirroring how the parser
    /// emits `->`/`->>` OpExprs. Reuses `make_text_const` / OpExpr layout from
    /// `build_chain_expr_for_spec`.
    unsafe fn make_op_expr(
        left: *mut pg_sys::Node,
        key: &str,
        opno: pg_sys::Oid,
        result_oid: pg_sys::Oid,
    ) -> *mut pg_sys::Node {
        unsafe {
            let key_const = make_text_const(key);
            let mut args: *mut pg_sys::List = std::ptr::null_mut();
            args = pg_sys::lappend(args, left as *mut _);
            args = pg_sys::lappend(args, key_const as *mut _);
            let op = pg_sys::palloc0(std::mem::size_of::<pg_sys::OpExpr>()) as *mut pg_sys::OpExpr;
            (*op).xpr.type_ = pg_sys::NodeTag::T_OpExpr;
            (*op).opno = opno;
            (*op).opresulttype = result_oid;
            (*op).args = args;
            (*op).location = -1;
            op as *mut pg_sys::Node
        }
    }

    #[pg_test]
    fn walk_chain_shape_simple_extract() {
        unsafe {
            let var = make_jsonb_var() as *mut pg_sys::Node;
            let expr = make_op_expr(var, "kind", JSONB_OBJECT_FIELD_TEXT_OPNO, pg_sys::TEXTOID);
            let shape = walk_chain_shape(expr).expect("recognized");
            assert_eq!(shape.keys, vec!["kind".to_string()]);
            assert_eq!(shape.leaf_kind, ColumnKind::Text);
            assert_eq!((*shape.inner_var).varattno, 1);
        }
    }

    #[pg_test]
    fn walk_chain_shape_nested_path() {
        // `((var -> 'a') -> 'b') ->> 'c'`
        unsafe {
            let var = make_jsonb_var() as *mut pg_sys::Node;
            let step_a = make_op_expr(var, "a", JSONB_OBJECT_FIELD_OPNO, pg_sys::JSONBOID);
            let step_b = make_op_expr(step_a, "b", JSONB_OBJECT_FIELD_OPNO, pg_sys::JSONBOID);
            let step_c = make_op_expr(step_b, "c", JSONB_OBJECT_FIELD_TEXT_OPNO, pg_sys::TEXTOID);
            let shape = walk_chain_shape(step_c).expect("recognized");
            assert_eq!(
                shape.keys,
                vec!["a".to_string(), "b".to_string(), "c".to_string()]
            );
            assert_eq!(shape.leaf_kind, ColumnKind::Text);
        }
    }

    #[pg_test]
    fn walk_chain_shape_with_outer_cast() {
        // `(var ->> 'n')::int4` — CoerceViaIO wrapper around the ->> chain.
        unsafe {
            let var = make_jsonb_var() as *mut pg_sys::Node;
            let inner = make_op_expr(var, "n", JSONB_OBJECT_FIELD_TEXT_OPNO, pg_sys::TEXTOID);
            let coerce = pg_sys::palloc0(std::mem::size_of::<pg_sys::CoerceViaIO>())
                as *mut pg_sys::CoerceViaIO;
            (*coerce).xpr.type_ = pg_sys::NodeTag::T_CoerceViaIO;
            (*coerce).arg = inner as *mut pg_sys::Expr;
            (*coerce).resulttype = pg_sys::INT4OID;
            (*coerce).location = -1;
            let shape = walk_chain_shape(coerce as *mut pg_sys::Node).expect("recognized");
            assert_eq!(shape.keys, vec!["n".to_string()]);
            assert_eq!(shape.leaf_kind, ColumnKind::Int32);
        }
    }

    #[pg_test]
    fn walk_chain_shape_rejects_bare_var() {
        unsafe {
            let var = make_jsonb_var();
            assert!(walk_chain_shape(var as *mut pg_sys::Node).is_none());
        }
    }

    #[pg_test]
    fn walk_chain_shape_rejects_no_terminal_text_arrow() {
        // `var -> 'k'` alone (no terminal `->>`) is not a recognized chain.
        unsafe {
            let var = make_jsonb_var() as *mut pg_sys::Node;
            let only_obj_field = make_op_expr(var, "k", JSONB_OBJECT_FIELD_OPNO, pg_sys::JSONBOID);
            assert!(walk_chain_shape(only_obj_field).is_none());
        }
    }

    #[pg_test]
    fn walk_chain_shape_rejects_null_node() {
        unsafe {
            assert!(walk_chain_shape(std::ptr::null_mut()).is_none());
        }
    }
}
