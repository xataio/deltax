//! PARALLEL MIXED path — multi-threaded segment processing for grouped
//! aggregates with text or mixed-type GROUP BY keys (where the COMPACT
//! path's "all-numeric needed cols" precondition doesn't hold).
//!
//! Two pieces live here together because the dispatch body is the only
//! consumer of the helper section:
//!
//! - **Worker helpers** (`process_segments_mixed`,
//!   `hash_mixed_key`, `MixedKey*`, `try_build_preselected`,
//!   `ParallelMixedConfig` / `Result`, `can_parallel_mixed`, etc.) —
//!   pure-Rust decompression + text-aware accumulator updates, safe to
//!   call off-thread. Several helpers (`can_parallel_mixed`,
//!   `is_text_group_col`, `numeric_col_used_only_by_constant_group_keys`)
//!   are also called from `begin_agg_scan`'s setup phase + the serial
//!   paths, so they stay `pub(super)`.
//! - **`dispatch_parallel_mixed_path`** — owns the worker scope,
//!   pipeline detoast, speculative top-N path, partitioned merge, and
//!   the final `AggScanState` build.
//!
//! The caller keeps the gate computation (rust_regex_infos compilation,
//! `mixed_col_not_null`, the `can_parallel_mixed_flag` check) so spec
//! and storage ownership transfers cleanly into this consuming dispatch.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::time::Instant;

use pgrx::pg_sys;

use super::super::batch_qual::{BatchCompareOp, BatchQual, evaluate_batch_quals};
use super::super::datum_utils::{collation_strcmp, string_to_datum};
use super::super::segments::{
    MetadataInfo, SegmentData, SegmentQualResult, classify_segment_quals_numeric,
    detoast_lazy_blobs, segment_skippable_by_dict, take_scan_buf_stats,
};
use super::super::text_col::{
    SegTextColumn, TextQualInfo, apply_text_eq_filter, apply_text_in_filter,
    apply_text_like_filter, decompress_length_sidecar, decompress_text_to_seg_col, strcoll_cmp,
};
use super::cd_set::hash128_str;
use super::extract::{constant_extract_key_for_segment, eval_extract};
use super::keys::CompactGroupMap;
use super::parallel_compact::{decompress_numeric_blob, is_numeric_type, parse_string_to_datum};
use super::regex::{RustRegexInfo, apply_case_when_to_seg_col, apply_regex_to_seg_col};
use super::state::{
    AggExecSpec, AggExpr, AggScanState, AggType, CaseWhenSpec, CaseWhenValue, GroupByColSpec,
    GroupByExpr, HavingFilter, HavingOp, OutputEntry,
};
use super::{
    CompactAccKind, CompactAccLayout, CompactAccStorage, CountDistinctSideCar, DictDistinctRemap,
    StringArena, build_dict_distinct_remaps, can_use_compact_accs, compact_finalize,
    compact_topn_select, datum_to_f64, datum_to_i128, i128_to_numeric_datum,
};

/// Compute a 128-bit hash of mixed integer and string group keys.
/// Uses two independent AHasher instances (different seeds) to produce two 64-bit
/// halves, giving collision probability ~2^-128.
pub(super) fn hash_mixed_key(ints: &[i64], strs: &[Option<&str>]) -> u128 {
    use std::hash::BuildHasher;
    let s1 = ahash::RandomState::with_seeds(
        0xc4a1_b2e3_d4f5_6789,
        0xa1b2_c3d4_e5f6_7890,
        0x1122_3344_5566_7788,
        0x99aa_bbcc_ddee_ff00,
    );
    let s2 = ahash::RandomState::with_seeds(
        0x1234_abcd_5678_ef01,
        0xaabb_ccdd_eeff_0011,
        0xfed0_cba9_8765_4321,
        0x0011_2233_4455_6677,
    );
    let mut h1 = s1.build_hasher();
    let mut h2 = s2.build_hasher();
    for &v in ints {
        v.hash(&mut h1);
        v.hash(&mut h2);
    }
    for s in strs {
        match s {
            Some(s) => {
                0u8.hash(&mut h1);
                s.hash(&mut h1);
                0u8.hash(&mut h2);
                s.hash(&mut h2);
            }
            None => {
                1u8.hash(&mut h1);
                1u8.hash(&mut h2);
            }
        }
    }
    ((h1.finish() as u128) << 64) | (h2.finish() as u128)
}

/// Value stored per group key component in MixedKeyStorage.
#[derive(Debug, Clone, Copy)]
pub(super) enum MixedKeyVal {
    Null,
    Int(i64),
    Str(u32, u32), // (offset, len) into arena
}

/// Per-worker side table mapping group_idx → actual key values.
/// Needed because the u128 hash is one-way — we need original values at finalization.
pub(super) struct MixedKeyStorage {
    pub(super) arena: StringArena,
    /// Flat storage: group i's key components are at keys[i * n_keys .. (i+1) * n_keys]
    pub(super) keys: Vec<MixedKeyVal>,
    pub(super) n_keys: usize,
}

impl MixedKeyStorage {
    pub(super) fn new(n_keys: usize) -> Self {
        MixedKeyStorage {
            arena: StringArena::new(),
            keys: Vec::new(),
            n_keys,
        }
    }

    /// Store key values for a new group. Must be called in order (group 0, 1, 2, ...).
    fn insert(&mut self, ints: &[i64], strs: &[Option<&str>], group_specs: &[GroupByColSpec]) {
        let mut int_idx = 0;
        let mut str_idx = 0;
        for gs in group_specs {
            if is_text_group_col(gs) {
                let s = strs[str_idx];
                str_idx += 1;
                match s {
                    Some(s) => {
                        let (off, len) = self.arena.alloc(s);
                        self.keys.push(MixedKeyVal::Str(off, len));
                    }
                    None => self.keys.push(MixedKeyVal::Null),
                }
            } else {
                self.keys.push(MixedKeyVal::Int(ints[int_idx]));
                int_idx += 1;
            }
        }
    }

    /// Get a key component for a group.
    #[inline]
    pub(super) fn get(&self, group_idx: u32, col: usize) -> MixedKeyVal {
        self.keys[group_idx as usize * self.n_keys + col]
    }
}

// strcoll_cmp is now in text_col.rs

/// Check if a GROUP BY column is a text type (including RegexpReplace/CaseWhen which produce text).
pub(super) fn is_text_group_col(gs: &GroupByColSpec) -> bool {
    let is_text_type = matches!(
        gs.type_oid,
        pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID | pg_sys::NAMEOID
    );
    match &gs.expr {
        GroupByExpr::Column | GroupByExpr::RegexpReplace { .. } | GroupByExpr::CaseWhen(_) => {
            is_text_type
        }
        _ => false,
    }
}

pub(super) fn case_when_references_col(spec: &CaseWhenSpec, col_idx: usize) -> bool {
    spec.clauses.iter().any(|clause| {
        clause.conditions.iter().any(|cond| cond.col_idx == col_idx)
            || matches!(&clause.result, CaseWhenValue::ColumnRef(ci) if *ci == col_idx)
    }) || matches!(&spec.default, CaseWhenValue::ColumnRef(ci) if *ci == col_idx)
}

pub(super) fn numeric_col_used_only_by_constant_group_keys(
    col_idx: usize,
    group_specs: &[GroupByColSpec],
    const_group_keys: &[Option<i64>],
    batch_quals: &[BatchQual],
    agg_specs: &[AggExecSpec],
) -> bool {
    if batch_quals.iter().any(|bq| bq.col_idx == col_idx) {
        return false;
    }
    if agg_specs
        .iter()
        .any(|spec| spec.col_idx >= 0 && spec.col_idx as usize == col_idx)
    {
        return false;
    }

    let mut saw_const_group_ref = false;
    for (gi, gs) in group_specs.iter().enumerate() {
        if gs.col_idx >= 0 && gs.col_idx as usize == col_idx {
            if const_group_keys.get(gi).and_then(|v| *v).is_some() {
                saw_const_group_ref = true;
            } else {
                return false;
            }
        }
        if let GroupByExpr::CaseWhen(ref spec) = gs.expr
            && case_when_references_col(spec, col_idx)
        {
            return false;
        }
    }

    saw_const_group_ref
}

pub(super) struct ParallelMixedConfig<'a> {
    pub(super) agg_specs: &'a [AggExecSpec],
    pub(super) group_specs: &'a [GroupByColSpec],
    pub(super) col_names: &'a [String],
    pub(super) col_types: &'a [pg_sys::Oid],
    pub(super) segment_by: &'a [String],
    /// Persisted `_col_idx` map (see `MetadataInfo.blob_idx`). Required by
    /// `segment_skippable_by_dict` so it can find each queried column's
    /// blob slot without re-counting from segment_by names.
    pub(super) blob_idx: &'a [Option<u16>],
    /// Pre-computed missing-value datums for columns added after this
    /// partition was compressed (see `MetadataInfo.missing_values`).
    pub(super) missing_values: &'a [Option<(pg_sys::Datum, bool)>],
    pub(super) needed_cols: &'a [bool],
    pub(super) batch_quals: &'a [BatchQual],
    pub(super) seg_filters: &'a [(usize, String)],
    pub(super) time_min: Option<i64>,
    pub(super) time_max: Option<i64>,
    pub(super) topn_spec: Option<(usize, usize, bool)>,
    /// Which needed_cols indices are text GROUP BY columns
    pub(super) text_group_col_flags: &'a [bool],
    /// Which needed_cols indices have text WHERE quals (EQ/NE/LIKE)
    pub(super) text_qual_infos: &'a [TextQualInfo],
    /// Compiled Rust regex info for RegexpReplace GROUP BY columns
    pub(super) rust_regex_infos: &'a [RustRegexInfo],
    /// Per-column flag: true when the text column is loaded in sidecar-only
    /// mode (length blob instead of main blob). Parallel to col_names.
    pub(super) sidecar_only_cols: &'a [bool],
    /// F8 optimization: when `Some`, filter phase-1 rows by group-key
    /// hash. Only rows whose `hash_mixed_key` output is in this set are
    /// inserted into the per-worker map; all others are skipped. This
    /// bounds each worker's map to `|preselected|` entries instead of
    /// the full group cardinality. Set iff the bare-LIMIT shape matches
    /// (no ORDER BY, no HAVING, no WHERE) and the Phase-0 probe
    /// succeeded in finding `bare_limit` distinct keys.
    pub(super) preselected_keys: Option<&'a hashbrown::HashSet<u128>>,
    /// Phase D: leader-precomputed dict-distinct remaps. Keyed by spec_idx
    /// for every CountDistinct(text) spec where every segment is dict-encoded
    /// for the col AND the global-string count is below the bitset threshold.
    /// Workers consult this to set bits in per-(spec, group) `Bitset`s
    /// (`CdKind::DictBitset`) instead of hashing strings into `HashSet<u128>`.
    /// Specs absent from the map keep the existing HashSet path. The
    /// chunk-offset arg to `process_segments_mixed` indexes into
    /// `per_segment` so each worker resolves `(seg_idx, local_dict_id)` →
    /// `global_id` without further coordination.
    pub(super) dict_distinct_remaps: &'a std::collections::HashMap<usize, DictDistinctRemap>,
}

// SAFETY: see equivalent impl on `ParallelCompactConfig` for the
// `missing_values` Datum-pointer lifetime argument.
unsafe impl Send for ParallelMixedConfig<'_> {}
unsafe impl Sync for ParallelMixedConfig<'_> {}

// TextQualInfo is now in text_col.rs

/// Result of parallel mixed aggregation from one worker thread.
pub(super) struct ParallelMixedResult {
    pub(super) compact_map: CompactGroupMap,
    pub(super) compact_storage: CompactAccStorage,
    pub(super) mixed_keys: MixedKeyStorage,
    pub(super) cd_sidecar: CountDistinctSideCar,
    pub(super) segments_processed: u64,
    pub(super) rows_processed: u64,
    pub(super) decompress_us: u64,
    pub(super) topk: Option<(Vec<u128>, i64)>,
}

// decompress_text_to_seg_col is now in text_col.rs

// apply_text_eq_filter and apply_text_like_filter are now in text_col.rs

/// Check if a query can use the parallel mixed (int+string) aggregation path.
pub(super) fn can_parallel_mixed(
    group_specs: &[GroupByColSpec],
    needed_cols: &[bool],
    col_types: &[pg_sys::Oid],
    col_not_null: &[bool],
    batch_quals: &[BatchQual],
    agg_specs: &[AggExecSpec],
) -> bool {
    if group_specs.len() > 6 {
        return false;
    }

    // No-GROUP-BY aggregates are allowed: workers funnel every row into a
    // single constant-key group (`hash_mixed_key(&[], &[])`). The text
    // requirement below still applies, so this only claims queries the
    // serial path would otherwise scan one-threaded (e.g.
    // `SELECT COUNT(*) WHERE url LIKE '%x%'`). The dispatch gate in
    // callbacks.rs additionally requires batch quals for the no-GROUP-BY
    // shape so unfiltered aggregates keep their existing paths.

    // Must have at least one text column involved (GROUP BY, qual, or agg) to justify
    // the mixed path instead of the compact path.
    let has_text_group = group_specs.iter().any(is_text_group_col);
    let has_text_qual = batch_quals.iter().any(|bq| {
        let t = bq.type_oid;
        t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID
    });
    let has_text_agg = agg_specs.iter().any(|s| {
        let t = s.col_type_oid;
        s.expr_kind == AggExpr::LengthOf
            || (matches!(
                s.agg_type,
                AggType::Min | AggType::Max | AggType::CountDistinct
            ) && (t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID))
    });
    if !has_text_group && !has_text_qual && !has_text_agg {
        return false;
    }

    // All GROUP BY columns must be either text columns (including RegexpReplace)
    // or integer-producing expressions
    for gs in group_specs {
        if is_text_group_col(gs) {
            continue; // text column (or RegexpReplace with Rust regex) — OK
        }
        // Must be an integer-producing expression.
        // The mixed per-row loop bails on NULL int keys (see has_null check
        // in process_segments_mixed): hash_mixed_key / MixedKeyStorage have
        // no NULL slot for the int side, so NULL groups would collapse into
        // Int(0). Require attnotnull for any numeric column we'd read, same
        // gate as can_use_compact_keys.
        match &gs.expr {
            GroupByExpr::Column => {
                let t = gs.type_oid;
                if !(t == pg_sys::INT2OID
                    || t == pg_sys::INT4OID
                    || t == pg_sys::INT8OID
                    || t == pg_sys::TIMESTAMPOID
                    || t == pg_sys::TIMESTAMPTZOID)
                {
                    return false;
                }
                if !col_not_null
                    .get(gs.col_idx as usize)
                    .copied()
                    .unwrap_or(false)
                {
                    return false;
                }
            }
            GroupByExpr::DateTrunc { .. }
            | GroupByExpr::Extract { .. }
            | GroupByExpr::AddConst { .. } => {
                if !col_not_null
                    .get(gs.col_idx as usize)
                    .copied()
                    .unwrap_or(false)
                {
                    return false;
                }
            }
            GroupByExpr::RegexpReplace { .. } | GroupByExpr::CaseWhen(_) => return false, // non-text RegexpReplace/CaseWhen not supported
        }
    }

    // Accumulators must be compact-compatible
    if !can_use_compact_accs(agg_specs) {
        return false;
    }

    // All non-text needed columns must be numeric (for thread-safe decompression)
    for (i, (&needed, &type_oid)) in needed_cols.iter().zip(col_types.iter()).enumerate() {
        if !needed {
            continue;
        }
        if is_numeric_type(type_oid) {
            continue; // numeric — fine
        }
        // Text column — must be a GROUP BY column, have a supported text qual,
        // or be used in a MIN/MAX aggregation
        let is_text_gb = group_specs
            .iter()
            .any(|gs| gs.col_idx as usize == i && is_text_group_col(gs));
        // Also check if this column is referenced by a CaseWhen result ColumnRef
        let is_case_when_ref = group_specs.iter().any(|gs| {
            if let GroupByExpr::CaseWhen(ref spec) = gs.expr {
                spec.clauses
                    .iter()
                    .any(|c| matches!(&c.result, CaseWhenValue::ColumnRef(ci) if *ci == i))
                    || matches!(&spec.default, CaseWhenValue::ColumnRef(ci) if *ci == i)
            } else {
                false
            }
        });
        let has_text_qual = batch_quals.iter().any(|bq| {
            bq.col_idx == i
                && ((matches!(bq.op, BatchCompareOp::Eq | BatchCompareOp::Ne)
                    && bq.text_const.is_some())
                    || (matches!(bq.op, BatchCompareOp::Like | BatchCompareOp::NotLike)
                        && bq.like_strategy.is_some())
                    || (bq.op == BatchCompareOp::InList && bq.in_list_text.is_some()))
        });
        let is_text_minmax_agg = agg_specs.iter().any(|s| {
            s.col_idx as usize == i
                && matches!(s.agg_type, AggType::Min | AggType::Max)
                && (type_oid == pg_sys::TEXTOID
                    || type_oid == pg_sys::VARCHAROID
                    || type_oid == pg_sys::BPCHAROID)
        });
        let is_text_cd_agg = agg_specs.iter().any(|s| {
            s.col_idx as usize == i
                && s.agg_type == AggType::CountDistinct
                && (type_oid == pg_sys::TEXTOID
                    || type_oid == pg_sys::VARCHAROID
                    || type_oid == pg_sys::BPCHAROID)
        });
        let is_text_length_agg = agg_specs.iter().any(|s| {
            s.col_idx as usize == i
                && s.expr_kind == AggExpr::LengthOf
                && (type_oid == pg_sys::TEXTOID
                    || type_oid == pg_sys::VARCHAROID
                    || type_oid == pg_sys::BPCHAROID)
        });
        if !is_text_gb
            && !is_case_when_ref
            && !has_text_qual
            && !is_text_minmax_agg
            && !is_text_cd_agg
            && !is_text_length_agg
        {
            return false; // unsupported column type
        }
    }

    // Text batch quals must be EQ/NE with text_const, LIKE/NotLike with
    // like_strategy, or InList with in_list_text.
    for bq in batch_quals {
        let t = bq.type_oid;
        if t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID {
            match bq.op {
                BatchCompareOp::Eq | BatchCompareOp::Ne => {
                    if bq.text_const.is_none() {
                        return false;
                    }
                }
                BatchCompareOp::Like | BatchCompareOp::NotLike => {
                    if bq.like_strategy.is_none() {
                        return false;
                    }
                }
                BatchCompareOp::InList => {
                    if bq.in_list_text.is_none() {
                        return false;
                    }
                }
                _ => return false, // unsupported text comparison
            }
        }
    }

    true
}

/// Process a chunk of segments on a worker thread using the mixed (int+string) path.
/// F8 Phase 0 — try to extract `bare_limit` distinct hashed group keys by
/// scanning up to `max_probe_segments` segments. Returns Some(set) iff the
/// set reaches `bare_limit` entries; None otherwise (caller falls back to
/// the normal full-agg path). Mirrors the decompression + key-building logic
/// of `process_segments_mixed` but without aggregation, WHERE filtering, or
/// top-N tracking. Caller must have gated this on: no WHERE clause, no
/// HAVING, no CaseWhen / RegexpReplace group-by expression.
#[allow(clippy::too_many_arguments)]
pub(super) fn try_build_preselected(
    bare_limit: usize,
    segments: &[SegmentData],
    group_specs: &[GroupByColSpec],
    col_names: &[String],
    col_types: &[pg_sys::Oid],
    segment_by: &[String],
    needed_cols: &[bool],
    text_group_col_flags: &[bool],
    max_probe_segments: usize,
) -> Option<hashbrown::HashSet<u128>> {
    if bare_limit == 0 {
        return None;
    }

    // Bail on group-by expressions the probe doesn't support.
    for gs in group_specs {
        match gs.expr {
            GroupByExpr::Column
            | GroupByExpr::AddConst { .. }
            | GroupByExpr::DateTrunc { .. }
            | GroupByExpr::Extract { .. } => {}
            _ => return None,
        }
    }

    let n_int_keys = group_specs
        .iter()
        .filter(|gs| !is_text_group_col(gs))
        .count();
    let n_str_keys = group_specs
        .iter()
        .filter(|gs| is_text_group_col(gs))
        .count();

    let mut keys: hashbrown::HashSet<u128> = hashbrown::HashSet::with_capacity(bare_limit.max(16));

    let probe_budget = max_probe_segments.min(segments.len());

    for seg in segments.iter().take(probe_budget) {
        if seg.row_count == 0 {
            continue;
        }

        // Decompress GROUP BY columns for this segment only.
        let mut numeric_cols: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
        let mut text_seg_cols: Vec<Option<SegTextColumn>> = Vec::new();
        let mut blob_idx = 0;
        let mut seg_val_idx = 0;

        for (col_idx, col_name) in col_names.iter().enumerate() {
            let type_oid = col_types[col_idx];

            if !needed_cols[col_idx] {
                if segment_by.contains(col_name) {
                    seg_val_idx += 1;
                } else {
                    blob_idx += 1;
                }
                numeric_cols.push(Vec::new());
                text_seg_cols.push(None);
                continue;
            }

            if segment_by.contains(col_name) {
                if text_group_col_flags[col_idx] {
                    let val = &seg.segment_values[seg_val_idx];
                    text_seg_cols.push(Some(SegTextColumn::SegBy(val.clone())));
                    numeric_cols.push(Vec::new());
                } else {
                    let val = &seg.segment_values[seg_val_idx];
                    let (datum, is_null) = match val {
                        Some(s) => (parse_string_to_datum(s, type_oid), false),
                        None => (pg_sys::Datum::from(0usize), true),
                    };
                    let repeated: Vec<(pg_sys::Datum, bool)> =
                        (0..seg.row_count).map(|_| (datum, is_null)).collect();
                    numeric_cols.push(repeated);
                    text_seg_cols.push(None);
                }
                seg_val_idx += 1;
            } else {
                // Guard against an under-sized blob vector (shouldn't happen
                // in practice, but Phase 0 runs very early — skip this
                // segment rather than panic).
                if blob_idx >= seg.compressed_blobs.len() {
                    return None;
                }
                let blob = &seg.compressed_blobs[blob_idx];
                if text_group_col_flags[col_idx] {
                    text_seg_cols.push(decompress_text_to_seg_col(blob));
                    numeric_cols.push(Vec::new());
                } else if is_numeric_type(type_oid) {
                    numeric_cols.push(decompress_numeric_blob(blob, type_oid));
                    text_seg_cols.push(None);
                } else {
                    // Unexpected type — bail cleanly.
                    return None;
                }
                blob_idx += 1;
            }
        }

        let row_count = seg.row_count as usize;
        let mut int_keys_buf = vec![0i64; n_int_keys];
        let mut str_keys_buf: Vec<Option<&str>> = vec![None; n_str_keys];

        for row in 0..row_count {
            let mut has_null = false;
            let mut int_idx = 0;
            let mut str_idx = 0;
            for gs in group_specs {
                if is_text_group_col(gs) {
                    let col_idx = gs.col_idx as usize;
                    str_keys_buf[str_idx] = text_seg_cols[col_idx]
                        .as_ref()
                        .and_then(|sc| sc.get_str(row));
                    str_idx += 1;
                } else {
                    let col = &numeric_cols[gs.col_idx as usize];
                    if col.is_empty() || col[row].1 {
                        has_null = true;
                        break;
                    }
                    int_keys_buf[int_idx] = match &gs.expr {
                        GroupByExpr::DateTrunc { unit_usecs, .. } => {
                            let pg_usec = col[row].0.value() as i64;
                            pg_usec.div_euclid(*unit_usecs) * *unit_usecs
                        }
                        GroupByExpr::Extract { unit, divisor, .. } => {
                            eval_extract(col[row].0.value() as i64, *divisor, unit)
                        }
                        GroupByExpr::AddConst { offset, .. } => col[row].0.value() as i64 + offset,
                        GroupByExpr::Column => col[row].0.value() as i64,
                        _ => unreachable!(),
                    };
                    int_idx += 1;
                }
            }
            if has_null {
                continue;
            }

            let hash_key = hash_mixed_key(&int_keys_buf[..n_int_keys], &str_keys_buf[..n_str_keys]);
            keys.insert(hash_key);
            if keys.len() >= bare_limit {
                return Some(keys);
            }
        }
    }

    // Probe exhausted without reaching bare_limit distinct keys.
    None
}

pub(super) fn process_segments_mixed(
    segments: &[SegmentData],
    chunk_offset: usize,
    config: &ParallelMixedConfig,
) -> ParallelMixedResult {
    let mut compact_map = CompactGroupMap::with_hasher(BuildHasherDefault::default());
    let mut compact_storage = CompactAccStorage::new(CompactAccLayout::new(config.agg_specs));
    let num_group_keys = config.group_specs.len();
    let mut mixed_keys = MixedKeyStorage::new(num_group_keys);
    // Phase D: classify each CountDistinct(text) spec as DictBitset when the
    // leader pre-pass produced a remap for it. Bitset size = global string
    // count for the column. Sized lookup map kept on the stack — at most a
    // handful of CountDistinct specs per query.
    let dict_remap_sizes: std::collections::HashMap<usize, u32> = config
        .dict_distinct_remaps
        .iter()
        .map(|(&spec_idx, remap)| (spec_idx, remap.global_count))
        .collect();
    let mut cd_sidecar =
        CountDistinctSideCar::new_with_dict_remaps(config.agg_specs, &dict_remap_sizes);
    let mut segments_processed: u64 = 0;
    let mut rows_processed: u64 = 0;
    let mut decompress_us: u64 = 0;

    // Count int and str group keys
    let n_int_keys = config
        .group_specs
        .iter()
        .filter(|gs| !is_text_group_col(gs))
        .count();
    let n_str_keys = config
        .group_specs
        .iter()
        .filter(|gs| is_text_group_col(gs))
        .count();

    for (rel_idx, seg) in segments.iter().enumerate() {
        // Phase D: absolute seg_idx into all_segments (and into each
        // dict_distinct_remaps.per_segment). The chunk-mode `for seg in segments`
        // doesn't expose this; we shadow it via `(rel_idx, seg)` so the bitset
        // lookup at the per-row insert site can resolve `(spec_idx, seg_idx,
        // local_id) → global_id` without further coordination.
        let seg_idx_in_all = chunk_offset + rel_idx;
        if seg.row_count == 0 {
            continue;
        }

        // Segment-by pruning
        if !config.seg_filters.is_empty() {
            let mut skip = false;
            for &(seg_val_idx, ref filter_val) in config.seg_filters {
                match &seg.segment_values[seg_val_idx] {
                    Some(val) if val == filter_val => {}
                    _ => {
                        skip = true;
                        break;
                    }
                }
            }
            if skip {
                continue;
            }
        }

        // Time-range pruning
        if let (Some(seg_min), Some(seg_max)) = (seg.min_time, seg.max_time) {
            if config.time_min.is_some_and(|query_min| seg_max < query_min) {
                continue;
            }
            if config.time_max.is_some_and(|query_max| seg_min > query_max) {
                continue;
            }
        }

        // Dictionary-based LIKE pruning
        if segment_skippable_by_dict(config.batch_quals, config.blob_idx, &seg.compressed_blobs) {
            continue;
        }

        // C.3 per-segment fast path: classify against the **numeric subset**
        // of batch_quals using col_minmax / nonzero_count. NonePass on any
        // numeric qual rules out the segment (text quals can only narrow
        // further); AllPass lets us skip the per-row numeric eval below
        // while still applying text quals on top of an empty selection.
        // No-op when batch_quals has no numeric entries (helper returns
        // Ambiguous in that case).
        let numeric_quals_all_pass = if config.batch_quals.is_empty() {
            true
        } else {
            match classify_segment_quals_numeric(seg, config.batch_quals, config.col_names) {
                SegmentQualResult::NonePass => continue,
                SegmentQualResult::AllPass => true,
                SegmentQualResult::Ambiguous => false,
            }
        };

        segments_processed += 1;

        let mut const_group_keys: Vec<Option<i64>> = vec![None; config.group_specs.len()];
        for (gi, gs) in config.group_specs.iter().enumerate() {
            if is_text_group_col(gs) {
                continue;
            }
            let GroupByExpr::Extract { unit, divisor, .. } = &gs.expr else {
                continue;
            };
            let col_idx = gs.col_idx as usize;
            let Some(col_name) = config.col_names.get(col_idx) else {
                continue;
            };
            let Some(cm) = seg.col_minmax.get(col_name) else {
                continue;
            };
            const_group_keys[gi] = constant_extract_key_for_segment(cm, *divisor, unit);
        }

        let skip_numeric_decompress: Vec<bool> = (0..config.col_names.len())
            .map(|col_idx| {
                numeric_col_used_only_by_constant_group_keys(
                    col_idx,
                    config.group_specs,
                    &const_group_keys,
                    config.batch_quals,
                    config.agg_specs,
                )
            })
            .collect();

        // Decompress needed columns. `config.blob_idx[col_idx]` is the
        // persisted `_col_idx` slot in `compressed_blobs`; `None` means
        // either the column is `segment_by` (value in `_meta`) or was
        // added after this partition was compressed (synthesize from
        // `config.missing_values[col_idx]`).
        let t_dec = Instant::now();
        let mut numeric_cols: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
        let mut text_seg_cols: Vec<Option<SegTextColumn>> = Vec::new();
        let mut seg_val_idx = 0;

        for (col_idx, col_name) in config.col_names.iter().enumerate() {
            let type_oid = config.col_types[col_idx];
            let is_segment_by = config.segment_by.contains(col_name);

            if !config.needed_cols[col_idx] {
                if is_segment_by {
                    seg_val_idx += 1;
                }
                numeric_cols.push(Vec::new());
                text_seg_cols.push(None);
                continue;
            }

            if is_segment_by {
                if config.text_group_col_flags[col_idx] {
                    // Text segment-by column
                    let val = &seg.segment_values[seg_val_idx];
                    text_seg_cols.push(Some(SegTextColumn::SegBy(val.clone())));
                    numeric_cols.push(Vec::new());
                } else {
                    // Numeric segment-by column
                    let val = &seg.segment_values[seg_val_idx];
                    let (datum, is_null) = match val {
                        Some(s) => (parse_string_to_datum(s, type_oid), false),
                        None => (pg_sys::Datum::from(0usize), true),
                    };
                    let repeated: Vec<(pg_sys::Datum, bool)> =
                        (0..seg.row_count).map(|_| (datum, is_null)).collect();
                    numeric_cols.push(repeated);
                    text_seg_cols.push(None);
                }
                seg_val_idx += 1;
            } else if let Some(slot) = config.blob_idx[col_idx] {
                let slot = slot as usize;
                if config
                    .sidecar_only_cols
                    .get(col_idx)
                    .copied()
                    .unwrap_or(false)
                {
                    // Text column in sidecar-only mode: main blob wasn't loaded;
                    // decode the length sidecar instead.
                    let sidecar_blob = &seg.text_length_blobs[slot];
                    text_seg_cols.push(decompress_length_sidecar(sidecar_blob));
                    numeric_cols.push(Vec::new());
                } else {
                    let blob = &seg.compressed_blobs[slot];
                    if config.text_group_col_flags[col_idx] {
                        // Text GROUP BY column — decompress to SegTextColumn
                        text_seg_cols.push(decompress_text_to_seg_col(blob));
                        numeric_cols.push(Vec::new());
                    } else if skip_numeric_decompress[col_idx] {
                        numeric_cols.push(Vec::new());
                        text_seg_cols.push(None);
                    } else if is_numeric_type(type_oid) {
                        // Numeric column
                        numeric_cols.push(decompress_numeric_blob(blob, type_oid));
                        text_seg_cols.push(None);
                    } else {
                        // Text column needed only for WHERE qual (not GROUP BY)
                        text_seg_cols.push(decompress_text_to_seg_col(blob));
                        numeric_cols.push(Vec::new());
                    }
                }
            } else {
                // Column added after this partition was compressed —
                // synthesize from the pre-computed missing value.
                let (datum, is_null) = config
                    .missing_values
                    .get(col_idx)
                    .copied()
                    .flatten()
                    .unwrap_or((pg_sys::Datum::from(0usize), true));
                let repeated: Vec<(pg_sys::Datum, bool)> =
                    (0..seg.row_count).map(|_| (datum, is_null)).collect();
                numeric_cols.push(repeated);
                text_seg_cols.push(None);
            }
        }
        // Build regex-transformed text columns for GROUP BY keys (separate from originals,
        // since the original column may also be needed for aggregation like MIN/AVG/length)
        let regex_text_cols: Vec<Option<SegTextColumn>> = if config.rust_regex_infos.is_empty() {
            Vec::new()
        } else {
            let mut cols: Vec<Option<SegTextColumn>> =
                (0..config.col_names.len()).map(|_| None).collect();
            for ri in config.rust_regex_infos {
                if let Some(ref seg_col) = text_seg_cols[ri.col_idx] {
                    cols[ri.col_idx] =
                        Some(apply_regex_to_seg_col(seg_col, &ri.regex, &ri.replacement));
                }
            }
            cols
        };

        decompress_us += t_dec.elapsed().as_micros() as u64;

        let row_count = seg.row_count as usize;

        // Build selection vector from quals
        // First: numeric batch quals — skip the per-row eval when C.3
        // metadata classification already proved every row passes the
        // numeric subset.
        let mut selection = if numeric_quals_all_pass {
            Vec::new()
        } else {
            evaluate_batch_quals(&numeric_cols, row_count, config.batch_quals, Vec::new())
        };

        // Then: text quals (applied on SegTextColumn, short-circuiting via selection)
        for tqi in config.text_qual_infos {
            match tqi {
                TextQualInfo::EqNe {
                    col_idx,
                    const_str,
                    is_ne,
                } => {
                    if let Some(ref seg_col) = text_seg_cols[*col_idx] {
                        apply_text_eq_filter(seg_col, const_str, *is_ne, row_count, &mut selection);
                    }
                }
                TextQualInfo::Like {
                    col_idx,
                    strategy,
                    negate,
                } => {
                    if let Some(ref seg_col) = text_seg_cols[*col_idx] {
                        apply_text_like_filter(
                            seg_col,
                            strategy,
                            *negate,
                            row_count,
                            &mut selection,
                        );
                    }
                }
                TextQualInfo::InList { col_idx, values } => {
                    if let Some(ref seg_col) = text_seg_cols[*col_idx] {
                        apply_text_in_filter(seg_col, values, row_count, &mut selection);
                    }
                }
            }
        }
        // Early skip: if all rows are filtered out, skip aggregation for this segment
        if !selection.is_empty() && !selection.iter().any(|&b| b) {
            continue;
        }

        // Build CaseWhen-transformed text columns (indexed by group spec index)
        let case_when_text_cols: Vec<Option<SegTextColumn>> = config
            .group_specs
            .iter()
            .map(|gs| {
                if let GroupByExpr::CaseWhen(ref spec) = gs.expr {
                    Some(apply_case_when_to_seg_col(
                        spec,
                        &numeric_cols,
                        &text_seg_cols,
                        row_count,
                        &selection,
                    ))
                } else {
                    None
                }
            })
            .collect();

        // Aggregation loop
        let mut int_keys = vec![0i64; n_int_keys];
        let mut str_keys: Vec<Option<&str>> = vec![None; n_str_keys];

        // Dict-aware fast path: a single dictionary-encoded text group key
        // (plain Column or a RegexpReplace whose transformed column is a Dict),
        // no int keys, no F8 preselect. Routes each row through a cached
        // dict-entry -> group index so the per-row hash + map probe collapses to
        // once per *distinct* dict entry per segment; repeated values (the common
        // case for dict columns) become a Vec index lookup. Reset per segment.
        let dict_fast: Option<&SegTextColumn> = if n_int_keys == 0
            && n_str_keys == 1
            && config.group_specs.len() == 1
            && config.preselected_keys.is_none()
        {
            let gs = &config.group_specs[0];
            let sc = match &gs.expr {
                GroupByExpr::Column => text_seg_cols[gs.col_idx as usize].as_ref(),
                GroupByExpr::RegexpReplace { .. } if !regex_text_cols.is_empty() => {
                    regex_text_cols[gs.col_idx as usize].as_ref()
                }
                _ => None,
            };
            match sc {
                Some(c @ SegTextColumn::Dict { .. }) => Some(c),
                _ => None,
            }
        } else {
            None
        };
        let mut dict_gidx_cache: Vec<u32> = match dict_fast {
            Some(SegTextColumn::Dict { entries, .. }) => vec![u32::MAX; entries.len()],
            _ => Vec::new(),
        };
        let mut dict_null_gidx: u32 = u32::MAX;

        for row in 0..row_count {
            if !selection.is_empty() && !selection[row] {
                continue;
            }
            rows_processed += 1;

            // Build key components
            let mut has_null = false;
            let mut int_idx = 0;
            let mut str_idx = 0;
            for (gi, gs) in config.group_specs.iter().enumerate() {
                if is_text_group_col(gs) {
                    // CaseWhen: use pre-computed column indexed by group spec index
                    if matches!(gs.expr, GroupByExpr::CaseWhen(_)) {
                        if let Some(Some(seg_col)) = case_when_text_cols.get(gi) {
                            str_keys[str_idx] = seg_col.get_str(row);
                        } else {
                            str_keys[str_idx] = None;
                        }
                        str_idx += 1;
                        continue;
                    }
                    let col_idx = gs.col_idx as usize;
                    // For RegexpReplace columns, use the pre-transformed column;
                    // for plain text columns, use the original
                    let seg_col_ref = if matches!(gs.expr, GroupByExpr::RegexpReplace { .. })
                        && !regex_text_cols.is_empty()
                    {
                        regex_text_cols[col_idx].as_ref()
                    } else {
                        text_seg_cols[col_idx].as_ref()
                    };
                    match seg_col_ref {
                        Some(seg_col) => {
                            str_keys[str_idx] = seg_col.get_str(row);
                            // NULL text key: leave str_keys[str_idx] as None.
                            // hash_mixed_key and MixedKeyStorage handle None correctly,
                            // producing a NULL group (matching PostgreSQL GROUP BY semantics).
                        }
                        None => {
                            str_keys[str_idx] = None;
                        }
                    }
                    str_idx += 1;
                } else {
                    if let Some(v) = const_group_keys[gi] {
                        int_keys[int_idx] = v;
                    } else {
                        let col = &numeric_cols[gs.col_idx as usize];
                        if col.is_empty() || col[row].1 {
                            has_null = true;
                            break;
                        }
                        int_keys[int_idx] = match &gs.expr {
                            GroupByExpr::DateTrunc { unit_usecs, .. } => {
                                let pg_usec = col[row].0.value() as i64;
                                pg_usec.div_euclid(*unit_usecs) * *unit_usecs
                            }
                            GroupByExpr::Extract { unit, divisor, .. } => {
                                eval_extract(col[row].0.value() as i64, *divisor, unit)
                            }
                            GroupByExpr::AddConst { offset, .. } => {
                                col[row].0.value() as i64 + offset
                            }
                            GroupByExpr::Column => col[row].0.value() as i64,
                            _ => unreachable!(),
                        };
                    }
                    int_idx += 1;
                }
            }

            if has_null {
                continue;
            }

            let group_idx = if let Some(SegTextColumn::Dict {
                entries,
                row_to_entry,
            }) = dict_fast
            {
                // Dict-aware fast path: resolve the group index from the cached
                // dict-entry -> group-index map; only hash + probe the global map
                // the first time each dict entry is seen this segment.
                let e = row_to_entry[row];
                if e == u32::MAX {
                    if dict_null_gidx == u32::MAX {
                        let hash_key = hash_mixed_key(&[], &[None]);
                        dict_null_gidx = match compact_map.entry(hash_key) {
                            hashbrown::hash_map::Entry::Occupied(en) => *en.get(),
                            hashbrown::hash_map::Entry::Vacant(en) => {
                                let idx = compact_storage.alloc_group();
                                cd_sidecar.alloc_group();
                                en.insert(idx);
                                mixed_keys.insert(&[], &[None], config.group_specs);
                                idx
                            }
                        };
                    }
                    dict_null_gidx
                } else {
                    let cached = dict_gidx_cache[e as usize];
                    if cached != u32::MAX {
                        cached
                    } else {
                        let s = entries[e as usize].as_str();
                        let hash_key = hash_mixed_key(&[], &[Some(s)]);
                        let gidx = match compact_map.entry(hash_key) {
                            hashbrown::hash_map::Entry::Occupied(en) => *en.get(),
                            hashbrown::hash_map::Entry::Vacant(en) => {
                                let idx = compact_storage.alloc_group();
                                cd_sidecar.alloc_group();
                                en.insert(idx);
                                mixed_keys.insert(&[], &[Some(s)], config.group_specs);
                                idx
                            }
                        };
                        dict_gidx_cache[e as usize] = gidx;
                        gidx
                    }
                }
            } else {
                let hash_key = hash_mixed_key(&int_keys[..n_int_keys], &str_keys[..n_str_keys]);

                // F8: when a preselected key set is supplied, skip rows whose
                // group-key hash is not in the set. The set is bounded to
                // `bare_limit` entries (typically 10), so the probe is an
                // L1-resident hashbrown lookup (~5 ns). Rows that hit proceed
                // to the normal Entry path below; rows that miss contribute
                // nothing to any output group.
                if let Some(preselected) = config.preselected_keys
                    && !preselected.contains(&hash_key)
                {
                    continue;
                }

                // Lookup or insert group
                if compact_map.len() == compact_map.capacity() {
                    let cap = compact_map.capacity();
                    if cap >= 32_000_000 {
                        compact_map.reserve(8_000_000);
                    }
                }
                match compact_map.entry(hash_key) {
                    hashbrown::hash_map::Entry::Occupied(e) => *e.get(),
                    hashbrown::hash_map::Entry::Vacant(e) => {
                        let idx = compact_storage.alloc_group();
                        cd_sidecar.alloc_group();
                        e.insert(idx);
                        // Store actual key values for this new group
                        mixed_keys.insert(
                            &int_keys[..n_int_keys],
                            &str_keys[..n_str_keys],
                            config.group_specs,
                        );
                        idx
                    }
                }
            };

            // Update compact accumulators (same as compact path)
            for (spec_idx, spec) in config.agg_specs.iter().enumerate() {
                let (_, kind) = compact_storage.layout.slots[spec_idx];
                match kind {
                    CompactAccKind::Count => {
                        match spec.agg_type {
                            AggType::CountStar => {
                                compact_storage.incr_count(group_idx, spec_idx, 1);
                            }
                            AggType::Count => {
                                let col = &numeric_cols[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    compact_storage.incr_count(group_idx, spec_idx, 1);
                                } else {
                                    // Check text columns for COUNT(text_col)
                                    if let Some(ref seg_col) = text_seg_cols[spec.col_idx as usize]
                                        && seg_col.get_str(row).is_some()
                                    {
                                        compact_storage.incr_count(group_idx, spec_idx, 1);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    CompactAccKind::SumInt => {
                        let col = &numeric_cols[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = datum_to_i128(col[row].0, spec.col_type_oid);
                            let sum_delta = if spec.expr_kind == AggExpr::AddConst {
                                v + spec.const_offset as i128
                            } else {
                                v
                            };
                            compact_storage.add_sum_int(group_idx, spec_idx, sum_delta, 1);
                        } else if spec.expr_kind == AggExpr::LengthOf
                            && let Some(ref seg_col) = text_seg_cols[spec.col_idx as usize]
                            && let Some(len) = seg_col.get_len(row)
                        {
                            compact_storage.add_sum_int(group_idx, spec_idx, len as i128, 1);
                        }
                    }
                    CompactAccKind::SumIntNarrow => {
                        let col = &numeric_cols[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = col[row].0.value() as i64;
                            let sum_delta = if spec.expr_kind == AggExpr::AddConst {
                                v + spec.const_offset
                            } else {
                                v
                            };
                            compact_storage.add_sum_int_narrow(group_idx, spec_idx, sum_delta, 1);
                        } else if spec.expr_kind == AggExpr::LengthOf
                            && let Some(ref seg_col) = text_seg_cols[spec.col_idx as usize]
                            && let Some(len) = seg_col.get_len(row)
                        {
                            compact_storage.add_sum_int_narrow(group_idx, spec_idx, len as i64, 1);
                        }
                    }
                    CompactAccKind::SumFloat => {
                        let col = &numeric_cols[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = datum_to_f64(col[row].0, spec.col_type_oid);
                            let sum_delta = if spec.expr_kind == AggExpr::AddConst {
                                v + spec.const_offset as f64
                            } else {
                                v
                            };
                            compact_storage.add_sum_float(group_idx, spec_idx, sum_delta, 1);
                        } else if spec.expr_kind == AggExpr::LengthOf
                            && let Some(ref seg_col) = text_seg_cols[spec.col_idx as usize]
                            && let Some(len) = seg_col.get_len(row)
                        {
                            compact_storage.add_sum_float(group_idx, spec_idx, len as f64, 1);
                        }
                    }
                    CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                        let col_idx = spec.col_idx as usize;
                        if let Some(ref seg_col) = text_seg_cols[col_idx]
                            && let Some(s) = seg_col.get_str(row)
                        {
                            let (cur_off, cur_len) =
                                compact_storage.read_min_max_str(group_idx, spec_idx);
                            let should_update = if cur_off == u32::MAX {
                                true // no current value
                            } else {
                                let cur = compact_storage.str_arena.get(cur_off, cur_len);
                                let cmp = strcoll_cmp(s, cur);
                                match kind {
                                    CompactAccKind::MinStr => cmp == std::cmp::Ordering::Less,
                                    CompactAccKind::MaxStr => cmp == std::cmp::Ordering::Greater,
                                    _ => unreachable!(),
                                }
                            };
                            if should_update {
                                let (new_off, new_len) = compact_storage.str_arena.alloc(s);
                                compact_storage
                                    .write_min_max_str(group_idx, spec_idx, new_off, new_len);
                            }
                        }
                    }
                    CompactAccKind::MinInt => {
                        let col = &numeric_cols[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = col[row].0.value() as i64;
                            compact_storage.update_min_int(group_idx, spec_idx, v);
                        }
                    }
                    CompactAccKind::MaxInt => {
                        let col = &numeric_cols[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = col[row].0.value() as i64;
                            compact_storage.update_max_int(group_idx, spec_idx, v);
                        }
                    }
                    CompactAccKind::CountDistinctInt => {
                        let col = &numeric_cols[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            cd_sidecar.insert_int(spec_idx, group_idx, col[row].0.value() as i64);
                        }
                    }
                    CompactAccKind::CountDistinctStr => {
                        let col_idx = spec.col_idx as usize;
                        let Some(ref seg_col) = text_seg_cols[col_idx] else {
                            continue;
                        };
                        if let Some(remap) = config.dict_distinct_remaps.get(&spec_idx) {
                            // Phase D bitset path: per-row work is local
                            // dict_id → global_id → set bit. No hashing or
                            // string materialisation. Eligibility is gated
                            // at the leader pre-pass — this branch only
                            // fires when every segment is dict-encoded for
                            // `col_idx` and the column's global cardinality
                            // fit under PHASE_D_MAX_GLOBAL_FOR_BITSET.
                            if let Some(local_id) = seg_col.dict_local_id(row) {
                                let seg_remap = &remap.per_segment[seg_idx_in_all];
                                let global_id = seg_remap[local_id as usize];
                                cd_sidecar.insert_dict_global(spec_idx, group_idx, global_id);
                            }
                        } else if let Some(s) = seg_col.get_str(row) {
                            cd_sidecar.insert_str(spec_idx, group_idx, hash128_str(s.as_bytes()));
                        }
                    }
                }
            }
        }
    }

    // Write CountDistinct counts into compact storage for top-K sorting
    if !cd_sidecar.is_empty() {
        cd_sidecar.write_counts_to_storage(&mut compact_storage, &compact_map);
    }

    // Compute top-K candidates while data is cache-hot (if requested)
    let topk = config.topn_spec.map(|(sort_slot, k, ascending)| {
        let (_, sort_kind) = compact_storage.layout.slots[sort_slot];
        let read_val = |gidx: u32| -> i64 {
            match sort_kind {
                CompactAccKind::Count => compact_storage.read_count(gidx, sort_slot),
                CompactAccKind::SumIntNarrow => {
                    compact_storage.read_sum_int_narrow(gidx, sort_slot).0
                }
                _ => compact_storage.read_count(gidx, sort_slot),
            }
        };

        if compact_map.len() <= k {
            let keys: Vec<u128> = compact_map.keys().copied().collect();
            return (keys, 0i64);
        }

        if !ascending {
            let mut heap: BinaryHeap<Reverse<(i64, u128)>> = BinaryHeap::with_capacity(k + 1);
            for (&key, &gidx) in &compact_map {
                let val = read_val(gidx);
                heap.push(Reverse((val, key)));
                if heap.len() > k {
                    heap.pop();
                }
            }
            let floor = heap.peek().map(|&Reverse((v, _))| v).unwrap_or(0);
            let keys: Vec<u128> = heap.into_iter().map(|Reverse((_, k))| k).collect();
            (keys, floor)
        } else {
            let mut heap: BinaryHeap<(i64, u128)> = BinaryHeap::with_capacity(k + 1);
            for (&key, &gidx) in &compact_map {
                let val = read_val(gidx);
                heap.push((val, key));
                if heap.len() > k {
                    heap.pop();
                }
            }
            let floor = heap.peek().map(|&(v, _)| v).unwrap_or(0);
            let keys: Vec<u128> = heap.into_iter().map(|(_, k)| k).collect();
            (keys, floor)
        }
    });

    ParallelMixedResult {
        compact_map,
        compact_storage,
        mixed_keys,
        cd_sidecar,
        segments_processed,
        rows_processed,
        decompress_us,
        topk,
    }
}

/// Read-only inputs threaded through the merge-phase sub-paths inside
/// `dispatch_parallel_mixed_path`. Built once after the worker scope
/// finishes (so the timing/counter fields are frozen).
struct MixedMergeCtx<'a> {
    output_map: &'a [OutputEntry],
    having_filters: &'a [HavingFilter],
    where_quals: *mut pg_sys::List,
    topn_limit: i64,
    topn_sort_col: usize,
    topn_ascending: bool,
    bare_limit: i64,
    batch_quals: &'a [BatchQual],
    n_workers: usize,
    num_result_cols: usize,
    has_group_by: bool,
    metadata_us: u64,
    heap_scan_us: u64,
    total_detoast_us: u64,
    total_cache_hits: u64,
    total_cache_misses: u64,
    total_cache_bytes_served: u64,
    decompress_us: u64,
    agg_us: u64,
    total_segments: u64,
    total_rows_processed: u64,
    t_wall: Instant,
    /// Size of `preselected_keys.as_ref()` at the time of construction;
    /// `0` when the dispatch didn't compute a preselected set. Used by
    /// `mixed_bare_limit` for the `f8_preselected` debug field.
    preselected_count: u64,
}

/// Bare-LIMIT short-circuit for the mixed path. Picks N groups from the
/// largest worker, copies their key bytes into a fresh `MixedKeyStorage`,
/// targeted-merges each key's accumulators across workers, finalizes.
///
/// # Safety
///
/// Inherits the `CompactAccStorage` accessor contract. Must run inside
/// an active PG transaction (finalize allocates datums).
#[inline]
unsafe fn mixed_bare_limit(
    ctx: &MixedMergeCtx<'_>,
    agg_specs: Vec<AggExecSpec>,
    group_specs: Vec<GroupByColSpec>,
    partial_results: &[ParallelMixedResult],
) -> AggScanState {
    unsafe {
        let n = ctx.bare_limit as usize;
        let t_merge = Instant::now();

        // Pick the largest worker
        let largest_idx = partial_results
            .iter()
            .enumerate()
            .max_by_key(|(_, r)| r.compact_map.len())
            .map(|(i, _)| i)
            .unwrap_or(0);

        // Collect first N group keys from largest worker
        let target_keys: Vec<u128> = partial_results[largest_idx]
            .compact_map
            .keys()
            .take(n)
            .copied()
            .collect();

        // Targeted merge: for each target key, merge accumulators from all workers
        let layout = CompactAccLayout {
            slots: partial_results[0].compact_storage.layout.slots.clone(),
            group_stride: partial_results[0].compact_storage.layout.group_stride,
        };
        let mut final_storage = CompactAccStorage::new(layout);
        let mut final_mixed_keys = MixedKeyStorage::new(group_specs.len());
        let mut final_cd_sidecar = CountDistinctSideCar::new(&agg_specs);

        for &key in &target_keys {
            let group_idx = final_storage.alloc_group();
            final_cd_sidecar.alloc_group();

            // Copy key values from the largest worker
            let src = &partial_results[largest_idx];
            let src_gidx = src.compact_map[&key];
            let n_cols = group_specs.len();
            for col in 0..n_cols {
                let kv = src.mixed_keys.get(src_gidx, col);
                match kv {
                    MixedKeyVal::Str(off, len) => {
                        let s = src.mixed_keys.arena.get(off, len);
                        let (new_off, new_len) = final_mixed_keys.arena.alloc(s);
                        final_mixed_keys
                            .keys
                            .push(MixedKeyVal::Str(new_off, new_len));
                    }
                    other => final_mixed_keys.keys.push(other),
                }
            }

            // Merge accumulators from all workers
            for result in partial_results {
                if let Some(&worker_gidx) = result.compact_map.get(&key) {
                    for (slot_idx, _) in agg_specs.iter().enumerate() {
                        let (_, kind) = final_storage.layout.slots[slot_idx];
                        match kind {
                            CompactAccKind::Count => {
                                let wc = result.compact_storage.read_count(worker_gidx, slot_idx);
                                final_storage.incr_count(group_idx, slot_idx, wc);
                            }
                            CompactAccKind::SumInt => {
                                let (ws, wc) =
                                    result.compact_storage.read_sum_int(worker_gidx, slot_idx);
                                final_storage.add_sum_int(group_idx, slot_idx, ws, wc);
                            }
                            CompactAccKind::SumIntNarrow => {
                                let (ws, wc) = result
                                    .compact_storage
                                    .read_sum_int_narrow(worker_gidx, slot_idx);
                                final_storage.add_sum_int_narrow(group_idx, slot_idx, ws, wc);
                            }
                            CompactAccKind::SumFloat => {
                                let (ws, wc) =
                                    result.compact_storage.read_sum_float(worker_gidx, slot_idx);
                                final_storage.add_sum_float(group_idx, slot_idx, ws, wc);
                            }
                            CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                let (w_off, w_len) = result
                                    .compact_storage
                                    .read_min_max_str(worker_gidx, slot_idx);
                                if w_off != u32::MAX {
                                    let w_str = result.compact_storage.str_arena.get(w_off, w_len);
                                    let (g_off, g_len) =
                                        final_storage.read_min_max_str(group_idx, slot_idx);
                                    let should_update = if g_off == u32::MAX {
                                        true
                                    } else {
                                        let g_str = final_storage.str_arena.get(g_off, g_len);
                                        let cmp = collation_strcmp(w_str, g_str);
                                        match kind {
                                            CompactAccKind::MinStr => cmp < 0,
                                            CompactAccKind::MaxStr => cmp > 0,
                                            _ => unreachable!(),
                                        }
                                    };
                                    if should_update {
                                        let w_str =
                                            result.compact_storage.str_arena.get(w_off, w_len);
                                        let (new_off, new_len) =
                                            final_storage.str_arena.alloc(w_str);
                                        final_storage.write_min_max_str(
                                            group_idx, slot_idx, new_off, new_len,
                                        );
                                    }
                                }
                            }
                            CompactAccKind::MinInt => {
                                let (w_val, w_has) = result
                                    .compact_storage
                                    .read_min_max_int(worker_gidx, slot_idx);
                                if w_has {
                                    final_storage.update_min_int(group_idx, slot_idx, w_val);
                                }
                            }
                            CompactAccKind::MaxInt => {
                                let (w_val, w_has) = result
                                    .compact_storage
                                    .read_min_max_int(worker_gidx, slot_idx);
                                if w_has {
                                    final_storage.update_max_int(group_idx, slot_idx, w_val);
                                }
                            }
                            CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                                final_cd_sidecar.union_from(
                                    slot_idx,
                                    group_idx,
                                    &result.cd_sidecar,
                                    worker_gidx,
                                );
                            }
                        }
                    }
                }
            }
        }

        // Write CountDistinct counts to storage
        if !final_cd_sidecar.is_empty() {
            for i in 0..target_keys.len() {
                let group_idx = i as u32;
                for e in &final_cd_sidecar.entries {
                    let count = e.count(group_idx);
                    final_storage.set_count(group_idx, e.spec_idx, count);
                }
            }
        }

        let merge_us = t_merge.elapsed().as_micros() as u64;

        // Finalize just N groups
        let pre_topn_groups: usize = partial_results.iter().map(|r| r.compact_map.len()).sum();
        let t_finalize = Instant::now();
        let mut result_rows = Vec::with_capacity(n);
        for (i, &_key) in target_keys.iter().enumerate() {
            let group_idx = i as u32;
            let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
            for (spec_idx, spec) in agg_specs.iter().enumerate() {
                agg_results.push(compact_finalize(&final_storage, group_idx, spec_idx, spec));
            }
            let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(ctx.num_result_cols);
            for entry in ctx.output_map {
                match entry {
                    OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                    OutputEntry::Group(gi) => {
                        let kv = final_mixed_keys.get(group_idx, *gi);
                        match kv {
                            MixedKeyVal::Int(v) => {
                                if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. }) {
                                    row.push((i128_to_numeric_datum(v as i128), false));
                                } else {
                                    row.push((pg_sys::Datum::from(v as usize), false));
                                }
                            }
                            MixedKeyVal::Str(off, len) => {
                                let s = final_mixed_keys.arena.get(off, len);
                                let datum = string_to_datum(s, group_specs[*gi].type_oid);
                                row.push((datum, false));
                            }
                            MixedKeyVal::Null => {
                                row.push((pg_sys::Datum::from(0usize), true));
                            }
                        }
                    }
                    OutputEntry::DerivedGroup { base_gi, delta } => {
                        match final_mixed_keys.get(group_idx, *base_gi) {
                            MixedKeyVal::Int(v) => {
                                row.push((pg_sys::Datum::from((v + delta) as usize), false))
                            }
                            _ => row.push((pg_sys::Datum::from(0usize), true)),
                        }
                    }
                    OutputEntry::Const(d, n) => row.push((*d, *n)),
                }
            }
            result_rows.push(row);
        }
        let finalize_us = t_finalize.elapsed().as_micros() as u64;

        AggScanState {
            _agg_specs: agg_specs,
            _group_specs: group_specs,
            result_rows,
            _num_result_cols: ctx.num_result_cols,
            metadata_us: ctx.metadata_us,
            heap_scan_us: ctx.heap_scan_us,
            detoast_us: ctx.total_detoast_us,
            blob_cache_hits: ctx.total_cache_hits,
            blob_cache_misses: ctx.total_cache_misses,
            blob_cache_bytes_served: ctx.total_cache_bytes_served,
            decompress_us: ctx.decompress_us,
            agg_us: ctx.agg_us,
            total_segments: ctx.total_segments,
            total_rows_processed: ctx.total_rows_processed,
            batch_quals_count: ctx.batch_quals.len(),
            where_quals_null: ctx.where_quals.is_null(),
            topn_sort_col: -1,
            topn_ascending: ctx.topn_ascending,
            pre_topn_groups: pre_topn_groups as u64,
            merge_us,
            finalize_us,
            n_workers: ctx.n_workers as u64,
            bare_limit: ctx.bare_limit,
            wall_us: ctx.t_wall.elapsed().as_micros() as u64,
            buf_stats: take_scan_buf_stats(),
            f8_preselected: ctx.preselected_count,
            ..AggScanState::default()
        }
    }
}

/// Full merge fallback for the mixed path. Adopts the largest worker
/// map as the base, merges remaining workers' entries (string keys via
/// `MixedKeyStorage` rather than packed u128), finalizes every group
/// with HAVING filtering. If top-N is active without a dedicated
/// optimization path, sorts the finalized rows in place and truncates.
///
/// # Safety
///
/// Inherits the `CompactAccStorage` accessor contract. Must run inside
/// an active PG transaction (finalize allocates datums).
#[inline]
unsafe fn mixed_full_merge(
    ctx: &MixedMergeCtx<'_>,
    agg_specs: Vec<AggExecSpec>,
    group_specs: Vec<GroupByColSpec>,
    mut partial_results: Vec<ParallelMixedResult>,
    compact_storage: &mut CompactAccStorage,
    compact_group_map: &mut CompactGroupMap,
) -> AggScanState {
    unsafe {
        let t_merge = Instant::now();

        let largest_idx = partial_results
            .iter()
            .enumerate()
            .max_by_key(|(_, r)| r.compact_map.len())
            .map(|(i, _)| i)
            .unwrap_or(0);
        let largest = partial_results.swap_remove(largest_idx);
        *compact_group_map = largest.compact_map;
        *compact_storage = largest.compact_storage;
        let mut merged_mixed_keys = largest.mixed_keys;
        let mut merged_cd_sidecar = largest.cd_sidecar;

        let remaining_entries: usize = partial_results.iter().map(|r| r.compact_map.len()).sum();
        compact_group_map.reserve(remaining_entries);

        let storage = &mut *compact_storage;
        for result in &partial_results {
            for (&hash_key, &worker_group_idx) in &result.compact_map {
                let global_group_idx = match compact_group_map.entry(hash_key) {
                    hashbrown::hash_map::Entry::Occupied(e) => *e.get(),
                    hashbrown::hash_map::Entry::Vacant(e) => {
                        let idx = storage.alloc_group();
                        merged_cd_sidecar.alloc_group();
                        e.insert(idx);
                        // Copy key values from this worker's MixedKeyStorage
                        let src_keys = &result.mixed_keys;
                        let n = group_specs.len();
                        for col in 0..n {
                            let kv = src_keys.get(worker_group_idx, col);
                            match kv {
                                MixedKeyVal::Str(off, len) => {
                                    let s = src_keys.arena.get(off, len);
                                    let (new_off, new_len) = merged_mixed_keys.arena.alloc(s);
                                    merged_mixed_keys
                                        .keys
                                        .push(MixedKeyVal::Str(new_off, new_len));
                                }
                                other => {
                                    merged_mixed_keys.keys.push(other);
                                }
                            }
                        }
                        idx
                    }
                };

                // Merge accumulators
                for (slot_idx, _) in agg_specs.iter().enumerate() {
                    let (_, kind) = storage.layout.slots[slot_idx];
                    match kind {
                        CompactAccKind::Count => {
                            let wc = result
                                .compact_storage
                                .read_count(worker_group_idx, slot_idx);
                            storage.incr_count(global_group_idx, slot_idx, wc);
                        }
                        CompactAccKind::SumInt => {
                            let (ws, wc) = result
                                .compact_storage
                                .read_sum_int(worker_group_idx, slot_idx);
                            storage.add_sum_int(global_group_idx, slot_idx, ws, wc);
                        }
                        CompactAccKind::SumIntNarrow => {
                            let (ws, wc) = result
                                .compact_storage
                                .read_sum_int_narrow(worker_group_idx, slot_idx);
                            storage.add_sum_int_narrow(global_group_idx, slot_idx, ws, wc);
                        }
                        CompactAccKind::SumFloat => {
                            let (ws, wc) = result
                                .compact_storage
                                .read_sum_float(worker_group_idx, slot_idx);
                            storage.add_sum_float(global_group_idx, slot_idx, ws, wc);
                        }
                        CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                            let (w_off, w_len) = result
                                .compact_storage
                                .read_min_max_str(worker_group_idx, slot_idx);
                            if w_off != u32::MAX {
                                let w_str = result.compact_storage.str_arena.get(w_off, w_len);
                                let (g_off, g_len) =
                                    storage.read_min_max_str(global_group_idx, slot_idx);
                                let should_update = if g_off == u32::MAX {
                                    true
                                } else {
                                    let g_str = storage.str_arena.get(g_off, g_len);
                                    let cmp = collation_strcmp(w_str, g_str);
                                    match kind {
                                        CompactAccKind::MinStr => cmp < 0,
                                        CompactAccKind::MaxStr => cmp > 0,
                                        _ => unreachable!(),
                                    }
                                };
                                if should_update {
                                    let w_str = result.compact_storage.str_arena.get(w_off, w_len);
                                    let (new_off, new_len) = storage.str_arena.alloc(w_str);
                                    storage.write_min_max_str(
                                        global_group_idx,
                                        slot_idx,
                                        new_off,
                                        new_len,
                                    );
                                }
                            }
                        }
                        CompactAccKind::MinInt => {
                            let (w_val, w_has) = result
                                .compact_storage
                                .read_min_max_int(worker_group_idx, slot_idx);
                            if w_has {
                                storage.update_min_int(global_group_idx, slot_idx, w_val);
                            }
                        }
                        CompactAccKind::MaxInt => {
                            let (w_val, w_has) = result
                                .compact_storage
                                .read_min_max_int(worker_group_idx, slot_idx);
                            if w_has {
                                storage.update_max_int(global_group_idx, slot_idx, w_val);
                            }
                        }
                        CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                            merged_cd_sidecar.union_from(
                                slot_idx,
                                global_group_idx,
                                &result.cd_sidecar,
                                worker_group_idx,
                            );
                        }
                    }
                }
            }
        }
        // Write final CountDistinct counts into compact storage
        if !merged_cd_sidecar.is_empty() {
            merged_cd_sidecar.write_counts_to_storage(storage, compact_group_map);
        }
        let merge_us = t_merge.elapsed().as_micros() as u64;

        // Finalize
        let pre_topn_groups = compact_group_map.len();
        let mut topn_select_us: u64 = 0;
        let t_finalize = Instant::now();

        // Helper closure to convert a group's keys to datums
        let finalize_mixed_group = |_hash_key: u128,
                                    group_idx: u32,
                                    storage: &CompactAccStorage,
                                    mixed_ks: &MixedKeyStorage|
         -> Vec<(pg_sys::Datum, bool)> {
            let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
            for (spec_idx, spec) in agg_specs.iter().enumerate() {
                agg_results.push(compact_finalize(storage, group_idx, spec_idx, spec));
            }
            let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(ctx.num_result_cols);
            for entry in ctx.output_map {
                match entry {
                    OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                    OutputEntry::Group(gi) => {
                        let kv = mixed_ks.get(group_idx, *gi);
                        match kv {
                            MixedKeyVal::Int(v) => {
                                if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. }) {
                                    row.push((i128_to_numeric_datum(v as i128), false));
                                } else {
                                    row.push((pg_sys::Datum::from(v as usize), false));
                                }
                            }
                            MixedKeyVal::Str(off, len) => {
                                let s = mixed_ks.arena.get(off, len);
                                let datum = string_to_datum(s, group_specs[*gi].type_oid);
                                row.push((datum, false));
                            }
                            MixedKeyVal::Null => {
                                row.push((pg_sys::Datum::from(0usize), true));
                            }
                        }
                    }
                    OutputEntry::DerivedGroup { base_gi, delta } => {
                        match mixed_ks.get(group_idx, *base_gi) {
                            MixedKeyVal::Int(v) => {
                                row.push((pg_sys::Datum::from((v + delta) as usize), false))
                            }
                            _ => row.push((pg_sys::Datum::from(0usize), true)),
                        }
                    }
                    OutputEntry::Const(d, n) => row.push((*d, *n)),
                }
            }
            row
        };

        let result_rows = if ctx.topn_limit > 0
            && ctx.having_filters.is_empty()
            && compact_group_map.len() > ctx.topn_limit as usize
        {
            let sort_slot = match ctx.output_map[ctx.topn_sort_col] {
                OutputEntry::Agg(ai) => ai,
                _ => unreachable!(),
            };
            let storage = &mut *compact_storage;
            let t_topn = Instant::now();
            let top_entries = compact_topn_select(
                compact_group_map,
                storage,
                sort_slot,
                ctx.topn_limit as usize,
                ctx.topn_ascending,
                agg_specs[sort_slot].agg_type == AggType::Avg,
            );
            topn_select_us = t_topn.elapsed().as_micros() as u64;
            let mut rows = Vec::with_capacity(top_entries.len());
            for &(hash_key, group_idx) in &top_entries {
                rows.push(finalize_mixed_group(
                    hash_key,
                    group_idx,
                    storage,
                    &merged_mixed_keys,
                ));
            }
            rows
        } else {
            let storage = &mut *compact_storage;
            let mut rows = Vec::new();
            'par_mixed_group_loop: for (_, &group_idx) in compact_group_map.iter() {
                let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                for (spec_idx, spec) in agg_specs.iter().enumerate() {
                    agg_results.push(compact_finalize(storage, group_idx, spec_idx, spec));
                }

                for hf in ctx.having_filters {
                    let (datum, is_null) = agg_results[hf.agg_idx];
                    if is_null {
                        continue 'par_mixed_group_loop;
                    }
                    let val = datum.value() as i64;
                    let pass = match hf.op {
                        HavingOp::Gt => val > hf.const_val,
                        HavingOp::Lt => val < hf.const_val,
                        HavingOp::Ge => val >= hf.const_val,
                        HavingOp::Le => val <= hf.const_val,
                        HavingOp::Eq => val == hf.const_val,
                        HavingOp::Ne => val != hf.const_val,
                    };
                    if !pass {
                        continue 'par_mixed_group_loop;
                    }
                }

                let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(ctx.num_result_cols);
                for entry in ctx.output_map {
                    match entry {
                        OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                        OutputEntry::Group(gi) => {
                            let kv = merged_mixed_keys.get(group_idx, *gi);
                            match kv {
                                MixedKeyVal::Int(v) => {
                                    if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. })
                                    {
                                        row.push((i128_to_numeric_datum(v as i128), false));
                                    } else {
                                        row.push((pg_sys::Datum::from(v as usize), false));
                                    }
                                }
                                MixedKeyVal::Str(off, len) => {
                                    let s = merged_mixed_keys.arena.get(off, len);
                                    let datum = string_to_datum(s, group_specs[*gi].type_oid);
                                    row.push((datum, false));
                                }
                                MixedKeyVal::Null => {
                                    row.push((pg_sys::Datum::from(0usize), true));
                                }
                            }
                        }
                        OutputEntry::DerivedGroup { base_gi, delta } => {
                            match merged_mixed_keys.get(group_idx, *base_gi) {
                                MixedKeyVal::Int(v) => {
                                    row.push((pg_sys::Datum::from((v + delta) as usize), false))
                                }
                                _ => row.push((pg_sys::Datum::from(0usize), true)),
                            }
                        }
                        OutputEntry::Const(d, n) => row.push((*d, *n)),
                    }
                }
                rows.push(row);
            }

            // Apply top-N on full result set (HAVING path or small groups)
            if ctx.topn_limit > 0 && ctx.has_group_by && rows.len() > ctx.topn_limit as usize {
                let si = ctx.topn_sort_col;
                if ctx.topn_ascending {
                    rows.sort_by_key(|row| {
                        let (datum, is_null) = row[si];
                        if is_null {
                            i64::MAX
                        } else {
                            datum.value() as i64
                        }
                    });
                } else {
                    rows.sort_by(|a, b| {
                        let (da, na) = a[si];
                        let (db, nb) = b[si];
                        let va = if na { i64::MIN } else { da.value() as i64 };
                        let vb = if nb { i64::MIN } else { db.value() as i64 };
                        vb.cmp(&va)
                    });
                }
                rows.truncate(ctx.topn_limit as usize);
            }
            rows
        };
        let finalize_us = t_finalize.elapsed().as_micros() as u64;

        AggScanState {
            _agg_specs: agg_specs,
            _group_specs: group_specs,
            result_rows,
            _num_result_cols: ctx.num_result_cols,
            metadata_us: ctx.metadata_us,
            heap_scan_us: ctx.heap_scan_us,
            detoast_us: ctx.total_detoast_us,
            blob_cache_hits: ctx.total_cache_hits,
            blob_cache_misses: ctx.total_cache_misses,
            blob_cache_bytes_served: ctx.total_cache_bytes_served,
            decompress_us: ctx.decompress_us,
            agg_us: ctx.agg_us,
            total_segments: ctx.total_segments,
            total_rows_processed: ctx.total_rows_processed,
            batch_quals_count: ctx.batch_quals.len(),
            where_quals_null: ctx.where_quals.is_null(),
            topn_limit: if ctx.topn_limit > 0 {
                ctx.topn_limit as u64
            } else {
                0
            },
            topn_sort_col: ctx.topn_sort_col as i64,
            topn_ascending: ctx.topn_ascending,
            pre_topn_groups: pre_topn_groups as u64,
            merge_us,
            finalize_us,
            topn_select_us,
            n_workers: ctx.n_workers as u64,
            wall_us: ctx.t_wall.elapsed().as_micros() as u64,
            buf_stats: take_scan_buf_stats(),
            ..AggScanState::default()
        }
    }
}

/// Result of one of the three top-N merge sub-paths inside
/// `dispatch_parallel_mixed_path`. The dispatch fn assembles a final
/// `AggScanState` from this via `build_mixed_topn_agg_scan_state`.
struct MixedMergeOutcome {
    result_rows: Vec<Vec<(pg_sys::Datum, bool)>>,
    pre_topn_groups: u64,
    merge_us: u64,
    finalize_us: u64,
    topn_select_us: u64,
}

#[inline]
fn build_mixed_topn_agg_scan_state(
    ctx: &MixedMergeCtx<'_>,
    agg_specs: Vec<AggExecSpec>,
    group_specs: Vec<GroupByColSpec>,
    outcome: MixedMergeOutcome,
) -> AggScanState {
    AggScanState {
        _agg_specs: agg_specs,
        _group_specs: group_specs,
        result_rows: outcome.result_rows,
        _num_result_cols: ctx.num_result_cols,
        metadata_us: ctx.metadata_us,
        heap_scan_us: ctx.heap_scan_us,
        detoast_us: ctx.total_detoast_us,
        blob_cache_hits: ctx.total_cache_hits,
        blob_cache_misses: ctx.total_cache_misses,
        blob_cache_bytes_served: ctx.total_cache_bytes_served,
        decompress_us: ctx.decompress_us,
        agg_us: ctx.agg_us,
        total_segments: ctx.total_segments,
        total_rows_processed: ctx.total_rows_processed,
        batch_quals_count: ctx.batch_quals.len(),
        where_quals_null: ctx.where_quals.is_null(),
        topn_limit: ctx.topn_limit as u64,
        topn_sort_col: ctx.topn_sort_col as i64,
        topn_ascending: ctx.topn_ascending,
        pre_topn_groups: outcome.pre_topn_groups,
        merge_us: outcome.merge_us,
        finalize_us: outcome.finalize_us,
        topn_select_us: outcome.topn_select_us,
        n_workers: ctx.n_workers as u64,
        wall_us: ctx.t_wall.elapsed().as_micros() as u64,
        buf_stats: take_scan_buf_stats(),
        ..AggScanState::default()
    }
}

/// Derived MIN/MAX-difference top-N (JSONBench Q4 shape): sorts groups
/// by `storage[max_slot] - storage[min_slot]`. Workers have produced
/// partial MAX/MIN per group; one pass over all (worker, hash) pairs
/// combines partials into per-key global MAX/MIN, applies a top-K heap
/// on `max - min`, then merges *only* the K winners' full accumulators.
///
/// Caller has already checked `derived_minmax_topn.is_some()` and
/// destructured the slot indices.
///
/// # Safety
///
/// Inherits the `CompactAccStorage` accessor contract. Must run inside
/// an active PG transaction (finalize allocates datums).
#[inline]
unsafe fn mixed_derived_minmax_topn(
    ctx: &MixedMergeCtx<'_>,
    agg_specs: Vec<AggExecSpec>,
    group_specs: Vec<GroupByColSpec>,
    partial_results: &[ParallelMixedResult],
    compact_storage: &mut CompactAccStorage,
    max_slot: usize,
    min_slot: usize,
) -> AggScanState {
    unsafe {
        let t_merge = Instant::now();
        let limit = ctx.topn_limit as usize;
        let ascending = ctx.topn_ascending;

        // Step 1+2: partition worker-local groups by hash, merge
        // each partition's MAX/MIN in parallel, and keep only local
        // top-K candidates. The old implementation built one large
        // leader HashMap and then scanned it; Q4 spends hundreds of
        // milliseconds there with ~1.35M distinct users.
        let n_partitions = ctx.n_workers.max(1);
        let mut buckets: Vec<Vec<(usize, u128, u32)>> =
            (0..n_partitions).map(|_| Vec::new()).collect();
        for (wi, result) in partial_results.iter().enumerate() {
            for (&hash_key, &wgidx) in &result.compact_map {
                let p = (((hash_key as u64) ^ ((hash_key >> 64) as u64)) as usize) % n_partitions;
                buckets[p].push((wi, hash_key, wgidx));
            }
        }

        let partition_results: Vec<(usize, Vec<(i128, u128)>)> = std::thread::scope(|s| {
            let workers = &partial_results;
            let handles: Vec<_> = buckets
                .into_iter()
                .map(|bucket| {
                    s.spawn(move || {
                        let mut per_key: hashbrown::HashMap<
                            u128,
                            (i64, i64, bool),
                            BuildHasherDefault<ahash::AHasher>,
                        > = hashbrown::HashMap::with_capacity_and_hasher(
                            bucket.len(),
                            Default::default(),
                        );

                        for (wi, hash_key, wgidx) in bucket {
                            let worker = &workers[wi];
                            let (max_val, max_has) =
                                worker.compact_storage.read_min_max_int(wgidx, max_slot);
                            let (min_val, min_has) =
                                worker.compact_storage.read_min_max_int(wgidx, min_slot);
                            let entry =
                                per_key
                                    .entry(hash_key)
                                    .or_insert((i64::MIN, i64::MAX, false));
                            if max_has && (!entry.2 || max_val > entry.0) {
                                entry.0 = max_val;
                                entry.2 = true;
                            }
                            if min_has && (entry.1 == i64::MAX || min_val < entry.1) {
                                entry.1 = min_val;
                            }
                        }

                        let unique_count = per_key.len();
                        if ascending {
                            let mut heap: std::collections::BinaryHeap<(i128, u128)> =
                                std::collections::BinaryHeap::with_capacity(limit + 1);
                            for (&hash_key, &(max_val, min_val, seen)) in &per_key {
                                if !seen {
                                    continue;
                                }
                                let derived = (max_val as i128).saturating_sub(min_val as i128);
                                heap.push((derived, hash_key));
                                if heap.len() > limit {
                                    heap.pop();
                                }
                            }
                            (unique_count, heap.into_vec())
                        } else {
                            let mut heap: std::collections::BinaryHeap<Reverse<(i128, u128)>> =
                                std::collections::BinaryHeap::with_capacity(limit + 1);
                            for (&hash_key, &(max_val, min_val, seen)) in &per_key {
                                if !seen {
                                    continue;
                                }
                                let derived = (max_val as i128).saturating_sub(min_val as i128);
                                heap.push(Reverse((derived, hash_key)));
                                if heap.len() > limit {
                                    heap.pop();
                                }
                            }
                            (
                                unique_count,
                                heap.into_iter()
                                    .map(|Reverse(candidate)| candidate)
                                    .collect(),
                            )
                        }
                    })
                })
                .collect();

            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        let pre_topn_groups: usize = partition_results
            .iter()
            .map(|(unique_count, _)| *unique_count)
            .sum();
        let mut heap: std::collections::BinaryHeap<Reverse<(i128, u128)>> =
            std::collections::BinaryHeap::with_capacity(limit + 1);
        let mut asc_heap: std::collections::BinaryHeap<(i128, u128)> =
            std::collections::BinaryHeap::with_capacity(limit + 1);
        for (_, candidates) in partition_results {
            for (derived, hash_key) in candidates {
                if ascending {
                    asc_heap.push((derived, hash_key));
                    if asc_heap.len() > limit {
                        asc_heap.pop();
                    }
                } else {
                    heap.push(Reverse((derived, hash_key)));
                    if heap.len() > limit {
                        heap.pop();
                    }
                }
            }
        }
        let winners: Vec<u128> = if ascending {
            asc_heap.into_iter().map(|(_, h)| h).collect()
        } else {
            heap.into_iter().map(|Reverse((_, h))| h).collect()
        };
        let topn_select_us = t_merge.elapsed().as_micros() as u64;

        // Step 3: full-merge accumulators for the K winners and
        // finalize. This is the existing speculative-path winner
        // merge — duplicated here rather than factored out to
        // keep this commit narrowly scoped.
        let t_fin = Instant::now();
        let storage = compact_storage;
        let mut result_rows = Vec::with_capacity(winners.len());
        let mut spec_cd_sidecar = CountDistinctSideCar::new(&agg_specs);

        for hash_key in winners {
            let global_idx = storage.alloc_group();
            spec_cd_sidecar.alloc_group();

            let mut key_source_worker: Option<usize> = None;
            for (wi, result) in partial_results.iter().enumerate() {
                if let Some(&worker_idx) = result.compact_map.get(&hash_key) {
                    if key_source_worker.is_none() {
                        key_source_worker = Some(wi);
                    }
                    for (slot_idx, _) in agg_specs.iter().enumerate() {
                        let (_, kind) = storage.layout.slots[slot_idx];
                        match kind {
                            CompactAccKind::Count => {
                                let wc = result.compact_storage.read_count(worker_idx, slot_idx);
                                storage.incr_count(global_idx, slot_idx, wc);
                            }
                            CompactAccKind::SumInt => {
                                let (ws, wc) =
                                    result.compact_storage.read_sum_int(worker_idx, slot_idx);
                                storage.add_sum_int(global_idx, slot_idx, ws, wc);
                            }
                            CompactAccKind::SumIntNarrow => {
                                let (ws, wc) = result
                                    .compact_storage
                                    .read_sum_int_narrow(worker_idx, slot_idx);
                                storage.add_sum_int_narrow(global_idx, slot_idx, ws, wc);
                            }
                            CompactAccKind::SumFloat => {
                                let (ws, wc) =
                                    result.compact_storage.read_sum_float(worker_idx, slot_idx);
                                storage.add_sum_float(global_idx, slot_idx, ws, wc);
                            }
                            CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                let (w_off, w_len) = result
                                    .compact_storage
                                    .read_min_max_str(worker_idx, slot_idx);
                                if w_off != u32::MAX {
                                    let w_str = result.compact_storage.str_arena.get(w_off, w_len);
                                    let (g_off, g_len) =
                                        storage.read_min_max_str(global_idx, slot_idx);
                                    let should_update = if g_off == u32::MAX {
                                        true
                                    } else {
                                        let g_str = storage.str_arena.get(g_off, g_len);
                                        let cmp = collation_strcmp(w_str, g_str);
                                        match kind {
                                            CompactAccKind::MinStr => cmp < 0,
                                            CompactAccKind::MaxStr => cmp > 0,
                                            _ => unreachable!(),
                                        }
                                    };
                                    if should_update {
                                        let w_str =
                                            result.compact_storage.str_arena.get(w_off, w_len);
                                        let (new_off, new_len) = storage.str_arena.alloc(w_str);
                                        storage.write_min_max_str(
                                            global_idx, slot_idx, new_off, new_len,
                                        );
                                    }
                                }
                            }
                            CompactAccKind::MinInt => {
                                let (w_val, w_has) = result
                                    .compact_storage
                                    .read_min_max_int(worker_idx, slot_idx);
                                if w_has {
                                    storage.update_min_int(global_idx, slot_idx, w_val);
                                }
                            }
                            CompactAccKind::MaxInt => {
                                let (w_val, w_has) = result
                                    .compact_storage
                                    .read_min_max_int(worker_idx, slot_idx);
                                if w_has {
                                    storage.update_max_int(global_idx, slot_idx, w_val);
                                }
                            }
                            CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                                spec_cd_sidecar.union_from(
                                    slot_idx,
                                    global_idx,
                                    &result.cd_sidecar,
                                    worker_idx,
                                );
                            }
                        }
                    }
                }
            }

            for e in &spec_cd_sidecar.entries {
                let count = e.count(global_idx);
                storage.set_count(global_idx, e.spec_idx, count);
            }

            let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
            for (spec_idx, spec) in agg_specs.iter().enumerate() {
                agg_results.push(compact_finalize(storage, global_idx, spec_idx, spec));
            }

            let source_wi = key_source_worker.unwrap();
            let source_gidx = *partial_results[source_wi]
                .compact_map
                .get(&hash_key)
                .unwrap();
            let mixed_ks = &partial_results[source_wi].mixed_keys;

            let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(ctx.num_result_cols);
            for entry in ctx.output_map {
                match entry {
                    OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                    OutputEntry::Group(gi) => {
                        let kv = mixed_ks.get(source_gidx, *gi);
                        match kv {
                            MixedKeyVal::Int(v) => {
                                if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. }) {
                                    row.push((i128_to_numeric_datum(v as i128), false));
                                } else {
                                    row.push((pg_sys::Datum::from(v as usize), false));
                                }
                            }
                            MixedKeyVal::Str(off, len) => {
                                let s = mixed_ks.arena.get(off, len);
                                let datum = string_to_datum(s, group_specs[*gi].type_oid);
                                row.push((datum, false));
                            }
                            MixedKeyVal::Null => {
                                row.push((pg_sys::Datum::from(0usize), true));
                            }
                        }
                    }
                    OutputEntry::DerivedGroup { base_gi, delta } => {
                        match mixed_ks.get(source_gidx, *base_gi) {
                            MixedKeyVal::Int(v) => {
                                row.push((pg_sys::Datum::from((v + delta) as usize), false))
                            }
                            _ => row.push((pg_sys::Datum::from(0usize), true)),
                        }
                    }
                    OutputEntry::Const(d, n) => row.push((*d, *n)),
                }
            }
            result_rows.push(row);
        }
        let finalize_us = t_fin.elapsed().as_micros() as u64;

        AggScanState {
            _agg_specs: agg_specs,
            _group_specs: group_specs,
            result_rows,
            _num_result_cols: ctx.num_result_cols,
            metadata_us: ctx.metadata_us,
            heap_scan_us: ctx.heap_scan_us,
            detoast_us: ctx.total_detoast_us,
            blob_cache_hits: ctx.total_cache_hits,
            blob_cache_misses: ctx.total_cache_misses,
            blob_cache_bytes_served: ctx.total_cache_bytes_served,
            decompress_us: ctx.decompress_us,
            agg_us: ctx.agg_us,
            total_segments: ctx.total_segments,
            total_rows_processed: ctx.total_rows_processed,
            batch_quals_count: ctx.batch_quals.len(),
            where_quals_null: ctx.where_quals.is_null(),
            topn_limit: ctx.topn_limit as u64,
            topn_sort_col: -3, // derived sentinel — see explain.rs
            topn_ascending: ctx.topn_ascending,
            pre_topn_groups: pre_topn_groups as u64,
            finalize_us,
            topn_select_us,
            n_workers: ctx.n_workers as u64,
            wall_us: ctx.t_wall.elapsed().as_micros() as u64,
            buf_stats: take_scan_buf_stats(),
            ..AggScanState::default()
        }
    }
}

/// Speculative top-N for the mixed path. Uses per-worker pre-computed
/// top-K candidates, merges only those, verifies no missed key could
/// beat the Nth result.
///
/// Returns `Some(outcome)` on success or all-tied; `None` on fallthrough
/// (not eligible / phase-2 too expensive / speculation failed without
/// ties). Caller falls through to partitioned/full merge.
///
/// # Safety
///
/// Inherits the `CompactAccStorage` accessor contract. Must run inside
/// an active PG transaction.
#[inline]
unsafe fn mixed_speculative_topn(
    ctx: &MixedMergeCtx<'_>,
    agg_specs: &[AggExecSpec],
    group_specs: &[GroupByColSpec],
    partial_results: &[ParallelMixedResult],
    compact_storage: &mut CompactAccStorage,
) -> Option<MixedMergeOutcome> {
    unsafe {
        let sort_slot_for_spec = match ctx.output_map[ctx.topn_sort_col] {
            OutputEntry::Agg(ai) => ai,
            _ => 0,
        };
        let sort_is_cd = ctx.topn_limit > 0
            && matches!(
                compact_storage.layout.slots[sort_slot_for_spec].1,
                CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr
            );
        let sort_is_avg =
            ctx.topn_limit > 0 && agg_specs[sort_slot_for_spec].agg_type == AggType::Avg;
        if ctx.topn_limit > 0 && ctx.having_filters.is_empty() && !sort_is_cd && !sort_is_avg {
            let sort_slot = sort_slot_for_spec;
            let (_, sort_kind) = compact_storage.layout.slots[sort_slot];
            let limit = ctx.topn_limit as usize;
            let k = (ctx.topn_limit as usize).max(1000);

            let read_sort = |storage: &CompactAccStorage, group_idx: u32| -> i64 {
                match sort_kind {
                    CompactAccKind::Count => storage.read_count(group_idx, sort_slot),
                    CompactAccKind::SumIntNarrow => {
                        storage.read_sum_int_narrow(group_idx, sort_slot).0
                    }
                    _ => storage.read_count(group_idx, sort_slot),
                }
            };

            let t_spec = Instant::now();

            // Phase 1: Collect pre-computed top-K candidates from workers
            let mut candidate_set: hashbrown::HashSet<u128, BuildHasherDefault<ahash::AHasher>> =
                hashbrown::HashSet::with_capacity_and_hasher(
                    k * partial_results.len(),
                    BuildHasherDefault::default(),
                );
            let mut floor_sum: i64 = 0;
            for result in partial_results {
                if let Some((keys, floor)) = &result.topk {
                    floor_sum = floor_sum.saturating_add(*floor);
                    for &key in keys {
                        candidate_set.insert(key);
                    }
                }
            }

            // Phase 2: For each candidate, sum sort values across all workers
            let mut merged: Vec<(i64, u128)> = Vec::with_capacity(candidate_set.len());
            for &key in &candidate_set {
                let mut total: i64 = 0;
                for result in partial_results {
                    if let Some(&gidx) = result.compact_map.get(&key) {
                        total = total.saturating_add(read_sort(&result.compact_storage, gidx));
                    }
                }
                merged.push((total, key));
            }

            // Phase 3: Sort and take top-N
            if !ctx.topn_ascending {
                merged.sort_unstable_by_key(|b| std::cmp::Reverse(b.0));
            } else {
                merged.sort_unstable_by_key(|a| a.0);
            }
            merged.truncate(limit);

            // Phase 4: Correctness check
            let speculative_ok = if merged.len() >= limit {
                let nth_value = merged[limit - 1].0;
                if !ctx.topn_ascending {
                    nth_value > floor_sum
                } else {
                    nth_value < floor_sum
                }
            } else {
                false
            };

            let topn_select_us = t_spec.elapsed().as_micros() as u64;

            if speculative_ok {
                // Phase 5: For each winner, merge accumulators and finalize
                let t_fin = Instant::now();
                let storage = compact_storage;
                let mut result_rows = Vec::with_capacity(merged.len());
                let mut spec_cd_sidecar = CountDistinctSideCar::new(agg_specs);

                // Find which worker has each winning key for MixedKeyStorage lookup
                for &(_, hash_key) in &merged {
                    let global_idx = storage.alloc_group();
                    spec_cd_sidecar.alloc_group();

                    // Targeted merge: only this key's accumulators across workers
                    let mut key_source_worker: Option<usize> = None;
                    for (wi, result) in partial_results.iter().enumerate() {
                        if let Some(&worker_idx) = result.compact_map.get(&hash_key) {
                            if key_source_worker.is_none() {
                                key_source_worker = Some(wi);
                            }
                            for (slot_idx, _) in agg_specs.iter().enumerate() {
                                let (_, kind) = storage.layout.slots[slot_idx];
                                match kind {
                                    CompactAccKind::Count => {
                                        let wc =
                                            result.compact_storage.read_count(worker_idx, slot_idx);
                                        storage.incr_count(global_idx, slot_idx, wc);
                                    }
                                    CompactAccKind::SumInt => {
                                        let (ws, wc) = result
                                            .compact_storage
                                            .read_sum_int(worker_idx, slot_idx);
                                        storage.add_sum_int(global_idx, slot_idx, ws, wc);
                                    }
                                    CompactAccKind::SumIntNarrow => {
                                        let (ws, wc) = result
                                            .compact_storage
                                            .read_sum_int_narrow(worker_idx, slot_idx);
                                        storage.add_sum_int_narrow(global_idx, slot_idx, ws, wc);
                                    }
                                    CompactAccKind::SumFloat => {
                                        let (ws, wc) = result
                                            .compact_storage
                                            .read_sum_float(worker_idx, slot_idx);
                                        storage.add_sum_float(global_idx, slot_idx, ws, wc);
                                    }
                                    CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                        let (w_off, w_len) = result
                                            .compact_storage
                                            .read_min_max_str(worker_idx, slot_idx);
                                        if w_off != u32::MAX {
                                            let w_str =
                                                result.compact_storage.str_arena.get(w_off, w_len);
                                            let (g_off, g_len) =
                                                storage.read_min_max_str(global_idx, slot_idx);
                                            let should_update = if g_off == u32::MAX {
                                                true
                                            } else {
                                                let g_str = storage.str_arena.get(g_off, g_len);
                                                let cmp = collation_strcmp(w_str, g_str);
                                                match kind {
                                                    CompactAccKind::MinStr => cmp < 0,
                                                    CompactAccKind::MaxStr => cmp > 0,
                                                    _ => unreachable!(),
                                                }
                                            };
                                            if should_update {
                                                let w_str = result
                                                    .compact_storage
                                                    .str_arena
                                                    .get(w_off, w_len);
                                                let (new_off, new_len) =
                                                    storage.str_arena.alloc(w_str);
                                                storage.write_min_max_str(
                                                    global_idx, slot_idx, new_off, new_len,
                                                );
                                            }
                                        }
                                    }
                                    CompactAccKind::MinInt => {
                                        let (w_val, w_has) = result
                                            .compact_storage
                                            .read_min_max_int(worker_idx, slot_idx);
                                        if w_has {
                                            storage.update_min_int(global_idx, slot_idx, w_val);
                                        }
                                    }
                                    CompactAccKind::MaxInt => {
                                        let (w_val, w_has) = result
                                            .compact_storage
                                            .read_min_max_int(worker_idx, slot_idx);
                                        if w_has {
                                            storage.update_max_int(global_idx, slot_idx, w_val);
                                        }
                                    }
                                    CompactAccKind::CountDistinctInt
                                    | CompactAccKind::CountDistinctStr => {
                                        spec_cd_sidecar.union_from(
                                            slot_idx,
                                            global_idx,
                                            &result.cd_sidecar,
                                            worker_idx,
                                        );
                                    }
                                }
                            }
                        }
                    }

                    // Write CountDistinct counts for this group
                    for e in &spec_cd_sidecar.entries {
                        let count = e.count(global_idx);
                        storage.set_count(global_idx, e.spec_idx, count);
                    }

                    // Finalize this group
                    let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                    for (spec_idx, spec) in agg_specs.iter().enumerate() {
                        agg_results.push(compact_finalize(storage, global_idx, spec_idx, spec));
                    }

                    // Get actual key values from the first worker that has this key
                    let source_wi = key_source_worker.unwrap();
                    let source_gidx = *partial_results[source_wi]
                        .compact_map
                        .get(&hash_key)
                        .unwrap();
                    let mixed_ks = &partial_results[source_wi].mixed_keys;

                    let mut row: Vec<(pg_sys::Datum, bool)> =
                        Vec::with_capacity(ctx.num_result_cols);
                    for entry in ctx.output_map {
                        match entry {
                            OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                            OutputEntry::Group(gi) => {
                                let kv = mixed_ks.get(source_gidx, *gi);
                                match kv {
                                    MixedKeyVal::Int(v) => {
                                        if matches!(
                                            group_specs[*gi].expr,
                                            GroupByExpr::Extract { .. }
                                        ) {
                                            row.push((i128_to_numeric_datum(v as i128), false));
                                        } else {
                                            row.push((pg_sys::Datum::from(v as usize), false));
                                        }
                                    }
                                    MixedKeyVal::Str(off, len) => {
                                        let s = mixed_ks.arena.get(off, len);
                                        let datum = string_to_datum(s, group_specs[*gi].type_oid);
                                        row.push((datum, false));
                                    }
                                    MixedKeyVal::Null => {
                                        row.push((pg_sys::Datum::from(0usize), true));
                                    }
                                }
                            }
                            OutputEntry::DerivedGroup { base_gi, delta } => {
                                match mixed_ks.get(source_gidx, *base_gi) {
                                    MixedKeyVal::Int(v) => {
                                        row.push((pg_sys::Datum::from((v + delta) as usize), false))
                                    }
                                    _ => row.push((pg_sys::Datum::from(0usize), true)),
                                }
                            }
                            OutputEntry::Const(d, n) => row.push((*d, *n)),
                        }
                    }
                    result_rows.push(row);
                }
                let finalize_us = t_fin.elapsed().as_micros() as u64;

                let pre_topn_groups: usize =
                    partial_results.iter().map(|r| r.compact_map.len()).sum();

                return Some(MixedMergeOutcome {
                    result_rows,
                    pre_topn_groups: pre_topn_groups as u64,
                    merge_us: 0,
                    finalize_us,
                    topn_select_us,
                });
            }
            // Speculation failed — check if all tied
            let nth_value = merged
                .get(limit.saturating_sub(1))
                .map(|x| x.0)
                .unwrap_or(0);
            let all_tied = merged.len() >= limit && merged.iter().all(|&(v, _)| v == nth_value);

            pgrx::log!(
                "pg_deltax mixed speculative top-N failed: candidates={} k={} floor_sum={} all_tied={}",
                merged.len(),
                k,
                floor_sum,
                all_tied,
            );

            if all_tied {
                merged.truncate(limit);

                let t_fin = Instant::now();
                let storage = compact_storage;
                let mut result_rows = Vec::with_capacity(merged.len());
                let mut spec_cd_sidecar = CountDistinctSideCar::new(agg_specs);

                for &(_, hash_key) in &merged {
                    let global_idx = storage.alloc_group();
                    spec_cd_sidecar.alloc_group();

                    let mut key_source_worker: Option<usize> = None;
                    for (wi, result) in partial_results.iter().enumerate() {
                        if let Some(&worker_idx) = result.compact_map.get(&hash_key) {
                            if key_source_worker.is_none() {
                                key_source_worker = Some(wi);
                            }
                            for (slot_idx, _) in agg_specs.iter().enumerate() {
                                let (_, kind) = storage.layout.slots[slot_idx];
                                match kind {
                                    CompactAccKind::Count => {
                                        let wc =
                                            result.compact_storage.read_count(worker_idx, slot_idx);
                                        storage.incr_count(global_idx, slot_idx, wc);
                                    }
                                    CompactAccKind::SumInt => {
                                        let (ws, wc) = result
                                            .compact_storage
                                            .read_sum_int(worker_idx, slot_idx);
                                        storage.add_sum_int(global_idx, slot_idx, ws, wc);
                                    }
                                    CompactAccKind::SumIntNarrow => {
                                        let (ws, wc) = result
                                            .compact_storage
                                            .read_sum_int_narrow(worker_idx, slot_idx);
                                        storage.add_sum_int_narrow(global_idx, slot_idx, ws, wc);
                                    }
                                    CompactAccKind::SumFloat => {
                                        let (ws, wc) = result
                                            .compact_storage
                                            .read_sum_float(worker_idx, slot_idx);
                                        storage.add_sum_float(global_idx, slot_idx, ws, wc);
                                    }
                                    CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                        let (w_off, w_len) = result
                                            .compact_storage
                                            .read_min_max_str(worker_idx, slot_idx);
                                        if w_off != u32::MAX {
                                            let w_str =
                                                result.compact_storage.str_arena.get(w_off, w_len);
                                            let (g_off, g_len) =
                                                storage.read_min_max_str(global_idx, slot_idx);
                                            let should_update = if g_off == u32::MAX {
                                                true
                                            } else {
                                                let g_str = storage.str_arena.get(g_off, g_len);
                                                let cmp = collation_strcmp(w_str, g_str);
                                                match kind {
                                                    CompactAccKind::MinStr => cmp < 0,
                                                    CompactAccKind::MaxStr => cmp > 0,
                                                    _ => unreachable!(),
                                                }
                                            };
                                            if should_update {
                                                let w_str = result
                                                    .compact_storage
                                                    .str_arena
                                                    .get(w_off, w_len);
                                                let (new_off, new_len) =
                                                    storage.str_arena.alloc(w_str);
                                                storage.write_min_max_str(
                                                    global_idx, slot_idx, new_off, new_len,
                                                );
                                            }
                                        }
                                    }
                                    CompactAccKind::MinInt => {
                                        let (w_val, w_has) = result
                                            .compact_storage
                                            .read_min_max_int(worker_idx, slot_idx);
                                        if w_has {
                                            storage.update_min_int(global_idx, slot_idx, w_val);
                                        }
                                    }
                                    CompactAccKind::MaxInt => {
                                        let (w_val, w_has) = result
                                            .compact_storage
                                            .read_min_max_int(worker_idx, slot_idx);
                                        if w_has {
                                            storage.update_max_int(global_idx, slot_idx, w_val);
                                        }
                                    }
                                    CompactAccKind::CountDistinctInt
                                    | CompactAccKind::CountDistinctStr => {
                                        spec_cd_sidecar.union_from(
                                            slot_idx,
                                            global_idx,
                                            &result.cd_sidecar,
                                            worker_idx,
                                        );
                                    }
                                }
                            }
                        }
                    }

                    for e in &spec_cd_sidecar.entries {
                        let count = e.count(global_idx);
                        storage.set_count(global_idx, e.spec_idx, count);
                    }

                    let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                    for (spec_idx, spec) in agg_specs.iter().enumerate() {
                        agg_results.push(compact_finalize(storage, global_idx, spec_idx, spec));
                    }

                    let source_wi = key_source_worker.unwrap();
                    let source_gidx = *partial_results[source_wi]
                        .compact_map
                        .get(&hash_key)
                        .unwrap();
                    let mixed_ks = &partial_results[source_wi].mixed_keys;

                    let mut row: Vec<(pg_sys::Datum, bool)> =
                        Vec::with_capacity(ctx.num_result_cols);
                    for entry in ctx.output_map {
                        match entry {
                            OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                            OutputEntry::Group(gi) => {
                                let kv = mixed_ks.get(source_gidx, *gi);
                                match kv {
                                    MixedKeyVal::Int(v) => {
                                        if matches!(
                                            group_specs[*gi].expr,
                                            GroupByExpr::Extract { .. }
                                        ) {
                                            row.push((i128_to_numeric_datum(v as i128), false));
                                        } else {
                                            row.push((pg_sys::Datum::from(v as usize), false));
                                        }
                                    }
                                    MixedKeyVal::Str(off, len) => {
                                        let s = mixed_ks.arena.get(off, len);
                                        let datum = string_to_datum(s, group_specs[*gi].type_oid);
                                        row.push((datum, false));
                                    }
                                    MixedKeyVal::Null => {
                                        row.push((pg_sys::Datum::from(0usize), true));
                                    }
                                }
                            }
                            OutputEntry::DerivedGroup { base_gi, delta } => {
                                match mixed_ks.get(source_gidx, *base_gi) {
                                    MixedKeyVal::Int(v) => {
                                        row.push((pg_sys::Datum::from((v + delta) as usize), false))
                                    }
                                    _ => row.push((pg_sys::Datum::from(0usize), true)),
                                }
                            }
                            OutputEntry::Const(d, n) => row.push((*d, *n)),
                        }
                    }
                    result_rows.push(row);
                }
                let finalize_us = t_fin.elapsed().as_micros() as u64;

                let pre_topn_groups: usize =
                    partial_results.iter().map(|r| r.compact_map.len()).sum();

                return Some(MixedMergeOutcome {
                    result_rows,
                    pre_topn_groups: pre_topn_groups as u64,
                    merge_us: 0,
                    finalize_us,
                    topn_select_us,
                });
            }
        }
        None
    }
}

/// Partitioned parallel merge + top-N for the mixed path. Partitions
/// the key space across `n_workers` threads; each merges its slice and
/// finds local top-N, then a final merge picks the global top-N.
///
/// Caller has already gated `topn_limit > 0`.
///
/// # Safety
///
/// Inherits the `CompactAccStorage` accessor contract. Must run inside
/// an active PG transaction (finalize allocates datums).
#[inline]
unsafe fn mixed_partitioned_topn(
    ctx: &MixedMergeCtx<'_>,
    agg_specs: &[AggExecSpec],
    group_specs: &[GroupByColSpec],
    partial_results: &[ParallelMixedResult],
) -> MixedMergeOutcome {
    unsafe {
        let t_merge = Instant::now();
        let limit = ctx.topn_limit as usize;
        let sort_slot = match ctx.output_map[ctx.topn_sort_col] {
            OutputEntry::Agg(ai) => ai,
            _ => unreachable!(),
        };
        let n_partitions = ctx.n_workers;
        let n_group_cols = group_specs.len();

        let pre_topn_groups: usize = partial_results.iter().map(|r| r.compact_map.len()).sum();

        // Each partition thread: merge its slice, find local top-N,
        // copy winners to mini storage + mini mixed keys, drop the rest.
        #[allow(clippy::type_complexity)]
        let partition_results: Vec<(
            CompactAccStorage,
            MixedKeyStorage,
            Vec<(i64, u128, u32)>,
        )> = std::thread::scope(|s| {
            let workers = partial_results;
            let specs = &agg_specs;
            let np = n_partitions;
            let ascending = ctx.topn_ascending;
            let ngk = n_group_cols;
            let hfilters = ctx.having_filters;

            let handles: Vec<_> = (0..np)
                .map(|p| {
                    s.spawn(move || {
                        let layout = CompactAccLayout::new(specs);
                        let n_slots = layout.slots.len();
                        let mut map: CompactGroupMap =
                            CompactGroupMap::with_hasher(Default::default());
                        let mut storage = CompactAccStorage::new(layout);
                        let mut cd_sidecar = CountDistinctSideCar::new(specs);
                        let mut mixed_ks = MixedKeyStorage::new(ngk);

                        // Merge entries from all workers belonging to this partition
                        for worker in workers {
                            for (&key, &wgidx) in &worker.compact_map {
                                if ((key as u64) ^ ((key >> 64) as u64)) as usize % np != p {
                                    continue;
                                }
                                let gidx = match map.entry(key) {
                                    hashbrown::hash_map::Entry::Occupied(e) => *e.get(),
                                    hashbrown::hash_map::Entry::Vacant(e) => {
                                        let idx = storage.alloc_group();
                                        cd_sidecar.alloc_group();
                                        // Copy key values from this worker
                                        for col in 0..ngk {
                                            let kv = worker.mixed_keys.get(wgidx, col);
                                            match kv {
                                                MixedKeyVal::Str(off, len) => {
                                                    let sv = worker.mixed_keys.arena.get(off, len);
                                                    let (no, nl) = mixed_ks.arena.alloc(sv);
                                                    mixed_ks.keys.push(MixedKeyVal::Str(no, nl));
                                                }
                                                other => mixed_ks.keys.push(other),
                                            }
                                        }
                                        e.insert(idx);
                                        idx
                                    }
                                };
                                for slot_idx in 0..n_slots {
                                    let (_, kind) = storage.layout.slots[slot_idx];
                                    match kind {
                                        CompactAccKind::Count => {
                                            let wc =
                                                worker.compact_storage.read_count(wgidx, slot_idx);
                                            storage.incr_count(gidx, slot_idx, wc);
                                        }
                                        CompactAccKind::SumInt => {
                                            let (ws, wc) = worker
                                                .compact_storage
                                                .read_sum_int(wgidx, slot_idx);
                                            storage.add_sum_int(gidx, slot_idx, ws, wc);
                                        }
                                        CompactAccKind::SumIntNarrow => {
                                            let (ws, wc) = worker
                                                .compact_storage
                                                .read_sum_int_narrow(wgidx, slot_idx);
                                            storage.add_sum_int_narrow(gidx, slot_idx, ws, wc);
                                        }
                                        CompactAccKind::SumFloat => {
                                            let (ws, wc) = worker
                                                .compact_storage
                                                .read_sum_float(wgidx, slot_idx);
                                            storage.add_sum_float(gidx, slot_idx, ws, wc);
                                        }
                                        CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                            let (w_off, w_len) = worker
                                                .compact_storage
                                                .read_min_max_str(wgidx, slot_idx);
                                            if w_off != u32::MAX {
                                                let w_str = worker
                                                    .compact_storage
                                                    .str_arena
                                                    .get(w_off, w_len);
                                                let (g_off, g_len) =
                                                    storage.read_min_max_str(gidx, slot_idx);
                                                let should_update = if g_off == u32::MAX {
                                                    true
                                                } else {
                                                    let g_str = storage.str_arena.get(g_off, g_len);
                                                    let cmp = strcoll_cmp(w_str, g_str);
                                                    match kind {
                                                        CompactAccKind::MinStr => {
                                                            cmp == std::cmp::Ordering::Less
                                                        }
                                                        _ => cmp == std::cmp::Ordering::Greater,
                                                    }
                                                };
                                                if should_update {
                                                    let w_str = worker
                                                        .compact_storage
                                                        .str_arena
                                                        .get(w_off, w_len);
                                                    let (new_off, new_len) =
                                                        storage.str_arena.alloc(w_str);
                                                    storage.write_min_max_str(
                                                        gidx, slot_idx, new_off, new_len,
                                                    );
                                                }
                                            }
                                        }
                                        CompactAccKind::MinInt => {
                                            let (w_val, w_has) = worker
                                                .compact_storage
                                                .read_min_max_int(wgidx, slot_idx);
                                            if w_has {
                                                storage.update_min_int(gidx, slot_idx, w_val);
                                            }
                                        }
                                        CompactAccKind::MaxInt => {
                                            let (w_val, w_has) = worker
                                                .compact_storage
                                                .read_min_max_int(wgidx, slot_idx);
                                            if w_has {
                                                storage.update_max_int(gidx, slot_idx, w_val);
                                            }
                                        }
                                        CompactAccKind::CountDistinctInt
                                        | CompactAccKind::CountDistinctStr => {
                                            cd_sidecar.union_from(
                                                slot_idx,
                                                gidx,
                                                &worker.cd_sidecar,
                                                wgidx,
                                            );
                                        }
                                    }
                                }
                            }
                        }

                        // Write CD counts into storage before top-N selection
                        cd_sidecar.write_counts_to_storage(&mut storage, &map);

                        // Local top-N selection using a heap
                        let (_, sort_kind) = storage.layout.slots[sort_slot];
                        let sort_is_avg = specs[sort_slot].agg_type == AggType::Avg;
                        let read_val = |gidx: u32| -> i64 {
                            if sort_is_avg {
                                let avg = match sort_kind {
                                    CompactAccKind::SumIntNarrow => {
                                        let (s, c) = storage.read_sum_int_narrow(gidx, sort_slot);
                                        if c > 0 { s as f64 / c as f64 } else { 0.0 }
                                    }
                                    CompactAccKind::SumFloat => {
                                        let (s, c) = storage.read_sum_float(gidx, sort_slot);
                                        if c > 0 { s / c as f64 } else { 0.0 }
                                    }
                                    _ => storage.read_count(gidx, sort_slot) as f64,
                                };
                                let bits = avg.to_bits() as i64;
                                if bits >= 0 { bits } else { bits ^ i64::MAX }
                            } else {
                                match sort_kind {
                                    CompactAccKind::Count => storage.read_count(gidx, sort_slot),
                                    CompactAccKind::SumIntNarrow => {
                                        storage.read_sum_int_narrow(gidx, sort_slot).0
                                    }
                                    _ => storage.read_count(gidx, sort_slot),
                                }
                            }
                        };

                        let having_read_val = |gidx: u32, slot: usize| -> i64 {
                            let (_, kind) = storage.layout.slots[slot];
                            match kind {
                                CompactAccKind::Count => storage.read_count(gidx, slot),
                                CompactAccKind::SumIntNarrow => {
                                    storage.read_sum_int_narrow(gidx, slot).0
                                }
                                _ => storage.read_count(gidx, slot),
                            }
                        };

                        let winners: Vec<(i64, u128, u32)> = if ascending {
                            let mut heap: BinaryHeap<(i64, u128, u32)> =
                                BinaryHeap::with_capacity(limit + 1);
                            for (&key, &gidx) in &map {
                                let mut passes = true;
                                for hf in hfilters {
                                    let val = having_read_val(gidx, hf.agg_idx);
                                    let ok = match hf.op {
                                        HavingOp::Gt => val > hf.const_val,
                                        HavingOp::Lt => val < hf.const_val,
                                        HavingOp::Ge => val >= hf.const_val,
                                        HavingOp::Le => val <= hf.const_val,
                                        HavingOp::Eq => val == hf.const_val,
                                        HavingOp::Ne => val != hf.const_val,
                                    };
                                    if !ok {
                                        passes = false;
                                        break;
                                    }
                                }
                                if !passes {
                                    continue;
                                }
                                let val = read_val(gidx);
                                heap.push((val, key, gidx));
                                if heap.len() > limit {
                                    heap.pop();
                                }
                            }
                            heap.into_vec()
                        } else {
                            let mut heap: BinaryHeap<Reverse<(i64, u128, u32)>> =
                                BinaryHeap::with_capacity(limit + 1);
                            for (&key, &gidx) in &map {
                                let mut passes = true;
                                for hf in hfilters {
                                    let val = having_read_val(gidx, hf.agg_idx);
                                    let ok = match hf.op {
                                        HavingOp::Gt => val > hf.const_val,
                                        HavingOp::Lt => val < hf.const_val,
                                        HavingOp::Ge => val >= hf.const_val,
                                        HavingOp::Le => val <= hf.const_val,
                                        HavingOp::Eq => val == hf.const_val,
                                        HavingOp::Ne => val != hf.const_val,
                                    };
                                    if !ok {
                                        passes = false;
                                        break;
                                    }
                                }
                                if !passes {
                                    continue;
                                }
                                let val = read_val(gidx);
                                heap.push(Reverse((val, key, gidx)));
                                if heap.len() > limit {
                                    heap.pop();
                                }
                            }
                            heap.into_iter().map(|Reverse(x)| x).collect()
                        };

                        drop(map);

                        // Copy winning groups to mini storage + mini mixed keys
                        let layout2 = CompactAccLayout::new(specs);
                        let stride = storage.layout.group_stride;
                        let mut mini = CompactAccStorage::new(layout2);
                        let mut mini_keys = MixedKeyStorage::new(ngk);
                        let mut top_entries = Vec::with_capacity(winners.len());

                        for (sort_val, key, old_gidx) in winners {
                            let new_gidx = mini.alloc_group();
                            let src = old_gidx as usize * stride;
                            let dst = new_gidx as usize * stride;
                            mini.buf[dst..dst + stride]
                                .copy_from_slice(&storage.buf[src..src + stride]);
                            // Remap MinStr/MaxStr arena references
                            for slot_idx in 0..n_slots {
                                let (_, kind) = storage.layout.slots[slot_idx];
                                if kind == CompactAccKind::MinStr || kind == CompactAccKind::MaxStr
                                {
                                    let (off, len) = storage.read_min_max_str(old_gidx, slot_idx);
                                    if off != u32::MAX {
                                        let val_str = storage.str_arena.get(off, len);
                                        let (no, nl) = mini.str_arena.alloc(val_str);
                                        mini.write_min_max_str(new_gidx, slot_idx, no, nl);
                                    }
                                }
                            }
                            // Copy mixed key values
                            for col in 0..ngk {
                                let kv = mixed_ks.get(old_gidx, col);
                                match kv {
                                    MixedKeyVal::Str(off, len) => {
                                        let sv = mixed_ks.arena.get(off, len);
                                        let (no, nl) = mini_keys.arena.alloc(sv);
                                        mini_keys.keys.push(MixedKeyVal::Str(no, nl));
                                    }
                                    other => mini_keys.keys.push(other),
                                }
                            }
                            top_entries.push((sort_val, key, new_gidx));
                        }

                        drop(storage);
                        drop(mixed_ks);

                        (mini, mini_keys, top_entries)
                    })
                })
                .collect();

            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        let merge_us = t_merge.elapsed().as_micros() as u64;

        // Merge all partition top entries, select global top-N
        let t_finalize = Instant::now();
        let mut all_candidates: Vec<(i64, u128, u32, usize)> = Vec::new();
        for (pi, (_, _, entries)) in partition_results.iter().enumerate() {
            for &(sort_val, key, gidx) in entries {
                all_candidates.push((sort_val, key, gidx, pi));
            }
        }
        if ctx.topn_ascending {
            all_candidates.sort_unstable_by_key(|a| a.0);
        } else {
            all_candidates.sort_unstable_by_key(|b| std::cmp::Reverse(b.0));
        }
        all_candidates.truncate(limit);

        let mut result_rows = Vec::with_capacity(limit);
        for &(_sort_val, _key, mini_gidx, pi) in &all_candidates {
            let storage = &partition_results[pi].0;
            let mixed_ks = &partition_results[pi].1;
            let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
            for (spec_idx, spec) in agg_specs.iter().enumerate() {
                agg_results.push(compact_finalize(storage, mini_gidx, spec_idx, spec));
            }
            let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(ctx.num_result_cols);
            for entry in ctx.output_map {
                match entry {
                    OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                    OutputEntry::Group(gi) => {
                        let kv = mixed_ks.get(mini_gidx, *gi);
                        match kv {
                            MixedKeyVal::Int(v) => {
                                if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. }) {
                                    row.push((i128_to_numeric_datum(v as i128), false));
                                } else {
                                    row.push((pg_sys::Datum::from(v as usize), false));
                                }
                            }
                            MixedKeyVal::Str(off, len) => {
                                let sv = mixed_ks.arena.get(off, len);
                                let datum = string_to_datum(sv, group_specs[*gi].type_oid);
                                row.push((datum, false));
                            }
                            MixedKeyVal::Null => {
                                row.push((pg_sys::Datum::from(0usize), true));
                            }
                        }
                    }
                    OutputEntry::DerivedGroup { base_gi, delta } => {
                        match mixed_ks.get(mini_gidx, *base_gi) {
                            MixedKeyVal::Int(v) => {
                                row.push((pg_sys::Datum::from((v + delta) as usize), false))
                            }
                            _ => row.push((pg_sys::Datum::from(0usize), true)),
                        }
                    }
                    OutputEntry::Const(d, n) => row.push((*d, *n)),
                }
            }
            result_rows.push(row);
        }
        let finalize_us = t_finalize.elapsed().as_micros() as u64;

        MixedMergeOutcome {
            result_rows,
            pre_topn_groups: pre_topn_groups as u64,
            merge_us,
            finalize_us,
            topn_select_us: 0,
        }
    }
}
/// Parallel-mixed path dispatch.
///
/// Caller MUST verify `can_parallel_mixed_flag` (i.e. has the mixed
/// preconditions, has compiled regex infos if any, ran
/// `can_parallel_mixed`) before invoking — this fn consumes
/// `agg_specs` / `group_specs` / `compact_storage` /
/// `rust_regex_infos` into the returned `AggScanState`.
///
/// SAFETY: calls `detoast_lazy_blobs` + worker-scope FFI. Must run
/// inside an active PG transaction.
#[allow(clippy::too_many_arguments)]
pub(super) unsafe fn dispatch_parallel_mixed_path(
    agg_specs: Vec<AggExecSpec>,
    group_specs: Vec<GroupByColSpec>,
    output_map: &[OutputEntry],
    having_filters: &[HavingFilter],
    where_quals: *mut pg_sys::List,
    topn_limit: i64,
    topn_sort_col: usize,
    topn_ascending: bool,
    bare_limit: i64,
    derived_minmax_topn: Option<(usize, usize)>,
    meta: &MetadataInfo,
    all_segments: &mut [SegmentData],
    needed_cols: &[bool],
    batch_quals: &[BatchQual],
    seg_filters: &[(usize, String)],
    sidecar_only_cols: &[bool],
    time_min: Option<i64>,
    time_max: Option<i64>,
    n_workers: usize,
    use_lazy: bool,
    num_result_cols: usize,
    metadata_us: u64,
    heap_scan_us: u64,
    t_wall: Instant,
    rust_regex_infos: Vec<RustRegexInfo>,
    mut compact_storage: Option<CompactAccStorage>,
    mut total_detoast_us: u64,
    mut total_cache_hits: u64,
    mut total_cache_misses: u64,
    mut total_cache_bytes_served: u64,
) -> AggScanState {
    let has_group_by = !group_specs.is_empty();
    // No-GROUP-BY queries arrive with `compact_storage` unset — the caller
    // only builds it when has_group_by — but the merge paths below need a
    // leader-side storage regardless.
    if compact_storage.is_none() {
        compact_storage = Some(CompactAccStorage::new(CompactAccLayout::new(&agg_specs)));
    }
    #[allow(unused_assignments)] // overwritten by `largest.compact_map` on the merge branch
    let mut compact_group_map: CompactGroupMap =
        CompactGroupMap::with_hasher(BuildHasherDefault::default());
    unsafe {
        let t2 = Instant::now();
        // For derived MIN/MAX-difference top-N, workers don't maintain a
        // direct-sort heap — sort key is recovered at the leader from
        // partial max/min, see the `derived_minmax_topn` branch below.
        let topn_spec =
            if topn_limit > 0 && having_filters.is_empty() && derived_minmax_topn.is_none() {
                let sort_slot = match output_map[topn_sort_col] {
                    OutputEntry::Agg(ai) => ai,
                    _ => unreachable!(),
                };
                // AVG sort can't use raw sum for speculative top-K pruning
                if agg_specs[sort_slot].agg_type == AggType::Avg {
                    None
                } else {
                    let k = (topn_limit as usize).max(1000);
                    Some((sort_slot, k, topn_ascending))
                }
            } else {
                None
            };

        // Build text_group_col_flags
        let mut text_group_col_flags: Vec<bool> = (0..meta.col_names.len())
            .map(|i| {
                group_specs
                    .iter()
                    .any(|gs| gs.col_idx as usize == i && is_text_group_col(gs))
            })
            .collect();
        // CaseWhen ColumnRef results reference text columns that need decompression
        for gs in &group_specs {
            if let GroupByExpr::CaseWhen(ref spec) = gs.expr {
                for clause in &spec.clauses {
                    if let CaseWhenValue::ColumnRef(ci) = &clause.result
                        && *ci < text_group_col_flags.len()
                    {
                        text_group_col_flags[*ci] = true;
                    }
                }
                if let CaseWhenValue::ColumnRef(ci) = &spec.default
                    && *ci < text_group_col_flags.len()
                {
                    text_group_col_flags[*ci] = true;
                }
            }
        }

        // Build text qual infos for worker threads.
        // Order: positive LIKE first (most selective — match a pattern),
        // then EQ/NE, then NOT LIKE (negated patterns pass most rows).
        // This maximizes short-circuit benefit when filters AND into selection.
        let mut text_qual_infos: Vec<TextQualInfo> = Vec::new();
        for bq in batch_quals {
            let t = bq.type_oid;
            if t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID {
                match bq.op {
                    BatchCompareOp::Eq | BatchCompareOp::Ne => {
                        if let Some(ref cs) = bq.text_const {
                            text_qual_infos.push(TextQualInfo::EqNe {
                                col_idx: bq.col_idx,
                                const_str: cs.clone(),
                                is_ne: bq.op == BatchCompareOp::Ne,
                            });
                        }
                    }
                    BatchCompareOp::Like | BatchCompareOp::NotLike => {
                        if let Some(ref strat) = bq.like_strategy {
                            text_qual_infos.push(TextQualInfo::Like {
                                col_idx: bq.col_idx,
                                strategy: strat.clone(),
                                negate: bq.op == BatchCompareOp::NotLike,
                            });
                        }
                    }
                    BatchCompareOp::InList => {
                        if let Some(ref vals) = bq.in_list_text {
                            text_qual_infos.push(TextQualInfo::InList {
                                col_idx: bq.col_idx,
                                values: vals.clone(),
                            });
                        }
                    }
                    _ => {}
                }
            }
        }
        // Reorder: cheap filters first to maximize short-circuit benefit.
        // EQ/NE are O(1) per row; InList against a small list is also O(1)
        // in the dict fast path; LIKE requires substring search.
        text_qual_infos.sort_by_key(|tqi| match tqi {
            TextQualInfo::EqNe { .. } => 0,                // EQ/NE — cheapest
            TextQualInfo::InList { .. } => 1,              // dict-keyed IN
            TextQualInfo::Like { negate: false, .. } => 2, // positive LIKE
            TextQualInfo::Like { negate: true, .. } => 3,  // NOT LIKE
        });

        // Pipeline detoast with parallel processing when enough segments.
        // Use fewer batches than the compact path (2 vs n_workers*2) because
        // the mixed path processes text columns which have high per-segment
        // cost. Fewer batches = fewer thread scope synchronization points.
        //
        // Gate on `!batch_quals.is_empty()`: pipeline overlap only pays off
        // when workers spend non-trivial time per segment (filter eval, more
        // complex aggregation) so the leader can usefully detoast the next
        // batch in parallel. Unfiltered text-grouping queries
        // (e.g. ClickBench Q18) have workers blast through segments
        // faster than detoast can keep up — the per-iteration thread::scope
        // synchronization is then pure overhead. Empirically on ClickBench:
        // pipeline gives ~+0.8s on Q22-class (heavy filter), ~-2.3s on
        // Q18-class (no filter). Net positive on filtered, net negative on
        // unfiltered.
        let use_pipeline =
            use_lazy && all_segments.len() >= n_workers * 16 && !batch_quals.is_empty();
        // Pipeline uses 2 batches: workers run on current batch while leader
        // detoasts the next. Pre-detoast must cover the *entire* first batch
        // before workers spawn, otherwise workers on un-detoasted segments
        // see empty blobs → SegTextColumn::Lz4 returns None → group keys
        // collapse to NULL and distinct group values are lost. Must match
        // PIPELINE_N_BATCHES below.
        const PIPELINE_N_BATCHES: usize = 2;

        if use_lazy {
            let t_detoast = Instant::now();
            if use_pipeline {
                let n_batches = PIPELINE_N_BATCHES.min(all_segments.len());
                let batch_size = all_segments.len().div_ceil(n_batches);
                let first_end = batch_size.min(all_segments.len());
                for seg in &mut all_segments[..first_end] {
                    let dl = detoast_lazy_blobs(seg);
                    total_cache_hits += dl.cache_hits;
                    total_cache_misses += dl.cache_misses;
                    total_cache_bytes_served += dl.cache_bytes_served;
                }
            } else {
                for seg in all_segments.iter_mut() {
                    let dl = detoast_lazy_blobs(seg);
                    total_cache_hits += dl.cache_hits;
                    total_cache_misses += dl.cache_misses;
                    total_cache_bytes_served += dl.cache_bytes_served;
                }
            }
            total_detoast_us += t_detoast.elapsed().as_micros() as u64;
        }

        // F8 Phase 0: when the query shape matches (bare LIMIT, no WHERE,
        // no HAVING, no non-trivial group-by expressions), pre-select
        // `bare_limit` distinct group-key hashes from the first few
        // segments. Workers will then filter phase-1 rows by this set,
        // bounding each worker's hash map to at most `bare_limit` entries.
        // Counts stay exact because every matching row is still counted.
        let has_case_when_group = group_specs
            .iter()
            .any(|gs| matches!(gs.expr, GroupByExpr::CaseWhen(_)));
        let has_regex_group = group_specs
            .iter()
            .any(|gs| matches!(gs.expr, GroupByExpr::RegexpReplace { .. }));
        let preselected_keys: Option<hashbrown::HashSet<u128>> = if bare_limit > 0
            && having_filters.is_empty()
            && batch_quals.is_empty()
            && where_quals.is_null()
            && !has_case_when_group
            && !has_regex_group
        {
            try_build_preselected(
                bare_limit as usize,
                all_segments,
                &group_specs,
                &meta.col_names,
                &meta.col_types,
                &meta.segment_by,
                needed_cols,
                &text_group_col_flags,
                /* max_probe_segments */ 4,
            )
        } else {
            None
        };

        // Phase D leader pre-pass: build per-spec dict-distinct remaps
        // for every eligible CountDistinct(text) spec. Workers consult
        // these through `ParallelMixedConfig::dict_distinct_remaps` to
        // set bits in per-(spec, group) bitsets instead of hashing
        // strings into HashSet<u128>. Specs not in the map fall back to
        // the existing HashSet path. Sequential today — see
        // `build_dict_distinct_remaps` for the cost/threshold logic.
        let dict_distinct_remaps = build_dict_distinct_remaps(all_segments, &agg_specs);

        let config = ParallelMixedConfig {
            agg_specs: &agg_specs,
            group_specs: &group_specs,
            col_names: &meta.col_names,
            col_types: &meta.col_types,
            segment_by: &meta.segment_by,
            blob_idx: &meta.blob_idx,
            missing_values: &meta.missing_values,
            needed_cols,
            batch_quals,
            seg_filters,
            time_min,
            time_max,
            topn_spec,
            text_group_col_flags: &text_group_col_flags,
            text_qual_infos: &text_qual_infos,
            rust_regex_infos: &rust_regex_infos,
            sidecar_only_cols,
            preselected_keys: preselected_keys.as_ref(),
            dict_distinct_remaps: &dict_distinct_remaps,
        };

        let mut pipeline_detoast_us: u64 = 0;
        let mut partial_results: Vec<ParallelMixedResult> = if use_pipeline {
            let n_batches = PIPELINE_N_BATCHES.min(all_segments.len());
            let batch_size = all_segments.len().div_ceil(n_batches);
            let mut results: Vec<ParallelMixedResult> = Vec::new();
            let mut batch_start = 0;
            let total_segs = all_segments.len();

            while batch_start < total_segs {
                let batch_end = (batch_start + batch_size).min(total_segs);
                let next_end = (batch_end + batch_size).min(total_segs);

                let (done, pending) = all_segments.split_at_mut(batch_end);
                let current_batch = &done[batch_start..];

                std::thread::scope(|s| {
                    let chunk_size = current_batch.len().div_ceil(n_workers);
                    let handles: Vec<_> = current_batch
                        .chunks(chunk_size)
                        .enumerate()
                        .map(|(ci, chunk)| {
                            let cfg = &config;
                            // Phase D: chunk_offset is the seg_idx of chunk[0]
                            // in the leader's all_segments view, used to index
                            // dict_distinct_remaps.per_segment.
                            let chunk_offset = batch_start + ci * chunk_size;
                            s.spawn(move || process_segments_mixed(chunk, chunk_offset, cfg))
                        })
                        .collect();

                    if batch_end < total_segs {
                        let t_pd = Instant::now();
                        for seg in &mut pending[..next_end - batch_end] {
                            let dl = detoast_lazy_blobs(seg);
                            total_cache_hits += dl.cache_hits;
                            total_cache_misses += dl.cache_misses;
                            total_cache_bytes_served += dl.cache_bytes_served;
                        }
                        pipeline_detoast_us += t_pd.elapsed().as_micros() as u64;
                    }

                    for h in handles {
                        results.push(h.join().unwrap());
                    }
                });

                batch_start = batch_end;
            }
            results
        } else {
            let chunk_size = all_segments.len().div_ceil(n_workers);
            std::thread::scope(|s| {
                let handles: Vec<_> = all_segments
                    .chunks(chunk_size)
                    .enumerate()
                    .map(|(ci, chunk)| {
                        let cfg = &config;
                        let chunk_offset = ci * chunk_size;
                        s.spawn(move || process_segments_mixed(chunk, chunk_offset, cfg))
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            })
        };

        // A no-GROUP-BY aggregate must emit exactly one row even when every
        // row was filtered out (COUNT(*) = 0, SUM/MIN/MAX = NULL). Workers
        // only allocate a group on the first passing row, so when none saw a
        // match, synthesize the single constant-key group with default
        // accumulator state; every merge path downstream then treats it like
        // an ordinary group.
        if !has_group_by
            && partial_results.iter().all(|r| r.compact_map.is_empty())
            && let Some(r0) = partial_results.first_mut()
        {
            let idx = r0.compact_storage.alloc_group();
            r0.cd_sidecar.alloc_group();
            r0.mixed_keys.insert(&[], &[], &group_specs);
            r0.compact_map.insert(hash_mixed_key(&[], &[]), idx);
        }

        // Accumulate stats
        let scan_wall_us = t2.elapsed().as_micros() as u64;
        let mut total_segments: u64 = 0;
        let mut total_rows_processed: u64 = 0;
        let mut decompress_us: u64 = 0;
        for result in &partial_results {
            total_segments += result.segments_processed;
            total_rows_processed += result.rows_processed;
            decompress_us = decompress_us.max(result.decompress_us);
        }
        total_detoast_us += pipeline_detoast_us;
        let agg_us = scan_wall_us.saturating_sub(decompress_us + pipeline_detoast_us);

        let merge_ctx = MixedMergeCtx {
            output_map,
            having_filters,
            where_quals,
            topn_limit,
            topn_sort_col,
            topn_ascending,
            bare_limit,
            batch_quals,
            n_workers,
            num_result_cols,
            has_group_by,
            metadata_us,
            heap_scan_us,
            total_detoast_us,
            total_cache_hits,
            total_cache_misses,
            total_cache_bytes_served,
            decompress_us,
            agg_us,
            total_segments,
            total_rows_processed,
            t_wall,
            preselected_count: preselected_keys
                .as_ref()
                .map(|s| s.len() as u64)
                .unwrap_or(0),
        };

        // Derived MIN/MAX-difference top-N — see `mixed_derived_minmax_topn`.
        if let Some((max_slot, min_slot)) = derived_minmax_topn {
            return mixed_derived_minmax_topn(
                &merge_ctx,
                agg_specs,
                group_specs,
                &partial_results,
                compact_storage.as_mut().unwrap(),
                max_slot,
                min_slot,
            );
        }

        // Speculative top-N — see `mixed_speculative_topn`.
        if let Some(outcome) = mixed_speculative_topn(
            &merge_ctx,
            &agg_specs,
            &group_specs,
            &partial_results,
            compact_storage.as_mut().unwrap(),
        ) {
            return build_mixed_topn_agg_scan_state(&merge_ctx, agg_specs, group_specs, outcome);
        }

        // Bare LIMIT short-circuit for mixed path — see `mixed_bare_limit`.
        if bare_limit > 0 && having_filters.is_empty() {
            return mixed_bare_limit(&merge_ctx, agg_specs, group_specs, &partial_results);
        }

        // Partitioned parallel merge + top-N — see `mixed_partitioned_topn`.
        if topn_limit > 0 {
            let outcome =
                mixed_partitioned_topn(&merge_ctx, &agg_specs, &group_specs, &partial_results);
            return build_mixed_topn_agg_scan_state(&merge_ctx, agg_specs, group_specs, outcome);
        }

        // Fallthrough: full merge path — see `mixed_full_merge`.
        mixed_full_merge(
            &merge_ctx,
            agg_specs,
            group_specs,
            partial_results,
            compact_storage.as_mut().unwrap(),
            &mut compact_group_map,
        )
    }
}
