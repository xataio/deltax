use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::pg_guard;

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::time::Instant;

use crate::compression;
use super::super::SyncStatic;
use super::batch_qual::{BatchCompareOp, BatchQual,
    extract_batch_quals, evaluate_batch_quals};
use super::datum_utils::{
    decompress_blob_to_datums, decompress_text_blob_to_raw_strings,
    decompress_text_blob_to_lengths, decompress_text_blob_with_like_filter,
    decompress_text_blob_with_eq_filter, string_to_datum, pg_type_name,
    count_non_null, collation_strcmp,
};
use super::segments::{
    SegmentData, load_metadata, load_segments_heap,
    segment_skippable_by_dict_like, extract_segment_filters,
};

/// Compute a 128-bit hash of a byte slice for COUNT(DISTINCT) on strings.
/// Uses two AHasher instances (AES-NI accelerated) with different fixed keys
/// to produce independent 64-bit halves, combined into u128. Collision
/// probability is negligible for any practical cardinality (~1 in 2^64 for
/// any pair).
fn hash128_str(data: &[u8]) -> u128 {
    use std::hash::BuildHasher;
    let s1 = ahash::RandomState::with_seeds(0xa1b2c3d4, 0xe5f6a7b8, 0x11223344, 0x55667788);
    let mut h1 = s1.build_hasher();
    h1.write(data);
    let lo = h1.finish();
    let s2 = ahash::RandomState::with_seeds(0x1234abcd, 0x5678ef01, 0xaabbccdd, 0xeeff0011);
    let mut h2 = s2.build_hasher();
    h2.write(data);
    let hi = h2.finish();
    (hi as u128) << 64 | lo as u128
}

// ============================================================================
// DeltaXAgg: aggregate pushdown (SUM, AVG, COUNT, COUNT(DISTINCT), GROUP BY)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum AggType { Sum, Count, CountStar, Avg, CountDistinct, Min, Max }

/// Expression kind for aggregate arguments.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum AggExpr {
    /// Plain column reference: AGG(col)
    Column,
    /// length(col): AGG(length(col)) — compute string lengths without varlena allocation
    LengthOf,
    /// col + const: AGG(col + N) — add integer constant before aggregation
    AddConst,
}

enum AggAccumulator {
    SumInt { sum: i128, count: i64 },
    SumFloat { sum: f64, count: i64 },
    Count { count: i64 },
    CountDistinctInt { seen: std::collections::HashSet<i64> },
    /// Stores SipHash-128 digests of strings instead of owned Strings.
    /// Bounded memory (16 bytes per distinct value) — same approach as ClickHouse's uniqExact.
    CountDistinctStr { seen: std::collections::HashSet<u128> },
    MinInt { val: Option<i64> },
    MaxInt { val: Option<i64> },
    MinFloat { val: Option<f64> },
    MaxFloat { val: Option<f64> },
    MinStr { val: Option<String> },
    MaxStr { val: Option<String> },
}

impl AggAccumulator {
    fn new_for(agg_type: AggType, col_type: pg_sys::Oid) -> Self {
        match agg_type {
            AggType::Sum | AggType::Avg => {
                if col_type == pg_sys::FLOAT4OID || col_type == pg_sys::FLOAT8OID {
                    AggAccumulator::SumFloat { sum: 0.0, count: 0 }
                } else {
                    AggAccumulator::SumInt { sum: 0, count: 0 }
                }
            }
            AggType::Count | AggType::CountStar => AggAccumulator::Count { count: 0 },
            AggType::CountDistinct => {
                if col_type == pg_sys::TEXTOID || col_type == pg_sys::VARCHAROID || col_type == pg_sys::BPCHAROID {
                    AggAccumulator::CountDistinctStr { seen: std::collections::HashSet::new() }
                } else {
                    AggAccumulator::CountDistinctInt { seen: std::collections::HashSet::new() }
                }
            }
            AggType::Min => {
                if col_type == pg_sys::TEXTOID || col_type == pg_sys::VARCHAROID || col_type == pg_sys::BPCHAROID {
                    AggAccumulator::MinStr { val: None }
                } else if col_type == pg_sys::FLOAT4OID || col_type == pg_sys::FLOAT8OID {
                    AggAccumulator::MinFloat { val: None }
                } else {
                    AggAccumulator::MinInt { val: None }
                }
            }
            AggType::Max => {
                if col_type == pg_sys::TEXTOID || col_type == pg_sys::VARCHAROID || col_type == pg_sys::BPCHAROID {
                    AggAccumulator::MaxStr { val: None }
                } else if col_type == pg_sys::FLOAT4OID || col_type == pg_sys::FLOAT8OID {
                    AggAccumulator::MaxFloat { val: None }
                } else {
                    AggAccumulator::MaxInt { val: None }
                }
            }
        }
    }

    fn clone_fresh(&self) -> Self {
        match self {
            AggAccumulator::SumInt { .. } => AggAccumulator::SumInt { sum: 0, count: 0 },
            AggAccumulator::SumFloat { .. } => AggAccumulator::SumFloat { sum: 0.0, count: 0 },
            AggAccumulator::Count { .. } => AggAccumulator::Count { count: 0 },
            AggAccumulator::CountDistinctInt { .. } => AggAccumulator::CountDistinctInt { seen: std::collections::HashSet::new() },
            AggAccumulator::CountDistinctStr { .. } => AggAccumulator::CountDistinctStr { seen: std::collections::HashSet::new() },
            AggAccumulator::MinInt { .. } => AggAccumulator::MinInt { val: None },
            AggAccumulator::MaxInt { .. } => AggAccumulator::MaxInt { val: None },
            AggAccumulator::MinFloat { .. } => AggAccumulator::MinFloat { val: None },
            AggAccumulator::MaxFloat { .. } => AggAccumulator::MaxFloat { val: None },
            AggAccumulator::MinStr { .. } => AggAccumulator::MinStr { val: None },
            AggAccumulator::MaxStr { .. } => AggAccumulator::MaxStr { val: None },
        }
    }
}

pub(crate) struct AggExecSpec {
    pub(crate) agg_type: AggType,
    pub(crate) col_idx: i32,               // -1 for COUNT(*)
    pub(crate) col_type_oid: pg_sys::Oid,  // source column type
    pub(crate) expr_kind: AggExpr,         // Column, LengthOf, or AddConst
    pub(crate) const_offset: i64,          // Only used when expr_kind == AddConst
}

// SAFETY: AggExecSpec contains only value types (i32, i64, Oid=u32, enums).
unsafe impl Send for AggExecSpec {}
unsafe impl Sync for AggExecSpec {}

/// Expression kind for GROUP BY columns.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum GroupByExpr {
    /// Plain column reference: GROUP BY col
    Column,
    /// regexp_replace(col, pattern, replacement): GROUP BY regexp_replace(col, ...)
    RegexpReplace { pattern: String, replacement: String, func_oid: u32, collation: u32 },
    /// date_trunc(unit, timestamp_col): GROUP BY date_trunc('minute', ts)
    DateTrunc { unit: String, unit_usecs: i64, func_oid: u32 },
    /// extract(field FROM timestamp_col): GROUP BY extract(minute FROM ts)
    Extract { unit: String, func_oid: u32 },
    /// col +/- const: GROUP BY col - 1  (offset is always stored as addition, so col-1 → offset=-1)
    AddConst { offset: i64, op_oid: u32 },
}

/// Convert a date_trunc unit string to microseconds.
/// Only sub-day units are supported (integer arithmetic is exact).
pub(crate) fn date_trunc_unit_to_usecs(unit: &str) -> i64 {
    match unit {
        "microsecond" | "microseconds" | "us" => 1,
        "millisecond" | "milliseconds" | "ms" => 1_000,
        "second" | "seconds" => 1_000_000,
        "minute" | "minutes" => 60_000_000,
        "hour" | "hours" => 3_600_000_000,
        "day" | "days" => 86_400_000_000,
        _ => 1, // fallback — should not happen (validated in hook)
    }
}

/// Extract a time field from PG epoch microseconds using pure arithmetic.
/// Only supports sub-day fields + dow + epoch (validated in hook).
fn extract_field_from_usecs(pg_usec: i64, unit: &str) -> i64 {
    match unit {
        "microsecond" | "microseconds" => {
            // PG returns second * 1_000_000 (including whole seconds within the minute)
            let usec_in_day = pg_usec.rem_euclid(86_400_000_000);
            let sec_of_min = (usec_in_day / 1_000_000) % 60;
            let frac_usec = usec_in_day.rem_euclid(1_000_000);
            sec_of_min * 1_000_000 + frac_usec
        }
        "millisecond" | "milliseconds" => {
            // PG returns second * 1000 (including whole seconds within the minute)
            let usec_in_day = pg_usec.rem_euclid(86_400_000_000);
            let sec_of_min = (usec_in_day / 1_000_000) % 60;
            let frac_ms = usec_in_day.rem_euclid(1_000_000) / 1_000;
            sec_of_min * 1_000 + frac_ms
        }
        "second" | "seconds" => {
            (pg_usec.rem_euclid(86_400_000_000) / 1_000_000) % 60
        }
        "minute" | "minutes" => {
            (pg_usec.rem_euclid(86_400_000_000) / 60_000_000) % 60
        }
        "hour" | "hours" => {
            pg_usec.rem_euclid(86_400_000_000) / 3_600_000_000
        }
        "dow" => {
            // Day of week (0=Sunday..6=Saturday)
            // PG epoch 2000-01-01 is a Saturday (dow=6)
            let days = pg_usec.div_euclid(86_400_000_000);
            (days + 6).rem_euclid(7)
        }
        "epoch" => {
            // PG epoch is 2000-01-01, Unix epoch offset = 946684800 seconds
            (pg_usec / 1_000_000) + 946_684_800
        }
        _ => 0, // Should not happen (validated in hook)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupByColSpec {
    pub(crate) col_idx: i32,  // 0-based column index
    pub(crate) type_oid: pg_sys::Oid,
    pub(crate) expr: GroupByExpr,
}

// SAFETY: GroupByColSpec contains only value types (i32, Oid=u32, strings, enums).
unsafe impl Send for GroupByColSpec {}
unsafe impl Sync for GroupByColSpec {}

/// A HAVING filter: compare an aggregate result against a constant.
#[derive(Debug, Clone, Copy)]
pub(crate) enum HavingOp { Gt, Lt, Ge, Le, Eq, Ne }

#[derive(Debug, Clone)]
pub(crate) struct HavingFilter {
    pub(crate) agg_idx: usize,    // index into agg_specs
    pub(crate) op: HavingOp,
    pub(crate) const_val: i64,    // constant value (int8)
}

/// State for DeltaXAgg (aggregate pushdown).
pub(crate) struct AggScanState {
    pub(crate) _agg_specs: Vec<AggExecSpec>,
    pub(crate) _group_specs: Vec<GroupByColSpec>,
    pub(crate) result_rows: Vec<Vec<(pg_sys::Datum, bool)>>,
    pub(crate) result_idx: usize,
    pub(crate) _num_result_cols: usize,
    pub(crate) metadata_us: u64,
    pub(crate) heap_scan_us: u64,
    pub(crate) decompress_us: u64,
    pub(crate) agg_us: u64,
    pub(crate) total_segments: u64,
    pub(crate) total_rows_processed: u64,
    pub(crate) batch_quals_count: usize,
    pub(crate) where_quals_null: bool,
    pub(crate) regex_cache_size: u64,
    pub(crate) regex_cache_calls: u64,
    pub(crate) topn_limit: u64,
    pub(crate) topn_sort_col: i64,
    pub(crate) topn_ascending: bool,
    pub(crate) pre_topn_groups: u64,
    pub(crate) merge_us: u64,
    pub(crate) finalize_us: u64,
    pub(crate) topn_select_us: u64,
    pub(crate) n_workers: u64,
    pub(crate) bare_limit: i64,
}


/// Static CustomExecMethods struct for DeltaXAgg.
pub(crate) static DELTAX_AGG_EXEC_METHODS: SyncStatic<pg_sys::CustomExecMethods> =
    SyncStatic(pg_sys::CustomExecMethods {
        CustomName: super::super::DELTAX_AGG_NAME.as_ptr(),
        BeginCustomScan: Some(begin_agg_scan),
        ExecCustomScan: Some(exec_agg_scan),
        EndCustomScan: Some(end_agg_scan),
        ReScanCustomScan: Some(rescan_agg_scan),
        MarkPosCustomScan: None,
        RestrPosCustomScan: None,
        EstimateDSMCustomScan: None,
        InitializeDSMCustomScan: None,
        ReInitializeDSMCustomScan: None,
        InitializeWorkerCustomScan: None,
        ShutdownCustomScan: None,
        ExplainCustomScan: Some(super::super::explain::explain_agg_scan),
    });


// ============================================================================
// DeltaXAgg execution callbacks
// ============================================================================

/// CreateCustomScanState callback for DeltaXAgg.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn create_agg_scan_state(
    cscan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    unsafe {
        let css = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScanState>())
            as *mut pg_sys::CustomScanState;

        (*css).ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
        (*css).methods = &DELTAX_AGG_EXEC_METHODS.0;
        (*css).custom_ps = (*cscan).custom_private;

        css as *mut pg_sys::Node
    }
}

/// Output mapping entry: which internal data to put at this slot position.
#[derive(Debug, Clone, Copy)]
enum OutputEntry {
    Agg(usize),    // index into agg_specs
    Group(usize),  // index into group_specs
    Const(pg_sys::Datum, bool),  // constant value + is_null
}

/// All fields deserialized from a DeltaXAgg node's custom_private list.
///
/// The planner (hook.rs / path.rs) packs the aggregate plan into a flat integer
/// list because PostgreSQL's custom scan API only allows passing a `List*` through
/// `custom_private`. This struct is the Rust-side representation after parsing.
struct ParsedAggPlan {
    companion_oids: Vec<pg_sys::Oid>,
    agg_specs: Vec<AggExecSpec>,
    group_specs: Vec<GroupByColSpec>,
    output_map: Vec<OutputEntry>,
    having_filters: Vec<HavingFilter>,
    where_quals: *mut pg_sys::List,
    topn_limit: i64,
    topn_sort_col: usize,
    topn_ascending: bool,
    bare_limit: i64,
}

/// Deserialize a DeltaXAgg custom_private list into structured Rust types.
///
/// The planner serializes the aggregate plan as a flat integer list with this layout:
///   [companion_oid, ..., -1 (sentinel),
///    num_aggs, (agg_type, col_idx, result_oid, col_type_oid, expr_kind [, extra])...,
///    num_groups, (col_idx, type_oid, expr_tag [, expr-specific fields])...,
///    num_output, (type, ref)...,
///    num_having, (agg_idx, op, const_val)...,
///    where_str_len, char0, char1, ...,
///    topn_limit [, sort_col, ascending]]
///
/// String values (regexp patterns, date_trunc units, WHERE clause) are encoded as
/// (length, byte0, byte1, ...) sequences within the integer list.
unsafe fn parse_agg_private(custom_private: *mut pg_sys::List) -> ParsedAggPlan {
    unsafe {
    let list_len = (*custom_private).length;
    let mut companion_oids: Vec<pg_sys::Oid> = Vec::new();
    let mut agg_specs: Vec<AggExecSpec> = Vec::new();
    let mut group_specs: Vec<GroupByColSpec> = Vec::new();
    let mut output_map: Vec<OutputEntry> = Vec::new();

    let mut idx = 0;
    // Parse OIDs until sentinel
    while idx < list_len {
        let val = pg_sys::list_nth_int(custom_private, idx);
        idx += 1;
        if val == -1 { break; }
        companion_oids.push(pg_sys::Oid::from(val as u32));
    }
    // Parse agg specs
    if idx < list_len {
        let num_aggs = pg_sys::list_nth_int(custom_private, idx) as usize;
        idx += 1;
        for _ in 0..num_aggs {
            let agg_type_val = pg_sys::list_nth_int(custom_private, idx);
            let col_idx = pg_sys::list_nth_int(custom_private, idx + 1);
            let result_oid = pg_sys::list_nth_int(custom_private, idx + 2) as u32;
            let col_type_oid = pg_sys::list_nth_int(custom_private, idx + 3) as u32;
            let expr_kind_val = pg_sys::list_nth_int(custom_private, idx + 4);
            idx += 5;
            let agg_type = match agg_type_val {
                0 => AggType::Sum,
                1 => AggType::Count,
                2 => AggType::CountStar,
                3 => AggType::Avg,
                4 => AggType::CountDistinct,
                5 => AggType::Min,
                6 => AggType::Max,
                _ => AggType::Count,
            };
            let (expr_kind, const_offset) = match expr_kind_val {
                1 => (AggExpr::LengthOf, 0i64),
                2 => {
                    let offset = pg_sys::list_nth_int(custom_private, idx) as i64;
                    idx += 1;
                    (AggExpr::AddConst, offset)
                }
                _ => (AggExpr::Column, 0i64),
            };
            let _ = result_oid; // parsed for offset, not stored
            agg_specs.push(AggExecSpec {
                agg_type,
                col_idx,
                col_type_oid: pg_sys::Oid::from(col_type_oid),
                expr_kind,
                const_offset,
            });
        }
    }
    // Parse group specs
    if idx < list_len {
        let num_groups = pg_sys::list_nth_int(custom_private, idx) as usize;
        idx += 1;
        for _ in 0..num_groups {
            let col_idx = pg_sys::list_nth_int(custom_private, idx);
            let type_oid = pg_sys::list_nth_int(custom_private, idx + 1) as u32;
            let expr_tag = pg_sys::list_nth_int(custom_private, idx + 2);
            idx += 3;
            let expr = if expr_tag == 1 {
                // RegexpReplace: func_oid, collation, pattern_len, pattern_bytes..., replacement_len, replacement_bytes...
                let func_oid = pg_sys::list_nth_int(custom_private, idx) as u32;
                let collation = pg_sys::list_nth_int(custom_private, idx + 1) as u32;
                idx += 2;
                let pattern_len = pg_sys::list_nth_int(custom_private, idx) as usize;
                idx += 1;
                let mut pattern_bytes = Vec::with_capacity(pattern_len);
                for _ in 0..pattern_len {
                    pattern_bytes.push(pg_sys::list_nth_int(custom_private, idx) as u8);
                    idx += 1;
                }
                let pattern = String::from_utf8_lossy(&pattern_bytes).into_owned();
                let replacement_len = pg_sys::list_nth_int(custom_private, idx) as usize;
                idx += 1;
                let mut replacement_bytes = Vec::with_capacity(replacement_len);
                for _ in 0..replacement_len {
                    replacement_bytes.push(pg_sys::list_nth_int(custom_private, idx) as u8);
                    idx += 1;
                }
                let replacement = String::from_utf8_lossy(&replacement_bytes).into_owned();
                GroupByExpr::RegexpReplace { pattern, replacement, func_oid, collation }
            } else if expr_tag == 2 {
                // DateTrunc: func_oid, unit_len, unit_bytes...
                let func_oid = pg_sys::list_nth_int(custom_private, idx) as u32;
                idx += 1;
                let unit_len = pg_sys::list_nth_int(custom_private, idx) as usize;
                idx += 1;
                let mut unit_bytes = Vec::with_capacity(unit_len);
                for _ in 0..unit_len {
                    unit_bytes.push(pg_sys::list_nth_int(custom_private, idx) as u8);
                    idx += 1;
                }
                let unit = String::from_utf8_lossy(&unit_bytes).into_owned();
                let unit_usecs = date_trunc_unit_to_usecs(&unit);
                GroupByExpr::DateTrunc { unit, unit_usecs, func_oid }
            } else if expr_tag == 3 {
                // Extract: func_oid, unit_len, unit_bytes...
                let func_oid = pg_sys::list_nth_int(custom_private, idx) as u32;
                idx += 1;
                let unit_len = pg_sys::list_nth_int(custom_private, idx) as usize;
                idx += 1;
                let mut unit_bytes = Vec::with_capacity(unit_len);
                for _ in 0..unit_len {
                    unit_bytes.push(pg_sys::list_nth_int(custom_private, idx) as u8);
                    idx += 1;
                }
                let unit = String::from_utf8_lossy(&unit_bytes).into_owned();
                GroupByExpr::Extract { unit, func_oid }
            } else if expr_tag == 4 {
                // AddConst: offset_i32, op_oid
                let offset = pg_sys::list_nth_int(custom_private, idx) as i64;
                let op_oid = pg_sys::list_nth_int(custom_private, idx + 1) as u32;
                idx += 2;
                GroupByExpr::AddConst { offset, op_oid }
            } else {
                GroupByExpr::Column
            };
            group_specs.push(GroupByColSpec {
                col_idx,
                type_oid: pg_sys::Oid::from(type_oid),
                expr,
            });
        }
    }
    // Parse output mapping
    if idx < list_len {
        let num_output = pg_sys::list_nth_int(custom_private, idx) as usize;
        idx += 1;
        for _ in 0..num_output {
            let otype = pg_sys::list_nth_int(custom_private, idx);
            let oref = pg_sys::list_nth_int(custom_private, idx + 1) as usize;
            idx += 2;
            if otype == 0 {
                output_map.push(OutputEntry::Agg(oref));
            } else if otype == 2 {
                // Const output: type_oid, const_val_hi, const_val_lo, is_null
                let type_oid = pg_sys::list_nth_int(custom_private, idx) as u32;
                let val_hi = pg_sys::list_nth_int(custom_private, idx + 1) as i64;
                let val_lo = pg_sys::list_nth_int(custom_private, idx + 2) as u32 as i64;
                let is_null = pg_sys::list_nth_int(custom_private, idx + 3) != 0;
                idx += 4;
                let const_val = (val_hi << 32) | val_lo;
                let datum = if is_null {
                    pg_sys::Datum::from(0usize)
                } else {
                    // Reconstruct datum based on type
                    match pg_sys::Oid::from(type_oid) {
                        pg_sys::INT2OID => pg_sys::Datum::from(const_val as i16 as usize),
                        pg_sys::INT4OID => pg_sys::Datum::from(const_val as i32 as usize),
                        _ => pg_sys::Datum::from(const_val as usize),
                    }
                };
                output_map.push(OutputEntry::Const(datum, is_null));
            } else {
                output_map.push(OutputEntry::Group(oref));
            }
        }
    }
    // If no output mapping (backward compat), default to aggs then groups
    if output_map.is_empty() {
        for i in 0..agg_specs.len() {
            output_map.push(OutputEntry::Agg(i));
        }
        for i in 0..group_specs.len() {
            output_map.push(OutputEntry::Group(i));
        }
    }

    // Parse HAVING filters
    let mut having_filters: Vec<HavingFilter> = Vec::new();
    if idx < list_len {
        let num_having = pg_sys::list_nth_int(custom_private, idx) as usize;
        idx += 1;
        for _ in 0..num_having {
            let agg_idx = pg_sys::list_nth_int(custom_private, idx) as usize;
            let op_val = pg_sys::list_nth_int(custom_private, idx + 1);
            let const_val = pg_sys::list_nth_int(custom_private, idx + 2) as i64;
            idx += 3;
            let op = match op_val {
                0 => HavingOp::Gt,
                1 => HavingOp::Lt,
                2 => HavingOp::Ge,
                3 => HavingOp::Le,
                4 => HavingOp::Eq,
                5 => HavingOp::Ne,
                _ => HavingOp::Gt,
            };
            having_filters.push(HavingFilter { agg_idx, op, const_val });
        }
    }
    // Read WHERE quals from custom_private (serialized as string by plan_agg_path).
    // Format: [str_len, char0, char1, ...] where str_len=0 means no quals.
    let where_quals: *mut pg_sys::List = if idx < list_len {
        let str_len = pg_sys::list_nth_int(custom_private, idx) as usize;
        idx += 1;
        if str_len > 0 {
            let mut chars: Vec<u8> = Vec::with_capacity(str_len + 1);
            for _ in 0..str_len {
                chars.push(pg_sys::list_nth_int(custom_private, idx) as u8);
                idx += 1;
            }
            chars.push(0); // null terminator
            pg_sys::stringToNode(chars.as_ptr() as *const std::ffi::c_char) as *mut pg_sys::List
        } else {
            std::ptr::null_mut()
        }
    } else {
        std::ptr::null_mut()
    };
    // Parse top-N info
    let mut topn_limit: i64 = 0;
    let mut topn_sort_col: usize = 0;
    let mut topn_ascending: bool = true;
    let mut bare_limit: i64 = 0;
    if idx < list_len {
        let limit_val = pg_sys::list_nth_int(custom_private, idx);
        idx += 1;
        if limit_val > 0 {
            let sort_col_raw = pg_sys::list_nth_int(custom_private, idx);
            idx += 1;
            topn_ascending = pg_sys::list_nth_int(custom_private, idx) != 0;
            idx += 1;
            if sort_col_raw < 0 {
                bare_limit = limit_val as i64; // bare LIMIT, no sort
            } else {
                topn_limit = limit_val as i64;
                topn_sort_col = sort_col_raw as usize;
            }
        }
    }
    let _ = idx;

    ParsedAggPlan {
        companion_oids,
        agg_specs,
        group_specs,
        output_map,
        having_filters,
        where_quals,
        topn_limit,
        topn_sort_col,
        topn_ascending,
        bare_limit,
    }
    } // unsafe
}

/// Try to answer a scalar aggregate query entirely from catalog metadata,
/// without loading or scanning any segments.
///
/// This is the fastest possible path: it uses pre-computed row counts
/// stored in deltax_partition by mark_partition_compressed().
/// Only works for ungrouped, unfiltered queries where every aggregate is COUNT(*).
///
/// The caller provides pre-fetched catalog data so this function has no
/// external dependencies and is easy to test:
/// - `row_counts`: one `Option<i64>` per companion OID (from `get_row_count`)
///
/// Returns Some(state) if the shortcut succeeded, None to fall through to
/// segment-based execution.
fn try_catalog_shortcut(
    plan: &ParsedAggPlan,
    _meta: &super::segments::MetadataInfo,
    row_counts: &[Option<i64>],
    metadata_us: u64,
) -> Option<AggScanState> {
    if !plan.group_specs.is_empty()
        || !plan.where_quals.is_null()
        || !plan.having_filters.is_empty()
    {
        return None;
    }

    let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
    for spec in &plan.agg_specs {
        match spec.agg_type {
            AggType::CountStar => {
                let mut total: i64 = 0;
                for rc in row_counts {
                    total += (*rc)?;
                }
                agg_results.push((pg_sys::Datum::from(total as usize), false));
            }
            _ => return None, // Non-catalog-answerable agg
        }
    }

    let num_result_cols = plan.output_map.len();
    let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
    for entry in &plan.output_map {
        match entry {
            OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
            OutputEntry::Group(_) => row.push((pg_sys::Datum::from(0usize), true)),
            OutputEntry::Const(d, n) => row.push((*d, *n)),
        }
    }
    Some(AggScanState {
        _agg_specs: Vec::new(),
        _group_specs: Vec::new(),
        result_rows: vec![row],
        result_idx: 0,
        _num_result_cols: num_result_cols,
        metadata_us,
        heap_scan_us: 0,
        decompress_us: 0,
        agg_us: 0,
        total_segments: 0,
        total_rows_processed: 0,
        batch_quals_count: 0,
        where_quals_null: true,
        regex_cache_size: 0,
        regex_cache_calls: 0,
        topn_limit: 0,
        topn_sort_col: 0,
        topn_ascending: true,
        pre_topn_groups: 0,
        merge_us: 0,
        finalize_us: 0,
        topn_select_us: 0,
        n_workers: 0,
        bare_limit: 0,
    })
}

/// Try to compute scalar aggregates using only per-segment metadata
/// (sum, nonnull_count, min, max) stored in the companion table, without
/// decompressing any column data.
///
/// This works for ungrouped, unfiltered queries where every aggregate is
/// SUM/AVG/COUNT on numeric columns, COUNT(*), or MIN/MAX. The companion
/// table stores pre-computed _sum_*, _min_*, _max_*, and _nonnull_count_*
/// columns for each segment, so we just accumulate across segments.
///
/// For SUM(col + C), we use the identity: SUM(col + C) = SUM(col) + C * COUNT(col).
///
/// The caller loads segments (via `load_segments_heap`) and passes them in,
/// keeping this function free of I/O and easy to test.
///
/// Returns Some(state) if metadata was sufficient, None to fall through to
/// full decompression.
fn try_metadata_fast_path(
    plan: &ParsedAggPlan,
    meta: &super::segments::MetadataInfo,
    segments: &[SegmentData],
    metadata_us: u64,
    heap_scan_us: u64,
) -> Option<AggScanState> {
    if !plan.group_specs.is_empty()
        || !plan.where_quals.is_null()
        || !plan.having_filters.is_empty()
    {
        return None;
    }

    // Check that all agg specs are metadata-resolvable
    let all_resolvable = plan.agg_specs.iter().all(|spec| {
        match spec.agg_type {
            AggType::CountStar => true,
            AggType::Sum => {
                (spec.expr_kind == AggExpr::Column || spec.expr_kind == AggExpr::AddConst)
                    && spec.col_idx >= 0 && {
                    let t = spec.col_type_oid;
                    t == pg_sys::INT2OID || t == pg_sys::INT4OID || t == pg_sys::INT8OID
                        || t == pg_sys::FLOAT4OID || t == pg_sys::FLOAT8OID
                }
            }
            AggType::Avg | AggType::Count => {
                spec.expr_kind == AggExpr::Column && spec.col_idx >= 0 && {
                    let t = spec.col_type_oid;
                    t == pg_sys::INT2OID || t == pg_sys::INT4OID || t == pg_sys::INT8OID
                        || t == pg_sys::FLOAT4OID || t == pg_sys::FLOAT8OID
                }
            }
            AggType::Min | AggType::Max => {
                spec.expr_kind == AggExpr::Column && spec.col_idx >= 0
            }
            _ => false,
        }
    });
    if !all_resolvable {
        return None;
    }

    // Check that metadata actually exists for all needed columns in all segments
    let sums_available = plan.agg_specs.iter().all(|spec| {
        match spec.agg_type {
            AggType::Sum | AggType::Avg | AggType::Count => {
                let col_name = &meta.col_names[spec.col_idx as usize];
                segments.is_empty()
                    || segments.iter().all(|seg| seg.col_sums.contains_key(col_name))
            }
            _ => true,
        }
    });
    let minmax_available = plan.agg_specs.iter().all(|spec| {
        match spec.agg_type {
            AggType::Min | AggType::Max => {
                let col_name = &meta.col_names[spec.col_idx as usize];
                segments.is_empty()
                    || segments.iter().all(|seg| seg.col_minmax.contains_key(col_name))
            }
            _ => true,
        }
    });

    if !sums_available || !minmax_available {
        return None;
    }

    // Accumulate from metadata
    let mut accumulators: Vec<AggAccumulator> = plan.agg_specs
        .iter()
        .map(|spec| AggAccumulator::new_for(spec.agg_type, spec.col_type_oid))
        .collect();

    for seg in segments {
        if seg.row_count == 0 { continue; }
        for (i, spec) in plan.agg_specs.iter().enumerate() {
            match spec.agg_type {
                AggType::CountStar => {
                    if let AggAccumulator::Count { count } = &mut accumulators[i] {
                        *count += seg.row_count as i64;
                    }
                }
                AggType::Count => {
                    let col_name = &meta.col_names[spec.col_idx as usize];
                    if let Some(cs) = seg.col_sums.get(col_name)
                        && let AggAccumulator::Count { count } = &mut accumulators[i]
                    {
                        *count += cs.nonnull_count;
                    }
                }
                AggType::Sum | AggType::Avg => {
                    let col_name = &meta.col_names[spec.col_idx as usize];
                    if let Some(cs) = seg.col_sums.get(col_name) {
                        if cs.sum_null { continue; }
                        // For SUM(col + C): SUM(col) + C * nonnull_count
                        let add_const = if spec.expr_kind == AggExpr::AddConst {
                            spec.const_offset
                        } else {
                            0
                        };
                        match &mut accumulators[i] {
                            AggAccumulator::SumInt { sum, count } => {
                                // Sum datum is NUMERIC — extract via numeric_out, parse as i128
                                let s = unsafe {
                                    let cstr = pg_sys::OidOutputFunctionCall(
                                        pg_sys::Oid::from(1702u32), // numeric_out
                                        cs.sum_datum,
                                    );
                                    let s = std::ffi::CStr::from_ptr(cstr)
                                        .to_string_lossy()
                                        .into_owned();
                                    pg_sys::pfree(cstr as *mut _);
                                    s
                                };
                                if let Ok(v) = s.parse::<i128>() {
                                    *sum += v + add_const as i128 * cs.nonnull_count as i128;
                                    *count += cs.nonnull_count;
                                }
                            }
                            AggAccumulator::SumFloat { sum, count } => {
                                // Sum datum is FLOAT8
                                let f = f64::from_bits(cs.sum_datum.value() as u64);
                                *sum += f + add_const as f64 * cs.nonnull_count as f64;
                                *count += cs.nonnull_count;
                            }
                            _ => {}
                        }
                    }
                }
                AggType::Min => {
                    let col_name = &meta.col_names[spec.col_idx as usize];
                    if let Some(cm) = seg.col_minmax.get(col_name) {
                        if cm.min_null { continue; }
                        match &mut accumulators[i] {
                            AggAccumulator::MinInt { val } => {
                                let v = cm.min_datum.value() as i64;
                                *val = Some(val.map_or(v, |cur| cur.min(v)));
                            }
                            AggAccumulator::MinFloat { val } => {
                                let v = f64::from_bits(cm.min_datum.value() as u64);
                                *val = Some(val.map_or(v, |cur| if v < cur { v } else { cur }));
                            }
                            _ => {}
                        }
                    }
                }
                AggType::Max => {
                    let col_name = &meta.col_names[spec.col_idx as usize];
                    if let Some(cm) = seg.col_minmax.get(col_name) {
                        if cm.max_null { continue; }
                        match &mut accumulators[i] {
                            AggAccumulator::MaxInt { val } => {
                                let v = cm.max_datum.value() as i64;
                                *val = Some(val.map_or(v, |cur| cur.max(v)));
                            }
                            AggAccumulator::MaxFloat { val } => {
                                let v = f64::from_bits(cm.max_datum.value() as u64);
                                *val = Some(val.map_or(v, |cur| if v > cur { v } else { cur }));
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
    }

    // Finalize accumulators
    let num_result_cols = plan.output_map.len();
    let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
    for (i, acc) in accumulators.iter().enumerate() {
        agg_results.push(unsafe { finalize_accumulator(acc, &plan.agg_specs[i]) });
    }
    let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
    for entry in &plan.output_map {
        match entry {
            OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
            OutputEntry::Group(_) => row.push((pg_sys::Datum::from(0usize), true)),
            OutputEntry::Const(d, n) => row.push((*d, *n)),
        }
    }

    let total_segments = segments.len() as u64;
    Some(AggScanState {
        _agg_specs: Vec::new(),
        _group_specs: Vec::new(),
        result_rows: vec![row],
        result_idx: 0,
        _num_result_cols: num_result_cols,
        metadata_us,
        heap_scan_us,
        decompress_us: 0,
        agg_us: 0,
        total_segments,
        total_rows_processed: 0,
        batch_quals_count: 0,
        where_quals_null: true,
        regex_cache_size: 0,
        regex_cache_calls: 0,
        topn_limit: 0,
        topn_sort_col: 0,
        topn_ascending: true,
        pre_topn_groups: 0,
        merge_us: 0,
        finalize_us: 0,
        topn_select_us: 0,
        n_workers: 0,
        bare_limit: 0,
    })
}

/// BeginCustomScan callback for DeltaXAgg: decompress and aggregate.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn begin_agg_scan(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: i32,
) {
    unsafe {
        let custom_private = (*node).custom_ps;
        if custom_private.is_null() {
            pgrx::error!("pg_deltax: missing custom_private in DeltaXAgg state");
        }

        let plan = parse_agg_private(custom_private);

        if plan.companion_oids.is_empty() {
            pgrx::error!("pg_deltax: DeltaXAgg has no companion tables");
        }

        // Get first companion table name for metadata
        let first_name = {
            let name_ptr = pg_sys::get_rel_name(plan.companion_oids[0]);
            if name_ptr.is_null() {
                pgrx::error!(
                    "pg_deltax: companion table not found for OID {}",
                    u32::from(plan.companion_oids[0])
                );
            }
            std::ffi::CStr::from_ptr(name_ptr)
                .to_string_lossy()
                .into_owned()
        };

        // Load metadata via SPI
        let t0 = Instant::now();
        let meta = Spi::connect(|client| load_metadata(client, &first_name));
        let metadata_us = t0.elapsed().as_micros() as u64;

        // Fast path 1: answer from catalog metadata (no segment scan at all)
        {
            let row_counts: Vec<Option<i64>> = plan.companion_oids.iter()
                .map(|&oid| super::super::cost::get_row_count(oid))
                .collect();
            if let Some(state) = try_catalog_shortcut(&plan, &meta, &row_counts, metadata_us) {
                let state_ptr = Box::into_raw(Box::new(state));
                (*node).custom_ps = state_ptr as *mut pg_sys::List;
                return;
            }
        }

        // Fast path 2: answer from per-segment metadata (no decompression)
        {
            let needs_sums = plan.agg_specs.iter().any(|s| matches!(s.agg_type, AggType::Sum | AggType::Avg));
            let needs_counts = plan.agg_specs.iter().any(|s| matches!(s.agg_type, AggType::Count));
            let needs_minmax = plan.agg_specs.iter().any(|s| matches!(s.agg_type, AggType::Min | AggType::Max));
            let needed_cols = vec![false; meta.col_names.len()];

            let t1 = Instant::now();
            let mut all_segments: Vec<SegmentData> = Vec::new();
            for &oid in &plan.companion_oids {
                let (segs, _, _) = load_segments_heap(
                    oid, &meta.col_names, &meta.segment_by, &needed_cols,
                    &meta.time_column, needs_minmax, &[], None, None, None,
                    &[], needs_sums || needs_counts,
                );
                all_segments.extend(segs);
            }
            let heap_scan_us = t1.elapsed().as_micros() as u64;

            if let Some(state) = try_metadata_fast_path(&plan, &meta, &all_segments, metadata_us, heap_scan_us) {
                let state_ptr = Box::into_raw(Box::new(state));
                (*node).custom_ps = state_ptr as *mut pg_sys::List;
                return;
            }
        }

        // Destructure plan for the full scan path
        let ParsedAggPlan {
            companion_oids, agg_specs, group_specs, output_map,
            having_filters, where_quals, topn_limit, topn_sort_col, topn_ascending,
            bare_limit,
        } = plan;

        // Build needed_cols: only columns referenced by aggregates and group-by
        let num_cols = meta.col_names.len();
        let mut needed_cols = vec![false; num_cols];
        for spec in &agg_specs {
            if spec.col_idx >= 0 && (spec.col_idx as usize) < num_cols {
                needed_cols[spec.col_idx as usize] = true;
            }
        }
        for gs in &group_specs {
            if gs.col_idx >= 0 && (gs.col_idx as usize) < num_cols {
                needed_cols[gs.col_idx as usize] = true;
            }
        }

        // Build length_cols: columns where ALL referencing agg specs use LengthOf.
        // These columns will be decompressed as int4 lengths instead of text datums.
        let length_cols: Vec<bool> = (0..num_cols)
            .map(|col_idx| {
                let refs: Vec<&AggExecSpec> = agg_specs
                    .iter()
                    .filter(|s| s.col_idx >= 0 && s.col_idx as usize == col_idx)
                    .collect();
                !refs.is_empty() && refs.iter().all(|s| s.expr_kind == AggExpr::LengthOf)
            })
            .collect();

        // Build count_distinct_only_str: text columns where ALL referencing agg specs
        // are CountDistinct and the column is not used in GROUP BY.  These can be
        // pre-accumulated directly from compressed data, skipping datum conversion.
        let count_distinct_only_str: Vec<bool> = (0..num_cols)
            .map(|col_idx| {
                let is_text = matches!(
                    meta.col_types[col_idx],
                    x if x == pg_sys::TEXTOID || x == pg_sys::VARCHAROID || x == pg_sys::BPCHAROID
                );
                if !is_text { return false; }
                let refs: Vec<&AggExecSpec> = agg_specs
                    .iter()
                    .filter(|s| s.col_idx >= 0 && s.col_idx as usize == col_idx)
                    .collect();
                if refs.is_empty() { return false; }
                let all_cd = refs.iter().all(|s| s.agg_type == AggType::CountDistinct);
                let in_group_by = group_specs.iter().any(|gs| gs.col_idx as usize == col_idx);
                all_cd && !in_group_by
            })
            .collect();

        // Build count_distinct_only_int: integer columns where ALL referencing agg specs
        // are CountDistinct and the column is not used in GROUP BY.  These can be
        // pre-accumulated directly from compressed data, skipping datum conversion.
        let count_distinct_only_int: Vec<bool> = (0..num_cols)
            .map(|col_idx| {
                let is_int = meta.col_types[col_idx] == pg_sys::INT2OID
                    || meta.col_types[col_idx] == pg_sys::INT4OID
                    || meta.col_types[col_idx] == pg_sys::INT8OID;
                if !is_int { return false; }
                let refs: Vec<&AggExecSpec> = agg_specs
                    .iter()
                    .filter(|s| s.col_idx >= 0 && s.col_idx as usize == col_idx)
                    .collect();
                if refs.is_empty() { return false; }
                let all_cd = refs.iter().all(|s| s.agg_type == AggType::CountDistinct);
                let in_group_by = group_specs.iter().any(|gs| gs.col_idx as usize == col_idx);
                all_cd && !in_group_by
            })
            .collect();

        // Extract batch quals and segment filters from WHERE clause (quals from custom_private)
        let (batch_quals, _handled_count) = extract_batch_quals(where_quals, &meta.col_names, &meta.col_types);

        for bq in &batch_quals {
            if bq.col_idx < num_cols {
                needed_cols[bq.col_idx] = true;
            }
        }
        let (seg_filters, time_min, time_max) = extract_segment_filters(
            where_quals,
            &meta.col_names,
            &meta.segment_by,
            &meta.time_column,
        );
        // Load segments from all companion tables (with lazy pruning)
        let t1 = Instant::now();
        let mut all_segments: Vec<SegmentData> = Vec::new();
        for &oid in &companion_oids {
            let (segs, _, _) = load_segments_heap(
                oid, &meta.col_names, &meta.segment_by, &needed_cols,
                &meta.time_column, false, &seg_filters, time_min, time_max, None,
                &batch_quals, false,
            );
            all_segments.extend(segs);
        }
        let heap_scan_us = t1.elapsed().as_micros() as u64;

        // Create per-segment memory context
        let query_ctx = (*(*node).ss.ps.state).es_query_cxt;
        let segment_mcxt = pg_sys::AllocSetContextCreateInternal(
            query_ctx,
            c"DeltaXAggSegment".as_ptr(),
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        );

        // Initialize accumulators
        let has_group_by = !group_specs.is_empty();
        let num_result_cols = output_map.len();

        let prototype_accumulators: Vec<AggAccumulator> = agg_specs
            .iter()
            .map(|spec| AggAccumulator::new_for(spec.agg_type, spec.col_type_oid))
            .collect();

        let mut global_accumulators = if !has_group_by {
            Some(prototype_accumulators.iter().map(|a| a.clone_fresh()).collect::<Vec<_>>())
        } else {
            None
        };
        let mut group_map: GroupMap = GroupMap::with_hasher(BuildHasherDefault::default());
        let mut string_arena = StringArena::new();
        // Flat accumulator storage: group i's accumulators are at
        // flat_accs[i * n_agg_specs .. (i+1) * n_agg_specs]
        let n_agg_specs = agg_specs.len();
        let mut flat_accs: Vec<AggAccumulator> = Vec::new();
        let is_single_group_key = group_specs.len() == 1;

        // Compact path: use flat byte buffer for accumulators when possible
        let use_compact_accs = has_group_by && can_use_compact_accs(&agg_specs);
        let mut compact_storage = if use_compact_accs {
            Some(CompactAccStorage::new(CompactAccLayout::new(&agg_specs)))
        } else {
            None
        };

        // Compact keys: pack integer GROUP BY keys into u128
        let use_compact_keys = has_group_by && can_use_compact_keys(&group_specs);
        let mut compact_group_map: CompactGroupMap = CompactGroupMap::with_hasher(BuildHasherDefault::default());

        // Check if any GROUP BY uses RegexpReplace — set up cross-segment caches
        let has_regexp_group = group_specs.iter().any(|gs| matches!(gs.expr, GroupByExpr::RegexpReplace { .. }));

        // Cross-segment regex dedup cache: input_string → regexp_replace result
        let mut regex_cache: HashMap<String, String> = HashMap::new();
        let mut regex_cache_calls: u64 = 0;

        // Build PG datums for regexp pattern/replacement (once, not per-segment)
        // and identify which columns need raw string decompression
        struct RegexpGroupInfo {
            group_idx: usize,
            func_oid: pg_sys::Oid,
            collation: pg_sys::Oid,
            pattern_datum: pg_sys::Datum,
            replacement_datum: pg_sys::Datum,
        }
        let mut regexp_group_infos: Vec<RegexpGroupInfo> = Vec::new();
        // Columns that need raw string decompression instead of PG datum decompression
        let mut raw_string_cols: Vec<bool> = vec![false; meta.col_names.len()];

        if has_regexp_group {
            for (gi, gs) in group_specs.iter().enumerate() {
                if let GroupByExpr::RegexpReplace { ref pattern, ref replacement, func_oid, collation } = gs.expr {
                    raw_string_cols[gs.col_idx as usize] = true;
                    let pattern_datum = {
                        let text = pg_sys::cstring_to_text_with_len(pattern.as_ptr() as *const _, pattern.len() as i32);
                        pg_sys::Datum::from(text as usize)
                    };
                    let replacement_datum = {
                        let text = pg_sys::cstring_to_text_with_len(replacement.as_ptr() as *const _, replacement.len() as i32);
                        pg_sys::Datum::from(text as usize)
                    };
                    regexp_group_infos.push(RegexpGroupInfo {
                        group_idx: gi,
                        func_oid: pg_sys::Oid::from(func_oid),
                        collation: pg_sys::Oid::from(collation),
                        pattern_datum,
                        replacement_datum,
                    });
                }
            }
        }

        // Mark text columns used for Min/Max aggregations as raw_string_cols
        // only when the compact accumulator path is active (MinStr/MaxStr need raw bytes).
        // The generic path handles MIN/MAX text via normal PG datums.
        if use_compact_accs {
            for spec in &agg_specs {
                if matches!(spec.agg_type, AggType::Min | AggType::Max) {
                    let t = spec.col_type_oid;
                    if t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID {
                        raw_string_cols[spec.col_idx as usize] = true;
                    }
                }
            }
        }

        // Identify text GROUP BY columns (dictionary or LZ4)
        let mut text_group_cols: Vec<bool> = vec![false; meta.col_names.len()];
        for gs in &group_specs {
            if matches!(gs.expr, GroupByExpr::Column)
                && (gs.type_oid == pg_sys::TEXTOID
                    || gs.type_oid == pg_sys::VARCHAROID
                    || gs.type_oid == pg_sys::BPCHAROID
                    || gs.type_oid == pg_sys::NAMEOID)
            {
                text_group_cols[gs.col_idx as usize] = true;
            }
        }

        // Per-segment decoded text data for GROUP BY columns.
        // Keeps decompressed string data alive during the row loop,
        // providing O(1) &str access per row without interning.
        let mut seg_text_columns: Vec<Option<SegTextColumn>> = Vec::new();

        // ============================================================
        // PARALLEL COMPACT PATH: multi-threaded segment processing
        // ============================================================
        // Conditions: compact keys + compact accs + all needed cols numeric +
        // all batch quals numeric + no regexp GROUP BY + enough segments.
        let n_workers = crate::get_parallel_workers();
        let can_parallel = use_compact_keys
            && use_compact_accs
            && n_workers > 1
            && all_segments.len() > 1
            && !has_regexp_group
            && all_needed_cols_numeric(&needed_cols, &meta.col_types)
            && batch_quals_all_numeric(&batch_quals);

        if can_parallel {
            let t2 = Instant::now();
            // If top-N is active with no HAVING, tell workers to compute
            // top-K candidates while their data is still cache-hot.
            let topn_spec = if topn_limit > 0 && having_filters.is_empty() {
                let sort_slot = match output_map[topn_sort_col] {
                    OutputEntry::Agg(ai) => ai,
                    _ => unreachable!(),
                };
                let k = (topn_limit as usize).max(1000);
                Some((sort_slot, k, topn_ascending))
            } else {
                None
            };

            let config = ParallelCompactConfig {
                agg_specs: &agg_specs,
                group_specs: &group_specs,
                col_names: &meta.col_names,
                col_types: &meta.col_types,
                segment_by: &meta.segment_by,
                needed_cols: &needed_cols,
                batch_quals: &batch_quals,
                seg_filters: &seg_filters,
                time_min,
                time_max,
                topn_spec,
            };

            let chunk_size = (all_segments.len() + n_workers - 1) / n_workers;
            let partial_results: Vec<ParallelCompactResult> = std::thread::scope(|s| {
                let handles: Vec<_> = all_segments.chunks(chunk_size).map(|chunk| {
                    let cfg = &config;
                    s.spawn(move || process_segments_compact(chunk, cfg))
                }).collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });

            // Accumulate stats from all workers
            let scan_wall_us = t2.elapsed().as_micros() as u64;
            let mut total_segments: u64 = 0;
            let mut total_rows_processed: u64 = 0;
            let mut decompress_us: u64 = 0;
            for result in &partial_results {
                total_segments += result.segments_processed;
                total_rows_processed += result.rows_processed;
                decompress_us = decompress_us.max(result.decompress_us);
            }
            let agg_us = scan_wall_us.saturating_sub(decompress_us);

            // ----------------------------------------------------------
            // Speculative top-N: use pre-computed top-K candidates from
            // each worker (computed while data was cache-hot), merge only
            // those, and verify no missed key could beat the Nth result.
            // ----------------------------------------------------------
            if topn_limit > 0 && having_filters.is_empty() {
                let sort_slot = match output_map[topn_sort_col] {
                    OutputEntry::Agg(ai) => ai,
                    _ => unreachable!(),
                };
                let (_, sort_kind) = compact_storage.as_ref().unwrap().layout.slots[sort_slot];
                let limit = topn_limit as usize;
                let k = (topn_limit as usize).max(1000);

                let read_sort = |storage: &CompactAccStorage, group_idx: u32| -> i64 {
                    match sort_kind {
                        CompactAccKind::Count => storage.read_count(group_idx, sort_slot),
                        CompactAccKind::SumIntNarrow => storage.read_sum_int_narrow(group_idx, sort_slot).0,
                        _ => storage.read_count(group_idx, sort_slot),
                    }
                };

                let t_spec = Instant::now();

                // Phase 1: Collect pre-computed top-K candidates from workers
                let mut candidate_set: hashbrown::HashSet<u128, BuildHasherDefault<ahash::AHasher>> =
                    hashbrown::HashSet::with_capacity_and_hasher(
                        k * partial_results.len(), BuildHasherDefault::default(),
                    );
                let mut floor_sum: i64 = 0;
                for result in &partial_results {
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
                    for result in &partial_results {
                        if let Some(&gidx) = result.compact_map.get(&key) {
                            total = total.saturating_add(read_sort(&result.compact_storage, gidx));
                        }
                    }
                    merged.push((total, key));
                }

                // Phase 3: Sort and take top-N
                if !topn_ascending {
                    merged.sort_unstable_by(|a, b| b.0.cmp(&a.0));
                } else {
                    merged.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                }
                merged.truncate(limit);

                // Phase 4: Correctness check — can any missed key beat the Nth result?
                let speculative_ok = if merged.len() >= limit {
                    let nth_value = merged[limit - 1].0;
                    if !topn_ascending {
                        nth_value > floor_sum  // missed key total ≤ floor_sum
                    } else {
                        nth_value < floor_sum  // missed key total ≥ floor_sum
                    }
                } else {
                    false
                };

                let topn_select_us = t_spec.elapsed().as_micros() as u64;

                if speculative_ok {
                    // Phase 5: For each winner, merge all accumulators and finalize
                    let t_fin = Instant::now();
                    let storage = compact_storage.as_mut().unwrap();
                    let num_group_keys = group_specs.len();
                    let mut result_rows = Vec::with_capacity(merged.len());

                    for &(_, packed_key) in &merged {
                        let global_idx = storage.alloc_group();

                        // Targeted merge: only this key's accumulators across workers
                        for result in &partial_results {
                            if let Some(&worker_idx) = result.compact_map.get(&packed_key) {
                                for (slot_idx, _) in agg_specs.iter().enumerate() {
                                    let (_, kind) = storage.layout.slots[slot_idx];
                                    match kind {
                                        CompactAccKind::Count => {
                                            let wc = result.compact_storage.read_count(worker_idx, slot_idx);
                                            *storage.count_mut(global_idx, slot_idx) += wc;
                                        }
                                        CompactAccKind::SumInt => {
                                            let (ws, wc) = result.compact_storage.read_sum_int(worker_idx, slot_idx);
                                            let (gs, gc) = storage.sum_int_mut(global_idx, slot_idx);
                                            *gs += ws;
                                            *gc += wc;
                                        }
                                        CompactAccKind::SumIntNarrow => {
                                            let (ws, wc) = result.compact_storage.read_sum_int_narrow(worker_idx, slot_idx);
                                            let (gs, gc) = storage.sum_int_narrow_mut(global_idx, slot_idx);
                                            *gs += ws;
                                            *gc += wc;
                                        }
                                        CompactAccKind::SumFloat => {
                                            let (ws, wc) = result.compact_storage.read_sum_float(worker_idx, slot_idx);
                                            let (gs, gc) = storage.sum_float_mut(global_idx, slot_idx);
                                            *gs += ws;
                                            *gc += wc;
                                        }
                                        CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                            let (w_off, w_len) = result.compact_storage.read_min_max_str(worker_idx, slot_idx);
                                            if w_off != u32::MAX {
                                                let w_str = result.compact_storage.str_arena.get(w_off, w_len);
                                                let (g_off, g_len) = storage.read_min_max_str(global_idx, slot_idx);
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
                                                    storage.write_min_max_str(global_idx, slot_idx, new_off, new_len);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Finalize this group
                        let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                        for (spec_idx, spec) in agg_specs.iter().enumerate() {
                            agg_results.push(compact_finalize(storage, global_idx, spec_idx, spec));
                        }
                        let keys = unpack_int_keys(packed_key, num_group_keys);
                        let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                        for entry in &output_map {
                            match entry {
                                OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                                OutputEntry::Group(gi) => {
                                    let v = keys[*gi];
                                    if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. }) {
                                        row.push((i128_to_numeric_datum(v as i128), false));
                                    } else {
                                        row.push((pg_sys::Datum::from(v as usize), false));
                                    }
                                }
                                OutputEntry::Const(d, n) => row.push((*d, *n)),
                            }
                        }
                        result_rows.push(row);
                    }
                    let finalize_us = t_fin.elapsed().as_micros() as u64;
                    let merge_us = 0u64; // no full merge performed

                    let pre_topn_groups: usize = partial_results.iter()
                        .map(|r| r.compact_map.len()).sum();

                    let state = AggScanState {
                        _agg_specs: agg_specs,
                        _group_specs: group_specs,
                        result_rows,
                        result_idx: 0,
                        _num_result_cols: num_result_cols,
                        metadata_us,
                        heap_scan_us,
                        decompress_us,
                        agg_us,
                        total_segments,
                        total_rows_processed,
                        batch_quals_count: batch_quals.len(),
                        where_quals_null: where_quals.is_null(),
                        regex_cache_size: 0,
                        regex_cache_calls: 0,
                        topn_limit: topn_limit as u64,
                        topn_sort_col: topn_sort_col as i64,
                        topn_ascending,
                        pre_topn_groups: pre_topn_groups as u64,
                        merge_us,
                        finalize_us,
                        topn_select_us,
                        n_workers: n_workers as u64,
                        bare_limit: 0,
                    };

                    let state_box = Box::new(state);
                    let state_ptr = Box::into_raw(state_box);
                    (*node).custom_ps = state_ptr as *mut pg_sys::List;
                    return;
                }
                // Speculation failed — fall through to full merge
                let spec_fail_us = t_spec.elapsed().as_micros() as u64;
                pgrx::log!(
                    "pg_deltax speculative top-N failed: candidates={} k={} nth={} floor_sum={} (wasted {:.1}ms)",
                    merged.len(), k,
                    merged.get(limit.saturating_sub(1)).map(|x| x.0).unwrap_or(0),
                    floor_sum,
                    spec_fail_us as f64 / 1000.0,
                );
            }

            // ----------------------------------------------------------
            // Bare LIMIT short-circuit for compact path: pick N groups
            // from largest worker, merge only those, finalize only those
            // ----------------------------------------------------------
            if bare_limit > 0 && having_filters.is_empty() {
                let n = bare_limit as usize;
                let t_merge = Instant::now();

                let largest_idx = partial_results.iter().enumerate()
                    .max_by_key(|(_, r)| r.compact_map.len())
                    .map(|(i, _)| i)
                    .unwrap_or(0);

                let target_keys: Vec<u128> = partial_results[largest_idx].compact_map
                    .keys().take(n).copied().collect();

                let storage = compact_storage.as_mut().unwrap();
                let num_group_keys = group_specs.len();

                let pre_topn_groups: usize = partial_results.iter()
                    .map(|r| r.compact_map.len()).sum();

                let mut result_rows = Vec::with_capacity(n);
                for &packed_key in &target_keys {
                    let global_idx = storage.alloc_group();

                    // Targeted merge: only this key's accumulators across workers
                    for result in &partial_results {
                        if let Some(&worker_idx) = result.compact_map.get(&packed_key) {
                            for (slot_idx, _) in agg_specs.iter().enumerate() {
                                let (_, kind) = storage.layout.slots[slot_idx];
                                match kind {
                                    CompactAccKind::Count => {
                                        let wc = result.compact_storage.read_count(worker_idx, slot_idx);
                                        *storage.count_mut(global_idx, slot_idx) += wc;
                                    }
                                    CompactAccKind::SumInt => {
                                        let (ws, wc) = result.compact_storage.read_sum_int(worker_idx, slot_idx);
                                        let (gs, gc) = storage.sum_int_mut(global_idx, slot_idx);
                                        *gs += ws;
                                        *gc += wc;
                                    }
                                    CompactAccKind::SumIntNarrow => {
                                        let (ws, wc) = result.compact_storage.read_sum_int_narrow(worker_idx, slot_idx);
                                        let (gs, gc) = storage.sum_int_narrow_mut(global_idx, slot_idx);
                                        *gs += ws;
                                        *gc += wc;
                                    }
                                    CompactAccKind::SumFloat => {
                                        let (ws, wc) = result.compact_storage.read_sum_float(worker_idx, slot_idx);
                                        let (gs, gc) = storage.sum_float_mut(global_idx, slot_idx);
                                        *gs += ws;
                                        *gc += wc;
                                    }
                                    CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                        let (w_off, w_len) = result.compact_storage.read_min_max_str(worker_idx, slot_idx);
                                        if w_off != u32::MAX {
                                            let w_str = result.compact_storage.str_arena.get(w_off, w_len);
                                            let (g_off, g_len) = storage.read_min_max_str(global_idx, slot_idx);
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
                                                storage.write_min_max_str(global_idx, slot_idx, new_off, new_len);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Finalize this group
                    let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                    for (spec_idx, spec) in agg_specs.iter().enumerate() {
                        agg_results.push(compact_finalize(storage, global_idx, spec_idx, spec));
                    }
                    let keys = unpack_int_keys(packed_key, num_group_keys);
                    let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                    for entry in &output_map {
                        match entry {
                            OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                            OutputEntry::Group(gi) => {
                                let v = keys[*gi];
                                if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. }) {
                                    row.push((i128_to_numeric_datum(v as i128), false));
                                } else {
                                    row.push((pg_sys::Datum::from(v as usize), false));
                                }
                            }
                            OutputEntry::Const(d, n) => row.push((*d, *n)),
                        }
                    }
                    result_rows.push(row);
                }
                let merge_us = t_merge.elapsed().as_micros() as u64;

                let state = AggScanState {
                    _agg_specs: agg_specs,
                    _group_specs: group_specs,
                    result_rows,
                    result_idx: 0,
                    _num_result_cols: num_result_cols,
                    metadata_us,
                    heap_scan_us,
                    decompress_us,
                    agg_us,
                    total_segments,
                    total_rows_processed,
                    batch_quals_count: batch_quals.len(),
                    where_quals_null: where_quals.is_null(),
                    regex_cache_size: 0,
                    regex_cache_calls: 0,
                    topn_limit: 0,
                    topn_sort_col: -1,
                    topn_ascending,
                    pre_topn_groups: pre_topn_groups as u64,
                    merge_us,
                    finalize_us: 0,
                    topn_select_us: 0,
                    n_workers: n_workers as u64,
                    bare_limit,
                };

                let state_box = Box::new(state);
                let state_ptr = Box::into_raw(state_box);
                (*node).custom_ps = state_ptr as *mut pg_sys::List;
                return;
            }

            // ----------------------------------------------------------
            // Partitioned parallel merge + top-N: partition key space
            // across threads, each merges its slice and finds local
            // top-N, then merge the local results.
            // ----------------------------------------------------------
            if topn_limit > 0 && having_filters.is_empty() {
                let t_merge = Instant::now();
                let limit = topn_limit as usize;
                let sort_slot = match output_map[topn_sort_col] {
                    OutputEntry::Agg(ai) => ai,
                    _ => unreachable!(),
                };
                let n_partitions = n_workers;

                let pre_topn_groups: usize = partial_results.iter()
                    .map(|r| r.compact_map.len()).sum();

                // Each partition thread: merge its slice, find local top-N,
                // copy winners to mini storage, drop the rest.
                let partition_results: Vec<(CompactAccStorage, Vec<(i64, u128, u32)>)> =
                    std::thread::scope(|s| {
                    let workers = &partial_results;
                    let specs = &agg_specs;
                    let np = n_partitions;
                    let ascending = topn_ascending;

                    let handles: Vec<_> = (0..np).map(|p| {
                        s.spawn(move || {
                            let layout = CompactAccLayout::new(specs);
                            let n_slots = layout.slots.len();
                            let mut map: CompactGroupMap =
                                CompactGroupMap::with_hasher(Default::default());
                            let mut storage = CompactAccStorage::new(layout);

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
                                            e.insert(idx);
                                            idx
                                        }
                                    };
                                    for slot_idx in 0..n_slots {
                                        let (_, kind) = storage.layout.slots[slot_idx];
                                        match kind {
                                            CompactAccKind::Count => {
                                                let wc = worker.compact_storage.read_count(wgidx, slot_idx);
                                                *storage.count_mut(gidx, slot_idx) += wc;
                                            }
                                            CompactAccKind::SumInt => {
                                                let (ws, wc) = worker.compact_storage.read_sum_int(wgidx, slot_idx);
                                                let (gs, gc) = storage.sum_int_mut(gidx, slot_idx);
                                                *gs += ws;
                                                *gc += wc;
                                            }
                                            CompactAccKind::SumIntNarrow => {
                                                let (ws, wc) = worker.compact_storage.read_sum_int_narrow(wgidx, slot_idx);
                                                let (gs, gc) = storage.sum_int_narrow_mut(gidx, slot_idx);
                                                *gs += ws;
                                                *gc += wc;
                                            }
                                            CompactAccKind::SumFloat => {
                                                let (ws, wc) = worker.compact_storage.read_sum_float(wgidx, slot_idx);
                                                let (gs, gc) = storage.sum_float_mut(gidx, slot_idx);
                                                *gs += ws;
                                                *gc += wc;
                                            }
                                            CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                                let (w_off, w_len) = worker.compact_storage.read_min_max_str(wgidx, slot_idx);
                                                if w_off != u32::MAX {
                                                    let w_bytes = worker.compact_storage.str_arena.get(w_off, w_len).as_bytes();
                                                    let (g_off, g_len) = storage.read_min_max_str(gidx, slot_idx);
                                                    let should_update = if g_off == u32::MAX {
                                                        true
                                                    } else {
                                                        let g_bytes = storage.str_arena.get(g_off, g_len).as_bytes();
                                                        match kind {
                                                            CompactAccKind::MinStr => w_bytes < g_bytes,
                                                            _ => w_bytes > g_bytes,
                                                        }
                                                    };
                                                    if should_update {
                                                        let w_str = worker.compact_storage.str_arena.get(w_off, w_len);
                                                        let (new_off, new_len) = storage.str_arena.alloc(w_str);
                                                        storage.write_min_max_str(gidx, slot_idx, new_off, new_len);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            // Local top-N selection using a heap
                            let (_, sort_kind) = storage.layout.slots[sort_slot];
                            let read_val = |gidx: u32| -> i64 {
                                match sort_kind {
                                    CompactAccKind::Count => storage.read_count(gidx, sort_slot),
                                    CompactAccKind::SumIntNarrow => storage.read_sum_int_narrow(gidx, sort_slot).0,
                                    _ => storage.read_count(gidx, sort_slot),
                                }
                            };

                            let winners: Vec<(i64, u128, u32)> = if ascending {
                                // Keep smallest N: max-heap evicts largest
                                let mut heap: BinaryHeap<(i64, u128, u32)> =
                                    BinaryHeap::with_capacity(limit + 1);
                                for (&key, &gidx) in &map {
                                    let val = read_val(gidx);
                                    heap.push((val, key, gidx));
                                    if heap.len() > limit { heap.pop(); }
                                }
                                heap.into_vec()
                            } else {
                                // Keep largest N: min-heap (Reverse) evicts smallest
                                let mut heap: BinaryHeap<Reverse<(i64, u128, u32)>> =
                                    BinaryHeap::with_capacity(limit + 1);
                                for (&key, &gidx) in &map {
                                    let val = read_val(gidx);
                                    heap.push(Reverse((val, key, gidx)));
                                    if heap.len() > limit { heap.pop(); }
                                }
                                heap.into_iter().map(|Reverse(x)| x).collect()
                            };

                            drop(map); // free partition map (~250MB)

                            // Copy winning groups to tiny mini-storage
                            let layout2 = CompactAccLayout::new(specs);
                            let stride = storage.layout.group_stride;
                            let mut mini = CompactAccStorage::new(layout2);
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
                                    if kind == CompactAccKind::MinStr || kind == CompactAccKind::MaxStr {
                                        let (off, len) = storage.read_min_max_str(old_gidx, slot_idx);
                                        if off != u32::MAX {
                                            let val_str = storage.str_arena.get(off, len);
                                            let (no, nl) = mini.str_arena.alloc(val_str);
                                            mini.write_min_max_str(new_gidx, slot_idx, no, nl);
                                        }
                                    }
                                }
                                top_entries.push((sort_val, key, new_gidx));
                            }

                            drop(storage); // free full partition storage

                            (mini, top_entries)
                        })
                    }).collect();

                    handles.into_iter().map(|h| h.join().unwrap()).collect()
                });

                drop(partial_results); // free all worker data

                let merge_us = t_merge.elapsed().as_micros() as u64;

                // Merge all partition top entries, select global top-N
                let t_finalize = Instant::now();
                let mut all_candidates: Vec<(i64, u128, u32, usize)> = Vec::new();
                for (pi, (_, entries)) in partition_results.iter().enumerate() {
                    for &(sort_val, key, gidx) in entries {
                        all_candidates.push((sort_val, key, gidx, pi));
                    }
                }
                if topn_ascending {
                    all_candidates.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                } else {
                    all_candidates.sort_unstable_by(|a, b| b.0.cmp(&a.0));
                }
                all_candidates.truncate(limit);

                let num_group_keys = group_specs.len();
                let mut result_rows = Vec::with_capacity(limit);
                for &(_sort_val, key, mini_gidx, pi) in &all_candidates {
                    let storage = &partition_results[pi].0;
                    let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                    for (spec_idx, spec) in agg_specs.iter().enumerate() {
                        agg_results.push(compact_finalize(storage, mini_gidx, spec_idx, spec));
                    }
                    let keys = unpack_int_keys(key, num_group_keys);
                    let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                    for entry in &output_map {
                        match entry {
                            OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                            OutputEntry::Group(gi) => {
                                let v = keys[*gi];
                                if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. }) {
                                    row.push((i128_to_numeric_datum(v as i128), false));
                                } else {
                                    row.push((pg_sys::Datum::from(v as usize), false));
                                }
                            }
                            OutputEntry::Const(d, n) => row.push((*d, *n)),
                        }
                    }
                    result_rows.push(row);
                }
                let finalize_us = t_finalize.elapsed().as_micros() as u64;

                let state = AggScanState {
                    _agg_specs: agg_specs,
                    _group_specs: group_specs,
                    result_rows,
                    result_idx: 0,
                    _num_result_cols: num_result_cols,
                    metadata_us,
                    heap_scan_us,
                    decompress_us,
                    agg_us,
                    total_segments,
                    total_rows_processed,
                    batch_quals_count: batch_quals.len(),
                    where_quals_null: where_quals.is_null(),
                    regex_cache_size: 0,
                    regex_cache_calls: 0,
                    topn_limit: topn_limit as u64,
                    topn_sort_col: topn_sort_col as i64,
                    topn_ascending,
                    pre_topn_groups: pre_topn_groups as u64,
                    merge_us,
                    finalize_us,
                    topn_select_us: 0,
                    n_workers: n_workers as u64,
                    bare_limit: 0,
                };

                let state_box = Box::new(state);
                let state_ptr = Box::into_raw(state_box);
                (*node).custom_ps = state_ptr as *mut pg_sys::List;
                return;
            }

            // ----------------------------------------------------------
            // Full merge path: adopt largest worker's map as base,
            // merge remaining workers, then finalize.
            // ----------------------------------------------------------
            let t_merge = Instant::now();
            let mut partial_results = partial_results;

            // Find the largest partial map and take it as the base
            let largest_idx = partial_results.iter().enumerate()
                .max_by_key(|(_, r)| r.compact_map.len())
                .map(|(i, _)| i)
                .unwrap_or(0);
            let largest = partial_results.swap_remove(largest_idx);
            compact_group_map = largest.compact_map;
            *compact_storage.as_mut().unwrap() = largest.compact_storage;

            // Pre-reserve for remaining entries
            let remaining_entries: usize = partial_results.iter()
                .map(|r| r.compact_map.len())
                .sum();
            compact_group_map.reserve(remaining_entries);

            let storage = compact_storage.as_mut().unwrap();
            for result in &partial_results {
                merge_compact_results(
                    &mut compact_group_map,
                    storage,
                    &result.compact_map,
                    &result.compact_storage,
                    &agg_specs,
                );
            }
            let merge_us = t_merge.elapsed().as_micros() as u64;

            // Finalize
            let pre_topn_groups = compact_group_map.len();
            let topn_select_us: u64 = 0;
            let t_finalize = Instant::now();
            let result_rows = {
                // Full-scan path (no top-N, or HAVING present)
                let storage = compact_storage.as_ref().unwrap();
                let num_group_keys = group_specs.len();
                let mut rows = Vec::new();
                'par_compact_group_loop: for (&packed_key, &group_idx) in &compact_group_map {
                    let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                    for (spec_idx, spec) in agg_specs.iter().enumerate() {
                        agg_results.push(compact_finalize(storage, group_idx, spec_idx, spec));
                    }

                    for hf in &having_filters {
                        let (datum, is_null) = agg_results[hf.agg_idx];
                        if is_null { continue 'par_compact_group_loop; }
                        let val = datum.value() as i64;
                        let pass = match hf.op {
                            HavingOp::Gt => val > hf.const_val,
                            HavingOp::Lt => val < hf.const_val,
                            HavingOp::Ge => val >= hf.const_val,
                            HavingOp::Le => val <= hf.const_val,
                            HavingOp::Eq => val == hf.const_val,
                            HavingOp::Ne => val != hf.const_val,
                        };
                        if !pass { continue 'par_compact_group_loop; }
                    }

                    let keys = unpack_int_keys(packed_key, num_group_keys);
                    let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                    for entry in &output_map {
                        match entry {
                            OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                            OutputEntry::Group(gi) => {
                                let v = keys[*gi];
                                if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. }) {
                                    row.push((i128_to_numeric_datum(v as i128), false));
                                } else {
                                    row.push((pg_sys::Datum::from(v as usize), false));
                                }
                            }
                            OutputEntry::Const(d, n) => row.push((*d, *n)),
                        }
                    }
                    rows.push(row);
                }

                // Apply top-N on full result set (HAVING path or small groups)
                if topn_limit > 0 && has_group_by && rows.len() > topn_limit as usize {
                    let si = topn_sort_col;
                    if topn_ascending {
                        rows.sort_by_key(|row| {
                            let (datum, is_null) = row[si];
                            if is_null { i64::MAX } else { datum.value() as i64 }
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
                    rows.truncate(topn_limit as usize);
                }
                rows
            };
            let finalize_us = t_finalize.elapsed().as_micros() as u64;

            let state = AggScanState {
                _agg_specs: agg_specs,
                _group_specs: group_specs,
                result_rows,
                result_idx: 0,
                _num_result_cols: num_result_cols,
                metadata_us,
                heap_scan_us,
                decompress_us,
                agg_us,
                total_segments,
                total_rows_processed,
                batch_quals_count: batch_quals.len(),
                where_quals_null: where_quals.is_null(),
                regex_cache_size: 0,
                regex_cache_calls: 0,
                topn_limit: if topn_limit > 0 { topn_limit as u64 } else { 0 },
                topn_sort_col: topn_sort_col as i64,
                topn_ascending,
                pre_topn_groups: pre_topn_groups as u64,
                merge_us,
                finalize_us,
                topn_select_us,
                n_workers: n_workers as u64,
                bare_limit: 0,
            };

            let state_box = Box::new(state);
            let state_ptr = Box::into_raw(state_box);
            (*node).custom_ps = state_ptr as *mut pg_sys::List;
            return;
        }

        // ============================================================
        // PARALLEL MIXED PATH: multi-threaded with string GROUP BY
        // ============================================================
        let can_parallel_mixed_flag = !can_parallel
            && has_group_by
            && n_workers > 1
            && all_segments.len() > 1
            && !has_regexp_group
            && can_parallel_mixed(&group_specs, &needed_cols, &meta.col_types, &batch_quals, &agg_specs);

        if can_parallel_mixed_flag {
            let t2 = Instant::now();
            let topn_spec = if topn_limit > 0 && having_filters.is_empty() {
                let sort_slot = match output_map[topn_sort_col] {
                    OutputEntry::Agg(ai) => ai,
                    _ => unreachable!(),
                };
                let k = (topn_limit as usize).max(1000);
                Some((sort_slot, k, topn_ascending))
            } else {
                None
            };

            // Build text_group_col_flags
            let text_group_col_flags: Vec<bool> = (0..meta.col_names.len()).map(|i| {
                group_specs.iter().any(|gs| gs.col_idx as usize == i && is_text_group_col(gs))
            }).collect();

            // Build text qual infos for worker threads
            let mut text_qual_infos: Vec<TextQualInfo> = Vec::new();
            for bq in &batch_quals {
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
                        _ => {}
                    }
                }
            }

            let config = ParallelMixedConfig {
                agg_specs: &agg_specs,
                group_specs: &group_specs,
                col_names: &meta.col_names,
                col_types: &meta.col_types,
                segment_by: &meta.segment_by,
                needed_cols: &needed_cols,
                batch_quals: &batch_quals,
                seg_filters: &seg_filters,
                time_min,
                time_max,
                topn_spec,
                text_group_col_flags: &text_group_col_flags,
                text_qual_infos: &text_qual_infos,
            };

            let chunk_size = all_segments.len().div_ceil(n_workers);
            let partial_results: Vec<ParallelMixedResult> = std::thread::scope(|s| {
                let handles: Vec<_> = all_segments.chunks(chunk_size).map(|chunk| {
                    let cfg = &config;
                    s.spawn(move || process_segments_mixed(chunk, cfg))
                }).collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });

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
            let agg_us = scan_wall_us.saturating_sub(decompress_us);

            // ----------------------------------------------------------
            // Speculative top-N: merge-skip using pre-computed top-K
            // ----------------------------------------------------------
            if topn_limit > 0 && having_filters.is_empty() {
                let sort_slot = match output_map[topn_sort_col] {
                    OutputEntry::Agg(ai) => ai,
                    _ => unreachable!(),
                };
                let (_, sort_kind) = compact_storage.as_ref().unwrap().layout.slots[sort_slot];
                let limit = topn_limit as usize;
                let k = (topn_limit as usize).max(1000);

                let read_sort = |storage: &CompactAccStorage, group_idx: u32| -> i64 {
                    match sort_kind {
                        CompactAccKind::Count => storage.read_count(group_idx, sort_slot),
                        CompactAccKind::SumIntNarrow => storage.read_sum_int_narrow(group_idx, sort_slot).0,
                        _ => storage.read_count(group_idx, sort_slot),
                    }
                };

                let t_spec = Instant::now();

                // Phase 1: Collect pre-computed top-K candidates from workers
                let mut candidate_set: hashbrown::HashSet<u128, BuildHasherDefault<ahash::AHasher>> =
                    hashbrown::HashSet::with_capacity_and_hasher(
                        k * partial_results.len(), BuildHasherDefault::default(),
                    );
                let mut floor_sum: i64 = 0;
                for result in &partial_results {
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
                    for result in &partial_results {
                        if let Some(&gidx) = result.compact_map.get(&key) {
                            total = total.saturating_add(read_sort(&result.compact_storage, gidx));
                        }
                    }
                    merged.push((total, key));
                }

                // Phase 3: Sort and take top-N
                if !topn_ascending {
                    merged.sort_unstable_by(|a, b| b.0.cmp(&a.0));
                } else {
                    merged.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                }
                merged.truncate(limit);

                // Phase 4: Correctness check
                let speculative_ok = if merged.len() >= limit {
                    let nth_value = merged[limit - 1].0;
                    if !topn_ascending {
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
                    let storage = compact_storage.as_mut().unwrap();
                    let mut result_rows = Vec::with_capacity(merged.len());

                    // Find which worker has each winning key for MixedKeyStorage lookup
                    for &(_, hash_key) in &merged {
                        let global_idx = storage.alloc_group();

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
                                            let wc = result.compact_storage.read_count(worker_idx, slot_idx);
                                            *storage.count_mut(global_idx, slot_idx) += wc;
                                        }
                                        CompactAccKind::SumInt => {
                                            let (ws, wc) = result.compact_storage.read_sum_int(worker_idx, slot_idx);
                                            let (gs, gc) = storage.sum_int_mut(global_idx, slot_idx);
                                            *gs += ws;
                                            *gc += wc;
                                        }
                                        CompactAccKind::SumIntNarrow => {
                                            let (ws, wc) = result.compact_storage.read_sum_int_narrow(worker_idx, slot_idx);
                                            let (gs, gc) = storage.sum_int_narrow_mut(global_idx, slot_idx);
                                            *gs += ws;
                                            *gc += wc;
                                        }
                                        CompactAccKind::SumFloat => {
                                            let (ws, wc) = result.compact_storage.read_sum_float(worker_idx, slot_idx);
                                            let (gs, gc) = storage.sum_float_mut(global_idx, slot_idx);
                                            *gs += ws;
                                            *gc += wc;
                                        }
                                        CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                            let (w_off, w_len) = result.compact_storage.read_min_max_str(worker_idx, slot_idx);
                                            if w_off != u32::MAX {
                                                let w_str = result.compact_storage.str_arena.get(w_off, w_len);
                                                let (g_off, g_len) = storage.read_min_max_str(global_idx, slot_idx);
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
                                                    storage.write_min_max_str(global_idx, slot_idx, new_off, new_len);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Finalize this group
                        let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                        for (spec_idx, spec) in agg_specs.iter().enumerate() {
                            agg_results.push(compact_finalize(storage, global_idx, spec_idx, spec));
                        }

                        // Get actual key values from the first worker that has this key
                        let source_wi = key_source_worker.unwrap();
                        let source_gidx = *partial_results[source_wi].compact_map.get(&hash_key).unwrap();
                        let mixed_ks = &partial_results[source_wi].mixed_keys;

                        let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                        for entry in &output_map {
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
                                OutputEntry::Const(d, n) => row.push((*d, *n)),
                            }
                        }
                        result_rows.push(row);
                    }
                    let finalize_us = t_fin.elapsed().as_micros() as u64;

                    let pre_topn_groups: usize = partial_results.iter()
                        .map(|r| r.compact_map.len()).sum();

                    let state = AggScanState {
                        _agg_specs: agg_specs,
                        _group_specs: group_specs,
                        result_rows,
                        result_idx: 0,
                        _num_result_cols: num_result_cols,
                        metadata_us,
                        heap_scan_us,
                        decompress_us,
                        agg_us,
                        total_segments,
                        total_rows_processed,
                        batch_quals_count: batch_quals.len(),
                        where_quals_null: where_quals.is_null(),
                        regex_cache_size: 0,
                        regex_cache_calls: 0,
                        topn_limit: topn_limit as u64,
                        topn_sort_col: topn_sort_col as i64,
                        topn_ascending,
                        pre_topn_groups: pre_topn_groups as u64,
                        merge_us: 0,
                        finalize_us,
                        topn_select_us,
                        n_workers: n_workers as u64,
                        bare_limit: 0,
                    };

                    let state_box = Box::new(state);
                    let state_ptr = Box::into_raw(state_box);
                    (*node).custom_ps = state_ptr as *mut pg_sys::List;
                    return;
                }
                // Speculation failed — fall through to full merge
                pgrx::log!(
                    "pg_deltax mixed speculative top-N failed: candidates={} k={} floor_sum={}",
                    merged.len(), k, floor_sum,
                );
            }

            // ----------------------------------------------------------
            // Bare LIMIT short-circuit for mixed path: pick N groups
            // from largest worker, merge only those, finalize only those
            // ----------------------------------------------------------
            if bare_limit > 0 && having_filters.is_empty() {
                let n = bare_limit as usize;
                let t_merge = Instant::now();

                // Pick the largest worker
                let largest_idx = partial_results.iter().enumerate()
                    .max_by_key(|(_, r)| r.compact_map.len())
                    .map(|(i, _)| i)
                    .unwrap_or(0);

                // Collect first N group keys from largest worker
                let target_keys: Vec<u128> = partial_results[largest_idx].compact_map
                    .keys().take(n).copied().collect();

                // Targeted merge: for each target key, merge accumulators from all workers
                let layout = CompactAccLayout {
                    slots: partial_results[0].compact_storage.layout.slots.clone(),
                    group_stride: partial_results[0].compact_storage.layout.group_stride,
                };
                let mut final_storage = CompactAccStorage::new(layout);
                let mut final_mixed_keys = MixedKeyStorage::new(group_specs.len());

                for &key in &target_keys {
                    let group_idx = final_storage.alloc_group();

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
                                final_mixed_keys.keys.push(MixedKeyVal::Str(new_off, new_len));
                            }
                            other => final_mixed_keys.keys.push(other),
                        }
                    }

                    // Merge accumulators from all workers
                    for result in &partial_results {
                        if let Some(&worker_gidx) = result.compact_map.get(&key) {
                            for (slot_idx, _) in agg_specs.iter().enumerate() {
                                let (_, kind) = final_storage.layout.slots[slot_idx];
                                match kind {
                                    CompactAccKind::Count => {
                                        let wc = result.compact_storage.read_count(worker_gidx, slot_idx);
                                        *final_storage.count_mut(group_idx, slot_idx) += wc;
                                    }
                                    CompactAccKind::SumInt => {
                                        let (ws, wc) = result.compact_storage.read_sum_int(worker_gidx, slot_idx);
                                        let (gs, gc) = final_storage.sum_int_mut(group_idx, slot_idx);
                                        *gs += ws;
                                        *gc += wc;
                                    }
                                    CompactAccKind::SumIntNarrow => {
                                        let (ws, wc) = result.compact_storage.read_sum_int_narrow(worker_gidx, slot_idx);
                                        let (gs, gc) = final_storage.sum_int_narrow_mut(group_idx, slot_idx);
                                        *gs += ws;
                                        *gc += wc;
                                    }
                                    CompactAccKind::SumFloat => {
                                        let (ws, wc) = result.compact_storage.read_sum_float(worker_gidx, slot_idx);
                                        let (gs, gc) = final_storage.sum_float_mut(group_idx, slot_idx);
                                        *gs += ws;
                                        *gc += wc;
                                    }
                                    CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                        let (w_off, w_len) = result.compact_storage.read_min_max_str(worker_gidx, slot_idx);
                                        if w_off != u32::MAX {
                                            let w_str = result.compact_storage.str_arena.get(w_off, w_len);
                                            let (g_off, g_len) = final_storage.read_min_max_str(group_idx, slot_idx);
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
                                                let w_str = result.compact_storage.str_arena.get(w_off, w_len);
                                                let (new_off, new_len) = final_storage.str_arena.alloc(w_str);
                                                final_storage.write_min_max_str(group_idx, slot_idx, new_off, new_len);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                let merge_us = t_merge.elapsed().as_micros() as u64;

                // Finalize just N groups
                let pre_topn_groups: usize = partial_results.iter()
                    .map(|r| r.compact_map.len()).sum();
                let t_finalize = Instant::now();
                let mut result_rows = Vec::with_capacity(n);
                for (i, &_key) in target_keys.iter().enumerate() {
                    let group_idx = i as u32;
                    let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                    for (spec_idx, spec) in agg_specs.iter().enumerate() {
                        agg_results.push(compact_finalize(&final_storage, group_idx, spec_idx, spec));
                    }
                    let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                    for entry in &output_map {
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
                            OutputEntry::Const(d, n) => row.push((*d, *n)),
                        }
                    }
                    result_rows.push(row);
                }
                let finalize_us = t_finalize.elapsed().as_micros() as u64;

                let state = AggScanState {
                    _agg_specs: agg_specs,
                    _group_specs: group_specs,
                    result_rows,
                    result_idx: 0,
                    _num_result_cols: num_result_cols,
                    metadata_us,
                    heap_scan_us,
                    decompress_us,
                    agg_us,
                    total_segments,
                    total_rows_processed,
                    batch_quals_count: batch_quals.len(),
                    where_quals_null: where_quals.is_null(),
                    regex_cache_size: 0,
                    regex_cache_calls: 0,
                    topn_limit: 0,
                    topn_sort_col: -1,
                    topn_ascending,
                    pre_topn_groups: pre_topn_groups as u64,
                    merge_us,
                    finalize_us,
                    topn_select_us: 0,
                    n_workers: n_workers as u64,
                    bare_limit,
                };

                let state_box = Box::new(state);
                let state_ptr = Box::into_raw(state_box);
                (*node).custom_ps = state_ptr as *mut pg_sys::List;
                return;
            }

            // ----------------------------------------------------------
            // Full merge path for mixed: adopt largest, merge rest
            // ----------------------------------------------------------
            let t_merge = Instant::now();
            let mut partial_results = partial_results;

            let largest_idx = partial_results.iter().enumerate()
                .max_by_key(|(_, r)| r.compact_map.len())
                .map(|(i, _)| i)
                .unwrap_or(0);
            let largest = partial_results.swap_remove(largest_idx);
            compact_group_map = largest.compact_map;
            *compact_storage.as_mut().unwrap() = largest.compact_storage;
            let mut merged_mixed_keys = largest.mixed_keys;

            let remaining_entries: usize = partial_results.iter()
                .map(|r| r.compact_map.len()).sum();
            compact_group_map.reserve(remaining_entries);

            let storage = compact_storage.as_mut().unwrap();
            for result in &partial_results {
                for (&hash_key, &worker_group_idx) in &result.compact_map {
                    let global_group_idx = match compact_group_map.entry(hash_key) {
                        hashbrown::hash_map::Entry::Occupied(e) => *e.get(),
                        hashbrown::hash_map::Entry::Vacant(e) => {
                            let idx = storage.alloc_group();
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
                                        merged_mixed_keys.keys.push(MixedKeyVal::Str(new_off, new_len));
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
                                let wc = result.compact_storage.read_count(worker_group_idx, slot_idx);
                                *storage.count_mut(global_group_idx, slot_idx) += wc;
                            }
                            CompactAccKind::SumInt => {
                                let (ws, wc) = result.compact_storage.read_sum_int(worker_group_idx, slot_idx);
                                let (gs, gc) = storage.sum_int_mut(global_group_idx, slot_idx);
                                *gs += ws;
                                *gc += wc;
                            }
                            CompactAccKind::SumIntNarrow => {
                                let (ws, wc) = result.compact_storage.read_sum_int_narrow(worker_group_idx, slot_idx);
                                let (gs, gc) = storage.sum_int_narrow_mut(global_group_idx, slot_idx);
                                *gs += ws;
                                *gc += wc;
                            }
                            CompactAccKind::SumFloat => {
                                let (ws, wc) = result.compact_storage.read_sum_float(worker_group_idx, slot_idx);
                                let (gs, gc) = storage.sum_float_mut(global_group_idx, slot_idx);
                                *gs += ws;
                                *gc += wc;
                            }
                            CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                let (w_off, w_len) = result.compact_storage.read_min_max_str(worker_group_idx, slot_idx);
                                if w_off != u32::MAX {
                                    let w_str = result.compact_storage.str_arena.get(w_off, w_len);
                                    let (g_off, g_len) = storage.read_min_max_str(global_group_idx, slot_idx);
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
                                        storage.write_min_max_str(global_group_idx, slot_idx, new_off, new_len);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            let merge_us = t_merge.elapsed().as_micros() as u64;

            // Finalize
            let pre_topn_groups = compact_group_map.len();
            let mut topn_select_us: u64 = 0;
            let t_finalize = Instant::now();

            // Helper closure to convert a group's keys to datums
            let finalize_mixed_group = |_hash_key: u128, group_idx: u32, storage: &CompactAccStorage, mixed_ks: &MixedKeyStorage| -> Vec<(pg_sys::Datum, bool)> {
                let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                for (spec_idx, spec) in agg_specs.iter().enumerate() {
                    agg_results.push(compact_finalize(storage, group_idx, spec_idx, spec));
                }
                let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                for entry in &output_map {
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
                        OutputEntry::Const(d, n) => row.push((*d, *n)),
                    }
                }
                row
            };

            let result_rows = if topn_limit > 0 && having_filters.is_empty()
                && compact_group_map.len() > topn_limit as usize
            {
                let sort_slot = match output_map[topn_sort_col] {
                    OutputEntry::Agg(ai) => ai,
                    _ => unreachable!(),
                };
                let storage = compact_storage.as_ref().unwrap();
                let t_topn = Instant::now();
                let top_entries = compact_topn_select(
                    &compact_group_map, storage, sort_slot,
                    topn_limit as usize, topn_ascending,
                );
                topn_select_us = t_topn.elapsed().as_micros() as u64;
                let mut rows = Vec::with_capacity(top_entries.len());
                for &(hash_key, group_idx) in &top_entries {
                    rows.push(finalize_mixed_group(hash_key, group_idx, storage, &merged_mixed_keys));
                }
                rows
            } else {
                let storage = compact_storage.as_ref().unwrap();
                let mut rows = Vec::new();
                'par_mixed_group_loop: for (_, &group_idx) in &compact_group_map {
                    let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                    for (spec_idx, spec) in agg_specs.iter().enumerate() {
                        agg_results.push(compact_finalize(storage, group_idx, spec_idx, spec));
                    }

                    for hf in &having_filters {
                        let (datum, is_null) = agg_results[hf.agg_idx];
                        if is_null { continue 'par_mixed_group_loop; }
                        let val = datum.value() as i64;
                        let pass = match hf.op {
                            HavingOp::Gt => val > hf.const_val,
                            HavingOp::Lt => val < hf.const_val,
                            HavingOp::Ge => val >= hf.const_val,
                            HavingOp::Le => val <= hf.const_val,
                            HavingOp::Eq => val == hf.const_val,
                            HavingOp::Ne => val != hf.const_val,
                        };
                        if !pass { continue 'par_mixed_group_loop; }
                    }

                    let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                    for entry in &output_map {
                        match entry {
                            OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                            OutputEntry::Group(gi) => {
                                let kv = merged_mixed_keys.get(group_idx, *gi);
                                match kv {
                                    MixedKeyVal::Int(v) => {
                                        if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. }) {
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
                            OutputEntry::Const(d, n) => row.push((*d, *n)),
                        }
                    }
                    rows.push(row);
                }

                // Apply top-N on full result set (HAVING path or small groups)
                if topn_limit > 0 && has_group_by && rows.len() > topn_limit as usize {
                    let si = topn_sort_col;
                    if topn_ascending {
                        rows.sort_by_key(|row| {
                            let (datum, is_null) = row[si];
                            if is_null { i64::MAX } else { datum.value() as i64 }
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
                    rows.truncate(topn_limit as usize);
                }
                rows
            };
            let finalize_us = t_finalize.elapsed().as_micros() as u64;

            let state = AggScanState {
                _agg_specs: agg_specs,
                _group_specs: group_specs,
                result_rows,
                result_idx: 0,
                _num_result_cols: num_result_cols,
                metadata_us,
                heap_scan_us,
                decompress_us,
                agg_us,
                total_segments,
                total_rows_processed,
                batch_quals_count: batch_quals.len(),
                where_quals_null: where_quals.is_null(),
                regex_cache_size: 0,
                regex_cache_calls: 0,
                topn_limit: if topn_limit > 0 { topn_limit as u64 } else { 0 },
                topn_sort_col: topn_sort_col as i64,
                topn_ascending,
                pre_topn_groups: pre_topn_groups as u64,
                merge_us,
                finalize_us,
                topn_select_us,
                n_workers: n_workers as u64,
                bare_limit: 0,
            };

            let state_box = Box::new(state);
            let state_ptr = Box::into_raw(state_box);
            (*node).custom_ps = state_ptr as *mut pg_sys::List;
            return;
        }

        // ============================================================
        // SINGLE-THREADED PATH (original)
        // ============================================================
        let t2 = Instant::now();
        let mut total_segments: u64 = 0;
        let mut total_rows_processed: u64 = 0;
        let mut decompress_us: u64 = 0;

        for seg in &all_segments {
            if seg.row_count == 0 {
                continue;
            }

            // Segment-by pruning
            if !seg_filters.is_empty() {
                let mut skip = false;
                for &(seg_val_idx, ref filter_val) in &seg_filters {
                    match &seg.segment_values[seg_val_idx] {
                        Some(val) if val == filter_val => {}
                        _ => { skip = true; break; }
                    }
                }
                if skip { continue; }
            }

            // Time-range pruning
            if let (Some(seg_min), Some(seg_max)) = (seg.min_time, seg.max_time) {
                if time_min.is_some_and(|query_min| seg_max < query_min) { continue; }
                if time_max.is_some_and(|query_max| seg_min > query_max) { continue; }
            }

            // Dictionary-based LIKE pruning: skip segment if no dict entry matches
            if segment_skippable_by_dict_like(
                &batch_quals, &meta.col_names, &meta.segment_by, &seg.compressed_blobs,
            ) {
                continue;
            }

            total_segments += 1;

            // Decompress needed columns
            let t_dec = Instant::now();
            pg_sys::MemoryContextReset(segment_mcxt);
            let old_ctx = pg_sys::MemoryContextSwitchTo(segment_mcxt);

            let mut decompressed: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
            // Raw strings for columns that need regexp_replace (parallel to decompressed)
            let mut raw_strings: Vec<Option<Vec<Option<String>>>> = Vec::new();
            let mut blob_idx = 0;
            let mut seg_val_idx = 0;
            let mut pre_selection: Vec<bool> = Vec::new();

            for (col_idx, col_name) in meta.col_names.iter().enumerate() {
                let type_oid = meta.col_types[col_idx];

                if !needed_cols[col_idx] {
                    if meta.segment_by.contains(col_name) {
                        seg_val_idx += 1;
                    } else {
                        blob_idx += 1;
                    }
                    decompressed.push(Vec::new());
                    raw_strings.push(None);
                    continue;
                }

                if meta.segment_by.contains(col_name) {
                    let val = &seg.segment_values[seg_val_idx];
                    let (datum, is_null) = match val {
                        Some(s) => (string_to_datum(s, type_oid), false),
                        None => (pg_sys::Datum::from(0), true),
                    };
                    let repeated: Vec<(pg_sys::Datum, bool)> =
                        (0..seg.row_count).map(|_| (datum, is_null)).collect();
                    decompressed.push(repeated);
                    raw_strings.push(None);
                    seg_val_idx += 1;
                } else {
                    let blob = &seg.compressed_blobs[blob_idx];
                    let typmod = meta.col_typmods[col_idx];

                    // Fast path: COUNT(DISTINCT) on text without GROUP BY or
                    // row-level WHERE — hash directly from compressed data,
                    // skipping all datum conversion.
                    if count_distinct_only_str[col_idx] && !has_group_by && batch_quals.is_empty() {
                        let cc_ref = compression::CompressedColumnRef::from_bytes(blob);
                        let accumulators = global_accumulators.as_mut().unwrap();
                        // Find the CountDistinctStr accumulator for this column
                        for (spec_idx, spec) in agg_specs.iter().enumerate() {
                            if spec.col_idx as usize == col_idx {
                                if let AggAccumulator::CountDistinctStr { seen } = &mut accumulators[spec_idx] {
                                    let non_null_count = count_non_null(cc_ref.null_bitmap, cc_ref.row_count as usize);
                                    match cc_ref.type_tag {
                                        compression::CompressionType::Dictionary
                                        | compression::CompressionType::DictionaryLz4 => {
                                            // Dict shortcut: hash only the dict entries — O(dict_size)
                                            let norm_buf;
                                            let dict_data = if cc_ref.type_tag == compression::CompressionType::DictionaryLz4 {
                                                norm_buf = compression::dictionary::normalize_lz4(cc_ref.data);
                                                &norm_buf[..]
                                            } else {
                                                cc_ref.data
                                            };
                                            let hdr = compression::dictionary::parse_header(dict_data);
                                            for entry in &hdr.dict {
                                                seen.insert(hash128_str(entry.as_bytes()));
                                            }
                                        }
                                        compression::CompressionType::Lz4 => {
                                            let (buf, ranges) = compression::lz4::decode_to_ranges(cc_ref.data, non_null_count);
                                            let empty_hash = hash128_str(b"");
                                            let mut has_empty = false;
                                            for &(off, len) in &ranges {
                                                if len == 0 {
                                                    has_empty = true;
                                                } else {
                                                    seen.insert(hash128_str(&buf[off..off + len]));
                                                }
                                            }
                                            if has_empty { seen.insert(empty_hash); }
                                        }
                                        compression::CompressionType::Lz4Blocked => {
                                            let (buf, ranges) = compression::lz4::decode_to_ranges_blocked(cc_ref.data, non_null_count, None);
                                            let empty_hash = hash128_str(b"");
                                            let mut has_empty = false;
                                            for &(off, len) in &ranges {
                                                if len == 0 {
                                                    has_empty = true;
                                                } else {
                                                    seen.insert(hash128_str(&buf[off..off + len]));
                                                }
                                            }
                                            if has_empty { seen.insert(empty_hash); }
                                        }
                                        compression::CompressionType::Constant => {
                                            // Single constant string — hash the raw bytes
                                            if non_null_count > 0 {
                                                seen.insert(hash128_str(cc_ref.data));
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                break;
                            }
                        }
                        // Push empty so the row loop skips this column
                        decompressed.push(Vec::new());
                        raw_strings.push(None);
                        blob_idx += 1;
                        continue;
                    }

                    // Fast path: COUNT(DISTINCT) on integer without GROUP BY or
                    // row-level WHERE — decode directly and insert into HashSet,
                    // skipping all datum conversion.
                    if count_distinct_only_int[col_idx] && !has_group_by && batch_quals.is_empty() {
                        let cc_ref = compression::CompressedColumnRef::from_bytes(blob);
                        let accumulators = global_accumulators.as_mut().unwrap();
                        for (spec_idx, spec) in agg_specs.iter().enumerate() {
                            if spec.col_idx as usize == col_idx {
                                if let AggAccumulator::CountDistinctInt { seen } = &mut accumulators[spec_idx] {
                                    let non_null_count = count_non_null(cc_ref.null_bitmap, cc_ref.row_count as usize);
                                    if non_null_count > 0 {
                                        let is_i64 = type_oid == pg_sys::INT8OID;
                                        match cc_ref.type_tag {
                                            compression::CompressionType::Constant => {
                                                if is_i64 {
                                                    let v = i64::from_le_bytes(cc_ref.data[..8].try_into().unwrap());
                                                    seen.insert(v);
                                                } else {
                                                    let v = i32::from_le_bytes(cc_ref.data[..4].try_into().unwrap());
                                                    seen.insert(v as i64);
                                                }
                                            }
                                            compression::CompressionType::ForBitpacked => {
                                                if is_i64 {
                                                    let vals = compression::bitpacked::decode_for_i64(cc_ref.data, non_null_count);
                                                    for v in vals { seen.insert(v); }
                                                } else {
                                                    let vals = compression::bitpacked::decode_for_i32(cc_ref.data, non_null_count);
                                                    for v in vals { seen.insert(v as i64); }
                                                }
                                            }
                                            compression::CompressionType::DeltaVarint => {
                                                if is_i64 {
                                                    let vals = compression::integer::decode_i64(cc_ref.data, non_null_count);
                                                    for v in vals { seen.insert(v); }
                                                } else {
                                                    let vals = compression::integer::decode_i32(cc_ref.data, non_null_count);
                                                    for v in vals { seen.insert(v as i64); }
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                break;
                            }
                        }
                        // Push empty so the row loop skips this column
                        decompressed.push(Vec::new());
                        raw_strings.push(None);
                        blob_idx += 1;
                        continue;
                    }

                    if raw_string_cols[col_idx] {
                        // Dictionary-optimized path: pre-warm regex cache from dict entries only
                        let cc_ref = compression::CompressedColumnRef::from_bytes(blob);
                        if cc_ref.type_tag == compression::CompressionType::Dictionary
                            || cc_ref.type_tag == compression::CompressionType::DictionaryLz4
                        {
                            let total_count = cc_ref.row_count as usize;
                            let non_null_count = count_non_null(cc_ref.null_bitmap, total_count);
                            let norm_buf;
                            let dict_data = if cc_ref.type_tag == compression::CompressionType::DictionaryLz4 {
                                norm_buf = compression::dictionary::normalize_lz4(cc_ref.data);
                                &norm_buf[..]
                            } else {
                                cc_ref.data
                            };
                            let (dict_entries, indices) =
                                compression::dictionary::decode_dict_and_indices(dict_data, non_null_count);

                            // Pre-warm regex cache from dict entries only — O(dict_size) calls
                            for &entry in &dict_entries {
                                let key = entry.to_string();
                                if !regex_cache.contains_key(&key) {
                                    for rgi in &regexp_group_infos {
                                        if group_specs[rgi.group_idx].col_idx as usize == col_idx {
                                            regex_cache_calls += 1;
                                            let input_datum = {
                                                let text = pg_sys::cstring_to_text_with_len(
                                                    entry.as_ptr() as *const _, entry.len() as i32,
                                                );
                                                pg_sys::Datum::from(text as usize)
                                            };
                                            let result_datum = pg_sys::OidFunctionCall3Coll(
                                                rgi.func_oid,
                                                rgi.collation,
                                                input_datum,
                                                rgi.pattern_datum,
                                                rgi.replacement_datum,
                                            );
                                            let cstr = pg_sys::text_to_cstring(result_datum.cast_mut_ptr());
                                            let s = std::ffi::CStr::from_ptr(cstr).to_string_lossy().into_owned();
                                            pg_sys::pfree(cstr as *mut _);
                                            regex_cache.insert(key.clone(), s);
                                            break;
                                        }
                                    }
                                }
                            }

                            // Build per-row strings from cached regex results via dict index
                            let has_ne_empty = batch_quals.iter().any(|bq| {
                                bq.col_idx == col_idx
                                    && bq.text_const.as_deref() == Some("")
                                    && bq.op == BatchCompareOp::Ne
                            });
                            let ne_sel = if has_ne_empty {
                                compression::dictionary::check_ne_empty(dict_data, non_null_count)
                            } else {
                                Vec::new()
                            };

                            let nn_strings: Vec<String> = indices
                                .iter()
                                .map(|&idx| dict_entries[idx as usize].to_string())
                                .collect();

                            // Reinsert nulls
                            if cc_ref.null_bitmap.is_empty() {
                                let strings: Vec<Option<String>> = nn_strings.into_iter().map(Some).collect();
                                let datums: Vec<(pg_sys::Datum, bool)> = strings.iter().map(|s| {
                                    match s {
                                        Some(_) => (pg_sys::Datum::from(0usize), false),
                                        None => (pg_sys::Datum::from(0usize), true),
                                    }
                                }).collect();
                                decompressed.push(datums);
                                raw_strings.push(Some(strings));
                                if !ne_sel.is_empty() {
                                    if pre_selection.is_empty() {
                                        pre_selection = ne_sel;
                                    } else {
                                        for (ps, s) in pre_selection.iter_mut().zip(ne_sel.iter()) {
                                            *ps = *ps && *s;
                                        }
                                    }
                                }
                            } else {
                                let mut strings = Vec::with_capacity(total_count);
                                let mut sel = if has_ne_empty { Vec::with_capacity(total_count) } else { Vec::new() };
                                let mut val_idx = 0;
                                for i in 0..total_count {
                                    let is_null = (cc_ref.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
                                    if is_null {
                                        strings.push(None);
                                        if has_ne_empty { sel.push(false); }
                                    } else {
                                        strings.push(Some(nn_strings[val_idx].clone()));
                                        if has_ne_empty && !ne_sel.is_empty() { sel.push(ne_sel[val_idx]); }
                                        else if has_ne_empty { sel.push(true); }
                                        val_idx += 1;
                                    }
                                }
                                let datums: Vec<(pg_sys::Datum, bool)> = strings.iter().map(|s| {
                                    match s {
                                        Some(_) => (pg_sys::Datum::from(0usize), false),
                                        None => (pg_sys::Datum::from(0usize), true),
                                    }
                                }).collect();
                                decompressed.push(datums);
                                raw_strings.push(Some(strings));
                                if !sel.is_empty() {
                                    if pre_selection.is_empty() {
                                        pre_selection = sel;
                                    } else {
                                        for (ps, s) in pre_selection.iter_mut().zip(sel.iter()) {
                                            *ps = *ps && *s;
                                        }
                                    }
                                }
                            }
                        } else {
                            // Non-dictionary: fall back to existing path
                            let (strings, sel) = decompress_text_blob_to_raw_strings(blob, &batch_quals, col_idx);
                            let datums: Vec<(pg_sys::Datum, bool)> = strings.iter().map(|s| {
                                match s {
                                    Some(_) => (pg_sys::Datum::from(0usize), false),
                                    None => (pg_sys::Datum::from(0usize), true),
                                }
                            }).collect();
                            decompressed.push(datums);
                            raw_strings.push(Some(strings));
                            if !sel.is_empty() {
                                if pre_selection.is_empty() {
                                    pre_selection = sel;
                                } else {
                                    for (ps, s) in pre_selection.iter_mut().zip(sel.iter()) {
                                        *ps = *ps && *s;
                                    }
                                }
                            }
                        }
                    } else if length_cols[col_idx] {
                        // Length-only column: decompress as int4 lengths.
                        let has_ne_empty = batch_quals.iter().any(|bq| {
                            bq.col_idx == col_idx
                                && bq.text_const.as_deref() == Some("")
                                && bq.op == BatchCompareOp::Ne
                        });
                        let (datums, len_sel) = decompress_text_blob_to_lengths(blob, has_ne_empty);
                        decompressed.push(datums);
                        raw_strings.push(None);
                        if !len_sel.is_empty() {
                            if pre_selection.is_empty() {
                                pre_selection = len_sel;
                            } else {
                                for (ps, ls) in pre_selection.iter_mut().zip(len_sel.iter()) {
                                    *ps = *ps && *ls;
                                }
                            }
                        }
                    } else {
                        // Normal text/non-text column
                        let like_qual = batch_quals.iter().find(|bq| {
                            bq.col_idx == col_idx
                                && matches!(bq.op, BatchCompareOp::Like | BatchCompareOp::NotLike)
                        });

                        let text_eq_qual = batch_quals.iter().find(|bq| {
                            bq.col_idx == col_idx
                                && bq.text_const.is_some()
                                && matches!(bq.op, BatchCompareOp::Eq | BatchCompareOp::Ne)
                        });

                        if let Some(bq) = like_qual {
                            let strat = bq.like_strategy.as_ref().unwrap();
                            let neg = bq.op == BatchCompareOp::NotLike;
                            let (datums, like_sel) =
                                decompress_text_blob_with_like_filter(blob, type_oid, typmod, strat, neg);
                            decompressed.push(datums);
                            if pre_selection.is_empty() {
                                pre_selection = like_sel;
                            } else {
                                for (ps, ls) in pre_selection.iter_mut().zip(like_sel.iter()) {
                                    *ps = *ps && *ls;
                                }
                            }
                        } else if let Some(bq) = text_eq_qual {
                            let const_str = bq.text_const.as_ref().unwrap();
                            let is_ne = bq.op == BatchCompareOp::Ne;
                            let (datums, eq_sel) = decompress_text_blob_with_eq_filter(
                                blob, type_oid, typmod, const_str, is_ne,
                            );
                            decompressed.push(datums);
                            if pre_selection.is_empty() {
                                pre_selection = eq_sel;
                            } else {
                                for (ps, es) in pre_selection.iter_mut().zip(eq_sel.iter()) {
                                    *ps = *ps && *es;
                                }
                            }
                        } else {
                            let type_name = pg_type_name(type_oid);
                            let datums = decompress_blob_to_datums(blob, &type_name, type_oid, typmod);
                            decompressed.push(datums);
                        }
                        raw_strings.push(None);
                    }
                    blob_idx += 1;
                }
            }

            pg_sys::MemoryContextSwitchTo(old_ctx);
            decompress_us += t_dec.elapsed().as_micros() as u64;

            let row_count = seg.row_count as usize;

            // Extract text GROUP BY info: intern strings and build per-row u32 ID vectors.
            // Handles both dictionary-encoded and LZ4-encoded text columns.
            // Build per-segment text column data for GROUP BY.
            // Keeps decoded string data alive during the row loop for O(1) &str access.
            seg_text_columns.clear();
            seg_text_columns.resize_with(meta.col_names.len(), || None);
            {
                let mut blob_idx2 = 0;
                let mut seg_val_idx2 = 0;
                for (col_idx, col_name) in meta.col_names.iter().enumerate() {
                    if meta.segment_by.contains(col_name) {
                        if needed_cols[col_idx] && text_group_cols[col_idx] {
                            let val = &seg.segment_values[seg_val_idx2];
                            seg_text_columns[col_idx] = Some(SegTextColumn::SegBy(val.clone()));
                        }
                        seg_val_idx2 += 1;
                        continue;
                    }
                    if needed_cols[col_idx] && text_group_cols[col_idx] {
                        let blob = &seg.compressed_blobs[blob_idx2];
                        if !blob.is_empty() {
                            let cc_ref = compression::CompressedColumnRef::from_bytes(blob);
                            let total = cc_ref.row_count as usize;
                            let nn_count = count_non_null(cc_ref.null_bitmap, total);

                            let seg_col = match cc_ref.type_tag {
                                compression::CompressionType::Dictionary
                                | compression::CompressionType::DictionaryLz4 => {
                                    let norm_buf;
                                    let dict_data = if cc_ref.type_tag == compression::CompressionType::DictionaryLz4 {
                                        norm_buf = compression::dictionary::normalize_lz4(cc_ref.data);
                                        &norm_buf[..]
                                    } else {
                                        cc_ref.data
                                    };
                                    let (dict_entries, nn_indices) =
                                        compression::dictionary::decode_dict_and_indices(dict_data, nn_count);
                                    let entries: Vec<String> = dict_entries.iter().map(|&s| s.to_string()).collect();

                                    // Expand nn_indices to full-row indices (u32::MAX for nulls)
                                    let row_to_entry = if cc_ref.null_bitmap.is_empty() {
                                        nn_indices.iter().map(|&idx| idx as u32).collect()
                                    } else {
                                        let mut re = Vec::with_capacity(total);
                                        let mut vi = 0;
                                        for i in 0..total {
                                            let is_null = (cc_ref.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
                                            if is_null {
                                                re.push(u32::MAX);
                                            } else {
                                                re.push(nn_indices[vi] as u32);
                                                vi += 1;
                                            }
                                        }
                                        re
                                    };
                                    SegTextColumn::Dict { entries, row_to_entry }
                                }
                                compression::CompressionType::Lz4 | compression::CompressionType::Lz4Blocked => {
                                    let (buf, ranges) = if cc_ref.type_tag == compression::CompressionType::Lz4 {
                                        compression::lz4::decode_to_ranges(cc_ref.data, nn_count)
                                    } else {
                                        compression::lz4::decode_to_ranges_blocked(cc_ref.data, nn_count, None)
                                    };

                                    // Expand ranges to full-row ranges (u32::MAX for nulls)
                                    let row_to_range = if cc_ref.null_bitmap.is_empty() {
                                        ranges.iter().map(|&(off, len)| (off as u32, len as u16)).collect()
                                    } else {
                                        let mut rr = Vec::with_capacity(total);
                                        let mut vi = 0;
                                        for i in 0..total {
                                            let is_null = (cc_ref.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
                                            if is_null {
                                                rr.push((u32::MAX, 0u16));
                                            } else {
                                                let (off, len) = ranges[vi];
                                                rr.push((off as u32, len as u16));
                                                vi += 1;
                                            }
                                        }
                                        rr
                                    };
                                    SegTextColumn::Lz4 { buf, row_to_range }
                                }
                                _ => {
                                    blob_idx2 += 1;
                                    continue;
                                }
                            };
                            seg_text_columns[col_idx] = Some(seg_col);
                        }
                    }
                    blob_idx2 += 1;
                }
            }

            // Evaluate batch quals (WHERE) if any.
            // pre_selection seeds the selection vector so that rows already
            // filtered by LIKE during decompression are skipped (their dummy
            // datums are never dereferenced).
            let selection = evaluate_batch_quals(&decompressed, row_count, &batch_quals, pre_selection);




            // Fast path: when no GROUP BY and all agg specs are SUM/AVG on the
            // same column with Column or AddConst expr, compute base_sum once
            // and derive each result as base_sum + const_offset * non_null_count.
            // This turns O(N * num_aggs) into O(N + num_aggs).
            if !has_group_by && agg_specs.len() > 1 {
                let first_col = agg_specs[0].col_idx;
                let first_type = agg_specs[0].col_type_oid;
                let all_same_col_sum = agg_specs.iter().all(|s| {
                    s.col_idx == first_col
                        && (s.agg_type == AggType::Sum || s.agg_type == AggType::Avg)
                        && (s.expr_kind == AggExpr::Column || s.expr_kind == AggExpr::AddConst)
                });
                if all_same_col_sum {
                    let col = &decompressed[first_col as usize];
                    if !col.is_empty() {
                        let accumulators = global_accumulators.as_mut().unwrap();
                        let mut base_sum: i128 = 0;
                        let mut non_null_count: i64 = 0;
                        let use_float = matches!(first_type, pg_sys::FLOAT4OID | pg_sys::FLOAT8OID);
                        let mut base_sum_f: f64 = 0.0;
                        for row in 0..row_count {
                            if !selection.is_empty() && !selection[row] {
                                continue;
                            }
                            if !col[row].1 {
                                if use_float {
                                    base_sum_f += datum_to_f64(col[row].0, first_type);
                                } else {
                                    base_sum += datum_to_i128(col[row].0, first_type);
                                }
                                non_null_count += 1;
                            }
                        }
                        total_rows_processed += if selection.is_empty() {
                            row_count as u64
                        } else {
                            selection.iter().filter(|&&v| v).count() as u64
                        };
                        for (spec_idx, spec) in agg_specs.iter().enumerate() {
                            let acc = &mut accumulators[spec_idx];
                            if use_float {
                                if let AggAccumulator::SumFloat { sum, count } = acc {
                                    *sum += base_sum_f + spec.const_offset as f64 * non_null_count as f64;
                                    *count += non_null_count;
                                }
                            } else {
                                if let AggAccumulator::SumInt { sum, count } = acc {
                                    *sum += base_sum + spec.const_offset as i128 * non_null_count as i128;
                                    *count += non_null_count;
                                }
                            }
                        }
                        continue; // skip the generic aggregate loop for this segment
                    }
                }
            }

            // Reusable buffers for the aggregate loop (avoid per-row heap allocation)
            let mut key_ref: Vec<GroupKeyRef> = Vec::with_capacity(group_specs.len());
            let mut regex_results: Vec<Option<String>> = Vec::new();

            // ============================================================
            // COMPACT PATH: packed u128 keys + flat byte-buffer accumulators
            // ============================================================
            if use_compact_keys && use_compact_accs {
                let storage = compact_storage.as_mut().unwrap();
                let num_group_keys = group_specs.len();

                for row in 0..row_count {
                    if !selection.is_empty() && !selection[row] {
                        continue;
                    }
                    total_rows_processed += 1;

                    // Build packed u128 key from integer GROUP BY columns
                    let mut int_keys: [i64; 2] = [0; 2];
                    let mut has_null = false;
                    for (ki, gs) in group_specs.iter().enumerate() {
                        let col = &decompressed[gs.col_idx as usize];
                        if col.is_empty() || col[row].1 {
                            has_null = true;
                            break;
                        }
                        int_keys[ki] = match &gs.expr {
                            GroupByExpr::DateTrunc { unit_usecs, .. } => {
                                let pg_usec = col[row].0.value() as i64;
                                pg_usec.div_euclid(*unit_usecs) * *unit_usecs
                            }
                            GroupByExpr::Extract { unit, .. } => {
                                let pg_usec = col[row].0.value() as i64;
                                extract_field_from_usecs(pg_usec, unit)
                            }
                            GroupByExpr::AddConst { offset, .. } => {
                                col[row].0.value() as i64 + offset
                            }
                            GroupByExpr::Column => {
                                col[row].0.value() as i64
                            }
                            _ => unreachable!(),
                        };
                    }

                    // Skip null groups (they don't appear in GROUP BY results)
                    if has_null { continue; }

                    let packed = if num_group_keys == 1 {
                        pack_int_key_1(int_keys[0])
                    } else {
                        pack_int_keys_2(int_keys[0], int_keys[1])
                    };

                    // Lookup or insert group.
                    // Cap hashmap growth: above 32M entries, reserve in 8M
                    // increments instead of letting hashbrown double.
                    if compact_group_map.len() == compact_group_map.capacity() {
                        let cap = compact_group_map.capacity();
                        let extra = if cap >= 32_000_000 {
                            8_000_000 // ~170MB at 21B/slot
                        } else {
                            0 // let hashbrown double normally for small maps
                        };
                        if extra > 0 {
                            compact_group_map.reserve(extra);
                        }
                    }
                    let group_idx = match compact_group_map.entry(packed) {
                        hashbrown::hash_map::Entry::Occupied(e) => *e.get(),
                        hashbrown::hash_map::Entry::Vacant(e) => {
                            let idx = storage.alloc_group();
                            e.insert(idx);
                            idx
                        }
                    };

                    // Update compact accumulators
                    for (spec_idx, spec) in agg_specs.iter().enumerate() {
                        let (_, kind) = storage.layout.slots[spec_idx];
                        match kind {
                            CompactAccKind::Count => {
                                match spec.agg_type {
                                    AggType::CountStar => {
                                        *storage.count_mut(group_idx, spec_idx) += 1;
                                    }
                                    AggType::Count => {
                                        let col = &decompressed[spec.col_idx as usize];
                                        if !col.is_empty() && !col[row].1 {
                                            *storage.count_mut(group_idx, spec_idx) += 1;
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            CompactAccKind::SumInt => {
                                let col = &decompressed[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    let v = datum_to_i128(col[row].0, spec.col_type_oid);
                                    let (sum, count) = storage.sum_int_mut(group_idx, spec_idx);
                                    if spec.expr_kind == AggExpr::AddConst {
                                        *sum += v + spec.const_offset as i128;
                                    } else {
                                        *sum += v;
                                    }
                                    *count += 1;
                                }
                            }
                            CompactAccKind::SumIntNarrow => {
                                let col = &decompressed[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    let v = col[row].0.value() as i64;
                                    let (sum, count) = storage.sum_int_narrow_mut(group_idx, spec_idx);
                                    if spec.expr_kind == AggExpr::AddConst {
                                        *sum += v + spec.const_offset;
                                    } else {
                                        *sum += v;
                                    }
                                    *count += 1;
                                }
                            }
                            CompactAccKind::SumFloat => {
                                let col = &decompressed[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    let v = datum_to_f64(col[row].0, spec.col_type_oid);
                                    let (sum, count) = storage.sum_float_mut(group_idx, spec_idx);
                                    if spec.expr_kind == AggExpr::AddConst {
                                        *sum += v + spec.const_offset as f64;
                                    } else {
                                        *sum += v;
                                    }
                                    *count += 1;
                                }
                            }
                            CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                let col_idx = spec.col_idx as usize;
                                if let Some(ref rs) = raw_strings[col_idx]
                                    && let Some(ref s) = rs[row] {
                                        let (cur_off, cur_len) = storage.read_min_max_str(group_idx, spec_idx);
                                        let should_update = if cur_off == u32::MAX {
                                            true
                                        } else {
                                            let cur = storage.str_arena.get(cur_off, cur_len);
                                            let cmp = collation_strcmp(s, cur);
                                            match kind {
                                                CompactAccKind::MinStr => cmp < 0,
                                                CompactAccKind::MaxStr => cmp > 0,
                                                _ => unreachable!(),
                                            }
                                        };
                                        if should_update {
                                            let (new_off, new_len) = storage.str_arena.alloc(s);
                                            storage.write_min_max_str(group_idx, spec_idx, new_off, new_len);
                                        }
                                }
                            }
                        }
                    }
                }
            }
            // ============================================================
            // GENERIC PATH: original GroupKey + AggAccumulator
            // ============================================================
            else {

            for row in 0..row_count {
                if !selection.is_empty() && !selection[row] {
                    continue;
                }

                total_rows_processed += 1;

                let accumulators = if has_group_by {
                    // Clear key_ref first to release borrows on regex_results
                    key_ref.clear();
                    // Pre-compute regex results for this row (needs mutable regex_cache,
                    // so must be done before building borrowed key_ref)
                    regex_results.clear();
                    if has_regexp_group {
                        for (gi, gs) in group_specs.iter().enumerate() {
                            if let GroupByExpr::RegexpReplace { .. } = &gs.expr {
                                let rs = raw_strings[gs.col_idx as usize].as_ref().unwrap();
                                if let Some(ref input_str) = rs[row] {
                                    let rgi = regexp_group_infos.iter().find(|r| r.group_idx == gi).unwrap();
                                    let result = regex_cache.entry(input_str.clone()).or_insert_with(|| {
                                        regex_cache_calls += 1;
                                        let input_datum = {
                                            let text = pg_sys::cstring_to_text_with_len(
                                                input_str.as_ptr() as *const _, input_str.len() as i32,
                                            );
                                            pg_sys::Datum::from(text as usize)
                                        };
                                        let result_datum = pg_sys::OidFunctionCall3Coll(
                                            rgi.func_oid,
                                            rgi.collation,
                                            input_datum,
                                            rgi.pattern_datum,
                                            rgi.replacement_datum,
                                        );
                                        let cstr = pg_sys::text_to_cstring(result_datum.cast_mut_ptr());
                                        let s = std::ffi::CStr::from_ptr(cstr).to_string_lossy().into_owned();
                                        pg_sys::pfree(cstr as *mut _);
                                        s
                                    });
                                    regex_results.push(Some(result.clone()));
                                } else {
                                    regex_results.push(None);
                                }
                            }
                        }
                    }

                    // Build temporary borrowed key (reuse buffer, no heap alloc)
                    let mut regex_idx = 0;
                    for gs in group_specs.iter() {
                        let col = &decompressed[gs.col_idx as usize];
                        if col.is_empty() || col[row].1 {
                            key_ref.push(GroupKeyRef::Null);
                            if matches!(&gs.expr, GroupByExpr::RegexpReplace { .. }) {
                                regex_idx += 1;
                            }
                        } else {
                            match &gs.expr {
                                GroupByExpr::RegexpReplace { .. } => {
                                    match &regex_results[regex_idx] {
                                        Some(s) => key_ref.push(GroupKeyRef::from_str(s.as_str())),
                                        None => key_ref.push(GroupKeyRef::Null),
                                    }
                                    regex_idx += 1;
                                }
                                GroupByExpr::DateTrunc { unit_usecs, .. } => {
                                    let pg_usec = col[row].0.value() as i64;
                                    let truncated = pg_usec.div_euclid(*unit_usecs) * *unit_usecs;
                                    key_ref.push(GroupKeyRef::Int(truncated));
                                }
                                GroupByExpr::Extract { unit, .. } => {
                                    let pg_usec = col[row].0.value() as i64;
                                    let extracted = extract_field_from_usecs(pg_usec, unit);
                                    key_ref.push(GroupKeyRef::Int(extracted));
                                }
                                GroupByExpr::AddConst { offset, .. } => {
                                    let datum = col[row].0;
                                    let v = datum.value() as i64;
                                    key_ref.push(GroupKeyRef::Int(v + offset));
                                }
                                GroupByExpr::Column => {
                                    // Text GROUP BY: get &str from decoded segment data
                                    if let Some(ref seg_text) = seg_text_columns[gs.col_idx as usize] {
                                        match seg_text.get_str(row) {
                                            Some(s) => key_ref.push(GroupKeyRef::from_str(s)),
                                            None => key_ref.push(GroupKeyRef::Null),
                                        }
                                    } else {
                                        let datum = col[row].0;
                                        key_ref.push(GroupKeyRef::Int(datum.value() as i64));
                                    }
                                }
                            }
                        }
                    }

                    // Use hashbrown raw_entry to avoid cloning the key for existing groups
                    let h = hash_group_key_ref(&key_ref);
                    let group_idx = match group_map.raw_entry_mut().from_hash(h, |stored| keys_match(stored, &key_ref, &string_arena)) {
                        hashbrown::hash_map::RawEntryMut::Occupied(e) => {
                            *e.into_mut()
                        }
                        hashbrown::hash_map::RawEntryMut::Vacant(e) => {
                            let owned_key = if is_single_group_key {
                                GroupKey::Single(key_ref[0].resolve(&mut string_arena))
                            } else {
                                GroupKey::Multi(key_ref.iter().map(|r| r.resolve(&mut string_arena)).collect())
                            };
                            let idx = (flat_accs.len() / n_agg_specs) as u32;
                            for proto in &prototype_accumulators {
                                flat_accs.push(proto.clone_fresh());
                            }
                            e.insert_with_hasher(h, owned_key, idx, |k| hash_group_key(k, &string_arena));
                            idx
                        }
                    };
                    &mut flat_accs[group_idx as usize * n_agg_specs .. (group_idx as usize + 1) * n_agg_specs]
                } else {
                    global_accumulators.as_mut().unwrap().as_mut_slice()
                };

                for (spec_idx, spec) in agg_specs.iter().enumerate() {
                    let acc = &mut accumulators[spec_idx];
                    match spec.agg_type {
                        AggType::CountStar => {
                            if let AggAccumulator::Count { count } = acc {
                                *count += 1;
                            }
                        }
                        AggType::Count => {
                            let col = &decompressed[spec.col_idx as usize];
                            if !col.is_empty() && !col[row].1
                                && let AggAccumulator::Count { count } = acc
                            {
                                *count += 1;
                            }
                        }
                        AggType::Sum | AggType::Avg => {
                            // When LengthOf + raw_string_cols, compute length from raw strings
                            // (decompressed has dummy 0 datums for raw_string_cols columns)
                            if spec.expr_kind == AggExpr::LengthOf
                                && raw_string_cols.get(spec.col_idx as usize).copied().unwrap_or(false)
                            {
                                if let Some(ref rs) = raw_strings[spec.col_idx as usize]
                                    && let Some(ref s) = rs[row]
                                {
                                    match acc {
                                        AggAccumulator::SumInt { sum, count } => {
                                            *sum += s.chars().count() as i128;
                                            *count += 1;
                                        }
                                        AggAccumulator::SumFloat { sum, count } => {
                                            *sum += s.chars().count() as f64;
                                            *count += 1;
                                        }
                                        _ => {}
                                    }
                                }
                            } else {
                                let col = &decompressed[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    let datum = col[row].0;
                                    match acc {
                                        AggAccumulator::SumInt { sum, count } => {
                                            let v = datum_to_i128(datum, spec.col_type_oid);
                                            if spec.expr_kind == AggExpr::AddConst {
                                                *sum += v + spec.const_offset as i128;
                                            } else {
                                                *sum += v;
                                            }
                                            *count += 1;
                                        }
                                        AggAccumulator::SumFloat { sum, count } => {
                                            let v = datum_to_f64(datum, spec.col_type_oid);
                                            if spec.expr_kind == AggExpr::AddConst {
                                                *sum += v + spec.const_offset as f64;
                                            } else {
                                                *sum += v;
                                            }
                                            *count += 1;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        AggType::CountDistinct => {
                            let col = &decompressed[spec.col_idx as usize];
                            if !col.is_empty() && !col[row].1 {
                                let datum = col[row].0;
                                match acc {
                                    AggAccumulator::CountDistinctInt { seen } => {
                                        seen.insert(datum.value() as i64);
                                    }
                                    AggAccumulator::CountDistinctStr { seen } => {
                                        let cstr = pg_sys::text_to_cstring(datum.cast_mut_ptr());
                                        let bytes = std::ffi::CStr::from_ptr(cstr).to_bytes();
                                        let hash = hash128_str(bytes);
                                        pg_sys::pfree(cstr as *mut _);
                                        seen.insert(hash);
                                    }
                                    _ => {}
                                }
                            }
                        }
                        AggType::Min => {
                            // For text columns referenced by raw_string_cols, use raw strings
                            if raw_string_cols.get(spec.col_idx as usize).copied().unwrap_or(false) {
                                if let Some(ref rs) = raw_strings[spec.col_idx as usize]
                                    && let Some(ref s) = rs[row]
                                    && let AggAccumulator::MinStr { val } = acc
                                    && val.as_ref().is_none_or(|cur| collation_strcmp(s, cur) < 0)
                                {
                                    *val = Some(s.clone());
                                }
                            } else {
                                let col = &decompressed[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    let datum = col[row].0;
                                    match acc {
                                        AggAccumulator::MinInt { val } => {
                                            let v = datum.value() as i64;
                                            if val.is_none_or(|cur| v < cur) {
                                                *val = Some(v);
                                            }
                                        }
                                        AggAccumulator::MinFloat { val } => {
                                            let v = datum_to_f64(datum, spec.col_type_oid);
                                            if val.is_none_or(|cur| v < cur) {
                                                *val = Some(v);
                                            }
                                        }
                                        AggAccumulator::MinStr { val } => {
                                            let cstr = pg_sys::text_to_cstring(datum.cast_mut_ptr());
                                            let s = std::ffi::CStr::from_ptr(cstr).to_string_lossy().into_owned();
                                            pg_sys::pfree(cstr as *mut _);
                                            if val.as_ref().is_none_or(|cur| collation_strcmp(&s, cur) < 0) {
                                                *val = Some(s);
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        AggType::Max => {
                            if raw_string_cols.get(spec.col_idx as usize).copied().unwrap_or(false) {
                                if let Some(ref rs) = raw_strings[spec.col_idx as usize]
                                    && let Some(ref s) = rs[row]
                                    && let AggAccumulator::MaxStr { val } = acc
                                    && val.as_ref().is_none_or(|cur| collation_strcmp(s, cur) > 0)
                                {
                                    *val = Some(s.clone());
                                }
                            } else {
                                let col = &decompressed[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    let datum = col[row].0;
                                    match acc {
                                        AggAccumulator::MaxInt { val } => {
                                            let v = datum.value() as i64;
                                            if val.is_none_or(|cur| v > cur) {
                                                *val = Some(v);
                                            }
                                        }
                                        AggAccumulator::MaxFloat { val } => {
                                            let v = datum_to_f64(datum, spec.col_type_oid);
                                            if val.is_none_or(|cur| v > cur) {
                                                *val = Some(v);
                                            }
                                        }
                                        AggAccumulator::MaxStr { val } => {
                                            let cstr = pg_sys::text_to_cstring(datum.cast_mut_ptr());
                                            let s = std::ffi::CStr::from_ptr(cstr).to_string_lossy().into_owned();
                                            pg_sys::pfree(cstr as *mut _);
                                            if val.as_ref().is_none_or(|cur| collation_strcmp(&s, cur) > 0) {
                                                *val = Some(s);
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                }
            }

            } // end generic path

        }

        let agg_us = t2.elapsed().as_micros() as u64 - decompress_us;

        // Finalize results using output mapping, applying HAVING filters
        let mut topn_select_us: u64 = 0;
        let t_finalize = Instant::now();
        let mut result_rows = if use_compact_keys && use_compact_accs
            && topn_limit > 0 && having_filters.is_empty()
            && compact_group_map.len() > topn_limit as usize
        {
            // Top-N pushdown: heap-select top-N by raw sort value, finalize only those
            let sort_slot = match output_map[topn_sort_col] {
                OutputEntry::Agg(ai) => ai,
                _ => unreachable!(),
            };
            let storage = compact_storage.as_ref().unwrap();
            let t_topn = Instant::now();
            let top_entries = compact_topn_select(
                &compact_group_map, storage, sort_slot,
                topn_limit as usize, topn_ascending,
            );
            topn_select_us = t_topn.elapsed().as_micros() as u64;
            let num_group_keys = group_specs.len();
            let mut rows = Vec::with_capacity(top_entries.len());
            for &(packed_key, group_idx) in &top_entries {
                let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                for (spec_idx, spec) in agg_specs.iter().enumerate() {
                    agg_results.push(compact_finalize(storage, group_idx, spec_idx, spec));
                }
                let keys = unpack_int_keys(packed_key, num_group_keys);
                let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                for entry in &output_map {
                    match entry {
                        OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                        OutputEntry::Group(gi) => {
                            let v = keys[*gi];
                            if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. }) {
                                row.push((i128_to_numeric_datum(v as i128), false));
                            } else {
                                row.push((pg_sys::Datum::from(v as usize), false));
                            }
                        }
                        OutputEntry::Const(d, n) => row.push((*d, *n)),
                    }
                }
                rows.push(row);
            }
            rows
        } else if use_compact_keys && use_compact_accs {
            // Full compact finalization (no top-N pushdown, or HAVING present)
            let storage = compact_storage.as_ref().unwrap();
            let num_group_keys = group_specs.len();
            let mut rows = Vec::new();
            'compact_group_loop: for (&packed_key, &group_idx) in &compact_group_map {
                let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                for (spec_idx, spec) in agg_specs.iter().enumerate() {
                    agg_results.push(compact_finalize(storage, group_idx, spec_idx, spec));
                }

                // Apply HAVING filters
                for hf in &having_filters {
                    let (datum, is_null) = agg_results[hf.agg_idx];
                    if is_null {
                        continue 'compact_group_loop;
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
                        continue 'compact_group_loop;
                    }
                }

                // Unpack keys back to i64 datums
                let keys = unpack_int_keys(packed_key, num_group_keys);
                let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                for entry in &output_map {
                    match entry {
                        OutputEntry::Agg(ai) => {
                            row.push(agg_results[*ai]);
                        }
                        OutputEntry::Group(gi) => {
                            let v = keys[*gi];
                            if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. }) {
                                row.push((i128_to_numeric_datum(v as i128), false));
                            } else {
                                row.push((pg_sys::Datum::from(v as usize), false));
                            }
                        }
                        OutputEntry::Const(d, n) => row.push((*d, *n)),
                    }
                }
                rows.push(row);
                if bare_limit > 0 && rows.len() >= bare_limit as usize {
                    break;
                }
            }
            rows
        } else if has_group_by {
            let mut rows = Vec::new();
            // Pre-finalize all agg results keyed by group
            'group_loop: for (key, &group_idx) in &group_map {
                let accs = &flat_accs[group_idx as usize * n_agg_specs .. (group_idx as usize + 1) * n_agg_specs];
                let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                for (spec_idx, spec) in agg_specs.iter().enumerate() {
                    agg_results.push(finalize_accumulator(&accs[spec_idx], spec));
                }

                // Apply HAVING filters on finalized aggregate values
                for hf in &having_filters {
                    let (datum, is_null) = agg_results[hf.agg_idx];
                    if is_null {
                        continue 'group_loop; // NULL doesn't satisfy HAVING
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
                        continue 'group_loop;
                    }
                }

                let key_slice = key.as_slice();
                let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                for entry in &output_map {
                    match entry {
                        OutputEntry::Agg(ai) => {
                            row.push(agg_results[*ai]);
                        }
                        OutputEntry::Group(gi) => {
                            match &key_slice[*gi] {
                                GroupKeyVal::Null => {
                                    row.push((pg_sys::Datum::from(0usize), true));
                                }
                                GroupKeyVal::Int(v) => {
                                    if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. }) {
                                        // extract() returns numeric — convert i64 to numeric datum
                                        row.push((i128_to_numeric_datum(*v as i128), false));
                                    } else {
                                        row.push((pg_sys::Datum::from(*v as usize), false));
                                    }
                                }
                                GroupKeyVal::Str(off, len) => {
                                    let s = string_arena.get(*off, *len);
                                    let datum = string_to_datum(s, group_specs[*gi].type_oid);
                                    row.push((datum, false));
                                }
                            }
                        }
                        OutputEntry::Const(d, n) => row.push((*d, *n)),
                    }
                }
                rows.push(row);
                if bare_limit > 0 && rows.len() >= bare_limit as usize {
                    break;
                }
            }
            rows
        } else if let Some(accumulators) = &global_accumulators {
            let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
            for (spec_idx, spec) in agg_specs.iter().enumerate() {
                agg_results.push(finalize_accumulator(&accumulators[spec_idx], spec));
            }
            let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
            for entry in &output_map {
                match entry {
                    OutputEntry::Agg(ai) => {
                        row.push(agg_results[*ai]);
                    }
                    OutputEntry::Group(_) => {
                        row.push((pg_sys::Datum::from(0usize), true));
                    }
                    OutputEntry::Const(d, n) => row.push((*d, *n)),
                }
            }
            vec![row]
        } else {
            vec![]
        };

        let finalize_us = t_finalize.elapsed().as_micros() as u64;

        // Apply top-N: sort by the specified output column and truncate
        // (compact top-N pushdown path already has correct results; this handles other paths)
        let pre_topn_groups = if use_compact_keys && use_compact_accs {
            compact_group_map.len()
        } else {
            result_rows.len()
        };
        if topn_limit > 0 && has_group_by && result_rows.len() > topn_limit as usize {
            let si = topn_sort_col;
            if topn_ascending {
                result_rows.sort_by_key(|row| {
                    let (datum, is_null) = row[si];
                    if is_null { i64::MAX } else { datum.value() as i64 }
                });
            } else {
                result_rows.sort_by(|a, b| {
                    let (da, na) = a[si];
                    let (db, nb) = b[si];
                    let va = if na { i64::MIN } else { da.value() as i64 };
                    let vb = if nb { i64::MIN } else { db.value() as i64 };
                    vb.cmp(&va) // reverse order for DESC
                });
            }
            result_rows.truncate(topn_limit as usize);
        }

        // Clean up segment memory context
        if !segment_mcxt.is_null() {
            pg_sys::MemoryContextDelete(segment_mcxt);
        }

        let state = AggScanState {
            _agg_specs: agg_specs,
            _group_specs: group_specs,
            result_rows,
            result_idx: 0,
            _num_result_cols: num_result_cols,
            metadata_us,
            heap_scan_us,
            decompress_us,
            agg_us,
            total_segments,
            total_rows_processed,
            batch_quals_count: batch_quals.len(),
            where_quals_null: where_quals.is_null(),
            regex_cache_size: regex_cache.len() as u64,
            regex_cache_calls,
            topn_limit: if topn_limit > 0 { topn_limit as u64 } else { 0 },
            topn_sort_col: topn_sort_col as i64,
            topn_ascending,
            pre_topn_groups: pre_topn_groups as u64,
            merge_us: 0,
            finalize_us,
            topn_select_us,
            n_workers: 0,
            bare_limit: 0,
        };

        let state_box = Box::new(state);
        let state_ptr = Box::into_raw(state_box);
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}

/// String arena: all group key strings packed into one Vec<u8>.
/// One deallocation instead of 275K individual String deallocations.
struct StringArena {
    buf: Vec<u8>,
}

impl StringArena {
    fn new() -> Self { Self { buf: Vec::new() } }

    fn alloc(&mut self, s: &str) -> (u32, u32) {
        let off = self.buf.len() as u32;
        let len = s.len() as u32;
        self.buf.extend_from_slice(s.as_bytes());
        (off, len)
    }

    fn get(&self, off: u32, len: u32) -> &str {
        std::str::from_utf8(&self.buf[off as usize..off as usize + len as usize]).unwrap_or("")
    }
}

/// Group key value for HashMap key (owned).
/// Str variant stores (offset, len) into a StringArena.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupKeyVal {
    Null,
    Int(i64),
    Str(u32, u32), // (offset, len) into StringArena
}

/// Group key that avoids heap allocation for the common single-column case.
/// For high-cardinality GROUP BY (275K+ groups), eliminating per-key Vec
/// allocation saves ~130ms of cleanup overhead when the HashMap is dropped.
enum GroupKey {
    Single(GroupKeyVal),
    Multi(Box<[GroupKeyVal]>),
}

impl GroupKey {
    fn as_slice(&self) -> &[GroupKeyVal] {
        match self {
            GroupKey::Single(v) => std::slice::from_ref(v),
            GroupKey::Multi(v) => v,
        }
    }
}

/// Borrowed version of GroupKeyVal for hash lookups without allocation.
#[derive(Debug, Clone, Copy)]
/// Borrowed group key component without lifetime parameter.
/// Uses raw pointer for strings to avoid borrow-checker conflicts when reusing
/// the key buffer across loop iterations while mutating regex_results.
/// SAFETY: The pointed-to str data must outlive the current row iteration
/// (guaranteed by seg_text_columns and regex_results living across the loop).
enum GroupKeyRef {
    Null,
    Int(i64),
    Str(*const str),
}

impl GroupKeyRef {
    /// Create a Str variant from a &str. The caller must ensure the str outlives this GroupKeyRef.
    fn from_str(s: &str) -> Self {
        GroupKeyRef::Str(s as *const str)
    }

    fn resolve(&self, arena: &mut StringArena) -> GroupKeyVal {
        match self {
            GroupKeyRef::Null => GroupKeyVal::Null,
            GroupKeyRef::Int(v) => GroupKeyVal::Int(*v),
            GroupKeyRef::Str(p) => {
                // SAFETY: pointer is valid for the current row iteration
                let s = unsafe { &**p };
                let (off, len) = arena.alloc(s);
                GroupKeyVal::Str(off, len)
            }
        }
    }

    fn matches_owned(&self, owned: &GroupKeyVal, arena: &StringArena) -> bool {
        match (self, owned) {
            (GroupKeyRef::Null, GroupKeyVal::Null) => true,
            (GroupKeyRef::Int(a), GroupKeyVal::Int(b)) => a == b,
            (GroupKeyRef::Str(p), GroupKeyVal::Str(off, len)) => {
                // SAFETY: pointer is valid for the current row iteration
                let s = unsafe { &**p };
                s == arena.get(*off, *len)
            }
            _ => false,
        }
    }
}

/// Hash a group key component into a Hasher with a type discriminant.
fn hash_key_component<H: Hasher>(h: &mut H, val: &GroupKeyVal, arena: &StringArena) {
    match val {
        GroupKeyVal::Null => 0u8.hash(h),
        GroupKeyVal::Int(v) => { 1u8.hash(h); v.hash(h); }
        GroupKeyVal::Str(off, len) => { 2u8.hash(h); arena.get(*off, *len).hash(h); }
    }
}

fn hash_ref_component<H: Hasher>(h: &mut H, val: &GroupKeyRef) {
    match val {
        GroupKeyRef::Null => 0u8.hash(h),
        GroupKeyRef::Int(v) => { 1u8.hash(h); v.hash(h); }
        GroupKeyRef::Str(p) => {
            // SAFETY: pointer is valid for the current row iteration
            let s = unsafe { &**p };
            2u8.hash(h); s.hash(h);
        }
    }
}

/// Compute hash for an owned GroupKey (needs arena to resolve strings).
fn hash_group_key(key: &GroupKey, arena: &StringArena) -> u64 {
    let mut hasher = ahash::AHasher::default();
    for val in key.as_slice() {
        hash_key_component(&mut hasher, val, arena);
    }
    hasher.finish()
}

/// Compute hash for a borrowed group key slice (no allocation).
fn hash_group_key_ref(key: &[GroupKeyRef]) -> u64 {
    let mut hasher = ahash::AHasher::default();
    for val in key {
        hash_ref_component(&mut hasher, val);
    }
    hasher.finish()
}

/// Check if a stored owned key matches a temporary borrowed key.
fn keys_match(stored: &GroupKey, temp: &[GroupKeyRef], arena: &StringArena) -> bool {
    let s = stored.as_slice();
    s.len() == temp.len()
        && s.iter().zip(temp.iter()).all(|(s, t)| t.matches_owned(s, arena))
}

/// Per-segment decoded text data for GROUP BY columns.
/// Keeps decompressed string data alive during the row loop,
/// providing O(1) &str access per row without interning.
enum SegTextColumn {
    /// Dictionary-compressed: dict entries + per-row index (null-expanded).
    Dict {
        entries: Vec<String>,
        /// Per-row index into `entries`. u32::MAX = null.
        row_to_entry: Vec<u32>,
    },
    /// LZ4/LZ4Blocked: decompressed buffer + per-row range (null-expanded).
    Lz4 {
        buf: Vec<u8>,
        /// Per-row (offset, len). offset == u32::MAX means null.
        row_to_range: Vec<(u32, u16)>,
    },
    /// Segment-by column: same value for all rows.
    SegBy(Option<String>),
}

impl SegTextColumn {
    /// Get the string for a given row, or None if null.
    fn get_str(&self, row: usize) -> Option<&str> {
        match self {
            SegTextColumn::Dict { entries, row_to_entry } => {
                let idx = row_to_entry[row];
                if idx == u32::MAX { None } else { Some(&entries[idx as usize]) }
            }
            SegTextColumn::Lz4 { buf, row_to_range } => {
                let (off, len) = row_to_range[row];
                if off == u32::MAX {
                    None
                } else {
                    Some(std::str::from_utf8(&buf[off as usize..off as usize + len as usize]).unwrap_or(""))
                }
            }
            SegTextColumn::SegBy(opt) => opt.as_deref(),
        }
    }
}

/// Type alias for the group map using hashbrown with raw_entry support.
/// Maps group keys to indices into flat accumulator storage.
/// Using u32 index instead of Vec<AggAccumulator> eliminates per-group heap allocation
/// for accumulators, saving ~130ms cleanup for 275K groups.
type GroupMap = hashbrown::HashMap<GroupKey, u32, BuildHasherDefault<ahash::AHasher>>;

/// Convert a datum to i128 for SUM accumulation.
fn datum_to_i128(datum: pg_sys::Datum, type_oid: pg_sys::Oid) -> i128 {
    match type_oid {
        pg_sys::INT2OID => (datum.value() as i16) as i128,
        pg_sys::INT4OID => (datum.value() as i32) as i128,
        pg_sys::INT8OID => (datum.value() as i64) as i128,
        _ => datum.value() as i128,
    }
}

/// Convert a datum to f64 for float SUM/AVG.
fn datum_to_f64(datum: pg_sys::Datum, type_oid: pg_sys::Oid) -> f64 {
    match type_oid {
        pg_sys::FLOAT4OID => f32::from_bits(datum.value() as u32) as f64,
        pg_sys::FLOAT8OID => f64::from_bits(datum.value() as u64),
        _ => datum.value() as f64,
    }
}

/// Convert an i128 value to a PostgreSQL NUMERIC datum.
///
/// For values fitting in i64, uses the fast `int8_numeric` path.
/// For larger values, converts via string representation.
unsafe fn i128_to_numeric_datum(val: i128) -> pg_sys::Datum {
    unsafe {
        if val >= i64::MIN as i128 && val <= i64::MAX as i128 {
            pg_sys::OidFunctionCall1Coll(
                pg_sys::Oid::from(1781u32),  // int8_numeric
                pg_sys::InvalidOid,
                pg_sys::Datum::from(val as i64 as usize),
            )
        } else {
            let s = std::ffi::CString::new(val.to_string()).unwrap();
            pg_sys::OidFunctionCall3Coll(
                pg_sys::Oid::from(1701u32),  // numeric_in
                pg_sys::InvalidOid,
                pg_sys::Datum::from(s.as_ptr()),
                pg_sys::Datum::from(0usize),
                pg_sys::Datum::from(-1i32 as usize),
            )
        }
    }
}

/// Finalize an accumulator into a (Datum, is_null) result pair.
unsafe fn finalize_accumulator(acc: &AggAccumulator, spec: &AggExecSpec) -> (pg_sys::Datum, bool) {
    unsafe {
        match acc {
            AggAccumulator::SumInt { sum, count } => {
                if *count == 0 {
                    return (pg_sys::Datum::from(0usize), true);
                }
                match spec.agg_type {
                    AggType::Sum => {
                        // SUM(int2/int4) → INT8, SUM(int8) → NUMERIC
                        if spec.col_type_oid == pg_sys::INT8OID {
                            // Result is NUMERIC — use i128_to_numeric for full range
                            (i128_to_numeric_datum(*sum), false)
                        } else {
                            // Result is INT8
                            (pg_sys::Datum::from(*sum as i64 as usize), false)
                        }
                    }
                    AggType::Avg => {
                        // AVG(int*) → NUMERIC — use exact NUMERIC arithmetic
                        let sum_numeric = i128_to_numeric_datum(*sum);
                        let count_numeric = pg_sys::OidFunctionCall1Coll(
                            pg_sys::Oid::from(1781u32),  // int8_numeric
                            pg_sys::InvalidOid,
                            pg_sys::Datum::from(*count as usize),
                        );
                        let datum = pg_sys::OidFunctionCall2Coll(
                            pg_sys::Oid::from(1727u32),  // numeric_div
                            pg_sys::InvalidOid,
                            sum_numeric,
                            count_numeric,
                        );
                        (datum, false)
                    }
                    _ => (pg_sys::Datum::from(*sum as i64 as usize), false),
                }
            }
            AggAccumulator::SumFloat { sum, count } => {
                if *count == 0 {
                    return (pg_sys::Datum::from(0usize), true);
                }
                match spec.agg_type {
                    AggType::Sum => {
                        // SUM(float4) → FLOAT4, SUM(float8) → FLOAT8
                        if spec.col_type_oid == pg_sys::FLOAT4OID {
                            let f4 = *sum as f32;
                            (pg_sys::Datum::from(f4.to_bits() as usize), false)
                        } else {
                            (pg_sys::Datum::from(sum.to_bits() as usize), false)
                        }
                    }
                    AggType::Avg => {
                        // AVG(float*) → FLOAT8
                        let avg = *sum / *count as f64;
                        (pg_sys::Datum::from(avg.to_bits() as usize), false)
                    }
                    _ => (pg_sys::Datum::from(sum.to_bits() as usize), false),
                }
            }
            AggAccumulator::Count { count } => {
                (pg_sys::Datum::from(*count as usize), false)
            }
            AggAccumulator::CountDistinctInt { seen } => {
                (pg_sys::Datum::from(seen.len()), false)
            }
            AggAccumulator::CountDistinctStr { seen } => {
                (pg_sys::Datum::from(seen.len()), false)
            }
            AggAccumulator::MinInt { val } | AggAccumulator::MaxInt { val } => {
                match val {
                    Some(v) => (pg_sys::Datum::from(*v as usize), false),
                    None => (pg_sys::Datum::from(0usize), true),
                }
            }
            AggAccumulator::MinFloat { val } | AggAccumulator::MaxFloat { val } => {
                match val {
                    Some(v) => {
                        if spec.col_type_oid == pg_sys::FLOAT4OID {
                            let f4 = *v as f32;
                            (pg_sys::Datum::from(f4.to_bits() as usize), false)
                        } else {
                            (pg_sys::Datum::from(v.to_bits() as usize), false)
                        }
                    }
                    None => (pg_sys::Datum::from(0usize), true),
                }
            }
            AggAccumulator::MinStr { val } | AggAccumulator::MaxStr { val } => {
                match val {
                    Some(s) => {
                        let datum = string_to_datum(s, spec.col_type_oid);
                        (datum, false)
                    }
                    None => (pg_sys::Datum::from(0usize), true),
                }
            }
        }
    }
}

/// ExecCustomScan callback for DeltaXAgg: return result rows.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn exec_agg_scan(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    unsafe {
        let scan_slot = (*node).ss.ss_ScanTupleSlot;
        let state = &mut *((*node).custom_ps as *mut AggScanState);

        if state.result_idx < state.result_rows.len() {
            pg_sys::ExecClearTuple(scan_slot);
            let row = &state.result_rows[state.result_idx];
            for (i, &(datum, is_null)) in row.iter().enumerate() {
                (*scan_slot).tts_values.add(i).write(datum);
                (*scan_slot).tts_isnull.add(i).write(is_null);
            }
            pg_sys::ExecStoreVirtualTuple(scan_slot);
            state.result_idx += 1;
            return scan_slot;
        }

        // EOF
        pg_sys::ExecClearTuple(scan_slot);
        scan_slot
    }
}

/// EndCustomScan callback for DeltaXAgg.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn end_agg_scan(
    node: *mut pg_sys::CustomScanState,
) {
    unsafe {
        let state_ptr = (*node).custom_ps as *mut AggScanState;
        if !state_ptr.is_null() {
            let state = Box::from_raw(state_ptr);
            let total_us = state.metadata_us + state.heap_scan_us + state.decompress_us
                + state.agg_us + state.merge_us + state.finalize_us + state.topn_select_us;
            pgrx::log!(
                "pg_deltax DeltaXAgg timing: total={:.1}ms  metadata={:.1}ms  heap_scan={:.1}ms  \
                 decompress={:.1}ms  agg={:.1}ms  merge={:.1}ms  finalize={:.1}ms  topn_select={:.1}ms  | \
                 workers={} segments={} rows_processed={} groups={} result_rows={} topn_limit={} bare_limit={}",
                total_us as f64 / 1000.0,
                state.metadata_us as f64 / 1000.0,
                state.heap_scan_us as f64 / 1000.0,
                state.decompress_us as f64 / 1000.0,
                state.agg_us as f64 / 1000.0,
                state.merge_us as f64 / 1000.0,
                state.finalize_us as f64 / 1000.0,
                state.topn_select_us as f64 / 1000.0,
                state.n_workers,
                state.total_segments,
                state.total_rows_processed,
                state.pre_topn_groups,
                state.result_rows.len(),
                state.topn_limit,
                state.bare_limit,
            );
            (*node).custom_ps = std::ptr::null_mut();
        }
    }
}

/// ReScanCustomScan callback for DeltaXAgg.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn rescan_agg_scan(
    node: *mut pg_sys::CustomScanState,
) {
    unsafe {
        let state = &mut *((*node).custom_ps as *mut AggScanState);
        state.result_idx = 0;
    }
}

// ============================================================================
// Compact Accumulator Storage (Phase 1)
// ============================================================================

/// Kind of accumulator slot in compact storage.
#[derive(Clone, Copy, Debug, PartialEq)]
enum CompactAccKind {
    Count,          // 8 bytes: i64
    SumInt,         // 24 bytes: i128 sum (16) + i64 count (8) — for INT8 columns
    SumIntNarrow,   // 16 bytes: i64 sum (8) + i64 count (8) — for INT2/INT4 columns
    SumFloat,       // 16 bytes: f64 sum (8) + i64 count (8)
    MinStr,         // 8 bytes: u32 arena_offset + u32 length (sentinel: u32::MAX, 0)
    MaxStr,         // 8 bytes: u32 arena_offset + u32 length (sentinel: u32::MAX, 0)
}

impl CompactAccKind {
    fn byte_size(self) -> usize {
        match self {
            CompactAccKind::Count => 8,
            CompactAccKind::SumInt => 24,
            CompactAccKind::SumIntNarrow => 16,
            CompactAccKind::SumFloat => 16,
            CompactAccKind::MinStr | CompactAccKind::MaxStr => 8,
        }
    }

    fn alignment(self) -> usize {
        match self {
            CompactAccKind::Count => 8,
            CompactAccKind::SumInt => 16, // i128 needs 16-byte alignment
            CompactAccKind::SumIntNarrow => 8,
            CompactAccKind::SumFloat => 8,
            CompactAccKind::MinStr | CompactAccKind::MaxStr => 4,
        }
    }
}

/// Layout of compact accumulator slots for one group.
struct CompactAccLayout {
    /// (byte_offset, kind) per aggregate
    slots: Vec<(usize, CompactAccKind)>,
    /// Total bytes per group (aligned to 16)
    group_stride: usize,
}

impl CompactAccLayout {
    fn new(specs: &[AggExecSpec]) -> Self {
        let mut offset: usize = 0;

        // Sort by alignment (descending) to minimize padding.
        // We need to maintain original order for indexing, so we compute
        // offsets in alignment order then map back.
        let mut indexed: Vec<(usize, CompactAccKind)> = specs.iter().enumerate().map(|(i, spec)| {
            let kind = compact_acc_kind(spec);
            (i, kind)
        }).collect();
        // Sort by alignment descending (i128 first, then i64/f64)
        indexed.sort_by(|a, b| b.1.alignment().cmp(&a.1.alignment()));

        let mut slots = vec![(0usize, CompactAccKind::Count); specs.len()];
        for (orig_idx, kind) in &indexed {
            let align = kind.alignment();
            offset = (offset + align - 1) & !(align - 1);
            slots[*orig_idx] = (offset, *kind);
            offset += kind.byte_size();
        }

        // Align stride to 16 so i128 fields in next group are aligned
        let group_stride = (offset + 15) & !15;

        CompactAccLayout { slots, group_stride }
    }
}

/// Determine the CompactAccKind for a given agg spec.
///
/// INT2/INT4 columns use SumIntNarrow (i64 sum, 16B) since their sums
/// cannot overflow i64 even at 2^31 rows × max value (2^31 × 2^31 < 2^63).
/// INT8 columns use SumInt (i128 sum, 24B) to handle potential overflow.
fn compact_acc_kind(spec: &AggExecSpec) -> CompactAccKind {
    match spec.agg_type {
        AggType::CountStar | AggType::Count => CompactAccKind::Count,
        AggType::Sum | AggType::Avg => {
            if spec.col_type_oid == pg_sys::FLOAT4OID || spec.col_type_oid == pg_sys::FLOAT8OID {
                CompactAccKind::SumFloat
            } else if spec.col_type_oid == pg_sys::INT2OID || spec.col_type_oid == pg_sys::INT4OID {
                CompactAccKind::SumIntNarrow
            } else {
                CompactAccKind::SumInt
            }
        }
        AggType::Min => {
            let t = spec.col_type_oid;
            if t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID {
                CompactAccKind::MinStr
            } else {
                unreachable!("compact_acc_kind: MIN on non-text not supported in compact path")
            }
        }
        AggType::Max => {
            let t = spec.col_type_oid;
            if t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID {
                CompactAccKind::MaxStr
            } else {
                unreachable!("compact_acc_kind: MAX on non-text not supported in compact path")
            }
        }
        _ => unreachable!("compact_acc_kind called for unsupported agg type"),
    }
}

/// Flat byte buffer holding compact accumulators for all groups.
struct CompactAccStorage {
    buf: Vec<u8>,
    layout: CompactAccLayout,
    str_arena: StringArena,
}

impl CompactAccStorage {
    fn new(layout: CompactAccLayout) -> Self {
        CompactAccStorage {
            buf: Vec::new(),
            layout,
            str_arena: StringArena::new(),
        }
    }


    /// Allocate accumulators for a new group. Returns the group index.
    ///
    /// Growth strategy: below 1GB, let Vec double normally. Above 1GB,
    /// grow by 2GB increments to cap peak waste at ~2GB instead of 100%.
    #[inline]
    fn alloc_group(&mut self) -> u32 {
        let new_len = self.buf.len() + self.layout.group_stride;
        if new_len > self.buf.capacity() {
            const GB: usize = 1 << 30;
            let extra = if self.buf.capacity() >= GB {
                2 * GB // fixed 2GB growth for large buffers
            } else {
                self.buf.capacity().max(self.layout.group_stride) // double (normal)
            };
            self.buf.reserve(extra);
        }
        let group_idx = self.buf.len() / self.layout.group_stride;
        self.buf.resize(new_len, 0);
        // Set MinStr/MaxStr sentinels (u32::MAX offset = no value)
        for slot_idx in 0..self.layout.slots.len() {
            let (_, kind) = self.layout.slots[slot_idx];
            if kind == CompactAccKind::MinStr || kind == CompactAccKind::MaxStr {
                unsafe { self.write_min_max_str(group_idx as u32, slot_idx, u32::MAX, 0); }
            }
        }
        group_idx as u32
    }

    /// Get a mutable i64 reference (for Count).
    #[inline]
    unsafe fn count_mut(&mut self, group_idx: u32, slot: usize) -> &mut i64 {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let ptr = self.buf.as_mut_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            &mut *(ptr as *mut i64)
        }
    }

    /// Get mutable references to (sum: i128, count: i64) for SumInt.
    #[inline]
    unsafe fn sum_int_mut(&mut self, group_idx: u32, slot: usize) -> (&mut i128, &mut i64) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self.buf.as_mut_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            let sum = &mut *(base as *mut i128);
            let count = &mut *(base.add(16) as *mut i64);
            (sum, count)
        }
    }

    /// Get mutable references to (sum: i64, count: i64) for SumIntNarrow.
    #[inline]
    unsafe fn sum_int_narrow_mut(&mut self, group_idx: u32, slot: usize) -> (&mut i64, &mut i64) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self.buf.as_mut_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            let sum = &mut *(base as *mut i64);
            let count = &mut *(base.add(8) as *mut i64);
            (sum, count)
        }
    }

    /// Get mutable references to (sum: f64, count: i64) for SumFloat.
    #[inline]
    unsafe fn sum_float_mut(&mut self, group_idx: u32, slot: usize) -> (&mut f64, &mut i64) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self.buf.as_mut_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            let sum = &mut *(base as *mut f64);
            let count = &mut *(base.add(8) as *mut i64);
            (sum, count)
        }
    }

    /// Read count value for finalization.
    #[inline]
    unsafe fn read_count(&self, group_idx: u32, slot: usize) -> i64 {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let ptr = self.buf.as_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            *(ptr as *const i64)
        }
    }

    /// Read (sum_i128, count) for finalization.
    #[inline]
    unsafe fn read_sum_int(&self, group_idx: u32, slot: usize) -> (i128, i64) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self.buf.as_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            let sum = *(base as *const i128);
            let count = *(base.add(16) as *const i64);
            (sum, count)
        }
    }

    /// Read (sum_i64, count) for finalization (narrow path).
    #[inline]
    unsafe fn read_sum_int_narrow(&self, group_idx: u32, slot: usize) -> (i64, i64) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self.buf.as_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            let sum = *(base as *const i64);
            let count = *(base.add(8) as *const i64);
            (sum, count)
        }
    }

    /// Read (sum_f64, count) for finalization.
    #[inline]
    unsafe fn read_sum_float(&self, group_idx: u32, slot: usize) -> (f64, i64) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self.buf.as_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            let sum = *(base as *const f64);
            let count = *(base.add(8) as *const i64);
            (sum, count)
        }
    }

    /// Read MinStr/MaxStr: returns (arena_offset, length). Sentinel is (u32::MAX, 0) = no value.
    #[inline]
    unsafe fn read_min_max_str(&self, group_idx: u32, slot: usize) -> (u32, u32) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self.buf.as_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            let off = *(base as *const u32);
            let len = *(base.add(4) as *const u32);
            (off, len)
        }
    }

    /// Write MinStr/MaxStr arena offset and length.
    #[inline]
    unsafe fn write_min_max_str(&mut self, group_idx: u32, slot: usize, off: u32, len: u32) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self.buf.as_mut_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            *(base as *mut u32) = off;
            *(base.add(4) as *mut u32) = len;
        }
    }

}

/// Check if all aggregates can use the compact accumulator path.
fn can_use_compact_accs(agg_specs: &[AggExecSpec]) -> bool {
    if agg_specs.is_empty() {
        return false;
    }
    agg_specs.iter().all(|spec| {
        match spec.agg_type {
            AggType::CountStar | AggType::Count => true,
            AggType::Sum | AggType::Avg => {
                let t = spec.col_type_oid;
                t == pg_sys::INT2OID || t == pg_sys::INT4OID || t == pg_sys::INT8OID
                    || t == pg_sys::FLOAT4OID || t == pg_sys::FLOAT8OID
            }
            AggType::Min | AggType::Max => {
                let t = spec.col_type_oid;
                t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID
            }
            _ => false,
        }
    })
}

/// Select the top-N (packed_key, group_idx) pairs by reading raw sort values
/// from compact storage, without full finalization.
///
/// Uses a BinaryHeap of size `limit` to find the top-N in O(n log limit) time.
unsafe fn compact_topn_select(
    map: &CompactGroupMap,
    storage: &CompactAccStorage,
    sort_slot: usize,
    limit: usize,
    ascending: bool,
) -> Vec<(u128, u32)> {
    unsafe {
        let (_, kind) = storage.layout.slots[sort_slot];
        if ascending {
            // Min-N: max-heap evicts the largest, keeping the smallest N
            let mut heap: BinaryHeap<(i64, u128, u32)> = BinaryHeap::with_capacity(limit + 1);
            for (&packed_key, &group_idx) in map {
                let val = match kind {
                    CompactAccKind::Count => storage.read_count(group_idx, sort_slot),
                    CompactAccKind::SumIntNarrow => storage.read_sum_int_narrow(group_idx, sort_slot).0,
                    _ => storage.read_count(group_idx, sort_slot),
                };
                heap.push((val, packed_key, group_idx));
                if heap.len() > limit {
                    heap.pop();
                }
            }
            let mut result: Vec<(u128, u32)> = heap.into_iter().map(|(_, k, g)| (k, g)).collect();
            // Sort ascending by re-reading values
            result.sort_by_key(|&(_, g)| match kind {
                CompactAccKind::Count => storage.read_count(g, sort_slot),
                CompactAccKind::SumIntNarrow => storage.read_sum_int_narrow(g, sort_slot).0,
                _ => storage.read_count(g, sort_slot),
            });
            result
        } else {
            // Max-N: min-heap (via Reverse) evicts the smallest, keeping the largest N
            let mut heap: BinaryHeap<Reverse<(i64, u128, u32)>> = BinaryHeap::with_capacity(limit + 1);
            for (&packed_key, &group_idx) in map {
                let val = match kind {
                    CompactAccKind::Count => storage.read_count(group_idx, sort_slot),
                    CompactAccKind::SumIntNarrow => storage.read_sum_int_narrow(group_idx, sort_slot).0,
                    _ => storage.read_count(group_idx, sort_slot),
                };
                heap.push(Reverse((val, packed_key, group_idx)));
                if heap.len() > limit {
                    heap.pop();
                }
            }
            let mut result: Vec<(u128, u32)> = heap.into_iter().map(|Reverse((_, k, g))| (k, g)).collect();
            // Sort descending by re-reading values
            result.sort_by(|&(_, ga), &(_, gb)| {
                let va = match kind {
                    CompactAccKind::Count => storage.read_count(ga, sort_slot),
                    CompactAccKind::SumIntNarrow => storage.read_sum_int_narrow(ga, sort_slot).0,
                    _ => storage.read_count(ga, sort_slot),
                };
                let vb = match kind {
                    CompactAccKind::Count => storage.read_count(gb, sort_slot),
                    CompactAccKind::SumIntNarrow => storage.read_sum_int_narrow(gb, sort_slot).0,
                    _ => storage.read_count(gb, sort_slot),
                };
                vb.cmp(&va)
            });
            result
        }
    }
}

/// Finalize a compact accumulator slot into a (Datum, is_null) pair.
unsafe fn compact_finalize(
    storage: &CompactAccStorage,
    group_idx: u32,
    slot: usize,
    spec: &AggExecSpec,
) -> (pg_sys::Datum, bool) {
    unsafe {
        let (_, kind) = storage.layout.slots[slot];
        match kind {
            CompactAccKind::Count => {
                let count = storage.read_count(group_idx, slot);
                (pg_sys::Datum::from(count as usize), false)
            }
            CompactAccKind::SumInt => {
                let (sum, count) = storage.read_sum_int(group_idx, slot);
                if count == 0 {
                    return (pg_sys::Datum::from(0usize), true);
                }
                match spec.agg_type {
                    AggType::Sum => {
                        if spec.col_type_oid == pg_sys::INT8OID {
                            (i128_to_numeric_datum(sum), false)
                        } else {
                            (pg_sys::Datum::from(sum as i64 as usize), false)
                        }
                    }
                    AggType::Avg => {
                        let sum_numeric = i128_to_numeric_datum(sum);
                        let count_numeric = pg_sys::OidFunctionCall1Coll(
                            pg_sys::Oid::from(1781u32),
                            pg_sys::InvalidOid,
                            pg_sys::Datum::from(count as usize),
                        );
                        let datum = pg_sys::OidFunctionCall2Coll(
                            pg_sys::Oid::from(1727u32),
                            pg_sys::InvalidOid,
                            sum_numeric,
                            count_numeric,
                        );
                        (datum, false)
                    }
                    _ => (pg_sys::Datum::from(sum as i64 as usize), false),
                }
            }
            CompactAccKind::SumIntNarrow => {
                let (sum, count) = storage.read_sum_int_narrow(group_idx, slot);
                if count == 0 {
                    return (pg_sys::Datum::from(0usize), true);
                }
                match spec.agg_type {
                    AggType::Sum => {
                        // SUM(int2/int4) → INT8
                        (pg_sys::Datum::from(sum as usize), false)
                    }
                    AggType::Avg => {
                        // AVG(int*) → NUMERIC
                        let sum_numeric = i128_to_numeric_datum(sum as i128);
                        let count_numeric = pg_sys::OidFunctionCall1Coll(
                            pg_sys::Oid::from(1781u32),
                            pg_sys::InvalidOid,
                            pg_sys::Datum::from(count as usize),
                        );
                        let datum = pg_sys::OidFunctionCall2Coll(
                            pg_sys::Oid::from(1727u32),
                            pg_sys::InvalidOid,
                            sum_numeric,
                            count_numeric,
                        );
                        (datum, false)
                    }
                    _ => (pg_sys::Datum::from(sum as usize), false),
                }
            }
            CompactAccKind::SumFloat => {
                let (sum, count) = storage.read_sum_float(group_idx, slot);
                if count == 0 {
                    return (pg_sys::Datum::from(0usize), true);
                }
                match spec.agg_type {
                    AggType::Sum => {
                        if spec.col_type_oid == pg_sys::FLOAT4OID {
                            let f4 = sum as f32;
                            (pg_sys::Datum::from(f4.to_bits() as usize), false)
                        } else {
                            (pg_sys::Datum::from(sum.to_bits() as usize), false)
                        }
                    }
                    AggType::Avg => {
                        let avg = sum / count as f64;
                        (pg_sys::Datum::from(avg.to_bits() as usize), false)
                    }
                    _ => (pg_sys::Datum::from(sum.to_bits() as usize), false),
                }
            }
            CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                let (off, len) = storage.read_min_max_str(group_idx, slot);
                if off == u32::MAX {
                    (pg_sys::Datum::from(0usize), true) // NULL
                } else {
                    let s = storage.str_arena.get(off, len);
                    let datum = string_to_datum(s, spec.col_type_oid);
                    (datum, false)
                }
            }
        }
    }
}

// ============================================================================
// Packed Integer Keys (Phase 2)
// ============================================================================

/// Check if all GROUP BY columns produce integer values and can be packed into u128.
fn can_use_compact_keys(group_specs: &[GroupByColSpec]) -> bool {
    if group_specs.is_empty() || group_specs.len() > 2 {
        return false; // u128 fits at most 2 x i64
    }
    group_specs.iter().all(|gs| {
        match &gs.expr {
            GroupByExpr::Column => {
                let t = gs.type_oid;
                t == pg_sys::INT2OID || t == pg_sys::INT4OID || t == pg_sys::INT8OID
                    || t == pg_sys::TIMESTAMPOID || t == pg_sys::TIMESTAMPTZOID
            }
            GroupByExpr::DateTrunc { .. } => true, // returns i64
            GroupByExpr::Extract { .. } => true,   // returns i64
            GroupByExpr::AddConst { .. } => true,  // returns i64
            GroupByExpr::RegexpReplace { .. } => false,
        }
    })
}

/// Pack up to 2 int64 keys into a u128.
#[inline]
fn pack_int_keys_2(k0: i64, k1: i64) -> u128 {
    (k0 as u64 as u128) | ((k1 as u64 as u128) << 64)
}

/// Pack a single int64 key into u128.
#[inline]
fn pack_int_key_1(k0: i64) -> u128 {
    k0 as u64 as u128
}

/// Unpack a u128 back into individual i64 keys.
#[inline]
fn unpack_int_keys(packed: u128, num_keys: usize) -> [i64; 2] {
    let k0 = packed as u64 as i64;
    let k1 = if num_keys > 1 { (packed >> 64) as u64 as i64 } else { 0 };
    [k0, k1]
}

/// Type alias for compact group map with u128 keys.
type CompactGroupMap = hashbrown::HashMap<u128, u32, BuildHasherDefault<ahash::AHasher>>;

// ============================================================================
// Parallel Compact Aggregation
// ============================================================================

use super::{PG_EPOCH_OFFSET_USEC, PG_EPOCH_OFFSET_DAYS};

/// Decompress a numeric/timestamp/date column from a compressed blob to
/// `Vec<(pg_sys::Datum, bool)>` using only pure-Rust decompression.
///
/// SAFETY: This function does NOT call any PG functions and is safe to call
/// from worker threads. Only handles integer, float, timestamp, date, and bool
/// types (pass-by-value types where Datum is just the raw value).
fn decompress_numeric_blob(
    blob: &[u8],
    type_oid: pg_sys::Oid,
) -> Vec<(pg_sys::Datum, bool)> {
    if blob.is_empty() {
        return Vec::new();
    }

    let cc = compression::CompressedColumnRef::from_bytes(blob);
    let total_count = cc.row_count as usize;
    let non_null_count = count_non_null(cc.null_bitmap, total_count);

    let nn_datums: Vec<pg_sys::Datum> = match cc.type_tag {
        compression::CompressionType::Gorilla => {
            if type_oid == pg_sys::TIMESTAMPOID || type_oid == pg_sys::TIMESTAMPTZOID {
                let timestamps = compression::gorilla::decode_timestamps(cc.data, non_null_count);
                timestamps.iter()
                    .map(|&usec| {
                        let pg_usec = usec - PG_EPOCH_OFFSET_USEC;
                        pg_sys::Datum::from(pg_usec as usize)
                    })
                    .collect()
            } else if type_oid == pg_sys::DATEOID {
                let timestamps = compression::gorilla::decode_timestamps(cc.data, non_null_count);
                timestamps.iter()
                    .map(|&usec| {
                        let unix_days = (usec / 86_400_000_000) as i32;
                        let pg_days = unix_days - PG_EPOCH_OFFSET_DAYS;
                        pg_sys::Datum::from(pg_days as usize)
                    })
                    .collect()
            } else if type_oid == pg_sys::FLOAT4OID {
                let floats = compression::gorilla::decode_floats_f32(cc.data, non_null_count);
                floats.iter()
                    .map(|&v| pg_sys::Datum::from(v.to_bits() as usize))
                    .collect()
            } else {
                // FLOAT8OID
                let floats = compression::gorilla::decode_floats(cc.data, non_null_count);
                floats.iter()
                    .map(|&v| pg_sys::Datum::from(v.to_bits() as usize))
                    .collect()
            }
        }
        compression::CompressionType::DeltaVarint => {
            if type_oid == pg_sys::INT2OID {
                let ints = compression::integer::decode_i32(cc.data, non_null_count);
                ints.iter().map(|&v| pg_sys::Datum::from(v as i16 as usize)).collect()
            } else if type_oid == pg_sys::INT4OID || type_oid == pg_sys::DATEOID {
                let ints = compression::integer::decode_i32(cc.data, non_null_count);
                ints.iter().map(|&v| pg_sys::Datum::from(v as usize)).collect()
            } else {
                // INT8OID, TIMESTAMPOID, TIMESTAMPTZOID
                let ints = compression::integer::decode_i64(cc.data, non_null_count);
                ints.iter().map(|&v| pg_sys::Datum::from(v as usize)).collect()
            }
        }
        compression::CompressionType::Constant => {
            if type_oid == pg_sys::INT2OID {
                let ints = compression::bitpacked::decode_constant_i32(cc.data, non_null_count);
                ints.iter().map(|&v| pg_sys::Datum::from(v as i16 as usize)).collect()
            } else if type_oid == pg_sys::INT4OID || type_oid == pg_sys::DATEOID {
                let ints = compression::bitpacked::decode_constant_i32(cc.data, non_null_count);
                ints.iter().map(|&v| pg_sys::Datum::from(v as usize)).collect()
            } else if type_oid == pg_sys::FLOAT4OID {
                let ints = compression::bitpacked::decode_constant_i64(cc.data, non_null_count);
                ints.iter().map(|&v| pg_sys::Datum::from(f32::from_bits(v as u32).to_bits() as usize)).collect()
            } else if type_oid == pg_sys::FLOAT8OID {
                let ints = compression::bitpacked::decode_constant_i64(cc.data, non_null_count);
                ints.iter().map(|&v| pg_sys::Datum::from(f64::from_bits(v as u64).to_bits() as usize)).collect()
            } else {
                let ints = compression::bitpacked::decode_constant_i64(cc.data, non_null_count);
                ints.iter().map(|&v| pg_sys::Datum::from(v as usize)).collect()
            }
        }
        compression::CompressionType::ForBitpacked => {
            if type_oid == pg_sys::INT2OID {
                let ints = compression::bitpacked::decode_for_i32(cc.data, non_null_count);
                ints.iter().map(|&v| pg_sys::Datum::from(v as i16 as usize)).collect()
            } else if type_oid == pg_sys::INT4OID || type_oid == pg_sys::DATEOID {
                let ints = compression::bitpacked::decode_for_i32(cc.data, non_null_count);
                ints.iter().map(|&v| pg_sys::Datum::from(v as usize)).collect()
            } else {
                let ints = compression::bitpacked::decode_for_i64(cc.data, non_null_count);
                ints.iter().map(|&v| pg_sys::Datum::from(v as usize)).collect()
            }
        }
        compression::CompressionType::BooleanBitmap => {
            let bools = compression::boolean::decode(cc.data, non_null_count);
            bools.iter().map(|&b| pg_sys::Datum::from(b as usize)).collect()
        }
        _ => {
            // Text/dictionary/lz4 types — should not happen in compact path
            Vec::new()
        }
    };

    // Reinsert nulls
    if cc.null_bitmap.is_empty() {
        nn_datums.iter().map(|&d| (d, false)).collect()
    } else {
        let mut result = Vec::with_capacity(total_count);
        let mut val_idx = 0;
        for i in 0..total_count {
            let is_null = (cc.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
            if is_null {
                result.push((pg_sys::Datum::from(0usize), true));
            } else {
                result.push((nn_datums[val_idx], false));
                val_idx += 1;
            }
        }
        result
    }
}

/// Check if all batch quals reference only numeric/comparable types (no text).
/// Text LIKE/Eq/Ne quals require PG functions during decompression, making them
/// unsafe for worker threads.
fn batch_quals_all_numeric(batch_quals: &[BatchQual]) -> bool {
    batch_quals.iter().all(|bq| {
        matches!(bq.type_oid,
            pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID
            | pg_sys::FLOAT4OID | pg_sys::FLOAT8OID
            | pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID
            | pg_sys::DATEOID | pg_sys::BOOLOID
        )
    })
}

/// Check if a column type is supported for thread-safe decompression.
fn is_numeric_type(type_oid: pg_sys::Oid) -> bool {
    matches!(type_oid,
        pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID
        | pg_sys::FLOAT4OID | pg_sys::FLOAT8OID
        | pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID
        | pg_sys::DATEOID | pg_sys::BOOLOID
    )
}

/// Configuration for parallel compact aggregation (read-only, shared across threads).
struct ParallelCompactConfig<'a> {
    agg_specs: &'a [AggExecSpec],
    group_specs: &'a [GroupByColSpec],
    col_names: &'a [String],
    col_types: &'a [pg_sys::Oid],
    segment_by: &'a [String],
    needed_cols: &'a [bool],
    batch_quals: &'a [BatchQual],
    seg_filters: &'a [(usize, String)],
    time_min: Option<i64>,
    time_max: Option<i64>,
    /// If set, each worker computes top-K candidates for speculative merge-skip.
    /// (sort_slot, k, ascending)
    topn_spec: Option<(usize, usize, bool)>,
}

/// Result of parallel compact aggregation from one worker thread.
struct ParallelCompactResult {
    compact_map: CompactGroupMap,
    compact_storage: CompactAccStorage,
    segments_processed: u64,
    rows_processed: u64,
    decompress_us: u64,
    /// Pre-computed top-K candidates: (keys, floor_value).
    /// Present when `config.topn_spec` is set.
    topk: Option<(Vec<u128>, i64)>,
}

/// Process a chunk of segments on a worker thread using the compact path.
///
/// Does decompression + aggregation entirely in pure Rust (no PG function calls).
/// Safe to call from any thread.
fn process_segments_compact(
    segments: &[SegmentData],
    config: &ParallelCompactConfig,
) -> ParallelCompactResult {
    let mut compact_map = CompactGroupMap::with_hasher(BuildHasherDefault::default());
    let mut compact_storage = CompactAccStorage::new(CompactAccLayout::new(config.agg_specs));
    let mut segments_processed: u64 = 0;
    let mut rows_processed: u64 = 0;
    let mut decompress_us: u64 = 0;
    let num_group_keys = config.group_specs.len();

    for seg in segments {
        if seg.row_count == 0 {
            continue;
        }

        // Segment-by pruning
        if !config.seg_filters.is_empty() {
            let mut skip = false;
            for &(seg_val_idx, ref filter_val) in config.seg_filters {
                match &seg.segment_values[seg_val_idx] {
                    Some(val) if val == filter_val => {}
                    _ => { skip = true; break; }
                }
            }
            if skip { continue; }
        }

        // Time-range pruning
        if let (Some(seg_min), Some(seg_max)) = (seg.min_time, seg.max_time) {
            if config.time_min.is_some_and(|query_min| seg_max < query_min) { continue; }
            if config.time_max.is_some_and(|query_max| seg_min > query_max) { continue; }
        }

        // Dictionary-based LIKE pruning
        if segment_skippable_by_dict_like(
            config.batch_quals, config.col_names, config.segment_by, &seg.compressed_blobs,
        ) {
            continue;
        }

        segments_processed += 1;

        // Decompress needed columns (pure Rust, no PG calls)
        let t_dec = Instant::now();
        let mut decompressed: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
        let mut blob_idx = 0;
        let mut seg_val_idx = 0;

        for (col_idx, col_name) in config.col_names.iter().enumerate() {
            let type_oid = config.col_types[col_idx];

            if !config.needed_cols[col_idx] {
                if config.segment_by.contains(col_name) {
                    seg_val_idx += 1;
                } else {
                    blob_idx += 1;
                }
                decompressed.push(Vec::new());
                continue;
            }

            if config.segment_by.contains(col_name) {
                // Parse segment_by string to integer datum directly (no PG calls)
                let val = &seg.segment_values[seg_val_idx];
                let (datum, is_null) = match val {
                    Some(s) => {
                        let d = parse_string_to_datum(s, type_oid);
                        (d, false)
                    }
                    None => (pg_sys::Datum::from(0usize), true),
                };
                let repeated: Vec<(pg_sys::Datum, bool)> =
                    (0..seg.row_count).map(|_| (datum, is_null)).collect();
                decompressed.push(repeated);
                seg_val_idx += 1;
            } else {
                let blob = &seg.compressed_blobs[blob_idx];
                decompressed.push(decompress_numeric_blob(blob, type_oid));
                blob_idx += 1;
            }
        }
        decompress_us += t_dec.elapsed().as_micros() as u64;

        let row_count = seg.row_count as usize;

        // Evaluate batch quals (pure Rust for numeric types)
        let selection = evaluate_batch_quals(&decompressed, row_count, config.batch_quals, Vec::new());

        // Compact aggregation loop (identical to single-threaded path)
        for row in 0..row_count {
            if !selection.is_empty() && !selection[row] {
                continue;
            }
            rows_processed += 1;

            // Build packed u128 key
            let mut int_keys: [i64; 2] = [0; 2];
            let mut has_null = false;
            for (ki, gs) in config.group_specs.iter().enumerate() {
                let col = &decompressed[gs.col_idx as usize];
                if col.is_empty() || col[row].1 {
                    has_null = true;
                    break;
                }
                int_keys[ki] = match &gs.expr {
                    GroupByExpr::DateTrunc { unit_usecs, .. } => {
                        let pg_usec = col[row].0.value() as i64;
                        pg_usec.div_euclid(*unit_usecs) * *unit_usecs
                    }
                    GroupByExpr::Extract { unit, .. } => {
                        let pg_usec = col[row].0.value() as i64;
                        extract_field_from_usecs(pg_usec, unit)
                    }
                    GroupByExpr::AddConst { offset, .. } => {
                        col[row].0.value() as i64 + offset
                    }
                    GroupByExpr::Column => {
                        col[row].0.value() as i64
                    }
                    _ => unreachable!(),
                };
            }

            if has_null { continue; }

            let packed = if num_group_keys == 1 {
                pack_int_key_1(int_keys[0])
            } else {
                pack_int_keys_2(int_keys[0], int_keys[1])
            };

            // Lookup or insert group
            if compact_map.len() == compact_map.capacity() {
                let cap = compact_map.capacity();
                if cap >= 32_000_000 {
                    compact_map.reserve(8_000_000);
                }
            }
            let group_idx = match compact_map.entry(packed) {
                hashbrown::hash_map::Entry::Occupied(e) => *e.get(),
                hashbrown::hash_map::Entry::Vacant(e) => {
                    let idx = compact_storage.alloc_group();
                    e.insert(idx);
                    idx
                }
            };

            // Update compact accumulators
            for (spec_idx, spec) in config.agg_specs.iter().enumerate() {
                let (_, kind) = compact_storage.layout.slots[spec_idx];
                match kind {
                    CompactAccKind::Count => {
                        match spec.agg_type {
                            AggType::CountStar => {
                                unsafe { *compact_storage.count_mut(group_idx, spec_idx) += 1; }
                            }
                            AggType::Count => {
                                let col = &decompressed[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    unsafe { *compact_storage.count_mut(group_idx, spec_idx) += 1; }
                                }
                            }
                            _ => {}
                        }
                    }
                    CompactAccKind::SumInt => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = datum_to_i128(col[row].0, spec.col_type_oid);
                            let (sum, count) = unsafe { compact_storage.sum_int_mut(group_idx, spec_idx) };
                            if spec.expr_kind == AggExpr::AddConst {
                                *sum += v + spec.const_offset as i128;
                            } else {
                                *sum += v;
                            }
                            *count += 1;
                        }
                    }
                    CompactAccKind::SumIntNarrow => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = col[row].0.value() as i64;
                            let (sum, count) = unsafe { compact_storage.sum_int_narrow_mut(group_idx, spec_idx) };
                            if spec.expr_kind == AggExpr::AddConst {
                                *sum += v + spec.const_offset;
                            } else {
                                *sum += v;
                            }
                            *count += 1;
                        }
                    }
                    CompactAccKind::SumFloat => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = datum_to_f64(col[row].0, spec.col_type_oid);
                            let (sum, count) = unsafe { compact_storage.sum_float_mut(group_idx, spec_idx) };
                            if spec.expr_kind == AggExpr::AddConst {
                                *sum += v + spec.const_offset as f64;
                            } else {
                                *sum += v;
                            }
                            *count += 1;
                        }
                    }
                    CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                        // compact parallel path requires all_needed_cols_numeric,
                        // so MinStr/MaxStr cannot appear here
                        unreachable!("MinStr/MaxStr in compact parallel worker")
                    }
                }
            }
        }
    }

    // Compute top-K candidates while data is cache-hot (if requested)
    let topk = config.topn_spec.map(|(sort_slot, k, ascending)| {
        let (_, sort_kind) = compact_storage.layout.slots[sort_slot];
        let read_val = |gidx: u32| -> i64 {
            unsafe {
                match sort_kind {
                    CompactAccKind::Count => compact_storage.read_count(gidx, sort_slot),
                    CompactAccKind::SumIntNarrow => compact_storage.read_sum_int_narrow(gidx, sort_slot).0,
                    _ => compact_storage.read_count(gidx, sort_slot),
                }
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
                if heap.len() > k { heap.pop(); }
            }
            let floor = heap.peek().map(|&Reverse((v, _))| v).unwrap_or(0);
            let keys: Vec<u128> = heap.into_iter().map(|Reverse((_, k))| k).collect();
            (keys, floor)
        } else {
            let mut heap: BinaryHeap<(i64, u128)> = BinaryHeap::with_capacity(k + 1);
            for (&key, &gidx) in &compact_map {
                let val = read_val(gidx);
                heap.push((val, key));
                if heap.len() > k { heap.pop(); }
            }
            let floor = heap.peek().map(|&(v, _)| v).unwrap_or(0);
            let keys: Vec<u128> = heap.into_iter().map(|(_, k)| k).collect();
            (keys, floor)
        }
    });

    ParallelCompactResult {
        compact_map,
        compact_storage,
        segments_processed,
        rows_processed,
        decompress_us,
        topk,
    }
}

/// Parse a string value to a Datum for numeric types (pure Rust, no PG calls).
/// Used for segment_by values on worker threads.
fn parse_string_to_datum(s: &str, type_oid: pg_sys::Oid) -> pg_sys::Datum {
    match type_oid {
        pg_sys::INT2OID => {
            let v: i16 = s.parse().unwrap_or(0);
            pg_sys::Datum::from(v as usize)
        }
        pg_sys::INT4OID => {
            let v: i32 = s.parse().unwrap_or(0);
            pg_sys::Datum::from(v as usize)
        }
        pg_sys::INT8OID | pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID => {
            let v: i64 = s.parse().unwrap_or(0);
            pg_sys::Datum::from(v as usize)
        }
        pg_sys::FLOAT4OID => {
            let v: f32 = s.parse().unwrap_or(0.0);
            pg_sys::Datum::from(v.to_bits() as usize)
        }
        pg_sys::FLOAT8OID => {
            let v: f64 = s.parse().unwrap_or(0.0);
            pg_sys::Datum::from(v.to_bits() as usize)
        }
        pg_sys::DATEOID => {
            let v: i32 = s.parse().unwrap_or(0);
            pg_sys::Datum::from(v as usize)
        }
        pg_sys::BOOLOID => {
            let v = s == "t" || s == "true" || s == "1";
            pg_sys::Datum::from(v as usize)
        }
        _ => pg_sys::Datum::from(0usize),
    }
}

/// Merge a worker's compact map+storage into the global map+storage.
fn merge_compact_results(
    global_map: &mut CompactGroupMap,
    global_storage: &mut CompactAccStorage,
    worker_map: &CompactGroupMap,
    worker_storage: &CompactAccStorage,
    agg_specs: &[AggExecSpec],
) {
    for (&packed_key, &worker_group_idx) in worker_map {
        let global_group_idx = match global_map.entry(packed_key) {
            hashbrown::hash_map::Entry::Occupied(e) => *e.get(),
            hashbrown::hash_map::Entry::Vacant(e) => {
                let idx = global_storage.alloc_group();
                e.insert(idx);
                idx
            }
        };

        // Merge each accumulator slot
        for (slot_idx, _spec) in agg_specs.iter().enumerate() {
            let (_, kind) = global_storage.layout.slots[slot_idx];
            match kind {
                CompactAccKind::Count => unsafe {
                    let worker_count = worker_storage.read_count(worker_group_idx, slot_idx);
                    *global_storage.count_mut(global_group_idx, slot_idx) += worker_count;
                },
                CompactAccKind::SumInt => unsafe {
                    let (worker_sum, worker_count) = worker_storage.read_sum_int(worker_group_idx, slot_idx);
                    let (global_sum, global_count) = global_storage.sum_int_mut(global_group_idx, slot_idx);
                    *global_sum += worker_sum;
                    *global_count += worker_count;
                },
                CompactAccKind::SumIntNarrow => unsafe {
                    let (worker_sum, worker_count) = worker_storage.read_sum_int_narrow(worker_group_idx, slot_idx);
                    let (global_sum, global_count) = global_storage.sum_int_narrow_mut(global_group_idx, slot_idx);
                    *global_sum += worker_sum;
                    *global_count += worker_count;
                },
                CompactAccKind::SumFloat => unsafe {
                    let (worker_sum, worker_count) = worker_storage.read_sum_float(worker_group_idx, slot_idx);
                    let (global_sum, global_count) = global_storage.sum_float_mut(global_group_idx, slot_idx);
                    *global_sum += worker_sum;
                    *global_count += worker_count;
                },
                CompactAccKind::MinStr | CompactAccKind::MaxStr => unsafe {
                    let (w_off, w_len) = worker_storage.read_min_max_str(worker_group_idx, slot_idx);
                    if w_off != u32::MAX {
                        let w_str = worker_storage.str_arena.get(w_off, w_len);
                        let (g_off, g_len) = global_storage.read_min_max_str(global_group_idx, slot_idx);
                        let should_update = if g_off == u32::MAX {
                            true
                        } else {
                            let g_str = global_storage.str_arena.get(g_off, g_len);
                            let cmp = collation_strcmp(w_str, g_str);
                            match kind {
                                CompactAccKind::MinStr => cmp < 0,
                                CompactAccKind::MaxStr => cmp > 0,
                                _ => unreachable!(),
                            }
                        };
                        if should_update {
                            let w_str = worker_storage.str_arena.get(w_off, w_len);
                            let (new_off, new_len) = global_storage.str_arena.alloc(w_str);
                            global_storage.write_min_max_str(global_group_idx, slot_idx, new_off, new_len);
                        }
                    }
                },
            }
        }
    }
}

/// Check if all needed columns (for aggs, groups, and batch quals) are numeric.
fn all_needed_cols_numeric(
    needed_cols: &[bool],
    col_types: &[pg_sys::Oid],
) -> bool {
    needed_cols.iter().zip(col_types.iter()).all(|(&needed, &type_oid)| {
        !needed || is_numeric_type(type_oid)
    })
}

// ============================================================================
// Parallel Mixed (int + string) Aggregation
// ============================================================================

/// Compute a 128-bit hash of mixed integer and string group keys.
/// Uses two independent AHasher instances (different seeds) to produce two 64-bit
/// halves, giving collision probability ~2^-128.
fn hash_mixed_key(ints: &[i64], strs: &[Option<&str>]) -> u128 {
    use std::hash::BuildHasher;
    let s1 = ahash::RandomState::with_seeds(0xc4a1_b2e3_d4f5_6789, 0xa1b2_c3d4_e5f6_7890, 0x1122_3344_5566_7788, 0x99aa_bbcc_ddee_ff00);
    let s2 = ahash::RandomState::with_seeds(0x1234_abcd_5678_ef01, 0xaabb_ccdd_eeff_0011, 0xfed0_cba9_8765_4321, 0x0011_2233_4455_6677);
    let mut h1 = s1.build_hasher();
    let mut h2 = s2.build_hasher();
    for &v in ints {
        v.hash(&mut h1);
        v.hash(&mut h2);
    }
    for s in strs {
        match s {
            Some(s) => {
                0u8.hash(&mut h1); s.hash(&mut h1);
                0u8.hash(&mut h2); s.hash(&mut h2);
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
enum MixedKeyVal {
    Null,
    Int(i64),
    Str(u32, u32), // (offset, len) into arena
}

/// Per-worker side table mapping group_idx → actual key values.
/// Needed because the u128 hash is one-way — we need original values at finalization.
struct MixedKeyStorage {
    arena: StringArena,
    /// Flat storage: group i's key components are at keys[i * n_keys .. (i+1) * n_keys]
    keys: Vec<MixedKeyVal>,
    n_keys: usize,
}

impl MixedKeyStorage {
    fn new(n_keys: usize) -> Self {
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
    fn get(&self, group_idx: u32, col: usize) -> MixedKeyVal {
        self.keys[group_idx as usize * self.n_keys + col]
    }

}

/// Check if a GROUP BY column is a text type.
fn is_text_group_col(gs: &GroupByColSpec) -> bool {
    matches!(gs.expr, GroupByExpr::Column)
        && matches!(gs.type_oid,
            pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID | pg_sys::NAMEOID)
}

/// Configuration for parallel mixed aggregation (read-only, shared across threads).
struct ParallelMixedConfig<'a> {
    agg_specs: &'a [AggExecSpec],
    group_specs: &'a [GroupByColSpec],
    col_names: &'a [String],
    col_types: &'a [pg_sys::Oid],
    segment_by: &'a [String],
    needed_cols: &'a [bool],
    batch_quals: &'a [BatchQual],
    seg_filters: &'a [(usize, String)],
    time_min: Option<i64>,
    time_max: Option<i64>,
    topn_spec: Option<(usize, usize, bool)>,
    /// Which needed_cols indices are text GROUP BY columns
    text_group_col_flags: &'a [bool],
    /// Which needed_cols indices have text WHERE quals (EQ/NE/LIKE)
    text_qual_infos: &'a [TextQualInfo],
}

/// Pre-extracted text qual info for worker threads.
#[derive(Clone)]
enum TextQualInfo {
    EqNe { col_idx: usize, const_str: String, is_ne: bool },
    Like { col_idx: usize, strategy: super::batch_qual::LikeStrategy, negate: bool },
}

/// Result of parallel mixed aggregation from one worker thread.
struct ParallelMixedResult {
    compact_map: CompactGroupMap,
    compact_storage: CompactAccStorage,
    mixed_keys: MixedKeyStorage,
    segments_processed: u64,
    rows_processed: u64,
    decompress_us: u64,
    topk: Option<(Vec<u128>, i64)>,
}

/// Decompress a text column blob into a SegTextColumn (pure Rust, thread-safe).
fn decompress_text_to_seg_col(blob: &[u8]) -> Option<SegTextColumn> {
    if blob.is_empty() {
        return None;
    }
    let cc = compression::CompressedColumnRef::from_bytes(blob);
    let total = cc.row_count as usize;
    let nn_count = count_non_null(cc.null_bitmap, total);

    match cc.type_tag {
        compression::CompressionType::Dictionary
        | compression::CompressionType::DictionaryLz4 => {
            let norm_buf;
            let dict_data = if cc.type_tag == compression::CompressionType::DictionaryLz4 {
                norm_buf = compression::dictionary::normalize_lz4(cc.data);
                &norm_buf[..]
            } else {
                cc.data
            };
            let (dict_entries, nn_indices) =
                compression::dictionary::decode_dict_and_indices(dict_data, nn_count);
            let entries: Vec<String> = dict_entries.iter().map(|&s| s.to_string()).collect();

            let row_to_entry = if cc.null_bitmap.is_empty() {
                nn_indices.iter().map(|&idx| idx as u32).collect()
            } else {
                let mut re = Vec::with_capacity(total);
                let mut vi = 0;
                for i in 0..total {
                    let is_null = (cc.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
                    if is_null {
                        re.push(u32::MAX);
                    } else {
                        re.push(nn_indices[vi] as u32);
                        vi += 1;
                    }
                }
                re
            };
            Some(SegTextColumn::Dict { entries, row_to_entry })
        }
        compression::CompressionType::Lz4 | compression::CompressionType::Lz4Blocked => {
            let (buf, ranges) = if cc.type_tag == compression::CompressionType::Lz4 {
                compression::lz4::decode_to_ranges(cc.data, nn_count)
            } else {
                compression::lz4::decode_to_ranges_blocked(cc.data, nn_count, None)
            };

            let row_to_range = if cc.null_bitmap.is_empty() {
                ranges.iter().map(|&(off, len)| (off as u32, len as u16)).collect()
            } else {
                let mut rr = Vec::with_capacity(total);
                let mut vi = 0;
                for i in 0..total {
                    let is_null = (cc.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
                    if is_null {
                        rr.push((u32::MAX, 0u16));
                    } else {
                        let (off, len) = ranges[vi];
                        rr.push((off as u32, len as u16));
                        vi += 1;
                    }
                }
                rr
            };
            Some(SegTextColumn::Lz4 { buf, row_to_range })
        }
        _ => None,
    }
}

/// Apply a text EQ/NE filter to a SegTextColumn, producing a selection bitmap.
fn apply_text_eq_filter(seg_col: &SegTextColumn, const_str: &str, is_ne: bool, row_count: usize) -> Vec<bool> {
    let mut sel = Vec::with_capacity(row_count);
    for row in 0..row_count {
        let s = seg_col.get_str(row);
        let pass = match s {
            Some(s) => {
                let eq = s == const_str;
                if is_ne { !eq } else { eq }
            }
            None => false, // NULL doesn't pass
        };
        sel.push(pass);
    }
    sel
}

/// Apply a text LIKE filter to a SegTextColumn, producing a selection bitmap.
fn apply_text_like_filter(seg_col: &SegTextColumn, strategy: &super::batch_qual::LikeStrategy, negate: bool, row_count: usize) -> Vec<bool> {
    use super::batch_qual::{LikeStrategy, sql_like_match};

    let matches_like = |text: &str| -> bool {
        let matched = match strategy {
            LikeStrategy::Contains(s) => text.contains(s.as_str()),
            LikeStrategy::StartsWith(s) => text.starts_with(s.as_str()),
            LikeStrategy::EndsWith(s) => text.ends_with(s.as_str()),
            LikeStrategy::Exact(s) => text == s.as_str(),
            LikeStrategy::General(p) => sql_like_match(text, p),
        };
        if negate { !matched } else { matched }
    };

    let mut sel = Vec::with_capacity(row_count);
    for row in 0..row_count {
        let pass = match seg_col.get_str(row) {
            Some(s) => matches_like(s),
            None => false,
        };
        sel.push(pass);
    }
    sel
}

/// Check if a query can use the parallel mixed (int+string) aggregation path.
fn can_parallel_mixed(
    group_specs: &[GroupByColSpec],
    needed_cols: &[bool],
    col_types: &[pg_sys::Oid],
    batch_quals: &[BatchQual],
    agg_specs: &[AggExecSpec],
) -> bool {
    if group_specs.is_empty() || group_specs.len() > 4 {
        return false;
    }

    // Must have at least one text GROUP BY column (otherwise compact path would handle it)
    let has_text_group = group_specs.iter().any(is_text_group_col);
    if !has_text_group {
        return false;
    }

    // All GROUP BY columns must be either plain int columns or plain text columns
    for gs in group_specs {
        if is_text_group_col(gs) {
            continue; // text column — OK
        }
        // Must be an integer-producing expression
        match &gs.expr {
            GroupByExpr::Column => {
                let t = gs.type_oid;
                if !(t == pg_sys::INT2OID || t == pg_sys::INT4OID || t == pg_sys::INT8OID
                    || t == pg_sys::TIMESTAMPOID || t == pg_sys::TIMESTAMPTZOID)
                {
                    return false;
                }
            }
            GroupByExpr::DateTrunc { .. } | GroupByExpr::Extract { .. } | GroupByExpr::AddConst { .. } => {}
            GroupByExpr::RegexpReplace { .. } => return false, // not thread-safe
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
        let is_text_gb = group_specs.iter().any(|gs| gs.col_idx as usize == i && is_text_group_col(gs));
        let has_text_qual = batch_quals.iter().any(|bq| {
            bq.col_idx == i && (
                (matches!(bq.op, BatchCompareOp::Eq | BatchCompareOp::Ne) && bq.text_const.is_some())
                || (matches!(bq.op, BatchCompareOp::Like | BatchCompareOp::NotLike) && bq.like_strategy.is_some())
            )
        });
        let is_text_minmax_agg = agg_specs.iter().any(|s| {
            s.col_idx as usize == i
                && matches!(s.agg_type, AggType::Min | AggType::Max)
                && (type_oid == pg_sys::TEXTOID || type_oid == pg_sys::VARCHAROID || type_oid == pg_sys::BPCHAROID)
        });
        if !is_text_gb && !has_text_qual && !is_text_minmax_agg {
            return false; // unsupported column type
        }
    }

    // Text batch quals must be EQ/NE with text_const or LIKE/NotLike with like_strategy
    for bq in batch_quals {
        let t = bq.type_oid;
        if t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID {
            match bq.op {
                BatchCompareOp::Eq | BatchCompareOp::Ne => {
                    if bq.text_const.is_none() { return false; }
                }
                BatchCompareOp::Like | BatchCompareOp::NotLike => {
                    if bq.like_strategy.is_none() { return false; }
                }
                _ => return false, // unsupported text comparison
            }
        }
    }

    true
}

/// Process a chunk of segments on a worker thread using the mixed (int+string) path.
fn process_segments_mixed(
    segments: &[SegmentData],
    config: &ParallelMixedConfig,
) -> ParallelMixedResult {
    let mut compact_map = CompactGroupMap::with_hasher(BuildHasherDefault::default());
    let mut compact_storage = CompactAccStorage::new(CompactAccLayout::new(config.agg_specs));
    let num_group_keys = config.group_specs.len();
    let mut mixed_keys = MixedKeyStorage::new(num_group_keys);
    let mut segments_processed: u64 = 0;
    let mut rows_processed: u64 = 0;
    let mut decompress_us: u64 = 0;

    // Count int and str group keys
    let n_int_keys = config.group_specs.iter().filter(|gs| !is_text_group_col(gs)).count();
    let n_str_keys = config.group_specs.iter().filter(|gs| is_text_group_col(gs)).count();

    for seg in segments {
        if seg.row_count == 0 {
            continue;
        }

        // Segment-by pruning
        if !config.seg_filters.is_empty() {
            let mut skip = false;
            for &(seg_val_idx, ref filter_val) in config.seg_filters {
                match &seg.segment_values[seg_val_idx] {
                    Some(val) if val == filter_val => {}
                    _ => { skip = true; break; }
                }
            }
            if skip { continue; }
        }

        // Time-range pruning
        if let (Some(seg_min), Some(seg_max)) = (seg.min_time, seg.max_time) {
            if config.time_min.is_some_and(|query_min| seg_max < query_min) { continue; }
            if config.time_max.is_some_and(|query_max| seg_min > query_max) { continue; }
        }

        // Dictionary-based LIKE pruning
        if segment_skippable_by_dict_like(
            config.batch_quals, config.col_names, config.segment_by, &seg.compressed_blobs,
        ) {
            continue;
        }

        segments_processed += 1;

        // Decompress needed columns
        let t_dec = Instant::now();
        let mut numeric_cols: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
        let mut text_seg_cols: Vec<Option<SegTextColumn>> = Vec::new();
        let mut blob_idx = 0;
        let mut seg_val_idx = 0;

        for (col_idx, col_name) in config.col_names.iter().enumerate() {
            let type_oid = config.col_types[col_idx];

            if !config.needed_cols[col_idx] {
                if config.segment_by.contains(col_name) {
                    seg_val_idx += 1;
                } else {
                    blob_idx += 1;
                }
                numeric_cols.push(Vec::new());
                text_seg_cols.push(None);
                continue;
            }

            if config.segment_by.contains(col_name) {
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
            } else {
                let blob = &seg.compressed_blobs[blob_idx];
                if config.text_group_col_flags[col_idx] {
                    // Text GROUP BY column — decompress to SegTextColumn
                    text_seg_cols.push(decompress_text_to_seg_col(blob));
                    numeric_cols.push(Vec::new());
                } else if is_numeric_type(type_oid) {
                    // Numeric column
                    numeric_cols.push(decompress_numeric_blob(blob, type_oid));
                    text_seg_cols.push(None);
                } else {
                    // Text column needed only for WHERE qual (not GROUP BY)
                    text_seg_cols.push(decompress_text_to_seg_col(blob));
                    numeric_cols.push(Vec::new());
                }
                blob_idx += 1;
            }
        }
        decompress_us += t_dec.elapsed().as_micros() as u64;

        let row_count = seg.row_count as usize;

        // Build selection vector from quals
        // First: numeric batch quals
        let mut selection = evaluate_batch_quals(&numeric_cols, row_count, config.batch_quals, Vec::new());

        // Then: text quals (applied on SegTextColumn)
        for tqi in config.text_qual_infos {
            match tqi {
                TextQualInfo::EqNe { col_idx, const_str, is_ne } => {
                    if let Some(ref seg_col) = text_seg_cols[*col_idx] {
                        let text_sel = apply_text_eq_filter(seg_col, const_str, *is_ne, row_count);
                        if selection.is_empty() {
                            selection = text_sel;
                        } else {
                            for (s, ts) in selection.iter_mut().zip(text_sel.iter()) {
                                *s = *s && *ts;
                            }
                        }
                    }
                }
                TextQualInfo::Like { col_idx, strategy, negate } => {
                    if let Some(ref seg_col) = text_seg_cols[*col_idx] {
                        let text_sel = apply_text_like_filter(seg_col, strategy, *negate, row_count);
                        if selection.is_empty() {
                            selection = text_sel;
                        } else {
                            for (s, ts) in selection.iter_mut().zip(text_sel.iter()) {
                                *s = *s && *ts;
                            }
                        }
                    }
                }
            }
        }

        // Aggregation loop
        let mut int_keys = vec![0i64; n_int_keys];
        let mut str_keys: Vec<Option<&str>> = vec![None; n_str_keys];

        for row in 0..row_count {
            if !selection.is_empty() && !selection[row] {
                continue;
            }
            rows_processed += 1;

            // Build key components
            let mut has_null = false;
            let mut int_idx = 0;
            let mut str_idx = 0;
            for gs in config.group_specs.iter() {
                if is_text_group_col(gs) {
                    let col_idx = gs.col_idx as usize;
                    match &text_seg_cols[col_idx] {
                        Some(seg_col) => {
                            str_keys[str_idx] = seg_col.get_str(row);
                            // NULL text key → skip (like compact path)
                            if str_keys[str_idx].is_none() {
                                has_null = true;
                                break;
                            }
                        }
                        None => {
                            has_null = true;
                            break;
                        }
                    }
                    str_idx += 1;
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
                        GroupByExpr::Extract { unit, .. } => {
                            let pg_usec = col[row].0.value() as i64;
                            extract_field_from_usecs(pg_usec, unit)
                        }
                        GroupByExpr::AddConst { offset, .. } => {
                            col[row].0.value() as i64 + offset
                        }
                        GroupByExpr::Column => {
                            col[row].0.value() as i64
                        }
                        _ => unreachable!(),
                    };
                    int_idx += 1;
                }
            }

            if has_null { continue; }

            let hash_key = hash_mixed_key(&int_keys[..n_int_keys], &str_keys[..n_str_keys]);

            // Lookup or insert group
            if compact_map.len() == compact_map.capacity() {
                let cap = compact_map.capacity();
                if cap >= 32_000_000 {
                    compact_map.reserve(8_000_000);
                }
            }
            let group_idx = match compact_map.entry(hash_key) {
                hashbrown::hash_map::Entry::Occupied(e) => *e.get(),
                hashbrown::hash_map::Entry::Vacant(e) => {
                    let idx = compact_storage.alloc_group();
                    e.insert(idx);
                    // Store actual key values for this new group
                    mixed_keys.insert(&int_keys[..n_int_keys], &str_keys[..n_str_keys], config.group_specs);
                    idx
                }
            };

            // Update compact accumulators (same as compact path)
            for (spec_idx, spec) in config.agg_specs.iter().enumerate() {
                let (_, kind) = compact_storage.layout.slots[spec_idx];
                match kind {
                    CompactAccKind::Count => {
                        match spec.agg_type {
                            AggType::CountStar => {
                                unsafe { *compact_storage.count_mut(group_idx, spec_idx) += 1; }
                            }
                            AggType::Count => {
                                let col = &numeric_cols[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    unsafe { *compact_storage.count_mut(group_idx, spec_idx) += 1; }
                                } else {
                                    // Check text columns for COUNT(text_col)
                                    if let Some(ref seg_col) = text_seg_cols[spec.col_idx as usize]
                                        && seg_col.get_str(row).is_some()
                                    {
                                        unsafe { *compact_storage.count_mut(group_idx, spec_idx) += 1; }
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
                            let (sum, count) = unsafe { compact_storage.sum_int_mut(group_idx, spec_idx) };
                            if spec.expr_kind == AggExpr::AddConst {
                                *sum += v + spec.const_offset as i128;
                            } else {
                                *sum += v;
                            }
                            *count += 1;
                        }
                    }
                    CompactAccKind::SumIntNarrow => {
                        let col = &numeric_cols[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = col[row].0.value() as i64;
                            let (sum, count) = unsafe { compact_storage.sum_int_narrow_mut(group_idx, spec_idx) };
                            if spec.expr_kind == AggExpr::AddConst {
                                *sum += v + spec.const_offset;
                            } else {
                                *sum += v;
                            }
                            *count += 1;
                        }
                    }
                    CompactAccKind::SumFloat => {
                        let col = &numeric_cols[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = datum_to_f64(col[row].0, spec.col_type_oid);
                            let (sum, count) = unsafe { compact_storage.sum_float_mut(group_idx, spec_idx) };
                            if spec.expr_kind == AggExpr::AddConst {
                                *sum += v + spec.const_offset as f64;
                            } else {
                                *sum += v;
                            }
                            *count += 1;
                        }
                    }
                    CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                        let col_idx = spec.col_idx as usize;
                        if let Some(ref seg_col) = text_seg_cols[col_idx]
                            && let Some(s) = seg_col.get_str(row) {
                                let (cur_off, cur_len) = unsafe { compact_storage.read_min_max_str(group_idx, spec_idx) };
                                let should_update = if cur_off == u32::MAX {
                                    true // no current value
                                } else {
                                    let cur = compact_storage.str_arena.get(cur_off, cur_len);
                                    let cmp = s.as_bytes().cmp(cur.as_bytes()); // memcmp (thread-safe)
                                    match kind {
                                        CompactAccKind::MinStr => cmp == std::cmp::Ordering::Less,
                                        CompactAccKind::MaxStr => cmp == std::cmp::Ordering::Greater,
                                        _ => unreachable!(),
                                    }
                                };
                                if should_update {
                                    let (new_off, new_len) = compact_storage.str_arena.alloc(s);
                                    unsafe { compact_storage.write_min_max_str(group_idx, spec_idx, new_off, new_len); }
                                }
                        }
                    }
                }
            }
        }
    }

    // Compute top-K candidates while data is cache-hot (if requested)
    let topk = config.topn_spec.map(|(sort_slot, k, ascending)| {
        let (_, sort_kind) = compact_storage.layout.slots[sort_slot];
        let read_val = |gidx: u32| -> i64 {
            unsafe {
                match sort_kind {
                    CompactAccKind::Count => compact_storage.read_count(gidx, sort_slot),
                    CompactAccKind::SumIntNarrow => compact_storage.read_sum_int_narrow(gidx, sort_slot).0,
                    _ => compact_storage.read_count(gidx, sort_slot),
                }
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
                if heap.len() > k { heap.pop(); }
            }
            let floor = heap.peek().map(|&Reverse((v, _))| v).unwrap_or(0);
            let keys: Vec<u128> = heap.into_iter().map(|Reverse((_, k))| k).collect();
            (keys, floor)
        } else {
            let mut heap: BinaryHeap<(i64, u128)> = BinaryHeap::with_capacity(k + 1);
            for (&key, &gidx) in &compact_map {
                let val = read_val(gidx);
                heap.push((val, key));
                if heap.len() > k { heap.pop(); }
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
        segments_processed,
        rows_processed,
        decompress_us,
        topk,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use pgrx::pg_sys;

    /// Helper: build a pg_sys::List of integers from a slice.
    unsafe fn build_int_list(values: &[i32]) -> *mut pg_sys::List {
        unsafe {
            let mut list: *mut pg_sys::List = std::ptr::null_mut();
            for &v in values {
                list = pg_sys::lappend_int(list, v);
            }
            list
        }
    }

    // -------------------------------------------------------------------
    // parse_agg_private tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_parse_single_count_star() {
        // Layout: [oid=1234, sentinel=-1, num_aggs=1, (type=2(CountStar), col=-1, result_oid=0, col_type=0, expr=0)]
        unsafe {
            let list = build_int_list(&[
                1234, -1,        // companion OID + sentinel
                1,               // num_aggs
                2, -1, 0, 0, 0, // CountStar: type=2, col=-1, result_oid=0, col_type=0, expr=Column(0)
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.companion_oids.len(), 1);
            assert_eq!(u32::from(plan.companion_oids[0]), 1234);
            assert_eq!(plan.agg_specs.len(), 1);
            assert_eq!(plan.agg_specs[0].agg_type, AggType::CountStar);
            assert_eq!(plan.agg_specs[0].col_idx, -1);
            assert_eq!(plan.agg_specs[0].expr_kind, AggExpr::Column);
            assert!(plan.group_specs.is_empty());
            assert!(plan.output_map.len() == 1); // default: aggs then groups
            assert!(plan.having_filters.is_empty());
            assert!(plan.where_quals.is_null());
            assert_eq!(plan.topn_limit, 0);
        }
    }

    #[pg_test]
    fn test_parse_multiple_companion_oids() {
        unsafe {
            let list = build_int_list(&[
                100, 200, 300, -1, // three companion OIDs + sentinel
                0,                 // num_aggs = 0
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.companion_oids.len(), 3);
            assert_eq!(u32::from(plan.companion_oids[0]), 100);
            assert_eq!(u32::from(plan.companion_oids[1]), 200);
            assert_eq!(u32::from(plan.companion_oids[2]), 300);
        }
    }

    #[pg_test]
    fn test_parse_all_agg_types() {
        // Test that all 7 AggType variants parse correctly.
        // Each agg spec: (type, col_idx, result_oid, col_type_oid, expr_kind)
        unsafe {
            let list = build_int_list(&[
                42, -1,           // companion OID + sentinel
                7,                // num_aggs
                0, 0, 0, 23, 0,  // Sum, col=0, INT4OID=23
                1, 1, 0, 23, 0,  // Count, col=1
                2, -1, 0, 0, 0,  // CountStar
                3, 2, 0, 701, 0, // Avg, col=2, FLOAT8OID=701
                4, 3, 0, 25, 0,  // CountDistinct, col=3, TEXTOID=25
                5, 0, 0, 23, 0,  // Min, col=0
                6, 0, 0, 23, 0,  // Max, col=0
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.agg_specs.len(), 7);
            assert_eq!(plan.agg_specs[0].agg_type, AggType::Sum);
            assert_eq!(plan.agg_specs[1].agg_type, AggType::Count);
            assert_eq!(plan.agg_specs[2].agg_type, AggType::CountStar);
            assert_eq!(plan.agg_specs[3].agg_type, AggType::Avg);
            assert_eq!(plan.agg_specs[4].agg_type, AggType::CountDistinct);
            assert_eq!(plan.agg_specs[5].agg_type, AggType::Min);
            assert_eq!(plan.agg_specs[6].agg_type, AggType::Max);
        }
    }

    #[pg_test]
    fn test_parse_expr_kinds() {
        // Test LengthOf (expr=1) and AddConst (expr=2, followed by offset)
        unsafe {
            let list = build_int_list(&[
                42, -1,              // companion OID + sentinel
                3,                   // num_aggs
                0, 0, 0, 23, 0,     // Sum, Column (expr=0)
                0, 1, 0, 23, 1,     // Sum, LengthOf (expr=1)
                0, 2, 0, 23, 2, 77, // Sum, AddConst (expr=2), offset=77
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.agg_specs[0].expr_kind, AggExpr::Column);
            assert_eq!(plan.agg_specs[0].const_offset, 0);
            assert_eq!(plan.agg_specs[1].expr_kind, AggExpr::LengthOf);
            assert_eq!(plan.agg_specs[2].expr_kind, AggExpr::AddConst);
            assert_eq!(plan.agg_specs[2].const_offset, 77);
        }
    }

    #[pg_test]
    fn test_parse_group_by_column() {
        // GROUP BY col: expr_tag=0
        unsafe {
            let list = build_int_list(&[
                42, -1,          // companion OID + sentinel
                0,               // num_aggs = 0
                1,               // num_groups = 1
                3, 25, 0,       // col_idx=3, type_oid=TEXTOID(25), expr_tag=0(Column)
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.group_specs.len(), 1);
            assert_eq!(plan.group_specs[0].col_idx, 3);
            assert_eq!(u32::from(plan.group_specs[0].type_oid), 25);
            assert_eq!(plan.group_specs[0].expr, GroupByExpr::Column);
        }
    }

    #[pg_test]
    fn test_parse_group_by_date_trunc() {
        // GROUP BY date_trunc('hour', ts): expr_tag=2, func_oid, unit_len, unit_bytes...
        unsafe {
            let unit = b"hour";
            let mut vals = vec![
                42i32, -1,        // companion OID + sentinel
                0,                // num_aggs = 0
                1,                // num_groups = 1
                0, 1184, 2,      // col_idx=0, type_oid=TIMESTAMPTZOID(1184), expr_tag=2(DateTrunc)
                100,              // func_oid
                unit.len() as i32, // unit_len
            ];
            for &b in unit.iter() {
                vals.push(b as i32);
            }
            let list = build_int_list(&vals);

            let plan = parse_agg_private(list);
            assert_eq!(plan.group_specs.len(), 1);
            match &plan.group_specs[0].expr {
                GroupByExpr::DateTrunc { unit, unit_usecs, func_oid } => {
                    assert_eq!(unit, "hour");
                    assert_eq!(*unit_usecs, 3_600_000_000);
                    assert_eq!(*func_oid, 100);
                }
                other => panic!("expected DateTrunc, got {:?}", other),
            }
        }
    }

    #[pg_test]
    fn test_parse_group_by_extract() {
        // GROUP BY extract(minute FROM ts): expr_tag=3
        unsafe {
            let unit = b"minute";
            let mut vals = vec![
                42i32, -1,
                0,                  // num_aggs
                1,                  // num_groups
                0, 1184, 3,        // col_idx=0, TIMESTAMPTZOID, expr_tag=3(Extract)
                200,               // func_oid
                unit.len() as i32,
            ];
            for &b in unit.iter() {
                vals.push(b as i32);
            }
            let list = build_int_list(&vals);

            let plan = parse_agg_private(list);
            match &plan.group_specs[0].expr {
                GroupByExpr::Extract { unit, func_oid } => {
                    assert_eq!(unit, "minute");
                    assert_eq!(*func_oid, 200);
                }
                other => panic!("expected Extract, got {:?}", other),
            }
        }
    }

    #[pg_test]
    fn test_parse_group_by_add_const() {
        // GROUP BY col + 5: expr_tag=4, offset, op_oid
        unsafe {
            let list = build_int_list(&[
                42, -1,
                0,              // num_aggs
                1,              // num_groups
                2, 23, 4,      // col_idx=2, INT4OID, expr_tag=4(AddConst)
                5, 551,        // offset=5, op_oid=551
            ]);

            let plan = parse_agg_private(list);
            match &plan.group_specs[0].expr {
                GroupByExpr::AddConst { offset, op_oid } => {
                    assert_eq!(*offset, 5);
                    assert_eq!(*op_oid, 551);
                }
                other => panic!("expected AddConst, got {:?}", other),
            }
        }
    }

    #[pg_test]
    fn test_parse_group_by_regexp_replace() {
        // GROUP BY regexp_replace(col, 'pat', 'rep'): expr_tag=1
        unsafe {
            let pattern = b"abc";
            let replacement = b"xyz";
            let mut vals = vec![
                42i32, -1,
                0,                         // num_aggs
                1,                         // num_groups
                1, 25, 1,                  // col_idx=1, TEXTOID, expr_tag=1(RegexpReplace)
                300, 100,                  // func_oid=300, collation=100
                pattern.len() as i32,
            ];
            for &b in pattern.iter() { vals.push(b as i32); }
            vals.push(replacement.len() as i32);
            for &b in replacement.iter() { vals.push(b as i32); }
            let list = build_int_list(&vals);

            let plan = parse_agg_private(list);
            match &plan.group_specs[0].expr {
                GroupByExpr::RegexpReplace { pattern, replacement, func_oid, collation } => {
                    assert_eq!(pattern, "abc");
                    assert_eq!(replacement, "xyz");
                    assert_eq!(*func_oid, 300);
                    assert_eq!(*collation, 100);
                }
                other => panic!("expected RegexpReplace, got {:?}", other),
            }
        }
    }

    #[pg_test]
    fn test_parse_output_map() {
        // Explicit output map: [num_output=3, (type=1,ref=0), (type=0,ref=0), (type=0,ref=1)]
        // type=0 → Agg, type=1 → Group
        unsafe {
            let list = build_int_list(&[
                42, -1,           // companion OID + sentinel
                2,                // num_aggs
                2, -1, 0, 0, 0,  // CountStar
                0, 0, 0, 23, 0,  // Sum col=0
                1,                // num_groups
                0, 23, 0,        // col_idx=0, INT4OID, Column
                3,                // num_output = 3
                1, 0,            // Group(0)
                0, 0,            // Agg(0)
                0, 1,            // Agg(1)
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.output_map.len(), 3);
            assert!(matches!(plan.output_map[0], OutputEntry::Group(0)));
            assert!(matches!(plan.output_map[1], OutputEntry::Agg(0)));
            assert!(matches!(plan.output_map[2], OutputEntry::Agg(1)));
        }
    }

    #[pg_test]
    fn test_parse_default_output_map() {
        // When no output map is specified, defaults to aggs then groups.
        unsafe {
            let list = build_int_list(&[
                42, -1,
                1,                // num_aggs
                2, -1, 0, 0, 0,  // CountStar
                1,                // num_groups
                0, 23, 0,        // col_idx=0, INT4OID, Column
                0,                // num_output = 0 (triggers default)
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.output_map.len(), 2);
            assert!(matches!(plan.output_map[0], OutputEntry::Agg(0)));
            assert!(matches!(plan.output_map[1], OutputEntry::Group(0)));
        }
    }

    #[pg_test]
    fn test_parse_having_filters() {
        unsafe {
            let list = build_int_list(&[
                42, -1,
                1,                // num_aggs
                2, -1, 0, 0, 0,  // CountStar
                0,                // num_groups
                1,                // num_output
                0, 0,             // Agg(0)
                2,                // num_having = 2
                0, 0, 10,        // agg_idx=0, op=Gt(0), const=10
                0, 4, 100,       // agg_idx=0, op=Eq(4), const=100
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.having_filters.len(), 2);
            assert_eq!(plan.having_filters[0].agg_idx, 0);
            assert!(matches!(plan.having_filters[0].op, HavingOp::Gt));
            assert_eq!(plan.having_filters[0].const_val, 10);
            assert!(matches!(plan.having_filters[1].op, HavingOp::Eq));
            assert_eq!(plan.having_filters[1].const_val, 100);
        }
    }

    #[pg_test]
    fn test_parse_topn() {
        unsafe {
            let list = build_int_list(&[
                42, -1,
                1,                // num_aggs
                2, -1, 0, 0, 0,  // CountStar
                0,                // num_groups
                1,                // num_output
                0, 0,             // Agg(0)
                0,                // num_having
                0,                // where_str_len = 0 (no WHERE)
                25,               // topn_limit = 25
                2,                // topn_sort_col = 2
                0,                // topn_ascending = false
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.topn_limit, 25);
            assert_eq!(plan.topn_sort_col, 2);
            assert!(!plan.topn_ascending);
        }
    }

    #[pg_test]
    fn test_parse_all_having_ops() {
        // Verify all 6 HavingOp variants: Gt=0, Lt=1, Ge=2, Le=3, Eq=4, Ne=5
        unsafe {
            let list = build_int_list(&[
                42, -1,
                1, 2, -1, 0, 0, 0,  // 1 agg: CountStar
                0,                    // num_groups
                1, 0, 0,             // num_output=1, Agg(0)
                6,                    // num_having = 6
                0, 0, 1,  // Gt
                0, 1, 2,  // Lt
                0, 2, 3,  // Ge
                0, 3, 4,  // Le
                0, 4, 5,  // Eq
                0, 5, 6,  // Ne
            ]);

            let plan = parse_agg_private(list);
            assert_eq!(plan.having_filters.len(), 6);
            assert!(matches!(plan.having_filters[0].op, HavingOp::Gt));
            assert!(matches!(plan.having_filters[1].op, HavingOp::Lt));
            assert!(matches!(plan.having_filters[2].op, HavingOp::Ge));
            assert!(matches!(plan.having_filters[3].op, HavingOp::Le));
            assert!(matches!(plan.having_filters[4].op, HavingOp::Eq));
            assert!(matches!(plan.having_filters[5].op, HavingOp::Ne));
        }
    }

    // -------------------------------------------------------------------
    // Shared test helpers
    // -------------------------------------------------------------------

    use super::super::segments::{MetadataInfo, SegmentData, ColSum, ColMinMax};

    fn make_meta(col_names: &[&str]) -> MetadataInfo {
        MetadataInfo {
            col_names: col_names.iter().map(|s| s.to_string()).collect(),
            col_types: col_names.iter().map(|_| pg_sys::Oid::from(23u32)).collect(),
            col_typmods: col_names.iter().map(|_| -1).collect(),
            segment_by: Vec::new(),
            time_column: "ts".to_string(),
        }
    }

    fn make_plan(
        agg_specs: Vec<AggExecSpec>,
        group_specs: Vec<GroupByColSpec>,
        having: Vec<HavingFilter>,
        where_null: bool,
    ) -> ParsedAggPlan {
        let output_map: Vec<OutputEntry> = (0..agg_specs.len())
            .map(OutputEntry::Agg)
            .chain((0..group_specs.len()).map(OutputEntry::Group))
            .collect();
        ParsedAggPlan {
            companion_oids: vec![pg_sys::Oid::from(9999u32)],
            agg_specs,
            group_specs,
            output_map,
            having_filters: having,
            where_quals: if where_null { std::ptr::null_mut() } else {
                // Non-null placeholder (never dereferenced in rejection path)
                std::ptr::dangling_mut::<pg_sys::List>()
            },
            topn_limit: 0,
            topn_sort_col: 0,
            topn_ascending: true,
            bare_limit: 0,
        }
    }

    fn make_agg_spec(agg_type: AggType, col_idx: i32, col_type_oid: u32) -> AggExecSpec {
        AggExecSpec {
            agg_type,
            col_idx,
            col_type_oid: pg_sys::Oid::from(col_type_oid),
            expr_kind: AggExpr::Column,
            const_offset: 0,
        }
    }

    fn make_empty_segment(row_count: i32) -> SegmentData {
        SegmentData {
            segment_values: Vec::new(),
            compressed_blobs: Vec::new(),
            row_count,
            min_time: None,
            max_time: None,
            col_minmax: HashMap::new(),
            col_sums: HashMap::new(),
            toast_pointers: Vec::new(),
        }
    }

    // -------------------------------------------------------------------
    // try_catalog_shortcut tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_catalog_shortcut_rejects_group_by() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)],
            vec![GroupByColSpec { col_idx: 0, type_oid: pg_sys::Oid::from(23u32), expr: GroupByExpr::Column }],
            Vec::new(), true,
        );
        assert!(try_catalog_shortcut(&plan, &meta, &[], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_rejects_where() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::CountStar, -1, 23)], Vec::new(), Vec::new(), false);
        assert!(try_catalog_shortcut(&plan, &meta, &[], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_rejects_having() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::CountStar, -1, 23)], Vec::new(),
            vec![HavingFilter { agg_idx: 0, op: HavingOp::Gt, const_val: 10 }],
            true,
        );
        assert!(try_catalog_shortcut(&plan, &meta, &[], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_rejects_sum() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::Sum, 1, 23)], Vec::new(), Vec::new(), true);
        assert!(try_catalog_shortcut(&plan, &meta, &[Some(100)], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_rejects_avg() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::Avg, 1, 23)], Vec::new(), Vec::new(), true);
        assert!(try_catalog_shortcut(&plan, &meta, &[Some(100)], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_rejects_min_max() {
        let meta = make_meta(&["ts", "value"]);
        for agg_type in [AggType::Min, AggType::Max] {
            let plan = make_plan(vec![make_agg_spec(agg_type, 1, 23)], Vec::new(), Vec::new(), true);
            assert!(try_catalog_shortcut(&plan, &meta, &[Some(100)], 0).is_none());
        }
    }

    #[pg_test]
    fn test_catalog_shortcut_count_star_single_partition() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::CountStar, -1, 23)], Vec::new(), Vec::new(), true);
        let state = try_catalog_shortcut(&plan, &meta, &[Some(42_000)], 0).unwrap();
        assert_eq!(state.result_rows.len(), 1);
        assert_eq!(state.result_rows[0][0].0.value(), 42_000usize);
        assert!(!state.result_rows[0][0].1); // not null
    }

    #[pg_test]
    fn test_catalog_shortcut_count_star_multi_partition() {
        let meta = make_meta(&["ts", "value"]);
        let mut plan = make_plan(vec![make_agg_spec(AggType::CountStar, -1, 23)], Vec::new(), Vec::new(), true);
        plan.companion_oids = vec![
            pg_sys::Oid::from(1u32),
            pg_sys::Oid::from(2u32),
            pg_sys::Oid::from(3u32),
        ];
        let state = try_catalog_shortcut(
            &plan, &meta,
            &[Some(100), Some(200), Some(300)],
            0,
        ).unwrap();
        assert_eq!(state.result_rows[0][0].0.value(), 600usize);
    }

    #[pg_test]
    fn test_catalog_shortcut_count_star_missing_row_count() {
        // If any partition's row count is None, the shortcut fails
        let meta = make_meta(&["ts", "value"]);
        let mut plan = make_plan(vec![make_agg_spec(AggType::CountStar, -1, 23)], Vec::new(), Vec::new(), true);
        plan.companion_oids = vec![pg_sys::Oid::from(1u32), pg_sys::Oid::from(2u32)];
        assert!(try_catalog_shortcut(&plan, &meta, &[Some(100), None], 0).is_none());
    }

    #[pg_test]
    fn test_catalog_shortcut_count_distinct_falls_through() {
        // CountDistinct is no longer a catalog shortcut — always falls through
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::CountDistinct, 1, 23)], Vec::new(), Vec::new(), true);
        assert!(try_catalog_shortcut(&plan, &meta, &[Some(1000)], 0).is_none());
    }

    // -------------------------------------------------------------------
    // try_metadata_fast_path tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_metadata_fast_path_rejects_group_by() {
        let meta = make_meta(&["ts", "value"]);
        let mut plan = make_plan(vec![make_agg_spec(AggType::Sum, 1, 23)], Vec::new(), Vec::new(), true);
        plan.group_specs = vec![GroupByColSpec {
            col_idx: 0, type_oid: pg_sys::Oid::from(23u32), expr: GroupByExpr::Column,
        }];
        assert!(try_metadata_fast_path(&plan, &meta, &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_where() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::Sum, 1, 23)], Vec::new(), Vec::new(), false);
        assert!(try_metadata_fast_path(&plan, &meta, &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_having() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 23)], Vec::new(),
            vec![HavingFilter { agg_idx: 0, op: HavingOp::Gt, const_val: 5 }],
            true,
        );
        assert!(try_metadata_fast_path(&plan, &meta, &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_count_distinct() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::CountDistinct, 1, 23)], Vec::new(), Vec::new(), true);
        assert!(try_metadata_fast_path(&plan, &meta, &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_text_sum() {
        let meta = make_meta(&["ts", "name"]);
        let plan = make_plan(vec![make_agg_spec(AggType::Sum, 1, 25)], Vec::new(), Vec::new(), true); // TEXTOID=25
        assert!(try_metadata_fast_path(&plan, &meta, &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_length_of_sum() {
        let meta = make_meta(&["ts", "name"]);
        let mut plan = make_plan(vec![make_agg_spec(AggType::Sum, 1, 23)], Vec::new(), Vec::new(), true);
        plan.agg_specs[0].expr_kind = AggExpr::LengthOf;
        assert!(try_metadata_fast_path(&plan, &meta, &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_count_star_empty() {
        // COUNT(*) with no segments → 0
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::CountStar, -1, 23)], Vec::new(), Vec::new(), true);
        let state = try_metadata_fast_path(&plan, &meta, &[], 0, 0).unwrap();
        assert_eq!(state.result_rows.len(), 1);
        assert_eq!(state.result_rows[0][0].0.value(), 0usize);
    }

    #[pg_test]
    fn test_metadata_fast_path_count_star() {
        // COUNT(*) sums row_count across segments
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::CountStar, -1, 23)], Vec::new(), Vec::new(), true);
        let segs = vec![
            make_empty_segment(1000),
            make_empty_segment(2000),
            make_empty_segment(500),
        ];
        let state = try_metadata_fast_path(&plan, &meta, &segs, 0, 0).unwrap();
        assert_eq!(state.result_rows[0][0].0.value(), 3500usize);
    }

    #[pg_test]
    fn test_metadata_fast_path_count_star_skips_zero_rows() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::CountStar, -1, 23)], Vec::new(), Vec::new(), true);
        let segs = vec![
            make_empty_segment(100),
            make_empty_segment(0),  // should be skipped
            make_empty_segment(50),
        ];
        let state = try_metadata_fast_path(&plan, &meta, &segs, 0, 0).unwrap();
        assert_eq!(state.result_rows[0][0].0.value(), 150usize);
    }

    #[pg_test]
    fn test_metadata_fast_path_min_int() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::Min, 1, 20)], Vec::new(), Vec::new(), true); // INT8OID=20
        let mut seg1 = make_empty_segment(100);
        seg1.col_minmax.insert("value".to_string(), ColMinMax {
            min_datum: pg_sys::Datum::from(50i64 as usize),
            max_datum: pg_sys::Datum::from(200i64 as usize),
            min_null: false,
            max_null: false,
            type_oid: pg_sys::Oid::from(20u32),
        });
        let mut seg2 = make_empty_segment(100);
        seg2.col_minmax.insert("value".to_string(), ColMinMax {
            min_datum: pg_sys::Datum::from(10i64 as usize),
            max_datum: pg_sys::Datum::from(300i64 as usize),
            min_null: false,
            max_null: false,
            type_oid: pg_sys::Oid::from(20u32),
        });
        let state = try_metadata_fast_path(&plan, &meta, &[seg1, seg2], 0, 0).unwrap();
        let result = state.result_rows[0][0].0.value() as i64;
        assert_eq!(result, 10);
    }

    #[pg_test]
    fn test_metadata_fast_path_max_int() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::Max, 1, 20)], Vec::new(), Vec::new(), true);
        let mut seg1 = make_empty_segment(100);
        seg1.col_minmax.insert("value".to_string(), ColMinMax {
            min_datum: pg_sys::Datum::from(10i64 as usize),
            max_datum: pg_sys::Datum::from(200i64 as usize),
            min_null: false,
            max_null: false,
            type_oid: pg_sys::Oid::from(20u32),
        });
        let mut seg2 = make_empty_segment(100);
        seg2.col_minmax.insert("value".to_string(), ColMinMax {
            min_datum: pg_sys::Datum::from(5i64 as usize),
            max_datum: pg_sys::Datum::from(999i64 as usize),
            min_null: false,
            max_null: false,
            type_oid: pg_sys::Oid::from(20u32),
        });
        let state = try_metadata_fast_path(&plan, &meta, &[seg1, seg2], 0, 0).unwrap();
        let result = state.result_rows[0][0].0.value() as i64;
        assert_eq!(result, 999);
    }

    #[pg_test]
    fn test_metadata_fast_path_min_skips_null() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::Min, 1, 20)], Vec::new(), Vec::new(), true);
        let mut seg1 = make_empty_segment(100);
        seg1.col_minmax.insert("value".to_string(), ColMinMax {
            min_datum: pg_sys::Datum::from(0usize),
            max_datum: pg_sys::Datum::from(0usize),
            min_null: true,  // all nulls in this segment
            max_null: true,
            type_oid: pg_sys::Oid::from(20u32),
        });
        let mut seg2 = make_empty_segment(100);
        seg2.col_minmax.insert("value".to_string(), ColMinMax {
            min_datum: pg_sys::Datum::from(77i64 as usize),
            max_datum: pg_sys::Datum::from(77i64 as usize),
            min_null: false,
            max_null: false,
            type_oid: pg_sys::Oid::from(20u32),
        });
        let state = try_metadata_fast_path(&plan, &meta, &[seg1, seg2], 0, 0).unwrap();
        let result = state.result_rows[0][0].0.value() as i64;
        assert_eq!(result, 77);
    }

    #[pg_test]
    fn test_metadata_fast_path_missing_minmax_metadata() {
        // If a segment doesn't have minmax metadata for the needed column, fall through
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::Min, 1, 20)], Vec::new(), Vec::new(), true);
        let seg = make_empty_segment(100); // no col_minmax
        assert!(try_metadata_fast_path(&plan, &meta, &[seg], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_missing_sum_metadata() {
        // If a segment doesn't have sum metadata for a SUM column, fall through
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::Sum, 1, 20)], Vec::new(), Vec::new(), true);
        let seg = make_empty_segment(100); // no col_sums
        assert!(try_metadata_fast_path(&plan, &meta, &[seg], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_count_with_nonnull() {
        // COUNT(col) reads nonnull_count from ColSum
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::Count, 1, 20)], Vec::new(), Vec::new(), true);
        let mut seg1 = make_empty_segment(1000);
        seg1.col_sums.insert("value".to_string(), ColSum {
            sum_datum: pg_sys::Datum::from(0usize),
            sum_null: true,
            nonnull_count: 900,
            type_oid: pg_sys::Oid::from(1700u32),
        });
        let mut seg2 = make_empty_segment(500);
        seg2.col_sums.insert("value".to_string(), ColSum {
            sum_datum: pg_sys::Datum::from(0usize),
            sum_null: true,
            nonnull_count: 450,
            type_oid: pg_sys::Oid::from(1700u32),
        });
        let state = try_metadata_fast_path(&plan, &meta, &[seg1, seg2], 0, 0).unwrap();
        assert_eq!(state.result_rows[0][0].0.value() as i64, 1350);
    }

    #[pg_test]
    fn test_metadata_fast_path_sum_float() {
        // SUM on float column: reads sum_datum as f64 bits
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 701)], // FLOAT8OID=701
            Vec::new(), Vec::new(), true,
        );
        let sum_val: f64 = 123.5;
        let mut seg = make_empty_segment(100);
        seg.col_sums.insert("value".to_string(), ColSum {
            sum_datum: pg_sys::Datum::from(sum_val.to_bits() as usize),
            sum_null: false,
            nonnull_count: 100,
            type_oid: pg_sys::Oid::from(701u32),
        });
        let state = try_metadata_fast_path(&plan, &meta, &[seg], 0, 0).unwrap();
        let result_bits = state.result_rows[0][0].0.value() as u64;
        let result = f64::from_bits(result_bits);
        assert!((result - 123.5).abs() < 1e-10);
    }

    #[pg_test]
    fn test_metadata_fast_path_sum_int_via_numeric() {
        // SUM on integer column: sum_datum is a NUMERIC, needs PG numeric_out to parse.
        // Build a real NUMERIC datum via SPI.
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 20)], // INT8OID=20
            Vec::new(), Vec::new(), true,
        );
        // Create a real NUMERIC datum for the value 42000 via numeric_in
        let numeric_datum: pg_sys::Datum = unsafe {
            let s = std::ffi::CString::new("42000").unwrap();
            pg_sys::OidFunctionCall3Coll(
                pg_sys::Oid::from(1701u32), // numeric_in
                pg_sys::InvalidOid,
                pg_sys::Datum::from(s.as_ptr()),
                pg_sys::Datum::from(0usize),
                pg_sys::Datum::from(-1i32 as usize),
            )
        };
        let mut seg = make_empty_segment(100);
        seg.col_sums.insert("value".to_string(), ColSum {
            sum_datum: numeric_datum,
            sum_null: false,
            nonnull_count: 100,
            type_oid: pg_sys::Oid::from(1700u32),
        });
        let state = try_metadata_fast_path(&plan, &meta, &[seg], 0, 0).unwrap();
        // SumInt finalized: returns NUMERIC datum — verify via numeric_out
        let result_datum = state.result_rows[0][0].0;
        let s = unsafe {
            let cstr = pg_sys::OidOutputFunctionCall(pg_sys::Oid::from(1702u32), result_datum);
            let s = std::ffi::CStr::from_ptr(cstr).to_string_lossy().into_owned();
            pg_sys::pfree(cstr as *mut _);
            s
        };
        assert_eq!(s.as_str(), "42000");
    }

    #[pg_test]
    fn test_metadata_fast_path_sum_add_const() {
        // SUM(col + 10): should compute SUM(col) + 10 * nonnull_count
        let meta = make_meta(&["ts", "value"]);
        let mut spec = make_agg_spec(AggType::Sum, 1, 701); // FLOAT8OID
        spec.expr_kind = AggExpr::AddConst;
        spec.const_offset = 10;
        let plan = make_plan(vec![spec], Vec::new(), Vec::new(), true);
        let base_sum: f64 = 100.0;
        let mut seg = make_empty_segment(50);
        seg.col_sums.insert("value".to_string(), ColSum {
            sum_datum: pg_sys::Datum::from(base_sum.to_bits() as usize),
            sum_null: false,
            nonnull_count: 50,
            type_oid: pg_sys::Oid::from(701u32),
        });
        let state = try_metadata_fast_path(&plan, &meta, &[seg], 0, 0).unwrap();
        let result = f64::from_bits(state.result_rows[0][0].0.value() as u64);
        // Expected: 100.0 + 10 * 50 = 600.0
        assert!((result - 600.0).abs() < 1e-10);
    }

    #[pg_test]
    fn test_metadata_fast_path_reports_segment_count() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::CountStar, -1, 23)], Vec::new(), Vec::new(), true);
        let segs = vec![make_empty_segment(10), make_empty_segment(20)];
        let state = try_metadata_fast_path(&plan, &meta, &segs, 123, 456).unwrap();
        assert_eq!(state.total_segments, 2);
        assert_eq!(state.metadata_us, 123);
        assert_eq!(state.heap_scan_us, 456);
    }

    // -------------------------------------------------------------------
    // date_trunc_unit_to_usecs tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_date_trunc_microsecond() {
        assert_eq!(date_trunc_unit_to_usecs("microsecond"), 1);
        assert_eq!(date_trunc_unit_to_usecs("microseconds"), 1);
        assert_eq!(date_trunc_unit_to_usecs("us"), 1);
    }

    #[pg_test]
    fn test_date_trunc_millisecond() {
        assert_eq!(date_trunc_unit_to_usecs("millisecond"), 1_000);
        assert_eq!(date_trunc_unit_to_usecs("milliseconds"), 1_000);
        assert_eq!(date_trunc_unit_to_usecs("ms"), 1_000);
    }

    #[pg_test]
    fn test_date_trunc_second() {
        assert_eq!(date_trunc_unit_to_usecs("second"), 1_000_000);
        assert_eq!(date_trunc_unit_to_usecs("seconds"), 1_000_000);
    }

    #[pg_test]
    fn test_date_trunc_minute() {
        assert_eq!(date_trunc_unit_to_usecs("minute"), 60_000_000);
        assert_eq!(date_trunc_unit_to_usecs("minutes"), 60_000_000);
    }

    #[pg_test]
    fn test_date_trunc_hour() {
        assert_eq!(date_trunc_unit_to_usecs("hour"), 3_600_000_000);
        assert_eq!(date_trunc_unit_to_usecs("hours"), 3_600_000_000);
    }

    #[pg_test]
    fn test_date_trunc_day() {
        assert_eq!(date_trunc_unit_to_usecs("day"), 86_400_000_000);
        assert_eq!(date_trunc_unit_to_usecs("days"), 86_400_000_000);
    }

    #[pg_test]
    fn test_date_trunc_unknown_fallback() {
        assert_eq!(date_trunc_unit_to_usecs("week"), 1);
        assert_eq!(date_trunc_unit_to_usecs(""), 1);
    }

    // -------------------------------------------------------------------
    // extract_field_from_usecs tests
    // -------------------------------------------------------------------

    // Helper: PG epoch usecs for 2000-01-01 12:34:56.789012
    // 12h=43200s, 34m=2040s, 56s → total 45296s → 45296_789012 usec
    const SAMPLE_USEC: i64 = 45_296_789_012;

    #[pg_test]
    fn test_extract_microsecond() {
        // PG EXTRACT(microsecond FROM ...) returns seconds_within_minute * 1_000_000 + frac_usec
        // 56 seconds + 789012 usec = 56_789_012
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "microsecond"), 56_789_012);
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "microseconds"), 56_789_012);
    }

    #[pg_test]
    fn test_extract_millisecond() {
        // PG EXTRACT(millisecond FROM ...) returns seconds_within_minute * 1_000 + frac_ms
        // 56 seconds + 789 ms = 56_789
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "millisecond"), 56_789);
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "milliseconds"), 56_789);
    }

    #[pg_test]
    fn test_extract_second() {
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "second"), 56);
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "seconds"), 56);
    }

    #[pg_test]
    fn test_extract_minute() {
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "minute"), 34);
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "minutes"), 34);
    }

    #[pg_test]
    fn test_extract_hour() {
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "hour"), 12);
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "hours"), 12);
    }

    #[pg_test]
    fn test_extract_dow() {
        // PG epoch 2000-01-01 is a Saturday (dow=6), day 0
        assert_eq!(extract_field_from_usecs(0, "dow"), 6); // Saturday
        // Day 1 = Sunday=0
        assert_eq!(extract_field_from_usecs(86_400_000_000, "dow"), 0);
        // Day 2 = Monday=1
        assert_eq!(extract_field_from_usecs(2 * 86_400_000_000, "dow"), 1);
    }

    #[pg_test]
    fn test_extract_epoch() {
        // PG epoch offset to Unix = 946684800 seconds
        assert_eq!(extract_field_from_usecs(0, "epoch"), 946_684_800);
        // 1 second after PG epoch
        assert_eq!(extract_field_from_usecs(1_000_000, "epoch"), 946_684_801);
    }

    #[pg_test]
    fn test_extract_negative_usec() {
        // Before PG epoch (negative usec): 1999-12-31 23:59:59.000000
        // -1 second = -1_000_000 usec
        let usec = -1_000_000i64;
        assert_eq!(extract_field_from_usecs(usec, "second"), 59);
        assert_eq!(extract_field_from_usecs(usec, "minute"), 59);
        assert_eq!(extract_field_from_usecs(usec, "hour"), 23);
        // Day before PG epoch (1999-12-31) is Friday=5
        assert_eq!(extract_field_from_usecs(usec, "dow"), 5);
    }

    #[pg_test]
    fn test_extract_unknown_fallback() {
        assert_eq!(extract_field_from_usecs(SAMPLE_USEC, "year"), 0);
    }

    // -------------------------------------------------------------------
    // datum_to_i128 tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_datum_to_i128_int2() {
        let d = pg_sys::Datum::from(-5i16 as usize);
        assert_eq!(datum_to_i128(d, pg_sys::INT2OID), -5);
    }

    #[pg_test]
    fn test_datum_to_i128_int4() {
        let d = pg_sys::Datum::from(-100_000i32 as usize);
        assert_eq!(datum_to_i128(d, pg_sys::INT4OID), -100_000);
    }

    #[pg_test]
    fn test_datum_to_i128_int8() {
        let d = pg_sys::Datum::from(-9_000_000_000i64 as usize);
        assert_eq!(datum_to_i128(d, pg_sys::INT8OID), -9_000_000_000);
    }

    #[pg_test]
    fn test_datum_to_i128_unknown_oid() {
        // Falls through to raw usize cast
        let d = pg_sys::Datum::from(42usize);
        assert_eq!(datum_to_i128(d, pg_sys::Oid::from(9999u32)), 42);
    }

    // -------------------------------------------------------------------
    // datum_to_f64 tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_datum_to_f64_float4() {
        let f: f32 = 1.5;
        let d = pg_sys::Datum::from(f.to_bits() as usize);
        let result = datum_to_f64(d, pg_sys::FLOAT4OID);
        assert!((result - 1.5f64).abs() < 0.001);
    }

    #[pg_test]
    fn test_datum_to_f64_float8() {
        let f: f64 = 1.23456789;
        let d = pg_sys::Datum::from(f.to_bits() as usize);
        let result = datum_to_f64(d, pg_sys::FLOAT8OID);
        assert!((result - 1.23456789).abs() < 1e-9);
    }

    #[pg_test]
    fn test_datum_to_f64_unknown_oid() {
        let d = pg_sys::Datum::from(100usize);
        assert_eq!(datum_to_f64(d, pg_sys::Oid::from(9999u32)), 100.0);
    }

    // -------------------------------------------------------------------
    // AggAccumulator::new_for tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_accumulator_new_sum_int() {
        let acc = AggAccumulator::new_for(AggType::Sum, pg_sys::INT4OID);
        assert!(matches!(acc, AggAccumulator::SumInt { sum: 0, count: 0 }));
    }

    #[pg_test]
    fn test_accumulator_new_sum_float() {
        let acc = AggAccumulator::new_for(AggType::Sum, pg_sys::FLOAT8OID);
        assert!(matches!(acc, AggAccumulator::SumFloat { .. }));
    }

    #[pg_test]
    fn test_accumulator_new_avg_float4() {
        let acc = AggAccumulator::new_for(AggType::Avg, pg_sys::FLOAT4OID);
        assert!(matches!(acc, AggAccumulator::SumFloat { .. }));
    }

    #[pg_test]
    fn test_accumulator_new_avg_int() {
        let acc = AggAccumulator::new_for(AggType::Avg, pg_sys::INT8OID);
        assert!(matches!(acc, AggAccumulator::SumInt { .. }));
    }

    #[pg_test]
    fn test_accumulator_new_count() {
        let acc = AggAccumulator::new_for(AggType::Count, pg_sys::INT4OID);
        assert!(matches!(acc, AggAccumulator::Count { count: 0 }));
    }

    #[pg_test]
    fn test_accumulator_new_count_star() {
        let acc = AggAccumulator::new_for(AggType::CountStar, pg_sys::Oid::from(0u32));
        assert!(matches!(acc, AggAccumulator::Count { count: 0 }));
    }

    #[pg_test]
    fn test_accumulator_new_count_distinct_text() {
        let acc = AggAccumulator::new_for(AggType::CountDistinct, pg_sys::TEXTOID);
        assert!(matches!(acc, AggAccumulator::CountDistinctStr { .. }));
    }

    #[pg_test]
    fn test_accumulator_new_count_distinct_int() {
        let acc = AggAccumulator::new_for(AggType::CountDistinct, pg_sys::INT4OID);
        assert!(matches!(acc, AggAccumulator::CountDistinctInt { .. }));
    }

    #[pg_test]
    fn test_accumulator_new_min_text() {
        let acc = AggAccumulator::new_for(AggType::Min, pg_sys::TEXTOID);
        assert!(matches!(acc, AggAccumulator::MinStr { val: None }));
    }

    #[pg_test]
    fn test_accumulator_new_min_float() {
        let acc = AggAccumulator::new_for(AggType::Min, pg_sys::FLOAT8OID);
        assert!(matches!(acc, AggAccumulator::MinFloat { val: None }));
    }

    #[pg_test]
    fn test_accumulator_new_min_int() {
        let acc = AggAccumulator::new_for(AggType::Min, pg_sys::INT4OID);
        assert!(matches!(acc, AggAccumulator::MinInt { val: None }));
    }

    #[pg_test]
    fn test_accumulator_new_max_varchar() {
        let acc = AggAccumulator::new_for(AggType::Max, pg_sys::VARCHAROID);
        assert!(matches!(acc, AggAccumulator::MaxStr { val: None }));
    }

    #[pg_test]
    fn test_accumulator_new_max_float4() {
        let acc = AggAccumulator::new_for(AggType::Max, pg_sys::FLOAT4OID);
        assert!(matches!(acc, AggAccumulator::MaxFloat { val: None }));
    }

    #[pg_test]
    fn test_accumulator_new_max_int() {
        let acc = AggAccumulator::new_for(AggType::Max, pg_sys::INT8OID);
        assert!(matches!(acc, AggAccumulator::MaxInt { val: None }));
    }

    // -------------------------------------------------------------------
    // AggAccumulator::clone_fresh tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_clone_fresh_sum_int_resets() {
        let acc = AggAccumulator::SumInt { sum: 999, count: 50 };
        let fresh = acc.clone_fresh();
        assert!(matches!(fresh, AggAccumulator::SumInt { sum: 0, count: 0 }));
    }

    #[pg_test]
    fn test_clone_fresh_sum_float_resets() {
        let acc = AggAccumulator::SumFloat { sum: 1.5, count: 10 };
        let fresh = acc.clone_fresh();
        assert!(matches!(fresh, AggAccumulator::SumFloat { sum, count } if sum == 0.0 && count == 0));
    }

    #[pg_test]
    fn test_clone_fresh_count_resets() {
        let acc = AggAccumulator::Count { count: 42 };
        let fresh = acc.clone_fresh();
        assert!(matches!(fresh, AggAccumulator::Count { count: 0 }));
    }

    #[pg_test]
    fn test_clone_fresh_min_int_resets() {
        let acc = AggAccumulator::MinInt { val: Some(7) };
        let fresh = acc.clone_fresh();
        assert!(matches!(fresh, AggAccumulator::MinInt { val: None }));
    }

    #[pg_test]
    fn test_clone_fresh_count_distinct_int_resets() {
        let mut seen = std::collections::HashSet::new();
        seen.insert(1i64);
        seen.insert(2);
        let acc = AggAccumulator::CountDistinctInt { seen };
        let fresh = acc.clone_fresh();
        if let AggAccumulator::CountDistinctInt { seen } = fresh {
            assert!(seen.is_empty());
        } else {
            panic!("wrong variant");
        }
    }

    // -------------------------------------------------------------------
    // finalize_accumulator tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_finalize_count() {
        let acc = AggAccumulator::Count { count: 42 };
        let spec = make_agg_spec(AggType::Count, 0, 20);
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        assert_eq!(datum.value(), 42);
    }

    #[pg_test]
    fn test_finalize_count_distinct_int() {
        let mut seen = std::collections::HashSet::new();
        seen.insert(10i64);
        seen.insert(20);
        seen.insert(30);
        let acc = AggAccumulator::CountDistinctInt { seen };
        let spec = make_agg_spec(AggType::CountDistinct, 0, 20);
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        assert_eq!(datum.value(), 3);
    }

    #[pg_test]
    fn test_finalize_count_distinct_str() {
        let mut seen = std::collections::HashSet::new();
        seen.insert(0xdeadbeef_u128);
        seen.insert(0xcafebabe_u128);
        let acc = AggAccumulator::CountDistinctStr { seen };
        let spec = make_agg_spec(AggType::CountDistinct, 0, 25);
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        assert_eq!(datum.value(), 2);
    }

    #[pg_test]
    fn test_finalize_sum_int_zero_count_is_null() {
        let acc = AggAccumulator::SumInt { sum: 0, count: 0 };
        let spec = make_agg_spec(AggType::Sum, 0, 20);
        let (_, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(is_null);
    }

    #[pg_test]
    fn test_finalize_sum_int4_returns_int8() {
        // SUM(int4) → INT8 datum
        let acc = AggAccumulator::SumInt { sum: 100_000, count: 10 };
        let spec = make_agg_spec(AggType::Sum, 0, 23); // INT4OID
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        assert_eq!(datum.value() as i64, 100_000);
    }

    #[pg_test]
    fn test_finalize_sum_int8_returns_numeric() {
        // SUM(int8) → NUMERIC
        let acc = AggAccumulator::SumInt { sum: 999_999_999, count: 5 };
        let spec = make_agg_spec(AggType::Sum, 0, 20); // INT8OID
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        // Verify via numeric_out
        let s = unsafe {
            let cstr = pg_sys::OidOutputFunctionCall(pg_sys::Oid::from(1702u32), datum);
            let s = std::ffi::CStr::from_ptr(cstr).to_string_lossy().into_owned();
            pg_sys::pfree(cstr as *mut _);
            s
        };
        assert_eq!(s, "999999999");
    }

    #[pg_test]
    fn test_finalize_sum_float_zero_count_is_null() {
        let acc = AggAccumulator::SumFloat { sum: 0.0, count: 0 };
        let spec = make_agg_spec(AggType::Sum, 0, 701);
        let (_, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(is_null);
    }

    #[pg_test]
    fn test_finalize_sum_float8() {
        let acc = AggAccumulator::SumFloat { sum: 1.5, count: 1 };
        let spec = make_agg_spec(AggType::Sum, 0, 701); // FLOAT8OID
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        let result = f64::from_bits(datum.value() as u64);
        assert!((result - 1.5).abs() < 1e-10);
    }

    #[pg_test]
    fn test_finalize_sum_float4() {
        let acc = AggAccumulator::SumFloat { sum: 2.5, count: 1 };
        let spec = make_agg_spec(AggType::Sum, 0, 700); // FLOAT4OID
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        let result = f32::from_bits(datum.value() as u32);
        assert!((result - 2.5).abs() < 0.001);
    }

    #[pg_test]
    fn test_finalize_avg_int() {
        // AVG(int) → NUMERIC (sum/count via PG numeric_div)
        let acc = AggAccumulator::SumInt { sum: 100, count: 4 };
        let spec = make_agg_spec(AggType::Avg, 0, 23); // INT4OID
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        let s = unsafe {
            let cstr = pg_sys::OidOutputFunctionCall(pg_sys::Oid::from(1702u32), datum);
            let s = std::ffi::CStr::from_ptr(cstr).to_string_lossy().into_owned();
            pg_sys::pfree(cstr as *mut _);
            s
        };
        assert_eq!(s, "25.0000000000000000");
    }

    #[pg_test]
    fn test_finalize_avg_float() {
        // AVG(float8) → FLOAT8 (sum/count)
        let acc = AggAccumulator::SumFloat { sum: 10.0, count: 4 };
        let spec = make_agg_spec(AggType::Avg, 0, 701);
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        let result = f64::from_bits(datum.value() as u64);
        assert!((result - 2.5).abs() < 1e-10);
    }

    #[pg_test]
    fn test_finalize_min_int_some() {
        let acc = AggAccumulator::MinInt { val: Some(-42) };
        let spec = make_agg_spec(AggType::Min, 0, 20);
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        assert_eq!(datum.value() as i64, -42);
    }

    #[pg_test]
    fn test_finalize_min_int_none_is_null() {
        let acc = AggAccumulator::MinInt { val: None };
        let spec = make_agg_spec(AggType::Min, 0, 20);
        let (_, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(is_null);
    }

    #[pg_test]
    fn test_finalize_max_float_some() {
        let acc = AggAccumulator::MaxFloat { val: Some(99.9) };
        let spec = make_agg_spec(AggType::Max, 0, 701);
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        let result = f64::from_bits(datum.value() as u64);
        assert!((result - 99.9).abs() < 1e-10);
    }

    #[pg_test]
    fn test_finalize_max_float_none_is_null() {
        let acc = AggAccumulator::MaxFloat { val: None };
        let spec = make_agg_spec(AggType::Max, 0, 701);
        let (_, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(is_null);
    }

    // -------------------------------------------------------------------
    // StringArena tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_arena_alloc_and_get() {
        let mut arena = StringArena::new();
        let (off, len) = arena.alloc("hello");
        assert_eq!(arena.get(off, len), "hello");
    }

    #[pg_test]
    fn test_arena_multiple_allocs() {
        let mut arena = StringArena::new();
        let (o1, l1) = arena.alloc("foo");
        let (o2, l2) = arena.alloc("bar");
        let (o3, l3) = arena.alloc("baz");
        assert_eq!(arena.get(o1, l1), "foo");
        assert_eq!(arena.get(o2, l2), "bar");
        assert_eq!(arena.get(o3, l3), "baz");
    }

    #[pg_test]
    fn test_arena_empty_string() {
        let mut arena = StringArena::new();
        let (off, len) = arena.alloc("");
        assert_eq!(arena.get(off, len), "");
    }

    #[pg_test]
    fn test_arena_unicode() {
        let mut arena = StringArena::new();
        let (off, len) = arena.alloc("hello\u{00e9}world");
        assert_eq!(arena.get(off, len), "hello\u{00e9}world");
    }

    // -------------------------------------------------------------------
    // GroupKeyRef / GroupKeyVal / GroupKey tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_group_key_ref_resolve_null() {
        let mut arena = StringArena::new();
        let r = GroupKeyRef::Null;
        assert_eq!(r.resolve(&mut arena), GroupKeyVal::Null);
    }

    #[pg_test]
    fn test_group_key_ref_resolve_int() {
        let mut arena = StringArena::new();
        let r = GroupKeyRef::Int(42);
        assert_eq!(r.resolve(&mut arena), GroupKeyVal::Int(42));
    }

    #[pg_test]
    fn test_group_key_ref_resolve_str() {
        let mut arena = StringArena::new();
        let s = "hello";
        let r = GroupKeyRef::from_str(s);
        let val = r.resolve(&mut arena);
        if let GroupKeyVal::Str(off, len) = val {
            assert_eq!(arena.get(off, len), "hello");
        } else {
            panic!("expected Str variant");
        }
    }

    #[pg_test]
    fn test_group_key_ref_matches_owned_null() {
        let arena = StringArena::new();
        assert!(GroupKeyRef::Null.matches_owned(&GroupKeyVal::Null, &arena));
        assert!(!GroupKeyRef::Null.matches_owned(&GroupKeyVal::Int(0), &arena));
    }

    #[pg_test]
    fn test_group_key_ref_matches_owned_int() {
        let arena = StringArena::new();
        assert!(GroupKeyRef::Int(5).matches_owned(&GroupKeyVal::Int(5), &arena));
        assert!(!GroupKeyRef::Int(5).matches_owned(&GroupKeyVal::Int(6), &arena));
        assert!(!GroupKeyRef::Int(5).matches_owned(&GroupKeyVal::Null, &arena));
    }

    #[pg_test]
    fn test_group_key_ref_matches_owned_str() {
        let mut arena = StringArena::new();
        let (off, len) = arena.alloc("test");
        let s = "test";
        let r = GroupKeyRef::from_str(s);
        assert!(r.matches_owned(&GroupKeyVal::Str(off, len), &arena));
        let s2 = "other";
        let r2 = GroupKeyRef::from_str(s2);
        assert!(!r2.matches_owned(&GroupKeyVal::Str(off, len), &arena));
    }

    #[pg_test]
    fn test_group_key_single_as_slice() {
        let key = GroupKey::Single(GroupKeyVal::Int(7));
        assert_eq!(key.as_slice().len(), 1);
        assert_eq!(key.as_slice()[0], GroupKeyVal::Int(7));
    }

    #[pg_test]
    fn test_group_key_multi_as_slice() {
        let key = GroupKey::Multi(vec![GroupKeyVal::Int(1), GroupKeyVal::Null].into_boxed_slice());
        assert_eq!(key.as_slice().len(), 2);
        assert_eq!(key.as_slice()[0], GroupKeyVal::Int(1));
        assert_eq!(key.as_slice()[1], GroupKeyVal::Null);
    }

    // -------------------------------------------------------------------
    // hash_group_key / hash_group_key_ref / keys_match tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_hash_consistency_int() {
        // hash_group_key and hash_group_key_ref must produce the same hash
        // for equivalent keys
        let mut arena = StringArena::new();
        let owned = GroupKey::Single(GroupKeyVal::Int(42));
        let borrowed = [GroupKeyRef::Int(42)];
        let _ = &mut arena; // arena unused for int keys, but needed for API
        assert_eq!(hash_group_key(&owned, &arena), hash_group_key_ref(&borrowed));
    }

    #[pg_test]
    fn test_hash_consistency_null() {
        let arena = StringArena::new();
        let owned = GroupKey::Single(GroupKeyVal::Null);
        let borrowed = [GroupKeyRef::Null];
        assert_eq!(hash_group_key(&owned, &arena), hash_group_key_ref(&borrowed));
    }

    #[pg_test]
    fn test_hash_consistency_str() {
        let mut arena = StringArena::new();
        let (off, len) = arena.alloc("hello");
        let owned = GroupKey::Single(GroupKeyVal::Str(off, len));
        let s = "hello";
        let borrowed = [GroupKeyRef::from_str(s)];
        assert_eq!(hash_group_key(&owned, &arena), hash_group_key_ref(&borrowed));
    }

    #[pg_test]
    fn test_hash_consistency_multi() {
        let mut arena = StringArena::new();
        let (off, len) = arena.alloc("world");
        let owned = GroupKey::Multi(
            vec![GroupKeyVal::Int(1), GroupKeyVal::Str(off, len), GroupKeyVal::Null]
                .into_boxed_slice(),
        );
        let s = "world";
        let borrowed = [GroupKeyRef::Int(1), GroupKeyRef::from_str(s), GroupKeyRef::Null];
        assert_eq!(hash_group_key(&owned, &arena), hash_group_key_ref(&borrowed));
    }

    #[pg_test]
    fn test_hash_different_values_differ() {
        let arena = StringArena::new();
        let k1 = GroupKey::Single(GroupKeyVal::Int(1));
        let k2 = GroupKey::Single(GroupKeyVal::Int(2));
        assert_ne!(hash_group_key(&k1, &arena), hash_group_key(&k2, &arena));
    }

    #[pg_test]
    fn test_hash_different_types_differ() {
        // Int(0) vs Null should hash differently (different discriminant)
        let arena = StringArena::new();
        let k1 = GroupKey::Single(GroupKeyVal::Int(0));
        let k2 = GroupKey::Single(GroupKeyVal::Null);
        assert_ne!(hash_group_key(&k1, &arena), hash_group_key(&k2, &arena));
    }

    #[pg_test]
    fn test_keys_match_single_int() {
        let arena = StringArena::new();
        let owned = GroupKey::Single(GroupKeyVal::Int(42));
        let temp = [GroupKeyRef::Int(42)];
        assert!(keys_match(&owned, &temp, &arena));
    }

    #[pg_test]
    fn test_keys_match_single_mismatch() {
        let arena = StringArena::new();
        let owned = GroupKey::Single(GroupKeyVal::Int(42));
        let temp = [GroupKeyRef::Int(99)];
        assert!(!keys_match(&owned, &temp, &arena));
    }

    #[pg_test]
    fn test_keys_match_length_mismatch() {
        let arena = StringArena::new();
        let owned = GroupKey::Single(GroupKeyVal::Int(42));
        let temp = [GroupKeyRef::Int(42), GroupKeyRef::Null];
        assert!(!keys_match(&owned, &temp, &arena));
    }

    #[pg_test]
    fn test_keys_match_multi_str() {
        let mut arena = StringArena::new();
        let (o1, l1) = arena.alloc("abc");
        let (o2, l2) = arena.alloc("xyz");
        let owned = GroupKey::Multi(
            vec![GroupKeyVal::Str(o1, l1), GroupKeyVal::Str(o2, l2)].into_boxed_slice(),
        );
        let s1 = "abc";
        let s2 = "xyz";
        let temp = [GroupKeyRef::from_str(s1), GroupKeyRef::from_str(s2)];
        assert!(keys_match(&owned, &temp, &arena));
    }

    #[pg_test]
    fn test_keys_match_multi_str_mismatch() {
        let mut arena = StringArena::new();
        let (o1, l1) = arena.alloc("abc");
        let (o2, l2) = arena.alloc("xyz");
        let owned = GroupKey::Multi(
            vec![GroupKeyVal::Str(o1, l1), GroupKeyVal::Str(o2, l2)].into_boxed_slice(),
        );
        let s1 = "abc";
        let s2 = "DIFFERENT";
        let temp = [GroupKeyRef::from_str(s1), GroupKeyRef::from_str(s2)];
        assert!(!keys_match(&owned, &temp, &arena));
    }
}
