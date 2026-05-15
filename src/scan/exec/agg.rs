use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::pg_guard;

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use regex::Regex;

use crate::compression;
use crate::compress::{decode_i64_to_f64, decode_i64_to_f32};
use super::super::SyncStatic;
use super::batch_qual::{BatchCompareOp, BatchQual,
    extract_batch_quals, evaluate_batch_quals,
    apply_batch_filter_i64, apply_batch_filter_i32, apply_batch_filter_i16,
    apply_batch_filter_f64, apply_batch_filter_f32, apply_batch_filter_in_list};
use super::datum_utils::{
    decompress_blob_to_datums, decompress_text_blob_to_raw_strings,
    decompress_text_blob_to_lengths, decompress_text_blob_with_like_filter,
    decompress_text_blob_with_eq_filter, decompress_text_blob_with_in_filter,
    string_to_datum, pg_type_name,
    count_non_null, collation_strcmp,
};
use super::segments::{
    SegmentData, load_metadata, load_segments_heap,
    segment_skippable_by_dict, extract_segment_filters,
    classify_segment_quals, classify_segment_quals_numeric, SegmentQualResult, is_zero_const,
    detoast_lazy_blobs,
};
use super::text_col::{SegTextColumn, TextQualInfo, decompress_length_sidecar, decompress_text_to_seg_col, apply_text_eq_filter, apply_text_in_filter, apply_text_like_filter, strcoll_cmp};

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

/// Decode a colstats-encoded i64 to the PG-native i64 representation.
///
/// For timestamps, converts Unix-epoch usec → PG-epoch usec.
/// For dates, converts Unix-epoch usec → PG-epoch days.
/// For plain integers, identity.
fn decode_encoded_to_pg_i64(encoded: i64, type_oid: pg_sys::Oid) -> i64 {
    match type_oid {
        pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID => {
            encoded - crate::compress::PG_EPOCH_OFFSET_USEC
        }
        pg_sys::DATEOID => {
            (encoded / 86_400_000_000) - crate::compress::PG_EPOCH_OFFSET_DAYS
        }
        _ => encoded,
    }
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

/// H.2: post-storage transform applied at finalize / partial-emit time.
///
/// MIN/MAX are linear in the input, so a monotonic affine transform on the
/// argument can be lifted to a transform on the result without changing the
/// argmin/argmax. We exploit this for JSONBench Q3/Q4's
/// `MIN(<const_timestamptz> + INTERVAL <unit> * <bigint>)` shape: store the
/// raw bigint (`time_us`), pick the min, then shift by `delta` at emit time
/// to recover the timestamptz value PG expects.
///
/// `None` is the identity (no shift); existing code paths default to it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum OutputTransform {
    None,
    /// Final emitted value = `stored_i64 + delta` (wrapping i64 add).
    /// Stored accumulator is i64 microseconds; emitted Datum is reinterpreted
    /// as TIMESTAMPTZOID (PG's internal representation is i64 µs from 2000-01-01).
    /// `delta` is precomputed by the recognizer in `hook.rs` from the
    /// constant epoch + interval coefficient.
    PgUsShift { delta: i64 },
}

/// `hashbrown` HashSet with ahash — the insert hot path for
/// COUNT(DISTINCT) accumulators. Swapped from `std::collections::HashSet`
/// (SipHash) for ~2-3× faster inserts; the serial CD merge on Q4 goes
/// from ~2.5 s to ~1 s as a result.
type CdSetInt = hashbrown::HashSet<i64, BuildHasherDefault<ahash::AHasher>>;
type CdSetStr = hashbrown::HashSet<u128, BuildHasherDefault<ahash::AHasher>>;

#[inline]
fn new_cd_set_int() -> CdSetInt {
    CdSetInt::with_hasher(BuildHasherDefault::default())
}

#[inline]
fn new_cd_set_str() -> CdSetStr {
    CdSetStr::with_hasher(BuildHasherDefault::default())
}

enum AggAccumulator {
    SumInt { sum: i128, count: i64 },
    SumFloat { sum: f64, count: i64 },
    Count { count: i64 },
    CountDistinctInt { seen: CdSetInt },
    /// Stores SipHash-128 digests of strings instead of owned Strings.
    /// Bounded memory (16 bytes per distinct value) — same approach as ClickHouse's uniqExact.
    CountDistinctStr { seen: CdSetStr },
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
                    AggAccumulator::CountDistinctStr { seen: new_cd_set_str() }
                } else {
                    AggAccumulator::CountDistinctInt { seen: new_cd_set_int() }
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
            AggAccumulator::CountDistinctInt { .. } => AggAccumulator::CountDistinctInt { seen: new_cd_set_int() },
            AggAccumulator::CountDistinctStr { .. } => AggAccumulator::CountDistinctStr { seen: new_cd_set_str() },
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
    /// Phase C.2 activation: when true, exec emits PG's partial-aggregate
    /// transition state (see `transtype_oid`) instead of the final value;
    /// a Final Aggregate node above DeltaXAgg combines partials via the
    /// aggregate's combinefn. Wired by C.2.f's planner construction.
    /// Default false → existing complete-aggregate behaviour.
    #[allow(dead_code)] // wired by C.2 activation in path.rs
    pub(crate) is_partial: bool,
    /// Aggregate's `aggtranstype` from `pg_aggregate.dat` — only meaningful
    /// when `is_partial = true`. For COUNT/SUM(int4) this is INT8;
    /// for SUM(int8) / AVG it's INTERNAL (serialized via aggserialfn);
    /// for MIN/MAX it's the column type. `InvalidOid` when `is_partial =
    /// false`.
    #[allow(dead_code)] // wired by C.2 activation in path.rs
    pub(crate) transtype_oid: pg_sys::Oid,
    /// H.2: monotonic transform applied at finalize / partial-emit. Default
    /// `None` for all existing call sites; recognizer in `hook.rs` sets
    /// `PgUsShift { delta }` for the timestamptz_pl_interval Aggref shape.
    pub(crate) output_transform: OutputTransform,
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
    /// extract(field FROM timestamp_col): GROUP BY extract(minute FROM ts).
    ///
    /// When `divisor == 0`, the input column at `col_idx` is a TIMESTAMP /
    /// TIMESTAMPTZ — read as i64 microseconds since the PG epoch
    /// (2000-01-01) and passed to `extract_field_from_usecs`.
    ///
    /// When `divisor > 0`, the input column is a BIGINT carrying microseconds
    /// since the unix epoch (1970-01-01) — typically a json_extract synthetic
    /// recovered from `(data->>'time_us')::bigint`. The recognizer matches
    /// `extract(unit FROM to_timestamp(<bigint_col> / <const>))`; `divisor`
    /// is the SQL-level divisor (e.g. 1_000_000 for `time_us / 1000000`),
    /// applied at evaluation time to recover unix seconds. Restricted to
    /// period-86400-invariant units (sub-day fields), where the unix-vs-PG
    /// epoch shift drops out of the answer; calendar-based fields fall back
    /// to the executor.
    Extract { unit: String, func_oid: u32, divisor: i64 },
    /// col +/- const: GROUP BY col - 1  (offset is always stored as addition, so col-1 → offset=-1)
    AddConst { offset: i64, op_oid: u32 },
    /// CASE WHEN ... THEN ... ELSE ... END
    CaseWhen(CaseWhenSpec),
}

/// Comparison operator for CASE WHEN conditions.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(i32)]
pub(crate) enum CaseWhenOp { Eq = 0, NotEq = 1 }

/// A single condition: col op const_val
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CaseWhenCondition {
    pub(crate) col_idx: usize,
    pub(crate) op: CaseWhenOp,
    pub(crate) const_val: i64,
}

/// The value produced by a THEN or ELSE branch.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum CaseWhenValue {
    ColumnRef(usize),
    StringConst(String),
}

/// A single WHEN clause: conditions (AND-combined) → result.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CaseWhenClause {
    pub(crate) conditions: Vec<CaseWhenCondition>,
    pub(crate) result: CaseWhenValue,
}

/// Full CASE WHEN spec: clauses evaluated in order, default is ELSE branch.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CaseWhenSpec {
    pub(crate) clauses: Vec<CaseWhenClause>,
    pub(crate) default: CaseWhenValue,
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

/// Evaluate a `GroupByExpr::Extract` on a single column-row value. When
/// `divisor == 0` the input is interpreted as PG-usec (existing path); when
/// `divisor > 0` the input is interpreted as bigint unix microseconds via
/// `extract_subday_from_bigint_scaled` below. Centralised so the five
/// per-row extract sites in this file stay one-line and consistent.
#[inline]
pub(crate) fn eval_extract(value: i64, divisor: i64, unit: &str) -> i64 {
    if divisor > 0 {
        extract_subday_from_bigint_scaled(value, divisor, unit)
    } else {
        extract_field_from_usecs(value, unit)
    }
}

/// If segment min/max prove an EXTRACT() group key is constant across the
/// segment, return that key. This lets the hot mixed-aggregate path avoid
/// decompressing a time column just to recompute the same bucket for every row.
fn constant_extract_key_for_segment(
    cm: &super::segments::ColMinMax,
    divisor: i64,
    unit: &str,
) -> Option<i64> {
    if cm.min_null || cm.max_null {
        return None;
    }
    let min_value = if divisor > 0 {
        cm.min_encoded
    } else {
        decode_encoded_to_pg_i64(cm.min_encoded, cm.type_oid)
    };
    let max_value = if divisor > 0 {
        cm.max_encoded
    } else {
        decode_encoded_to_pg_i64(cm.max_encoded, cm.type_oid)
    };
    let min_key = eval_extract(min_value, divisor, unit);
    let max_key = eval_extract(max_value, divisor, unit);
    if min_key != max_key {
        return None;
    }

    let bucket_width = match unit {
        "second" | "seconds" => 1_000_000,
        "minute" | "minutes" => 60_000_000,
        "hour" | "hours" => 3_600_000_000,
        _ => return None,
    };
    let (min_bucket, max_bucket) = if divisor > 0 {
        let width = (bucket_width / 1_000_000_i64).saturating_mul(divisor);
        if width <= 0 {
            return None;
        }
        (min_value.div_euclid(width), max_value.div_euclid(width))
    } else {
        (
            min_value.div_euclid(bucket_width),
            max_value.div_euclid(bucket_width),
        )
    };
    (min_bucket == max_bucket).then_some(min_key)
}

/// Extract a sub-day field from a BIGINT column whose value, when divided
/// by `divisor`, yields seconds since the unix epoch (1970-01-01). Used for
/// the `extract(unit FROM to_timestamp(bigint_col / divisor))` shape — the
/// recognizer in `hook.rs` only emits this for units whose value depends
/// only on `unix_secs % 86400` (microsecond/millisecond/second/minute/hour),
/// so the unix-vs-PG epoch shift (a multiple of 86400) drops out and we
/// don't need to convert to PG-usec first.
///
/// `divisor` must be positive; the recognizer enforces this.
fn extract_subday_from_bigint_scaled(value: i64, divisor: i64, unit: &str) -> i64 {
    let unix_secs = value.div_euclid(divisor);
    let secs_in_day = unix_secs.rem_euclid(86_400);
    match unit {
        "microsecond" | "microseconds" => {
            // Whole-second arithmetic only: `to_timestamp(bigint / divisor)`
            // already truncated below the second, so any sub-second component
            // of the original bigint is lost. The recognizer accepts the unit
            // anyway because the answer remains exact for divisors that are
            // multiples of 1_000_000 (the only shape we've seen in practice).
            (secs_in_day % 60) * 1_000_000
        }
        "millisecond" | "milliseconds" => {
            (secs_in_day % 60) * 1_000
        }
        "second" | "seconds" => {
            secs_in_day % 60
        }
        "minute" | "minutes" => {
            (secs_in_day / 60) % 60
        }
        "hour" | "hours" => {
            secs_in_day / 3_600
        }
        _ => 0, // recognizer rejects other units in the divisor>0 path
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
    pub(crate) detoast_us: u64,
    pub(crate) decompress_us: u64,
    pub(crate) agg_us: u64,
    /// Blob cache hits accumulated across all `detoast_lazy_blobs` calls
    /// for this scan. Surfaced in EXPLAIN as `DeltaX Blob Cache`.
    pub(crate) blob_cache_hits: u64,
    pub(crate) blob_cache_misses: u64,
    pub(crate) blob_cache_bytes_served: u64,
    pub(crate) total_segments: u64,
    pub(crate) total_rows_processed: u64,
    pub(crate) batch_quals_count: usize,
    pub(crate) where_quals_null: bool,
    pub(crate) segments_metadata_resolved: u64,
    pub(crate) segments_decompressed: u64,
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
    pub(crate) wall_us: u64,
    pub(crate) buf_stats: super::segments::ScanBufferStats,
    /// F8 (`PERF_IMPROVEMENTS.md` #44): number of preselected keys used to
    /// filter Phase-1 rows. 0 when the optimization didn't fire.
    pub(crate) f8_preselected: u64,
    /// Parallel-DeltaXAgg shared state (Phase C). Null when running serially
    /// (no Gather above us). Set by `initialize_dsm_deltax_agg` on the
    /// leader and by `init_worker_deltax_agg` on each worker.
    #[allow(dead_code)] // Phase C.0 scaffold; consumers added in C.1+.
    pub(crate) pscan: *mut DeltaXAggPState,
    /// True for parallel workers; the worker's `begin_agg_scan` short-
    /// circuits past SPI + heap scan + accumulator construction and the
    /// leader handles result emission. False on the leader and on serial
    /// scans.
    #[allow(dead_code)] // Phase C.0 scaffold; consumers added in C.1+.
    pub(crate) is_parallel_worker: bool,
    /// Phase C.2.c — deferred parallel-exec state. `Some` when the planner
    /// chose the parallel-aware path (`plan.parallel_aware == true`); `None`
    /// for serial / internal-rayon paths. The leader populates it in
    /// `begin_agg_scan`'s parallel branch from already-loaded metadata +
    /// segments + extracted quals; workers populate it in
    /// `init_worker_deltax_agg` via re-SPI (V2 follow-up will share via
    /// DSM).
    #[allow(dead_code)] // Phase C.2.c scaffold; consumers added in C.2.d/e.
    pub(crate) exec_ctx: Option<Box<AggExecContext>>,
}


/// Phase C.2.c — bundle of state the parallel-aware DeltaXAgg path passes from
/// `begin_agg_scan` (which loads metadata + segments + extracts quals) to
/// `exec_agg_scan` (which claims segments via the DSM cursor, aggregates,
/// merges, finalises). In the serial / internal-rayon path this is unused —
/// the locals stay inside `begin_agg_scan` because the whole aggregation runs
/// there.
///
/// The eligibility predicate in `add_agg_path` (C.2.f) excludes COUNT(DISTINCT),
/// HAVING, Top-N pushdown, LIMIT, regex GROUP BY, and non-numeric paths, so
/// this struct only tracks state the compact path actually uses.
#[allow(dead_code)] // wired by C.2.d / C.2.e
pub(crate) struct AggExecContext {
    pub(super) meta: super::segments::MetadataInfo,
    /// Segments loaded by the leader once. V1 workers re-load via SPI per
    /// PARALLEL_AGG.md; V2 follow-up will share the leader's list via DSM.
    pub(super) all_segments: Vec<SegmentData>,
    pub(super) agg_specs: Vec<AggExecSpec>,
    pub(super) group_specs: Vec<GroupByColSpec>,
    pub(super) output_map: Vec<OutputEntry>,
    pub(super) needed_cols: Vec<bool>,
    pub(super) batch_quals: Vec<BatchQual>,
    pub(super) seg_filters: Vec<(usize, String)>,
    pub(super) time_min: Option<i64>,
    pub(super) time_max: Option<i64>,
    pub(super) topn_spec: Option<(usize, usize, bool)>,
    pub(super) num_result_cols: usize,
    /// True after the leader has merged worker partials into `result_rows`.
    /// Reset by `rescan_agg_scan`.
    pub(super) merged: bool,
    /// True after a worker has serialised its partial into the slab.
    /// Reset by `rescan_agg_scan`. Unused on the leader.
    pub(super) worker_done: bool,
    /// Phase C.2 activation: when true, this CustomScan emits per-group
    /// partial-aggregate transition states (via `compact_emit_partial`) for
    /// a Final Aggregate above to combine. Default false for the existing
    /// complete-aggregate path.
    #[allow(dead_code)] // wired by C.2 activation
    pub(super) is_partial: bool,
}

// ============================================================================
// Parallel-aware DSM scaffolding (Phase B of the parallel-DeltaXAgg plan).
//
// The full design lives in `dev/docs/JSON_EXTRACT.md` follow-up section /
// the matching plan file. Phase B lays only the type + hook surface so a
// future commit can flip `parallel_workers > 0` without further
// CustomExecMethods churn. With `parallel_workers = 0` (current state in
// `path.rs::add_agg_path`) PG never invokes these callbacks, so the stubs
// stay dormant until Phase C wires real worker work in.
//
// `DeltaXAggPState` mirrors `DeltaXAppendPState` (`scan/exec/decompress.rs`)
// — same `next_segment` cursor + per-worker timing slots. Phase C will
// extend with `partial_offsets` / `partial_caps` / `partial_lens` describing
// each worker's reserved slab in the DSM partial-state region.
// ============================================================================

/// Max combined leader+worker slots we track per scan. Matches
/// `super::decompress::MAX_WORKER_SLOTS`; both must agree because Phase C
/// shares the same per-process slot computation helper.
#[allow(dead_code)] // Phase B scaffolding; activated in Phase C.
pub(crate) const MAX_AGG_WORKER_SLOTS: usize =
    super::decompress::MAX_WORKER_SLOTS;

/// Per-worker timing counters in the DSM region. Phase C populates this
/// during `shutdown_deltax_agg`; the leader aggregates for EXPLAIN. Default
/// (zeros) is a valid "this slot was never populated" signal.
#[allow(dead_code)] // Phase B scaffolding; activated in Phase C.
#[repr(C)]
#[derive(Copy, Clone, Default)]
pub(crate) struct AggTimingShmem {
    pub(crate) populated: u32,
    pub(crate) _pad: u32,
    pub(crate) segments_decompressed: u64,
    pub(crate) rows_in: u64,
    pub(crate) rows_filtered_qual: u64,
    pub(crate) groups_emitted_local: u64,
    pub(crate) hash_probe_us: u64,
    pub(crate) accum_update_us: u64,
    pub(crate) distinct_union_us: u64,
    pub(crate) partial_serialize_us: u64,
}

/// Per-worker partial-result slab size in bytes. Conservative default —
/// fits ~1M groups for typical accumulator widths. Phase F adds a GUC
/// (`pg_deltax.parallel_agg_partial_state_mb`) and tuplestore spill on
/// overflow. Today, overflow erroes out cleanly via `serialize_partial_into`.
pub(crate) const PARTIAL_SLAB_SIZE_BYTES: usize = 32 * 1024 * 1024;

/// Shared DSM state for parallel `DeltaXAgg`. POD; zero-initialised state is
/// the empty "no segments claimed yet" condition. The full DSM region is
/// `[DeltaXAggPState][slab 0 (leader)][slab 1 (worker 0)]…` — each slab is
/// `PARTIAL_SLAB_SIZE_BYTES` long and at offset
/// `size_of::<DeltaXAggPState>() + slot_idx * PARTIAL_SLAB_SIZE_BYTES`.
///
/// Synchronisation contract: workers serialise their `ParallelCompactResult`
/// into their slab, write `partial_lens[k]` (the byte count actually written),
/// then set `worker_timings[k].populated = 1` with `Release`. The leader
/// spin-waits on `populated` reads with `Acquire`, then deserialises the
/// first `partial_lens[k]` bytes of slot k's slab. Skipping the
/// Release/Acquire pair on `populated` is undefined behaviour.
#[allow(dead_code)] // Phase B scaffolding; partial_lens used in C.2.
#[repr(C)]
pub(crate) struct DeltaXAggPState {
    /// Workers `fetch_add(1)` to claim the next segment index.
    pub(crate) next_segment: AtomicU64,
    /// Total segments the leader pre-loaded; set in `initialize_dsm_deltax_agg`.
    pub(crate) total_segments: u64,
    /// Number of timing slots populated (leader + nworkers).
    pub(crate) n_worker_slots: u32,
    /// Per-slab byte capacity; mirrors `PARTIAL_SLAB_SIZE_BYTES` so workers
    /// don't have to reach into a const.
    pub(crate) partial_slab_size: u32,
    /// Bytes actually written into each slab by the corresponding process.
    /// `partial_lens[k]` is set BEFORE `worker_timings[k].populated = 1`
    /// (with Release ordering) so the leader's Acquire read on `populated`
    /// makes the slab contents visible.
    pub(crate) partial_lens: [AtomicU64; MAX_AGG_WORKER_SLOTS],
    /// Per-process timing aggregation. Slot 0 = leader, 1..=N = workers.
    pub(crate) worker_timings: [AggTimingShmem; MAX_AGG_WORKER_SLOTS],
}

impl DeltaXAggPState {
    /// Pointer to slot `slot_idx`'s slab. Caller must ensure the DSM
    /// region was sized for this slot count.
    #[allow(dead_code)] // Phase C.2.
    #[inline]
    pub(crate) unsafe fn slab_ptr(&self, slot_idx: usize) -> *mut u8 {
        unsafe {
            let base = self as *const _ as *const u8;
            base.add(std::mem::size_of::<DeltaXAggPState>())
                .add(slot_idx * PARTIAL_SLAB_SIZE_BYTES) as *mut u8
        }
    }
}

/// EstimateDSMCustomScan: bytes for `DeltaXAggPState` + N+1 partial-state
/// slabs (one per leader/worker). The slab count is fixed at the cap so
/// re-sizing isn't needed if the planner picks a smaller worker count
/// later — wasted DSM bytes in that case are bounded by
/// `PARTIAL_SLAB_SIZE_BYTES * (MAX_AGG_WORKER_SLOTS - 1 - nworkers)`.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn estimate_dsm_deltax_agg(
    _node: *mut pg_sys::CustomScanState,
    pcxt: *mut pg_sys::ParallelContext,
) -> pg_sys::Size {
    unsafe {
        let nworkers = (*pcxt).nworkers as usize;
        let nslots = (nworkers + 1).min(MAX_AGG_WORKER_SLOTS);
        (std::mem::size_of::<DeltaXAggPState>() + nslots * PARTIAL_SLAB_SIZE_BYTES)
            as pg_sys::Size
    }
}

/// InitializeDSMCustomScan: leader populates the shared region after its own
/// `BeginCustomScan` has run. `coordinate` is a `DeltaXAggPState` carved out
/// of PG's parallel-context DSM segment.
///
/// Phase C.1 wires the cursor + slot count. Phase C.2 will set
/// `total_segments` from the leader's `segments_data` and slot the per-
/// worker partial-result region offsets.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn initialize_dsm_deltax_agg(
    node: *mut pg_sys::CustomScanState,
    pcxt: *mut pg_sys::ParallelContext,
    coordinate: *mut std::ffi::c_void,
) {
    unsafe {
        let ps = coordinate as *mut DeltaXAggPState;
        std::ptr::write_bytes(ps as *mut u8, 0, std::mem::size_of::<DeltaXAggPState>());

        let nworkers = (*pcxt).nworkers as usize;
        if nworkers + 1 > MAX_AGG_WORKER_SLOTS {
            pgrx::error!(
                "pg_deltax: parallel worker count {} exceeds MAX_AGG_WORKER_SLOTS {}",
                nworkers,
                MAX_AGG_WORKER_SLOTS - 1,
            );
        }

        // `next_segment` is zero from `write_bytes`; AtomicU64 has the same
        // memory representation as `u64` so that's a valid initial state.
        (*ps).total_segments = 0;
        (*ps).n_worker_slots = (nworkers + 1) as u32;
        (*ps).partial_slab_size = PARTIAL_SLAB_SIZE_BYTES as u32;
        // `partial_lens` is zero-initialised by `write_bytes`; AtomicU64
        // shares layout with u64.

        let state_ptr = (*node).custom_ps as *mut AggScanState;
        if !state_ptr.is_null() {
            (*state_ptr).pscan = ps;
            // Phase C.2.c: leader's `begin_agg_scan` parallel branch already
            // populated `exec_ctx.all_segments`; publish the segment count
            // to the cursor range so workers' `next_segment.fetch_add` knows
            // when to stop. If exec_ctx is None (planner chose
            // parallel_aware but begin's parallel branch didn't fire — a
            // bug), workers will see total_segments == 0 and exit cleanly.
            if let Some(ref ctx) = (*state_ptr).exec_ctx {
                (*ps).total_segments = ctx.all_segments.len() as u64;
            }
        }
    }
}

/// ReInitializeDSMCustomScan: reset the shared cursor and per-slot timing
/// before a rescan. The leader is the only caller.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn reinit_dsm_deltax_agg(
    _node: *mut pg_sys::CustomScanState,
    _pcxt: *mut pg_sys::ParallelContext,
    coordinate: *mut std::ffi::c_void,
) {
    unsafe {
        let ps = coordinate as *mut DeltaXAggPState;
        (*ps).next_segment.store(0, Ordering::Relaxed);
        for slot in (*ps).worker_timings.iter_mut() {
            *slot = AggTimingShmem::default();
        }
        for len in (*ps).partial_lens.iter_mut() {
            len.store(0, Ordering::Relaxed);
        }
    }
}

/// InitializeWorkerCustomScan: worker attaches DSM, re-runs the leader's
/// metadata + segment + qual prelude, and stashes the result on the
/// worker-side `AggScanState.exec_ctx` so `exec_agg_scan`'s claim loop
/// (Phase C.2.d) can drive segment fetches from the shared cursor.
///
/// V1 hydrates via SPI (per PARALLEL_AGG.md C.2.c "re-SPI for now"); V2
/// follow-up will share leader-loaded segments via DSM (mirroring
/// `append_wire`).
///
/// SPI is legal inside `InitializeWorkerCustomScan` because PG opens a
/// transaction in each worker before calling our hooks.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn init_worker_deltax_agg(
    node: *mut pg_sys::CustomScanState,
    _toc: *mut pg_sys::shm_toc,
    coordinate: *mut std::ffi::c_void,
) {
    unsafe {
        // The worker's `begin_agg_scan` short-circuit installed a minimal
        // stub state — replace it now with a deferred state populated by
        // re-SPI'd metadata + segments. Mirror of decompress.rs's
        // `init_worker_deltax_append`.
        let cscan = (*node).ss.ps.plan as *mut pg_sys::CustomScan;
        let custom_private = (*cscan).custom_private;
        if custom_private.is_null() {
            pgrx::error!("pg_deltax: parallel DeltaXAgg worker has no custom_private");
        }
        let plan = parse_agg_private(custom_private);
        if plan.companion_oids.is_empty() {
            pgrx::error!("pg_deltax: parallel DeltaXAgg worker has no companion tables");
        }

        // The leader extracted batch_quals from `where_quals`; reproduce on
        // the worker and clear `ps.qual` if all quals are batch-handled, so
        // PG doesn't re-evaluate them after `exec_agg_scan` returns. Mirrors
        // decompress.rs's qual-clear pattern.
        let plan_qual = (*(*node).ss.ps.plan).qual;

        super::segments::reset_scan_buf_stats();
        let ctx = build_agg_exec_context_from_plan(plan);

        // If batch_quals fully cover the planner's qual list, clear it so
        // PG's executor won't double-evaluate after exec emits virtual rows.
        if !plan_qual.is_null()
            && !ctx.batch_quals.is_empty()
            && ctx.batch_quals.len() as i32 == (*plan_qual).length
        {
            (*node).ss.ps.qual = std::ptr::null_mut();
        }

        let mut state = build_deferred_agg_state(ctx, /* is_worker */ true);
        state.pscan = coordinate as *mut DeltaXAggPState;

        let state_ptr = Box::into_raw(Box::new(state));
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}

/// ShutdownCustomScan: each process records its worker-slot timing into
/// `worker_timings[slot]` while DSM is still attached, so the leader can
/// aggregate per-process numbers for EXPLAIN.
///
/// Phase C.1 just stamps `populated = 1` so the leader sees that this slot
/// participated. Phase C.2 will serialise the process's local
/// `ParallelCompactResult` into the slot's slab and copy timing.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn shutdown_deltax_agg(
    node: *mut pg_sys::CustomScanState,
) {
    unsafe {
        let state_ptr = (*node).custom_ps as *mut AggScanState;
        if state_ptr.is_null() {
            return;
        }
        let state = &mut *state_ptr;
        if state.pscan.is_null() {
            return;
        }
        let slot_idx = current_agg_worker_slot();
        let ps = &mut *state.pscan;
        if slot_idx >= ps.n_worker_slots as usize {
            return;
        }
        let slot = &mut ps.worker_timings[slot_idx];
        slot.populated = 1;
    }
}

/// Returns the DSM slot index for the current process. Slot 0 is the
/// leader; worker N (0-indexed in PG) gets slot N+1.
#[allow(dead_code)] // Phase C.1; widely used in C.2.
unsafe fn current_agg_worker_slot() -> usize {
    unsafe {
        let n = pg_sys::ParallelWorkerNumber;
        if n < 0 {
            0
        } else {
            ((n as usize) + 1).min(MAX_AGG_WORKER_SLOTS - 1)
        }
    }
}

/// Allocate a minimal `AggScanState` for parallel-worker processes that
/// short-circuit past the leader's heavy SPI + heap-scan + accumulator
/// construction. The worker won't emit rows (its `exec_agg_scan` returns
/// EOF immediately); Phase C.2 will replace this with an actual segment-
/// claim + partial-aggregate loop driving real `ParallelCompactResult`
/// state on the worker.
#[allow(dead_code)] // Phase C.1; activated by `begin_agg_scan` short-circuit.
fn build_minimal_worker_state() -> AggScanState {
    AggScanState {
        _agg_specs: Vec::new(),
        _group_specs: Vec::new(),
        result_rows: Vec::new(),
        result_idx: 0,
        _num_result_cols: 0,
        metadata_us: 0,
        heap_scan_us: 0,
        detoast_us: 0,
        blob_cache_hits: 0,
        blob_cache_misses: 0,
        blob_cache_bytes_served: 0,
        decompress_us: 0,
        agg_us: 0,
        total_segments: 0,
        total_rows_processed: 0,
        batch_quals_count: 0,
        where_quals_null: true,
        segments_metadata_resolved: 0,
        segments_decompressed: 0,
        regex_cache_size: 0,
        regex_cache_calls: 0,
        topn_limit: 0,
        topn_sort_col: -1,
        topn_ascending: true,
        pre_topn_groups: 0,
        merge_us: 0,
        finalize_us: 0,
        topn_select_us: 0,
        n_workers: 0,
        bare_limit: 0,
        wall_us: 0,
        buf_stats: super::segments::ScanBufferStats::default(),
        f8_preselected: 0,
        pscan: std::ptr::null_mut(),
        is_parallel_worker: true,
        exec_ctx: None,
    }
}

/// Build an `AggExecContext` for the parallel-aware path. Used by both the
/// leader (in `begin_agg_scan`'s parallel branch) and workers (in
/// `init_worker_deltax_agg`). V1 workers re-SPI metadata + re-load segments;
/// V2 follow-up will share the leader's hydration via DSM (mirroring
/// `append_wire`).
///
/// SAFETY: Calls into `load_agg_metadata_from_plan` + `load_segments_heap`
/// which use SPI; caller must be inside a PG transaction (which is true for
/// any `BeginCustomScan` / `InitializeWorkerCustomScan` callback).
#[allow(dead_code)] // wired by C.2.d / C.2.e
unsafe fn build_agg_exec_context_from_plan(plan: ParsedAggPlan) -> AggExecContext {
    unsafe {
        let (meta, _metadata_us) = load_agg_metadata_from_plan(&plan.companion_oids);

        let ParsedAggPlan {
            companion_oids,
            agg_specs,
            group_specs,
            output_map,
            having_filters: _,
            where_quals,
            topn_limit,
            topn_sort_col,
            topn_ascending,
            derived_minmax_topn: _,
            bare_limit: _,
            is_partial,
        } = plan;

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

        let (batch_quals, _handled) = extract_batch_quals(
            where_quals,
            &meta.col_names,
            &meta.col_types,
        );
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

        let mut all_segments: Vec<SegmentData> = Vec::new();
        for &oid in &companion_oids {
            let (segs, _, _, _, _, _) = load_segments_heap(
                oid,
                &meta.col_names,
                &meta.segment_by,
                &needed_cols,
                &meta.time_column,
                /* load_minmax */ false,
                &seg_filters,
                time_min,
                time_max,
                /* lazy_cols */ None,
                &batch_quals,
                /* needed_stats_cols */ &[],
                &meta.col_types,
                /* needed_minmax_cols */ &[],
                /* skip_text */ false,
            );
            all_segments.extend(segs);
        }

        // Speculative top-K is excluded from the parallel-aware path's
        // eligibility predicate (Top-N pushdown is gated off in C.2.f), so
        // `topn_spec` remains `None` here. Plumbed through for forward-
        // compat once Top-N joins parallel.
        let topn_spec = if topn_limit > 0 && !output_map.is_empty() {
            match output_map[topn_sort_col] {
                OutputEntry::Agg(ai) if agg_specs[ai].agg_type != AggType::Avg => {
                    let k = (topn_limit as usize).max(1000);
                    Some((ai, k, topn_ascending))
                }
                _ => None,
            }
        } else {
            None
        };

        let num_result_cols = output_map.len();

        AggExecContext {
            meta,
            all_segments,
            agg_specs,
            group_specs,
            output_map,
            needed_cols,
            batch_quals,
            seg_filters,
            time_min,
            time_max,
            topn_spec,
            num_result_cols,
            merged: false,
            worker_done: false,
            is_partial,
        }
    }
}

/// Construct a deferred-exec `AggScanState` for the parallel-aware path:
/// empty `result_rows`, populated `exec_ctx`, all timers zero. Workers and
/// the leader use this as their initial state — the actual claim+merge work
/// runs in `exec_agg_scan`.
#[allow(dead_code)] // wired by C.2.d / C.2.e
fn build_deferred_agg_state(ctx: AggExecContext, is_worker: bool) -> AggScanState {
    AggScanState {
        _agg_specs: Vec::new(),
        _group_specs: Vec::new(),
        result_rows: Vec::new(),
        result_idx: 0,
        _num_result_cols: ctx.num_result_cols,
        metadata_us: 0,
        heap_scan_us: 0,
        detoast_us: 0,
        blob_cache_hits: 0,
        blob_cache_misses: 0,
        blob_cache_bytes_served: 0,
        decompress_us: 0,
        agg_us: 0,
        total_segments: 0,
        total_rows_processed: 0,
        batch_quals_count: ctx.batch_quals.len(),
        where_quals_null: ctx.batch_quals.is_empty() && ctx.seg_filters.is_empty(),
        segments_metadata_resolved: 0,
        segments_decompressed: 0,
        regex_cache_size: 0,
        regex_cache_calls: 0,
        topn_limit: 0,
        topn_sort_col: -1,
        topn_ascending: true,
        pre_topn_groups: 0,
        merge_us: 0,
        finalize_us: 0,
        topn_select_us: 0,
        n_workers: 0,
        bare_limit: 0,
        wall_us: 0,
        buf_stats: super::segments::ScanBufferStats::default(),
        f8_preselected: 0,
        pscan: std::ptr::null_mut(),
        is_parallel_worker: is_worker,
        exec_ctx: Some(Box::new(ctx)),
    }
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
        // Phase C.1: hook bodies are functional scaffolding. Workers can
        // attach to DSM and flag themselves as `is_parallel_worker = true`,
        // but they don't yet claim segments — `begin_agg_scan` short-
        // circuits into `build_minimal_worker_state` and `exec_agg_scan`
        // returns EOF. Phase C.2 wires up the actual segment-claim and
        // partial-aggregate work.
        //
        // `add_agg_path` still sets `parallel_workers = 0` until C.4, so
        // these hooks remain unused under the default cost path.
        EstimateDSMCustomScan: Some(estimate_dsm_deltax_agg),
        InitializeDSMCustomScan: Some(initialize_dsm_deltax_agg),
        ReInitializeDSMCustomScan: Some(reinit_dsm_deltax_agg),
        InitializeWorkerCustomScan: Some(init_worker_deltax_agg),
        ShutdownCustomScan: Some(shutdown_deltax_agg),
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
pub(super) enum OutputEntry {
    Agg(usize),    // index into agg_specs
    Group(usize),  // index into group_specs
    Const(pg_sys::Datum, bool),  // constant value + is_null
    /// Derived from another group key: value = group_keys[base_gi] + delta.
    /// Used for eliminated redundant GROUP BY expressions (e.g. GROUP BY col, col-1, col-2).
    DerivedGroup { base_gi: usize, delta: i64 },
}

/// All fields deserialized from a DeltaXAgg node's custom_private list.
///
/// The planner (hook.rs / path.rs) packs the aggregate plan into a flat integer
/// list because PostgreSQL's custom scan API only allows passing a `List*` through
/// `custom_private`. This struct is the Rust-side representation after parsing.
pub(super) struct ParsedAggPlan {
    pub(super) companion_oids: Vec<pg_sys::Oid>,
    pub(super) agg_specs: Vec<AggExecSpec>,
    pub(super) group_specs: Vec<GroupByColSpec>,
    pub(super) output_map: Vec<OutputEntry>,
    pub(super) having_filters: Vec<HavingFilter>,
    pub(super) where_quals: *mut pg_sys::List,
    pub(super) topn_limit: i64,
    pub(super) topn_sort_col: usize,
    pub(super) topn_ascending: bool,
    /// `Some((max_slot, min_slot))` when the sort key is a derived expression
    /// `storage[max] - storage[min]` over two compact-storage slots — the
    /// JSONBench-Q4 shape `ORDER BY EXTRACT(EPOCH FROM (MAX-MIN))*N`.
    /// When set, `topn_sort_col` is the sentinel `usize::MAX` (the path layer
    /// emits `TOPN_SORT_COL_DERIVED = -3`, which we map to `usize::MAX` here)
    /// and the runtime uses `derived_minmax_topn` for sort-value computation.
    pub(super) derived_minmax_topn: Option<(usize, usize)>,
    pub(super) bare_limit: i64,
    /// Phase C.2 activation: when true, this CustomScan is the partial-mode
    /// node below a Gather + Final Aggregate. exec_agg_scan emits per-group
    /// rows whose values match PG's `aggtranstype` (via `compact_emit_partial`)
    /// instead of final-aggregate values. Default false → existing
    /// complete-aggregate path.
    #[allow(dead_code)] // wired by C.2 activation in path.rs
    pub(super) is_partial: bool,
}

/// Deserialize a CaseWhenValue from a custom_private integer list.
unsafe fn deserialize_case_when_value_inline(
    list: *mut pg_sys::List,
    idx: &mut i32,
) -> CaseWhenValue {
    unsafe {
    let tag = pg_sys::list_nth_int(list, *idx);
    *idx += 1;
    if tag == 0 {
        let col_idx = pg_sys::list_nth_int(list, *idx) as usize;
        *idx += 1;
        CaseWhenValue::ColumnRef(col_idx)
    } else {
        let str_len = pg_sys::list_nth_int(list, *idx) as usize;
        *idx += 1;
        let mut bytes = Vec::with_capacity(str_len);
        for _ in 0..str_len {
            bytes.push(pg_sys::list_nth_int(list, *idx) as u8);
            *idx += 1;
        }
        CaseWhenValue::StringConst(String::from_utf8_lossy(&bytes).into_owned())
    }
    }
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
            // H.2: OutputTransform trailer — only present for Min/Max
            // (keeps the wire format identical for Count/Sum/Avg/CountDistinct).
            // tag(0=None,1=PgUsShift); when tag==1, followed by lo+hi i32s
            // for the i64 delta.
            let output_transform = if matches!(agg_type, AggType::Min | AggType::Max) && idx < list_len {
                let tag = pg_sys::list_nth_int(custom_private, idx);
                idx += 1;
                match tag {
                    1 => {
                        let lo = pg_sys::list_nth_int(custom_private, idx) as u32 as u64;
                        let hi = pg_sys::list_nth_int(custom_private, idx + 1) as u32 as u64;
                        idx += 2;
                        let delta = ((hi << 32) | lo) as i64;
                        OutputTransform::PgUsShift { delta }
                    }
                    _ => OutputTransform::None,
                }
            } else {
                OutputTransform::None
            };
            let _ = result_oid; // parsed for offset, not stored
            agg_specs.push(AggExecSpec {
                agg_type,
                col_idx,
                col_type_oid: pg_sys::Oid::from(col_type_oid),
                expr_kind,
                const_offset,
                is_partial: false,
                transtype_oid: pg_sys::InvalidOid,
                output_transform,
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
                // Extract: func_oid, divisor_hi, divisor_lo, unit_len, unit_bytes...
                let func_oid = pg_sys::list_nth_int(custom_private, idx) as u32;
                idx += 1;
                let div_hi = pg_sys::list_nth_int(custom_private, idx) as i64;
                idx += 1;
                let div_lo = pg_sys::list_nth_int(custom_private, idx) as u32 as i64;
                idx += 1;
                let divisor = (div_hi << 32) | div_lo;
                let unit_len = pg_sys::list_nth_int(custom_private, idx) as usize;
                idx += 1;
                let mut unit_bytes = Vec::with_capacity(unit_len);
                for _ in 0..unit_len {
                    unit_bytes.push(pg_sys::list_nth_int(custom_private, idx) as u8);
                    idx += 1;
                }
                let unit = String::from_utf8_lossy(&unit_bytes).into_owned();
                GroupByExpr::Extract { unit, func_oid, divisor }
            } else if expr_tag == 4 {
                // AddConst: offset_i32, op_oid
                let offset = pg_sys::list_nth_int(custom_private, idx) as i64;
                let op_oid = pg_sys::list_nth_int(custom_private, idx + 1) as u32;
                idx += 2;
                GroupByExpr::AddConst { offset, op_oid }
            } else if expr_tag == 5 {
                // CaseWhen
                let num_clauses = pg_sys::list_nth_int(custom_private, idx) as usize;
                idx += 1;
                let mut clauses = Vec::with_capacity(num_clauses);
                for _ in 0..num_clauses {
                    let num_conditions = pg_sys::list_nth_int(custom_private, idx) as usize;
                    idx += 1;
                    let mut conditions = Vec::with_capacity(num_conditions);
                    for _ in 0..num_conditions {
                        let cond_col_idx = pg_sys::list_nth_int(custom_private, idx) as usize;
                        let op_val = pg_sys::list_nth_int(custom_private, idx + 1);
                        let const_hi = pg_sys::list_nth_int(custom_private, idx + 2) as i64;
                        let const_lo = pg_sys::list_nth_int(custom_private, idx + 3) as u32 as i64;
                        idx += 4;
                        let op = if op_val == 0 { CaseWhenOp::Eq } else { CaseWhenOp::NotEq };
                        let const_val = (const_hi << 32) | const_lo;
                        conditions.push(CaseWhenCondition { col_idx: cond_col_idx, op, const_val });
                    }
                    let result = deserialize_case_when_value_inline(custom_private, &mut idx);
                    clauses.push(CaseWhenClause { conditions, result });
                }
                let default = deserialize_case_when_value_inline(custom_private, &mut idx);
                GroupByExpr::CaseWhen(CaseWhenSpec { clauses, default })
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
            } else if otype == 3 {
                // DerivedGroup: base_gi in oref, delta_hi, delta_lo
                let delta_hi = pg_sys::list_nth_int(custom_private, idx) as i64;
                let delta_lo = pg_sys::list_nth_int(custom_private, idx + 1) as u32 as i64;
                idx += 2;
                let delta = (delta_hi << 32) | delta_lo;
                output_map.push(OutputEntry::DerivedGroup { base_gi: oref, delta });
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
    let mut derived_minmax_topn: Option<(usize, usize)> = None;
    if idx < list_len {
        let limit_val = pg_sys::list_nth_int(custom_private, idx);
        idx += 1;
        if limit_val > 0 {
            let sort_col_raw = pg_sys::list_nth_int(custom_private, idx);
            idx += 1;
            topn_ascending = pg_sys::list_nth_int(custom_private, idx) != 0;
            idx += 1;
            // -1 = bare LIMIT (no sort); -3 = derived MIN/MAX-difference
            // sort (Q4 shape) — two additional ints follow with the
            // (max_agg_idx, min_agg_idx) pair.
            if sort_col_raw == -3 {
                let max_idx = pg_sys::list_nth_int(custom_private, idx) as usize;
                idx += 1;
                let min_idx = pg_sys::list_nth_int(custom_private, idx) as usize;
                idx += 1;
                topn_limit = limit_val as i64;
                topn_sort_col = usize::MAX; // unused — see derived_minmax_topn
                derived_minmax_topn = Some((max_idx, min_idx));
            } else if sort_col_raw < 0 {
                bare_limit = limit_val as i64; // bare LIMIT, no sort
            } else {
                topn_limit = limit_val as i64;
                topn_sort_col = sort_col_raw as usize;
            }
        }
    }
    // Phase C.2 activation: trailing `is_partial` flag (1 byte). Older
    // plans / test fixtures may end without it — default false in that
    // case so existing paths stay compatible.
    let is_partial = if idx < list_len {
        let v = pg_sys::list_nth_int(custom_private, idx) != 0;
        idx += 1;
        v
    } else {
        false
    };
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
        derived_minmax_topn,
        bare_limit,
        is_partial,
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
            OutputEntry::Group(_) | OutputEntry::DerivedGroup { .. } => row.push((pg_sys::Datum::from(0usize), true)),
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
        detoast_us: 0,
        blob_cache_hits: 0,
        blob_cache_misses: 0,
        blob_cache_bytes_served: 0,
        decompress_us: 0,
        agg_us: 0,
        total_segments: 0,
        total_rows_processed: 0,
        batch_quals_count: 0,
        where_quals_null: true,
        segments_metadata_resolved: 0,
        segments_decompressed: 0,
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
        bare_limit: 0, wall_us: 0, buf_stats: super::segments::take_scan_buf_stats(), f8_preselected: 0, pscan: std::ptr::null_mut(), is_parallel_worker: false, exec_ctx: None,
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
    batch_quals: &[BatchQual],
    metadata_us: u64,
    heap_scan_us: u64,
) -> Option<AggScanState> {
    // Still bail on GROUP BY and HAVING
    if !plan.group_specs.is_empty() || !plan.having_filters.is_empty() {
        return None;
    }

    let has_where = !plan.where_quals.is_null();

    if has_where {
        // Bail if no batch quals extracted (unhandled qual types)
        if batch_quals.is_empty() { return None; }
        // Bail if any qual is on a non-numeric type (text LIKE etc.)
        let numeric_types = [pg_sys::INT2OID, pg_sys::INT4OID, pg_sys::INT8OID,
            pg_sys::FLOAT4OID, pg_sys::FLOAT8OID,
            pg_sys::TIMESTAMPOID, pg_sys::TIMESTAMPTZOID, pg_sys::DATEOID];
        if batch_quals.iter().any(|bq| !numeric_types.contains(&bq.type_oid)) {
            return None;
        }
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
            // Min/Max can only be resolved from metadata when there's no WHERE
            AggType::Min | AggType::Max => {
                !has_where && spec.expr_kind == AggExpr::Column && spec.col_idx >= 0
            }
            _ => false, // CountDistinct always bails
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

    // Accumulate from metadata (with optional filtered decompression for ambiguous segments)
    let mut accumulators: Vec<AggAccumulator> = plan.agg_specs
        .iter()
        .map(|spec| AggAccumulator::new_for(spec.agg_type, spec.col_type_oid))
        .collect();

    // Pre-classify segments
    let (allpass, ambiguous): (Vec<&SegmentData>, Vec<&SegmentData>) = if has_where {
        let mut ap = Vec::new();
        let mut amb = Vec::new();
        for seg in segments {
            if seg.row_count == 0 {
                continue;
            }
            match classify_segment_quals(seg, batch_quals, &meta.col_names) {
                SegmentQualResult::AllPass => ap.push(seg),
                SegmentQualResult::NonePass => {} // skip — no rows pass
                SegmentQualResult::Ambiguous => amb.push(seg),
            }
        }
        (ap, amb)
    } else {
        (
            segments.iter().filter(|s| s.row_count > 0).collect(),
            vec![],
        )
    };

    // AllPass: instant metadata accumulation
    for seg in &allpass {
        accumulate_segment_metadata(&mut accumulators, seg, &plan.agg_specs, meta);
    }
    let segments_metadata_resolved = allpass.len() as u64;

    // Try to resolve ambiguous segments via nonzero_count metadata
    // Only works for: single qual, Ne/Eq with const 0, all aggs are CountStar
    let ambiguous = if !ambiguous.is_empty()
        && batch_quals.len() == 1
        && plan.agg_specs.iter().all(|s| s.agg_type == AggType::CountStar)
    {
        let bq = &batch_quals[0];
        if is_zero_const(bq.const_datum, bq.type_oid)
            && matches!(bq.op, BatchCompareOp::Ne | BatchCompareOp::Eq)
        {
            let col_name = &meta.col_names[bq.col_idx];
            // Check all ambiguous segments have nonzero_count metadata
            let all_have_nz = ambiguous.iter().all(|seg| {
                seg.col_sums.get(col_name)
                    .map(|cs| cs.nonzero_count >= 0)
                    .unwrap_or(false)
            });
            if all_have_nz {
                // Resolve from metadata
                for seg in &ambiguous {
                    let cs = seg.col_sums.get(col_name).unwrap();
                    let passing = match bq.op {
                        BatchCompareOp::Ne => cs.nonzero_count,
                        BatchCompareOp::Eq => cs.nonnull_count - cs.nonzero_count,
                        _ => unreachable!(),
                    };
                    for (i, spec) in plan.agg_specs.iter().enumerate() {
                        if spec.agg_type == AggType::CountStar
                            && let AggAccumulator::Count { count } = &mut accumulators[i]
                        {
                            *count += passing;
                        }
                    }
                }
                vec![] // all resolved
            } else {
                ambiguous
            }
        } else {
            ambiguous
        }
    } else {
        ambiguous
    };

    // If ambiguous segments remain but have no blobs loaded (metadata-only fast path),
    // bail out — the full scan path will handle them with proper blob loading.
    if !ambiguous.is_empty()
        && ambiguous.iter().any(|s| s.compressed_blobs.iter().all(|b| b.is_empty()))
    {
        return None;
    }

    // Ambiguous: parallel decompression
    let n_workers = crate::get_parallel_workers();
    let (segments_decompressed, agg_us) = if !ambiguous.is_empty() && n_workers > 1 {
        let chunk_size = ambiguous.len().div_ceil(n_workers);
        let results: Vec<_> = std::thread::scope(|s| {
            ambiguous
                .chunks(chunk_size)
                .map(|chunk| {
                    let specs = &plan.agg_specs;
                    let bqs = batch_quals;
                    let m = meta;
                    s.spawn(move || {
                        let mut local_acc: Vec<AggAccumulator> = specs
                            .iter()
                            .map(|sp| AggAccumulator::new_for(sp.agg_type, sp.col_type_oid))
                            .collect();
                        let t = Instant::now();
                        for seg in chunk {
                            unsafe {
                                accumulate_segment_decompressed(
                                    &mut local_acc, seg, bqs, specs, m,
                                );
                            }
                        }
                        (local_acc, chunk.len() as u64, t.elapsed().as_micros() as u64)
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect()
        });
        let mut total_decomp = 0u64;
        let mut max_us = 0u64;
        for (local_acc, cnt, us) in results {
            for (dst, src) in accumulators.iter_mut().zip(local_acc.iter()) {
                merge_accumulator(dst, src);
            }
            total_decomp += cnt;
            max_us = max_us.max(us);
        }
        (total_decomp, max_us)
    } else if !ambiguous.is_empty() {
        let t = Instant::now();
        for seg in &ambiguous {
            unsafe {
                accumulate_segment_decompressed(
                    &mut accumulators, seg, batch_quals, &plan.agg_specs, meta,
                );
            }
        }
        (ambiguous.len() as u64, t.elapsed().as_micros() as u64)
    } else {
        (0, 0)
    };

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
            OutputEntry::Group(_) | OutputEntry::DerivedGroup { .. } => row.push((pg_sys::Datum::from(0usize), true)),
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
        detoast_us: 0,
        blob_cache_hits: 0,
        blob_cache_misses: 0,
        blob_cache_bytes_served: 0,
        decompress_us: 0,
        agg_us,
        total_segments,
        total_rows_processed: 0,
        batch_quals_count: batch_quals.len(),
        where_quals_null: !has_where,
        segments_metadata_resolved,
        segments_decompressed,
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
        bare_limit: 0, wall_us: 0, buf_stats: super::segments::take_scan_buf_stats(), f8_preselected: 0, pscan: std::ptr::null_mut(), is_parallel_worker: false, exec_ctx: None,
    })
}

/// Merge a source accumulator into a destination (used for parallel reduction).
/// Only Count/SumInt/SumFloat are used in filtered fast path (Min/Max/CountDistinct bail earlier).
fn merge_accumulator(dst: &mut AggAccumulator, src: &AggAccumulator) {
    match (dst, src) {
        (AggAccumulator::Count { count: dc }, AggAccumulator::Count { count: sc }) => *dc += sc,
        (
            AggAccumulator::SumInt { sum: ds, count: dc },
            AggAccumulator::SumInt { sum: ss, count: sc },
        ) => {
            *ds += ss;
            *dc += sc;
        }
        (
            AggAccumulator::SumFloat { sum: ds, count: dc },
            AggAccumulator::SumFloat { sum: ss, count: sc },
        ) => {
            *ds += ss;
            *dc += sc;
        }
        _ => {}
    }
}

/// Accumulate aggregate results from segment metadata (no decompression).
fn accumulate_segment_metadata(
    accumulators: &mut [AggAccumulator],
    seg: &SegmentData,
    agg_specs: &[AggExecSpec],
    meta: &super::segments::MetadataInfo,
) {
    for (i, spec) in agg_specs.iter().enumerate() {
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
                    let add_const = if spec.expr_kind == AggExpr::AddConst {
                        spec.const_offset
                    } else {
                        0
                    };
                    match &mut accumulators[i] {
                        AggAccumulator::SumInt { sum, count } => {
                            let v = if let Some(v) = cs.sum_i128 {
                                v
                            } else {
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
                                match s.parse::<i128>() {
                                    Ok(v) => v,
                                    Err(_) => continue,
                                }
                            };
                            *sum += v + add_const as i128 * cs.nonnull_count as i128;
                            *count += cs.nonnull_count;
                        }
                        AggAccumulator::SumFloat { sum, count } => {
                            let f = if let Some(v) = cs.sum_i128 {
                                v as f64
                            } else if let Some(v) = cs.sum_f64 {
                                v
                            } else if cs.type_oid == pg_sys::NUMERICOID {
                                // Normalized colstats stores all sums as NUMERIC
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
                                match s.parse::<f64>() {
                                    Ok(v) => v,
                                    Err(_) => continue,
                                }
                            } else {
                                // Legacy wide meta table: sum stored as FLOAT8
                                f64::from_bits(cs.sum_datum.value() as u64)
                            };
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
                            // Convert from colstats encoding to PG-native datum domain
                            let v = decode_encoded_to_pg_i64(cm.min_encoded, cm.type_oid);
                            *val = Some(val.map_or(v, |cur| cur.min(v)));
                        }
                        AggAccumulator::MinFloat { val } => {
                            let v = if cm.type_oid == pg_sys::FLOAT4OID {
                                decode_i64_to_f32(cm.min_encoded) as f64
                            } else {
                                decode_i64_to_f64(cm.min_encoded)
                            };
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
                            // Convert from colstats encoding to PG-native datum domain
                            let v = decode_encoded_to_pg_i64(cm.max_encoded, cm.type_oid);
                            *val = Some(val.map_or(v, |cur| cur.max(v)));
                        }
                        AggAccumulator::MaxFloat { val } => {
                            let v = if cm.type_oid == pg_sys::FLOAT4OID {
                                decode_i64_to_f32(cm.max_encoded) as f64
                            } else {
                                decode_i64_to_f64(cm.max_encoded)
                            };
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

/// Decompress an ambiguous segment, apply batch quals, and accumulate agg results.
unsafe fn accumulate_segment_decompressed(
    accumulators: &mut [AggAccumulator],
    seg: &SegmentData,
    batch_quals: &[BatchQual],
    agg_specs: &[AggExecSpec],
    meta: &super::segments::MetadataInfo,
) {
    let row_count = seg.row_count as usize;
    if row_count == 0 { return; }

    // Collect unique column indices needed (qual columns + agg columns)
    let mut col_indices: Vec<usize> = Vec::new();
    for bq in batch_quals {
        if !col_indices.contains(&bq.col_idx) {
            col_indices.push(bq.col_idx);
        }
    }
    for spec in agg_specs {
        if spec.col_idx >= 0 && !col_indices.contains(&(spec.col_idx as usize)) {
            col_indices.push(spec.col_idx as usize);
        }
    }

    // Decompress needed columns. Map col_idx → decompressed datums.
    let mut decompressed: HashMap<usize, Vec<(pg_sys::Datum, bool)>> = HashMap::new();
    for &col_idx in &col_indices {
        let col_name = &meta.col_names[col_idx];
        if meta.segment_by.contains(col_name) {
            continue; // segment_by columns are not in compressed_blobs
        }
        // Compute blob index (skip segment_by columns)
        let mut blob_idx = 0;
        for (ci, cn) in meta.col_names.iter().enumerate() {
            if ci == col_idx { break; }
            if !meta.segment_by.contains(cn) { blob_idx += 1; }
        }
        if blob_idx < seg.compressed_blobs.len() {
            let blob = &seg.compressed_blobs[blob_idx];
            let data_type = pg_type_name(meta.col_types[col_idx]);
            let typmod = meta.col_typmods[col_idx];
            let datums = unsafe { decompress_blob_to_datums(blob, &data_type, meta.col_types[col_idx], typmod) };
            decompressed.insert(col_idx, datums);
        }
    }

    // Build selection vector from batch quals
    let mut sel = vec![true; row_count];
    for bq in batch_quals {
        if let Some(col) = decompressed.get(&bq.col_idx) {
            if col.is_empty() { continue; }
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
                _ => {}
            }
        }
    }

    // Accumulate from filtered rows
    for (i, spec) in agg_specs.iter().enumerate() {
        match spec.agg_type {
            AggType::CountStar => {
                if let AggAccumulator::Count { count } = &mut accumulators[i] {
                    *count += sel.iter().filter(|&&s| s).count() as i64;
                }
            }
            AggType::Count => {
                if let AggAccumulator::Count { count } = &mut accumulators[i]
                    && let Some(col) = decompressed.get(&(spec.col_idx as usize))
                {
                    for (j, &selected) in sel.iter().enumerate() {
                        if selected && j < col.len() && !col[j].1 {
                            *count += 1;
                        }
                    }
                }
            }
            AggType::Sum | AggType::Avg => {
                let add_const = if spec.expr_kind == AggExpr::AddConst {
                    spec.const_offset
                } else {
                    0
                };
                if let Some(col) = decompressed.get(&(spec.col_idx as usize)) {
                    match &mut accumulators[i] {
                        AggAccumulator::SumInt { sum, count } => {
                            for (j, &selected) in sel.iter().enumerate() {
                                if selected && j < col.len() && !col[j].1 {
                                    let v = col[j].0.value() as i64;
                                    *sum += v as i128 + add_const as i128;
                                    *count += 1;
                                }
                            }
                        }
                        AggAccumulator::SumFloat { sum, count } => {
                            for (j, &selected) in sel.iter().enumerate() {
                                if selected && j < col.len() && !col[j].1 {
                                    let v = if spec.col_type_oid == pg_sys::FLOAT4OID {
                                        f32::from_bits(col[j].0.value() as u32) as f64
                                    } else {
                                        f64::from_bits(col[j].0.value() as u64)
                                    };
                                    *sum += v + add_const as f64;
                                    *count += 1;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {} // Min/Max/CountDistinct not supported with WHERE in this path
        }
    }
}

/// Load metadata via SPI for a `DeltaXAgg` plan. Returns the
/// `MetadataInfo` plus elapsed micros for the `metadata_us` timer.
///
/// Phase-A extraction from `begin_agg_scan`. The same metadata structure
/// (`col_names`, `col_types`, `col_typmods`, `segment_by`, `time_column`)
/// is what Phase C's `init_worker_deltax_agg` will hydrate from
/// `append_wire` instead of running SPI a second time per worker.
unsafe fn load_agg_metadata_from_plan(
    companion_oids: &[pg_sys::Oid],
) -> (super::segments::MetadataInfo, u64) {
    unsafe {
        if companion_oids.is_empty() {
            pgrx::error!("pg_deltax: load_agg_metadata_from_plan called with empty oids");
        }
        let first_name = {
            let name_ptr = pg_sys::get_rel_name(companion_oids[0]);
            if name_ptr.is_null() {
                pgrx::error!(
                    "pg_deltax: companion table not found for OID {}",
                    u32::from(companion_oids[0])
                );
            }
            std::ffi::CStr::from_ptr(name_ptr)
                .to_string_lossy()
                .into_owned()
        };
        let t0 = Instant::now();
        let meta = Spi::connect(|client| load_metadata(client, &first_name));
        let metadata_us = t0.elapsed().as_micros() as u64;
        (meta, metadata_us)
    }
}

/// BeginCustomScan callback for DeltaXAgg: decompress and aggregate.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn begin_agg_scan(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: i32,
) {
    unsafe {
        // Phase C.1: parallel-worker short-circuit. Each worker process runs
        // its own `BeginCustomScan` (PG calls it per-process for parallel-
        // aware paths). Workers must NOT duplicate the leader's SPI + heap
        // scan + accumulator construction here — that runs in
        // `init_worker_deltax_agg` once DSM is wired up, so the worker has
        // the leader's segment-cursor handle to drive the claim loop.
        if pg_sys::ParallelWorkerNumber >= 0 {
            let state = build_minimal_worker_state();
            let state_ptr = Box::into_raw(Box::new(state));
            (*node).custom_ps = state_ptr as *mut pg_sys::List;
            return;
        }

        // Phase C.2.c: parallel-aware leader branch. When the planner chose
        // the parallel path (gated by `add_agg_path` + `recommend_agg_workers`
        // in C.2.f), the leader hydrates an `AggExecContext` once and defers
        // all per-segment work to `exec_agg_scan`, where it claims segments
        // alongside workers via `pscan.next_segment.fetch_add`. The serial
        // / internal-rayon path below stays unchanged for `parallel_aware ==
        // false`.
        if (*(*node).ss.ps.plan).parallel_aware {
            let custom_private = (*node).custom_ps;
            if custom_private.is_null() {
                pgrx::error!("pg_deltax: missing custom_private in DeltaXAgg state");
            }
            let plan = parse_agg_private(custom_private);
            if plan.companion_oids.is_empty() {
                pgrx::error!("pg_deltax: DeltaXAgg has no companion tables");
            }
            super::segments::reset_scan_buf_stats();
            let ctx = build_agg_exec_context_from_plan(plan);
            let state = build_deferred_agg_state(ctx, /* is_worker */ false);
            let state_ptr = Box::into_raw(Box::new(state));
            (*node).custom_ps = state_ptr as *mut pg_sys::List;
            return;
        }

        let t_wall = Instant::now();
        // Reset per-phase buffer accumulator for this scan. `load_segments_heap`
        // writes to the thread-local; the AggScanState ctor reads it back out
        // via `take_scan_buf_stats()`.
        super::segments::reset_scan_buf_stats();
        let custom_private = (*node).custom_ps;
        if custom_private.is_null() {
            pgrx::error!("pg_deltax: missing custom_private in DeltaXAgg state");
        }

        let plan = parse_agg_private(custom_private);

        if plan.companion_oids.is_empty() {
            pgrx::error!("pg_deltax: DeltaXAgg has no companion tables");
        }

        let (meta, metadata_us) = load_agg_metadata_from_plan(&plan.companion_oids);

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

        // Fast path 2: answer from per-segment metadata (with selective decompression for filtered queries)
        // Skip early when conditions that try_metadata_fast_path checks immediately
        // would fail: GROUP BY, HAVING, CountDistinct, or non-numeric agg columns.
        let t_fp2 = Instant::now();
        if plan.group_specs.is_empty() && plan.having_filters.is_empty()
            && plan.agg_specs.iter().all(|s| s.agg_type != AggType::CountDistinct)
        {
            let needs_sums = plan.agg_specs.iter().any(|s| matches!(s.agg_type, AggType::Sum | AggType::Avg));
            let needs_counts = plan.agg_specs.iter().any(|s| matches!(s.agg_type, AggType::Count));
            let needs_minmax = plan.agg_specs.iter().any(|s| matches!(s.agg_type, AggType::Min | AggType::Max));
            let num_cols = meta.col_names.len();

            // Extract batch quals early for the filtered metadata fast path
            let fast_batch_quals = if !plan.where_quals.is_null() {
                let (bqs, handled) = extract_batch_quals(plan.where_quals, &meta.col_names, &meta.col_types);
                if handled as i32 == (*plan.where_quals).length { bqs } else { vec![] }
            } else {
                vec![]
            };

            let mut load_minmax = needs_minmax;
            if !fast_batch_quals.is_empty() {
                load_minmax = true;
            }

            // Build list of columns needing stats (sum/nonnull/nonzero) from colstats
            let mut needed_stats_set: std::collections::HashSet<String> = std::collections::HashSet::new();
            if needs_sums || needs_counts {
                for s in &plan.agg_specs {
                    if s.col_idx >= 0 && matches!(s.agg_type, AggType::Sum | AggType::Avg | AggType::Count) {
                        needed_stats_set.insert(meta.col_names[s.col_idx as usize].clone());
                    }
                }
            }
            for bq in &fast_batch_quals {
                needed_stats_set.insert(meta.col_names[bq.col_idx].clone());
            }
            let needed_stats_cols: Vec<String> = needed_stats_set.into_iter().collect();

            // Extract segment-by/time filters for pruning
            let (seg_filters, time_min, time_max) = if !plan.where_quals.is_null() {
                extract_segment_filters(
                    plan.where_quals, &meta.col_names, &meta.segment_by, &meta.time_column,
                )
            } else {
                (vec![], None, None)
            };

            // Build list of columns needing minmax from colstats
            let needed_minmax_cols: Vec<String> = plan.agg_specs.iter()
                .filter(|s| matches!(s.agg_type, AggType::Min | AggType::Max))
                .map(|s| meta.col_names[s.col_idx as usize].clone())
                .collect();

            // Fast path: load metadata only (no blobs) — Phase 2 is skipped
            let no_blobs = vec![false; num_cols];
            let t1 = Instant::now();
            let mut all_segments: Vec<SegmentData> = Vec::new();
            for &oid in &plan.companion_oids {
                let (segs, _, _, _, _, _) = load_segments_heap(
                    oid, &meta.col_names, &meta.segment_by, &no_blobs,
                    &meta.time_column, load_minmax, &seg_filters, time_min, time_max, None,
                    &fast_batch_quals, &needed_stats_cols,
                    &meta.col_types,
                    &needed_minmax_cols,
                    false,
                );
                all_segments.extend(segs);
            }
            let heap_scan_us = t1.elapsed().as_micros() as u64;

            if let Some(state) = try_metadata_fast_path(&plan, &meta, &all_segments, &fast_batch_quals, metadata_us, heap_scan_us) {
                let state_ptr = Box::into_raw(Box::new(state));
                (*node).custom_ps = state_ptr as *mut pg_sys::List;
                return;
            }
        }

        // Destructure plan for the full scan path
        let ParsedAggPlan {
            companion_oids, agg_specs, group_specs, output_map,
            having_filters, where_quals, topn_limit, topn_sort_col, topn_ascending,
            derived_minmax_topn, bare_limit, is_partial: _,
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
            // CaseWhen references additional columns in conditions and result values
            if let GroupByExpr::CaseWhen(ref spec) = gs.expr {
                for clause in &spec.clauses {
                    for cond in &clause.conditions {
                        if cond.col_idx < num_cols {
                            needed_cols[cond.col_idx] = true;
                        }
                    }
                    if let CaseWhenValue::ColumnRef(ci) = &clause.result
                        && *ci < num_cols {
                        needed_cols[*ci] = true;
                    }
                }
                if let CaseWhenValue::ColumnRef(ci) = &spec.default
                    && *ci < num_cols {
                    needed_cols[*ci] = true;
                }
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

        // Build sidecar_only_cols: text columns where every aggregate on the
        // column is LengthOf AND every batch qual is `= ''` / `<> ''`, and the
        // column is not in GROUP BY. For such columns we can skip detoasting
        // the main text blob and use the compact per-row length sidecar.
        //
        // Only the parallel mixed path knows how to read from the sidecar; if
        // that path won't run for this query, we must load the main blob as
        // usual. So the sidecar flags are cleared when the query isn't a
        // parallel-mixed candidate.
        let sidecar_candidate: Vec<bool> = (0..num_cols).map(|col_idx| {
            let t = meta.col_types[col_idx];
            let is_text = t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID;
            if !is_text { return false; }
            if !needed_cols[col_idx] { return false; }

            // Must not appear in GROUP BY
            if group_specs.iter().any(|gs| gs.col_idx >= 0 && gs.col_idx as usize == col_idx) {
                return false;
            }
            // Every agg on this column must be LengthOf
            let agg_refs: Vec<&AggExecSpec> = agg_specs.iter()
                .filter(|s| s.col_idx >= 0 && s.col_idx as usize == col_idx)
                .collect();
            if !agg_refs.iter().all(|s| s.expr_kind == AggExpr::LengthOf) {
                return false;
            }
            // Every batch qual on this column must be EQ/NE with empty string
            let qual_refs: Vec<&BatchQual> = batch_quals.iter()
                .filter(|bq| bq.col_idx == col_idx)
                .collect();
            let all_quals_ok = qual_refs.iter().all(|bq| {
                matches!(bq.op, BatchCompareOp::Eq | BatchCompareOp::Ne)
                    && matches!(bq.text_const.as_deref(), Some(""))
            });
            if !all_quals_ok { return false; }
            // Must have at least one usage (otherwise the column wouldn't be needed)
            !agg_refs.is_empty() || !qual_refs.is_empty()
        }).collect();

        // Gate sidecar activation on the parallel-mixed path being usable.
        //
        // can_parallel_mixed_flag (computed later) also requires
        // `all_segments.len() > 1` — but segments aren't loaded yet.
        // Without checking that upstream, a query that ends up going
        // through the non-parallel path reads from `compressed_blobs`
        // entries that were never loaded (because sidecar mode skipped
        // them), silently returning NULL from AVG(length()).
        //
        // Use a conservative catalog-based row estimate: one default
        // segment is 30K rows, so require at least 2× that to be
        // confident the parallel path will fire.
        const SIDECAR_MIN_ROWS: i64 = 60_000;
        let estimated_rows: i64 = companion_oids
            .iter()
            .map(|&oid| super::super::cost::get_row_count(oid).unwrap_or(0))
            .sum();
        let sidecar_only_cols: Vec<bool> = if sidecar_candidate.iter().any(|&s| s)
            && crate::get_parallel_workers() > 1
            && estimated_rows >= SIDECAR_MIN_ROWS
            && can_parallel_mixed(&group_specs, &needed_cols, &meta.col_types, &meta.col_not_null, &batch_quals, &agg_specs)
        {
            sidecar_candidate
        } else {
            vec![false; num_cols]
        };

        // For load_segments_heap, suppress main-blob loading for sidecar-only
        // columns. We load their length sidecars separately below.
        let mut needed_cols_main: Vec<bool> = needed_cols.clone();
        for (i, &s) in sidecar_only_cols.iter().enumerate() {
            if s {
                needed_cols_main[i] = false;
            }
        }

        let mut needed_minmax_set: std::collections::HashSet<String> = std::collections::HashSet::new();
        for gs in &group_specs {
            if matches!(gs.expr, GroupByExpr::Extract { .. })
                && gs.col_idx >= 0
                && (gs.col_idx as usize) < meta.col_names.len()
            {
                needed_minmax_set.insert(meta.col_names[gs.col_idx as usize].clone());
            }
        }
        let needed_minmax_cols: Vec<String> = needed_minmax_set.into_iter().collect();

        // Load segments from all companion tables (with lazy pruning)
        let n_workers = crate::get_parallel_workers();
        let use_lazy = n_workers > 1;
        let lazy_cols: Vec<bool> = needed_cols_main.clone();
        let mut all_segments: Vec<SegmentData> = Vec::new();
        let mut total_detoast_us: u64 = 0;
        // Blob-cache accumulators paired with `total_detoast_us` so call
        // sites can fold the `DetoastLazyStats` returned by the lazy
        // helpers. Surfaced in EXPLAIN as `DeltaX Blob Cache`.
        let mut total_cache_hits: u64 = 0;
        let mut total_cache_misses: u64 = 0;
        let mut total_cache_bytes_served: u64 = 0;
        for &oid in &companion_oids {
            let (mut segs, _, _, _, _, dt_us) = load_segments_heap(
                oid, &meta.col_names, &meta.segment_by, &needed_cols_main,
                &meta.time_column, !needed_minmax_cols.is_empty(), &seg_filters, time_min, time_max,
                if use_lazy { Some(&lazy_cols) } else { None },
                &batch_quals, &needed_minmax_cols,
                &meta.col_types,
                &needed_minmax_cols,
                false,
            );
            // Load text-length sidecars for the columns in sidecar-only mode.
            if sidecar_only_cols.iter().any(|&s| s) {
                let sidecar_detoast_us = super::segments::load_text_length_sidecars(
                    oid, &meta.col_names, &meta.segment_by, &sidecar_only_cols, &mut segs,
                );
                total_detoast_us += sidecar_detoast_us;
            }
            all_segments.extend(segs);
            total_detoast_us += dt_us;
        }
        // heap_scan_us includes fast-path-2 heap scan (eager detoast for metadata
        // check) plus the main lazy scan.  t_fp2 was started before fast-path-2.
        let heap_scan_us = t_fp2.elapsed().as_micros() as u64;

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
        let use_compact_keys = has_group_by && can_use_compact_keys(&group_specs, &meta.col_not_null);
        let mut compact_group_map: CompactGroupMap = CompactGroupMap::with_hasher(BuildHasherDefault::default());
        let mut cd_sidecar = CountDistinctSideCar::new(&agg_specs);

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
            // CaseWhen ColumnRef results reference text columns that need decompression
            if let GroupByExpr::CaseWhen(ref spec) = gs.expr {
                for clause in &spec.clauses {
                    if let CaseWhenValue::ColumnRef(ci) = &clause.result
                        && *ci < text_group_cols.len() {
                        text_group_cols[*ci] = true;
                    }
                }
                if let CaseWhenValue::ColumnRef(ci) = &spec.default
                    && *ci < text_group_cols.len() {
                    text_group_cols[*ci] = true;
                }
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

            // Pipeline detoast with parallel processing when enough segments
            // to amortize thread::scope overhead; otherwise single scope.
            let use_pipeline = use_lazy && all_segments.len() >= n_workers * 16;

            if use_lazy {
                let t_detoast = Instant::now();
                if use_pipeline {
                    // Detoast only the first batch; rest overlaps with workers
                    let n_batches = (n_workers * 2).max(2).min(all_segments.len());
                    let batch_size = all_segments.len().div_ceil(n_batches);
                    let first_end = batch_size.min(all_segments.len());
                    for seg in &mut all_segments[..first_end] {
                        let dl = detoast_lazy_blobs(seg);
                        total_cache_hits += dl.cache_hits;
                        total_cache_misses += dl.cache_misses;
                        total_cache_bytes_served += dl.cache_bytes_served;
                    }
                } else {
                    // Few segments — detoast all upfront, single scope below
                    for seg in &mut all_segments {
                        let dl = detoast_lazy_blobs(seg);
                        total_cache_hits += dl.cache_hits;
                        total_cache_misses += dl.cache_misses;
                        total_cache_bytes_served += dl.cache_bytes_served;
                    }
                }
                total_detoast_us += t_detoast.elapsed().as_micros() as u64;
            }

            let mut pipeline_detoast_us: u64 = 0;
            let partial_results: Vec<ParallelCompactResult> = if use_pipeline {
                let n_batches = (n_workers * 2).max(2).min(all_segments.len());
                let batch_size = all_segments.len().div_ceil(n_batches);
                let mut results: Vec<ParallelCompactResult> = Vec::new();
                let mut batch_start = 0;
                let total_segs = all_segments.len();

                while batch_start < total_segs {
                    let batch_end = (batch_start + batch_size).min(total_segs);
                    let next_end = (batch_end + batch_size).min(total_segs);

                    let (done, pending) = all_segments.split_at_mut(batch_end);
                    let current_batch = &done[batch_start..];

                    std::thread::scope(|s| {
                        let chunk_size = current_batch.len().div_ceil(n_workers);
                        let handles: Vec<_> = current_batch.chunks(chunk_size).map(|chunk| {
                            let cfg = &config;
                            s.spawn(move || process_segments_compact(chunk, cfg))
                        }).collect();

                        // Main thread detoasts next batch while workers run
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
                // Single scope — original path (or lazy already detoasted above)
                let chunk_size = all_segments.len().div_ceil(n_workers);
                std::thread::scope(|s| {
                    let handles: Vec<_> = all_segments.chunks(chunk_size).map(|chunk| {
                        let cfg = &config;
                        s.spawn(move || process_segments_compact(chunk, cfg))
                    }).collect();
                    handles.into_iter().map(|h| h.join().unwrap()).collect()
                })
            };

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
            total_detoast_us += pipeline_detoast_us;
            let agg_us = scan_wall_us.saturating_sub(decompress_us + pipeline_detoast_us);

            // ----------------------------------------------------------
            // Speculative top-N: use pre-computed top-K candidates from
            // each worker (computed while data was cache-hot), merge only
            // those, and verify no missed key could beat the Nth result.
            // ----------------------------------------------------------
            let sort_slot_for_compact_spec = match output_map[topn_sort_col] {
                OutputEntry::Agg(ai) => ai,
                _ => 0,
            };
            let compact_sort_is_cd = topn_limit > 0 && matches!(
                compact_storage.as_ref().unwrap().layout.slots[sort_slot_for_compact_spec].1,
                CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr
            );
            let _has_any_cd_agg = compact_storage.as_ref().unwrap().layout.slots.iter()
                .any(|(_, k)| matches!(k, CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr));
            let compact_sort_is_avg = topn_limit > 0 && agg_specs[sort_slot_for_compact_spec].agg_type == AggType::Avg;
            if topn_limit > 0 && having_filters.is_empty() && !compact_sort_is_cd && !compact_sort_is_avg {
                let sort_slot = sort_slot_for_compact_spec;
                let (_, sort_kind) = compact_storage.as_ref().unwrap().layout.slots[sort_slot];
                let limit = topn_limit as usize;
                let k = (topn_limit as usize).max(1000);

                let read_sort = |storage: &CompactAccStorage, group_idx: u32| -> i64 {
                    match sort_kind {
                        CompactAccKind::Count => storage.read_count(group_idx, sort_slot),
                        CompactAccKind::SumIntNarrow => storage.read_sum_int_narrow(group_idx, sort_slot).0,
                        CompactAccKind::MinInt | CompactAccKind::MaxInt => {
                            storage.read_min_max_int(group_idx, sort_slot).0
                        }
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

                // Cost guard: Phase 2 iterates candidates × partial_results.
                // For low-cardinality GROUP BY, candidate_set is small → fast.
                // For high-cardinality with many pipeline batches, candidate_set
                // can be huge → skip speculative and go straight to full merge.
                let phase2_ops = candidate_set.len() as u64 * partial_results.len() as u64;
                if phase2_ops > 10_000_000 {
                    pgrx::log!(
                        "pg_deltax speculative top-N skipped: phase2 too expensive \
                         (candidates={} × results={} = {} ops)",
                        candidate_set.len(), partial_results.len(), phase2_ops,
                    );
                } else {

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
                    merged.sort_unstable_by_key(|b| std::cmp::Reverse(b.0));
                } else {
                    merged.sort_unstable_by_key(|a| a.0);
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
                    // Phase 5: For each winner, merge all accumulators and finalize.
                    //
                    // CountDistinct specs use a parallel partitioned count
                    // (same pattern as the no-GROUP-BY CD merge): 16 threads
                    // each own a hash partition, walk every worker's per-
                    // (winner, cd-spec) set, and count only values routing
                    // to their partition. Buckets are disjoint → final
                    // count = Σ bucket sizes. This replaces a serial
                    // `HashSet::extend` loop that was 98% of finalize on
                    // Q9-style queries (top-10 GROUP BY with a
                    // COUNT(DISTINCT) over a ~million-distinct column).
                    let t_fin = Instant::now();
                    let storage = compact_storage.as_mut().unwrap();
                    let num_group_keys = group_specs.len();
                    let n_winners = merged.len();

                    // Pre-resolve (winner, worker) -> worker_group_idx so
                    // worker threads don't hash-lookup repeatedly. None means
                    // the worker doesn't have that winner's key at all.
                    let winner_worker_idx: Vec<Vec<Option<u32>>> = merged.iter()
                        .map(|&(_, packed_key)| {
                            partial_results.iter()
                                .map(|r| r.compact_map.get(&packed_key).copied())
                                .collect()
                        })
                        .collect();

                    // Identify CD slots; these will be computed in parallel.
                    let cd_slot_specs: Vec<(usize, bool)> = agg_specs.iter()
                        .enumerate()
                        .filter_map(|(slot_idx, spec)| {
                            if spec.agg_type == AggType::CountDistinct {
                                let is_str = matches!(spec.col_type_oid,
                                    pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID);
                                Some((slot_idx, is_str))
                            } else {
                                None
                            }
                        })
                        .collect();

                    // Parallel partitioned count of CD slots across winners.
                    // Shape: cd_counts[winner_idx][cd_slot_rank] = i64 distinct.
                    let cd_counts: Vec<Vec<i64>> = if !cd_slot_specs.is_empty() {
                        const CD_WIN_PARTITIONS: usize = 16;
                        fn cd_part_int(v: i64) -> usize {
                            let mut x = v as u64;
                            x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
                            x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
                            x ^= x >> 31;
                            (x >> 60) as usize & (CD_WIN_PARTITIONS - 1)
                        }
                        fn cd_part_str(v: u128) -> usize {
                            ((v >> 124) as usize) & (CD_WIN_PARTITIONS - 1)
                        }

                        let partial_refs = &partial_results;
                        let winner_worker_ref = &winner_worker_idx;
                        let cd_specs_ref = &cd_slot_specs;

                        // bucket_counts[p][winner][cd_rank] = i64 partition-local count
                        let bucket_counts: Vec<Vec<Vec<i64>>> = std::thread::scope(|s| {
                            let handles: Vec<_> = (0..CD_WIN_PARTITIONS).map(|p| {
                                s.spawn(move || {
                                    // Per-winner per-cd-rank disjoint set
                                    let n_cd = cd_specs_ref.len();
                                    let mut local_int: Vec<Vec<CdSetInt>> = (0..n_winners)
                                        .map(|_| (0..n_cd).map(|_| new_cd_set_int()).collect())
                                        .collect();
                                    let mut local_str: Vec<Vec<CdSetStr>> = (0..n_winners)
                                        .map(|_| (0..n_cd).map(|_| new_cd_set_str()).collect())
                                        .collect();

                                    for (winner_idx, per_worker_gidx) in winner_worker_ref.iter().enumerate() {
                                        for (worker_idx, &maybe_gidx) in per_worker_gidx.iter().enumerate() {
                                            let Some(w_gidx) = maybe_gidx else { continue; };
                                            let worker_cd = &partial_refs[worker_idx].cd_sidecar;
                                            for (cd_rank, &(slot_idx, is_str)) in cd_specs_ref.iter().enumerate() {
                                                // Find the matching entry in worker's cd_sidecar.
                                                let Some(oe) = worker_cd.entries.iter()
                                                    .find(|e| e.spec_idx == slot_idx) else { continue; };
                                                if is_str {
                                                    let src = &oe.sets_str[w_gidx as usize];
                                                    let dst = &mut local_str[winner_idx][cd_rank];
                                                    for &v in src {
                                                        if cd_part_str(v) == p {
                                                            dst.insert(v);
                                                        }
                                                    }
                                                } else {
                                                    let src = &oe.sets_int[w_gidx as usize];
                                                    let dst = &mut local_int[winner_idx][cd_rank];
                                                    for &v in src {
                                                        if cd_part_int(v) == p {
                                                            dst.insert(v);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    // Return per-winner per-cd-rank counts.
                                    (0..n_winners).map(|w| {
                                        (0..n_cd).map(|c| {
                                            let (_, is_str) = cd_specs_ref[c];
                                            if is_str {
                                                local_str[w][c].len() as i64
                                            } else {
                                                local_int[w][c].len() as i64
                                            }
                                        }).collect()
                                    }).collect()
                                })
                            }).collect();
                            handles.into_iter().map(|h| h.join().unwrap()).collect()
                        });

                        // Sum per (winner, cd_rank) across partitions.
                        let n_cd = cd_slot_specs.len();
                        let mut total: Vec<Vec<i64>> = (0..n_winners)
                            .map(|_| vec![0i64; n_cd])
                            .collect();
                        for bucket in &bucket_counts {
                            for w in 0..n_winners {
                                for c in 0..n_cd {
                                    total[w][c] += bucket[w][c];
                                }
                            }
                        }
                        total
                    } else {
                        vec![vec![]; n_winners]
                    };

                    let mut result_rows = Vec::with_capacity(merged.len());
                    for (winner_idx, &(_, packed_key)) in merged.iter().enumerate() {
                        let global_idx = storage.alloc_group();

                        // Merge non-CD accumulators (cheap — few bytes per
                        // winner × worker × slot).
                        for (worker_idx, &maybe_gidx) in winner_worker_idx[winner_idx].iter().enumerate() {
                            let Some(worker_idx_w) = maybe_gidx else { continue; };
                            let result = &partial_results[worker_idx];
                            for (slot_idx, _) in agg_specs.iter().enumerate() {
                                let (_, kind) = storage.layout.slots[slot_idx];
                                match kind {
                                    CompactAccKind::Count => {
                                        let wc = result.compact_storage.read_count(worker_idx_w, slot_idx);
                                        *storage.count_mut(global_idx, slot_idx) += wc;
                                    }
                                    CompactAccKind::SumInt => {
                                        let (ws, wc) = result.compact_storage.read_sum_int(worker_idx_w, slot_idx);
                                        let (gs, gc) = storage.sum_int_mut(global_idx, slot_idx);
                                        *gs += ws;
                                        *gc += wc;
                                    }
                                    CompactAccKind::SumIntNarrow => {
                                        let (ws, wc) = result.compact_storage.read_sum_int_narrow(worker_idx_w, slot_idx);
                                        let (gs, gc) = storage.sum_int_narrow_mut(global_idx, slot_idx);
                                        *gs += ws;
                                        *gc += wc;
                                    }
                                    CompactAccKind::SumFloat => {
                                        let (ws, wc) = result.compact_storage.read_sum_float(worker_idx_w, slot_idx);
                                        let (gs, gc) = storage.sum_float_mut(global_idx, slot_idx);
                                        *gs += ws;
                                        *gc += wc;
                                    }
                                    CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                        let (w_off, w_len) = result.compact_storage.read_min_max_str(worker_idx_w, slot_idx);
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
                                    CompactAccKind::MinInt => {
                                        let (w_val, w_has) = result.compact_storage.read_min_max_int(worker_idx_w, slot_idx);
                                        if w_has { storage.update_min_int(global_idx, slot_idx, w_val); }
                                    }
                                    CompactAccKind::MaxInt => {
                                        let (w_val, w_has) = result.compact_storage.read_min_max_int(worker_idx_w, slot_idx);
                                        if w_has { storage.update_max_int(global_idx, slot_idx, w_val); }
                                    }
                                    CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                                        // Handled by parallel pass above.
                                    }
                                }
                            }
                        }

                        // Write CD counts from parallel pass into storage.
                        for (cd_rank, &(slot_idx, _)) in cd_slot_specs.iter().enumerate() {
                            *storage.count_mut(global_idx, slot_idx) = cd_counts[winner_idx][cd_rank];
                        }

                        // Finalize this group.
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
                                OutputEntry::DerivedGroup { base_gi, delta } => {
                                    let v = keys[*base_gi] + delta;
                                    row.push((pg_sys::Datum::from(v as usize), false));
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
                        detoast_us: total_detoast_us,
                        blob_cache_hits: total_cache_hits,
                        blob_cache_misses: total_cache_misses,
                        blob_cache_bytes_served: total_cache_bytes_served,
                        decompress_us,
                        agg_us,
                        total_segments,
                        total_rows_processed,
                        batch_quals_count: batch_quals.len(),
                        where_quals_null: where_quals.is_null(),
                        segments_metadata_resolved: 0,
                        segments_decompressed: 0,
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
                        bare_limit: 0, wall_us: t_wall.elapsed().as_micros() as u64, buf_stats: super::segments::take_scan_buf_stats(), f8_preselected: 0, pscan: std::ptr::null_mut(), is_parallel_worker: false, exec_ctx: None,
                    };

                    let state_box = Box::new(state);
                    let state_ptr = Box::into_raw(state_box);
                    (*node).custom_ps = state_ptr as *mut pg_sys::List;
                    return;
                }
                // Speculation failed — check if all candidates are tied.
                // When nth_value == all merged candidates' values (e.g. COUNT on unique keys
                // where every group has count=1), any N groups are valid — skip the expensive
                // partitioned merge and use the bare_limit-style shortcut.
                let nth_value = merged.get(limit.saturating_sub(1)).map(|x| x.0).unwrap_or(0);
                let all_tied = merged.len() >= limit
                    && merged.iter().all(|&(v, _)| v == nth_value);

                let spec_fail_us = t_spec.elapsed().as_micros() as u64;
                pgrx::log!(
                    "pg_deltax speculative top-N failed: candidates={} k={} nth={} floor_sum={} all_tied={} (wasted {:.1}ms)",
                    merged.len(), k,
                    nth_value,
                    floor_sum,
                    all_tied,
                    spec_fail_us as f64 / 1000.0,
                );

                if all_tied {
                    // All candidate groups have the same sort value — any N are valid.
                    // Use the first N candidates directly (they're already merged).
                    merged.truncate(limit);

                    let t_fin = Instant::now();
                    let storage = compact_storage.as_mut().unwrap();
                    let num_group_keys = group_specs.len();
                    let mut result_rows = Vec::with_capacity(merged.len());
                    let mut spec_cd_sidecar = CountDistinctSideCar::new(&agg_specs);

                    for &(_, packed_key) in &merged {
                        let global_idx = storage.alloc_group();
                        spec_cd_sidecar.alloc_group();

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
                                        CompactAccKind::MinInt => {
                                            let (w_val, w_has) = result.compact_storage.read_min_max_int(worker_idx, slot_idx);
                                            if w_has { storage.update_min_int(global_idx, slot_idx, w_val); }
                                        }
                                        CompactAccKind::MaxInt => {
                                            let (w_val, w_has) = result.compact_storage.read_min_max_int(worker_idx, slot_idx);
                                            if w_has { storage.update_max_int(global_idx, slot_idx, w_val); }
                                        }
                                        CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                                            spec_cd_sidecar.union_from(slot_idx, global_idx, &result.cd_sidecar, worker_idx);
                                        }
                                    }
                                }
                            }
                        }

                        for e in &spec_cd_sidecar.entries {
                            let count = e.count(global_idx);
                            *storage.count_mut(global_idx, e.spec_idx) = count;
                        }

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
                                OutputEntry::DerivedGroup { base_gi, delta } => {
                                    let v = keys[*base_gi] + delta;
                                    row.push((pg_sys::Datum::from(v as usize), false));
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
                        detoast_us: total_detoast_us,
                        blob_cache_hits: total_cache_hits,
                        blob_cache_misses: total_cache_misses,
                        blob_cache_bytes_served: total_cache_bytes_served,
                        decompress_us,
                        agg_us,
                        total_segments,
                        total_rows_processed,
                        batch_quals_count: batch_quals.len(),
                        where_quals_null: where_quals.is_null(),
                        segments_metadata_resolved: 0,
                        segments_decompressed: 0,
                        regex_cache_size: 0,
                        regex_cache_calls: 0,
                        topn_limit: topn_limit as u64,
                        topn_sort_col: topn_sort_col as i64,
                        topn_ascending,
                        pre_topn_groups: pre_topn_groups as u64,
                        merge_us: 0,
                        finalize_us,
                        topn_select_us: spec_fail_us,
                        n_workers: n_workers as u64,
                        bare_limit: 0, wall_us: t_wall.elapsed().as_micros() as u64, buf_stats: super::segments::take_scan_buf_stats(), f8_preselected: 0, pscan: std::ptr::null_mut(), is_parallel_worker: false, exec_ctx: None,
                    };

                    let state_box = Box::new(state);
                    let state_ptr = Box::into_raw(state_box);
                    (*node).custom_ps = state_ptr as *mut pg_sys::List;
                    return;
                }
            } // end else (phase2 cost guard)
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

                let mut bare_cd_sidecar = CountDistinctSideCar::new(&agg_specs);
                let mut result_rows = Vec::with_capacity(n);
                for &packed_key in &target_keys {
                    let global_idx = storage.alloc_group();
                    bare_cd_sidecar.alloc_group();

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
                                    CompactAccKind::MinInt => {
                                        let (w_val, w_has) = result.compact_storage.read_min_max_int(worker_idx, slot_idx);
                                        if w_has { storage.update_min_int(global_idx, slot_idx, w_val); }
                                    }
                                    CompactAccKind::MaxInt => {
                                        let (w_val, w_has) = result.compact_storage.read_min_max_int(worker_idx, slot_idx);
                                        if w_has { storage.update_max_int(global_idx, slot_idx, w_val); }
                                    }
                                    CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                                        bare_cd_sidecar.union_from(slot_idx, global_idx, &result.cd_sidecar, worker_idx);
                                    }
                                }
                            }
                        }
                    }

                    // Write CountDistinct counts for this group
                    for e in &bare_cd_sidecar.entries {
                        let count = e.count(global_idx);
                        *storage.count_mut(global_idx, e.spec_idx) = count;
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
                            OutputEntry::DerivedGroup { base_gi, delta } => {
                                let v = keys[*base_gi] + delta;
                                row.push((pg_sys::Datum::from(v as usize), false));
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
                    detoast_us: total_detoast_us,
                    blob_cache_hits: total_cache_hits,
                    blob_cache_misses: total_cache_misses,
                    blob_cache_bytes_served: total_cache_bytes_served,
                    decompress_us,
                    agg_us,
                    total_segments,
                    total_rows_processed,
                    batch_quals_count: batch_quals.len(),
                    where_quals_null: where_quals.is_null(),
                    segments_metadata_resolved: 0,
                    segments_decompressed: 0,
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
                    wall_us: t_wall.elapsed().as_micros() as u64,
                    buf_stats: super::segments::take_scan_buf_stats(),
                    // Compact (int-only) path doesn't wire F8 today.
                    f8_preselected: 0, pscan: std::ptr::null_mut(), is_parallel_worker: false, exec_ctx: None,
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
            if topn_limit > 0 {
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
                #[allow(clippy::type_complexity)]
                let partition_results: Vec<(CompactAccStorage, Vec<(i64, u128, u32)>)> =
                    std::thread::scope(|s| {
                    let workers = &partial_results;
                    let specs = &agg_specs;
                    let np = n_partitions;
                    let ascending = topn_ascending;
                    let hfilters = &having_filters;

                    let handles: Vec<_> = (0..np).map(|p| {
                        s.spawn(move || {
                            let layout = CompactAccLayout::new(specs);
                            let n_slots = layout.slots.len();
                            let mut map: CompactGroupMap =
                                CompactGroupMap::with_hasher(Default::default());
                            let mut storage = CompactAccStorage::new(layout);
                            let mut cd_sidecar = CountDistinctSideCar::new(specs);

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
                                                    let w_str = worker.compact_storage.str_arena.get(w_off, w_len);
                                                    let (g_off, g_len) = storage.read_min_max_str(gidx, slot_idx);
                                                    let should_update = if g_off == u32::MAX {
                                                        true
                                                    } else {
                                                        let g_str = storage.str_arena.get(g_off, g_len);
                                                        let cmp = strcoll_cmp(w_str, g_str);
                                                        match kind {
                                                            CompactAccKind::MinStr => cmp == std::cmp::Ordering::Less,
                                                            _ => cmp == std::cmp::Ordering::Greater,
                                                        }
                                                    };
                                                    if should_update {
                                                        let w_str = worker.compact_storage.str_arena.get(w_off, w_len);
                                                        let (new_off, new_len) = storage.str_arena.alloc(w_str);
                                                        storage.write_min_max_str(gidx, slot_idx, new_off, new_len);
                                                    }
                                                }
                                            }
                                            CompactAccKind::MinInt => {
                                                let (w_val, w_has) = worker.compact_storage.read_min_max_int(wgidx, slot_idx);
                                                if w_has { storage.update_min_int(gidx, slot_idx, w_val); }
                                            }
                                            CompactAccKind::MaxInt => {
                                                let (w_val, w_has) = worker.compact_storage.read_min_max_int(wgidx, slot_idx);
                                                if w_has { storage.update_max_int(gidx, slot_idx, w_val); }
                                            }
                                            CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                                                cd_sidecar.union_from(slot_idx, gidx, &worker.cd_sidecar, wgidx);
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
                                        CompactAccKind::SumIntNarrow => storage.read_sum_int_narrow(gidx, sort_slot).0,
                                        CompactAccKind::MinInt | CompactAccKind::MaxInt => {
                                            storage.read_min_max_int(gidx, sort_slot).0
                                        }
                                        _ => storage.read_count(gidx, sort_slot),
                                    }
                                }
                            };

                            let having_read_val = |gidx: u32, slot: usize| -> i64 {
                                let (_, kind) = storage.layout.slots[slot];
                                match kind {
                                    CompactAccKind::Count => storage.read_count(gidx, slot),
                                    CompactAccKind::SumIntNarrow => storage.read_sum_int_narrow(gidx, slot).0,
                                    _ => storage.read_count(gidx, slot),
                                }
                            };

                            let winners: Vec<(i64, u128, u32)> = if ascending {
                                // Keep smallest N: max-heap evicts largest
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
                                        if !ok { passes = false; break; }
                                    }
                                    if !passes { continue; }
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
                                        if !ok { passes = false; break; }
                                    }
                                    if !passes { continue; }
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
                    all_candidates.sort_unstable_by_key(|a| a.0);
                } else {
                    all_candidates.sort_unstable_by_key(|b| std::cmp::Reverse(b.0));
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
                            OutputEntry::DerivedGroup { base_gi, delta } => {
                                let v = keys[*base_gi] + delta;
                                row.push((pg_sys::Datum::from(v as usize), false));
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
                    detoast_us: total_detoast_us,
                    blob_cache_hits: total_cache_hits,
                    blob_cache_misses: total_cache_misses,
                    blob_cache_bytes_served: total_cache_bytes_served,
                    decompress_us,
                    agg_us,
                    total_segments,
                    total_rows_processed,
                    batch_quals_count: batch_quals.len(),
                    where_quals_null: where_quals.is_null(),
                    segments_metadata_resolved: 0,
                    segments_decompressed: 0,
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
                    bare_limit: 0, wall_us: t_wall.elapsed().as_micros() as u64, buf_stats: super::segments::take_scan_buf_stats(), f8_preselected: 0, pscan: std::ptr::null_mut(), is_parallel_worker: false, exec_ctx: None,
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
            let mut global_cd_sidecar = largest.cd_sidecar;

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
                    &mut global_cd_sidecar,
                    &result.compact_map,
                    &result.compact_storage,
                    &result.cd_sidecar,
                    &agg_specs,
                );
            }
            // Write merged CD counts back to storage
            global_cd_sidecar.write_counts_to_storage(storage, &compact_group_map);
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
                            OutputEntry::DerivedGroup { base_gi, delta } => {
                                let v = keys[*base_gi] + delta;
                                row.push((pg_sys::Datum::from(v as usize), false));
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
                detoast_us: total_detoast_us,
                blob_cache_hits: total_cache_hits,
                blob_cache_misses: total_cache_misses,
                blob_cache_bytes_served: total_cache_bytes_served,
                decompress_us,
                agg_us,
                total_segments,
                total_rows_processed,
                batch_quals_count: batch_quals.len(),
                where_quals_null: where_quals.is_null(),
                segments_metadata_resolved: 0,
                segments_decompressed: 0,
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
                bare_limit: 0, wall_us: t_wall.elapsed().as_micros() as u64, buf_stats: super::segments::take_scan_buf_stats(), f8_preselected: 0, pscan: std::ptr::null_mut(), is_parallel_worker: false, exec_ctx: None,
            };

            let state_box = Box::new(state);
            let state_ptr = Box::into_raw(state_box);
            (*node).custom_ps = state_ptr as *mut pg_sys::List;
            return;
        }

        // ============================================================
        // PARALLEL MIXED PATH: multi-threaded with string GROUP BY
        // ============================================================
        // Try to compile regexp patterns with Rust regex for thread-safe parallel execution
        let mut rust_regex_infos: Vec<RustRegexInfo> = Vec::new();
        if has_regexp_group {
            let regexp_count = group_specs.iter()
                .filter(|gs| matches!(gs.expr, GroupByExpr::RegexpReplace { .. }))
                .count();
            for gs in group_specs.iter() {
                if let GroupByExpr::RegexpReplace { ref pattern, ref replacement, .. } = gs.expr
                    && let Some(compiled) = try_compile_rust_regex(pattern) {
                    let rust_replacement = convert_pg_replacement(replacement);
                    rust_regex_infos.push(RustRegexInfo {
                        regex: compiled,
                        replacement: rust_replacement,
                        col_idx: gs.col_idx as usize,
                    });
                }
            }
            if rust_regex_infos.len() != regexp_count {
                rust_regex_infos.clear(); // all-or-nothing: if any failed, fall back entirely
            }
        }
        let all_regexp_compiled = !has_regexp_group || !rust_regex_infos.is_empty();

        let mut mixed_col_not_null = meta.col_not_null.clone();
        mixed_col_not_null.resize(meta.col_names.len(), false);
        for gs in &group_specs {
            if !matches!(gs.expr, GroupByExpr::Extract { .. })
                || gs.col_idx < 0
                || (gs.col_idx as usize) >= meta.col_names.len()
            {
                continue;
            }
            let col_idx = gs.col_idx as usize;
            let col_name = &meta.col_names[col_idx];
            if all_segments.iter().all(|seg| {
                seg.col_sums
                    .get(col_name)
                    .is_some_and(|cs| cs.nonnull_count == seg.row_count as i64)
            }) {
                mixed_col_not_null[col_idx] = true;
            }
        }

        let can_parallel_mixed_flag = !can_parallel
            && has_group_by
            && (n_workers > 1 || derived_minmax_topn.is_some())
            && all_segments.len() > 1
            && all_regexp_compiled
            && can_parallel_mixed(&group_specs, &needed_cols, &meta.col_types, &mixed_col_not_null, &batch_quals, &agg_specs);

        if can_parallel_mixed_flag {
            let t2 = Instant::now();
            // For derived MIN/MAX-difference top-N, workers don't maintain a
            // direct-sort heap — sort key is recovered at the leader from
            // partial max/min, see the `derived_minmax_topn` branch below.
            let topn_spec = if topn_limit > 0
                && having_filters.is_empty()
                && derived_minmax_topn.is_none()
            {
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
            let mut text_group_col_flags: Vec<bool> = (0..meta.col_names.len()).map(|i| {
                group_specs.iter().any(|gs| gs.col_idx as usize == i && is_text_group_col(gs))
            }).collect();
            // CaseWhen ColumnRef results reference text columns that need decompression
            for gs in &group_specs {
                if let GroupByExpr::CaseWhen(ref spec) = gs.expr {
                    for clause in &spec.clauses {
                        if let CaseWhenValue::ColumnRef(ci) = &clause.result
                            && *ci < text_group_col_flags.len() {
                            text_group_col_flags[*ci] = true;
                        }
                    }
                    if let CaseWhenValue::ColumnRef(ci) = &spec.default
                        && *ci < text_group_col_flags.len() {
                        text_group_col_flags[*ci] = true;
                    }
                }
            }

            // Build text qual infos for worker threads.
            // Order: positive LIKE first (most selective — match a pattern),
            // then EQ/NE, then NOT LIKE (negated patterns pass most rows).
            // This maximizes short-circuit benefit when filters AND into selection.
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
                TextQualInfo::Like { negate: false, .. } => 2,  // positive LIKE
                TextQualInfo::Like { negate: true, .. } => 3,   // NOT LIKE
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
            let use_pipeline = use_lazy
                && all_segments.len() >= n_workers * 16
                && !batch_quals.is_empty();
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
                    for seg in &mut all_segments {
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
            let has_case_when_group = group_specs.iter()
                .any(|gs| matches!(gs.expr, GroupByExpr::CaseWhen(_)));
            let has_regex_group = group_specs.iter()
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
                    &all_segments,
                    &group_specs,
                    &meta.col_names,
                    &meta.col_types,
                    &meta.segment_by,
                    &needed_cols,
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
            let dict_distinct_remaps = build_dict_distinct_remaps(&all_segments, &agg_specs);

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
                rust_regex_infos: &rust_regex_infos,
                sidecar_only_cols: &sidecar_only_cols,
                preselected_keys: preselected_keys.as_ref(),
                dict_distinct_remaps: &dict_distinct_remaps,
            };

            let mut pipeline_detoast_us: u64 = 0;
            let partial_results: Vec<ParallelMixedResult> = if use_pipeline {
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
                        let handles: Vec<_> = current_batch.chunks(chunk_size).enumerate().map(|(ci, chunk)| {
                            let cfg = &config;
                            // Phase D: chunk_offset is the seg_idx of chunk[0]
                            // in the leader's all_segments view, used to index
                            // dict_distinct_remaps.per_segment.
                            let chunk_offset = batch_start + ci * chunk_size;
                            s.spawn(move || process_segments_mixed(chunk, chunk_offset, cfg))
                        }).collect();

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
                    let handles: Vec<_> = all_segments.chunks(chunk_size).enumerate().map(|(ci, chunk)| {
                        let cfg = &config;
                        let chunk_offset = ci * chunk_size;
                        s.spawn(move || process_segments_mixed(chunk, chunk_offset, cfg))
                    }).collect();
                    handles.into_iter().map(|h| h.join().unwrap()).collect()
                })
            };

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

            // ----------------------------------------------------------
            // Derived MIN/MAX-difference top-N (JSONBench Q4 shape):
            // sort by `storage[max_slot] - storage[min_slot]`. Workers
            // have produced partial MAX/MIN per group; one pass over all
            // (worker, hash) pairs combines partials into per-key
            // global MAX/MIN, applies a top-K heap on `max - min`, and
            // then merges *only the K winners'* full accumulators.
            // Skips the full 1.35M-group merge + finalize that the
            // direct-aggregate paths below would otherwise do.
            // ----------------------------------------------------------
            if let Some((max_slot, min_slot)) = derived_minmax_topn {
                let t_merge = Instant::now();
                let limit = topn_limit as usize;
                let ascending = topn_ascending;

                // Step 1+2: partition worker-local groups by hash, merge
                // each partition's MAX/MIN in parallel, and keep only local
                // top-K candidates. The old implementation built one large
                // leader HashMap and then scanned it; Q4 spends hundreds of
                // milliseconds there with ~1.35M distinct users.
                let n_partitions = n_workers.max(1);
                let mut buckets: Vec<Vec<(usize, u128, u32)>> =
                    (0..n_partitions).map(|_| Vec::new()).collect();
                for (wi, result) in partial_results.iter().enumerate() {
                    for (&hash_key, &wgidx) in &result.compact_map {
                        let p = (((hash_key as u64) ^ ((hash_key >> 64) as u64)) as usize)
                            % n_partitions;
                        buckets[p].push((wi, hash_key, wgidx));
                    }
                }

                let partition_results: Vec<(usize, Vec<(i128, u128)>)> =
                    std::thread::scope(|s| {
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
                                        let (max_val, max_has) = worker
                                            .compact_storage
                                            .read_min_max_int(wgidx, max_slot);
                                        let (min_val, min_has) = worker
                                            .compact_storage
                                            .read_min_max_int(wgidx, min_slot);
                                        let entry = per_key
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
                                            let derived =
                                                (max_val as i128).saturating_sub(min_val as i128);
                                            heap.push((derived, hash_key));
                                            if heap.len() > limit {
                                                heap.pop();
                                            }
                                        }
                                        (unique_count, heap.into_vec())
                                    } else {
                                        let mut heap: std::collections::BinaryHeap<
                                            Reverse<(i128, u128)>,
                                        > = std::collections::BinaryHeap::with_capacity(limit + 1);
                                        for (&hash_key, &(max_val, min_val, seen)) in &per_key {
                                            if !seen {
                                                continue;
                                            }
                                            let derived =
                                                (max_val as i128).saturating_sub(min_val as i128);
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
                let storage = compact_storage.as_mut().unwrap();
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
                                    CompactAccKind::MinInt => {
                                        let (w_val, w_has) = result.compact_storage.read_min_max_int(worker_idx, slot_idx);
                                        if w_has { storage.update_min_int(global_idx, slot_idx, w_val); }
                                    }
                                    CompactAccKind::MaxInt => {
                                        let (w_val, w_has) = result.compact_storage.read_min_max_int(worker_idx, slot_idx);
                                        if w_has { storage.update_max_int(global_idx, slot_idx, w_val); }
                                    }
                                    CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                                        spec_cd_sidecar.union_from(slot_idx, global_idx, &result.cd_sidecar, worker_idx);
                                    }
                                }
                            }
                        }
                    }

                    for e in &spec_cd_sidecar.entries {
                        let count = e.count(global_idx);
                        *storage.count_mut(global_idx, e.spec_idx) = count;
                    }

                    let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                    for (spec_idx, spec) in agg_specs.iter().enumerate() {
                        agg_results.push(compact_finalize(storage, global_idx, spec_idx, spec));
                    }

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
                            OutputEntry::DerivedGroup { base_gi, delta } => {
                                match mixed_ks.get(source_gidx, *base_gi) {
                                    MixedKeyVal::Int(v) => row.push((pg_sys::Datum::from((v + delta) as usize), false)),
                                    _ => row.push((pg_sys::Datum::from(0usize), true)),
                                }
                            }
                            OutputEntry::Const(d, n) => row.push((*d, *n)),
                        }
                    }
                    result_rows.push(row);
                }
                let finalize_us = t_fin.elapsed().as_micros() as u64;

                let state = AggScanState {
                    _agg_specs: agg_specs,
                    _group_specs: group_specs,
                    result_rows,
                    result_idx: 0,
                    _num_result_cols: num_result_cols,
                    metadata_us,
                    heap_scan_us,
                    detoast_us: total_detoast_us,
                    blob_cache_hits: total_cache_hits,
                    blob_cache_misses: total_cache_misses,
                    blob_cache_bytes_served: total_cache_bytes_served,
                    decompress_us,
                    agg_us,
                    total_segments,
                    total_rows_processed,
                    batch_quals_count: batch_quals.len(),
                    where_quals_null: where_quals.is_null(),
                    segments_metadata_resolved: 0,
                    segments_decompressed: 0,
                    regex_cache_size: 0,
                    regex_cache_calls: 0,
                    topn_limit: topn_limit as u64,
                    topn_sort_col: -3, // derived sentinel — see explain.rs
                    topn_ascending,
                    pre_topn_groups: pre_topn_groups as u64,
                    merge_us: 0,
                    finalize_us,
                    topn_select_us,
                    n_workers: n_workers as u64,
                    bare_limit: 0,
                    wall_us: t_wall.elapsed().as_micros() as u64,
                    buf_stats: super::segments::take_scan_buf_stats(),
                    f8_preselected: 0,
                    pscan: std::ptr::null_mut(),
                    is_parallel_worker: false,
                    exec_ctx: None,
                };

                let state_box = Box::new(state);
                let state_ptr = Box::into_raw(state_box);
                (*node).custom_ps = state_ptr as *mut pg_sys::List;
                return;
            }

            // ----------------------------------------------------------
            // Speculative top-N: merge-skip using pre-computed top-K
            // ----------------------------------------------------------
            // CountDistinct sort values can't be summed across workers for speculative
            // top-N (worker counts overestimate merged count due to set overlap).
            let sort_slot_for_spec = match output_map[topn_sort_col] {
                OutputEntry::Agg(ai) => ai,
                _ => 0,
            };
            let sort_is_cd = topn_limit > 0 && matches!(
                compact_storage.as_ref().unwrap().layout.slots[sort_slot_for_spec].1,
                CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr
            );
            let sort_is_avg = topn_limit > 0 && agg_specs[sort_slot_for_spec].agg_type == AggType::Avg;
            if topn_limit > 0 && having_filters.is_empty() && !sort_is_cd && !sort_is_avg {
                let sort_slot = sort_slot_for_spec;
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
                    merged.sort_unstable_by_key(|b| std::cmp::Reverse(b.0));
                } else {
                    merged.sort_unstable_by_key(|a| a.0);
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
                    let mut spec_cd_sidecar = CountDistinctSideCar::new(&agg_specs);

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
                                        CompactAccKind::MinInt => {
                                            let (w_val, w_has) = result.compact_storage.read_min_max_int(worker_idx, slot_idx);
                                            if w_has { storage.update_min_int(global_idx, slot_idx, w_val); }
                                        }
                                        CompactAccKind::MaxInt => {
                                            let (w_val, w_has) = result.compact_storage.read_min_max_int(worker_idx, slot_idx);
                                            if w_has { storage.update_max_int(global_idx, slot_idx, w_val); }
                                        }
                                        CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                                            spec_cd_sidecar.union_from(slot_idx, global_idx, &result.cd_sidecar, worker_idx);
                                        }
                                    }
                                }
                            }
                        }

                        // Write CountDistinct counts for this group
                        for e in &spec_cd_sidecar.entries {
                            let count = e.count(global_idx);
                            *storage.count_mut(global_idx, e.spec_idx) = count;
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
                                OutputEntry::DerivedGroup { base_gi, delta } => {
                                    match mixed_ks.get(source_gidx, *base_gi) {
                                        MixedKeyVal::Int(v) => row.push((pg_sys::Datum::from((v + delta) as usize), false)),
                                        _ => row.push((pg_sys::Datum::from(0usize), true)),
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
                        detoast_us: total_detoast_us,
                        blob_cache_hits: total_cache_hits,
                        blob_cache_misses: total_cache_misses,
                        blob_cache_bytes_served: total_cache_bytes_served,
                        decompress_us,
                        agg_us,
                        total_segments,
                        total_rows_processed,
                        batch_quals_count: batch_quals.len(),
                        where_quals_null: where_quals.is_null(),
                        segments_metadata_resolved: 0,
                        segments_decompressed: 0,
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
                        bare_limit: 0, wall_us: t_wall.elapsed().as_micros() as u64, buf_stats: super::segments::take_scan_buf_stats(), f8_preselected: 0, pscan: std::ptr::null_mut(), is_parallel_worker: false, exec_ctx: None,
                    };

                    let state_box = Box::new(state);
                    let state_ptr = Box::into_raw(state_box);
                    (*node).custom_ps = state_ptr as *mut pg_sys::List;
                    return;
                }
                // Speculation failed — check if all tied
                let nth_value = merged.get(limit.saturating_sub(1)).map(|x| x.0).unwrap_or(0);
                let all_tied = merged.len() >= limit
                    && merged.iter().all(|&(v, _)| v == nth_value);

                pgrx::log!(
                    "pg_deltax mixed speculative top-N failed: candidates={} k={} floor_sum={} all_tied={}",
                    merged.len(), k, floor_sum, all_tied,
                );

                if all_tied {
                    merged.truncate(limit);

                    let t_fin = Instant::now();
                    let storage = compact_storage.as_mut().unwrap();
                    let mut result_rows = Vec::with_capacity(merged.len());
                    let mut spec_cd_sidecar = CountDistinctSideCar::new(&agg_specs);

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
                                        CompactAccKind::MinInt => {
                                            let (w_val, w_has) = result.compact_storage.read_min_max_int(worker_idx, slot_idx);
                                            if w_has { storage.update_min_int(global_idx, slot_idx, w_val); }
                                        }
                                        CompactAccKind::MaxInt => {
                                            let (w_val, w_has) = result.compact_storage.read_min_max_int(worker_idx, slot_idx);
                                            if w_has { storage.update_max_int(global_idx, slot_idx, w_val); }
                                        }
                                        CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                                            spec_cd_sidecar.union_from(slot_idx, global_idx, &result.cd_sidecar, worker_idx);
                                        }
                                    }
                                }
                            }
                        }

                        for e in &spec_cd_sidecar.entries {
                            let count = e.count(global_idx);
                            *storage.count_mut(global_idx, e.spec_idx) = count;
                        }

                        let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                        for (spec_idx, spec) in agg_specs.iter().enumerate() {
                            agg_results.push(compact_finalize(storage, global_idx, spec_idx, spec));
                        }

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
                                OutputEntry::DerivedGroup { base_gi, delta } => {
                                    match mixed_ks.get(source_gidx, *base_gi) {
                                        MixedKeyVal::Int(v) => row.push((pg_sys::Datum::from((v + delta) as usize), false)),
                                        _ => row.push((pg_sys::Datum::from(0usize), true)),
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
                        detoast_us: total_detoast_us,
                        blob_cache_hits: total_cache_hits,
                        blob_cache_misses: total_cache_misses,
                        blob_cache_bytes_served: total_cache_bytes_served,
                        decompress_us,
                        agg_us,
                        total_segments,
                        total_rows_processed,
                        batch_quals_count: batch_quals.len(),
                        where_quals_null: where_quals.is_null(),
                        segments_metadata_resolved: 0,
                        segments_decompressed: 0,
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
                        bare_limit: 0, wall_us: t_wall.elapsed().as_micros() as u64, buf_stats: super::segments::take_scan_buf_stats(), f8_preselected: 0, pscan: std::ptr::null_mut(), is_parallel_worker: false, exec_ctx: None,
                    };

                    let state_box = Box::new(state);
                    let state_ptr = Box::into_raw(state_box);
                    (*node).custom_ps = state_ptr as *mut pg_sys::List;
                    return;
                }
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
                                    CompactAccKind::MinInt => {
                                        let (w_val, w_has) = result.compact_storage.read_min_max_int(worker_gidx, slot_idx);
                                        if w_has { final_storage.update_min_int(group_idx, slot_idx, w_val); }
                                    }
                                    CompactAccKind::MaxInt => {
                                        let (w_val, w_has) = result.compact_storage.read_min_max_int(worker_gidx, slot_idx);
                                        if w_has { final_storage.update_max_int(group_idx, slot_idx, w_val); }
                                    }
                                    CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                                        final_cd_sidecar.union_from(slot_idx, group_idx, &result.cd_sidecar, worker_gidx);
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
                            *final_storage.count_mut(group_idx, e.spec_idx) = count;
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
                            OutputEntry::DerivedGroup { base_gi, delta } => {
                                match final_mixed_keys.get(group_idx, *base_gi) {
                                    MixedKeyVal::Int(v) => row.push((pg_sys::Datum::from((v + delta) as usize), false)),
                                    _ => row.push((pg_sys::Datum::from(0usize), true)),
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
                    detoast_us: total_detoast_us,
                    blob_cache_hits: total_cache_hits,
                    blob_cache_misses: total_cache_misses,
                    blob_cache_bytes_served: total_cache_bytes_served,
                    decompress_us,
                    agg_us,
                    total_segments,
                    total_rows_processed,
                    batch_quals_count: batch_quals.len(),
                    where_quals_null: where_quals.is_null(),
                    segments_metadata_resolved: 0,
                    segments_decompressed: 0,
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
                    wall_us: t_wall.elapsed().as_micros() as u64,
                    buf_stats: super::segments::take_scan_buf_stats(),
                    f8_preselected: preselected_keys.as_ref()
                        .map(|s| s.len() as u64).unwrap_or(0),
                    pscan: std::ptr::null_mut(),
                    is_parallel_worker: false, exec_ctx: None,
                };

                let state_box = Box::new(state);
                let state_ptr = Box::into_raw(state_box);
                (*node).custom_ps = state_ptr as *mut pg_sys::List;
                return;
            }

            // ----------------------------------------------------------
            // Partitioned parallel merge + top-N for mixed path:
            // partition key space across threads, each merges its slice
            // and finds local top-N, then merge the local results.
            // ----------------------------------------------------------
            if topn_limit > 0 {
                let t_merge = Instant::now();
                let limit = topn_limit as usize;
                let sort_slot = match output_map[topn_sort_col] {
                    OutputEntry::Agg(ai) => ai,
                    _ => unreachable!(),
                };
                let n_partitions = n_workers;
                let n_group_cols = group_specs.len();

                let pre_topn_groups: usize = partial_results.iter()
                    .map(|r| r.compact_map.len()).sum();

                // Each partition thread: merge its slice, find local top-N,
                // copy winners to mini storage + mini mixed keys, drop the rest.
                #[allow(clippy::type_complexity)]
                let partition_results: Vec<(CompactAccStorage, MixedKeyStorage, Vec<(i64, u128, u32)>)> =
                    std::thread::scope(|s| {
                    let workers = &partial_results;
                    let specs = &agg_specs;
                    let np = n_partitions;
                    let ascending = topn_ascending;
                    let ngk = n_group_cols;
                    let hfilters = &having_filters;

                    let handles: Vec<_> = (0..np).map(|p| {
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
                                                    let w_str = worker.compact_storage.str_arena.get(w_off, w_len);
                                                    let (g_off, g_len) = storage.read_min_max_str(gidx, slot_idx);
                                                    let should_update = if g_off == u32::MAX {
                                                        true
                                                    } else {
                                                        let g_str = storage.str_arena.get(g_off, g_len);
                                                        let cmp = strcoll_cmp(w_str, g_str);
                                                        match kind {
                                                            CompactAccKind::MinStr => cmp == std::cmp::Ordering::Less,
                                                            _ => cmp == std::cmp::Ordering::Greater,
                                                        }
                                                    };
                                                    if should_update {
                                                        let w_str = worker.compact_storage.str_arena.get(w_off, w_len);
                                                        let (new_off, new_len) = storage.str_arena.alloc(w_str);
                                                        storage.write_min_max_str(gidx, slot_idx, new_off, new_len);
                                                    }
                                                }
                                            }
                                            CompactAccKind::MinInt => {
                                                let (w_val, w_has) = worker.compact_storage.read_min_max_int(wgidx, slot_idx);
                                                if w_has { storage.update_min_int(gidx, slot_idx, w_val); }
                                            }
                                            CompactAccKind::MaxInt => {
                                                let (w_val, w_has) = worker.compact_storage.read_min_max_int(wgidx, slot_idx);
                                                if w_has { storage.update_max_int(gidx, slot_idx, w_val); }
                                            }
                                            CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                                                cd_sidecar.union_from(slot_idx, gidx, &worker.cd_sidecar, wgidx);
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
                                        CompactAccKind::SumIntNarrow => storage.read_sum_int_narrow(gidx, sort_slot).0,
                                        _ => storage.read_count(gidx, sort_slot),
                                    }
                                }
                            };

                            let having_read_val = |gidx: u32, slot: usize| -> i64 {
                                let (_, kind) = storage.layout.slots[slot];
                                match kind {
                                    CompactAccKind::Count => storage.read_count(gidx, slot),
                                    CompactAccKind::SumIntNarrow => storage.read_sum_int_narrow(gidx, slot).0,
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
                                        if !ok { passes = false; break; }
                                    }
                                    if !passes { continue; }
                                    let val = read_val(gidx);
                                    heap.push((val, key, gidx));
                                    if heap.len() > limit { heap.pop(); }
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
                                        if !ok { passes = false; break; }
                                    }
                                    if !passes { continue; }
                                    let val = read_val(gidx);
                                    heap.push(Reverse((val, key, gidx)));
                                    if heap.len() > limit { heap.pop(); }
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
                                    if kind == CompactAccKind::MinStr || kind == CompactAccKind::MaxStr {
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
                    }).collect();

                    handles.into_iter().map(|h| h.join().unwrap()).collect()
                });

                drop(partial_results);

                let merge_us = t_merge.elapsed().as_micros() as u64;

                // Merge all partition top entries, select global top-N
                let t_finalize = Instant::now();
                let mut all_candidates: Vec<(i64, u128, u32, usize)> = Vec::new();
                for (pi, (_, _, entries)) in partition_results.iter().enumerate() {
                    for &(sort_val, key, gidx) in entries {
                        all_candidates.push((sort_val, key, gidx, pi));
                    }
                }
                if topn_ascending {
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
                    let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
                    for entry in &output_map {
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
                                    MixedKeyVal::Int(v) => row.push((pg_sys::Datum::from((v + delta) as usize), false)),
                                    _ => row.push((pg_sys::Datum::from(0usize), true)),
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
                    detoast_us: total_detoast_us,
                    blob_cache_hits: total_cache_hits,
                    blob_cache_misses: total_cache_misses,
                    blob_cache_bytes_served: total_cache_bytes_served,
                    decompress_us,
                    agg_us,
                    total_segments,
                    total_rows_processed,
                    batch_quals_count: batch_quals.len(),
                    where_quals_null: where_quals.is_null(),
                    segments_metadata_resolved: 0,
                    segments_decompressed: 0,
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
                    bare_limit: 0, wall_us: t_wall.elapsed().as_micros() as u64, buf_stats: super::segments::take_scan_buf_stats(), f8_preselected: 0, pscan: std::ptr::null_mut(), is_parallel_worker: false, exec_ctx: None,
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
            let mut merged_cd_sidecar = largest.cd_sidecar;

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
                            CompactAccKind::MinInt => {
                                let (w_val, w_has) = result.compact_storage.read_min_max_int(worker_group_idx, slot_idx);
                                if w_has { storage.update_min_int(global_group_idx, slot_idx, w_val); }
                            }
                            CompactAccKind::MaxInt => {
                                let (w_val, w_has) = result.compact_storage.read_min_max_int(worker_group_idx, slot_idx);
                                if w_has { storage.update_max_int(global_group_idx, slot_idx, w_val); }
                            }
                            CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                                merged_cd_sidecar.union_from(slot_idx, global_group_idx, &result.cd_sidecar, worker_group_idx);
                            }
                        }
                    }
                }
            }
            // Write final CountDistinct counts into compact storage
            if !merged_cd_sidecar.is_empty() {
                merged_cd_sidecar.write_counts_to_storage(storage, &compact_group_map);
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
                        OutputEntry::DerivedGroup { base_gi, delta } => {
                            match mixed_ks.get(group_idx, *base_gi) {
                                MixedKeyVal::Int(v) => row.push((pg_sys::Datum::from((v + delta) as usize), false)),
                                _ => row.push((pg_sys::Datum::from(0usize), true)),
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
                    agg_specs[sort_slot].agg_type == AggType::Avg,
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
                            OutputEntry::DerivedGroup { base_gi, delta } => {
                                match merged_mixed_keys.get(group_idx, *base_gi) {
                                    MixedKeyVal::Int(v) => row.push((pg_sys::Datum::from((v + delta) as usize), false)),
                                    _ => row.push((pg_sys::Datum::from(0usize), true)),
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
                detoast_us: total_detoast_us,
                blob_cache_hits: total_cache_hits,
                blob_cache_misses: total_cache_misses,
                blob_cache_bytes_served: total_cache_bytes_served,
                decompress_us,
                agg_us,
                total_segments,
                total_rows_processed,
                batch_quals_count: batch_quals.len(),
                where_quals_null: where_quals.is_null(),
                segments_metadata_resolved: 0,
                segments_decompressed: 0,
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
                bare_limit: 0, wall_us: t_wall.elapsed().as_micros() as u64, buf_stats: super::segments::take_scan_buf_stats(), f8_preselected: 0, pscan: std::ptr::null_mut(), is_parallel_worker: false, exec_ctx: None,
            };

            let state_box = Box::new(state);
            let state_ptr = Box::into_raw(state_box);
            (*node).custom_ps = state_ptr as *mut pg_sys::List;
            return;
        }

        // ============================================================
        // PARALLEL COUNT(DISTINCT) PATH: no GROUP BY, all aggs are
        // CountDistinct — parallelize by splitting segments across
        // threads, each builds local HashSets, then merge.
        // ============================================================
        let all_count_distinct = !has_group_by
            && n_workers > 1
            && all_segments.len() > 1
            && batch_quals.is_empty()
            && !agg_specs.is_empty()
            && agg_specs.iter().all(|s| s.agg_type == AggType::CountDistinct);

        if all_count_distinct {
            let t2 = Instant::now();

            struct ParallelCdConfig<'a> {
                agg_specs: &'a [AggExecSpec],
                col_names: &'a [String],
                col_types: &'a [pg_sys::Oid],
                segment_by: &'a [String],
                needed_cols: &'a [bool],
                seg_filters: &'a [(usize, String)],
                time_min: Option<i64>,
                time_max: Option<i64>,
                count_distinct_only_str: &'a [bool],
                count_distinct_only_int: &'a [bool],
            }
            // SAFETY: contains only references to data that outlives the thread scope
            unsafe impl Send for ParallelCdConfig<'_> {}
            unsafe impl Sync for ParallelCdConfig<'_> {}

            struct ParallelCdResult {
                int_sets: Vec<CdSetInt>,
                str_sets: Vec<CdSetStr>,
                segments_processed: u64,
            }

            let config = ParallelCdConfig {
                agg_specs: &agg_specs,
                col_names: &meta.col_names,
                col_types: &meta.col_types,
                segment_by: &meta.segment_by,
                needed_cols: &needed_cols,
                seg_filters: &seg_filters,
                time_min,
                time_max,
                count_distinct_only_str: &count_distinct_only_str,
                count_distinct_only_int: &count_distinct_only_int,
            };

            fn process_cd_segments(
                segments: &[SegmentData],
                config: &ParallelCdConfig,
            ) -> ParallelCdResult {
                let n_aggs = config.agg_specs.len();
                let mut int_sets: Vec<CdSetInt> = (0..n_aggs)
                    .map(|_| new_cd_set_int())
                    .collect();
                let mut str_sets: Vec<CdSetStr> = (0..n_aggs)
                    .map(|_| new_cd_set_str())
                    .collect();
                let mut segments_processed = 0u64;

                for seg in segments {
                    if seg.row_count == 0 { continue; }

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

                    segments_processed += 1;

                    // Process each needed column's compressed blob
                    let mut blob_idx = 0;
                    let mut _seg_val_idx = 0;
                    for (col_idx, col_name) in config.col_names.iter().enumerate() {
                        if !config.needed_cols[col_idx] {
                            if config.segment_by.contains(col_name) {
                                _seg_val_idx += 1;
                            } else {
                                blob_idx += 1;
                            }
                            continue;
                        }
                        if config.segment_by.contains(col_name) {
                            // Segment-by column: one value per segment, from segment_values
                            let spec_idx = config.agg_specs.iter().position(|s| s.col_idx as usize == col_idx);
                            if let (Some(si), Some(val)) = (spec_idx, &seg.segment_values[_seg_val_idx]) {
                                if config.count_distinct_only_str[col_idx] {
                                    str_sets[si].insert(hash128_str(val.as_bytes()));
                                } else if config.count_distinct_only_int[col_idx]
                                    && let Ok(v) = val.parse::<i64>()
                                {
                                    int_sets[si].insert(v);
                                }
                            }
                            _seg_val_idx += 1;
                            continue;
                        }

                        let blob = &seg.compressed_blobs[blob_idx];
                        let type_oid = config.col_types[col_idx];
                        blob_idx += 1;

                        // Find the agg spec for this column
                        let spec_idx = config.agg_specs.iter().position(|s| s.col_idx as usize == col_idx);
                        let spec_idx = match spec_idx {
                            Some(i) => i,
                            None => continue,
                        };

                        let cc_ref = compression::CompressedColumnRef::from_bytes(blob);
                        let non_null_count = count_non_null(cc_ref.null_bitmap, cc_ref.row_count as usize);
                        if non_null_count == 0 { continue; }

                        if config.count_distinct_only_str[col_idx] {
                            let seen = &mut str_sets[spec_idx];
                            match cc_ref.type_tag {
                                compression::CompressionType::Dictionary
                                | compression::CompressionType::DictionaryLz4 => {
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
                                        if len == 0 { has_empty = true; }
                                        else { seen.insert(hash128_str(&buf[off..off + len])); }
                                    }
                                    if has_empty { seen.insert(empty_hash); }
                                }
                                compression::CompressionType::Lz4Blocked => {
                                    let (buf, ranges) = compression::lz4::decode_to_ranges_blocked(cc_ref.data, non_null_count, None);
                                    let empty_hash = hash128_str(b"");
                                    let mut has_empty = false;
                                    for &(off, len) in &ranges {
                                        if len == 0 { has_empty = true; }
                                        else { seen.insert(hash128_str(&buf[off..off + len])); }
                                    }
                                    if has_empty { seen.insert(empty_hash); }
                                }
                                compression::CompressionType::Constant => {
                                    seen.insert(hash128_str(cc_ref.data));
                                }
                                _ => {}
                            }
                        } else if config.count_distinct_only_int[col_idx] {
                            let seen = &mut int_sets[spec_idx];
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
                }

                ParallelCdResult { int_sets, str_sets, segments_processed }
            }

            // Pipeline detoast with parallel processing
            let use_cd_pipeline = use_lazy && all_segments.len() >= n_workers * 16;
            if use_lazy {
                let t_detoast = Instant::now();
                if use_cd_pipeline {
                    let n_batches = (n_workers * 2).max(2).min(all_segments.len());
                    let batch_size = all_segments.len().div_ceil(n_batches);
                    let first_end = batch_size.min(all_segments.len());
                    for seg in &mut all_segments[..first_end] {
                        let dl = detoast_lazy_blobs(seg);
                        total_cache_hits += dl.cache_hits;
                        total_cache_misses += dl.cache_misses;
                        total_cache_bytes_served += dl.cache_bytes_served;
                    }
                } else {
                    for seg in &mut all_segments {
                        let dl = detoast_lazy_blobs(seg);
                        total_cache_hits += dl.cache_hits;
                        total_cache_misses += dl.cache_misses;
                        total_cache_bytes_served += dl.cache_bytes_served;
                    }
                }
                total_detoast_us += t_detoast.elapsed().as_micros() as u64;
            }

            let mut pipeline_detoast_us: u64 = 0;
            let partial_results: Vec<ParallelCdResult> = if use_cd_pipeline {
                let n_batches = (n_workers * 2).max(2).min(all_segments.len());
                let batch_size = all_segments.len().div_ceil(n_batches);
                let mut results: Vec<ParallelCdResult> = Vec::new();
                let mut batch_start = 0;
                let total_segs = all_segments.len();

                while batch_start < total_segs {
                    let batch_end = (batch_start + batch_size).min(total_segs);
                    let next_end = (batch_end + batch_size).min(total_segs);

                    let (done, pending) = all_segments.split_at_mut(batch_end);
                    let current_batch = &done[batch_start..];

                    std::thread::scope(|s| {
                        let chunk_size = current_batch.len().div_ceil(n_workers);
                        let handles: Vec<_> = current_batch.chunks(chunk_size).map(|chunk| {
                            let cfg = &config;
                            s.spawn(move || process_cd_segments(chunk, cfg))
                        }).collect();

                        // Main thread detoasts next batch while workers run
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
                    let handles: Vec<_> = all_segments.chunks(chunk_size).map(|chunk| {
                        s.spawn(|| process_cd_segments(chunk, &config))
                    }).collect();
                    handles.into_iter().map(|h| h.join().unwrap()).collect()
                })
            };

            let agg_us = t2.elapsed().as_micros() as u64;

            let mut total_segments = 0u64;
            for partial in &partial_results {
                total_segments += partial.segments_processed;
            }

            // Parallel partitioned merge of worker CD sets.
            //
            // Every path entering this block has `all_count_distinct == true`,
            // so each spec is a `CountDistinct` and we only need the final
            // `len()` — no need to materialize a global set. Partition the
            // output keyspace into `CD_MERGE_PARTITIONS` buckets by a
            // fixed-seed hash; each thread owns one bucket, walks every
            // worker's set, and inserts only values that route to it. Buckets
            // are disjoint → total distinct count = Σ bucket.len(). This
            // removes the single-threaded 2.5 s stall on Q4 (workers were
            // already parallel; the old final merge was not).
            const CD_MERGE_PARTITIONS: usize = 16;
            fn cd_part_int(v: i64) -> usize {
                // SplitMix64-style finalizer — cheap, well-distributed.
                let mut x = v as u64;
                x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
                x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
                x ^= x >> 31;
                (x >> 60) as usize & (CD_MERGE_PARTITIONS - 1)
            }
            fn cd_part_str(v: u128) -> usize {
                // u128 values are already SipHash-128 digests; top bits are
                // uniformly random.
                ((v >> 124) as usize) & (CD_MERGE_PARTITIONS - 1)
            }

            let t_merge = Instant::now();
            let n_specs = agg_specs.len();
            // Per-partition counts: bucket_counts[partition][spec] = i64 distinct.
            let partial_refs = &partial_results;
            let is_str: Vec<bool> = agg_specs.iter()
                .map(|s| matches!(s.col_type_oid,
                    pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID))
                .collect();
            let is_str_ref = &is_str;
            let bucket_counts: Vec<Vec<i64>> = std::thread::scope(|s| {
                let handles: Vec<_> = (0..CD_MERGE_PARTITIONS).map(|p| {
                    s.spawn(move || {
                        // Per-spec local disjoint set; only the one matching
                        // this spec's column type is used.
                        let mut local_int: Vec<CdSetInt> = (0..n_specs)
                            .map(|_| new_cd_set_int()).collect();
                        let mut local_str: Vec<CdSetStr> = (0..n_specs)
                            .map(|_| new_cd_set_str()).collect();
                        for partial in partial_refs {
                            for spec_idx in 0..n_specs {
                                if is_str_ref[spec_idx] {
                                    for &v in &partial.str_sets[spec_idx] {
                                        if cd_part_str(v) == p {
                                            local_str[spec_idx].insert(v);
                                        }
                                    }
                                } else {
                                    for &v in &partial.int_sets[spec_idx] {
                                        if cd_part_int(v) == p {
                                            local_int[spec_idx].insert(v);
                                        }
                                    }
                                }
                            }
                        }
                        (0..n_specs).map(|spec_idx| {
                            if is_str_ref[spec_idx] {
                                local_str[spec_idx].len() as i64
                            } else {
                                local_int[spec_idx].len() as i64
                            }
                        }).collect::<Vec<i64>>()
                    })
                }).collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });
            let merge_us = t_merge.elapsed().as_micros() as u64;

            // Sum bucket counts per spec to get final distinct count.
            let mut final_counts: Vec<i64> = vec![0; n_specs];
            for bucket in &bucket_counts {
                for spec_idx in 0..n_specs {
                    final_counts[spec_idx] += bucket[spec_idx];
                }
            }

            // Build agg_results directly from counts (every spec is CD here).
            let agg_results: Vec<(pg_sys::Datum, bool)> = final_counts.iter()
                .map(|&c| (pg_sys::Datum::from(c as usize), false))
                .collect();
            let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
            for entry in &output_map {
                match entry {
                    OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                    OutputEntry::Group(_) | OutputEntry::DerivedGroup { .. } => row.push((pg_sys::Datum::from(0usize), true)),
                    OutputEntry::Const(d, n) => row.push((*d, *n)),
                }
            }

            let actual_workers = partial_results.len();

            let state = AggScanState {
                _agg_specs: agg_specs,
                _group_specs: group_specs,
                result_rows: vec![row],
                result_idx: 0,
                _num_result_cols: num_result_cols,
                metadata_us,
                heap_scan_us,
                detoast_us: total_detoast_us + pipeline_detoast_us,
                blob_cache_hits: total_cache_hits,
                blob_cache_misses: total_cache_misses,
                blob_cache_bytes_served: total_cache_bytes_served,
                decompress_us: 0,
                agg_us,
                total_segments,
                total_rows_processed: 0,
                batch_quals_count: 0,
                where_quals_null: where_quals.is_null(),
                segments_metadata_resolved: 0,
                segments_decompressed: 0,
                regex_cache_size: 0,
                regex_cache_calls: 0,
                topn_limit: 0,
                topn_sort_col: -1,
                topn_ascending,
                pre_topn_groups: 0,
                merge_us,
                finalize_us: 0,
                topn_select_us: 0,
                n_workers: actual_workers as u64,
                bare_limit: 0, wall_us: t_wall.elapsed().as_micros() as u64, buf_stats: super::segments::take_scan_buf_stats(), f8_preselected: 0, pscan: std::ptr::null_mut(), is_parallel_worker: false, exec_ctx: None,
            };

            let state_box = Box::new(state);
            let state_ptr = Box::into_raw(state_box);
            (*node).custom_ps = state_ptr as *mut pg_sys::List;
            return;
        }

        // ============================================================
        // SINGLE-THREADED PATH (original)
        // ============================================================
        // If lazy loading was used (parallel was possible but conditions
        // weren't met for compact/mixed), detoast all segments now.
        if use_lazy {
            let t_detoast = Instant::now();
            for seg in &mut all_segments {
                let dl = detoast_lazy_blobs(seg);
                total_cache_hits += dl.cache_hits;
                total_cache_misses += dl.cache_misses;
                total_cache_bytes_served += dl.cache_bytes_served;
            }
            total_detoast_us += t_detoast.elapsed().as_micros() as u64;
        }
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
            if segment_skippable_by_dict(
                &batch_quals, &meta.col_names, &meta.segment_by, &seg.compressed_blobs,
            ) {
                continue;
            }

            total_segments += 1;

            let mut const_group_keys: Vec<Option<i64>> = vec![None; group_specs.len()];
            for (gi, gs) in group_specs.iter().enumerate() {
                if is_text_group_col(gs) {
                    continue;
                }
                let GroupByExpr::Extract { unit, divisor, .. } = &gs.expr else {
                    continue;
                };
                let col_idx = gs.col_idx as usize;
                let Some(col_name) = meta.col_names.get(col_idx) else {
                    continue;
                };
                let Some(cm) = seg.col_minmax.get(col_name) else {
                    continue;
                };
                const_group_keys[gi] = constant_extract_key_for_segment(cm, *divisor, unit);
            }

            let skip_numeric_decompress: Vec<bool> = (0..meta.col_names.len())
                .map(|col_idx| {
                    numeric_col_used_only_by_constant_group_keys(
                        col_idx,
                        &group_specs,
                        &const_group_keys,
                        &batch_quals,
                        &agg_specs,
                    )
                })
                .collect();

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

                    if skip_numeric_decompress[col_idx] {
                        decompressed.push(Vec::new());
                        raw_strings.push(None);
                        blob_idx += 1;
                        continue;
                    }

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
                                        compression::CompressionType::Constant
                                            // Single constant string — hash the raw bytes
                                            if non_null_count > 0 => {
                                                seen.insert(hash128_str(cc_ref.data));
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
                        let text_in_qual = batch_quals.iter().find(|bq| {
                            bq.col_idx == col_idx
                                && bq.in_list_text.is_some()
                                && bq.op == BatchCompareOp::InList
                        });

                        if let Some(bq) = like_qual {
                            let strat = bq.like_strategy.as_ref().unwrap();
                            let neg = bq.op == BatchCompareOp::NotLike;
                            let (datums, like_sel) =
                                decompress_text_blob_with_like_filter(blob, type_oid, typmod, strat, neg, None);
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
                                blob, type_oid, typmod, const_str, is_ne, None,
                            );
                            decompressed.push(datums);
                            if pre_selection.is_empty() {
                                pre_selection = eq_sel;
                            } else {
                                for (ps, es) in pre_selection.iter_mut().zip(eq_sel.iter()) {
                                    *ps = *ps && *es;
                                }
                            }
                        } else if let Some(bq) = text_in_qual {
                            let strs = bq.in_list_text.as_ref().unwrap();
                            let (datums, in_sel) = decompress_text_blob_with_in_filter(
                                blob, type_oid, typmod, strs, /* is_not_in */ false, None,
                            );
                            decompressed.push(datums);
                            if pre_selection.is_empty() {
                                pre_selection = in_sel;
                            } else {
                                for (ps, is_) in pre_selection.iter_mut().zip(in_sel.iter()) {
                                    *ps = *ps && *is_;
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

            // Pre-compute CaseWhen GROUP BY columns into SegTextColumn
            let case_when_seg_cols: Vec<Option<SegTextColumn>> = group_specs.iter().map(|gs| {
                if let GroupByExpr::CaseWhen(ref spec) = gs.expr {
                    Some(apply_case_when_to_seg_col(spec, &decompressed, &seg_text_columns, row_count, &selection))
                } else {
                    None
                }
            }).collect();


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
                            GroupByExpr::Extract { unit, divisor, .. } => {
                                eval_extract(col[row].0.value() as i64, *divisor, unit)
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
                            cd_sidecar.alloc_group();
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
                            CompactAccKind::MinInt => {
                                let col = &decompressed[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    let v = col[row].0.value() as i64;
                                    storage.update_min_int(group_idx, spec_idx, v);
                                }
                            }
                            CompactAccKind::MaxInt => {
                                let col = &decompressed[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    let v = col[row].0.value() as i64;
                                    storage.update_max_int(group_idx, spec_idx, v);
                                }
                            }
                            CompactAccKind::CountDistinctInt => {
                                let col = &decompressed[spec.col_idx as usize];
                                if !col.is_empty() && !col[row].1 {
                                    cd_sidecar.insert_int(spec_idx, group_idx, col[row].0.value() as i64);
                                }
                            }
                            CompactAccKind::CountDistinctStr => {
                                let col_idx = spec.col_idx as usize;
                                if let Some(ref rs) = raw_strings[col_idx]
                                    && let Some(ref s) = rs[row] {
                                    cd_sidecar.insert_str(spec_idx, group_idx, hash128_str(s.as_bytes()));
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
                    for (gi, gs) in group_specs.iter().enumerate() {
                        // CaseWhen has col_idx=-1, handle separately via pre-computed SegTextColumn
                        if let GroupByExpr::CaseWhen(_) = &gs.expr {
                            if let Some(Some(seg_col)) = case_when_seg_cols.get(gi) {
                                match seg_col.get_str(row) {
                                    Some(s) => key_ref.push(GroupKeyRef::from_str(s)),
                                    None => key_ref.push(GroupKeyRef::Null),
                                }
                            } else {
                                key_ref.push(GroupKeyRef::Null);
                            }
                            continue;
                        }
                        if let Some(v) = const_group_keys[gi] {
                            key_ref.push(GroupKeyRef::Int(v));
                            continue;
                        }
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
                                GroupByExpr::Extract { unit, divisor, .. } => {
                                    let extracted = eval_extract(col[row].0.value() as i64, *divisor, unit);
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
                                GroupByExpr::CaseWhen(_) => unreachable!(),
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

        // Write CountDistinct counts from sidecar into compact storage
        if use_compact_keys && use_compact_accs {
            cd_sidecar.write_counts_to_storage(compact_storage.as_mut().unwrap(), &compact_group_map);
        }

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
                agg_specs[sort_slot].agg_type == AggType::Avg,
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
                        OutputEntry::DerivedGroup { base_gi, delta } => {
                            let v = keys[*base_gi] + delta;
                            row.push((pg_sys::Datum::from(v as usize), false));
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
                        OutputEntry::DerivedGroup { base_gi, delta } => {
                            let v = keys[*base_gi] + delta;
                            row.push((pg_sys::Datum::from(v as usize), false));
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
                        OutputEntry::DerivedGroup { base_gi, delta } => {
                            match &key_slice[*base_gi] {
                                GroupKeyVal::Int(v) => row.push((pg_sys::Datum::from((*v + delta) as usize), false)),
                                _ => row.push((pg_sys::Datum::from(0usize), true)),
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
                    OutputEntry::Group(_) | OutputEntry::DerivedGroup { .. } => {
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
            detoast_us: total_detoast_us,
            blob_cache_hits: total_cache_hits,
            blob_cache_misses: total_cache_misses,
            blob_cache_bytes_served: total_cache_bytes_served,
            decompress_us,
            agg_us,
            total_segments,
            total_rows_processed,
            batch_quals_count: batch_quals.len(),
            where_quals_null: where_quals.is_null(),
            segments_metadata_resolved: 0,
            segments_decompressed: 0,
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
            bare_limit: 0, wall_us: t_wall.elapsed().as_micros() as u64, buf_stats: super::segments::take_scan_buf_stats(), f8_preselected: 0, pscan: std::ptr::null_mut(), is_parallel_worker: false, exec_ctx: None,
        };

        let state_box = Box::new(state);
        let state_ptr = Box::into_raw(state_box);
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}

/// String arena: all group key strings packed into one Vec<u8>.
/// One deallocation instead of 275K individual String deallocations.
pub(super) struct StringArena {
    pub(super) buf: Vec<u8>,
}

impl StringArena {
    fn new() -> Self { Self { buf: Vec::new() } }

    pub(super) fn alloc(&mut self, s: &str) -> (u32, u32) {
        let off = self.buf.len() as u32;
        let len = s.len() as u32;
        self.buf.extend_from_slice(s.as_bytes());
        (off, len)
    }

    pub(super) fn get(&self, off: u32, len: u32) -> &str {
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

// SegTextColumn is now in text_col.rs

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

        // Phase C.2 activation: partial-mode runtime. Every process (leader
        // and workers) claims segments via the shared DSM cursor, builds a
        // process-local accumulator, finalises into per-group partial-state
        // rows, and emits them. PG's Gather concatenates rows from N
        // processes; the Final Aggregate above combines partials per group
        // via aggcombinefn. No DSM-slab serialise / spin-wait / leader
        // merge — each process is independent.
        let is_partial_mode = state
            .exec_ctx
            .as_ref()
            .map(|c| c.is_partial)
            .unwrap_or(false);
        if is_partial_mode {
            let already_done = state
                .exec_ctx
                .as_ref()
                .map(|c| c.merged)
                .unwrap_or(true);
            if !already_done {
                run_partial_aggregate_in_process(state);
            }
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
            pg_sys::ExecClearTuple(scan_slot);
            return scan_slot;
        }

        // Phase C.2.d: parallel workers run the claim+aggregate+serialise
        // loop on their first exec call. Subsequent calls return EOF — the
        // leader picks up partial slabs in C.2.e via a spin-wait on
        // `populated`. Workers emit no rows themselves.
        if state.is_parallel_worker {
            if state.exec_ctx.is_some() {
                let already_done = state
                    .exec_ctx
                    .as_ref()
                    .map(|c| c.worker_done)
                    .unwrap_or(true);
                if !already_done {
                    run_worker_partial_aggregate(state);
                }
            }
            pg_sys::ExecClearTuple(scan_slot);
            return scan_slot;
        }

        // Phase C.2.e: leader's first call in the parallel-aware path.
        // Claim segments alongside workers, spin-wait for worker partials,
        // deserialise + merge them into the leader-local accumulator,
        // finalise into `result_rows`. Subsequent calls fall through to the
        // shared row-emit loop below.
        if let Some(ctx) = state.exec_ctx.as_ref()
            && !ctx.merged
        {
            run_leader_merge_and_finalise(state);
        }

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

/// Phase C.2.e: leader's first-call body. Run the same chunked-claim loop as
/// workers (leader is slot 0), spin-wait for each worker slot's `populated`
/// flag with `Acquire`, deserialise each populated slab, merge into the
/// leader's global accumulator, finalise into `result_rows`, mark
/// `ctx.merged = true`. Subsequent exec calls emit cached rows.
unsafe fn run_leader_merge_and_finalise(state: &mut AggScanState) {
    unsafe {
        if state.pscan.is_null() {
            return;
        }
        let total_segments;
        let n_worker_slots;
        {
            let ps = &*state.pscan;
            total_segments = ps.total_segments;
            n_worker_slots = ps.n_worker_slots as usize;
        }

        let ctx = match state.exec_ctx.as_mut() {
            Some(c) => c,
            None => return,
        };

        // Leader-local accumulator. Leader is slot 0 so it claims segments
        // alongside workers; that's fine — `next_segment.fetch_add` is
        // shared.
        let mut global = ParallelCompactResult::empty(&ctx.agg_specs);

        if total_segments > 0 {
            let cfg = ParallelCompactConfig {
                agg_specs: &ctx.agg_specs,
                group_specs: &ctx.group_specs,
                col_names: &ctx.meta.col_names,
                col_types: &ctx.meta.col_types,
                segment_by: &ctx.meta.segment_by,
                needed_cols: &ctx.needed_cols,
                batch_quals: &ctx.batch_quals,
                seg_filters: &ctx.seg_filters,
                time_min: ctx.time_min,
                time_max: ctx.time_max,
                topn_spec: ctx.topn_spec,
            };

            const CHUNK: u64 = 4;
            loop {
                let start = (*state.pscan).next_segment.fetch_add(CHUNK, Ordering::Relaxed);
                if start >= total_segments {
                    break;
                }
                let end = (start + CHUNK).min(total_segments);
                let slice = &ctx.all_segments[start as usize..end as usize];
                let chunk_result = process_segments_compact(slice, &cfg);
                merge_compact_results(
                    &mut global.compact_map,
                    &mut global.compact_storage,
                    &mut global.cd_sidecar,
                    &chunk_result.compact_map,
                    &chunk_result.compact_storage,
                    &chunk_result.cd_sidecar,
                    &ctx.agg_specs,
                );
                global.segments_processed += chunk_result.segments_processed;
                global.rows_processed += chunk_result.rows_processed;
                global.decompress_us = global.decompress_us.max(chunk_result.decompress_us);
            }
        }

        // Spin-wait for each worker slot to publish its partial. Slot 0 is
        // the leader (us) so iterate 1..n_worker_slots. Spin-loop with a
        // backoff to avoid burning a core when all workers are CPU-bound on
        // their final merge.
        for slot in 1..n_worker_slots {
            let mut spin: u32 = 0;
            loop {
                let populated = (*state.pscan).worker_timings[slot]
                    .populated;
                // The populated field is a plain u32; emit an Acquire fence
                // after observing 1 so the slab + partial_lens writes
                // become visible. (worker_timings isn't itself atomic to
                // keep the struct POD-zeroable.)
                if populated == 1 {
                    std::sync::atomic::fence(Ordering::Acquire);
                    break;
                }
                spin = spin.saturating_add(1);
                if spin < 1024 {
                    std::hint::spin_loop();
                } else {
                    std::thread::yield_now();
                    spin = 0;
                }
            }
        }

        // Deserialise each populated slab and merge into the leader's global.
        for slot in 1..n_worker_slots {
            let len = (*state.pscan).partial_lens[slot].load(Ordering::Acquire);
            if len == 0 {
                continue; // empty/unused slot
            }
            let slab_ptr = (*state.pscan).slab_ptr(slot);
            match super::agg_wire::deserialize_partial(slab_ptr, len, &ctx.agg_specs) {
                Ok(worker) => {
                    merge_compact_results(
                        &mut global.compact_map,
                        &mut global.compact_storage,
                        &mut global.cd_sidecar,
                        &worker.compact_map,
                        &worker.compact_storage,
                        &worker.cd_sidecar,
                        &ctx.agg_specs,
                    );
                    global.segments_processed += worker.segments_processed;
                    global.rows_processed += worker.rows_processed;
                    global.decompress_us = global.decompress_us.max(worker.decompress_us);
                }
                Err(e) => {
                    pgrx::error!(
                        "pg_deltax: failed to deserialise worker slot {} partial: {:?}",
                        slot,
                        e,
                    );
                }
            }
        }

        // Finalise into result_rows. When ctx.is_partial, emit each agg
        // slot's PG aggtranstype value (combined upstream by Final Agg);
        // otherwise emit the user-visible final value.
        let result_rows = finalise_compact_into_result_rows(
            &global.compact_map,
            &global.compact_storage,
            &ctx.agg_specs,
            &ctx.group_specs,
            &ctx.output_map,
            ctx.num_result_cols,
            ctx.is_partial,
        );

        state.total_segments = global.segments_processed;
        state.total_rows_processed = global.rows_processed;
        state.decompress_us = global.decompress_us;
        state.result_rows = result_rows;
        state.result_idx = 0;
        ctx.merged = true;
    }
}

/// Iterate `global_map`'s groups and turn each one into an output row using
/// `compact_finalize` per agg spec and `output_map` for column placement.
/// Mirrors the post-merge emit loop in `begin_agg_scan`'s rayon path.
///
/// The parallel-aware path's eligibility (C.2.f) excludes HAVING, Top-N, and
/// CountDistinct, so this is the simplest variant — no filter pass, no sort,
/// no special-case finalisation.
///
/// When `is_partial = true` (Phase C.2 activation), each agg slot is emitted
/// via `compact_emit_partial` (returning the PG `aggtranstype` value) instead
/// of the user-visible final value — a Final Aggregate node above DeltaXAgg
/// then combines the partials via `aggcombinefn`.
unsafe fn finalise_compact_into_result_rows(
    global_map: &CompactGroupMap,
    global_storage: &CompactAccStorage,
    agg_specs: &[AggExecSpec],
    group_specs: &[GroupByColSpec],
    output_map: &[OutputEntry],
    num_result_cols: usize,
    is_partial: bool,
) -> Vec<Vec<(pg_sys::Datum, bool)>> {
    unsafe {
        let num_group_keys = group_specs.len();
        let mut rows: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::with_capacity(global_map.len());
        for (&packed_key, &group_idx) in global_map {
            let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(agg_specs.len());
            for (spec_idx, spec) in agg_specs.iter().enumerate() {
                let val = if is_partial {
                    compact_emit_partial(global_storage, group_idx, spec_idx, spec)
                } else {
                    compact_finalize(global_storage, group_idx, spec_idx, spec)
                };
                agg_results.push(val);
            }
            let keys = unpack_int_keys(packed_key, num_group_keys);
            let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
            for entry in output_map {
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
                    OutputEntry::DerivedGroup { base_gi, delta } => {
                        let v = keys[*base_gi] + delta;
                        row.push((pg_sys::Datum::from(v as usize), false));
                    }
                    OutputEntry::Const(d, n) => row.push((*d, *n)),
                }
            }
            rows.push(row);
        }
        rows
    }
}

/// Phase C.2 activation — partial-mode in-process aggregation. Every
/// process (leader and workers) calls this on its first `exec_agg_scan`
/// invocation. Claims segments via the shared DSM cursor, accumulates into
/// a process-local `ParallelCompactResult`, then finalises into
/// `state.result_rows` using `compact_emit_partial` per slot. PG's Gather
/// and the Final Aggregate above combine partials per group via the
/// aggregate's `aggcombinefn`.
///
/// Differs from `run_worker_partial_aggregate` (complete-mode workers
/// writing to DSM) and `run_leader_merge_and_finalise` (complete-mode
/// leader merging worker DSM slabs) — neither is invoked in partial mode
/// because there's no inter-process DSM merge step.
unsafe fn run_partial_aggregate_in_process(state: &mut AggScanState) {
    unsafe {
        if state.pscan.is_null() {
            return;
        }
        let total_segments = (*state.pscan).total_segments;

        let ctx = match state.exec_ctx.as_mut() {
            Some(c) => c,
            None => return,
        };

        let mut local = ParallelCompactResult::empty(&ctx.agg_specs);

        if total_segments > 0 {
            let cfg = ParallelCompactConfig {
                agg_specs: &ctx.agg_specs,
                group_specs: &ctx.group_specs,
                col_names: &ctx.meta.col_names,
                col_types: &ctx.meta.col_types,
                segment_by: &ctx.meta.segment_by,
                needed_cols: &ctx.needed_cols,
                batch_quals: &ctx.batch_quals,
                seg_filters: &ctx.seg_filters,
                time_min: ctx.time_min,
                time_max: ctx.time_max,
                topn_spec: None,
            };
            const CHUNK: u64 = 4;
            loop {
                let start = (*state.pscan).next_segment.fetch_add(CHUNK, Ordering::Relaxed);
                if start >= total_segments {
                    break;
                }
                let end = (start + CHUNK).min(total_segments);
                let slice = &ctx.all_segments[start as usize..end as usize];
                let chunk_result = process_segments_compact(slice, &cfg);
                merge_compact_results(
                    &mut local.compact_map,
                    &mut local.compact_storage,
                    &mut local.cd_sidecar,
                    &chunk_result.compact_map,
                    &chunk_result.compact_storage,
                    &chunk_result.cd_sidecar,
                    &ctx.agg_specs,
                );
                local.segments_processed += chunk_result.segments_processed;
                local.rows_processed += chunk_result.rows_processed;
                local.decompress_us = local.decompress_us.max(chunk_result.decompress_us);
            }
        }

        // Finalise into per-group partial-state rows. is_partial=true →
        // compact_emit_partial chooses the right `aggtranstype` value per
        // slot; PG's Final Aggregate above combines via aggcombinefn.
        let result_rows = finalise_compact_into_result_rows(
            &local.compact_map,
            &local.compact_storage,
            &ctx.agg_specs,
            &ctx.group_specs,
            &ctx.output_map,
            ctx.num_result_cols,
            /* is_partial */ true,
        );

        state.total_segments = local.segments_processed;
        state.total_rows_processed = local.rows_processed;
        state.decompress_us = local.decompress_us;
        state.result_rows = result_rows;
        state.result_idx = 0;
        ctx.merged = true;
    }
}

/// Phase C.2.d: claim segments from the shared cursor, run
/// `process_segments_compact` on each chunk, accumulate into a worker-local
/// `ParallelCompactResult`, then serialise the result into the worker's DSM
/// slab. The leader's `exec_agg_scan` first call (Phase C.2.e) reads back
/// the slab via `agg_wire::deserialize_partial`.
///
/// Chunked claim (`CHUNK = 4`) amortises atomic cache-line bouncing across N
/// workers; load imbalance is bounded by `CHUNK` segments. Empty-claim case
/// (cursor already exhausted) is handled naturally — `serialize_partial_into`
/// emits a header-only ~96-byte wire that the leader's deserialiser turns
/// into an empty result with no merge effect.
///
/// Memory ordering: `partial_lens[slot]` is written with `Release` here;
/// `shutdown_deltax_agg` later writes `worker_timings[slot].populated = 1`
/// (also Release). The leader's `Acquire` load on `populated` synchronises
/// with both writes in program order.
unsafe fn run_worker_partial_aggregate(state: &mut AggScanState) {
    unsafe {
        const CHUNK: u64 = 4;

        if state.pscan.is_null() {
            return;
        }
        let ps = &*state.pscan;
        let total_segments = ps.total_segments;
        let slab_cap = ps.partial_slab_size;

        let slot = current_agg_worker_slot();
        if slot >= ps.n_worker_slots as usize {
            return;
        }
        let slab_ptr = ps.slab_ptr(slot);

        let ctx = match state.exec_ctx.as_mut() {
            Some(c) => c,
            None => return,
        };

        let mut local = ParallelCompactResult::empty(&ctx.agg_specs);

        if total_segments > 0 {
            let cfg = ParallelCompactConfig {
                agg_specs: &ctx.agg_specs,
                group_specs: &ctx.group_specs,
                col_names: &ctx.meta.col_names,
                col_types: &ctx.meta.col_types,
                segment_by: &ctx.meta.segment_by,
                needed_cols: &ctx.needed_cols,
                batch_quals: &ctx.batch_quals,
                seg_filters: &ctx.seg_filters,
                time_min: ctx.time_min,
                time_max: ctx.time_max,
                topn_spec: ctx.topn_spec,
            };

            loop {
                let start = ps.next_segment.fetch_add(CHUNK, Ordering::Relaxed);
                if start >= total_segments {
                    break;
                }
                let end = (start + CHUNK).min(total_segments);
                let slice = &ctx.all_segments[start as usize..end as usize];
                let chunk_result = process_segments_compact(slice, &cfg);

                merge_compact_results(
                    &mut local.compact_map,
                    &mut local.compact_storage,
                    &mut local.cd_sidecar,
                    &chunk_result.compact_map,
                    &chunk_result.compact_storage,
                    &chunk_result.cd_sidecar,
                    &ctx.agg_specs,
                );
                local.segments_processed += chunk_result.segments_processed;
                local.rows_processed += chunk_result.rows_processed;
                local.decompress_us = local.decompress_us.max(chunk_result.decompress_us);
            }
        }

        match super::agg_wire::serialize_partial_into(slab_ptr, slab_cap, &local, &ctx.agg_specs) {
            Ok(written) => {
                // SAFETY: Release-store `partial_lens[slot]` after the slab
                // bytes are written so the leader's Acquire-load on
                // `populated` (in `shutdown_deltax_agg`) makes the slab
                // contents visible.
                (*state.pscan).partial_lens[slot].store(written as u64, Ordering::Release);
                ctx.worker_done = true;
            }
            Err(super::agg_wire::SerError::Overflow { needed, have }) => {
                pgrx::error!(
                    "pg_deltax: parallel-agg partial slab overflow ({} bytes needed, {} available); \
                     spill to per-worker tuplestore is Phase F",
                    needed,
                    have,
                );
            }
        }
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
            let total_us = state.wall_us;
            pgrx::log!(
                "pg_deltax DeltaXAgg timing: total={:.1}ms  metadata={:.1}ms  heap_scan={:.1}ms  [detoast={:.1}ms]  \
                 decompress={:.1}ms  agg={:.1}ms  merge={:.1}ms  finalize={:.1}ms  topn_select={:.1}ms  | \
                 workers={} segments={} rows_processed={} groups={} result_rows={} topn_limit={} bare_limit={} f8_preselected={}",
                total_us as f64 / 1000.0,
                state.metadata_us as f64 / 1000.0,
                state.heap_scan_us as f64 / 1000.0,
                state.detoast_us as f64 / 1000.0,
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
                state.f8_preselected,
            );
            (*node).custom_ps = std::ptr::null_mut();
        }
    }
}

/// ReScanCustomScan callback for DeltaXAgg.
///
/// For parallel-aware scans, also clears `result_rows` and `exec_ctx.merged`
/// so the next exec re-runs the full claim+merge cycle. PG calls
/// `reinit_dsm_deltax_agg` separately to zero the cursor + per-slot state in
/// shared memory.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn rescan_agg_scan(
    node: *mut pg_sys::CustomScanState,
) {
    unsafe {
        let state = &mut *((*node).custom_ps as *mut AggScanState);
        state.result_idx = 0;
        if let Some(ctx) = state.exec_ctx.as_mut() {
            ctx.merged = false;
            ctx.worker_done = false;
            state.result_rows.clear();
        }
    }
}

// ============================================================================
// Compact Accumulator Storage (Phase 1)
// ============================================================================

/// Kind of accumulator slot in compact storage.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum CompactAccKind {
    Count,              // 8 bytes: i64
    SumInt,             // 24 bytes: i128 sum (16) + i64 count (8) — for INT8 columns
    SumIntNarrow,       // 16 bytes: i64 sum (8) + i64 count (8) — for INT2/INT4 columns
    SumFloat,           // 16 bytes: f64 sum (8) + i64 count (8)
    MinStr,             // 8 bytes: u32 arena_offset + u32 length (sentinel: u32::MAX, 0)
    MaxStr,             // 8 bytes: u32 arena_offset + u32 length (sentinel: u32::MAX, 0)
    CountDistinctInt,   // 8 bytes: i64 count cache (real data in CountDistinctSideCar)
    CountDistinctStr,   // 8 bytes: i64 count cache (real data in CountDistinctSideCar)
    MinInt,             // 16 bytes: i64 val + i64 has_value flag (0 = unset)
    MaxInt,             // 16 bytes: i64 val + i64 has_value flag (0 = unset)
}

impl CompactAccKind {
    /// 8-bit on-wire tag. Stable across versions because parallel-agg DSM wire
    /// format depends on it.
    #[allow(dead_code)] // wired by C.2.d/e via agg_wire
    pub(super) fn wire_tag(self) -> u8 {
        match self {
            CompactAccKind::Count => 0,
            CompactAccKind::SumInt => 1,
            CompactAccKind::SumIntNarrow => 2,
            CompactAccKind::SumFloat => 3,
            CompactAccKind::MinStr => 4,
            CompactAccKind::MaxStr => 5,
            CompactAccKind::CountDistinctInt => 6,
            CompactAccKind::CountDistinctStr => 7,
            CompactAccKind::MinInt => 8,
            CompactAccKind::MaxInt => 9,
        }
    }

    fn byte_size(self) -> usize {
        match self {
            CompactAccKind::Count
            | CompactAccKind::CountDistinctInt
            | CompactAccKind::CountDistinctStr => 8,
            CompactAccKind::SumInt => 24,
            CompactAccKind::SumIntNarrow => 16,
            CompactAccKind::SumFloat => 16,
            CompactAccKind::MinStr | CompactAccKind::MaxStr => 8,
            CompactAccKind::MinInt | CompactAccKind::MaxInt => 16,
        }
    }

    fn alignment(self) -> usize {
        match self {
            CompactAccKind::Count
            | CompactAccKind::CountDistinctInt
            | CompactAccKind::CountDistinctStr => 8,
            CompactAccKind::SumInt => 16, // i128 needs 16-byte alignment
            CompactAccKind::SumIntNarrow => 8,
            CompactAccKind::SumFloat => 8,
            CompactAccKind::MinStr | CompactAccKind::MaxStr => 4,
            CompactAccKind::MinInt | CompactAccKind::MaxInt => 8,
        }
    }
}

/// Layout of compact accumulator slots for one group.
pub(super) struct CompactAccLayout {
    /// (byte_offset, kind) per aggregate
    pub(super) slots: Vec<(usize, CompactAccKind)>,
    /// Total bytes per group (aligned to 16)
    pub(super) group_stride: usize,
}

impl CompactAccLayout {
    pub(super) fn new(specs: &[AggExecSpec]) -> Self {
        let mut offset: usize = 0;

        // Sort by alignment (descending) to minimize padding.
        // We need to maintain original order for indexing, so we compute
        // offsets in alignment order then map back.
        let mut indexed: Vec<(usize, CompactAccKind)> = specs.iter().enumerate().map(|(i, spec)| {
            let kind = compact_acc_kind(spec);
            (i, kind)
        }).collect();
        // Sort by alignment descending (i128 first, then i64/f64)
        indexed.sort_by_key(|b| std::cmp::Reverse(b.1.alignment()));

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
            } else if t == pg_sys::INT2OID || t == pg_sys::INT4OID || t == pg_sys::INT8OID
                || t == pg_sys::DATEOID
                || t == pg_sys::TIMESTAMPOID || t == pg_sys::TIMESTAMPTZOID
            {
                CompactAccKind::MinInt
            } else {
                unreachable!("compact_acc_kind: MIN on type {:?} not supported in compact path", t)
            }
        }
        AggType::Max => {
            let t = spec.col_type_oid;
            if t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID {
                CompactAccKind::MaxStr
            } else if t == pg_sys::INT2OID || t == pg_sys::INT4OID || t == pg_sys::INT8OID
                || t == pg_sys::DATEOID
                || t == pg_sys::TIMESTAMPOID || t == pg_sys::TIMESTAMPTZOID
            {
                CompactAccKind::MaxInt
            } else {
                unreachable!("compact_acc_kind: MAX on type {:?} not supported in compact path", t)
            }
        }
        AggType::CountDistinct => {
            let t = spec.col_type_oid;
            if t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID {
                CompactAccKind::CountDistinctStr
            } else {
                CompactAccKind::CountDistinctInt
            }
        }
    }
}

/// Phase D bitset: a bare `Vec<u64>` with set/or/popcount. Used for
/// COUNT(DISTINCT text) when the column is dictionary-encoded across every
/// participating segment — workers set bits indexed by leader-precomputed
/// global string IDs, the merge step OR's two bitsets, and finalisation
/// returns `count_ones`. Avoids a `bitvec` dep — `count_ones` on `u64`
/// lowers to POPCNT on x86_64 and CNT on aarch64.
#[derive(Clone)]
pub(super) struct Bitset {
    words: Vec<u64>,
    nbits: u32,
}

impl Bitset {
    pub(super) fn with_size(nbits: u32) -> Self {
        let nwords = nbits.div_ceil(64) as usize;
        Bitset { words: vec![0u64; nwords], nbits }
    }
    #[inline]
    pub(super) fn set(&mut self, idx: u32) {
        debug_assert!(idx < self.nbits, "Bitset::set out of range");
        let w = (idx >> 6) as usize;
        let b = idx & 63;
        self.words[w] |= 1u64 << b;
    }
    #[inline]
    pub(super) fn or_with(&mut self, other: &Bitset) {
        debug_assert_eq!(self.words.len(), other.words.len());
        for (a, b) in self.words.iter_mut().zip(other.words.iter()) {
            *a |= *b;
        }
    }
    pub(super) fn count_ones(&self) -> u64 {
        self.words.iter().map(|w| w.count_ones() as u64).sum()
    }
}

/// Phase D dict-eligible CountDistinct(text) global remap. Built once at the
/// leader after segments are loaded — see `build_dict_distinct_remaps`.
///
/// `per_segment[seg_idx][local_dict_id] = global_id`. `seg_idx` is the
/// position in the leader's `all_segments` Vec; `local_dict_id` is the entry
/// position in that segment's per-column dictionary; `global_id` is a unique
/// integer in `[0, global_count)` shared across every segment. Workers set
/// bit `global_id` in their per-(spec,group) `Bitset` instead of hashing the
/// raw string.
pub(super) struct DictDistinctRemap {
    pub(super) global_count: u32,
    pub(super) per_segment: Vec<Vec<u32>>,
}

/// Skip Phase D's bitset path when the per-column **post-dedup global
/// string count** exceeds this. Bitset size is `global_count` bits per
/// (spec, group, worker); at 10M × 16 groups × 16 workers ≈ 320 MB —
/// tolerable on bench-class boxes (m6i.8xlarge has 128 GB) but starts
/// mattering on small ones. Tighten if real workloads push past this.
///
/// Checked AFTER the parallel pre-pass — at that point we know the
/// actual deduplicated count, not the looser per-segment-dict-size sum.
pub(super) const PHASE_D_MAX_GLOBAL_FOR_BITSET: u32 = 10_000_000;

/// Skip Phase D entirely when the **sum of per-segment dict sizes**
/// exceeds this. Sum is a loose upper bound on global cardinality
/// (post-dedup `global_count` ≤ sum). A high sum implies expensive
/// pre-pass: LZ4-decompressing dicts, hashing entries, allocating
/// per-thread String keys. JSONBench Q1's `x_did` (~10K entries per
/// segment × 2700 segs ≈ 27M sum, ~3.6M unique post-dedup) sits in the
/// sweet spot — well under this gate, comfortably under
/// `PHASE_D_MAX_GLOBAL_FOR_BITSET` after dedup. UUID-style columns
/// where every row is unique blow past the sum gate without the
/// pre-pass even running.
///
/// Tuned 4× looser than the global cap to leave room for typical 4-10×
/// dedup ratios while still bounding worst-case pre-pass memory.
pub(super) const PHASE_D_MAX_DICT_SIZE_SUM: u64 = 50_000_000;

/// Phase D leader pre-pass: walk each `CountDistinct(text)` spec's
/// per-segment dictionary blob, build a global string-ID interner, and
/// emit per-segment local→global remap tables. Returns one
/// `DictDistinctRemap` per eligible spec (keyed by its index in
/// `agg_specs`). Specs whose columns aren't dict-encoded across every
/// segment, or whose global cardinality exceeds
/// `PHASE_D_MAX_GLOBAL_FOR_BITSET`, are absent from the result and stay
/// on the `HashSet<u128>` path.
///
/// Parallelised via `std::thread::scope`:
///
///   1. **Phase 1 (parallel)** — each worker takes a chunk of segments,
///      parses every dict (LZ4-decompressing as needed), and builds two
///      things: a per-thread `local_entries: Vec<String>` recording
///      strings in insertion order, and `seg_local_remaps: Vec<Vec<u32>>`
///      mapping `(seg_in_chunk, local_dict_id) → local_thread_id` (the
///      string's index in `local_entries`).
///   2. **Phase 2 (sequential)** — merge each thread's `local_entries`
///      into the global interner. For each thread `t` we end up with
///      `thread_to_global[t][local_thread_id] = global_id`. This is the
///      only sequential bottleneck and is dominated by HashMap probes
///      against the global interner; LZ4 decompression and per-string
///      hashing already happened in parallel.
///   3. **Phase 3 (parallel)** — each worker rewrites its
///      `seg_local_remaps` into the final `per_segment[seg_idx][local_dict_id]
///      = global_id` slabs by indexing into `thread_to_global[my_thread]`.
///      Pure array lookup; runs in well under 100ms even on Q1-scale data.
pub(super) fn build_dict_distinct_remaps(
    all_segments: &[super::segments::SegmentData],
    agg_specs: &[AggExecSpec],
) -> std::collections::HashMap<usize, DictDistinctRemap> {
    use crate::compression::{CompressedColumnRef, CompressionType, dictionary};

    let mut remaps = std::collections::HashMap::new();
    let n_workers = crate::get_parallel_workers().max(1);

    for (spec_idx, spec) in agg_specs.iter().enumerate() {
        if spec.agg_type != AggType::CountDistinct {
            continue;
        }
        if !matches!(
            spec.col_type_oid,
            pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID
        ) {
            continue;
        }
        if spec.col_idx < 0 {
            continue;
        }
        let col_idx = spec.col_idx as usize;

        // Eligibility: every segment whose blob is non-empty must be
        // dict-encoded for this column AND the sum of per-segment dict
        // sizes must fit under PHASE_D_MAX_GLOBAL_FOR_BITSET. The sum is
        // an upper bound on global cardinality (after dedup it can only
        // shrink) and lives in the first 4 bytes of `cc_ref.data` — no
        // LZ4 decompression on bail. Segment-by columns store values in
        // `segment_values` (outside `compressed_blobs`); for those the
        // spec stays on the HashSet path even if every other segment is
        // dict-encoded — too narrow to bother with a separate code path.
        let mut eligible = true;
        let mut dict_size_sum: u64 = 0;
        for seg in all_segments {
            if col_idx >= seg.compressed_blobs.len() {
                eligible = false;
                break;
            }
            let blob = &seg.compressed_blobs[col_idx];
            if blob.is_empty() {
                continue;
            }
            let comp = CompressionType::from_u8(blob[0]);
            if !matches!(
                comp,
                CompressionType::Dictionary | CompressionType::DictionaryLz4
            ) {
                eligible = false;
                break;
            }
            let cc_ref = CompressedColumnRef::from_bytes(blob);
            if cc_ref.data.len() < 4 {
                eligible = false;
                break;
            }
            let dict_size =
                u32::from_le_bytes(cc_ref.data[0..4].try_into().unwrap()) as u64;
            dict_size_sum = dict_size_sum.saturating_add(dict_size);
            if dict_size_sum > PHASE_D_MAX_DICT_SIZE_SUM {
                eligible = false;
                break;
            }
        }
        if !eligible {
            continue;
        }

        // ---------- Phase 1 (parallel): per-thread local interners ----------
        struct LocalPrePass {
            /// `local_entries[local_thread_id]` = entry string. Insertion
            /// order — preserved so Phase 2's sequential merge is a clean
            /// linear walk and `thread_to_global[t]` is indexable directly
            /// by `local_thread_id`.
            local_entries: Vec<String>,
            /// `seg_local_remaps[seg_in_chunk][local_dict_id] = local_thread_id`.
            seg_local_remaps: Vec<Vec<u32>>,
        }

        let chunk_size = all_segments.len().div_ceil(n_workers).max(1);
        let local_results: Vec<LocalPrePass> = std::thread::scope(|s| {
            all_segments
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(move || {
                        let mut lookup: hashbrown::HashMap<
                            String,
                            u32,
                            BuildHasherDefault<ahash::AHasher>,
                        > = hashbrown::HashMap::with_hasher(BuildHasherDefault::default());
                        let mut local_entries: Vec<String> = Vec::new();
                        let mut seg_local_remaps: Vec<Vec<u32>> =
                            Vec::with_capacity(chunk.len());
                        for seg in chunk {
                            let blob = &seg.compressed_blobs[col_idx];
                            if blob.is_empty() {
                                seg_local_remaps.push(Vec::new());
                                continue;
                            }
                            let cc_ref = CompressedColumnRef::from_bytes(blob);
                            let norm_buf;
                            let dict_data: &[u8] = if cc_ref.type_tag
                                == CompressionType::DictionaryLz4
                            {
                                norm_buf = dictionary::normalize_lz4(cc_ref.data);
                                &norm_buf[..]
                            } else {
                                cc_ref.data
                            };
                            let header = dictionary::parse_header(dict_data);
                            let mut seg_remap: Vec<u32> =
                                Vec::with_capacity(header.dict.len());
                            for &entry in &header.dict {
                                let local_id = match lookup.get(entry) {
                                    Some(&id) => id,
                                    None => {
                                        let id = local_entries.len() as u32;
                                        local_entries.push(entry.to_string());
                                        lookup.insert(entry.to_string(), id);
                                        id
                                    }
                                };
                                seg_remap.push(local_id);
                            }
                            seg_local_remaps.push(seg_remap);
                        }
                        LocalPrePass { local_entries, seg_local_remaps }
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect()
        });

        // ---------- Phase 2 (sequential): merge into global interner -------
        // `thread_to_global[t][local_thread_id] = global_id`.
        let mut global_interner: hashbrown::HashMap<
            String,
            u32,
            BuildHasherDefault<ahash::AHasher>,
        > = hashbrown::HashMap::with_hasher(BuildHasherDefault::default());
        let mut thread_to_global: Vec<Vec<u32>> =
            Vec::with_capacity(local_results.len());
        for local in &local_results {
            let mut t_remap: Vec<u32> = Vec::with_capacity(local.local_entries.len());
            for entry in &local.local_entries {
                let global_id = match global_interner.get(entry) {
                    Some(&id) => id,
                    None => {
                        let id = global_interner.len() as u32;
                        global_interner.insert(entry.clone(), id);
                        id
                    }
                };
                t_remap.push(global_id);
            }
            thread_to_global.push(t_remap);
        }

        let global_count = global_interner.len() as u32;
        if global_count == 0 {
            continue;
        }
        if global_count > PHASE_D_MAX_GLOBAL_FOR_BITSET {
            // Post-dedup the column has more unique strings than the
            // bitset memory budget allows. Drop the pre-pass work and let
            // workers fall back to HashSet<u128>. Pre-pass effort wasted
            // for this query, but the sum gate above keeps the wasted
            // work bounded; in practice this branch only fires on truly
            // pathological cardinality (>10M unique).
            continue;
        }

        // ---------- Phase 3 (parallel): rewrite local IDs to global IDs ----
        // Each worker takes its slice of `local_results` + its slot in
        // `thread_to_global`, rewrites `seg_local_remaps` in place. The
        // resulting `Vec<Vec<u32>>` per chunk is concatenated below into
        // `per_segment` in the original `all_segments` order — chunks were
        // contiguous slices in Phase 1, so the order is preserved.
        let global_chunks: Vec<Vec<Vec<u32>>> = std::thread::scope(|s| {
            local_results
                .into_iter()
                .zip(thread_to_global.iter())
                .map(|(mut local, t_remap)| {
                    s.spawn(move || {
                        for seg_remap in &mut local.seg_local_remaps {
                            for slot in seg_remap.iter_mut() {
                                *slot = t_remap[*slot as usize];
                            }
                        }
                        local.seg_local_remaps
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect()
        });

        let mut per_segment: Vec<Vec<u32>> = Vec::with_capacity(all_segments.len());
        for chunk in global_chunks {
            per_segment.extend(chunk);
        }
        debug_assert_eq!(per_segment.len(), all_segments.len());

        remaps.insert(
            spec_idx,
            DictDistinctRemap {
                global_count,
                per_segment,
            },
        );
    }

    remaps
}

/// Side-car storage for COUNT(DISTINCT) accumulators.
/// Each CountDistinct agg spec gets a Vec of HashSets indexed by group_idx.
/// Int columns store raw i64 values; text columns store 128-bit hash digests.
/// Dict-eligible text columns (Phase D) use per-group `Bitset` indexed by
/// leader-precomputed global string IDs.
pub(super) struct CountDistinctSideCar {
    entries: Vec<CdEntry>,
}

#[derive(PartialEq, Eq, Clone, Copy)]
pub(super) enum CdKind {
    Int,
    Str,
    /// Dict-encoded text with leader pre-pass: per-group `Bitset` of size
    /// `global_count` (the bitset_size below). Merge is bit-OR; finalise is
    /// `count_ones`. Eligibility checked in `build_dict_distinct_remaps`.
    DictBitset,
}

struct CdEntry {
    spec_idx: usize,
    kind: CdKind,
    /// `bitset_size` for `DictBitset`; otherwise unused.
    bitset_size: u32,
    sets_int: Vec<hashbrown::HashSet<i64, BuildHasherDefault<ahash::AHasher>>>,
    sets_str: Vec<hashbrown::HashSet<u128, BuildHasherDefault<ahash::AHasher>>>,
    bitsets: Vec<Bitset>,
}

impl CdEntry {
    /// Count of distinct values seen for `group_idx`. The output of every
    /// CountDistinct accumulator finalisation, regardless of representation.
    #[inline]
    fn count(&self, group_idx: u32) -> i64 {
        let i = group_idx as usize;
        match self.kind {
            CdKind::Str => self.sets_str[i].len() as i64,
            CdKind::Int => self.sets_int[i].len() as i64,
            CdKind::DictBitset => self.bitsets[i].count_ones() as i64,
        }
    }
}

impl CountDistinctSideCar {
    /// Default constructor: every text CountDistinct uses the HashSet<u128>
    /// path. Phase D's bitset path is opted in via `new_with_dict_remaps`,
    /// which classifies eligible text specs as `DictBitset` instead.
    pub(super) fn new(agg_specs: &[AggExecSpec]) -> Self {
        Self::new_inner(agg_specs, &Default::default())
    }

    /// Phase D entry point. `dict_remap_sizes` maps spec_idx → bitset size
    /// (the global string-ID count) for every CountDistinct(text) spec the
    /// leader has confirmed is dict-encoded across all relevant segments.
    /// Specs absent from the map keep the HashSet<u128> behaviour.
    pub(super) fn new_with_dict_remaps(
        agg_specs: &[AggExecSpec],
        dict_remap_sizes: &std::collections::HashMap<usize, u32>,
    ) -> Self {
        Self::new_inner(agg_specs, dict_remap_sizes)
    }

    fn new_inner(
        agg_specs: &[AggExecSpec],
        dict_remap_sizes: &std::collections::HashMap<usize, u32>,
    ) -> Self {
        let mut entries = Vec::new();
        for (i, spec) in agg_specs.iter().enumerate() {
            if spec.agg_type == AggType::CountDistinct {
                let is_str = matches!(spec.col_type_oid,
                    pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID);
                let bitset_size = if is_str {
                    dict_remap_sizes.get(&i).copied().unwrap_or(0)
                } else { 0 };
                let kind = if is_str && bitset_size > 0 {
                    CdKind::DictBitset
                } else if is_str {
                    CdKind::Str
                } else {
                    CdKind::Int
                };
                entries.push(CdEntry {
                    spec_idx: i,
                    kind,
                    bitset_size,
                    sets_int: Vec::new(),
                    sets_str: Vec::new(),
                    bitsets: Vec::new(),
                });
            }
        }
        CountDistinctSideCar { entries }
    }

    pub(super) fn alloc_group(&mut self) {
        for e in &mut self.entries {
            match e.kind {
                CdKind::Str => e.sets_str.push(
                    hashbrown::HashSet::with_hasher(BuildHasherDefault::default())),
                CdKind::Int => e.sets_int.push(
                    hashbrown::HashSet::with_hasher(BuildHasherDefault::default())),
                CdKind::DictBitset => e.bitsets.push(Bitset::with_size(e.bitset_size)),
            }
        }
    }

    fn insert_int(&mut self, spec_idx: usize, group_idx: u32, val: i64) {
        for e in &mut self.entries {
            if e.spec_idx == spec_idx {
                e.sets_int[group_idx as usize].insert(val);
                return;
            }
        }
    }

    fn insert_str(&mut self, spec_idx: usize, group_idx: u32, hash: u128) {
        for e in &mut self.entries {
            if e.spec_idx == spec_idx {
                e.sets_str[group_idx as usize].insert(hash);
                return;
            }
        }
    }

    /// Phase D: set the bit for `global_id` in the per-group bitset of the
    /// (dict-eligible) CountDistinct(text) spec at `spec_idx`. `global_id`
    /// must be `< bitset_size` (`< DictDistinctRemap::global_count`).
    fn insert_dict_global(&mut self, spec_idx: usize, group_idx: u32, global_id: u32) {
        for e in &mut self.entries {
            if e.spec_idx == spec_idx {
                e.bitsets[group_idx as usize].set(global_id);
                return;
            }
        }
    }

    #[allow(dead_code)]
    fn len(&self, spec_idx: usize, group_idx: u32) -> i64 {
        for e in &self.entries {
            if e.spec_idx == spec_idx {
                return match e.kind {
                    CdKind::Str => e.sets_str[group_idx as usize].len() as i64,
                    CdKind::Int => e.sets_int[group_idx as usize].len() as i64,
                    CdKind::DictBitset => e.bitsets[group_idx as usize].count_ones() as i64,
                };
            }
        }
        0
    }

    fn union_from(&mut self, spec_idx: usize, dst_group: u32, other: &Self, src_group: u32) {
        for (e, oe) in self.entries.iter_mut().zip(other.entries.iter()) {
            if e.spec_idx == spec_idx {
                match e.kind {
                    CdKind::Str => {
                        let src = &oe.sets_str[src_group as usize];
                        e.sets_str[dst_group as usize].extend(src.iter().copied());
                    }
                    CdKind::Int => {
                        let src = &oe.sets_int[src_group as usize];
                        e.sets_int[dst_group as usize].extend(src.iter().copied());
                    }
                    CdKind::DictBitset => {
                        let src = &oe.bitsets[src_group as usize];
                        e.bitsets[dst_group as usize].or_with(src);
                    }
                }
                return;
            }
        }
    }

    /// Write cached counts into compact storage Count slots for top-N sorting.
    fn write_counts_to_storage(&self, storage: &mut CompactAccStorage, map: &CompactGroupMap) {
        for e in &self.entries {
            for (_, &gidx) in map.iter() {
                let count = match e.kind {
                    CdKind::Str => e.sets_str[gidx as usize].len() as i64,
                    CdKind::Int => e.sets_int[gidx as usize].len() as i64,
                    CdKind::DictBitset => e.bitsets[gidx as usize].count_ones() as i64,
                };
                unsafe { *storage.count_mut(gidx, e.spec_idx) = count; }
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Flat byte buffer holding compact accumulators for all groups.
pub(super) struct CompactAccStorage {
    pub(super) buf: Vec<u8>,
    pub(super) layout: CompactAccLayout,
    pub(super) str_arena: StringArena,
}

impl CompactAccStorage {
    pub(super) fn new(layout: CompactAccLayout) -> Self {
        CompactAccStorage {
            buf: Vec::new(),
            layout,
            str_arena: StringArena::new(),
        }
    }

    /// Reconstruct from a layout + raw `buf` bytes + raw string arena bytes.
    /// Used by parallel-agg DSM deserialise path; keeps byte interpretation
    /// behind a single constructor so tests cover a stable contract.
    #[allow(dead_code)] // wired by C.2.b's deserialiser
    pub(super) fn from_parts(layout: CompactAccLayout, buf: Vec<u8>, str_arena_buf: Vec<u8>) -> Self {
        CompactAccStorage {
            buf,
            layout,
            str_arena: StringArena { buf: str_arena_buf },
        }
    }


    /// Allocate accumulators for a new group. Returns the group index.
    ///
    /// Growth strategy: below 1GB, let Vec double normally. Above 1GB,
    /// grow by 2GB increments to cap peak waste at ~2GB instead of 100%.
    #[inline]
    pub(super) fn alloc_group(&mut self) -> u32 {
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
    pub(super) unsafe fn count_mut(&mut self, group_idx: u32, slot: usize) -> &mut i64 {
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
    pub(super) unsafe fn read_count(&self, group_idx: u32, slot: usize) -> i64 {
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
    pub(super) unsafe fn read_min_max_str(&self, group_idx: u32, slot: usize) -> (u32, u32) {
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
    pub(super) unsafe fn write_min_max_str(&mut self, group_idx: u32, slot: usize, off: u32, len: u32) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self.buf.as_mut_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            *(base as *mut u32) = off;
            *(base.add(4) as *mut u32) = len;
        }
    }

    /// Read MinInt/MaxInt: returns (value, has_value). `has_value=false` means
    /// no value has been observed yet (zero-init from `alloc_group`).
    #[inline]
    pub(super) unsafe fn read_min_max_int(&self, group_idx: u32, slot: usize) -> (i64, bool) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self.buf.as_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            let val = *(base as *const i64);
            let has = *(base.add(8) as *const i64);
            (val, has != 0)
        }
    }

    /// Write MinInt/MaxInt value + has_value flag.
    #[inline]
    pub(super) unsafe fn write_min_max_int(&mut self, group_idx: u32, slot: usize, val: i64, has: bool) {
        unsafe {
            let (offset, _) = self.layout.slots[slot];
            let base = self.buf.as_mut_ptr()
                .add(group_idx as usize * self.layout.group_stride + offset);
            *(base as *mut i64) = val;
            *(base.add(8) as *mut i64) = if has { 1 } else { 0 };
        }
    }

    /// Update MinInt: replace stored value if `candidate < stored` or no value yet.
    #[inline]
    pub(super) unsafe fn update_min_int(&mut self, group_idx: u32, slot: usize, candidate: i64) {
        unsafe {
            let (val, has) = self.read_min_max_int(group_idx, slot);
            if !has || candidate < val {
                self.write_min_max_int(group_idx, slot, candidate, true);
            }
        }
    }

    /// Update MaxInt: replace stored value if `candidate > stored` or no value yet.
    #[inline]
    pub(super) unsafe fn update_max_int(&mut self, group_idx: u32, slot: usize, candidate: i64) {
        unsafe {
            let (val, has) = self.read_min_max_int(group_idx, slot);
            if !has || candidate > val {
                self.write_min_max_int(group_idx, slot, candidate, true);
            }
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
                    || t == pg_sys::INT2OID || t == pg_sys::INT4OID || t == pg_sys::INT8OID
                    || t == pg_sys::DATEOID
                    || t == pg_sys::TIMESTAMPOID || t == pg_sys::TIMESTAMPTZOID
            }
            AggType::CountDistinct => {
                let t = spec.col_type_oid;
                t == pg_sys::INT2OID || t == pg_sys::INT4OID || t == pg_sys::INT8OID
                    || t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID
            }
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
    sort_is_avg: bool,
) -> Vec<(u128, u32)> {
    unsafe {
        let (_, kind) = storage.layout.slots[sort_slot];
        let read_val = |group_idx: u32| -> i64 {
            if sort_is_avg {
                let avg = match kind {
                    CompactAccKind::SumIntNarrow => {
                        let (s, c) = storage.read_sum_int_narrow(group_idx, sort_slot);
                        if c > 0 { s as f64 / c as f64 } else { 0.0 }
                    }
                    CompactAccKind::SumFloat => {
                        let (s, c) = storage.read_sum_float(group_idx, sort_slot);
                        if c > 0 { s / c as f64 } else { 0.0 }
                    }
                    _ => storage.read_count(group_idx, sort_slot) as f64,
                };
                let bits = avg.to_bits() as i64;
                if bits >= 0 { bits } else { bits ^ i64::MAX }
            } else {
                match kind {
                    CompactAccKind::Count => storage.read_count(group_idx, sort_slot),
                    CompactAccKind::SumIntNarrow => storage.read_sum_int_narrow(group_idx, sort_slot).0,
                    CompactAccKind::MinInt | CompactAccKind::MaxInt => {
                        storage.read_min_max_int(group_idx, sort_slot).0
                    }
                    _ => storage.read_count(group_idx, sort_slot),
                }
            }
        };
        if ascending {
            // Min-N: max-heap evicts the largest, keeping the smallest N
            let mut heap: BinaryHeap<(i64, u128, u32)> = BinaryHeap::with_capacity(limit + 1);
            for (&packed_key, &group_idx) in map {
                let val = read_val(group_idx);
                heap.push((val, packed_key, group_idx));
                if heap.len() > limit {
                    heap.pop();
                }
            }
            let mut result: Vec<(u128, u32)> = heap.into_iter().map(|(_, k, g)| (k, g)).collect();
            result.sort_by_key(|&(_, g)| read_val(g));
            result
        } else {
            // Max-N: min-heap (via Reverse) evicts the smallest, keeping the largest N
            let mut heap: BinaryHeap<Reverse<(i64, u128, u32)>> = BinaryHeap::with_capacity(limit + 1);
            for (&packed_key, &group_idx) in map {
                let val = read_val(group_idx);
                heap.push(Reverse((val, packed_key, group_idx)));
                if heap.len() > limit {
                    heap.pop();
                }
            }
            let mut result: Vec<(u128, u32)> = heap.into_iter().map(|Reverse((_, k, g))| (k, g)).collect();
            result.sort_by_key(|&(_, gb)| std::cmp::Reverse(read_val(gb)));
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
            CompactAccKind::MinInt | CompactAccKind::MaxInt => {
                let (val, has) = storage.read_min_max_int(group_idx, slot);
                if !has {
                    (pg_sys::Datum::from(0usize), true) // NULL
                } else {
                    // H.2: monotonic post-shift for the timestamptz_pl_interval
                    // recognizer. `OutputTransform::None` is the no-op identity.
                    let out = match spec.output_transform {
                        OutputTransform::None => val,
                        OutputTransform::PgUsShift { delta } => val.wrapping_add(delta),
                    };
                    (pg_sys::Datum::from(out as usize), false)
                }
            }
            CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                // Count was pre-written into the compact slot by write_counts_to_storage
                let count = storage.read_count(group_idx, slot);
                (pg_sys::Datum::from(count as usize), false)
            }
        }
    }
}

/// Phase C.2 activation — emit a partial-aggregate transition state into a
/// `(Datum, is_null)` pair for the Final Aggregate node above us to combine
/// via `aggcombinefn`. Mirrors `compact_finalize` but stops one step earlier:
/// returns the value at PG's `aggtranstype` rather than the user-visible
/// final type.
///
/// Coverage:
/// - `Count` → `int8` count (combinefn `int8pl`). Same as finalize for this slot.
/// - `SumIntNarrow` (SUM only) → `int8` sum (combinefn `int8pl`).
/// - `SumFloat` (SUM only) → `float8` sum (combinefn `float8_combine` —
///   actually pl, since float8 sum has no count component).
/// - `MinStr` / `MaxStr` → `text` directly (combinefn `text_smaller` /
///   `text_larger`).
/// - `SumInt` (SUM(int8) / AVG / SumIntNarrow AVG / SumFloat AVG) — NOT yet
///   implemented because the `aggtranstype = internal` path needs
///   `int8_avg_serialize` to produce a `bytea` partial state. Add eligibility
///   gate in `add_agg_path` so we don't reach this with an unsupported shape.
/// - `Count` for COUNT(DISTINCT) — has no `aggcombinefn` in PG core; must be
///   excluded by gating.
///
/// `unreachable!` in the unsupported branches is the bug-catcher: if planner
/// gating drifts and we land here at runtime, we'd produce silently-wrong
/// results otherwise.
#[allow(dead_code)] // wired by the C.2 activation planner code in path.rs
unsafe fn compact_emit_partial(
    storage: &CompactAccStorage,
    group_idx: u32,
    slot: usize,
    spec: &AggExecSpec,
) -> (pg_sys::Datum, bool) {
    unsafe {
        let (_, kind) = storage.layout.slots[slot];
        match kind {
            CompactAccKind::Count => {
                // partial state for `count` and `count(*)` is `int8`.
                let count = storage.read_count(group_idx, slot);
                (pg_sys::Datum::from(count as usize), false)
            }
            CompactAccKind::SumIntNarrow => {
                // SUM(int2/int4): partial state is `int8` (the running sum).
                // count is unused at the partial level for SUM. AVG path
                // not supported here yet — gating must reject it.
                if spec.agg_type != AggType::Sum {
                    unreachable!(
                        "compact_emit_partial: SumIntNarrow only supports Sum (got {:?}); \
                         planner gating drift",
                        spec.agg_type,
                    );
                }
                let (sum, _count) = storage.read_sum_int_narrow(group_idx, slot);
                (pg_sys::Datum::from(sum as usize), false)
            }
            CompactAccKind::SumFloat => {
                // SUM(float4/float8): partial state is `float8` (the running
                // sum). count is unused at the partial level. combinefn is
                // `float8pl`.
                if spec.agg_type != AggType::Sum {
                    unreachable!(
                        "compact_emit_partial: SumFloat only supports Sum (got {:?}); \
                         planner gating drift",
                        spec.agg_type,
                    );
                }
                let (sum, count) = storage.read_sum_float(group_idx, slot);
                if count == 0 {
                    return (pg_sys::Datum::from(0usize), true);
                }
                (pg_sys::Datum::from(sum.to_bits() as usize), false)
            }
            CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                // partial state for MIN/MAX is the value itself; combinefn
                // is `text_smaller` / `text_larger`. Same emit as finalize.
                let (off, len) = storage.read_min_max_str(group_idx, slot);
                if off == u32::MAX {
                    (pg_sys::Datum::from(0usize), true)
                } else {
                    let s = storage.str_arena.get(off, len);
                    let datum = string_to_datum(s, spec.col_type_oid);
                    (datum, false)
                }
            }
            CompactAccKind::MinInt | CompactAccKind::MaxInt => {
                // partial state for MIN/MAX(int|timestamp) is the value itself;
                // combinefn is `int*smaller`/`int*larger` (or `timestamp*_smaller`
                // / `timestamp*_larger`). Same emit as finalize — apply the
                // monotonic OutputTransform here too so the post-shift value
                // is what flows up to PG's combinefn.
                let (val, has) = storage.read_min_max_int(group_idx, slot);
                if !has {
                    (pg_sys::Datum::from(0usize), true)
                } else {
                    let out = match spec.output_transform {
                        OutputTransform::None => val,
                        OutputTransform::PgUsShift { delta } => val.wrapping_add(delta),
                    };
                    (pg_sys::Datum::from(out as usize), false)
                }
            }
            CompactAccKind::SumInt => {
                // SUM(int8) partial state is `internal` via int8_avg_serialize
                // → `bytea`. Not implemented yet; gating must reject SUM(int8)
                // / AVG until we wire int8_avg_serialize.
                unreachable!(
                    "compact_emit_partial: SumInt (transtype=internal) not yet supported \
                     for partial emit; planner gating drift",
                );
            }
            CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                unreachable!(
                    "compact_emit_partial: COUNT(DISTINCT) has no PG aggcombinefn; \
                     planner gating drift",
                );
            }
        }
    }
}

// ============================================================================
// Packed Integer Keys (Phase 2)
// ============================================================================

/// Phase C.2.f — public re-export for `path.rs::add_agg_path`. Diverging the
/// path-level and exec-level eligibility checks would silently mismatch
/// leader and worker, so add_agg_path calls into the canonical predicate.
///
/// Callers in the planner don't have segment metadata loaded, so they pass
/// `&[]` for `col_not_null`. The canonical predicate then rejects every
/// `Column` / `DateTrunc` / `Extract` / `AddConst` group key — leaving only
/// `group_specs.is_empty()` as a viable parallel-agg shape at plan time.
pub(crate) fn can_use_compact_keys_path(
    group_specs: &[GroupByColSpec],
    col_not_null: &[bool],
) -> bool {
    can_use_compact_keys(group_specs, col_not_null)
}

/// Check if all GROUP BY columns produce integer values and can be packed into u128.
fn can_use_compact_keys(group_specs: &[GroupByColSpec], col_not_null: &[bool]) -> bool {
    if group_specs.is_empty() || group_specs.len() > 2 {
        return false; // u128 fits at most 2 x i64
    }
    group_specs.iter().all(|gs| {
        match &gs.expr {
            GroupByExpr::Column => {
                if !col_not_null.get(gs.col_idx as usize).copied().unwrap_or(false) {
                    return false;
                }
                let t = gs.type_oid;
                t == pg_sys::INT2OID || t == pg_sys::INT4OID || t == pg_sys::INT8OID
                    || t == pg_sys::TIMESTAMPOID || t == pg_sys::TIMESTAMPTZOID
            }
            GroupByExpr::DateTrunc { .. } | GroupByExpr::Extract { .. } | GroupByExpr::AddConst { .. } => {
                col_not_null.get(gs.col_idx as usize).copied().unwrap_or(false)
            }
            GroupByExpr::RegexpReplace { .. } => false,
            GroupByExpr::CaseWhen(_) => false,
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
pub(super) type CompactGroupMap = hashbrown::HashMap<u128, u32, BuildHasherDefault<ahash::AHasher>>;

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
///
/// No-null fast path: writes `(Datum, false)` tuples directly from the decoder
/// output in a single pass, skipping the intermediate `Vec<Datum>`. Cuts
/// allocations per column-per-segment from 3–4 down to 2 — material on
/// queries like Q40 that filter on multiple i64 hash columns across many
/// segments (see `QUERY_ANALYSIS.md` #48 investigation).
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

    // Build `Vec<(Datum, bool)>` in two branches: no-null fast path (single
    // allocation for the output) vs null-containing path (decode into
    // `Vec<Datum>` then weave nulls).
    if cc.null_bitmap.is_empty() {
        return decompress_numeric_no_nulls(&cc, type_oid, total_count);
    }

    let nn_datums = decompress_numeric_nn_datums(&cc, type_oid, non_null_count);
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

/// No-null fast path for `decompress_numeric_blob`: write `(Datum, false)`
/// tuples directly from decoder output. Saves one `Vec<Datum>` allocation
/// + one copy pass vs the null-containing path.
#[inline]
fn decompress_numeric_no_nulls(
    cc: &compression::CompressedColumnRef<'_>,
    type_oid: pg_sys::Oid,
    total_count: usize,
) -> Vec<(pg_sys::Datum, bool)> {
    let mut out = Vec::with_capacity(total_count);
    match cc.type_tag {
        compression::CompressionType::Gorilla => {
            if type_oid == pg_sys::TIMESTAMPOID || type_oid == pg_sys::TIMESTAMPTZOID {
                let timestamps = compression::gorilla::decode_timestamps(cc.data, total_count);
                for usec in timestamps {
                    let pg_usec = usec - PG_EPOCH_OFFSET_USEC;
                    out.push((pg_sys::Datum::from(pg_usec as usize), false));
                }
            } else if type_oid == pg_sys::DATEOID {
                let timestamps = compression::gorilla::decode_timestamps(cc.data, total_count);
                for usec in timestamps {
                    let unix_days = (usec / 86_400_000_000) as i32;
                    let pg_days = unix_days - PG_EPOCH_OFFSET_DAYS;
                    out.push((pg_sys::Datum::from(pg_days as usize), false));
                }
            } else if type_oid == pg_sys::FLOAT4OID {
                let floats = compression::gorilla::decode_floats_f32(cc.data, total_count);
                for v in floats {
                    out.push((pg_sys::Datum::from(v.to_bits() as usize), false));
                }
            } else {
                // FLOAT8OID
                let floats = compression::gorilla::decode_floats(cc.data, total_count);
                for v in floats {
                    out.push((pg_sys::Datum::from(v.to_bits() as usize), false));
                }
            }
        }
        compression::CompressionType::DeltaVarint => {
            if type_oid == pg_sys::INT2OID {
                let ints = compression::integer::decode_i32(cc.data, total_count);
                for v in ints {
                    out.push((pg_sys::Datum::from(v as i16 as usize), false));
                }
            } else if type_oid == pg_sys::INT4OID || type_oid == pg_sys::DATEOID {
                let ints = compression::integer::decode_i32(cc.data, total_count);
                for v in ints {
                    out.push((pg_sys::Datum::from(v as usize), false));
                }
            } else {
                // INT8OID, TIMESTAMPOID, TIMESTAMPTZOID
                let ints = compression::integer::decode_i64(cc.data, total_count);
                for v in ints {
                    out.push((pg_sys::Datum::from(v as usize), false));
                }
            }
        }
        compression::CompressionType::Constant => {
            if type_oid == pg_sys::INT2OID {
                let ints = compression::bitpacked::decode_constant_i32(cc.data, total_count);
                for v in ints {
                    out.push((pg_sys::Datum::from(v as i16 as usize), false));
                }
            } else if type_oid == pg_sys::INT4OID || type_oid == pg_sys::DATEOID {
                let ints = compression::bitpacked::decode_constant_i32(cc.data, total_count);
                for v in ints {
                    out.push((pg_sys::Datum::from(v as usize), false));
                }
            } else if type_oid == pg_sys::FLOAT4OID {
                let ints = compression::bitpacked::decode_constant_i64(cc.data, total_count);
                for v in ints {
                    out.push((pg_sys::Datum::from(f32::from_bits(v as u32).to_bits() as usize), false));
                }
            } else if type_oid == pg_sys::FLOAT8OID {
                let ints = compression::bitpacked::decode_constant_i64(cc.data, total_count);
                for v in ints {
                    out.push((pg_sys::Datum::from(f64::from_bits(v as u64).to_bits() as usize), false));
                }
            } else {
                let ints = compression::bitpacked::decode_constant_i64(cc.data, total_count);
                for v in ints {
                    out.push((pg_sys::Datum::from(v as usize), false));
                }
            }
        }
        compression::CompressionType::ForBitpacked => {
            if type_oid == pg_sys::INT2OID {
                let ints = compression::bitpacked::decode_for_i32(cc.data, total_count);
                for v in ints {
                    out.push((pg_sys::Datum::from(v as i16 as usize), false));
                }
            } else if type_oid == pg_sys::INT4OID || type_oid == pg_sys::DATEOID {
                let ints = compression::bitpacked::decode_for_i32(cc.data, total_count);
                for v in ints {
                    out.push((pg_sys::Datum::from(v as usize), false));
                }
            } else {
                let ints = compression::bitpacked::decode_for_i64(cc.data, total_count);
                for v in ints {
                    out.push((pg_sys::Datum::from(v as usize), false));
                }
            }
        }
        compression::CompressionType::BooleanBitmap => {
            let bools = compression::boolean::decode(cc.data, total_count);
            for b in bools {
                out.push((pg_sys::Datum::from(b as usize), false));
            }
        }
        _ => {
            // Text/dictionary/lz4 types — should not happen in compact path
        }
    }
    out
}

/// Null-containing path: decode only the non-null values into `Vec<Datum>`.
/// Caller weaves nulls back into the final output.
#[inline]
fn decompress_numeric_nn_datums(
    cc: &compression::CompressedColumnRef<'_>,
    type_oid: pg_sys::Oid,
    non_null_count: usize,
) -> Vec<pg_sys::Datum> {
    match cc.type_tag {
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
        _ => Vec::new(),
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
pub(super) struct ParallelCompactResult {
    pub(super) compact_map: CompactGroupMap,
    pub(super) compact_storage: CompactAccStorage,
    pub(super) cd_sidecar: CountDistinctSideCar,
    pub(super) segments_processed: u64,
    pub(super) rows_processed: u64,
    pub(super) decompress_us: u64,
    /// Pre-computed top-K candidates: (keys, floor_value).
    /// Present when `config.topn_spec` is set.
    pub(super) topk: Option<(Vec<u128>, i64)>,
}

impl ParallelCompactResult {
    /// Construct an empty result for the given agg specs. Used by parallel
    /// workers as the per-process accumulator before serialising to DSM.
    #[allow(dead_code)] // wired by C.2.d
    pub(super) fn empty(agg_specs: &[AggExecSpec]) -> Self {
        Self {
            compact_map: CompactGroupMap::with_hasher(BuildHasherDefault::default()),
            compact_storage: CompactAccStorage::new(CompactAccLayout::new(agg_specs)),
            cd_sidecar: CountDistinctSideCar::new(agg_specs),
            segments_processed: 0,
            rows_processed: 0,
            decompress_us: 0,
            topk: None,
        }
    }
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
    let mut cd_sidecar = CountDistinctSideCar::new(config.agg_specs);
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
        if segment_skippable_by_dict(
            config.batch_quals, config.col_names, config.segment_by, &seg.compressed_blobs,
        ) {
            continue;
        }

        // C.3 per-segment fast path: classify the segment vs batch_quals
        // using col_minmax / nonzero_count metadata BEFORE any decompression.
        // The partial path's eligibility predicate ensures every batch_qual
        // is numeric (`is_batch_comparable_type`), so classify_segment_quals
        // always has the metadata it needs — no Ambiguous-from-mixed-types
        // edge case here (that's only relevant in `process_segments_mixed`).
        let seg_qual_result = if config.batch_quals.is_empty() {
            SegmentQualResult::AllPass
        } else {
            classify_segment_quals(seg, config.batch_quals, config.col_names)
        };
        if matches!(seg_qual_result, SegmentQualResult::NonePass) {
            // No row in this segment passes the quals — skip entirely.
            // Saves the full decompression cost for the segment.
            continue;
        }
        let quals_all_pass = matches!(seg_qual_result, SegmentQualResult::AllPass);

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

        // Evaluate batch quals — but only if metadata couldn't already
        // prove all rows pass. AllPass means we can use an empty selection
        // (every row included) and skip the per-row qual evaluation loop.
        let selection = if quals_all_pass {
            Vec::new()
        } else {
            evaluate_batch_quals(&decompressed, row_count, config.batch_quals, Vec::new())
        };

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
                    GroupByExpr::Extract { unit, divisor, .. } => {
                        eval_extract(col[row].0.value() as i64, *divisor, unit)
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
                    cd_sidecar.alloc_group();
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
                    CompactAccKind::MinInt => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = col[row].0.value() as i64;
                            unsafe { compact_storage.update_min_int(group_idx, spec_idx, v); }
                        }
                    }
                    CompactAccKind::MaxInt => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = col[row].0.value() as i64;
                            unsafe { compact_storage.update_max_int(group_idx, spec_idx, v); }
                        }
                    }
                    CompactAccKind::CountDistinctInt => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            cd_sidecar.insert_int(spec_idx, group_idx, col[row].0.value() as i64);
                        }
                    }
                    CompactAccKind::CountDistinctStr => {
                        // compact path requires all_needed_cols_numeric
                        unreachable!("CountDistinctStr in compact parallel worker")
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
                    CompactAccKind::MinInt | CompactAccKind::MaxInt => {
                        compact_storage.read_min_max_int(gidx, sort_slot).0
                    }
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

    // Write CD counts to compact storage before top-K evaluation
    cd_sidecar.write_counts_to_storage(&mut compact_storage, &compact_map);

    ParallelCompactResult {
        compact_map,
        compact_storage,
        cd_sidecar,
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
    global_cd: &mut CountDistinctSideCar,
    worker_map: &CompactGroupMap,
    worker_storage: &CompactAccStorage,
    worker_cd: &CountDistinctSideCar,
    agg_specs: &[AggExecSpec],
) {
    for (&packed_key, &worker_group_idx) in worker_map {
        let global_group_idx = match global_map.entry(packed_key) {
            hashbrown::hash_map::Entry::Occupied(e) => *e.get(),
            hashbrown::hash_map::Entry::Vacant(e) => {
                let idx = global_storage.alloc_group();
                global_cd.alloc_group();
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
                CompactAccKind::MinInt => unsafe {
                    let (w_val, w_has) = worker_storage.read_min_max_int(worker_group_idx, slot_idx);
                    if w_has {
                        global_storage.update_min_int(global_group_idx, slot_idx, w_val);
                    }
                },
                CompactAccKind::MaxInt => unsafe {
                    let (w_val, w_has) = worker_storage.read_min_max_int(worker_group_idx, slot_idx);
                    if w_has {
                        global_storage.update_max_int(global_group_idx, slot_idx, w_val);
                    }
                },
                CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                    global_cd.union_from(slot_idx, global_group_idx, worker_cd, worker_group_idx);
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

// strcoll_cmp is now in text_col.rs

/// Check if a GROUP BY column is a text type (including RegexpReplace/CaseWhen which produce text).
fn is_text_group_col(gs: &GroupByColSpec) -> bool {
    let is_text_type = matches!(gs.type_oid,
        pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID | pg_sys::NAMEOID);
    match &gs.expr {
        GroupByExpr::Column | GroupByExpr::RegexpReplace { .. } | GroupByExpr::CaseWhen(_) => is_text_type,
        _ => false,
    }
}

fn case_when_references_col(spec: &CaseWhenSpec, col_idx: usize) -> bool {
    spec.clauses.iter().any(|clause| {
        clause.conditions.iter().any(|cond| cond.col_idx == col_idx)
            || matches!(&clause.result, CaseWhenValue::ColumnRef(ci) if *ci == col_idx)
    }) || matches!(&spec.default, CaseWhenValue::ColumnRef(ci) if *ci == col_idx)
}

fn numeric_col_used_only_by_constant_group_keys(
    col_idx: usize,
    group_specs: &[GroupByColSpec],
    const_group_keys: &[Option<i64>],
    batch_quals: &[BatchQual],
    agg_specs: &[AggExecSpec],
) -> bool {
    if batch_quals.iter().any(|bq| bq.col_idx == col_idx) {
        return false;
    }
    if agg_specs.iter().any(|spec| spec.col_idx >= 0 && spec.col_idx as usize == col_idx) {
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

/// Check if pattern contains POSIX character classes like [:alpha:] inside bracket expressions.
fn has_posix_classes(pattern: &str) -> bool {
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
fn convert_pg_replacement(replacement: &str) -> String {
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
fn pg_pattern_to_rust(pattern: &str) -> String {
    let mut result = String::with_capacity(pattern.len() + 8);
    // Enable dot-all mode so . matches \n (matching PG's ARE default)
    result.push_str("(?s)");

    // Replace unescaped $ at end of pattern with \z
    if let Some(prefix) = pattern.strip_suffix('$') {
        let preceding_backslashes = prefix
            .chars().rev().take_while(|&c| c == '\\').count();
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
fn try_compile_rust_regex(pattern: &str) -> Option<Regex> {
    if !crate::get_parallel_regex() {
        return None;
    }
    if has_posix_classes(pattern) {
        warning!("pg_deltax: regex pattern contains POSIX character classes, falling back to PG regex (pattern: {})", pattern);
        return None;
    }
    let rust_pattern = pg_pattern_to_rust(pattern);
    match Regex::new(&rust_pattern) {
        Ok(re) => Some(re),
        Err(e) => {
            warning!("pg_deltax: regex pattern not supported by Rust regex crate, falling back to PG regex (pattern: {}, error: {})", pattern, e);
            None
        }
    }
}

/// Info for a regexp GROUP BY column that compiled successfully with Rust regex.
struct RustRegexInfo {
    regex: Regex,
    replacement: String,
    col_idx: usize,
}

/// Evaluate a CASE WHEN expression on a segment, producing a SegTextColumn.
///
/// For each row, evaluates clauses in order; first match wins, else default.
/// Condition columns come from `numeric_cols`, result ColumnRef values from `text_seg_cols`.
fn apply_case_when_to_seg_col(
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

    SegTextColumn::Dict { entries, row_to_entry }
}

/// Apply a Rust regex replacement to a SegTextColumn, producing a new transformed column.
/// The original column is not modified (needed for aggregations on the same column).
/// For Dict columns, only applies regex to unique dict entries (O(dict_size)).
/// For LZ4 columns, converts to Dict after applying regex.
fn apply_regex_to_seg_col(seg_col: &SegTextColumn, regex: &Regex, replacement: &str) -> SegTextColumn {
    match seg_col {
        SegTextColumn::Dict { entries, row_to_entry } => {
            let new_entries: Vec<String> = entries.iter()
                .map(|e| regex.replace(e, replacement).into_owned())
                .collect();
            SegTextColumn::Dict { entries: new_entries, row_to_entry: row_to_entry.clone() }
        }
        SegTextColumn::Lz4 { buf, row_to_range } => {
            let mut unique_map: HashMap<String, u32> = HashMap::new();
            let mut entries: Vec<String> = Vec::new();
            let mut new_row_to_entry: Vec<u32> = Vec::with_capacity(row_to_range.len());
            for &(off, len) in row_to_range {
                if off == u32::MAX {
                    new_row_to_entry.push(u32::MAX);
                } else {
                    let s = std::str::from_utf8(&buf[off as usize..off as usize + len as usize]).unwrap_or("");
                    let replaced = regex.replace(s, replacement).into_owned();
                    let idx = *unique_map.entry(replaced.clone()).or_insert_with(|| {
                        let idx = entries.len() as u32;
                        entries.push(replaced);
                        idx
                    });
                    new_row_to_entry.push(idx);
                }
            }
            SegTextColumn::Dict { entries, row_to_entry: new_row_to_entry }
        }
        SegTextColumn::SegBy(opt) => {
            let new_opt = opt.as_deref().map(|s| regex.replace(s, replacement).into_owned());
            SegTextColumn::SegBy(new_opt)
        }
        SegTextColumn::Lengths { lengths, null_bitmap } => {
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
    /// Compiled Rust regex info for RegexpReplace GROUP BY columns
    rust_regex_infos: &'a [RustRegexInfo],
    /// Per-column flag: true when the text column is loaded in sidecar-only
    /// mode (length blob instead of main blob). Parallel to col_names.
    sidecar_only_cols: &'a [bool],
    /// F8 optimization: when `Some`, filter phase-1 rows by group-key
    /// hash. Only rows whose `hash_mixed_key` output is in this set are
    /// inserted into the per-worker map; all others are skipped. This
    /// bounds each worker's map to `|preselected|` entries instead of
    /// the full group cardinality. Set iff the bare-LIMIT shape matches
    /// (no ORDER BY, no HAVING, no WHERE) and the Phase-0 probe
    /// succeeded in finding `bare_limit` distinct keys.
    preselected_keys: Option<&'a hashbrown::HashSet<u128>>,
    /// Phase D: leader-precomputed dict-distinct remaps. Keyed by spec_idx
    /// for every CountDistinct(text) spec where every segment is dict-encoded
    /// for the col AND the global-string count is below the bitset threshold.
    /// Workers consult this to set bits in per-(spec, group) `Bitset`s
    /// (`CdKind::DictBitset`) instead of hashing strings into `HashSet<u128>`.
    /// Specs absent from the map keep the existing HashSet path. The
    /// chunk-offset arg to `process_segments_mixed` indexes into
    /// `per_segment` so each worker resolves `(seg_idx, local_dict_id)` →
    /// `global_id` without further coordination.
    dict_distinct_remaps: &'a std::collections::HashMap<usize, DictDistinctRemap>,
}

// TextQualInfo is now in text_col.rs

/// Result of parallel mixed aggregation from one worker thread.
struct ParallelMixedResult {
    compact_map: CompactGroupMap,
    compact_storage: CompactAccStorage,
    mixed_keys: MixedKeyStorage,
    cd_sidecar: CountDistinctSideCar,
    segments_processed: u64,
    rows_processed: u64,
    decompress_us: u64,
    topk: Option<(Vec<u128>, i64)>,
}

// decompress_text_to_seg_col is now in text_col.rs

// apply_text_eq_filter and apply_text_like_filter are now in text_col.rs

/// Check if a query can use the parallel mixed (int+string) aggregation path.
fn can_parallel_mixed(
    group_specs: &[GroupByColSpec],
    needed_cols: &[bool],
    col_types: &[pg_sys::Oid],
    col_not_null: &[bool],
    batch_quals: &[BatchQual],
    agg_specs: &[AggExecSpec],
) -> bool {
    if group_specs.is_empty() || group_specs.len() > 6 {
        return false;
    }

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
            || (matches!(s.agg_type, AggType::Min | AggType::Max | AggType::CountDistinct)
                && (t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID))
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
                if !(t == pg_sys::INT2OID || t == pg_sys::INT4OID || t == pg_sys::INT8OID
                    || t == pg_sys::TIMESTAMPOID || t == pg_sys::TIMESTAMPTZOID)
                {
                    return false;
                }
                if !col_not_null.get(gs.col_idx as usize).copied().unwrap_or(false) {
                    return false;
                }
            }
            GroupByExpr::DateTrunc { .. } | GroupByExpr::Extract { .. } | GroupByExpr::AddConst { .. } => {
                if !col_not_null.get(gs.col_idx as usize).copied().unwrap_or(false) {
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
        let is_text_gb = group_specs.iter().any(|gs| gs.col_idx as usize == i && is_text_group_col(gs));
        // Also check if this column is referenced by a CaseWhen result ColumnRef
        let is_case_when_ref = group_specs.iter().any(|gs| {
            if let GroupByExpr::CaseWhen(ref spec) = gs.expr {
                spec.clauses.iter().any(|c| matches!(&c.result, CaseWhenValue::ColumnRef(ci) if *ci == i))
                    || matches!(&spec.default, CaseWhenValue::ColumnRef(ci) if *ci == i)
            } else {
                false
            }
        });
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
        let is_text_cd_agg = agg_specs.iter().any(|s| {
            s.col_idx as usize == i
                && s.agg_type == AggType::CountDistinct
                && (type_oid == pg_sys::TEXTOID || type_oid == pg_sys::VARCHAROID || type_oid == pg_sys::BPCHAROID)
        });
        let is_text_length_agg = agg_specs.iter().any(|s| {
            s.col_idx as usize == i
                && s.expr_kind == AggExpr::LengthOf
                && (type_oid == pg_sys::TEXTOID || type_oid == pg_sys::VARCHAROID || type_oid == pg_sys::BPCHAROID)
        });
        if !is_text_gb && !is_case_when_ref && !has_text_qual && !is_text_minmax_agg && !is_text_cd_agg && !is_text_length_agg {
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
                    if bq.text_const.is_none() { return false; }
                }
                BatchCompareOp::Like | BatchCompareOp::NotLike => {
                    if bq.like_strategy.is_none() { return false; }
                }
                BatchCompareOp::InList => {
                    if bq.in_list_text.is_none() { return false; }
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
fn try_build_preselected(
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

    let n_int_keys = group_specs.iter().filter(|gs| !is_text_group_col(gs)).count();
    let n_str_keys = group_specs.iter().filter(|gs| is_text_group_col(gs)).count();

    let mut keys: hashbrown::HashSet<u128> =
        hashbrown::HashSet::with_capacity(bare_limit.max(16));

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
                        GroupByExpr::AddConst { offset, .. } => {
                            col[row].0.value() as i64 + offset
                        }
                        GroupByExpr::Column => col[row].0.value() as i64,
                        _ => unreachable!(),
                    };
                    int_idx += 1;
                }
            }
            if has_null {
                continue;
            }

            let hash_key = hash_mixed_key(
                &int_keys_buf[..n_int_keys],
                &str_keys_buf[..n_str_keys],
            );
            keys.insert(hash_key);
            if keys.len() >= bare_limit {
                return Some(keys);
            }
        }
    }

    // Probe exhausted without reaching bare_limit distinct keys.
    None
}

fn process_segments_mixed(
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
    let mut cd_sidecar = CountDistinctSideCar::new_with_dict_remaps(
        config.agg_specs,
        &dict_remap_sizes,
    );
    let mut segments_processed: u64 = 0;
    let mut rows_processed: u64 = 0;
    let mut decompress_us: u64 = 0;

    // Count int and str group keys
    let n_int_keys = config.group_specs.iter().filter(|gs| !is_text_group_col(gs)).count();
    let n_str_keys = config.group_specs.iter().filter(|gs| is_text_group_col(gs)).count();

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
        if segment_skippable_by_dict(
            config.batch_quals, config.col_names, config.segment_by, &seg.compressed_blobs,
        ) {
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
            } else if config.sidecar_only_cols.get(col_idx).copied().unwrap_or(false) {
                // Text column in sidecar-only mode: main blob wasn't loaded;
                // decode the length sidecar instead.
                let sidecar_blob = &seg.text_length_blobs[blob_idx];
                text_seg_cols.push(decompress_length_sidecar(sidecar_blob));
                numeric_cols.push(Vec::new());
                blob_idx += 1;
            } else {
                let blob = &seg.compressed_blobs[blob_idx];
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
                blob_idx += 1;
            }
        }
        // Build regex-transformed text columns for GROUP BY keys (separate from originals,
        // since the original column may also be needed for aggregation like MIN/AVG/length)
        let regex_text_cols: Vec<Option<SegTextColumn>> = if config.rust_regex_infos.is_empty() {
            Vec::new()
        } else {
            let mut cols: Vec<Option<SegTextColumn>> = (0..config.col_names.len()).map(|_| None).collect();
            for ri in config.rust_regex_infos {
                if let Some(ref seg_col) = text_seg_cols[ri.col_idx] {
                    cols[ri.col_idx] = Some(apply_regex_to_seg_col(seg_col, &ri.regex, &ri.replacement));
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
                TextQualInfo::EqNe { col_idx, const_str, is_ne } => {
                    if let Some(ref seg_col) = text_seg_cols[*col_idx] {
                        apply_text_eq_filter(seg_col, const_str, *is_ne, row_count, &mut selection);
                    }
                }
                TextQualInfo::Like { col_idx, strategy, negate } => {
                    if let Some(ref seg_col) = text_seg_cols[*col_idx] {
                        apply_text_like_filter(seg_col, strategy, *negate, row_count, &mut selection);
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
        let case_when_text_cols: Vec<Option<SegTextColumn>> = config.group_specs.iter().map(|gs| {
            if let GroupByExpr::CaseWhen(ref spec) = gs.expr {
                Some(apply_case_when_to_seg_col(spec, &numeric_cols, &text_seg_cols, row_count, &selection))
            } else {
                None
            }
        }).collect();

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
                            GroupByExpr::Column => {
                                col[row].0.value() as i64
                            }
                            _ => unreachable!(),
                        };
                    }
                    int_idx += 1;
                }
            }

            if has_null { continue; }

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
            let group_idx = match compact_map.entry(hash_key) {
                hashbrown::hash_map::Entry::Occupied(e) => *e.get(),
                hashbrown::hash_map::Entry::Vacant(e) => {
                    let idx = compact_storage.alloc_group();
                    cd_sidecar.alloc_group();
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
                        } else if spec.expr_kind == AggExpr::LengthOf
                            && let Some(ref seg_col) = text_seg_cols[spec.col_idx as usize]
                            && let Some(len) = seg_col.get_len(row) {
                            let (sum, count) = unsafe { compact_storage.sum_int_mut(group_idx, spec_idx) };
                            *sum += len as i128;
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
                        } else if spec.expr_kind == AggExpr::LengthOf
                            && let Some(ref seg_col) = text_seg_cols[spec.col_idx as usize]
                            && let Some(len) = seg_col.get_len(row) {
                            let (sum, count) = unsafe { compact_storage.sum_int_narrow_mut(group_idx, spec_idx) };
                            *sum += len as i64;
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
                        } else if spec.expr_kind == AggExpr::LengthOf
                            && let Some(ref seg_col) = text_seg_cols[spec.col_idx as usize]
                            && let Some(len) = seg_col.get_len(row) {
                            let (sum, count) = unsafe { compact_storage.sum_float_mut(group_idx, spec_idx) };
                            *sum += len as f64;
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
                                    let cmp = strcoll_cmp(s, cur);
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
                    CompactAccKind::MinInt => {
                        let col = &numeric_cols[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = col[row].0.value() as i64;
                            unsafe { compact_storage.update_min_int(group_idx, spec_idx, v); }
                        }
                    }
                    CompactAccKind::MaxInt => {
                        let col = &numeric_cols[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = col[row].0.value() as i64;
                            unsafe { compact_storage.update_max_int(group_idx, spec_idx, v); }
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
                        let Some(ref seg_col) = text_seg_cols[col_idx] else { continue };
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
        cd_sidecar,
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
                5, 0, 0, 23, 0, 0,  // Min, col=0, OutputTransform=None
                6, 0, 0, 23, 0, 0,  // Max, col=0, OutputTransform=None
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
        // GROUP BY extract(minute FROM ts): expr_tag=3, divisor=0
        unsafe {
            let unit = b"minute";
            let mut vals = vec![
                42i32, -1,
                0,                  // num_aggs
                1,                  // num_groups
                0, 1184, 3,        // col_idx=0, TIMESTAMPTZOID, expr_tag=3(Extract)
                200,               // func_oid
                0, 0,              // divisor (i64 hi/lo): 0 = pg_usec input
                unit.len() as i32,
            ];
            for &b in unit.iter() {
                vals.push(b as i32);
            }
            let list = build_int_list(&vals);

            let plan = parse_agg_private(list);
            match &plan.group_specs[0].expr {
                GroupByExpr::Extract { unit, func_oid, divisor } => {
                    assert_eq!(unit, "minute");
                    assert_eq!(*func_oid, 200);
                    assert_eq!(*divisor, 0);
                }
                other => panic!("expected Extract, got {:?}", other),
            }
        }
    }

    #[pg_test]
    fn test_parse_group_by_extract_with_divisor() {
        // GROUP BY extract(hour FROM to_timestamp(bigint_col / 1_000_000)):
        // expr_tag=3 with non-zero divisor.
        unsafe {
            let unit = b"hour";
            let divisor: i64 = 1_000_000;
            let mut vals = vec![
                42i32, -1,
                0,                  // num_aggs
                1,                  // num_groups
                0, 20, 3,          // col_idx=0, INT8OID, expr_tag=3(Extract)
                200,               // func_oid
                (divisor >> 32) as i32,
                divisor as i32,
                unit.len() as i32,
            ];
            for &b in unit.iter() {
                vals.push(b as i32);
            }
            let list = build_int_list(&vals);

            let plan = parse_agg_private(list);
            match &plan.group_specs[0].expr {
                GroupByExpr::Extract { unit, func_oid, divisor } => {
                    assert_eq!(unit, "hour");
                    assert_eq!(*func_oid, 200);
                    assert_eq!(*divisor, 1_000_000);
                }
                other => panic!("expected Extract, got {:?}", other),
            }
        }
    }

    #[test]
    fn test_extract_subday_from_bigint_scaled_hour() {
        // 2024-05-09 12:34:56 UTC = unix epoch seconds 1715258096
        // hour-of-day at that point is 12.
        let unix_us: i64 = 1_715_258_096_000_000;
        assert_eq!(
            extract_subday_from_bigint_scaled(unix_us, 1_000_000, "hour"),
            12,
        );
        // minute = 34, second = 56
        assert_eq!(
            extract_subday_from_bigint_scaled(unix_us, 1_000_000, "minute"),
            34,
        );
        assert_eq!(
            extract_subday_from_bigint_scaled(unix_us, 1_000_000, "second"),
            56,
        );
        // Pre-unix-epoch (negative) — div_euclid handles the sign correctly,
        // so hour-of-day is still positive within [0, 24).
        let pre_epoch: i64 = -1; // -1us before 1970 → 23:59:59 prev day
        assert_eq!(
            extract_subday_from_bigint_scaled(pre_epoch, 1_000_000, "hour"),
            23,
        );
    }

    #[test]
    fn test_constant_extract_key_for_segment_same_hour() {
        let cm = super::super::segments::ColMinMax {
            min_encoded: 1_715_258_096_000_000,
            max_encoded: 1_715_259_000_000_000,
            min_null: false,
            max_null: false,
            type_oid: pg_sys::INT8OID,
        };
        assert_eq!(
            constant_extract_key_for_segment(&cm, 1_000_000, "hour"),
            Some(12),
        );
    }

    #[test]
    fn test_constant_extract_key_for_segment_crossing_hour() {
        let cm = super::super::segments::ColMinMax {
            min_encoded: 1_715_259_599_000_000,
            max_encoded: 1_715_259_601_000_000,
            min_null: false,
            max_null: false,
            type_oid: pg_sys::INT8OID,
        };
        assert_eq!(
            constant_extract_key_for_segment(&cm, 1_000_000, "hour"),
            None,
        );
    }

    #[test]
    fn test_constant_extract_key_for_segment_same_minute() {
        let cm = super::super::segments::ColMinMax {
            min_encoded: 10 * 60_000_000 + 20_000_000,
            max_encoded: 10 * 60_000_000 + 40_000_000,
            min_null: false,
            max_null: false,
            type_oid: pg_sys::INT8OID,
        };
        assert_eq!(
            constant_extract_key_for_segment(&cm, 0, "minute"),
            Some(10),
        );
    }

    #[test]
    fn test_constant_extract_key_for_segment_same_minute_value_across_hour() {
        let cm = super::super::segments::ColMinMax {
            min_encoded: 27 * 60_000_000,
            max_encoded: 87 * 60_000_000,
            min_null: false,
            max_null: false,
            type_oid: pg_sys::INT8OID,
        };
        assert_eq!(
            constant_extract_key_for_segment(&cm, 0, "minute"),
            None,
        );
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
            col_not_null: col_names.iter().map(|_| false).collect(),
            segment_by: Vec::new(),
            order_by: Vec::new(),
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
            derived_minmax_topn: None,
            bare_limit: 0,
            is_partial: false,
        }
    }

    fn make_agg_spec(agg_type: AggType, col_idx: i32, col_type_oid: u32) -> AggExecSpec {
        AggExecSpec {
            agg_type,
            col_idx,
            col_type_oid: pg_sys::Oid::from(col_type_oid),
            expr_kind: AggExpr::Column,
            const_offset: 0,
            is_partial: false,
            transtype_oid: pg_sys::InvalidOid,
            output_transform: OutputTransform::None,
        }
    }

    fn make_empty_segment(row_count: i32) -> SegmentData {
        SegmentData {
            companion_oid: pg_sys::InvalidOid,
            segment_id: 0,
            segment_values: Vec::new(),
            compressed_blobs: Vec::new(),
            text_length_blobs: Vec::new(),
            row_count,
            min_time: None,
            max_time: None,
            col_minmax: HashMap::new(),
            col_sums: HashMap::new(),
            toast_pointers: Vec::new(),
            cached_blob_pins: Vec::new(),
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
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_where() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::Sum, 1, 23)], Vec::new(), Vec::new(), false);
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_having() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(
            vec![make_agg_spec(AggType::Sum, 1, 23)], Vec::new(),
            vec![HavingFilter { agg_idx: 0, op: HavingOp::Gt, const_val: 5 }],
            true,
        );
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_count_distinct() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::CountDistinct, 1, 23)], Vec::new(), Vec::new(), true);
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_text_sum() {
        let meta = make_meta(&["ts", "name"]);
        let plan = make_plan(vec![make_agg_spec(AggType::Sum, 1, 25)], Vec::new(), Vec::new(), true); // TEXTOID=25
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_rejects_length_of_sum() {
        let meta = make_meta(&["ts", "name"]);
        let mut plan = make_plan(vec![make_agg_spec(AggType::Sum, 1, 23)], Vec::new(), Vec::new(), true);
        plan.agg_specs[0].expr_kind = AggExpr::LengthOf;
        assert!(try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_count_star_empty() {
        // COUNT(*) with no segments → 0
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::CountStar, -1, 23)], Vec::new(), Vec::new(), true);
        let state = try_metadata_fast_path(&plan, &meta, &[], &[], 0, 0).unwrap();
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
        let state = try_metadata_fast_path(&plan, &meta, &segs, &[], 0, 0).unwrap();
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
        let state = try_metadata_fast_path(&plan, &meta, &segs, &[], 0, 0).unwrap();
        assert_eq!(state.result_rows[0][0].0.value(), 150usize);
    }

    #[pg_test]
    fn test_metadata_fast_path_min_int() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::Min, 1, 20)], Vec::new(), Vec::new(), true); // INT8OID=20
        let mut seg1 = make_empty_segment(100);
        seg1.col_minmax.insert("value".to_string(), ColMinMax {
            min_encoded: 50i64,
            max_encoded: 200i64,
            min_null: false,
            max_null: false,
            type_oid: pg_sys::Oid::from(20u32),
        });
        let mut seg2 = make_empty_segment(100);
        seg2.col_minmax.insert("value".to_string(), ColMinMax {
            min_encoded: 10i64,
            max_encoded: 300i64,
            min_null: false,
            max_null: false,
            type_oid: pg_sys::Oid::from(20u32),
        });
        let state = try_metadata_fast_path(&plan, &meta, &[seg1, seg2], &[], 0, 0).unwrap();
        let result = state.result_rows[0][0].0.value() as i64;
        assert_eq!(result, 10);
    }

    #[pg_test]
    fn test_metadata_fast_path_max_int() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::Max, 1, 20)], Vec::new(), Vec::new(), true);
        let mut seg1 = make_empty_segment(100);
        seg1.col_minmax.insert("value".to_string(), ColMinMax {
            min_encoded: 10i64,
            max_encoded: 200i64,
            min_null: false,
            max_null: false,
            type_oid: pg_sys::Oid::from(20u32),
        });
        let mut seg2 = make_empty_segment(100);
        seg2.col_minmax.insert("value".to_string(), ColMinMax {
            min_encoded: 5i64,
            max_encoded: 999i64,
            min_null: false,
            max_null: false,
            type_oid: pg_sys::Oid::from(20u32),
        });
        let state = try_metadata_fast_path(&plan, &meta, &[seg1, seg2], &[], 0, 0).unwrap();
        let result = state.result_rows[0][0].0.value() as i64;
        assert_eq!(result, 999);
    }

    #[pg_test]
    fn test_metadata_fast_path_min_skips_null() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::Min, 1, 20)], Vec::new(), Vec::new(), true);
        let mut seg1 = make_empty_segment(100);
        seg1.col_minmax.insert("value".to_string(), ColMinMax {
            min_encoded: 0i64,
            max_encoded: 0i64,
            min_null: true,  // all nulls in this segment
            max_null: true,
            type_oid: pg_sys::Oid::from(20u32),
        });
        let mut seg2 = make_empty_segment(100);
        seg2.col_minmax.insert("value".to_string(), ColMinMax {
            min_encoded: 77i64,
            max_encoded: 77i64,
            min_null: false,
            max_null: false,
            type_oid: pg_sys::Oid::from(20u32),
        });
        let state = try_metadata_fast_path(&plan, &meta, &[seg1, seg2], &[], 0, 0).unwrap();
        let result = state.result_rows[0][0].0.value() as i64;
        assert_eq!(result, 77);
    }

    #[pg_test]
    fn test_metadata_fast_path_missing_minmax_metadata() {
        // If a segment doesn't have minmax metadata for the needed column, fall through
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::Min, 1, 20)], Vec::new(), Vec::new(), true);
        let seg = make_empty_segment(100); // no col_minmax
        assert!(try_metadata_fast_path(&plan, &meta, &[seg], &[], 0, 0).is_none());
    }

    #[pg_test]
    fn test_metadata_fast_path_missing_sum_metadata() {
        // If a segment doesn't have sum metadata for a SUM column, fall through
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::Sum, 1, 20)], Vec::new(), Vec::new(), true);
        let seg = make_empty_segment(100); // no col_sums
        assert!(try_metadata_fast_path(&plan, &meta, &[seg], &[], 0, 0).is_none());
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
            sum_i128: None,
            sum_f64: None,
            nonnull_count: 900,
            nonzero_count: -1,
            type_oid: pg_sys::Oid::from(1700u32),
        });
        let mut seg2 = make_empty_segment(500);
        seg2.col_sums.insert("value".to_string(), ColSum {
            sum_datum: pg_sys::Datum::from(0usize),
            sum_null: true,
            sum_i128: None,
            sum_f64: None,
            nonnull_count: 450,
            nonzero_count: -1,
            type_oid: pg_sys::Oid::from(1700u32),
        });
        let state = try_metadata_fast_path(&plan, &meta, &[seg1, seg2], &[], 0, 0).unwrap();
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
            sum_i128: None,
            sum_f64: None,
            nonnull_count: 100,
            nonzero_count: -1,
            type_oid: pg_sys::Oid::from(701u32),
        });
        let state = try_metadata_fast_path(&plan, &meta, &[seg], &[], 0, 0).unwrap();
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
            sum_i128: None,
            sum_f64: None,
            nonnull_count: 100,
            nonzero_count: -1,
            type_oid: pg_sys::Oid::from(1700u32),
        });
        let state = try_metadata_fast_path(&plan, &meta, &[seg], &[], 0, 0).unwrap();
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
            sum_i128: None,
            sum_f64: None,
            nonnull_count: 50,
            nonzero_count: -1,
            type_oid: pg_sys::Oid::from(701u32),
        });
        let state = try_metadata_fast_path(&plan, &meta, &[seg], &[], 0, 0).unwrap();
        let result = f64::from_bits(state.result_rows[0][0].0.value() as u64);
        // Expected: 100.0 + 10 * 50 = 600.0
        assert!((result - 600.0).abs() < 1e-10);
    }

    #[pg_test]
    fn test_metadata_fast_path_reports_segment_count() {
        let meta = make_meta(&["ts", "value"]);
        let plan = make_plan(vec![make_agg_spec(AggType::CountStar, -1, 23)], Vec::new(), Vec::new(), true);
        let segs = vec![make_empty_segment(10), make_empty_segment(20)];
        let state = try_metadata_fast_path(&plan, &meta, &segs, &[], 123, 456).unwrap();
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
        let mut seen = new_cd_set_int();
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
        let mut seen = new_cd_set_int();
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
        let mut seen = new_cd_set_str();
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

    // -------------------------------------------------------------------
    // Rust regex helper tests
    // -------------------------------------------------------------------

    #[pg_test]
    fn test_has_posix_classes_alpha() {
        assert!(has_posix_classes("[[:alpha:]]"));
    }

    #[pg_test]
    fn test_has_posix_classes_digit() {
        assert!(has_posix_classes("[[:digit:]]"));
    }

    #[pg_test]
    fn test_has_posix_classes_plain_range() {
        assert!(!has_posix_classes("[a-z]"));
    }

    #[pg_test]
    fn test_has_posix_classes_no_brackets() {
        assert!(!has_posix_classes("abc.*def"));
    }

    #[pg_test]
    fn test_convert_pg_replacement_capture_groups() {
        assert_eq!(convert_pg_replacement(r"\1"), "$1");
        assert_eq!(convert_pg_replacement(r"foo\1bar\2"), "foo$1bar$2");
    }

    #[pg_test]
    fn test_convert_pg_replacement_whole_match() {
        assert_eq!(convert_pg_replacement(r"\&"), "$0");
    }

    #[pg_test]
    fn test_convert_pg_replacement_literal_backslash() {
        assert_eq!(convert_pg_replacement(r"\\"), "\\");
    }

    #[pg_test]
    fn test_convert_pg_replacement_no_escapes() {
        assert_eq!(convert_pg_replacement("plain text"), "plain text");
    }

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

    #[pg_test]
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
