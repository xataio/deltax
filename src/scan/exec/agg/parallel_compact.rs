//! PARALLEL COMPACT path — multi-threaded segment processing for ungrouped
//! and integer-key-grouped aggregates where every needed column is numeric.
//!
//! Two pieces live here together because the dispatch body is the only
//! consumer of the helper section:
//!
//! - **Worker helpers** (`process_segments_compact`,
//!   `decompress_numeric_*`, `merge_compact_results`, etc.) — pure-Rust
//!   decompression + accumulator updates, safe to call off-thread.
//! - **`dispatch_parallel_compact_path`** — owns the worker scope,
//!   pipeline detoast, speculative top-N path, partitioned merge, and the
//!   final `AggScanState` build.
//!
//! Eligibility check (`parallel_compact_eligible`) stays in the caller so
//! `agg_specs` / `group_specs` / `compact_storage` ownership transfers
//! cleanly into the consuming dispatch on the hot path.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::hash::BuildHasherDefault;
use std::time::Instant;

use pgrx::pg_sys;

use super::super::batch_qual::{BatchQual, evaluate_batch_quals};
use super::super::datum_utils::{collation_strcmp, count_non_null};
use super::super::segments::{
    MetadataInfo, SegmentData, SegmentQualResult, classify_segment_quals, detoast_lazy_blobs,
    segment_skippable_by_dict, take_scan_buf_stats,
};
use super::super::text_col::strcoll_cmp;
use super::super::{PG_EPOCH_OFFSET_DAYS, PG_EPOCH_OFFSET_USEC};
use super::cd_set::{CdSetInt, CdSetStr, new_cd_set_int, new_cd_set_str};
use super::extract::eval_extract;
use super::keys::{CompactGroupMap, pack_int_key_1, pack_int_keys_2, unpack_int_keys};
use super::state::{
    AggExecSpec, AggExpr, AggScanState, AggType, GroupByColSpec, GroupByExpr, HavingFilter,
    HavingOp, OutputEntry,
};
use super::{
    CompactAccKind, CompactAccLayout, CompactAccStorage, CountDistinctSideCar, compact_finalize,
    datum_to_f64, datum_to_i128, i128_to_numeric_datum,
};
use crate::compression;

/// Gate for the parallel-compact dispatch. Caller checks this before
/// invoking [`dispatch_parallel_compact_path`].
#[allow(clippy::too_many_arguments)]
pub(super) fn parallel_compact_eligible(
    use_compact_keys: bool,
    use_compact_accs: bool,
    n_workers: usize,
    all_segments_len: usize,
    has_regexp_group: bool,
    needed_cols: &[bool],
    col_types: &[pg_sys::Oid],
    batch_quals: &[BatchQual],
) -> bool {
    use_compact_keys
        && use_compact_accs
        && n_workers > 1
        && all_segments_len > 1
        && !has_regexp_group
        && all_needed_cols_numeric(needed_cols, col_types)
        && batch_quals_all_numeric(batch_quals)
}

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
pub(super) fn decompress_numeric_blob(
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
pub(super) fn decompress_numeric_no_nulls(
    cc: &compression::CompressedColumnRef<'_>,
    type_oid: pg_sys::Oid,
    total_count: usize,
) -> Vec<(pg_sys::Datum, bool)> {
    let mut out = Vec::with_capacity(total_count);
    match cc.type_tag {
        compression::CompressionType::Gorilla => {
            // Decode straight into `out` via the `_each` callback — no
            // intermediate `Vec<primitive>`.
            if type_oid == pg_sys::TIMESTAMPOID || type_oid == pg_sys::TIMESTAMPTZOID {
                compression::gorilla::decode_timestamps_each(cc.data, total_count, |usec| {
                    out.push((pg_sys::Datum::from((usec - PG_EPOCH_OFFSET_USEC) as usize), false));
                });
            } else if type_oid == pg_sys::DATEOID {
                compression::gorilla::decode_timestamps_each(cc.data, total_count, |usec| {
                    let unix_days = (usec / 86_400_000_000) as i32;
                    out.push((
                        pg_sys::Datum::from((unix_days - PG_EPOCH_OFFSET_DAYS) as usize),
                        false,
                    ));
                });
            } else if type_oid == pg_sys::FLOAT4OID {
                compression::gorilla::decode_floats_f32_each(cc.data, total_count, |v| {
                    out.push((pg_sys::Datum::from(v.to_bits() as usize), false));
                });
            } else {
                // FLOAT8OID
                compression::gorilla::decode_floats_each(cc.data, total_count, |v| {
                    out.push((pg_sys::Datum::from(v.to_bits() as usize), false));
                });
            }
        }
        compression::CompressionType::DeltaVarint => {
            if type_oid == pg_sys::INT2OID {
                compression::integer::decode_i32_each(cc.data, total_count, |v| {
                    out.push((pg_sys::Datum::from(v as i16 as usize), false));
                });
            } else if type_oid == pg_sys::INT4OID || type_oid == pg_sys::DATEOID {
                compression::integer::decode_i32_each(cc.data, total_count, |v| {
                    out.push((pg_sys::Datum::from(v as usize), false));
                });
            } else {
                // INT8OID, TIMESTAMPOID, TIMESTAMPTZOID
                compression::integer::decode_i64_each(cc.data, total_count, |v| {
                    out.push((pg_sys::Datum::from(v as usize), false));
                });
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
                    out.push((
                        pg_sys::Datum::from(f32::from_bits(v as u32).to_bits() as usize),
                        false,
                    ));
                }
            } else if type_oid == pg_sys::FLOAT8OID {
                let ints = compression::bitpacked::decode_constant_i64(cc.data, total_count);
                for v in ints {
                    out.push((
                        pg_sys::Datum::from(f64::from_bits(v as u64).to_bits() as usize),
                        false,
                    ));
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
pub(super) fn decompress_numeric_nn_datums(
    cc: &compression::CompressedColumnRef<'_>,
    type_oid: pg_sys::Oid,
    non_null_count: usize,
) -> Vec<pg_sys::Datum> {
    match cc.type_tag {
        compression::CompressionType::Gorilla => {
            if type_oid == pg_sys::TIMESTAMPOID || type_oid == pg_sys::TIMESTAMPTZOID {
                let timestamps = compression::gorilla::decode_timestamps(cc.data, non_null_count);
                timestamps
                    .iter()
                    .map(|&usec| {
                        let pg_usec = usec - PG_EPOCH_OFFSET_USEC;
                        pg_sys::Datum::from(pg_usec as usize)
                    })
                    .collect()
            } else if type_oid == pg_sys::DATEOID {
                let timestamps = compression::gorilla::decode_timestamps(cc.data, non_null_count);
                timestamps
                    .iter()
                    .map(|&usec| {
                        let unix_days = (usec / 86_400_000_000) as i32;
                        let pg_days = unix_days - PG_EPOCH_OFFSET_DAYS;
                        pg_sys::Datum::from(pg_days as usize)
                    })
                    .collect()
            } else if type_oid == pg_sys::FLOAT4OID {
                let floats = compression::gorilla::decode_floats_f32(cc.data, non_null_count);
                floats
                    .iter()
                    .map(|&v| pg_sys::Datum::from(v.to_bits() as usize))
                    .collect()
            } else {
                let floats = compression::gorilla::decode_floats(cc.data, non_null_count);
                floats
                    .iter()
                    .map(|&v| pg_sys::Datum::from(v.to_bits() as usize))
                    .collect()
            }
        }
        compression::CompressionType::DeltaVarint => {
            if type_oid == pg_sys::INT2OID {
                let ints = compression::integer::decode_i32(cc.data, non_null_count);
                ints.iter()
                    .map(|&v| pg_sys::Datum::from(v as i16 as usize))
                    .collect()
            } else if type_oid == pg_sys::INT4OID || type_oid == pg_sys::DATEOID {
                let ints = compression::integer::decode_i32(cc.data, non_null_count);
                ints.iter()
                    .map(|&v| pg_sys::Datum::from(v as usize))
                    .collect()
            } else {
                let ints = compression::integer::decode_i64(cc.data, non_null_count);
                ints.iter()
                    .map(|&v| pg_sys::Datum::from(v as usize))
                    .collect()
            }
        }
        compression::CompressionType::Constant => {
            if type_oid == pg_sys::INT2OID {
                let ints = compression::bitpacked::decode_constant_i32(cc.data, non_null_count);
                ints.iter()
                    .map(|&v| pg_sys::Datum::from(v as i16 as usize))
                    .collect()
            } else if type_oid == pg_sys::INT4OID || type_oid == pg_sys::DATEOID {
                let ints = compression::bitpacked::decode_constant_i32(cc.data, non_null_count);
                ints.iter()
                    .map(|&v| pg_sys::Datum::from(v as usize))
                    .collect()
            } else if type_oid == pg_sys::FLOAT4OID {
                let ints = compression::bitpacked::decode_constant_i64(cc.data, non_null_count);
                ints.iter()
                    .map(|&v| pg_sys::Datum::from(f32::from_bits(v as u32).to_bits() as usize))
                    .collect()
            } else if type_oid == pg_sys::FLOAT8OID {
                let ints = compression::bitpacked::decode_constant_i64(cc.data, non_null_count);
                ints.iter()
                    .map(|&v| pg_sys::Datum::from(f64::from_bits(v as u64).to_bits() as usize))
                    .collect()
            } else {
                let ints = compression::bitpacked::decode_constant_i64(cc.data, non_null_count);
                ints.iter()
                    .map(|&v| pg_sys::Datum::from(v as usize))
                    .collect()
            }
        }
        compression::CompressionType::ForBitpacked => {
            if type_oid == pg_sys::INT2OID {
                let ints = compression::bitpacked::decode_for_i32(cc.data, non_null_count);
                ints.iter()
                    .map(|&v| pg_sys::Datum::from(v as i16 as usize))
                    .collect()
            } else if type_oid == pg_sys::INT4OID || type_oid == pg_sys::DATEOID {
                let ints = compression::bitpacked::decode_for_i32(cc.data, non_null_count);
                ints.iter()
                    .map(|&v| pg_sys::Datum::from(v as usize))
                    .collect()
            } else {
                let ints = compression::bitpacked::decode_for_i64(cc.data, non_null_count);
                ints.iter()
                    .map(|&v| pg_sys::Datum::from(v as usize))
                    .collect()
            }
        }
        compression::CompressionType::BooleanBitmap => {
            let bools = compression::boolean::decode(cc.data, non_null_count);
            bools
                .iter()
                .map(|&b| pg_sys::Datum::from(b as usize))
                .collect()
        }
        _ => Vec::new(),
    }
}

/// Check if all batch quals reference only numeric/comparable types (no text).
/// Text LIKE/Eq/Ne quals require PG functions during decompression, making them
/// unsafe for worker threads.
pub(super) fn batch_quals_all_numeric(batch_quals: &[BatchQual]) -> bool {
    batch_quals.iter().all(|bq| {
        matches!(
            bq.type_oid,
            pg_sys::INT2OID
                | pg_sys::INT4OID
                | pg_sys::INT8OID
                | pg_sys::FLOAT4OID
                | pg_sys::FLOAT8OID
                | pg_sys::TIMESTAMPOID
                | pg_sys::TIMESTAMPTZOID
                | pg_sys::DATEOID
                | pg_sys::BOOLOID
        )
    })
}

/// Check if a column type is supported for thread-safe decompression.
pub(super) fn is_numeric_type(type_oid: pg_sys::Oid) -> bool {
    matches!(
        type_oid,
        pg_sys::INT2OID
            | pg_sys::INT4OID
            | pg_sys::INT8OID
            | pg_sys::FLOAT4OID
            | pg_sys::FLOAT8OID
            | pg_sys::TIMESTAMPOID
            | pg_sys::TIMESTAMPTZOID
            | pg_sys::DATEOID
            | pg_sys::BOOLOID
    )
}

/// Configuration for parallel compact aggregation (read-only, shared across threads).
pub(super) struct ParallelCompactConfig<'a> {
    pub(super) agg_specs: &'a [AggExecSpec],
    pub(super) group_specs: &'a [GroupByColSpec],
    pub(super) col_names: &'a [String],
    pub(super) col_types: &'a [pg_sys::Oid],
    pub(super) segment_by: &'a [String],
    /// Persisted `_col_idx` map (see `MetadataInfo.blob_idx`).
    pub(super) blob_idx: &'a [Option<u16>],
    /// Pre-computed missing-value datums for columns added after this
    /// partition was compressed (see `MetadataInfo.missing_values`).
    pub(super) missing_values: &'a [Option<(pg_sys::Datum, bool)>],
    pub(super) needed_cols: &'a [bool],
    pub(super) batch_quals: &'a [BatchQual],
    pub(super) seg_filters: &'a [(usize, String)],
    pub(super) time_min: Option<i64>,
    pub(super) time_max: Option<i64>,
    /// If set, each worker computes top-K candidates for speculative merge-skip.
    /// (sort_slot, k, ascending)
    pub(super) topn_spec: Option<(usize, usize, bool)>,
}

// SAFETY: ParallelCompactConfig holds a `&[Option<(Datum, bool)>]` for
// missing-value synthesis. The `Datum` may point into a relcache-pinned
// tupdesc (for pass-by-reference defaults) or be self-contained (for
// pass-by-value). Workers only read these values during the agg scope,
// which is bounded by `thread::scope` — the relcache entry stays
// pinned for the whole query, so the pointer is valid. No worker writes.
unsafe impl Send for ParallelCompactConfig<'_> {}
unsafe impl Sync for ParallelCompactConfig<'_> {}

/// Result of parallel compact aggregation from one worker thread.
pub(crate) struct ParallelCompactResult {
    pub(crate) compact_map: CompactGroupMap,
    pub(crate) compact_storage: CompactAccStorage,
    pub(crate) cd_sidecar: CountDistinctSideCar,
    pub(crate) segments_processed: u64,
    pub(crate) rows_processed: u64,
    pub(crate) decompress_us: u64,
    /// Pre-computed top-K candidates: (keys, floor_value).
    /// Present when `config.topn_spec` is set.
    pub(crate) topk: Option<(Vec<u128>, i64)>,
}

impl ParallelCompactResult {
    /// Construct an empty result for the given agg specs. Used by parallel
    /// workers as the per-process accumulator before serialising to DSM.
    #[allow(dead_code)] // wired by C.2.d
    pub(crate) fn empty(agg_specs: &[AggExecSpec]) -> Self {
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
pub(super) fn process_segments_compact(
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

        // Decompress needed columns (pure Rust, no PG calls). For each
        // logical column, consult `config.blob_idx` (the persisted
        // `_col_idx` map): `Some(slot)` → read from
        // `compressed_blobs[slot]`; `None` AND segment_by → take the
        // per-segment value from `_meta`; `None` AND not-segment_by →
        // column was added after this partition was compressed, so
        // synthesize from `config.missing_values[col_idx]`.
        let t_dec = Instant::now();
        let mut decompressed: Vec<Vec<(pg_sys::Datum, bool)>> = Vec::new();
        let mut seg_val_idx = 0;

        for (col_idx, col_name) in config.col_names.iter().enumerate() {
            let type_oid = config.col_types[col_idx];
            let is_segment_by = config.segment_by.contains(col_name);

            if !config.needed_cols[col_idx] {
                if is_segment_by {
                    seg_val_idx += 1;
                }
                decompressed.push(Vec::new());
                continue;
            }

            if is_segment_by {
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
            } else if let Some(slot) = config.blob_idx[col_idx] {
                let blob = &seg.compressed_blobs[slot as usize];
                decompressed.push(decompress_numeric_blob(blob, type_oid));
            } else {
                // Column added to the parent after this partition was
                // compressed — no blob exists. Synthesize the missing
                // value (one constant Datum per row).
                let (datum, is_null) = config
                    .missing_values
                    .get(col_idx)
                    .copied()
                    .flatten()
                    .unwrap_or((pg_sys::Datum::from(0usize), true));
                let repeated: Vec<(pg_sys::Datum, bool)> =
                    (0..seg.row_count).map(|_| (datum, is_null)).collect();
                decompressed.push(repeated);
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
                    GroupByExpr::AddConst { offset, .. } => col[row].0.value() as i64 + offset,
                    GroupByExpr::Column => col[row].0.value() as i64,
                    _ => unreachable!(),
                };
            }

            if has_null {
                continue;
            }

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
                    CompactAccKind::Count => match spec.agg_type {
                        AggType::CountStar => {
                            compact_storage.incr_count(group_idx, spec_idx, 1);
                        }
                        AggType::Count => {
                            let col = &decompressed[spec.col_idx as usize];
                            if !col.is_empty() && !col[row].1 {
                                compact_storage.incr_count(group_idx, spec_idx, 1);
                            }
                        }
                        _ => {}
                    },
                    CompactAccKind::SumInt => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = datum_to_i128(col[row].0, spec.col_type_oid);
                            let sum_delta = if spec.expr_kind == AggExpr::AddConst {
                                v + spec.const_offset as i128
                            } else {
                                v
                            };
                            compact_storage.add_sum_int(group_idx, spec_idx, sum_delta, 1);
                        }
                    }
                    CompactAccKind::SumIntNarrow => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = col[row].0.value() as i64;
                            let sum_delta = if spec.expr_kind == AggExpr::AddConst {
                                v + spec.const_offset
                            } else {
                                v
                            };
                            compact_storage.add_sum_int_narrow(group_idx, spec_idx, sum_delta, 1);
                        }
                    }
                    CompactAccKind::SumFloat => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = datum_to_f64(col[row].0, spec.col_type_oid);
                            let sum_delta = if spec.expr_kind == AggExpr::AddConst {
                                v + spec.const_offset as f64
                            } else {
                                v
                            };
                            compact_storage.add_sum_float(group_idx, spec_idx, sum_delta, 1);
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
                            compact_storage.update_min_int(group_idx, spec_idx, v);
                        }
                    }
                    CompactAccKind::MaxInt => {
                        let col = &decompressed[spec.col_idx as usize];
                        if !col.is_empty() && !col[row].1 {
                            let v = col[row].0.value() as i64;
                            compact_storage.update_max_int(group_idx, spec_idx, v);
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
            match sort_kind {
                CompactAccKind::Count => compact_storage.read_count(gidx, sort_slot),
                CompactAccKind::SumIntNarrow => {
                    compact_storage.read_sum_int_narrow(gidx, sort_slot).0
                }
                CompactAccKind::MinInt | CompactAccKind::MaxInt => {
                    compact_storage.read_min_max_int(gidx, sort_slot).0
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
pub(super) fn parse_string_to_datum(s: &str, type_oid: pg_sys::Oid) -> pg_sys::Datum {
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
pub(super) fn merge_compact_results(
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
                CompactAccKind::Count => {
                    let worker_count = worker_storage.read_count(worker_group_idx, slot_idx);
                    global_storage.incr_count(global_group_idx, slot_idx, worker_count);
                }
                CompactAccKind::SumInt => {
                    let (worker_sum, worker_count) =
                        worker_storage.read_sum_int(worker_group_idx, slot_idx);
                    global_storage.add_sum_int(
                        global_group_idx,
                        slot_idx,
                        worker_sum,
                        worker_count,
                    );
                }
                CompactAccKind::SumIntNarrow => {
                    let (worker_sum, worker_count) =
                        worker_storage.read_sum_int_narrow(worker_group_idx, slot_idx);
                    global_storage.add_sum_int_narrow(
                        global_group_idx,
                        slot_idx,
                        worker_sum,
                        worker_count,
                    );
                }
                CompactAccKind::SumFloat => {
                    let (worker_sum, worker_count) =
                        worker_storage.read_sum_float(worker_group_idx, slot_idx);
                    global_storage.add_sum_float(
                        global_group_idx,
                        slot_idx,
                        worker_sum,
                        worker_count,
                    );
                }
                CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                    let (w_off, w_len) =
                        worker_storage.read_min_max_str(worker_group_idx, slot_idx);
                    if w_off != u32::MAX {
                        let w_str = worker_storage.str_arena.get(w_off, w_len);
                        let (g_off, g_len) =
                            global_storage.read_min_max_str(global_group_idx, slot_idx);
                        let should_update = if g_off == u32::MAX {
                            true
                        } else {
                            let g_str = global_storage.str_arena.get(g_off, g_len);
                            // SAFETY: collation_strcmp wraps PG strcoll FFI; caller is in active PG transaction.
                            let cmp = unsafe { collation_strcmp(w_str, g_str) };
                            match kind {
                                CompactAccKind::MinStr => cmp < 0,
                                CompactAccKind::MaxStr => cmp > 0,
                                _ => unreachable!(),
                            }
                        };
                        if should_update {
                            let w_str = worker_storage.str_arena.get(w_off, w_len);
                            let (new_off, new_len) = global_storage.str_arena.alloc(w_str);
                            global_storage.write_min_max_str(
                                global_group_idx,
                                slot_idx,
                                new_off,
                                new_len,
                            );
                        }
                    }
                }
                CompactAccKind::MinInt => {
                    let (w_val, w_has) =
                        worker_storage.read_min_max_int(worker_group_idx, slot_idx);
                    if w_has {
                        global_storage.update_min_int(global_group_idx, slot_idx, w_val);
                    }
                }
                CompactAccKind::MaxInt => {
                    let (w_val, w_has) =
                        worker_storage.read_min_max_int(worker_group_idx, slot_idx);
                    if w_has {
                        global_storage.update_max_int(global_group_idx, slot_idx, w_val);
                    }
                }
                CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr => {
                    global_cd.union_from(slot_idx, global_group_idx, worker_cd, worker_group_idx);
                }
            }
        }
    }
}

/// Check if all needed columns (for aggs, groups, and batch quals) are numeric.
pub(super) fn all_needed_cols_numeric(needed_cols: &[bool], col_types: &[pg_sys::Oid]) -> bool {
    needed_cols
        .iter()
        .zip(col_types.iter())
        .all(|(&needed, &type_oid)| !needed || is_numeric_type(type_oid))
}

/// Parallel-compact path dispatch.
///
/// Callers MUST verify [`parallel_compact_eligible`] before invoking this
/// — it consumes `agg_specs` / `group_specs` / `compact_storage` to build
/// the returned `AggScanState`.
///
/// SAFETY: calls `detoast_lazy_blobs` and worker-scope FFI. Must run
/// inside an active PG transaction (guaranteed when invoked from a
/// `BeginCustomScan` callback).
/// Read-only inputs threaded through the merge-phase sub-paths inside
/// `dispatch_parallel_compact_path`. Built once after the worker scope
/// finishes (so the timing/counter fields are frozen).
struct CompactMergeCtx<'a> {
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
}

/// Bare-LIMIT short-circuit for the compact path. Pick N groups from the
/// largest worker, merge only those keys across workers, finalize only
/// those rows. Skips the global merge entirely.
///
/// # Safety
///
/// Inherits the `CompactAccStorage` accessor contract and calls
/// `finalize_accumulator` / `i128_to_numeric_datum` internally — must
/// run inside an active PG transaction.
#[inline]
unsafe fn compact_bare_limit(
    ctx: &CompactMergeCtx<'_>,
    agg_specs: Vec<AggExecSpec>,
    group_specs: Vec<GroupByColSpec>,
    partial_results: &[ParallelCompactResult],
    compact_storage: &mut CompactAccStorage,
) -> AggScanState {
    unsafe {
        let n = ctx.bare_limit as usize;
        let t_merge = Instant::now();

        let largest_idx = partial_results
            .iter()
            .enumerate()
            .max_by_key(|(_, r)| r.compact_map.len())
            .map(|(i, _)| i)
            .unwrap_or(0);

        let target_keys: Vec<u128> = partial_results[largest_idx]
            .compact_map
            .keys()
            .take(n)
            .copied()
            .collect();

        let storage = compact_storage;
        let num_group_keys = group_specs.len();

        let pre_topn_groups: usize = partial_results.iter().map(|r| r.compact_map.len()).sum();

        let mut bare_cd_sidecar = CountDistinctSideCar::new(&agg_specs);
        let mut result_rows = Vec::with_capacity(n);
        for &packed_key in &target_keys {
            let global_idx = storage.alloc_group();
            bare_cd_sidecar.alloc_group();

            for result in partial_results {
                if let Some(&worker_idx) = result.compact_map.get(&packed_key) {
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
                                bare_cd_sidecar.union_from(
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

            for e in &bare_cd_sidecar.entries {
                let count = e.count(global_idx);
                storage.set_count(global_idx, e.spec_idx, count);
            }

            let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
            for (spec_idx, spec) in agg_specs.iter().enumerate() {
                agg_results.push(compact_finalize(storage, global_idx, spec_idx, spec));
            }
            let keys = unpack_int_keys(packed_key, num_group_keys);
            let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(ctx.num_result_cols);
            for entry in ctx.output_map {
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
            n_workers: ctx.n_workers as u64,
            bare_limit: ctx.bare_limit,
            wall_us: ctx.t_wall.elapsed().as_micros() as u64,
            buf_stats: take_scan_buf_stats(),
            ..AggScanState::default()
        }
    }
}

/// Full merge fallback for the compact path. Adopts the largest worker
/// map as the base, merges all other workers' entries, then finalizes
/// every group with HAVING filtering. If a top-N is active without a
/// dedicated optimization path, sorts the finalized rows and truncates.
///
/// # Safety
///
/// Inherits the `CompactAccStorage` accessor contract. Must run
/// inside an active PG transaction (finalize allocates NUMERIC datums).
#[inline]
unsafe fn compact_full_merge(
    ctx: &CompactMergeCtx<'_>,
    agg_specs: Vec<AggExecSpec>,
    group_specs: Vec<GroupByColSpec>,
    mut partial_results: Vec<ParallelCompactResult>,
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
        let mut global_cd_sidecar = largest.cd_sidecar;

        let remaining_entries: usize = partial_results.iter().map(|r| r.compact_map.len()).sum();
        compact_group_map.reserve(remaining_entries);

        let storage = compact_storage;
        for result in &partial_results {
            merge_compact_results(
                compact_group_map,
                storage,
                &mut global_cd_sidecar,
                &result.compact_map,
                &result.compact_storage,
                &result.cd_sidecar,
                &agg_specs,
            );
        }
        global_cd_sidecar.write_counts_to_storage(storage, compact_group_map);
        let merge_us = t_merge.elapsed().as_micros() as u64;

        let pre_topn_groups = compact_group_map.len();
        let topn_select_us: u64 = 0;
        let t_finalize = Instant::now();
        let result_rows = {
            let num_group_keys = group_specs.len();
            let mut rows = Vec::new();
            'par_compact_group_loop: for (&packed_key, &group_idx) in compact_group_map.iter() {
                let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                for (spec_idx, spec) in agg_specs.iter().enumerate() {
                    agg_results.push(compact_finalize(storage, group_idx, spec_idx, spec));
                }

                for hf in ctx.having_filters {
                    let (datum, is_null) = agg_results[hf.agg_idx];
                    if is_null {
                        continue 'par_compact_group_loop;
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
                        continue 'par_compact_group_loop;
                    }
                }

                let keys = unpack_int_keys(packed_key, num_group_keys);
                let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(ctx.num_result_cols);
                for entry in ctx.output_map {
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

/// Result of one of the two top-N merge sub-paths inside
/// `dispatch_parallel_compact_path`. The dispatch fn assembles a final
/// `AggScanState` from this via `build_topn_agg_scan_state`.
struct CompactMergeOutcome {
    result_rows: Vec<Vec<(pg_sys::Datum, bool)>>,
    pre_topn_groups: u64,
    merge_us: u64,
    finalize_us: u64,
    topn_select_us: u64,
}

#[inline]
fn build_topn_agg_scan_state(
    ctx: &CompactMergeCtx<'_>,
    agg_specs: Vec<AggExecSpec>,
    group_specs: Vec<GroupByColSpec>,
    outcome: CompactMergeOutcome,
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

/// Speculative top-N for the compact path. Uses per-worker pre-computed
/// top-K candidates (built while data was cache-hot), merges only those
/// candidates, and verifies no missed key could beat the Nth result.
///
/// Returns:
/// - `Some(outcome)` on a successful speculation (Nth result provably
///   beats every key not in the candidate set) or when every candidate
///   tied at the Nth value (any N are valid).
/// - `None` on fallthrough: not eligible (no top-N, HAVING active,
///   sort is COUNT(DISTINCT) or AVG), Phase 2 too expensive, or
///   speculation failed and ties don't apply. Caller falls through to
///   the partitioned merge or full merge path.
///
/// # Safety
///
/// Inherits the `CompactAccStorage` accessor contract. Must run
/// inside an active PG transaction for the build-time finalize step.
#[inline]
unsafe fn compact_speculative_topn(
    ctx: &CompactMergeCtx<'_>,
    agg_specs: &[AggExecSpec],
    group_specs: &[GroupByColSpec],
    partial_results: &[ParallelCompactResult],
    compact_storage: &mut CompactAccStorage,
) -> Option<CompactMergeOutcome> {
    unsafe {
        // ----------------------------------------------------------
        let sort_slot_for_compact_spec = match ctx.output_map[ctx.topn_sort_col] {
            OutputEntry::Agg(ai) => ai,
            _ => 0,
        };
        let compact_sort_is_cd = ctx.topn_limit > 0
            && matches!(
                compact_storage.layout.slots[sort_slot_for_compact_spec].1,
                CompactAccKind::CountDistinctInt | CompactAccKind::CountDistinctStr
            );
        let compact_sort_is_avg =
            ctx.topn_limit > 0 && agg_specs[sort_slot_for_compact_spec].agg_type == AggType::Avg;
        if ctx.topn_limit > 0
            && ctx.having_filters.is_empty()
            && !compact_sort_is_cd
            && !compact_sort_is_avg
        {
            let sort_slot = sort_slot_for_compact_spec;
            let (_, sort_kind) = compact_storage.layout.slots[sort_slot];
            let limit = ctx.topn_limit as usize;
            let k = (ctx.topn_limit as usize).max(1000);

            let read_sort = |storage: &CompactAccStorage, group_idx: u32| -> i64 {
                match sort_kind {
                    CompactAccKind::Count => storage.read_count(group_idx, sort_slot),
                    CompactAccKind::SumIntNarrow => {
                        storage.read_sum_int_narrow(group_idx, sort_slot).0
                    }
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

            // Cost guard: Phase 2 iterates candidates × partial_results.
            // For low-cardinality GROUP BY, candidate_set is small → fast.
            // For high-cardinality with many pipeline batches, candidate_set
            // can be huge → skip speculative and go straight to full merge.
            let phase2_ops = candidate_set.len() as u64 * partial_results.len() as u64;
            if phase2_ops > 10_000_000 {
                pgrx::log!(
                    "pg_deltax speculative top-N skipped: phase2 too expensive \
                         (candidates={} × results={} = {} ops)",
                    candidate_set.len(),
                    partial_results.len(),
                    phase2_ops,
                );
            } else {
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

                // Phase 4: Correctness check — can any missed key beat the Nth result?
                let speculative_ok = if merged.len() >= limit {
                    let nth_value = merged[limit - 1].0;
                    if !ctx.topn_ascending {
                        nth_value > floor_sum // missed key total ≤ floor_sum
                    } else {
                        nth_value < floor_sum // missed key total ≥ floor_sum
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
                    let storage = compact_storage;
                    let num_group_keys = group_specs.len();
                    let n_winners = merged.len();

                    // Pre-resolve (winner, worker) -> worker_group_idx so
                    // worker threads don't hash-lookup repeatedly. None means
                    // the worker doesn't have that winner's key at all.
                    let winner_worker_idx: Vec<Vec<Option<u32>>> = merged
                        .iter()
                        .map(|&(_, packed_key)| {
                            partial_results
                                .iter()
                                .map(|r| r.compact_map.get(&packed_key).copied())
                                .collect()
                        })
                        .collect();

                    // Identify CD slots; these will be computed in parallel.
                    let cd_slot_specs: Vec<(usize, bool)> = agg_specs
                        .iter()
                        .enumerate()
                        .filter_map(|(slot_idx, spec)| {
                            if spec.agg_type == AggType::CountDistinct {
                                let is_str = matches!(
                                    spec.col_type_oid,
                                    pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID
                                );
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
                            let handles: Vec<_> = (0..CD_WIN_PARTITIONS)
                                .map(|p| {
                                    s.spawn(move || {
                                        // Per-winner per-cd-rank disjoint set
                                        let n_cd = cd_specs_ref.len();
                                        let mut local_int: Vec<Vec<CdSetInt>> = (0..n_winners)
                                            .map(|_| (0..n_cd).map(|_| new_cd_set_int()).collect())
                                            .collect();
                                        let mut local_str: Vec<Vec<CdSetStr>> = (0..n_winners)
                                            .map(|_| (0..n_cd).map(|_| new_cd_set_str()).collect())
                                            .collect();

                                        for (winner_idx, per_worker_gidx) in
                                            winner_worker_ref.iter().enumerate()
                                        {
                                            for (worker_idx, &maybe_gidx) in
                                                per_worker_gidx.iter().enumerate()
                                            {
                                                let Some(w_gidx) = maybe_gidx else {
                                                    continue;
                                                };
                                                let worker_cd =
                                                    &partial_refs[worker_idx].cd_sidecar;
                                                for (cd_rank, &(slot_idx, is_str)) in
                                                    cd_specs_ref.iter().enumerate()
                                                {
                                                    // Find the matching entry in worker's cd_sidecar.
                                                    let Some(oe) = worker_cd
                                                        .entries
                                                        .iter()
                                                        .find(|e| e.spec_idx == slot_idx)
                                                    else {
                                                        continue;
                                                    };
                                                    if is_str {
                                                        let src = &oe.sets_str[w_gidx as usize];
                                                        let dst =
                                                            &mut local_str[winner_idx][cd_rank];
                                                        for &v in src {
                                                            if cd_part_str(v) == p {
                                                                dst.insert(v);
                                                            }
                                                        }
                                                    } else {
                                                        let src = &oe.sets_int[w_gidx as usize];
                                                        let dst =
                                                            &mut local_int[winner_idx][cd_rank];
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
                                        (0..n_winners)
                                            .map(|w| {
                                                (0..n_cd)
                                                    .map(|c| {
                                                        let (_, is_str) = cd_specs_ref[c];
                                                        if is_str {
                                                            local_str[w][c].len() as i64
                                                        } else {
                                                            local_int[w][c].len() as i64
                                                        }
                                                    })
                                                    .collect()
                                            })
                                            .collect()
                                    })
                                })
                                .collect();
                            handles.into_iter().map(|h| h.join().unwrap()).collect()
                        });

                        // Sum per (winner, cd_rank) across partitions.
                        let n_cd = cd_slot_specs.len();
                        let mut total: Vec<Vec<i64>> =
                            (0..n_winners).map(|_| vec![0i64; n_cd]).collect();
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
                        for (worker_idx, &maybe_gidx) in
                            winner_worker_idx[winner_idx].iter().enumerate()
                        {
                            let Some(worker_idx_w) = maybe_gidx else {
                                continue;
                            };
                            let result = &partial_results[worker_idx];
                            for (slot_idx, _) in agg_specs.iter().enumerate() {
                                let (_, kind) = storage.layout.slots[slot_idx];
                                match kind {
                                    CompactAccKind::Count => {
                                        let wc = result
                                            .compact_storage
                                            .read_count(worker_idx_w, slot_idx);
                                        storage.incr_count(global_idx, slot_idx, wc);
                                    }
                                    CompactAccKind::SumInt => {
                                        let (ws, wc) = result
                                            .compact_storage
                                            .read_sum_int(worker_idx_w, slot_idx);
                                        storage.add_sum_int(global_idx, slot_idx, ws, wc);
                                    }
                                    CompactAccKind::SumIntNarrow => {
                                        let (ws, wc) = result
                                            .compact_storage
                                            .read_sum_int_narrow(worker_idx_w, slot_idx);
                                        storage.add_sum_int_narrow(global_idx, slot_idx, ws, wc);
                                    }
                                    CompactAccKind::SumFloat => {
                                        let (ws, wc) = result
                                            .compact_storage
                                            .read_sum_float(worker_idx_w, slot_idx);
                                        storage.add_sum_float(global_idx, slot_idx, ws, wc);
                                    }
                                    CompactAccKind::MinStr | CompactAccKind::MaxStr => {
                                        let (w_off, w_len) = result
                                            .compact_storage
                                            .read_min_max_str(worker_idx_w, slot_idx);
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
                                            .read_min_max_int(worker_idx_w, slot_idx);
                                        if w_has {
                                            storage.update_min_int(global_idx, slot_idx, w_val);
                                        }
                                    }
                                    CompactAccKind::MaxInt => {
                                        let (w_val, w_has) = result
                                            .compact_storage
                                            .read_min_max_int(worker_idx_w, slot_idx);
                                        if w_has {
                                            storage.update_max_int(global_idx, slot_idx, w_val);
                                        }
                                    }
                                    CompactAccKind::CountDistinctInt
                                    | CompactAccKind::CountDistinctStr => {
                                        // Handled by parallel pass above.
                                    }
                                }
                            }
                        }

                        // Write CD counts from parallel pass into storage.
                        for (cd_rank, &(slot_idx, _)) in cd_slot_specs.iter().enumerate() {
                            storage.set_count(global_idx, slot_idx, cd_counts[winner_idx][cd_rank]);
                        }

                        // Finalize this group.
                        let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
                        for (spec_idx, spec) in agg_specs.iter().enumerate() {
                            agg_results.push(compact_finalize(storage, global_idx, spec_idx, spec));
                        }
                        let keys = unpack_int_keys(packed_key, num_group_keys);
                        let mut row: Vec<(pg_sys::Datum, bool)> =
                            Vec::with_capacity(ctx.num_result_cols);
                        for entry in ctx.output_map {
                            match entry {
                                OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                                OutputEntry::Group(gi) => {
                                    let v = keys[*gi];
                                    if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. })
                                    {
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

                    let pre_topn_groups: usize =
                        partial_results.iter().map(|r| r.compact_map.len()).sum();

                    return Some(CompactMergeOutcome {
                        result_rows,
                        pre_topn_groups: pre_topn_groups as u64,
                        merge_us,
                        finalize_us,
                        topn_select_us,
                    });
                }
                // Speculation failed — check if all candidates are tied.
                // When nth_value == all merged candidates' values (e.g. COUNT on unique keys
                // where every group has count=1), any N groups are valid — skip the expensive
                // partitioned merge and use the ctx.bare_limit-style shortcut.
                let nth_value = merged
                    .get(limit.saturating_sub(1))
                    .map(|x| x.0)
                    .unwrap_or(0);
                let all_tied = merged.len() >= limit && merged.iter().all(|&(v, _)| v == nth_value);

                let spec_fail_us = t_spec.elapsed().as_micros() as u64;
                pgrx::log!(
                    "pg_deltax speculative top-N failed: candidates={} k={} nth={} floor_sum={} all_tied={} (wasted {:.1}ms)",
                    merged.len(),
                    k,
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
                    let storage = compact_storage;
                    let num_group_keys = group_specs.len();
                    let mut result_rows = Vec::with_capacity(merged.len());
                    let mut spec_cd_sidecar = CountDistinctSideCar::new(agg_specs);

                    for &(_, packed_key) in &merged {
                        let global_idx = storage.alloc_group();
                        spec_cd_sidecar.alloc_group();

                        for result in partial_results {
                            if let Some(&worker_idx) = result.compact_map.get(&packed_key) {
                                for (slot_idx, _) in agg_specs.iter().enumerate() {
                                    let (_, kind) = storage.layout.slots[slot_idx];
                                    match kind {
                                        CompactAccKind::Count => {
                                            let wc = result
                                                .compact_storage
                                                .read_count(worker_idx, slot_idx);
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
                                            storage
                                                .add_sum_int_narrow(global_idx, slot_idx, ws, wc);
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
                                                let w_str = result
                                                    .compact_storage
                                                    .str_arena
                                                    .get(w_off, w_len);
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
                        let keys = unpack_int_keys(packed_key, num_group_keys);
                        let mut row: Vec<(pg_sys::Datum, bool)> =
                            Vec::with_capacity(ctx.num_result_cols);
                        for entry in ctx.output_map {
                            match entry {
                                OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
                                OutputEntry::Group(gi) => {
                                    let v = keys[*gi];
                                    if matches!(group_specs[*gi].expr, GroupByExpr::Extract { .. })
                                    {
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

                    let pre_topn_groups: usize =
                        partial_results.iter().map(|r| r.compact_map.len()).sum();

                    return Some(CompactMergeOutcome {
                        result_rows,
                        pre_topn_groups: pre_topn_groups as u64,
                        merge_us: 0,
                        finalize_us,
                        topn_select_us: spec_fail_us,
                    });
                }
            } // end else (phase2 cost guard)
        }
        None
    }
}

/// Partitioned parallel merge + top-N for the compact path. Partitions
/// the key space across `n_workers` threads; each merges its slice,
/// finds local top-N, then a final merge picks the global top-N.
///
/// Caller has already gated `topn_limit > 0`.
///
/// # Safety
///
/// Inherits the `CompactAccStorage` accessor contract. Must run
/// inside an active PG transaction (final finalize allocates datums).
#[inline]
unsafe fn compact_partitioned_topn(
    ctx: &CompactMergeCtx<'_>,
    agg_specs: &[AggExecSpec],
    group_specs: &[GroupByColSpec],
    partial_results: &[ParallelCompactResult],
) -> CompactMergeOutcome {
    unsafe {
        let t_merge = Instant::now();
        let limit = ctx.topn_limit as usize;
        let sort_slot = match ctx.output_map[ctx.topn_sort_col] {
            OutputEntry::Agg(ai) => ai,
            _ => unreachable!(),
        };
        let n_partitions = ctx.n_workers;

        let pre_topn_groups: usize = partial_results.iter().map(|r| r.compact_map.len()).sum();

        // Each partition thread: merge its slice, find local top-N,
        // copy winners to mini storage, drop the rest.
        #[allow(clippy::type_complexity)]
        let partition_results: Vec<(CompactAccStorage, Vec<(i64, u128, u32)>)> =
            std::thread::scope(|s| {
                let workers = partial_results;
                let specs = &agg_specs;
                let np = n_partitions;
                let ascending = ctx.topn_ascending;
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
                                                let wc = worker
                                                    .compact_storage
                                                    .read_count(wgidx, slot_idx);
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
                                                        let g_str =
                                                            storage.str_arena.get(g_off, g_len);
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
                                            let (s, c) =
                                                storage.read_sum_int_narrow(gidx, sort_slot);
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
                                        CompactAccKind::Count => {
                                            storage.read_count(gidx, sort_slot)
                                        }
                                        CompactAccKind::SumIntNarrow => {
                                            storage.read_sum_int_narrow(gidx, sort_slot).0
                                        }
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
                                    CompactAccKind::SumIntNarrow => {
                                        storage.read_sum_int_narrow(gidx, slot).0
                                    }
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
                                    if kind == CompactAccKind::MinStr
                                        || kind == CompactAccKind::MaxStr
                                    {
                                        let (off, len) =
                                            storage.read_min_max_str(old_gidx, slot_idx);
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
                    })
                    .collect();

                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });

        let merge_us = t_merge.elapsed().as_micros() as u64;

        // Merge all partition top entries, select global top-N
        let t_finalize = Instant::now();
        let mut all_candidates: Vec<(i64, u128, u32, usize)> = Vec::new();
        for (pi, (_, entries)) in partition_results.iter().enumerate() {
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

        let num_group_keys = group_specs.len();
        let mut result_rows = Vec::with_capacity(limit);
        for &(_sort_val, key, mini_gidx, pi) in &all_candidates {
            let storage = &partition_results[pi].0;
            let mut agg_results: Vec<(pg_sys::Datum, bool)> = Vec::new();
            for (spec_idx, spec) in agg_specs.iter().enumerate() {
                agg_results.push(compact_finalize(storage, mini_gidx, spec_idx, spec));
            }
            let keys = unpack_int_keys(key, num_group_keys);
            let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(ctx.num_result_cols);
            for entry in ctx.output_map {
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

        CompactMergeOutcome {
            result_rows,
            pre_topn_groups: pre_topn_groups as u64,
            merge_us,
            finalize_us,
            topn_select_us: 0,
        }
    }
}

/// Dispatch entry for the parallel-compact path. Spawns a worker scope
/// to populate per-worker `ParallelCompactResult` partials, then runs
/// the appropriate merge phase (bare-LIMIT / speculative-topN /
/// partitioned-topN / full-merge).
///
/// # Safety
///
/// Inherits the `CompactAccStorage` accessor contract. Calls
/// `detoast_lazy_blobs` and PG FFI inside the worker scope. Must run
/// inside an active PG transaction (`BeginCustomScan` invariant).
#[allow(clippy::too_many_arguments, clippy::ptr_arg)]
pub(super) unsafe fn dispatch_parallel_compact_path(
    agg_specs: Vec<AggExecSpec>,
    group_specs: Vec<GroupByColSpec>,
    output_map: &[OutputEntry],
    having_filters: &[HavingFilter],
    where_quals: *mut pg_sys::List,
    topn_limit: i64,
    topn_sort_col: usize,
    topn_ascending: bool,
    bare_limit: i64,
    meta: &MetadataInfo,
    all_segments: &mut [SegmentData],
    needed_cols: &[bool],
    batch_quals: &[BatchQual],
    seg_filters: &[(usize, String)],
    time_min: Option<i64>,
    time_max: Option<i64>,
    n_workers: usize,
    use_lazy: bool,
    num_result_cols: usize,
    metadata_us: u64,
    heap_scan_us: u64,
    t_wall: Instant,
    mut compact_storage: Option<CompactAccStorage>,
    mut total_detoast_us: u64,
    mut total_cache_hits: u64,
    mut total_cache_misses: u64,
    mut total_cache_bytes_served: u64,
) -> AggScanState {
    let has_group_by = !group_specs.is_empty();
    #[allow(unused_assignments)] // overwritten by `largest.compact_map` on the merge branch
    let mut compact_group_map: CompactGroupMap =
        CompactGroupMap::with_hasher(BuildHasherDefault::default());
    unsafe {
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
            blob_idx: &meta.blob_idx,
            missing_values: &meta.missing_values,
            needed_cols,
            batch_quals,
            seg_filters,
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
                for seg in all_segments.iter_mut() {
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
                    let handles: Vec<_> = current_batch
                        .chunks(chunk_size)
                        .map(|chunk| {
                            let cfg = &config;
                            s.spawn(move || process_segments_compact(chunk, cfg))
                        })
                        .collect();

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
                let handles: Vec<_> = all_segments
                    .chunks(chunk_size)
                    .map(|chunk| {
                        let cfg = &config;
                        s.spawn(move || process_segments_compact(chunk, cfg))
                    })
                    .collect();
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

        let merge_ctx = CompactMergeCtx {
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
        };

        // Speculative top-N — see `compact_speculative_topn`.
        if let Some(outcome) = compact_speculative_topn(
            &merge_ctx,
            &agg_specs,
            &group_specs,
            &partial_results,
            compact_storage.as_mut().unwrap(),
        ) {
            return build_topn_agg_scan_state(&merge_ctx, agg_specs, group_specs, outcome);
        }

        // Bare LIMIT short-circuit for compact path — see `compact_bare_limit`.
        if bare_limit > 0 && having_filters.is_empty() {
            return compact_bare_limit(
                &merge_ctx,
                agg_specs,
                group_specs,
                &partial_results,
                compact_storage.as_mut().unwrap(),
            );
        }

        // Partitioned parallel merge + top-N — see `compact_partitioned_topn`.
        if topn_limit > 0 {
            let outcome =
                compact_partitioned_topn(&merge_ctx, &agg_specs, &group_specs, &partial_results);
            return build_topn_agg_scan_state(&merge_ctx, agg_specs, group_specs, outcome);
        }

        // Fallthrough: full merge path — see `compact_full_merge`.
        compact_full_merge(
            &merge_ctx,
            agg_specs,
            group_specs,
            partial_results,
            compact_storage.as_mut().unwrap(),
            &mut compact_group_map,
        )
    }
}
