//! Parallel COUNT(DISTINCT)-only path.
//!
//! Activates when the query is ungrouped, has no WHERE-clause batch quals,
//! and every aggregate is `COUNT(DISTINCT ...)`. Each worker walks a chunk
//! of segments and builds local `CdSetInt` / `CdSetStr` digests; the leader
//! then partitions the keyspace into `CD_MERGE_PARTITIONS` buckets and runs
//! a second thread-scope to count each bucket independently (every entry
//! is a CountDistinct, so we only need the final `len()` — no global set
//! needed).
//!
//! Returns `Some(AggScanState)` when the path runs; `None` when its gate
//! fails so `begin_agg_scan` falls through to the next path.

use std::time::Instant;

use pgrx::pg_sys;

use super::super::batch_qual::BatchQual;
use super::super::datum_utils::count_non_null;
use super::super::segments::{MetadataInfo, SegmentData, detoast_lazy_blobs, take_scan_buf_stats};
use super::cd_set::{CdSetInt, CdSetStr, hash128_str, new_cd_set_int, new_cd_set_str};
use super::state::{AggExecSpec, AggScanState, AggType, GroupByColSpec, OutputEntry};
use crate::compression;

struct ParallelCdConfig<'a> {
    agg_specs: &'a [AggExecSpec],
    col_names: &'a [String],
    col_types: &'a [pg_sys::Oid],
    segment_by: &'a [String],
    /// Persisted `_col_idx` map (see `MetadataInfo.blob_idx`). `Some(slot)`
    /// → read from `compressed_blobs[slot]`. `None` → segment_by (read
    /// from `_meta`) or column added after compression (synthesized via
    /// `missing_values`).
    blob_idx: &'a [Option<u16>],
    /// Pre-computed missing-value datums for columns added after the
    /// partition was compressed.
    missing_values: &'a [Option<(pg_sys::Datum, bool)>],
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

fn process_cd_segments(segments: &[SegmentData], config: &ParallelCdConfig) -> ParallelCdResult {
    let n_aggs = config.agg_specs.len();
    let mut int_sets: Vec<CdSetInt> = (0..n_aggs).map(|_| new_cd_set_int()).collect();
    let mut str_sets: Vec<CdSetStr> = (0..n_aggs).map(|_| new_cd_set_str()).collect();
    let mut segments_processed = 0u64;

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

        segments_processed += 1;

        // Process each needed column's compressed blob. `config.blob_idx`
        // gives the persisted `_col_idx` per column: Some → read from
        // `compressed_blobs[slot]`; None → segment_by (handled via
        // `_seg_val_idx`) or column added after compression (synthesize
        // from `missing_values`).
        let mut _seg_val_idx = 0;
        for (col_idx, col_name) in config.col_names.iter().enumerate() {
            let is_segment_by = config.segment_by.contains(col_name);
            if !config.needed_cols[col_idx] {
                if is_segment_by {
                    _seg_val_idx += 1;
                }
                continue;
            }
            if is_segment_by {
                // Segment-by column: one value per segment, from segment_values
                let spec_idx = config
                    .agg_specs
                    .iter()
                    .position(|s| s.col_idx as usize == col_idx);
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

            let blob_slot = config.blob_idx.get(col_idx).copied().flatten();
            let type_oid = config.col_types[col_idx];

            // Column added after this partition was compressed → no blob,
            // just one constant Datum to record. count-distinct on a
            // column with a constant value contributes one distinct entry
            // (regardless of seg.row_count).
            let Some(slot) = blob_slot else {
                let spec_idx = config
                    .agg_specs
                    .iter()
                    .position(|s| s.col_idx as usize == col_idx);
                if let Some(si) = spec_idx
                    && let Some((datum, is_null)) =
                        config.missing_values.get(col_idx).copied().flatten()
                    && !is_null
                {
                    if config.count_distinct_only_str[col_idx] {
                        // For text columns the Datum points at a
                        // varlena; hash the bytes once since every row
                        // would yield the same value.
                        unsafe {
                            let s = pg_sys::pg_detoast_datum_packed(
                                datum.cast_mut_ptr::<pg_sys::varlena>(),
                            );
                            let len = pgrx::varlena::varsize_any_exhdr(s);
                            // `vardata_any` returns `*const c_char`,
                            // which is `i8` on some platforms (e.g.
                            // x86_64-linux) and `u8` on others
                            // (aarch64-darwin). `cast::<u8>()` makes the
                            // resulting slice type portable.
                            let body: *const u8 = pgrx::varlena::vardata_any(s).cast();
                            let slice = std::slice::from_raw_parts(body, len);
                            str_sets[si].insert(hash128_str(slice));
                        }
                    } else if config.count_distinct_only_int[col_idx] {
                        int_sets[si].insert(datum.value() as i64);
                    }
                }
                continue;
            };
            let blob = &seg.compressed_blobs[slot as usize];

            // Find the agg spec for this column
            let spec_idx = config
                .agg_specs
                .iter()
                .position(|s| s.col_idx as usize == col_idx);
            let spec_idx = match spec_idx {
                Some(i) => i,
                None => continue,
            };

            let cc_ref = compression::CompressedColumnRef::from_bytes(blob);
            let non_null_count = count_non_null(cc_ref.null_bitmap, cc_ref.row_count as usize);
            if non_null_count == 0 {
                continue;
            }

            if config.count_distinct_only_str[col_idx] {
                let seen = &mut str_sets[spec_idx];
                match cc_ref.type_tag {
                    compression::CompressionType::Dictionary
                    | compression::CompressionType::DictionaryLz4 => {
                        let norm_buf;
                        let dict_data =
                            if cc_ref.type_tag == compression::CompressionType::DictionaryLz4 {
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
                        let (buf, ranges) =
                            compression::lz4::decode_to_ranges(cc_ref.data, non_null_count);
                        let empty_hash = hash128_str(b"");
                        let mut has_empty = false;
                        for &(off, len) in &ranges {
                            if len == 0 {
                                has_empty = true;
                            } else {
                                seen.insert(hash128_str(&buf[off..off + len]));
                            }
                        }
                        if has_empty {
                            seen.insert(empty_hash);
                        }
                    }
                    compression::CompressionType::Lz4Blocked => {
                        let (buf, ranges) = compression::lz4::decode_to_ranges_blocked(
                            cc_ref.data,
                            non_null_count,
                            None,
                        );
                        let empty_hash = hash128_str(b"");
                        let mut has_empty = false;
                        for &(off, len) in &ranges {
                            if len == 0 {
                                has_empty = true;
                            } else {
                                seen.insert(hash128_str(&buf[off..off + len]));
                            }
                        }
                        if has_empty {
                            seen.insert(empty_hash);
                        }
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
                            let vals =
                                compression::bitpacked::decode_for_i64(cc_ref.data, non_null_count);
                            for v in vals {
                                seen.insert(v);
                            }
                        } else {
                            let vals =
                                compression::bitpacked::decode_for_i32(cc_ref.data, non_null_count);
                            for v in vals {
                                seen.insert(v as i64);
                            }
                        }
                    }
                    compression::CompressionType::DeltaVarint => {
                        if is_i64 {
                            let vals =
                                compression::integer::decode_i64(cc_ref.data, non_null_count);
                            for v in vals {
                                seen.insert(v);
                            }
                        } else {
                            let vals =
                                compression::integer::decode_i32(cc_ref.data, non_null_count);
                            for v in vals {
                                seen.insert(v as i64);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    ParallelCdResult {
        int_sets,
        str_sets,
        segments_processed,
    }
}

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
    // u128 values are already SipHash-128 digests; top bits are uniformly random.
    ((v >> 124) as usize) & (CD_MERGE_PARTITIONS - 1)
}

/// Returns `true` when the parallel COUNT(DISTINCT) path is eligible.
/// The caller checks this before invoking
/// [`dispatch_parallel_count_distinct_path`] so spec ownership can transfer
/// cleanly.
pub(super) fn parallel_count_distinct_eligible(
    agg_specs: &[AggExecSpec],
    group_specs: &[GroupByColSpec],
    batch_quals: &[BatchQual],
    all_segments_len: usize,
    n_workers: usize,
) -> bool {
    let has_group_by = !group_specs.is_empty();
    !has_group_by
        && n_workers > 1
        && all_segments_len > 1
        && batch_quals.is_empty()
        && !agg_specs.is_empty()
        && agg_specs
            .iter()
            .all(|s| s.agg_type == AggType::CountDistinct)
}

/// Parallel COUNT(DISTINCT) dispatch.
///
/// Callers MUST verify [`parallel_count_distinct_eligible`] before
/// invoking this — it consumes `agg_specs` / `group_specs` to install
/// them in the returned `AggScanState`.
///
/// SAFETY: calls `detoast_lazy_blobs` (PG FFI) on the segments. Must run
/// inside an active PG transaction — guaranteed when invoked from a
/// `BeginCustomScan` callback.
#[allow(clippy::too_many_arguments)]
pub(super) unsafe fn dispatch_parallel_count_distinct_path(
    agg_specs: Vec<AggExecSpec>,
    group_specs: Vec<GroupByColSpec>,
    output_map: &[OutputEntry],
    where_quals: *mut pg_sys::List,
    topn_ascending: bool,
    meta: &MetadataInfo,
    all_segments: &mut [SegmentData],
    needed_cols: &[bool],
    seg_filters: &[(usize, String)],
    time_min: Option<i64>,
    time_max: Option<i64>,
    count_distinct_only_str: &[bool],
    count_distinct_only_int: &[bool],
    n_workers: usize,
    use_lazy: bool,
    num_result_cols: usize,
    metadata_us: u64,
    heap_scan_us: u64,
    t_wall: Instant,
    total_detoast_us: &mut u64,
    total_cache_hits: &mut u64,
    total_cache_misses: &mut u64,
    total_cache_bytes_served: &mut u64,
) -> AggScanState {
    let t2 = Instant::now();

    let config = ParallelCdConfig {
        agg_specs: &agg_specs,
        col_names: &meta.col_names,
        col_types: &meta.col_types,
        segment_by: &meta.segment_by,
        blob_idx: &meta.blob_idx,
        missing_values: &meta.missing_values,
        needed_cols,
        seg_filters,
        time_min,
        time_max,
        count_distinct_only_str,
        count_distinct_only_int,
    };

    // Pipeline detoast with parallel processing
    let use_cd_pipeline = use_lazy && all_segments.len() >= n_workers * 16;
    if use_lazy {
        let t_detoast = Instant::now();
        if use_cd_pipeline {
            let n_batches = (n_workers * 2).max(2).min(all_segments.len());
            let batch_size = all_segments.len().div_ceil(n_batches);
            let first_end = batch_size.min(all_segments.len());
            for seg in &mut all_segments[..first_end] {
                let dl = unsafe { detoast_lazy_blobs(seg) };
                *total_cache_hits += dl.cache_hits;
                *total_cache_misses += dl.cache_misses;
                *total_cache_bytes_served += dl.cache_bytes_served;
            }
        } else {
            for seg in all_segments.iter_mut() {
                let dl = unsafe { detoast_lazy_blobs(seg) };
                *total_cache_hits += dl.cache_hits;
                *total_cache_misses += dl.cache_misses;
                *total_cache_bytes_served += dl.cache_bytes_served;
            }
        }
        *total_detoast_us += t_detoast.elapsed().as_micros() as u64;
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
                let handles: Vec<_> = current_batch
                    .chunks(chunk_size)
                    .map(|chunk| {
                        let cfg = &config;
                        s.spawn(move || process_cd_segments(chunk, cfg))
                    })
                    .collect();

                // Main thread detoasts next batch while workers run
                if batch_end < total_segs {
                    let t_pd = Instant::now();
                    for seg in &mut pending[..next_end - batch_end] {
                        let dl = unsafe { detoast_lazy_blobs(seg) };
                        *total_cache_hits += dl.cache_hits;
                        *total_cache_misses += dl.cache_misses;
                        *total_cache_bytes_served += dl.cache_bytes_served;
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
                .map(|chunk| s.spawn(|| process_cd_segments(chunk, &config)))
                .collect();
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
    let t_merge = Instant::now();
    let n_specs = agg_specs.len();
    let partial_refs = &partial_results;
    let is_str: Vec<bool> = agg_specs
        .iter()
        .map(|s| {
            matches!(
                s.col_type_oid,
                pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID
            )
        })
        .collect();
    let is_str_ref = &is_str;
    let bucket_counts: Vec<Vec<i64>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..CD_MERGE_PARTITIONS)
            .map(|p| {
                s.spawn(move || {
                    let mut local_int: Vec<CdSetInt> =
                        (0..n_specs).map(|_| new_cd_set_int()).collect();
                    let mut local_str: Vec<CdSetStr> =
                        (0..n_specs).map(|_| new_cd_set_str()).collect();
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
                    (0..n_specs)
                        .map(|spec_idx| {
                            if is_str_ref[spec_idx] {
                                local_str[spec_idx].len() as i64
                            } else {
                                local_int[spec_idx].len() as i64
                            }
                        })
                        .collect::<Vec<i64>>()
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let merge_us = t_merge.elapsed().as_micros() as u64;

    let mut final_counts: Vec<i64> = vec![0; n_specs];
    for bucket in &bucket_counts {
        for spec_idx in 0..n_specs {
            final_counts[spec_idx] += bucket[spec_idx];
        }
    }

    let agg_results: Vec<(pg_sys::Datum, bool)> = final_counts
        .iter()
        .map(|&c| (pg_sys::Datum::from(c as usize), false))
        .collect();
    let mut row: Vec<(pg_sys::Datum, bool)> = Vec::with_capacity(num_result_cols);
    for entry in output_map {
        match entry {
            OutputEntry::Agg(ai) => row.push(agg_results[*ai]),
            OutputEntry::Group(_) | OutputEntry::DerivedGroup { .. } => {
                row.push((pg_sys::Datum::from(0usize), true))
            }
            OutputEntry::Const(d, n) => row.push((*d, *n)),
        }
    }

    let actual_workers = partial_results.len();
    crate::scan::exec::background_drop(partial_results);

    AggScanState {
        _agg_specs: agg_specs,
        _group_specs: group_specs,
        result_rows: vec![row],
        _num_result_cols: num_result_cols,
        metadata_us,
        heap_scan_us,
        detoast_us: *total_detoast_us + pipeline_detoast_us,
        blob_cache_hits: *total_cache_hits,
        blob_cache_misses: *total_cache_misses,
        blob_cache_bytes_served: *total_cache_bytes_served,
        agg_us,
        total_segments,
        where_quals_null: where_quals.is_null(),
        topn_sort_col: -1,
        topn_ascending,
        merge_us,
        n_workers: actual_workers as u64,
        wall_us: t_wall.elapsed().as_micros() as u64,
        buf_stats: take_scan_buf_stats(),
        ..AggScanState::default()
    }
}
