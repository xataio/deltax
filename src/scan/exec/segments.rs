use pgrx::pg_sys;

use std::collections::HashMap;

use crate::compression;
use super::batch_qual::{BatchQual, BatchCompareOp, LikeStrategy, sql_like_match};
use super::datum_utils::{pg_type_oid, tupdesc_get_attr};


/// Which dict check to perform in `segment_skippable_by_dict`.
#[derive(Clone, Copy, PartialEq)]
enum DictCheck { Eq, Ne, Like, NotLike }

/// Filter for pruning segments based on min/max metadata in the companion table.
/// Built from batch quals with orderable types (int, float, timestamp, date).
pub(super) struct MinMaxFilter {
    pub(super) min_attno: usize,          // attno of _min_{col} in companion tuple
    pub(super) max_attno: usize,          // attno of _max_{col} in companion tuple
    pub(super) op: BatchCompareOp,        // Eq, Lt, Le, Gt, Ge, InList
    pub(super) const_datum: pg_sys::Datum,
    pub(super) type_oid: pg_sys::Oid,
    pub(super) in_list_i64: Option<Vec<i64>>, // for InList op
}

/// Check whether a segment might contain rows matching the filter.
/// Returns `true` if the segment should be kept (may match), `false` if it can be skipped.
pub(super) fn segment_passes_minmax_filter(
    f: &MinMaxFilter,
    values: &[pg_sys::Datum],
    nulls: &[bool],
) -> bool {
    // If either min or max is null, we can't prove anything — keep the segment
    if nulls[f.min_attno] || nulls[f.max_attno] {
        return true;
    }

    let seg_min_datum = values[f.min_attno];
    let seg_max_datum = values[f.max_attno];

    // Extract values based on type
    match f.type_oid {
        pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID
        | pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID | pg_sys::DATEOID => {
            let seg_min = seg_min_datum.value() as i64;
            let seg_max = seg_max_datum.value() as i64;
            match f.op {
                BatchCompareOp::InList => {
                    // Skip segment if NO value in the list falls within [seg_min, seg_max]
                    if let Some(ref values) = f.in_list_i64 {
                        values.iter().any(|&v| v >= seg_min && v <= seg_max)
                    } else {
                        true
                    }
                }
                _ => {
                    let c = f.const_datum.value() as i64;
                    match f.op {
                        BatchCompareOp::Eq => seg_min <= c && seg_max >= c,
                        BatchCompareOp::Ne => !(seg_min == c && seg_max == c),
                        BatchCompareOp::Lt => seg_min < c,
                        BatchCompareOp::Le => seg_min <= c,
                        BatchCompareOp::Gt => seg_max > c,
                        BatchCompareOp::Ge => seg_max >= c,
                        _ => true, // Like, NotLike — can't prune
                    }
                }
            }
        }
        pg_sys::FLOAT4OID => {
            let seg_min = f32::from_bits(seg_min_datum.value() as u32) as f64;
            let seg_max = f32::from_bits(seg_max_datum.value() as u32) as f64;
            let c = f32::from_bits(f.const_datum.value() as u32) as f64;
            match f.op {
                BatchCompareOp::Eq => seg_min <= c && seg_max >= c,
                BatchCompareOp::Ne => !(seg_min == c && seg_max == c),
                BatchCompareOp::Lt => seg_min < c,
                BatchCompareOp::Le => seg_min <= c,
                BatchCompareOp::Gt => seg_max > c,
                BatchCompareOp::Ge => seg_max >= c,
                _ => true,
            }
        }
        pg_sys::FLOAT8OID => {
            let seg_min = f64::from_bits(seg_min_datum.value() as u64);
            let seg_max = f64::from_bits(seg_max_datum.value() as u64);
            let c = f64::from_bits(f.const_datum.value() as u64);
            match f.op {
                BatchCompareOp::Eq => seg_min <= c && seg_max >= c,
                BatchCompareOp::Ne => !(seg_min == c && seg_max == c),
                BatchCompareOp::Lt => seg_min < c,
                BatchCompareOp::Le => seg_min <= c,
                BatchCompareOp::Gt => seg_max > c,
                BatchCompareOp::Ge => seg_max >= c,
                _ => true,
            }
        }
        _ => true, // Unknown type — can't prune
    }
}

/// Per-column min/max metadata from the companion table.
pub(super) struct ColMinMax {
    pub(super) min_datum: pg_sys::Datum,
    pub(super) max_datum: pg_sys::Datum,
    pub(super) min_null: bool,
    pub(super) max_null: bool,
    pub(super) type_oid: pg_sys::Oid,
}

/// Per-column sum metadata from the companion table.
#[allow(dead_code)]
pub(super) struct ColSum {
    pub(super) sum_datum: pg_sys::Datum,
    pub(super) sum_null: bool,
    pub(super) nonnull_count: i64,
    pub(super) type_oid: pg_sys::Oid,  // NUMERICOID or FLOAT8OID
}

/// Check whether a segment can be skipped based on dictionary pruning for LIKE quals.
///
/// For each LIKE/NOT LIKE batch qual, finds the corresponding compressed blob and
/// checks if it's dictionary-encoded. If so, tests dictionary entries against the
/// Check whether a segment can be skipped based on dictionary pruning for text quals.
///
/// For each LIKE/NOT LIKE/Eq/Ne batch qual on dict-encoded text columns, finds the
/// corresponding compressed blob and checks dictionary entries:
/// - **Like**: skip if NO dict entry matches the pattern (no row can match)
/// - **NotLike**: skip if ALL dict entries match the pattern (every row is filtered)
/// - **Eq**: skip if NO dict entry equals the constant (no row can match)
/// - **Ne**: skip if ALL dict entries equal the constant (every row is filtered)
///
/// Returns `true` if the segment should be skipped.
pub(super) fn segment_skippable_by_dict(
    batch_quals: &[BatchQual],
    col_names: &[String],
    segment_by: &[String],
    compressed_blobs: &[Vec<u8>],
) -> bool {
    for bq in batch_quals {
        // Determine which operation we're checking
        let check = match (&bq.op, &bq.like_strategy) {
            (BatchCompareOp::Like, Some(_)) => DictCheck::Like,
            (BatchCompareOp::NotLike, Some(_)) => DictCheck::NotLike,
            (BatchCompareOp::Eq, _) if bq.text_const.is_some() => DictCheck::Eq,
            (BatchCompareOp::Ne, _) if bq.text_const.is_some() => DictCheck::Ne,
            _ => continue,
        };

        // Compute blob index for this column
        let mut blob_idx = 0;
        for (ci, cn) in col_names.iter().enumerate() {
            if ci == bq.col_idx {
                break;
            }
            if !segment_by.contains(cn) {
                blob_idx += 1;
            }
        }

        let blob = &compressed_blobs[blob_idx];
        if blob.len() < 6 {
            continue;
        }

        // Check if dictionary-encoded
        let type_tag = compression::CompressionType::from_u8(blob[0]);
        let is_dict = matches!(
            type_tag,
            compression::CompressionType::Dictionary | compression::CompressionType::DictionaryLz4
        );
        if !is_dict {
            continue;
        }

        // Parse the compressed column header to get the data portion
        let cc = compression::CompressedColumnRef::from_bytes(blob);

        // Normalize DictionaryLz4 → Dictionary format for header parsing
        let norm_buf;
        let dict_data = if type_tag == compression::CompressionType::DictionaryLz4 {
            norm_buf = compression::dictionary::normalize_lz4(cc.data);
            &norm_buf[..]
        } else {
            cc.data
        };

        // Check dictionary entries against the predicate
        let any_match = compression::dictionary::any_entry_matches(dict_data, |text| {
            match check {
                DictCheck::Eq => text == bq.text_const.as_ref().unwrap().as_str(),
                DictCheck::Ne => text != bq.text_const.as_ref().unwrap().as_str(),
                DictCheck::Like | DictCheck::NotLike => {
                    let strategy = bq.like_strategy.as_ref().unwrap();
                    let matched = match strategy {
                        LikeStrategy::Contains(s) => text.contains(s.as_str()),
                        LikeStrategy::StartsWith(s) => text.starts_with(s.as_str()),
                        LikeStrategy::EndsWith(s) => text.ends_with(s.as_str()),
                        LikeStrategy::Exact(s) => text == s.as_str(),
                        LikeStrategy::General(p) => sql_like_match(text, p),
                    };
                    if check == DictCheck::NotLike { !matched } else { matched }
                }
            }
        });

        if !any_match {
            return true; // No rows can match — skip segment
        }
    }

    false
}

pub(super) struct SegmentData {
    pub(super) segment_values: Vec<Option<String>>,
    pub(super) compressed_blobs: Vec<Vec<u8>>,
    pub(super) row_count: i32,
    pub(super) min_time: Option<i64>,
    pub(super) max_time: Option<i64>,
    /// Per-column min/max (column name → ColMinMax).
    pub(super) col_minmax: HashMap<String, ColMinMax>,
    /// Per-column sum metadata (column name → ColSum).
    pub(super) col_sums: HashMap<String, ColSum>,
    /// Deferred TOAST pointer copies for lazy detoasting (Top-N only).
    /// Parallel to compressed_blobs: non-empty means "not yet detoasted, call
    /// detoast_lazy_blobs() to materialize". Empty means already detoasted or
    /// not needed.
    pub(super) toast_pointers: Vec<Vec<u8>>,
}

// SAFETY: SegmentData is shared across threads only via immutable references
// during parallel aggregation. The pg_sys::Datum fields in ColMinMax/ColSum
// are not accessed on worker threads (only compressed_blobs, segment_values,
// row_count, and time bounds are used). All accessed fields are safe Rust types.
unsafe impl Send for SegmentData {}
unsafe impl Sync for SegmentData {}

/// Metadata returned by the SPI metadata query.
pub(super) struct MetadataInfo {
    pub(super) col_names: Vec<String>,
    pub(super) col_types: Vec<pg_sys::Oid>,
    pub(super) col_typmods: Vec<i32>,
    pub(super) segment_by: Vec<String>,
    pub(super) order_by: Vec<String>,
    pub(super) time_column: String,
}

/// Load metadata (column names, types, segment_by) from catalog via SPI.
/// `companion_name` is the meta table name (e.g. "<partition>_meta"). The `_meta`
/// suffix is stripped to find the partition in the catalog.
pub(super) fn load_metadata(
    client: &pgrx::spi::SpiClient<'_>,
    companion_name: &str,
) -> MetadataInfo {
    // Strip _meta suffix to get the partition name for catalog lookup
    let partition_name = companion_name
        .strip_suffix("_meta")
        .unwrap_or(companion_name);

    // Get the partition's deltatable info
    let mut ht_result = client
        .select(
            "SELECT h.segment_by, h.order_by, h.time_column, h.schema_name, h.table_name
             FROM deltax_partition p
             JOIN deltax_deltatable h ON h.id = p.deltatable_id
             WHERE p.table_name = $1 AND p.is_compressed = true",
            None,
            &[partition_name.into()],
        )
        .expect("failed to query partition info");

    let ht_row = ht_result.next().unwrap_or_else(|| {
        pgrx::error!(
            "pg_deltax: no compressed partition info found for {}",
            companion_name
        );
    });

    let segment_by: Vec<String> = ht_row
        .get_datum_by_ordinal(1)
        .unwrap()
        .value::<Vec<String>>()
        .unwrap()
        .unwrap_or_default();
    let order_by: Vec<String> = ht_row
        .get_datum_by_ordinal(2)
        .unwrap()
        .value::<Vec<String>>()
        .unwrap()
        .unwrap_or_default();
    let time_column: String = ht_row
        .get_datum_by_ordinal(3)
        .unwrap()
        .value::<String>()
        .unwrap()
        .unwrap();
    let parent_schema: String = ht_row
        .get_datum_by_ordinal(4)
        .unwrap()
        .value::<String>()
        .unwrap()
        .unwrap();
    let parent_table: String = ht_row
        .get_datum_by_ordinal(5)
        .unwrap()
        .value::<String>()
        .unwrap()
        .unwrap();

    // Get column info from the parent table (pg_attribute gives us atttypmod)
    let col_result = client
        .select(
            "SELECT a.attname::text, t.typname::text, a.atttypmod
             FROM pg_attribute a
             JOIN pg_type t ON a.atttypid = t.oid
             JOIN pg_class c ON a.attrelid = c.oid
             JOIN pg_namespace n ON c.relnamespace = n.oid
             WHERE n.nspname = $1 AND c.relname = $2
               AND a.attnum > 0 AND NOT a.attisdropped
             ORDER BY a.attnum",
            None,
            &[parent_schema.as_str().into(), parent_table.as_str().into()],
        )
        .expect("failed to get column info");

    let mut col_names = Vec::new();
    let mut col_type_names = Vec::new();
    let mut col_typmods = Vec::new();
    for row in col_result {
        let name: String = row.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap().unwrap();
        let type_name: String = row.get_datum_by_ordinal(2).unwrap().value::<String>().unwrap().unwrap();
        let typmod: i32 = row.get_datum_by_ordinal(3).unwrap().value::<i32>().unwrap().unwrap_or(-1);
        col_names.push(name);
        col_type_names.push(type_name);
        col_typmods.push(typmod);
    }

    let col_types: Vec<pg_sys::Oid> = col_type_names.iter().map(|tn| pg_type_oid(tn)).collect();

    MetadataInfo {
        col_names,
        col_types,
        col_typmods,
        segment_by,
        order_by,
        time_column,
    }
}

/// Load segment data via two-phase scan: meta table (no TOAST) then blob table
/// (column-major, sequential TOAST I/O per column).
///
/// Phase 1: Heap-scan the meta table to extract segment_by values, row counts,
/// min/max, sums, and apply pruning. Zero TOAST I/O (no BYTEA columns).
///
/// Phase 2: Index-scan the blob table for each needed column, reading only
/// surviving segments. TOAST chunks are contiguous per column for sequential I/O.
///
/// When `lazy_cols` is provided, columns marked true are stored as TOAST pointer
/// copies (~18 bytes each) instead of being fully detoasted. Call
/// `detoast_lazy_blobs()` later to materialize them on demand.
#[allow(clippy::too_many_arguments)]
pub(super) unsafe fn load_segments_heap(
    meta_oid: pg_sys::Oid,
    col_names: &[String],
    segment_by: &[String],
    needed_cols: &[bool],
    time_column: &str,
    load_minmax: bool,
    segment_by_filters: &[(usize, String)],
    time_min: Option<i64>,
    time_max: Option<i64>,
    lazy_cols: Option<&[bool]>,
    batch_quals: &[BatchQual],
    load_sums: bool,
) -> (Vec<SegmentData>, u64, u64, u64) {  // last u64 = detoast_us
    unsafe {
        // ================================================================
        // Phase 1: Scan meta table — no TOAST I/O
        // ================================================================
        let rel = pg_sys::table_open(meta_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        let tupdesc = (*rel).rd_att;
        let natts = (*tupdesc).natts as usize;

        // Build column-name-to-attno mapping from meta TupleDesc
        let mut attno_map: HashMap<String, usize> = HashMap::new();
        let mut att_type_oids: HashMap<String, pg_sys::Oid> = HashMap::new();
        for i in 0..natts {
            let att = &*tupdesc_get_attr(tupdesc, i);
            if att.attisdropped {
                continue;
            }
            let name = std::ffi::CStr::from_ptr(att.attname.data.as_ptr())
                .to_string_lossy()
                .into_owned();
            att_type_oids.insert(name.clone(), att.atttypid);
            attno_map.insert(name, i);
        }

        // Locate attribute indices for segment_by columns and _row_count
        let mut segment_by_attnos: Vec<(usize, pg_sys::Oid)> = Vec::new();
        for name in col_names {
            if segment_by.contains(name)
                && let Some(&attno) = attno_map.get(name.as_str())
            {
                let type_oid = att_type_oids[name.as_str()];
                segment_by_attnos.push((attno, type_oid));
            }
        }

        let row_count_attno = attno_map.get("_row_count").copied();
        let segment_id_attno = attno_map.get("_segment_id").copied();

        let min_time_name = format!("_min_{}", time_column);
        let max_time_name = format!("_max_{}", time_column);
        let min_time_attno = attno_map.get(min_time_name.as_str()).copied();
        let max_time_attno = attno_map.get(max_time_name.as_str()).copied();

        // Discover per-column min/max columns
        let mut minmax_col_attnos: Vec<(String, usize, usize, pg_sys::Oid)> = Vec::new();
        if load_minmax {
            for col_name in col_names {
                if segment_by.contains(col_name) {
                    continue;
                }
                let min_name = format!("_min_{}", col_name);
                let max_name = format!("_max_{}", col_name);
                if let (Some(&min_att), Some(&max_att)) = (
                    attno_map.get(min_name.as_str()),
                    attno_map.get(max_name.as_str()),
                ) {
                    let type_oid = att_type_oids.get(min_name.as_str()).copied()
                        .unwrap_or(pg_sys::InvalidOid);
                    minmax_col_attnos.push((col_name.clone(), min_att, max_att, type_oid));
                }
            }
        }

        // Discover per-column sum/nonnull_count columns
        let mut sum_col_attnos: Vec<(String, usize, usize, pg_sys::Oid)> = Vec::new();
        if load_sums {
            for col_name in col_names {
                if segment_by.contains(col_name) {
                    continue;
                }
                let sum_name = format!("_sum_{}", col_name);
                let nonnull_name = format!("_nonnull_count_{}", col_name);
                if let (Some(&sum_att), Some(&nn_att)) = (
                    attno_map.get(sum_name.as_str()),
                    attno_map.get(nonnull_name.as_str()),
                ) {
                    let type_oid = att_type_oids.get(sum_name.as_str()).copied()
                        .unwrap_or(pg_sys::InvalidOid);
                    sum_col_attnos.push((col_name.clone(), sum_att, nn_att, type_oid));
                }
            }
        }

        // Build min/max predicate filters from batch quals
        let mut minmax_filters: Vec<MinMaxFilter> = Vec::new();
        for bq in batch_quals {
            match bq.op {
                BatchCompareOp::Like | BatchCompareOp::NotLike => continue,
                _ => {}
            }
            match bq.type_oid {
                pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID
                | pg_sys::FLOAT4OID | pg_sys::FLOAT8OID
                | pg_sys::DATEOID | pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID => {}
                _ => continue,
            }
            let col_name = &col_names[bq.col_idx];
            let min_name = format!("_min_{}", col_name);
            let max_name = format!("_max_{}", col_name);
            if let (Some(&min_att), Some(&max_att)) = (
                attno_map.get(min_name.as_str()),
                attno_map.get(max_name.as_str()),
            ) {
                minmax_filters.push(MinMaxFilter {
                    min_attno: min_att,
                    max_attno: max_att,
                    op: bq.op,
                    const_datum: bq.const_datum,
                    type_oid: bq.type_oid,
                    in_list_i64: bq.in_list_i64.clone(),
                });
            }
        }

        // Begin meta table scan
        let snapshot = pg_sys::GetActiveSnapshot();
        let flags: u32 = pg_sys::ScanOptions::SO_TYPE_SEQSCAN
            | pg_sys::ScanOptions::SO_ALLOW_STRAT
            | pg_sys::ScanOptions::SO_ALLOW_SYNC
            | pg_sys::ScanOptions::SO_ALLOW_PAGEMODE;
        let scan = (*(*rel).rd_tableam).scan_begin.unwrap()(
            rel,
            snapshot,
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            flags,
        );

        // Surviving segment metadata: (index_in_segments_vec, segment_id)
        let mut segments: Vec<SegmentData> = Vec::new();
        let mut surviving_segment_ids: Vec<i32> = Vec::new();
        let mut segments_skipped: u64 = 0;
        let mut segments_minmax_skipped: u64 = 0;
        let mut heap_getnext_us: u64 = 0;
        let mut deform_us: u64 = 0;
        let mut values = vec![pg_sys::Datum::from(0); natts];
        let mut nulls = vec![true; natts];

        // Build col_idx mapping: for each col_names[i] that is not segment_by,
        // compute its col_idx (0-based among non-segment-by columns)
        let mut col_idx_map: Vec<Option<u16>> = Vec::new(); // parallel to col_names: Some(col_idx) for non-seg-by, None for seg-by
        let mut num_blob_cols: usize = 0;
        {
            let mut ci: u16 = 0;
            for name in col_names {
                if segment_by.contains(name) {
                    col_idx_map.push(None);
                } else {
                    col_idx_map.push(Some(ci));
                    ci += 1;
                    num_blob_cols += 1;
                }
            }
        }

        loop {
            let getnext_start = std::time::Instant::now();
            let tuple = pg_sys::heap_getnext(
                scan,
                pg_sys::ScanDirection::ForwardScanDirection,
            );
            heap_getnext_us += getnext_start.elapsed().as_micros() as u64;
            if tuple.is_null() {
                break;
            }

            let deform_start = std::time::Instant::now();
            pg_sys::heap_deform_tuple(
                tuple,
                tupdesc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            );
            deform_us += deform_start.elapsed().as_micros() as u64;

            // Extract _segment_id
            let segment_id = match segment_id_attno {
                Some(attno) if !nulls[attno] => values[attno].value() as i32,
                _ => 0,
            };

            // Extract segment_by values
            let mut segment_values: Vec<Option<String>> = Vec::new();
            for &(attno, type_oid) in &segment_by_attnos {
                if !nulls[attno] {
                    let mut typoutput: pg_sys::Oid = pg_sys::InvalidOid;
                    let mut typisvarlena: bool = false;
                    pg_sys::getTypeOutputInfo(type_oid, &mut typoutput, &mut typisvarlena);
                    let cstr = pg_sys::OidOutputFunctionCall(typoutput, values[attno]);
                    let s = std::ffi::CStr::from_ptr(cstr)
                        .to_string_lossy()
                        .into_owned();
                    pg_sys::pfree(cstr as *mut _);
                    segment_values.push(Some(s));
                } else {
                    segment_values.push(None);
                }
            }

            let row_count = match row_count_attno {
                Some(attno) if !nulls[attno] => values[attno].value() as i32,
                _ => 0,
            };

            let seg_min_time = match min_time_attno {
                Some(attno) if !nulls[attno] => Some(values[attno].value() as i64),
                _ => None,
            };
            let seg_max_time = match max_time_attno {
                Some(attno) if !nulls[attno] => Some(values[attno].value() as i64),
                _ => None,
            };

            // --- Pruning (same logic as before, zero TOAST I/O) ---

            if !segment_by_filters.is_empty() {
                let mut skip = false;
                for &(seg_val_idx, ref filter_val) in segment_by_filters {
                    match &segment_values.get(seg_val_idx).and_then(|v| v.as_ref()) {
                        Some(val) if *val == filter_val => {}
                        _ => { skip = true; break; }
                    }
                }
                if skip {
                    segments_skipped += 1;
                    continue;
                }
            }

            if let (Some(s_min), Some(s_max)) = (seg_min_time, seg_max_time)
                && (time_min.is_some_and(|qmin| s_max < qmin)
                    || time_max.is_some_and(|qmax| s_min > qmax))
            {
                segments_skipped += 1;
                continue;
            }

            if !minmax_filters.is_empty() {
                let mut minmax_skip = false;
                for f in &minmax_filters {
                    if !segment_passes_minmax_filter(f, &values, &nulls) {
                        minmax_skip = true;
                        break;
                    }
                }
                if minmax_skip {
                    segments_skipped += 1;
                    segments_minmax_skipped += 1;
                    continue;
                }
            }

            // --- Segment survived pruning ---

            // Extract per-column min/max
            let mut col_minmax = HashMap::new();
            for (col_name, min_att, max_att, type_oid) in &minmax_col_attnos {
                col_minmax.insert(col_name.clone(), ColMinMax {
                    min_datum: if nulls[*min_att] { pg_sys::Datum::from(0usize) } else { values[*min_att] },
                    max_datum: if nulls[*max_att] { pg_sys::Datum::from(0usize) } else { values[*max_att] },
                    min_null: nulls[*min_att],
                    max_null: nulls[*max_att],
                    type_oid: *type_oid,
                });
            }

            // Extract per-column sum/nonnull_count
            let mut col_sums = HashMap::new();
            for (col_name, sum_att, nn_att, type_oid) in &sum_col_attnos {
                let sum_null = nulls[*sum_att];
                let sum_datum = if sum_null { pg_sys::Datum::from(0usize) } else { values[*sum_att] };
                let nonnull_count = if nulls[*nn_att] { 0i64 } else { values[*nn_att].value() as i64 };
                col_sums.insert(col_name.clone(), ColSum {
                    sum_datum,
                    sum_null,
                    nonnull_count,
                    type_oid: *type_oid,
                });
            }

            // Pre-allocate empty blob slots — will be filled in Phase 2
            let compressed_blobs: Vec<Vec<u8>> = vec![Vec::new(); num_blob_cols];
            let toast_pointers: Vec<Vec<u8>> = vec![Vec::new(); num_blob_cols];

            surviving_segment_ids.push(segment_id);
            segments.push(SegmentData {
                segment_values,
                compressed_blobs,
                row_count,
                min_time: seg_min_time,
                max_time: seg_max_time,
                col_minmax,
                col_sums,
                toast_pointers,
            });
        }

        // End meta scan
        (*(*rel).rd_tableam).scan_end.unwrap()(scan);
        pg_sys::table_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

        pgrx::log!(
            "load_segments_heap phase1: segments={} skipped={} heap_getnext={:.1}ms deform={:.1}ms",
            segments.len(),
            segments_skipped,
            heap_getnext_us as f64 / 1000.0,
            deform_us as f64 / 1000.0,
        );

        // ================================================================
        // Phase 2: Scan blob table — sequential TOAST I/O per column
        // ================================================================
        let mut detoast_us: u64 = 0;

        // Check if any blobs are needed
        let any_blobs_needed = col_names.iter().enumerate().any(|(i, name)| {
            !segment_by.contains(name) && needed_cols[i]
        });

        if !segments.is_empty() && any_blobs_needed {
            // Derive blob table OID from meta table name
            let meta_name_ptr = pg_sys::get_rel_name(meta_oid);
            let meta_name = std::ffi::CStr::from_ptr(meta_name_ptr)
                .to_string_lossy()
                .into_owned();
            let meta_ns_oid = pg_sys::get_rel_namespace(meta_oid);

            // Strip "_meta" suffix to get partition name, then add "_blobs"
            let partition_name = meta_name.strip_suffix("_meta").unwrap_or(&meta_name);
            let blobs_name = format!("{}_blobs", partition_name);
            let blobs_cname = std::ffi::CString::new(blobs_name).unwrap();
            let blob_oid = pg_sys::get_relname_relid(blobs_cname.as_ptr(), meta_ns_oid);

            if blob_oid != pg_sys::InvalidOid {
                // Build surviving segment_id → segment index mapping
                let mut seg_id_to_idx: HashMap<i32, usize> = HashMap::new();
                for (idx, &sid) in surviving_segment_ids.iter().enumerate() {
                    seg_id_to_idx.insert(sid, idx);
                }

                // Determine which col_idx values we need
                let mut needed_col_indices: Vec<(u16, usize)> = Vec::new(); // (col_idx, blob_slot_idx)
                for (i, name) in col_names.iter().enumerate() {
                    if segment_by.contains(name) {
                        continue;
                    }
                    let ci = col_idx_map[i].unwrap();
                    if needed_cols[i] {
                        needed_col_indices.push((ci, ci as usize));
                    }
                }

                // Open blob table + its PK index
                let blob_rel = pg_sys::table_open(blob_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
                let blob_tupdesc = (*blob_rel).rd_att;

                // Find PK index OID — first index that is primary
                let pk_index_oid = {
                    let mut pk_oid = pg_sys::InvalidOid;
                    let index_list = pg_sys::RelationGetIndexList(blob_rel);
                    if !index_list.is_null() {
                        let n = (*index_list).length;
                        for i in 0..n {
                            let idx_oid =
                                (*(*index_list).elements.add(i as usize)).oid_value;
                            let idx_rel = pg_sys::index_open(idx_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
                            let is_primary = if !(*idx_rel).rd_index.is_null() {
                                (*(*idx_rel).rd_index).indisprimary
                            } else { false };
                            pg_sys::index_close(idx_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
                            if is_primary {
                                pk_oid = idx_oid;
                                break;
                            }
                        }
                        pg_sys::list_free(index_list);
                    }
                    pk_oid
                };

                let detoast_start = std::time::Instant::now();

                if pk_index_oid != pg_sys::InvalidOid {
                    let idx_rel = pg_sys::index_open(pk_index_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

                    for &(col_idx, blob_slot) in &needed_col_indices {
                        let is_lazy = lazy_cols.is_some_and(|lc| {
                            // Find the original col_names index for this col_idx
                            col_names.iter().enumerate().any(|(i, name)| {
                                !segment_by.contains(name) && col_idx_map[i] == Some(col_idx) && i < lc.len() && lc[i]
                            })
                        });

                        // Set up scan key: _col_idx = col_idx (SMALLINT equality)
                        let mut skey = [pg_sys::ScanKeyData::default()];
                        pg_sys::ScanKeyInit(
                            &mut skey[0],
                            1,  // attnum 1 = _col_idx
                            pg_sys::BTEqualStrategyNumber as u16,
                            pg_sys::F_INT2EQ.into(),
                            pg_sys::Datum::from(col_idx as i16),
                        );

                        #[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
                        let scan = pg_sys::index_beginscan(blob_rel, idx_rel, snapshot, 1, 0);
                        #[cfg(feature = "pg18")]
                        let scan = pg_sys::index_beginscan(blob_rel, idx_rel, snapshot, std::ptr::null_mut(), 1, 0);
                        pg_sys::index_rescan(scan, skey.as_mut_ptr(), 1, std::ptr::null_mut(), 0);

                        // Allocate slot for tuple extraction
                        let slot = pg_sys::table_slot_create(blob_rel, std::ptr::null_mut());

                        loop {
                            if !pg_sys::index_getnext_slot(scan, pg_sys::ScanDirection::ForwardScanDirection, slot) {
                                break;
                            }

                            // Extract _segment_id (attnum 2) and _data (attnum 3)
                            let mut blob_values = [pg_sys::Datum::from(0); 3];
                            let mut blob_nulls = [true; 3];
                            pg_sys::slot_getallattrs(slot);
                            let tts_values = (*slot).tts_values;
                            let tts_isnull = (*slot).tts_isnull;
                            for j in 0..3usize {
                                blob_values[j] = *tts_values.add(j);
                                blob_nulls[j] = *tts_isnull.add(j);
                            }

                            if blob_nulls[1] {
                                continue; // no segment_id — skip
                            }
                            let seg_id = blob_values[1].value() as i32;

                            // Check if this segment survived pruning
                            let seg_idx = match seg_id_to_idx.get(&seg_id) {
                                Some(&idx) => idx,
                                None => continue, // pruned — skip without detoasting
                            };

                            if blob_nulls[2] {
                                // null blob — leave empty
                                continue;
                            }

                            if is_lazy {
                                // Lazy: copy just the TOAST pointer
                                let varlena_ptr = blob_values[2].cast_mut_ptr::<pg_sys::varlena>();
                                let ptr_size = pgrx::varsize_any(varlena_ptr);
                                let mut ptr_copy = vec![0u8; ptr_size];
                                std::ptr::copy_nonoverlapping(
                                    varlena_ptr as *const u8,
                                    ptr_copy.as_mut_ptr(),
                                    ptr_size,
                                );
                                segments[seg_idx].toast_pointers[blob_slot] = ptr_copy;
                            } else {
                                // Eager: detoast immediately
                                let varlena_ptr: *mut pg_sys::varlena = blob_values[2].cast_mut_ptr();
                                let detoasted = pg_sys::pg_detoast_datum(varlena_ptr);
                                let len = pgrx::varsize_any_exhdr(detoasted);
                                let data = pgrx::vardata_any(detoasted);
                                #[allow(clippy::unnecessary_cast)]
                                let bytes = std::slice::from_raw_parts(
                                    data as *const u8,
                                    len,
                                )
                                .to_vec();
                                let was_toasted = detoasted != varlena_ptr;
                                if was_toasted {
                                    pg_sys::pfree(detoasted as *mut _);
                                }
                                segments[seg_idx].compressed_blobs[blob_slot] = bytes;
                            }
                        }

                        pg_sys::ExecDropSingleTupleTableSlot(slot);
                        pg_sys::index_endscan(scan);
                    }

                    pg_sys::index_close(idx_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
                } else {
                    // Fallback: sequential scan of blob table (no PK index found)
                    let blob_flags: u32 = pg_sys::ScanOptions::SO_TYPE_SEQSCAN
                        | pg_sys::ScanOptions::SO_ALLOW_STRAT
                        | pg_sys::ScanOptions::SO_ALLOW_SYNC
                        | pg_sys::ScanOptions::SO_ALLOW_PAGEMODE;
                    let blob_scan = (*(*blob_rel).rd_tableam).scan_begin.unwrap()(
                        blob_rel, snapshot, 0, std::ptr::null_mut(), std::ptr::null_mut(), blob_flags,
                    );

                    let blob_natts = (*blob_tupdesc).natts as usize;
                    let mut bv = vec![pg_sys::Datum::from(0); blob_natts];
                    let mut bn = vec![true; blob_natts];

                    // Build set of needed col indices for fast lookup
                    let needed_set: std::collections::HashSet<u16> = needed_col_indices.iter().map(|&(ci, _)| ci).collect();

                    loop {
                        let tuple = pg_sys::heap_getnext(blob_scan, pg_sys::ScanDirection::ForwardScanDirection);
                        if tuple.is_null() { break; }
                        pg_sys::heap_deform_tuple(tuple, blob_tupdesc, bv.as_mut_ptr(), bn.as_mut_ptr());

                        if bn[0] || bn[1] { continue; }
                        let ci = bv[0].value() as u16;
                        let seg_id = bv[1].value() as i32;

                        if !needed_set.contains(&ci) { continue; }
                        let seg_idx = match seg_id_to_idx.get(&seg_id) {
                            Some(&idx) => idx,
                            None => continue,
                        };
                        if bn[2] { continue; }

                        let blob_slot = ci as usize;
                        let is_lazy = lazy_cols.is_some_and(|lc| {
                            col_names.iter().enumerate().any(|(i, name)| {
                                !segment_by.contains(name) && col_idx_map[i] == Some(ci) && i < lc.len() && lc[i]
                            })
                        });

                        if is_lazy {
                            let varlena_ptr = bv[2].cast_mut_ptr::<pg_sys::varlena>();
                            let ptr_size = pgrx::varsize_any(varlena_ptr);
                            let mut ptr_copy = vec![0u8; ptr_size];
                            std::ptr::copy_nonoverlapping(varlena_ptr as *const u8, ptr_copy.as_mut_ptr(), ptr_size);
                            segments[seg_idx].toast_pointers[blob_slot] = ptr_copy;
                        } else {
                            let varlena_ptr: *mut pg_sys::varlena = bv[2].cast_mut_ptr();
                            let detoasted = pg_sys::pg_detoast_datum(varlena_ptr);
                            let len = pgrx::varsize_any_exhdr(detoasted);
                            let data = pgrx::vardata_any(detoasted);
                            #[allow(clippy::unnecessary_cast)]
                            let bytes = std::slice::from_raw_parts(data as *const u8, len).to_vec();
                            if detoasted != varlena_ptr { pg_sys::pfree(detoasted as *mut _); }
                            segments[seg_idx].compressed_blobs[blob_slot] = bytes;
                        }
                    }

                    (*(*blob_rel).rd_tableam).scan_end.unwrap()(blob_scan);
                }

                detoast_us = detoast_start.elapsed().as_micros() as u64;

                pg_sys::table_close(blob_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            }
        }

        pgrx::log!(
            "load_segments_heap phase2: segments={} skipped={} detoast={:.1}ms",
            segments.len(),
            segments_skipped,
            detoast_us as f64 / 1000.0,
        );

        (segments, segments_skipped, segments_minmax_skipped, detoast_us)
    }
}

/// Materialize deferred TOAST pointers for a segment.
///
/// For each blob index that has a non-empty toast_pointer, calls pg_detoast_datum
/// on the stored pointer copy and replaces the empty compressed_blob with the
/// detoasted data. Clears the toast_pointer after detoasting.
pub(super) unsafe fn detoast_lazy_blobs(seg: &mut SegmentData) {
    unsafe {
        for bi in 0..seg.toast_pointers.len() {
            if seg.toast_pointers[bi].is_empty() {
                continue;
            }
            let ptr = seg.toast_pointers[bi].as_ptr() as *mut pg_sys::varlena;
            let detoasted = pg_sys::pg_detoast_datum(ptr);
            let len = pgrx::varsize_any_exhdr(detoasted);
            let data = pgrx::vardata_any(detoasted);
            #[allow(clippy::unnecessary_cast)]
            let bytes = std::slice::from_raw_parts(data as *const u8, len).to_vec();
            if detoasted != ptr {
                pg_sys::pfree(detoasted as *mut _);
            }
            seg.compressed_blobs[bi] = bytes;
            seg.toast_pointers[bi].clear();
        }
    }
}

/// Materialize deferred TOAST pointers for specific blob indices only.
///
/// Like `detoast_lazy_blobs` but only processes the given blob indices,
/// leaving other blobs lazy. Used in top-N Phase 1 to detoast only
/// filter + sort column blobs while deferring Phase 2 columns.
pub(super) unsafe fn detoast_lazy_blobs_selective(seg: &mut SegmentData, blob_indices: &[usize]) {
    unsafe {
        for &bi in blob_indices {
            if bi >= seg.toast_pointers.len() || seg.toast_pointers[bi].is_empty() {
                continue;
            }
            let ptr = seg.toast_pointers[bi].as_ptr() as *mut pg_sys::varlena;
            let detoasted = pg_sys::pg_detoast_datum(ptr);
            let len = pgrx::varsize_any_exhdr(detoasted);
            let data = pgrx::vardata_any(detoasted);
            #[allow(clippy::unnecessary_cast)]
            let bytes = std::slice::from_raw_parts(data as *const u8, len).to_vec();
            if detoasted != ptr {
                pg_sys::pfree(detoasted as *mut _);
            }
            seg.compressed_blobs[bi] = bytes;
            seg.toast_pointers[bi].clear();
        }
    }
}

/// Extract segment pruning filters from the plan qual (raw expression tree).
///
/// Walks OpExpr nodes looking for:
/// - Equality filters on segment_by columns (e.g. `CounterID = 62`)
/// - Range filters on the time column (e.g. `ts >= '2023-01-01'`)
///
/// Returns (segment_by_filters, time_min, time_max).
pub(super) unsafe fn extract_segment_filters(
    qual_list: *mut pg_sys::List,
    col_names: &[String],
    segment_by: &[String],
    time_column: &str,
) -> (Vec<(usize, String)>, Option<i64>, Option<i64>) {
    let mut segment_by_filters: Vec<(usize, String)> = Vec::new();
    let mut time_min: Option<i64> = None;
    let mut time_max: Option<i64> = None;

    if qual_list.is_null() {
        return (segment_by_filters, time_min, time_max);
    }

    unsafe {
        // Build segment_by column name -> segment_values index mapping
        let mut seg_val_index_map: HashMap<&str, usize> = HashMap::new();
        let mut seg_val_idx = 0;
        for name in col_names {
            if segment_by.contains(name) {
                seg_val_index_map.insert(name.as_str(), seg_val_idx);
                seg_val_idx += 1;
            }
        }

        let nquals = (*qual_list).length;
        for i in 0..nquals {
            let cell = (*qual_list).elements.add(i as usize);
            let node = (*cell).ptr_value as *const pg_sys::Node;
            if node.is_null() {
                continue;
            }

            let tag = (*node).type_;
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
            let opname = std::ffi::CStr::from_ptr(opname_ptr)
                .to_str()
                .unwrap_or("");

            // Get the two args
            let arg0 = (*(*args).elements.add(0)).ptr_value as *const pg_sys::Node;
            let arg1 = (*(*args).elements.add(1)).ptr_value as *const pg_sys::Node;
            if arg0.is_null() || arg1.is_null() {
                continue;
            }

            // Identify Var and Const (handle both orderings)
            let (var_node, const_node, var_on_left) =
                if (*arg0).type_ == pg_sys::NodeTag::T_Var
                    && (*arg1).type_ == pg_sys::NodeTag::T_Const
                {
                    (arg0 as *const pg_sys::Var, arg1 as *const pg_sys::Const, true)
                } else if (*arg0).type_ == pg_sys::NodeTag::T_Const
                    && (*arg1).type_ == pg_sys::NodeTag::T_Var
                {
                    (arg1 as *const pg_sys::Var, arg0 as *const pg_sys::Const, false)
                } else {
                    continue;
                };

            if (*const_node).constisnull {
                continue;
            }

            // Convert 1-based varattno to 0-based column index
            let varattno = (*var_node).varattno as i32;
            if varattno < 1 || varattno as usize > col_names.len() {
                continue;
            }
            let col_idx = (varattno - 1) as usize;
            let col_name = &col_names[col_idx];

            // Check if this is a segment_by equality filter
            if opname == "="
                && let Some(&sv_idx) = seg_val_index_map.get(col_name.as_str())
            {
                // Extract const value as string (matches how segment_values are stored)
                let mut typoutput: pg_sys::Oid = pg_sys::InvalidOid;
                let mut typisvarlena: bool = false;
                pg_sys::getTypeOutputInfo(
                    (*const_node).consttype,
                    &mut typoutput,
                    &mut typisvarlena,
                );
                let cstr = pg_sys::OidOutputFunctionCall(typoutput, (*const_node).constvalue);
                let s = std::ffi::CStr::from_ptr(cstr)
                    .to_string_lossy()
                    .into_owned();
                pg_sys::pfree(cstr as *mut _);
                segment_by_filters.push((sv_idx, s));
            }

            // Check if this is a time column range filter
            if col_name == time_column {
                let ts_val = (*const_node).constvalue.value() as i64;

                // Normalize operator direction (if Var is on right, flip the operator)
                let effective_op = if var_on_left {
                    opname
                } else {
                    match opname {
                        ">=" => "<=",
                        ">" => "<",
                        "<=" => ">=",
                        "<" => ">",
                        _ => opname,
                    }
                };

                match effective_op {
                    ">=" | ">" => {
                        // Lower bound: take the maximum of all lower bounds
                        time_min = Some(match time_min {
                            Some(existing) => existing.max(ts_val),
                            None => ts_val,
                        });
                    }
                    "<=" | "<" => {
                        // Upper bound: take the minimum of all upper bounds
                        time_max = Some(match time_max {
                            Some(existing) => existing.min(ts_val),
                            None => ts_val,
                        });
                    }
                    _ => {}
                }
            }
        }
    }

    (segment_by_filters, time_min, time_max)
}
