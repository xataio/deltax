use pgrx::pg_sys;

use std::collections::HashMap;

use crate::compression;
use super::batch_qual::{BatchQual, BatchCompareOp, LikeStrategy, sql_like_match};
use super::datum_utils::{pg_type_oid, tupdesc_get_attr};

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
                        BatchCompareOp::Lt => seg_min < c,
                        BatchCompareOp::Le => seg_min <= c,
                        BatchCompareOp::Gt => seg_max > c,
                        BatchCompareOp::Ge => seg_max >= c,
                        _ => true, // Ne, Like, NotLike — can't prune
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
/// LIKE pattern. If no entry matches (for LIKE) or all entries match (for NOT LIKE),
/// the segment definitely has zero matching rows and can be skipped entirely.
///
/// Returns `true` if the segment should be skipped.
pub(super) fn segment_skippable_by_dict_like(
    batch_quals: &[BatchQual],
    col_names: &[String],
    segment_by: &[String],
    compressed_blobs: &[Vec<u8>],
) -> bool {
    // Find LIKE/NOT LIKE quals
    for bq in batch_quals {
        let (strategy, negate) = match (&bq.op, &bq.like_strategy) {
            (BatchCompareOp::Like, Some(s)) => (s, false),
            (BatchCompareOp::NotLike, Some(s)) => (s, true),
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

        // Check if any dictionary entry matches the LIKE pattern
        let any_match = compression::dictionary::any_entry_matches(dict_data, |text| {
            let matched = match strategy {
                LikeStrategy::Contains(s) => text.contains(s.as_str()),
                LikeStrategy::StartsWith(s) => text.starts_with(s.as_str()),
                LikeStrategy::EndsWith(s) => text.ends_with(s.as_str()),
                LikeStrategy::Exact(s) => text == s.as_str(),
                LikeStrategy::General(p) => sql_like_match(text, p),
            };
            if negate { !matched } else { matched }
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
    pub(super) time_column: String,
}

/// Load metadata (column names, types, segment_by) from catalog via SPI.
pub(super) fn load_metadata(
    client: &pgrx::spi::SpiClient<'_>,
    companion_name: &str,
) -> MetadataInfo {
    // Get the partition's deltatable info
    let mut ht_result = client
        .select(
            "SELECT h.segment_by, h.order_by, h.time_column, h.schema_name, h.table_name
             FROM deltax_partition p
             JOIN deltax_deltatable h ON h.id = p.deltatable_id
             WHERE p.table_name = $1 AND p.is_compressed = true",
            None,
            &[companion_name.into()],
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
        time_column,
    }
}

/// Load segment data from the companion table via direct heap scan.
///
/// Bypasses SPI entirely — opens the companion table, iterates all tuples
/// with `heap_getnext`, and extracts segment_by values, compressed BYTEA blobs,
/// and row counts directly from the heap tuples.
///
/// When `lazy_cols` is provided, columns marked true are stored as TOAST pointer
/// copies (~18 bytes each) instead of being fully detoasted. Call
/// `detoast_lazy_blobs()` later to materialize them on demand.
#[allow(clippy::too_many_arguments)]
pub(super) unsafe fn load_segments_heap(
    companion_oid: pg_sys::Oid,
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
) -> (Vec<SegmentData>, u64, u64) {
    unsafe {
        // Open companion table with AccessShareLock
        let rel = pg_sys::table_open(companion_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        let tupdesc = (*rel).rd_att;
        let natts = (*tupdesc).natts as usize;

        // Build column-name-to-attno mapping from companion TupleDesc
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

        // Locate attribute indices for segment_by columns, compressed columns, and _row_count
        let mut segment_by_attnos: Vec<(usize, pg_sys::Oid)> = Vec::new(); // (attno, type_oid)
        for name in col_names {
            if segment_by.contains(name)
                && let Some(&attno) = attno_map.get(name.as_str())
            {
                let type_oid = att_type_oids[name.as_str()];
                segment_by_attnos.push((attno, type_oid));
            }
        }

        let mut compressed_attnos: Vec<Option<usize>> = Vec::new(); // Some(attno) for needed, None for unneeded
        let mut blob_is_lazy: Vec<bool> = Vec::new(); // parallel to compressed_attnos: true = store TOAST pointer only
        for (idx, name) in col_names.iter().enumerate() {
            if !segment_by.contains(name) {
                if needed_cols[idx] {
                    let comp_name = format!("_{}_compressed", name);
                    compressed_attnos.push(attno_map.get(comp_name.as_str()).copied());
                    blob_is_lazy.push(lazy_cols.is_some_and(|lc| idx < lc.len() && lc[idx]));
                } else {
                    compressed_attnos.push(None);
                    blob_is_lazy.push(false);
                }
            }
        }

        let row_count_attno = attno_map.get("_row_count").copied();

        let min_time_name = format!("_min_{}", time_column);
        let max_time_name = format!("_max_{}", time_column);
        let min_time_attno = attno_map.get(min_time_name.as_str()).copied();
        let max_time_attno = attno_map.get(max_time_name.as_str()).copied();

        // Discover per-column min/max columns: (col_name, min_attno, max_attno, type_oid)
        // Only needed for MinMax pushdown scans — skip for regular decompress scans
        // to avoid overhead from deforming 100+ extra attributes.
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

        // Discover per-column sum/nonnull_count columns: (col_name, sum_attno, nonnull_count_attno, type_oid)
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
            // Only orderable types (not BOOL, not text, not LIKE/NotLike/Ne)
            match bq.op {
                BatchCompareOp::Ne | BatchCompareOp::Like | BatchCompareOp::NotLike => continue,
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

        // Begin table scan via TableAmRoutine vtable
        // (table_beginscan is static inline in C, so we call via the vtable)
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

        // Iterate all tuples
        let mut segments = Vec::new();
        let mut segments_skipped: u64 = 0;
        let mut segments_minmax_skipped: u64 = 0;
        let mut values = vec![pg_sys::Datum::from(0); natts];
        let mut nulls = vec![true; natts];

        loop {
            let tuple = pg_sys::heap_getnext(
                scan,
                pg_sys::ScanDirection::ForwardScanDirection,
            );
            if tuple.is_null() {
                break;
            }

            // Deform tuple into datums + nulls arrays
            pg_sys::heap_deform_tuple(
                tuple,
                tupdesc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            );

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

            // Extract _row_count (INT4) — cheap, needed before pruning
            let row_count = match row_count_attno {
                Some(attno) if !nulls[attno] => values[attno].value() as i32,
                _ => 0,
            };

            // Extract min/max time (TIMESTAMPTZ stored as i64 PG epoch microseconds) — cheap
            let seg_min_time = match min_time_attno {
                Some(attno) if !nulls[attno] => Some(values[attno].value() as i64),
                _ => None,
            };
            let seg_max_time = match max_time_attno {
                Some(attno) if !nulls[attno] => Some(values[attno].value() as i64),
                _ => None,
            };

            // --- Lazy pruning: skip blob detoasting for segments that fail filters ---

            // Check segment_by filters
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

            // Check time range filters
            if let (Some(s_min), Some(s_max)) = (seg_min_time, seg_max_time)
                && (time_min.is_some_and(|qmin| s_max < qmin)
                    || time_max.is_some_and(|qmax| s_min > qmax))
            {
                segments_skipped += 1;
                continue;
            }

            // Check min/max predicate filters
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

            // --- Segment passed pruning: detoast blobs ---

            // Extract compressed BYTEA blobs
            let mut compressed_blobs: Vec<Vec<u8>> = Vec::new();
            let mut toast_pointers: Vec<Vec<u8>> = Vec::new();
            for (bi, opt_attno) in compressed_attnos.iter().enumerate() {
                match opt_attno {
                    Some(attno) => {
                        let attno = *attno;
                        if !nulls[attno] {
                            if blob_is_lazy[bi] {
                                // Lazy: copy just the TOAST pointer (~18 bytes)
                                let varlena_ptr = values[attno].cast_mut_ptr::<pg_sys::varlena>();
                                let ptr_size = pgrx::varsize_any(varlena_ptr);
                                let mut ptr_copy = vec![0u8; ptr_size];
                                std::ptr::copy_nonoverlapping(
                                    varlena_ptr as *const u8,
                                    ptr_copy.as_mut_ptr(),
                                    ptr_size,
                                );
                                compressed_blobs.push(Vec::new());
                                toast_pointers.push(ptr_copy);
                            } else {
                                // Eager: detoast immediately
                                let varlena_ptr: *mut pg_sys::varlena =
                                    values[attno].cast_mut_ptr();
                                let detoasted = pg_sys::pg_detoast_datum(varlena_ptr);
                                let len = pgrx::varsize_any_exhdr(detoasted);
                                let data = pgrx::vardata_any(detoasted);
                                let bytes = std::slice::from_raw_parts(
                                    data as *const u8,
                                    len,
                                )
                                .to_vec();
                                if detoasted != varlena_ptr {
                                    pg_sys::pfree(detoasted as *mut _);
                                }
                                compressed_blobs.push(bytes);
                                toast_pointers.push(Vec::new());
                            }
                        } else {
                            compressed_blobs.push(Vec::new());
                            toast_pointers.push(Vec::new());
                        }
                    }
                    None => {
                        // Unneeded column — empty placeholder to keep blob_idx mapping
                        compressed_blobs.push(Vec::new());
                        toast_pointers.push(Vec::new());
                    }
                }
            }

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

        // End scan + close relation
        (*(*rel).rd_tableam).scan_end.unwrap()(scan);
        pg_sys::table_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

        (segments, segments_skipped, segments_minmax_skipped)
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
