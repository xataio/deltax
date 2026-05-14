use pgrx::pg_sys;

use std::cell::RefCell;
use std::collections::HashMap;

use crate::compression;
use crate::compress::{encode_f64_to_i64, encode_f32_to_i64};
use super::batch_qual::{BatchQual, BatchCompareOp, LikeStrategy, sql_like_match};
use super::datum_utils::{pg_type_oid, tupdesc_get_attr};

/// Cached colstats row for a single segment: min/max/sum/counts.
struct CachedColStatsRow {
    min_encoded: i64,
    max_encoded: i64,
    min_null: bool,
    max_null: bool,
    sum_i128: Option<i128>,  // Integer sums (e.g. "42000" parses to i128)
    sum_f64: Option<f64>,    // Float sums (e.g. "123.5" parses to f64 but not i128)
    sum_null: bool,
    nonnull_count: i64,
    nonzero_count: i64,
}

/// Cached colstats for a (colstats_oid, col_idx) pair: segment_id → row data.
struct CachedColStats {
    rows: HashMap<i32, CachedColStatsRow>,
}

thread_local! {
    /// Cache: (colstats_oid, col_idx) → CachedColStats.
    /// Invalidated via invalidate_colstats_cache() on compress/decompress.
    static COLSTATS_CACHE: RefCell<HashMap<(pg_sys::Oid, i16), CachedColStats>> =
        RefCell::new(HashMap::new());
}

pub(in crate::scan) fn invalidate_colstats_cache() {
    COLSTATS_CACHE.with(|c| c.borrow_mut().clear());
}


/// Which dict check to perform in `segment_skippable_by_dict`.
#[derive(Clone, Copy, PartialEq)]
enum DictCheck { Eq, Ne, Like, NotLike }

/// Filter for pruning segments based on min/max metadata in the normalized colstats table.
/// Built from batch quals with orderable types (int, float, timestamp, date).
pub(super) struct MinMaxFilter {
    pub(super) col_idx: i16,              // _col_idx in normalized colstats
    pub(super) op: BatchCompareOp,        // Eq, Lt, Le, Gt, Ge, InList
    pub(super) const_i64: i64,            // pre-encoded constant
    pub(super) in_list_i64: Option<Vec<i64>>,
}

/// Check whether a segment might contain rows matching the filter using encoded i64 min/max.
/// Returns `true` if the segment should be kept (may match), `false` if it can be skipped.
pub(super) fn segment_passes_minmax_filter(
    f: &MinMaxFilter,
    seg_min: i64,
    seg_max: i64,
) -> bool {
    match f.op {
        BatchCompareOp::InList => {
            if let Some(ref values) = f.in_list_i64 {
                values.iter().any(|&v| v >= seg_min && v <= seg_max)
            } else {
                true
            }
        }
        _ => {
            let c = f.const_i64;
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

/// Look up the per-partition btree index on `(_col_idx, _min, _max)` and
/// compute the set of segment_ids whose stored [_min, _max] range covers
/// every queried equality value. Returns `None` if the index isn't present
/// (older partition compressed before the index was added) or the table
/// can't be opened — caller falls back to the regular colstats scan.
///
/// `filters` is the list of `(col_idx, value_i64)` equality predicates,
/// already encoded with the same `encode_datum_to_i64` rule used to populate
/// `_min` / `_max` at compression time.
unsafe fn lookup_segments_by_minmax_index(
    colstats_oid: pg_sys::Oid,
    filters: &[(i16, i64)],
) -> Option<std::collections::HashSet<i32>> {
    if filters.is_empty() {
        return None;
    }
    unsafe {
        let cs_rel = pg_sys::table_open(
            colstats_oid,
            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
        );

        // Find the btree on (_col_idx, _min, _max). Skip the PK
        // (`indisprimary == true`, on (_col_idx, _segment_id)).
        let mut minmax_idx_oid = pg_sys::InvalidOid;
        let index_list = pg_sys::RelationGetIndexList(cs_rel);
        if !index_list.is_null() {
            let n = (*index_list).length;
            for i in 0..n {
                let idx_oid = (*(*index_list).elements.add(i as usize)).oid_value;
                let idx_rel = pg_sys::index_open(
                    idx_oid,
                    pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                );
                let info = (*idx_rel).rd_index;
                let is_target = if !info.is_null() {
                    let is_primary = (*info).indisprimary;
                    let nkeys = (*info).indnkeyatts as usize;
                    // Read the indkey attribute numbers; key 1 = _col_idx (1),
                    // key 2 = _min (3), key 3 = _max (4) — values are 1-based
                    // attnums on the colstats table.
                    if !is_primary && nkeys >= 3 {
                        let indkey =
                            (*info).indkey.values.as_ptr();
                        *indkey.add(0) == 1
                            && *indkey.add(1) == 3
                            && *indkey.add(2) == 4
                    } else {
                        false
                    }
                } else {
                    false
                };
                pg_sys::index_close(
                    idx_rel,
                    pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                );
                if is_target {
                    minmax_idx_oid = idx_oid;
                    break;
                }
            }
            pg_sys::list_free(index_list);
        }

        if minmax_idx_oid == pg_sys::InvalidOid {
            pg_sys::table_close(cs_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            return None;
        }

        // Find the _segment_id and _max attribute positions on the heap.
        let cs_tupdesc = (*cs_rel).rd_att;
        let cs_natts = (*cs_tupdesc).natts as usize;
        let mut sid_att: Option<usize> = None;
        let mut max_att: Option<usize> = None;
        for i in 0..cs_natts {
            let att = &*tupdesc_get_attr(cs_tupdesc, i);
            if att.attisdropped {
                continue;
            }
            let name = std::ffi::CStr::from_ptr(att.attname.data.as_ptr())
                .to_string_lossy();
            if name == "_segment_id" {
                sid_att = Some(i);
            } else if name == "_max" {
                max_att = Some(i);
            }
        }
        let (Some(sid_att), Some(max_att)) = (sid_att, max_att) else {
            pg_sys::table_close(cs_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            return None;
        };

        let snapshot = pg_sys::GetActiveSnapshot();
        let idx_rel = pg_sys::index_open(
            minmax_idx_oid,
            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
        );
        let slot = pg_sys::table_slot_create(cs_rel, std::ptr::null_mut());

        // Per-filter candidate set; intersect across filters at the end.
        let mut combined: Option<std::collections::HashSet<i32>> = None;

        for &(col_idx, value) in filters {
            let mut skey = [pg_sys::ScanKeyData::default(); 2];
            // _col_idx = col_idx
            pg_sys::ScanKeyInit(
                &mut skey[0],
                1,
                pg_sys::BTEqualStrategyNumber as u16,
                pg_sys::F_INT2EQ.into(),
                pg_sys::Datum::from(col_idx),
            );
            // _min <= value (BTLessEqualStrategyNumber on attnum 2 = _min)
            pg_sys::ScanKeyInit(
                &mut skey[1],
                2,
                pg_sys::BTLessEqualStrategyNumber as u16,
                pg_sys::F_INT8LE.into(),
                pg_sys::Datum::from(value as usize),
            );

            #[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
            let scan = pg_sys::index_beginscan(cs_rel, idx_rel, snapshot, 2, 0);
            #[cfg(feature = "pg18")]
            let scan = pg_sys::index_beginscan(
                cs_rel,
                idx_rel,
                snapshot,
                std::ptr::null_mut(),
                2,
                0,
            );
            pg_sys::index_rescan(scan, skey.as_mut_ptr(), 2, std::ptr::null_mut(), 0);

            let mut this: std::collections::HashSet<i32> = std::collections::HashSet::new();
            loop {
                if !pg_sys::index_getnext_slot(
                    scan,
                    pg_sys::ScanDirection::ForwardScanDirection,
                    slot,
                ) {
                    break;
                }
                pg_sys::slot_getallattrs(slot);
                let tts_values = (*slot).tts_values;
                let tts_isnull = (*slot).tts_isnull;
                if *tts_isnull.add(sid_att) || *tts_isnull.add(max_att) {
                    continue;
                }
                // Post-filter: _max >= value.
                let max_v = (*tts_values.add(max_att)).value() as i64;
                if max_v < value {
                    continue;
                }
                let seg_id = (*tts_values.add(sid_att)).value() as i32;
                this.insert(seg_id);
            }
            pg_sys::index_endscan(scan);

            combined = Some(match combined.take() {
                None => this,
                Some(prev) => prev.intersection(&this).copied().collect(),
            });
            // Early-exit if intersection is already empty.
            if combined.as_ref().is_some_and(|s| s.is_empty()) {
                break;
            }
        }

        pg_sys::ExecDropSingleTupleTableSlot(slot);
        pg_sys::index_close(idx_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        pg_sys::table_close(cs_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

        combined
    }
}

/// Encode a pg_sys::Datum to i64 for the given type OID, matching the order-preserving
/// encoding used in the colstats table.
///
/// Timestamps and dates are stored in the colstats table as Unix-epoch microseconds
/// (matching the internal TypedColumn representation), so we must convert from PG's
/// native representation (PG-epoch) when encoding filter constants.
fn encode_datum_to_i64(datum: pg_sys::Datum, type_oid: pg_sys::Oid) -> Option<i64> {
    match type_oid {
        pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID => {
            Some(datum.value() as i64)
        }
        pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID => {
            // PG stores as PG-epoch microseconds; colstats stores as Unix-epoch microseconds
            let pg_epoch_usec = datum.value() as i64;
            Some(pg_epoch_usec + crate::compress::PG_EPOCH_OFFSET_USEC)
        }
        pg_sys::DATEOID => {
            // PG stores as PG-epoch days (int32); colstats stores as Unix-epoch microseconds
            let pg_epoch_days = datum.value() as i32 as i64;
            Some((pg_epoch_days + crate::compress::PG_EPOCH_OFFSET_DAYS) * 86_400_000_000)
        }
        pg_sys::FLOAT4OID => {
            let v = f32::from_bits(datum.value() as u32);
            Some(encode_f32_to_i64(v))
        }
        pg_sys::FLOAT8OID => {
            let v = f64::from_bits(datum.value() as u64);
            Some(encode_f64_to_i64(v))
        }
        _ => None,
    }
}

/// Returns Some(true) if all rows provably satisfy the qual,
/// Some(false) if no rows satisfy (already pruned by load_segments_heap),
/// None if ambiguous (must decompress).
pub(super) fn segment_all_rows_pass(
    cm: &ColMinMax,
    op: BatchCompareOp,
    const_datum: pg_sys::Datum,
) -> Option<bool> {
    if cm.min_null || cm.max_null {
        return None;
    }

    // Encode the constant datum to i64 for comparison with stored encoded values
    let c = encode_datum_to_i64(const_datum, cm.type_oid)?;
    let seg_min = cm.min_encoded;
    let seg_max = cm.max_encoded;

    match op {
        BatchCompareOp::Eq => {
            if seg_min == c && seg_max == c { Some(true) }
            else if seg_max < c || seg_min > c { Some(false) }
            else { None }
        }
        BatchCompareOp::Ne => {
            if seg_min > c || seg_max < c { Some(true) }
            else if seg_min == c && seg_max == c { Some(false) }
            else { None }
        }
        BatchCompareOp::Gt => {
            if seg_min > c { Some(true) }
            else if seg_max <= c { Some(false) }
            else { None }
        }
        BatchCompareOp::Ge => {
            if seg_min >= c { Some(true) }
            else if seg_max < c { Some(false) }
            else { None }
        }
        BatchCompareOp::Lt => {
            if seg_max < c { Some(true) }
            else if seg_min >= c { Some(false) }
            else { None }
        }
        BatchCompareOp::Le => {
            if seg_max <= c { Some(true) }
            else if seg_min > c { Some(false) }
            else { None }
        }
        BatchCompareOp::InList | BatchCompareOp::Like | BatchCompareOp::NotLike => None,
    }
}

/// Result of classifying whether all rows in a segment satisfy all quals.
pub(super) enum SegmentQualResult {
    /// Metadata proves all rows satisfy all quals and no NULLs in qual columns.
    AllPass,
    /// Metadata proves NO rows satisfy the quals (e.g. nonzero_count == 0 with Ne 0).
    NonePass,
    /// Cannot determine from metadata — must decompress.
    Ambiguous,
}

/// Returns true if the datum is zero for the given numeric type OID.
pub(super) fn is_zero_const(datum: pg_sys::Datum, type_oid: pg_sys::Oid) -> bool {
    match type_oid {
        pg_sys::INT2OID => datum.value() as i16 == 0,
        pg_sys::INT4OID => datum.value() as i32 == 0,
        pg_sys::INT8OID => datum.value() as i64 == 0,
        pg_sys::FLOAT4OID => f32::from_bits(datum.value() as u32) == 0.0,
        pg_sys::FLOAT8OID => f64::from_bits(datum.value() as u64) == 0.0,
        _ => false,
    }
}

/// Classify a segment by **only** the numeric subset of `batch_quals` —
/// useful in the mixed text+numeric path where text quals have no
/// `col_minmax` metadata to consult. Returns `Ambiguous` if no numeric
/// quals are present (caller should fall through to per-row eval).
///
/// `NonePass` is sound: a numeric qual that rejects every row in the
/// segment also rejects the same rows under any text qual, so the
/// segment can be skipped entirely. `AllPass` here means the numeric
/// quals pass for every row — text quals may still filter; the caller
/// uses this to skip the per-row numeric `evaluate_batch_quals` step
/// while keeping the text qual application.
pub(super) fn classify_segment_quals_numeric(
    seg: &SegmentData,
    batch_quals: &[BatchQual],
    col_names: &[String],
) -> SegmentQualResult {
    use super::batch_qual::is_batch_comparable_type;
    let mut any_numeric = false;
    let mut any_nonepass = false;
    let mut any_ambiguous = false;
    for bq in batch_quals {
        if !is_batch_comparable_type(bq.type_oid) {
            continue; // skip text quals — handled per-row by caller
        }
        any_numeric = true;
        let col_name = &col_names[bq.col_idx];
        let cm = match seg.col_minmax.get(col_name) {
            Some(cm) => cm,
            None => {
                any_ambiguous = true;
                continue;
            }
        };
        match segment_all_rows_pass(cm, bq.op, bq.const_datum) {
            Some(true) => {}
            Some(false) => {
                any_nonepass = true;
            }
            None => {
                any_ambiguous = true;
            }
        }
    }
    if !any_numeric {
        return SegmentQualResult::Ambiguous;
    }
    if any_nonepass {
        return SegmentQualResult::NonePass;
    }
    if any_ambiguous {
        return SegmentQualResult::Ambiguous;
    }
    // All numeric quals pass via minmax. Check NULLs in the numeric
    // qual columns: minmax covers only non-NULL values.
    for bq in batch_quals {
        if !is_batch_comparable_type(bq.type_oid) {
            continue;
        }
        let col_name = &col_names[bq.col_idx];
        match seg.col_sums.get(col_name) {
            Some(cs) => {
                if cs.nonnull_count < seg.row_count as i64 {
                    return SegmentQualResult::Ambiguous;
                }
            }
            None => return SegmentQualResult::Ambiguous,
        }
    }
    SegmentQualResult::AllPass
}

/// Classify a segment: can we prove all rows pass all batch quals using metadata?
pub(super) fn classify_segment_quals(
    seg: &SegmentData,
    batch_quals: &[BatchQual],
    col_names: &[String],
) -> SegmentQualResult {
    let mut any_nonepass = false;
    for bq in batch_quals {
        let col_name = &col_names[bq.col_idx];
        let cm = match seg.col_minmax.get(col_name) {
            Some(cm) => cm,
            None => return SegmentQualResult::Ambiguous,
        };
        match segment_all_rows_pass(cm, bq.op, bq.const_datum) {
            Some(true) => {} // this qual is satisfied for all rows
            Some(false) => return SegmentQualResult::NonePass,
            None => {
                // minmax couldn't resolve — try nonzero_count for Ne/Eq with 0
                if is_zero_const(bq.const_datum, bq.type_oid)
                    && let Some(cs) = seg.col_sums.get(col_name)
                    && cs.nonzero_count >= 0 && cs.nonnull_count == seg.row_count as i64
                {
                    match bq.op {
                        BatchCompareOp::Ne if cs.nonzero_count == 0 => {
                            // All values are zero → Ne 0 passes for no rows
                            any_nonepass = true;
                            continue;
                        }
                        BatchCompareOp::Eq if cs.nonzero_count == cs.nonnull_count => {
                            // All values are nonzero → Eq 0 passes for no rows
                            any_nonepass = true;
                            continue;
                        }
                        _ => {}
                    }
                }
                return SegmentQualResult::Ambiguous;
            }
        }
    }
    if any_nonepass {
        return SegmentQualResult::NonePass;
    }
    // All quals passed via minmax. Now check for NULLs in qual columns:
    // min/max covers only non-NULL values, so if NULLs exist, we can't trust row_count.
    for bq in batch_quals {
        let col_name = &col_names[bq.col_idx];
        match seg.col_sums.get(col_name) {
            Some(cs) => {
                if cs.nonnull_count < seg.row_count as i64 {
                    return SegmentQualResult::Ambiguous;
                }
            }
            None => return SegmentQualResult::Ambiguous,
        }
    }
    SegmentQualResult::AllPass
}

/// Per-column min/max metadata from the companion table, stored as order-preserving i64 encodings.
pub(super) struct ColMinMax {
    pub(super) min_encoded: i64,
    pub(super) max_encoded: i64,
    pub(super) min_null: bool,
    pub(super) max_null: bool,
    pub(super) type_oid: pg_sys::Oid,
}

/// Per-column sum metadata from the companion table.
#[allow(dead_code)]
pub(super) struct ColSum {
    pub(super) sum_datum: pg_sys::Datum,
    pub(super) sum_null: bool,
    pub(super) sum_i128: Option<i128>,  // Cached/pre-converted integer sum
    pub(super) sum_f64: Option<f64>,    // Cached/pre-converted float sum (when i128 parse fails)
    pub(super) nonnull_count: i64,
    pub(super) nonzero_count: i64,  // -1 = unavailable (column missing in older meta tables)
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
    compressed_blobs: &[BlobBytes],
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

/// One per-column compressed blob stored in `SegmentData`. Lets cache
/// hits skip the `to_vec()` copy: instead of materialising the cached
/// bytes into a backend-heap `Vec<u8>`, `Cached` keeps a raw pointer into
/// the DSA-backed `BlobCachePin` allocation. The corresponding pin lives
/// in `SegmentData::cached_blob_pins`, which is declared AFTER
/// `compressed_blobs` so Rust drops `compressed_blobs` first — the raw
/// pointers go out of scope before the pins release the entry.
///
/// `Deref<Target = [u8]>` so existing consumer code that takes `&[u8]`
/// keeps working without changes.
pub(crate) enum BlobBytes {
    Owned(Vec<u8>),
    /// Borrowed bytes from the blob cache. Valid for the lifetime of
    /// the surrounding `SegmentData` (i.e. until the matching pin in
    /// `cached_blob_pins` drops).
    Cached { data: *const u8, len: u32 },
}

impl Default for BlobBytes {
    fn default() -> Self {
        Self::Owned(Vec::new())
    }
}

impl std::ops::Deref for BlobBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            BlobBytes::Owned(v) => v.as_slice(),
            BlobBytes::Cached { data, len } => unsafe {
                std::slice::from_raw_parts(*data, *len as usize)
            },
        }
    }
}

// SAFETY: Cached's raw pointer references DSA shared memory whose
// lifetime is guaranteed by the matching `BlobCachePin` in the same
// `SegmentData`. The pin uses atomic pin_count to keep the entry
// resident across all readers and prevents eviction. The bytes are
// only ever read, never written.
unsafe impl Send for BlobBytes {}
unsafe impl Sync for BlobBytes {}

pub(super) struct SegmentData {
    /// Source companion-table OID. Populated by the caller after
    /// `load_segments_heap` returns; used by `fetch_segment_blobs` to re-open
    /// the right `_blobs` table when blobs are materialised on-claim.
    pub(super) companion_oid: pg_sys::Oid,
    /// Companion-table segment id (used to fetch sidecar/bloom data after
    /// the main load).
    pub(super) segment_id: i32,
    pub(super) segment_values: Vec<Option<String>>,
    pub(super) compressed_blobs: Vec<BlobBytes>,
    /// Per-text-column length sidecar blobs (parallel to compressed_blobs).
    /// Non-empty when the planner has marked a text column as sidecar-only;
    /// holds the compressed u32-per-row length array instead of the main blob.
    pub(super) text_length_blobs: Vec<Vec<u8>>,
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
    /// Pins for blobs served from the shared blob cache. Holding these pins
    /// guarantees the underlying DSA-backed bytes outlive every read of
    /// `compressed_blobs` (including parallel-worker reads, since detoast
    /// runs on the leader before worker dispatch and segments are owned by
    /// the leader's `DecompressState`). Released automatically on drop.
    pub(super) cached_blob_pins: Vec<crate::blob_cache::BlobCachePin>,
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
    pub(super) col_not_null: Vec<bool>,
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
            "SELECT h.segment_by, h.order_by, h.time_column, h.schema_name, h.table_name,
                    h.json_extract
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
    let json_extract: Option<serde_json::Value> = ht_row
        .get_datum_by_ordinal(6)
        .unwrap()
        .value::<pgrx::datum::JsonB>()
        .unwrap()
        .map(|j| j.0);

    // Get column info from the parent table (pg_attribute gives us atttypmod)
    let col_result = client
        .select(
            "SELECT a.attname::text, t.typname::text, a.atttypmod, a.attnotnull
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
    let mut col_not_null = Vec::new();
    for row in col_result {
        let name: String = row.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap().unwrap();
        let type_name: String = row.get_datum_by_ordinal(2).unwrap().value::<String>().unwrap().unwrap();
        let typmod: i32 = row.get_datum_by_ordinal(3).unwrap().value::<i32>().unwrap().unwrap_or(-1);
        let not_null: bool = row.get_datum_by_ordinal(4).unwrap().value::<bool>().unwrap().unwrap_or(false);
        col_names.push(name);
        col_type_names.push(type_name);
        col_typmods.push(typmod);
        col_not_null.push(not_null);
    }

    let mut col_types: Vec<pg_sys::Oid> = col_type_names.iter().map(|tn| pg_type_oid(tn)).collect();

    // Append synthetic columns from json_extract (in spec order). These map
    // 1-to-1 with the extracted ColumnMeta entries that were appended at
    // compress time, so their `_col_idx` slots are physical_count + i. The
    // executor uses col_names/col_types indexed by `_col_idx`, so they need
    // to be visible here too.
    if let Some(jx) = json_extract {
        let specs = crate::compress::parse_extract_specs(&jx);
        for spec in specs {
            col_names.push(spec.target_name.clone());
            col_types.push(crate::scan::json_extract::kind_to_type_oid(spec.target_kind));
            col_typmods.push(-1);
        }
    }

    MetadataInfo {
        col_names,
        col_types,
        col_typmods,
        col_not_null,
        segment_by,
        order_by,
        time_column,
    }
}

/// Per-phase shared-buffer counters captured from `pgBufferUsage` deltas,
/// so EXPLAIN (ANALYZE, BUFFERS) can distinguish where I/O happened
/// (meta vs bloom pruning vs blob detoast) — custom-scan work runs in
/// BeginCustomScan, outside PG's own node-level instrumentation.
#[derive(Default, Clone, Copy)]
pub(crate) struct ScanBufferStats {
    pub(crate) meta_hit: i64,
    pub(crate) meta_read: i64,
    pub(crate) bloom_hit: i64,
    pub(crate) bloom_read: i64,
    pub(crate) blob_hit: i64,
    pub(crate) blob_read: i64,
}

impl ScanBufferStats {
    fn accumulate(&mut self, other: &ScanBufferStats) {
        self.meta_hit += other.meta_hit;
        self.meta_read += other.meta_read;
        self.bloom_hit += other.bloom_hit;
        self.bloom_read += other.bloom_read;
        self.blob_hit += other.blob_hit;
        self.blob_read += other.blob_read;
    }
}

// Thread-local accumulator for buffer stats produced by `load_segments_heap`.
// Callers reset via `reset_scan_buf_stats()` at the start of a BeginCustomScan
// callback and read the accumulated value via `take_scan_buf_stats()` before
// stashing it in their state struct. This avoids threading a parameter through
// the many agg/count/minmax fast-path helpers that construct state.
thread_local! {
    static LAST_SCAN_BUF_STATS: std::cell::Cell<ScanBufferStats> =
        const { std::cell::Cell::new(ScanBufferStats {
            meta_hit: 0, meta_read: 0,
            bloom_hit: 0, bloom_read: 0,
            blob_hit: 0, blob_read: 0,
        }) };
}

pub(crate) fn reset_scan_buf_stats() {
    LAST_SCAN_BUF_STATS.with(|c| c.set(ScanBufferStats::default()));
}

pub(crate) fn take_scan_buf_stats() -> ScanBufferStats {
    LAST_SCAN_BUF_STATS.with(|c| c.replace(ScanBufferStats::default()))
}

fn accumulate_scan_buf_stats(delta: &ScanBufferStats) {
    LAST_SCAN_BUF_STATS.with(|c| {
        let mut cur = c.get();
        cur.accumulate(delta);
        c.set(cur);
    });
}

/// Snapshot `(shared_blks_hit, shared_blks_read)` from the global
/// `pgBufferUsage` counter. Used to compute per-phase deltas in
/// `load_segments_heap`.
#[inline]
unsafe fn shared_buf_snapshot() -> (i64, i64) {
    unsafe {
        let bu = std::ptr::addr_of!(pg_sys::pgBufferUsage);
        ((*bu).shared_blks_hit, (*bu).shared_blks_read)
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
    needed_stats_cols: &[String],
    col_types: &[pg_sys::Oid],
    needed_minmax_cols: &[String],
    // `skip_blob_load = true` skips Phase 2 entirely. Callers that fetch blobs
    // on-claim via `fetch_segment_blobs` pass true — compressed_blobs and
    // toast_pointers stay empty at return.
    skip_blob_load: bool,
) -> (Vec<SegmentData>, u64, u64, u64, u64, u64) {
    // Returns: (segments, total_skipped, minmax_skipped, bloom_skipped,
    // valbitmap_skipped, detoast_us). Segment-level pruning counters are
    // additive: `total_skipped` = sum of every reason we dropped a segment.
    // Buffer stats are accumulated into a thread-local via `accumulate_scan_buf_stats`;
    // callers read them with `take_scan_buf_stats()` after all companion OIDs are processed.
    unsafe {
        let mut buf_stats = ScanBufferStats::default();
        let (t0_hit, t0_read) = shared_buf_snapshot();

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

        // Discover per-column sum/nonnull_count/nonzero_count columns
        let load_sums = !needed_stats_cols.is_empty();
        let mut sum_col_attnos: Vec<(String, usize, usize, Option<usize>, pg_sys::Oid)> = Vec::new();
        if load_sums {
            for col_name in col_names {
                if segment_by.contains(col_name) {
                    continue;
                }
                let sum_name = format!("_sum_{}", col_name);
                let nonnull_name = format!("_nonnull_count_{}", col_name);
                let nonzero_name = format!("_nonzero_count_{}", col_name);
                if let (Some(&sum_att), Some(&nn_att)) = (
                    attno_map.get(sum_name.as_str()),
                    attno_map.get(nonnull_name.as_str()),
                ) {
                    let nz_att = attno_map.get(nonzero_name.as_str()).copied();
                    let type_oid = att_type_oids.get(sum_name.as_str()).copied()
                        .unwrap_or(pg_sys::InvalidOid);
                    sum_col_attnos.push((col_name.clone(), sum_att, nn_att, nz_att, type_oid));
                }
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

        // Build bloom filter checks from batch quals (Eq and InList on numeric types)
        struct BloomCheck {
            col_idx: u16,
            hashes: Vec<u64>,
        }
        // Build valbitmap checks from batch quals (text Eq on low-card columns
        // whose partition-level value list is in `column_valmap`). Each check
        // carries the bit indices the segment must contain at least one of.
        // `prune_all = true` means the queried constant doesn't appear in
        // ANY segment of this partition — every segment can be skipped without
        // even reading the bitmap table.
        struct ValbitmapCheck {
            col_idx: u16,
            wanted_bits: Vec<u8>,
            prune_all: bool,
        }
        let mut bloom_checks: Vec<BloomCheck> = Vec::new();
        let mut valbitmap_checks: Vec<ValbitmapCheck> = Vec::new();
        let valmap = crate::scan::cost::get_column_valmap(meta_oid);
        for bq in batch_quals {
            match bq.op {
                BatchCompareOp::Eq | BatchCompareOp::InList => {}
                _ => continue,
            }
            let col_name = &col_names[bq.col_idx];
            if segment_by.contains(col_name) {
                continue;
            }
            let ci = match col_idx_map[bq.col_idx] {
                Some(ci) => ci,
                None => continue,
            };

            // Numeric / temporal types → bloom (existing path).
            let is_numeric_type = matches!(
                bq.type_oid,
                pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID
                | pg_sys::FLOAT4OID | pg_sys::FLOAT8OID
                | pg_sys::DATEOID | pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID
            );
            if is_numeric_type {
                let hashes = if bq.op == BatchCompareOp::InList {
                    if let Some(ref vals) = bq.in_list_i64 {
                        vals.iter().map(|&v| crate::bloom::hash_datum_i64(v)).collect()
                    } else {
                        continue;
                    }
                } else {
                    let val_i64 = match bq.type_oid {
                        pg_sys::FLOAT4OID => (bq.const_datum.value() as u32) as i64,
                        pg_sys::FLOAT8OID => bq.const_datum.value() as i64,
                        _ => bq.const_datum.value() as i64,
                    };
                    vec![crate::bloom::hash_datum_i64(val_i64)]
                };
                bloom_checks.push(BloomCheck { col_idx: ci, hashes });
                continue;
            }

            // Text Eq on a column with a partition-level valmap → exact bitmap
            // pruning. InList not yet supported for valbitmap (would need
            // text_const_list on BatchQual; not in the struct today).
            if bq.op == BatchCompareOp::Eq
                && super::batch_qual::is_text_type(bq.type_oid)
                && let Some(ref needle) = bq.text_const
                && let Some(values) = valmap.get(col_name)
            {
                let bit = values.iter().position(|v| v == needle);
                match bit {
                    Some(idx) => {
                        valbitmap_checks.push(ValbitmapCheck {
                            col_idx: ci,
                            wanted_bits: vec![idx as u8],
                            prune_all: false,
                        });
                    }
                    None => {
                        // Constant never appeared at compress time → no segment
                        // can match. Mark the column for "prune everything".
                        valbitmap_checks.push(ValbitmapCheck {
                            col_idx: ci,
                            wanted_bits: vec![],
                            prune_all: true,
                        });
                    }
                }
            }
        }
        let mut segments_bloom_skipped: u64 = 0;
        let mut segments_valbitmap_skipped: u64 = 0;

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

            // --- Segment survived time/segment_by pruning ---

            // Extract per-column min/max (time column from meta — identity encoding for timestamp/date)
            let mut col_minmax = HashMap::new();
            for (col_name, min_att, max_att, type_oid) in &minmax_col_attnos {
                let min_null = nulls[*min_att];
                let max_null = nulls[*max_att];
                let min_enc = if min_null { 0i64 } else { values[*min_att].value() as i64 };
                let max_enc = if max_null { 0i64 } else { values[*max_att].value() as i64 };
                col_minmax.insert(col_name.clone(), ColMinMax {
                    min_encoded: min_enc,
                    max_encoded: max_enc,
                    min_null,
                    max_null,
                    type_oid: *type_oid,
                });
            }

            // Also populate time column minmax when requested by caller
            // (e.g. DeltaXMinMax on the time column) — avoids colstats scan.
            // Must encode PG-epoch datum → Unix-epoch i64 to match colstats encoding.
            if needed_minmax_cols.iter().any(|n| n == time_column) && !col_minmax.contains_key(time_column)
                && let (Some(min_att), Some(max_att)) = (min_time_attno, max_time_attno)
            {
                let min_null = nulls[min_att];
                let max_null = nulls[max_att];
                let time_type_oid = att_type_oids.get(format!("_min_{}", time_column).as_str())
                    .copied()
                    .unwrap_or(pg_sys::TIMESTAMPTZOID);
                let encode_time = |raw: i64| -> i64 {
                    match time_type_oid {
                        pg_sys::DATEOID => {
                            // raw is PG-epoch days → Unix-epoch microseconds
                            (raw + crate::compress::PG_EPOCH_OFFSET_DAYS) * 86_400_000_000
                        }
                        pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID => {
                            // raw is PG-epoch usec → Unix-epoch usec
                            raw + crate::compress::PG_EPOCH_OFFSET_USEC
                        }
                        _ => raw,
                    }
                };
                let min_enc = if min_null { 0i64 } else { encode_time(values[min_att].value() as i64) };
                let max_enc = if max_null { 0i64 } else { encode_time(values[max_att].value() as i64) };
                col_minmax.insert(time_column.to_string(), ColMinMax {
                    min_encoded: min_enc,
                    max_encoded: max_enc,
                    min_null,
                    max_null,
                    type_oid: time_type_oid,
                });
            }

            // Extract per-column sum/nonnull_count/nonzero_count
            let mut col_sums = HashMap::new();
            for (col_name, sum_att, nn_att, nz_att, type_oid) in &sum_col_attnos {
                let sum_null = nulls[*sum_att];
                let sum_datum = if sum_null { pg_sys::Datum::from(0usize) } else { values[*sum_att] };
                let nonnull_count = if nulls[*nn_att] { 0i64 } else { values[*nn_att].value() as i64 };
                let nonzero_count = match nz_att {
                    Some(att) => if nulls[*att] { -1i64 } else { values[*att].value() as i64 },
                    None => -1i64,  // column missing in older meta tables
                };
                col_sums.insert(col_name.clone(), ColSum {
                    sum_datum,
                    sum_null,
                    sum_i128: None,
                    sum_f64: None,
                    nonnull_count,
                    nonzero_count,
                    type_oid: *type_oid,
                });
            }

            // Pre-allocate empty blob slots — will be filled in Phase 2.
            // resize_with avoids requiring BlobBytes: Clone (it isn't).
            let mut compressed_blobs: Vec<BlobBytes> = Vec::with_capacity(num_blob_cols);
            compressed_blobs.resize_with(num_blob_cols, BlobBytes::default);
            let text_length_blobs: Vec<Vec<u8>> = vec![Vec::new(); num_blob_cols];
            let toast_pointers: Vec<Vec<u8>> = vec![Vec::new(); num_blob_cols];

            surviving_segment_ids.push(segment_id);
            segments.push(SegmentData {
                companion_oid: meta_oid,
                segment_id,
                segment_values,
                compressed_blobs,
                text_length_blobs,
                row_count,
                min_time: seg_min_time,
                max_time: seg_max_time,
                col_minmax,
                col_sums,
                toast_pointers,
                cached_blob_pins: Vec::new(),
            });
        }

        // End meta scan
        (*(*rel).rd_tableam).scan_end.unwrap()(scan);
        pg_sys::table_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

        let (t1_hit, t1_read) = shared_buf_snapshot();
        buf_stats.meta_hit = t1_hit - t0_hit;
        buf_stats.meta_read = t1_read - t0_read;

        // ================================================================
        // Phase 1b: Scan normalized colstats table for per-column stats
        // Only opened when we need non-time column stats and have surviving segments.
        // ================================================================
        // Build set of column names that already have minmax in the meta table.
        // Always include the time column — its min/max is loaded from meta
        // regardless of `load_minmax`.
        let mut meta_minmax_names: std::collections::HashSet<&str> = minmax_col_attnos
            .iter()
            .map(|(name, ..)| name.as_str())
            .collect();
        if min_time_attno.is_some() && max_time_attno.is_some() {
            meta_minmax_names.insert(time_column);
        }

        let need_colstats = !segments.is_empty() && (
            // Need sum data that's not in meta?
            (load_sums && sum_col_attnos.is_empty())
            // Caller needs minmax for specific columns not already in meta?
            || (!needed_minmax_cols.is_empty()
                && needed_minmax_cols.iter().any(|n| !meta_minmax_names.contains(n.as_str())))
            // Have batch quals on non-time orderable columns not covered by meta?
            || batch_quals.iter().any(|bq| {
                !matches!(bq.op, BatchCompareOp::Like | BatchCompareOp::NotLike)
                && matches!(bq.type_oid,
                    pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID
                    | pg_sys::FLOAT4OID | pg_sys::FLOAT8OID
                    | pg_sys::DATEOID | pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID)
                && {
                    let col_name = &col_names[bq.col_idx];
                    let min_name = format!("_min_{}", col_name);
                    !attno_map.contains_key(min_name.as_str())
                }
            })
        );

        if need_colstats {
            let meta_name_ptr = pg_sys::get_rel_name(meta_oid);
            let meta_name_str = std::ffi::CStr::from_ptr(meta_name_ptr)
                .to_string_lossy()
                .into_owned();
            let meta_ns_oid = pg_sys::get_rel_namespace(meta_oid);
            let partition_name = meta_name_str.strip_suffix("_meta").unwrap_or(&meta_name_str);
            let colstats_name = format!("{}_colstats", partition_name);
            let colstats_cname = std::ffi::CString::new(colstats_name).unwrap();
            let colstats_oid = pg_sys::get_relname_relid(colstats_cname.as_ptr(), meta_ns_oid);

            if colstats_oid != pg_sys::InvalidOid {
                // Build col_idx -> (column_name, original_type_oid) mapping
                // (non-segment-by columns, 0-based, same order as blob table)
                let mut idx_to_col: Vec<(String, pg_sys::Oid)> = Vec::new();
                let mut col_to_idx: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
                for (i, name) in col_names.iter().enumerate() {
                    if !segment_by.contains(name) {
                        let ci = idx_to_col.len();
                        idx_to_col.push((name.clone(), col_types[i]));
                        col_to_idx.insert(name.as_str(), ci);
                    }
                }

                // Build surviving segment_id -> index mapping
                let mut seg_id_to_idx: HashMap<i32, usize> = HashMap::new();
                for (idx, &sid) in surviving_segment_ids.iter().enumerate() {
                    seg_id_to_idx.insert(sid, idx);
                }

                // Build minmax filters for colstats (batch quals on non-time orderable columns)
                let mut cs_minmax_filters: Vec<MinMaxFilter> = Vec::new();
                for bq in batch_quals {
                    match bq.op {
                        BatchCompareOp::Like | BatchCompareOp::NotLike => continue,
                        _ => {}
                    }
                    let col_name = &col_names[bq.col_idx];
                    let min_name = format!("_min_{}", col_name);
                    if attno_map.contains_key(min_name.as_str()) { continue; } // already in meta
                    if segment_by.contains(col_name) { continue; }

                    let ci = match col_idx_map[bq.col_idx] {
                        Some(ci) => ci as i16,
                        None => continue,
                    };

                    let const_i64 = match encode_datum_to_i64(bq.const_datum, bq.type_oid) {
                        Some(v) => v,
                        None => continue,
                    };

                    // For float in-list, re-encode from raw datum bits to order-preserving i64
                    let encoded_in_list = bq.in_list_i64.as_ref().map(|vals| {
                        vals.iter().map(|&v| {
                            match bq.type_oid {
                                pg_sys::FLOAT4OID => encode_f32_to_i64(f32::from_bits(v as u32)),
                                pg_sys::FLOAT8OID => encode_f64_to_i64(f64::from_bits(v as u64)),
                                _ => v, // int/timestamp/date: identity
                            }
                        }).collect()
                    });

                    cs_minmax_filters.push(MinMaxFilter {
                        col_idx: ci,
                        op: bq.op,
                        const_i64,
                        in_list_i64: encoded_in_list,
                    });
                }

                // Collect the set of _col_idx values we actually need:
                // - minmax filter columns (from batch quals)
                // - columns caller needs minmax for (needed_minmax_cols)
                // - columns caller needs stats for (needed_stats_cols)
                let mut needed_col_idxs: std::collections::HashSet<i16> = std::collections::HashSet::new();
                for f in &cs_minmax_filters {
                    needed_col_idxs.insert(f.col_idx);
                }
                for name in needed_minmax_cols {
                    if let Some(&ci) = col_to_idx.get(name.as_str()) {
                        needed_col_idxs.insert(ci as i16);
                    }
                }
                for name in needed_stats_cols {
                    if let Some(&ci) = col_to_idx.get(name.as_str()) {
                        needed_col_idxs.insert(ci as i16);
                    }
                }

                let mut cs_pruned_ids: std::collections::HashSet<i32> = std::collections::HashSet::new();

                // Check colstats cache — populate segments from cached data and
                // remove fully-cached col_idxs so we skip scanning them.
                COLSTATS_CACHE.with(|cache| {
                    let cache = cache.borrow();
                    let mut cached_idxs: Vec<i16> = Vec::new();
                    for &ci in &needed_col_idxs {
                        if let Some(cached) = cache.get(&(colstats_oid, ci)) {
                            let (ref col_name, orig_type_oid) = idx_to_col[ci as usize];
                            let mut all_found = true;
                            for (&sid, &seg_idx) in &seg_id_to_idx {
                                if let Some(row) = cached.rows.get(&sid) {
                                    // Apply minmax filters from cache
                                    if !cs_minmax_filters.is_empty() && !cs_pruned_ids.contains(&sid) {
                                        let mut skip = false;
                                        for f in &cs_minmax_filters {
                                            if f.col_idx == ci && !row.min_null && !row.max_null
                                                && !segment_passes_minmax_filter(f, row.min_encoded, row.max_encoded)
                                            {
                                                skip = true;
                                                break;
                                            }
                                        }
                                        if skip {
                                            cs_pruned_ids.insert(sid);
                                            segments_minmax_skipped += 1;
                                            continue;
                                        }
                                    }
                                    if load_minmax {
                                        segments[seg_idx].col_minmax.insert(col_name.clone(), ColMinMax {
                                            min_encoded: row.min_encoded,
                                            max_encoded: row.max_encoded,
                                            min_null: row.min_null,
                                            max_null: row.max_null,
                                            type_oid: orig_type_oid,
                                        });
                                    }
                                    if load_sums {
                                        segments[seg_idx].col_sums.insert(col_name.clone(), ColSum {
                                            sum_datum: pg_sys::Datum::from(0usize),
                                            sum_null: row.sum_null,
                                            sum_i128: row.sum_i128,
                                            sum_f64: row.sum_f64,
                                            nonnull_count: row.nonnull_count,
                                            nonzero_count: row.nonzero_count,
                                            type_oid: pg_sys::NUMERICOID,
                                        });
                                    }
                                } else {
                                    all_found = false;
                                }
                            }
                            if all_found {
                                cached_idxs.push(ci);
                            }
                        }
                    }
                    for ci in cached_idxs {
                        needed_col_idxs.remove(&ci);
                    }
                });

                // If all needed col_idxs were served from cache, skip opening colstats table
                COLSTATS_CACHE.with(|cache| {
                    let cache = cache.borrow();
                    let cache_size = cache.len();
                    let has_oid = cache.keys().any(|&(oid, _)| oid == colstats_oid);
                    pgrx::log!(
                        "colstats_cache: oid={:?} remaining_uncached={} cache_entries={} has_oid={}",
                        colstats_oid,
                        needed_col_idxs.len(),
                        cache_size,
                        has_oid,
                    );
                });

                // Indexed minmax pruning: when every column we need from
                // colstats is the target of an equality minmax filter (the
                // common point-lookup shape), use the per-partition btree on
                // `(_col_idx, _min, _max)` to compute the surviving seg_ids
                // directly. Skips iterating ~all colstats rows on the slow
                // PK-scan path (heap_scan: ~30 ms → ~1 ms for queries like
                // `WHERE order_id = N`). Mirrors TimescaleDB's
                // `compress_hyper_*__ts_meta_min_*__ts_meta_max_*__t_idx`.
                let eq_filter_cols: Vec<(i16, i64)> = cs_minmax_filters
                    .iter()
                    .filter(|f| matches!(f.op, BatchCompareOp::Eq))
                    .map(|f| (f.col_idx, f.const_i64))
                    .collect();
                let all_needed_are_eq_filters = !eq_filter_cols.is_empty()
                    && needed_col_idxs.len() == eq_filter_cols.len()
                    && eq_filter_cols
                        .iter()
                        .all(|(ci, _)| needed_col_idxs.contains(ci));
                if all_needed_are_eq_filters
                    && let Some(survivors) = lookup_segments_by_minmax_index(
                        colstats_oid,
                        &eq_filter_cols,
                    )
                {
                    // Mark every seg_id NOT in the survivor set as pruned.
                    for &sid in &surviving_segment_ids {
                        if !cs_pruned_ids.contains(&sid)
                            && !survivors.contains(&sid)
                        {
                            cs_pruned_ids.insert(sid);
                            segments_minmax_skipped += 1;
                        }
                    }
                    // Bypass the colstats heap scan — we already have every
                    // seg_id we need, and the caller didn't ask for cached
                    // min/max or sums (load_minmax false + empty
                    // needed_minmax_cols / needed_stats_cols is implied by
                    // needed_col_idxs == filter cols).
                    needed_col_idxs.clear();
                }

                if needed_col_idxs.is_empty() {
                    // Remove colstats-pruned segments
                    if !cs_pruned_ids.is_empty() {
                        let mut i = 0;
                        while i < segments.len() {
                            if cs_pruned_ids.contains(&surviving_segment_ids[i]) {
                                segments.swap_remove(i);
                                surviving_segment_ids.swap_remove(i);
                                segments_skipped += 1;
                            } else {
                                i += 1;
                            }
                        }
                    }
                } else {

                // Open normalized colstats table and locate fixed columns
                let cs_rel = pg_sys::table_open(colstats_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
                let cs_tupdesc = (*cs_rel).rd_att;
                let cs_natts = (*cs_tupdesc).natts as usize;

                let mut cs_col_idx_att: Option<usize> = None;
                let mut cs_seg_id_att: Option<usize> = None;
                let mut cs_min_att: Option<usize> = None;
                let mut cs_max_att: Option<usize> = None;
                let mut cs_sum_att: Option<usize> = None;
                let mut cs_nonnull_att: Option<usize> = None;
                let mut cs_nonzero_att: Option<usize> = None;
                let mut cs_ndistinct_att: Option<usize> = None;
                for i in 0..cs_natts {
                    let att = &*tupdesc_get_attr(cs_tupdesc, i);
                    if att.attisdropped { continue; }
                    let name = std::ffi::CStr::from_ptr(att.attname.data.as_ptr()).to_string_lossy();
                    match name.as_ref() {
                        "_col_idx" => cs_col_idx_att = Some(i),
                        "_segment_id" => cs_seg_id_att = Some(i),
                        "_min" => cs_min_att = Some(i),
                        "_max" => cs_max_att = Some(i),
                        "_sum" => cs_sum_att = Some(i),
                        "_nonnull_count" => cs_nonnull_att = Some(i),
                        "_nonzero_count" => cs_nonzero_att = Some(i),
                        "_ndistinct" => cs_ndistinct_att = Some(i),
                        _ => {}
                    }
                }

                // Decide: index scan (few columns) vs seq scan (many columns).
                // Index scan reads only needed col_idx rows via PK (_col_idx, _segment_id).
                // Threshold: use index scan if < 50% of columns needed.
                let use_index_scan = needed_col_idxs.len() < idx_to_col.len() / 2 + 1
                    || needed_col_idxs.len() <= 4;

                // Find PK index OID for index scan path
                let pk_index_oid = if use_index_scan {
                    let mut pk_oid = pg_sys::InvalidOid;
                    let index_list = pg_sys::RelationGetIndexList(cs_rel);
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
                } else {
                    pg_sys::InvalidOid
                };

                // Accumulate raw colstats rows into a per-(col_idx, segment_id) map
                // for cache population, independent of pruning decisions.
                let mut cs_raw_rows: HashMap<(i16, i32), CachedColStatsRow> = HashMap::new();

                // Helper closure: process one colstats row from slot values/nulls
                macro_rules! process_colstats_row {
                    ($vals:expr, $nls:expr, $ci_att:expr, $sid_att:expr,
                     $min_att:expr, $max_att:expr, $sum_att:expr, $nn_att:expr, $nz_att:expr) => {{
                        let seg_id = if !$nls[$sid_att] { $vals[$sid_att].value() as i32 } else { continue; };
                        let seg_idx = match seg_id_to_idx.get(&seg_id) {
                            Some(&idx) => idx,
                            None => continue,
                        };

                        let col_idx_val = if !$nls[$ci_att] { $vals[$ci_att].value() as i16 } else { continue; };
                        if col_idx_val < 0 || col_idx_val as usize >= idx_to_col.len() { continue; }
                        let (ref col_name, orig_type_oid) = idx_to_col[col_idx_val as usize];

                        let min_null = $nls[$min_att];
                        let max_null = $nls[$max_att];
                        let min_enc = if min_null { 0i64 } else { $vals[$min_att].value() as i64 };
                        let max_enc = if max_null { 0i64 } else { $vals[$max_att].value() as i64 };

                        // Extract sum data for both segment population and cache
                        let sum_null = $nls[$sum_att];
                        let sum_datum = if sum_null { pg_sys::Datum::from(0usize) } else { $vals[$sum_att] };
                        let nonnull_count = if $nls[$nn_att] { 0i64 } else { $vals[$nn_att].value() as i64 };
                        let nonzero_count = if $nls[$nz_att] { -1i64 } else { $vals[$nz_att].value() as i64 };

                        // Convert NUMERIC sum to i128/f64 at scan time for caching
                        let (sum_i128, sum_f64): (Option<i128>, Option<f64>) = if sum_null {
                            (None, None)
                        } else {
                            let cstr = pg_sys::OidOutputFunctionCall(
                                pg_sys::Oid::from(1702u32), // numeric_out
                                sum_datum,
                            );
                            let s = std::ffi::CStr::from_ptr(cstr)
                                .to_string_lossy();
                            let i = s.parse::<i128>().ok();
                            let f = if i.is_none() { s.parse::<f64>().ok() } else { None };
                            pg_sys::pfree(cstr as *mut _);
                            (i, f)
                        };

                        // Store raw row for cache population (before pruning)
                        cs_raw_rows.insert((col_idx_val, seg_id), CachedColStatsRow {
                            min_encoded: min_enc,
                            max_encoded: max_enc,
                            min_null,
                            max_null,
                            sum_i128,
                            sum_f64,
                            sum_null,
                            nonnull_count,
                            nonzero_count,
                        });

                        // Apply pruning
                        if cs_pruned_ids.contains(&seg_id) { continue; }

                        if !cs_minmax_filters.is_empty() {
                            let mut skip = false;
                            for f in &cs_minmax_filters {
                                if f.col_idx == col_idx_val && !min_null && !max_null
                                    && !segment_passes_minmax_filter(f, min_enc, max_enc)
                                {
                                    skip = true;
                                    break;
                                }
                            }
                            if skip {
                                cs_pruned_ids.insert(seg_id);
                                segments_minmax_skipped += 1;
                                continue;
                            }
                        }

                        if load_minmax {
                            segments[seg_idx].col_minmax.insert(col_name.clone(), ColMinMax {
                                min_encoded: min_enc,
                                max_encoded: max_enc,
                                min_null,
                                max_null,
                                type_oid: orig_type_oid,
                            });
                        }

                        if load_sums {
                            let sum_type_oid = if !sum_null {
                                let sum_attr = &*tupdesc_get_attr(cs_tupdesc, $sum_att);
                                sum_attr.atttypid
                            } else {
                                pg_sys::NUMERICOID
                            };
                            segments[seg_idx].col_sums.insert(col_name.clone(), ColSum {
                                sum_datum,
                                sum_null,
                                sum_i128,
                                sum_f64,
                                nonnull_count,
                                nonzero_count,
                                type_oid: sum_type_oid,
                            });
                        }
                    }};
                }

                if let (Some(ci_att), Some(sid_att), Some(min_att), Some(max_att),
                        Some(sum_att), Some(nn_att), Some(nz_att), Some(_nd_att)) =
                    (cs_col_idx_att, cs_seg_id_att, cs_min_att, cs_max_att,
                     cs_sum_att, cs_nonnull_att, cs_nonzero_att, cs_ndistinct_att)
                {
                    if use_index_scan && pk_index_oid != pg_sys::InvalidOid {
                        // Index scan path: one scan per needed col_idx
                        let cs_snapshot = pg_sys::GetActiveSnapshot();
                        let idx_rel = pg_sys::index_open(pk_index_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
                        let slot = pg_sys::table_slot_create(cs_rel, std::ptr::null_mut());

                        for &col_idx_val in &needed_col_idxs {
                            let mut skey = [pg_sys::ScanKeyData::default()];
                            pg_sys::ScanKeyInit(
                                &mut skey[0],
                                1, // attnum 1 = _col_idx (first column in PK)
                                pg_sys::BTEqualStrategyNumber as u16,
                                pg_sys::F_INT2EQ.into(),
                                pg_sys::Datum::from(col_idx_val),
                            );

                            #[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
                            let scan = pg_sys::index_beginscan(cs_rel, idx_rel, cs_snapshot, 1, 0);
                            #[cfg(feature = "pg18")]
                            let scan = pg_sys::index_beginscan(cs_rel, idx_rel, cs_snapshot, std::ptr::null_mut(), 1, 0);
                            pg_sys::index_rescan(scan, skey.as_mut_ptr(), 1, std::ptr::null_mut(), 0);

                            loop {
                                if !pg_sys::index_getnext_slot(scan, pg_sys::ScanDirection::ForwardScanDirection, slot) {
                                    break;
                                }
                                pg_sys::slot_getallattrs(slot);
                                let tts_values = std::slice::from_raw_parts((*slot).tts_values, cs_natts);
                                let tts_isnull = std::slice::from_raw_parts((*slot).tts_isnull, cs_natts);

                                process_colstats_row!(tts_values, tts_isnull, ci_att, sid_att,
                                    min_att, max_att, sum_att, nn_att, nz_att);
                            }

                            pg_sys::index_endscan(scan);
                        }

                        pg_sys::ExecDropSingleTupleTableSlot(slot);
                        pg_sys::index_close(idx_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
                    } else {
                        // Seq scan path: scan all rows, filter by needed col_idx
                        let cs_snapshot = pg_sys::GetActiveSnapshot();
                        let cs_flags: u32 = pg_sys::ScanOptions::SO_TYPE_SEQSCAN
                            | pg_sys::ScanOptions::SO_ALLOW_STRAT
                            | pg_sys::ScanOptions::SO_ALLOW_SYNC
                            | pg_sys::ScanOptions::SO_ALLOW_PAGEMODE;
                        let cs_scan = (*(*cs_rel).rd_tableam).scan_begin.unwrap()(
                            cs_rel, cs_snapshot, 0, std::ptr::null_mut(), std::ptr::null_mut(), cs_flags,
                        );

                        let mut cs_values = vec![pg_sys::Datum::from(0); cs_natts];
                        let mut cs_nulls = vec![true; cs_natts];

                        loop {
                            let tuple = pg_sys::heap_getnext(
                                cs_scan,
                                pg_sys::ScanDirection::ForwardScanDirection,
                            );
                            if tuple.is_null() { break; }

                            pg_sys::heap_deform_tuple(
                                tuple, cs_tupdesc,
                                cs_values.as_mut_ptr(), cs_nulls.as_mut_ptr(),
                            );

                            // Skip columns we don't need in seq scan path
                            if !needed_col_idxs.is_empty() {
                                let ci = if !cs_nulls[ci_att] { cs_values[ci_att].value() as i16 } else { continue; };
                                if !needed_col_idxs.contains(&ci) { continue; }
                            }

                            process_colstats_row!(cs_values, cs_nulls, ci_att, sid_att,
                                min_att, max_att, sum_att, nn_att, nz_att);
                        }

                        (*(*cs_rel).rd_tableam).scan_end.unwrap()(cs_scan);
                    }
                }

                pg_sys::table_close(cs_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

                // Populate colstats cache from the raw rows collected during scan.
                // Uses cs_raw_rows which includes all segments (even pruned ones).
                COLSTATS_CACHE.with(|cache| {
                    let mut cache = cache.borrow_mut();
                    for ((ci, sid), row) in cs_raw_rows.drain() {
                        let entry = cache.entry((colstats_oid, ci)).or_insert_with(|| CachedColStats {
                            rows: HashMap::new(),
                        });
                        entry.rows.insert(sid, row);
                    }
                });

                // Remove colstats-pruned segments
                if !cs_pruned_ids.is_empty() {
                    let mut i = 0;
                    while i < segments.len() {
                        if cs_pruned_ids.contains(&surviving_segment_ids[i]) {
                            segments.swap_remove(i);
                            surviving_segment_ids.swap_remove(i);
                            segments_skipped += 1;
                        } else {
                            i += 1;
                        }
                    }
                }

                } // end else (uncached col_idxs scan)
            }
        }

        let (t1b_hit, t1b_read) = shared_buf_snapshot();
        buf_stats.meta_hit += t1b_hit - t1_hit;
        buf_stats.meta_read += t1b_read - t1_read;

        // ================================================================
        // Bloom phase: PK index scan per bloom-checked column to prune
        // surviving segments. Mirrors the Phase 2 blob index-scan loop.
        // ================================================================
        if !bloom_checks.is_empty() && !segments.is_empty() {
            let meta_name_ptr = pg_sys::get_rel_name(meta_oid);
            let meta_name_str = std::ffi::CStr::from_ptr(meta_name_ptr)
                .to_string_lossy()
                .into_owned();
            let meta_ns_oid = pg_sys::get_rel_namespace(meta_oid);
            let partition_name = meta_name_str.strip_suffix("_meta").unwrap_or(&meta_name_str);
            let blooms_name = format!("{}_blooms", partition_name);
            let blooms_cname = std::ffi::CString::new(blooms_name).unwrap();
            let blooms_oid = pg_sys::get_relname_relid(blooms_cname.as_ptr(), meta_ns_oid);

            if blooms_oid != pg_sys::InvalidOid {
                // Build surviving segment_id → segment index mapping
                let mut seg_id_to_idx: HashMap<i32, usize> = HashMap::new();
                for (idx, &sid) in surviving_segment_ids.iter().enumerate() {
                    seg_id_to_idx.insert(sid, idx);
                }

                let blooms_rel = pg_sys::table_open(blooms_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
                let blooms_tupdesc = (*blooms_rel).rd_att;
                let blooms_natts = (*blooms_tupdesc).natts as usize;

                // Locate attnos for _segment_id, _num_hashes, _data once from the tupdesc
                let mut seg_id_att: Option<usize> = None;
                let mut num_hashes_att: Option<usize> = None;
                let mut data_att: Option<usize> = None;
                for i in 0..blooms_natts {
                    let attr = &*tupdesc_get_attr(blooms_tupdesc, i);
                    let name = std::ffi::CStr::from_ptr(attr.attname.data.as_ptr())
                        .to_string_lossy();
                    if name == "_segment_id" { seg_id_att = Some(i); }
                    else if name == "_num_hashes" { num_hashes_att = Some(i); }
                    else if name == "_data" { data_att = Some(i); }
                }

                // Find PK index OID — first index that is primary
                let pk_index_oid = {
                    let mut pk_oid = pg_sys::InvalidOid;
                    let index_list = pg_sys::RelationGetIndexList(blooms_rel);
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

                if let (Some(sid_att), Some(nh_att), Some(dat_att), true) =
                    (seg_id_att, num_hashes_att, data_att, pk_index_oid != pg_sys::InvalidOid)
                {
                    let snapshot = pg_sys::GetActiveSnapshot();
                    let idx_rel = pg_sys::index_open(pk_index_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
                    let mut bloom_pruned_ids: std::collections::HashSet<i32> = std::collections::HashSet::new();

                    for bc in &bloom_checks {
                        // Set up scan key: _col_idx = col_idx (SMALLINT equality)
                        let mut skey = [pg_sys::ScanKeyData::default()];
                        pg_sys::ScanKeyInit(
                            &mut skey[0],
                            1, // attnum 1 = _col_idx
                            pg_sys::BTEqualStrategyNumber as u16,
                            pg_sys::F_INT2EQ.into(),
                            pg_sys::Datum::from(bc.col_idx as i16),
                        );

                        #[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
                        let scan = pg_sys::index_beginscan(blooms_rel, idx_rel, snapshot, 1, 0);
                        #[cfg(feature = "pg18")]
                        let scan = pg_sys::index_beginscan(blooms_rel, idx_rel, snapshot, std::ptr::null_mut(), 1, 0);
                        pg_sys::index_rescan(scan, skey.as_mut_ptr(), 1, std::ptr::null_mut(), 0);

                        let slot = pg_sys::table_slot_create(blooms_rel, std::ptr::null_mut());

                        loop {
                            if !pg_sys::index_getnext_slot(scan, pg_sys::ScanDirection::ForwardScanDirection, slot) {
                                break;
                            }

                            pg_sys::slot_getallattrs(slot);
                            let tts_values = (*slot).tts_values;
                            let tts_isnull = (*slot).tts_isnull;

                            if *tts_isnull.add(sid_att) || *tts_isnull.add(nh_att) || *tts_isnull.add(dat_att) {
                                continue;
                            }
                            let seg_id = (*tts_values.add(sid_att)).value() as i32;

                            if !seg_id_to_idx.contains_key(&seg_id) { continue; }

                            let num_hashes = (*tts_values.add(nh_att)).value() as u8;

                            // Detoast bloom data
                            let varlena_ptr = (*tts_values.add(dat_att)).cast_mut_ptr::<pg_sys::varlena>();
                            let detoasted = pg_sys::pg_detoast_datum(varlena_ptr);
                            let data_ptr = pgrx::vardata_any(detoasted);
                            let data_len = pgrx::varsize_any_exhdr(detoasted);
                            #[allow(clippy::unnecessary_cast)]
                            let bloom_bytes = std::slice::from_raw_parts(data_ptr as *const u8, data_len);

                            let bf = crate::bloom::BloomFilter::from_bytes(bloom_bytes, num_hashes);
                            let any_match = bc.hashes.iter().any(|&h| bf.might_contain(h));

                            if detoasted != varlena_ptr {
                                pg_sys::pfree(detoasted as *mut _);
                            }

                            if !any_match {
                                bloom_pruned_ids.insert(seg_id);
                            }
                        }

                        pg_sys::ExecDropSingleTupleTableSlot(slot);
                        pg_sys::index_endscan(scan);
                    }

                    pg_sys::index_close(idx_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

                    // Remove bloom-pruned segments (segments and surviving_segment_ids are parallel)
                    if !bloom_pruned_ids.is_empty() {
                        let before = segments.len();
                        let mut i = 0;
                        while i < segments.len() {
                            if bloom_pruned_ids.contains(&surviving_segment_ids[i]) {
                                segments.swap_remove(i);
                                surviving_segment_ids.swap_remove(i);
                            } else {
                                i += 1;
                            }
                        }
                        let pruned = before - segments.len();
                        segments_skipped += pruned as u64;
                        segments_bloom_skipped += pruned as u64;
                    }
                }

                pg_sys::table_close(blooms_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            }
        }

        let (t2_hit, t2_read) = shared_buf_snapshot();
        buf_stats.bloom_hit = t2_hit - t1b_hit;
        buf_stats.bloom_read = t2_read - t1b_read;

        // ----------------------------------------------------------------
        // Segment pruning via per-segment value-presence bitmap (text Eq).
        // Mirrors the bloom block above: open `<partition>_valbitmap` by
        // PK on `(_col_idx, _segment_id)`, fetch `_bits`, test the bit
        // recorded in `valmap` for the queried constant. Exact (no false
        // positives), so a clear bit guarantees the segment can be skipped.
        // ----------------------------------------------------------------
        if !valbitmap_checks.is_empty() && !segments.is_empty() {
            // First handle "constant absent from partition entirely" — no
            // need to even open the bitmap table for those.
            if valbitmap_checks.iter().any(|c| c.prune_all) {
                let pruned = segments.len();
                segments.clear();
                surviving_segment_ids.clear();
                segments_skipped += pruned as u64;
                segments_valbitmap_skipped += pruned as u64;
            } else {
                let meta_name_ptr = pg_sys::get_rel_name(meta_oid);
                let meta_name_str = std::ffi::CStr::from_ptr(meta_name_ptr)
                    .to_string_lossy()
                    .into_owned();
                let meta_ns_oid = pg_sys::get_rel_namespace(meta_oid);
                let partition_name = meta_name_str.strip_suffix("_meta").unwrap_or(&meta_name_str);
                let valbitmap_name = format!("{}_valbitmap", partition_name);
                let valbitmap_cname = std::ffi::CString::new(valbitmap_name).unwrap();
                let valbitmap_oid =
                    pg_sys::get_relname_relid(valbitmap_cname.as_ptr(), meta_ns_oid);

                if valbitmap_oid != pg_sys::InvalidOid {
                    let mut seg_id_to_idx: HashMap<i32, usize> = HashMap::new();
                    for (idx, &sid) in surviving_segment_ids.iter().enumerate() {
                        seg_id_to_idx.insert(sid, idx);
                    }

                    let vb_rel = pg_sys::table_open(
                        valbitmap_oid,
                        pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                    );
                    let vb_tupdesc = (*vb_rel).rd_att;
                    let vb_natts = (*vb_tupdesc).natts as usize;

                    let mut seg_id_att: Option<usize> = None;
                    let mut bits_att: Option<usize> = None;
                    for i in 0..vb_natts {
                        let attr = &*tupdesc_get_attr(vb_tupdesc, i);
                        let name = std::ffi::CStr::from_ptr(attr.attname.data.as_ptr())
                            .to_string_lossy();
                        if name == "_segment_id" {
                            seg_id_att = Some(i);
                        } else if name == "_bits" {
                            bits_att = Some(i);
                        }
                    }

                    let pk_index_oid = {
                        let mut pk_oid = pg_sys::InvalidOid;
                        let index_list = pg_sys::RelationGetIndexList(vb_rel);
                        if !index_list.is_null() {
                            let n = (*index_list).length;
                            for i in 0..n {
                                let idx_oid = (*(*index_list).elements.add(i as usize)).oid_value;
                                let idx_rel = pg_sys::index_open(
                                    idx_oid,
                                    pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                                );
                                let is_primary = if !(*idx_rel).rd_index.is_null() {
                                    (*(*idx_rel).rd_index).indisprimary
                                } else {
                                    false
                                };
                                pg_sys::index_close(
                                    idx_rel,
                                    pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                                );
                                if is_primary {
                                    pk_oid = idx_oid;
                                    break;
                                }
                            }
                            pg_sys::list_free(index_list);
                        }
                        pk_oid
                    };

                    if let (Some(sid_att), Some(bits_a), true) =
                        (seg_id_att, bits_att, pk_index_oid != pg_sys::InvalidOid)
                    {
                        let snapshot = pg_sys::GetActiveSnapshot();
                        let idx_rel = pg_sys::index_open(
                            pk_index_oid,
                            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                        );
                        let mut vb_pruned_ids: std::collections::HashSet<i32> =
                            std::collections::HashSet::new();

                        for vc in &valbitmap_checks {
                            if vc.prune_all {
                                continue;
                            }
                            let mut skey = [pg_sys::ScanKeyData::default()];
                            pg_sys::ScanKeyInit(
                                &mut skey[0],
                                1, // attnum 1 = _col_idx
                                pg_sys::BTEqualStrategyNumber as u16,
                                pg_sys::F_INT2EQ.into(),
                                pg_sys::Datum::from(vc.col_idx as i16),
                            );

                            #[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
                            let scan = pg_sys::index_beginscan(vb_rel, idx_rel, snapshot, 1, 0);
                            #[cfg(feature = "pg18")]
                            let scan = pg_sys::index_beginscan(
                                vb_rel,
                                idx_rel,
                                snapshot,
                                std::ptr::null_mut(),
                                1,
                                0,
                            );
                            pg_sys::index_rescan(scan, skey.as_mut_ptr(), 1, std::ptr::null_mut(), 0);

                            let slot = pg_sys::table_slot_create(vb_rel, std::ptr::null_mut());

                            loop {
                                if !pg_sys::index_getnext_slot(
                                    scan,
                                    pg_sys::ScanDirection::ForwardScanDirection,
                                    slot,
                                ) {
                                    break;
                                }
                                pg_sys::slot_getallattrs(slot);
                                let tts_values = (*slot).tts_values;
                                let tts_isnull = (*slot).tts_isnull;
                                if *tts_isnull.add(sid_att) || *tts_isnull.add(bits_a) {
                                    continue;
                                }
                                let seg_id = (*tts_values.add(sid_att)).value() as i32;
                                if !seg_id_to_idx.contains_key(&seg_id) {
                                    continue;
                                }

                                let varlena_ptr =
                                    (*tts_values.add(bits_a)).cast_mut_ptr::<pg_sys::varlena>();
                                let detoasted = pg_sys::pg_detoast_datum(varlena_ptr);
                                let data_ptr = pgrx::vardata_any(detoasted);
                                let data_len = pgrx::varsize_any_exhdr(detoasted);
                                #[allow(clippy::unnecessary_cast)]
                                let bits = std::slice::from_raw_parts(
                                    data_ptr as *const u8,
                                    data_len,
                                );

                                // A segment passes if any wanted bit is set.
                                let passes = vc.wanted_bits.iter().any(|&bi| {
                                    let byte = (bi / 8) as usize;
                                    let mask = 1u8 << (bi % 8);
                                    byte < bits.len() && (bits[byte] & mask) != 0
                                });

                                if detoasted != varlena_ptr {
                                    pg_sys::pfree(detoasted as *mut _);
                                }

                                if !passes {
                                    vb_pruned_ids.insert(seg_id);
                                }
                            }

                            pg_sys::ExecDropSingleTupleTableSlot(slot);
                            pg_sys::index_endscan(scan);
                        }

                        pg_sys::index_close(idx_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

                        if !vb_pruned_ids.is_empty() {
                            let before = segments.len();
                            let mut i = 0;
                            while i < segments.len() {
                                if vb_pruned_ids.contains(&surviving_segment_ids[i]) {
                                    segments.swap_remove(i);
                                    surviving_segment_ids.swap_remove(i);
                                } else {
                                    i += 1;
                                }
                            }
                            let pruned = before - segments.len();
                            segments_skipped += pruned as u64;
                            segments_valbitmap_skipped += pruned as u64;
                        }
                    }

                    pg_sys::table_close(vb_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
                }
            }
        }

        pgrx::log!(
            "load_segments_heap phase1: segments={} skipped={} (minmax={} bloom={} valbitmap={}) heap_getnext={:.1}ms deform={:.1}ms",
            segments.len(),
            segments_skipped,
            segments_minmax_skipped,
            segments_bloom_skipped,
            segments_valbitmap_skipped,
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

        if !segments.is_empty() && any_blobs_needed && !skip_blob_load {
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
                                // Eager path: try the cache, fall back to detoast.
                                let cache_key = crate::blob_cache::BlobCacheKey::new(
                                    meta_oid, seg_id, blob_slot,
                                );
                                if let Some(pin) = crate::blob_cache::get_pinned(&cache_key) {
                                    let s = pin.as_slice();
                                    segments[seg_idx].compressed_blobs[blob_slot] =
                                        BlobBytes::Cached { data: s.as_ptr(), len: s.len() as u32 };
                                    segments[seg_idx].cached_blob_pins.push(pin);
                                } else {
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
                                    crate::blob_cache::insert(&cache_key, &bytes);
                                    segments[seg_idx].compressed_blobs[blob_slot] = BlobBytes::Owned(bytes);
                                }
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
                            let cache_key = crate::blob_cache::BlobCacheKey::new(
                                meta_oid, seg_id, blob_slot,
                            );
                            if let Some(pin) = crate::blob_cache::get_pinned(&cache_key) {
                                let s = pin.as_slice();
                                segments[seg_idx].compressed_blobs[blob_slot] =
                                    BlobBytes::Cached { data: s.as_ptr(), len: s.len() as u32 };
                                segments[seg_idx].cached_blob_pins.push(pin);
                            } else {
                                let varlena_ptr: *mut pg_sys::varlena = bv[2].cast_mut_ptr();
                                let detoasted = pg_sys::pg_detoast_datum(varlena_ptr);
                                let len = pgrx::varsize_any_exhdr(detoasted);
                                let data = pgrx::vardata_any(detoasted);
                                #[allow(clippy::unnecessary_cast)]
                                let bytes = std::slice::from_raw_parts(data as *const u8, len).to_vec();
                                if detoasted != varlena_ptr { pg_sys::pfree(detoasted as *mut _); }
                                crate::blob_cache::insert(&cache_key, &bytes);
                                segments[seg_idx].compressed_blobs[blob_slot] = BlobBytes::Owned(bytes);
                            }
                        }
                    }

                    (*(*blob_rel).rd_tableam).scan_end.unwrap()(blob_scan);
                }

                detoast_us = detoast_start.elapsed().as_micros() as u64;

                pg_sys::table_close(blob_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            }
        }

        let (t3_hit, t3_read) = shared_buf_snapshot();
        buf_stats.blob_hit = t3_hit - t2_hit;
        buf_stats.blob_read = t3_read - t2_read;

        accumulate_scan_buf_stats(&buf_stats);

        pgrx::log!(
            "load_segments_heap phase2: segments={} skipped={} detoast={:.1}ms",
            segments.len(),
            segments_skipped,
            detoast_us as f64 / 1000.0,
        );

        (
            segments,
            segments_skipped,
            segments_minmax_skipped,
            segments_bloom_skipped,
            segments_valbitmap_skipped,
            detoast_us,
        )
    }
}

/// Load text-length sidecar blobs for the columns marked sidecar-only, writing
/// them into each segment's `text_length_blobs[blob_slot]`. Returns the elapsed
/// detoast time in microseconds.
///
/// Uses an index scan on the `<partition>_text_lengths` PK (same pattern as the
/// main blob loader). Silently no-ops when the table doesn't exist (old data
/// compressed before the sidecar was introduced).
pub(super) unsafe fn load_text_length_sidecars(
    meta_oid: pg_sys::Oid,
    col_names: &[String],
    segment_by: &[String],
    sidecar_cols: &[bool],
    segments: &mut [SegmentData],
) -> u64 {
    if segments.is_empty() || !sidecar_cols.iter().any(|&s| s) {
        return 0;
    }

    unsafe {
        // Derive text_lengths table OID from meta table name
        let meta_name_ptr = pg_sys::get_rel_name(meta_oid);
        let meta_name = std::ffi::CStr::from_ptr(meta_name_ptr)
            .to_string_lossy()
            .into_owned();
        let meta_ns_oid = pg_sys::get_rel_namespace(meta_oid);
        let partition_name = meta_name.strip_suffix("_meta").unwrap_or(&meta_name);
        let tl_name = format!("{}_text_lengths", partition_name);
        let tl_cname = std::ffi::CString::new(tl_name).unwrap();
        let tl_oid = pg_sys::get_relname_relid(tl_cname.as_ptr(), meta_ns_oid);

        if tl_oid == pg_sys::InvalidOid {
            // Data compressed before the sidecar feature — no sidecar to load.
            return 0;
        }

        // Build col_idx -> blob_slot mapping (same rule as load_segments_heap)
        let mut col_idx_map: Vec<Option<u16>> = Vec::new();
        let mut ci: u16 = 0;
        for name in col_names {
            if segment_by.contains(name) {
                col_idx_map.push(None);
            } else {
                col_idx_map.push(Some(ci));
                ci += 1;
            }
        }

        // Determine which col_idx values we need sidecars for
        let mut needed_col_idxs: Vec<u16> = Vec::new();
        for (i, &is_sidecar) in sidecar_cols.iter().enumerate() {
            if is_sidecar
                && let Some(ci) = col_idx_map[i]
            {
                needed_col_idxs.push(ci);
            }
        }
        if needed_col_idxs.is_empty() {
            return 0;
        }

        // Build segment_id -> index-in-segments map
        let mut seg_id_to_idx: HashMap<i32, usize> = HashMap::new();
        for (idx, seg) in segments.iter().enumerate() {
            seg_id_to_idx.insert(seg.segment_id, idx);
        }

        let t_start = std::time::Instant::now();

        let rel = pg_sys::table_open(tl_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

        // Find PK index
        let pk_index_oid = {
            let mut pk_oid = pg_sys::InvalidOid;
            let index_list = pg_sys::RelationGetIndexList(rel);
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

        let snapshot = pg_sys::GetActiveSnapshot();

        if pk_index_oid != pg_sys::InvalidOid {
            let idx_rel = pg_sys::index_open(pk_index_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

            for &col_idx in &needed_col_idxs {
                let mut skey = [pg_sys::ScanKeyData::default()];
                pg_sys::ScanKeyInit(
                    &mut skey[0],
                    1, // _col_idx
                    pg_sys::BTEqualStrategyNumber as u16,
                    pg_sys::F_INT2EQ.into(),
                    pg_sys::Datum::from(col_idx as i16),
                );

                #[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
                let scan = pg_sys::index_beginscan(rel, idx_rel, snapshot, 1, 0);
                #[cfg(feature = "pg18")]
                let scan = pg_sys::index_beginscan(rel, idx_rel, snapshot, std::ptr::null_mut(), 1, 0);
                pg_sys::index_rescan(scan, skey.as_mut_ptr(), 1, std::ptr::null_mut(), 0);

                let slot = pg_sys::table_slot_create(rel, std::ptr::null_mut());

                loop {
                    if !pg_sys::index_getnext_slot(scan, pg_sys::ScanDirection::ForwardScanDirection, slot) {
                        break;
                    }
                    pg_sys::slot_getallattrs(slot);
                    let tts_values = (*slot).tts_values;
                    let tts_isnull = (*slot).tts_isnull;

                    // attnum 2 = _segment_id, attnum 3 = _data
                    if *tts_isnull.add(1) || *tts_isnull.add(2) {
                        continue;
                    }
                    let seg_id = (*tts_values.add(1)).value() as i32;
                    let seg_idx = match seg_id_to_idx.get(&seg_id) {
                        Some(&i) => i,
                        None => continue, // pruned
                    };

                    let varlena_ptr: *mut pg_sys::varlena = (*tts_values.add(2)).cast_mut_ptr();
                    let detoasted = pg_sys::pg_detoast_datum(varlena_ptr);
                    let len = pgrx::varsize_any_exhdr(detoasted);
                    let data = pgrx::vardata_any(detoasted);
                    #[allow(clippy::unnecessary_cast)]
                    let bytes = std::slice::from_raw_parts(data as *const u8, len).to_vec();
                    if detoasted != varlena_ptr {
                        pg_sys::pfree(detoasted as *mut _);
                    }
                    segments[seg_idx].text_length_blobs[col_idx as usize] = bytes;
                }

                pg_sys::ExecDropSingleTupleTableSlot(slot);
                pg_sys::index_endscan(scan);
            }

            pg_sys::index_close(idx_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        }

        pg_sys::table_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

        t_start.elapsed().as_micros() as u64
    }
}

/// Fetch compressed blobs for a single segment via `(_col_idx, _segment_id)`
/// PK lookup on the companion `_blobs` table. Populates
/// `seg.compressed_blobs[col_idx]` for every non-segment-by column marked in
/// `needed_cols`, detoasting each value in place. Idempotent (skips columns
/// already populated).
///
/// Called on-claim from `load_next_segment` (parallel and serial paths after
/// §5.7 DSM sharing): instead of the leader eagerly detoasting every
/// segment's blobs in `load_segments_heap`, each claimant fetches only the
/// blobs for segments it actually processes — so blob I/O is parallelised
/// across workers.
pub(super) unsafe fn fetch_segment_blobs(
    companion_oid: pg_sys::Oid,
    segment_id: i32,
    col_names: &[String],
    segment_by: &[String],
    needed_cols: &[bool],
    seg: &mut SegmentData,
) -> u64 {
    let t_start = std::time::Instant::now();
    unsafe {
        // Pre-size blob slots if empty (first fetch).
        let num_blob_cols = col_names.iter()
            .filter(|n| !segment_by.contains(*n))
            .count();
        if seg.compressed_blobs.is_empty() {
            seg.compressed_blobs = Vec::with_capacity(num_blob_cols);
            seg.compressed_blobs.resize_with(num_blob_cols, BlobBytes::default);
        }

        // Derive `{partition}_blobs` OID from meta OID.
        let meta_name_ptr = pg_sys::get_rel_name(companion_oid);
        if meta_name_ptr.is_null() {
            return t_start.elapsed().as_micros() as u64;
        }
        let meta_name = std::ffi::CStr::from_ptr(meta_name_ptr)
            .to_string_lossy()
            .into_owned();
        let meta_ns_oid = pg_sys::get_rel_namespace(companion_oid);
        let partition_name = meta_name.strip_suffix("_meta").unwrap_or(&meta_name);
        let blobs_name = format!("{}_blobs", partition_name);
        let blobs_cname = std::ffi::CString::new(blobs_name).unwrap();
        let blob_oid = pg_sys::get_relname_relid(blobs_cname.as_ptr(), meta_ns_oid);
        if blob_oid == pg_sys::InvalidOid {
            return t_start.elapsed().as_micros() as u64;
        }

        // Build col_idx mapping: each non-segment-by col_name → dense col_idx
        // (used as the first PK column in `_blobs`). Same rule as `load_segments_heap`.
        let mut col_idx_map: Vec<Option<u16>> = Vec::new();
        {
            let mut ci: u16 = 0;
            for name in col_names {
                if segment_by.contains(name) {
                    col_idx_map.push(None);
                } else {
                    col_idx_map.push(Some(ci));
                    ci += 1;
                }
            }
        }

        let blob_rel = pg_sys::table_open(blob_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

        // Find PK index OID on `_blobs` — assumed `(_col_idx, _segment_id)`.
        let pk_index_oid = {
            let mut pk_oid = pg_sys::InvalidOid;
            let index_list = pg_sys::RelationGetIndexList(blob_rel);
            if !index_list.is_null() {
                let n = (*index_list).length;
                for i in 0..n {
                    let idx_oid = (*(*index_list).elements.add(i as usize)).oid_value;
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

        if pk_index_oid == pg_sys::InvalidOid {
            pg_sys::table_close(blob_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            return t_start.elapsed().as_micros() as u64;
        }

        let idx_rel = pg_sys::index_open(pk_index_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        let snapshot = pg_sys::GetActiveSnapshot();

        for (i, name) in col_names.iter().enumerate() {
            if segment_by.contains(name) || !needed_cols[i] {
                continue;
            }
            let col_idx = match col_idx_map[i] {
                Some(ci) => ci,
                None => continue,
            };
            let blob_slot = col_idx as usize;
            if !seg.compressed_blobs[blob_slot].is_empty() {
                continue; // already fetched
            }

            // Cache fast path: skip the index lookup + heap I/O + detoast
            // entirely if this (companion, segment, col) blob is already
            // in the shared blob cache. The pin keeps the DSA bytes alive
            // for the lifetime of the segment.
            let cache_key =
                crate::blob_cache::BlobCacheKey::new(companion_oid, segment_id, blob_slot);
            if let Some(pin) = crate::blob_cache::get_pinned(&cache_key) {
                let s = pin.as_slice();
                seg.compressed_blobs[blob_slot] =
                    BlobBytes::Cached { data: s.as_ptr(), len: s.len() as u32 };
                seg.cached_blob_pins.push(pin);
                continue;
            }

            // Two-column PK scankey: (_col_idx = ci, _segment_id = seg_id).
            let mut skeys = [pg_sys::ScanKeyData::default(); 2];
            pg_sys::ScanKeyInit(
                &mut skeys[0],
                1,
                pg_sys::BTEqualStrategyNumber as u16,
                pg_sys::F_INT2EQ.into(),
                pg_sys::Datum::from(col_idx as i16),
            );
            pg_sys::ScanKeyInit(
                &mut skeys[1],
                2,
                pg_sys::BTEqualStrategyNumber as u16,
                pg_sys::F_INT4EQ.into(),
                pg_sys::Datum::from(segment_id),
            );

            #[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
            let scan = pg_sys::index_beginscan(blob_rel, idx_rel, snapshot, 2, 0);
            #[cfg(feature = "pg18")]
            let scan = pg_sys::index_beginscan(blob_rel, idx_rel, snapshot, std::ptr::null_mut(), 2, 0);
            pg_sys::index_rescan(scan, skeys.as_mut_ptr(), 2, std::ptr::null_mut(), 0);

            let slot = pg_sys::table_slot_create(blob_rel, std::ptr::null_mut());

            if pg_sys::index_getnext_slot(scan, pg_sys::ScanDirection::ForwardScanDirection, slot) {
                pg_sys::slot_getallattrs(slot);
                let tts_isnull = (*slot).tts_isnull;
                let tts_values = (*slot).tts_values;
                let data_null = *tts_isnull.add(2);
                if !data_null {
                    let data_datum = *tts_values.add(2);
                    let varlena_ptr: *mut pg_sys::varlena = data_datum.cast_mut_ptr();
                    let detoasted = pg_sys::pg_detoast_datum(varlena_ptr);
                    let len = pgrx::varsize_any_exhdr(detoasted);
                    let data = pgrx::vardata_any(detoasted);
                    #[allow(clippy::unnecessary_cast)]
                    let bytes = std::slice::from_raw_parts(data as *const u8, len).to_vec();
                    if detoasted != varlena_ptr {
                        pg_sys::pfree(detoasted as *mut _);
                    }
                    crate::blob_cache::insert(&cache_key, &bytes);
                    seg.compressed_blobs[blob_slot] = BlobBytes::Owned(bytes);
                }
            }

            pg_sys::ExecDropSingleTupleTableSlot(slot);
            pg_sys::index_endscan(scan);
        }

        pg_sys::index_close(idx_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        pg_sys::table_close(blob_rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
    }
    t_start.elapsed().as_micros() as u64
}

/// Materialize a single blob slot from the cache (on hit) or via
/// `pg_detoast_datum` (on miss). On miss, the freshly-detoasted bytes
/// are also inserted into the cache best-effort.
///
/// Returns `(bytes_served_from_cache, hit)`. `hit` is true when the
/// blob came from the cache; the caller can use this to bump per-query
/// stats counters.
unsafe fn detoast_blob_slot(seg: &mut SegmentData, bi: usize) -> (u64, bool) {
    unsafe {
        let key = crate::blob_cache::BlobCacheKey::new(
            seg.companion_oid,
            seg.segment_id,
            bi,
        );
        if let Some(pin) = crate::blob_cache::get_pinned(&key) {
            let slice = pin.as_slice();
            let len = slice.len() as u64;
            // Borrow directly from the pin — no memcpy. The pin lives
            // in cached_blob_pins until SegmentData drops, and
            // compressed_blobs is declared before cached_blob_pins so
            // the BlobBytes::Cached pointers drop first.
            seg.compressed_blobs[bi] =
                BlobBytes::Cached { data: slice.as_ptr(), len: slice.len() as u32 };
            seg.toast_pointers[bi].clear();
            seg.cached_blob_pins.push(pin);
            return (len, true);
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
        crate::blob_cache::insert(&key, &bytes);
        seg.compressed_blobs[bi] = BlobBytes::Owned(bytes);
        seg.toast_pointers[bi].clear();
        (0, false)
    }
}

/// Materialize deferred TOAST pointers for a segment.
///
/// For each blob index that has a non-empty toast_pointer, calls pg_detoast_datum
/// on the stored pointer copy and replaces the empty compressed_blob with the
/// detoasted data. Clears the toast_pointer after detoasting.
///
/// Returns the [`DetoastLazyStats`] aggregated over all blobs that were
/// materialised on this call.
pub(super) unsafe fn detoast_lazy_blobs(seg: &mut SegmentData) -> DetoastLazyStats {
    let mut stats = DetoastLazyStats::default();
    unsafe {
        for bi in 0..seg.toast_pointers.len() {
            if seg.toast_pointers[bi].is_empty() {
                continue;
            }
            let (bytes_from_cache, hit) = detoast_blob_slot(seg, bi);
            if hit {
                stats.cache_hits += 1;
                stats.cache_bytes_served += bytes_from_cache;
            } else {
                stats.cache_misses += 1;
            }
        }
    }
    stats
}

/// Materialize deferred TOAST pointers for specific blob indices only.
///
/// Like `detoast_lazy_blobs` but only processes the given blob indices,
/// leaving other blobs lazy. Used in top-N Phase 1 to detoast only
/// filter + sort column blobs while deferring Phase 2 columns.
pub(super) unsafe fn detoast_lazy_blobs_selective(
    seg: &mut SegmentData,
    blob_indices: &[usize],
) -> DetoastLazyStats {
    let mut stats = DetoastLazyStats::default();
    unsafe {
        for &bi in blob_indices {
            if bi >= seg.toast_pointers.len() || seg.toast_pointers[bi].is_empty() {
                continue;
            }
            let (bytes_from_cache, hit) = detoast_blob_slot(seg, bi);
            if hit {
                stats.cache_hits += 1;
                stats.cache_bytes_served += bytes_from_cache;
            } else {
                stats.cache_misses += 1;
            }
        }
    }
    stats
}

/// Per-call counters returned by the lazy-detoast helpers. Callers fold
/// these into their `ScanTiming` so the totals show up in EXPLAIN.
#[derive(Copy, Clone, Default, Debug)]
pub(crate) struct DetoastLazyStats {
    pub(crate) cache_hits: u64,
    pub(crate) cache_misses: u64,
    pub(crate) cache_bytes_served: u64,
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
