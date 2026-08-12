//! Condition cache: shared-memory cache of per-segment qual-selection
//! bitmaps. See `dev/docs/CONDITION_CACHE.md` for the design.
//!
//! Entries live in the blob cache's DSA (key kind `KEY_KIND_CONDITION`),
//! keyed by `(companion_oid, segment_id, qual_fingerprint)`. The
//! fingerprint canonically encodes the row-level filter set a scan site
//! applies — its `BatchQual` list and its `TextQualInfo` list, in
//! application order — so two paths share entries exactly when they apply
//! identical filters.
//!
//! Correctness rules enforced by callers (see the design doc):
//! - never engage for segments with tombstones (`seg.tombstones.is_some()`);
//! - never engage for partial-segment evaluation (Top-N `cutoff_row`);
//! - only engage when every selection-vector writer at the site is covered
//!   by the fingerprint (fail closed via [`fingerprint_quals`] returning
//!   `None`).

use pgrx::pg_sys;

use crate::blob_cache::{self, BlobCacheKey};

use super::batch_qual::{BatchCompareOp, BatchQual, LikeStrategy, is_batch_comparable_type};
use super::text_col::TextQualInfo;

/// Bumped whenever fingerprint semantics or the payload format change.
/// Old entries become unreachable (disjoint fingerprints) and age out.
const FP_VERSION: u64 = 1;

const PAYLOAD_MAGIC: u8 = 0xC1;
const PAYLOAD_BITMAP: u8 = 0;
const PAYLOAD_ALL_PASS: u8 = 1;
const PAYLOAD_NONE_PASS: u8 = 2;
const PAYLOAD_HEADER_LEN: usize = 8;

/// Decoded cache hit.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum CachedSelection {
    /// Every row passes. Maps to the in-tree "empty selection vector"
    /// convention.
    AllPass,
    /// No row passes — the segment can be skipped without decoding.
    NonePass,
    /// Per-row pass/fail, length == the segment's row count.
    Bitmap(Vec<bool>),
}

/// Leader-prepared condition-cache state for a `std::thread::scope`
/// dispatch. LWLocks are only legal on backend threads, so the backend
/// thread that owns the batch looks every segment up front (packed
/// payload bytes — cheap to hold), scope workers decode hits locally,
/// and computed selections travel back for the leader to insert after
/// the scope joins.
///
/// Keyed by `(companion_oid, segment_id)` so it works for every dispatch
/// shape (batched or not) without index bookkeeping.
pub(super) struct CondBatch {
    /// Fingerprint of the applied filter set; None = cache not engaged.
    pub(super) fp: Option<u64>,
    cached: std::collections::HashMap<(u32, i32), Vec<u8>>,
    /// Columns referenced only by fingerprinted filters — safe to leave
    /// undecoded when a segment's selection came from the cache.
    pub(super) filter_only_cols: Vec<bool>,
}

impl CondBatch {
    /// Inert batch state (cache off or filter set unfingerprintable).
    pub(super) fn disabled(n_cols: usize) -> Self {
        CondBatch {
            fp: None,
            cached: std::collections::HashMap::new(),
            filter_only_cols: vec![false; n_cols],
        }
    }

    /// Engage the cache with fingerprint `fp` and prefetch every
    /// segment's cached payload. Backend threads only.
    pub(super) fn prefetch(&mut self, fp: u64, segments: &[super::segments::SegmentData]) {
        self.fp = Some(fp);
        for seg in segments {
            if seg.row_count == 0 || seg.tombstones.is_some() {
                continue;
            }
            let key = BlobCacheKey::new_condition(seg.companion_oid, seg.segment_id, fp);
            if let Some(pin) = blob_cache::get_pinned(&key) {
                let bytes = pin.as_slice().to_vec();
                if !bytes.is_empty() {
                    self.cached
                        .insert((seg.companion_oid.to_u32(), seg.segment_id), bytes);
                }
            }
        }
    }

    /// Decode the cached selection for a segment, if any. Row-count
    /// mismatches read as a miss. Safe from any thread (no locks).
    pub(super) fn hit(&self, seg: &super::segments::SegmentData) -> Option<CachedSelection> {
        self.fp?;
        let bytes = self
            .cached
            .get(&(seg.companion_oid.to_u32(), seg.segment_id))?;
        decode_payload(bytes, seg.row_count as usize)
    }
}

/// Encode a computed selection into the packed payload form, for transport
/// from a scope worker back to the backend thread. Pure code — safe to
/// call from any thread.
pub(super) fn encode_selection_payload(selection: &[bool], row_count: usize) -> Option<Vec<u8>> {
    encode_payload(selection, row_count)
}

/// Insert a pre-encoded payload (produced by [`encode_selection_payload`])
/// under a raw companion OID. Backend threads only.
pub(super) fn store_encoded_raw(companion_oid: u32, segment_id: i32, qual_fp: u64, payload: &[u8]) {
    if !enabled() {
        return;
    }
    let key = BlobCacheKey {
        companion_oid,
        segment_id: segment_id as u32,
        col_idx: 0,
        kind: crate::blob_cache::KEY_KIND_CONDITION,
        qual_fp,
    };
    blob_cache::insert(&key, payload);
}

/// Cheap pre-gate. Checks the GUC and that the blob cache is actually
/// live in this process (`is_usable` is false in session-preload mode,
/// where the shmem hooks never ran) — so a disabled cache costs zero
/// fingerprinting/encoding work, not just discarded inserts.
pub(super) fn enabled() -> bool {
    crate::CONDITION_CACHE.get() && blob_cache::is_usable()
}

/// Look up the cached selection for `(companion_oid, segment_id, qual_fp)`.
/// `row_count` must match the stored row count or the entry is ignored
/// (belt-and-braces against segment-id aliasing).
pub(super) fn lookup(
    companion_oid: pg_sys::Oid,
    segment_id: i32,
    qual_fp: u64,
    row_count: usize,
) -> Option<CachedSelection> {
    if !enabled() {
        return None;
    }
    let key = BlobCacheKey::new_condition(companion_oid, segment_id, qual_fp);
    let pin = blob_cache::get_pinned(&key)?;
    decode_payload(pin.as_slice(), row_count)
}

/// Best-effort store of a computed selection. `selection` follows the
/// in-tree convention: empty means "all rows pass"; otherwise its length
/// must equal `row_count` (mismatches are silently not cached).
pub(super) fn store(
    companion_oid: pg_sys::Oid,
    segment_id: i32,
    qual_fp: u64,
    row_count: usize,
    selection: &[bool],
) {
    if !enabled() {
        return;
    }
    let Some(payload) = encode_payload(selection, row_count) else {
        return;
    };
    let key = BlobCacheKey::new_condition(companion_oid, segment_id, qual_fp);
    blob_cache::insert(&key, &payload);
}

// ---------------------------------------------------------------------------
// Payload codec
// ---------------------------------------------------------------------------

fn encode_payload(selection: &[bool], row_count: usize) -> Option<Vec<u8>> {
    if row_count == 0 || row_count > u32::MAX as usize {
        return None;
    }
    if !selection.is_empty() && selection.len() != row_count {
        return None;
    }
    let kind = if selection.is_empty() || selection.iter().all(|&b| b) {
        PAYLOAD_ALL_PASS
    } else if !selection.iter().any(|&b| b) {
        PAYLOAD_NONE_PASS
    } else {
        PAYLOAD_BITMAP
    };
    let n_words = row_count.div_ceil(64);
    let mut out = Vec::with_capacity(PAYLOAD_HEADER_LEN + 8 * n_words);
    out.extend_from_slice(&[PAYLOAD_MAGIC, kind, 0, 0]);
    out.extend_from_slice(&(row_count as u32).to_le_bytes());
    if kind == PAYLOAD_BITMAP {
        let mut word: u64 = 0;
        for (i, &b) in selection.iter().enumerate() {
            if b {
                word |= 1u64 << (i % 64);
            }
            if i % 64 == 63 {
                out.extend_from_slice(&word.to_le_bytes());
                word = 0;
            }
        }
        if !row_count.is_multiple_of(64) {
            out.extend_from_slice(&word.to_le_bytes());
        }
    }
    Some(out)
}

fn decode_payload(bytes: &[u8], expected_row_count: usize) -> Option<CachedSelection> {
    if bytes.len() < PAYLOAD_HEADER_LEN || bytes[0] != PAYLOAD_MAGIC {
        return None;
    }
    let kind = bytes[1];
    let row_count = u32::from_le_bytes(bytes[4..8].try_into().ok()?) as usize;
    if row_count != expected_row_count {
        return None;
    }
    match kind {
        PAYLOAD_ALL_PASS => Some(CachedSelection::AllPass),
        PAYLOAD_NONE_PASS => Some(CachedSelection::NonePass),
        PAYLOAD_BITMAP => {
            let n_words = row_count.div_ceil(64);
            let body = &bytes[PAYLOAD_HEADER_LEN..];
            if body.len() != 8 * n_words {
                return None;
            }
            let mut sel = vec![false; row_count];
            for (w, chunk) in body.chunks_exact(8).enumerate() {
                let word = u64::from_le_bytes(chunk.try_into().ok()?);
                if word == 0 {
                    continue;
                }
                let base = w * 64;
                let top = (base + 64).min(row_count);
                for (bit, s) in sel[base..top].iter_mut().enumerate() {
                    *s = (word >> bit) & 1 == 1;
                }
            }
            Some(CachedSelection::Bitmap(sel))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Qual fingerprinting
// ---------------------------------------------------------------------------

/// FNV-1a, hand-rolled: the fingerprint must be identical across backend
/// processes (leader + PG parallel workers + later sessions), which std's
/// hashers do not promise.
struct Fp(u64);

impl Fp {
    fn new() -> Self {
        Fp(0xcbf2_9ce4_8422_2325)
    }
    #[inline]
    fn u8(&mut self, b: u8) {
        self.0 = (self.0 ^ b as u64).wrapping_mul(0x0000_0100_0000_01B3);
    }
    fn u64(&mut self, v: u64) {
        for b in v.to_le_bytes() {
            self.u8(b);
        }
    }
    fn bytes(&mut self, s: &[u8]) {
        self.u64(s.len() as u64);
        for &b in s {
            self.u8(b);
        }
    }
    fn opt_str(&mut self, s: &Option<String>) {
        match s {
            None => self.u8(0),
            Some(v) => {
                self.u8(1);
                self.bytes(v.as_bytes());
            }
        }
    }
}

/// Explicit stable mapping — enum layout changes must not shift tags.
fn op_tag(op: BatchCompareOp) -> u8 {
    match op {
        BatchCompareOp::Eq => 1,
        BatchCompareOp::Ne => 2,
        BatchCompareOp::Lt => 3,
        BatchCompareOp::Le => 4,
        BatchCompareOp::Gt => 5,
        BatchCompareOp::Ge => 6,
        BatchCompareOp::Like => 7,
        BatchCompareOp::NotLike => 8,
        BatchCompareOp::InList => 9,
    }
}

fn like_tag(s: &LikeStrategy) -> (u8, &str) {
    match s {
        LikeStrategy::Contains(p) => (1, p.as_str()),
        LikeStrategy::StartsWith(p) => (2, p.as_str()),
        LikeStrategy::EndsWith(p) => (3, p.as_str()),
        LikeStrategy::Exact(p) => (4, p.as_str()),
        LikeStrategy::General(p) => (5, p.as_str()),
    }
}

/// Canonical fingerprint of the row-level filter set a site applies: its
/// `BatchQual` list plus its `TextQualInfo` list, in application order.
///
/// Returns `None` (poisoned → bypass the cache) when there is nothing to
/// cache or when a qual carries a constant with no canonical encoding
/// (a pointer datum with no decoded text/list form).
pub(super) fn fingerprint_quals(
    batch_quals: &[BatchQual],
    text_quals: &[TextQualInfo],
) -> Option<u64> {
    if batch_quals.is_empty() && text_quals.is_empty() {
        return None;
    }
    let mut h = Fp::new();
    h.u64(FP_VERSION);
    for bq in batch_quals {
        h.u8(0x01);
        h.u64(bq.col_idx as u64);
        h.u8(op_tag(bq.op));
        h.u64(bq.type_oid.to_u32() as u64);
        // Encode every present constant representation in a fixed order,
        // with presence markers, so no ambiguity is possible.
        h.opt_str(&bq.text_const);
        match &bq.like_strategy {
            None => h.u8(0),
            Some(ls) => {
                let (tag, pat) = like_tag(ls);
                h.u8(0x10 | tag);
                h.bytes(pat.as_bytes());
            }
        }
        match &bq.in_list_i64 {
            None => h.u8(0),
            Some(list) => {
                h.u8(1);
                h.u64(list.len() as u64);
                for v in list {
                    h.u64(*v as u64);
                }
            }
        }
        match &bq.in_list_text {
            None => h.u8(0),
            Some(list) => {
                h.u8(1);
                h.u64(list.len() as u64);
                for v in list {
                    h.bytes(v.as_bytes());
                }
            }
        }
        if is_batch_comparable_type(bq.type_oid) {
            // By-value datum: the u64 payload is the constant itself.
            h.u8(1);
            h.u64(bq.const_datum.value() as u64);
        } else if bq.text_const.is_none()
            && bq.like_strategy.is_none()
            && bq.in_list_i64.is_none()
            && bq.in_list_text.is_none()
        {
            // Pointer datum with no canonical decoded form — we cannot
            // fingerprint what evaluation would compare against.
            return None;
        } else {
            h.u8(0);
        }
    }
    for tq in text_quals {
        match tq {
            TextQualInfo::EqNe {
                col_idx,
                const_str,
                is_ne,
            } => {
                h.u8(0x02);
                h.u64(*col_idx as u64);
                h.u8(u8::from(*is_ne));
                h.bytes(const_str.as_bytes());
            }
            TextQualInfo::Like {
                col_idx,
                strategy,
                negate,
            } => {
                h.u8(0x03);
                h.u64(*col_idx as u64);
                h.u8(u8::from(*negate));
                let (tag, pat) = like_tag(strategy);
                h.u8(tag);
                h.bytes(pat.as_bytes());
            }
            TextQualInfo::InList { col_idx, values } => {
                h.u8(0x04);
                h.u64(*col_idx as u64);
                h.u64(values.len() as u64);
                for v in values {
                    h.bytes(v.as_bytes());
                }
            }
        }
    }
    Some(h.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(sel: &[bool], rc: usize) -> Option<CachedSelection> {
        decode_payload(&encode_payload(sel, rc)?, rc)
    }

    #[test]
    fn payload_all_pass_roundtrips() {
        assert_eq!(roundtrip(&[], 100), Some(CachedSelection::AllPass));
        assert_eq!(roundtrip(&[true; 100], 100), Some(CachedSelection::AllPass));
    }

    #[test]
    fn payload_none_pass_roundtrips() {
        assert_eq!(roundtrip(&[false; 65], 65), Some(CachedSelection::NonePass));
    }

    #[test]
    fn payload_bitmap_roundtrips() {
        for rc in [1usize, 63, 64, 65, 127, 128, 30_000] {
            let sel: Vec<bool> = (0..rc).map(|i| i % 3 == 0 || i == rc - 1).collect();
            if sel.iter().all(|&b| b) {
                continue; // degenerates to AllPass; covered above
            }
            match roundtrip(&sel, rc) {
                Some(CachedSelection::Bitmap(out)) => assert_eq!(out, sel, "rc={rc}"),
                other => panic!("rc={rc}: unexpected {other:?}"),
            }
        }
    }

    #[test]
    fn payload_row_count_mismatch_is_a_miss() {
        let p = encode_payload(&[false; 64], 64).unwrap();
        assert_eq!(decode_payload(&p, 65), None);
    }

    #[test]
    fn payload_rejects_garbage() {
        assert_eq!(decode_payload(&[], 10), None);
        assert_eq!(decode_payload(&[0xC1, 9, 0, 0, 10, 0, 0, 0], 10), None); // bad kind
        assert_eq!(decode_payload(&[0x00; 8], 10), None); // bad magic
        // Truncated bitmap body.
        let mut p = encode_payload(&(0..128).map(|i| i % 2 == 0).collect::<Vec<_>>(), 128).unwrap();
        p.truncate(p.len() - 8);
        assert_eq!(decode_payload(&p, 128), None);
    }

    #[test]
    fn payload_mismatched_selection_len_not_encoded() {
        assert!(encode_payload(&[true; 10], 20).is_none());
        assert!(encode_payload(&[], 0).is_none());
    }

    #[test]
    fn fingerprint_empty_filter_set_is_poisoned() {
        assert_eq!(fingerprint_quals(&[], &[]), None);
    }

    #[test]
    fn fingerprint_distinguishes_constants_and_ops() {
        let mk = |op, datum: usize| BatchQual {
            col_idx: 2,
            op,
            const_datum: pg_sys::Datum::from(datum),
            type_oid: pg_sys::INT8OID,
            ..Default::default()
        };
        let a = fingerprint_quals(&[mk(BatchCompareOp::Eq, 5)], &[]).unwrap();
        let b = fingerprint_quals(&[mk(BatchCompareOp::Eq, 6)], &[]).unwrap();
        let c = fingerprint_quals(&[mk(BatchCompareOp::Ne, 5)], &[]).unwrap();
        assert_ne!(a, b);
        assert_ne!(a, c);
        // Same inputs → same fingerprint (stability within a process).
        assert_eq!(
            a,
            fingerprint_quals(&[mk(BatchCompareOp::Eq, 5)], &[]).unwrap()
        );
    }

    #[test]
    fn fingerprint_text_pointer_datum_without_decoded_form_is_poisoned() {
        // A text-typed qual whose constant only exists as a pointer datum
        // (no text_const / like_strategy / lists) cannot be fingerprinted.
        let bq = BatchQual {
            col_idx: 1,
            op: BatchCompareOp::Eq,
            const_datum: pg_sys::Datum::from(0xdead_beefusize),
            type_oid: pg_sys::TEXTOID,
            ..Default::default()
        };
        assert_eq!(fingerprint_quals(&[bq], &[]), None);
    }

    #[test]
    fn fingerprint_covers_text_quals() {
        let like = TextQualInfo::Like {
            col_idx: 3,
            strategy: LikeStrategy::Contains("google".into()),
            negate: false,
        };
        let like_neg = TextQualInfo::Like {
            col_idx: 3,
            strategy: LikeStrategy::Contains("google".into()),
            negate: true,
        };
        let a = fingerprint_quals(&[], std::slice::from_ref(&like)).unwrap();
        let b = fingerprint_quals(&[], std::slice::from_ref(&like_neg)).unwrap();
        assert_ne!(a, b);
        let eq = TextQualInfo::EqNe {
            col_idx: 3,
            const_str: "google".into(),
            is_ne: false,
        };
        let c = fingerprint_quals(&[], &[eq]).unwrap();
        assert_ne!(a, c);
    }

    #[test]
    fn fingerprint_order_sensitive() {
        // Application order is part of the identity (deliberate: sites
        // apply filters in a fixed order; different order → different set).
        let q1 = TextQualInfo::EqNe {
            col_idx: 1,
            const_str: "a".into(),
            is_ne: false,
        };
        let q2 = TextQualInfo::EqNe {
            col_idx: 2,
            const_str: "b".into(),
            is_ne: false,
        };
        let ab = fingerprint_quals(&[], &[q1.clone(), q2.clone()]).unwrap();
        let ba = fingerprint_quals(&[], &[q2, q1]).unwrap();
        assert_ne!(ab, ba);
    }
}
