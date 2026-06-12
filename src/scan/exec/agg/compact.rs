//! Compact accumulator storage — flat byte-buffer representation of
//! per-group accumulator state used by the parallel-compact path and
//! the serial COMPACT sub-path. Plus the finalize machinery
//! (`compact_finalize`, `compact_emit_partial`, `finalize_accumulator`)
//! and the small datum-conversion helpers
//! (`datum_to_i128`, `datum_to_f64`, `i128_to_numeric_datum`) that all
//! the dispatches share.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::hash::BuildHasherDefault;

use pgrx::pg_sys;

use super::super::datum_utils::string_to_datum;
use super::super::segments::SegmentData;
use super::state::{AggAccumulator, AggExecSpec, AggType, OutputTransform};

/// String arena: all group key strings packed into one `Vec<u8>`.
/// One deallocation instead of 275K individual String deallocations.
pub(crate) struct StringArena {
    pub(crate) buf: Vec<u8>,
}

impl StringArena {
    pub(crate) fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub(crate) fn alloc(&mut self, s: &str) -> (u32, u32) {
        let off = self.buf.len() as u32;
        let len = s.len() as u32;
        self.buf.extend_from_slice(s.as_bytes());
        (off, len)
    }

    pub(crate) fn get(&self, off: u32, len: u32) -> &str {
        let slice = &self.buf[off as usize..off as usize + len as usize];
        // SAFETY: `alloc` only ever appends whole `&str` byte ranges, and
        // callers pass back the exact (off, len) `alloc` returned.
        debug_assert!(std::str::from_utf8(slice).is_ok());
        unsafe { std::str::from_utf8_unchecked(slice) }
    }
}

/// Convert a datum to i128 for SUM accumulation.
pub(crate) fn datum_to_i128(datum: pg_sys::Datum, type_oid: pg_sys::Oid) -> i128 {
    match type_oid {
        pg_sys::INT2OID => (datum.value() as i16) as i128,
        pg_sys::INT4OID => (datum.value() as i32) as i128,
        pg_sys::INT8OID => (datum.value() as i64) as i128,
        _ => datum.value() as i128,
    }
}

/// Convert a datum to f64 for float SUM/AVG.
pub(crate) fn datum_to_f64(datum: pg_sys::Datum, type_oid: pg_sys::Oid) -> f64 {
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
///
/// # Safety
///
/// Must run inside an active PG transaction. The returned datum is
/// palloc'd in `CurrentMemoryContext`; lifetime is up to PG.
pub(crate) unsafe fn i128_to_numeric_datum(val: i128) -> pg_sys::Datum {
    unsafe {
        if val >= i64::MIN as i128 && val <= i64::MAX as i128 {
            pg_sys::OidFunctionCall1Coll(
                pg_sys::Oid::from(1781u32), // int8_numeric
                pg_sys::InvalidOid,
                pg_sys::Datum::from(val as i64 as usize),
            )
        } else {
            let s = std::ffi::CString::new(val.to_string()).unwrap();
            pg_sys::OidFunctionCall3Coll(
                pg_sys::Oid::from(1701u32), // numeric_in
                pg_sys::InvalidOid,
                pg_sys::Datum::from(s.as_ptr()),
                pg_sys::Datum::from(0usize),
                pg_sys::Datum::from(-1i32 as usize),
            )
        }
    }
}

/// Finalize an accumulator into a (Datum, is_null) result pair.
///
/// # Safety
///
/// Must run inside an active PG transaction — some branches (SumInt
/// → NUMERIC, Avg, MinStr/MaxStr) allocate datums via PG FFI.
pub(crate) unsafe fn finalize_accumulator(
    acc: &AggAccumulator,
    spec: &AggExecSpec,
) -> (pg_sys::Datum, bool) {
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
                            pg_sys::Oid::from(1781u32), // int8_numeric
                            pg_sys::InvalidOid,
                            pg_sys::Datum::from(*count as usize),
                        );
                        let datum = pg_sys::OidFunctionCall2Coll(
                            pg_sys::Oid::from(1727u32), // numeric_div
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
            AggAccumulator::Count { count } => (pg_sys::Datum::from(*count as usize), false),
            AggAccumulator::CountDistinctInt { seen } => (pg_sys::Datum::from(seen.len()), false),
            AggAccumulator::CountDistinctStr { seen } => (pg_sys::Datum::from(seen.len()), false),
            AggAccumulator::MinInt { val } | AggAccumulator::MaxInt { val } => match val {
                Some(v) => (pg_sys::Datum::from(*v as usize), false),
                None => (pg_sys::Datum::from(0usize), true),
            },
            AggAccumulator::MinFloat { val } | AggAccumulator::MaxFloat { val } => match val {
                Some(v) => {
                    if spec.col_type_oid == pg_sys::FLOAT4OID {
                        let f4 = *v as f32;
                        (pg_sys::Datum::from(f4.to_bits() as usize), false)
                    } else {
                        (pg_sys::Datum::from(v.to_bits() as usize), false)
                    }
                }
                None => (pg_sys::Datum::from(0usize), true),
            },
            AggAccumulator::MinStr { val } | AggAccumulator::MaxStr { val } => match val {
                Some(s) => {
                    let datum = string_to_datum(s, spec.col_type_oid);
                    (datum, false)
                }
                None => (pg_sys::Datum::from(0usize), true),
            },
        }
    }
}
// ============================================================================

/// Kind of accumulator slot in compact storage.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum CompactAccKind {
    Count,            // 8 bytes: i64
    SumInt,           // 24 bytes: i128 sum (16) + i64 count (8) — for INT8 columns
    SumIntNarrow,     // 16 bytes: i64 sum (8) + i64 count (8) — for INT2/INT4 columns
    SumFloat,         // 16 bytes: f64 sum (8) + i64 count (8)
    MinStr,           // 8 bytes: u32 arena_offset + u32 length (sentinel: u32::MAX, 0)
    MaxStr,           // 8 bytes: u32 arena_offset + u32 length (sentinel: u32::MAX, 0)
    CountDistinctInt, // 8 bytes: i64 count cache (real data in CountDistinctSideCar)
    CountDistinctStr, // 8 bytes: i64 count cache (real data in CountDistinctSideCar)
    MinInt,           // 16 bytes: i64 val + i64 has_value flag (0 = unset)
    MaxInt,           // 16 bytes: i64 val + i64 has_value flag (0 = unset)
}

impl CompactAccKind {
    /// 8-bit on-wire tag. Stable across versions because parallel-agg DSM wire
    /// format depends on it.
    #[allow(dead_code)] // wired by C.2.d/e via agg_wire
    pub(crate) fn wire_tag(self) -> u8 {
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

    pub(crate) fn byte_size(self) -> usize {
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

    pub(crate) fn alignment(self) -> usize {
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
pub(crate) struct CompactAccLayout {
    /// (byte_offset, kind) per aggregate
    pub(crate) slots: Vec<(usize, CompactAccKind)>,
    /// Total bytes per group (aligned to 16)
    pub(crate) group_stride: usize,
}

impl CompactAccLayout {
    pub(crate) fn new(specs: &[AggExecSpec]) -> Self {
        let mut offset: usize = 0;

        // Sort by alignment (descending) to minimize padding.
        // We need to maintain original order for indexing, so we compute
        // offsets in alignment order then map back.
        let mut indexed: Vec<(usize, CompactAccKind)> = specs
            .iter()
            .enumerate()
            .map(|(i, spec)| {
                let kind = compact_acc_kind(spec);
                (i, kind)
            })
            .collect();
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

        CompactAccLayout {
            slots,
            group_stride,
        }
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
            } else if t == pg_sys::INT2OID
                || t == pg_sys::INT4OID
                || t == pg_sys::INT8OID
                || t == pg_sys::DATEOID
                || t == pg_sys::TIMESTAMPOID
                || t == pg_sys::TIMESTAMPTZOID
            {
                CompactAccKind::MinInt
            } else {
                unreachable!(
                    "compact_acc_kind: MIN on type {:?} not supported in compact path",
                    t
                )
            }
        }
        AggType::Max => {
            let t = spec.col_type_oid;
            if t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID {
                CompactAccKind::MaxStr
            } else if t == pg_sys::INT2OID
                || t == pg_sys::INT4OID
                || t == pg_sys::INT8OID
                || t == pg_sys::DATEOID
                || t == pg_sys::TIMESTAMPOID
                || t == pg_sys::TIMESTAMPTZOID
            {
                CompactAccKind::MaxInt
            } else {
                unreachable!(
                    "compact_acc_kind: MAX on type {:?} not supported in compact path",
                    t
                )
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
pub(crate) struct Bitset {
    words: Vec<u64>,
    nbits: u32,
}

impl Bitset {
    pub(crate) fn with_size(nbits: u32) -> Self {
        let nwords = nbits.div_ceil(64) as usize;
        Bitset {
            words: vec![0u64; nwords],
            nbits,
        }
    }
    #[inline]
    pub(crate) fn set(&mut self, idx: u32) {
        debug_assert!(idx < self.nbits, "Bitset::set out of range");
        let w = (idx >> 6) as usize;
        let b = idx & 63;
        self.words[w] |= 1u64 << b;
    }
    #[inline]
    pub(crate) fn or_with(&mut self, other: &Bitset) {
        debug_assert_eq!(self.words.len(), other.words.len());
        for (a, b) in self.words.iter_mut().zip(other.words.iter()) {
            *a |= *b;
        }
    }
    pub(crate) fn count_ones(&self) -> u64 {
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
pub(crate) struct DictDistinctRemap {
    pub(crate) global_count: u32,
    pub(crate) per_segment: Vec<Vec<u32>>,
}

/// Skip Phase D's bitset path when the per-column **post-dedup global
/// string count** exceeds this. Bitset size is `global_count` bits per
/// (spec, group, worker); at 10M × 16 groups × 16 workers ≈ 320 MB —
/// tolerable on bench-class boxes (m6i.8xlarge has 128 GB) but starts
/// mattering on small ones. Tighten if real workloads push past this.
///
/// Checked AFTER the parallel pre-pass — at that point we know the
/// actual deduplicated count, not the looser per-segment-dict-size sum.
pub(crate) const PHASE_D_MAX_GLOBAL_FOR_BITSET: u32 = 10_000_000;

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
pub(crate) const PHASE_D_MAX_DICT_SIZE_SUM: u64 = 50_000_000;

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
pub(crate) fn build_dict_distinct_remaps(
    all_segments: &[SegmentData],
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
            let dict_size = u32::from_le_bytes(cc_ref.data[0..4].try_into().unwrap()) as u64;
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
                        let mut seg_local_remaps: Vec<Vec<u32>> = Vec::with_capacity(chunk.len());
                        for seg in chunk {
                            let blob = &seg.compressed_blobs[col_idx];
                            if blob.is_empty() {
                                seg_local_remaps.push(Vec::new());
                                continue;
                            }
                            let cc_ref = CompressedColumnRef::from_bytes(blob);
                            let norm_buf;
                            let dict_data: &[u8] =
                                if cc_ref.type_tag == CompressionType::DictionaryLz4 {
                                    norm_buf = dictionary::normalize_lz4(cc_ref.data);
                                    &norm_buf[..]
                                } else {
                                    cc_ref.data
                                };
                            let header = dictionary::parse_header(dict_data);
                            let mut seg_remap: Vec<u32> = Vec::with_capacity(header.dict.len());
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
                        LocalPrePass {
                            local_entries,
                            seg_local_remaps,
                        }
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
        let mut thread_to_global: Vec<Vec<u32>> = Vec::with_capacity(local_results.len());
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
pub(crate) struct CountDistinctSideCar {
    pub(crate) entries: Vec<CdEntry>,
}

#[derive(PartialEq, Eq, Clone, Copy)]
pub(crate) enum CdKind {
    Int,
    Str,
    /// Dict-encoded text with leader pre-pass: per-group `Bitset` of size
    /// `global_count` (the bitset_size below). Merge is bit-OR; finalise is
    /// `count_ones`. Eligibility checked in `build_dict_distinct_remaps`.
    DictBitset,
}

pub(crate) struct CdEntry {
    pub(crate) spec_idx: usize,
    pub(crate) kind: CdKind,
    /// `bitset_size` for `DictBitset`; otherwise unused.
    pub(crate) bitset_size: u32,
    pub(crate) sets_int: Vec<hashbrown::HashSet<i64, BuildHasherDefault<ahash::AHasher>>>,
    pub(crate) sets_str: Vec<hashbrown::HashSet<u128, BuildHasherDefault<ahash::AHasher>>>,
    pub(crate) bitsets: Vec<Bitset>,
}

impl CdEntry {
    /// Count of distinct values seen for `group_idx`. The output of every
    /// CountDistinct accumulator finalisation, regardless of representation.
    #[inline]
    pub(crate) fn count(&self, group_idx: u32) -> i64 {
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
    pub(crate) fn new(agg_specs: &[AggExecSpec]) -> Self {
        Self::new_inner(agg_specs, &Default::default())
    }

    /// Phase D entry point. `dict_remap_sizes` maps spec_idx → bitset size
    /// (the global string-ID count) for every CountDistinct(text) spec the
    /// leader has confirmed is dict-encoded across all relevant segments.
    /// Specs absent from the map keep the HashSet<u128> behaviour.
    pub(crate) fn new_with_dict_remaps(
        agg_specs: &[AggExecSpec],
        dict_remap_sizes: &std::collections::HashMap<usize, u32>,
    ) -> Self {
        Self::new_inner(agg_specs, dict_remap_sizes)
    }

    pub(crate) fn new_inner(
        agg_specs: &[AggExecSpec],
        dict_remap_sizes: &std::collections::HashMap<usize, u32>,
    ) -> Self {
        let mut entries = Vec::new();
        for (i, spec) in agg_specs.iter().enumerate() {
            if spec.agg_type == AggType::CountDistinct {
                let is_str = matches!(
                    spec.col_type_oid,
                    pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID
                );
                let bitset_size = if is_str {
                    dict_remap_sizes.get(&i).copied().unwrap_or(0)
                } else {
                    0
                };
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

    pub(crate) fn alloc_group(&mut self) {
        for e in &mut self.entries {
            match e.kind {
                CdKind::Str => e.sets_str.push(hashbrown::HashSet::with_hasher(
                    BuildHasherDefault::default(),
                )),
                CdKind::Int => e.sets_int.push(hashbrown::HashSet::with_hasher(
                    BuildHasherDefault::default(),
                )),
                CdKind::DictBitset => e.bitsets.push(Bitset::with_size(e.bitset_size)),
            }
        }
    }

    pub(crate) fn insert_int(&mut self, spec_idx: usize, group_idx: u32, val: i64) {
        for e in &mut self.entries {
            if e.spec_idx == spec_idx {
                e.sets_int[group_idx as usize].insert(val);
                return;
            }
        }
    }

    pub(crate) fn insert_str(&mut self, spec_idx: usize, group_idx: u32, hash: u128) {
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
    pub(crate) fn insert_dict_global(&mut self, spec_idx: usize, group_idx: u32, global_id: u32) {
        for e in &mut self.entries {
            if e.spec_idx == spec_idx {
                e.bitsets[group_idx as usize].set(global_id);
                return;
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn len(&self, spec_idx: usize, group_idx: u32) -> i64 {
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

    pub(crate) fn union_from(
        &mut self,
        spec_idx: usize,
        dst_group: u32,
        other: &Self,
        src_group: u32,
    ) {
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
    pub(crate) fn write_counts_to_storage<S: std::hash::BuildHasher>(
        &self,
        storage: &mut CompactAccStorage,
        map: &hashbrown::HashMap<u128, u32, S>,
    ) {
        for e in &self.entries {
            for (_, &gidx) in map.iter() {
                let count = match e.kind {
                    CdKind::Str => e.sets_str[gidx as usize].len() as i64,
                    CdKind::Int => e.sets_int[gidx as usize].len() as i64,
                    CdKind::DictBitset => e.bitsets[gidx as usize].count_ones() as i64,
                };
                storage.set_count(gidx, e.spec_idx, count);
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Flat byte buffer holding compact accumulators for all groups.
///
/// Layout: groups packed as `[group_stride × num_groups]` bytes; within
/// each group, slot `i` lives at byte offset `layout.slots[i].0` and
/// has size/kind dictated by `layout.slots[i].1`. Field alignment is
/// enforced by `CompactAccLayout::new()`.
///
/// The accessor methods below (`read_count`/`incr_count`/`read_sum_int`
/// /`add_sum_int`/…) are safe: they go through bounds-checked slice
/// indexing into `buf` and read/write via `from_le_bytes`/`to_le_bytes`,
/// which LLVM lowers to plain loads/stores on little-endian targets.
/// The caller is responsible for matching the slot's `CompactAccKind`
/// to the access type (e.g. `read_count` on a `Count` slot only); the
/// dispatch logic inspects `layout.slots[slot].1` before choosing.
/// Mismatched kinds produce incorrect numeric results but no UB.
pub(crate) struct CompactAccStorage {
    pub(crate) buf: Vec<u8>,
    pub(crate) layout: CompactAccLayout,
    pub(crate) str_arena: StringArena,
}

impl CompactAccStorage {
    pub(crate) fn new(layout: CompactAccLayout) -> Self {
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
    pub(crate) fn from_parts(
        layout: CompactAccLayout,
        buf: Vec<u8>,
        str_arena_buf: Vec<u8>,
    ) -> Self {
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
    pub(crate) fn alloc_group(&mut self) -> u32 {
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
                self.write_min_max_str(group_idx as u32, slot_idx, u32::MAX, 0);
            }
        }
        group_idx as u32
    }

    /// Byte offset of (group_idx, slot)'s field in `buf`.
    #[inline]
    fn field_start(&self, group_idx: u32, slot: usize) -> usize {
        let (offset, _) = self.layout.slots[slot];
        group_idx as usize * self.layout.group_stride + offset
    }

    // ----- Count slot (8 bytes: i64 count) -----

    /// Read the current count.
    #[inline]
    pub(crate) fn read_count(&self, group_idx: u32, slot: usize) -> i64 {
        let s = self.field_start(group_idx, slot);
        i64::from_le_bytes(self.buf[s..s + 8].try_into().unwrap())
    }

    /// Add `delta` to the count.
    #[inline]
    pub(crate) fn incr_count(&mut self, group_idx: u32, slot: usize, delta: i64) {
        let s = self.field_start(group_idx, slot);
        let bytes: &mut [u8; 8] = (&mut self.buf[s..s + 8]).try_into().unwrap();
        let cur = i64::from_le_bytes(*bytes);
        *bytes = (cur + delta).to_le_bytes();
    }

    /// Overwrite the count with `val`.
    #[inline]
    pub(crate) fn set_count(&mut self, group_idx: u32, slot: usize, val: i64) {
        let s = self.field_start(group_idx, slot);
        self.buf[s..s + 8].copy_from_slice(&val.to_le_bytes());
    }

    // ----- SumInt slot (24 bytes: i128 sum + i64 count) -----

    /// Read (sum_i128, count).
    #[inline]
    pub(crate) fn read_sum_int(&self, group_idx: u32, slot: usize) -> (i128, i64) {
        let s = self.field_start(group_idx, slot);
        let sum = i128::from_le_bytes(self.buf[s..s + 16].try_into().unwrap());
        let count = i64::from_le_bytes(self.buf[s + 16..s + 24].try_into().unwrap());
        (sum, count)
    }

    /// Add `(sum_delta, count_delta)` to (sum, count).
    #[inline]
    pub(crate) fn add_sum_int(
        &mut self,
        group_idx: u32,
        slot: usize,
        sum_delta: i128,
        count_delta: i64,
    ) {
        let s = self.field_start(group_idx, slot);
        let sum_bytes: &mut [u8; 16] = (&mut self.buf[s..s + 16]).try_into().unwrap();
        *sum_bytes = (i128::from_le_bytes(*sum_bytes) + sum_delta).to_le_bytes();
        let count_bytes: &mut [u8; 8] = (&mut self.buf[s + 16..s + 24]).try_into().unwrap();
        *count_bytes = (i64::from_le_bytes(*count_bytes) + count_delta).to_le_bytes();
    }

    // ----- SumIntNarrow slot (16 bytes: i64 sum + i64 count) -----

    /// Read (sum_i64, count).
    #[inline]
    pub(crate) fn read_sum_int_narrow(&self, group_idx: u32, slot: usize) -> (i64, i64) {
        let s = self.field_start(group_idx, slot);
        let sum = i64::from_le_bytes(self.buf[s..s + 8].try_into().unwrap());
        let count = i64::from_le_bytes(self.buf[s + 8..s + 16].try_into().unwrap());
        (sum, count)
    }

    /// Add `(sum_delta, count_delta)` to the narrow (sum, count).
    #[inline]
    pub(crate) fn add_sum_int_narrow(
        &mut self,
        group_idx: u32,
        slot: usize,
        sum_delta: i64,
        count_delta: i64,
    ) {
        let s = self.field_start(group_idx, slot);
        let sum_bytes: &mut [u8; 8] = (&mut self.buf[s..s + 8]).try_into().unwrap();
        *sum_bytes = (i64::from_le_bytes(*sum_bytes) + sum_delta).to_le_bytes();
        let count_bytes: &mut [u8; 8] = (&mut self.buf[s + 8..s + 16]).try_into().unwrap();
        *count_bytes = (i64::from_le_bytes(*count_bytes) + count_delta).to_le_bytes();
    }

    // ----- SumFloat slot (16 bytes: f64 sum + i64 count) -----

    /// Read (sum_f64, count).
    #[inline]
    pub(crate) fn read_sum_float(&self, group_idx: u32, slot: usize) -> (f64, i64) {
        let s = self.field_start(group_idx, slot);
        let sum = f64::from_le_bytes(self.buf[s..s + 8].try_into().unwrap());
        let count = i64::from_le_bytes(self.buf[s + 8..s + 16].try_into().unwrap());
        (sum, count)
    }

    /// Add `(sum_delta, count_delta)` to the float (sum, count).
    #[inline]
    pub(crate) fn add_sum_float(
        &mut self,
        group_idx: u32,
        slot: usize,
        sum_delta: f64,
        count_delta: i64,
    ) {
        let s = self.field_start(group_idx, slot);
        let sum_bytes: &mut [u8; 8] = (&mut self.buf[s..s + 8]).try_into().unwrap();
        *sum_bytes = (f64::from_le_bytes(*sum_bytes) + sum_delta).to_le_bytes();
        let count_bytes: &mut [u8; 8] = (&mut self.buf[s + 8..s + 16]).try_into().unwrap();
        *count_bytes = (i64::from_le_bytes(*count_bytes) + count_delta).to_le_bytes();
    }

    // ----- MinStr / MaxStr slot (8 bytes: u32 arena_offset + u32 length) -----

    /// Read MinStr/MaxStr: returns (arena_offset, length). Sentinel is
    /// (u32::MAX, 0) = no value.
    #[inline]
    pub(crate) fn read_min_max_str(&self, group_idx: u32, slot: usize) -> (u32, u32) {
        let s = self.field_start(group_idx, slot);
        let off = u32::from_le_bytes(self.buf[s..s + 4].try_into().unwrap());
        let len = u32::from_le_bytes(self.buf[s + 4..s + 8].try_into().unwrap());
        (off, len)
    }

    /// Write MinStr/MaxStr arena offset and length.
    #[inline]
    pub(crate) fn write_min_max_str(&mut self, group_idx: u32, slot: usize, off: u32, len: u32) {
        let s = self.field_start(group_idx, slot);
        self.buf[s..s + 4].copy_from_slice(&off.to_le_bytes());
        self.buf[s + 4..s + 8].copy_from_slice(&len.to_le_bytes());
    }

    // ----- MinInt / MaxInt slot (16 bytes: i64 val + i64 has_value flag) -----

    /// Read MinInt/MaxInt: returns (value, has_value). `has_value=false`
    /// means no value has been observed yet (zero-init from `alloc_group`).
    #[inline]
    pub(crate) fn read_min_max_int(&self, group_idx: u32, slot: usize) -> (i64, bool) {
        let s = self.field_start(group_idx, slot);
        let val = i64::from_le_bytes(self.buf[s..s + 8].try_into().unwrap());
        let has = i64::from_le_bytes(self.buf[s + 8..s + 16].try_into().unwrap());
        (val, has != 0)
    }

    /// Write MinInt/MaxInt value + has_value flag.
    #[inline]
    pub(crate) fn write_min_max_int(&mut self, group_idx: u32, slot: usize, val: i64, has: bool) {
        let s = self.field_start(group_idx, slot);
        self.buf[s..s + 8].copy_from_slice(&val.to_le_bytes());
        self.buf[s + 8..s + 16].copy_from_slice(&i64::from(has).to_le_bytes());
    }

    /// Update MinInt: replace stored value if `candidate < stored` or no
    /// value yet.
    #[inline]
    pub(crate) fn update_min_int(&mut self, group_idx: u32, slot: usize, candidate: i64) {
        let (val, has) = self.read_min_max_int(group_idx, slot);
        if !has || candidate < val {
            self.write_min_max_int(group_idx, slot, candidate, true);
        }
    }

    /// Update MaxInt: replace stored value if `candidate > stored` or no
    /// value yet.
    #[inline]
    pub(crate) fn update_max_int(&mut self, group_idx: u32, slot: usize, candidate: i64) {
        let (val, has) = self.read_min_max_int(group_idx, slot);
        if !has || candidate > val {
            self.write_min_max_int(group_idx, slot, candidate, true);
        }
    }
}

/// Check if all aggregates can use the compact accumulator path.
pub(crate) fn can_use_compact_accs(agg_specs: &[AggExecSpec]) -> bool {
    if agg_specs.is_empty() {
        return false;
    }
    agg_specs.iter().all(|spec| match spec.agg_type {
        AggType::CountStar | AggType::Count => true,
        AggType::Sum | AggType::Avg => {
            let t = spec.col_type_oid;
            t == pg_sys::INT2OID
                || t == pg_sys::INT4OID
                || t == pg_sys::INT8OID
                || t == pg_sys::FLOAT4OID
                || t == pg_sys::FLOAT8OID
        }
        AggType::Min | AggType::Max => {
            let t = spec.col_type_oid;
            t == pg_sys::TEXTOID
                || t == pg_sys::VARCHAROID
                || t == pg_sys::BPCHAROID
                || t == pg_sys::INT2OID
                || t == pg_sys::INT4OID
                || t == pg_sys::INT8OID
                || t == pg_sys::DATEOID
                || t == pg_sys::TIMESTAMPOID
                || t == pg_sys::TIMESTAMPTZOID
        }
        AggType::CountDistinct => {
            let t = spec.col_type_oid;
            t == pg_sys::INT2OID
                || t == pg_sys::INT4OID
                || t == pg_sys::INT8OID
                || t == pg_sys::TEXTOID
                || t == pg_sys::VARCHAROID
                || t == pg_sys::BPCHAROID
        }
    })
}

/// Select the top-N (packed_key, group_idx) pairs by reading raw sort values
/// from compact storage, without full finalization.
///
/// Uses a BinaryHeap of size `limit` to find the top-N in O(n log limit) time.
pub(crate) fn compact_topn_select<S: std::hash::BuildHasher>(
    map: &hashbrown::HashMap<u128, u32, S>,
    storage: &CompactAccStorage,
    sort_slot: usize,
    limit: usize,
    ascending: bool,
    sort_is_avg: bool,
) -> Vec<(u128, u32)> {
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
        let mut result: Vec<(u128, u32)> =
            heap.into_iter().map(|Reverse((_, k, g))| (k, g)).collect();
        result.sort_by_key(|&(_, gb)| std::cmp::Reverse(read_val(gb)));
        result
    }
}

/// Finalize a compact accumulator slot into a (Datum, is_null) pair.
///
/// # Safety
///
/// Inherits the `CompactAccStorage` accessor contract (see the
/// struct's safety section). Also must run inside an active PG
/// transaction — NUMERIC paths palloc via PG FFI.
pub(crate) unsafe fn compact_finalize(
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
pub(crate) fn compact_emit_partial(
    storage: &CompactAccStorage,
    group_idx: u32,
    slot: usize,
    spec: &AggExecSpec,
) -> (pg_sys::Datum, bool) {
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

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::super::cd_set::{new_cd_set_int, new_cd_set_str};
    use super::super::state::{AggAccumulator, AggExecSpec, AggExpr, AggType, OutputTransform};
    use super::*;
    use pgrx::pg_sys;
    use pgrx::prelude::*;

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

    // -------------------------------------------------------------------
    // datum_to_i128 tests
    // -------------------------------------------------------------------

    #[test]
    fn test_datum_to_i128_int2() {
        let d = pg_sys::Datum::from(-5i16 as usize);
        assert_eq!(datum_to_i128(d, pg_sys::INT2OID), -5);
    }

    #[test]
    fn test_datum_to_i128_int4() {
        let d = pg_sys::Datum::from(-100_000i32 as usize);
        assert_eq!(datum_to_i128(d, pg_sys::INT4OID), -100_000);
    }

    #[test]
    fn test_datum_to_i128_int8() {
        let d = pg_sys::Datum::from(-9_000_000_000i64 as usize);
        assert_eq!(datum_to_i128(d, pg_sys::INT8OID), -9_000_000_000);
    }

    #[test]
    fn test_datum_to_i128_unknown_oid() {
        // Falls through to raw usize cast
        let d = pg_sys::Datum::from(42usize);
        assert_eq!(datum_to_i128(d, pg_sys::Oid::from(9999u32)), 42);
    }

    // -------------------------------------------------------------------
    // datum_to_f64 tests
    // -------------------------------------------------------------------

    #[test]
    fn test_datum_to_f64_float4() {
        let f: f32 = 1.5;
        let d = pg_sys::Datum::from(f.to_bits() as usize);
        let result = datum_to_f64(d, pg_sys::FLOAT4OID);
        assert!((result - 1.5f64).abs() < 0.001);
    }

    #[test]
    fn test_datum_to_f64_float8() {
        let f: f64 = 1.23456789;
        let d = pg_sys::Datum::from(f.to_bits() as usize);
        let result = datum_to_f64(d, pg_sys::FLOAT8OID);
        assert!((result - 1.23456789).abs() < 1e-9);
    }

    #[test]
    fn test_datum_to_f64_unknown_oid() {
        let d = pg_sys::Datum::from(100usize);
        assert_eq!(datum_to_f64(d, pg_sys::Oid::from(9999u32)), 100.0);
    }

    // -------------------------------------------------------------------
    // StringArena tests
    // -------------------------------------------------------------------

    #[test]
    fn test_arena_alloc_and_get() {
        let mut arena = StringArena::new();
        let (off, len) = arena.alloc("hello");
        assert_eq!(arena.get(off, len), "hello");
    }

    #[test]
    fn test_arena_multiple_allocs() {
        let mut arena = StringArena::new();
        let (o1, l1) = arena.alloc("foo");
        let (o2, l2) = arena.alloc("bar");
        let (o3, l3) = arena.alloc("baz");
        assert_eq!(arena.get(o1, l1), "foo");
        assert_eq!(arena.get(o2, l2), "bar");
        assert_eq!(arena.get(o3, l3), "baz");
    }

    #[test]
    fn test_arena_empty_string() {
        let mut arena = StringArena::new();
        let (off, len) = arena.alloc("");
        assert_eq!(arena.get(off, len), "");
    }

    #[test]
    fn test_arena_unicode() {
        let mut arena = StringArena::new();
        let (off, len) = arena.alloc("hello\u{00e9}world");
        assert_eq!(arena.get(off, len), "hello\u{00e9}world");
    }

    // -------------------------------------------------------------------
    // finalize_accumulator tests
    //
    // Stays `#[pg_test]` because some branches (SumInt → NUMERIC, Avg)
    // allocate PG numeric datums via `pg_sys` and need a live backend.
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
        let acc = AggAccumulator::SumInt {
            sum: 100_000,
            count: 10,
        };
        let spec = make_agg_spec(AggType::Sum, 0, 23); // INT4OID
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        assert_eq!(datum.value() as i64, 100_000);
    }

    #[pg_test]
    fn test_finalize_sum_int8_returns_numeric() {
        // SUM(int8) → NUMERIC
        let acc = AggAccumulator::SumInt {
            sum: 999_999_999,
            count: 5,
        };
        let spec = make_agg_spec(AggType::Sum, 0, 20); // INT8OID
        let (datum, is_null) = unsafe { finalize_accumulator(&acc, &spec) };
        assert!(!is_null);
        // Verify via numeric_out
        let s = unsafe {
            let cstr = pg_sys::OidOutputFunctionCall(pg_sys::Oid::from(1702u32), datum);
            let s = std::ffi::CStr::from_ptr(cstr)
                .to_string_lossy()
                .into_owned();
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
            let s = std::ffi::CStr::from_ptr(cstr)
                .to_string_lossy()
                .into_owned();
            pg_sys::pfree(cstr as *mut _);
            s
        };
        assert_eq!(s, "25.0000000000000000");
    }

    #[pg_test]
    fn test_finalize_avg_float() {
        // AVG(float8) → FLOAT8 (sum/count)
        let acc = AggAccumulator::SumFloat {
            sum: 10.0,
            count: 4,
        };
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
}
