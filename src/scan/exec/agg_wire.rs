//! DSM wire format for serialised per-worker partial aggregate results.
//!
//! Phase C.2 of `dev/docs/PARALLEL_AGG.md` makes `DeltaXAgg` parallel-aware.
//! Each worker accumulates a `ParallelCompactResult` into a per-process
//! [`AggScanState`] and, on `ExecCustomScan` first-call, serialises it into a
//! 32 MB DSM slab (`DeltaXAggPState::partial_lens[k]` records the byte count).
//! The leader spin-waits on `populated == 1`, deserialises each slab, and
//! merges via `merge_compact_results`.
//!
//! Layout (one slab, offset-addressed from the slab base):
//!
//! ```text
//! ┌──────────────────────────────────────────┐ 0
//! │ PartialMeta (fixed header)               │  magic, version, flags, telemetry,
//! │                                          │  sub-region offsets/lengths
//! ├──────────────────────────────────────────┤ group_map_off
//! │ group_map: 20 bytes × num_groups         │  (u128 packed_key, u32 group_idx)
//! │                                          │  unsorted (merge is order-independent)
//! ├──────────────────────────────────────────┤ storage_off
//! │ CompactAccStorage.buf                    │  opaque bytes — group_stride × num_groups
//! ├──────────────────────────────────────────┤ str_arena_off
//! │ StringArena.buf                          │  opaque bytes — MIN/MAX text payloads
//! ├──────────────────────────────────────────┤ topk_off (only when HAS_TOPK)
//! │ topk keys: u128 × topk_num_keys          │
//! └──────────────────────────────────────────┘
//! ```
//!
//! `CompactAccLayout` is **not** carried on-wire — both leader and workers
//! receive the same `agg_specs` via `custom_private` and rebuild it
//! deterministically with `CompactAccLayout::new(agg_specs)`. The wire format
//! does record per-slot `(offset, kind)` pairs so the deserialiser can
//! validate the recovered layout matches the sender's (catches drift if the
//! layout function ever becomes non-deterministic).
//!
//! `cd_sidecar` (COUNT(DISTINCT)) is **excluded** in V1 — Phase D revisits
//! when the parallel CountDistinct path lands. The header bit is reserved.
//!
//! All multi-byte fields use the host endianness; pgrx targets x86-64 /
//! aarch64 (both LE) so this is safe. u128 keys are read/written via
//! `copy_nonoverlapping` to sidestep alignment concerns.
//!
//! Top-level `dead_code` allow: the public surface (`serialize_partial_into`,
//! `deserialize_partial`, error types) is consumed by Phase C.2.d/e and not
//! yet wired in this commit. Tests in this file exercise the full surface.

#![allow(dead_code)]

use std::hash::BuildHasherDefault;

use super::agg::{
    AggExecSpec, CompactAccLayout, CompactAccStorage, CompactGroupMap, CountDistinctSideCar,
    ParallelCompactResult,
};

/// Magic at the start of every partial slab: ASCII "DXPW".
pub(super) const PARTIAL_MAGIC: u32 = 0x44_58_50_57;
/// V1: layout described in module docs.
pub(super) const PARTIAL_VERSION: u32 = 1;

/// Bit 0: top-K candidates present.
const FLAG_HAS_TOPK: u32 = 1 << 0;
/// Bit 1: cd_sidecar present (reserved — Phase D).
#[allow(dead_code)]
const FLAG_HAS_CD_SIDECAR: u32 = 1 << 1;

/// On-wire stride of a group_map entry: `(u128 key, u32 idx)`.
const GROUP_MAP_ENTRY_BYTES: u32 = 20;

/// Per-slot validation record: `(u32 offset, u8 kind, [u8;3] pad)`.
const SLOT_RECORD_BYTES: u32 = 8;

/// Fixed-size header at the start of a serialised partial result. `#[repr(C)]`
/// so workers and leader (same binary) agree byte-for-byte on layout.
#[repr(C)]
#[derive(Debug, Default)]
pub(super) struct PartialMeta {
    pub(super) magic: u32,
    pub(super) version: u32,
    pub(super) flags: u32,
    pub(super) num_groups: u32,
    /// Used only when `FLAG_HAS_TOPK` is set; otherwise zero.
    pub(super) topk_num_keys: u32,
    /// Padding so subsequent i64/u64 fields land on 8-byte alignment.
    _pad0: u32,
    /// `topk` floor; only meaningful when `FLAG_HAS_TOPK` is set.
    pub(super) topk_floor: i64,
    pub(super) segments_processed: u64,
    pub(super) rows_processed: u64,
    pub(super) decompress_us: u64,
    /// Number of agg slots — used to validate against the receiver's `agg_specs.len()`.
    pub(super) num_slots: u32,
    /// Group stride from the sender's `CompactAccLayout` — validates layout determinism.
    pub(super) group_stride: u32,
    pub(super) slots_off: u32,
    pub(super) slots_len: u32,
    pub(super) group_map_off: u32,
    pub(super) group_map_len: u32,
    pub(super) storage_off: u32,
    pub(super) storage_len: u32,
    pub(super) str_arena_off: u32,
    pub(super) str_arena_len: u32,
    pub(super) topk_off: u32,
    pub(super) topk_len: u32,
    /// Total bytes written including the header. Sanity check on receive.
    pub(super) total_size: u32,
    _pad1: u32,
}

const HEADER_SIZE: u32 = std::mem::size_of::<PartialMeta>() as u32;

const _: () = assert!(std::mem::size_of::<PartialMeta>().is_multiple_of(8));
const _: () = assert!(std::mem::align_of::<PartialMeta>() == 8);

/// Computed layout for a `ParallelCompactResult`. Total size includes the
/// header.
#[derive(Debug, Default, Clone)]
pub(super) struct PartialLayout {
    pub(super) slots_off: u32,
    pub(super) slots_len: u32,
    pub(super) group_map_off: u32,
    pub(super) group_map_len: u32,
    pub(super) storage_off: u32,
    pub(super) storage_len: u32,
    pub(super) str_arena_off: u32,
    pub(super) str_arena_len: u32,
    pub(super) topk_off: u32,
    pub(super) topk_len: u32,
    pub(super) total_size: u32,
    pub(super) flags: u32,
    pub(super) topk_num_keys: u32,
}

#[derive(Debug)]
#[allow(dead_code)] // exercised by tests + future EXPLAIN renders
pub(super) enum SerError {
    /// The layout requires more bytes than the slab capacity.
    Overflow { needed: u32, have: u32 },
}

#[derive(Debug, PartialEq)]
#[allow(dead_code)] // exercised by tests + future error reporting
pub(super) enum DeError {
    Truncated {
        needed: u32,
        have: u64,
    },
    MagicMismatch,
    VersionMismatch {
        got: u32,
        expected: u32,
    },
    /// Sender's layout disagrees with receiver's reconstruction. Indicates
    /// `CompactAccLayout::new(agg_specs)` is non-deterministic or the receiver
    /// was given a different `agg_specs` than the sender.
    LayoutMismatch,
}

fn round_up(n: u32, align: u32) -> u32 {
    (n + align - 1) & !(align - 1)
}

/// Compute the on-wire layout. Total size includes the header.
pub(super) fn layout_partial(
    result: &ParallelCompactResult,
    agg_specs: &[AggExecSpec],
) -> PartialLayout {
    debug_assert_eq!(result.compact_storage.layout.slots.len(), agg_specs.len());

    let num_groups = result.compact_map.len() as u32;
    let stride = result.compact_storage.layout.group_stride as u32;

    // 4-byte aligned for u32 records.
    let slots_off = HEADER_SIZE;
    let slots_len = (agg_specs.len() as u32) * SLOT_RECORD_BYTES;

    // Group map entries are 20 bytes each, accessed via copy_nonoverlapping
    // → no alignment requirement. Keep the region 4-byte aligned for
    // readability in hex dumps.
    let group_map_off = round_up(slots_off + slots_len, 4);
    let group_map_len = num_groups * GROUP_MAP_ENTRY_BYTES;

    // Storage buf is a flat copy of `CompactAccStorage.buf` — group_stride is
    // 16-aligned (i128 may be the first field per group), so align region to
    // 16 to preserve in-buf alignment.
    let storage_off = round_up(group_map_off + group_map_len, 16);
    let storage_len = num_groups * stride;
    debug_assert_eq!(storage_len as usize, result.compact_storage.buf.len());

    // String arena is opaque bytes — read via .get(off, len) on the receiver.
    let str_arena_off = round_up(storage_off + storage_len, 4);
    let str_arena_len = result.compact_storage.str_arena.buf.len() as u32;

    let mut flags: u32 = 0;
    let mut topk_num_keys: u32 = 0;
    let topk_off;
    let topk_len;
    if let Some((keys, _floor)) = &result.topk {
        flags |= FLAG_HAS_TOPK;
        topk_num_keys = keys.len() as u32;
        topk_off = round_up(str_arena_off + str_arena_len, 16);
        topk_len = topk_num_keys * 16; // u128 each
    } else {
        topk_off = 0;
        topk_len = 0;
    }

    let total_size = if topk_len > 0 {
        topk_off + topk_len
    } else {
        str_arena_off + str_arena_len
    };

    PartialLayout {
        slots_off,
        slots_len,
        group_map_off,
        group_map_len,
        storage_off,
        storage_len,
        str_arena_off,
        str_arena_len,
        topk_off,
        topk_len,
        total_size,
        flags,
        topk_num_keys,
    }
}

/// Serialise `result` into the slab at `slab` (capacity `cap`). Returns the
/// number of bytes written on success.
///
/// SAFETY: caller owns the slab for at least the DSM-attach window. Bytes are
/// written via raw pointers without invoking `Drop` on the destination.
pub(super) unsafe fn serialize_partial_into(
    slab: *mut u8,
    cap: u32,
    result: &ParallelCompactResult,
    agg_specs: &[AggExecSpec],
) -> Result<u32, SerError> {
    let layout = layout_partial(result, agg_specs);
    if layout.total_size > cap {
        return Err(SerError::Overflow {
            needed: layout.total_size,
            have: cap,
        });
    }

    unsafe {
        // Zero region so reserved fields and padding are well-defined.
        std::ptr::write_bytes(slab, 0, layout.total_size as usize);

        // ---------------- Header ----------------
        let hdr = slab as *mut PartialMeta;
        (*hdr).magic = PARTIAL_MAGIC;
        (*hdr).version = PARTIAL_VERSION;
        (*hdr).flags = layout.flags;
        (*hdr).num_groups = result.compact_map.len() as u32;
        (*hdr).topk_num_keys = layout.topk_num_keys;
        (*hdr).topk_floor = result.topk.as_ref().map(|(_, f)| *f).unwrap_or(0);
        (*hdr).segments_processed = result.segments_processed;
        (*hdr).rows_processed = result.rows_processed;
        (*hdr).decompress_us = result.decompress_us;
        (*hdr).num_slots = agg_specs.len() as u32;
        (*hdr).group_stride = result.compact_storage.layout.group_stride as u32;
        (*hdr).slots_off = layout.slots_off;
        (*hdr).slots_len = layout.slots_len;
        (*hdr).group_map_off = layout.group_map_off;
        (*hdr).group_map_len = layout.group_map_len;
        (*hdr).storage_off = layout.storage_off;
        (*hdr).storage_len = layout.storage_len;
        (*hdr).str_arena_off = layout.str_arena_off;
        (*hdr).str_arena_len = layout.str_arena_len;
        (*hdr).topk_off = layout.topk_off;
        (*hdr).topk_len = layout.topk_len;
        (*hdr).total_size = layout.total_size;

        // ---------------- Slot records ----------------
        let slots_ptr = slab.add(layout.slots_off as usize);
        for (i, (off, kind)) in result.compact_storage.layout.slots.iter().enumerate() {
            let entry = slots_ptr.add(i * SLOT_RECORD_BYTES as usize);
            // [u32 offset][u8 kind][u8 _pad0][u8 _pad1][u8 _pad2]
            let off_u32 = *off as u32;
            std::ptr::copy_nonoverlapping(&off_u32 as *const u32 as *const u8, entry, 4);
            *entry.add(4) = kind.wire_tag();
        }

        // ---------------- Group map ----------------
        let gm_ptr = slab.add(layout.group_map_off as usize);
        for (i, (key, idx)) in result.compact_map.iter().enumerate() {
            let entry = gm_ptr.add(i * GROUP_MAP_ENTRY_BYTES as usize);
            std::ptr::copy_nonoverlapping(key as *const u128 as *const u8, entry, 16);
            std::ptr::copy_nonoverlapping(idx as *const u32 as *const u8, entry.add(16), 4);
        }

        // ---------------- Storage buf ----------------
        if layout.storage_len > 0 {
            std::ptr::copy_nonoverlapping(
                result.compact_storage.buf.as_ptr(),
                slab.add(layout.storage_off as usize),
                layout.storage_len as usize,
            );
        }

        // ---------------- Str arena buf ----------------
        if layout.str_arena_len > 0 {
            std::ptr::copy_nonoverlapping(
                result.compact_storage.str_arena.buf.as_ptr(),
                slab.add(layout.str_arena_off as usize),
                layout.str_arena_len as usize,
            );
        }

        // ---------------- topk keys ----------------
        if layout.topk_len > 0 {
            let keys = &result.topk.as_ref().unwrap().0;
            let topk_ptr = slab.add(layout.topk_off as usize);
            for (i, k) in keys.iter().enumerate() {
                std::ptr::copy_nonoverlapping(
                    k as *const u128 as *const u8,
                    topk_ptr.add(i * 16),
                    16,
                );
            }
        }
    }

    Ok(layout.total_size)
}

/// Deserialise a slab previously written by `serialize_partial_into`. The
/// resulting `ParallelCompactResult` owns its `compact_map` /
/// `compact_storage` / `cd_sidecar`; the slab is no longer referenced after
/// this call returns.
///
/// SAFETY: caller guarantees `slab` points to at least `len` bytes that were
/// written by `serialize_partial_into` with a matching `agg_specs`.
pub(super) unsafe fn deserialize_partial(
    slab: *const u8,
    len: u64,
    agg_specs: &[AggExecSpec],
) -> Result<ParallelCompactResult, DeError> {
    if len < HEADER_SIZE as u64 {
        return Err(DeError::Truncated {
            needed: HEADER_SIZE,
            have: len,
        });
    }

    unsafe {
        let hdr = &*(slab as *const PartialMeta);
        if hdr.magic != PARTIAL_MAGIC {
            return Err(DeError::MagicMismatch);
        }
        if hdr.version != PARTIAL_VERSION {
            return Err(DeError::VersionMismatch {
                got: hdr.version,
                expected: PARTIAL_VERSION,
            });
        }
        if (hdr.total_size as u64) > len {
            return Err(DeError::Truncated {
                needed: hdr.total_size,
                have: len,
            });
        }
        if hdr.num_slots as usize != agg_specs.len() {
            return Err(DeError::LayoutMismatch);
        }

        // Rebuild `CompactAccLayout` from `agg_specs` and validate against the
        // sender's slot records. If they disagree, `CompactAccLayout::new` has
        // become non-deterministic — bail with LayoutMismatch.
        let recv_layout = CompactAccLayout::new(agg_specs);
        if recv_layout.group_stride as u32 != hdr.group_stride {
            return Err(DeError::LayoutMismatch);
        }
        let slots_ptr = slab.add(hdr.slots_off as usize);
        for (i, (off, kind)) in recv_layout.slots.iter().enumerate() {
            let entry = slots_ptr.add(i * SLOT_RECORD_BYTES as usize);
            let mut off_bytes = [0u8; 4];
            std::ptr::copy_nonoverlapping(entry, off_bytes.as_mut_ptr(), 4);
            let sender_off = u32::from_ne_bytes(off_bytes);
            let sender_tag = *entry.add(4);
            if sender_off != *off as u32 || sender_tag != kind.wire_tag() {
                return Err(DeError::LayoutMismatch);
            }
        }

        // Group map.
        let gm_ptr = slab.add(hdr.group_map_off as usize);
        let num_groups = hdr.num_groups as usize;
        let mut compact_map =
            CompactGroupMap::with_capacity_and_hasher(num_groups, BuildHasherDefault::default());
        for i in 0..num_groups {
            let entry = gm_ptr.add(i * GROUP_MAP_ENTRY_BYTES as usize);
            let mut key_bytes = [0u8; 16];
            std::ptr::copy_nonoverlapping(entry, key_bytes.as_mut_ptr(), 16);
            let key = u128::from_ne_bytes(key_bytes);
            let mut idx_bytes = [0u8; 4];
            std::ptr::copy_nonoverlapping(entry.add(16), idx_bytes.as_mut_ptr(), 4);
            let idx = u32::from_ne_bytes(idx_bytes);
            compact_map.insert(key, idx);
        }

        // Storage buf — opaque copy.
        let mut storage_buf = vec![0u8; hdr.storage_len as usize];
        if hdr.storage_len > 0 {
            std::ptr::copy_nonoverlapping(
                slab.add(hdr.storage_off as usize),
                storage_buf.as_mut_ptr(),
                hdr.storage_len as usize,
            );
        }

        // Str arena buf — opaque copy. SAFETY contract: offsets stored inside
        // CompactAccStorage.buf for MinStr/MaxStr slots refer to byte offsets
        // in this str_arena_buf. `merge_compact_results` reads worker offsets
        // via worker.str_arena.get(off, len) without rebasing, so the
        // deserialised arena must reproduce the worker's local arena exactly.
        let mut str_arena_buf = vec![0u8; hdr.str_arena_len as usize];
        if hdr.str_arena_len > 0 {
            std::ptr::copy_nonoverlapping(
                slab.add(hdr.str_arena_off as usize),
                str_arena_buf.as_mut_ptr(),
                hdr.str_arena_len as usize,
            );
        }

        // topk
        let topk = if (hdr.flags & FLAG_HAS_TOPK) != 0 {
            let topk_ptr = slab.add(hdr.topk_off as usize);
            let mut keys = Vec::with_capacity(hdr.topk_num_keys as usize);
            for i in 0..hdr.topk_num_keys as usize {
                let mut key_bytes = [0u8; 16];
                std::ptr::copy_nonoverlapping(topk_ptr.add(i * 16), key_bytes.as_mut_ptr(), 16);
                keys.push(u128::from_ne_bytes(key_bytes));
            }
            Some((keys, hdr.topk_floor))
        } else {
            None
        };

        let compact_storage =
            CompactAccStorage::from_parts(recv_layout, storage_buf, str_arena_buf);

        // cd_sidecar is empty in V1 (Phase D wires it up).
        let cd_sidecar = CountDistinctSideCar::new(agg_specs);

        Ok(ParallelCompactResult {
            compact_map,
            compact_storage,
            cd_sidecar,
            segments_processed: hdr.segments_processed,
            rows_processed: hdr.rows_processed,
            decompress_us: hdr.decompress_us,
            topk,
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use crate::scan::exec::agg::{AggExpr, AggType, OutputTransform};
    use pgrx::pg_sys;

    fn count_star_specs() -> Vec<AggExecSpec> {
        vec![AggExecSpec {
            agg_type: AggType::CountStar,
            col_idx: -1,
            col_type_oid: pg_sys::INT8OID,
            expr_kind: AggExpr::Column,
            const_offset: 0,
            is_partial: false,
            transtype_oid: pg_sys::InvalidOid,
            output_transform: OutputTransform::None,
        }]
    }

    fn sum_int4_count_specs() -> Vec<AggExecSpec> {
        vec![
            AggExecSpec {
                agg_type: AggType::Sum,
                col_idx: 0,
                col_type_oid: pg_sys::INT4OID,
                expr_kind: AggExpr::Column,
                const_offset: 0,
                is_partial: false,
                transtype_oid: pg_sys::InvalidOid,
                output_transform: OutputTransform::None,
            },
            AggExecSpec {
                agg_type: AggType::CountStar,
                col_idx: -1,
                col_type_oid: pg_sys::INT8OID,
                expr_kind: AggExpr::Column,
                const_offset: 0,
                is_partial: false,
                transtype_oid: pg_sys::InvalidOid,
                output_transform: OutputTransform::None,
            },
        ]
    }

    fn min_text_specs() -> Vec<AggExecSpec> {
        vec![AggExecSpec {
            agg_type: AggType::Min,
            col_idx: 0,
            col_type_oid: pg_sys::TEXTOID,
            expr_kind: AggExpr::Column,
            const_offset: 0,
            is_partial: false,
            transtype_oid: pg_sys::InvalidOid,
            output_transform: OutputTransform::None,
        }]
    }

    /// Construct a result with `groups` (key → count) using the count-star spec.
    fn mk_count_result(groups: &[(u128, i64)]) -> ParallelCompactResult {
        let specs = count_star_specs();
        let mut r = ParallelCompactResult::empty(&specs);
        for &(key, count) in groups {
            let idx = r.compact_storage.alloc_group();
            r.cd_sidecar.alloc_group();
            r.compact_map.insert(key, idx);
            r.compact_storage.set_count(idx, 0, count);
        }
        r
    }

    #[test]
    fn partial_wire_empty() {
        let specs = count_star_specs();
        let r = ParallelCompactResult::empty(&specs);
        let layout = layout_partial(&r, &specs);
        let mut buf = vec![0u8; layout.total_size as usize];
        unsafe {
            let written =
                serialize_partial_into(buf.as_mut_ptr(), buf.len() as u32, &r, &specs).unwrap();
            assert_eq!(written, layout.total_size);
            let back = deserialize_partial(buf.as_ptr(), written as u64, &specs).unwrap();
            assert_eq!(back.compact_map.len(), 0);
            assert_eq!(back.segments_processed, 0);
            assert_eq!(back.rows_processed, 0);
            assert!(back.topk.is_none());
        }
    }

    #[test]
    fn partial_wire_single_group() {
        let specs = count_star_specs();
        let mut r = mk_count_result(&[(0xCAFE, 42)]);
        r.segments_processed = 7;
        r.rows_processed = 1234;
        r.decompress_us = 999;
        let layout = layout_partial(&r, &specs);
        let mut buf = vec![0u8; layout.total_size as usize];
        unsafe {
            serialize_partial_into(buf.as_mut_ptr(), buf.len() as u32, &r, &specs).unwrap();
            let back = deserialize_partial(buf.as_ptr(), layout.total_size as u64, &specs).unwrap();
            assert_eq!(back.compact_map.len(), 1);
            let &gidx = back.compact_map.get(&0xCAFE).unwrap();
            assert_eq!(back.compact_storage.read_count(gidx, 0), 42);
            assert_eq!(back.segments_processed, 7);
            assert_eq!(back.rows_processed, 1234);
            assert_eq!(back.decompress_us, 999);
        }
    }

    #[test]
    fn partial_wire_multi_group_with_strings() {
        let specs = min_text_specs();
        let mut r = ParallelCompactResult::empty(&specs);
        // Three groups, each carrying a MIN(text) value.
        for (key, s) in [(0x11u128, "alpha"), (0x22, "beta"), (0x33, "gamma")] {
            let idx = r.compact_storage.alloc_group();
            r.cd_sidecar.alloc_group();
            r.compact_map.insert(key, idx);
            let (off, len) = r.compact_storage.str_arena.alloc(s);
            r.compact_storage.write_min_max_str(idx, 0, off, len);
        }
        let layout = layout_partial(&r, &specs);
        let mut buf = vec![0u8; layout.total_size as usize];
        unsafe {
            serialize_partial_into(buf.as_mut_ptr(), buf.len() as u32, &r, &specs).unwrap();
            let back = deserialize_partial(buf.as_ptr(), layout.total_size as u64, &specs).unwrap();
            assert_eq!(back.compact_map.len(), 3);
            for (key, expected) in [(0x11u128, "alpha"), (0x22, "beta"), (0x33, "gamma")] {
                let &gidx = back.compact_map.get(&key).unwrap();
                let (off, len) = back.compact_storage.read_min_max_str(gidx, 0);
                assert_eq!(back.compact_storage.str_arena.get(off, len), expected);
            }
        }
    }

    #[test]
    fn partial_wire_topk_present() {
        let specs = sum_int4_count_specs();
        let mut r = ParallelCompactResult::empty(&specs);
        let idx = r.compact_storage.alloc_group();
        r.cd_sidecar.alloc_group();
        r.compact_map.insert(0xAA, idx);
        r.topk = Some((vec![0xAAu128, 0xBB, 0xCC], 99));
        let layout = layout_partial(&r, &specs);
        assert_ne!(layout.topk_off, 0);
        let mut buf = vec![0u8; layout.total_size as usize];
        unsafe {
            serialize_partial_into(buf.as_mut_ptr(), buf.len() as u32, &r, &specs).unwrap();
            let back = deserialize_partial(buf.as_ptr(), layout.total_size as u64, &specs).unwrap();
            let (keys, floor) = back.topk.unwrap();
            assert_eq!(keys, vec![0xAAu128, 0xBB, 0xCC]);
            assert_eq!(floor, 99);
        }
    }

    #[test]
    fn partial_wire_overflow() {
        let specs = count_star_specs();
        let r = mk_count_result(&[(1, 1), (2, 2), (3, 3)]);
        let layout = layout_partial(&r, &specs);
        // Cap one byte short of needed.
        let mut buf = vec![0u8; layout.total_size as usize];
        unsafe {
            let res = serialize_partial_into(buf.as_mut_ptr(), layout.total_size - 1, &r, &specs);
            match res {
                Err(SerError::Overflow { needed, have }) => {
                    assert_eq!(needed, layout.total_size);
                    assert_eq!(have, layout.total_size - 1);
                }
                Ok(_) => panic!("expected Overflow"),
            }
        }
    }

    #[test]
    fn partial_wire_magic_mismatch() {
        let specs = count_star_specs();
        let buf = vec![0u8; HEADER_SIZE as usize];
        unsafe {
            let res = deserialize_partial(buf.as_ptr(), buf.len() as u64, &specs);
            assert_eq!(res.err(), Some(DeError::MagicMismatch));
        }
    }

    #[test]
    fn partial_wire_telemetry_roundtrip() {
        let specs = count_star_specs();
        let mut r = mk_count_result(&[(0x1, 1), (0x2, 2), (0x3, 3), (0x4, 4)]);
        r.segments_processed = 13;
        r.rows_processed = 100_000;
        r.decompress_us = 555_555;
        let layout = layout_partial(&r, &specs);
        let mut buf = vec![0u8; layout.total_size as usize];
        unsafe {
            serialize_partial_into(buf.as_mut_ptr(), buf.len() as u32, &r, &specs).unwrap();
            let back = deserialize_partial(buf.as_ptr(), layout.total_size as u64, &specs).unwrap();
            assert_eq!(back.segments_processed, 13);
            assert_eq!(back.rows_processed, 100_000);
            assert_eq!(back.decompress_us, 555_555);
            assert_eq!(back.compact_map.len(), 4);
            for (k, expected) in [(0x1u128, 1i64), (0x2, 2), (0x3, 3), (0x4, 4)] {
                let gidx = *back.compact_map.get(&k).unwrap();
                assert_eq!(back.compact_storage.read_count(gidx, 0), expected);
            }
        }
    }

    #[test]
    fn round_up_handles_alignment() {
        // 8-byte align (storage region must be u64-aligned).
        assert_eq!(round_up(0, 8), 0);
        assert_eq!(round_up(1, 8), 8);
        assert_eq!(round_up(7, 8), 8);
        assert_eq!(round_up(8, 8), 8);
        assert_eq!(round_up(9, 8), 16);
        // 16-byte align (i128 accumulator first-field constraint).
        assert_eq!(round_up(15, 16), 16);
        assert_eq!(round_up(16, 16), 16);
        assert_eq!(round_up(17, 16), 32);
    }

    #[test]
    fn partial_wire_version_mismatch_rejected() {
        // Serialise V1, hand-corrupt the version field, attach must fail.
        // Without this guard, a future V2 leader feeds a V1 worker garbage.
        let specs = count_star_specs();
        let r = mk_count_result(&[(1, 1)]);
        let layout = layout_partial(&r, &specs);
        let mut buf = vec![0u8; layout.total_size as usize];
        unsafe {
            serialize_partial_into(buf.as_mut_ptr(), buf.len() as u32, &r, &specs).unwrap();
            // Bump version to provoke the mismatch path.
            let hdr = buf.as_mut_ptr() as *mut PartialMeta;
            (*hdr).version = PARTIAL_VERSION.wrapping_add(1);
            let res = deserialize_partial(buf.as_ptr(), layout.total_size as u64, &specs);
            match res {
                Err(DeError::VersionMismatch { got, expected }) => {
                    assert_eq!(got, PARTIAL_VERSION.wrapping_add(1));
                    assert_eq!(expected, PARTIAL_VERSION);
                }
                Err(other) => panic!("expected VersionMismatch, got {:?}", other),
                Ok(_) => panic!("expected VersionMismatch, got Ok"),
            }
        }
    }

    #[test]
    fn partial_wire_truncated_buffer_rejected() {
        // Slab shorter than the header must surface as Truncated rather
        // than reading past the buffer.
        let specs = count_star_specs();
        let short_buf = vec![0u8; (HEADER_SIZE - 1) as usize];
        unsafe {
            let res = deserialize_partial(short_buf.as_ptr(), short_buf.len() as u64, &specs);
            assert!(matches!(res, Err(DeError::Truncated { .. })));
        }
    }

    #[test]
    fn partial_wire_slot_count_mismatch_rejected() {
        // Sender + receiver must agree on agg_specs.len(). If a worker
        // started with 1 spec and the leader reconstructs with 2, the
        // accumulator layout is incompatible and the merge step would
        // silently overrun group storage.
        let sender_specs = count_star_specs();
        let r = mk_count_result(&[(1, 1)]);
        let layout = layout_partial(&r, &sender_specs);
        let mut buf = vec![0u8; layout.total_size as usize];
        unsafe {
            serialize_partial_into(buf.as_mut_ptr(), buf.len() as u32, &r, &sender_specs).unwrap();
            // Receiver tries to decode with sum+count specs (2 slots, not 1).
            let receiver_specs = sum_int4_count_specs();
            let res = deserialize_partial(buf.as_ptr(), layout.total_size as u64, &receiver_specs);
            assert_eq!(res.err(), Some(DeError::LayoutMismatch));
        }
    }
}
