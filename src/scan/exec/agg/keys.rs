//! Packed integer GROUP BY keys: stash up to two `i64` group-by values
//! into a single `u128` so the compact-aggregation hot path can index a
//! `HashMap<u128, u32>` instead of allocating a `Vec<i64>` per row.

use std::hash::BuildHasherDefault;

use pgrx::pg_sys;

use super::{GroupByColSpec, GroupByExpr};

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
pub(super) fn can_use_compact_keys(group_specs: &[GroupByColSpec], col_not_null: &[bool]) -> bool {
    if group_specs.is_empty() || group_specs.len() > 2 {
        return false; // u128 fits at most 2 x i64
    }
    group_specs.iter().all(|gs| match &gs.expr {
        GroupByExpr::Column => {
            if !col_not_null
                .get(gs.col_idx as usize)
                .copied()
                .unwrap_or(false)
            {
                return false;
            }
            let t = gs.type_oid;
            t == pg_sys::INT2OID
                || t == pg_sys::INT4OID
                || t == pg_sys::INT8OID
                || t == pg_sys::TIMESTAMPOID
                || t == pg_sys::TIMESTAMPTZOID
        }
        GroupByExpr::DateTrunc { .. }
        | GroupByExpr::Extract { .. }
        | GroupByExpr::AddConst { .. } => col_not_null
            .get(gs.col_idx as usize)
            .copied()
            .unwrap_or(false),
        GroupByExpr::RegexpReplace { .. } => false,
        GroupByExpr::CaseWhen(_) => false,
    })
}

/// Pack up to 2 int64 keys into a u128.
#[inline]
pub(super) fn pack_int_keys_2(k0: i64, k1: i64) -> u128 {
    (k0 as u64 as u128) | ((k1 as u64 as u128) << 64)
}

/// Pack a single int64 key into u128.
#[inline]
pub(super) fn pack_int_key_1(k0: i64) -> u128 {
    k0 as u64 as u128
}

/// Unpack a u128 back into individual i64 keys.
#[inline]
pub(super) fn unpack_int_keys(packed: u128, num_keys: usize) -> [i64; 2] {
    let k0 = packed as u64 as i64;
    let k1 = if num_keys > 1 {
        (packed >> 64) as u64 as i64
    } else {
        0
    };
    [k0, k1]
}

/// Type alias for compact group map with u128 keys.
pub(crate) type CompactGroupMap = hashbrown::HashMap<u128, u32, BuildHasherDefault<ahash::AHasher>>;

/// Hasher for maps keyed by an already-uniform 128-bit digest (the mixed
/// path's group-key hashes from `hash_mixed_key`): fold the two halves
/// instead of re-hashing 16 bytes through AHash. Saves the hash on every
/// probe/insert and makes growth rehashes near-free (ClickBench Q18 spent
/// ~10% of CPU re-hashing keys during table doublings).
///
/// NOT suitable for `CompactGroupMap`'s packed *raw* int keys
/// (parallel_compact) — those aren't uniformly distributed and need a real
/// hash to avoid clustering.
#[derive(Default)]
pub(crate) struct DigestFoldHasher(u64);

impl std::hash::Hasher for DigestFoldHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // FNV-style fallback; the digest-map key path only calls write_u128.
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    #[inline]
    fn write_u128(&mut self, v: u128) {
        self.0 = (v as u64) ^ ((v >> 64) as u64);
    }
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

/// Group map keyed by 128-bit digests (parallel mixed path).
pub(crate) type DigestGroupMap =
    hashbrown::HashMap<u128, u32, BuildHasherDefault<DigestFoldHasher>>;

/// Set of 128-bit digests (F8 preselect, speculative top-N candidates).
pub(crate) type DigestSet = hashbrown::HashSet<u128, BuildHasherDefault<DigestFoldHasher>>;
