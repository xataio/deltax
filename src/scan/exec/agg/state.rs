//! Type definitions for the DeltaXAgg executor: aggregate accumulators,
//! per-aggregate / per-group-by specs, scan state, parallel DSM scaffolding.
//!
//! Pure type defs only — no executor logic. Behaviour lives in `parser.rs`
//! (deserialising `custom_private`), `metadata.rs` (segment-metadata fast
//! paths), and the remaining sections of `mod.rs` (callbacks, compact /
//! parallel paths, finalize).

use std::sync::atomic::AtomicU64;

use pgrx::pg_sys;

use super::super::batch_qual::BatchQual;
use super::super::segments::{MetadataInfo, ScanBufferStats, SegmentData};
use super::cd_set::{CdSetInt, CdSetStr, new_cd_set_int, new_cd_set_str};

// ============================================================================
// DeltaXAgg: aggregate pushdown (SUM, AVG, COUNT, COUNT(DISTINCT), GROUP BY)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum AggType {
    Sum,
    Count,
    CountStar,
    Avg,
    CountDistinct,
    Min,
    Max,
}

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
    PgUsShift {
        delta: i64,
    },
}

pub(crate) enum AggAccumulator {
    SumInt {
        sum: i128,
        count: i64,
    },
    SumFloat {
        sum: f64,
        count: i64,
    },
    Count {
        count: i64,
    },
    CountDistinctInt {
        seen: CdSetInt,
    },
    /// Stores SipHash-128 digests of strings instead of owned Strings.
    /// Bounded memory (16 bytes per distinct value) — same approach as ClickHouse's uniqExact.
    CountDistinctStr {
        seen: CdSetStr,
    },
    MinInt {
        val: Option<i64>,
    },
    MaxInt {
        val: Option<i64>,
    },
    MinFloat {
        val: Option<f64>,
    },
    MaxFloat {
        val: Option<f64>,
    },
    MinStr {
        val: Option<String>,
    },
    MaxStr {
        val: Option<String>,
    },
}

impl AggAccumulator {
    pub(crate) fn new_for(agg_type: AggType, col_type: pg_sys::Oid) -> Self {
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
                if col_type == pg_sys::TEXTOID
                    || col_type == pg_sys::VARCHAROID
                    || col_type == pg_sys::BPCHAROID
                {
                    AggAccumulator::CountDistinctStr {
                        seen: new_cd_set_str(),
                    }
                } else {
                    AggAccumulator::CountDistinctInt {
                        seen: new_cd_set_int(),
                    }
                }
            }
            AggType::Min => {
                if col_type == pg_sys::TEXTOID
                    || col_type == pg_sys::VARCHAROID
                    || col_type == pg_sys::BPCHAROID
                {
                    AggAccumulator::MinStr { val: None }
                } else if col_type == pg_sys::FLOAT4OID || col_type == pg_sys::FLOAT8OID {
                    AggAccumulator::MinFloat { val: None }
                } else {
                    AggAccumulator::MinInt { val: None }
                }
            }
            AggType::Max => {
                if col_type == pg_sys::TEXTOID
                    || col_type == pg_sys::VARCHAROID
                    || col_type == pg_sys::BPCHAROID
                {
                    AggAccumulator::MaxStr { val: None }
                } else if col_type == pg_sys::FLOAT4OID || col_type == pg_sys::FLOAT8OID {
                    AggAccumulator::MaxFloat { val: None }
                } else {
                    AggAccumulator::MaxInt { val: None }
                }
            }
        }
    }

    pub(crate) fn clone_fresh(&self) -> Self {
        match self {
            AggAccumulator::SumInt { .. } => AggAccumulator::SumInt { sum: 0, count: 0 },
            AggAccumulator::SumFloat { .. } => AggAccumulator::SumFloat { sum: 0.0, count: 0 },
            AggAccumulator::Count { .. } => AggAccumulator::Count { count: 0 },
            AggAccumulator::CountDistinctInt { .. } => AggAccumulator::CountDistinctInt {
                seen: new_cd_set_int(),
            },
            AggAccumulator::CountDistinctStr { .. } => AggAccumulator::CountDistinctStr {
                seen: new_cd_set_str(),
            },
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
    pub(crate) col_idx: i32,              // -1 for COUNT(*)
    pub(crate) col_type_oid: pg_sys::Oid, // source column type
    pub(crate) expr_kind: AggExpr,        // Column, LengthOf, or AddConst
    pub(crate) const_offset: i64,         // Only used when expr_kind == AddConst
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
    RegexpReplace {
        pattern: String,
        replacement: String,
        func_oid: u32,
        collation: u32,
    },
    /// date_trunc(unit, timestamp_col): GROUP BY date_trunc('minute', ts)
    DateTrunc {
        unit: String,
        unit_usecs: i64,
        func_oid: u32,
    },
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
    Extract {
        unit: String,
        func_oid: u32,
        divisor: i64,
    },
    /// col +/- const: GROUP BY col - 1  (offset is always stored as addition, so col-1 → offset=-1)
    AddConst { offset: i64, op_oid: u32 },
    /// CASE WHEN ... THEN ... ELSE ... END
    CaseWhen(CaseWhenSpec),
}

/// Comparison operator for CASE WHEN conditions.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(i32)]
pub(crate) enum CaseWhenOp {
    Eq = 0,
    NotEq = 1,
}

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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupByColSpec {
    pub(crate) col_idx: i32, // 0-based column index
    pub(crate) type_oid: pg_sys::Oid,
    pub(crate) expr: GroupByExpr,
}

// SAFETY: GroupByColSpec contains only value types (i32, Oid=u32, strings, enums).
unsafe impl Send for GroupByColSpec {}
unsafe impl Sync for GroupByColSpec {}

/// A HAVING filter: compare an aggregate result against a constant.
#[derive(Debug, Clone, Copy)]
pub(crate) enum HavingOp {
    Gt,
    Lt,
    Ge,
    Le,
    Eq,
    Ne,
}

#[derive(Debug, Clone)]
pub(crate) struct HavingFilter {
    pub(crate) agg_idx: usize, // index into agg_specs
    pub(crate) op: HavingOp,
    pub(crate) const_val: i64, // constant value (int8)
}

/// State for DeltaXAgg (aggregate pushdown).
///
/// `Default` returns the canonical "all-zero / null-pointer / empty-Vec"
/// baseline. Construction sites use `..Default::default()` to fill in
/// the dozens of zero-by-default fields, keeping the explicit
/// initialisers focused on whatever the path actually computes.
#[derive(Default)]
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
    pub(crate) buf_stats: ScanBufferStats,
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
    pub(super) meta: MetadataInfo,
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
pub(crate) const MAX_AGG_WORKER_SLOTS: usize = super::super::decompress::MAX_WORKER_SLOTS;

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
    /// Pointer to slot `slot_idx`'s slab.
    ///
    /// # Safety
    ///
    /// `slot_idx < n_worker_slots` (the DSM region must have been
    /// sized to include that slot's slab). `&self` must point into a
    /// live `DeltaXAggPState` mapped by `initialize_dsm_deltax_agg`.
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

/// Output mapping entry: which internal data to put at this slot position.
#[derive(Debug, Clone, Copy)]
pub(super) enum OutputEntry {
    Agg(usize),                 // index into agg_specs
    Group(usize),               // index into group_specs
    Const(pg_sys::Datum, bool), // constant value + is_null
    /// Derived from another group key: value = group_keys[base_gi] + delta.
    /// Used for eliminated redundant GROUP BY expressions (e.g. GROUP BY col, col-1, col-2).
    DerivedGroup {
        base_gi: usize,
        delta: i64,
    },
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

use std::hash::{BuildHasherDefault, Hash, Hasher};

use super::compact::StringArena;

/// Group key value for HashMap key (owned).
/// Str variant stores (offset, len) into a StringArena.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GroupKeyVal {
    Null,
    Int(i64),
    Str(u32, u32), // (offset, len) into StringArena
}

/// Group key that avoids heap allocation for the common single-column case.
/// For high-cardinality GROUP BY (275K+ groups), eliminating per-key Vec
/// allocation saves ~130ms of cleanup overhead when the HashMap is dropped.
pub(super) enum GroupKey {
    Single(GroupKeyVal),
    Multi(Box<[GroupKeyVal]>),
}

impl GroupKey {
    pub(super) fn as_slice(&self) -> &[GroupKeyVal] {
        match self {
            GroupKey::Single(v) => std::slice::from_ref(v),
            GroupKey::Multi(v) => v,
        }
    }
}

/// Borrowed group key component without lifetime parameter.
/// Uses raw pointer for strings to avoid borrow-checker conflicts when reusing
/// the key buffer across loop iterations while mutating regex_results.
/// SAFETY: The pointed-to str data must outlive the current row iteration
/// (guaranteed by seg_text_columns and regex_results living across the loop).
#[derive(Debug, Clone, Copy)]
pub(super) enum GroupKeyRef {
    Null,
    Int(i64),
    Str(*const str),
}

impl GroupKeyRef {
    /// Create a Str variant from a &str. The caller must ensure the str outlives this GroupKeyRef.
    pub(super) fn from_str(s: &str) -> Self {
        GroupKeyRef::Str(s as *const str)
    }

    pub(super) fn resolve(&self, arena: &mut StringArena) -> GroupKeyVal {
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

    pub(super) fn matches_owned(&self, owned: &GroupKeyVal, arena: &StringArena) -> bool {
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

fn hash_key_component<H: Hasher>(h: &mut H, val: &GroupKeyVal, arena: &StringArena) {
    match val {
        GroupKeyVal::Null => 0u8.hash(h),
        GroupKeyVal::Int(v) => {
            1u8.hash(h);
            v.hash(h);
        }
        GroupKeyVal::Str(off, len) => {
            2u8.hash(h);
            arena.get(*off, *len).hash(h);
        }
    }
}

fn hash_ref_component<H: Hasher>(h: &mut H, val: &GroupKeyRef) {
    match val {
        GroupKeyRef::Null => 0u8.hash(h),
        GroupKeyRef::Int(v) => {
            1u8.hash(h);
            v.hash(h);
        }
        GroupKeyRef::Str(p) => {
            // SAFETY: pointer is valid for the current row iteration
            let s = unsafe { &**p };
            2u8.hash(h);
            s.hash(h);
        }
    }
}

/// Compute hash for an owned GroupKey (needs arena to resolve strings).
pub(super) fn hash_group_key(key: &GroupKey, arena: &StringArena) -> u64 {
    let mut hasher = ahash::AHasher::default();
    for val in key.as_slice() {
        hash_key_component(&mut hasher, val, arena);
    }
    hasher.finish()
}

/// Compute hash for a borrowed group key slice (no allocation).
pub(super) fn hash_group_key_ref(key: &[GroupKeyRef]) -> u64 {
    let mut hasher = ahash::AHasher::default();
    for val in key {
        hash_ref_component(&mut hasher, val);
    }
    hasher.finish()
}

/// Check if a stored owned key matches a temporary borrowed key.
pub(super) fn keys_match(stored: &GroupKey, temp: &[GroupKeyRef], arena: &StringArena) -> bool {
    let s = stored.as_slice();
    s.len() == temp.len()
        && s.iter()
            .zip(temp.iter())
            .all(|(s, t)| t.matches_owned(s, arena))
}

/// Type alias for the group map using hashbrown with raw_entry support.
/// Maps group keys to indices into flat accumulator storage.
/// Using u32 index instead of Vec<AggAccumulator> eliminates per-group heap allocation
/// for accumulators, saving ~130ms cleanup for 275K groups.
pub(super) type GroupMap = hashbrown::HashMap<GroupKey, u32, BuildHasherDefault<ahash::AHasher>>;

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use super::super::cd_set::new_cd_set_int;
    use super::super::compact::StringArena;
    use super::{
        AggAccumulator, AggType, GroupKey, GroupKeyRef, GroupKeyVal, hash_group_key,
        hash_group_key_ref, keys_match,
    };
    use pgrx::pg_sys;

    // -------------------------------------------------------------------
    // AggAccumulator::new_for tests
    // -------------------------------------------------------------------

    #[test]
    fn test_accumulator_new_sum_int() {
        let acc = AggAccumulator::new_for(AggType::Sum, pg_sys::INT4OID);
        assert!(matches!(acc, AggAccumulator::SumInt { sum: 0, count: 0 }));
    }

    #[test]
    fn test_accumulator_new_sum_float() {
        let acc = AggAccumulator::new_for(AggType::Sum, pg_sys::FLOAT8OID);
        assert!(matches!(acc, AggAccumulator::SumFloat { .. }));
    }

    #[test]
    fn test_accumulator_new_avg_float4() {
        let acc = AggAccumulator::new_for(AggType::Avg, pg_sys::FLOAT4OID);
        assert!(matches!(acc, AggAccumulator::SumFloat { .. }));
    }

    #[test]
    fn test_accumulator_new_avg_int() {
        let acc = AggAccumulator::new_for(AggType::Avg, pg_sys::INT8OID);
        assert!(matches!(acc, AggAccumulator::SumInt { .. }));
    }

    #[test]
    fn test_accumulator_new_count() {
        let acc = AggAccumulator::new_for(AggType::Count, pg_sys::INT4OID);
        assert!(matches!(acc, AggAccumulator::Count { count: 0 }));
    }

    #[test]
    fn test_accumulator_new_count_star() {
        let acc = AggAccumulator::new_for(AggType::CountStar, pg_sys::Oid::from(0u32));
        assert!(matches!(acc, AggAccumulator::Count { count: 0 }));
    }

    #[test]
    fn test_accumulator_new_count_distinct_text() {
        let acc = AggAccumulator::new_for(AggType::CountDistinct, pg_sys::TEXTOID);
        assert!(matches!(acc, AggAccumulator::CountDistinctStr { .. }));
    }

    #[test]
    fn test_accumulator_new_count_distinct_int() {
        let acc = AggAccumulator::new_for(AggType::CountDistinct, pg_sys::INT4OID);
        assert!(matches!(acc, AggAccumulator::CountDistinctInt { .. }));
    }

    #[test]
    fn test_accumulator_new_min_text() {
        let acc = AggAccumulator::new_for(AggType::Min, pg_sys::TEXTOID);
        assert!(matches!(acc, AggAccumulator::MinStr { val: None }));
    }

    #[test]
    fn test_accumulator_new_min_float() {
        let acc = AggAccumulator::new_for(AggType::Min, pg_sys::FLOAT8OID);
        assert!(matches!(acc, AggAccumulator::MinFloat { val: None }));
    }

    #[test]
    fn test_accumulator_new_min_int() {
        let acc = AggAccumulator::new_for(AggType::Min, pg_sys::INT4OID);
        assert!(matches!(acc, AggAccumulator::MinInt { val: None }));
    }

    #[test]
    fn test_accumulator_new_max_varchar() {
        let acc = AggAccumulator::new_for(AggType::Max, pg_sys::VARCHAROID);
        assert!(matches!(acc, AggAccumulator::MaxStr { val: None }));
    }

    #[test]
    fn test_accumulator_new_max_float4() {
        let acc = AggAccumulator::new_for(AggType::Max, pg_sys::FLOAT4OID);
        assert!(matches!(acc, AggAccumulator::MaxFloat { val: None }));
    }

    #[test]
    fn test_accumulator_new_max_int() {
        let acc = AggAccumulator::new_for(AggType::Max, pg_sys::INT8OID);
        assert!(matches!(acc, AggAccumulator::MaxInt { val: None }));
    }

    // -------------------------------------------------------------------
    // AggAccumulator::clone_fresh tests
    // -------------------------------------------------------------------

    #[test]
    fn test_clone_fresh_sum_int_resets() {
        let acc = AggAccumulator::SumInt {
            sum: 999,
            count: 50,
        };
        let fresh = acc.clone_fresh();
        assert!(matches!(fresh, AggAccumulator::SumInt { sum: 0, count: 0 }));
    }

    #[test]
    fn test_clone_fresh_sum_float_resets() {
        let acc = AggAccumulator::SumFloat {
            sum: 1.5,
            count: 10,
        };
        let fresh = acc.clone_fresh();
        assert!(
            matches!(fresh, AggAccumulator::SumFloat { sum, count } if sum == 0.0 && count == 0)
        );
    }

    #[test]
    fn test_clone_fresh_count_resets() {
        let acc = AggAccumulator::Count { count: 42 };
        let fresh = acc.clone_fresh();
        assert!(matches!(fresh, AggAccumulator::Count { count: 0 }));
    }

    #[test]
    fn test_clone_fresh_min_int_resets() {
        let acc = AggAccumulator::MinInt { val: Some(7) };
        let fresh = acc.clone_fresh();
        assert!(matches!(fresh, AggAccumulator::MinInt { val: None }));
    }

    #[test]
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
    // GroupKeyRef / GroupKeyVal / GroupKey tests
    // -------------------------------------------------------------------

    #[test]
    fn test_group_key_ref_resolve_null() {
        let mut arena = StringArena::new();
        let r = GroupKeyRef::Null;
        assert_eq!(r.resolve(&mut arena), GroupKeyVal::Null);
    }

    #[test]
    fn test_group_key_ref_resolve_int() {
        let mut arena = StringArena::new();
        let r = GroupKeyRef::Int(42);
        assert_eq!(r.resolve(&mut arena), GroupKeyVal::Int(42));
    }

    #[test]
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

    #[test]
    fn test_group_key_ref_matches_owned_null() {
        let arena = StringArena::new();
        assert!(GroupKeyRef::Null.matches_owned(&GroupKeyVal::Null, &arena));
        assert!(!GroupKeyRef::Null.matches_owned(&GroupKeyVal::Int(0), &arena));
    }

    #[test]
    fn test_group_key_ref_matches_owned_int() {
        let arena = StringArena::new();
        assert!(GroupKeyRef::Int(5).matches_owned(&GroupKeyVal::Int(5), &arena));
        assert!(!GroupKeyRef::Int(5).matches_owned(&GroupKeyVal::Int(6), &arena));
        assert!(!GroupKeyRef::Int(5).matches_owned(&GroupKeyVal::Null, &arena));
    }

    #[test]
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

    #[test]
    fn test_group_key_single_as_slice() {
        let key = GroupKey::Single(GroupKeyVal::Int(7));
        assert_eq!(key.as_slice().len(), 1);
        assert_eq!(key.as_slice()[0], GroupKeyVal::Int(7));
    }

    #[test]
    fn test_group_key_multi_as_slice() {
        let key = GroupKey::Multi(vec![GroupKeyVal::Int(1), GroupKeyVal::Null].into_boxed_slice());
        assert_eq!(key.as_slice().len(), 2);
        assert_eq!(key.as_slice()[0], GroupKeyVal::Int(1));
        assert_eq!(key.as_slice()[1], GroupKeyVal::Null);
    }

    // -------------------------------------------------------------------
    // hash_group_key / hash_group_key_ref / keys_match tests
    // -------------------------------------------------------------------

    #[test]
    fn test_hash_consistency_int() {
        // hash_group_key and hash_group_key_ref must produce the same hash
        // for equivalent keys
        let mut arena = StringArena::new();
        let owned = GroupKey::Single(GroupKeyVal::Int(42));
        let borrowed = [GroupKeyRef::Int(42)];
        let _ = &mut arena; // arena unused for int keys, but needed for API
        assert_eq!(
            hash_group_key(&owned, &arena),
            hash_group_key_ref(&borrowed)
        );
    }

    #[test]
    fn test_hash_consistency_null() {
        let arena = StringArena::new();
        let owned = GroupKey::Single(GroupKeyVal::Null);
        let borrowed = [GroupKeyRef::Null];
        assert_eq!(
            hash_group_key(&owned, &arena),
            hash_group_key_ref(&borrowed)
        );
    }

    #[test]
    fn test_hash_consistency_str() {
        let mut arena = StringArena::new();
        let (off, len) = arena.alloc("hello");
        let owned = GroupKey::Single(GroupKeyVal::Str(off, len));
        let s = "hello";
        let borrowed = [GroupKeyRef::from_str(s)];
        assert_eq!(
            hash_group_key(&owned, &arena),
            hash_group_key_ref(&borrowed)
        );
    }

    #[test]
    fn test_hash_consistency_multi() {
        let mut arena = StringArena::new();
        let (off, len) = arena.alloc("world");
        let owned = GroupKey::Multi(
            vec![
                GroupKeyVal::Int(1),
                GroupKeyVal::Str(off, len),
                GroupKeyVal::Null,
            ]
            .into_boxed_slice(),
        );
        let s = "world";
        let borrowed = [
            GroupKeyRef::Int(1),
            GroupKeyRef::from_str(s),
            GroupKeyRef::Null,
        ];
        assert_eq!(
            hash_group_key(&owned, &arena),
            hash_group_key_ref(&borrowed)
        );
    }

    #[test]
    fn test_hash_different_values_differ() {
        let arena = StringArena::new();
        let k1 = GroupKey::Single(GroupKeyVal::Int(1));
        let k2 = GroupKey::Single(GroupKeyVal::Int(2));
        assert_ne!(hash_group_key(&k1, &arena), hash_group_key(&k2, &arena));
    }

    #[test]
    fn test_hash_different_types_differ() {
        // Int(0) vs Null should hash differently (different discriminant)
        let arena = StringArena::new();
        let k1 = GroupKey::Single(GroupKeyVal::Int(0));
        let k2 = GroupKey::Single(GroupKeyVal::Null);
        assert_ne!(hash_group_key(&k1, &arena), hash_group_key(&k2, &arena));
    }

    #[test]
    fn test_keys_match_single_int() {
        let arena = StringArena::new();
        let owned = GroupKey::Single(GroupKeyVal::Int(42));
        let temp = [GroupKeyRef::Int(42)];
        assert!(keys_match(&owned, &temp, &arena));
    }

    #[test]
    fn test_keys_match_single_mismatch() {
        let arena = StringArena::new();
        let owned = GroupKey::Single(GroupKeyVal::Int(42));
        let temp = [GroupKeyRef::Int(99)];
        assert!(!keys_match(&owned, &temp, &arena));
    }

    #[test]
    fn test_keys_match_length_mismatch() {
        let arena = StringArena::new();
        let owned = GroupKey::Single(GroupKeyVal::Int(42));
        let temp = [GroupKeyRef::Int(42), GroupKeyRef::Null];
        assert!(!keys_match(&owned, &temp, &arena));
    }

    #[test]
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

    #[test]
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
}
