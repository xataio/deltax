mod callbacks;
mod cd_set;
mod compact;
mod extract;
mod keys;
mod metadata;
mod parallel_cd;
mod parallel_compact;
mod parallel_mixed;
mod parser;
mod regex;
mod serial;
mod state;
#[cfg(any(test, feature = "pg_test"))]
mod test_utils;

// External callers (scan/exec/mod.rs → path.rs, agg_wire.rs, etc.)
// access these via `agg::X`; the dispatches/callbacks reach them as
// `super::X` from inside their own files.
pub(crate) use callbacks::create_agg_scan_state;
pub(crate) use compact::{
    CompactAccKind, CompactAccLayout, CompactAccStorage, CountDistinctSideCar, DictDistinctRemap,
    StringArena, build_dict_distinct_remaps, can_use_compact_accs, compact_finalize,
    compact_topn_select, datum_to_f64, datum_to_i128, finalize_accumulator, i128_to_numeric_datum,
};
pub(crate) use keys::{CompactGroupMap, can_use_compact_keys_path};
pub(crate) use parallel_compact::ParallelCompactResult;
pub(crate) use state::{
    AggExecSpec, AggExpr, AggScanState, AggType, CaseWhenClause, CaseWhenCondition, CaseWhenOp,
    CaseWhenSpec, CaseWhenValue, GroupByColSpec, GroupByExpr, HavingFilter, HavingOp,
    MAX_AGG_WORKER_SLOTS, OutputTransform,
};
