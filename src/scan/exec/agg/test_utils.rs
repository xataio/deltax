//! Test-only helpers shared by per-module test blocks under `agg/`.
//!
//! Gated on `cfg(any(test, feature = "pg_test"))` so the cost lands in
//! test builds only.

use std::collections::HashMap;

use pgrx::pg_sys;

use super::super::segments::{MetadataInfo, SegmentData};
use super::state::{
    AggExecSpec, AggExpr, AggType, GroupByColSpec, HavingFilter, OutputEntry, OutputTransform,
    ParsedAggPlan,
};

/// Build a `pg_sys::List` of integers from a slice. Used by the
/// `parse_agg_private` tests to feed canned wire payloads.
///
/// SAFETY: calls `pg_sys::lappend_int` (palloc-backed); must run inside
/// an active PG transaction.
pub(super) unsafe fn build_int_list(values: &[i32]) -> *mut pg_sys::List {
    unsafe {
        let mut list: *mut pg_sys::List = std::ptr::null_mut();
        for &v in values {
            list = pg_sys::lappend_int(list, v);
        }
        list
    }
}

/// Empty `MetadataInfo` with the given column names, all INT4 typed.
pub(super) fn make_meta(col_names: &[&str]) -> MetadataInfo {
    MetadataInfo {
        col_names: col_names.iter().map(|s| s.to_string()).collect(),
        col_types: col_names.iter().map(|_| pg_sys::Oid::from(23u32)).collect(),
        col_typmods: col_names.iter().map(|_| -1).collect(),
        col_not_null: col_names.iter().map(|_| false).collect(),
        segment_by: Vec::new(),
        order_by: Vec::new(),
        time_column: "ts".to_string(),
    }
}

/// Build a `ParsedAggPlan` from the supplied specs. `where_null=false`
/// uses a dangling-but-non-null placeholder for `where_quals` since
/// rejection paths never dereference it.
pub(super) fn make_plan(
    agg_specs: Vec<AggExecSpec>,
    group_specs: Vec<GroupByColSpec>,
    having: Vec<HavingFilter>,
    where_null: bool,
) -> ParsedAggPlan {
    let output_map: Vec<OutputEntry> = (0..agg_specs.len())
        .map(OutputEntry::Agg)
        .chain((0..group_specs.len()).map(OutputEntry::Group))
        .collect();
    ParsedAggPlan {
        companion_oids: vec![pg_sys::Oid::from(9999u32)],
        agg_specs,
        group_specs,
        output_map,
        having_filters: having,
        where_quals: if where_null {
            std::ptr::null_mut()
        } else {
            std::ptr::dangling_mut::<pg_sys::List>()
        },
        topn_limit: 0,
        topn_sort_col: 0,
        topn_ascending: true,
        derived_minmax_topn: None,
        bare_limit: 0,
        is_partial: false,
    }
}

/// Minimal `AggExecSpec` (Column expression, no offset, not partial).
pub(super) fn make_agg_spec(agg_type: AggType, col_idx: i32, col_type_oid: u32) -> AggExecSpec {
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

/// Empty `SegmentData` with the given row count.
pub(super) fn make_empty_segment(row_count: i32) -> SegmentData {
    SegmentData {
        companion_oid: pg_sys::InvalidOid,
        segment_id: 0,
        segment_values: Vec::new(),
        compressed_blobs: Vec::new(),
        text_length_blobs: Vec::new(),
        row_count,
        min_time: None,
        max_time: None,
        col_minmax: HashMap::new(),
        col_sums: HashMap::new(),
        toast_pointers: Vec::new(),
        cached_blob_pins: Vec::new(),
    }
}
