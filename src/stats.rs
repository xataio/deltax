//! Populate `pg_class.reltuples` and `pg_statistic` for compressed
//! partitions so PG's built-in selectivity functions stop falling back
//! to `DEFAULT_EQ_SEL` (0.005 for numeric equality, ~2.5e-5 for text
//! equality). This is the ingredient that lets the planner pick the
//! right join side on queries like Q17 (`event_type='Delivered'`) and
//! keeps point lookups (Q07 `order_id = N`) off the parallel path.
//!
//! Source of truth:
//! - `deltax.deltax_partition.row_count` — authoritative total rows
//! - `deltax.deltax_partition.column_ndistinct` — per-column merged-HLL
//!   estimate written by `compress.rs` at compress time (or SQL
//!   fallback for the standalone analyze UDF)
//! - `_<partition>_colstats._nonnull_count` — summed for nullfrac

use std::collections::HashMap;

use cardinality_estimator::CardinalityEstimator;
use pgrx::pg_sys;
use pgrx::spi::{self, SpiClient};

use crate::compress::ColumnMeta;

/// Write `pg_class.reltuples` + one `pg_statistic` row per column for
/// the compressed child partition.
pub fn write_partition_stats(
    client: &mut SpiClient,
    part_rel_oid: pg_sys::Oid,
    col_ndistinct: &HashMap<String, i64>,
    row_count: i64,
    colstats_fqn: &str,
    columns: &[ColumnMeta],
) -> spi::SpiResult<()> {
    if row_count <= 0 {
        return Ok(());
    }

    // Single SPI pass over the colstats table: per-column SUM(nonnull) (→
    // nullfrac) and SUM(_sum) (→ avg length → stawidth for text). One row per
    // non-segment-by column, keyed by `_col_idx`.
    let colstats_agg = load_colstats_aggregates(client, colstats_fqn)?;

    // Fetch `(attname, attnum, attlen, atttypid)` for every non-dropped
    // column of the partition so we can map our `ColumnMeta` back to PG's
    // attnum, pick stawidth from attlen, and choose a histogram encoding
    // from atttypid.
    let attrs = load_pg_attribute(client, part_rel_oid)?;

    // Per-column `[min, max]` (order-preserving i64 encoding), persisted at
    // compression time. Feeds the per-column histogram so range predicates
    // on the order-by / time column stop collapsing to a default
    // selectivity. Empty if the partition predates `column_minmax`.
    let col_minmax = load_column_minmax(client, part_rel_oid)?;
    // Per-segment minimums per `_col_idx` → a multi-bucket child histogram (#6)
    // instead of the 2-point `[min, max]`. Segments are ~equal-sized, so the
    // mins are ~equi-depth bucket boundaries.
    let col_seg_mins = load_colstats_segment_mins(client, colstats_fqn)?;
    // Per-column distinct-value lists for low-cardinality columns, used to
    // write an MCV (exact equality selectivity + ~0 for absent values).
    let col_valmap = load_column_valmap(client, part_rel_oid)?;
    // Per-value occurrence counts for those same columns → real MCV
    // frequencies. Empty for partitions predating `column_valcounts`, in which
    // case `mcv_freqs` falls back to the old uniform split.
    let col_valcounts = load_column_valcounts(client, part_rel_oid)?;
    // Partial MCV (heavy hitters) for high-cardinality text columns the <=32
    // valmap doesn't cover; written with the real HLL stadistinct so the tail
    // is still estimated.
    let col_mcv = load_column_mcv(client, part_rel_oid)?;

    // Average tuple width (→ `pg_class.relpages`) is accumulated from the
    // accurate per-column widths inside the loop below.
    let mut sum_widths: i64 = 0;

    // Walk our pg_deltax columns, match to the partition's pg_attribute
    // entry by name, emit one pg_statistic row each. Segment-by columns are
    // stored as the partition's segment_values (not in the blob), so they have
    // no `_colstats` entry; their stats come from the meta table, folded into
    // `col_ndistinct` / `col_valmap` / `col_valcounts` at compress time (see
    // `augment_segment_by_stats`). Without them PG defaults `WHERE segkey = X`
    // to 0.005 — and the segment key is the column users filter/join on most.
    // `nonseg_idx` tracks the colstats `_col_idx`, so it advances only for
    // non-segment-by columns.
    let mut nonseg_idx: i32 = 0;
    for col in columns {
        let attr = match attrs.iter().find(|a| a.attname == col.name) {
            Some(a) => a,
            None => {
                if !col.is_segment_by {
                    nonseg_idx += 1;
                }
                continue; // column was dropped post-compression
            }
        };
        // Accurate width for text (avg length + varlena header) instead of the
        // flat 32; segment-by columns aren't in colstats so they use attlen/32.
        let agg = if col.is_segment_by {
            None
        } else {
            colstats_agg.get(&nonseg_idx)
        };
        let stawidth = column_stawidth(attr, agg);
        sum_widths += stawidth as i64;

        // NOT NULL columns have no nulls by definition. For nullable
        // columns, derive nullfrac from the colstats nonnull sum — but a
        // missing or 0 entry means "no per-column info" (e.g. the order-by
        // time column isn't indexed the same way in `_colstats`), NOT that
        // the column is all-null. Treating 0 as all-null wrote
        // `stanullfrac = 1.0` for `event_created`, which zeroed its range
        // selectivity `(1 - nullfrac)` and neutralised the histogram.
        let stanullfrac = if attr.attnotnull {
            0.0
        } else if col.is_segment_by {
            // Segment-by columns aren't in `_colstats`. The valcounts (when
            // present, i.e. <=32 distinct) is the complete non-null value set
            // for the partition, so it gives an exact nullfrac; for a high-card
            // segment key we lack the null count and assume non-null.
            match col_valcounts.get(&col.name) {
                Some(counts) => {
                    let nonnull: i64 = counts.values().sum();
                    ((row_count - nonnull) as f32 / row_count as f32).clamp(0.0, 1.0)
                }
                None => 0.0,
            }
        } else {
            let nonnull = agg.map(|&(n, _)| n).filter(|&n| n > 0).unwrap_or(row_count);
            ((row_count - nonnull) as f32 / row_count as f32).clamp(0.0, 1.0)
        };

        let ndistinct = col_ndistinct.get(&col.name).copied().unwrap_or(0);

        // Slot 1: a histogram for ordered types, else an MCV for a
        // low-cardinality column. The valmap is the column's *complete*
        // distinct-value set for the partition, so an MCV over it lets PG
        // estimate equality on an absent value at ~0 (1/ndistinct gets that
        // badly wrong — see Q19). Equality filters on `order_events` are
        // estimated per-child then summed, so this child-level MCV is what the
        // planner actually reads. When writing an MCV, stadistinct must equal
        // the value count (the persisted ndistinct is a per-segment MAX that
        // can under-count) so non-MCV values get 0.
        let histogram = col_minmax.get(&col.name).and_then(|&(lo, hi)| {
            histogram_eligible(attr.atttypid, lo, hi).then(|| {
                // Multi-bucket from this column's per-segment mins; fall back to
                // the 2-point [min, max] if a single segment / no per-segment data.
                let mins = col_seg_mins
                    .get(&nonseg_idx)
                    .cloned()
                    .unwrap_or_else(|| vec![lo]);
                let bounds = build_histogram_bounds(attr.atttypid, mins, hi, MAX_HISTOGRAM_BOUNDS)
                    .unwrap_or_else(|| vec![lo, hi]);
                Slot1::Histogram {
                    type_oid: attr.atttypid,
                    bounds,
                }
            })
        });
        // A multi-bucket histogram means the column's per-segment mins are
        // monotonic — i.e. it's physically (~ascending) sorted — so write a
        // correlation of ~1.0 (#5). 2-point histograms are unsorted → none.
        let correlation = match &histogram {
            Some(Slot1::Histogram { type_oid, bounds }) if bounds.len() > 2 => {
                Some((*type_oid, 1.0f32))
            }
            _ => None,
        };
        let (slot, eff_ndistinct) = match histogram {
            Some(h) => (Some(h), ndistinct),
            // MCV only for text columns — `build_mcv_arrays` builds a text[]
            // `stavalues` with the text `=` operator, so a non-text valmap entry
            // (e.g. an integer segment key) must fall through to stadistinct.
            None if is_text_type(attr.atttypid) => {
                if let Some(vals) = col_valmap.get(&col.name).filter(|v| v.len() >= 2) {
                    // Complete MCV (<=32 distinct): stadistinct = value count, so
                    // absent values estimate ~0.
                    let empty = HashMap::new();
                    let counts = col_valcounts.get(&col.name).unwrap_or(&empty);
                    let freqs = mcv_freqs(vals, counts, row_count);
                    (
                        Some(Slot1::Mcv {
                            values: vals.clone(),
                            freqs,
                        }),
                        vals.len() as i64,
                    )
                } else if let Some(counts) = col_mcv.get(&col.name).filter(|m| m.len() >= 2) {
                    // Partial MCV (high-card heavy hitters): keep the real HLL
                    // `stadistinct` (eff_ndistinct = ndistinct) so PG estimates
                    // the hot values from the MCV and the tail from the remainder.
                    let (values, freqs) = ordered_mcv(counts, row_count, 100);
                    (Some(Slot1::Mcv { values, freqs }), ndistinct)
                } else {
                    (None, ndistinct)
                }
            }
            None => (None, ndistinct),
        };
        let stadistinct = stadistinct_value(eff_ndistinct, row_count);

        upsert_pg_statistic_row(
            client,
            part_rel_oid,
            attr.attnum,
            stadistinct,
            stanullfrac,
            stawidth,
            slot,
            correlation,
            false,
        )?;

        if !col.is_segment_by {
            nonseg_idx += 1;
        }
    }

    let avg_tuple_width = sum_widths.max(32);
    update_reltuples(client, part_rel_oid, row_count, avg_tuple_width as i32)?;

    // Make the new stats visible to other backends at commit time.
    invalidate_relcache(part_rel_oid);

    Ok(())
}

/// Per-`_col_idx` colstats aggregates: `(nonnull_count, length_sum)`. For text
/// columns `_sum` holds `SUM(length(value))` over non-null rows (see
/// `compress::compute_typed_sum`), so `length_sum / nonnull` is the average
/// character length — the basis for an accurate `stawidth` instead of the flat
/// 32. For non-text columns `_sum` is the numeric sum (ignored for width).
fn load_colstats_aggregates(
    client: &mut SpiClient,
    colstats_fqn: &str,
) -> spi::SpiResult<HashMap<i32, (i64, i64)>> {
    // `SUM(_sum)` is cast to float8, not int8: for a numeric column `_sum` is
    // the value sum, which can exceed i64 and would raise NumericValueOutOfRange
    // on an int8 cast. We only consult it for text (length sum, always small and
    // exactly representable in f64), so the float path is lossless where it
    // matters and merely approximate (and unused) for numeric columns.
    let query = format!(
        "SELECT _col_idx::int4, SUM(_nonnull_count)::int8, SUM(_sum)::float8 \
         FROM {} GROUP BY _col_idx",
        colstats_fqn
    );
    let mut out = HashMap::new();
    for row in client.select(&query, None, &[])? {
        let idx: i32 = row
            .get_datum_by_ordinal(1)
            .ok()
            .and_then(|d| d.value::<i32>().ok().flatten())
            .unwrap_or(-1);
        let nonnull: i64 = row
            .get_datum_by_ordinal(2)
            .ok()
            .and_then(|d| d.value::<i64>().ok().flatten())
            .unwrap_or(0);
        // Read as f64 (see query comment) and saturate to i64; for text the
        // length sum is exact, for numeric it's unused.
        let len_sum: i64 = row
            .get_datum_by_ordinal(3)
            .ok()
            .and_then(|d| d.value::<f64>().ok().flatten())
            .map(|f| f.max(0.0) as i64)
            .unwrap_or(0);
        if idx >= 0 {
            out.insert(idx, (nonnull, len_sum));
        }
    }
    Ok(out)
}

/// Load the per-segment `_min` values per `_col_idx` from a colstats table,
/// sorted ascending. Each segment is ~equal-sized, so these are ~equi-depth
/// histogram bucket boundaries — the basis for a multi-bucket child histogram
/// (#6) and the per-segment parent histogram (#7), versus the old 2-point
/// `[min, max]`.
fn load_colstats_segment_mins(
    client: &mut SpiClient,
    colstats_fqn: &str,
) -> spi::SpiResult<HashMap<i32, Vec<i64>>> {
    let query = format!(
        "SELECT _col_idx::int4, _min::int8 FROM {} \
         WHERE _min IS NOT NULL ORDER BY _col_idx, _min",
        colstats_fqn
    );
    let mut out: HashMap<i32, Vec<i64>> = HashMap::new();
    for row in client.select(&query, None, &[])? {
        let idx: i32 = row
            .get_datum_by_ordinal(1)
            .ok()
            .and_then(|d| d.value::<i32>().ok().flatten())
            .unwrap_or(-1);
        let min: Option<i64> = row
            .get_datum_by_ordinal(2)
            .ok()
            .and_then(|d| d.value::<i64>().ok().flatten());
        if idx >= 0
            && let Some(m) = min
        {
            out.entry(idx).or_default().push(m);
        }
    }
    Ok(out)
}

/// `stawidth` for one column. Fixed-width types use `attlen`. Text/varchar use
/// the average character length from colstats (`len_sum / nonnull`) plus the
/// varlena header (1 byte short, 4 bytes long) — far better than the flat 32,
/// which over-reports short codes and under-reports wide text (URL/Title) and so
/// mis-sizes sort/hash work-mem + `relpages`. Other varlena (jsonb/bytea, no
/// `_sum`) and missing data fall back to the 32-byte default.
fn column_stawidth(attr: &AttrInfo, agg: Option<&(i64, i64)>) -> i32 {
    if attr.attlen > 0 {
        return attr.attlen as i32;
    }
    if is_text_type(attr.atttypid)
        && let Some(&(nonnull, len_sum)) = agg
        && nonnull > 0
        && len_sum > 0
    {
        let avg = (len_sum as f64 / nonnull as f64).round() as i32;
        let header = if avg < 127 { 1 } else { 4 };
        return (avg + header).max(1);
    }
    stawidth_for_attlen(attr.attlen)
}

struct AttrInfo {
    attname: String,
    attnum: i16,
    attlen: i16,
    atttypid: pg_sys::Oid,
    attnotnull: bool,
}

fn load_pg_attribute(
    client: &mut SpiClient,
    rel_oid: pg_sys::Oid,
) -> spi::SpiResult<Vec<AttrInfo>> {
    let rel_oid_int = u32::from(rel_oid) as i64;
    let query = "SELECT attname::text, attnum::int2, attlen::int2, atttypid::int8, attnotnull \
                 FROM pg_attribute \
                 WHERE attrelid = $1::oid AND attnum > 0 AND NOT attisdropped \
                 ORDER BY attnum";
    let mut out = Vec::new();
    for row in client.select(query, None, &[rel_oid_int.into()])? {
        let attname: String = row
            .get_datum_by_ordinal(1)
            .ok()
            .and_then(|d| d.value::<String>().ok().flatten())
            .unwrap_or_default();
        let attnum: i16 = row
            .get_datum_by_ordinal(2)
            .ok()
            .and_then(|d| d.value::<i16>().ok().flatten())
            .unwrap_or(0);
        let attlen: i16 = row
            .get_datum_by_ordinal(3)
            .ok()
            .and_then(|d| d.value::<i16>().ok().flatten())
            .unwrap_or(-1);
        let atttypid: pg_sys::Oid = row
            .get_datum_by_ordinal(4)
            .ok()
            .and_then(|d| d.value::<i64>().ok().flatten())
            .map(|v| pg_sys::Oid::from(v as u32))
            .unwrap_or(pg_sys::InvalidOid);
        let attnotnull: bool = row
            .get_datum_by_ordinal(5)
            .ok()
            .and_then(|d| d.value::<bool>().ok().flatten())
            .unwrap_or(false);
        if !attname.is_empty() {
            out.push(AttrInfo {
                attname,
                attnum,
                attlen,
                atttypid,
                attnotnull,
            });
        }
    }
    Ok(out)
}

/// Whether we emit a `[min, max]` histogram for a column of this type, given
/// the order-preserving i64 bounds. Skips types we can't decode and constant
/// columns (`lo >= hi`), where a degenerate 1-bucket histogram would only
/// confuse range selectivity. Date bounds are compared at day granularity
/// (colstats stores dates as Unix-epoch microseconds; `decode_encoded_to_datum`
/// truncates to days), so a range inside a single day is treated as constant.
fn histogram_eligible(type_oid: pg_sys::Oid, lo: i64, hi: i64) -> bool {
    if !histogram_type_eligible(type_oid) {
        return false;
    }
    match type_oid {
        pg_sys::DATEOID => (lo / 86_400_000_000) < (hi / 86_400_000_000),
        _ => lo < hi,
    }
}

/// Whether a column type takes a text MCV (`build_mcv_arrays` emits a `text[]`
/// `stavalues` + the text `=` operator). Equality-selectivity types whose value
/// set we capture as text: `text`/`varchar`/`bpchar`/`name`/`char`.
fn is_text_type(type_oid: pg_sys::Oid) -> bool {
    matches!(
        type_oid,
        pg_sys::TEXTOID
            | pg_sys::VARCHAROID
            | pg_sys::BPCHAROID
            | pg_sys::NAMEOID
            | pg_sys::CHAROID
    )
}

/// Types whose order-preserving i64 colstats encoding we can decode back to
/// a native Datum for a histogram. FLOAT4/8 use the order-preserving float
/// encoding (`encode_f*_to_i64` / `decode_encoded_to_datum`). TEXT/NUMERIC
/// fall through.
fn histogram_type_eligible(type_oid: pg_sys::Oid) -> bool {
    matches!(
        type_oid,
        pg_sys::INT2OID
            | pg_sys::INT4OID
            | pg_sys::INT8OID
            | pg_sys::FLOAT4OID
            | pg_sys::FLOAT8OID
            | pg_sys::TIMESTAMPOID
            | pg_sys::TIMESTAMPTZOID
            | pg_sys::DATEOID
    )
}

/// Construct the 2-element `[min, max]` bounds array Datum for a histogram
/// slot, decoding the order-preserving i64 colstats encoding back to the
/// column's native type. Caller must have checked `histogram_eligible`.
///
/// `pg_statistic.stavaluesN` is an `anyarray` pseudo-type column, so it can't
/// be populated through a SQL `INSERT` of a concrete array (PG rejects the
/// row-type mismatch). PG's own ANALYZE forms the tuple in C with a real
/// array Datum; we do the same.
unsafe fn build_histogram_array(type_oid: pg_sys::Oid, bounds: &[i64]) -> pg_sys::Datum {
    let mut elems: Vec<pg_sys::Datum> = bounds
        .iter()
        .map(|&v| crate::scan::exec::count_minmax::decode_encoded_to_datum(v, type_oid))
        .collect();
    let mut typlen: i16 = 0;
    let mut typbyval: bool = false;
    let mut typalign: std::os::raw::c_char = 0;
    let arr = unsafe {
        pg_sys::get_typlenbyvalalign(type_oid, &mut typlen, &mut typbyval, &mut typalign);
        pg_sys::construct_array(
            elems.as_mut_ptr(),
            elems.len() as i32,
            type_oid,
            typlen as i32,
            typbyval,
            typalign,
        )
    };
    pg_sys::Datum::from(arr)
}

/// Look up the btree operator for a type and strategy number (1 = `<`,
/// 3 = `=`), used as `staop` for the histogram / MCV slot. Returns
/// `InvalidOid` if the type has no such btree operator.
fn lookup_strategy_operator(
    client: &mut SpiClient,
    type_oid: pg_sys::Oid,
    strategy: i32,
) -> pg_sys::Oid {
    let t = u32::from(type_oid) as i64;
    client
        .select(
            "SELECT amopopr::int8 FROM pg_amop \
             WHERE amoplefttype = $1::oid AND amoprighttype = $1::oid \
               AND amopstrategy = $2 AND amopmethod = 403 LIMIT 1",
            None,
            &[t.into(), strategy.into()],
        )
        .ok()
        .and_then(|t| t.into_iter().next())
        .and_then(|row| {
            row.get_datum_by_ordinal(1)
                .ok()
                .and_then(|d| d.value::<i64>().ok().flatten())
        })
        .map(|v| pg_sys::Oid::from(v as u32))
        .unwrap_or(pg_sys::InvalidOid)
}

/// Load the persisted per-column `[min, max]` (order-preserving i64) map for
/// a compressed partition from `deltax.deltax_partition.column_minmax`.
/// Empty map if the partition predates the column or has no eligible cols.
fn load_column_minmax(
    client: &mut SpiClient,
    part_rel_oid: pg_sys::Oid,
) -> spi::SpiResult<HashMap<String, (i64, i64)>> {
    let part_oid_int = u32::from(part_rel_oid) as i64;
    let json_text: Option<String> = client
        .select(
            "SELECT column_minmax::text FROM deltax.deltax_partition \
             WHERE table_name = (SELECT relname FROM pg_class WHERE oid = $1::oid) \
               AND is_compressed = true",
            None,
            &[part_oid_int.into()],
        )?
        .next()
        .and_then(|row| {
            row.get_datum_by_ordinal(1)
                .ok()
                .and_then(|d| d.value::<String>().ok().flatten())
        });
    Ok(json_text
        .and_then(|t| crate::scan::cost::parse_minmax_json(&t))
        .unwrap_or_default())
}

/// Load the persisted per-column distinct-value lists (`column_valmap`) for a
/// compressed partition. Only low-cardinality columns (≤ 32 distinct) have an
/// entry. Empty map if the partition predates the column.
fn load_column_valmap(
    client: &mut SpiClient,
    part_rel_oid: pg_sys::Oid,
) -> spi::SpiResult<HashMap<String, Vec<String>>> {
    let part_oid_int = u32::from(part_rel_oid) as i64;
    let json_text: Option<String> = client
        .select(
            "SELECT column_valmap::text FROM deltax.deltax_partition \
             WHERE table_name = (SELECT relname FROM pg_class WHERE oid = $1::oid) \
               AND is_compressed = true",
            None,
            &[part_oid_int.into()],
        )?
        .next()
        .and_then(|row| {
            row.get_datum_by_ordinal(1)
                .ok()
                .and_then(|d| d.value::<String>().ok().flatten())
        });
    let mut out: HashMap<String, Vec<String>> = HashMap::new();
    if let Some(text) = json_text
        && let Ok(serde_json::Value::Object(obj)) = serde_json::from_str::<serde_json::Value>(&text)
    {
        for (name, val) in obj {
            if let serde_json::Value::Array(arr) = val {
                let vals: Vec<String> = arr
                    .into_iter()
                    .filter_map(|v| match v {
                        serde_json::Value::String(s) => Some(s),
                        _ => None,
                    })
                    .collect();
                if !vals.is_empty() {
                    out.insert(name, vals);
                }
            }
        }
    }
    Ok(out)
}

/// Load the persisted per-column per-value occurrence counts
/// (`column_valcounts`) for a compressed partition. Shape on disk is
/// `{col_name: {value: count}}`; returned as `{col_name: {value: count}}`.
/// Same column set as `column_valmap` (≤ 32 distinct). Empty for partitions
/// compressed by a build predating `column_valcounts` — callers then fall
/// back to uniform MCV frequencies.
fn load_column_valcounts(
    client: &mut SpiClient,
    part_rel_oid: pg_sys::Oid,
) -> spi::SpiResult<HashMap<String, HashMap<String, i64>>> {
    let part_oid_int = u32::from(part_rel_oid) as i64;
    let json_text: Option<String> = client
        .select(
            "SELECT column_valcounts::text FROM deltax.deltax_partition \
             WHERE table_name = (SELECT relname FROM pg_class WHERE oid = $1::oid) \
               AND is_compressed = true",
            None,
            &[part_oid_int.into()],
        )?
        .next()
        .and_then(|row| {
            row.get_datum_by_ordinal(1)
                .ok()
                .and_then(|d| d.value::<String>().ok().flatten())
        });
    Ok(json_text
        .map(|t| parse_valcounts_json(&t))
        .unwrap_or_default())
}

/// Load the persisted partial MCV (`column_mcv`) for a compressed partition —
/// the heavy hitters of high-cardinality text columns (not covered by the
/// `<=32` valmap). Same `{col: {value: count}}` shape as `column_valcounts`;
/// `stats.rs` writes these as an MCV while keeping the real HLL `stadistinct`.
fn load_column_mcv(
    client: &mut SpiClient,
    part_rel_oid: pg_sys::Oid,
) -> spi::SpiResult<HashMap<String, HashMap<String, i64>>> {
    let part_oid_int = u32::from(part_rel_oid) as i64;
    let json_text: Option<String> = client
        .select(
            "SELECT column_mcv::text FROM deltax.deltax_partition \
             WHERE table_name = (SELECT relname FROM pg_class WHERE oid = $1::oid) \
               AND is_compressed = true",
            None,
            &[part_oid_int.into()],
        )?
        .next()
        .and_then(|row| {
            row.get_datum_by_ordinal(1)
                .ok()
                .and_then(|d| d.value::<String>().ok().flatten())
        });
    Ok(json_text
        .map(|t| parse_valcounts_json(&t))
        .unwrap_or_default())
}

/// Order an MCV value→count map by descending count (PG's `most_common_*`
/// convention), value as a stable tiebreak, capped at `cap` entries. Returns
/// the parallel `(values, freqs)` with `freq = count / row_count`.
fn ordered_mcv(
    counts: &HashMap<String, i64>,
    row_count: i64,
    cap: usize,
) -> (Vec<String>, Vec<f32>) {
    let mut pairs: Vec<(&String, i64)> = counts.iter().map(|(v, &c)| (v, c)).collect();
    pairs.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    pairs.truncate(cap);
    let values: Vec<String> = pairs.iter().map(|(v, _)| (*v).clone()).collect();
    let map: HashMap<String, i64> = pairs.iter().map(|(v, c)| ((*v).clone(), *c)).collect();
    let freqs = mcv_freqs(&values, &map, row_count);
    (values, freqs)
}

/// Parse a `{col_name: {value: count}}` JSON object into a per-column
/// value→count map. Non-object values and non-integer counts are skipped.
fn parse_valcounts_json(text: &str) -> HashMap<String, HashMap<String, i64>> {
    let mut out: HashMap<String, HashMap<String, i64>> = HashMap::new();
    if let Ok(serde_json::Value::Object(obj)) = serde_json::from_str::<serde_json::Value>(text) {
        for (name, val) in obj {
            if let serde_json::Value::Object(counts) = val {
                let mut m: HashMap<String, i64> = HashMap::new();
                for (v, c) in counts {
                    if let Some(n) = c.as_i64() {
                        m.insert(v, n);
                    }
                }
                if !m.is_empty() {
                    out.insert(name, m);
                }
            }
        }
    }
    out
}

/// Map an MCV's value list to `most_common_freqs` using per-value counts.
/// `freq = count / row_count` (PG's convention: a fraction of *all* rows, so
/// the freqs sum to the non-null fraction — which is exactly what makes an
/// absent value estimate ~0 in `var_eq_const`, since `1 - Σfreq - nullfrac`
/// then collapses to 0). When `counts` is empty (a partition predating
/// `column_valcounts`) we fall back to the old uniform `1/n` split. A value
/// present in the valmap always has count ≥ 1; the tiny positive floor is
/// defensive so PG never sees a 0/negative frequency for a listed value.
fn mcv_freqs(values: &[String], counts: &HashMap<String, i64>, row_count: i64) -> Vec<f32> {
    let n = values.len();
    if n == 0 {
        return Vec::new();
    }
    if counts.is_empty() || row_count <= 0 {
        return vec![1.0f32 / n as f32; n];
    }
    values
        .iter()
        .map(|v| {
            let c = counts.get(v).copied().unwrap_or(0).max(0);
            let f = c as f64 / row_count as f64;
            (f as f32).clamp(f32::MIN_POSITIVE, 1.0)
        })
        .collect()
}

/// Translate pg_attribute.attlen into a `stawidth`. Fixed-width types
/// use attlen directly; varlena types (`attlen < 0`) get a conservative
/// 32-byte default — pg_statistic's `stawidth` only feeds I/O and
/// width-dependent cost paths, not the equality selectivity we care
/// about here, so a rough estimate is fine.
fn stawidth_for_attlen(attlen: i16) -> i32 {
    if attlen > 0 { attlen as i32 } else { 32 }
}

/// Encode ndistinct per PG's sign convention: positive = absolute count
/// of distinct values; negative = fraction of `row_count`. PG's ANALYZE
/// flips to the fraction form when ndistinct / row_count > 0.1, which
/// lets the estimator handle tables that grow without a re-ANALYZE.
fn stadistinct_value(ndistinct: i64, row_count: i64) -> f32 {
    if ndistinct <= 0 || row_count <= 0 {
        return 0.0;
    }
    let nd = ndistinct as f64;
    let rc = row_count as f64;
    if nd < 0.1 * rc {
        nd as f32
    } else {
        (-nd / rc) as f32
    }
}

/// `UPDATE pg_class SET reltuples = $1, relpages = ... WHERE oid = $2`.
/// Keep `relpages >= 1` so PG doesn't mistake us for "never analyzed"
/// in its cost paths.
fn update_reltuples(
    client: &mut SpiClient,
    rel_oid: pg_sys::Oid,
    row_count: i64,
    avg_tuple_width: i32,
) -> spi::SpiResult<()> {
    let rel_oid_int = u32::from(rel_oid) as i64;
    let rel_pages: i32 = {
        let tuples_per_page = (8192 / avg_tuple_width.max(1)).max(1) as i64;
        ((row_count + tuples_per_page - 1) / tuples_per_page).max(1) as i32
    };
    client.update(
        "UPDATE pg_class SET reltuples = $1::real, relpages = $2::int \
         WHERE oid = $3::oid",
        None,
        &[
            (row_count as f32).into(),
            rel_pages.into(),
            rel_oid_int.into(),
        ],
    )?;
    Ok(())
}

/// DELETE-then-INSERT on pg_statistic for a single (rel, attnum, inherit=false)
/// row. pg_statistic has no convenient upsert (the unique index is on
/// `(starelid, staattnum, stainherit)` but it's a system index not
/// advertised for `ON CONFLICT`), so two-step is the conventional
/// pattern — same thing `update_attstats` does in the backend.
/// Slot-1 content for a `pg_statistic` row: either a histogram (range
/// selectivity for ordered types) or an MCV list (equality selectivity for
/// low-cardinality columns — and, crucially, ~0 for values that don't appear,
/// which `1/ndistinct` gets badly wrong).
#[derive(Clone)]
enum Slot1 {
    Histogram {
        type_oid: pg_sys::Oid,
        bounds: Vec<i64>,
    },
    Mcv {
        values: Vec<String>,
        /// `most_common_freqs`, parallel to `values`. Real per-value
        /// frequencies (count / row_count); uniform `1/n` only for partitions
        /// predating `column_valcounts`.
        freqs: Vec<f32>,
    },
}

/// Fully resolved fields for one `pg_statistic` slot (operators + array Datums)
/// ready for the tuple. `stavalues` is optional — a correlation slot carries
/// only `stanumbers` (the coefficient), no values array.
struct Slot1Data {
    stakind: i16,
    staop: pg_sys::Oid,
    stacoll: pg_sys::Oid,
    stanumbers: Option<pg_sys::Datum>,
    stavalues: Option<pg_sys::Datum>,
}

#[allow(clippy::too_many_arguments)]
fn upsert_pg_statistic_row(
    client: &mut SpiClient,
    attrelid: pg_sys::Oid,
    attnum: i16,
    stadistinct: f32,
    stanullfrac: f32,
    stawidth: i32,
    slot: Option<Slot1>,
    correlation: Option<(pg_sys::Oid, f32)>,
    stainherit: bool,
) -> spi::SpiResult<()> {
    let attrelid_int = u32::from(attrelid) as i64;
    client.update(
        "DELETE FROM pg_statistic \
         WHERE starelid = $1::oid AND staattnum = $2::int2 AND stainherit = $3",
        None,
        &[attrelid_int.into(), attnum.into(), stainherit.into()],
    )?;

    // Resolve the slots (operators + array Datums) before forming the tuple.
    // Slot 1: a histogram or MCV. Slot 2 (optional): correlation.
    let mut slots: Vec<Slot1Data> = Vec::new();
    match slot {
        Some(Slot1::Histogram { type_oid, bounds }) if bounds.len() >= 2 => {
            let ltopr = lookup_strategy_operator(client, type_oid, 1); // btree `<`
            if ltopr != pg_sys::InvalidOid {
                slots.push(Slot1Data {
                    stakind: pg_sys::STATISTIC_KIND_HISTOGRAM as i16,
                    staop: ltopr,
                    stacoll: pg_sys::InvalidOid,
                    stanumbers: None,
                    stavalues: Some(unsafe { build_histogram_array(type_oid, &bounds) }),
                });
            }
        }
        Some(Slot1::Mcv { values, freqs }) if values.len() >= 2 => {
            // MCV is written for text columns; resolve the text `=` operator.
            let eqop = lookup_strategy_operator(client, pg_sys::TEXTOID, 3); // btree `=`
            if eqop != pg_sys::InvalidOid {
                let (vals, numbers) = unsafe { build_mcv_arrays(&values, &freqs) };
                slots.push(Slot1Data {
                    stakind: pg_sys::STATISTIC_KIND_MCV as i16,
                    staop: eqop,
                    stacoll: pg_sys::DEFAULT_COLLATION_OID,
                    stanumbers: Some(numbers),
                    stavalues: Some(vals),
                });
            }
        }
        _ => {}
    }
    // Correlation (kind 3) for a physically-sorted column. `staop` is the type's
    // btree `<`; `stanumbers` is the single coefficient; no `stavalues`.
    if let Some((type_oid, corr)) = correlation {
        let ltopr = lookup_strategy_operator(client, type_oid, 1);
        if ltopr != pg_sys::InvalidOid {
            slots.push(Slot1Data {
                stakind: pg_sys::STATISTIC_KIND_CORRELATION as i16,
                staop: ltopr,
                stacoll: pg_sys::InvalidOid,
                stanumbers: Some(unsafe { build_float4_array(&[corr]) }),
                stavalues: None,
            });
        }
    }

    // The catalog DELETE above must be visible to the heap insert's unique
    // index check.
    unsafe { pg_sys::CommandCounterIncrement() };
    unsafe {
        form_and_insert_pg_statistic(
            attrelid,
            attnum,
            stadistinct,
            stanullfrac,
            stawidth,
            slots,
            stainherit,
        )
    };
    Ok(())
}

/// Build a `float4[]` Datum (used for a correlation slot's single coefficient).
unsafe fn build_float4_array(vals: &[f32]) -> pg_sys::Datum {
    let mut datums: Vec<pg_sys::Datum> = vals
        .iter()
        .map(|x| pg_sys::Datum::from(x.to_bits() as usize))
        .collect();
    unsafe {
        let (mut fl, mut fb, mut fa): (i16, bool, std::os::raw::c_char) = (0, false, 0);
        pg_sys::get_typlenbyvalalign(pg_sys::FLOAT4OID, &mut fl, &mut fb, &mut fa);
        let arr = pg_sys::construct_array(
            datums.as_mut_ptr(),
            vals.len() as i32,
            pg_sys::FLOAT4OID,
            fl as i32,
            fb,
            fa,
        );
        pg_sys::Datum::from(arr)
    }
}

/// Build the `stavalues` (text[]) and `stanumbers` (float4[]) array Datums for
/// an MCV slot from a low-cardinality column's distinct values and their
/// `most_common_freqs` (real per-value frequencies; see `mcv_freqs`). The
/// freqs sum to the non-null fraction, so a *present* value estimates at its
/// true frequency (e.g. `event_type='Approved'` ≈ 41%, not a flat `1/n`) while
/// an *absent* value still estimates ~0 (`1 - Σfreq - nullfrac` → 0).
unsafe fn build_mcv_arrays(values: &[String], freqs: &[f32]) -> (pg_sys::Datum, pg_sys::Datum) {
    use pgrx::IntoDatum;
    let n = values.len();
    debug_assert_eq!(values.len(), freqs.len());

    let mut val_datums: Vec<pg_sys::Datum> = values
        .iter()
        .map(|s| {
            s.clone()
                .into_datum()
                .unwrap_or(pg_sys::Datum::from(0usize))
        })
        .collect();
    let mut freq_datums: Vec<pg_sys::Datum> = freqs
        .iter()
        .map(|x| pg_sys::Datum::from(x.to_bits() as usize))
        .collect();

    unsafe {
        let (mut tl, mut tb, mut ta): (i16, bool, std::os::raw::c_char) = (0, false, 0);
        pg_sys::get_typlenbyvalalign(pg_sys::TEXTOID, &mut tl, &mut tb, &mut ta);
        let values_arr = pg_sys::construct_array(
            val_datums.as_mut_ptr(),
            n as i32,
            pg_sys::TEXTOID,
            tl as i32,
            tb,
            ta,
        );
        let (mut fl, mut fb, mut fa): (i16, bool, std::os::raw::c_char) = (0, false, 0);
        pg_sys::get_typlenbyvalalign(pg_sys::FLOAT4OID, &mut fl, &mut fb, &mut fa);
        let numbers_arr = pg_sys::construct_array(
            freq_datums.as_mut_ptr(),
            n as i32,
            pg_sys::FLOAT4OID,
            fl as i32,
            fb,
            fa,
        );
        (
            pg_sys::Datum::from(values_arr),
            pg_sys::Datum::from(numbers_arr),
        )
    }
}

/// Form a `pg_statistic` tuple in C and insert it. Slot 1 carries a
/// `STATISTIC_KIND_HISTOGRAM` (2) when `hist_slot` is `Some((ltopr, array))`:
/// `staop1` is the type's btree `<`, `stacoll1` stays 0 (eligible types are
/// non-collatable), `stavalues1` is the 2-element bounds array. The unused
/// slots are zeroed (stakind/staop/stacoll) or NULL (stanumbers/stavalues).
/// `stanumbers*` stay NULL — we claim no MCV/correlation, only the histogram.
unsafe fn form_and_insert_pg_statistic(
    attrelid: pg_sys::Oid,
    attnum: i16,
    stadistinct: f32,
    stanullfrac: f32,
    stawidth: i32,
    slots: Vec<Slot1Data>,
    stainherit: bool,
) {
    use pgrx::IntoDatum;

    let natts = pg_sys::Natts_pg_statistic as usize;
    let mut values: Vec<pg_sys::Datum> = vec![pg_sys::Datum::from(0usize); natts];
    let mut nulls: Vec<bool> = vec![true; natts];

    let put =
        |values: &mut [pg_sys::Datum], nulls: &mut [bool], anum: u32, d: Option<pg_sys::Datum>| {
            let i = (anum - 1) as usize;
            match d {
                Some(v) => {
                    values[i] = v;
                    nulls[i] = false;
                }
                None => nulls[i] = true,
            }
        };
    // `staopN` / `stacollN` are NOT NULL columns whose unused value is 0.
    // pgrx's `Oid::into_datum()` maps `InvalidOid` (0) to SQL NULL, which
    // would leave a NULL `stacoll1` — and PG then ignores the histogram
    // slot entirely. Build a non-null zero Datum directly instead.
    let oid_d = |o: pg_sys::Oid| Some(pg_sys::Datum::from(u32::from(o) as usize));
    // Same NOT-NULL reasoning for `stakindN` (int2) and the float4 columns:
    // build non-null Datums directly so a 0/0.0 value doesn't become NULL.
    let i16_d = |v: i16| Some(pg_sys::Datum::from(v as u16 as usize));
    let f32_d = |v: f32| Some(pg_sys::Datum::from(v.to_bits() as usize));
    let i32_d = |v: i32| Some(pg_sys::Datum::from(v as u32 as usize));

    put(
        &mut values,
        &mut nulls,
        pg_sys::Anum_pg_statistic_starelid,
        oid_d(attrelid),
    );
    put(
        &mut values,
        &mut nulls,
        pg_sys::Anum_pg_statistic_staattnum,
        attnum.into_datum(),
    );
    put(
        &mut values,
        &mut nulls,
        pg_sys::Anum_pg_statistic_stainherit,
        stainherit.into_datum(),
    );
    put(
        &mut values,
        &mut nulls,
        pg_sys::Anum_pg_statistic_stanullfrac,
        f32_d(stanullfrac),
    );
    put(
        &mut values,
        &mut nulls,
        pg_sys::Anum_pg_statistic_stawidth,
        i32_d(stawidth),
    );
    put(
        &mut values,
        &mut nulls,
        pg_sys::Anum_pg_statistic_stadistinct,
        f32_d(stadistinct),
    );

    // Five (kind, op, coll, numbers, values) slots, filled in order from
    // `slots` (slot 1 = histogram/MCV, slot 2 = correlation, …); the rest empty.
    for slot in 0u32..5 {
        let (kind, op, coll, numbers, vals): (
            i16,
            pg_sys::Oid,
            pg_sys::Oid,
            Option<pg_sys::Datum>,
            Option<pg_sys::Datum>,
        ) = match slots.get(slot as usize) {
            Some(s) => (s.stakind, s.staop, s.stacoll, s.stanumbers, s.stavalues),
            None => (0, pg_sys::InvalidOid, pg_sys::InvalidOid, None, None),
        };
        put(
            &mut values,
            &mut nulls,
            pg_sys::Anum_pg_statistic_stakind1 + slot,
            i16_d(kind),
        );
        put(
            &mut values,
            &mut nulls,
            pg_sys::Anum_pg_statistic_staop1 + slot,
            oid_d(op),
        );
        put(
            &mut values,
            &mut nulls,
            pg_sys::Anum_pg_statistic_stacoll1 + slot,
            oid_d(coll),
        );
        put(
            &mut values,
            &mut nulls,
            pg_sys::Anum_pg_statistic_stanumbers1 + slot,
            numbers,
        );
        put(
            &mut values,
            &mut nulls,
            pg_sys::Anum_pg_statistic_stavalues1 + slot,
            vals,
        );
    }

    unsafe {
        let rel = pg_sys::table_open(
            pg_sys::StatisticRelationId,
            pg_sys::RowExclusiveLock as pg_sys::LOCKMODE,
        );
        let tuple = pg_sys::heap_form_tuple((*rel).rd_att, values.as_mut_ptr(), nulls.as_mut_ptr());
        pg_sys::CatalogTupleInsert(rel, tuple);
        pg_sys::heap_freetuple(tuple);
        pg_sys::table_close(rel, pg_sys::RowExclusiveLock as pg_sys::LOCKMODE);
    }
}

/// Propagate a relcache invalidation so other backends pick up the
/// fresh pg_statistic/pg_class rows on next planning. Compression
/// already holds AccessExclusiveLock on the partition, so this is
/// the only catalog-cache invalidation needed.
fn invalidate_relcache(rel_oid: pg_sys::Oid) {
    unsafe {
        pg_sys::CacheInvalidateRelcacheByRelid(rel_oid);
    }
}

/// Entry point for the standalone `deltax_analyze_partition` UDF. The
/// authoritative per-column distinct counts are the merged-HLL estimates
/// written to `deltax.deltax_partition.column_ndistinct` at compression
/// time, so read those rather than re-deriving from `_colstats`.
///
/// The old fallback — `SUM(per-segment _ndistinct)` from `_colstats` —
/// badly overcounts low-cardinality columns (it double-counts every
/// value that appears in more than one segment: `event_type` with 9
/// distinct values summed to 264 across ~30 segments), which made the
/// planner treat the column as near-unique and pick worse join orders.
/// Columns absent from the persisted map (e.g. a partition compressed by
/// a build that predates HLL persistence) are simply left for PG to
/// default, which is neutral rather than actively wrong.
pub fn analyze_partition_from_catalog(
    client: &mut SpiClient,
    part_rel_oid: pg_sys::Oid,
    colstats_fqn: &str,
    columns: &[ColumnMeta],
    row_count: i64,
) -> spi::SpiResult<()> {
    // Read the persisted merged-HLL map (column name -> ndistinct) for
    // this partition. column_ndistinct is keyed by column name, the same
    // key write_partition_stats expects.
    let part_oid_int = u32::from(part_rel_oid) as i64;
    let json_text: Option<String> = client
        .select(
            "SELECT column_ndistinct::text FROM deltax.deltax_partition \
             WHERE table_name = (SELECT relname FROM pg_class WHERE oid = $1::oid) \
               AND is_compressed = true",
            None,
            &[part_oid_int.into()],
        )?
        .next()
        .and_then(|row| {
            row.get_datum_by_ordinal(1)
                .ok()
                .and_then(|d| d.value::<String>().ok().flatten())
        });

    let mut col_ndistinct: HashMap<String, i64> = HashMap::new();
    if let Some(text) = json_text {
        crate::scan::cost::parse_ndistinct_json(&text, &mut col_ndistinct);
    }
    // Cap at row_count to keep stadistinct_value's sign convention sane.
    for v in col_ndistinct.values_mut() {
        *v = (*v).min(row_count);
    }

    write_partition_stats(
        client,
        part_rel_oid,
        &col_ndistinct,
        row_count,
        colstats_fqn,
        columns,
    )
}

/// Deserialize a partition's `column_hll` JSON (`{col_name: <sketch>}`) and
/// union each column's sketch into the table-wide accumulator. Sketches that
/// fail to parse are skipped (the caller falls back to the heuristic).
fn merge_partition_hll_json(text: &str, acc: &mut HashMap<String, CardinalityEstimator<u64>>) {
    let Ok(serde_json::Value::Object(obj)) = serde_json::from_str::<serde_json::Value>(text) else {
        return;
    };
    for (name, val) in obj {
        if let Ok(sketch) = serde_json::from_value::<CardinalityEstimator<u64>>(val) {
            match acc.get_mut(&name) {
                Some(existing) => existing.merge(&sketch),
                None => {
                    acc.insert(name, sketch);
                }
            }
        }
    }
}

/// Union a partition's `column_valmap` JSON (`{col_name: ["v0", "v1", ...]}`)
/// into a table-wide per-column distinct-value set. Per-partition valmaps only
/// list the values present in that partition, so the union across all
/// partitions is the table's full value set for the (low-cardinality) column.
fn union_valmap_json(text: &str, acc: &mut HashMap<String, std::collections::BTreeSet<String>>) {
    let Ok(serde_json::Value::Object(obj)) = serde_json::from_str::<serde_json::Value>(text) else {
        return;
    };
    for (name, val) in obj {
        if let serde_json::Value::Array(arr) = val {
            let set = acc.entry(name).or_default();
            for v in arr {
                if let serde_json::Value::String(s) = v {
                    set.insert(s);
                }
            }
        }
    }
}

/// Merge per-partition distinct counts into a table-wide estimate. Columns
/// whose per-partition value ranges are mostly disjoint (e.g. the time column,
/// or order_id correlated with time) have additive distinct counts → SUM;
/// columns with overlapping ranges (the same low-cardinality values in every
/// partition, e.g. `event_type`) repeat → MAX. Capped at the table row count.
fn merge_ndistinct(nds: &[i64], ranges: &[(i64, i64)], total_rows: i64) -> i64 {
    if nds.is_empty() {
        return 0;
    }
    let sum: i64 = nds.iter().sum();
    let max: i64 = *nds.iter().max().unwrap();
    let disjoint = if ranges.len() == nds.len() && ranges.len() > 1 {
        let mut r = ranges.to_vec();
        r.sort_unstable();
        let overlaps = (1..r.len()).filter(|&i| r[i].0 <= r[i - 1].1).count();
        (overlaps as f64) < 0.5 * ((r.len() - 1) as f64)
    } else {
        false
    };
    let nd = if disjoint { sum } else { max };
    nd.clamp(1, total_rows.max(1))
}

/// Build a table-wide histogram's bound list from per-partition `[min, max]`:
/// the sorted per-partition minimums plus the global maximum. For roughly
/// equal-sized partitions this is an equi-depth histogram over the order-by
/// column; for columns with a constant range it collapses to `[min, max]`.
/// Maximum histogram bounds we emit (PG's default `statistics_target` of 100
/// buckets → 101 boundaries).
const MAX_HISTOGRAM_BOUNDS: usize = 101;

/// Build multi-bucket histogram bounds from per-segment (or per-partition)
/// minimums plus the column max. Because segments are ~equal-sized, the
/// minimums are ~equi-depth bucket boundaries (this is what makes the parent
/// histogram robust to unequal partition sizes — #7). Sorted, deduped (DATE at
/// day granularity), and downsampled to at most `max_bounds`. `None` for fewer
/// than 2 distinct bounds.
fn build_histogram_bounds(
    type_oid: pg_sys::Oid,
    mut mins: Vec<i64>,
    global_max: i64,
    max_bounds: usize,
) -> Option<Vec<i64>> {
    if !histogram_type_eligible(type_oid) || mins.is_empty() {
        return None;
    }
    mins.sort_unstable();
    let lo = mins[0];
    let largest_min = *mins.last().unwrap();
    let range = (global_max as i128) - (lo as i128);
    if range <= 0 {
        return None; // constant (or inverted) — no histogram
    }
    // Per-segment mins only partition the value range when the column is
    // (nearly) physically sorted — then the segment mins span most of [min, max].
    // For an unsorted column the mins cluster near the column minimum and would
    // make a skewed histogram, so fall back to the uniform 2-point [min, max].
    let well_distributed = ((largest_min as i128) - (lo as i128)) * 2 >= range;
    if !well_distributed {
        return build_two_point(type_oid, lo, global_max);
    }
    mins.push(global_max);
    mins.sort_unstable();
    if type_oid == pg_sys::DATEOID {
        mins.dedup_by_key(|b| *b / 86_400_000_000);
    } else {
        mins.dedup();
    }
    if mins.len() < 2 {
        return None;
    }
    Some(downsample_bounds(mins, max_bounds))
}

/// The 2-point `[min, max]` histogram (uniform assumption), gated on the same
/// eligibility/constant checks as `histogram_eligible`.
fn build_two_point(type_oid: pg_sys::Oid, lo: i64, hi: i64) -> Option<Vec<i64>> {
    histogram_eligible(type_oid, lo, hi).then(|| vec![lo, hi])
}

/// Pick at most `max_bounds` evenly-spaced values from an already-sorted list,
/// always keeping the first and last (so the histogram still spans [min, max]).
fn downsample_bounds(bounds: Vec<i64>, max_bounds: usize) -> Vec<i64> {
    let n = bounds.len();
    if n <= max_bounds || max_bounds < 2 {
        return bounds;
    }
    let mut out = Vec::with_capacity(max_bounds);
    for i in 0..max_bounds {
        out.push(bounds[i * (n - 1) / (max_bounds - 1)]);
    }
    out.dedup();
    out
}

fn parent_histogram_bounds(type_oid: pg_sys::Oid, ranges: &[(i64, i64)]) -> Option<Vec<i64>> {
    if !histogram_type_eligible(type_oid) || ranges.is_empty() {
        return None;
    }
    let mut bounds: Vec<i64> = ranges.iter().map(|r| r.0).collect();
    bounds.sort_unstable();
    bounds.push(ranges.iter().map(|r| r.1).max().unwrap());
    // Strictly ascending: drop adjacent duplicates (for DATE, at day
    // granularity, since the element Datum truncates to days).
    if type_oid == pg_sys::DATEOID {
        bounds.dedup_by_key(|b| *b / 86_400_000_000);
    } else {
        bounds.dedup();
    }
    (bounds.len() >= 2).then_some(bounds)
}

/// Populate the parent relation's `pg_class.reltuples` + `pg_statistic`
/// (`stainherit = true`, the inheritance-tree stats PG reads for a
/// partitioned parent) by merging the per-partition stats persisted in
/// `deltax.deltax_partition`. DeltaXAppend scans the parent as one baserel,
/// so the planner reads JOIN selectivity (e.g. `oe.order_id = oi.order_id`)
/// from the parent's stats — without them, join cardinality is badly
/// mis-estimated and the planner picks hash joins where nested loops win.
pub fn write_table_stats(client: &mut SpiClient, schema: &str, table: &str) -> spi::SpiResult<()> {
    let parent_oid: pg_sys::Oid = {
        let fqn = format!("{}.{}", quote_ident(schema), quote_ident(table));
        let mut oid = pg_sys::InvalidOid;
        for row in client.select(&format!("SELECT '{}'::regclass::oid", fqn), None, &[])? {
            oid = row
                .get_datum_by_ordinal(1)
                .ok()
                .and_then(|d| d.value::<pg_sys::Oid>().ok().flatten())
                .unwrap_or(pg_sys::InvalidOid);
        }
        oid
    };
    if parent_oid == pg_sys::InvalidOid {
        return Ok(());
    }

    // Gather per-partition (ndistinct, minmax) lists per column + total rows,
    // plus a merged table-wide HLL per column (accurate global distinct count,
    // unlike the SUM/MAX heuristic over per-partition estimates).
    let mut total_rows: i64 = 0;
    let mut nd_by_col: HashMap<String, Vec<i64>> = HashMap::new();
    let mut mm_by_col: HashMap<String, Vec<(i64, i64)>> = HashMap::new();
    let mut hll_by_col: HashMap<String, CardinalityEstimator<u64>> = HashMap::new();
    // Table-wide distinct value lists for low-cardinality columns (the union
    // of the per-partition valmaps), used to write an MCV list.
    let mut vm_by_col: HashMap<String, std::collections::BTreeSet<String>> = HashMap::new();
    // Table-wide per-value counts (summed across partitions) for those same
    // columns → real parent `most_common_freqs`.
    let mut vc_by_col: HashMap<String, HashMap<String, i64>> = HashMap::new();
    // Table-wide partial-MCV counts (summed across partitions) for high-card
    // text columns → parent partial MCV with real `stadistinct`.
    let mut mcv_by_col: HashMap<String, HashMap<String, i64>> = HashMap::new();
    let query = "SELECT row_count, column_ndistinct::text, column_minmax::text, column_hll::text, \
                column_valmap::text, column_valcounts::text, column_mcv::text \
                 FROM deltax.deltax_partition \
                 WHERE is_compressed = true AND deltatable_id = (\
                     SELECT id FROM deltax.deltax_deltatable \
                     WHERE schema_name = $1 AND table_name = $2)";
    for row in client.select(query, None, &[schema.into(), table.into()])? {
        let rc: i64 = row
            .get_datum_by_ordinal(1)
            .ok()
            .and_then(|d| d.value::<i64>().ok().flatten())
            .unwrap_or(0);
        total_rows += rc;
        if let Some(text) = row
            .get_datum_by_ordinal(2)
            .ok()
            .and_then(|d| d.value::<String>().ok().flatten())
        {
            let mut m = HashMap::new();
            crate::scan::cost::parse_ndistinct_json(&text, &mut m);
            for (name, nd) in m {
                nd_by_col.entry(name).or_default().push(nd);
            }
        }
        if let Some(text) = row
            .get_datum_by_ordinal(3)
            .ok()
            .and_then(|d| d.value::<String>().ok().flatten())
            && let Some(m) = crate::scan::cost::parse_minmax_json(&text)
        {
            for (name, mm) in m {
                mm_by_col.entry(name).or_default().push(mm);
            }
        }
        if let Some(text) = row
            .get_datum_by_ordinal(4)
            .ok()
            .and_then(|d| d.value::<String>().ok().flatten())
        {
            merge_partition_hll_json(&text, &mut hll_by_col);
        }
        if let Some(text) = row
            .get_datum_by_ordinal(5)
            .ok()
            .and_then(|d| d.value::<String>().ok().flatten())
        {
            union_valmap_json(&text, &mut vm_by_col);
        }
        if let Some(text) = row
            .get_datum_by_ordinal(6)
            .ok()
            .and_then(|d| d.value::<String>().ok().flatten())
        {
            for (name, counts) in parse_valcounts_json(&text) {
                let acc = vc_by_col.entry(name).or_default();
                for (v, c) in counts {
                    *acc.entry(v).or_insert(0) += c;
                }
            }
        }
        if let Some(text) = row
            .get_datum_by_ordinal(7)
            .ok()
            .and_then(|d| d.value::<String>().ok().flatten())
        {
            for (name, counts) in parse_valcounts_json(&text) {
                let acc = mcv_by_col.entry(name).or_default();
                for (v, c) in counts {
                    *acc.entry(v).or_insert(0) += c;
                }
            }
        }
    }
    if total_rows <= 0 {
        return Ok(());
    }

    let attrs = load_pg_attribute(client, parent_oid)?;

    // Parent nullfrac/stawidth per column = the row-count-weighted average of
    // the children's values (written by write_partition_stats). A hardcoded
    // nullfrac 0 badly over-estimates `<> const` / `IS NOT NULL` on a mostly-
    // NULL column; a flat-32 stawidth mis-sizes sort/hash work-mem on wide text.
    let nullfrac_by_attnum = load_parent_nullfrac(client, parent_oid).unwrap_or_default();
    let stawidth_by_attnum = load_parent_stawidth(client, parent_oid).unwrap_or_default();
    // Per-segment mins across all partitions → a fine, ~equi-depth parent
    // histogram (#7); falls back to per-partition mins when unavailable.
    let seg_mins_by_name =
        load_parent_segment_mins(client, schema, table, &attrs).unwrap_or_default();
    let parent_stawidth = |attr: &AttrInfo| -> i32 {
        stawidth_by_attnum
            .get(&attr.attnum)
            .copied()
            .filter(|&w| w > 0)
            .unwrap_or_else(|| stawidth_for_attlen(attr.attlen))
    };
    let avg_tuple_width = attrs
        .iter()
        .map(|a| parent_stawidth(a) as i64)
        .sum::<i64>()
        .max(32);

    for attr in &attrs {
        let empty_nd: Vec<i64> = Vec::new();
        let empty_mm: Vec<(i64, i64)> = Vec::new();
        let nds = nd_by_col.get(&attr.attname).unwrap_or(&empty_nd);
        let mms = mm_by_col.get(&attr.attname).unwrap_or(&empty_mm);

        // Prefer the merged table-wide HLL estimate (accurate global distinct);
        // fall back to the SUM/MAX heuristic for partitions that predate
        // `column_hll`.
        let merged_nd = match hll_by_col.get(&attr.attname) {
            Some(hll) => (hll.estimate() as i64).clamp(1, total_rows.max(1)),
            None => merge_ndistinct(nds, mms, total_rows),
        };
        let stanullfrac = if attr.attnotnull {
            0.0
        } else {
            nullfrac_by_attnum
                .get(&attr.attnum)
                .copied()
                .unwrap_or(0.0)
                .clamp(0.0, 1.0)
        };
        let stawidth = parent_stawidth(attr);

        // Slot 1: a histogram for ordered types (range selectivity), else an
        // MCV list for a low-cardinality column whose full value set we know
        // from the valmap union (equality selectivity, and ~0 for values that
        // never appear — `1/ndistinct` gets those badly wrong; see Q19). When
        // writing an MCV, stadistinct must equal the value count so non-MCV
        // values estimate 0.
        // Prefer a fine per-segment histogram (pooled across partitions);
        // fall back to per-partition mins. Both gated on histogram eligibility
        // via the bound builders.
        let histogram = match seg_mins_by_name.get(&attr.attname) {
            Some(mins) if !mins.is_empty() => {
                let global_max = mms.iter().map(|r| r.1).max().unwrap_or(i64::MIN);
                build_histogram_bounds(
                    attr.atttypid,
                    mins.clone(),
                    global_max,
                    MAX_HISTOGRAM_BOUNDS,
                )
            }
            _ => parent_histogram_bounds(attr.atttypid, mms),
        }
        .map(|bounds| Slot1::Histogram {
            type_oid: attr.atttypid,
            bounds,
        });
        // Multi-bucket histogram → physically-sorted column → correlation ~1.0.
        let correlation = match &histogram {
            Some(Slot1::Histogram { type_oid, bounds }) if bounds.len() > 2 => {
                Some((*type_oid, 1.0f32))
            }
            _ => None,
        };
        let (slot, eff_ndistinct) = match histogram {
            Some(h) => (Some(h), merged_nd),
            // Only write an MCV when the valmap union covers EVERY distinct
            // value (count matches the merged-HLL n_distinct). A partition with
            // >32 distinct values contributes no valmap, so the union can be a
            // strict subset — writing an MCV from it would both miss values
            // (absent-value selectivity wrong) and, worse, advertise a tiny
            // n_distinct for a high-cardinality column (e.g. backup_processor:
            // union of 12 vs ~634 actual), which mis-plans Top-N / range
            // queries.
            None if is_text_type(attr.atttypid) => {
                if let Some(set) = vm_by_col
                    .get(&attr.attname)
                    .filter(|s| s.len() >= 2 && s.len() as i64 == merged_nd)
                {
                    // Complete MCV: the valmap union covers every distinct value
                    // (count matches the merged-HLL n_distinct), so stadistinct =
                    // value count and absent values estimate ~0.
                    let values: Vec<String> = set.iter().cloned().collect();
                    let empty = HashMap::new();
                    let counts = vc_by_col.get(&attr.attname).unwrap_or(&empty);
                    let freqs = mcv_freqs(&values, counts, total_rows);
                    let n = values.len() as i64;
                    (Some(Slot1::Mcv { values, freqs }), n)
                } else if let Some(counts) = mcv_by_col.get(&attr.attname).filter(|m| m.len() >= 2)
                {
                    // Partial MCV (high-card heavy hitters, merged across
                    // partitions): keep the real merged HLL n_distinct so PG
                    // estimates the tail from the remainder.
                    let (values, freqs) = ordered_mcv(counts, total_rows, 100);
                    (Some(Slot1::Mcv { values, freqs }), merged_nd)
                } else {
                    (None, merged_nd)
                }
            }
            None => (None, merged_nd),
        };
        let stadistinct = stadistinct_value(eff_ndistinct, total_rows);

        upsert_pg_statistic_row(
            client,
            parent_oid,
            attr.attnum,
            stadistinct,
            stanullfrac,
            stawidth,
            slot.clone(),
            correlation,
            true,
        )?;
        // Mirror the same stats to the stainherit=false variant: when
        // `pg_deltax.flatten_partitions` plans the parent un-expanded
        // (rte->inh = false), examine_variable() reads the non-inherited
        // rows — without this mirror, every selectivity there would fall
        // back to defaults.
        upsert_pg_statistic_row(
            client,
            parent_oid,
            attr.attnum,
            stadistinct,
            stanullfrac,
            stawidth,
            slot,
            correlation,
            false,
        )?;
    }

    update_reltuples(client, parent_oid, total_rows, avg_tuple_width as i32)?;
    invalidate_relcache(parent_oid);
    Ok(())
}

/// Row-count-weighted average of the child partitions' `stanullfrac` per
/// attribute, used as the parent's nullfrac. Children are analyzed before the
/// table-level pass, so their `pg_statistic` rows already exist; partitions
/// without stats simply don't contribute.
fn load_parent_nullfrac(
    client: &mut SpiClient,
    parent_oid: pg_sys::Oid,
) -> spi::SpiResult<HashMap<i16, f32>> {
    let parent_int = u32::from(parent_oid) as i64;
    let query = "SELECT s.staattnum::int2, \
                        (SUM(s.stanullfrac::float8 * GREATEST(c.reltuples, 0)::float8) \
                         / NULLIF(SUM(GREATEST(c.reltuples, 0)::float8), 0))::float4 \
                 FROM pg_statistic s \
                 JOIN pg_inherits i ON i.inhrelid = s.starelid \
                 JOIN pg_class c ON c.oid = s.starelid \
                 WHERE i.inhparent = $1::oid AND s.stainherit = false \
                 GROUP BY s.staattnum";
    let mut out: HashMap<i16, f32> = HashMap::new();
    for row in client.select(query, None, &[parent_int.into()])? {
        let attnum: i16 = row
            .get_datum_by_ordinal(1)
            .ok()
            .and_then(|d| d.value::<i16>().ok().flatten())
            .unwrap_or(0);
        let nf: f32 = row
            .get_datum_by_ordinal(2)
            .ok()
            .and_then(|d| d.value::<f32>().ok().flatten())
            .unwrap_or(0.0);
        if attnum > 0 {
            out.insert(attnum, nf);
        }
    }
    Ok(out)
}

/// Gather per-segment `_min` values per column NAME across all compressed
/// partitions of the deltatable, for a fine, ~equi-depth parent histogram (#7).
/// Segments are ~equal-sized, so pooling every partition's segment mins gives
/// boundaries weighted by partition size automatically — fixing the old
/// per-partition-mins histogram that assumed equal partitions. The colstats
/// `_col_idx` is the non-segment-by column position, mapped here to the column
/// name via pg_attribute order minus the deltatable's `segment_by`.
fn load_parent_segment_mins(
    client: &mut SpiClient,
    schema: &str,
    table: &str,
    attrs: &[AttrInfo],
) -> spi::SpiResult<HashMap<String, Vec<i64>>> {
    // Segment-by columns (not in colstats) — skipped when numbering _col_idx.
    let mut segment_by: std::collections::HashSet<String> = std::collections::HashSet::new();
    for row in client.select(
        "SELECT unnest(segment_by)::text FROM deltax.deltax_deltatable \
         WHERE schema_name = $1 AND table_name = $2",
        None,
        &[schema.into(), table.into()],
    )? {
        if let Some(s) = row
            .get_datum_by_ordinal(1)
            .ok()
            .and_then(|d| d.value::<String>().ok().flatten())
        {
            segment_by.insert(s);
        }
    }
    // _col_idx (non-seg position, attnum order) → column name.
    let mut idx_to_name: HashMap<i32, String> = HashMap::new();
    let mut idx: i32 = 0;
    for a in attrs {
        if segment_by.contains(&a.attname) {
            continue;
        }
        idx_to_name.insert(idx, a.attname.clone());
        idx += 1;
    }

    let mut out: HashMap<String, Vec<i64>> = HashMap::new();
    let parts: Vec<String> = client
        .select(
            "SELECT table_name FROM deltax.deltax_partition \
             WHERE is_compressed = true AND deltatable_id = (\
                 SELECT id FROM deltax.deltax_deltatable \
                 WHERE schema_name = $1 AND table_name = $2)",
            None,
            &[schema.into(), table.into()],
        )?
        .filter_map(|row| {
            row.get_datum_by_ordinal(1)
                .ok()
                .and_then(|d| d.value::<String>().ok().flatten())
        })
        .collect();
    for part in parts {
        let colstats_fqn = format!(
            "\"_deltax_compressed\".\"{}_colstats\"",
            part.replace('"', "\"\"")
        );
        let per_idx = match load_colstats_segment_mins(client, &colstats_fqn) {
            Ok(m) => m,
            Err(_) => continue, // partition predating colstats / transient
        };
        for (i, mins) in per_idx {
            if let Some(name) = idx_to_name.get(&i) {
                out.entry(name.clone()).or_default().extend(mins);
            }
        }
    }
    Ok(out)
}

/// Row-count-weighted average of the child partitions' `stawidth` per
/// attribute, used as the parent's width. Mirrors `load_parent_nullfrac`:
/// children are analyzed before the table-level pass, so their accurate
/// (text avg-length-based) widths already exist. Rounded to an int; attributes
/// without child stats simply don't contribute (caller falls back to attlen/32).
fn load_parent_stawidth(
    client: &mut SpiClient,
    parent_oid: pg_sys::Oid,
) -> spi::SpiResult<HashMap<i16, i32>> {
    let parent_int = u32::from(parent_oid) as i64;
    let query = "SELECT s.staattnum::int2, \
                        round(SUM(s.stawidth::float8 * GREATEST(c.reltuples, 0)::float8) \
                         / NULLIF(SUM(GREATEST(c.reltuples, 0)::float8), 0))::int4 \
                 FROM pg_statistic s \
                 JOIN pg_inherits i ON i.inhrelid = s.starelid \
                 JOIN pg_class c ON c.oid = s.starelid \
                 WHERE i.inhparent = $1::oid AND s.stainherit = false \
                 GROUP BY s.staattnum";
    let mut out: HashMap<i16, i32> = HashMap::new();
    for row in client.select(query, None, &[parent_int.into()])? {
        let attnum: i16 = row
            .get_datum_by_ordinal(1)
            .ok()
            .and_then(|d| d.value::<i16>().ok().flatten())
            .unwrap_or(0);
        let w: i32 = row
            .get_datum_by_ordinal(2)
            .ok()
            .and_then(|d| d.value::<i32>().ok().flatten())
            .unwrap_or(0);
        if attnum > 0 && w > 0 {
            out.insert(attnum, w);
        }
    }
    Ok(out)
}

/// Minimal SQL identifier quoting for building a `schema.table` regclass
/// literal. Doubles embedded quotes; always wraps in double quotes.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stawidth_for_attlen_uses_fixed_width_directly() {
        // Positive attlen → bytes-per-row for a fixed-width type. Negative
        // (varlena, cstring) → conservative 32-byte default per the comment.
        assert_eq!(stawidth_for_attlen(1), 1);
        assert_eq!(stawidth_for_attlen(8), 8);
        assert_eq!(stawidth_for_attlen(16), 16);
        assert_eq!(stawidth_for_attlen(-1), 32);
        assert_eq!(stawidth_for_attlen(-2), 32);
        assert_eq!(stawidth_for_attlen(0), 32);
    }

    fn attr(attlen: i16, atttypid: pg_sys::Oid) -> AttrInfo {
        AttrInfo {
            attname: "c".to_string(),
            attnum: 1,
            attlen,
            atttypid,
            attnotnull: false,
        }
    }

    #[test]
    fn ordered_mcv_sorts_by_count_desc_and_caps() {
        let mut counts = HashMap::new();
        counts.insert("A".to_string(), 50);
        counts.insert("B".to_string(), 30);
        counts.insert("C".to_string(), 20);
        // Descending by count, freq = count/row_count.
        let (vals, freqs) = ordered_mcv(&counts, 100, 10);
        assert_eq!(vals, vec!["A", "B", "C"]);
        assert!((freqs[0] - 0.5).abs() < 1e-6);
        assert!((freqs[1] - 0.3).abs() < 1e-6);
        assert!((freqs[2] - 0.2).abs() < 1e-6);
        // Cap keeps only the top-N.
        let (vals2, freqs2) = ordered_mcv(&counts, 100, 2);
        assert_eq!(vals2, vec!["A", "B"]);
        assert_eq!(freqs2.len(), 2);
    }

    #[test]
    fn column_stawidth_uses_avg_length_for_text() {
        // Fixed-width types ignore colstats and use attlen.
        assert_eq!(
            column_stawidth(&attr(8, pg_sys::INT8OID), Some(&(100, 999))),
            8
        );

        // Text: avg length (len_sum / nonnull) + varlena header. 6000/100 = 60
        // chars, < 127 → +1 short header = 61 (vs the flat 32).
        assert_eq!(
            column_stawidth(&attr(-1, pg_sys::TEXTOID), Some(&(100, 6000))),
            61
        );
        // Wide text: 200 chars avg → +4 long header = 204.
        assert_eq!(
            column_stawidth(&attr(-1, pg_sys::TEXTOID), Some(&(10, 2000))),
            204
        );
        // Short codes get a small width, not 32.
        assert_eq!(
            column_stawidth(&attr(-1, pg_sys::TEXTOID), Some(&(100, 300))),
            4
        );
    }

    #[test]
    fn column_stawidth_falls_back_to_32() {
        // Text with no colstats aggregate, or zero rows → 32 default.
        assert_eq!(column_stawidth(&attr(-1, pg_sys::TEXTOID), None), 32);
        assert_eq!(
            column_stawidth(&attr(-1, pg_sys::TEXTOID), Some(&(0, 0))),
            32
        );
        // Non-text varlena (jsonb has no length `_sum`) → 32, never the numeric
        // `_sum` misread as a length.
        assert_eq!(
            column_stawidth(&attr(-1, pg_sys::JSONBOID), Some(&(100, 999999))),
            32
        );
    }

    #[test]
    fn partition_hll_serialize_merge_roundtrip_unions_distinct() {
        // Two partitions with disjoint value sets; the merged estimate should
        // approximate the union cardinality (~2000), not either half.
        let mut a = CardinalityEstimator::<u64>::new();
        for v in 0u64..1000 {
            a.insert(&v);
        }
        let mut b = CardinalityEstimator::<u64>::new();
        for v in 1000u64..2000 {
            b.insert(&v);
        }
        let ja =
            crate::compress::serialize_partition_hll(&["k"], std::slice::from_ref(&a)).unwrap();
        let jb =
            crate::compress::serialize_partition_hll(&["k"], std::slice::from_ref(&b)).unwrap();

        let mut acc: HashMap<String, CardinalityEstimator<u64>> = HashMap::new();
        merge_partition_hll_json(&ja, &mut acc);
        merge_partition_hll_json(&jb, &mut acc);

        let est = acc.get("k").unwrap().estimate() as i64;
        assert!((1800..=2200).contains(&est), "union estimate off: {est}");
    }

    #[test]
    fn merge_partition_hll_json_ignores_garbage() {
        let mut acc: HashMap<String, CardinalityEstimator<u64>> = HashMap::new();
        merge_partition_hll_json("not json", &mut acc);
        merge_partition_hll_json("[1,2,3]", &mut acc); // not an object
        assert!(acc.is_empty());
    }

    #[test]
    fn merge_ndistinct_sums_disjoint_ranges() {
        // Three partitions with non-overlapping value ranges → additive.
        let nds = [100, 100, 100];
        let ranges = [(0, 99), (100, 199), (200, 299)];
        assert_eq!(merge_ndistinct(&nds, &ranges, 10_000), 300);
    }

    #[test]
    fn merge_ndistinct_maxes_overlapping_ranges() {
        // Same low-cardinality values in every partition (identical range) →
        // the distinct count doesn't grow; take the max, not the sum.
        let nds = [9, 8, 9];
        let ranges = [(0, 8), (0, 8), (0, 8)];
        assert_eq!(merge_ndistinct(&nds, &ranges, 10_000), 9);
    }

    #[test]
    fn merge_ndistinct_caps_at_row_count() {
        let nds = [800, 800];
        let ranges = [(0, 799), (800, 1599)];
        assert_eq!(merge_ndistinct(&nds, &ranges, 1000), 1000);
    }

    #[test]
    fn parent_histogram_bounds_builds_sorted_multibucket() {
        // Disjoint, out-of-order partition ranges → sorted mins + global max.
        let ranges = [(200, 299), (0, 99), (100, 199)];
        let b = parent_histogram_bounds(pg_sys::INT4OID, &ranges).unwrap();
        assert_eq!(b, vec![0, 100, 200, 299]);
    }

    #[test]
    fn build_histogram_bounds_from_segment_mins() {
        // Per-segment mins + global max → sorted multi-bucket bounds.
        let b = build_histogram_bounds(pg_sys::INT4OID, vec![0, 100, 200], 299, 101).unwrap();
        assert_eq!(b, vec![0, 100, 200, 299]);
        // Single segment → 2-point [min, max].
        let b = build_histogram_bounds(pg_sys::INT4OID, vec![0], 99, 101).unwrap();
        assert_eq!(b, vec![0, 99]);
        // Constant → fewer than 2 distinct bounds → None.
        assert!(build_histogram_bounds(pg_sys::INT4OID, vec![5], 5, 101).is_none());
        // Floats are eligible (#4).
        assert!(build_histogram_bounds(pg_sys::FLOAT8OID, vec![0, 50], 100, 101).is_some());
        // Text never.
        assert!(build_histogram_bounds(pg_sys::TEXTOID, vec![0, 5], 9, 101).is_none());
        // Clustered mins (unsorted column: all near the min) → collapse to the
        // 2-point [min, max] rather than a skewed multi-bucket histogram.
        assert_eq!(
            build_histogram_bounds(pg_sys::INT8OID, vec![0, 1, 2], 1000, 101).unwrap(),
            vec![0, 1000]
        );
    }

    #[test]
    fn downsample_bounds_keeps_endpoints_and_caps() {
        // 11 sorted values capped at 5 → 5 evenly-spaced, first & last kept.
        let out = downsample_bounds((0..11).collect(), 5);
        assert_eq!(out.len(), 5);
        assert_eq!(out[0], 0);
        assert_eq!(*out.last().unwrap(), 10);
        assert!(out.windows(2).all(|w| w[0] < w[1]), "ascending: {out:?}");
        // Below the cap → unchanged.
        assert_eq!(downsample_bounds(vec![1, 2, 3], 100), vec![1, 2, 3]);
    }

    #[test]
    fn build_histogram_bounds_downsamples_many_segments() {
        // 1000 segment mins + max → capped at MAX_HISTOGRAM_BOUNDS, spanning ends.
        let mins: Vec<i64> = (0..1000).collect();
        let b = build_histogram_bounds(pg_sys::INT8OID, mins, 1000, MAX_HISTOGRAM_BOUNDS).unwrap();
        assert!(b.len() <= MAX_HISTOGRAM_BOUNDS, "len {}", b.len());
        assert_eq!(b[0], 0);
        assert_eq!(*b.last().unwrap(), 1000);
        assert!(b.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn parent_histogram_bounds_collapses_constant_range_to_two_points() {
        let ranges = [(0, 27), (0, 27), (0, 27)];
        let b = parent_histogram_bounds(pg_sys::INT4OID, &ranges).unwrap();
        assert_eq!(b, vec![0, 27]);
    }

    #[test]
    fn parent_histogram_bounds_none_for_ineligible_or_empty() {
        assert!(parent_histogram_bounds(pg_sys::TEXTOID, &[(0, 9)]).is_none());
        assert!(parent_histogram_bounds(pg_sys::INT4OID, &[]).is_none());
        // All identical single value → fewer than 2 distinct bounds.
        assert!(parent_histogram_bounds(pg_sys::INT4OID, &[(5, 5), (5, 5)]).is_none());
    }

    #[test]
    fn histogram_eligible_accepts_ordered_int_and_time_types() {
        assert!(histogram_eligible(pg_sys::INT2OID, 1, 2));
        assert!(histogram_eligible(pg_sys::INT4OID, 100, 9000));
        assert!(histogram_eligible(pg_sys::INT8OID, -5, 5));
        assert!(histogram_eligible(
            pg_sys::TIMESTAMPOID,
            1_000_000,
            2_000_000
        ));
        assert!(histogram_eligible(
            pg_sys::TIMESTAMPTZOID,
            1_000_000,
            2_000_000
        ));
    }

    #[test]
    fn histogram_eligible_date_compares_at_day_granularity() {
        let day = 86_400_000_000i64;
        // Spans two distinct days → eligible.
        assert!(histogram_eligible(pg_sys::DATEOID, day, 3 * day));
        // Same day (µs apart) → not eligible (would be a constant histogram).
        assert!(!histogram_eligible(pg_sys::DATEOID, 1, 100));
    }

    #[test]
    fn histogram_eligible_rejects_constant_and_unsupported() {
        // Constant column (lo == hi) and inverted bounds → skip.
        assert!(!histogram_eligible(pg_sys::INT4OID, 7, 7));
        assert!(!histogram_eligible(pg_sys::INT4OID, 9, 1));
        // Text has no histogram-decodable encoding → skip.
        assert!(!histogram_eligible(pg_sys::TEXTOID, 0, 10));
    }

    #[test]
    fn histogram_eligible_accepts_floats() {
        // FLOAT4/8 use the order-preserving i64 encoding (#4).
        assert!(histogram_eligible(pg_sys::FLOAT4OID, 0, 10));
        assert!(histogram_eligible(pg_sys::FLOAT8OID, -5, 5));
        // Constant still rejected.
        assert!(!histogram_eligible(pg_sys::FLOAT8OID, 3, 3));
    }

    #[test]
    fn mcv_freqs_uses_real_per_value_counts() {
        // Skewed enum: 'A' 80%, 'B' 20% over 100 rows → real freqs, not 1/2.
        let values = vec!["A".to_string(), "B".to_string()];
        let mut counts = HashMap::new();
        counts.insert("A".to_string(), 80);
        counts.insert("B".to_string(), 20);
        let f = mcv_freqs(&values, &counts, 100);
        assert!((f[0] - 0.8).abs() < 1e-6, "freq(A)={}", f[0]);
        assert!((f[1] - 0.2).abs() < 1e-6, "freq(B)={}", f[1]);
        // Complete coverage, no nulls → freqs sum to ~1.0.
        assert!((f.iter().sum::<f32>() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn mcv_freqs_sums_below_one_with_nulls() {
        // 60 non-null rows out of 100 → freqs sum to the non-null fraction
        // (0.6), which is what makes an absent value estimate ~0 in PG's
        // `1 - Σfreq - nullfrac`.
        let values = vec!["A".to_string(), "B".to_string()];
        let mut counts = HashMap::new();
        counts.insert("A".to_string(), 40);
        counts.insert("B".to_string(), 20);
        let f = mcv_freqs(&values, &counts, 100);
        assert!(
            (f.iter().sum::<f32>() - 0.6).abs() < 1e-6,
            "sum={}",
            f.iter().sum::<f32>()
        );
    }

    #[test]
    fn mcv_freqs_falls_back_to_uniform_without_counts() {
        // Partition predating column_valcounts → empty counts → uniform 1/n
        // (the prior behaviour), so the change is a no-op for old data.
        let values = vec![
            "A".to_string(),
            "B".to_string(),
            "C".to_string(),
            "D".to_string(),
        ];
        let f = mcv_freqs(&values, &HashMap::new(), 100);
        assert_eq!(f.len(), 4);
        assert!(f.iter().all(|&x| (x - 0.25).abs() < 1e-6), "freqs={:?}", f);
    }

    #[test]
    fn mcv_freqs_empty_and_zero_rowcount() {
        assert!(mcv_freqs(&[], &HashMap::new(), 100).is_empty());
        // row_count <= 0 → uniform fallback rather than a divide-by-zero.
        let values = vec!["A".to_string(), "B".to_string()];
        let f = mcv_freqs(&values, &HashMap::new(), 0);
        assert!(f.iter().all(|&x| (x - 0.5).abs() < 1e-6));
    }

    #[test]
    fn parse_valcounts_json_parses_nested_object() {
        let m = parse_valcounts_json(r#"{"kind":{"A":80,"B":20},"x":{"v":5}}"#);
        assert_eq!(m["kind"]["A"], 80);
        assert_eq!(m["kind"]["B"], 20);
        assert_eq!(m["x"]["v"], 5);
    }

    #[test]
    fn parse_valcounts_json_ignores_garbage() {
        assert!(parse_valcounts_json("not json").is_empty());
        assert!(parse_valcounts_json("[1,2,3]").is_empty()); // not an object
        // Non-integer counts dropped; an all-garbage column yields no entry.
        assert!(parse_valcounts_json(r#"{"k":{"a":"oops"}}"#).is_empty());
    }

    #[test]
    fn stadistinct_value_returns_zero_for_unknown_inputs() {
        assert_eq!(stadistinct_value(0, 100), 0.0);
        assert_eq!(stadistinct_value(-1, 100), 0.0);
        assert_eq!(stadistinct_value(50, 0), 0.0);
        assert_eq!(stadistinct_value(50, -10), 0.0);
    }

    #[test]
    fn stadistinct_value_emits_absolute_count_when_ndistinct_is_small() {
        // PG convention: positive stadistinct is an absolute count of
        // distinct values, used when ndistinct/row_count ≤ 0.1 — the table
        // is "wide enough" that the count is meaningful as the table grows.
        assert_eq!(stadistinct_value(10, 1000), 10.0);
        assert_eq!(stadistinct_value(99, 1000), 99.0);
    }

    #[test]
    fn stadistinct_value_flips_to_fraction_at_density_threshold() {
        // PG convention: when ndistinct/row_count > 0.1, store the
        // *negated fraction* so the estimator scales correctly as the
        // partition gains/loses rows without a re-ANALYZE.
        let v = stadistinct_value(500, 1000);
        assert!((v - (-0.5)).abs() < 1e-6, "got {}", v);

        let v2 = stadistinct_value(900, 1000);
        assert!((v2 - (-0.9)).abs() < 1e-6, "got {}", v2);

        // Just past the 0.1 boundary → still negative fraction form.
        let v3 = stadistinct_value(101, 1000);
        assert!(
            v3 < 0.0,
            "boundary should flip to fraction form, got {}",
            v3
        );
    }
}
