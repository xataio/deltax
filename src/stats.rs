//! Populate `pg_class.reltuples` and `pg_statistic` for compressed
//! partitions so PG's built-in selectivity functions stop falling back
//! to `DEFAULT_EQ_SEL` (0.005 for numeric equality, ~2.5e-5 for text
//! equality). This is the ingredient that lets the planner pick the
//! right join side on queries like Q17 (`event_type='Delivered'`) and
//! keeps point lookups (Q07 `order_id = N`) off the parallel path.
//!
//! Source of truth:
//! - `deltax_partition.row_count` — authoritative total rows
//! - `deltax_partition.column_ndistinct` — per-column merged-HLL
//!   estimate written by `compress.rs` at compress time (or SQL
//!   fallback for the standalone analyze UDF)
//! - `_<partition>_colstats._nonnull_count` — summed for nullfrac

use std::collections::HashMap;

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

    // Single SPI pass over the colstats table to get per-column
    // SUM(nonnull). One row per non-segment-by column, in _col_idx order.
    let nonnull_by_col_idx = load_nonnull_counts(client, colstats_fqn)?;

    // Fetch `(attname, attnum, attlen)` for every non-dropped column of
    // the partition so we can map our `ColumnMeta` back to PG's attnum
    // and pick stawidth from attlen.
    let attrs = load_pg_attribute(client, part_rel_oid)?;

    // Estimate average tuple width — feeds `pg_class.relpages`.
    let mut sum_widths: i64 = 0;
    for a in &attrs {
        sum_widths += stawidth_for_attlen(a.attlen) as i64;
    }
    let avg_tuple_width = sum_widths.max(32);

    // Walk our pg_deltax columns, match to the partition's pg_attribute
    // entry by name, emit one pg_statistic row each. Segment-by columns
    // have null estimates from our HLL map (they're stored as the
    // partition's segment_values, not in the blob), so we skip them
    // here and let PG default them — there are usually only a handful.
    let mut nonseg_idx: i32 = 0;
    for col in columns {
        if col.is_segment_by {
            continue;
        }
        let attnum = match attrs.iter().find(|a| a.attname == col.name) {
            Some(a) => a.attnum,
            None => {
                nonseg_idx += 1;
                continue; // column was dropped post-compression
            }
        };
        let attlen = attrs
            .iter()
            .find(|a| a.attname == col.name)
            .map(|a| a.attlen)
            .unwrap_or(-1);
        let stawidth = stawidth_for_attlen(attlen);

        let nonnull = nonnull_by_col_idx
            .get(&nonseg_idx)
            .copied()
            .unwrap_or(row_count);
        let stanullfrac = {
            let frac = (row_count - nonnull) as f32 / row_count as f32;
            frac.clamp(0.0, 1.0)
        };

        let ndistinct = col_ndistinct.get(&col.name).copied().unwrap_or(0);
        let stadistinct = stadistinct_value(ndistinct, row_count);

        upsert_pg_statistic_row(
            client,
            part_rel_oid,
            attnum,
            stadistinct,
            stanullfrac,
            stawidth,
        )?;

        nonseg_idx += 1;
    }

    update_reltuples(client, part_rel_oid, row_count, avg_tuple_width as i32)?;

    // Make the new stats visible to other backends at commit time.
    invalidate_relcache(part_rel_oid);

    Ok(())
}

/// Load `SUM(_nonnull_count)` per `_col_idx` in a single pass.
fn load_nonnull_counts(
    client: &mut SpiClient,
    colstats_fqn: &str,
) -> spi::SpiResult<HashMap<i32, i64>> {
    let query = format!(
        "SELECT _col_idx::int4, SUM(_nonnull_count)::int8 \
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
        if idx >= 0 {
            out.insert(idx, nonnull);
        }
    }
    Ok(out)
}

struct AttrInfo {
    attname: String,
    attnum: i16,
    attlen: i16,
}

fn load_pg_attribute(
    client: &mut SpiClient,
    rel_oid: pg_sys::Oid,
) -> spi::SpiResult<Vec<AttrInfo>> {
    let rel_oid_int = u32::from(rel_oid) as i64;
    let query = "SELECT attname::text, attnum::int2, attlen::int2 \
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
        if !attname.is_empty() {
            out.push(AttrInfo {
                attname,
                attnum,
                attlen,
            });
        }
    }
    Ok(out)
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
fn upsert_pg_statistic_row(
    client: &mut SpiClient,
    attrelid: pg_sys::Oid,
    attnum: i16,
    stadistinct: f32,
    stanullfrac: f32,
    stawidth: i32,
) -> spi::SpiResult<()> {
    let attrelid_int = u32::from(attrelid) as i64;
    client.update(
        "DELETE FROM pg_statistic \
         WHERE starelid = $1::oid AND staattnum = $2::int2 AND stainherit = false",
        None,
        &[attrelid_int.into(), attnum.into()],
    )?;
    // All 5 stakindN / staopN / stacollN slots are zero-filled; all 5
    // stanumbersN / stavaluesN arrays are NULL. That yields a minimal
    // tuple that populates equality selectivity via `stadistinct` +
    // `stanullfrac` without claiming any MCV/histogram data.
    client.update(
        "INSERT INTO pg_statistic (\
            starelid, staattnum, stainherit, \
            stanullfrac, stawidth, stadistinct, \
            stakind1, stakind2, stakind3, stakind4, stakind5, \
            staop1, staop2, staop3, staop4, staop5, \
            stacoll1, stacoll2, stacoll3, stacoll4, stacoll5, \
            stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5, \
            stavalues1, stavalues2, stavalues3, stavalues4, stavalues5\
         ) VALUES (\
            $1::oid, $2::int2, false, \
            $3::real, $4::int4, $5::real, \
            0, 0, 0, 0, 0, \
            0, 0, 0, 0, 0, \
            0, 0, 0, 0, 0, \
            NULL, NULL, NULL, NULL, NULL, \
            NULL, NULL, NULL, NULL, NULL\
         )",
        None,
        &[
            attrelid_int.into(),
            attnum.into(),
            stanullfrac.into(),
            stawidth.into(),
            stadistinct.into(),
        ],
    )?;
    Ok(())
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

/// Entry point for the standalone `deltax_analyze_partition` UDF. We
/// don't have the HLL sketches here (they were consumed at compress
/// time), so fall back to summing per-segment ndistinct from the
/// `_colstats` table, capped at the partition's row_count.
pub fn analyze_partition_from_catalog(
    client: &mut SpiClient,
    part_rel_oid: pg_sys::Oid,
    colstats_fqn: &str,
    columns: &[ColumnMeta],
    row_count: i64,
) -> spi::SpiResult<()> {
    // SUM(per-segment ndistinct) capped at row_count. Less accurate
    // than the compression-time HLL merge but strictly better than
    // PG's defaults for already-compressed partitions.
    let query = format!(
        "SELECT _col_idx::int4, SUM(_ndistinct)::int8 \
         FROM {} GROUP BY _col_idx",
        colstats_fqn
    );
    let mut by_col_idx: HashMap<i32, i64> = HashMap::new();
    for row in client.select(&query, None, &[])? {
        let idx: i32 = row
            .get_datum_by_ordinal(1)
            .ok()
            .and_then(|d| d.value::<i32>().ok().flatten())
            .unwrap_or(-1);
        let nd: i64 = row
            .get_datum_by_ordinal(2)
            .ok()
            .and_then(|d| d.value::<i64>().ok().flatten())
            .unwrap_or(0);
        if idx >= 0 {
            by_col_idx.insert(idx, nd.min(row_count));
        }
    }

    let mut col_ndistinct: HashMap<String, i64> = HashMap::new();
    let mut nonseg_idx: i32 = 0;
    for col in columns {
        if col.is_segment_by {
            continue;
        }
        if let Some(&nd) = by_col_idx.get(&nonseg_idx) {
            col_ndistinct.insert(col.name.clone(), nd);
        }
        nonseg_idx += 1;
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
