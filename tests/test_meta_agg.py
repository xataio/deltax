"""Integration tests for the metadata-only aggregate fast path.

v1 scope (2026-04): `MIN / MAX / SUM / COUNT(col) / COUNT(*)` queries
answer straight from per-segment stats when the WHERE clause is empty
or contains only segment-by equality predicates.

Time-column WHERE clauses are explicitly NOT handled in v1 because a
segment whose `[min_time, max_time]` straddles a time-range boundary
contributes all its rows to `row_count` / `col_sums` / `col_minmax`,
even though some rows fall outside the filter.  Adding partial-
segment handling (or partition-bound analysis) is a follow-up —
RTABench Q02 still routes through DeltaXAgg.
"""

import pytest


def _seed(db, n_partitions=3, rows_per_partition=30_000, n_devices=50, n_kinds=5):
    from datetime import datetime, timedelta, timezone

    db.execute(
        "CREATE TABLE events ("
        "  ts TIMESTAMPTZ NOT NULL,"
        "  device_id INT,"
        "  kind TEXT,"
        "  val INT"
        ")"
    )
    db.execute(
        "SELECT deltax_create_table('events', 'ts', '1 day', %s)",
        (n_partitions - 1,),
    )
    db.execute(
        "SELECT deltax_enable_compression('events', "
        "  segment_by => ARRAY['device_id'], "
        "  order_by => ARRAY['ts'])"
    )
    db.commit()

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    window_us = 22 * 3600 * 1_000_000
    spacing_us = max(1, window_us // max(1, rows_per_partition - 1))

    for p in range(n_partitions):
        part_start = today - timedelta(days=1) + timedelta(days=p) + timedelta(hours=1)
        db.execute(
            "INSERT INTO events (ts, device_id, kind, val) "
            "SELECT %s::timestamptz + (i::bigint * %s::bigint * interval '1 microsecond'), "
            "       (i %% %s)::int, "
            "       CASE i %% %s "
            "         WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' "
            "         WHEN 3 THEN 'D' ELSE 'E' END, "
            "       i::int "
            "FROM generate_series(0, %s) i",
            (part_start, spacing_us, n_devices, n_kinds, rows_per_partition - 1),
        )
    db.commit()

    for (name,) in db.execute(
        "SELECT partition_name FROM deltax_partition_info('events') "
        "ORDER BY range_start"
    ).fetchall():
        cnt = db.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
        if cnt > 0:
            db.execute("SELECT deltax_compress_partition(%s)", (name,))
    db.commit()

    db.rollback()
    db.autocommit = True
    db.execute("ANALYZE events")
    db.autocommit = False


def _plan(db, sql):
    rows = db.execute(f"EXPLAIN (FORMAT TEXT) {sql}").fetchall()
    return "\n".join(r[0] for r in rows)


def _run_with_fastpath(db, enabled):
    val = "off" if enabled else "on"
    db.execute(f"SET pg_deltax.disable_meta_agg_fastpath = {val}")


# ---------------------------------------------------------------------------


def test_max_no_where_uses_fast_path(db):
    _seed(db)
    _run_with_fastpath(db, True)

    sql = "SELECT max(val) FROM events"
    plan = _plan(db, sql)
    assert "DeltaXMinMax" in plan, plan
    assert "Aggregate" not in plan, plan

    fast = db.execute(sql).fetchone()[0]
    _run_with_fastpath(db, False)
    ref = db.execute(sql).fetchone()[0]
    assert fast == ref


def test_min_max_sum_count_with_no_where(db):
    """Multi-aggregate: MIN/MAX/SUM/COUNT(col)/COUNT(*) in one query."""
    _seed(db)
    _run_with_fastpath(db, True)
    sql = "SELECT min(val), max(val), sum(val), count(val), count(*) FROM events"
    plan = _plan(db, sql)
    assert "DeltaXMinMax" in plan, plan

    row_fast = db.execute(sql).fetchone()
    _run_with_fastpath(db, False)
    row_ref = db.execute(sql).fetchone()
    assert row_fast == row_ref


def test_count_star_no_where_uses_catalog_fast_path(db):
    _seed(db)
    plan = _plan(db, "SELECT count(*) FROM events")
    assert "DeltaXCount" in plan, plan


def test_segment_by_equality_uses_fast_path(db):
    _seed(db)
    _run_with_fastpath(db, True)
    plan = _plan(db, "SELECT max(val) FROM events WHERE device_id = 7")
    assert "DeltaXMinMax" in plan, plan

    fast = db.execute("SELECT max(val) FROM events WHERE device_id = 7").fetchone()[0]
    _run_with_fastpath(db, False)
    ref = db.execute("SELECT max(val) FROM events WHERE device_id = 7").fetchone()[0]
    assert fast == ref


def test_count_star_with_segment_by_uses_fast_path(db):
    _seed(db)
    _run_with_fastpath(db, True)
    plan = _plan(db, "SELECT count(*) FROM events WHERE device_id = 3")
    assert "DeltaXCount" in plan, plan

    fast = db.execute("SELECT count(*) FROM events WHERE device_id = 3").fetchone()[0]
    _run_with_fastpath(db, False)
    ref = db.execute("SELECT count(*) FROM events WHERE device_id = 3").fetchone()[0]
    assert fast == ref


def test_time_where_aligned_to_partitions_uses_fast_path(db):
    """WHERE bounds that fully contain every surviving partition are
    safe — every row in those partitions also satisfies WHERE, so the
    per-segment `col_minmax` / `row_count` / `col_sums` are exact."""
    from datetime import datetime, timedelta, timezone
    _seed(db)
    _run_with_fastpath(db, True)
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    # Daily partitions, range chosen to wrap all 3 day-partitions entirely.
    lo = (today - timedelta(days=2)).isoformat()
    hi = (today + timedelta(days=3)).isoformat()
    sql = (
        f"SELECT max(val) FROM events "
        f"WHERE ts >= '{lo}'::timestamptz AND ts < '{hi}'::timestamptz"
    )
    plan = _plan(db, sql)
    assert "DeltaXMinMax" in plan, plan

    fast = db.execute(sql).fetchone()[0]
    _run_with_fastpath(db, False)
    ref = db.execute(sql).fetchone()[0]
    assert fast == ref


def test_time_where_mid_partition_falls_back(db):
    """WHERE bounds that slice through a partition leave that
    partition only partially covered — some rows inside it don't
    satisfy WHERE, so metadata aggregation would overcount.  Must
    fall back to DeltaXAgg."""
    from datetime import datetime, timedelta, timezone
    _seed(db)
    _run_with_fastpath(db, True)
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    # Lower bound is mid-day, so the partition containing it is partial.
    lo = (today + timedelta(hours=6)).isoformat()
    plan = _plan(db, f"SELECT max(val) FROM events WHERE ts >= '{lo}'::timestamptz")
    assert "DeltaXMinMax" not in plan, plan


def test_non_key_column_filter_falls_back(db):
    """WHERE on a non-time, non-segment-by column must NOT use fast path."""
    _seed(db)
    _run_with_fastpath(db, True)
    plan = _plan(db, "SELECT max(val) FROM events WHERE kind = 'A'")
    assert "DeltaXMinMax" not in plan, plan


def test_or_filter_falls_back(db):
    _seed(db)
    _run_with_fastpath(db, True)
    plan = _plan(db, "SELECT max(val) FROM events WHERE device_id = 1 OR device_id = 2")
    assert "DeltaXMinMax" not in plan, plan


def test_segment_by_range_falls_back(db):
    """Segment-by only accepts equality in extract_segment_filters."""
    _seed(db)
    _run_with_fastpath(db, True)
    plan = _plan(db, "SELECT max(val) FROM events WHERE device_id > 10")
    assert "DeltaXMinMax" not in plan, plan


def test_minmax_on_text_falls_back(db):
    """MIN/MAX on TEXT cannot be answered from the encoded-i64 metadata."""
    _seed(db)
    _run_with_fastpath(db, True)
    plan = _plan(db, "SELECT min(kind) FROM events")
    assert "DeltaXMinMax" not in plan, plan


def test_guc_forces_fallback(db):
    """GUC disables SUM fast path.  MAX-only still uses the legacy MinMax
    path (predates P1); only multi-aggregate / SUM / COUNT(col) is gated
    by the new `disable_meta_agg_fastpath` GUC."""
    _seed(db)
    _run_with_fastpath(db, False)
    plan = _plan(db, "SELECT sum(val) FROM events")
    assert "DeltaXMinMax" not in plan, plan


@pytest.mark.parametrize("func", ["min", "max", "sum", "count"])
def test_single_aggregate_correctness(db, func):
    _seed(db)
    sql = f"SELECT {func}(val) FROM events"

    _run_with_fastpath(db, True)
    fast = db.execute(sql).fetchone()[0]
    _run_with_fastpath(db, False)
    ref = db.execute(sql).fetchone()[0]
    assert fast == ref, f"{func}(val): fast={fast} ref={ref}"


@pytest.mark.parametrize("func", ["min", "max", "sum", "count"])
def test_aggregate_with_segment_by_correctness(db, func):
    _seed(db)
    sql = f"SELECT {func}(val) FROM events WHERE device_id = 5"

    _run_with_fastpath(db, True)
    fast = db.execute(sql).fetchone()[0]
    _run_with_fastpath(db, False)
    ref = db.execute(sql).fetchone()[0]
    assert fast == ref, f"{func}(val): fast={fast} ref={ref}"
