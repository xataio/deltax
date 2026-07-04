"""Integration tests for the metadata-only aggregate fast path.

`MIN / MAX / SUM / COUNT(col) / COUNT(*)` queries answer straight from
per-segment stats when the WHERE clause is empty or contains only
segment-by equality and/or time-column range predicates.

Time-range WHERE clauses no longer need to align with partition (or
segment) boundaries: segments fully inside the bounds fold from
metadata, and the segments straddling a bound are decoded and filtered
row-wise in the executor (`seg_fully_in_bounds` in count_minmax.rs).
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
        "SELECT deltax.deltax_create_table('events', 'ts', '1 day', %s)",
        (n_partitions - 1,),
    )
    db.execute(
        "SELECT deltax.deltax_enable_compression('events', "
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
        "SELECT partition_name FROM deltax.deltax_partition_info('events') "
        "ORDER BY range_start"
    ).fetchall():
        cnt = db.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
        if cnt > 0:
            db.execute("SELECT deltax.deltax_compress_partition(%s)", (name,))
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


def test_time_where_mid_partition_uses_fast_path_and_stays_exact(db):
    """WHERE bounds that slice through a partition leave some segments
    only partially covered. The fast path still fires: fully-contained
    segments fold from metadata, straddling segments are decoded and
    filtered row-wise — so the answer must match the DeltaXAgg result
    exactly, for every aggregate kind and for both bound directions."""
    from datetime import datetime, timedelta, timezone
    _seed(db)
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    # Bounds are mid-day, so the partitions containing them are partial.
    lo = (today + timedelta(hours=6)).isoformat()
    hi = (today + timedelta(hours=20, minutes=13, seconds=7)).isoformat()
    for where in (
        f"ts >= '{lo}'::timestamptz",
        f"ts > '{lo}'::timestamptz AND ts <= '{hi}'::timestamptz",
        f"ts < '{hi}'::timestamptz",
        f"ts >= '{lo}'::timestamptz AND ts < '{hi}'::timestamptz AND device_id = 7",
    ):
        sql = (
            "SELECT count(*), count(val), min(val), max(val), sum(val), max(ts) "
            f"FROM events WHERE {where}"
        )
        _run_with_fastpath(db, True)
        plan = _plan(db, sql)
        assert "DeltaXMinMax" in plan, plan
        fast = db.execute(sql).fetchone()
        _run_with_fastpath(db, False)
        slow = db.execute(sql).fetchone()
        assert fast == slow, f"{where}: fast={fast} slow={slow}"


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


def _seed_low_card_text(db, n_partitions=3, rows_per_partition=30_000, n_kinds=5):
    """Helper for Phase D tests. Builds a table with low-cardinality
    `kind` (5 values) and `tag` (12 values) — both dictionary-encoded —
    plus a high-cardinality `user_id` text column (~rows_per_partition
    distinct) that should force the HashSet fallback. Multi-partition so
    Phase D's leader pre-pass actually walks several segment dicts and
    builds the global remap."""
    from datetime import datetime, timedelta, timezone

    db.execute(
        "CREATE TABLE events_d ("
        "  ts TIMESTAMPTZ NOT NULL,"
        "  kind TEXT,"
        "  tag TEXT,"
        "  user_id TEXT"
        ")"
    )
    db.execute(
        "SELECT deltax.deltax_create_table('events_d', 'ts', '1 day', %s)",
        (n_partitions - 1,),
    )
    db.execute(
        "SELECT deltax.deltax_enable_compression('events_d', "
        "  order_by => ARRAY['ts'])"
    )
    db.commit()

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    window_us = 22 * 3600 * 1_000_000
    spacing_us = max(1, window_us // max(1, rows_per_partition - 1))

    for p in range(n_partitions):
        part_start = today - timedelta(days=1) + timedelta(days=p) + timedelta(hours=1)
        db.execute(
            "INSERT INTO events_d (ts, kind, tag, user_id) "
            "SELECT %s::timestamptz + (i::bigint * %s::bigint * interval '1 microsecond'), "
            "       CASE i %% %s "
            "         WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' "
            "         WHEN 3 THEN 'D' ELSE 'E' END, "
            "       'tag_' || ((i %% 12)::text), "
            "       'u_' || (i + %s * 100000)::text "
            "FROM generate_series(0, %s) i",
            (part_start, spacing_us, n_kinds, p, rows_per_partition - 1),
        )
    db.commit()

    for (name,) in db.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('events_d') "
        "ORDER BY range_start"
    ).fetchall():
        cnt = db.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
        if cnt > 0:
            db.execute("SELECT deltax.deltax_compress_partition(%s)", (name,))
    db.commit()
    db.rollback()
    db.autocommit = True
    db.execute("ANALYZE events_d")
    db.autocommit = False


def test_phase_d_count_distinct_low_card_text(db):
    """Phase D bitset path: GROUP BY low-card text + COUNT(DISTINCT
    low-card text). Both `kind` (5 vals) and `tag` (12 vals) are
    dictionary-encoded and well under the bitset threshold, so workers
    use Bitset + global string IDs. Result must match the reference path."""
    _seed_low_card_text(db)
    sql = (
        "SELECT kind, COUNT(*), COUNT(DISTINCT tag) AS u "
        "FROM events_d GROUP BY kind ORDER BY kind"
    )
    fast = db.execute(sql).fetchall()
    # Force the serial CountDistinct path (HashSet<u128>, no Phase D
    # bitset) by disabling internal worker parallelism. The serial path
    # is the orthogonal reference for correctness — same query, same
    # data, different distinct-set representation.
    db.execute("SET pg_deltax.parallel_workers = 1")
    try:
        ref = db.execute(sql).fetchall()
    finally:
        db.execute("RESET pg_deltax.parallel_workers")
    assert fast == ref, f"Phase D CountDistinct: fast={fast} ref={ref}"


def test_phase_d_count_distinct_high_card_falls_back(db):
    """High-card text column (`user_id`, ~rows_per_partition unique)
    blows past PHASE_D_MAX_GLOBAL_FOR_BITSET. Phase D should bail at the
    eligibility check and the query should use the existing HashSet<u128>
    path. Correctness gate: result still matches reference."""
    _seed_low_card_text(db)
    sql = (
        "SELECT kind, COUNT(DISTINCT user_id) AS u "
        "FROM events_d GROUP BY kind ORDER BY kind"
    )
    fast = db.execute(sql).fetchall()
    # Force the serial CountDistinct path (HashSet<u128>, no Phase D
    # bitset) by disabling internal worker parallelism. The serial path
    # is the orthogonal reference for correctness — same query, same
    # data, different distinct-set representation.
    db.execute("SET pg_deltax.parallel_workers = 1")
    try:
        ref = db.execute(sql).fetchall()
    finally:
        db.execute("RESET pg_deltax.parallel_workers")
    assert fast == ref, f"high-card CountDistinct: fast={fast} ref={ref}"


def test_phase_d_count_distinct_with_where(db):
    """Phase D with a WHERE filter that selects ~half the rows — must
    still produce the correct distinct count per group, exercising the
    interaction between batch_quals row-skipping and the per-row bitset
    insert."""
    _seed_low_card_text(db)
    sql = (
        "SELECT kind, COUNT(DISTINCT tag) AS u "
        "FROM events_d WHERE kind IN ('A', 'B') "
        "GROUP BY kind ORDER BY kind"
    )
    fast = db.execute(sql).fetchall()
    # Force the serial CountDistinct path (HashSet<u128>, no Phase D
    # bitset) by disabling internal worker parallelism. The serial path
    # is the orthogonal reference for correctness — same query, same
    # data, different distinct-set representation.
    db.execute("SET pg_deltax.parallel_workers = 1")
    try:
        ref = db.execute(sql).fetchall()
    finally:
        db.execute("RESET pg_deltax.parallel_workers")
    assert fast == ref, f"Phase D + WHERE: fast={fast} ref={ref}"


def test_disable_parallel_agg_guc(db):
    """`pg_deltax.disable_parallel_agg = on` is the operator escape hatch
    for the partial+Gather+FinalAgg path. With it on, only the complete
    CustomScan DeltaXAgg path runs (still internally parallel via rayon).
    Correctness gate: numeric-WHERE queries on a multi-segment table —
    the shape that *would* trigger the partial path — produce identical
    results either way."""
    _seed(db, n_partitions=3, rows_per_partition=30_000)
    sql = "SELECT count(*), sum(val) FROM events WHERE val > 100"
    default = db.execute(sql).fetchone()
    db.execute("SET pg_deltax.disable_parallel_agg = on")
    try:
        disabled = db.execute(sql).fetchone()
    finally:
        db.execute("RESET pg_deltax.disable_parallel_agg")
    assert default == disabled, (
        f"disable_parallel_agg results differ: default={default} disabled={disabled}"
    )


def test_c3_segment_skipping_with_numeric_where(db):
    """C.3 per-segment fast path: a numeric WHERE that rejects an entire
    segment via col_minmax should let the worker skip it without
    decompression. Correctness gate: result matches the same query with
    `pg_deltax.parallel_workers=1` (forces the serial path that doesn't
    use the C.3 short-circuit), proving NonePass classification matches
    per-row evaluation."""
    _seed(db, n_partitions=3, rows_per_partition=30_000)
    # WHERE val < 1000 — only the first ~1000 rows of each partition
    # match. Per-segment col_minmax should resolve most segments as
    # NonePass (val >= 1000 throughout).
    sql = (
        "SELECT count(*), sum(val) FROM events WHERE val < 1000"
    )
    fast = db.execute(sql).fetchone()
    db.execute("SET pg_deltax.parallel_workers = 1")
    try:
        ref = db.execute(sql).fetchone()
    finally:
        db.execute("RESET pg_deltax.parallel_workers")
    assert fast == ref, f"C.3 NonePass: fast={fast} ref={ref}"


def test_c3_segment_allpass_with_numeric_where(db):
    """C.3 AllPass path: a WHERE that's satisfied by every row (val >= 0
    over a non-negative-only `val` column) should let the worker skip
    `evaluate_batch_quals` entirely. Result must equal the unfiltered
    aggregate."""
    _seed(db, n_partitions=3, rows_per_partition=30_000)
    sql_filtered = "SELECT count(*), sum(val) FROM events WHERE val >= 0"
    sql_unfiltered = "SELECT count(*), sum(val) FROM events"
    a = db.execute(sql_filtered).fetchone()
    b = db.execute(sql_unfiltered).fetchone()
    assert a == b, f"C.3 AllPass: filtered={a} unfiltered={b}"


def test_phase_d_null_count_distinct(db):
    """COUNT(DISTINCT col) excludes NULLs by SQL semantics. Phase D's
    bitset path must observe this — the dict NULL sentinel maps to
    `local_id == u32::MAX` and is filtered before bit-set insertion.
    Build a fresh table where every row with `kind='B'` has `tag=NULL`,
    so the Phase D bitset path exercises both null and non-null inputs."""
    from datetime import datetime, timedelta, timezone

    db.execute(
        "CREATE TABLE events_dn ("
        "  ts TIMESTAMPTZ NOT NULL,"
        "  kind TEXT,"
        "  tag TEXT"
        ")"
    )
    db.execute("SELECT deltax.deltax_create_table('events_dn', 'ts', '1 day', 2)")
    db.execute("SELECT deltax.deltax_enable_compression('events_dn', order_by => ARRAY['ts'])")
    db.commit()

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    rows_per_partition = 30_000
    window_us = 22 * 3600 * 1_000_000
    spacing_us = max(1, window_us // (rows_per_partition - 1))
    for p in range(3):
        part_start = today - timedelta(days=1) + timedelta(days=p) + timedelta(hours=1)
        db.execute(
            "INSERT INTO events_dn (ts, kind, tag) "
            "SELECT %s::timestamptz + (i::bigint * %s::bigint * interval '1 microsecond'), "
            "       CASE i %% 5 WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' "
            "                   WHEN 3 THEN 'D' ELSE 'E' END, "
            # tag = NULL whenever kind='B' (i%5==1) — exercise the dict
            # NULL sentinel inside Phase D's per-row insert.
            "       CASE WHEN i %% 5 = 1 THEN NULL "
            "            ELSE 'tag_' || ((i %% 12)::text) END "
            "FROM generate_series(0, %s) i",
            (part_start, spacing_us, rows_per_partition - 1),
        )
    db.commit()
    for (name,) in db.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('events_dn') ORDER BY range_start"
    ).fetchall():
        cnt = db.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
        if cnt > 0:
            db.execute("SELECT deltax.deltax_compress_partition(%s)", (name,))
    db.commit()

    sql = (
        "SELECT kind, COUNT(DISTINCT tag) AS u "
        "FROM events_dn GROUP BY kind ORDER BY kind"
    )
    fast = db.execute(sql).fetchall()
    db.execute("SET pg_deltax.parallel_workers = 1")
    try:
        ref = db.execute(sql).fetchall()
    finally:
        db.execute("RESET pg_deltax.parallel_workers")
    assert fast == ref, f"Phase D NULL: fast={fast} ref={ref}"
    # Spot-check: kind='B' has all-NULL tags → COUNT(DISTINCT tag) == 0.
    by_kind = {k: u for (k, u) in fast}
    assert by_kind["B"] == 0, by_kind


def test_mixed_path_pipeline_detoast_covers_full_first_batch(db):
    """Regression: the parallel-mixed aggregation path pipelines detoast
    with worker execution — leader pre-detoasts batch 0, then workers run
    on batch K while leader detoasts batch K+1. Both loops must use the
    SAME `n_batches` value, otherwise pre-detoast covers only a fraction
    of batch 0 and workers race on toast pointers that read as empty
    blobs → SegTextColumn::None → group keys collapse to NULL → distinct
    group values silently lost.

    Trigger conditions:
      * `pg_deltax.parallel_workers > 1` → `use_lazy = true`
      * `all_segments.len() >= n_workers * 16` → pipeline mode engages
      * high-cardinality text GROUP BY column whose compressed blob
        exceeds the TOAST threshold so it's stored as a toast pointer
        (without TOAST, lazy detoast is a no-op and the bug doesn't fire)

    Cross-check: `SELECT count(*) FROM (... GROUP BY t)` must equal
    `SELECT COUNT(DISTINCT t)` over the same input. The bug previously
    made the former lower by ~half (one NULL group per pipeline chunk).
    """
    from datetime import datetime, timedelta, timezone

    # Need ≥ n_workers * 16 = 32 segments to engage pipeline mode at
    # `pg_deltax.parallel_workers = 2`. segment_size=4000 × 10 segs/part
    # × 4 partitions = 40 segments — comfortably over threshold.
    n_partitions = 4
    rows_per_partition = 40_000
    segment_size = 4000

    db.execute(
        "CREATE TABLE pipe_events ("
        "  ts TIMESTAMPTZ NOT NULL,"
        "  user_id TEXT"
        ")"
    )
    db.execute(
        "SELECT deltax.deltax_create_table('pipe_events', 'ts', '1 day', %s)",
        (n_partitions - 1,),
    )
    db.execute(
        "SELECT deltax.deltax_enable_compression('pipe_events', "
        "  order_by => ARRAY['ts'], segment_size => %s)",
        (segment_size,),
    )
    db.commit()

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    window_us = 22 * 3600 * 1_000_000
    spacing_us = max(1, window_us // max(1, rows_per_partition - 1))

    for p in range(n_partitions):
        part_start = today - timedelta(days=1) + timedelta(days=p) + timedelta(hours=1)
        # user_id must be both unique per row AND high-entropy: an MD5 of
        # `(partition, row)` gives 32 hex chars with ~no compressibility,
        # so pg_deltax's Lz4Blocked-encoded segment blob (~4000 × ~32 B ≈
        # 128 KB) doesn't shrink under PG's external-toast threshold (~2
        # KB after PG's secondary lz4 pass). Without this the blob stays
        # inline, lazy detoast is a no-op, and the bug doesn't fire.
        db.execute(
            "INSERT INTO pipe_events (ts, user_id) "
            "SELECT %s::timestamptz + (i::bigint * %s::bigint * interval '1 microsecond'), "
            "       md5(%s::text || ':' || i::text) "
            "FROM generate_series(0, %s) i",
            (part_start, spacing_us, p, rows_per_partition - 1),
        )
    db.commit()

    for (name,) in db.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('pipe_events') "
        "ORDER BY range_start"
    ).fetchall():
        cnt = db.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
        if cnt > 0:
            db.execute("SELECT deltax.deltax_compress_partition(%s)", (name,))
    db.commit()
    db.rollback()
    db.autocommit = True
    db.execute("ANALYZE pipe_events")
    db.autocommit = False

    # Confirm we actually have enough segments to trigger the pipeline
    # at n_workers=2. If seeding ever drops below the threshold the test
    # silently stops exercising the bug — make it loud.
    seg_count = 0
    for (name,) in db.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('pipe_events')"
    ).fetchall():
        meta_name = f"_deltax_compressed.{name}_meta"
        if db.execute(
            "SELECT to_regclass(%s) IS NOT NULL", (meta_name,)
        ).fetchone()[0]:
            seg_count += db.execute(f'SELECT count(*) FROM {meta_name}').fetchone()[0]
    assert seg_count >= 32, (
        f"test setup produces only {seg_count} segments; need >=32 to engage "
        f"the mixed-path detoast pipeline at parallel_workers=2"
    )

    # Force n_workers=2 so the pipeline engages with as few rows as possible.
    db.execute("SET pg_deltax.parallel_workers = 2")
    try:
        # CRITICAL: the GROUP BY must run as the *outer* upper-rel target,
        # otherwise the upper-paths hook doesn't fire and PG falls back to
        # HashAggregate → DeltaXAppend (which is correct but doesn't
        # exercise the mixed-path bug). Wrapping the GROUP BY in `count(*)
        # FROM (... GROUP BY)` triggers exactly that fallback. Materialize
        # the GROUP BY result into a TEMP TABLE — the CREATE TABLE AS path
        # routes the GROUP BY through DeltaXAgg, after which we can count
        # rows from a plain heap scan.
        db.execute("DROP TABLE IF EXISTS groupby_result")
        db.execute(
            "CREATE TEMP TABLE groupby_result AS "
            "SELECT user_id, COUNT(*) AS c FROM pipe_events GROUP BY user_id"
        )
        groupby_count = db.execute(
            "SELECT count(*) FROM groupby_result"
        ).fetchone()[0]
        # Reference: COUNT(DISTINCT) goes through process_cd_segments —
        # a different code path that was correct even with the original
        # bug — so it's a trustworthy ground truth.
        distinct_count = db.execute(
            "SELECT COUNT(DISTINCT user_id) FROM pipe_events"
        ).fetchone()[0]
    finally:
        db.execute("RESET pg_deltax.parallel_workers")

    expected = n_partitions * rows_per_partition  # every user_id unique
    assert groupby_count == distinct_count == expected, (
        f"mixed-path GROUP BY lost rows: groupby={groupby_count} "
        f"distinct={distinct_count} expected={expected}"
    )
