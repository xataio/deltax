"""Integration tests for pg_statistic / pg_class.reltuples population on
compressed partitions.

The purpose is to fix PG's planner falling back to default selectivity
(0.005 numeric-eq, ~2.5e-5 text-eq) on compressed partitions because
they have `pg_class.reltuples = 0` and no `pg_statistic` rows.
"""

from datetime import datetime, timedelta, timezone


def _seed(db, n_partitions=4, rows_per_partition=50_000, high_card=5000):
    """Create a partitioned deltax table with three columns of controlled
    cardinality and compress every populated partition.

    - `uid` — INT8, `high_card` distinct values (simulates join key).
    - `kind` — TEXT, 5 distinct values (simulates low-cardinality enum).
    - `val` — FLOAT8, ~unique (simulates measurement column).
    """
    db.execute(
        "CREATE TABLE events ("
        "  ts TIMESTAMPTZ NOT NULL,"
        "  uid BIGINT,"
        "  kind TEXT,"
        "  val FLOAT8"
        ")"
    )
    db.execute(
        "SELECT deltax.deltax_create_table('events', 'ts', '1 day', %s)",
        (n_partitions - 1,),
    )
    db.execute(
        "SELECT deltax.deltax_enable_compression('events', order_by => ARRAY['ts'])"
    )
    db.commit()

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    window_us = 22 * 3600 * 1_000_000
    spacing_us = max(1, window_us // max(1, rows_per_partition - 1))

    for p in range(n_partitions):
        part_start = today - timedelta(days=1) + timedelta(days=p) + timedelta(hours=1)
        db.execute(
            "INSERT INTO events (ts, uid, kind, val) "
            "SELECT %s::timestamptz + (i::bigint * %s::bigint * interval '1 microsecond'), "
            "       (i %% %s)::bigint, "
            "       CASE WHEN i %% 5 = 0 THEN 'A' "
            "            WHEN i %% 5 = 1 THEN 'B' "
            "            WHEN i %% 5 = 2 THEN 'C' "
            "            WHEN i %% 5 = 3 THEN 'D' "
            "            ELSE 'E' END, "
            "       random() "
            "FROM generate_series(0, %s) i",
            (part_start, spacing_us, high_card, rows_per_partition - 1),
        )
    db.commit()

    assert db.execute("SELECT count(*) FROM events_default").fetchone()[0] == 0

    for (name,) in db.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('events') "
        "ORDER BY range_start"
    ).fetchall():
        cnt = db.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
        if cnt > 0:
            db.execute("SELECT deltax.deltax_compress_partition(%s)", (name,))
    db.commit()


def _first_compressed_partition(db):
    row = db.execute(
        "SELECT table_name FROM deltax.deltax_partition WHERE is_compressed = true "
        "ORDER BY range_start LIMIT 1"
    ).fetchone()
    assert row is not None, "no compressed partitions"
    return row[0]


def _stats_for(db, part_name, attname):
    """Return (stadistinct, stanullfrac, stawidth) from pg_statistic."""
    row = db.execute(
        "SELECT s.stadistinct, s.stanullfrac, s.stawidth "
        "FROM pg_statistic s "
        "JOIN pg_attribute a ON a.attrelid = s.starelid AND a.attnum = s.staattnum "
        "WHERE s.starelid = %s::regclass AND a.attname = %s",
        (part_name, attname),
    ).fetchone()
    return row  # None if no stats


def test_compress_populates_pg_statistic(db):
    """After compression, pg_statistic must have a row per non-dropped
    column with stadistinct reflecting the partition-level HLL merge."""
    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)

    part = _first_compressed_partition(db)
    # 50K rows per partition, 5000 distinct uid values → stadistinct should be
    # a positive absolute around 5000 (less than 10% of 50K).
    stats = _stats_for(db, part, "uid")
    assert stats is not None, f"no pg_statistic row for {part}.uid"
    stadist, nullfrac, width = stats
    # HLL tolerance ~2%, random-mod uid distribution gives exact 5000 very
    # reliably for this fixture.
    assert 4800 <= stadist <= 5200, f"stadistinct(uid)={stadist}, expected ~5000"
    assert 0.0 <= nullfrac < 0.01, f"nullfrac(uid)={nullfrac}"
    assert width == 8, f"stawidth(uid)={width} (BIGINT = 8)"

    # kind has 5 distinct values → stadistinct should be ≈5.
    stats = _stats_for(db, part, "kind")
    assert stats is not None
    stadist, _nullfrac, _width = stats
    assert 4 <= stadist <= 6, f"stadistinct(kind)={stadist}, expected ~5"


def test_compress_updates_reltuples(db):
    """pg_class.reltuples must reflect actual row count so PG's selectivity
    estimators stop using default 0.005 for eq."""
    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)
    part = _first_compressed_partition(db)
    rel_tuples = db.execute(
        "SELECT reltuples::bigint FROM pg_class WHERE oid = %s::regclass",
        (part,),
    ).fetchone()[0]
    # We expect exactly 50K rows per partition from the fixture.
    assert 49_000 <= rel_tuples <= 50_000, f"reltuples={rel_tuples}, expected ~50000"


def test_plan_row_estimate_uses_populated_stats(db):
    """EXPLAIN row-estimate after compression should match
    rel_tuples / ndistinct (not default 0.5%)."""
    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)

    # Equality on the high-card column: 50K rows / 5000 distinct ≈ 10.
    plan = db.execute(
        "EXPLAIN (FORMAT JSON) SELECT * FROM events WHERE uid = 42"
    ).fetchone()[0]
    import json
    root = json.loads(plan) if isinstance(plan, str) else plan
    # Walk the plan tree for the lowest `Plan Rows` (usually the scan).
    def find_scan_rows(node):
        rows = [node.get("Plan Rows", 0)]
        for child in node.get("Plans", []) or []:
            rows += find_scan_rows(child)
        return rows
    rows = find_scan_rows(root[0]["Plan"])
    assert rows, "no plan rows"
    # With 50K * 2 partitions = 100K total and ndistinct=5000, expect ~20.
    # Default selectivity (0.005) would give 50K * 0.005 * 2 = 500. So
    # anything under 100 signals we're on the populated-stats path.
    total_rows = max(rows)
    assert total_rows < 100, (
        f"Plan Rows={total_rows} — equality selectivity looks like the default "
        f"0.005 rather than 1/ndistinct. pg_statistic may not be populated."
    )


def test_analyze_is_intercepted_on_compressed_partition(db):
    """Running `ANALYZE <compressed_partition>` must not clobber the
    pg_statistic rows we maintain at compress time."""
    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)
    part = _first_compressed_partition(db)

    before = _stats_for(db, part, "uid")
    assert before is not None

    db.rollback()
    db.autocommit = True
    db.execute(f'ANALYZE "{part}"')
    db.autocommit = False

    after = _stats_for(db, part, "uid")
    assert after is not None, (
        "pg_statistic row was deleted by ANALYZE — the ProcessUtility "
        "hook should have filtered this compressed partition out"
    )
    # Values should be identical — ANALYZE never touched them.
    assert before[0] == after[0], f"stadistinct changed: {before[0]} -> {after[0]}"


def test_deltax_analyze_partition_is_idempotent(db):
    """Calling deltax_analyze_partition on a freshly-compressed partition
    should produce the same stats (within HLL tolerance)."""
    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)
    part = _first_compressed_partition(db)

    before = _stats_for(db, part, "uid")
    db.execute("SELECT deltax.deltax_analyze_partition(%s)", (part,))
    db.commit()
    after = _stats_for(db, part, "uid")

    assert before is not None and after is not None
    row_count = db.execute(
        "SELECT reltuples::bigint FROM pg_class WHERE oid = %s::regclass",
        (part,),
    ).fetchone()[0]
    # Translate PG's signed stadistinct into absolute distinct count
    # (positive = count, negative fraction = -frac * rowcount) so HLL
    # and SUM-capped fallback estimates are on the same scale.
    def absolute(nd, rc):
        return nd if nd >= 0 else -nd * rc
    before_abs = absolute(before[0], row_count)
    after_abs = absolute(after[0], row_count)
    # The fallback SUM-capped path is less accurate than the HLL merge
    # (especially for time-clustered keys); allow a 3× tolerance.
    assert max(before_abs, after_abs) / max(min(before_abs, after_abs), 1) < 3.0, (
        f"stadistinct drift too large: before={before[0]} ({before_abs}), "
        f"after={after[0]} ({after_abs})"
    )


def test_autovacuum_disabled_on_compressed(db):
    """After compression, the partition should have
    autovacuum_enabled = off so autovacuum doesn't clobber stats."""
    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)
    part = _first_compressed_partition(db)
    options = db.execute(
        "SELECT reloptions FROM pg_class WHERE oid = %s::regclass",
        (part,),
    ).fetchone()[0]
    opts = options or []
    assert any("autovacuum_enabled=off" in o for o in opts), (
        f"autovacuum_enabled=off not in reloptions: {opts}"
    )
