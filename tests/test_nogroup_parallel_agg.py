"""Integration tests for no-GROUP-BY aggregates with text-column WHERE quals.

These shapes (e.g. ClickBench Q20 `SELECT COUNT(*) FROM hits WHERE URL LIKE
'%google%'`) route through the parallel mixed path with a single
constant-key group. The tests pin the PG-visible semantics the path must
preserve:

- exact counts vs. an uncompressed reference table
- exactly one output row even when zero rows match (COUNT = 0, SUM/MIN/MAX
  NULL, AVG NULL)
- combinations of text LIKE / NOT LIKE / equality with numeric quals
"""


def _seed(db, n_partitions=3, rows_per_partition=30_000):
    from datetime import datetime, timedelta, timezone

    db.execute(
        "CREATE TABLE events ("
        "  ts TIMESTAMPTZ NOT NULL,"
        "  device_id INT,"
        "  url TEXT,"
        "  val INT"
        ")"
    )
    # Uncompressed reference table receiving identical rows.
    db.execute(
        "CREATE TABLE events_ref ("
        "  ts TIMESTAMPTZ NOT NULL,"
        "  device_id INT,"
        "  url TEXT,"
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
        # High-cardinality URLs (LZ4-encoded) with a rare needle substring,
        # plus a NULL sprinkle: i % 97 == 0 -> contains 'google',
        # i % 31 == 0 -> NULL.
        insert = (
            "INSERT INTO {} (ts, device_id, url, val) "
            "SELECT %s::timestamptz + (i::bigint * %s::bigint * interval '1 microsecond'), "
            "       (i %% 7)::int, "
            "       CASE WHEN i %% 31 = 0 THEN NULL "
            "            WHEN i %% 97 = 0 THEN 'http://google.com/q=' || i "
            "            ELSE 'http://site-' || (i * 2654435761) || '.example.com/page' || i "
            "       END, "
            "       i::int "
            "FROM generate_series(0, %s) i"
        )
        params = (part_start, spacing_us, rows_per_partition - 1)
        db.execute(insert.format("events"), params)
        db.execute(insert.format("events_ref"), params)
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


def _both(db, sql):
    got = db.execute(sql.format(t="events")).fetchall()
    want = db.execute(sql.format(t="events_ref")).fetchall()
    return got, want


def test_count_star_like(db):
    _seed(db)
    got, want = _both(db, "SELECT COUNT(*) FROM {t} WHERE url LIKE '%google%'")
    assert got == want
    assert want[0][0] > 0  # needle must actually occur


def test_count_star_not_like(db):
    _seed(db)
    got, want = _both(db, "SELECT COUNT(*) FROM {t} WHERE url NOT LIKE '%google%'")
    assert got == want


def test_count_star_like_zero_matches_emits_one_row(db):
    _seed(db)
    got, want = _both(
        db, "SELECT COUNT(*) FROM {t} WHERE url LIKE '%no-such-needle-xyzzy%'"
    )
    assert got == want == [(0,)]


def test_sum_avg_with_like(db):
    _seed(db)
    got, want = _both(
        db,
        "SELECT COUNT(*), SUM(val), AVG(val)::numeric(20,6), COUNT(val) "
        "FROM {t} WHERE url LIKE '%google%'",
    )
    assert got == want


def test_sum_is_null_on_zero_matches(db):
    _seed(db)
    got, want = _both(
        db,
        "SELECT COUNT(*), SUM(val), AVG(val) FROM {t} "
        "WHERE url LIKE '%no-such-needle-xyzzy%'",
    )
    assert got == want
    assert got[0] == (0, None, None)


def test_text_eq_and_numeric_qual(db):
    _seed(db)
    got, want = _both(
        db,
        "SELECT COUNT(*), SUM(val) FROM {t} "
        "WHERE url <> '' AND val % 2 = 0 AND val > 1000",
    )
    # `val % 2` isn't a batch qual — exercises the fallback combination too.
    assert got == want
    got, want = _both(
        db,
        "SELECT COUNT(*), SUM(val) FROM {t} WHERE url LIKE 'http://google%' AND val > 1000",
    )
    assert got == want


def test_count_distinct_with_text_qual(db):
    _seed(db)
    got, want = _both(
        db,
        "SELECT COUNT(DISTINCT device_id) FROM {t} WHERE url LIKE '%google%'",
    )
    assert got == want
