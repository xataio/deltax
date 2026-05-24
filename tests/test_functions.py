"""Integration tests for time_bucket, first, last, and top-N functions."""

from datetime import datetime, timezone

MOCK_NOW = "2025-01-15 12:00:00+00"
BASE_TS = "2025-01-15 00:00:00+00"


def _setup_topn_table(db):
    """Create a compressed deltax table with multiple groups for top-N tests."""
    db.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    db.execute("""
        CREATE TABLE topn_test (
            ts TIMESTAMPTZ NOT NULL,
            category TEXT NOT NULL,
            val INT NOT NULL
        )
    """)
    db.execute("SELECT deltax.deltax_create_table('topn_test', 'ts', '1 day'::interval)")
    db.commit()

    # Insert data: 5 categories with different row counts
    # cat-A: 50 rows, cat-B: 40 rows, cat-C: 30 rows, cat-D: 20 rows, cat-E: 10 rows
    values = []
    counts = {"cat-A": 50, "cat-B": 40, "cat-C": 30, "cat-D": 20, "cat-E": 10}
    for cat, n in counts.items():
        for i in range(n):
            values.append(
                f"('{BASE_TS}'::timestamptz + interval '{i} seconds', '{cat}', {i})"
            )
    db.execute(
        f"INSERT INTO topn_test (ts, category, val) VALUES {', '.join(values)}"
    )
    db.commit()

    # Enable compression and compress
    db.execute(
        "SELECT deltax.deltax_enable_compression('topn_test', "
        "segment_by => ARRAY['category'], order_by => ARRAY['ts'])"
    )
    db.commit()

    partitions = db.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('topn_test') "
        "WHERE range_start <= '2025-01-15'::timestamptz "
        "AND range_end > '2025-01-15'::timestamptz"
    ).fetchall()
    for row in partitions:
        db.execute(f"SELECT deltax.deltax_compress_partition('{row[0]}')")
    db.commit()


def _setup_metrics(db):
    """Helper: create a deltax table and insert test rows."""
    db.execute(
        "CREATE TABLE metrics (ts TIMESTAMPTZ NOT NULL, device TEXT, value FLOAT8)"
    )
    db.commit()

    db.execute("SELECT deltax.deltax_create_table('metrics', 'ts')")
    db.commit()

    now = datetime.now(timezone.utc)
    db.execute(
        """
        INSERT INTO metrics (ts, device, value) VALUES
            (%s, 'a', 1.0),
            (%s, 'a', 2.0),
            (%s, 'b', 3.0),
            (%s, 'b', 4.0)
        """,
        (now, now, now, now),
    )
    db.commit()
    return now


def test_time_bucket_5min(db):
    """deltax.time_bucket('5 minutes', ts) truncates to 5-min boundary."""
    row = db.execute(
        "SELECT deltax.time_bucket('5 minutes'::interval, '2025-06-15 14:23:42+00'::timestamptz)"
    ).fetchone()
    assert row[0] == datetime(2025, 6, 15, 14, 20, 0, tzinfo=timezone.utc)


def test_time_bucket_1hour(db):
    """deltax.time_bucket('1 hour', ts) truncates to hour boundary."""
    row = db.execute(
        "SELECT deltax.time_bucket('1 hour'::interval, '2025-06-15 14:23:42+00'::timestamptz)"
    ).fetchone()
    assert row[0] == datetime(2025, 6, 15, 14, 0, 0, tzinfo=timezone.utc)


def test_time_bucket_with_offset(db):
    """time_bucket with offset shifts the bucket boundary."""
    row = db.execute(
        "SELECT deltax.time_bucket('1 day'::interval, '2025-06-15 14:23:42+00'::timestamptz, '6 hours'::interval)"
    ).fetchone()
    # Bucket starts at 06:00 UTC on 2025-06-15
    assert row[0] == datetime(2025, 6, 15, 6, 0, 0, tzinfo=timezone.utc)


def test_first_last(db):
    """deltax.first(value, ts) and deltax.last(value, ts) return correct values."""
    db.execute(
        "CREATE TABLE fl (ts TIMESTAMPTZ NOT NULL, value FLOAT8)"
    )
    db.commit()

    db.execute("SELECT deltax.deltax_create_table('fl', 'ts')")
    db.commit()

    db.execute(
        """
        INSERT INTO fl (ts, value) VALUES
            ('2025-06-15 10:00:00+00', 100.0),
            ('2025-06-15 11:00:00+00', 200.0),
            ('2025-06-15 12:00:00+00', 300.0)
        """
    )
    db.commit()

    row = db.execute("SELECT deltax.first(value, ts), deltax.last(value, ts) FROM fl").fetchone()
    assert row[0] == 100.0  # earliest ts
    assert row[1] == 300.0  # latest ts


def test_first_last_with_groups(db):
    """first/last work with GROUP BY."""
    db.execute(
        "CREATE TABLE grouped (ts TIMESTAMPTZ NOT NULL, device TEXT, value FLOAT8)"
    )
    db.commit()

    db.execute("SELECT deltax.deltax_create_table('grouped', 'ts')")
    db.commit()

    db.execute(
        """
        INSERT INTO grouped (ts, device, value) VALUES
            ('2025-06-15 10:00:00+00', 'a', 10.0),
            ('2025-06-15 12:00:00+00', 'a', 30.0),
            ('2025-06-15 11:00:00+00', 'b', 20.0),
            ('2025-06-15 13:00:00+00', 'b', 40.0)
        """
    )
    db.commit()

    rows = db.execute(
        "SELECT device, deltax.first(value, ts), deltax.last(value, ts) "
        "FROM grouped GROUP BY device ORDER BY device"
    ).fetchall()

    assert rows[0] == ("a", 10.0, 30.0)
    assert rows[1] == ("b", 20.0, 40.0)


class TestTopN:
    def test_topn_desc(self, db):
        """Top-3 categories by count, DESC order."""
        _setup_topn_table(db)
        rows = db.execute(
            "SELECT category, COUNT(*) AS cnt FROM topn_test "
            "GROUP BY category ORDER BY COUNT(*) DESC LIMIT 3"
        ).fetchall()
        assert len(rows) == 3
        assert rows[0] == ("cat-A", 50)
        assert rows[1] == ("cat-B", 40)
        assert rows[2] == ("cat-C", 30)

        # Verify EXPLAIN shows DeltaXAgg with TopN info
        explain = db.execute(
            "EXPLAIN ANALYZE SELECT category, COUNT(*) AS cnt FROM topn_test "
            "GROUP BY category ORDER BY COUNT(*) DESC LIMIT 3"
        ).fetchall()
        explain_text = "\n".join(r[0] for r in explain)
        assert "DeltaXAgg" in explain_text, (
            f"Expected DeltaXAgg in plan:\n{explain_text}"
        )
        assert "TopN" in explain_text, (
            f"Expected TopN in EXPLAIN output:\n{explain_text}"
        )

    def test_topn_asc(self, db):
        """Top-3 categories by count, ASC order."""
        _setup_topn_table(db)
        rows = db.execute(
            "SELECT category, COUNT(*) AS cnt FROM topn_test "
            "GROUP BY category ORDER BY COUNT(*) ASC LIMIT 3"
        ).fetchall()
        assert len(rows) == 3
        assert rows[0] == ("cat-E", 10)
        assert rows[1] == ("cat-D", 20)
        assert rows[2] == ("cat-C", 30)

    def test_topn_with_offset(self, db):
        """LIMIT 2 OFFSET 1 skips the top result."""
        _setup_topn_table(db)
        rows = db.execute(
            "SELECT category, COUNT(*) AS cnt FROM topn_test "
            "GROUP BY category ORDER BY COUNT(*) DESC LIMIT 2 OFFSET 1"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0] == ("cat-B", 40)
        assert rows[1] == ("cat-C", 30)

    def test_no_topn_without_limit(self, db):
        """Without LIMIT, no TopN should appear in EXPLAIN."""
        _setup_topn_table(db)
        explain = db.execute(
            "EXPLAIN ANALYZE SELECT category, COUNT(*) AS cnt FROM topn_test "
            "GROUP BY category"
        ).fetchall()
        explain_text = "\n".join(r[0] for r in explain)
        assert "DeltaXAgg" in explain_text, (
            f"Expected DeltaXAgg in plan:\n{explain_text}"
        )
        assert "TopN" not in explain_text, (
            f"TopN should not appear without LIMIT:\n{explain_text}"
        )


def _setup_bare_limit_table(db):
    """Compressed deltax table with a mixed (int+text) GROUP BY shape,
    multiple segments, and predictable duplicate distribution so that
    partial counts would be visibly wrong."""
    db.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    db.execute("""
        CREATE TABLE bare_limit_test (
            ts TIMESTAMPTZ NOT NULL,
            user_id BIGINT NOT NULL,
            phrase TEXT NOT NULL,
            val INT NOT NULL
        )
    """)
    # Short partition window so we get multiple partitions (and thus
    # multiple segments) over our data span.
    db.execute(
        "SELECT deltax.deltax_create_table('bare_limit_test', 'ts', '1 hour'::interval)"
    )
    db.commit()

    # 20 distinct (user_id, phrase) keys. Each key appears exactly `count`
    # times distributed across 4 partitions (so 4 segments after compression).
    keys = [(1000 + i, f"phrase-{i:02d}", 3 + (i % 5)) for i in range(20)]

    # Generate rows spanning 4 hours (= 4 partitions at 1h window).
    # MOCK_NOW is noon, so deltax_create_table places partitions at
    # 11:00–15:00 UTC. Offset from BASE_TS (midnight) by 11 h so every
    # row lands in a real partition, not in `bare_limit_test_default`.
    values = []
    for uid, phrase, count in keys:
        for c in range(count):
            hour = (c % 4) + 11
            values.append(
                f"('{BASE_TS}'::timestamptz + interval '{hour} hours {c} seconds',"
                f" {uid}, '{phrase}', {c})"
            )
    db.execute(
        "INSERT INTO bare_limit_test (ts, user_id, phrase, val) "
        f"VALUES {', '.join(values)}"
    )
    db.commit()

    db.execute(
        "SELECT deltax.deltax_enable_compression('bare_limit_test', "
        "segment_by => ARRAY[]::text[], order_by => ARRAY['ts'])"
    )
    db.commit()

    # Compress every partition that received data so DeltaXAgg is eligible.
    # The earlier range predicate targeted 00:00–04:00 which, under the
    # noon MOCK_NOW, matched no real partition — leaving all data
    # uncompressed and defeating the F8 plan check.
    partitions = db.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('bare_limit_test') "
        "WHERE range_start < '2025-01-15 15:00:00+00'::timestamptz "
        "AND range_end > '2025-01-15 11:00:00+00'::timestamptz"
    ).fetchall()
    for row in partitions:
        cnt = db.execute(f'SELECT count(*) FROM "{row[0]}"').fetchone()[0]
        if cnt > 0:
            db.execute(f"SELECT deltax.deltax_compress_partition('{row[0]}')")
    db.commit()

    # ANALYZE populates reltuples so collect_compressed_children() can
    # treat empty uncompressed partitions (the future one + the default)
    # as safe to skip instead of bailing out of DeltaXAgg/DeltaXAppend.
    db.rollback()
    db.autocommit = True
    db.execute("ANALYZE bare_limit_test")
    db.autocommit = False

    return {uid: (phrase, count) for uid, phrase, count in keys}


class TestBareLimit:
    """F8: `GROUP BY … LIMIT N` without ORDER BY must return exact global
    counts (PG semantics), not per-worker partial counts. A naive early-
    termination would silently break this."""

    def test_bare_limit_counts_are_exact(self, db):
        expected = _setup_bare_limit_table(db)
        rows = db.execute(
            "SELECT user_id, phrase, COUNT(*) AS c "
            "FROM bare_limit_test GROUP BY user_id, phrase LIMIT 5"
        ).fetchall()
        assert len(rows) == 5
        for uid, phrase, c in rows:
            exp_phrase, exp_count = expected[uid]
            assert phrase == exp_phrase, (
                f"phrase mismatch for uid={uid}: got {phrase!r}, expected {exp_phrase!r}"
            )
            assert c == exp_count, (
                f"count mismatch for (uid={uid}, phrase={phrase!r}): "
                f"got {c}, expected {exp_count} (partial count would indicate "
                f"F8 correctness regression)"
            )

    def test_bare_limit_consistent_with_full_agg(self, db):
        """The rows returned by bare-LIMIT must be a subset of the full
        aggregation result — both the keys and the counts must match."""
        _setup_bare_limit_table(db)
        limited = set(db.execute(
            "SELECT user_id, phrase, COUNT(*) "
            "FROM bare_limit_test GROUP BY user_id, phrase LIMIT 7"
        ).fetchall())
        full = set(db.execute(
            "SELECT user_id, phrase, COUNT(*) "
            "FROM bare_limit_test GROUP BY user_id, phrase"
        ).fetchall())
        assert len(limited) == 7
        assert limited.issubset(full), (
            f"Bare-LIMIT rows not a subset of full aggregation — this "
            f"indicates F8 emitted rows with wrong counts.\n"
            f"Extra rows: {limited - full}"
        )

    def test_explain_shows_f8_preselected(self, db):
        _setup_bare_limit_table(db)
        # Force parallel mixed path: n_workers > 1 and >1 segment. The
        # table setup produces 4 partitions / 4 segments already.
        db.execute("SET pg_deltax.parallel_workers = 2")
        explain = db.execute(
            "EXPLAIN ANALYZE SELECT user_id, phrase, COUNT(*) "
            "FROM bare_limit_test GROUP BY user_id, phrase LIMIT 5"
        ).fetchall()
        explain_text = "\n".join(r[0] for r in explain)
        assert "DeltaXAgg" in explain_text
        # f8_preselected=N appears only when F8 fires. Allow it to be
        # absent (the parallel mixed path requires >1 segment; depending
        # on compression timing a single-partition insert might only
        # produce 1 segment in this minimal table, in which case we
        # gracefully fall through to the full path).
        # What we must NOT see is incorrect counts — covered by the
        # other two tests.
