"""Integration tests for time_bucket, first, and last functions."""

from datetime import datetime, timezone


def _setup_metrics(db):
    """Helper: create a seaturtle table and insert test rows."""
    db.execute(
        "CREATE TABLE metrics (ts TIMESTAMPTZ NOT NULL, device TEXT, value FLOAT8)"
    )
    db.commit()

    db.execute("SELECT seaturtle_create_table('metrics', 'ts')")
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
    """time_bucket('5 minutes', ts) truncates to 5-min boundary."""
    row = db.execute(
        "SELECT time_bucket('5 minutes'::interval, '2025-06-15 14:23:42+00'::timestamptz)"
    ).fetchone()
    assert row[0] == datetime(2025, 6, 15, 14, 20, 0, tzinfo=timezone.utc)


def test_time_bucket_1hour(db):
    """time_bucket('1 hour', ts) truncates to hour boundary."""
    row = db.execute(
        "SELECT time_bucket('1 hour'::interval, '2025-06-15 14:23:42+00'::timestamptz)"
    ).fetchone()
    assert row[0] == datetime(2025, 6, 15, 14, 0, 0, tzinfo=timezone.utc)


def test_time_bucket_with_offset(db):
    """time_bucket with offset shifts the bucket boundary."""
    row = db.execute(
        "SELECT time_bucket('1 day'::interval, '2025-06-15 14:23:42+00'::timestamptz, '6 hours'::interval)"
    ).fetchone()
    # Bucket starts at 06:00 UTC on 2025-06-15
    assert row[0] == datetime(2025, 6, 15, 6, 0, 0, tzinfo=timezone.utc)


def test_first_last(db):
    """first(value, ts) and last(value, ts) return correct values."""
    db.execute(
        "CREATE TABLE fl (ts TIMESTAMPTZ NOT NULL, value FLOAT8)"
    )
    db.commit()

    db.execute("SELECT seaturtle_create_table('fl', 'ts')")
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

    row = db.execute("SELECT first(value, ts), last(value, ts) FROM fl").fetchone()
    assert row[0] == 100.0  # earliest ts
    assert row[1] == 300.0  # latest ts


def test_first_last_with_groups(db):
    """first/last work with GROUP BY."""
    db.execute(
        "CREATE TABLE grouped (ts TIMESTAMPTZ NOT NULL, device TEXT, value FLOAT8)"
    )
    db.commit()

    db.execute("SELECT seaturtle_create_table('grouped', 'ts')")
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
        "SELECT device, first(value, ts), last(value, ts) "
        "FROM grouped GROUP BY device ORDER BY device"
    ).fetchall()

    assert rows[0] == ("a", 10.0, 30.0)
    assert rows[1] == ("b", 20.0, 40.0)
