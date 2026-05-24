"""Integration tests for deltax_create_table, partition_info, and deltatable_info."""

from datetime import datetime, timezone


def test_create_table_basic(db):
    """deltax_create_table with defaults creates 5 partitions (1 past + 1 current + 3 future)."""
    db.execute(
        "CREATE TABLE metrics (ts TIMESTAMPTZ NOT NULL, device TEXT, value FLOAT8)"
    )
    db.commit()

    row = db.execute("SELECT deltax.deltax_create_table('metrics', 'ts')").fetchone()
    db.commit()

    assert "Created deltax table" in row[0]
    assert "5 partitions" in row[0]

    partitions = db.execute(
        "SELECT * FROM deltax.deltax_partition_info('metrics')"
    ).fetchall()
    assert len(partitions) == 5


def test_create_table_custom_interval(db):
    """1-hour interval produces partition names with YYYYMMDD_HHMM format."""
    db.execute(
        "CREATE TABLE hourly (ts TIMESTAMPTZ NOT NULL, val FLOAT8)"
    )
    db.commit()

    db.execute("SELECT deltax.deltax_create_table('hourly', 'ts', '1 hour')")
    db.commit()

    partitions = db.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('hourly')"
    ).fetchall()
    assert len(partitions) == 5

    # Sub-daily partitions use YYYYMMDD_HHMM format
    for (name,) in partitions:
        # e.g. hourly_p20260224_1400
        suffix = name.replace("hourly_p", "")
        assert "_" in suffix, f"Expected YYYYMMDD_HHMM format, got: {name}"


def test_create_table_custom_premake(db):
    """premake=1 creates 3 partitions (1 past + 1 current + 1 future)."""
    db.execute(
        "CREATE TABLE few (ts TIMESTAMPTZ NOT NULL, val FLOAT8)"
    )
    db.commit()

    row = db.execute(
        "SELECT deltax.deltax_create_table('few', 'ts', '1 day', 1)"
    ).fetchone()
    db.commit()

    assert "3 partitions" in row[0]

    partitions = db.execute(
        "SELECT * FROM deltax.deltax_partition_info('few')"
    ).fetchall()
    assert len(partitions) == 3


def test_create_table_already_exists(db):
    """Calling deltax_create_table twice returns 'already a deltax table'."""
    db.execute(
        "CREATE TABLE dup (ts TIMESTAMPTZ NOT NULL, val FLOAT8)"
    )
    db.commit()

    db.execute("SELECT deltax.deltax_create_table('dup', 'ts')")
    db.commit()

    row = db.execute("SELECT deltax.deltax_create_table('dup', 'ts')").fetchone()
    db.commit()

    assert "already a deltax table" in row[0]


def test_insert_and_query(db):
    """Insert rows and SELECT with WHERE on time column."""
    db.execute(
        "CREATE TABLE sensor (ts TIMESTAMPTZ NOT NULL, device TEXT, value FLOAT8)"
    )
    db.commit()

    db.execute("SELECT deltax.deltax_create_table('sensor', 'ts')")
    db.commit()

    now = datetime.now(timezone.utc)
    db.execute(
        "INSERT INTO sensor VALUES (%s, 'dev1', 10.0), (%s, 'dev2', 20.0)",
        (now, now),
    )
    db.commit()

    rows = db.execute(
        "SELECT device, value FROM sensor WHERE ts >= %s ORDER BY device", (now,)
    ).fetchall()
    assert len(rows) == 2
    assert rows[0] == ("dev1", 10.0)
    assert rows[1] == ("dev2", 20.0)


def test_deltatable_info(db):
    """deltax_deltatable_info returns correct metadata."""
    db.execute(
        "CREATE TABLE ht_test (ts TIMESTAMPTZ NOT NULL, val FLOAT8)"
    )
    db.commit()

    db.execute("SELECT deltax.deltax_create_table('ht_test', 'ts', '1 day')")
    db.commit()

    row = db.execute(
        "SELECT schema_name, table_name, time_column, partition_interval, num_partitions "
        "FROM deltax.deltax_deltatable_info('ht_test')"
    ).fetchone()

    assert row[0] == "public"
    assert row[1] == "ht_test"
    assert row[2] == "ts"
    # partition_interval comes back as an interval type
    assert "1 day" in str(row[3])
    assert row[4] == 5  # default premake=3 → 5 partitions


def test_partition_info_ordering(db):
    """Partitions are returned ordered by range_start."""
    db.execute(
        "CREATE TABLE ordered (ts TIMESTAMPTZ NOT NULL, val FLOAT8)"
    )
    db.commit()

    db.execute("SELECT deltax.deltax_create_table('ordered', 'ts')")
    db.commit()

    rows = db.execute(
        "SELECT range_start FROM deltax.deltax_partition_info('ordered')"
    ).fetchall()

    starts = [r[0] for r in rows]
    assert starts == sorted(starts), "Partitions should be ordered by range_start"
