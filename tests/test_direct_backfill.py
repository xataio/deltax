"""Integration tests for direct backfill (COPY FROM with FORMAT deltax_compress)."""

import io
import csv
from datetime import datetime, timezone, timedelta

import psycopg
import pytest


def _setup_table(db, table_name="backfill", interval="1 day", segment_by=None, order_by=None):
    """Create a deltax table with compression enabled."""
    db.execute(
        f"CREATE TABLE {table_name} (ts TIMESTAMPTZ NOT NULL, device TEXT, value FLOAT8)"
    )
    db.commit()

    db.execute(f"SELECT deltax.deltax_create_table('{table_name}', 'ts', '{interval}')")
    db.commit()

    seg_by = f"ARRAY{segment_by}" if segment_by else "ARRAY[]::text[]"
    ord_by = f"ARRAY{order_by}" if order_by else "ARRAY['ts']"
    db.execute(
        f"SELECT deltax.deltax_enable_compression('{table_name}', segment_by => {seg_by}, order_by => {ord_by})"
    )
    db.commit()


def _generate_csv(rows, include_header=False):
    """Generate CSV string from rows [(ts, device, value), ...]."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    if include_header:
        writer.writerow(["ts", "device", "value"])
    for row in rows:
        writer.writerow(row)
    return buf.getvalue()


def _copy_from_stdin(db, table_name, csv_data, extra_opts=""):
    """Execute COPY FROM STDIN with FORMAT deltax_compress.

    Uses DELIMITER ',' since test data is CSV (comma-separated).
    The underlying COPY parser defaults to TEXT format (tab-separated),
    so we explicitly set the delimiter for our CSV test data.
    """
    opts = "FORMAT deltax_compress, DELIMITER ','"
    if extra_opts:
        opts += ", " + extra_opts
    sql = f"COPY {table_name} FROM STDIN WITH ({opts})"
    with db.cursor() as cur:
        with cur.copy(sql) as copy:
            copy.write(csv_data.encode())
    db.commit()


def test_basic_roundtrip(db):
    """Load rows via direct backfill, verify they are queryable."""
    _setup_table(db)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    rows = [(now.isoformat(), "dev1", 10.0), (now.isoformat(), "dev2", 20.0)]
    _copy_from_stdin(db, "backfill", _generate_csv(rows))

    result = db.execute("SELECT COUNT(*) FROM backfill").fetchone()
    assert result[0] == 2

    result = db.execute(
        "SELECT device, value FROM backfill ORDER BY device"
    ).fetchall()
    assert result[0] == ("dev1", 10.0)
    assert result[1] == ("dev2", 20.0)


def test_multi_partition(db):
    """Load data spanning multiple partitions."""
    _setup_table(db)

    base = datetime.now(timezone.utc).replace(microsecond=0)
    rows = []
    # Spread across 3 days (yesterday, today, tomorrow)
    for day_offset in [-1, 0, 1]:
        ts = base + timedelta(days=day_offset)
        rows.append((ts.isoformat(), f"dev{day_offset}", float(day_offset)))

    _copy_from_stdin(db, "backfill", _generate_csv(rows))

    result = db.execute("SELECT COUNT(*) FROM backfill").fetchone()
    assert result[0] == 3

    # Check compression stats - at least 2 partitions should be compressed
    stats = db.execute(
        "SELECT partition_name, is_compressed, row_count FROM deltax.deltax_compression_stats('backfill') WHERE is_compressed = true"
    ).fetchall()
    assert len(stats) >= 2


def test_query_after_backfill(db):
    """Run aggregate and filter queries on backfilled data."""
    _setup_table(db)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    rows = []
    for i in range(100):
        ts = now + timedelta(seconds=i)
        rows.append((ts.isoformat(), f"dev{i % 5}", float(i)))

    _copy_from_stdin(db, "backfill", _generate_csv(rows))

    # Aggregate
    result = db.execute("SELECT SUM(value), COUNT(*), AVG(value) FROM backfill").fetchone()
    assert result[0] == pytest.approx(sum(range(100)))
    assert result[1] == 100

    # Filter
    result = db.execute(
        "SELECT COUNT(*) FROM backfill WHERE device = 'dev0'"
    ).fetchone()
    assert result[0] == 20

    # Time range filter
    result = db.execute(
        f"SELECT COUNT(*) FROM backfill WHERE ts >= '{now.isoformat()}' AND ts < '{(now + timedelta(seconds=50)).isoformat()}'"
    ).fetchone()
    assert result[0] == 50


def test_already_compressed_error(db):
    """Loading into an already-compressed partition should fail."""
    _setup_table(db)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    rows = [(now.isoformat(), "dev1", 10.0)]

    # First load works
    _copy_from_stdin(db, "backfill", _generate_csv(rows))

    # Second load into same partition should error (already compressed)
    with pytest.raises(psycopg.errors.InternalError_, match="already compressed"):
        _copy_from_stdin(db, "backfill", _generate_csv(rows))
    db.rollback()


def test_compression_not_enabled_error(db):
    """FORMAT deltax_compress on a table without compression enabled should fail."""
    db.execute(
        "CREATE TABLE nocomp (ts TIMESTAMPTZ NOT NULL, device TEXT, value FLOAT8)"
    )
    db.commit()
    db.execute("SELECT deltax.deltax_create_table('nocomp', 'ts')")
    db.commit()

    now = datetime.now(timezone.utc).replace(microsecond=0)
    rows = [(now.isoformat(), "dev1", 10.0)]

    with pytest.raises(psycopg.errors.InternalError_, match="compression not enabled"):
        _copy_from_stdin(db, "nocomp", _generate_csv(rows))
    db.rollback()


def test_large_load_multiple_segments(db):
    """Load enough rows to trigger multiple segment flushes."""
    _setup_table(db)

    # Default segment_size is 30000, load 35000 rows
    now = datetime.now(timezone.utc).replace(microsecond=0)
    rows = []
    for i in range(35000):
        ts = now + timedelta(seconds=i)
        rows.append((ts.isoformat(), f"dev{i % 10}", float(i)))

    _copy_from_stdin(db, "backfill", _generate_csv(rows))

    result = db.execute("SELECT COUNT(*) FROM backfill").fetchone()
    assert result[0] == 35000

    # Verify the total sum is correct
    result = db.execute("SELECT SUM(value) FROM backfill").fetchone()
    assert result[0] == pytest.approx(sum(range(35000)))


def test_partial_segment(db):
    """Load fewer than segment_size rows — creates a partial segment."""
    _setup_table(db)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    rows = []
    for i in range(100):
        ts = now + timedelta(seconds=i)
        rows.append((ts.isoformat(), "dev1", float(i)))

    _copy_from_stdin(db, "backfill", _generate_csv(rows))

    result = db.execute("SELECT COUNT(*) FROM backfill").fetchone()
    assert result[0] == 100

    result = db.execute("SELECT SUM(value) FROM backfill").fetchone()
    assert result[0] == pytest.approx(sum(range(100)))


def test_with_header(db):
    """Verify FORMAT deltax_compress works with HEADER true option."""
    _setup_table(db)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    rows = [(now.isoformat(), "dev1", 10.0)]
    csv_data = _generate_csv(rows, include_header=True)

    _copy_from_stdin(db, "backfill", csv_data, extra_opts="HEADER true")

    result = db.execute("SELECT COUNT(*) FROM backfill").fetchone()
    assert result[0] == 1


def test_null_values(db):
    """Handle NULL values in non-time columns."""
    _setup_table(db)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    # TEXT format uses \N for NULL by default; with DELIMITER ',' we use that convention
    csv_data = f"{now.isoformat()},\\N,\\N\n{now.isoformat()},dev1,10.0\n"

    _copy_from_stdin(db, "backfill", csv_data)

    result = db.execute("SELECT COUNT(*) FROM backfill").fetchone()
    assert result[0] == 2

    result = db.execute("SELECT COUNT(*) FROM backfill WHERE device IS NULL").fetchone()
    assert result[0] == 1

    result = db.execute("SELECT COUNT(*) FROM backfill WHERE value IS NULL").fetchone()
    assert result[0] == 1


def test_not_deltax_table_error(db):
    """FORMAT deltax_compress on a non-deltax table should error."""
    db.execute("CREATE TABLE plain (ts TIMESTAMPTZ, val FLOAT8)")
    db.commit()

    now = datetime.now(timezone.utc).replace(microsecond=0)
    rows = [(now.isoformat(), 10.0)]
    buf = io.StringIO()
    writer = csv.writer(buf)
    for row in rows:
        writer.writerow(row)

    with pytest.raises(psycopg.errors.InternalError_, match="not a deltax table"):
        sql = "COPY plain FROM STDIN WITH (FORMAT deltax_compress, DELIMITER ',')"
        with db.cursor() as cur:
            with cur.copy(sql) as copy:
                copy.write(buf.getvalue().encode())
        db.commit()
    db.rollback()


def test_normal_copy_still_works(db):
    """Normal COPY (without FORMAT deltax_compress) should work as before."""
    _setup_table(db)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    csv_data = f"{now.isoformat()},dev1,10.0\n"

    sql = "COPY backfill FROM STDIN WITH (FORMAT csv)"
    with db.cursor() as cur:
        with cur.copy(sql) as copy:
            copy.write(csv_data.encode())
    db.commit()

    result = db.execute("SELECT COUNT(*) FROM backfill").fetchone()
    assert result[0] == 1


def test_csv_format_with_quoted_fields(db):
    """FORMAT deltax_compress_csv handles CSV-quoted fields (embedded
    commas, quotes) by routing through PG's CSV parser."""
    _setup_table(db)

    # CSV data: JSON-like text in 'device' column with embedded commas,
    # plus a row with a double-quote that needs CSV escaping ("").
    now = datetime.now(timezone.utc).replace(microsecond=0)
    csv_data = (
        f'{now.isoformat()},"{{""terminal"": ""Berlin"", ""lane"": 3}}",1.0\n'
        f'{now.isoformat()},"dev with, comma",2.0\n'
        f'{now.isoformat()},dev_plain,3.0\n'
    )
    sql = "COPY backfill FROM STDIN WITH (FORMAT deltax_compress_csv)"
    with db.cursor() as cur:
        with cur.copy(sql) as copy:
            copy.write(csv_data.encode())
    db.commit()

    rows = db.execute(
        "SELECT device, value FROM backfill ORDER BY value"
    ).fetchall()
    assert rows[0] == ('{"terminal": "Berlin", "lane": 3}', 1.0)
    assert rows[1] == ("dev with, comma", 2.0)
    assert rows[2] == ("dev_plain", 3.0)


def test_with_segment_by(db):
    """Direct backfill with segment_by columns — stores data without segment grouping.

    Note: direct backfill currently doesn't group by segment_by values.
    All rows go into the same segments with NULL segment_by.
    The data is still correctly queryable via decompression.
    """
    _setup_table(db, segment_by="['device']", order_by="['ts']")

    now = datetime.now(timezone.utc).replace(microsecond=0)
    rows = []
    for i in range(100):
        ts = now + timedelta(seconds=i)
        rows.append((ts.isoformat(), f"dev{i % 3}", float(i)))

    _copy_from_stdin(db, "backfill", _generate_csv(rows))

    result = db.execute("SELECT COUNT(*) FROM backfill").fetchone()
    assert result[0] == 100

    # Verify total sum is correct
    result = db.execute("SELECT SUM(value) FROM backfill").fetchone()
    assert result[0] == pytest.approx(sum(range(100)))
